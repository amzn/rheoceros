# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from unittest.mock import MagicMock

from intelliflow.api_ext import *
from intelliflow.core.application.application import Application
from intelliflow.core.platform.definitions.compute import (
    ComputeFailedSessionState,
    ComputeFailedSessionStateType,
    ComputeResourceDesc,
    ComputeResponse,
    ComputeSessionDesc,
    ComputeSessionState,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.signal_processing import Slot
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSApplicationExecutionChain(AWSTestBase):
    app: Application = None

    def test_application_signal_propagation(self):
        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        def invoke_lambda_function(lambda_client, function_name, function_params, is_async=True):
            """Synchronize internal async lambda invocations so that the chaining would be active during testing."""
            return self.app.platform.processor.process(function_params, use_activated_instance=False)

        self.patch_aws_start(glue_catalog_has_all_tables=True, invoke_lambda_function_mock=invoke_lambda_function)

        self.app = TestAWSApplicationExecutionControl._create_test_application(self, "exec_chain")

        # d_unified_cust_shipment_items
        # Extend the pipeline
        # 1 - TWO LEVEL PROPAGATION
        repeat_ducsi = self.app.get_data("REPEAT_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        ducsi_with_so = self.app.get_data("DUCSI_WITH_SO", context=Application.QueryContext.DEV_CONTEXT)[0]

        join_node = self.app.create_data(
            id="JOIN_NODE", inputs=[repeat_ducsi, ducsi_with_so], compute_targets="output=REPEAT_DUCSI.join(DUCSI_WITH_SO, ['customer_id])"
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = self.app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        self.app._dev_context = dev_context
        #

        self.app.activate()

        assert self.app.poll(join_node[1]["2020-12-28"]) == (None, None)

        # mock batch_compute for instant success on batch jobs
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return TestAWSApplicationExecutionControl.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_1")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return TestAWSApplicationExecutionControl.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED)

        self.app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        self.app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        # now feed the system with external signals
        self.app.process(self.app["DEXML_DUCSI"][1]["2020-12-28"])
        self.app.process(self.app["ship_options"])

        # join_node must have been calculated already
        join_output_partition_path, _ = self.app.poll(join_node[1]["2020-12-28"])
        assert join_output_partition_path.endswith("internal_data/JOIN_NODE/1/2020-12-28")

        # 2- THREE LEVEL PROPAGATION
        # now add a tail node to the pipeline that will have two compute targets.
        #  - inlined compute that will fail
        #  - a trivial batch compute that copies the input data
        # Propagation will reach down to the tail, however tail node execution will marked as failed due to
        # one of the compute targets.

        # reload the app to get rid of the mocks (otherwise during the next activation, we will get PicklingError)
        self.app = AWSApplication("exec_chain", self.region)
        self.app.attach()
        join_node2 = self.app.create_data(
            id="JOIN_NODE_2",
            inputs=[join_node],
            compute_targets=[InlinedCompute(lambda input_map, output, params: int("str")), GlueBatchCompute("output=JOIN_NODE")],
        )

        self.app.activate()

        # mock batch_compute again for instant success on batch jobs
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return TestAWSApplicationExecutionControl.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_2")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return TestAWSApplicationExecutionControl.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED)

        self.app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        self.app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        # now feed the system with external signals again (use next day partition to avoid confusion)
        self.app.process(self.app["DEXML_DUCSI"][1]["2020-12-29"])
        self.app.process(self.app["ship_options"])

        # first check again and see join_node partition created again
        join_output_partition_path, _ = self.app.poll(join_node[1]["2020-12-29"])
        assert join_output_partition_path == self.app.materialize(join_node[1]["2020-12-29"])[0]

        # BUT the tail node ('join_node2') should return None since the most recent execution was not successful.
        path, compute_records = self.app.poll(join_node2[1]["2020-12-29"])
        assert path is None
        assert len(compute_records) == 1
        assert compute_records[0].session_state.state_type == ComputeSessionStateType.FAILED

        # now check the execution history down the whole chain
        assert len(list(self.app.get_inactive_compute_records("REPEAT_DUCSI"))) == 2  # 2 execution x 1 compute = 2
        assert len(list(self.app.get_inactive_compute_records(self.app["DUCSI_WITH_SO"]))) == 2
        assert len(list(self.app.get_inactive_compute_records(join_node))) == 2
        assert len(list(self.app.get_inactive_compute_records(join_node2))) == 2  # 1 execution x 2 compute targets = 2
        # there should be no route with an active record left at this point.
        assert not self.app.get_active_routes()

        self.patch_aws_stop()
