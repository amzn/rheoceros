# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import threading
import time
from typing import Callable
from unittest.mock import MagicMock

import pytest

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.application.application import Application
from intelliflow.core.platform import development as development_module
from intelliflow.core.platform.compute_targets.email import EMAIL
from intelliflow.core.platform.compute_targets.slack import Slack
from intelliflow.core.platform.constructs import ConstructPermission
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
from intelliflow.core.signal_processing.routing_runtime_constructs import RouteMetadataAction
from intelliflow.core.signal_processing.signal import *
from intelliflow.core.signal_processing.signal_source import DatasetMetadata
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.hook import GenericComputeDescriptorHookVerifier, GenericRoutingHookImpl, OnExecBeginHookImpl
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationExecutionHooks(AWSTestBase):
    def _create_test_application(self, id_or_app: Union[str, Application]):
        if isinstance(id_or_app, str):
            id = id_or_app
            app = AWSApplication(id, region=self.region)
        else:
            app = id_or_app
            id = app.id

        ducsi_data = app.marshal_external_data(
            GlueTable(
                "booker",
                "d_unified_cust_shipment_items",
                partition_keys=["region_id", "ship_day"],
            ),
            "DEXML_DUCSI",
            {"region_id": {"type": DimensionType.LONG, "ship_day": {"format": "%Y-%m-%d", "type": DimensionType.DATETIME}}},
            {"1": {"*": {"timezone": "PST"}}},
            SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT"]}),
        )

        # add a dimensionless table (important corner-case)
        ship_options = app.marshal_external_data(
            GlueTable(
                "dexbi",
                "d_ship_option",
                partition_keys=[],
            ),
            "ship_options",
            {},
            {},
            SignalIntegrityProtocol("FILE_CHECK", {"file": ["DELTA", "SNAPSHOT"]}),
        )

        return app

    def _test_all_application_hooks(self, hook_generator: Callable):
        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("exec-hooks")

        ducsi_data = app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        ship_options = app.get_data("ship_options", context=Application.QueryContext.DEV_CONTEXT)[0]

        email_obj = EMAIL(sender="if-test-list@amazon.com", recipient_list=["yunusko@amazon.com"])

        on_exec_begin_hook = hook_generator()
        on_exec_skipped_hook = hook_generator()
        on_compute_success_hook = hook_generator()
        on_compute_failure_hook = hook_generator()
        on_compute_retry_hook = hook_generator()
        on_success_hook = hook_generator()
        on_failure_hook = hook_generator()
        exec_checkpoints = [
            RouteCheckpoint(checkpoint_in_secs=10, slot=hook_generator()),
            RouteCheckpoint(checkpoint_in_secs=20, slot=hook_generator()),
        ]
        exec_metadata_actions = [
            RouteMetadataAction(
                condition=lambda output_metadata: output_metadata[DatasetMetadata.RECORD_COUNT.value] < 5, slot=hook_generator()
            ),
            RouteMetadataAction(
                condition=lambda output_metadata: output_metadata["custom_metadata_field"] == "hello", slot=hook_generator()
            ),
        ]

        def metadata_provider_compute(input_map, output, params):
            params["runtime_platform"].storage.save_internal_metadata(
                output, {DatasetMetadata.RECORD_COUNT.value: 4, "custom_metadata_field": "hello"}
            )

        repeat_ducsi = app.create_data(
            id="REPEAT_DUCSI",
            inputs={
                "DEXML_DUCSI": ducsi_data,
            },
            compute_targets=[Glue("output=DEXML_DUCSI.limit(100)"), InlinedCompute(metadata_provider_compute)],
            execution_hook=RouteExecutionHook(
                on_exec_begin=on_exec_begin_hook,
                on_exec_skipped=on_exec_skipped_hook,
                on_compute_success=on_compute_success_hook,
                on_compute_failure=on_compute_failure_hook,
                on_compute_retry=on_compute_retry_hook,
                on_success=on_success_hook,
                on_failure=on_failure_hook,
                checkpoints=exec_checkpoints,
                metadata_actions=exec_metadata_actions,
            ),
        )

        on_exec_skipped_hook_2 = hook_generator()
        on_pending_node_created_hook = hook_generator()
        on_expiration_hook = hook_generator()
        pending_node_checkpoints = [RouteCheckpoint(checkpoint_in_secs=10, slot=hook_generator())]

        # we will be using this second node for Pending Node checks mostly
        app.create_data(
            id="DUCSI_WITH_SO",
            inputs={"DEXML_DUCSI": ducsi_data["*"][:-2], "SHIP_OPTIONS": ship_options},
            compute_targets="output=DEXML_DUCSI.limit(100).join(SHIP_OPTIONS, DEXML_DUCSI.customer_ship_option == SHIP_OPTIONS.ship_option)",
            execution_hook=RouteExecutionHook(on_exec_skipped=on_exec_skipped_hook_2),
            pending_node_hook=RoutePendingNodeHook(
                on_pending_node_created=on_pending_node_created_hook, on_expiration=on_expiration_hook, checkpoints=pending_node_checkpoints
            ),
            pending_node_expiration_ttl_in_secs=20,
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate()

        # 1- Inject DUCSI event to trigger execution on the first node/route and create a pending node on the second.
        # mock batch_compute response
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            assert materialized_output.alias == "REPEAT_DUCSI"
            return TestAWSApplicationExecutionControl.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_id")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            assert session_desc.session_id == "job_id"
            return TestAWSApplicationExecutionControl.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING, session_desc)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.process(
            ducsi_data[1]["2020-12-25"],
            # make it SYNC (use the local processor instance in sync mode)
            with_activated_processor=False,
        )
        assert len(app.get_active_routes()) == 1

        # check if the first exec hook has been hit and done with its own logic
        assert on_exec_begin_hook.verify(app)
        assert not on_exec_skipped_hook.verify(app)
        assert not on_compute_failure_hook.verify(app)
        assert on_compute_success_hook.verify(app)  # InlinedCompute succeeds right away in the first pass
        assert not on_compute_retry_hook.verify(app)
        assert not on_success_hook.verify(app)
        assert not on_failure_hook.verify(app)

        # check the pending node hooks registered on the second route.
        assert on_pending_node_created_hook.verify(app)
        assert not on_exec_skipped_hook_2.verify(app)
        assert not on_expiration_hook.verify(app)

        # emulate runtime Processor behaviour, to check the routes otherwise checkpoints won't be checked.
        # reason: in unit-tests the Processor does not 'run' in the background. So the following call is somewhat like
        # a 'next cycle/tick',
        app.update_active_routes_status()
        assert not any([c.slot.verify(app) for c in exec_checkpoints])
        # metadata actions must be fired up already thanks to InlinedCompute
        assert all([c.slot.verify(app) for c in exec_metadata_actions])
        assert not any([c.slot.verify(app) for c in pending_node_checkpoints])

        time.sleep(10)
        # next-cycle again
        app.update_active_routes_status()
        # execution passed the checkpoint 10 secs
        assert exec_checkpoints[0].slot.verify(app)
        # pending node passed the checkpoint 10 secs
        assert pending_node_checkpoints[0].slot.verify(app)

        # now the second internal data node (route) in the system actually waits for its second input dependency
        # 'ship_options'. Previous process call with ducsi has created a pending node in it as well. a signal for
        # 'ship_options' will complete that pending node and cause a trigger.
        ship_options = app.get_data("ship_options", context=Application.QueryContext.DEV_CONTEXT)[0]

        # mock again
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            assert materialized_output.alias == "DUCSI_WITH_SO"
            return TestAWSApplicationExecutionControl.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_id2")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return TestAWSApplicationExecutionControl.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING, session_desc)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.process(
            ship_options,
            # make it SYNC (use the local processor instance in sync mode)
            with_activated_processor=False,
        )

        assert len(app.get_active_routes()) == 2

        # check idempotency
        app.process(ducsi_data[1]["2020-12-25"], with_activated_processor=False)
        # now we can check the skipped hook due to idempotency related call above
        assert on_exec_skipped_hook.verify(app)

        # no effect (still the same count on the mock objects)
        app.process(ship_options, with_activated_processor=False)
        assert on_exec_skipped_hook_2.verify(app)

        # initiate another trigger on 'REPEAT_DUCSI' with a different partition (12-26)
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            assert materialized_output.alias == "REPEAT_DUCSI"
            return TestAWSApplicationExecutionControl.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_id3")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return TestAWSApplicationExecutionControl.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING, session_desc)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.process(ducsi_data[1]["2020-12-26"], with_activated_processor=False)

        # finish first job (from 12-25 on both routes), since Processor is not running in the background now
        #   we will have to use related app API to force update RoutingTable status.
        # only active record remaining should be the most recent one (12-26):
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            raise RuntimeError(
                "This should not be called since we are not suppoed to yield a new active record" "at this point in this test"
            )

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            if session_desc.session_id in ["job_id"]:  # first active record
                return TestAWSApplicationExecutionControl.create_batch_compute_session_state(
                    ComputeSessionStateType.COMPLETED, session_desc
                )
            else:
                return TestAWSApplicationExecutionControl.create_batch_compute_session_state(
                    ComputeSessionStateType.PROCESSING, session_desc
                )

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.update_active_routes_status()

        assert on_compute_success_hook.verify(app)
        assert not on_compute_failure_hook.verify(app)
        assert not on_compute_retry_hook.verify(app)
        assert on_success_hook.verify(app)
        assert all([c.slot.verify(app) for c in exec_metadata_actions])
        assert not on_failure_hook.verify(app)

        # we now have only one active record (active batch compute session) and a pending node, move 15 secs to:
        #  - cause expiration on the only job of the second route
        time.sleep(20)
        app.update_active_routes_status()
        assert on_expiration_hook.verify(app)

        # move 10 more to:
        #  - cause second checkpoint to be called on the first route (due to second execution)
        time.sleep(25)
        app.update_active_routes_status()
        assert exec_checkpoints[1].slot.verify(app)

        # finish the third job (12-26) of the first route with FAILURE
        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            if session_desc.session_id in ["job_id3"]:  # third active record
                return TestAWSApplicationExecutionControl.create_batch_compute_failed_session_state(
                    ComputeFailedSessionStateType.APP_INTERNAL, session_desc
                )
            else:
                return TestAWSApplicationExecutionControl.create_batch_compute_session_state(
                    ComputeSessionStateType.PROCESSING, session_desc
                )

        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.update_active_routes_status()

        assert on_compute_failure_hook.verify(app)
        assert on_failure_hook.verify(app)

        assert not on_compute_retry_hook.verify(app)

        self.patch_aws_stop()

    def test_all_application_hooks_generic(self):
        self._test_all_application_hooks(lambda: GenericRoutingHookImpl())

    def test_all_application_hooks_with_EMAIL(self):
        email_obj = EMAIL(
            sender="if-test-list@amazon.com",
            recipient_list=["yunusko@amazon.com"],
            body="first input path URI: ${input0}, output path: ${output}, region: ${region_id}, date: ${ship_day}",
            subject="first input path URI: ${input0}, output path: ${output}, region: ${region_id}, date: ${ship_day}",
        )

        action = email_obj.action()
        assert action.describe_slot()["sender"]
        self._test_all_application_hooks(lambda: GenericComputeDescriptorHookVerifier(action))

    def test_all_application_hooks_with_slack(self):
        secret_arn = "arn:aws:secretsmanager:us-east-1:123456789012:secret:secret-name-abcd"
        slack_obj = Slack(
            recipient_list=["https://hooks.slack.com/workflows/1/", secret_arn],
            message="test message. first input path URI: ${input0}, output path: ${output}, region: ${region_id}, date: ${ship_day}",
        )
        slack_action = slack_obj.action()
        assert slack_action.describe_slot()["message"]

        def _create_url_from_aws_secret(recipient: str, **params):
            return ("https://hooks.slack.com/workflows/2/",)

        slack_action._create_url = _create_url_from_aws_secret
        self._test_all_application_hooks(lambda: GenericComputeDescriptorHookVerifier(slack_action))

        assert len(slack_action.permissions) == 2
        assert slack_action.permissions[0].action[0] == slack_action.AWS_GET_SECRET_VALUE_ACTION
        assert slack_action.permissions[1].action[0] == "kms:Encrypt"
        assert slack_action.permissions[1].resource[0].startswith("arn:aws:kms")

    def test_application_hooks_generate_right_permissions(self):
        """Test system provided compute targets' compatibility and runtime permission contribution as hooks"""
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        self.app = AWSApplication(app_name="sys-hooks", region=self.region)

        email_obj = EMAIL(sender="if-test-list@amazon.com", recipient_list=["yunusko@amazon.com"])

        self.app.create_data(
            id="dummy_node_EMAIL_as_pending_trigger_hook",
            compute_targets=[NOOPCompute],
            pending_node_hook=RoutePendingNodeHook(on_pending_node_created=email_obj.action()),
        )

        # Test permissions applied to runtime / exec role as well
        # keep reference of actual policy updater method so that we can retore it at the end.
        real_put_inlined_policy = development_module.put_inlined_policy

        def put_inlined_policy(
            role_name: str, policy_name: str, action_resource_pairs: Set[ConstructPermission], base_session: "boto3.Session"
        ) -> None:
            if "IntelliFlowExeRole" in role_name:
                # check EMAIL resource in runtime permission resources (SES ARN, etc)
                assert any([email_obj.sender in resource for perm in action_resource_pairs for resource in perm.resource])

        development_module.put_inlined_policy = MagicMock(side_effect=put_inlined_policy)

        # above mock / callback should be called during the activation
        self.app.activate()
        # just make sure that it was called actually (otherwise there is no point in this test :)
        assert development_module.put_inlined_policy.call_count > 0

        # restore
        development_module.put_inlined_policy = real_put_inlined_policy

        self.patch_aws_stop()

        # Test permissions applied to runtime / exec role as well

    def test_application_hooks_serialization_error(self):
        """Map the underlying serialization error to a better exception (ValueError)"""
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        self.app = AWSApplication(app_name="hook-ser-err", region=self.region)

        class A:
            def __init__(self):
                import boto3

                # will cause SerializationError
                self._session = boto3.client("s3")

            def __call__(self, *args, **kwargs):
                pass

        # will cause SerializationError
        A.__name__ = "C"

        with pytest.raises((ValueError, TypeError)):
            self.app.create_data(
                id="dummy_node_SIM_as_exec_hook",
                compute_targets=[NOOPCompute],
                execution_hook=RouteExecutionHook(on_exec_begin=A()),
            )

        self.patch_aws_stop()

    def test_application_retry_hook(self):
        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("retry-hook")

        ducsi_data = app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]

        on_failure_hook = GenericRoutingHookImpl()
        on_compute_retry_hook = GenericRoutingHookImpl()
        on_failure_hook2 = GenericRoutingHookImpl()
        on_compute_retry_hook2 = GenericRoutingHookImpl()

        app.create_data(
            id="REPEAT_DUCSI",
            inputs={
                "DEXML_DUCSI": ducsi_data,
            },
            compute_targets=[GlueBatchCompute(code="output=DEXML_DUCSI.limit(100)", retry_count=1)],
            execution_hook=RouteExecutionHook(on_compute_retry=on_compute_retry_hook, on_failure=on_failure_hook),
        )

        app.create_data(
            id="REPEAT_DUCSI2",
            inputs={
                "DEXML_DUCSI": ducsi_data,
            },
            compute_targets=[GlueBatchCompute(code="output=DEXML_DUCSI.limit(100)", retry_count=0)],
            execution_hook=RouteExecutionHook(on_compute_retry=on_compute_retry_hook2, on_failure=on_failure_hook2),
        )

        app.activate()

        # 1- Inject DUCSI event to trigger execution on the nodes/routes
        # mock batch_compute response
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            # both of the nodes will have a new compute session
            return TestAWSApplicationExecutionControl.create_batch_compute_response(
                ComputeSuccessfulResponseType.PROCESSING, f"job_id-{materialized_output.alias}"
            )

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return TestAWSApplicationExecutionControl.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.process(
            ducsi_data[1]["2020-12-25"],
            # make it SYNC (use the local processor instance in sync mode)
            with_activated_processor=False,
        )

        assert not on_compute_retry_hook.verify(app)
        assert not on_compute_retry_hook2.verify(app)
        assert not on_failure_hook.verify(app)
        assert not on_failure_hook2.verify(app)

        # now make sure that during the periodical check both of the nodes fails in a transient way.
        # this causes implicit retries and yields brand new sessions, however this should not count towards retry limit.
        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return TestAWSApplicationExecutionControl.create_batch_compute_failed_session_state(
                ComputeFailedSessionStateType.TRANSIENT, session_desc
            )

        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        # emulate runtime Processor behaviour, to check the routes otherwise checkpoints won't be checked.
        # reason: in unit-tests the Processor does not 'run' in the background. So the following call is somewhat like
        # a 'next cycle/tick',
        app.update_active_routes_status()

        assert not on_compute_retry_hook.verify(app)
        assert not on_compute_retry_hook2.verify(app)
        assert not on_failure_hook.verify(app)
        assert not on_failure_hook2.verify(app)

        # now emulate a job failure:
        # only the node with retry > 0 should be retried
        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return TestAWSApplicationExecutionControl.create_batch_compute_failed_session_state(
                ComputeFailedSessionStateType.APP_INTERNAL, session_desc
            )

        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.update_active_routes_status()

        # retried!
        assert on_compute_retry_hook.verify(app)
        assert not on_failure_hook.verify(app)
        # will never be retried since retry_count is 0.
        # this should actually be failed and terminated.
        assert not on_compute_retry_hook2.verify(app)
        assert on_failure_hook2.verify(app)

        # now during the second check max_retry_count of 1 must be hit and the compute must fail.
        app.update_active_routes_status()
        assert on_failure_hook.verify(app)

        self.patch_aws_stop()

    def test_application_metadata_action_failure(self):
        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        app = self._create_test_application("meta-hooks")

        ducsi_data = app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]

        exec_metadata_actions = [
            RouteMetadataAction(
                condition=lambda output_metadata: output_metadata[DatasetMetadata.RECORD_COUNT.value] < 1,
                slot=FailedMetadataAction("Failed due to zero count in region ${region_id} on date ${ship_day}"),
            ),
        ]

        # test BatchCompute path
        repeat_ducsi = app.create_data(
            id="REPEAT_DUCSI",
            inputs={
                "DEXML_DUCSI": ducsi_data,
            },
            compute_targets=[GlueBatchCompute(code="output=DEXML_DUCSI.limit(100)")],
            execution_hook=RouteExecutionHook(metadata_actions=exec_metadata_actions),
        )

        def metadata_provider_compute(input_map, output, params):
            params["runtime_platform"].storage.save_internal_metadata(output, {DatasetMetadata.RECORD_COUNT.value: 0})

        # test InlinedCompute path
        app.create_data(
            id="REPEAT_DUCSI2",
            inputs={
                "DEXML_DUCSI": ducsi_data,
            },
            compute_targets=[InlinedCompute(metadata_provider_compute)],
            execution_hook=RouteExecutionHook(metadata_actions=exec_metadata_actions),
        )

        app.activate()

        # 1- Inject DUCSI event to trigger execution on the nodes/routes
        # mock batch_compute response
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            # both of the nodes will have a new compute session
            return TestAWSApplicationExecutionControl.create_batch_compute_response(
                ComputeSuccessfulResponseType.PROCESSING, f"job_id-{materialized_output.alias}"
            )

        # let the Batch Compute complete
        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return TestAWSApplicationExecutionControl.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        # artificially get the output metadata ready for the batch compute
        app.platform.storage.save_internal_metadata(repeat_ducsi[1]["2020-12-25"].get_signal(), {DatasetMetadata.RECORD_COUNT.value: 0})

        # trigger the executions by sending in the common input
        app.process(
            ducsi_data[1]["2020-12-25"],
            # make it SYNC (use the local processor instance in sync mode)
            with_activated_processor=False,
        )

        # show that both must have failed
        path, _ = app.poll(app["REPEAT_DUCSI"][1]["2020-12-25"])
        assert not path
        path, _ = app.poll(app["REPEAT_DUCSI2"][1]["2020-12-25"])
        assert not path

        self.patch_aws_stop()
