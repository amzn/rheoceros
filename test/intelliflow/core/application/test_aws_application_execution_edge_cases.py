# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time

from mock import MagicMock

from intelliflow.api_ext import *
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


class TestAWSApplicationExecutionEdgeCases(AWSTestBase):
    def test_application_batch_compute_transient_error(self):
        """
        This test wants to capture the orchestration behaviour when BatchCompute driver returns TRANSIENT_ERROR
        and IF is supposed to retry implicitly.


        Test is designed to mock BatchCompute interface in such a way that;
           - Execution completes with one retry
           - Each compute call returns a different JOB ID (session id)
           - get_session_state call returns TRANSIENT_ERROR for the first job_id and COMPLETED for the second.
           - BathCompute::can_retry is patched to eliminate any complex retry deferral logic to make local testing
           feasible with reasonal test execution times. can_retry unconditional True means that transient errors are
           guaranteed to be handled in the next cycle.
        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("implicit_retry", self.region)

        test_node = app.create_data("test_node", compute_targets="NOT IMPORTANT")

        app.activate()

        call_count: int = 0

        def compute(
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            nonlocal call_count
            job_id = None
            if call_count == 0:
                job_id = "job_1"
            elif call_count == 1:
                # second compute request from the Processor during the 'retry' attempt
                job_id = "job_2"
            else:
                raise RuntimeError(f"call_count [{call_count}] should not exceed 1 in this test!")

            call_count = call_count + 1

            return ComputeSuccessfulResponse(
                ComputeSuccessfulResponseType.PROCESSING, ComputeSessionDesc(job_id, ComputeResourceDesc("job_name", "job_arn"))
            )

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            if session_desc.session_id == "job_1":
                return ComputeFailedSessionState(
                    ComputeFailedSessionStateType.TRANSIENT,
                    session_desc,
                    [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))],
                )
            elif session_desc.session_id == "job_2":
                return ComputeSessionState(
                    session_desc,
                    ComputeSessionStateType.COMPLETED,
                    [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))],
                )

        def can_retry(active_compute_record: "RoutingTable.ComputeRecord") -> bool:
            """Overwrite default retry strategy, othwerwise retry might be deferred."""
            return True

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        for driver in app.platform.batch_compute._drivers:
            driver.can_retry = MagicMock(side_effect=can_retry)

        app.execute(test_node, wait=False)

        # in unit-tests Processor is not running, emulate Processor next-cycle
        app.update_active_routes_status()  # within this call orchestration will get the TRANSIENT ERROR and call compute again
        # emulate second cycle
        app.update_active_routes_status()  # in this cycle, BatchCompute::get_session_state will return COMPLETED

        # check successful execution
        path, _ = app.poll(test_node)
        assert path

        self.patch_aws_stop()

    def test_application_batch_compute_transient_error_with_default_retry_strategy(self):
        """
        With a test flow almost identical to previous test above, this test aims to cover retry strategy against
        TRANSIENT errors.

        Test is designed to mock BatchCompute interface in such a way that;
           - Execution completes with one retry
           - Each compute call returns a different JOB ID (session id)
           - get_session_state call returns TRANSIENT_ERROR for the first job_id and COMPLETED for the second.
           - batch_compute.can_retry is not patched (keep the retry strategy alive, either BatchCompute::can_retry or
           the can_retry impl of the winner BatchCompute driver in this scenario). The goal is to cover the generic
           orchestration code path that calls 'can_retry'
           - BatchCompute::get_max_wait_time_for_next_retry_in_secs is patched with a small duration to allow sane test
           execution times with the default retry logic (if BatchCompute::can_retry is used)

        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("implicit_retry", self.region)

        test_node = app.create_data("test_node", compute_targets="NOT IMPORTANT")

        app.activate()

        call_count: int = 0

        def compute(
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            nonlocal call_count
            job_id = None
            if call_count == 0:
                job_id = "job_1"
            elif call_count == 1:
                # second compute request from the Processor during the 'retry' attempt
                job_id = "job_2"
            else:
                raise RuntimeError(f"call_count [{call_count}] should not exceed 1 in this test!")

            call_count = call_count + 1

            return ComputeSuccessfulResponse(
                ComputeSuccessfulResponseType.PROCESSING, ComputeSessionDesc(job_id, ComputeResourceDesc("job_name", "job_arn"))
            )

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            if session_desc.session_id == "job_1":
                return ComputeFailedSessionState(
                    ComputeFailedSessionStateType.TRANSIENT,
                    session_desc,
                    [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))],
                )
            elif session_desc.session_id == "job_2":
                return ComputeSessionState(
                    session_desc,
                    ComputeSessionStateType.COMPLETED,
                    [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))],
                )

        def get_max_wait_time_for_next_retry_in_secs() -> int:
            return 60

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        for driver in app.platform.batch_compute._drivers:
            driver.get_max_wait_time_for_next_retry_in_secs = MagicMock(side_effect=get_max_wait_time_for_next_retry_in_secs)

        app.execute(test_node, wait=False)

        orchestration_cycle_time_in_secs = 5
        self.activate_event_propagation(app, cycle_time_in_secs=orchestration_cycle_time_in_secs)

        # as the elapse time gets close to get_max_wait_time_for_next_retry_in_secs (60 secs)
        # then the likelihood of BathCompte::can_retry yielding the next call to compute increases.
        # so this poll should definitely exit within 2 mins with a probability of ~ 0.99
        path, _ = app.poll(test_node)
        assert path, "Execution must have been successful!"

        self.patch_aws_stop()
