# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time
from unittest.mock import MagicMock

from intelliflow.api_ext import *
from intelliflow.core.platform.definitions.compute import (
    ComputeFailedResponse,
    ComputeFailedResponseType,
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
            route: Route,
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
            route: Route,
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

    def test_application_can_kill_records_with_transient_error(self):
        """
        This test wants to capture the orchestration behaviour when BatchCompute driver returns TRANSIENT_ERROR
        and the user wants to force-kill it.

        This is a corner case because normally kill logic is quite trivial and offloaded to BC driver where a normal
        record would be stopped and then its status would be later on picked up by orchestration as error.

        But when the error is TRANSIENT, then BC driver cannot help here as the compute tech might see the run as FAILED
        already and cannot stop anything. TRANSIENT retry is our own feature and therefore kill request on records
         with that failure type should be handled by orchestration.
        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("kill_transient", self.region)

        test_node = app.create_data("test_node", compute_targets="NOT IMPORTANT")

        app.activate()

        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return ComputeSuccessfulResponse(
                ComputeSuccessfulResponseType.PROCESSING, ComputeSessionDesc("job_1", ComputeResourceDesc("job_name", "job_arn"))
            )

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return ComputeFailedSessionState(
                ComputeFailedSessionStateType.TRANSIENT,
                session_desc,
                [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))],
            )

        def can_retry(active_compute_record: "RoutingTable.ComputeRecord") -> bool:
            """Overwrite default retry strategy, emulate deferred TRANSIENT error so that kill on it can be tested."""
            return False

        def kill_session(active_compute_record: "RoutingTable.ComputeRecord") -> bool:
            """Overwrite default retry strategy, othwerwise retry might be deferred."""
            raise RuntimeError("Job cannot be stopped in this state!")

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.platform.batch_compute.kill_session = MagicMock(side_effect=kill_session)
        for driver in app.platform.batch_compute._drivers:
            driver.can_retry = MagicMock(side_effect=can_retry)

        app.execute(test_node, wait=False)

        # in unit-tests Processor is not running, emulate Processor next-cycle
        app.update_active_routes_status()  # within this call orchestration will get the TRANSIENT ERROR and call compute again
        # check active records
        assert app.has_active_record(test_node)

        # emulate second cycle
        app.update_active_routes_status()  # in this cycle, again transient
        # still looks as ACTIVE as TRANSIENTs will be retried
        assert app.has_active_record(test_node)

        assert app.kill(test_node)  # implicitly marked as FORCE_STOPPED or TRANSIENT_FORCE_STOPPED

        # before orchestration's next cycle it should still look as active
        assert app.has_active_record(test_node)

        app.update_active_routes_status()  # in this cycle, orchestration detect that it was forcefully stopped

        assert not app.has_active_record(test_node)

        # check unsuccessful execution
        path, _ = app.poll(test_node)
        assert not path

        self.patch_aws_stop()

    def test_application_can_kill_records_with_transient_error_in_initial_response_state(self):
        """
        (Extension to previous test `test_application_can_kill_records_with_transient_error`.)

        This test wants to capture the orchestration behaviour when BatchCompute driver returns TRANSIENT_ERROR
        in compute call directly without creating a compute session (e.g cluster, job in cloud).
        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("kill_transient2", self.region)

        test_node = app.create_data("test_node", compute_targets="NOT IMPORTANT")

        app.activate()

        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return ComputeFailedResponse(
                ComputeFailedResponseType.TRANSIENT,
                ComputeResourceDesc("job_name", "job_arn"),
                "error_code",
                "Error",
            )

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            raise RuntimeError("THIS CALL SHOULD NOT BE POSSIBLE")

        def can_retry(active_compute_record: "RoutingTable.ComputeRecord") -> bool:
            """Overwrite default retry strategy, emulate deferred TRANSIENT error so that kill on it can be tested."""
            return False

        def kill_session(active_compute_record: "RoutingTable.ComputeRecord") -> bool:
            """This should not be possible because here we are testing the records that were rejected by the driver
            so they should not have a session (response_state -> FAILED). So kill logic should attempt to move them
            from TRANSIENT to TRANSIENT_FORCE_STOPPED directly without taking any other action."""
            raise RuntimeError("THIS CALL SHOULD NOT BE POSSIBLE")

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.platform.batch_compute.kill_session = MagicMock(side_effect=kill_session)
        for driver in app.platform.batch_compute._drivers:
            driver.can_retry = MagicMock(side_effect=can_retry)

        app.execute(test_node, wait=False)

        # in unit-tests Processor is not running, emulate Processor next-cycle
        app.update_active_routes_status()  # within this call orchestration will get the TRANSIENT ERROR and call compute again
        # check active records
        assert app.has_active_record(test_node)

        # emulate second cycle
        app.update_active_routes_status()  # in this cycle, again transient
        # still looks as ACTIVE as TRANSIENTs will be retried
        assert app.has_active_record(test_node)

        assert app.kill(test_node)  # implicitly marked as FORCE_STOPPED or TRANSIENT_FORCE_STOPPED

        # before orchestration's next cycle it should still look as active
        assert app.has_active_record(test_node)

        app.update_active_routes_status()  # in this cycle, orchestration detect that it was forcefully stopped

        assert not app.has_active_record(test_node)

        # check unsuccessful execution
        path, _ = app.poll(test_node)
        assert not path

        self.patch_aws_stop()
