import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

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
from intelliflow.core.signal_processing.signal_source import InternalDatasetSignalSourceAccessSpec
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.data_emulation import add_test_data
from intelliflow.utils.test.hook import GenericRoutingHookImpl
from intelliflow.utils.test.inlined_compute import FailureCompute, NOOPCompute


class TestAWSApplicationOutputRetention(AWSTestBase):
    """
    This test module wants to capture all code-paths in retention engine (including auto-refresh mechanism)
    """

    def test_application_output_retention(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        app = AWSApplication("retention-1", self.region)

        timer = app.add_timer("timer", "rate(1 day)", time_dimension_format="%Y-%m-%d %H:%M:%S")

        retention_in_secs = 20
        rip_hook = GenericRoutingHookImpl()

        def retention_condition(dims: Dict[str, Any]) -> bool:
            from datetime import datetime, timedelta

            return (datetime.now() - timedelta(seconds=retention_in_secs)) < dims["time"]

        test_node = app.create_data(
            "test_node",
            inputs=[timer],
            compute_targets=[NOOPCompute],
            output_retention=RouteRetention(condition=retention_condition, rip_hook=rip_hook),
        )

        rip_hook2 = GenericRoutingHookImpl()
        test_node2 = app.create_data(
            "test_node2",
            inputs=[timer],
            compute_targets=[NOOPCompute],
            output_retention=RouteRetention(condition=lambda dims: True, rip_hook=rip_hook2),
        )

        app.activate()

        self.activate_retention_engine(app, cycle_time_in_secs=5)
        self.activate_event_propagation(app, cycle_time_in_secs=10)

        partition_time = datetime.now()
        output = test_node[partition_time]
        assert app.execute(output, wait=False)
        output2 = test_node2[partition_time]
        assert app.execute(output2, wait=True)

        assert not rip_hook.verify(app)
        assert not rip_hook2.verify(app)
        output_signal = output.get_signal()
        assert app.platform.storage.load_internal_metadata(output_signal) is None
        assert app.platform.storage.load_internal_metadata(output_signal, InternalDatasetSignalSourceAccessSpec.EXECUTION_METADATA_FILE)
        # emulate data creation
        add_test_data(app, output, "_SUCCESS", "")
        assert len([x for x in app.platform.storage.load_internal(output_signal)]) > 0

        time.sleep(30)
        assert rip_hook.verify(app)
        assert (
            app.platform.storage.load_internal_metadata(output_signal, InternalDatasetSignalSourceAccessSpec.EXECUTION_METADATA_FILE)
            is None
        )

        assert not [x for x in app.platform.storage.load_internal(output_signal)]

        # finally prove that retention engine will reject executions on old partitions
        with pytest.raises(RuntimeError):
            assert app.execute(test_node[datetime.now() - timedelta(seconds=retention_in_secs + 1)], wait=True)

        # should raise in async mode as well
        with pytest.raises(RuntimeError):
            assert app.execute(test_node[datetime.now() - timedelta(seconds=retention_in_secs + 1)], wait=False)

        # second node should not be impacted by any retention action
        output_signal2 = output2.get_signal()
        assert not rip_hook2.verify(app)
        assert app.platform.storage.load_internal_metadata(output_signal2, InternalDatasetSignalSourceAccessSpec.EXECUTION_METADATA_FILE)
        # should succeed on the second node because its retention condition always returns True
        assert app.execute(test_node2[datetime.now() - timedelta(seconds=retention_in_secs + 1)], wait=True)

        self.patch_aws_stop()

    def test_application_output_retention_with_refresh(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        app = AWSApplication("retention-2", self.region)

        timer = app.add_timer("timer", "rate(1 day)", time_dimension_format="%Y-%m-%d %H:%M:%S")

        retention_in_secs = 30
        rip_hook = GenericRoutingHookImpl()

        def retention_condition(dims: Dict[str, Any]) -> bool:
            from datetime import datetime, timedelta

            return (datetime.now() - timedelta(seconds=retention_in_secs)) < dims["time"]

        refresh_hook = GenericRoutingHookImpl()  # will never be called (refresh not enabled)
        test_node = app.create_data(
            "test_node",
            inputs=[timer],
            compute_targets=[Glue("CODE")],  # use BatchCompute
            output_retention=RouteRetention(condition=retention_condition, rip_hook=rip_hook, refresh_hook=refresh_hook),
        )

        rip_hook_child1 = GenericRoutingHookImpl()
        refresh_hook_child1 = GenericRoutingHookImpl()
        test_node_child1 = app.create_data(
            "test_node_child1",
            inputs=[test_node],
            compute_targets=[Glue("CODE")],  # use BatchCompute
            output_retention=RouteRetention(
                condition=retention_condition, refresh_period_in_secs=10, rip_hook=rip_hook_child1, refresh_hook=refresh_hook_child1
            ),
        )

        # child 2 -> refresh only
        rip_hook_child2 = GenericRoutingHookImpl()
        refresh_hook_child2 = GenericRoutingHookImpl()
        test_node_child2 = app.create_data(
            "test_node_child2",
            inputs=[test_node],
            compute_targets=[NOOPCompute],  # use BatchCompute
            output_retention=RouteRetention(refresh_period_in_secs=10, rip_hook=rip_hook_child2, refresh_hook=refresh_hook_child2),
        )

        # will never get refreshed because refresh period is too long
        rip_hook2 = GenericRoutingHookImpl()
        refresh_hook2 = GenericRoutingHookImpl()
        test_node2 = app.create_data(
            "test_node2",
            inputs=[timer],
            compute_targets=[NOOPCompute],
            output_retention=RouteRetention(
                condition=lambda dims: True, refresh_period_in_secs=10000, rip_hook=rip_hook2, refresh_hook=refresh_hook2
            ),
        )

        app.activate()

        # mock batch_compute for instant success on batch jobs
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

            return TestAWSApplicationExecutionControl.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_1")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

            return TestAWSApplicationExecutionControl.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        self.activate_retention_engine(app, cycle_time_in_secs=6)
        self.activate_event_propagation(app, cycle_time_in_secs=12)

        partition_time = datetime.now()
        output = test_node[partition_time]
        assert app.execute(output, wait=True)
        output2 = test_node2[partition_time]
        assert app.execute(output2, wait=True)

        assert not rip_hook.verify(app)
        assert not rip_hook_child1.verify(app)
        assert not rip_hook_child2.verify(app)
        assert not rip_hook2.verify(app)
        assert not refresh_hook.verify(app)
        assert not refresh_hook_child1.verify(app)
        assert not refresh_hook_child2.verify(app)
        assert not refresh_hook2.verify(app)
        output_signal = output.get_signal()
        assert app.platform.storage.load_internal_metadata(output_signal) is None
        assert app.platform.storage.load_internal_metadata(output_signal, InternalDatasetSignalSourceAccessSpec.EXECUTION_METADATA_FILE)
        # emulate data creation
        add_test_data(app, output, "_SUCCESS", "")
        assert len([x for x in app.platform.storage.load_internal(output_signal)]) > 0

        time.sleep(20)
        assert not rip_hook.verify(app)
        assert not rip_hook_child1.verify(app)
        assert not rip_hook_child2.verify(app)
        assert not rip_hook2.verify(app)
        # check childs have been executed and put exec metadata already (two times already)
        assert app.platform.storage.load_internal_metadata(
            test_node_child1[partition_time].get_signal(), InternalDatasetSignalSourceAccessSpec.EXECUTION_METADATA_FILE
        )
        assert app.platform.storage.load_internal_metadata(
            test_node_child2[partition_time].get_signal(), InternalDatasetSignalSourceAccessSpec.EXECUTION_METADATA_FILE
        )
        assert app.poll(test_node_child1[partition_time])[0]
        assert app.poll(test_node_child2[partition_time])[0]
        assert not refresh_hook.verify(app)
        assert refresh_hook_child1.verify(app)
        assert refresh_hook_child2.verify(app)
        assert not refresh_hook2.verify(app)  # will never get refreshed

        add_test_data(app, test_node_child1[partition_time], "_SUCCESS", "")

        time.sleep(30)
        assert rip_hook.verify(app)
        assert (
            app.platform.storage.load_internal_metadata(output_signal, InternalDatasetSignalSourceAccessSpec.EXECUTION_METADATA_FILE)
            is None
        )

        assert rip_hook_child1.verify(app)
        assert (
            app.platform.storage.load_internal_metadata(
                test_node_child1[partition_time].get_signal(), InternalDatasetSignalSourceAccessSpec.EXECUTION_METADATA_FILE
            )
            is None
        )

        # second child should still be retained
        assert not rip_hook_child2.verify(app)
        assert app.platform.storage.load_internal_metadata(
            test_node_child2[partition_time].get_signal(), InternalDatasetSignalSourceAccessSpec.EXECUTION_METADATA_FILE
        )

        assert not [x for x in app.platform.storage.load_internal(output_signal)]
        assert not [x for x in app.platform.storage.load_internal(test_node_child1[partition_time].get_signal())]

        # finally prove that retention engine will reject executions on old partitions
        with pytest.raises(RuntimeError):
            assert app.execute(test_node[datetime.now() - timedelta(seconds=retention_in_secs + 1)], wait=True)

        # should raise in async mode as well
        with pytest.raises(RuntimeError):
            assert app.execute(test_node[datetime.now() - timedelta(seconds=retention_in_secs + 1)], wait=False)

        # child with retention should rejected as well
        with pytest.raises(RuntimeError):
            assert app.execute(test_node_child1[datetime.now() - timedelta(seconds=retention_in_secs + 1)], wait=False)

        # second node should not be impacted by any retention action
        output_signal2 = output2.get_signal()
        assert not rip_hook2.verify(app)
        assert app.platform.storage.load_internal_metadata(output_signal2, InternalDatasetSignalSourceAccessSpec.EXECUTION_METADATA_FILE)
        # should succeed on the second node because its retention condition always returns True
        assert app.execute(test_node2[datetime.now() - timedelta(seconds=retention_in_secs + 1)], wait=True)

        self.patch_aws_stop()

    def test_application_output_retention_refresh_dependency_check(self):
        """By tracking the trigger times of refresh hooks make sure that retention engine orchestrates refreshes
        based on the dependency tree by not prematurely refreshing the children while their parent's refresh is overdue"""
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        class RefreshTrackerHook:
            def __call__(self, routing_table: "RoutingTable", *args, **kwargs) -> None:
                assert kwargs.get("dimensions", None) is not None, "Output 'dimensions' map not in hook params"
                assert kwargs.get("dimensions_map", None) is not None, "Output 'dimensions_map' map not in hook params"
                output = kwargs.get("_materialized_output")
                metadata = routing_table.get_platform().storage.load_internal_metadata(output)
                if not metadata:
                    metadata = dict()
                metadata.setdefault("refresh_timestamps", []).append(datetime.now().timestamp())
                routing_table.get_platform().storage.save_internal_metadata(output, metadata)

                time.sleep(1)

        app = AWSApplication("retention-3", self.region)

        timer = app.add_timer("timer", "rate(1 day)", time_dimension_format="%Y-%m-%d %H:%M:%S")

        refresh_hook = RefreshTrackerHook()  # will never be called (refresh not enabled)
        test_node = app.create_data(
            "test_node",
            inputs=[timer],
            compute_targets=[Glue("CODE")],  # use BatchCompute
            output_retention=RouteRetention(refresh_period_in_secs=20, refresh_hook=refresh_hook),
        )

        refresh_hook_child1 = RefreshTrackerHook()
        test_node_child1 = app.create_data(
            "test_node_child1",
            inputs=[test_node],
            compute_targets=[Glue("CODE")],  # use BatchCompute
            output_retention=RouteRetention(refresh_period_in_secs=10, refresh_hook=refresh_hook_child1),
        )

        # child 2
        refresh_hook_child2 = RefreshTrackerHook()
        test_node_child2 = app.create_data(
            "test_node_child2",
            inputs=[test_node],
            compute_targets=[NOOPCompute],  # use BatchCompute
            output_retention=RouteRetention(refresh_period_in_secs=10, refresh_hook=refresh_hook_child2),
        )

        app.activate()

        # mock batch_compute for instant success on batch jobs
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

            return TestAWSApplicationExecutionControl.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_1")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

            return TestAWSApplicationExecutionControl.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        self.activate_retention_engine(app, cycle_time_in_secs=5)
        self.activate_event_propagation(app, cycle_time_in_secs=10)

        partition_time = datetime.now()
        output = test_node[partition_time]
        assert app.execute(output, wait=True)

        time.sleep(5)
        test_node_metadata = app.platform.storage.load_internal_metadata(output.get_signal())
        while not test_node_metadata or not test_node_metadata.get("refresh_timestamps", None):
            test_node_metadata = app.platform.storage.load_internal_metadata(output.get_signal())
            time.sleep(5)
        child1_metadata = app.platform.storage.load_internal_metadata(test_node_child1[partition_time].get_signal())
        while not child1_metadata or not child1_metadata.get("refresh_timestamps", None):
            child1_metadata = app.platform.storage.load_internal_metadata(test_node_child1[partition_time].get_signal())
            time.sleep(5)
        child2_metadata = app.platform.storage.load_internal_metadata(test_node_child2[partition_time].get_signal())
        while (
            not child2_metadata or not child2_metadata.get("refresh_timestamps", None) or len(child2_metadata.get("refresh_timestamps")) < 2
        ):
            child2_metadata = app.platform.storage.load_internal_metadata(test_node_child2[partition_time].get_signal())
            time.sleep(5)

        # in each refresh cycle parent must be refreshed first
        test_node_metadata = app.platform.storage.load_internal_metadata(output.get_signal())
        test_node_triggers = test_node_metadata["refresh_timestamps"]
        child1_metadata = app.platform.storage.load_internal_metadata(test_node_child1[partition_time].get_signal())
        child1_triggers = child1_metadata["refresh_timestamps"]
        child2_triggers = child2_metadata["refresh_timestamps"]
        # first input refreshes should not be blocked by parent
        assert child1_triggers[0] < test_node_triggers[0]
        assert child2_triggers[0] < test_node_triggers[0]

        # but the subsequent ones should wait for the parent
        for i in range(1, len(test_node_triggers)):
            assert child1_triggers[i] > test_node_triggers[0]
            assert child2_triggers[i] > test_node_triggers[0]

        self.patch_aws_stop()
