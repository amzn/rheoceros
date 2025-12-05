# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import random
import string
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from intelliflow.api_ext import *
from intelliflow.core.platform.definitions.compute import (
    ComputeResourceDesc,
    ComputeResponse,
    ComputeSessionDesc,
    ComputeSessionState,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.signal_processing import Slot
from intelliflow.core.signal_processing.routing_runtime_constructs import RuntimeLinkNode
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.inlined_compute import NOOPCompute


def compute_processing_response(
    route: Route,
    materialized_inputs: List[Signal],
    slot: Slot,
    materialized_output: Signal,
    execution_ctx_id: str,
    retry_session_desc: Optional[ComputeSessionDesc] = None,
) -> ComputeResponse:
    return ComputeSuccessfulResponse(
        ComputeSuccessfulResponseType.PROCESSING,
        ComputeSessionDesc(f"job_{materialized_output.get_materialized_resource_paths()[0]}", ComputeResourceDesc("job_name", "job_arn")),
    )


def compute_completed_response(
    route: Route,
    materialized_inputs: List[Signal],
    slot: Slot,
    materialized_output: Signal,
    execution_ctx_id: str,
    retry_session_desc: Optional[ComputeSessionDesc] = None,
) -> ComputeResponse:
    return ComputeSuccessfulResponse(
        ComputeSuccessfulResponseType.COMPLETED,
        ComputeSessionDesc(f"job_{materialized_output.get_materialized_resource_paths()[0]}", ComputeResourceDesc("job_name", "job_arn")),
    )


def get_completed_session_state(
    session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
) -> ComputeSessionState:
    return ComputeSessionState(
        (
            ComputeSessionDesc(
                f"job_{active_compute_record.materialized_output.get_materialized_resource_paths()[0]}",
                ComputeResourceDesc("job_name", "job_arn"),
            )
            if not session_desc
            else session_desc
        ),
        # COMPLETED !!!
        ComputeSessionStateType.COMPLETED,
        [ComputeExecutionDetails("<start_time>", "<end_time>", dict())],
    )


def get_processing_session_state(
    session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
) -> ComputeSessionState:
    return ComputeSessionState(
        (
            ComputeSessionDesc(
                f"job_{active_compute_record.materialized_output.get_materialized_resource_paths()[0]}",
                ComputeResourceDesc("job_name", "job_arn"),
            )
            if not session_desc
            else session_desc
        ),
        ComputeSessionStateType.PROCESSING,
        [ComputeExecutionDetails("<start_time>", "<end_time>", dict())],
    )


class TestAWSApplicationRoutingInternals(AWSTestBase):
    def test_application_route_pending_node_management(self):
        self.patch_aws_start()

        app_name = "test-routing"
        app = AWSApplication(app_name, self.region)

        dummy_input_signal1 = app.add_timer("timer1", "rate(1 day)")

        dummy_input_signal2 = app.add_timer("timer2", "rate(1 day)")

        node1 = app.create_data("test_node1", inputs=[dummy_input_signal1, dummy_input_signal2], compute_targets=[NOOPCompute])

        app.activate()

        # inject input1 to create a pending node
        app.process(dummy_input_signal1["2021-07-19"])

        route_record: RoutingTable.RouteRecord = app.get_active_route(node1)
        assert len(route_record.route.pending_nodes) == 1
        assert len(route_record.persisted_pending_node_ids) == 1
        assert route_record.persisted_pending_node_ids == {list(route_record.route.pending_nodes)[0].node_id}

        # inject input2 to consume the pending node by triggering an execution over
        #  the link of: input1['2021-07-19']  ~ input2['2021-07-19']
        app.process(dummy_input_signal2["2021-07-19"])

        route_record = app.get_active_route(node1)
        assert not route_record.route.pending_nodes
        assert not route_record.persisted_pending_node_ids

        # Test pending nodes cleanup when the route is deleted implicitly.
        #  1- Create two pending nodes
        #  2- then remove the route from the app, leave the app empty and reactivate
        #  this diff will be detected as an integrity change and IF will wipe out the route.
        #  3- check there is no active route.
        #  4- then add the same route (with the same name and parameters) back to the app and activate.
        #  5- verify that previous pending nodes are not mistakenly attached to the same route again.
        #  This will prove that IF wipes out Pending Nodes along with the owner route (at step [2] above).

        # 1- now leave the system with two pending nodes
        app.process(dummy_input_signal1["2021-07-19"])
        app.process(dummy_input_signal1["2021-07-20"])

        # 2- reload the app, start a new DEV session
        app = AWSApplication(app_name, self.region)
        # activate the empty dev state (context).
        # This will cause existing route to be wiped out!
        app.activate()

        # 3- check there is no active route.
        assert not app.get_active_routes()

        # 4- then add the same route (with the same name and parameters) back to the app and activate.
        node1 = app.create_data("test_node1", inputs=[dummy_input_signal1, dummy_input_signal2], compute_targets=[NOOPCompute])

        app.activate()

        # 5.1- there should be no active routes
        assert not app.get_active_routes()

        # 5.2- create an active route record and let it have a pending node
        app.process(dummy_input_signal1["2021-07-19"])

        # 5.3- Verify that pending nodes from step (1) above are not here.
        route_record = app.get_active_route(node1)
        assert len(route_record.route.pending_nodes) == 1
        assert len(route_record.persisted_pending_node_ids) == 1
        pending_node: RuntimeLinkNode = list(route_record.route.pending_nodes)[0]
        assert route_record.persisted_pending_node_ids == {pending_node.node_id}

        # paranoidally make sure that the pending node is from (5.2) actually, meaning that
        # it is pending on '2021-07-19' not on previously wiped out pending nodes.
        material_output_for_pending_node: Signal = pending_node.materialize_output(
            route_record.route.output, route_record.route.output_dim_matrix, force=True
        )  # force (attempt) since the node is not ready yet,
        # but it should succeed since there is one common dim.
        assert DimensionFilter.check_equivalence(
            material_output_for_pending_node.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({"2021-07-19": {}})
        )

        # now large-scale continous increase in pending nodes and then consumption
        for i in range(99):
            app.process(dummy_input_signal1[datetime(2021, 7, 20) + timedelta(i)])

        route_record = app.get_active_route(node1)
        assert len(route_record.route.pending_nodes) == 100

        # start initiating actual triggers by injecting input2 on each pending node.
        # this will consume pending nodes 1 by 1.
        for i in range(100):
            app.process(dummy_input_signal2[datetime(2021, 7, 19) + timedelta(i)])
            assert len(app.get_active_route(node1).route.pending_nodes) == int(99 - i)

        route_record = app.get_active_route(node1)
        assert not route_record.route.pending_nodes
        assert not route_record.persisted_pending_node_ids

        # Finally do a similar large scale test but this time use the large number of PendingNodes
        #  to test the batch deletion during the deletion of Route.
        # Use a number greater than 25 (don't assume that internal driver will rely on <table>.batch_writer)
        for i in range(29):
            app.process(dummy_input_signal1[datetime(2021, 7, 20) + timedelta(i)])

        # reload the app, start a new DEV session
        app = AWSApplication(app_name, self.region)
        # activate the empty dev state (context).
        # This will cause existing route to be wiped out!
        app.activate()

        # if the Route is gone, its aggregations such as PendingNode (and soon Active ComputeRecords)
        # must have been wiped out too (using a batch deletion probably).
        assert not app.get_active_routes()

        self.patch_aws_stop()

    def test_application_route_active_compute_record_management(self):
        """Large scale test with the following scenario:

        - use local orchestration that will run in a background thread (closest flow to actual runs on cloud)
            - throught the rest of the test whenever user level APIs are used, avoid concurrency with the orchestration
        - Simple DAG: (timer1, timer2) -> node1 -> node_sink
            - add extra dummy metadata to timer signals to stress out persistence
        - feed node1 with 80 x timer1 -> 80 pending nodes (due to missing timer2) events
        - feed node1 with 40 x timer2 -> 40 pending nodes + 40 newly activated compute records (ACRs in PROCESSING state)
        - before proceeding, due a stress test and reload the application and force an activation to check the
        integrity of Routing persistence: check number of pending nodes and ACRs.
        - feed node1 with the rest of timer2s (+40) -> 80 ACRs (all in PROCESSING state)
        - Patch BatchCompute in a way that orchestration queries for ACRs will return COMPLETED
        - Wait for orchestration to do the rest. Complete all node1s and then auto-trigger and complete all sink_nodes
            - Orchestration must auto-trigger sink_node partitions upon the completion of node1 partitions
            - Then wait for orchestration to scan all 80 ACRs on sink_node, detect completion and move them into history as IACRs
        - Finally go over two nodes and verify that their pending node and ACR acounts are zero, but IACR counts are
        80 each.
        """
        self.patch_aws_start()

        experiment_partition_count = 80  # WARNING use an even number as the test relies on division by 2
        app_name = "test-ACRs"
        app = AWSApplication(app_name, self.region)

        dummy_input_signal1 = app.add_timer(
            "timer1",
            "rate(1 day)",
            dummy_metadata_to_increase_signal_size="".join(random.choice(string.ascii_letters) for _ in range(1000)),
            dummy_metadata_to_increase_signal_size2="".join(random.choice(string.digits) for _ in range(1000)),
        )

        dummy_input_signal2 = app.add_timer("timer2", "rate(1 day)")

        node1 = app.create_data("test_node1", inputs=[dummy_input_signal1, dummy_input_signal2], compute_targets=[Glue("SPARK CODE")])

        node_sink = app.create_data("sink_node", inputs=[node1], compute_targets=[NOOPCompute])

        app.activate()

        # enable local orchestration (node1 -> node_sink automatic push)
        orchestration_cycle_time_in_secs = 10
        self.activate_event_propagation(app, cycle_time_in_secs=orchestration_cycle_time_in_secs)

        # patch BatchCompute for persistent ACR on node1 (otherwise ACR will be consumed immediately)
        app.platform.batch_compute.compute = MagicMock(side_effect=compute_processing_response)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_processing_session_state)

        # create pending nodes for 60 days
        for i in range(experiment_partition_count):
            app.process(dummy_input_signal1[datetime(2022, 1, 1) + timedelta(days=i)])

        route_record: RoutingTable.RouteRecord = app.get_active_route(node1)
        assert len(route_record.route.pending_nodes) == experiment_partition_count
        assert len(route_record.persisted_pending_node_ids) == experiment_partition_count
        assert not route_record.persisted_active_compute_record_ids
        # at this point, node_sink must not have been triggered yet. should not even have an active record
        assert not app.get_active_route(node_sink)

        # trigger the first half of partitions by injecting the second input for them
        for i in range(int(experiment_partition_count / 2)):
            app.process(dummy_input_signal2[datetime(2022, 1, 1) + timedelta(days=i)])

        time.sleep(orchestration_cycle_time_in_secs)
        # 1/2 -> pending, 1/2 -> active compute
        route_record = app.get_active_route(node1)
        assert len(route_record.route.pending_nodes) == experiment_partition_count / 2
        assert len(route_record.persisted_pending_node_ids) == experiment_partition_count / 2
        # check ACRs
        assert len(route_record.active_compute_records) == experiment_partition_count / 2
        assert len(route_record.persisted_active_compute_record_ids) == experiment_partition_count / 2

        # BEGIN collaborative load / activation to prove that current route state won't get disrupted.
        app2 = AWSApplication(app_name, self.region)
        app2.attach()
        app2.activate()
        route_record = app2.get_active_route(node1)
        assert len(route_record.route.pending_nodes) == experiment_partition_count / 2
        # check ACRs
        assert len(route_record.active_compute_records) == experiment_partition_count / 2
        # END

        # now trigger the rest and see everything has moved from being a PendingNode into an ACR
        for i in range(int(experiment_partition_count / 2)):
            app.process(dummy_input_signal2[datetime(2022, 1, 1) + timedelta(int(experiment_partition_count / 2)) + timedelta(days=i)])

        time.sleep(orchestration_cycle_time_in_secs)
        # at this point we should have active computes on all partitions
        route_record = app.get_active_route(node1)
        assert not route_record.route.pending_nodes
        assert not route_record.persisted_pending_node_ids
        # check ACRs
        assert len(route_record.active_compute_records) == experiment_partition_count
        assert len(route_record.persisted_active_compute_record_ids) == experiment_partition_count
        # but sink node must still be inactive since there has been no completion on the first node yet
        assert not app.get_active_route(node_sink)

        # All SUCCESS (patch BatchCompute to return COMPLETED for all ACRs to RoutingTable query in the next cycle of orchestation)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_completed_session_state)

        # when an ACR is done, it is moved out as a historical/inactive CR
        # we will use the following routine in the rest of the test for checking all of node_sink activations are moved/
        # done and also later done as a sanity-check even on the first node as well.
        def _get_inactive_compute_record_count(app, node) -> int:
            inactive_count = 0
            inactive_cr_it = app.get_inactive_compute_records(node)
            try:
                while next(inactive_cr_it):
                    inactive_count = inactive_count + 1
            except StopIteration:
                pass
            return inactive_count

        # wait for orchestration to sweep all by waiting for all of the node_sink activations to move to inactive.
        while _get_inactive_compute_record_count(app, node_sink) < experiment_partition_count:
            time.sleep(1)

        assert not app.has_active_routes()

        # final check on both nodes, they should both have no pending nodes or ACRs but valid partitions generated for
        # the entire exp range.
        for node in (node1, node_sink):
            route_record = app.get_active_route(node)
            assert not route_record.route.pending_nodes
            assert not route_record.persisted_pending_node_ids
            assert not route_record.active_compute_records
            assert not route_record.persisted_active_compute_record_ids
            # check if everything pushed to ACR history (inactive records)
            assert _get_inactive_compute_record_count(app, node) == experiment_partition_count

            # and finally check platform can detect newly created partitions on the node
            for i in range(int(experiment_partition_count)):
                path, _ = app.poll(node[datetime(2022, 1, 1) + timedelta(days=i)])
                assert path

        assert not app.get_active_routes()

        self.patch_aws_stop()

    def test_application_route_inactive_compute_record_search(self):
        """
        Test searching the compute record history table by different parameters.

        First creating two nodes, one with completed session state result, one with failed session state result.
        Then query on the table using parameters including trigger_range, deactivated_range, session_state, or slot_type.
        """
        self.patch_aws_start()

        app_name = "test-history"
        app = AWSApplication(app_name, self.region)
        test_time = datetime.now()

        dummy_input_signal1 = app.add_timer("timer1", "rate(1 day)")
        dummy_input_signal2 = app.add_timer("timer2", "rate(1 day)")

        node1 = app.create_data("test_node1", inputs=[dummy_input_signal1], compute_targets=[Glue("SPARK CODE")])

        node2 = app.create_data("test_node2", inputs=[dummy_input_signal2], compute_targets=[Glue("SPARK CODE")])

        app.activate()

        # inject input1 to create a completed compute record
        app.platform.batch_compute.compute = MagicMock(side_effect=compute_completed_response)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_completed_session_state)

        app.process(dummy_input_signal1["2022-08-01"])

        while len(list(app.get_inactive_compute_records(node1))) < 1:
            time.sleep(1)

        # Check record is saved to history table
        inactive_records1 = list(app.get_inactive_compute_records(node1))
        assert len(inactive_records1) == 1

        # Inject input2 create another failed compute record
        app.platform.batch_compute.compute = MagicMock(side_effect=Exception("Test"))
        app.process(dummy_input_signal2["2022-08-01"])

        while len(list(app.get_inactive_compute_records(node2))) < 1:
            time.sleep(1)

        # Check record is saved to history table
        inactive_records2 = list(app.get_inactive_compute_records(node2))
        assert len(inactive_records2) == 1

        test_time_int = int(test_time.timestamp())
        test_time_plus_1_day = int((test_time + timedelta(days=1)).timestamp())
        test_time_plus_2_day = int((test_time + timedelta(days=2)).timestamp())

        # Node 1 return a completed record
        assert len(list(app.get_inactive_compute_records(node1, session_state=ComputeSessionStateType.COMPLETED))) == 1
        # Node 2 does not have any completed record
        assert len(list(app.get_inactive_compute_records(node2, session_state=ComputeSessionStateType.COMPLETED))) == 0

        # Return result when trigger range is correct
        assert len(list(app.get_inactive_compute_records(node1, trigger_range=(test_time_int, test_time_plus_1_day)))) == 1

        # Return no result when trigger range is incorrect
        assert len(list(app.get_inactive_compute_records(node1, trigger_range=(test_time_plus_1_day, test_time_plus_2_day)))) == 0

        # Return result when deactivated_range is correct
        assert len(list(app.get_inactive_compute_records(node1, deactivated_range=(test_time_int, test_time_plus_1_day)))) == 1

        # Return no result when deactivated_range is incorrect
        assert len(list(app.get_inactive_compute_records(node1, deactivated_range=(test_time_plus_1_day, test_time_plus_2_day)))) == 0

        # Return result for slot type ASYNC_BATCH_COMPUTE
        assert len(list(app.get_inactive_compute_records(node1, slot_type=SlotType.ASYNC_BATCH_COMPUTE))) == 1

        # Return no result for slot type SYNC_INLINED
        assert len(list(app.get_inactive_compute_records(node1, slot_type=SlotType.SYNC_INLINED))) == 0

        # Test using all parameters
        assert (
            len(
                list(
                    app.get_inactive_compute_records(
                        node1,
                        session_state=ComputeSessionStateType.COMPLETED,
                        trigger_range=(test_time_int, test_time_plus_1_day),
                        deactivated_range=(test_time_int, test_time_plus_1_day),
                        slot_type=SlotType.ASYNC_BATCH_COMPUTE,
                        ascending=True,
                    ),
                )
            )
            == 1
        )

        assert (
            len(
                list(
                    app.get_inactive_compute_records(
                        node2,
                        session_state=ComputeSessionStateType.COMPLETED,
                        trigger_range=(test_time_int, test_time_plus_1_day),
                        deactivated_range=(test_time_int, test_time_plus_1_day),
                        slot_type=SlotType.ASYNC_BATCH_COMPUTE,
                        ascending=True,
                    ),
                )
            )
            == 0
        )

        self.patch_aws_stop()

    def test_application_route_internals_during_activation(self):
        self.patch_aws_start()

        app_name = "test-route-act"
        app = AWSApplication(app_name, self.region)

        dummy_input_signal1 = app.add_timer("timer1", "rate(1 day)")

        dummy_input_signal2 = app.add_timer("timer2", "rate(1 day)")

        node1 = app.create_data("test_node1", inputs=[dummy_input_signal1, dummy_input_signal2], compute_targets=[NOOPCompute])

        node2 = app.create_data("test_node2", inputs=[dummy_input_signal1, node1], compute_targets=[NOOPCompute])

        app.activate()

        # inject input1 to create a pending node
        app.process(dummy_input_signal1["2023-08-22"], target_route_id=node1)

        route_record: RoutingTable.RouteRecord = app.get_active_route(node1)
        assert len(route_record.route.pending_nodes) == 1

        app.process(dummy_input_signal1["2023-08-23"], target_route_id=node2)
        app.process(node1["2023-08-23"], is_blocked=True)
        route_record: RoutingTable.RouteRecord = app.get_active_route(node2)
        assert len(route_record.route.pending_nodes) == 1

        # now change the execution related parameters of the nodes and test the pending execution state as part of
        # changeset management during activation
        app.attach()

        # this should unblock this node
        node1 = app.patch_data(node1, inputs=[dummy_input_signal1])

        # this would normally unblock but node1 is in "blocked" state so still must stay as pending
        app.patch_data(node2, inputs=[dummy_input_signal1, node1], output_dimension_spec={}, output_dim_links=[])

        # this must keep the pending executions state and because of the changes in inputs:
        # - unblock the execution on the first node because its second input that was blocking the execution is removed
        # - retain the pending execution on the second node because its remaining input is still marked as "blocked".
        app.activate()

        # let node1 execute. pending node -> active compute transition is not allowed during activation and in this test
        # orchestration cycle is not on so this will provide the needed "next cycle" here
        app.update_active_routes_status()

        route_record: RoutingTable.RouteRecord = app.get_active_route(node1)
        assert len(route_record.route.pending_nodes) == 0

        route_record = app.get_active_route(node2)
        assert len(route_record.route.pending_nodes) == 1

        # unblock the execution
        app.process(node1["2023-08-23"], target_route_id=node2, is_blocked=False)

        # it must have executed
        route_record = app.get_active_route(node2)
        assert len(route_record.route.pending_nodes) == 0

        self.patch_aws_stop()
