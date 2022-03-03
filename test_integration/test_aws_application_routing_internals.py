# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time
from datetime import datetime, timedelta

import pytest

from intelliflow.api_ext import *
from intelliflow.core.signal_processing.routing_runtime_constructs import RuntimeLinkNode
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.integ_test import AWSIntegTestMixin
from intelliflow.utils.test.inlined_compute import InlinedComputeRetryVerifier, NOOPCompute


class TestAWSApplicationRoutingInternals(AWSIntegTestMixin):
    def setup(self):
        super().setup("IntelliFlow")

    def test_application_route_pending_node_management(self):
        app_name = "test-routing"
        app = super()._create_test_application(app_name, reset_if_exists=True)

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
        app = super()._create_test_application(app_name, reset_if_exists=False)
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
        for i in range(9):
            app.process(dummy_input_signal1[datetime(2021, 7, 20) + timedelta(i)])

        route_record = app.get_active_route(node1)
        assert len(route_record.route.pending_nodes) == 10

        # start initiating actual triggers by injecting input2 on each pending node.
        # this will consume pending nodes 1 by 1.
        for i in range(10):
            app.process(dummy_input_signal2[datetime(2021, 7, 19) + timedelta(i)])
            assert len(app.get_active_route(node1).route.pending_nodes) == int(9 - i)

        route_record = app.get_active_route(node1)
        assert not route_record.route.pending_nodes
        assert not route_record.persisted_pending_node_ids

        # Finally do a similar large scale test but this time use the large number of PendingNodes
        #  to test the batch deletion during the deletion of Route.
        # Use a number greater than 25 (don't assume that internal driver will rely on <table>.batch_writer)
        for i in range(26):
            app.process(dummy_input_signal1[datetime(2021, 7, 20) + timedelta(i)])

        # reload the app, start a new DEV session
        app = super()._create_test_application(app_name, reset_if_exists=False)
        # activate the empty dev state (context).
        # This will cause existing route to be wiped out!
        app.activate()

        # if the Route is gone, its aggregations such as PendingNode (and soon Active ComputeRecords)
        # must have been wiped out too.
        assert not app.get_active_routes()

        app.terminate()
        app.delete()

    def test_application_route_pending_node_management_remote(self):
        """Force the utilization of remote orchestration to make sure that everything (permissions, compatibility) are
        well setup by underlying drivers."""
        app_name = "test-remote"
        app = super()._create_test_application(app_name, reset_if_exists=True)

        dummy_input_signal1 = app.marshal_external_data(
            S3Dataset("111222333444", "bucket", "dataset1", "{}"), "test_ext_data", {"day": {"type": Type.DATETIME, "format": "%Y-%m-%d"}}
        )

        dummy_input_signal2 = app.marshal_external_data(
            S3Dataset("111222333444", "bucket", "dataset2", "{}"), "test_ext_data2", {"day": {"type": Type.DATETIME, "format": "%Y-%m-%d"}}
        )

        node1 = app.create_data("test_node1", inputs=[dummy_input_signal1, dummy_input_signal2], compute_targets=[NOOPCompute])

        app.activate()

        # inject input1 to create a pending node, using remote orchestration in cloud
        app.process(dummy_input_signal1["2021-07-19"], with_activated_processor=True, is_async=False)

        route_record: RoutingTable.RouteRecord = app.get_active_route(node1)
        assert len(route_record.route.pending_nodes) == 1
        assert len(route_record.persisted_pending_node_ids) == 1
        assert route_record.persisted_pending_node_ids == {list(route_record.route.pending_nodes)[0].node_id}
        assert not route_record.active_compute_records
        assert not route_record.persisted_active_compute_record_ids

        # inject input2 to consume the pending node by triggering an execution over
        #  the link of: input1['2021-07-19']  ~ input2['2021-07-19']
        app.process(
            dummy_input_signal2["2021-07-19"],
            # use remote Processor (orchestration), will end up using EXEC role
            with_activated_processor=True,
        )

        # since the req was sent asynchronously, we have to poll the state change
        route_record = app.get_active_route(node1)
        wait_time = 0
        while route_record.route.pending_nodes:
            time.sleep(5)
            wait_time = wait_time + 5
            assert wait_time < 120, (
                "An execution must have spawned for 2021-07-19 by remote orchestration but looks "
                "like its PendingNode still exists. It might be a problem in delete operation too "
                "if execution is detected."
            )
            route_record = app.get_active_route(node1)
        assert not route_record.persisted_pending_node_ids
        # NOOPCompute inlined compute must have completed in the same cycle
        assert not route_record.active_compute_records
        assert not route_record.persisted_active_compute_record_ids

        # Test pending nodes cleanup when the route is deleted implicitly.
        #  1- Create two pending nodes
        #  2- then remove the route from the app, leave the app empty and reactivate
        #  this diff will be detected as an integrity change and IF will wipe out the route.
        #  3- check there is no active route.
        #  4- then add the same route (with the same name and parameters) back to the app and activate.
        #  5- verify that previous pending nodes are not mistakenly attached to the same route again.
        #  This will prove that IF wipes out Pending Nodes along with the owner route (at step [2] above).

        # 1- now leave the system with two pending nodes
        app.process(dummy_input_signal1["2021-07-19"], with_activated_processor=True, is_async=False)
        app.process(dummy_input_signal1["2021-07-20"], with_activated_processor=True, is_async=False)

        # 2- reload the app, start a new DEV session
        app = super()._create_test_application(app_name, reset_if_exists=False)
        # activate the empty dev state (context).
        # This will cause existing route to be wiped out!
        app.activate()

        # 3- check there is no active route.
        assert not app.get_active_routes()

        # 4- then add the same route (same parameters) back to the app and activate.
        node1 = app.create_data("test_node1", inputs=[dummy_input_signal1, dummy_input_signal2], compute_targets=[NOOPCompute])

        app.activate()

        # 5.1- there should be no active routes
        assert not app.get_active_routes()

        # 5.2- create an active route record and let it have a pending node
        app.process(dummy_input_signal1["2021-07-19"], with_activated_processor=True)

        # 5.3- Verify that pending nodes from step (1) above are not here.
        wait_time = 0
        route_record = app.get_active_route(node1)
        while not route_record or not route_record.route.pending_nodes:
            time.sleep(5)
            wait_time = wait_time + 5
            assert wait_time < 120, "A pending node must have created for 2021-07-19 by remote orchestration!"
            route_record = app.get_active_route(node1)

        assert len(route_record.route.pending_nodes) == 1
        assert len(route_record.persisted_pending_node_ids) == 1
        pending_node: RuntimeLinkNode = list(route_record.route.pending_nodes)[0]
        assert route_record.persisted_pending_node_ids == {pending_node.node_id}

        # make sure that the pending node is from (5.2) actually, meaning that
        # it is pending on '2021-07-19' not on previously wiped out pending nodes.
        material_output_for_pending_node: Signal = pending_node.materialize_output(
            route_record.route.output, route_record.route.output_dim_matrix, force=True
        )  # force (attempt) since the node is not ready yet,
        # but it should succeed since there is one common dim.
        assert DimensionFilter.check_equivalence(
            material_output_for_pending_node.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({"2021-07-19": {}})
        )

        # now large-scale continous increase in pending nodes and then consumption
        for i in range(9):
            app.process(dummy_input_signal1[datetime(2021, 7, 20) + timedelta(days=i)], with_activated_processor=True)
            time.sleep(1)

        route_record = app.get_active_route(node1)
        while len(route_record.route.pending_nodes) < 10:
            time.sleep(5)
            wait_time = wait_time + 5
            assert wait_time < 120, "A pending node count must have reached 10 by now in remote orchestration!"
            route_record = app.get_active_route(node1)

        assert len(route_record.route.pending_nodes) == 10

        # start initiating actual triggers by injecting input2 on each pending node.
        # this will consume pending nodes 1 by 1.
        for i in range(10):
            app.process(dummy_input_signal2[datetime(2021, 7, 19) + timedelta(days=i)], with_activated_processor=True)
            time.sleep(1)

        route_record = app.get_active_route(node1)
        while route_record.route.pending_nodes:
            time.sleep(5)
            wait_time = wait_time + 5
            assert wait_time < 120, "No pending node was not expected in remote orchestration!"
            route_record = app.get_active_route(node1)
        assert not route_record.route.pending_nodes
        assert not route_record.persisted_pending_node_ids

        assert not app.get_active_routes()

        app.terminate()
        app.delete()

    def test_application_route_pending_node_management_during_propagation_remote(self):
        """Force the utilization of remote orchestration to make sure that everything (permissions, compatibility) are
        well setup by underlying drivers when event propagation is in place between two nodes."""
        app_name = "test-remote2"
        app = super()._create_test_application(app_name, reset_if_exists=True)

        dummy_input_signal1 = app.marshal_external_data(
            S3Dataset("111222333444", "bucket", "dataset1", "{}"),
            "test_ext_data",
            {"day": {"type": Type.DATETIME, "format": "%Y-%m-%d-%H"}},
        )  # notice the extra hour component

        dummy_input_signal2 = app.marshal_external_data(
            S3Dataset("111222333444", "bucket", "dataset2", "{}"),
            "test_ext_data2",
            {"day": {"type": Type.DATETIME, "format": "%Y-%m-%d-%H"}},
        )

        node1 = app.create_data(
            "test_node1",
            inputs=[dummy_input_signal1, dummy_input_signal2],
            compute_targets=[NOOPCompute],
            output_dimension_spec={"day": {type: Type.DATETIME, "format": "%Y-%m-%d", "hour": {type: Type.DATETIME, "format": "%H"}}},
            output_dim_links=[
                # first_input('day') -> output('day')  [AUTOMATICALLY LINKED]
                # first_input('day') -> output('hour')
                ("hour", EQUALS, dummy_input_signal1("day"))
            ],
        )

        # will be used to stress orchestration so that it will defer the final (successful) retry to the next cycle.
        # we will end up checking the remote persistence (state management) performance across multiple cycles.
        inlined_compute_that_retries = InlinedComputeRetryVerifier(retry_count=1, storage_bucket=app.platform.storage._bucket_name)
        node2 = app.create_data("test_node2", inputs=[node1["*"]["23"]], compute_targets=[InlinedCompute(inlined_compute_that_retries)])

        app.activate()

        # 1- emulate async, remote event ingestion on node1 for an entire day (24 hours)
        for i in range(24):
            # a pair of these events (linked on the same 'day' dimension) should cause an execution on node1
            app.process(dummy_input_signal1[datetime(2022, 1, 1, i)], with_activated_processor=True, is_async=False)
            time.sleep(1)
            app.process(dummy_input_signal2[datetime(2022, 1, 1, i)], with_activated_processor=True)
            self.poll(app, node1["2022-01-01"][i], expect_failure=False, duration=600)

        # 2- wait for steady-state in the system when node2 execution is done too
        self.poll(app, node2["2022-01-01"]["23"], expect_failure=False, duration=600)
        # also verify that multi-cycle execution has succeeded (during the retry)
        assert inlined_compute_that_retries.verify(app)

        # now check the orchestration state
        route_record1 = app.get_active_route(node1)
        route_record2 = app.get_active_route(node2)

        assert not route_record1.route.pending_nodes
        assert not route_record1.active_execution_context_state
        assert not route_record1.active_compute_records
        assert not route_record2.route.pending_nodes
        assert not route_record1.active_execution_context_state
        assert not route_record1.active_compute_records

        app.terminate()
        app.delete()

    # TODO create variants of above tests for different computes (e.g BatchCompute alternatives)
