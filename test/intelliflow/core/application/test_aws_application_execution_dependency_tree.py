import concurrent.futures
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
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.data_emulation import add_test_data, check_test_data
from intelliflow.utils.test.inlined_compute import FailureCompute, NOOPCompute


class TestAWSApplicationExecutionDependencyTree(AWSTestBase):
    """
    This test module wants to capture the advanced utilization of Application::execute with different combinations of
    `update_dependency_tree` and `recursive`.
    """

    def test_application_execute_dtree_async_single_node(self):
        """
        Single node, capture edge-cases and show no difference in behaviour as compared to other modes of operation.
        """
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        app = AWSApplication("execute-dtree-1", self.region)

        test_node = app.create_data("test_node", compute_targets=[NOOPCompute])

        app.execute(test_node, wait=False, recursive=True, update_dependency_tree=True)

        # emulate Processor next-cycle
        app.update_active_routes_status()

        # check successful execution
        path, _ = app.poll(test_node)
        assert path

        # try again with recursive False
        app.execute(test_node, wait=False, recursive=False, update_dependency_tree=True)
        app.update_active_routes_status()

        # check successful execution
        path, _ = app.poll(test_node)
        assert path

        # CLEANUP
        add_test_data(app, test_node, "_SUCCESS", "")
        assert check_test_data(app, test_node, "_SUCCESS"), "Could not create artificial completion file!"
        app.execute(test_node, wait=True, recursive=False, update_dependency_tree=True, executor_mode=Application.ExecutorMode.CLEANUP)
        assert not check_test_data(app, test_node, "_SUCCESS"), "Could not clean up node!"
        #

        self.patch_aws_stop()

    def test_application_execute_dtree_sync_single_node(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        app = AWSApplication("execute-dtree-2", self.region)

        test_node = app.create_data("test_node", compute_targets=[NOOPCompute])

        app.execute(test_node, wait=True, recursive=True, update_dependency_tree=True)

        # emulate Processor next-cycle
        app.update_active_routes_status()

        # check successful execution
        path, _ = app.poll(test_node)
        assert path

        # CLEANUP with recursive=True path
        add_test_data(app, test_node, "_SUCCESS", "")
        assert check_test_data(app, test_node, "_SUCCESS"), "Could not create artificial completion file!"
        app.execute(test_node, wait=True, recursive=True, update_dependency_tree=True, executor_mode=Application.ExecutorMode.CLEANUP)
        assert not check_test_data(app, test_node, "_SUCCESS"), "Could not clean up node!"
        #

        self.patch_aws_stop()

    def test_application_execute_dtree_single_branch_two_nodes(self):
        """Prove that `update_dependency_tree=True` will not cause any redundant executions in this example where, even
        without this flag, orchestration would execute B eventually upon completion of A.

        Also, we have to make sure that `A` won't be re-executed due to `recursive=True` is being active on downstream
        node `B`.
        """
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)
        app = AWSApplication("execute-dtree-3", self.region)

        a = app.create_data("A", compute_targets=[NOOPCompute])

        b = app.create_data("B", inputs=[a], compute_targets=[NOOPCompute])

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=3)

        app.execute(a, wait=True, recursive=True, update_dependency_tree=True)

        path, _ = app.poll(a)
        assert path, "Execution must have been successful!"

        time.sleep(2 * 3)  # wait at least two orchestration cycles (downstream tree update is always async)
        path, _ = app.poll(b)
        assert path, "Execution must have been successful!"

        # only 1 execution on "A"
        inactive_records = app.get_inactive_compute_records(a)
        assert len(list(inactive_records)) == 1

        # only 1 execution on "B"
        inactive_records = app.get_inactive_compute_records(b)
        assert len(list(inactive_records)) == 1

        # now do it asynchronously (assume that business logic of A has changed and the branch needs to be updated)
        app.execute(a, wait=False, recursive=True, update_dependency_tree=True)

        path, _ = app.poll(a)
        assert path, "Execution must have been successful!"

        time.sleep(2 * 3)
        path, _ = app.poll(b)

        # total of 2 executions on "A" now
        inactive_records = app.get_inactive_compute_records(a)
        assert len(list(inactive_records)) == 2

        # total of 2 executions on "B" now
        inactive_records = app.get_inactive_compute_records(b)
        assert len(list(inactive_records)) == 2

        assert path, "Execution must have been successful!"

        # CLEANUP #1 one level only (not checking dep tree)
        add_test_data(app, a, "_SUCCESS", "")
        add_test_data(app, b, "_SUCCESS", "")
        assert check_test_data(app, a, "_SUCCESS"), "Could not create artificial completion file!"
        assert check_test_data(app, b, "_SUCCESS"), "Could not create artificial completion file!"
        app.execute(a, wait=True, recursive=True, update_dependency_tree=False, executor_mode=Application.ExecutorMode.CLEANUP)
        assert not check_test_data(app, a, "_SUCCESS"), "Could not clean up node 'a'!"
        assert check_test_data(app, b, "_SUCCESS"), "Should have left node 'b' untouched!"
        #
        # CLEANUP #1.1 top-down (this time should visit 'b' as well and clean it up)
        app.execute(a, wait=True, recursive=True, update_dependency_tree=True, executor_mode=Application.ExecutorMode.CLEANUP)
        assert not check_test_data(app, a, "_SUCCESS"), "Could not clean up node 'a'!"
        assert not check_test_data(app, b, "_SUCCESS"), "Could not clean up node 'b'!"

        # CLEANUP #2 Bottom-up
        add_test_data(app, a, "_SUCCESS", "")
        add_test_data(app, b, "_SUCCESS", "")
        # note that entry point is 'b' (child) this time
        app.execute(b, wait=True, recursive=True, update_dependency_tree=True, executor_mode=Application.ExecutorMode.CLEANUP)
        assert not check_test_data(app, a, "_SUCCESS"), "Could not clean up node 'a'!"
        assert not check_test_data(app, b, "_SUCCESS"), "Could not clean up node 'b'!"

        # CLEANUP #2.1 one level only (child cleans itself up only with recursive=False)
        add_test_data(app, a, "_SUCCESS", "")
        add_test_data(app, b, "_SUCCESS", "")
        # note that entry point is 'b' (child) this time
        app.execute(b, wait=True, recursive=False, update_dependency_tree=True, executor_mode=Application.ExecutorMode.CLEANUP)
        assert check_test_data(app, a, "_SUCCESS"), "Should have left node 'a' untouched!"
        assert not check_test_data(app, b, "_SUCCESS"), "Could not clean up node 'b'!"
        # show that update_dependency_tree is ineffective since entry point is the the leaf node, we still expect 'a'
        # to stay untouched.
        app.execute(b, wait=True, recursive=False, update_dependency_tree=False, executor_mode=Application.ExecutorMode.CLEANUP)
        assert check_test_data(app, a, "_SUCCESS"), "Should have left node 'a' untouched!"
        assert not check_test_data(app, b, "_SUCCESS"), "Could not clean up node 'b'!"
        # now clean up with recursive=True
        app.execute(b, wait=True, recursive=True, update_dependency_tree=False, executor_mode=Application.ExecutorMode.CLEANUP)
        assert not check_test_data(app, a, "_SUCCESS"), "Could not clean up node 'a'!"
        assert not check_test_data(app, b, "_SUCCESS"), "Could not clean up node 'b'!"

        self.patch_aws_stop()

    def test_application_execute_dtree_single_child(self):
        """Test a simple case of one internal node and an external node (two parents) feeding a
        single child node. We will highlight the impact of `recursive` on child node's update as part of this test."""
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)
        app = AWSApplication("execute-dtree-4", self.region)

        ext1 = app.marshal_external_data(
            S3Dataset("111222333444", "bucket-execute-dtree-4", "data", "{}", "{}"),
            "ext_data_1",
            {"region": {"type": DimensionType.LONG, "day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        ext2 = app.marshal_external_data(
            S3Dataset("111222333444", "bucket-execute-dtree-4", "data2", "{}", "{}"),
            "ext_data_2",
            {"region": {"type": DimensionType.LONG, "day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        a = app.create_data("A", inputs=[ext1], compute_targets=[NOOPCompute])

        b = app.create_data("B", inputs=[ext2, a], compute_targets=[NOOPCompute])

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=3)

        # emulate partition readiness
        # - ext1 ready but ext2 not!
        add_test_data(app, ext1[1]["2023-11-20"], object_name="_SUCCESS", data="")

        with pytest.raises(ValueError):
            # external data is not ready 'recursive' mode should detect and raise when it is checking node "B"
            app.execute(a[1]["2023-11-20"], wait=False, recursive=True, update_dependency_tree=True)

        # recursion on "B" should detect ext2 partition
        add_test_data(app, ext2[1]["2023-11-20"], object_name="_SUCCESS", data="")
        # and it should go through now
        app.execute(a[1]["2023-11-20"], wait=False, recursive=True, update_dependency_tree=True)

        time.sleep(2 * 3)
        # now check the entire branch
        path, _ = app.poll(a[1]["2023-11-20"])
        assert path, "Execution must have been successful!"

        path, _ = app.poll(b[1]["2023-11-20"])
        assert path, "Execution must have been successful!"

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        # CLEANUP #1 check external data is not touched even when recursive=True
        assert check_test_data(app, ext1[1]["2023-11-20"], "_SUCCESS"), "Could not create artificial completion file!"
        assert check_test_data(app, ext2[1]["2023-11-20"], "_SUCCESS"), "Could not create artificial completion file!"
        app.execute(
            a[1]["2023-11-20"], wait=True, recursive=True, update_dependency_tree=False, executor_mode=Application.ExecutorMode.CLEANUP
        )
        assert check_test_data(app, ext1[1]["2023-11-20"], "_SUCCESS"), "Should have left external data node 'ext1' untouched!"
        assert check_test_data(app, ext2[1]["2023-11-20"], "_SUCCESS"), "Should have left external data node 'ext2' untouched!"

        # CLEAN #1.1 show 'a' having data impacts nothing
        add_test_data(app, a[1]["2023-11-20"], "_SUCCESS", "")
        assert check_test_data(app, a[1]["2023-11-20"], "_SUCCESS"), "Could not create artificial completion file!"
        app.execute(
            a[1]["2023-11-20"], wait=True, recursive=True, update_dependency_tree=False, executor_mode=Application.ExecutorMode.CLEANUP
        )
        assert check_test_data(app, ext1[1]["2023-11-20"], "_SUCCESS"), "Should have left external data node 'ext1' untouched!"
        assert check_test_data(app, ext2[1]["2023-11-20"], "_SUCCESS"), "Should have left external data node 'ext2' untouched!"
        assert not check_test_data(app, a[1]["2023-11-20"], "_SUCCESS"), "Could not clean up node 'a'!"

        self.patch_aws_stop()

    def test_application_execute_dtree_two_childs(self):
        """Extend the hierarchy from the previous test ("single child") and add a 3rd layer node "C" that will consume
        "B" and also "A". But it will need a range of "A" (multiple partitions).

        So it covers this corner-case:

        - The tip of the backfilling node "A" will have requests from "C" on its historical partitions as part of the
        nested "dependency tree" update process (to satisfy its dependency on a weekly range of "A"). We need to show
        that this recursive process (that cycles back to the originator node "A") will not interfere with the ongoing
        execution (with "update_dependency_tree") request on the original partition and just ignores the executions on
        the other partitions of "A". Those partitions should exist before the tree update. So recursive execution
        doest not attempt to backfill upstream stream nodes that are as part of the dependency tree. This logic is in
        `Application::_execute_recursive` and it just checks the "node id" rather than "node + dimension values" to skip
        recursive execution on an ancestor node.
        """
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)
        app = AWSApplication("execute-dtree-5", self.region)

        ext1 = app.marshal_external_data(
            S3Dataset("111222333444", "bucket-execute-dtree-5", "data", "{}", "{}"),
            "ext_data_1",
            {"region": {"type": DimensionType.LONG, "day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        ext2 = app.marshal_external_data(
            S3Dataset("111222333444", "bucket-execute-dtree-5", "data2", "{}", "{}"),
            "ext_data_2",
            {"region": {"type": DimensionType.LONG, "day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        a = app.create_data("A", inputs=[ext1], compute_targets=[NOOPCompute])

        b = app.create_data("B", inputs=[ext2, a], compute_targets=[NOOPCompute])
        # C: relies on 7 days of "A" and "B"
        c = app.create_data("C", inputs=[b, a[1][:-7].ref.range_check(True)], compute_targets=[NOOPCompute])

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=3)

        # emulate partition readiness
        for i in range(7):
            add_test_data(app, ext1[1][f"2023-11-{14 + i}"], object_name="_SUCCESS", data="")

        add_test_data(app, ext2[1][f"2023-11-20"], object_name="_SUCCESS", data="")

        # This should happen (path of recursion in Application::execute):
        # A[2023-11-20] -> B[2023-11-20] -> C[2023-11-20] ->
        #                                               A[2023-11-20] (DONOT EXECUTE: mark it as BLOCKED)
        #                                               A[2023-11-19] (SKIP even if non-existent, parent is in tree)
        #                                               A[2023-11-18] (SKIP even if non-existent, parent is in tree)
        #                                               A[2023-11-17] (SKIP even if non-existent, parent is in tree)
        #                                               A[2023-11-16] (SKIP even if non-existent, parent is in tree)
        #                                               A[2023-11-15] (SKIP even if non-existent, parent is in tree)
        #                                               A[2023-11-14] (SKIP even if non-existent, parent is in tree)
        #                                            <---
        #                   B[2023-11-20] (DONOT EXECUTE: mark A[2023-11-20] as BLOCKED, create a pending exec on B)
        #               <---
        # A[2023-11-20] (EXECUTE!) (END of local / client side RECURSION in Application::execute)
        #
        # The rest of the follow will happen in orchestration (remotely) and eventually it will unwind as:
        #
        #               ---> B[2023-11-20] (EXECUTE upon completion of A[2023-11-20])
        #                                 ---> C[2023-11-20] (EXECUTE upon completion of B[2023-11-20]
        app.execute(a[1]["2023-11-20"], wait=False, recursive=True, update_dependency_tree=True)

        time.sleep(3 * 3)

        # now check the entire branch
        #  - only A["2023-11-20"] and B["2023-11-20"] must have been executed
        #  - C["2023-11-20"] must be pending on 6 historical partitions of A ("2023-11-19", ..., "2023-11-14")
        for i in range(6):
            date = f"2023-11-{14 + i}"
            path, _ = app.poll(a[1][date])
            assert not path, f"Execution on A[{date!r}] should not exist!"

        path, _ = app.poll(a[1]["2023-11-20"])
        assert path, f"Execution on A['2023-11-20'] must have been successful!"
        inactive_records = app.get_inactive_compute_records(a)
        assert len(list(inactive_records)) == 1

        path, _ = app.poll(b[1]["2023-11-20"])
        assert path, f"Execution on B['2023-11-20'] must have been successful!"
        inactive_records = app.get_inactive_compute_records(b)
        assert len(list(inactive_records)) == 1

        # C should not have any executions yet
        path, _ = app.poll(c[1]["2023-11-20"])
        assert not path, "Execution on C should not exist yet!"

        inactive_records = app.get_inactive_compute_records(c)
        assert len(list(inactive_records)) == 0

        c_route = app.get_active_route(c)
        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        # Now satisfy A's historical partitions and see if C's pending execution will be unblocked
        for i in range(6):
            date = f"2023-11-{14 + i}"
            # option 1
            app.execute(a[1][date], wait=False)
            # option 2
            # add_test_data(app, a[1][date], object_name="_SUCCESS", data="")

        time.sleep(2 * 3)

        path, _ = app.poll(c[1]["2023-11-20"])
        assert path, "Execution on C['2023-11-20'] must have been successful!"

        # make sure redundant executions did not occur
        inactive_records = app.get_inactive_compute_records(a)
        assert len(list(inactive_records)) == 7

        inactive_records = app.get_inactive_compute_records(b)
        assert len(list(inactive_records)) == 1

        inactive_records = app.get_inactive_compute_records(c)
        assert len(list(inactive_records)) == 1

        # CLEAN #1.1 show deletion of 'a' on 2023-11-14 will;
        #  - not touch B (as it directly consumes 2023-11-20), not using a range of 'a'
        #  - clean up C (as its 2023-11-20 partition relies on 7 days of a and uses deleted partition)
        add_test_data(app, a[1]["2023-11-14"], "_SUCCESS", "")
        add_test_data(app, b[1]["2023-11-20"], "_SUCCESS", "")
        add_test_data(app, c[1]["2023-11-20"], "_SUCCESS", "")
        assert check_test_data(app, a[1]["2023-11-14"], "_SUCCESS"), "Could not create artificial completion file!"
        assert check_test_data(app, b[1]["2023-11-20"], "_SUCCESS"), "Could not create artificial completion file!"
        assert check_test_data(app, c[1]["2023-11-20"], "_SUCCESS"), "Could not create artificial completion file!"
        app.execute(
            a[1]["2023-11-14"], wait=True, recursive=False, update_dependency_tree=True, executor_mode=Application.ExecutorMode.CLEANUP
        )
        assert check_test_data(app, ext1[1]["2023-11-14"], "_SUCCESS"), "Should have left external data node 'ext1' untouched!"
        assert check_test_data(app, ext2[1]["2023-11-20"], "_SUCCESS"), "Should have left external data node 'ext2' untouched!"
        # now check the internals
        assert not check_test_data(app, a[1]["2023-11-14"], "_SUCCESS"), "Could not clean up node 'a'!"
        # b should survive
        assert check_test_data(app, b[1]["2023-11-20"], "_SUCCESS"), "'b' node must survive deletion"
        # c should be impacted due to ranged dependency on a
        assert not check_test_data(app, c[1]["2023-11-20"], "_SUCCESS"), "Could not clean up node 'c'!"

        # CLEANUP #2 simple recursive bottom-up deletion from c to a will destroy entire range
        # c -> b -> and 7 days on a must be wiped out
        for i in range(7):
            add_test_data(app, a[1][f"2023-11-{14 + i}"], object_name="_SUCCESS", data="")
        add_test_data(app, c[1]["2023-11-20"], "_SUCCESS", "")
        assert check_test_data(app, a[1]["2023-11-14"], "_SUCCESS"), "Could not create artificial completion file!"
        assert check_test_data(app, a[1]["2023-11-20"], "_SUCCESS"), "Could not create artificial completion file!"
        assert check_test_data(app, b[1]["2023-11-20"], "_SUCCESS"), "Could not create artificial completion file!"
        assert check_test_data(app, c[1]["2023-11-20"], "_SUCCESS"), "Could not create artificial completion file!"
        # recursive -> True
        app.execute(
            c[1]["2023-11-20"], wait=True, recursive=True, update_dependency_tree=True, executor_mode=Application.ExecutorMode.CLEANUP
        )
        for i in range(7):
            assert not check_test_data(app, a[1][f"2023-11-{14 + i}"], "_SUCCESS"), "Could not clean up node 'a'!"
        assert not check_test_data(app, b[1]["2023-11-20"], "_SUCCESS"), "'b' node must survive deletion"
        assert not check_test_data(app, c[1]["2023-11-20"], "_SUCCESS"), "Could not clean up node 'c'!"

        self.patch_aws_stop()

    @pytest.mark.parametrize(
        "concurrent_executor_pool_size",
        [
            (0),
            (3),
        ],
    )
    def test_application_execute_dtree_two_childs_update_past_executions(self, concurrent_executor_pool_size: int):
        """Modify the previous test to capture the following corner case:

        - "C" relies on multiple partitions "B" (weekly). Previously C had 3 days of successful executions between the
        dates "2023-11-20" and "2023-11-22". Show that execution of "A" on "2023-11-20" will automatically update all
        of those partitions as they transitively rely on it due to ranged access intermediate node "B".
        """
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True, concurrent_executor_pool_size=concurrent_executor_pool_size)

        app = AWSApplication("execute-dtree-6", self.region)

        ext1 = app.marshal_external_data(
            S3Dataset("111222333444", "bucket-execute-dtree-6", "data", "{}", "{}"),
            "ext_data_1",
            {"region": {"type": DimensionType.LONG, "day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        a = app.create_data("A", inputs=[ext1], compute_targets=[NOOPCompute])

        b = app.create_data("B", inputs=[a], compute_targets=[NOOPCompute])

        c = app.create_data("C", inputs=[b[1][:-7].range_check(True)], compute_targets=[NOOPCompute])

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=3)

        # emulate partition readiness
        for i in range((7 + 2)):
            add_test_data(app, ext1[1][f"2023-11-{14 + i}"], object_name="_SUCCESS", data="")
            app.execute(a[1][f"2023-11-{14 + i}"], concurrent_executor_pool=self.pool)
            add_test_data(app, a[1][f"2023-11-{14 + i}"], object_name="_SUCCESS", data="")
            add_test_data(app, b[1][f"2023-11-{14 + i}"], object_name="_SUCCESS", data="")

        time.sleep(2 * 3)

        # show that before the actual test C has only 3 past executions and each one of them ran only once
        for i in range(3):
            date = f"2023-11-{20 + i}"
            inactive_records = app.get_inactive_compute_records(c)
            assert (
                len(
                    [
                        inactive_record
                        for inactive_record in inactive_records
                        if DimensionFilter.check_equivalence(
                            inactive_record.materialized_output.domain_spec.dimension_filter_spec,
                            c[1][date].get_signal().domain_spec.dimension_filter_spec,
                        )
                    ]
                )
                == 1
            ), f"Redundant execution on C[{date!r}] found!"

        inactive_records = app.get_inactive_compute_records(a)
        assert len(list(inactive_records)) == 9  # A["2023-11-14"] ... A["2023-11-22"]

        inactive_records = app.get_inactive_compute_records(b)
        assert len(list(inactive_records)) == 9  # B["2023-11-14"] ... B["2023-11-22"]

        time.sleep(3)
        # now check the entire branch

        # now assume that the user realized that A["2023-11-20"] needs to rerun and its dependencies must be refreshed.
        # so the "scan end" datetime input (that defines the end of the past executions search interval) easily captures
        # the existing partitions of C.
        app.execute(
            a[1]["2023-11-20"],
            wait=False,
            recursive=True,
            update_dependency_tree=True,
            dependency_tree_scan_end=datetime.now(),
            concurrent_executor_pool=self.pool,
        )

        time.sleep(2 * 3)

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        # make sure redundant executions did not occur
        inactive_records = app.get_inactive_compute_records(a)
        assert len(list(inactive_records)) == (9 + 1)  # only A["2023-11-20"] is expected to be executed

        inactive_records = app.get_inactive_compute_records(b)
        assert len(list(inactive_records)) == (9 + 1)  # same for B

        inactive_records = app.get_inactive_compute_records(c)
        assert len(list(inactive_records)) == (
            3 + 3
        )  # but C should update the entire range because its 3 partitions all rely on updated partition on B

        # now also make sure that number match was not just a coincidence and each C partition has been re-executed once
        for i in range(3):
            date = f"2023-11-{20 + i}"
            inactive_records = app.get_inactive_compute_records(c)
            assert (
                len(
                    [
                        inactive_record
                        for inactive_record in inactive_records
                        if DimensionFilter.check_equivalence(
                            inactive_record.materialized_output.domain_spec.dimension_filter_spec,
                            c[1][date].get_signal().domain_spec.dimension_filter_spec,
                        )
                    ]
                )
                == 2
            ), f"Redundant execution on C[{date!r}] found!"

        # now assume that the user realized that A["2023-11-20"] needs to be rerun again but downstream partitions
        # with a more recent date would be excluded (e.g C["2023-11-21"]). we will use scan end parameter for this.
        # skipped partitions have execution trigger timestamps more recent than scan end param here.
        app.execute(
            a[1]["2023-11-20"],
            wait=False,
            recursive=True,
            update_dependency_tree=True,
            dependency_tree_scan_end=datetime.now() - timedelta(hours=1),
            concurrent_executor_pool=self.pool,
        )

        # we must see 1 execution on A, B and C. C's partitions 2023-11-21 and 2023-11-22 must be skipped.
        inactive_records = app.get_inactive_compute_records(a)
        assert len(list(inactive_records)) == (9 + 1 + 1)  # only A["2023-11-20"] is expected to be executed

        inactive_records = app.get_inactive_compute_records(b)
        assert len(list(inactive_records)) == (9 + 1 + 1)  # same for B

        inactive_records = app.get_inactive_compute_records(c)
        assert len(list(inactive_records)) == (3 + 3 + 1)  # but C should update only one partition because of scan datum

        # CLEANUP just 7 days of cleanup on 'a' should take care of entire dependency tree
        for i in range(7):
            app.execute(
                a[1][f"2023-11-{14 + i}"],
                wait=True,
                recursive=False,
                update_dependency_tree=True,
                executor_mode=Application.ExecutorMode.CLEANUP,
            )

        # check 2023-11-14 - 2024-11-20 range is gone on both 'a' and 'b'
        for i in range(7):
            assert not check_test_data(app, a[1][f"2023-11-{14 + i}"], "_SUCCESS"), "Could not clean up node 'a'!"
            assert not check_test_data(app, b[1][f"2023-11-{14 + i}"], "_SUCCESS"), "Could not clean up node 'b'!"

        # check 2023-11-20 - 2024-11-22 range is gone on 'c'
        for i in range(2):
            assert not check_test_data(app, c[1][f"2023-11-{20 + i}"], "_SUCCESS"), "Could not clean up node 'c'!"

        self.patch_aws_stop()

    @pytest.mark.parametrize(
        "concurrent_executor_pool_size",
        [
            (0),
            (2),
        ],
    )
    def test_application_execute_dtree_ranged_access_same_input(self, concurrent_executor_pool_size: int):
        """Same input used with different ranges. We would like to capture this corner case and make sure that no
        redundant executions happen."""
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True, concurrent_executor_pool_size=concurrent_executor_pool_size)
        app = AWSApplication("execute-dtree-7", self.region)

        ext1 = app.marshal_external_data(
            S3Dataset("111222333444", "bucket-execute-dtree-7", "data", "{}", "{}"),
            "ext_data_1",
            {"region": {"type": DimensionType.LONG, "day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        a = app.create_data("A", inputs=[ext1], compute_targets=[NOOPCompute])

        b = app.create_data("B", inputs=[a], compute_targets=[NOOPCompute])

        c = app.create_data(
            "C",
            inputs={
                "b_today": b,
                # tomorrow and the day after
                "b_range_next_two_days": b[1][1:2].range_check(True),
            },
            compute_targets=[NOOPCompute],
        )

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=3)

        # emulate partition readiness
        for i in range(5):
            add_test_data(app, ext1[1][f"2023-11-{20 + i}"], object_name="_SUCCESS", data="")
            app.execute(a[1][f"2023-11-{20 + i}"], concurrent_executor_pool=self.pool)
            add_test_data(app, a[1][f"2023-11-{20 + i}"], object_name="_SUCCESS", data="")
            add_test_data(app, b[1][f"2023-11-{20 + i}"], object_name="_SUCCESS", data="")

        time.sleep(2 * 3)

        # show that before the actual test C has only 3 past executions and each one of them ran only once
        for i in range(3):
            date = f"2023-11-{20 + i}"
            inactive_records = app.get_inactive_compute_records(c)
            assert (
                len(
                    [
                        inactive_record
                        for inactive_record in inactive_records
                        if DimensionFilter.check_equivalence(
                            inactive_record.materialized_output.domain_spec.dimension_filter_spec,
                            c[1][date].get_signal().domain_spec.dimension_filter_spec,
                        )
                    ]
                )
                == 1
            ), f"Redundant execution on C[{date!r}] found!"

        inactive_records = app.get_inactive_compute_records(a)
        assert len(list(inactive_records)) == 5

        inactive_records = app.get_inactive_compute_records(b)
        assert len(list(inactive_records)) == 5

        # now check the entire branch

        # now assume that the user realized that A["2023-11-20"] needs to rerun and its dependencies must be refreshed.
        # so the "scan end" datetime input (that defines the end of the past executions search interval) easily captures
        # the existing partition of C. Only 2023-11-20 partition of C must be updated.
        app.execute(
            a[1]["2023-11-20"],
            wait=False,
            recursive=True,
            update_dependency_tree=True,
            dependency_tree_scan_end=datetime.now(),
            concurrent_executor_pool=self.pool,
        )

        time.sleep(3)

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        # make sure redundant executions did not occur
        inactive_records = app.get_inactive_compute_records(a)
        assert len(list(inactive_records)) == (5 + 1)  # only A["2023-11-20"] is expected to be executed

        inactive_records = app.get_inactive_compute_records(b)
        assert len(list(inactive_records)) == (5 + 1)  # same for B

        inactive_records = app.get_inactive_compute_records(c)
        assert len(list(inactive_records)) == (3 + 1)  # only one partition of C must be updated

        # now also make sure that number match was not just a coincidence and each C partition has been re-executed once
        date = "2023-11-20"
        inactive_records = app.get_inactive_compute_records(c)
        assert (
            len(
                [
                    inactive_record
                    for inactive_record in inactive_records
                    if DimensionFilter.check_equivalence(
                        inactive_record.materialized_output.domain_spec.dimension_filter_spec,
                        c[1][date].get_signal().domain_spec.dimension_filter_spec,
                    )
                ]
            )
            == 1
        ), f"Redundant execution on C[{date!r}] found!"

        self.patch_aws_stop()

    @pytest.mark.parametrize(
        "concurrent_executor_pool_size",
        [
            (0),
            (5),
        ],
    )
    def test_application_execute_diamond_hierarchy_with_references_and_fully_scheduled(self, concurrent_executor_pool_size: int):
        """Internal nodes as references and with dependency check, forming a diamond hierarchy.
        We need to show that when tree update is in progress even "references" will be "blocked" in downstream nodes and
        be pending upon.
        """
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True, concurrent_executor_pool_size=concurrent_executor_pool_size)
        app = AWSApplication("execute-dtree-8", self.region)

        daily_timer = app.add_timer(
            "DAILY_TIMER",
            schedule_expression="cron(0 0 1 * ? *)",
            # Run at 00:00 am (UTC) every 1st day of the month
            time_dimension_id="date",
        )

        regionalized_timer = app.project(
            "REGIONAL_DAILY_TIMER",
            input=daily_timer,
            output_dimension_spec={
                "region_id": {
                    type: DimensionType.LONG,
                    "marketplace_id": {type: DimensionType.LONG, "date": {type: DimensionType.DATETIME, "format": "%Y-%m-%d"}},
                }
            },
            output_dimension_filter={
                1: {1: {"*": {}}},
            },
        )

        a = app.create_data("node_0", inputs=[regionalized_timer], compute_targets=[NOOPCompute])

        b = app.create_data("node_1_1", inputs=[regionalized_timer, a.ref.range_check(True)], compute_targets=[NOOPCompute])
        c = app.create_data(
            "node_1_2",
            inputs=[regionalized_timer, a.ref.range_check(True)],
            compute_targets=[InlinedCompute(lambda input_map, output, params: time.sleep(1))],
        )

        d = app.create_data(
            "node_2",
            inputs=[regionalized_timer, b.ref.range_check(True), c.ref.range_check(True)],
            compute_targets=[NOOPCompute],
        )

        app.activate()

        test_date = "2024-03-30"
        # emulate partition readiness
        add_test_data(app, a[1][1][test_date], object_name="_SUCCESS", data="")
        add_test_data(app, b[1][1][test_date], object_name="_SUCCESS", data="")
        add_test_data(app, c[1][1][test_date], object_name="_SUCCESS", data="")
        add_test_data(app, d[1][1][test_date], object_name="_SUCCESS", data="")

        self.activate_event_propagation(app, cycle_time_in_secs=10)

        # first check that the app history is empty
        inactive_records = app.get_inactive_compute_records(a)
        assert len(list(inactive_records)) == 0

        # now assume that the user realized that "node_0" needs to rerun and its dependencies must be refreshed.
        app.execute(
            a[1][1][test_date],
            wait=False,
            recursive=True,
            update_dependency_tree=True,
            concurrent_executor_pool=self.pool,
        )

        time.sleep(3 * 3)

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        # make sure redundant executions did not occur and there is only one on each node.
        # and also check if executions happened in expected order (first a -> b,c -> d)
        prev_exec_time = -1
        for node in [a, b, c, d]:
            inactive_records = list(app.get_inactive_compute_records(node))
            assert len(inactive_records) == 1
            compute_record = inactive_records[0]
            assert prev_exec_time <= compute_record.deactivated_timestamp_utc, "Executions did not occur in right order!"
            prev_exec_time = compute_record.deactivated_timestamp_utc

        self.patch_aws_stop()

    @pytest.mark.parametrize(
        "concurrent_executor_pool_size",
        [
            (0),
            (7 * 5),
        ],
    )
    def test_application_execute_diamond_hierarchy_with_references_and_fully_scheduled_batch(self, concurrent_executor_pool_size: int):
        """Run a modified version of the test above over multiple dimensions using the batch API"""
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True, concurrent_executor_pool_size=concurrent_executor_pool_size)
        app = AWSApplication("execute-dtree-9", self.region)

        daily_timer = app.add_timer(
            "DAILY_TIMER",
            schedule_expression="cron(0 0 1 * ? *)",
            # Run at 00:00 am (UTC) every 1st day of the month
            time_dimension_id="date",
        )

        regionalized_timer = app.project(
            "REGIONAL_DAILY_TIMER",
            input=daily_timer,
            output_dimension_spec={
                "region_id": {
                    type: DimensionType.LONG,
                    "marketplace_id": {type: DimensionType.LONG, "date": {type: DimensionType.DATETIME, "format": "%Y-%m-%d"}},
                }
            },
            output_dimension_filter={
                1: {1: {"*": {}}},
            },
        )

        a = app.create_data("node_0", inputs=[regionalized_timer], compute_targets=[NOOPCompute])

        b = app.create_data("node_1_1", inputs=[regionalized_timer, a.ref.range_check(True)], compute_targets=[NOOPCompute])
        c = app.create_data("node_1_2", inputs=[regionalized_timer, a.ref.range_check(True)], compute_targets=[NOOPCompute])

        d = app.create_data(
            "node_2",
            inputs=[regionalized_timer, b.ref.range_check(True), c.ref.range_check(True)],
            compute_targets=[NOOPCompute],
        )

        app.activate()

        range_in_days = 14
        test_date = datetime(2024, 3, 30)
        roots = [a[1][1][test_date + timedelta(days=i)] for i in range(range_in_days)]

        self.activate_event_propagation(app, cycle_time_in_secs=10)

        app.execute_batch(
            roots,
            wait=False,
            recursive=True,
            update_dependency_tree=True,
            concurrent_executor_pool=self.pool,
        )

        time.sleep(3 * 3)

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        # make sure redundant executions did not occur and there is only range_in_days on each node.
        # and also check if executions happened in expected order (first a -> b,c -> d)
        for node in [a, b, c, d]:
            inactive_records = list(app.get_inactive_compute_records(node))
            assert len(inactive_records) == range_in_days
            compute_record = inactive_records[0]
            assert compute_record.session_state.state_type == ComputeSessionStateType.COMPLETED, "Execution must be COMPLETED"

        self.patch_aws_stop()

    @pytest.mark.parametrize(
        "concurrent_executor_pool_size",
        [
            (0),
            (35),
        ],
    )
    def test_application_execute_diamond_hierarchy_with_references_and_fully_scheduled_batch_ranged(
        self, concurrent_executor_pool_size: int
    ):
        """Run a modified version of the test above with a more complex extension having ranged access (aggregations)"""
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True, concurrent_executor_pool_size=0)
        app = AWSApplication("execute-dtree-10", self.region)

        daily_timer = app.add_timer(
            "DAILY_TIMER",
            schedule_expression="cron(0 0 1 * ? *)",
            # Run at 00:00 am (UTC) every 1st day of the month
            time_dimension_id="date",
        )

        regionalized_timer = app.project(
            "REGIONAL_DAILY_TIMER",
            input=daily_timer,
            output_dimension_spec={
                "region_id": {
                    type: DimensionType.LONG,
                    "marketplace_id": {type: DimensionType.LONG, "date": {type: DimensionType.DATETIME, "format": "%Y-%m-%d"}},
                }
            },
            output_dimension_filter={
                1: {1: {"*": {}}},
            },
        )

        a = app.create_data("node_0", inputs=[regionalized_timer], compute_targets=[NOOPCompute])

        b = app.create_data("node_1_1", inputs=[regionalized_timer, a.ref.range_check(True)], compute_targets=[NOOPCompute])
        c = app.create_data("node_1_2", inputs=[regionalized_timer, a.ref.range_check(True)], compute_targets=[NOOPCompute])
        b_1 = app.create_data(
            "node_1_1_1",
            inputs=[regionalized_timer, c.ref.range_check(True), b["*"]["*"][:-21].ref.range_check(True)],
            compute_targets=[NOOPCompute],
        )
        c_1 = app.create_data(
            "node_1_2_1",
            inputs=[regionalized_timer, b.ref.range_check(True), c["*"]["*"][:-7].ref.range_check(True)],
            compute_targets=[NOOPCompute],
        )

        d = app.create_data(
            "node_2",
            inputs=[
                regionalized_timer,
                b.ref.range_check(True),
                c.ref.range_check(True),
                b_1.ref.range_check(True),
                c_1.ref.range_check(True),
            ],
            compute_targets=[NOOPCompute],
        )

        e = app.create_data(
            "node_3",
            inputs=[
                regionalized_timer,
                b_1.ref.range_check(True),
                c_1.ref.range_check(True),
            ],
            compute_targets=[NOOPCompute],
        )

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=10)

        # 1- backfill`the entire tree
        app.execute(d[1][1]["2024-12-24"], recursive=True)
        app.execute(d[1][1]["2024-12-26"], recursive=True)

        app.execute(e[1][1]["2024-12-24"], recursive=True)
        app.execute(e[1][1]["2024-12-26"], recursive=True)

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        # these nodes must have one execution
        for node in [b_1, c_1, d, e]:
            inactive_records = list(app.get_inactive_compute_records(node))
            assert len(inactive_records) == 1 + 1
            compute_record = inactive_records[0]
            assert compute_record.session_state.state_type == ComputeSessionStateType.COMPLETED, "Execution must be COMPLETED"

        for node in [a, b]:
            inactive_records = list(app.get_inactive_compute_records(node))
            assert len(inactive_records) == 21 + 2
            compute_record = inactive_records[0]
            assert compute_record.session_state.state_type == ComputeSessionStateType.COMPLETED, "Execution must be COMPLETED"

        inactive_records = list(app.get_inactive_compute_records(c))
        assert len(inactive_records) == 7 + 2
        compute_record = inactive_records[0]
        assert compute_record.session_state.state_type == ComputeSessionStateType.COMPLETED, "Execution must be COMPLETED"

        # 2- now update "a" and see if the entire tree will be updated
        app.execute(
            a[1][1]["2024-12-24"],
            wait=False,
            recursive=True,
            update_dependency_tree=True,
            concurrent_executor_pool=self.pool,
        )

        time.sleep(15)
        app.update_active_routes_status()

        # uncomment for debugging
        # app.admin_console()

        for node in [b_1, c_1, d, e]:
            inactive_records = list(app.get_inactive_compute_records(node))
            assert len(inactive_records) == 1 + 1 + (1 + 1)
            compute_record = inactive_records[0]
            assert compute_record.session_state.state_type == ComputeSessionStateType.COMPLETED, "Execution must be COMPLETED"

        for node in [a, b]:
            inactive_records = list(app.get_inactive_compute_records(node))
            assert len(inactive_records) == 21 + 2 + (1)
            compute_record = inactive_records[0]
            assert compute_record.session_state.state_type == ComputeSessionStateType.COMPLETED, "Execution must be COMPLETED"

        inactive_records = list(app.get_inactive_compute_records(c))
        assert len(inactive_records) == 7 + 2 + (1)
        compute_record = inactive_records[0]
        assert compute_record.session_state.state_type == ComputeSessionStateType.COMPLETED, "Execution must be COMPLETED"

        self.patch_aws_stop()
