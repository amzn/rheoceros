# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from datetime import datetime, timedelta

import pytest
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
from intelliflow.utils.test.data_emulation import add_test_data
from intelliflow.utils.test.inlined_compute import FailureCompute, NOOPCompute


class TestAWSApplicationExecutionRecursive(AWSTestBase):
    """
    This test module wants to capture the advanced utilization of Application::execute when 'recursive' is set True.
    """

    def test_application_execute_recursive_async_single_node(self):
        """
        Single node, capture edge-cases and show no difference in behaviour as compared to 'recursive=False'
        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("execute-recur-1", self.region)

        test_node = app.create_data("test_node", compute_targets=[NOOPCompute])

        app.execute(test_node, wait=False, recursive=True)

        # emulate Processor next-cycle
        app.update_active_routes_status()

        # check successful execution
        path, _ = app.poll(test_node)
        assert path

        self.patch_aws_stop()

    def test_application_execute_recursive_sync_single_node(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("execute-recur-2", self.region)

        test_node = app.create_data("test_node", compute_targets=[NOOPCompute])

        app.execute(test_node, wait=True, recursive=True)

        # emulate Processor next-cycle
        app.update_active_routes_status()

        # check successful execution
        path, _ = app.poll(test_node)
        assert path

        self.patch_aws_stop()

    def test_application_execute_recursive_with_local_orchestration(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("execute-recur-3", self.region)

        test_node = app.create_data("test_node", compute_targets=[NOOPCompute])

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=5)

        app.execute(test_node, wait=True, recursive=True)

        path, _ = app.poll(test_node)
        assert path, "Execution must have been successful!"

        self.patch_aws_stop()

    def test_application_execute_recursive_single_branch_two_nodes_wait(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)
        app = AWSApplication("execute-recur-4", self.region)

        a = app.create_data("A", compute_targets=[NOOPCompute])

        b = app.create_data("B", inputs=[a], compute_targets=[NOOPCompute])

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=5)

        app.execute(b, wait=True, recursive=True)

        path, _ = app.poll(b)
        assert path, "Execution must have been successful!"

        path, _ = app.poll(a)
        assert path, "Execution must have been successful!"

        self.patch_aws_stop()

    def test_application_execute_recursive_single_branch_two_nodes_no_wait(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("execute-recur-5", self.region)

        a = app.create_data("A", compute_targets=[NOOPCompute])

        b = app.create_data("B", inputs=[a], compute_targets=[NOOPCompute])

        app.activate()

        orchestration_cycle_time_in_secs = 5
        self.activate_event_propagation(app, cycle_time_in_secs=orchestration_cycle_time_in_secs)

        app.execute(b, wait=False, recursive=True)

        # now check the entire branch
        path, _ = app.poll(b)
        assert path, "Execution must have been successful!"

        # recursive execution on a
        path, _ = app.poll(a)
        assert path, "Execution must have been successful!"

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        self.patch_aws_stop()

    def test_application_execute_recursive_single_branch_two_nodes_failure(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("exec-recur-5-1", self.region)

        a = app.create_data("A", compute_targets=[FailureCompute])

        b = app.create_data("B", inputs=[a], compute_targets=[NOOPCompute])

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=5)

        # recursive exec on A should fail the entire execution
        with pytest.raises(RuntimeError):
            app.execute(b, wait=True, recursive=True)

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        self.patch_aws_stop()

    def test_application_execute_recursive_single_branch_two_nodes_ranged(self):
        """This test wants to capture the edge-case where the first parent partition is ready but one of the historicals
        should be executed in recursive mode.
        So at the end of the test, we verify;
        - No redundant executions on A[today]
        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("exec-recur-5-2", self.region)

        a = app.create_data("A", inputs=[app.add_timer("T", "rate(1 day)")], compute_targets=[NOOPCompute])

        today = datetime.now()

        # make A ready for today
        app.execute(a[today], wait=True)

        b = app.create_data("B", inputs=[a[:-2]], compute_targets=[NOOPCompute])

        app.execute(b[today], wait=True, recursive=True)

        assert app.poll(b[today])[0], "Execution must have been successful!"
        assert app.poll(a[today - timedelta(1)])[0], "Recursive execution on node 'A' could not be found!"

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        # Verify no redundant executions on A[today], there should only be one (from the manual execution in the beginning)
        inactive_records = app.get_inactive_compute_records(a)
        assert (
            len(
                [
                    inactive_record
                    for inactive_record in inactive_records
                    if DimensionFilter.check_equivalence(
                        inactive_record.materialized_output.domain_spec.dimension_filter_spec,
                        a[today].get_signal().domain_spec.dimension_filter_spec,
                    )
                ]
            )
            == 1
        ), f"Redundant execution on A[{today.strftime('%Y-%m-%d')}] found!"

        self.patch_aws_stop()

    def test_application_execute_recursive_single_branch_two_nodes_ranged_no_wait(self):
        """This test is a modified version of 'test_application_execute_recursive_single_branch_two_nodes_ranged'.
        The difference is 'wait=False' on recursive execution.

        Here we capture the enforced 'sync' execution on the historical gap of parent node.
        Even if 'async' is desired by the user, IF enforces sync on recursive/nested execution on a 'range' of parent
        node when TIP for the parent is ready but any of the partitions in the range is missing.
        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("exec-recur-5-3", self.region)

        a = app.create_data("A", inputs=[app.add_timer("T", "rate(1 day)")], compute_targets=[NOOPCompute])

        today = datetime.now()

        # make A ready for today
        app.execute(a[today], wait=True)

        b = app.create_data("B", inputs=[a[:-3]], compute_targets=[NOOPCompute])

        app.execute(b[today], wait=False, recursive=True)

        assert app.poll(b[today])[0], "Execution must have been successful!"
        assert app.poll(a[today - timedelta(1)])[0], "Recursive execution on node 'A' could not be found!"
        assert app.poll(a[today - timedelta(2)])[0], "Recursive execution on node 'A' could not be found!"

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        self.patch_aws_stop()

    def test_application_execute_recursive_single_branch_two_internal_one_external(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)
        app = AWSApplication("execute-recur-6", self.region)

        ext = app.marshal_external_data(
            S3Dataset("111222333444", "bucket-exec-recurse", "data", "{}", "{}"),
            "ext_data_1",
            {"region": {"type": DimensionType.LONG, "day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        a = app.create_data("A", inputs=[ext], compute_targets=[NOOPCompute])

        b = app.create_data("B", inputs=[a], compute_targets=[NOOPCompute])

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=5)

        with pytest.raises(ValueError):
            # external data is not ready 'recursive' mode should detect and raise
            app.execute(b[1]["2022-03-04"], wait=True, recursive=True)

        # emulate partition readiness
        add_test_data(app, ext[1]["2022-03-04"], object_name="_SUCCESS", data="")

        # should go smoothly now!
        app.execute(b[1]["2022-03-04"], wait=True, recursive=True)

        # now check the entire branch
        path, _ = app.poll(b[1]["2022-03-04"])
        assert path, "Execution must have been successful!"

        path, _ = app.poll(a[1]["2022-03-04"])
        assert path, "Execution must have been successful!"

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        self.patch_aws_stop()

    def test_application_execute_recursive_single_branch_two_internal_one_external_ranged(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)
        app = AWSApplication("execute-recur-7", self.region)

        ext = app.marshal_external_data(
            S3Dataset(self.account_id, "bucket-exec-recurse", "data", "{}"),
            "ext_data_1",
            {"day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        # note that "A" depends on a range of 'NOW - 3' partitions on 'ext_data_1'
        a = app.create_data("A", inputs=[ext[:-3]], compute_targets=[NOOPCompute])

        b = app.create_data("B", inputs=[a], compute_targets=[NOOPCompute])

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=5)

        today = datetime.now()
        tomorrow = today + timedelta(1)
        yesterday = today - timedelta(1)

        # today -> READY
        add_test_data(app, ext[today], object_name="_SUCCESS", data="")

        # CASE 1: RECURSIVE EXECUTION ON TODAY
        with pytest.raises(ValueError):
            # only 1 partition of external data is ready 2 missing
            app.execute(b[today], wait=True, recursive=True)

        # (today - 1)-> READY
        add_test_data(app, ext[yesterday], object_name="_SUCCESS", data="")

        with pytest.raises(ValueError):
            # 2 partitions of external data is ready 1 missing
            app.execute(b[today], wait=True, recursive=True)

        # (today - 2)-> READY
        add_test_data(app, ext[yesterday - timedelta(1)], object_name="_SUCCESS", data="")

        # should go smoothly now!
        app.execute(b[today], wait=True, recursive=True)

        # now check the entire branch
        path, _ = app.poll(b[today])
        assert path, "Execution must have been successful!"

        path, _ = app.poll(a[today])
        assert path, "Execution must have been successful!"

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        # end CASE 1

        # CASE 2: RECURSIVE EXECUTION ON TOMORROW
        with pytest.raises(ValueError):
            # only 1 partition of external data is missing, 2 is already ready
            app.execute(b[tomorrow], wait=True, recursive=True)

        add_test_data(app, ext[tomorrow], object_name="_SUCCESS", data="")

        # do not wait
        app.execute(b[tomorrow], wait=False, recursive=True)

        path, _ = app.poll(b[tomorrow])
        assert path, "Execution must have been successful!"

        path, _ = app.poll(a[tomorrow])
        assert path, "Execution must have been successful!"

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        self.patch_aws_stop()

    def test_application_execute_recursive_single_branch_two_internal_one_external_ranged_on_intermediate(self):
        """Different than the previous test, we focus more on the ranged and recursive execution on
        the intermediate node"""
        self.patch_aws_start(glue_catalog_has_all_tables=True)
        app = AWSApplication("execute-recur-8", self.region)

        ext = app.marshal_external_data(
            S3Dataset(self.account_id, "bucket-exec-recurse", "data", "{}"),
            "ext_data_1",
            {"day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        # note that "A" depends on a range of 'NOW - 2' partitions on 'ext_data_1'
        a = app.create_data("A", inputs=[ext[:-2]], compute_targets=[NOOPCompute])

        # note that "B" depends on a range of 'NOW - 2' partitions on 'A'
        b = app.create_data("B", inputs=[a[:-2]], compute_targets=[NOOPCompute])

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=5)

        today = datetime.now()
        tomorrow = today + timedelta(1)
        yesterday = today - timedelta(1)

        # make entire range for external data ready (we make tomorrow ready too because second execution on 'A' will need it)
        add_test_data(app, ext[yesterday - timedelta(1)], object_name="_SUCCESS", data="")
        add_test_data(app, ext[yesterday], object_name="_SUCCESS", data="")
        add_test_data(app, ext[today], object_name="_SUCCESS", data="")
        add_test_data(app, ext[tomorrow], object_name="_SUCCESS", data="")

        # do not wait and demand (backfiller) executions on two branches
        app.execute(b[today], wait=False, recursive=True)
        app.execute(b[tomorrow], wait=False, recursive=True)

        assert app.poll(b[today])[0], "Execution on B must have been successful!"
        assert app.poll(a[today])[0], "Execution on A must have been successful!"
        # very important! recursively executed yesterday becase b[today] required a[today] + a[yesterday]
        assert app.poll(a[yesterday])[0], "Execution on A must have been successful!"

        assert app.poll(b[tomorrow])[0], "Execution on B must have been successful!"
        assert app.poll(a[tomorrow])[0], "Execution on A must have been successful!"

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        self.patch_aws_stop()

    def test_application_execute_recursive_multiple_branches(self):
        """Diamond topology that has different input (advanced) modes such as 'nearest'."""
        self.patch_aws_start(glue_catalog_has_all_tables=True)
        app = AWSApplication("execute-recur-9", self.region)

        ext = app.marshal_external_data(
            S3Dataset(self.account_id, "bucket-exec-recurse", "data", "{}"),
            "ext_data_1",
            {"day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        # note that "A" depends on a range of 'NOW - 2' partitions on 'ext_data_1'
        a = app.create_data(
            "A", inputs=[ext[:-2].nearest(), app.add_timer("T", "rate(1 day)", time_dimension_id="day")], compute_targets=[NOOPCompute]
        )

        # note that "B" depends on a range of 'NOW - 2' partitions on 'ext_data_1'
        b = app.create_data("B", inputs=[ext[:-2]], compute_targets=[NOOPCompute])

        c = app.create_data("C", inputs=[ext], compute_targets=[NOOPCompute])

        # use 'nearest' (which would trigger execution on nearest, TIP partition if entire range is missing)
        # and 'ref' (which would still go through entire range check when 'recursive'=True)
        d = app.create_data("D", inputs=[a[:-15].nearest(), b[:-2].ref, c], compute_targets=[NOOPCompute])

        app.activate()

        self.activate_event_propagation(app, cycle_time_in_secs=5)

        today = datetime.now()
        tomorrow = today + timedelta(1)
        yesterday = today - timedelta(1)

        add_test_data(app, ext[yesterday - timedelta(1)], object_name="_SUCCESS", data="")
        add_test_data(app, ext[yesterday], object_name="_SUCCESS", data="")
        add_test_data(app, ext[today], object_name="_SUCCESS", data="")
        add_test_data(app, ext[tomorrow], object_name="_SUCCESS", data="")

        # TODAY
        app.execute(d[today], wait=True, recursive=True)

        # A -> today only
        assert app.poll(a[today])[0], "Execution on A must have been successful!"
        assert not app.poll(a[yesterday])[0], "This execution should not exist on A due to 'nearest'"

        # B -> today and yesterday because D has two days dependency on it
        assert app.poll(b[today])[0], "Execution on B must have been successful!"
        assert app.poll(b[yesterday])[0], "Execution on B must have been successful!"

        assert app.poll(c[today])[0], "Execution on C must have been successful!"
        assert not app.poll(c[yesterday])[0], "This execution should not exist on C"

        assert app.poll(d[today])[0], "Execution on D must have been successful!"

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        # TOMORROW
        app.execute(d[tomorrow], wait=True, recursive=True)

        # VERY IMPORTANT! A -> still today only (due to the effect of 'nearest')
        assert not app.poll(a[tomorrow])[0], "This execution should not exist on A due to 'nearest'"
        assert app.poll(a[today])[0], "Execution on A must have been successful!"
        assert not app.poll(a[yesterday])[0], "This execution should not exist on A due to 'nearest'"

        # B -> new execution on TOMORROW
        assert app.poll(b[tomorrow])[0], "Execution on B must have been successful!"

        # C -> new execution on TOMORROW
        assert app.poll(c[tomorrow])[0], "Execution on C must have been successful!"

        assert app.poll(d[tomorrow])[0], "Execution on D must have been successful!"

        assert not app.get_active_routes(), "There should not be any left-over active records after recursive execution!"

        self.patch_aws_stop()
