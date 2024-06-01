# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

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
        a = app.create_data("A", inputs=[ext[:-3].range_check(True)], compute_targets=[NOOPCompute])

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

    def test_application_execute_recursive_chaos_with_batch_compute(self):
        """Relatively complex topology with latent compute that fails with TRANSIENT error in a random fashion."""
        self.patch_aws_start(glue_catalog_has_all_tables=True)
        app = AWSApplication("exec-recur-10", self.region)

        region = "NA"
        region_id = 1
        marketplace_id = 1

        tommy_external = app.marshal_external_data(
            S3Dataset(self.account_id, "bucket-exec-recurse", "data", "{}", "{}", "{}"),
            id=f"tommy_external",
            dimension_spec={
                "org": {
                    "type": DimensionType.STRING,
                    "format": lambda org: org.upper(),
                    # convenience: no matter what user input (in execute, process API) output upper
                    "partition_date": {
                        "type": DimensionType.DATETIME,
                        "format": "%Y%m%d",
                        "partition_hour": {"type": DimensionType.DATETIME, "format": "%Y%m%d%H"},
                    },
                }
            },
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        pdex_external = app.marshal_external_data(
            S3Dataset(self.account_id, "bucket-exec-recurse-2", "pdex_logs", "{}", "{}"),
            id="pdex_external",
            dimension_spec={
                "region": {type: DimensionType.STRING, "dataset_date": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d %H"}}
            },
        )

        dama = app.marshal_external_data(
            S3Dataset(self.account_id, "bucket-exec-recurse", "dama", "{}"),
            id="d_asins_marketplace_attributes",
            dimension_spec={
                "region_id": {
                    type: DimensionType.LONG,
                }
            },
        )

        xdf = app.marshal_external_data(
            S3Dataset(
                self.account_id,
                "searchdata-core-xdf-beta",
                "",
                "{}",
                "{}*",  # * -> to ignore the last two digit gibberish added as a suffix to partition value
                dataset_format=DataFormat.PARQUET,
            ),
            id="xdf_external",
            dimension_spec={
                "cdo_region": {
                    "type": DimensionType.STRING,
                    "format": lambda dim: dim.lower(),
                    "insensitive": True,
                    "day": {"type": DimensionType.DATETIME, "format": "%Y%m%d"},
                }
            },
        )

        pdex_consolidated = app.create_data(
            id=f"pdex_consolidated_{region}_{marketplace_id}",
            inputs={"pdex_external": pdex_external[region]["*"]},
            output_dimension_spec={
                "region_id": {
                    type: DimensionType.LONG,
                    "marketplace_id": {
                        type: DimensionType.LONG,
                        "day": {"format": "%Y-%m-%d", type: DimensionType.DATETIME, "hour": {type: DimensionType.LONG}},
                    },
                }
            },
            output_dim_links=[
                ("region_id", EQUALS, region_id),
                ("marketplace_id", EQUALS, marketplace_id),
                ("day", lambda hour_dt: hour_dt.date(), pdex_external("dataset_date")),
                ("hour", lambda hour_dt: hour_dt.hour, pdex_external("dataset_date")),
                # add reverse lookup from output to pdex_external for better testability and debugging exp
                (pdex_external("dataset_date"), lambda day, hour: datetime(day.year, day.month, day.day, hour), ("day", "hour")),
            ],
            compute_targets=[
                BatchCompute(
                    code="""PYSPARK CODE HERE""",
                    lang=Lang.PYTHON,
                    GlueVersion="3.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=150,
                    Timeout=4 * 60,  # 4 hours
                )
            ],
            dataset_format=DataFormat.PARQUET,
        )

        def pdex_to_tommy_converter(day_in_utc, hour_in_utc):
            from datetime import datetime, timedelta, timezone

            return datetime(day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc) + timedelta(hours=-7)

        def tommy_to_pdex_day_converter(t_d):
            from datetime import datetime, timedelta, timezone

            return datetime(t_d.year, t_d.month, t_d.day, t_d.hour) + timedelta(hours=7)

        def tommy_to_pdex_hour_converter(t_d):
            from datetime import datetime, timedelta, timezone

            return (datetime(t_d.year, t_d.month, t_d.day, t_d.hour) + timedelta(hours=7)).hour

        def pdex_to_xdf_converter(day_in_utc, hour_in_utc):
            from datetime import datetime, timedelta, timezone

            return datetime(day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc) + timedelta(hours=-7)

        def output_to_tommy_external_reverse_link(o_day, o_hour):
            from datetime import datetime

            return datetime(o_day.year, o_day.month, o_day.day, o_hour)

        tommy_data_node = app.create_data(
            id=f"C2P_TOMMY_DATA_{region}_{marketplace_id}",
            inputs={
                "tommy_external": tommy_external["US"]["*"]["*"].ref.range_check(True),
                "digital_external": dama[region_id].ref,
                "xdf_external": xdf["NA"][:-14].nearest(),
                "pdex_consolidated": pdex_consolidated[region_id][marketplace_id]["*"]["*"],
            },
            input_dim_links=[
                # Tommy and Tommy derivatives are partitioned by day/hour in local time, so we need to convert.
                # Conversion relies on semantics in the pdex_consolidated node guaranteeing utc times, and is thus fragile
                (tommy_external("partition_date"), pdex_to_tommy_converter, pdex_consolidated("day", "hour")),
                (tommy_external("partition_hour"), pdex_to_tommy_converter, pdex_consolidated("day", "hour")),
                # provide reverse links for testing convenience (from console.py or in the notebooks) when tommy external
                # is provided as seed for execute or process APIs.
                (
                    pdex_consolidated("day"),
                    tommy_to_pdex_day_converter,
                    tommy_external("partition_hour"),
                ),
                (
                    pdex_consolidated("hour"),
                    tommy_to_pdex_hour_converter,
                    tommy_external("partition_hour"),
                ),
                (xdf("day"), pdex_to_xdf_converter, pdex_consolidated("day", "hour")),
            ],
            output_dimension_spec={
                "region_id": {
                    type: DimensionType.LONG,
                    "marketplace_id": {
                        type: DimensionType.LONG,
                        "day": {"format": "%Y-%m-%d", type: DimensionType.DATETIME, "hour": {type: DimensionType.LONG}},
                    },
                }
            },
            output_dim_links=[
                ("region_id", EQUALS, region_id),
                ("marketplace_id", EQUALS, marketplace_id),
                ("day", EQUALS, tommy_external("partition_date")),
                ("hour", lambda hour_dt: hour_dt.hour, tommy_external("partition_hour")),
                # define the reverse link for testing and debugging convenience
                #  output("day", "hour") -> tommy_external("hour")
                (tommy_external("partition_hour"), output_to_tommy_external_reverse_link, ("day", "hour")),
            ],
            compute_targets=[
                BatchCompute(
                    code="""PYSPARK CODE HERE""",
                    lang=Lang.PYTHON,
                    GlueVersion="1.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=150,
                    Timeout=4 * 60,  # 4 hours
                )
            ],
            dataset_format=DataFormat.PARQUET,
            auto_input_dim_linking_enabled=False,
        )

        c2p_data_node = app.create_data(
            id=f"C2P_DATA_{region.upper()}_{marketplace_id}",
            inputs={
                # Dummy input to to have read only when 24th partition is ready.
                "dummy_last_hour_condition": tommy_data_node[region_id][marketplace_id]["*"][23],
                "tommy_data_digital": tommy_data_node[region_id][marketplace_id]["*"][:-24].range_check(True),
            },
            output_dimension_spec={
                "region_id": {
                    type: DimensionType.LONG,
                    "marketplace_id": {
                        type: DimensionType.LONG,
                        "day": {type: DimensionType.DATETIME, "granularity": DatetimeGranularity.DAY, "format": "%Y-%m-%d"},
                    },
                }
            },
            output_dim_links=[
                ("region_id", EQUALS, region_id),
                ("marketplace_id", EQUALS, marketplace_id),
                ("day", EQUALS, tommy_data_node("day")),
            ],
            compute_targets=[
                BatchCompute(
                    code="output=C2P_TOMMY_DATA_1_1",
                    lang=Lang.PYTHON,
                    GlueVersion="4.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=25,
                    Timeout=8 * 60,
                )
            ],
            dataset_format=DataFormat.PARQUET,
        )

        app.activate()

        # CREATE LOCAL ORCHESTRATION
        orchestration_cycle_time_in_secs = 10
        self.activate_event_propagation(app, cycle_time_in_secs=orchestration_cycle_time_in_secs)

        # patch BatchCompute
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return ComputeSuccessfulResponse(
                ComputeSuccessfulResponseType.PROCESSING,
                ComputeSessionDesc(
                    f"job_{materialized_output.get_materialized_resource_paths()[0]}", ComputeResourceDesc("job_name", "job_arn")
                ),
            )

        # patch BatchCompute::get_session_state in such a way that it'll provide a chaotic execution env
        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            from random import uniform

            new_session_desc = (
                ComputeSessionDesc(
                    f"job_{active_compute_record.materialized_output.get_materialized_resource_paths()[0]}",
                    ComputeResourceDesc("job_name", "job_arn"),
                )
                if not session_desc
                else session_desc
            )

            chaos = uniform(0, 1.0)
            if chaos < 0.33:
                return ComputeFailedSessionState(
                    ComputeFailedSessionStateType.TRANSIENT,
                    new_session_desc,
                    [ComputeExecutionDetails("<start_time>", "<end_time>", dict())],
                )
            elif chaos < 0.66:
                return ComputeSessionState(
                    new_session_desc,
                    ComputeSessionStateType.COMPLETED,
                    [ComputeExecutionDetails("<start_time>", "<end_time>", dict())],
                )
            else:
                return ComputeSessionState(
                    new_session_desc,
                    ComputeSessionStateType.PROCESSING,
                    [ComputeExecutionDetails("<start_time>", "<end_time>", dict())],
                )

        def get_max_wait_time_for_next_retry_in_secs() -> int:
            return 30

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        for driver in app.platform.batch_compute._drivers:
            driver.get_max_wait_time_for_next_retry_in_secs = MagicMock(side_effect=get_max_wait_time_for_next_retry_in_secs)

        # app.execute(tommy_data_node[1][1]["2022-04-11"][1], wait=True, recursive=True)

        date = datetime(2022, 4, 11)

        # now emulate external data readiness
        for i in range(24):
            tommy_date = date + timedelta(hours=i)
            add_test_data(app, tommy_external["us"][tommy_date][tommy_date], object_name="_SUCCESS", data="")

        for i in range(24):
            pdex_date = date + timedelta(hours=7 + i)
            add_test_data(app, pdex_external["NA"][pdex_date], object_name="_SUCCESS", data="")

        add_test_data(app, dama[1], object_name="_SUCCESS", data="")
        add_test_data(app, xdf["NA"][date], object_name="_SUCCESS", data="")

        # start recursive execution
        app.execute(c2p_data_node[1][1][date], wait=False, recursive=True)

        # now track the status of executions starting from the root level
        # all of the "pdex_consolidated" executions should be in parallel
        for i in range(24):
            pdex_date = date + timedelta(hours=7 + i)
            path, _ = app.poll(pdex_consolidated[1][1][pdex_date][pdex_date.hour])
            assert path, f"pdex_consolidated execution for date {str(pdex_date)} could not be started!"

        for i in range(24):
            tommy_date = date + timedelta(hours=i)
            path, _ = app.poll(tommy_data_node[1][1][tommy_date][tommy_date.hour])
            assert path

        # and now finally check the target of recursive execution
        path, _ = app.poll(c2p_data_node[1][1][date])
        assert path

        self.patch_aws_stop()
