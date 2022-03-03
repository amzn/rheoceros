# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time
from datetime import datetime

import pytest

from intelliflow.api_ext import *
from intelliflow.core.application.application import Application
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.data_emulation import add_test_data
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationExternalDataLinking(AWSTestBase):

    app: Application = None

    def test_application_external_data_linking(self):
        def invoke_lambda_function(lambda_client, function_name, function_params, is_async=True):
            """Synchronize internal async lambda invocations so that the chaining would be active during testing."""
            return self.app.platform.processor.process(function_params, use_activated_instance=False)

        self.patch_aws_start(glue_catalog_has_all_tables=True, invoke_lambda_function_mock=invoke_lambda_function)

        self.app = AWSApplication("i-fanout", self.region)

        encryption_key = "arn:aws:kms:us-east-1:111222333444:key/aaaaaaaa-bbbb-4444-cccc-666666666666"
        search_tommy_data = self.app.marshal_external_data(
            external_data_desc=S3Dataset(
                "111222333444",
                "a9-sa-data-tommy-datasets-prod",
                "tommy-asin-parquet-hourly",
                "{}",
                "{}",
                "{}",
                dataset_format=DataFormat.PARQUET,
                encryption_key=encryption_key,
            ).link(
                # Link the PROXY !!!
                # events coming from this proxy/link will yield valid Signals/triggers in the system
                GlueTable(database="searchdata", table_name="tommy_hourly")
            ),
            id="tommy_external",
            dimension_spec={
                "org": {
                    "type": DimensionType.STRING,
                    "format": LOWER,  # final value for this partition should always be lower case
                    INSENSITIVE: True,  # incoming events might be upper case, so relax routing logic.
                    "day": {
                        "type": DimensionType.LONG,
                        "hour": {
                            "type": DimensionType.LONG,
                        },
                    },
                }
            },
            dimension_filter={
                "Se": {  # this is just a seed (raw value). final state is determined after spec (formatting) above gets applied.
                    "*": {"*": {}}
                }
            },
            # Extra trigger source in case search team decides to add hadoop completion file with or without Glue partition
            # updates. RheocerOS idempotency will take care repetitive messages and in case of long durations in between
            # those messages, the worst case scenario would be a redundant execution.
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        search_data_reactor_node = self.app.create_data(id="SEARCH_DATA_REACTOR", inputs=[search_tommy_data], compute_targets=[NOOPCompute])

        run_when_search_data_verified = self.app.create_data(
            id="WHEN_SEARCH_DATA_VERIFIED", inputs=[search_data_reactor_node, search_tommy_data], compute_targets=[NOOPCompute]
        )

        self.app.activate()

        # Emulate Glue Table partition change event.
        # Synchronously process (without async remote call to Processor [Lambda])
        self.app.process(
            {
                "version": "0",
                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "detail-type": "Glue Data Catalog Table State Change",
                "source": "aws.glue",
                "account": "427809481713",
                "time": "2021-03-08T18:57:01Z",
                "region": "us-east-1",
                "resources": ["arn:aws:glue:us-east-1:427809481713:table/searchdata/tommy_hourly"],
                "detail": {
                    "databaseName": "searchdata",
                    "changedPartitions": ["[SE, 20210308, 2021030817]"],
                    "typeOfChange": "BatchCreatePartition",
                    "tableName": "tommy_hourly",
                },
            }
        )

        path, compute_records = self.app.poll(search_data_reactor_node["se"]["20210308"]["2021030817"])
        assert path

        path, compute_records = self.app.poll(run_when_search_data_verified["se"]["20210308"]["2021030817"])
        assert path

        # change the 'typeOfChange' event type to 'DeletePartition' and prove that it won't trigger.
        self.app.process(
            {
                "version": "0",
                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "detail-type": "Glue Data Catalog Table State Change",
                "source": "aws.glue",
                "account": "427809481713",
                "time": "2021-03-09T18:57:01Z",
                "region": "us-east-1",
                "resources": ["arn:aws:glue:us-east-1:427809481713:table/searchdata/tommy_hourly"],
                "detail": {
                    "databaseName": "searchdata",
                    "changedPartitions": ["[SE, 20210309, 2021030917]"],
                    "typeOfChange": "DeletePartition",
                    "tableName": "tommy_hourly",
                },
            }
        )

        path, _ = self.app.poll(search_data_reactor_node["se"]["20210309"]["2021030917"])
        assert not path

        path, _ = self.app.poll(run_when_search_data_verified["se"]["20210309"]["2021030917"])
        assert not path

        self.patch_aws_stop()

    def test_application_external_data_linking_with_multiple_inputs(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        self.app = AWSApplication("i-fanout", self.region)

        encryption_key = "arn:aws:kms:us-east-1:111222333444:key/aaaaaaaa-bbbb-4444-cccc-666666666666"
        search_tommy_data = self.app.marshal_external_data(
            external_data_desc=S3Dataset(
                "111222333444", "a9-sa-data-tommy-datasets-prod", "tommy-asin-parquet-hourly", "{}", "{}", "{}"
            ).link(
                # events coming from this proxy/link will yield valid Signals/triggers in the system
                GlueTable(database="searchdata", table_name="tommy_hourly")
            ),
            id="tommy_external",
            dimension_spec={
                "org": {
                    "type": DimensionType.STRING,
                    "format": LOWER,  # final value for this partition should always be lower case
                    INSENSITIVE: True,  # incoming events might be upper case, so relax routing logic.
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%Y%m%d",
                        "hour": {
                            "type": DimensionType.DATETIME,  # Use DATETIME on 'hour' this time
                            "format": "%Y%m%d%H",  # specify the format otherwise IF will not be able to parse YYYYMMMDDHH to datetime
                        },
                    },
                }
            },
            dimension_filter={
                "Se": {  # this is just a seed (raw value). final state is determined after spec (formatting) above gets applied.
                    "*": {"*": {}}
                }
            },
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        reference_data = self.app.marshal_external_data(
            external_data_desc=S3Dataset("111222333444", "pdex-data", "raw_data", "{}"),
            id="pdex",
            dimension_spec={"dataset_date": {"type": DimensionType.DATETIME}},
        )

        tommy_hourly_digits = 2

        tommy_hourly = self.app.create_data(
            id="TOMMY_HOURLY",
            inputs=[search_tommy_data, reference_data.ref.range_check(True)],
            input_dim_links=[
                # if pdex partition was named as 'hour', then we would not need this while auto_input_dim_linking=True
                (reference_data("dataset_date"), EQUALS, search_tommy_data("hour"))
            ],
            compute_targets=[NOOPCompute],
            output_dimension_spec={
                "org": {
                    "type": DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%Y-%m-%d",
                        "hour": {"type": DimensionType.LONG, "digits": tommy_hourly_digits},  # e.g '01', '23'
                    },
                }
            },
            output_dim_links=[("hour", lambda search_h: search_h.hour, search_tommy_data("hour"))],
        )

        # this is the preffered / most optimized way of creating this node (check the next node TOMMY_DAILY_2 for
        # comparison.
        tommy_daily = self.app.create_data(
            id="TOMMY_DAILY",
            inputs={"_wait_for_last_hour": tommy_hourly["*"]["*"][23], "tommy_data": tommy_hourly["*"]["*"][:-24].ref.range_check(True)},
            compute_targets=[NOOPCompute],
            output_dimension_spec={"org": {"type": DimensionType.STRING, "day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}}},
        )

        # create another (but equivalent) tommy_daily node with different runtime/orchestration behaviour (mem/perf),
        # and also with a different day dimension format. 'day' dimension is intentionally different to make sure
        # whether IF uses user provided format to cope with 'bad' datetime formats in marshalling/fileringview __getitem__
        # inputs (dimension values)
        tommy_daily_2 = self.app.create_data(
            id="TOMMY_DAILY_2",
            inputs={
                "_trigger_signal": tommy_hourly["*"]["*"][23],
                # in the absences of 'ref', each incoming tommy_hourly signal
                #  will create a PendingNode at runtime. 23 of those nodes will be zombies
                # and will never get triggered. They will expire based on 'pending_node_expiration_ttl_in_secs'.
                # IF is already able to dected those nodes, but currently (as of 12/2021) does not take
                # any action (garbage-collection) on them still relies on 'pending_node_expiration_ttl_in_secs'.
                "tommy_data": tommy_hourly["*"]["*"][:-24],
            },
            compute_targets=[NOOPCompute],
            output_dimension_spec={
                "org": {
                    "type": DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        # different datetime format which is normally not supported by dateutil (due to '-' between hour and minute)
                        # thanks to this format, now when we do
                        #    tommy_daily_2['us']['2021-05-06 23-00']
                        # IF will be able to parse 'day' dimension.
                        "format": "%Y-%m-%d %H-%M",
                    },
                }
            },
        )

        # the first assertion would have failed if 'format' was not provided for 'day' dimension, because this datetime
        # string is not valid for us when purely relying on dateutil. But IF fallbacks to user provided format and 'strptime'
        # to see if the string can be evaluated the way user wants and be converted to a datetime object.
        assert self.app.materialize(tommy_daily_2["se"]["2021-05-06 23-00"])[0].endswith("TOMMY_DAILY_2/se/2021-05-06 23-00")
        # the rest would succeed even without the format (but final path would be in '%Y-%m-%d %H:%M%:S' by default)
        assert self.app.materialize(tommy_daily_2["se"]["2021-05-06 23:00"])[0].endswith("TOMMY_DAILY_2/se/2021-05-06 23-00")
        assert self.app.materialize(tommy_daily_2["se"]["2021-05-06 23:00:00"])[0].endswith("TOMMY_DAILY_2/se/2021-05-06 23-00")
        assert self.app.materialize(tommy_daily_2["se"][datetime(2021, 5, 6, 23)])[0].endswith("TOMMY_DAILY_2/se/2021-05-06 23-00")

        with pytest.raises(ValueError):
            # S -> capital, tommy_external's dimension filter on 'org' (which enforces 'lower' as format) is auto adapted
            # by tommy_daily_2: so the result is a value error at build-time on 'Se' being rejected by filtering.
            # only acceptable value is 'se' naturally.
            self.app.materialize(tommy_daily_2["Se"]["2021-05-06 23-00"])

        self.app.activate()

        orchestration_cycle_time_in_secs = 15
        self.activate_event_propagation(self.app, cycle_time_in_secs=orchestration_cycle_time_in_secs)

        # first make sure that reference_data is ready (make sure that the partition is in the source bucket)
        for hour in range(24):
            # add partition
            add_test_data(self.app, reference_data[f"2021-03-08T{str(hour).zfill(tommy_hourly_digits)}"], "DUMMY", "")
            # we don't need to inject/emulate event for reference_data because it is used 'as reference' in tommy_hourly
            # self.app.process(reference_data[f'2021-03-08T{str(hour).zfill(tommy_hourly_digits)}'])

        # Emulate Glue Table partition change event for an entire day
        # Each event injection (process API below) should trigger an execution on tommy_hourly since other input
        # 'reference_data' (pdex) is already satisfied for the entire day.
        for hour in range(24):
            self.app.process(
                {
                    "version": "0",
                    "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                    "detail-type": "Glue Data Catalog Table State Change",
                    "source": "aws.glue",
                    "account": "427809481713",
                    "time": "2021-03-08T18:57:01Z",
                    "region": "us-east-1",
                    "resources": ["arn:aws:glue:us-east-1:427809481713:table/searchdata/tommy_hourly"],
                    "detail": {
                        "databaseName": "searchdata",
                        "changedPartitions": [f"[SE, 20210308, 20210308{str(hour).zfill(tommy_hourly_digits)}]"],
                        "typeOfChange": "BatchCreatePartition",
                        "tableName": "tommy_hourly",
                    },
                }
            )

        # make sure that all of the tommy_hourly partitions are created
        for hour in range(24):
            # intentionally show different formats for 'day', 'hour' dimension to check DimensionVariant resolve capabilities
            path, _ = self.app.poll(tommy_hourly["se"]["20210308"][hour])
            assert path.endswith(f"TOMMY_HOURLY/se/2021-03-08/{str(hour).zfill(tommy_hourly_digits)}")
            # in local test we don't generate the actual partition so let's emulate the creation of partition now,
            # after making sure that orchestration was successful.
            # this is required because in this test we use range_check(True) on tommy_hourly with 24 hours range in
            # tommy_daily.
            add_test_data(self.app, tommy_hourly["se"]["2021-03-08"][str(hour).zfill(5)], "_SUCCESS", "")

        # we need this sleep in this local test because we rely on asynchronous dependency check on the second input of
        # tommy daily which relies on 24 hours of partitions as a reference. those partitions are put into storage in
        # above loop.
        time.sleep(orchestration_cycle_time_in_secs + 1)

        # now check the auto-trigger on downstream tommy_daily node
        path, compute_records = self.app.poll(tommy_daily["se"]["2021-03-08"])
        assert path

        path, compute_records = self.app.poll(tommy_daily_2["se"]["2021-03-08"])
        assert path

        self.patch_aws_stop()
