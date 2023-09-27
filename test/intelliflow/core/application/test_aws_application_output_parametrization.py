# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import timedelta

import pytest

from intelliflow.api_ext import *
from intelliflow.core.signal_processing.signal import *
from intelliflow.core.signal_processing.signal_source import DatasetSignalSourceAccessSpec
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationOutputParametrization(AWSTestBase):
    def test_application_output_data_format(self):
        self.patch_aws_start()

        app = AWSApplication("output-data", self.region)

        data1 = app.create_data(
            id="data1", compute_targets="output=spark.sql('select * from DUMMY')", dataset_format=DatasetSignalSourceFormat.PARQUET
        )

        data2 = app.create_data(
            id="data2", compute_targets="output=spark.sql('select * from DUMMY')", data_format=DatasetSignalSourceFormat.PARQUET
        )

        data3 = app.create_data(
            id="data3", compute_targets="output=spark.sql('select * from DUMMY')", data_format=DatasetSignalSourceFormat.CSV
        )

        # signal is what a node is going to dump down to app.platform, so underlying drivers will use it to
        # check output parametrization. in this unit-test we are trying to make sure that app level signal object
        # generation does pass on data format well.
        data1_signal = data1.signal()
        assert isinstance(data1_signal.resource_access_spec, DatasetSignalSourceAccessSpec)
        assert data1_signal.resource_access_spec.data_format == DatasetSignalSourceFormat.PARQUET

        data2_signal = data2.signal()
        assert data2_signal.resource_access_spec.data_format == DatasetSignalSourceFormat.PARQUET

        assert data3.signal().resource_access_spec.data_format == DatasetSignalSourceFormat.CSV
        self.patch_aws_stop()

    def test_application_output_bidirectional_nontrivial_linking(self):
        self.patch_aws_start()

        region_id = 1
        marketplace_id = 1

        app = AWSApplication("adv-output-link", self.region)

        timer = app.add_timer("adex_timer", "rate(5 minutes)", time_dimension_id="time")

        adex_kickoff_job = app.create_data(
            id="ADEX_BladeKickOffJob",
            inputs=[timer],
            input_dim_links=[],
            output_dimension_spec={
                "region_id": {
                    type: DimensionType.LONG,
                    "marketplace_id": {type: DimensionType.LONG, "cutoff_date": {type: DimensionType.DATETIME, "format": "%Y-%m-%d"}},
                }
            },
            output_dim_links=[
                ("region_id", lambda dim: dim, region_id),
                ("marketplace_id", lambda dim: dim, marketplace_id),
                ("cutoff_date", lambda dim: dim - timedelta(days=7), timer("time")),
                # needed if 'execute' API will be used in the most common form where the desired
                # output partition will be provided only (e.g app.execute(adex_kickoff_job[1][1][DATETIME]),
                # which would require output('cutoff_date') to timer('time') mapping/linking.
                (timer("time"), lambda dim: dim + timedelta(days=7), "cutoff_date"),
            ],
            compute_targets=[NOOPCompute],
            dataset_format=DatasetSignalSourceFormat.PARQUET,
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate()

        # 1- test IF automatically materializes the timer input as "2021-06-27" using the output link
        app.execute(adex_kickoff_job[region_id][marketplace_id]["2021-06-20"])
        path, most_recent_exec_records = app.poll(adex_kickoff_job[region_id][marketplace_id]["2021-06-20"])
        # we know that the most recent exec should have only one compute record (since "compute_targets" has one).
        noop_compute_entity = most_recent_exec_records[0]
        materialized_timer_input = noop_compute_entity.materialized_inputs[0]
        assert DimensionFilter.check_equivalence(
            materialized_timer_input.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({"2021-06-27": {}})  # +7 auto-shift !
        )

        # 2- now simulate runtime situation, where timer input triggers the execution and output gets materialized
        #  with -7 shift.
        # use local Processor to avoid eventual consistency into execution, otherwise poll would miss the execution
        app.process(timer["2021-06-28"], with_activated_processor=False)
        path, most_recent_exec_records = app.poll(adex_kickoff_job[region_id][marketplace_id]["2021-06-21"])
        assert path
        noop_compute_entity = most_recent_exec_records[0]
        assert DimensionFilter.check_equivalence(
            noop_compute_entity.materialized_output.domain_spec.dimension_filter_spec,
            DimensionFilter.load_raw({region_id: {marketplace_id: {"2021-06-21": {}}}}),  # -7 auto-shift !
        )

        self.patch_aws_stop()
