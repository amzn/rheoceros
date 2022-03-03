# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time
from datetime import datetime, timedelta

import pytest

from intelliflow.api_ext import *
from intelliflow.core.platform.definitions.aws.s3.object_wrapper import object_exists
from intelliflow.core.signal_processing.dimension_constructs import StringVariant
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.data_emulation import add_test_data
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationAdvancedDimensionLinking(AWSTestBase):
    """
    This test suite was created to cover edge-cases, advanced use-cases in input-input, input-output, output-input
    dimension linking.
    """

    def test_application_advanced_dimension_linking_multiple_dimensions_to_single_input_dimension(self):
        """
        Case 1:
         - Test N->1 mapping from an input to another input.
         - Test N->1 mapping from an output to another input.
        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("n_to_1_mapping", self.region)

        ext_data_1 = app.marshal_external_data(
            external_data_desc=S3Dataset("111222333444", "bucket", "foo", "{}", "{}").link(
                GlueTable(database="my_database", table_name="foo")
            ),
            id="external_data1",
            dimension_spec={
                "day": {
                    "type": DimensionType.DATETIME,
                    "format": "%Y%m%d",  # declare how it is used in the bucket (e.g '20211122')
                    "hour": {"type": DimensionType.LONG},
                }
            },
        )

        ext_data_2 = app.marshal_external_data(
            external_data_desc=S3Dataset("111222333444", "bucket", "bar", "{}"),
            id="external_data2",
            dimension_spec={
                "timestamp": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d %H:%M:%S", "granularity": DatetimeGranularity.HOUR}
            },
        )

        # Visualization of what will happen at runtime:
        #
        #            ext_data_1 ----------> ext_data_2
        #                    \
        #                     \
        #                      \
        #                      _\|
        #                         output
        #
        #  (I have spent at least 5 mins to draw this)
        #
        mapping_1 = app.create_data(
            id="mapping_1",
            inputs=[
                ext_data_1,
                # since this is 'ref', incoming events from thje second input are not
                # required for execution trigger. events from the first input will
                # determine the trigger and now from each event (and its dimension values [day, hour])
                # second dataset partition ('timestamp') must be inferred at runtime. So please check the input_dim_linking to understand how.
                ext_data_2.ref,
            ],
            input_dim_links=[
                # N -> 1 mapping, ext_data_1(day, hour) -> ext_data_2(timestamp)
                (
                    ext_data_2("timestamp"),  # DESTINATION/TO
                    # notice how two dimension [day, hour] declaration on SOURCE
                    # changed the signature of the mapper lambda function here.
                    #
                    # lambda argument types:
                    #  - day <datetime>: since 1st input 'day' dimension is declared as DATETIME,
                    #  - hour <int> : since 1st input 'hour' is declared as LONG above in marshal_external_data call for external_data_1
                    #
                    # ex:
                    #   day: datetime('2021-11-22')
                    #   hour: 2021112213  (last two digits represent the hour)
                    lambda day, hour: day + timedelta(hours=int(str(hour)[8:])),
                    # MAPPER/OPERATOR
                    ext_data_1("day", "hour"),  # SOURCE/FROM
                )
            ],
            compute_targets=[NOOPCompute],  # do nothing
        )

        # now declare it in a way that output uses second input's dimension
        #
        #            ext_data_1 (day,hour)  -------->  ext_data_2 (timestamp)
        #                                             /
        #                                            /
        #                                           /
        #                            output (timestamp)
        #
        mapping_2 = app.create_data(
            id="mapping_2",
            inputs=[ext_data_1, ext_data_2.ref],
            input_dim_links=[
                (
                    ext_data_2("timestamp"),  # DESTINATION/TO
                    lambda day, hour: day + timedelta(hours=int(str(hour)[8:])),
                    ext_data_1("day", "hour"),  # SOURCE/FROM
                )
            ],
            output_dimension_spec={
                # will be auto-linked to ext_data_2('timestamp')
                # will be determined transitively via
                #    'ext_data_1(day hour) -> ext_data_2(timestamp) -> output(timestamp)'
                "timestamp": {
                    "type": DimensionType.DATETIME,
                    "format": "%Y-%m-%d %H",  # no need to keep the minute and second components
                    "granularity": DatetimeGranularity.HOUR,
                }
            },
            compute_targets=[NOOPCompute],  # do nothing
        )

        # A different way of achieving the same, this time from the output back to the other input.
        #
        #            ext_data_1(day, hour)    ext_data_2(timestamp)
        #                            \               /
        #                             \             /
        #                              \           /
        #                              _\|        /
        #                              output(timestamp)
        #
        mapping_2_1 = app.create_data(
            id="mapping_2_1",
            inputs=[
                # output will get its dimension spec from ext_data_2, and will have only 'timestamp'
                ext_data_2.ref,
                ext_data_1,
            ],
            output_dim_links=[
                (
                    "timestamp",  # Output dimension 'timestamp'
                    lambda day, hour: day + timedelta(hours=int(str(hour)[8:])),
                    ext_data_1("day", "hour"),  # SOURCE/FROM
                )
            ],
            compute_targets=[NOOPCompute],  # do nothing
        )

        # now declare it in a way that the output is implicitly/automatically linked to the first input
        # (since output_dimension_spec is not overwritten), and the second input will be determined from it.
        # runtime linking down to ext_data_2(timestamp) will be like this:
        #    ext_data_1(day, hour)  -> output(timestamp) -> ext_data_2(timestamp)
        #
        # link from output(timestamp) -> ext_data_2(timestamp) is declared within output_dim_links here:
        #            ext_data_1             ext_data_2
        #                    \             /
        #                     \           /
        #                      \         /
        #                      _\|      /
        #                         output
        #
        mapping_3 = app.create_data(
            id="mapping_3",
            inputs=[ext_data_1, ext_data_2.ref],
            output_dim_links=[
                (
                    ext_data_2("timestamp"),  # DESTINATION/TO
                    lambda day, hour: day + timedelta(hours=int(str(hour)[8:])),
                    ("day", "hour"),  # Output dimensions
                )
            ],
            compute_targets=[NOOPCompute],  # do nothing
        )

        app.activate()

        # synthetically inject tommy hourly event into the system
        # Emulate Glue Table partition change event
        # (note: this is an async call)
        app.process(ext_data_1["20211122"]["2021112223"])

        # now verify that all of the routes have been triggered/executed successfully.
        path, compute_records = app.poll(mapping_1["20211122"]["2021112223"])
        assert path

        # dimension value can be anythinf: '2021-11-22T23' or '2021-11-22 23:00:00' or a datetime object
        path, compute_records = app.poll(mapping_2["2021-11-22 23"])
        assert path

        path, compute_records = app.poll(mapping_2_1["2021-11-22 23"])
        assert path

        path, compute_records = app.poll(mapping_3["20211122"]["2021112223"])
        assert path

        self.patch_aws_stop()
