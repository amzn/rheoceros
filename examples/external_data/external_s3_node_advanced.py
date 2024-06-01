# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from datetime import datetime

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
import logging

logger = flow.init_basic_logging()

app = AWSApplication("i-fanout", "us-east-1")

try:
    app.marshal_external_data(
        S3Dataset("427809481713",
                  "<BUCKET_NAME>",
                  "datanet_data/transit_time_map",
                   dataset_format=DataFormat.CSV,
                  delimiter="|")
        , "transit_time_map_2020"
    )
    assert False, "should fail due to missing dimension_spec"
except ValueError:
    pass

# show that import will succeed, but we won't use this external data in this example.
app.marshal_external_data(
    S3Dataset("427809481713",
              "<BUCKET_NAME>",
              "datanet_data/transit_time_map",
              dataset_format=DataFormat.CSV,
              delimiter="|")
    , "transit_time_map_2020"
    , dimension_spec= {}  # no partitions/dimensions
)


# Import the following external, raw datasource
# https://s3.console.aws.amazon.com/s3/buckets/<BUCKET_NAME>?region=us-east-1&prefix=adpds-output-data/1/2021/09/15/&showversions=false
adpd_shadow_json_AS_SIGNAL = app.marshal_external_data(
    external_data_desc= S3Dataset("427809481713", "<BUCKET_NAME>", "adpds-output-data", "{}", "{}", "{}", "{}",
                                  dataset_format=DataFormat.JSON)
    # in spark code, this will be the default DataFrame name/alias (but you can overwrite it in 'inputs' if you use map instead of list)
    , id="adpd_shadow_data"
    , dimension_spec={
        'region': {
            'type': DimensionType.LONG,  # 1, 3 , 5
            'year': {
                'type': DimensionType.DATETIME,
                'granularity': DatetimeGranularity.YEAR,  # important if you do range operations in 'inputs' (like read multiple years)
                'format': '%Y',  # year, remember from %Y%m%d, this should output 2021
                'month': {
                    # unfortunately we cannot use DATETIME with this since we avoid inferring a two digit as datetime representing month at runtime.
                    # IF will infer '09' as 9th day of month.
                    'type': DimensionType.STRING,
                    'day': {
                        'type': DimensionType.DATETIME,
                        'granularity': DatetimeGranularity.DAY,
                        'format': '%d'  # day, remember from %Y%m%d, this should output 29
                    }
                }
            }
        }
    }
    # OPTIONAL
    , dimension_filter={
        "*": {  # if you want to pre-filter (so that in this notebook anything else than '1' should not be used, then add it here)
            "*": {  # any year
                "*": {  # any month
                    "*": {  # any day
                    }
                }
            }
        }
    },
    # ADPD Prod data currently does not use any 'completion' file.
    # But declaring this will at least eliminat any update from the upstream partition causing event processing/trigger in this pipeline.
    protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)

adpd_shadow_json_processed = app.create_data("adpd_shadow_json2",
                                 inputs=[adpd_shadow_json_AS_SIGNAL],
                                 compute_targets=[
                                     BatchCompute(
                                         # adpd_shadow_data alias comes from signal decl( the 'id'), you can overwrite it above using a map instead of an array
                                         # 'adpd_shadow_data' will contain daily adpd shadow prod data only because no relative range has defined as the last dimension.
                                         code=f"""
output = spark.sql("select * from adpd_shadow_data").limit(1)
                                         """,
                                         lang=Lang.PYTHON,
                                         GlueVersion="2.0",
                                         WorkerType=GlueWorkerType.G_1X.value,
                                         NumberOfWorkers=50,
                                         Timeout=10 * 60  # 10 hours
                                     )
                                 ])
# This example shows that using SparkSQL API, one can input multiple statements which will be executed sequentially
adpd_shadow_json_SparkSql = app.create_data("adpd_shadow_json3",
                                 inputs=[adpd_shadow_json_AS_SIGNAL],
                                 compute_targets=[
                                     SparkSQL(
                                         # adpd_shadow_data alias comes from signal decl( the 'id'), you can overwrite it above using a map instead of an array
                                         # 'adpd_shadow_data' will contain daily adpd shadow prod data only because no relative range has defined as the last dimension.
                                         f"""
CREATE TEMP VIEW TEST_VIEW AS (
    select *
    from adpd_shadow_data
    where FC = 'DFW7'
);
select * from TEST_VIEW limit 10
                                         """,
                                         GlueVersion="2.0",
                                         WorkerType=GlueWorkerType.G_1X.value,
                                         NumberOfWorkers=50,
                                         Timeout=10 * 60  # 10 hours
                                     )
                                 ])

daily_timer = app.add_timer("daily_timer",
                            "rate(1 day)",
                            time_dimension_id="day")

adpd_shadow_json_processed_SCHEDULED = app.create_data("adpd_shadow_json3",
                                 inputs=[daily_timer,
                                         adpd_shadow_json_AS_SIGNAL
                                         #adpd_shadow_json_AS_SIGNAL[1]['*']['*'][:-14]
                                         ],
                                 input_dim_links=[
                                     (adpd_shadow_json_AS_SIGNAL('year'), EQUALS, daily_timer('day')),
                                     # e.g '2021-09-12 00:00'<datetime> -> '09'<string>
                                     (adpd_shadow_json_AS_SIGNAL('month'), lambda day: day.strftime("%m"), daily_timer('day')),
                                     # the following is actually redundant since IF auto-links same dim names
                                     (adpd_shadow_json_AS_SIGNAL('day'), EQUALS, daily_timer('day'))
                                 ],
                                 compute_targets=[
                                     BatchCompute(
                                         # adpd_shadow_data alias comes from signal decl( the 'id'), you can overwrite it above using a map instead of an array
                                         # 'adpd_shadow_data' DataFrame will hold 14 days of data thanks to -14 relative input in above 'inputs' array.
                                         code=f"""
output = spark.sql("select * from adpd_shadow_data").limit(1)
                                         """,
                                         lang=Lang.PYTHON,
                                         GlueVersion="2.0",
                                         WorkerType=GlueWorkerType.G_1X.value,
                                         NumberOfWorkers=50,
                                         Timeout=10 * 60  # 10 hours
                                     )
                                 ])
app.execute(adpd_shadow_json_processed_SCHEDULED["2021-09-21"], wait=True)

# please not that 'execute' will auto-activate the app if it needs to be activated (in case of topology, node semantics change)
output_path = app.execute(adpd_shadow_json_processed[1]['2021']['09']['15'])
# OR
# day = datetime(2021, 9, 15)
# output_path = app.execute(adpd_shadow_json_processed[1][day][day.strftime('%m')][day])

# this will use adpd_shadow from 2021/09/15 implicitly
# input_dim_links above will get year and month from daily timer automatically
output_path = app.execute(adpd_shadow_json_processed_SCHEDULED[datetime(2021, 9, 15)])
# OR
# app.execute(adpd_shadow_json_processed_SCHEDULED["2021-09-15"])
