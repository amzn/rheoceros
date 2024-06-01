# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# This example demonstrates a simple data-pipeline that makes a hybrid use of big-data technologies on different nodes;
#    - show external (S3) data ingestion using PrestoSQL
#    - show intermediate ETL using PrestoSQL (both input and output are internal datasets)
#      - show how PrestoSQL output auto-triggers downstream nodes and is used by them.
#
#   - show how nodes with different big-data technologies are seamlessly bound together using same high-level IF APIs.
#
#   The design of this example is based on other two examples: 'advanced_input_modes' and 'advanced_input_modes_nearest'
#   to cover advanced input configuration with PrestoSQL based nodes.
#   Then finally a new node depend on one of those PrestoSQL based nodes.
#
import time

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

from intelliflow.core.signal_processing.dimension_constructs import StringVariant

flow.init_basic_logging()

region = "us-east-1"
app_name = "andes-ant-app"

# automatically reads default credentials
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
app = AWSApplication(app_name, region)

# first S3 signal
eureka_offline_training_data = app.add_external_data(
    data_id="eureka_training_data",
    s3_dataset=S3("1111111111111",
                  "<model-training-data-bucket>",
                  "eureka_p3/v8_00/training-data",
                  StringVariant('NA', 'region'),
                  AnyDate('day', {'format': '%Y-%m-%d'})))

# second S3 signal
eureka_offline_all_data = app.marshal_external_data(
    S3Dataset("111111111111","<TRAINING_DATA_BUCKET>", "eureka_p3/v8_00/all-data-prod","partition_day={}", dataset_format=DataFormat.CSV)
    , "eureka_training_all_data"
    , {
        'day': {
            'type': DimensionType.DATETIME,
            'format': '%Y-%m-%d'
        }
    }
    , {
        "*": {
        }
    },
    SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)


default_selection_features_SPARK = app.create_data(id='eureka_default', # data_id
                                             inputs={
                                                 "offline_training_data": eureka_offline_training_data,
                                                 "offline_data": eureka_offline_all_data[:-2].no_event
                                             },
                                             compute_targets=[ # when inputs are ready, trigger the following
                                                 Glue(
                                                     "output = offline_data.unionAll(offline_training_data).limit(10)",
                                                     GlueVersion="2.0",
                                                     WorkerType=GlueWorkerType.G_1X.value,
                                                     NumberOfWorkers=50
                                                 )
                                             ])


default_selection_features_PRESTOSQL = app.create_data(id='eureka_default_presto',
                                                   inputs={
                                                       "offline_training_data": eureka_offline_training_data,
                                                       "offline_data": eureka_offline_all_data[:-2].no_event
                                                   },
                                                   compute_targets=[
                                                       PrestoSQL(
                                                          """
                                                          SELECT * FROM offline_training_data
                                                          UNION ALL
                                                          SELECT *, '1' as region FROM offline_data
                                                          """,
                                                       )
                                                   ])

default_selection_features_PRESTOSQL_LAYER2 = app.create_data(id='eureka_default_presto_limited',
                                                       inputs=[default_selection_features_PRESTOSQL],
                                                       compute_targets=[
                                                           PrestoSQL(
                                                               """
                                                               SELECT * FROM
                                                                    eureka_default_presto
                                                               LIMIT 10
                                                               """,
                                                           )
                                                       ])


pick_latest_from_eureka_PRESTO = app.create_data(id='latest_eureka_presto',
                                          inputs={
                                              "independent_signal": eureka_offline_all_data,
                                              # this input only has the following partitions
                                              #  - 2021-03-18
                                              #  - 2021-03-17
                                              #  - 2021-03-16
                                              # we are going to exploit this in the following execute calls to check
                                              # 'nearest' functionality
                                              "offline_training_data": eureka_offline_training_data['*'][:-14].nearest()  # or latest()
                                          },
                                          compute_targets=[
                                              PrestoSQL("SELECT * from offline_training_data LIMIT 10")
                                          ])


# validate and convert to CSV (from PARQUET)
pick_latest_from_eureka_PRESTO_VALIDATION_IN_SPARK = app.create_data(id='validate_latest_eureka',
                                                 inputs=[pick_latest_from_eureka_PRESTO],
                                                 compute_targets=[ # when inputs are ready, trigger the following
                                                     Glue(
                                                         """
output = latest_eureka_presto
                                                         """,
                                                         GlueVersion="2.0",
                                                         WorkerType=GlueWorkerType.G_1X.value,
                                                         NumberOfWorkers=10
                                                     )
                                                 ],
                                                 dataset_format=DatasetSignalSourceFormat.CSV)


## DEBUG BEGIN
#app.activate()
## Disable remote orchestration and debug everything locally
## Comment out till 'DEBUG END' once done
#app.pause()
#
#app.process(eureka_offline_training_data['NA']['2021-03-18'], target_route_id=default_selection_features_PRESTOSQL)
#
## emulate Processor cycles
#while True:
#    app.update_active_routes_status()
### DEBUG END

# direct execution
# test the 1st PrestoSQL node (ingestion node)
path = app.execute(default_selection_features_PRESTOSQL['NA']['2021-03-18'])
assert path  # should succeed

time.sleep(5)

# test auto-trigger of 2nd layer node (also PrestoSQL)
path, _ = app.poll(default_selection_features_PRESTOSQL_LAYER2['NA']['2021-03-18'])
assert path

# should succeed since 2021-03-18 is within 14 days of range from 2021-03-20
# please note that 'nearest' input 'offline_training_data' does not have that partition so during the execution
# it should pick up 2021-03-18 only.
path = app.execute(pick_latest_from_eureka_PRESTO['2021-03-20'])
assert path  # should succeed
try:
    # execute is different than runtime behaviour, it won't keep waiting for the 'nearest=True' signal.
    # relies on compute failure if no data is not found within the range.
    # (however in normal execution, orchestration waits for the completion of a partition within the user provided range).
    app.execute(pick_latest_from_eureka_PRESTO['2021-04-20'])
    assert False
except:
    pass

time.sleep(5)
path, _ = app.poll(pick_latest_from_eureka_PRESTO_VALIDATION_IN_SPARK['2021-03-20'])
assert path

# Emulate runtime signal processing using 2021-03-19 partition
# Emulate incoming signal on 'independent signal' [2021-03-19]
# Even if the second ('nearest') input did not receive an event yet, the execution will begin thanks to the existence of
# partition 2021-03-18 on it.
app.process(eureka_offline_all_data['2021-03-19'])

time.sleep(5)
# hook up with the execution on '2021-03-19' which must be using 2021-03-18 on the 'nearest' input.
# yeah, the execution must have started without waiting for the second input since it is 'nearest' and have a partition
# within the expected range.
path, _ = app.poll(pick_latest_from_eureka_PRESTO['2021-03-19'])
assert path

# test 2nd layer node (Spark)
time.sleep(5)
path, _ = app.poll(pick_latest_from_eureka_PRESTO_VALIDATION_IN_SPARK['2021-03-19'])
assert path

app.pause()

