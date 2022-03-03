# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# This example covers advanced input mode 'nearest' (or 'latest').
#
# 'nearest' flag on an input signal tells RheocerOS to scan its range and keeps it blocked until a resource
# is available. Then during the execution, RheocerOS is expected to use the resource (e.g partition) closest to
# the execution dimensions (the 'tip' of the trigger group, e.g execution date).
import intelliflow.api_ext as flow
from intelliflow.api_ext import *

from intelliflow.core.signal_processing.dimension_constructs import StringVariant

flow.init_basic_logging()

# automatically reads default credentials
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
app = AWSApplication("advanced-inputs", "us-east-1")

eureka_offline_training_data = app.add_external_data(
    data_id="eureka_training_data",
    s3_dataset=S3("111222333444",
                  "model-training-data-bucket", # bucket
                  "cradle_eureka_p3/v8_00/training-data", # folder prefix till partitions
                  StringVariant('NA', 'region'),
                  AnyDate('day', {'format': '%Y-%m-%d'})))

eureka_offline_all_data = app.marshal_external_data(
    S3Dataset("111222333444","model-training-data-bucket", "cradle_eureka_p3/v8_00/all-data-prod","partition_day={}", dataset_format=DataFormat.CSV)
    , "eureka_training_all_data"
    , dimension_spec={
        'day': {
            'type': DimensionType.DATETIME,
            'format': '%Y-%m-%d'
        }
    }
    , dimension_filter={
        "*": {
        }
    },
    protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)


pick_latest_from_eureka = app.create_data(id='latest_eureka',
                                          inputs={
                                              "independent_signal": eureka_offline_all_data,
                                              # this input only has the following partitions
                                              #  - 2020-03-18
                                              #  - 2020-03-17
                                              #  - 2020-03-16
                                              # we are going to exploit this in the following execute calls to check
                                              # 'nearest' functionality
                                              "offline_training_data": eureka_offline_training_data['*'][:-14].nearest()  # or latest()
                                          },
                                          compute_targets=[ # when inputs are ready, trigger the following
                                              BatchCompute(
                                                  """
from pyspark.sql.functions import col
day = dimensions['day']
output = offline_training_data.limit(10)
# most recent partition of this dataset is from 2020-03-18 so as long as the test range (of 14 days) covers this, then
# the following assertion should be ok.
# 5th column is 'order_day' and PySpark returns a datetime object here.
assert output.collect()[0][4].strftime('%Y-%m-%d') == ('2020-03-18')  # 'latest' order_day
                                                  """,
                                                  Lang.PYTHON,
                                                  ABI.GLUE_EMBEDDED,
                                                  GlueVersion="2.0",
                                                  WorkerType=GlueWorkerType.G_1X.value,
                                                  NumberOfWorkers=30
                                              )
                                          ])

# should succeed since 2020-03-18 is within 14 days of range from 2020-03-20
# please note that 'nearest' input 'offline_training_data' does not have that partition so during the execution
# it should pick up 2020-03-18 only.
path = app.execute(pick_latest_from_eureka['2020-03-20'])
assert path  # should succeed
try:
    # execute is different than runtime behaviour, it won't keep waiting for the 'nearest=True' signal.
    # relies on compute failure if no data is not found within the range.
    # (however in normal execution, orchestration waits for the completion of a partition within the user provided range).
    app.execute(pick_latest_from_eureka['2020-04-20'])
    assert False
except:
    pass

# Emulate runtime signal processing using 2020-03-19 partition
# Emulate incoming signal on 'independent signal' [2020-03-19]
# Even if the second ('nearest') input did not receive an event yet, the execution will begin thanks to the existence of
# partition 2020-03-18 on it.
app.process(eureka_offline_all_data['2020-03-19'])

# hook up with the execution on '2020-03-19' which must be using 2020-03-18 on the 'nearest' input.
# yeah, the execution must have started without waiting for the second input since it is 'nearest' and have a partition
# within the expected range.
path, _ = app.poll(pick_latest_from_eureka['2020-03-19'])
assert path

app.pause()

