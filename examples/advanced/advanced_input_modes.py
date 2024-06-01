# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# This example covers advanced input modes such as 'reference' and 'range_check'.
#
# 'reference' inputs dont mandate event-polling on them and can be used on data which just needs to be inputted
# to the compute side.
#
# 'range_check' controls whether a 'ranged' input (with multiple materialized paths, as in a range of data partitions)
# will checked for the existence of data spawning its entire domain. For example, if you want to make sure that a
# trigger will happen only all of the data partitions from a range of a dataset are ready then you use this flag on
# an input in Application::create_data.

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

from intelliflow.core.signal_processing.dimension_constructs import StringVariant

flow.init_basic_logging()

region = "us-east-1"

# automatically reads default credentials
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
app = AWSApplication("advanced-inputs", region)

eureka_offline_training_data = app.add_external_data(
        data_id="eureka_training_data",
        s3_dataset=S3("999888777666",
                      "<TRAINING_DATA_BUCKET>",
                      "eureka/v8_00/training-data",
                      StringVariant('NA', 'region'),
                      AnyDate('day', {'format': '%Y-%m-%d'})))

eureka_offline_all_data = app.marshal_external_data(
    S3Dataset("999888777666","<TRAINING_DATA_BUCKET>", "eureka/v8_00/all-data-prod", "partition_day={}", dataset_format=DataFormat.CSV)
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

default_selection_features = app.create_data(id='eureka_default', # data_id
                                             inputs={
                                                 "offline_training_data": eureka_offline_training_data,
                                                 # REFERENCE! Routing runtime won't wait for an event on this one as long as
                                                 # it can be mapped from the dimensions of the other non-reference input.
                                                 "offline_data": eureka_offline_all_data[:-2].no_event
                                             },
                                             compute_targets=[ # when inputs are ready, trigger the following
                                                 BatchCompute(
                                                     "output = offline_data.unionAll(offline_training_data).limit(10)",
                                                     Lang.PYTHON,
                                                     ABI.GLUE_EMBEDDED,
                                                     GlueVersion="2.0",
                                                     WorkerType=GlueWorkerType.G_1X.value,
                                                     NumberOfWorkers=50
                                                 )
                                             ])

# Introduce external dataset signal from glue catalog
d_ad_orders_na = app.marshal_external_data(
    GlueTable("dex_ml_catalog", "d_ad_orders_na")
    , "d_ad_orders_na"
    , {
        'order_day': {  # optional (user overwrites partition name and its format for internal use)
            'type': DimensionType.DATETIME,
            'format': '%Y-%m-%d',
            'timezone': 'PST'
        }
    }
    , {
        '*': {}
    }
)

merge_eureka_default_with_AD = app.create_data(id="REPEAT_AD_ORDERS",
                                               # please note that inputs won't be linked since their dimension names are diff
                                               # and also there is no input_dim_links provided. this enables the following weird
                                               # join of partitions from different dates.
                                              inputs=[default_selection_features,
                                                      d_ad_orders_na['2021-01-13'].as_reference() # or no_event or ref or reference()
                                                      ],
                                              compute_targets=[
                                                  BatchCompute("output=eureka_default.join(d_ad_orders_na, ['order_day'], how='left').limit(100)",
                                                               WorkerType=GlueWorkerType.G_1X.value,
                                                               NumberOfWorkers=50,
                                                               GlueVersion="2.0")
                                              ])


app.activate(allow_concurrent_executions=False)

# INJECT events/signals

# now this should create an avalanche in the pipeline down to the final node
app.process(eureka_offline_training_data['NA']['2020-03-18'])

# 1- check the first node and poll till it ends (check the console for updates on execution state).
path, _ = app.poll(default_selection_features['NA']['2020-03-18'] )
assert path  # path generation is a quick & dirty way to check success on the target partition in this case

# 2- now the second node must have been started
path, _ = app.poll(merge_eureka_default_with_AD['NA']['2020-03-18'] )
assert path

app.pause()

