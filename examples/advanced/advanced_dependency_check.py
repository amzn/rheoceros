# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

# This example covers the advanced input mode 'range_check' only.
#
# 'range_check' controls whether a 'ranged' input (with multiple materialized paths, as in a range of data partitions)
# will be checked for the existence of data spawning its entire domain. For example, if you want to make sure that a
# trigger will happen only all the data partitions from a range of a dataset are ready then you use this flag on
# an input in Application::create_data.

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.application.core_application import ApplicationState

from intelliflow.core.signal_processing.dimension_constructs import StringVariant
from intelliflow.utils.test.inlined_compute import NOOPCompute

flow.init_basic_logging()

# automatically reads default credentials
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
app = AWSApplication("advanced-inputs", "us-east-1", enforce_runtime_compatibility=False)

if app.state != ApplicationState.INACTIVE:
    # reset routing state, previous execution data
    app.platform.routing_table._clear_active_routes()

eureka_offline_training_data = app.add_external_data(
    data_id="eureka_training_data",
    s3_dataset=S3("111111111111",
                  "<BUCKET_NAE>",
                  "folder_1/folder_2/dataset_name",
                  # partitions
                  StringVariant('NA', 'region'),
                  AnyDate('day', {'format': '%Y-%m-%d'})))

default_selection_features = app.create_data(id='eureka_default',
                                             inputs={
                                                 # Range !!!
                                                 "offline_training_data": eureka_offline_training_data['NA'][:-3].range_check(True),
                                                 "offline_training_data_shifted_range": eureka_offline_training_data['NA'][-1:-3].range_check(True),
                                                 "offline_training_data_yesterday": eureka_offline_training_data['NA'][-1:].range_check(True),
                                                 "datum": eureka_offline_training_data['NA']['2020-03-18']
                                             },
                                             compute_targets=[  # when inputs are ready, trigger the following
                                                 NOOPCompute
                                             ])

app.activate(allow_concurrent_executions=False)

# DEPENDENCY/RANGE/COMPLETION CHECK
# BEGIN Emulating incoming completion events from the raw/external S3 dataset

# CREATE THE TIP (a trigger group/node) @ NA + 2020-03-18 (target partition/execution for this test)
# now since 'range_check' is enabled, it will need:
#   - 2020-03-17
#   - 2020-03-16
app.process(eureka_offline_training_data['NA']['2020-03-18'])

# it won't yield anything since range_check fails due to missing partitions
path, _ = app.poll(default_selection_features['NA']['2020-03-18'])
assert not path

# still needs:
#   - 2020-03-16
app.process(eureka_offline_training_data['NA']['2020-03-17'])
path, _ = app.poll(default_selection_features['NA']['2020-03-18'])
assert not path

app.process(eureka_offline_training_data['NA']['2020-03-16'])
path, _ = app.poll(default_selection_features['NA']['2020-03-18'])
# 2020-03-18 DONE!
assert path

app.pause()

