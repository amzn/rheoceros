# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

flow.init_basic_logging(root_level=logging.CRITICAL)

app = AWSApplication("andes-ant-app", "us-east-1")
# attach/pull the existing DAG/Conf
app.attach()

'''Exploration Examples'''
# Note: the following data exploration and preview/load operations assume
# that the application was activated (already running).
# analyze external glue table
ducsi = app['DEXML_DUCSI']
print(ducsi.metadata())
print(ducsi[1]['2021-01-12'].dimension_values())
# preview/load external data
app.preview_data(ducsi[1]['2021-01-12'], limit=10, columns=3)

# analyze internal data
repeat_ducsi = app['REPEAT_DUCSI']
print(repeat_ducsi.dimensions())
print(repeat_ducsi.dimension_values())
print(repeat_ducsi.partition_values())
print(repeat_ducsi.path_format())
print(repeat_ducsi.paths())
print(repeat_ducsi[3]['2021-01-12'].dimensions())
print(repeat_ducsi[3]['2021-01-12'].dimension_values())
print(repeat_ducsi[3]['2021-01-12'].partition_values())
print(repeat_ducsi[3]['2021-01-12'].path_format())
print(repeat_ducsi[3]['2021-01-12'].paths())

# preview/load internal
data, format = app.preview_data(repeat_ducsi[3]['2021-01-12'], limit=10, columns=3)
data_it = app.load_data(repeat_ducsi[3]['2021-01-12'], limit=10, format=DataFrameFormat.PANDAS)
data = next(data_it)

''' Execution Examples'''
# assuming the app was activated before.
# should not activate the application if it was activated already.
# returned value represents the physical path of the underlying partition for example and should be inputted to
# other tools, frameworks like Pandas, Spark, boto3 (again maybe) for further processing, checks.

#materialized_output_path = app.execute(app['REPEAT_DUCSI'][1]['2020-12-01'])
#materialized_output_path = app.execute(app['REPEAT_DUCSI'], app['DEXML_DUCSI'][1]['2020-12-01'])
ducsi_data = app['DEXML_DUCSI']
repeat_ducsi = app['REPEAT_DUCSI']
target2 = app.create_data(id="REPEAT_DUCSI2",
                          inputs={
                              "DEXML_DUCSI": ducsi_data,
                              "REPEAT_DUCSI": repeat_ducsi,
                              "REPEAT_DUCSI_RANGE": repeat_ducsi['*'][:-30]
                          },
                          compute_targets="output=DEXML_DUCSI.join(REPEAT_DUCSI, ['customer_id]).join(REPEAT_DUCSI_RANGE, ['customer_id'])"
                          )
output_path = app.execute(target2[1]['2020-06-21'])
# OR
#output_path = app.execute(target2, [ducsi_data[1]['2020-06-21'], repeat_ducsi[1]['2020-06-21']])

# app.process(app["DEXML_DUCSI"][1]['2020-12-01'])

# materialized_output_path, record = app.poll(app['REPEAT_DUCSI'][1]['2020-12-01'])

