# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

flow.init_basic_logging()

# TODO Create integ tests similar to the following application
# Following application shows that if the user provides range check as False
# then IF would create Spark DataFrame from whatever materialised input paths
# are available in the input range.
app = AWSApplication("range-check", "us-east-1", "286245245617")

# The following external dataset has 2020-10-24 to 2020-10-31 range available.
rush_order_analysis = app.marshal_external_data(
    S3Dataset("286245245617", "edx-to-s3-dexml-commondc-beta", "dex-ml/glue-data/all/rush-order-analysis", "{}", "{}",
              dataset_format=DataFormat.CSV)
    , "rush_order_analysis"
    , {
        'region': {
            'type': DimensionType.LONG,
            'day': {
                'type': DimensionType.DATETIME
            }
        }
    }
    , {
        "*": {
            "*": {
                'format': '%Y-%m-%d',
                'timezone': 'PST',
            }
        }
    },
    SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)

# This node would show that the Python based compute would succeed eventhough
# complete range is not available. This is because the user provide range check as False.
# Note that the input dataset "rush_order_analysis" has only 2020-10-24 to 2020-10-31 range available.
rush_random_python_incomplete = app.create_data(id='rush_random_python_incomplete',
                                             inputs={
                                                 "rush_order_analysis": rush_order_analysis[1][:-3].range_check(False),
                                                 "datum": rush_order_analysis[1]['2020-10-24']
                                             },
                                             compute_targets=[
                                                 BatchCompute(
                                                     code="output=rush_order_analysis.limit(100)",
                                                     lang=Lang.PYTHON,
                                                     GlueVersion="2.0",
                                                     WorkerType=GlueWorkerType.G_1X.value,
                                                     NumberOfWorkers=50,
                                                     Timeout=8 * 60  # 8 hours
                                                 )
                                             ])

# What happens when there are no inputs available in the given range.
# And range_check is False.
rush_random_python_none = app.create_data(id='rush_random_python_none',
                                             inputs={
                                                 "rush_order_analysis": rush_order_analysis[1][:-3].range_check(False),
                                                 "datum": rush_order_analysis[1]['2020-10-23']
                                             },
                                             compute_targets=[
                                                 BatchCompute(
                                                     code="output=rush_order_analysis.limit(100)",
                                                     lang=Lang.PYTHON,
                                                     GlueVersion="2.0",
                                                     WorkerType=GlueWorkerType.G_1X.value,
                                                     NumberOfWorkers=50,
                                                     Timeout=8 * 60  # 8 hours
                                                 )
                                             ])

# This is normal complete range with range check True
rush_random_python_complete = app.create_data(id='rush_random_python_complete',
                                             inputs={
                                                 "rush_order_analysis": rush_order_analysis[1][:-3].range_check(False),
                                                 "datum": rush_order_analysis[1]['2020-10-27']
                                             },
                                             compute_targets=[
                                                 BatchCompute(
                                                     code="output=rush_order_analysis.limit(100)",
                                                     lang=Lang.PYTHON,
                                                     GlueVersion="2.0",
                                                     WorkerType=GlueWorkerType.G_1X.value,
                                                     NumberOfWorkers=50,
                                                     Timeout=8 * 60  # 8 hours
                                                 )
                                             ])

# This is Scala based compute which has range check False. Similar to the
# rush_random_python_incomplete node.
rush_random_scala_incomplete = app.create_data(id='rush_random_scala_incomplete',
                                             inputs={
                                                 "rush_order_analysis": rush_order_analysis[1][:-3].range_check(False),
                                                 "datum": rush_order_analysis[1]['2020-10-25']
                                             },
                                             compute_targets=[
                                                 BatchCompute(
                                                     scala_script("rush_order_analysis.limit(100)"),
                                                     lang=Lang.SCALA,
                                                     GlueVersion="2.0",
                                                     WorkerType=GlueWorkerType.G_1X.value,
                                                     NumberOfWorkers=50,
                                                     Timeout=3 * 60  # 3 hours
                                                 )
                                             ])

# Complete range check for Scala based compute.
rush_random_scala_complete = app.create_data(id='rush_random_scala_complete',
                                             inputs={
                                                 "rush_order_analysis": rush_order_analysis[1][:-3].range_check(False),
                                                 "datum": rush_order_analysis[1]['2020-10-31']
                                             },
                                             compute_targets=[
                                                 BatchCompute(
                                                     scala_script("rush_order_analysis.limit(100)"),
                                                     lang=Lang.SCALA,
                                                     GlueVersion="2.0",
                                                     WorkerType=GlueWorkerType.G_1X.value,
                                                     NumberOfWorkers=50,
                                                     Timeout=3 * 60  # 3 hours
                                                 )
                                             ])

# When there are no inputs available for input range. And range_check is False.
rush_random_scala_none = app.create_data(id='rush_random_scala_none',
                                             inputs={
                                                 "rush_order_analysis": rush_order_analysis[1][:-3].range_check(False),
                                                 "datum": rush_order_analysis[1]['2020-10-22']
                                             },
                                             compute_targets=[
                                                 BatchCompute(
                                                     scala_script("rush_order_analysis.limit(100)"),
                                                     lang=Lang.SCALA,
                                                     GlueVersion="2.0",
                                                     WorkerType=GlueWorkerType.G_1X.value,
                                                     NumberOfWorkers=50,
                                                     Timeout=3 * 60  # 3 hours
                                                 )
                                             ])

app.activate(allow_concurrent_executions=True)
# Python Compute: No input signals in the range. range_check is False.
# Should raise RuntimeError in Glue and no path should
# be returned.
app.process(app["rush_order_analysis"][1]['2020-10-23'])
path, _ = app.poll(rush_random_python_none[1]['2020-10-23'])
assert not path

# Scala Compute: No input signals in the range. range_check is False.
# Should raise RuntimeError in Glue and no path should
# be returned.
app.process(app["rush_order_analysis"][1]['2020-10-22'])
path, _ = app.poll(rush_random_scala_none[1]['2020-10-22'])
assert not path

# Python Compute: Incomplete range and range_check is False.
# Compute should return DataFrame from whatever is available.
app.process(app["rush_order_analysis"][1]['2020-10-24'])
path, _ = app.poll(rush_random_python_incomplete[1]['2020-10-24'])
assert path

# Python Compute: Complete range. range_check is True. Normal scenario.
app.process(app["rush_order_analysis"][1]['2020-10-27'])
path, _ = app.poll(rush_random_python_complete[1]['2020-10-27'])
assert path

# Scala Compute: Incomplete range and range_check is False.
# Compute should return DataFrame from whatever is available.
app.process(app["rush_order_analysis"][1]['2020-10-25'])
path, _ = app.poll(rush_random_scala_incomplete[1]['2020-10-25'])
assert path

# Scala Compute: Complete range. range_check is True. Normal scenario.
app.process(app["rush_order_analysis"][1]['2020-10-31'])
path, _ = app.poll(rush_random_scala_complete[1]['2020-10-31'])
assert path
app.terminate()
app.delete()
