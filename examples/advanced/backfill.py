# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from datetime import datetime, timedelta

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

from intelliflow.utils.test.inlined_compute import NOOPCompute

flow.init_basic_logging()

today = datetime.now()

# - uses default AWS credentials for the bootstrapper/admin entity
# - use 'access_id' and 'access_key' parameters to provide your own bootstrapper credentials
app = AWSApplication("backfill-2", "us-east-1")

ext = app.marshal_external_data(
    S3Dataset("111222333444", "bucket-exec-recurse", "data", "{}")
    , "ext_data_1"
    , {
        'day': {
            'type': DimensionType.DATETIME,
            'format': '%Y-%m-%d'
        }
    }
    , protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)

# note that "A" depends on a range of 'NOW - 2' partitions on 'ext_data_1'
a = app.create_data("A",
                    inputs=[
                        ext[:-2].nearest(),
                        app.add_timer("T", "rate(1 day)", time_dimension_id="day")
                    ],
                    compute_targets=[NOOPCompute])

# note that "B" depends on a range of 'NOW - 2' partitions on 'ext_data_1'
b = app.create_data("B", inputs=[ext[:-2]], compute_targets=[NOOPCompute])

c = app.create_data("C", inputs=[ext], compute_targets=[NOOPCompute])

# use 'nearest' (which would trigger execution on nearest, TIP partition if entire range is missing)
# and 'ref' (which would still go through entire range check when 'recursive'=True)
d = app.create_data("D", inputs=[a[:-15].nearest(), b[:-2].ref, c], compute_targets=[NOOPCompute])

app.activate()

today = datetime.now()
tomorrow = today + timedelta(1)
yesterday = today - timedelta(1)

# experiment with artificially adding partitions for upstream data to see their impact on recursive execution below.
#
#add_test_data(app, ext[yesterday], object_name="_SUCCESS", data="")
#add_test_data(app, ext[today], object_name="_SUCCESS", data="")
#add_test_data(app, ext[tomorrow], object_name="_SUCCESS", data="")

# TODAY
# recursive = True -> automatically backfills the DAG (required partitions on B,C and A).
# if your Python process dies, you can call this API again (without having to call 'activate' again).
# note: executions are in AWS, wait=True does not mean that the actual workflow is executed on this machine.
app.execute(d[today], wait=True, recursive=True)

app.pause()
