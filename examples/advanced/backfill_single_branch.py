# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from datetime import datetime, timedelta

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

from intelliflow.utils.test.inlined_compute import NOOPCompute

flow.init_basic_logging()

today = datetime.now()

app = AWSApplication("backfill-1", "us-east-1")

a = app.create_data("A",
                    inputs=[app.add_timer("T", "rate(1 day)")],
                    compute_targets=[NOOPCompute])

b = app.create_data("B", inputs=[a[:-2]], compute_targets=[NOOPCompute])

app.execute(b[today], wait=True, recursive=True)

assert app.poll(b[today])[0], "Execution must have been successful!"
assert app.poll(a[today - timedelta(1)])[0], "Recursive execution on node 'A' could not be found!"

app.pause()
