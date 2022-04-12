# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from intelliflow.api_ext import *


def create(app: AWSApplication, tommy_data_node: MarshalerNode) -> MarshalerNode:
    return app.create_data(
        "tommy_presto", inputs={"tommy_jones": tommy_data_node}, compute_targets=[PrestoSQL("""SELECT * FROM tommy_jones LIMIT 10""")]
    )
