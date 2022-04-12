# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from intelliflow.api_ext import *


def create(app: AWSApplication) -> MarshalerNode:
    return app.glue_table(database="dex-ml-beta", table_name="pdex_logs", id="pdex_external")
