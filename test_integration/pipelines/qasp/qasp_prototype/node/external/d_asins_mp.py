# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from intelliflow.api_ext import *


def create(app: AWSApplication) -> MarshalerNode:
    return app.glue_table(
        "booker",
        "d_asins_marketplace_attributes",
        dimension_spec={
            "region_id": {
                type: DimensionType.LONG,
            }
        },
    )
