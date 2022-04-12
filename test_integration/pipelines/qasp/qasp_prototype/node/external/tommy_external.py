# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from intelliflow.api_ext import *


def create(app: AWSApplication, encryption_key: str) -> MarshalerNode:
    return app.glue_table(
        database="searchdata",
        table_name="tommy_asin_hourly",
        encryption_key=encryption_key,
        id=f"tommy_external",
        dimension_spec={
            "org": {
                "type": DimensionType.STRING,
                "format": lambda org: org.upper(),
                # convenience: no matter what user input (in execute, process API) output upper
                "partition_date": {
                    "type": DimensionType.DATETIME,
                    "format": "%Y%m%d",
                    "partition_hour": {"type": DimensionType.DATETIME, "format": "%Y%m%d%H"},
                },
            }
        },
    )
