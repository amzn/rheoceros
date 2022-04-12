# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from datetime import datetime

from intelliflow.api_ext import *


def create(app: AWSApplication, pdex_external: MarshalerNode, region: str, region_id: int, marketplace_id: int) -> MarshalerNode:
    return app.create_data(
        id=f"pdex_consolidated_{region}_{marketplace_id}",
        inputs={"pdex_external": pdex_external[region]["*"]},
        output_dimension_spec={
            "region_id": {
                type: DimensionType.LONG,
                "marketplace_id": {
                    type: DimensionType.LONG,
                    "day": {"format": "%Y-%m-%d", type: DimensionType.DATETIME, "hour": {type: DimensionType.LONG}},
                },
            }
        },
        output_dim_links=[
            ("region_id", EQUALS, region_id),
            ("marketplace_id", EQUALS, marketplace_id),
            ("day", lambda hour_dt: hour_dt.date(), pdex_external("dataset_date")),
            ("hour", lambda hour_dt: hour_dt.hour, pdex_external("dataset_date")),
            # add reverse lookup from output to pdex_external for better testability and debugging exp
            (pdex_external("dataset_date"), lambda day, hour: datetime(day.year, day.month, day.day, hour), ("day", "hour")),
        ],
        compute_targets=[
            BatchCompute(
                code="""
from pyspark.sql.functions import col, floor, min, when

SECONDS_PER_DAY = 86400

output = (
    pdex_external.withColumn("timeToDelivery", col("latestDeliveryDate") - col("datestamp"))
    .withColumn("timeToDelivery", when(col("timeToDelivery") > 0, col("timeToDelivery")))
    .withColumnRenamed("requestInputId", "request_id")
    .withColumnRenamed("sessionId", "session")
    .groupBy("session", "request_id", "asin")
    .agg(min("timeToDelivery").alias("fastestTimeToDeliveryInSecond"))
    .withColumn("fastestTimeToDeliveryInDay", floor(col("fastestTimeToDeliveryInSecond") / SECONDS_PER_DAY))
)
                """,
                lang=Lang.PYTHON,
                retry_count=0,
                GlueVersion="3.0",
                WorkerType=GlueWorkerType.G_2X.value,
                NumberOfWorkers=75,
                Timeout=4 * 60,  # 4 hours
            )
        ],
        dataset_format=DataFormat.PARQUET,
    )
