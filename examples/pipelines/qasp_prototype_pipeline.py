# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from datetime import datetime, timezone, timedelta
from typing import Callable

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

logger = flow.init_basic_logging()

region="NA"
region_id = 1
marketplace_id=1

app = AWSApplication("c2p-beta-2", "us-east-1")

encryption_key = "arn:aws:kms:us-east-1:111222333444:key/aaaa5530-bbbb-cc95-8888-6666fab00054"

mmy_external = app.glue_table(database="database", table_name="mmy_asin_hourly", encryption_key=encryption_key,
        id=f"mmy_external",
        dimension_spec={
            "org": {
                "type": DimensionType.STRING,
                "format": lambda org: org.upper(),  # convenience: no matter what user input (in execute, process API) output upper
                "partition_date": {
                    "type": DimensionType.DATETIME,
                    "format": "%Y%m%d",
                    "partition_hour": {"type": DimensionType.DATETIME, "format": "%Y%m%d%H"},
                },
            }
        }
    )

xdex_external = app.glue_table(provider="ml-beta-db", table_name="xdex_logs", id="xdex_external")

dama = app.glue_table("database1", "marketplace_attributes",
                 dimension_spec={
                     "region_id": {  # overwrite the partition_key and define its type (otherwise the key name from the catalog will be picked automatically)
                         type: DimensionType.LONG,
                     }
                 })

xdf = app.marshal_external_data(
    S3Dataset(
        "111222333444",
        "database-core-xdf-test",
        "",
        "{}",
        "{}*",  # * -> to ignore the last two digit gibberish added as a suffix to partition value
        dataset_format=DataFormat.PARQUET,
        encryption_key=encryption_key,
    ),
    id="xdf_external",
    dimension_spec={
        "cdo_region": {
            "type": DimensionType.STRING,
            "format": lambda dim: dim.lower(),
            "insensitive": True,
            "day": {"type": DimensionType.DATETIME, "format": "%Y%m%d"},
        }
    }
)

xdex_consolidated= app.create_data(
    id=f"xdex_consolidated_{region}_{marketplace_id}",
    inputs={"xdex_external": xdex_external[region]["*"]},
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
        ("day", lambda hour_dt: hour_dt.date(), xdex_external("dataset_date")),
        ("hour", lambda hour_dt: hour_dt.hour, xdex_external("dataset_date")),
        # add reverse lookup from output to xdex_external for better testability and debugging exp
        (
            xdex_external("dataset_date"),
            lambda day, hour: datetime(day.year, day.month, day.day, hour),
            ("day", "hour")
        ),
    ],
    compute_targets=[
        BatchCompute(
            code="""
from pyspark.sql.functions import col, floor, min, when

SECONDS_PER_DAY = 86400

output = (
    xdex_external.withColumn("timeToDelivery", col("latestDeliveryDate") - col("datestamp"))
    .withColumn("timeToDelivery", when(col("timeToDelivery") > 0, col("timeToDelivery")))
    .withColumnRenamed("requestInputId", "request_id")
    .withColumnRenamed("sessionId", "session")
    .groupBy("session", "request_id", "asin")
    .agg(min("timeToDelivery").alias("fastestTimeToDeliveryInSecond"))
    .withColumn("fastestTimeToDeliveryInDay", floor(col("fastestTimeToDeliveryInSecond") / SECONDS_PER_DAY))
)
            """,
            lang=Lang.PYTHON,
            retry_count=1,
            GlueVersion="2.0",
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=150,
            Timeout=4 * 60,  # 4 hours
        )
    ],
    dataset_format=DataFormat.PARQUET,
)


def xdex_to_mmy_converter() -> Callable[[datetime, int], datetime]:
    return lambda day_in_utc, hour_in_utc: datetime(day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc, tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-7)))


mmy_data_node = app.create_data(
    id=f"C2P_TOMMY_DATA_{region}_{marketplace_id}",
    inputs={
        "mmy_external": mmy_external["US"]["*"]["*"].ref.range_check(True),
        "digital_external": dama[region_id].ref,
        "xdf_external": xdf["NA"][:-14].nearest(),
        "xdex_consolidated": xdex_consolidated[region_id][marketplace_id]["*"]["*"],
    },
    input_dim_links=[
        # Tommy and Tommy derivatives are partitioned by day/hour in local time, so we need to convert.
        # Conversion relies on semantics in the xdex_consolidated node guaranteeing utc times, and is thus fragile
        (
            mmy_external("partition_date"),
            lambda day_in_utc, hour_in_utc: datetime(day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc, tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-7))),
            xdex_consolidated("day", "hour")
        ),
        (
            mmy_external("partition_hour"),
            lambda day_in_utc, hour_in_utc: datetime(day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc, tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-7))),
            xdex_consolidated("day", "hour")
        ),
        # provide reverse links for testing convenience (from console.py or in the notebooks) when mmy external
        # is provided as seed for execute or process APIs.
        (
            xdex_consolidated("day"),
            lambda t_d: datetime(t_d.year, t_d.month, t_d.day, t_d.hour, tzinfo=timezone(timedelta(hours=-7))).astimezone(timezone.utc),
            mmy_external("partition_date"),
        ),
        (
            xdex_consolidated("hour"),
            lambda t_h: datetime(t_h.year, t_h.month, t_h.day, t_h.hour, tzinfo=timezone(timedelta(hours=-7))).astimezone(timezone.utc).hour,
            mmy_external("partition_hour"),
        ),
        (
            xdf("day"),
            lambda day_in_utc, hour_in_utc: datetime(day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc, tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-7))),
            xdex_consolidated("day", "hour")
        ),
    ],
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
        ("day", EQUALS, mmy_external("partition_date")),
        ("hour", lambda hour_dt: hour_dt.hour, mmy_external("partition_hour")),
        # define the reverse link for testing and debugging convenience
        #  output("day", "hour") -> mmy_external("hour")
        (mmy_external("partition_hour"), lambda o_day, o_hour: datetime(o_day.year, o_day.month, o_day.day, o_hour),
         ("day", "hour")),
    ],
    compute_targets=[
        BatchCompute(
            code="""
from datetime import datetime, timedelta
from enum import Enum

from pyspark.sql.functions import array_contains, col, lit, when

# KMS specific config
class C2PPipelineDimensions(str, Enum):
    REGION_ID = "region_id"
    MARKETPLACE_ID = "marketplace_id"
    HOUR = "hour"
    DAY = "day"

class C2PCommonConfigs(str, Enum):
    MARKETPLACE_ID = "marketplace_id"
    SESSION = "session"
    SESSION_TYPE = "session_type"
    IS_ROBOT = "is_robot"
    ASIN = "asin"
    DAYS_TO_DELIVER = "days_to_deliver"
    IS_DIGITAL_PRODUCT = "is_digital_product"
    CUSTOMER_ID = "customer_id"
    INJECTED_DTD = "injected_dtd"

HEADER = Enum(
    "HEADER",
    f'''{C2PCommonConfigs.MARKETPLACE_ID}
        {C2PCommonConfigs.SESSION}
        {C2PCommonConfigs.IS_ROBOT}
        {C2PCommonConfigs.ASIN}
        {C2PCommonConfigs.DAYS_TO_DELIVER}
        ''',
    )

hour = dimensions[C2PPipelineDimensions.HOUR]
day = dimensions[C2PPipelineDimensions.DAY]
datetime_day = datetime.strptime(str(day).split()[0], "%Y-%m-%d")
datetime_hour = datetime_day + timedelta(hours=int(hour))

partition_date = int(datetime_hour.timestamp() * 1000)

region_id = int(dimensions[C2PPipelineDimensions.REGION_ID])
marketplace_id = int(dimensions[C2PPipelineDimensions.MARKETPLACE_ID])

DIGITAL_PRODUCT_TYPES = [
    "E_GIFT_CARD",
    "A_MEMBERSHIP",
    "C_DRIVE",
    "C_S_SUBSCRIPTION",
    "D_BUNDLE",
    "D_AUDIO",
    "D_MOVIE",
    "D_MUSIC_ALBUM",
    "D_SOFTWARE",
    "D_TV_EPISODE",
    "D_VIDEO_GAME",
    "E_BUNDLE",
    "KINDLE_UNLIMITED",
    "LIT_SERIES",
    "MAG_SUBS",
    "MOBILE_APP",
]

mmy_input_columns = [
    C2PCommonConfigs.MARKETPLACE_ID,
    C2PCommonConfigs.SESSION,
    C2PCommonConfigs.IS_ROBOT,
    C2PCommonConfigs.ASIN,
    C2PCommonConfigs.DAYS_TO_DELIVER,
]

digital_table = (
    digital_external.filter(col("region_id") == region_id)
    .filter(col("marketplace_id") == marketplace_id)
    .filter(col("product_type").isNotNull())
    .withColumn(C2PCommonConfigs.IS_DIGITAL_PRODUCT, when(col("product_type").isin(DIGITAL_PRODUCT_TYPES), 1).otherwise(0))
    .select(C2PCommonConfigs.ASIN, C2PCommonConfigs.MARKETPLACE_ID, C2PCommonConfigs.IS_DIGITAL_PRODUCT)
    .dropDuplicates([C2PCommonConfigs.MARKETPLACE_ID, C2PCommonConfigs.ASIN])
)

mmy_filtered_df = (
    mmy_external.select(*mmy_input_columns)
    .filter(col(HEADER.marketplace_id.name) == lit(marketplace_id))
    .filter(col(C2PCommonConfigs.CUSTOMER_ID).isNotNull())
    .filter(col(HEADER.keywords.name).isNotNull())
    .filter(col(HEADER.search_type.name) == "kw")
    .filter(col(HEADER.is_robot.name) != 1)
)
mmy_with_xdex = (
    mmy_filtered_df.join(xdex_consolidated, ["session", "request_id", "asin"], how="left")
    .withColumn(
        C2PCommonConfigs.INJECTED_DTD,
        when(col(C2PCommonConfigs.DAYS_TO_DELIVER).isNotNull(), col(C2PCommonConfigs.DAYS_TO_DELIVER)).otherwise(
            col("fastestTimeToDeliveryInDay")
        ),
    )
    .na.fill(999, [C2PCommonConfigs.INJECTED_DTD])
)

xdf_filtered_df = (
    xdf_external.select("marketplaceid", C2PCommonConfigs.ASIN, "is_alm_only")
    .withColumnRenamed("marketplaceid", C2PCommonConfigs.MARKETPLACE_ID)
    .filter(col(C2PCommonConfigs.MARKETPLACE_ID) == lit(marketplace_id))
    .dropDuplicates([C2PCommonConfigs.MARKETPLACE_ID, C2PCommonConfigs.ASIN])
)

mmy_digit = (
    mmy_with_xdex.join(digital_table, [C2PCommonConfigs.MARKETPLACE_ID, C2PCommonConfigs.ASIN], how="left")
    .withColumn(C2PCommonConfigs.IS_DIGITAL_PRODUCT, when(col(C2PCommonConfigs.IS_DIGITAL_PRODUCT) == 1, 1).otherwise(0))
    .withColumn(
        C2PCommonConfigs.INJECTED_DTD, when(col(C2PCommonConfigs.IS_DIGITAL_PRODUCT) == 1, 0).otherwise(col(C2PCommonConfigs.INJECTED_DTD))
    )
    .withColumn(HEADER.partition_date.name, lit(partition_date))
)

# coalesce for partitions to go down less than 3500 to relax ingestion from Athena (PrestoSQL)
output = mmy_digit.join(xdf_filtered_df, [C2PCommonConfigs.MARKETPLACE_ID, C2PCommonConfigs.ASIN], how="left").withColumn(
    C2PCommonConfigs.INJECTED_DTD, when(array_contains("is_alm_only", 1), 0).otherwise(col(C2PCommonConfigs.INJECTED_DTD))
).coalesce(500)
                """,
            lang=Lang.PYTHON,
            retry_count=1,
            GlueVersion="3.0",
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=150,
            Timeout=4 * 60,  # 4 hours
        )
    ],
    dataset_format=DataFormat.PARQUET,
    auto_input_dim_linking_enabled=False
)

mmy_data_node_SCALA = app.create_data(
    id=f"C2P_TOMMY_DATA_SCALA_{region}_{marketplace_id}",
    inputs={
        "mmy_external": mmy_external["US"]["*"]["*"].ref.range_check(True),
        "digital_external": dama[region_id].ref,
        "xdf_external": xdf["NA"][:-14].nearest(),
        "xdex_consolidated": xdex_consolidated[region_id][marketplace_id]["*"]["*"],
    },
    input_dim_links=[
        (
            mmy_external("partition_date"),
            lambda day_in_utc, hour_in_utc: datetime(day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc,
                                                     tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-7))),
            xdex_consolidated("day", "hour")
        ),
        (
            mmy_external("partition_hour"),
            lambda day_in_utc, hour_in_utc: datetime(day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc,
                                                     tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-7))),
            xdex_consolidated("day", "hour")
        ),
        # provide reverse links for testing convenience (from console.py or in the notebooks) when mmy external
        # is provided as seed for execute or process APIs.
        (
            xdex_consolidated("day"),
            lambda t_d: datetime(t_d.year, t_d.month, t_d.day, t_d.hour,
                                 tzinfo=timezone(timedelta(hours=-7))).astimezone(timezone.utc),
            mmy_external("partition_date"),
        ),
        (
            xdex_consolidated("hour"),
            lambda t_h: datetime(t_h.year, t_h.month, t_h.day, t_h.hour,
                                 tzinfo=timezone(timedelta(hours=-7))).astimezone(timezone.utc).hour,
            mmy_external("partition_hour"),
        ),
        (
            xdf("day"),
            lambda day_in_utc, hour_in_utc: datetime(day_in_utc.year, day_in_utc.month, day_in_utc.day, hour_in_utc,
                                                     tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=-7))),
            xdex_consolidated("day", "hour")
        ),
    ],
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
        ("day", EQUALS, mmy_external("partition_date")),
        ("hour", lambda hour_dt: hour_dt.hour, mmy_external("partition_hour")),
        # define the reverse link for testing and debugging convenience
        #  output("day", "hour") -> mmy_external("hour")
        (mmy_external("partition_hour"), lambda o_day, o_hour: datetime(o_day.year, o_day.month, o_day.day, o_hour),
         ("day", "hour")),
    ],
    compute_targets=[
        BatchCompute(
            code=scala_script("""
        val mmy = mmy_external.limit(10)
        val mmy_count = mmy.count()
        val dama = digital_external.limit(10).count()
        val xdf = xdf_external.limit(10).count()
        val xdex = xdex_consolidated.limit(10).count()
        mmy 
                """),
            lang=Lang.SCALA,
            GlueVersion="1.0",
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=150,
            Timeout=4 * 60,  # 4 hours
        )
    ],
    dataset_format=DataFormat.PARQUET,
    auto_input_dim_linking_enabled=False
)

mmy_data_filtered_PRESTO = app.create_data("mmy_presto",
                                             inputs={"mmy_jones": mmy_data_node},
                                             compute_targets=[
                                                 PrestoSQL(
                                                     """SELECT * FROM mmy_jones LIMIT 10"""
                                                 )
                                             ])
app.activate()

app.execute(mmy_data_node[1][1]["2022-03-30"][15], wait=False, recursive=True)
# app.execute(mmy_data_node_SCALA[1][1]["2022-03-10"][15], wait=False)
# app.execute(mmy_data_filtered_PRESTO[1][1]["2022-03-10"][15], wait=False)

path, records = app.poll(mmy_data_node[1][1]["2022-03-10"][15])
# path2, records2 = app.poll(mmy_data_node_SCALA[1][1]["2022-03-10"][15])
# path3, records3 = app.poll(mmy_data_filtered_PRESTO[1][1]["2022-03-10"][15])

app.pause()