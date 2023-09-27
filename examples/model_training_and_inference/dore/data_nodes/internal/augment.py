# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from examples.model_training_and_inference.dore.regionalization.dominant_so_names import REGION_TO_DOMINANT_SO_NAMES_MAP, REGION_TO_ADDITIONAL_SO_NAMES_MAP

from pyspark.sql import functions as F
from pyspark.sql.functions import array_contains
from pyspark.sql.types import *


region = dimensions["region"]

# add_same_request_SO_set, get_request_level_data,
key_cols = ["requestId", "orderEpoch"]

# simplify_so_names
dominant_SO_names_all = REGION_TO_DOMINANT_SO_NAMES_MAP[region]

additional_SO_names = REGION_TO_ADDITIONAL_SO_NAMES_MAP[region]

dominant_SO_names_all.extend(additional_SO_names)

core_so_names_str = "(" + ",".join(["'" + a + "'" for a in dominant_SO_names_all]) + ")"
daily_data = daily_data.withColumn(
    "simplified_so_name", F.expr("CASE WHEN so_name in {} THEN so_name ELSE 'other_so_name' END".format(core_so_names_str))
)

df_request_level_all = (
    daily_data.groupBy(*key_cols)
    .agg(F.collect_set("simplified_so_name").alias("SO_set_all"))
    .orderBy(F.col("orderEpoch").asc(), F.col("requestId").asc())
)
df_request_level_all = df_request_level_all.withColumn("SO_set_all_size", F.size("SO_set_all"))

df_request_level_survival = (
    daily_data.where(F.expr("isFiltered=0")).groupBy(*key_cols).agg(F.collect_set("simplified_so_name").alias("SO_set_survival"))
)
df_request_level_survival = df_request_level_survival.withColumn("SO_set_survival_size", F.size("SO_set_survival"))

df_request_level = df_request_level_all.join(df_request_level_survival, key_cols, "left_outer")
df_request_level = df_request_level.fillna(0, ["SO_set_survival_size"])

daily_data_w_request_level_so_set = daily_data.join(df_request_level, key_cols)

dominant_SO_names_all.append("other_so_name")
for shipOption_name in dominant_SO_names_all:
    daily_data_w_request_level_so_set = daily_data_w_request_level_so_set.withColumn(
        shipOption_name, array_contains("SO_set_all", shipOption_name)
    )
    daily_data_w_request_level_so_set = daily_data_w_request_level_so_set.withColumn(
        shipOption_name, F.expr("CASE WHEN {}='true' THEN 1 ELSE 0 END".format(shipOption_name))
    )

daily_data_preprocessed_augmented = daily_data_w_request_level_so_set.drop("SO_set_all").drop("SO_set_survival")
additional_int_type_colnames = ["orderTimeHour", "orderDayOfWeek"]
for colname in additional_int_type_colnames:
    daily_data_preprocessed_augmented = daily_data_preprocessed_augmented.withColumn(colname, F.col(colname).cast(IntegerType()))

output = daily_data_preprocessed_augmented.coalesce(5)
