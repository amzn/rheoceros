# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from examples.model_training_and_inference.dore.regionalization.dominant_env_SO_names import (
    REGION_TO_DOMINANT_ENV_SO_NAMES_MAP,
    REGION_TO_DOMINANT_SO_NAMES_MAP,
)
from pyspark.sql import functions as F

region = dimensions["region"]
dominant_SO_names_all = REGION_TO_DOMINANT_SO_NAMES_MAP[region]
dominant_env_SO_names_all = REGION_TO_DOMINANT_ENV_SO_NAMES_MAP[region]

other_feature_list = ["simplified_so_name", "simplified_deliveryPrograms", "orderTimeHour"]


def further_simplify_so_name(df, core_simplified_so_names, candidate_so_name_to_be_removed=[]):
    # print("core_simplified_so_names in further_simplify_so_name()", core_simplified_so_names)
    core_simplified_so_names_str = "(" + ",".join(["'" + so + "'" for so in core_simplified_so_names]) + ")"
    df = df.withColumn(
        "simplified_so_name",
        F.expr("CASE WHEN simplified_so_name in {} THEN simplified_so_name ELSE 'other_so_name' END".format(core_simplified_so_names_str)),
    )
    if len(candidate_so_name_to_be_removed) > 0:
        candidate_so_name_to_be_removed_str = "( " + " OR ".join([so_name + "=1" for so_name in candidate_so_name_to_be_removed]) + " )"
        df = df.withColumn(
            "other_so_name",
            F.expr("CASE WHEN other_so_name=1 THEN 1 WHEN {} THEN 1 ELSE 0 END".format(candidate_so_name_to_be_removed_str)),
        )
    return df


# simplify the deliveryProgram to only care about those in the core_deliveryPrograms
def simplify_deliveryProgram(df, core_deliveryPrograms):
    core_deliveryPrograms = core_deliveryPrograms[:]
    core_deliveryPrograms_str = "(" + ",".join(["'" + a + "'" for a in core_deliveryPrograms]) + ")"
    df = df.withColumn(
        "simplified_deliveryPrograms",
        F.expr("CASE WHEN deliveryProgram in {} THEN deliveryProgram ELSE 'other_deliveryProgram' END".format(core_deliveryPrograms_str)),
    )
    return df


def downsample(df, max_event, key_event_features=["requestId", "orderEpoch"]):
    event_map = df.select(*key_event_features).distinct()
    event_map = event_map.dropna()
    event_map.cache()
    event_map_total = event_map.count()
    sample_ratio = float(1.0 * max_event / event_map_total)
    print("sample ratio is {}, max_event is {}, total event is {}".format(sample_ratio, max_event, event_map_total))
    if sample_ratio > 1:
        print("keep all data")
        sample_data = df
    else:
        sample_event = event_map.sample(False, sample_ratio, seed=0)
        sample_data = df.join(sample_event, key_event_features, "inner")
    return sample_data


# add new row to make the specific column having the values specified in col_values. This is to create fake rows in the eval data so the encoded categorical variable would be consistent between train and eval data.
def add_new_row(df, colname, col_values):
    new_row = df.limit(1)
    new_row.cache()
    new_row.count()
    for v in col_values:
        new_row = new_row.withColumn(colname, lit(v))
        df = df.union(new_row)
    new_row.unpersist()
    return df


# use custom argument passed from app code
is_training = args.get("is_for_training", "false")
is_training = True if is_training.lower() == "true" else False
if is_training:
    yesterday = today_and_yesterday.subtract(today_augment_data)
    df = yesterday
    df = further_simplify_so_name(df, dominant_SO_names_all[:])
else:  # validation
    df = today_augment_data
    unique_so_names_list = [so.simplified_so_name for so in df_train.select("simplified_so_name").distinct().collect()]
    unique_delivery_programs_list = [
        delivery_program.simplified_deliveryPrograms
        for delivery_program in df_train.select("simplified_deliveryPrograms").distinct().collect()
    ]
    df = further_simplify_so_name(df, unique_so_names_list[:])
    is_offline_testing = args.get("is_offline_testing", "false")
    is_offline_testing = True if is_offline_testing.lower() == "true" else False
    if is_offline_testing:
        unique_delivery_programs_list.remove("other_deliveryProgram")
    df = simplify_deliveryProgram(df, unique_delivery_programs_list[:])


# print(dominant_SO_names)
df_sample = downsample(df, max_event=1000000, key_event_features=["requestId", "orderEpoch"])
df_sample.cache()  # this may help on performance


# this is to add rows with specific values of simplified_so_name
# this is only to be done for eval data so the simplified_so_name would have the same levels as in the training data and thus the fitted model
if is_training is False and len(unique_so_names_list) > 0:
    df_sample = add_new_row(df_sample, "simplified_so_name", unique_so_names_list)

if is_training is False and len(unique_delivery_programs_list) > 0:
    df_sample = add_new_row(df_sample, "simplified_deliveryPrograms", unique_delivery_programs_list)

all_env_so_name = dominant_env_SO_names_all[:]
all_env_so_name.append("other_so_name")

input_features = other_feature_list[:]
input_features.extend(all_env_so_name[:])
output_label = ["isFiltered"]

lean_feature_set = input_features[:]
lean_feature_set.insert(0, "isFiltered")
df_sample = df_sample.select(*lean_feature_set)

output = df_sample.coalesce(1)