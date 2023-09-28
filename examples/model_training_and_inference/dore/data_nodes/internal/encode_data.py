# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from pyspark.sql import functions as F

cat_feature_list = ["simplified_so_name", "simplified_deliveryPrograms"]
contains_isFiltered = args.get("contains_isFiltered", "false")
contains_isFiltered = (contains_isFiltered.lower() == 'true')

so_names = df.select("simplified_so_name").distinct().rdd.flatMap(lambda x: x).collect()
delivery_programs = df.select("simplified_deliveryPrograms").distinct().rdd.flatMap(lambda x: x).collect()
so_names_expr = [F.when(F.col("simplified_so_name") == so, 1).otherwise(0).alias("simplified_so_name_" + so) for so in so_names]
delivery_programs_expr = [
    F.when(F.col("simplified_deliveryPrograms") == delivery_program, 1)
    .otherwise(0)
    .alias("simplified_deliveryPrograms_" + delivery_program)
    for delivery_program in delivery_programs
]
df = df.select(F.col("*"), *so_names_expr + delivery_programs_expr)
df = df.drop(*cat_feature_list).coalesce(1)
df = df.select(sorted(df.columns, reverse=False))
other_columns = [c for c in df.columns if c not in {"isFiltered"}]
output = df.select("isFiltered", *other_columns) if contains_isFiltered else df.select(*other_columns).limit(500050)
