# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from examples.model_training_and_inference.dore.regionalization.clientId import REGION_TO_CLIENT_ID_MAP
from examples.model_training_and_inference.dore.regionalization.core_deliveryPrograms import REGION_TO_CORE_DELIVERY_PROGRAMS_MAP
from pyspark.sql import functions as F

# pre_process_data, process_specific_columns, process_so_name

region = dimensions["region"]

daily_data = daily_data.withColumn("so_name", F.regexp_replace("shipOptionName", " ", "_"))
daily_data = daily_data.withColumn("so_name", F.regexp_replace("so_name", "-", "_"))
daily_data = daily_data.withColumn("so_name", F.regexp_replace("so_name", "/", "_"))
daily_data = daily_data.withColumn("so_name", F.regexp_replace("so_name", ",", "_"))

# process_deliveryProgram
daily_data = daily_data.fillna("NON_PRIME_SHIPPING", ["deliveryProgram"])

# simplify_deliveryProgram
core_deliveryPrograms = REGION_TO_CORE_DELIVERY_PROGRAMS_MAP[region]
core_deliveryPrograms_str = "(" + ",".join(["'" + a + "'" for a in core_deliveryPrograms]) + ")"
daily_data = daily_data.withColumn(
    "simplified_deliveryPrograms",
    F.expr("CASE WHEN deliveryProgram in {} THEN deliveryProgram ELSE 'other_deliveryProgram' END".format(core_deliveryPrograms_str)),
)

# simplify_clientId
core_clientId = REGION_TO_CLIENT_ID_MAP[region]

daily_data = daily_data.withColumn("simplified_clientId", F.expr("CASE WHEN client_id is null THEN 'Unknown' ELSE client_id END"))
core_clientId_str = "(" + ",".join(["'" + a + "'" for a in core_clientId]) + ")"
daily_data = daily_data.withColumn(
    "simplified_clientId",
    F.expr("CASE WHEN simplified_clientId in {} THEN simplified_clientId ELSE 'other_clientId' END".format(core_clientId_str)),
)

# simplify_zip
daily_data = daily_data.withColumn(
    "zip_validated", F.expr("CASE WHEN destinationPostCode REGEXP '^[0-9]+' THEN destinationPostCode ELSE 'other_zip' END")
)
daily_data = daily_data.withColumn("zip_validated", F.expr("CASE WHEN LENGTH(zip_validated)=5 THEN zip_validated ELSE 'other_zip' END"))
daily_data = daily_data.withColumn(
    "zip1", F.expr("CASE WHEN zip_validated='other_zip' THEN zip_validated ELSE SUBSTRING(zip_validated, 1, 1) END")
)
daily_data = daily_data.withColumn(
    "zip2", F.expr("CASE WHEN zip_validated='other_zip' THEN zip_validated ELSE SUBSTRING(zip_validated, 1, 2) END")
)
daily_data = daily_data.withColumn(
    "zip3", F.expr("CASE WHEN zip_validated='other_zip' THEN zip_validated ELSE SUBSTRING(zip_validated, 1, 3) END")
)

output = daily_data