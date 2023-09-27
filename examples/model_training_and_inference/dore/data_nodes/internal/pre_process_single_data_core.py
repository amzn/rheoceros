# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from pyspark.sql import functions as F
from pyspark.sql.functions import lit

# set the lean relevant columns:
lean_relevant_columns = [
    "isFiltered",
    "requestId",
    "orderEpoch",
    "client_id",
    "deliveryProgram",
    "shipOptionName",
    "orderTimeHour",
    "orderDayOfWeek",
    "destinationPostCode",
    "date_start",
    "marketplaceId",
    "promise_start_date",
    "promise_end_date",
    "fastTrack",
    "promiseQuality",
    "charge",
]

# read dynamic partition value for dimension 'day' from IntelliFlow provided 'dimensions' map
daily_data_date = dimensions["day"]

daily_data = daily_data.withColumn("date_start", lit(daily_data_date))
daily_data = daily_data.withColumn("promise_start_date", from_unixtime(daily_data.startDate))
daily_data = daily_data.withColumn("promise_end_date", from_unixtime(daily_data.byDate))
daily_data = daily_data.select(*lean_relevant_columns)

daily_data.createOrReplaceTempView("filter_data")
daily_data = spark.sql(
    "SELECT *, ROW_NUMBER() OVER (PARTITION BY requestId, shipOptionName, orderEpoch ORDER BY deliveryProgram ) row_number_within_group FROM filter_data"
).where(F.expr("row_number_within_group=1"))
daily_data = daily_data.drop("row_number_within_group")

# always use this assignment to 'output' to save the data in PySpark loads
output = daily_data