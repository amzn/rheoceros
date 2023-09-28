# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
from intelliflow.core.platform.definitions.aws.common import exponential_retry
from pyspark.sql.functions import col, concat, length, lit, struct, to_json
from pyspark.sql.types import *


def update_ddb_item(dynamodb, table, row):
    try:
        table.update_item(Key={
                              "key": row.key,
                          },
                          UpdateExpression=f"set #kval = :dex_record",
                          ExpressionAttributeNames={"#kval": "value", "#keyname": "key"},
                          ExpressionAttributeValues={":dex_record": row.value},
                          ReturnValues="UPDATED_NEW",
                          ConditionExpression=f"attribute_not_exists(#keyname)",
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        # Swallow the expected condition check failures if item already exists or we are backfilling older data
        pass

# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
# Primary key need less than 2048 bytes
# MP need 1 byte, delimiter is unicode \u0001 need 4 byt

DYNAMODB_PRIMARY_KEY_LENGTH_LIMITATION = 2043

# This is calculated using this formula: Number of partitions = (Number of executors * 8) + Buffer
# Number of executors = # of Glue G.1x workers - 1 (subtract 1 because that is for driver)
# Please ensure number of partitions is greater than number of vCPU's. Number of vCPU's = (Number of executors * 8)
# Why 200? This is because currently this node has 25 workers and hence 192 vCPU's using above formula. Buffer is 8.
PARTITION_COUNT_BEFORE_DDB_DUMP = 200

# Add new test row into dynamoDB
QU_INTEGRATION_TEST_KEY = "DummyQueryOnlyForIntegrationPointTest"

ddb_table_name = args["dynamodb_table"]

# TTL_SUPPORT
#from datetime import datetime, timedelta
#day = dimensions["date"]
#current_date = datetime.strptime(str(day).split()[0], "%Y-%m-%d")
#ttl_value = (current_date + timedelta(days=7)).timestamp() * 1000

dex_data_daily = dex_data_daily.withColumn("marketplace_id", lit(1))

# Add new test row into dynamoDB
dex_smoothing_schema = [
    "marketplace_id",
    "keywords",
    "score",
]
test_row = spark.createDataFrame([(1, QU_INTEGRATION_TEST_KEY, 2)], dex_smoothing_schema)
dex_data_daily_added = dex_data_daily.union(test_row)

dynamodb_dump_data = (
    dex_data_daily_added.withColumn("key", concat(col("marketplace_id"), lit("\u0001"), col("keywords")))
    .withColumn(
        "value",
        to_json(
            struct(
                "score",
            )
        ),
    )
    # TTL_SUPPORT
    # .withColumn("expire_date", lit(ttl_value))
    #.select("key", "value", "expire_date")
    .select("key", "value")
    .filter(length(col("key")) < DYNAMODB_PRIMARY_KEY_LENGTH_LIMITATION)
)


def sink_data(rows):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(ddb_table_name)
    for row in rows:
        exponential_retry(update_ddb_item, ["ProvisionedThroughputExceededException", "ThrottlingException"], dynamodb, table, row)


dex_daily_dyf = dynamodb_dump_data.repartition(PARTITION_COUNT_BEFORE_DDB_DUMP)

dex_daily_dyf.foreachPartition(sink_data)

# write the written row count as output
data = [(dex_data_daily.count())]
schema = StructType([
    StructField("dump_count", LongType(), True),
])

output = spark.createDataFrame(data=data, schema=schema)
