# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import time
import uuid
from datetime import datetime

import boto3
import botocore
from boto3.dynamodb.conditions import Attr

from intelliflow.core.platform.definitions.aws.common import MAX_SLEEP_INTERVAL_PARAM, exponential_retry
from intelliflow.core.platform.definitions.aws.ddb.client_wrapper import create_table

logger = logging.getLogger(__name__)

DEAD_LOCK_RECORD_TTL_IN_SECS = 1 * 60 * 60


def _current_timestamp() -> int:
    return int(datetime.utcnow().timestamp())


def create_lock_table(ddb_resource, lock_table_name):
    table = None
    try:
        table = exponential_retry(
            create_table,
            ["LimitExceededException", "InternalServerError"],
            ddb_resource=ddb_resource,
            table_name=lock_table_name,
            key_schema=[{"AttributeName": "lock", "KeyType": "HASH"}],
            attribute_def=[{"AttributeName": "lock", "AttributeType": "S"}],
            provisioned_throughput={"ReadCapacityUnits": 20, "WriteCapacityUnits": 20},
        )

        exponential_retry(
            table.meta.client.update_time_to_live,
            ["InternalServerError"],
            TableName=lock_table_name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
        )
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise

    return table


def lock(lock_table, lock_id: str, retainer: str, lock_duration_in_secs: int, retain_attempt_duration_in_secs: int):
    retained = False
    retain_attempt_start = _current_timestamp()
    while True:
        now = _current_timestamp()
        lock_end_timestamp = now + lock_duration_in_secs
        ttl = DEAD_LOCK_RECORD_TTL_IN_SECS + lock_end_timestamp // 1000
        try:
            exponential_retry(
                lock_table.put_item,
                ["LimitExceededException", "InternalServerError"],
                Item={"lock": lock_id, "lock_end_timestamp": lock_end_timestamp, "retainer": retainer, "ttl": ttl},
                # Note: allow recursive by adding Attr("retainer").eq(retainer)
                #  Why? in case of errors, when the same worker somehow attemts to retain the same lock, dont
                #  cause unnecessary latency.
                ConditionExpression=Attr("lock_end_timestamp").lt(now)
                | Attr("retainer").eq("")
                | Attr("retainer").eq(retainer)
                | Attr("lock").not_exists(),
                **{MAX_SLEEP_INTERVAL_PARAM: 8},
            )
            retained = True
            break
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                if (_current_timestamp() - retain_attempt_start) > retain_attempt_duration_in_secs:
                    break
                else:
                    time.sleep(0.2)
    return retained


def release(lock_table, lock_id, retainer):
    try:
        exponential_retry(
            lock_table.put_item,
            ["LimitExceededException", "InternalServerError"],
            Item={
                "lock": lock_id,
                "lock_end_timestamp": 0,
                "retainer": "",
            },
            ConditionExpression=Attr("retainer").eq(retainer) | Attr("lock").not_exists(),
            **{MAX_SLEEP_INTERVAL_PARAM: 4},
        )
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
    return True
