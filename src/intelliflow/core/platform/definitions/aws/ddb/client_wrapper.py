# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from enum import Enum, unique
from typing import Optional

from botocore.exceptions import ClientError

from intelliflow.core.platform.definitions.aws.common import MAX_SLEEP_INTERVAL_PARAM, exponential_retry, get_code_for_exception

logger = logging.getLogger(__name__)


@unique
class BillingMode(str, Enum):
    PAY_PER_REQUEST = "PAY_PER_REQUEST"
    PROVISIONED = "PROVISIONED"


def create_table(
    ddb_resource,
    table_name,
    key_schema,
    attribute_def,
    provisioned_throughput=None,
    local_secondary_index=None,
    billing_mode: BillingMode = BillingMode.PAY_PER_REQUEST,
    **extra_args,
):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
    """
    args = {"TableName": table_name, "KeySchema": key_schema, "AttributeDefinitions": attribute_def, "BillingMode": billing_mode.value}
    if billing_mode == BillingMode.PROVISIONED:
        args.update({"ProvisionedThroughput": provisioned_throughput})

    if local_secondary_index:
        args.update({"LocalSecondaryIndexes": local_secondary_index})

    if extra_args:
        # overwrite if any overlap
        args.update(extra_args)

    try:
        table = ddb_resource.create_table(**args)
        table.meta.client.get_waiter("table_exists").wait(TableName=table_name)
        logger.info("Successfully created the table: %s", table_name)
        enable_PITR(table)
        return table
    except ClientError:
        raise


def update_table(
    ddb_table,
    attribute_def,
    provisioned_throughput=None,
    billing_mode: BillingMode = BillingMode.PAY_PER_REQUEST,
    **extra_args,
):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.update_table
    """
    args = {"AttributeDefinitions": attribute_def, "BillingMode": billing_mode.value}
    if billing_mode == BillingMode.PROVISIONED:
        args.update({"ProvisionedThroughput": provisioned_throughput})

    if extra_args:
        # overwrite if any overlap
        args.update(extra_args)

    try:
        table = ddb_table.update(**args)
        logger.info("Successfully updated the table: %s", ddb_table.table_name)
        enable_PITR(ddb_table)
        return table
    except ClientError:
        raise


def enable_PITR(ddb_table) -> None:
    try:
        exponential_retry(
            ddb_table.meta.client.update_continuous_backups,
            [],
            TableName=ddb_table.table_name,
            PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled": True},
        )
        logger.info("Successfully enabled PITR on the table: %s", ddb_table.table_name)
    except ClientError as err:
        if err.response["Error"]["Code"] not in ["ContinuousBackupsUnavailableException", "ContinuousBackupsUnavailable"]:
            raise


def update_ttl(ddb_table, ttl_attribute_name: Optional[str] = None) -> None:
    """Refer
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_time_to_live.html
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/describe_time_to_live.html

    Important things to note:
    - If already updated within the last one hour, it might raise ValidationException or ResourceInUseException, we will
    wait on this condition for better UX in consecutive activations with different attributes and if the attr name is
    same, we will avoid the call here
    - Definition of ttl attr for a table means the enablement of TTL on it. Simplifying user experience by not avoiding
    and extra state variable at table level
    """
    needs_to_be_updated = False
    current_attribute_name = None
    try:
        response = exponential_retry(
            ddb_table.meta.client.describe_time_to_live,
            [],
            TableName=ddb_table.table_name,
        )

        ttl_desc = response["TimeToLiveDescription"]
        status = ttl_desc["TimeToLiveStatus"]
        if status in ["DISABLED", "DISABLING"] and ttl_attribute_name:
            needs_to_be_updated = True
        elif status in ["ENABLED", "ENABLING"] and ttl_desc["AttributeName"] != ttl_attribute_name:
            needs_to_be_updated = True
            if not ttl_attribute_name:
                current_attribute_name = ttl_desc["AttributeName"]
    except ClientError as err:
        if err.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
            raise

        if ttl_attribute_name:
            needs_to_be_updated = True

    if needs_to_be_updated:
        enabled = True
        attribute_name = ttl_attribute_name
        if not attribute_name:
            enabled = False
            attribute_name = current_attribute_name  # required even for disable action

        logger.critical(f"Updating TTL for table {ddb_table.table_name!r} (Enabled={enabled!r}, AttributeName= {attribute_name!r}) ...")

        response = exponential_retry(
            ddb_table.meta.client.update_time_to_live,
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb/client/update_time_to_live.html
            #  "Any additional UpdateTimeToLive calls for the same table during this one hour duration result in a ValidationException."
            ["ResourceInUseException", "ValidationException"],
            TableName=ddb_table.table_name,
            TimeToLiveSpecification={"Enabled": enabled, "AttributeName": attribute_name},
            # retry against consecutive valid updates (DDB might reject the request if updated already within the last hour
            **{MAX_SLEEP_INTERVAL_PARAM: (256 + 1)},
        )
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise RuntimeError(
                f"Could not update TTL for table {ddb_table.table_name!r}! (Enabled={enabled!r}, AttributeName= {attribute_name!r})"
            )


def delete_table(table):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.delete
    :param table:
    :return:
    """
    try:
        response = table.delete()
        table.meta.client.get_waiter("table_not_exists").wait(TableName=table.table_name)
        logger.info("Table %s has been successfully deleted", table.table_name)
        return response
    except ClientError:
        raise


def get_ddb_item(table, key):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.get_item
    :param table:
    :param key:
    :return:
    """
    try:
        response = table.get_item(Key=key)
        logger.info("Got successful response for Key: %s from Table %s", json.dumps(key), table.table_name)
        return response
    except ClientError as error:
        error_code = get_code_for_exception(error)
        if error_code not in ["ResourceNotFoundException"]:
            logger.exception("Got exception during get_item operation on key: %s and table: %s", json.dumps(key), table.table_name)
            raise error


def put_ddb_item(table, item):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.put_item
    :param table:
    :param item:
    :return:
    """
    try:
        response = table.put_item(Item=item)
        return response
    except ClientError:
        logger.exception("Got exception during put_item operation on item: %s for table: %s", json.dumps(item), table.table_name)
        raise


def delete_ddb_item(table, key):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.delete_item
    :param table:
    :param key:
    :return:
    """
    try:
        response = table.delete_item(Key=key)
        logger.info("Successfully deleted item with key: %s from table %s", json.dumps(key), table.table_name)
        return response
    except ClientError:
        logger.exception("Got exception during delete_item operation on table: %s on" "key: %s", table.table_name, json.dumps(key))
        raise


def query_ddb_table(table, key_cond_expr, scan_index_forward, **query_kwargs):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.query
    :param table:
    :param key_cond_expr:
    :param scan_index_forward:
    :param query_kwargs:
    :return:
    """
    try:
        response = table.query(KeyConditionExpression=key_cond_expr, ScanIndexForward=scan_index_forward, **query_kwargs)
        return response
    except ClientError:
        logger.exception(
            "Got exception during ddb table query operation. TableName: %s," "KeyConditionExpression: %s, Query Kwargs: %s",
            table.table_name,
            str(key_cond_expr),
            json.dumps(query_kwargs),
        )
        raise


def scan_ddb_table(table, **scan_kwargs):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.scan
    :param table:
    :param scan_kwargs:
    :return:
    """
    try:
        response = table.scan(**scan_kwargs)
        return response
    except ClientError:
        logger.exception(
            "Exception occurred during scan operation for table: %s, with " "scan args: %s", table.table_name, json.dumps(scan_kwargs)
        )
        raise


def put_item_batch(batch_writer, item):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.batch_writer
    :param batch_writer:
    :param item:
    :return:
    """
    try:
        batch_writer.put_item(Item=item)
    except ClientError:
        logger.exception("Exception occurred during put item using batch writer for table")
