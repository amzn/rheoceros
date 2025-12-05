# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
from enum import Enum, unique
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import boto3
from botocore.exceptions import ClientError

from intelliflow.core.platform.definitions.aws.common import exponential_retry, get_code_for_exception
from intelliflow.core.platform.definitions.compute import ComputeFailedSessionStateType, ComputeSessionStateType
from intelliflow.core.signal_processing.definitions.compute_defs import Lang

logger = logging.getLogger(__name__)


ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST: Set[str] = {"TooManyRequestsException", "InternalServerException"}

ATHENA_MAX_QUERY_STRING_LENGTH_IN_BYTES: int = 262144

# TODO dynamically generate the buffer when view format is impl'd
# Each input will be auto transformed into a view for each execution,
# this name translation for user provided table_names requires extra space.
INTELLIFLOW_ATHENA_INPUT_VIEW_LENGTH = 200


class AthenaQueryFailed(Exception):
    pass


def query(athena, query: str, database: str, workgroup: str, wait: bool = False, poll_interval_in_secs: int = 2) -> str:
    response = exponential_retry(
        athena.start_query_execution,
        ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST,
        QueryString=query,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    execution_id = response["QueryExecutionId"]
    if wait:
        _wait_for_query(athena, execution_id, poll_interval_in_secs)

    return execution_id


def _wait_for_query(athena, query_execution_id, poll_interval_in_secs: int = 2):
    is_running = True
    while is_running:
        response = exponential_retry(
            athena.get_query_execution, ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST, QueryExecutionId=query_execution_id
        )
        # state: QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
        status = response["QueryExecution"]["Status"]["State"]
        if status == "SUCCEEDED":
            is_running = False
        elif status in ["CANCELED", "FAILED"]:
            raise AthenaQueryFailed(status)
        elif status in ["QUEUED", "RUNNING"]:
            logger.debug(
                f"Sleeping for Athena query (exec_id: {query_execution_id!r}, state: {status!r}) {poll_interval_in_secs!r} seconds..."
            )
            time.sleep(poll_interval_in_secs)
        else:
            raise AthenaQueryFailed(status)


def get_athena_query_execution_state_type(state) -> ComputeSessionStateType:
    if state == "SUCCEEDED":
        return ComputeSessionStateType.COMPLETED
    elif state in ["CANCELED", "FAILED"]:
        return ComputeSessionStateType.FAILED
    elif state in ["QUEUED", "RUNNING"]:
        return ComputeSessionStateType.PROCESSING
    else:
        logger.critical(
            f"AWS Athena introduced a new state type {state}!"
            f" Marking it as {ComputeSessionStateType.UNKNOWN}. "
            f" This should be addressed ASAP by RheocerOS Core,"
            f" or your app should upgrade to a newer RheocerOS version."
        )
        return ComputeSessionStateType.UNKNOWN


def get_athena_query_execution_failure_type(query_execution_response) -> ComputeFailedSessionStateType:
    status = query_execution_response["QueryExecution"]["Status"]
    state = status["State"]
    if state in ["CANCELED"]:
        return ComputeFailedSessionStateType.STOPPED
    elif state in ["FAILED"]:
        # TODO Map status['StateChangeReason']
        #  to TRANSIENT failures (ComputeFailedSessionStateType.TRANSIENT)
        #  Check
        #  https://docs.aws.amazon.com/athena/latest/ug/troubleshooting-athena.html
        #  Particularly for Throttling
        #  https://docs.aws.amazon.com/athena/latest/ug/troubleshooting-athena.html#troubleshooting-athena-throttling-issues
        return ComputeFailedSessionStateType.APP_INTERNAL
    else:
        logger.critical(
            f"AWS Athena introduced a new state type {state}!"
            f" Marking it as {ComputeFailedSessionStateType.UNKNOWN}. "
            f" This should be addressed ASAP by RheocerOS Core,"
            f" or your app should upgrade to a newer RheocerOS version."
        )
        return ComputeFailedSessionStateType.UNKNOWN


def create_or_update_workgroup(athena, workgroup: str, output_location: str, enforce: bool = True, description: str = "", **tags):
    try:
        response = exponential_retry(
            athena.get_work_group,
            ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST.union({"AccessDeniedException", "AccessDenied"}),
            WorkGroup=workgroup,
        )
    except ClientError as error:
        error_code = get_code_for_exception(error)
        if error_code in ["InvalidRequestException"]:
            # Ex: "An error occurred (InvalidRequestException) when calling the GetWorkGroup operation: WorkGroup eureka-dev-yunusko_427809481713_us-east-1 is not found"
            response = {}
        else:
            raise error

    if response and "WorkGroup" in response and "Name" in response["WorkGroup"]:
        exponential_retry(
            athena.update_work_group,
            ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST,
            WorkGroup=workgroup,
            Description=description,
            ConfigurationUpdates={
                "EnforceWorkGroupConfiguration": enforce,
                "ResultConfigurationUpdates": {"OutputLocation": output_location},
                "PublishCloudWatchMetricsEnabled": True,
            },
            State="ENABLED",
        )
    else:
        exponential_retry(
            athena.create_work_group,
            ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST,
            Name=workgroup,
            Configuration={
                "ResultConfiguration": {"OutputLocation": output_location},
                "EnforceWorkGroupConfiguration": enforce,
                "PublishCloudWatchMetricsEnabled": True,
            },
            Description=description,
            Tags=[{"Key": str(key), "Value": str(value)} for key, value in tags.items()],
        )


def delete_workgroup(athena, workgroup: str):
    exponential_retry(athena.delete_work_group, ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST, WorkGroup=workgroup, RecursiveDeleteOption=True)


def get_table_metadata(athena, database: str, table: str):
    return exponential_retry(
        athena.get_table_metadata,
        ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST,
        CatalogName="AwsDataCatalog",
        DatabaseName=database,
        TableName=table,
    )
