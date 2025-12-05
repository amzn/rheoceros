# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import io
import json
import logging
import random
import time
import zipfile
from typing import Dict, Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


def create_lambda_deployment_package(function_file_name):
    """
    Creates a Lambda deployment package in ZIP format in an in-memory buffer. This
    buffer can be passed directly to AWS Lambda when creating the function.
    :param function_file_name: The name of the file that contains the Lambda handler
                               function.
    :return: The deployment package.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zipped:
        zipped.write(function_file_name)
    buffer.seek(0)
    return buffer.read()


def create_lambda_function(
    lambda_client,
    function_name,
    description,
    handler_name,
    iam_role_arn,
    deployment_package,
    python_major_ver,
    python_minor_ver,
    dead_letter_target_arn: Optional[str] = None,
    memory_size=1024,
    timeout=900,
    **kwargs,
):
    """
    Deploys the AWS Lambda function.
    :param lambda_client: The Boto3 AWS Lambda client object.
    :param function_name: The name of the AWS Lambda function.
    :param handler_name: The fully qualified name of the handler function. This
                         must include the file name and the function name.
    :param iam_role: The IAM role to use for the function.
    :param deployment_package: dictionary object contains S3 path, bucket name that points to the deployment package
                               uploaded to S3. The dictionary must contains key "S3Bucket", "S3Key"
                               with string as value.
    :param python_major_ver: Python major version as '3' in '3.7',
    :param python_minor_ver Python major version as '7' in '3.7',
    :param dead_letter_target_arn: DLQ to be used by Async queue of this lambda for failed requests
    :return: The Amazon Resource Name (ARN) of the newly created function.
    """
    args = {
        "FunctionName": function_name,
        "Description": description,
        "Timeout": timeout,
        "MemorySize": memory_size,
        "Runtime": "python{}.{}".format(python_major_ver, python_minor_ver),
        "Role": iam_role_arn,
        "Handler": handler_name,
        "Code": {"S3Bucket": deployment_package["S3Bucket"], "S3Key": deployment_package["S3Key"]},
        "Environment": {"Variables": {key: value for key, value in kwargs.items()}},
        # do not publish a version, use the $LATEST
        "Publish": False,
    }
    if dead_letter_target_arn:
        args.update({"DeadLetterConfig": {"TargetArn": dead_letter_target_arn}})
    try:
        response = lambda_client.create_function(**args)

        # support wide-range of boto versions by checking the existence
        if "function_exists" in lambda_client.waiter_names:
            lambda_client.get_waiter("function_exists").wait(FunctionName=function_name)
        if "function_active" in lambda_client.waiter_names:
            lambda_client.get_waiter("function_active").wait(FunctionName=function_name)
        function_arn = response["FunctionArn"]
        logger.info("Created function '%s' with ARN: '%s'.", function_name, response["FunctionArn"])
    except ClientError:
        logger.exception("Couldn't create function %s.", function_name)
        raise
    else:
        return function_arn


def update_lambda_function_code(lambda_client, function_name, deployment_package):
    """
    Updates the code of an existing AWS Lambda function.
    :param lambda_client: The Boto3 AWS Lambda client object.
    :param function_name: The name of the AWS Lambda function.
    :param deployment_package: dictionary object contains S3 path, bucket name that points to the deployment package
                               uploaded to S3. The dictionary must contains key "S3Bucket", "S3Key"
                               with string as value.
    :return: The Amazon Resource Name (ARN) of the newly updated function.
    """
    try:
        response = lambda_client.update_function_code(
            # do not publish, use $LATEST version
            FunctionName=function_name,
            S3Bucket=deployment_package["S3Bucket"],
            S3Key=deployment_package["S3Key"],
            Publish=False,
        )
        function_arn = response["FunctionArn"]
        if "function_updated" in lambda_client.waiter_names:
            lambda_client.get_waiter("function_updated").wait(FunctionName=function_name)
        else:
            time.sleep(10)
        logger.info("Updated function code for '%s' with ARN: '%s'.", function_name, response["FunctionArn"])
    except ClientError:
        logger.exception("Couldn't update function code for %s.", function_name)
        raise
    else:
        return function_arn


def update_lambda_function_conf(
    lambda_client,
    function_name,
    description,
    handler_name,
    iam_role_arn,
    python_major_ver,
    python_minor_ver,
    dead_letter_target_arn: Optional[str] = None,
    memory_size=1024,
    timeout=900,
    **kwargs,
):
    """
    Updates the conf of an existing AWS Lambda function.
    :param lambda_client: The Boto3 AWS Lambda client object.
    :param function_name: The name of the AWS Lambda function.
    :param handler_name: The fully qualified name of the handler function. This
                         must include the file name and the function name.
    :param iam_role: The IAM role to use for the function.
    :param kwargs: keyword args that would be passed in as Environment variables.
    :return: The Amazon Resource Name (ARN) of the newly updated function.
    """
    args = {
        "FunctionName": function_name,
        "Description": description,
        "Timeout": timeout,
        "MemorySize": memory_size,
        "Runtime": "python{}.{}".format(python_major_ver, python_minor_ver),
        "Role": iam_role_arn,
        "Handler": handler_name,
        "Environment": {"Variables": {key: value for key, value in kwargs.items()}},
    }
    if dead_letter_target_arn:
        args.update({"DeadLetterConfig": {"TargetArn": dead_letter_target_arn}})
    try:
        response = lambda_client.update_function_configuration(**args)
        function_arn = response["FunctionArn"]
        if "function_updated" in lambda_client.waiter_names:
            lambda_client.get_waiter("function_updated").wait(FunctionName=function_name)
        else:
            time.sleep(10)
        logger.info("Updated function conf for '%s' with ARN: '%s'.", function_name, response["FunctionArn"])
    except ClientError:
        logger.exception("Couldn't update function conf for %s.", function_name)
        raise
    else:
        return function_arn


def delete_lambda_function(lambda_client, function_name):
    """
    Deletes an AWS Lambda function.
    :param lambda_client: The Boto3 AWS Lambda client object.
    :param function_name: The name of the function to delete.
    """
    try:
        lambda_client.delete_function(FunctionName=function_name)
    except ClientError:
        raise


def invoke_lambda_function(lambda_client, function_name, function_params, is_async=True):
    """
    Invokes an AWS Lambda function.
    :param lambda_client: The Boto3 AWS Lambda client object.
    :param function_name: The name of the function to invoke.
    :param function_params: The parameters of the function as a dict. This dict
                            is serialized to JSON before it is sent to AWS Lambda.
    :return: The response from the function invocation.
    """
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            Payload=json.dumps(function_params).encode(),
            InvocationType="Event" if is_async else "RequestResponse",
        )
        logger.info("Invoked function %s.", function_name)
    except ClientError:
        logger.exception("Couldn't invoke function %s.", function_name)
        raise
    return response


def get_lambda_digest(lambda_client, function_name: str) -> Optional[str]:
    """
    Get AWS Lambda function's SHA256 digest, if exist.
    :param lambda_client: The Boto3 AWS Lambda client object.
    :param function_name: The name of the function to query.
    :return: SHA256 of the lambda as str, None if such lambda cannot be found.
    """
    lambda_detail = _get_lambda_function_details(lambda_client, function_name)
    return lambda_detail["Configuration"]["CodeSha256"] if lambda_detail else None


def get_lambda_arn(lambda_client, function_name: str) -> Optional[str]:
    """
    Get AWS Lambda function's ARN using function name, if such lambda exist in that region.
    :param lambda_client: The Boto3 AWS Lambda client object.
    :param function_name: The name of the function to find.
    :return: ARN of the lambda as str, None if such lambda cannot be found.
    """
    lambda_detail = _get_lambda_function_details(lambda_client, function_name)
    return lambda_detail["Configuration"]["FunctionArn"] if lambda_detail else None


def add_permission(lambda_client, function_name, statement_id, action, principal, source_arn=None, source_account=None):
    """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.add_permission"""

    kwargs = {
        "FunctionName": function_name,
        "StatementId": statement_id,  # should be unique
        "Action": action,  # 'lambda:InvokeFunction'
        "Principal": principal,  # 's3.amazonaws.com', IAM role arn
    }
    if source_arn:
        kwargs.update({"SourceArn": source_arn})

    if source_account:
        kwargs.update({"SourceAccount": source_account})

    try:
        response = lambda_client.add_permission(**kwargs)
        statement = response["Statement"]
        logger.info("added permission to function: '%s'. new statement: '%s'.", function_name, statement)
    except ClientError:
        logger.exception("Couldn't add permission to function %s.", function_name)
        raise
    else:
        return statement


def remove_permission(lambda_client, function_name, statement_id):
    """https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.remove_permission"""
    try:
        response = lambda_client.remove_permission(
            FunctionName=function_name,
            StatementId=statement_id,  # should be unique
        )
        logger.info("removed permission %s from function: '%s'.", statement_id, function_name)
    except ClientError:
        raise


def put_function_concurrency(lambda_client, function_name, concurreny_limit):
    try:
        lambda_client.put_function_concurrency(FunctionName=function_name, ReservedConcurrentExecutions=concurreny_limit)
        logger.info(f"Updated concurrency of function: {function_name!r} to new level: {concurreny_limit}.")
    except ClientError:
        logger.exception(f"Couldn't update concurrency of function {function_name!r}! concurrency: {concurreny_limit}")
        raise


def delete_function_concurrency(lambda_client, function_name):
    try:
        lambda_client.delete_function_concurrency(FunctionName=function_name)
    except ClientError:
        logger.exception(f"Couldn't delete concurrency of function {function_name!r}!")
        raise


def _get_lambda_function_details(lambda_client, function_name: str) -> Optional[Dict]:
    """
    Get details about an AWS Lambda function.
    :param lambda_client: The Boto3 AWS Lambda client object.
    :param function_name: The name of the function to invoke.
    :param function_params: The parameters of the function as a dict. This dict
                            is serialized to JSON before it is sent to AWS Lambda.
    :return: The response from the function invocation. dictionary contains details about the lambda or None if no such
             lambda is found
    """
    try:
        return lambda_client.get_function(FunctionName=function_name)
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "ResourceNotFoundException":
            return None
        logger.error("Couldn't check lambda '%s'! Error: %s", function_name, str(ex))
        raise
