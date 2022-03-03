# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import io
import json
import logging
import time
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


# https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-queues.html
MAX_QUEUE_NAME_SIZE = 80


def create_queue(
    sqs_client,
    queue_name: str,
    msg_retention_period_in_secs: Optional[int] = 345600,  # 4 days (max 1209600 = 14 days)
    visibility_timeout_in_secs: Optional[int] = 30,
    **kwargs,
) -> str:
    """
    Creates a new SQS queue.
    Wraps
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.create_queue

    - for high-level configurable retry and more importantly for testability.
    ('client_wrappers' are our test-time mocking/stubbing interfaces, they encapsulate).
    - limit parametrization for simplicity (best on current IF use-cases).

    :param sqs_client: The Boto3 AWS SQS client object.
    :param queue_name: The name of the Queue to be created
    :param msg_retention_period_in_secs: See 'create_queue / 'Attributes':'MessageRetentionPeriod'
    :param visibility_timeout_in_secs: See 'create_queue / 'Attributes':'VisibilityTimeout'
    :return: The URL of the newly created function.
    """

    attributes = dict(kwargs)
    attributes.update({"MessageRetentionPeriod": str(msg_retention_period_in_secs), "VisibilityTimeout": str(visibility_timeout_in_secs)})
    try:
        response = sqs_client.create_queue(QueueName=queue_name, Attributes=attributes)

        queue_url = response["QueueUrl"]
        logger.info("Created queue '%s' with URL: '%s'.", queue_name, queue_url)
    except ClientError:
        logger.exception("Couldn't create queue %s.", queue_name)
        raise
    else:
        return queue_url


def get_queue_arn(sqs_client, queue_url: str) -> str:
    """Encapsulates SQS::get_queue_attributes with special attribute QueueArn.

    :param sqs_client: The Boto3 AWS SQS client object.
    :param queue_url: URL of the queue
    :return:  The Amazon Resource Name (ARN) of the queue.
    """
    try:
        response = sqs_client.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])

        queue_arn = response["Attributes"]["QueueArn"]
        logger.info("Retrieved queue ARN: '%s' for URL: '%s'.", queue_arn, queue_url)
    except ClientError:
        logger.exception("Couldn't retrieve ARN for queue URL: %s.", queue_url)
        raise
    else:
        return queue_arn


def set_queue_attributes(
    sqs_client,
    queue_url: str,
    msg_retention_period_in_secs: Optional[int] = 345600,  # 4 days (max 1209600 = 14 days)
    visibility_timeout_in_secs: Optional[int] = 30,
    **kwargs,
) -> None:
    """
    Updates the attributes of an SQS queue.
    Wraps
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#SQS.Client.set_queue_attributes

    :param sqs_client: The Boto3 AWS SQS client object.
    :param queue_url: The URL of the Queue to be created
    :param msg_retention_period_in_secs: See 'create_queue / 'Attributes':'MessageRetentionPeriod'
    :param visibility_timeout_in_secs: See 'create_queue / 'Attributes':'VisibilityTimeout'
    :return: None
    """
    attributes = dict(kwargs)
    attributes.update({"MessageRetentionPeriod": str(msg_retention_period_in_secs), "VisibilityTimeout": str(visibility_timeout_in_secs)})
    try:
        sqs_client.set_queue_attributes(QueueUrl=queue_url, Attributes=attributes)

        logger.info(f"Updated queue {queue_url!r} with attributes : {attributes!r}.")
    except ClientError:
        logger.exception("Couldn't update queue %s.", queue_url)
        raise
