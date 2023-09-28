# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import uuid
from enum import Enum
from typing import Any, Dict, Optional

from intelliflow.core.platform.definitions.aws.common import exponential_retry

logger = logging.getLogger(__name__)


def topic_exists(sns, topic_arn: str) -> bool:
    topic_exists: bool = False
    try:
        exponential_retry(sns.get_topic_attributes, ["InternalErrorException"], TopicArn=topic_arn)
        topic_exists = True
    except sns.exceptions.NotFoundException:
        topic_exists = False
    return topic_exists


def create_topic_arn(region: str, account_id: str, topic_name: str) -> str:
    return f"arn:aws:sns:{region}:{account_id}:{topic_name}"


def get_topic_policy(sns, topic_arn: str) -> Optional[Dict[str, Any]]:
    policy_str = exponential_retry(sns.get_topic_attributes, ["InternalErrorException", "NotFoundException"], TopicArn=topic_arn,)[
        "Attributes"
    ].get("Policy", None)
    return json.loads(policy_str) if policy_str else None


class SNSPublisherType(str, Enum):
    S3_BUCKET = "s3_bucket"


def add_publisher_to_topic(sns, topic_arn: str, publisher_type: SNSPublisherType, publisher_resource: str) -> None:
    current_policy = get_topic_policy(sns, topic_arn)
    current_statements = current_policy.get("Statement", []) if current_policy else []

    topic_root_policy = None
    if publisher_type == SNSPublisherType.S3_BUCKET:
        source_bucket = publisher_resource

        bucket_already_in_policy = False
        for s in current_statements:
            if (
                "s3.amazonaws.com" in s.get("Principal", dict()).get("Service", "").lower()
                and f"arn:aws:s3:*:*:{source_bucket}" == s.get("Condition", dict()).get("ArnLike", dict()).get("AWS:SourceArn", "").lower()
            ):
                bucket_already_in_policy = True
                break

        if not bucket_already_in_policy:
            topic_root_policy = {
                "Version": "2012-10-17",
                "Id": str(uuid.uuid1()),
                "Statement": current_statements
                + [
                    {
                        "Sid": str(uuid.uuid1()),
                        "Effect": "Allow",
                        "Principal": {"Service": "s3.amazonaws.com"},
                        "Action": "sns:Publish",
                        "Resource": topic_arn,
                        "Condition": {"ArnLike": {"AWS:SourceArn": f"arn:aws:s3:*:*:{source_bucket}"}},
                    },
                ],
            }
    else:
        raise NotImplementedError(f"Producer type {publisher_type!r} registration to SNS not supported yet!")

    if topic_root_policy:
        exponential_retry(
            sns.set_topic_attributes,
            ["InternalErrorException", "NotFoundException"],
            TopicArn=topic_arn,
            AttributeName="Policy",
            AttributeValue=json.dumps(topic_root_policy),
        )


def remove_publisher_from_topic(sns, topic_arn: str, publisher_type: SNSPublisherType, publisher_resource: str) -> None:
    current_policy = get_topic_policy(sns, topic_arn)
    current_statements = current_policy.get("Statement", []) if current_policy else []

    topic_root_policy = None
    if publisher_type == SNSPublisherType.S3_BUCKET:
        source_bucket = publisher_resource

        bucket_already_in_policy = False
        new_statements = []
        for s in current_statements:
            if (
                "s3.amazonaws.com" in s.get("Principal", dict()).get("Service", "").lower()
                and source_bucket in s.get("Condition", dict()).get("ArnLike", dict()).get("AWS:SourceArn", "").lower()
            ):
                bucket_already_in_policy = True
            else:
                new_statements.append(s)

        if bucket_already_in_policy:
            topic_root_policy = {"Version": "2012-10-17", "Id": str(uuid.uuid1()), "Statement": new_statements}
    else:
        raise NotImplementedError(f"Producer type {publisher_type!r} registration to SNS not supported yet!")

    if topic_root_policy:
        exponential_retry(
            sns.set_topic_attributes,
            ["InternalErrorException", "NotFoundException"],
            TopicArn=topic_arn,
            AttributeName="Policy",
            AttributeValue=json.dumps(topic_root_policy),
        )


def find_subscription(sns, topic_arn: str, endpoint: str) -> Optional[str]:
    """Find the subscription for the endpoint (if any)

    :param topic_arn: arn for SNS topic
    :param endpoint: resource path/URL/ARN used during the creation of the subscription.
           Refer
           https://boto3.amazonaws.com/v1/documentation/api/1.9.42/reference/services/sns.html#SNS.Client.subscribe
           for possible formats of an endpoint depending on the protocol used during subscription.
           Examples: ARN for lambda or sqs, URL for http or https, etc.

    :returns the subscription arn if 'endpoint' is already subscribed to 'topic_arn'
    """
    retryables = {"InternalErrorException"}
    next_token = None
    while True:
        try:
            if next_token:
                response = exponential_retry(sns.list_subscriptions_by_topic, retryables, TopicArn=topic_arn, NextToken=endpoint)
            else:
                response = exponential_retry(sns.list_subscriptions_by_topic, retryables, TopicArn=topic_arn)
        except sns.exceptions.NotFoundException:
            return None

        subscriptions = response.get("Subscriptions", None)
        if subscriptions:
            for subs in subscriptions:
                if subs.get("Endpoint", None) == endpoint:
                    return subs["SubscriptionArn"]

        next_token = response.get("NextToken", None)
        if next_token is None:
            break
