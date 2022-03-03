# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional

from intelliflow.core.platform.definitions.aws.common import exponential_retry

logger = logging.getLogger(__name__)


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
