# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging

from botocore.exceptions import ClientError

from intelliflow.core.platform.definitions.aws.common import get_code_for_exception

logger = logging.getLogger(__name__)


def register_scalable_target(auto_scaling_client, service_name, resource_id, scalable_dimension, min_capacity, max_capacity):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/application-autoscaling.html#ApplicationAutoScaling.Client.register_scalable_target
    :param auto_scaling_client:
    :param service_name:
    :param resource_id:
    :param scalable_dimension:
    :param min_capacity:
    :param max_capacity:
    :return:
    """

    try:
        auto_scaling_client.register_scalable_target(
            ServiceNamespace=service_name,
            ResourceId=resource_id,
            ScalableDimension=scalable_dimension,
            MinCapacity=min_capacity,
            MaxCapacity=max_capacity,
        )

        logger.info(
            "Auto Scaling successfully registered for %s"
            "resource id: %s with dimension: %s"
            "and with Min Capacity: %s, Max Capacity: %s",
            service_name,
            resource_id,
            scalable_dimension,
            min_capacity,
            max_capacity,
        )
    except ClientError:
        logger.exception(
            "Couldn't register autoscaling with service %s of " "resource: %s with dimension %s",
            service_name,
            resource_id,
            scalable_dimension,
        )
        raise


def put_target_tracking_scaling_policy(auto_scaling_client, service_name, resource_id, scalable_dimension, policy_dict):
    """Refer
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/application-autoscaling.html#ApplicationAutoScaling.Client.put_scaling_policy
    """
    try:
        auto_scaling_client.put_scaling_policy(
            ServiceNamespace=service_name,
            ResourceId=resource_id,
            PolicyType="TargetTrackingScaling",
            PolicyName=policy_dict["PolicyName"],
            ScalableDimension=scalable_dimension,
            TargetTrackingScalingPolicyConfiguration=policy_dict["PolicyConfig"],
        )

        logger.info(
            "Successfully put target tracking auto scaling policy with %s" "for resource %s, with policy %s and dimension %s",
            service_name,
            resource_id,
            json.dumps(policy_dict),
            scalable_dimension,
        )
    except ClientError as error:
        error_code = get_code_for_exception(error)
        if error_code in ["ValidationException"]:
            raise Exception(
                f"Could not put target tracking auto-scaling policy for {resource_id!r}! "
                "If you have previously set the auto-scaling using a different target (e.g manually via "
                "AWS Console) other than RheocerOS managed target please remove and redo this action "
                "again."
            ) from error
        raise


# TODO add alarms related with scalable policy
# Deregister will not delete the alarms
def deregister_scalable_target(auto_scaling_client, service_name, resource_id, scalable_dimension):
    """
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/application-autoscaling.html#ApplicationAutoScaling.Client.deregister_scalable_target
    :param auto_scaling_client:
    :param service_name:
    :param resource_id:
    :param scalable_dimension:
    :return:
    """

    try:
        auto_scaling_client.deregister_scalable_target(
            ServiceNamespace=service_name, ResourceId=resource_id, ScalableDimension=scalable_dimension
        )
        logger.info(
            "Successfully deregistered scalable target with %s for resource %s for dimension %s",
            service_name,
            resource_id,
            scalable_dimension,
        )
    except ClientError:
        raise


def delete_target_tracking_scaling_policy(auto_scaling_client, service_name, resource_id, scalable_dimension, policy_name):
    """Refer
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/application-autoscaling.html#ApplicationAutoScaling.Client.delete_scaling_policy
    """
    try:
        auto_scaling_client.delete_scaling_policy(
            PolicyName=policy_name, ServiceNamespace=service_name, ResourceId=resource_id, ScalableDimension=scalable_dimension
        )

        logger.info(
            "Successfully deleted target tracking auto scaling policy with %s" "for resource %s, with policy %s and dimension %s",
            service_name,
            resource_id,
            policy_name,
            scalable_dimension,
        )
    except ClientError:
        logger.exception("Couldnt delete target tracking autoscaling policy with %s, for resource %s", service_name, resource_id)
        raise
