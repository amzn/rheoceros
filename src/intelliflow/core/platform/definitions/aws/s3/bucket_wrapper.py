# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import uuid
from typing import Set

from botocore.exceptions import ClientError, WaiterError

from intelliflow.core.platform.definitions.aws.common import (
    _check_statement_field_equality,
    _is_trust_policy_AWS_principle_deleted,
    is_waiter_timeout,
)

logger = logging.getLogger(__name__)

"""
Refer 
https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/python/example_code/s3/s3_basics/bucket_wrapper.py
"""

MAX_BUCKET_LEN = 63


def create_bucket(s3, name, region):
    """
    Create an Amazon S3 bucket with the specified name and in the specified Region.
    :param name: The name of the bucket to create. This name must be globally unique
                 and must adhere to bucket naming requirements.
    :param region: The Region in which to create the bucket. If this is not specified,
                   the Region configured in your shared credentials is used. If no
                   Region is configured, 'us-east-1' is used.
    :return: The newly created bucket.
    """

    try:
        if region != "us-east-1":
            bucket = s3.create_bucket(Bucket=name, CreateBucketConfiguration={"LocationConstraint": region})
        else:
            bucket = s3.create_bucket(Bucket=name)

        bucket.wait_until_exists()
        s3.meta.client.get_waiter("bucket_exists").wait(Bucket=name)

        logger.info("Created bucket '%s' in region=%s", bucket.name, s3.meta.client.meta.region_name)
    except ClientError as error:
        logger.exception("Couldn't create bucket named '%s' in region=%s.", name, region)
        if error.response["Error"]["Code"] == "IllegalLocationConstraintException":
            logger.error(
                "When the session Region is anything other than us-east-1, "
                "you must specify a LocationConstraint that matches the "
                "session Region. The current session Region is %s and the "
                "LocationConstraint Region is %s.",
                s3.meta.client.meta.region_name,
                region,
            )
        raise error
    else:
        return bucket


def bucket_exists(s3, bucket_name):
    """
    Determine whether a bucket with the specified name exists.
    :param bucket_name: The name of the bucket to check.
    :return: True when the bucket exists; otherwise, False.
    """
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
        logger.info("Bucket %s exists.", bucket_name)
        exists = True
    except ClientError:
        logger.warning("Bucket %s doesn't exist or you don't have access to it.", bucket_name)
        exists = False
    return exists


def get_buckets(s3):
    """
    Get the buckets in all Regions for the current account.
    :return: The list of buckets.
    """
    try:
        buckets = list(s3.buckets.all())
        logger.info("Got buckets: %s.", buckets)
    except ClientError:
        logger.exception("Couldn't get buckets.")
        raise
    else:
        return buckets


def get_bucket(s3, name):
    return s3.Bucket(name)


def get_bucket_by_list(s3, name):
    for bucket in get_buckets(s3):
        if bucket.name == name:
            return bucket


def delete_bucket(bucket):
    """
    Delete a bucket. The bucket must be empty or an error is raised.
    (see also `object_wrapper::empty_bucket`)
    :param bucket: The bucket to delete.
    """
    try:
        bucket.delete()
        try:
            bucket.wait_until_not_exists()
        except WaiterError as error:
            if not is_waiter_timeout(error):  # sometimes it takes time for S3 to delete, that is OK, we can move on.
                raise
        logger.info("Bucket %s successfully deleted.", bucket.name)
        return True
    except ClientError:
        logger.exception("Couldn't delete bucket %s.", bucket.name)
        raise


def remove_notification(s3, bucket_name, topic_arns: Set[str], queue_arns: Set[str], lambda_arns: Set[str]) -> None:
    """ref:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucketnotification
    so we first "load" a conf and then update and "put" it back.
    Requires:
    ["s3:GetBucketNotificationConfiguration", "s3:PutBucketNotificationConfiguration"]
    """
    if topic_arns or queue_arns or lambda_arns:
        try:
            bucket_notification = s3.BucketNotification(bucket_name)
            bucket_notification.load()
        except ClientError as err:
            logger.exception("Couldn't get bucket notification! bucket %s. Error: %s", bucket_name, str(err))
            raise
    else:
        logger.error("No valid bucket notifications were specified!")
        raise ValueError("No valid bucket notifications are valid!" "Supported inputs are SNS topic, SQS queue and Lambda ARNs.")

    current_topic_confs = bucket_notification.topic_configurations if bucket_notification.topic_configurations else []
    current_queue_confs = bucket_notification.queue_configurations if bucket_notification.queue_configurations else []
    current_lambda_confs = bucket_notification.lambda_function_configurations if bucket_notification.lambda_function_configurations else []

    data = {}
    if current_topic_confs:
        data.setdefault(
            "TopicConfigurations",
            [current_topic for current_topic in current_topic_confs if current_topic["TopicArn"] not in topic_arns]
            if topic_arns
            else current_topic_confs,
        )
    if current_queue_confs:
        data.setdefault(
            "QueueConfigurations",
            [current_queue for current_queue in current_queue_confs if current_queue["QueueArn"] not in queue_arns]
            if queue_arns
            else current_queue_confs,
        )

    if current_lambda_confs:
        data.setdefault(
            "LambdaFunctionConfigurations",
            [current_lambda for current_lambda in current_lambda_confs if current_lambda["LambdaFunctionArn"] not in lambda_arns]
            if lambda_arns
            else current_lambda_confs,
        )

    if data:
        try:
            bucket_notification = s3.BucketNotification(bucket_name)
            response = bucket_notification.put(NotificationConfiguration=data)
            logger.info("Bucket notification updated successfully! bucket: %s notification_cof: %s.", bucket_name, data)
            return response
        except ClientError as err:
            logger.exception("Couldn't update bucket notification! bucket %s. Error: %s", bucket_name, str(err))
            raise


def put_notification(s3, bucket_name, topic_arns, queue_arns, lambda_arns, events=["s3:ObjectCreated:*"]) -> None:
    """ref:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#bucketnotification
    we need add_or_update semantics for this API.
    so we first "load" a conf and then update and "put" it back.
    Requires:
    ["s3:GetBucketNotificationConfiguration", "s3:PutBucketNotificationConfiguration"]
    """
    if topic_arns or queue_arns or lambda_arns:
        try:
            bucket_notification = s3.BucketNotification(bucket_name)
            bucket_notification.load()
        except ClientError as err:
            logger.exception("Couldn't get bucket notification! bucket %s. Error: %s", bucket_name, str(err))
            raise
    else:
        logger.error("No valid bucket notifications were specified!")
        raise ValueError("No valid bucket notifications are valid!" "Supported inputs are SNS topic, SQS queue and Lambda ARNs.")

    current_topic_arns = (
        {conf["TopicArn"] for conf in bucket_notification.topic_configurations} if bucket_notification.topic_configurations else {}
    )
    current_queue_arns = (
        {conf["QueueArn"] for conf in bucket_notification.queue_configurations} if bucket_notification.queue_configurations else {}
    )
    current_lambda_arns = (
        {conf["LambdaFunctionArn"] for conf in bucket_notification.lambda_function_configurations}
        if bucket_notification.lambda_function_configurations
        else {}
    )

    data = {}
    if topic_arns or current_topic_arns:
        default = data.setdefault(
            "TopicConfigurations", bucket_notification.topic_configurations if bucket_notification.topic_configurations else []
        )
        if topic_arns:
            default.extend([{"TopicArn": topic, "Events": events} for topic in topic_arns if topic not in current_topic_arns])

    if queue_arns or current_queue_arns:
        default = data.setdefault(
            "QueueConfigurations", bucket_notification.queue_configurations if bucket_notification.queue_configurations else []
        )
        if queue_arns:
            default.extend([{"QueueArn": queue, "Events": events} for queue in queue_arns if queue not in current_queue_arns])

    if lambda_arns or current_lambda_arns:
        default = data.setdefault(
            "LambdaFunctionConfigurations",
            bucket_notification.lambda_function_configurations if bucket_notification.lambda_function_configurations else [],
        )
        if lambda_arns:
            default.extend(
                [{"LambdaFunctionArn": _lambda, "Events": events} for _lambda in lambda_arns if _lambda not in current_lambda_arns]
            )

    if data:
        try:
            bucket_notification = s3.BucketNotification(bucket_name)
            response = bucket_notification.put(NotificationConfiguration=data)
            logger.info("Bucket notification updated successfully! bucket: %s notification_cof: %s.", bucket_name, data)
            return response
        except ClientError as err:
            logger.exception("Couldn't update bucket notification! bucket %s. Error: %s", bucket_name, str(err))
            raise


def get_acl(s3, bucket_name):
    """
    Get the ACL of the specified bucket.
    :param bucket_name: The name of the bucket to retrieve.
    :return: The ACL of the bucket.
    """
    try:
        acl = s3.Bucket(bucket_name).Acl()
        logger.info("Got ACL for bucket %s owned by %s.", bucket_name, acl.owner["DisplayName"])
    except ClientError:
        logger.exception("Couldn't get ACL for bucket %s.", bucket_name)
        raise
    else:
        return acl


def put_cors(s3, bucket_name, cors_rules):
    """
    Apply CORS rules to a bucket. CORS rules specify the HTTP actions that are
    allowed from other domains.
    :param bucket_name: The name of the bucket where the rules are applied.
    :param cors_rules: The CORS rules to apply.
    """
    try:
        s3.Bucket(bucket_name).Cors().put(CORSConfiguration={"CORSRules": cors_rules})
        logger.info("Put CORS rules %s for bucket '%s'.", cors_rules, bucket_name)
        return True
    except ClientError:
        logger.exception("Couldn't put CORS rules for bucket %s.", bucket_name)
        raise


def get_cors(s3, bucket_name):
    """
    Get the CORS rules for the specified bucket.
    :param bucket_name: The name of the bucket to check.
    :return The CORS rules for the specified bucket.
    """
    try:
        cors = s3.Bucket(bucket_name).Cors()
        logger.info("Got CORS rules %s for bucket '%s'.", cors.cors_rules, bucket_name)
    except ClientError:
        logger.exception(("Couldn't get CORS for bucket %s.", bucket_name))
        raise
    else:
        return cors


def delete_cors(s3, bucket_name):
    """
    Delete the CORS rules from the specified bucket.
    :param bucket_name: The name of the bucket to update.
    """
    try:
        s3.Bucket(bucket_name).Cors().delete()
        logger.info("Deleted CORS from bucket '%s'.", bucket_name)
        return True
    except ClientError:
        logger.exception("Couldn't delete CORS from bucket '%s'.", bucket_name)
        raise


def update_policy(s3, bucket_name, policy_with_updated_statements, removed_statements=None) -> None:
    """
    Add or update the bucket policy.
    Statements should have
    :param bucket_name: The name of the bucket to receive the policy.
    :param policy_with_updated_statements: The policy to be added to the existing policy (if any) or set as the new policy
    :param removed_statements: statements to be removed from the policy (each should contain a 'Sid')
    """
    try:
        policy = s3.Bucket(bucket_name).Policy()
        current_policy_doc = None
        try:
            policy.load()
            if policy.policy:
                current_policy_doc = json.loads(policy.policy)
        except ClientError as policy_error:
            if policy_error.response["Error"]["Code"] not in ["NoSuchBucketPolicy"]:
                raise

        if not current_policy_doc:
            if not policy_with_updated_statements:
                return
            current_policy_doc = policy_with_updated_statements
        else:
            current_statements = current_policy_doc["Statement"]
            new_statements = policy_with_updated_statements["Statement"] if policy_with_updated_statements else []
            removed_sids = {r["Sid"] for r in removed_statements if r.get("Sid", None)} if removed_statements else set()
            if not new_statements and not removed_sids:
                return
            if isinstance(current_statements, list):
                # remove deleted entities
                removed_statement_indexes = []
                for i, statement in enumerate(current_statements):
                    if "AWS" in statement["Principal"]:
                        trusted_entities = statement["Principal"]["AWS"]
                        trusted_entities = trusted_entities if isinstance(trusted_entities, list) else [trusted_entities]
                        remaining_entities = set()
                        for trusted_entity in trusted_entities:
                            if not _is_trust_policy_AWS_principle_deleted(trusted_entity):
                                remaining_entities.add(trusted_entity)
                        if not remaining_entities:
                            removed_statement_indexes.append(i)
                        statement["Principal"]["AWS"] = list(remaining_entities)

                # delete in reverse order so that iteration won't mess up
                for i in sorted(removed_statement_indexes, reverse=True):
                    del current_statements[i]

                # we might have already created our entries in this doc,
                # let's try to find them and update them.
                # if not found, add them.
                for new_statement in new_statements:
                    statement_found = False
                    for current_statement in current_statements:
                        if "Sid" in current_statement and "Sid" in new_statement and new_statement["Sid"] == current_statement["Sid"]:
                            current_statement["Effect"] = new_statement["Effect"]
                            current_statement["Principal"] = new_statement["Principal"]
                            current_statement["Action"] = new_statement["Action"]
                            current_statement["Resource"] = new_statement["Resource"]
                            statement_found = True
                            break
                        elif "Sid" not in current_statement and "Sid" not in new_statement:
                            if new_statement["Effect"] == current_statement["Effect"] and _check_statement_field_equality(
                                current_statement, new_statement, "Principal"
                            ):
                                current_statement["Action"] = new_statement["Action"]
                                current_statement["Resource"] = new_statement["Resource"]
                                statement_found = True
                                break
                    if not statement_found:
                        current_statements.append(new_statement)

                if removed_sids:
                    statements = []
                    for statement in current_statements:
                        if statement.get("Sid", None) not in removed_sids:
                            statements.append(statement)

                    current_policy_doc["Statement"] = statements
            else:
                statement = current_statements
                if "AWS" in statement["Principal"]:
                    trusted_entities = statement["Principal"]["AWS"]
                    trusted_entities = trusted_entities if isinstance(trusted_entities, list) else [trusted_entities]
                    remaining_entities = set()
                    for trusted_entity in trusted_entities:
                        if not _is_trust_policy_AWS_principle_deleted(trusted_entity):
                            remaining_entities.add(trusted_entity)
                    if remaining_entities:
                        statement["Principal"]["AWS"] = list(remaining_entities)
                    else:
                        current_statements = None

                # this might be our first time updating it
                current_policy_doc["Statement"] = (
                    [current_statements] if current_statements else [] + [new_statement for new_statement in new_statements]
                )

        s3.Bucket(bucket_name).Policy().put(Policy=json.dumps(current_policy_doc))
        policy = s3.Bucket(bucket_name).Policy()
        logger.info("Put policy %s for bucket '%s'.", policy, bucket_name)
    except ClientError:
        # logger.exception("Couldn't update policy for bucket '%s'.", bucket_name)
        raise


def put_policy(s3, bucket_name, policy):
    """
    Apply a security policy to a bucket. Policies typically grant users the ability
    to perform specific actions, such as listing the objects in the bucket.
    :param bucket_name: The name of the bucket to receive the policy.
    :param policy: The policy to apply to the bucket.
    """
    try:
        # The policy must be in JSON format.
        s3.Bucket(bucket_name).Policy().put(Policy=json.dumps(policy))
        logger.info("Put policy %s for bucket '%s'.", policy, bucket_name)
        return True
    except ClientError:
        # logger.exception("Couldn't apply policy to bucket '%s'.", bucket_name)
        raise


def get_policy(s3, bucket_name):
    """
    Get the security policy of a bucket.
    :param bucket_name: The bucket to retrieve.
    :return: The security policy of the specified bucket.
    """
    try:
        policy = s3.Bucket(bucket_name).Policy()
        policy.load()
        logger.info("Got policy %s for bucket '%s'.", policy.policy, bucket_name)
    except ClientError:
        logger.exception("Couldn't get policy for bucket '%s'.", bucket_name)
        raise
    else:
        return json.loads(policy.policy)


def delete_policy(s3, bucket_name):
    """
    Delete the security policy from the specified bucket.
    :param bucket_name: The name of the bucket to update.
    """
    try:
        s3.Bucket(bucket_name).Policy().delete()
        logger.info("Deleted policy for bucket '%s'.", bucket_name)
        return True
    except ClientError:
        logger.exception("Couldn't delete policy for bucket '%s'.", bucket_name)
        raise


def put_lifecycle_configuration(s3, bucket_name, lifecycle_rules):
    """
    Apply a lifecycle configuration to a bucket. The lifecycle configuration can
    be used to archive or delete the objects in the bucket according to specified
    parameters, such as a number of days.
    :param bucket_name: The name of the bucket to update.
    :param lifecycle_rules: The lifecycle rules to apply.
    """
    try:
        s3.Bucket(bucket_name).LifecycleConfiguration().put(LifecycleConfiguration={"Rules": lifecycle_rules})
        logger.info("Put lifecycle rules %s for bucket '%s'.", lifecycle_rules, bucket_name)
        return True
    except ClientError:
        logger.exception("Couldn't put lifecycle rules for bucket '%s'.", bucket_name)
        raise


def get_lifecycle_configuration(s3, bucket_name):
    """
    Get the lifecycle configuration of the specified bucket.
    :param bucket_name: The name of the bucket to retrieve.
    :return: The lifecycle rules of the specified bucket.
    """
    try:
        config = s3.Bucket(bucket_name).LifecycleConfiguration()
        logger.info("Got lifecycle rules %s for bucket '%s'.", config.rules, bucket_name)
    except:
        logger.exception("Couldn't get lifecycle configuration for bucket '%s'.", bucket_name)
        raise
    else:
        return config.rules


def delete_lifecycle_configuration(s3, bucket_name):
    """
    Remove the lifecycle configuration from the specified bucket.
    :param bucket_name: The name of the bucket to update.
    """
    try:
        s3.Bucket(bucket_name).LifecycleConfiguration().delete()
        logger.info("Deleted lifecycle configuration for bucket '%s'.", bucket_name)
        return True
    except ClientError:
        logger.exception("Couldn't delete lifecycle configuration for bucket '%s'.", bucket_name)
        raise
