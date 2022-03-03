# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
from typing import Set, Tuple

from botocore.exceptions import ClientError

from intelliflow.core.platform.definitions.aws.common import _is_trust_policy_AWS_principle_deleted

logger = logging.getLogger(__name__)


_allow_block_sid = "DONOT_DELETE_allow_use_of_the_key"
_admin_sid = "DONOT_DELETE_admin_access"

# https://docs.aws.amazon.com/kms/latest/developerguide/deleting-keys.html
KMS_MIN_DELETION_WAITING_PERIOD_IN_DAYS = 7
KMS_MAX_DELETION_WAITING_PERIOD_IN_DAYS = 30


def create_cmk(kms_client, policy: str, desc: str = "RheocerOS CMK") -> Tuple[str, str]:
    """Create a KMS (Symmetric) Customer Master Key

    The created CMK is a Customer-managed key stored in AWS KMS.

    Please note that a brand new key is 'enabled' by default.
    https://docs.aws.amazon.com/kms/latest/developerguide/key-state.html

    :param desc: key description
    :return Tuple(KeyId, KeyArn) where:
        KeyId: AWS globally-unique string ID
        KeyArn: Amazon Resource Name of the CMK
    """

    try:
        response = kms_client.create_key(Policy=policy, Description=desc)
    except ClientError:
        logger.info("Couldn't create new KMS CMK (desc=%s).", desc)
        raise
    else:
        # Return the key ID and ARN
        return response["KeyMetadata"]["KeyId"], response["KeyMetadata"]["Arn"]


def schedule_cmk_deletion(kms_client, id_or_arn: str, pending_window_in_days=KMS_MIN_DELETION_WAITING_PERIOD_IN_DAYS) -> "datetime":
    """Refer
       https://boto3.amazonaws.com/v1/documentation/api/1.9.42/reference/services/kms.html#KMS.Client.schedule_key_deletion

    :returns <datetime> 'DeletionDate' from KMS response; by when the key will be irrevocably deleted.
    """
    if pending_window_in_days < KMS_MIN_DELETION_WAITING_PERIOD_IN_DAYS or pending_window_in_days > KMS_MAX_DELETION_WAITING_PERIOD_IN_DAYS:
        raise ValueError(
            f"Please provide a 'pending_window_in_days' value between 7 and 30 (inclusive) for the " f"deletion of KMS CMK {id_or_arn}."
        )
    try:
        response = kms_client.schedule_key_deletion(KeyId=id_or_arn, PendingWindowInDays=pending_window_in_days)
    except ClientError as err:
        if err.response["Error"]["Code"] not in ["NotFoundException", "NotFound", "ResourceNotFoundException"]:
            # see it was already in PendingDeletion state
            key_metadata = kms_client.describe_key(KeyId=id_or_arn)
            if "KeyMetadata" in key_metadata and key_metadata["KeyMetadata"]["KeyState"] == "PendingDeletion":
                return key_metadata["KeyMetadata"]["DeletionDate"]
        raise
    else:
        return response["DeletionDate"]


def get_cmk(kms_client, id_or_arn_or_alias: str) -> Tuple[str, str]:
    """refer
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/kms.html#KMS.Client.describe_key

    :return Tuple(KeyId, KeyArn) where:
        KeyId: AWS globally-unique string ID
        KeyArn: Amazon Resource Name of the CMK
    """
    try:
        response = kms_client.describe_key(KeyId=id_or_arn_or_alias)
    except ClientError as err:
        if err.response["Error"]["Code"] not in ["NotFoundException"]:
            logger.info("Couldn't get KMS CMK (alias=%s).", id_or_arn_or_alias)
            raise
        return None, None

    if "KeyMetadata" in response:
        return response["KeyMetadata"]["KeyId"], response["KeyMetadata"]["Arn"]

    return None, None


def create_alias(kms_client, alias_name: str, target_key_id: str) -> None:
    try:
        response = kms_client.create_alias(AliasName=alias_name, TargetKeyId=target_key_id)
    except ClientError:
        logger.info("Couldn't create new alias '%s' for KMS CMK.", alias_name)
        raise


def update_alias(kms_client, alias_name: str, target_key_id: str) -> None:
    try:
        response = kms_client.update_alias(AliasName=alias_name, TargetKeyId=target_key_id)
    except ClientError:
        raise


def delete_alias(kms_client, alias_name: str):
    try:
        kms_client.delete_alias(AliasName=alias_name)
    except ClientError:
        logger.info("Couldn't delete KMS alias '%s'.", alias_name)
        raise


def create_default_policy(account_id: str, users_to_be_added: Set[str], admins: Set[str], trust_same_account=False) -> str:
    default_policy = {"Version": "2012-10-17", "Id": "IntelliFlow-CMK-policy", "Statement": []}

    if admins or trust_same_account:
        admin_list = list(admins)
        if trust_same_account:
            admin_list.append(f"arn:aws:iam::{account_id}:root")
        # see https://docs.aws.amazon.com/kms/latest/developerguide/key-policies.html#key-policy-default-allow-root-enable-iam
        default_policy["Statement"].append(
            {"Sid": _admin_sid, "Effect": "Allow", "Principal": {"AWS": admin_list}, "Action": "kms:*", "Resource": "*"}
        )
    elif not users_to_be_added:
        raise ValueError(f"Cannot risk KMS CMK lockout due to no AWS entity as a trustee in the policy.")

    if users_to_be_added:
        current_statements = default_policy["Statement"]

        new_aws_entity_list = list(users_to_be_added)
        current_statements.append(
            {
                "Sid": _allow_block_sid,
                "Effect": "Allow",
                "Principal": {"AWS": new_aws_entity_list},
                "Action": ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"],
                "Resource": "*",
            }
        )

    return json.dumps(default_policy)


def put_cmk_policy(
    kms_client,
    key_id: str,
    account_id: str,
    users_to_be_added: Set[str] = set(),
    users_to_be_removed: Set[str] = set(),
    trust_same_account: bool = None,
) -> None:
    default_policy = {"Version": "2012-10-17", "Id": f"IntelliFlow-CMK-{key_id}-policy", "Statement": []}
    same_account_root = f"arn:aws:iam::{account_id}:root"
    # https://docs.aws.amazon.com/kms/latest/developerguide/programming-key-policies.html
    # the only valid policy name is 'default'
    policy_name = "default"

    change_detected = False
    current_policy_doc = None
    try:
        response = kms_client.get_key_policy(KeyId=key_id, PolicyName=policy_name)
        current_policy_doc = json.loads(response["Policy"])
    except ClientError as policy_error:
        if policy_error.response["Error"]["Code"] not in ["NotFoundException"]:
            raise
        current_policy_doc = default_policy
        change_detected = True

    current_statements = current_policy_doc["Statement"]
    new_aws_entity_set = set(users_to_be_added)
    allow_block_found = False
    admin_block_found = False
    removed_statement_indexes = []
    for i, statement in enumerate(current_statements):
        sid = statement.get("Sid", None)
        if sid == _allow_block_sid:
            allow_block_found = True
            current_aws_entity_list = statement["Principal"]["AWS"]
            current_aws_entity_list = [current_aws_entity_list] if isinstance(current_aws_entity_list, str) else current_aws_entity_list
            # first check deleted entities
            for current_entity in current_aws_entity_list:
                if _is_trust_policy_AWS_principle_deleted(current_entity):
                    users_to_be_removed.add(current_entity)
            if users_to_be_added or users_to_be_removed:
                if bool(new_aws_entity_set - set(current_aws_entity_list)) or set(current_aws_entity_list).intersection(
                    users_to_be_removed
                ):
                    change_detected = True
                for current_aws_entity in current_aws_entity_list:
                    if current_aws_entity not in users_to_be_removed:
                        new_aws_entity_set.add(current_aws_entity)
                if new_aws_entity_set:
                    statement["Principal"]["AWS"] = list(new_aws_entity_set)
                else:
                    removed_statement_indexes.append(i)
        elif sid == _admin_sid:
            admin_block_found = True
            current_admins_set = statement["Principal"]["AWS"]
            current_admins_set = {current_admins_set} if isinstance(current_admins_set, str) else set(current_admins_set)
            new_admins_set = set(current_admins_set)
            # - clean-up if a zombie entity is still here (due to manual modifications, deletions, etc)
            # - clean-up deleted users from admin list as well (if in there)
            for current_entity in current_admins_set:
                if _is_trust_policy_AWS_principle_deleted(current_entity) or current_entity in users_to_be_removed:
                    change_detected = True
                    new_admins_set.remove(current_entity)
            if trust_same_account is not None:
                if trust_same_account is True and same_account_root not in new_admins_set:
                    change_detected = True
                    new_admins_set.add(same_account_root)
                elif trust_same_account is False and same_account_root in new_admins_set:
                    change_detected = True
                    new_admins_set.remove(same_account_root)
            if new_admins_set != current_admins_set:
                statement["Principal"]["AWS"] = list(new_admins_set)

    # delete in reverse order so that iteration won't mess up
    for i in sorted(removed_statement_indexes, reverse=True):
        del current_statements[i]

    if not allow_block_found and new_aws_entity_set:
        change_detected = True
        current_statements.append(
            {
                "Sid": _allow_block_sid,
                "Effect": "Allow",
                "Principal": {"AWS": list(new_aws_entity_set)},
                "Action": ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"],
                "Resource": "*",
            }
        )

    if not admin_block_found and trust_same_account is True:
        change_detected = True
        # this normally should not happen (unless the policy had some other admin level statement)
        # we still want to have our own well-defined block.
        default_policy["Statement"].append(
            {"Sid": _admin_sid, "Effect": "Allow", "Principal": {"AWS": same_account_root}, "Action": "kms:*", "Resource": "*"}
        )

    if change_detected:
        try:
            response = kms_client.put_key_policy(
                KeyId=key_id, PolicyName=policy_name, Policy=json.dumps(current_policy_doc), BypassPolicyLockoutSafetyCheck=False
            )
        except ClientError:
            logger.info("Couldn't update policy for KMS key '%s'.", key_id)
            raise


def enable_key_rotation(kms_client, key_id: str):
    try:
        response = kms_client.enable_key_rotation(KeyId=key_id)
    except ClientError:
        logger.info("Couldn't enable key rotation for KMS key '%s'.", key_id)
        raise
