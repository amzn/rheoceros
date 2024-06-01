# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import codecs
import datetime
import importlib
import json
import logging
import os
import time
from enum import Enum, unique
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union, cast, overload

import boto3
import botocore
from botocore.exceptions import *
from dateutil.tz import tzlocal

from intelliflow.core.entity import CoreData

module_logger = logging.getLogger(__name__)

# TODO discuss with the team if IF_ROLE should be tied with app
#      (if so, will impact AWSConfiguration API and the following role format [{1} -> app_id / context_id])
# IF_ROLE_FORMAT = "arn:aws:iam::{0}:role/{1}-IntelliFlow"
# IF_ASSUMED_ROLE_FORMAT = "arn:aws:sts::{0}:assumed-role/{1}-IntelliFlow/{2}"

# TODO post-MVP use more granular IAM scheme
# https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-quotas.html
AWS_MAX_ROLE_NAME_SIZE = 64
# users will be expected to provide credentials for (or to create IF_DEV_ROLE_FORMAT)
IF_DEV_ROLE_NAME_FORMAT = "{0}-{1}-IntelliFlowDevRole"
IF_DEV_ROLE_FORMAT = "arn:aws:iam::{0}:role/{1}-{2}-IntelliFlowDevRole"
IF_EXE_ROLE_NAME_FORMAT = "{0}-{1}-IntelliFlowExeRole"
IF_EXE_ROLE_FORMAT = "arn:aws:iam::{0}:role/{1}-{2}-IntelliFlowExeRole"

IF_DEV_POLICY_NAME_FORMAT = "{0}-DevelopmentPolicy"
IF_EXE_POLICY_NAME_FORMAT = "{0}-ExecutionPolicy"
IF_DEV_EXE_TRANSITION_POLICY_NAME_FORMAT = "{0}-DevToExecPolicy"
IF_DEV_REMOVED_DRIVERS_TEMP_POLICY_NAME_FORMAT = "{0}-DevelopmentPolicy-Removed-Temp"

AWS_STS_ARN_ROOT = "arn:aws:sts"
AWS_ASSUMED_ROLE_ARN_ROOT = "arn:aws:sts::{0}:assumed-role/{1}"

# IAM max
MAX_ASSUME_ROLE_DURATION = 43200  # 12 hours
# Below IAM limit is in effect in the context of collaboration (assuming role of an upstream app):
# "The requested DurationSeconds exceeds the 1 hour session limit for roles assumed by role chaining."
MAX_CHAINED_ROLE_DURATION = 3600


@unique
class CommonParams(str, Enum):
    BOTO_SESSION = "AWS_BOTO_SESSION"
    HOST_BOTO_SESSION = "HOST_AWS_BOTO_SESSION"
    REGION = "AWS_REGION"
    ACCOUNT_ID = "AWS_ACCOUNT_ID"
    ASSUMED_ROLE_USER = "ASSUMED_ROLE_USER"
    IF_DEV_ROLE = "INTELLIFLOW_IAM_DEV_ROLE"
    IF_EXE_ROLE = "INTELLIFLOW_IAM_EXE_ROLE"
    IF_ACCESS_PAIR = "AWS_ACCESS_PAIR"
    IF_ADMIN_ROLE = "INTELLIFLOW_IAM_ADMIN_ROLE"
    IF_ADMIN_ACCESS_PAIR = "AWS_IF_ADMIN_ACCESS_PAIR"
    AUTHENTICATE_WITH_DEV_ROLE = "AUTHENTICATE_WITH_IAM_DEV_ROLE"
    AUTHENTICATE_WITH_ACCOUNT_ID = "AUTHENTICATE_WITH_ACCOUNT_ID"
    AUTO_UPDATE_DEV_ROLE = "UPDATE_IAM_DEV_ROLE"
    DEFAULT_CREDENTIALS_AS_ADMIN = "DEFAULT_AS_ADMIN"


class AWSAccessPair(CoreData):
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str) -> None:
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key


IF_DEFAULT_SESSION_DURATION = 36000  # estimated avg dev session / user activity + provisioning time


def get_code_for_exception(error):
    if isinstance(error, ClientError) and "Code" in error.response["Error"]:
        return error.response["Error"]["Code"]
    elif isinstance(error, WaiterError) and "Error" in error.last_response:
        return error.last_response["Error"]["Code"]
    elif hasattr(error, "error_code"):
        return error.error_code

    return error.__class__.__name__


def get_aws_account_id_from_arn(arn: str) -> str:
    return arn.split(":")[4]


def get_aws_region_from_arn(arn: str) -> str:
    return arn.split("/")[1]


def is_waiter_timeout(error: WaiterError) -> bool:
    return isinstance(error, WaiterError) and "Error" not in error.last_response


# common AWS service errors
AWS_COMMON_RETRYABLE_ERRORS = [
    "TooManyRequestsException",
    "Throttling",
    "ThrottlingException",
    "RequestLimitExceeded",
    "Unavailable",
    "InternalFailure",
    "InternalError",
    "InternalServerError",
    "LimitExceededException",
    "ServiceUnavailable",
    "ServiceUnavailableException",
    # TODO check https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/CommonErrors.html
    # Now add botocore common retryable errors
    "ConnectTimeoutError",
    "ReadTimeoutError",
    # We evaluate the following as retryable due to eventual consistency
    "InvalidClientTokenId",
    "UnrecognizedClientException",
    "InvalidAccessKeyId",
]


MAX_SLEEP_INTERVAL_PARAM = "_max_sleep_time_in_secs"
MAX_SLEEP_INTERVAL_DEFAULT = 64 + 1


def exponential_retry(func, service_retryable_errors, *func_args, **func_kwargs):
    """
    Retries the specified function with a simple exponential backoff algorithm.
    :param func: The function to retry.
    :param service_retryable_errors: AWS service specific retryable error codes. These are added to an internal list
                                    of AWS common retryable errors to get a final list of retryable errors. Anything else
                                    is raised without a retry.
    :param func_args: The positional arguments to pass to the function.
    :param func_kwargs: The keyword arguments to pass to the function.
    :return: The return value of the retried function.
    """
    retryables = list(AWS_COMMON_RETRYABLE_ERRORS)
    retryables.extend(service_retryable_errors)
    sleepy_time = 1
    if MAX_SLEEP_INTERVAL_PARAM in func_kwargs:
        max_sleepy_time = func_kwargs.get(MAX_SLEEP_INTERVAL_PARAM)
        del func_kwargs[MAX_SLEEP_INTERVAL_PARAM]
    else:
        max_sleepy_time = MAX_SLEEP_INTERVAL_DEFAULT
    func_return = None
    while True:
        try:
            func_return = func(*func_args, **func_kwargs)
            module_logger.debug("Ran %s, got %s.", func.__name__ if hasattr(func, "__name__") else str(func), func_return)
            break
        except Exception as error:
            error_code = get_code_for_exception(error)
            if error_code in retryables:
                module_logger.critical(
                    f"Sleeping for {sleepy_time} to give AWS time to " f"connect resources. Retryable error_code={error_code!r}"
                )
                time.sleep(sleepy_time)
                sleepy_time = sleepy_time * 2
                if sleepy_time < max_sleepy_time:
                    continue
            raise
    return func_return


def generate_statement_id(seed: str) -> str:
    return codecs.encode(seed.encode(), "base64").decode().strip().rstrip("=").replace("\n", "")


def get_session_with_account(account_id: str, region: str) -> boto3.Session:
    raise NotImplementedError("Environment does not support creating sessions from AWS account only.")


@overload
def get_session(role_ARN: str = None, region: str = None, duration: int = IF_DEFAULT_SESSION_DURATION) -> boto3.Session: ...


@overload
def get_session(aws_access_pair: AWSAccessPair = None, region: str = None, duration: int = IF_DEFAULT_SESSION_DURATION) -> boto3.Session: ...


def get_session(
    role_ARN_or_pair: Union[AWSAccessPair, str] = None, region: str = None, duration: int = IF_DEFAULT_SESSION_DURATION
) -> boto3.Session:
    """
    Wrapper around boto3.Session()

    Parameters
    role_ARN_or_pair : Union[AWSAccessPair, str], The ARN of the AWS role that the framework should assume or a :class:`AWSAccessPair` (key_id and access_key)
    region: string, AWS region
    duration: int, duration (in seconds) for credentials validity

    Returns
    boto3.Session
    """

    if not role_ARN_or_pair:
        # Use system defaults (~/.aws, etc).
        module_logger.warning("Creating boto3.Session with system defaults.")
        return boto3.Session(None, None, None, region)

    if isinstance(role_ARN_or_pair, AWSAccessPair):
        module_logger.warning("Creating boto3.Session with access key pair.")
        return boto3.Session(role_ARN_or_pair.aws_access_key_id, role_ARN_or_pair.aws_secret_access_key, None, region)
    else:  # role arn
        # TODO
        raise NotImplementedError("Creating sessions with role only not implemented yet!")


@overload
def get_caller_identity(session: boto3.Session, region: str) -> str: ...


@overload
def get_caller_identity(access_pair: AWSAccessPair, region: str) -> str: ...


def get_caller_identity(access_pair_or_session: Union[AWSAccessPair, boto3.Session], region: str) -> str:
    if isinstance(access_pair_or_session, AWSAccessPair):
        sts = boto3.client(
            "sts",
            aws_access_key_id=access_pair_or_session.aws_access_key_id,
            aws_secret_access_key=access_pair_or_session.aws_secret_access_key,
            region_name=region,
        )
    else:
        sts = access_pair_or_session.client(service_name="sts", region_name=region)
    return exponential_retry(sts.get_caller_identity, ["AccessDenied"])["Arn"]


@overload
def get_aws_account_id(session: boto3.Session, region: str) -> str: ...


@overload
def get_aws_account_id(access_pair: AWSAccessPair, region: str) -> str: ...


def get_aws_account_id(access_pair_or_session: Union[AWSAccessPair, boto3.Session], region: str) -> str:
    user_arn = get_caller_identity(access_pair_or_session, region)
    return get_aws_account_id_from_arn(user_arn)


def get_assumed_role_session(
    role_arn: str,
    base_session: boto3.Session,
    duration: int = MAX_ASSUME_ROLE_DURATION,
    external_id: str = None,
    context_id: str = None,
    context_region: str = None,
):
    sts_connection = base_session.client("sts")
    kwargs = {"RoleArn": role_arn, "RoleSessionName": "username" if not context_id else context_id, "DurationSeconds": duration}
    if external_id:
        kwargs.update({"ExternalId": external_id})
    # IAM propagation is annoying, so following a create_role action, we have to
    # enable the retry logic for a subsequent assume_role action.
    assume_role_object = exponential_retry(sts_connection.assume_role, ["AccessDenied"], **kwargs)

    credentials = assume_role_object["Credentials"]
    return boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        region_name=context_region if context_region else base_session.region_name,
    )


def _get_assumed_role_session(role_arn: str, base_session: botocore.session.Session = None) -> boto3.Session:
    base_session = base_session or boto3.session.Session()._session
    fetcher = botocore.credentials.AssumeRoleCredentialFetcher(
        client_creator=base_session.create_client,
        source_credentials=base_session.get_credentials(),
        role_arn=role_arn,
        extra_args={
            #    'RoleSessionName': None # set this if you want something non-default
        },
    )
    creds = botocore.credentials.DeferredRefreshableCredentials(
        method="assume-role", refresh_using=fetcher.fetch_credentials, time_fetcher=lambda: datetime.datetime.now(tzlocal())
    )
    botocore_session = botocore.session.Session()
    botocore_session._credentials = creds
    return boto3.Session(botocore_session=botocore_session)


def is_assumed_role_session(user_arn: str) -> bool:
    return user_arn.startswith(AWS_STS_ARN_ROOT) and user_arn.split(":")[5].startswith("assumed-role")


def _get_trust_policy(allowed_services: Sequence[str], allowed_aws_entities: Set[str], external_ids: List[str]) -> str:
    """Example allowed_service: 'ec2.amazonaws.com'"""

    statement = []
    trust_policy = {
        "Version": "2012-10-17",
    }

    if allowed_services:
        statement.extend(
            [{"Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"} for service in allowed_services]
        )

    if allowed_aws_entities:
        account_statements = [
            {"Effect": "Allow", "Principal": {"AWS": entity}, "Action": ["sts:AssumeRole", "sts:TagSession"]}
            for entity in allowed_aws_entities
        ]

        if external_ids:
            for acct_statement in account_statements:
                acct_statement.update({"Condition": {"StringEquals": {"sts:ExternalId": external_ids}}})

        statement.extend(account_statements)

    trust_policy.update({"Statement": statement})
    return json.dumps(trust_policy)


def is_in_role_trust_policy(role_name: str, base_session: boto3.Session, aws_entity: str) -> bool:
    iam = base_session.client("iam")

    role = exponential_retry(iam.get_role, ["Throttling"], RoleName=role_name)
    existing_trust_policy = role["Role"]["AssumeRolePolicyDocument"]
    for statement in existing_trust_policy["Statement"]:
        if "AWS" in statement["Principal"]:
            if aws_entity == statement["Principal"]["AWS"]:
                return True

    return False


def has_role(role_name: str, base_session: boto3.Session) -> bool:
    iam = base_session.client("iam")
    try:
        role = exponential_retry(iam.get_role, ["Throttling"], RoleName=role_name)
        return role
    except iam.exceptions.NoSuchEntityException:
        return False


def create_role(
    role_name: str,
    base_session: boto3.Session,
    region: str,
    auto_trust_caller_identity=True,
    allowed_services: Sequence[str] = [],
    allowed_aws_entities: Sequence[str] = [],
    external_ids: Optional[List[str]] = None,
):
    """
    Creates a role that lets a list of specified services assume the role.

    :return: The newly created role.
    """
    iam = base_session.client("iam")

    trusted_entities = set(allowed_aws_entities)
    if auto_trust_caller_identity:
        trusted_entities.add(get_caller_identity(base_session, region))

    try:
        role = exponential_retry(
            iam.create_role,
            ["AccessDenied", "ServiceFailureException"],
            RoleName=role_name,
            AssumeRolePolicyDocument=_get_trust_policy(allowed_services, trusted_entities, external_ids),
            MaxSessionDuration=MAX_ASSUME_ROLE_DURATION,
        )
        if "role_exists" in iam.waiter_names:
            iam.get_waiter("role_exists").wait(RoleName=role_name)
            time.sleep(3)
        else:
            time.sleep(15)  # give some time for IAM propagation

        module_logger.info(f"Created role {role_name} for new services {allowed_services} and entities {trusted_entities}")
    except ClientError as ex:
        module_logger.exception("Couldn't create role %s. Exception: %s", role_name, str(ex))
        raise
    else:
        return role


def attach_aws_managed_policy(role_name: str, managed_policy_name: str, base_session: boto3.Session) -> None:
    """
    managed_policy_name: can be a full AWS managed policy arn or just the policy name
    """
    iam = base_session.client("iam")
    policy_arn = normalize_policy_arn(managed_policy_name)
    try:
        iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)

        module_logger.info(f"Attached AWS managed policy {managed_policy_name!r} to the role {role_name!r}")
    except ClientError as ex:
        module_logger.exception("Could not attach AWS managed policy to role %s. Exception: %s", role_name, str(ex))
        raise


def has_aws_managed_policy(managed_policy_name: str, base_session: boto3.Session) -> bool:
    """
    managed_policy_name: can be a full AWS managed policy arn or just the policy name
    """
    iam = base_session.client("iam")
    policy_arn = normalize_policy_arn(managed_policy_name)
    try:
        response = exponential_retry(iam.get_policy, ["ServiceFailureException", "AccessDenied"], PolicyArn=policy_arn)
        if response and "Policy" in response:
            return True
    except ClientError as err:
        if err.response["Error"]["Code"] not in ["NoSuchEntity", "NoSuchEntityException"]:
            raise
    return False


def normalize_policy_arn(managed_policy_name):
    return managed_policy_name if managed_policy_name.startswith("arn:") else f"arn:aws:iam::aws:policy/{managed_policy_name}"


def update_role(
    role_name: str,
    base_session: boto3.Session,
    region: str,
    auto_trust_caller_identity=True,
    allowed_services: Sequence[str] = [],
    new_allowed_aws_entities: Sequence[str] = [],
    keep_existing_aws_entities: bool = False,
    external_ids: Optional[List[str]] = None,
) -> None:
    """
    Updates a role that lets a list of specified services and entities assume the role (trust policy).
    """
    iam = base_session.client("iam")

    trusted_entities = set(new_allowed_aws_entities)
    if auto_trust_caller_identity:
        trusted_entities.add(get_caller_identity(base_session, region))

    if keep_existing_aws_entities:
        role = exponential_retry(iam.get_role, ["Throttling"], RoleName=role_name)
        existing_trust_policy = role["Role"]["AssumeRolePolicyDocument"]
        for statement in existing_trust_policy["Statement"]:
            if "AWS" in statement["Principal"]:
                existing_trusted_entities = statement["Principal"]["AWS"]
                existing_trusted_entities = (
                    existing_trusted_entities if isinstance(existing_trusted_entities, list) else [existing_trusted_entities]
                )
                for existing_entity in existing_trusted_entities:
                    if not _is_trust_policy_AWS_principle_deleted(existing_entity):
                        trusted_entities.add(existing_entity)

    try:
        exponential_retry(
            iam.update_assume_role_policy,
            ["Throttling", "MalformedPolicyDocument"],  # malformed policy might be received if any of the entities is new
            PolicyDocument=_get_trust_policy(allowed_services, trusted_entities, external_ids),
            RoleName=role_name,
        )
        module_logger.info(f"Updated role {role_name} for new services {allowed_services} and new entities {new_allowed_aws_entities}")

    except ClientError:
        module_logger.exception("Couldn't create or update role %s.", role_name)
        raise


def _is_trust_policy_AWS_principle_IAM_unique_id(principle: str) -> bool:
    """Refer "Understanding unique ID prefixes" from;
        https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_identifiers.html

    Since we are only interested in analyzing IAM entities that can be used as Principle, we are checking
    IAM users and roles for now.

    For Groups and Instances profiles please check
        https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html
        as to why 'you cannot specify IAM groups and instance profiles as principals'.
    """
    if principle.startswith("AIDA"):
        # IAM User
        return True

    if principle.startswith("AROA"):
        # IAM Role
        return True

    return False


def _is_trust_policy_AWS_principle_deleted(principle: str) -> bool:
    """Implementation based on:
        https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html

    Handle 'AWS' type principles. So other types 'Federated', 'CanonicalUser' and 'Service' (AWS services) are ignored.

    Intended to be used in scenaraios where RheocerOS wants to make sure that external modifications won't disturb the
    highl-level application workflows (and cause deadends in case of deleted roles, etc).
    RheocerOS uses this to clean the policies of the entities that it manages, such as execution and development roles
    for platforms.
    """
    if principle == "*":
        # Anonymous User
        return False

    if principle.startswith("arn:aws"):
        # - root user
        # - IAM role
        # - IAM user
        # - Assumed role sessions
        return False

    # other cases such as explicit account ID use (e.g "AWS": "123456789012") is implicitly handled by the following
    # call.
    return _is_trust_policy_AWS_principle_IAM_unique_id(principle)


def _check_statement_field_equality(statement1: Dict[str, Any], statement2: Dict[str, Any], field_name: str) -> bool:
    field1 = statement1.get(field_name, None)
    field2 = statement2.get(field_name, None)
    if field1 and field2:
        if field_name == "Principal":
            if type(field1) != type(field2):
                return False

            principle1: Dict[str, str] = cast("dict", field1)
            principle2: Dict[str, str] = cast("dict", field2)
            if set(principle1.keys()) != set(principle2.keys()):
                return False

            for key1, value1 in principle1.items():
                for key2, value2 in principle2.items():
                    if key1 == key2 and not _check_field_value_equality(value1, value2):
                        return False
            return True
        else:
            return _check_field_value_equality(field1, field2)

    return False


def _check_field_value_equality(value1: Union[str, List[str]], value2: Union[str, List[str]]) -> bool:
    values1 = value1 if isinstance(value1, list) else [value1]
    values2 = value2 if isinstance(value2, list) else [value2]
    return set(values1) == set(values2)


def update_role_trust_policy(
    role_name: str,
    base_session: boto3.Session,
    region: str,
    new_aws_entities: Set[str] = set(),
    removed_aws_entities: Set[str] = set(),
    check_caller_identity: bool = False,
    external_id: str = None,
) -> None:
    """
    Updates a role that lets a list of specified entities assume the role (trust policy).
    """
    iam = base_session.client(service_name="iam")

    role = exponential_retry(iam.get_role, ["Throttling"], RoleName=role_name)
    existing_trust_policy = role["Role"]["AssumeRolePolicyDocument"]

    root_statement = existing_trust_policy["Statement"]

    caller_identity = None
    if check_caller_identity:
        caller_identity = get_caller_identity(base_session, region)

    caller_identity_found = False
    existing_entities = set()
    entities_to_be_added = set(new_aws_entities)
    entities_to_be_removed = set()
    entities_removed = False
    for i, statement in enumerate(root_statement):
        if "AWS" in statement["Principal"]:
            trusted_entities = statement["Principal"]["AWS"]
            trusted_entities = trusted_entities if isinstance(trusted_entities, list) else [trusted_entities]
            for trusted_entity in trusted_entities:
                existing_entities.add(trusted_entity)
                if trusted_entity in removed_aws_entities or _is_trust_policy_AWS_principle_deleted(trusted_entity):
                    entities_to_be_removed.add(trusted_entity)
                elif trusted_entity == caller_identity:
                    caller_identity_found = True

    if check_caller_identity and not caller_identity_found:
        entities_to_be_added.add(caller_identity)

    removed_statement_indexes = []
    for i, statement in enumerate(root_statement):
        if "AWS" in statement["Principal"]:
            trusted_entities = statement["Principal"]["AWS"]
            trusted_entities = set(trusted_entities) if isinstance(trusted_entities, list) else {trusted_entities}
            remaining_entities = trusted_entities - entities_to_be_removed
            if remaining_entities != trusted_entities:
                entities_removed = True
                if not remaining_entities:
                    removed_statement_indexes.append(i)
                else:
                    statement["Principal"]["AWS"] = list(remaining_entities)

    # delete in reverse order so that iteration won't mess up
    for i in sorted(removed_statement_indexes, reverse=True):
        del root_statement[i]

    entities_to_be_added = entities_to_be_added - existing_entities
    if entities_to_be_added:
        # OPTIMIZATION: Merge trivial/basic statements. check if we can find a common statement for new basic trustees.
        #  basic trustees are the ones without an external_id (with basic statement structure).
        #  This is against ACLSizeLimit (trust policy size).
        shared_statement_for_basic_trustees = None
        for i, statement in enumerate(root_statement):
            if "AWS" in statement["Principal"]:
                # make sure that the statement structure for the shared statement only has the following three.
                if (
                    set(statement.keys()) == {"Principal", "Effect", "Action"}
                    and statement["Effect"] == "Allow"
                    and statement["Action"] == "sts:AssumeRole"
                ):
                    shared_statement_for_basic_trustees = statement

        if shared_statement_for_basic_trustees and not external_id:
            # OPTIMIZATION: Try to pack all of the trivial trustees into the same statement.
            trusted_entities = shared_statement_for_basic_trustees["Principal"]["AWS"]
            trusted_entities = trusted_entities if isinstance(trusted_entities, list) else [trusted_entities]
            trusted_entities.extend(entities_to_be_added)
            shared_statement_for_basic_trustees["Principal"]["AWS"] = trusted_entities
        else:
            # if external_id is defined or an existing compatible statement is not found,
            # we should create a dedicated statement block.
            account_statement = {
                "Effect": "Allow",
                "Principal": {"AWS": [entity for entity in entities_to_be_added]},
                "Action": "sts:AssumeRole",
            }

            if external_id:
                account_statement.update({"Condition": {"StringEquals": {"sts:ExternalId": [external_id]}}})

            root_statement.append(account_statement)

    if entities_removed or entities_to_be_added:
        try:
            exponential_retry(
                iam.update_assume_role_policy, ["Throttling"], PolicyDocument=json.dumps(existing_trust_policy), RoleName=role_name
            )
            module_logger.info(
                f"Updated the trust policy for role {role_name} for new entities {new_aws_entities} and removed entities {removed_aws_entities}"
            )

        except ClientError:
            module_logger.exception("Couldn't update the trust policy for role %s.", role_name)
            raise
    else:
        module_logger.info("Skipping role trusted policy update since no change detected against the existing trusted policy...")


def _get_size_optimized_statements(permissions: Set["ConstructPermission"]) -> Dict[Set[str], Set[str]]:
    actions_bucketed_statements = {}
    for resources, actions in permissions:
        actions_bucketed_statements.setdefault(frozenset(actions), set(resources)).update(resources)

    return actions_bucketed_statements


def put_inlined_policy(
    role_name: str, policy_name: str, action_resource_pairs: Set["ConstructPermission"], base_session: boto3.Session
) -> None:
    iam = base_session.client("iam")

    size_optimized_statements = _get_size_optimized_statements(action_resource_pairs)
    inlined_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": [action for action in actions], "Resource": [resource for resource in resources]}
            for actions, resources in size_optimized_statements.items()
        ],
    }

    exponential_retry(
        iam.put_role_policy,
        ["ServiceFailureException"],
        PolicyDocument=json.dumps(inlined_policy),
        PolicyName=policy_name,
        RoleName=role_name,
    )


def delete_inlined_policy(role_name: str, policy_name: str, base_session: boto3.Session) -> None:
    iam = base_session.client("iam")

    try:
        exponential_retry(
            iam.delete_role_policy,
            ["ServiceFailureException", "AccessDenied"],  # eventual consistency: e.g quick transition from activation to deletion
            PolicyName=policy_name,
            RoleName=role_name,
        )
    except ClientError as err:
        if err.response["Error"]["Code"] not in ["NoSuchEntity"]:
            raise err


def list_inlined_policies(role_name: str, base_session: boto3.Session, iam=None) -> Iterator[str]:
    """Implementation based on
         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.list_role_policies

    with implicit pagination support.
    """
    if iam is None:
        iam = base_session.client("iam")

    retryables = ["ServiceFailureException"]

    marker: str = None
    try:
        while True:
            if marker:
                response = exponential_retry(iam.list_role_policies, retryables, RoleName=role_name, Marker=marker)
            else:
                response = exponential_retry(iam.list_role_policies, retryables, RoleName=role_name)

                yield from response["PolicyNames"]

            if response.get("IsTruncated", False) is True:
                marker = response["Marker"]
            else:
                break
    except Exception as error:
        error_code = get_code_for_exception(error)
        if error_code not in ["NoSuchEntity", "NoSuchEntityException"]:
            raise
        yield from []


def list_attached_policies(role_name: str, base_session: boto3.Session, iam=None) -> Iterator[Tuple[str, str]]:
    """Implementation based on
         https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/iam.html#IAM.Client.list_attached_role_policies

    with implicit pagination support.

    :returns an iterator of tuples of PolicyArn and PolicyName pairs.
    """
    if iam is None:
        iam = base_session.client("iam")

    retryables = ["ServiceFailureException"]

    marker: str = None
    try:
        while True:
            if marker:
                response = exponential_retry(iam.list_attached_role_policies, retryables, RoleName=role_name, Marker=marker)
            else:
                response = exponential_retry(iam.list_attached_role_policies, retryables, RoleName=role_name)

                yield from [(policy["PolicyArn"], policy["PolicyName"]) for policy in response["AttachedPolicies"]]

            if response.get("IsTruncated", False) is True:
                marker = response["Marker"]
            else:
                break
    except Exception as error:
        error_code = get_code_for_exception(error)
        if error_code not in ["NoSuchEntity", "NoSuchEntityException"]:
            raise
        yield from []


def delete_role(role_name: str, base_session: boto3.Session) -> None:
    """Refer
        https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_manage_delete.html#roles-managingrole-deleting-console

    Logic:
        - Remove (delete) inlined policies
        - Remove (detach) managed policies
        - Delete the role
    """
    iam = base_session.client("iam")

    inlined_policy_it = list_inlined_policies(role_name, base_session, iam)
    for policy_name in inlined_policy_it:
        exponential_retry(iam.delete_role_policy, ["ServiceFailureException"], RoleName=role_name, PolicyName=policy_name)

    attached_policy_it = list_attached_policies(role_name, base_session, iam)
    for policy_arn, _ in attached_policy_it:
        exponential_retry(iam.detach_role_policy, ["ServiceFailureException"], RoleName=role_name, PolicyArn=policy_arn)

    exponential_retry(iam.delete_role, ["ServiceFailureException", "ConcurrentModificationException"], RoleName=role_name)
