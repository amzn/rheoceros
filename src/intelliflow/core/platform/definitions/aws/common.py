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

# admin role to be used in bootstrapper compute envs in AWS (e.g Lambda)
IF_BOOTSTRAPPER_ROLE_FORMAT = "arn:aws:iam::{0}:role/IntelliFlow-Native-Bootstrapper"

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
    new_aws_services: Set[str] = set(),
    removed_aws_services: Set[str] = set(),
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

    # Process existing AWS entities
    for _, statement in enumerate(root_statement):
        if "AWS" in statement["Principal"]:
            trusted_entities = statement["Principal"]["AWS"]
            trusted_entities = trusted_entities if isinstance(trusted_entities, list) else [trusted_entities]
            for trusted_entity in trusted_entities:
                existing_entities.add(trusted_entity)
                if trusted_entity in removed_aws_entities or _is_trust_policy_AWS_principle_deleted(trusted_entity):
                    entities_to_be_removed.add(trusted_entity)
                elif trusted_entity == caller_identity:
                    caller_identity_found = True

    # Process existing AWS services
    existing_services = set()
    services_to_be_added = set(new_aws_services)
    services_to_be_removed = set()
    services_removed = False

    for _, statement in enumerate(root_statement):
        if "Service" in statement["Principal"]:
            trusted_services = statement["Principal"]["Service"]
            trusted_services = trusted_services if isinstance(trusted_services, list) else [trusted_services]
            for trusted_service in trusted_services:
                existing_services.add(trusted_service)
                if trusted_service in removed_aws_services:
                    services_to_be_removed.add(trusted_service)

    if check_caller_identity and not caller_identity_found:
        entities_to_be_added.add(caller_identity)

    # Remove services and entities from existing statements
    removed_statement_indexes = []
    for i, statement in enumerate(root_statement):

        # Handle AWS entities removal
        if "AWS" in statement["Principal"]:
            trusted_entities = statement["Principal"]["AWS"]
            trusted_entities = set(trusted_entities) if isinstance(trusted_entities, list) else {trusted_entities}
            remaining_entities = trusted_entities - entities_to_be_removed
            if remaining_entities != trusted_entities:
                entities_removed = True
                if not remaining_entities:
                    # Check if there are services in this statement, if not mark for removal
                    if "Service" not in statement["Principal"]:
                        removed_statement_indexes.append(i)
                        continue
                    else:
                        # Remove AWS principal but keep Service principal
                        del statement["Principal"]["AWS"]
                else:
                    statement["Principal"]["AWS"] = list(remaining_entities)

        # Handle Service removal
        if "Service" in statement["Principal"]:
            trusted_services = statement["Principal"]["Service"]
            trusted_services = set(trusted_services) if isinstance(trusted_services, list) else {trusted_services}
            remaining_services = trusted_services - services_to_be_removed
            if remaining_services != trusted_services:
                services_removed = True
                if not remaining_services:
                    # Check if there are AWS entities in this statement, if not mark for removal
                    if "AWS" not in statement["Principal"]:
                        removed_statement_indexes.append(i)
                    else:
                        # Remove Service principal but keep AWS principal
                        del statement["Principal"]["Service"]
                else:
                    statement["Principal"]["Service"] = list(remaining_services)

    # delete in reverse order so that iteration won't mess up
    for i in sorted(removed_statement_indexes, reverse=True):
        del root_statement[i]

    # Handle adding new AWS entities (existing logic)
    entities_to_be_added = entities_to_be_added - existing_entities
    if entities_to_be_added:
        # OPTIMIZATION: Merge trivial/basic statements. check if we can find a common statement for new basic trustees.
        #  basic trustees are the ones without an external_id (with basic statement structure).
        #  This is against ACLSizeLimit (trust policy size).
        shared_statement_for_basic_trustees = None
        for _, statement in enumerate(root_statement):
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

    # Handle adding new AWS services (new logic)
    services_to_be_added = services_to_be_added - existing_services
    if services_to_be_added:
        # OPTIMIZATION: Try to find an existing Service statement to merge with
        shared_statement_for_services = None
        for _, statement in enumerate(root_statement):
            if "Service" in statement["Principal"]:
                # Look for a basic service statement that we can merge with
                if (
                    set(statement.keys()) == {"Principal", "Effect", "Action"}
                    and statement["Effect"] == "Allow"
                    and statement["Action"] == "sts:AssumeRole"
                ):
                    shared_statement_for_services = statement
                    break

        if shared_statement_for_services:
            # OPTIMIZATION: Add new services to existing Service statement
            trusted_services = shared_statement_for_services["Principal"]["Service"]
            trusted_services = trusted_services if isinstance(trusted_services, list) else [trusted_services]
            trusted_services.extend(services_to_be_added)
            shared_statement_for_services["Principal"]["Service"] = trusted_services
        else:
            # Create a new Service statement
            service_statement = {
                "Effect": "Allow",
                "Principal": {"Service": [service for service in services_to_be_added]},
                "Action": "sts:AssumeRole",
            }
            root_statement.append(service_statement)

    if entities_removed or entities_to_be_added or services_removed or services_to_be_added:
        try:
            exponential_retry(
                iam.update_assume_role_policy, ["Throttling"], PolicyDocument=json.dumps(existing_trust_policy), RoleName=role_name
            )
            module_logger.info(
                f"Updated the trust policy for role {role_name} for new entities {new_aws_entities}, removed entities {removed_aws_entities}, new services {new_aws_services}, and removed services {removed_aws_services}"
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


# AWS IAM Limits
AWS_INLINE_POLICY_SIZE_LIMIT = 10240  # bytes, total for all inline policies per role
AWS_MANAGED_POLICY_SIZE_LIMIT = 6144  # bytes per managed policy
AWS_MAX_MANAGED_POLICIES_PER_ROLE = 10  # maximum managed policies per role

# Safety buffer for policy size estimation to avoid borderline cases
# AWS may calculate policy size differently than our estimation
AWS_INLINE_POLICY_SIZE_BUFFER = 500  # bytes buffer for inline policies
AWS_MANAGED_POLICY_SIZE_BUFFER = 200  # bytes buffer for managed policies

# IntelliFlow managed policy naming constants
IF_MANAGED_POLICY_PART_SEPARATOR = "-Part-"
IF_MANAGED_POLICY_ROLE_SEPARATOR = "-MP-"  # Managed Policy separator


def _build_managed_policy_name(role_name: str, policy_name: str, part_number: Optional[int] = None) -> str:
    """Build a consistent managed policy name that ties it to a specific role.

    Format: {role_name}-MP-{policy_name} or {role_name}-MP-{policy_name}-Part-{N}
    This ensures policies are clearly associated with their role for safe cleanup.
    """
    base_name = f"{role_name}{IF_MANAGED_POLICY_ROLE_SEPARATOR}{policy_name}"
    if part_number is not None:
        return f"{base_name}{IF_MANAGED_POLICY_PART_SEPARATOR}{part_number}"
    return base_name


def _build_managed_policy_arn(account_id: str, role_name: str, policy_name: str, part_number: Optional[int] = None) -> str:
    """Build the full ARN for a managed policy."""
    managed_policy_name = _build_managed_policy_name(role_name, policy_name, part_number)
    return f"arn:aws:iam::{account_id}:policy/{managed_policy_name}"


def _is_role_managed_policy_name(role_name: str, policy_name: str) -> bool:
    """Check if a policy name belongs to a specific role's managed policies."""
    expected_prefix = f"{role_name}{IF_MANAGED_POLICY_ROLE_SEPARATOR}"
    return policy_name.startswith(expected_prefix)


def _parse_managed_policy_base_name(role_name: str, full_policy_name: str) -> Optional[str]:
    """Extract the base policy name from a full managed policy name for this role."""
    expected_prefix = f"{role_name}{IF_MANAGED_POLICY_ROLE_SEPARATOR}"
    if not full_policy_name.startswith(expected_prefix):
        return None

    # Remove the role prefix
    remaining = full_policy_name[len(expected_prefix) :]

    # Remove part suffix if present
    if IF_MANAGED_POLICY_PART_SEPARATOR in remaining:
        return remaining.split(IF_MANAGED_POLICY_PART_SEPARATOR)[0]

    return remaining


def _estimate_policy_size(statements: List[Dict[str, Any]]) -> int:
    """Estimate the size of a policy document in bytes."""
    policy_doc = {"Version": "2012-10-17", "Statement": statements}
    return len(json.dumps(policy_doc, separators=(",", ":")))


def _partition_statements_by_size(statements: List[Dict[str, Any]], max_size: int) -> List[List[Dict[str, Any]]]:
    """Partition policy statements into chunks that fit within size limits."""
    partitions = []
    current_partition = []

    for statement in statements:
        # Test adding this statement to current partition
        test_partition = current_partition + [statement]
        # Use safety buffer for partitioning to avoid borderline cases
        safe_max_size = max_size - AWS_MANAGED_POLICY_SIZE_BUFFER
        if _estimate_policy_size(test_partition) <= safe_max_size:
            current_partition.append(statement)
        else:
            # Current partition is full, start a new one
            if current_partition:
                partitions.append(current_partition)
            current_partition = [statement]

            # Check if single statement exceeds limit
            if _estimate_policy_size(current_partition) > safe_max_size:
                raise ValueError(f"Single policy statement exceeds AWS managed policy size limit ({max_size} bytes)")

    if current_partition:
        partitions.append(current_partition)

    return partitions


def _create_or_update_managed_policy(policy_arn: str, policy_name: str, policy_document: str, base_session: boto3.Session) -> None:
    """Create or update a customer-managed policy."""
    iam = base_session.client("iam")

    try:
        # Try to get existing policy
        exponential_retry(iam.get_policy, ["ServiceFailureException"], PolicyArn=policy_arn)

        # Policy exists, check version count and clean up if needed before creating new version
        versions_response = exponential_retry(iam.list_policy_versions, ["ServiceFailureException"], PolicyArn=policy_arn)
        versions = versions_response.get("Versions", [])

        # AWS allows max 5 versions, so if we have 4 or more, delete the oldest non-default ones
        if len(versions) >= 4:
            # Sort by CreateDate to find oldest versions first
            non_default_versions = [v for v in versions if not v.get("IsDefaultVersion", False)]
            non_default_versions.sort(key=lambda x: x.get("CreateDate"))

            # Delete oldest versions to make room (keep at least 2 versions: current default + 1 more)
            versions_to_delete = non_default_versions[:-1] if len(non_default_versions) > 1 else non_default_versions

            for version in versions_to_delete:
                try:
                    exponential_retry(
                        iam.delete_policy_version, ["ServiceFailureException"], PolicyArn=policy_arn, VersionId=version["VersionId"]
                    )
                    module_logger.info(f"Deleted old policy version {version['VersionId']} for {policy_name}")
                except Exception as e:
                    module_logger.warning(f"Could not delete policy version {version['VersionId']}: {e}")

        # Now create a new version
        try:
            exponential_retry(
                iam.create_policy_version,
                ["ServiceFailureException"],  # Don't retry LimitExceededException, handle it explicitly
                PolicyArn=policy_arn,
                PolicyDocument=policy_document,
                SetAsDefault=True,
            )
            module_logger.info(f"Updated managed policy {policy_name} with new version")
        except ClientError as limit_err:
            if limit_err.response["Error"]["Code"] == "LimitExceeded":
                # If we still hit limit after cleanup, do more aggressive cleanup
                module_logger.warning(f"Still hitting version limit for {policy_name}, doing aggressive cleanup")

                # Get fresh version list and delete all non-default versions
                versions_response = exponential_retry(iam.list_policy_versions, ["ServiceFailureException"], PolicyArn=policy_arn)
                versions = versions_response.get("Versions", [])

                for version in versions:
                    if not version.get("IsDefaultVersion", False):
                        try:
                            exponential_retry(
                                iam.delete_policy_version, ["ServiceFailureException"], PolicyArn=policy_arn, VersionId=version["VersionId"]
                            )
                            module_logger.info(f"Aggressively deleted policy version {version['VersionId']} for {policy_name}")
                        except Exception as e:
                            module_logger.warning(f"Could not delete policy version {version['VersionId']}: {e}")

                # Try creating new version again
                exponential_retry(
                    iam.create_policy_version,
                    ["ServiceFailureException"],
                    PolicyArn=policy_arn,
                    PolicyDocument=policy_document,
                    SetAsDefault=True,
                )
                module_logger.info(f"Updated managed policy {policy_name} with new version after aggressive cleanup")
            else:
                raise

    except ClientError as err:
        if err.response["Error"]["Code"] in ["NoSuchEntity", "NoSuchEntityException"]:
            # Policy doesn't exist, create it
            exponential_retry(iam.create_policy, ["ServiceFailureException"], PolicyName=policy_name, PolicyDocument=policy_document)
            module_logger.info(f"Created managed policy {policy_name}")
        else:
            raise


def _attach_managed_policy(role_name: str, policy_arn: str, base_session: boto3.Session) -> None:
    """Attach a managed policy to a role."""
    iam = base_session.client("iam")

    try:
        exponential_retry(iam.attach_role_policy, ["ServiceFailureException"], RoleName=role_name, PolicyArn=policy_arn)
    except ClientError as err:
        # Ignore if already attached
        if err.response["Error"]["Code"] not in ["EntityAlreadyExists"]:
            raise


def _detach_managed_policies_by_prefix(role_name: str, base_policy_name: str, base_session: boto3.Session) -> None:
    """Detach all managed policies for a specific base policy name from a role."""
    iam = base_session.client("iam")

    try:
        # List attached policies
        response = exponential_retry(iam.list_attached_role_policies, ["ServiceFailureException"], RoleName=role_name)

        for policy in response.get("AttachedPolicies", []):
            policy_name = policy["PolicyName"]
            policy_arn = policy["PolicyArn"]

            # Check if this is one of our managed policies for this role and base policy name
            if _is_role_managed_policy_name(role_name, policy_name):
                parsed_base_name = _parse_managed_policy_base_name(role_name, policy_name)
                if parsed_base_name == base_policy_name:
                    exponential_retry(iam.detach_role_policy, ["ServiceFailureException"], RoleName=role_name, PolicyArn=policy_arn)
                    module_logger.info(f"Detached managed policy {policy_name} from role {role_name}")

    except ClientError as err:
        if err.response["Error"]["Code"] not in ["NoSuchEntity", "NoSuchEntityException"]:
            raise


def put_managed_policy(
    role_name: str, policy_name: str, action_resource_pairs: Set["ConstructPermission"], base_session: boto3.Session
) -> None:
    """Create or update customer-managed policies instead of inline policy to avoid AWS size limits.

    This function replaces put_inlined_policy functionality by:
    1. Estimating the total size of permissions
    2. Partitioning them into multiple managed policies if needed (to stay under AWS limits)
    3. Creating/updating the managed policies
    4. Attaching them to the role

    Args:
        role_name: The IAM role name to attach policies to
        policy_name: Base name for the managed policies (will be suffixed with -Part-N if partitioned)
        action_resource_pairs: Set of ConstructPermission objects containing actions and resources
        base_session: Boto3 session for AWS operations
    """
    iam = base_session.client("iam")
    account_id = base_session.client("sts").get_caller_identity()["Account"]

    # Generate optimized statements (same logic as put_inlined_policy)
    size_optimized_statements = _get_size_optimized_statements(action_resource_pairs)
    statements = [
        {"Effect": "Allow", "Action": [action for action in actions], "Resource": [resource for resource in resources]}
        for actions, resources in size_optimized_statements.items()
    ]

    # Check if we need to partition based on estimated size
    total_size = _estimate_policy_size(statements)

    # Use safety buffer for single managed policy check
    safe_managed_limit = AWS_MANAGED_POLICY_SIZE_LIMIT - AWS_MANAGED_POLICY_SIZE_BUFFER
    if total_size <= safe_managed_limit:
        # Single policy is sufficient
        policy_document = json.dumps({"Version": "2012-10-17", "Statement": statements})

        managed_policy_name = _build_managed_policy_name(role_name, policy_name)
        policy_arn = _build_managed_policy_arn(account_id, role_name, policy_name)
        _create_or_update_managed_policy(policy_arn, managed_policy_name, policy_document, base_session)
        _attach_managed_policy(role_name, policy_arn, base_session)

        module_logger.info(f"Created/updated single managed policy {managed_policy_name} ({total_size} bytes)")

    else:
        # Need to partition into multiple policies
        partitions = _partition_statements_by_size(statements, AWS_MANAGED_POLICY_SIZE_LIMIT)

        if len(partitions) > AWS_MAX_MANAGED_POLICIES_PER_ROLE:
            raise ValueError(
                f"Policy requires {len(partitions)} managed policies, but AWS allows maximum "
                f"{AWS_MAX_MANAGED_POLICIES_PER_ROLE} per role. Total size: {total_size} bytes"
            )

        # First, detach any existing policies for this base policy name to avoid accumulation
        _detach_managed_policies_by_prefix(role_name, policy_name, base_session)

        # Create and attach partitioned policies
        for i, partition_statements in enumerate(partitions, 1):
            managed_policy_name = _build_managed_policy_name(role_name, policy_name, i)
            policy_document = json.dumps({"Version": "2012-10-17", "Statement": partition_statements})

            policy_arn = _build_managed_policy_arn(account_id, role_name, policy_name, i)
            _create_or_update_managed_policy(policy_arn, managed_policy_name, policy_document, base_session)
            _attach_managed_policy(role_name, policy_arn, base_session)

            partition_size = _estimate_policy_size(partition_statements)
            module_logger.info(f"Created/updated managed policy {managed_policy_name} ({partition_size} bytes)")

        module_logger.info(
            f"Successfully partitioned policy {policy_name} into {len(partitions)} managed policies " f"(total: {total_size} bytes)"
        )


def put_inlined_policy(
    role_name: str, policy_name: str, action_resource_pairs: Set["ConstructPermission"], base_session: boto3.Session
) -> None:
    """Legacy function that now uses managed policies to avoid AWS inline policy size limits.

    This function maintains the same interface as before but internally uses put_managed_policy
    to avoid the 10,240 byte limit on inline policies by using customer-managed policies instead.
    """
    # Check if the policy would exceed inline policy limits
    size_optimized_statements = _get_size_optimized_statements(action_resource_pairs)
    statements = [
        {"Effect": "Allow", "Action": [action for action in actions], "Resource": [resource for resource in resources]}
        for actions, resources in size_optimized_statements.items()
    ]

    estimated_size = _estimate_policy_size(statements)

    # Use safety buffer to avoid borderline cases where AWS calculates size differently
    safe_limit = AWS_INLINE_POLICY_SIZE_LIMIT - AWS_INLINE_POLICY_SIZE_BUFFER

    if estimated_size > safe_limit:
        module_logger.warning(
            f"Policy {policy_name} estimated size ({estimated_size} bytes) exceeds safe inline policy limit "
            f"({safe_limit} bytes) with buffer. Using managed policies instead."
        )
        # Use managed policies to avoid size limit
        put_managed_policy(role_name, policy_name, action_resource_pairs, base_session)
    else:
        # Use original inline policy logic for smaller policies
        iam = base_session.client("iam")
        inlined_policy = {
            "Version": "2012-10-17",
            "Statement": statements,
        }

        try:
            exponential_retry(
                iam.put_role_policy,
                ["ServiceFailureException", "AccessDenied"],  # Remove LimitExceededException from retryables
                PolicyDocument=json.dumps(inlined_policy),
                PolicyName=policy_name,
                RoleName=role_name,
            )
        except ClientError as err:
            if err.response["Error"]["Code"] == "LimitExceeded":
                # Fallback to managed policies if AWS rejects due to size limit
                module_logger.warning(
                    f"AWS rejected inline policy {policy_name} due to size limit. "
                    f"Estimated size: {estimated_size} bytes, AWS limit: {AWS_INLINE_POLICY_SIZE_LIMIT} bytes. "
                    f"Falling back to managed policies."
                )
                put_managed_policy(role_name, policy_name, action_resource_pairs, base_session)
            else:
                raise


def _delete_managed_policy(policy_arn: str, base_session: boto3.Session) -> None:
    """Delete a customer-managed policy after detaching it from all entities."""
    iam = base_session.client("iam")

    try:
        # Get all entities attached to this policy
        entities_response = exponential_retry(iam.list_entities_for_policy, ["ServiceFailureException"], PolicyArn=policy_arn)

        # Detach from all roles
        for role in entities_response.get("PolicyRoles", []):
            exponential_retry(iam.detach_role_policy, ["ServiceFailureException"], RoleName=role["RoleName"], PolicyArn=policy_arn)

        # Detach from all users
        for user in entities_response.get("PolicyUsers", []):
            exponential_retry(iam.detach_user_policy, ["ServiceFailureException"], UserName=user["UserName"], PolicyArn=policy_arn)

        # Detach from all groups
        for group in entities_response.get("PolicyGroups", []):
            exponential_retry(iam.detach_group_policy, ["ServiceFailureException"], GroupName=group["GroupName"], PolicyArn=policy_arn)

        # Delete all non-default versions first
        versions_response = exponential_retry(iam.list_policy_versions, ["ServiceFailureException"], PolicyArn=policy_arn)

        for version in versions_response.get("Versions", []):
            if not version["IsDefaultVersion"]:
                exponential_retry(
                    iam.delete_policy_version, ["ServiceFailureException"], PolicyArn=policy_arn, VersionId=version["VersionId"]
                )

        # Finally delete the policy
        exponential_retry(iam.delete_policy, ["ServiceFailureException"], PolicyArn=policy_arn)

        module_logger.info(f"Successfully deleted managed policy {policy_arn}")

    except ClientError as err:
        if err.response["Error"]["Code"] not in ["NoSuchEntity", "NoSuchEntityException"]:
            raise err


def delete_managed_policy(role_name: str, policy_name: str, base_session: boto3.Session) -> None:
    """Delete customer-managed policies associated with the given role and policy name.

    This function handles both single policies and partitioned policies using the role-specific naming scheme.
    """
    iam = base_session.client("iam")
    account_id = base_session.client("sts").get_caller_identity()["Account"]

    # Try to delete the main policy first
    main_policy_arn = _build_managed_policy_arn(account_id, role_name, policy_name)
    try:
        exponential_retry(iam.get_policy, ["ServiceFailureException"], PolicyArn=main_policy_arn)
        _delete_managed_policy(main_policy_arn, base_session)
    except ClientError as err:
        if err.response["Error"]["Code"] not in ["NoSuchEntity", "NoSuchEntityException"]:
            raise err

    # Also try to delete any partitioned policies (Part-1, Part-2, etc.)
    part_number = 1
    while part_number <= AWS_MAX_MANAGED_POLICIES_PER_ROLE:
        part_policy_arn = _build_managed_policy_arn(account_id, role_name, policy_name, part_number)

        try:
            exponential_retry(iam.get_policy, ["ServiceFailureException"], PolicyArn=part_policy_arn)
            _delete_managed_policy(part_policy_arn, base_session)
            part_number += 1
        except ClientError as err:
            if err.response["Error"]["Code"] in ["NoSuchEntity", "NoSuchEntityException"]:
                # No more partitioned policies
                break
            else:
                raise err


def delete_inlined_policy(role_name: str, policy_name: str, base_session: boto3.Session) -> None:
    """Delete both inline and managed policies associated with the given role and policy name.

    This function first tries to delete inline policies, then attempts to delete managed policies
    to provide full cleanup capability for policies created by either put_inlined_policy or put_managed_policy.
    """
    iam = base_session.client("iam")

    # First try to delete inline policy
    try:
        exponential_retry(
            iam.delete_role_policy,
            ["ServiceFailureException", "AccessDenied"],  # eventual consistency: e.g quick transition from activation to deletion
            PolicyName=policy_name,
            RoleName=role_name,
        )
        module_logger.info(f"Deleted inline policy {policy_name} from role {role_name}")
    except ClientError as err:
        if err.response["Error"]["Code"] not in ["NoSuchEntity", "NoSuchEntityException"]:
            raise err

    # Then try to delete managed policies (both single and partitioned)
    try:
        delete_managed_policy(role_name, policy_name, base_session)
    except ClientError as err:
        if err.response["Error"]["Code"] not in ["NoSuchEntity", "NoSuchEntityException"]:
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
        - Delete customer-managed policies created by IntelliFlow (if any)
        - Delete the role
    """
    iam = base_session.client("iam")

    # Get account ID for customer-managed policy ARN construction
    try:
        account_id = base_session.client("sts").get_caller_identity()["Account"]
    except Exception as e:
        module_logger.warning(f"Could not get account ID for cleaning customer-managed policies: {e}")
        account_id = None

    # 1. Delete inline policies
    inlined_policy_it = list_inlined_policies(role_name, base_session, iam)
    for policy_name in inlined_policy_it:
        exponential_retry(iam.delete_role_policy, ["ServiceFailureException"], RoleName=role_name, PolicyName=policy_name)

    # 2. Detach managed policies and identify IntelliFlow customer-managed policies for deletion
    intelliflow_managed_policies_to_delete = []
    attached_policy_it = list_attached_policies(role_name, base_session, iam)
    for policy_arn, policy_name in attached_policy_it:
        # Detach the policy
        exponential_retry(iam.detach_role_policy, ["ServiceFailureException"], RoleName=role_name, PolicyArn=policy_arn)

        # Check if this is an IntelliFlow customer-managed policy for this specific role (not AWS managed)
        if account_id and policy_arn.startswith(f"arn:aws:iam::{account_id}:policy/"):
            # Check if policy name matches this role's managed policy pattern
            if _is_role_managed_policy_name(role_name, policy_name):
                intelliflow_managed_policies_to_delete.append(policy_arn)

    # 3. Delete IntelliFlow customer-managed policies
    for policy_arn in intelliflow_managed_policies_to_delete:
        try:
            _delete_managed_policy(policy_arn, base_session)
        except Exception as e:
            module_logger.warning(f"Could not delete IntelliFlow managed policy {policy_arn}: {e}")

    # 4. Delete the role
    exponential_retry(iam.delete_role, ["ServiceFailureException", "ConcurrentModificationException"], RoleName=role_name)


def segregate_trusted_entities(trusted_entities_list: List[str]) -> Tuple[List[str], List[str]]:
    """
    Segregates a mixed list of trusted entities into AWS services and AWS entities.

    Args:
        trusted_entities_list: Mixed list containing AWS services (e.g., "lambda.amazonaws.com")
                              and AWS entities (e.g., IAM role ARNs, user ARNs)

    Returns:
        Tuple[List[str], List[str]]: A tuple containing (aws_services, aws_entities)
            - aws_services: List of AWS service endpoints (e.g., "lambda.amazonaws.com")
            - aws_entities: List of AWS entity ARNs (e.g., IAM roles, users)
    """
    aws_services = []
    aws_entities = []

    for entity in trusted_entities_list:
        if entity.endswith(".amazonaws.com"):
            # AWS service endpoints end with .amazonaws.com
            aws_services.append(entity)
        else:
            # Everything else is treated as AWS entity (IAM roles, users, etc.)
            aws_entities.append(entity)

    return aws_services, aws_entities
