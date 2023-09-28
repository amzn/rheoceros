# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from datetime import datetime
from time import time
from uuid import uuid4

import boto3
from boto3 import Session
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session

from intelliflow.core.platform.definitions.aws.common import MAX_SLEEP_INTERVAL_PARAM

from .common import exponential_retry

module_logger = logging.getLogger(__name__)

# check for max session duration
# https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use.html#id_roles_use_view-role-max-session
# The default is 1800 (30 minutes),
# since by default RefreshableCredentials will refresh credential will expire with 15 minutes.
DEFAULT_DURATION_SECONDS = 1800


class RefreshableBotoSession:
    """
    Boto Helper class which lets us create refreshable session, so that we can cache the client or resource.
    Usage
    -----
    session = RefreshableBotoSession(
        sts_arn="some_role_arn",
        session_name="some_name",
        region_name="some_region_name",
        base_session=base_session
    ).refreshable_session()
    client = session.client("s3") # we now can cache this client object without worrying about expiring credentials
    """

    def __init__(
        self,
        region_name: str = None,
        role_arn: str = None,
        base_session: Session = None,
        session_name: str = None,
        external_id: str = None,
        duration_seconds: int = DEFAULT_DURATION_SECONDS,
    ):
        """
        Initialize `RefreshableBotoSession`
        Parameters
        ----------
        region_name : str (optional)
            Default region when creating new connection.
        role_arn : str (optional)
            The role arn to assume by sts before creating session.
        base_session: Session
            The base session used to create sts client.
        session_name : str (optional)
            An identifier for the assumed role session. (required when `sts_arn` is given)
        duration_seconds: int
            Duration seconds for the session, the time should bigger than 15 minutes and smaller than 12 hours.
            RefreshableCredentials will refresh credential which will expire within 15 minutes.
            So the value cannot be 900 - exactly 15 minutes.
        """

        self.region_name = region_name
        self.role_arn = role_arn
        self.base_session = base_session
        self.duration_seconds = duration_seconds or DEFAULT_DURATION_SECONDS
        self.session_name = session_name or "refreshable_session_" + uuid4().hex
        self.external_id = external_id

    def __get_session_credentials(self):
        """
        Get session credentials.
        If sts_arn is given, get credential by assuming given role.
        If sts_arn is not given, return the base session credential.
        """
        if self.role_arn:
            sts_client = self.base_session.client(
                service_name="sts", region_name=self.region_name if self.region_name else self.base_session.region_name
            )
            kwargs = {
                "RoleArn": self.role_arn,
                "RoleSessionName": self.session_name,
                "DurationSeconds": self.duration_seconds,
                MAX_SLEEP_INTERVAL_PARAM: 128 + 1,
            }
            if self.external_id:
                kwargs.update({"ExternalId": self.external_id})
            # IAM propagation is annoying, so following a create_role action, we have to
            # enable the retry logic for a subsequent assume_role action.
            assume_role_object = exponential_retry(sts_client.assume_role, ["AccessDenied"], **kwargs)
            credentials = assume_role_object["Credentials"]

            credentials = {
                "access_key": credentials["AccessKeyId"],
                "secret_key": credentials["SecretAccessKey"],
                "token": credentials["SessionToken"],
                "expiry_time": credentials["Expiration"].isoformat(),
            }
        else:
            session_credentials = self.base_session.get_credentials().__dict__
            credentials = {
                "access_key": session_credentials.get("access_key"),
                "secret_key": session_credentials.get("secret_key"),
                "token": session_credentials.get("token"),
                "expiry_time": datetime.fromtimestamp(time() + self.duration_seconds).isoformat(),
            }

        return credentials

    def refreshable_session(self) -> Session:
        """
        Get refreshable boto3 session.
        """
        try:
            # get refreshable credentials
            refreshable_credentials = RefreshableCredentials.create_from_metadata(
                metadata=self.__get_session_credentials(),
                refresh_using=self.__get_session_credentials,
                method="sts-assume-role",
            )

            # attach refreshable credentials current session
            session = get_session()
            session._credentials = refreshable_credentials
            session.set_config_variable("region", self.region_name)
            auto_refresh_session = Session(botocore_session=session)

            return auto_refresh_session

        except Exception as error:
            module_logger.exception("Couldn't create refreshable session %s. Exception: %s", self.session_name, str(error))
            raise
