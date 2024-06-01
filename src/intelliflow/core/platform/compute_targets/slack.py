# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import logging
from typing import ClassVar, List, Optional
from uuid import uuid4

import requests

from intelliflow.core.entity import CoreData
from intelliflow.core.permission import Permission
from intelliflow.core.platform.compute_targets.common_node_desc_utils import *
from intelliflow.core.platform.compute_targets.descriptor import ComputeDescriptor
from intelliflow.core.platform.constructs import ConstructPermission, RoutingComputeInterface, RoutingHookInterface
from intelliflow.core.platform.definitions.aws.common import (
    IF_DEV_ROLE_FORMAT,
    IF_DEV_ROLE_NAME_FORMAT,
    IF_EXE_ROLE_FORMAT,
    MAX_SLEEP_INTERVAL_PARAM,
)
from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
from intelliflow.core.platform.definitions.aws.common import (
    delete_inlined_policy,
    exponential_retry,
    generate_statement_id,
    get_aws_account_id_from_arn,
    get_aws_region_from_arn,
    put_inlined_policy,
)
from intelliflow.core.platform.definitions.compute import (
    ComputeInternalError,
    ComputeRetryableInternalError,
    ComputeRuntimeTemplateRenderer,
)
from intelliflow.core.serialization import dumps
from intelliflow.core.signal_processing import Signal, Slot
from intelliflow.core.signal_processing.slot import SlotType
from intelliflow.utils.url_validation import validate_url

logger = logging.getLogger(__name__)


class SlackAction(RoutingComputeInterface.IInlinedCompute, RoutingHookInterface.IHook, ComputeDescriptor):
    AWS_GET_SECRET_VALUE_ACTION: str = "secretsmanager:GetSecretValue"
    _SECRET_CHECK_CACHE: ClassVar[Dict[str, bool]] = {}

    def __init__(self, slack: "Slack" = None):
        self.slack = slack
        self._permissions = (
            [Permission(resource=[arn], action=[self.AWS_GET_SECRET_VALUE_ACTION]) for arn in slack.aws_secret_arns] if slack else None
        )
        self._permissions_for_secret = None

    @property
    def slack(self):
        return self._slack

    @slack.setter
    def slack(self, slack_obj):
        self._slack = slack_obj

    @property
    def permissions(self) -> Optional[List[Permission]]:
        if self._permissions and self._permissions_for_secret:
            return self._permissions + self._permissions_for_secret
        elif self._permissions:
            return self._permissions
        elif self._permissions_for_secret:
            return self._permissions_for_secret

        return None

    # overrides (ComputeDescriptor::parametrize)
    def parametrize(self, platform: "HostPlatform") -> None:
        if self.slack:
            # add permission to use aws/secretmanager AWS managed key (implicitly used by GetSecretValue action)
            account_id = platform.conf.get_param(AWSCommonParams.ACCOUNT_ID)
            region = platform.conf.get_param(AWSCommonParams.REGION)
            self._permissions_for_secret = [
                Permission(
                    action=["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"],
                    resource=[f"arn:aws:kms:{region}:{account_id}:key/*"],
                )
            ]

    # overrides (ComputeDescriptor::create_slot)
    def create_slot(self, output_signal: Signal) -> Slot:
        return Slot(SlotType.SYNC_INLINED, dumps(self), None, None, dict(), None, self.permissions, 0)

    def describe_slot(self) -> Dict[str, Any]:
        # donot expose any sensitive detail, the class type here is enough for high-level description of this action
        return {"message": self.slack.message}

    # overrides (ComputeDescriptor::activation_completed):w
    def activation_completed(self, platform: "HostPlatform") -> None:
        """System calls this to allow descriptor to know that system is up&running, all of the underlying resources
        are created. If the compute target needs to do further provisioning that relies on system resources, now it is
        time to set them up.

        We use this callback to update IF dev role with the secret access permissions required by this object
        """
        # Due overwriting functionality SlackAction is cloned with each 'action' call, so we might have multiple instances
        # this ComputeDescriptor. We need to avoid this check on the same secret arn multiple times using the
        # class level global cache.
        unique_context_id = platform.conf._generate_unique_id_for_context()
        arns_to_be_added = [arn for arn in self.slack.aws_secret_arns if not self._SECRET_CHECK_CACHE.get(unique_context_id + arn, None)]
        if not arns_to_be_added:
            return

        admin_session = platform.conf.get_admin_session()
        dev_role = IF_DEV_ROLE_NAME_FORMAT.format(platform.context_id, platform.processor.region)

        def _generate_policy_id(arn: str) -> str:
            return generate_statement_id(f"SLACK_{arn}".replace(Slack.AWS_SECRET_ARN_PREFIX, ""))

        policy_id = _generate_policy_id(arns_to_be_added[0])
        old_policy_ids = [_generate_policy_id(arn) for arn in arns_to_be_added[1:]]
        for old_policy_id in old_policy_ids:
            delete_inlined_policy(dev_role, old_policy_id, admin_session)

        # Allow dev-role to assume so during tests where Application::process uses the local Processor (platform).
        action_resources = [ConstructPermission([arn], [self.AWS_GET_SECRET_VALUE_ACTION]) for arn in arns_to_be_added]
        # make inlined policy unique for each role
        put_inlined_policy(dev_role, policy_id, set(action_resources), admin_session)
        # TODO "terminate" is not called on `ComputeDescriptor`s removed from the app
        #  Add changeset management for `ComputeDescriptor`s
        #  Without this, we might leave the dev role with unwanted statements for secrets not used in the app anymore.

        # No need to update the exec role!
        # self._permission propagation via Slot automatically updates the exec role during the activation
        for arn in arns_to_be_added:
            self._SECRET_CHECK_CACHE[unique_context_id + arn] = True

    # satisfies both
    #  - RoutingComputeInterface.IInlinedCompute
    #  - RoutingHookInterface.IHook
    def __call__(
        self,
        input_map_OR_routing_table: Union[Dict[str, Signal], "RoutingTable"],
        materialized_output_OR_route_record: Union[Signal, "RoutingTable.RouteRecord"],
        *args,
        **params,
    ) -> Any:
        # establish params dict for inlined compute. 'params' as kwargs is to be compatible with IHook interface. so
        # it should be an empty dict for IInlinedCompute.
        # we don't provide params as kwargs in IInlinedCompute interface, so for backwards compatibility reasons
        # we have to do this adaptation here to be compatible with hooks.
        # TODO evaluate passing 'params' as kwargs and change all of the inlined compute based computes in current IF
        #  apps (should not be many, so should not be a problem).
        inlined_compute_args = InlinedComputeParamExtractor(
            input_map_OR_routing_table, materialized_output_OR_route_record, *args, **params
        )
        message = self.slack.message
        if inlined_compute_args:
            _, _, params = inlined_compute_args
            if message:
                # runtime parametrization of desc (e.g variables mapping to input/output alias' and output dimensions)
                # using the pattern ${VARIABLE} will be mapped to their runtime values.
                renderer = ComputeRuntimeTemplateRenderer(
                    list(input_map_OR_routing_table.values()), materialized_output_OR_route_record, params
                )
                message = renderer.render(message)

        route_id, node_info = get_route_information_from_callback(
            input_map_OR_routing_table, materialized_output_OR_route_record, *args, **params
        )
        # truncate default node info (length > 10K causes Slack worfkflow msg_blocks_too_long error)
        # TODO create a better, shorter slack message format
        node_info = node_info[:5000]

        # simply removing {}""'' from output string for a better look
        trans_table = node_info.maketrans("", "", "{}\"\"''")

        data = {"Content": message + node_info.translate(trans_table)}

        for recipient in self.slack.recipient_list:
            try:
                url = self._create_url(recipient, **params)
                if not validate_url("".join(url)):
                    raise Exception("SSRF URL validation failed:" + "".join(url))
                response = requests.post(url=url, data=json.dumps(data), timeout=7)
                response.raise_for_status()
            except requests.exceptions.HTTPError as errh:
                error_message = "A HTTP error occurred with status: {0} when posting to Slack workflow API"
                logger.error(error_message.format(repr(errh)))
                raise ComputeInternalError(error_message.format(repr(errh)))
            except requests.exceptions.ConnectionError as errc:
                error_message = "A Connecting Error occurred when posting to Slack workflow API: {0}"
                logger.error(error_message.format(repr(errc)))
                # Retryable
                raise ComputeRetryableInternalError(error_message.format(repr(errc)))
            except requests.exceptions.Timeout as errt:
                error_message = "A Timeout Error occurred when posting to Slack workflow API: {0}"
                logger.error(error_message.format(repr(errt)))
                # Retryable
                raise ComputeRetryableInternalError(error_message.format(repr(errt)))
            except requests.exceptions.RequestException as err:
                error_message = "An Unknown Error occurred when posting to Slack workflow API: {0}"
                logger.error(error_message.format(repr(err)))
                raise ComputeInternalError(error_message.format(repr(err)))

    def _create_url(self, recipient: str, **params):
        if recipient in self.slack.aws_secret_arns:
            arn = recipient
            session = params[AWSCommonParams.BOTO_SESSION]
            region = params[AWSCommonParams.REGION]
            client = session.client(service_name="secretsmanager", region_name=region)

            response = exponential_retry(
                client.get_secret_value,
                {},
                **{"SecretId": arn, MAX_SLEEP_INTERVAL_PARAM: 8},
            )

            # Decrypts secret using the associated KMS key.
            recipient = json.loads(response["SecretString"])["slack_workflow_url"]
        return recipient


class Slack(CoreData):
    VALID_URL_PREFIX = "https://hooks.slack.com/workflows"
    AWS_SECRET_ARN_PREFIX = "arn:aws:secretsmanager"

    def __init__(
        self,
        recipient_list: List[str],
        message: str = "",
    ) -> None:
        """
        Parameters
        ----------
        recipient_list : List[str]
            A list a valid slack workflow urls. Refer to ./doc/SLACK_NOTIFICATION_SETUP.md to create working slack workflows.
        message : str
            Optional. A brief message
        """
        self.recipient_list = recipient_list
        self.message = message

    @property
    def recipient_list(self):
        return self._recipient_list

    @property
    def raw_workflow_urls(self):
        return [url for url in self._recipient_list if not url.lower().startswith(self.AWS_SECRET_ARN_PREFIX)]

    @property
    def aws_secret_arns(self):
        return [arn for arn in self._recipient_list if arn.lower().startswith(self.AWS_SECRET_ARN_PREFIX)]

    @recipient_list.setter
    def recipient_list(self, new_list: List[str]):
        if len(new_list) == 0:
            raise ValueError("A list of valid Slack Workflow URLs is required.")

        for url in new_list:
            if self.VALID_URL_PREFIX not in url.lower() and not url.lower().startswith(self.AWS_SECRET_ARN_PREFIX):
                logger.warning(
                    f"Recipient {url!r} does not seem to be a valid Slack workflow url that starts with {self.VALID_URL_PREFIX!r} "
                    f"nor an AWS Secret ARN!"
                )

        self._recipient_list = new_list

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, new_message: str):
        self._message = new_message

    def action(self, recipient_list: List[str] = None, message: str = ""):
        recipient_list = recipient_list or self.recipient_list
        message = message or self.message

        slack = Slack(recipient_list, message)

        return SlackAction(slack)
