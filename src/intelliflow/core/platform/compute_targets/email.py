# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, Dict, List, Union

from botocore.exceptions import ClientError

from intelliflow.core.entity import CoreData
from intelliflow.core.permission import Permission
from intelliflow.core.platform.compute_targets.common_node_desc_utils import *
from intelliflow.core.platform.compute_targets.descriptor import ComputeDescriptor
from intelliflow.core.platform.constructs import ConstructParamsDict, RoutingComputeInterface, RoutingHookInterface
from intelliflow.core.platform.definitions.aws.common import AWS_COMMON_RETRYABLE_ERRORS
from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
from intelliflow.core.platform.definitions.aws.common import exponential_retry, get_code_for_exception
from intelliflow.core.platform.definitions.compute import (
    ComputeInternalError,
    ComputeRetryableInternalError,
    ComputeRuntimeTemplateRenderer,
)
from intelliflow.core.serialization import dumps
from intelliflow.core.signal_processing import Signal, Slot
from intelliflow.core.signal_processing.slot import SlotType

logger = logging.getLogger(__name__)

CHARSET = "UTF-8"


class DestinationEmail(CoreData):
    def __init__(self, ToAddresses: List[str], CcAddresses: List[str], BccAddresses: List[str]) -> None:
        self.ToAddresses = ToAddresses
        self.CcAddresses = CcAddresses
        self.BccAddresses = BccAddresses


class BaseEmailData(CoreData):
    def __init__(self, Data: str, Charset: str = CHARSET) -> None:
        self.Data = Data
        self.Charset = Charset


class EmailBody(CoreData):
    def __init__(self, Text: BaseEmailData) -> None:
        self.Text = Text


class EmailMessage(CoreData):
    def __init__(self, Body: EmailBody, Subject: BaseEmailData) -> None:
        self.Body = Body
        self.Subject = Subject


class EmailRequestModel(CoreData):
    def __init__(self, Destination: DestinationEmail, Message: EmailMessage, Source: str, ConfigurationSetName: str):
        self.Destination = Destination
        self.Message = Message
        self.Source = Source
        self.ConfigurationSetName = ConfigurationSetName

    def get_json_request(self):
        # Filters the keys of the dict which have None as value
        def filter_none(d):
            if isinstance(d, dict):
                return {k: filter_none(v) for k, v in d.items() if v is not None}
            else:
                return d

        json_request_raw = json.loads(json.dumps(self, default=lambda o: getattr(o, "__dict__", str(o))))

        json_request_filtered = filter_none(json_request_raw)
        return json_request_filtered


class EmailAction(RoutingComputeInterface.IInlinedCompute, RoutingHookInterface.IHook, ComputeDescriptor):
    CHARSET = "UTF-8"

    def __init__(self, email: "EMAIL"):
        self._email = email

    @property
    def email(self):
        return self._email

    @property
    def permissions(self):
        return self._permissions

    # overrides (ComputeDescriptor::parametrize)
    def parametrize(self, platform: "HostPlatform") -> None:
        # default to platform account and region if user did not specify them
        aws_account = self._email.aws_account if self._email.aws_account else platform.conf.get_param(AWSCommonParams.ACCOUNT_ID)
        aws_region = self._email.aws_region if self._email.aws_region else platform.conf.get_param(AWSCommonParams.REGION)
        permission_resource = f"arn:aws:ses:{aws_region}:{aws_account}:identity/{self._email.sender}"
        self._permissions = [Permission(resource=[permission_resource], action=["ses:SendEmail"])]

    # overrides (ComputeDescriptor::create_slot)
    def create_slot(self, output_signal: Signal) -> Slot:
        return Slot(SlotType.SYNC_INLINED, dumps(self), None, None, dict(), None, self._permissions, 0)

    def describe_slot(self) -> Dict[str, Any]:
        # donot expose any sensitive detail, the class type here is enough for high-level description of this action
        return {name: repr(value) for name, value in self._email.__dict__.items()}

    # overrides (ComputeDescriptor::activation_completed):w
    def activation_completed(self, platform: "HostPlatform") -> None:
        """System calls this to allow descriptor to know that system is up&running, all of the underlying resources
        are created. If the compute target needs to do further provisioning that relies on system resources, now it is
        time to set them up."""
        pass

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
        email_body = self.email.body
        email_subject = self.email.subject
        if inlined_compute_args:
            _, _, params = inlined_compute_args
            # runtime parametrization of desc (e.g variables mapping to input/output alias' and output dimensions)
            # using the pattern ${VARIABLE} will be mapped to their runtime values.
            renderer = ComputeRuntimeTemplateRenderer(
                list(input_map_OR_routing_table.values()), materialized_output_OR_route_record, params
            )
            if email_body:
                email_body = renderer.render(email_body)

            if email_subject:
                email_subject = renderer.render(email_subject)

        base_session = params[AWSCommonParams.BOTO_SESSION]
        client = base_session.client("ses", region_name=params[AWSCommonParams.REGION])

        callback_type = params.get(RoutingHookInterface.HOOK_TYPE_PARAM, RoutingComputeInterface.IInlinedCompute).__name__
        route_id, node_info = get_route_information_from_callback(
            input_map_OR_routing_table, materialized_output_OR_route_record, *args, **params
        )
        subject_extension = (
            "RheocerOS_App: " + params["UNIQUE_ID_FOR_CONTEXT"] + " - RouteID: " + route_id + " - Trigger: " + callback_type
        )

        dest_email = DestinationEmail(
            ToAddresses=self.email.recipient_list, CcAddresses=self.email.cc_list, BccAddresses=self.email.bcc_list
        )

        email_body = EmailBody(Text=BaseEmailData(Data=email_body + "\n\n" + node_info if email_body is not None else node_info))
        subject = BaseEmailData(Data=email_subject + " - " + subject_extension if email_subject is not None else subject_extension)

        email_msg = EmailMessage(Body=email_body, Subject=subject)

        email_request_model = EmailRequestModel(
            Destination=dest_email, Message=email_msg, Source=self.email.sender, ConfigurationSetName=self.email.config_set_name
        )

        email_request_model_json = email_request_model.get_json_request()
        response = None
        try:
            # https://docs.aws.amazon.com/ses/latest/APIReference/API_SendEmail.html
            response = exponential_retry(client.send_email, [], **email_request_model_json)
        except ClientError as e:
            error_code = get_code_for_exception(e)
            if error_code in AWS_COMMON_RETRYABLE_ERRORS:
                raise ComputeRetryableInternalError(
                    f"Got retryable error {error_code!r} from AWS SES! Will retry " f"sending the email in the next cycle."
                )
            else:
                raise ComputeInternalError("Request to AWS SES has been rejected: {0}".format(repr(e)))
        return response


class EMAIL(CoreData):
    def __init__(
        self,
        sender: str,
        recipient_list: List[str],
        aws_account: str = None,
        aws_region: str = None,
        cc_list: List[str] = None,
        bcc_list: List[str] = None,
        subject: str = None,
        body: str = None,
        config_set_name: str = None,
    ):
        self.sender = sender
        self.recipient_list = recipient_list
        self.aws_account = aws_account
        self.aws_region = aws_region
        self.cc_list = cc_list
        self.bcc_list = bcc_list
        self.subject = subject
        self.body = body
        self.config_set_name = config_set_name

    def action(self, sender=None, recipient_list=None, cc_list=None, bcc_list=None, subject=None, body=None, config_set_name=None):
        sender = self.sender if sender is None else sender
        recipient_list = self.recipient_list if recipient_list is None else recipient_list
        cc_list = self.cc_list if cc_list is None else cc_list
        bcc_list = self.bcc_list if bcc_list is None else bcc_list
        subject = self.subject if subject is None else subject
        body = self.body if body is None else body
        config_set_name = self.config_set_name if config_set_name is None else config_set_name

        if sender is None:
            logger.error("sender param is required")
            raise ValueError("sender param is required")

        if recipient_list is None:
            logger.error("recipient_list param is required")
            raise ValueError("recipient_list param is required")

        email = EMAIL(
            sender=sender,
            recipient_list=recipient_list,
            aws_account=self.aws_account,
            aws_region=self.aws_region,
            cc_list=cc_list,
            bcc_list=bcc_list,
            subject=subject,
            body=body,
            config_set_name=config_set_name,
        )

        email_action = EmailAction(email)
        return email_action
