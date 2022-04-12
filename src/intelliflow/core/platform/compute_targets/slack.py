# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import logging
from typing import List

import requests

from intelliflow.core.entity import CoreData
from intelliflow.core.platform.compute_targets.common_node_desc_utils import *
from intelliflow.core.platform.compute_targets.descriptor import ComputeDescriptor
from intelliflow.core.platform.constructs import RoutingComputeInterface, RoutingHookInterface
from intelliflow.core.platform.definitions.compute import ComputeInternalError, ComputeRetryableInternalError
from intelliflow.core.serialization import dumps
from intelliflow.core.signal_processing import Signal, Slot
from intelliflow.core.signal_processing.slot import SlotType

logger = logging.getLogger(__name__)


class SlackAction(RoutingComputeInterface.IInlinedCompute, RoutingHookInterface.IHook, ComputeDescriptor):
    def __init__(self, slack: "Slack" = None):
        self.slack = slack
        self._permissions = None

    @property
    def slack(self):
        return self._slack

    @slack.setter
    def slack(self, slack_obj):
        self._slack = slack_obj

    @property
    def permissions(self):
        return self._permissions

    # overrides (ComputeDescriptor::create_slot)
    def create_slot(self, output_signal: Signal) -> Slot:

        return Slot(SlotType.SYNC_INLINED, dumps(self), None, None, dict(), None, self.permissions, 0)

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
        if inlined_compute_args:
            _, _, params = inlined_compute_args

        route_id, node_info = get_route_information_from_callback(
            input_map_OR_routing_table, materialized_output_OR_route_record, *args, **params
        )

        # simply removing {}""'' from output string for a better look
        trans_table = node_info.maketrans("", "", "{}\"\"''")

        data = {"Content": self.slack.message + node_info.translate(trans_table)}

        for url in self.slack.recipient_list:
            try:
                response = requests.post(url=url, data=json.dumps(data), timeout=5)
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


class Slack(CoreData):
    VALID_URL_PREFIX = "https://hooks.slack.com/workflows"

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

    @recipient_list.setter
    def recipient_list(self, new_list: List[str]):
        if len(new_list) == 0:
            raise ValueError("A list of valid Slack Workflow URLs is required.")

        for url in new_list:
            if self.VALID_URL_PREFIX not in url.lower():
                logger.warning(f"URL {url!r} does not seem to be a valid Slack workflow url that starts with {self.VALID_URL_PREFIX!r}")

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
