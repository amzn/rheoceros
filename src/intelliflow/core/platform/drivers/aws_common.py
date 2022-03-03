# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from abc import ABC
from typing import Union

import boto3

from intelliflow.core.platform.constructs import ConstructParamsDict

from ...signal_processing import Signal
from ...signal_processing.signal_source import SignalSourceAccessSpec
from ..definitions.aws.common import CommonParams as AWSCommonParams


class AWSConstructMixin:
    def __init__(self, params: ConstructParamsDict):
        super().__init__(params)
        self._session = params[AWSCommonParams.BOTO_SESSION]
        self._region = params[AWSCommonParams.REGION]
        self._account_id = params[AWSCommonParams.ACCOUNT_ID]

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._session = params[AWSCommonParams.BOTO_SESSION]

    # overrides
    def _serializable_copy_init(self, org_instance: "AWSConstructMixin") -> None:
        super()._serializable_copy_init(org_instance)
        self._params = copy.copy(org_instance._params)
        if AWSCommonParams.BOTO_SESSION in self._params:
            del self._params[AWSCommonParams.BOTO_SESSION]
        self._session = None

    @property
    def session(self):
        return self._session

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def region(self) -> str:
        return self._region

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        """Construct has been revived (deserialized) in AWS (by an AWS service) this time.
        we need to update the session object with a default one.
        """
        super().runtime_init(platform, context_owner)
        self._session = boto3.Session()
        self._params[AWSCommonParams.BOTO_SESSION] = self._session

    def _get_upstream_platform_for(
        self, signal_or_spec: Union[Signal, SignalSourceAccessSpec], dev_platform: "DevelopmentPlatform"
    ) -> "DevelopmentPlatform":
        """Convenience method for AWS drivers that want to fetch the upstream platform object
        for an upstream signal, using the underlying Platform's upstream connections map.
        That map keeps a track of upstream app uuid and their proxy platform instances.
        From the signal/access_spec, it is easy to retrieve the owner app/context UUID. Then
        the rest of the logic is to use this uuid to get the corresponding platform instance.

        It should be used whenever an external/upstream resource is to be accessed/modified.
        """
        context_uuid: str = None
        if isinstance(signal_or_spec, Signal):
            context_uuid = signal_or_spec.resource_access_spec.get_owner_context_uuid()
        else:
            context_uuid = signal_or_spec.get_owner_context_uuid()

        if context_uuid and context_uuid in dev_platform.upstream_connections:
            return dev_platform.upstream_connections[context_uuid]

        return None

    def _get_session_for(self, signal_or_spec: Union[Signal, SignalSourceAccessSpec], dev_platform: "DevelopmentPlatform") -> boto3.Session:
        """Convenience method for AWS drivers that want to fetch the appropriate session
        for an upstream signal, using the underlying Platform's upstream connections map.
        That map keeps a track of upstream app uuid and their proxy platform instances.
        From the signal/access_spec, it is easy to retrieve the owner app/context UUID. Then
        the rest of the logic is to use this uuid to get the corresponding platform instance
        that should contain the required session (because upstream/downstream platforms are
        guaranteed to match by the framework, and we don't even need to do extensive type
        checking and casting here).

        It should be used whenever an external/upstream resource is to be accessed/modified.

        It can be safely used for other Signal types as well. Default behaviour in that case
        is to fall back to the original session that belongs to the current platform/conf.
        """
        context_uuid: str = None
        if isinstance(signal_or_spec, Signal):
            context_uuid = signal_or_spec.resource_access_spec.get_owner_context_uuid()
        else:
            context_uuid = signal_or_spec.get_owner_context_uuid()

        if context_uuid and context_uuid in dev_platform.upstream_connections:
            upstream_platform = dev_platform.upstream_connections[context_uuid]
            return upstream_platform.conf.get_param(AWSCommonParams.BOTO_SESSION)

        return self._session
