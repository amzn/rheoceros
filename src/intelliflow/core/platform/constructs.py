# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
import traceback
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Any, ClassVar, Dict, Iterable, Iterator, List, NewType, Optional, Sequence, Set, Tuple, Type, Union, cast
from uuid import uuid4

from intelliflow.core.entity import CoreData
from intelliflow.core.platform.definitions.compute import (
    ComputeExecutionDetails,
    ComputeFailedResponse,
    ComputeFailedResponseType,
    ComputeFailedSessionState,
    ComputeFailedSessionStateType,
    ComputeInternalError,
    ComputeResourceDesc,
    ComputeResponse,
    ComputeResponseType,
    ComputeRetryableInternalError,
    ComputeSessionDesc,
    ComputeSessionState,
    ComputeSessionStateType,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.serialization import Serializable, loads
from intelliflow.core.signal_processing import DimensionFilter, Signal, Slot
from intelliflow.core.signal_processing.definitions.dimension_defs import Type as DimensionType
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import (
    METRIC_DIMENSION_SPEC,
    MetricDimension,
    MetricPeriod,
    MetricStatistic,
    MetricStatisticData,
    MetricSubDimensionMapType,
    MetricValueCountPairData,
)
from intelliflow.core.signal_processing.routing_runtime_constructs import Route, RouteID, current_timestamp_in_utc
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalLinkNode, SignalProvider, SignalType
from intelliflow.core.signal_processing.signal_source import (
    METRIC_VISUALIZATION_PERIOD_HINT,
    METRIC_VISUALIZATION_STAT_HINT,
    InternalDatasetSignalSourceAccessSpec,
    InternalMetricSignalSourceAccessSpec,
    SignalSourceAccessSpec,
    SignalSourceType,
)
from intelliflow.core.signal_processing.slot import SlotType

logger = logging.getLogger(__name__)


ConstructParamsDict = Dict[str, Any]


class ConstructPermission(CoreData):
    """Generic representation of what a Construct wants as permission in dev or run-time mode.
    This module intentionally avoids the reuse of 'permission.Permission' due to separate, dedicated
    mechanisms to gather devtime and runtime permissions from underlying drivers.
    """

    def __init__(self, resource: List[str], action: List[str]) -> None:
        self.resource = resource
        self.action = action

    def __eq__(self, other) -> bool:
        return set(self.resource) == set(other.resource) and set(self.action) == set(other.action)

    def __hash__(self) -> int:
        return hash((frozenset(self.resource), frozenset(self.action)))

    def _as_tuple(self) -> tuple:
        return (self.resource, self.action)

    def __len__(self):
        return len(self._as_tuple())

    def __iter__(self):
        return iter(self._as_tuple())


class ConstructPermissionGroup(CoreData):
    def __init__(self, group_id: str, permissions: Set[ConstructPermission]) -> None:
        self.group_id = group_id
        self.permissions = permissions

    def __eq__(self, other) -> bool:
        return self.group_id == other.group_id

    def __hash__(self) -> int:
        return hash(self.group_id)


class ConstructInternalMetricDesc(CoreData):
    """Class used by drivers to provide their route-level or high-level internal metrics.

    This is a short description of an internal metric <Signal>. Fields here are the only varying parts among the
    different internal/route metrics of the same driver or across different drivers.

    This class helps reduce code duplication and verbosity related to Signal object instantiation in drivers, when
    _provide_route_metrics or _provide_internal_metrics is called.
    """

    def __init__(
        self,
        id: str,
        metric_names: List[str],
        extra_sub_dimensions: Optional[MetricSubDimensionMapType] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        self.id = id
        self.metric_names = metric_names
        self.extra_sub_dimensions = extra_sub_dimensions
        self.metadata = metadata

    def __eq__(self, other) -> bool:
        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)


class UpstreamGrants(CoreData):
    def __init__(self, permission_groups: Set[ConstructPermissionGroup]) -> None:
        self.permission_groups = permission_groups

    def __lshift__(self, other: "UpstreamGrants"):
        """Ingest 'other' into this"""
        for group in self.permission_groups:
            for other_group in other.permission_groups:
                if group == other_group:
                    group.permissions.update(other_group.permissions)

        self.permission_groups.update(other.permission_groups - self.permission_groups)


# UpstreamRevokes = NewType("UpstreamRevokes", UpstreamGrants)
UpstreamRevokes = UpstreamGrants


class DataHandlingType(str, Enum):
    """https://skb.highcastle.a2z.com/guides/88#known-systems"""

    PASSING = "PASSING"
    PROCESSING = "PROCESSING"
    PERSISTING = "PERSISTING"


class EncryptionKeyAllocationLevel(str, Enum):
    HIGH = "ALL"
    PER_RESOURCE = "PER_RESOURCE"


class ConstructEncryption(CoreData):
    """Basic data class containing metadata to control the encryption related features of a driver.

    @:param key_allocation_level: EncryptionKeyAllocationLevel
    @:param key_rotation_cycle_in_days: int:
                None values mean 'use default' in Construct impl
    @:param is_hard_rotation: bool,
               With hard rotation, old data encrypted with prev key cannot be decrpyted with the new one.
               So a re-encryption might required with the new key. For example, KMS default rotation is NOT hard.
    @:param reencrypt_old_data_during_hard_rotation: bool
               Controls the behaviour on old data when 'is_hard_rotation' is enabled.
    @:param trust_access_from_same_root: bool
    """

    def __init__(
        self,
        key_allocation_level: EncryptionKeyAllocationLevel,
        # None values mean 'use default' in Construct impl"""
        key_rotation_cycle_in_days: int,
        # With hard rotation, old data encrypted with prev key cannot be decrpyted with the new one.
        # So a re-encryption might required with the new key. For example, KMS default rotation is NOT hard."""
        is_hard_rotation: bool,
        # Controls the behaviour on old data when 'is_hard_rotation' is enabled."""
        reencrypt_old_data_during_hard_rotation: bool,
        trust_access_from_same_root: bool,
    ) -> None:
        self.key_allocation_level = key_allocation_level
        self.key_rotation_cycle_in_days = key_rotation_cycle_in_days
        self.is_hard_rotation = is_hard_rotation
        self.reencrypt_old_data_during_hard_rotation = reencrypt_old_data_during_hard_rotation
        self.trust_access_from_same_root = trust_access_from_same_root


class ConstructPersistenceSecurityDef(CoreData):
    def __init__(self, encryption: ConstructEncryption) -> None:
        self.encryption = encryption


class ConstructPassingSecurityDef(CoreData):
    def __init__(self, protocol: str) -> None:
        self.protocol = protocol


class ConstructProcessingSecurityDef(CoreData):
    def __init__(self, zero_sensitive_data_after_use: bool, enforce_privilege_separation: bool) -> None:
        self.zero_sensitive_data_after_use = zero_sensitive_data_after_use
        self.enforce_privilege_separation = enforce_privilege_separation


class ConstructSecurityConf(CoreData):
    def __init__(
        self,
        persisting: Optional[ConstructPersistenceSecurityDef],
        passing: Optional[ConstructPassingSecurityDef],
        processing: Optional[ConstructProcessingSecurityDef],
    ) -> None:
        self.persisting = persisting
        self.passing = passing
        self.processing = processing


class _PendingConnRequest(CoreData):
    """Encapsulates devtime connection requests from other constructs"""

    def __init__(self, resource_type: str, resource_path: str, source_construct: "BaseConstruct") -> None:
        self.resource_type = resource_type
        self.resource_path = resource_path
        self.source_construct = source_construct

    def __eq__(self, other: "_PendingConnRequest") -> bool:
        return (
            self.resource_type == other.resource_type
            and self.resource_path == other.resource_path
            and (self.source_construct.__class__ == other.source_construct.__class__)
        )

    def __hash__(self) -> int:
        return hash(self.resource_path)


class BaseConstruct(Serializable, ABC):
    @abstractmethod
    def __init__(self, params: ConstructParamsDict) -> None:
        self._dev_platform: "DevelopmentPlatform" = None
        self._runtime_platform: "RuntimePlatform" = None
        self._params = params
        self._pending_connection_requests: Set[_PendingConnRequest] = set()
        self._pending_external_signals: Set[Signal] = set()
        self._pending_internal_routes: Set[Route] = set()
        self._pending_internal_signals: Set[Signal] = set()
        self._pending_security_conf: ConstructSecurityConf = None
        self._processed_connection_requests: Set[_PendingConnRequest] = set()
        self._processed_external_signals: Set[Signal] = set()
        self._processed_internal_routes: Set[Route] = set()
        self._processed_internal_signals: Set[Signal] = set()
        self._processed_security_conf: ConstructSecurityConf = None
        self._terminated = False

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        self._params = params
        # TODO remove after v1.0 release (temporary backwards compatibility)
        if not getattr(self, "_pending_security_conf", None):
            self._pending_security_conf: ConstructSecurityConf = None
        if not getattr(self, "_processed_security_conf", None):
            self._processed_security_conf: ConstructSecurityConf = None

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        self._dev_platform: "DevelopmentPlatform" = None
        self._runtime_platform: "RuntimePlatform" = None

    def update_serialized_cross_refs(self, original_to_serialized_map: Dict["BaseConstruct", "BaseConstruct"]):
        # we maintain explicit references to other constructs (because of pending conn requests),
        # we need to update them with their serialized versions.
        serialized_processed_connection_requests: Set[_PendingConnRequest] = set()
        for conn_request in self._processed_connection_requests:
            serialized_processed_connection_requests.add(
                _PendingConnRequest(
                    conn_request.resource_type, conn_request.resource_path, original_to_serialized_map[conn_request.source_construct]
                )
            )
        self._processed_connection_requests = serialized_processed_connection_requests

        serialized_pending_connection_requests: Set[_PendingConnRequest] = set()
        for conn_request in self._pending_connection_requests:
            serialized_pending_connection_requests.add(
                _PendingConnRequest(
                    conn_request.resource_type, conn_request.resource_path, original_to_serialized_map[conn_request.source_construct]
                )
            )
        self._pending_connection_requests = serialized_pending_connection_requests

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(params={self._params})"

    @abstractmethod
    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        self._dev_platform = platform
        self._terminated = False

    @abstractmethod
    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        self._runtime_platform = platform

    def get_platform(self) -> "Platform":
        return self._dev_platform if self._dev_platform else self._runtime_platform

    def is_in_runtime(self) -> bool:
        return self._runtime_platform

    @abstractmethod
    def provide_runtime_trusted_entities(self) -> List[str]:
        # not sure now if development time callbacks will influence this or not.
        # thinking that underlying resource provisioning might be dynamic and depend on dev-time state of an app.
        # so not making it a classmethod.
        pass

    @abstractmethod
    def provide_runtime_default_policies(self) -> List[str]:
        ...

    @abstractmethod
    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        pass

    @classmethod
    @abstractmethod
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        pass

    def _consolidate_metrics(self, metric_signals: List[Signal]) -> List[Signal]:
        consolidated_metric_signals = []
        for metric_signal in metric_signals:
            dimension_filter_spec = metric_signal.domain_spec.dimension_filter_spec
            if not dimension_filter_spec.check_spec_match(METRIC_DIMENSION_SPEC):
                # Driver impl error! So raising type error here to indicate a problem with the configuration of the
                # framework or the impl of the driver which is definitely not compatible with the metric format
                # expectation of the framework.
                raise TypeError(
                    f"Metric (id={metric_signal.alias}: Dimension filter {dimension_filter_spec!r} is not compatible "
                    f"with the expected dimension spec {METRIC_DIMENSION_SPEC!r}."
                )

            dimension_filter_spec.set_spec(METRIC_DIMENSION_SPEC)
            consolidated_metric_signals.append(
                Signal(
                    metric_signal.type,
                    metric_signal.resource_access_spec,
                    SignalDomainSpec(METRIC_DIMENSION_SPEC, dimension_filter_spec, None),
                    metric_signal.alias,
                )
            )

        return consolidated_metric_signals

    def provide_system_metrics(self) -> List[Signal]:
        return self._consolidate_metrics(self._provide_system_metrics())

    @abstractmethod
    def _provide_system_metrics(self) -> List[Signal]:
        """Provide metrics auto-generated by the underlying system components/resources. These metrics types are
        explicit and internally represented as 'external' metric types such as CW_METRIC, etc."""
        ...

    @classmethod
    def _create_internal_metric_alias(cls, id: str, route: Optional[Route] = None) -> str:
        return id + (("." + route.output.alias) if route is not None else "")

    def _convert_internal_metric_desc_to_signal(
        self, internal_metric: ConstructInternalMetricDesc, route: Optional[Route] = None
    ) -> Signal:
        """Converts simple ConstructInternalMetricDesc objects from driver impl to actual metric Signals.

        This method avoids code-duplication in drivers by containing the boilerplate to create an internal signal.

        Used inside provide_internal_metrics and provide_route_metrics wrappers that convert simple metric descriptions
        to actual Signals.
        """
        # - sub-dimensions
        sub_dimensions = {}
        if route is not None:
            #   add common RouteID sub-dimension to the metric first
            sub_dimensions = {"RouteID": route.output.alias}  # or route.route_id
        #   then add internal metric sub dimensions (if any)
        if internal_metric.extra_sub_dimensions:
            sub_dimensions.update(internal_metric.extra_sub_dimensions)

        # always add id as metric group id to isolate internal metrics from each other, as they might overlap over
        # 'Name' dimensions and sub_dimensions. So, differentiate sub_dimensions.
        sub_dimensions.update({InternalMetricSignalSourceAccessSpec.METRIC_GROUP_ID_SUB_DIMENSION_KEY: internal_metric.id})

        # - metadata
        kwargs = dict()
        kwargs.update(
            {
                # internal signals should have this set
                SignalSourceAccessSpec.OWNER_CONTEXT_UUID: self.get_platform().conf._generate_unique_id_for_context()
            }
        )
        if internal_metric.metadata:
            kwargs.update(internal_metric.metadata)

        # convert into an internal metric Signal
        return Signal(
            SignalType.INTERNAL_METRIC_DATA_CREATION,
            InternalMetricSignalSourceAccessSpec(sub_dimensions, **kwargs),
            SignalDomainSpec(
                dimension_filter_spec=DimensionFilter.load_raw(
                    {
                        metric_name: {  # Name
                            "*": {  # Statistic
                                "*": {  # Period  (AWS emits with 1 min by default), let user decide
                                    "*": {}  # Time  (always leave it 'any' if not experimenting)
                                }
                            }
                        }
                        for metric_name in internal_metric.metric_names
                    }
                ),
                dimension_spec=None,
                integrity_check_protocol=None,
            ),
            # for route metrics, id is specialized so that at runtime Diagnostics can retrieve them uniquely.
            self._create_internal_metric_alias(internal_metric.id, route),
        )

    def provide_internal_metrics(self) -> List[Signal]:
        metric_signals = []
        internal_metrics: List[ConstructInternalMetricDesc] = self._provide_internal_metrics()
        if internal_metrics:
            for internal_metric in internal_metrics:
                metric_signals.append(self._convert_internal_metric_desc_to_signal(internal_metric))
        return self._consolidate_metrics(metric_signals)

    @abstractmethod
    def _provide_internal_metrics(self) -> List[ConstructInternalMetricDesc]:
        """Provide internal (orchestration) metrics (of type INTERNAL_METRIC) that should be managed by RheocerOS and emitted by this
        driver via Diagnostics::emit.
        These metrics are logical metrics generated by the driver (with no assumption on other drivers and other details
        about the underlying platform). So as a driver impl, you want Diagnostics driver to manage those metrics and
        bind them to alarms, etc. Example: High-level orchestration metrics which are not route specific such as
        Processor runtime failures. These type of metrics are from RheocerOS' own business/orchestration logic
        which are independent from underlying system metrics (provided via 'provide_system_metrics'),
        """
        ...

    def provide_metrics_for_new_routes(self) -> List[Signal]:
        """Provide driver specific route metrics for all of the new/pending routes that wait to be activated."""
        # New route candidates are pending, cached in self._pending_internal_routes
        return [metric for route in self._pending_internal_routes for metric in self.provide_route_metrics(route)]

    def provide_route_metrics(self, route: Route) -> List[Signal]:
        """Converts simple ConstructInternalMetricDesc objects from driver impl to actual metric Signals.

        This method avoids code-duplication in drivers by containing the boilerplate to create an internal signal.
        """
        route_metrics = []
        internal_metrics: List[ConstructInternalMetricDesc] = self._provide_route_metrics(route)
        if internal_metrics:
            for internal_metric in internal_metrics:
                route_metrics.append(self._convert_internal_metric_desc_to_signal(internal_metric, route))
        return self._consolidate_metrics(route_metrics)

    @abstractmethod
    def _provide_route_metrics(self, route: Route) -> List[ConstructInternalMetricDesc]:
        """Provide orchestration metrics for the route/node (of type INTERNAL_METRIC) that should be managed by
        RheocerOS and emitted by this driver via Diagnostics::emit.
        These metrics are logical metrics generated by the driver (with no assumption on other drivers and other details
        about the underlying platform). So as a driver impl, you want Diagnostics driver to manage those metrics and
        bind them to alarms, etc. Example: Routing metrics.
        """
        ...

    def metric(self, id: str, route: Optional[Route] = None) -> "Diagnostics.MetricAction":
        """Create MetricAction object for (route metric or high-level internal metric) using the Diagnostics module from
         the platform. MetricAction can later be used to specify the NAME dimension and then finally to 'emit'.

        Examples:
            self.metric("processor.event.type")["S3"].emit(1)
            self.metric("routing_table.route.state", route)["COMPLETED"].emit(1)

        Internal metric to be emitted must have been reported by this driver via _provide_route_metrics (if route is not
        None) or _provide_internal_metrics during the last activation, otherwise this metric object will have no effect
        (NOOP) even if 'emit' is called on it.

        :param route: optional route object that will be used to form the internal metric ID (group alias) that will be
        used by the underlying Diagnostics driver to retrieve the internal metric for emission.
        :return: Diagnostics.MetricAction object wrapping the internal metric signal for dimension specialization. If
        metric signal cannot be found using 'id', then a NOOP MetricAction object is returned so that following 'emit'
        call won't raise.
        """
        internal_metric_alias = self._create_internal_metric_alias(id, route)
        metric_action = self.get_platform().diagnostics.get_internal_metric(internal_metric_alias)
        return metric_action if metric_action else Diagnostics._NOOPMetricAction()

    def provide_alarms(self) -> List[Signal]:
        return []

    @abstractmethod
    def _provide_internal_alarms(self) -> List[Signal]:
        """Provide internal alarms (of type INTERNAL_ALARM OR INTERNAL_COMPOSITE_ALARM) managed by RheocerOS"""
        ...

    @abstractmethod
    def activate(self) -> None:
        pass

    def _update_bootstrapper(self, bootstrapper: "RuntimePlatform") -> None:
        pass

    def process_signals(self) -> None:
        if not self.terminated:
            self._process_internal(self._pending_internal_routes, self._processed_internal_routes)
            self._process_internal_signals(self._pending_internal_signals, self._processed_internal_signals)
            self._process_external(self._pending_external_signals, self._processed_external_signals)

    def process_connections(self) -> None:
        if not self.terminated:
            self._process_construct_connections(self._pending_connection_requests, self._processed_connection_requests)

    def process_security_conf(self) -> None:
        if not self.terminated:
            self._process_security_conf(self._pending_security_conf, self._processed_security_conf)

    def process_downstream_connection(self, downstream_platform: "DevelopmentPlatform") -> UpstreamGrants:
        """Called by downstream platform to establish/update the connection"""
        return UpstreamGrants(set())

    def terminate_downstream_connection(self, downstream_platform: "DevelopmentPlatform") -> UpstreamRevokes:
        """Called by downstream platform to request the termination of the connection"""
        return UpstreamRevokes(set())

    @abstractmethod
    def rollback(self) -> None:
        """Platform activation failed. Roll-back most recent activation."""
        # TODO this common module should provide more advanced mechanisms
        #  to facilitate rollback in construct impls, via a more transactional approach.
        # Or Versioning will be considered during each activation (this mechanism will be deprecated).
        self._revert_security_conf(self._pending_security_conf, self._processed_security_conf)
        self._revert_construct_connections(self._pending_connection_requests, self._processed_connection_requests)
        self._revert_external(self._pending_external_signals, self._processed_external_signals)
        self._revert_internal(self._pending_internal_routes, self._processed_internal_routes)
        self._revert_internal_signals(self._pending_internal_signals, self._processed_internal_signals)

    @property
    def terminated(self) -> bool:
        return self._terminated

    @abstractmethod
    def terminate(self) -> None:
        """Delete underlying resources / policies / permissions

        Drivers call this at the end of their termination process.
        """
        self._terminated = True

    @abstractmethod
    def check_update(self, prev_construct: "BaseConstruct") -> None:
        self.transfer_transaction_history(prev_construct)

    def transfer_transaction_history(self, prev_construct: "BaseConstruct") -> None:
        self._processed_connection_requests = prev_construct._processed_connection_requests
        self._processed_external_signals = prev_construct._processed_external_signals
        self._processed_internal_routes = prev_construct._processed_internal_routes
        self._processed_internal_signals = prev_construct._processed_internal_signals
        # TODO remove after v1.0 release (temporary backwards compatibility)
        if getattr(prev_construct, "_processed_security_conf", None):
            self._processed_security_conf = prev_construct._processed_security_conf
        else:
            self._processed_security_conf: ConstructSecurityConf = None

    def hook_external(self, signals: List[Signal]) -> None:
        self._pending_external_signals.update(signals)

    def hook_internal(self, route: "Route") -> None:
        self._pending_internal_routes.add(route)

    def hook_internal_signal(self, signal: "Signal") -> None:
        self._pending_internal_signals.add(signal)

    def connect_construct(self, construct_resource_type: str, construct_resource_path: str, other: "BaseConstruct") -> None:
        self._pending_connection_requests.add(_PendingConnRequest(construct_resource_type, construct_resource_path, other))

    def hook_security_conf(
        self, security_conf: ConstructSecurityConf, platform_security_conf: Dict[Type["BaseConstruct"], ConstructSecurityConf]
    ) -> None:
        self._pending_security_conf = security_conf

    @abstractmethod
    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        ...

    @abstractmethod
    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        ...

    @abstractmethod
    def _process_internal_signals(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        ...

    @abstractmethod
    def _process_construct_connections(
        self, new_construct_conns: Set["_PendingConnRequest"], current_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        ...

    @abstractmethod
    def _process_security_conf(self, new_security_conf: ConstructSecurityConf, current_security_conf: ConstructSecurityConf) -> None:
        ...

    @abstractmethod
    def _revert_external(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        ...

    @abstractmethod
    def _revert_internal(self, routes: Set[Route], prev_routes: Set[Route]) -> None:
        ...

    @abstractmethod
    def _revert_internal_signals(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        ...

    @abstractmethod
    def _revert_construct_connections(
        self, construct_conns: Set["_PendingConnRequest"], prev_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        ...

    @abstractmethod
    def _revert_security_conf(selfs, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        ...

    def activation_completed(self) -> None:
        # now we can safely adapt the new state. whole app got into new configuration consistently.
        self._processed_connection_requests = self._pending_connection_requests
        self._processed_external_signals = self._pending_external_signals
        self._processed_internal_routes = self._pending_internal_routes
        self._processed_internal_signals = self._pending_internal_signals
        self._processed_security_conf = self._pending_security_conf
        self._pending_connection_requests = set()
        self._pending_external_signals = set()
        self._pending_internal_routes = set()
        self._pending_internal_signals = set()
        self._pending_security_conf = None


class Storage(BaseConstruct, ABC):
    @abstractmethod
    def get_storage_resource_path(self) -> str:
        """URI for object-storage. Internal-data might or might not be the same, depending on driver impls."""
        ...

    def get_internal_data_resource_path(self) -> str:
        """Impls need to override if they use a separate tech for internal-data

        Example: S3 -> Object storage
                 another AWS storage tech -> Internal data
        """
        return self.get_storage_resource_path()

    @abstractmethod
    def get_event_channel_type(self) -> str:
        ...

    @abstractmethod
    def get_event_channel_resource_path(self) -> str:
        ...

    @abstractmethod
    def subscribe_downstream_resource(self, downstream_platform: "DevelopmentPlatform", resource_type: str, resource_path: str):
        ...

    @abstractmethod
    def unsubscribe_downstream_resource(self, downstream_platform: "DevelopmentPlatform", resource_type: str, resource_path: str):
        ...

    @abstractmethod
    def get_object_uri(self, folders: Sequence[str], object_name: str) -> str:
        ...

    @abstractmethod
    def check_object(self, folders: Sequence[str], object_name: str) -> bool:
        ...

    def load(self, folders: Sequence[str], object_name: str) -> str:
        return self.load_object(folders, object_name).decode("utf-8")

    @abstractmethod
    def load_object(self, folders: Sequence[str], object_name: str) -> bytes:
        ...

    def save(self, data: str, folders: Sequence[str], object_name: str) -> None:
        self.save_object(data.encode("utf-8"), folders, object_name)

    @abstractmethod
    def save_object(self, data: bytes, folders: Sequence[str], object_name: str) -> None:
        ...

    @abstractmethod
    def delete(self, folders: Sequence[str], object_name: str) -> None:
        ...

    @abstractmethod
    def check_folder(self, prefix_or_folders: Union[str, Sequence[str]]) -> bool:
        ...

    @abstractmethod
    def load_folder(self, prefix_or_folders: Union[str, Sequence[str]], limit: int = None) -> Iterator[Tuple[str, bytes]]:
        """returns (materialized full path ~ data) pairs"""
        ...

    @abstractmethod
    def load_internal(self, internal_signal: Signal, limit: int = None) -> Iterator[Tuple[str, bytes]]:
        """returns (materialized full path ~ data) pairs of all of the objects represented by this signal

        If internal signal represents a domain (multiple folder for example), then 'limit' param is applied
        to each one of them (not as limit to the cumulative sum of number of object from each folder).
        """
        ...

    @abstractmethod
    def delete_internal(self, internal_signal: Signal, entire_domain: bool = True) -> bool:
        """Deletes the internal data represented by this signal.

        If 'entire_domain' is True (by default), then internal data represented by the all possible dimension values
        for the signal (the domain) will be wiped out. For that case, the behaviour does not depend on whether the
        signal is materialized or not (uses the 'folder' / root information for the deletion).

        if 'entire_domain' is False, then the signal should be materialized. A specific part of the internal data
        (aka partition for datasets) gets wiped out and the remaining parts of the data stays intact along with folder
        structure.

        returns True if the data is completely wiped out.
        """
        ...

    @abstractmethod
    def is_internal(self, source_type: SignalSourceType, resource_path: str) -> bool:
        ...

    def map_materialized_signal(self, materialized_signal) -> Optional[Signal]:
        """Converts a materialized signal to its internal representation if it is an Internal signal actually.

        Returns None if the external, materialized signal does not belong to this Storage (not Internal)."""
        source_type = materialized_signal.resource_access_spec.source
        resource_path = materialized_signal.get_materialized_resource_paths()[0]
        if self.is_internal(source_type, resource_path):
            spec = self.map_incoming_event(source_type, resource_path)
            internal_spec = InternalDatasetSignalSourceAccessSpec.create_from_external(materialized_signal.resource_access_spec, spec)
            return materialized_signal.create_from_spec(internal_spec)

    @abstractmethod
    def map_incoming_event(self, source_type: SignalSourceType, resource_path: str) -> Optional[SignalSourceAccessSpec]:
        ...

    @abstractmethod
    def map_internal_data_access_spec(self, data_access_spec: SignalSourceAccessSpec) -> SignalSourceAccessSpec:
        """Map an internal spec (which is agnostic from actual Platform::Storage impl) to
        a new concrete access spec that would be used as a template to create concrete resource paths.

        Might serve other framework entities such as BatchCompute where an internal resource path should be
        passed into another context (i.e Spark).

        Example:

        INTERNAL -> S3, Redshift, etc (via instantiating a new resource access spec)

        Parameters
        ----------
        data_access_spec: SignalSourceAccessSpec, Spec of type SignalSourceType::INTERNAL. If type is not INTERNAL, then the result is no-op.

        Returns
        -------
        SignalSourceAccessSpec, A new access spec if input spec is INTERNAL otherwise raises ValueError
        """
        ...

    @abstractmethod
    def map_internal_signal(self, signal: Signal) -> Signal:
        """Map an internal signal (which is agnostic from actual Platform::Storage impl) to
        a new concrete Signal based on the Storage impl.

        This basically means that the SignalType and SignalSourceAccessSpec will be mapped/changed accordingly.

        This functionality is important for collaboration between different Applications where
        an upstream Signal (of Internal type) should be mapped to an external Signal for its consumer.

        Returns
        -------
        Signal, A new (mapped) Signal if input is INTERNAL otherwise raises ValueError
        """
        ...

    @abstractmethod
    def deactivate(self) -> None:
        """Storage termination is handled in an exceptional way.
        Normal platform activation sequence does not attempt to terminate a Storage. For more details on why, please
        refer high-level (Platform, Application) termination and deletion related APIs.
        So in order to let Storage to be notified about a termination on the rest of the system (other Platform
        drivers), Platform calls this method on the Storage driver.
        So a Storage is expected to use this callback to revert whatever has been done in Storage::activate.
        """
        ...


class BatchCompute(BaseConstruct, ABC):
    """Abstraction for big-data compute component (one or more resources) for the runtime infrastructure of an application.

    Provides an interface to the `ProcessingUnit` to handle BATCH_COMPUTE typed workloads (`Slot`s)

    See implementations under `driver` module to have a better idea.
    """

    @classmethod
    @abstractmethod
    def driver_spec(cls) -> DimensionFilter:
        """
        Determines compute/slot routing into this driver when used within CompositeBatchCompute.

        Expected format for the filter:

        {
           <Lang> :{
             <ABI> : {
                 param1: {
                    type: DimensionType.STRING,
                     param1_value: {
                        type: DimensionType.STRING
                     }
                 ...
                paramN: {
                   type: DimensionType.STRING,
                    paramN_value: {
                       type: DimensionType.STRING
                    }
             }
        }

        if no parameters are needed by the driver, then '*' can be used for param key and value.

        :return: <DimensionFilter>
        """
        ...

    def provide_output_attributes(self, slot: Slot, user_attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """validates user attrs and then proceeds to interpret slot to provide default output attrs mandated by
        the compute operations in this driver (e.g output dataset attrs after batch compute runs / new partitions)."""
        ...

    @abstractmethod
    def compute(
        self,
        materialized_inputs: List[Signal],
        slot: Slot,
        materialized_output: Signal,
        execution_ctx_id: str,
        retry_session_desc: Optional[ComputeSessionDesc] = None,
    ) -> ComputeResponse:
        pass

    DEFAULT_MAX_WAIT_TIME_FOR_NEXT_RETRY_IN_SECS: ClassVar[int] = 60 * 60

    def get_max_wait_time_for_next_retry_in_secs(self) -> int:
        """Can be owerwritten to change the maximum interval used by the default retry strategy used by
          BatchCompute::can_try
        unless it is replaced with a different impl by a BatchCompute driver.
        """
        return self.DEFAULT_MAX_WAIT_TIME_FOR_NEXT_RETRY_IN_SECS

    def can_retry(self, active_compute_record: "RoutingTable.ComputeRecord") -> bool:
        """Check compute record and decide if it is ok for the underlying technology to accept a new request
        Default implementation uses a simple randomized retry strategy favoring least recent executions. Driver impls
        can implement more complex decision logic by overwriting this method.
        """
        from random import uniform

        elapsed_time = current_timestamp_in_utc() - active_compute_record.trigger_timestamp_utc
        max_wait_time = self.get_max_wait_time_for_next_retry_in_secs()
        wait_time = max_wait_time - elapsed_time
        if wait_time < (max_wait_time / 30):  # %3 deferral probability for the rest of the lifetime of a retryable compute
            wait_time = max_wait_time / 30  # keep it probabilistic even if elapsed time is > (MAX - 1) minutes
        return uniform(0, 1.0) > (wait_time / max_wait_time)

    @abstractmethod
    def get_session_state(
        self, session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
    ) -> ComputeSessionState:
        pass

    def terminate_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        """IF Orchestration tells the driver that it is totally done with this compute and
        will not even retry it or take any other action on it.

        BatchCompute drivers can use this for a guaranteed terminal sync point to do clean-ups, terminations, etc.
        """
        pass

    def kill_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        pass

    @abstractmethod
    def query_external_source_spec(
        self, ext_signal_source: SignalSourceAccessSpec
    ) -> Optional[Tuple[SignalSourceAccessSpec, "DimensionSpec"]]:
        """Tries to fetch the full description of input external signal source.

        This first helps to verify access from this BatchCompute impl to the target signal source and also load the
        complete/detailed access specification so as to facilitate high/application level external input configuration.

        Extended details for partition keys and primary keys are encapsulated by the returned access spec object, whereas
        the detailed (typed) description and again the order of the partition keys are to be found in the DimensionSpec object.

        Parameters
        ----------
        ext_signal_source: access spec with bare-bones params to enable querying

        Returns
        -------
        if target resource exists, then returns a pair of SignalSourceAccessSpec and DimensionSpec, otherwise None.
        Partition Keys are contained in both but DimensionSpec contains the type information. If there are no
        partition keys, the DimensionSpec is empty (newly instantiated with 'DimensionSpec()').
        """
        pass


class CompositeBatchCompute(BatchCompute):
    """Provides abstraction to the rest of the platform by encapsulating all of the underlying
    BatchCompute drivers and managing/routing computation requests"""

    BATCH_COMPUTE_DRIVERS_PARAM = "BATCH_COMPUTE_DRIVERS"

    def __init__(self, params: ConstructParamsDict) -> None:
        """
        Must be initiated with CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM defined with 'params'.
        """
        super().__init__(params)
        self._drivers_priority_list = self._load_drivers_list(params)
        # now instantiate the drivers!
        self._drivers = [driver_type(params) for driver_type in self._drivers_priority_list]

        if not all([isinstance(driver, BatchCompute) for driver in self._drivers]):
            raise ValueError(
                f"{self.__class__.__name__} must be instantiated with {self.BATCH_COMPUTE_DRIVERS_PARAM!r} "
                f"param defined properly as a list of types extending <BatchCompute>."
            )

        self._removed_drivers = []

    def _load_drivers_list(self, params: ConstructParamsDict) -> List[Type[BatchCompute]]:
        drivers_priority_list: List[Type[BatchCompute]] = params.get(self.BATCH_COMPUTE_DRIVERS_PARAM, None)
        if not drivers_priority_list:
            raise ValueError(
                f"{self.__class__.__name__} can be instantiated with at least one BatchCompute driver!"
                f" Please define {self.BATCH_COMPUTE_DRIVERS_PARAM!r} properly as a list of types extending <BatchCompute>."
            )

        if len(set(drivers_priority_list)) != len(drivers_priority_list):
            raise ValueError(
                f"All of the driver types in {self.BATCH_COMPUTE_DRIVERS_PARAM} must be unique while"
                f" instantiating {self.__class__.__name__}."
            )

        return drivers_priority_list

    @classmethod
    def driver_spec(cls) -> DimensionFilter:
        # it is actually possible to create hierarchy of CompositeBatchCompute drivers along with explicit
        # driver impls on the same level. But using this type directly cannot allowed since we would need
        # a driver_spec representing the composite compute support of all of the drivers hiden behind a custom
        # composite batch compute impl. Otherwise, compute routing in 'compute' and 'get_session_state' cannot work.
        raise TypeError(
            f"{cls.__name__} cannot be listed within {cls.BATCH_COMPUTE_DRIVERS_PARAM!r} explicitly! "
            f"Consider extending it if you are willing to create your own composite BatchCompute driver."
        )

    def provide_output_attributes(self, slot: Slot, user_attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        driver: BatchCompute = self._find_batch_compute(slot)
        if driver is None:
            raise ValueError(
                f"Cannot find a BatchCompute compatible with slot {slot!r}! One or more parameters in compute/slot definition might be missing. "
                f"Check the following BatchCompute specs: {[repr(bc.driver_spec().pretty()) for bc in self._drivers]}."
            )
        return driver.provide_output_attributes(slot, user_attrs)

    def _find_batch_compute(self, slot: Slot) -> Optional[BatchCompute]:
        if slot.type.is_batch_compute():
            if slot.extra_params:
                params_sub_filter = {
                    str(key): {
                        type: DimensionType.STRING,
                        # we have to comply with driver spec filter structure (if value is not string just mark it as '*')
                        # driver_spec check stops at rool level parameters and their string values (if any)
                        str(value) if isinstance(value, str) else "*": {},
                    }
                    for key, value in slot.extra_params.items()
                }
            else:  # if there are no extra parameter then first two dimensions LANG and ABI will be the determinants.
                params_sub_filter = {"*": {type: DimensionType.STRING, "*": {type: DimensionType.STRING}}}

            slot_filter = DimensionFilter.load_raw(
                {int(slot.code_lang.value): {int(slot.code_abi.value): {key: value for key, value in params_sub_filter.items()}}}
            )

            match: Tuple[DimensionFilter, BatchCompute] = None
            for batch_compute in self._drivers:
                filtered_spec = batch_compute.driver_spec().chain(slot_filter)
                if filtered_spec:
                    # max-algo to find the match with max spec params.
                    # dont check spec params if extra_params for slot is not defined
                    # (when filtering is only based Lang + ABI)
                    if not match or (
                        slot.extra_params and match[0].get_total_dimension_count() < filtered_spec.get_total_dimension_count()
                    ):
                        match = (filtered_spec, batch_compute)

            return match[1] if match else None

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        """Reloaded during the initialization of an existing (previously activated) application for development"""
        super()._deserialized_init(params)
        # in between different development sessions, the drivers list might be changed by the user
        current_drivers_list = self._drivers_priority_list
        self._drivers_priority_list = self._load_drivers_list(params)
        self._removed_drivers = []
        if current_drivers_list != self._drivers_priority_list:
            logger.critical(
                f"New {self.BATCH_COMPUTE_DRIVERS_PARAM!r} list is different than its previously activated"
                f" version. If the newly activated drivers cannot support ongoing executions and leave"
                f" them orphaned, their result might be reported as failures."
            )

            new_drivers = []
            for driver_type in self._drivers_priority_list:
                if driver_type in set(self._drivers_priority_list) - set(current_drivers_list):
                    # new driver (use the constructor to initiate the driver)
                    new_drivers.append(driver_type(params))
                else:
                    # existing driver
                    for driver in self._drivers:
                        if type(driver) == driver_type:
                            # kept by the user (driver instance is deserialized, just let it know about it)
                            driver._deserialized_init(params)
                            new_drivers.append(driver)
                            break

            for driver_type in set(current_drivers_list) - set(self._drivers_priority_list):
                for driver in self._drivers:
                    if type(driver) == driver_type:
                        # still need to do this to keep the sane operation of the driver till the 'terminate' call
                        driver._deserialized_init(params)
                        self._removed_drivers.append(driver)

            self._drivers = new_drivers

            if not all([isinstance(driver, BatchCompute) for driver in self._drivers]):
                raise ValueError(
                    f"{self.__class__.__name__} must be instantiated with {self.BATCH_COMPUTE_DRIVERS_PARAM!r} "
                    f"param defined properly as a list of types extending <BatchCompute>."
                )
        else:
            for driver in self._drivers:
                driver._deserialized_init(params)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        super()._serializable_copy_init(org_instance)
        self._drivers = [driver.serializable_copy() for driver in self._drivers]
        # unique to this abstract driver impl:
        # -  reset the entire _params dict since in this generic scope we cannot eliminate serialization unfriendly fields.
        # -  this abstract impl cannot assume anything about the platform (AWS, etc).
        # to be restored in runtime_init as well (_deserialized_init takes care of complete param restoration already).
        self._params = None

    def update_serialized_cross_refs(self, original_to_serialized_map: Dict["BaseConstruct", "BaseConstruct"]):
        super().update_serialized_cross_refs(original_to_serialized_map)
        for driver in self._drivers:
            driver.update_serialized_cross_refs(original_to_serialized_map)

    def compute(
        self,
        materialized_inputs: List[Signal],
        slot: Slot,
        materialized_output: Signal,
        execution_ctx_id: str,
        retry_session_desc: Optional[ComputeSessionDesc] = None,
    ) -> ComputeResponse:
        driver: BatchCompute = self._find_batch_compute(slot)
        if not driver:
            raise ValueError(
                f"Slot is not supported by any of the BatchCompute drivers! "
                f"Slot: {slot!r},"
                f"Active BatchCompute drivers: {self._drivers_priority_list!r}"
            )
        return driver.compute(materialized_inputs, slot, materialized_output, execution_ctx_id, retry_session_desc)

    def can_retry(self, active_compute_record: "RoutingTable.ComputeRecord") -> bool:
        driver: BatchCompute = self._get_driver(active_compute_record.slot, active_compute_record.state.resource_desc.driver_type)
        return driver.can_retry(active_compute_record)

    def _get_driver(self, slot: Slot, driver_type: Optional[BatchCompute] = None) -> BatchCompute:
        if driver_type:
            driver_index: int = self._drivers_priority_list.index(driver_type)
            return self._drivers[driver_index]

        driver = self._find_batch_compute(slot)
        # Backwards compatibility: default to 0 when driver_type is not defined in an existing compute resource.
        # TODO remove defaulting to 0 when all of the apps are activated with batch_compute_overhaul
        return driver if driver else self._drivers[0]

    def get_session_state(
        self, session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
    ) -> ComputeSessionState:
        driver_type: Optional[BatchCompute] = session_desc.resource_desc.driver_type
        driver: BatchCompute = self._get_driver(active_compute_record.slot, driver_type)
        return driver.get_session_state(session_desc, active_compute_record)

    def terminate_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        driver: BatchCompute = self._get_driver(active_compute_record.slot, active_compute_record.state.resource_desc.driver_type)
        driver.terminate_session(active_compute_record)

    def kill_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        driver: BatchCompute = self._get_driver(active_compute_record.slot, active_compute_record.state.resource_desc.driver_type)
        driver.kill_session(active_compute_record)

    def query_external_source_spec(
        self, ext_signal_source: SignalSourceAccessSpec
    ) -> Optional[Tuple[SignalSourceAccessSpec, "DimensionSpec"]]:
        for driver in self._drivers:
            result = driver.query_external_source_spec(ext_signal_source)
            if result:
                return result
        return None

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)
        for driver in self._drivers + self._removed_drivers:
            driver.dev_init(platform)

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        # not critical, clone runtime params from Processor
        # for future extensions in this driver, which would need to rely on params.
        self._params = dict(platform.storage._params)
        super().runtime_init(platform, context_owner)
        for driver in self._drivers:
            driver.runtime_init(platform, context_owner)

    def provide_runtime_trusted_entities(self) -> List[str]:
        entities = []
        for driver in self._drivers:
            entities.extend(driver.provide_runtime_trusted_entities())
        return entities

    def provide_runtime_default_policies(self) -> List[str]:
        policies = []
        for driver in self._drivers:
            policies.extend(driver.provide_runtime_default_policies())
        return policies

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        permissions = []
        for driver in self._drivers:
            permissions.extend(driver.provide_runtime_permissions())
        return permissions

    @classmethod
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        # no need to validate the list here, will go through the validation eventually in __init__
        permissions = []
        drivers_priority_list: List[Type[BatchCompute]] = params.get(cls.BATCH_COMPUTE_DRIVERS_PARAM, None)
        if drivers_priority_list:
            for driver_type in drivers_priority_list:
                permissions.extend(driver_type.provide_devtime_permissions(params))
        return permissions

    def _provide_system_metrics(self) -> List[Signal]:
        metrics = []
        for driver in self._drivers:
            metrics.extend(driver._provide_system_metrics())
        return metrics

    def _provide_internal_metrics(self) -> List[ConstructInternalMetricDesc]:
        metrics = []
        for driver in self._drivers:
            metrics.extend(driver._provide_internal_metrics())
        return metrics

    def _provide_route_metrics(self, route: Route) -> List[ConstructInternalMetricDesc]:
        metrics = []
        for driver in self._drivers:
            metrics.extend(driver._provide_route_metrics(route))
        return metrics

    def _provide_internal_alarms(self) -> List[Signal]:
        alarms = []
        for driver in self._drivers:
            alarms.extend(driver._provide_internal_alarms())
        return alarms

    def activate(self) -> None:
        for driver in self._drivers:
            driver.activate()
        self._terminate_removed_drivers()
        super().activate()

    def _terminate_removed_drivers(self) -> None:
        """If this development session has started with a different drivers list, then terminate the removed drivers.
        See how '_removed_drivers' are detected in '_deserialized_init'.
        """
        if self._removed_drivers:
            logger.critical(f"Terminating removed BatchCompute drivers {[type(driver) for driver in self._removed_drivers]!r} ...")
            for driver in self._removed_drivers:
                driver.terminate()
            logger.critical("Termination for removed BatchCompute drivers done!")
            self._removed_drivers = []

    def _update_bootstrapper(self, bootstrapper: "RuntimePlatform") -> None:
        for driver in self._drivers:
            driver._update_bootstrapper(bootstrapper)

    def rollback(self) -> None:
        for driver in self._drivers:
            driver.rollback()
        super().rollback()

    def terminate(self) -> None:
        for driver in self._drivers:
            driver.terminate()
        self._terminate_removed_drivers()
        super().terminate()

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)
        for driver in self._drivers:
            driver.check_update(prev_construct)

    def process_signals(self) -> None:
        super().process_signals()
        for driver in self._drivers:
            driver.process_signals()

    def process_connections(self) -> None:
        super().process_connections()
        for driver in self._drivers:
            driver.process_connections()

    def process_security_conf(self) -> None:
        super().process_security_conf()
        for driver in self._drivers:
            driver.process_security_conf()

    def process_downstream_connection(self, downstream_platform: "DevelopmentPlatform") -> UpstreamGrants:
        conf_grants: UpstreamGrants = super().process_downstream_connection(downstream_platform)
        for driver in self._drivers:
            conf_grants << driver.process_downstream_connection(downstream_platform)
        return conf_grants

    def terminate_downstream_connection(self, downstream_platform: "DevelopmentPlatform") -> UpstreamRevokes:
        conf_revokes: UpstreamRevokes = super().terminate_downstream_connection(downstream_platform)
        for driver in self._drivers:
            conf_revokes << driver.terminate_downstream_connection(downstream_platform)
        return conf_revokes

    def hook_external(self, signals: List[Signal]) -> None:
        super().hook_external(signals)
        # In multi BatchCompute conf, external signals are hooked by drivers based on Route/Node analysis.
        # Please see 'hook_internal' callback.
        # for driver in self._drivers:
        #    driver.hook_external(signals)

    def hook_internal(self, route: "Route") -> None:
        super().hook_internal(route)
        # Analyze route and find respective drivers for each slot.
        # Then based on slot-driver pairs, call;
        #  - hook_external
        #  - hook_internal
        drivers = set()
        for slot in route.slots:
            if slot.type.is_batch_compute():
                winning_driver = self._find_batch_compute(slot)
                if not winning_driver:
                    raise ValueError(
                        f"Slot for route {route.route_id!r} not supported by any of the BatchCompute drivers!\n"
                        f" Driver list: {self._drivers_priority_list!r},\n"
                        f" Slot: {slot!r}"
                    )
                drivers.add(winning_driver)
                # reset the alias' as we don't want same signal from different routes to be treated/persisted differently in drivers.
                external_signals = [
                    signal.clone(None) for signal in route.link_node.signals if signal.resource_access_spec.source.is_external()
                ]
                internal_signals = [
                    signal.clone(None) for signal in route.link_node.signals if signal.resource_access_spec.source.is_internal_signal()
                ]
                if external_signals:
                    winning_driver.hook_external(external_signals)
                if internal_signals:
                    for internal_signal in internal_signals:
                        winning_driver.hook_internal_signal(internal_signal)

        for winning_driver in drivers:
            winning_driver.hook_internal(route)

    def hook_internal_signal(self, signal: "Signal") -> None:
        super().hook_internal_signal(signal)
        # In multi BatchCompute conf, internal signals are hooked by drivers based on Route/Node analysis.
        # Please see 'hook_internal' callback.
        # for driver in self._drivers:
        #    driver.hook_internal_signal(signal)

    def connect_construct(self, construct_resource_type: str, construct_resource_path: str, other: "BaseConstruct") -> None:
        super().connect_construct(construct_resource_type, construct_resource_path, other)
        for driver in self._drivers:
            driver.connect_construct(construct_resource_type, construct_resource_path, other)

    def hook_security_conf(
        self, security_conf: ConstructSecurityConf, platform_security_conf: Dict[Type["BaseConstruct"], ConstructSecurityConf]
    ) -> None:
        super().hook_security_conf(security_conf, platform_security_conf)
        for driver in self._drivers:
            driver.hook_security_conf(security_conf, platform_security_conf)

    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        pass

    def _process_internal_signals(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_construct_connections(
        self, new_construct_conns: Set["_PendingConnRequest"], current_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _process_security_conf(self, new_security_conf: ConstructSecurityConf, current_security_conf: ConstructSecurityConf) -> None:
        pass

    def _revert_external(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        pass

    def _revert_internal(self, routes: Set[Route], prev_routes: Set[Route]) -> None:
        pass

    def _revert_internal_signals(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        pass

    def _revert_construct_connections(
        self, construct_conns: Set["_PendingConnRequest"], prev_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _revert_security_conf(self, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        pass

    def activation_completed(self) -> None:
        super().activation_completed()
        for driver in self._drivers:
            driver.activation_completed()


class Diagnostics(BaseConstruct, ABC):
    """Abstraction for system wide diagnostics signal (i.e Metric, Alarm, Logs) management, runtime retrieval
    and emission.

    Provides an interface to the other Platform drivers to retrieve or generate diagnostics data.

    See implementations under `driver` module to have a better idea.

    Design:
        TODO refer alarming&metrics design doc
    """

    class MetricAction(CoreData, SignalProvider):
        def __init__(self, signal: Signal, diagnostics: "Diagnostics", filtered_values: List[Any] = []) -> None:
            self.signal = signal
            self.diagnostics = diagnostics
            self.filtered_values = filtered_values

        def emit(
            self, value: Union[float, MetricStatisticData, List[MetricValueCountPairData]], timestamp: Optional[datetime] = None
        ) -> None:
            """Implement
            TODO refer alarming&metrics design doc
            """
            self.diagnostics.emit(self, value, timestamp)

        # overrides
        def get_signal(self, alias: str = None) -> Signal:
            if self.filtered_values:
                new_filter = DimensionFilter.load_raw(self.filtered_values, cast=self.signal.domain_spec.dimension_spec)
                if new_filter is None:  # type mismatch with dim spec
                    raise ValueError(
                        f"Filtering on metric {self.signal.alias!r} using values {self.filtered_values!r} "
                        f"is not type compatible with its dim spec:\n: {self.signal.domain_spec.dimension_spec!r}"
                    )

                def _is_ready(self, new_filter):
                    return not self.filtered_values or self.signal.domain_spec.dimension_filter_spec.check_spec_match(new_filter)

                # check if filter is ready
                if not _is_ready(self, new_filter):
                    new_filter = self.signal.resource_access_spec.auto_complete_filter(
                        self.filtered_values, cast=self.signal.domain_spec.dimension_spec
                    )

                    if new_filter is None or not _is_ready(self, new_filter):
                        raise ValueError(
                            f"Filtering on metric {self.signal.alias!r} is not compatible with its filter spec:"
                            f"\n: {self.signal.domain_spec.dimension_filter_spec.pretty()!r}"
                        )

                new_filter = self.signal.domain_spec.dimension_filter_spec.chain(new_filter)
                if self.signal.domain_spec.dimension_filter_spec and not new_filter:
                    raise ValueError(
                        f"New filter ({self.filtered_values!r}) on new metric ({self.signal.alias!r}) yields empty"
                        f" result against source signal's filter:\n "
                        f" {self.signal.domain_spec.dimension_filter_spec.pretty()!r}"
                    )

                return self.signal.filter(new_filter, alias)
            else:
                return self.signal.clone(self.signal.alias if not alias else alias)

        def __getitem__(self, slice_filter: Any) -> "Diagnostics.MetricAction":
            expected_dimensions = [MetricDimension.NAME.value, MetricDimension.STATISTIC.value, MetricDimension.PERIOD.value]
            if len(self.filtered_values) >= len(expected_dimensions):
                raise ValueError(
                    f"Redundant filtering for metric ({self.signal.alias!r}) performed on top of "
                    f"{expected_dimensions!r} = [{self.filtered_values!r}]!"
                )

            if isinstance(slice_filter, slice):
                raise ValueError(
                    f"Dimension value {slice_filter!r} provided for metric {self.signal.alias!r} should "
                    f"be a material value of type STRING, LONG or DATETIME."
                )
            return Diagnostics.MetricAction(self.signal, self.diagnostics, self.filtered_values + [slice_filter])

    class _NOOPMetricAction(MetricAction):
        """NOOP MetricAction used internally by drivers to skip emission when internal metric ID is not found.
        In a normal activation sequence, internal (orchestration, routing) metrics from drivers are normally pushed to
        Diagnostics as internal signals for runtime emission. But in isolated driver tests for example, driver's own
        internal metrics won't exist in Diagnostics and cause 'emit' to raise. Other strategy would be to change
        driver tests to feed its internal metrics to Diagnostics. But it is less scalable and also not enforcing
        metric ID compatibility/existence (between declaration and actual emission) for drivers is OK since they are not
         managed by users.
        """

        def __init__(self) -> None:
            super().__init__(None, None, None)

        def emit(
            self, value: Union[float, MetricStatisticData, List[MetricValueCountPairData]], timestamp: Optional[datetime] = None
        ) -> None:
            pass

        def __getitem__(self, slice_filter: Any) -> "Diagnostics.MetricAction":
            return self

    @classmethod
    def check_metric_materialization(cls, metric_signal: Signal, for_emission=False) -> None:
        """Method to check if the metric can be used in alarming or emission.
        It should be materialized (name, statistic and period should all be material) and also it should map to only
        one metric entity. Its DimensionFilter should contain one branch of material name, statistic and period values.
        """
        if not for_emission:
            if len(metric_signal.get_materialized_resource_paths()) > 1:
                import json

                filter_spec_dump = json.dumps(
                    {
                        "dimensions": [
                            (dim_name, dim.type, dim.params)
                            for dim_name, dim in metric_signal.domain_spec.dimension_spec.get_flattened_dimension_map().items()
                        ],
                        "current_filter_values": metric_signal.domain_spec.dimension_filter_spec.pretty(),
                    },
                    indent=11,
                )
                raise ValueError(
                    f"Metric signal ({repr(metric_signal.alias)}) is not materialized! Its dimension filter spec "
                    f"maps to multiple resources. The following filter spec tree should have only one branch:\n "
                    f" {filter_spec_dump}"
                )

            material_dimensions = [MetricDimension.NAME, MetricDimension.STATISTIC, MetricDimension.PERIOD]
        else:
            # for emission, first materialized resource is picked. so we just need to check the name of the first
            # branch here.
            material_dimensions = [MetricDimension.NAME]

        for metric_dim in material_dimensions:
            metric_variant: "DimensionVariant" = metric_signal.domain_spec.dimension_filter_spec.find_dimension_by_name(metric_dim.value)
            if not metric_variant.is_material_value():
                raise ValueError(
                    f"Metric signal ({metric_signal.alias!r}) {metric_dim.value} dimension value "
                    f" {metric_variant.value!r} is not material! Either change metric dimesion filter"
                    f" spec to have its first (top level) dimension 'NAME' to have at least one"
                    f" a concrete string value or specify the name dimension during the emit call"
                    f" (e.g runtime_plaform.diagnostics[{metric_signal.alias!r}]['<METRIC_NAME>'].emit(...)"
                )

    def get_internal_metric(
        self, alias: str, sub_dimensions: Optional[MetricSubDimensionMapType] = None
    ) -> Optional["Diagnostics.MetricAction"]:
        """Refer the following section from the design doc for 'runtime utilization':
             TODO refer alarmign and metrics doc

        First metric that satisfy the following two conditions is returned:
            - its alias matches the 'alias'
            - if 'sub_dimensions' is not None, its sub_dimensions must be equal to or a superset of 'sub_dimensions'
        """
        # (1) normally we would only use _processed_internal_signals (post-activation cache).
        # but RheocerOS allows metric emission test even before activation (during testing),before "pending" signals
        # become "processed".
        # TODO the other reason to add pending version of signals is the fact that update_bootstrapper call in
        # activation happens before 'activation_completed' (where pending -> processed transition happens).
        # So at runtime, bootstapper contains the serialized version of drivers that still contain processed signals
        # in 'pending' buffer/storage.
        for internal_signal_store in [
            self._pending_internal_signals,  # prioritize dev-time / new metrics
            self._processed_internal_signals,
        ]:  #
            for signal in internal_signal_store:
                if (
                    signal.resource_access_spec.source in [SignalSourceType.INTERNAL_METRIC]
                    and signal.alias == alias
                    and (
                        sub_dimensions is None
                        or all(signal.resource_access_spec.sub_dimensions.get(key, None) == value for key, value in sub_dimensions.items())
                    )
                ):
                    return Diagnostics.MetricAction(signal, self)
        return None

    def __getitem__(self, metric_id_with_or_without_subdimensions) -> Optional["Diagnostics.MetricAction"]:
        if isinstance(metric_id_with_or_without_subdimensions, str):
            metric_id = metric_id_with_or_without_subdimensions
            sub_dimensions = None
        elif isinstance(metric_id_with_or_without_subdimensions, slice):
            metric_id = metric_id_with_or_without_subdimensions.start
            sub_dimensions = metric_id_with_or_without_subdimensions.stop
        else:
            return None

        return self.get_internal_metric(metric_id, sub_dimensions)

    def emit(
        self,
        metric_signal_or_provider: Union[Signal, SignalProvider],
        value: Union[float, MetricStatisticData, List[MetricValueCountPairData]],
        timestamp: Optional[datetime] = None,
    ) -> None:
        metric_signal = (
            metric_signal_or_provider
            if isinstance(metric_signal_or_provider, Signal)
            else cast("SignalProvider", metric_signal_or_provider).get_signal()
        )
        if not metric_signal.type.is_metric():
            raise ValueError(
                f"Input signal (id: {metric_signal.alias!r}, source_type: {metric_signal.resource_access_spec.source!r}) "
                f"to Diagnostics::emit is not a metric!"
            )
        if metric_signal.resource_access_spec.source not in [SignalSourceType.INTERNAL_METRIC]:
            #  Do not enforce the emission on internal metrics only? (user or driver created metrics of type INTERNAL_METRIC)
            #  We are not enforcing it so that we can even emit for external metrics for testability (mainly integ-test
            #  automation).
            logger.critical(
                f"Input signal {metric_signal.alias!r} provided to emit has type {metric_signal.resource_access_spec.source!r}! "
                f"RheocerOS recommends emission on internal metrics created by the user to avoid interference with system "
                f"emitted metrics or with directly imported external metrics."
            )
        # currently we expect the driver to pick the first branch if signal maps to multiple resurces.
        # and apply 'alias' as the default name (if left as 'any') during emission.
        # the reason we keep this defaulting and behaviour here (not in MetricAction:get_signal for example) is that
        # they are specific to emission.
        self.check_metric_materialization(metric_signal, for_emission=True)
        self._do_emit(metric_signal, value, timestamp)

    @abstractmethod
    def _do_emit(
        self,
        metric_signal: Signal,
        value: Union[float, MetricStatisticData, List[Union[float, MetricValueCountPairData]]],
        timestamp: Optional[datetime] = None,
    ) -> None:
        ...

    @abstractmethod
    def get_event_channel_type(self) -> str:
        ...

    @abstractmethod
    def get_event_channel_resource_path(self) -> str:
        ...

    @abstractmethod
    def subscribe_downstream_resource(self, downstream_platform: "DevelopmentPlatform", resource_type: str, resource_path: str):
        ...

    @abstractmethod
    def unsubscribe_downstream_resource(self, downstream_platform: "DevelopmentPlatform", resource_type: str, resource_path: str):
        ...

    # overrides
    def _process_internal_signals(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        supported_alarm_types = [SignalSourceType.INTERNAL_ALARM, SignalSourceType.INTERNAL_COMPOSITE_ALARM]
        new_internal_alarms = {s for s in new_signals if s.resource_access_spec.source in supported_alarm_types}
        current_internal_alarms = {s for s in current_signals if s.resource_access_spec.source in supported_alarm_types}

        processed_resources: Set[str] = {s.resource_access_spec.path_format for s in current_internal_alarms}
        new_processed_resources: Set[str] = {s.resource_access_spec.path_format for s in new_internal_alarms}
        resources_to_be_deleted = processed_resources - new_processed_resources
        self._process_internal_alarms(new_internal_alarms, current_internal_alarms, resources_to_be_deleted)

    @abstractmethod
    def _process_internal_alarms(
        self, new_alarms: Set[Signal], current_alarms: Set[Signal], resource_paths_to_be_deleted: Set[str]
    ) -> None:
        ...

    # overrides
    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        """Register this diagnostics driver's event-channel to 'raw' (non-upstream, not IF managed) external alarms.
        Skips upstream external alarms. Processor should be able to register them directly. We are avoiding an extra
        hop in connection from upstream down to the Processor. And that direct connection is guaranteed to work.

        Normally Processor can register to both but:
            - Processor registration to an alarm is not guaranteed (ex: Lambda registration for CW composite alarms)
            - And there is too much alarming tech related details involved for which Diagnostics impl would be the right
        place for encapsulation.

        So for external (non IF-managed) Alarms, Diagnostics driver is taking the responsibility.
        """
        supported_external_alarm_sources = [SignalSourceType.CW_ALARM, SignalSourceType.CW_COMPOSITE_ALARM]
        # eliminate upstream signals and check if the external signal is an alarm.
        is_external_raw_alarm = (
            lambda s: s.resource_access_spec.is_mapped_from_upstream() and s.resource_access_spec.source in supported_external_alarm_sources
        )
        self._process_raw_external_alarms(
            {s for s in new_signals if is_external_raw_alarm(s)}, {s for s in current_signals if is_external_raw_alarm(s)}
        )

    @abstractmethod
    def _process_raw_external_alarms(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        ...

    @abstractmethod
    def is_internal(self, source_type: SignalSourceType, resource_path: str) -> bool:
        ...

    def map_materialized_signal(self, materialized_signal) -> Optional[Signal]:
        """Converts a materialized signal to its internal representation if it is an Internal signal actually.

        Returns None if the external, materialized signal does not belong to this driver (not Internal)."""
        internal_spec = self.map_external_access_spec(materialized_signal.resource_access_spec)
        if internal_spec:
            return materialized_signal.create_from_spec(internal_spec)

    @abstractmethod
    def map_incoming_event(self, source_type: SignalSourceType, resource_path: str) -> Optional[SignalSourceAccessSpec]:
        ...

    @abstractmethod
    def map_external_access_spec(self, access_spec: Optional[SignalSourceAccessSpec] = None) -> Optional[SignalSourceAccessSpec]:
        """Map an external diagnostic signal to its internal version (if it is actually internal and governed by
        this driver)."""
        ...

    @abstractmethod
    def map_internal_access_spec(self, data_access_spec: SignalSourceAccessSpec) -> SignalSourceAccessSpec:
        """Map an internal diagnostic signal to its external/raw version"""
        ...

    @abstractmethod
    def map_internal_signal(self, signal: Signal) -> Signal:
        """Map an internal signal (which is agnostic from actual Platform::Diagnostics impl) to
        a new concrete Signal based on the Diagnostics impl.

        This basically means that the SignalType and SignalSourceAccessSpec will be mapped/changed accordingly.

        This functionality is important for collaboration between different Applications where
        an upstream Signal (of Internal type) should be mapped to an external Signal for its consumer.

        Returns
        -------
        Signal, A new (mapped) Signal if input is INTERNAL otherwise raises ValueError
        """
        ...

    @abstractmethod
    def get_unique_internal_alarm_name(self, alarm_name: str) -> str:
        ...


FEEDBACK_SIGNAL_TYPE_EVENT_KEY: str = "if_internal_feedback_signal"
FEEDBACK_SIGNAL_PROCESSING_MODE_KEY: str = "if_internal_feedback_signal_process_mode"


class FeedBackSignalProcessingMode(str, Enum):
    FULL_DOMAIN = "FULL_DOMAIN"
    ONLY_HEAD = "ONLY_HEAD"


ProcessorEvent = Dict[str, Any]


class QueuedProcessorEvent(CoreData):
    """Envelope that carries internal Queueing related metadata along with the actual event.
    Ex: SQS Message data
    """

    # TODO use this in 'receive' and 'delete' APIs
    def __init__(self, event: ProcessorEvent, queue_metadata: Any) -> None:
        self.event = event
        self.queue_metadata = queue_metadata


class ProcessorQueue(BaseConstruct, ABC):
    """Abstraction for the event / instruction queue for the runtime infrastructure of the entire application.

    A Processor (as an event dispatcher loop) should be able to 'process' events from this queue in each cycle.
    So for a platform configuration, events compatible with its ProcessingUnit can be queued here (with the same format).

    This queue can also be regarded as the main entry point for an application. Any kind of internal/external (manual or automatic)
    trigger can first go through this queue. But this notion depends on internal provisioning scheme of ProcessingUnit.
    Processor can by-pass queue and configure direct notification channels onto itself (ex: SNS -> Lambda, etc).

    See implementations under `driver` module to have a better idea.
    """

    @classmethod
    def create_event(cls, signal_or_event: Union[Signal, ProcessorEvent]) -> ProcessorEvent:
        return signal_or_event if not isinstance(signal_or_event, Signal) else {FEEDBACK_SIGNAL_TYPE_EVENT_KEY: signal_or_event.serialize()}

    # TODO change name to avoid confusion with our Westeros framework where Processor concept means something totally different,
    # and causing confusion here.
    @abstractmethod
    def send(
        self,
        events: Union[
            Sequence[ProcessorEvent],
            ProcessorEvent,
            # support Signal for convenience
            Sequence[Signal],
            Signal,
        ],
    ) -> None:
        pass

    # TODO return Sequence[QueuedProcessorEvent]
    @abstractmethod
    def receive(self, visibility_timeout_in_seconds: int, max_count: int = None) -> Sequence[ProcessorEvent]:
        pass

    # TODO expect Sequence[QueuedProcessorEvent]
    @abstractmethod
    def delete(self, events: Sequence[ProcessorEvent]) -> None:
        pass


SIGNAL_TARGET_ROUTE_ID_KEY: str = "if_signal_target_route_id"


class ProcessingUnit(BaseConstruct, ABC):
    """Main loop for an application that continuously;

    - Fetches new Signals from ProcessingQueue;
    - Redirects them into the `RoutingTable`,
    - Calls `BatchCompute` for completed `Route`s, or executes their `Slot`s if their type is Inlined (no async big-data workload).

    Alternatively;
    - Receives external/internal events (push from complete batch-compute jobs or external resources),
    - Converts them into `Signal`s and puts them into the ProcessingQueue.

    See implementations under `driver` module to have a better idea.
    """

    @abstractmethod
    def process(
        self,
        signal_or_event: Union[Signal, Dict[str, Any]],
        use_activated_instance=False,
        processing_mode=FeedBackSignalProcessingMode.ONLY_HEAD,
        target_route_id: Optional[RouteID] = None,
        is_async=True,
    ) -> Optional[List["RoutingTable.Response"]]:
        """Injects a new signal or raw event into the system.

        Signal or event is either processed locally or remotely in the activated resource (depending on the driver).

        :param signal_or_event: A feedback signal or a high-level application signal or a raw event supported by the
        underlying Processor driver.
        :param use_activated_instance: if it is True, this API makes a remote call to the activated instances/resource
        created by the underlying Processor impl (sub-class). If it is False (default), this call is handled within the
        same Python process domain (which is the preferred case for unit-tests and more commonly for local debugging).
        :param processing_mode: If the input signal represents multiple resources (e.g dataset with a range of partitions),
        then this flag represents which one to use as the actual event/impetus into the system. ONLY_HEAD makes the tip
        of the range to be used (e.g most recent partition on a 'date' range). FULL RANGE causes a Signal explosion and
        multiple Processor cycles implicitly, where each atomic resource (e.g data partition) is treated as a separate
        Signal.
        :param target_route_id: When the signal is injected and being checked against multiple routes, this parameter
        limits the execution to a specific one only, otherwise (by default) the system uses any incoming signal/event
        in multicast mode against all of the existing Routes. For example, if an incoming event can trigger multiple
        routes and you want to limit this behaviour for more deterministic tests, etc then use this parameter to
        specify a route and limit the execution scope.
        :param is_async: when 'use_activate_instance' is True, then this parameter can be used to control whether
        the remote call will be async or not.
        """
        ...

    @property
    def concurrency_limit(self) -> Optional[int]:
        """Get the concurrent execution (signal processing) limit used by this Processor
        based on current Platform conf.

        Returns:
             Optional[int]: determines the max concurrent threads/containers/exec contexts that this Processor
             will use at runtime. 'None' value means, the driver relies on the underlying tech to manage it with
             no specific limit (e.g default AWS Lambda concurrency limit -> unreserved account limit).
        """
        # default impl relies on routing_table's synchronization support.
        # if routing_table does not provide it, then we have to set concurrency as 1.
        if self.get_platform().routing_table.is_synchronized:
            return None

        return 1

    def pause(self) -> None:
        """Implementation should pause signal/event ingestion in the background.
        Direct/synchronous invocations to 'process' should still be allowed.
        """
        self._is_paused = True

    def resume(self) -> None:
        """Re-enable asynchronous signal/event ingestion/processing in the background."""
        self._is_paused = False

    def is_paused(self) -> bool:
        try:
            return self._is_paused
        except AttributeError:
            return False


class RoutingActivationStrategy(str, Enum):
    ALWAYS_CLEAR_ACTIVE_ROUTES = "always_clear_active_routes"
    CLEAR_MODIFIED_ACTIVE_ROUTES_ONLY = "clear_modified_active_routes_only"


class RoutingComputeInterface:
    # TODO Python 3.7 pickling causes "TypeError: can't pickle _abc_data objects" for interfaces extending 'ABC'
    #      Probably the issue is related to inner ABC pickling.
    # class IInlinedCompute(ABC):
    class IInlinedCompute:
        @abstractmethod
        def __call__(self, input_map: Dict[str, Signal], materialized_output: Signal, params: ConstructParamsDict) -> Any:
            ...


class RoutingHookInterface:
    # this param is guaranteed to be passed to each hook as part of param dictionary. value would be equivalent to
    # the callback type (execution_begin, etc). this helps common callback code to react differently against different
    # hooks.
    HOOK_TYPE_PARAM: ClassVar[str] = "HOOK_TYPE_PARAM"

    class IHook:
        @abstractmethod
        def __call__(self, routing_table: "RoutingTable", route_record: "RoutingTable.RouteRecord", *args, **params) -> None:
            ...

    class Execution:
        class IExecutionBeginHook:
            @abstractmethod
            def __call__(
                self,
                routing_table: "RoutingTable",
                route_record: "RoutingTable.RouteRecord",
                execution_context: "Route.ExecutionContext",
                current_timestamp_in_utc: int,
                **params,
            ) -> None:
                ...

        class IExecutionSkippedHook:
            @abstractmethod
            def __call__(
                self,
                routing_table: "RoutingTable",
                route_record: "RoutingTable.RouteRecord",
                execution_context: "Route.ExecutionContext",
                current_timestamp_in_utc: int,
                **params,
            ) -> None:
                ...

        class IComputeSuccessHook:
            @abstractmethod
            def __call__(
                self,
                routing_table: "RoutingTable",
                route_record: "RoutingTable.RouteRecord",
                compute_record: "RoutingTable.ComputeRecord",
                current_timestamp_in_utc: int,
                **params,
            ) -> None:
                ...

        class IComputeFailureHook:
            @abstractmethod
            def __call__(
                self,
                routing_table: "RoutingTable",
                route_record: "RoutingTable.RouteRecord",
                compute_record: "RoutingTable.ComputeRecord",
                current_timestamp_in_utc: int,
                **params,
            ) -> None:
                ...

        class IComputeRetryHook:
            @abstractmethod
            def __call__(
                self,
                routing_table: "RoutingTable",
                route_record: "RoutingTable.RouteRecord",
                compute_record: "RoutingTable.ComputeRecord",
                current_timestamp_in_utc: int,
                **params,
            ) -> None:
                ...

        class IExecutionSuccessHook:
            @abstractmethod
            def __call__(
                self,
                routing_table: "RoutingTable",
                route_record: "RoutingTable.RouteRecord",
                execution_context_id: str,
                materialized_inputs: List[Signal],
                materialized_output: Signal,
                current_timestamp_in_utc: int,
                **params,
            ) -> None:
                ...

        class IExecutionFailureHook:
            @abstractmethod
            def __call__(
                self,
                routing_table: "RoutingTable",
                route_record: "RoutingTable.RouteRecord",
                execution_context_id: str,
                materialized_inputs: List[Signal],
                materialized_output: Signal,
                current_timestamp_in_utc: int,
                **params,
            ) -> None:
                ...

        class IExecutionCheckpointHook:
            @abstractmethod
            def __call__(
                self,
                routing_table: "RoutingTable",
                route_record: "RoutingTable.RouteRecord",
                execution_context_id: str,
                active_compute_records: List["RoutingTable.ComputeRecord"],
                checkpoint_in_secs: int,
                current_timestamp_in_utc: int,
                **params,
            ) -> None:
                ...

    class PendingNode:
        class IPendingNodeCreationHook:
            @abstractmethod
            def __call__(
                self,
                routing_table: "RoutingTable",
                route_record: "RoutingTable.RouteRecord",
                pending_node: "RuntimeLinkNode",
                current_timestamp_in_utc: int,
                **params,
            ):
                ...

        class IPendingNodeExpirationHook:
            @abstractmethod
            def __call__(
                self,
                routing_table: "RoutingTable",
                route_record: "RoutingTable.RouteRecord",
                pending_node: "RuntimeLinkNode",
                current_timestamp_in_utc: int,
                **params,
            ):
                ...

        class IPendingCheckpointHook:
            @abstractmethod
            def __call__(
                self,
                routing_table: "RoutingTable",
                route_record: "RoutingTable.RouteRecord",
                pending_node: "RuntimeLinkNode",
                checkpoint_in_secs: int,
                current_timestamp_in_utc: int,
                **params,
            ):
                ...


class RoutingTable(BaseConstruct, ABC):
    """Each logical Node of an application is represented as a `Route` within this component.

    Main client of this component is the `ProcessingUnit`, which redirects incoming events/`Signal`s into the
    RoutingTable.

    This component is responsible from persistence and synchronization of actions against its internal `Route`s.

    See implementations under `driver` module to have a better idea.
    """

    class RouteIndex:
        """Immutable index to find matching Routes against an incoming Signal.
        Each Platform activation invalidates the current index and causes the generation
        of a new one.

        An index is responsible to map an incoming signal to eligible Route IDs, so it is
        agnostic from the actual state of Routes which are stored/persisted within the RoutingTable.
        """

        def __init__(self) -> None:
            self._index: List[Route] = []

        def clear(self) -> None:
            self._index.clear()

        def add(self, route: Route):
            self._index.append(route)

        def find(self, signal_type: SignalType, source_type: SignalSourceType, resource_path: str) -> Dict[Route, Signal]:
            matches: Dict[Route, Signal] = dict()
            for route in self._index:
                for signal in route.link_node.signals:
                    matching_signal: Signal = signal.create(signal_type, source_type, resource_path)
                    if matching_signal:
                        matches[route] = matching_signal
                        # move to next Route
                        break
            return matches

        def get_all_routes(self) -> Iterable[Route]:
            return self._index

        def get_route(self, routeId: RouteID) -> Optional[Route]:
            for route in self._index:
                if route.route_id == routeId:
                    return route

    class ComputeRecord(Serializable):
        def __init__(
            self,
            trigger_timestamp_utc: int,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_context_id: str,
            state: ComputeResponse = None,
            session_state: ComputeSessionState = None,
            deactivated_timestamp_utc: int = None,
        ) -> None:
            self._record_id: str = str(uuid4())  # used for persistence record-keeping/changeset only
            self._trigger_timestamp_utc = trigger_timestamp_utc
            self._materialized_inputs = materialized_inputs
            self._slot = slot
            self._materialized_output = materialized_output
            self._execution_context_id = execution_context_id
            self._state = state
            self._session_state = session_state
            self._last_checkpoint_mark = None
            self._deactivated_timestamp_utc = deactivated_timestamp_utc
            self._number_of_attempts_on_failure: int = 0

        @property
        def record_id(self) -> str:
            # TODO backwards compatibility. remove.
            if not getattr(self, "_record_id", None):
                self._record_id: str = str(uuid4())
            return self._record_id

        @property
        def trigger_timestamp_utc(self) -> int:
            return self._trigger_timestamp_utc

        @property
        def slot(self) -> Slot:
            return self._slot

        @property
        def materialized_inputs(self) -> List[Signal]:
            return self._materialized_inputs

        @property
        def materialized_output(self) -> Signal:
            return self._materialized_output

        @property
        def execution_context_id(self) -> str:
            return self._execution_context_id

        @property
        def state(self) -> ComputeResponse:
            return self._state

        @state.setter
        def state(self, val: ComputeResponse) -> None:
            self._state = val

        @property
        def session_state(self) -> ComputeSessionState:
            return self._session_state

        @session_state.setter
        def session_state(self, val: ComputeSessionState) -> None:
            self._session_state = val

        @property
        def last_checkpoint_mark(self) -> int:
            return getattr(self, "_last_checkpoint_mark", None)

        @last_checkpoint_mark.setter
        def last_checkpoint_mark(self, value) -> None:
            self._last_checkpoint_mark = value

        @property
        def deactivated_timestamp_utc(self) -> int:
            return self._deactivated_timestamp_utc

        @deactivated_timestamp_utc.setter
        def deactivated_timestamp_utc(self, val: int) -> None:
            self._deactivated_timestamp_utc = val

        @property
        def number_of_attempts_on_failure(self) -> int:
            return getattr(self, "_number_of_attempts_on_failure", 0)

        @number_of_attempts_on_failure.setter
        def number_of_attempts_on_failure(self, value: int) -> None:
            self._number_of_attempts_on_failure = value

        def __hash__(self) -> int:
            return hash(self.slot.type)

        def __eq__(self, other: "ComputeRecord") -> bool:
            return (
                self.slot.type == other.slot.type
                and self.slot.code == other.slot.code
                and self.materialized_output.get_materialized_resource_paths()
                == other.materialized_output.get_materialized_resource_paths()
            )

        def __repr__(self):
            return (
                f"{self.__class__.__name__}(inputs={self._materialized_inputs!r},"
                f"slot={self._slot}"
                f"output={self._materialized_output}"
                f"state={self._state}"
                f"session_state={self._session_state})"
            )

    class RouteRecord(Serializable):
        def __init__(
            self,
            route: Route,
            active_compute_records: Set["RoutingTable.ComputeRecord"] = None,
            active_execution_context_state: Dict[str, bool] = None,
            persisted_pending_node_ids: Set[str] = None,
            persisted_active_compute_record_ids: Set[str] = None,
        ) -> None:
            self._route: Route = route
            self._active_compute_records: Set[RoutingTable.ComputeRecord] = active_compute_records if active_compute_records else set()
            self._active_execution_context_state: Dict[str, bool] = (
                active_execution_context_state if active_execution_context_state else dict()
            )
            # OPTIMIZATION: allows changeset management by drivers
            self._persisted_pending_node_ids: Set[str] = persisted_pending_node_ids if persisted_pending_node_ids else set()
            self._persisted_active_compute_record_ids: Set[str] = (
                persisted_active_compute_record_ids if persisted_active_compute_record_ids else set()
            )

        @property
        def route(self) -> Route:
            return self._route

        @property
        def active_compute_records(self) -> Set["RoutingTable.ComputeRecord"]:
            return self._active_compute_records

        @property
        def active_execution_context_state(self) -> Dict[str, bool]:
            return self._active_execution_context_state

        @property
        def persisted_pending_node_ids(self) -> Set[str]:
            return self._persisted_pending_node_ids

        @property
        def persisted_active_compute_record_ids(self) -> Set[str]:
            return self._persisted_active_compute_record_ids

        def __repr__(self) -> str:
            return f"{self.__class__.__name__}(route={self._route!r}, active_compute_records={self._active_compute_records!r})"

        def has_active_record_for(self, compute_record_or_output: Union["RoutingTable.ComputeRecord", Signal]) -> bool:
            if isinstance(compute_record_or_output, RoutingTable.ComputeRecord):
                return compute_record_or_output in self._active_compute_records
            else:
                output_signal: Signal = compute_record_or_output
                return any(
                    [
                        DimensionFilter.check_equivalence(
                            output_signal.domain_spec.dimension_filter_spec,
                            active_rec.materialized_output.domain_spec.dimension_filter_spec,
                        )
                        for active_rec in self._active_compute_records
                    ]
                )

        def get_active_records_of(self, output: Signal) -> List["RoutingTable.ComputeRecord"]:
            return [
                record
                for record in self._active_compute_records
                if DimensionFilter.check_equivalence(
                    record.materialized_output.domain_spec.dimension_filter_spec, output.domain_spec.dimension_filter_spec
                )
            ]

        def add_active_compute_record(self, compute_record: "RoutingTable.ComputeRecord") -> None:
            self._active_compute_records.add(compute_record)

        def remove_active_compute_record(self, deactivated_record: "RoutingTable.ComputeRecord") -> None:
            self._active_compute_records.remove(deactivated_record)

        def has_remaining_active_record_for(self, context_id: str) -> bool:
            return any([compute_record.execution_context_id == context_id for compute_record in self._active_compute_records])

        def get_remaining_active_records_for(self, context_id: str) -> List["RoutingTable.ComputeRecord"]:
            return [compute_record for compute_record in self._active_compute_records if compute_record.execution_context_id == context_id]

        def add_new_execution_context(self, context_id: str) -> None:
            if context_id not in self._active_execution_context_state:
                self._active_execution_context_state[context_id] = True

        def remove_execution_context(self, context_id: str) -> None:
            if context_id in self._active_execution_context_state:
                del self._active_execution_context_state[context_id]

        def disable_trigger_on_execution_context(self, context_id: str) -> None:
            self._active_execution_context_state[context_id] = False

        def can_trigger_execution_context(self, context_id: str) -> bool:
            return self._active_execution_context_state.get(context_id, False)

    class Response(Serializable):
        def __init__(self) -> None:
            self._routes: Dict[Route, Dict[Route.ExecutionContext, List["RoutingTable.ComputeRecord"]]] = dict()

        @property
        def routes(self) -> Dict[Route, Dict[Route.ExecutionContext, List["RoutingTable.ComputeRecord"]]]:
            return self._routes

        def dump(self) -> Dict[Any, Any]:
            return {r.route_id: {e.id: repr(records) for e, records in e_dict.items()} for r, e_dict in self._routes.items()}

        def simple_dump(self) -> Dict[Any, Any]:
            """Return a map of <RoutID>s and IDs for the newly spawned executions.
            Result does not contain the full dump of executions and routes, so it is intended to be
            used against RoutingTable APIs later to retrieve detailed information on the final
            status of those executions, materialized inputs/outputs.
            """
            return {r.route_id: [e.id for e in e_dict.keys()] for r, e_dict in self._routes.items()}

        def add_route(self, route: Route) -> None:
            execution_contexts: Dict[Route.ExecutionContext, List["RoutingTable.ComputeRecord"]] = dict()
            self._routes[route] = execution_contexts

        def add_execution_context(self, route: Route, exec_context: Route.ExecutionContext) -> None:
            self._routes[route][exec_context] = []

        def add_compute_record(
            self, route: Route, exec_context: Route.ExecutionContext, compute_record: "RoutingTable.ComputeRecord"
        ) -> None:
            self._routes[route][exec_context].append(compute_record)

        def get_execution_contexts(
            self, route: Union[RouteID, Route]
        ) -> Optional[Dict[Route.ExecutionContext, List["RoutingTable.ComputeRecord"]]]:
            if isinstance(route, Route):
                return self._routes.get(route, None)
            else:
                for r, e_dict in self._routes.items():
                    if r.route_id == route:
                        return e_dict
            return None

    def __init__(self, params: ConstructParamsDict) -> None:
        """Provide a default route_index if the construct impl does not specify any.

        A RoutingTable Construct impl can specify a RouteIndex that would suit (more optimized)
        its underlying resources, provide different features (metrics, better logging, etc) and be even persisted to a remote resource.
        """
        super().__init__(params)
        # TODO read the RouteIndex impl type from 'params'
        self._route_index = RoutingTable.RouteIndex()
        self._activation_strategy: RoutingActivationStrategy = RoutingActivationStrategy.CLEAR_MODIFIED_ACTIVE_ROUTES_ONLY

    @property
    def activation_strategy(self) -> RoutingActivationStrategy:
        return self._activation_strategy

    @activation_strategy.setter
    def activation_strategy(self, strategy: RoutingActivationStrategy) -> None:
        self._activation_strategy = strategy

    # overrides
    def terminate(self) -> None:
        self._route_index.clear()
        super().terminate()

    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        self._route_index.clear()
        # add new_routes to index
        for route in new_routes:
            self._route_index.add(route)

        # update active routes
        # 1- clear non-existent routes for aesthetical purposes
        self._delete(current_routes - new_routes)

        if self.activation_strategy == RoutingActivationStrategy.CLEAR_MODIFIED_ACTIVE_ROUTES_ONLY:
            # 2- invalidate modified routes (e.g discard the whole active record from persistent routing table, pending nodes / events).
            for current in current_routes:
                for new in new_routes:
                    if current == new:
                        if not current.check_integrity(new):
                            # integrity check means that routes are not compatible in terms of runtime characteristics.
                            # input/output format, data, slots have changed and we cannot reliably update the existing
                            # state of the route (pending nodes, active computes, etc). It is time to reset.
                            # trade-off: currently active computes and pending nodes (dependency check, etc) will be
                            # orphaned. Real damage is to 'pending nodes' actually, since that data is kind of lost.
                            try:
                                logger.warning(
                                    f"Integrity change detected in route {new.route_id!r}! Its existing"
                                    f" active compute records (if any) will be tombstoned as inactive and also"
                                    f" its pending execution candidates (if any) will be deleted."
                                )
                                self._lock(current.route_id)
                                # load and check active records,
                                # we won't track them anymore, move them to historical db
                                route_record = self._load(current.route_id)
                                if route_record:
                                    if route_record.active_compute_records:
                                        logger.warning(
                                            f"Moving {len(route_record.active_compute_records)} active "
                                            f" compute records to inactive state..."
                                        )
                                        # FUTURE consider forcefully stopping these to eliminate implicit event binding
                                        # upon their probable successful completion. Currently (as of 11/2020) not a big concern.
                                        self._save_inactive_compute_records(route_record.route, list(route_record.active_compute_records))
                            except AttributeError as attr_err:
                                # TODO METRICS_SUPPORT
                                logger.critical(
                                    f"Encountered incompatible route-record in RoutingTable while transferring "
                                    f"active records for route: {current.route_id}"
                                )
                            except Exception as err:
                                logger.critical(
                                    f"Encountered error: '{str(err)}', while deleting the modified active route: {current.route_id}"
                                    f" stack trace: {traceback.format_exc()}"
                                )
                            finally:
                                # delete the old record from active route table.
                                # next time a signal hits the same route from RoutingIndex, it will be loaded with
                                # the up-to-date version.
                                self._delete({current})
                                self._release(current.route_id)
                        elif not current.check_auxiliary_data_integrity(new):
                            # intentionally separating the logic from integrity check above to avoid complexity,
                            # reuse will be handled in a different refactoring scope.
                            try:
                                self._lock(current.route_id)
                                route_record = self._load(current.route_id)
                                route_record.route.transfer_auxiliary_data(new)
                                self._save(route_record)
                            except AttributeError as attr_err:
                                logger.critical(
                                    f"Encountered incompatible route-record in RoutingTable while transferring "
                                    f"active records for route: {current.route_id}"
                                )
                                self._delete({current})
                            finally:
                                self._release(current.route_id)

    def _revert_internal(self, routes: Set[Route], prev_routes: Set[Route]) -> None:
        self._route_index.clear()
        # add prev_routes back
        for route in prev_routes:
            self._route_index.add(route)

    def _provide_route_metrics(self, route: Route) -> List[ConstructInternalMetricDesc]:
        # TODO generic routing table metrics (subclasses should call this)
        return [
            ConstructInternalMetricDesc(id="routing_table.receive", metric_names=["RouteHit", "RouteLoadError", "RouteSaveError"]),
            ConstructInternalMetricDesc(
                id="routing_table.receive.hook",
                metric_names=[
                    hook_type.__name__
                    for hook_type in [
                        RoutingHookInterface.Execution.IExecutionSkippedHook,
                        RoutingHookInterface.Execution.IExecutionBeginHook,
                        RoutingHookInterface.Execution.IExecutionSuccessHook,
                        RoutingHookInterface.Execution.IExecutionFailureHook,
                        RoutingHookInterface.Execution.IComputeFailureHook,
                        RoutingHookInterface.Execution.IComputeSuccessHook,
                        RoutingHookInterface.Execution.IComputeRetryHook,
                        RoutingHookInterface.Execution.IExecutionCheckpointHook,
                        RoutingHookInterface.PendingNode.IPendingNodeCreationHook,
                        RoutingHookInterface.PendingNode.IPendingNodeExpirationHook,
                        RoutingHookInterface.PendingNode.IPendingCheckpointHook,
                    ]
                ],
            ),
            ConstructInternalMetricDesc(
                id="routing_table.receive.hook.time_in_utc",
                metric_names=[
                    hook_type.__name__
                    for hook_type in [
                        RoutingHookInterface.Execution.IExecutionSkippedHook,
                        RoutingHookInterface.Execution.IExecutionBeginHook,
                        RoutingHookInterface.Execution.IExecutionSuccessHook,
                        RoutingHookInterface.Execution.IExecutionFailureHook,
                        RoutingHookInterface.Execution.IComputeFailureHook,
                        RoutingHookInterface.Execution.IComputeSuccessHook,
                        RoutingHookInterface.Execution.IComputeRetryHook,
                        RoutingHookInterface.Execution.IExecutionCheckpointHook,
                        RoutingHookInterface.PendingNode.IPendingNodeCreationHook,
                        RoutingHookInterface.PendingNode.IPendingNodeExpirationHook,
                        RoutingHookInterface.PendingNode.IPendingCheckpointHook,
                    ]
                ],
                metadata={METRIC_VISUALIZATION_PERIOD_HINT: MetricPeriod.ONE_MIN, METRIC_VISUALIZATION_STAT_HINT: MetricStatistic.MAXIMUM},
            ),
        ]

    def _provide_internal_metrics(self) -> List[ConstructInternalMetricDesc]:
        # TODO return internal signals that will be emitted by this abstract class from within hook callback execution
        #  methods (e.g _execute_compute_hook) or from high-level orchestration errors.
        #  please note that this metrics can be retrieved via Diagnostics::__getitem__ or get_internal_metric APIs,
        #  so there is no need to main this list in a global scope.
        return [
            ConstructInternalMetricDesc(
                id="routing_table.filter.event.type",
                metric_names=["InternalStorage", "InternalDiagnostics", "External"],
                # since metric names are shared with the other event type, we have to
                # discriminate using a sub-dimension.
                extra_sub_dimensions={"filter_mode": "True"},
            ),
            ConstructInternalMetricDesc(id="routing_table.event.type", metric_names=["InternalStorage", "InternalDiagnostics", "External"]),
            ConstructInternalMetricDesc(id="routing_table.receive", metric_names=["RouteHit", "RouteLoadError", "RouteSaveError"]),
            # emit collective (not route specific) version of some of the hooks
            ConstructInternalMetricDesc(
                id="routing_table.receive.hook",
                metric_names=[
                    hook_type.__name__
                    for hook_type in [
                        RoutingHookInterface.Execution.IExecutionSkippedHook,
                        RoutingHookInterface.Execution.IExecutionBeginHook,
                        RoutingHookInterface.Execution.IExecutionSuccessHook,
                        RoutingHookInterface.Execution.IExecutionFailureHook,
                        RoutingHookInterface.Execution.IComputeFailureHook,
                        RoutingHookInterface.Execution.IComputeSuccessHook,
                        RoutingHookInterface.Execution.IComputeRetryHook,
                    ]
                ],
            ),
        ]

    def get_all_routes(self) -> Iterable[Route]:
        return self._route_index.get_all_routes()

    def receive(
        self,
        signal_type: SignalType,
        source_type: SignalSourceType,
        resource_path: str,
        target_route_id: Optional[RouteID] = None,
        filter_only=False,
    ) -> "RoutingTable.Response":
        """RheocerOS routing core that relies on persistence/synchronization methods from
        RoutingTable impls.

        Algo:
        - First check if it is recognized by our internal data store (Platform:Storage) and create
        a source access spec accordingly (to be able to go on with the APIs in the rest of the algo).
        - Get matching Route's from the in-memory index. Index should give the input signals that match
        the incoming raw signal.
        - For each route, ping them with this incoming event. Routes maintain their internal state,
        previously received events and keep pending for the condition of a "trigger" (all inputs satisfied).
            - Lock the route so that other Processors / RoutingTables in other concurrent exec contexts wont
            read and modify its state (underlying RoutingTable should provide this synchronization).
            - For this, we call Route::receive and expect for a result to understand if the route has been
            triggered finally.
            - if the trigger happened, then Route responds with execution contexts and feedback signals (reactions)
            - For each execution context, we can run computes now. For that to happen, we have to materialize
            input and output objects within and have each one of those signals to map a pyhsical resource.
            - Call compute nodes; If it is inlined, don't keep the record and get done with it. If it is
            async (like BATCH_COMPUTE), get the initial response to the request, persist it and keep the
            compute record as 'active'. This is important for RoutingTable::check_active_records to visit
            this record and for both status tracking and also for RETRIES. There are some corner cases
            being handled against the initial response. Please see the impl below to get a better idea.
            - Unlock/Release the route record.

        Parameters
        ----------
        signal_type: type of the signal that incorporates both the source_type and the event type.
        source_type: type of the resource that emits this signal
        resource_path: materialized/concrete path that maps to a physical resource
        target_route_id: if specified, RoutingTable makes sure that only the target route receives the incomming
        signal, otherwise the entire table is scanned.

        Returns
        -------
        None
        """
        response = RoutingTable.Response()
        logger.critical(f"Routing signal_type={signal_type}, source_type={source_type}, resource_path={resource_path} ...")
        if target_route_id:
            logger.critical(f"Will only check route: {target_route_id!r}")
        # first check if it is internal
        data_access_spec = self.get_platform().storage.map_incoming_event(source_type, resource_path)
        if not data_access_spec:  # not internal data
            data_access_spec = self.get_platform().diagnostics.map_incoming_event(source_type, resource_path)
            if not data_access_spec:  # not internal diagnostics data
                data_access_spec = SignalSourceAccessSpec(source_type, resource_path, None)
            else:
                self.metric(f"routing_table{'.filter' if filter_only else ''}.event.type")["InternalDiagnostics"].emit(1)
        else:
            self.metric(f"routing_table{'.filter' if filter_only else ''}.event.type")["InternalStorage"].emit(1)

        # create signal for resource_path from matching Route from the internal index.
        # we can only create candidate signals by scanning our index.
        # (doing a prefix/compatibility check against existing route input signals).
        #
        # find matching / eligible [Route, Signal] pairs
        # TODO add comment on why 1 Signal retrieval is enough for 1 Route.
        route_and_signal_map = self._route_index.find(signal_type, data_access_spec.source, data_access_spec.path_format)
        if filter_only:
            for route, _ in route_and_signal_map.items():
                if not target_route_id or route.route_id == target_route_id:
                    response.add_route(route)
        elif route_and_signal_map:
            logger.info("Found matching routes for incoming signal:")
            self.metric("routing_table.receive")["RouteHit"].emit(len(route_and_signal_map.keys()))
            for route, incoming_signal in route_and_signal_map.items():
                if target_route_id and route.route_id != target_route_id:
                    logger.critical(f"Skipping route={route.route_id} since this session targets {target_route_id!r} only.")
                    continue

                logger.critical(f"Processing route={route.route_id}")
                self.metric("routing_table.receive", route=route)["RouteHit"].emit(1)
                # blocking wait / synchronous access to a specific Route
                self._lock(route.route_id)
                route_record: RoutingTable.RouteRecord = self._load(route.route_id)
                if not route_record:
                    logger.info(f"First time checking on this route.")
                    # clone the immutable route from the index and create a brand new
                    # record for it.
                    route_record = RoutingTable.RouteRecord(route.clone())
                else:
                    self._check_active_compute_records_for(route_record)
                    # OPTIMIZATION: it is safe to skip ready_node check since ready-check will implicitly be done
                    #               as part of this signal ingestion within Route::receive down there (for each impacted
                    #               pending node). And actually it is not mandatory as _check_active_compute_records_for
                    # self._check_ready_pending_nodes_for(route, route_record)  # do this before expiration check (next), graceful
                    self._check_pending_nodes_for(route_record)

                # the following operation is stateful (changes the internal state of a route)
                # so TODO best way to rollback
                route_response: Optional[Route.Response] = route_record.route.receive(incoming_signal, self.get_platform())
                if route_response:
                    response.add_route(route_record.route)

                    self._process_route_response(route, route_record, route_response, response)
                elif not incoming_signal.is_reference:
                    logger.critical(
                        f"Incoming signal has been rejected by Route. This is an anomaly since RoutingTable"
                        f" implementation acts on compatible Signal-Route pairs."
                        f" If this is not due to an incompatibility between RouteIndex and persisted table,"
                        f" then treat this as an RuntimeError (if not due to a transient/deployment time"
                        f" issue, which should go away after RheocerOS pause mechanism."
                        f"Signal: {incoming_signal!r}"
                        f"Route: {route_record.route!r}"
                    )

                self._save(route_record, suppress_errors_and_emit=True)
                self._release(route_record.route.route_id)

        return response

    def _process_route_response(
        self,
        route: Route,
        route_record: "RoutingTable.RouteRecord",
        route_response: Route.Response,
        response: Optional["RoutingTable.Response"] = None,
    ) -> None:
        # send the reactions to our global instruction queue
        # i.e backfilling requests into the same application to another one
        # ProcessingUnit will consume these events and do necessary actions.
        self.get_platform().processor_queue.send(route_response.reactions)

        for pending_node in route_response.new_pending_nodes:
            self._execute_pending_node_hook(
                route_record.route.pending_node_hook.on_pending_node_created,
                route_record,
                pending_node,
                RoutingHookInterface.PendingNode.IPendingNodeCreationHook,
            )

        for execution_context in route_response.new_execution_contexts:
            if response:
                response.add_execution_context(route_record.route, execution_context)
            materialized_inputs: List[Signal] = []
            materialized_output: Signal = None
            # materialize/unbox INTERNAL inputs and outputs
            for input_signal in execution_context.completed_link_node.ready_signals:
                if input_signal.resource_access_spec.source == SignalSourceType.INTERNAL:
                    access_spec = self.get_platform().storage.map_internal_data_access_spec(input_signal.resource_access_spec)
                    materialized_inputs.append(input_signal.create_from_spec(access_spec))
                else:
                    # TODO make sure input_signal is instantiated by the Route (not the original version)
                    materialized_inputs.append(input_signal)

            # now the output
            if execution_context.output.resource_access_spec.source == SignalSourceType.INTERNAL:
                access_spec = self.get_platform().storage.map_internal_data_access_spec(execution_context.output.resource_access_spec)
                materialized_output = execution_context.output.create_from_spec(access_spec)
            else:
                materialized_output = execution_context.output

            # TODO check node.is_idempotent
            if route_record.has_active_record_for(materialized_output):
                # skip the whole execution if we have an active record from previous executions,
                # working on the same output.
                self._execute_init_hook(
                    route.execution_hook.on_exec_skipped,
                    route_record,
                    execution_context,
                    RoutingHookInterface.Execution.IExecutionSkippedHook,
                )
            else:
                # now we can assume that the following loop will activate new compute records.
                self._execute_init_hook(
                    route.execution_hook.on_exec_begin, route_record, execution_context, RoutingHookInterface.Execution.IExecutionBeginHook
                )

                for slot in execution_context.slots:
                    compute_candidate = RoutingTable.ComputeRecord(
                        self._current_timestamp_in_utc(), materialized_inputs, slot, materialized_output, execution_context.id
                    )
                    # TODO check slot.is_idempotent as part of the following if-statement.
                    #      note that this check is not even hit if node level idempotency is active above.
                    if not route_record.has_active_record_for(compute_candidate):
                        route_record.add_new_execution_context(execution_context.id)
                        if response:
                            response.add_compute_record(route_record.route, execution_context, compute_candidate)
                        logger.info(f"Activating new compute on: {materialized_output.get_materialized_resource_paths()[0]!r}")
                        if slot.type == SlotType.SYNC_INLINED:
                            # TODO evaluate ABI, Language
                            #  - use the same input / output serialization we use for batch-compute
                            #  - create locals variables (ask impls to provide locals and inject them), i.e embedded ABI
                            #  For now, assuming Python inlined (with no requirement from the env)
                            compute_response, compute_session_state = self._execute_inlined(materialized_inputs, slot, materialized_output)
                            compute_candidate.state = compute_response
                            compute_candidate.session_state = compute_session_state
                            # check if retryable failure, in all other cases terminate the execution.
                            #  Example: user/app code has a downstream dependency which was down during the execution.
                            if (
                                compute_response.response_type == ComputeResponseType.FAILED
                                and cast(ComputeFailedResponse, compute_response).failed_response_type
                                == ComputeFailedResponseType.TRANSIENT
                            ):
                                logger.info(f"Will retry in the next cycle.")
                                route_record.add_active_compute_record(compute_candidate)
                            else:
                                if compute_response.response_type != ComputeResponseType.SUCCESS:
                                    route_record.disable_trigger_on_execution_context(execution_context.id)
                                    compute_hook = route_record.route.execution_hook.on_compute_failure
                                    hook_type = RoutingHookInterface.Execution.IComputeFailureHook
                                else:
                                    compute_hook = route_record.route.execution_hook.on_compute_success
                                    hook_type = RoutingHookInterface.Execution.IComputeSuccessHook
                                compute_candidate.deactivated_timestamp_utc = self._current_timestamp_in_utc()
                                # immediately save as historical record.
                                self._save_inactive_compute_records(route_record.route, [compute_candidate], suppress_errors_and_emit=True)
                                self._execute_compute_hook(compute_hook, route_record, compute_candidate, hook_type)
                                logger.info(f"Successfully deactivated compute {compute_candidate.session_state!r}")
                        elif slot.type == SlotType.ASYNC_BATCH_COMPUTE:
                            # FUTURE convert whole BATCH_COMPUTE to REMOTE_COMPUTE (batch + async normal combined)
                            try:
                                compute_response = self.get_platform().batch_compute.compute(
                                    materialized_inputs, slot, materialized_output, execution_ctx_id=execution_context.id
                                )
                            except Exception as error:
                                logger.critical(
                                    f"Could not initiate batch compute on route {route_record.route.route_id} for"
                                    f" {materialized_output.get_materialized_resource_paths()[0]!r}"
                                    f" due to an internal error in the driver: {error!r}"
                                )
                                compute_response = ComputeFailedResponse(
                                    ComputeFailedResponseType.COMPUTE_INTERNAL, ComputeResourceDesc(None, None), None, repr(error)
                                )

                            compute_candidate.state = compute_response
                            if compute_candidate.state.response_type == ComputeResponseType.SUCCESS:
                                logger.info(
                                    f"Got {ComputeResponseType.SUCCESS.value} response for {SlotType.ASYNC_BATCH_COMPUTE.value}!"
                                    f"Waiting to get initial session state..."
                                )
                                # not mandatory but let's try to get a response from the batch-compute
                                # and let it do the heavy-lifting in filling the initial state of internal
                                # session data (i.e aws glue job state). router is agnostic from this details.
                                try:
                                    internal_session_state: ComputeSessionState = self.get_platform().batch_compute.get_session_state(
                                        compute_response.session_desc, compute_candidate
                                    )
                                except Exception as error:
                                    # integration problem, treat it as failure at orchestration level
                                    internal_session_state = ComputeFailedSessionState(
                                        ComputeFailedSessionStateType.COMPUTE_INTERNAL,
                                        compute_response.session_desc,
                                        [
                                            ComputeExecutionDetails(
                                                compute_candidate.trigger_timestamp_utc,
                                                self._current_timestamp_in_utc(),
                                                {
                                                    "ErrorMessage": "Will be marked as failure due to unexpected error in compute session state retrieval: "
                                                    + str(error)
                                                },
                                            )
                                        ],
                                    )

                                compute_candidate.session_state = internal_session_state
                                # following statement added for testing support (to facilitate tests actually)
                                if internal_session_state.state_type == ComputeSessionStateType.COMPLETED:
                                    compute_candidate.deactivated_timestamp_utc = self._current_timestamp_in_utc()
                                    self._save_inactive_compute_records(
                                        route_record.route, [compute_candidate], suppress_errors_and_emit=True
                                    )
                                    self.get_platform().batch_compute.terminate_session(compute_candidate)
                                    self._execute_compute_hook(
                                        route_record.route.execution_hook.on_compute_success,
                                        route_record,
                                        compute_candidate,
                                        RoutingHookInterface.Execution.IComputeSuccessHook,
                                    )
                                else:
                                    route_record.add_active_compute_record(compute_candidate)
                                logger.info(
                                    f"Invocation for {SlotType.ASYNC_BATCH_COMPUTE} type compute is done."
                                    f" Current session state: {internal_session_state.state_type!r}."
                                    f" Materialized output path: {compute_candidate.materialized_output.get_materialized_resource_paths()[0]!r}"
                                )
                            else:
                                failed_state: ComputeFailedResponse = cast(ComputeFailedResponse, compute_response)
                                logger.error(f"Got {failed_state!r} response for {SlotType.ASYNC_BATCH_COMPUTE.value}!")
                                if failed_state.failed_response_type == ComputeFailedResponseType.TRANSIENT:
                                    logger.info(f"Will retry in the next cycle.")
                                    # TODO read retry_count from active_compute_record.slot
                                    # keep this as active and retry in the next cycle of `check_active_routes`
                                    route_record.add_active_compute_record(compute_candidate)
                                else:
                                    route_record.disable_trigger_on_execution_context(execution_context.id)
                                    logger.info("Deactivating unrecoverably failed compute and marking it as a historical record...")
                                    # terminate
                                    compute_candidate.deactivated_timestamp_utc = self._current_timestamp_in_utc()
                                    self._save_inactive_compute_records(
                                        route_record.route, [compute_candidate], suppress_errors_and_emit=True
                                    )
                                    self.get_platform().batch_compute.terminate_session(compute_candidate)
                                    self._execute_compute_hook(
                                        route_record.route.execution_hook.on_compute_failure,
                                        route_record,
                                        compute_candidate,
                                        RoutingHookInterface.Execution.IComputeFailureHook,
                                    )
                        else:
                            # TODO use 'hook_internal' and raise ValueError during development
                            raise RuntimeError(
                                f"{self.__class__.__name__}: Unsupported slot_type: {slot.type!r} detected"
                                f" while processing route: {route!r}."
                            )

                    else:
                        logger.critical(
                            f"Idempotency Warning: Ignoring the following compute candidate since"
                            f" it is already active! Discarded compute request: {compute_candidate!r}"
                        )

                # RheocerOS high-level, context aware completion check mechanism
                # Logic: if all of the compute declarations are satisfied for a route
                #        then downstream routes can be triggered via a feed-back signal
                #        using the materialized version of target/output signal.
                if not route_record.has_remaining_active_record_for(execution_context.id):
                    if route_record.can_trigger_execution_context(execution_context.id):
                        # probably the context had only INLINED compute slots and they've all succeeded.
                        # we can trigger downstream routes with materialized_output
                        self.get_platform().processor_queue.send(materialized_output)
                        completion_hook = route.execution_hook.on_success
                        hook_type = RoutingHookInterface.Execution.IExecutionSuccessHook
                    else:
                        completion_hook = route.execution_hook.on_failure
                        hook_type = RoutingHookInterface.Execution.IExecutionFailureHook
                    self._execute_context_hook(
                        completion_hook, route_record, execution_context.id, materialized_inputs, materialized_output, hook_type
                    )
                    route_record.remove_execution_context(execution_context.id)

    def check_active_routes(self) -> None:
        """Periodical check triggered by ProcessingUnit

        Call batch-compute to get internal session state for each active compute record
        """
        logger.critical("Checking active routes...")
        for route in self._route_index.get_all_routes():
            self.check_active_route(route)

    def check_active_route(self, route: Union[RouteID, Route]) -> None:
        try:
            if not isinstance(route, Route):
                route_id = route
                route = self._route_index.get_route(route_id)
            else:
                route_id = cast(Route, route).route_id
            route_record: RoutingTable.RouteRecord = None
            self._lock(route_id)
            route_record = self._load(route_id)
            if route_record:
                self._check_active_compute_records_for(route_record)
                self._check_ready_pending_nodes_for(route, route_record)  # do this before expiration check (next), graceful
                self._check_pending_nodes_for(route_record)
        except Exception as err:
            logger.critical(
                f"Encountered error: '{str(err)}',  while checking the status of route: {route_id!r}"
                f" stack trace: {traceback.format_exc()}"
            )
        finally:
            if route_record:
                self._save(route_record, suppress_errors_and_emit=True)
            self._release(route_id)

    def _check_active_compute_records_for(self, route_record: "RoutingTable.RouteRecord") -> None:
        """Update the status of active compute records and detect completions and feed the platform
        back with trigger signals.

        State of route_record object is modified/updated.
        """
        inactive_records: List[RoutingTable.ComputeRecord] = []
        for active_compute_record in route_record.active_compute_records:
            if active_compute_record.slot.type == SlotType.ASYNC_BATCH_COMPUTE:
                retry: bool = False
                retry_session_desc = None
                if active_compute_record.state.response_type == ComputeResponseType.SUCCESS:
                    state = cast(ComputeSuccessfulResponse, active_compute_record.state)
                    if active_compute_record.session_state and active_compute_record.session_state.session_desc:
                        most_recent_session_desc: ComputeSessionDesc = active_compute_record.session_state.session_desc
                    else:
                        most_recent_session_desc: ComputeSessionDesc = state.session_desc

                    try:
                        internal_session_state: ComputeSessionState = self.get_platform().batch_compute.get_session_state(
                            most_recent_session_desc, active_compute_record
                        )
                    except Exception as error:
                        # BatchCompute non-transient exceptions are permanent failures for orchestration,
                        # don't disrupt the overall flow and move on
                        internal_session_state = ComputeFailedSessionState(
                            ComputeFailedSessionStateType.COMPUTE_INTERNAL,
                            most_recent_session_desc,
                            [
                                ComputeExecutionDetails(
                                    active_compute_record.trigger_timestamp_utc,
                                    self._current_timestamp_in_utc(),
                                    {
                                        "ErrorMessage": "Marked as failure due to unexpected error in compute session state retrieval: "
                                        + str(error)
                                    },
                                )
                            ],
                        )

                    if active_compute_record.session_state:
                        # transfer/keep previous exec history for this compute record
                        if active_compute_record.session_state.session_desc.session_id != internal_session_state.session_desc.session_id:
                            # a new session has been spawned internall for the compute record (probably an implicit retry [e.g AWS Glue retry]
                            # or as a result of internal complex workflow [e.g Athena driver workflow moves from Glue prologue to CTAS execution]),
                            # IF regards it as an orchestration responsibility to manage execution details/history persistence.
                            # So transfer previous exec details into the session_state. use orchestrator level depth limit,
                            # in other re-runs we are not interested in keeping the exec details (ex: TRANSIENT failure auto reruns which would be
                            # of no).
                            internal_session_state.executions = (
                                active_compute_record.session_state.executions[: ComputeSessionState.EXECUTION_DETAILS_DEPTH]
                                + internal_session_state.executions
                            )
                        else:  # just replace the most recent (the last) one.
                            internal_session_state.executions = (
                                active_compute_record.session_state.executions[:-1] + internal_session_state.executions
                            )

                    active_compute_record.session_state = internal_session_state
                    if not internal_session_state.state_type.is_active():
                        logger.critical(f"Detected inactive record on route: {route_record.route.route_id!r}!")
                        if (
                            internal_session_state.state_type == ComputeSessionStateType.FAILED
                            and cast(ComputeFailedSessionState, internal_session_state).failed_type
                            == ComputeFailedSessionStateType.TRANSIENT
                        ):
                            # Compute (system) TRANSIENT failures dont get counted towards max retry count.
                            if self.get_platform().batch_compute.can_retry(active_compute_record):
                                logger.critical(f"Compute state is retryable (TRANSIENT failure). Will attempt retry now...")
                                retry = True
                                retry_session_desc = internal_session_state.session_desc
                            else:
                                logger.critical(
                                    f"Compute state is retryable (TRANSIENT failure) but BatchCompute driver rejects "
                                    f"the retry now. Will attempt to retry in the next cycle..."
                                )

                        elif (
                            internal_session_state.state_type == ComputeSessionStateType.FAILED
                            and active_compute_record.number_of_attempts_on_failure < active_compute_record.slot.max_retry_count
                        ):
                            next_attempt = active_compute_record.number_of_attempts_on_failure + 1
                            logger.critical(
                                f"Compute state is retryable (max retry count={active_compute_record.slot.max_retry_count}, "
                                f"number of retries so far={active_compute_record.number_of_attempts_on_failure}, "
                                f"failure_type={internal_session_state.failed_type!r}). "
                                f"Will attempt retry now (attempt number={next_attempt})..."
                            )
                            active_compute_record.number_of_attempts_on_failure = next_attempt
                            # for retries due to internal application logic (user code), we don't use BatchCompute::can_retry
                            retry = True
                            retry_session_desc = internal_session_state.session_desc
                            self._execute_compute_hook(
                                route_record.route.execution_hook.on_compute_retry,
                                route_record,
                                active_compute_record,
                                RoutingHookInterface.Execution.IComputeRetryHook,
                            )
                        else:
                            if internal_session_state.state_type != ComputeSessionStateType.COMPLETED:
                                route_record.disable_trigger_on_execution_context(active_compute_record.execution_context_id)
                                compute_hook = route_record.route.execution_hook.on_compute_failure
                                hook_type = RoutingHookInterface.Execution.IComputeFailureHook
                            else:
                                compute_hook = route_record.route.execution_hook.on_compute_success
                                hook_type = RoutingHookInterface.Execution.IComputeSuccessHook

                            logger.critical(f"Deactivating compute and marking it as a historical record...")
                            # logger.info(f"Compute record: {active_compute_record!r}")
                            active_compute_record.deactivated_timestamp_utc = self._current_timestamp_in_utc()
                            inactive_records.append(active_compute_record)
                            self.get_platform().batch_compute.terminate_session(active_compute_record)
                            self._execute_compute_hook(compute_hook, route_record, active_compute_record, hook_type)
                elif active_compute_record.state.response_type == ComputeResponseType.FAILED:
                    logger.error(f"Detected active record in {ComputeResponseType.FAILED.value} state")
                    state = cast(ComputeFailedResponse, active_compute_record.state)
                    if state.failed_response_type == ComputeFailedResponseType.TRANSIENT:
                        if self.get_platform().batch_compute.can_retry(active_compute_record):
                            logger.critical("Failure reason is TRANSIENT. Will attempt retry now...")
                            retry = True
                        else:
                            logger.critical(
                                "BatchCompute driver decided to defer retry for TRANSIENT error. Will check" "in the next cycle."
                            )
                    else:
                        route_record.disable_trigger_on_execution_context(active_compute_record.execution_context_id)
                        # this would happen after the 1st retry (TRANSIENT -> another failure state)
                        active_compute_record.deactivated_timestamp_utc = self._current_timestamp_in_utc()
                        inactive_records.append(active_compute_record)
                        logger.critical("Deactivating failed compute and marking it as a historical record...")
                        logger.info(
                            f"Compute record session state: {active_compute_record.session_state!r}\n"
                            f"Output path: {active_compute_record.materialized_output.get_materialized_resource_paths()[0]!r}"
                        )
                        self.get_platform().batch_compute.terminate_session(active_compute_record)
                        self._execute_compute_hook(
                            route_record.route.execution_hook.on_compute_failure,
                            route_record,
                            active_compute_record,
                            RoutingHookInterface.Execution.IComputeFailureHook,
                        )

                if retry:
                    logger.critical(
                        f"Attempting retry on compute record: {active_compute_record.session_state!r}\n"
                        f" Output path: {active_compute_record.materialized_output.get_materialized_resource_paths()[0]!r}"
                    )
                    try:
                        compute_response = self.get_platform().batch_compute.compute(
                            active_compute_record.materialized_inputs,
                            active_compute_record.slot,
                            active_compute_record.materialized_output,
                            execution_ctx_id=active_compute_record.execution_context_id,
                            retry_session_desc=retry_session_desc,
                        )
                    except Exception as error:
                        logger.critical(
                            f"Could not retry batch compute on route {route_record.route.route_id} for"
                            f" {active_compute_record.materialized_output.get_materialized_resource_paths()[0]!r}"
                            f" due to an internal error in the driver: {error!r}"
                        )
                        compute_response = ComputeFailedResponse(
                            ComputeFailedResponseType.COMPUTE_INTERNAL, ComputeResourceDesc(None, None), None, repr(error)
                        )
                    # set the state and keep moving without doing anything else,
                    # next check cycle will visit this new state again.
                    active_compute_record.state = compute_response
                    # resetting active session state to indicate a brand-new compute cycle,
                    # so that in the next cycle previous session_state won't be picked up
                    # for query (get_session_state) which would cause infinite retry loops.
                    active_compute_record.session_state = None
            elif active_compute_record.slot.type == SlotType.SYNC_INLINED:
                # for inlined compute, we just focus on retries here (only case when an inlined compute would
                #   be active within this periodical check).
                if (
                    active_compute_record.state.response_type == ComputeResponseType.FAILED
                    and cast(ComputeFailedResponse, active_compute_record.state).failed_response_type == ComputeFailedResponseType.TRANSIENT
                ):
                    # new attempt to execute the inlined compute
                    self._execute_compute_hook(
                        route_record.route.execution_hook.on_compute_retry,
                        route_record,
                        active_compute_record,
                        RoutingHookInterface.Execution.IComputeRetryHook,
                    )
                    compute_response, compute_session_state = self._execute_inlined(
                        active_compute_record.materialized_inputs, active_compute_record.slot, active_compute_record.materialized_output
                    )

                    active_compute_record.state = compute_response
                    active_compute_record.session_state = compute_session_state
                    # check if retryable failure, in all other cases terminate the execution.
                    #  Example: user/app code has a downstream dependency which was down during the execution.
                    if (
                        compute_response.response_type == ComputeResponseType.FAILED
                        and cast(ComputeFailedResponse, compute_response).failed_response_type == ComputeFailedResponseType.TRANSIENT
                    ):
                        # if inlined compute is still TRANSIENT, then do nothing and wait for next cycle
                        pass
                    else:
                        if compute_response.response_type != ComputeResponseType.SUCCESS:
                            route_record.disable_trigger_on_execution_context(active_compute_record.execution_context_id)
                            compute_hook = route_record.route.execution_hook.on_compute_failure
                            hook_type = RoutingHookInterface.Execution.IComputeFailureHook
                        else:
                            compute_hook = route_record.route.execution_hook.on_compute_success
                            hook_type = RoutingHookInterface.Execution.IComputeSuccessHook
                        active_compute_record.deactivated_timestamp_utc = self._current_timestamp_in_utc()
                        inactive_records.append(active_compute_record)
                        self._execute_compute_hook(compute_hook, route_record, active_compute_record, hook_type)
                        logger.info(
                            f"Successfully deactivated inlined compute {active_compute_record.session_state!r}\n"
                            f" Output path: {active_compute_record.materialized_output.get_materialized_resource_paths()[0]!r}"
                        )

        for inactive_record in inactive_records:
            route_record.remove_active_compute_record(inactive_record)

            # RheocerOS high-level, context aware completion check mechanism
            # Logic: if all of the compute declarations are satisfied for a route
            #        then downstream routes can be triggered via a feed-back signal
            #        using the materialized version of target/output signal.
            if not route_record.has_remaining_active_record_for(inactive_record.execution_context_id):
                if route_record.can_trigger_execution_context(inactive_record.execution_context_id):
                    # all of the compute slots have succeeded.
                    # we can now trigger downstream routes with materialized_output
                    self.get_platform().processor_queue.send(inactive_record.materialized_output)
                    logger.critical(
                        f"Output signal (materialized_paths={inactive_record.materialized_output.get_materialized_resource_paths()!r}) "
                        f" for completed execution context (route_id={route_record.route.route_id})"
                        f" has been queued successfully."
                    )
                    completion_hook = route_record.route.execution_hook.on_success
                    hook_type = RoutingHookInterface.Execution.IExecutionSuccessHook
                else:
                    completion_hook = route_record.route.execution_hook.on_failure
                    hook_type = RoutingHookInterface.Execution.IExecutionFailureHook
                self._execute_context_hook(
                    completion_hook,
                    route_record,
                    inactive_record.execution_context_id,
                    inactive_record.materialized_inputs,
                    inactive_record.materialized_output,
                    hook_type,
                )
                route_record.remove_execution_context(inactive_record.execution_context_id)

        if inactive_records:
            self._save_inactive_compute_records(route_record.route, inactive_records, suppress_errors_and_emit=True)

        # 'execution' checkpoints on remaining active executions
        if route_record.route.has_execution_checkpoints():
            for activate_execution_ctx_id in route_record.active_execution_context_state.keys():
                active_records = route_record.get_remaining_active_records_for(activate_execution_ctx_id)
                datum = min(compute_record.trigger_timestamp_utc for compute_record in active_records)
                previously_checked_records = [compute_record for compute_record in active_records if compute_record.last_checkpoint_mark]
                if previously_checked_records:
                    last_checkpoint_timestamp = max(compute_record.last_checkpoint_mark for compute_record in previously_checked_records)
                else:
                    last_checkpoint_timestamp = None
                elapsed_time = self._current_timestamp_in_utc() - datum
                logger.critical(
                    f"Execution checkpoint. elapsed_time: {elapsed_time}, last_checkpoint_timestamp: {last_checkpoint_timestamp}"
                )
                checkpoints = route_record.route.get_next_execution_checkpoints(elapsed_time, last_checkpoint_timestamp)
                if checkpoints:
                    for checkpoint in checkpoints:
                        logger.critical(f"Execution checkpoint. {checkpoint!r}")
                        self._execute_active_context_checkpoint_hook(
                            checkpoint.slot,
                            route_record,
                            activate_execution_ctx_id,
                            active_records,
                            checkpoint.checkpoint_in_secs,
                            RoutingHookInterface.Execution.IExecutionCheckpointHook,
                        )
                        for active_record in active_records:
                            active_record.last_checkpoint_mark = checkpoint.checkpoint_in_secs

    def _check_pending_nodes_for(self, route_record: "RoutingTable.RouteRecord") -> None:
        """Check the status of pending nodes of this route.

        - make sure that pending node checkpoints are called.
        - make sure that pending nodes expire (if route has TTL defined).

        State of route_record object is modified/updated.
        """
        if route_record.route.has_pending_node_checkpoints():
            for pending_node in route_record.route.pending_nodes:
                elapsed_time = self._current_timestamp_in_utc() - pending_node.activated_timestamp
                checkpoints = route_record.route.get_next_pending_node_checkpoint(elapsed_time, pending_node.last_checkpoint_mark)
                if checkpoints:
                    for checkpoint in checkpoints:
                        logger.critical(f"Pending checkpoint. {checkpoint!r} elapsed_time: {elapsed_time}")
                        self._execute_pending_node_checkpoint_hook(
                            checkpoint.slot,
                            route_record,
                            pending_node,
                            checkpoint.checkpoint_in_secs,
                            RoutingHookInterface.PendingNode.IPendingCheckpointHook,
                        )
                        pending_node.last_checkpoint_mark = checkpoint.checkpoint_in_secs

        expired_nodes: Set["RuntimeLinkNode"] = route_record.route.check_expired_nodes()
        for pending_node in expired_nodes:
            self._execute_pending_node_hook(
                route_record.route.pending_node_hook.on_expiration,
                route_record,
                pending_node,
                RoutingHookInterface.PendingNode.IPendingNodeExpirationHook,
            )

    def _check_ready_pending_nodes_for(self, route: Route, route_record: "RoutingTable.RouteRecord") -> None:
        """Check the route for pending nodes that would need 'pull' operations to check things like;
            - 'range/completion checks'

        and probably become ready now.

        :param route: route instance from immutable RoutingTable::RouteIndex
        :param route_record: record represents the persisted version of the same route
        :return: None
        """
        route_response: Optional[Route.Response] = route_record.route.check_new_ready_pending_nodes(self.get_platform())
        self._process_route_response(route, route_record, route_response)

    @classmethod
    def _current_timestamp_in_utc(cls) -> int:
        return current_timestamp_in_utc()

    def _execute_inlined(
        self, materialized_inputs: List[Signal], slot: Slot, materialized_output: Signal
    ) -> Tuple[ComputeResponse, ComputeSessionState]:
        start_time = self._current_timestamp_in_utc()
        code_response = None
        try:
            # expose all of the internal params of the construct impl to client's code
            # ex: BOTO_SESSION, etc
            args = params = self._params
            # now setup convenience local variables
            input_map: Dict[str, Signal] = {}
            for i, input_signal in enumerate(materialized_inputs):
                exec(f"input{i} = materialized_inputs[{i}]")
                if input_signal.alias:
                    exec(f"{input_signal.alias} = materialized_inputs[{i}]")
                    input_map[input_signal.alias] = input_signal

            exec(f"output = materialized_output")
            code = loads(slot.code)
            if isinstance(code, str):
                exec(code)
            else:
                code_response = code(input_map, materialized_output, params)
        except Exception as err:
            stack_trace: str = traceback.format_exc()
            logger.critical(f"Inlined compute execution failed!" f"Error:{str(err)}, Stack Trace:{stack_trace}")
            if isinstance(err, ComputeInternalError):
                failure_session_state_type = ComputeFailedSessionStateType.APP_INTERNAL
                if isinstance(err, ComputeRetryableInternalError):
                    logger.critical(f"Internal error is transient, will retry in the next cycle.")
                    failure_response_type = ComputeFailedResponseType.TRANSIENT
                else:
                    failure_response_type = ComputeFailedResponseType.APP_INTERNAL
            else:
                failure_response_type = ComputeFailedResponseType.BAD_SLOT
                failure_session_state_type = ComputeFailedSessionStateType.UNKNOWN
            return (
                ComputeFailedResponse(
                    failure_response_type, ComputeResourceDesc(None, None), None, "Error:" + str(err) + ", Stack Trace:" + stack_trace
                ),
                ComputeFailedSessionState(
                    failure_session_state_type,
                    None,
                    [ComputeExecutionDetails(start_time, self._current_timestamp_in_utc(), {"stack_trace": stack_trace})],
                ),
            )

        end_time = self._current_timestamp_in_utc()
        session_desc = ComputeSessionDesc(
            f"{self.__class__.__name__}::_execute_inlined"
            f"start time: {start_time},"
            f"end time: {end_time}"
            f"elapsed in secs: {end_time - start_time}"
            f"response: {code_response!r}",
            ComputeResourceDesc(None, None),
        )
        return (
            ComputeSuccessfulResponse(ComputeSuccessfulResponseType.COMPLETED, session_desc),
            ComputeSessionState(
                session_desc,
                ComputeSessionStateType.COMPLETED,
                [ComputeExecutionDetails(start_time, end_time, {"response": repr(code_response)})],
            ),
        )

    def _execute_init_hook(
        self,
        slot: Slot,
        route_record: "RoutingTable.RouteRecord",  # persistent route object
        execution_context: "Route.ExecutionContext",
        hook_type: Type,
    ) -> None:
        timestamp_in_utc = self._current_timestamp_in_utc()
        self.metric("routing_table.receive.hook")[hook_type.__name__].emit(1)
        self.metric("routing_table.receive.hook", route=route_record.route)[hook_type.__name__].emit(1)
        self.metric("routing_table.receive.hook.time_in_utc", route=route_record.route)[hook_type.__name__].emit(timestamp_in_utc)
        if slot:
            params = dict(self._params)
            params.update(
                {
                    RoutingHookInterface.HOOK_TYPE_PARAM: hook_type,
                    "_route_record": route_record,
                    "_execution_context": execution_context,
                    "_current_timestamp_in_utc": timestamp_in_utc,
                }
            )
            try:
                code = loads(slot.code)
                if isinstance(code, str):
                    exec(code)
                else:
                    code(self, route_record, execution_context, timestamp_in_utc, **params)
            except Exception as err:
                stack_trace: str = traceback.format_exc()
                logger.critical(f"Inlined execution context hook failed!" f"Error:{str(err)}, Stack Trace:{stack_trace}")

    def _execute_context_hook(
        self,
        slot: Slot,
        route_record: "RoutingTable.RouteRecord",  # persistent route object
        execution_ctx_id: str,
        materialized_inputs: List[Signal],
        materialized_output: Signal,
        hook_type: Type,
    ) -> None:
        timestamp_in_utc = self._current_timestamp_in_utc()
        self.metric("routing_table.receive.hook")[hook_type.__name__].emit(1)
        self.metric("routing_table.receive.hook", route=route_record.route)[hook_type.__name__].emit(1)
        self.metric("routing_table.receive.hook.time_in_utc", route=route_record.route)[hook_type.__name__].emit(timestamp_in_utc)
        if slot:
            params = dict(self._params)
            params.update(
                {
                    RoutingHookInterface.HOOK_TYPE_PARAM: hook_type,
                    "_route_record": route_record,
                    "_execution_ctx_id": execution_ctx_id,
                    "_materialized_inputs": materialized_inputs,
                    "_materialized_output": materialized_output,
                    "_current_timestamp_in_utc": timestamp_in_utc,
                }
            )
            try:
                code = loads(slot.code)
                if isinstance(code, str):
                    exec(code)
                else:
                    code(self, route_record, execution_ctx_id, materialized_inputs, materialized_output, timestamp_in_utc, **params)
            except Exception as err:
                stack_trace: str = traceback.format_exc()
                logger.critical(f"Inlined execution context hook failed!" f"Error:{str(err)}, Stack Trace:{stack_trace}")

    def _execute_compute_hook(
        self,
        slot: Slot,
        route_record: "RoutingTable.RouteRecord",  # persistent route object
        compute_record: "RoutingTable.ComputeRecord",
        hook_type: Type,
    ) -> None:
        timestamp_in_utc = self._current_timestamp_in_utc()
        self.metric("routing_table.receive.hook")[hook_type.__name__].emit(1)
        self.metric("routing_table.receive.hook", route=route_record.route)[hook_type.__name__].emit(1)
        self.metric("routing_table.receive.hook.time_in_utc", route=route_record.route)[hook_type.__name__].emit(timestamp_in_utc)
        if slot:
            params = dict(self._params)
            params.update(
                {
                    RoutingHookInterface.HOOK_TYPE_PARAM: hook_type,
                    "_route_record": route_record,
                    "_compute_record": compute_record,
                    "_current_timestamp_in_utc": timestamp_in_utc,
                }
            )
            try:
                code = loads(slot.code)
                if isinstance(code, str):
                    exec(code)
                else:
                    code(self, route_record, compute_record, timestamp_in_utc, **params)
            except Exception as err:
                stack_trace: str = traceback.format_exc()
                logger.critical(f"Inlined compute hook failed!" f"Error:{str(err)}, Stack Trace:{stack_trace}")

    def _execute_active_context_checkpoint_hook(
        self,
        slot: Slot,
        route_record: "RoutingTable.RouteRecord",  # persistent route object
        execution_ctx_id: str,
        active_compute_records: List["RoutingTable.ComputeRecord"],
        checkpoint_in_secs: int,
        hook_type: Type,
    ) -> None:
        timestamp_in_utc = self._current_timestamp_in_utc()
        self.metric("routing_table.receive.hook", route=route_record.route)[hook_type.__name__].emit(1)
        self.metric("routing_table.receive.hook.time_in_utc", route=route_record.route)[hook_type.__name__].emit(timestamp_in_utc)
        if slot:
            params = dict(self._params)
            params.update(
                {
                    RoutingHookInterface.HOOK_TYPE_PARAM: hook_type,
                    "_route_record": route_record,
                    "_execution_ctx_id": execution_ctx_id,
                    "_active_compute_records": active_compute_records,
                    "_checkpoint_in_secs": checkpoint_in_secs,
                    "_current_timestamp_in_utc": timestamp_in_utc,
                }
            )
            try:
                code = loads(slot.code)
                if isinstance(code, str):
                    exec(code)
                else:
                    code(self, route_record, execution_ctx_id, active_compute_records, checkpoint_in_secs, timestamp_in_utc, **params)
            except Exception as err:
                stack_trace: str = traceback.format_exc()
                logger.critical(f"Inlined execution context checkpoint hook failed!" f"Error:{str(err)}, Stack Trace:{stack_trace}")

    def _execute_pending_node_hook(
        self,
        slot: Slot,
        route_record: "RoutingTable.RouteRecord",  # persistent route object
        pending_node: "RuntimeLinkNode",
        hook_type: Type,
    ) -> None:
        timestamp_in_utc = self._current_timestamp_in_utc()
        self.metric("routing_table.receive.hook", route=route_record.route)[hook_type.__name__].emit(1)
        self.metric("routing_table.receive.hook.time_in_utc", route=route_record.route)[hook_type.__name__].emit(timestamp_in_utc)
        if slot:
            params = dict(self._params)
            params.update(
                {
                    RoutingHookInterface.HOOK_TYPE_PARAM: hook_type,
                    "_route_record": route_record,
                    "_pending_node": pending_node,
                    "_current_timestamp_in_utc": timestamp_in_utc,
                }
            )
            try:
                code = loads(slot.code)
                if isinstance(code, str):
                    exec(code)
                else:
                    code(self, route_record, pending_node, timestamp_in_utc, **params)
            except Exception as err:
                stack_trace: str = traceback.format_exc()
                logger.critical(f"Inlined pending node hook failed!" f"Error:{str(err)}, Stack Trace:{stack_trace}")

    def _execute_pending_node_checkpoint_hook(
        self,
        slot: Slot,
        route_record: "RoutingTable.RouteRecord",  # persistent route object
        pending_node: "RuntimeLinkNode",
        checkpoint_in_secs: int,
        hook_type: Type,
    ) -> None:
        timestamp_in_utc = self._current_timestamp_in_utc()
        self.metric("routing_table.receive.hook", route=route_record.route)[hook_type.__name__].emit(1)
        self.metric("routing_table.receive.hook.time_in_utc", route=route_record.route)[hook_type.__name__].emit(timestamp_in_utc)
        if slot:
            params = dict(self._params)
            params.update(
                {
                    RoutingHookInterface.HOOK_TYPE_PARAM: hook_type,
                    "_route_record": route_record,
                    "_pending_node": pending_node,
                    "_checkpoint_in_secs": checkpoint_in_secs,
                    "_current_timestamp_in_utc": timestamp_in_utc,
                }
            )
            try:
                code = loads(slot.code)
                if isinstance(code, str):
                    exec(code)
                else:
                    code(self, route_record, pending_node, checkpoint_in_secs, timestamp_in_utc, **params)
            except Exception as err:
                stack_trace: str = traceback.format_exc()
                logger.critical(f"Inlined pending node hook failed!" f"Error:{str(err)}, Stack Trace:{stack_trace}")

    @property
    def is_synchronized(self) -> bool:
        """Inform the rest of the Platform as to whether a routing impl supports synchronization
        of concurrent operations against <Route>s."""
        return True

    @abstractmethod
    def _lock(self, route_id: RouteID) -> None:
        pass

    @abstractmethod
    def _release(self, route_id: RouteID) -> None:
        pass

    @abstractmethod
    def _load(self, route_id: RouteID) -> RouteRecord:
        """Retrieve and lock the internal record for a route.
        If a record does not exist yet, create a new one and lock it.
        """
        ...

    def optimistic_read(self, route_id: RouteID) -> RouteRecord:
        return self._load(route_id)

    @abstractmethod
    def _save(self, route: RouteRecord, suppress_errors_and_emit: Optional[bool] = False) -> None:
        """Save the active route state.
        When 'suppress_errors_and_emit' is True, Routing is on a 'critical path' and exception unwinding is not
        allowed. Driver is expected to swallow the error and emit Routing internal metric."""
        ...

    @abstractmethod
    def _delete(self, routes: Set[Route]) -> None:
        ...

    @abstractmethod
    def _clear_all(self) -> None:
        ...

    @abstractmethod
    def _clear_active_routes(self) -> None:
        ...

    @abstractmethod
    def load_active_route_records(self) -> Iterator["RoutingTable.RouteRecord"]:
        ...

    @abstractmethod
    def query_inactive_records(
        self,
        slot_types: Set[SlotType],
        trigger_range: Tuple[int, int],
        deactivated_range: Tuple[int, int],
        elapsed_range: Tuple[int, int],
        response_states: Set[Enum],
        response_sub_states: Set[Enum],
        session_states: Set[Enum],
        session_sub_states: Set[Enum],
        ascending: bool = False,
    ) -> Iterator[Tuple[Route, "RoutingTable.ComputeRecord"]]:
        ...

    @abstractmethod
    def load_inactive_compute_records(self, route_id: RouteID, ascending: bool = True) -> Iterator["RoutingTable.ComputeRecord"]:
        """Load historical records from the secondary storage (where we will keep the historical records)"""
        ...

    @abstractmethod
    def _save_inactive_compute_records(
        self, route: Route, inactive_records: Sequence["RoutingTable.ComputeRecord"], suppress_errors_and_emit: Optional[bool] = False
    ) -> None:
        """Save historical records to the secondary storage (where we will keep the historical records).
        When 'suppress_errors_and_emit' is True, Routing is on a critical path and exception unwinding is not
        allowed. Driver is expected to swallow the error and emit Routing internal metric."""
        ...
