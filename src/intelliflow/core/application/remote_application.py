# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
import logging
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Type, Union, cast

from intelliflow.core.application.context.node.base import DataNode, Node
from intelliflow.core.application.context.node.marshaling.nodes import MarshalerNode
from intelliflow.core.entity import CoreData
from intelliflow.core.platform.development import Configuration, HostPlatform, RemotePlatform
from intelliflow.core.serialization import Serializable
from intelliflow.core.signal_processing import Signal
from intelliflow.core.signal_processing.signal_source import SignalSourceType

log = logging.getLogger("app." + __name__)


# TODO discuss changing name to "UpstreamApplication"
class RemoteApplication(Serializable["RemoteApplication"]):
    """Wrapper around a target/remote application which can be imported into another (host application)

    Aggregated Application object APIs are exposed in a way that they would make sense against a remote app.
    Functionalities such as new node (date, model, etc) addition and activation are hiden.
    """

    def __init__(self, id_: str, conf: Configuration, host_app: "Application") -> None:
        self._id = id_
        self._conf = conf
        self._remote_app = self._create_app(host_app.platform)

    @property
    def id(self) -> str:
        return self._id

    @property
    def platform(self) -> "DevelopmentPlatform":
        # let it fail if explicitly called on a transitive app
        return self._remote_app.platform

    @property
    def remote_app(self) -> "Application":
        return self._remote_app

    @property
    def is_transitive(self) -> bool:
        """Whether this remote app is a parent of a remote app (parent of the host/client application, i.e grand parent)?
        Framework does not attempt to load and expose those transitive app dependencies.
        Only the direct ancestors, one level only as can be observed in Context::_deserialized_init
        where we load remote apps of an application if it is the host/client application."""
        return not self._remote_app

    def _create_app(self, host_platform: HostPlatform) -> "Application":
        new_conf = copy.deepcopy(self._conf)
        new_conf.set_upstream(host_platform.conf)

        from intelliflow.core.application.application import Application

        return Application(self._id, RemotePlatform(new_conf), enforce_runtime_compatibility=True)

    def _serializable_copy_init(self, org_instance: "RemoteApplication") -> None:
        super()._serializable_copy_init(org_instance)
        self._remote_app = None

    def _deserialized_init(self, host_platform: HostPlatform) -> None:
        self._remote_app = self._create_app(host_platform)

    def __eq__(self, other: Union["RemoteApplication", Configuration]) -> bool:
        _id: "ApplicationID" = other.id if isinstance(other, RemoteApplication) else other.context_id
        conf: Configuration = other._remote_app.platform.conf if isinstance(other, RemoteApplication) else other
        return (
            self.id == _id
            and self.remote_app.platform.conf.__class__ == conf.__class__
            and self.remote_app.platform.conf._generate_unique_id_for_context() == conf._generate_unique_id_for_context()
        )

    def __hash__(self) -> int:
        return hash(self.id)

    @property
    def uuid(self) -> str:
        # let it fail if explicitly called on a transitive app
        return self._remote_app.uuid

    def map_as_external(self, signal: Signal) -> Signal:
        if signal.resource_access_spec.get_owner_context_uuid() == self.uuid and signal.resource_access_spec.source in [
            SignalSourceType.INTERNAL,
            SignalSourceType.INTERNAL_METRIC,
            SignalSourceType.INTERNAL_ALARM,
            SignalSourceType.INTERNAL_COMPOSITE_ALARM,
        ]:
            if signal.resource_access_spec.source == SignalSourceType.INTERNAL:
                mapped_signal = self._remote_app.platform.storage.map_internal_signal(signal)
            elif signal.resource_access_spec.source in [
                SignalSourceType.INTERNAL_ALARM,
                SignalSourceType.INTERNAL_METRIC,
                SignalSourceType.INTERNAL_COMPOSITE_ALARM,
            ]:
                mapped_signal = self._remote_app.platform.diagnostics.map_internal_signal(signal)

            # mapped signal should still retain the owner context information
            # which is within its resource access spec
            mapped_signal.resource_access_spec.set_as_mapped_from_upstream()
            return mapped_signal

    # overrides
    def refresh(self) -> None:
        if not self.is_transitive:
            self._remote_app.refresh()

    def query_data(self, query: str) -> Mapping[str, Node]:
        if not self.is_transitive:
            return self._remote_app.query_data(
                query, self._remote_app.QueryApplicationScope.CURRENT_APP_ONLY, self._remote_app.QueryContext.ACTIVE_RUNTIME_CONTEXT
            )

    def query(self, query_visitors: Sequence[Node.QueryVisitor]) -> Optional[Mapping[str, Node]]:
        if not self.is_transitive:
            return self._remote_app.query(
                query_visitors,
                self._remote_app.QueryApplicationScope.CURRENT_APP_ONLY,
                self._remote_app.QueryContext.ACTIVE_RUNTIME_CONTEXT,
            )

    def get_data(self, data_id: str) -> Optional[List[MarshalerNode]]:
        if not self.is_transitive:
            return self._remote_app.get_data(
                data_id, self._remote_app.QueryApplicationScope.CURRENT_APP_ONLY, self._remote_app.QueryContext.ACTIVE_RUNTIME_CONTEXT
            )

    def get_timer(self, timer_id: str) -> Optional[List[MarshalerNode]]:
        if not self.is_transitive:
            return self._remote_app.get_timer(
                timer_id, self._remote_app.QueryApplicationScope.CURRENT_APP_ONLY, self._remote_app.QueryContext.ACTIVE_RUNTIME_CONTEXT
            )

    def list(self, node_type: Type[Node] = MarshalerNode) -> Optional[Iterable[Node]]:
        if not self.is_transitive:
            return self._remote_app.list(
                node_type, self._remote_app.QueryApplicationScope.CURRENT_APP_ONLY, self._remote_app.QueryContext.ACTIVE_RUNTIME_CONTEXT
            )

    def list_data(self) -> Optional[Sequence[MarshalerNode]]:
        if not self.is_transitive:
            return self._remote_app.list_data(
                self._remote_app.QueryApplicationScope.CURRENT_APP_ONLY, self._remote_app.QueryContext.ACTIVE_RUNTIME_CONTEXT
            )

    def __getitem__(self, entity_id) -> MarshalerNode:
        # FUTURE when other node types are introduced call their respective retrieval API
        # ex: model(model_id)
        marshalers = self.get_data(entity_id)
        if marshalers:
            if len(marshalers) > 1:
                raise ValueError(
                    f"Cannot resolve entity! Entity '{entity_id}' maps two multiple <DataNode>s in the remote application!"
                    f"Number of mapped entities: {len(marshalers)}, entities: {marshalers}"
                )
            return marshalers[0]

        marshalers = self.get_timer(entity_id)
        if marshalers:
            if len(marshalers) > 1:
                raise ValueError(
                    f"Cannot resolve entity! Entity '{entity_id}' maps two multiple <TimerNode>s in the remote application!"
                    f"Number of mapped entities: {len(marshalers)}, entities: {marshalers}"
                )
            return marshalers[0]

        raise ValueError(f"Cannot find any existing node with ID '{entity_id}' within the remote application '{self._id}'")


class DownstreamApplication(CoreData):
    """Class that has the minimum state [only ID and platform info] of a remote application.

    It used in the context of granting 'import' permissions to other applications where
    instantiation/loading of the target application might not be possible.
    """

    def __init__(self, id: str, conf: Configuration) -> None:
        self.id = id
        self.conf = conf

    def __eq__(self, other: "DownstreamApplication") -> bool:
        return (
            self.id == other.id
            and self.conf.__class__ == other.conf.__class__
            and self.conf._generate_unique_id_for_context() == other.conf._generate_unique_id_for_context()
        )

    def __hash__(self) -> int:
        return hash(self.id)
