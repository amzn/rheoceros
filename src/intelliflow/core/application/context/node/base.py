# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Mapping, Optional, Sequence, Type, cast
from uuid import uuid4

from intelliflow.core.application.context.traversal import ContextVisitor
from intelliflow.core.platform.development import HostPlatform
from intelliflow.core.serialization import Serializable, dumps, loads
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import (
    ALARM_STATE_TRANSITION_DIMENSION_TYPE,
    ALARM_TIME_DIMENSION_TYPE,
    METRIC_NAME_DIMENSION_TYPE,
    METRIC_PERIOD_DIMENSION_TYPE,
    METRIC_STATISTIC_DIMENSION_TYPE,
    METRIC_TIME_DIMENSION_TYPE,
    AlarmDimension,
    MetricDimension,
    MetricSubDimensionMapType,
)
from intelliflow.core.signal_processing.dimension_constructs import DateVariant, Dimension, DimensionFilter, DimensionSpec
from intelliflow.core.signal_processing.signal import Signal, SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import (
    AlarmSignalSourceAccessSpec,
    CompositeAlarmSignalSourceAccessSpec,
    MetricSignalSourceAccessSpec,
    SignalSourceAccessSpec,
    TimerSignalSourceAccessSpec,
)
from intelliflow.core.signal_processing.slot import Slot


class Node(Serializable["Node"], ABC):
    """Abstract class representing an atomic signal/slot provider within Application flow.

    A Node generically represents a persistent action taken against an entity flowing through
    an Application. This entities are represented with Signal abstraction. So basically a Node
    is at least a transformation or a listener on a Signal. As a transformation, it should
    definitely provide a Signal, and as a listener it should be taking some persistent action
    and yielding new data in Platform::storage, which cannot be exposed as a Signal to the rest
    of the flow.

    So it should have at least one Slot and optionally expose a Signal. Because of this fact,
    a Node should register its connections (input Signals and at least one Slot) during Activation
    so that in runtime its Slot(s) can be executed, based on changes in its input Signal(s).

    Obeys Serializable protocol to guarantee serialization at Context level.
    """

    class QueryVisitor(ContextVisitor):
        """Node impls can (are supposed to) provide QueryVisitor impls to abstract high-level modules
        from the details of how to build a query for a particular node type (and its attributes)"""

        def __init__(self, node_type: Type["Node"], *args, **kwargs) -> None:
            """Impls should provide a pretty interface for more convenient high-level instantiation a query visitor"""
            self._results: Dict[str, "Node"] = dict()
            self._args: Sequence[Any] = list(args) if args else []
            self._kwargs: Dict[str, Any] = dict(kwargs) if kwargs else {}
            self._node_type = node_type if node_type else kwargs.get("node_type", None)

        @property
        def results(self) -> Mapping[str, "Node"]:
            return self._results

        # overrides
        def visit(self, node: "Node") -> None:
            if self.match(node):
                self._results[node.node_id] = node

        def match(self, node: "Node") -> bool:
            return type(node) == self._node_type or isinstance(node, self._node_type)

    # parent and child_nodes strong/direct references would normally mean memory leak here.
    # currently this issue is intentionally ignored based on the particular use-cases that
    # we are targeting for POC (as of 07/2020).
    # TODO Will move to 'weakref' or ignore it at all, depepending on our FUTURE evaluation.

    def __init__(self, parent_node: "Node", child_nodes: List["Node"] = None, node_id: str = None) -> None:
        """
        Create a Node.

        Parameters
        ----------
        parent_node: Node
            Container Node. Owner/creator of this Node. Not to be confused with an upstream relationship.
            That type of relationship will be up to sub-classes / Node implementations.
        node_id: str
            Unique ID for this Node. Within a Context this id is used to retrieve the handle for this Node.
        *child_node : Node
            Children.
        """

        self._parent_node = parent_node
        self._child_nodes = [] if child_nodes is None else child_nodes
        # don't forget that our serializer should not call __init__ on new objects during deserialization.
        # so this assignment is safe for now since we are using pickle. Pickle does not call __init__.
        self._node_id = "{0}-{1}".format(self.__class__.__name__, uuid4()) if node_id is None else node_id
        self._check_cylic_dependency()

    def _check_cylic_dependency(self) -> None:
        if self._parent_node == self or self in self._child_nodes:
            raise TypeError("Node ({0}) has a cyclic relationship")
        # TODO check the whole hierarchy

    def __hash__(self) -> int:
        return hash(self._node_id)

    def __eq__(self, other: Any) -> bool:
        try:
            return self._node_id == cast("Node", other).node_id
        except AttributeError:
            return NotImplemented

    @property
    def parent_node(self) -> "Node":
        return self._parent_node

    @parent_node.setter
    def parent_node(self, value: "Node") -> None:
        self._parent_node = value

    @property
    def node_id(self) -> str:
        return self._node_id

    @abstractmethod
    def _id(self) -> str:
        pass

    @property
    def route_id(self) -> str:
        return self._node_id

    def is_root(self) -> bool:
        return self._parent_node is None

    @property
    def child_nodes(self) -> Sequence["Node"]:
        return self._child_nodes

    def add_child(self, child: "Node") -> None:
        child.parent_node = self
        self._child_nodes.append(child)

    @abstractmethod
    def signal(self) -> Optional[Signal]:
        """What does this Node represent?

        Emit it as a Signal during development and use the same during 'activation'
        This method is particularly important while Nodes are referring each other.
        While a Node binds to another one (like MarshalerNode to a DataNode) or when
        a Node is implicitly used as an input for another Node (i.e Application::createDataset),
        this method provides the necessary abstraction in terms of representing this particular
        Node impl (a subclass) as a Signal object.
        """
        pass

    @abstractmethod
    def _slots(self) -> Optional[List[Slot]]:
        """A Node is supposed to provide at least one Slot.

        if a Slot is not going to be provided, then the existence of Node impl
        should be questioned as it can only be a development-time entity such
        as the views in "node.marshaling.filter_views" module. Currently only
        exception to this logic is <ExternalDataNode>. That is why the return
        type is still Optional. Otherwise this would not make sense.

        The reason this method is supposed to be 'private' is that currently
        we cannot envision a scenario where the slots would be accessed
        externally. So sole purpose of this abstract method is to high-light
        the above logic behind having Node impls.
        """
        pass

    def activate(self, platform: HostPlatform) -> None:
        """Do activation while the high-level Context (and Application) is being interpreted/activated.

        Node activates itself and then scans its children.
        This basic logic is what determines the order of route entries (i.e Code Segment)
        within a DAG of Nodes (that represents an <Instruction>). High-level order of route entries
        (aligned with the flow of Application code) is determined by :class:`InstructionChain` .
        """
        self.do_activate(platform)
        for child_node in self._child_nodes:
            child_node.activate(platform)

    @abstractmethod
    def do_activate(self, platform: HostPlatform) -> None:
        pass

    def accept(self, visitor: ContextVisitor) -> None:
        visitor.visit(self)
        for child_node in self._child_nodes:
            child_node.accept(visitor)


class DataNode(Node, ABC):
    """Base class for all Data (new Signal) provider Nodes.

    It encapsulates bare-bones fields for the creation of a new Signal.
    """

    class QueryVisitor(Node.QueryVisitor):
        def __init__(self, data_id: str, *args, **kwargs) -> None:
            super().__init__(None, args, kwargs)
            self._data_id: str = data_id if data_id else kwargs.get("data_id", None)
            self._exact_match: bool = kwargs.get("exact_match", False)

        # overrides
        def match(self, node: "Node") -> bool:
            if isinstance(node, DataNode):
                if self._exact_match:
                    return self._data_id == node.data_id
                else:
                    return self._data_id in node.data_id

            return False

    def __init__(
        self,
        data_id: str,
        source_access_spec: SignalSourceAccessSpec,
        domain_spec: SignalDomainSpec,
        parent_node: Node = None,
        child_nodes: List[Node] = None,
        node_id: str = None,
    ) -> None:
        """
        Create a DataNode.

        Parameters
        ----------
        data_id: str
            Identifier for the logical data object represented by this Node.
        resource_format: str
            See Signal::resource_format
        domain_spec: SignalDomainSpec
            See Signal::domain_spec
        parent_node: Node
            Container Node. Owner/creator of this Node. Not to be confused with an upstream relationship.
            That type of relationship will be up to sub-classes / Node implementations.
        node_id: str
            Unique ID for this Node. Within a Context this id is used to retrieve the handle for this Node.
        *child_node : Node
            Children.
        """

        super().__init__(parent_node, child_nodes, node_id)
        self._data_id = data_id
        self._source_access_spec = source_access_spec
        self._domain_spec = domain_spec

    # overrides
    @property
    def _id(self) -> str:
        return self.data_id

    @property
    def data_id(self) -> str:
        return self._data_id

    # overrides
    @property
    def route_id(self) -> str:
        # It is by design that we allow the same route_id generation for the same data_id.
        # It is the same scoping semantics that Python uses, which allows the most recent node to
        # overwrite / update the route (during activation).
        return "{0}-{1}".format(self.__class__.__name__, self._data_id)


class TimerNode(Node):
    class QueryVisitor(Node.QueryVisitor):
        def __init__(self, timer_id: str, *args, **kwargs) -> None:
            super().__init__(None, args, kwargs)
            self._timer_id: str = timer_id if timer_id else kwargs.get("timer_id", None)
            self._exact_match: bool = kwargs.get("exact_match", False)

        # overrides
        def match(self, node: "Node") -> bool:
            if isinstance(node, TimerNode):
                if self._exact_match:
                    return self._timer_id == node.timer_id
                else:
                    return self._timer_id in node.timer_id

            return False

    def __init__(
        self,
        timer_id: str,
        schedule_expression: str,
        date_dimension: DateVariant,
        context_id: str,
        parent_node: Node = None,
        child_nodes: List[Node] = None,
        node_id: str = None,
        **kwargs,
    ) -> None:
        super().__init__(parent_node, child_nodes, node_id)
        self._timer_id = timer_id
        self._schedule_expresssion = schedule_expression

        spec: DimensionSpec = DimensionSpec()
        spec.add_dimension(Dimension(date_dimension.name, date_dimension.type, date_dimension.params), None)

        dim_filter: DimensionFilter = DimensionFilter()
        dim_filter.add_dimension(date_dimension, None)

        source_access_spec = TimerSignalSourceAccessSpec(timer_id, schedule_expression, context_id, **kwargs)

        domain_spec = SignalDomainSpec(spec, dim_filter, None)

        self._timer_signal = Signal(SignalType.TIMER_EVENT, source_access_spec, domain_spec, timer_id, False)

    def signal(self) -> Optional[Signal]:
        return self._timer_signal

    def _slots(self) -> Optional[List[Slot]]:
        return []

    def do_activate(self, platform: HostPlatform) -> None:
        platform.connect_internal_signal(self._timer_signal)

    @property
    def timer_id(self) -> str:
        return self._timer_id

    # overrides
    @property
    def _id(self) -> str:
        return self.timer_id


class MetricNode(Node, ABC):
    class QueryVisitor(Node.QueryVisitor):
        def __init__(self, metric_id: str, sub_dimensions: MetricSubDimensionMapType, *args, **kwargs) -> None:
            super().__init__(None, *args, **kwargs)
            self._metric_id: str = metric_id if metric_id else kwargs.get("metric_id", None)
            self._sub_dimensions = sub_dimensions
            self._exact_match: bool = kwargs.get("exact_match", False)

        # overrides
        def match(self, node: "Node") -> bool:
            if isinstance(node, MetricNode):
                id_match = True
                sub_dims_match = True
                if self._metric_id:
                    if self._exact_match:
                        id_match = self._metric_id == node.metric_id
                    else:
                        id_match = self._metric_id in node.metric_id

                if id_match:
                    if self._sub_dimensions:
                        sub_dims_match = all([node.sub_dimensions.get(key, None) == value for key, value in self._sub_dimensions.items()])
                return id_match and sub_dims_match

            return False

    def __init__(
        self,
        signal_type: SignalType,
        metric_id: str,
        dimension_filter_spec: DimensionFilter,
        source_access_spec: MetricSignalSourceAccessSpec,
        parent_node: Node = None,
        child_nodes: List[Node] = None,
        node_id: str = None,
    ) -> None:
        super().__init__(parent_node, child_nodes, node_id)
        self._metric_id = metric_id
        self._source_access_spec = source_access_spec

        dimension_spec: DimensionSpec = DimensionSpec.load_from_pretty(
            {
                MetricDimension.NAME: {
                    type: METRIC_NAME_DIMENSION_TYPE,
                    MetricDimension.STATISTIC: {
                        type: METRIC_STATISTIC_DIMENSION_TYPE,
                        MetricDimension.PERIOD: {
                            type: METRIC_PERIOD_DIMENSION_TYPE,
                            MetricDimension.TIME: {type: METRIC_TIME_DIMENSION_TYPE, "format": "%Y-%m-%d %H:%M:%S"},
                        },
                    },
                }
            }
        )

        if not dimension_filter_spec.check_spec_match(dimension_spec):
            raise ValueError(
                f"Metric (id={metric_id}: Dimension filter {dimension_filter_spec!r} is not compatible "
                f"with the expected dimension spec {dimension_spec!r}."
            )

        dimension_filter_spec.set_spec(dimension_spec)

        domain_spec = SignalDomainSpec(dimension_spec, dimension_filter_spec, None)

        self._metric_signal = Signal(signal_type, source_access_spec, domain_spec, metric_id)

    def signal(self) -> Optional[Signal]:
        return self._metric_signal

    def _slots(self) -> Optional[List[Slot]]:
        raise NotImplemented

    @property
    def metric_id(self) -> str:
        return self._metric_id

    @property
    def sub_dimensions(self) -> MetricSubDimensionMapType:
        return self._source_access_spec.sub_dimensions

    # overrides
    @property
    def _id(self) -> str:
        return self.metric_id


class AlarmNode(Node, ABC):
    class QueryVisitor(Node.QueryVisitor):
        def __init__(self, alarm_id: str, *args, **kwargs) -> None:
            super().__init__(None, *args, **kwargs)
            self._alarm_id: str = alarm_id if alarm_id else kwargs.get("alarm_id", None)
            self._exact_match: bool = kwargs.get("exact_match", False)

        # overrides
        def match(self, node: "Node") -> bool:
            if isinstance(node, AlarmNode):
                if self._exact_match:
                    return self._alarm_id == node.alarm_id
                else:
                    return self._alarm_id in node.alarm_id

            return False

    def __init__(
        self,
        signal_type: SignalType,
        alarm_id: str,
        dimension_filter_spec: DimensionFilter,
        source_access_spec: AlarmSignalSourceAccessSpec,
        parent_node: Node = None,
        child_nodes: List[Node] = None,
        node_id: str = None,
    ) -> None:
        super().__init__(parent_node, child_nodes, node_id)
        self._alarm_id = alarm_id
        self._source_access_spec = source_access_spec

        dimension_spec: DimensionSpec = DimensionSpec.load_from_pretty(
            {
                AlarmDimension.STATE_TRANSITION: {
                    type: ALARM_STATE_TRANSITION_DIMENSION_TYPE,
                    AlarmDimension.TIME: {type: ALARM_TIME_DIMENSION_TYPE, "format": "%Y-%m-%d %H:%M:%S"},
                }
            }
        )

        if not dimension_filter_spec.check_spec_match(dimension_spec):
            raise ValueError(
                f"Alarm (id={alarm_id}: Dimension filter {dimension_filter_spec!r} is not compatible "
                f"with the expected dimension spec {dimension_spec!r}."
            )

        dimension_filter_spec.set_spec(dimension_spec)

        domain_spec = SignalDomainSpec(dimension_spec, dimension_filter_spec, None)

        self._alarm_signal = Signal(signal_type, source_access_spec, domain_spec, alarm_id)

    def signal(self) -> Optional[Signal]:
        return self._alarm_signal

    def _slots(self) -> Optional[List[Slot]]:
        raise NotImplemented

    @property
    def alarm_id(self) -> str:
        return self._alarm_id

    # overrides
    @property
    def _id(self) -> str:
        return self.alarm_id


class CompositeAlarmNode(Node, ABC):
    class QueryVisitor(Node.QueryVisitor):
        def __init__(self, alarm_id: str, *args, **kwargs) -> None:
            super().__init__(None, *args, **kwargs)
            self._alarm_id: str = alarm_id if alarm_id else kwargs.get("alarm_id", None)
            self._exact_match: bool = kwargs.get("exact_match", False)

        # overrides
        def match(self, node: "Node") -> bool:

            if isinstance(node, CompositeAlarmNode):
                if self._exact_match:
                    return self._alarm_id == node.alarm_id
                else:
                    return self._alarm_id in node.alarm_id

            return False

    def __init__(
        self,
        signal_type: SignalType,
        alarm_id: str,
        dimension_filter_spec: DimensionFilter,
        source_access_spec: CompositeAlarmSignalSourceAccessSpec,
        parent_node: Node = None,
        child_nodes: List[Node] = None,
        node_id: str = None,
    ) -> None:
        super().__init__(parent_node, child_nodes, node_id)
        self._alarm_id = alarm_id
        self._source_access_spec = source_access_spec

        dimension_spec: DimensionSpec = DimensionSpec.load_from_pretty(
            {
                AlarmDimension.STATE_TRANSITION: {
                    type: ALARM_STATE_TRANSITION_DIMENSION_TYPE,
                    AlarmDimension.TIME: {type: ALARM_TIME_DIMENSION_TYPE, "format": "%Y-%m-%d %H:%M:%S"},
                }
            }
        )

        if not dimension_filter_spec.check_spec_match(dimension_spec):
            raise ValueError(
                f"Composite alarm (id={alarm_id}: Dimension filter {dimension_filter_spec!r} is not "
                f"compatible with the expected dimension spec {dimension_spec!r}."
            )

        dimension_filter_spec.set_spec(dimension_spec)

        domain_spec = SignalDomainSpec(dimension_spec, dimension_filter_spec, None)

        self._alarm_signal = Signal(signal_type, source_access_spec, domain_spec, alarm_id)

    def signal(self) -> Optional[Signal]:
        return self._alarm_signal

    def _slots(self) -> Optional[List[Slot]]:
        raise NotImplemented

    @property
    def alarm_id(self) -> str:
        return self._alarm_id

    # overrides
    @property
    def _id(self) -> str:
        return self.alarm_id
