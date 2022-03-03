# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
import json
from typing import Any, Dict, List, Optional, Sequence

from intelliflow.core.application.context.node.filtered_views import FilteredView
from intelliflow.core.platform.development import HostPlatform
from intelliflow.core.signal_processing import DimensionFilter, Signal, Slot
from intelliflow.core.signal_processing.dimension_constructs import Dimension, DimensionSpec, RawDimensionFilterInput
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal import SignalDimensionTuple, SignalDomainSpec, SignalLinkNode, SignalProvider

from ..base import Node
from ..internal import MonitoringConfig
from ..internal.nodes import MonitoringView


class MarshalerNode(Node, SignalProvider):
    """TODO refer doc

    Underlying node impls exposes themselves to user-level code via this node which provides a unified API across almost
    all of the high-level, generic operations in the framework.
    """

    def __init__(
        self,
        bound: Node,
        desc: str
        # TODO extended marshaling support
    ) -> None:
        if bound is None:
            raise RuntimeError("'bound' is None. MarshalerNode should be bound to a Node")
        super().__init__(bound)
        self._bound = bound
        self._desc = desc

    @property
    def bound(self) -> Node:
        return self._bound

    # overrides
    @property
    def _id(self) -> str:
        return self.bound._id

    def __call__(self, *dimension_name: Sequence[str]) -> SignalDimensionTuple:
        """Convenience method wrapping `dimension()`"""
        return self.dimension(*dimension_name)

    def dimension(self, *dimensions: Sequence[str]) -> SignalDimensionTuple:
        signal: Signal = self._bound.signal().clone(None)
        dims: List[Dimension] = []
        for dimension_name in dimensions:
            dim: Optional[Dimension] = signal.domain_spec.dimension_spec.find_dimension_by_name(dimension_name)
            if not dim:
                raise ValueError(f"Cannot find dimension {dimension_name} in {signal!r}")
            dims.append(dim)
        return SignalDimensionTuple(signal, *dims)

    def as_reference(self) -> "MarshalingView":
        return MarshalingView(self, self.signal(), None).as_reference()

    def reference(self) -> "MarshalingView":
        return self.as_reference()

    @property
    def ref(self) -> "MarshalingView":
        return self.as_reference()

    def range_check(self, enabled: Optional[bool] = True) -> "MarshalingView":
        return MarshalingView(self, self.signal(), None).range_check(enabled)

    def check_range(self) -> "MarshalingView":
        return self.range_check(True)

    def nearest(self) -> "MarshalingView":
        return MarshalingView(self, self.signal(), None).nearest()

    def latest(self) -> "MarshalingView":
        return self.nearest()

    def filter(self, new_filter: RawDimensionFilterInput, new_alias: str) -> Signal:
        """Build a new filtered version of the signal this node is tracking / bound to"""
        return self._bound.signal().chain(new_filter, new_alias)

    def __invert__(self) -> Any:
        """Overwrite bitwise not for high-level convenient operations between Signals."""
        return ~self.signal()

    def __and__(self, other) -> Any:
        """Overwrite bitwise & for high-level operations operations between Signals."""
        return self.signal() & other

    def __or__(self, other) -> Any:
        """Overwrite bitwise | for high-level operations operations between Signals."""
        return self.signal() | other

    def __getitem__(self, slice_filter) -> "MarshalingView":
        """Convenience indexing method for high-level modules to build new filters for a Signal.

        Please see :class:`FilteredView` :: `__getitem__` for more details.

        This initiates the first level for the target filter and returns a FilteredView.
        After that, nested levels of the filter can be created by subsequent (cascaded) calls
        to FilterView chain.

        Parameters
        ----------
        slice_filter: a single value or a slice (i.e [start:end] without step) or multiple slices (numpy style).

        Returns
        -------
        :class:`FilteredView` : a new filtered view wrapping the same source `Signal` (copy semantics)
        """
        if self.signal().domain_spec.dimension_filter_spec:
            dimension = next(iter(self.signal().domain_spec.dimension_spec.get_flattened_dimension_map().keys()))
            return MarshalingView(self, self.signal(), FilteredView.get_raw_filtering_input(slice_filter, dimension))
        else:
            raise ValueError(f"Cannot create a filtered view on a Signal with no dimensions! Signal: {self.signal()!r}")

    # overrides
    def do_activate(self, platform: HostPlatform) -> None:
        """TODO post-MVP
        auto-generate marshaler/listener code to extract our metadata/marshalling
        info from the input data node.
        platform.connect_internal(Route(self._node_id,
                                  self._signal_link_node,
                                  self._output_signal,
                                  self._output_dim_matrix,
                                  self._slots(),
                                  self._auto_backfilling_enabled))
        """
        pass

    # overrides
    def signal(self) -> Optional[Signal]:
        return self._bound.signal()

    # overrides SignalProvider::get_signal
    def get_signal(self, alias: str = None) -> Signal:
        signal = self._bound.signal()
        return signal.clone(signal.alias if alias is None else alias)

    # overrides
    def _slots(self) -> Optional[List[Slot]]:
        pass

    def add_monitor(self, filter: DimensionFilter, config: MonitoringConfig):
        # TODO post-MVP create a child MonitoringNode.
        #  - (?) append another MarshalerNode to it, and return that new marshaler.
        pass

    def describe(self) -> str:
        # TODO merge with FilteredView::stats. Move to a common node analyzer module.
        signal: Signal = self._bound.signal()
        stats = {"id/alias": signal.alias, "source_type": signal.resource_access_spec.source.value}
        if signal.type.is_metric():
            stats.update({"metric_stats": signal.resource_access_spec.create_stats_from_filter(signal.domain_spec.dimension_filter_spec)})
        else:
            stats.update(
                {
                    "dimensions": [
                        (dim_name, dim.type, dim.params)
                        for dim_name, dim in signal.domain_spec.dimension_spec.get_flattened_dimension_map().items()
                    ],
                    "current_filter_values": signal.domain_spec.dimension_filter_spec.pretty(),
                }
            )
        return json.dumps(stats, indent=10, default=repr)

    def access_spec(self) -> Dict[str, Any]:
        signal: Signal = self._bound.signal()
        return {
            "id": self._bound._id,
            "source_type": signal.resource_access_spec.source.value,
            "path_format": signal.resource_access_spec.path_format,
            "attributes": copy.deepcopy(signal.resource_access_spec.attrs),
        }

    def attributes(self) -> Dict[Any, Any]:
        return self.access_spec()["attributes"]

    def metadata(self) -> str:
        return json.dumps(self.attributes(), indent=6)

    ''' TODO enable this functionality once there is an workaround for calls onto this from serialization.
    def __getattr__(self, item) -> Any:
        """Enables convenient access into attributes/metadata.
        Gets called whenever an item is not found via __getattribute__

        Example (based on user defined / custom metadata:
            my_data.CTI
            my_data.partition_keys
            my_data.columns
        """
        attrs = self.attributes()
        for key, value in attrs.items():
            if item.lower() == key.lower():
                return value

        raise AttributeError(f"item {item!r} cannot be found in {json.dumps(attrs, indent=6)}")
    '''

    def domain_spec(self) -> SignalDomainSpec:
        return copy.deepcopy(self._bound.signal().domain_spec)

    def dimension_spec(self) -> DimensionSpec:
        return copy.deepcopy(self._bound.signal().domain_spec.dimension_spec)

    def dimension_filter_spec(self) -> DimensionFilter:
        return copy.deepcopy(self._bound.signal().domain_spec.dimension_filter_spec)

    def paths(self) -> List[str]:
        return self._bound.signal().get_materialized_resource_paths()

    def path_format(self) -> str:
        return self._bound.signal().resource_access_spec.path_format

    def dimensions(self) -> str:
        """render human-readable dimension spec in name:type pairs and also render dimension filter."""
        return json.dumps(
            {
                "dimensions": [
                    (dim_name, dim.type, dim.params)
                    for dim_name, dim in self._bound.signal().domain_spec.dimension_spec.get_flattened_dimension_map().items()
                ],
                "current_filter_values": self._bound.signal().domain_spec.dimension_filter_spec.pretty(),
            },
            indent=11,
        )

    def partitions(self) -> str:
        return self.dimensions()

    def dimension_values(self) -> str:
        return json.dumps(self._bound.signal().domain_spec.dimension_filter_spec.pretty(), indent=10)

    def partition_values(self) -> str:
        return self.dimension_values()


class MarshalingView(FilteredView):
    def __init__(self, marshaler_node: MarshalerNode, signal: Signal, filtering_levels: RawDimensionFilterInput) -> None:
        self._marshaler_node = marshaler_node
        super().__init__(signal, filtering_levels)

    @property
    def marshaler_node(self) -> MarshalerNode:
        return self._marshaler_node

    # overrides
    def _chain(self, signal: Signal, filtering_levels: RawDimensionFilterInput) -> "FilterView":
        return self.__class__(self._marshaler_node, signal, filtering_levels)

    def is_ready(self) -> bool:
        target_resource_range: Signal = self.get_signal()
        # TODO post-MVP
        # using the integrity check protocol from the signal
        # check each
        # target_resource_range.get_materialized_access_specs()
        pass

    def add_monitoring(self) -> MonitoringView:
        # TODO post-MVP
        # add
        pass
