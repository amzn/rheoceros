# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import Any, Dict

from intelliflow.core.application.context.node.base import MetricNode
from intelliflow.core.platform.development import HostPlatform
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import MetricSubDimensionMapType
from intelliflow.core.signal_processing.dimension_constructs import DimensionFilter
from intelliflow.core.signal_processing.signal import SignalType
from intelliflow.core.signal_processing.signal_source import CWMetricSignalSourceAccessSpec, SignalSourceAccessSpec


class ExternalMetricNode(MetricNode):
    class Descriptor(ABC):
        def __init__(self, **kwargs) -> None:
            self._data_kwargs: Dict[str, Any] = dict(kwargs)

        def add(self, key: str, value: Any) -> None:
            self._data_kwargs[key] = value

        @abstractmethod
        def create_node(
            self, metric_id: str, dimension_filter_spec, sub_dimensions: MetricSubDimensionMapType, platform: HostPlatform
        ) -> "ExternalMetricNode":
            pass

        @abstractmethod
        def create_source_spec(self, sub_dimensions: MetricSubDimensionMapType) -> SignalSourceAccessSpec:
            pass

    def __init__(self, desc: Descriptor, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._descriptor = desc

    # overrides
    def do_activate(self, platform: HostPlatform) -> None:
        platform.connect_external([self.signal().clone(None)])


class CWMetricNode(ExternalMetricNode):
    class Descriptor(ExternalMetricNode.Descriptor):
        def __init__(self, namespace: str, **kwargs) -> None:
            super().__init__(**kwargs)
            self._namespace = namespace

        # overrides
        def create_node(
            self, metric_id: str, dimension_filter_spec: DimensionFilter, sub_dimensions: MetricSubDimensionMapType, platform: HostPlatform
        ) -> ExternalMetricNode:
            return CWMetricNode(self, metric_id, dimension_filter_spec, sub_dimensions)

        # overrides
        def create_source_spec(self, sub_dimensions: MetricSubDimensionMapType) -> SignalSourceAccessSpec:
            return CWMetricSignalSourceAccessSpec(self._namespace, sub_dimensions, **self._data_kwargs)

    def __init__(
        self, desc: Descriptor, metric_id: str, dimension_filter_spec: DimensionFilter, sub_dimensions: MetricSubDimensionMapType
    ) -> None:
        cw_metric_source_access_spec = desc.create_source_spec(sub_dimensions)
        super().__init__(desc, SignalType.CW_METRIC_DATA_CREATION, metric_id, dimension_filter_spec, cw_metric_source_access_spec)
