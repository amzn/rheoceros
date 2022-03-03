# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Sequence, Union

from intelliflow.core.application.context.node.base import AlarmNode, CompositeAlarmNode
from intelliflow.core.platform.development import HostPlatform
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import AlarmParams, CompositeAlarmParams
from intelliflow.core.signal_processing.dimension_constructs import DimensionFilter
from intelliflow.core.signal_processing.signal import SignalType
from intelliflow.core.signal_processing.signal_source import (
    CWAlarmSignalSourceAccessSpec,
    CWCompositeAlarmSignalSourceAccessSpec,
    SignalSourceAccessSpec,
)


class ExternalAlarmNode(AlarmNode):
    class Descriptor(ABC):
        def __init__(self, **kwargs) -> None:
            self._data_kwargs: Dict[str, Any] = dict(kwargs)

        def add(self, key: str, value: Any) -> None:
            self._data_kwargs[key] = value

        @abstractmethod
        def create_node(
            self, alarm_id: str, dimension_filter_spec: DimensionFilter, alarm_params: AlarmParams, platform: HostPlatform
        ) -> "ExternalAlarmNode":
            pass

        @abstractmethod
        def create_source_spec(self, alarm_id: str, alarm_params: AlarmParams) -> SignalSourceAccessSpec:
            pass

    def __init__(self, desc: Descriptor, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._descriptor = desc

    # overrides
    def do_activate(self, platform: HostPlatform) -> None:
        platform.connect_external([self.signal().clone(None)])


class CWAlarmNode(ExternalAlarmNode):
    class Descriptor(ExternalAlarmNode.Descriptor):
        def __init__(self, account_id: str, region_id: str, **kwargs) -> None:
            super().__init__(**kwargs)
            self._account_id = account_id
            self._region_id = region_id

        # overrides
        def create_node(
            self, alarm_id: str, dimension_filter_spec: DimensionFilter, alarm_params: AlarmParams, platform: HostPlatform
        ) -> ExternalAlarmNode:
            return CWAlarmNode(self, alarm_id, dimension_filter_spec, alarm_params)

        # overrides
        def create_source_spec(self, alarm_id: str, alarm_params: AlarmParams) -> SignalSourceAccessSpec:
            return CWAlarmSignalSourceAccessSpec(alarm_id, self._account_id, self._region_id, alarm_params, **self._data_kwargs)

    def __init__(self, desc: Descriptor, alarm_id: str, dimension_filter_spec: DimensionFilter, alarm_params: AlarmParams) -> None:
        cw_alarm_source_access_spec = desc.create_source_spec(alarm_id, alarm_params)
        super().__init__(desc, SignalType.CW_ALARM_STATE_CHANGE, alarm_id, dimension_filter_spec, cw_alarm_source_access_spec)


class ExternalCompositeAlarmNode(CompositeAlarmNode):
    class Descriptor(ABC):
        def __init__(self, **kwargs) -> None:
            self._data_kwargs: Dict[str, Any] = dict(kwargs)

        def add(self, key: str, value: Any) -> None:
            self._data_kwargs[key] = value

        @abstractmethod
        def create_node(
            self, alarm_id: str, dimension_filter_spec: DimensionFilter, alarm_params: CompositeAlarmParams, platform: HostPlatform
        ) -> "ExternalCompositeAlarmNode":
            pass

        @abstractmethod
        def create_source_spec(self, alarm_id: str, alarm_params: CompositeAlarmParams) -> SignalSourceAccessSpec:
            pass

    def __init__(self, desc: Descriptor, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._descriptor = desc

    # overrides
    def do_activate(self, platform: HostPlatform) -> None:
        platform.connect_external([self.signal().clone(None)])


class CWCompositeAlarmNode(ExternalCompositeAlarmNode):
    class Descriptor(ExternalCompositeAlarmNode.Descriptor):
        def __init__(self, account_id: str, region_id: str, **kwargs) -> None:
            super().__init__(**kwargs)
            self._account_id = account_id
            self._region_id = region_id

        # overrides
        def create_node(
            self, alarm_id: str, dimension_filter_spec: DimensionFilter, alarm_params: CompositeAlarmParams, platform: HostPlatform
        ) -> ExternalCompositeAlarmNode:
            return CWCompositeAlarmNode(self, alarm_id, dimension_filter_spec, alarm_params)

        # overrides
        def create_source_spec(self, alarm_id: str, alarm_params: CompositeAlarmParams) -> SignalSourceAccessSpec:
            return CWCompositeAlarmSignalSourceAccessSpec(alarm_id, self._account_id, self._region_id, alarm_params, **self._data_kwargs)

    def __init__(self, desc: Descriptor, alarm_id: str, dimension_filter_spec: DimensionFilter, alarm_params: CompositeAlarmParams) -> None:
        cw_composite_alarm_source_access_spec = desc.create_source_spec(alarm_id, alarm_params)
        super().__init__(desc, SignalType.CW_ALARM_STATE_CHANGE, alarm_id, dimension_filter_spec, cw_composite_alarm_source_access_spec)
