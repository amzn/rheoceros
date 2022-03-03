# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List, Union

from intelliflow.core.application.context.node.base import AlarmNode, CompositeAlarmNode
from intelliflow.core.platform.development import HostPlatform
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import (
    AlarmComparisonOperator,
    AlarmParams,
    AlarmTreatMissingData,
    CompositeAlarmParams,
    MetricExpression,
)
from intelliflow.core.signal_processing.dimension_constructs import DimensionFilter
from intelliflow.core.signal_processing.signal import SignalType
from intelliflow.core.signal_processing.signal_source import (
    InternalAlarmSignalSourceAccessSpec,
    InternalCompositeAlarmSignalSourceAccessSpec,
)


class InternalAlarmNode(AlarmNode):
    def __init__(self, alarm_id: str, dimension_filter_spec: DimensionFilter, alarm_params: AlarmParams, **kwargs) -> None:
        source_access_spec = InternalAlarmSignalSourceAccessSpec(alarm_id, alarm_params, **kwargs)
        super().__init__(SignalType.INTERNAL_ALARM_STATE_CHANGE, alarm_id, dimension_filter_spec, source_access_spec)

    def do_activate(self, platform: HostPlatform) -> None:
        platform.connect_internal_signal(self._alarm_signal)


class InternalCompositeAlarmNode(CompositeAlarmNode):
    def __init__(self, alarm_id: str, dimension_filter_spec: DimensionFilter, alarm_params: CompositeAlarmParams, **kwargs) -> None:
        source_access_spec = InternalCompositeAlarmSignalSourceAccessSpec(alarm_id, alarm_params, **kwargs)
        super().__init__(SignalType.INTERNAL_ALARM_STATE_CHANGE, alarm_id, dimension_filter_spec, source_access_spec)

    def do_activate(self, platform: HostPlatform) -> None:
        platform.connect_internal_signal(self._alarm_signal)
