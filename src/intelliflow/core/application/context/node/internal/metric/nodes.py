# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from intelliflow.core.application.context.node.base import MetricNode
from intelliflow.core.platform.development import HostPlatform
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import MetricSubDimensionMapType
from intelliflow.core.signal_processing.dimension_constructs import DimensionFilter
from intelliflow.core.signal_processing.signal import SignalType
from intelliflow.core.signal_processing.signal_source import InternalMetricSignalSourceAccessSpec


class InternalMetricNode(MetricNode):
    def __init__(self, metric_id: str, dimension_filter_spec: DimensionFilter, sub_dimensions: MetricSubDimensionMapType, **kwargs) -> None:
        # add metric_id as group id to avoid conflicts between metrics that have overlaps over 'Name' dimension and
        # 'sub_dimensions'.
        internal_sub_dimensions = dict(sub_dimensions)
        internal_sub_dimensions.update({InternalMetricSignalSourceAccessSpec.METRIC_GROUP_ID_SUB_DIMENSION_KEY: metric_id})

        source_access_spec = InternalMetricSignalSourceAccessSpec(internal_sub_dimensions, **kwargs)
        super().__init__(SignalType.INTERNAL_METRIC_DATA_CREATION, metric_id, dimension_filter_spec, source_access_spec)

    def do_activate(self, platform: HostPlatform) -> None:
        platform.connect_internal_signal(self._metric_signal)
