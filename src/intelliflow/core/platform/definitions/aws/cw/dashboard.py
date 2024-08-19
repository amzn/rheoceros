# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
from enum import Enum, unique
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from botocore.exceptions import ClientError

from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
from intelliflow.core.platform.definitions.aws.common import exponential_retry, get_code_for_exception
from intelliflow.core.signal_processing import Signal
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import MetricDimension, MetricPeriod, MetricStatistic
from intelliflow.core.signal_processing.dimension_constructs import AnyVariant
from intelliflow.core.signal_processing.signal import SignalType
from intelliflow.core.signal_processing.signal_source import (
    METRIC_VISUALIZATION_PERIOD_HINT,
    METRIC_VISUALIZATION_STAT_HINT,
    METRIC_VISUALIZATION_TYPE_HINT,
    MetricSignalSourceAccessSpec,
)

logger = logging.getLogger(__name__)

CW_DASHBOARD_WIDTH_MAX = 24


def _create_widget_root(
    widget_type: str, x: Optional[int] = None, y: Optional[int] = None, width: Optional[int] = None, height: Optional[int] = None
) -> Dict[str, Any]:
    widget = {"type": widget_type}

    if x is not None:
        widget.update({"x": x})

    if y is not None:
        widget.update({"y": y})

    if width is not None:
        widget.update({"width": width})

    if height is not None:
        widget.update({"height": height})

    return widget


def create_text_widget(
    markdown: str, x: Optional[int] = None, y: Optional[int] = None, width: Optional[int] = None, height: Optional[int] = None
) -> Dict[str, Any]:
    """Creates a dictionary that represents a CW Dashboard text widget.

    :param markdown:  https://docs.aws.amazon.com/awsconsolehelpdocs/latest/gsg/aws-markdown.html
    :return: dict object that can be serialized into JSON
    """
    widget = _create_widget_root("text", x, y, width, height)
    widget.update({"properties": {"markdown": markdown}})
    return widget


def create_alarm_status_widget(
    platform: "Platform",
    title: str,
    alarms: List[Signal],
    x: Optional[int] = None,
    y: Optional[int] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> Dict[str, Any]:
    """Creates a dictionary that represents a CW Dashboard alarm status widget.
        Refer
          https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/CloudWatch-Dashboard-Body-Structure.html#CloudWatch-Dashboard-Properties-Alarm-Widget-Object

    :param platform: Platform object that will  be used to convert internal alarms to their external (pure CW Alarm)
    representation.
    :return: dict object that can be serialized into JSON
    """
    widget = _create_widget_root("alarm", x, y, width, height)
    alarms = [
        platform.diagnostics.map_internal_signal(alarm) if alarm.type == SignalType.INTERNAL_ALARM_STATE_CHANGE else alarm
        for alarm in alarms
    ]
    widget.update(
        {"properties": {"alarms": [alarm.resource_access_spec.arn for alarm in alarms], "sortBy": "stateUpdatedTimestamp", "title": title}}
    )
    return widget


@unique
class LegendPosition(str, Enum):
    RIGHT = "right"
    BOTTOM = "bottom"
    HIDDEN = "hidden"


def create_metric_widget(
    platform: "Platform",
    title: str,
    metrics_or_alarm: List[Signal],
    legend_pos: Optional[LegendPosition] = LegendPosition.RIGHT,
    x: Optional[int] = None,
    y: Optional[int] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> Dict[str, Any]:
    """Creates a dictionary that represents a CW Dashboard metric type widget.
        Refer
          https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/CloudWatch-Dashboard-Body-Structure.html

    :param platform: Platform object that will be used to convert internal alarm to their external (pure CW Alarm)
    representation or in the metrics case to extract AWS region.
    :param metrics_or_alarm: an alarm signal or at least one metric signals for the widget will be created.
    :param legend_pos: determines the position of legend on each metric graph. default is LegendPosition.RIGHT
    :return: dict object that can be serialized into JSON
    """
    if len(metrics_or_alarm) > 1 and not all([signal.type.is_metric() for signal in metrics_or_alarm]):
        # common mistake: metric signals mixed with alarm.
        # CW alarm rendering does not allow 'metrics' definition in the same widget.
        # Also there can be only one alarm as an annotation in a signal metric widget.
        # refer
        #   https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/CloudWatch-Dashboard-Body-Structure.html#CloudWatch-Dashboard-Properties-Annotation-Format
        raise ValueError(
            f"If the metric widget will contain multiple signals, then all of them must be metric!"
            f" Current list of signal types provided to create_widget: {[s.type.value for s in metrics_or_alarm]}"
        )

    widget = _create_widget_root("metric", x, y, width, height)

    # extract widget defaults as hints from Signal level metadata, make sure that there are no conflicts
    #  for each metric, they will be overwritten per concrete materialized Metric::Name if Period and Stat dimensions
    #  are materialized also.
    view_hints: Set[str] = {s.resource_access_spec.attrs.get(METRIC_VISUALIZATION_TYPE_HINT, None) for s in metrics_or_alarm}
    stat_hints: Set[MetricStatistic] = {s.resource_access_spec.attrs.get(METRIC_VISUALIZATION_STAT_HINT, None) for s in metrics_or_alarm}
    period_hints: Set[MetricPeriod] = {s.resource_access_spec.attrs.get(METRIC_VISUALIZATION_PERIOD_HINT, None) for s in metrics_or_alarm}

    if len(view_hints) > 1:
        raise ValueError(
            f"Conflicting METRIC_VISUALIZATION_TYPE_HINT definitions {view_hints!r} for rendering of "
            f"signals {[s.alias for s in metrics_or_alarm]}"
        )
    if len(stat_hints) > 1:
        raise ValueError(
            f"Conflicting METRIC_VISUALIZATION_STAT_HINT definitions {stat_hints!r} for rendering of "
            f"signals {[s.alias for s in metrics_or_alarm]}"
        )
    if len(period_hints) > 1:
        raise ValueError(
            f"Conflicting METRIC_VISUALIZATION_PERIOD_HINT definitions {period_hints!r} for rendering of "
            f"signals {[s.alias for s in metrics_or_alarm]}"
        )

    view: Optional[str] = next(iter(view_hints)) if view_hints else None
    stat: Optional[MetricStatistic] = next(iter(stat_hints)) if stat_hints else None
    period: Optional[MetricPeriod] = next(iter(period_hints)) if period_hints else None

    properties = {
        "title": title,
        "legend": {"position": legend_pos.value},
        "liveData": False,
        # Timezone format is + or - followed by four digits. The first two digits indicate the number of hours ahead or
        # behind of UTC, and the final two digits are the number of minutes. The default is +0000.
        # "timezone": "+0130"
    }

    if view:
        properties.update({"view": view if view else "timeSeries"})  # | singleValue | bar | pie

    if period:
        properties.update({"period": period.value})  # default is 300

    if stat:
        properties.update({"stat": stat.value})

    if metrics_or_alarm[0].type.is_alarm():
        alarm = metrics_or_alarm[0]
        if alarm.type == SignalType.INTERNAL_ALARM_STATE_CHANGE:
            alarm = platform.diagnostics.map_internal_signal(alarm)

        properties.update({"annotations": {"alarms": [alarm.resource_access_spec.arn]}, "region": alarm.resource_access_spec.region_id})

        # TODO required for alarms from other accounts (actual external ones?)
        # CW will use the current account by default
        # properties.update({
        #     "accountId": alarm.resource_access_spec.account_id
        # })
    else:
        platform_region = platform.conf.get_param(AWSCommonParams.REGION)
        # multiple metric rendering
        metric_signals = metrics_or_alarm
        properties.update({"region": platform_region})

        metrics = []
        for metric_signal in metric_signals:
            # refer
            #  https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/CloudWatch-Dashboard-Body-Structure.html#CloudWatch-Dashboard-Properties-Metrics-Array-Format
            metric_access_spec: MetricSignalSourceAccessSpec = metric_signal.resource_access_spec
            cw_metrics = metric_access_spec.create_stats_from_filter(metric_signal.domain_spec.dimension_filter_spec)

            # a metric signal can map to one or more concrete CW metrics based on signal's dimension filter
            for cw_metric in cw_metrics:
                metric_name = cw_metric["Metric"]["MetricName"]
                if metric_name != AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR:
                    # [Namespace, MetricName, [{DimensionName,DimensionValue}...] {Rendering Properties Object} ]
                    metric = [cw_metric["Metric"]["Namespace"], metric_name]
                    for sub_dim_key, sub_dim_value in metric_access_spec.sub_dimensions.items():
                        metric.extend([sub_dim_key, sub_dim_value])

                    rendering_properties = {"label": metric_name}
                    metric_period = cw_metric["Period"]
                    metric_stat = cw_metric["Stat"]
                    # overwrite stat/period if those dimensions are specified
                    if metric_period != AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR:
                        rendering_properties.update({"period": int(metric_period)})
                        if period and period.value != metric_period:
                            logger.warning(
                                f"Overwriting metric period hint {period!r} for {metric_signal.alias} with "
                                f"{metric_period!r} while rendering metric name {metric_name!r}"
                            )
                    if metric_stat != AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR:
                        rendering_properties.update({"stat": str(metric_stat)})
                        if stat and stat.value != metric_stat:
                            logger.warning(
                                f"Overwriting metric stat hint {stat!r} for {metric_signal.alias} with "
                                f"{metric_stat!r} while rendering metric name {metric_name!r}"
                            )
                    metric.append(rendering_properties)

                    metrics.append(metric)
                else:
                    logger.warning(
                        f"Metric signal {metric_signal.alias!r} does not have its metric <Name> dimension materialized! "
                        f"Will not rendered in the dashboard."
                    )
                    # raise ValueError(f"Metric signal {metric_signal.alias!r} does not have its metric <Name> dimension materialized!")

        properties.update({"metrics": metrics})

    widget.update({"properties": properties})

    return widget
