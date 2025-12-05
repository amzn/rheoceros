from typing import Optional

from intelliflow.api_ext import AWSApplication
from intelliflow.core.platform.constructs import ProcessingUnit, RoutingTable
from intelliflow.core.platform.development import HostPlatform
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import (
    AlarmComparisonOperator,
    AlarmDefaultActionsMap,
    AlarmTreatMissingData,
    CWAInternal,
    MetricPeriod,
    MetricStatistic,
)


def create_orchestration_alarms(app: AWSApplication, cti: CWAInternal.CTI, ticketing_enabled: bool, dedupe: Optional[str] = None) -> None:
    uuid: str = app.uuid
    orchestration_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.ORCHESTRATION)

    processor_metrics_map = orchestration_metrics_map[ProcessingUnit]
    routing_metrics_map = orchestration_metrics_map[RoutingTable]

    processor_error_signal = processor_metrics_map["processor.event.error.type"]

    processor_alarm = app.create_alarm(
        id="orchestration_processor_core_error_alarm",
        target_metric_or_expression="(m1 > 0 OR m2 > 0)",
        metrics={
            "m1": processor_error_signal["NameError"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m2": processor_error_signal["Exception"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
        },
        number_of_evaluation_periods=1,
        number_of_datapoint_periods=1,
        comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
        # since we are using logical/conditional operator in metric math, the output series only contains 1s or 0s.
        # so threshold will be 1 to detect.
        threshold=1,
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {CWAInternal(cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} orchesration core event processor error!"))}
                if ticketing_enabled
                else set()
            ),
            OK_ACTIONS=set(),
            INSUFFICIENT_DATA_ACTIONS=set(),
        ),
    )

    # Capture most common orchestration issues (due to DDB, etc) when IntelliFlow cannot load or persist the state of a node
    routing_persistence_failure_signal = routing_metrics_map["routing_table.receive"]

    routing_persistence_alarm = app.create_alarm(
        id="orchestration_routing_persistence_alarm",
        target_metric_or_expression="(m1 > 0 OR m2 > 0)",
        metrics={
            "m1": routing_persistence_failure_signal["RouteLoadError"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m2": routing_persistence_failure_signal["RouteSaveError"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
        },
        number_of_evaluation_periods=3,
        number_of_datapoint_periods=2,
        comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
        threshold=1,
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {CWAInternal(cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} orchesration core routing persistence error!"))}
                if ticketing_enabled
                else set()
            ),
            OK_ACTIONS=set(),
            INSUFFICIENT_DATA_ACTIONS=set(),
        ),
    )

    # Retention engine
    processor_retention_error_signal = processor_metrics_map["processor.retention.event.error.type"]

    retention_processor_alarm = app.create_alarm(
        id="orchestration_processor_retention_error_alarm",
        target_metric_or_expression=processor_retention_error_signal["Exception"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
        number_of_evaluation_periods=1,
        number_of_datapoint_periods=1,
        comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
        threshold=1,
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {CWAInternal(cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} orchesration retention event processor error!"))}
                if ticketing_enabled
                else set()
            ),
            OK_ACTIONS=set(),
            INSUFFICIENT_DATA_ACTIONS=set(),
        ),
    )

    routing_retention_failure_signal = routing_metrics_map["routing_table.retention"]
    routing_retention_alarm = app.create_alarm(
        id="routing_retention_alarm",
        target_metric_or_expression="(m1 > 0 OR m2 > 0)",
        metrics={
            "m1": routing_retention_failure_signal["RouteRetentionCheckError"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m2": routing_retention_failure_signal["RouteRetentionRefreshCheckError"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
        },
        number_of_evaluation_periods=3,
        number_of_datapoint_periods=2,
        comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
        threshold=1,
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {CWAInternal(cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} orchesration retention runtime error!"))}
                if ticketing_enabled
                else set()
            ),
            OK_ACTIONS=set(),
            INSUFFICIENT_DATA_ACTIONS=set(),
        ),
    )
