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


def create_system_alarms(app: AWSApplication, cti: CWAInternal.CTI, ticketing_enabled: bool, dedupe: Optional[str] = None) -> None:
    uuid: str = app.uuid
    system_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.SYSTEM)

    routing_metrics_map = system_metrics_map[RoutingTable]
    # 1-use metric ID/alias' to retrieve them from the map
    # 2-and then use 'MetricName' to get a concrete/materialized metric to be used in an Alarm.
    # these signals can be bind into alarms now
    routing_table_metric_signal = routing_metrics_map["routingTable"]
    routing_pending_nodes_table_metric_signal = routing_metrics_map["routingPendingNodeTable"]
    routing_active_records_table_metric_signal = routing_metrics_map["routingActiveComputeRecordsTable"]
    routing_inactive_records_table_metric_signal = routing_metrics_map["routingHistoryTable"]
    routing_lock_table_signal = routing_metrics_map["routingHistoryTable"]

    # low level alarm into an underlying driver resource
    # raise if write throttling in IntelliFlow routing table is more than 50 in 15 mins in two data-points out of 3.
    #       or route object retrieval latency is more than 500ms.
    # Please note that this alarm can still be aggregated into a composite alarm (which would still use the same dedupe_str)
    # this can enable you to partition alarm definitions and then merge them to create system wide OE view.
    app.create_alarm(
        id="routing_write_throttling_alarm",
        # refer
        #   https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html
        target_metric_or_expression="(m1 > 0 OR m2 > 0 OR m3 > 0 OR m4 > 0 OR m5 > 0)",
        metrics={
            "m1": routing_table_metric_signal["WriteThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m2": routing_pending_nodes_table_metric_signal["WriteThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m3": routing_active_records_table_metric_signal["WriteThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m4": routing_inactive_records_table_metric_signal["WriteThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m5": routing_lock_table_signal["WriteThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
        },
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {
                    # TODO sev-2! as this will cause event propagation issues and also data loss
                    CWAInternal(cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} routing tables writes are being throttled!"))
                }
                if ticketing_enabled
                else set()
            ),
            OK_ACTIONS=set(),
            INSUFFICIENT_DATA_ACTIONS=set(),
        ),
    )

    app.create_alarm(
        id="routing_read_throttling_alarm",
        target_metric_or_expression="(m1 > 0 OR m2 > 0 OR m3 > 0 OR m4 > 0 OR m5 > 0)",
        metrics={
            "m1": routing_table_metric_signal["ReadThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m2": routing_pending_nodes_table_metric_signal["ReadThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m3": routing_active_records_table_metric_signal["ReadThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m4": routing_inactive_records_table_metric_signal["ReadThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m5": routing_lock_table_signal["ReadThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
        },
        number_of_evaluation_periods=3,
        number_of_datapoint_periods=2,
        comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
        # since we are using logical/conditional operator in metric math, the output series only contains 1s or 0s.
        # so threshold will be 1 to detect.
        threshold=1,
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {CWAInternal(cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} routing tables reads are being throttled!"))}
                if ticketing_enabled
                else set()
            ),
        ),
    )

    processor_metrics_map = system_metrics_map[ProcessingUnit]
    processor_core_metric_signal = processor_metrics_map["processor.core"]
    processor_filter_metric_signal = processor_metrics_map["processor.filter"]
    processor_replay_metric_signal = processor_metrics_map["processor.replay"]
    processor_retention_metric_signal = processor_metrics_map["processor.retention"]
    # processor_dlq_metric_signal = processor_metrics_map["processor.dlq"]

    app.create_alarm(
        id="processor_core_DLQ_errors_alarm",
        target_metric_or_expression=processor_core_metric_signal["DeadLetterErrors"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {
                    # TODO sev-2! as this means event loss from lambda ASYNC queue
                    CWAInternal(
                        cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} core processor DLQ error!"), severity=CWAInternal.Severity.SEV3
                    )
                }
                if ticketing_enabled
                else set()
            ),
        ),
    )

    # Sev5 on core Processor (paranoid / warning shot for oncall)
    # if Processor has an execution Error (message sent to DLQ probably)
    app.create_alarm(
        id="processor_core_errors_alarm",
        # refer
        #   https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html
        target_metric_or_expression="(m1 > 0 OR m2 > 0)",
        metrics={
            "m1": processor_core_metric_signal["Errors"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m2": processor_core_metric_signal["DeadLetterErrors"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
        },
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {
                    CWAInternal(
                        cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} core processor errors!"), severity=CWAInternal.Severity.SEV5
                    )
                }
                if ticketing_enabled
                else set()
            ),
        ),
    )

    # Sev5 on core Processor (paranoid / warning shot for oncall)
    # duration gets closer to 15 mins. 600 x 1000 (10 minutes, approacing to 15 mins timeout)
    app.create_alarm(
        id="processor_core_duration_alarm",
        # refer
        #   https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html
        target_metric_or_expression=processor_core_metric_signal["Duration"][MetricStatistic.MAXIMUM][MetricPeriod.MINUTES(5)],
        threshold=600 * 100,
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {
                    CWAInternal(
                        cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} core processor duration!"), severity=CWAInternal.Severity.SEV5
                    )
                }
                if ticketing_enabled
                else set()
            ),
        ),
    )

    app.create_alarm(
        id="processor_filter_errors_alarm",
        target_metric_or_expression="(m1 > 0 OR m2 > 0)",
        metrics={
            "m1": processor_filter_metric_signal["Errors"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m2": processor_filter_metric_signal["DeadLetterErrors"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
        },
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {
                    # errors are not so likely at filtering level. take it more seriously.
                    CWAInternal(
                        cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} filter processor errors!"), severity=CWAInternal.Severity.SEV3
                    )
                }
                if ticketing_enabled
                else set()
            ),
        ),
    )

    app.create_alarm(
        id="processor_replay_errors_alarm",
        target_metric_or_expression="(m1 > 0 OR m2 > 0)",
        metrics={
            "m1": processor_replay_metric_signal["Errors"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            "m2": processor_replay_metric_signal["DeadLetterErrors"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
        },
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {
                    # TODO sev-2 as this would mean event loss from DLQ
                    CWAInternal(
                        cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} DLQ redrive lambda error!"), severity=CWAInternal.Severity.SEV3
                    )
                }
                if ticketing_enabled
                else set()
            ),
        ),
    )

    # Retention
    app.create_alarm(
        id="processor_retention_duration_alarm",
        target_metric_or_expression=processor_retention_metric_signal["Duration"][MetricStatistic.MAXIMUM][MetricPeriod.MINUTES(5)],
        threshold=600 * 100,
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {
                    CWAInternal(
                        cti=cti,
                        dedupe_str=(dedupe if dedupe else f"{uuid} retention processor duration!"),
                        severity=CWAInternal.Severity.SEV5,
                    )
                }
                if ticketing_enabled
                else set()
            ),
        ),
    )

    app.create_alarm(
        id="processor_retention_errors_alarm",
        target_metric_or_expression=processor_retention_metric_signal["Errors"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
        comparison_operator=AlarmComparisonOperator.GreaterThanThreshold,
        threshold=0,
        treat_missing_data=AlarmTreatMissingData.NOT_BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS=(
                {
                    CWAInternal(
                        cti=cti, dedupe_str=(dedupe if dedupe else f"{uuid} retention processor error!"), severity=CWAInternal.Severity.SEV3
                    )
                }
                if ticketing_enabled
                else set()
            ),
        ),
    )

    # TODO use "ApproximateAgeOfOldestMessage" metric from processor_dlq_metric_signal metric group (represents SQS DLQ
    #  in AWSConfiguration)
