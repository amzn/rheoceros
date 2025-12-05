from typing import Optional, Union

from intelliflow.api_ext import AWSApplication
from intelliflow.core.application.context.node.internal.nodes import InternalDataNode
from intelliflow.core.application.context.node.marshaling.nodes import MarshalerNode, MarshalingView
from intelliflow.core.platform.constructs import RoutingHookInterface, RoutingTable
from intelliflow.core.signal_processing import Slot
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import (
    AlarmComparisonOperator,
    AlarmDefaultActionsMap,
    AlarmTreatMissingData,
    CWAInternal,
    MetricPeriod,
    MetricStatistic,
)
from intelliflow.core.signal_processing.routing_runtime_constructs import RoutePendingNodeHook
from intelliflow.utils.test.inlined_compute import NOOPCompute


def create_availability_alarm(
    app: AWSApplication,
    node: MarshalerNode,
    cti: CWAInternal.CTI,
    ticketing_enabled: bool,
    metric_period: MetricPeriod = MetricPeriod.MINUTES(24 * 60),
    dedupe: Optional[str] = None,
) -> None:
    # this API will implicitly do all of the necessary validations / checks (e.g node must be an internal data node)
    node_metrics = app.get_route_metrics(node)

    internal_data_node: InternalDataNode = node.bound
    node_id: str = internal_data_node.data_id
    if dedupe is None:
        dedupe = f"{app.id} {node_id} availability!"

    node_routing_metrics_map = node_metrics[RoutingTable]

    node_state_metric_signal = node_routing_metrics_map["routing_table.receive.hook" + "." + node_id]

    # rule: if (number of success < 1) OR (data not found [breaching]) over the last 24 hours
    app.create_alarm(
        id=f"{node_id}_AVAILABILITY_ALARM",
        target_metric_or_expression=node_state_metric_signal[RoutingHookInterface.Execution.IExecutionSuccessHook.__name__][
            MetricStatistic.SUM
        ][metric_period],
        number_of_evaluation_periods=1,
        number_of_datapoint_periods=1,
        comparison_operator=AlarmComparisonOperator.LessThanThreshold,
        threshold=1,
        # CRITICAL for availability alarm
        treat_missing_data=AlarmTreatMissingData.BREACHING,
        default_actions=AlarmDefaultActionsMap(
            ALARM_ACTIONS={CWAInternal(cti=cti, dedupe_str=dedupe, severity=CWAInternal.Severity.SEV3)} if ticketing_enabled else set(),
        ),
    )


def create_availability_alarm_extended(
    app: AWSApplication,
    monitoring_init_signal: MarshalerNode,
    node: Union[MarshalerNode, MarshalingView],
    alarm_period_in_hours: int,
    alarm_action: Slot,
) -> MarshalerNode:
    internal_data_node: InternalDataNode = node.bound if isinstance(node, MarshalerNode) else node.marshaler_node.bound
    node_id: str = internal_data_node.data_id

    return app.create_data(
        id=f"{node_id}_AVAILABILITY_ALARM_EXTENDED_{alarm_period_in_hours}_HOURS",
        inputs=[monitoring_init_signal, node.ref.range_check(True)],
        compute_targets=[NOOPCompute],
        pending_node_hook=RoutePendingNodeHook(
            on_expiration=alarm_action,
        ),
        pending_node_expiration_ttl_in_secs=alarm_period_in_hours * 60 * 60,
    )
