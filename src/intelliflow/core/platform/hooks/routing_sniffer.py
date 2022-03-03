# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from typing import Callable, ClassVar, List, Optional, Sequence, cast

from intelliflow.api import (
    IComputeFailureHook,
    IComputeRetryHook,
    IComputeSuccessHook,
    IExecutionBeginHook,
    IExecutionCheckpointHook,
    IExecutionFailureHook,
    IExecutionSkippedHook,
    IExecutionSuccessHook,
    IPendingCheckpointHook,
    IPendingNodeCreationHook,
    IPendingNodeExpirationHook,
)
from intelliflow.core.platform.constructs import RoutingTable
from intelliflow.core.serialization import dumps
from intelliflow.core.signal_processing import Signal
from intelliflow.core.signal_processing.routing_runtime_constructs import (
    Route,
    RouteCheckpoint,
    RouteExecutionHook,
    RoutePendingNodeHook,
    RuntimeLinkNode,
)


class SnifferHookBase:
    """Encapsulates the common logic and mechanisms to create a minimalist routing/execution tracking solution.

    It aims to create a folder in Storage that gives a real-time and historical view of executions that can be
    trivially viewed from front-end extensions or be plumbed into more sophisticated, streamed monitoring mechanisms.

    For example a custom AWS monitoring solution that hook up with Storage bucket and use HOOK_DATA_FOLDER event
    filter to monitor the application (pipeline, etc) to create metrics, alarms, dashboards on both AWS and CDO side.

    Will be the root-level folder in Storage to keep hook-data with a similar structure as 'internal_data',
    with partitioning (sub-folders) being same.

    Example:

    internal_data:
    s3://.../internal_data/C2P_DATA_NA_1/1/1/2021-03-03

    versus (mirrored by)

    hook_data:
    executions:
    s3://.../c2p_internal_execution_data/C2P_DATA_NA_1/1/1/2021-03-03/executions/{timestamp}/{EXEC_UUID}/on_exec_begin.data
    s3://.../c2p_internal_execution_data/C2P_DATA_NA_1/1/1/2021-03-03/executions/{timestamp}/{EXEC_UUID}/on_exec_success.data
    s3://.../c2p_internal_execution_data/C2P_DATA_NA_1/1/1/2021-03-03/executions/{timestamp}/{EXEC_UUID}/on_exec_failure.data
    s3://.../c2p_internal_execution_data/C2P_DATA_NA_1/1/1/2021-03-03/executions/{timestamp}/{EXEC_UUID}/checkpoints/{offset}/on_exec_failure.data

    pending nodes (events)
    unmaterialized:
    s3://.../c2p_internal_execution_data/C2P_DATA_NA_1/pending_nodes/{timestamp}/{PENDING_NODE_UUID}/on_pending_node_created.data
    materialized (ouput can be materialized):
    s3://.../c2p_internal_execution_data/C2P_DATA_NA_1/1/1/2021-03-03/pending_nodes/{timestamp}/{PENDING_NODE_UUID}/on_pending_node_created.data
    s3://.../c2p_internal_execution_data/C2P_DATA_NA_1/1/1/2021-03-03/pending_nodes/{timestamp}/{PENDING_NODE_UUID}/on_pending_node_expired.data
    s3://.../c2p_internal_execution_data/C2P_DATA_NA_1/1/1/2021-03-03/pending_nodes/{timestamp}/{PENDING_NODE_UUID}/checkpoints/{offset}/on_pending_node_checkpoint.data

    Please note that PENDING_NODE_UUID == EXEC_UUID for pending nodes that get executed (not expired).
    """

    HOOK_DATA_FOLDER: ClassVar[str] = "routing_sniffer_data"
    EXECUTIONS_SUB_FOLDER: ClassVar[str] = "executions"
    PENDING_NODES_SUB_FOLDER: ClassVar[str] = "pending_nodes"
    CHECKPOINT_SUB_FOLDER: ClassVar[str] = "checkpoints"

    def wake_up(self, routing_table: RoutingTable):
        self._routing_table = routing_table

    def save_execution_data(self, execution_context: Route.ExecutionContext, timestamp: int):
        # FIXME / TODO wont work! we need to get the branch for TIP DimensionFilter::get_dimension_branch
        partitions = [
            str(cast("DimensionVariant", dim).value)
            for _, dim in execution_context.output.domain_spec.dimension_filter_spec.get_flattened_dimension_map().items()
        ]
        self._save_execution_data(
            # TODO expect high-level param from user for format (native, repr, json, etc)
            data=dumps(execution_context),
            data_id=execution_context.output.alias,
            execution_id=execution_context.id,
            timestamp=timestamp,
            partitions=partitions,
        )

    def save_compute_data(self, compute_record: RoutingTable.ComputeRecord, timestamp: int):
        # FIXME / TODO wont work! we need to get the branch for TIP DimensionFilter::get_dimension_branch
        partitions = [
            str(cast("DimensionVariant", dim).value)
            for _, dim in compute_record.materialized_output.domain_spec.dimension_filter_spec.get_flattened_dimension_map().items()
        ]
        self._save_execution_data(
            # TODO expect high-level param from user for format (native, repr, json, etc)
            data=dumps(compute_record),
            data_id=compute_record.materialized_output.alias,
            execution_id=compute_record.execution_context_id,
            timestamp=timestamp,
            partitions=partitions,
        )

    def _save_execution_data(self, data: str, data_id: str, execution_id: str, timestamp: int, partitions: Sequence[str]):
        self._routing_table.get_platform().storage.save(
            data,
            [self.HOOK_DATA_FOLDER, data_id] + partitions + [self.EXECUTIONS_SUB_FOLDER, str(timestamp), execution_id],
            self.__class__.__name__,
        )

    def _save_execution_checkpoint_data(
        self, data: str, data_id: str, execution_id: str, timestamp: int, checkpoint_offset_in_secs: int, partitions: Sequence[str]
    ):
        self._routing_table.get_platform().storage.save(
            data,
            [self.HOOK_DATA_FOLDER, data_id]
            + partitions
            + [self.EXECUTIONS_SUB_FOLDER, str(timestamp), execution_id, self.CHECKPOINT_SUB_FOLDER, checkpoint_offset_in_secs],
            self.__class__.__name__,
        )

    def save_pending_node_data(
        self, pending_node: RuntimeLinkNode, route: Route, timestamp: int, checkpoint_offset_in_secs: Optional[int] = None
    ):
        partitions = []
        try:
            materialized_output = pending_node.materialize_output(route.output, route._output_dim_matrix, force=True)
            if materialized_output.domain_spec.dimension_filter_spec.is_material():
                # FIXME / TODO wont work! we need to get the branch for TIP DimensionFilter::get_dimension_branch
                partitions = [
                    str(cast("DimensionVariant", dim).value)
                    for _, dim in materialized_output.domain_spec.dimension_filter_spec.get_flattened_dimension_map().items()
                ]
        except ValueError:
            # some of the dimensions of output is not satisfied/mapped from ready signals.
            # node is still pending in unmaterialized mode.
            partitions = []

        self._save_pending_node_data(
            # TODO expect high-level param from user for format (native, repr, json, etc)
            data=dumps(pending_node),
            data_id=route.output.alias,
            pending_node_id=pending_node.node_id,
            timestamp=timestamp,
            partitions=partitions,
            checkpoint_offset_in_secs=checkpoint_offset_in_secs,
        )

    def _save_pending_node_data(
        self,
        data: str,
        data_id: str,
        pending_node_id: str,
        timestamp: int,
        partitions: Sequence[str],
        checkpoint_offset_in_secs: Optional[int] = None,
    ):
        self._routing_table.get_platform().storage.save(
            data,
            [self.HOOK_DATA_FOLDER, data_id] + partitions + [self.PENDING_NODES_SUB_FOLDER, str(timestamp), pending_node_id] + []
            if checkpoint_offset_in_secs is None
            else [self.CHECKPOINT_SUB_FOLDER, checkpoint_offset_in_secs],
            self.__class__.__name__,
        )


class OnExecBegin(SnifferHookBase, IExecutionBeginHook):
    def __call__(
        self,
        routing_table: RoutingTable,
        route_record: RoutingTable.RouteRecord,
        execution_context: Route.ExecutionContext,
        current_timestamp_in_utc: int,
    ) -> None:
        self.wake_up(routing_table)
        self.save_execution_data(execution_context=execution_context, timestamp=current_timestamp_in_utc)


class OnExecutionSkipped(SnifferHookBase, IExecutionSkippedHook):
    def __call__(
        self,
        routing_table: RoutingTable,
        route_record: RoutingTable.RouteRecord,
        execution_context: Route.ExecutionContext,
        current_timestamp_in_utc: int,
    ) -> None:
        self.wake_up(routing_table)
        self.save_execution_data(execution_context=execution_context, timestamp=current_timestamp_in_utc)


class OnComputeSuccess(SnifferHookBase, IComputeSuccessHook):
    def __call__(
        self,
        routing_table: RoutingTable,
        route_record: RoutingTable.RouteRecord,
        compute_record: RoutingTable.ComputeRecord,
        current_timestamp_in_utc: int,
    ) -> None:
        self.wake_up(routing_table)
        self.save_compute_data(compute_record=compute_record, timestamp=current_timestamp_in_utc)


class OnComputeFailure(SnifferHookBase, IComputeFailureHook):
    def __call__(
        self,
        routing_table: RoutingTable,
        route_record: RoutingTable.RouteRecord,
        compute_record: RoutingTable.ComputeRecord,
        current_timestamp_in_utc: int,
    ) -> None:
        self.wake_up(routing_table)
        self.save_compute_data(compute_record=compute_record, timestamp=current_timestamp_in_utc)


class OnComputeRetry(SnifferHookBase, IComputeRetryHook):
    def __call__(
        self,
        routing_table: RoutingTable,
        route_record: RoutingTable.RouteRecord,
        compute_record: RoutingTable.ComputeRecord,
        current_timestamp_in_utc: int,
    ) -> None:
        self.wake_up(routing_table)
        self.save_compute_data(compute_record=compute_record, timestamp=current_timestamp_in_utc)


class OnExecutionSuccess(SnifferHookBase, IExecutionSuccessHook):
    def __call__(
        self,
        routing_table: RoutingTable,
        route_record: RoutingTable.RouteRecord,
        execution_context_id: str,
        materialized_inputs: List[Signal],
        materialized_output: Signal,
        current_timestamp_in_utc: int,
    ) -> None:
        self.wake_up(routing_table)
        # FIXME / TODO wont work! we need to get the branch for TIP DimensionFilter::get_dimension_branch
        partitions = [
            str(cast("DimensionVariant", dim).value)
            for _, dim in materialized_output.domain_spec.dimension_filter_spec.get_flattened_dimension_map().items()
        ]
        self._save_execution_data(
            # TODO expect high-level param from user for format (native, repr, json, etc)
            data=dumps(execution_context_id),
            data_id=materialized_output.alias,
            execution_id=execution_context_id,
            timestamp=current_timestamp_in_utc,
            partitions=partitions,
        )


class OnExecutionFailure(SnifferHookBase, IExecutionFailureHook):
    def __call__(
        self,
        routing_table: RoutingTable,
        route_record: RoutingTable.RouteRecord,
        execution_context_id: str,
        materialized_inputs: List[Signal],
        materialized_output: Signal,
        current_timestamp_in_utc: int,
    ) -> None:
        self.wake_up(routing_table)
        # FIXME / TODO wont work! we need to get the branch for TIP DimensionFilter::get_dimension_branch
        partitions = [
            str(cast("DimensionVariant", dim).value)
            for _, dim in materialized_output.domain_spec.dimension_filter_spec.get_flattened_dimension_map().items()
        ]
        self._save_execution_data(
            data=dumps(execution_context_id),
            data_id=materialized_output.alias,
            execution_id=execution_context_id,
            timestamp=current_timestamp_in_utc,
            partitions=partitions,
        )


class OnExecutionCheckpoint(SnifferHookBase, IExecutionCheckpointHook):
    def __call__(
        self,
        routing_table: RoutingTable,
        route_record: RoutingTable.RouteRecord,
        execution_context_id: str,
        active_compute_records: List[RoutingTable.ComputeRecord],
        checkpoint_in_secs: int,
        current_timestamp_in_utc: int,
    ) -> None:
        self.wake_up(routing_table)
        materialized_output = active_compute_records[0].materialized_output
        # FIXME / TODO wont work! we need to get the branch for TIP DimensionFilter::get_dimension_branch
        partitions = [
            str(cast("DimensionVariant", dim).value)
            for _, dim in materialized_output.domain_spec.dimension_filter_spec.get_flattened_dimension_map().items()
        ]
        self._save_execution_checkpoint_data(
            data=dumps(active_compute_records),
            data_id=materialized_output.alias,
            execution_id=execution_context_id,
            timestamp=current_timestamp_in_utc,
            checkpoint_offset_in_secs=checkpoint_in_secs,
            partitions=partitions,
        )


class OnPendingNodeCreation(SnifferHookBase, IPendingNodeCreationHook):
    def __call__(
        self,
        routing_table: RoutingTable,
        route_record: RoutingTable.RouteRecord,
        pending_node: RuntimeLinkNode,
        current_timestamp_in_utc: int,
    ):
        self.wake_up(routing_table)
        self.save_pending_node_data(pending_node=pending_node, route=route_record.route, timestamp=current_timestamp_in_utc)


class OnPendingNodeExpiration(SnifferHookBase, IPendingNodeExpirationHook):
    def __call__(
        self,
        routing_table: RoutingTable,
        route_record: RoutingTable.RouteRecord,
        pending_node: RuntimeLinkNode,
        current_timestamp_in_utc: int,
    ):
        self.wake_up(routing_table)
        self.save_pending_node_data(pending_node=pending_node, route=route_record.route, timestamp=current_timestamp_in_utc)


class OnPendingCheckpoint(SnifferHookBase, IPendingCheckpointHook):
    def __call__(
        self,
        routing_table: RoutingTable,
        route_record: RoutingTable.RouteRecord,
        pending_node: RuntimeLinkNode,
        checkpoint_in_secs: int,
        current_timestamp_in_utc: int,
    ):
        self.wake_up(routing_table)
        self.save_pending_node_data(
            pending_node=pending_node,
            route=route_record.route,
            timestamp=current_timestamp_in_utc,
            checkpoint_offset_in_secs=checkpoint_in_secs,
        )


def ExecutionSniffer(max_execution_checkpoint_in_hours: Optional[int] = None) -> RouteExecutionHook:
    DEFAULT_EXECUTION_CHECKPOINT_MAX_IN_HOURS = 24
    return RouteExecutionHook(
        on_exec_begin=OnExecBegin(),
        on_exec_skipped=OnExecutionSkipped(),
        on_compute_success=OnComputeSuccess(),
        on_compute_failure=OnComputeFailure(),
        on_compute_retry=OnComputeRetry(),
        on_success=OnExecutionSuccess(),
        on_failure=OnExecutionFailure(),
        checkpoints=[
            RouteCheckpoint(checkpoint_in_secs=c_in_hours * 60 * 60, slot=OnExecutionCheckpoint())
            for c_in_hours in range(
                0,
                cls.DEFAULT_EXECUTION_CHECKPOINT_MAX_IN_HOURS
                if max_execution_checkpoint_in_hours is None
                else max_execution_checkpoint_in_hours,
            )
        ],
    )


def PendingNodeSniffer() -> RoutePendingNodeHook:
    return RoutePendingNodeHook(
        on_pending_node_created=OnPendingNodeCreation(),
        on_expiration=OnPendingNodeExpiration(),
        checkpoints=[
            # RouteCheckpoint(checkpoint_in_secs=c_in_hours * 60 *60, slot=OnPendingCheckpoint()) for c_in_hours in range(0, 7)
        ],
    )
