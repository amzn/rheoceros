# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from typing import Any, Dict, List, Tuple, Union

from intelliflow.core.platform.constructs import ConstructParamsDict, RoutingComputeInterface, RoutingHookInterface
from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
from intelliflow.core.serialization import dumps
from intelliflow.core.signal_processing import Signal


# Extractors below are provided to facilitate the implementation of compute targets that implement multiple
# interfaces (RoutingComputeInterface + RoutingHookInterface) using the Callable type.
# They also guarantee coupling with those interfaces and make sure that we will reflect interface changes in system
# provided compute targets.
class _InlinedComputeParamExtractor(RoutingComputeInterface.IInlinedCompute):
    def __call__(
        self, input_map: Dict[str, Signal], materialized_output: Signal, params: ConstructParamsDict, *args, **kwargs
    ) -> Tuple[Dict[str, Signal], Signal, ConstructParamsDict]:
        if isinstance(input_map, dict) and isinstance(materialized_output, Signal) and isinstance(params, dict):
            return input_map, materialized_output, params


InlinedComputeParamExtractor = _InlinedComputeParamExtractor()


class _ExecutionInitHookParamExtractor(
    RoutingHookInterface.Execution.IExecutionBeginHook, RoutingHookInterface.Execution.IExecutionSkippedHook
):
    def __call__(
        self,
        routing_table: "RoutingTable",
        route_record: "RoutingTable.RouteRecord",
        execution_context: "Route.ExecutionContext",
        current_timestamp_in_utc: int,
        **params,
    ) -> Tuple["RoutingTable", "RoutingTable.RouteRecord", "Route.ExecutionContext", int, dict]:
        return routing_table, route_record, execution_context, current_timestamp_in_utc, params


ExecutionInitHookParamExtractor = _ExecutionInitHookParamExtractor()


class _ExecutionContextHookParamExtractor(
    RoutingHookInterface.Execution.IExecutionSuccessHook, RoutingHookInterface.Execution.IExecutionFailureHook
):
    def __call__(
        self,
        routing_table: "RoutingTable",
        route_record: "RoutingTable.RouteRecord",
        execution_context_id: str,
        materialized_inputs: List[Signal],
        materialized_output: Signal,
        current_timestamp_in_utc: int,
        **params,
    ) -> Tuple["RoutingTable", "RoutingTable.RouteRecord", str, List[Signal], Signal, int, dict]:
        return routing_table, route_record, execution_context_id, materialized_inputs, materialized_output, current_timestamp_in_utc, params


ExecutionContextHookParamExtractor = _ExecutionContextHookParamExtractor()


class _ComputeHookParamExtractor(
    RoutingHookInterface.Execution.IComputeSuccessHook,
    RoutingHookInterface.Execution.IComputeFailureHook,
    RoutingHookInterface.Execution.IComputeRetryHook,
):
    def __call__(
        self,
        routing_table: "RoutingTable",
        route_record: "RoutingTable.RouteRecord",
        compute_record: "RoutingTable.ComputeRecord",
        current_timestamp_in_utc: int,
        **params,
    ) -> Tuple["RoutingTable", "RoutingTable.RouteRecord", "RoutingTable.ComputeRecord", int, dict]:
        return routing_table, route_record, compute_record, current_timestamp_in_utc, params


ComputeHookParamExtractor = _ComputeHookParamExtractor()


class _ExecutionCheckPointHookParamExtractor(RoutingHookInterface.Execution.IExecutionCheckpointHook):
    def __call__(
        self,
        routing_table: "RoutingTable",
        route_record: "RoutingTable.RouteRecord",
        execution_context_id: str,
        active_compute_records: List["RoutingTable.ComputeRecord"],
        checkpoint_in_secs: int,
        current_timestamp_in_utc: int,
        **params,
    ) -> Tuple["RoutingTable", "RoutingTable.RouteRecord", str, "RoutingTable.ComputeRecord", int, int, dict]:
        return (
            routing_table,
            route_record,
            execution_context_id,
            active_compute_records,
            checkpoint_in_secs,
            current_timestamp_in_utc,
            params,
        )


ExecutionCheckPointHookParamExtractor = _ExecutionCheckPointHookParamExtractor()


class _PendingNodeHookParamExtractor(RoutingHookInterface.PendingNode.IPendingNodeCreationHook):
    def __call__(
        self,
        routing_table: "RoutingTable",
        route_record: "RoutingTable.RouteRecord",
        pending_node: "RuntimeLinkNode",
        current_timestamp_in_utc: int,
        **params,
    ) -> Tuple["RoutingTable", "RoutingTable.RouteRecord", "RuntimeLinkNode", int, dict]:
        return routing_table, route_record, pending_node, current_timestamp_in_utc, params


PendingNodeHookParamExtractor = _PendingNodeHookParamExtractor()


class _PendingNodeCheckpointHookParamExtractor(RoutingHookInterface.PendingNode.IPendingCheckpointHook):
    def __call__(
        self,
        routing_table: "RoutingTable",
        route_record: "RoutingTable.RouteRecord",
        pending_node: "RuntimeLinkNode",
        checkpoint_in_secs: int,
        current_timestamp_in_utc: int,
        **params,
    ) -> Tuple["RoutingTable", "RoutingTable.RouteRecord", "RuntimeLinkNode", int, int, dict]:
        return routing_table, route_record, pending_node, checkpoint_in_secs, current_timestamp_in_utc, params


PendingNodeCheckpointHookParamExtractor = _PendingNodeCheckpointHookParamExtractor()


def get_dimension_info_for_signal(input_signal):
    paths = input_signal.get_materialized_resource_paths()
    info = {"apex/tip": paths[0]}
    if len(paths) > 1:
        info.update({"range_depth": len(paths)})
    info.update({"dimension_filter": input_signal.domain_spec.dimension_filter_spec.pretty()})
    return info


def _cleanup_params(params):
    if AWSCommonParams.IF_ACCESS_PAIR in params:
        del params[AWSCommonParams.IF_ACCESS_PAIR]
    if AWSCommonParams.IF_ADMIN_ACCESS_PAIR in params:
        del params[AWSCommonParams.IF_ADMIN_ACCESS_PAIR]


def get_compute_record_information(compute_record, routing_table: "RoutingTable", with_state: bool = False) -> Dict[str, Any]:
    compute_record_details = routing_table.describe_compute_record(compute_record)
    compute_record_info = {
        "trigger_timestamp_utc": compute_record.trigger_timestamp_utc,
        "deactivated_timestamp_utc": compute_record.deactivated_timestamp_utc,
        "state": repr(compute_record.state),
        "number_of_attempts_on_failure": compute_record.number_of_attempts_on_failure,
    }
    if with_state:
        compute_record_info.update({"state_state": dumps(compute_record.state)})
        compute_record_info.update({"session_state": dumps(compute_record.session_state)})
    if not compute_record_details:
        generic_details = None
        if compute_record.session_state and compute_record.session_state.executions:
            generic_details = compute_record.session_state.executions[::-1][0].details
        else:
            generic_details = f"Could not retrieve details for compute record from the platform!"
        generic_slot = dict()
        generic_slot["type"] = compute_record.slot.type
        generic_slot["lang"] = compute_record.slot.code_lang
        generic_slot["code"] = compute_record.slot.code
        generic_slot["code_abi"] = compute_record.slot.code_abi
        compute_record_info.update({"slot": compute_record.slot, "details": generic_details})
    else:
        compute_record_info.update(
            {
                "slot": compute_record_details.get("slot", None),
                "details": compute_record_details.get("details", None),
            }
        )
    return compute_record_info


def get_node_information(params, input_map, materialized_output) -> Dict[str, Any]:
    input_signal_info = {}
    output_signal_info = {}

    for input_signal_key in input_map.keys():
        input_signal_alias = input_map[input_signal_key].alias
        input_signal_info[input_signal_alias] = get_dimension_info_for_signal(input_map[input_signal_key])

    output_signal_info[materialized_output.alias] = get_dimension_info_for_signal(materialized_output)
    node_info = {}
    node_info["APP_CONTEXT_ID"] = params["UNIQUE_ID_FOR_CONTEXT"]
    node_info["CALLBACK_TYPE"] = params.get(RoutingHookInterface.HOOK_TYPE_PARAM, RoutingComputeInterface.IInlinedCompute).__name__
    node_info["OUTPUT"] = output_signal_info
    node_info["INPUTS"] = input_signal_info
    # node_info['CALLBACK_PARAMS'] = repr(_cleanup_params(dict(params)))

    return node_info


def get_pending_node_information(params, ready_input_map, pending_input_map, materialized_output) -> Dict[str, Any]:
    input_signal_info = {}
    pending_input_signal_info = {}

    for input_signal_key in ready_input_map.keys():
        input_signal_alias = ready_input_map[input_signal_key].alias
        input_signal_info[input_signal_alias] = get_dimension_info_for_signal(ready_input_map[input_signal_key])

    for input_signal_key in pending_input_map.keys():
        input_signal_alias = pending_input_map[input_signal_key].alias
        pending_input_signal_info[input_signal_alias] = get_dimension_info_for_signal(pending_input_map[input_signal_key])

    node_info = {}
    node_info["APP_CONTEXT_ID"] = params["UNIQUE_ID_FOR_CONTEXT"]
    node_info["CALLBACK_TYPE"] = params.get(RoutingHookInterface.HOOK_TYPE_PARAM, RoutingComputeInterface.IInlinedCompute).__name__
    if materialized_output:
        output_signal_info = {}
        output_signal_info[materialized_output.alias] = get_dimension_info_for_signal(materialized_output)
        node_info["OUTPUT"] = output_signal_info
    else:
        node_info["OUTPUT"] = "Not materialized yet (needs at least one more pending input to be ready)"
    node_info["READY_INPUTS"] = input_signal_info
    node_info["PENDING_INPUTS"] = pending_input_signal_info
    # node_info['CALLBACK_PARAMS'] = _cleanup_params(dict(params))

    return node_info


def get_route_information_from_callback(
    input_map_OR_routing_table: Union[Dict[str, Signal], "RoutingTable"],
    materialized_output_OR_route_record: Union[Signal, "RoutingTable.RouteRecord"],
    *args,
    **params,
) -> Tuple[str, str]:
    """Returns a tuple of (RoutID <str>, repr'd Node information dict <str>)"""
    inlined_compute_args = InlinedComputeParamExtractor(input_map_OR_routing_table, materialized_output_OR_route_record, *args, **params)
    if inlined_compute_args:
        # INLINED_COMPUTE callback
        input_map, materialized_output, params = inlined_compute_args
        node_info = get_node_information(params, input_map, materialized_output)
        route_id = materialized_output.alias
    else:  # it is a HOOK!
        routing_table = input_map_OR_routing_table
        route_record = materialized_output_OR_route_record
        route_id = route_record.route.route_id
        callback_type = params[RoutingHookInterface.HOOK_TYPE_PARAM]
        from intelliflow.core.signal_processing.routing_runtime_constructs import Route

        if callback_type in [RoutingHookInterface.Execution.IExecutionBeginHook, RoutingHookInterface.Execution.IExecutionSkippedHook]:
            _, _, execution_context, _, _ = ExecutionInitHookParamExtractor(
                input_map_OR_routing_table, materialized_output_OR_route_record, *args, **params
            )
            materialized_inputs: List[Signal] = execution_context.completed_link_node.ready_signals
            input_map = {input.alias: input for input in materialized_inputs}
            materialized_output: Signal = execution_context.output
            node_info = get_node_information(params, input_map, materialized_output)
            # TODO read
            # node_info["EXECUTION_SLOTS"] = [repr(slot) for slot in execution_context.slots]
        elif callback_type in [RoutingHookInterface.Execution.IExecutionSuccessHook, RoutingHookInterface.Execution.IExecutionFailureHook]:
            _, _, _, materialized_inputs, materialized_output, _, _ = ExecutionContextHookParamExtractor(
                input_map_OR_routing_table, materialized_output_OR_route_record, *args, **params
            )
            input_map = {input.alias: input for input in materialized_inputs}
            node_info = get_node_information(params, input_map, materialized_output)
            # TODO find the active compute record from RouteRecord (it should still be there, fix constructs if it is deactivated
            #   before the callback). Or directly pass it to the callback, but in that case interface needs to be changed.
        elif callback_type in [
            RoutingHookInterface.Execution.IComputeSuccessHook,
            RoutingHookInterface.Execution.IComputeFailureHook,
            RoutingHookInterface.Execution.IComputeRetryHook,
        ]:
            _, _, compute_record, _, _ = ComputeHookParamExtractor(
                input_map_OR_routing_table, materialized_output_OR_route_record, *args, **params
            )
            materialized_inputs: List[Signal] = compute_record.materialized_inputs
            input_map = {input.alias: input for input in materialized_inputs}
            materialized_output: Signal = compute_record.materialized_output
            node_info = get_node_information(params, input_map, materialized_output)
            node_info.update({"COMPUTE_RECORD": get_compute_record_information(compute_record, routing_table)})
        elif callback_type in [RoutingHookInterface.Execution.IExecutionCheckpointHook]:
            _, _, _, active_compute_records, _, _, _ = ExecutionCheckPointHookParamExtractor(
                input_map_OR_routing_table, materialized_output_OR_route_record, *args, **params
            )
            materialized_inputs: List[Signal] = active_compute_records[0].materialized_inputs
            input_map = {input.alias: input for input in materialized_inputs}
            materialized_output: Signal = active_compute_records[0].materialized_output
            node_info = get_node_information(params, input_map, materialized_output)
            node_info.update(
                {
                    "COMPUTE_RECORDS": [
                        get_compute_record_information(compute_record, routing_table) for compute_record in active_compute_records
                    ]
                }
            )
        elif callback_type in [
            RoutingHookInterface.PendingNode.IPendingNodeCreationHook,
            RoutingHookInterface.PendingNode.IPendingNodeExpirationHook,
            RoutingHookInterface.PendingNode.IPendingCheckpointHook,
        ]:
            if callback_type in [
                RoutingHookInterface.PendingNode.IPendingNodeCreationHook,
                RoutingHookInterface.PendingNode.IPendingNodeExpirationHook,
            ]:
                _, _, pending_node, _, _ = PendingNodeHookParamExtractor(
                    input_map_OR_routing_table, materialized_output_OR_route_record, *args, **params
                )
            elif callback_type in [RoutingHookInterface.PendingNode.IPendingCheckpointHook]:
                _, _, pending_node, _, _, _ = PendingNodeCheckpointHookParamExtractor(
                    input_map_OR_routing_table, materialized_output_OR_route_record, *args, **params
                )
            ready_input_map = {input.alias: input for input in pending_node.ready_signals}
            pending_input_map = {input.alias: input for input in pending_node.signals if input not in pending_node.ready_signals}
            materialized_output: Signal = None
            try:
                # unmaterialized dimensions will like '*' (for Any) and/or ':+/-range' (for Relative) thanks to 'force'
                materialized_output = pending_node.materialize_output(
                    route_record.route.output, route_record.route.output_dim_matrix, force=True
                )
            except:
                pass
            node_info = get_pending_node_information(params, ready_input_map, pending_input_map, materialized_output)

    return (route_id, json.dumps(node_info, indent=11, default=repr))
