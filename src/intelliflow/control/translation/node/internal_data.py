import copy
import json
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, List, cast

from _collections_abc import Iterator
from dateutil import tz

from intelliflow.core.application.context.instruction import Instruction
from intelliflow.core.application.context.node.external.nodes import GlueTableDataNode, S3DataNode
from intelliflow.core.application.context.node.internal.nodes import InternalDataNode
from intelliflow.core.application.core_application import ApplicationState
from intelliflow.core.platform.compute_targets.common_node_desc_utils import get_compute_record_information
from intelliflow.core.platform.constructs import RoutingTable
from intelliflow.core.platform.definitions.compute import (
    ComputeFailedResponse,
    ComputeFailedSessionState,
    ComputeLogQuery,
    ComputeResponse,
    ComputeResponseType,
    ComputeSessionState,
    ComputeSessionStateType,
)
from intelliflow.core.serialization import loads
from intelliflow.core.signal_processing.routing_runtime_constructs import Route, RuntimeLinkNode
from intelliflow.core.signal_processing.signal import Signal
from intelliflow.core.signal_processing.signal_source import (
    AndesSignalSourceAccessSpec,
    GlueTableSignalSourceAccessSpec,
    InternalDatasetSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
    TimerSignalSourceAccessSpec,
)


def map_inactive_compute_record(inactiveRecord: RoutingTable.ComputeRecord, routingTable: RoutingTable) -> Dict[str, Any]:
    # inactiveRecord.number_of_attempts_on_failure
    computeRecordInfo = get_compute_record_information(inactiveRecord, routingTable, with_state=True)
    slotDetails = computeRecordInfo.get("slot", None)
    currentRecordTriggerTimestampUtc = computeRecordInfo.get("trigger_timestamp_utc", None)
    if currentRecordTriggerTimestampUtc:
        currentRecordTriggerTime = datetime.fromtimestamp(currentRecordTriggerTimestampUtc)
        computeRecordInfo.update({"trigger_timestamp": currentRecordTriggerTime, "slot": slotDetails})
    currentRecordDeactivatedTimestampUtc = computeRecordInfo.get("deactivated_timestamp_utc", None)
    if currentRecordDeactivatedTimestampUtc:
        currentRecordDeactivatedTime = datetime.fromtimestamp(currentRecordDeactivatedTimestampUtc)
        computeRecordInfo.update({"deactivated_timestamp": currentRecordDeactivatedTime, "slot": slotDetails})
    computeRecordState = computeRecordInfo.get("state_state", None)
    initial_state: ComputeResponse = loads(computeRecordState)
    if initial_state.response_type == ComputeResponseType.SUCCESS:

        session_state: ComputeSessionState = loads(computeRecordInfo.get("session_state", None))
        initial_state_value = initial_state.response_type.value
        if session_state:
            overall_state_value = execution_state_value = session_state.state_type.value
            if session_state.state_type == ComputeSessionStateType.FAILED:
                execution_state_value = session_state.state_type.value
        else:
            execution_state_value = None
            # It can be transient. If execution_state and overall_state
            overall_state_value = ComputeSessionStateType.PROCESSING
    else:
        failed_initial_state: ComputeFailedResponse = initial_state
        overall_state_value = initial_state.response_type.value
        initial_state_value = failed_initial_state.failed_response_type.value
        execution_state_value = None
    #     Check if state is null (Initial state) --success--> Check session_state (execution state)
    # Extract the input and output signals
    inputSignals = []
    for input in inactiveRecord.materialized_inputs:
        inputSignals.append(
            {
                "alias": input.alias,
                "path": input.get_materialized_resource_paths()[0] if input.get_materialized_resource_paths() else "",
                "dimensionValueMap": input.dimension_values_map(),
            }
        )
    outputSignal = dict()
    if inactiveRecord.materialized_output:
        outputSignal.update(
            {
                "alias": inactiveRecord.materialized_output.alias,
                "path": (
                    inactiveRecord.materialized_output.get_materialized_resource_paths()[0]
                    if inactiveRecord.materialized_output.get_materialized_resource_paths()
                    else ""
                ),
                "dimensionValueMap": inactiveRecord.materialized_output.dimension_values_map(),
                "dimensionTypeMap": inactiveRecord.materialized_output.dimension_type_map(),
            }
        )

    return {
        "inputSignals": inputSignals,
        "outputSignal": outputSignal,
        "description": computeRecordInfo,
        "initialState": initial_state_value,
        "executionState": execution_state_value,
        "overallState": overall_state_value,
    }


def map_active_compute_record(activeRecord: RoutingTable.ComputeRecord, routingTable: RoutingTable) -> Dict[str, Any]:
    computeRecordInfo = get_compute_record_information(activeRecord, routingTable, with_state=True)
    computeRecordInfo.pop("deactivated_timestamp_utc", None)
    slotDetails = computeRecordInfo.get("slot", None)
    currentRecordTriggerTimestampUtc = computeRecordInfo.get("trigger_timestamp_utc", None)
    if currentRecordTriggerTimestampUtc:
        currentRecordTriggerTime = datetime.fromtimestamp(currentRecordTriggerTimestampUtc)
        computeRecordInfo.update({"trigger_timestamp": currentRecordTriggerTime, "slot": slotDetails})
    computeRecordState = computeRecordInfo.get("state_state", None)
    initial_state: ComputeResponse = loads(computeRecordState)
    if initial_state.response_type == ComputeResponseType.SUCCESS:

        session_state: ComputeSessionState = loads(computeRecordInfo.get("session_state", None))
        initial_state_value = initial_state.response_type.value
        if session_state:
            overall_state_value = execution_state_value = session_state.state_type.value
            if session_state.state_type == ComputeSessionStateType.FAILED:
                execution_state_value = cast(ComputeFailedSessionState, session_state).failed_type.value
        else:
            execution_state_value = None
            # It can be transient. If execution_state and overall_state
            overall_state_value = ComputeSessionStateType.PROCESSING
    else:
        failed_initial_state: ComputeFailedResponse = initial_state
        overall_state_value = initial_state.response_type.value
        initial_state_value = failed_initial_state.failed_response_type.value
        execution_state_value = None
    # Extract the input and output signals
    inputSignals = []
    for input in activeRecord.materialized_inputs:
        inputSignals.append(
            {
                "alias": input.alias,
                "path": input.get_materialized_resource_paths()[0] if input.get_materialized_resource_paths() else "",
                "dimensionValueMap": input.dimension_values_map(),
            }
        )
    outputSignal = dict()
    if activeRecord.materialized_output:
        outputSignal.update(
            {
                "alias": activeRecord.materialized_output.alias,
                "path": (
                    activeRecord.materialized_output.get_materialized_resource_paths()[0]
                    if activeRecord.materialized_output.get_materialized_resource_paths()
                    else ""
                ),
                "dimensionValueMap": activeRecord.materialized_output.dimension_values_map(),
                "dimensionTypeMap": activeRecord.materialized_output.dimension_type_map(),
            }
        )

    return {
        "inputSignals": inputSignals,
        "outputSignal": outputSignal,
        "description": computeRecordInfo,
        "initialState": initial_state_value,
        "executionState": execution_state_value,
        "overallState": overall_state_value,
    }


def map_pending_node(pendingNode: RuntimeLinkNode) -> Dict[str, Any]:
    # Extract the details of signals which the node is waiting for
    expectedInputs = []
    if pendingNode.signals:
        for pendingSignal in pendingNode.signals:
            # serializedPendingSignal = pendingSignal.to_json()
            expectedInputs.append(
                {
                    "alias": pendingSignal.alias,
                    "path": pendingSignal.get_materialized_resource_paths()[0] if pendingSignal.get_materialized_resource_paths() else "",
                }
            )

    # Extract the details of signals which the node has already received
    receivedInputs = []
    if pendingNode.ready_signals:
        for readySignal in pendingNode.ready_signals:
            waitingForDependencies = False
            dependencyList = pendingNode.range_check_state.get(readySignal.unique_key(), None)
            if dependencyList and dependencyList.remaining_paths:
                waitingForDependencies = True

            isBlocked = False
            blocked_range = pendingNode.blocked_range_check_state.get(readySignal.unique_key(), None)
            if blocked_range:
                isBlocked = True

            receivedInputs.append(
                {
                    "alias": readySignal.alias,
                    "path": readySignal.get_materialized_resource_paths()[0] if readySignal.get_materialized_resource_paths() else "",
                    "waitingForDependencies": waitingForDependencies,
                    "isBlocked": isBlocked,
                    "inputDimensionValueMap": readySignal.dimension_values_map(),
                }
            )
    dependencyCheckState = {}
    if pendingNode.range_check_state:
        dependencyCheckState = pendingNode.range_check_state

    blocked_inputs_state = {}
    if pendingNode.blocked_range_check_state:
        blocked_inputs_state = pendingNode.blocked_range_check_state

    return {
        "pendingNodeId": pendingNode.node_id,
        "expectedInputs": expectedInputs,
        "receivedInputs": receivedInputs,
        "blockedInputs": {key.alias: sorted(list(value)) for key, value in blocked_inputs_state.items()},
        "dependencyCheckState": {
            key.alias: {
                "remaining_paths": sorted(list(value.remaining_paths)),
                "completed_paths": sorted(list(value.completed_paths)),
            }
            for key, value in dependencyCheckState.items()
        },
    }
