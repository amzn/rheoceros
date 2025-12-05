import copy
import json
import traceback
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

from _collections_abc import Iterator
from dateutil import tz

from intelliflow.api_ext import DataFrameFormat
from intelliflow.core.application.application import Application
from intelliflow.core.application.context.instruction import Instruction
from intelliflow.core.application.context.node.external.nodes import GlueTableDataNode, S3DataNode
from intelliflow.core.application.context.node.internal.nodes import InternalDataNode
from intelliflow.core.application.core_application import ApplicationState
from intelliflow.core.application.remote_application import RemoteApplication
from intelliflow.core.platform.compute_targets.common_node_desc_utils import get_compute_record_information
from intelliflow.core.platform.compute_targets.descriptor import ComputeDescriptor
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
from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.dimension_constructs import (
    DIMENSION_VARIANT_IDENTICAL_MAP_FUNC,
    DimensionFilter,
    DimensionVariantMapper,
)
from intelliflow.core.signal_processing.routing_runtime_constructs import Route, RouteID, RuntimeLinkNode
from intelliflow.core.signal_processing.signal import Signal
from intelliflow.core.signal_processing.signal_source import (
    AndesSignalSourceAccessSpec,
    GlueTableSignalSourceAccessSpec,
    InternalDatasetSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
    TimerSignalSourceAccessSpec,
)

from .translation.node.internal_data import map_active_compute_record, map_inactive_compute_record, map_pending_node


def _get_hook_summary(hook: "Slot"):
    code = loads(hook.code)
    if isinstance(code, ComputeDescriptor):
        return {
            "type": "ComputeDescriptor",
            "descriptor_type": code.__class__.__name__,
            "details": code.describe_slot(),
        }
    else:
        return {"type": "Callable", "code": hook.describe_code()}


def _get_instruction_summary(app, dev_context, instruction):
    signal1: Signal = instruction.output_node.signal()

    type = signal1.type.name
    args = instruction.args[0]
    specific_type = args.__class__

    node_summary = None
    upstream_input_instructions = {}
    if type == "EXTERNAL_S3_OBJECT_CREATION":
        s3SignalSourceAccessSpec: S3SignalSourceAccessSpec = signal1.resource_access_spec
        account_Id = s3SignalSourceAccessSpec.account_id
        data_format = s3SignalSourceAccessSpec.data_format
        data_folder = s3SignalSourceAccessSpec.data_folder
        encryption_key = s3SignalSourceAccessSpec.encryption_key
        bucket_name = s3SignalSourceAccessSpec.bucket
        partition_keys: [] = s3SignalSourceAccessSpec.partition_keys
        path_format = s3SignalSourceAccessSpec.path_format
        attrs = s3SignalSourceAccessSpec.attrs
        owner_context_uuid = attrs.get("owner_context_uuid")
        glueTableSignalSourceAccessSpec: GlueTableSignalSourceAccessSpec = (
            s3SignalSourceAccessSpec.proxy if (specific_type == GlueTableDataNode.Descriptor) else ""
        )
        table_name = glueTableSignalSourceAccessSpec.table_name if (specific_type == GlueTableDataNode.Descriptor) else ""
        database = glueTableSignalSourceAccessSpec.database if (specific_type == GlueTableDataNode.Descriptor) else ""

        node_summary = {
            "id": instruction.output_node._id,
            "child_nodes": get_node_outbound(instruction),
            "metadata": {
                "account_id": account_Id,
                "table_name": table_name,
                "data_format": data_format,
                "encryption_key": encryption_key,
                "bucket_name": bucket_name,
                "partition_keys": partition_keys,
                "partition_filters": signal1.domain_spec.dimension_filter_spec.pretty(),
                "path_format": path_format,
                "owner_context_uuid": owner_context_uuid,
                "database": database,
                "data_folder": (data_folder if data_folder else ""),
            },
            "node_type": "GLUE_TABLE" if (specific_type == GlueTableDataNode.Descriptor) else "EXTERNAL_S3_NODE",
        }
    elif type == "INTERNAL_PARTITION_CREATION":
        internalSourceAccessSpec: InternalDatasetSignalSourceAccessSpec = signal1.resource_access_spec
        folder = internalSourceAccessSpec.folder
        partition_keys: [] = internalSourceAccessSpec.partition_keys
        path_format = internalSourceAccessSpec.path_format
        source = internalSourceAccessSpec.source

        route: Route = cast("InternalDataNode", instruction.symbol_tree).create_route()
        input_signals: List[Signal] = route.link_node.signals
        # find upstream inputs separately. we use dev_context.instructions below for "inputs" and it won't have'em.
        for input_signal in input_signals:
            owner_context_uuid = input_signal.resource_access_spec.get_owner_context_uuid()
            if owner_context_uuid != app.uuid:
                upstream_instruction = None
                for parent_app in dev_context.external_data:
                    # parent_app.remote_app should be None for "grandma" apps. let instruction be None for those.
                    if parent_app.remote_app and owner_context_uuid == parent_app.uuid:
                        # we have to map the input back to its application's domain
                        # (INTERNAL -> (child app consumes as) EXTERNAL -> INTERNAL)
                        mapped_input_signal = parent_app.remote_app.platform.storage.map_materialized_signal(input_signal)
                        if mapped_input_signal is None:
                            # external signal in parent app, use the input as is
                            mapped_input_signal = input_signal
                        for parent_instruction in parent_app.remote_app.active_context.instruction_chain:
                            upstream_signal: Signal = parent_instruction.output_node.signal()
                            if upstream_signal.clone(alias=None, deep=False) == mapped_input_signal.clone(alias=None, deep=False):
                                upstream_instruction = (parent_app, parent_instruction)
                                # DONOT break, find the latest instruction that matches
                        if upstream_instruction:
                            break
                upstream_input_instructions[input_signal] = upstream_instruction

        execution_hook = {}
        if route.execution_hook:
            if route.execution_hook.on_exec_begin:
                execution_hook.update({"on_exec_begin": _get_hook_summary(route.execution_hook.on_exec_begin)})
            if route.execution_hook.on_exec_skipped:
                execution_hook.update({"on_exec_skipped": _get_hook_summary(route.execution_hook.on_exec_skipped)})
            if route.execution_hook.on_compute_success:
                execution_hook.update({"on_compute_success": _get_hook_summary(route.execution_hook.on_compute_success)})
            if route.execution_hook.on_compute_failure:
                execution_hook.update({"on_compute_failure": _get_hook_summary(route.execution_hook.on_compute_failure)})
            if route.execution_hook.on_compute_retry:
                execution_hook.update({"on_compute_retry": _get_hook_summary(route.execution_hook.on_compute_retry)})
            if route.execution_hook.on_success:
                execution_hook.update({"on_success": _get_hook_summary(route.execution_hook.on_success)})
            if route.execution_hook.on_failure:
                execution_hook.update({"on_failure": _get_hook_summary(route.execution_hook.on_failure)})
            if route.execution_hook.checkpoints:
                execution_hook.update(
                    {
                        "checkpoints": [
                            {
                                "checkpoint_in_secs": checkpoint.checkpoint_in_secs,
                                "callback": _get_hook_summary(checkpoint.slot),
                            }
                            for checkpoint in route.execution_hook.checkpoints
                        ]
                    }
                )

            if route.execution_hook.get_metadata_actions:
                execution_hook.update(
                    {
                        "metadata_actions": [
                            {
                                "condition": metadata_action.describe_condition(),
                                "callback": _get_hook_summary(metadata_action.slot),
                            }
                            for metadata_action in route.execution_hook.get_metadata_actions
                        ]
                    }
                )

        pending_node_hook = {}
        if route.pending_node_hook:
            if route.pending_node_hook.on_pending_node_created:
                pending_node_hook.update({"on_pending_node_created": _get_hook_summary(route.pending_node_hook.on_pending_node_created)})
            if route.pending_node_hook.on_pending_node_created:
                pending_node_hook.update({"on_pending_node_created": _get_hook_summary(route.pending_node_hook.on_pending_node_created)})
            if route.pending_node_hook.checkpoints:
                pending_node_hook.update(
                    {
                        "checkpoints": [
                            {
                                "checkpoint_in_secs": checkpoint.checkpoint_in_secs,
                                "callback": _get_hook_summary(checkpoint.slot),
                            }
                            for checkpoint in route.pending_node_hook.checkpoints
                        ]
                    }
                )

        output_retention = {}
        if route.output_retention:
            if route.output_retention.condition:
                output_retention.update({"condition": route.output_retention.describe_condition()})

            if route.output_retention.refresh_period_in_secs:
                output_retention.update({"refresh_period_in_secs": route.output_retention.refresh_period_in_secs})

            if route.output_retention.rip_slot:
                output_retention.update({"rip_hook": _get_hook_summary(route.output_retention.rip_slot)})

            if route.output_retention.refresh_slot:
                output_retention.update({"refresh_hook": _get_hook_summary(route.output_retention.refresh_slot)})

        node_summary = {
            "id": instruction.output_node._id,
            "child_nodes": get_node_outbound(instruction),
            "metadata": {
                "folder": folder,
                "partition_keys": partition_keys,
                "partition_values": signal1.dimension_values_map(),
                "partition_types": signal1.dimension_type_map(),
                "partition_filters": signal1.domain_spec.dimension_filter_spec.pretty(),
                "data_attributes": {
                    "data_type": (signal1.resource_access_spec.data_type.value if signal1.resource_access_spec.data_type else None),
                    "data_format": signal1.resource_access_spec.data_format.value,
                    "data_header_exists": signal1.resource_access_spec.data_header_exists,
                    "data_schema_file": signal1.resource_access_spec.data_schema_file,
                    "data_schema_type": (
                        signal1.resource_access_spec.data_schema_type.value if signal1.resource_access_spec.data_schema_type else None
                    ),
                    "data_delimiter": signal1.resource_access_spec.data_delimiter,
                    "data_folder": (signal1.resource_access_spec.data_folder if signal1.resource_access_spec.data_folder else ""),
                },
                "path_format": path_format,
                "source": source,
                "inputs": [
                    {
                        "node_id": inst_link.instruction.output_node._id,
                        "alias": input_signals[i].alias,
                        "owner_context_uuid": input_signals[i].resource_access_spec.get_owner_context_uuid(),
                        "is_reference": input_signals[i].is_reference,
                        "range_check_required": input_signals[i].range_check_required,
                        "nearest_the_tip_in_range": input_signals[i].nearest_the_tip_in_range,
                        "partitions": input_signals[i].get_materialized_resource_paths(),
                        "partition_values": input_signals[i].dimension_values_map(),
                        "partition_filters": input_signals[i].domain_spec.dimension_filter_spec.pretty(),
                    }
                    for i, inst_link in enumerate(instruction.inbound)
                    if inst_link.instruction
                ],
                "upstream_inputs": [
                    {
                        # do not expose nodes from grandma
                        "node_id": (instruction[1].output_node._id if instruction else ""),
                        "is_grand_app": (instruction is None),
                        "alias": input_signal.alias,
                        "owner_context_uuid": input_signal.resource_access_spec.get_owner_context_uuid(),
                        "is_reference": input_signal.is_reference,
                        "range_check_required": input_signal.range_check_required,
                        "nearest_the_tip_in_range": input_signal.nearest_the_tip_in_range,
                        "partitions": input_signal.get_materialized_resource_paths(),
                        "partition_values": input_signal.dimension_values_map(),
                        "partition_filters": input_signal.domain_spec.dimension_filter_spec.pretty(),
                    }
                    for input_signal, instruction in upstream_input_instructions.items()
                ],
                # For nodes with no inputs, abstract GROUND TETHER signal won't have any instruction
                "slots": [
                    {
                        "type": slot.type.value,
                        "code": slot.code,
                        "code_lang": (slot.code_lang.value if slot.code_lang else Lang.PYTHON.value),
                        "code_abi": (slot.code_abi.value if slot.code_abi else ABI.NONE.value),
                        "code_metadata": {
                            "code_type": slot.code_metadata.code_type,
                            "target_entity": slot.code_metadata.target_entity,
                            "target_method": slot.code_metadata.target_method,
                            "external_library_paths": slot.code_metadata.external_library_paths,
                        },
                        "extra_params": slot.extra_params,
                        "extra_permissions": slot.compute_permissions,
                        "retry_count": slot.retry_count,
                    }
                    for slot in route.slots
                ],
                "execution_hook": execution_hook,
                "pending_node_hook": pending_node_hook,
                "output_retention": output_retention,
            },
            "node_type": "INTERNAL_DATA_NODE",
        }

    elif type == "TIMER_EVENT":
        timerSourceAccessSpec: TimerSignalSourceAccessSpec = signal1.resource_access_spec
        schedule_expression = timerSourceAccessSpec.schedule_expression

        node_summary = {
            "id": instruction.output_node._id,
            "child_nodes": get_node_outbound(instruction),
            "metadata": {
                "schedule": schedule_expression,
                "dimensions": [
                    (dim_name, dim.type, dim.params)
                    for dim_name, dim in signal1.domain_spec.dimension_spec.get_flattened_dimension_map().items()
                ],
                "dimension_filters": signal1.domain_spec.dimension_filter_spec.pretty(),
            },
            "node_type": "TIMER_EVENT",
        }

    elif type == "EXTERNAL_ANDES_SNAPSHOT_CREATION":
        andesSignalSourceAccessSpec: AndesSignalSourceAccessSpec = signal1.resource_access_spec
        provider = andesSignalSourceAccessSpec.provider
        table_name = andesSignalSourceAccessSpec.table_name
        partition_keys = andesSignalSourceAccessSpec.partition_keys
        primary_keys = andesSignalSourceAccessSpec.primary_keys
        path_format = andesSignalSourceAccessSpec.path_format
        storage_type = andesSignalSourceAccessSpec.storage_type

        node_summary = {
            "id": instruction.output_node._id,
            "inbound": get_node_inbound(instruction),
            "outbound": get_node_outbound(instruction),
            "metadata": {
                "provider": provider,
                "table_name": table_name,
                "partition_keys": partition_keys,
                "partition_filters": signal1.domain_spec.dimension_filter_spec.pretty(),
                "primary_keys": primary_keys,
                "path_format": path_format,
                "storage_type": storage_type,
            },
            "node_type": "EXTERNAL_ANDES_SNAPSHOT_CREATION",
        }

    else:
        node_summary = {
            "id": instruction.output_node._id,
            "inbound": get_node_inbound(instruction),
            "outbound": get_node_outbound(instruction),
            "metadata": {
                "source_type": signal1.resource_access_spec.source.value,
                "path_format": signal1.resource_access_spec.path_format,
                "dimensions": [
                    (dim_name, dim.type, dim.params)
                    for dim_name, dim in signal1.domain_spec.dimension_spec.get_flattened_dimension_map().items()
                ],
                "dimension_filters": signal1.domain_spec.dimension_filter_spec.pretty(),
                "attributes": copy.deepcopy(signal1.resource_access_spec.attrs),
            },
            "node_type": signal1.resource_access_spec.source.value,
        }

    return (node_summary, upstream_input_instructions)


def get_node_inbound(instruction: Instruction):
    data = []
    for instructionLink in instruction.inbound:
        if instructionLink.instruction:
            data.append({"node_id": instructionLink.instruction.output_node._id})
    return data


def get_node_outbound(instruction: Instruction):
    data = []
    if instruction.outbound.values():
        for i in range(0, len(instruction.outbound.values())):
            for instructionLink in list(instruction.outbound.values())[i]:
                if instructionLink.instruction:
                    data.append({"node_id": instructionLink.instruction.output_node._id})
    return data


def get_nodes_summary(app):
    """Get summary of nodes in the application including their metadata and relationships."""
    dev_context = app._active_context or app._dev_context

    data = _get_nodes_summary(app, dev_context)

    parent_data = {
        parent_app.uuid: {
            "id": parent_app.id,
            "state": parent_app.remote_app.state.name,
            "nodes": _get_nodes_summary(parent_app, parent_app.remote_app.active_context),
        }
        for parent_app in dev_context.external_data
    }

    children = {child_app.conf._generate_unique_id_for_context(): {"id": child_app.id} for child_app in dev_context.downstream_dependencies}

    result = {"nodes": data, "parents": parent_data, "children": children}

    return result


def _get_nodes_summary(app, dev_context):
    data = []
    upstream_nodes = {}
    for instruction in dev_context.instruction_chain:
        node_summary, upstream_input_instructions = _get_instruction_summary(app, dev_context, instruction)
        if "metadata" in node_summary:
            node_summary["metadata"]["owner_context_uuid"] = app.uuid
        data.append(node_summary)
        for _, parent_app_and_instruction in upstream_input_instructions.items():
            if parent_app_and_instruction:  # skip grandapps
                parent_app, parent_instruction = parent_app_and_instruction
                upstream_nodes.setdefault(parent_app.uuid, {})[parent_instruction.output_node._id] = (
                    parent_app,
                    parent_instruction,
                )

    upstream_data = []
    for parent_uuid, imported_nodes_map in upstream_nodes.items():
        for node_id, parent_app_instruction in imported_nodes_map.items():
            parent_app, instruction = parent_app_instruction
            node_summary, _ = _get_instruction_summary(parent_app, parent_app.remote_app.active_context, instruction)
            if "metadata" in node_summary:
                node_summary["metadata"]["owner_context_uuid"] = parent_uuid
            upstream_data.append(node_summary)

    if upstream_data:
        data = upstream_data + data

    return data


def get_nodes_exec_summary(app):
    """Get execution summary of nodes in the application."""
    dev_context = app._active_context or app._dev_context
    routing_table: RoutingTable = app.platform.routing_table

    active_records_data = {}
    # TODO uncomment when get_active_compute_records is moved into Application
    #  Or ApplicationExt will be turned into plugins for Application that would be auto-register even for RemoteApplication
    # active_compute_records: Iterator[RoutingTable.ComputeRecord] = app.get_active_compute_records()
    active_compute_records: Iterator[RoutingTable.ComputeRecord] = app.platform.routing_table.load_active_compute_records()
    for activeRecord in active_compute_records:
        mapped_data = map_active_compute_record(activeRecord, routing_table)
        # del mapped_data["inputSignals"]
        node_id = activeRecord.materialized_output.alias
        active_records_data.setdefault(node_id, []).append(mapped_data)

    pending_nodes_data = {}
    # TODO uncomment after ApplicationExt refactoring
    # pending_nodes: Iterator[Tuple[RouteID, RuntimeLinkNode]] = app.get_pending_nodes()
    pending_nodes: Iterator[Tuple[RouteID, RuntimeLinkNode]] = app.platform.routing_table.load_pending_nodes()
    for route_id, pending_node in pending_nodes:
        mapped_data = map_pending_node(pending_node)
        # we will later use this pending node ref to materialize the output
        mapped_data["_pending_node"] = pending_node
        pending_nodes_data.setdefault(route_id, []).append(mapped_data)

    data = []
    for instruction in dev_context.instruction_chain:
        output_signal: Signal = instruction.output_node.signal()

        type = output_signal.type.name
        if type == "INTERNAL_PARTITION_CREATION":
            route: Route = cast("InternalDataNode", instruction.symbol_tree).create_route()
            route_id = route.route_id
            node_id = instruction.output_node._id

            pending_response = pending_nodes_data.get(route_id, [])
            for pending_data in pending_response:
                pending_node: RuntimeLinkNode = pending_data["_pending_node"]
                materialized_output: Signal = pending_node.materialize_output(
                    route.output, route.output_dim_matrix, force=True, allow_partial=True
                )
                pending_data["outputSignal"] = {
                    "path": (
                        materialized_output.get_materialized_resource_paths()[0]
                        if materialized_output.get_materialized_resource_paths()
                        else ""
                    ),
                    "dimensionValueMap": materialized_output.dimension_values_map(),
                    "dimensionTypeMap": materialized_output.dimension_type_map(),
                }
                del pending_data["_pending_node"]

            active_response = active_records_data.get(node_id, [])

            node = {
                "id": node_id,
                "node_type": "INTERNAL_DATA_NODE",
                "execution_data": {
                    "inactiveRecords": [],
                    "activeRecords": active_response,
                    "pendingRecords": pending_response,
                },
            }
            data.append(node)

    result = {"nodes": data}
    return result


def get_nodes_exec_dimensions_summary(app, limit=300):
    """Get summary of execution dimensions for nodes in the application."""
    dimensions_value_summary: Dict[str, List[Any]] = {}
    inactive_compute_records: Iterator[RoutingTable.ComputeRecord] = app.get_inactive_compute_records(limit=limit)

    for inactiveRecord in inactive_compute_records:
        if inactiveRecord:
            output_signal = inactiveRecord.materialized_output
            dim_value_map = output_signal.dimension_values_map()
            for key, values in dim_value_map.items():
                curr_values = dimensions_value_summary.get(key, [])
                dimensions_value_summary[key] = list(set(curr_values).union(values))

    result = {"dimension_values": dimensions_value_summary}
    return result


def _get_filters(compute_record: RoutingTable.ComputeRecord, dimensionValueMap: Dict[str, List[Any]]) -> List[DimensionFilter]:
    filters: List[DimensionFilter] = []
    dim_names = [
        key for key in dimensionValueMap.keys() if compute_record.materialized_output.domain_spec.dimension_spec.find_dimension_by_name(key)
    ]
    dimension_mappers = []
    for dim_name in dim_names:
        values = dimensionValueMap[dim_name]
        for value in values:
            mapper_from_own_filter = DimensionVariantMapper(source=dim_name, target=dim_name, func=DIMENSION_VARIANT_IDENTICAL_MAP_FUNC)
            mapper_from_own_filter.set_materialized_value(value)
            dimension_mappers.append(mapper_from_own_filter)

    # create filter
    # if any of the dimensions of the input signal is not mapped, then this will fail.
    try:
        new_input_filter = DimensionFilter.materialize(
            compute_record.materialized_output.domain_spec.dimension_spec,
            dimension_mappers,
            coalesce_relatives=True,  # use Any variable if abstract relative range (RelativeVariant)
            allow_partial=True,  # use Any variable if a dim is not satisfied / materialized by mappers
        )
        filters.append(new_input_filter)
    except (ValueError, TypeError) as err:
        pass

    return filters


def get_nodes_filtered_exec_summary(app, dimensionValueMap, state_type_str=None):
    """Get filtered execution summary of nodes in the application."""
    if not state_type_str or state_type_str == "ALL":
        state_type = None
    else:
        state_type = ComputeSessionStateType(state_type_str)
    routing_table: RoutingTable = app.platform.routing_table

    inactive_records_data = {}
    inactive_compute_records: Iterator[RoutingTable.ComputeRecord] = app.get_inactive_compute_records()

    for inactiveRecord in inactive_compute_records:
        dim_filters = _get_filters(inactiveRecord, dimensionValueMap)
        output_signal = inactiveRecord.materialized_output
        filtered_out = False
        # apply filters if any
        for filter in dim_filters:
            try:
                # TODO pass ignore validation on "relative_min" or "min" on DATETIME
                test_filter = output_signal.domain_spec.dimension_filter_spec.chain(filter)
                if output_signal.domain_spec.dimension_filter_spec and not test_filter:
                    filtered_out = True
                    break
            except ValueError:  # due to issues like "relative_min" inactive record cannot be reinstantiated
                filtered_out = True
                break

        if not filtered_out:
            node_id = inactiveRecord.materialized_output.alias
            output_dimension_values_key = repr(inactiveRecord.materialized_output.dimension_values_map())
            node_inactive_records = inactive_records_data.setdefault(node_id, {})
            previous_record_on_same_output = node_inactive_records.get(output_dimension_values_key, None)
            # favor most recent execution
            if (
                not previous_record_on_same_output
                or previous_record_on_same_output.trigger_timestamp_utc < inactiveRecord.trigger_timestamp_utc
            ):
                node_inactive_records[output_dimension_values_key] = inactiveRecord

    dev_context = app._active_context or app._dev_context
    data = []
    for instruction in dev_context.instruction_chain:
        output_signal: Signal = instruction.output_node.signal()

        type = output_signal.type.name
        if type == "INTERNAL_PARTITION_CREATION":
            route: Route = cast("InternalDataNode", instruction.symbol_tree).create_route()
            node_id = instruction.output_node._id

            inactive_response = [
                map_inactive_compute_record(inactive_record, routing_table)
                for output_key, inactive_record in inactive_records_data.get(node_id, {}).items()
                if not state_type or inactive_record.session_state.state_type == state_type
            ]

            node = {
                "id": node_id,
                "node_type": "INTERNAL_DATA_NODE",
                "execution_data": {
                    "inactiveRecords": inactive_response,
                    "activeRecords": [],
                    "pendingRecords": [],
                },
            }
            data.append(node)

    result = {"nodes": data}
    return result


def compute_node_list(app):
    """Compute list of nodes in the application."""
    dev_context = app._active_context or app._dev_context
    data = []
    for instruction in dev_context.instruction_chain:
        data.append(
            {
                "node_id": instruction.output_node._id,
                "inbound": get_node_inbound(instruction),
                "outbound": get_node_outbound(instruction),
            }
        )
    return data


def get_active_node_state(app, node_id):
    """Get state of active nodes in the application."""
    app_state = app.state
    active_node_state_response = {"activeRecords": [], "pendingRecords": []}
    routing_table: RoutingTable = app.platform.routing_table
    # Check if the current node is an Internal Data node
    data_nodes = app.get_data(node_id)
    if not data_nodes:
        # TODO: Handle it with error handling
        return active_node_state_response
    current_node = data_nodes[0]
    if not isinstance(current_node.bound, InternalDataNode):
        # TODO: Handle it with error handling and modify the error message
        return active_node_state_response

    route = current_node.bound.create_route()

    # Check if the application is in ACTIVE state
    if app_state == ApplicationState.ACTIVE:
        # Check and retrieve all the pending nodes
        route_record: RoutingTable.RouteRecord = app.get_active_route(node_id)
        pending_nodes = []
        if route_record:
            for pending_node in route_record.route.pending_nodes:
                mapped_data = map_pending_node(pending_node)
                materialized_output: Signal = pending_node.materialize_output(
                    route.output, route.output_dim_matrix, force=True, allow_partial=True
                )
                mapped_data["outputSignal"] = {
                    "path": (
                        materialized_output.get_materialized_resource_paths()[0]
                        if materialized_output.get_materialized_resource_paths()
                        else ""
                    ),
                    "dimensionValueMap": materialized_output.dimension_values_map(),
                    "dimensionTypeMap": materialized_output.dimension_type_map(),
                }
                pending_nodes.append(mapped_data)

        active_node_state_response["pendingRecords"] = pending_nodes

        # Active records
        active_compute_records_iterator: Iterator[RoutingTable.ComputeRecord] = app.get_active_compute_records(node_id)
        active_records = []
        for active_record in active_compute_records_iterator:
            mapped_data = map_active_compute_record(active_record, routing_table)
            active_records.append(mapped_data)
        active_node_state_response["activeRecords"] = active_records

    return active_node_state_response


def get_inactive_node_state(app, node_id, limit=200):
    """Get state of inactive nodes in the application."""
    inactive_node_state_response = {"inactiveRecords": []}
    routing_table: RoutingTable = app.platform.routing_table
    # Check if the current node is an Internal Data node
    data_nodes = app.get_data(node_id)
    if not data_nodes:
        # TODO: Handle it with error handling
        return inactive_node_state_response
    current_node = data_nodes[0]
    if not isinstance(current_node.bound, InternalDataNode):
        # TODO: Handle it with error handling and modify the error message
        return inactive_node_state_response
    # Inactive records
    inactive_compute_records_iterator: Iterator[RoutingTable.ComputeRecord] = app.get_inactive_compute_records(
        node_id, ascending=False, limit=limit
    )
    inactive_records = []
    for inactive_record in inactive_compute_records_iterator:
        mapped_data = map_inactive_compute_record(inactive_record, routing_table)
        inactive_records.append(mapped_data)

    inactive_node_state_response["inactiveRecords"] = inactive_records
    return inactive_node_state_response


def get_cloud_watch_logs(app, node_id, dimension_values=None):
    """Get cloud watch logs for a node."""
    cloud_watch_logs_response = []
    # Check if the current node is an Internal Data node
    data_nodes = app.get_data(node_id)
    if not data_nodes:
        # TODO: Handle it with error handling
        return cloud_watch_logs_response
    current_node = data_nodes[0]
    if not isinstance(current_node.bound, InternalDataNode):
        # TODO: Handle it with error handling and modify the error message
        return cloud_watch_logs_response
    if dimension_values:
        for values in dimension_values:
            current_node = current_node[values]
    compute_queries: List[ComputeLogQuery] = app.get_compute_record_logs(current_node)
    compute_query_results = []
    # TODO: Raise internal error when empty or not supported driver
    if compute_queries:
        # TODO: Handle nextToken
        for query in compute_queries:
            if query:
                compute_query_results.append(
                    {
                        "records": query.records,
                        "resource_urls": query.resource_urls,
                        "next_token": query.next_token,
                    }
                )
    return {"results": compute_query_results}


def get_execution_diagnostics(app, node_id, dimension_values=None, filter_pattern=None, limit=1000):
    """Get execution diagnostics for a node."""
    exec_diagnostics_response = {"compute_queries": []}
    # Check if the current node is an Internal Data node
    data_nodes = app.get_data(node_id)
    if not data_nodes:
        # TODO: Handle it with error handling
        return exec_diagnostics_response
    current_node = data_nodes[0]
    if not isinstance(current_node.bound, InternalDataNode):
        # TODO: Handle it with error handling and modify the error message
        return exec_diagnostics_response
    if dimension_values:
        for values in dimension_values:
            current_node = current_node[values]

    _, compute_records = app.poll(current_node, wait=False)
    if not compute_records:
        raise ValueError("Cannot find previous executions!")

    for compute_record in compute_records:
        compute_queries: [ComputeLogQuery] = app.get_compute_record_logs(
            compute_record, error_only=False, filter_pattern=filter_pattern, limit=limit
        )
        compute_query_results = []
        if compute_queries:
            compute_query = compute_queries[0]
            if compute_query:
                mapped_data = map_active_compute_record(compute_record, app.platform.routing_table)
                exec_diagnostics_response["compute_queries"].append(
                    {
                        "compute_type": compute_record.state.resource_desc.driver_type.__name__,
                        "compute_log_query": {"records": compute_query.records, "resource_urls": compute_query.resource_urls},
                        "details": mapped_data,
                        "overallState": mapped_data["overallState"],
                    }
                )

    return exec_diagnostics_response


def create_execution(app, node_id, dimension_values=None, wait=True, recursive=True, update_tree=True):
    """Create execution for a node."""
    error = {}
    try:
        app.attach()
        data_nodes = app.get_data(
            node_id,
            app_scope=Application.QueryApplicationScope.CURRENT_APP_ONLY,  # can manage executions from the same app only (not from upstream apps)
            context=Application.QueryContext.ALL,
        )
        if not data_nodes:
            raise ValueError(f"Cannot find internal node {node_id}!")

        current_node = data_nodes[0]
        if dimension_values:
            for values in dimension_values:
                current_node = current_node[values]

        # TODO enable integrity check when Flask server is deployed to its own server.
        #      till then local thread starves very quickly.
        app.execute(current_node, wait=wait, recursive=recursive, integrity_check=False, update_dependency_tree=update_tree)
    except (ValueError, TypeError) as err:
        error = {
            "message": f"An error occured while trying to start execution(node_id={node_id!r}, dimension_values={dimension_values!r})! Error: {repr(err)}",
            "stack_trace": traceback.format_exc(),
        }

    return {"error": error}


def kill_execution(app, node_id, dimension_values=None):
    """Kill execution for a node."""
    error = {}
    try:
        data_nodes = app.get_data(
            node_id,
            app_scope=Application.QueryApplicationScope.CURRENT_APP_ONLY,  # can manage executions from the same app only (not from upstream apps)
            context=Application.QueryContext.ALL,
        )
        if not data_nodes:
            raise ValueError(f"Cannot find internal node {node_id}!")

        current_node = data_nodes[0]
        if dimension_values:
            for values in dimension_values:
                current_node = current_node[values]

        app.kill(current_node)

    except ValueError as error:
        error = {
            "message": f"An error occured while trying to kill execution (node_id={node_id!r}, dimension_values={dimension_values!r})! Error: {repr(error)}",
            "stack_trace": traceback.format_exc(),
        }

    return {"error": error}


def delete_pending(app, node_id, pending_node_id):
    """Delete pending node."""
    error = {}
    try:
        data_nodes = app.get_data(
            node_id,
            app_scope=Application.QueryApplicationScope.CURRENT_APP_ONLY,  # can manage executions from the same app only (not from upstream apps)
            context=Application.QueryContext.ALL,
        )
        if not data_nodes:
            raise ValueError(f"Cannot find internal node {node_id}!")

        current_node = data_nodes[0]

        app.delete_pending_node(current_node, pending_node_id)
    except ValueError as error:
        error = {
            "message": f"An error occured while trying to delete pending node (node_id={node_id!r}, pending_node_id={pending_node_id!r})! Error: {repr(error)}",
            "stack_trace": traceback.format_exc(),
        }

    return {"error": error}


def load_data(app, node_id, dimension_values=None, limit=10):
    """Load data for a node."""
    error = {}
    data = None
    try:
        data_nodes = app.get_data(
            node_id,
            app_scope=Application.QueryApplicationScope.CURRENT_APP_ONLY,  # can manage executions from the same app only (not from upstream apps)
            context=Application.QueryContext.ALL,
        )
        if not data_nodes:
            raise ValueError(f"Cannot find internal node {node_id}!")

        current_node = data_nodes[0]
        if dimension_values:
            for values in dimension_values:
                current_node = current_node[values]

        if app.has_active_record(current_node):
            raise ValueError(
                f"Node: {node_id!r} has active execution on dimensions {dimension_values!r}! "
                f"Please try later upon the successful completion of that execution."
            )

        data = next(app.load_data(current_node, limit, DataFrameFormat.ORIGINAL, iterate=False))

    except ValueError as error:
        error = {
            "message": f"An error occured while trying to load data for (node_id={node_id!r}, dimension_values={dimension_values!r})! Error: {repr(error)}",
            "stack_trace": traceback.format_exc(),
        }

    return {"data": data, "error": error}


def refresh_app(app):
    """Refresh the application."""
    error = {}
    if app.state != ApplicationState.INACTIVE:
        try:
            app.refresh(full_stack=True)
            app.attach()
        except ValueError as error:
            error = {
                "message": f"An error occured while trying to refresh the application! Error: {repr(error)}",
                "stack_trace": traceback.format_exc(),
            }
    else:
        error = {
            "message": f"Cannot refresh an inactive application!",
        }

    return {"error": error}
