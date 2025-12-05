# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import uuid
from enum import Enum
from typing import ClassVar, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Type, Union, cast

import boto3
import botocore
from boto3.dynamodb.conditions import Attr, Key, Or
from botocore.exceptions import ClientError

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing import DimensionFilter, Signal
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import MetricStatistic
from intelliflow.core.signal_processing.routing_runtime_constructs import Route, RouteID
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import CWMetricSignalSourceAccessSpec
from intelliflow.core.signal_processing.slot import SlotType

from ...constructs import (
    ConstructInternalMetricDesc,
    ConstructParamsDict,
    ConstructPermission,
    ConstructSecurityConf,
    RoutingActivationStrategy,
    RoutingTable,
)
from ...definitions.aws.auto_scaling.client_wrapper import (
    delete_target_tracking_scaling_policy,
    deregister_scalable_target,
    put_target_tracking_scaling_policy,
    register_scalable_target,
)
from ...definitions.aws.common import CommonParams as AWSCommonParams
from ...definitions.aws.common import exponential_retry
from ...definitions.aws.ddb.client_wrapper import (
    BillingMode,
    create_table,
    delete_ddb_item,
    delete_table,
    enable_PITR,
    get_ddb_item,
    put_ddb_item,
    query_ddb_table,
    scan_ddb_table,
)
from ...definitions.aws.ddb.lock import create_lock_table, lock, release
from ...definitions.common import ActivationParams
from ...definitions.compute import ComputeResponseType, ComputeSessionStateType
from ..aws_common import AWSConstructMixin

module_logger = logging.getLogger(__name__)


class AWSDDBRoutingTable(AWSConstructMixin, RoutingTable):
    """RoutingTable impl based on DDB.

    Trade-offs:

        Pros:

        Cons:

    """

    ROUTES_TABLE_NAME_FORMAT: ClassVar[str] = "IntelliFlow-Routes-{0}-{1}-{2}"
    ROUTES_HISTORY_TABLE_NAME_FORMAT: ClassVar[str] = "IntelliFlow-Routes-History-{0}-{1}-{2}"
    ROUTES_PENDING_NODES_TABLE_NAME_FORMAT: ClassVar[str] = "IntelliFlow-Routes-PendingNodes-{0}-{1}-{2}"
    ROUTES_ACTIVE_COMPUTE_RECORDS_TABLE_NAME_FORMAT: ClassVar[str] = "IntelliFlow-Routes-ActiveComputeRecords-{0}-{1}-{2}"
    ROUTES_LOCK_TABLE_NAME_FORMAT: ClassVar[str] = "IntelliFlow-Routes-Lock-{0}-{1}-{2}"

    HISTORY_TABLE_COMPUTE_TRIGGER_INDEX_NAME = "compute_trigger_index"
    HISTORY_TABLE_COMPUTE_FINISHED_INDEX_NAME = "compute_finished_index"
    HISTORY_TABLE_SLOT_TYPE_INDEX_NAME = "slot_type_index"
    HISTORY_TABLE_SESSION_STATE_INDEX_NAME = "session_state_index"

    BILLING_MODE_PARAM: ClassVar[str] = "BillingMode"
    DEFAULT_BILLING_MODE: BillingMode = BillingMode.PAY_PER_REQUEST

    def __init__(self, params: ConstructParamsDict) -> None:
        """Called the first time this construct is added/configured within a platform.

        Subsequent sessions maintain the state of a construct, so the following init
        operations occur in the very beginning of a construct's life-cycle within an app.
        """
        super().__init__(params)
        self._ddb = self._session.resource("dynamodb", region_name=self._region, config=self._ddb_client_dev_config())
        self._ddb_scaling = self._session.client("application-autoscaling", region_name=self._region)
        self._routing_table_name = None
        self._routing_table = None
        self._routing_history_table_name = None
        self._routing_history_table = None
        self._routing_pending_nodes_table_name = None
        self._routing_pending_nodes_table = None
        self._routing_active_compute_records_table_name = None
        self._routing_active_compute_records_table = None
        self._routing_lock_table_name = None
        self._routing_lock_table = None
        self._routing_lock_retainer_id = self._create_retainer_id_for_session()

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._ddb = self._session.resource("dynamodb", region_name=self._region, config=self._ddb_client_dev_config())
        self._ddb_scaling = self._session.client("application-autoscaling", region_name=self._region)
        self._routing_lock_retainer_id = self._create_retainer_id_for_session()

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._ddb = None
        self._routing_table = None
        self._routing_history_table = None
        self._routing_pending_nodes_table = None
        self._routing_active_compute_records_table = None
        self._routing_lock_table = None
        self._ddb_scaling = None
        self._routing_lock_retainer_id = None

    @classmethod
    def _ddb_client_dev_config(cls):
        # TODO tune, use only in dev-mode for parallel executions (not required at runtime)
        return botocore.client.Config(max_pool_connections=20)

    @property
    def routing_table_name(self):
        return self._routing_table_name

    @property
    def routing_history_table_name(self):
        return self._routing_history_table_name

    @property
    def routing_pending_nodes_table_name(self):
        return self._routing_pending_nodes_table_name

    @property
    def routing_active_compute_records_table_name(self):
        return self._routing_active_compute_records_table_name

    @property
    def routing_lock_table_name(self):
        return self._routing_lock_table_name

    @property
    def is_synchronized(self) -> bool:
        # because `_lock` and `_release` are implemented
        return True

    @property
    def billing_mode(self) -> BillingMode:
        billing_mode = self._params.get(self.BILLING_MODE_PARAM, self.DEFAULT_BILLING_MODE)
        return billing_mode if billing_mode else self.DEFAULT_BILLING_MODE

    @classmethod
    def _create_retainer_id_for_session(self) -> str:
        return str(uuid.uuid4())

    def _lock(
        self,
        route_id: RouteID,
        lock_duration_in_secs: int = RoutingTable.DEFAULT_LOCK_DURATION_IN_SECS,
        retain_attempt_duration_in_secs: int = RoutingTable.DEFAULT_LOCK_RETAIN_ATTEMPT_DURATION_IN_SECS,
    ) -> bool:
        return lock(
            self._routing_lock_table,
            route_id,
            self._routing_lock_retainer_id,
            lock_duration_in_secs=lock_duration_in_secs,
            retain_attempt_duration_in_secs=retain_attempt_duration_in_secs,
        )

    def _release(self, route_id: RouteID) -> None:
        return release(self._routing_lock_table, route_id, self._routing_lock_retainer_id)

    def _load(self, route_id: RouteID) -> Optional[RoutingTable.RouteRecord]:
        """Load the internal record for a route."""
        response = exponential_retry(
            get_ddb_item,
            ["ProvisionedThroughputExceededException", "RequestLimitExceeded", "InternalServerError"],
            self._routing_table,
            key={"route_id": route_id},
        )
        if response and "Item" in response:
            route: Route = loads(response["Item"]["serialized_route"])
            # support both 'pending node' persistence: 1- embedded (in routing table) 2- in pending node tables
            #  if pending_nodes attribute is None, this driver has used the other table for pending nodes persistence.
            #  TODO added as backwards compatibility, will remove this check in a future release.
            if route.pending_nodes is None:
                # aggregate pending nodes, it means that previously pending nodes have been moved into a separate table.
                pending_nodes: Set["RuntimeLinkNode"] = set()
                persisted_pending_node_ids: Set[str] = set()
                pending_nodes_it = self.load_pending_nodes(route_id)
                for pending_node in pending_nodes_it:
                    pending_nodes.add(pending_node)
                    persisted_pending_node_ids.add(pending_node.node_id)
                route._pending_nodes = pending_nodes
            else:
                persisted_pending_node_ids = {pending_node.node_id for pending_node in route.pending_nodes}

            # similarly do a backwards compatible active compute records load
            # TODO remove this load and the following 'if' statement
            active_compute_records: Set["RoutingTable.ComputeRecord"] = loads(response["Item"]["serialized_active_compute_records"])
            persisted_active_compute_record_ids: Set[str] = set()
            if active_compute_records is None:
                # aggregate active compute records, it means that previously active compute records have been moved into a separate table.
                active_compute_records = set()
                active_compute_records_it = self.load_active_compute_records(route_id)
                for active_compute_record in active_compute_records_it:
                    active_compute_records.add(active_compute_record)
                    persisted_active_compute_record_ids.add(active_compute_record.record_id)

            try:
                return RoutingTable.RouteRecord(
                    route,
                    active_compute_records,
                    loads(response["Item"]["serialized_active_execution_context_state"]),
                    persisted_pending_node_ids,
                    persisted_active_compute_record_ids,
                )
            except AttributeError as serialization_error:
                logging.critical(f"Incompatible route record detected for route: {route_id!r}. Ignoring...")
                self.metric("routing_table.receive")["RouteLoadError"].emit(1)
                self.metric("routing_table.receive", route=route)["RouteLoadError"].emit(1)

    def _save(self, route_record: RoutingTable.RouteRecord, suppress_errors_and_emit: Optional[bool] = False) -> None:
        save_retryables = [
            "ProvisionedThroughputExceededException",
            "TransactionConflictException",
            "RequestLimitExceeded",
            "InternalServerError",
        ]
        try:
            # separate out pending_nodes from route before serialization
            pending_nodes = route_record.route.pending_nodes
            route_record.route._pending_nodes = None

            exponential_retry(
                put_ddb_item,
                save_retryables,
                table=self._routing_table,
                item={
                    "route_id": route_record.route.route_id,
                    "serialized_route": dumps(route_record.route, compress=True),
                    # TODO remove, kept as backwards compatibility
                    "serialized_active_compute_records": dumps(None),
                    "serialized_active_execution_context_state": dumps(route_record.active_execution_context_state, compress=True),
                },
            )
            # restore
            route_record.route._pending_nodes = pending_nodes

            # 1. now save pending_nodes
            # 1.1- check the nodes to be deleted
            pending_nodes_to_be_deleted = route_record.persisted_pending_node_ids - {pending_node.node_id for pending_node in pending_nodes}
            # why use non-batch delete?
            #  - different than save below, here we favor reliability (and retries on individual records) over
            #  optimization on deletion large number of pending nodes (which is not expected to happen.
            for pending_node_id in pending_nodes_to_be_deleted:
                self._delete_pending_node(route_record.route.route_id, pending_node_id)

            # 1.2- save/update the new or remaining ones
            exponential_retry(self._save_pending_nodes, save_retryables, route_record.route.route_id, pending_nodes)

            # 2. save active compute records
            # 2.1- check the records to be deleted
            active_records_to_be_deleted = route_record.persisted_active_compute_record_ids - {
                r.record_id for r in route_record.active_compute_records
            }
            for active_compute_record_id in active_records_to_be_deleted:
                self._delete_active_compute_record(route_record.route.route_id, active_compute_record_id)

            # 2.2- save/update the new or remaining ones
            exponential_retry(
                self._save_active_compute_records, save_retryables, route_record.route.route_id, route_record.active_compute_records
            )
        except ClientError as error:
            module_logger.critical(
                f"{self.__class__.__name__}: and error occurred while trying to save"
                f" route: {route_record.route.route_id!r}! Error: {error!r}"
            )
            if not suppress_errors_and_emit:
                raise
            else:
                # TODO try to move to a secondary storage but no matter what 'do emit' Routing internal metric
                self.metric("routing_table.receive")["RouteSaveError"].emit(1)
                self.metric("routing_table.receive", route=route_record.route)["RouteSaveError"].emit(1)

    def _delete(self, routes: Set[Route]) -> None:
        delete_retryables = [
            "ProvisionedThroughputExceededException",
            "TransactionConflictException",
            "RequestLimitExceeded",
            "InternalServerError",
        ]
        try:
            for route in routes:
                # 1- Wipe its aggregations (Pending Nodes, Active Compute Records, etc )
                #  1.1-reload the pending nodes,
                #   why reload? at this point, we don't know if 'Route' object is provided during dev-time
                #   or called at runtime after being loaded from DDB. In the former case, route won't contain them.
                #   Please note that primary key for Pending Node table is [route_id, pending_node_id]
                #   we have to provide both. First read them all and them call batch delete on them.
                pending_nodes_it = self.load_pending_nodes(route.route_id)
                all_pending_node_ids = set()
                for pending_node in pending_nodes_it:
                    all_pending_node_ids.add(pending_node.node_id)

                if all_pending_node_ids:
                    exponential_retry(self._delete_pending_nodes, delete_retryables, route.route_id, all_pending_node_ids)

                #  1.2-reload the active compute records,
                active_compute_records_it = self.load_active_compute_records(route.route_id)
                all_active_compute_record_ids = set()
                for active_compute_record in active_compute_records_it:
                    all_active_compute_record_ids.add(active_compute_record.record_id)

                if all_active_compute_record_ids:
                    exponential_retry(self._delete_active_compute_records, delete_retryables, route.route_id, all_active_compute_record_ids)

                # 2- Finally delete the route from Routing Table
                exponential_retry(delete_ddb_item, delete_retryables, table=self._routing_table, key={"route_id": route.route_id})
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceNotFoundException":
                module_logger.error(f"An error occurred while trying to delete the routes: {[r.route_id for r in routes]}")
                raise

    def load_active_route_records(self) -> Iterator["RoutingTable.RouteRecord"]:
        scan_kwargs = {}

        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = exponential_retry(
                scan_ddb_table,
                ["InternalServerError", "RequestLimitExceeded", "ProvisionedThroughputExceededException"],
                self._routing_table,
                **scan_kwargs,
            )
            items = response.get("Items", [])
            for item in items:
                route: Route = loads(item["serialized_route"])
                yield self._load(route.route_id)

            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None

    def load_active_route_record(self, route_id: RouteID) -> "RoutingTable.RouteRecord":
        return self._load(route_id)

    def load_inactive_compute_records(
        self,
        route_id: Optional[RouteID] = None,
        ascending: bool = False,
        trigger_range: Tuple[int, int] = None,
        deactivated_range: Tuple[int, int] = None,
        slot_type: SlotType = None,
        session_state: ComputeSessionStateType = None,
        limit: Optional[int] = None,
    ) -> Iterator["RoutingTable.ComputeRecord"]:
        if route_id:
            return self.query_inactive_compute_records(
                route_id, ascending, trigger_range, deactivated_range, slot_type, session_state, limit
            )
        else:
            return self.scan_inactive_compute_records(ascending, trigger_range, deactivated_range, slot_type, session_state, limit)

    def query_inactive_compute_records(
        self,
        route_id: RouteID,
        ascending: bool = False,
        trigger_range: Tuple[int, int] = None,
        deactivated_range: Tuple[int, int] = None,
        slot_type: SlotType = None,
        session_state: ComputeSessionStateType = None,
        limit: Optional[int] = None,
    ) -> Iterator["RoutingTable.ComputeRecord"]:
        query_kwargs = {}
        done = False
        start_key = None
        filter_exp = None
        key_cond_expr = Key("route_id").eq(route_id)
        index_name = None

        # Check existing secondary indexes of the table, only use if this secondary index is created
        secondary_indexes_raw = self._routing_history_table.local_secondary_indexes or []
        secondary_indexes = [secondary_index_raw["IndexName"] for secondary_index_raw in secondary_indexes_raw]

        # Looking for range key and index name
        if trigger_range and self.HISTORY_TABLE_COMPUTE_TRIGGER_INDEX_NAME in secondary_indexes:
            key_cond_expr = key_cond_expr & Key("compute_trigger_timestamp_utc").between(*trigger_range)
            index_name = self.HISTORY_TABLE_COMPUTE_TRIGGER_INDEX_NAME
        elif deactivated_range and self.HISTORY_TABLE_COMPUTE_FINISHED_INDEX_NAME in secondary_indexes:
            key_cond_expr = key_cond_expr & Key("compute_finished_timestamp_utc").between(*deactivated_range)
            index_name = self.HISTORY_TABLE_COMPUTE_FINISHED_INDEX_NAME
        elif slot_type and self.HISTORY_TABLE_SLOT_TYPE_INDEX_NAME in secondary_indexes:
            key_cond_expr = key_cond_expr & Key("slot_type").eq(slot_type.value)
            index_name = self.HISTORY_TABLE_SLOT_TYPE_INDEX_NAME
        elif session_state and self.HISTORY_TABLE_SESSION_STATE_INDEX_NAME in secondary_indexes:
            key_cond_expr = key_cond_expr & Key("compute_session_state").eq(session_state.value)
            index_name = self.HISTORY_TABLE_SESSION_STATE_INDEX_NAME

        if index_name:
            query_kwargs["IndexName"] = index_name

        # Compute the filter expression
        if trigger_range and index_name != self.HISTORY_TABLE_COMPUTE_TRIGGER_INDEX_NAME:
            trigger_range_exp = Attr("compute_trigger_timestamp_utc").between(*trigger_range)
            filter_exp = trigger_range_exp
        if deactivated_range and index_name != self.HISTORY_TABLE_COMPUTE_FINISHED_INDEX_NAME:
            deactivated_range_exp = Attr("compute_finished_timestamp_utc").between(*deactivated_range)
            filter_exp = filter_exp & deactivated_range_exp if filter_exp else deactivated_range_exp
        if slot_type and index_name != self.HISTORY_TABLE_SLOT_TYPE_INDEX_NAME:
            slot_type_exp = Attr("slot_type").eq(slot_type.value)
            filter_exp = filter_exp & slot_type_exp if filter_exp else slot_type_exp
        if session_state and index_name != self.HISTORY_TABLE_SESSION_STATE_INDEX_NAME:
            session_state_exp = Attr("compute_session_state").eq(session_state.value)
            filter_exp = filter_exp & session_state_exp if filter_exp else session_state_exp

        retrieved_count: int = 0
        while not done:
            if start_key:
                query_kwargs["ExclusiveStartKey"] = start_key
            if filter_exp:
                query_kwargs["FilterExpression"] = filter_exp
            response = exponential_retry(
                query_ddb_table,
                ["InternalServerError", "RequestLimitExceeded", "ProvisionedThroughputExceededException"],
                table=self._routing_history_table,
                key_cond_expr=key_cond_expr,
                scan_index_forward=ascending,
                **query_kwargs,
            )
            items = response.get("Items", [])
            for item in items:
                try:
                    record = loads(item["serialized_inactive_compute_record"])
                    yield record
                    retrieved_count = retrieved_count + 1
                    if limit is not None and retrieved_count >= limit:
                        break
                except AttributeError as serialization_error:
                    # TODO METRICS_SUPPORT (backwards incompatible framework change)
                    logging.critical(f"Incompatible inactive record detected for route: {route_id!r}. Ignoring...")

            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None

    def scan_inactive_compute_records(
        self,
        ascending: bool = False,
        trigger_range: Tuple[int, int] = None,
        deactivated_range: Tuple[int, int] = None,
        slot_type: SlotType = None,
        session_state: ComputeSessionStateType = None,
        limit: Optional[int] = None,
    ) -> Iterator["RoutingTable.ComputeRecord"]:
        scan_kwargs = {}
        done = False
        start_key = None
        filter_exp = None

        # Compute the filter expression
        if trigger_range:
            trigger_range_exp = Attr("compute_trigger_timestamp_utc").between(*trigger_range)
            filter_exp = trigger_range_exp
        if deactivated_range:
            deactivated_range_exp = Attr("compute_finished_timestamp_utc").between(*deactivated_range)
            filter_exp = filter_exp & deactivated_range_exp if filter_exp else deactivated_range_exp
        if slot_type:
            slot_type_exp = Attr("slot_type").eq(slot_type.value)
            filter_exp = filter_exp & slot_type_exp if filter_exp else slot_type_exp
        if session_state:
            session_state_exp = Attr("compute_session_state").eq(session_state.value)
            filter_exp = filter_exp & session_state_exp if filter_exp else session_state_exp

        retrieved_count: int = 0
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            if filter_exp:
                scan_kwargs["FilterExpression"] = filter_exp
            response = exponential_retry(
                scan_ddb_table,
                ["InternalServerError", "RequestLimitExceeded", "ProvisionedThroughputExceededException"],
                table=self._routing_history_table,
                **scan_kwargs,
            )
            items = response.get("Items", [])
            for item in items:
                try:
                    record = loads(item["serialized_inactive_compute_record"])
                    yield record
                    retrieved_count = retrieved_count + 1
                    if limit is not None and retrieved_count >= limit:
                        break
                except (AttributeError, ModuleNotFoundError) as serialization_error:
                    # TODO METRICS_SUPPORT (backwards incompatible framework change)
                    logging.critical(f"Incompatible inactive record detected during scan. Ignoring...")

            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None

    def _save_inactive_compute_records(
        self, route: Route, inactive_records: Sequence["RoutingTable.ComputeRecord"], suppress_errors_and_emit: Optional[bool] = False
    ) -> None:
        """save historical records to the secondary storage (where we will keep the historical records)
        Relies on
             https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.batch_writer

             which takes care of buffering/bucketing and also retries for unprocessed items.
        """
        try:
            # drop PNs due to DDB record size limit. if we don't do this, then in large-scale operations ICRs might drop
            pending_nodes = route.pending_nodes
            route._pending_nodes = None
            # FUTURE enable if we need this in the future (to refer the old snapshot of Route for any use-case)
            # serialized_route = dumps(route, compress=True)
            route._pending_nodes = pending_nodes

            # with self._routing_history_table.batch_writer() as batch:
            with self._routing_history_table.batch_writer(overwrite_by_pkeys=["route_id", "compute_record_sort_key"]) as batch:
                for inactive_record in inactive_records:
                    batch.put_item(
                        Item={
                            "route_id": route.route_id,
                            # prefix with timestamp so that implicit sorting by DDB would make sense for our queries.
                            "compute_record_sort_key": f"{inactive_record.trigger_timestamp_utc}{uuid.uuid4()}",
                            "compute_trigger_timestamp_utc": inactive_record.trigger_timestamp_utc,
                            "compute_finished_timestamp_utc": inactive_record.deactivated_timestamp_utc,
                            "compute_elapsed_time": (
                                inactive_record.deactivated_timestamp_utc - inactive_record.trigger_timestamp_utc
                                if inactive_record.deactivated_timestamp_utc
                                else None
                            ),
                            "compute_response_state": inactive_record.state.response_type.value,
                            "compute_response_sub_state": (
                                inactive_record.state.successful_response_type.value
                                if inactive_record.state.response_type == ComputeResponseType.SUCCESS
                                else inactive_record.state.failed_response_type.value
                            ),
                            "compute_session_state": (
                                inactive_record.session_state.state_type.value
                                if inactive_record.session_state
                                else ComputeSessionStateType.FAILED.value
                            ),
                            "compute_session_sub_state": (
                                inactive_record.session_state.failed_type.value
                                if inactive_record.session_state
                                and inactive_record.session_state.state_type == ComputeSessionStateType.FAILED
                                else ""
                            ),  # cannot leave empty if will be used as a secondary index, in that case set as ComputeFailedResponseType.UNKNOWN
                            "slot_type": inactive_record.slot.type.value,
                            # TODO create a GSI to get compute records that belong to the same context together
                            "execution_context_id": inactive_record.execution_context_id,
                            # "serialized_route": serialized_route,
                            "serialized_inactive_compute_record": dumps(inactive_record, compress=True),
                        }
                    )
        except ClientError as error:
            module_logger.critical(
                f"{self.__class__.__name__}: and error occurred while trying to save inactive compute"
                f" records {inactive_records!r} for route {route!r}. Error: {error}"
            )
            if not suppress_errors_and_emit:
                raise
            else:
                # TODO try to move to a secondary storage but no matter what 'do emit' Routing internal metric
                pass

    def load_pending_nodes(
        self, route_id: Optional[RouteID] = None, ascending: bool = False
    ) -> Union[Iterator[Tuple[RouteID, "RuntimeLinkNode"]], Iterator["RuntimeLinkNode"]]:
        if route_id:
            return self.query_pending_nodes(route_id, ascending)
        else:
            return self.scan_all_pending_nodes(ascending)

    def scan_all_pending_nodes(self, ascending: bool = False) -> Iterator[Tuple[RouteID, "RuntimeLinkNode"]]:
        """DDB scan based internal impl that serves user exposed load function `load_pending_nodes`"""
        scan_kwargs = {}

        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = exponential_retry(
                scan_ddb_table,
                ["InternalServerError", "RequestLimitExceeded", "ProvisionedThroughputExceededException"],
                self._routing_pending_nodes_table,
                **scan_kwargs,
            )
            items = response.get("Items", [])
            for item in items:
                try:
                    record = loads(item["serialized_pending_node"])
                    yield item["route_id"], record
                except AttributeError as serialization_error:
                    # TODO METRICS_SUPPORT (backwards incompatible framework change)
                    logging.critical(f"Incompatible pending node detected! Ignoring...")

            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None

    def query_pending_nodes(self, route_id: RouteID, ascending: bool = False) -> Iterator["RuntimeLinkNode"]:
        """DDB query based internal impl that serves user exposed load function"""
        query_kwargs = {}
        done = False
        start_key = None
        while not done:
            if start_key:
                query_kwargs["ExclusiveStartKey"] = start_key
            response = exponential_retry(
                query_ddb_table,
                ["InternalServerError", "RequestLimitExceeded"],
                table=self._routing_pending_nodes_table,
                key_cond_expr=Key("route_id").eq(route_id),
                scan_index_forward=ascending,
                **query_kwargs,
            )
            items = response.get("Items", [])
            for item in items:
                try:
                    record = loads(item["serialized_pending_node"])
                    yield record
                except AttributeError as serialization_error:
                    # TODO METRICS_SUPPORT (backwards incompatible framework change)
                    logging.critical(f"Incompatible pending node detected for route: {route_id!r}. Ignoring...")

            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None

    def _save_pending_nodes(self, route_id: RouteID, pending_nodes: Set["RuntimeLinkNode"]) -> None:
        try:
            with self._routing_pending_nodes_table.batch_writer(overwrite_by_pkeys=["route_id", "pending_node_id"]) as batch:
                for pending_node in pending_nodes:
                    batch.put_item(
                        Item={
                            "route_id": route_id,
                            "pending_node_id": pending_node.node_id,
                            "serialized_pending_node": dumps(pending_node, compress=True),
                        }
                    )
        except ClientError as error:
            module_logger.error(
                f"{self.__class__.__name__}: and error occurred while trying to save pending node"
                f" records {pending_nodes!r} for route {route_id!r}. Error: {error}"
            )
            raise

    def _delete_pending_node(self, route_id: RouteID, pending_node_id: str) -> None:
        try:
            exponential_retry(
                delete_ddb_item,
                ["ProvisionedThroughputExceededException", "TransactionConflictException", "RequestLimitExceeded", "InternalServerError"],
                table=self._routing_pending_nodes_table,
                key={"route_id": route_id, "pending_node_id": pending_node_id},
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceNotFoundException":
                module_logger.error(
                    f"An error occurred while trying to delete the pending_node: {pending_node_id!r} from" f" route: {route_id!r}"
                )
                raise

    def _delete_pending_nodes(self, route_id: RouteID, pending_node_ids: Set[str]) -> None:
        try:
            with self._routing_pending_nodes_table.batch_writer(overwrite_by_pkeys=["route_id", "pending_node_id"]) as batch:
                for pending_node_id in pending_node_ids:
                    batch.delete_item(Key={"route_id": route_id, "pending_node_id": pending_node_id})
        except ClientError as error:
            module_logger.error(
                f"{self.__class__.__name__}: an error occurred while trying to delete pending node"
                f" records {pending_node_ids!r} for route {route_id!r}. Error: {error}"
            )
            raise

    def load_active_compute_records(
        self, route_id: Optional[RouteID] = None, ascending: bool = False
    ) -> Iterator["RoutingTable.ComputeRecord"]:
        if route_id:
            return self.query_active_compute_records(route_id, ascending)
        else:
            return self.scan_all_active_compute_records(ascending)

    def scan_all_active_compute_records(self, ascending: bool = False) -> Iterator["RoutingTable.ComputeRecord"]:
        """DDB scan based internal impl that serves user exposed load function"""
        scan_kwargs = {}

        done = False
        start_key = None
        while not done:
            if start_key:
                scan_kwargs["ExclusiveStartKey"] = start_key
            response = exponential_retry(
                scan_ddb_table,
                ["InternalServerError", "RequestLimitExceeded", "ProvisionedThroughputExceededException"],
                self._routing_active_compute_records_table,
                **scan_kwargs,
            )
            items = response.get("Items", [])
            for item in items:
                try:
                    record = loads(item["serialized_active_compute_record"])
                    yield record
                except AttributeError as serialization_error:
                    # TODO METRICS_SUPPORT (backwards incompatible framework change)
                    logging.critical(f"Incompatible active compute record detected! Ignoring...")

            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None

    def query_active_compute_records(self, route_id: RouteID, ascending: bool = False) -> Iterator["RoutingTable.ComputeRecord"]:
        """DDB query based internal impl that serves user exposed load function"""
        query_kwargs = {}
        done = False
        start_key = None
        while not done:
            if start_key:
                query_kwargs["ExclusiveStartKey"] = start_key
            response = exponential_retry(
                query_ddb_table,
                ["InternalServerError", "RequestLimitExceeded"],
                table=self._routing_active_compute_records_table,
                key_cond_expr=Key("route_id").eq(route_id),
                scan_index_forward=ascending,
                **query_kwargs,
            )
            items = response.get("Items", [])
            for item in items:
                try:
                    record = loads(item["serialized_active_compute_record"])
                    yield record
                except AttributeError as serialization_error:
                    # TODO METRICS_SUPPORT (backwards incompatible framework change)
                    logging.critical(f"Incompatible active compute record detected for route: {route_id!r}. Ignoring...")

            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None

    def _save_active_compute_records(self, route_id: RouteID, active_compute_records: Set["RoutingTable.ComputeRecord"]) -> None:
        try:
            with self._routing_active_compute_records_table.batch_writer(
                overwrite_by_pkeys=["route_id", "active_compute_record_id"]
            ) as batch:
                for active_record in active_compute_records:
                    batch.put_item(
                        Item={
                            "route_id": route_id,
                            "active_compute_record_id": active_record.record_id,
                            "serialized_active_compute_record": dumps(active_record, compress=True),
                        }
                    )
        except ClientError as error:
            module_logger.error(
                f"{self.__class__.__name__}: and error occurred while trying to save active compute"
                f" records {active_compute_records!r} for route {route_id!r}. Error: {error}"
            )
            raise

    def _delete_active_compute_record(self, route_id: RouteID, active_record_id: str) -> None:
        try:
            exponential_retry(
                delete_ddb_item,
                ["ProvisionedThroughputExceededException", "TransactionConflictException", "RequestLimitExceeded", "InternalServerError"],
                table=self._routing_active_compute_records_table,
                key={"route_id": route_id, "active_compute_record_id": active_record_id},
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceNotFoundException":
                module_logger.error(
                    f"An error occurred while trying to delete the active compute record: {active_record_id!r} from" f" route: {route_id!r}"
                )
                raise

    def _delete_active_compute_records(self, route_id: RouteID, active_compute_record_ids: Set[str]) -> None:
        try:
            with self._routing_active_compute_records_table.batch_writer(
                overwrite_by_pkeys=["route_id", "active_compute_record_id"]
            ) as batch:
                for active_compute_record_id in active_compute_record_ids:
                    batch.delete_item(Key={"route_id": route_id, "active_compute_record_id": active_compute_record_id})
        except ClientError as error:
            module_logger.error(
                f"{self.__class__.__name__}: an error occurred while trying to delete pending node"
                f" records {active_compute_record_ids!r} for route {route_id!r}. Error: {error}"
            )
            raise

    def _deregister_scalable_target(self, table) -> None:
        # we have to read the billing mode from the table (rather than using self.billing_mode), otherwise we'd not be
        # able to switch between different modes and end up making redundant service calls here while auto-scaling is
        # not active (e.g billing_mode == PAY_PER_REQUEST)
        if table.billing_mode_summary:
            # if this field exists, then billing mode is PAY_PER_REQUEST avoid making redundant service calls
            return

        table_name = table.table_name
        # READ
        try:
            exponential_retry(
                delete_target_tracking_scaling_policy,
                ["ConcurrentUpdateException", "InternalServiceException"],
                auto_scaling_client=self._ddb_scaling,
                service_name="dynamodb",
                resource_id="table/{}".format(table_name),
                scalable_dimension="dynamodb:table:ReadCapacityUnits",
                policy_name="DDBReadScaling_{}".format(table_name),
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ObjectNotFoundException"]:
                raise

        try:
            exponential_retry(
                deregister_scalable_target,
                ["ConcurrentUpdateException", "InternalServiceException"],
                auto_scaling_client=self._ddb_scaling,
                service_name="dynamodb",
                resource_id="table/{}".format(table_name),
                scalable_dimension="dynamodb:table:ReadCapacityUnits",
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ObjectNotFoundException"]:
                raise
        # end READ

        # WRITE
        try:
            exponential_retry(
                delete_target_tracking_scaling_policy,
                ["ConcurrentUpdateException", "InternalServiceException"],
                auto_scaling_client=self._ddb_scaling,
                service_name="dynamodb",
                resource_id="table/{}".format(table_name),
                scalable_dimension="dynamodb:table:WriteCapacityUnits",
                policy_name="DDBWriteScaling_{}".format(table_name),
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ObjectNotFoundException"]:
                raise

        try:
            exponential_retry(
                deregister_scalable_target,
                ["ConcurrentUpdateException", "InternalServiceException"],
                auto_scaling_client=self._ddb_scaling,
                service_name="dynamodb",
                resource_id="table/{}".format(table_name),
                scalable_dimension="dynamodb:table:WriteCapacityUnits",
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ObjectNotFoundException"]:
                raise
        # end WRITE

    def _delete_table(self, table_name: str) -> None:
        table = self._ddb.Table(table_name)
        self._deregister_scalable_target(table)
        try:
            exponential_retry(
                delete_table,
                [
                    "LimitExceededException",
                    "InternalServerError",
                    # this one was capture during resilience integ-test.
                    # repetitive activate->terminate cycle, or a very quick activate->terminate transition might
                    # cause the following:
                    # "Attempt to change a resource which is still in use: Table: <TABLE> is in the process of being updated."
                    "ResourceInUseException",
                ],
                table=table,
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                raise

    # overrides
    def _clear_all(self) -> None:
        module_logger.info(f"{self.__class__.__name__}: Clearing all of the routing tables...")
        self._delete_table(self._routing_table_name)
        self._delete_table(self._routing_history_table_name)
        self._delete_table(self._routing_pending_nodes_table_name)
        self._delete_table(self._routing_active_compute_records_table_name)
        self._delete_table(self._routing_lock_table_name)
        self._create_route_table()
        self._create_route_history_table()
        self._create_route_pending_nodes_table()
        self._create_route_active_compute_records_table()
        self._create_route_lock_table()
        module_logger.info(
            f"Tables "
            f"{self._routing_table_name!r}, "
            f"{self._routing_history_table_name!r}, "
            f"{self._routing_pending_nodes_table_name!r}, "
            f"{self._routing_active_compute_records_table_name!r} "
            f"{self._routing_lock_table_name!r} "
            f"have been cleared!"
        )

    # overrides
    def _clear_active_routes(self) -> None:
        module_logger.info(f"{self.__class__.__name__}: Clearing active routes table {self._routing_table_name}...")
        self._delete_table(self._routing_table_name)
        self._delete_table(self._routing_pending_nodes_table_name)
        self._delete_table(self._routing_active_compute_records_table_name)
        self._delete_table(self._routing_lock_table_name)
        self._create_route_table()
        self._create_route_pending_nodes_table()
        self._create_route_active_compute_records_table()
        self._create_route_lock_table()
        module_logger.info(
            f"Tables "
            f"{self._routing_table_name!r},"
            f"{self._routing_pending_nodes_table_name!r},"
            f"{self._routing_active_compute_records_table_name!r}"
            f"{self._routing_lock_table_name!r}"
            f" have been cleared!"
        )

    # overrides
    def clear_history(self) -> None:
        module_logger.info(f"{self.__class__.__name__}: Clearing historical/inactive compute records...")
        self._delete_table(self._routing_history_table_name)
        self._create_route_history_table()
        module_logger.info(f"Table " f"{self._routing_history_table_name!r}, " f"has been cleared!")

    def terminate(self) -> None:
        """Designed to be resilient against repetitive calls in case of retries in the high-level
        termination work-flow.
        """
        module_logger.info(f"{self.__class__.__name__}: Deleting routing tables...")
        if self._routing_table_name:
            self._delete_table(self._routing_table_name)
            self._routing_table_name = None
        if self._routing_history_table_name:
            self._delete_table(self._routing_history_table_name)
            self._routing_history_table_name = None
        if self._routing_pending_nodes_table_name:
            self._delete_table(self._routing_pending_nodes_table_name)
            self._routing_pending_nodes_table_name = None
        if self._routing_active_compute_records_table_name:
            self._delete_table(self._routing_active_compute_records_table_name)
            self._routing_active_compute_records_table_name = None
        if self._routing_lock_table_name:
            self._delete_table(self._routing_lock_table_name)
            self._routing_lock_table_name = None
        module_logger.info("Routing tables are deleted!")
        super().terminate()

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)
        # 1- active routes table (actual route object storage)
        self._routing_table_name: str = self.ROUTES_TABLE_NAME_FORMAT.format(
            self.__class__.__name__, self._dev_platform.context_id.lower(), self._region
        )
        self._validate_table_name_on_dev_init(self._routing_table_name)
        self._routing_table = self._ddb.Table(self._routing_table_name)

        # 2- inactive records table
        self._routing_history_table_name: str = self.ROUTES_HISTORY_TABLE_NAME_FORMAT.format(
            self.__class__.__name__, self._dev_platform.context_id.lower(), self._region
        )
        self._validate_table_name_on_dev_init(self._routing_history_table_name)
        self._routing_history_table = self._ddb.Table(self._routing_history_table_name)

        # 3- pending nodes table
        self._routing_pending_nodes_table_name: str = self.ROUTES_PENDING_NODES_TABLE_NAME_FORMAT.format(
            self.__class__.__name__, self._dev_platform.context_id.lower(), self._region
        )
        self._validate_table_name_on_dev_init(self._routing_pending_nodes_table_name)
        self._routing_pending_nodes_table = self._ddb.Table(self._routing_pending_nodes_table_name)

        # 4- active compute records table
        self._routing_active_compute_records_table_name: str = self.ROUTES_ACTIVE_COMPUTE_RECORDS_TABLE_NAME_FORMAT.format(
            self.__class__.__name__, self._dev_platform.context_id.lower(), self._region
        )
        self._validate_table_name_on_dev_init(self._routing_active_compute_records_table_name)
        self._routing_active_compute_records_table = self._ddb.Table(self._routing_active_compute_records_table_name)

        # 5- lock table
        self._routing_lock_table_name: str = self.ROUTES_LOCK_TABLE_NAME_FORMAT.format(
            self.__class__.__name__, self._dev_platform.context_id.lower(), self._region
        )
        self._validate_table_name_on_dev_init(self._routing_lock_table_name)
        self._routing_lock_table = self._ddb.Table(self._routing_lock_table_name)

    def _validate_table_name_on_dev_init(self, table_name: str):
        if len(table_name) > 255:
            raise ValueError(
                f"Cannot dev_init {self.__class__.__name__} due to very long"
                f" AWS DynamoDB Name {table_name} (limit <= 255),"
                f" as a result of very long context_id '{self._dev_platform.context_id}'."
            )

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        """Whole platform got bootstrapped at runtime. For other runtime services, this
        construct should be initialized (ex: context_owner: Lambda, Glue, etc)"""
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._ddb = boto3.resource("dynamodb", region_name=self._region)
        self._routing_table = self._ddb.Table(self._routing_table_name)
        self._routing_history_table = self._ddb.Table(self._routing_history_table_name)
        self._routing_pending_nodes_table = self._ddb.Table(self._routing_pending_nodes_table_name)
        self._routing_active_compute_records_table = self._ddb.Table(self._routing_active_compute_records_table_name)
        self._routing_lock_table = self._ddb.Table(self._routing_lock_table_name)
        self._routing_lock_retainer_id = self._create_retainer_id_for_session()

    def provide_runtime_trusted_entities(self) -> List[str]:
        return []
        # return ["dynamodb.amazonaws.com", "dynamodb.application-autoscaling.amazonaws.com"]

    def provide_runtime_default_policies(self) -> List[str]:
        return []

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        return [
            ConstructPermission(
                [
                    f"arn:aws:dynamodb:{self._region}:{self._account_id}:table/{self._routing_table_name}*",
                    f"arn:aws:dynamodb:{self._region}:{self._account_id}:table/{self._routing_history_table_name}*",
                    f"arn:aws:dynamodb:{self._region}:{self._account_id}:table/{self._routing_pending_nodes_table_name}*",
                    f"arn:aws:dynamodb:{self._region}:{self._account_id}:table/{self._routing_active_compute_records_table_name}*",
                    f"arn:aws:dynamodb:{self._region}:{self._account_id}:table/{self._routing_lock_table_name}*",
                ],
                [
                    "dynamodb:Describe*",
                    "dynamodb:List*",
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:PutItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:BatchWriteItem",
                    # "dynamodb:BatchGetItem",
                    # "dax:*",
                    # "application-autoscaling:*"
                ],
            ),
        ]

    @classmethod
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        region: str = params[AWSCommonParams.REGION]
        account_id: str = params[AWSCommonParams.ACCOUNT_ID]
        context_id: str = params[ActivationParams.CONTEXT_ID].lower()
        return [
            ConstructPermission(
                [
                    # add asterisk to support table sub-resources such as indexes (for Query operation)
                    # e.g arn:aws:dynamodb:<REGION>:<ACCOUNT>:table/<TABLE_NAME>/index/compute_trigger_index
                    f"arn:aws:dynamodb:{region}:{account_id}:table/{cls.ROUTES_TABLE_NAME_FORMAT.format(cls.__name__, context_id, region)}*",
                    f"arn:aws:dynamodb:{region}:{account_id}:table/{cls.ROUTES_HISTORY_TABLE_NAME_FORMAT.format(cls.__name__, context_id, region)}*",
                    f"arn:aws:dynamodb:{region}:{account_id}:table/{cls.ROUTES_PENDING_NODES_TABLE_NAME_FORMAT.format(cls.__name__, context_id, region)}*",
                    f"arn:aws:dynamodb:{region}:{account_id}:table/{cls.ROUTES_ACTIVE_COMPUTE_RECORDS_TABLE_NAME_FORMAT.format(cls.__name__, context_id, region)}*",
                    f"arn:aws:dynamodb:{region}:{account_id}:table/{cls.ROUTES_LOCK_TABLE_NAME_FORMAT.format(cls.__name__, context_id, region)}*",
                ],
                ["dynamodb:*"],
            ),
            ConstructPermission(
                ["*"],
                ["dax:*", "application-autoscaling:*", "iam:CreateServiceLinkedRole", "iam:DeleteServiceLinkedRole"],
            ),
        ]

    def _provide_route_metrics(self, route: Route) -> List[ConstructInternalMetricDesc]:
        metrics = super()._provide_route_metrics(route)
        # todo
        # metrics.extend(...)
        return metrics

    # overrides
    def _provide_internal_metrics(self) -> List[ConstructInternalMetricDesc]:
        """Provide internal metrics (of type INTERNAL_METRIC) that should be managed by RheocerOS and emitted by this
        driver via Diagnostics::emit.
        These metrics are logical metrics generated by the driver (with no assumption on other drivers and other details
        about the underlying platform). So as a driver impl, you want Diagnostics driver to manage those metrics and
        bind them to alarms, etc. Example: Routing metrics.
        """
        metrics = super()._provide_internal_metrics()
        # todo
        # metrics.extend(...)
        return metrics

    # overrides
    def _provide_internal_alarms(self) -> List[Signal]:
        """Provide internal alarms (of type INTERNAL_ALARM OR INTERNAL_COMPOSITE_ALARM) managed/emitted
        by this driver impl"""
        return []

    # overrides
    def _provide_system_metrics(self) -> List[Signal]:
        """Expose system generated metrics to the rest of the platform in a consolidated, filtered and
        well-defined RheocerOS metric signal format.

        Refer
            https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/metrics-dimensions.html

        for remarks on supported periods and statistics.
        """
        table_names_and_alias = {
            self._routing_table_name: "routingTable",
            self._routing_history_table_name: "routingHistoryTable",
            self._routing_pending_nodes_table_name: "routingPendingNodeTable",
            self._routing_active_compute_records_table_name: "routingActiveComputeRecordsTable",
            self._routing_lock_table_name: "routingLockTable",
        }
        # where dimension is Type='count'
        table_metrics_and_supported_statistics = {
            "ConsumedWriteCapacityUnits": ["*"],
            # [MetricStatistic.MINIMUM, MetricStatistic.MAXIMUM, MetricStatistic.SUM, MetricStatistic.AVERAGE, MetricStatistic.SAMPLE_COUNT],
            "ConsumedReadCapacityUnits": ["*"],
            "ProvisionedWriteCapacityUnits": ["*"],
            "ProvisionedReadCapacityUnits": ["*"],
            "TransactionConflict": ["*"],
            "ReadThrottleEvents": ["*"],
            "WriteThrottleEvents": ["*"],
            # TODO enable when default CW dashboard max widget limit issue is handled
            # "ConditionalCheckFailedRequests": []
        }

        supported_operations = ["BatchWriteItem", "DeleteItem", "GetItem", "PutItem", "Query", "Scan"]
        operation_metrics_and_supported_statistics = {"SuccessfulRequestLatency": ["*"], "ThrottledRequests": ["*"]}

        return [
            Signal(
                SignalType.CW_METRIC_DATA_CREATION,
                CWMetricSignalSourceAccessSpec(
                    "AWS/DynamoDB",
                    {"TableName": table_name},
                    # metadata (should be visible in front-end as well)
                    **{"Notes": "Supports 1 min period"},
                ),
                SignalDomainSpec(
                    dimension_filter_spec=DimensionFilter.load_raw(
                        {
                            metric_name: {  # Name  (overwrite in filter spec to make them visible to the user. otherwise user should specify the name,
                                #          in that case platform abstraction is broken since user should have a very clear idea about what is
                                #          providing these metrics).
                                statistic: {  # Statistic
                                    "*": {  # Period  (AWS emits with 1 min by default), let user decide.
                                        "*": {}  # Time  always leave it 'any' if not experimenting.
                                    }
                                }
                                for statistic in table_metrics_and_supported_statistics[metric_name]
                            }
                            for metric_name in table_metrics_and_supported_statistics.keys()
                        }
                    ),
                    dimension_spec=None,
                    integrity_check_protocol=None,
                ),
                # make sure that default metric alias/ID complies with CW expectation (first letter lower case).
                table_alias,
            )
            for table_name, table_alias in table_names_and_alias.items()
        ] + [
            Signal(
                SignalType.CW_METRIC_DATA_CREATION,
                CWMetricSignalSourceAccessSpec(
                    "AWS/DynamoDB",
                    {"TableName": table_name, "Operation": operation},
                    # metadata (should be visible in front-end as well)
                    **{"Notes": "Supports 1 min period"},
                ),
                SignalDomainSpec(
                    dimension_filter_spec=DimensionFilter.load_raw(
                        {
                            metric_name: {  # Name  (overwrite in filter spec to make them visible to the user. otherwise user should specify the name,
                                #          in that case platform abstraction is broken since user should have a very clear idea about what is
                                #          providing these metrics).
                                statistic: {  # Statistic
                                    "*": {  # Period  (AWS emits with 1 min by default), let user decide.
                                        "*": {}  # Time  always leave it 'any' if not experimenting.
                                    }
                                }
                                for statistic in operation_metrics_and_supported_statistics[metric_name]
                            }
                            for metric_name in operation_metrics_and_supported_statistics.keys()
                        }
                    ),
                    dimension_spec=None,
                    integrity_check_protocol=None,
                ),
                # make sure that default metric alias/ID complies with CW expectation (first letter lower case).
                f"{table_alias}.{operation}",
            )
            for table_name, table_alias in table_names_and_alias.items()
            for operation in supported_operations
        ]

    def _update_route_table_read_scaling(self, table_name, min_capacity: int = 20, max_capacity: int = 500):
        if self.billing_mode == BillingMode.PAY_PER_REQUEST:
            return

        exponential_retry(
            register_scalable_target,
            ["ConcurrentUpdateException", "InternalServiceException"],
            auto_scaling_client=self._ddb_scaling,
            service_name="dynamodb",
            resource_id="table/{}".format(table_name),
            scalable_dimension="dynamodb:table:ReadCapacityUnits",
            min_capacity=min_capacity,
            max_capacity=max_capacity,
        )

        exponential_retry(
            put_target_tracking_scaling_policy,
            ["ConcurrentUpdateException", "InternalServiceException"],
            auto_scaling_client=self._ddb_scaling,
            service_name="dynamodb",
            resource_id="table/{}".format(table_name),
            scalable_dimension="dynamodb:table:ReadCapacityUnits",
            policy_dict={
                "PolicyName": "DDBReadScaling_{}".format(table_name),
                "PolicyConfig": {
                    "TargetValue": 50.0,
                    "PredefinedMetricSpecification": {"PredefinedMetricType": "DynamoDBReadCapacityUtilization"},
                },
            },
        )

    def _update_route_table_write_scaling(self, table_name, min_capacity: int = 5, max_capacity: int = 500):
        if self.billing_mode == BillingMode.PAY_PER_REQUEST:
            return

        exponential_retry(
            register_scalable_target,
            ["ConcurrentUpdateException", "InternalServiceException"],
            auto_scaling_client=self._ddb_scaling,
            service_name="dynamodb",
            resource_id="table/{}".format(table_name),
            scalable_dimension="dynamodb:table:WriteCapacityUnits",
            min_capacity=min_capacity,
            max_capacity=max_capacity,
        )

        exponential_retry(
            put_target_tracking_scaling_policy,
            ["ConcurrentUpdateException", "InternalServiceException"],
            auto_scaling_client=self._ddb_scaling,
            service_name="dynamodb",
            resource_id="table/{}".format(table_name),
            scalable_dimension="dynamodb:table:WriteCapacityUnits",
            policy_dict={
                "PolicyName": "DDBWriteScaling_{}".format(table_name),
                "PolicyConfig": {
                    "TargetValue": 50.0,
                    "PredefinedMetricSpecification": {"PredefinedMetricType": "DynamoDBWriteCapacityUtilization"},
                },
            },
        )

    def _update_route_table_scaling(self, table_name: str, rcu_min: int = 40, rcu_max: int = 500, wcu_min: int = 20, wcu_max: int = 500):
        self._update_route_table_read_scaling(table_name, rcu_min, rcu_max)
        self._update_route_table_write_scaling(table_name, wcu_min, wcu_max)

    def _create_route_table(self):
        try:
            table = exponential_retry(
                create_table,
                ["LimitExceededException", "InternalServerError"],
                ddb_resource=self._ddb,
                table_name=self._routing_table_name,
                key_schema=[{"AttributeName": "route_id", "KeyType": "HASH"}],  # Partition key
                attribute_def=[{"AttributeName": "route_id", "AttributeType": "S"}],
                # When billing mode is provisioned we need to provide the
                # RCUs and WCUs but the following values are just dummy.
                # Refer Auto Scaling Capacities above.
                provisioned_throughput=(
                    {"ReadCapacityUnits": 20, "WriteCapacityUnits": 20} if self.billing_mode == BillingMode.PROVISIONED else None
                ),
                billing_mode=self.billing_mode,
            )

        except ClientError as error:
            if error.response["Error"]["Code"] == "ResourceInUseException":
                self._update_route_table_scaling(self._routing_table_name)
                # BACKWARDS compatibility
                # FUTURE remove (create_table and update_table enables PITR)
                enable_PITR(self._routing_table)
            raise

        self._update_route_table_scaling(self._routing_table_name)
        return table

    def _create_route_history_table(self):
        try:
            table = exponential_retry(
                create_table,
                ["LimitExceededException", "InternalServerError"],
                ddb_resource=self._ddb,
                table_name=self._routing_history_table_name,
                key_schema=[
                    {"AttributeName": "route_id", "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "compute_record_sort_key", "KeyType": "RANGE"},  # Partition key
                ],
                attribute_def=[
                    {"AttributeName": "route_id", "AttributeType": "S"},
                    {"AttributeName": "compute_record_sort_key", "AttributeType": "S"},
                    {"AttributeName": "compute_trigger_timestamp_utc", "AttributeType": "N"},
                    {"AttributeName": "compute_finished_timestamp_utc", "AttributeType": "N"},
                    {"AttributeName": "slot_type", "AttributeType": "N"},
                    {"AttributeName": "compute_session_state", "AttributeType": "S"},
                ],
                local_secondary_index=[
                    {
                        "IndexName": self.HISTORY_TABLE_COMPUTE_TRIGGER_INDEX_NAME,
                        "KeySchema": [
                            {"AttributeName": "route_id", "KeyType": "HASH"},  # Partition key
                            {"AttributeName": "compute_trigger_timestamp_utc", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                    {
                        "IndexName": self.HISTORY_TABLE_COMPUTE_FINISHED_INDEX_NAME,
                        "KeySchema": [
                            {"AttributeName": "route_id", "KeyType": "HASH"},  # Partition key
                            {"AttributeName": "compute_finished_timestamp_utc", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                    {
                        "IndexName": self.HISTORY_TABLE_SLOT_TYPE_INDEX_NAME,
                        "KeySchema": [
                            {"AttributeName": "route_id", "KeyType": "HASH"},  # Partition key
                            {"AttributeName": "slot_type", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                    {
                        "IndexName": self.HISTORY_TABLE_SESSION_STATE_INDEX_NAME,
                        "KeySchema": [
                            {"AttributeName": "route_id", "KeyType": "HASH"},  # Partition key
                            {"AttributeName": "compute_session_state", "KeyType": "RANGE"},
                        ],
                        "Projection": {"ProjectionType": "ALL"},
                    },
                ],
                # When billing mode is provisioned we need to provide the
                # RCUs and WCUs but the following values are just dummy.
                # Refer Auto Scaling Capacities above.
                provisioned_throughput=(
                    {"ReadCapacityUnits": 20, "WriteCapacityUnits": 10} if self.billing_mode == BillingMode.PROVISIONED else None
                ),
                billing_mode=self.billing_mode,
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "ResourceInUseException":
                self._update_route_table_scaling(self._routing_history_table_name, rcu_min=400, rcu_max=1000, wcu_min=50, wcu_max=1000)
                # BACKWARDS compatibility
                # FUTURE remove (create_table and update_table enables PITR)
                enable_PITR(self._routing_history_table)
            raise

        self._update_route_table_scaling(self._routing_history_table_name, rcu_min=400, rcu_max=1000, wcu_min=50, wcu_max=1000)
        return table

    def _create_route_pending_nodes_table(self):
        try:
            table = exponential_retry(
                create_table,
                ["LimitExceededException", "InternalServerError"],
                ddb_resource=self._ddb,
                table_name=self._routing_pending_nodes_table_name,
                key_schema=[
                    {"AttributeName": "route_id", "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "pending_node_id", "KeyType": "RANGE"},  # Partition key
                ],
                attribute_def=[
                    {"AttributeName": "route_id", "AttributeType": "S"},
                    {"AttributeName": "pending_node_id", "AttributeType": "S"},
                ],
                # When billing mode is provisioned we need to provide the
                # RCUs and WCUs but the following values are just dummy.
                # Refer Auto Scaling Capacities above.
                provisioned_throughput=(
                    {"ReadCapacityUnits": 20, "WriteCapacityUnits": 10} if self.billing_mode == BillingMode.PROVISIONED else None
                ),
                billing_mode=self.billing_mode,
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "ResourceInUseException":
                self._update_route_table_scaling(
                    self._routing_pending_nodes_table_name, rcu_min=100, rcu_max=1000, wcu_min=75, wcu_max=1000
                )
                # BACKWARDS compatibility
                # FUTURE remove (create_table and update_table enables PITR)
                enable_PITR(self._routing_pending_nodes_table)
            raise

        self._update_route_table_scaling(self._routing_pending_nodes_table_name, rcu_min=100, rcu_max=1000, wcu_min=75, wcu_max=1000)
        return table

    def _create_route_active_compute_records_table(self):
        try:
            table = exponential_retry(
                create_table,
                ["LimitExceededException", "InternalServerError"],
                ddb_resource=self._ddb,
                table_name=self._routing_active_compute_records_table_name,
                key_schema=[
                    {"AttributeName": "route_id", "KeyType": "HASH"},  # Partition key
                    {"AttributeName": "active_compute_record_id", "KeyType": "RANGE"},  # Partition key
                ],
                attribute_def=[
                    {"AttributeName": "route_id", "AttributeType": "S"},
                    {"AttributeName": "active_compute_record_id", "AttributeType": "S"},
                ],
                # When billing mode is provisioned we need to provide the
                # RCUs and WCUs but the following values are just dummy.
                # Refer Auto Scaling Capacities above.
                provisioned_throughput=(
                    {"ReadCapacityUnits": 20, "WriteCapacityUnits": 10} if self.billing_mode == BillingMode.PROVISIONED else None
                ),
                billing_mode=self.billing_mode,
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "ResourceInUseException":
                self._update_route_table_scaling(
                    self._routing_active_compute_records_table_name, rcu_min=100, rcu_max=1000, wcu_min=50, wcu_max=1000
                )
                # BACKWARDS compatibility
                # FUTURE remove (create_table and update_table enables PITR)
                enable_PITR(self._routing_active_compute_records_table)
            raise

        self._update_route_table_scaling(
            self._routing_active_compute_records_table_name, rcu_min=100, rcu_max=1000, wcu_min=50, wcu_max=1000
        )
        return table

    def _create_route_lock_table(self):
        try:
            table = create_lock_table(self._ddb, self._routing_lock_table_name)
        except ClientError as error:
            if error.response["Error"]["Code"] == "ResourceInUseException":
                self._update_route_table_scaling(self._routing_lock_table_name, rcu_min=20, wcu_min=50, wcu_max=1000)
                # BACKWARDS compatibility
                # FUTURE remove (create_table and update_table enables PITR)
                enable_PITR(self._routing_lock_table)
            raise

        self._update_route_table_scaling(self._routing_lock_table_name, rcu_min=20, wcu_min=50, wcu_max=1000)
        return table

    def activate(self) -> None:
        """Provide the impl specific resources, the rest will be taken care of by RoutingTable"""
        try:
            self._create_route_table()
        except ClientError as error:
            if error.response["Error"]["Code"] == "ResourceInUseException":
                # let the app-owner decide on clearing the active routes.
                # developers can access routing_table instance and do the same (like before/after an activation).
                if self.activation_strategy == RoutingActivationStrategy.ALWAYS_CLEAR_ACTIVE_ROUTES:
                    self._clear_active_routes()
                pass
            else:
                module_logger.error(f"An error occurred while trying to create/reset {self._routing_table_name}")
                raise

        try:
            # just check the existence
            # this RoutingTable impl, keeps the full historical track of ComputeRecords (even for Routes which are not
            # in use anymore).
            self._create_route_history_table()
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceInUseException":
                module_logger.error(
                    f"An error occurred while trying to provision/update routing table " f"{self._routing_history_table_name}!"
                )
                raise

        try:
            self._create_route_pending_nodes_table()
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceInUseException":
                module_logger.error(
                    f"An error occurred while trying to provision/update routing table " f"{self._routing_pending_nodes_table_name}!"
                )
                raise

        try:
            self._create_route_active_compute_records_table()
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceInUseException":
                module_logger.error(
                    f"An error occurred while trying to provision/update routing table "
                    f"{self._routing_active_compute_records_table_name}!"
                )
                raise

        try:
            self._create_route_lock_table()
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceInUseException":
                module_logger.error(
                    f"An error occurred while trying to provision/update routing table " f"{self._routing_lock_table_name}!"
                )
                raise

        # other details are taken care of by the abstract RoutingTable.
        super().activate()

    def rollback(self) -> None:
        super().rollback()

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)
        # TODO

    def hook_security_conf(
        self, security_conf: ConstructSecurityConf, platform_security_conf: Dict[Type["BaseConstruct"], ConstructSecurityConf]
    ) -> None:
        if security_conf and security_conf.persisting:
            raise NotImplementedError(f"Security/Encryption in RoutingTable driver {self.__class__.__name__} not supported yet.")

        super().hook_security_conf(security_conf, platform_security_conf)

    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_internal_signals(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_construct_connections(
        self, new_construct_conns: Set["_PendingConnRequest"], current_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _process_security_conf(self, new_security_conf: ConstructSecurityConf, current_security_conf: ConstructSecurityConf) -> None:
        pass

    def _revert_external(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        pass

    def _revert_construct_connections(
        self, construct_conns: Set["_PendingConnRequest"], prev_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _revert_internal_signals(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        # TODO
        pass

    def _revert_security_conf(selfs, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        pass


class AWSS3RoutingTable(RoutingTable):
    """RoutingTable impl based on S3.

    Trade-offs:

        Pros:

        Cons:

    """

    pass
