# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
import logging
import time
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, Type, Union, cast

from intelliflow.core.application.context.context import Context
from intelliflow.core.application.context.instruction import Instruction
from intelliflow.core.application.context.node.base import AlarmNode, CompositeAlarmNode, DataNode, MetricNode, Node, TimerNode
from intelliflow.core.application.context.node.external.alarm.nodes import ExternalAlarmNode
from intelliflow.core.application.context.node.external.metric.nodes import ExternalMetricNode
from intelliflow.core.application.context.node.external.nodes import ExternalDataNode
from intelliflow.core.application.context.node.filtered_views import FilteredView
from intelliflow.core.application.context.node.internal.alarm.nodes import InternalAlarmNode, InternalCompositeAlarmNode
from intelliflow.core.application.context.node.internal.metric.nodes import InternalMetricNode
from intelliflow.core.application.context.node.internal.nodes import InternalDataNode
from intelliflow.core.application.context.node.marshaling.nodes import MarshalerNode, MarshalingView
from intelliflow.core.entity import CoreData
from intelliflow.core.platform.compute_targets.descriptor import ComputeDescriptor
from intelliflow.core.platform.constructs import BaseConstruct, ConstructSecurityConf, FeedBackSignalProcessingMode, RoutingTable
from intelliflow.core.platform.definitions.compute import (
    ComputeFailedSessionState,
    ComputeFailedSessionStateType,
    ComputeResponseType,
    ComputeSessionStateType,
)
from intelliflow.core.platform.development import Configuration, HostPlatform
from intelliflow.core.platform.endpoint import DevEndpoint
from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.dimension_defs import NameType as OutputDimensionNameType
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import (
    AlarmComparisonOperator,
    AlarmDefaultActionsMap,
    AlarmParams,
    AlarmRule,
    AlarmTreatMissingData,
    AlarmType,
    CompositeAlarmParams,
    MetricExpression,
    MetricSubDimensionMap,
    MetricSubDimensionMapType,
)
from intelliflow.core.signal_processing.dimension_constructs import (
    DIMENSION_VARIANT_IDENTICAL_MAP_FUNC,
    Dimension,
    DimensionFilter,
    DimensionSpec,
    DimensionVariant,
    DimensionVariantFactory,
    DimensionVariantMapFunc,
    RawDimensionFilterInput,
)
from intelliflow.core.signal_processing.routing_runtime_constructs import (
    Route,
    RouteExecutionHook,
    RouteID,
    RoutePendingNodeHook,
    RuntimeLinkNode,
)
from intelliflow.core.signal_processing.signal import (
    DimensionLinkMatrix,
    Signal,
    SignalDimensionLink,
    SignalDimensionTuple,
    SignalDomainSpec,
    SignalIntegrityProtocol,
    SignalLinkNode,
    SignalProvider,
)
from intelliflow.core.signal_processing.signal_source import SignalSourceAccessSpec, SignalSourceType
from intelliflow.core.signal_processing.slot import SlotType

from .core_application import ApplicationID, ApplicationState, CoreApplication

logger = logging.getLogger(__name__)


class _DevPersistedState(CoreData):
    def __init__(self, serialized_dev_context: str) -> None:
        self.serialized_dev_context = serialized_dev_context


class Application(CoreApplication):
    class QueryContext(Enum):
        DEV_CONTEXT = 1
        ACTIVE_RUNTIME_CONTEXT = 2
        ALL = 3

    class QueryApplicationScope(Enum):
        CURRENT_APP_ONLY = 1
        EXTERNAL_APPS_ONLY = 2
        ALL = 3

    def __init__(self, id: ApplicationID, platform: HostPlatform) -> None:
        super().__init__(id, platform)
        self._dev_context: Context = Context()

    def attach(self):
        if self._active_context:
            self._dev_context = self._active_context.clone()

    def save_dev_state(self) -> None:
        dev_state = _DevPersistedState(self._dev_context.serialize(True))
        persisted_state: str = dumps(dev_state)
        self._platform.storage.save(persisted_state, [Application.__name__], _DevPersistedState.__name__)

    def load_dev_state(self) -> None:
        if self._platform.storage.check_object([Application.__name__], _DevPersistedState.__name__):
            persisted_state: str = self._platform.storage.load([Application.__name__], _DevPersistedState.__name__)
            saved_state: _DevPersistedState = loads(persisted_state)
            self._dev_context = loads(saved_state.serialized_dev_context)
            self._dev_context._deserialized_init(self._platform)

    @property
    def dev_context(self) -> Context:
        return self._dev_context

    @property
    def uuid(self) -> str:
        return self._platform.conf._generate_unique_id_for_context()

    def provision_remote_dev_env(self, endpoint_attrs: Dict[str, Any]) -> DevEndpoint:
        """Adds or updates a remote dev-env (e.g notebook) relying on the current
        configuration of the platform.

        For example; AWSConfiguration -> Sagemaker Notebook Instance

        Returns a DevEndpoint object which can be used to analyze, start and stop the remote endpoint.
        """
        return cast("HostPlatform", self.platform).provision_remote_dev_env(endpoint_attrs)

    def get_remote_dev_env(self) -> DevEndpoint:
        return cast("HostPlatform", self.platform).get_remote_dev_env()

    def activate(self, allow_concurrent_executions=True) -> None:
        """Interpret dev context and generate platform code in the forms of
          - Signals, Routes and Platform (up/downstream) connections

        Warning: this API is not synchronized. it is client's (application owner's) responsibility to manage/avoid
        possible concurrent calls to this API for the same application, from the same [other threads/processes] or
        other endpoints].

        :param allow_concurrent_executions: enables a strict orchestration to control concurrent (async) executions
        that would use the older version of the application and cause complications.
        """
        logger.critical(f"Activating application: {self.id!r}")
        self._dev_context.check_referential_integrity()
        if self.platform.terminated or self.state == ApplicationState.TERMINATING:
            # support re-activating a terminated app instance.
            # simply go through the same logic in Application::__init__ on behalf of the client.
            # otherwise, clients would need to create a new application instance with the same platform
            # for reactivation.
            self.platform.context_id = self.id
            self.platform.terminated = False
        if not allow_concurrent_executions and self._state == ApplicationState.ACTIVE:
            logger.critical(f"Concurrent executions are not allowed. Pausing background/remote signal processing...")
            self.pause()
            logger.critical(f"Signal processing paused!")
            while self.has_active_routes():
                active_route_records = self.get_active_routes()
                logger.critical(f"There are still {len(active_route_records)} active routes/nodes.")
                logger.critical(f"Route IDs: {[r.route.route_id for r in active_route_records]!r}")
                logger.critical(f"Waiting for 60 seconds...")
                time.sleep(60)
                self.update_active_routes_status()
            logger.critical("All routes idle now! Resuming the activation process...")

        if self._active_context:
            #  get downstream/upstream change set
            for remote_app in self._active_context.external_data - self._dev_context.external_data:
                self._platform.disconnect_upstream(remote_app.platform)

            for downstream_dependency in self._active_context.downstream_dependencies - self._dev_context.downstream_dependencies:
                self._platform.disconnect_downstream(downstream_dependency.id, downstream_dependency.conf)

        # new platform connections, Signals and Routes will be generated by the following call to _dev_context.
        self._dev_context.activate(self._platform)
        self._platform.activate()
        self._activation_completed()
        # current dev context is now the active state
        self._active_context = self._dev_context.clone()

        try:
            self._platform.processor.resume()
        except Exception as err:
            logger.critical(f"An error {err!r} encountered while resuming the application after the successful activation.")
            logger.critical(
                f"Application is in a coherent state but still PAUSED. A subsequent call on "
                f"Application::resume API might help recover from this state."
            )
            raise err

        self._state = ApplicationState.ACTIVE
        # persist the core / active state.
        super()._save()
        logger.critical(f"Activation complete!")

    def _activation_completed(self) -> None:
        # first iterate over updated nodes
        # we will skip checking upstream 'create_data' calls on this. we have to use the updated node information
        # and compute_targets from 'update_data' API (if any)
        def _notify(self, instruction: Instruction):
            # TODO indexed access will be removed when Instruction will get the API stackframe via inject
            compute_descriptors: Sequence[ComputeDescriptor] = instruction.args[5]
            if compute_descriptors:
                for comp_desc in compute_descriptors:
                    comp_desc.activation_completed(self.platform)

            execution_hook: Optional[RouteExecutionHook] = instruction.args[6]
            pending_node_hook: Optional[RoutePendingNodeHook] = instruction.args[7]
            for hook in (execution_hook, pending_node_hook):
                if hook:
                    for callback in hook.callbacks():
                        if isinstance(callback, ComputeDescriptor):
                            callback.activation_completed(self.platform)

        updated_nodes = set()
        for instruction in self._dev_context.instruction_chain:
            if instruction.user_command == "update_data":
                _notify(self, instruction)

                updated_nodes.add(instruction.output_node.route_id)

        for instruction in self._dev_context.instruction_chain:
            if instruction.user_command == "create_data" and instruction.output_node.route_id not in updated_nodes:
                _notify(self, instruction)

    def terminate(self, wait_for_active_routes=False) -> None:
        """Do a graceful shutdown of the whole application.

        Graceful in a way that it is logically the inverse of activation.
        It is comprehensive, guaranteed to reverse the activation (if any).
        This approach enables us to achieve reuse in low-level termination sequences as well.

        It is like going back to a state where it can be assumed that there has been no activations before,
        keeping the data from the active state intact. It does not change the current development context but
        nullifies the active state. So in order to activate again, same application object can be reused to call
        'activate' yielding the same active state from before the termination.

        And more importantly, it is ok to make repetitive calls to this API. It is safe to do that
        even in scenarios where the workflow fails and another attempt is intended to complete the
        termination.

        But it is not safe to call this API concurrently (on the same machine or from other endpoints).

        :param wait_for_active_routes: graceful shutdown in terms of active executions, application will
        pause but also wait for active executions to end in order to achieve a steady-state, only then
        will resume the actual termination process.
        """
        if self._state not in [ApplicationState.ACTIVE, ApplicationState.PAUSED, ApplicationState.TERMINATING]:
            raise RuntimeError(f"Cannot terminate while application state is {self._state!r}!")
        logger.critical(f"Terminating application: {self.id!r}")
        logger.critical(f"Pausing background/remote signal processing...")
        self.pause()
        logger.critical(f"Signal processing paused!")
        if wait_for_active_routes:
            # sleep some time allow event propagation after the pause. if some events that were received before
            # the pause are still being processed, then this duration will be enough for them to go through routing
            # and possibly trigger executions. Those possible executions will be tracked in the subsequent loop.
            time.sleep(5)
            logger.critical("Checking active routes/executions...")
            while self.has_active_routes():
                active_route_records = self.get_active_routes()
                logger.critical(f"There are still {len(active_route_records)} active routes/nodes.")
                logger.critical(f"Route IDs: {[r.route.route_id for r in active_route_records]!r}")
                logger.critical(f"Waiting for 60 seconds...")
                time.sleep(60)
                self.update_active_routes_status()
            logger.critical("All routes idle now! Resuming the termination process...")

        self._state = ApplicationState.TERMINATING
        super()._save()

        # detach from upstream/downstreams
        # why is this important? it is to make sure that downstream access (via RemoteApplication, etc) won't be
        # possible and cause undefined behaviour from this point on.
        # and also for upstream applications to get notified from this termination.
        if self._active_context:
            for remote_app in self._active_context.external_data:
                self._platform.disconnect_upstream(remote_app.platform)

            for downstream_dependency in self._active_context.downstream_dependencies:
                self._platform.disconnect_downstream(downstream_dependency.id, downstream_dependency.conf)

        self._platform.terminate()
        self._state = ApplicationState.INACTIVE
        self._active_context = Context()
        super()._save()

    def delete(self) -> None:
        """Delete the remaining resources/data of an inactive (or terminated) app.

        Why is this separate from 'terminate' API?
        Because, we want to separate the deallocation of runtime resources from internal data which might in some
        scenarios intended to outlive an RheocerOS application. A safe-guard against deleting peta-bytes of data
        as part of the termination. Independent from the app, data might be used by other purposes. Or more importantly,
        same data can be hooked up by the reincarnated version of the same application (as long as the ID and platform
        configuration match). So we are providing this flexibility to the user.

        And in a more trivial scenario where an application needs to be deleted before even an activation happens,
        then this API can be used to clean up the resources (e.g application data in storage). Because, RheocerOS
        applications are distributed right from begining (instantiation), even before the activation and might require
        resource allocation.

        Upon successful execution of this command, using this Application object will not be allowed.
        If the same internal data is intended to be reused again, then a new Application object with the same ID and
        platform configuration should be created.
        """
        if self._state == ApplicationState.INACTIVE:
            logger.critical(f"Deleting application: {self.id!r}")
            self._platform.delete()
            self._state = ApplicationState.DELETED
        else:
            raise RuntimeError("Only an INACTIVE (terminated) Application can be deleted. You might need to call 'terminate()' first.")

    def pause(self) -> None:
        if self._state == ApplicationState.ACTIVE:
            self._platform.processor.pause()
            self._state = ApplicationState.PAUSED
            super()._save()
            logger.critical("Application paused!")
        else:
            logger.critical(f"Cannot pause the application while the state is {self._state!r}! " f"It should be ACTIVE.")

    def resume(self) -> None:
        if self._state == ApplicationState.PAUSED:
            self._platform.processor.resume()
            self._state = ApplicationState.ACTIVE
            super()._save()
            logger.critical("Application resumed! Ingesting pending or new signals...")
        else:
            logger.critical(f"Cannot resume the application while the state is {self._state!r}! " f"It should be PAUSED.")

    # overrides
    def refresh(self) -> None:
        # important for mount/link support (post MVP)
        # HostPlatform does not support refresh, that is against the whole point behind
        # forward-looking development effort. Refresh might invalidate platform/conf updates/overwrites.
        # So for Application we intend to update the (Core state) active_context only (i.e collaborative editing against same
        # app).
        CoreApplication._sync(self)

        # now refresh the remote applications in the dev-context
        # (active context and its remote applications [if any] has already been updated)
        for remote_app in self._dev_context.external_data:
            remote_app.refresh()

    @classmethod
    def _build_instruction(
        cls, api: str, entity: Union[Node, ApplicationID], inputs: List[Signal], output_node: Node, symbol_tree: Node, *args, **kwargs
    ) -> Instruction:
        return Instruction(entity, inputs, api, args, kwargs, output_node, symbol_tree)

    def get_route_metrics(self, route: Union[str, MarshalerNode, MarshalingView]) -> Dict[Type[BaseConstruct], Dict[str, FilteredView]]:
        """Retrieve all of the internal route/node specific metrics that will be emitted by this application once it is
        activated. These are comprehensive set of metrics including (but not limited to); orchestration state
        transitions around this node (detailed route session state transitions, transient errors / retries, persistence
        failures), runtime edge-cases that cannot be captured by hooks (see RouteExecutionHook, RoutePendingNodeHook in
        create_data API), etc.

        Warning: Input route should belong to this Application (not from imported upstream applications) and its
        reference should be from the current development context (which would be activated) not from the already
        activated context.

        :param route: route/node ID as string; route/node object as a return value from a upstream create_data API call
        or retrieved from application dev-context.
        :return: Metrics are provided in a dictionary for each platform driver [ProcessingUnit, BatchCompute,
        RoutingTable, Storage, Diagnostics, ProcessorQueue]. Routing metric signal alias' used as keys within those
        dictionaries and the values can be used as metric inputs into create_alarm calls. For those metric signals,
        'Name' dimension (first dimension of any metric) must be specified before inputting to create_alarm call. User
        can analyze available 'Name's for the metric signal by calling 'describe()' method on it.

        Example:
            my_etl_node = app.create_data("my_node", compute_target="output=spark('select * from foo')")

            route_metrics_map = app.get_route_metrics(my_etl_node)

            routing_table_metrics_map = route_metrics_map[RoutingTable]
            batch_compute_map = route_metrics_map[BatchCompute]
            # see supported metric signal alias'
            # print(routing_table_metrics.keys())  # ['routing_table.receive', 'routing_table.receive.hook',
                                                   #   'routingTable.my_node.initial_state']

            my_node_triggered_hook_metric = routing_table_metrics_map["routing_table.receive.hook"]
            # dump all of the possible metric 'Name' dimension values
            # my_node_triggered_hook_metric.describe()   # ['IExecutionSucccessHook', 'IExecutionFailureHook', ...]

            # use in an alarm by specifying 'Name' dimension as retry hook type 'IComputeRetryHook' which was observed
            # by 'describe()' calls above.
            # example: have more visibility into transient errors such as "GlueVersion 2.0/3.0 'Resource unavailable'",
            # 2 implicit retries within 60 mins.
            # this also covers forced orchestration retries based on user provided 'retry_count' param on any compute.
            my_node_transient_error_alarm = app.create_alarm(id="my_node_transient_alarm",
                                                           target_metric_or_expression=my_node_triggered_hook_metric['IComputeRetryHook'][MetricStatistic.SUM][MetricPeriod.MINUTES(60)],
                                                           number_of_evaluation_periods=1,
                                                           number_of_datapoint_periods=1,
                                                           comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                                           threshold=2)
            # send email whenever alarm is triggered
            alarm_trigger_node = app.create_data("alarm_action", inputs=[my_node_transient_error_alarm],
                                                 compute_targets=[EMAIL(sender="...", recipient_list=['...']).action()])
        """
        # this 'if' statement's entire purpose is parameter handling to create the <Route> object to be passed to
        # HostPlatform::get_route_metrics call. Pretty typical prologue used in other application APIs which minor
        # differences (type checks, validations).
        if isinstance(route, (str, MarshalerNode, MarshalingView)):
            if isinstance(route, str):
                # - limit the search to this app (exclude upstream apps) since we can extract internal routing metrics.
                # - limit the search to new development context, so as to avoid problematic use of active nodes which
                # might not exist in the new topology. If we allow this, metrics (from here) might be used for
                # downstream alarming which would never work if the active node is to be deleted in the next activation.
                routes = self.get_data(route, Application.QueryApplicationScope.CURRENT_APP_ONLY, Application.QueryContext.DEV_CONTEXT)
                if not routes:
                    raise ValueError(
                        f"Node/Route with ID {route!r} could not be found within the current development" f" context of this application!"
                    )
                internal_data_node = cast(InternalDataNode, routes[0].bound)
            else:
                node = route.marshaler_node if isinstance(route, MarshalingView) else route
                if not isinstance(node.bound, InternalDataNode):
                    raise ValueError(f"Node {type(node.bound)} provided as 'route' parameter to 'get_route_metrics' is not internal!")
                internal_data_node = cast(InternalDataNode, node.bound)

            owner_context_uuid = internal_data_node.signal().resource_access_spec.get_owner_context_uuid()
            if owner_context_uuid != self.uuid:
                raise ValueError(
                    f"'route' ({internal_data_node.data_id!r} provided to get_route_metrics is from "
                    f"another application ({owner_context_uuid!r})! It should be internal to this "
                    f"application ({self.uuid!r})"
                )

            # check whether the node exists in the current dev context. if the user pulled in an active node without
            # adding it to the dev-context, then extracting metrics and using them in downstream alarms would be
            # a false-promise (also an obscure) bug (aka bad, broken routing) for the user.
            if not self._is_data_updated(internal_data_node.data_id):
                raise ValueError(
                    f"Node {internal_data_node.data_id!r} provided as 'route' parameter to "
                    f"'get_route_metrics' has not been added to new development context! Extracting"
                    f" metrics from a node that will not get activated along with the new development"
                    f" context is not allowed."
                )

            route = internal_data_node.create_route()
        else:
            raise ValueError(
                f"'route' parameter type {type(route)} provided to 'get_route_metric' is not supported! "
                f"Please provide route/node ID as string or a node or filtered node object directly to "
                f"retrieve routing metrics."
            )

        metric_signals: Dict[Type["BaseConstruct", List[Signal]]] = self.platform.get_route_metrics(route)
        return {const_type: {s.alias: MarshalingView(None, s, None) for s in metrics} for const_type, metrics in metric_signals.items()}

    def get_platform_metrics(
        self, platform_metric_type: Optional[HostPlatform.MetricType] = HostPlatform.MetricType.ALL
    ) -> Dict[Type[BaseConstruct], Dict[str, FilteredView]]:
        """Get underlying platform metrics which are:
            - auto-generated by system components /resources (e.g AWS resources)
            - used by underlying RheocerOS drivers to emit metrics for specific events, state-transitions in this
            app (at runtime). An example would be routing related changes or more importantly Processor errors, etc.
            - or both

        These won't include routing metrics which should be retrieved via 'get_route_metrics' for each node that was
        added to the new development context of this application (via an upstream create_data call).

        :param platform_metric_type: Changes the result set based on:

            - HostPlatform.MetricType.ORCHESTRATION: will include Platform's own orchestration
        metrics only
            - HostPlatform.MetricType.SYSTEM: will include metrics for underlying system resources only
            - HostPlatform.MetricType.ALL: will include both platform internal orchestration and system metrics. This is
            the default.

        :return: A dict that would have the following format:

        {
            Construct_Type <[Processor | BatchCompute | RoutingTable | Diagnostics | Storage | ProcessorQueue]>: {
                Metric_1_Alias <str>: <MetricSignal>
                ...
                Metric_2_Alias <str>: <MetricSignal>
            }
        }

        Metrics are provided in a dictionary for each platform driver [ProcessingUnit, BatchCompute,
        RoutingTable, Storage, Diagnostics, ProcessorQueue]. Routing metric signal alias' used as keys within those
        dictionaries and the values can be used as metric inputs into create_alarm calls. For those metric signals,
        'Name' dimension (first dimension of any metric) must be specified before inputting to create_alarm call. User
        can analyze available 'Name's for the metric signal by calling 'describe()' method on it.

        Example:
            system_metrics_map = app.get_platform_metrics()
            processor_metrics_map = system_metrics_map[ProcessingUnit]
            # see supported metric signal alias'
            # print(processor_metrics_map.keys())

            # Dump system metrics and see their alias' and sub-dimensions!
            #for metric in processor_metrics_map.values():
            #    print(metric.dimensions())
            #    # dumps metric group ID/alias -> specific MetricNames and other details
            #    print(metric.describe())

            # plese note that "processor.core" is not a concrete metric Name dimension value yet, it is just the
            # alias used internally by the Processor driver impl to group different metrics (with same structure,
            # sub dimensions) into one metric signal.
            processor_core_metric_signal = processor_metrics_map["processor.core"]
            # dump all of the possible metric 'Name' dimension values
            # processor_core_metric_signal.describe()

            # use in an alarm by specifying 'Name' dimension as 'Errors' which was observed by 'describe()' calls above.
            processor_alarm = app.create_alarm(id="processor_alarm",
                                                   target_metric_or_expression="(m1 > 1 OR m2 > 600000)",
                                                   metrics={
                                                       "m1": processor_core_metric_signal['Errors'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                                                       "m2": ...
                                                   ...)
        """
        metric_signals: Dict[Type["BaseConstruct", List[Signal]]] = self.platform.get_metrics()
        return {const_type: {s.alias: MarshalingView(None, s, None) for s in metrics} for const_type, metrics in metric_signals.items()}

    def marshal_external_metric(
        self,
        external_metric_desc: ExternalMetricNode.Descriptor,
        id: str,
        dimension_filter: Optional[Union[DimensionFilter, Dict[str, Any]]] = None,
        sub_dimensions: Optional[MetricSubDimensionMapType] = MetricSubDimensionMap(),
        tags: str = None,
    ) -> MarshalerNode:
        """Import/add an external metric (e.g AWS CW Metric) resource as a signal into the application.

        dimension_filter uses the following spec for specialization/overwrites:

           { MetricDimension.NAME <STRING>: {
                MetricDimension.STATISTIC <MetricStatistic | STRING>: {
                   MetricDimension.PERIOD <MetricPeriod | LONG>: {
                      MetricDimension.TIME <DATETIME>: {
                      }
                   }
                }
            }

        Returned object can be used as an input to create_data API.
        """
        if self.get_metric(
            metric_id=id,
            sub_dimensions=None,  # ignore sub_dimensions
            app_scope=Application.QueryApplicationScope.CURRENT_APP_ONLY,
            context=Application.QueryContext.DEV_CONTEXT,
        ):
            raise ValueError(
                f"MetricNode with id {id!r} already exists!"
                f"You might want to use 'update_external_metric' API to modify an existing metric node."
            )

        if dimension_filter is None:
            dimension_filter = DimensionFilter.load_raw(
                {
                    "*": {  # Any MetricDimension.NAME
                        "*": {"*": {"*": {}}}  # Any MetricDimension.STATISTIC  # Any MetricDimension.PERIOD  # Any MetricDimension.TIME
                    }
                }
            )

        dim_filter: DimensionFilter = (
            DimensionFilter.load_raw(dimension_filter) if not isinstance(dimension_filter, DimensionFilter) else dimension_filter
        )

        external_metric_desc.add(SignalSourceAccessSpec.OWNER_CONTEXT_UUID, self.uuid)
        metric_node: ExternalMetricNode = external_metric_desc.create_node(id, dim_filter, sub_dimensions, self.platform)
        marshaler_node = MarshalerNode(metric_node, tags)
        metric_node.add_child(marshaler_node)

        # - keep the execution order
        # - bookkeeping high-level code
        # TODO use "inspect" module or "sys._getFrame" to simplify
        #  instruction build operation from this api (func) call.
        new_inst: Instruction = self._build_instruction(
            "marshal_external_metric",
            self._id,
            None,
            marshaler_node,
            metric_node,
            id,
            external_metric_desc,
            dimension_filter,
            sub_dimensions,
            tags,
        )
        self._dev_context.add_instruction(new_inst)
        return marshaler_node

    def create_metric(
        self,
        id: str,
        dimension_filter: Optional[Union[DimensionFilter, Dict[str, Any]]] = None,
        sub_dimensions: Optional[MetricSubDimensionMapType] = MetricSubDimensionMap(),
        **kwargs,
    ) -> MarshalerNode:
        """Creates an internal metric definition that will depend on the MetricsStore impl of the underlying Platform.

        'id' can be used to retrieve the metric def from the MetricsStore at runtime to 'emit' metrics. So it actually
        represents the logical/abstract 'Metric Group' for the retrieval of the declaration during development and also
        at runtime.

        Same 'id' is also used as the default 'alias' (metric id) within create_alarm API for MetricExpressions if alias
        is not provided by the user.

        Please note that, different metric definitions created with different 'id's here might cannot contribute
        to same metric instances at runtime even if the same 'Name' dimension values are used during emission. A Metric
        instance is uniquely defined by 'Name' dimension and 'sub-dimensions', and RheocerOS implicitly adds 'id' as a
        metric group ID sub-dimension to each internal metric created with this API.

            Example:
                # declare your internal / custom metrics in application
                generic_internal_metric1 = app.create_metric(id="metric_group1")
                generic_internal_metric2 = app.create_metric(id="metric_group2")
                generic_internal_metric3 = app.create_metric(id="metric_group3", sub_dimensions={"error_type": "foo"})

                # then in your inlined or batch compute (e.g Spark) code, do the follow to emit at runtime:
                # it won't contribute to the metric declared by "metric_group1"
                runtime_platform.diagnostics["metric_group2"]["Error"].emit(1)
                # emit from the first metric internal metric decl, this will contribute to a different metric instance
                # despite that they are emitted using the same metric name 'Error'
                runtime_platform.diagnostics["metric_group1"]["Error"].emit(1)
                # same again, will contribute to a different metric even if the same metric Name is used
                runtime_platform.diagnostics["metric_group3"]["Error"].emit(1)
                runtime_platform.diagnostics["metric_group3"]["Success"].emit([MetricValueCountPairData(5.0), # Count = 1 by default
                                                                               MetricValueCountPairData(Value=3.0, Count=2)])

        Returned 'MarshalerNode' object can be used as an input to create_alarm call or to analyze the metric object.

            Example

            alarm = app.create_alarm(id="daily_error_tracker",
                                   target_metric_or_expression=generic_internal_metric1["Error"][MetricStatistic.SUM][MetricPeriod.MINUTES(24 * 60)],
                                   number_of_evaluation_periods=1,
                                   number_of_datapoint_periods=1,
                                   comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                   threshold=1)
        """

        # check if "ID + subdimensions" is unique
        if self.get_metric(id, sub_dimensions, Application.QueryApplicationScope.CURRENT_APP_ONLY, Application.QueryContext.DEV_CONTEXT):
            raise ValueError(
                f"MetricNode with id {id!r} and sub_dimensions {sub_dimensions!r} already exists!"
                f"You might want to use 'update_metric' API to modify an existing metric node."
            )

        if dimension_filter is None:
            dimension_filter = DimensionFilter.load_raw(
                {
                    "*": {  # Any MetricDimension.NAME
                        "*": {"*": {"*": {}}}  # Any MetricDimension.STATISTIC  # Any MetricDimension.PERIOD  # Any MetricDimension.TIME
                    }
                }
            )

        dim_filter: DimensionFilter = (
            DimensionFilter.load_raw(dimension_filter) if not isinstance(dimension_filter, DimensionFilter) else dimension_filter
        )

        kwargs.update({SignalSourceAccessSpec.OWNER_CONTEXT_UUID: self.uuid})
        metric_node = InternalMetricNode(id, dim_filter, sub_dimensions, **kwargs)
        marshaler_node = MarshalerNode(metric_node, None)
        metric_node.add_child(marshaler_node)

        new_inst: Instruction = self._build_instruction(
            "create_metric", self._id, None, marshaler_node, metric_node, id, dimension_filter, sub_dimensions, **kwargs
        )
        self._dev_context.add_instruction(new_inst)
        return marshaler_node

    def get_metric(
        self,
        metric_id: str,
        sub_dimensions: Optional[MetricSubDimensionMapType] = None,
        app_scope: Optional[QueryApplicationScope] = QueryApplicationScope.ALL,
        context: Optional[QueryContext] = QueryContext.ACTIVE_RUNTIME_CONTEXT,
    ) -> List[MarshalerNode]:
        """Returns the metrics (internal or external) that have the id 'metric_id' and contain the given sub dimensions.

        @param metric_id: only the metrics whose IDs match this param are returned.
        @param sub_dimensions: Optional parameter to narrow down the return value (possible list of metrics) if there
        are multiple internal metrics with the same 'metric_id'.
        @param app_scope determines from which applications the metrics should be retrieved. By default it searches in
        this application and also all applications this applications is connected with
        (see Application::import_upstream_application).
        @param context within all of the applications (determined by 'app_scope'), this determines from which contexts
        the metrics should be retrieved. By default it searches in already activated (remotely active) context/topology.
        So a newly added (inactive) metric node cannot be found using this API unless this parameter is set to
        DEV_CONTEXT or ALL explicitly.
        """
        return [
            child_node
            for data_node in self.query([MetricNode.QueryVisitor(metric_id, sub_dimensions, exact_match=True)], app_scope, context).values()
            for child_node in data_node.child_nodes
            if isinstance(child_node, MarshalerNode)
        ]

    def marshal_external_alarm(
        self,
        external_alarm_desc: ExternalAlarmNode.Descriptor,
        id: str,
        dimension_filter: Optional[Union[DimensionFilter, Dict[str, Any]]] = None,
        alarm_params: Optional[AlarmParams] = None,
        tags: str = None,
    ) -> MarshalerNode:
        """Import/add an external (metric or composite) alarm resource as a new signal into the application."""
        if self.get_alarm(
            alarm_id=id,
            alarm_type=AlarmType.ALL,
            app_scope=Application.QueryApplicationScope.CURRENT_APP_ONLY,
            context=Application.QueryContext.DEV_CONTEXT,
        ):
            raise ValueError(
                f"An AlarmNode or CompositeAlarmNode with id {id!r} already exists!"
                f"You might want to use 'update_external_alarm' API to modify an existing alarm node."
            )

        if dimension_filter is None:
            dimension_filter = DimensionFilter.load_raw({"*": {"*": {}}})  # Any AlarmDimension.STATE_TRANSITION  # Any AlarmDimension.TIME

        dim_filter: DimensionFilter = (
            DimensionFilter.load_raw(dimension_filter) if not isinstance(dimension_filter, DimensionFilter) else dimension_filter
        )

        external_alarm_desc.add(SignalSourceAccessSpec.OWNER_CONTEXT_UUID, self.uuid)
        alarm_node: ExternalAlarmNode = external_alarm_desc.create_node(id, dim_filter, alarm_params, self.platform)
        marshaler_node = MarshalerNode(alarm_node, tags)
        alarm_node.add_child(marshaler_node)

        new_inst: Instruction = self._build_instruction(
            "marshal_external_alarm",
            self._id,
            None,
            marshaler_node,
            alarm_node,
            id,
            external_alarm_desc,
            dimension_filter,
            alarm_params,
            tags,
        )
        self._dev_context.add_instruction(new_inst)
        return marshaler_node

    def create_alarm(
        self,
        id: str,
        target_metric_or_expression: Union[Union[FilteredView, MarshalerNode, Signal], MetricExpression, str],
        number_of_evaluation_periods: Optional[int] = 1,
        number_of_datapoint_periods: Optional[int] = 1,
        comparison_operator: Optional[AlarmComparisonOperator] = AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
        threshold: Optional[float] = 1,
        metrics: Optional[
            Union[List[Union[FilteredView, MarshalerNode, Signal]], Dict[str, Union[FilteredView, MarshalerNode, Signal]]]
        ] = None,
        metric_expressions: Optional[List[MetricExpression]] = None,
        default_actions: Optional[AlarmDefaultActionsMap] = None,
        description: Optional[str] = None,
        treat_missing_data: Optional[AlarmTreatMissingData] = AlarmTreatMissingData.MISSING,
        dimension_filter: Optional[Union[DimensionFilter, Dict[str, Any]]] = None,
        **kwargs,
    ) -> MarshalerNode:
        """Creates an internal alarm that depends on the MetricsStore model impl of the underlying Platform object

        @param target_metric_or_expression: 'target_metric_or_expression' is a MetricExpression, then 'metrics' input
             must be defined, otherwise 'metrics' or metric_expressions definitions will be rejected by the API
             (not accepted if it is a metric signal).
        @param default_actions: If 'default_actions' is defined, then alarm will have direct connections to those actions even without the
            returned object being used in subsequent create_data calls.
        @param description: When 'default_actions' is defined, then this parameter becomes more important since it is
            the only way to pass extra information to those actions (ex. description for CW Internal actions). In other
            cases where alarm is going to be used in create_data, then a more flexible and RheocerOS decorated, rich data
            is provided to actions and also within create_data / compute_targets, users can do more to further configure
            descriptions and message as part of the action (ticketing, email, etc).
        """
        if self.get_alarm(
            alarm_id=id,
            alarm_type=AlarmType.ALL,
            app_scope=Application.QueryApplicationScope.CURRENT_APP_ONLY,
            context=Application.QueryContext.DEV_CONTEXT,
        ):
            raise ValueError(
                f"An alarm or composite alarm with id {id!r} already exists!"
                f"You might want to use 'update_alarm' API to modify an existing alarm node."
            )

        input_signals: List[Signal] = None
        metric_signals: List[Signal] = []

        if isinstance(target_metric_or_expression, str):
            target_metric_or_expression = MetricExpression(expression=target_metric_or_expression)

        if isinstance(target_metric_or_expression, MetricExpression):
            if not metrics:
                raise ValueError(f"Please provide metric signals for the alarm: {id!r}!")

            if isinstance(metrics, Dict):
                metric_signals = [self._get_input_signal(filtered_view, alias) for alias, filtered_view in metrics.items()]
            else:
                metric_signals = [self._get_input_signal(filtered_view) for filtered_view in metrics]

            metric_signals = self._check_upstream(metric_signals)
            # validate signal type
            for s in metric_signals:
                if not s.type.is_metric():
                    raise ValueError(f"Input {s.alias!r} to alarm {id!r} is not a metric!")
                # at this point metric signals should be materialized by the client (name, stats, period)
                self.platform.diagnostics.check_metric_materialization(s)
            input_signals = metric_signals
        else:
            if metrics or metric_expressions:
                raise ValueError(
                    f"Metrics and metric_expressions should not be defined for the alarm {id!r} that" f"f is linked to a single metric!"
                )
            # convert to Signal
            target_metric_or_expression = self._check_upstream_signal(self._get_input_signal(target_metric_or_expression))
            if not target_metric_or_expression.type.is_metric():
                raise ValueError(f"Input {target_metric_or_expression.alias!r} provided as a target to alarm {id!r} is " f"not a metric!")
            # should be materialized by the client (name, stats, period)
            self.platform.diagnostics.check_metric_materialization(target_metric_or_expression)
            input_signals = [target_metric_or_expression]

        if default_actions is None:
            default_actions = AlarmDefaultActionsMap(set())
        else:
            # TODO add validation for each action
            pass

        alarm_params = AlarmParams(
            target_metric_or_expression,
            metric_signals,
            metric_expressions if metric_expressions else [],
            number_of_evaluation_periods,
            number_of_datapoint_periods,
            comparison_operator,
            threshold,
            default_actions,
            description,
            treat_missing_data,
        )

        if dimension_filter is None:
            dimension_filter = DimensionFilter.load_raw({"*": {"*": {}}})  # Any AlarmDimension.STATE_TRANSITION  # Any AlarmDimension.TIME

        kwargs.update({SignalSourceAccessSpec.OWNER_CONTEXT_UUID: self.uuid})
        alarm_node = InternalAlarmNode(id, dimension_filter, alarm_params, **kwargs)
        marshaler_node = MarshalerNode(alarm_node, None)
        alarm_node.add_child(marshaler_node)

        new_inst: Instruction = self._build_instruction(
            "create_alarm", self._id, input_signals, marshaler_node, alarm_node, id, dimension_filter, alarm_params, **kwargs
        )
        self._dev_context.add_instruction(new_inst)
        return marshaler_node

    def create_composite_alarm(
        self,
        id: str,
        alarm_rule: Union[AlarmRule, Signal],
        default_actions: Optional[AlarmDefaultActionsMap] = None,
        description: Optional[str] = None,
        dimension_filter: Optional[Union[DimensionFilter, Dict[str, Any]]] = None,
        **kwargs,
    ) -> MarshalerNode:
        """Creates an internal alarm that depends on the MetricsStore model impl of the underlying Platform object.

        If 'default_actions' is defined, then alarm will have direct connections to those actions even without the
        returned object being used in subsequent create_data calls.

        @param alarm_rule: Either signal Alarm signal or an AlarmRule implicitly created from an expression that contains
        bitwise AND, OR or INVERTED alarm signals or other nested expressions.
            Examples:
               ~my_alarm['OK']
               my_alarm['ALARM']
               my_alarm['ALARM'] & (other_alarm['ALARM'] | foo_alarm['ALARM'])
               my_alarm & (other_alarm | foo_alarm)

           If 'state_transition' dimension ('ALARM', 'OK', 'INSUFFICIENT_DATA' ) is not specified, then 'ALARM' state
           is used by default.

        Returned object can alternatively be used in subsequent create_data calls as an input signal.
        """
        if self.get_alarm(
            alarm_id=id,
            alarm_type=AlarmType.ALL,
            app_scope=Application.QueryApplicationScope.CURRENT_APP_ONLY,
            context=Application.QueryContext.DEV_CONTEXT,
        ):
            raise ValueError(
                f"An AlarmNode or CompositeAlarmNode with id {id!r} already exists!"
                f"You might want to use 'update_alarm' API to modify an existing alarm node."
            )

        if default_actions is None:
            default_actions = AlarmDefaultActionsMap(set())

        alarm_params = CompositeAlarmParams(alarm_rule, default_actions, description)

        if dimension_filter is None:
            dimension_filter = DimensionFilter.load_raw({"*": {"*": {}}})  # Any AlarmDimension.STATE_TRANSITION  # Any AlarmDimension.TIME

        dim_filter: DimensionFilter = (
            DimensionFilter.load_raw(dimension_filter) if not isinstance(dimension_filter, DimensionFilter) else dimension_filter
        )

        kwargs.update({SignalSourceAccessSpec.OWNER_CONTEXT_UUID: self.uuid})
        alarm_node = InternalCompositeAlarmNode(id, dim_filter, alarm_params, **kwargs)
        marshaler_node = MarshalerNode(alarm_node, None)
        alarm_node.add_child(marshaler_node)

        new_inst: Instruction = self._build_instruction(
            "create_composite_alarm",
            self._id,
            alarm_rule.get_alarms(),
            marshaler_node,
            alarm_node,
            id,
            dimension_filter,
            alarm_params,
            **kwargs,
        )
        self._dev_context.add_instruction(new_inst)
        return marshaler_node

    def get_alarm(
        self,
        alarm_id: str,
        alarm_type: AlarmType = AlarmType.ALL,
        app_scope: Optional[QueryApplicationScope] = QueryApplicationScope.ALL,
        context: Optional[QueryContext] = QueryContext.ACTIVE_RUNTIME_CONTEXT,
    ) -> List[MarshalerNode]:
        """Returns the alamrs (internal or external) based on the alarm type"""
        alarms: Dict[str, Node] = dict()
        if alarm_type in [AlarmType.METRIC, AlarmType.ALL]:
            alarms.update(self.query([AlarmNode.QueryVisitor(alarm_id, exact_match=True)], app_scope, context))

        if alarm_type in [AlarmType.COMPOSITE, AlarmType.ALL]:
            alarms.update(self.query([CompositeAlarmNode.QueryVisitor(alarm_id, exact_match=True)], app_scope, context))

        return [child_node for node in alarms.values() for child_node in node.child_nodes if isinstance(child_node, MarshalerNode)]

    def create_timer(self, id: str, schedule_expression: str, date_dimension: DimensionVariant, **kwargs) -> MarshalerNode:
        """Create new timer signal within this application.
        :param id: internal ID of the new signal, can be used to retrieve this signal using get_timer API. It will be
        used as the default alias for this signal if it is used as an input in downstream create_data calls.
        :param schedule_expression: expression supported by underlying platform configuration. E.g in AWSConfiguration
        this parameter is AWS CloudWatch scheduled expressions that can be in either CRON format or "rate(x [minute(s)|day(s)|...])
        :param date_dimension: Just as other RheocerOS signals, this timer will have a DimensionSpec with only one
        dimension. Provide an AnyVariant object with name, type DATETIME and parameters ('format', 'granularity').
        :param kwargs: user provided metadata (for purposes such as cataloguing, etc)
        :return: A new MarshalerNode that encapsulates the timer in development time. Returned value can be used
        as an input to many other Application APIs in a convenient way. Most important of them is 'create_data'
        which can use MarshalerNode and its filtered version (FilteredView) as inputs.
        """

        if self.get_timer(id, Application.QueryApplicationScope.CURRENT_APP_ONLY, Application.QueryContext.DEV_CONTEXT):
            raise ValueError(
                f"TimerNode with id {id!r} already exists!" f"You might want to use 'update_timer' API to modify an existing timer node."
            )

        kwargs.update({SignalSourceAccessSpec.OWNER_CONTEXT_UUID: self.uuid})
        timer_node = TimerNode(id, schedule_expression, date_dimension, self._id, **kwargs)
        marshaler_node = MarshalerNode(timer_node, None)
        timer_node.add_child(marshaler_node)

        new_inst: Instruction = self._build_instruction(
            "create_timer", self._id, None, marshaler_node, timer_node, id, schedule_expression, date_dimension, **kwargs
        )
        self._dev_context.add_instruction(new_inst)
        return marshaler_node

    def get_timer(
        self,
        timer_id: str,
        app_scope: QueryApplicationScope = QueryApplicationScope.ALL,
        context: QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT,
    ) -> List[MarshalerNode]:
        """Retrieves the timer signal from this app or across all of the linked apps based on 'app_scope' parameter."""
        return [
            child_node
            for data_node in self.query([TimerNode.QueryVisitor(timer_id, exact_match=True)], app_scope, context).values()
            for child_node in data_node.child_nodes
            if isinstance(child_node, MarshalerNode)
        ]

    def marshal_external_data(
        self,
        external_data_desc: ExternalDataNode.Descriptor,
        id: Optional[str] = None,
        dimension_spec: Optional[Union[DimensionSpec, Dict[str, Any]]] = None,
        dimension_filter: Optional[Union[DimensionFilter, Dict[str, Any]]] = None,
        protocol: SignalIntegrityProtocol = None,
        tags: str = None,
    ) -> MarshalerNode:
        """Import an external resource (e.g dataset) as a signal so that it can be used in other API calls such as
        create_data, etc.

        :param external_data_desc: Use a resource specific descriptor (such as S3Dataset, GlueTable) to provide details
        of the external resource.
        :param id: internal ID or alias of the signal. You can use this value to retrieve the node reference from the
        application object. It is also used as an alias for the signal/resource in compute codes (Spark, PrestoSQL)
        if not overwritten by the user in a create_data 'inputs' map for example. Will be provided by
        'external_data_desc' object if left as None.
        :param dimension_spec: Specify the dimension spec (e.g partitions and their nested structure). Will be provided
        by 'external_data_desc' object if left as None. If you are not OK with default dimension names and type
        specification provided by the descriptor, then use this spec parameter to overwrite. To check the default spec
        first, you can call 'describe' on the returned MarshalerNode object from this API and analyze the dimensions.
        :param dimension_filter: Specify the dimension values as default filters on the signal. These values will be
        used as prefilters and narrow down the scope for the signal in downstream API calls where this signal is used.
        :param protocol: Specify the protocol that indicates 'trigger' condition or an indicator of an update on the
        underlying resource. For example, '_SUCCESS' file for datasets: SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
        :param tags: User provided extra data to be attached to the node. For structured metadata,
        prefer using 'external_data_desc' kwargs parameter.
        :return: A new MarshalerNode that encapsulates the external data in development time. Returned value can be used
        as an input to many other Application APIs in a convenient way. Most important of them is 'create_data'
        which can use MarshalerNode and its filtered version (FilteredView) as inputs.
        """
        if id is None:
            id = external_data_desc.provide_default_id()
        elif not isinstance(id, str):
            raise ValueError(f"marshal_external_data 'id' is not a string! Provided type: {type(id)}")

        if self.get_data(id, Application.QueryApplicationScope.CURRENT_APP_ONLY, Application.QueryContext.DEV_CONTEXT):
            raise ValueError(
                f"DataNode with id {id!r} already exists!" f"You might want to use 'update_data' API to modify an existing data node."
            )

        if dimension_spec is not None:
            dim_spec: DimensionSpec = (
                DimensionSpec.load_from_pretty(dimension_spec) if not isinstance(dimension_spec, DimensionSpec) else dimension_spec
            )
            if dimension_filter is not None:
                dim_filter: DimensionFilter = (
                    DimensionFilter.load_raw(dimension_filter, cast=dim_spec, error_out=True)
                    if not isinstance(dimension_filter, DimensionFilter)
                    else dimension_filter
                )
                if dim_filter is None:
                    raise ValueError(
                        f"marshal_external_data (id={id}): 'dimension_filter' parameter "
                        f"(type: {type(dimension_filter)}) is not valid! "
                        f"Provide a dict or an instance of DimensionFilter for 'dimension_filter'."
                    )
            else:
                dim_filter: DimensionFilter = DimensionFilter.all_pass(for_spec=dim_spec)

            if not dim_filter.check_spec_match(dim_spec):
                raise ValueError(
                    f"marshal_external_data (id={id}): Dimension filter {dim_filter!r} is not compatible "
                    f"with the dimension spec {dim_spec!r}"
                )

            dim_filter.set_spec(dim_spec)
        elif external_data_desc.requires_dimension_spec():
            raise ValueError(f"'dimension_spec' should be provided for external data {external_data_desc!r}")
        else:
            dim_spec: DimensionSpec = None
            dim_filter: DimensionFilter = None
            if dimension_filter is not None:
                # allow dim_filter to be used for now, external_data_desc will auto-build dim_spec and do the
                # necessary check on user provided filter and set the spec.
                dim_filter = (
                    DimensionFilter.load_raw(dimension_filter, cast=None, error_out=True)
                    if not isinstance(dimension_filter, DimensionFilter)
                    else dimension_filter
                )

        if protocol is not None and not isinstance(protocol, SignalIntegrityProtocol):
            raise ValueError(
                f"marshal_external_data (id={id}) 'protocol' is not of type SignalIntegrityProtocol! Provided type: {type(protocol)}"
            )

        domain_spec = SignalDomainSpec(dim_spec, dim_filter, protocol)
        external_data_desc.add(SignalSourceAccessSpec.OWNER_CONTEXT_UUID, self.uuid)
        data_node: ExternalDataNode = external_data_desc.create_node(id, domain_spec, self.platform)
        marshaler_node = MarshalerNode(data_node, tags)
        data_node.add_child(marshaler_node)

        # - keep the execution order
        # - bookkeeping high-level code
        # TODO use "inspect" module or "sys._getFrame" to simplify
        #  instruction build operation from this api (func) call.
        new_inst: Instruction = self._build_instruction(
            "marshal_external_data",
            self._id,
            None,
            marshaler_node,
            data_node,
            external_data_desc,
            id,
            dimension_spec,
            dimension_filter,
            protocol,
            tags,
        )
        self._dev_context.add_instruction(new_inst)
        return marshaler_node

    def _get_input_signal(self, input_view: Union[FilteredView, MarshalerNode, Signal], alias: str = None) -> Signal:
        if isinstance(input_view, Signal):
            return input_view.clone(alias if alias is not None else input_view.alias)
        elif isinstance(input_view, (FilteredView, SignalProvider)):
            return input_view.get_signal(alias)
        else:
            input_signal = input_view.signal()
            # filter view keeps signal's original alias, so to keep the parity we have to do the following alias
            # check within clone (which does not do that by default).
            return input_signal.clone(alias if alias is not None else input_signal.alias)

    def create_data(
        self,
        id: str,
        inputs: Union[List[Union[FilteredView, MarshalerNode]], Dict[str, Union[FilteredView, MarshalerNode]]],
        input_dim_links: Sequence[Tuple[SignalDimensionTuple, DimensionVariantMapFunc, SignalDimensionTuple]],
        output_dimension_spec: Union[Dict[str, Any], DimensionSpec],
        output_dim_links: Optional[
            Sequence[
                Tuple[
                    Union[OutputDimensionNameType, SignalDimensionTuple],
                    DimensionVariantMapFunc,
                    Union[OutputDimensionNameType, Tuple[OutputDimensionNameType, ...], SignalDimensionTuple],
                ]
            ]
        ],
        compute_targets: Sequence[ComputeDescriptor],
        execution_hook: RouteExecutionHook = None,
        pending_node_hook: RoutePendingNodeHook = None,
        pending_node_expiration_ttl_in_secs: int = None,
        auto_input_dim_linking_enabled=True,
        auto_output_dim_linking_enabled=True,
        auto_backfilling_enabled=False,
        protocol: SignalIntegrityProtocol = InternalDataNode.DEFAULT_DATA_COMPLETION_PROTOCOL,
        **kwargs,
    ) -> MarshalerNode:
        """Add a new data node to this Application, using inputs from the same application or other (upstream) applications
        (if any).

        This action is effective on the client side (front-end) with no effect on the target runtime, before Application::activate()
        is called. So this is essentially a build-time action.

        Client determines the ultimate Application topology by using this API along with other data generation APIs.

        Then when the application is activated, either external signals/events will cause executions (remotely) on routes
        that represent this nodes at runtime, or manually by calling 'Application::process'.

        For an easier way of testing and execution this type of nodes, please see Application::execute API which provides
        a great deal of synchronicity and also convenience to experiment with runtime behaviour of them.

        :param id: id/route_id/data_id of the new internal data node to be created. this ID will be used to find/get this
        node while using the other APIs of the Application. It is also used as part of the route_id to be used in runtime.
        :param inputs: Filtered or unfiltered references of other data nodes which are the return values of previous calls
        to node generating APIs such as marshal_external_data or again the same API 'create_data'.
        :param input_dim_links: How should those inputs be linked to each other over their dimensions? This is important
        to determine executions at runtime. While empty, if 'auto_input_dim_linking_enabled' is set False,
        then any combination of input signals would yield an execution.
        :param output_dimension_spec: What are the dimensions of the signal that would represent this new node? And what is the
        structure/order?
        :param output_dim_links: How should the output and the inputs relate each other? Which dimensions of the output can
        be retrieved from which input dimensions at runtime?
        :param compute_targets: When incoming signals for the inputs are linked successfully and a new execution context is created
        at runtime, which compute targets should be run using those signals and their material dimension values?
        :param execution_hook: Provide an instance of <ExecutionHook> (or <RouteExecutionHook>) to have runtime hooks
        into your own code along with remote execution and compute actions. Each callback/hook can either be pure Python
        Callable or a Callable wrapped by InlinedCompute type. RheocerOS provides interfaces for each hook type. Please
        see the internal types from class <RoutingHookInterface.Execution>: <IExecutionBeginHook>, <IExecutionSkippedHook>,
        <IExecutionSuccessHook>, <IExecutionFailureHook>, ...
        :param pending_node_hook: Provide an instance of <PendingNodeHook> (or <RoutePendingNodeHook>) to have runtime hooks
        into your own code when pending event-trigger groups (pending nodes) are created (first ever event is received), expired or
        when a checkpoint is hit. For expiration hook to be called, the next param 'pending_node_expiration_ttl_in_secs' must be
        defined. Defining expiration hook without an expiration TTL is not allowed. Each callback/hook can either be pure Python
        Callable or a Callable wrapped by InlinedCompute type. RheocerOS provides interfaces for each hook type. Please
        see the internal types from class <RoutingHookInterface.PendingNode>: <IPendingNodeCreationHook>, <IPendingNodeExpirationHook>,
        <IPendingCheckpointHook>
        :param pending_node_expiration_ttl_in_secs: Determine how long the system should keep track of a pending event trigger
        group. For example: an event was received a week ago on a particular dimension values (e.g date partition), but for the
        other inputs of your data node, there has been no events so far. This forms a Pending Node and without a TTL RheocerOS
        persists and tracks them forever until routing data reset (incompatible update), terminate or internal error occurs.
        :param auto_input_dim_linking_enabled: Enables the convenience functionality to link inputs to each other over
        same 'dimensions'. Unique dimensions are still left unlinked.
        :param auto_output_dim_linking_enabled: Enables the convenience functionality to link output dimensions to any
        of the inputs based on the assumption of dimension name equality.
        :param auto_backfilling_enabled: TODO
        :param protocol: completition protocol for the output. default value if "_SUCCESS" file based pritimitive
        protocol (also used by Hadoop, etc).
        :param kwargs: Provide metadata. Format and content are up to the client and they are guaranteed to be preserved.

        :return: A new MarshalerNode that encapsulates the internal data on the client side. Returned value can be used
        as an input to many other Application APIs in a convenient way. Most important of them is again this same API
        'create_data' which can use MarshalerNode and its filtered version (FilteredView) as inputs.
        """

        if self.get_data(id, Application.QueryApplicationScope.CURRENT_APP_ONLY, Application.QueryContext.DEV_CONTEXT):
            raise ValueError(
                f"DataNode with id {id!r} already exists!" f"You might want to use 'update_data' API to modify an existing data node."
            )

        marshaler_node = self._create_data_node(
            id,
            inputs,
            input_dim_links,
            output_dimension_spec,
            output_dim_links,
            compute_targets,
            execution_hook,
            pending_node_hook,
            pending_node_expiration_ttl_in_secs,
            auto_input_dim_linking_enabled,
            auto_output_dim_linking_enabled,
            auto_backfilling_enabled,
            protocol,
            **kwargs,
        )
        data_node = marshaler_node.bound
        new_inst: Instruction = self._build_instruction(
            "create_data",
            self._id,
            data_node.signal_link_node.signals,  # input signals
            marshaler_node,
            data_node,
            id,
            inputs,
            input_dim_links,
            output_dimension_spec,
            output_dim_links,
            compute_targets,
            execution_hook,
            pending_node_hook,
            pending_node_expiration_ttl_in_secs,
            auto_input_dim_linking_enabled,
            auto_output_dim_linking_enabled,
            auto_backfilling_enabled,
            protocol,
            **kwargs,
        )
        self._dev_context.add_instruction(new_inst)
        return marshaler_node

    def _create_data_node(
        self,
        id: str,
        inputs: Union[List[Union[FilteredView, MarshalerNode]], Dict[str, Union[FilteredView, MarshalerNode]]],
        input_dim_links: Sequence[Tuple[SignalDimensionTuple, DimensionVariantMapFunc, SignalDimensionTuple]],
        output_dimension_spec: Union[Dict[str, Any], DimensionSpec],
        output_dim_links: Optional[
            Sequence[
                Tuple[
                    Union[OutputDimensionNameType, SignalDimensionTuple],
                    DimensionVariantMapFunc,
                    Union[OutputDimensionNameType, Tuple[OutputDimensionNameType, ...], SignalDimensionTuple],
                ]
            ]
        ],
        compute_targets: Sequence[ComputeDescriptor],
        execution_hook: RouteExecutionHook = None,
        pending_node_hook: RoutePendingNodeHook = None,
        pending_node_expiration_ttl_in_secs: int = None,
        auto_input_dim_linking_enabled=True,
        auto_output_dim_linking_enabled=True,
        auto_backfilling_enabled=False,
        protocol: SignalIntegrityProtocol = InternalDataNode.DEFAULT_DATA_COMPLETION_PROTOCOL,
        **kwargs,
    ) -> MarshalerNode:
        if output_dimension_spec and not isinstance(output_dimension_spec, (DimensionSpec, dict)):
            raise ValueError(
                f"Wrong type {output_dimension_spec!r} for 'output_dimension_spec'! It should be of type DimensionSpec or Dict[str, Any]."
            )

        output_dim_spec: DimensionSpec = (
            DimensionSpec.load_from_pretty(output_dimension_spec)
            if not isinstance(output_dimension_spec, DimensionSpec)
            else output_dimension_spec
        )

        input_signals: List[Signal] = []
        if inputs:
            if isinstance(inputs, Dict):
                input_signals = [self._get_input_signal(filtered_view, alias) for alias, filtered_view in inputs.items()]
            else:
                input_signals = [self._get_input_signal(filtered_view) for filtered_view in inputs]

            input_signals = self._check_upstream(input_signals)
        else:
            input_signals = [
                # tether this new dangling node to the system so that it can be controlled via execute or process APIs
                # for example.
                Signal.ground(id, output_dim_spec)
            ]
            # enforce, since ground already adapts output's spec. the rest of the machinery relies on this.
            auto_output_dim_linking_enabled = True

        signal_link_node = SignalLinkNode(input_signals)

        for input_dim_link in input_dim_links:
            func = input_dim_link[1] if input_dim_link[1] else DIMENSION_VARIANT_IDENTICAL_MAP_FUNC
            signal_link_node.add_link(
                SignalDimensionLink(self._check_upstream(input_dim_link[0]), func, self._check_upstream(input_dim_link[2]))
            )
            """
            # check if func is no-op / identical map
            if func(input_dim_link[2]) is input_dim_link[2]:
                # swap the direction for user convenience
                # even if this declaration exists, it should not upset core routing module.
                signal_link_node.add_link(SignalDimensionLink(input_dim_link[2], func, input_dim_link[0]))
            """

        if auto_input_dim_linking_enabled:
            # compensate the rest of the dimension link matrix for inputs based on the assumption that
            # they are left off due to trivial dimensional equality.
            # automatically create the links for inputs that have identical dimensions (if not provided by user already).
            signal_link_node.compensate_missing_links()

        # if a link has its first (DESTINATION/TARGET) entry as SignalDimensionTuple, then it is an assignment from
        #  OUTPUT to an INPUT, and that SignalDimensionTuple belongs to an input. If it is a string literal (it represents
        #  an output dimension).
        output_dim_link_matrix: DimensionLinkMatrix = list(
            map(
                lambda link: SignalDimensionLink(
                    SignalDimensionTuple(None, Dimension(link[0], None)) if not isinstance(link[0], SignalDimensionTuple) else link[0],
                    link[1] if link[1] else DIMENSION_VARIANT_IDENTICAL_MAP_FUNC,
                    self._check_upstream(link[2]) if isinstance(link[2], SignalDimensionTuple)
                    # literal value assignment, use the same dimension:name to create a variant/value
                    else SignalDimensionTuple(None, DimensionVariantFactory.create_variant(link[2], {Dimension.NAME_FIELD_ID: link[0]}))
                    # when link[0] is not str (<OutputDimensionNameType>), then it is output -> input link assignment,
                    # so on the right hand side either one output dimension or a tuple of output dimensions:
                    #   Union[OutputDimensionNameType, Tuple[OutputDimensionNameType]]
                    if isinstance(link[0], OutputDimensionNameType)
                    else SignalDimensionTuple(None, *link[2])
                    if isinstance(link[2], (tuple, list))
                    else SignalDimensionTuple(None, link[2]),
                ),
                output_dim_links,
            )
        )

        if output_dim_spec:
            if not auto_output_dim_linking_enabled:
                if {key for key in output_dim_spec.get_flattened_dimension_map().keys()} != set(
                    [link.lhs_dim.dimensions[0].name for link in output_dim_link_matrix]
                ):
                    raise ValueError(
                        f"Please define output link matrix for all of the dimensions specified in the spec."
                        f" data_id={id}, spec: {output_dim_spec!r}, output_link_matrix: {output_dim_link_matrix!r}"
                    )
            else:
                # try to find it from the inputs
                for missing_dim_name in {key for key in output_dim_spec.get_flattened_dimension_map().keys()} - set(
                    [link.lhs_dim.dimensions[0].name for link in output_dim_link_matrix]
                ):
                    linked_signal_dimension_tuple = None
                    for input_signal in input_signals:
                        linked_candidate = input_signal.clone(None)
                        source_dimension = linked_candidate.domain_spec.dimension_spec.find_dimension_by_name(missing_dim_name)
                        source_dimension_value = linked_candidate.domain_spec.dimension_filter_spec.find_dimension_by_name(missing_dim_name)
                        # we allow mappings from dependent signals since the actual problem should be captured by
                        # cyclic dependency check. e.g output dim -> dependent dim -> output dim
                        # so this check is meaningless to avoid zombie nodes and also counter-productive.
                        # see the TODO at the end of this method
                        if source_dimension:  # and \
                            # (not input_signal.is_dependent or source_dimension_value.is_material_value()):
                            linked_signal_dimension_tuple = SignalDimensionTuple(linked_candidate, source_dimension)
                            break

                    if not linked_signal_dimension_tuple:
                        raise ValueError(
                            f"Please define output link matrix for all of the dimensions specified in the spec."
                            f" Cannot link output dimension {missing_dim_name!r} to any of the independent (non-ref, no nearest) inputs."
                            f" data_id={id}, spec: {output_dim_spec!r}, output_link_matrix: {output_dim_link_matrix!r}"
                        )

                    output_dim_link_matrix.append(
                        SignalDimensionLink(
                            SignalDimensionTuple(None, Dimension(missing_dim_name, None)),
                            DIMENSION_VARIANT_IDENTICAL_MAP_FUNC,
                            self._check_upstream(linked_signal_dimension_tuple),
                        )
                    )

        kwargs.update({SignalSourceAccessSpec.OWNER_CONTEXT_UUID: self.uuid})

        # allow compute_targets to be parametrized (platform aware)
        for compute_target in compute_targets:
            compute_target.parametrize(self.platform)

        # ask each compute target to provide compute specific (mandated) output parameters and check them against
        # user provided attrs (if any). E.g dataset header
        for compute_target in compute_targets:
            compute_attrs: Optional[Dict[str, Any]] = compute_target.create_output_attributes(self.platform, kwargs)
            if compute_attrs:
                kwargs.update(compute_attrs)

        # some of the callbacks within the hooks might be custom targets implementing ComputeDescriptor
        # they need the same parametrization call with the platform object. this is architecturally the best location
        # to make that call (Application object binding platform to those low level entities during development).
        for hook in (execution_hook, pending_node_hook):
            if hook:
                for callback in hook.callbacks():
                    if isinstance(callback, ComputeDescriptor):
                        callback.parametrize(self.platform)

        data_node = InternalDataNode(
            id,
            signal_link_node,
            output_dim_spec,
            output_dim_link_matrix,
            compute_targets,
            execution_hook,
            pending_node_hook,
            pending_node_expiration_ttl_in_secs,
            auto_backfilling_enabled,
            protocol,
            **kwargs,
        )

        # TODO implement cyclic dependency check for the entire graph of inputs and the output
        # signal_link_node.check_dependency_graph(data_node.signal(), data_node.output_dim_matrix)
        # make check_dangling_dependents private and move it inside check_dependency_graph
        signal_link_node.check_dangling_dependents(data_node.signal(), data_node.output_dim_matrix)

        marshaler_node = MarshalerNode(data_node, None)
        data_node.add_child(marshaler_node)

        return marshaler_node

    def update_data(
        self,
        id: str,
        inputs: Union[List[Union[FilteredView, MarshalerNode]], Dict[str, Union[FilteredView, MarshalerNode]]],
        input_dim_links: Sequence[Tuple[SignalDimensionTuple, DimensionVariantMapFunc, SignalDimensionTuple]],
        output_dimension_spec: Union[Dict[str, Any], DimensionSpec],
        output_dim_links: Optional[
            Sequence[
                Tuple[
                    Union[OutputDimensionNameType, SignalDimensionTuple],
                    DimensionVariantMapFunc,
                    Union[OutputDimensionNameType, Tuple[OutputDimensionNameType, ...], SignalDimensionTuple],
                ]
            ]
        ],
        compute_targets: Sequence[ComputeDescriptor],
        execution_hook: RouteExecutionHook = None,
        pending_node_hook: RoutePendingNodeHook = None,
        pending_node_expiration_ttl_in_secs: int = None,
        auto_input_dim_linking_enabled=True,
        auto_output_dim_linking_enabled=True,
        auto_backfilling_enabled=False,
        protocol: SignalIntegrityProtocol = InternalDataNode.DEFAULT_DATA_COMPLETION_PROTOCOL,
        enforce_referential_integrity=True,
        **kwargs,
    ) -> MarshalerNode:
        """Update an existing node if its signalling properties are going to stay intact or if the node does not have
         any dependent nodes. So if the signalling properties of the node is still the same, then whether the node has
         dependent nodes or not is ignored and update is always allowed. However, if dependent node update is required,
         RheocerOS raises an exception while 'enforce_referential_integrity' is True. By setting that flag to False,
         you can force an update to go through but in that case the only way for you to activate the application is
         to satisfy the update requirement on the dependent nodes.

        Changing the signalling properties usually means a change in the output dimension spec, dimension filter,
        integrity protocol and output_dim_links which along with 'inputs' determine the signal representation of this
        node. This signal representation might be an input to a dependent node.

        If the change won't effect the signaling properties, then even no warning is emitted and update can be considered
        to be seamless.
        """
        data_nodes: List[MarshalerNode] = self.get_data(
            id, Application.QueryApplicationScope.CURRENT_APP_ONLY, Application.QueryContext.DEV_CONTEXT
        )
        if not data_nodes:
            raise ValueError(f"Cannot update a non-existent data node {id!r}!")

        if len(data_nodes) > 1:
            raise RuntimeError(f"There are more than one data node with ID {id!r}! Application dev context corrupted.")

        inst_index: int = self._dev_context.get_instruction_index(data_nodes[0])
        inst: Instruction = self._dev_context.instruction_chain[inst_index]
        dependent_instructions = inst.outbound

        marshaler_node = self._create_data_node(
            id,
            inputs,
            input_dim_links,
            output_dimension_spec,
            output_dim_links,
            compute_targets,
            execution_hook,
            pending_node_hook,
            pending_node_expiration_ttl_in_secs,
            auto_input_dim_linking_enabled,
            auto_output_dim_linking_enabled,
            auto_backfilling_enabled,
            protocol,
            **kwargs,
        )

        if dependent_instructions:
            prev_output = cast(InternalDataNode, data_nodes[0].bound).signal()
            new_output = cast(InternalDataNode, marshaler_node.bound).signal()
            if not prev_output.check_integrity(new_output):
                dependents = [dep_links[0].instruction.output_node.signal().alias for dep_links in dependent_instructions.values()]
                if enforce_referential_integrity:
                    ref_error_msg = (
                        f"Referential Integrity Error! Cannot update data node with dependencies "
                        f"{dependents}. "
                        f"If you still want to proceed "
                        f"with the update, then set 'enforce_referential_integrity=False' and then update "
                        f"those dependent nodes as well."
                    )
                    logger.error(ref_error_msg)
                    raise ValueError(ref_error_msg)
                else:
                    logger.warning(
                        f"You are updating/replacing an intermediate node on which the following "
                        f"{len(dependent_instructions)} nodes might be depending on: {dependents}"
                    )
                    logger.warning(
                        f"Please you must update them as well, because this change impacts the signalling"
                        f" aspect of the node (dimension spec, filtering spec, integrity protocol, etc)."
                    )
                    # after this warning, if the user does not satisfy the referential integrity (update the dependents
                    # as well), then activation will detect the issue and fail early
                    # (see Context::check_referential_integrity).

        # now it is safe to do the update
        removed_index = self._dev_context.remove_instruction(data_nodes[0])
        new_inst: Instruction = self._build_instruction(
            "update_data",
            self._id,
            marshaler_node.bound.signal_link_node.signals,  # input signals
            marshaler_node,
            marshaler_node.bound,
            id,
            inputs,
            input_dim_links,
            output_dimension_spec,
            output_dim_links,
            compute_targets,
            execution_hook,
            pending_node_hook,
            pending_node_expiration_ttl_in_secs,
            auto_input_dim_linking_enabled,
            auto_output_dim_linking_enabled,
            auto_backfilling_enabled,
            protocol,
            **kwargs,
        )
        self._dev_context.insert_instruction(removed_index, new_inst)
        return marshaler_node

    def patch_data(
        self,
        id_or_node: Union[str, MarshalingView, MarshalerNode],
        inputs: Optional[Union[List[Union[FilteredView, MarshalerNode]], Dict[str, Union[FilteredView, MarshalerNode]]]] = None,
        input_dim_links: Optional[Sequence[Tuple[SignalDimensionTuple, DimensionVariantMapFunc, SignalDimensionTuple]]] = None,
        output_dimension_spec: Optional[Union[Dict[str, Any], DimensionSpec]] = None,
        output_dim_links: Optional[
            Sequence[
                Tuple[
                    Union[OutputDimensionNameType, SignalDimensionTuple],
                    DimensionVariantMapFunc,
                    Union[OutputDimensionNameType, Tuple[OutputDimensionNameType, ...], SignalDimensionTuple],
                ]
            ]
        ] = None,
        compute_targets: Optional[Sequence[ComputeDescriptor]] = None,
        execution_hook: Optional[RouteExecutionHook] = None,
        pending_node_hook: Optional[RoutePendingNodeHook] = None,
        pending_node_expiration_ttl_in_secs: Optional[int] = None,
        auto_input_dim_linking_enabled: bool = True,
        auto_output_dim_linking_enabled: bool = True,
        auto_backfilling_enabled: bool = False,
        protocol: SignalIntegrityProtocol = InternalDataNode.DEFAULT_DATA_COMPLETION_PROTOCOL,
        enforce_referential_integrity=False,
        **kwargs,
    ) -> MarshalerNode:
        """Patch any attribute of an existing data node. The rest of the node definition will stay intact. So this API
        allows partial, more flexible updates on a node.
        This is a convenience API mostly useful in scenarios where application code/topology (in dev context) needs to
        be modified and activated into a new stack for purposes such as easy testing via localization of compute
        (with NOOPCompute, RandomTimedNOOPCompute, etc), temporarily disabling system integrations.
        This API implicitly uses Application::update_data so the underlying behaviour to enforce referential integrity
        during this update operation is controlled by the parameter 'enforce_referential_integrity'.
        But differently due to the nature of most common scenarios where this convenience API is supposed to be used,
        'enforce_referential_integrity' parameter is set to False.
        """
        id: str = None
        if isinstance(id_or_node, str):
            id = id_or_node
        elif isinstance(id_or_node, MarshalingView):
            id = id_or_node.marshaler_node.signal().alias
        elif isinstance(id_or_node, MarshalerNode):
            id = id_or_node.signal().alias
        else:
            raise ValueError(f"Wrong input type {type(id_or_node)} for Application::patch_data(Union[str, MarshalingView, MarshalerNode])!")

        data_nodes: List[MarshalerNode] = self.get_data(
            id, Application.QueryApplicationScope.CURRENT_APP_ONLY, Application.QueryContext.DEV_CONTEXT
        )
        if not data_nodes:
            raise ValueError(f"Cannot patch a non-existent data node {id!r}!")

        if len(data_nodes) > 1:
            raise RuntimeError(f"There are more than one data node with ID {id!r}! Application dev context corrupted.")

        instructions: List[Instruction] = self._dev_context.get_instructions(data_nodes[0])
        latest_inst: Instruction = instructions[-1:][0]
        patched_kwargs = dict(latest_inst.kwargs)
        patched_kwargs.update(kwargs)
        return self.update_data(
            id,
            latest_inst.args[1] if inputs is None else inputs,
            latest_inst.args[2] if input_dim_links is None else input_dim_links,
            latest_inst.args[3] if output_dimension_spec is None else output_dimension_spec,
            latest_inst.args[4] if output_dim_links is None else output_dim_links,
            latest_inst.args[5] if compute_targets is None else compute_targets,
            latest_inst.args[6] if execution_hook is None else execution_hook,
            latest_inst.args[7] if pending_node_hook is None else pending_node_hook,
            latest_inst.args[8] if pending_node_expiration_ttl_in_secs is None else pending_node_expiration_ttl_in_secs,
            latest_inst.args[9] if auto_input_dim_linking_enabled is None else auto_input_dim_linking_enabled,
            latest_inst.args[10] if auto_output_dim_linking_enabled is None else auto_output_dim_linking_enabled,
            latest_inst.args[11] if auto_backfilling_enabled is None else auto_backfilling_enabled,
            latest_inst.args[12] if protocol is None else protocol,
            enforce_referential_integrity,
            **patched_kwargs,
        )

    def get_data(
        self,
        data_id: str,
        app_scope: QueryApplicationScope = QueryApplicationScope.ALL,
        context: QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT,
    ) -> List[MarshalerNode]:
        return [
            child_node
            for data_node in self.query([DataNode.QueryVisitor(data_id, exact_match=True)], app_scope, context).values()
            for child_node in data_node.child_nodes
            if isinstance(child_node, MarshalerNode)
        ]
        """
        data_marshalers: List[MarshalerNode] = []
        for data_node in self.query([DataNode.QueryVisitor(data_id)], app_scope, context).values():
            for child_node in data_node.child_nodes:
               if isinstance(child_node, MarshalerNode):
                   data_marshalers.append(child_node)

        return data_marshalers
        """

    def get_upstream_data(self, data_id: str, context: QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT) -> List[MarshalerNode]:
        return self.get_data(data_id, Application.QueryApplicationScope.EXTERNAL_APPS_ONLY, context)

    def list(
        self,
        node_type: Type[Node] = MarshalerNode,
        app_scope: QueryApplicationScope = QueryApplicationScope.ALL,
        context: QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT,
    ) -> Iterable[Node]:
        return self.query([Node.QueryVisitor(node_type)], app_scope, context).values()

    def list_data(
        self, app_scope: QueryApplicationScope = QueryApplicationScope.ALL, context: QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT
    ) -> Sequence[MarshalerNode]:
        result: List[MarshalerNode] = []
        for data_node in self.list(DataNode, app_scope, context):
            for child_node in data_node.child_nodes:
                if isinstance(child_node, MarshalerNode):
                    result.append(child_node)
        return result

    def query(
        self,
        query_visitors: Sequence[Node.QueryVisitor],
        app_scope: QueryApplicationScope = QueryApplicationScope.ALL,
        context: QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT,
    ) -> Mapping[str, Node]:
        for visitor in query_visitors:
            if app_scope in [Application.QueryApplicationScope.ALL, Application.QueryApplicationScope.CURRENT_APP_ONLY]:
                if context in [Application.QueryContext.ALL, Application.QueryContext.DEV_CONTEXT]:
                    self._dev_context.accept(visitor)
                if context in [Application.QueryContext.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT]:
                    if self._active_context:
                        self._active_context.accept(visitor)

        if app_scope in [Application.QueryApplicationScope.ALL, Application.QueryApplicationScope.EXTERNAL_APPS_ONLY]:
            if context in [Application.QueryContext.ALL, Application.QueryContext.DEV_CONTEXT]:
                for external_app in self._dev_context.external_data:
                    external_app.query(query_visitors)
            if context in [Application.QueryContext.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT]:
                if self._active_context:
                    for external_app in self._active_context.external_data:
                        external_app.query(query_visitors)

        query_result: Dict[str, Node] = dict()
        for query_visitor in query_visitors:
            query_result.update(query_visitor.results)

        return query_result

    def query_data(
        self,
        query: str,
        app_scope: QueryApplicationScope = QueryApplicationScope.ALL,
        context: QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT,
    ) -> Mapping[str, MarshalerNode]:
        result: Dict[str, MarshalerNode] = dict()
        for data_node in self.query([DataNode.QueryVisitor(query)], app_scope, context).values():
            for child_node in data_node.child_nodes:
                if isinstance(child_node, MarshalerNode):
                    result[data_node.data_id] = child_node
        return result

    def search_data(
        self,
        keyword: str,
        app_scope: QueryApplicationScope = QueryApplicationScope.ALL,
        context: QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT,
    ) -> Mapping[str, MarshalerNode]:
        result: Dict[str, MarshalerNode] = dict()
        all_data = self.list_data(app_scope, context)
        keyword_lower = keyword.lower()
        for data in all_data:
            if keyword_lower in repr(data.access_spec()).lower() or keyword_lower in data._id.lower():
                result[data._id] = data
        return result

    def ground(
        self,
        input: Union[Signal, Dict[str, Any], FilteredView, MarshalerNode],
        with_activated_processor=False,
        processing_mode=FeedBackSignalProcessingMode.ONLY_HEAD,
        target_route_id: Optional[Union[RouteID, MarshalerNode]] = None,
        is_async=True,
    ) -> Optional[List["RoutingTable.Response"]]:
        """Implicitly calls the process on the ground signal for the input's connection with the system.

        Input should be a dangling node (with no dependencies), otherwise the effect is NOOP.

        So the logical effect is to feed/input the ground signal into the system and satisfy the input condition of
        'input' signal/node and possibly cause execution on it. So this is different than calling process on the same
        input in which case 'input' itself is injected into the system and the nodes relying on it (as an input)
        will be satisfied or possibly executed.

            equivalent to:

                Application::process( GROUND of 'input')

        Ultimate effect is quite similar to execute but this call does not wait or monitor the resulted execution
        (if any).

        Return type is same as <Application::process>.
        """
        if isinstance(input, (FilteredView, SignalProvider, MarshalerNode)):
            checked_input = Signal.ground(self._check_upstream_signal(self._get_input_signal(input)))
        else:
            checked_input = Signal.ground(input if not isinstance(input, Signal) else self._check_upstream_signal(input))

        return self.process(checked_input, with_activated_processor, processing_mode, target_route_id, is_async)

    def process(
        self,
        input: Union[Signal, Dict[str, Any], FilteredView, MarshalerNode],
        with_activated_processor=False,
        processing_mode=FeedBackSignalProcessingMode.ONLY_HEAD,
        target_route_id: Optional[Union[RouteID, MarshalerNode]] = None,
        is_async=True,
    ) -> Optional[List["RoutingTable.Response"]]:
        """Injects a new signal or raw event into the system.

        Signal or event is either processed locally or remotely in the activated resource (depending on the driver).

        :param input: A node or a filtered view or a signal/raw event supported by the
        underlying Processor driver. Most common scenario is to use the data/model/timer node references as a input
        to this API (as 'MarshalerNode' if no dimension exists or a filtered view [with material dimension values]).
        Signal/raw event mode is mostly used in tests or during manual injections.
        :param with_activated_processor: if it is True, this API makes a remote call to the activated instances/resource
        created by the underlying Processor impl (sub-class). If it is False (default), this call is handled within the
        same Python process domain (which is the preferred case for unit-tests and more commonly for local debugging).
        :param processing_mode: If the input signal represents multiple resources (e.g dataset with a range of partitions),
        then this flag represents which one to use as the actual event/impetus into the system. ONLY_HEAD makes the tip
        of the range to be used (e.g most recent partition on a 'date' range). FULL RANGE causes a Signal explosion and
        multiple Processor cycles implicitly, where each atomic resource (e.g data partition) is treated as a separate
        Signal.
        :param target_route_id: When the signal is injected and being checked against multiple routes, this parameter
        limits the execution to a specific one only, otherwise (by default) the system uses any incoming signal/event
        in multicast mode against all of the existing Routes. For example, if an incoming event can trigger multiple
        routes and you want to limit this behaviour for more deterministic tests, etc then use this parameter to
        specify a route and limit the execution scope.
        :param is_async: when 'with_activated_processor' is True, then this parameter can be used to control whether
        the remote call will be async or not.
        """
        if with_activated_processor and self.state != ApplicationState.ACTIVE:
            raise RuntimeError(
                f"Cannot call remote processor unless the application state is {ApplicationState.ACTIVE!r}!"
                f" Current state: {self.state!r}"
            )
        elif not with_activated_processor and self.state not in [ApplicationState.ACTIVE, ApplicationState.PAUSED]:
            raise RuntimeError(
                f"Cannot call processor unless the application is in "
                f" any of {[ApplicationState.ACTIVE, ApplicationState.PAUSED]!r} states!"
                f" Current state: {self.state!r}"
            )

        if isinstance(input, (FilteredView, SignalProvider, MarshalerNode)):
            checked_input = self._check_upstream_signal(self._get_input_signal(input))
        else:
            checked_input = input if not isinstance(input, Signal) else self._check_upstream_signal(input)

        return self._platform.processor.process(
            checked_input,
            with_activated_processor,
            processing_mode,
            target_route_id if isinstance(target_route_id, RouteID) else self._get_route_id(target_route_id),
            is_async,
        )

    def _get_route_id(self, route: Union[str, MarshalerNode, "Route"]) -> RouteID:
        route_id: RouteID = None
        if isinstance(route, str):
            routes = self.get_data(route)
            if not routes:
                raise ValueError(f"Node/Route with ID {route!r} could not be found!")
            route_id = routes[0].bound.route_id
        elif isinstance(route, Route):
            route_id = route.route_id
        elif isinstance(route, MarshalerNode):
            route_id = cast(MarshalerNode, route).bound.route_id
        return route_id

    def get_active_routes(self) -> List[RoutingTable.RouteRecord]:
        # TODO use generator and return Iterator
        return [record for record in self.platform.routing_table.load_active_route_records() if record.active_compute_records]

    def get_active_route(self, route: Union[str, MarshalerNode, "Route"]) -> RoutingTable.RouteRecord:
        """Optimistically read the data (does not use routing_table synch mechanism, so susceptible to concurrency issues).

        Can throw
        """
        route_id: RouteID = self._get_route_id(route)
        return self.platform.routing_table.optimistic_read(route_id)

    def has_active_routes(self) -> bool:
        for record in self.platform.routing_table.load_active_route_records():
            if record.active_compute_records:
                return True
        return False

    def update_active_routes_status(self) -> None:
        self.platform.routing_table.check_active_routes()

    def update_active_route_status(self, route: Union[str, MarshalerNode, "Route"]) -> None:
        route_id: RouteID = self._get_route_id(route)
        self.platform.routing_table.check_active_route(route_id)

    def _is_data_active(self, internal_data_id: str) -> bool:
        """Checks whether the internal data has been as part of the application during the most recent activation.

        It is mainly used by Application::poll and Application::execute APIs.
        """
        return bool(
            self.get_data(
                internal_data_id, Application.QueryApplicationScope.CURRENT_APP_ONLY, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT
            )
        )

    def _is_data_updated(self, internal_data_id: str) -> bool:
        """Checks whether the internal data has been just created or updated during this development session.

        It is mainly used by Application::poll and Application::execute APIs.
        """
        return bool(
            self.get_data(internal_data_id, Application.QueryApplicationScope.CURRENT_APP_ONLY, Application.QueryContext.DEV_CONTEXT)
        )

    def poll(
        self, output: Union[MarshalingView, MarshalerNode], datum: Optional[Union["datetime", int]] = None
    ) -> Tuple[Optional[str], Optional[List[RoutingTable.ComputeRecord]]]:
        """Checks if there is any active computes for the target output and then waits for the completion of it.

        If there is no active records for it, then this method checks the inactive (historical) compute records for the
        output.

        Also see Application::execute and Application::materialize APIs. This API differs from Application::execute in a
        way that it does not attempt to cause any new executions on the target node. It basically guarantees to
        check if the output has ever been created/updated by this application and (if so) whether it was successful
        in the most recent execution.

        :param output: Materialized view of an internal data node (it can also be upstream).
        :param datum: optional datetime to specify start/threshold time to check compute records of executed node.
        can be of type datetime or int representing timestamp in UTC.
        :return: A tuple of the materialized path of the output and the compute record objects for the most recent execution.
        If the compute record belongs to a failed execution, then the first element of the tuple (path) is None.
        if there has been no executions on the node, then the returned value is None.
        Materialized path represents full physical path of the target output, which should be compatible with other
        frameworks (such as Pandas, Spark, boto3) to do further retrieval, analysis.
        """
        node: MarshalerNode = None
        if isinstance(output, MarshalingView):
            node = output.marshaler_node
        elif isinstance(output, MarshalerNode):
            node = output
        else:
            logger.error(f"Please provide a data node (if it is dimensionless) or filtered material version of it.")
            raise ValueError(
                f"Wrong input type {type(output)} for Application::poll(Union[MarshalingView, MarshalerNode], datum: Optional[Union[datetime, int]])."
            )

        if not isinstance(node.bound, InternalDataNode):
            # TODO we can now support external nodes as well
            raise ValueError(f"Can only poll internal data nodes!")

        # at this point we don't know whether it is an upstream data node or not.
        internal_data_node = cast(InternalDataNode, node.bound)
        materialized_output: Signal = self._get_input_signal(output)
        # TODO support node declarations with RelativeVariants which would have multipled branches in their filter spec
        if not materialized_output.domain_spec.dimension_filter_spec.is_material():
            logger.error(f"Input data node {internal_data_node.data_id!r} to Application::poll does not have" f" a materialized view!")
            logger.error(f"Dimension filter: {materialized_output.domain_spec.dimension_filter_spec!r}")
            logger.error("Please use concrete dimension values for the input.")
            raise ValueError(f"Can poll on materialized data node views only!")

        platform = self.platform
        owner_context_uuid = internal_data_node.signal().resource_access_spec.get_owner_context_uuid()
        if owner_context_uuid != self.uuid:
            platform = self._dev_context.get_upstream_app(owner_context_uuid).platform
            # if the node is upstream, it should already belong to an activated upstream app.
        else:
            is_route_new = not self._is_data_active(internal_data_node.data_id)

            if is_route_new:
                raise ValueError(
                    f"Cannot poll input data node {internal_data_node.data_id!r} since"
                    f" the application has not been activated with this new data node yet."
                )
            else:
                if self._is_data_updated(internal_data_node.data_id):
                    logger.warning(
                        f"Polling an updated data node {internal_data_node.data_id!r}"
                        f" without activation might yield unexpected results! Polling result"
                        f" (if any) might be safe to use if dimension spec is still consistent"
                        f" with the previous version. Similarly it might not be able to"
                        f" detect completions for ongoing/active executions or inactive ones."
                    )
        if datum is not None:
            from datetime import datetime

            if isinstance(datum, datetime):
                datum = int(datum.timestamp())
            elif not isinstance(datum, int):
                raise ValueError("please provide a value that is either datetime or an int value that can be converted into a datetime")

            if datetime(1970, 1, 1).timestamp() >= datum or datum > int(datetime.utcnow().timestamp()):
                # datum must be greater than the start of utc and also less than the current day.
                raise ValueError(
                    f"""
                                 Cannot poll {internal_data_node.data_id!r}!
                                 The datum parameter "{datum}" is out of the required datetime range.
                                 Must be greater than:{datetime(1970, 1, 1)!r}! and must be less than (NOW):{datetime.utcnow()!r}
                                 """
                )

        logger.critical(f"Polling {materialized_output.get_materialized_resource_paths()[0]!r}")
        # now we can start actual polling.
        # begin with active records (anything ongoing now?)
        active_executions = set()
        min_trigger_timestamp_utc = None
        while True:
            route_record = platform.routing_table.optimistic_read(internal_data_node.route_id)
            if route_record and route_record.has_active_record_for(materialized_output):
                active_records = route_record.get_active_records_of(materialized_output)
                active_executions.update([r.execution_context_id for r in active_records])
                min_timestamp = min([r.trigger_timestamp_utc for r in active_records])
                min_trigger_timestamp_utc = (
                    min_timestamp
                    if min_trigger_timestamp_utc is None or min_timestamp < min_trigger_timestamp_utc
                    else min_trigger_timestamp_utc
                )
                logger.critical(f"Data node {internal_data_node.data_id!r} has active execution(s) on it now.")
                logger.critical(
                    f"At least one of those executions is working on the same output: {materialized_output.get_materialized_resource_paths()[0]}"
                )
                logger.critical(f"Active compute sessions: {[record.session_state for record in active_records]!r}.")
                logger.critical(f"Will wait for it to complete first. Will check in 30 seconds...")
                time.sleep(30)
            else:
                break

        if active_executions:
            logger.critical("Active executions are complete! Will check the status...")

        # find the most recent execution on the target output and check its status
        most_recent_execution = None
        completed_compute_records = []
        while True:
            inactive_records = platform.routing_table.load_inactive_compute_records(internal_data_node.route_id, ascending=False)
            logger.critical(f"Checking the result of compute records:")
            if inactive_records:
                active_to_inactive_eventual_consistency = False
                for inactive_record in inactive_records:
                    if active_executions and inactive_record.trigger_timestamp_utc < min_trigger_timestamp_utc:
                        if not most_recent_execution:
                            logger.critical(
                                f"Most recent execution is yet to be found in inactive records. "
                                f"Will sleep for 10 seconds for eventual consistency."
                            )
                            time.sleep(10)
                            # re-scan from the tip of historical records
                            active_to_inactive_eventual_consistency = True
                        else:
                            # finish/exit the scan successfully if previous iterations hit the target compute records.
                            logger.critical(f"Completed scanning inactive records for {most_recent_execution!r}.")
                            logger.critical(
                                f"Inactive compute sessions: {[record.session_state for record in completed_compute_records]!r}."
                            )
                        break

                    # kind of 'group by' compute records by execution_id and also check the output
                    if (
                        (datum is None or inactive_record.trigger_timestamp_utc >= datum)
                        and (most_recent_execution is None or most_recent_execution == inactive_record.execution_context_id)
                        and DimensionFilter.check_equivalence(
                            inactive_record.materialized_output.domain_spec.dimension_filter_spec,
                            materialized_output.domain_spec.dimension_filter_spec,
                        )
                    ):
                        # found a compute record that worked on the same output
                        if inactive_record.session_state:
                            if inactive_record.session_state.state_type == ComputeSessionStateType.COMPLETED:
                                most_recent_execution = inactive_record.execution_context_id
                                completed_compute_records.append(inactive_record)
                            else:
                                logger.error(
                                    f"During the most recent execution on this output, one of the compute targets could not be completed! "
                                )
                                logger.error(
                                    f" Problematic target: {(inactive_record.slot.type, inactive_record.slot.code_lang, inactive_record.slot.code_abi, inactive_record.slot.extra_params)!r}"
                                )
                                logger.error(f" Initial response state: {(inactive_record.state)!r}")
                                logger.error(f" Session state: {inactive_record.session_state!r}")
                                logger.error(f" Expected output: {materialized_output.get_materialized_resource_paths()[0]}")
                                if inactive_record.session_state.executions:
                                    logger.error(f"Abbreviated list of errors from all of the attempts (retries):")
                                    for exec_details in inactive_record.session_state.executions:
                                        if "ErrorMessage" in exec_details.details:
                                            logger.error(exec_details.details["ErrorMessage"])
                                # we don't care about the state of other sibling compute records that worked on the same input
                                # as part of the same execution. If any of them is failed, then the output is not reliable.
                                return None, [inactive_record]
                        else:
                            logger.error(
                                f"During the most recent execution on this node {internal_data_node.data_id!r}, one of compute targets could not be started!"
                            )
                            logger.error(f" Expected output: {materialized_output.get_materialized_resource_paths()[0]}")
                            logger.error(
                                f" Problematic target: {(inactive_record.slot.type, inactive_record.slot.code_lang, inactive_record.slot.code_abi, inactive_record.slot.extra_params)!r}"
                            )
                            logger.error(f" Initial response state: {(inactive_record.state)!r}")
                            # we don't care about the state of other sibling compute records that worked on the same input
                            # as part of the same execution. If any of them is failed, then the output is not reliable.
                            return None, [inactive_record]
                if not active_to_inactive_eventual_consistency:
                    # if all of the records have been scanned, exit the loop
                    break
            elif active_executions:
                logger.critical(
                    f"Most recent execution is yet to be found in inactive records. " f"Will sleep for 10 seconds for eventual consistency."
                )
                time.sleep(10)
            else:
                # nothing to do, no historical records either. break out of the loop.
                break

        if most_recent_execution:
            logger.critical(f"Most recent execution {most_recent_execution!r} is COMPLETE!")
            # now we are sure that data is ready, we can now return the materialized output signal.
            # it can be used by client to act on the data now.
            # now we should care about whether it is upstream or not (necessary for resource path materialization).
            # (materialized_output at this point has a materialized filter_spec, we need to materialize its resource
            # access spec as well.
            input_signal = self._check_upstream([materialized_output])[0]
            input_signal = self._materialize_internal(input_signal)
            materialized_paths = input_signal.get_materialized_resource_paths()
            logger.critical(f"Returning materialized path: {materialized_paths[0]!r}")
            return materialized_paths[0], completed_compute_records
        return None, None

    def kill(self, output: Union[MarshalingView, MarshalerNode]) -> bool:
        """Checks if there is any active computes for the target output and then attempts to kill them.

        If there is no active records for it, then it simply reports it and returns with no other effect.

        Outcome of this action can be polled/tracked by a subsequent Application::poll call.

        :param output: Materialized view of an internal data node (it can also be upstream).
        :return: True if active executions were found and killed, otherwise returns False
        """
        node: MarshalerNode = None
        if isinstance(output, MarshalingView):
            node = output.marshaler_node
        elif isinstance(output, MarshalerNode):
            node = output
        else:
            logger.error(f"Please provide a data node (if it is dimensionless) or filtered material version of it.")
            raise ValueError(f"Wrong input type {type(output)} for Application::kill(Union[MarshalingView, MarshalerNode]).")

        if not isinstance(node.bound, InternalDataNode):
            raise ValueError(f"Can only kill internal data nodes!")

        # at this point we don't know whether it is an upstream data node or not.
        internal_data_node = cast(InternalDataNode, node.bound)
        materialized_output: Signal = self._get_input_signal(output)
        # TODO support node declarations with RelativeVariants which would have multiple branches in their filter spec
        if not materialized_output.domain_spec.dimension_filter_spec.is_material():
            logger.error(f"Input data node {internal_data_node.data_id!r} to Application::kill does not have" f" a materialized view!")
            logger.error(f"Dimension filter: {materialized_output.domain_spec.dimension_filter_spec!r}")
            logger.error("Please use concrete dimension values for the input.")
            raise ValueError(f"Can kill materialized data node views only!")

        platform = self.platform
        owner_context_uuid = internal_data_node.signal().resource_access_spec.get_owner_context_uuid()
        if owner_context_uuid != self.uuid:
            platform = self._dev_context.get_upstream_app(owner_context_uuid).platform
            # if the node is upstream, it should already belong to an activated upstream app.
        else:
            is_route_new = not self._is_data_active(internal_data_node.data_id)

            if is_route_new:
                raise ValueError(
                    f"Cannot kill input data node {internal_data_node.data_id!r} since"
                    f" the application has not been activated with this new data node yet."
                )
            else:
                if self._is_data_updated(internal_data_node.data_id):
                    logger.warning(
                        f"Killing an updated data node {internal_data_node.data_id!r}"
                        f" without activation might yield unexpected results! Kill action"
                        f" might be safe if dimension spec is still consistent"
                        f" with the previous version. Similarly it might not be able to"
                        f" detect completions for ongoing/active executions or inactive ones."
                    )

        logger.critical(f"Attempting to kill active executions on {materialized_output.get_materialized_resource_paths()[0]!r}")
        # find active records and call kill on each one of them
        active_executions = set()
        route_record = platform.routing_table.optimistic_read(internal_data_node.route_id)
        if route_record and route_record.has_active_record_for(materialized_output):
            active_records = route_record.get_active_records_of(materialized_output)
            # skip the records which did not return SUCCESS to the initial compute call (check initial response 'state')
            active_records = [r for r in active_records if r.state.response_type == ComputeResponseType.SUCCESS]
            active_executions.update([r.execution_context_id for r in active_records])
            if active_records:
                logger.critical(f"Data node {internal_data_node.data_id!r} has active execution(s) on it now.")
                logger.critical(
                    f"At least one of those executions is working on the same output: {materialized_output.get_materialized_resource_paths()[0]}"
                )
                logger.critical(f"Active compute sessions: {[record.session_state for record in active_records]!r}.")
                logger.critical(f"Attempting to kill the active sessions...")
                for active_record in active_records:
                    if active_record.slot.type.is_batch_compute():
                        platform.batch_compute.kill_session(active_record)

        if active_executions:
            logger.critical(
                "Kill signals have been successfully sent into the system. "
                "Please use 'poll' API on the same output to track the final state of the executions."
            )
            return True
        else:
            logger.critical("No active execution on output!")
        return False

    def execute(
        self,
        target: Union[MarshalingView, MarshalerNode],
        material_inputs: Optional[
            Union[Sequence[Union[Signal, FilteredView, MarshalerNode]], Union[Signal, FilteredView, MarshalerNode]]
        ] = None,
        wait: Optional[bool] = True,
    ) -> str:
        """Activates the application and starts an execution only against the input data node using the materialized
        input signals.

        Whole sequence is synchronous and the function ultimately returns the materialized resource path for the
        output if no error is encountered.

        Returned path represents the physical (Pandas, Spark, etc) compatible physical path of the output of the
        specific execution.

        :param target: Either a filtered view or a direct reference of a data node. If this input is materialized and
        it is possible to construct the materialized versions of its inputs (depending on output dimension links, etc)
        then second parameter 'material_inputs' is optional.
        :param material_inputs: Materialized inputs of the input data node ('target'). This parameter can be left empty
        in cases where 'target' (as output) is trivially bound to its inputs over basic dimensional equality and also
        it is materialized. In those cases, RheocerOS can auto-generate the material versions of the inputs.

        :returns a materialized/physical resource path that represents the outcome of this execution on 'target'.

        Also see Application::poll and Application::materialize
        """
        node: MarshalerNode = None
        if isinstance(target, MarshalingView):
            node = target.marshaler_node
        elif isinstance(target, MarshalerNode):
            node = target
        else:
            logger.error(f"Please provide a data node or a filtered material version of it.")
            raise ValueError(f"Wrong input type {type(target)} for Application::execute API.")

        if not isinstance(node.bound, InternalDataNode):
            raise ValueError(f"Can only execute internal data nodes!")

        internal_data_node = cast(InternalDataNode, node.bound)

        owner_context_uuid = internal_data_node.signal().resource_access_spec.get_owner_context_uuid()
        if owner_context_uuid != self.uuid:
            raise ValueError(
                f"Can execute internal nodes owned by this application (id={self.id!r}, uuid={self.uuid!r} only!"
                f" However input node {internal_data_node.data_id!r} is actually owned by another application"
                f" with uuid={owner_context_uuid!r}"
            )

        output: Signal = self._get_input_signal(target)
        auto_generated_material_inputs: Set[Signal] = set()
        if not material_inputs:
            # support happy-path (when output and input signals' dimensions can be mapped trivially).
            # this is the only case when filtering from target (if any) is used. once we get the material_inputs
            # we will still use the runtime_link_node to extract the output filter for the sake of consistency
            # and as a means of full-stack check (compatible) with the runtime behaviour.
            if not output.domain_spec.dimension_filter_spec.is_material():
                raise ValueError(f"Cannot execute target {internal_data_node.data_id!r} without inputs if it is not materialized!")

            # this will raise an exception if the operation is not possible (any of the input dims cannot be mapped
            #  from the output).
            material_inputs = internal_data_node.signal_link_node.get_materialized_inputs_for_output(
                output, internal_data_node.output_dim_matrix  # verified to be materialized already
            )
            auto_generated_material_inputs = set(material_inputs)
        else:
            material_inputs = material_inputs if isinstance(material_inputs, List) else [material_inputs]
            material_input_signals = [
                self._get_input_signal(input) if not isinstance(input, Signal) else input for input in material_inputs
            ]
            if len(material_inputs) != len(internal_data_node.signal_link_node.signals):
                if not output.domain_spec.dimension_filter_spec.is_material():
                    # try to materialize output from other materialized inputs
                    test_node = RuntimeLinkNode(internal_data_node.signal_link_node)
                    for input in material_input_signals:
                        test_node.receive(input)
                    output = test_node.materialize_output(output, internal_data_node.output_dim_matrix, force=True)

                if output.domain_spec.dimension_filter_spec.is_material():
                    # output is materialized and we just have some of the inputs. attempt to compensate the missing inputs.
                    # materialization handles references as well.
                    # will raise an exception if all of the inputs cannot be mapped from others (and the output).
                    material_inputs = internal_data_node.signal_link_node.get_materialized_inputs_for_output(
                        output,  # verified to be materialized already
                        internal_data_node.output_dim_matrix,
                        already_materialized_inputs=material_input_signals,
                    )
                    auto_generated_material_inputs = set(material_inputs) - set(material_input_signals)

        inputs = material_inputs if isinstance(material_inputs, List) else [material_inputs]
        input_signals = [self._get_input_signal(input) if not isinstance(input, Signal) else input for input in inputs]
        input_signals = self._check_upstream(input_signals)

        # reset alias' (otherwise below check within RuntimeLinkNode won't work)
        alias_list = [signal.alias for signal in input_signals]
        input_signals = [signal.clone(None) for signal in input_signals]

        # for client's convenience, check the runtime behaviour of the input signals against the node.
        # whether they will actually cause a trigger or not?
        link_node = RuntimeLinkNode(copy.deepcopy(internal_data_node.signal_link_node))
        for i, signal in enumerate(input_signals):
            if not link_node.can_receive(signal):
                raise ValueError(
                    f"Cannot execute node! Input signal with alias ({alias_list[i]!r}) and order [{i}] is "
                    f"not compatible with target node ({internal_data_node.data_id!r})."
                )
            if not signal.domain_spec.dimension_filter_spec.is_material():
                raise ValueError(
                    f"Cannot execute node! Input signal with alias ({alias_list[i]!r}) and order [{i}] is "
                    f"not materialized. Its dimensions contain special characters. Following dimension filter "
                    f"should contain concrete/material values only: {signal.domain_spec.dimension_filter_spec!r}"
                )

            ready_signal_count = len(link_node.ready_signals)
            # now feed the materialized signal
            # we don't provide output and dim matrix (for references) because materialization above will guarantee to
            # have all of the inputs at this point (including references too).
            if not link_node.receive(signal):
                if signal.clone(alias_list[i]) not in auto_generated_material_inputs:
                    raise ValueError(
                        f"Cannot execute node! Input signal with alias ({alias_list[i]!r}) and order [{i}] is "
                        f"rejected from the execution of {internal_data_node.data_id!r}. "
                        f"Its dimension filter is not compatible with previous inputs."
                    )

            if ready_signal_count == len(link_node.ready_signals):
                logger.critical(
                    f"Redundant input signal with alias ({alias_list[i]!r}) and order [{i}] is detected for"
                    f" the execution of node {internal_data_node.data_id!r}."
                )

        if not link_node.is_ready():
            raise ValueError(
                f"Cannot execute node {internal_data_node.data_id!r}! Not all of the inputs are satisfied."
                f" Unsatisfied inputs: {[signal.unique_key() for signal in link_node.signals if signal not in link_node.ready_signals]}"
            )

        active_version = self.get_data(
            internal_data_node.data_id, Application.QueryApplicationScope.CURRENT_APP_ONLY, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT
        )
        is_route_new = not bool(active_version)

        # now we can start the entire execution
        # TODO support node declarations with RelativeVariants which would have multipled branches in their filter spec
        materialized_output: Signal = link_node.materialize_output(internal_data_node.signal(), internal_data_node.output_dim_matrix)
        if not is_route_new:
            # however, first make sure that no other active compute record exists on the same route and on the same
            # output. Otherwise, this possible execution will be rejected remotely (idempotency) and cause inconvenience
            # for the client.
            while True:
                route_record = self.platform.routing_table.optimistic_read(internal_data_node.route_id)
                if route_record and route_record.has_active_record_for(materialized_output):
                    active_records = route_record.get_active_records_of(materialized_output)
                    logger.critical(
                        f"Data node {internal_data_node.data_id!r} has other active execution(s) on it now."
                        f" And at least one of those executions is working on the same output: {materialized_output.get_materialized_resource_paths()[0]}"
                        f"Active compute sessions: {[record.session_state for record in active_records]!r}."
                        f" Will wait for it to complete first. Will check in 30 seconds..."
                    )
                    time.sleep(30)
                else:
                    logger.critical(f"No active execution detected! Will proceed with this execution now...")
                    break

        # 1 - make sure that the application is activated if the node is new (only in dev-context) or updated
        if is_route_new:
            logger.critical(f"Activating the application since the node {internal_data_node.data_id!r} is new...")
            self.activate()
        else:
            updated_version = self.get_data(
                internal_data_node.data_id, Application.QueryApplicationScope.CURRENT_APP_ONLY, Application.QueryContext.DEV_CONTEXT
            )
            if updated_version:
                active_route = cast(InternalDataNode, active_version[0].bound).create_route()
                dev_route = cast(InternalDataNode, updated_version[0].bound).create_route()
                if not active_route.check_integrity(dev_route):
                    logger.critical(f"Activating the application since the node/route {internal_data_node.data_id!r} is updated...")
                    self.activate()

        # 2- Process the inputs (feed them into the application, order does not matter)
        logger.critical(f"Sending input signals from {alias_list!r} into the system...")
        execution_context_and_records: Tuple["Route.ExecutionContext", List["RoutingTable.ComputeRecord"]] = None
        idempotency_check_detected: bool = False
        while not execution_context_and_records:
            for i, input in enumerate(inputs):
                response: RoutingTable.Response = self.process(
                    input,
                    # use local Processor for the synchronous logic required here.
                    with_activated_processor=False,
                    processing_mode=FeedBackSignalProcessingMode.ONLY_HEAD,
                    target_route_id=internal_data_node.route_id,
                )[0]
                execution_contexts: Optional[
                    Dict["Route.ExecutionContext", List["RoutingTable.ComputeRecord"]]
                ] = response.get_execution_contexts(internal_data_node.route_id)
                if not input_signals[i].is_reference and execution_contexts is None:
                    # this should not happen after all those RuntimeLinkNode based checks (internal error),
                    # unless the input is a reference which might not yield a response from routing.
                    raise RuntimeError(f"Internal Error: Route ({internal_data_node.data_id}) rejected input: {input.alias!r}!")

                if execution_contexts:
                    if len(execution_contexts) > 1:
                        logger.critical("New execution contexts on this route have been detected!")
                        logger.warning(
                            "Multiple executions started probably due to some of the inputs effecting"
                            " a range. This happens very rarely when one of the inputs is not linked"
                            " or it is dimensionless (look-up, etc) and causes triggers on a pending"
                            " node which was partially satisfied by previously received signals."
                        )
                        logger.critical(
                            f"Finding the right context for output: {materialized_output.get_materialized_resource_paths()[0]!r}"
                        )
                    else:
                        logger.critical("New execution context on this route has been detected!")

                    for exec_ctx, active_compute_records in execution_contexts.items():
                        if DimensionFilter.check_equivalence(
                            exec_ctx.output.domain_spec.dimension_filter_spec, materialized_output.domain_spec.dimension_filter_spec
                        ):
                            if len(exec_ctx.slots) == len(active_compute_records):
                                execution_context_and_records = (exec_ctx, active_compute_records)
                            else:
                                idempotency_check_detected = True
                            break

                    if i < len(inputs) - 1:
                        logger.critical(
                            f"Execution started even before some of the inputs are processed/injected."
                            f"This means that either a pending node for the output existed before the "
                            f"execution or at least one of the inputs is a dependent (e.g ref, nearest)."
                        )
                    if execution_context_and_records:
                        # break early before feeding other inputs to avoid creating new pending nodes unnecessarily.
                        break

            if not execution_context_and_records:
                if not idempotency_check_detected:
                    raise RuntimeError(
                        f"Execution could not be started on node: {internal_data_node.data_id!r}! "
                        f"Check inputs to verify all conditions necessary for the executions exist."
                    )

                # This is a paranoid level handling of a scenario where we would have a concurrent execution
                # between active compute records check (previous while loop) and this loop.
                route_record = self.platform.routing_table.optimistic_read(internal_data_node.route_id)
                active_records = route_record.get_active_records_of(materialized_output)
                if active_records:
                    logger.critical(
                        f"Data node {internal_data_node.data_id!r} has other active execution(s) on it now."
                        f" And at least one of those executions is working on the same output: {materialized_output.get_materialized_resource_paths()[0]}"
                        f"Active compute sessions: {[record.session_state for record in active_records]!r}."
                    )
                else:
                    logger.critical(
                        "Unsuccessful execution attempt! "
                        "Might be due to a concurrent execution in the background, "
                        "which must be complete now"
                    )
                logger.critical(
                    "Will wait for 30 seconds for next attempt. "
                    "Optionally you can abort this process right now safely, if you have seen this so many "
                    "times already (more than a reasonable wait time for compute on this node)."
                )
                time.sleep(30)

        # check the initial state of the compute records (failures, etc)
        execution_context = execution_context_and_records[0]
        active_compute_records = execution_context_and_records[1]
        execution_timestamp_in_utc = min([r.trigger_timestamp_utc for r in active_compute_records])

        logger.critical("Execution started!")
        logger.critical(f"Execution ID: {execution_context.id}")
        logger.critical(f"Output: {materialized_output.get_materialized_resource_paths()[0]}")

        logger.critical("Active Compute Records (corresponding to the compute_targets you attached to this node):")
        logger.critical("-----------------------------------------------------")
        all_complete = True
        for active_record in active_compute_records:
            logger.critical(
                f" Compute target: {(active_record.slot.type, active_record.slot.code_lang, active_record.slot.code_abi, active_record.slot.extra_params)!r}"
            )
            logger.critical(f" Initial response state: {active_record.state!r}")
            logger.critical(f" Session state: {active_record.session_state!r}")
            if not active_record.session_state or active_record.session_state.state_type != ComputeSessionStateType.COMPLETED:
                all_complete = False
        if wait:
            initial_wait_time_in_secs = 5 if all_complete else 30  # give some time for possible eventual consistency
            logger.critical(f"Will check the result of execution in {initial_wait_time_in_secs} seconds...")
            time.sleep(initial_wait_time_in_secs)
            # as soon as the executions are over, IF marks/moves them as historical compute records.
            # read the most recent inactive records
            while True:
                inactive_records = self.platform.routing_table.load_inactive_compute_records(internal_data_node.route_id, ascending=False)
                logger.critical(f"Checking the result of compute records:")
                completed_records = set()
                if inactive_records:
                    for inactive_record in inactive_records:
                        # time check is to avoid a scan against the entire history
                        if (
                            inactive_record.trigger_timestamp_utc >= execution_timestamp_in_utc
                            and inactive_record.execution_context_id == execution_context.id
                        ):
                            # now we know that inactive_record belongs to this execution
                            for active_record in active_compute_records:
                                if active_record == inactive_record:
                                    if inactive_record.session_state:
                                        if inactive_record.session_state.state_type == ComputeSessionStateType.COMPLETED:
                                            completed_records.add(inactive_record)
                                        else:
                                            logger.error(f"One of the compute targets could not be completed! ")
                                            logger.error(
                                                f" Problematic target: {(inactive_record.slot.type, inactive_record.slot.code_lang, inactive_record.slot.code_abi, inactive_record.slot.extra_params)!r}"
                                            )
                                            logger.error(f" Initial response state: {(inactive_record.state)!r}")
                                            logger.error(f" Session state: {inactive_record.session_state!r}")
                                            if inactive_record.session_state.state_type == ComputeSessionStateType.FAILED:
                                                logger.error(f"Abbreviated list of errors from all of the attempts (retries):")
                                                errors = []
                                                for i, exec_details in enumerate(inactive_record.session_state.executions):
                                                    if "ErrorMessage" in exec_details.details:
                                                        exec_error = exec_details.details["ErrorMessage"]
                                                        if len(inactive_record.session_state.executions) > 1:
                                                            exec_error = f"(attempt {i + 1}): {exec_error}"
                                                        logger.error(exec_error)
                                                        errors.append(exec_error)
                                                failed_session_state = cast(ComputeFailedSessionState, inactive_record.session_state)
                                                raise RuntimeError(
                                                    f"Execution attempt on {internal_data_node.data_id!r} could not be completed "
                                                    f" due to {failed_session_state.failed_type.value!r} failure! Errors: {errors!r}"
                                                )
                                    else:
                                        logger.error(
                                            f"Execution on one of compute targets of node {internal_data_node.data_id!r}"
                                            f" could not be started!"
                                        )
                                        logger.error(
                                            f" Problematic target: {(inactive_record.slot.type, inactive_record.slot.code_lang, inactive_record.slot.code_abi, inactive_record.slot.extra_params)!r}"
                                        )
                                        logger.error(f" Initial response state : {(inactive_record.state)!r}")
                                        logger.error(f" Session state: {inactive_record.session_state!r}")
                                        raise RuntimeError(
                                            f"Execution attempt on {internal_data_node.data_id!r} could not be started successfully!"
                                        )
                if len(completed_records) == len(active_compute_records):
                    logger.critical("Execution successfully completed!")
                    break
                else:
                    logger.critical("Execution is not complete yet.")
                    logger.critical(f" Incomplete count: {len(active_compute_records) - len(completed_records)}")
                    logger.critical(f" Completed count: {len(completed_records)}")
                    logger.critical("-----------------------------------------------------")
                    still_active = False
                    route_record = self.platform.routing_table.optimistic_read(internal_data_node.route_id)
                    if route_record and route_record.has_active_record_for(materialized_output):
                        # this will pull the most recent session_state (hence compute retry attempts, etc)
                        active_records = route_record.get_active_records_of(materialized_output)
                        if active_records:
                            still_active = True
                            logger.critical("Waiting for the following compute targets to complete:")
                            for active_record in active_records:
                                logger.critical("-----------------------------------------------------")
                                logger.critical(
                                    f" Compute target: {(active_record.slot.type, active_record.slot.code_lang, active_record.slot.code_abi, active_record.slot.extra_params)!r}"
                                )
                                logger.critical(f" Initial response state: {(active_record.state)!r}")
                                logger.critical(f" Session state: {active_record.session_state!r}")
                    if not still_active:
                        logger.critical(
                            "Looks like there is no active compute record anymore."
                            " They have just been marked as incomplete due to eventual consistency."
                            " System will detect the their final status as 'inactive' records very shortly."
                            " If you see this message consecutively, there must be a problem."
                        )
                    logger.critical("-----------------------------------------------------")
                    logger.critical("Will check the result of execution in 30 seconds again...")
                    time.sleep(30)
        else:
            logger.critical(
                "Exiting without waiting for the execution. You can use Application::poll API on the same "
                "output node to hook with the execution later on."
            )
        # now we are sure that data is ready, we can now return the materialized output signal.
        # it can be used by client to act on the data now.
        input_signal = self._materialize_internal(materialized_output)
        materialized_paths = input_signal.get_materialized_resource_paths()
        return materialized_paths[0]

    def materialize(self, signal_view: Union[FilteredView, MarshalerNode], material_values: RawDimensionFilterInput = None) -> List[str]:
        """Return materialized resource path(s) for the input signal.

        This API is expected to be used in scenarios when user needs the concrete/physical path(s) of a signal that
        represents its domain (or the filtered version of that domain).

        Input can be any signal returned by other node creation/update APIs such as 'create_data', or a filtered
        version of it.

        Example:

            dataset = app.create_data(...)
            paths = app.materialize(dataset['NA']['2020-05-21'])
            print(paths)
                ['s3://internal-bucket/root-folter/NA/2020-05-21',
                's3://internal-bucket/root-folter/NA/2020-05-20',
                's3://internal-bucket/root-folter/NA/2020-05-19']

        Parameters
        -----------
        signal_view: input data/timer/model signal encapsulated by an app-level filtered view or a marshaler node.

        Returns
        -----------
        One of more physical paths depending on the final state of the filering applied to the signal.
        Special variant '*' (for un-materialized signals) is left as is.
        Relative variants are exploded into a full range depending on the filter parameter by the user, if the input
        is a MarshalerNode and no filtering applied towards the relative dimension yet, then the dimension is left as is.
        """
        input_signal: Signal = None
        if isinstance(signal_view, FilteredView):
            if signal_view.is_complete():
                filter = signal_view.signal.domain_spec.dimension_filter_spec.apply(signal_view.new_filter)
                if not filter and signal_view.signal.domain_spec.dimension_filter_spec:
                    raise ValueError(
                        f"Error in Application::materialize API. "
                        f"Filtering values {signal_view.filtering_levels!r} are out of the filter domain of "
                        f"dimension spec: {signal_view.signal.domain_spec.dimension_spec!r}, "
                        f"filter spec: {signal_view.signal.domain_spec.dimension_filter_spec!r}"
                    )
                input_signal = signal_view.signal.filter(filter)
            else:
                raise ValueError(
                    f"Error in Application::materialize API. "
                    f"Filtering values {signal_view.filtering_levels!r} not compatible with the "
                    f"dimension spec: {signal_view.signal.domain_spec.dimension_spec!r}."
                )
        elif isinstance(signal_view, MarshalerNode):
            input_signal = signal_view.signal().clone(None)
        else:
            raise ValueError(f"Input type {type(signal_view)} is not compatible with Application::materialize API.")

        use_tip_if_expands = False

        # check if material or not
        if material_values:
            # example: app.materialize(all_sog_data[1][:-2]. [1, "2020-10-29"]
            #           ->  materialized paths:
            #                 "s3://.../all_ship_options/1/2020-10-29"
            #                 "s3://.../all_ship_options/1/2020-10-28"
            if input_signal.domain_spec.dimension_filter_spec.is_material():
                raise ValueError(
                    f"Cannot apply material_values {material_values!r} to an already "
                    f"materialized filter: {input_signal.domain_spec.dimension_filter_spec!r}. "
                    f"Map of materialized dimensions: {input_signal.domain_spec.dimension_filter_spec.get_flattened_dimension_map()!r}"
                )

            material_filter = DimensionFilter.load_raw(material_values, cast=input_signal.domain_spec.dimension_spec)
            if material_filter is None or not input_signal.domain_spec.dimension_filter_spec.check_compatibility(material_filter, False):
                raise ValueError(
                    f"Material values {material_values!r} is not compatible with the final state of "
                    f"input filter {input_signal.domain_spec.dimension_filter_spec!r}. "
                    f"Please check the order, quantity and type of dimensions."
                )
            materialized_input_filter = input_signal.domain_spec.dimension_filter_spec.apply(material_filter)
            if materialized_input_filter is not None:
                if not materialized_input_filter and input_signal.domain_spec.dimension_filter_spec:
                    raise ValueError(
                        f"Material values {material_values!r} are out of the filter domain of "
                        f"dimension spec: {input_signal.domain_spec.dimension_spec!r}, "
                        f"filter spec: {input_signal.domain_spec.dimension_filter_spec!r}"
                    )
                input_signal = input_signal.filter(materialized_input_filter)
            else:
                raise ValueError(
                    f"Material values {material_values!r} do not fall into the domain of "
                    f"filtered signal: {input_signal.domain_spec.dimension_filter_spec!r}"
                )
        elif not input_signal.domain_spec.dimension_filter_spec.is_material():
            logger.warning(
                f"Input signal is not materialized yet. But materialization will still generate abstract resource paths with "
                f"special charaters (such as '*'). If your intention was to get physical paths, then use the second param "
                f"'material_values' to further specify the tip of the materialization so that path generation "
                f"can unfold based on those values."
            )
        else:
            # mimic the runtime behaviour.
            # user provided a material filter and intends to get the corresponding 'output' path for it.
            use_tip_if_expands = True

        input_signal = self._check_upstream([input_signal])[0]
        input_signal = self._materialize_internal(input_signal)
        materialized_paths = input_signal.get_materialized_resource_paths()
        return materialized_paths if not use_tip_if_expands else [materialized_paths[0]]

    def _materialize_internal(self, input_signal: Signal) -> Signal:
        if input_signal.resource_access_spec.get_owner_context_uuid() == self.uuid:
            if input_signal.resource_access_spec.source == SignalSourceType.INTERNAL:
                return self.platform.storage.map_internal_signal(input_signal)
            elif input_signal.resource_access_spec.source in [
                SignalSourceType.INTERNAL_METRIC,
                SignalSourceType.INTERNAL_ALARM,
                SignalSourceType.INTERNAL_COMPOSITE_ALARM,
            ]:
                return self.platform.diagnostics.map_internal_signal(input_signal)
        return input_signal

    def _check_upstream(self, input: Union[List[Signal], SignalDimensionTuple]) -> Union[List[Signal], SignalDimensionTuple]:
        """Check (internal) inputs from upstream applications and transform them into external signals for this application."""
        if not self._dev_context.external_data:
            return input

        if isinstance(input, List):
            return [self._check_upstream_signal(input_signal) for input_signal in input]
        else:
            return SignalDimensionTuple(self._check_upstream_signal(input.signal), *input.dimensions) if input.signal else input

    def _check_upstream_signal(self, input_signal: Signal) -> Signal:
        if input_signal.resource_access_spec.source in [
            SignalSourceType.INTERNAL,
            SignalSourceType.INTERNAL_METRIC,
            SignalSourceType.INTERNAL_ALARM,
            SignalSourceType.INTERNAL_COMPOSITE_ALARM,
        ]:
            owner_context_uuid = input_signal.resource_access_spec.get_owner_context_uuid()
            if owner_context_uuid != self.uuid:
                return self._dev_context.get_upstream_app(owner_context_uuid).map_as_external(input_signal)
        return input_signal

    def _create_remote_app(self, id: ApplicationID, conf: Configuration) -> "RemoteApplication":
        from intelliflow.core.application.remote_application import RemoteApplication

        return RemoteApplication(id, conf, self)

    def import_upstream_application(self, id: ApplicationID, conf: Configuration) -> "RemoteApplication":
        remote_app = self._create_remote_app(id, conf)
        if remote_app == self.platform.conf:
            raise ValueError(f"Cannot import self as an upstream application!")
        self._dev_context.add_upstream_app(remote_app)
        return remote_app

    def export_to_downstream_application(self, id: ApplicationID, conf: Configuration):
        conf.set_downstream(self._platform.conf, id)
        self._dev_context.add_downstream_app(id, conf)

    def get_upstream_applications(
        self, id: ApplicationID, conf: Configuration, search_context: QueryContext = QueryContext.ACTIVE_RUNTIME_CONTEXT
    ) -> Mapping[QueryContext, List["RemoteApplication"]]:
        """Find the application from the search_context (either from activated state or current dev state)

        Returns a map of search context and upstream application list pairs.
        """
        conf._set_context(id)

        upstream_apps = dict()
        if search_context in [Application.QueryContext.ALL, Application.QueryContext.DEV_CONTEXT]:
            for remote_app in self._dev_context.external_data:
                if remote_app == conf:
                    upstream_apps.setdefault(Application.QueryContext.DEV_CONTEXT, []).append(remote_app)
        if search_context in [Application.QueryContext.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT]:
            if self._active_context:
                for remote_app in self._active_context.external_data:
                    if remote_app == conf:
                        upstream_apps.setdefault(Application.QueryContext.ACTIVE_RUNTIME_CONTEXT, []).append(remote_app)

        return upstream_apps

    def _get_platform_for(self, signal: Signal) -> "DevelopmentPlatform":
        platform = self.platform
        owner_context_uuid = signal.resource_access_spec.get_owner_context_uuid()
        if owner_context_uuid != self.uuid:
            # if the node is upstream, it should already belong to an activated upstream app.
            remote_app = self._dev_context.get_upstream_app(owner_context_uuid)
            if remote_app:
                platform = remote_app.platform
            else:  # check active context now
                remote_app = self._active_context.get_upstream_app(owner_context_uuid)
                platform = remote_app.platform if remote_app else None
        return platform

    def set_security_conf(self, construct_type: Type[BaseConstruct], conf: ConstructSecurityConf) -> None:
        self._dev_context.add_security_conf(construct_type, conf)


__test__ = {name: value for name, value in locals().items() if name.startswith("test_")}

if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=False)
