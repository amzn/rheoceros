# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from abc import abstractmethod
from textwrap import dedent
from typing import Any, Callable, ClassVar, Dict, List, Optional, Sequence, Union
from uuid import uuid4

from intelliflow.core.application.context.node.base import DataNode
from intelliflow.core.application.context.node.filtered_views import FilteredView
from intelliflow.core.permission import Permission
from intelliflow.core.platform.compute_targets.descriptor import ComputeDescriptor as ComputeTargetDescriptor
from intelliflow.core.platform.development import HostPlatform
from intelliflow.core.serialization import SerializationError, dumps
from intelliflow.core.signal_processing import Signal, Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.dimension_constructs import DimensionSpec
from intelliflow.core.signal_processing.routing_runtime_constructs import Route, RouteCheckpoint, RouteExecutionHook, RoutePendingNodeHook
from intelliflow.core.signal_processing.signal import (
    DimensionLinkMatrix,
    SignalDimensionLink,
    SignalDomainSpec,
    SignalIntegrityProtocol,
    SignalLinkNode,
    SignalType,
)
from intelliflow.core.signal_processing.signal_source import PARTITION_KEYS_KEY, InternalDatasetSignalSourceAccessSpec
from intelliflow.core.signal_processing.slot import SlotType

# TODO
MonitoringConfig = Dict[str, Any]


class InternalDataNode(DataNode):
    DEFAULT_DATA_COMPLETION_PROTOCOL: ClassVar[SignalIntegrityProtocol] = SignalIntegrityProtocol("MANIFEST_CHECK", {"file": "_SUCCESS"})

    MAX_RETRY_COUNT: ClassVar[int] = 100

    class ComputeDescriptor(ComputeTargetDescriptor):
        @abstractmethod
        def __init__(
            self, type: SlotType, code: str, lang: Lang, abi: ABI, permissions: List[Permission] = None, retry_count: int = 0, **kwargs
        ) -> None:
            if permissions:
                if any([not perm.context for perm in permissions]):
                    raise ValueError(f"Permission context is missing from one of the permissions in {permissions!r}!")

            if retry_count < 0 or retry_count > InternalDataNode.MAX_RETRY_COUNT:
                raise ValueError(f"retry_count ({retry_count}) for slot {type!r} must be between 0 and {InternalDataNode.MAX_RETRY_COUNT}")

            self._slot = Slot(type, code, lang, abi, dict(kwargs), None, permissions, retry_count)

        # overrides
        def create_slot(self, output_signal: Signal) -> Slot:
            return Slot(
                self._slot.type,
                self._slot.code,
                self._slot.code_lang,
                self._slot.code_abi,
                self._slot.extra_params,
                output_signal,
                self._slot.compute_permissions,
                self._slot.retry_count,
            )

        # overrides
        def activation_completed(self, platform: "HostPlatform") -> None:
            pass

    class InlinedComputeDescriptor(ComputeDescriptor):
        # overrides
        def __init__(self, code: Callable, permissions: List[Permission] = None, **kwargs) -> None:
            super().__init__(SlotType.SYNC_INLINED, dumps(code), None, None, permissions, **kwargs)

    class BatchComputeDescriptor(ComputeDescriptor):
        # overrides
        def __init__(self, code: str, lang: Lang, abi: ABI, permissions: List[Permission], retry_count: int, **kwargs) -> None:
            # This should be the best place we can format user code. We can't move this into base class
            # ComputeDescriptor, because `code` in InlinedComputeDescriptor is a serialized(encoded) string of user's
            # Callable, and the "encoded" string can contain '\n'.
            if type(code) == str and lang == Lang.PYTHON:
                # `code` can be either of type `str`, or its subclass (`SlotCode`)
                # We don't do formatting when input is `SlotCode`, because in practice, `SlotCode` is typically created
                # to represent code from a python module, or in another language (e.g. Scala), in which case indentation
                # does not cause problems
                # If we decide to format SlotCode as well, notice applying any `str` methods on `SlotCode` will return
                # a new `str` object (rather than a SlotCode object) and cause we lose code metadata info.
                code = dedent(code).strip("\r\n")
            super().__init__(SlotType.ASYNC_BATCH_COMPUTE, code, lang, abi, permissions, retry_count, **kwargs)

        # overrides
        def create_output_attributes(
            self, platform: "HostPlatform", inputs: List[Signal], user_attrs: Dict[str, Any]
        ) -> Optional[Dict[str, Any]]:
            """Compute descriptor asks the related construct/driver for default compute args (that would possibly be passed
            into nodes/routes).
            """
            return platform.batch_compute.provide_output_attributes(inputs, self.create_slot(None), user_attrs)

    def __init__(
        self,
        data_id: str,
        signal_link_node: SignalLinkNode,
        output_dim_spec: DimensionSpec,
        output_dim_matrix: DimensionLinkMatrix,
        compute_targets: Sequence[ComputeTargetDescriptor],
        execution_hook: Optional[RouteExecutionHook] = None,
        pending_node_hook: Optional[RoutePendingNodeHook] = None,
        pending_node_expiration_ttl_in_secs: int = None,
        auto_backfilling_enabled: bool = False,
        protocol: SignalIntegrityProtocol = DEFAULT_DATA_COMPLETION_PROTOCOL,
        **kwargs,
    ) -> None:
        self._compute_targets = list(compute_targets)
        self._signal_link_node = signal_link_node
        self._output_dim_matrix = output_dim_matrix
        self._auto_backfilling_enabled = auto_backfilling_enabled
        self._execution_hook = self._check_execution_callables(execution_hook)
        self._pending_node_hook = self._check_pending_node_callables(pending_node_hook)
        self._pending_node_expiration_ttl_in_secs = pending_node_expiration_ttl_in_secs

        output_dimension_filter = signal_link_node.get_output_filter(output_dim_spec, output_dim_matrix)

        data_args = dict(kwargs)
        data_args[PARTITION_KEYS_KEY] = [key for key in output_dim_spec.get_flattened_dimension_map().keys()]

        output_source_access_spec = InternalDatasetSignalSourceAccessSpec(data_id, output_dim_spec, **data_args)

        output_domain_spec = SignalDomainSpec(output_dim_spec, output_dimension_filter, protocol)

        self._output_signal = Signal(SignalType.INTERNAL_PARTITION_CREATION, output_source_access_spec, output_domain_spec, data_id, False)

        super().__init__(
            data_id, output_source_access_spec, output_domain_spec, None, None, None  # parent_node  # child_nodes
        )  # node_id (will automatically set)

        # add originator / unique route_id and compute types info to metadata
        output_source_access_spec.route_id = self.route_id
        output_source_access_spec.slot_types = [slot.type for slot in self._slots()]

    @classmethod
    def _check_hook_type(cls, route_hook: Optional[Union[ComputeTargetDescriptor, Callable, Slot]]) -> Optional[Slot]:
        if route_hook:
            try:
                if isinstance(route_hook, ComputeTargetDescriptor):
                    return route_hook.create_slot(None)
                elif isinstance(route_hook, Callable):
                    return InternalDataNode.InlinedComputeDescriptor(route_hook).create_slot(None)
                elif isinstance(route_hook, Slot):
                    return route_hook
                else:
                    raise ValueError(
                        f"Wrong type {type(route_hook)} provided as hook! It should be either a <ComputeDescriptor>, <Callable> or <Slot>."
                    )
            except SerializationError as se:
                raise ValueError(f"Hook is not serializable! Hook: ({route_hook!r}). Error: ", se)

    def _check_execution_callables(self, execution_hook: Optional[RouteExecutionHook]) -> Optional[RouteExecutionHook]:
        """Check the callables and convert them to Slots by reusing InlinedComputeDescriptor

        For user convenience, we support Callables (probably implementing RoutingHookInterface.Execution.I...).
        But framework strictly expects Slot type on the hooks and the necessary conversion.
        Please see RoutingTable for more details on how those Slots are executed.
        """

        if execution_hook:
            return RouteExecutionHook(
                on_exec_begin=self._check_hook_type(execution_hook.on_exec_begin),
                on_exec_skipped=self._check_hook_type(execution_hook.on_exec_skipped),
                on_compute_success=self._check_hook_type(execution_hook.on_compute_success),
                on_compute_failure=self._check_hook_type(execution_hook.on_compute_failure),
                on_compute_retry=self._check_hook_type(execution_hook.on_compute_retry),
                on_success=self._check_hook_type(execution_hook.on_success),
                on_failure=self._check_hook_type(execution_hook.on_failure),
                checkpoints=[RouteCheckpoint(c.checkpoint_in_secs, self._check_hook_type(c.slot)) for c in execution_hook.checkpoints]
                if execution_hook.checkpoints
                else execution_hook.checkpoints,
            )

    def _check_pending_node_callables(self, pending_node_hook: Optional[RoutePendingNodeHook]) -> Optional[RoutePendingNodeHook]:
        """Check the callables and convert them to Slots by reusing InlinedComputeDescriptor

        For user convenience, we support Callables (probably implementing RoutingHookInterface.Execution.I...).
        But framework strictly expects Slot type on the hooks and the necessary conversion.
        Please see RoutingTable for more details on how those Slots are executed.
        """

        if pending_node_hook:
            return RoutePendingNodeHook(
                on_pending_node_created=self._check_hook_type(pending_node_hook.on_pending_node_created),
                on_expiration=self._check_hook_type(pending_node_hook.on_expiration),
                checkpoints=[RouteCheckpoint(c.checkpoint_in_secs, self._check_hook_type(c.slot)) for c in pending_node_hook.checkpoints]
                if pending_node_hook.checkpoints
                else pending_node_hook.checkpoints,
            )

    @property
    def signal_link_node(self) -> SignalLinkNode:
        return self._signal_link_node

    @property
    def output_dim_matrix(self) -> DimensionLinkMatrix:
        return self._output_dim_matrix

    @property
    def execution_hook(self) -> Optional[RouteExecutionHook]:
        return getattr(self, "_execution_hook", None)

    @property
    def pending_node_hook(self) -> Optional[RoutePendingNodeHook]:
        return getattr(self, "_pending_node_hook", None)

    @property
    def pending_node_expiration_ttl_in_secs(self) -> int:
        return getattr(self, "_pending_node_expiration_ttl_in_secs", None)

    def signal(self) -> Optional[Signal]:
        return self._output_signal

    def _slots(self) -> Optional[List[Slot]]:
        return [compute_target.create_slot(self._output_signal) for compute_target in self._compute_targets]

    def create_route(self) -> Route:
        return Route(
            self.route_id,
            self._signal_link_node,
            self._output_signal,
            self._output_dim_matrix,
            self._slots(),
            self._auto_backfilling_enabled,
            self.execution_hook,
            self.pending_node_expiration_ttl_in_secs,
            self.pending_node_hook,
        )

    def do_activate(self, platform: HostPlatform) -> None:
        # hey platform, please run my slots whenever this signal (link) group/node is satisfied.
        # Materialize my output based on the runtime version of those signals and also
        # do "auto range completion / backfilling" if possible.
        #
        # My slots expect these signals and the output as params.
        platform.connect_internal(self.create_route())


class MonitoringNode(InternalDataNode):
    # TODO complete post-MVP

    # expect param MonitoringConfig in
    def __init__(self, data_id: str, monitoring_alias: str, input_signal: Signal, config: Dict[str, Any]) -> None:
        self._config = config
        self._input_signal = input_signal

        monitoring_data_id: str = f"{data_id!r}-{monitoring_alias!r}-{uuid4()!r}"
        signal_link_node = SignalLinkNode([input_signal])
        output_dim_spec = copy.deepcopy(input_signal.domain_spec.dimension_spec)
        output_dim_matrix = self._create_output_dim_matrix()
        monitoring_compute = self._create_monitoring_compute()
        super().__init__(monitoring_data_id, signal_link_node, output_dim_spec, output_dim_matrix, [monitoring_compute], False)

    def _create_output_dim_matrix(self) -> List[SignalDimensionLink]:
        # TODO post-MVP
        # use self._input_signal to create it
        # for each dimension of input_signal, create an identical counterpart
        pass

    def _create_monitoring_compute(self) -> InternalDataNode.BatchComputeDescriptor:
        # TODO post-MVP
        # read the config and create the batch compute accordingly.
        pass

    def add_assertion(self):
        pass


class MonitoringView(FilteredView):
    pass
