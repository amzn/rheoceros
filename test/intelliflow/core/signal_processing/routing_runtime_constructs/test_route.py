# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
import time
from test.intelliflow.core.signal_processing.dimension_constructs.test_dimension_spec import TestDimensionSpec
from test.intelliflow.core.signal_processing.routing_runtime_constructs import create_incoming_signal
from test.intelliflow.core.signal_processing.signal.test_signal_link_node import signal_dimension_tuple

import pytest

from intelliflow.core.platform.constructs import RoutingHookInterface
from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.routing_runtime_constructs import *
from intelliflow.core.signal_processing.signal import *
from intelliflow.core.signal_processing.signal_source import InternalDatasetSignalSourceAccessSpec
from intelliflow.core.signal_processing.slot import SlotType


def _create_hook(code: str = "pass") -> Slot:
    return Slot(SlotType.SYNC_INLINED, dumps(code), None, None, None, None)


class TestRoute:
    @classmethod
    def _route_1_basic(cls):
        from test.intelliflow.core.signal_processing.test_slot import TestSlot
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal
        from test.intelliflow.core.signal_processing.signal.test_signal_link_node import TestSignalLinkNode

        signal_link_node = copy.deepcopy(TestSignalLinkNode.signal_link_node_1)

        output_spec = DimensionSpec.load_from_pretty({"output_dim": {type: Type.LONG}})

        output_dim_link_matrix = [
            SignalDimensionLink(
                signal_dimension_tuple(None, "output_dim"), lambda x: x, signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1")
            )
        ]
        output_filter = signal_link_node.get_output_filter(
            output_spec,
            # Logical equivalent -> output_dim = (signal_internal_1('dim_1_1')
            output_dim_link_matrix,
        )
        output_signal = Signal(
            TestSignal.signal_internal_1.type,
            InternalDatasetSignalSourceAccessSpec("sample_data", output_spec, **{}),
            SignalDomainSpec(output_spec, output_filter, TestSignal.signal_internal_1.domain_spec.integrity_check_protocol),
            "sample_data",
        )
        return Route(
            f"InternalDataNode-{output_signal.alias}",
            signal_link_node,
            output_signal,
            output_dim_link_matrix,
            [TestSlot.slot_batch_compute_basic],
            False,
        )

    @classmethod
    def _route_2_two_inputs_linked(cls):
        from test.intelliflow.core.signal_processing.test_slot import TestSlot
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal
        from test.intelliflow.core.signal_processing.signal.test_signal_link_node import TestSignalLinkNode

        signal_link_node = copy.deepcopy(TestSignalLinkNode.signal_link_node_2)

        output_spec = DimensionSpec.load_from_pretty({"output_dim": {type: Type.LONG}})

        output_dim_link_matrix = [
            SignalDimensionLink(
                signal_dimension_tuple(None, "output_dim"), lambda x: x, signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1")
            )
        ]
        output_filter = signal_link_node.get_output_filter(output_spec, output_dim_link_matrix)
        output_signal = Signal(
            TestSignal.signal_internal_1.type,
            InternalDatasetSignalSourceAccessSpec("sample_data_2", output_spec, **{}),
            SignalDomainSpec(output_spec, output_filter, TestSignal.signal_internal_1.domain_spec.integrity_check_protocol),
            "sample_data_2",
        )

        return Route(
            f"InternalDataNode-{output_signal.alias}",
            signal_link_node,
            output_signal,
            output_dim_link_matrix,
            [TestSlot.slot_batch_compute_basic],
            False,
        )

    @classmethod
    def _route_3_three_inputs_unlinked(cls):
        from test.intelliflow.core.signal_processing.test_slot import TestSlot
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal
        from test.intelliflow.core.signal_processing.signal.test_signal_link_node import TestSignalLinkNode

        signal_link_node = copy.deepcopy(TestSignalLinkNode.signal_link_node_3_complex)
        # create sample expected output
        output_spec = DimensionSpec.load_from_pretty(
            {"output_dim_1": {type: Type.LONG, "output_dim_2": {type: Type.LONG, "output_dim_3": {type: Type.LONG}}}}
        )

        output_dim_link_matrix = [
            SignalDimensionLink(
                signal_dimension_tuple(None, "output_dim_1"),
                lambda x: x,
                signal_dimension_tuple(TestSignal.signal_internal_complex_1, "dim_1_1"),
            ),
            SignalDimensionLink(
                signal_dimension_tuple(None, "output_dim_2"),
                # input's sub dimension is of type String, convert it.
                # because output spec expects it to be of type Long.
                lambda x: ord(x),
                signal_dimension_tuple(TestSignal.signal_internal_complex_1, "dim_1_2"),
            ),
            SignalDimensionLink(
                signal_dimension_tuple(None, "output_dim_3"),
                # and this one is from the 3rd input (which has only one dim 'dim_1_1')
                lambda x: x,
                signal_dimension_tuple(TestSignal.signal_s3_1, "dim_1_1"),
            ),
        ]
        output_filter = signal_link_node.get_output_filter(output_spec, output_dim_link_matrix)
        output_signal = Signal(
            SignalType.INTERNAL_PARTITION_CREATION,
            InternalDatasetSignalSourceAccessSpec("sample_data_3", output_spec, **{}),
            SignalDomainSpec(output_spec, output_filter, TestSignal.signal_internal_complex_1.domain_spec.integrity_check_protocol),
            "sample_data_3",
        )

        return Route(
            f"InternalDataNode-{output_signal.alias}",
            signal_link_node,
            output_signal,
            output_dim_link_matrix,
            [TestSlot.slot_batch_compute_basic],
            False,
        )

    @classmethod
    def _route_3_three_inputs_linked(cls):
        from test.intelliflow.core.signal_processing.test_slot import TestSlot
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal
        from test.intelliflow.core.signal_processing.signal.test_signal_link_node import TestSignalLinkNode

        signal_link_node = copy.deepcopy(TestSignalLinkNode.signal_link_node_3_complex)

        # add links (since the dimension names on same, use the auto-linking of dimensions,
        # so that;
        #   signal_internal_complex_1['dim_1_1'] == signal_s3_1['dim_1_1'], etc
        signal_link_node.compensate_missing_links()

        # create sample expected output
        output_spec = DimensionSpec.load_from_pretty(
            {
                "output_dim_1": {
                    type: Type.LONG,
                    "output_dim_2": {
                        type: Type.LONG,
                    },
                }
            }
        )

        output_dim_link_matrix = [
            SignalDimensionLink(
                signal_dimension_tuple(None, "output_dim_1"),
                # from the second dimension of the first/second inputs (convert to Long)
                lambda x: ord(x),
                signal_dimension_tuple(TestSignal.signal_internal_complex_1, "dim_1_2"),
            ),
            SignalDimensionLink(
                signal_dimension_tuple(None, "output_dim_2"),
                # and this one is from the 3rd input (which has only one dim 'dim_1_1')
                lambda x: x,
                signal_dimension_tuple(TestSignal.signal_s3_1, "dim_1_1"),
            ),
        ]
        output_filter = signal_link_node.get_output_filter(output_spec, output_dim_link_matrix)
        output_signal = Signal(
            SignalType.INTERNAL_PARTITION_CREATION,
            InternalDatasetSignalSourceAccessSpec("sample_data_4", output_spec, **{}),
            SignalDomainSpec(output_spec, output_filter, TestSignal.signal_internal_complex_1.domain_spec.integrity_check_protocol),
            "sample_data_4",
        )

        return Route(
            f"InternalDataNode-{output_signal.alias}",
            signal_link_node,
            output_signal,
            output_dim_link_matrix,
            [TestSlot.slot_batch_compute_basic],
            False,
        )

    def test_route_init(self):
        assert self._route_1_basic()

    def test_route_init_with_hooks(self):
        route = self._route_1_basic()

        Route(
            route.route_id,
            route.link_node,
            route.output,
            route._output_dim_matrix,
            route.slots,
            False,
            RouteExecutionHook(
                on_exec_begin=_create_hook(),
                on_exec_skipped=_create_hook(),
                on_compute_success=_create_hook(),
                on_compute_failure=_create_hook(),
                on_success=_create_hook(),
                on_failure=_create_hook(),
                checkpoints=[RouteCheckpoint(5, _create_hook())],
            ),
            30 * 24 * 60 * 60,
            RoutePendingNodeHook(on_pending_node_created=_create_hook(), on_expiration=_create_hook(), checkpoints=None),
        )

        # check another instantiation case + checkpoint sorting
        assert (
            Route(
                route.route_id,
                route.link_node,
                route.output,
                route._output_dim_matrix,
                route.slots,
                False,
                RouteExecutionHook(
                    on_exec_begin=_create_hook(),
                    on_exec_skipped=_create_hook(),
                    on_compute_success=_create_hook(),
                    on_compute_failure=_create_hook(),
                    on_success=_create_hook(),
                    on_failure=_create_hook(),
                    checkpoints=[],
                ),
                None,
                RoutePendingNodeHook(
                    on_pending_node_created=_create_hook(),
                    on_expiration=None,
                    checkpoints=[RouteCheckpoint(2, _create_hook()), RouteCheckpoint(1, _create_hook())],
                ),
            )
            .pending_node_hook.checkpoints[0]
            .checkpoint_in_secs
            == 1
        )

    def test_route_init_with_hook_chain(self):
        route = self._route_1_basic()

        callback1_var = None
        callback1_var_expected = 1

        def _callback1(*args, **kwargs):
            nonlocal callback1_var
            callback1_var = callback1_var_expected

        callback2_var = None
        callback2_var_expected = 2

        def _callback2(*args, **kwargs):
            nonlocal callback2_var
            callback2_var = callback2_var_expected

        hook1 = RouteExecutionHook(
            on_exec_begin=_create_hook(),
            on_exec_skipped=_callback1,
            on_compute_success=_create_hook(),
            on_compute_failure=_create_hook(),
            on_success=_create_hook(),
            on_failure=_create_hook(),
            checkpoints=[RouteCheckpoint(5, _create_hook())],
        )

        hook2 = RouteExecutionHook(
            on_exec_begin=_create_hook(),
            on_exec_skipped=_callback2,
            on_compute_success=_create_hook(),
            on_compute_failure=_create_hook(),
            on_success=_create_hook(),
            on_failure=_create_hook(),
            checkpoints=[RouteCheckpoint(10, _create_hook())],
        )
        exec_hook_chain = hook1.chain(hook2)

        pending_hook1 = RoutePendingNodeHook(
            on_pending_node_created=_create_hook(), on_expiration=_create_hook(), checkpoints=[RouteCheckpoint(5, _create_hook())]
        )
        pending_hook2 = RoutePendingNodeHook(
            on_pending_node_created=_create_hook(), on_expiration=_create_hook(), checkpoints=[RouteCheckpoint(10, _create_hook())]
        )
        pending_hook3 = RoutePendingNodeHook(
            on_pending_node_created=_create_hook(), on_expiration=_create_hook(), checkpoints=[RouteCheckpoint(13, _create_hook())]
        )
        pending_hook_chain = pending_hook1.chain(pending_hook2, pending_hook3)
        pending_hook_chain_2 = pending_hook1.chain(pending_hook2).chain(pending_hook3)

        Route(
            route.route_id,
            route.link_node,
            route.output,
            route._output_dim_matrix,
            route.slots,
            False,
            exec_hook_chain,
            24 * 60 * 60,
            pending_hook_chain,
        )

        assert len(exec_hook_chain.checkpoints) == 2
        assert len(pending_hook_chain.checkpoints) == 3
        assert len(pending_hook_chain_2.checkpoints) == 3

        exec_hook_chain.on_exec_begin()
        pending_hook_chain.on_pending_node_created()
        pending_hook_chain_2.on_expiration()

        exec_hook_chain.on_exec_skipped()
        assert callback1_var == callback1_var_expected
        assert callback2_var == callback2_var_expected

    def test_route_equality(self):
        assert self._route_1_basic() == self._route_1_basic()
        assert Route("test", None, None, [], [], False) == Route("test", None, None, [], [], False)
        assert Route("test", None, None, [], [], False) != Route("test2", None, None, [], [], False)
        assert self._route_1_basic() == self._route_1_basic().clone()

    def test_route_check_integrity(self):
        route = self._route_1_basic()
        assert route.check_integrity(self._route_1_basic())
        route2 = self._route_2_two_inputs_linked()
        # Route is very sensitive about an integrity check against a different Route. This is very critical
        # for whole Routing module. It should not occur! A safe-guard against a high-level (e.g RoutingTable) bug.
        with pytest.raises(ValueError):
            assert route.check_integrity(route2)
        # make id equal so that check move on to other fields
        route2._id = route.route_id
        assert not route.check_integrity(route2)

        assert route.check_integrity(Route(route.route_id, route.link_node, route.output, route._output_dim_matrix, route.slots, False))
        assert not route.check_integrity(
            Route(route.route_id, route2.link_node, route.output, route._output_dim_matrix, route.slots, False)
        )
        assert not route.check_integrity(
            Route(route.route_id, route.link_node, route2.output, route._output_dim_matrix, route.slots, False)
        )
        assert not route.check_integrity(Route(route.route_id, route.link_node, route.output, [], route.slots, False))
        assert not route.check_integrity(Route(route.route_id, route.link_node, route.output, route._output_dim_matrix, [], False))

    def test_route_check_integrity_noops(self):
        """show that some type of changes in route should not invalidate the integrity"""
        route = self._route_3_three_inputs_linked()

        # dim matrix ordering should not alter the semantics of route
        new_route = copy.deepcopy(route)
        new_route.link_node.link_matrix.reverse()
        new_route.output_dim_matrix.reverse()

        # TODO evaluate slots order? currently impacting integrity but not as critical as dim matrice

        assert route.check_integrity(new_route)

    @pytest.mark.parametrize(
        "execution_hook_1, pending_node_ttl_1, pending_hook_1, execution_hook_2, pending_node_ttl_2, pending_hook_2, result",
        [
            (None, 30 * 24 * 60 * 60, None, None, 24 * 60 * 60, None, False),
            (
                RouteExecutionHook(
                    on_exec_begin=_create_hook(),
                    on_exec_skipped=_create_hook(),
                    on_compute_success=_create_hook(),
                    on_compute_failure=_create_hook(),
                    on_success=_create_hook(),
                    on_failure=_create_hook(),
                    checkpoints=[RouteCheckpoint(checkpoint_in_secs=5, slot=_create_hook())],
                ),
                30 * 24 * 60 * 60,
                RoutePendingNodeHook(
                    on_pending_node_created=_create_hook(),
                    on_expiration=_create_hook(),
                    checkpoints=[RouteCheckpoint(checkpoint_in_secs=1, slot=_create_hook()), RouteCheckpoint(2, _create_hook())],
                ),
                RouteExecutionHook(
                    on_exec_begin=_create_hook(),
                    on_exec_skipped=_create_hook(),
                    on_compute_success=_create_hook(),
                    on_compute_failure=_create_hook(),
                    on_success=_create_hook(),
                    on_failure=_create_hook(),
                    checkpoints=[RouteCheckpoint(5, _create_hook())],
                ),
                30 * 24 * 60 * 60,
                RoutePendingNodeHook(
                    on_pending_node_created=_create_hook(),
                    on_expiration=_create_hook(),
                    # also test that checkpoint other should not matter as long as values are same
                    checkpoints=[RouteCheckpoint(2, _create_hook()), RouteCheckpoint(1, _create_hook())],
                ),
                True,
            ),
            (
                RouteExecutionHook(on_exec_begin=_create_hook()),
                30 * 24 * 60 * 60,
                RoutePendingNodeHook(),
                RouteExecutionHook(on_exec_begin=_create_hook()),
                30 * 24 * 60 * 60,
                RoutePendingNodeHook(),
                True,
            ),
            (
                RouteExecutionHook(on_exec_begin=_create_hook("print('diff')")),
                30 * 24 * 60 * 60,
                RoutePendingNodeHook(),
                RouteExecutionHook(on_exec_begin=_create_hook()),
                30 * 24 * 60 * 60,
                RoutePendingNodeHook(),
                False,
            ),
            (None, None, None, None, None, None, True),
            (
                RouteExecutionHook(on_exec_begin=None, on_exec_skipped=None),
                None,
                None,
                RouteExecutionHook(on_exec_begin=None, on_exec_skipped=_create_hook()),
                None,
                None,
                False,
            ),
            (
                RouteExecutionHook(on_exec_begin=None, on_exec_skipped=_create_hook()),
                None,
                None,
                RouteExecutionHook(on_exec_begin=None, on_exec_skipped=None),
                None,
                None,
                False,
            ),
            (
                RouteExecutionHook(
                    on_exec_begin=None,
                    on_exec_skipped=None,
                    on_compute_success=None,
                    on_compute_failure=None,
                    on_success=None,
                    on_failure=None,
                    checkpoints=[RouteCheckpoint(1, _create_hook())],
                ),
                None,
                RoutePendingNodeHook(),
                RouteExecutionHook(
                    on_exec_begin=None,
                    on_exec_skipped=None,
                    on_compute_success=None,
                    on_compute_failure=None,
                    on_success=None,
                    on_failure=None,
                    # change the value of first checkpoint
                    checkpoints=[RouteCheckpoint(5, _create_hook())],
                ),
                None,
                RoutePendingNodeHook(),
                False,
            ),
            (
                RouteExecutionHook(),
                None,
                RoutePendingNodeHook(
                    on_pending_node_created=_create_hook(), on_expiration=None, checkpoints=[RouteCheckpoint(2, _create_hook())]
                ),
                RouteExecutionHook(),
                None,
                RoutePendingNodeHook(
                    on_pending_node_created=_create_hook(),
                    on_expiration=None,
                    # also test that checkpoint other should not matter as long as values are same
                    checkpoints=[RouteCheckpoint(1, _create_hook())],
                ),
                False,
            ),
            (
                None,
                None,
                RoutePendingNodeHook(on_pending_node_created=None, on_expiration=None, checkpoints=[RouteCheckpoint(1, _create_hook())]),
                None,
                None,
                RoutePendingNodeHook(
                    on_pending_node_created=None,
                    on_expiration=None,
                    # also test that checkpoint other should not matter as long as values are same
                    checkpoints=[RouteCheckpoint(1, _create_hook("print('diff 2')"))],
                ),
                False,
            ),
        ],
    )
    def test_route_check_auxiliary_integrity(
        self, execution_hook_1, pending_node_ttl_1, pending_hook_1, execution_hook_2, pending_node_ttl_2, pending_hook_2, result
    ):
        route = self._route_1_basic()
        assert (
            Route(
                route.route_id,
                route.link_node,
                route.output,
                route._output_dim_matrix,
                route.slots,
                False,
                execution_hook_1,
                pending_node_ttl_1,
                pending_hook_1,
            ).check_auxiliary_data_integrity(
                Route(
                    route.route_id,
                    route.link_node,
                    route.output,
                    route._output_dim_matrix,
                    route.slots,
                    False,
                    execution_hook_2,
                    pending_node_ttl_2,
                    pending_hook_2,
                )
            )
            == result
        )

    def test_route_serialization(self):
        route = self._route_1_basic()
        assert route == loads(dumps(route))

    def test_route_receive_basic(self):
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal

        route = self._route_1_basic()

        # route will reject incompatible signal
        assert not route.receive(create_incoming_signal(TestSignal.signal_s3_1, [1]))
        assert not route._pending_nodes

        # successful trigger # 1
        response: Optional[Route.Response] = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        assert response
        assert len(response.new_execution_contexts) == 1
        assert response.new_execution_contexts[0].slots
        assert DimensionFilter.check_equivalence(
            response.new_execution_contexts[0].output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {}})
        )
        # since the node completed immediately (since it has only one input),
        # also removed from the internal pending nodes.
        assert not route._pending_nodes

        # successful trigger # 2
        response: Optional[Route.Response] = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [2]))
        assert DimensionFilter.check_equivalence(
            response.new_execution_contexts[0].output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({2: {}})
        )
        # since the node completed immediately (since it has only one input),
        # also removed from the internal pending nodes.
        assert not route._pending_nodes

    def test_route_receive_two_inputs_linked(self):
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal

        route = self._route_2_two_inputs_linked()

        # will consume the event, create a new pending node but return no 'new_execution_contexts'
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        assert not response.new_execution_contexts
        assert len(response.new_pending_nodes) == 1
        assert len(route._pending_nodes) == 1

        # will consume the event, create a new pending node but return no 'new_execution_contexts'
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [2]))
        assert not response.new_execution_contexts
        assert len(response.new_pending_nodes) == 1
        assert len(route._pending_nodes) == 2  # please note that it is 2 now!

        # will consume again with no internal effect
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [2]))
        assert not response.new_execution_contexts
        assert not response.new_pending_nodes
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        assert not response.new_execution_contexts
        assert not response.new_pending_nodes
        assert len(route._pending_nodes) == 2  # please note that it is 2 still

        # send in a Signal that belongs to the second input but with different dim value
        # will create another pending node since it is neither '1' nor '2' (linking is active).
        response = route.receive(create_incoming_signal(TestSignal.signal_s3_1, [3]))
        assert not response.new_execution_contexts
        assert len(response.new_pending_nodes) == 1
        assert len(route._pending_nodes) == 3  # please note that it is 3 now!

        # Completions
        # unleash the third pending node (which is pending on its first input with dim value 3)
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [3]))
        assert len(response.new_execution_contexts) == 1
        assert not response.new_pending_nodes
        assert DimensionFilter.check_equivalence(
            response.new_execution_contexts[0].output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({3: {}})
        )
        assert len(route._pending_nodes) == 2  # please note that it got back to 2!

        # unleash the fist node
        response = route.receive(create_incoming_signal(TestSignal.signal_s3_1, [1]))
        assert len(response.new_execution_contexts) == 1
        assert DimensionFilter.check_equivalence(
            response.new_execution_contexts[0].output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {}})
        )
        assert len(route._pending_nodes) == 1

        # and finally the second node
        response = route.receive(create_incoming_signal(TestSignal.signal_s3_1, [2]))
        assert len(response.new_execution_contexts) == 1
        assert DimensionFilter.check_equivalence(
            response.new_execution_contexts[0].output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({2: {}})
        )
        assert not route._pending_nodes

    def test_route_receive_three_inputs_unlinked(self):
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal

        route = self._route_3_three_inputs_unlinked()

        # will consume the event, create a new pending node but return no 'new_execution_contexts'
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [1, "y"]))
        assert not response.new_execution_contexts
        assert len(route._pending_nodes) == 1

        # will consume the event, create a new pending node but return no 'new_execution_contexts'
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "y"]))
        assert not response.new_execution_contexts
        assert len(route._pending_nodes) == 2  # please note that it is 2 now!

        # will consume again with no internal effect
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "y"]))
        assert not response.new_execution_contexts
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [1, "y"]))
        assert not response.new_execution_contexts
        assert len(route._pending_nodes) == 2  # please note that it is 2 still

        # EFFECT of missing linking (N-N logic)
        # incoming signal will satisfy all of the pending nodes
        response = route.receive(create_incoming_signal(TestSignal.signal_s3_1, [3]))
        assert len(response.new_execution_contexts) == 2
        assert not route._pending_nodes  # please note that it got back to 0 now!

        # we have to compare this way since the order is not guarateed
        if DimensionFilter.check_equivalence(
            response.new_execution_contexts[0].output.domain_spec.dimension_filter_spec,
            DimensionFilter.load_raw(
                {
                    2: {  # from the 1st dim of the 1st input signal
                        121: {3: {}}  # ord('y') from the second dim of the 1st input signal  # from the 3rd input
                    }
                }
            ),
        ):
            assert DimensionFilter.check_equivalence(
                response.new_execution_contexts[1].output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {121: {3: {}}}})
            )
        else:
            assert DimensionFilter.check_equivalence(
                response.new_execution_contexts[1].output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({2: {121: {3: {}}}})
            )

    def test_route_receive_three_inputs_linked(self):
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal

        route = self._route_3_three_inputs_linked()

        # will consume the event, create a new pending node but return no 'new_execution_contexts'
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [1, "y"]))
        assert not response.new_execution_contexts
        assert len(route._pending_nodes) == 1

        # will consume the event, create a new pending node but return no 'new_execution_contexts'
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "y"]))
        assert not response.new_execution_contexts
        assert len(route._pending_nodes) == 2

        # EFFECT of linking
        # incoming signal will not satisfy dimensional linking and will just create another node.
        response = route.receive(create_incoming_signal(TestSignal.signal_s3_1, [3]))
        assert not response.new_execution_contexts
        assert len(route._pending_nodes) == 3  # please note that it is 3 now!

        # unleash the most recent node
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [3, "y"]))
        assert len(response.new_execution_contexts) == 1
        assert len(route._pending_nodes) == 2
        assert DimensionFilter.check_equivalence(
            response.new_execution_contexts[0].output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({121: {3: {}}})
        )

        # unleash the node that created first
        response = route.receive(create_incoming_signal(TestSignal.signal_s3_1, [1]))
        assert len(response.new_execution_contexts) == 1
        assert len(route._pending_nodes) == 1
        assert DimensionFilter.check_equivalence(
            response.new_execution_contexts[0].output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({121: {1: {}}})
        )

        # unleash the node that created second
        response = route.receive(create_incoming_signal(TestSignal.signal_s3_1, [2]))
        assert len(response.new_execution_contexts) == 1
        assert not route._pending_nodes  # no remaining pending nodes!
        assert DimensionFilter.check_equivalence(
            response.new_execution_contexts[0].output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({121: {2: {}}})
        )

    def test_route_check_expired_nodes(self):
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal

        route = self._route_2_two_inputs_linked()

        route = Route(
            route.route_id,
            route.link_node,
            route.output,
            route._output_dim_matrix,
            route.slots,
            False,
            RouteExecutionHook(),
            5,  # seconds
            RoutePendingNodeHook(),
        )

        route.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        route.receive(create_incoming_signal(TestSignal.signal_internal_1, [2]))
        assert len(route._pending_nodes) == 2
        # send in a Signal that belongs to the second input but with different dim value
        # will create another pending node since it is neither '1' nor '2' (linking is active).
        response = route.receive(create_incoming_signal(TestSignal.signal_s3_1, [3]))
        assert not response.new_execution_contexts
        assert len(response.new_pending_nodes) == 1
        assert len(route._pending_nodes) == 3  # please note that it is 3 now!

        # Completions
        # unleash the third pending node (which is pending on its first input with dim value 3)
        route.receive(create_incoming_signal(TestSignal.signal_internal_1, [3]))
        assert len(route._pending_nodes) == 2  # please note that it got back to 2!

        # just make sure that it has been at least 5 seconds after the creation of those pending nodes.
        time.sleep(5)

        expired_nodes = route.check_expired_nodes()
        assert len(expired_nodes) == 2
        assert len(route._pending_nodes) == 0

    def test_route_zombie_node_on_other_input_already_materialized(self):
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal

        route = self._route_2_two_inputs_linked()

        # create new route to make sure that the second input is already materialized on value 3 [for dim_1_1]!
        new_signal_link_node = SignalLinkNode(
            [TestSignal.signal_internal_1, create_incoming_signal(TestSignal.signal_s3_1.clone("test_signal_from_S3"), [3])]
        )
        new_signal_link_node.compensate_missing_links()

        route = Route(
            route.route_id,
            new_signal_link_node,
            route.output,
            route._output_dim_matrix,
            route.slots,
            False,
            route.execution_hook,
            route.pending_node_ttl_in_secs,  # seconds
            route.pending_node_hook,
        )

        # since second input is locked on 3, this event would yield a zombie node
        # 1 != 3
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        assert not response.new_execution_contexts
        assert len(response.new_pending_nodes) == 1
        assert len(route._pending_nodes) == 1
        assert next(iter(response.new_pending_nodes)).is_zombie

        # same again  2 != 3
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [2]))
        # since second input is locked on 3, this event would yield a zombie node
        assert not response.new_execution_contexts
        assert len(response.new_pending_nodes) == 1
        assert len(route._pending_nodes) == 2
        assert next(iter(response.new_pending_nodes)).is_zombie

        # new pending node! 3 == 3
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [3]))
        assert not response.new_execution_contexts
        assert len(response.new_pending_nodes) == 1
        assert len(route._pending_nodes) == 3
        # new node should NOT be a zombie, waiting for TestSignal.signal_s3_1[3] to come in
        assert not next(iter(response.new_pending_nodes)).is_zombie

    def test_route_zombie_node_not_possible_when_inputs_unlinked(self):
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal

        route = self._route_2_two_inputs_linked()

        # create new route to make sure that the second input is already materialized on value 3 [for dim_1_1]!
        new_signal_link_node = SignalLinkNode(
            [TestSignal.signal_internal_1, create_incoming_signal(TestSignal.signal_s3_1.clone("test_signal_from_S3"), [3])]
        )
        # UNLINKED !
        # new_signal_link_node.compensate_missing_links()

        route = Route(
            route.route_id,
            new_signal_link_node,
            route.output,
            route._output_dim_matrix,
            route.slots,
            False,
            route.execution_hook,
            route.pending_node_ttl_in_secs,  # seconds
            route.pending_node_hook,
        )

        # since second input is locked on 3, this event can NOT yield a zombie node since they are unlinked
        # 1 != 3
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        assert not response.new_execution_contexts
        assert len(response.new_pending_nodes) == 1
        assert len(route._pending_nodes) == 1
        assert not next(iter(response.new_pending_nodes)).is_zombie

    def test_route_zombie_node_not_possible_when_other_is_a_materialized_reference_even_if_inputs_linked(self):
        """Actually yields execution immediately since the second input is a materialized reference"""
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal

        route = self._route_2_two_inputs_linked()

        # create new route to make sure that the second input is already materialized on value 3 [for dim_1_1]!
        new_signal_link_node = SignalLinkNode(
            [
                TestSignal.signal_internal_1,
                # materialized reference input
                create_incoming_signal(TestSignal.signal_s3_1.clone("test_signal_from_S3").as_reference(), [3]),
            ]
        )
        # LINK !
        new_signal_link_node.compensate_missing_links()

        route = Route(
            route.route_id,
            new_signal_link_node,
            route.output,
            route._output_dim_matrix,
            route.slots,
            False,
            route.execution_hook,
            route.pending_node_ttl_in_secs,
            route.pending_node_hook,
        )

        # although second input is locked on 3, this event can NOT yield a zombie node since it is a material reference.
        # 1 != 3
        response = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        # yields execution !
        assert response.new_execution_contexts
        assert len(response.new_pending_nodes) == 0
        assert len(route._pending_nodes) == 0

        # DONE
        # We are actually done but let's show that even if they are unlinked, the result would not change.
        new_signal_link_node = SignalLinkNode(
            [
                TestSignal.signal_internal_1,
                # materialized reference input
                create_incoming_signal(TestSignal.signal_s3_1.clone("test_signal_from_S3").as_reference(), [3]),
            ]
        )
        # UNLINK !
        # new_signal_link_node.compensate_missing_links()
        route = Route(
            route.route_id,
            new_signal_link_node,
            route.output,
            route._output_dim_matrix,
            route.slots,
            False,
            route.execution_hook,
            route.pending_node_ttl_in_secs,
            route.pending_node_hook,
        )

        response = route.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        # yields execution again!
        assert response.new_execution_contexts
        assert len(response.new_pending_nodes) == 0
        assert len(route._pending_nodes) == 0
