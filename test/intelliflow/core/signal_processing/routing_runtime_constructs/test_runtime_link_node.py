# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from test.intelliflow.core.signal_processing.routing_runtime_constructs import create_incoming_signal
from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal
from test.intelliflow.core.signal_processing.signal.test_signal_link_node import TestSignalLinkNode, signal_dimension_tuple
from typing import cast

import pytest

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.routing_runtime_constructs import *
from intelliflow.core.signal_processing.signal import *
from intelliflow.core.signal_processing.signal_source import InternalDatasetSignalSourceAccessSpec


class TestRuntimeLinkNode:
    runtime_link_node_1 = RuntimeLinkNode(TestSignalLinkNode.signal_link_node_1)
    runtime_link_node_2 = RuntimeLinkNode(TestSignalLinkNode.signal_link_node_2)
    runtime_link_node_3_complex = RuntimeLinkNode(TestSignalLinkNode.signal_link_node_3_complex)

    def test_runtime_link_node_init(self):
        RuntimeLinkNode(SignalLinkNode([]))
        RuntimeLinkNode(TestSignalLinkNode.signal_link_node_2_without_link)
        node = RuntimeLinkNode(TestSignalLinkNode.signal_link_node_1)
        assert not node.ready_signals
        assert node.node_id
        assert not node.is_ready()

    def test_runtime_link_node_serialization(self):
        deserialized_node: RuntimeLinkNode = loads(dumps(self.runtime_link_node_2))
        assert deserialized_node.link_matrix == self.runtime_link_node_2.link_matrix

    def test_runtime_link_node_equality(self):
        assert self.runtime_link_node_1 == self.runtime_link_node_1
        assert self.runtime_link_node_1 != self.runtime_link_node_2
        assert RuntimeLinkNode(TestSignalLinkNode.signal_link_node_1) != self.runtime_link_node_1

    def test_runtime_link_node_receive_simple(self):
        runtime_link_node_1_cloned = copy.deepcopy(self.runtime_link_node_1)
        # create sample expected output
        output_spec = DimensionSpec.load_from_pretty({"output_dim": {type: Type.LONG}})

        output_dim_link_matrix = [
            SignalDimensionLink(
                signal_dimension_tuple(None, "output_dim"), lambda x: x, signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1")
            )
        ]
        output_filter = runtime_link_node_1_cloned.get_output_filter(
            output_spec,
            # Logical equivalent -> output_dim = (signal_internal_1('dim_1_1')
            output_dim_link_matrix,
        )
        output_signal = Signal(
            TestSignal.signal_internal_1.type,
            TestSignal.signal_internal_1.resource_access_spec,
            SignalDomainSpec(output_spec, output_filter, TestSignal.signal_internal_1.domain_spec.integrity_check_protocol),
            "sample_output_data",
        )

        # reject unknown signal
        assert not runtime_link_node_1_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [1]))
        assert not runtime_link_node_1_cloned.is_ready()
        assert not runtime_link_node_1_cloned.can_receive(TestSignal.signal_s3_1)

        # this should yield nothing since the node is not ready yet
        assert not runtime_link_node_1_cloned.materialize_output(output_signal, output_dim_link_matrix)

        is_consumed = runtime_link_node_1_cloned.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        assert is_consumed
        assert runtime_link_node_1_cloned.is_ready()

        # repetitive attempt to inject the same event. will return already 'consumed'
        assert runtime_link_node_1_cloned.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        assert runtime_link_node_1_cloned.receive(
            create_incoming_signal(TestSignal.signal_internal_1, "/internal_data/my_data_1/1/_SUCCESS")
        )
        assert runtime_link_node_1_cloned.is_ready()

        # another event (with dim value = 3) received but this node will reject it since it is already a
        # 'ready' pending node.
        assert not runtime_link_node_1_cloned.receive(create_incoming_signal(TestSignal.signal_internal_1, [3]))
        assert not runtime_link_node_1_cloned.receive(create_incoming_signal(TestSignal.signal_internal_1, [4]))

        # check the previously consumed event again
        assert runtime_link_node_1_cloned.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        # try to get the output signal
        materialized_output = runtime_link_node_1_cloned.materialize_output(output_signal, output_dim_link_matrix)
        assert materialized_output
        assert DimensionFilter.check_equivalence(materialized_output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {}}))

    def test_runtime_link_node_receive_two_inputs(self):
        runtime_link_node_2_cloned = copy.deepcopy(self.runtime_link_node_2)
        # create sample expected output
        output_spec = DimensionSpec.load_from_pretty({"output_dim": {type: Type.LONG}})

        output_dim_link_matrix = [
            SignalDimensionLink(
                signal_dimension_tuple(None, "output_dim"), lambda x: x, signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1")
            )
        ]
        output_filter = runtime_link_node_2_cloned.get_output_filter(output_spec, output_dim_link_matrix)
        output_signal = Signal(
            TestSignal.signal_internal_1.type,
            InternalDatasetSignalSourceAccessSpec("sample_output_data", output_spec, **{}),
            SignalDomainSpec(output_spec, output_filter, TestSignal.signal_internal_1.domain_spec.integrity_check_protocol),
            "sample_output_data",
        )
        assert not runtime_link_node_2_cloned.receive(create_incoming_signal(TestSignal.signal_andes_1, [1]))
        assert not runtime_link_node_2_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [1, "str"]))

        # 1/2- first succesfull injection
        assert runtime_link_node_2_cloned.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        # not ready yet because we have another input
        assert not runtime_link_node_2_cloned.is_ready()
        assert len(runtime_link_node_2_cloned.ready_signals) == 1

        # won't change anything
        assert runtime_link_node_2_cloned.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        assert not runtime_link_node_2_cloned.is_ready()

        # will be rejected since for the same signal an incoming signal has been consumed already
        assert not runtime_link_node_2_cloned.receive(create_incoming_signal(TestSignal.signal_internal_1, [999]))

        assert not runtime_link_node_2_cloned.materialize_output(output_signal, output_dim_link_matrix)

        # wrong signal again while waiting for the second input
        assert not runtime_link_node_2_cloned.receive(create_incoming_signal(TestSignal.signal_andes_1, [1]))
        assert not runtime_link_node_2_cloned.is_ready()

        # right input but wrong dim value [2] (against the rule enforced by the dim link matrix:
        #   TestSignal.signal_internal1('dim_1_1') == TestSignal.signal_s3_1('dim_1_1')
        assert not runtime_link_node_2_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [2]))
        # again wrong dim value [333] (it should be 1 since the first input has already been received with value [1])
        assert not runtime_link_node_2_cloned.receive(
            create_incoming_signal(TestSignal.signal_s3_1, "s3://bucket/my_data_2/333/PARTITION_READY")
        )
        assert not runtime_link_node_2_cloned.is_ready()
        assert len(runtime_link_node_2_cloned.ready_signals) == 1

        # 2/2- finally let's input the long-waited second signal
        assert runtime_link_node_2_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [1]))
        assert runtime_link_node_2_cloned.is_ready()
        assert len(runtime_link_node_2_cloned.ready_signals) == 2

        # Now the runtime node is good-to-go (pending) any further compatible events will just be consumed but wont impact
        # the internal state.
        assert runtime_link_node_2_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, "s3://bucket/my_data_2/1/PARTITION_READY"))
        assert runtime_link_node_2_cloned.receive(
            create_incoming_signal(TestSignal.signal_internal_1, "/internal_data/my_data_1/1/_SUCCESS")
        )

        assert len(runtime_link_node_2_cloned.ready_signals) == 2
        # try to get the output signal
        materialized_output = runtime_link_node_2_cloned.materialize_output(output_signal, output_dim_link_matrix)
        assert materialized_output
        assert DimensionFilter.check_equivalence(materialized_output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {}}))

    def test_runtime_link_node_receive_complex_unlinked(self):
        runtime_link_node_3_cloned = copy.deepcopy(self.runtime_link_node_3_complex)
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
        output_filter = runtime_link_node_3_cloned.get_output_filter(output_spec, output_dim_link_matrix)
        output_signal = Signal(
            SignalType.INTERNAL_PARTITION_CREATION,
            InternalDatasetSignalSourceAccessSpec("sample_output_data", output_spec, **{}),
            SignalDomainSpec(output_spec, output_filter, TestSignal.signal_internal_complex_1.domain_spec.integrity_check_protocol),
            "sample_output_data",
        )

        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_andes_1, [1]))
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        assert len(runtime_link_node_3_cloned.ready_signals) == 0

        # 1/3 - first successful injection
        assert runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [1]))
        assert len(runtime_link_node_3_cloned.ready_signals) == 1

        with pytest.raises(OverflowError):
            # check overflow on range expansion over second dimension value 'a' (its dimension filter has a relative
            # variant _:-2 on it, it will overflow.
            copy.deepcopy(runtime_link_node_3_cloned).receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "a"]))

        # 2/3 and 3/3 together since the first two inputs of this node is of same Signal (spanning different ranges only)
        assert runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "y"]))
        # please note that even if the partitions won't match, RheocerOS will bind the inputs and complete the node.
        assert runtime_link_node_3_cloned.is_ready()
        assert len(runtime_link_node_3_cloned.ready_signals) == 3

        # redundant injection attempts (will return consumed = False)
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [2]))
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [1, "s"]))

        # second dim value is different (will return consumed = False)
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "v"]))
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "z"]))

        # will return consumed since already consumed
        assert runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [1]))
        assert runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "y"]))
        assert runtime_link_node_3_cloned.is_ready()
        assert len(runtime_link_node_3_cloned.ready_signals) == 3

        # try to get the output signal
        materialized_output = runtime_link_node_3_cloned.materialize_output(output_signal, output_dim_link_matrix)
        assert materialized_output
        assert DimensionFilter.check_equivalence(
            materialized_output.domain_spec.dimension_filter_spec,
            DimensionFilter.load_raw({2: {121: {1: {}}}}),  # ord('y') from the second input  # from the 3rd input
        )

    def test_runtime_link_node_receive_complex_linked(self):
        runtime_link_node_3_cloned = copy.deepcopy(self.runtime_link_node_3_complex)

        # add links (since the dimension names on same, use the auto-linking of dimensions,
        # so that;
        #   signal_internal_complex_1['dim_1_1'] == signal_s3_1['dim_1_1'], etc
        runtime_link_node_3_cloned.compensate_missing_links()

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
        output_filter = runtime_link_node_3_cloned.get_output_filter(output_spec, output_dim_link_matrix)
        output_signal = Signal(
            SignalType.INTERNAL_PARTITION_CREATION,
            TestSignal.signal_internal_complex_1.resource_access_spec,
            SignalDomainSpec(output_spec, output_filter, TestSignal.signal_internal_complex_1.domain_spec.integrity_check_protocol),
            "sample_output_data",
        )

        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_andes_1, [1]))
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_1, [1]))
        assert len(runtime_link_node_3_cloned.ready_signals) == 0

        # 1/3 - first successful injection
        assert runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [1]))
        assert len(runtime_link_node_3_cloned.ready_signals) == 1

        # link check will be in effect. this will be rejected (different scenario than the prev test)
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "y"]))
        assert not runtime_link_node_3_cloned.is_ready()
        assert len(runtime_link_node_3_cloned.ready_signals) == 1

        # redundant injection attempts (will return consumed = False)
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [2]))
        # will return consumed = True but won't effect the internal state
        assert runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [1]))

        # 2/3, 3/3 - satisfiy link condition
        assert runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [1, "y"]))
        assert runtime_link_node_3_cloned.is_ready()
        assert len(runtime_link_node_3_cloned.ready_signals) == 3

        # will return consumed since already consumed
        assert runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [1]))
        assert runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [1, "y"]))
        assert runtime_link_node_3_cloned.is_ready()
        assert len(runtime_link_node_3_cloned.ready_signals) == 3

        # redundant injection attempt (will return consumed = False)
        # first dim does not match with the already consumed one
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "y"]))
        # first dim does not match with the already consumed one
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [1, "v"]))
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [1, "z"]))
        assert runtime_link_node_3_cloned.is_ready()
        assert len(runtime_link_node_3_cloned.ready_signals) == 3

        # try to get the output signal
        materialized_output = runtime_link_node_3_cloned.materialize_output(output_signal, output_dim_link_matrix)
        assert materialized_output
        assert DimensionFilter.check_equivalence(
            materialized_output.domain_spec.dimension_filter_spec,
            DimensionFilter.load_raw(
                {
                    121: {  # from tip [ ord('y') ] of the second dimension of first signal (used by 1st, 2nd inputs)
                        1: {}  # from the 3rd input
                    }
                }
            ),
        )

        # NOW start all over again, this time change incoming signal order and keep the flow simple
        runtime_link_node_3_cloned = copy.deepcopy(self.runtime_link_node_3_complex)
        runtime_link_node_3_cloned.compensate_missing_links()

        # 1/3, 2/3 - first successful injection (that satisfies first two inputs of same Signal)
        assert runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "t"]))
        assert len(runtime_link_node_3_cloned.ready_signals) == 2

        # unsatisfied link condition (dim value should be 2)
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [1]))
        # incompatible injection attempts against the first signal (first two inputs)
        # incompatible 1st dim
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [1, "t"]))
        # incompatible 2nd dim
        assert not runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_internal_complex_1, [2, "z"]))
        assert len(runtime_link_node_3_cloned.ready_signals) == 2

        # 3/3 - satisfiy link condition
        assert runtime_link_node_3_cloned.receive(create_incoming_signal(TestSignal.signal_s3_1, [2]))
        assert runtime_link_node_3_cloned.is_ready()
        assert len(runtime_link_node_3_cloned.ready_signals) == 3

        materialized_output = runtime_link_node_3_cloned.materialize_output(output_signal, output_dim_link_matrix)
        assert DimensionFilter.check_equivalence(
            materialized_output.domain_spec.dimension_filter_spec,
            DimensionFilter.load_raw(
                {
                    116: {  # from tip [ ord('t') ] of the second dimension of first signal (used by 1st, 2nd inputs)
                        2: {}  # from the 3rd input
                    }
                }
            ),
        )
