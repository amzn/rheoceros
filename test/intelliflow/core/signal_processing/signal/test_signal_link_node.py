# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal
from typing import cast

import pytest

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.dimension_constructs import DimensionVariantFactory
from intelliflow.core.signal_processing.signal import *
from intelliflow.core.signal_processing.signal_source import InternalDatasetSignalSourceAccessSpec


def signal_dimension_tuple(signal: Signal, dimension_name_or_variant: Union[str, DimensionVariant]) -> SignalDimensionTuple:
    if signal:
        signal: Signal = signal.clone(None)
        dim: Optional[Dimension] = signal.domain_spec.dimension_spec.find_dimension_by_name(
            dimension_name_or_variant if isinstance(dimension_name_or_variant, str) else dimension_name_or_variant.name
        )
        if not dim:
            raise ValueError(f"Cannot find dimension {dimension_name_or_variant} in {signal!r}")
        return SignalDimensionTuple(signal, dim)
    else:
        return SignalDimensionTuple(
            None, Dimension(dimension_name_or_variant, None) if isinstance(dimension_name_or_variant, str) else dimension_name_or_variant
        )


class TestSignalLinkNode:

    signal_link_node_1 = SignalLinkNode([TestSignal.signal_internal_1])
    signal_link_node_1_cloned = copy.deepcopy(signal_link_node_1)

    signal_link_node_2 = SignalLinkNode([TestSignal.signal_internal_1, TestSignal.signal_s3_1.clone("test_signal_from_S3")])
    signal_link_node_2_without_link = copy.deepcopy(signal_link_node_2)
    signal_link_node_2_with_non_trivial_link = copy.deepcopy(signal_link_node_2)
    signal_link_node_2.add_link(
        SignalDimensionLink(
            signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
            DIMENSION_VARIANT_IDENTICAL_MAP_FUNC,
            signal_dimension_tuple(TestSignal.signal_s3_1, "dim_1_1"),
        )
    )
    signal_link_node_2_with_non_trivial_link.add_link(
        SignalDimensionLink(
            signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
            lambda dim_1_1: dim_1_1 + 1,
            signal_dimension_tuple(TestSignal.signal_s3_1, "dim_1_1"),
        )
    )

    signal_link_node_3_complex = SignalLinkNode(
        [
            TestSignal.signal_internal_complex_1,
            TestSignal.signal_internal_complex_1.clone("upstream_data_ranged").filter({"*": {"_:-2": {}}}, transfer_spec=True),
            TestSignal.signal_s3_1,
        ]
    )

    # non-trivial N-1 mapping (despite that the mapper func is trivial equality)
    signal_link_node_3_complex_with_non_trivial_link = copy.deepcopy(signal_link_node_3_complex)
    signal_link_node_3_complex_with_non_trivial_link.add_link(
        SignalDimensionLink(
            signal_dimension_tuple(TestSignal.signal_s3_1, "dim_1_1"),
            lambda a, b: a,
            SignalDimensionTuple(
                TestSignal.signal_internal_complex_1,
                # multiple dimensions
                TestSignal.signal_internal_complex_1.domain_spec.dimension_spec.find_dimension_by_name("dim_1_1"),
                TestSignal.signal_internal_complex_1.domain_spec.dimension_spec.find_dimension_by_name("dim_1_2"),
            ),
        )
    )

    signal_link_node_3_complex_with_non_trivial_link_diff_order = SignalLinkNode(
        [
            TestSignal.signal_internal_complex_1,
            TestSignal.signal_s3_1,
            TestSignal.signal_internal_complex_1.clone("upstream_data_ranged").filter({"*": {"_:-2": {}}}, transfer_spec=True),
        ]
    )
    signal_link_node_3_complex_with_non_trivial_link_diff_order.add_link(
        SignalDimensionLink(
            signal_dimension_tuple(TestSignal.signal_s3_1, "dim_1_1"),
            lambda a, b: a,
            SignalDimensionTuple(
                TestSignal.signal_internal_complex_1.clone(None),
                # multiple dimensions
                TestSignal.signal_internal_complex_1.domain_spec.dimension_spec.find_dimension_by_name("dim_1_1"),
                TestSignal.signal_internal_complex_1.domain_spec.dimension_spec.find_dimension_by_name("dim_1_2"),
            ),
        )
    )

    signal_link_node_3_complex_with_non_trivial_link_2 = SignalLinkNode(
        [
            TestSignal.signal_s3_1,
            TestSignal.signal_internal_complex_1,
            TestSignal.signal_internal_complex_1.clone("upstream_data_ranged").filter({"*": {"_:-2": {}}}, transfer_spec=True),
        ]
    )
    # reverse the direction for non-trivial link
    signal_link_node_3_complex_with_non_trivial_link_2.add_link(
        SignalDimensionLink(
            signal_dimension_tuple(TestSignal.signal_internal_complex_1, "dim_1_1"),
            lambda a: a + a,
            signal_dimension_tuple(TestSignal.signal_s3_1, "dim_1_1"),
        )
    )

    def test_signal_link_node_init(self):
        SignalLinkNode([TestSignal.signal_internal_1])
        # will raise due to same alias on the same signal
        with pytest.raises(ValueError):
            SignalLinkNode([TestSignal.signal_internal_1, TestSignal.signal_internal_1_cloned])

        # raise again due to same alias'
        with pytest.raises(ValueError):
            SignalLinkNode([TestSignal.signal_internal_1, TestSignal.signal_s3_1])

        # even if the alias' None, underlying signal cannot be resolved to be unique
        with pytest.raises(ValueError):
            SignalLinkNode([TestSignal.signal_internal_1.clone(None), TestSignal.signal_internal_1.clone(None)])

        # even if the domain_spec::filter is different, the resolve issue is still there since Signal::__eq__ does not
        # use the filter during that check. they still treated to be same
        with pytest.raises(ValueError):
            SignalLinkNode([TestSignal.signal_internal_1.clone(None), TestSignal.signal_internal_1.clone(None).filter(DimensionFilter())])

        # however this case is valid since signals will be resolved to be unique
        SignalLinkNode([TestSignal.signal_internal_1.clone(None), TestSignal.signal_s3_1.clone(None)])

        # should be fine now as the second signal is inputted with a different alias
        link_node = SignalLinkNode([TestSignal.signal_internal_1, TestSignal.signal_internal_1_cloned.clone("test_signal_2")])
        assert not link_node.link_matrix
        assert link_node.signals == [TestSignal.signal_internal_1, TestSignal.signal_internal_1_cloned.clone("test_signal_2")]

        assert len(self.signal_link_node_2.link_matrix) == 1

    def test_signal_link_node_equality(self):
        assert SignalLinkNode([]) == SignalLinkNode([])
        assert self.signal_link_node_1 == SignalLinkNode([TestSignal.signal_internal_1])
        assert self.signal_link_node_1 == self.signal_link_node_1_cloned
        assert self.signal_link_node_1 != self.signal_link_node_2
        # diff in the equality of underlying Signals will cause diff at the node level. So 'alias' change is enough.
        assert self.signal_link_node_1 != SignalLinkNode([TestSignal.signal_internal_1.clone("dummt_alias")])
        # even if the alias is same, signals are not same
        assert self.signal_link_node_1 != SignalLinkNode([TestSignal.signal_s3_1])

    def test_signal_link_node_serialization(self):
        assert self.signal_link_node_1 == loads(dumps(self.signal_link_node_1))
        assert self.signal_link_node_2 == loads(dumps(self.signal_link_node_2))

    def test_signal_link_node_check_integrity(self):
        assert self.signal_link_node_1.check_integrity(self.signal_link_node_1_cloned)
        assert not self.signal_link_node_1.check_integrity(self.signal_link_node_2)

        # change the underlying filter of the signal and cause a diff in integrity
        assert not self.signal_link_node_1.check_integrity(SignalLinkNode([TestSignal.signal_internal_1.filter(DimensionFilter())]))

        # check the effect of dimension link matrix
        assert not self.signal_link_node_2.check_integrity(self.signal_link_node_2_without_link)

    def test_signal_link_node_add_link(self):
        signal_link_node_2_cloned = copy.deepcopy(self.signal_link_node_2)
        signal_link_node_2_cloned.add_link(
            SignalDimensionLink(
                signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
                lambda x: x.capitalize(),
                signal_dimension_tuple(TestSignal.signal_s3_1, "dim_1_1"),
            )
        )

        # raise due to bad link that has an unrecognized source signal (TestSignal.signal_andes_1)
        with pytest.raises(ValueError):
            signal_link_node_2_cloned.add_link(
                SignalDimensionLink(
                    signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
                    lambda x: x.capitalize(),
                    signal_dimension_tuple(TestSignal.signal_andes_1, "dim_1_1"),
                )
            )

    def test_signal_link_node_get_dim_mappers(self):
        mappers: List[DimensionVariantMapper] = self.signal_link_node_2.get_dim_mappers(
            from_signal=TestSignal.signal_internal_1, to_signal=TestSignal.signal_s3_1
        )
        assert len(mappers) == 1
        mappers = self.signal_link_node_2.get_dim_mappers(from_signal=TestSignal.signal_s3_1, to_signal=TestSignal.signal_internal_1)
        assert len(mappers) == 1
        assert not self.signal_link_node_2.get_dim_mappers(from_signal=None, to_signal=TestSignal.signal_internal_1)
        assert not self.signal_link_node_2.get_dim_mappers(from_signal=TestSignal.signal_s3_1, to_signal=None)
        assert not self.signal_link_node_2.get_dim_mappers(from_signal=None, to_signal=None)

    def test_signal_link_node_get_output_filter(self):
        # 0
        assert DimensionFilter.check_equivalence(
            DimensionFilter(),
            self.signal_link_node_2.get_output_filter(
                DimensionSpec(),
                # Logical equivalent -> output_dim = str(signal_internal_1('dim_1_1'))
                [
                    SignalDimensionLink(
                        signal_dimension_tuple(None, "output_dim"),
                        lambda x: f"{x}_str",
                        signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
                    )
                ],
            ),
        )

        output_spec = DimensionSpec.load_from_pretty({"output_dim": {type: Type.STRING}})
        # 1
        output_filter = self.signal_link_node_2.get_output_filter(
            output_spec,
            # Logical equivalent -> output_dim = str(signal_internal_1('dim_1_1'))
            [
                SignalDimensionLink(
                    signal_dimension_tuple(None, "output_dim"),
                    lambda x: f"{x}_str",
                    signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
                )
            ],
        )
        assert DimensionFilter.check_equivalence(output_filter, DimensionFilter.load_raw({"*": {type: Type.STRING}}))

        # 2
        output_filter = self.signal_link_node_2_without_link.get_output_filter(
            output_spec,
            # Logical equivalent -> output_dim = str(signal_internal_1('dim_1_1'))
            [
                SignalDimensionLink(
                    signal_dimension_tuple(None, "output_dim"),
                    lambda x: f"{x}_str",
                    signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
                )
            ],
        )

        # expect the same result (link on inputs won't change anything here from output's perspective)
        assert DimensionFilter.check_equivalence(output_filter, DimensionFilter.load_raw({"*": {type: Type.STRING}}))

        # 3
        output_filter = self.signal_link_node_1.get_output_filter(
            output_spec,
            # Logical equivalent -> output_dim = str(signal_internal_1('dim_1_1'))
            [
                SignalDimensionLink(
                    signal_dimension_tuple(None, "output_dim"),
                    lambda x: f"{x}_str",
                    signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
                )
            ],
        )

        # expect the same result
        assert DimensionFilter.check_equivalence(output_filter, DimensionFilter.load_raw({"*": {type: Type.STRING}}))

        signal_link_node_1_relative = SignalLinkNode(
            [TestSignal.signal_internal_1.filter(DimensionFilter.load_raw({"_:-3": {}}), transfer_spec=True)]
        )

        # 5 incompatible spec and dimension 'dynamic_dim'
        with pytest.raises(ValueError):
            signal_link_node_1_relative.get_output_filter(
                output_spec,
                [
                    SignalDimensionLink(
                        signal_dimension_tuple(None, "dynamic_dim"),
                        lambda x: f"{x}_str",
                        signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
                    )
                ],
            )
        # cannot assign from Dimension to Dimension
        # source should be DimensionVariant if it represents a literal value
        with pytest.raises(AttributeError):
            signal_link_node_1_relative.get_output_filter(
                output_spec,
                [
                    # intent: Logical equivalant -> region = 'NA'
                    SignalDimensionLink(
                        signal_dimension_tuple(None, "region"),
                        lambda x: x,
                        # this should be a DimensionVariant!
                        signal_dimension_tuple(None, "NA"),
                    )
                ],
            )

        complex_output_spec = DimensionSpec.load_from_pretty({"region": {type: Type.STRING, "dynamic_dim": {type: Type.STRING}}})

        # 4 output from relative input (to 'dynamic_dim' dimension) and literal value 'NA' (to new root level dimension 'region')
        output_filter = signal_link_node_1_relative.get_output_filter(
            complex_output_spec,
            # Logical equivalent -> output_dim = str(signal_internal_1('dim_1_1'))
            [
                SignalDimensionLink(
                    signal_dimension_tuple(None, "dynamic_dim"),
                    lambda x: f"{x}_str",
                    signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
                ),
                # Logical equivalant -> region = 'NA'
                SignalDimensionLink(
                    signal_dimension_tuple(None, "region"),
                    lambda x: x,
                    signal_dimension_tuple(None, DimensionVariantFactory.create_variant("NA", {Dimension.NAME_FIELD_ID: "region"})),
                ),
            ],
        )

        # expect the same result (thanks to relative dimension coalescence for output)
        assert DimensionFilter.check_equivalence(output_filter, DimensionFilter.load_raw({"NA": {"*": {type: Type.STRING}}}))

    def test_signal_link_node_get_materialized_inputs_for_output(self):
        output = TestSignal.signal_internal_1.filter(DimensionFilter.load_raw({1: {}}), transfer_spec=True)

        # fails due to no mappings + input is not material
        with pytest.raises(ValueError):
            self.signal_link_node_1.get_materialized_inputs_for_output(output, [], auto_bind_unlinked_input_dimensions=False)

        materialized_inputs = self.signal_link_node_1.get_materialized_inputs_for_output(
            output, [], auto_bind_unlinked_input_dimensions=True
        )
        assert DimensionFilter.check_equivalence(
            materialized_inputs[0].domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {}})
        )

        signal_link_node_with_materialized_input = SignalLinkNode(
            [TestSignal.signal_internal_1.filter(DimensionFilter.load_raw({2: {}}), transfer_spec=True)]
        )

        # there is no link between the output and the input but the input is already materialized, return it.
        materialized_inputs = signal_link_node_with_materialized_input.get_materialized_inputs_for_output(output, [])
        assert len(materialized_inputs) == 1
        assert DimensionFilter.check_equivalence(
            materialized_inputs[0].domain_spec.dimension_filter_spec, DimensionFilter.load_raw({2: {}})
        )

        # now materialized input signal and output are linked but materialized output (as an input to this API)
        # is not within the filtering domain of input. Input value -> 2 whereas output -> 1
        # In this particular case, RheocerOS evaluates it as a TypeError (filtering spec mismatch).
        with pytest.raises(TypeError):
            materialized_inputs = signal_link_node_with_materialized_input.get_materialized_inputs_for_output(
                output,
                [
                    SignalDimensionLink(
                        signal_dimension_tuple(None, "dim_1_1"),
                        lambda x: x,
                        signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
                    )
                ],
            )

            # same thing with an input that has empty filter (no dimensions)
        signal_link_node_with_materialized_input = SignalLinkNode([TestSignal.signal_internal_with_no_dimensions])
        # there is no link between the output and the input but the input is already materialized, return it.
        materialized_inputs = signal_link_node_with_materialized_input.get_materialized_inputs_for_output(output, [])
        assert DimensionFilter.check_equivalence(materialized_inputs[0].domain_spec.dimension_filter_spec, DimensionFilter())

        materialized_inputs = self.signal_link_node_1.get_materialized_inputs_for_output(
            output,
            [
                SignalDimensionLink(
                    signal_dimension_tuple(None, "dim_1_1"), lambda x: x, signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1")
                )
            ],
        )
        assert DimensionFilter.check_equivalence(
            materialized_inputs[0].domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {}})
        )

        with pytest.raises(ValueError):
            # RheocerOS won't be able to map to the input dim since the function is not equality.
            self.signal_link_node_1.get_materialized_inputs_for_output(
                output,
                [
                    SignalDimensionLink(
                        signal_dimension_tuple(None, "dim_1_1"),
                        # NOT EQUALITY
                        lambda x: x * x,
                        signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1"),
                    )
                ],
            )

    def test_signal_link_node_get_materialized_inputs_for_output_transitive(self):
        output = TestSignal.signal_internal_complex_1.filter(
            DimensionFilter.load_raw({1: {"NA": {}}}), new_alias="output_data", transfer_spec=True
        )

        signal_link_node_3_complex_inputs_linked = copy.deepcopy(self.signal_link_node_3_complex)
        signal_link_node_3_complex_inputs_linked.compensate_missing_links()

        hybrid_output_dim_matrix = [
            SignalDimensionLink(
                signal_dimension_tuple(None, "dim_1_1"), lambda x: x, signal_dimension_tuple(TestSignal.signal_s3_1, "dim_1_1")
            ),
            SignalDimensionLink(
                signal_dimension_tuple(None, "dim_1_2"),
                lambda x: x,
                signal_dimension_tuple(TestSignal.signal_internal_complex_1, "dim_1_2"),
            ),
        ]

        # output had a hybrid relationship. one dimension from the first signal and the other from the second.
        # since the inputs are linked. first dimension of the second signal 'signal_internal_complex_1' will be
        # mapped from fully materialized 'signal_s3_1' (first input).
        materialized_inputs = signal_link_node_3_complex_inputs_linked.get_materialized_inputs_for_output(output, hybrid_output_dim_matrix)
        assert len(materialized_inputs) == 3
        assert DimensionFilter.check_equivalence(
            materialized_inputs[0].domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {}})
        )
        assert DimensionFilter.check_equivalence(
            materialized_inputs[1].domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {"NA": {}}})
        )
        assert DimensionFilter.check_equivalence(
            materialized_inputs[2].domain_spec.dimension_filter_spec,
            DimensionFilter.load_raw({1: {"NA": {}, "MZ": {}}}),  # 'upstream_data_ranged'
        )  # alphanumerial 'NA - 1' = 'MZ'

        # now create the same node without links this time.
        # show the effect of 'auto_bind_unlinked_input_dimensions'. if it is not enabled then second signal cannot be
        # materialized, since its first dimension 'dim_1_1' is dangling.
        signal_link_node_3_complex_cloned = copy.deepcopy(self.signal_link_node_3_complex)

        with pytest.raises(ValueError):
            # output cannot fully map to/materialize any of the inputs and also inputs are not linked.
            # if 'auto_binding' is also disabled, then impossible to compensate missing dimensions.
            materialized_inputs = signal_link_node_3_complex_cloned.get_materialized_inputs_for_output(
                output, hybrid_output_dim_matrix, auto_bind_unlinked_input_dimensions=False
            )

        materialized_inputs = signal_link_node_3_complex_cloned.get_materialized_inputs_for_output(
            output, hybrid_output_dim_matrix, auto_bind_unlinked_input_dimensions=True
        )
        assert len(materialized_inputs) == 3
        assert DimensionFilter.check_equivalence(
            materialized_inputs[0].domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {}})
        )
        assert DimensionFilter.check_equivalence(
            materialized_inputs[1].domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {"NA": {}}})
        )
        assert DimensionFilter.check_equivalence(
            materialized_inputs[2].domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {"NA": {}, "MZ": {}}})
        )

    def test_signal_link_node_get_materialized_inputs_for_output__missing_dims_already_materialized(self):
        output = TestSignal.signal_internal_complex_1.filter(
            DimensionFilter.load_raw({1: {"NA": {}}}), new_alias="output_data", transfer_spec=True
        )

        signal_node = SignalLinkNode(
            [
                # first dim 'dim_1_1' of input 1 is materialized but it will be missing from the output dim matrix.
                TestSignal.signal_internal_complex_1.filter(DimensionFilter.load_raw({1: {"*": {}}}), transfer_spec=True),
                TestSignal.signal_s3_1,
            ]
        )

        hybrid_output_dim_matrix = [
            SignalDimensionLink(
                signal_dimension_tuple(None, "dim_1_1"), lambda x: x, signal_dimension_tuple(TestSignal.signal_s3_1, "dim_1_1")
            ),
            SignalDimensionLink(
                signal_dimension_tuple(None, "dim_1_2"),
                lambda x: x,
                signal_dimension_tuple(TestSignal.signal_internal_complex_1, "dim_1_2"),
            ),
        ]

        # so even if 'auto_bind_unlinked_input_dimensions' is False, missing dim is already materialized. so
        # everythin should be ok.
        materialized_inputs = signal_node.get_materialized_inputs_for_output(
            output, hybrid_output_dim_matrix, auto_bind_unlinked_input_dimensions=False
        )
        assert len(materialized_inputs) == 2
        assert DimensionFilter.check_equivalence(
            materialized_inputs[0].domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {}})
        )
        assert DimensionFilter.check_equivalence(
            materialized_inputs[1].domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {"NA": {}}})
        )

    def test_signal_link_node_get_materialized_inputs_for_output__missing_dims_alien(self):
        from test.intelliflow.core.signal_processing.dimension_constructs.test_dimension_filter import TestDimensionFilter

        dimension_spec_branch_lvl_2_with_alien_dim = DimensionSpec(
            [Dimension("alien_dim", Type.LONG)], [DimensionSpec([Dimension("dim_1_2", Type.STRING)], [None])]
        )
        filter = copy.deepcopy(TestDimensionFilter.dimension_filter_branch_1)
        filter.set_spec(dimension_spec_branch_lvl_2_with_alien_dim)
        signal_internal_complex_1_with_alien_dim = Signal(
            SignalType.INTERNAL_PARTITION_CREATION,
            InternalDatasetSignalSourceAccessSpec("upstream_data_1", dimension_spec_branch_lvl_2_with_alien_dim, **{}),
            SignalDomainSpec(
                dimension_spec_branch_lvl_2_with_alien_dim, filter, SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
            ),
            "upstream_data_1",
            False,
        )

        output = TestSignal.signal_internal_complex_1.filter(
            DimensionFilter.load_raw({1: {"NA": {}}}), new_alias="output_data", transfer_spec=True
        )

        signal_node = SignalLinkNode(
            [
                # first dim 'dim_1_1' of input 1 is materialized but it will be missing from the output dim matrix.
                signal_internal_complex_1_with_alien_dim,
                TestSignal.signal_s3_1,
            ]
        )

        hybrid_output_dim_matrix = [
            SignalDimensionLink(
                signal_dimension_tuple(None, "dim_1_1"), lambda x: x, signal_dimension_tuple(TestSignal.signal_s3_1, "dim_1_1")
            ),
            SignalDimensionLink(
                signal_dimension_tuple(None, "dim_1_2"),
                lambda x: x,
                signal_dimension_tuple(signal_internal_complex_1_with_alien_dim, "dim_1_2"),
            ),
        ]
        # at this point 'alien_dim' of 'signal_internal_complex_1_with_alien_dim' has no connections within
        # input or output dim matrices.
        # so irrespective of 'auto_bind_unlinked_input_dimensions', materialization should fail!
        with pytest.raises(ValueError):
            materialized_inputs = signal_node.get_materialized_inputs_for_output(
                output, hybrid_output_dim_matrix, auto_bind_unlinked_input_dimensions=False
            )

        with pytest.raises(ValueError):
            materialized_inputs = signal_node.get_materialized_inputs_for_output(
                output, hybrid_output_dim_matrix, auto_bind_unlinked_input_dimensions=True
            )

        signal_node.compensate_missing_links()

        # same results irrespective of whether links exist on the other dim or not
        with pytest.raises(ValueError):
            materialized_inputs = signal_node.get_materialized_inputs_for_output(
                output, hybrid_output_dim_matrix, auto_bind_unlinked_input_dimensions=False
            )

        with pytest.raises(ValueError):
            materialized_inputs = signal_node.get_materialized_inputs_for_output(
                output, hybrid_output_dim_matrix, auto_bind_unlinked_input_dimensions=True
            )

    def test_signal_link_node_compensate_missing_links(self):
        assert not self.signal_link_node_2_without_link.link_matrix
        cloned_node = copy.deepcopy(self.signal_link_node_2_without_link)

        cloned_node.compensate_missing_links()

        assert len(cloned_node.link_matrix) == 1
        auto_link = cloned_node.link_matrix[0]
        assert auto_link.lhs_dim.dimensions == auto_link.rhs_dim.dimensions
        assert auto_link.dim_link_func.__code__.co_code == DIMENSION_VARIANT_IDENTICAL_MAP_FUNC.__code__.co_code

        # changes nothing on a node with only one (input) signal
        cloned_node = copy.deepcopy(self.signal_link_node_1)
        cloned_node.compensate_missing_links()
        assert not cloned_node.link_matrix

        # TODO test missing dimensions
        # signal_link_node_2 = SignalLinkNode([TestSignal.signal_internal_1, TestSignal.signal_s3_1.clone('test_signal_from_S3')])

        cloned_complex_node = copy.deepcopy(self.signal_link_node_3_complex)
        cloned_complex_node.compensate_missing_links()
        # 4 links should be created between;
        # - two common dimensions of input1 and input2
        # - one common dimension between input3 and input1, and input2 and input3
        assert len(cloned_complex_node.link_matrix) == 4
        assert not cloned_complex_node.get_dim_mappers(from_signal=None, to_signal=None)
        assert not cloned_complex_node.get_dim_mappers(from_signal=None, to_signal=TestSignal.signal_internal_complex_1)
        assert not cloned_complex_node.get_dim_mappers(
            from_signal=None, to_signal=TestSignal.signal_internal_complex_1.clone("upstream_data_ranged")
        )
        assert not cloned_complex_node.get_dim_mappers(from_signal=TestSignal.signal_internal_complex_1, to_signal=None)
        assert not cloned_complex_node.get_dim_mappers(
            from_signal=TestSignal.signal_internal_complex_1.clone("upstream_data_ranged"), to_signal=None
        )
        assert (
            len(
                cloned_complex_node.get_dim_mappers(
                    from_signal=TestSignal.signal_internal_complex_1,
                    to_signal=TestSignal.signal_internal_complex_1.clone("upstream_data_ranged"),
                )
            )
            == 2
        )
        assert (
            len(
                cloned_complex_node.get_dim_mappers(
                    from_signal=TestSignal.signal_internal_complex_1.clone("upstream_data_ranged"),
                    to_signal=TestSignal.signal_internal_complex_1,
                )
            )
            == 2
        )
        assert (
            len(
                cloned_complex_node.get_dim_mappers(
                    from_signal=TestSignal.signal_internal_complex_1.clone("upstream_data_ranged"), to_signal=TestSignal.signal_s3_1
                )
            )
            == 1
        )
        assert (
            len(
                cloned_complex_node.get_dim_mappers(
                    from_signal=TestSignal.signal_s3_1, to_signal=TestSignal.signal_internal_complex_1.clone("upstream_data_ranged")
                )
            )
            == 1
        )
        assert (
            len(cloned_complex_node.get_dim_mappers(from_signal=TestSignal.signal_internal_complex_1, to_signal=TestSignal.signal_s3_1))
            == 1
        )
        assert (
            len(cloned_complex_node.get_dim_mappers(from_signal=TestSignal.signal_s3_1, to_signal=TestSignal.signal_internal_complex_1))
            == 1
        )

    def test_signal_link_node_compensate_missing_links_avoid_nontrivial_reverse_links(self):
        # does not add redundant links over existing non-trivial links
        cloned_node = copy.deepcopy(self.signal_link_node_2_with_non_trivial_link)
        assert len(cloned_node.link_matrix) == 1
        cloned_node.compensate_missing_links()
        # nothing changes
        assert len(cloned_node.link_matrix) == 1
        assert len(cloned_node.get_dim_mappers(from_signal=TestSignal.signal_s3_1, to_signal=TestSignal.signal_internal_1)) == 1

        # use a node with three inputs
        # - input1:
        # - input2 (aliased version of input1)
        # - input3
        # non-trivial link is from input1 to input3
        # there should not be auto-link between input2-input3 and also from input3 to input1 due to no-trivial link from
        # input1 to input3.
        link_node_permutations = [
            self.signal_link_node_3_complex_with_non_trivial_link,
            self.signal_link_node_3_complex_with_non_trivial_link_diff_order,
        ]
        for link_node in link_node_permutations:
            cloned_node = copy.deepcopy(link_node)
            assert len(cloned_node.link_matrix) == 1
            cloned_node.compensate_missing_links()
            # only two more links between the two versions of "signal_internal_complex_1" is added.
            # not anything from signal_s3_1("dim_1_1") back to any of those signals (with different alias)
            assert len(cloned_node.link_matrix) == 3
            assert len(cloned_node.get_dim_mappers(from_signal=TestSignal.signal_internal_complex_1, to_signal=TestSignal.signal_s3_1)) == 1
            assert (
                len(
                    cloned_node.get_dim_mappers(
                        from_signal=TestSignal.signal_internal_complex_1,
                        to_signal=TestSignal.signal_internal_complex_1.clone("upstream_data_ranged"),
                    )
                )
                == 2
            )
            assert (
                len(
                    cloned_node.get_dim_mappers(
                        from_signal=TestSignal.signal_internal_complex_1.clone("upstream_data_ranged"),
                        to_signal=TestSignal.signal_internal_complex_1,
                    )
                )
                == 2
            )
            # and due to non-trival connection, the reverse link should not be automatically established
            assert not cloned_node.get_dim_mappers(from_signal=TestSignal.signal_s3_1, to_signal=TestSignal.signal_internal_complex_1)
            assert not cloned_node.get_dim_mappers(
                from_signal=TestSignal.signal_s3_1, to_signal=TestSignal.signal_internal_complex_1.clone("upstream_data_ranged")
            )

        cloned_node = copy.deepcopy(self.signal_link_node_3_complex_with_non_trivial_link_2)
        assert len(cloned_node.link_matrix) == 1
        cloned_node.compensate_missing_links()
        # only two more links between the two versions of "signal_internal_complex_1" is added.
        # not anything from any of those back onto signal_s3_1("dim_1_1")
        assert len(cloned_node.link_matrix) == 3
        assert len(cloned_node.get_dim_mappers(from_signal=TestSignal.signal_internal_complex_1, to_signal=TestSignal.signal_s3_1)) == 0
        assert len(cloned_node.get_dim_mappers(from_signal=TestSignal.signal_s3_1, to_signal=TestSignal.signal_internal_complex_1)) == 1
        assert (
            len(
                cloned_node.get_dim_mappers(
                    from_signal=TestSignal.signal_s3_1, to_signal=TestSignal.signal_internal_complex_1.clone("upstream_data_ranged")
                )
            )
            == 1
        )
        assert (
            len(
                cloned_node.get_dim_mappers(
                    from_signal=TestSignal.signal_internal_complex_1,
                    to_signal=TestSignal.signal_internal_complex_1.clone("upstream_data_ranged"),
                )
            )
            == 2
        )

    def test_signal_link_node_can_receive(self):
        assert self.signal_link_node_1.can_receive(TestSignal.signal_internal_1)
        assert self.signal_link_node_1.can_receive(TestSignal.signal_internal_1.clone(None))
        assert not self.signal_link_node_1.can_receive(TestSignal.signal_internal_1.clone("diff_alias"))
        assert not self.signal_link_node_1.can_receive(TestSignal.signal_s3_1)
