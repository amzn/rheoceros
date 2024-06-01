# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from collections import OrderedDict

import pytest

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.dimension_constructs import Dimension, DimensionSpec


class TestDimension:
    dim_long = Dimension("long dim", Type.LONG)
    dim_long_cloned = copy.deepcopy(dim_long)
    dim_string = Dimension("string dim", Type.STRING, None)
    dim_string_cloned = copy.deepcopy(dim_string)
    dim_datetime = Dimension("dim 3", Type.DATETIME, params={"format": "%Y-%m-%d"})
    dim_datetime_cloned = copy.deepcopy(dim_datetime)

    def test_dimension_init_and_api(self):
        Dimension("dim1", Type.LONG)
        Dimension("dim 2", Type.STRING, params={"noop_metadata1": "metadata value"})
        date_dim = Dimension("dim 3", Type.DATETIME, params={"format": "%Y-%m-%d"})

        assert date_dim.name == "dim 3"
        assert date_dim.type == Type.DATETIME
        assert date_dim.params == {"format": "%Y-%m-%d"}

        date_dim.name = "dim 3 1"
        date_dim.type = Type.LONG
        date_dim.params = None

        assert date_dim.name == "dim 3 1"
        assert date_dim.type == Type.LONG
        assert date_dim.params is None

    def test_dimension_serialization(self):
        assert self.dim_long == loads(dumps(self.dim_long))
        assert self.dim_string == loads(dumps(self.dim_string))
        assert self.dim_datetime == loads(dumps(self.dim_datetime))

    def test_dimension_equality(self):
        assert self.dim_long == self.dim_long_cloned
        assert self.dim_string == self.dim_string_cloned
        assert self.dim_datetime == self.dim_datetime_cloned

        assert {self.dim_long, self.dim_string, self.dim_datetime} != {self.dim_string}
        assert {self.dim_long, self.dim_string, self.dim_datetime} == {self.dim_string, self.dim_datetime, self.dim_long}

    def test_dimension_inequality(self):
        assert self.dim_long != self.dim_string
        assert self.dim_long != self.dim_datetime
        assert self.dim_string != self.dim_datetime

    def test_dimension_checks(self):
        assert Dimension.is_dimension(("dim", {}))
        assert Dimension.is_dimension(("dim", {"attr": "value"}))
        assert not Dimension.is_dimension(("attr", "value"))
        assert not Dimension.is_dimension((Dimension.TYPE_FIELD_ID, Type.LONG))
        assert not Dimension.is_dimension((type, Type.LONG))
        assert not Dimension.is_dimension((type, {}))

        assert Dimension.is_dimension_param(("attr", "value"))
        assert not Dimension.is_dimension_param(("dim", {}))


class TestDimensionSpec:
    dimension_spec_single_dim = DimensionSpec([Dimension("dim_1_1", Type.LONG)], [None])

    dimension_spec_branch_lvl_2 = DimensionSpec(
        [Dimension("dim_1_1", Type.LONG)], [DimensionSpec([Dimension("dim_1_2", Type.STRING)], [None])]
    )

    dimension_spec_tree_1 = copy.deepcopy(dimension_spec_branch_lvl_2).add_dimension(Dimension("dim_2_1", Type.DATETIME), None)

    dimension_spec_branch_lvl_2_string_and_datetime = DimensionSpec(
        [Dimension("dim_1_1", Type.STRING)], [DimensionSpec([Dimension("dim_1_2", Type.DATETIME, {"format": "%Y-%m-%d"})], [None])]
    )

    def test_dimension_spec_init(self):
        DimensionSpec()
        # single dimension - 1 level
        DimensionSpec([Dimension("dim1", Type.STRING)], [None])
        # two root dimensions, one nested dimension
        DimensionSpec(
            [Dimension("dim1", Type.LONG), Dimension("dim2", Type.STRING)],
            [DimensionSpec([Dimension("sub_param", Type.LONG)], [None]), None],
        )

    @pytest.mark.parametrize(
        "dimensions, sub_dim_specs",
        [
            ([Dimension("dim1", Type.DATETIME)], []),
            ([Dimension("dim1", Type.DATETIME)], None),
            # list sizes won't match
            ([Dimension("dim1", Type.DATETIME)], [None, None]),
            # duplicates (on the same branch)
            ([Dimension("dim1", Type.DATETIME)], [DimensionSpec([Dimension("dim1", Type.STRING)], [None])]),
            # duplicates (first one is nested on the first branch, then other one is the second root dimension)
            (
                [Dimension("dim1", Type.DATETIME), Dimension("dim1_1", Type.LONG)],
                [DimensionSpec([Dimension("dim1_1", Type.STRING)], [None]), None],
            ),
        ],
    )
    def test_dimension_spec_init_failure(self, dimensions, sub_dim_specs):
        with pytest.raises(Exception) as error:
            DimensionSpec(dimensions, sub_dim_specs)
        assert error.typename == "ValueError"

    def test_dimension_serialization(self):
        assert DimensionSpec() == loads(dumps(DimensionSpec()))
        assert self.dimension_spec_single_dim == loads(dumps(self.dimension_spec_single_dim))
        assert self.dimension_spec_branch_lvl_2 == loads(dumps(self.dimension_spec_branch_lvl_2))
        assert self.dimension_spec_tree_1 == loads(dumps(self.dimension_spec_tree_1))

    def test_dimension_spec_add_dimension(self):
        spec = DimensionSpec()
        spec.add_dimension(Dimension("dim_1_1", Type.LONG), None)
        assert spec == self.dimension_spec_single_dim
        # check duplicate
        with pytest.raises(Exception) as error:
            spec.add_dimension(Dimension("dim_1_1", Type.LONG), None)
        assert error.typename == "ValueError"

        spec = DimensionSpec()
        spec.add_dimension(Dimension("dim_1_1", Type.LONG), DimensionSpec([Dimension("dim_1_2", Type.STRING)], [None]))

        assert spec == self.dimension_spec_branch_lvl_2

        with pytest.raises(Exception) as error:
            spec.add_dimension(None, DimensionSpec([Dimension("dim_1_2", Type.STRING)], [None]))
        assert error.typename == "ValueError"

    def test_dimension_spec_get_dimensions(self):
        assert not DimensionSpec().get_dimensions()
        assert DimensionSpec().get_dimensions() is not None
        for dim, spec in DimensionSpec().get_dimensions():
            assert False

        dim_iter = iter(self.dimension_spec_branch_lvl_2.get_dimensions())

        first_dim = next(dim_iter)
        assert first_dim[0] == Dimension("dim_1_1", Type.LONG)
        assert first_dim[1] == DimensionSpec([Dimension("dim_1_2", Type.STRING)], [None])
        with pytest.raises(Exception) as error:
            next(dim_iter)

        dim_iter = self.dimension_spec_tree_1.get_dimensions()
        all_dims = [dim for dim, spec in dim_iter]

        assert all_dims == [Dimension("dim_1_1", Type.LONG), Dimension("dim_2_1", Type.DATETIME)]

    def test_dimension_spec_get_root_dimensions(self):
        assert not DimensionSpec().get_root_dimensions()
        assert DimensionSpec().get_root_dimensions() is not None
        for dim in DimensionSpec().get_root_dimensions():
            assert False

        assert next(iter(self.dimension_spec_tree_1.get_root_dimensions())) == Dimension("dim_1_1", Type.LONG)
        assert list(self.dimension_spec_tree_1.get_root_dimensions()) == [
            Dimension("dim_1_1", Type.LONG),
            Dimension("dim_2_1", Type.DATETIME),
        ]

        assert list(next(iter(self.dimension_spec_branch_lvl_2.get_dimensions()))[1].get_root_dimensions()) == [
            Dimension("dim_1_2", Type.STRING)
        ]

    def test_dimension_spec_get_all_sub_dimensions(self):
        assert not DimensionSpec().get_all_sub_dimensions()
        assert [spec for spec in self.dimension_spec_single_dim.get_all_sub_dimensions()] == [None]
        assert [spec for spec in self.dimension_spec_branch_lvl_2.get_all_sub_dimensions()] == [
            DimensionSpec([Dimension("dim_1_2", Type.STRING)], [None])
        ]
        assert [spec for spec in self.dimension_spec_tree_1.get_all_sub_dimensions()] == [
            DimensionSpec([Dimension("dim_1_2", Type.STRING)], [None]),
            None,
        ]

    def test_dimension_spec_get_flattened_dimension_map(self):
        assert not DimensionSpec().get_flattened_dimension_map()
        assert self.dimension_spec_single_dim.get_flattened_dimension_map() == {"dim_1_1": Dimension("dim_1_1", Type.LONG)}
        assert self.dimension_spec_branch_lvl_2.get_flattened_dimension_map() == {
            "dim_1_1": Dimension("dim_1_1", Type.LONG),
            "dim_1_2": Dimension("dim_1_2", Type.STRING),
        }

    def test_dimension_spec_get_total_dimension_count(self):
        assert DimensionSpec().get_total_dimension_count() == 0
        assert self.dimension_spec_single_dim.get_total_dimension_count() == 1
        assert self.dimension_spec_branch_lvl_2.get_total_dimension_count() == 2
        assert (
            DimensionSpec([Dimension("dim_1_1", Type.LONG), Dimension("dim_1_2", Type.LONG)], [None, None]).get_total_dimension_count() == 2
        )
        assert (
            DimensionSpec(
                [Dimension("dim_1_1", Type.LONG), Dimension("dim_1_2", Type.LONG)],
                [DimensionSpec([Dimension("dim_1_1_1"), Dimension("dim_1_1_2", Type.DATETIME)], [None, None]), None],
            ).get_total_dimension_count()
            == 4
        )

    def test_dimension_spec_find_dimension_by_name(self):
        assert DimensionSpec().find_dimension_by_name("dim") is None
        assert self.dimension_spec_single_dim.find_dimension_by_name("dim_1_1") == Dimension("dim_1_1", Type.LONG)

    def test_dimension_spec_check_compatibility(self):
        assert DimensionSpec().check_compatibility(None)
        assert DimensionSpec().check_compatibility([])
        assert DimensionSpec().check_compatibility(DimensionSpec())

        assert not self.dimension_spec_single_dim.check_compatibility(None)
        assert not self.dimension_spec_single_dim.check_compatibility([])
        assert not self.dimension_spec_single_dim.check_compatibility(DimensionSpec())

        assert self.dimension_spec_single_dim.check_compatibility(copy.deepcopy(self.dimension_spec_single_dim))
        assert self.dimension_spec_single_dim.check_compatibility(
            copy.deepcopy(self.dimension_spec_single_dim), enable_breadth_check=False, enable_type_check=True
        )
        assert self.dimension_spec_single_dim.check_compatibility(
            copy.deepcopy(self.dimension_spec_single_dim), enable_breadth_check=True, enable_type_check=False
        )

        # test type check
        assert self.dimension_spec_single_dim.check_compatibility(
            DimensionSpec([Dimension("dim_1_1", Type.STRING)], [None]), enable_breadth_check=True, enable_type_check=False
        )
        # should fail due to enabled type-check
        assert not self.dimension_spec_single_dim.check_compatibility(
            DimensionSpec([Dimension("dim_1_1", Type.STRING)], [None]), enable_breadth_check=True, enable_type_check=True
        )

        # list tests
        assert self.dimension_spec_single_dim.check_compatibility(["any value"])
        assert self.dimension_spec_single_dim.check_compatibility(["any value"], enable_breadth_check=False)

        assert not self.dimension_spec_single_dim.check_compatibility(["any 1", 2])

        assert self.dimension_spec_branch_lvl_2.check_compatibility(["any 1", 2])
        assert not self.dimension_spec_branch_lvl_2.check_compatibility(["any value"])

        assert not self.dimension_spec_single_dim.check_compatibility(self.dimension_spec_branch_lvl_2)
        assert not self.dimension_spec_branch_lvl_2.check_compatibility(self.dimension_spec_single_dim)
        assert not self.dimension_spec_single_dim.check_compatibility(
            self.dimension_spec_branch_lvl_2, enable_breadth_check=True, enable_type_check=False
        )
        assert not self.dimension_spec_single_dim.check_compatibility(
            self.dimension_spec_branch_lvl_2, enable_breadth_check=False, enable_type_check=True
        )

        assert not self.dimension_spec_single_dim.check_compatibility(self.dimension_spec_tree_1)
        assert not self.dimension_spec_single_dim.check_compatibility(self.dimension_spec_tree_1, enable_breadth_check=False)

        assert not self.dimension_spec_tree_1.check_compatibility([])
        assert not self.dimension_spec_tree_1.check_compatibility(["dummy"])
        assert not self.dimension_spec_tree_1.check_compatibility(["dummy 1", "dummy 2"])

        assert self.dimension_spec_tree_1.check_compatibility(["dummy 1", "dummy 2"], enable_breadth_check=False)

        assert not self.dimension_spec_tree_1.check_compatibility(self.dimension_spec_branch_lvl_2)
        assert self.dimension_spec_tree_1.check_compatibility(self.dimension_spec_branch_lvl_2, enable_breadth_check=False)
        assert self.dimension_spec_branch_lvl_2.check_compatibility(self.dimension_spec_tree_1, enable_breadth_check=False)

        spec_with_diff_lvl_2_type = DimensionSpec(
            [Dimension("dim_1_1", Type.LONG)], [DimensionSpec([Dimension("dim_1_2", Type.DATETIME)], [None])]
        )
        assert not self.dimension_spec_tree_1.check_compatibility(spec_with_diff_lvl_2_type, enable_breadth_check=False)
        assert self.dimension_spec_tree_1.check_compatibility(
            spec_with_diff_lvl_2_type, enable_breadth_check=False, enable_type_check=False
        )

    def test_dimension_spec_load_from_pretty(self):
        assert DimensionSpec.load_from_pretty(None) == DimensionSpec()
        assert DimensionSpec.load_from_pretty({}) == DimensionSpec()

        assert self.dimension_spec_single_dim == DimensionSpec.load_from_pretty({"dim_1_1": {type: Type.LONG}})

        # extra params
        assert not self.dimension_spec_single_dim == DimensionSpec.load_from_pretty(
            {"dim_1_1": {type: Type.LONG, "param_key": "param_value"}}
        )

        assert DimensionSpec([Dimension("dim_1_1", Type.LONG, {"param_key": "param_value"})], [None]) == DimensionSpec.load_from_pretty(
            {"dim_1_1": {type: Type.LONG, "param_key": "param_value"}}
        )

        assert self.dimension_spec_branch_lvl_2 == DimensionSpec.load_from_pretty(
            {"dim_1_1": {"type": Type.LONG, "dim_1_2": {type: Type.STRING}}}
        )

        assert self.dimension_spec_tree_1 == DimensionSpec.load_from_pretty(
            {"dim_1_1": {"type": Type.LONG, "dim_1_2": {type: Type.STRING}}, "dim_2_1": {"type": Type.DATETIME}}
        )

        # test dim overwrite in the same scope, last one will be the active
        dim_spec = DimensionSpec.load_from_pretty({"dim_1_1": {"type": Type.LONG}, "dim_1_1": {"type": Type.DATETIME}})
        root_dims = list(dim_spec.get_root_dimensions())
        assert len(root_dims) == 1
        assert root_dims[0].type == Type.DATETIME

        # name-check will be active across the hierarchy
        with pytest.raises(ValueError):
            DimensionSpec.load_from_pretty({"dim_1_1": {"type": Type.LONG, "dim_1_1": {"type": Type.DATETIME}}})

        with pytest.raises(ValueError):
            DimensionSpec.load_from_pretty(
                {
                    "dim_1_1": {"type": Type.LONG, "dim_1_2": {"type": Type.DATETIME}},  # duplicate
                    "dim_1_2": {"type": Type.STRING},  # duplicate
                }
            )

        with pytest.raises((ValueError, KeyError)):
            DimensionSpec.load_from_pretty({"dummy_name": {type: None}})

    def test_dimension_spec_equality(self):
        assert DimensionSpec() == {}
        assert DimensionSpec() == OrderedDict()
        assert self.dimension_spec_single_dim == copy.deepcopy(self.dimension_spec_single_dim)
        assert self.dimension_spec_single_dim != DimensionSpec()
        assert self.dimension_spec_single_dim != self.dimension_spec_branch_lvl_2

    def test_dimension_spec_boolean(self):
        assert not DimensionSpec()
        assert self.dimension_spec_single_dim

    def test_dimension_spec_set(self):
        assert {self.dimension_spec_single_dim, self.dimension_spec_branch_lvl_2, self.dimension_spec_tree_1} == {
            self.dimension_spec_tree_1,
            self.dimension_spec_single_dim,
            self.dimension_spec_branch_lvl_2,
        }

        assert self.dimension_spec_single_dim in {self.dimension_spec_single_dim}

        assert self.dimension_spec_single_dim not in {self.dimension_spec_branch_lvl_2, self.dimension_spec_tree_1}
