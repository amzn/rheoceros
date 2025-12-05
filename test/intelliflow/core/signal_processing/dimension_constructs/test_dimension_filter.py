# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from collections import OrderedDict
from datetime import date, datetime, timedelta
from test.intelliflow.core.signal_processing.dimension_constructs.test_dimension_spec import TestDimensionSpec
from typing import cast

import pytest
from dateutil.tz import tzlocal

from intelliflow.api import INSENSITIVE, LOWER, UPPER
from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.dimension_defs import DatetimeGranularity, Type
from intelliflow.core.signal_processing.dimension_constructs import (
    AnyVariant,
    DateVariant,
    Dimension,
    DimensionFilter,
    DimensionSpec,
    DimensionVariant,
    DimensionVariantReader,
    LongVariant,
    RelativeVariant,
    StringVariant,
)


class TestDimensionFilter:
    dimension_filter_basic_str_1 = DimensionFilter.load_raw({"strvalue": {}})
    dimension_filter_basic_str_2 = DimensionFilter.load_raw({"": {}})

    dimension_filter_basic_long_1 = DimensionFilter.load_raw({111222333444555666777888999000: {}})
    dimension_filter_basic_long_2 = DimensionFilter.load_raw({-111222333444555666777888999000: {}})
    dimension_filter_basic_long_3 = DimensionFilter.load_raw({0: {}})
    dimension_filter_basic_long_4 = DimensionFilter.load_raw({"111222333444555666777888999000": {}})
    dimension_filter_basic_long_5 = DimensionFilter.load_raw({"-111222333444555666777888999000": {}})

    dimension_filter_basic_datetime_1 = DimensionFilter.load_raw({datetime(2020, 6, 15): {}})
    dimension_filter_basic_datetime_2 = DimensionFilter.load_raw({"2020-06-15": {}})
    dimension_filter_basic_datetime_3 = DimensionFilter.load_raw({"2020-06-15T00:00:00": {}})
    dimension_filter_basic_datetime_4 = DimensionFilter.load_raw({"2020-06-15 00:00:00": {}})

    dimension_filter_basic_any = DimensionFilter.load_raw({"*": {}})
    dimension_filter_basic_any_type_string = copy.deepcopy(dimension_filter_basic_any)
    dimension_filter_basic_any_type_string.set_spec(DimensionSpec.load_from_pretty({"dim": {type: Type.STRING}}))
    dimension_filter_basic_any_type_long = copy.deepcopy(dimension_filter_basic_any)
    dimension_filter_basic_any_type_long.set_spec(DimensionSpec.load_from_pretty({"dim_1_1": {type: Type.LONG}}))

    dimension_filter_basic_relative_1 = DimensionFilter.load_raw({"_:-2": {}})
    dimension_filter_basic_relative_1_shift_minus_1 = DimensionFilter.load_raw({"_:-2:-1": {}})
    dimension_filter_basic_relative_2 = DimensionFilter.load_raw({"_:2": {}})
    dimension_filter_basic_relative_2_shift_plus_1 = DimensionFilter.load_raw({"_:2:1": {}})
    dimension_filter_basic_relative_1_type_datetime = copy.deepcopy(dimension_filter_basic_relative_1)
    dimension_filter_basic_relative_1_type_datetime.set_spec(DimensionSpec.load_from_pretty({"dim": {type: Type.DATETIME}}))

    # filter for TestDimensionSpec.dimension_spec_branch_lvl_2
    dimension_filter_branch_1 = DimensionFilter.load_raw({"*": {"*": {}}})
    dimension_filter_branch_1.set_spec(TestDimensionSpec.dimension_spec_branch_lvl_2)

    datetime_now = datetime.utcnow().date()

    def test_dimension_filter_init(self):
        DimensionFilter()
        # pure values
        DimensionFilter([DimensionVariant("dim_value_1_1"), DimensionVariant(12)], [None, None])
        # + names
        DimensionFilter([DimensionVariant("dim_value_1_1", "dim1"), DimensionVariant(12, "dim2")], [None, None])
        # + types
        DimensionFilter([DimensionVariant("dim_value_1_1", "dim1", Type.STRING), DimensionVariant(12, "dim2", Type.LONG)], [None, None])
        # branch (same values)
        DimensionFilter([DimensionVariant("dim_value_1_1")], [DimensionFilter([DimensionVariant("dim_value_1_1")], [None])])
        now = datetime.now()
        DimensionFilter([DimensionVariant(now)], [DimensionFilter([DimensionVariant(now)], [None])])
        # treat empty string as a proper name value
        DimensionFilter([DimensionVariant("dim_value_1_1", "")], [DimensionFilter([DimensionVariant("dim_value_1_2", "")], [None])])
        # duplicate 'names' (on the same branch)
        DimensionFilter(
            [DimensionVariant("dim_value_1_1", "dim_1_1")], [DimensionFilter([DimensionVariant("dim_value_1_2", "dim_1_1")], [None])]
        ),

    @pytest.mark.parametrize(
        "dim_variants, sub_dim_filters",
        [
            ([DimensionVariant("dim_value")], []),
            ([DimensionVariant("dim_value")], None),
            # list sizes won't match
            ([DimensionVariant("dim_value")], [None, None]),
        ],
    )
    def test_dimension_filter_init_failure(self, dim_variants, sub_dim_filters):
        with pytest.raises(Exception) as error:
            DimensionFilter(dim_variants, sub_dim_filters)
        assert error.typename == "ValueError"

    @pytest.mark.parametrize(
        "filter",
        [
            dimension_filter_basic_str_1,
            dimension_filter_basic_str_2,
            dimension_filter_basic_long_1,
            dimension_filter_basic_long_2,
            dimension_filter_basic_long_3,
            dimension_filter_basic_long_4,
            dimension_filter_basic_long_5,
            dimension_filter_basic_datetime_1,
            dimension_filter_basic_datetime_2,
            dimension_filter_basic_datetime_3,
            dimension_filter_basic_datetime_4,
            dimension_filter_basic_any,
            dimension_filter_basic_relative_1,
            dimension_filter_basic_relative_1_shift_minus_1,
            dimension_filter_basic_relative_2,
            dimension_filter_basic_relative_2_shift_plus_1,
        ],
    )
    def test_dimension_filter_serialization(self, filter):
        assert filter == loads(dumps(filter))

    @pytest.mark.parametrize(
        "filter, dimension, value",
        [
            (dimension_filter_basic_str_1, Dimension("name"), "strvalue"),
            (dimension_filter_basic_str_1, Dimension(""), "strvalue"),
            (dimension_filter_basic_str_1, Dimension(None), "strvalue"),
            (dimension_filter_basic_str_2, Dimension("name"), ""),
            (dimension_filter_basic_long_1, Dimension("name"), 111222333444555666777888999000),
            (dimension_filter_basic_long_2, Dimension("name"), -111222333444555666777888999000),
            (dimension_filter_basic_long_3, Dimension("name"), 0),
            (dimension_filter_basic_long_4, Dimension("name"), 111222333444555666777888999000),
            (dimension_filter_basic_long_5, Dimension("name"), -111222333444555666777888999000),
            (dimension_filter_basic_datetime_1, Dimension("name"), "2020-06-15 00:00:00"),
            (dimension_filter_basic_datetime_2, Dimension("name"), "2020-06-15 00:00:00"),
            (dimension_filter_basic_datetime_3, Dimension("name"), "2020-06-15 00:00:00"),
            (dimension_filter_basic_datetime_4, Dimension("name"), "2020-06-15 00:00:00"),
            (dimension_filter_basic_any, Dimension("name"), "*"),
            (dimension_filter_basic_relative_1, Dimension("name"), "_:-2"),
            # shift goes into params,
            (dimension_filter_basic_relative_1_shift_minus_1, Dimension("name"), "_:-2"),
            (dimension_filter_basic_relative_2, Dimension("name"), "_:2"),
        ],
    )
    def test_dimension_filter_feed_on_basic_filters(self, filter: DimensionFilter, dimension, value):
        reader = DimensionVariantReader([dimension])
        assert reader.source == tuple([dimension])
        filter_clone = copy.deepcopy(filter)
        filter_clone.set_spec(DimensionSpec([dimension], [None]))
        filter_clone.feed([reader])

        assert reader.variants[dimension.name] == list(filter_clone.get_root_dimensions())
        assert reader.values[dimension.name] == [value]

    def test_dimension_filter_feed_corner_cases(self):
        reader = DimensionVariantReader([Dimension("dim_name")])
        # filter has no dim with valid 'name' attribute
        self.dimension_filter_basic_str_1.feed([reader])

        assert not reader.variants["dim_name"]

        filter = DimensionFilter.load_raw({"dim_val_1": {datetime(2020, 11, 30): {}}})

        reader = DimensionVariantReader([Dimension(None)])
        filter.feed([reader])
        assert reader.variants == {None: [StringVariant("dim_val_1"), DateVariant(datetime(2020, 11, 30))]}

        filter.set_spec(DimensionSpec.load_from_pretty({"dim1": {type: Type.STRING, "": {type: Type.DATETIME}}}))

        reader = DimensionVariantReader([Dimension("")])
        filter.feed([reader])
        assert reader.variants[""] == [DateVariant(datetime(2020, 11, 30))]

        reader = DimensionVariantReader([Dimension("dim2")])
        filter.feed([reader])
        assert not reader.variants["dim2"]

    @pytest.mark.parametrize(
        "filter, filter2, result",
        [
            (DimensionFilter(), {}, {}),
            (dimension_filter_basic_str_1, {"strvalue": {}}, {"strvalue": {}}),
            (dimension_filter_basic_str_1, {"strvalue2": {}}, {}),
            (dimension_filter_basic_str_1, {}, None),
            (dimension_filter_basic_str_1, {"*": {}}, {"strvalue": {}}),
            (dimension_filter_basic_str_1, {"_:-2": {}}, {"strvalue": {}, "strvalud": {}}),
            (dimension_filter_basic_str_1, {"_:2": {}}, {"strvalue": {}, "strvaluf": {}}),
            # spec mismatch
            (dimension_filter_basic_str_1, {"str_value": {"str_value2": {}}}, None),
            (dimension_filter_basic_str_1, {datetime.now(): {}}, None),
            (dimension_filter_basic_str_1, {1: {}}, None),
            (dimension_filter_basic_str_1, {"1": {}}, None),
            (dimension_filter_basic_long_1, {111222333444555666777888999000: {}}, {111222333444555666777888999000: {}}),
            (dimension_filter_basic_long_1, {"*": {}}, {111222333444555666777888999000: {}}),
            (dimension_filter_basic_long_1, {"_:2": {}}, {111222333444555666777888999000: {}, 111222333444555666777888999001: {}}),
            (dimension_filter_basic_long_2, {-111222333444555666777888999000: {}}, {-111222333444555666777888999000: {}}),
            # spec (type) mismatch
            (dimension_filter_basic_long_3, {"str": {}}, None),
            (dimension_filter_basic_long_3, {datetime.now(): {}}, None),
            (dimension_filter_basic_long_3, {"_:-2": {}}, {0: {}, -1: {}}),
            (dimension_filter_basic_long_4, {111222333444555666777888999000: {}}, {111222333444555666777888999000: {}}),
            (dimension_filter_basic_long_5, {-111222333444555666777888999000: {}}, {-111222333444555666777888999000: {}}),
            (dimension_filter_basic_long_4, {-111222333444555666777888999000: {}}, {}),
            (dimension_filter_basic_datetime_1, {"2020-06-15": {}}, {datetime(2020, 6, 15): {}}),
            (dimension_filter_basic_datetime_1, {"2020-06-15 00:00:00": {}}, {datetime(2020, 6, 15): {}}),
            (dimension_filter_basic_datetime_1, {"2020-06-15 00:00": {}}, {datetime(2020, 6, 15): {}}),
            (dimension_filter_basic_datetime_1, {"2020-06-15 00": {}}, {datetime(2020, 6, 15): {}}),
            (dimension_filter_basic_datetime_1, {"2020-06-15T00:00:00": {}}, {datetime(2020, 6, 15): {}}),
            (dimension_filter_basic_datetime_1, {"2020-06-15T00:00": {}}, {datetime(2020, 6, 15): {}}),
            (dimension_filter_basic_datetime_1, {datetime(2020, 6, 15): {}}, {datetime(2020, 6, 15): {}}),
            (dimension_filter_basic_datetime_1, {datetime(2020, 6, 16): {}}, {}),
            (dimension_filter_basic_datetime_1, {"2020-06-15T00:00:01": {}}, {}),
            (dimension_filter_basic_datetime_1, {1: {}}, None),
            (dimension_filter_basic_datetime_1, {"string": {}}, None),
            (dimension_filter_basic_datetime_2, {"2020-06-15": {}}, {"2020-06-15": {}}),
            (dimension_filter_basic_datetime_2, {"2020-06-15 00:00:00": {}}, {"2020-06-15": {}}),
            (dimension_filter_basic_datetime_2, {"2020-06-16": {}}, {}),
            (dimension_filter_basic_datetime_2, {"*": {}}, {"2020-06-15": {}}),
            (dimension_filter_basic_datetime_2, {"_:2": {}}, {"2020-06-15": {}, "2020-06-16": {}}),
            (dimension_filter_basic_datetime_2, {"_:-2": {}}, {"2020-06-15": {}, "2020-06-14": {}}),
            (dimension_filter_basic_any, {"*": {}}, {"*": {}}),
            (dimension_filter_basic_any, {"string": {}}, {"string": {}}),
            (dimension_filter_basic_any, {"1": {}}, {"1": {}}),
            (dimension_filter_basic_any, {1: {}}, {1: {}}),
            (dimension_filter_basic_any, {datetime(2020, 11, 30): {}}, {datetime(2020, 11, 30): {}}),
            (dimension_filter_basic_any, {"2020-06-15": {}}, {"2020-06-15": {}}),
            # when relative variant is applied against 'any', result is expected to be the relative variant itself
            (dimension_filter_basic_any, {"_:2": {}}, {"_:2": {}}),
            (dimension_filter_basic_any, {"_:-2": {}}, {"_:-2": {}}),
            (dimension_filter_basic_relative_1, {5: {}}, {5: {}, 4: {}}),
            # shift/transforms should not cause any difference in underlying variants
            (dimension_filter_basic_relative_1_shift_minus_1, {5: {}}, {5: {}, 4: {}}),
            (dimension_filter_basic_relative_2, {5: {}}, {5: {}, 6: {}}),
            # shift/transforms should not cause any difference in underlying variants
            (dimension_filter_basic_relative_2_shift_plus_1, {5: {}}, {5: {}, 6: {}}),
            (dimension_filter_basic_relative_1, {"strvalue": {}}, {"strvalue": {}, "strvalud": {}}),
            (dimension_filter_basic_relative_2, {"strvalue": {}}, {"strvalue": {}, "strvaluf": {}}),
            (dimension_filter_basic_relative_1, {"2020-06-15": {}}, {"2020-06-15": {}, "2020-06-14": {}}),
            (dimension_filter_basic_relative_2, {"2020-06-15": {}}, {"2020-06-15": {}, "2020-06-16": {}}),
            (dimension_filter_basic_relative_1, {"*": {}}, {"_:-2": {}}),
            (dimension_filter_basic_relative_2, {"*": {}}, {"_:2": {}}),
            (DimensionFilter.load_raw({"NA": {"*": {}}, "EU": {"*": {}}}), {"NA": {"2020-11-29": {}}}, {"NA": {"2020-11-29": {}}}),
            (
                DimensionFilter.load_raw({"NA": {"*": {}}, "EU": {"_:-3": {}}}),
                {"EU": {"2020-11-29": {}}},
                {"EU": {"2020-11-29": {}, "2020-11-28": {}, "2020-11-27": {}}},
            ),
            (
                DimensionFilter.load_raw({"*": {type: Type.STRING}, "_:-2": {type: Type.DATETIME}}),
                {"str_value": {}, "str_value1": {}, "2020-06-20": {}},  # type block 1  # type block 2
                {"str_value": {}, "str_value1": {}, "2020-06-20": {}, "2020-06-19": {}},  # type block 1  # type block 2 (range exploded)
            ),
        ],
    )
    def test_dimension_filter_apply(self, filter, filter2, result):
        new_filter = filter.apply(DimensionFilter.load_raw(filter2))
        if result is None:
            assert new_filter is None
        else:
            assert DimensionFilter.check_equivalence(DimensionFilter.load_raw(result), new_filter)

    @pytest.mark.parametrize(
        "filter, filter2",
        [
            (dimension_filter_basic_relative_1, {"_:-1": {}}),
            (dimension_filter_basic_relative_1_shift_minus_1, {"_:-1": {}}),
            (dimension_filter_basic_relative_1, {"_:1": {}}),
            (dimension_filter_basic_relative_2, {"_:1": {}}),
            (dimension_filter_basic_relative_2_shift_plus_1, {"_:1": {}}),
            (dimension_filter_basic_relative_2, {"_:-1": {}}),
        ],
    )
    def test_dimension_filter_apply_failure(self, filter, filter2):
        with pytest.raises(RuntimeError):
            filter.apply(DimensionFilter.load_raw(filter2))

    @pytest.mark.parametrize(
        "filter, filter2, result",
        [
            (DimensionFilter(), {}, {}),
            (dimension_filter_basic_relative_1, {"_:-1": {}}, {"_:-1": {}}),
            (dimension_filter_basic_relative_1_shift_minus_1, {"_:-1": {}}, {"_:-1": {}}),
            (dimension_filter_basic_relative_1, {"_:1": {}}, {"_:0": {}}),
            (dimension_filter_basic_relative_1_shift_minus_1, {"_:1:-1": {}}, {"_:0": {}}),
            (dimension_filter_basic_relative_2, {"_:1": {}}, {"_:1": {}}),
            (dimension_filter_basic_relative_2_shift_plus_1, {"_:1": {}}, {"_:1": {}}),
            (dimension_filter_basic_relative_2, {"_:-1": {}}, {"_:0": {}}),
            (dimension_filter_basic_relative_2_shift_plus_1, {"_:-1": {}}, {"_:0": {}}),
            (dimension_filter_basic_relative_1, {5: {}}, {5: {}, 4: {}}),
            (dimension_filter_basic_relative_2, {5: {}}, {5: {}, 6: {}}),
            (dimension_filter_basic_relative_1, {"strvalue": {}}, {"strvalue": {}, "strvalud": {}}),
            (dimension_filter_basic_relative_2, {"strvalue": {}}, {"strvalue": {}, "strvaluf": {}}),
            (dimension_filter_basic_relative_1, {"2020-06-15": {}}, {"2020-06-15": {}, "2020-06-14": {}}),
            (dimension_filter_basic_relative_2, {"2020-06-15": {}}, {"2020-06-15": {}, "2020-06-16": {}}),
            (DimensionFilter.load_raw({"NA": {"*": {}}, "EU": {"_:-3": {}}}), {"EU": {"_:-2": {}}}, {"EU": {"_:-2": {}}}),
            # this one just checks that shift/transform does not impact the actual value
            (DimensionFilter.load_raw({"NA": {"*": {}}, "EU": {"_:-3:-1": {}}}), {"EU": {"_:-2": {}}}, {"EU": {"_:-2": {}}}),
        ],
    )
    def test_dimension_filter_chain(self, filter, filter2, result):
        new_filter = filter.chain(DimensionFilter.load_raw(filter2), False)
        if result is None:
            assert new_filter is None
        else:
            assert DimensionFilter.load_raw(result).is_equivalent(new_filter)

    @pytest.mark.parametrize(
        "filter, filter2, result",
        [
            (dimension_filter_basic_relative_1_shift_minus_1, {5: {}}, {4: {}, 3: {}}),
            (dimension_filter_basic_relative_2_shift_plus_1, {5: {}}, {6: {}, 7: {}}),
            (dimension_filter_basic_relative_1_shift_minus_1, {"strvalue": {}}, {"strvalud": {}, "strvaluc": {}}),
            (dimension_filter_basic_relative_2_shift_plus_1, {"strvalue": {}}, {"strvaluf": {}, "strvalug": {}}),
            (dimension_filter_basic_relative_1_shift_minus_1, {"2020-06-15": {}}, {"2020-06-14": {}, "2020-06-13": {}}),
            (dimension_filter_basic_relative_2_shift_plus_1, {"2020-06-15": {}}, {"2020-06-16": {}, "2020-06-17": {}}),
        ],
    )
    def test_dimension_filter_chain_with_shift(self, filter, filter2, result):
        new_filter = filter.chain(DimensionFilter.load_raw(filter2), False)
        # in order to check the effect of shift, we need to transform
        new_filter = new_filter.transform()
        assert DimensionFilter.load_raw(result).is_equivalent(new_filter)

    def test_dimension_filter_chain_preserves_shift(self):
        new_filter = self.dimension_filter_basic_relative_1_shift_minus_1.chain(DimensionFilter.load_raw({"_:-1": {}}), False)
        variant = next(iter(new_filter.get_root_dimensions()))
        shift = variant.params.get(DimensionVariant.RANGE_SHIFT_FIELD_ID, None)
        assert shift == -1

    @pytest.mark.parametrize(
        "filter, filter2, result",
        [
            (
                DimensionFilter.load_raw({"*": {type: Type.STRING, "name": "region"}, "NA": {Dimension.NAME_FIELD_ID: "region"}}),
                {"NA": {}},
                # result
                {"NA": {}},  # without duplicate elimination, we would have two StringVariants with 'NA' here.
            ),
            (
                DimensionFilter.load_raw(
                    {
                        "*": {Dimension.TYPE_FIELD_ID: Type.STRING, Dimension.NAME_FIELD_ID: "region", "foo": {}},
                        "NA": {Dimension.NAME_FIELD_ID: "region", "bar": {}},
                    }
                ),
                {"NA": {"*": {}}},
                # result (sub-specs are different, we end up with two identical variants at the root level [not eliminated])
                DimensionFilter(
                    [StringVariant("NA"), StringVariant("NA")],
                    [DimensionFilter([StringVariant("foo")], [None]), DimensionFilter([StringVariant("bar")], [None])],
                ),
            ),
            (
                DimensionFilter(
                    [StringVariant("NA")], [DimensionFilter([AnyVariant(None, Type.DATETIME), DateVariant("2020-03-18")], [None, None])]
                ),
                {"NA": {"2020-03-18": {}}},
                # result (identicals are eliminated!)
                {"NA": {"2020-03-18": {}}},
            ),
        ],
    )
    def test_dimension_filter_chain_eliminate_identicals(self, filter, filter2, result):
        new_filter = filter.chain(DimensionFilter.load_raw(filter2), False)
        if result is None:
            assert new_filter is None
        else:
            result_filter = result if isinstance(result, DimensionFilter) else DimensionFilter.load_raw(result)
            assert result_filter.is_equivalent(new_filter)

    @pytest.mark.parametrize(
        "lhs, rhs, result",
        [
            (dimension_filter_basic_str_1, dimension_filter_basic_str_2, True),
            (dimension_filter_basic_str_1, dimension_filter_basic_long_1, False),
            (dimension_filter_basic_str_1, dimension_filter_basic_long_4, False),
            (dimension_filter_basic_str_1, dimension_filter_basic_datetime_1, False),
            (dimension_filter_basic_str_1, dimension_filter_basic_datetime_2, False),
            (dimension_filter_basic_str_1, dimension_filter_basic_any, True),
            (dimension_filter_basic_str_1, dimension_filter_basic_any_type_string, True),
            (dimension_filter_basic_str_1, dimension_filter_basic_any_type_long, False),
            (dimension_filter_basic_str_1, dimension_filter_basic_relative_1, True),
            (dimension_filter_basic_str_1, dimension_filter_basic_relative_1_type_datetime, False),
            (dimension_filter_basic_any, dimension_filter_basic_long_1, True),
            (dimension_filter_basic_any_type_string, dimension_filter_basic_long_1, False),
            (dimension_filter_basic_any, dimension_filter_basic_relative_1, True),
            (dimension_filter_basic_any_type_long, dimension_filter_basic_relative_1_type_datetime, False),
            (DimensionFilter(), DimensionFilter(), True),
            ({}, {}, True),
            (dimension_filter_basic_str_1, {}, False),
            ({}, dimension_filter_basic_str_1, False),
            # rest of the tests: type groups at the same level should be satisfied
            ({"1": {}, "2": {}}, {3: {}}, True),
            ({"1": {}, "2": {}}, {3: {}, "4": {}, 5: {}}, True),
            ({"1": {}, "2": {}, "string": {}}, {3: {}}, False),
            (
                {
                    "1": {},
                    "2": {},
                },
                {3: {}, "string": {}},
                False,
            ),
            ({"1": {}, "2": {}, "string": {}}, {3: {}, "string2": {}}, True),
            (
                {
                    "01-05-2020": {},
                    "2020-05-02": {},
                },
                {datetime.now(): {}},
                True,
            ),
            (
                {
                    "01-05-2020": {},
                    "2020-05-02": {},
                },
                {datetime.now(): {}, 111: {}},
                False,
            ),
            (
                {
                    "str": {
                        "2020-05-02": {},
                    },
                },
                {
                    "str2": {
                        datetime.now(): {},
                    }
                },
                True,
            ),
            (
                {
                    "str": {
                        "2020-05-02": {},
                    },
                },
                {"str2": {datetime.now(): {}, "str3": {}}},
                False,
            ),
            (
                {
                    "str": {
                        "2020-05-02": {},
                    },
                },
                {"str2": {}},
                False,
            ),
            (
                {
                    "*": {
                        "*": {},
                    },
                },
                {"str2": {"str3": {}}},
                True,
            ),
            (
                {
                    "1": {
                        datetime(2020, 6, 15): {},
                    },
                },
                {"*": {"_:-15": {}}},
                True,
            ),
            (
                {"*": {"_:-15": {}}},
                {
                    "1": {
                        datetime(2020, 6, 15): {},
                    },
                },
                True,
            ),
            (DimensionFilter([AnyVariant(), AnyVariant()], [None, None]), {"str_value": {}}, False),  # missing type
            (
                DimensionFilter([AnyVariant(), AnyVariant()], [None, None]),
                {
                    # type 1
                    "str_value": {},
                    # type block 2
                    "1": {},
                    "2": {},
                },
                True,
            ),
            (
                {"*": {}, "_:-10": {}},  # type 1  # type 2
                {"str_value": {}, "str_value1": {}, "2020-06-20": {}},  # type block 1  # type block 2
                True,
            ),
        ],
    )
    def test_dimension_filter_check_spec_match_filter_to_filter(self, lhs, rhs, result):
        left_filter = lhs if isinstance(lhs, DimensionFilter) else DimensionFilter.load_raw(lhs)
        right_filter = rhs if isinstance(rhs, DimensionFilter) else DimensionFilter.load_raw(rhs)
        assert left_filter.check_spec_match(right_filter) == result

    @pytest.mark.parametrize(
        "lhs, rhs_spec, result",
        [
            ({}, {}, True),
            (DimensionFilter(), DimensionSpec(), True),
            ({}, DimensionSpec(), True),
            (DimensionFilter(), {}, True),
            (dimension_filter_basic_str_1, {"dim_name": {type: Type.STRING}}, True),
            (dimension_filter_basic_str_1, {"dim_name2": {type: Type.STRING}}, True),
            (dimension_filter_basic_str_1, {"dim_name": {type: Type.LONG}}, False),
            (dimension_filter_basic_str_1, {"dim_name": {type: Type.DATETIME}}, False),
            (dimension_filter_basic_long_1, {"dim_name": {type: Type.LONG}}, True),
            (dimension_filter_basic_any, {"dim_name": {type: Type.LONG}}, True),
            (dimension_filter_basic_any, {"dim_name": {type: Type.DATETIME}}, True),
            (dimension_filter_basic_any, {"dim_name": {type: Type.STRING}}, True),
            (dimension_filter_basic_any_type_long, {"dim_name": {type: Type.LONG}}, True),
            (dimension_filter_basic_any_type_long, {"dim_name": {type: Type.STRING}}, False),
            (dimension_filter_basic_any_type_long, {"dim_name": {type: Type.DATETIME}}, False),
            (dimension_filter_basic_relative_1, {"dim_name": {type: Type.LONG}}, True),
            (dimension_filter_basic_relative_1, {"dim_name": {type: Type.STRING}}, True),
            (dimension_filter_basic_relative_1, {"dim_name": {type: Type.DATETIME}}, True),
            (dimension_filter_basic_relative_1_type_datetime, {"dim_name": {type: Type.DATETIME}}, True),
            (dimension_filter_basic_relative_1_type_datetime, {"dim_name": {type: Type.STRING}}, False),
            (dimension_filter_basic_relative_1_type_datetime, {"dim_name": {type: Type.LONG}}, False),
            (
                {"*": {"_:-15": {}}},
                {
                    "dim": {type: Type.STRING, "dim2": {type: Type.DATETIME}},
                },
                True,
            ),
            (
                {"str_value": {"2020-06-10": {}}},
                {
                    "dim": {type: Type.STRING, "dim2": {type: Type.DATETIME}},
                },
                True,
            ),
            (
                {"str_value": {}},
                {
                    "dim": {type: Type.STRING, "dim2": {type: Type.DATETIME}},
                },
                False,
            ),
            (
                {"str_value": {"2020-06-10": {}}},
                {
                    "dim": {type: Type.STRING},
                },
                False,
            ),
            (
                {"100": {}},
                {
                    "dim": {type: Type.LONG},
                },
                True,
            ),
            (
                {"100": {}},
                {
                    "dim": {type: Type.STRING},
                },
                False,
            ),
            (
                {"str1": {}, "str2": {}, "str3": {}, "str4": {}},
                {
                    "dim": {type: Type.STRING},
                },
                True,
            ),
            (
                {"str1": {}, "str2": {}, 1: {}, 2: {}},
                {
                    "str_dim": {type: Type.STRING},
                    "long_dim": {type: Type.LONG},
                },
                True,
            ),
            (
                {
                    "str1": {"2020-06-09": {}, "2020-06-10": {}},
                    "str2": {
                        "2020-06-09": {},
                        "2020-06-10": {},
                    },
                    1: {},
                    2: {},
                },
                {
                    "str_dim": {type: Type.STRING, "date_dim": {type: Type.DATETIME, "format": "%Y-%m-%d"}},
                    "long_dim": {type: Type.LONG},
                },
                True,
            ),
            (
                {
                    "str1": {"2020-06-09": {}, "2020-06-10": {}},
                    "str2": {
                        "2020-06-09": {},
                        "2020-06-10": {},
                    },
                    1: {
                        "2020-06-09": {},
                        "2020-06-10": {},
                    },
                    2: {},
                },
                {
                    "str_dim": {type: Type.STRING, "date_dim": {type: Type.DATETIME, "format": "%Y-%m-%d"}},
                    "long_dim": {type: Type.LONG},
                },
                False,
            ),
            (
                {
                    "str1": {},
                    "str2": {},
                    1: {
                        "2020-06-09": {},
                        "2020-06-10": {},
                    },
                    2: {
                        "2020-06-09": {},
                        "2020-06-10": {},
                    },
                },
                {
                    "str_dim": {
                        type: Type.STRING,
                    },
                    "long_dim": {type: Type.LONG, "date_dim": {type: Type.DATETIME, "format": "%Y-%m-%d"}},
                },
                True,
            ),
            (
                {
                    "str1": {
                        2: {
                            "2020-06-09": {},
                            "2020-06-10": {},
                        }
                    },
                    1: {
                        "2020-06-09": {},
                        "2020-06-10": {},
                    },
                },
                {
                    "str_dim": {
                        type: Type.STRING,
                        "long_dim_1": {
                            type: Type.LONG,
                            "date_dim_2": {
                                type: Type.DATETIME,
                            },
                        },
                    },
                    "long_dim_2": {
                        type: Type.LONG,
                        "date_dim_2": {
                            type: Type.DATETIME,
                        },
                    },
                },
                True,
            ),
            # check resolve functionality
            # without user hint
            (
                {"2020-05-06suffix": {}},
                {"dim": {type: Type.STRING}},  # StringVariant will pick this up, DateVariant will reject (UNMATCH)
                True,
            ),
            ({1.1: {}}, {"dim": {type: Type.LONG}}, True),  # will resolve to Long (= int(1) )  # LongVariant will be the decisive match
        ],
    )
    def test_dimension_filter_check_spec_match_filter_to_spec(self, lhs, rhs_spec, result):
        left_filter = lhs if isinstance(lhs, DimensionFilter) else DimensionFilter.load_raw(lhs)
        right_spec = lhs if isinstance(rhs_spec, DimensionSpec) else DimensionSpec.load_from_pretty(rhs_spec)
        assert left_filter.check_spec_match(right_spec) == result

    @pytest.mark.parametrize(
        "lhs, rhs_spec, result",
        [
            ([], {}, True),
            ([], DimensionSpec(), True),
            (["*"], {"dummy_name": {type: Type.STRING}}, True),
            (["*"], {None: {type: Type.STRING}}, True),
            (["*"], {None: {type: Type.LONG}}, True),
            (["*"], {None: {type: Type.DATETIME}}, True),
            ([("*", {type: Type.STRING})], {"dim_name": {type: Type.STRING}}, True),
            ([("*", {type: Type.STRING})], {"dim_name": {type: Type.LONG}}, False),
            (["_:-1"], {"dim_name": {type: Type.STRING}}, True),
            (["_:-1"], {None: {type: Type.STRING}}, True),
            (["_:-1"], {None: {type: Type.LONG}}, True),
            (["_:-1"], {None: {type: Type.DATETIME}}, True),
            ([("_:-1", {type: Type.STRING})], {"dim_name": {type: Type.STRING}}, True),
            ([("_:-1", {type: Type.STRING})], {"dim_name": {type: Type.LONG}}, False),
            (["str"], {"dim_name": {type: Type.STRING}}, True),
            ([("str", {type: Type.STRING})], {"dim_name": {type: Type.STRING}}, True),
            # in case of type ambuigity String will win (as the only tentative).
            (["1.01"], {"dim_name": {type: Type.STRING}}, True),
            (["0.9"], {"dim_name": {type: Type.STRING}}, True),
            ([1.01], {"dim_name": {type: Type.LONG}}, True),
            ([0.9], {"dim_name": {type: Type.LONG}}, True),
            # even if the input is string, Long will win here due to no ambiguity.
            (["1"], {"dim_name": {type: Type.LONG}}, True),
            ([100], {"dim_name": {type: Type.LONG}}, True),
            ([(100, {type: Type.LONG})], {"dim_name": {type: Type.LONG}}, True),
            ([datetime.now()], {"dim_name": {type: Type.DATETIME}}, True),
            (["2020-06-20"], {"dim_name": {type: Type.DATETIME}}, True),
            ([("2020-06-20", {type: Type.DATETIME, "format": "%m-%d-%Y"})], {"dim": {type: Type.DATETIME}}, True),
            (["str", 1, "2020-06-20"], {"dim1": {type: Type.STRING, "dim2": {type: Type.LONG, "dim3": {type: Type.DATETIME}}}}, True),
            (["*", "*", "*"], {None: {type: Type.STRING, None: {type: Type.LONG, None: {type: Type.DATETIME}}}}, True),
        ],
    )
    def test_dimension_filter_check_spec_match_filter_as_list_to_spec(self, lhs, rhs_spec, result):
        left_filter = DimensionFilter.load_raw(lhs)
        right_spec = lhs if isinstance(rhs_spec, DimensionSpec) else DimensionSpec.load_from_pretty(rhs_spec)
        assert left_filter.check_spec_match(right_spec) == result

    @pytest.mark.parametrize(
        "lhs, rhs_spec",
        [
            ({}, {}),
            (DimensionFilter(), DimensionSpec()),
            ({}, DimensionSpec()),
            (DimensionFilter(), {}),
            (dimension_filter_basic_str_1, {"dim_name": {type: Type.STRING}}),
            (dimension_filter_basic_long_1, {"dim_name": {type: Type.LONG}}),
            (dimension_filter_basic_datetime_1, {"dim_name": {type: Type.DATETIME}}),
            (dimension_filter_basic_any, {"dim_name": {type: Type.LONG}}),
            (dimension_filter_basic_any, {"dim_name": {type: Type.DATETIME}}),
            (dimension_filter_basic_any, {"dim_name": {type: Type.STRING}}),
            (dimension_filter_basic_relative_1, {"dim_name": {type: Type.LONG}}),
            (dimension_filter_basic_relative_1, {"dim_name": {type: Type.STRING}}),
            (dimension_filter_basic_relative_1, {"dim_name": {type: Type.DATETIME}}),
            (
                {"*": {"_:-15": {}}},
                {
                    "dim": {type: Type.STRING, "dim2": {type: Type.DATETIME}},
                },
            ),
            (
                DimensionFilter([AnyVariant(), AnyVariant()], [None, None]),
                {
                    "dim": {
                        type: Type.STRING,
                    },
                    "dim2": {type: Type.DATETIME},
                },
            ),
            (
                {"*": {}, "100": {}, "101": {}, "102": {}, "2020-05-11": {}, "2020-05-12": {}},
                {
                    "dim": {
                        type: Type.STRING,
                    },
                    "dim2": {
                        type: Type.LONG,
                    },
                    "dim3": {type: Type.DATETIME, "format": "%d-%m-%Y", "timezone": "PST"},
                },
            ),
            (
                {"*": {"100": {}, "101": {}, "102": {}, "str_value1": {}, "str_value2": {}}, "NA": {"2020-05-11": {}, "2020-05-12": {}}},
                {
                    "dim": {
                        type: Type.STRING,
                        "dim2": {
                            type: Type.LONG,
                        },
                        "dim3": {type: Type.STRING},
                    },
                    "region": {type: Type.STRING, "day": {type: Type.DATETIME, "format": "%d-%m-%Y", "timezone": "PST"}},
                },
            ),
            (
                {"*": {"*": {}}},
                {  # extra type at the second level (valid for set_spec, second one will be ignored)
                    "dim": {type: Type.LONG, "dim1": {type: Type.STRING}, "dim2": {type: Type.STRING}},
                },
            ),
            (
                {3: {type: Type.LONG, "2020-12-04 00:00:00": {type: Type.DATETIME}}},
                {  # use another filter (with extra variants) as a spec (happens in runtime, see Signal::create)
                    3: {
                        type: Type.LONG,
                        "2020-12-04 00:00:00": {
                            type: Type.DATETIME,
                            "format": "%d-%m-%Y",  # these params will be transfered over
                            "timezone": "UTC",
                        },
                        "2020-12-03 00:00:00": {
                            type: Type.DATETIME,
                            "format": "%d-%m-%Y",  # these params will be transfered over
                            "timezone": "UTC",
                        },
                    }
                },
            ),
        ],
    )
    def test_dimension_filter_set_spec(self, lhs, rhs_spec):
        left_filter = copy.deepcopy(lhs) if isinstance(lhs, DimensionFilter) else DimensionFilter.load_raw(lhs)
        right_spec = rhs_spec if isinstance(rhs_spec, DimensionSpec) else DimensionSpec.load_from_pretty(rhs_spec)
        left_filter.set_spec(right_spec)

    @pytest.mark.parametrize(
        "lhs, rhs_spec",
        [
            (dimension_filter_basic_str_1, DimensionSpec()),
            (dimension_filter_basic_str_1, {}),
            (dimension_filter_basic_str_1, {"dim_name": {type: Type.LONG}}),
            (dimension_filter_basic_str_1, {"dim_name": {type: Type.DATETIME}}),
            (dimension_filter_basic_long_1, {"dim_name": {type: Type.STRING}}),
            (dimension_filter_basic_long_1, {"dim_name": {type: Type.DATETIME}}),
            (dimension_filter_basic_datetime_1, {"dim_name": {type: Type.STRING}}),
            (dimension_filter_basic_datetime_1, {"dim_name": {type: Type.LONG}}),
            (dimension_filter_basic_any_type_long, {"dim_name": {type: Type.STRING}}),
            (dimension_filter_basic_any_type_long, {"dim_name": {type: Type.DATETIME}}),
            (dimension_filter_basic_relative_1_type_datetime, {"dim_name": {type: Type.STRING}}),
            (dimension_filter_basic_relative_1_type_datetime, {"dim_name": {type: Type.LONG}}),
            ({"*": {}}, {}),
            ({"*": {"_:-15": {}}}, {"dim": {type: Type.STRING}}),  # missing nested type
            (
                {"*": {"_:-15": {}}},
                {  # extra nested type
                    "dim": {type: Type.STRING, "dim2": {type: Type.DATETIME, "dim3": {type: Type.LONG}}},
                },
            ),
            (
                DimensionFilter([AnyVariant(), AnyVariant()], [None, None]),
                {  # missing type at the root level
                    "dim": {
                        type: Type.STRING,
                    },
                },
            ),
            (
                {"*": {"*": {}, "_:-3": {}}},  # required type
                {  # missing type at the second level
                    "dim": {type: Type.LONG, "dim1": {type: Type.STRING}},
                },
            ),
            (
                {"*": {}, "100": {}, "101": {}, "102": {}, "2020-05-11": {}, "2020-05-12": {}},
                {
                    "dim": {
                        type: Type.STRING,
                    },
                    "dim2": {  # wrong type (must be LONG)
                        type: Type.STRING,
                    },
                    "dim3": {type: Type.DATETIME, "format": "%d-%m-%Y", "timezone": "PST"},
                },
            ),
            (
                {"*": {"100": {}, "101": {}, "102": {}, "str_value1": {}, "str_value2": {}}, "NA": {"str_param1": {}, "str_param2": {}}},
                {
                    "dim": {
                        type: Type.STRING,
                        "dim2": {
                            type: Type.LONG,
                        },
                        "dim3": {type: Type.STRING},
                    },
                    "region": {
                        type: Type.STRING,
                        "day": {type: Type.DATETIME, "format": "%d-%m-%Y", "timezone": "PST"},  # wrong type (must be STRING)
                    },
                },
            ),
        ],
    )
    def test_dimension_filter_set_spec_failure(self, lhs, rhs_spec):
        left_filter = copy.deepcopy(lhs) if isinstance(lhs, DimensionFilter) else DimensionFilter.load_raw(lhs)
        right_spec = rhs_spec if isinstance(rhs_spec, DimensionSpec) else DimensionSpec.load_from_pretty(rhs_spec)
        with pytest.raises(TypeError):
            left_filter.set_spec(right_spec)

    @pytest.mark.parametrize(
        "raw_list",
        [
            ([]),
            (["*"]),
            (["_:-1"]),
            (["str"]),
            (["1"]),
            ([100]),
            ([datetime.now()]),
            (["2020-06-20"]),
            ([("str", {type: Type.STRING})]),
            ([(100, {type: Type.LONG})]),
            ([("2020-06-20", {type: Type.DATETIME, "format": "%m-%d-%Y"})]),
            (["str", 1, "2020-06-20"]),
            (["*", "*", "*"]),
        ],
    )
    def test_dimension_filter_load_raw_list(self, raw_list):
        assert DimensionFilter.load_raw(raw_list) is not None

    @pytest.mark.parametrize(
        "raw_list, cast, variant_type, data_type",
        [
            ([], {"dim": {type: Type.STRING}}, None, None),  # during cast layout does not matter unless there is ambiguity
            (["*"], {"dim": {type: Type.DATETIME}}, AnyVariant, Type.DATETIME),
            (["*"], {"dim": {type: Type.STRING}}, AnyVariant, Type.STRING),
            (["_:-1"], {"dim": {type: Type.DATETIME}}, RelativeVariant, Type.DATETIME),
            (["_:-1:-1"], {"dim": {type: Type.DATETIME}}, RelativeVariant, Type.DATETIME),
            ([1], {"dim": {type: Type.STRING}}, StringVariant, Type.STRING),
            ([1], {"dim": {type: Type.LONG}}, LongVariant, Type.LONG),
            ([1], {}, LongVariant, Type.LONG),
            (["1"], {"dim": {type: Type.STRING}}, StringVariant, Type.STRING),
            (["1"], {"dim": {type: Type.LONG}}, LongVariant, Type.LONG),
            # auto-inference
            (["1"], {}, LongVariant, Type.LONG),
        ],
    )
    def test_dimension_filter_load_raw_list_with_cast_spec(self, raw_list, cast, variant_type, data_type):
        filter = DimensionFilter.load_raw(raw_list, DimensionSpec.load_from_pretty(cast))
        assert filter is not None
        if variant_type is None:
            assert not filter
        else:
            variant = next(iter(filter.get_root_dimensions()))
            assert isinstance(variant, variant_type)
            assert variant.type == data_type

    @pytest.mark.parametrize(
        "raw_filter",
        [
            # first check wrong type declaration (not a commonly used case, but our API support this)
            ({"str": {type: Type.LONG}}),  # LongVariant won't init and raise because of enforced 'str' value on it
            ([("str", {type: Type.LONG})]),
            ([("gibberish", {type: Type.DATETIME})]),
            ({datetime.now(): {type: Type.LONG}}),  # will cause TypeError as type does not match the resolved variant (DateVariant)
            ([(datetime.now(), {type: Type.LONG})]),
        ],
    )
    def test_dimension_filter_load_raw_failure(self, raw_filter):
        assert DimensionFilter.load_raw(raw_filter) is None

    @pytest.mark.parametrize(
        "raw_filter",
        [
            (
                # test how DimensionFilter allows dimensions with the same name (spec) at the same levels.
                {
                    "NA": {
                        "name": "region",
                        2: {
                            "2020-06-09": {},
                            "2020-06-10": {},
                        },
                    },
                    "EU": {
                        "name": "region",
                        2: {
                            "2020-06-09": {},
                            "2020-06-10": {},
                        },
                    },
                }
            )
        ],
    )
    def test_dimension_filter_load_raw_corner_cases(self, raw_filter):
        assert DimensionFilter.load_raw(raw_filter) is not None

    @pytest.mark.parametrize(
        "raw_dict, cast, variant_type, data_type",
        [
            ({}, {"dim": {type: Type.STRING}}, None, None),  # during cast layout does not matter unless there is ambiguity
            ({"*": {}}, {"dim": {type: Type.DATETIME}}, AnyVariant, Type.DATETIME),
            ({"*": {}}, {"dim": {type: Type.STRING}}, AnyVariant, Type.STRING),
            ({"_:-1": {}}, {"dim": {type: Type.DATETIME}}, RelativeVariant, Type.DATETIME),
            ({"_:-1:-1": {}}, {"dim": {type: Type.DATETIME}}, RelativeVariant, Type.DATETIME),
            ({1: {}}, {"dim": {type: Type.STRING}}, StringVariant, Type.STRING),
            ({1: {}}, {"dim": {type: Type.LONG}}, LongVariant, Type.LONG),
            ({1: {}}, {"dim": {type: Type.LONG}, "dim2": {type: Type.STRING}}, LongVariant, Type.LONG),  # extra type won't be used
            ({1: {}}, {"dim": {type: Type.STRING}, "dim2": {type: Type.LONG}}, StringVariant, Type.STRING),  # extra type won't be used
            ({1: {}, 2: {}}, {"dim": {type: Type.LONG}, "dim2": {type: Type.STRING}}, LongVariant, Type.LONG),
        ],
    )
    def test_dimension_filter_load_raw_dict_with_cast_spec(self, raw_dict, cast, variant_type, data_type):
        filter = DimensionFilter.load_raw(raw_dict, DimensionSpec.load_from_pretty(cast))
        assert filter is not None
        if variant_type is None:
            assert not filter
        else:
            variant = next(iter(filter.get_root_dimensions()))
            assert isinstance(variant, variant_type)
            assert variant.type == data_type

    @pytest.mark.parametrize(
        "raw_dict, cast",
        [
            # error: raw variant count > spec type count  causes  ambiguity
            ({1: {}, 2: {}, 3: {}}, {"dim": {type: Type.LONG}, "dim2": {type: Type.STRING}})
        ],
    )
    def test_dimension_filter_load_raw_dict_with_cast_spec_failure(self, raw_dict, cast):
        assert DimensionFilter.load_raw(raw_dict, DimensionSpec.load_from_pretty(cast)) is None

    @pytest.mark.parametrize(
        "raw_filter, formatted_string_value, raw_typed_value, raw_string",
        [
            ({"str": {}}, "str", "str", "str"),
            ({"StR": {"format": LOWER}}, "str", "str", "StR"),
            ({"sTr": {"format": UPPER}}, "STR", "STR", "sTr"),
            ({"str": {"format": lambda dim: "_".join(list(dim))}}, "s_t_r", "s_t_r", "str"),
        ],
    )
    def test_dimension_filter_stringvariant_formatting(self, raw_filter, formatted_string_value, raw_typed_value, raw_string):
        filter = DimensionFilter.load_raw(raw_filter)
        variant = next(iter(filter.get_root_dimensions()))
        assert isinstance(variant, StringVariant)
        assert cast(StringVariant, variant).value == formatted_string_value
        assert cast(StringVariant, variant).raw_value == raw_typed_value
        assert cast(StringVariant, variant).raw_string == raw_string

    @pytest.mark.parametrize(
        "raw_filter",
        [
            (
                {
                    "str": {
                        # change type to int
                        "format": lambda dim: 5
                    }
                }
            ),
            (
                {
                    "str": {
                        # change type to int
                        "format": lambda dim: "5"
                    }
                }
            ),
            (
                {
                    "str": {
                        # change type to date
                        "format": lambda dim: "2021-03-11"
                    }
                }
            ),
        ],
    )
    def test_dimension_filter_stringvariant_formatting_error(self, raw_filter):
        with pytest.raises(ValueError):
            DimensionFilter.load_raw(raw_filter, error_out=True)

    @pytest.mark.parametrize(
        "raw_filter, raw_string, result",
        [
            ({"str": {INSENSITIVE: True}}, "STR", True),
            ({"str": {}}, "STr", False),
            ({"str": {INSENSITIVE: False}}, StringVariant("STr"), False),
            # make that left side controls the case sensitivity during equality
            ({"str": {INSENSITIVE: False}}, StringVariant("STr", name=None, params={"insensitive": True}), False),
        ],
    )
    def test_dimension_filter_stringvariant_case_sensitivity(self, raw_filter, raw_string, result: bool):
        filter = DimensionFilter.load_raw(raw_filter)
        variant = next(iter(filter.get_root_dimensions()))
        assert isinstance(variant, StringVariant)
        assert result == (variant == raw_string)

    @pytest.mark.parametrize(
        "raw_filter, long_value_with_digits",
        [
            ({"1": {}}, 1),
            ({1: {}}, 1),
            ({"1": {"digits": 0}}, 1),
            ({1: {"digits": 0}}, 1),
            ({"1": {"digits": 1}}, 1),
            ({1: {"digits": 1}}, 1),
            ({"1": {"digits": 2}}, "01"),
            ({1: {"digits": 2}}, "01"),
        ],
    )
    def test_dimension_filter_long_digits(self, raw_filter, long_value_with_digits):
        filter = DimensionFilter.load_raw(raw_filter)
        variant = next(iter(filter.get_root_dimensions()))
        assert isinstance(variant, LongVariant)
        assert cast(LongVariant, variant).value == long_value_with_digits

    @pytest.mark.parametrize(
        "raw_filter, date_value, date_raw_value",
        [
            ({"2020-05-06": {}}, "2020-05-06 00:00:00", datetime(2020, 5, 6)),
            ({date(2020, 5, 6): {}}, "2020-05-06 00:00:00", datetime(2020, 5, 6)),
            ({"2020-05-06 01:00:00": {"format": "%m-%d-%Y"}}, "05-06-2020", datetime(2020, 5, 6, 1)),
            ({"2020-05-06 01:00:00": {}}, "2020-05-06 01:00:00", datetime(2020, 5, 6, 1)),
            ({"2020-05-06T01:00:00": {}}, "2020-05-06 01:00:00", datetime(2020, 5, 6, 1)),
            ({"2020-05-06-01:38:00": {}}, "2020-05-06 01:38:00", datetime(2020, 5, 6, 1, 38)),
            (
                {"2020-05-06-_@_01:38:01": {}},  # Test all valid separators from <DateVariant::_SUPPORTED_DATE_TIME_SEPARATORS>
                "2020-05-06 01:38:01",
                datetime(2020, 5, 6, 1, 38, 1),
            ),
            (
                {  # dateutil.tz.tzlocal is ALLOWED! tzoffet cases (such as "2020-05-06 01-03" where '-03' read as offset)
                    # are not allowed.
                    "2021-03-05T19:30:00Z": {}
                },
                "2021-03-05 19:30:00+00:00",
                datetime(2021, 3, 5, 19, 30), #  tzinfo=tzlocal()), # TODO on Windows tests tzutc() is set
            ),
            # Exceptional Formats which would normally not be allowed due to DATE -> LONG conversion. These are allowed
            # but should not be used on incoming events (since they will be resolved to LONGs due to missing type information
            # when read from raw resource paths).
            ({"2020-05-06 10:00": {type: Type.DATETIME, "format": "%Y%m%d"}}, "20200506", datetime(2020, 5, 6, 10)),
            ({"2020-05-06 10:00": {type: Type.DATETIME, "format": "%d%m%Y"}}, "06052020", datetime(2020, 5, 6, 10)),
            ({"2020-05-06 10:00": {type: Type.DATETIME, "format": "%m%d%Y"}}, "05062020", datetime(2020, 5, 6, 10)),
            (
                {
                    "2020-05-06 01:03": {
                        type: Type.DATETIME,
                        "format": "%Y-%m-%d %H-%M",  # '-' is not a valid a separator between hour and minute
                    }
                },
                "2020-05-06 01-03",
                datetime(2020, 5, 6, 1, 3),
            ),
            (
                {"2020-05-06 01:03": {type: Type.DATETIME, "format": "gibberish"}},  # check one-directional format
                "gibberish",
                datetime(2020, 5, 6, 1, 3),
            ),
            (
                {2024: {type: Type.DATETIME, "format": "%Y %H:%M"}},  # int -> datetime
                "2024 00:00",
                datetime(2024, datetime.now().month, datetime.now().day),
            ),
            (
                {
                    22: {
                        type: Type.DATETIME
                        # by default it will read as 'day' by IF inference
                    }
                },
                f"{datetime.now().year}-{str(datetime.now().month).zfill(2)}-22 00:00:00",
                datetime(datetime.now().year, datetime.now().month, 22),
            ),
            ({22: {type: Type.DATETIME, "format": "%d"}}, "22", datetime(datetime.now().year, datetime.now().month, 22)),
            (
                {12: {type: Type.DATETIME, "format": "%m"}},
                "12",
                # see the effect of %m here in the raw date value.
                datetime(datetime.now().year, 12, 1),
            ),
            ({"13": {type: Type.DATETIME, "format": "%H"}}, "13", datetime(datetime.now().year, datetime.now().month, 13, 13)),
            ({"52": {type: Type.DATETIME, "format": "%M"}}, "52", datetime(1900, 1, 1, 0, 52)),
            (
                {"02": {type: Type.DATETIME, "format": "%M"}},  # use a min value that overlaps with day, month, hour range
                "02",
                datetime(1900, 1, 1, 0, 2),
            ),
            ({"59": {type: Type.DATETIME, "format": "%S"}}, "59", datetime(1900, 1, 1, 0, 0, 59)),
            (
                {"09": {type: Type.DATETIME, "format": "%S"}},  # use a sec value that overlaps with day, month, hour, min range
                "09",
                datetime(1900, 1, 1, 0, 0, 9),
            ),
        ],
    )
    def test_dimension_filter_datevariant_corner_cases(self, raw_filter, date_value, date_raw_value):
        filter = DimensionFilter.load_raw(raw_filter)
        variant = next(iter(filter.get_root_dimensions()))
        assert isinstance(variant, DateVariant)
        assert cast(DateVariant, variant).value == date_value
        assert cast(DateVariant, variant).raw_value.replace(tzinfo=None) == date_raw_value

    @pytest.mark.parametrize(
        "raw_filter, date_value, date_raw_value",
        [
            (
                {"2022-11-4": {type: Type.DATETIME, "min": datetime(2022, 11, 3)}},
                "2022-11-04 00:00:00",
                datetime(2022, 11, 4, 0, 0, 0),
            ),
            (
                {"2022-11-4": {type: Type.DATETIME, "min": "2022-11-3"}},
                "2022-11-04 00:00:00",
                datetime(2022, 11, 4, 0, 0, 0),
            ),
            (
                # here relative_min will automatically use "granularity" defined on this variant.
                # so 2 here means "2 days earlier from now"
                {datetime_now - timedelta(days=1): {type: Type.DATETIME, "relative_min": 2, "format": "%Y-%m-%d %H:%M:%S"}},
                (datetime_now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
                datetime_now - timedelta(days=1),
            ),
            (
                {datetime_now: {type: Type.DATETIME, "relative_min": timedelta(2), "format": "%Y-%m-%d %H:%M:%S"}},
                # use a sec value that overlaps with day, month, hour, min range
                datetime_now.strftime("%Y-%m-%d %H:%M:%S"),
                datetime_now,
            ),
        ],
    )
    def test_dimension_filter_datevariant_min_and_relative_min_parameters(self, raw_filter, date_value, date_raw_value):
        filter = DimensionFilter.load_raw(raw_filter)
        assert filter, "DimensionFilter must have been loaded successfully!"
        variant = next(iter(filter.get_root_dimensions()))
        assert isinstance(variant, DateVariant)
        assert cast(DateVariant, variant).value == date_value
        assert cast(DateVariant, variant).raw_value == date_raw_value or variant.raw_value.date() == date_raw_value

    @pytest.mark.parametrize(
        "raw_filter",
        [
            ({"2022-11-3": {type: Type.DATETIME, "min": datetime(2022, 11, 4)}},),
            ({"2022-11-3": {type: Type.DATETIME, "min": "2022-11-4"}},),
            ({"2022-11-3 00:00:00": {type: Type.DATETIME, "min": "2022-11-3 00:00:01"}},),
            ({datetime_now - timedelta(days=3): {type: Type.DATETIME, "relative_min": timedelta(days=1)}},),
            (
                # here 2 maps to "2 hours earlier from now" because granularity is set as HOUR
                {datetime_now - timedelta(hours=4): {type: Type.DATETIME, "granularity": DatetimeGranularity.HOUR, "relative_min": 2}},
            ),
            (
                # here relative_min will automatically use "granularity" defined on this variant.
                # so 2 here means "2 days earlier from now". because default granularity is DAY.
                {datetime_now - timedelta(days=4): {type: Type.DATETIME, "relative_min": 2}},
            ),
        ],
    )
    def test_dimension_filter_datevariant_min_and_relative_min_reject_date_value(self, raw_filter):
        assert DimensionFilter.load_raw(raw_filter) is None

    def test_dimension_filter_datevariant_min_chain(self):
        filter = DimensionFilter.load_raw({"*": {type: Type.DATETIME, "min": "2022-11-01"}})

        incoming_filter = {"2022-10-20": {}}
        # first make sure that normally this filter cannot be created using the spec from "filter"
        assert DimensionFilter.load_raw(incoming_filter, cast=filter.get_spec()) is None
        # but let's test the behavior if we create the raw incoming filter and apply to the datetime with "min" condition
        with pytest.raises(ValueError):
            new_filter = filter.chain(DimensionFilter.load_raw(incoming_filter), False)

        # but if it is within range, then everything should be ok
        incoming_filter = {"2022-11-01": {}}
        new_filter = filter.chain(DimensionFilter.load_raw(incoming_filter), False)
        assert new_filter

    def test_dimension_filter_datevariant_relative_min_chain(self):
        filter = DimensionFilter.load_raw({"*": {type: Type.DATETIME, "relative_min": timedelta(2)}})

        incoming_filter = {datetime.utcnow() - timedelta(days=4): {}}  # way too old
        with pytest.raises(ValueError):
            filter.chain(DimensionFilter.load_raw(incoming_filter), False)

        incoming_filter = {datetime.utcnow(): {}}  # recent
        new_filter = filter.chain(DimensionFilter.load_raw(incoming_filter), False)
        assert new_filter

    @pytest.mark.parametrize(
        "raw_filter",
        [
            ({"gibberish": {type: Type.DATETIME}}),  # otherwise will be resolved as StringVariant
            (
                {  # '-' as a separator between time fields is not good
                    # resolver yields unmatch but type is honored and a new DateVariant is created.
                    # However, DateVariant checks if the datetime instance is mistakenly interpreted with timezone
                    # (here minutes component (-3) becomes a tzinfo component). This causes ValueError.
                    "2020-05-06 01-03": {
                        type: Type.DATETIME,
                    }
                }
            ),
            ({object(): {}}),
        ],
    )
    def test_dimension_filter_cannot_resolve(self, raw_filter):
        assert DimensionFilter.load_raw(raw_filter) is None

    @pytest.mark.parametrize(
        "raw_filter, type",
        [
            ({"2020-05-06": {type: Type.STRING}}, StringVariant),
            ({"2020-05-06": {type: Type.DATETIME}}, DateVariant),
            (
                {
                    "1": {
                        type: Type.STRING,
                    }
                },
                StringVariant,
            ),
            (
                {
                    1: {
                        type: Type.STRING,
                    }
                },
                StringVariant,
            ),
            (
                {
                    1: {
                        type: Type.LONG,
                    }
                },
                LongVariant,
            ),
            (
                {
                    20210714: {
                        type: Type.LONG,
                    }
                },
                LongVariant,
            ),
            (
                {
                    "20210714": {
                        type: Type.LONG,
                    }
                },
                LongVariant,
            ),
            (
                {
                    "20210714": {
                        type: Type.DATETIME,
                    }
                },
                DateVariant,
            ),
            (
                {
                    "102107": {
                        type: Type.LONG,
                    }
                },
                LongVariant,
            ),
            ({"12107": {}}, LongVariant),  # and if type is not provided it will be resolved to be LONG (due to DateVariant size >= 6 logic)
            (
                {
                    "102107": {  # shoud be resolved as datetime(2007, 10, 21, 0, 0)
                        type: Type.DATETIME,
                    }
                },
                DateVariant,
            ),
        ],
    )
    def test_dimension_filter_honors_user_provided_type(self, raw_filter, type):
        filter = DimensionFilter.load_raw(raw_filter, error_out=True)
        variant = next(iter(filter.get_root_dimensions()))
        assert isinstance(variant, type)

    @pytest.mark.parametrize(
        "raw_filter",
        [
            (
                {
                    20210713.00: {  # will implicitly cast to str but the outcome will still not be ok (20210713 would be ok)
                        type: Type.DATETIME
                    }
                }
            ),
            ({"202107": {type: Type.DATETIME}}),  # will fail due to bad month
            ({"str": {type: Type.LONG}}),
        ],
    )
    def test_dimension_filter_cannot_cast(self, raw_filter):
        assert DimensionFilter.load_raw(raw_filter) is None

    @pytest.mark.parametrize(
        "raw_filter",
        [
            (
                {
                    "2020-05-06foo": {
                        # this is tentative datetime but still user's type will be picked up.
                        type: Type.STRING,
                    }
                }
            ),
            (
                {
                    "Foo2020-05-06": {
                        type: Type.STRING,
                    }
                }
            ),
        ],
    )
    def test_dimension_filter_resolves_user_provided_type_due_to_ambiguity(self, raw_filter):
        assert DimensionFilter.load_raw(raw_filter)

    @pytest.mark.parametrize(
        "raw_filter",
        [
            {
                # "-03" interpreted as timezone, then DateVariant yields UNMATCH
                "2020-05-06 01-03": {}
            }
        ],
    )
    def test_dimension_filter_rejected_datevariant_auto_resolved_into_string(self, raw_filter):
        filter = DimensionFilter.load_raw(raw_filter)
        variant = next(iter(filter.get_root_dimensions()))
        assert isinstance(variant, StringVariant)

    @pytest.mark.parametrize(
        "raw_filter",
        [
            {  # tentatively resolved into both StringVariant and DateVariant. If type is not specified, StringVariant wins.
                "Foo2020-05-06": {}
            }
        ],
    )
    def test_dimension_filter_tentative_auto_resolved_into_string(self, raw_filter):
        filter = DimensionFilter.load_raw(raw_filter)
        variant = next(iter(filter.get_root_dimensions()))
        assert isinstance(variant, StringVariant)

    def test_dimension_filter_materialize(self):
        # TODO postponed this due to heavy coverage of this API in high-level modules (signal,
        #  routing_runtime_constructs, etc).
        #    (actual reason for this is the required heavy-lifting of DimensionVariantMappers which would require to
        #     traverse other filters (source_filters) and then be used during the projections onto the target spec)
        assert True
