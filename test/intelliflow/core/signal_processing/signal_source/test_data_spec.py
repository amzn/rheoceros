# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from test.intelliflow.core.signal_processing.signal_source.test_basic_types import TestBasicTypes

import pytest

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.signal_source import *
from intelliflow.utils.spark import SparkType


class TestDataAccessSpec:
    access_spec_internal = SignalSourceAccessSpec(
        SignalSourceType.S3, f"s://bucket/my_test_data/{DIMENSION_PLACEHOLDER_FORMAT}/{DIMENSION_PLACEHOLDER_FORMAT}", {}
    )
    data_access_spec_internal = DatasetSignalSourceAccessSpec(
        SignalSourceType.S3, f"s://bucket/my_test_data/{DIMENSION_PLACEHOLDER_FORMAT}/{DIMENSION_PLACEHOLDER_FORMAT}", {}
    )

    data_access_spec_internal_cloned = copy.deepcopy(data_access_spec_internal)

    data_access_spec_internal_with_attrs = DatasetSignalSourceAccessSpec(
        SignalSourceType.S3,
        f"s://bucket/my_test_data/{DIMENSION_PLACEHOLDER_FORMAT}/{DIMENSION_PLACEHOLDER_FORMAT}",
        {
            DATASET_FORMAT_KEY: DatasetSignalSourceFormat.PARQUET,
            DATASET_TYPE_KEY: DatasetType.APPEND,
            PARTITION_KEYS_KEY: ["dim1"],
            PRIMARY_KEYS_KEY: ["prim1"],
        },
    )

    data_access_spec_internal_with_attrs_with_diff_schema = DatasetSignalSourceAccessSpec(
        data_access_spec_internal_with_attrs.source,
        data_access_spec_internal_with_attrs.path_format,
        {k: v for k, v in list(data_access_spec_internal_with_attrs.attrs.items()) + [(DATASET_SCHEMA_FILE_KEY, "SCHEMA.spark.json")]},
    )

    data_access_spec_internal_with_attrs_with_diff_schema_type = DatasetSignalSourceAccessSpec(
        data_access_spec_internal_with_attrs.source,
        data_access_spec_internal_with_attrs.path_format,
        {
            k: v
            for k, v in list(data_access_spec_internal_with_attrs.attrs.items())
            + [(DATASET_SCHEMA_TYPE_KEY, DatasetSchemaType.SPARK_SCHEMA_JSON)]
        },
    )

    data_access_spec_internal_with_attrs_with_extra = DatasetSignalSourceAccessSpec(
        SignalSourceType.S3,
        f"s://bucket/my_test_data/{DIMENSION_PLACEHOLDER_FORMAT}/{DIMENSION_PLACEHOLDER_FORMAT}",
        {
            DATASET_FORMAT_KEY: DatasetSignalSourceFormat.PARQUET,
            DATASET_TYPE_KEY: DatasetType.APPEND,
            PARTITION_KEYS_KEY: ["dim1"],
            PRIMARY_KEYS_KEY: ["prim1"],
            "dummy_key": "dummy_value",
        },
    )

    data_access_spec_internal_with_attrs_with_diff_schema_definition = DatasetSignalSourceAccessSpec(
        data_access_spec_internal_with_attrs.source,
        data_access_spec_internal_with_attrs.path_format,
        {
            k: v
            for k, v in list(data_access_spec_internal_with_attrs.attrs.items())
            + [(DATASET_SCHEMA_DEFINITION_KEY, [("column1", "type", True)])]
        },
    )

    def test_data_signal_source_access_spec_api(self):
        assert (
            self.data_access_spec_internal.source == self.data_access_spec_internal_cloned.source
            and self.data_access_spec_internal.path_format == self.data_access_spec_internal_cloned.path_format
            and self.data_access_spec_internal.attrs == self.data_access_spec_internal_cloned.attrs
            and self.data_access_spec_internal.dataset_type == self.data_access_spec_internal_cloned.dataset_type
            and self.data_access_spec_internal.data_format == self.data_access_spec_internal_cloned.data_format
            and self.data_access_spec_internal.partition_keys == self.data_access_spec_internal_cloned.partition_keys
            and self.data_access_spec_internal.primary_keys == self.data_access_spec_internal_cloned.primary_keys
        )

        assert self.data_access_spec_internal.data_format == DEFAULT_DATASET_FORMAT
        assert self.data_access_spec_internal.dataset_type == DEFAULT_DATASET_TYPE
        assert self.data_access_spec_internal.partition_keys == []
        assert self.data_access_spec_internal.primary_keys == []

        assert self.data_access_spec_internal_with_attrs.data_format == DatasetSignalSourceFormat.PARQUET
        assert self.data_access_spec_internal_with_attrs.dataset_type == DatasetType.APPEND
        assert self.data_access_spec_internal_with_attrs.partition_keys == ["dim1"]
        assert self.data_access_spec_internal_with_attrs.primary_keys == ["prim1"]

    def test_data_signal_source_access_spec_equality(self):
        assert self.data_access_spec_internal == self.data_access_spec_internal_cloned
        assert self.data_access_spec_internal_with_attrs != self.data_access_spec_internal_with_attrs_with_extra

    def test_data_signal_source_access_spec_integrity(self):
        assert self.data_access_spec_internal.check_integrity(self.data_access_spec_internal_cloned)
        assert self.data_access_spec_internal_with_attrs.check_integrity(self.data_access_spec_internal_with_attrs_with_extra)
        # should fail due to different parametrization
        assert not self.data_access_spec_internal.check_integrity(self.data_access_spec_internal_with_attrs)
        assert not self.data_access_spec_internal_with_attrs.check_integrity(self.data_access_spec_internal_with_attrs_with_diff_schema)
        assert not self.data_access_spec_internal_with_attrs.check_integrity(
            self.data_access_spec_internal_with_attrs_with_diff_schema_type
        )
        assert not self.data_access_spec_internal_with_attrs.check_integrity(
            self.data_access_spec_internal_with_attrs_with_diff_schema_definition
        )

        # verify type check even if the parametrization is same
        assert not self.data_access_spec_internal.check_integrity(self.access_spec_internal)

    def test_data_signal_source_access_spec_partition_key_count(self):
        filter_2 = DimensionSpec.load_from_pretty({"1": {type: Type.LONG, "2": {type: Type.LONG}, "2_1": {type: Type.STRING}}})
        filter_3 = DimensionSpec.load_from_pretty({"1": {type: Type.LONG, "2": {type: Type.LONG, "3": {type: Type.LONG}}}})

        assert DatasetSignalSourceAccessSpec.get_partition_key_count(filter_2) == 2
        assert DatasetSignalSourceAccessSpec.get_partition_key_count(filter_3) == 3
        assert DatasetSignalSourceAccessSpec.get_partition_key_count({}) == 0
        assert DatasetSignalSourceAccessSpec.get_partition_key_count(None) == 0

    def test_data_signal_source_access_spec_schema_definition(self):
        def _create_data_spec_with_schema(schema_def):
            return DatasetSignalSourceAccessSpec(
                self.data_access_spec_internal_with_attrs.source,
                self.data_access_spec_internal_with_attrs.path_format,
                {
                    k: v
                    for k, v in list(self.data_access_spec_internal_with_attrs.attrs.items())
                    + [(DATASET_SCHEMA_DEFINITION_KEY, schema_def)]
                },
            )

        assert _create_data_spec_with_schema(None), "'None' schema def should be ok!"

        # schema def must be a list
        with pytest.raises(ValueError):
            _create_data_spec_with_schema({})

        # cannot be an empty list
        with pytest.raises(ValueError):
            _create_data_spec_with_schema([])

        # go over various cases bad field definitions
        with pytest.raises(ValueError):
            _create_data_spec_with_schema([{}])

        with pytest.raises(ValueError):
            _create_data_spec_with_schema([1])

        with pytest.raises(ValueError):
            _create_data_spec_with_schema([1, 2, 3])

        with pytest.raises(ValueError):
            _create_data_spec_with_schema([()])

        # name should be a string
        with pytest.raises(ValueError):
            _create_data_spec_with_schema([(1, "IntegerType()", True)])

        _create_data_spec_with_schema([("column1", "IntegerType()", True)])

        # check data "type" now
        with pytest.raises(ValueError):
            _create_data_spec_with_schema([("column1", None, True)])

        with pytest.raises(ValueError):
            _create_data_spec_with_schema([("column1", 1234, True)])

        # nullable should be boolean
        with pytest.raises(ValueError):
            _create_data_spec_with_schema([("column1", "IntegerType()", "True")])

        # check Nested definitions
        #
        #  nested definition cannot be empty
        with pytest.raises(ValueError):
            _create_data_spec_with_schema([("column1", "IntegerType()", True), ("column2", [], True)])
        # nested definition cannot be empty
        with pytest.raises(ValueError):
            _create_data_spec_with_schema([("column1", "IntegerType()", True), ("column2", None, True)])

        _create_data_spec_with_schema([("column1", "IntegerType()", True), ("column2", [("column2_1", "my_type", True)], True)])

        array = SparkType.ArrayType(SparkType.StringType)
        map = SparkType.MapType(SparkType.StringType, SparkType.LongType)
        _create_data_spec_with_schema([("column1", array, True), ("column2", [("column2_1", map, True)], True)])

        # validate nested level 2
        _create_data_spec_with_schema(
            [
                ("column1", SparkType.IntegerType, True),
                ("column2", [("column2_1", [("column2_1_1", SparkType.TimestampType, True)], True)], True),
            ]
        )

        with pytest.raises(ValueError):
            _create_data_spec_with_schema(
                [
                    ("column1", SparkType.IntegerType, True),
                    (
                        "column2",
                        [
                            (
                                "column2_1",
                                [
                                    # missing name
                                    (SparkType.TimestampType, True)
                                ],
                                True,
                            )
                        ],
                        True,
                    ),
                ]
            )
