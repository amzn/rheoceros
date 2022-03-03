# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from test.intelliflow.core.signal_processing.signal_source.test_basic_types import TestBasicTypes

import pytest

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.signal_source import *


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
        # verify type check even if the parametrization is same
        assert not self.data_access_spec_internal.check_integrity(self.access_spec_internal)

    def test_data_signal_source_access_spec_partition_key_count(self):
        filter_2 = DimensionSpec.load_from_pretty({"1": {type: Type.LONG, "2": {type: Type.LONG}, "2_1": {type: Type.STRING}}})
        filter_3 = DimensionSpec.load_from_pretty({"1": {type: Type.LONG, "2": {type: Type.LONG, "3": {type: Type.LONG}}}})

        assert DatasetSignalSourceAccessSpec.get_partition_key_count(filter_2) == 2
        assert DatasetSignalSourceAccessSpec.get_partition_key_count(filter_3) == 3
        assert DatasetSignalSourceAccessSpec.get_partition_key_count({}) == 0
        assert DatasetSignalSourceAccessSpec.get_partition_key_count(None) == 0
