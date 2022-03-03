# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from test.intelliflow.core.signal_processing.signal_source.test_internal_data_spec import TestInternalDataAccessSpec

import pytest

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.signal_source import *


class TestGlueTableDataAccessSpec:
    gt_data_access_spec = GlueTableSignalSourceAccessSpec("provider", "table", ["part_key"], None)

    gt_data_access_spec_cloned = copy.deepcopy(gt_data_access_spec)

    gt_data_access_spec_with_diff_data_format = GlueTableSignalSourceAccessSpec(
        "provider", "table", ["part_key"], None, **{DATASET_FORMAT_KEY: DatasetSignalSourceFormat.PARQUET}
    )

    @pytest.mark.parametrize(
        "provider, table, partition_keys, dimension_spec",
        [
            (None, "table", ["part_key"], None),
            (None, "table", ["part_key"], None),
            (None, "table", ["part_key"], DimensionSpec.load_from_pretty({"part_key": {type: Type.STRING}})),
            ("", "table", ["part_key"], None),
            ("", "table", ["part_key"], DimensionSpec.load_from_pretty({"part_key": {type: Type.STRING}})),
            ("provider", None, ["part_key"], None),
            ("provider", "", ["part_key"], None),
            ("provider", "table", None, None),
        ],
    )
    def test_validate_gt_data_signal_source_access(self, provider, table, partition_keys, dimension_spec):
        with pytest.raises(Exception) as error:
            GlueTableSignalSourceAccessSpec(provider, table, partition_keys, dimension_spec)
        assert error.typename == "ValueError"

    def test_gt_data_signal_source_access_spec_init(self):
        GlueTableSignalSourceAccessSpec(
            "provider", "table", ["part_key"], DimensionSpec.load_from_pretty({"part_key": {type: Type.STRING}})
        )

    def test_gt_data_signal_source_access_spec_api(self):
        assert (
            self.gt_data_access_spec.database == self.gt_data_access_spec_cloned.database
            and self.gt_data_access_spec.table_name == self.gt_data_access_spec_cloned.table_name
        )

        assert self.gt_data_access_spec.source == SignalSourceType.GLUE_TABLE
        assert self.gt_data_access_spec.path_format == f"glue_table://provider/table/{DIMENSION_PLACEHOLDER_FORMAT}"

        assert self.gt_data_access_spec.data_format == DEFAULT_DATASET_FORMAT
        assert self.gt_data_access_spec.partition_keys == ["part_key"]
        assert self.gt_data_access_spec_with_diff_data_format.data_format == DatasetSignalSourceFormat.PARQUET

    def test_gt_data_signal_source_access_spec_equality(self):
        assert self.gt_data_access_spec == self.gt_data_access_spec_cloned

    @pytest.mark.parametrize(
        "provider, table, partition_keys, dimension_spec",
        [
            ("provider1", "table", ["part_key"], None),
            ("provider", "table1", ["part_key"], None),
            ("provider", "table1", None, DimensionSpec.load_from_pretty({"part_key": {type: Type.STRING}})),
            ("provider", "table", ["part_key", "part_key1"], None),
            ("provider", "table", [], None),
        ],
    )
    def test_gt_data_signal_source_access_spec_inequality(self, provider, table, partition_keys, dimension_spec):
        assert self.gt_data_access_spec != GlueTableSignalSourceAccessSpec(provider, table, partition_keys, dimension_spec)

    def test_gt_data_signal_source_access_spec_serialization(self):
        assert self.gt_data_access_spec == loads(dumps(self.gt_data_access_spec))

    def test_gt_data_signal_source_access_spec_integrity(self):
        assert self.gt_data_access_spec.check_integrity(self.gt_data_access_spec_cloned)
        # code-path for super's integrity check yielding False
        assert not self.gt_data_access_spec.check_integrity(self.gt_data_access_spec_with_diff_data_format)
        assert not self.gt_data_access_spec.check_integrity(TestInternalDataAccessSpec.internal_data_access_spec)

    @pytest.mark.parametrize(
        "provider, table_name, partition_values, expected_path",
        [
            ("provider", "table", [], "glue_table://provider/table"),
            ("provider", "table", ["part1"], "glue_table://provider/table/part1"),
            ("provider", "table", ["part1", "part2"], "glue_table://provider/table/part1/part2"),
        ],
    )
    def test_gt_data_signal_source_access_spec_create_materialized_path(self, provider, table_name, partition_values, expected_path):
        assert GlueTableSignalSourceAccessSpec.create_materialized_path(provider, table_name, partition_values) == expected_path

    def test_gt_data_signal_source_access_spec_get_partitions(self):
        partitions = GlueTableSignalSourceAccessSpec.get_partitions(DimensionFilter.load_raw({1: {"2020-05-01": {}}}))
        assert partitions[0] == [1, "2020-05-01 00:00:00"]
