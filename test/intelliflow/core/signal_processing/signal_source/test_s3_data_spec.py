# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from test.intelliflow.core.signal_processing.signal_source.test_internal_data_spec import TestInternalDataAccessSpec

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.signal_source import *


class TestS3DataAccessSpec:
    s3_data_access_spec = S3SignalSourceAccessSpec(
        "111222333444", "bucket", "my_data_1", DimensionSpec.load_from_pretty({"dim1": {"dim2": {}}}, False), **{}
    )

    s3_data_access_spec_cloned = copy.deepcopy(s3_data_access_spec)

    s3_data_access_wo_folder = S3SignalSourceAccessSpec(
        "111222333444", "bucket", "", DimensionSpec.load_from_pretty({"dim1": {"dim2": {}}}, False), **{}
    )

    s3_data_access_spec_with_partitions = S3SignalSourceAccessSpec("111222333444", "bucket", "my_data_1", "{}", "{}")

    s3_data_access_spec_from_internal = S3SignalSourceAccessSpec(
        "111222333444", "bucket", TestInternalDataAccessSpec.internal_data_access_spec
    )

    s3_data_access_spec_from_internal_and_redundant_params = S3SignalSourceAccessSpec(
        "111222333444", "bucket", TestInternalDataAccessSpec.internal_data_access_spec, "dimX", "dimY"
    )

    s3_data_access_spec_with_diff_params = S3SignalSourceAccessSpec("111222333444", "bucket", "my_data_1", "dim1", "dim2", "dim3")

    s3_data_access_spec_with_dim_eccentric_dim_1 = S3SignalSourceAccessSpec("111222333444", "bucket", "my_data_1", "dim_key={}")

    s3_data_access_spec_with_diff_id = S3SignalSourceAccessSpec("111222333444", "my_data_2", "dim1", "dim2", **{})

    s3_data_access_spec_with_diff_account_id = S3SignalSourceAccessSpec("222333444555", "my_data_1", "dim1", "dim2", **{})

    s3_data_access_spec_with_diff_data_format = S3SignalSourceAccessSpec(
        "111222333444",
        "bucket",
        "my_data_1",
        DimensionSpec.load_from_pretty({"dim1": {"dim2": {}}}, False),
        **{DATASET_FORMAT_KEY: DatasetSignalSourceFormat.PARQUET},
    )

    def test_s3_data_signal_source_access_spec_api(self):
        assert (
            self.s3_data_access_spec.account_id == self.s3_data_access_spec_cloned.account_id
            and self.s3_data_access_spec.bucket == self.s3_data_access_spec_cloned.bucket
            and self.s3_data_access_spec.folder == self.s3_data_access_spec_cloned.folder
        )

        assert self.s3_data_access_spec.source == SignalSourceType.S3
        assert (
            self.s3_data_access_spec.path_format == f"s3://bucket/my_data_1/{DIMENSION_PLACEHOLDER_FORMAT}/{DIMENSION_PLACEHOLDER_FORMAT}"
        )
        assert self.s3_data_access_spec.folder == "my_data_1"

        assert self.s3_data_access_spec.data_format == DEFAULT_DATASET_FORMAT
        assert self.s3_data_access_spec.dataset_type == DEFAULT_DATASET_TYPE
        assert self.s3_data_access_spec.partition_keys == []
        assert self.s3_data_access_spec.primary_keys == []

    def test_s3_data_signal_source_access_spec_equality(self):
        assert self.s3_data_access_spec == self.s3_data_access_spec_cloned
        assert self.s3_data_access_spec == self.s3_data_access_spec_with_partitions
        assert self.s3_data_access_spec_from_internal == S3SignalSourceAccessSpec(
            "111222333444",
            "bucket",
            f"{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_data_1",
            DimensionSpec.load_from_pretty({"dim1": {"dim2": {}}}, False),
            **{
                DATASET_SCHEMA_FILE_KEY: InternalDatasetSignalSourceAccessSpec.build_schema_file_name(),
                DATASET_SCHEMA_TYPE_KEY: InternalDatasetSignalSourceAccessSpec.SCHEMA_FILE_DEFAULT_TYPE,
            },
        )
        assert self.s3_data_access_spec_from_internal == self.s3_data_access_spec_from_internal_and_redundant_params
        assert self.s3_data_access_spec != self.s3_data_access_spec_with_diff_id
        assert self.s3_data_access_spec != self.s3_data_access_spec_with_diff_account_id
        assert self.s3_data_access_spec != self.s3_data_access_spec_with_diff_params

    def test_s3_data_signal_source_access_spec_serialization(self):
        assert self.s3_data_access_spec == loads(dumps(self.s3_data_access_spec))

    def test_s3_data_signal_source_access_spec_integrity(self):
        assert self.s3_data_access_spec.check_integrity(self.s3_data_access_spec_cloned)
        # code-path for super's integrity check yielding False
        assert not self.s3_data_access_spec.check_integrity(self.s3_data_access_spec_with_diff_data_format)
        # verify type check even if the parametrization is same
        assert not self.s3_data_access_spec.check_integrity(TestInternalDataAccessSpec.internal_data_access_spec)

    def test_s3_signal_source_access_spec_path_format_with_eccentric_dim_1(self):
        resp = self.s3_data_access_spec_with_dim_eccentric_dim_1.extract_source("s3://bucket/my_data_1/dim_key=testVal/_SUCCESS")
        assert resp.dimension_values[0] == "testVal"

    def test_s3_signal_source_access_spec_path_format_wo_folder(self):
        path_format = self.s3_data_access_wo_folder.path_format
        folder = self.s3_data_access_wo_folder.folder
        assert folder == ""
        assert path_format == "s3://bucket/{}/{}"
