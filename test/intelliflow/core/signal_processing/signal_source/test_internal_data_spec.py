# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from test.intelliflow.core.signal_processing.signal_source.test_data_spec import TestDataAccessSpec

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.signal_source import *


class TestInternalDataAccessSpec:
    internal_data_access_spec = InternalDatasetSignalSourceAccessSpec(
        "my_data_1", DimensionSpec.load_from_pretty({"dim1": {"dim2": {}}}, False), **{}
    )

    internal_data_access_spec_cloned = copy.deepcopy(internal_data_access_spec)

    internal_data_access_spec_with_partitions = InternalDatasetSignalSourceAccessSpec("my_data_1", "dim1", "dim2", **{})

    internal_data_access_spec_with_diff_params = InternalDatasetSignalSourceAccessSpec("my_data_1", "dim1", "dim2", "dim3", **{})

    internal_data_access_spec_with_diff_id = InternalDatasetSignalSourceAccessSpec("my_data_2", "dim1", "dim2", **{})

    internal_data_access_spec_without_schema_and_header = InternalDatasetSignalSourceAccessSpec(
        "my_data_1",
        DimensionSpec.load_from_pretty({"dim1": {"dim2": {}}}, False),
        **{DATASET_SCHEMA_FILE_KEY: None, DATASET_HEADER_KEY: False},
    )

    def test_internal_data_signal_source_access_spec_api(self):
        assert (
            self.internal_data_access_spec.data_id == self.internal_data_access_spec_cloned.data_id
            and self.internal_data_access_spec.folder == self.internal_data_access_spec_cloned.folder
        )

        assert self.internal_data_access_spec.source == SignalSourceType.INTERNAL
        assert (
            self.internal_data_access_spec.path_format
            == f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_data_1/{DIMENSION_PLACEHOLDER_FORMAT}/{DIMENSION_PLACEHOLDER_FORMAT}"
        )
        assert self.internal_data_access_spec.data_id == "my_data_1"
        assert self.internal_data_access_spec.folder == InternalDatasetSignalSourceAccessSpec.FOLDER + "/" + "my_data_1"

        assert self.internal_data_access_spec.dataset_type == DEFAULT_DATASET_TYPE
        # we don't even bother with these details for internal data
        assert self.internal_data_access_spec.partition_keys == []
        assert self.internal_data_access_spec.primary_keys == []

        assert self.internal_data_access_spec.data_schema_file
        assert self.internal_data_access_spec.data_header_exists

        assert self.internal_data_access_spec_without_schema_and_header.data_schema_file is None
        assert self.internal_data_access_spec_without_schema_and_header.data_schema_type is not None
        assert not self.internal_data_access_spec_without_schema_and_header.data_header_exists

    def test_internal_data_signal_source_create_from_external(self):
        s3_data_access_spec = S3SignalSourceAccessSpec(
            "111222333444",
            "bucket",
            "internal_data/my_data_1",
            DimensionSpec.load_from_pretty({"dim1": {"dim2": {}}}, False),
            **{DATASET_SCHEMA_FILE_KEY: "_SCHEMA", DATASET_SCHEMA_TYPE_KEY: DatasetSchemaType.SPARK_SCHEMA_JSON},
        )
        internal_representation = SignalSourceAccessSpec(SignalSourceType.INTERNAL, "/internal/my_data_1/{}/{}", None)
        internal_spec = InternalDatasetSignalSourceAccessSpec.create_from_external(s3_data_access_spec, internal_representation)
        assert internal_spec
        assert internal_spec.data_schema_file == "_SCHEMA" + "." + DatasetSchemaType.SPARK_SCHEMA_JSON
        assert internal_spec.data_schema_type == DatasetSchemaType.SPARK_SCHEMA_JSON
        assert internal_spec.data_header_exists

        wrong_representation = SignalSourceAccessSpec(SignalSourceType.GLUE_TABLE, "/internal/my_data_1/{}/{}", None)
        assert not InternalDatasetSignalSourceAccessSpec.create_from_external(s3_data_access_spec, wrong_representation)

    def test_internal_data_signal_source_create_from_external_default(self):
        s3_data_access_spec = S3SignalSourceAccessSpec(
            "111222333444",
            "bucket",
            "internal_data/my_data_1",
            DimensionSpec.load_from_pretty({"dim1": {"dim2": {}}}, False),
            **{DATASET_SCHEMA_FILE_KEY: None},
        )
        internal_representation = SignalSourceAccessSpec(SignalSourceType.INTERNAL, "/internal/my_data_1/{}/{}", None)
        internal_spec = InternalDatasetSignalSourceAccessSpec.create_from_external(s3_data_access_spec, internal_representation)
        assert internal_spec
        assert internal_spec.data_schema_file is None
        # will still be defaulted
        assert internal_spec.data_schema_type == DatasetSchemaType.SPARK_SCHEMA_JSON
        assert internal_spec.data_header_exists

    def test_internal_data_signal_source_create_from_external_no_schema_and_header(self):
        s3_data_access_spec = S3SignalSourceAccessSpec(
            "111222333444",
            "bucket",
            "internal_data/my_data_1",
            DimensionSpec.load_from_pretty({"dim1": {"dim2": {}}}, False),
            **{DATASET_SCHEMA_FILE_KEY: None, DATASET_SCHEMA_TYPE_KEY: DatasetSchemaType.SPARK_SCHEMA_JSON, DATASET_HEADER_KEY: False},
        )
        internal_representation = SignalSourceAccessSpec(SignalSourceType.INTERNAL, "/internal/my_data_1/{}/{}", None)
        internal_spec = InternalDatasetSignalSourceAccessSpec.create_from_external(s3_data_access_spec, internal_representation)
        assert internal_spec
        assert internal_spec.data_schema_file is None
        assert internal_spec.data_schema_type == DatasetSchemaType.SPARK_SCHEMA_JSON
        assert not internal_spec.data_header_exists

    def test_internal_data_signal_source_create_from_external_no_dimension(self):
        s3_data_access_spec = S3SignalSourceAccessSpec(
            "111222333444",
            "bucket",
            "internal_data/my_data_1",
            # NO PARTITIONS
            DimensionSpec(),
            **{DATASET_SCHEMA_FILE_KEY: None},
        )
        internal_representation = SignalSourceAccessSpec(SignalSourceType.INTERNAL, "/internal_data/my_data_1", None)
        internal_spec = InternalDatasetSignalSourceAccessSpec.create_from_external(s3_data_access_spec, internal_representation)
        assert internal_spec
        assert internal_spec.data_id == "my_data_1"
        assert internal_spec.path_format == f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_data_1"
        assert internal_spec.data_schema_file is None

    def test_internal_data_signal_source_access_spec_equality(self):
        assert self.internal_data_access_spec == self.internal_data_access_spec_cloned
        assert self.internal_data_access_spec == self.internal_data_access_spec_with_partitions
        assert self.internal_data_access_spec != self.internal_data_access_spec_with_diff_id
        assert self.internal_data_access_spec != self.internal_data_access_spec_with_diff_params

    def test_internal_data_signal_source_access_spec_serialization(self):
        assert self.internal_data_access_spec == loads(dumps(self.internal_data_access_spec))

    def test_internal_data_signal_source_access_spec_integrity(self):
        assert self.internal_data_access_spec.check_integrity(self.internal_data_access_spec_cloned)
        # verify type check even if the parametrization is same
        assert not self.internal_data_access_spec.check_integrity(TestDataAccessSpec.data_access_spec_internal)
