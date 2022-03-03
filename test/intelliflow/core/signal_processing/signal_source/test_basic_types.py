# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy

import pytest

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.signal_source import *


class TestBasicTypes:
    source = SignalSource(SignalSourceType.INTERNAL, ["1", "2020-06-22 00:01:00"], "_SUCCESS")
    source_identical = SignalSource(SignalSourceType.INTERNAL, ["1", "2020-06-22 00:01:00"], "_SUCCESS")
    source2 = SignalSource(SignalSourceType.S3, ["NA", "2020-06-22"], "_SUCCESS")

    access_spec_internal = SignalSourceAccessSpec(
        SignalSourceType.INTERNAL,
        f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_test_data/{DIMENSION_PLACEHOLDER_FORMAT}/{DIMENSION_PLACEHOLDER_FORMAT}",
        {},
    )
    access_spec_internal_cloned: SignalSourceAccessSpec = copy.deepcopy(access_spec_internal)
    access_spec_internal_attrs = SignalSourceAccessSpec(
        SignalSourceType.INTERNAL,
        f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_test_data/{DIMENSION_PLACEHOLDER_FORMAT}/{DIMENSION_PLACEHOLDER_FORMAT}",
        {DATASET_FORMAT_KEY: "csv"},
    )
    # test context_id
    application_uuid = "my_application-111222333444-us-east-1"
    access_spec_s3 = SignalSourceAccessSpec(
        SignalSourceType.S3,
        f"s3://bucket/folder/{DIMENSION_PLACEHOLDER_FORMAT}/{DIMENSION_PLACEHOLDER_FORMAT}",
        {DATASET_FORMAT_KEY: "parquet", SignalSourceAccessSpec.OWNER_CONTEXT_UUID: application_uuid},
    )

    access_spec_no_dimensions = SignalSourceAccessSpec(
        SignalSourceType.GLUE_TABLE, f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/data_with_no_dims", {}
    )

    # SignalSourceType
    def test_signal_source_type(self):
        assert SignalSourceType.S3.value == "S3"

    # SignalSource
    def test_signal_source_equality(self):
        assert self.source == self.source_identical
        assert self.source != self.source2

    # TODO
    # def test_signal_source_immutability(self):
    #    with pytest.raises(Exception) as error:
    #        self.source.type = SignalSourceType.GLUE_TABLE

    #    assert error.typename == 'AttributeError'

    def test_signal_source_serialization(self):
        assert self.source == loads(dumps(self.source))

    # SignalSourceAccessSpec
    def test_signal_source_access_spec_api(self):
        assert (
            self.access_spec_internal.source == self.access_spec_internal_cloned.source
            and self.access_spec_internal.path_format == self.access_spec_internal_cloned.path_format
            and self.access_spec_internal.attrs == self.access_spec_internal_cloned.attrs
        )

        assert self.access_spec_internal.get_owner_context_uuid() is None
        assert self.access_spec_s3.get_owner_context_uuid() == self.application_uuid

        assert self.access_spec_internal == self.access_spec_internal_cloned
        assert self.access_spec_internal != self.access_spec_internal_attrs

    def test_signal_source_access_spec_integrity(self):
        assert self.access_spec_internal.check_integrity(self.access_spec_internal_attrs)

    def test_signal_source_access_spec_upstream_mapping_state(self):
        assert not self.access_spec_internal.is_mapped_from_upstream()

        spec_cloned: SignalSourceAccessSpec = copy.deepcopy(self.access_spec_internal)
        spec_cloned.set_as_mapped_from_upstream()
        assert spec_cloned.is_mapped_from_upstream()

    def test_signal_source_extract_source(self):
        materialized_resource1 = f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_test_data/dim1_value/dim2_value/_SUCCESS"
        signal_source = self.access_spec_internal.extract_source(materialized_resource1)

        assert signal_source.type == self.access_spec_internal.source
        assert signal_source.dimension_values == ["dim1_value", "dim2_value"]
        assert signal_source.name == "_SUCCESS"

        # attrs should not change the behaviour
        signal_source = self.access_spec_internal_attrs.extract_source(materialized_resource1)
        assert signal_source.type == self.access_spec_internal.source
        assert signal_source.dimension_values == ["dim1_value", "dim2_value"]
        assert signal_source.name == "_SUCCESS"

    def test_signal_source_extract_source_failure(self):
        # resource name should follow the dimension values
        materialized_resource1_no_resource = f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_test_data/dim1_value/dim2_value"
        assert not self.access_spec_internal.extract_source(materialized_resource1_no_resource)

        # bad folder/data name (parts should match)
        materialized_resource2 = f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/wrong_data_name/dim1_value/dim2_value/_SUCCESS"
        assert not self.access_spec_internal.extract_source(materialized_resource2)

        # bad format
        materialized_resource3 = f"s3://{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_test_data/dim1_value/dim2_value/_SUCCESS"
        assert not self.access_spec_internal.extract_source(materialized_resource3)

        # bad format 2 (dimension quantity does not match)
        materialized_resource4 = f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/wrong_data_name/dim1_value/_SUCCESS"
        assert not self.access_spec_internal.extract_source(materialized_resource4)

    def test_signal_source_materialization(self):
        filter = DimensionFilter.load_raw({"NA": {"2020-06-11": {"format": "%Y-%m-%d"}}})

        # test internal
        access_specs = self.access_spec_internal.materialize_for_filter(filter)
        assert len(access_specs) == 1
        spec = access_specs[0]
        assert (
            spec.source == self.access_spec_internal.source
            and spec.path_format == self.access_spec_internal.path_format.format("NA", "2020-06-11")
            and spec.attrs == self.access_spec_internal.attrs
        )

        access_specs = self.access_spec_s3.materialize_for_filter(filter)
        assert len(access_specs) == 1
        spec = access_specs[0]
        assert spec.path_format == "s3://bucket/folder/NA/2020-06-11"

        access_specs = self.access_spec_no_dimensions.materialize_for_filter({})
        assert len(access_specs) == 1
        spec = access_specs[0]
        assert spec.path_format == self.access_spec_no_dimensions.path_format

    def test_signal_source_materialization_failure(self):
        assert self.access_spec_internal.materialize_for_filter(None) == []
        assert self.access_spec_internal.materialize_for_filter({}) == []
        assert self.access_spec_internal.materialize_for_filter(DimensionFilter.load_raw({"NA": {}})) == []
