# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from test.intelliflow.core.signal_processing.dimension_constructs.test_dimension_filter import TestDimensionFilter
from test.intelliflow.core.signal_processing.dimension_constructs.test_dimension_spec import TestDimensionSpec
from typing import cast

import pytest

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.dimension_constructs import (
    AnyVariant,
    DateVariant,
    Dimension,
    DimensionFilter,
    DimensionSpec,
    DimensionVariant,
    DimensionVariantReader,
    StringVariant,
)
from intelliflow.core.signal_processing.signal import *
from intelliflow.core.signal_processing.signal_source import (
    DATASET_TYPE_KEY,
    DIMENSION_PLACEHOLDER_FORMAT,
    DatasetSignalSourceAccessSpec,
    DatasetType,
    GlueTableSignalSourceAccessSpec,
    InternalDatasetSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
)


class TestSignal:
    signal_internal_1 = Signal(
        SignalType.INTERNAL_PARTITION_CREATION,
        InternalDatasetSignalSourceAccessSpec("my_data_1", TestDimensionSpec.dimension_spec_single_dim, **{}),
        SignalDomainSpec(
            TestDimensionSpec.dimension_spec_single_dim,
            TestDimensionFilter.dimension_filter_basic_any_type_long,
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        ),
        "test_signal",
        False,
    )

    signal_internal_1_cloned = copy.deepcopy(signal_internal_1)

    signal_internal_with_no_dimensions = Signal(
        SignalType.INTERNAL_PARTITION_CREATION,
        InternalDatasetSignalSourceAccessSpec("my_data_1", DimensionSpec(), **{}),
        SignalDomainSpec(DimensionSpec(), DimensionFilter(), SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})),
        "test_signal",
        False,
    )

    signal_internal_with_no_dimensions_and_protocol = Signal(
        SignalType.INTERNAL_PARTITION_CREATION,
        InternalDatasetSignalSourceAccessSpec("my_data_1", DimensionSpec(), **{}),
        SignalDomainSpec(DimensionSpec(), DimensionFilter(), None),
        "test_signal",
        False,
    )

    signal_internal_complex_1 = Signal(
        SignalType.INTERNAL_PARTITION_CREATION,
        InternalDatasetSignalSourceAccessSpec("upstream_data_1", TestDimensionSpec.dimension_spec_branch_lvl_2, **{}),
        SignalDomainSpec(
            TestDimensionSpec.dimension_spec_branch_lvl_2,
            TestDimensionFilter.dimension_filter_branch_1,
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        ),
        "upstream_data_1",
        False,
    )

    signal_internal_with_string_and_date_dims = Signal(
        SignalType.INTERNAL_PARTITION_CREATION,
        InternalDatasetSignalSourceAccessSpec("my_data_1", TestDimensionSpec.dimension_spec_branch_lvl_2_string_and_datetime, **{}),
        SignalDomainSpec(
            TestDimensionSpec.dimension_spec_branch_lvl_2_string_and_datetime,
            TestDimensionFilter.dimension_filter_branch_1,
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        ),
        "test_signal",
        False,
    )

    signal_s3_1 = Signal(
        SignalType.EXTERNAL_S3_OBJECT_CREATION,
        S3SignalSourceAccessSpec("111222333444", "bucket", "my_data_2", "{}"),
        SignalDomainSpec(
            TestDimensionSpec.dimension_spec_single_dim,
            TestDimensionFilter.dimension_filter_basic_any_type_long,
            SignalIntegrityProtocol("FILE_CHECK", {"file": "PARTITION_READY"}),
        ),
        "test_signal",
        False,
    )

    signal_andes_1 = Signal(
        SignalType.EXTERNAL_GLUE_TABLE_PARTITION_UPDATE,
        GlueTableSignalSourceAccessSpec("provider", "table", ["part_key"], None),
        SignalDomainSpec(
            TestDimensionSpec.dimension_spec_single_dim,
            TestDimensionFilter.dimension_filter_basic_any_type_long,
            SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT", "DELTA"]}),
        ),
        "test_signal",
        False,
    )

    def test_signal_init_and_properties(self):
        signal_internal_1_2 = Signal(
            SignalType.INTERNAL_PARTITION_CREATION,
            InternalDatasetSignalSourceAccessSpec("my_data_1", TestDimensionSpec.dimension_spec_single_dim, **{}),
            SignalDomainSpec(
                TestDimensionSpec.dimension_spec_single_dim,
                TestDimensionFilter.dimension_filter_basic_any_type_long,
                SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
            ),
            "test_signal",
            False,
        )

        assert self.signal_internal_1.type == signal_internal_1_2.type
        assert self.signal_internal_1.resource_access_spec == signal_internal_1_2.resource_access_spec
        assert self.signal_internal_1.domain_spec == signal_internal_1_2.domain_spec
        assert self.signal_internal_1.alias == signal_internal_1_2.alias

    @pytest.mark.parametrize(
        "signal1, signal2, result",
        [
            (signal_internal_1, signal_internal_1, True),
            (signal_internal_1, signal_internal_1_cloned, True),
            (signal_internal_1, signal_internal_with_no_dimensions, False),
            (signal_internal_1, signal_s3_1, False),
            (signal_internal_1, signal_andes_1, False),
            (signal_s3_1, signal_andes_1, False),
            # check alias is None on the second signal
            (
                signal_internal_1,
                Signal(
                    signal_internal_1.type, signal_internal_1.resource_access_spec, signal_internal_1.domain_spec, None
                ),  # won't cause issue if it is none),
                True,
            ),
            (
                signal_internal_1,
                Signal(signal_internal_1.type, signal_internal_1.resource_access_spec, signal_internal_1.domain_spec, "diff_alias"),
                False,
            ),
            # check type
            (
                signal_internal_1,
                Signal(
                    SignalType.EXTERNAL_S3_OBJECT_CREATION,
                    signal_internal_1.resource_access_spec,
                    signal_internal_1.domain_spec,
                    "test_signal",
                ),
                False,
            ),
            # check the impact of changes on resource access spec and domain spec
            (
                signal_internal_1,
                Signal(
                    signal_internal_1.type,
                    signal_internal_with_no_dimensions.resource_access_spec,
                    signal_internal_1.domain_spec,
                    "test_signal",
                ),
                False,
            ),
            # as long as type and path format are matched, then other attrs from the spec is ignored
            (
                signal_internal_1,
                Signal(
                    signal_internal_1.type,
                    SignalSourceAccessSpec(
                        SignalSourceType.INTERNAL,
                        f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_data_1/{DIMENSION_PLACEHOLDER_FORMAT}",
                        {"dummy_key": "value"},
                    ),
                    signal_internal_1.domain_spec,
                    "test_signal",
                ),
                True,
            ),
            # data_id differs, hence the path_format -> False
            (
                signal_internal_1,
                Signal(
                    signal_internal_1.type,
                    SignalSourceAccessSpec(
                        SignalSourceType.INTERNAL,
                        f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_data_2/{DIMENSION_PLACEHOLDER_FORMAT}",
                        {"dummy_key": "value"},
                    ),
                    signal_internal_1.domain_spec,
                    "test_signal",
                ),
                False,
            ),
            # signal source type differs
            (
                signal_internal_1,
                Signal(
                    signal_internal_1.type,
                    SignalSourceAccessSpec(
                        SignalSourceType.GLUE_TABLE,
                        f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_data_1/{DIMENSION_PLACEHOLDER_FORMAT}",
                        {},
                    ),
                    signal_internal_1.domain_spec,
                    "test_signal",
                ),
                False,
            ),
            # domain spec change is ignored in terms of equality check
            # it is the responsibility of the client to provide the resource access spec and domain spec in a consistent way
            # so this case is not actually very likely, but showcasing the fact that domain_spec is ignored.
            (
                signal_internal_1,
                Signal(
                    signal_internal_1.type,
                    signal_internal_1.resource_access_spec,
                    TestDimensionSpec.dimension_spec_tree_1,  # unrelated spec but still will return true
                    "test_signal",
                ),
                True,
            ),
            (
                Signal(None, SignalSourceAccessSpec(None, None, None), SignalDomainSpec(None, None, None)),
                Signal(None, SignalSourceAccessSpec(None, None, None), SignalDomainSpec(None, None, None)),
                True,
            ),
        ],
    )
    def test_signal_equality(self, signal1, signal2, result):
        assert (signal1 == signal2) == result
        assert (signal2 == signal1) == result

    def test_signal_equality_fail(self):
        with pytest.raises(AttributeError):
            Signal(None, None, None) == Signal(None, None, None)

    # TODO
    # def test_signal_immutability(self):
    #    with pytest.raises(Exception):
    #        self.signal_internal_1.type = SignalType.EXTERNAL_S3_OBJECT_CREATION

    #    with pytest.raises(Exception):
    #        self.signal_internal_1.domain_spec = SignalDomainSpec(None, None, None)

    #    with pytest.raises(Exception):
    #        self.signal_internal_1.resource_access_spec = SignalSourceAccessSpec(SignalSourceType.GLUE_TABLE, 'glue_table://provider/table/part', {})

    #    with pytest.raises(Exception):
    #        self.signal_internal_1.alias = 'new_alias'

    @pytest.mark.parametrize("signal", [(signal_internal_1), (signal_internal_with_no_dimensions), (signal_s3_1), (signal_andes_1)])
    def test_signal_serialization(self, signal):
        assert signal == loads(dumps(signal))
        assert signal == Signal.deserialize(signal.serialize())

    @pytest.mark.parametrize(
        "signal1, signal2, result",
        [
            (signal_internal_1, signal_internal_1, True),
            (signal_internal_1, signal_internal_1_cloned, True),
            (signal_internal_1, signal_internal_with_no_dimensions, False),
            (signal_internal_1, signal_s3_1, False),
            (signal_internal_with_no_dimensions, signal_internal_with_no_dimensions_and_protocol, False),
            # test effect of nested dim change on filter
            (signal_internal_complex_1, signal_internal_complex_1.filter({"*": {"_:-2": {}}}), False),
            (
                signal_internal_1,
                Signal(
                    signal_internal_1.type,
                    # base class for all of the dataset specs
                    InternalDatasetSignalSourceAccessSpec("my_data_1", "part_key", dummy_key="value"),  # extra param ignored
                    signal_internal_1.domain_spec,
                    "test_signal",
                ),
                True,
            ),
            (
                signal_internal_1,
                Signal(
                    signal_internal_1.type,
                    # base class for all of the dataset specs
                    # type is different so internal spec will detect the change
                    DatasetSignalSourceAccessSpec(
                        SignalSourceType.INTERNAL,
                        f"/{InternalDatasetSignalSourceAccessSpec.FOLDER}/my_data_1/{DIMENSION_PLACEHOLDER_FORMAT}",
                        {"dummy_key": "value"},
                    ),
                    signal_internal_1.domain_spec,
                    "test_signal",
                ),
                False,
            ),
            (
                signal_internal_1,
                Signal(
                    signal_internal_1.type,
                    # param change will be detected
                    InternalDatasetSignalSourceAccessSpec("my_data_1", "part_key", **{DATASET_TYPE_KEY: "APPEND"}),
                    signal_internal_1.domain_spec,
                    "test_signal",
                ),
                False,
            ),
            # domain spec change is not ignored during integrity check (impacts semantics and runtime behaviour)
            (
                signal_internal_1,
                Signal(
                    signal_internal_1.type,
                    signal_internal_1.resource_access_spec,
                    signal_internal_with_no_dimensions_and_protocol.domain_spec,
                    "test_signal",
                ),
                False,
            ),
            (
                signal_internal_1,
                Signal(signal_internal_1.type, signal_internal_1.resource_access_spec, signal_internal_1.domain_spec, "diff_alias"),
                False,
            ),
            # check type
            (
                signal_internal_1,
                Signal(
                    SignalType.EXTERNAL_S3_OBJECT_CREATION,
                    signal_internal_1.resource_access_spec,
                    signal_internal_1.domain_spec,
                    "test_signal",
                ),
                False,
            ),
        ],
    )
    def test_signal_check_integrity(self, signal1, signal2, result):
        assert signal1.check_integrity(signal2) == result

    def test_signal_clone(self):
        clone1 = self.signal_internal_1.clone("new_alias")
        assert clone1.alias == "new_alias"
        assert clone1.type == self.signal_internal_1.type
        assert clone1.resource_access_spec == self.signal_internal_1.resource_access_spec
        assert clone1.domain_spec == self.signal_internal_1.domain_spec

        shallow = self.signal_internal_1.clone("new_alias2", deep=False)
        assert shallow.alias == "new_alias2"
        assert clone1.alias == "new_alias"
        assert shallow.type == self.signal_internal_1.type
        assert shallow.resource_access_spec == self.signal_internal_1.resource_access_spec
        assert shallow.domain_spec == self.signal_internal_1.domain_spec

    def test_signal_apply(self):
        # heavily relies on DimensionFilter::apply which is extensively tested in dimension_constructs/test_dimension_filter.py

        # successful filtering
        result = self.signal_internal_with_string_and_date_dims.filter(
            DimensionFilter.load_raw({"NA": {"*": {}}, "EU": {"_:-3": {}}})
        ).apply(self.signal_internal_with_string_and_date_dims.filter({"EU": {"2020-11-29": {}}}))

        assert DimensionFilter.check_equivalence(
            DimensionFilter.load_raw({"EU": {"2020-11-29": {}, "2020-11-28": {}, "2020-11-27": {}}}),
            result.domain_spec.dimension_filter_spec,
        )
        # spec mismatch
        result = self.signal_internal_with_string_and_date_dims.filter(DimensionFilter()).apply(
            self.signal_internal_with_string_and_date_dims.filter({"EU": {"2020-11-29": {}}})
        )
        assert result is None

        result = self.signal_internal_with_string_and_date_dims.filter({"EU": {"2020-11-29": {}}}).apply(
            self.signal_internal_with_string_and_date_dims.filter(DimensionFilter())
        )
        assert result is None

        # empty result
        result = self.signal_internal_with_string_and_date_dims.filter({"EU": {"2020-11-29": {}}}).apply(
            self.signal_internal_with_string_and_date_dims.filter({"NA": {"2020-11-29": {}}})
        )
        assert result.domain_spec.dimension_filter_spec == {}

        # finally a valid case of empty spec being applied to another empty spec
        # outcome is a valid signal
        result = self.signal_internal_with_string_and_date_dims.filter({}).apply(
            self.signal_internal_with_string_and_date_dims.filter(DimensionFilter())
        )
        assert result.domain_spec.dimension_filter_spec == {}

        with pytest.raises(RuntimeError):
            # internal filter will raise runtime error due to two relative filters being applied to each other
            result = self.signal_internal_with_string_and_date_dims.filter({"EU": {"_:-2": {}}}).apply(
                self.signal_internal_with_string_and_date_dims.filter({"EU": {"_:-3": {}}})
            )

    def test_signal_shift(self):
        """Slightly modified version of test_signal_apply to show the effects of transformations (such as range shift)
        on a signal"""
        # shift EU dimension by 2 days into the past (from NOW at runtime)
        result: Signal = self.signal_internal_with_string_and_date_dims.filter(
            DimensionFilter.load_raw({"NA": {"*": {}}, "EU": {"_:-3:-2": {}}}), transfer_spec=True
        ).apply(self.signal_internal_with_string_and_date_dims.filter({"EU": {"2020-11-29": {}}}))

        # show that underlying values are agnostic from transformations
        assert DimensionFilter.check_equivalence(
            DimensionFilter.load_raw({"EU": {"2020-11-29": {}, "2020-11-28": {}, "2020-11-27": {}}}),
            result.domain_spec.dimension_filter_spec,
        )

        # show low-evel dimension filter exhibits the effect of transformations when needed
        assert DimensionFilter.check_equivalence(
            DimensionFilter.load_raw({"EU": {"2020-11-27": {}, "2020-11-26": {}, "2020-11-25": {}}}),
            result.domain_spec.dimension_filter_spec.transform(),
        )

        paths = result.get_materialized_resource_paths()
        assert paths[0].endswith("2020-11-27")
        assert paths[1].endswith("2020-11-26")
        assert paths[2].endswith("2020-11-25")

        assert result.dimension_values("dim_1_2") == ["2020-11-27", "2020-11-26", "2020-11-25"]

    def test_signal_create_from_spec(self):
        from test.intelliflow.core.signal_processing.signal_source.test_internal_data_spec import TestInternalDataAccessSpec

        clone1 = self.signal_internal_1.create_from_spec(TestInternalDataAccessSpec.internal_data_access_spec_with_partitions)
        assert clone1.resource_access_spec == TestInternalDataAccessSpec.internal_data_access_spec_with_partitions
        assert clone1.type == self.signal_internal_1.type
        assert clone1.domain_spec == self.signal_internal_1.domain_spec
        assert clone1.alias == self.signal_internal_1.alias

    @pytest.mark.parametrize(
        "signal, signal_type, source_type, materialized_resource_path, result",
        [
            (
                signal_internal_1,
                signal_internal_1.type,
                signal_internal_1.resource_access_spec.source,
                "/internal_data/my_data_1/1/_SUCCESS",
                True,
            ),
            (signal_s3_1, SignalType.EXTERNAL_S3_OBJECT_CREATION, SignalSourceType.S3, "s3://bucket/my_data_2/999/PARTITION_READY", True),
            (
                signal_s3_1,
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                SignalSourceType.S3,
                "s3://bucket/my_data_2/999/part-000-111.csv",  # not compatible with the basic protocol
                False,
            ),
            (
                signal_internal_1,
                signal_internal_1.type,
                signal_internal_1.resource_access_spec.source,
                "/internal_data/my_data_1/str/_SUCCESS",  # incoming 'dim value' is string!
                False,
            ),
            (
                signal_internal_1,
                signal_internal_1.type,
                signal_internal_1.resource_access_spec.source,
                "/internal_data/my_data_1/9",  # '9' will be treated as the resource name and will cause mismatch
                False,
            ),
            (
                signal_internal_1,
                signal_internal_1.type,
                signal_internal_1.resource_access_spec.source,
                "/internal_data/my_data_1/_SUCCESS",  # no dimensions
                False,
            ),
            # signal type is different
            (
                signal_internal_1,
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                signal_internal_1.resource_access_spec.source,
                "/internal_data/my_data_1/1/_SUCCESS",
                False,
            ),
            # signal source type is different
            (signal_internal_1, signal_internal_1.type, SignalSourceType.S3, "/internal_data/my_data_1/1/_SUCCESS", False),
            # path format does not match
            (signal_internal_1, signal_internal_1.type, SignalSourceType.S3, "s3://internal_data/my_data_1/1/_SUCCESS", False),
            (
                signal_internal_with_no_dimensions,
                SignalType.INTERNAL_PARTITION_CREATION,
                SignalSourceType.INTERNAL,
                "/internal_data/my_data_1/_SUCCESS",  # no dimensions, which is fine
                True,
            ),
            (
                signal_internal_with_no_dimensions,
                SignalType.INTERNAL_PARTITION_CREATION,
                SignalSourceType.INTERNAL,
                "/internal_data/my_data_1/1/2/_SUCCESS",  # dimensions won't match the resource access spec (with no dims)
                False,
            ),
            (
                signal_internal_with_no_dimensions_and_protocol,
                SignalType.INTERNAL_PARTITION_CREATION,
                SignalSourceType.INTERNAL,
                "/internal_data/my_data_1/RANDOM_RESOURCE_NAME",  # no protocol so resource is welcome
                True,
            ),
            (
                signal_andes_1,
                SignalType.EXTERNAL_GLUE_TABLE_PARTITION_UPDATE,
                SignalSourceType.GLUE_TABLE,
                "glue_table://provider/table/1/SNAPSHOT",
                True,
            ),
            (
                signal_andes_1,
                SignalType.EXTERNAL_GLUE_TABLE_PARTITION_UPDATE,
                SignalSourceType.GLUE_TABLE,
                "glue_table://provider/table/1/DELTA",  # protocol is still ok
                True,
            ),
            (
                signal_andes_1,
                SignalType.EXTERNAL_GLUE_TABLE_PARTITION_UPDATE,
                SignalSourceType.GLUE_TABLE,
                "glue_table://provider/table/DELTA",  # no dimensions, not ok
                False,
            ),
            (
                signal_internal_complex_1,
                SignalType.INTERNAL_PARTITION_CREATION,
                SignalSourceType.INTERNAL,
                "/internal_data/upstream_data_1/999/NA/_SUCCESS",
                True,
            ),
            (
                signal_internal_complex_1,
                SignalType.INTERNAL_PARTITION_CREATION,
                SignalSourceType.INTERNAL,
                "/internal_data/upstream_data_1/999/_SUCCESS",  # missing second STRING typed dim
                False,
            ),
        ],
    )
    def test_signal_create(self, signal, signal_type, source_type, materialized_resource_path, result):
        response = signal.create(signal_type, source_type, materialized_resource_path)
        assert (not response) == (not result)

    def test_signal_chain(self):
        # heavily relies on DimensionFilter::chain which is extensively tested in dimension_constructs/test_dimension_filter.py

        # successful filtering (same behaviour as 'apply')
        result = self.signal_internal_with_string_and_date_dims.filter(
            DimensionFilter.load_raw({"NA": {"*": {}}, "EU": {"_:-3": {}}})
        ).chain(self.signal_internal_with_string_and_date_dims.filter({"EU": {"2020-11-29": {}}}))

        assert DimensionFilter.check_equivalence(
            DimensionFilter.load_raw({"EU": {"2020-11-29": {}, "2020-11-28": {}, "2020-11-27": {}}}),
            result.domain_spec.dimension_filter_spec,
        )
        assert result.alias == self.signal_internal_with_string_and_date_dims.alias

        # relative variant applied towards the filter "NA / '*'"
        # relative variant should overwrite
        result = self.signal_internal_with_string_and_date_dims.filter(
            DimensionFilter.load_raw({"NA": {"*": {}}, "EU": {"_:-3": {}}})
        ).chain(
            {"NA": {"_:-15": {}}},  # use raw chain(RawDimensionFilterInput) overload
            # use alias
            "input_dataset",
        )

        assert DimensionFilter.check_equivalence(DimensionFilter.load_raw({"NA": {"_:-15": {}}}), result.domain_spec.dimension_filter_spec)
        assert result.alias == "input_dataset"

        # spec mismatch
        result = self.signal_internal_with_string_and_date_dims.filter(DimensionFilter()).chain(
            self.signal_internal_with_string_and_date_dims.filter({"EU": {"2020-11-29": {}}})
        )
        assert result is None

        result = self.signal_internal_with_string_and_date_dims.filter({"EU": {"2020-11-29": {}}}).chain(
            DimensionFilter()
        )  # use chain(DimensionFilter) overload
        assert result is None

        # empty result
        result = self.signal_internal_with_string_and_date_dims.filter({"EU": {"2020-11-29": {}}}).chain(
            self.signal_internal_with_string_and_date_dims.filter({"NA": {"2020-11-29": {}}})
        )
        assert result.domain_spec.dimension_filter_spec == {}

        # finally a valid case of empty spec being chained with another empty spec
        # outcome is a valid signal
        result = self.signal_internal_with_string_and_date_dims.filter({}).chain(
            self.signal_internal_with_string_and_date_dims.filter(DimensionFilter())
        )
        assert result.domain_spec.dimension_filter_spec == {}

        # perfectly ok during dev-time chaining, intersection will yield relative -2
        result = self.signal_internal_with_string_and_date_dims.filter({"EU": {"_:-2": {}}}).chain(
            self.signal_internal_with_string_and_date_dims.filter({"EU": {"_:-3": {}}})
        )
        assert DimensionFilter.check_equivalence(DimensionFilter.load_raw({"EU": {"_:-2": {}}}), result.domain_spec.dimension_filter_spec)

        # AnyVariant applied to Relative -> Relative
        result = self.signal_internal_with_string_and_date_dims.filter({"EU": {"_:-3": {}}}).chain(
            self.signal_internal_with_string_and_date_dims.filter({"EU": {"*": {}}})
        )
        assert DimensionFilter.check_equivalence(DimensionFilter.load_raw({"EU": {"_:-3": {}}}), result.domain_spec.dimension_filter_spec)

    def test_signal_dimension_value_access(self):
        assert self.signal_internal_with_string_and_date_dims.dimension_values("dim_1_1") == ["*"]
        assert self.signal_internal_with_string_and_date_dims.dimension_values("dim_1_2") == ["*"]
        assert self.signal_internal_with_string_and_date_dims.dimension_values_map() == {"dim_1_1": ["*"], "dim_1_2": ["*"]}
        assert self.signal_internal_with_string_and_date_dims.tip_value("dim_1_1") == "*"
        assert self.signal_internal_with_string_and_date_dims.tip_value("dim_1_2") == "*"
        assert self.signal_internal_with_string_and_date_dims["dim_1_1"] == "*"
        assert self.signal_internal_with_string_and_date_dims["dim_1_2"] == "*"

        with pytest.raises(ValueError):
            self.signal_internal_with_string_and_date_dims.dimension_values("WRONG_DIM_NAME")
        with pytest.raises(ValueError):
            self.signal_internal_with_string_and_date_dims.tip_value("WRONG_DIM_NAME")
        with pytest.raises(ValueError):
            self.signal_internal_with_string_and_date_dims["WRONG_DIM_NAME"]

        new_filter = DimensionFilter.load_raw({"NA": {"*": {}}, "EU": {"_:-3": {}}})
        new_filter.set_spec(self.signal_internal_with_string_and_date_dims.domain_spec.dimension_spec)
        filtered_signal = self.signal_internal_with_string_and_date_dims.filter(new_filter)

        assert filtered_signal.dimension_values("dim_1_1") == ["NA", "EU"]
        assert filtered_signal.dimension_values("dim_1_2") == ["*", "_:-3"]
        assert filtered_signal.dimension_values_map() == {"dim_1_1": ["NA", "EU"], "dim_1_2": ["*", "_:-3"]}
        assert filtered_signal.tip_value("dim_1_1") == "NA"
        assert filtered_signal.tip_value("dim_1_2") == "*"
        assert filtered_signal["dim_1_1"] == "NA"
        assert filtered_signal["dim_1_2"] == "*"

        # mimic what would happen at runtime against and incoming signal (from a raw event)
        materialized_signal = filtered_signal.chain(self.signal_internal_with_string_and_date_dims.filter({"EU": {"2020-11-29": {}}}))
        assert materialized_signal.dimension_values("dim_1_1") == ["EU"]
        assert materialized_signal.dimension_values("dim_1_2") == ["2020-11-29", "2020-11-28", "2020-11-27"]
        assert materialized_signal.dimension_values_map() == {"dim_1_1": ["EU"], "dim_1_2": ["2020-11-29", "2020-11-28", "2020-11-27"]}
        assert materialized_signal.tip_value("dim_1_1") == "EU"
        assert materialized_signal.tip_value("dim_1_2") == "2020-11-29"
        assert materialized_signal["dim_1_1"] == "EU"
        assert materialized_signal["dim_1_2"] == "2020-11-29"
