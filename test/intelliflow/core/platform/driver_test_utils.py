# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from test.intelliflow.core.signal_processing.routing_runtime_constructs.test_route import TestRoute
from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal

from intelliflow.core.signal_processing import Signal
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalIntegrityProtocol, SignalType
from intelliflow.core.signal_processing.signal_source import (
    DatasetType,
    GlueTableSignalSourceAccessSpec,
    InternalDatasetSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
    SignalSourceAccessSpec,
    SignalSourceType,
    TimerSignalSourceAccessSpec,
)
from intelliflow.mixins.aws.test import AWSTestBase


# Common utils for driver tests.
class DriverTestUtils:
    def init_common_utils(self):
        from test.intelliflow.core.signal_processing.dimension_constructs.test_dimension_filter import TestDimensionFilter
        from test.intelliflow.core.signal_processing.dimension_constructs.test_dimension_spec import TestDimensionSpec

        self.test_provider = "provider1"
        self.test_table_name = "table1"
        self.test_non_existent_table = "non_existent_table"
        self.signal_unsupported = Signal(
            SignalType._AMZN_RESERVED_3,
            SignalSourceAccessSpec(SignalSourceType._AMZN_RESERVED_1, "edx:iad::manifest/dex-ml/predicted-data/all/{}", {}),
            SignalDomainSpec(
                TestDimensionSpec.dimension_spec_single_dim,
                TestDimensionFilter.dimension_filter_basic_any_type_long,
                SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
            ),
            "test_signal",
            True,
        )

        self.signal_s3_missing_acct_id = Signal(
            SignalType.EXTERNAL_S3_OBJECT_CREATION,
            S3SignalSourceAccessSpec("", "bucket", "my_data_2", "{}"),
            SignalDomainSpec(
                TestDimensionSpec.dimension_spec_single_dim,
                TestDimensionFilter.dimension_filter_basic_any_type_long,
                SignalIntegrityProtocol("FILE_CHECK", {"file": "PARTITION_READY"}),
            ),
            "test_signal",
            False,
        )

        self.test_signal_s3 = Signal(
            SignalType.EXTERNAL_S3_OBJECT_CREATION,
            S3SignalSourceAccessSpec(AWSTestBase.account_id, "bucket", "my_data_2", "{}"),
            SignalDomainSpec(
                TestDimensionSpec.dimension_spec_single_dim,
                TestDimensionFilter.dimension_filter_basic_any_type_long,
                SignalIntegrityProtocol("FILE_CHECK", {"file": "PARTITION_READY"}),
            ),
            "test_signal",
            False,
        )

        self.test_signal_andes = Signal(
            SignalType.EXTERNAL_GLUE_TABLE_PARTITION_UPDATE,
            GlueTableSignalSourceAccessSpec(self.test_provider, self.test_non_existent_table, ["part_key"], None),
            SignalDomainSpec(
                TestDimensionSpec.dimension_spec_single_dim,
                TestDimensionFilter.dimension_filter_basic_any_type_long,
                SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT", "DELTA"]}),
            ),
            "test_signal",
            False,
        )

        self.signal_internal_1 = TestSignal.signal_internal_1
        self.route_1_basic = TestRoute._route_1_basic()
