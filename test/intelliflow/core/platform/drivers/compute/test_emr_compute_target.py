from intelliflow.api_ext import EMR
from intelliflow.core.platform.drivers.compute.aws_emr import InstanceConfig, RuntimeConfig
from intelliflow.core.signal_processing import Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.slot import SlotType
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.spark import (
    C5D_24XLARGE_SPARK_CLI_ARGS,
    EMRFS_SITE_CLUSTER_CONFIG,
    HIGH_THROUGHPUT_EXTRA_SPARK_ARGS,
    MAPRED_SITE_CLUSTER_CONFIG,
    YARN_SITE_CLUSTER_CONFIG,
)


class TestEMRComputeTarget(AWSTestBase):
    def test_emr_compute_defaults(self):
        expected_code = "output=None"
        emr = EMR(code=expected_code)
        assert emr._slot.code == expected_code
        assert emr._slot.code_lang == Lang.PYTHON
        assert emr._slot.code_abi == ABI.GLUE_EMBEDDED
        assert isinstance(emr._slot.extra_params["InstanceConfig"], InstanceConfig)
        assert isinstance(emr._slot.extra_params["RuntimeConfig"], RuntimeConfig)

    def test_emr_compute_spark_advanced(self):
        expected_code = "output=None"
        spark_cli_args = C5D_24XLARGE_SPARK_CLI_ARGS + HIGH_THROUGHPUT_EXTRA_SPARK_ARGS
        configurations = [EMRFS_SITE_CLUSTER_CONFIG, MAPRED_SITE_CLUSTER_CONFIG, YARN_SITE_CLUSTER_CONFIG]
        emr = EMR(
            code=expected_code,
            SparkCliArgs=spark_cli_args,
            Configurations=configurations,
        )
        assert emr._slot.extra_params["Configurations"] == configurations
        assert emr._slot.extra_params["SparkCliArgs"] == spark_cli_args
        assert isinstance(emr._slot.extra_params["InstanceConfig"], InstanceConfig)
        assert isinstance(emr._slot.extra_params["RuntimeConfig"], RuntimeConfig)
