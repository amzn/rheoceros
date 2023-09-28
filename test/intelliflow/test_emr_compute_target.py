# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from intelliflow.api_ext import EMR
from intelliflow.core.platform.drivers.compute.aws_emr import InstanceConfig, RuntimeConfig
from intelliflow.core.signal_processing import Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.slot import SlotType
from intelliflow.mixins.aws.test import AWSTestBase


class TestEMRComputeTarget(AWSTestBase):
    def test_emr_compute_defaults(self):
        expected_code = "output=None"
        emr = EMR(code=expected_code)
        assert emr._slot.code == expected_code
        assert emr._slot.code_lang == Lang.PYTHON
        assert emr._slot.code_abi == ABI.GLUE_EMBEDDED
        assert isinstance(emr._slot.extra_params["InstanceConfig"], InstanceConfig)
        assert isinstance(emr._slot.extra_params["RuntimeConfig"], RuntimeConfig)
