# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import platform
import sys
from unittest.mock import patch

import pytest

MAC_EXCLUDED_PACKAGES = []


# This is to replace the native libraries that dont exist for macos.
# One needs to have installed the library on their macos as a replacemant.
def pytest_configure(config):
    if platform.system() == "Darwin":
        sys.path = [p for p in sys.path if not any([package_name in p for package_name in MAC_EXCLUDED_PACKAGES])]


@pytest.fixture
def mock_AndesNativeClient():
    with patch("intelliflow.core.signal_processing.routing_runtime_constructs.AndesNativeClient") as myclass_mocked:
        # myclass_mocked.return_value.is_partition_present.return_value = True
        yield myclass_mocked
