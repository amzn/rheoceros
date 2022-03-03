# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import platform
import sys

MAC_EXCLUDED_PACKAGES = []


# This is to replace the native libraries that dont exist for macos.
# One needs to have installed the library on their macos as a replacemant.
def pytest_configure(config):
    if platform.system() == "Darwin":
        sys.path = [p for p in sys.path if not any([package_name in p for package_name in MAC_EXCLUDED_PACKAGES])]
