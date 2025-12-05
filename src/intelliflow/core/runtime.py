# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import platform
import sys

PYTHON_VERSION_MAJOR = sys.version_info[0]
PYTHON_VERSION_MINOR = sys.version_info[1]
PYTHON_VERSION_BUILD = platform.python_version_tuple()[2]
