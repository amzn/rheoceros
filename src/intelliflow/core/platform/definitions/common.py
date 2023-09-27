# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum, unique


@unique
class ActivationParams(str, Enum):
    CONTEXT_ID = "CONTEXT"
    UNIQUE_ID_FOR_CONTEXT = "UNIQUE_ID_FOR_CONTEXT"
    UNIQUE_ID_FOR_HOST_CONTEXT = "UNIQUE_ID_FOR_HOST_CONTEXT"
    # using pkg_resources terminology of workingset, which perfectly fits our general use
    # of this param (aws lambda zip, etc), while still being generic enough.
    RUNTIME_WORKING_SET_ZIP = "WORKING_SET_ZIP"
