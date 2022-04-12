# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Common EMR definitions which are agnostic from different ABI implementations.

This *can* be provided to EMR for further client code convenience. But this should not be
mandatory.
"""

JOB_NAME_PARAM: str = "JOB_NAME"
INPUT_MAP_PARAM: str = "INPUT_MAP"
CLIENT_CODE_PARAM: str = "CLIENT_CODE"
CLIENT_CODE_BUCKET: str = "CLIENT_CODE_BUCKET"
CLIENT_CODE_METADATA: str = "CLIENT_CODE_METADATA"
CLIENT_CODE_ABI: str = "CLIENT_CODE_ABI"
OUTPUT_PARAM: str = "OUTPUT"
BOOTSTRAPPER_PLATFORM_KEY_PARAM: str = "BOOTSTRAPPER_PATH"
USER_EXTRA_PARAMS_PARAM: str = "USER_EXTRA_PARAM"
AWS_REGION: str = "AWS_REGION"
WORKING_SET_OBJECT_PARAM: str = "WORKING_SET_OBJECT"
