# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import os
import platform
import subprocess
import sys
from pathlib import Path

import pytest

from intelliflow.mixins.aws.integ_test import AWSIntegTestMixin

MAC_EXCLUDED_PACKAGES = []


@pytest.fixture(scope="session", autouse=True)
def global_init():
    import intelliflow.api_ext as flow

    flow.init_basic_logging(root_level=logging.CRITICAL)
    os.environ["INTELLIFLOW_APP_ROOT"] = str(Path(__file__).parent.parent)

    # Configuration to be used by all of the integ-tests in this folder that extends <AWSIntegTestMixin>
    app_name = "IntelliFlow"
    stage_key = AWSIntegTestMixin.create_stage_key(app_name)
    if not os.getenv(stage_key):
        os.environ[stage_key] = "dev"

    aws_acc_id_key = AWSIntegTestMixin.create_aws_account_id_key(app_name)
    if not os.getenv(aws_acc_id_key):
        os.environ[aws_acc_id_key] = "427809481713"

    aws_cross_acc_id_key = AWSIntegTestMixin.create_aws_cross_account_id_key(app_name)
    if not os.getenv(aws_cross_acc_id_key):
        os.environ[aws_cross_acc_id_key] = "427809481713"  # "842027028048"

    aws_region_key = AWSIntegTestMixin.create_aws_region(app_name)
    if not os.getenv(aws_region_key):
        os.environ[aws_region_key] = "us-east-1"


# This is to replace the native libraries that dont exist for macos.
# One needs to have installed the library on their macos as a replacemant.
def pytest_configure(config):
    if platform.system() == "Darwin":
        sys.path = [p for p in sys.path if not any([package_name in p for package_name in MAC_EXCLUDED_PACKAGES])]
