# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from test.intelliflow.core.platform.driver_test_utils import DriverTestUtils

import boto3
import pytest

from intelliflow.core.platform.definitions.aws.common import CommonParams
from intelliflow.core.platform.development import AWSConfiguration, HostPlatform
from intelliflow.core.platform.drivers.compute.aws_emr import AWSEMRBatchCompute
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSGlueBatchComputeBasic(AWSTestBase, DriverTestUtils):
    params = {}

    expected_emr_bucket = "if-awsemr-test123-123456789012-us-east-1"

    def setup_platform_and_params(self):
        self.init_common_utils()
        self.params[CommonParams.BOTO_SESSION] = boto3.Session(None, None, None, self.region)
        self.params[CommonParams.REGION] = self.region
        self.params[CommonParams.ACCOUNT_ID] = self.account_id
        self.params[CommonParams.IF_DEV_ROLE] = "DevRole"
        self.params[CommonParams.IF_EXE_ROLE] = "ExeRole"
        self.mock_compute = AWSEMRBatchCompute(self.params)
        self.mock_host_platform = HostPlatform(
            AWSConfiguration.builder()
            .with_default_credentials(as_admin=True)
            .with_region("us-east-1")
            .with_batch_compute(AWSEMRBatchCompute)
            .build()
        )

    def test_init_constructor(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        assert self.mock_compute._emr is not None
        assert self.mock_compute._ec2 is not None
        assert self.mock_compute._s3 is not None
        assert self.mock_compute._bucket is None
        assert self.mock_compute._bucket_name is None
        assert self.mock_compute._iam is not None
        assert self.mock_compute._intelliflow_python_workingset_key is None
        self.patch_aws_stop()

    def test_compute_dev_init_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        self.mock_host_platform.context_id = "test123"
        self.mock_compute.dev_init(self.mock_host_platform)
        assert self.mock_compute._bucket_name == self.expected_emr_bucket
        assert self.mock_compute._intelliflow_python_workingset_key == "batch/bundle.zip"
        self.patch_aws_stop()

    def test_compute_dev_init_exception_max_bucket_len(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        with pytest.raises(Exception) as error:
            context_id = str()
            for c in range(30):
                context_id += str(c)
            self.mock_host_platform._context_id = context_id
            self.mock_compute.dev_init(self.mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_compute_dev_init_exception_max_job_name_len(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        with pytest.raises(Exception) as error:
            context_id = str()
            for c in range(300):
                context_id += str(c)
            self.mock_host_platform._context_id = context_id
            self.mock_compute.dev_init(self.mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_compute_dev_init_exception_job_name_pattern(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        with pytest.raises(Exception) as error:
            self.mock_host_platform._context_id = "😀"
            self.mock_compute.dev_init(self.mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()
