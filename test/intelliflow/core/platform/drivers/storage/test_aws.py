# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json

import boto3
import pytest

from intelliflow.core.platform.constructs import ConstructParamsDict
from intelliflow.core.platform.definitions.aws.common import CommonParams, exponential_retry
from intelliflow.core.platform.definitions.aws.s3.bucket_wrapper import bucket_exists, get_policy
from intelliflow.core.platform.development import AWSConfiguration, DevelopmentPlatform, HostPlatform
from intelliflow.core.platform.drivers.storage.aws import AWSS3StorageBasic
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSS3StorageBasic(AWSTestBase):
    def setup_platform_and_params(self):
        self.params = {}
        self.expected_bucket_policy_list = [
            {
                "Effect": "Allow",
                "Principal": {"AWS": ["arn:aws:iam::123456789012:role/test123_s_2-us-east-1-IntelliFlowDevRole"]},
                "Action": ["s3:*"],
                "Resource": ["arn:aws:s3:::if-test123_s_2-123456789012-us-east-1/*", "arn:aws:s3:::if-test123_s_2-123456789012-us-east-1"],
            },
            {
                "Effect": "Allow",
                "Principal": {"AWS": ["DevRole"]},
                "Action": ["s3:*"],
                "Resource": ["arn:aws:s3:::if-test123_s_2-123456789012-us-east-1/*", "arn:aws:s3:::if-test123_s_2-123456789012-us-east-1"],
            },
            {
                "Effect": "Allow",
                "Principal": {"AWS": ["ExeRole"]},
                "Action": ["s3:*"],
                "Resource": ["arn:aws:s3:::if-test123_s_2-123456789012-us-east-1/*", "arn:aws:s3:::if-test123_s_2-123456789012-us-east-1"],
            },
        ]

        self.expected_topic_policy = {
            "Version": "2012-10-17",
            "Id": "c0c6bb72-4171-11eb-9fcd-38f9d3528f2e",
            "Statement": [
                {
                    "Sid": "c1344ec6-4171-11eb-9fcd-38f9d3528f2e",
                    "Effect": "Allow",
                    "Principal": {"Service": "s3.amazonaws.com"},
                    "Action": "sns:Publish",
                    "Resource": "arn:aws:sns:us-east-1:123456789012:if-test123_s_2-AWSS3StorageBasic",
                    "Condition": {"ArnLike": {"AWS:SourceArn": "arn:aws:s3:*:*:if-test123_s_2-123456789012-us-east-1"}},
                }
            ],
        }

        self.params[CommonParams.BOTO_SESSION] = boto3.Session(None, None, None, self.region)
        self.params[CommonParams.REGION] = self.region
        self.params[CommonParams.ACCOUNT_ID] = self.account_id
        self.params[CommonParams.IF_DEV_ROLE] = "DevRole"
        self.params[CommonParams.IF_EXE_ROLE] = "ExeRole"

    def get_driver_and_platform(self):
        mock_platform = HostPlatform(AWSConfiguration.builder().with_default_credentials(as_admin=True).with_region("us-east-1").build())
        # Prevent intermittent test failures due to platform state loading during context_id assignment
        mock_platform.should_load_constructs = lambda: False
        return (
            AWSS3StorageBasic(self.params),
            mock_platform,
        )

    def test_storage_successful_dev_init(self):
        context_id = "test123_s_1"
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_storage, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = context_id
        mock_storage.dev_init(mock_host_platform)
        assert mock_host_platform._context_id in mock_storage._bucket_name
        assert mock_host_platform._context_id in mock_storage._topic_name
        assert mock_host_platform._context_id in mock_storage._topic_arn
        assert bucket_exists(mock_storage._s3, mock_storage._bucket_name)
        self.patch_aws_stop()

    def test_storage_failure_dev_init(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_storage, mock_host_platform = self.get_driver_and_platform()
        context_id = str()
        for c in range(68):
            context_id += str(c)
        mock_host_platform._context_id = context_id
        with pytest.raises(Exception) as error:
            mock_storage.dev_init(mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    # TODO https://issues.amazon.com/issues/SVEN-343
    # def test_storage_activate_successful(self):
    #    self.patch_aws_start()
    #    self.setup_platform_and_params()
    #    mock_storage, mock_host_platform = self.get_driver_and_platform()
    #    mock_host_platform.context_id = "test123_s_2"
    #    mock_storage = mock_host_platform.storage
    #    mock_storage.activate()
    #    bucket_policy = get_policy(mock_storage._s3, mock_storage._bucket_name)
    #    expected_bucket_policy_list_str = json.dumps(self.expected_bucket_policy_list)
    #    assert any(json.dumps(e) in expected_bucket_policy_list_str for e in bucket_policy["Statement"])

    #    topic_policy_res = mock_storage._sns.get_topic_attributes(TopicArn=mock_storage._topic_arn)
    #    topic_policy_extracted = json.loads(topic_policy_res["Attributes"]["Policy"])["Statement"][0]

    #    del self.expected_topic_policy["Statement"][0]["Sid"]
    #    del topic_policy_extracted["Sid"]

    #    assert topic_policy_extracted == self.expected_topic_policy["Statement"][0]
    #    self.patch_aws_stop()
