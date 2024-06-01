# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from test.intelliflow.core.signal_processing.dimension_constructs.test_dimension_filter import TestDimensionFilter
from test.intelliflow.core.signal_processing.dimension_constructs.test_dimension_spec import TestDimensionSpec

import boto3
import pytest

from intelliflow.core.platform.compute_targets.email import EMAIL
from intelliflow.core.platform.definitions.aws.common import CommonParams
from intelliflow.core.signal_processing import Signal
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalIntegrityProtocol, SignalType
from intelliflow.core.signal_processing.signal_source import InternalDatasetSignalSourceAccessSpec
from intelliflow.mixins.aws.test import AWSTestBase


class TestEmailComputeTarget(AWSTestBase):
    AWS_ACC_ID = "123456789"
    AWS_REGION = "us-eas-1"
    INPUT_SIGNAL = Signal(
        SignalType.INTERNAL_PARTITION_CREATION,
        InternalDatasetSignalSourceAccessSpec("my_data_1", TestDimensionSpec.dimension_spec_single_dim, **{}),
        SignalDomainSpec(
            TestDimensionSpec.dimension_spec_single_dim,
            TestDimensionFilter.dimension_filter_basic_any_type_long,
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        ),
        "input_signal",
        False,
    )

    OUTPUT_SIGNAL = Signal(
        SignalType.INTERNAL_PARTITION_CREATION,
        InternalDatasetSignalSourceAccessSpec("my_data_2", TestDimensionSpec.dimension_spec_single_dim, **{}),
        SignalDomainSpec(
            TestDimensionSpec.dimension_spec_single_dim,
            TestDimensionFilter.dimension_filter_basic_any_type_long,
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        ),
        "output_signal",
        False,
    )

    def test_email_validate_expected_email_request(self):
        self.patch_aws_start()

        email_obj = EMAIL(
            sender="if-test-list@amazon.com",
            recipient_list=["if-email-test-list@amazon.com"],
            aws_account=self.AWS_ACC_ID,
            aws_region=self.AWS_REGION,
        )

        subject = "IF Test Email"
        body = "IF Test Email body"

        out = email_obj.action(subject=subject, body=body)

        assert out.email.sender == email_obj.sender
        assert set(out.email.recipient_list) == set(email_obj.recipient_list)
        assert out.email.subject == subject
        assert out.email.body == body
        self.patch_aws_stop()

    def test_email_validate_action_modified_email_request(self):
        self.patch_aws_start()
        email_obj = EMAIL(
            sender="if-test-list@amazon.com",
            recipient_list=["if-email-test-list@amazon.com"],
            aws_account=self.AWS_ACC_ID,
            aws_region=self.AWS_REGION,
        )

        subject = "IF Test Email"
        body = "IF Test Email body"

        out = email_obj.action(sender="shanant@amazon.com", subject=subject, body=body)

        assert out.email.sender != email_obj.sender
        assert set(out.email.recipient_list) == set(email_obj.recipient_list)
        assert out.email.subject == subject
        assert out.email.body == body
        self.patch_aws_stop()

    @pytest.mark.skip(reason="TODO temporarily due to moto=4.x upgrade. botocore version is not new. getting AWS region error")
    def test_email_successful_action(self):
        input_map = {"input_signal": self.INPUT_SIGNAL}
        materialized_output = self.OUTPUT_SIGNAL

        params = {}
        params[CommonParams.BOTO_SESSION] = boto3.Session(region_name=self.AWS_REGION)
        params[CommonParams.REGION] = self.AWS_REGION
        params["UNIQUE_ID_FOR_CONTEXT"] = "random"

        self.patch_aws_start()
        ses_client = params[CommonParams.BOTO_SESSION].client("ses", region_name=self.AWS_REGION)
        ses_client.verify_email_identity(EmailAddress="if-test-list@amazon.com")
        email_obj = EMAIL(
            sender="if-test-list@amazon.com",
            recipient_list=["if-email-test-list@amazon.com"],
            aws_account=self.AWS_ACC_ID,
            aws_region=self.AWS_REGION,
        )

        subject = "IF Test Email"
        body = "IF Test Email body"

        email_action_compute = email_obj.action(subject=subject, body=body)
        response = email_action_compute.__call__(input_map, materialized_output, params)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        self.patch_aws_stop()

        # TODO Implement the exception handling test cases introduced with
        #  "Add Retry support to Inlined/Hook compute execution mechanism"
