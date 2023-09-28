# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from test.intelliflow.core.signal_processing.signal_source.test_internal_data_spec import TestInternalDataAccessSpec

import pytest

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.signal_source import *


class TestSNSAccessSpec:
    sns_access_spec = SNSSignalSourceAccessSpec("topic", "111222333444", "us-east-1", True)
    sns_access_spec_cloned = copy.deepcopy(sns_access_spec)

    sns_access_spec_no_retain = SNSSignalSourceAccessSpec("topic", "111222333444", "us-east-1", False)

    sns_access_spec_attrs = SNSSignalSourceAccessSpec("topic", "111222333444", "us-east-1", True, dummy_key="dummy_value")

    def test_sns_signal_source_access_spec_api(self):
        assert (
            self.sns_access_spec.topic == self.sns_access_spec_cloned.topic
            and self.sns_access_spec.topic_arn == self.sns_access_spec_cloned.topic_arn
            and self.sns_access_spec.account_id == self.sns_access_spec_cloned.account_id
            and self.sns_access_spec.region == self.sns_access_spec_cloned.region
            and self.sns_access_spec.retain_ownership == self.sns_access_spec_cloned.retain_ownership
        )

        assert self.sns_access_spec.source == SignalSourceType.SNS

    def test_sns_signal_source_access_spec_equality(self):
        assert self.sns_access_spec == self.sns_access_spec_cloned

    @pytest.mark.parametrize(
        "topic, account_id, region, retain, attrs",
        [
            ("topic1", "111222333444", "us-east-1", True, {}),
            ("topic", "211222333444", "us-east-1", True, {}),
            ("topic", "111222333444", "wrong_Region", True, {}),
            ("topic", "111222333444", "us-east-1", False, {}),
            ("topic", "111222333444", "us-east-1", False, {"dummy_key": "dummy_value"}),
        ],
    )
    def test_sns_signal_source_access_spec_inequality(self, topic, account_id, region, retain, attrs):
        assert self.sns_access_spec != SNSSignalSourceAccessSpec(topic, account_id, region, retain, **attrs)

    def test_sns_signal_source_access_spec_serialization(self):
        assert self.sns_access_spec == loads(dumps(self.sns_access_spec))

    def test_sns_signal_source_access_spec_integrity(self):
        assert self.sns_access_spec.check_integrity(self.sns_access_spec_cloned)
        # attrs will be ignored during the check
        assert self.sns_access_spec.check_integrity(self.sns_access_spec_attrs)
        # "retain_ownership" not part of the path_format, so should be checked separately
        assert not self.sns_access_spec.check_integrity(self.sns_access_spec_no_retain)
        assert not self.sns_access_spec.check_integrity(TestInternalDataAccessSpec.internal_data_access_spec)

    def test_sns_signal_source_access_spec_path_creation(self):
        assert SNSSignalSourceAccessSpec.create_resource_path("topic", "11122233344", "us-east-1", "2020-05-16")

    def test_sns_signal_source_access_spec_extract_time(self):
        assert SNSSignalSourceAccessSpec.extract_time("sns://111222333444/us-east-1/my_topic/2020-05-15/topic") == "2020-05-15"
        assert SNSSignalSourceAccessSpec.extract_time("sns://111222333444/us-east-1/my_topic/2020-05-15") == "2020-05-15"
