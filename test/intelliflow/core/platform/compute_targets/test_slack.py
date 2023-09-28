# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from test.intelliflow.core.signal_processing.dimension_constructs.test_dimension_filter import TestDimensionFilter
from test.intelliflow.core.signal_processing.dimension_constructs.test_dimension_spec import TestDimensionSpec

import boto3
import pytest
import requests
import responses

from intelliflow.api_ext import *
from intelliflow.core.platform.compute_targets.slack import Slack
from intelliflow.core.platform.definitions.aws.common import CommonParams
from intelliflow.core.platform.definitions.compute import ComputeInternalError, ComputeRetryableInternalError
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import InternalDatasetSignalSourceAccessSpec
from intelliflow.mixins.aws.test import AWSTestBase


class TestSlackComputeTarget(AWSTestBase):
    bad_url = "https://badurl.slack.com"
    good_url_1 = "https://hooks.slack.com/workflows/1/"
    good_url_2 = "https://hooks.slack.com/workflows/2/"
    good_url_as_aws_secret = "arn:aws:secretsmanager:us-east-1:123456789012:secret:if-my-app-slack-workflow-url-5wjhy3"
    good_url_base_but_does_not_exist = "https://hooks.slack.com/workflows/does_not_exsit/"
    good_url_base_but_connection_error = "https://hooks.slack.com/workflows/connection_issue/"
    good_url_base_but_time_out = "https://hooks.slack.com/workflows/time_out/"
    good_url_base_but_unknown_error = "https://hooks.slack.com/workflows/unknown_error"

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

    input_map = {"input_signal": INPUT_SIGNAL}
    materialized_output = OUTPUT_SIGNAL
    params = {CommonParams.BOTO_SESSION: boto3.Session(), "UNIQUE_ID_FOR_CONTEXT": "random"}

    def test_slack_should_throw_exception_with_empty_recipient_list(self):
        with pytest.raises(ValueError) as error:
            Slack(recipient_list=[])
        assert "A list of valid Slack Workflow URLs is required." in str(error.value)

    def test_slack_should_log_warning_with_bad_recipient_list(self, caplog):
        Slack(recipient_list=[self.bad_url])
        assert "does not seem to be a valid Slack workflow url that starts with" in caplog.text

    def test_slack_should_log_warning_when_modifying_with_bad_workflow_url(self, caplog):
        slack_obj = Slack(recipient_list=[self.good_url_1])
        slack_obj.action(recipient_list=[self.good_url_1, self.bad_url])
        assert "does not seem to be a valid Slack workflow url that starts with" in caplog.text

    def test_slack_should_validate_with_legit_data(self):
        slack_obj = Slack(recipient_list=[self.good_url_1, self.good_url_2], message="default message")
        assert slack_obj.recipient_list[0] == self.good_url_1
        assert slack_obj.recipient_list[1] == self.good_url_2
        assert slack_obj.message == "default message"

    def test_slack_should_update_data_when_modifying_with_legit_data(self):
        message_before = "default message"
        message_after = "updated message"
        slack_obj = Slack(recipient_list=[self.good_url_1], message=message_before)
        slack_action_obj = slack_obj.action(recipient_list=[self.good_url_2], message=message_after)
        assert slack_action_obj.slack.recipient_list[0] == self.good_url_2
        assert slack_action_obj.slack.message == message_after

    def test_slack_aws_secret(self):
        slack_obj = Slack(recipient_list=[self.good_url_1, self.good_url_as_aws_secret], message="message")
        assert slack_obj.aws_secret_arns == [self.good_url_as_aws_secret]
        assert slack_obj.raw_workflow_urls == [self.good_url_1]
        slack_action_obj = slack_obj.action()
        assert slack_action_obj.permissions

    @responses.mock.activate
    def test_slack_should_throw_exception_when_posting_to_url_with_bad_token(self):
        responses.mock.add(responses.mock.POST, self.good_url_base_but_does_not_exist, status=400)

        slack_obj = Slack(recipient_list=[self.good_url_base_but_does_not_exist])
        slack_action_obj = slack_obj.action(message="testing message")

        with pytest.raises(ComputeInternalError) as err:
            slack_action_obj.__call__(self.input_map, self.materialized_output, self.params)
        assert "A HTTP error occurred with status" in str(err.value)

    @responses.mock.activate
    def test_slack_should_throw_exception_when_having_connection_issue(self):
        responses.mock.add(responses.mock.POST, self.good_url_base_but_connection_error, body=requests.ConnectionError())

        slack_obj = Slack(recipient_list=[self.good_url_base_but_connection_error])
        slack_action_obj = slack_obj.action(message="testing message")

        with pytest.raises(ComputeRetryableInternalError) as err:
            slack_action_obj.__call__(self.input_map, self.materialized_output, self.params)
        assert "A Connecting Error occurred when posting to Slack workflow API" in str(err.value)

    @responses.mock.activate
    def test_slack_should_throw_exception_when_posting_time_out(self):
        responses.mock.add(responses.mock.POST, self.good_url_base_but_time_out, body=requests.Timeout())

        slack_obj = Slack(recipient_list=[self.good_url_base_but_time_out])
        slack_action_obj = slack_obj.action(message="testing message")

        with pytest.raises(ComputeRetryableInternalError) as err:
            slack_action_obj.__call__(self.input_map, self.materialized_output, self.params)
        assert "A Timeout Error occurred when posting to Slack workflow API" in str(err.value)

    @responses.mock.activate
    def test_slack_should_throw_exception_with_unknown_error(self):
        responses.mock.add(responses.mock.POST, self.good_url_base_but_unknown_error, body=requests.RequestException())

        slack_obj = Slack(recipient_list=[self.good_url_base_but_unknown_error])
        slack_action_obj = slack_obj.action(message="testing message")

        with pytest.raises(ComputeInternalError) as err:
            slack_action_obj.__call__(self.input_map, self.materialized_output, self.params)
        assert "An Unknown Error occurred when posting to Slack workflow API" in str(err.value)

    @responses.mock.activate
    def test_slack_should_success_when_posting_with_no_error(self):
        responses.mock.add(responses.mock.POST, self.good_url_1, status=200)
        responses.mock.add(responses.mock.POST, self.good_url_2, status=200)

        slack_obj = Slack(recipient_list=[self.good_url_1, self.good_url_2])
        slack_action_obj = slack_obj.action(message="testing message")
        try:
            slack_action_obj.__call__(self.input_map, self.materialized_output, self.params)
        except Exception as exception:
            assert False, f"An exception {exception} is raised when sending out Slack message"
