# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time
from datetime import datetime

import pytest

from intelliflow.api_ext import *
from intelliflow.core.application.application import Application
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.data_emulation import add_test_data
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationSNSSupport(AWSTestBase):
    def test_application_sns_cross_account_fanout(self):
        self.patch_aws_start()

        forwarder_app = AWSApplication("data-forwarder", self.region)

        offline_all_data = forwarder_app.marshal_external_data(
            S3Dataset(self.account_id, "sns-test-bucket", "training-data", "{}", "{}", dataset_format=DataFormat.CSV).link(
                SNS(topic_name="sns_fanout_test_topic")
            ),
            "training_all_data",
            {
                "region": {
                    type: DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%Y-%m-%d",
                    },
                }
            },
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        offline_all_data_2 = forwarder_app.marshal_external_data(
            S3Dataset(self.account_id, "sns-test-bucket-2", "training-data", "{}", "{}", dataset_format=DataFormat.CSV).link(
                # use the same topic for fan-out
                SNS(topic_name="sns_fanout_test_topic")
            ),
            "training_all_data_2",
            {
                "region": {
                    type: DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%Y-%m-%d",
                    },
                }
            },
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        # create the downstream / consumer app
        consumer_app = AWSApplication("data-consumer", self.region)
        consumer_app.activate()

        # authorize downstream
        forwarder_app.authorize_downstream(consumer_app.id, self.account_id, self.region)
        forwarder_app.activate()

        consumer_app.import_upstream(forwarder_app.id, self.account_id, self.region)

        eureka_data = offline_all_data
        eureka_data_2 = offline_all_data_2

        trigger_test = consumer_app.create_data("trigger_test", inputs=[eureka_data], compute_targets=[NOOPCompute])

        trigger_test2 = consumer_app.create_data("trigger_test_2", inputs=[eureka_data_2], compute_targets=[NOOPCompute])

        consumer_app.activate()

        # ------
        # 1- Test the first channel (first bucket into first downstream node)
        # add _SUCCESS file to source bucket to trigger cross-account event propagation and execution in downstream account
        add_test_data(forwarder_app, offline_all_data["NA"]["2021-03-18"], object_name="_SUCCESS", data="")

        # FUTURE Enable (local AWS service integration emulation)
        # give some time for propagation (otherwise poll can report None on most recent execution)
        # account1(S3 -> SNS) -> account2(Lambda)
        # time.sleep(3)
        # FUTURE Disable (inject the event into the system due to missing local S3 -> SNS integration)
        consumer_app.process(offline_all_data["NA"]["2021-03-18"])

        #
        path, records = consumer_app.poll(trigger_test["NA"]["2021-03-18"])
        assert path, "'trigger_test' node must have triggered automatically!"

        # validate second note has not been triggered yet
        path, _ = consumer_app.poll(trigger_test2["NA"]["2021-03-18"])
        assert not path, "'trigger_test_2' node must NOT have triggered as second bucket did not send the notification yet!"

        # ------
        # 2- Test the second channel (second bucket into second downstream node)
        # add _SUCCESS file to second source bucket to trigger cross-account event propagation and execution in downstream account
        add_test_data(forwarder_app, offline_all_data_2["NA"]["2021-03-18"], object_name="_SUCCESS", data="")

        # FUTURE enable (just rely on local AWS service integration)
        # time.sleep(3)
        # FUTURE Disable (inject the event into the system due to missing local S3 -> SNS integration)
        consumer_app.process(offline_all_data_2["NA"]["2021-03-18"])

        path, _ = consumer_app.poll(trigger_test2["NA"]["2021-03-18"])
        assert path, "'trigger_test_2' node must have triggered automatically!"

        # intentionally call terminate to test the cleanup code
        consumer_app.terminate()
        forwarder_app.terminate()

        self.patch_aws_stop()

    def test_application_sns_as_proxy_same_app_trigger_notification_overlap(self):
        self.patch_aws_start()

        app = AWSApplication("sns-trigger", self.region)

        offline_all_data = app.marshal_external_data(
            S3Dataset(self.account_id, "sns-test-bucket", "training-data", "{}", "{}", dataset_format=DataFormat.CSV).link(
                SNS(topic_name="sns_fanout_test_topic")
            ),
            "training_all_data",
            {
                "region": {
                    type: DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%Y-%m-%d",
                    },
                }
            },
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        # try to load a different dataset from the same bucket but this time with a different topic
        # should fail due to same bucket being affilliated with multiple topics (notification overlap not allowed)
        offline_all_data_2 = app.marshal_external_data(
            S3Dataset(self.account_id, "sns-test-bucket", "different-data", "{}", "{}", dataset_format=DataFormat.CSV).link(
                # SAME TOPIC
                SNS(topic_name="sns_fanout_test_topic_2")
            ),
            "training_all_data_2",
            {
                "region": {
                    type: DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%Y-%m-%d",
                    },
                }
            },
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        trigger_test = app.create_data("trigger_test", inputs=[offline_all_data, offline_all_data_2], compute_targets=[NOOPCompute])
        # should fail due to "1 bucket -> 2 topics"
        with pytest.raises(ValueError):
            app.activate()

        self.patch_aws_stop()

    def test_application_sns_as_proxy_same_app_trigger_success(self):
        self.patch_aws_start()

        app = AWSApplication("sns-trigger", self.region)

        offline_all_data = app.marshal_external_data(
            S3Dataset(self.account_id, "sns-test-bucket", "training-data", "{}", "{}", dataset_format=DataFormat.CSV).link(
                SNS(topic_name="sns_fanout_test_topic")
            ),
            "training_all_data",
            {
                "region": {
                    type: DimensionType.STRING,
                    "day": {
                        "type": DimensionType.DATETIME,
                        "format": "%Y-%m-%d",
                    },
                }
            },
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        trigger_test = app.create_data("trigger_test", inputs=[offline_all_data], compute_targets=[NOOPCompute])
        app.activate()

        app.process(offline_all_data["EU"]["2022-07-28"])

        output_path, _ = app.poll(trigger_test["EU"]["2022-07-28"])
        assert output_path, "Could not detect execution!"

        self.patch_aws_stop()

    def test_application_sns_as_external_signal_same_app_trigger(self):
        self.patch_aws_start()

        app = AWSApplication("sns-trigger", self.region)

        sns_trigger_1 = app.marshal_external_data(SNS(topic_name="test-topic"))
        # check defaults
        assert sns_trigger_1.signal().resource_access_spec.region == self.region
        assert sns_trigger_1.signal().resource_access_spec.account_id == self.account_id

        second_topic_region = "us-west-2"
        sns_trigger_2 = app.marshal_external_data(SNS(topic_name="test-topic-2", region=second_topic_region))
        assert sns_trigger_2.signal().resource_access_spec.region == second_topic_region
        assert sns_trigger_2.signal().resource_access_spec.account_id == self.account_id

        trigger_test = app.create_data("trigger_test", inputs=[sns_trigger_1, sns_trigger_2], compute_targets=[NOOPCompute])

        app.activate()

        app.process(sns_trigger_1["2022-07-28"])

        output_path, _ = app.poll(trigger_test["2022-07-28"])
        assert not output_path, "Execution must not have started!"

        app.process(sns_trigger_2["2022-07-28"])
        output_path, _ = app.poll(trigger_test["2022-07-28"])
        assert output_path, "Execution must have started!"

        app.terminate()

        self.patch_aws_stop()

    def test_application_sns_ownership(self):
        self.patch_aws_start()

        app = AWSApplication("sns-trigger", self.region)

        # if SNS topic does not exist then should fail because app does not own it
        with pytest.raises(ValueError):
            app.marshal_external_data(SNS(topic_name="test-topic", retain_ownership=False))

        # create the topic
        sns = app.platform.processor.session.client("sns", region_name=self.region)
        sns.create_topic(Name="test-topic")

        sns_trigger_1 = app.marshal_external_data(SNS(topic_name="test-topic", retain_ownership=False))

        # should succeed
        app.activate()

        # use a different account on the topic and set retain_ownership=True
        with pytest.raises(ValueError):
            app.marshal_external_data(SNS(topic_name="test-topic-2", account_id="99988877766655", retain_ownership=True))

        app.terminate()
        self.patch_aws_stop()
