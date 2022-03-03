# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from test.intelliflow.core.platform.driver_test_utils import DriverTestUtils

import boto3
import pytest
from mock import MagicMock

import intelliflow.core.platform.drivers.processor.aws as processor_driver
from intelliflow.api_ext import AnyDate
from intelliflow.core.platform.definitions.aws.common import CommonParams
from intelliflow.core.platform.definitions.aws.s3.bucket_wrapper import create_bucket, get_policy
from intelliflow.core.platform.definitions.common import ActivationParams
from intelliflow.core.platform.development import AWSConfiguration, HostPlatform
from intelliflow.core.platform.drivers.processor.aws import AWSLambdaProcessorBasic
from intelliflow.core.signal_processing import Signal
from intelliflow.core.signal_processing.dimension_constructs import DateVariant, Dimension, DimensionFilter, DimensionSpec
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalIntegrityProtocol, SignalType
from intelliflow.core.signal_processing.signal_source import (
    DatasetType,
    InternalDatasetSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
    SignalSourceAccessSpec,
    SignalSourceType,
    TimerSignalSourceAccessSpec,
)
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSLambdaProcessorBasic(AWSTestBase, DriverTestUtils):

    params = {}

    expected_queue_url = "https://queue.amazonaws.com/123456789012/if-AWSLambdaProcessorBasic-test123-us-east-1-DLQ"
    expected_queue_arn = "arn:aws:sqs:us-east-1:123456789012:if-AWSLambdaProcessorBasic-test123-us-east-1-DLQ"
    expected_lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:if-AWSLambdaProcessorBasic-test123-us-east-1"
    expected_lambda_name = "if-AWSLambdaProcessorBasic-test123-us-east-1"
    expected_filter_lambda_arn = "arn:aws:lambda:us-east-1:123456789012:function:if-AWSLambdaProcessorBasic-test123-us-east-1-FILTER"
    expected_filter_lambda_name = "if-AWSLambdaProcessorBasic-test123-us-east-1-FILTER"

    expected_s3_policy_statement = [
        {
            "Sid": "unique123_dev_role_bucket_access",
            "Effect": "Allow",
            "Principal": {"AWS": "DevRole"},
            "Action": ["s3:List*", "s3:Put*", "s3:Get*"],
            "Resource": ["arn:aws:s3:::bucket"],
        },
        {
            "Sid": "unique123_dev_role_object_access",
            "Effect": "Allow",
            "Principal": {"AWS": "DevRole"},
            "Action": ["s3:Get*"],
            "Resource": ["arn:aws:s3:::bucket/my_data_2/*"],
        },
        {
            "Sid": "unique123_exec_role_bucket_access",
            "Effect": "Allow",
            "Principal": {"AWS": "ExeRole"},
            "Action": ["s3:List*", "s3:Get*"],
            "Resource": ["arn:aws:s3:::bucket"],
        },
        {
            "Sid": "unique123_exec_role_object_access",
            "Effect": "Allow",
            "Principal": {"AWS": "ExeRole"},
            "Action": ["s3:Get*"],
            "Resource": ["arn:aws:s3:::bucket/my_data_2/*"],
        },
    ]

    andes_event = {
        "version": "0",
        "id": "88b8adf0-ed56-ca29-3691-db4329d89b81",
        "detail-type": "Glue Data Catalog Table State Change",
        "source": "aws.glue",
        "account": "842027028048",
        "time": "2020-10-05T18:41:18Z",
        "region": "us-east-1",
        "resources": ["arn:aws:glue:us-east-1:842027028048:table/booker/d_unified_cust_shipment_items"],
        "detail": {
            "databaseName": "booker",
            "changedPartitions": ["[3, 2020-10-05 00:00:00, 1601921790685, 1601921790685, SNAPSHOT]"],
            "typeOfChange": "BatchCreatePartition",
            "tableName": "d_unified_cust_shipment_items",
        },
    }
    expected_andes_resource_path = (
        "glue_table://booker/d_unified_cust_shipment_items/3/2020-10-05 00:00:00/1601921790685/1601921790685/SNAPSHOT"
    )
    s3_event = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "us-east-1",
                "eventTime": "2020-12-09T07:12:07.478Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {"principalId": "AWS:AROAWHG3OUPYTMIYIUY2G:Mradle-Batch-9a6152f3-5e2c-4a18-afb3-de48fe9eb68a"},
                "requestParameters": {"sourceIPAddress": "10.6.240.104"},
                "responseElements": {
                    "x-amz-request-id": "65FA440D283F5F6D",
                    "x-amz-id-2": "nTDigJaIMWUZyW4kSOyf/C2sYPrbzSX0W77HziSN51Q011Jk8tiX5hOIDo22JEvv34kOqtgGxVWZrRnX4PlKhToEMF5lkNSk",
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "MDgzMWNhM2MtYzBjNy00MmE0LTgzZWMtNjQ3N2FkNTE2M2Ux",
                    "bucket": {
                        "name": "dex-ml-eureka-model-training-data",
                        "ownerIdentity": {"principalId": "AZB59FR3ENND3"},
                        "arn": "arn:aws:s3:::dex-ml-eureka-model-training-data",
                    },
                    "object": {
                        "key": "cradle_eureka_p3/v8_00/all-data-prod/partition_day%3D2020-12-02/_SUCCESS",
                        "size": 0,
                        "eTag": "d41d8cd98f00b204e9800998ecf8427e",
                        "versionId": "WfCm7xjLULdcOlWxgBbDVst4M07wMyTM",
                        "sequencer": "005FD078CD7BB50846",
                    },
                },
            }
        ]
    }
    expected_s3_resource_path = (
        "s3://dex-ml-eureka-model-training-data/cradle_eureka_p3/v8_00/all-data-prod/partition_day=2020-12-02/_SUCCESS"
    )

    def setup_platform_and_params(self):
        self.init_common_utils()
        self.params[CommonParams.BOTO_SESSION] = boto3.Session(None, None, None, self.region)
        self.params[CommonParams.REGION] = self.region
        self.params[CommonParams.ACCOUNT_ID] = self.account_id
        self.params[CommonParams.IF_DEV_ROLE] = "DevRole"
        self.params[CommonParams.IF_EXE_ROLE] = "ExeRole"
        self.mock_processor = AWSLambdaProcessorBasic(self.params)
        self.mock_host_platform = HostPlatform(
            AWSConfiguration.builder()
            .with_default_credentials(as_admin=True)
            .with_region("us-east-1")
            .with_processor(AWSLambdaProcessorBasic)
            .build()
        )

    def create_timer_signal(self, context_id):
        time_dim = AnyDate("time", {DateVariant.FORMAT_PARAM: "%Y-%m-%d", DateVariant.TIMEZONE_PARAM: "UTC"})

        timer_source_access_spec = TimerSignalSourceAccessSpec("test_timer", "rate(1 day)", context_id)

        spec: DimensionSpec = DimensionSpec()
        spec.add_dimension(Dimension(time_dim.name, time_dim.type, time_dim.params), None)

        dim_filter: DimensionFilter = DimensionFilter()
        dim_filter.add_dimension(time_dim, None)

        domain_spec = SignalDomainSpec(spec, dim_filter, None)
        test_timer_signal = Signal(SignalType.TIMER_EVENT, timer_source_access_spec, domain_spec, "test_timer", False)
        return test_timer_signal

    def assert_s3_policies(self, policy):
        assert len(policy["Statement"]) == len(self.expected_s3_policy_statement)
        for st in self.expected_s3_policy_statement:
            for res_st in policy["Statement"]:
                if st["Sid"] == res_st["Sid"]:
                    assert st["Principal"]["AWS"] == res_st["Principal"]["AWS"]
                    assert sorted(st["Action"]) == sorted(res_st["Action"])
                    assert st["Resource"] == res_st["Resource"]

    def test_processor_dev_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        assert self.mock_processor._lambda_name == "if-AWSLambdaProcessorBasic-test123-us-east-1"
        assert (
            self.mock_processor._lambda_arn == "arn:aws:lambda:us-east-1:123456789012:function:if-AWSLambdaProcessorBasic-test123-us-east-1"
        )
        assert self.mock_processor._bucket_name == AWSLambdaProcessorBasic.BOOTSTRAPPER_ROOT_FORMAT.format(
            "awslambda".lower(), self.mock_host_platform._context_id.lower(), self.account_id, self.region
        )
        self.patch_aws_stop()

    def test_processor_dlq_name_len_max_80_exception(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        with pytest.raises(Exception) as error:
            context_id = str()
            for c in range(25):
                context_id += str(c)
            self.mock_host_platform._context_id = context_id
            self.mock_processor.dev_init(self.mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_processor_dev_bucket_len_max_64_exception(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        with pytest.raises(Exception) as error:
            context_id = str()
            for c in range(20):
                context_id += str(c)
            self.mock_host_platform._context_id = context_id
            self.mock_processor.dev_init(self.mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_processor_main_loop_timer_id_len_greater_than_64_exception(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        with pytest.raises(Exception) as error:
            context_id = str()
            for c in range(17):
                context_id += str(c)
            self.mock_host_platform._context_id = context_id
            self.mock_processor.dev_init(self.mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_processor_catalog_event_rule_id_len_greater_than_64_exception(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        with pytest.raises(Exception) as error:
            context_id = str()
            for c in range(15):
                context_id += str(c)
            self.mock_host_platform._context_id = context_id
            self.mock_processor.dev_init(self.mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_processor_activate_success(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        events = self.params[CommonParams.BOTO_SESSION].client(service_name="events", region_name=self.region)
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        self.mock_processor.activate()
        assert self.mock_processor._dlq_url == self.expected_queue_url
        assert self.mock_processor._dlq_arn == self.expected_queue_arn
        assert self.mock_processor._lambda_arn == self.expected_lambda_arn
        assert self.mock_processor._filter_lambda_arn == self.expected_filter_lambda_arn
        response = events.list_targets_by_rule(Rule=self.mock_processor._main_loop_timer_id)
        assert response["Targets"][0]["Id"] == self.expected_lambda_name
        self.patch_aws_stop()

    def test_processor_hook_external_not_supported_signal_exception(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        with pytest.raises(Exception) as error:
            self.mock_processor.hook_external([self.signal_unsupported])
        assert error.typename == "NotImplementedError"
        self.patch_aws_stop()

    def test_processor_hook_external_s3_missing_acct_id_exception(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        glue_client = self.params[CommonParams.BOTO_SESSION].client(service_name="glue", region_name=self.region)
        glue_client.create_database(DatabaseInput={"Name": self.test_provider})
        glue_client.create_table(DatabaseName=self.test_provider, TableInput={"Name": self.test_table_name})
        self.setup_platform_and_params()
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        with pytest.raises(Exception) as error:
            self.mock_processor.hook_external([self.test_signal_andes])
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_processor_process_external_new_glue_table_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        events = self.params[CommonParams.BOTO_SESSION].client(service_name="events", region_name=self.region)
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        self.mock_processor.activate()
        self.mock_processor._process_external_glue_table({self.test_signal_andes}, {})
        response = events.list_targets_by_rule(Rule=self.mock_processor._glue_catalog_event_channel_rule_id)
        assert response["Targets"][0]["Id"] == self.expected_filter_lambda_name
        self.patch_aws_stop()

    def test_processor_process_external_old_glue_table_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        events = self.params[CommonParams.BOTO_SESSION].client(service_name="events", region_name=self.region)
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        self.mock_processor.activate()
        self.mock_processor._process_external_glue_table({self.test_signal_andes}, {})
        response = events.list_targets_by_rule(Rule=self.mock_processor._glue_catalog_event_channel_rule_id)
        assert response["Targets"][0]["Id"] == self.expected_filter_lambda_name
        num_of_remove_calls_previous = processor_driver.remove_permission.call_count
        self.mock_processor._process_external_glue_table({}, {self.test_signal_andes})
        num_of_remove_calls_current = processor_driver.remove_permission.call_count
        assert (num_of_remove_calls_current - num_of_remove_calls_previous) == 1
        self.patch_aws_stop()

    def test_processor_process_external_s3_new_signal_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        s3 = boto3.resource("s3", region_name=self.region)
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        self.mock_processor.activate()
        self.mock_processor._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT] = "unique123"
        create_bucket(s3, "bucket", self.region)
        num_add_permission_before = processor_driver.add_permission.call_count
        self.mock_processor._process_external_S3({self.test_signal_s3}, {})
        num_add_permission_after = processor_driver.add_permission.call_count
        assert (num_add_permission_after - num_add_permission_before) == 1
        bucket_notification = s3.BucketNotification("bucket")
        bucket_notification.load()
        assert bucket_notification.lambda_function_configurations[0]["LambdaFunctionArn"] == self.expected_filter_lambda_arn
        assert bucket_notification.lambda_function_configurations[0]["Events"][0] == "s3:ObjectCreated:*"
        policy = get_policy(s3, "bucket")
        self.assert_s3_policies(policy)
        self.patch_aws_stop()

    def test_processor_process_external_s3_old_signal_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        s3 = boto3.resource("s3", region_name=self.region)
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        self.mock_processor.activate()
        self.mock_processor._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT] = "unique123"
        create_bucket(s3, "bucket", self.region)
        self.mock_processor._process_external_S3({self.test_signal_s3}, {})
        num_of_remove_calls_previous = processor_driver.remove_permission.call_count
        self.mock_processor._process_external_S3({}, {self.test_signal_s3})
        num_of_remove_calls_current = processor_driver.remove_permission.call_count
        assert (num_of_remove_calls_current - num_of_remove_calls_previous) == 1
        self.patch_aws_stop()

    def test_processor_process_new_timer_signal_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        events = self.params[CommonParams.BOTO_SESSION].client(service_name="events", region_name=self.region)
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        self.mock_processor.activate()
        test_timer_signal = self.create_timer_signal("test123")
        num_add_permission_before = processor_driver.add_permission.call_count
        self.mock_processor._process_internal_timers_signals({test_timer_signal}, {})
        num_add_permission_after = processor_driver.add_permission.call_count
        assert (num_add_permission_after - num_add_permission_before) == 1
        response = events.list_targets_by_rule(Rule=test_timer_signal.resource_access_spec.timer_id)
        assert response["Targets"][0]["Id"] == self.expected_lambda_name
        self.patch_aws_stop()

    def test_processor_process_old_timer_signal_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        events = self.params[CommonParams.BOTO_SESSION].client(service_name="events", region_name=self.region)
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        self.mock_processor.activate()
        test_timer_signal = self.create_timer_signal("test123")
        self.mock_processor._process_internal_timers_signals({test_timer_signal}, {})
        num_remove_permission_before = processor_driver.remove_permission.call_count
        self.mock_processor._process_internal_timers_signals({}, {test_timer_signal})
        num_remove_permission_after = processor_driver.remove_permission.call_count
        assert (num_remove_permission_after - num_remove_permission_before) == 1
        with pytest.raises(Exception) as error:
            events.list_targets_by_rule(Rule=test_timer_signal.resource_access_spec.timer_id)
        assert error.typename == "ResourceNotFoundException"
        self.patch_aws_stop()

    def test_processor_event_handler_glue_table_event(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        self.mock_processor.activate()
        self.mock_host_platform._routing_table = MagicMock(return_value=MagicMock())
        self.mock_host_platform.routing_table.receive = MagicMock()
        self.mock_processor.event_handler(self.mock_host_platform, self.andes_event, None)
        # Asserting resource path argument in receive call
        assert self.mock_host_platform.routing_table.receive.call_args_list[0][0][2] == self.expected_andes_resource_path
        self.patch_aws_stop()

    def test_processor_event_handler_s3_event(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        self.mock_host_platform.context_id = "test123"
        self.mock_processor.dev_init(self.mock_host_platform)
        self.mock_processor.activate()
        self.mock_host_platform._routing_table = MagicMock(return_value=MagicMock())
        self.mock_host_platform.routing_table.receive = MagicMock()
        self.mock_processor.event_handler(self.mock_host_platform, self.s3_event, None)
        assert self.mock_host_platform.routing_table.receive.call_args_list[0][0][2] == self.expected_s3_resource_path
        self.patch_aws_stop()
