# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
import datetime
from test.intelliflow.core.platform.driver_test_utils import DriverTestUtils
from test.intelliflow.core.signal_processing.routing_runtime_constructs import create_incoming_signal
from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal
from typing import Any, Dict, List, cast
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import ClientError
from dateutil.tz import tzlocal

import intelliflow.core.platform.drivers.compute.aws_sagemaker.transform_job as compute_driver
from intelliflow.core.platform.constructs import ConstructPermission, RoutingTable
from intelliflow.core.platform.definitions.aws.common import CommonParams
from intelliflow.core.platform.definitions.aws.sagemaker.client_wrapper import SagemakerBuiltinAlgo
from intelliflow.core.platform.definitions.compute import (
    ComputeFailedResponse,
    ComputeFailedResponseType,
    ComputeFailedSessionState,
    ComputeResourceDesc,
    ComputeResponseType,
    ComputeSessionDesc,
    ComputeSessionStateType,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.platform.development import AWSConfiguration, HostPlatform
from intelliflow.core.platform.drivers.compute.aws_sagemaker.transform_job import AWSSagemakerTransformJobBatchCompute
from intelliflow.core.signal_processing import DimensionSpec, Slot
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.routing_runtime_constructs import RuntimeLinkNode
from intelliflow.core.signal_processing.signal import Signal, SignalDimensionLink, SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import (
    ContentType,
    DatasetSignalSourceAccessSpec,
    DataType,
    InternalDatasetSignalSourceAccessSpec,
    ModelSignalSourceFormat,
    SignalSourceAccessSpec,
    SignalSourceType,
)
from intelliflow.core.signal_processing.slot import SlotType
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSSagemakerBatchCompute(AWSTestBase, DriverTestUtils):
    params = {}

    def setup_platform_and_params(self):
        self.init_common_utils()
        self.params[CommonParams.BOTO_SESSION] = boto3.Session(None, None, None, self.region)
        self.params[CommonParams.REGION] = self.region
        self.params[CommonParams.ACCOUNT_ID] = self.account_id
        self.params[CommonParams.IF_DEV_ROLE] = "arn:partition:111111111:role/DevRole"
        self.params[CommonParams.IF_EXE_ROLE] = "arn:partition:111111111:role/ExeRole"

    def get_driver_and_platform(self):
        mock_compute = AWSSagemakerTransformJobBatchCompute(self.params)
        mock_platform = HostPlatform(
            AWSConfiguration.builder()
            .with_default_credentials(as_admin=True)
            .with_region("us-east-1")
            .with_batch_compute(AWSSagemakerTransformJobBatchCompute)
            .build()
        )
        mock_platform.should_load_constructs = lambda: False

        from collections import namedtuple

        TestStorage = namedtuple("TestStorage", ["get_storage_resource_path"])
        mock_platform._storage = TestStorage(get_storage_resource_path=lambda: "arn:aws:s3:dummy")

        return (
            mock_compute,
            mock_platform,
        )

    def test_init_constructor_sm(self):
        self.patch_aws_start()
        self.setup_platform_and_params()

        mock_compute, _ = self.get_driver_and_platform()

        assert mock_compute._sagemaker is not None
        assert mock_compute._s3 is not None

        self.patch_aws_stop()

    def test_activate_should_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()

        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test1"
        mock_compute.dev_init(mock_host_platform)

        mock_compute.activate()

        self.patch_aws_stop()

    def test_compute_when_internal_signal_should_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test2"
        mock_compute.dev_init(mock_host_platform)

        input_signal, test_slot, materialized_output = self._prepare_test_input_internal_signal()
        mock_compute.provide_output_attributes(input_signal, test_slot, {})
        mock_compute.activate()
        compute_response = mock_compute.compute(None, input_signal, test_slot, materialized_output, "test_execution_id")

        assert compute_response.response_type == ComputeResponseType.SUCCESS
        assert compute_response.resource_desc.resource_name.startswith("if-transform-sm_test2-")
        # this is defined in patch_sagemaker_start()
        assert compute_response.resource_desc.resource_path == "sagemaker_batch_transform_job_arn"
        (
            _sagemaker,
            actual_job_name,
            actual_model_name,
            actual_transform_output,
            actual_transform_input,
            actual_transform_resources,
        ) = compute_driver.start_batch_transform_job.call_args_list[0][0]
        assert actual_job_name.startswith("if-transform-sm_test2-")
        assert actual_model_name.startswith("if-sm_test2-")
        # gets defaulted when transform resources are not provided
        assert actual_transform_resources.get("InstanceType") == "ml.m4.xlarge"
        assert actual_transform_resources.get("InstanceCount") == 1
        assert actual_transform_input.get("DataSource").get("S3DataSource").get("S3DataType") == "S3Prefix"
        assert actual_transform_input.get("DataSource").get("S3DataSource").get("S3Uri") == ""
        assert actual_transform_output.get("S3OutputPath") == "/internal_data/my_data_1/1"
        assert actual_transform_output.get("Accept") == ContentType.CSV
        assert actual_transform_output.get("AssembleWith") == "Line"

        self.patch_aws_stop()

    def test_compute_when_get_model_artifacts_path_returns_none_for_builtin_algo_should_fail(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test3"
        mock_compute.dev_init(mock_host_platform)

        input_signal, test_slot, materialized_output = self._prepare_test_input_internal_signal()
        model_signal = input_signal[0]
        algo_spec = model_signal.resource_access_spec.attrs["model_metadata"]["AlgorithmSpecification"]
        del algo_spec["TrainingImage"]
        algo_spec["AlgorithmName"] = SagemakerBuiltinAlgo.XGBOOST.value
        mock_compute.provide_output_attributes(input_signal, test_slot, {})
        mock_compute.activate()
        with patch.object(mock_compute, "_get_model_artifacts_path", autospec=True, return_value=None):
            compute_response = mock_compute.compute(None, input_signal, test_slot, materialized_output, "test_execution_id")

            assert compute_response.response_type == ComputeResponseType.FAILED
            assert isinstance(compute_response, ComputeFailedResponse)
            compute_response = cast(ComputeFailedResponse, compute_response)
            assert compute_response.resource_desc.resource_name.startswith("if-transform-sm_test3-")
            assert compute_response.failed_response_type == ComputeFailedResponseType.UNKNOWN
            compute_driver.create_sagemaker_model.assert_not_called()
            compute_driver.start_batch_transform_job.assert_not_called()

        self.patch_aws_stop()

    def test_compute_when_get_model_artifacts_path_returns_none_for_custom_algo_should_not_fail(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test4"
        mock_compute.dev_init(mock_host_platform)

        input_signal, test_slot, materialized_output = self._prepare_test_input_internal_signal()
        mock_compute.provide_output_attributes(input_signal, test_slot, {})
        mock_compute.activate()
        with patch.object(mock_compute, "_get_model_artifacts_path", autospec=True, return_value=None):
            compute_response = mock_compute.compute(None, input_signal, test_slot, materialized_output, "test_execution_id")

            assert compute_response.response_type == ComputeResponseType.SUCCESS
            compute_driver.create_sagemaker_model.assert_called()
            compute_driver.start_batch_transform_job.assert_called()

        self.patch_aws_stop()

    def test_compute_when_create_model_fail_with_retriable_error_should_fail_with_transient_response(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test5"
        mock_compute.dev_init(mock_host_platform)

        input_signal, test_slot, materialized_output = self._prepare_test_input_internal_signal()
        mock_compute.provide_output_attributes(input_signal, test_slot, {})
        mock_compute.activate()
        mock_error_response = {"Error": {"Code": "ResourceLimitExceeded"}}
        with patch.object(compute_driver, "create_sagemaker_model", side_effect=ClientError(mock_error_response, "op")):
            compute_response = mock_compute.compute(None, input_signal, test_slot, materialized_output, "test_execution_id")

            assert compute_response.response_type == ComputeResponseType.FAILED
            assert isinstance(compute_response, ComputeFailedResponse)
            compute_response = cast(ComputeFailedResponse, compute_response)
            assert compute_response.resource_desc.resource_name.startswith("if-transform-sm_test5-")
            assert compute_response.failed_response_type == ComputeFailedResponseType.TRANSIENT
            compute_driver.start_batch_transform_job.assert_not_called()

        self.patch_aws_stop()

    def test_compute_when_create_model_fail_with_non_retriable_error_should_fail(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test6"
        mock_compute.dev_init(mock_host_platform)

        input_signal, test_slot, materialized_output = self._prepare_test_input_internal_signal()
        mock_compute.provide_output_attributes(input_signal, test_slot, {})
        mock_compute.activate()
        mock_error_response = {"Error": {"Code": "AccessDeniedError"}}
        with patch.object(compute_driver, "create_sagemaker_model", side_effect=ClientError(mock_error_response, "op")):
            compute_response = mock_compute.compute(None, input_signal, test_slot, materialized_output, "test_execution_id")

            assert compute_response.response_type == ComputeResponseType.FAILED
            assert isinstance(compute_response, ComputeFailedResponse)
            compute_response = cast(ComputeFailedResponse, compute_response)
            assert compute_response.resource_desc.resource_name.startswith("if-transform-sm_test6-")
            assert compute_response.failed_response_type == ComputeFailedResponseType.UNKNOWN
            compute_driver.start_batch_transform_job.assert_not_called()

        self.patch_aws_stop()

    def test_compute_when_start_batch_transform_job_fail_with_retriable_error_should_fail_with_transient_response(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test7"
        mock_compute.dev_init(mock_host_platform)

        input_signal, test_slot, materialized_output = self._prepare_test_input_internal_signal()
        mock_compute.provide_output_attributes(input_signal, test_slot, {})
        mock_compute.activate()
        mock_error_response = {"Error": {"Code": "ResourceLimitExceeded"}}
        with patch.object(compute_driver, "start_batch_transform_job", side_effect=ClientError(mock_error_response, "op")):
            compute_response = mock_compute.compute(None, input_signal, test_slot, materialized_output, "test_execution_id")

            assert compute_response.response_type == ComputeResponseType.FAILED
            assert isinstance(compute_response, ComputeFailedResponse)
            compute_response = cast(ComputeFailedResponse, compute_response)
            assert compute_response.resource_desc.resource_name.startswith("if-transform-sm_test7-")
            assert compute_response.failed_response_type == ComputeFailedResponseType.TRANSIENT

        self.patch_aws_stop()

    def test_compute_when_start_batch_transform_job_fail_with_non_retriable_error_should_fail(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test8"
        mock_compute.dev_init(mock_host_platform)

        input_signal, test_slot, materialized_output = self._prepare_test_input_internal_signal()
        mock_compute.provide_output_attributes(input_signal, test_slot, {})
        mock_compute.activate()
        mock_error_response = {"Error": {"Code": "AccessDeniedError"}}
        with patch.object(compute_driver, "start_batch_transform_job", side_effect=ClientError(mock_error_response, "op")):
            compute_response = mock_compute.compute(None, input_signal, test_slot, materialized_output, "test_execution_id")

            assert compute_response.response_type == ComputeResponseType.FAILED
            assert isinstance(compute_response, ComputeFailedResponse)
            compute_response = cast(ComputeFailedResponse, compute_response)
            assert compute_response.resource_desc.resource_name.startswith("if-transform-sm_test8-")
            assert compute_response.failed_response_type == ComputeFailedResponseType.UNKNOWN

        self.patch_aws_stop()

    def test_compute_when_signal_source_s3(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test9"
        mock_compute.dev_init(mock_host_platform)

        input_signal, test_slot, materialized_output = self._prepare_test_input_s3_signal()

        mock_compute.provide_output_attributes(input_signal, test_slot, {})
        mock_compute.activate()
        compute_response = mock_compute.compute(None, input_signal, test_slot, materialized_output, "test_execution_id")

        assert compute_response.response_type == ComputeResponseType.SUCCESS
        assert compute_response.resource_desc.resource_name.startswith("if-transform-sm_test9-")
        # this is defined in patch_sagemaker_start()
        assert compute_response.resource_desc.resource_path == "sagemaker_batch_transform_job_arn"
        (
            _sagemaker,
            actual_job_name,
            actual_model_name,
            actual_transform_output,
            actual_transform_input,
            actual_transform_resources,
        ) = compute_driver.start_batch_transform_job.call_args_list[0][0]

        assert actual_job_name.startswith("if-transform-sm_test9-")
        assert actual_model_name.startswith("if-sm_test9-")

        # gets defaulted when transform resources are not provided
        assert actual_transform_resources.get("InstanceType") == "ml.m4.xlarge"
        assert actual_transform_resources.get("InstanceCount") == 1
        assert actual_transform_input.get("DataSource").get("S3DataSource").get("S3DataType") == "S3Prefix"
        assert actual_transform_input.get("DataSource").get("S3DataSource").get("S3Uri") == ""
        assert actual_transform_output.get("S3OutputPath") == "/internal_data/my_data_1/1"
        assert actual_transform_output.get("Accept") == ContentType.CSV
        assert actual_transform_output.get("AssembleWith") == "Line"

        self.patch_aws_stop()

    def test_compute_dev_init(self):
        """
        With shorter UUID, the auto-generated job name should always be valid.
        """
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test10"

        mock_compute.dev_init(mock_host_platform)

        self.patch_aws_stop()

    def test_get_session_state_when_job_completed(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test11"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.activate()

        session_state = mock_compute.get_session_state(
            ComputeSessionDesc("job_name", ComputeResourceDesc("job_name", "arn", driver=AWSSagemakerTransformJobBatchCompute.__class__)),
            None,
        )

        assert session_state.state_type == ComputeSessionStateType.COMPLETED

        self.patch_aws_stop()

    def test_get_session_state_when_job_failed(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test12"
        mock_compute.dev_init(mock_host_platform)

        mocked_job_failed_response = {
            "TransformJobStatus": "Failed",
            "CreationTime": datetime.datetime(2022, 6, 22, 23, 51, 34, 337000, tzinfo=tzlocal()),
            "TransformStartTime": datetime.datetime(2022, 6, 22, 0, 6, 30, 539000, tzinfo=tzlocal()),
            "TransformEndTime": datetime.datetime(2022, 6, 22, 0, 7, 46, 611000, tzinfo=tzlocal()),
        }
        # Note we can not use autospec=True here, because describe_sagemaker_transform_job has already been substituted
        # by a MagicMock at this moment (in patch_sagemaker_start), autospec a MagicMock object has no sense
        with patch.object(compute_driver, "describe_sagemaker_transform_job", return_value=mocked_job_failed_response) as mocked_method:
            session_state = mock_compute.get_session_state(
                ComputeSessionDesc(
                    "job_name", ComputeResourceDesc("job_name", "arn", driver=AWSSagemakerTransformJobBatchCompute.__class__)
                ),
                None,
            )

            assert mocked_method.call_args_list[0][0][1] == "job_name"
            assert isinstance(session_state, ComputeFailedSessionState)
            assert session_state.state_type == ComputeSessionStateType.FAILED

        self.patch_aws_stop()

    def test_get_session_state_when_describe_transform_job_raise_retryable_exception_should_return_transient_failed(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test13"
        mock_compute.dev_init(mock_host_platform)

        mock_error_response = {"Error": {"Code": "InternalServerException"}}
        # Note we can not use autospec=True here, because describe_sagemaker_transform_job has already been substituted
        # by a MagicMock at this moment (in patch_sagemaker_start), autospec a MagicMock object has no sense
        with patch.object(
            compute_driver, "describe_sagemaker_transform_job", side_effect=ClientError(mock_error_response, "op")
        ) as mocked_method:
            session_state = mock_compute.get_session_state(
                ComputeSessionDesc(
                    "job_name", ComputeResourceDesc("job_name", "arn", driver=AWSSagemakerTransformJobBatchCompute.__class__)
                ),
                None,
            )

            assert mocked_method.call_args_list[0][0][1] == "job_name"
            assert session_state.state_type == ComputeSessionStateType.TRANSIENT_UNKNOWN

        self.patch_aws_stop()

    def test_get_session_state_when_describe_transform_job_errored_should_return_failed_state(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test14"
        mock_compute.dev_init(mock_host_platform)

        mock_error_response = {"Error": {"Code": "AccessDeniedError"}}
        # Note we can not use autospec=True here, because describe_sagemaker_transform_job has already been substituted
        # by a MagicMock at this moment (in patch_sagemaker_start), autospec a MagicMock object has no sense
        with patch.object(
            compute_driver, "describe_sagemaker_transform_job", side_effect=ClientError(mock_error_response, "op")
        ) as mocked_method:
            session_state = mock_compute.get_session_state(
                ComputeSessionDesc(
                    "job_name", ComputeResourceDesc("job_name", "arn", driver=AWSSagemakerTransformJobBatchCompute.__class__)
                ),
                None,
            )

            assert mocked_method.call_args_list[0][0][1] == "job_name"
            assert isinstance(session_state, ComputeFailedSessionState)
            session_state = cast(ComputeFailedSessionState, session_state)
            assert session_state.failed_type == ComputeSessionStateType.UNKNOWN

        self.patch_aws_stop()

    def test_kill_session_when_job_is_processing(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test15"
        mock_compute.dev_init(mock_host_platform)

        mock_transform_job_name = "mock_transform_job_name"
        active_compute_record = RoutingTable.ComputeRecord(
            trigger_timestamp_utc=None,
            materialized_inputs=None,
            slot=None,
            materialized_output=None,
            execution_context_id=None,
            state=ComputeSuccessfulResponse(ComputeSuccessfulResponseType.PROCESSING, ComputeSessionDesc(mock_transform_job_name, None)),
            session_state=None,
        )
        mock_compute.kill_session(active_compute_record)

        assert mock_transform_job_name == compute_driver.stop_batch_transform_job.call_args_list[0][0][1]

        self.patch_aws_stop()

    def test_kill_session_when_job_is_failed(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test16"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.activate()

        active_compute_record = RoutingTable.ComputeRecord(
            trigger_timestamp_utc=None,
            materialized_inputs=None,
            slot=None,
            materialized_output=None,
            execution_context_id=None,
            state=ComputeFailedResponse(None, None, None, None),
            session_state=None,
        )
        mock_compute.kill_session(active_compute_record)

        assert not compute_driver.stop_batch_transform_job.called

        self.patch_aws_stop()

    def test_kill_session_when_stop_batch_transform_job_first_failed_with_retryable_exception_then_succeed(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test17"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.activate()

        mock_transform_job_name = "mock_transform_job_name"
        active_compute_record = RoutingTable.ComputeRecord(
            trigger_timestamp_utc=None,
            materialized_inputs=None,
            slot=None,
            materialized_output=None,
            execution_context_id=None,
            state=ComputeSuccessfulResponse(ComputeSuccessfulResponseType.PROCESSING, ComputeSessionDesc(mock_transform_job_name, None)),
            session_state=None,
        )

        mock_error_response = {"Error": {"Code": "ValidationException"}}
        with patch.object(compute_driver, "stop_batch_transform_job", side_effect=[ClientError(mock_error_response, "op"), ""]):
            mock_compute.kill_session(active_compute_record)

        self.patch_aws_stop()

    def test_provide_devtime_permissions(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test18"
        mock_compute.dev_init(mock_host_platform)
        permissions = mock_compute.provide_devtime_permissions(self.params)

        expected_permissions = [
            ConstructPermission(
                resource=["arn:aws:sagemaker:us-east-1:123456789012:transform-job/if-transform-*-*"], action=["sagemaker:*"]
            ),
            ConstructPermission(resource=["arn:aws:sagemaker:us-east-1:123456789012:model/if-*-*"], action=["sagemaker:*"]),
            ConstructPermission(["*"], ["sagemaker:Get*", "sagemaker:List*", "sagemaker:Describe*"]),
            ConstructPermission(resource=["arn:aws:ecr:us-east-1:123456789012:repository/*"], action=["ecr:Describe*"]),
            ConstructPermission(resource=["*"], action=["s3:List*", "s3:Get*", "s3:Head*"]),
        ]
        assert expected_permissions == permissions
        self.patch_aws_stop()

    def test_provide_output_attributes_incorrect_model_artifact(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test19"
        mock_compute.dev_init(mock_host_platform)

        inputs: List[Signal] = [
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                TestSignal.signal_internal_1.resource_access_spec,
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            )
        ]
        test_user_attrs: Dict[str, Any] = {}
        test_slot = Slot(
            SlotType.ASYNC_BATCH_COMPUTE,
            None,
            None,
            None,
            None,
            None,
        )

        # 1 - Test no model artifact passed as input
        with pytest.raises(Exception) as error:
            mock_compute.provide_output_attributes(inputs, test_user_attrs, test_slot)
        assert error.typename == "ValueError"

        # Only one input should be model artifact
        inputs: List[Signal] = [
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.SAGEMAKER,
                    "",
                    {
                        "data_type": DataType.MODEL_ARTIFACT,
                    },
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
            Signal(
                SignalType.INTERNAL_PARTITION_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.SAGEMAKER,
                    "",
                    {
                        "data_type": DataType.MODEL_ARTIFACT,
                    },
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
        ]

        with pytest.raises(Exception) as error:
            mock_compute.provide_output_attributes(inputs, test_user_attrs, test_slot)
        assert error.typename == "ValueError"

        test_signal = Signal(
            SignalType.EXTERNAL_S3_OBJECT_CREATION,
            SignalSourceAccessSpec(SignalSourceType.SAGEMAKER, "", {"data_type": DataType.MODEL_ARTIFACT}),
            SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
        )
        # Only one input should be model artifact
        inputs: List[Signal] = [
            test_signal,
        ]

        with pytest.raises(Exception) as error:
            mock_compute.provide_output_attributes(inputs, test_user_attrs, test_slot)
        assert error.typename == "ValueError"

        self.patch_aws_stop()

    def test_provide_output_attributes_incorrect_metadata(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test20"
        mock_compute.dev_init(mock_host_platform)

        test_user_attrs: Dict[str, Any] = {}
        test_slot = Slot(
            SlotType.ASYNC_BATCH_COMPUTE,
            None,
            None,
            None,
            None,
            None,
        )
        # Input model has no metadata
        inputs: List[Signal] = [
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.SAGEMAKER,
                    "",
                    {"data_type": DataType.MODEL_ARTIFACT, "model_format": ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB},
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
        ]

        with pytest.raises(Exception) as error:
            mock_compute.provide_output_attributes(inputs, test_user_attrs, test_slot)
        assert error.typename == "ValueError"

        # Input model has multiple metadata
        inputs: List[Signal] = {
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.SAGEMAKER,
                    "",
                    {"data_type": DataType.MODEL_ARTIFACT, "model_format": ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB},
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
        }

        with pytest.raises(Exception) as error:
            mock_compute.provide_output_attributes(inputs, test_user_attrs, test_slot)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_provide_output_attributes_incorrect_algo_specs(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test21"
        mock_compute.dev_init(mock_host_platform)

        test_user_attrs: Dict[str, Any] = {}

        test_slot = Slot(
            SlotType.ASYNC_BATCH_COMPUTE,
            None,
            None,
            None,
            None,
            None,
        )
        # 2 - No Algo specs
        inputs: List[Signal] = {
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.SAGEMAKER,
                    "",
                    {
                        "data_type": DataType.MODEL_ARTIFACT,
                        "model_format": ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB,
                        "model_metadata": {"AlgorithmSpecification": {}},
                    },
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
        }

        with pytest.raises(Exception) as error:
            mock_compute.provide_output_attributes(inputs, test_user_attrs, test_slot)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_provide_output_attributes_incorrect_datasets(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test22"
        mock_compute.dev_init(mock_host_platform)

        test_user_attrs: Dict[str, Any] = {}

        test_slot = Slot(
            SlotType.ASYNC_BATCH_COMPUTE,
            None,
            None,
            None,
            None,
            None,
        )
        # 2 - No dataset input
        inputs: List[Signal] = {
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.SAGEMAKER,
                    "",
                    {
                        "data_type": DataType.MODEL_ARTIFACT,
                        "model_format": ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB,
                        "model_metadata": {
                            "AlgorithmSpecification": {
                                "TrainingImage": Any,  # Use something else here
                            }
                        },
                    },
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
        }

        with pytest.raises(Exception) as error:
            mock_compute.provide_output_attributes(inputs, test_user_attrs, test_slot)
        assert error.typename == "ValueError"

        # multiple datasets
        inputs: List[Signal] = {
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.SAGEMAKER,
                    "",
                    {
                        "data_type": DataType.MODEL_ARTIFACT,
                        "model_format": ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB,
                        "model_metadata": {
                            "AlgorithmSpecification": {
                                "TrainingImage": Any,  # Use something else here
                            }
                        },
                    },
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
            Signal(
                SignalType.INTERNAL_METRIC_DATA_CREATION,
                DatasetSignalSourceAccessSpec(SignalSourceType.INTERNAL, "", {"data_type": DataType.DATASET}),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                DatasetSignalSourceAccessSpec(SignalSourceType.INTERNAL, "", {"data_type": DataType.DATASET}),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
        }

        with pytest.raises(Exception) as error:
            mock_compute.provide_output_attributes(inputs, test_user_attrs, test_slot)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_provide_output_attributes_incorrect_dataset_header(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test23"
        mock_compute.dev_init(mock_host_platform)

        test_user_attrs = {}

        test_slot = Slot(
            SlotType.ASYNC_BATCH_COMPUTE,
            None,
            None,
            None,
            None,
            None,
        )

        # Test input data set having header == "true" condition
        inputs: List[Signal] = {
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.SAGEMAKER,
                    "",
                    {
                        "data_type": DataType.MODEL_ARTIFACT,
                        "model_format": ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB,
                        "model_metadata": {
                            "AlgorithmSpecification": {
                                "TrainingImage": Any,  # Use something else here
                            }
                        },
                    },
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
            Signal(
                SignalType.INTERNAL_METRIC_DATA_CREATION,
                DatasetSignalSourceAccessSpec(SignalSourceType.INTERNAL, "", {"data_type": DataType.DATASET, "header": "True"}),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
        }

        test_slot = {
            "data_type": Any,
        }

        with pytest.raises(Exception) as error:
            mock_compute.provide_output_attributes(inputs, test_user_attrs, test_slot)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_provide_output_attributes_incorrect_datakey(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test24"
        mock_compute.dev_init(mock_host_platform)

        test_user_attrs: Dict[str, Any] = {}

        test_slot = Slot(
            SlotType.ASYNC_BATCH_COMPUTE,
            None,
            None,
            None,
            None,
            None,
        )

        # Test data type key defined or left empty
        inputs: List[Signal] = {
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.SAGEMAKER,
                    "",
                    {
                        "data_type": DataType.MODEL_ARTIFACT,
                        "model_format": ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB,
                        "model_metadata": {
                            "AlgorithmSpecification": {
                                "TrainingImage": Any,  # Use something else here
                            }
                        },
                    },
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
            Signal(
                SignalType.INTERNAL_METRIC_DATA_CREATION,
                InternalDatasetSignalSourceAccessSpec(SignalSourceType.INTERNAL, "", {"data_type": DataType.DATASET, "header": "false"}),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
        }

        test_slot = {
            "data_type": Any,
        }

        with pytest.raises(Exception) as error:
            mock_compute.provide_output_attributes(inputs, test_user_attrs, test_slot)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_provide_output_attributes_incorrect_output_header(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test25"
        mock_compute.dev_init(mock_host_platform)

        test_user_attrs = {
            "header": True,
        }

        test_slot = Slot(
            SlotType.ASYNC_BATCH_COMPUTE,
            "",
            None,
            None,
            None,
            None,
        )

        # Test output data set having header == "true"
        inputs: List[Signal] = {
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.SAGEMAKER,
                    "",
                    {
                        "data_type": DataType.MODEL_ARTIFACT,
                        "model_format": ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB,
                        "model_metadata": {
                            "AlgorithmSpecification": {
                                "TrainingImage": Any,  # Use something else here
                            }
                        },
                    },
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
            Signal(
                SignalType.INTERNAL_METRIC_DATA_CREATION,
                DatasetSignalSourceAccessSpec(SignalSourceType.INTERNAL, "", {"data_type": DataType.DATASET, "header": "false"}),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
        }

        test_slot = {
            "data_type": Any,
        }

        with pytest.raises(Exception) as error:
            mock_compute.provide_output_attributes(inputs, test_user_attrs, test_slot)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_provide_runtime_default_policies(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test26"
        mock_compute.dev_init(mock_host_platform)

        runtime_default_policies = mock_compute.provide_runtime_default_policies()
        assert runtime_default_policies == []

        self.patch_aws_stop()

    def test_provide_runtime_trusted_entities(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "sm_test27"
        mock_compute.dev_init(mock_host_platform)

        runtime_trusted_entities = mock_compute.provide_runtime_trusted_entities()
        assert runtime_trusted_entities == ["sagemaker.amazonaws.com"]

        self.patch_aws_stop()

    @staticmethod
    def _prepare_test_input_internal_signal():
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal
        from test.intelliflow.core.signal_processing.signal.test_signal_link_node import TestSignalLinkNode, signal_dimension_tuple

        runtime_link_node_1 = RuntimeLinkNode(TestSignalLinkNode.signal_link_node_1)
        runtime_link_node_1_cloned = copy.deepcopy(runtime_link_node_1)
        output_spec = DimensionSpec.load_from_pretty({"output_dim": {type: Type.LONG}})

        output_dim_link_matrix = [
            SignalDimensionLink(
                signal_dimension_tuple(None, "output_dim"), lambda x: x, signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1")
            )
        ]
        output_filter = runtime_link_node_1_cloned.get_output_filter(output_spec, output_dim_link_matrix)
        output_signal = Signal(
            TestSignal.signal_internal_1.type,
            TestSignal.signal_internal_1.resource_access_spec,
            SignalDomainSpec(output_spec, output_filter, TestSignal.signal_internal_1.domain_spec.integrity_check_protocol),
            "sample_output_data",
        )

        inputs: List[Signal] = [
            Signal(
                SignalType.INTERNAL_PARTITION_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.INTERNAL,
                    "S3://bucket-name/model.tar.gz",
                    {
                        "data_type": DataType.MODEL_ARTIFACT,
                        "model_format": ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB,
                        "model_metadata": {
                            "AlgorithmSpecification": {
                                "TrainingImage": Any,  # Use something else here
                            }
                        },
                        # "bucket": "S3://bucket-name/*/model.tar.gz"
                    },
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
                "test-signal-001",
                False,
            ),
            Signal(
                SignalType.INTERNAL_METRIC_DATA_CREATION,
                DatasetSignalSourceAccessSpec(SignalSourceType.INTERNAL, "", {"data_type": DataType.DATASET, "header": "false"}),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
        ]

        input_signal = create_incoming_signal(TestSignal.signal_internal_1, [1])
        runtime_link_node_1_cloned.receive(input_signal)
        assert runtime_link_node_1_cloned.is_ready()
        materialized_output = runtime_link_node_1_cloned.materialize_output(output_signal, output_dim_link_matrix)

        test_slot = Slot(
            SlotType.ASYNC_BATCH_COMPUTE,
            "",
            None,
            None,
            {"data_type": DataType.DATASET, "TransformResources": None},
            None,
        )

        return inputs, test_slot, materialized_output

    @staticmethod
    def _prepare_test_input_s3_signal():
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal
        from test.intelliflow.core.signal_processing.signal.test_signal_link_node import TestSignalLinkNode, signal_dimension_tuple

        runtime_link_node_1 = RuntimeLinkNode(TestSignalLinkNode.signal_link_node_1)
        runtime_link_node_1_cloned = copy.deepcopy(runtime_link_node_1)
        output_spec = DimensionSpec.load_from_pretty({"output_dim": {type: Type.LONG}})

        output_dim_link_matrix = [
            SignalDimensionLink(
                signal_dimension_tuple(None, "output_dim"), lambda x: x, signal_dimension_tuple(TestSignal.signal_internal_1, "dim_1_1")
            )
        ]
        output_filter = runtime_link_node_1_cloned.get_output_filter(output_spec, output_dim_link_matrix)
        output_signal = Signal(
            TestSignal.signal_internal_1.type,
            TestSignal.signal_internal_1.resource_access_spec,
            SignalDomainSpec(output_spec, output_filter, TestSignal.signal_internal_1.domain_spec.integrity_check_protocol),
            "sample_output_data",
        )

        inputs: List[Signal] = [
            Signal(
                SignalType.EXTERNAL_S3_OBJECT_CREATION,
                SignalSourceAccessSpec(
                    SignalSourceType.S3,
                    "S3://bucket-name/model.tar.gz",
                    {
                        "data_type": DataType.MODEL_ARTIFACT,
                        "model_format": ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB,
                        "model_metadata": {
                            "AlgorithmSpecification": {
                                "TrainingImage": Any,  # Use something else here
                            }
                        },
                        # "bucket": "S3://bucket-name/*/model.tar.gz"
                    },
                ),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
                "test-signal-001",
                False,
            ),
            Signal(
                SignalType.INTERNAL_METRIC_DATA_CREATION,
                DatasetSignalSourceAccessSpec(SignalSourceType.INTERNAL, "", {"data_type": DataType.DATASET, "header": "false"}),
                SignalDomainSpec(DimensionSpec.load_from_pretty({"input_dim": {type: Type.LONG}}), None, None),
            ),
        ]

        input_signal = create_incoming_signal(TestSignal.signal_internal_1, [1])
        runtime_link_node_1_cloned.receive(input_signal)
        assert runtime_link_node_1_cloned.is_ready()
        materialized_output = runtime_link_node_1_cloned.materialize_output(output_signal, output_dim_link_matrix)

        test_slot = Slot(
            SlotType.ASYNC_BATCH_COMPUTE,
            "",
            None,
            None,
            {"data_type": DataType.DATASET, "TransformResources": None},
            None,
        )

        return inputs, test_slot, materialized_output
