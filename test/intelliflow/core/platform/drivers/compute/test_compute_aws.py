# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from test.intelliflow.core.platform.driver_test_utils import DriverTestUtils
from test.intelliflow.core.signal_processing.routing_runtime_constructs import create_incoming_signal
from unittest.mock import MagicMock

import boto3
import pytest
import json

import intelliflow.core.platform.drivers.compute.aws as compute_driver
from intelliflow.core.platform.definitions.aws.common import CommonParams
from intelliflow.core.platform.definitions.aws.emr.client_wrapper import get_emr_cluster_failure_type
from intelliflow.core.platform.definitions.aws.glue.client_wrapper import GlueJobLanguage
from intelliflow.core.platform.definitions.aws.s3.bucket_wrapper import bucket_exists
from intelliflow.core.platform.definitions.compute import ComputeFailedSessionStateType
from intelliflow.core.platform.development import AWSConfiguration, HostPlatform
from intelliflow.core.platform.drivers.compute.aws import AWSGlueBatchComputeBasic
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.routing_runtime_constructs import *
from intelliflow.core.signal_processing.signal import SignalDimensionLink, SignalDomainSpec
from intelliflow.core.signal_processing.slot import SlotType
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSGlueBatchComputeBasic(AWSTestBase, DriverTestUtils):
    expected_glue_job_name = "IntelliFlow-AWSGlueBatchComputeBasic-GlueDefaultABIPython-test123-us-east-1"
    expected_glue_job_arn = (
        "arn:aws:glue:us-east-1:123456789012:job/IntelliFlow-AWSGlueBatchComputeBasic-GlueDefaultABIPython-test123-us-east-1"
    )
    expected_glue_job_name_v_2_0 = "IntelliFlow-AWSGlueBatchComputeBasic-GlueDefaultABIPython-test123v2_0-us-east-1"
    expected_glue_job_arn_v_2_0 = (
        "arn:aws:glue:us-east-1:123456789012:job/IntelliFlow-AWSGlueBatchComputeBasic-GlueDefaultABIPython-test123v2_0-us-east-1"
    )
    expected_glue_bucket = "if-awsglue-test123-123456789012-us-east-1"

    # Slots targeting different value errors
    slot_with_incorrect_max_cap = Slot(
        SlotType.ASYNC_BATCH_COMPUTE, "output = input1", Lang.PYTHON, ABI.GLUE_EMBEDDED, {"MaxCapacity": 1}, None
    )
    slot_with_incorrect_timeout = Slot(
        SlotType.ASYNC_BATCH_COMPUTE, "output = input1", Lang.PYTHON, ABI.GLUE_EMBEDDED, {"MaxCapacity": 50, "Timeout": 0}, None
    )
    slot_leading_to_either_max_capacity_or_worker_type_allowed_exception = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {"MaxCapacity": 50, "Timeout": 20, "WorkerType": "GTX"},
        None,
    )
    slot_with_incorrect_worker_type = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {"Timeout": 20, "WorkerType": "GTX", "NumberOfWorkers": 10},
        None,
    )
    slot_with_incorrect_num_of_workers = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {"Timeout": 20, "WorkerType": "G.1X", "NumberOfWorkers": 0},
        None,
    )
    slot_with_incorrect_num_of_workers_for_worker_type_g_1_x = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {"Timeout": 20, "WorkerType": "G.1X", "NumberOfWorkers": 300},
        None,
    )
    slot_with_incorrect_num_of_workers_for_worker_type_g_2_x = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {"Timeout": 20, "WorkerType": "G.2X", "NumberOfWorkers": 150},
        None,
    )

    def setup_platform_and_params(self):
        self.params = {}
        self.init_common_utils()
        self.params[CommonParams.BOTO_SESSION] = boto3.Session(None, None, None, self.region)
        self.params[CommonParams.REGION] = self.region
        self.params[CommonParams.ACCOUNT_ID] = self.account_id
        self.params[CommonParams.IF_DEV_ROLE] = "DevRole"
        self.params[CommonParams.IF_EXE_ROLE] = "ExeRole"

    def get_signals_slots(self):
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
        input_signal = create_incoming_signal(TestSignal.signal_internal_1, [1])
        runtime_link_node_1_cloned.receive(input_signal)
        assert runtime_link_node_1_cloned.is_ready()
        materialized_output = runtime_link_node_1_cloned.materialize_output(output_signal, output_dim_link_matrix)
        test_slot = Slot(
            SlotType.ASYNC_BATCH_COMPUTE, "output = input1", Lang.PYTHON, ABI.GLUE_EMBEDDED, {"MaxCapacity": 2, "GlueVersion": "1.0"}, None
        )

        return input_signal, test_slot, materialized_output

    def get_driver_and_platform(self):
        platform = HostPlatform(
            AWSConfiguration.builder()
            .with_default_credentials(as_admin=True)
            .with_region("us-east-1")
            .with_batch_compute(AWSGlueBatchComputeBasic)
            .build()
        )
        platform.should_load_constructs = lambda: False
        return (
            AWSGlueBatchComputeBasic(self.params),
            platform,
        )

    def test_compute_dev_init_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123"
        mock_compute.dev_init(mock_host_platform)
        assert mock_compute._glue_job_lang_map[GlueJobLanguage.PYTHON]["1.0"]["job_name"] == self.expected_glue_job_name
        assert mock_compute._glue_job_lang_map[GlueJobLanguage.PYTHON]["1.0"]["job_arn"] == self.expected_glue_job_arn
        assert mock_compute._glue_job_lang_map[GlueJobLanguage.PYTHON]["2.0"]["job_name"] == self.expected_glue_job_name_v_2_0
        assert mock_compute._glue_job_lang_map[GlueJobLanguage.PYTHON]["2.0"]["job_arn"] == self.expected_glue_job_arn_v_2_0
        assert mock_compute._bucket_name == self.expected_glue_bucket
        self.patch_aws_stop()

    def test_compute_dev_init_exception_max_bucket_len(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        with pytest.raises(Exception) as error:
            context_id = str()
            for c in range(30):
                context_id += str(c)
            mock_host_platform._context_id = context_id
            mock_compute.dev_init(mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_compute_dev_init_exception_max_job_name_len(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        with pytest.raises(Exception) as error:
            context_id = str()
            for c in range(300):
                context_id += str(c)
            mock_host_platform._context_id = context_id
            mock_compute.dev_init(mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_compute_provide_output_attributes(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        input_signal, test_slot, materialized_output = self.get_signals_slots()
        mock_compute.provide_output_attributes([input_signal], test_slot, dict())

        test_slot.extra_params.update({"partition_by": "1/0"})
        with pytest.raises(ValueError):
            mock_compute.provide_output_attributes([input_signal], test_slot, dict())

        test_slot.extra_params.update({"partition_by": [1, 2]})
        with pytest.raises(ValueError):
            mock_compute.provide_output_attributes([input_signal], test_slot, dict())

        test_slot.extra_params.update({"partition_by": '["col1", "col2"]'})
        with pytest.raises(ValueError):
            mock_compute.provide_output_attributes([input_signal], test_slot, dict())

        test_slot.extra_params.update({"partition_by": ["col1", "col2"]})
        mock_compute.provide_output_attributes([input_signal], test_slot, dict())

        test_slot.code_lang = Lang.SCALA
        with pytest.raises(ValueError):
            mock_compute.provide_output_attributes([input_signal], test_slot, dict())

        self.patch_aws_stop()

    def test_compute_hook_external_not_supported_signal_exception(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "glue_test2"
        mock_compute.dev_init(mock_host_platform)
        with pytest.raises(Exception) as error:
            mock_compute.hook_external([self.signal_unsupported])
        assert error.typename == "NotImplementedError"
        self.patch_aws_stop()

    def test_compute_hook_internal_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "glue_test3"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.hook_internal(self.route_1_basic)
        self.patch_aws_stop()

    @pytest.mark.parametrize(
        "test_slot",
        [
            slot_with_incorrect_max_cap,
            slot_with_incorrect_timeout,
            slot_leading_to_either_max_capacity_or_worker_type_allowed_exception,
            slot_with_incorrect_worker_type,
            slot_with_incorrect_num_of_workers,
            slot_with_incorrect_num_of_workers_for_worker_type_g_1_x,
            slot_with_incorrect_num_of_workers_for_worker_type_g_2_x,
        ],
    )
    def test_compute_hook_internal_with_incorrect_slot_exception(self, test_slot):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "glue_test4"
        mock_compute.dev_init(mock_host_platform)
        test_route = self.route_1_basic
        test_route._slots = [test_slot]
        with pytest.raises(Exception) as error:
            mock_compute.hook_internal(test_route)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_compute_activate_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "glue_test5"
        mock_compute.dev_init(mock_host_platform)
        mock_compute._extract_used_glue_jobs = MagicMock(
            side_effect=lambda: {
                GlueJobLanguage.PYTHON: {"1.0", "2.0", "3.0", "4.0", "5.0"},
                GlueJobLanguage.SCALA: {"1.0", "2.0", "3.0", "4.0", "5.0"},
            }
        )
        mock_compute.activate()
        s3 = boto3.resource("s3")
        assert bucket_exists(s3, mock_compute._bucket_name)
        # now for each version we create another job
        # versions multiplied by the number of langs (2),
        # so we expect the call_count to be 8.
        assert compute_driver.create_glue_job.call_count == 10
        assert compute_driver.delete_glue_job.call_count == 0

        mock_compute._extract_used_glue_jobs = MagicMock(
            side_effect=lambda: {GlueJobLanguage.PYTHON: {"2.0", "3.0"}, GlueJobLanguage.SCALA: {"2.0", "3.0"}}
        )
        mock_compute.activate()
        # now we expect the call_count to increase by 4 only as "1.0" version is not used.
        assert compute_driver.create_glue_job.call_count == (10 + 4)
        # three glue jobs (python-1.0 and scala-1.0) must be deleted
        assert compute_driver.delete_glue_job.call_count == (0 + 6)
        self.patch_aws_stop()

    def test_compute_computefunc_successful(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123"
        mock_compute.dev_init(mock_host_platform)
        mock_compute._extract_used_glue_jobs = MagicMock(
            side_effect=lambda: {GlueJobLanguage.PYTHON: {"1.0", "2.0", "3.0"}, GlueJobLanguage.SCALA: {"1.0", "2.0", "3.0"}}
        )
        mock_compute.activate()
        input_signal, test_slot, materialized_output = self.get_signals_slots()
        mock_compute.compute(None, [input_signal], test_slot, materialized_output, "test_execution_id")
        assert compute_driver.start_glue_job.call_args_list[0][0][3] == self.expected_glue_job_name
        assert compute_driver.start_glue_job.call_args_list[0][0][4]["--MaxCapacity"] == "2"
        assert compute_driver.start_glue_job.call_args_list[0][0][4]["--enable-glue-datacatalog"] == ""
        assert self.expected_glue_job_name in compute_driver.start_glue_job.call_args_list[0][0][4]["--INPUT_MAP"]
        assert self.expected_glue_job_name in compute_driver.start_glue_job.call_args_list[0][0][4]["--CLIENT_CODE"]
        assert compute_driver.start_glue_job.call_args_list[0][0][4]["--CLIENT_CODE_BUCKET"] == self.expected_glue_bucket
        assert self.expected_glue_job_name in compute_driver.start_glue_job.call_args_list[0][0][4]["--OUTPUT"]
        self.patch_aws_stop()

    @pytest.mark.parametrize(
        "describe_cluster_json",
        [
            {
                "State": "TERMINATED_WITH_ERRORS",
                "StateChangeReason": {
                    "Code": "VALIDATION_ERROR",
                    "Message": "The subnet is not large enough: the default subnet in your VPC is not large enough.",
                },
            },
            {
                "State": "TERMINATED_WITH_ERRORS",
                "StateChangeReason": {"Code": "VALIDATION_ERROR", "Message": "The EBS volume limit was exceeded"},
            },
            # TODO investigate why this has been considered as TRANSIENT before? Bad bootstrapping script would cause retries because of this.
            # {
            #    "State": "TERMINATED_WITH_ERRORS",
            #    "StateChangeReason": {
            #        "Code": "BOOTSTRAP_FAILURE",
            #        "Message": "On the master instance (i-0991cbf940d71cbfd), application provisioning failed",
            #    },
            # },
            {
                "State": "TERMINATED_WITH_ERRORS",
                "StateChangeReason": {
                    "Code": "INTERNAL_ERROR",
                    "Message": "EC2 is out of capacity for r5.24xlarge in availability zone us-east-1a. Learn more at https://docs.aws.amazon.com/console/elasticmapreduce/ERROR_noinstancecapacity",
                },
            },
            {
                "State": "TERMINATED_WITH_ERRORS",
                "StateChangeReason": {
                    "Code": "INTERNAL_ERROR",
                    "Message": "Throttled from Amazon EC2 while launching cluster",
                },
            },
            {
                "State": "TERMINATED_WITH_ERRORS",
                "StateChangeReason": {
                    "Code": "INTERNAL_ERROR",
                    "Message": "Failed to provision instances due to throttling from Amazon EC2",
                },
            },
            {
                "State": "TERMINATED_WITH_ERRORS",
                "StateChangeReason": {
                    "Code": "INTERNAL_ERROR",
                    "Message": "Request exceeds the EC2 service quota for that type.",
                },
            },
            {
                "State": "TERMINATED_WITH_ERRORS",
                "ErrorDetails": [
                    {
                        "ErrorCode": "INTERNAL_ERROR_EC2_INSUFFICIENT_CAPACITY_AZ",
                        "ErrorData": [
                            {"instance-type": "a_random_scifi_concept"},
                            {"availability-zone": "check your subnet"},
                            {
                                "public-doc": "https://docs.aws.amazon.com/emr/latest/ManagementGuide/INTERNAL_ERROR_EC2_INSUFFICIENT_CAPACITY_AZ.html"
                            },
                        ],
                        "ErrorMessage": "we believe you are doing the right thing",
                    }
                ],
                "StateChangeReason": {
                    "Code": "INTERNAL_ERROR",
                    "Message": "MAKE SURE that TRANSIENT error is extracted from ErrorDetails",
                },
            },
        ],
    )
    def test_transient_failures(self, describe_cluster_json):
        assert get_emr_cluster_failure_type(describe_cluster_json) == ComputeFailedSessionStateType.TRANSIENT
