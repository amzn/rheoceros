# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from test.intelliflow.core.platform.driver_test_utils import DriverTestUtils
from test.intelliflow.core.signal_processing.routing_runtime_constructs import create_incoming_signal
from unittest.mock import MagicMock

import boto3
import pytest

import intelliflow.core.platform.drivers.compute.aws_emr as compute_driver
from intelliflow.core.platform.constructs import RoutingTable
from intelliflow.core.platform.definitions.aws.common import CommonParams
from intelliflow.core.platform.definitions.aws.emr.client_wrapper import EmrReleaseLabel, build_glue_catalog_configuration
from intelliflow.core.platform.definitions.aws.emr.script.batch.common import (
    APPLICATIONS,
    EMR_CONFIGURATIONS,
    EMR_INSTANCES_SPECS,
    EXECUTION_ID,
    EXTRA_JARS,
    INSTANCE_CONFIG_KEY,
    RUNTIME_CONFIG_KEY,
    SECURITY_CONFIGURATION,
    SPARK_CLI_ARGS,
)
from intelliflow.core.platform.definitions.aws.s3.bucket_wrapper import bucket_exists
from intelliflow.core.platform.definitions.common import ActivationParams
from intelliflow.core.platform.definitions.compute import (
    ComputeExecutionDetails,
    ComputeFailedResponse,
    ComputeFailedSessionState,
    ComputeFailedSessionStateType,
    ComputeResourceDesc,
    ComputeSessionDesc,
    ComputeSessionState,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.platform.development import AWSConfiguration, HostPlatform
from intelliflow.core.platform.drivers.compute.aws_emr import AWSEMRBatchCompute
from intelliflow.core.signal_processing import DimensionSpec, Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.routing_runtime_constructs import RuntimeLinkNode
from intelliflow.core.signal_processing.signal import Signal, SignalDimensionLink, SignalDomainSpec
from intelliflow.core.signal_processing.slot import SlotType
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.algorithm import chunk_iter


class TestAWSEMRBatchCompute(AWSTestBase, DriverTestUtils):
    expected_emr_bucket = "if-awsemr-test123-e-1"

    slot_good = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {
            INSTANCE_CONFIG_KEY: compute_driver.InstanceConfig(25),
            RUNTIME_CONFIG_KEY: compute_driver.RuntimeConfig.GlueVersion_1_0,
            SPARK_CLI_ARGS: ["--conf", "a=b"],
            APPLICATIONS: ["CustomApp"],
            EXTRA_JARS: ["s3://a/a.jar", "s3://b/b.jar"],
            SECURITY_CONFIGURATION: "scname",
            EMR_INSTANCES_SPECS: {"EmrManagedMasterSecurityGroup": "managed-sg"},
            EMR_CONFIGURATIONS: [
                {
                    "Classification": "class1",
                    "Properties": {
                        "prop1": "value1",
                    },
                }
            ],
        },
        None,
    )

    slot_with_incorrect_instance_config = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {"InstanceConfig": "foo"},
        None,
    )

    slot_with_incorrect_runtime_config = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {"RuntimeConfig": "foo"},
        None,
    )

    slot_with_incorrect_extra_jar = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {EXTRA_JARS: "foo"},
        None,
    )

    slot_with_wrong_type_element_extra_jar = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {EXTRA_JARS: [1]},
        None,
    )

    slot_with_too_long_security_config = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {SECURITY_CONFIGURATION: "foo" * 10000},
        None,
    )

    slot_with_wrong_type_security_config = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {SECURITY_CONFIGURATION: {}},
        None,
    )

    slot_with_wrong_type_instances_specs = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {SECURITY_CONFIGURATION: []},
        None,
    )

    slot_with_reserved_key_instances_specs = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {SECURITY_CONFIGURATION: {"KeepJobFlowAliveWhenNoSteps": True}},
        None,
    )

    slot_with_wrong_type_configurations = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {EMR_CONFIGURATIONS: {}},
        None,
    )

    slot_with_wrong_type_applications = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {APPLICATIONS: "adsf"},
        None,
    )

    slot_with_wrong_type_element_applications = Slot(
        SlotType.ASYNC_BATCH_COMPUTE,
        "output = input1",
        Lang.PYTHON,
        ABI.GLUE_EMBEDDED,
        {APPLICATIONS: [{}]},
        None,
    )

    def setup_platform_and_params(self):
        self.params = {}
        self.init_common_utils()
        self.params[CommonParams.BOTO_SESSION] = boto3.Session(None, None, None, self.region)
        self.params[CommonParams.REGION] = self.region
        self.params[CommonParams.ACCOUNT_ID] = self.account_id
        self.params[CommonParams.IF_DEV_ROLE] = "arn:partition:111111111:role/DevRole"
        self.params[CommonParams.IF_EXE_ROLE] = "arn:partition:111111111:role/ExeRole"
        self.params[ActivationParams.UNIQUE_ID_FOR_CONTEXT] = "unique_id"

    def get_driver_and_platform(self):
        mock_compute = AWSEMRBatchCompute(self.params)
        mock_compute._iam = MagicMock()
        return (
            mock_compute,
            HostPlatform(
                AWSConfiguration.builder()
                .with_default_credentials(as_admin=True)
                .with_region("us-east-1")
                .with_batch_compute(AWSEMRBatchCompute)
                .with_param(ActivationParams.UNIQUE_ID_FOR_CONTEXT, "unique_id")
                .build()
            ),
        )

    def test_init_constructor_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        assert mock_compute._emr is not None
        assert mock_compute._ec2 is not None
        assert mock_compute._s3 is not None
        assert mock_compute._bucket is None
        assert mock_compute._bucket_name is None
        assert mock_compute._iam is not None
        assert mock_compute._intelliflow_python_workingset_key is None
        self.patch_aws_stop()

    def test_compute_dev_init_successful_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        self.params[ActivationParams.UNIQUE_ID_FOR_CONTEXT] = "test123_e_1"
        mock_compute.dev_init(mock_host_platform)
        assert mock_compute._bucket_name == self.expected_emr_bucket
        assert mock_compute._intelliflow_python_workingset_key == "batch/bundle.zip"
        self.patch_aws_stop()

    def test_compute_dev_init_exception_max_bucket_len_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        with pytest.raises(Exception) as error:
            context_id = str()
            for c in range(60):
                context_id += str("a")
            self.params[ActivationParams.UNIQUE_ID_FOR_CONTEXT] = context_id
            mock_compute.dev_init(mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_compute_dev_init_exception_max_job_name_len_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        with pytest.raises(Exception) as error:
            context_id = str()
            for c in range(300):
                context_id += str(c)
            self.params[ActivationParams.UNIQUE_ID_FOR_CONTEXT] = context_id
            mock_compute.dev_init(mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_compute_dev_init_exception_job_name_pattern_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        with pytest.raises(Exception) as error:
            self.params[ActivationParams.UNIQUE_ID_FOR_CONTEXT] = "😀"
            mock_compute.dev_init(mock_host_platform)
        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_compute_provide_output_attributes_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        input_signal, test_slot, materialized_output = self.get_signals_slots()
        mock_compute.provide_output_attributes([input_signal], test_slot, dict())

        test_slot.extra_params.update({"partition_by": "boot me"})
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

        self.patch_aws_stop()

    def test_compute_activate_successful_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_e_2"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.activate()
        s3 = boto3.resource("s3")
        assert bucket_exists(s3, mock_compute._bucket_name)
        self.patch_aws_stop()

    def test_compute_provide_runtime_default_policies(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_e_2_1"
        mock_compute.dev_init(mock_host_platform)

        managed_policies_before = mock_compute.provide_runtime_default_policies()

        # mock IAM managed policy check to return True (exists) for any policy
        import intelliflow.core.platform.drivers.compute.aws_emr as emr_compute_driver

        _real_iam_func = emr_compute_driver.has_aws_managed_policy

        def has_aws_managed_policy(*args, **kwargs):
            return True

        emr_compute_driver.has_aws_managed_policy = MagicMock(side_effect=has_aws_managed_policy)

        managed_policies_after = mock_compute.provide_runtime_default_policies()

        assert len(managed_policies_after) - len(managed_policies_before) > 0

        emr_compute_driver.has_aws_managed_policy = _real_iam_func
        self.patch_aws_stop()

    def test_compute_get_session_state_successful_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_e_3"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.activate()
        mock_compute.get_session_state(
            ComputeSessionDesc("cluster_id", ComputeResourceDesc("cluster_id", "arn", driver=AWSEMRBatchCompute.__class__)), None
        )
        self.patch_aws_stop()

    def test_compute_get_session_state_ec2_limit_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_e_4"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.activate()

        def mock_describe_emr_cluster(emr_client, clusterId):
            return {
                "Cluster": {
                    "Id": "j-AAAAAAAAAAAAA",
                    "Name": "IntelliFlow-app-name-AWSEMRBatchCompute-py-glue_embedded-GlueVersion_2_0-id",
                    "Status": {
                        "State": "TERMINATED_WITH_ERRORS",
                        "StateChangeReason": {
                            "Code": "INTERNAL_ERROR",
                            "Message": "The request to create the EMR cluster or add EC2 instances to it failed. The number of vCPUs for instance type m5.xlarge exceeds the EC2 service quota for that type. Request a service quota increase for your AWS account or choose a different instance type and retry the request. For more information, see https://docs.aws.amazon.com/console/elasticmapreduce/vcpu-limit",
                        },
                        "Timeline": {
                            "CreationDateTime": "2022-06-03T01:06:12.067000+00:00",
                            "EndDateTime": "2022-06-03T01:09:48.224000+00:00",
                        },
                    },
                    "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
                    "KerberosAttributes": {},
                    "ClusterArn": "arn:aws:elasticmapreduce:us-east-1:123456789012:cluster/j-AAAAAAAAAAAAA",
                }
            }

        compute_driver.describe_emr_cluster.side_effect = mock_describe_emr_cluster
        session_state = mock_compute.get_session_state(
            ComputeSessionDesc("cluster_id", ComputeResourceDesc("cluster_id", "arn", driver=AWSEMRBatchCompute.__class__)), None
        )
        assert isinstance(session_state, ComputeFailedSessionState)
        assert session_state.failed_type == ComputeFailedSessionStateType.TRANSIENT
        assert compute_driver.get_emr_step.called
        assert session_state.executions[0].details["step_details"]["Status"]["State"] == "COMPLETED"
        self.patch_aws_stop()

    def test_translate_runtime_config_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        assert (
            mock_compute._translate_runtime_config({"RuntimeConfig": compute_driver.RuntimeConfig.GlueVersion_3_0}, [])
            == compute_driver.RuntimeConfig.GlueVersion_3_0
        )

        assert mock_compute._translate_runtime_config({"GlueVersion": "2.0"}, []) == compute_driver.RuntimeConfig.GlueVersion_2_0

        assert mock_compute._translate_runtime_config({"GlueVersion": "4.0"}, []) == compute_driver.RuntimeConfig.GlueVersion_4_0

        self.patch_aws_stop()

    def test_translate_instance_config_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        assert mock_compute._translate_instance_config(
            {"InstanceConfig": compute_driver.InstanceConfig(10)}
        ) == compute_driver.InstanceConfig(10)

        assert mock_compute._translate_instance_config({"NumberOfWorkers": 10, "WorkerType": "G.1X"}) == compute_driver.InstanceConfig(
            10, "m5.xlarge"
        )

        self.patch_aws_stop()

    def test_hook_internal_happy_path_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_e_5"
        mock_compute.dev_init(mock_host_platform)
        test_route = self.route_1_basic
        test_route._slots = [self.slot_good]
        mock_compute.hook_internal(test_route)
        self.patch_aws_stop()

    @pytest.mark.parametrize(
        "test_slot, app_index",
        [
            (slot_with_incorrect_instance_config, 0),
            (slot_with_incorrect_runtime_config, 1),
            (slot_with_incorrect_extra_jar, 2),
            (slot_with_reserved_key_instances_specs, 3),
            (slot_with_wrong_type_instances_specs, 4),
            (slot_with_too_long_security_config, 5),
            (slot_with_wrong_type_configurations, 6),
            (slot_with_wrong_type_security_config, 7),
            (slot_with_wrong_type_element_extra_jar, 8),
            (slot_with_wrong_type_applications, 9),
            (slot_with_wrong_type_element_applications, 10),
        ],
    )
    def test_hook_internal_incompatible_slot_emr(self, test_slot, app_index):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = f"test123_e_s_{app_index}"
        mock_compute.dev_init(mock_host_platform)
        test_route = self.route_1_basic
        test_route._slots = [test_slot]
        with pytest.raises(Exception) as error:
            mock_compute.hook_internal(test_route)
        assert error.typename in ["ValueError", "NotImplementedError"]
        self.patch_aws_stop()

    def test_build_emr_cli_arg_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = app_name = "test123_e_7"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.activate()
        boilerplate = "boilerplate"
        code_key = "codeKey"
        extra_jars = ["a", "b"]
        input_map_key = "inputMapKey"
        output_param_key = "outputKey"
        user_args = ["--conf", "userA", "--test"]
        working_set_object = "s3://working_set/object"
        expected_args = [
            "spark-submit",
            "--deploy-mode",
            "cluster",
            "--jars",
            "a,b",
            "--conf",
            "spark.sql.catalogImplementation=hive",
            "--conf",
            "userA",
            "--test",
            boilerplate,
        ]
        execution_id = "aaaa-aaaa-aaa"

        extra_params = {"key1": "value1", "key2": 2}
        ignored_bundle_modules = ["boto3", "botocore"]
        actual_args = mock_compute._build_emr_cli_arg(
            app_name,
            boilerplate,
            code_key,
            extra_jars,
            input_map_key,
            output_param_key,
            user_args,
            working_set_object,
            extra_params,
            execution_id,
            ignored_bundle_modules,
        )
        assert expected_args == actual_args[: len(expected_args)]
        assert ["--key1", "value1", "--key2", "2"] == actual_args[len(expected_args) : len(expected_args) + 4]
        assert f"--{EXECUTION_ID}" in actual_args
        assert execution_id in actual_args
        self.patch_aws_stop()

    def test_validate_spark_cli_args_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()

        with pytest.raises(Exception) as error:
            mock_compute._validate_spark_cli_args(["invalidValue", "--conf", "a=b"])
        assert error.typename == "ValueError"

        with pytest.raises(Exception) as error:
            mock_compute._validate_spark_cli_args(["--conf", "a=b", "invalidValue"])
        assert error.typename == "ValueError"

        with pytest.raises(Exception) as error:
            mock_compute._validate_spark_cli_args(["--conf", "a=b", "--jars", "a,b"])
        assert error.typename == "ValueError"

        with pytest.raises(Exception) as error:
            mock_compute._validate_spark_cli_args([123])
        assert error.typename == "ValueError"

        with pytest.raises(Exception) as error:
            mock_compute._validate_spark_cli_args({"123"})
        assert error.typename == "ValueError"

        mock_compute._validate_spark_cli_args(None)
        mock_compute._validate_spark_cli_args([])
        mock_compute._validate_spark_cli_args(["--conf", "c=d", "--conf", "a=b"])
        mock_compute._validate_spark_cli_args(["--verbose", "--conf", "a=b"])
        self.patch_aws_stop()

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
            SlotType.ASYNC_BATCH_COMPUTE,
            "output = input1",
            Lang.PYTHON,
            ABI.GLUE_EMBEDDED,
            {"MaxCapacity": 2, "GlueVersion": "1.0", EXTRA_JARS: ["s3://somewhere/a.jar", "s3://swelse/b.jar"]},
            None,
        )

        return input_signal, test_slot, materialized_output

    def test_compute_computefunc_successful_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        self.params[compute_driver.AWSEMRBatchCompute.EMR_CLUSTER_SUBNET_ID] = "subnet-12345789asdfwer"
        self.params[ActivationParams.UNIQUE_ID_FOR_CONTEXT] = "test123_e_8"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.activate()
        input_signal, test_slot, materialized_output = self.get_signals_slots()
        applications = ["Pig", "Custom"]
        test_slot.extra_params[APPLICATIONS] = applications
        security_config = "scname"
        test_slot.extra_params[SECURITY_CONFIGURATION] = security_config
        custom_config = {
            "Classification": "class1",
            "Properties": {
                "prop1": "value1",
            },
        }
        test_slot.extra_params[EMR_CONFIGURATIONS] = [custom_config]

        expected_subnet_id = "sn-3"
        instances_specs = {
            "EmrManagedMasterSecurityGroup": "managed1",
            "EmrManagedSlaveSecurityGroup": "managed2",
            "Ec2SubnetIds": ["sn-1", "sn-2"],
            "Ec2SubnetId": expected_subnet_id,
            "AdditionalMasterSecurityGroups": ["sg-1", "sg-2"],
        }
        test_slot.extra_params[EMR_INSTANCES_SPECS] = instances_specs

        mock_compute.compute(None, [input_signal], test_slot, materialized_output, "test_execution_id")

        (
            _emr,
            actual_job_name,
            actual_emr_release_label,
            actual_log_path,
            actual_applications,
            actual_spark_cli_args,
            actual_if_exe_role,
            actual_configurations,
            actual_instances_specs,
            actual_security_config,
            actual_bootstrap_actions,
        ) = compute_driver.start_emr_job_flow.call_args_list[0][0]
        assert actual_job_name.startswith("IntelliFlow-test123_e_8-AWSEMRBatchCompute-py-glue_embedded-GlueVersion_1_0")
        assert actual_emr_release_label == EmrReleaseLabel.VERSION_5_36_0.aws_label
        assert actual_log_path.startswith("s3://if-awsemr-test123-e-8/emr_logs/batch/jobs/sample_output_data/1/")
        assert set(actual_applications) == {"Hue", "Spark", "Hadoop", "Pig", "Custom"}

        jars_param_index = actual_spark_cli_args.index("--jars")
        jars = actual_spark_cli_args[jars_param_index + 1]
        assert "s3://somewhere/a.jar,s3://swelse/b.jar" in jars
        assert " " not in jars

        assert actual_configurations == [custom_config] + [build_glue_catalog_configuration()]

        assert actual_instances_specs["InstanceGroups"][1] == {
            "InstanceRole": "CORE",
            "InstanceType": "m5.xlarge",
            "InstanceCount": 24,
            "Market": "ON_DEMAND",
        }

        assert actual_instances_specs["Ec2SubnetId"] == expected_subnet_id

        for key in instances_specs.keys():
            assert actual_instances_specs[key] == instances_specs[key]

        assert security_config == actual_security_config

        self.patch_aws_stop()

    def test_compute_app_level_subnet_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        expected_subnet_id = "subnet-12345789asdfwer"
        self.params[compute_driver.AWSEMRBatchCompute.EMR_CLUSTER_SUBNET_ID] = expected_subnet_id

        mock_host_platform._context_id = "test123_e_9"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.activate()
        input_signal, test_slot, materialized_output = self.get_signals_slots()

        mock_compute.compute(None, [input_signal], test_slot, materialized_output, "test_execution_id")

        (
            _emr,
            actual_job_name,
            actual_emr_release_label,
            actual_log_path,
            actual_applications,
            actual_spark_cli_args,
            actual_if_exe_role,
            actual_configurations,
            actual_instances_specs,
            actual_security_config,
            actual_bootstrap_actions,
        ) = compute_driver.start_emr_job_flow.call_args_list[0][0]

        assert actual_instances_specs["Ec2SubnetId"] == expected_subnet_id

        self.patch_aws_stop()

    def test_terminate_successful_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_e_10"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.activate()

        def mock_list_emr_clusters(*args, **kwargs):
            return [
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI3"},
                {"Name": mock_compute._build_job_prefix() + "-xxxxxxxx", "Id": "j-CIUHDBP3OI3"},
            ]

        compute_driver.list_emr_clusters.side_effect = mock_list_emr_clusters

        mock_compute.terminate()

        assert compute_driver.terminate_emr_job_flow.call_args_list[0][0][1] == ["j-CIUHDBP3OI3"]
        assert compute_driver.delete_instance_profile.call_args_list[0][0][1] == "ExeRole"
        assert compute_driver.delete_bucket.call_args_list[0][0][0] is not None

        self.patch_aws_stop()

    def test_terminate_successful_more_cluster_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_e_11"
        mock_compute.dev_init(mock_host_platform)
        mock_compute.activate()

        def mock_list_emr_clusters_more(*args, **kwargs):
            return [
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI0"},
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI1"},
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI2"},
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI3"},
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI4"},
                {"Name": mock_compute._build_job_prefix() + "-xxxxxxxx12", "Id": "j-CIUHDBP3OI1"},
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI5"},
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI6"},
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI7"},
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI8"},
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI9"},
                {"Name": mock_compute._build_job_prefix() + "-xxxxxxxx", "Id": "j-CIUHDBP3OI2"},
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI10"},
                {"Name": "cluster_not_by_this_app", "Id": "j-XLKJDFP3OI11"},
            ] + [{"Name": mock_compute._build_job_prefix() + "-xxxxxxxx", "Id": "j-CIUHDBP3OI2"}] * 10

        compute_driver.list_emr_clusters.side_effect = mock_list_emr_clusters_more

        mock_compute.terminate()

        assert compute_driver.terminate_emr_job_flow.call_args_list[0][0][1] == ["j-CIUHDBP3OI1"] + ["j-CIUHDBP3OI2"] * 9
        assert compute_driver.terminate_emr_job_flow.call_args_list[1][0][1] == ["j-CIUHDBP3OI2"] * 2
        assert compute_driver.delete_instance_profile.call_args_list[0][0][1] == "ExeRole"
        assert compute_driver.delete_bucket.call_args_list[0][0][0] is not None

        self.patch_aws_stop()

    def test_kill_session_happy_path_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_e_12"

        cluster_id = "j-XOKJ234LDO2"

        record = RoutingTable.ComputeRecord(
            None,
            None,
            None,
            None,
            None,
            ComputeSuccessfulResponse(ComputeSuccessfulResponseType.PROCESSING, ComputeSessionDesc(cluster_id, None)),
            None,
        )

        mock_compute.kill_session(record)

        assert compute_driver.terminate_emr_job_flow.call_args_list[0][0][1] == ["j-XOKJ234LDO2"]

        self.patch_aws_stop()

    def test_kill_session_failed_record_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_e_13"

        cluster_id = "j-XOKJ234LDO2"

        record = RoutingTable.ComputeRecord(
            None,
            None,
            None,
            None,
            None,
            ComputeFailedResponse(
                None,
                None,
                None,
                None,
            ),
            None,
        )

        mock_compute.kill_session(record)

        assert not compute_driver.terminate_emr_job_flow.called

        self.patch_aws_stop()

    def test_describe_compute_record_emr(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_compute, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_e_13"
        mock_cluster_response = {"Id": "j-2FH88JIO365F3"}
        active_record = RoutingTable.ComputeRecord(
            None,
            None,
            Slot(SlotType.ASYNC_BATCH_COMPUTE, "mock_code", Lang.PYTHON, ABI.GLUE_EMBEDDED, None, None),
            None,
            None,
            None,
            session_state=ComputeSessionState(None, None, [ComputeExecutionDetails(0, 1, dict(mock_cluster_response))]),
        )

        result = mock_compute.describe_compute_record(active_record)

        assert not compute_driver.get_emr_step.called
        assert result["slot"] == {
            "type": SlotType.ASYNC_BATCH_COMPUTE,
            "lang": Lang.PYTHON,
            "code": "mock_code",
            "code_abi": ABI.GLUE_EMBEDDED,
        }
        assert result["details"]["cluster_details"] == {"Id": "j-2FH88JIO365F3"}

        self.patch_aws_stop()
