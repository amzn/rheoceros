import copy
import sys
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

from intelliflow.api import DataFormat, S3Dataset
from intelliflow.api_ext import *
from intelliflow.core.application.application import Application
from intelliflow.core.application.context.node.internal.nodes import InternalDataNode
from intelliflow.core.application.context.node.marshaling.nodes import MarshalerNode
from intelliflow.core.application.core_application import ApplicationState
from intelliflow.core.platform.constructs import BatchCompute as BatchComputeDriver
from intelliflow.core.platform.constructs import CompositeBatchCompute, RoutingTable
from intelliflow.core.platform.definitions.aws.batch.common import AWS_BATCH_ORCHESTRATOR_ROLE_ARN_PARAM
from intelliflow.core.platform.definitions.aws.common import IF_DEV_ROLE_NAME_FORMAT, exponential_retry
from intelliflow.core.platform.definitions.aws.emr.client_wrapper import create_job_flow_instance_profile
from intelliflow.core.platform.definitions.compute import (
    ComputeFailedResponseType,
    ComputeResourceDesc,
    ComputeSessionDesc,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.platform.development import AWSConfiguration, HostPlatform
from intelliflow.core.platform.drivers.compute.aws import AWSGlueBatchComputeBasic
from intelliflow.core.platform.drivers.compute.aws_batch import AWSBatchCompute
from intelliflow.core.platform.drivers.compute.aws_emr import AWSEMRBatchCompute
from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.dimension_constructs import DimensionVariantFactory
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSApplicationBatchJob(AWSTestBase):
    @classmethod
    def add_external_data(
        cls, app, id: str, bucket: str, header: bool = False, schema: bool = False, protocol: bool = False, dataset_format=DataFormat.CSV
    ) -> MarshalerNode:
        return app.marshal_external_data(
            S3Dataset("111222333444", bucket, "batch_job_input", "{}", "{}", dataset_format=dataset_format, header=header, schema=schema),
            id,
            dimension_spec={"region": {type: DimensionType.LONG, "day": {type: DimensionType.DATETIME}}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}) if protocol else None,
        )

    def create_compute_env_resources(self, app) -> Tuple[str, List[str], str]:
        # - instance role
        iam = app.platform.processor.session.client("iam", region_name=self.region)
        if_exe_role_name = IF_DEV_ROLE_NAME_FORMAT.format(app.id, self.region)
        instance_profile_name = AWSBatchCompute._build_instance_profile_name(if_exe_role_name)
        create_job_flow_instance_profile(iam, if_exe_role_name, instance_profile_name)
        instance_profile_arn = f"arn:aws:iam::{self.account_id}:instance-profile/{instance_profile_name}"

        # - security group and subnet IDs (use the default VPC)
        ec2 = app.platform.processor.session.client("ec2", region_name=self.region)
        vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        vpc_id = vpcs["Vpcs"][0]["VpcId"]
        params = {
            "Filters": [
                {"Name": "vpc-id", "Values": [vpc_id]},
            ]
        }
        subnets = ec2.describe_subnets(**params)
        subnet_list = [subnet["SubnetId"] for subnet in subnets["Subnets"]]

        security_group_id = ec2.create_security_group(
            GroupName="sg-abcdefghjklabcd11",
            Description="test security group",
        )["GroupId"]

        return instance_profile_arn, subnet_list, security_group_id

    def test_application_init_with_batch_compute_unmanaged(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        # check default AWS configuration and make sure that AWS Batch based drivers are offered
        supported_aws_constructs = AWSConfiguration.get_supported_constructs_map()
        assert AWSBatchCompute in supported_aws_constructs[BatchComputeDriver]
        app = AWSApplication("batch_job", self.region, **{CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM: [AWSBatchCompute]})

        with pytest.raises(ValueError):
            app.create_data(
                id="BATCH_JOB_NODE_WITHOUT_INPUT",
                compute_targets=[
                    AWSBatchJob(
                        containerOverrides={
                            "command": int,
                        },
                        jobDefinition="<jobDefinitionName>",
                        jobQueue="<jobQueueName>",
                    )
                ],
            )

        with pytest.raises(ValueError):
            app.create_data(
                id="BATCH_JOB_NODE_WITHOUT_INPUT",
                compute_targets=[
                    AWSBatchJob(
                        containerOverrides={"command": {}},
                        jobDefinition="<jobDefinitionName>",
                        jobQueue="<jobQueueName>",
                    )
                ],
            )

        # fail due to missing jobDefinition
        with pytest.raises(TypeError):
            app.create_data(
                id="BATCH_JOB_NODE_WITHOUT_INPUT",
                compute_targets=[
                    AWSBatchJob(
                        containerOverrides={
                            "command": ["--arg1", "arg1"],
                        },
                        jobQueue="<jobQueueName>",
                    )
                ],
            )

        # fail due to missing jobQueue
        with pytest.raises(TypeError):
            app.create_data(
                id="BATCH_JOB_NODE_WITHOUT_INPUT",
                compute_targets=[
                    AWSBatchJob(
                        containerOverrides={
                            "command": ["--arg1", "arg1"],
                        },
                        jobDefinition="<jobDefinitionName>",
                    )
                ],
            )

        # wrong type on jobDefinition
        with pytest.raises(ValueError):
            node_1 = app.create_data(
                id="BATCH_JOB_NODE_WITHOUT_INPUT",
                compute_targets=[
                    AWSBatchJob(
                        containerOverrides={
                            "command": ["--arg1", "arg1"],
                        },
                        jobDefinition=[],
                        jobQueue="<jobQueueName>",
                    )
                ],
            )

        # wrong type on jobQueue
        with pytest.raises(ValueError):
            node_1 = app.create_data(
                id="BATCH_JOB_NODE_WITHOUT_INPUT",
                compute_targets=[
                    AWSBatchJob(
                        containerOverrides={
                            "command": ["--arg1", "arg1"],
                        },
                        jobDefinition="<jobDefinitionName>",
                        jobQueue=[],
                    )
                ],
            )

        node_1 = app.create_data(
            id="BATCH_JOB_NODE_WITHOUT_INPUT",
            compute_targets=[
                AWSBatchJob(
                    containerOverrides={
                        "command": ["--arg1", "arg1"],
                    },
                    jobDefinition="<jobDefinitionName>",
                    jobQueue="<jobQueueName>",
                )
            ],
        )

        input = self.add_external_data(app, "bucket", "ext_training_data_with_header", header=True)

        node_2 = app.create_data(
            id="BATCH_JOB_NODE_WITH_INPUT",
            inputs=[input],
            compute_targets=[
                AWSBatchJob(
                    "jobDefinitionName_1",
                    "jobQueueName_1",
                    containerOverrides={
                        # relies on template rendering at runtime
                        "command": ["--i", "${input0}", "-o", "${output}"],
                    },
                )
            ],
        )

        node_3 = app.create_data(
            id="BATCH_JOB_NODE_IN_ANOTHER_ACCOUNT",
            inputs=[input],
            compute_targets=[
                AWSBatchJob(
                    containerOverrides={
                        # relies on template rendering at runtime
                        "command": ["--i", "${input0}", "--o", "${output}", "--r", "${region}"],
                    },
                    jobDefinition="jobDefinitionNameOrArn_2",
                    jobQueue="jobQueueNameOrArn_2",
                    **{AWS_BATCH_ORCHESTRATOR_ROLE_ARN_PARAM: f"arn:aws:iam::123456789012:role/TEST-ORCHESTRATOR-ROLE"},
                )
            ],
        )

        # add this one for compute failure due to misconfigured param
        node_4_with_bad_command_arg = app.create_data(
            id="BATCH_JOB_EXEC_WILL_FAIL_DUE_TO_BAD_COMMAND",
            inputs=[input],
            compute_targets=[
                AWSBatchJob(
                    containerOverrides={
                        # relies on template rendering at runtime
                        "command": ["--i", "${input0}", "--o", "${output}", "--r", "${NON_EXISTENT_PARAM_SCOPE}"],
                    },
                    jobDefinition="jobDefinitionNameOrArn_4",
                    jobQueue="jobQueueNameOrArn_4",
                )
            ],
        )

        # should fail due to non-existent unmanaged jobDefinition and jobQueues
        with pytest.raises(ValueError):
            app.activate()

        # hook AWS Batch APIs to test runtime parametrization and existence of unmanaged resources
        import intelliflow.core.platform.drivers.compute.aws_batch as aws_batch_driver

        # mock the existence of resources
        def describe_compute_environments(batch_client, retryable_exceptions, **api_params):
            # return the api_params within the "response"
            return {"computeEnvironments": [api_params]}

        def describe_job_queues(batch_client, retryable_exceptions, **api_params):
            # return the api_params within the "response"
            return {"jobQueues": [api_params]}

        def describe_job_definitions(batch_client, retryable_exceptions, **api_params):
            """wrapped for better testability"""
            # return the api_params within the "response"
            return {"jobDefinitions": [api_params]}

        real_describe_compute_environments = aws_batch_driver.describe_compute_environments
        real_describe_job_queues = aws_batch_driver.describe_job_queues
        real_describe_job_definitions = aws_batch_driver.describe_job_definitions
        aws_batch_driver.describe_compute_environments = MagicMock(side_effect=describe_compute_environments)
        aws_batch_driver.describe_job_queues = MagicMock(side_effect=describe_job_queues)
        aws_batch_driver.describe_job_definitions = MagicMock(side_effect=describe_job_definitions)

        # should succeed now!
        app.activate()

        # test execution but hook submit API for parameter rendering
        def submit_job(batch_client, retryable_exceptions, api_params):
            # test parametrization
            command = api_params["containerOverrides"]["command"]
            assert command
            for i, cmd in enumerate(command):
                if cmd in ["--i", "--o"]:
                    assert command[i + 1].startswith("s3://")
                elif cmd == "--r":
                    assert command[i + 1] == "1"

            success_response = {
                "jobId": str(uuid.uuid1()),
                "jobName": api_params["jobName"],
                "ResponseMetadata": {},
            }
            return success_response

        def describe_jobs(batch_client, retryable_exceptions, **api_params):
            return {
                "jobs": [
                    {
                        "jobArn": "JOB_ARN",
                        "jobName": "JOB_NAME",
                        "status": "SUCCEEDED",
                        "statusReason": "reason",
                        "createdAt": 1490626709525,
                        "startedAt": 1490627034798,
                        "stoppedAt": 1490627034949,
                        "jobQueue": "",
                        "jobDefinition": "",
                    }
                ]
            }

        real_submit_job = aws_batch_driver.submit_job
        real_describe_jobs = aws_batch_driver.describe_jobs
        aws_batch_driver.submit_job = MagicMock(side_effect=submit_job)
        aws_batch_driver.describe_jobs = MagicMock(side_effect=describe_jobs)

        app.execute(node_1, wait=True)
        app.execute(node_2[1]["2023-03-29"], wait=False)
        app.execute(node_3[1]["2023-03-29"], wait=True)
        with pytest.raises(RuntimeError):
            app.execute(node_4_with_bad_command_arg[1]["2023-03-29"], wait=True)

        # test failure
        def describe_jobs(batch_client, retryable_exceptions, **api_params):
            return {
                "jobs": [
                    {
                        "jobArn": "JOB_ARN",
                        "jobName": "JOB_NAME",
                        "status": "FAILED",
                        "statusReason": "Essential container in task exited",
                        "createdAt": 1490626709525,
                        "startedAt": 1490627034798,
                        "stoppedAt": 1490627034949,
                        "jobQueue": "",
                        "jobDefinition": "",
                    }
                ]
            }

        aws_batch_driver.describe_jobs = MagicMock(side_effect=describe_jobs)

        # activate local orchestrator loop, because it will be required for the execution tests below
        self.activate_event_propagation(app, cycle_time_in_secs=3)

        # previously successful execution should fail now due to failure indicating describe_jobs response
        with pytest.raises(RuntimeError):
            app.execute(node_3[1]["2023-03-29"], wait=True)

        # test long running job due to TRANSIENT error at the same time
        def describe_jobs(batch_client, retryable_exceptions, **api_params):
            return {
                "jobs": [
                    {
                        "jobArn": "JOB_ARN",
                        "jobName": "JOB_NAME",
                        "status": "FAILED",
                        # see https://aws.amazon.com/blogs/compute/introducing-retry-strategies-for-aws-batch/
                        # this is the only auto-handled transient case as of 04/02/2024 supported by the framework
                        "statusReason": "Host EC2 could not be allocated",
                        "createdAt": 1490626709525,
                        "startedAt": 1490627034798,
                        "stoppedAt": 1490627034949,
                        "jobQueue": "",
                        "jobDefinition": "",
                    }
                ]
            }

        aws_batch_driver.describe_jobs = MagicMock(side_effect=describe_jobs)
        # exit without waiting
        assert app.execute(node_3[1]["2023-03-29"], wait=False).startswith("s3://")
        # now check the status of the execution and see it is still active
        active_records = list(app.get_active_compute_records(node_3))
        assert len(active_records) == 1
        assert active_records[0].session_state
        assert active_records[0].session_state.state_type == ComputeSessionStateType.FAILED
        assert active_records[0].session_state.failed_type == ComputeFailedResponseType.TRANSIENT

        aws_batch_driver.submit_job = real_submit_job
        aws_batch_driver.describe_jobs = real_describe_jobs
        aws_batch_driver.describe_compute_environments = real_describe_compute_environments
        aws_batch_driver.describe_job_queues = real_describe_job_queues
        aws_batch_driver.describe_job_definitions = real_describe_job_definitions

        app.terminate()

        self.patch_aws_stop()

    def test_application_init_with_batch_compute_managed(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        app_id = "batch_job_mngd"
        app = AWSApplication(
            app_id,
            self.region,
            **{CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM: [AWSGlueBatchComputeBasic, AWSEMRBatchCompute, AWSBatchCompute]},
        )

        input = self.add_external_data(app, "bucket", "ext_training_data_with_header", header=True)
        input_2 = self.add_external_data(app, "bucket2", "batch_job_input", header=True)

        compute_env_name = "compute-env-1"
        # create compute env first, normally we should wait for its status to be VALID but in moto we should not expect
        # a problem when job queue is being created.
        instance_profile_arn, subnet_list, security_group_id = self.create_compute_env_resources(app)

        # - compute env
        response = app.platform.processor.session.client("batch", region_name=self.region).create_compute_environment(
            **{
                "computeEnvironmentName": compute_env_name,
                "type": "MANAGED",
                "state": "ENABLED",
                "computeResources": {
                    "type": "EC2",
                    "allocationStrategy": "BEST_FIT_PROGRESSIVE",
                    "minvCpus": 4,
                    "maxvCpus": 4096,
                    "desiredvCpus": 4,
                    "instanceTypes": ["g4dn.xlarge", "g4dn.2xlarge", "g5.xlarge", "g5.2xlarge", "p3.2xlarge"],
                    "subnets": subnet_list,
                    "securityGroupIds": [security_group_id],
                    "instanceRole": instance_profile_arn,
                    "ec2Configuration": [{"imageType": "ECS_AL2_NVIDIA"}],
                },
                # "serviceRole": f"arn:aws:iam::{self.account_id}:role/aws-service-role/batch.amazonaws.com/AWSServiceRoleForBatch",
                # Moto requires this (Batch would default to AWSServiceRoleForBatch if left undefined)
                "serviceRole": app.platform.conf.get_param(AWSCommonParams.IF_DEV_ROLE),
            }
        )
        compute_env_arn = response["computeEnvironmentArn"]

        # node with job definition and queue without managed compute env (envs passed as string)
        node_1 = app.create_data(
            id="MANAGED_NODE_1",
            inputs={"input1_alias": input, "input2_alias": input_2},
            compute_targets=[
                AWSBatchJob(
                    containerOverrides={
                        # relies on template rendering at runtime
                        "command": ["--input1", "${input1_alias}", "--input2", "${input1}", "--o", "${output}", "--r", "${region}"],
                    },
                    jobDefinition={
                        "jobDefinitionName": "product-dna-gpu",
                        "type": "container",
                        # "parameters": {},
                        # User provided retry (will be applied even before IF orchestration takes action)
                        "retryStrategy": {"attempts": 1, "evaluateOnExit": [{"onExitCode": "*", "action": "retry"}]},
                        "containerProperties": {
                            "image": "808832487431.dkr.ecr.us-east-1.amazonaws.com/product-dna:scientist-dev",
                            "command": [
                                "/opt/conda/bin/python",
                                "/app/src/product_dna/scripts/embed_v2.py",
                                "--i",
                                "${input0}",
                                "--o",
                                "${output}",
                            ],
                            # LET Framework use the EXEC role for these
                            # "jobRoleArn": f"arn:aws:iam::{self.account_id}:role/PDNACoreExecutorStack-bet-ProductDNACoreProductDNAD-QyubCJ5jGoot",
                            # "executionRoleArn": f"arn:aws:iam::{self.account_id}:role/PDNACoreExecutorStack-bet-ProductDNACoreProductDNAD-QyubCJ5jGoot",
                            "volumes": [],
                            "environment": [],
                            "mountPoints": [],
                            "readonlyRootFilesystem": False,
                            "ulimits": [],
                            "resourceRequirements": [
                                {"value": "61440", "type": "MEMORY"},
                                {"value": "8", "type": "VCPU"},
                                {"value": "1", "type": "GPU"},
                            ],
                            "linuxParameters": {"devices": [], "sharedMemorySize": 1024, "tmpfs": []},
                            "secrets": [],
                        },
                        "tags": {},
                        "propagateTags": False,
                        "platformCapabilities": ["EC2"],
                    },
                    jobQueue={
                        "jobQueueName": "product-dna-gpu",
                        "state": "ENABLED",
                        "priority": 1,
                        "computeEnvironmentOrder": [
                            {
                                "order": 1,
                                "computeEnvironment": compute_env_arn,  # f"arn:aws:batch:{self.region}:{self.account_id}:compute-environment/{compute_env_name}"
                            },
                        ],
                        "tags": {"string": "string"},
                    },
                )
            ],
        )

        assert app.execute(node_1[1]["2023-03-29"], wait=False)

        app.terminate()

        self.patch_aws_stop()

    def test_application_init_with_batch_compute_managed_conflicting_definition(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        app = AWSApplication(
            "batch_job_mngd2",
            self.region,
            **{CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM: [AWSGlueBatchComputeBasic, AWSEMRBatchCompute, AWSBatchCompute]},
        )

        input = self.add_external_data(app, "bucket", "ext_training_data_with_header", header=True)
        input_2 = self.add_external_data(app, "bucket2", "batch_job_input", header=True)

        node_1 = app.create_data(
            id="MANAGED_NODE_1",
            inputs={"input1_alias": input, "input2_alias": input_2},
            compute_targets=[
                AWSBatchJob(
                    containerOverrides={
                        # relies on template rendering at runtime
                        "command": ["--input1", "${input1_alias}", "--input2", "${input1}", "--o", "${output}", "--r", "${region}"],
                    },
                    jobDefinition={
                        "jobDefinitionName": "product-dna-gpu",
                        "type": "container",
                        # "parameters": {},
                        "containerProperties": {
                            "image": "808832487431.dkr.ecr.us-east-1.amazonaws.com/product-dna:scientist-dev",
                            "command": [
                                "/opt/conda/bin/python",
                                "/app/src/product_dna/scripts/embed_v2.py",
                                "--i",
                                "${$input0}",
                                "--o",
                                "${output}",
                            ],
                            "resourceRequirements": [
                                {"value": "61440", "type": "MEMORY"},
                                {"value": "8", "type": "VCPU"},
                                {"value": "1", "type": "GPU"},
                            ],
                        },
                    },
                    jobQueue="jobQueueStr",
                )
            ],
        )

        # same job definition with conflicting data must raise during activation
        # make "image" parameter different!
        node_1 = app.create_data(
            id="MANAGED_NODE_2",
            inputs={"input1_alias": input, "input2_alias": input_2},
            compute_targets=[
                AWSBatchJob(
                    containerOverrides={
                        # relies on template rendering at runtime
                        "command": ["--input1", "${input1_alias}", "--input2", "${input1}", "--o", "${output}", "--r", "${region}"],
                    },
                    jobDefinition={
                        "jobDefinitionName": "product-dna-gpu",
                        "type": "container",
                        # "parameters": {},
                        "containerProperties": {
                            "image": "808832487431.dkr.ecr.us-east-1.amazonaws.com/product-dna:scientist-dev-DIFFERENT-IMAGE",
                            "command": [
                                "/opt/conda/bin/python",
                                "/app/src/product_dna/scripts/embed_v2.py",
                                "--i",
                                "${$input0}",
                                "--o",
                                "${output}",
                            ],
                            "resourceRequirements": [
                                {"value": "61440", "type": "MEMORY"},
                                {"value": "8", "type": "VCPU"},
                                {"value": "1", "type": "GPU"},
                            ],
                        },
                    },
                    jobQueue="jobQueueStr",
                )
            ],
        )

        # raises due to different dictionaries defined for the same job definition (using the same job definition name)
        with pytest.raises(ValueError):
            app.activate()

        # reload the app. revert the image and do the same on jobQueue
        app = AWSApplication(
            "batch_job_mngd2",
            self.region,
            **{CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM: [AWSGlueBatchComputeBasic, AWSEMRBatchCompute, AWSBatchCompute]},
        )

        input = self.add_external_data(app, "bucket", "ext_training_data_with_header", header=True)
        input_2 = self.add_external_data(app, "bucket2", "batch_job_input", header=True)

        node_1 = app.create_data(
            id="MANAGED_NODE_1",
            inputs={"input1_alias": input, "input2_alias": input_2},
            compute_targets=[
                AWSBatchJob(
                    containerOverrides={
                        # relies on template rendering at runtime
                        "command": ["--input1", "${input1_alias}", "--input1", "${input1}", "--o", "${output}", "--r", "${region}"],
                    },
                    jobDefinition="jobDefinition",  # unmanaged
                    jobQueue={
                        "jobQueueName": "product-dna-gpu",
                        "state": "ENABLED",
                        "priority": 1,
                        "computeEnvironmentOrder": [
                            {
                                "order": 1,
                                "computeEnvironment": f"arn:aws:batch:{self.region}:{self.account_id}:compute-environment/compute_env_1",
                            },
                            {
                                "order": 2,
                                "computeEnvironment": f"arn:aws:batch:{self.region}:{self.account_id}:compute-environment/compute_env_2",
                            },
                        ],
                    },
                )
            ],
        )

        # same job queue with conflicting data must raise during activation
        # make the first "computeEnvironment" sub-parameter different!
        node_1 = app.create_data(
            id="MANAGED_NODE_2",
            inputs={"input1_alias": input, "input2_alias": input_2},
            compute_targets=[
                AWSBatchJob(
                    containerOverrides={
                        # relies on template rendering at runtime
                        "command": ["--input1", "${input1_alias}", "--input2", "${input1}", "--o", "${output}", "--r", "${region}"],
                    },
                    jobDefinition="jobDefinition",
                    jobQueue={
                        "jobQueueName": "product-dna-gpu",
                        "state": "ENABLED",
                        "priority": 1,
                        "computeEnvironmentOrder": [
                            {
                                "order": 1,
                                "computeEnvironment": f"arn:aws:batch:{self.region}:{self.account_id}:compute-environment/compute_env_DIFFERENT",
                            },
                            {
                                "order": 2,
                                "computeEnvironment": f"arn:aws:batch:{self.region}:{self.account_id}:compute-environment/compute_env_2",
                            },
                        ],
                    },
                )
            ],
        )

        # raises due to different dictionaries defined for the same job queue (using the same job queue name)
        with pytest.raises(ValueError):
            app.activate()

        self.patch_aws_stop()

    def test_application_init_with_batch_compute_managed_changeset(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        app = AWSApplication(
            "batch_job_mngd3",
            self.region,
            **{CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM: [AWSGlueBatchComputeBasic, AWSEMRBatchCompute, AWSBatchCompute]},
        )

        input = self.add_external_data(app, "bucket", "ext_training_data_with_header", header=True)
        input_2 = self.add_external_data(app, "bucket2", "batch_job_input", header=True)

        jobDefinition = {
            "jobDefinitionName": "product-dna-gpu",
            "type": "container",
            # "parameters": {},
            "containerProperties": {
                "image": "808832487431.dkr.ecr.us-east-1.amazonaws.com/product-dna:scientist-dev",
                "command": [
                    "/opt/conda/bin/python",
                    "/app/src/product_dna/scripts/embed_v2.py",
                    "--i",
                    "${$input0}",
                    "--o",
                    "${output}",
                ],
                "resourceRequirements": [
                    {"value": "61440", "type": "MEMORY"},
                    {"value": "8", "type": "VCPU"},
                    {"value": "1", "type": "GPU"},
                ],
                "linuxParameters": {"devices": [], "sharedMemorySize": 1024, "tmpfs": []},
            },
        }

        instance_profile_arn, subnet_list, security_group_id = self.create_compute_env_resources(app)

        jobQueue = {
            "jobQueueName": "product-dna-gpu",
            "state": "ENABLED",
            "priority": 1,
            "computeEnvironmentOrder": [
                {
                    "order": 1,
                    # IF managed compute env 1
                    "computeEnvironment": {
                        "computeEnvironmentName": "product-dna-gpu-1",
                        "type": "MANAGED",
                        "state": "ENABLED",
                        "computeResources": {
                            "type": "EC2",
                            "allocationStrategy": "BEST_FIT_PROGRESSIVE",
                            "minvCpus": 4,
                            "maxvCpus": 4096,
                            "desiredvCpus": 4,
                            "instanceTypes": ["g4dn.xlarge", "g4dn.2xlarge", "g5.xlarge", "g5.2xlarge", "p3.2xlarge"],
                            "subnets": subnet_list,
                            "securityGroupIds": [security_group_id],
                            # IF should do the same automatically (moto checks this role)
                            # "instanceRole": instance_profile_arn,
                            "ec2Configuration": [{"imageType": "ECS_AL2_NVIDIA"}],
                        },
                        # let AWS Batch use AWSServiceRoleForBatch implicitly
                        # "serviceRole": f"arn:aws:iam::{self.account_id}:role/aws-service-role/batch.amazonaws.com/AWSServiceRoleForBatch",
                        # MOTO complains so let's set it
                        "serviceRole": app.platform.conf.get_param(AWSCommonParams.IF_DEV_ROLE),
                        "updatePolicy": {"terminateJobsOnUpdate": False, "jobExecutionTimeoutMinutes": 30},
                    },
                },
                # second IF managed compute env
                {
                    "order": 2,
                    "computeEnvironment": {
                        "computeEnvironmentName": "product-dna-gpu-2",
                        "type": "MANAGED",
                        "state": "ENABLED",
                        "computeResources": {
                            "type": "EC2",
                            "allocationStrategy": "BEST_FIT_PROGRESSIVE",
                            "minvCpus": 8,
                            "maxvCpus": 2048,
                            "desiredvCpus": 8,
                            "instanceTypes": ["g4dn.xlarge", "g4dn.2xlarge", "g5.xlarge", "g5.2xlarge", "p3.2xlarge"],
                            "subnets": subnet_list,
                            "securityGroupIds": [security_group_id],
                            # "instanceRole": "<LET IF USE EXEC ROLE>",
                            "ec2Configuration": [{"imageType": "ECS_AL2_NVIDIA"}],
                        },
                        # normally not required. make moto happy due to missing AWSServiceRoleForBatch
                        "serviceRole": app.platform.conf.get_param(AWSCommonParams.IF_DEV_ROLE),
                    },
                },
            ],
        }

        def _add_node_1():
            return app.create_data(
                id="MANAGED_NODE_1",
                inputs={"input1_alias": input, "input2_alias": input_2},
                compute_targets=[
                    AWSBatchJob(
                        containerOverrides={
                            # relies on template rendering at runtime
                            "command": ["--input1", "${input1_alias}", "--input2", "${input1}", "--o", "${output}", "--r", "${region}"],
                        },
                        jobDefinition=jobDefinition,
                        jobQueue=jobQueue,
                    )
                ],
            )

        _add_node_1()

        # reuse the same definitions
        def _add_node_1_1():
            return app.create_data(
                id="MANAGED_NODE_1_1",
                inputs={"input1_alias": input, "input2_alias": input_2},
                compute_targets=[
                    AWSBatchJob(
                        containerOverrides={
                            # relies on template rendering at runtime
                            "command": ["--input1", "${input1_alias}", "--input2", "${input1}", "--o", "${output}", "--r", "${region}"],
                        },
                        jobDefinition=jobDefinition,
                        jobQueue=jobQueue,
                    )
                ],
            )

        _add_node_1_1()

        # another job definition using the same values on other parameters
        jobDefinition2 = dict(jobDefinition)
        jobDefinition2.update({"jobDefinitionName": "product-dna-gpu-2"})
        # another queue using the same compute envs
        jobQueue2 = dict(jobQueue)
        jobQueue2.update({"jobQueueName": "product-dna-gpu-2"})

        def _add_node_2():
            return app.create_data(
                id="MANAGED_NODE_2",
                inputs={"input1_alias": input, "input2_alias": input_2},
                compute_targets=[
                    AWSBatchJob(
                        containerOverrides={
                            # relies on template rendering at runtime
                            "command": ["--input1", "${input1_alias}", "--input2", "${input1}", "--o", "${output}", "--r", "${region}"],
                        },
                        jobDefinition=jobDefinition2,
                        jobQueue=jobQueue2,
                    )
                ],
            )

        _add_node_2()

        jobDefinition3 = {
            "jobDefinitionName": "product-dna-gpu-3",
            "type": "container",
            "containerProperties": {
                "image": "808832487431.dkr.ecr.us-east-1.amazonaws.com/product-dna:scientist-dev",
                "command": [
                    "/opt/conda/bin/python",
                    "/app/src/product_dna/scripts/embed_v2.py",
                ],
                "resourceRequirements": [
                    {"value": "61440", "type": "MEMORY"},
                    {"value": "8", "type": "VCPU"},
                    {"value": "1", "type": "GPU"},
                ],
            },
        }

        jobQueue3 = {
            "jobQueueName": "product-dna-gpu-3",
            "state": "ENABLED",
            "priority": 1,
            "computeEnvironmentOrder": [
                {
                    "order": 1,
                    # IF managed compute env 3
                    "computeEnvironment": {
                        "computeEnvironmentName": "product-dna-gpu-3",
                        "type": "MANAGED",
                        "state": "ENABLED",
                        "computeResources": {
                            "type": "EC2",
                            "allocationStrategy": "BEST_FIT_PROGRESSIVE",
                            "minvCpus": 4,
                            "maxvCpus": 4096,
                            "desiredvCpus": 4,
                            "instanceTypes": [
                                "g4dn.xlarge",
                                "g4dn.2xlarge",
                            ],
                            "subnets": subnet_list,
                            "securityGroupIds": [security_group_id],
                            "ec2Configuration": [{"imageType": "ECS_AL2_NVIDIA"}],
                        },
                        "serviceRole": app.platform.conf.get_param(AWSCommonParams.IF_DEV_ROLE),
                    },
                },
            ],
        }

        node_3 = app.create_data(
            id="MANAGED_NODE_3",
            inputs={"input1_alias": input, "input2_alias": input_2},
            compute_targets=[
                AWSBatchJob(
                    containerOverrides={
                        # relies on template rendering at runtime
                        "command": ["--input1", "${input1_alias}", "--input2", "${input1}", "--o", "${output}", "--r", "${region}"],
                    },
                    jobDefinition=jobDefinition3,
                    jobQueue=jobQueue3,
                )
            ],
        )

        # should succeed
        app.activate()

        # now reload the app and change the topology by removing node3
        app = AWSApplication(
            "batch_job_mngd3",
            self.region,
            **{CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM: [AWSGlueBatchComputeBasic, AWSEMRBatchCompute, AWSBatchCompute]},
        )
        input = self.add_external_data(app, "bucket", "ext_training_data_with_header", header=True)
        input_2 = self.add_external_data(app, "bucket2", "batch_job_input", header=True)

        _add_node_1()
        _add_node_1_1()
        _add_node_2()

        # auto-delete managed resources declared for MANAGED_NODE_3 (unique jodef3, jobqueue3 and compute envs)
        app.activate()

        # now reload the app and change the topology by removing node3
        app = AWSApplication(
            "batch_job_mngd3",
            self.region,
            **{CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM: [AWSGlueBatchComputeBasic, AWSEMRBatchCompute, AWSBatchCompute]},
        )
        input = self.add_external_data(app, "bucket", "ext_training_data_with_header", header=True)
        input_2 = self.add_external_data(app, "bucket2", "batch_job_input", header=True)

        _add_node_1()

        app.activate()

        # now reload the app do complete clean-up (via empty topology activation)
        app = AWSApplication(
            "batch_job_mngd3",
            self.region,
            **{CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM: [AWSGlueBatchComputeBasic, AWSEMRBatchCompute, AWSBatchCompute]},
        )
        app.activate()

        app.terminate()

        self.patch_aws_stop()
