# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
from intelliflow.core.platform.development import AWSConfiguration, HostPlatform
from intelliflow.core.platform.drivers.compute.aws import AWSGlueBatchComputeBasic
from intelliflow.core.platform.drivers.compute.aws_emr import AWSEMRBatchCompute
from intelliflow.core.platform.drivers.compute.aws_sfn import AWSSFNCompute
from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.dimension_constructs import DimensionVariantFactory
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSApplicationSFNStateMachine(AWSTestBase):
    @classmethod
    def add_external_data(
        cls, app, id: str, bucket: str, header: bool = False, schema: bool = False, protocol: bool = False, dataset_format=DataFormat.CSV
    ) -> MarshalerNode:
        return app.marshal_external_data(
            S3Dataset(
                "111222333444",
                bucket,
                "sfn_job_input",
                "{}",
                "{}",
                dataset_format=dataset_format,
                header=header,
                schema=schema,
                data_folder="actual_dataset",
            ),
            id,
            dimension_spec={"region": {type: DimensionType.LONG, "day": {type: DimensionType.DATETIME}}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}) if protocol else None,
        )

    def test_application_init_with_sfn_compute_basic(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        # check default AWS configuration and make sure that AWS Batch based drivers are offered
        supported_aws_constructs = AWSConfiguration.get_supported_constructs_map()
        assert AWSSFNCompute in supported_aws_constructs[BatchComputeDriver]
        app = AWSApplication("sfn_sm", self.region, **{CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM: [AWSSFNCompute]})

        # check basic validations
        with pytest.raises(ValueError):
            app.create_data(
                id="SM_NODE_0",
                compute_targets=[AWSSSFNStateMachine(None)],
            )

        with pytest.raises(ValueError):
            app.create_data(
                id="SM_NODE_0",
                compute_targets=[AWSSSFNStateMachine(definition=1)],
            )

        # cannot define publish
        with pytest.raises(ValueError):
            app.create_data(
                id="SM_NODE_0",
                compute_targets=[
                    AWSSSFNStateMachine(
                        definition='{"StartAt": "FirstState", "States": {"FirstState": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:FUNCTION_NAME", "End": true}}}',
                        publish=False,
                    )
                ],
            )

        # cannot define name
        with pytest.raises(ValueError):
            app.create_data(
                id="SM_NODE_0",
                compute_targets=[
                    AWSSSFNStateMachine(
                        definition='{"StartAt": "FirstState", "States": {"FirstState": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:FUNCTION_NAME", "End": true}}}',
                        name="dex-machina",
                    )
                ],
            )

        # cannot define publish
        with pytest.raises(ValueError):
            app.create_data(
                id="SM_NODE_0",
                compute_targets=[
                    AWSSSFNStateMachine(
                        definition='{"StartAt": "FirstState", "States": {"FirstState": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:FUNCTION_NAME", "End": true}}}',
                        publish=False,
                    )
                ],
            )

        sm_node_1 = app.create_data(
            id="SM_NODE_1",
            compute_targets=[
                AWSSSFNStateMachine(
                    definition={
                        "StartAt": "FirstState",
                        "States": {
                            "FirstState": {
                                "Type": "Task",
                                "Resource": "arn:aws:lambda:us-east-1:123456789012:function:FUNCTION_NAME",
                                "End": True,
                            },
                        },
                    }
                )
            ],
        )

        sm_node_2 = app.create_data(
            id="SM_NODE_2",
            compute_targets=[
                AWSSSFNStateMachine(
                    definition='{"StartAt": "FirstState", "States": {"FirstState": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:FUNCTION_NAME", "End": true}}}',
                    extra_permissions=[
                        Permission(resource=["arn:aws:lambda:us-east-1:123456789012:function:FUNCTION_NAME"], action=["lambda:*"])
                    ],
                )
            ],
        )

        input = self.add_external_data(app, "bucket", "ext_training_data_with_header", header=True)

        # Refer
        # https://docs.aws.amazon.com/step-functions/latest/dg/connect-bedrock.html
        sm_node_input_1 = app.create_data(
            id="SM_NODE_INPUT_1",
            inputs=[input],
            compute_targets=[
                StateMachine(
                    definition={
                        "StartAt": "LLMState",
                        "States": {
                            "LLMState": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::bedrock:invokeModel",
                                "Parameters": {
                                    "ModelId": "cohere.command-text-v14",
                                    "Body": {"prompt.$": "$.prompt_one", "max_tokens": 250},
                                    "ContentType": "application/json",
                                    "Accept": "*/*",
                                },
                                # use IF provided 'input' JSON to the SM and extract 'output' from it
                                "ResultPath": "$.output.materialized_resource_path",  # "$.result_one",
                                "ResultSelector": {"result_one.$": "$.Body.generations[0].text"},
                                "End": True,
                            },
                        },
                    }
                )
            ],
        )

        app.activate()

        # activate local orchestrator loop, because it will be required for the execution tests below
        self.activate_event_propagation(app, cycle_time_in_secs=3)

        app.execute(sm_node_1, wait=False)
        app.execute(sm_node_input_1[1]["2023-03-29"], wait=False)

        assert len(app.get_active_routes()) == 2

        app.kill(sm_node_1)
        app.kill(sm_node_input_1[1]["2023-03-29"])

        path, _ = app.poll(sm_node_1)
        assert not path  # forced kill

        path, _ = app.poll(sm_node_input_1[1]["2023-03-29"])
        assert not path  # forced kill

        assert len(app.get_active_routes()) == 0

        app.terminate()

        self.patch_aws_stop()

    def test_application_init_with_sfn_changeset(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        # check default AWS configuration and make sure that AWS Batch based drivers are offered
        supported_aws_constructs = AWSConfiguration.get_supported_constructs_map()
        assert AWSSFNCompute in supported_aws_constructs[BatchComputeDriver]
        app = AWSApplication(
            "sfn_sm_change", self.region, **{CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM: [AWSEMRBatchCompute, AWSSFNCompute]}
        )

        sm_node_1 = app.create_data(
            id="SM_NODE_1",
            compute_targets=[
                AWSSSFNStateMachine(
                    definition={
                        "StartAt": "FirstState",
                        "States": {
                            "FirstState": {
                                "Type": "Task",
                                "Resource": "arn:aws:lambda:us-east-1:123456789012:function:FUNCTION_NAME",
                                "End": True,
                            },
                        },
                    }
                )
            ],
        )

        sm_node_2 = app.create_data(
            id="SM_NODE_2",
            compute_targets=[
                AWSSSFNStateMachine(
                    definition='{"StartAt": "FirstState", "States": {"FirstState": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:FUNCTION_NAME", "End": true}}}'
                )
            ],
        )

        app.activate()

        # reload the app with empty dev-context
        app = AWSApplication(
            "sfn_sm_change", self.region, **{CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM: [AWSEMRBatchCompute, AWSSFNCompute]}
        )

        # declare the second node only to see the first node auto-deleted during activation
        sm_node_2 = app.create_data(
            id="SM_NODE_2",
            compute_targets=[
                AWSSSFNStateMachine(
                    definition='{"StartAt": "FirstState", "States": {"FirstState": {"Type": "Task", "Resource": "arn:aws:lambda:us-east-1:123456789012:function:FUNCTION_NAME", "End": true}}}'
                )
            ],
        )

        app.activate()

        app.terminate()

        self.patch_aws_stop()
