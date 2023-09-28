# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from intelliflow.api_ext import *
from intelliflow.core.application.context.node.marshaling.nodes import MarshalerNode
from intelliflow.core.platform.constructs import BatchCompute as BatchComputeDriver
from intelliflow.core.platform.development import AWSConfiguration
from intelliflow.core.platform.drivers.compute.aws_sagemaker.training_job import AWSSagemakerTrainingJobBatchCompute
from intelliflow.core.platform.drivers.compute.aws_sagemaker.transform_job import AWSSagemakerTransformJobBatchCompute
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSApplicationModelTransformJob(AWSTestBase):
    @classmethod
    def add_external_training_data(
        cls, app, id: str, header: bool = False, schema: bool = False, protocol: bool = False, dataset_format=DataFormat.CSV
    ) -> MarshalerNode:
        return app.marshal_external_data(
            S3Dataset("111222333444", "bucket", "training_dataset", "{}", dataset_format=dataset_format, header=header, schema=schema),
            id,
            dimension_spec={"day": {type: DimensionType.DATETIME}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}) if protocol else None,
        )

    def test_application_init_transform_driver(self):
        self.patch_aws_start()

        # check default AWS configuration and make sure that Sagemaker based drivers are offered
        supported_aws_constructs = AWSConfiguration.get_supported_constructs_map()
        assert AWSSagemakerTrainingJobBatchCompute in supported_aws_constructs[BatchComputeDriver]
        assert AWSSagemakerTransformJobBatchCompute in supported_aws_constructs[BatchComputeDriver]

        app = AWSApplication("test_application", self.region)

        training_data = self.add_external_training_data(app, "ext_training_data_OK", header=False)

        xgboost_model_data_advanced = app.train_xgboost(
            id="eureka_xgboost_model_advanced",
            training_data=training_data,
            validation_data=training_data,
            extra_compute_targets=[InlinedCompute(lambda a, b, c: print("Do something in AWS when model training/execution starts"))],
            retry_count=1,
            training_job_params={"ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 30}},
        )

        daily_timer = app.add_timer("daily_timer", "rate(7 days)", time_dimension_id="day")

        transform_data = app.marshal_external_data(
            S3Dataset(
                "222333444555",
                "eureka-model-training-data",
                "eureka_p3/v8_00/all-data-prod",
                "partition_day={}",
                dataset_format=DataFormat.CSV,
                header=False,
            ),
            "eureka_training_all_data",
            {"day": {"type": DimensionType.DATETIME}},
            {
                "*": {
                    "format": "%Y-%m-%d",
                    "timezone": "PST",
                },
            },
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        # fail due to no model artifact
        with pytest.raises(ValueError, match="model"):
            transform_result_data = app.create_data(
                id="batch_transform",
                inputs=[transform_data.ref, daily_timer],
                compute_targets=[
                    SagemakerTransformJob(
                        TransformResources={
                            "InstanceType": "ml.m4.4xlarge",
                            "InstanceCount": 1,
                        }
                    )
                ],
            )

        xgboost_model_data = app.train_xgboost(
            id="eureka_xgboost_model", header=False, training_data=training_data, validation_data=training_data
        )

        # fail due to more than one model artifact
        with pytest.raises(ValueError):
            transform_result_data = app.create_data(
                id="batch_transform",
                inputs=[xgboost_model_data, xgboost_model_data_advanced, transform_data.ref, daily_timer],
                compute_targets=[
                    SagemakerTransformJob(
                        TransformResources={
                            "InstanceType": "ml.m4.4xlarge",
                            "InstanceCount": 1,
                        }
                    )
                ],
            )

        # fail due to output having header == "true"
        with pytest.raises(ValueError):
            transform_result_data = app.create_data(
                id="batch_transform",
                header="true",
                inputs=[xgboost_model_data, transform_data.ref, daily_timer],
                compute_targets=[
                    SagemakerTransformJob(
                        TransformResources={
                            "InstanceType": "ml.m4.4xlarge",
                            "InstanceCount": 1,
                        }
                    )
                ],
            )

        # Successful validation OK
        transform_result_data = app.create_data(
            id="batch_transform_OK",
            header=False,
            inputs=[xgboost_model_data, transform_data.ref, daily_timer],
            compute_targets=[
                SagemakerTransformJob(
                    TransformResources={
                        "InstanceType": "ml.m4.4xlarge",
                        "InstanceCount": 1,
                    }
                )
            ],
        )

        app.activate()

        self.patch_aws_stop()
