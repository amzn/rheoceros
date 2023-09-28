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
from intelliflow.core.platform.constructs import RoutingTable
from intelliflow.core.platform.definitions.compute import (
    ComputeResourceDesc,
    ComputeSessionDesc,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.platform.development import AWSConfiguration, HostPlatform
from intelliflow.core.platform.drivers.compute.aws_sagemaker.training_job import AWSSagemakerTrainingJobBatchCompute
from intelliflow.core.platform.drivers.compute.aws_sagemaker.transform_job import AWSSagemakerTransformJobBatchCompute
from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.dimension_constructs import DimensionVariantFactory
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSApplicationModelTrainingBasic(AWSTestBase):
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

    def test_application_init_with_model_extensions(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        # check default AWS configuration and make sure that Sagemaker based drivers are offered
        supported_aws_constructs = AWSConfiguration.get_supported_constructs_map()
        assert AWSSagemakerTrainingJobBatchCompute in supported_aws_constructs[BatchComputeDriver]
        app = AWSApplication("model_basic", self.region)

        training_data = self.add_external_training_data(app, "ext_training_data_with_header", header=True)

        # fail due to header
        with pytest.raises(ValueError):
            xgboost_model_data = app.train_xgboost(
                id="eureka_xgboost_model",
                # TODO use this after range_shift feature is added to DimensionVariant
                # training_data=training_data[-1:],  # NOW - 1 as training data
                training_data=training_data,
                validation_data=training_data,
            )  # NOW as validation

        training_data = self.add_external_training_data(app, "ext_training_data_with_protocol", header=False, protocol=True)
        # fail due to protocol file
        with pytest.raises(ValueError):
            xgboost_model_data = app.train_xgboost(
                id="eureka_xgboost_model",
                # TODO use this after range_shift feature is added to DimensionVariant
                # training_data=training_data[-1:],  # NOW - 1 as training data
                training_data=training_data,
                validation_data=training_data,
            )  # NOW as validation

        training_data = self.add_external_training_data(app, "ext_training_data_with_schema", header=False, schema=True, protocol=False)
        # fail due to schema file
        with pytest.raises(ValueError):
            xgboost_model_data = app.train_xgboost(
                id="eureka_xgboost_model", training_data=training_data, validation_data=training_data
            )  # NOW as validation

        training_data = self.add_external_training_data(app, "ext_training_data_OK")
        # should succeed
        xgboost_model_data = app.train_xgboost(id="eureka_xgboost_model", training_data=training_data, validation_data=training_data)

        training_data_parquet = self.add_external_training_data(app, "ext_training_data_wrong_format", dataset_format=DataFormat.PARQUET)
        # fail due to missing CSV dataset format
        with pytest.raises(ValueError):
            xgboost_model_data = app.train_xgboost(
                id="eureka_xgboost_model_2", training_data=training_data_parquet, validation_data=training_data_parquet
            )

        # fail due to extra batch-compute declaration
        with pytest.raises(ValueError):
            xgboost_model_data_advanced = app.train_xgboost(
                id="eureka_xgboost_model_advanced",
                training_data=training_data,
                validation_data=training_data,
                extra_compute_targets=[SparkSQL("select foo from bar")],
                retry_count=1,
                training_job_params={"ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 30}},
            )

        xgboost_model_data_advanced = app.train_xgboost(
            id="eureka_xgboost_model_advanced",
            training_data=training_data,
            validation_data=training_data,
            extra_compute_targets=[InlinedCompute(lambda a, b, c: print("Do something in AWS when model training/execution starts"))],
            retry_count=1,
            training_job_params={"ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 30}},
        )

        # should fail due to wrong metadata declarations
        with pytest.raises(ValueError):
            app.train_xgboost(
                id="eureka_xgboost_model_advanced_2",
                training_data=training_data,
                validation_data=training_data,
                extra_compute_targets=[InlinedCompute(lambda a, b, c: print("Do something in AWS when model training/execution starts"))],
                retry_count=1,
                training_job_params={"ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 30}},
                # schema on model artifact
                schema="_SCHEMA_FILE_NAME",
            )

        with pytest.raises(ValueError):
            app.train_xgboost(
                id="eureka_xgboost_model_advanced_2",
                training_data=training_data,
                validation_data=training_data,
                extra_compute_targets=[InlinedCompute(lambda a, b, c: print("Do something in AWS when model training/execution starts"))],
                retry_count=1,
                training_job_params={"ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 30}},
                # schema on model artifact
                schema_type="spark.json",
            )

        # should fail due to wrong metadata declarations
        with pytest.raises(ValueError):
            app.train_xgboost(
                id="eureka_xgboost_model_advanced_2",
                training_data=training_data,
                validation_data=training_data,
                extra_compute_targets=[InlinedCompute(lambda a, b, c: print("Do something in AWS when model training/execution starts"))],
                retry_count=1,
                training_job_params={"ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 30}},
                # header on model artifact
                header=True,
            )

        app.train_xgboost(
            id="eureka_xgboost_model_advanced_2",
            training_data=training_data,
            validation_data=training_data,
            extra_compute_targets=[InlinedCompute(lambda a, b, c: print("Do something in AWS when model training/execution starts"))],
            retry_count=1,
            training_job_params={"ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 30}},
            # explicit declaration of compatible metadata (if left empty, they will auto-defined by underlying Training Job BatchCompute driver)
            header=False,
            schema=None,
            schema_type=None,
        )

        # override training image with one of other xgboost version, since the image uri is not in builtin_algos map,
        # it will be treated as custom algorithm, then HyperParameters are mandatory
        with pytest.raises(ValueError, match="HyperParameters"):
            app.train_xgboost(
                id="eureka_xgboost_model_advanced_3",
                training_data=training_data,
                validation_data=training_data,
                extra_compute_targets=[InlinedCompute(lambda a, b, c: print("Do something in AWS when model training/execution starts"))],
                retry_count=1,
                training_job_params={
                    "ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 30},
                    "AlgorithmSpecification": {
                        "TrainingImage": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.0-1",
                    },
                },
                header=False,
                schema=None,
                schema_type=None,
            )

        # happy case with custom training image
        app.train_xgboost(
            id="eureka_xgboost_model_advanced_3",
            training_data=training_data,
            validation_data=training_data,
            extra_compute_targets=[InlinedCompute(lambda a, b, c: print("Do something in AWS when model training/execution starts"))],
            retry_count=1,
            training_job_params={
                "ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 30},
                "AlgorithmSpecification": {
                    "TrainingImage": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.0-1",
                },
                "HyperParameters": {
                    "booster": "gbtree",
                    "eta": "0.1",
                    "gamma": "0",
                    "max_depth": "6",
                    "objective": "binary:logistic",
                    "eval_metric": "auc",
                },
            },
            header=False,
            schema=None,
            schema_type=None,
        )

        app.activate()

        self.patch_aws_stop()
