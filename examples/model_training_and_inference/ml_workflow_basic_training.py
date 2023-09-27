# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
import logging

from intelliflow.core.signal_processing.dimension_constructs import StringVariant
from intelliflow.core.platform.constructs import CompositeBatchCompute
from intelliflow.core.platform.drivers.compute.aws import AWSGlueBatchComputeBasic
from intelliflow.core.platform.drivers.compute.aws_sagemaker.training_job import AWSSagemakerTrainingJobBatchCompute

app_name = "ml-flow-train"

flow.init_basic_logging()
log = logging.getLogger(app_name)

app = AWSApplication(
    app_name,
    HostPlatform(
        AWSConfiguration.builder()
            .with_default_credentials(True)
            .with_region("us-east-1")
            .with_param(CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM,
                        [
                            AWSGlueBatchComputeBasic,
                            AWSSagemakerTrainingJobBatchCompute,
                        ])
            .build()
    )
)

training = app.marshal_external_data(
    S3Dataset("AWS_ACCOUNT_ID_1", "eureka-model-training-data", "eureka_p3/v8_00/all-data-prod",
              "partition_day={}", dataset_format=DataFormat.CSV,
              header=False,
              schema=None,
              schema_type=None),
    "eureka_training_all_data",
    {
        'day': {
            'type': DimensionType.DATETIME
        }
    },
    {
        "*": {
            'format': '%Y-%m-%d',
            'timezone': 'PST',
        },
    }
)

validation = app.add_external_data(
    "eureka_validation_data",
    S3("AWS_ACCOUNT_ID_1",
       "eureka-model-training-data",
       "eureka_p3/v8_00/training-data",
       StringVariant('NA', 'region'),
       AnyDate('day', {'format': '%Y-%m-%d'}),
       header=False,
       schema=None,
       schema_type=None,
       ),
    completion_file=None
)

xgboost_model_data = app.train_xgboost(
    id="eureka_xgboost_model",
    training_data=training,
    validation_data=validation,
    training_job_params={
        "HyperParameters": {"max_depth": "5", "eta": "0.9",
                            "objective": "multi:softprob", "num_class": "2",
                            "num_round": "20"},
        "ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m4.16xlarge",
                           "VolumeSizeInGB": 30},
        "AlgorithmSpecification": {
            "TrainingImage": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.0-1",
        }
    }
)

slack_notification_recipient_1 = "<a legit slack workflow url>"
slack_obj = Slack(recipient_list=[slack_notification_recipient_1])

xgboost_model_data_advanced = app.train_xgboost(
    id="eureka_xgboost_model_advanced",
    training_data=training,
    validation_data=validation,
    extra_compute_targets=[
        InlinedCompute(
            lambda a, b, c: print(
                "Do something in AWS when model training/execution starts")
        )
    ],
    execution_hook=RouteExecutionHook(
        on_exec_begin=slack_obj.action(
            message="Advanced xgboost training has just started!"),
        on_compute_success=slack_obj.action(
            message="Advanced xgboost training complete!")
    ),
    retry_count=1,
    training_job_params={
        "ResourceConfig": {
            "InstanceCount": 1,
            "InstanceType": "ml.m5.xlarge",
            "VolumeSizeInGB": 30
        }
    }
)

app.activate()
