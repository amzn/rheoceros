# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.drivers.compute.aws_emr import AWSEMRBatchCompute

flow.init_basic_logging()

app = AWSApplication(
    "<YOUR_RHEOEROS_APP_NAME>",
    HostPlatform(
        AWSConfiguration.builder()
            .with_default_credentials(True)
            .with_region("us-east-1")
            # Required only if you want to use subnet id for EMR driver
            .with_param(AWSEMRBatchCompute.EMR_CLUSTER_SUBNET_ID, "<YOUR_SUBNET_ID>")
            .build()
    ),
)

# read your workflow URL from Secrets Manager
slack_obj = Slack(recipient_list=["<SLACK_WORKFLOW_URL>"])

daily_timer = app.add_timer(
    "daily_timer",
    "rate(7 days)",
    time_dimension_id="day"
)

training_input = app.marshal_external_data(
    S3Dataset(
        "<SM_S3_BUCKET_ACC_ID>",
        "sagemaker-us-east-1-<SM_S3_BUCKET_ACC_ID>",
        "TEST-CUSTOM-SAGEMAKER-DOCKER/training_input_data",
        dataset_format=DataFormat.CSV,
        delimiter=",",
    ),
    "training_input",
    dimension_spec={},
)

xgboost_model = app.create_data(
    id="xgboost_model",
    inputs={
        'training': training_input,
    },
    compute_targets=[
        SagemakerTrainingJob(
            AlgorithmSpecification={
                "TrainingImage": "<SM_S3_BUCKET_ACC_ID>.dkr.ecr.us-east-1.amazonaws.com/xgboost-extended-container-test:latest",
            },
            HyperParameters={
                'booster': 'gbtree',
                'eta': "0.1",
                'gamma': "0",
                'max_depth': "6",
                'objective': "binary:logistic",
                'eval_metric': "auc"
            }
        )
    ],
    # output_dimension_spec={
    #     "dummy": {
    #         type: DimensionType.STRING
    #     }
    # },
    # output_dim_links=[
    #     ("dummy", EQUALS, "training_output")
    # ]
)

transform_input = app.marshal_external_data(
    S3Dataset(
        "<SM_S3_BUCKET_ACC_ID>",
        "sagemaker-us-east-1-<SM_S3_BUCKET_ACC_ID>",
        "TEST-CUSTOM-SAGEMAKER-DOCKER/inference_input_data",
        dataset_format=DataFormat.CSV,
        delimiter=",",
        header=False,
        schema=False,
    ),
    "inference_input",
    dimension_spec={},
)

# TRANSFORM
transform_result_data = app.create_data(
    id="transform_result",
    inputs=[
        # By using `ref.range_check(True)`, we ensure `xgboost_model_data` and `transform_data` exists in S3, but
        # we don't require completion signals from their nodes (so don't require them to be re-executed).
        xgboost_model.ref,
        transform_input,
    ],
    compute_targets=[
        SagemakerTransformJob(
            TransformResources={
                'InstanceType': 'ml.m5.xlarge',
                'InstanceCount': 1,
            }
        )
    ]
)

app.activate()
#app.admin_console()

