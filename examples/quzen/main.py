# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.constructs import CompositeBatchCompute
from intelliflow.core.platform.drivers.compute.aws import AWSGlueBatchComputeBasic

flow.init_basic_logging()
log = logging.getLogger("./intelliFlowApp_logs")

app_name = "ddb-flow"
aws_region = "us-east-1"
DDB_TABLE = "<TARGET_DYNAMODB_TABLE>"
DDB_TABLE_ACC_ID = "<DDB_TABLE_AWS_ACCOUNT_ID>"

app = AWSApplication(
    app_name,
    HostPlatform(AWSConfiguration.builder()
                 .with_default_credentials(True)
                 .with_region(aws_region)
                 .with_param(CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM,
                             # In this example, we only want to use AWS Glue
                             [AWSGlueBatchComputeBasic])
                 .build()))

speed_intent_data = app.marshal_external_data(
    S3Dataset(
        "<BUCKET_AWS_ACC_ID>",
        "<BUCKET_NAME>",
        "<FOLDER_NAME_IF_ANY>",
        # in this example we assume that upstream S3 data is not partitioned
        dataset_format=DataFormat.PARQUET,
    ),
    id="speed_intent_data",
    protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
)


sink_node = app.create_data(
    id=f"QUZEN_DDB_SINK",
    inputs={
        "dex_data_daily": speed_intent_data
    },
    compute_targets=[
        BatchCompute(
            # Optimize the DDB writes
            # https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect.html#aws-glue-programming-etl-connect-dynamodb
            code=python_module("examples.quzen.compute"),
            lang=Lang.PYTHON,
            extra_permissions=[
                                # allow access to dec account bucket's 'aws/s3' key (uses aws/s3 key for SSE not CMK)
                                #Permission(resource=["arn:aws:kms:us-east-1:<KMS_ACCOUNT_ID>:key/<KMS_KEY>"],
                                #          action=["kms:*"]),
                                Permission(resource=[f"arn:aws:dynamodb:us-east-1:{DDB_TABLE_ACC_ID}:table/{DDB_TABLE}"],
                                          action=[
                                              "dynamodb:Describe*",
                                              "dynamodb:List*",
                                              "dynamodb:GetItem",
                                              "dynamodb:Query",
                                              "dynamodb:Scan",
                                              "dynamodb:PutItem",
                                              "dynamodb:UpdateItem",
                                              "dynamodb:BatchWriteItem",
                                          ])],
            GlueVersion="2.0",
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=25,
            Timeout=60 * 60,
            retry_count=0,
            # job params
            dynamodb_table=DDB_TABLE,
        )
    ],
    )

app.activate()
