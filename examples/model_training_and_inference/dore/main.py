# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Example of model training and model inferencing API. As of 2022/11/24, IF supports Sagemaker xgboost and PCA for
training, Sagemaker BatchTransform for inferencing.

This example is a simplified version of a separate package (Rheoceros Python project). All migrated under examples folder
here.

WARNING: Donot forget to add permissions for your application's (auto-created) exec role if the training data bucket is not in the same AWS acc.
"""

import intelliflow.api_ext as flow
from examples.model_training_and_inference.dore.util.linking import timer_to_pre_process_data_day, \
    pre_process_data_day_to_timer
from intelliflow.api_ext import *
from intelliflow.core.platform.constructs import CompositeBatchCompute
from intelliflow.core.platform.drivers.compute.aws import AWSGlueBatchComputeBasic
from intelliflow.core.platform.drivers.compute.aws_sagemaker.training_job import AWSSagemakerTrainingJobBatchCompute
from intelliflow.core.platform.drivers.compute.aws_sagemaker.transform_job import AWSSagemakerTransformJobBatchCompute

flow.init_basic_logging()
log = logging.getLogger("./intelliFlowApp_logs")

app_name = "sm-train-infer"
aws_region = "us-east-1"

app = AWSApplication(
    app_name,
    HostPlatform(AWSConfiguration.builder()
                 .with_default_credentials(True)
                 .with_region(aws_region)
                 .with_param(CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM,
                             # In this example, we only want to use AWS Glue and AWS Sagemaker
                             [AWSGlueBatchComputeBasic,
                              AWSSagemakerTrainingJobBatchCompute,
                              AWSSagemakerTransformJobBatchCompute])
                 .build()))

pdex_data_account_id = "<S3_BUCKET_ACC_ID>"
pdex_data_s3_bucket = "inconsistency-data-123"
pdex_data_folder = "output/DORE"
pdex_data_region = "NA"
pdex_smart_filtering_data_node_id = "pdex_smart_filtered"

pdex_smart_filtering_data = app.marshal_external_data(
    S3Dataset(
        pdex_data_account_id,
        pdex_data_s3_bucket,
        pdex_data_folder,
        # first partition key is marketplace, e.g. "NA"
        "{}",
        # second partition key is day, e.g. "2022-07-23"
        "xdex-smart-filtering-merged-with-header-v1.2-{}",
        # third partition key is index number, e.g. 0
        "{}",
        dataset_format=DataFormat.CSV,
        delimiter=","
    ),
    id=pdex_smart_filtering_data_node_id,
    # dimension_spec describes 3 dimensions in order: "marketplace", "day" and "index", they correspond to the 3
    # partition keys we declared above.
    # When this S3Dataset is materialized (i.e. 3 placeholders are filled with actual values), S3Dataset will point to
    # f"s3://{pdex_data_s3_bucket}/{pdex_data_folder}/{1st_partition_value}/{2nd_partition_value}/{3rd_partition_value}".
    # For example, if marketplace is set to "NA", day is set to "2022-07-23", index is set to 0, then the target S3
    # folder is f"s3://{BUCKET}/output/DORE/NA/xdex-smart-filtering-merged-with-header-v1.2-2022-07-23/0/"
    dimension_spec={
        "region": {
            "type": DimensionType.STRING,
            "day": {
                "type": DimensionType.DATETIME,
                "format": "%Y-%m-%d",
                "index": {
                    "type": DimensionType.LONG
                }
            },
        }
    },
    dimension_filter={
        "*": {
            "*": {
                7: {}
            }
        }
    },
    # The protocol S3Dataset uses to determine whether a S3 folder is considered valid.
    # Here it looks for a file named _SUCCESS within the folder.
    protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
)

# Adding daily timer to trigger data preprocessing job to run daily. More info:
daily_timer = app.add_timer("daily_timer", "rate(1 day)",
                            # override the dimension name in this timer from "time" (default) to "day"
                            time_dimension_id="day",
                            time_dimension_granularity=DatetimeGranularity.DAY)

# Dimension linking allows you to provide a custom function to map an input dimension to an output dimension
# Utilizing Rheoceros auto dimension linking to output all node results to this path
pre_process_single_date_core = app.create_data(
    id=f"pre_process_single_date_core_{pdex_data_region}",
    # you can use Python's slice syntax to create data with a range of values.
    # syntax: data[(+/-)range_start_shift : (+/-)relative_range_end]
    # e.g. we apply slice [-3: -5] on "index" dimension, since there is a default dimension sieving index=7,
    # [-2: -4] would represent [5, 4, 3, 2]
    # similarly, [:-8] would represent [7, 6, 5, 4, 3, 2, 1, 0]
    # In the end, we enable range check, which requires 8 indexes
    # augment_data["prod"]["DORE"]["1.1"][pdex_data_region]["*"][7] and
    # augment_data["prod"]["DORE"]["1.1"][pdex_data_region]["*"][6] and
    # ...
    # augment_data["prod"]["DORE"]["1.1"][pdex_data_region]["*"][0] to be present for this signal to be ready
    inputs={"daily_data": pdex_smart_filtering_data[pdex_data_region]["*"][:-8].ref.range_check(True),
            "timer": daily_timer},
    # Input dimension linking allows you to provide a custom function `f()` as JOIN condition between 2 input dimensions
    # the JOIN condition will be `f(dim_2_value) == dim_1_value`
    # here we use `timer_to_pre_process_data_day()` to link pdex_smart_filtering_data("day") with daily_timer("day").
    # the JOIN condition is `timer_to_pre_process_data_day(daily_timer("day")) == pdex_smart_filtering_data("day")`.
    input_dim_links=[(pdex_smart_filtering_data("day"), timer_to_pre_process_data_day, daily_timer("day"))],
    # Outputting results to folder path based on dataset, model name, model version, and region.
    output_dimension_spec={
        "dataset": {
            type: DimensionType.STRING,
            "modelName": {
                type: DimensionType.STRING,
                "modelVersion": {
                    type: DimensionType.STRING,
                    "region": {
                        type: DimensionType.STRING,
                        "day": {type: DimensionType.DATETIME, "granularity": DatetimeGranularity.DAY,
                                "format": "%Y-%m-%d"},
                    }
                }
            }
        }
    },
    # output_dim_links allows you to provide custom functions to map an input dimension to an output dimension
    # each 3-tuple is composed of (output_dim_name, mapper, input_dim) or (output_dim_name, mapper, literal_value)
    # here we are assigning literal values to each output dimension
    output_dim_links=[
        ("dataset", EQUALS, "prod"),
        ("modelName", EQUALS, "DORE"),
        ("modelVersion", EQUALS, "1.1"),
        # with auto_output_dim_linking_enabled=True, the value of "region" and "day" output dimension can be
        # automatically assigned with the value of "region" and "day" dimension in the input signals (i.e.
        # pdex_smart_filtering_data). So technically, linking on "region" is not necessary here,
        # it's explicitly written out for better readability
        ("region", EQUALS, pdex_data_region),
        ("day", timer_to_pre_process_data_day, daily_timer("day")),
        # Not required for runtime - Used for execute API
        (daily_timer("day"), pre_process_data_day_to_timer, "day"),
        (pdex_smart_filtering_data("day"), EQUALS, "day"),
    ],
    compute_targets=[
        Glue(
            # you can use string literals of your code if compute logic is short and simple
            # otherwise it is recommended to put your code in a module and refer to your module like this
            # the module path works the same as in import statements

            code=python_module("examples.model_training_and_inference.dore.data_nodes.internal.pre_process_single_data_core"),
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=100,
            GlueVersion="3.0",
        )
    ],
)

dedupe = app.create_data(
    id=f"dedupe_{pdex_data_region}",
    inputs={"daily_data": pre_process_single_date_core},
    compute_targets=[
        Glue(
            code=python_module("examples.model_training_and_inference.dore.data_nodes.internal.dedupe"),
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=100,
            GlueVersion="3.0",
        )
    ],
)

augment_data = app.create_data(
    id=f"augment_data_{pdex_data_region}",
    inputs={"daily_data": dedupe},
    compute_targets=[
        Glue(
            code=python_module("examples.model_training_and_inference.dore.data_nodes.internal.augment"),
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=100,
            GlueVersion="3.0",
        )
    ],
    dataset_format=DataFormat.PARQUET,
)

# you can use Python's slice syntax to create data with a range of values.
# syntax: data[(+/-)range_start_shift : (+/-)relative_range_end]
# e.g. we apply slice [-3: -5] on "day" dimension, if the input value of "day" is "2022-11-20", [-3: -5] would represent
# ["2022-11-17", "2022-11-16", "2022-11-15", "2022-11-14", "2022-11-13"]
# similarly [:-2] would represent ["2022-11-20", "2022-11-19"]
# In the end, we enable range check, which requires both
# augment_data["prod"]["DORE"]["1.1"][pdex_data_region]["2022-11-20"] and
# augment_data["prod"]["DORE"]["1.1"][pdex_data_region]["2022-11-19"] to be present for this signal to be ready
two_days_of_augment_data_with_completion_check = augment_data["prod"]["DORE"]["1.1"][pdex_data_region][:-2].range_check(
    True)

prepare_data_for_training = app.create_data(
    id=f"prepare_data_for_training_{pdex_data_region}",
    inputs={"today_and_yesterday": two_days_of_augment_data_with_completion_check,
            "today_augment_data": augment_data},
    compute_targets=[
        Glue(
            code=python_module("examples.model_training_and_inference.dore.data_nodes.internal.prepare_data_for_training_and_validation"),
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=100,
            GlueVersion="3.0",
            is_for_training="true",
        )
    ]
)

prepare_data_for_validation = app.create_data(
    id=f"prepare_data_for_validation_{pdex_data_region}",
    inputs={"today_augment_data": augment_data, "df_train": prepare_data_for_training},
    compute_targets=[
        Glue(
            code=python_module("examples.model_training_and_inference.dore.data_nodes.internal.prepare_data_for_training_and_validation"),
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=100,
            GlueVersion="3.0",
            is_for_training="false",
        )
    ]
)

encode_train = app.create_data(
    id=f"encode_train_{pdex_data_region}",
    inputs={"df": prepare_data_for_training},
    compute_targets=[
        Glue(
            code=python_module("examples.model_training_and_inference.dore.data_nodes.internal.encode_data"),
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=100,
            GlueVersion="3.0",
            contains_isFiltered=True
        )
    ],
    # make sure that the output partition will only have data files (not to update Sagemaker builtin algos)
    header=False,
    schema=None,
    protocol=None,
)

encode_validation = app.create_data(
    id=f"encode_validation_{pdex_data_region}",
    inputs={"df": prepare_data_for_validation},
    compute_targets=[
        Glue(
            code=python_module("examples.model_training_and_inference.dore.data_nodes.internal.encode_data"),
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=100,
            GlueVersion="3.0",
            contains_isFiltered=True
        )
    ],
    # make sure that the output partition will only have data files (not to update Sagemaker builtin algos)
    header=False,
    schema=None,
    protocol=None,
)

# Using hyper parameter values from original model training notebook:
xgboost_model_data = app.train_xgboost(
    id=f"xgboost_model_{pdex_data_region}",
    training_data=encode_train,
    validation_data=encode_validation,
    training_job_params={
        "HyperParameters": {"max_depth": "5", "eta": "0.9", "objective": "multi:softprob", "num_class": "2",
                            "num_round": "20"},
        "ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 30},
    },
)

# test exec
# app.execute(xgboost_model_data["prod"]["DORE"]["1.1"]["NA"]["2022-11-20"], recursive=True)

batch_transform_input = app.create_data(
    id="batch_transform_input",
    inputs={"df": prepare_data_for_validation},
    compute_targets=[
        Glue(
            code=python_module("examples.model_training_and_inference.dore.data_nodes.internal.encode_data"),
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=100,
            GlueVersion="3.0",
            # here we need to exclude "isFiltered" column as it is the label column (1st column in the dataset)
            contains_isFiltered=False
        )
    ],
    # make sure that the output partition will only have data files (not to update Sagemaker builtin algos)
    header=False,
    schema=None,
    protocol=None,
)

transform_result_data = app.create_data(
    id="batch_transform",
    inputs=[
        xgboost_model_data,
        batch_transform_input.ref,
        daily_timer,
    ],
    compute_targets=[
        SagemakerTransformJob(
            # refer
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_transform_job
            #  DO NOT declare ModelName, TransformInput, TransformOutput, TranformJobName.
            #  Setting those will be NOOP as they will be managed by the framework at runtime
            # Please note that even 'TransformResources' is OPTIONAL
            TransformResources={
                'InstanceType': 'ml.m4.4xlarge',
                'InstanceCount': 1,
            }
        )
    ])

# This includes "examples" module into the bundle.zip deployed in AWS runtime, so that other related modules required
# in this DORE example can be found and loaded in AWS runtime
set_deployment_conf(app_extra_modules={"examples"})

app.activate()
