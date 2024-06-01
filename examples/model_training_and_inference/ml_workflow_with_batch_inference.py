# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
Example of model training and model inferencing API.
"""

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.constructs import CompositeBatchCompute
from intelliflow.core.platform.drivers.compute.aws import AWSGlueBatchComputeBasic
from intelliflow.core.platform.drivers.compute.aws_emr import AWSEMRBatchCompute, RuntimeConfig
from intelliflow.core.platform.drivers.compute.aws_sagemaker.training_job import AWSSagemakerTrainingJobBatchCompute
from intelliflow.core.platform.drivers.compute.aws_sagemaker.transform_job import AWSSagemakerTransformJobBatchCompute
from intelliflow.core.platform.drivers.processor.aws import AWSLambdaProcessorBasic

from intelliflow.core.signal_processing.dimension_constructs import StringVariant

app_name = "ml-flow-batch"

flow.init_basic_logging()

log = logging.getLogger(app_name)

app = AWSApplication(app_name, HostPlatform(AWSConfiguration.builder()
                                            .with_default_credentials(True)
                                            .with_region("us-east-1")
                                            .with_param(CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM,
                                                        [
                                                            AWSGlueBatchComputeBasic,
                                                            AWSSagemakerTrainingJobBatchCompute,
                                                            AWSSagemakerTransformJobBatchCompute
                                                        ])
                                            .with_param(AWSLambdaProcessorBasic.CORE_LAMBDA_CONCURRENCY_PARAM, 3)
                                            .build()))

daily_timer = app.add_timer("daily_timer",
                            "rate(7 days)",
                            time_dimension_id="day")

titanic_schema = [
    # SparkType is just a helper, second field (type) expects a string, actual type object is to be 'eval'ed in the
    # compute environment. We are expected to provide a schema definition compatible with the compute environment
    # (which is Glue/pyspark in this example [see ML_PROCESSED node's compute definition]).
    # ("PassengerId", "LongType()", True),
    ("PassengerId", SparkType.LongType, True),
    ("Survived", SparkType.ShortType, True),
    ("Pclass", SparkType.ShortType, True),
    ("Name", SparkType.StringType, True),
    ("Sex", SparkType.StringType, True),
    ("Age", SparkType.ShortType, True),
    ("SibSp", SparkType.ShortType, True),
    ("Parch", SparkType.ShortType, True),
    ("Ticket", SparkType.StringType, True),
    ("Fare", SparkType.DecimalType, True),
    ("Cabin", SparkType.StringType, True),
    ("Embarked", SparkType.StringType, True),
]

raw_data = app.add_external_data(
    "raw_data",
    S3(account_id="<S3_BUCKET_ACC_ID>",
       bucket="rheoceros-tutorial-2023",
       key_prefix="titanic",
       dataset_format=DataFormat.CSV,
       delimiter=",",
       schema_def=titanic_schema
       ),
    completion_file=None)

preprocessed_data = app.create_data(
    id="ML_PROCESSED",
    # since training_data (raw_data) is from a different account, our app won't get a signal, we need to use ref / reference here
    inputs=[daily_timer, raw_data.ref.range_check(True)],
    compute_targets=[
        Glue(
            """
            import pyspark.sql.functions as F

            data = raw_data.select(['Survived', 'Pclass', 'Sex', 'SibSp', 'Parch', 'Fare', 'Embarked'])
            # fill missing values
            data = data.na.fill({'Embarked': 'S'})

            # simple feature engineering
            data = data.withColumn('family_size', F.col('SibSp') + F.col('Parch') + F.lit(1))

            # convert categorical to numerical by one-hot-encoding
            categorical_dict = {
                'Sex': ['female', 'male'],
                'Embarked': ['S', 'C', 'Q']
            }

            for cat_feature, val_list in categorical_dict.items():
                for val in val_list:
                    ohe_name = cat_feature + '_' + val
                    data = data.withColumn(ohe_name, (F.col(cat_feature)==val).cast('integer'))

            # create a randome number (0,1) used for train, validation, test split
            data = data.withColumn('rand', F.rand(seed=42))

            # only select engineered columns as features
            output = data.select(['Survived', 'Pclass', 'SibSp', 'Parch', 'Fare', 'family_size', 'Sex_female', 'Sex_male', 'Embarked_S', 'Embarked_C', 'Embarked_Q', 'rand'])
            """
        )
    ]
)

train = app.create_data(
    id="TRAIN",
    inputs=[preprocessed_data],
    compute_targets=[
        SparkSQL(
            """
                SELECT Survived, Pclass, SibSp, Parch, Fare, family_size, Sex_female, Sex_male, Embarked_S, Embarked_C, Embarked_Q
                    FROM ML_PROCESSED
                        WHERE rand<0.5
            """,

            NumberOfWorkers=3
        )
    ],
    # Sagemaker XGBoost and BatchTransform both require input dataset has no header row
    header=False,
    schema=None,
    protocol=None
)

validation = app.create_data(
    id="VALIDATION",
    inputs=[preprocessed_data],
    compute_targets=[
        SparkSQL(
            """
                SELECT Survived, Pclass, SibSp, Parch, Fare, family_size, Sex_female, Sex_male, Embarked_S, Embarked_C, Embarked_Q
                    FROM ML_PROCESSED
                        WHERE rand>=0.5 and rand<0.8
            """,
            GlueVersion="2.0",
            WorkerType=GlueWorkerType.G_1X.value,
            NumberOfWorkers=3,
        )
    ],
    # Sagemaker XGBoost and BatchTransform both require input dataset has no header row
    header=False,
    schema=None,
    protocol=None
)

test = app.create_data(
    id="TEST",
    inputs=[preprocessed_data],
    compute_targets=[
        SparkSQL(
            """
                SELECT Survived, Pclass, SibSp, Parch, Fare, family_size, Sex_female, Sex_male, Embarked_S, Embarked_C, Embarked_Q
                    FROM ML_PROCESSED
                        WHERE rand>=0.8
            """,
            GlueVersion="3.0",
            NumberOfWorkers=3,
        )
    ],

    # Save with header for this one for evaluation later on
    header=True,
    schema=None,
    protocol=None
)

test_no_label = app.create_data(
    id="TEST_NO_LABEL",
    inputs=[test],
    compute_targets=[
        Glue(
            """
                output = TEST.drop('Survived')
            """,
            NumberOfWorkers=3,
        )
    ],
    # Sagemaker XGBoost and BatchTransform both require input dataset has no header row
    header=False,
    schema=None,
    protocol=None
)

xgboost_model_data = app.train_xgboost(
    id="XGBOOST_MODEL",
    training_data=train,
    validation_data=validation,
    training_job_params={
        "HyperParameters": {
            "booster": "gbtree",
            "eta": "0.2",
            "gamma": "0",
            "max_depth": "5",
            "objective": "binary:logistic",
            "num_round": "5",
            "eval_metric": "error"
        },
        "ResourceConfig": {
            "InstanceCount": 1,
            "InstanceType": "ml.m5.xlarge",
            "VolumeSizeInGB": 20
        },
    }
)

# TRANSFORM (creates a dataset [SM BT behaviour])
batch_inference = app.create_data(
    id="BATCH_INFERENCE",
    inputs=[
        # By using `ref.range_check(True)` (for demo purposes), we ensure `transform_data` exists in S3, but
        # we don't require completion signals from their nodes (so don't require them to be re-executed).
        xgboost_model_data,
        test_no_label.ref.range_check(True),
    ],
    compute_targets=[
        SagemakerTransformJob(
            TransformResources={
                'InstanceType': 'ml.m4.4xlarge',
                'InstanceCount': 1,
            }
        )
    ],
)

evaluation = app.create_data(
    id="EVALUATION",
    inputs=[batch_inference, test],
    compute_targets=[
        # EMR
        Glue(
            """
                # TODO use BATCH_INFERENCE and TEST dataframes and do evaluation

                # from pyspark.mllib.evaluation import BinaryClassificationMetrics
                # from pyspark.sql.types import StructType,StructField, DoubleType
                # import pyspark.sql.functions as F
                # 


                # join the labels with scores
                test_data = TEST.withColumn('id', F.monotonically_increasing_id())
                score_data = BATCH_INFERENCE.toDF('score').withColumn('id', F.monotonically_increasing_id())
                test_data = test_data.join(score_data, on='id')


                # evaluation
                eval_data_rdd = test_data.select('Survived', 'score').withColumn(
                    'Survived', F.col('Survived').cast('float')).rdd

                metrics = BinaryClassificationMetrics(eval_data_rdd)
                auc = metrics.areaUnderROC


                # collect metric in DF
                data = [(auc,)]
                schema = StructType([

                    StructField("AUC",DoubleType(),True)

                  ])

                output = spark.createDataFrame(data=data,schema=schema)
            """,
            NumberOfWorkers=3,
        )
    ],
)

evaluation_scala = app.create_data(
    id="EVALUATION_SCALA",
    inputs=[batch_inference, test],
    compute_targets=[
        # EMR
        Glue(
            """
                // TODO use BATCH_INFERENCE and TEST dataframes and do evaluation
                BATCH_INFERENCE
            """,
            lang=Lang.SCALA,
            NumberOfWorkers=3,
        )
    ],
)

app.activate()

# - When recursive=True, framework checks if this route has received all input signals. Traverses the hierarchy and automatically
# backfills missing partitions of parent nodes.
# app.execute(batch_inference["2023-05-04"], recursive=True)


# from datetime import datetime
# app.execute(batch_inference[datetime.now()], recursive=True)
