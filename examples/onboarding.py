# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

flow.init_basic_logging()
# log = logging.getLogger("./intelliFlowApp_logs")

app_name = "onboarding"
region = "us-east-1"

# on local or cloud computer use default AWS credentials from the system
# assume that the accoutn is '222333444555'
app = AWSApplication(app_name, region)
# on AWS Sagemaker notebook:
# app = AWSApplication(app_name, region)

# to wipe out existing app (to create another one with a different name maybe)
# app.terminate()
# app.delete()

# IMPORT PDEX_COMBINE EXTERNAL DATA
# This is one of the data node decleared. It reads in pdex_combine data
# The partition can be specified within the node.
pdex_combine = app.marshal_external_data(
    S3Dataset("222333444555",  # from the same account
              "cindex-in-search-metric-collection-demo",  # the S3 bucket
              "edx_dump/pdex/v2", "{}", "{}",  # the datapath with partition
              dataset_format=DataFormat.PARQUET)
    , "pdex_combine"
    # Here the partition is decleared as region and day.
    , {
        'region': {
            type: DimensionType.STRING,
            'day': {
                'format': '%Y-%m-%d',
                'type': DimensionType.DATETIME
            }
        }
    }
    , {
        "*": {  # ex: 'NA', 'EU', etc
            "*": {  # whatever date it is
            }
        }
    }
    #     , SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
    #     The above line will check the _SUCCESS file and make the pipeline event-based
    #     The pipeline will be triggered once it detects the file.
)

# IMPORT XDF EXTERNAL DATA
SEARCH_OPEN_DATA_AWS_ACCOUNT_ID = "111222333444"
tommy_enc_key = "arn:aws:kms:us-east-1:111222333444:key/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

xdf_external = app.marshal_external_data(
    S3Dataset(SEARCH_OPEN_DATA_AWS_ACCOUNT_ID,
              "searchdata-core-xdf-demo",
              "",
              "{}", "{}*",  # partition dimension
              dataset_format=DataFormat.PARQUET, encryption_key=tommy_enc_key)
    , "xdf_external"
    # partition is decleared as region and day. The partition can be accessed later in the code
    # block as dimensions['day'] which will give you the dataset day it processed.
    , {
        'region': {
            'type': DimensionType.STRING,
            'format': lambda dim: dim.lower(),
            'insensitive': True,
            'day': {
                'type': DimensionType.DATETIME,
                'format': '%Y%m%d'
            }
        }
    }
    , {
        '*': {
            '*': {
            }
        }
    }
)

# Datanode to process the core logic - here we just count the number of rows in each dataset
hello_world = app.create_data(id="hello_world",
                              inputs={
                                  "pdex": pdex_combine,
                                  # the datanode decleared above, the dimension of this hello_world
                                  # will be defaulted to the first inputs - pdex_combine
                                  "xdf": xdf_external['*']['*'].ref.range_check(True)
                                  # the range_check here will make sure the current region and day partition
                                  # does exists.
                              },
                              compute_targets=[
                                  BatchCompute(
                                      code=f"""
from pyspark.sql.types import *

data = [(pdex.count(),xdf.count())]
schema = StructType([ \
    StructField("pdexcount",LongType(),True), \
    StructField("xdfcount",LongType(),True)
  ])

output = spark.createDataFrame(data=data,schema=schema)
                                   """,
                                      lang=Lang.PYTHON,
                                      retry_count=0,
                                      GlueVersion="2.0",
                                      WorkerType=GlueWorkerType.G_1X.value,
                                      NumberOfWorkers=50,
                                      Timeout=90 * 60  # 90 hours
                                  )
                              ], dataset_format=DataFormat.PARQUET)

app.activate()

app.execute(hello_world['NA']['2021-08-01'])

# You will see the data in the S3 bucket under the path
# s3://if-onboarding-427809481713-us-east-1/internal_data/hello_world/NA/2021-07-30