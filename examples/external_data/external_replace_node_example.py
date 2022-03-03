# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *


app_name = f"andes-replace"

flow.init_basic_logging()

app = AWSApplication(app_name, "us-east-1")

o_slam_pkg = app.marshal_external_data(
    GlueTable("atrops_ddl", "o_slam_packages")
    # optionally overwrite the 'id', if not specified then 'o_slam_packages' from the catalog is used by default
    , "o_slam_pkg"
    # add dimension spec for partition keys.
    #  - partition names can be overwritten here, rest of the application will use the dimension names from this spec
    # as the partition keys
    #  - if this is not compatible with the partition keys from the catalog then this call will raise
    , {
        'region_id': {
            'type': DimensionType.LONG,
            'request_date': {
                'type': DimensionType.DATETIME,
                'format': '%Y-%m-%d',
            }
        }
    }
    , {
        '1': {
            '*': {
                'timezone': 'PST',
            }
        }
    },
    SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT"]})
)

ship_options = app.marshal_external_data(
    GlueTable("dexbi", "d_ship_option"),
    "ship_options"  # optionally overwrite the 'id', if not specified then 'd_ship_option' is used by default
)

repeat_slam = app.create_data(id="repeat_slam",
                              inputs={
                                  "o_slam_pkg": o_slam_pkg
                              },
                              compute_targets=[
                                BatchCompute(
                                    code="output = o_slam_pkg.limit(100)",
                                    WorkerType=GlueWorkerType.G_1X.value,
                                    NumberOfWorkers=100,
                                    GlueVersion="1.0"
                                      )])

repeat_ship_option = app.create_data(id="repeat_ship_option",
                              inputs={
                                  "ship_options": ship_options
                              },
                              compute_targets=[
                                BatchCompute(
                                    code="output=ship_options.where((col(\"marketplace_id\") == 1) & (col(\"region_id\") == 1))",
                                    WorkerType=GlueWorkerType.G_1X.value,
                                    NumberOfWorkers=100,
                                    GlueVersion="1.0"
                                      )])

app.activate(allow_concurrent_executions=True)

app.process(
    {
        "version": "0",
        "id": "03d4df04-f8c1-f231-eb10-e585f826a6d0",
        "detail-type": "Glue Data Catalog Table State Change",
        "source": "aws.glue",
        "account": "427809481713",
        "time": "2021-05-28T06:03:13Z",
        "region": "us-east-1",
        "resources": ["arn:aws:glue:us-east-1:427809481713:table/atrops_ddl/o_slam_packages"],
        "detail": {
            "databaseName": "atrops_ddl",
            "changedPartitions": ["[1, 2021-05-23 00:00:00, 1622181290861, 1622181290861, SNAPSHOT]"],
            "typeOfChange": "BatchCreatePartition",
            "tableName": "o_slam_packages"
        }
    }
)


# app.process(
#     {
#         "version": "0",
#         "id": "cad7770d-abc5-2f3d-757f-7d6011f9cf8b",
#         "detail-type": "Glue Data Catalog Table State Change",
#         "source": "aws.glue",
#         "account": "427809481713",
#         "time": "2021-05-24T09:21:46Z",
#         "region": "us-east-1",
#         "resources": ["arn:aws:glue:us-east-1:427809481713:table/dexbi/d_ship_option"],
#         "detail": {
#             "databaseName": "dexbi",
#             "changedPartitions": ["[1621847744107, 1621847744107, SNAPSHOT]"],
#             "typeOfChange": "BatchCreatePartition",
#             "tableName": "d_ship_option"
#         }
#     }
# )

app.pause()


