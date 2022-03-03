# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from pprint import pprint

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

flow.init_basic_logging()

app = AWSApplication("catalog-client", "us-east-1")

# import catalog application so that its nodes will be visible via app or catalog_app objects and bindable into
# subsequent API calls as inputs (e.g create_data)
catalog_app = app.import_upstream("dex-catalog",'842027028048', 'us-east-1')

all_data_from_catalog = catalog_app.list_data()
for catalog_data in all_data_from_catalog:
    pprint(catalog_data.access_spec())
    pprint(catalog_data.domain_spec())

dexml_data = catalog_app.query_data("DEXML")
pprint(dexml_data)

ducsi_data = catalog_app.get_data("DEXML_DUCSI")[0]
access_spec = ducsi_data.access_spec()
dimension_spec = ducsi_data.dimension_spec()

filtered_ducsi = app.create_data("REPEAT_DUCSI",
                               {
                                   "DEXML_DUCSI": ducsi_data["*"][:-3]
                               },
                               [],
                               {
                                   'region_id': {
                                       'type': DimensionType.LONG,
                                       'ship_day': {
                                           'type': DimensionType.DATETIME
                                       }
                                   }
                               },
                               [
                                   ('region_id', lambda dim: dim, ducsi_data('region_id')),
                                   ('ship_day', lambda dim: dim, ducsi_data('ship_day'))

                               ],
                               [
                                   GlueBatchCompute(
                                                    "spark.sql('select * customer_id from DEXML_DUCSI')"
                                                    )
                               ]
                               )

app.activate()

# app.process(
#     {
#         "version": "0",
#         "id": "88b8adf0-ed56-ca29-3691-db4329d89b81",
#         "detail-type": "Glue Data Catalog Table State Change",
#         "source": "aws.glue",
#         "account": "842027028048",
#         "time": "2020-10-05T18:41:18Z",
#         "region": "us-east-1",
#         "resources": ["arn:aws:glue:us-east-1:842027028048:table/booker/d_unified_cust_shipment_items"],
#         "detail": {
#             "databaseName": "booker",
#             "changedPartitions": ["[1, 2020-08-25 00:00:00, 1598383013700, 1601921779888, DELTA]",
#                                   "[1, 2020-08-26 00:00:00, 1598469462125, 1601921777946, DELTA]",
#                                   "[1, 2020-08-27 00:00:00, 1598555275159, 1601921779117, DELTA]",
#                                   "[3, 2020-10-05 00:00:00, 1601921790685, 1601921790685, SNAPSHOT]"],
#             "typeOfChange": "BatchCreatePartition",
#             "tableName": "d_unified_cust_shipment_items"
#         }
#     }
# )

app.process(ducsi_data[1]['2020-10-01'])
# app.process(repeat_dshipoption[1])
