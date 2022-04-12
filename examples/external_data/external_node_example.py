# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import time

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

app_name = f"andes-ant-app"

flow.init_basic_logging()

app = AWSApplication(app_name, "us-east-1")
app.set_security_conf(Storage, ConstructSecurityConf(
                                    persisting=ConstructPersistenceSecurityDef(
                                        ConstructEncryption(EncryptionKeyAllocationLevel.HIGH,
                                                            key_rotation_cycle_in_days=365,
                                                            is_hard_rotation=False,
                                                            reencrypt_old_data_during_hard_rotation=False,
                                                            trust_access_from_same_root=True)),
                                    passing=None,
                                    processing=None))

ducsi_data = app.marshal_external_data(external_data_desc=GlueTable("booker", "d_unified_cust_shipment_items"))
assert ducsi_data.bound.data_id == "d_unified_cust_shipment_items"

ducsi_data_explicit_import = app.marshal_external_data(
    GlueTable("booker", "d_unified_cust_shipment_items", partition_keys=["region_id", "ship_day"])
    , "DEXML_DUCSI2"
    , {
        'region_id': {
            'type': DimensionType.LONG,
            'ship_day': {
                'format': '%Y-%m-%d',
                'type': DimensionType.DATETIME
            }
        }
    }
    , {
        '1': {
            '*': {
                'timezone': 'PST',
            }
        },
        '2': {
            '*': {
                'timezone': 'UTC',
            }

        },
        '3': {
            '*': {
                'timezone': 'UTC',
            }

        },
        '4': {
            '*': {
                'timezone': 'UTC',
            }
        },
        '5': {
            '*': {
                'timezone': 'UTC',
            }
        }
    },

    SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT"]})
)
assert ducsi_data_explicit_import.bound.data_id == "DEXML_DUCSI2"

# as easy as it can get.
d_ad_orders_na = app.marshal_external_data(GlueTable("dex_ml_catalog", "d_ad_orders_na"))

# Add a glue table with explicit decl (for documentation, strictly-typed declaration). meaning that "partition_key"
# can be omitted (it can be retrieved from the catalog automatically).
d_ad_orders_na_explicit_decl = app.marshal_external_data(
    GlueTable("dex_ml_catalog", "d_ad_orders_na", partition_keys=["order_day"])
    , "d_ad_orders_na2"
    , {
        'order_day': {
            'type': DimensionType.DATETIME,
            'format': '%Y-%m-%d',
            'timezone': 'PST'
        }
    }
    , {
        '*': {}
    }
)

repeat_ducsi = app.create_data(id="REPEAT_DUCSI",
                               inputs={
                                   "DEXML_DUCSI": ducsi_data
                               },
                               compute_targets="output=DEXML_DUCSI.limit(100)")

repeat_d_ad_orders_na = app.create_data(id="REPEAT_AD_ORDERS",
                                        inputs=[d_ad_orders_na],
                                        compute_targets=[
                                            BatchCompute("output=d_ad_orders_na.limit(100)",
                                             WorkerType=GlueWorkerType.G_1X.value,
                                             NumberOfWorkers=50,
                                             GlueVersion="2.0")
                                        ])

ducsi_with_AD_orders_NA = app.create_data(id="DUCSI_WITH_AD_ORDERS_NA",
                               inputs=[
                                   d_ad_orders_na, # keep it as the first input to make this node adapt its dimension spec (due to default behaviour in AWSApplication::create_data)
                                   ducsi_data["1"]["*"]
                               ],
                               input_dim_links=[
                                   (ducsi_data('ship_day'),
                                    lambda dim: dim
                                    , d_ad_orders_na('order_day'))
                               ],
                               compute_targets=[
                                   BatchCompute("""
output=d_unified_cust_shipment_items.limit(100).join(d_ad_orders_na.limit(100), ['customer_id']).limit(10).drop(*('customer_id', 'order_day'))
                                   """,
                                                WorkerType=GlueWorkerType.G_1X.value,
                                                NumberOfWorkers=100,
                                                Timeout=3 * 60,  # 3 hours
                                                GlueVersion="3.0"
                                                )
                               ]
                               )


# experiment early (warning: will activate the application with the nodes added so far).
#materialized_output_path = app.execute(repeat_ducsi, ducsi_data[1]['2020-12-01'])

json_str = app.dev_context.to_json()
dev_context = CoreData.from_json(json_str)

app._dev_context = dev_context

app.activate(allow_concurrent_executions=False)

# trigger execution on 'ducsi_with_AD_orders_NA'
# intentionally use process on the first execution to check raw (glue table) event handling as well.
# 1- 2021-01-13
# inject synthetic 'd_ad_orders_na' into the system
app.process(
    {
        "version": "0",
        "id": "3a0fe2d2-eedc-0535-4505-96dc8e6eb33a",
        "detail-type": "Glue Data Catalog Table State Change",
        "source": "aws.glue",
        "account": "842027028048",
        "time": "2021-01-13T00:39:25Z",
        "region": "us-east-1",
        "resources": ["arn:aws:glue:us-east-1:842027028048:table/dex_ml_catalog/d_ad_orders_na"],
        "detail": {
            "databaseName": "dex_ml_catalog",
            "changedPartitions": ["[2021-01-13 00:00:00]"],
            "typeOfChange": "BatchCreatePartition",
            "tableName": "d_ad_orders_na"
        }
    }
)

app.process(ducsi_data[1]["2021-01-13"],
            # let ducsi trigger only 'ducsi_with_AD_orders_NA'
            target_route_id=ducsi_with_AD_orders_NA)
time.sleep(5)

# now hook up with the ongoing (remote) execution on node ducsi_with_AD_orders_NA
path, compute_records = app.poll(ducsi_with_AD_orders_NA["2021-01-13"])
assert path

## 2- 2021-02-13
#app.execute(ducsi_with_AD_orders_NA["2021-02-13"])

# INJECT events/signals
'''
app.process(
    {
        "version": "0",
        "id": "88b8adf0-ed56-ca29-3691-db4329d89b81",
        "detail-type": "Glue Data Catalog Table State Change",
        "source": "aws.glue",
        "account": "842027028048",
        "time": "2020-10-05T18:41:18Z",
        "region": "us-east-1",
        "resources": ["arn:aws:glue:us-east-1:842027028048:table/booker/d_unified_cust_shipment_items"],
        "detail": {
            "databaseName": "booker",
            "changedPartitions": ["[1, 2020-08-25 00:00:00, 1598383013700, 1601921779888, DELTA]",
                                  "[1, 2020-08-26 00:00:00, 1598469462125, 1601921777946, DELTA]",
                                  "[1, 2020-08-27 00:00:00, 1598555275159, 1601921779117, DELTA]",
                                  "[3, 2020-10-05 00:00:00, 1601921790685, 1601921790685, SNAPSHOT]"],
            "typeOfChange": "BatchCreatePartition",
            "tableName": "d_unified_cust_shipment_items"
        }
    }
)

# app.process(ducsi_data[1]['2020-10-01'])
# OR
# app.process(
#     {
#         'version': '0',
#         'id': '476839e2-051f-4c1d-599e-19434ac1f737',
#         'detail-type': 'Glue Data Catalog Table State Change',
#         'source': 'aws.glue',
#         'account': '842027028048',
#         'time': '2020-10-09T15:20:26Z',
#         'region': 'us-east-1',
#         'resources': ['arn:aws:glue:us-east-1:842027028048:table/booker/d_unified_cust_shipment_items'],
#         'detail': {
#             'databaseName': 'booker',
#             'changedPartitions': ['[1, 2020-09-26 00:00:00, 1601215697495, 1602255290552, DELTA]',
#                                   '[1, 2020-09-27 00:00:00, 1601302736273, 1602255291770, DELTA]',
#                                   '[1, 2020-09-28 00:00:00, 1601389766872, 1602255291002, DELTA]',
#                                   '[1, 2020-09-29 00:00:00, 1601475165167, 1602255289080, DELTA]',
#                                   '[1, 2020-09-30 00:00:00, 1601562078445, 1602255290910, DELTA]',
#                                   '[1, 2020-10-01 00:00:00, 1601650941217, 1602255290101, DELTA]',
#                                   '[1, 2020-10-02 00:00:00, 1601735067793, 1602255291571, DELTA]',
#                                   '[1, 2020-10-03 00:00:00, 1601819116332, 1602255291101, DELTA]',
#                                   '[1, 2020-10-04 00:00:00, 1601905874938, 1602255291435, DELTA]',
#                                   '[1, 2020-10-05 00:00:00, 1601993633608, 1602255294240, DELTA]',
#                                   '[1, 2020-10-06 00:00:00, 1602084066586, 1602255293955, DELTA]',
#                                   '[1, 2020-10-07 00:00:00, 1602187865755, 1602255296624, DELTA]',
#                                   '[1, 2020-10-08 00:00:00, 1602255327381, 1602255327381, SNAPSHOT]'],
#             'typeOfChange': 'BatchCreatePartition',
#             'tableName': 'd_unified_cust_shipment_items'
#         }
#     }
# )

# app.process(repeat_dshipoption[1])
'''

app.pause()
