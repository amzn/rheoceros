# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

flow.init_basic_logging()

app = AWSApplication("dex-catalog", "us-east-1")

app.authorize_downstream("catalog-client", '842027028048', 'us-east-1')
app.authorize_downstream("catalog-client-2", '842027028048', 'us-east-1')

ducsi_data = app.marshal_external_data(
    GlueTable("booker", "d_unified_cust_shipment_items",
                 # optional actually (can be retrieved automatically but explicitly declared here for
                 # - early detection of incompatible changes in the catalog
                 # - better readibility
                 partition_keys=["region_id", "ship_day"],
                 # metadata
                 CTI="Delivery Experience/Machine Learning/Checkout",
                 Columns={
                    "order_day" : {
                        'description': '...',
                        'notes': {
                            '@random_guy': 'Please check my note in in this wiki doc: ...'
                         }
                    }
                 },
                 Privacy="Critical"
                 )
    ,id="DEXML_DUCSI_NA"
    , dimension_filter={
        '1': {  # allow region_id=1 to be int downstream
            '*': {  # any date
                'timezone': 'PST',
            }
        },
        '2': {
            '*': {
                'timezone': 'GMT',
            }

        },
        '3': {
            '*': {
                'timezone': 'GMT',
            }

        },
        '4': {
            '*': {
                'timezone': 'GMT',
            }
        },
        '5': {
            '*': {
                'format': '%Y-%m-%d',
                'timezone': 'GMT',
            }
        }
    }
)

app.activate()
