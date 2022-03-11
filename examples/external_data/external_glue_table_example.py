# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

logger = flow.init_basic_logging()

app = AWSApplication("i-fanout", "us-east-1")

o_transits = app.glue_table("clickstream", "o_transits",
                            # overwrite dimension_spec to specify date partition's format
                            # by default it might use the full/raw spec (e.g %Y-%m-%d %H:%M:%S) depending on incoming
                            # event's value or direct user partition input via execute/process API.
                            dimension_spec={
                                "region_id": {
                                    type: DimensionType.LONG,
                                    "day": { # does not have to be the original partition name ('first_viewed_page_day')
                                        type: DimensionType.DATETIME,
                                        "format": "%Y-%m-%d"
                                    }
                                }
                            }
                            )
# This example shows how you can alternatively access the table from the catalog in a direct way.
#  -commented SparkSQL block uses IF compacted, managed dataframe
#  - active SparkSQL directly accesses the table so it has its own compaction logic to get the latest snapshot of the
# table.
o_transits_filtered = app.create_data(id="test_o_transits",
                                 inputs=[o_transits],
                                 compute_targets=[
                                     SparkSQL(f"""
                                     
                                         SELECT marketplace_id, first_viewed_page_datetime, last_viewed_page_datetime, session_id, last_known_customer_id, page_view_count, glance_view_count, landing_page_type, purchases_count
                                          from o_transits
                                          where
                                            marketplace_id = 1 and
                                              purchases_count != 0 and
                                               is_robot = 0 and
                                                 traffic_channel_id != 900 and
                                                   session_id is not NULL and
                                                     is_internal = 0
                                     
                                              """,
                                         WorkerType=GlueWorkerType.G_1X.value,
                                         NumberOfWorkers=250,
                                         Timeout=4 * 60,  # 3 hours
                                         GlueVersion="1.0"
                                     )
                                 ])

# This example shows how you can alternatively access the table from the catalog in a direct way.
# Pyspark code here directly accesses the table so it has its own compaction logic to get the latest snapshot of the
# table.
o_transits_filtered_direct_access = app.create_data(id="test_o_transits_direct",
                                      inputs=[o_transits],
                                      compute_targets=[
                                          BatchCompute(code="""
region_id = dimensions['region_id']
date = dimensions['day']  # [:10]

output=spark.sql(f'''
                                    SELECT * FROM (
                                            SELECT marketplace_id, first_viewed_page_datetime, last_viewed_page_datetime, session_id, last_known_customer_id, page_view_count, glance_view_count, landing_page_type, purchases_count,
                                                   rank() OVER (PARTITION BY region_id, first_viewed_page_day ORDER BY ecs_snapshot DESC) as rnk
                                              FROM clickstream.o_transits
                                              where
                                                region_id = {region_id} and
                                                first_viewed_page_day = CAST('{date}' AS TIMESTAMP) and
                                                  marketplace_id = 1 and
                                                    purchases_count != 0 and
                                                     is_robot = 0 and
                                                       traffic_channel_id != 900 and
                                                         session_id is not NULL and
                                                           is_internal = 0
                                        ) subquery
                                        where subquery.rnk=1
                 ''')
                                        """,
                                                       WorkerType=GlueWorkerType.G_1X.value,
                                                       NumberOfWorkers=250,
                                                       Timeout=4 * 60,  # 3 hours
                                                       GlueVersion="1.0"
                                                       )
                                      ])

# early test
# region_id -> 1, first_viewed_page_day -> "2022-02-15"
# app.execute(o_transits_filtered[1]["2022-02-16"])

app.execute(o_transits_filtered_direct_access[1]["2022-02-15"])

atrops_o_slam_packages = app.marshal_external_data(GlueTable(database="atrops_ddl", table_name="o_slam_packages"))
# RheocerOS internal node_id is 'table_name' by default (since 'id' param to marshal_external_data is left None).
assert atrops_o_slam_packages.bound.data_id == "o_slam_packages"

encryption_key = "arn:aws:kms:us-east-1:123456789012:key/aaaaaaaa-bbbb-cccc-8888-111111222222"

# will fail due to missing encryption_key (because Glue Catalog has the metadata that indicates that it is encrypted)
try:
    app.marshal_external_data(external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches"),
                              id="my_tommy_searches")
    assert False, "shoul fail due to missing 'encryption_key'"
except ValueError:
    pass

tommy_searches = app.marshal_external_data(external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches",
                                                                        encryption_key=encryption_key),
                                           id="my_tommy_searches")

tommy_desc = tommy_searches['us']['20210824'].describe()
print(tommy_desc)

atrops_desc = atrops_o_slam_packages.describe()
print(atrops_desc)


assert tommy_searches.bound.data_id == "my_tommy_searches"

tommy_searches = app.marshal_external_data(
                            # always checks the catalog
                            external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches",
                                                         encryption_key=encryption_key)
                            , dimension_filter ={  # carries the type info as well (partition names will be from catalog)
                                '*': {
                                    'type': DimensionType.STRING,
                                    'format': lambda dim: dim.lower(),
                                    # their Glue events and S3 path uses org partition in a case insensitive way (declare here so that IF knows about it).
                                    'insensitive': True,
                                    '*': {
                                        'type': DimensionType.DATETIME,
                                        'format': '%Y-%m-%d'  # internally convert search partition format %Y%m%d
                                    }
                                }
                            })

tommy_searches2 = app.marshal_external_data(
    # always checks the catalog
    external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches",
                                 encryption_key=encryption_key)
    , id="tommy_searches2"
    , dimension_filter ={  # missing first partition name ('org') will be fetched from catalog
        '*': {
            'type': DimensionType.STRING,
            'format': lambda dim: dim.lower(),
            # their Glue events and S3 path uses org partition in a case insensitive way (declare here so that IF knows about it).
            'insensitive': True,
            '*': {
                'name': 'part_date',  # catalog has this as 'partition_date' but user's choice will be honored here
                'type': DimensionType.DATETIME,
                'format': '%Y-%m-%d'  # internally convert search partition format %Y%m%d
            }
        }
    })

try:
    app.marshal_external_data(
        # always checks the catalog
        external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches",
                                     encryption_key=encryption_key)
        , id="tommy_searches3"
        , dimension_spec ={  # missing first partition type (STRING) will be fetched from catalog
            'org': {
                'part_date': {
                    'type': DimensionType.DATETIME, # this will be honored (catalog as this dimension as STRING)
                    'format': '%Y-%m-%d'
                }
            }
        })
    assert False, "should fail due to missing 'type' for 'org' dimension!"
except KeyError:
    pass

# this dataset is important, since Glue Table proxy should be used at runtime. publisher adds new partitions
tommy_searches4 = app.marshal_external_data(
                            # always checks the catalog
                            external_data_desc=GlueTable(database="searchdata", table_name="tommy_searches",
                                                          encryption_key=encryption_key)
                            , id="tommy_searches4"
                            , dimension_spec={
                                'org': {
                                    'type': DimensionType.STRING,
                                    'format': lambda dim: dim.lower(),
                                    # their Glue events and S3 path uses org partition in a case insensitive way (declare here so that IF knows about it).
                                    'insensitive': True,
                                    'partition_date': {
                                        'type': DimensionType.DATETIME,
                                        'format': '%Y-%m-%d'  # internally convert search partition format %Y%m%d
                                    }
                                }
                            }
                        )

ducsi_data = app.marshal_external_data(
    # checks the catalog and tries to do validation and compensates missing data.
    external_data_desc=GlueTable("booker", "d_unified_cust_shipment_items",
                                # primary_keys will be auto-retrieved from the catalog
                                #
                                table_type="APPEND"),
    id= "DEXML_DUCSI",
    # 'dimension spec' will be auto-generated based on the names and types of partition keys from the catalog
    protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT"]})
)

d_ad_orders_na = app.marshal_external_data(
                    external_data_desc=GlueTable("dex_ml_catalog", "d_ad_orders_na")
                    # this spec will be auto-generated using info from Glue catalog
                    #{
                    #    'order_day': {
                    #        'type': DimensionType.DATETIME,
                    #    }
                    #},
)


tommy_searches_filtered = app.create_data(id="TOMMY_SEARCHES_FILTERED",
                                          inputs=[tommy_searches],
                                          compute_targets=[
                                              BatchCompute(
                                                  "output = tommy_searches.limit(5)",
                                                  WorkerType=GlueWorkerType.G_1X.value,
                                                  NumberOfWorkers=20,
                                                  GlueVersion="2.0")
                                          ])

repeat_d_ad_orders_na = app.create_data(id="REPEAT_AD_ORDERS",
                                        inputs=[d_ad_orders_na],
                                        compute_targets=[
                                            BatchCompute(
                                                "output=d_ad_orders_na.limit(100)",
                                                 WorkerType=GlueWorkerType.G_1X.value,
                                                 NumberOfWorkers=50,
                                                 GlueVersion="2.0")
                                        ])

app.activate()

# inject tommy_searches event
app.process(tommy_searches['us']['20210914'])
# expect successful operations on its dependencies
path, _ = app.poll(tommy_searches_filtered['us']['20210914'])
assert path

app.pause()

