# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

'''
This example demonstrates the import of two different datasets into an application and their advanced dimension linking
to control event based trigger at runtime.
'''
import time
from datetime import timedelta

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

from intelliflow.utils.test.inlined_compute import NOOPCompute

logger = flow.init_basic_logging()

# automatically reads default credentials
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
app = AWSApplication("i-fanout", "us-east-1")

encryption_key = "arn:aws:kms:us-east-1:800261124827:key/5be55530-bb8e-4e95-8185-6e1afab0de54"

tommy_external = app.marshal_external_data(
    external_data_desc= S3Dataset("800261124827", "a9-sa-data-tommy-datasets-prod", "tommy-asin-parquet-hourly", "{}", "{}", "{}",
                                  dataset_format=DataFormat.PARQUET, encryption_key=encryption_key)
                        .link(
                            # Link the PROXY !!!
                            # events coming from this proxy/link will yield valid Signals/triggers in the system
                            GlueTable(database="searchdata", table_name="tommy_asin_hourly")
                        )
    , id="tommy_external"
    , dimension_spec={
        'org': {
            'type': DimensionType.STRING,
            'format': lambda org: org.lower(), # make sure that this dimension exposes itself as 'lower case' (so that s3 path is contructed correctly)
            'insensitive': True,  # incoming events from Glue might have UPPER case strings
            'day': {
                'type': DimensionType.DATETIME,
                'format': '%Y%m%d',
                'hour': {
                    'type': DimensionType.LONG,
                }
            }
        }
    }
)


pdex = app.marshal_external_data(GlueTable("DEX-ML-BETA", "pdex_logs"), id="pdex_external")

tommy_hourly = app.create_data(id=f"C2P_TOMMY_DATA",
                               inputs={"tommy_external": tommy_external,
                                       # since this is 'ref', incoming pdex events are not required for execution trigger. tommy events
                                       # will determine the trigger and now from each tommy event (and its dimension values [region, day, hour])
                                       # pdex partition must be inferred at runtime. So please check the input_dim_linking to understand how.
                                       "pdex_external": pdex.ref.range_check(True)
                                       },
                               input_dim_links=[
                                   # N -> 1 mapping, tommy(day, hour) -> pdex(dataset_date)
                                   (
                                       pdex('dataset_date'),  # DESTINATION/TO

                                       # notice how two dimension [day, hour] declaration on SOURCE (tommy_external)
                                        # changed the signature of the mapper lambda function here.
                                        #
                                        # lambda argument types:
                                        #  - day <datetime>: since tommy 'day' dimension is declared as DATETIME,
                                        #  - hour <int> : since tommy 'hour' is declared as LONG above in marshal_external_data call for tommy_external
                                        #
                                        # so the mapper is very intuitive
                                       lambda day, hour: day + timedelta(hours=int(str(hour)[8:])) + timedelta(hours=-8),
                                       # MAPPER/OPERATOR

                                       tommy_external('day', 'hour')  # SOURCE/FROM
                                   )
                               ],
                               compute_targets=[
                                   NOOPCompute  # do nothing
                               ])

# alternative runtime trigger semantics #1
tommy_hourly_PDEX_as_PIVOT = app.create_data(id=f"C2P_TOMMY_DATA_triggered_by_PDEX",
                               inputs={
                                       "pdex_external": pdex,
                                       "tommy_external": tommy_external.ref.range_check(True)
                                       },
                               input_dim_links=[
                                   # reverse mapping from 'tommy_hourly' input_dim_links above.
                                   # at runtime PDEX signal will be the execution trigger so IF must be able to infer
                                   # which tommy_external partition to materialize from PDEX dataset_date.
                                   (
                                       tommy_external('hour'),  # DESTINATION/TO

                                       lambda dataset_date: str((dataset_date + timedelta(hours=8)).hour).zfill(2),
                                       # MAPPER/OPERATOR

                                       pdex('dataset_date')  # SOURCE/FROM
                                   ),
                                   (
                                       tommy_external('day'),  # DESTINATION/TO

                                       lambda dataset_date: dataset_date,

                                       pdex('dataset_date')  # SOURCE/FROM
                                   )
                               ],
                               compute_targets=[
                                   NOOPCompute
                               ])

# alternative runtime trigger semantics #2
tommy_hourly_TWO_EVENTS = app.create_data(id=f"C2P_TOMMY_DATA_STRICT",
                                             inputs={
                                                 # events from two inputs must be received to trigger an event
                                                 "pdex_external": pdex,
                                                 "tommy_external": tommy_external
                                             },
                                             input_dim_links=[
                                                 # N -> 1 mapping, tommy(day, hour) -> pdex(dataset_date)
                                                 (
                                                     pdex('dataset_date'),  # DESTINATION/TO

                                                     # notice how two dimension [day, hour] declaration on SOURCE (tommy_external)
                                                     # changed the signature of the mapper lambda function here.
                                                     #
                                                     # lambda argument types:
                                                     #  - day <datetime>: since tommy 'day' dimension is declared as DATETIME,
                                                     #  - hour <int> : since tommy 'hour' is declared as LONG above in marshal_external_data call for tommy_external
                                                     #
                                                     # so the mapper is very intuitive
                                                     lambda day, hour: day + timedelta(hours=hour) + timedelta(hours=-8),
                                                     # MAPPER/OPERATOR

                                                     tommy_external('day', 'hour')  # SOURCE/FROM
                                                 ),
                                                 (
                                                     tommy_external('hour'),  # DESTINATION/TO

                                                     lambda dataset_date: str((dataset_date + timedelta(hours=8)).hour).zfill(2),
                                                     # MAPPER/OPERATOR

                                                     pdex('dataset_date')  # SOURCE/FROM
                                                 ),
                                                 (
                                                     tommy_external('day'),  # DESTINATION/TO

                                                     lambda dataset_date: dataset_date,

                                                     pdex('dataset_date')  # SOURCE/FROM
                                                 )
                                             ],
                                             compute_targets=[
                                                 NOOPCompute
                                             ])

app.activate()

# synthetically inject tommy hourly event into the system
# Emulate raw Glue Table partition change event.
# Synchronously process (without async remote call to Processor [Lambda])
app.process(
    {
        "version": "0",
        "id": "3a1926d0-2de1-fb1b-9741-cad729bb7cb4",
        "detail-type": "Glue Data Catalog Table State Change",
        "source": "aws.glue",
        "account": "427809481713",
        "time": "2021-11-22T18:57:01Z",
        "region": "us-east-1",
        "resources": ["arn:aws:glue:us-east-1:427809481713:table/searchdata/tommy_asin_hourly"],
        "detail": {
            "databaseName": "searchdata",
            "changedPartitions": ["[SE, 20211122, 2021112200]"],
            "typeOfChange": "BatchCreatePartition",
            "tableName": "tommy_asin_hourly",
        }
    }
)

path, compute_records = app.poll(tommy_hourly['se']['20211122']['2021112200'])
assert path
time.sleep(5)

# check other node has not been triggered (it is specifically waiting for PDEX signal)
path, _ = app.poll(tommy_hourly_PDEX_as_PIVOT['20211122']['2021112200'])
assert not path

# third node needs two events to be received. so verify that it has not been triggered
path, _ = app.poll(tommy_hourly_TWO_EVENTS['20211122']['2021112200'])
assert not path

# synthetically inject PDEX event into the system
app.process(pdex["2021-11-22T00"])

# now second node should be complete
path, _ = app.poll(tommy_hourly_PDEX_as_PIVOT['20211122']['2021112200'])
assert path  # EXECUTED!
# third node should also be complete
path, _ = app.poll(tommy_hourly_TWO_EVENTS['20211122']['2021112200'])
assert path  # EXECUTED!
