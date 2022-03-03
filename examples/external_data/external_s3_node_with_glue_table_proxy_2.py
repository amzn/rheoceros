# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

logger = flow.init_basic_logging()

app = AWSApplication("i-fanout", "us-east-1")

encryption_key = "arn:aws:kms:us-east-1:800261124827:key/5be55530-bb8e-4e95-8185-6e1afab0de54"

tommy_searches = app.glue_table(database="searchdata", table_name="tommy_searches", encryption_key=encryption_key)

# alternative import of same dataset
# differences:
#  - allows case insensitve use of 'org' partition for user convenience
#    E.g
#      app.execute(tommy_searches_filtered['uS']['20220206'])
#      or
#      app.execute(tommy_searches_filtered['US']['20220206'])
#
#  - transforms 'partition_data' string typed partition to DATETIME so that RheocerOS range operations would work
#    e.g create intermediate data to use last 7 days of tommy_searches
#        aggregated_data = app.create_data("aggregation",
#                                          inputs=[tommy_searches_explicit[:-7]]
#                                          ...
# this would not be possible with 'tommy_searches' since it has 'partition_date' as string in catalog
tommy_searches_explicit = app.marshal_external_data(
    external_data_desc= GlueTable(database="searchdata", table_name="tommy_searches", encryption_key=encryption_key)
    , id="tommy_searches_explicit"
    , dimension_spec={
        'org': {
            'type': DimensionType.STRING,
            # think about this as a 'getter'. allow users to use lower/upper/mixed case letters to define 'org' ('us, 'US')
            # when calling 'execute' 'process' APIs
            # this makes sure that actual physical path will have this dimension in UPPER case
            'format': lambda dim: dim.upper(),
            'insensitive': True,
            'partition_date': {
                #
                'type': DimensionType.DATETIME,
                'format': '%Y%m%d'  # we have to specify this because this is how it is formatted in the catalog and in actual physical path (S3)
            }
        }
    }
)

compute_params = {
    "WorkerType": GlueWorkerType.G_2X.value,
    "NumberOfWorkers": 100,
    "GlueVersion": "3.0"
}

tommy_searches_filtered = app.create_data(id="TOMMY_SEARCHES_FILTERED",
                                          inputs=[tommy_searches],
                                          compute_targets=[
                                              BatchCompute(
                                                  "output = tommy_searches.sample(0.0000001)",
                                                  **compute_params
                                              )
                                          ])

tommy_searches_filtered_SCALA = app.create_data(id="TOMMY_SEARCHES_FILTERED_SCALA",
                                                inputs=[tommy_searches],
                                                compute_targets=[
                                                    BatchCompute(
                                                        "tommy_searches.sample(0.00000001)",
                                                        lang=Lang.SCALA,
                                                        **compute_params
                                                    )
                                                ])

tommy_searches_filtered_PRESTO = app.create_data(id="TOMMY_SEARCHES_FILTERED_PRESTO",
                                                 inputs=[tommy_searches],
                                                 compute_targets=[
                                                     PrestoSQL("select * from tommy_searches LIMIT 5")
                                                 ])

app.activate()

#inject tommy_searches event
#use this to execute all of the intermediate nodes at once, so
#tommy_searches_filtered , tommy_searches_filtered_SCALA, tommy_searches_filtered_PRESTO
#would be executed in parallel.
app.process(tommy_searches['US']['20220215'])
#
#poll the output of its dependencies
app.poll(tommy_searches_filtered['US']['20220215'])
app.poll(tommy_searches_filtered_SCALA['US']['20220215'])
app.poll(tommy_searches_filtered_PRESTO['US']['20220215'])

# targeted execution (will only execute 'tommy_searches_filtered'
# app.execute(tommy_searches_filtered['US']['20220206'])
app.execute(tommy_searches_filtered_SCALA['US']['20220206'])

# if 'tommy_searches_exp' is used, then user can input 'org' dimension in a case insensitive way
# app.execute(tommy_searches_filtered['us']['20220206'])

# use this to execute all of the intermediate nodes at once, so
#  tommy_searches_filtered , tommy_searches_filtered_SCALA, tommy_searches_filtered_PRESTO
# would be executed in parallel.
app.process(tommy_searches['us']['20210914'])

# expect successful operations on its dependencies
path, _ = app.poll(tommy_searches_filtered['us']['20210914'])
assert path
path, _ = app.poll(tommy_searches_filtered_SCALA['us']['20210914'])
assert path
path, _ = app.poll(tommy_searches_filtered_PRESTO['us']['20210914'])
assert path

app.pause()

