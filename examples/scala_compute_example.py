# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

app_name = f"andes-ant-app"

flow.init_basic_logging()

region_id = 1
marketplace_id = 1

app = AWSApplication(app_name, "us-east-1")

timer = app.add_timer("adex_timer",  # node IDs should use in Python/Scala variable naming scheme (convert '-' to '_')
                      "rate(5 minutes)",
                      time_dimension_id="time")


def output_time_dim_mapper(dim):
    from datetime import timedelta
    return dim - timedelta(days=7)


def timer_to_output_dim_mapper(timer_dim):
    from datetime import timedelta
    return timer_dim + timedelta(days=7)


adex_kickoff_job = app.create_data(id="ADEX_BladeKickOffJob",
                                   inputs=[timer],
                                   input_dim_links=[],
                                   output_dimension_spec=
                                   {
                                       "region_id": {
                                           type: DimensionType.LONG,
                                           "marketplace_id": {
                                               type: DimensionType.LONG,
                                               "cutoff_date": {
                                                   type: DimensionType.DATETIME,
                                                   'format': '%Y-%m-%d'
                                                }
                                           }
                                       }
                                   },
                                   output_dim_links=[
                                       ('region_id', lambda dim: dim, region_id),
                                       ('marketplace_id', lambda dim: dim, marketplace_id),
                                       # note: timedelta is imported from 'intelliflow.api', so no runtime impor issues.
                                       ('cutoff_date', output_time_dim_mapper, timer('time')),
                                       # needed if 'execute' API will be used in the most common form where the desired
                                       # output partition will be provided only (e.g app.execute(adex_kickoff_job[1][1][DATETIME]),
                                       # which would require output('cutoff_date') to timer('time') mapping/linking.
                                       (timer('time'), timer_to_output_dim_mapper, 'cutoff_date')
                                   ],
                                   compute_targets=[
                                       BatchCompute(
                                           scala_script("""import com.amazon.dexmlbladeglue.jobs.BladeKickOffJob
                                                        print(dimensions)
                                                        val args2: Array[String] = Array("--cutoffDate", dimensions("cutoff_date").toString, "--region", "1", "--accountAlias", "beta", "--marketplaces", "1")
                                                        BladeKickOffJob.main(args.flatMap({ case (k, v) => List(k, v) }).toArray ++ args2)
                                                       """
                                                        ,
                                                        external_library_paths=[
                                                            # REPLACE with another super JAR to run the example
                                                            "s3://{JAR_BUCKET}/batch/DexmlBladeGlue-super.jar"
                                                        ]
                                                        ),
                                           lang=Lang.SCALA,
                                           GlueVersion="2.0",
                                           WorkerType=GlueWorkerType.G_1X.value,
                                           NumberOfWorkers=2,
                                           Timeout=3 * 60,  # 3 hours
                                           my_param="PARAM1",
                                           args1="v1",
                                           conf="spark.hadoop.fs.s3n.impl=com.amazon.ws.emr.hadoop.fs.EmrFileSystem"
                                       )
                                   ],
                                   dataset_format=DatasetSignalSourceFormat.PARQUET
                                   )

app.activate()

from datetime import datetime
app.execute(adex_kickoff_job[1][1][datetime.now()])


#  'time': '2021-06-24T21:26:43Z'
#   will cause execution on
#  output cutoff_date: '2021-06-17'
#app.process(
#{'version': '0', 'id': '7c289f9d-0d83-e481-8279-257acdc04fac', 'detail-type': 'Scheduled Event', 'source': 'aws.events', 'account': '842027028048', 'time': '2021-06-24T21:26:43Z', 'region': 'us-east-1', 'resources': ['arn:aws:events:us-east-1:842027028048:rule/andes-ant-app-adex-timer'], 'detail': {}}
#)
