# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
import logging

log = flow.init_basic_logging(root_level=logging.WARN)

app = AWSApplication("dxg-proto", "us-east-1")

timer_signal_daily = app.add_timer(id="daily_timer"
                                   # https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-create-rule-schedule.html#rate-expressions
                                   , schedule_expression="rate(1 day)"  # you can use CRON tab here as well
                                   , time_dimension_id="day")

pdex = app.marshal_external_data(
    external_data_desc=S3Dataset("111222333444",
                                 "dxg-dev-external-data-poc",
                                 "pdex",
                                 # placeholder for 'region'
                                 "{}",
                                 # placeholder for 'day'
                                 "{}",
                                 dataset_format=DataFormat.JSON)
    # in spark code, this will be the default DataFrame name/alias (but you can overwrite it in 'inputs' if you use map instead of list)
    , id="pdex"
    , dimension_spec= {
          # partition 1
          'region': {
              'type': DimensionType.STRING,

              # partition 2
              'day': {
                  'type': DimensionType.DATETIME,
                  'granularity': DatetimeGranularity.DAY,  # redundant because IF's default is day
                  'format': '%Y-%m-%d' # format of day partition in resource (s3) path, e.g 2021-06-21
              }

          }
      }
    # this dataset currently does not use any 'completion' file.
    # But declaring this will at least eliminate any update from the upstream partition causing event processing/trigger in this pipeline.
    # By using this today, framework will not allow this to be used in an event-based manner. why? because this file '_SUCCESS' is
    # not being generated today. If we don't declare this and use 'pdex' signal in an event-based manner, it will cause
    # event ingestion (and evaluation for) for each partition file! Idempotency is there but if events are spread out
    # then they might cause re-calculations.
    , protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)

tpdex = app.create_data(id="tpdex",  # default DataFrame ID in downstream nodes (if not overwritten)
                        inputs=[pdex['NA']['*'].ref.range_check(True),
                                timer_signal_daily],
                        # ref -> dont wait for a new event, range_check: make sure []
                        compute_targets=[
                            BatchCompute(
                                scala_script("""                                 

                                          pdex
                                            .filter(
                                              (col("addressId") isNotNull)
                                                && (col("requestId") isNotNull)
                                                && (col("asin") isNotNull)
                                                && (col("entityId") isNotNull)
                                                && (col("deliveryOption") isNotNull)
                                            )
                                            .select(
                                              pdex.col("*"),
                                              explode(col("deliveryOption")).as("flatDeliveryOption")
                                            )
                                            .select(
                                              "datestamp",
                                              "entityId",
                                              "requestId",
                                              "marketplaceId",
                                              "asin",
                                              "sessionId",
                                              "merchant_id",
                                              "client_id",
                                              "addressId",
                                              "offerSku"
                                            )
                                            .distinct()
                                               """
                                             # ,
                                             # external_library_paths=["s3://BUCKET_FOR_JAR/FOLDER/SUPER-JAR-NAME.jar"]
                                             ),
                                lang=Lang.SCALA,
                                GlueVersion="2.0",
                                WorkerType=GlueWorkerType.G_1X.value,
                                NumberOfWorkers=50
                            )
                        ])

app.activate()

app.process(timer_signal_daily['2021-09-20'], target_route_id=tpdex)

app.poll(tpdex['NA']['2021-09-20'])

# app.execute(tpdex['NA']['2021-09-20'])


