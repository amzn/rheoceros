# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

flow.init_basic_logging()

app = AWSApplication("scala-python", "us-east-1")

d_ad_orders_na = app.glue_table("dex_ml_catalog", "d_ad_orders_na")

repeat_d_ad_orders_na_scala = app.create_data(id="REPEAT_AD_ORDERS_IN_SCALA",
                                        inputs=[d_ad_orders_na],
                                        compute_targets=[
                                            BatchCompute(
                                                scala_script("d_ad_orders_na.limit(100)"
                                                             #, external_library_paths=["s3://test-dexml-blade-beta/lib/DexmlBladeGlue-super.jar"]
                                                             ),
                                                lang=Lang.SCALA,
                                                GlueVersion="3.0",
                                                WorkerType=GlueWorkerType.G_1X.value,
                                                NumberOfWorkers=50,
                                                Timeout=3 * 60  # 3 hours
                                            )
                                        ])

repeat_d_ad_orders_na_python = app.create_data(id="REPEAT_AD_ORDERS_IN_PYTHON",
                                               inputs=[d_ad_orders_na],
                                               compute_targets=[
                                                   BatchCompute(
                                                       "output = d_ad_orders_na.limit(100)",
                                                       GlueVersion="3.0",
                                                       WorkerType=GlueWorkerType.G_1X.value,
                                                       NumberOfWorkers=50,
                                                       Timeout=3 * 60  # 3 hours
                                                   )
                                               ])

'''
repeat_d_ad_orders_na_scala_complex = app.create_data(id="REPEAT_AD_ORDERS_IN_SCALA_COMPLEX",
                                              inputs=[d_ad_orders_na],
                                              compute_targets=[
                                                  BatchCompute(
                                                      scala_script("""import com.amazon.dexmlbladeglue.jobs.TrendFeaturesJob
                                                                 
                                                                    val order_day = dimensions("order_day")
                                                                    val my_param = args("my_param")
                                                                    // input dataframes are available both as local variables and also within
                                                                    // the spark context
                                                                    // other variables are also available (spark, inputs, inputTable, execParams)
                                                                    TrendFeatureWorker.main(args)
                                                                   """
                                                                   , external_library_paths=["s3://test-dexml-blade-beta/lib/DexmlBladeGlue-super.jar"]
                                                                   ),
                                                      lang=Lang.SCALA,
                                                      GlueVersion="2.0",
                                                      WorkerType=GlueWorkerType.G_1X.value,
                                                      NumberOfWorkers=50,
                                                      Timeout=3 * 60,  # 3 hours
                                                      my_param="PARAM1"
                                                  )
                                              ])
'''

app.activate(allow_concurrent_executions=False)

app.process(d_ad_orders_na["2021-01-14"])

app.poll(repeat_d_ad_orders_na_scala["2021-01-14"])
app.poll(repeat_d_ad_orders_na_python["2021-01-14"])

app.pause()

