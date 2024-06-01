# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import time

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.constructs import CompositeBatchCompute
from intelliflow.core.platform.drivers.compute.aws import AWSGlueBatchComputeBasic
from intelliflow.core.platform.drivers.compute.aws_emr import AWSEMRBatchCompute, RuntimeConfig
from intelliflow.core.platform.drivers.compute.aws_sagemaker.training_job import AWSSagemakerTrainingJobBatchCompute
from intelliflow.core.platform.drivers.compute.aws_sagemaker.transform_job import AWSSagemakerTransformJobBatchCompute
from intelliflow.core.platform.drivers.processor.aws import AWSLambdaProcessorBasic
from intelliflow.utils.test.inlined_compute import NOOPCompute

app_name = "regional-timer"
log = logging.getLogger(app_name)

flow.init_basic_logging()
flow.init_config()

app = AWSApplication(app_name, "us-east-1", "720967891397")

daily_timer = app.add_timer("DAILY_TIMER",
                            schedule_expression="cron(0 0 1 * ? *)",  # Run at 00:00 am (UTC) every 1st day of the month
                            time_dimension_id="date")

regionalized_timer = app.project("REGIONAL_DAILY_TIMER",
                                 input=daily_timer,
                                 output_dimension_spec={
                                     "region_id": {
                                         type: DimensionType.LONG,
                                         "marketplace_id": {
                                             type: DimensionType.LONG,
                                             "date": {
                                                 type: DimensionType.DATETIME,
                                                 "format": "%Y-%m-%d"
                                             }
                                         }
                                     }
                                 },
                                 output_dimension_filter={
                                     1: { # NA
                                         1: { # US
                                            "*": { # will be retrieved from daily_timer("date") at runtime

                                            }
                                         },
                                         771770: { #  MEX
                                             "*": {

                                             }
                                         }
                                     },
                                     2: {  # EU
                                         3: {  # UK
                                             "*": {

                                             }
                                         },
                                         4: {  # DE
                                             "*": {

                                             }
                                         }
                                     },
                                 })

regional_daily_job = app.create_data("REGIONAL_JOB",
                                     inputs=[regionalized_timer],
                                     compute_targets=[
                                        NOOPCompute
                                     ])

valve = app.create_data("TRIGGER_NODE",
                        compute_targets=[NOOPCompute],
                        output_dimension_spec={
                                                    "region_id": {
                                                        type: DimensionType.LONG,
                                                        "marketplace_id": {
                                                            type: DimensionType.LONG,
                                                            "date": {
                                                                type: DimensionType.DATETIME,
                                                                "format": "%Y-%m-%d"
                                                            }
                                                        }
                                                    }
                                                })

on_demand_regional_job = app.create_data("ON_DEMAND_REGIONAL_JOB",
                                         inputs=[valve, regional_daily_job.ref.range_check(True)],
                                         compute_targets=[
                                             NOOPCompute
                                         ])

app.activate()


# first verify non-existence of partitions
assert app.poll(regional_daily_job[1][1]["2023-10-15"]) == (None, None)
assert app.poll(regional_daily_job[1][771770]["2023-10-15"]) == (None, None)
assert app.poll(regional_daily_job[2][3]["2023-10-15"]) == (None, None)
assert app.poll(regional_daily_job[2][4]["2023-10-15"]) == (None, None)

# now simulate timer trigger
app.process(daily_timer["2023-10-15"])
#app.process(daily_timer["2023-10-6"], with_activated_processor=True)
#time.sleep(60)

path, _ = app.poll(regional_daily_job[1][1]["2023-10-15"])
assert path.endswith("REGIONAL_JOB/1/1/2023-10-15")

path, _ = app.poll(regional_daily_job[1][771770]["2023-10-15"])
assert path.endswith("REGIONAL_JOB/1/771770/2023-10-15")

path, _ = app.poll(regional_daily_job[2][3]["2023-10-15"])
assert path.endswith("REGIONAL_JOB/2/3/2023-10-15")

path, _ = app.poll(regional_daily_job[2][4]["2023-10-15"])
assert path.endswith("REGIONAL_JOB/2/4/2023-10-15")

#app.execute(regional_daily_job[1][1]["2023-10-16"], recursive=True)

app.admin_console()
