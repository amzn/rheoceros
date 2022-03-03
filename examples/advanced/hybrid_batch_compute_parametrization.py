# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.constructs import CompositeBatchCompute
from intelliflow.core.platform.drivers.compute.aws import AWSGlueBatchComputeBasic
from intelliflow.core.platform.drivers.compute.aws_emr import AWSEMRBatchCompute

flow.init_basic_logging()

# uses default credentials (explicitly set using the AWSConfiguration fluent API below)
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
app = AWSApplication("hybrid-app", HostPlatform(AWSConfiguration.builder()
                                                             # explicitly allow the framework to use default credentials
                                                             # to create applications dev-role.
                                                             # these credentials are not used till app deletion (delete
                                                             # API) where we'd again need permission to wipe out the
                                                             # dev-role as the last action.
                                                             .with_default_credentials(as_admin=True)
                                                             .with_region("us-east-1")
                                                             .with_param(CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM,
                                                                         [
                                                                             AWSGlueBatchComputeBasic,
                                                                             AWSEMRBatchCompute
                                                                         ])
                                                             .build()))

should_run_on_glue = app.create_data(id='glue_node',
                                    compute_targets=[
                                        BatchCompute(
                                            "<SPARK CODE>",
                                            # will pick Glue thanks to order in the list above
                                            GlueVersion="2.0",
                                            WorkerType=GlueWorkerType.G_1X.value,
                                            NumberOfWorkers=50
                                        )
                                    ])

should_run_on_EMR = app.create_data(id='emr_node',
                                    compute_targets=[
                                        BatchCompute(
                                            "<SPARK CODE>",
                                            # will pick EMR over the existence of InstanceConfig
                                            GlueVersion="2.0",
                                            InstanceConfig={'master': {}, 'compute': {}}
                                        )
                                    ])

should_run_on_EMR2 = app.create_data(id='emr_node2',
                                   compute_targets=[
                                       BatchCompute(
                                           "<SPARK CODE>",
                                           # will pick EMR over the existence of RuntimeConfig
                                           RuntimeConfig="GlueVerion-2.0",
                                       )
                                   ])

# both drivers support this compute, so the first one will be chosen (Glue driver in this case)
spark_node = app.create_data(id="should_run_on_first_driver",
                              compute_targets=[SparkSQL("select * from DUMMY")])

app.activate()

