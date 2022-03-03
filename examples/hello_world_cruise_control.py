# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.compute_targets.email import EMAIL

flow.init_basic_logging()

app = AWSApplication("hello-world", "us-east-1", "427809481713")

# attach/pull the existing DAG/Conf
app.attach()

'''Exploration Examples'''
# Note: the following data exploration and preview/load operations assume
# that the application was activated (already running).
node_1_1 = app['Node_1_1']
node_1_3 = app['Node_1_3']


email_obj = EMAIL(sender="if-test-list@amazon.com",  # sender from the same account
                  recipient_list=["helloworldtest@amazon.com"])

node_1_3 = app.update_data(id=f"Node_1_3",
                           inputs=[node_1_1],
                           compute_targets=[
                               InlinedCompute(
                                   lambda input_map, output, params: print("succeed")
                               )
                           ],
                           execution_hook=RouteExecutionHook(
                                                on_failure=email_obj.action(subject="IF Node_1_3 Failure Test", body="<body>"),
                                                on_success=email_obj.action(subject="IF Node_1_3 Success Test", body="<body>")
                                                               )
                           )

app.execute(node_1_3)
# alternatively, inject node_1_1 signal however instead of global ingestion of event by all nodes, use 'node_1_3' as target
# app.process(node_1_1, target_route_id=node_1_3)


# MOVE TO NOTEBOOK
dev_endpoint: DevEndpoint = app.provision_remote_dev_env({
    'InstanceType': 'ml.t2.medium',
    'VolumeSizeInGB': 50
})

# at this point dev_endpoint might or might not be active (e.g Sagemaker notebook instance state: 'InService')
# it is up to the user to control this via 'dev_endpoint' object.
if not dev_endpoint.is_active():
    dev_endpoint.start(True)
