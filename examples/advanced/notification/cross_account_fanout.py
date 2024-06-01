# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import time

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.application.core_application import ApplicationState
from intelliflow.utils.test.data_emulation import add_test_data
from intelliflow.utils.test.inlined_compute import NOOPCompute

flow.init_basic_logging()


# Initiate forwarder_app in AWS_ACCOUNT_2 (you can run forwarder_app topology in a separate file but definitely before this script)
# We are going to demonstrate SNS notification channel creation (for S3 bucket events) from this account into AWS_ACCOUNT_1
forwarder_app = AWSApplication("data-forwarder", "us-east-1", access_id="AWS_ACCOUNT_2_ID", access_key="AWS_ACCOUNT_2_KEY")

offline_all_data = forwarder_app.marshal_external_data(
    S3Dataset("AWS_ACCOUNT_1", "<model-training-data-test>", "training-data", "{}", "{}",
              dataset_format=DataFormat.CSV)
        .link(
        SNS(topic_name="training_data_event_hub")
    )
    , "training_all_data"
    , {
        "region": {
            type: DimensionType.STRING,
            'day': {
                'type': DimensionType.DATETIME,
                'format': '%Y-%m-%d',
            }
        }
    },
    protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)

offline_all_data_2 = forwarder_app.marshal_external_data(
    S3Dataset("AWS_ACCOUNT_1", "<model-training-data-test-2>", "training-data", "{}", "{}",
              dataset_format=DataFormat.CSV)
        .link(
        # use the same topic for fan-out
        SNS(topic_name="training_data_event_hub")
    )
    , "training_all_data_2"
    , {
        "region": {
            type: DimensionType.STRING,
            'day': {
                'type': DimensionType.DATETIME,
                'format': '%Y-%m-%d',
            }
        }
    },
    protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)

# create the downstream / consumer app
consumer_app = AWSApplication("data-consumer", "us-east-1")
if consumer_app.state not in [ApplicationState.ACTIVE, ApplicationState.PAUSED]:
    consumer_app.activate()

# authorize downstream
forwarder_app.authorize_downstream(consumer_app.id, "AWS_ACCOUNT_2", "us-east-1")
forwarder_app.activate()

consumer_app.import_upstream(forwarder_app.id, "AWS_ACCOUNT_1", "us-east-1")

# data = forwarder_app["training_all_data"]
data = offline_all_data
data_2 = offline_all_data_2

trigger_test = consumer_app.create_data("trigger_test",
                                        inputs=[data],
                                        compute_targets=[
                                            NOOPCompute
                                        ])

trigger_test2 = consumer_app.create_data("trigger_test_2",
                                         inputs=[data_2],
                                         compute_targets=[
                                             NOOPCompute
                                         ])

consumer_app.activate()


# ------
# 1- Test the first channel (first bucket into first downstream node)
# add _SUCCESS file to source bucket to trigger cross-account event propagation and execution in downstream account
add_test_data(forwarder_app, offline_all_data["NA"]["2021-03-18"], object_name="_SUCCESS", data="")

# give some time for propagation (otherwise poll can report None on most recent execution)
# account1(S3 -> SNS) -> account2(Lambda)
time.sleep(10)

path, records = consumer_app.poll(trigger_test["NA"]["2021-03-18"])
assert path, "'trigger_test' node must have triggered automatically!"

# validate second note has not been triggered yet
path, _ = consumer_app.poll(trigger_test2["NA"]["2021-03-18"])
assert not path, "'trigger_test_2' node must NOT have triggered as second bucket did not send the notification yet!"

# ------
# 2- Test the second channel (second bucket into second downstream node)
# add _SUCCESS file to second source bucket to trigger cross-account event propagation and execution in downstream account
add_test_data(forwarder_app, offline_all_data_2["NA"]["2021-03-18"], object_name="_SUCCESS", data="")

time.sleep(10)

path, _ = consumer_app.poll(trigger_test2["NA"]["2021-03-18"])
assert path, "'trigger_test_2' node must have triggered automatically!"

consumer_app.terminate()
forwarder_app.terminate()