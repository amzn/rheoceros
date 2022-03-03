# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
import logging

log = flow.init_basic_logging(root_level=logging.WARN)

app_name = 'remote-sync-tst'
region = "us-east-1"

# uses 'default' credentials on the machine by default
app = AWSApplication(app_name, region)
dev_endpoint: DevEndpoint = app.get_remote_dev_env()

#print(dev_endpoint.attrs)

# at this point dev_endpoint might or might not be active (e.g Sagemaker notebook instance state: 'InService')
# it is up to the user to control this via 'dev_endpoint' object.
if not dev_endpoint.is_active():
    dev_endpoint.start(True)

# print the presigned url to the console and expect user to click
print("You can now click on the link below to go to your remote dev end:")
print(dev_endpoint.generate_url(session_duration_in_secs=7200))

