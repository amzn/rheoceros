# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""
On-demand provisioning of a Sagemaker notebook instance that would have the same runtime as this host env.
"""

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
import logging

from intelliflow.core.application.core_application import ApplicationState

log = flow.init_basic_logging(root_level=logging.WARN)

app_name = 'onboarding'
region = "us-east-1"

log.warning("Initiating application provisioning...")

log.warning(f"Add/update application for {app_name!r}")

app = AWSApplication(app_name, region)

if app.state == ApplicationState.INACTIVE: # if app is brand new or previously 'terminated' (see Application::terminate)
    app.activate()

# can be called on an existing notebook for update purposes also (update framework + application code + all Python
# dependencies and make them available on the notebook via 'IntelliFlow' kernel)
# this is a very important functionality to share your app and data with others (e.g scientists) for;
# - collaboration on the same app
# - cloning, sandboxing by changing the 'app_name' so that your business logic from this project can be remotely
# modified, and executed to create new data in the context of a brand new app.
dev_endpoint: DevEndpoint = app.provision_remote_dev_env({
                    # Framework forms a transparent thin layer between your notebook and this high-level code.
                    # at this point, you know that you are using an AWS app, which behind the scenes rely on a platform with
                    # an AWS configuration that uses Sagemaker notebook instances. Either know it from the doc or use the same API
                    # to get the DevEndpoint object and check its type, then call again as an update operation.

                    # check https://docs.aws.amazon.com/sagemaker/latest/dg/notebooks-available-instance-types.html
                    # for other 'InstanceType's
                    'InstanceType': 'ml.t2.medium',
                    'VolumeSizeInGB': 50
                })

# at this point dev_endpoint might or might not be active (e.g Sagemaker notebook instance state: 'InService')
# it is up to the user to control this via 'dev_endpoint' object.
if not dev_endpoint.is_active():
    dev_endpoint.start(True)

# print the presigned url to the console and expect user to click
print("You can now click on the link below to go to your remote dev end:")
print(dev_endpoint.generate_url(session_duration_in_secs=7200))

