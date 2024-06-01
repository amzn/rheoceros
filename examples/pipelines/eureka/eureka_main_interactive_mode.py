# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Sample RheocerOS platform packed into one file for demonstration purposes.

Rather than packed into a single file, it can very well reside in a separate package
that depends on RheocerOS module and more importantly separate project structure
spanning its own sub-modules that relies on same "app" object.

That is why this is named with a 'main' suffix to indicate the fact that this type
of files can be assumed to be "main" entry points for an RheocerOS platform.

Main entry point can initiate the app object and pass it into platform specific
sub-modules and entities to act on it using RheocerOS APIs.

Recommendation for Activation (PyCharm):

- Check "Run with Python Console" in the app config of this file.
    (Right click and then choose "Edit 'eureka_main'..." from the context menu.)
- Keep interacting with the "flow" and "app" after the activation:
    - query app node/provisioning status, other API calls.

-
"""
import intelliflow.api as flow
from intelliflow.api import *

region = "us-east-1" #TODO read from ENV
stage = "beta"
app_name = f"eureka-{stage}"
flow.init_basic_logging("./eureka_main_interactive_mode")

app = flow.create_or_update_application(app_name,
                                        HostPlatform(AWSConfiguration.builder()
                                                     .with_default_credentials(as_admin=True)
                                                     .with_region(region)
                                                     .build()))

# Invocation
# - use existing signals (materialize) to emulate internal trigger
# - use <SignalGenerator>s (S3SignalGenerator) to emulate external trigger
app.process({
    "Records":[
        {
            "eventVersion":"2.0",
            "eventSource":"aws:s3",
            "awsRegion":"us-east-1",
            "eventTime":"1970-01-01T00:00:00.000Z",
            "eventName":"ObjectCreated:Put",
            "s3":{
                "s3SchemaVersion":"1.0",
                "bucket":{
                    "name":"<TRAINING_DATA_BUCKET>",
                    "arn":"arn:aws:s3:::<TRAINING_DATA_BUCKET>"
                },
                "object":{
                    "key":"eureka_p3/v8_00/training-data/NA/2020-08-01/_SUCCESS",
                }
            }
        }
    ]
})


