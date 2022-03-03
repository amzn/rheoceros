# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.compute_targets.email import EMAIL

# Refer new_app_template/EMAIL_ACTION_SETUP_AND_INFO before proceeding
app_name = f"email-app-new"

flow.init_basic_logging()

# automatically reads default credentials
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
app = AWSApplication(app_name, "us-east-1")

if_email_input_node = app.marshal_external_data(
    S3Dataset("427809481713", "if-email-test", "dex-ml/test-dataset", "{}", "{}",
              dataset_format=DataFormat.CSV)
    , "if_email_input_node"
    , {
        'region': {
            'type': DimensionType.LONG,
            'day': {
                'type': DimensionType.DATETIME
            }
        }
    }
    , {
        "*": {
            "*": {
                'format': '%Y-%m-%d',
                'timezone': 'PST',
            }
        }
    },
    SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)


# see 'doc/EMAIL_ACTION_SETUP_AND_INFO'
email_obj = EMAIL(sender="if-test-list@amazon.com",
                  recipient_list=["rheoceros@amazon.com"])

subject = "IF Test Email"
body = "IF Test Email body"

if_email_data_node = app.create_data(id=f"if_email_data_node",
                                     inputs={"if_email_input_node": if_email_input_node},
                                     compute_targets=[email_obj.action(subject=subject, body=body)],
                                     execution_hook=RouteExecutionHook(on_exec_begin=email_obj.action(subject=subject + " from execution hook", body=body))
                                     )

app.activate(allow_concurrent_executions=True)
app.process(app["if_email_input_node"][1]['2020-10-23'], with_activated_processor=True)
