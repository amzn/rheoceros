# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

'''Sample admin script to tear down multiple active applications at the same

Since this example uses default credentials (on the same machine), all of those hypothetical apps can be assumed to be
running in the same account.
'''

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

import logging

from intelliflow.core.application.core_application import ApplicationState

logger = flow.init_basic_logging(root_level=logging.INFO)

apps_to_be_removed = [
    ("dex-catalog", "us-east-1"),
    ("dc-beta-app", "us-east-1"),
    ("catalog-client", "us-east-1"),
    ("catalog-client-2", "us-east-1"),
    ("andes-ant-apt", "us-east-1"),
    ("andes-ant-a", "us-east-1")
]

for id, region in apps_to_be_removed:
    logger.critical(f"Removing AWS Application(id={id}, region={region} ...")
    app = AWSApplication(id, region)
    if app.state in [ApplicationState.ACTIVE, ApplicationState.PAUSED, ApplicationState.TERMINATING]:
        app.terminate()
    logger.critical("Termination complete!")

for id, region, account_id in apps_to_be_removed:
    logger.critical(f"Deleting AWS Application(id={id}, region={region}, account_id={account_id}) ...")
    app = AWSApplication(id, region, account_id, enforce_runtime_compatibility=False)
    app.delete()
    logger.critical("Deletion complete!")

