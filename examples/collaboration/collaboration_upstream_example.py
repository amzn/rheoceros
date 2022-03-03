# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

import logging

flow.init_basic_logging()
log = logging.getLogger()

# automatically reads default credentials
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
#
# assume that this parent app is in '427809481713'
parent_app = AWSApplication("eureka-dev", "us-east-1")
parent_app.attach()  # need to pull in active topology (otherwise it will get reset during the activation)
parent_app.authorize_downstream("blade-app-NA", '842027028048', 'us-east-1')
parent_app.activate()

# now 'blade-app-NA' app in account '842027028048' can import this parent app ('eureka-dev-yunusko')
# by doing this (in its own workflow code)
# eureka_dev_parent_app = app.import_upstream("eureka-dev",'427809481713', 'us-east-1')
