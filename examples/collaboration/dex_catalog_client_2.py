# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

flow.init_basic_logging()

app = AWSApplication("catalog-client-2", "us-east-1")

catalog_app = app.import_upstream("dex-catalog",'842027028048', 'us-east-1')




