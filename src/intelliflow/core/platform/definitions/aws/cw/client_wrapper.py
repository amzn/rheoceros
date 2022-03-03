# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

from intelliflow.core.platform.definitions.aws.common import exponential_retry

logger = logging.getLogger(__name__)


# wrapped here for easy mocking support (TODO remove after moto 2.x)
def put_composite_alarm(cw, **params):
    return cw.put_composite_alarm(**params)
