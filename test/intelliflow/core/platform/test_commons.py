# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

import boto3
import mock
import pytest
from mock import MagicMock
from moto import (
    mock_applicationautoscaling,
    mock_autoscaling,
    mock_dynamodb2,
    mock_events,
    mock_glue,
    mock_iam,
    mock_kms,
    mock_lambda,
    mock_s3,
    mock_sns,
    mock_sqs,
    mock_sts,
)

# NOTE that the objects from the modules that use the wrappers are being imported
# doing the same on 'intelliflow.core.platform.definitions.aws.glue.client_wrapper' wont' work
import intelliflow.core.platform.drivers.compute.aws as compute_driver
import intelliflow.core.platform.drivers.processor.aws as processor_driver
import intelliflow.core.platform.drivers.routing.aws as routing_driver
import intelliflow.core.platform.drivers.storage.aws as storage_driver

# FUTURE add test commons which are specific to framework's use-cases (not reusable by dependent app packages).
