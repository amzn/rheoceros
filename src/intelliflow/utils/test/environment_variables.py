# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest import TestCase, mock


class TestEnvironmentVariables(TestCase):
    @mock.patch("intelliflow.core.deployment.IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE")
    def test_ensure_remote_env_detection(self, IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE):
        IS_ON_SAGEMAKER_NOTEBOOK_INSTANCE.return_value = True
        from intelliflow.core.deployment import is_on_remote_dev_env

        self.assertTrue(is_on_remote_dev_env())
