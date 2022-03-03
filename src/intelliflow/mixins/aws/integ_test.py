# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib
import os
import time

from botocore.exceptions import ClientError

from intelliflow.api_ext import AWSApplication, MarshalerNode, MarshalingView
from intelliflow.core.application.core_application import ApplicationState


class AWSIntegTestMixin:
    @classmethod
    def create_stage_key(cls, app_name: str) -> str:
        return f"{app_name}_integ_tests_stage"

    @classmethod
    def create_aws_account_id_key(cls, app_name: str) -> str:
        return f"{app_name}_integ_tests_aws_account_id"

    @classmethod
    def create_aws_cross_account_id_key(cls, app_name: str) -> str:
        return f"{app_name}_integ_tests_aws_cross_account_id"

    @classmethod
    def create_aws_region(cls, app_name: str) -> str:
        return f"{app_name}_integ_tests_aws_region"

    def setup(self, app_name: str, **kwargs):
        if getattr(super(), "setup", None):
            super().setup(app_name, **kwargs)

        stage_key = self.create_stage_key(app_name)
        self.stage = os.getenv(stage_key)
        assert self.stage, f"{self.__class__.__name__} needs {stage_key!r} environment variable to be defined!"

        account_id_key = self.create_aws_account_id_key(app_name)
        self.account_id = os.getenv(account_id_key)
        assert self.account_id, f"{self.__class__.__name__} needs {account_id_key!r} environment variable to be defined!"

        cross_account_id_key = self.create_aws_cross_account_id_key(app_name)
        self.cross_account_id = os.getenv(cross_account_id_key)
        assert self.stage, f"{self.__class__.__name__} needs {cross_account_id_key!r} environment variable to be defined!"

        region_key = self.create_aws_region(app_name)
        self.region = os.getenv(region_key)
        assert self.stage, f"{self.__class__.__name__} needs {region_key!r} environment variable to be defined!"

    def _create_test_application(self, id: str, from_other_account=False, reset_if_exists=False):
        time.sleep(2)  # a little break in between consecutive inte-tests using the same 'id'.

        token_retry_count = 3
        while token_retry_count > 0:
            try:
                app_args = {
                    "app_name": id + "-" + self.stage,
                    "region": self.region
                    # TODO support from_other_account and map it to 'profile' parameter here
                }
                app = AWSApplication(**app_args)
                if reset_if_exists and app.state != ApplicationState.INACTIVE:
                    app.terminate()
                    app.delete()
                    app = AWSApplication(**app_args)
                return app
            except ClientError as error:
                if error.response["Error"]["Code"] in ["UnrecognizedClientException", "ExpiredTokenException", "InvalidAccessKeyId"]:
                    token_retry_count = token_retry_count - 1
                else:
                    raise

    def poll(self, app, materialized_node, expect_failure=False, duration=3600):
        node = None
        if isinstance(materialized_node, MarshalingView):
            node = materialized_node.marshaler_node
        elif isinstance(materialized_node, MarshalerNode):
            node = materialized_node
        start = time.time()
        while True:
            path, records = app.poll(materialized_node)
            if records is not None:
                if expect_failure:
                    assert not path, f"Expected failure but the node {node.bound.route_id!r} yielded success."
                else:
                    assert path, f"Expected success but the node {node.bound.route_id!r} yielded failure."
                return path, records
            time.sleep(10)
            elapsed_time_in_secs = time.time() - start
            assert elapsed_time_in_secs < duration, f"Test failed due to timeout while polling on {node.bound.route_id!r}"
