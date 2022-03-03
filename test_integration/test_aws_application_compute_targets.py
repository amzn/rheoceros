# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time
from datetime import datetime

import pytest
from mock import MagicMock

from intelliflow.api_ext import *
from intelliflow.mixins.aws.integ_test import AWSIntegTestMixin


class TestAWSApplicationComputeTargets(AWSIntegTestMixin):
    """Integ-test covering parametrization of compute_targets using remote Batch/Inlined compute runtimes (Glue,
    EMR, Lambda, etc).
    """

    def setup(self):
        super().setup("IntelliFlow")

    def _create_app(self) -> AWSApplication:
        app = self._create_test_application("comp-targets", from_other_account=True)
        app.marshal_external_data(
            GlueTable("dex_ml_catalog", "d_ad_orders_na"),
            "d_ad_orders_na",
            {"order_day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d", "timezone": "PST"}},
            {"*": {}},
        )
        return app

    def test_application_user_params_in_different_langs(self):
        app = self._create_app()

        d_ad_orders_na = app.get_data("d_ad_orders_na", context=Application.QueryContext.DEV_CONTEXT)[0]

        repeat_d_ad_orders_na_scala = app.create_data(
            id="REPEAT_AD_ORDERS_IN_SCALA",
            inputs=[d_ad_orders_na],
            compute_targets=[
                BatchCompute(
                    scala_script(
                        """
                                                                           assert(args("my_param1") == "my_param1_value")
                                                                           assert(args("my_int_param1") == "1")
                                                                           d_ad_orders_na.limit(10)
                                                                           """
                        # , external_library_paths=["s3://test-dexml-blade-beta/lib/DexmlBladeGlue-super.jar"]
                    ),
                    lang=Lang.SCALA,
                    GlueVersion="2.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=10,
                    Timeout=3 * 60,  # 3 hours
                    # user params
                    my_param1="my_param1_value",
                    # please note that it will be stringified at runtime
                    my_int_param1=1,
                )
            ],
        )

        repeat_d_ad_orders_na_python = app.create_data(
            id="REPEAT_AD_ORDERS_IN_PYTHON",
            inputs=[d_ad_orders_na],
            compute_targets=[
                BatchCompute(
                    """
assert args["my_param1"] == "my_param1_value"
assert args["my_int_param1"] == "1"
output = d_ad_orders_na.limit(10)
                                                               """,
                    GlueVersion="2.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=10,
                    Timeout=3 * 60,
                    # user params
                    my_param1="my_param1_value",
                    # please note that it will be stringified at runtime
                    my_int_param1=1,
                )
            ],
        )

        app.activate()
        app.process(d_ad_orders_na["2021-01-14"])

        self.poll(app, repeat_d_ad_orders_na_scala["2021-01-14"], expect_failure=False)
        self.poll(app, repeat_d_ad_orders_na_python["2021-01-14"], expect_failure=False)

        app.terminate()
        app.delete()
