# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time

from intelliflow.api_ext import *
from intelliflow.core.application.application import Application
from intelliflow.mixins.aws.integ_test import AWSIntegTestMixin


class TestAWSApplicationExecutionAdvanced(AWSIntegTestMixin):
    """Test focuses on:
    - Compute bundling (BatchCompute code is not a simple script, relies on other modules or packages).
    - Hybrid compute types in different nodes or within the same node for each BatchCompute declaration.
    """

    def setup(self):
        super().setup("IntelliFlow")

    def test_application_signal_propagation_with_hybrid_compute(self):
        app = super()._create_test_application("advanced", from_other_account=True, reset_if_exists=True)

        ducsi_data = app.marshal_external_data(
            GlueTable("booker", "d_unified_cust_shipment_items"),
            "DEXML_DUCSI",
            dimension_spec={"region_id": {"type": DimensionType.LONG, "day": {"format": "%Y-%m-%d", "type": DimensionType.DATETIME}}},
            dimension_filter={
                "1": {
                    "*": {
                        "timezone": "PST",
                    }
                }
            },
        )

        d_ad_orders_na = app.marshal_external_data(
            GlueTable("dex_ml_catalog", "d_ad_orders_na"),
            "d_ad_orders_na",
            {"day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d", "timezone": "PST"}},
            {"*": {}},
        )

        repeat_ducsi = app.create_data(
            id="REPEAT_DUCSI",
            inputs={"DEXML_DUCSI": ducsi_data},
            compute_targets=[
                BatchCompute(
                    """
from intelliflow.utils.test.batch_compute import limit
output=limit(DEXML_DUCSI, 100)
                                                       """,
                    WorkerType=GlueWorkerType.G_2X.value,
                    NumberOfWorkers=50,
                    GlueVersion="1.0",  # Version 1.0 !
                )
            ],
        )

        repeat_d_ad_orders_na = app.create_data(
            id="REPEAT_AD_ORDERS",
            inputs=[d_ad_orders_na],
            compute_targets=[
                BatchCompute(
                    "d_ad_orders_na.limit(100)",
                    lang=Lang.SCALA,
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=50,
                    GlueVersion="1.0",
                )
            ],
        )

        repeat_d_ad_orders_na_gv_3 = app.create_data(
            id="REPEAT_AD_ORDERS_GV_3",
            inputs=[d_ad_orders_na],
            compute_targets=[
                BatchCompute(
                    "d_ad_orders_na.limit(100)",
                    lang=Lang.SCALA,
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=50,
                    GlueVersion="3.0",
                )
            ],
        )

        repeat_d_ad_orders_na_in_scala_from_internal_data = app.create_data(
            id="REPEAT_REPEAT_AD_ORDERS",
            inputs=[repeat_d_ad_orders_na],
            compute_targets=[
                BatchCompute(
                    "REPEAT_AD_ORDERS.limit(100)",
                    lang=Lang.SCALA,
                    # use G_2X
                    WorkerType=GlueWorkerType.G_2X.value,
                    NumberOfWorkers=20,
                    # scala in v2.0
                    GlueVersion="2.0",
                )
            ],
        )

        repeat_d_ad_orders_na_in_scala_from_internal_data_gv_3 = app.create_data(
            id="REPEAT_REPEAT_AD_ORDERS_GV_3",
            inputs=[repeat_d_ad_orders_na_gv_3],
            compute_targets=[
                BatchCompute(
                    "REPEAT_AD_ORDERS.limit(100)",
                    lang=Lang.SCALA,
                    # use G_2X
                    WorkerType=GlueWorkerType.G_2X.value,
                    NumberOfWorkers=20,
                    # scala in v2.0
                    GlueVersion="2.0",
                )
            ],
        )

        ducsi_with_AD_orders_NA = app.create_data(
            id="DUCSI_WITH_AD_ORDERS_NA",
            inputs=[repeat_d_ad_orders_na, repeat_ducsi["1"]["*"]],
            # input_dim_links=[
            #    # Note: do not use any linker function from the test namespace. It
            #    # will cause 'test_integration' module not found in runtime since
            #    # test modules are not as part of the bundles for drivers. So use
            #    # the function definition from core framework.
            #    (repeat_ducsi('ship_day'), EQUALS, repeat_d_ad_orders_na('order_day'))
            # ],
            compute_targets=[
                BatchCompute(
                    """
output=REPEAT_DUCSI.join(REPEAT_AD_ORDERS, ['customer_id'], how='full').limit(10).drop(*('customer_id', 'order_day'))

# tests bootstrapping
assert runtime_platform
                                                                   """,
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=30,
                    Timeout=3 * 60,  # 3 hours
                    GlueVersion="2.0",
                )
            ],
        )

        ducsi_with_AD_orders_NA_gv_3 = app.create_data(
            id="DUCSI_WITH_AD_ORDERS_NA_GV_3",
            inputs=[repeat_d_ad_orders_na_gv_3, repeat_ducsi["1"]["*"]],
            compute_targets=[
                BatchCompute(
                    """
output=REPEAT_DUCSI.join(REPEAT_AD_ORDERS, ['customer_id'], how='full').limit(10).drop(*('customer_id', 'order_day'))

# tests bootstrapping
assert runtime_platform
                                                                           """,
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=30,
                    Timeout=3 * 60,
                    GlueVersion="3.0",
                )
            ],
        )

        app.activate(allow_concurrent_executions=False)

        # trigger execution on 'ducsi_with_AD_orders_NA'
        # intentionally use process on the first execution to check raw (glue table) event handling as well.
        # 1- 2021-01-13
        # inject synthetic 'd_ad_orders_na' into the system
        app.process(d_ad_orders_na["2021-01-13"])
        app.process(ducsi_data[1]["2021-01-13"])

        self.poll(app, repeat_ducsi[1]["2021-01-13"], expect_failure=False)
        self.poll(app, repeat_d_ad_orders_na["2021-01-13"], expect_failure=False)
        self.poll(app, ducsi_with_AD_orders_NA["2021-01-13"], expect_failure=False)

        self.poll(app, repeat_d_ad_orders_na_in_scala_from_internal_data["2021-01-13"], expect_failure=False)

        # GV 3 pipe
        self.poll(app, repeat_d_ad_orders_na_gv_3["2021-01-13"], expect_failure=False)
        self.poll(app, ducsi_with_AD_orders_NA_gv_3["2021-01-13"], expect_failure=False)

        self.poll(app, repeat_d_ad_orders_na_in_scala_from_internal_data_gv_3["2021-01-13"], expect_failure=False)

        app.terminate()
        app.delete()
