# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time

from intelliflow.core.application.application import Application
from intelliflow.mixins.aws.integ_test import AWSIntegTestMixin


class TestAWSApplicationExecutionChain(AWSIntegTestMixin):
    def setup(self):
        super().setup("IntelliFlow")
        self.app: Application = None

    def test_application_signal_propagation(self):
        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        self.app = super()._create_test_application("exec-chain", reset_if_exists=True)
        self.app = TestAWSApplicationExecutionControl._create_test_application(self, self.app)

        # d_unified_cust_shipment_items
        # Extend the pipeline
        # 1 - TWO LEVEL PROPAGATION
        repeat_ducsi = self.app.get_data("REPEAT_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        ducsi_with_so = self.app.get_data("DUCSI_WITH_SO", context=Application.QueryContext.DEV_CONTEXT)[0]

        join_node = self.app.create_data(
            id="JOIN_NODE",
            inputs=[repeat_ducsi, ducsi_with_so],
            compute_targets="output=REPEAT_DUCSI.alias('r_d').join(DUCSI_WITH_SO, ['customer_id']).select('r_d.*')",
        )

        self.app.activate()

        from datetime import datetime, timedelta

        target_date = datetime.now() - timedelta(days=2)
        # strip off time
        target_date = datetime(target_date.year, target_date.month, target_date.day)

        assert self.app.poll(join_node[1][target_date]) == (None, None)

        # now feed the system with raw external signals
        self.app.process(self.app["DEXML_DUCSI"][1][target_date])
        self.app.process(self.app["ship_options"])

        # wait for LEVEL 1 to complete
        self.poll(self.app, materialized_node=repeat_ducsi[1][target_date], expect_failure=False)
        self.poll(self.app, materialized_node=ducsi_with_so[1][target_date], expect_failure=False)

        # now check level 2
        join_output_partition_path, _ = self.poll(self.app, materialized_node=join_node[1][target_date], expect_failure=False)
        # DUCSI node uses "%Y-%m-%d %H:%M:%S" as datetime format
        assert join_output_partition_path.endswith(f'internal_data/JOIN_NODE/1/{target_date.strftime("%Y-%m-%d %H:%M:%S")}')

        self.app.terminate()
        self.app.delete()
