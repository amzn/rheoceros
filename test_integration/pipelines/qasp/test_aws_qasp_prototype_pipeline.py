# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from datetime import datetime, timedelta, timezone
from typing import Callable

from intelliflow.api_ext import *
from intelliflow.core.deployment import DeploymentConfiguration
from intelliflow.mixins.aws.integ_test import AWSIntegTestMixin
from test_integration.pipelines.qasp.qasp_prototype.node.external import d_asins_mp as d_asins_marketplace_attrs
from test_integration.pipelines.qasp.qasp_prototype.node.external import pdex_logs as pdex
from test_integration.pipelines.qasp.qasp_prototype.node.external import tommy_external as tommy_ext
from test_integration.pipelines.qasp.qasp_prototype.node.external import xdf as search_xdf
from test_integration.pipelines.qasp.qasp_prototype.node.internal import pdex_consolidated as pdex_cons
from test_integration.pipelines.qasp.qasp_prototype.node.internal import tommy_data_filtered_prestosql as tommy_data_presto
from test_integration.pipelines.qasp.qasp_prototype.node.internal import tommy_data_pyspark as tommy_data_pyspark
from test_integration.pipelines.qasp.qasp_prototype.node.internal import tommy_data_scala_spark as tommy_data_scala


class TestAWSQASPPrototypePipeline(AWSIntegTestMixin):
    def setup(self):
        super().setup("IntelliFlow")

    def test_application_signal_propagation(self):
        # make sure pipeline root will be deployed to AWS (by default framework will not detect this)
        DeploymentConfiguration.app_extra_modules = {"test_integration.pipelines.qasp.qasp_prototype"}

        app = super()._create_test_application("test-qasp", reset_if_exists=True)

        region = "NA"
        region_id = 1
        marketplace_id = 1
        encryption_key = "arn:aws:kms:us-east-1:800261124827:key/5be55530-bb8e-4e95-8185-6e1afab0de54"

        # let eternal nodes join the app
        tommy_external = tommy_ext.create(app, encryption_key)

        pdex_external = pdex.create(app)

        dama = d_asins_marketplace_attrs.create(app)

        xdf = search_xdf.create(app, encryption_key)

        # internal nodes
        pdex_consolidated = pdex_cons.create(app, pdex_external, region, region_id, marketplace_id)

        tommy_data_node = tommy_data_pyspark.create(app, tommy_external, dama, xdf, pdex_consolidated, region, region_id, marketplace_id)

        tommy_data_node_SCALA = tommy_data_scala.create(
            app, tommy_external, dama, xdf, pdex_consolidated, region, region_id, marketplace_id
        )

        tommy_data_filtered_PRESTO = tommy_data_presto.create(app, tommy_data_node)

        app.activate()

        day = datetime.now() - timedelta(2)
        # this test is written before 'recursive' support in 'execute' API, so we have to make sure pdex_consolidated
        # upstream node is ready before moving to the next.
        # once complete: this should auto-trigger tommy_data nodes (which are in PDT but pdex is in UTC so we use hour=15 here)
        app.execute(pdex_consolidated[1][1][day][15])

        path, records = app.poll(tommy_data_node[1][1][day][8])
        assert path, "Tommy data PySpark execution must have succeeded!"
        path2, records2 = app.poll(tommy_data_node_SCALA[1][1][day][8])
        assert path2, "Tommy data Scala execution must have succeeded!"
        path3, records3 = app.poll(tommy_data_filtered_PRESTO[1][1][day][8])
        # TODO HIVE_CANNOT_OPEN_SPLIT: Error opening Hive split (due to high partition count from tommy_data_node)
        # assert path3, "Tommy data PrestoSQL execution must have succeeded!"

        app.terminate()
        app.delete()
