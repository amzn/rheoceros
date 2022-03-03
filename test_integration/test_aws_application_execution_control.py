# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import time

import pytest

from intelliflow.api_ext import *
from intelliflow.core.application.application import Application
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.integ_test import AWSIntegTestMixin


class TestAWSApplicationExecutionControl(AWSIntegTestMixin):
    def setup(self):
        super().setup("IntelliFlow")

    def _create_application(self, id: str):
        # reuse the unit-test version
        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        app = super()._create_test_application(id, reset_if_exists=True)
        app = TestAWSApplicationExecutionControl._create_test_application(self, app)
        return app

    def test_application_process(self):
        app = self._create_application("andes-down")

        # try to process while not activated and fail
        ducsi_data = app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        with pytest.raises(RuntimeError):
            app.process(ducsi_data[1]["2021-10-01"])

        # activate and process (in SYNC mode) using 'repeat_ducsi'
        app.activate()

        app.process(
            ducsi_data[1]["2021-12-25"],
            # make it SYNC (use the local processor instance in sync mode)
            # will end up testing dev-role permissions as well (due to local use of orchestration)
            with_activated_processor=False,
        )

        assert len(app.get_active_routes()) == 1
        assert app.get_active_routes()[0].route.route_id == "InternalDataNode-REPEAT_DUCSI"

        # now the second internal data node (route) in the system actually waits for its second input dependency
        # 'ship_options'. Previous process call with ducsi has created a pending node in it as well. a signal for
        # 'ship_options' will complete that pending node and cause a trigger.
        ship_options = app.get_data("ship_options", context=Application.QueryContext.DEV_CONTEXT)[0]
        app.process(
            ship_options,
            # make it ASYNC (use remote Processor in cloud)
            # will end up testing exec role permissions with remote orchestrator
            with_activated_processor=True,
        )

        # poll due to asynchronicity
        wait_time = 0
        while len(app.get_active_routes()) != 2:
            time.sleep(5)
            wait_time = wait_time + 5
            assert wait_time < 120, f"Application active route count must have increased to 2! " f"Problem in remote Processor."

        assert {route_record.route.route_id for route_record in app.get_active_routes()} == {
            "InternalDataNode-REPEAT_DUCSI",
            "InternalDataNode-DUCSI_WITH_SO",
        }
        # also check there is no Pending Nodes remaining (they must have turned into active compute records)
        assert all(
            (not route_record.persisted_pending_node_ids and not route_record.route.pending_nodes)
            for route_record in app.get_active_routes()
        )

        # check idempotency
        app.process(ducsi_data[1]["2021-12-25"], with_activated_processor=False)
        # no effect (still the same count on the mock objects)
        assert len(app.get_active_routes()) == 2
        app.process(ship_options, with_activated_processor=False)

        # initiate another trigger on 'REPEAT_DUCSI' with a different partition (12-26)
        app.process(ducsi_data[1]["2021-12-26"], with_activated_processor=True)
        wait_time = 0
        while len(app.get_active_route("REPEAT_DUCSI").active_compute_records) != 2:
            time.sleep(5)
            wait_time = wait_time + 5
            assert wait_time < 120, "Active compute record count on REPEAT-DUCSI must be 2! Problem in remote Processor"

        app.update_active_routes_status()

        # we don't care about the results of those computations for this test.
        # just terminate, clean-up and exit.
        app.terminate()
        app.delete()

    def test_application_poll_on_updated_route(self):
        # assume that this is session 1
        app = self._create_application("andes-down")
        # original state will be activated
        app.activate()

        # on another devpoint, assume that this is session 2 (post-activation)
        # initiate an update
        app = super()._create_test_application("andes-down")

        ducsi_data = app.marshal_external_data(
            GlueTable("booker", "d_unified_cust_shipment_items"),
            "DEXML_DUCSI",
            {"region_id": {"type": DimensionType.LONG, "ship_day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d %H:%M:%S"}}},
            {
                "1": {"*": {"timezone": "PST"}},
            },
        )

        repeat_ducsi = app.create_data(
            id="REPEAT_DUCSI",
            inputs={
                "DEXML_DUCSI": ducsi_data,
            },
            compute_targets="output=DEXML_DUCSI.sample(True, 0.0001)",
        )

        # it won't fail (raise error), will still try to find the most recent successful execution on the target,
        # but will eventually yield None since there has been no execution on this route yet.
        assert app.poll(repeat_ducsi[1]["2020-12-25"]) == (None, None)

        app.terminate()
        app.delete()

    @classmethod
    def _update_upstream_parent_app(cls, app):
        eureka_offline_training_data = app.marshal_external_data(
            S3Dataset(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data-prod",
                "partition_day={}",
                dataset_format=DataFormat.CSV,
            ),
            "eureka_training_data",
            {"day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        eureka_offline_all_data = app.marshal_external_data(
            S3Dataset(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/all-data-prod",
                "partition_day={}",
                dataset_format=DataFormat.CSV,
            ),
            "eureka_training_all_data",
            {"day": {"format": "%Y-%m-%d", "type": DimensionType.DATETIME}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        app.create_data(
            "eureka_default_selection_data_over_two_days",  # data_id
            {"offline_data": eureka_offline_all_data[:-2], "offline_training_data": eureka_offline_training_data[:-2]},
            compute_targets=[  # when inputs are ready, trigger the following
                BatchCompute(
                    "output = offline_data.subtract(offline_training_data).limit(100)",
                    Lang.PYTHON,
                    ABI.GLUE_EMBEDDED,
                    # extra_params
                    Timeout=60,  # in minutes
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=50,
                    GlueVersion="2.0",
                )
            ],
        )
        return app

    def test_application_poll_on_upstream_data(self):
        app = self._create_application("child-app")
        app.activate()

        # enrich the test by forcing cross-account access
        upstream_app = super()._create_test_application("parent-app", from_other_account=True)
        upstream_app = self._update_upstream_parent_app(upstream_app)
        # connect them
        upstream_app.authorize_downstream(app.id, self.account_id, self.region)
        upstream_app.activate()

        app.import_upstream(upstream_app.id, upstream_app.platform.processor.account_id, self.region)
        app.activate()

        # fetch from app (parent's data is visible within child's scope)
        eureka_data_from_parent = app["eureka_default_selection_data_over_two_days"]

        from datetime import datetime, timedelta

        target_date = datetime.now() - timedelta(days=30)
        target_date = datetime(target_date.year, target_date.month, target_date.day)

        # data is not ready yet
        assert app.poll(eureka_data_from_parent[target_date]) == (None, None)
        assert app.poll(upstream_app["eureka_default_selection_data_over_two_days"][target_date]) == (None, None)

        # trigger upstream execution on 'eureka_default_selection_data_over_two_days' by injecting its two inputs.
        # (these injections mimic _SUCCESS file generation in those upstream datasets)
        upstream_app.process(upstream_app["eureka_training_data"][target_date])
        upstream_app.process(upstream_app["eureka_training_all_data"][target_date])

        assert app.poll(eureka_data_from_parent[target_date])[0].endswith(
            f"internal_data/eureka_default_selection_data_over_two_days/{target_date.strftime('%Y-%m-%d')}"
        )

        app.terminate()
        app.delete()
        upstream_app.terminate()
        upstream_app.delete()

    def test_application_execute_with_material_inputs(self):
        app = self._create_application("andes-down")

        ducsi_data = app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        repeat_ducsi = app.get_data("REPEAT_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]

        from datetime import datetime, timedelta

        target_date = datetime.now() - timedelta(days=10)
        # strip off time to imitate the actual partition format of this glue table
        target_date = datetime(target_date.year, target_date.month, target_date.day)

        # first make sure REPEAT_DUCSI will be ready
        app.execute(repeat_ducsi[1][target_date])

        target2 = app.create_data(
            id="REPEAT_DUCSI2",
            inputs={"DEXML_DUCSI": ducsi_data, "REPEAT_DUCSI": repeat_ducsi, "REPEAT_DUCSI_RANGE": repeat_ducsi["*"][:-1]},
            compute_targets="output=DEXML_DUCSI.unionAll(REPEAT_DUCSI).unionAll(REPEAT_DUCSI_RANGE.limit(100))",
        )

        # show that 'REPEAT_DUCSI2' can execute well if materialized.
        output_path = app.execute(target2, ducsi_data[1][target_date])
        assert output_path.endswith(f"internal_data/REPEAT_DUCSI2/1/{target_date.strftime('%Y-%m-%d %H:%M:%S')}")

        ship_options = app.get_data("ship_options", context=Application.QueryContext.DEV_CONTEXT)[0]

        # and now a successful run 'DUCSI_WITH_SO'
        ducsi_with_so = app["DUCSI_WITH_SO"]
        output_path = app.execute(ducsi_with_so, [ducsi_data[1][target_date], ship_options])
        # s3://if-andes_downstream-123456789012-us-east-1/internal_data/DUCSI_WITH_SO/1/2020-06-21 00:00:00
        assert output_path.endswith(f"internal_data/DUCSI_WITH_SO/1/{target_date.strftime('%Y-%m-%d %H:%M:%S')}")

        app.terminate()
        app.delete()
