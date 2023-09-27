# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime
import threading
import time
from unittest.mock import MagicMock

import pytest

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.application.application import Application
from intelliflow.core.platform.definitions.compute import (
    ComputeFailedSessionState,
    ComputeFailedSessionStateType,
    ComputeResourceDesc,
    ComputeResponse,
    ComputeSessionDesc,
    ComputeSessionState,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.signal_processing import Slot
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationExecutionControl(AWSTestBase):
    def _next_processor_cycle_event(self, app: Application) -> Dict[str, Any]:
        return {
            "version": "0",
            "id": "f4d328c4-653b-cc47-1abb-439fdde1896f",
            "detail-type": "Scheduled Event",
            "source": "aws.events",
            "account": repr(self.account_id),
            "time": "2020-12-27T19:53:06Z",
            "region": repr(self.region),
            "resources": [f"arn:aws:events:us-east-1:{self.account_id}:rule/if-AWSLambdaProcessorBasic-{app.id}-east-1-loop"],
            "detail": {},
        }

    def _create_test_application(self, id_or_app: Union[str, Application], **params):
        if isinstance(id_or_app, str):
            id = id_or_app
            app = AWSApplication(id, self.region, **params)
        else:
            app = id_or_app
            id = app.id

        ducsi_data = app.marshal_external_data(
            GlueTable("booker", "d_unified_cust_shipment_items", partition_keys=["region_id", "ship_day"]),
            "DEXML_DUCSI",
            {"region_id": {"type": DimensionType.LONG, "ship_day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d %H:%M:%S"}}},
            {
                "1": {"*": {"timezone": "PST"}},
                "2": {"*": {"timezone": "UTC"}},
            },
            SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT"]}),
        )

        # add a dimensionless table (important corner-case)
        ship_options = app.marshal_external_data(
            GlueTable("dexbi", "d_ship_option"),
            "ship_options",
            {},
            {},
            SignalIntegrityProtocol("FILE_CHECK", {"file": ["DELTA", "SNAPSHOT"]}),
        )

        repeat_ducsi = app.create_data(
            id="REPEAT_DUCSI",
            inputs={
                "DEXML_DUCSI": ducsi_data,
            },
            compute_targets="output=DEXML_DUCSI.limit(100)",
        )

        app.create_data(
            id="DUCSI_WITH_SO",
            inputs={"DEXML_DUCSI": ducsi_data["*"][:-1], "SHIP_OPTIONS": ship_options},
            compute_targets=[
                BatchCompute(
                    "output=DEXML_DUCSI.sample(True, 0.0000001).join(SHIP_OPTIONS.select('ship_option'), DEXML_DUCSI.customer_ship_option == SHIP_OPTIONS.ship_option).limit(10)",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=70,
                    GlueVersion="1.0",
                )
            ],
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        return app

    @classmethod
    def create_batch_compute_response(cls, response_type: ComputeSuccessfulResponseType, session_id: str) -> ComputeSuccessfulResponse:
        batch_compute_resp = ComputeSuccessfulResponse(
            response_type, ComputeSessionDesc(session_id, ComputeResourceDesc("job_name", "job_arn"))
        )
        return batch_compute_resp

    @classmethod
    def create_batch_compute_session_state(
        cls, session_state_type: ComputeSessionStateType, session_desc: Optional[ComputeSessionDesc] = None
    ) -> ComputeSessionState:
        session_state = ComputeSessionState(
            ComputeSessionDesc("job_id", ComputeResourceDesc("job_name", "job_arn")) if not session_desc else session_desc,
            session_state_type,
            [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))],
        )
        return session_state

    @classmethod
    def create_batch_compute_failed_session_state(
        cls, failure_type: ComputeFailedSessionStateType, session_desc: ComputeSessionDesc
    ) -> ComputeSessionState:
        session_state = ComputeFailedSessionState(
            failure_type, session_desc, [ComputeExecutionDetails("<start_time>", "<end_time>", dict({"param1": 1, "param2": 2}))]
        )
        return session_state

    def test_application_materialize(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("andes_downstream")
        dexml_ducsi = app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]

        # check incomplete filtering
        with pytest.raises(ValueError):
            app.materialize(dexml_ducsi[1])
        # incompatible input
        with pytest.raises(ValueError):
            app.materialize(str)

        # materialize all of the nodes
        assert app.materialize(dexml_ducsi[1]["*"]) == ["glue_table://booker/d_unified_cust_shipment_items/1/*"]
        assert app.materialize(dexml_ducsi[2]["*"]) == ["glue_table://booker/d_unified_cust_shipment_items/2/*"]
        assert app.materialize(dexml_ducsi[2]["2020-01-12"]) == ["glue_table://booker/d_unified_cust_shipment_items/2/2020-01-12 00:00:00"]
        # filtering on the first input not compatible
        with pytest.raises(ValueError):
            app.materialize(dexml_ducsi[2]["not a date"])

        # filter out of range
        with pytest.raises(ValueError):
            app.materialize(dexml_ducsi[3]["2020-01-12"])

        # check if material values are provided for a filter which is already 'material'
        with pytest.raises(ValueError):
            app.materialize(dexml_ducsi[1]["2020-01-12"], [1, "2020-01-01"])

        # check if material values are compatible with the input node/signal
        with pytest.raises(ValueError):
            app.materialize(dexml_ducsi, ["NA", "2020-01-01"])

        # check if material values are within the domain (dimension_filter_spec) of input signal
        # as you can see from the app above 'DEXML_DUCSI' declares filtering for region_id 1 and 2 only.
        with pytest.raises(ValueError):
            app.materialize(dexml_ducsi, [3, "2020-01-01"])

        # handle relative range explosion
        # 1- no material values so the relative range is left as is
        assert app.materialize(dexml_ducsi[2][:-7]) == ["glue_table://booker/d_unified_cust_shipment_items/2/_:-7"]

        # 2- input filter has an explicitly specified relative with material values are specified. honor the request.
        assert app.materialize(dexml_ducsi[2][:-3], [2, "2020-01-08"]) == [
            "glue_table://booker/d_unified_cust_shipment_items/2/2020-01-08 00:00:00",
            "glue_table://booker/d_unified_cust_shipment_items/2/2020-01-07 00:00:00",
            "glue_table://booker/d_unified_cust_shipment_items/2/2020-01-06 00:00:00",
        ]

        ship_options = app.get_data("ship_options", context=Application.QueryContext.DEV_CONTEXT)[0]
        assert app.materialize(ship_options) == ["glue_table://dexbi/d_ship_option"]

        repeat_ducsi = app.get_data("REPEAT_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        materialized_paths = app.materialize(repeat_ducsi[2]["2020-01-08"])
        assert len(materialized_paths) == 1
        assert materialized_paths[0].endswith("internal_data/REPEAT_DUCSI/2/2020-01-08 00:00:00")

        ducsi_with_so = app.get_data("DUCSI_WITH_SO", context=Application.QueryContext.DEV_CONTEXT)[0]
        # ducsi_with_so has an outfilter with relative range, let's see the behaviour when the input is material already,
        # it should return the TIP
        materialized_paths = app.materialize(ducsi_with_so[2]["2020-01-08"])
        assert len(materialized_paths) == 1
        assert materialized_paths[0].endswith("internal_data/DUCSI_WITH_SO/2/2020-01-08 00:00:00")

        self.patch_aws_stop()

    def test_application_process(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("andes_downstream")

        # try to process while not activated and fail
        ducsi_data = app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        with pytest.raises(RuntimeError):
            app.process(ducsi_data[1]["2020-10-01"])

        # activate and process (in SYNC mode) using 'repeat_ducsi'
        app.activate()
        # mock batch_compute response
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            assert materialized_output.alias == "REPEAT_DUCSI"
            return self.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_id")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            assert session_desc.session_id == "job_id"
            return self.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.process(
            ducsi_data[1]["2020-12-25"],
            # make it SYNC (use the local processor instance in sync mode)
            with_activated_processor=False,
        )
        assert app.platform.batch_compute.compute.call_count == 1
        assert app.platform.batch_compute.get_session_state.call_count == 1

        assert len(app.get_active_routes()) == 1
        assert app.get_active_routes()[0].route.route_id == "InternalDataNode-REPEAT_DUCSI"

        # now the second internal data node (route) in the system actually waits for its second input dependency
        # 'ship_options'. Previous process call with ducsi has created a pending node in it as well. a signal for
        # 'ship_options' will complete that pending node and cause a trigger.
        ship_options = app.get_data("ship_options", context=Application.QueryContext.DEV_CONTEXT)[0]
        # mock again
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            assert materialized_output.alias == "DUCSI_WITH_SO"
            return self.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_id2")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return self.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING, session_desc)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.process(
            ship_options,
            # make it SYNC (use the local processor instance in sync mode)
            with_activated_processor=False,
        )
        assert app.platform.batch_compute.compute.call_count == 1
        assert app.platform.batch_compute.get_session_state.call_count == 1

        assert len(app.get_active_routes()) == 2
        assert {route_record.route.route_id for route_record in app.get_active_routes()} == {
            "InternalDataNode-REPEAT_DUCSI",
            "InternalDataNode-DUCSI_WITH_SO",
        }

        # check idempotency
        app.process(ducsi_data[1]["2020-12-25"], with_activated_processor=False)
        # no effect (still the same count on the mock objects)
        assert app.platform.batch_compute.compute.call_count == 1
        assert len(app.get_active_routes()) == 2
        app.process(ship_options, with_activated_processor=False)
        assert app.platform.batch_compute.compute.call_count == 1

        # initiate another trigger on 'REPEAT_DUCSI' with a different partition (12-26)
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            assert materialized_output.alias == "REPEAT_DUCSI"
            return self.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_id3")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return self.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING, session_desc)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.process(ducsi_data[1]["2020-12-26"], with_activated_processor=False)
        assert app.platform.batch_compute.compute.call_count == 1
        assert len(app.get_active_route("REPEAT_DUCSI").active_compute_records) == 2

        # finish first two jobs (from 12-25 on both routes), since Processor is not running in the background now
        #   we will have to use related app API to force update RoutingTable status.
        # only active record remaining should be the most recent one (12-26):
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            raise RuntimeError(
                "This should not be called since we are not suppoed to yield a new active record" "at this point in this test"
            )

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            if session_desc.session_id in ["job_id", "job_id2"]:  # first two active records
                return self.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED, session_desc)
            else:
                return self.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING, session_desc)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.update_active_routes_status()
        assert app.platform.batch_compute.compute.call_count == 0
        assert app.platform.batch_compute.get_session_state.call_count == 3  # all (3) of the active compute records will be checked

        # only remaining active records is for '12-26' on route 'REPEAT-DUCSI'
        assert len(app.get_active_routes()) == 1
        assert app.get_active_routes()[0].route.route_id == "InternalDataNode-REPEAT_DUCSI"
        assert len(app.get_active_route("REPEAT_DUCSI").active_compute_records) == 1
        active_output = next(iter(app.get_active_route("REPEAT_DUCSI").active_compute_records)).materialized_output
        assert DimensionFilter.check_equivalence(
            active_output.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({1: {"2020-12-26": {}}})
        )
        assert app.has_active_record(app["REPEAT_DUCSI"][1]["2020-12-26"])
        assert not app.has_active_record(app["REPEAT_DUCSI"][1]["2020-12-25"])
        assert not app.has_active_record(app["REPEAT_DUCSI"][1]["2020-12-27"])

        self.patch_aws_stop()

    def test_application_poll_validations(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("andes_downstream")
        # - bad input type
        with pytest.raises(ValueError):
            app.poll(int)

        # - yes even a signal is not supported, since polling requires app-level (front-end) information that
        # can be provided by node types.
        with pytest.raises(ValueError):
            app.poll(
                Signal(
                    SignalType.INTERNAL_PARTITION_CREATION,
                    SignalSourceAccessSpec(SignalSourceType.INTERNAL, "/internal_data/data/{}", {}),
                    SignalDomainSpec(None, None, None),
                )
            )

        # - external data node not allowed
        with pytest.raises(ValueError):
            app.poll(app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0])

        # - try to poll on an inactive application.
        repeat_ducsi = app.get_data("REPEAT_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]

        # - input filter is not materialized
        with pytest.raises(ValueError):
            app.poll(repeat_ducsi[1]["*"])

        # - this will still fail since the data node does not exist in the active state of the app.
        # it is not possible since the app has not actually been activated yet.
        with pytest.raises(ValueError):
            app.poll(repeat_ducsi[1]["2020-12-26"])

        self.patch_aws_stop()

    def test_application_poll_on_active_route(self):
        """Test the scenario when we have the node active on the target partition (materialized output)."""
        self.patch_aws_start(glue_catalog_has_all_tables=True)
        flow.init_basic_logging(None, True, logging.CRITICAL)

        app = self._create_test_application("andes_downstream")

        # first activate
        app.activate()

        ducsi = app["DEXML_DUCSI"]
        repeat_ducsi = app["REPEAT_DUCSI"]
        # mock batch_compute APIs to make the route active.
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return self.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_id1")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            assert session_desc.session_id == "job_id1"
            return self.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.process(
            ducsi[1]["2020-12-25"],
            # make it SYNC (use the local processor instance in sync mode)
            with_activated_processor=False,
        )
        # at this point 'REPEAT_DUCSI' should have an active compute record (on [1]['2020-12-25']

        # we should let the app artificially & asynchronously go into the next-cycle and have it check on routes.
        # when it will be checking on the routes, will find out that active compute record is done (thx to this mock).
        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return self.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED)

        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        def next_cycle():
            time.sleep(15)  # delay to let poll happen first
            # app.process(self._next_processor_cycle_event(app))
            app.platform.routing_table.check_active_routes()

        processor_thread = threading.Thread(target=next_cycle, args=())
        processor_thread.start()

        materialized_path, _ = app.poll(repeat_ducsi[1]["2020-12-25"])
        assert materialized_path.endswith("internal_data/REPEAT_DUCSI/1/2020-12-25 00:00:00")
        # now the system should be back in idle mode
        assert len(app.get_active_routes()) == 0

        # poll again, this time will check the inactive records since there is no active execution
        # and return the same result (from the most recent execution above).
        materialized_path, _ = app.poll(repeat_ducsi[1]["2020-12-25"])
        assert materialized_path.endswith("internal_data/REPEAT_DUCSI/1/2020-12-25 00:00:00")

        # now let's check the scenario where poll catches up with the most recent execution and returns None
        # despite the result of the previous successful execution from above.
        # trigger update (a new execution) on same partition again.
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return self.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_id1_1")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            assert session_desc.session_id == "job_id1_1"
            return self.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.process(
            ducsi[1]["2020-12-25"],
            # make it SYNC (use the local processor instance in sync mode)
            with_activated_processor=False,
        )
        # at this point 'REPEAT_DUCSI' should have a new active compute record (on [1]['2020-12-25'])

        # BUT let's set things up in a way that this execution is going to fail, poll will detect this and won't
        # mistakenly return the path. Once the partition update is failed, it is invalidated logically and poll is
        # supposed to return None on it till a new successful execution.
        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            # ATTENTION: FAILED !!!
            return self.create_batch_compute_failed_session_state(ComputeFailedSessionStateType.APP_INTERNAL, session_desc)

        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        def next_cycle():
            time.sleep(15)  # delay to let poll happen first
            # app.process(self._next_processor_cycle_event(app))
            app.platform.routing_table.check_active_routes()

        processor_thread = threading.Thread(target=next_cycle, args=())
        processor_thread.start()

        path, compute_records = app.poll(repeat_ducsi[1]["2020-12-25"])
        assert path is None
        assert len(compute_records) == 1
        assert isinstance(compute_records[0].session_state, ComputeFailedSessionState)
        assert len(app.get_active_routes()) == 0

        def _datetime(str_value: str) -> int:
            return int(datetime.datetime.fromisoformat(str_value).timestamp())

        datum_lower_bound = int(datetime.datetime(1971, 1, 1).timestamp())
        datum_upper_bound = int((datetime.datetime.now() + datetime.timedelta(hours=1)).timestamp())
        # failing with datum below lower bound
        with pytest.raises(ValueError):
            app.poll(repeat_ducsi[1]["*"], datum=datum_upper_bound)

        # will fail with datum above upper bound
        with pytest.raises(ValueError):
            app.poll(repeat_ducsi[1]["2020-12-26"], datum=datum_lower_bound)

        # succeed with datum within range
        path, record = app.poll(repeat_ducsi[1]["2020-12-25"], datum=datetime.datetime(2020, 12, 25))
        assert path is None
        assert len(compute_records) == 1
        assert isinstance(compute_records[0].session_state, ComputeFailedSessionState)
        assert len(app.get_active_routes()) == 0

        self.patch_aws_stop()

    def test_application_poll_on_updated_route(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        # assume that this is session 1
        app = self._create_test_application("andes_downstream")
        # original state will be activated
        app.activate()

        # on another devpoint, assume that this is session 2 (post-activation)
        # initiate an update
        app = AWSApplication("andes_downstream", self.region)

        ducsi_data = app.marshal_external_data(
            GlueTable(
                "booker",
                "d_unified_cust_shipment_items",
                partition_keys=["region_id", "ship_day"],
            ),
            "DEXML_DUCSI",
            {"region_id": {"type": DimensionType.LONG, "ship_day": {"type": DimensionType.DATETIME}}},
            {
                "1": {
                    "*": {
                        "format": "%Y-%m-%d %H:%M:%S",
                        "timezone": "PST",
                    }
                },
            },
            SignalIntegrityProtocol("FILE_CHECK", {"file": ["SNAPSHOT"]}),
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

        self.patch_aws_stop()

    def test_application_poll_on_upstream_data(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("child_app")
        app.activate()

        from test.intelliflow.core.application.test_aws_application_create_and_query import TestAWSApplicationBuild

        upstream_app = TestAWSApplicationBuild._create_test_application(self, "parent_app")
        # connect them
        upstream_app.authorize_downstream("child_app", self.account_id, self.region)
        upstream_app.activate()

        app.import_upstream("parent_app", self.account_id, self.region)
        app.activate()

        # fetch from app (parent's data is visible within child's scope)
        eureka_data_from_parent = app["eureka_default_selection_data_over_two_days"]

        # data is not ready yet
        assert app.poll(eureka_data_from_parent["NA"]["2020-12-26"]) == (None, None)
        # also test the ability to check active records from parent app
        assert not app.has_active_record(eureka_data_from_parent["NA"]["2020-12-26"])
        assert app.poll(upstream_app["eureka_default_selection_data_over_two_days"]["NA"]["2020-12-26"]) == (None, None)

        # trigger upstream execution on 'eureka_default_selection_data_over_two_days' by injecting its two inputs.
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return self.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_id2_1")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            assert session_desc.session_id == "job_id2_1"
            return self.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING, session_desc)

        upstream_app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        upstream_app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        upstream_app.process(upstream_app["eureka_training_data"]["NA"]["2020-12-26"])
        upstream_app.process(upstream_app["eureka_training_all_data"]["2020-12-26"])

        # let the execution be succeeded when asked
        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            assert session_desc.session_id == "job_id2_1"
            return self.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED, session_desc)

        upstream_app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        def next_cycle():
            time.sleep(15)  # delay to let poll happen first
            # app.process(self._next_processor_cycle_event(app))
            upstream_app.platform.routing_table.check_active_routes()

        processor_thread = threading.Thread(target=next_cycle, args=())
        processor_thread.start()

        assert app.has_active_record(eureka_data_from_parent["NA"]["2020-12-26"])
        assert app.poll(eureka_data_from_parent["NA"]["2020-12-26"])[0].endswith(
            "internal_data/eureka_default_selection_data_over_two_days/NA/26-12-2020"
        )

        self.patch_aws_stop()

    def test_application_has_active_compute_record_validations(self):
        """Capture corner-cases for Application::has_active_records which are not handled in other test-cases.
        For other cases such as checking the records for an upstream data node please other test-cases
        (e.g test_application_poll_on_upstream_data)
        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("has_act_recs")
        # - bad input type
        with pytest.raises(ValueError):
            app.has_active_record(int)

        # - yes even a signal is not supported, since active record checking requires app-level (front-end) information that
        # can be provided by node types.
        with pytest.raises(ValueError):
            app.has_active_record(
                Signal(
                    SignalType.INTERNAL_PARTITION_CREATION,
                    SignalSourceAccessSpec(SignalSourceType.INTERNAL, "/internal_data/data/{}", {}),
                    SignalDomainSpec(None, None, None),
                )
            )

        # - external data node not allowed
        with pytest.raises(ValueError):
            app.has_active_record(app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0])

        # - try to check active records on an inactive application.
        repeat_ducsi = app.get_data("REPEAT_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]

        # - input filter is not materialized
        with pytest.raises(ValueError):
            app.has_active_record(repeat_ducsi[1]["*"])

        assert not app.has_active_record(repeat_ducsi[1]["2020-12-26"])

        app.activate()

        assert not app.has_active_record(repeat_ducsi[1]["2020-12-26"])

        self.patch_aws_stop()

    def test_application_execute_validations(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("andes_downstream")

        # - bad input type
        with pytest.raises(ValueError):
            app.execute(int)

        # - external data node not allowed
        ducsi_data = app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        with pytest.raises(ValueError):
            app.execute(ducsi_data[1]["2020-05-12"])

        alien_app = self._create_test_application("random")

        # - upstream or alien data cannot be executed
        with pytest.raises(ValueError):
            app.execute(alien_app.get_data("REPEAT_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0][1]["2020-05-12"])

        # - data node will be valid, however it won't have any material inputs.
        #   it should either be material and mappable on the inputs or have material versions of its inputs.
        target = app.get_data("REPEAT_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        # target has dimensions to be materialized (region, day), but here we pass the marshaler node as is.
        with pytest.raises(ValueError):
            app.execute(target)

        # - input (target) node will be materialized and we won't supply material inputs along with it.
        # however the problem will be related to our inability to map the materialized target to its inputs.
        # create a new data node to simulate this scenario (check 'output_dim_links')
        target2 = app.create_data(
            id="REPEAT_DUCSI2",
            inputs={
                "DEXML_DUCSI": ducsi_data,
            },
            output_dim_links=[
                # overwrite the link for 'ship_day' (trim hours, etc)
                ("ship_day", lambda dim: dim.strftime("%Y-%m-%d"), ducsi_data("ship_day"))
            ],
            compute_targets="output=DEXML_DUCSI",
        )
        with pytest.raises(ValueError):
            region_id = 1
            app.execute(target2[1]["2020-12-28"])

        # we have to activate now. if not then the following mocking will cause problem during the implicit
        # activation triggered by the execute. if activated alredy and the target node has not changed, then
        # implicit activation won't occur.
        # TODO how to handle PicklingError s when drivers are mocked?
        app.activate()

        # now check the happy-path to be a reference against false-negatives in the previous steps
        # show that 'REPEAT_DUCSI' can execute well if materialized.
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return self.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_1")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            # immediately finish the batch-compute (unrealistic but perfectly fits this test)
            # asynchronicity in this API will be thoroughly tested in the following tests below.
            return self.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        output_path = app.execute(target[1]["2020-12-28"])
        # s3://if-andes_downstream-123456789012-us-east-1/internal_data/REPEAT_DUCSI/1/2020-12-28 00:00:00
        assert output_path.endswith("internal_data/REPEAT_DUCSI/1/2020-12-28 00:00:00")

        self.patch_aws_stop()

    def test_application_execute_with_material_inputs(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("andes_downstream")

        ducsi_data = app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        repeat_ducsi = app.get_data("REPEAT_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]

        target2 = app.create_data(
            id="REPEAT_DUCSI2",
            inputs={"DEXML_DUCSI": ducsi_data, "REPEAT_DUCSI": repeat_ducsi, "REPEAT_DUCSI_RANGE": repeat_ducsi["*"][:-30]},
            compute_targets="output=DEXML_DUCSI.join(REPEAT_DUCSI, ['customer_id]).join(REPEAT_DUCSI_RANGE, ['customer_id']",
        )

        # we have to activate now. if not then the following mocking will cause problem during the implicit
        # activation triggered by the execute. if activated alredy and the target node has not changed, then
        # implicit activation won't occur.
        # TODO how to handle PicklingError s when drivers are mocked?
        app.activate()

        # now check the happy-path to be a reference against false-negatives in the previous steps
        # show that 'REPEAT_DUCSI' can execute well if materialized.
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return self.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_1")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return self.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        output_path = app.execute(target2, [ducsi_data[1]["2020-06-21"], repeat_ducsi[1]["2020-06-21"]])
        # s3://if-andes_downstream-123456789012-us-east-1/internal_data/REPEAT_DUCSI2/1/2020-06-21 00:00:00
        assert output_path.endswith("internal_data/REPEAT_DUCSI2/1/2020-06-21 00:00:00")

        # BAD INPUTS
        # unrelated input
        ship_options = app.get_data("ship_options", context=Application.QueryContext.DEV_CONTEXT)[0]
        with pytest.raises(ValueError):
            output_path = app.execute(target2, [ship_options, ducsi_data[1]["2020-06-21"], repeat_ducsi[1]["2020-06-21"]])

        # first input is not 'material'!
        with pytest.raises(ValueError):
            output_path = app.execute(target2, [ducsi_data, repeat_ducsi[1]["2020-06-21"]])

        # second input is not 'material'!
        with pytest.raises(ValueError):
            output_path = app.execute(target2, [ducsi_data[1]["2020-06-21"], repeat_ducsi])

        # inputs are not linked! (diff values on first dimensions)
        with pytest.raises(ValueError):
            output_path = app.execute(target2, [ducsi_data[1]["2020-06-21"], repeat_ducsi[2]["2020-06-21"]])

        # inputs are not linked! (diff values on second dimensions)
        with pytest.raises(ValueError):
            output_path = app.execute(target2, [ducsi_data[1]["2020-06-22"], repeat_ducsi[1]["2020-06-21"]])

        # we are not tolerating redundant bad inputs despite that first two inputs would actually form a trigger group.
        with pytest.raises(ValueError):
            output_path = app.execute(target2, [ducsi_data[1]["2020-06-21"], repeat_ducsi[1]["2020-06-21"], repeat_ducsi[1]["2020-06-22"]])

        # and now a successful run 'DUCSI_WITH_SO'
        ducsi_with_so = app["DUCSI_WITH_SO"]
        output_path = app.execute(ducsi_with_so, [ducsi_data[1]["2020-06-21"], ship_options])
        # s3://if-andes_downstream-123456789012-us-east-1/internal_data/DUCSI_WITH_SO/1/2020-06-21 00:00:00
        assert output_path.endswith("internal_data/DUCSI_WITH_SO/1/2020-06-21 00:00:00")

        self.patch_aws_stop()

    def test_application_execute_with_auto_materialized_inputs(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = self._create_test_application("andes_downstream")

        ducsi_data = app.get_data("DEXML_DUCSI", context=Application.QueryContext.DEV_CONTEXT)[0]
        ship_options = app.get_data("ship_options", context=Application.QueryContext.DEV_CONTEXT)[0]

        daily_timer = app.add_timer("daily_timer", "rate(1 day)", time_dimension_id="ship_day")

        daily_so = app.create_data(id="DAILY_SO", inputs=[daily_timer, ship_options], compute_targets="output=ship_options")

        join_with_daily_so = app.create_data(
            id="DUCSI_WITH_DAILY_SO",
            inputs={"DEXML_DUCSI": ducsi_data, "DAILY_SO": daily_so},
            compute_targets="output=DEXML_DUCSI.join(DAILY_SO, ['customer_ship_option])",
        )

        complex_query_with_references = app.create_data(
            id="DUCSI_MERGED_TWO_WEEKS_DAILY",
            inputs={
                "timer": daily_timer,
                "DEXML_DUCSI_LOWER_RANGE_WEEK": ducsi_data[1][:-7].ref,  # into the past
                "DAILY_DUCSI_HIGHER_RANGE_WEEK": ducsi_data[1][:7].ref,  # into the future
                "SHIP_OPTIONS": daily_so,
            },
            compute_targets="output=DEXML_DUCSI_LOWER_RANGE_WEEK.unionAll(DAILY_DUCSI_HIGHER_RANGE_WEEK)",
        )

        # we have to activate now. if not then the following mocking will cause problem during the implicit
        # activation triggered by the execute. if activated alredy and the target node has not changed, then
        # implicit activation won't occur.
        # TODO how to handle PicklingError s when drivers are mocked?
        app.activate()

        # now check the happy-path to be a reference against false-negatives in the previous steps
        # show that 'REPEAT_DUCSI' can execute well if materialized.
        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return self.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, "job_1")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            return self.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        output_path = app.execute(join_with_daily_so[1]["2020-06-21"])
        # s3://if-andes_downstream-123456789012-us-east-1/internal_data/DUCSI_WITH_DAILY_SO/1/2020-06-21 00:00:00
        assert output_path.endswith("internal_data/DUCSI_WITH_DAILY_SO/1/2020-06-21 00:00:00")

        # now execute another partition without blocking on the execution.
        app.execute(join_with_daily_so[1]["2020-06-22"], wait=False)
        output_path, _ = app.poll(join_with_daily_so[1]["2020-06-22"])
        assert output_path.endswith("internal_data/DUCSI_WITH_DAILY_SO/1/2020-06-22 00:00:00")

        output_path = app.execute(complex_query_with_references["2021-03-31"], wait=True)
        assert output_path.endswith("internal_data/DUCSI_MERGED_TWO_WEEKS_DAILY/2021-03-31")

        self.patch_aws_stop()

    def test_application_execute_with_complex_input_node(self):
        from datetime import timedelta, datetime

        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("tommy-asin-app", self.region)

        PIPELINE_DAY_DIMENSION = "day"

        daily_timer = app.add_timer("daily_timer", "rate(1 day)", time_dimension_id=PIPELINE_DAY_DIMENSION)

        # We are using the encryption key provided by Search team
        # TODO this might fail because I am using the one provided for "tommiy_asin_hourly"
        # Contact search team to get the right encryption key for the following bucket.
        tommy_asin_encryption_key = "arn:aws:kms:us-east-1:800261124827:key/5be55530-bb8e-4e95-8185-6e1afab0de54"

        tommy_daily = app.marshal_external_data(
            S3Dataset(
                "800261124827",
                "searchdata-core-tommy-asin-consolidated-parquet-prod",
                None,
                "{}",  # partition -> 'org'
                "{}",  # partition -> 'search_date'
                dataset_format=DataFormat.PARQUET,
            ).link(
                # Link the PROXY !!!
                # events coming from this proxy/link will yield valid Signals/triggers in the system
                GlueTable(database="searchdata", table_name="tommy_asin")
            ),
            "tommy_daily",
            {
                "org": {
                    "type": DimensionType.STRING,
                    "format": lambda dim: dim.lower(),
                    # As long as insensitive == True, case on the dimension value within this filter would not make a diff (after the recent bug-fix within this API that did not apply spec params to filter).
                    # Whatever the value here will be exposed post-fomatting (lower in the case) internally and also externally as it is being materialized (as part of S3 path generation for example).
                    # The param that takes care of runtime comparison (against incoming Glue events) is 'insensitive' as used here.
                    "insensitive": True,
                    PIPELINE_DAY_DIMENSION: {  # we can use whatever dimension/partition name we want internally
                        "type": DimensionType.LONG
                        # have to use LONG here since it will be inferred as LONG at runtime.
                        # search team's format choice is just terrible (e.g 20210526)
                    },
                }
            },
            {"*": {"*": {}}},
        )

        # import in a different way:
        #    - use UPPER with String dimension (first dim)
        #    - use DATETIME on the second dimension
        tommy_daily2 = app.marshal_external_data(
            S3Dataset(
                "800261124827",
                "searchdata-core-tommy-asin-consolidated-parquet-prod",
                None,
                "{}",  # partition -> 'org'
                "{}",  # partition -> 'search_date'
                dataset_format=DataFormat.PARQUET,
            ).link(
                # Link the PROXY !!!
                # events coming from this proxy/link will yield valid Signals/triggers in the system
                GlueTable(database="searchdata", table_name="tommy_asin")
            ),
            "tommy_daily2",
            {
                "org": {
                    "type": DimensionType.STRING,
                    # use upper!
                    "format": lambda dim: dim.upper(),
                    "insensitive": True,
                    PIPELINE_DAY_DIMENSION: {  # we can use whatever dimension/partition name we want internally
                        "type": DimensionType.DATETIME,
                        "format": "%Y-%m-%d",
                    },
                }
            },
            {"*": {"*": {}}},
        )

        tommy_daily_etl = app.create_data(
            "tommy_daily_etl",
            inputs=[
                tommy_daily,
                # this new data will pick its dimensions implicitly from tommy since it is the first input
                daily_timer,
            ],
            compute_targets=[NOOPCompute],
            output_dimension_spec={
                "org": {
                    type: DimensionType.STRING,
                    PIPELINE_DAY_DIMENSION: {
                        # Convert the 'day' format into a proper datetime internally, downstream nodes won't have to do this
                        "format": "%Y-%m-%d",
                        type: DimensionType.DATETIME,
                    },
                }
            },
            # NOW provide the rule to convert from tommy_asin's LONG formatted date to internal %Y-%m-%d
            #  example: 20210531 -> 2021-05-31
            output_dim_links=[
                # please note that link for 'org' is not required since output auto linking is active.
                (  # TO
                    PIPELINE_DAY_DIMENSION,
                    # MAP/ OPERATOR in Python lambda
                    lambda dim: (datetime(int(str(dim)[:4]), int(str(dim)[4:6]), int(str(dim)[6:8]))),
                    # FROM
                    tommy_daily(PIPELINE_DAY_DIMENSION),
                ),
                # this outputs DAY is Equals to daily_timer('day')
                (PIPELINE_DAY_DIMENSION, EQUALS, daily_timer(PIPELINE_DAY_DIMENSION)),
            ],
            input_dim_links=[
                # please note that link for 'org' is not required since output auto linking is active.
                (  # TO
                    daily_timer(PIPELINE_DAY_DIMENSION),
                    # MAP/ OPERATOR in Python lambda (another way doing the same as above)
                    # 20210531 -> 2021-05-31
                    lambda dim: f"{str(dim)[:4]}-{str(dim)[4:6]}-{str(dim)[6:8]}",
                    # FROM
                    tommy_daily(PIPELINE_DAY_DIMENSION),
                )
            ],
        )

        app.validate(tommy_daily_etl, tommy_daily["us"]["20210526"])
        # expose the linking issue if the LONG typed hour dimension on the input is different
        # User provided link will raise this as expected at runtime
        with pytest.raises(RuntimeError):
            app.validate(tommy_daily_etl, tommy_daily["us"]["111"])

        # show how simple it is to rely on auto spec adaptation from the first input and auto-input linking since
        # both inputs also match on dimension type (datetime) and name ('day' [PIPELINE_DAY_DIMENSION])
        tommy_daily_etl2 = app.create_data(
            "tommy_daily_etl2",
            inputs=[
                # this new data will pick its dimensions implicitly from tommy since it is the first input
                tommy_daily2,
                daily_timer,
            ],
            compute_targets=[NOOPCompute],
        )

        # save the pipeline
        app.activate()

        tommy_daily_etl.describe()

        output_path = app.execute(tommy_daily_etl, tommy_daily["us"]["20210526"])
        assert output_path.endswith("internal_data/tommy_daily_etl/us/2021-05-26")

        output_path = app.execute(tommy_daily_etl, [tommy_daily["US"]["20210525"], daily_timer["2021-05-25"]])
        assert output_path.endswith("internal_data/tommy_daily_etl/us/2021-05-25")

        with pytest.raises(ValueError):
            # since mapper function is not trivial and we currently don't have a link from output('day') to tommy_asin('day')
            app.execute(tommy_daily_etl["us"]["2021-05-25"])

        with pytest.raises(ValueError):
            # tommy_daily uses LONG but %Y-%m-%d cannot be converted to LONG, basically a typing error.
            app.execute(tommy_daily_etl, tommy_daily["US"]["2021-05-25"])

        # now test the other version
        output_path = app.execute(tommy_daily_etl2, tommy_daily2["us"]["20210526"])
        assert output_path.endswith("internal_data/tommy_daily_etl2/US/2021-05-26")  # check us -> US

        # now update the input dim link to have a mapping to tommy_daily from the rest (thanks to daily_timer)
        tommy_daily_etl = app.update_data(
            "tommy_daily_etl",
            inputs=[
                tommy_daily,
                # this new data will pick its dimensions implicitly from tommy since it is the first input
                daily_timer,
            ],
            compute_targets=[NOOPCompute],
            output_dimension_spec={
                "org": {
                    type: DimensionType.STRING,
                    PIPELINE_DAY_DIMENSION: {
                        # Convert the 'day' format into a proper datetime internally, downstream nodes won't have to do this
                        "format": "%Y-%m-%d",
                        type: DimensionType.DATETIME,
                    },
                }
            },
            # NOW provide the rule to convert from tommy_asin's LONG formatted date to internal %Y-%m-%d
            #  example: 20210531 -> 2021-05-31
            output_dim_links=[
                # please note that link for 'org' is not required since output auto linking is active.
                (  # TO
                    PIPELINE_DAY_DIMENSION,
                    # MAP/ OPERATOR in Python lambda
                    lambda dim: (datetime(int(str(dim)[:4]), int(str(dim)[4:6]), int(str(dim)[6:8]))),
                    # FROM
                    tommy_daily(PIPELINE_DAY_DIMENSION),
                ),
                # this outputs DAY is Equals to daily_timer('day')
                (PIPELINE_DAY_DIMENSION, EQUALS, daily_timer(PIPELINE_DAY_DIMENSION)),
            ],
            input_dim_links=[
                # please note that link for 'org' is not required since output auto linking is active.
                (  # TO
                    daily_timer(PIPELINE_DAY_DIMENSION),
                    # MAP/ OPERATOR in Python lambda (another way doing the same as above)
                    # 20210531 -> 2021-05-31
                    lambda dim: f"{str(dim)[:4]}-{str(dim)[4:6]}-{str(dim)[6:8]}",
                    # FROM
                    tommy_daily(PIPELINE_DAY_DIMENSION),
                ),
                # NECESSARY MAPPING! This completes the graph, now RheocerOS can materialize tommy_asin from others
                (  # TO
                    tommy_daily(PIPELINE_DAY_DIMENSION),
                    # 2021-05-31 -> 20210531
                    lambda dim: int(dim.strftime("%Y%m%d")),
                    # FROM
                    daily_timer(PIPELINE_DAY_DIMENSION),
                ),
            ],
        )

        # now using the materialized output, anything can be materialized and execution would be started.
        app.execute(tommy_daily_etl["us"]["2021-05-25"])

        self.patch_aws_stop()

    def test_application_execute_implicit_activation(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("exec-act", self.region)

        # temporary hack until we have a testing anchor on Application,
        # to track activate calls without disturbing the internal sequence.
        app._activate_original = app.activate

        def activate_hook():
            app._activate_original()

        app.activate = MagicMock(side_effect=activate_hook)

        assert app.activate.call_count == 0

        def my_lambda_reactor(input_map: Dict[str, Signal], materialized_output: Signal, params: "ConstructParamsDict") -> Any:
            from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams

            pass

        node_1_1 = app.create_data(id=f"Node_1_1", compute_targets=[InlinedCompute(my_lambda_reactor), NOOPCompute])

        def on_exec_begin_hook(
            routing_table: "RoutingTable",
            route_record: "RoutingTable.RouteRecord",
            execution_context: "Route.ExecutionContext",
            current_timestamp_in_utc: int,
            **params,
        ):
            pass

        app.execute(node_1_1)
        # verify that activation was called implicitly
        assert app.activate.call_count == 1

        app.execute(node_1_1)
        # verify that activation NOT called the second time
        assert app.activate.call_count == 1

        node_1_2 = app.create_data(
            id=f"Node_1_2",
            inputs=[node_1_1],
            compute_targets=[NOOPCompute],
            execution_hook=RouteExecutionHook(on_exec_begin=on_exec_begin_hook),
        )

        app.execute(node_1_1)
        # verify that activation was called implicitly due to change in the dependency tree of the first node (a new child added)
        assert app.activate.call_count == 2

        def on_exec_begin_hook_new(*args, **params):
            print("new hook")

        # change the hook and verify implicit activation
        node_1_2 = app.update_data(
            id=f"Node_1_2",
            inputs=[node_1_1],
            compute_targets=[NOOPCompute],
            execution_hook=RouteExecutionHook(on_exec_begin=on_exec_begin_hook_new),
        )

        app.execute(node_1_1)
        # verify that activation was called implicitly due to change in the hook of second node
        assert app.activate.call_count == 3

        def on_exec_failure_hook(
            routing_table: "RoutingTable",
            route_record: "RoutingTable.RouteRecord",
            execution_context_id: str,
            materialized_inputs: List[Signal],
            materialized_output: Signal,
            current_timestamp_in_utc: int,
            **params,
        ) -> None:
            pass

        node_1_3 = app.create_data(
            id=f"Node_1_3",
            inputs=[node_1_1],
            compute_targets=[InlinedCompute(lambda input_map, output, params: int("foo"))],
            execution_hook=RouteExecutionHook(on_failure=on_exec_failure_hook),
        )

        app.execute(node_1_1)
        # verify that activation was called implicitly due to change in the dependency tree of the first node (a new child added)
        assert app.activate.call_count == 4

        # update the first node and verify that no activation should occur
        node_1_1 = app.update_data(id=f"Node_1_1", compute_targets=[InlinedCompute(my_lambda_reactor), NOOPCompute])

        app.execute(node_1_1)
        # no activation!
        assert app.activate.call_count == 4

        def new_reactor(*args, **kwargs):
            pass

        # update the first node to change compute target
        node_1_1 = app.update_data(
            id=f"Node_1_1", compute_targets=[InlinedCompute(new_reactor), InlinedCompute(my_lambda_reactor), NOOPCompute]
        )

        app.execute(node_1_1)
        # verify that activation was called implicitly due to change in the dependency tree of the first node (a new child added)
        assert app.activate.call_count == 5

        app.execute(node_1_1)
        # verify that activation should not occur
        assert app.activate.call_count == 5

        def retry_hook(*args, **kwargs):
            pass

        # verify that a new hook should trigger implicit activation
        node_1_1 = app.update_data(
            id=f"Node_1_1",
            compute_targets=[InlinedCompute(new_reactor), InlinedCompute(my_lambda_reactor), NOOPCompute],
            execution_hook=RouteExecutionHook(on_compute_retry=retry_hook),
        )

        app.execute(node_1_1)
        # verify that activation should occur
        assert app.activate.call_count == 6

        # patch the 3rd node with same hook
        node_1_3 = app.patch_data("Node_1_3", execution_hook=RouteExecutionHook(on_failure=on_exec_failure_hook))

        app.execute(node_1_1)
        # verify that activation should not occur
        assert app.activate.call_count == 6

        # patch the 3rd node with a new hook (new callback)
        node_1_3 = app.patch_data(
            "Node_1_3", execution_hook=RouteExecutionHook(on_failure=on_exec_failure_hook, on_compute_retry=retry_hook)
        )

        app.execute(node_1_1)
        # verify that activation should occur
        assert app.activate.call_count == 7

        # remove hook altogether and verify that there will be activation again
        node_1_3 = app.patch_data("Node_1_3", execution_hook=RouteExecutionHook())

        app.execute(node_1_1)
        # verify that activation should occur
        assert app.activate.call_count == 8

        # finally execute on other nodes and verify that no activation would occur
        app.execute(node_1_2)
        with pytest.raises(RuntimeError):
            app.execute(node_1_3)  # designed to fail (execute maps execution failure to runtime on user side)
        # verify that activation should NOT occur
        assert app.activate.call_count == 8

        self.patch_aws_stop()

    def test_application_kill_batch_compute(self):
        """
        This test wants to capture the orchestration behaviour when user wants to forcefully kill an execution on a
        node.

        Test is designed to mock BatchCompute interface in such a way that;
           - An execution is started and then killed for two different nodes
           - For each node, there will be one call to 'BatchCompute::compute' which triggers the execution initially
           - There will be multiple calls to 'BatchCompute::get_session_state', the ones before the 'BatchCompute::kill'
           call will return PROCESSING, and the one after the BatchCompute::kill call will return STOPPED
           - 'kill' API will set a global param killed and 'get_session_state' will use it to return STOPPED.
        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("test-kill", self.region)

        test_node = app.create_data("test_node", compute_targets="NOT IMPORTANT")

        test_node_with_dimensions = app.create_data(
            "test_node2", compute_targets="NOT IMPORTANT", output_dimension_spec={"dim_name": {type: DimensionType.DATETIME}}
        )

        app.activate()

        kill_state: Dict[str, bool] = dict()

        def compute(
            route: Route,
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            return self.create_batch_compute_response(ComputeSuccessfulResponseType.PROCESSING, f"{materialized_output.alias}_job_id")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            nonlocal kill_state
            if not kill_state.get(active_compute_record.materialized_output.alias, False):
                return self.create_batch_compute_session_state(ComputeSessionStateType.PROCESSING, session_desc)
            else:
                return self.create_batch_compute_failed_session_state(ComputeFailedSessionStateType.STOPPED, session_desc)

        def kill_session(active_compute_record: "RoutingTable.ComputeRecord") -> None:
            nonlocal kill_state
            kill_state[active_compute_record.materialized_output.alias] = True

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        app.platform.batch_compute.kill_session = MagicMock(side_effect=kill_session)

        app.execute(test_node, wait=False)
        app.execute(test_node_with_dimensions["2021-10-25"], wait=False)

        # in unit-tests Processor is not running, emulate Processor next-cycle
        app.update_active_routes_status()  # within this call orchestration will get PROCESSING so executions still on
        assert len(app.get_active_routes()) == 2
        assert len(app.get_active_route("test_node").active_compute_records) == 1
        assert len(app.get_active_route("test_node2").active_compute_records) == 1
        # emulate second cycle, nothing should change
        app.update_active_routes_status()  # still will return PROCESSING
        assert len(app.get_active_route("test_node").active_compute_records) == 1
        assert len(app.get_active_route("test_node2").active_compute_records) == 1
        assert app.has_active_record(test_node)
        assert app.has_active_record(test_node_with_dimensions["2021-10-25"])

        # now kill it
        assert app.kill(test_node)

        app.update_active_routes_status()  # next cycle will get STOPPED from get_session_state and end the 1st exec.
        assert len(app.get_active_routes()) == 1
        assert not app.has_active_record(test_node)
        assert len(app.get_active_route("test_node").active_compute_records) == 0
        assert len(app.get_active_route("test_node2").active_compute_records) == 1
        # now check with poll
        path, compute_records = app.poll(test_node)
        assert not path  # since it was not forcefully stopped, most recent execution invalidated the resource path.

        # test unmaterialized input error (date partition must be provided)
        with pytest.raises(ValueError):
            app.kill(test_node_with_dimensions)

        # now attempt to kill the remaining execution but using a wrong a partition which does not have exec on it.
        # the effect should be NOOP (since there is no exec on this partition)
        assert not app.kill(test_node_with_dimensions["2021-10-26"])  # should be 25!
        app.update_active_routes_status()
        assert len(app.get_active_routes()) == 1  # still ON
        assert len(app.get_active_route("test_node2").active_compute_records) == 1  # still ON

        # Now kill the actual execution using the right partition
        assert app.kill(test_node_with_dimensions["2021-10-25"])

        app.update_active_routes_status()  # next cycle will get STOPPED from get_session_state and end the 2nd exec.
        assert len(app.get_active_routes()) == 0
        assert len(app.get_active_route("test_node2").active_compute_records) == 0
        path, compute_records = app.poll(test_node_with_dimensions["2021-10-25"])
        assert not path

        self.patch_aws_stop()

    def test_application_kill_validations(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("test-kill", self.region)

        new_node = app.create_data("new_dummy_node", compute_targets="DUMMY")

        # app not activated, cannot kill
        with pytest.raises(ValueError):
            app.kill(new_node)

        with pytest.raises(ValueError):
            app.kill("new_dummy_node")  # wrong input type before the activation

        app.activate()

        with pytest.raises(ValueError):
            app.kill("new_dummy_node")  # wrong input type after the activation

        # kill is NOOP here, should not cause any trouble tough
        app.kill(new_node)

        external_data = app.marshal_external_data(
            S3Dataset("111222333444", "bucket", None, "{}"),
            "dummy_external_data",
            {
                "region": {
                    "type": DimensionType.STRING,
                }
            },
        )
        # can only kill internal nodes (before the activation)
        with pytest.raises(ValueError):
            app.kill(external_data["NA"])

        app.activate()

        # can only kill internal nodes (after the activation)
        with pytest.raises(ValueError):
            app.kill(external_data["NA"])

        self.patch_aws_stop()
