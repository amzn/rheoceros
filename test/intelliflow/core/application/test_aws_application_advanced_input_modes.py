# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# TODO see the integ-test version and patch / decorate it for unit-tests.
import time
from datetime import datetime

import pytest

from intelliflow.api_ext import *
from intelliflow.core.platform.definitions.aws.s3.object_wrapper import object_exists
from intelliflow.core.signal_processing.dimension_constructs import StringVariant
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.data_emulation import add_test_data
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationAdvancedInputModes(AWSTestBase):
    """
    This covers advanced input modes such as 'reference' and 'range_check'.

    'reference' inputs dont mandate event-polling on them and can be used on data which just needs to be inputted
    to the compute side.

    'range_check' controls whether a 'ranged' input (with multiple materialized paths, as in a range of data partitions)
    will checked for the existence of data spawning its entire domain. For example, if you want to make sure that a
    trigger will happen only all of the data partitions from a range of a dataset are ready then you use this flag on
    an input in Application::create_data.
    """

    app: AWSApplication = None

    def _create_app(self) -> AWSApplication:
        def invoke_lambda_function(lambda_client, function_name, function_params, is_async=True):
            """Synchronize internal async lambda invocations so that the chaining would be active during testing."""
            return self.app.platform.processor.process(function_params, use_activated_instance=False)

        self.patch_aws_start(glue_catalog_has_all_tables=True, invoke_lambda_function_mock=invoke_lambda_function)

        self.app = AWSApplication("advanced-inputs", self.region)
        return self.app

    def test_application_reference_inputs_in_different_layers(self):
        app = self._create_app()

        eureka_offline_training_data_UNMATERIALIZED = app.add_external_data(
            data_id="eureka_training_data_UNMATERIALIZED",
            s3_dataset=S3(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data",
                AnyString("region"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
            ),
        )

        eureka_offline_training_data_UNMATERIALIZED_with_diff_region_name = app.add_external_data(
            data_id="eureka_training_data_UNMATERIALIZED_with_different_dimension_name",
            s3_dataset=S3(
                "427809481713",
                "dex-ml-eureka-model-training-data-2",
                "cradle_eureka_p3/v8_00/training-data",
                AnyString("region_DIFFERENT"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
            ),
        )

        eureka_offline_training_data = app.add_external_data(
            data_id="eureka_training_data",
            s3_dataset=S3(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data",
                StringVariant("NA", "region"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
            ),
        )

        eureka_offline_all_data = app.marshal_external_data(
            S3Dataset(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/all-data",
                "partition_day={}",
                dataset_format=DataFormat.CSV,
            ),
            "eureka_training_all_data",
            {"day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}},
            {"*": {}},
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        # first validate reference only input sets are not accepted
        with pytest.raises(ValueError):
            app.create_data(id="should_fail", inputs=[eureka_offline_all_data[:-7].as_reference()], compute_targets=[NOOPCompute])

        # even if reference is fully materialized, as the sole input, it be rejected.
        with pytest.raises(ValueError):
            app.create_data(
                id="should_fail1_1", inputs=[eureka_offline_all_data["2021-10-01"].as_reference()], compute_targets=[NOOPCompute]
            )

        # now test the scenarios where the first input is dependent (reference or nearest)
        # in these cases, output dimension spec is still implicitly set as the spec of
        # the first dependent signal and unmaterialized dimensions are opportunistically mapped
        # from the rest of the inputs (if necessary).
        app.create_data(
            id="should_NOT_fail1",
            inputs=[
                eureka_offline_all_data[:-7].as_reference(),
                # output dimension 'day' will be mapped from the
                # independent signal here
                eureka_offline_training_data_UNMATERIALIZED["*"][:-2],
            ],
            compute_targets=[NOOPCompute],
        )

        app.create_data(
            id="should_NOT_fail2",
            inputs=[
                # 'region' will be mapped from this (dependent/reference input)
                # because it is materialized already
                eureka_offline_training_data["NA"][:-2].as_reference(),
                # immaterial 'day' dimension of the output  will be mapped from this independent signal
                eureka_offline_all_data[:-2],
            ],
            compute_targets=[NOOPCompute],
        )

        with pytest.raises(ValueError):
            app.create_data(
                id="should_fail2_0",
                inputs=[
                    # first reference input's 'region' is  unmaterialized and cannot be satisfied by
                    # other inputs either.
                    eureka_offline_training_data_UNMATERIALIZED["*"][:-2].ref,
                    eureka_offline_training_data_UNMATERIALIZED_with_diff_region_name,
                ],
                compute_targets=[NOOPCompute],
            )

        # check other scenarios where first input is a regular independent signal
        with pytest.raises(ValueError):
            app.create_data(
                id="should_fail2",
                inputs=[
                    eureka_offline_all_data[:-2],
                    # unmaterialized ('*' [any region]) and unmapped dimension on reference no allowed
                    eureka_offline_training_data_UNMATERIALIZED["*"][:-2].ref,
                ],
                compute_targets=[NOOPCompute],
            )

        default_selection_features = app.create_data(
            id="eureka_default",
            inputs={
                "offline_training_data": eureka_offline_training_data,
                # REFERENCE! Routing runtime won't wait for an event on this one as long as
                # it can be mapped from the dimensions of the other non-reference input.
                "offline_data": eureka_offline_all_data[:-2].as_reference(),
            },
            compute_targets=[NOOPCompute],  # when inputs are ready, trigger the following
        )

        d_ad_orders_na = app.marshal_external_data(
            GlueTable("dex_ml_catalog", "d_ad_orders_na", partition_keys=["order_day"]),
            "d_ad_orders_na",
            {"order_day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d", "timezone": "PST"}},
            {"*": {}},
        )

        merge_eureka_default_with_AD = app.create_data(
            id="REPEAT_AD_ORDERS",
            # please note that inputs won't be linked since their dimension names are diff
            # and also there is no input_dim_links provided. this enables the following weird
            # join of partitions from different dates.
            inputs=[default_selection_features, d_ad_orders_na["2021-01-13"].as_reference()],  # or no_event or ref or reference()
            compute_targets=[NOOPCompute],
        )

        app.activate(allow_concurrent_executions=False)

        # now this should create an avalanche in the pipeline down to the final node
        app.process(eureka_offline_training_data["NA"]["2020-03-18"], with_activated_processor=True)

        # 1- check the first node and poll till it ends (check the console for updates on execution state).
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        assert path

        # 2- now the second node must have been started
        path, _ = app.poll(merge_eureka_default_with_AD["NA"]["2020-03-18"])
        assert path

        self.patch_aws_stop()

    def test_application_range_check_succeeds_via_event_ingestion(self):
        app = self._create_app()

        eureka_offline_training_data = app.add_external_data(
            data_id="eureka_training_data",
            s3_dataset=S3(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data",
                StringVariant("NA", "region"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
            ),
        )

        default_selection_features = app.create_data(
            id="eureka_default",
            inputs={
                # Range !!!
                "offline_training_data": eureka_offline_training_data["NA"][:-3].range_check(True),
            },
            compute_targets=[NOOPCompute],  # when inputs are ready, trigger the following
        )

        second_layer_ranged_data = app.create_data(
            id="ranged_internal_data", inputs=[default_selection_features["NA"][:-2].range_check(True)], compute_targets=[NOOPCompute]
        )

        app.activate(allow_concurrent_executions=False)

        # CREATE THE TIP (a trigger group/node) @ NA + 2020-03-18 (target partition/execution for this test)
        # now since 'range_check' is enabled, it will need:
        #   - 2020-03-17
        #   - 2020-03-16
        app.process(eureka_offline_training_data["NA"]["2020-03-18"])

        # it won't yield anything since range_check fails due to missing partitions
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        assert not path

        # still needs:
        #   - 2020-03-16
        app.process(eureka_offline_training_data["NA"]["2020-03-17"])
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        assert not path

        app.process(eureka_offline_training_data["NA"]["2020-03-16"])
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        # DONE!
        assert path

        # NOW check the second layer for 2020-03-19
        # - 2020-03-18 is READY
        # - 2020-03-19 (TIP) is NOT READY
        path, _ = app.poll(second_layer_ranged_data["NA"]["2020-03-19"])
        assert not path

        # synthetically feed default_selection_features 2020-03-19 into the system
        # external data will trigger 1st layer and then in turn it will feed the 2nd layer.
        # (this pending_node [with TIP at 2020-03-18 is consumed], let's mark it as complete and pull will succeed on it
        add_test_data(app, eureka_offline_training_data["NA"]["2020-03-18"], "_SUCCESS", "")
        app.process(eureka_offline_training_data["NA"]["2020-03-19"])
        path, _ = app.poll(default_selection_features["NA"]["2020-03-19"])
        # 1st layer
        assert path
        path, _ = app.poll(second_layer_ranged_data["NA"]["2020-03-19"])
        # 2nd layer DONE !
        assert path

        self.patch_aws_stop()

    def test_application_range_check_succeeds_via_event_ingestion_reverse_order(self):
        app = self._create_app()

        eureka_offline_training_data = app.add_external_data(
            data_id="eureka_training_data",
            s3_dataset=S3(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data",
                StringVariant("NA", "region"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
            ),
        )

        default_selection_features = app.create_data(
            id="eureka_default",
            inputs=[
                # Range !!!
                eureka_offline_training_data["NA"][:-3].range_check(True)
            ],
            compute_targets=[NOOPCompute],  # when inputs are ready, trigger the following
        )

        # this second node is important to check the utilization of Zombie nodes as part of the range_check.
        # zombie nodes are event trigger groups which will always be partially satisfied to do linking conditions.
        # For example; in the node below, second input mandates '2020-03-18' and the user is relying on default
        # 'auto_input_dim_linking = True' which auto-links 'region' and 'day' dimensions on two of the inputs.
        # This means that a runtime node completion/trigger/execution would only happen when 'day'='2020-03-18'.
        # But this does not mean that the system will discard runtime pending nodes that will created for incoming
        # events with different days (03-16 and 03-17). These nodes are zombie nodes and when 2020-03-18 node is
        # evaluated (created), those nodes are used to retrieve completion information to avoid unnecessary remote IO.
        default_selection_features2 = app.create_data(
            id="eureka_default_specific_date",
            inputs={
                # Range !!!
                "alias1": eureka_offline_training_data["NA"][:-3].range_check(True),
                "alias2": eureka_offline_training_data["NA"]["2020-03-18"],
            },
            compute_targets=[NOOPCompute],  # when inputs are ready, trigger the following
        )

        app.activate(allow_concurrent_executions=False)

        # Simulate a typical scenario where events are received chronologically sorted.
        app.process(eureka_offline_training_data["NA"]["2020-03-16"])
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        assert not path
        assert len(app.get_active_route(default_selection_features2).route.pending_nodes) == 1
        assert next(iter(app.get_active_route(default_selection_features2).route.pending_nodes)).is_zombie

        app.process(eureka_offline_training_data["NA"]["2020-03-17"])
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        assert not path
        assert len(app.get_active_route(default_selection_features2).route.pending_nodes) == 2
        assert all([pending_node.is_zombie for pending_node in app.get_active_route(default_selection_features2).route.pending_nodes])

        app.process(eureka_offline_training_data["NA"]["2020-03-18"])
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        # DONE! range_check uses existing pending nodes (for 03-16 and 03-17) to complete the node
        assert path
        # DONE! second route (that has zombie event groups) must have yielded a succesful execution on 2020-03-18.
        path, _ = app.poll(default_selection_features2["NA"]["2020-03-18"])

        # we don't garbage-collect zombies at this moment. at a safe-guard for future change in the future to promote
        # further testing on this behaviour.
        assert len(app.get_active_route(default_selection_features2).route.pending_nodes) == 2
        assert all([pending_node.is_zombie for pending_node in app.get_active_route(default_selection_features2).route.pending_nodes])

        self.patch_aws_stop()

    def test_application_range_check_succeeds_via_poll(self):
        app = self._create_app()

        bucket_name = "dex-ml-eureka-model-training-data"

        eureka_offline_training_data = app.add_external_data(
            data_id="eureka_training_data",
            s3_dataset=S3(
                "427809481713",
                bucket_name,
                "cradle_eureka_p3/v8_00/training-data",
                StringVariant("NA", "region"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
            ),
            completion_file="_SUCCESS",
        )

        default_selection_features = app.create_data(
            id="eureka_default",
            inputs={
                # Range !!!
                "offline_training_data": eureka_offline_training_data["NA"][:-3].check_range(),
            },
            compute_targets=[NOOPCompute],  # when inputs are ready, trigger the following
        )

        app.activate(allow_concurrent_executions=False)

        # CREATE THE TIP (a trigger group/node) @ NA + 2020-03-18 (target partition/execution for this test)
        # now since 'range_check' is enabled, it will need:
        #   - 2020-03-17
        #   - 2020-03-16
        app.process(eureka_offline_training_data["NA"]["2020-03-18"])

        # it won't yield anything since range_check fails due to missing partitions
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        assert not path

        # emulate partition update in the background
        add_test_data(app, eureka_offline_training_data["NA"]["2020-03-17"], "_SUCCESS", "")
        # s3 = app.platform.routing_table.session.resource("s3", region_name="us-east-1")
        # bucket = get_bucket(s3, bucket_name)
        # assert object_exists(s3, bucket, app.materialize(eureka_offline_training_data['NA']['2020-03-17'])[0].replace(f"s3://{bucket_name}/", "") + "/" + "_SUCCESS")
        # since this is unit-test, Processor is not working remotely so we have to manually trigger the next
        # next-cycle in the route state check (active routes, pending node for expiration and range-checks)
        app.update_active_routes_status()

        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        # still needs:
        #   - 2020-03-16
        assert not path

        add_test_data(app, eureka_offline_training_data["NA"]["2020-03-16"], "_SUCCESS", "")
        app.update_active_routes_status()
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        # DONE!
        assert path

        self.patch_aws_stop()

    def test_application_nearest_input_basic(self):
        app = self._create_app()

        eureka_offline_training_data = app.add_external_data(
            data_id="eureka_training_data",
            s3_dataset=S3(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data",
                StringVariant("NA", "region"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
            ),
        )

        eureka_offline_all_data = app.marshal_external_data(
            S3Dataset(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/all-data",
                "partition_day={}",
                dataset_format=DataFormat.CSV,
            ),
            "eureka_training_all_data",
            {"day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}},
            {"*": {}},
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        default_selection_features = app.create_data(
            id="eureka_default",
            inputs={"offline_training_data": eureka_offline_training_data, "offline_data": eureka_offline_all_data[:-7].nearest()},
            compute_targets=[NOOPCompute],  # when inputs are ready, trigger the following
        )

        # happy path, system will satisfy both inputs and nearest will operate as a normal input over the same date
        # partition. no difference here.
        assert app.execute(default_selection_features["NA"][datetime.now()])
        assert app.poll(default_selection_features["NA"][datetime.now()])[0]
        assert len(app.get_active_route(default_selection_features).route.pending_nodes) == 0

        # 1- first normal input is satisfied but nearest is different than 'ref' so it blocks none of the paths from
        # range is not ready.
        app.process(eureka_offline_training_data["NA"]["2020-03-18"])
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        assert not path
        assert len(app.get_active_route(default_selection_features).route.pending_nodes) == 1

        # event for 'nearest' comes in for the day before but it should be ok since it is within the range
        app.process(eureka_offline_all_data["2020-03-17"])
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        # succeeded! output using '2020-03-17' for 'offline_data' is ready.
        assert path
        # 2020-03-18 node will be removed/consumed but 2020-03-17 on nearest input will have its own pending node now,
        # since it represents a different TIP (trigger group).
        assert len(app.get_active_route(default_selection_features).route.pending_nodes) == 1

        # check the reverse input mode
        default_selection_features_reverse_inputs = app.create_data(
            id="eureka_default2",
            inputs={"offline_data": eureka_offline_all_data[:-7].nearest(), "offline_training_data": eureka_offline_training_data},
            compute_targets=[NOOPCompute],  # when inputs are ready, trigger the following
            output_dim_links=[("day", EQUALS, eureka_offline_training_data("day"))],
        )
        # again this trivial case should succeed as usual
        assert app.execute(default_selection_features_reverse_inputs["2020-04-01"])
        assert len(app.get_active_route(default_selection_features_reverse_inputs).route.pending_nodes) == 0

        # 2.1 - satisfy the nearest. it is different than 'ref' signals, this won't get wasted and cause a pending signal.
        app.process(eureka_offline_all_data["2020-04-02"])
        assert len(app.get_active_route(default_selection_features_reverse_inputs).route.pending_nodes) == 1

        # 2.2 - the following day event comes for normal/independent (second) input
        #       nearest is automatically materialized for 2020-04-03 and since it is range is satisfied already
        #       execution begins. This happens thanks to range transfer from previous node (2020-04-02)
        app.process(eureka_offline_training_data["NA"]["2020-04-03"])
        path, _ = app.poll(default_selection_features_reverse_inputs["2020-04-03"])
        assert path
        assert len(app.get_active_route(default_selection_features_reverse_inputs).route.pending_nodes) == 1

        # 3- finally do the previous test with artifical partitions
        add_test_data(app, eureka_offline_all_data["2020-05-01"], "_SUCCESS", "")
        # this should be enough to cause a trigger on the following day.
        app.process(eureka_offline_training_data["NA"]["2020-05-02"])
        path, _ = app.poll(default_selection_features_reverse_inputs["2020-05-02"])
        assert path
        # remaining node is from "2.1"
        assert len(app.get_active_route(default_selection_features_reverse_inputs).route.pending_nodes) == 1

        # consume the remaining node
        app.process(eureka_offline_training_data["NA"]["2020-04-02"])
        assert len(app.get_active_route(default_selection_features_reverse_inputs).route.pending_nodes) == 0

        self.patch_aws_stop()

    def test_application_nearest_hybrid_utilization_with_other_input_modes(self):
        app = self._create_app()

        daily_timer = app.add_timer(id="pipeline_dailyly_timer", schedule_expression="rate(1 day)", time_dimension_id="day")

        eureka_offline_training_data = app.add_external_data(
            data_id="eureka_training_data",
            s3_dataset=S3(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data",
                StringVariant("NA", "region"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
            ),
        )

        # 1- Hybrid with Ref
        default_selection_features = app.create_data(
            id="eureka_default",
            inputs=[
                daily_timer,
                # both 'nearest' and 'ref'
                # 3 days into the future :D
                eureka_offline_training_data["*"][:3].nearest().ref,
            ],
            compute_targets=[NOOPCompute],  # when inputs are ready, trigger the following
        )
        # again this trivial case should succeed as usual
        assert app.execute(default_selection_features["2020-04-01"])
        assert len(app.get_active_route(default_selection_features).route.pending_nodes) == 0

        # should block since 'nearst' would still mandate the existence of at least one partition within the next 3 days
        # please note that if it was only a ref, then this would trigger the execution
        assert app.process(daily_timer["2020-05-01"])
        assert not app.poll(default_selection_features["2020-05-01"])[0]
        assert len(app.get_active_route(default_selection_features).route.pending_nodes) == 1
        app.process(eureka_offline_training_data["NA"]["2020-05-02"])
        assert app.poll(default_selection_features["2020-05-01"])[0]
        assert len(app.get_active_route(default_selection_features).route.pending_nodes) == 0

        # now the major impact of being a reference: this won't cause a new pending node since the signal was marked as
        # reference. RheocerOS would still avoid pending node creation for ref signals.
        # This is the current (non-critical) behaviour from RheocerOS routing. Can be removed if wont be used as an
        # optimization hint.
        app.process(eureka_offline_training_data["NA"]["2020-04-02"])
        assert len(app.get_active_route(default_selection_features).route.pending_nodes) == 0

        # 2- Hybrid with range_check
        # show that setting nearest and range_check at the same time is meaningless and system won't allow it.
        with pytest.raises(ValueError):
            app.create_data(
                id="eureka_default2",
                inputs=[
                    daily_timer,
                    # both 'nearest' and 'range_check(True)'
                    eureka_offline_training_data[:-2].nearest().check_range(),
                ],
                compute_targets=[NOOPCompute],
            )

        with pytest.raises(ValueError):
            app.create_data(
                id="eureka_default2",
                inputs=[
                    daily_timer,
                    # both 'nearest' and 'range_check(True)'
                    eureka_offline_training_data[:-2].check_range().nearest(),
                ],
                compute_targets=[NOOPCompute],
            )

        self.patch_aws_stop()

    def test_application_nearest_input_validations(self):
        app = self._create_app()

        eureka_offline_all_data = app.marshal_external_data(
            S3Dataset(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/all-data",
                "partition_day={}",
                dataset_format=DataFormat.CSV,
            ),
            "eureka_training_all_data",
            {"day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}},
            {"*": {}},
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        # 1 - just like 'ref' signals 'nearest' signals are also assumed to be dependent.
        #     so currently, we are restricting the scenarios where inputs are dependent only.
        with pytest.raises(ValueError):
            app.create_data(id="should_fail", inputs=[eureka_offline_all_data[:-7].nearest()], compute_targets=[NOOPCompute])

        # 2- Output dimension should not depend on the dimension of a dependent signal.
        #   Another restriction to promote good practice in RheocerOS app development.

        daily_timer = app.add_timer(id="pipeline_dailyly_timer", schedule_expression="rate(1 day)", time_dimension_id="day")
        with pytest.raises(ValueError):
            # manually link output to 'nearest' signal
            app.create_data(
                id="should_fail2",
                inputs=[daily_timer, eureka_offline_all_data[:-7].nearest()],
                compute_targets=[NOOPCompute],
                output_dim_links=[
                    # eureka_offline_all_data::day ==> output::my_day
                    "my_day",
                    lambda dim: dim,
                    eureka_offline_all_data("day"),
                ],
            )

        with pytest.raises(ValueError):
            # 'nearest' signal is unlinked. has no links with other inputs or the output.
            app.create_data(
                id="should_fail2",
                inputs=[daily_timer, eureka_offline_all_data[:-7].nearest()],
                compute_targets=[NOOPCompute],
                output_dim_links=[
                    # daily_timer::day ==> output::my_day
                    "my_day",
                    lambda dim: dim,
                    daily_timer("day"),
                ],
            )

        # 3- now fail because 'nearest' signal has a dim that cannot be mapped from other inputs.
        # this is generally a rule for a runtime condition where dependent signals block execution
        # due to unmapped dimensions requiring an incoming event. this eliminates the meaning of nearest
        # and reference signals (where for the latter it basically means 'zombie' node creation.
        eureka_offline_training_data = app.add_external_data(
            data_id="eureka_training_data",
            s3_dataset=S3(
                "427809481713",
                "dex-ml-eureka-model-training-data",
                "cradle_eureka_p3/v8_00/training-data",
                AnyString("region"),
                AnyDate("day", {"format": "%Y-%m-%d"}),
            ),
        )

        with pytest.raises(ValueError):
            app.create_data(
                id="should_fail3",
                inputs=[
                    daily_timer,
                    # problematic 'region' param on the dependent signal
                    # again this is an intentional restriction by RheocerOS to guide the user
                    # through a practical use of 'nearest' flag. Otherwise an incoming event would be
                    # required to satisfy the 'region' dimension. An event with the same date value.
                    eureka_offline_training_data["*"][:-7].nearest(),
                ],
                compute_targets=[NOOPCompute],
            )

        # materialized version of above should succeed (region = EU)
        app.create_data(
            id="should_succeed1", inputs=[daily_timer, eureka_offline_training_data["EU"][:-7].nearest()], compute_targets=[NOOPCompute]
        )

        # should fail if all of the inputs are dependent
        with pytest.raises(ValueError):
            app.create_data(
                id="should_fail5",
                inputs=[eureka_offline_all_data.ref, eureka_offline_training_data["EU"][:-7].nearest()],
                compute_targets=[NOOPCompute],
            )

        # when there is an independent signal, it should succeed!
        app.create_data(
            id="should_succeed2",
            inputs=[daily_timer, eureka_offline_all_data.ref, eureka_offline_training_data["EU"][:-7].nearest()],
            compute_targets=[NOOPCompute],
        )

        self.patch_aws_stop()

    def test_application_enforce_alias_for_same_input(self):
        app = self._create_app()

        external_data = app.add_external_data(
            data_id="eureka_training_data",
            s3_dataset=S3("427809481713", "bucket", "folder", StringVariant("NA", "region"), AnyDate("day", {"format": "%Y-%m-%d"})),
            completion_file="_SUCCESS",
        )

        with pytest.raises(ValueError):
            app.create_data(
                id="test_internal_data",
                inputs=[external_data["NA"]["*"], external_data["NA"][:-7]],  # last week
                compute_targets=[NOOPCompute],
            )

        new_internal_node = app.create_data(
            id="test_internal_data",
            inputs={"event_day_data": external_data["NA"]["*"], "last_week_data": external_data["NA"][:-7]},
            compute_targets=[NOOPCompute],
        )
        assert new_internal_node
        self.patch_aws_stop()

    def test_application_supports_dangling_nodes(self):
        """Test creation/execution on root-level data nodes with no inputs"""
        app = self._create_app()

        data1 = app.create_data(id="data_with_no_dims", compute_targets=[NOOPCompute])  # when inputs are ready, trigger the following

        data2 = app.create_data(id="data_with_dims", output_dimension_spec={"day": {type: Type.DATETIME}}, compute_targets=[NOOPCompute])

        data2_1 = app.create_data(id="data_lvl_2", inputs=[data2], compute_targets=[NOOPCompute])

        app.activate()

        # First check ground is NOOP on already integrated nodes (with inputs)
        app.ground(data2_1["2020-12-28"], is_async=False)
        assert not app.poll(data2_1["2020-12-28"])[0]
        assert not app.poll(data1)[0]
        assert not app.poll(data2["2020-12-28"])[0]

        app.ground(data1, is_async=False)
        output_path, _ = app.poll(data1)
        assert output_path.endswith("internal_data/data_with_no_dims")

        app.ground(data2["2020-12-27"], is_async=False)
        output_path, _ = app.poll(data2["2020-12-27"])
        assert output_path.endswith("internal_data/data_with_dims/2020-12-27 00:00:00")

        output_path = app.execute(data2["2020-12-28"])
        assert output_path.endswith("internal_data/data_with_dims/2020-12-28 00:00:00")

        # check propagation, make sure that propagation is seamless over executions on dangling/root nodes.
        output_path, _ = app.poll(data2_1["2020-12-27"])
        assert output_path.endswith("internal_data/data_lvl_2/2020-12-27 00:00:00")

        output_path, _ = app.poll(data2_1["2020-12-28"])
        assert output_path.endswith("internal_data/data_lvl_2/2020-12-28 00:00:00")

        # now again check ground is NOOP on already integrated nodes (with inputs)
        app.ground(data2_1["2020-12-29"], is_async=False)
        assert not app.poll(data2_1["2020-12-29"])[0]
        # should not have any side-effects on other nodes either
        assert not app.poll(data2["2020-12-29"])[0]

        self.patch_aws_stop()
