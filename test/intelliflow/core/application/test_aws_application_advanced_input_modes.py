# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# TODO see the integ-test version and patch / decorate it for unit-tests.
import time
from datetime import datetime, timedelta

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

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

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

    def test_application_reference_inputs_in_different_layers_2(self):
        app = self._create_app()

        timer_signal_daily = app.add_timer(
            id="daily_timer", schedule_expression="rate(1 day)", time_dimension_id="day"  # you can use CRON tab here as well
        )

        pdex_data = app.marshal_external_data(
            S3Dataset(
                "111222333444",
                "dex-inc-bucket",
                "output",
                "filtered-data-merged-with-header-v1.2-{}",
                dataset_format=DataFormat.CSV,
            ),
            "smart_filtered_data",
            {"day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        # should succeed and output should get auto-linked to (non-reference signal) timer here
        preprocess_1_dedupe_2 = app.create_data(
            id="preprocessed_deduped",
            inputs={
                "daily_data": pdex_data.ref,
                "timer": timer_signal_daily,
            },
            compute_targets=[NOOPCompute],
        )

        self.patch_aws_stop()

    def test_application_reference_inputs_in_different_layers_3(self):
        """In this test, we test a scenario where the reference input is not linked to the independent input (timer)
        but that reference is auto-linked with the output over dimension "day".

        In this scenario, the reference input does not seem to be "dangling" (SignalLinkNode::check_dangling_references)
        would not catch it but it is actually "unsatisfied" for runtime execution, meaning that incoming indepedent
        timer signal will not be able to materialize it. We show that create_data API

        LINKING VISUALIZATION:
        -----------

           INPUT 1: daily_timer ("time")

           INPUT 2 (ref): filtering_data ("day")   <---->  OUTPUT("day")

        -----------

        At runtime input 1 will be received but input 2 (as described by the user) has a risk of not receiving events
        and due to missing linking from "time" to "day" it will be impossible for framework to infer and materialize
        that input.
        """
        app = self._create_app()

        smart_filtering_data = app.marshal_external_data(
            S3Dataset(
                "111222333444",
                "filtering-data-bucket",
                "output",
                "{}",  # partition 1
                "filtering-data-with-header-v1.2-{}",  # partition 2
                "{}",  # partition 3
                dataset_format=DataFormat.CSV,
                delimiter=",",
            ),
            id="filtering_data",
            dimension_spec={
                "region": {
                    "type": DimensionType.STRING,
                    "day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d", "index": {"type": DimensionType.LONG}},
                }
            },
            dimension_filter={"*": {"*": {7: {}}}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        daily_timer = app.add_timer(
            "daily_timer",
            "rate(1 day)",
            # DO NOT SET dimension_id and leave it as "time" (default timer dimension id)
            # time_dimension_id="day",
            time_dimension_granularity=DatetimeGranularity.DAY,
        )

        # final analysis in Application::_create_data_node will capture this problem
        pre_process_single_date_core_1 = app.create_data(
            id=f"pre_process_single_date_core_1",
            inputs={"daily_data": smart_filtering_data["NA"]["*"][:-8].ref.range_check(True), "timer": daily_timer},
            output_dimension_spec={
                "dataset": {
                    type: DimensionType.STRING,
                    "modelName": {
                        type: DimensionType.STRING,
                        "modelVersion": {
                            type: DimensionType.STRING,
                            "region": {
                                type: DimensionType.STRING,
                                "day": {type: DimensionType.DATETIME, "granularity": DatetimeGranularity.DAY, "format": "%Y-%m-%d"},
                            },
                        },
                    },
                }
            },
            output_dim_links=[
                ("dataset", EQUALS, "prod"),
                ("modelName", EQUALS, "DORE"),
                ("modelVersion", EQUALS, "1.1"),
                ("region", EQUALS, "NA"),
            ],
            compute_targets=[NOOPCompute],
        )

        # should SUCCEED now as we provide the link from timer to data and now at runtime the framework will be
        # able to materialize the reference
        pre_process_single_date_core_2 = app.create_data(
            id=f"pre_process_single_date_core_2",
            inputs={"daily_data": smart_filtering_data["NA"]["*"][:-8].ref.range_check(True), "timer": daily_timer},
            # INPUT LINKING from timer to daily_data reference
            input_dim_links=[(smart_filtering_data("day"), EQUALS, daily_timer("time"))],
            output_dimension_spec={
                "dataset": {
                    type: DimensionType.STRING,
                    "modelName": {
                        type: DimensionType.STRING,
                        "modelVersion": {
                            type: DimensionType.STRING,
                            "region": {
                                type: DimensionType.STRING,
                                "day": {type: DimensionType.DATETIME, "granularity": DatetimeGranularity.DAY, "format": "%Y-%m-%d"},
                            },
                        },
                    },
                }
            },
            output_dim_links=[
                ("dataset", EQUALS, "prod"),
                ("modelName", EQUALS, "DORE"),
                ("modelVersion", EQUALS, "1.1"),
                ("region", EQUALS, "NA"),
            ],
            compute_targets=[NOOPCompute],
        )

        with pytest.raises(ValueError):
            app.validate(pre_process_single_date_core_1)

        # here timer cannot be inferred from the output
        with pytest.raises(ValueError):
            app.validate(pre_process_single_date_core_1["prod"]["DORE"]["1.1"]["NA"]["2023-02-08"])

        # even with the materialize values should fail due to "dangling reference" that cannot be materialized at runtime
        with pytest.raises(ValueError):
            app.validate(pre_process_single_date_core_1["prod"]["DORE"]["1.1"]["NA"]["2023-02-08"], daily_timer["2023-02-08"])

        # even the analysis on the second one should FAIL due to unmaterialized dimensions
        with pytest.raises(ValueError):
            app.validate(pre_process_single_date_core_2)

        app.validate(pre_process_single_date_core_2["prod"]["DORE"]["1.1"]["NA"]["2023-02-08"])
        app.validate(pre_process_single_date_core_2["prod"]["DORE"]["1.1"]["NA"]["2023-02-08"], daily_timer["2023-02-08"])

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

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

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

        app.validate(default_selection_features2["NA"]["2020-03-18"])
        # second input will fail the analysis due to hardcoded condition on day dimension
        with pytest.raises(ValueError):
            app.validate(default_selection_features2["NA"]["2020-03-19"])

        # both of them will reject the execution at runtime with incompatible region dimension, let's capture it early
        with pytest.raises(ValueError):
            app.validate(default_selection_features2["EU"]["2020-03-18"])

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

    def test_application_range_shift(self):
        app = self._create_app()

        daily_timer = app.add_timer("daily_timer", "rate(1 day)", time_dimension_id="day")

        offline_training_data = app.marshal_external_data(
            S3Dataset(
                "111222333444",
                "bucket",
                "training_data",
                "partition_day={}",
                dataset_format=DataFormat.CSV,
            ),
            "external_data",
            {"day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d"}},
            {"*": {}},
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        preprocess = app.create_data(
            id="preprocess_data",
            inputs={
                "offline_training_data": offline_training_data[:-3].ref.range_check(True),
                # starting from "the day before"
                "offline_training_data_shifted_range": offline_training_data[-1:-3].ref.range_check(True),
                "offline_training_data_yesterday": offline_training_data[-1:].ref.range_check(True),
                "timer": daily_timer,
            },
            compute_targets=[NOOPCompute],  # when inputs are ready, trigger the following
        )

        add_test_data(app, offline_training_data["2022-06-01"], "_SUCCESS", "")
        add_test_data(app, offline_training_data["2022-05-31"], "_SUCCESS", "")
        add_test_data(app, offline_training_data["2022-05-30"], "_SUCCESS", "")
        add_test_data(app, offline_training_data["2022-05-29"], "_SUCCESS", "")

        app.activate()

        # should trigger execution on "preprocess"
        app.process(daily_timer["2022-06-01"])

        # execution must have completed immediately (thanks to NOOPCompute) without any async workflow
        path, records = app.poll(preprocess["2022-06-01"])
        assert path
        success_record: RoutingTable.ComputeRecord = records[0]
        offline_training_data = [input for input in success_record.materialized_inputs if input.alias == "offline_training_data"][0]
        offline_training_data_shifted_range = [
            input for input in success_record.materialized_inputs if input.alias == "offline_training_data_shifted_range"
        ][0]
        offline_training_data_yesterday = [
            input for input in success_record.materialized_inputs if input.alias == "offline_training_data_yesterday"
        ][0]

        assert offline_training_data.get_materialized_resource_paths()[0].endswith("2022-06-01")
        assert offline_training_data.get_materialized_resource_paths()[1].endswith("2022-05-31")
        assert offline_training_data.get_materialized_resource_paths()[2].endswith("2022-05-30")

        assert len(offline_training_data_shifted_range.get_materialized_resource_paths()) == 3
        assert offline_training_data_shifted_range.get_materialized_resource_paths()[0].endswith("2022-05-31")
        assert offline_training_data_shifted_range.get_materialized_resource_paths()[1].endswith("2022-05-30")
        assert offline_training_data_shifted_range.get_materialized_resource_paths()[2].endswith("2022-05-29")

        assert offline_training_data_yesterday.get_materialized_resource_paths()[0].endswith("2022-05-31")

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
        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate()

        now = datetime.now()

        # "nearest" range is not satisfied. no partition exists within 7 days range
        with pytest.raises(RuntimeError):
            app.execute(default_selection_features["NA"][now])

        add_test_data(app, eureka_offline_all_data[now - timedelta(2)], "_SUCCESS", "")

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

        # show that this will have no effect on pending node for 2020-03-18
        # incoming events for "dependent" signals (reference, nearest) are just ignored in dependency check.
        # we always read from remote partition for resource / partition existence.
        app.process(eureka_offline_all_data["2020-03-17"])
        path, _ = app.poll(default_selection_features["NA"]["2020-03-18"])
        assert not path

        # 'nearest' detects the completion for the day before but it should be ok since it is within the range
        add_test_data(app, eureka_offline_all_data["2020-03-17"], "_SUCCESS", "")
        # force next-cycle (orchestrator checks pending executions, etc)
        app.update_active_routes_status()
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
        add_test_data(app, eureka_offline_all_data["2020-04-01"], "_SUCCESS", "")
        assert app.execute(default_selection_features_reverse_inputs["2020-04-01"])
        assert len(app.get_active_route(default_selection_features_reverse_inputs).route.pending_nodes) == 0

        # 2- do another test with artifical partitions
        add_test_data(app, eureka_offline_all_data["2020-05-01"], "_SUCCESS", "")
        # this should be enough to cause a trigger on the following day.
        app.process(eureka_offline_training_data["NA"]["2020-05-02"])
        path, _ = app.poll(default_selection_features_reverse_inputs["2020-05-02"])
        assert path
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

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate()

        add_test_data(app, eureka_offline_training_data["NA"]["2020-04-01"], "_SUCCESS", "")

        # again this trivial case should succeed as usual
        assert app.execute(default_selection_features["2020-04-01"])
        assert len(app.get_active_route(default_selection_features).route.pending_nodes) == 0

        # should block since 'nearst' would still mandate the existence of at least one partition within the next 3 days
        # please note that if it was only a ref, then this would trigger the execution
        assert app.process(daily_timer["2020-05-01"])
        assert not app.poll(default_selection_features["2020-05-01"])[0]
        assert len(app.get_active_route(default_selection_features).route.pending_nodes) == 1
        add_test_data(app, eureka_offline_training_data["NA"]["2020-05-02"], "_SUCCESS", "")
        app.update_active_routes_status()
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

        daily_timer = app.add_timer(id="pipeline_dailyly_timer", schedule_expression="rate(1 day)", time_dimension_id="day")

        # dependents are force-linked over same dimension names, so here even if input linking is False, second input
        # is not dangling
        app.create_data(
            id="will_force_link_dependents",
            inputs=[daily_timer, eureka_offline_all_data[:-7].nearest()],
            compute_targets=[NOOPCompute],
            output_dim_links=[
                (
                    # daily_timer::day ==> output::my_day
                    "my_day",
                    lambda dim: dim,
                    daily_timer("day"),
                )
            ],
            output_dimension_spec={"my_day": {type: DimensionType.DATETIME}},
            auto_input_dim_linking_enabled=False,
        )

        # 3- now fail because 'nearest' signal has a dim that cannot be mapped from other inputs.
        # this is generally a rule for a runtime condition where dependent signals block execution
        # due to unmapped dimensions requiring an incoming event. this eliminates the meaning of nearest
        # and reference signals (where for the latter it basically means 'zombie' node creation.

        # 3-1
        daily_timer = app.add_timer(id="timer_with_different_dim_name", schedule_expression="rate(1 day)", time_dimension_id="day_tona")
        with pytest.raises(ValueError):
            # output and nearest (dependent) signals will never be materialized at runtime
            app.create_data(
                id="should_fail3_1",
                inputs=[eureka_offline_all_data.nearest(), daily_timer],
                compute_targets=[NOOPCompute],
            )

        daily_timer = app.add_timer(id="daily_timer", schedule_expression="rate(1 day)", time_dimension_id="day")

        # TODO enable after "graph analyzer" for input/output links is implemented
        # with pytest.raises(ValueError):
        #    # output and nearest (dependent) signals will never be materialized at runtime
        #    app.create_data(
        #        id="should_fail3_2",
        #        inputs=[eureka_offline_all_data.nearest(), daily_timer],
        #        compute_targets=[NOOPCompute],
        #        output_dimension_spec={
        #            "day": {
        #                type: DimensionType.DATETIME,
        #            }
        #        }
        #    )

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

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

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

    def test_application_range_expansion_over_materialized_dimension_filter(self):
        """Test scenario:
        - Signal has a materialized value in dimension filter already in its "external data declaration"
        - When inputted to a data-node a range is defined
        - Range expansion should use the materialized (hard-coded) value from its dimension filter as the TIP for the
        desired range. Range expansion should occur based on the depth defined by the user. So the input should represent
        multiple resource paths (partitions) starting from the hardcoded value up/down to the end of the range.
        """
        app = self._create_app()
        daily_timer = app.add_timer(
            "daily_timer", "rate(1 day)", time_dimension_id="day", time_dimension_granularity=DatetimeGranularity.DAY
        )

        # - define the signal
        smart_filtering_data = app.marshal_external_data(
            S3Dataset(
                self.account_id,
                "bucket",
                "folder",
                "{}",  # partition #1
                "smart-defaulting-data-{}",  # partition #2
                "{}",  # partition #3
                dataset_format=DataFormat.CSV,
                delimiter=",",
            ),
            id="smart_filtering_external_data",
            dimension_spec={
                "region": {
                    "type": DimensionType.STRING,
                    "day": {"type": DimensionType.DATETIME, "format": "%Y-%m-%d", "index": {"type": DimensionType.LONG}},
                }
            },
            # HARDCODED (prefiltered) value of 7 on partition "index"
            dimension_filter={"*": {"*": {7: {}}}},
            protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        # define links from/to timer to the other input
        def timer_to_pre_process_data_day(timer_date: datetime) -> datetime:
            return timer_date - timedelta(days=1)

        def pre_process_data_day_to_timer(pre_process_date: datetime) -> datetime:
            return pre_process_date + timedelta(days=1)

        region = "NA"
        pre_process_single_date_core_NA = app.create_data(
            id=f"pre_process_single_date_core_{region}",
            inputs={"daily_data": smart_filtering_data[region]["*"][:-8].reference(), "timer": daily_timer},
            input_dim_links=[
                (smart_filtering_data("day"), timer_to_pre_process_data_day, daily_timer("day")),
                (daily_timer("day"), pre_process_data_day_to_timer, smart_filtering_data("day")),
            ],
            output_dimension_spec={
                "dataset": {
                    type: DimensionType.STRING,
                    "modelName": {
                        type: DimensionType.STRING,
                        "modelVersion": {
                            type: DimensionType.STRING,
                            "region": {
                                type: DimensionType.STRING,
                                "day": {type: DimensionType.DATETIME, "granularity": DatetimeGranularity.DAY, "format": "%Y-%m-%d"},
                            },
                        },
                    },
                }
            },
            output_dim_links=[
                ("dataset", EQUALS, "prod"),
                ("modelName", EQUALS, "DORE"),
                ("modelVersion", EQUALS, "1.1"),
                ("region", EQUALS, region),  # Declaration is unnecessary - adding for better readability
                ("day", timer_to_pre_process_data_day, daily_timer("day")),
                (daily_timer("day"), pre_process_data_day_to_timer, "day"),  # Not required for runtime - Used for execute API
                (smart_filtering_data("day"), EQUALS, "day"),
            ],
            compute_targets=[NOOPCompute],
        )
        # do an early check even without activation. even in the interpreted node data daily_data input should have the range already.
        node_route: Route = pre_process_single_date_core_NA.bound.create_route()
        daily_data_signal: Signal = [signal for signal in node_route.link_node.signals if signal.alias == "daily_data"][0]
        materialized_resource_paths = daily_data_signal.get_materialized_resource_paths()
        assert len(materialized_resource_paths) == 8
        assert materialized_resource_paths[0].endswith("7")
        assert materialized_resource_paths[7].endswith("0")

        app.execute(pre_process_single_date_core_NA["prod"]["DORE"]["1.1"][region]["2022-09-13"])

        path, compute_records = app.poll(pre_process_single_date_core_NA["prod"]["DORE"]["1.1"][region]["2022-09-13"])
        assert path
        compute_record = compute_records[0]

        # this is the signal that would be received by batch-compute drivers or InlinedCompute code
        daily_data_signal_from_execution: Signal = [
            signal for signal in compute_record.materialized_inputs if signal.alias == "daily_data"
        ][0]
        materialized_resource_paths = daily_data_signal_from_execution.get_materialized_resource_paths()
        assert len(materialized_resource_paths) == 8
        assert materialized_resource_paths[0].endswith("7")
        assert materialized_resource_paths[7].endswith("0")

        # extra sanity-check on both inputs and the output to make sure that links have worked well too
        timer_signal: Signal = [signal for signal in compute_record.materialized_inputs if signal.alias == "timer"][0]
        DimensionFilter.check_equivalence(
            timer_signal.domain_spec.dimension_filter_spec, DimensionFilter.load_raw({"2022-09-14": {}})  # one day ahead
        )

        DimensionFilter.check_equivalence(
            daily_data_signal_from_execution.domain_spec.dimension_filter_spec,
            DimensionFilter.load_raw(
                {
                    "NA": {
                        "2022-09-13": {  # same as the output
                            7: {},
                            6: {},
                            5: {},
                            4: {},
                            3: {},
                            2: {},
                            1: {},
                            0: {},
                        }
                    }
                }
            ),
        )
        # and finally the output
        DimensionFilter.check_equivalence(
            compute_record.materialized_output.domain_spec.dimension_filter_spec,
            DimensionFilter.load_raw({"prod": {"DORE": {"1.1": {region: {"2022-09-13"}}}}}),  # as provided in "execute" call
        )

        self.patch_aws_stop()

    def test_application_date_range_on_input_events(self):
        app = self._create_app()

        training_data = app.marshal_external_data(
            S3Dataset("12345678901", "bucket", "data_folder", "partition_day={}", dataset_format=DataFormat.CSV),
            "trainin_all",
            dimension_spec={
                "day": {
                    "type": DimensionType.DATETIME,
                    "format": "%Y-%m-%d",
                    # LIMIT !!! block events from this S3 source with partitions earlier than this
                    "min": "2021-01-01",
                }
            },
            dimension_filter={"*": {}},
        )

        verification = app.add_external_data(
            data_id="verification",
            s3_dataset=S3("111222333444", "bucket", "folder", AnyDate("day", {"format": "%Y-%m-%d"})),
            completion_file="_SUCCESS",
        )

        internal_node = app.create_data(
            id="internal_data",
            inputs=[training_data, verification],
            compute_targets=[NOOPCompute],
        )

        path = app.materialize(internal_node["2021-01-01"])
        assert path

        # rejected due to "min" date being violated
        with pytest.raises(ValueError):
            app.materialize(internal_node["2020-12-01"])

        internal_node_with_link = app.create_data(
            id="internal_data2",
            inputs=[verification, training_data],
            input_dim_links=[(training_data("day"), lambda ver_date: ver_date - timedelta(days=5), verification("day"))],
            compute_targets=[NOOPCompute],
        )

        app.activate()

        with pytest.raises(ValueError):
            app.process(training_data[datetime(2020, 12, 28)])

        app.process(training_data[datetime(2021, 1, 2)])  # > "min" date

        # now show that due to input link above even if verification (hence the output as it adapts the first input's spec by default here)
        # has a date greater than "min" date training_data input will raise due to its mapped date value being earlier than "min" date
        with pytest.raises(ValueError):
            app.execute(internal_node_with_link[datetime(2021, 1, 3)])

        # this will ok because training_data("day") will map to a date greater than its "min" date
        app.execute(internal_node_with_link[datetime(2021, 1, 6)], wait=False)

        self.patch_aws_stop()
