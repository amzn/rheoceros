# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from unittest.mock import MagicMock

import pytest

from intelliflow.api_ext import *
from intelliflow.core.application.application import Application
from intelliflow.core.platform.definitions.aws.cw.dashboard import CW_DASHBOARD_WIDTH_MAX, LegendPosition
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
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import AlarmDimension
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplicationAlarmingMetrics(AWSTestBase):
    def test_application_alarming_and_metrics_system_metrics_using_all_apis(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("alarming-sys", self.region)

        # SYSTEM METRICS
        system_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.SYSTEM)

        # check system metrics and see their alias' and sub-dimensions!
        for driver_metric_map in system_metrics_map.values():
            for metric in driver_metric_map.values():
                # dumps metric group ID/alias -> specific MetricNames and other details
                assert json.loads(metric.describe())["metric_stats"]

        processor_metrics_map = system_metrics_map[ProcessingUnit]
        routing_metrics_map = system_metrics_map[RoutingTable]

        # 1-use metric ID/alias' to retrieve them from the map
        # 2-and then use 'MetricName' to get a concrete/materialized metric to be used in an Alarm.
        # these signals can be bind into alarms now
        routing_table_metric_signal = routing_metrics_map["routingTable"]
        routing_table_getitem_operation_metric_signal = routing_metrics_map["routingTable.GetItem"]
        # the following can be directly inputted to an alarm by starting with Statistic as the first dimension

        processor_core_metric_signal = processor_metrics_map["processor.core"]

        # low level alarm into an underlying driver resource
        # raise if write throttling in RheocerOS routing table is more than 50 in 15 mins in two data-points out of 3.
        #       or route object retrieval latency is more than 500ms.
        #
        # Please note that this alarm can still be aggregated into a composite alarm (which would still use the same dedupe_str)
        # this can enable you to partition alarm definitions and then merge them to create system wide OE view.
        routing_table_alarm = app.create_alarm(
            id="routing_table_alarm",
            target_metric_or_expression="(m1 > 50 OR m2 > 500)",
            metrics={
                "m1": routing_table_metric_signal["WriteThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(15)],
                "m2": routing_table_getitem_operation_metric_signal["SuccessfulRequestLatency"][MetricStatistic.AVERAGE][
                    MetricPeriod.MINUTES(15)
                ],
            },
            number_of_evaluation_periods=3,
            number_of_datapoint_periods=2,
            comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
            # since we are using logical/conditional operator in metric math, the output series only contains 1s or 0s.
            # so threshold will be 1 to detect.
            threshold=1,
            default_actions=AlarmDefaultActionsMap(
                ALARM_ACTIONS=set(),
                OK_ACTIONS=set(),
                INSUFFICIENT_DATA_ACTIONS=set(),
            ),
        )

        # Sev5 on core Processor (paranoid / warning shot for oncall)
        # if Processor has an execution Error (message sent to DLQ probably) or duration max hits 600 x 1000 (10 minutes, approacing to 15 mins timeout)
        processor_alarm = app.create_alarm(
            id="processor_alarm",
            # refer
            #   https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html
            target_metric_or_expression="(m1 > 1 OR m2 > 600000)",
            metrics={
                "m1": processor_core_metric_signal["Errors"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                "m2": processor_core_metric_signal["Duration"][MetricStatistic.MAXIMUM][MetricPeriod.MINUTES(5)],
            },
            number_of_evaluation_periods=1,
            number_of_datapoint_periods=1,
            comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
            # since we are using logical/conditional operator in metric math, the output series only contains 1s or 0s.
            # so threshold will be 1 to detect.
            threshold=1,
            default_actions=AlarmDefaultActionsMap(ALARM_ACTIONS=set()),
        )

        # External METRICS
        # import a metric definition from the same account
        # generic representation of the metrics from this particular lambda.
        external_lambda_metric = app.marshal_external_metric(
            external_metric_desc=CWMetric(namespace="AWS/Lambda"), id="lambda_metric", sub_dimensions={"FunctionName": "LambdaFunction"}
        )

        # import the same metric in a different, more flexible way.
        #   This shows Lambda Error metric can be imported into the system in
        #   different ways (different IDs, default alias').
        #   And also with defaults/overwrites on metric dimensions
        external_lambda_error_metric_on_another_func = app.marshal_external_metric(
            external_metric_desc=CWMetric(namespace="AWS/Lambda"),
            id="my_test_function_error",
            dimension_filter={
                "Error": {  # only keep 'Error'
                    MetricStatistic.SUM: {  # support SUM only
                        MetricPeriod.MINUTES(5): {
                            # restrict the use of this metric with 5 mins period only (in alarms)
                            "*": {}  # (reserved) Any MetricDimension.TIME
                        }
                    }
                }
            },
            sub_dimensions={"functionname": "LambdaFunction"},
        )

        system_failure_alarm = app.create_alarm(
            id="system_failure",
            #  refer
            #    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html#metric-math-syntax
            target_metric_or_expression="SUM(METRICS())",
            # will validate if metrics are materialized (i.e NAME, Statistic and Period dimensions are material or not).
            metrics=[
                external_lambda_metric["Error"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                external_lambda_error_metric_on_another_func,
            ],
            number_of_evaluation_periods=5,
            number_of_datapoint_periods=3,
            comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
            threshold=1,
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        # CUSTOM DASHBOARDS
        dashboard_id = "dashboard_with_defaults"

        app.create_dashboard(dashboard_id)
        with pytest.raises(ValueError):
            # cannot create another dashboard with the same id
            app.create_dashboard(dashboard_id)

        with pytest.raises(ValueError):
            app.create_text_widget("NON_EXISTENT_DASHBOARD_ID", markdown="test")

        app.create_text_widget(
            dashboard_id,
            markdown="Custom dashboard (with default widget attributes) to present all of the alarming and metrics stuff from this example app. ",
        )

        with pytest.raises(ValueError):
            app.create_metric_widget("NON_EXISTENT_DASHBOARD_ID", [external_lambda_metric])

        app.create_metric_widget(
            dashboard_id,
            [
                external_lambda_metric["Error"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                external_lambda_error_metric_on_another_func,
                routing_table_metric_signal["WriteThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            ],
        )

        with pytest.raises(ValueError):
            # cannot use multiple alarms with this API (unlike metric signals)
            app.create_metric_widget(dashboard_id, [system_failure_alarm, processor_alarm])

        app.create_metric_widget(dashboard_id, [system_failure_alarm])
        app.create_alarm_widget(dashboard_id, processor_alarm)

        with pytest.raises(ValueError):
            app.create_alarm_status_widget("NON_EXISTENT_DASHBOARD_ID", "test", [system_failure_alarm])

        app.create_alarm_status_widget(
            dashboard_id, "Status widget for all of the alarms together", [system_failure_alarm, processor_alarm]
        )
        # the dashboard will be auto-created during the activation

        dashboard_id = "dashboard_with_params"
        app.create_dashboard(
            dashboard_id,
            # pass CW params to underlying driver
            start="-PT2W",
            periodOverride="inherit",
        )

        app.create_text_widget(
            dashboard_id,
            markdown="Custom dashboard (with default widget attributes) to present all of the alarming and metrics stuff from this example app. ",
            width=CW_DASHBOARD_WIDTH_MAX,
            height=4,
            # x=None,  # CW will fit automatically on the same row if possible (if left undefined or as None)
            # y=None
        )
        app.create_metric_widget(
            dashboard_id,
            [routing_table_metric_signal["WriteThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(15)]],
            legend_pos=LegendPosition.RIGHT,
            width=8,
            height=6,
            x=None,
            y=None,
        )
        app.create_alarm_status_widget(
            dashboard_id,
            "Status widget for all of the alarms together",
            [processor_alarm, system_failure_alarm],
            width=CW_DASHBOARD_WIDTH_MAX,
            height=6,
            x=20,
            y=20,
        )

        app.activate(allow_concurrent_executions=False)

        # yields None, external metrics cannot be emitted (note that this is not due to unmaterialized metric Name)
        assert app.platform.diagnostics["my_test_function_error"] is None
        assert app.platform.diagnostics["lambda_metric"] is None

        self.patch_aws_stop()

    def test_application_alarming_and_metrics_custom_metric(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("alarming-cust", self.region)

        # 'id' field here represents 'Metric Group Id' (metric sub-space), it will be a part of the concrete metric instance
        # (in AWS CloudWactch for example) as a sub_dimension ({'MetricGroupId': 'my_custom_spark_error_metric'}) along with
        # other sub-dimensions.
        # It is also used to retrieve this abstract 'metric' declaration during development and also from Diagnostics
        # module at runtime.
        # Do the following in your Spark code to emit at runtime for example:
        #   runtime_platform.diagnostics["my_custom_spark_error_metric"]["Error"].emit(1)
        internal_spark_error_metric = app.create_metric(
            id="my_custom_spark_error_metric",
            dimension_filter={
                "Error": {  # only keep 'Error' as NAME dimension
                    "*": {  # support all statistic (no filter spec)
                        "*": {
                            # support all periods (during the emission sets Storage Resolution to 1)
                            "*": {}  # (reserved) Any MetricDimension.TIME
                        }
                    }
                }
            },
        )

        # totally separate unrelated metric due to different sub_dimensions (despite identical metric IDs)
        internal_spark_error_metric2 = app.create_metric(
            id="my_custom_spark_error_metric",
            sub_dimensions={"marketplace_id": "1"},
            dimension_filter={"Error": {"*": {"*": {"*": {}}}}},  # only allowed NAME dimension
        )

        internal_spark_error_metric3 = app.create_metric(
            id="my_custom_spark_error_metric",
            sub_dimensions={"marketplace_id": "3"},
            # dimension_filter is not specified, so during emission
            # NAME dimension should be specified explicitly
        )

        # Example for internal / custom metric declaration with sub-dimensions (for further specialization)
        # When Metric Name is specified during emission, this would look like this in Cloudwatch:
        # {
        # "Namespace": "if-alarming-cust-<ACC_ID>-<REGION_ID>",
        # "MetricName": "<WHATEVER NAME you'll use during in emit call (e.g 'Error')",
        # "Dimensions": {"MetricGroupId": "my_custom_spark_cumulative",
        #                "my_custom_dim": "ALL"}
        # }
        internal_spark_metric_ALL = app.create_metric(
            id="my_custom_spark_cumulative",
            sub_dimensions={"my_custom_dim": "ALL"},
            # dimension_filter is not specified, so during emission
            # NAME dimension should be specified explicitly
        )

        # 'id' here is just a key to retrieve this metric declaration from Application and also from Diagnostics at runtime.
        # So if 'Error' metric name will be used to emit from this, it won't contribute to the same metric from
        # 'my_custom_spark_error_metric' above at runtime. Because metrics are uniquely defined by Name + sub_dimensions and
        # as mentioned above 'id' is automatically added to sub_dimensions to differentiate/isolate all of the internal metrics
        # from each other.
        #
        # This can be emitted from your Spark code by doing the following for example:
        #
        #   runtime_platform.diagnostics["my_app_error_metric_def"]["Error"].emit(1)
        generic_internal_metric = app.create_metric(id="my_app_error_metric_def")

        # ALARM with default ticket action
        etl_error_alarm = app.create_alarm(
            id="one_or_more_spark_executions_failed",
            target_metric_or_expression=internal_spark_error_metric["Error"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            number_of_evaluation_periods=1,
            number_of_datapoint_periods=1,
            comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
            threshold=1,
        )

        etl_error_alarm_with_all_metric_variants = app.create_alarm(
            id="one_or_more_spark_executions_failed_2",
            target_metric_or_expression="SUM(METRICS()) > 0",
            metrics=[
                internal_spark_error_metric["Error"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                internal_spark_error_metric2["Error"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                internal_spark_error_metric3["Failure"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            ],
        )

        generic_internal_alarm = app.create_alarm(
            id="generic_error_alarm",
            #  refer
            #    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html#metric-math-syntax
            target_metric_or_expression="errors > 0 OR failures > 0",
            # returns a time series with each point either 1 or 0
            metrics={
                "errors": generic_internal_metric["MY_CUSTOM_ERROR"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                "failures": generic_internal_metric["MY_CUSTOM_FAILURE"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            },
            number_of_evaluation_periods=1,
            number_of_datapoint_periods=1,
            comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
            threshold=1,
        )

        composite_alarm = app.create_composite_alarm(
            id="system_monitor", alarm_rule=~(etl_error_alarm["OK"] | generic_internal_alarm["OK"])
        )

        monitor_failure_reactor = app.create_data(
            id="system_failure_reactor", inputs=[composite_alarm[AlarmState.ALARM.value]], compute_targets=[NOOPCompute]
        )

        # show retrieval scheme for internal custom metrics before the activation
        assert not app.get_metric("my_custom_spark_error_metric")
        assert set(app.get_metric("my_custom_spark_error_metric", context=Application.QueryContext.DEV_CONTEXT)) == {
            internal_spark_error_metric,
            internal_spark_error_metric2,
            internal_spark_error_metric3,
        }
        matched_metrics = app.get_metric(
            "my_custom_spark_error_metric", sub_dimensions={"marketplace_id": "1"}, context=Application.QueryContext.DEV_CONTEXT
        )
        assert len(matched_metrics) == 1
        assert internal_spark_error_metric2 is matched_metrics[0]

        # check alarm APIs
        assert not app.get_alarm("one_or_more_spark_executions_failed_2")
        assert not app.alarm("one_or_more_spark_executions_failed_2")
        with pytest.raises(ValueError):
            alarm_node = app["one_or_more_spark_executions_failed_2"]
        assert set(app.get_alarm("one_or_more_spark_executions_failed_2", context=Application.QueryContext.DEV_CONTEXT)) == {
            etl_error_alarm_with_all_metric_variants
        }
        assert not app.get_alarm(
            "one_or_more_spark_executions_failed_2", alarm_type=AlarmType.COMPOSITE, context=Application.QueryContext.DEV_CONTEXT
        )

        # CUSTOM DASHBOARD
        dashboard_id = "my_dashboard"
        app.create_dashboard(dashboard_id)
        # show that no other signal than alarm or metrics can be used here
        with pytest.raises(ValueError):
            app.create_metric_widget(dashboard_id, [monitor_failure_reactor])

        with pytest.raises(ValueError):
            app.create_alarm_widget(dashboard_id, monitor_failure_reactor)

        with pytest.raises(ValueError):
            app.create_alarm_status_widget(dashboard_id, "title", [monitor_failure_reactor])

        app.create_alarm_status_widget(dashboard_id, "all my alarms", [generic_internal_alarm, composite_alarm])

        app.activate(allow_concurrent_executions=False)

        assert app.get_alarm("one_or_more_spark_executions_failed_2")[0] == etl_error_alarm_with_all_metric_variants
        assert app["one_or_more_spark_executions_failed_2"] == etl_error_alarm_with_all_metric_variants
        with pytest.raises(ValueError):
            alarm_node = app["one_or_more_spark_executions_failed_2" : AlarmType.COMPOSITE]

        # multiple internal custom metrics with the same ID
        with pytest.raises(ValueError):
            metric = app["my_custom_spark_error_metric"]

        with pytest.raises(ValueError):
            app.metric("my_custom_spark_error_metric")

        assert set(app.get_metric("my_custom_spark_error_metric")) == {
            internal_spark_error_metric,
            internal_spark_error_metric2,
            internal_spark_error_metric3,
        }

        assert (
            internal_spark_error_metric2.signal()
            == app.metric("my_custom_spark_error_metric", sub_dimensions={"marketplace_id": "1"}).signal()
        )
        # use Application::__getitem__
        assert internal_spark_error_metric2.signal() == app["my_custom_spark_error_metric":{"marketplace_id": "1"}].signal()
        assert internal_spark_error_metric3.signal() == app["my_custom_spark_error_metric":{"marketplace_id": "3"}].signal()

        # none of the variants of "my_custom_spark_error_metric" have both of these sub-dimensions
        assert app.metric("my_custom_spark_error_metric", sub_dimensions={"marketplace_id": "1", "region": "EU"}) is None

        assert app.metric("my_custom_spark_error_metric", sub_dimensions={"marketplace_id": "SUB DIMENSION DOES NOT MATCH!"}) is None

        with pytest.raises(ValueError):
            metric = app["my_custom_spark_error_metric":{"marketplace_id": "RANDOM NONEXISTENT SUB!"}]

        with pytest.raises(ValueError):
            # there is no metric definition with mp_id 2
            metric = app["my_custom_spark_error_metric":{"marketplace_id": "2"}]

        # RUNNTIME (test emission)
        # TEST / MANUAL emission from local Python dev-endpoint
        # These calls are actually supposed to be done at runtime (in Spark, Lambda code, etc)

        # sub_dimensions=None -> matches all of the variants
        # AMBIGUOUS call any of the metric group variants can be picked here
        # This means that there is no guaranteed way to emit from the 1st variant (without sub-dimensions)
        assert app.platform.diagnostics.get_internal_metric("my_custom_spark_error_metric").signal in {
            internal_spark_error_metric.signal(),
            internal_spark_error_metric2.signal(),
            internal_spark_error_metric3.signal(),
        }
        app.platform.diagnostics.get_internal_metric("my_custom_spark_error_metric", sub_dimensions={"marketplace_id": "1"}).emit(1)
        # make sure that we are not emitting the same signal actually. check underlying signal inequality
        assert (
            app.platform.diagnostics.get_internal_metric("my_custom_spark_error_metric", sub_dimensions={"marketplace_id": "3"}).signal
            != app.platform.diagnostics.get_internal_metric("my_custom_spark_error_metric", sub_dimensions={"marketplace_id": "1"}).signal
        )

        # check __getitem__ interface

        # sub_dimensions = None -> matches all of the variants.
        # AMBIGUOUS call, any of the metric group variants can be picked here.
        # This means that there is no guaranteed way to emit from the 1st variant (without sub-dimensions)
        assert app.platform.diagnostics["my_custom_spark_error_metric"].signal in {
            internal_spark_error_metric.signal(),
            internal_spark_error_metric2.signal(),
            internal_spark_error_metric3.signal(),
        }
        assert app.platform.diagnostics[12345] is None  # with unsupported type cannot retrieve anything
        assert app.platform.diagnostics[dict()] is None
        assert app.platform.diagnostics["my_custom_spark_error_METRIC"] is None  # IF ID/alias' case-sensitive
        app.platform.diagnostics["my_custom_spark_error_metric":{"marketplace_id": "1"}].emit(1)
        # because "marketplace_id" "2" does not exist"
        assert app.platform.diagnostics["my_custom_spark_error_metric":{"marketplace_id": "2"}] is None

        with pytest.raises(ValueError):
            # 3rd variant above (see internal_spark_error_metric3) does not pre-define NAME dimension
            app.platform.diagnostics["my_custom_spark_error_metric":{"marketplace_id": "3"}].emit(1)

        # sub_dimensions = {} -> matches all of the variants
        # Diagnostics module can pick any of them at runtime.
        assert app.platform.diagnostics["my_custom_spark_error_metric":{}].signal in {
            internal_spark_error_metric.signal(),
            internal_spark_error_metric2.signal(),
            internal_spark_error_metric3.signal(),
        }
        assert (
            app.platform.diagnostics["my_custom_spark_error_metric":{"marketplace_id": "3"}].signal
            != app.platform.diagnostics["my_custom_spark_error_metric":{"marketplace_id": "1"}].signal
        )
        assert (
            app.platform.diagnostics["my_custom_spark_error_metric":{"marketplace_id": "1"}].signal
            == app.platform.diagnostics["my_custom_spark_error_metric":{"marketplace_id": "1"}].signal
        )

        with pytest.raises(ValueError):
            # fails due to mising metric NAME dimension (check related create_metric call below)
            app.platform.diagnostics["my_app_error_metric_def"].emit(1)
        # fetch via ID, but for metric emission "metric NAME" should be defined for a concrete metric instance to be emitted.
        app.platform.diagnostics["my_app_error_metric_def"]["MY_CUSTOM_ERROR"].emit(1)
        app.platform.diagnostics["my_app_error_metric_def"]["MY_CUSTOM_FAILURE"].emit(
            [MetricValueCountPairData(5.0), MetricValueCountPairData(Value=3.0, Count=2)]  # Count = 1 by default
        )

        app.platform.diagnostics["my_custom_spark_cumulative"]["Error"].emit(
            MetricStatisticData(SampleCount=10, Sum=20.0, Minimum=0.5, Maximum=10.5), timestamp=datetime(2021, 11, 1, 13)
        )

        self.patch_aws_stop()

    def test_application_alarming_and_metrics_alarm_ingestion(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app_name = "alarming-ingest"
        app = AWSApplication(app_name, self.region)

        generic_internal_metric = app.create_metric(id="my_app_error_metric_def")

        internal_alarm_id = "generic_error_alarm"
        generic_internal_alarm = app.create_alarm(
            id=internal_alarm_id,
            # returns a time series with each point either 1 or 0
            target_metric_or_expression="errors > 0 OR failures > 0",
            metrics={
                "errors": generic_internal_metric["MY_CUSTOM_ERROR"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                "failures": generic_internal_metric["MY_CUSTOM_FAILURE"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            },
            number_of_evaluation_periods=1,
            number_of_datapoint_periods=1,
            comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
            threshold=1,
        )

        monitor_failure_reactor = app.create_data(
            id="system_failure_reactor",
            inputs=[generic_internal_alarm[AlarmState.ALARM.value]],
            compute_targets=[NOOPCompute],  # no operation for test
        )

        monitor_health_reactor = app.create_data(
            id="system_health_restoration_reactor",
            inputs=[generic_internal_alarm["OK"]],  # will get triggered in both cases
            compute_targets=[NOOPCompute],  # no operation for test
        )

        composite_alarm_id = "composite_alarm"
        composite_alarm = app.create_composite_alarm(
            id=composite_alarm_id,
            # create an alarm within the rule (exhibit functional semantics if alarm ref won't be accessed anywhere else)
            # note that alarm rule uses 'ALARM' state by default
            # (if not alarm state is not specified for alarms used within)
            alarm_rule=generic_internal_alarm
            & app.create_alarm(
                id="_embedded_alarm_of_composite_alarm",
                target_metric_or_expression=generic_internal_metric["NUCLEAR_LAUNCH_DETECTED"][MetricStatistic.SAMPLE_COUNT][
                    MetricPeriod.MINUTES(5)
                ],
                number_of_evaluation_periods=1,
                number_of_datapoint_periods=1,
                comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                threshold=1,
            ),
        )

        nuclear_attack_reactor = app.create_data(
            id="nuclear_attack_reaction_node",
            inputs=[composite_alarm["ALARM"]],
            compute_targets=[
                # No Operation
                # don't hit the red button, because this is just a test
                NOOPCompute
            ],
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate(allow_concurrent_executions=False)

        # 1- Use application level references as internal signals to trigger
        # OK -> ALARM
        alarm_timestamp = "2021-11-10 00:00:00"
        app.process(generic_internal_alarm["ALARM"][alarm_timestamp])

        path, _ = app.poll(monitor_failure_reactor["ALARM"][alarm_timestamp])
        assert path

        # verify OK->ALARM does not trigger health monitor
        path, _ = app.poll(monitor_health_reactor["OK"][alarm_timestamp])
        assert not path

        # ALARM->OK (30 mins later)
        alarm_timestamp = "2021-11-10 00:30:00"
        app.process(generic_internal_alarm["OK"][alarm_timestamp])
        # verify ALARM->OK does not trigger failure monitor
        path, _ = app.poll(monitor_failure_reactor["ALARM"][alarm_timestamp])
        assert not path

        path, _ = app.poll(monitor_health_reactor["OK"][alarm_timestamp])
        assert path  # gets triggered and successful

        # 2- EMULATE Runtime Alarm event Ingestion from AWS Cloudwatch
        # Now use raw AWS CW Alarm events (SNS version, not CW eventbridge format which is also supported)
        # use 'generic_error_alarm' (as CW_ALARM, not CW_COMPOSITE_ALARM)
        # This should have equivalent effect as
        #   app.process(generic_internal_alarm["ALARM"]["2021-11-10 02:45:55"])
        alarm_timestamp = "2021-11-10T02:45:55"
        app.process(
            {
                "Records": [
                    {
                        "EventSource": "aws:sns",
                        "EventVersion": "1.0",
                        "EventSubscriptionArn": "NOT_IMPORTANT",
                        "Sns": {
                            "Type": "Notification",
                            "MessageId": "12c36222-e1fe-56f5-9540-3827d55d7732",
                            "TopicArn": "NOT_IMPORTANT",
                            "Subject": "NOT_IMPORTANT",
                            "Message": f'{{"AlarmName":"if-{app_name}_{self.account_id}_{self.region}-{internal_alarm_id}","AlarmDescription":"","AWSAccountId":"{self.account_id}","NewStateValue":"ALARM","NewStateReason":"NOT_IMPORTANT","StateChangeTime":"{alarm_timestamp}.640+0000","Region":"US East (N. Virginia)","AlarmArn":"arn:aws:cloudwatch:{self.region}:{self.account_id}:alarm:if-{app_name}_{self.account_id}_{self.region}-{internal_alarm_id}","OldStateValue":"OK","Trigger":{{}}}}',
                            "Timestamp": "NOT_IMPORTANT / SHOULD NOT BE USED",
                            "SignatureVersion": "1",
                            "Signature": "NOT_IMPORTANT",
                            "SigningCertUrl": "NOT_IMPORTANT",
                            "UnsubscribeUrl": "NOT_IMPORTANT",
                            "MessageAttributes": {},
                        },
                    }
                ]
            }
        )

        # now check the propagation (alarm triggers dependent node)
        path, _ = app.poll(monitor_failure_reactor["ALARM"][alarm_timestamp])
        assert path

        # inject 'composite_alarm' (as CW_COMPOSITE_ALARM, not CW_ALARM)
        # This should have equivalent effect as
        #   app.process(composite["ALARM"]["2021-11-11 03:05:01"])
        alarm_timestamp = "2021-11-11T03:05:01"
        app.process(
            {
                "Records": [
                    {
                        "EventSource": "aws:sns",
                        "EventVersion": "1.0",
                        "EventSubscriptionArn": "NOT_IMPORTANT",
                        "Sns": {
                            "Type": "Notification",
                            "MessageId": "22c22222-eeee-56f5-0541-9827d55d7777",
                            "TopicArn": "NOT_IMPORTANT",
                            "Subject": "NOT_IMPORTANT",
                            "Message": f'{{"AlarmName":"if-{app_name}_{self.account_id}_{self.region}-{composite_alarm_id}","AlarmDescription":"","AWSAccountId":"{self.account_id}","NewStateValue":"ALARM","NewStateReason":"NOT_IMPORTANT","StateChangeTime":"{alarm_timestamp}.640+0000","Region":"US East (N. Virginia)","AlarmArn":"arn:aws:cloudwatch:{self.region}:{self.account_id}:alarm:if-{app_name}_{self.account_id}_{self.region}-{composite_alarm_id}","OldStateValue":"OK","AlarmRule":"EXISTENCE IS ENOUGH to understand whether this is COMPOSITE","TriggeringChildren":[]}}',
                            "Timestamp": "NOT_IMPORTANT / SHOULD NOT BE USED",
                            "SignatureVersion": "1",
                            "Signature": "NOT_IMPORTANT",
                            "SigningCertUrl": "NOT_IMPORTANT",
                            "UnsubscribeUrl": "NOT_IMPORTANT",
                            "MessageAttributes": {},
                        },
                    }
                ]
            }
        )

        # now check the propagation (alarm triggers the node that depends on the composite alarm)
        path, _ = app.poll(nuclear_attack_reactor["ALARM"][alarm_timestamp])
        assert path

        self.patch_aws_stop()

    def test_application_alarming_and_metrics_orchestration_metrics(self):
        """Test orchestration metrics in development time to capture compatibility with the
        related APIs and the UX.
        Integ-tests capture the entire flow by triggering executions and observing the
        imlicit effect of orchestration metrics on the dependent alarms, etc.

        Note: orchestration metrics differ from 'routing' internal metrics. They provide holistic / cumulative view
        over events, state-transitions over routes.
        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app_name = "alarming-orch1"
        app = AWSApplication(app_name, self.region)

        # ORCHESTRATION METRICS
        orchestration_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.ORCHESTRATION)

        # check system metrics and see their alias' and sub-dimensions!
        for driver_metric_map in orchestration_metrics_map.values():
            for metric in driver_metric_map.values():
                # dumps metric group ID/alias -> specific MetricNames and other details
                assert json.loads(metric.describe())["metric_stats"]

        processor_metrics_map = orchestration_metrics_map[ProcessingUnit]
        routing_metrics_map = orchestration_metrics_map[RoutingTable]

        processor_error_signal = processor_metrics_map["processor.event.error.type"]

        processor_alarm = app.create_alarm(
            id="processor_error_alarm",
            target_metric_or_expression="(m1 > 0 OR m2 > 0)",
            metrics={
                "m1": processor_error_signal["NameError"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                "m2": processor_error_signal["Exception"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            },
        )

        # Capture most common orchestration issues (due to DDB, etc) when RheocerOS cannot load or persist the state
        # of a node also route hit metric is provided within this signal.
        # Please note that all of the routes contribute to 'orchestration' metrics.
        # For the 'route' specific version of these metrics please check the next (route specific) test in this module.
        cumulative_routing_receive_state_signal = routing_metrics_map["routing_table.receive"]
        for receive_route_metric_name in ["RouteHit", "RouteLoadError", "RouteSaveError"]:
            # cheating here by using 'protected' Application::_get_input_signal that powers most of the Application APIs
            # to be able to check the materialization of signal with the user provided dimensions, in a consistent way.
            assert app._get_input_signal(
                cumulative_routing_receive_state_signal[receive_route_metric_name][MetricStatistic.SUM][MetricPeriod.MINUTES(5)]
            )

        routing_receive_hook_metric_signal = routing_metrics_map["routing_table.receive.hook"]
        # verify all of the critical state-change/hook points are tracked by the metric
        for hook_type in [
            RoutingHookInterface.Execution.IExecutionSkippedHook,
            RoutingHookInterface.Execution.IExecutionBeginHook,
            RoutingHookInterface.Execution.IExecutionSuccessHook,
            RoutingHookInterface.Execution.IExecutionFailureHook,
            RoutingHookInterface.Execution.IComputeFailureHook,
            RoutingHookInterface.Execution.IComputeSuccessHook,
            RoutingHookInterface.Execution.IComputeRetryHook,
        ]:
            assert app._get_input_signal(
                routing_receive_hook_metric_signal[hook_type.__name__][MetricStatistic.SUM][MetricPeriod.MINUTES(5)]
            )

        routing_persistence_alarm = app.create_alarm(
            id="routing_persistence_alarm",
            target_metric_or_expression="(m1 > 0 OR m2 > 0)",
            metrics={
                "m1": cumulative_routing_receive_state_signal["RouteLoadError"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                "m2": cumulative_routing_receive_state_signal["RouteSaveError"][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
            },
            number_of_evaluation_periods=3,
            number_of_datapoint_periods=2,
        )

        orchestration_monitor = app.create_composite_alarm(
            id="orchestration_monitor", alarm_rule=processor_alarm | routing_persistence_alarm
        )

        processor_failure_reactor = app.create_data(
            id="orchestration_failure_reactor", inputs=[orchestration_monitor[AlarmState.ALARM.value]], compute_targets=[NOOPCompute]
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate()

        self.patch_aws_stop()

    def test_application_alarming_and_metrics_orchestration_route_metrics(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app_name = "alarming-orch2"
        app = AWSApplication(app_name, self.region)

        # 1- let's create a sample node with no inputs. we will execute this manually only.
        node_id = "metric_emitting_node"
        test_node = app.create_data(id=node_id, compute_targets=[NOOPCompute])

        # get orchestration metrics specific to this node
        test_node_metrics = app.get_route_metrics(test_node)

        for driver_metric_map in test_node_metrics.values():
            for metric in driver_metric_map.values():
                # dumps metric group ID/alias -> specific MetricNames and other details
                assert json.loads(metric.describe())["metric_stats"]

        test_node_routing_metrics_map = test_node_metrics[RoutingTable]

        test_node_state_metric_signal = test_node_routing_metrics_map["routing_table.receive.hook" + "." + node_id]

        # verify all of the critical state-change/hook points are tracked by the metric
        for hook_type in [
            RoutingHookInterface.Execution.IExecutionSkippedHook,
            RoutingHookInterface.Execution.IExecutionBeginHook,
            RoutingHookInterface.Execution.IExecutionSuccessHook,
            RoutingHookInterface.Execution.IExecutionFailureHook,
            RoutingHookInterface.Execution.IComputeFailureHook,
            RoutingHookInterface.Execution.IComputeSuccessHook,
            RoutingHookInterface.Execution.IComputeRetryHook,
            RoutingHookInterface.Execution.IExecutionCheckpointHook,
            RoutingHookInterface.PendingNode.IPendingNodeCreationHook,
            RoutingHookInterface.PendingNode.IPendingNodeExpirationHook,
            RoutingHookInterface.PendingNode.IPendingCheckpointHook,
        ]:
            # if the signal does not have the hook within its dimension filter (as one of the 'Name' dimension values)
            assert app._get_input_signal(test_node_state_metric_signal[hook_type.__name__][MetricStatistic.SUM][MetricPeriod.MINUTES(5)])

        test_node_failure_alarm = app.create_alarm(
            id="metric_emitting_node_failure",
            target_metric_or_expression=test_node_state_metric_signal[RoutingHookInterface.Execution.IExecutionFailureHook.__name__][
                MetricStatistic.SUM
            ][MetricPeriod.MINUTES(5)],
            number_of_evaluation_periods=1,
            number_of_datapoint_periods=1,
            comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
            # since we are using logical/conditional operator in metric math, the output series only contains 1s or 0s.
            # so threshold will be 1 to detect.
            threshold=1,
        )

        test_node_failure_reactor = app.create_data(
            id="test_node_failure_reactor",
            inputs=[test_node_failure_alarm["ALARM"]],
            compute_targets=[NOOPCompute],
            # we are overwriting dimension spec here since the default dimension spec
            # from input alarm would have 'time' dimension format in seconds "%Y-%m-%d %H:%M:%S"
            # which would be very hard to track with poll API using datetime.now(), example:
            #     path, records=app.poll(test_node_failure_reactor['ALARM'][datetime.now()])
            #
            # othwerwise this parameter can be easily ignored, see 'test_node_monitor_reactor' below.
            output_dimension_spec={
                AlarmDimension.STATE_TRANSITION.value: {
                    "type": "STRING",
                    AlarmDimension.TIME.value: {
                        "type": "DATETIME",
                        # Alarm default time format is high-precision (seconds level)
                        # Use hour-level time dimension on the output of this node.
                        # this is the reason we are defining the spec here.
                        "format": "%Y-%m-%d %H",
                    },
                }
            },
        )

        test_node_state_timing_signal = test_node_routing_metrics_map["routing_table.receive.hook.time_in_utc" + "." + node_id]

        # detect if execution is longer than hour (60 * 60 secs)
        test_node_exec_duration_alarm = app.create_alarm(
            id="metric_emitting_long_execution",
            target_metric_or_expression="(end1 - start) > 3600 OR (end2 - start) > 3600",
            metrics={
                # both in utc (seconds) and over a window of 3 hours
                "start": test_node_state_timing_signal[RoutingHookInterface.Execution.IExecutionBeginHook.__name__][MetricStatistic.SUM][
                    MetricPeriod.MINUTES(3 * 60)
                ],
                "end1": test_node_state_timing_signal[RoutingHookInterface.Execution.IExecutionSuccessHook.__name__][MetricStatistic.SUM][
                    MetricPeriod.MINUTES(3 * 60)
                ],
                "end2": test_node_state_timing_signal[RoutingHookInterface.Execution.IExecutionFailureHook.__name__][MetricStatistic.SUM][
                    MetricPeriod.MINUTES(3 * 60)
                ],
            },
        )

        test_node_monitor = app.create_composite_alarm(
            id="test_node_monitor", alarm_rule=test_node_failure_alarm | test_node_exec_duration_alarm
        )

        test_node_monitor_reactor = app.create_data(
            id="test_node_monitor_reactor",
            inputs=[test_node_monitor["ALARM"]],
            compute_targets=[
                NOOPCompute
                # replace with Spark, PrestoSQL, Lambda code, Email,..
            ],
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate()

        # extra coverage for low-precision reactor 'time' dimension
        #  test the effect of output_dimension_spec overwrite to use simplified (daily) time format on the output
        #  of reactor node
        high_precision_alarm_time = "2021-11-06T23:53:22"
        app.process(test_node_failure_alarm["ALARM"][high_precision_alarm_time])

        low_precision_alarm_time = "2021-11-06T23"  # or "2021-11-06 23"
        path, _ = app.poll(test_node_failure_reactor["ALARM"][low_precision_alarm_time])
        assert path

        self.patch_aws_stop()
