# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.compute_targets.email import EMAIL
from intelliflow.core.platform.constructs import BatchCompute as BatchComputeDriver

flow.init_basic_logging()

app_name = "alarming-ex"

# automatically reads default credentials
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
app = AWSApplication(app_name, "us-east-1")

# SYSTEM METRICS
system_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.SYSTEM)

processor_metrics_map = system_metrics_map[ProcessingUnit]
batch_compute_metrics_map = system_metrics_map[BatchComputeDriver]
routing_metrics_map = system_metrics_map[RoutingTable]


# Dump system metrics and see their alias' and sub-dimensions!
for metric in processor_metrics_map.values():
    print(metric.dimensions())
    # dumps metric group ID/alias -> specific MetricNames and other details
    print(metric.describe())

for metric in batch_compute_metrics_map.values():
    print(metric.dimensions())
    # dumps metric group ID/alias -> specific MetricNames and other details
    print(metric.describe())

for metric in routing_metrics_map.values():
    print(metric.dimensions())
    # dumps metric group ID/alias -> specific MetricNames and other details
    print(metric.describe())

# 1-use metric ID/alias' to retrieve them from the map
# 2-and then use 'MetricName' to get a concrete/materialized metric to be used in an Alarm.
# these signals can be bind into alarms now
routing_table_metric_signal = routing_metrics_map['routingTable']
print(routing_table_metric_signal.describe())
routing_table_getitem_operation_metric_signal = routing_metrics_map['routingTable.GetItem']
# the following can be directly inputted to an alarm by starting with Statistic as the first dimension
routing_table_getitem_operation_latency_metric = routing_table_getitem_operation_metric_signal["SuccessfulRequestLatency"]

# Get AWS Glue 'Type=count' Python GlueVersion=2.0 metrics
# This is an actual RheocerOS metric descriptor/signal (provided by AWS Glue) that can be inputted to an alarm. But its 'metric name' dimension has too
# many values. They all have one thing in common: being count based. So it is advised to further specify the 'name' dimension
# of this metric descriptor before inputting it to an alarm.
batch_compute_python_glue_version_2_0_metric_unfiltered = batch_compute_metrics_map['batchCompute.typeCount.python.2.0']
# 'name' dimension of the metric is specified. when inputting to an alarm the remaining dimensions 'statistic', 'period' can be
# specified by the user (see create_alarm API calls below).
batch_compute_aggregate_elapsed_time_materialized_metric = batch_compute_python_glue_version_2_0_metric_unfiltered["glue.driver.aggregate.elapsedTime"]

batch_compute_python_glue_version_1_0_metric_unfiltered = batch_compute_metrics_map['batchCompute.typeCount.python.1.0']
batch_compute_python_glue_version_3_0_metric_unfiltered = batch_compute_metrics_map['batchCompute.typeCount.python.3.0']

processor_core_metric_signal = processor_metrics_map["processor.core"]

# low level alarm into an underlying driver resource
# raise if write throttling in RheocerOS routing table is more than 50 in 15 mins in two data-points out of 3.
#       or route object retrieval latency is more than 500ms.
#
# Please note that this alarm can still be aggregated into a composite alarm (which would still use the same dedupe_str)
# this can enable you to partition alarm definitions and then merge them to create system wide OE view.
routing_table_alarm = app.create_alarm(id="routing_table_alarm",
                                   # refer
                                   #   https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html
                                   target_metric_or_expression="(m1 > 50 OR m2 > 500)",
                                   metrics={
                                      "m1": routing_table_metric_signal['WriteThrottleEvents'][MetricStatistic.SUM][MetricPeriod.MINUTES(15)],
                                      "m2": routing_table_getitem_operation_latency_metric[MetricStatistic.AVERAGE][MetricPeriod.MINUTES(15)]
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
                                       INSUFFICIENT_DATA_ACTIONS=set()
                                   )
                                   )

# Sev5 on core Processor (paranoid / warning shot for oncall)
# if Processor has an execution Error (message sent to DLQ probably) or duration max hits 600 x 1000 (10 minutes, approacing to 15 mins timeout)
processor_alarm = app.create_alarm(id="processor_alarm",
                                       # refer
                                       #   https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html
                                       target_metric_or_expression="(m1 > 1 OR m2 > 600000)",
                                       metrics={
                                           "m1": processor_core_metric_signal['Errors'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                                           "m2": processor_core_metric_signal['Duration'][MetricStatistic.MAXIMUM][MetricPeriod.MINUTES(5)]
                                       },
                                       number_of_evaluation_periods=1,
                                       number_of_datapoint_periods=1,
                                       comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                       # since we are using logical/conditional operator in metric math, the output series only contains 1s or 0s.
                                       # so threshold will be 1 to detect.
                                       threshold=1,
                                       default_actions=AlarmDefaultActionsMap(
                                           ALARM_ACTIONS=set(),
                                           OK_ACTIONS=set(),
                                           INSUFFICIENT_DATA_ACTIONS=set()
                                       )
                                       )

# Create a sample alarm to track the job failures in underlying BatchCompute driver
batch_compute_alarm = app.create_alarm(id="batch_compute_failed_task",
                                       # refer
                                       #   https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html
                                       target_metric_or_expression="(m1 > 1 OR m2 > 1 OR m3 > 1)",
                                       metrics={
                                           "m1": batch_compute_python_glue_version_1_0_metric_unfiltered['glue.driver.aggregate.numFailedTask'][MetricStatistic.SUM][MetricPeriod.MINUTES(15)],
                                           "m2": batch_compute_python_glue_version_2_0_metric_unfiltered['glue.driver.aggregate.numFailedTask'][MetricStatistic.SUM][MetricPeriod.MINUTES(15)],
                                           "m3": batch_compute_python_glue_version_3_0_metric_unfiltered['glue.driver.aggregate.numFailedTask'][MetricStatistic.SUM][ MetricPeriod.MINUTES(15)]
                                       },
                                       number_of_evaluation_periods=1,
                                       number_of_datapoint_periods=1,
                                       comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                       # since we are using logical/conditional operator in metric math, the output series only contains 1s or 0s.
                                       # so threshold will be 1 to detect.
                                       threshold=1,
                                       default_actions=AlarmDefaultActionsMap(
                                           ALARM_ACTIONS=set(),
                                           OK_ACTIONS=set(),
                                           INSUFFICIENT_DATA_ACTIONS=set()
                                       )
                                       )

# METRICS
# import a metric definition from the same account
# generic representation of the metrics from this particular lambda.
external_lambda_metric = app.marshal_external_metric(external_metric_desc=CWMetric(namespace="AWS/Lambda"),
                                              id="lambda_metric",
                                              sub_dimensions={
                                                  "FunctionName": "if-AWSLambdaProcessorBasic-andes-shanant-us-east-1"
                                              })

# import the same metric in a different, more flexible way.
#   This shows Lambda Error metric can be imported into the system in
#   different ways (different IDs, default alias').
#   And also with defaults/overwrites on metric dimensions
external_lambda_error_metric_on_another_func = app.marshal_external_metric(
                                               external_metric_desc=CWMetric(namespace="AWS/Lambda"),
                                               id="my_test_function_error",
                                               dimension_filter=\
                                               {
                                                   "Error": {  # only keep 'Error'
                                                       MetricStatistic.SUM: {  # support SUM only
                                                           MetricPeriod.MINUTES(5): {  # restrict the use of this metric with 5 mins period only (in alarms)
                                                               "*": {  # (reserved) Any MetricDimension.TIME
                                                               }
                                                           }
                                                       }
                                                   }
                                               },
                                               sub_dimensions={
                                                   "FunctionName": "if-AWSLambdaProcessorBasic-andes-shanant-us-east-1"
                                               })

# 'id' field here represents 'Metric Group Id' (metric sub-space), it will be a part of the concrete metric instance
# (in AWS CloudWactch for example) as a sub_dimension ({'MetricGroupId': 'my_custom_spark_error_metric'}) along with
# other sub-dimensions.
# It is also used to retrieve this abstract 'metric' declaration during development and also from Diagnostics
# module at runtime.
# Do the following in your Spark code to emit at runtime for example:

#   runtime_platform.diagnostics["my_custom_spark_error_metric"]["Error"].emit(1)
internal_spark_error_metric = app.create_metric(id="my_custom_spark_error_metric",
                                    dimension_filter={
                                        "Error": {  # only keep 'Error'
                                            "*": {  # support all statistic (no filter spec)
                                                "*": {  # support all periods (during the emission sets Storage Resolution to 1)
                                                    "*": {  # (reserved) Any MetricDimension.TIME
                                                    }
                                                }
                                            }
                                        }
                                    }
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

# WARNING
#  - please note that same 'id' can be shared among metrics unless their sub_dimensions are identical.
#  - this metric and the one above map to different metric groups at runtime. so metrics emitted from them cannot
# overlap. they should be treated as totally different when being inputted into alarms, etc.
generic_internal_metric_2 = app.create_metric(id="my_app_error_metric_def",
                                              sub_dimensions={
                                                  "marketplace": "1",
                                                  "region": "NA"
                                              })

# ALARMS
error_system_level_dedupe_string = f"{app_name}-ERROR"

# ALARM with default ticket action
etl_error_alarm = app.create_alarm(id="one_or_more_spark_executions_failed",
                                   target_metric_or_expression=internal_spark_error_metric['Error'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                                   number_of_evaluation_periods=1,
                                   number_of_datapoint_periods=1,
                                   comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                   threshold=1,
                                   default_actions=AlarmDefaultActionsMap(
                                                   ALARM_ACTIONS=set(),
                                                   OK_ACTIONS=set(),
                                                   INSUFFICIENT_DATA_ACTIONS=set()
                                                   )
                                   )

generic_internal_alarm = app.create_alarm(id="generic_error_alarm",
                                   #  refer
                                   #    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html#metric-math-syntax
                                   target_metric_or_expression="errors > 0 OR failures > 0",  # returns a time series with each point either 1 or 0
                                   metrics={
                                       "errors": generic_internal_metric['MY_CUSTOM_ERROR'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                                       "failures": generic_internal_metric['MY_CUSTOM_FAILURE'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)]
                                   },
                                   number_of_evaluation_periods=1,
                                   number_of_datapoint_periods=1,
                                   comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                   threshold=1,
                                   default_actions=AlarmDefaultActionsMap(ALARM_ACTIONS=set())
                                   )

# no default actions on this one, will be used in a composite alarm.
system_failure_alarm = app.create_alarm(
                                 id="system_failure",
                                 #  refer
                                 #    https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/using-metric-math.html#metric-math-syntax
                                 target_metric_or_expression="SUM(METRICS())",
                                 # will validate if metrics are materialized (i.e NAME, Statistic and Period dimensions are material or not).
                                 metrics=[external_lambda_metric['Error'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                                          external_lambda_error_metric_on_another_func],
                                 number_of_evaluation_periods=5,
                                 number_of_datapoint_periods=3,
                                 comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                 threshold=1
                                 )

composite_alarm = app.create_composite_alarm(id="system_monitor",
                                             # equivalent logic
                                             # alarm_rule=~(etl_error_alarm['OK'] & generic_internal_alarm['OK'] & system_failure_alarm['OK']),
                                             alarm_rule=etl_error_alarm | generic_internal_alarm['ALARM'] | ~system_failure_alarm[AlarmState.OK.value],
                                             default_actions = AlarmDefaultActionsMap(
                                                ALARM_ACTIONS=set())
                                            )


class MonitorFailureReactorLambda(IInlinedCompute):

    def __call__(self, input_map: Dict[str, Signal], materialized_output: Signal, params: Dict[str, Any]) -> Any:
        from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams

        print(f"Hello from AWS Lambda account_id {params[AWSCommonParams.ACCOUNT_ID]}, region {params[AWSCommonParams.REGION]}")
        s3 = params[AWSCommonParams.BOTO_SESSION].client("s3")
        cw = params[AWSCommonParams.BOTO_SESSION].client("cloudwatch")

        # do extra stuff when the following monitor ('system_failure_reactor') is in alarm.
        # TODO
        return

class MonitorReactorLambda(IInlinedCompute):

    def __call__(self, input_map: Dict[str, Signal], materialized_output: Signal, params: Dict[str, Any]) -> Any:
        from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
        from intelliflow.core.signal_processing.definitions.metric_alarm_defs import AlarmDimension

        composite_alarm_signal = input_map.values()[0]
        print(f"Reacting to {composite_alarm_signal.get_materialized_resource_paths()[0]} in account_id {params[AWSCommonParams.ACCOUNT_ID]}, region {params[AWSCommonParams.REGION]}")

        # "'OK' or 'ALARM' or 'INSUFFICIENT_DATA'"
        current_alarm_state: str = composite_alarm_signal.domain_spec.dimension_filter_spec.find_dimension_by_name(AlarmDimension.STATE_TRANSITION).value
        print(f"New state of alarm {composite_alarm_signal.alias} is {current_alarm_state!r}.")

        s3 = params[AWSCommonParams.BOTO_SESSION].client("s3")
        cw = params[AWSCommonParams.BOTO_SESSION].client("cloudwatch")

        # do extra stuff when the monitor ('system_failure_reactor') is either in alarm or ok state.
        # TODO
        return


monitor_failure_reactor = app.create_data(id="system_failure_reactor",
                                  inputs=[composite_alarm[AlarmState.ALARM.value]],
                                  compute_targets=[
                                      # reactor 1
                                      InlinedCompute(MonitorFailureReactorLambda()),
                                      # also send an email
                                      EMAIL(
                                        sender="if-test-list@amazon.com",
                                        recipient_list=["if-test@amazon.com"]
                                           ).action(sender="", subject="SYSTEM IS NOT HEALTHY", body="Provide your custom description")
                                  ])

monitor_reactor = app.create_data(id="system_monitor_reactor",
                                  inputs=[composite_alarm['*']],  # will get triggered in both cases
                                  compute_targets=[
                                      InlinedCompute(MonitorReactorLambda())
                                  ])

# TEST emission from batch compute
# use 'runtime_platform'
trigger_alarm = app.create_data("trigger_alarm",
                                compute_targets=[
                                    BatchCompute(
                                        code="""
runtime_platform.diagnostics["my_app_error_metric_def"]["MY_CUSTOM_ERROR"].emit(1)
# DONOT use MetricValueCountPairData in GlueVersion 1.0, boto version does not support Values, Counts parameters and
# will cause ParamValidationError.
#runtime_platform.diagnostics["my_app_error_metric_def"]["MY_CUSTOM_FAILURE"].emit([MetricValueCountPairData(5.0), # Count = 1 by default
#                                                                                  MetricValueCountPairData(Value=3.0, Count=2)])


from pyspark.sql.types import *
field = [StructField('FIELDNAME_1', StringType(), True),
         StructField('FIELDNAME_2', StringType(), True)]
schema = StructType(field)
output = spark.createDataFrame(sc.emptyRDD(), schema)
                                        """,
                                        WorkerType=GlueWorkerType.G_1X.value,
                                        NumberOfWorkers=5,
                                        GlueVersion="2.0")])

app.activate(allow_concurrent_executions=False)

# TEST / MANUAL emission from local Python dev-endpoint
'''
 app.platform.diagnostics.get_internal_metric("my_custom_spark_error_metric").emit(1)
 # fails due to mising metric NAME dimension (check related create_metric call below)
 # app.platform.diagnostics["my_app_error_metric_def"].emit(1)
 # fetch via ID, but for metric emission "metric NAME" should be defined for a concrete metric instance to be emitted.
 app.platform.diagnostics["my_app_error_metric_def"]["MY_CUSTOM_ERROR"].emit(1)
 app.platform.diagnostics["my_app_error_metric_def"]["MY_CUSTOM_FAILURE"].emit([MetricValueCountPairData(5.0), # Count = 1 by default
                                                                              MetricValueCountPairData(Value=3.0, Count=2)])
'''
'''Emit for the other variant of custom/produced metrci my_app_error_metric_def'''
app.platform.diagnostics["my_app_error_metric_def": {"marketplace": "1", "region": "NA"}]["MY_CUSTOM_ERROR"].emit(1)

# test remote (BatchCompute) emission
app.execute(trigger_alarm)

# trigger execution on 'ducsi_with_AD_orders_NA'
# intentionally use process on the first execution to check raw (glue table) event handling as well.
# 1- 2021-01-13
# inject synthetic 'd_ad_orders_na' into the system
app.process(
    {
        "version": "0",
        "id": "3a0fe2d2-eedc-0535-4505-96dc8e6eb33a",
        "detail-type": "Glue Data Catalog Table State Change",
        "source": "aws.glue",
        "account": "842027028048",
        "time": "2021-01-13T00:39:25Z",
        "region": "us-east-1",
        "resources": ["arn:aws:glue:us-east-1:842027028048:table/dex_ml_catalog/d_ad_orders_na"],
        "detail": {
            "databaseName": "dex_ml_catalog",
            "changedPartitions": ["[2021-01-13 00:00:00]"],
            "typeOfChange": "BatchCreatePartition",
            "tableName": "d_ad_orders_na"
        }
    }
)

# OPTIONAL freeze event-propagation (triggers, executions), cloud infrastructure
# - avoid costs
# - note: immediate pause (right after async 'process' call above) might prevent remote execution. so this test/example
# code was written to be used with interactive debugger (with enough wait-time for execution to complete and its alarming
# effects / notifications to be received)
app.pause()
