# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from datetime import datetime, timedelta

import time

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.compute_targets.email import EMAIL
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import AlarmDimension
from intelliflow.utils.test.inlined_compute import NOOPCompute

flow.init_basic_logging()

# automatically reads default credentials
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
app = AWSApplication("sim-app", "us-east-1")


#1- let's create a sample node with no inputs. we will execute this manually only.
test_node = app.create_data(id="metric_emitting_node",
                            compute_targets=[
                                InlinedCompute(
                                    lambda x, y, z: int("FAIL!")
                                )
                            ])

# get orchestration metrics specific to this node
test_node_metrics = app.get_route_metrics(test_node)

test_node_routing_metrics_map = test_node_metrics[RoutingTable]
#for alias, metric_signal in test_node_routing_metrics_map.items():
#    print(f"Metric signal alias for test_node: {alias}")
#    print("Actual/concrete metric stats:")
#    print(metric_signal.describe())

test_node_state_metric_signal = test_node_routing_metrics_map["routing_table.receive.hook" + ".metric_emitting_node"]

test_node_failure_alarm = app.create_alarm(id="metric_emitting_node_failure",
                                   target_metric_or_expression=test_node_state_metric_signal[RoutingHookInterface.Execution.IExecutionFailureHook.__name__][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                                   number_of_evaluation_periods=1,
                                   number_of_datapoint_periods=1,
                                   comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                   # since we are using logical/conditional operator in metric math, the output series only contains 1s or 0s.
                                   # so threshold will be 1 to detect.
                                   threshold=1
                                   )

test_node_failure_reactor = app.create_data(id="test_node_failure_reactor",
                                            inputs=[test_node_failure_alarm["ALARM"]],
                                            compute_targets=[
                                                EMAIL(sender="if-test-list@amazon.com",  # sender from the same account (see './doc/EMAIL_ACTION_SETUP_AND_INFO.md')
                                                      recipient_list=["foo-ml-engineers@amazon.com"]).action()
                                            ],
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
                                                          "format": "%Y-%m-%d" # this is the reason we are defining the spec here
                                                       }
                                                }
                                            })


test_node_state_timing_signal = test_node_routing_metrics_map["routing_table.receive.hook.time_in_utc" + ".metric_emitting_node"]

# detect if execution is longer than hour (60 * 60 secs)
test_node_exec_duration_alarm = app.create_alarm(id="metric_emitting_long_execution",
                                           target_metric_or_expression="(end1 - start) > 3600 OR (end2 - start) > 3600",
                                           metrics={
                                               # both in utc (seconds) and over a window of 3 hours
                                               "start": test_node_state_metric_signal[RoutingHookInterface.Execution.IExecutionBeginHook.__name__][MetricStatistic.SUM][MetricPeriod.MINUTES(3 * 60)],
                                               "end1": test_node_state_metric_signal[RoutingHookInterface.Execution.IExecutionSuccessHook.__name__][ MetricStatistic.SUM][MetricPeriod.MINUTES(3 * 60)],
                                               "end2": test_node_state_metric_signal[ RoutingHookInterface.Execution.IExecutionFailureHook.__name__][ MetricStatistic.SUM][MetricPeriod.MINUTES(3 * 60)]
                                           },
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

test_node_monitor = app.create_composite_alarm(id="test_node_monitor",
                                               alarm_rule=test_node_failure_alarm | test_node_exec_duration_alarm)

test_node_monitor_reactor = app.create_data(id="test_node_monitor_reactor",
                                            inputs=[test_node_monitor["ALARM"]],
                                            compute_targets=[
                                                NOOPCompute  # replace with Spark, PrestoSQL, Lambda code, Email,..
                                            ])

app.activate()


# TIP: go to AWS console and observe the status of alarms (especially 'test_node_monitor') in auto-generated Cloudwatch Dashboard.

# TEST STRATEGY 1: use natural application flow starting from auto-emitted orchestration metrics via manual node exec,
# these metrics will trigger alarms, then those alarms will trigger downstream reactor nodes.
#
# now start the chain of events
# - start a forced execution on test node
try:
    app.execute(test_node)
    assert False, "test_node execution must have failed!"
except RuntimeError:
    pass

# it must have failed
path, failure_compute_records = app.poll(test_node)
assert not path and failure_compute_records

# - then (in AWS) failure emits test_node_state_metric_signal with the following metric stat:
#      {
#           "MetricName": "IExecutionFailureHook,
#           "Dimensions": {
#               "RouteID": "metric_emitting_node"
#           }
#      }
#
# - this metric triggers test_node_failure_alarm (AlarmName="metric_emitting_node_failure")
# - and this alarm triggers:
#    1- test_node_monitor (AlarmName="test_node_monitor")
#    2- More importantly, the reactor node test_node_failure_reactor created to send out an EMAIL
#
# so let's poll the reactor node programmatically (it should also send out an email)
while True:
    path, records = app.poll(test_node_failure_reactor['ALARM'][datetime.now()])

    if not path and not records:
        # no execution detected yet [poll immediately returned (None, None)], let's poll again
        time.sleep(5)
    else:
        assert path, "trigger/execution on 'test_node_failure_reactor' must have succeeded"
        break

# TEST STRATEGY 2: By-pass metrics and directly inject syntetic raw CW alarms into the platform, these should
# trigger downstream reactor nodes only. This is inferior to the strategy above as it ignores the propagation from
# metrics to Alarms. This method will be heavily used in unit-tests.

## EMULATE Alarm state-change transition for alarm 'metric_emitting_node_failure'
#app.process(
#    {'Records': [
#                  {
#                   'EventSource': 'aws:sns',
#                   'EventVersion': '1.0',
#                   'EventSubscriptionArn': 'arn:aws:sns:us-east-1:427809481713:if-sim-app-AWSCloudWatchDiagnostics:4761dba2-cacd-42d4-8f9a-03de0745188d',
#                    'Sns': {
#                          'Type': 'Notification',
#                          'MessageId': '57c992bc-21f6-5a74-bda5-c8b9155571ca',
#                          'TopicArn': 'arn:aws:sns:us-east-1:427809481713:if-sim-app-AWSCloudWatchDiagnostics',
#                          'Subject': 'ALARM: "if-sim-app_427809481713_us-east-1-metric_emitting_node_failure" in US East (N. Virginia)',
#                          'Message': '{"AlarmName":"if-sim-app_427809481713_us-east-1-metric_emitting_node_failure","AlarmDescription":null,"AWSAccountId":"427809481713","NewStateValue":"ALARM","NewStateReason":"Threshold Crossed: 1 out of the last 1 datapoints [1.0 (10/11/21 02:40:00)] was greater than or equal to the threshold (1.0) (minimum 1 datapoint for OK -> ALARM transition).","StateChangeTime":"2021-11-10T02:45:55.640+0000","Region":"US East (N. Virginia)","AlarmArn":"arn:aws:cloudwatch:us-east-1:427809481713:alarm:if-sim-app_427809481713_us-east-1-metric_emitting_node_failure","OldStateValue":"INSUFFICIENT_DATA","Trigger":{"MetricName":"IExecutionFailureHook","Namespace":"sim-app_427809481713_us-east-1","StatisticType":"Statistic","Statistic":"SUM","Unit":"None","Dimensions":[{"value":"routing_table.receive.hook","name":"MetricGroupId"},{"value":"metric_emitting_node","name":"RouteID"}],"Period":300,"EvaluationPeriods":1,"ComparisonOperator":"GreaterThanOrEqualToThreshold","Threshold":1.0,"TreatMissingData":"- TreatMissingData:                    missing","EvaluateLowSampleCountPercentile":""}}',
#                          'Timestamp': '2021-11-10T02:45:55.681Z', 'SignatureVersion': '1',
#                          'Signature': 'gokyYzsZJSOhlIb8dc+tWfB2eHAHr7/pydp6qWhhCl36aKJvbVxz2G/XnySu/RgnYJyC7gmonO27j7ystjMr7GRncYFmWrJ7PneuVl/NLUyexvjdQWxOekyzgwqVCLhi4qcM/wGqHEBrgNHTccTFVn4MeJDHYfygHCpq3038t6oUIS8tEAD0o+LIIYBjLzawBgg7f9IKuzAAb6yJICCrgd6Fx3GutKYpsnz+QkykqL2oqeLvL67hdWfki3qf/K5H/y2caMmx0yC1PJYg2+59pCE6FEa65C8oILclJ3siKcBsto/Mo+gUV8fUHw6yMriaPypeFWIKncGGIG5rXwUkrw==',
#                          'SigningCertUrl': 'https://sns.us-east-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem',
#                          'UnsubscribeUrl': 'https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:427809481713:if-sim-app-AWSCloudWatchDiagnostics:4761dba2-cacd-42d4-8f9a-03de0745188d',
#                          'MessageAttributes': {}
#                    }
#                 }
#    ]
#    }
#)
#
#
## EMULATE Alarm state-change transition for alarm 'test_node_monitor'
#app.process(
#    {'Records': [
#            {
#                'EventSource': 'aws:sns',
#                'EventVersion': '1.0',
#                'EventSubscriptionArn': 'arn:aws:sns:us-east-1:427809481713:if-sim-app-AWSCloudWatchDiagnostics:4761dba2-cacd-42d4-8f9a-03de0745188d',
#                'Sns': {
#                    'Type': 'Notification',
#                    'MessageId': '12c36222-e1fe-56f5-9540-3827d55d7732',
#                    'TopicArn': 'arn:aws:sns:us-east-1:427809481713:if-sim-app-AWSCloudWatchDiagnostics',
#                    'Subject': 'ALARM: "if-sim-app_427809481713_us-east-1-test_node_monitor" in US East (N. Virginia)',
#                    'Message': '{"AlarmName":"if-sim-app_427809481713_us-east-1-test_node_monitor","AlarmDescription":"","AWSAccountId":"427809481713","NewStateValue":"ALARM","NewStateReason":"arn:aws:cloudwatch:us-east-1:427809481713:alarm:if-sim-app_427809481713_us-east-1-metric_emitting_node_failure transitioned to ALARM at Wednesday 10 November, 2021 02:45:55 UTC","StateChangeTime":"2021-11-10T02:45:55.640+0000","Region":"US East (N. Virginia)","AlarmArn":"arn:aws:cloudwatch:us-east-1:427809481713:alarm:if-sim-app_427809481713_us-east-1-test_node_monitor","OldStateValue":"OK","AlarmRule":"ALARM(arn:aws:cloudwatch:us-east-1:427809481713:alarm:if-sim-app_427809481713_us-east-1-metric_emitting_node_failure) OR ALARM(arn:aws:cloudwatch:us-east-1:427809481713:alarm:if-sim-app_427809481713_us-east-1-metric_emitting_long_execution)","TriggeringChildren":[{"Arn":"arn:aws:cloudwatch:us-east-1:427809481713:alarm:if-sim-app_427809481713_us-east-1-metric_emitting_node_failure","State":{"Value":"ALARM","Timestamp":"2021-11-10T02:45:55.640+0000"}}]}',
#                    'Timestamp': '2021-11-10T02:45:55.703Z',
#                    'SignatureVersion': '1',
#                    'Signature': 'VfDdFteYpXtbXstajjnDU94tQbd3gFsZ6xYZBNnFJoeXBQ6j4PiGrxif8T1CJPF9/gOik1ANtR/vnc69gevO94x95iRrKiBuemd4h7POeAirN++9upO5YpLQItx6zBqL/5YL0rbMoAc47pBsliYj63/hIYB07umokKAgk2RNbF46kqbphvgrMLvj8eLL0gGwOSIh8PnbvSmSGc8NPIsUCmoJYYnClPVtiFQLpn9dNogTDg86hrWQz/3Lkty9hsOgMIEbrHrR2/KwkXOcSMXpSBe3IFyPySykYQcI9kI/YWZRiYNDGJi6s7fhBRfC2PBsR37/zL0UQGqqd3jGgVZhFA==',
#                    'SigningCertUrl': 'https://sns.us-east-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem',
#                    'UnsubscribeUrl': 'https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:427809481713:if-sim-app-AWSCloudWatchDiagnostics:4761dba2-cacd-42d4-8f9a-03de0745188d',
#                    'MessageAttributes': {}
#                }
#            }
#    ]
#    }
#)

app.pause()


