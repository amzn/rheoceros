# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.compute_targets.email import EMAIL

flow.init_basic_logging()

app_name = "sim-app"
# automatically reads default credentials
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
app = AWSApplication(app_name, "us-east-1")
dedupe_string_for_system_failure = f"{app_name}-system-failure"

# ORCHESTRATION METRICS
orchestration_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.ORCHESTRATION)

processor_metrics_map = orchestration_metrics_map[ProcessingUnit]
routing_metrics_map = orchestration_metrics_map[RoutingTable]

## Dump system metrics and see their alias' and sub-dimensions!
#for metric in processor_metrics_map.values():
#    # dumps metric group ID/alias -> specific MetricNames and other details
#    print(metric.describe())
#
#for metric in routing_metrics_map.values():
#    # dumps metric group ID/alias -> specific MetricNames and other details
#    print(metric.describe())

processor_error_signal = processor_metrics_map["processor.event.error.type"]

processor_alarm = app.create_alarm(id="processor_error_alarm",
                                   target_metric_or_expression="(m1 > 0 OR m2 > 0)",
                                   metrics={
                                       "m1": processor_error_signal['NameError'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                                       "m2": processor_error_signal['Exception'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)]
                                   },
                                   number_of_evaluation_periods=1,
                                   number_of_datapoint_periods=1,
                                   comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                   # since we are using logical/conditional operator in metric math, the output series only contains 1s or 0s.
                                   # so threshold will be 1 to detect.
                                   threshold=1
                                   )


# Capture most common orchestration issues (due to DDB, etc) when RheocerOS cannot load or persist the state of a node
routing_persistence_failure_signal = routing_metrics_map["routing_table.receive"]


routing_persistence_alarm = app.create_alarm(id="routing_persistence_alarm",
                                   target_metric_or_expression="(m1 > 0 OR m2 > 0)",
                                   metrics={
                                       "m1": routing_persistence_failure_signal['RouteLoadError'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)],
                                       "m2": routing_persistence_failure_signal['RouteSaveError'][MetricStatistic.SUM][MetricPeriod.MINUTES(5)]
                                   },
                                   number_of_evaluation_periods=3,
                                   number_of_datapoint_periods=2,
                                   comparison_operator=AlarmComparisonOperator.GreaterThanOrEqualToThreshold,
                                   threshold=1
                                   )

orchestration_monitor = app.create_composite_alarm(id="orchestration_monitor",
                                                   alarm_rule=processor_alarm | routing_persistence_alarm)

email_obj = EMAIL(sender="if-test-list@amazon.com",  # sender from the same account (see './doc/EMAIL_ACTION_SETUP_AND_INFO.md')
                  recipient_list=["if-test@amazon.com"])
processor_failure_reactor = app.create_data(id="orchestration_failure_reactor",
                                            inputs=[orchestration_monitor[AlarmState.ALARM.value]],
                                            compute_targets=[
                                                email_obj.action(subject=f"RheocerOS orchestration failures detected in application {app_name!r}!",
                                                                 body="Either Processor event-handling (Lambda, etc) or Routing (DynanoDB) persistence issues have detected in the system."
                                                                      " Monitor the system closely and do manual executions on impacted runs (if any).")
                                            ])

app.activate()

app.pause()
