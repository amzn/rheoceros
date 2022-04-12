# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
from intelliflow.core.platform.compute_targets.slack import Slack

flow.init_basic_logging()

app = AWSApplication("your-app-name", "us-east-1")


def example_inline_compute_failure(input_map, output, params):
    raise ValueError("something went wrong")


def example_inline_compute_code(input_map, output, params):
    print("Hello from AWS!")


# This implementation relies on Slack workflow to send notifications as a DM or to a specific channel.
# Refer to ./doc/SLACK_NOTIFICATION_SETUP.md to create working slack workflows.
# Once workflow is created, copy & paste url here as recipient
# The notification will contain your simple message and node info

slack_notification_recipient_1 = "a legit slack workflow url"
slack_notification_recipient_2 = "another legit slack workflow url"
slack_notification_recipient_bad = "bad url"

slack_obj = Slack(recipient_list=[slack_notification_recipient_1])

node_1_1 = app.create_data(id=f"Node_1_1",
                           compute_targets=[
                               InlinedCompute(example_inline_compute_code)
                           ],
                           execution_hook=RouteExecutionHook(
                               on_exec_begin=slack_obj.action(message="node_1_1 computation started")
                           )
                           )

node_1_2 = app.create_data(id=f"Node_1_2",
                           inputs=[node_1_1],
                           compute_targets=[
                               slack_obj.action(message=" Node1-1 execution finished.")
                           ],
                           execution_hook=RouteExecutionHook(
                               on_exec_begin=slack_obj.action(message="Node1-2 computation started."),
                               on_compute_success=slack_obj.action(message=" Node1-2 Computation succeed"),
                           ))

# you can overwrite recipient list in action
node_1_3 = app.create_data(id=f"Node_1_3",
                           inputs=[node_1_1],
                           compute_targets=[
                               InlinedCompute(
                                   example_inline_compute_failure
                               )
                           ],
                           execution_hook=RouteExecutionHook(
                               on_failure=slack_obj.action(
                                   # you can overwrite recipient list in action
                                   recipient_list=[slack_notification_recipient_1, slack_notification_recipient_2],
                                   message="Node1-3 Computation failed"))
                           )

app.activate()
app.execute(node_1_1)
