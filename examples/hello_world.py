# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
# Refer './doc/EMAIL_ACTION_SETUP_AND_INFO.md' before proceeding
from intelliflow.core.platform.compute_targets.email import EMAIL

from intelliflow.core.platform.constructs import ConstructParamsDict
from intelliflow.utils.test.inlined_compute import NOOPCompute

flow.init_basic_logging()

# automatically reads default credentials (and creates this app in that AWS account)
# default credentials should belong to an entity that can either:
#  - do everything (admin, use it for quick prototyping!)
#  - can create/delete roles (dev role of this app) that would have this app-name and IntelliFlow in it
#  - can get/assume its dev-role (won't help if this is the first run)
app = AWSApplication("hello-world", "us-east-1")



def example_inline_compute_code(input_map, output, params):
    """ Function to be called within RheocerOS core.
    Below parameters and also inputs as local variables in both
        - alias form ('order_completed_count_amt')
        - indexed form  (input0, input1, ...)
    will be available within the context.

    input_map: [alias -> input signal]
    output: output signal
    params: contains platform parameters (such as 'AWS_BOTO_SESSION' if an AWS based configuration is being used)

    Ex:
    s3 = params['AWS_BOTO_SESSION'].resource('s3')
    """
    print("Hello from AWS!")


class MyLambdaReactor(IInlinedCompute):

    def __call__(self, input_map: Dict[str, Signal], materialized_output: Signal, params: ConstructParamsDict) -> Any:
        from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
        pass


node_1_1 = app.create_data(id=f"Node_1_1",
                           compute_targets=[
                               InlinedCompute( example_inline_compute_code ),
                               InlinedCompute( MyLambdaReactor() ),
                               NOOPCompute
                           ]
                           )

email_address = "your_own_email_address"  # e.g "if-test-list@amazon.com"
email_obj = EMAIL(sender="if-test-list@amazon.com",  # sender from the same account
                  recipient_list=[email_address])

node_1_2 = app.create_data(id=f"Node_1_2",
                           inputs=[node_1_1],
                           compute_targets=[email_obj.action(subject="IF Test Email", body="First Node Computation as Email")],
                           execution_hook=RouteExecutionHook(on_exec_begin=email_obj.action(subject="Hello from execution hook of Node_1_2", body="Node_1_2 exec started!"))
                           )


app.execute(node_1_1)


# check the execution on node_1_2
# test event propagation in AWS
path, comp_records = app.poll(node_1_2)
assert path


