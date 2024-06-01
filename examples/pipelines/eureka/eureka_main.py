# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Sample RheocerOS platform packed into one file for demonstration purposes.

Rather than packed into a single file, it can very well reside in a separate package
that depends on RheocerOS module and more importantly separate project structure
spanning its own sub-modules that relies on same "app" object.

That is why this is named with a 'main' suffix to indicate the fact that this type
of files can be assumed to be "main" entry points for an RheocerOS application.

Main entry point can initiate the app object and pass it into platform specific
sub-modules and entities to act on it using RheocerOS APIs.

Recommendation for Activation (PyCharm):

- Check "Run with Python Console" in the app config of this file.
    (Right click and then choose "Edit 'eureka_main'..." from the context menu.)
- Keep interacting with the "flow" and "app" after the activation:
    - query app node/provisioning status, other API calls.
"""
import intelliflow.api as flow
from intelliflow.api import *
import logging

region = "us-east-1"
stage = "beta"
app_name = f"eureka-{stage}"

# or (3) provide credentials in which IF tries to get STS (assume role) credentials for IF dev Role
#       (3.1) if marked as admin, AWSConfiguration gives the option to auto-update the underlying roles/policies.
access_key_id = ""
secret_access_key = ""

# or (4) use system default credentials, should be able to assume IF_DEV_ROLE
# TODO discuss with the team if IF_ROLE should be tied with app
#      (if so, pass 'app_name' to AWSConfiguration builder)

flow.init_basic_logging("./eureka_main_logs")
log = logging.getLogger(app_name)

supported_aws_constructs = AWSConfiguration.get_supported_constructs_map()

app = flow.create_or_update_application(app_name,
                                        HostPlatform(AWSConfiguration.builder()
                                                                     #.with_dev_access_pair(access_key_id, secret_access_key)
                                                                     .with_default_credentials(as_admin=True)
                                                                     .with_region(region)
                                                                     .build()))

eureka_offline_data = app.marshal_external_data(
                S3Dataset("<BUCKET_ACC_ID>", "<TRAINING_DATA_BUCKET>", "eureka_p3/v8_00/training-data", "{}", "{}", dataset_format=DataFormat.CSV)
                , "eureka_training_features"
                # DEFINE Data Signal Spec (Flow <Type> / Dimensions)
                , {
                    'region': {
                        'type': DimensionType.STRING,
                        'day': {
                            'type': DimensionType.DATETIME
                        }
                    }
                }
                , {
                    "NA": {
                        "*": {
                            'format': '%Y-%m-%d',
                            'timezone': 'PST',
                        }
                    },
                    "EU": {
                        "*": {
                            'format': '%Y-%m-%d',
                            'timezone': 'UTC',
                        }
                    }
                },
                SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
                )

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

training_features_1 = app.create_data('order_completed_count_amt', # data_id
                                 # eureka_offline_data.filter({'*': {'*': {}}}, 'offline_data')
                                 # {alias -> filtered input map}
                                 {
                                     "offline_data": eureka_offline_data["*"]["*"],
                                 },
                                 [],  # dim link matrix for inputs
                                 {
                                        'region': {
                                            'type': DimensionType.STRING,
                                            'day': {
                                                'type': DimensionType.DATETIME,
                                                'format': '%Y-%m-%d',
                                            }
                                        }
                                 },
                                 [
                                     ('region', lambda dim: dim, eureka_offline_data('region')),
                                     ('day', lambda dim: dim, eureka_offline_data('day'))
                                 ],
                                 [ # when inputs are ready, trigger the following
                                      InlinedCompute(
                                          #lambda inputs, output: print("Hello World from AWS")
                                          example_inline_compute_code
                                      ),
                                      BatchCompute(
                                          "output = offline_data.withColumn('new_col', lit('hello'))",
                                          Lang.PYTHON,
                                          ABI.GLUE_EMBEDDED,
                                          # extra_params
                                          # interpreted by the underlying Compute (Glue)
                                          # and also passed in as job run args (irrespective of compute).
                                          spark_ver="2.4",
                                          # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run
                                          MaxCapacity=5,
                                          Timeout=30, # in minutes,
                                          GlueVersion="1.0"
                                      )
                                 ],
                                 auto_backfilling_enabled=False
                                 )

app.activate()

all_internal_data = app.list()

'''

all_external_data = app.list(flow.ExternalDataNode)
all_s3_data = app.list(flow.S3DataNode)

query_result = app.query()

#app.query(MarshalerNode.QueryVisitor())

# stash
#app.save_dev_state()

# pop probably in another environment
#app.load_dev_state()


# attach so that current dev context syncs up with active (running) context

#app.attach()

'''
