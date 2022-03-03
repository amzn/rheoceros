# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import intelliflow.api_ext as flow
from intelliflow.api_ext import *
import logging

from intelliflow.core.application.context.node.base import DataNode
from intelliflow.core.signal_processing.dimension_constructs import StringVariant

region = "us-east-1"
stage = "dev-yunusko"
app_name = f"eureka-{stage}"

flow.init_basic_logging()
log = logging.getLogger(app_name)

app = AWSApplication(app_name, region)
app.authorize_downstream("andes-ant-app", '427809481713', 'us-east-1')
app.authorize_downstream("andes-ant-app", '842027028048', 'us-east-1')

eureka_offline_training_data = app.add_external_data(
                                            "eureka_training_data",
                                            S3("427809481713",
                                               "dex-ml-eureka-model-training-data",
                                               "cradle_eureka_p3/v8_00/training-data",
                                               StringVariant('NA', 'region'),
                                               AnyDate('my_day_my_way', {'format': '%Y-%m-%d'})))

eureka_offline_all_data = app.marshal_external_data(
    S3Dataset("427809481713", "dex-ml-eureka-model-training-data", "cradle_eureka_p3/v8_00/all-data-prod", "partition_day={}", dataset_format=DataFormat.CSV)
    , "eureka_training_all_data"
    , {
        'day': {
            'type': DimensionType.DATETIME
        }
    }
    , {
        "*": {
            'format': '%Y-%m-%d',
            'timezone': 'PST',
        },
    },
    SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)


default_selection_features = app.create_data('eureka_default_selection_data_over_two_days', # data_id
                                      {
                                          "offline_data": eureka_offline_all_data[:-2],
                                          "offline_training_data": eureka_offline_training_data["NA"][:-2]
                                      },
                                      [
                                          (eureka_offline_all_data('day'), lambda dim: dim, eureka_offline_training_data('my_day_my_way'))
                                      ],  # dim link matrix for inputs
                                      {
                                          'reg': {
                                              'type': DimensionType.STRING,
                                              'day': {
                                                  'type': DimensionType.DATETIME,
                                                  'format': '%d-%m-%Y',
                                              }
                                          }
                                      },
                                      [
                                         ('reg', lambda dim: dim, eureka_offline_training_data('region')),
                                         ('day', lambda dim: dim.strftime('%d-%m-%Y'), eureka_offline_all_data('day'))
                                      ],
                                      [ # when inputs are ready, trigger the following
                                          BatchCompute(
                                              "output = offline_data.subtract(offline_training_data)",
                                              Lang.PYTHON,
                                              ABI.GLUE_EMBEDDED,
                                              # extra_params
                                              # interpreted by the underlying Compute (Glue)
                                              # and also passed in as job run args (irrespective of compute).
                                              spark_ver="2.4",
                                              # see https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run
                                              MaxCapacity=5,
                                              Timeout=30, # in minutes
                                              GlueVersion="1.0"
                                          )
                                      ],
                                      auto_backfilling_enabled=False
                                      )

default_completed_selection_features = app.create_data('eureka_default_completed_features', # data_id
                                             [
                                                default_selection_features['*']['*']
                                             ],
                                             [],  # dim link matrix for inputs
                                             {
                                                 'reg': {
                                                     'type': DimensionType.STRING,
                                                     'day': {
                                                         'type': DimensionType.DATETIME,
                                                         'format': '%d-%m-%Y',
                                                     }
                                                 }
                                             },
                                             [
                                                 ('reg', lambda dim: dim, default_selection_features('reg')),
                                                 ('day', lambda dim: dim, default_selection_features('day'))
                                             ],
                                             [ # when inputs are ready, trigger the following
                                                 BatchCompute(
                                                     python_module('example_module.test_glue_etl_script'),
                                                     Lang.PYTHON,
                                                     ABI.GLUE_EMBEDDED,
                                                     spark_ver="2.4",
                                                     MaxCapacity=5,
                                                     Timeout=30, # in minutes
                                                     GlueVersion="1.0"
                                                 )
                                             ],
                                             auto_backfilling_enabled=False
                                             )


all_nodes = app.list()
eureka_nodes = app.query([DataNode.QueryVisitor('eureka_')])

app.activate()

# INJECT EVENTS / trigger for debugging
# (for more convenient on-demand executions please check Application::execute API used in other examples)
'''
app.process(eureka_offline_all_data['2020-03-18'])

#app.process(eureka_offline_training_data['EU']['2020-03-17'])

app.process(eureka_offline_training_data['NA']['2020-03-18'])

app.process({
    "Records":[
        {
            "eventVersion":"2.0",
            "eventSource":"aws:s3",
            "awsRegion":"us-east-1",
            "eventTime":"1970-01-01T00:00:00.000Z",
            "eventName":"ObjectCreated:Put",
            "s3":{
                "s3SchemaVersion":"1.0",
                "bucket":{
                    "name":"dex-ml-eureka-model-training-data",
                    "arn":"arn:aws:s3:::dex-ml-eureka-model-training-data"
                },
                "object":{
                    "key":"cradle_eureka_p3/v8_00/all-data/partition_day=2020-03-18/_SUCCESS",
                }
            }
        }
    ]
})

# test with raw SNS (carrying S3 records)
app.process(
    {'Records': [{'EventSource': 'aws:sns', 'EventVersion': '1.0', 'EventSubscriptionArn': 'arn:aws:sns:us-east-1:427809481713:if-eureka-dev-yunusko-AWSS3StorageBasic:9437d594-5d6a-45ac-9911-2a46eae5547c', 'Sns': {'Type': 'Notification', 'MessageId': '6da9fbb8-b682-5626-8896-ac5d6894e39d', 'TopicArn': 'arn:aws:sns:us-east-1:427809481713:if-eureka-dev-yunusko-AWSS3StorageBasic', 'Subject': 'Amazon S3 Notification', 'Message': '{"Records":[{"eventVersion":"2.1","eventSource":"aws:s3","awsRegion":"us-east-1","eventTime":"2020-09-23T23:35:44.900Z","eventName":"ObjectCreated:Put","userIdentity":{"principalId":"AWS:AROAJBRZ52PL2YCJCF7KK:yunusko"},"requestParameters":{"sourceIPAddress":"72.21.198.67"},"responseElements":{"x-amz-request-id":"12F3E1F68EB4ED17","x-amz-id-2":"Qt/fXtbQXf662BsqXkt8srjS1uWPjdsvcBNHfEKMQtPXiRj5eeMmyQelwdNhghMfahRGH9KnYRlSaS4nJrd3xz/fbM4RkAJv"},"s3":{"s3SchemaVersion":"1.0","configurationId":"MTk5ZjJhMGItZDE4YS00NTVjLTg2ZDYtYzgwODc0OWIyZDMy","bucket":{"name":"if-eureka-dev-yunusko-427809481713-us-east-1","ownerIdentity":{"principalId":"AZB59FR3ENND3"},"arn":"arn:aws:s3:::if-eureka-dev-yunusko-427809481713-us-east-1"},"object":{"key":"internal_data/test/part-00000-51a50763-0187-45ae-8a90-5fb3351f87db-c000.csv","size":8544683,"eTag":"88454b9f8de7c7e43d84b11f79e5e573","sequencer":"005F6BDBD4129CF6FF"}}}]}', 'Timestamp': '2020-09-23T23:35:50.595Z', 'SignatureVersion': '1', 'Signature': 'fvcq243uqL8MlJMl+dSzZhcvFuzgR59Yv6Sd9SPdFBieOvqIjpfGbvD3Ho8snUsFOxTncDYtM0ChWmy9WrJLfs5QW37x88eOoLTey14cW2FZCGoC11uDsmq6GmJycCJ2kKs6nD5qYtnMSFcVkHGZJ6rAmgT65IBQNnYio6NFtOPW5q8ZhQjjDkPv8CZ7KB+79juqYLaLVPbZSXxixfmec6UMiWS5KXLE17nISHIvmGv/fBY9looslytLjpHUE/sQwO8CzyzI1neX8TsSioUzerXKi3w0PqnkW9cRdUbj2BLvsTo1gO0mvyggay3goQ1gL8atDSz/PjTnYWH0eYOAQA==', 'SigningCertUrl': 'https://sns.us-east-1.amazonaws.com/SimpleNotificationService-a86cb10b4e1f29c941702d737128f7b6.pem', 'UnsubscribeUrl': 'https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:427809481713:if-eureka-dev-yunusko-AWSS3StorageBasic:9437d594-5d6a-45ac-9911-2a46eae5547c', 'MessageAttributes': {}}}]}
)
'''

# Miscellaneous Debugging /Analysis code that pulls remote (cloud) state of routes created above
'''
# Historical compute records 
inactive_records = app.get_inactive_compute_records('eureka_default_selection_data_over_two_days')
for inactive_record in inactive_records:
    for exec_details in get_execution_details(inactive_record):
        print(exec_details.details)  # error message (if failed), compute details, etc

# get application's routing table (runtime version of app topology)
#route_index = app.get_routing_table()

# all of the nodes as <Route> objects
#active_routes = app.get_active_routes()

# force orchestration to 'tick' (do next-cycle) which scans all of the routes from route_index, checks executions,
# pending events, etc
#app.update_active_routes_status()

# retrieve the runtime state of an active route/node
#output_data = app['eureka_default_selection_data_over_two_days']
#active_route_for_output = app.get_active_route(output_data)

# get ongoing/active <ComputeRecord>s of the route/node
#active_records = app.get_active_compute_records(output_data)

# get completed or failed past/historical <ComputeRecord>s of the route/node
#inactive_records = app.get_inactive_compute_records('eureka_default_selection_data_over_two_days')
#for inactive_record in inactive_records:
#    print(inactive_record)
#
## force platform to refresh the routing status
#app.update_active_routes_status()
#
'''