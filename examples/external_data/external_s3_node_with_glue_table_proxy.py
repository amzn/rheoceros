# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import time

import intelliflow.api_ext as flow
from intelliflow.api_ext import *

from intelliflow.core.platform.constructs import ConstructParamsDict

logger = flow.init_basic_logging()

app = AWSApplication("i-fanout", "us-east-1")
app.set_security_conf(Storage, ConstructSecurityConf(persisting=ConstructPersistenceSecurityDef(
                                                         ConstructEncryption(EncryptionKeyAllocationLevel.HIGH,
                                                                             key_rotation_cycle_in_days=365,
                                                                             is_hard_rotation=False,
                                                                             reencrypt_old_data_during_hard_rotation=False,
                                                                             trust_access_from_same_root=True)),
                                                     passing=None,
                                                     processing=None))

encryption_key = "arn:aws:kms:us-east-1:800261124827:key/5be55530-bb8e-4e95-8185-6e1afab0de54"
search_tommy_data = app.marshal_external_data(
    external_data_desc= S3Dataset("800261124827", "a9-sa-data-tommy-datasets-prod", "tommy-asin-parquet-hourly", "{}", "{}", "{}",
                                  dataset_format=DataFormat.PARQUET, encryption_key=encryption_key)
                            .link(
                                  # Link the PROXY !!!
                                  # events coming from this proxy/link will yield valid Signals/triggers in the system
                                  GlueTable(database="searchdata", table_name="tommy_hourly")
                                 )
    , id="tommy_external"
    , dimension_spec={
        'org': {
            'type': DimensionType.STRING,
            'format': lambda org: org.lower(), # make sure that this dimension exposes itself as 'lower case'
            'insensitive': True,  # incoming events might have UPPER case strings
            'day': {
                'type': DimensionType.LONG,
                'hour': {
                    'type': DimensionType.LONG,
                }
            }
        }
    }
    , dimension_filter={
        "SE": {
            "*": {
                "*": {

                }
            }
        }
    },
    # Extra trigger source in case search team decides to add hadoop completion file with or without Glue partition
    # updates. RheocerOS idempotency will take care repetitive messages and in case of long durations in between
    # those messages, the worst case scenario would be a redundant execution.
    protocol=SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"})
)

class MySearchDataReactorLambda(IInlinedCompute):

    def __call__(self, input_map: Dict[str, Signal], materialized_output: Signal, params: ConstructParamsDict) -> Any:
        from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams

        print(f"Hello from AWS Lambda account_id {params[AWSCommonParams.ACCOUNT_ID]}, region {params[AWSCommonParams.REGION]}")
        tommy_external = input_map['tommy_external']
        input_path = tommy_external.get_materialized_resource_paths()[0]
        print(f"A new S3 event was received from {tommy_external.resource_access_spec.bucket!r}")
        print(f"A new search tommy data partition update has been received! Partition: {input_path}")

        # TODO load into Pandas DF directly from intput_path
        s3 = params[AWSCommonParams.BOTO_SESSION].client("s3")

        from urllib.parse import urlparse
        s3_url = urlparse(input_path, allow_fragments=False)
        bucket = s3_url.netloc  # input
        key = s3_url.path.lstrip('/') + "/"
        response = s3.list_objects(Bucket=bucket, Prefix=key, MaxKeys=1, Delimiter="/")

        #for partition_part in response.get('CommonPrefixes'):
        #    # TODO Analyze one partition, check schema, etc
        #    print(f"Analyzing s3://{bucket}/{partition_part.get('Prefix')} ...")

        output_path = materialized_output.get_materialized_resource_paths()[0]
        if materialized_output.resource_access_spec.source == SignalSourceType.S3:
            # save metadata to output
            s3_url = urlparse(output_path, allow_fragments=False)
            data_file = s3_url.path.lstrip('/') + "/part-0000.csv"
            s3.put_object(Body=b"foo|bar\n1|2'", Bucket=materialized_output.resource_access_spec.bucket, Key=data_file)
            success_file = s3_url.path.lstrip('/') + "/" + InternalDataNode.DEFAULT_DATA_COMPLETION_PROTOCOL.args['file']
            s3.put_object(Body=b"", Bucket=materialized_output.resource_access_spec.bucket, Key=success_file)


search_data_reactor_node = app.create_data(id="SEARCH_DATA_REACTOR",
                                           inputs=[search_tommy_data],
                                           compute_targets=[
                                               InlinedCompute(MySearchDataReactorLambda())
                                           ])

run_when_search_data_verified = app.create_data(id="WHEN_SEARCH_DATA_VERIFIED",
                                                inputs=[
                                                        #search_data_reactor_node,
                                                        search_tommy_data],
                                                compute_targets = [
                                                    BatchCompute("output = tommy_external.limit(10).select(*('asin', 'session', 'search_type'))",
                                                                 WorkerType=GlueWorkerType.G_1X.value,
                                                                 NumberOfWorkers=20,
                                                                 GlueVersion="2.0")
                                                ],
                                                # delete pending nodes after 7 days
                                                pending_node_expiration_ttl_in_secs=7 * 24 * 60 * 60)

run_when_search_data_verified_SCALA = app.create_data(id="WHEN_SEARCH_DATA_VERIFIED_SCALA",
                                                inputs=[
                                                        #search_data_reactor_node,
                                                        search_tommy_data],
                                                compute_targets = [
                                                    BatchCompute(
                                                                scala_script('tommy_external.limit(10).select(col("asin"), col("session"), col("search_type"))'),
                                                                lang = Lang.SCALA,
                                                                WorkerType=GlueWorkerType.G_1X.value,
                                                                NumberOfWorkers=20,
                                                                GlueVersion="2.0")
                                                ],
                                                # delete pending nodes after 7 days
                                                pending_node_expiration_ttl_in_secs=7 * 24 * 60 * 60)

run_when_search_data_verified_PRESTO = app.create_data(id="WHEN_SEARCH_DATA_VERIFIED_PRESTO",
                                                      inputs=[
                                                              #search_data_reactor_node,
                                                              search_tommy_data],
                                                      compute_targets = [
                                                          PrestoSQL(
                                                              "select asin, session, search_type from tommy_external Limit 10",
                                                              )
                                                      ])

app.activate()

# Emulate Glue Table partition change event.
# Synchronously process (without async remote call to Processor [Lambda])
app.process(
{
    "version": "0",
    "id": "3a1926d0-2de1-fb1b-9741-cad729bb7cb4",
    "detail-type": "Glue Data Catalog Table State Change",
    "source": "aws.glue",
    "account": "427809481713",
    "time": "2021-09-15T18:57:01Z",
    "region": "us-east-1",
    "resources": ["arn:aws:glue:us-east-1:427809481713:table/searchdata/tommy_hourly"],
    "detail": {
        "databaseName": "searchdata",
        "changedPartitions": ["[SE, 20210915, 2021091500]"],
        "typeOfChange": "BatchCreatePartition",
        "tableName": "tommy_hourly",
    }
}
)

path, compute_records = app.poll(search_data_reactor_node['se']['20210915']['2021091500'])
assert path
time.sleep(5)

path, _ = app.poll(run_when_search_data_verified['se']['20210915']['2021091500'])
assert path
logger.critical(f"A new partition for {run_when_search_data_verified.route_id!r} has been created in {path!r}.")

path, _ = app.poll(run_when_search_data_verified_SCALA['se']['20210915']['2021091500'])
assert path
logger.critical(f"A new partition for {run_when_search_data_verified_SCALA.route_id!r} has been created in {path!r}.")

path, _ = app.poll(run_when_search_data_verified_PRESTO['se']['20210915']['2021091500'])
assert path
logger.critical(f"A new partition for {run_when_search_data_verified_PRESTO.route_id!r} has been created in {path!r}.")

app.pause()

