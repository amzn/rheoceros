# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Template for standalone EMR job script that would embed user Spark code.
We also name this ABI as "DEFAULT" ABI and represented by GLUE_EMBEDDED within RheocerOS.

User code gets set as "client" code.
Then EMR based constructs are supposed to get the final state of this script and send it
to EMR during job creation.
"""

import base64
from typing import ClassVar, Optional, Set

from intelliflow.core.platform.definitions.aws.emr.script.batch.common import (
    AWS_REGION,
    BOOTSTRAPPER_PLATFORM_KEY_PARAM,
    CLIENT_CODE_BUCKET,
    CLIENT_CODE_PARAM,
    EXECUTION_ID,
    IGNORED_BUNDLE_MODULES_PARAM,
    INPUT_MAP_PARAM,
    JOB_NAME_PARAM,
    OUTPUT_PARAM,
    USER_EXTRA_PARAMS_PARAM,
    WORKING_SET_OBJECT_PARAM,
)


class EmrDefaultABIPython:
    NONOVERRIDABLE_PARAMS: ClassVar[Set[str]] = {
        JOB_NAME_PARAM,
        INPUT_MAP_PARAM,
        CLIENT_CODE_PARAM,
        CLIENT_CODE_BUCKET,
        OUTPUT_PARAM,
        BOOTSTRAPPER_PLATFORM_KEY_PARAM,
        USER_EXTRA_PARAMS_PARAM,
        IGNORED_BUNDLE_MODULES_PARAM,
        AWS_REGION,
        WORKING_SET_OBJECT_PARAM,
    }

    def __init__(self, var_args: Optional[Set[str]] = None, prologue_code: Optional[str] = None, epilogue_code: Optional[str] = None):
        """
        Instantiate a different version of this script.

        Parameters
        ----------
        var_args: extra arguments on top of non-overridable EmrDefaultABI::NONOVERRIDABLE_PARAMs, they can be
        either app-developer provided or EMR construct impl specific. These are exposed to embedded client do as well.

        prologue_code: An EMR based construct can provide prologue code to support more enhanced ABIs or to evaluate
        some of the 'var_args' which are construct specific (neither user provided nor NONOVERRIDABLE).

        epilogue_code: An EMR based construct can provide epilogue code to be executed at the end of the script
        to take a set of final actions based on Contruct impl and some of the 'var_args'. Ex: Any epilogue functionality
        can be exposed to app-developers as a high-level feature (such as dumping data to different medium that
        authorized IF_EXEC_ROLE already).
        """
        self._args = self.NONOVERRIDABLE_PARAMS.update(var_args) if var_args else self.NONOVERRIDABLE_PARAMS
        self._prologue_code = prologue_code if prologue_code else ""
        self._epilogue_code = epilogue_code if epilogue_code else ""

    def generate_emr_script(self) -> str:
        return (
            """
import sys
import os
import datetime
import json
import logging
import argparse
import subprocess
import base64 as b64

from pyspark import SparkConf, SparkContext
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.utils import *

def install_if_bundle(working_set_object: str, ignored_bundle_modules):
    subprocess.run(f"rm -rf /tmp/tmp-intelliflow/".split())
    subprocess.run(f"aws s3 cp {working_set_object} /tmp/".split())
    subprocess.run(f"unzip -qq -o /tmp/{working_set_object.split('/')[-1]} -d /tmp/tmp-intelliflow/".split())
    sys.path.insert(0, '/tmp/tmp-intelliflow/')
    
    import importlib
    for ignored_module in ignored_bundle_modules:
        subprocess.run(f"rm -rf /tmp/tmp-intelliflow/{ignored_module}".split())
        if ignored_module in sys.modules:
            importlib.reload(sys.modules[ignored_module])

def run_emr_bootstrap(job_name,
                      code_bucket,
                      client_code_path,
                      input_map_path,
                      output_map_path,
                      bootstrapper_path,
                      aws_region,
                      prologue_code,
                      epilogue_code,
                      working_set_object,
                      args):

    '''
    [Insert bootstrap description here.]

    :param job_name: The name of the IntelliFlow application name.
    :param code_bucket: The URI of the S3 bucket that holds the application script and input and output maps,
                        such as 's3://if-application-123456789012-us-east-1/'.
    :param client_code_path: The path from the main code bucket to the application script,
                             such as '.../slot_code.py'.
    :param input_map_path: The path from the main code bucket to the application's input map,
                           such as '.../input_map.json'.
    :param output_map_path: The path from the main code bucket to the application's output map,
                            such as '.../output_map.json'.
    :param bootstrapper_path: The path to IntelliFlow RuntimePlatform.
    '''
    import ast

    import boto3

    s3 = boto3.resource('s3', region_name=aws_region)
    glue = boto3.client('glue', region_name=aws_region)

    code_obj = s3.Object(code_bucket, client_code_path)
    client_code = code_obj.get()['Body'].read().decode('utf-8')

    input_map_obj = s3.Object(code_bucket, input_map_path)
    input_map_str = input_map_obj.get()['Body'].read().decode('utf-8')

    output_obj = s3.Object(code_bucket, output_map_path)
    output_str = output_obj.get()['Body'].read().decode('utf-8')

    conf = SparkConf().setAppName(job_name)
    conf.setIfMissing("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    sc = SparkContext.getOrCreate(conf=conf)
    sc.addPyFile(working_set_object)
    spark = SparkSession(sc)\\
            .builder.config(conf=conf)\\
            .enableHiveSupport().getOrCreate()

    from intelliflow.core.signal_processing import Signal
    from intelliflow.core.platform.development import RuntimePlatform
    from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
    from intelliflow.core.platform.definitions.aws.glue import catalog as glue_catalog
    from intelliflow.utils.spark import create_spark_schema

    bootstrapper_obj = s3.Object(code_bucket, bootstrapper_path)
    serialized_bootstrapper_str = bootstrapper_obj.get()['Body'].read().decode('utf-8')
    runtime_platform: RuntimePlatform = RuntimePlatform.deserialize(serialized_bootstrapper_str)
    runtime_platform.runtime_init(runtime_platform.batch_compute)

    logger = logging.getLogger(__name__)

    def create_view(name, sql_code):
        df = spark.sql(sql_code)
        df.registerTempTable(name)
        return df

    def create_query(database: str, table_name: str, partition_keys, all_partitions) -> str:
        pr_tb = '`{0}`.`{1}`'.format(database, table_name)
        partition_conditions = []
        for i, partition_key in enumerate(partition_keys):
            partition_values = [partition[i] for partition in all_partitions]
            condition = f'{partition_key} in ({",".join([repr(val) for val in partition_values])})'
            partition_conditions.append(condition)

        where_clause = f'{" AND ".join(partition_conditions)}' if partition_conditions else ''
        return f'''
                  SELECT * FROM {pr_tb}
                      {" WHERE " + where_clause if where_clause else ""}
                '''

    input_map = json.loads(input_map_str)

    def load_input_df(input, sc, aws_region):
        if input.get("data_type", "dataset") not in ["dataset", None]:
            return None
    
        if input['encryption_key']:
            # first set encryption (if defined)
            if input['resource_type'] == 'S3':
                sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3.cse.enabled", "true")
                sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3.cse.kms.keyId", input['encryption_key'])
                sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3.cse.kms.region", aws_region)
                sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3.cse.encryptionMaterialsProvider",
                                                  "com.amazon.ws.emr.hadoop.fs.cse.KMSEncryptionMaterialsProvider")
            else:
                raise NotImplementedError("Dont know how to use encryption_key for resource type {0}!".format(input['resource_type']))

        proxy = input.get('proxy', None)
        database = None
        table_name = None
        if proxy and proxy['resource_type'] == 'GLUE_TABLE':
            # read from the catalog
            # refer GlueTableSignalSourceAccessSpec
            database = proxy['attrs']['_database']
            table_name = proxy['attrs']['_table_name']
        elif input['resource_type'] == 'GLUE_TABLE':
            first_materialized_path = input['resource_materialized_paths'][0]
            gt_path_parts = first_materialized_path[len("glue_table://"):].split("/")
            database = gt_path_parts[0]
            table_name = gt_path_parts[1]

        input_df = None
        for i, resource_path in enumerate(input['resource_materialized_paths']):
            new_df = None
            if database and table_name:
                next_partitions = [input['partitions'][i]] if input['partition_keys'] else input['partitions']
                next_partition_values = [str(d) for d in next_partitions[0]] if next_partitions else []
                # TODO catch glue exception and map retryables to ComputeRetryableInternalError
                if not next_partitions or not (input['range_check_required'] or input['nearest_the_tip_in_range']) or glue_catalog.is_partition_present(glue, database, table_name, next_partition_values):
                    if input['resource_type'] == 'S3':
                        # keep the path as is if no partitions 
                        if next_partition_values:
                            # overwrite full path from catalog (the actual reason we've started reading from catalog in the first place)
                            # let spark read directly from the path (more optimized than all of the other options [including DynamicFrame with push_down_predicate])
                            resource_path = glue_catalog.get_location(glue, database, table_name, next_partition_values)
                            if not resource_path:
                                if not input['range_check_required']:
                                    continue
                                else:
                                    raise RuntimeError("{0} path does not exist! Dimensions: {1}"
                                                       "Either there is a problem with range_check mechanism or the partition has been deleted "
                                                       "after the execution has started.".format(resource_path, next_partition_values))
                    else:
                        # no point to use push_down_predicates via DynamicFrame's here either as S3 is the only supported storage for that
                        new_df = spark.sql(create_query(database, table_name, input['partition_keys'], next_partitions))
                elif not input['range_check_required']:
                    logger.error("{0} path does not exist (Dimensions: {1}). Since range check is not required. Continuing with next available path".format(resource_path, next_partition_values))
                    continue
                else:
                    raise RuntimeError("{0} path does not exist! "
                                       "Either there is a problem with range_check mechanism or the partition has been deleted "
                                       "after the execution has started.".format(resource_path))
            if not new_df:
                # append data sub folder if it is defined by the user
                data_folder = input.get("data_folder", None)
                if data_folder:
                    resource_path = resource_path.rstrip("/") + "/" + data_folder
                # native access via input materialized paths
                schema_def = input.get("data_schema_def", None)
                spark_schema_def = create_spark_schema(schema_def) if schema_def else None
                try:
                    if input['data_format'] == 'parquet':
                        if schema_def:
                            new_df = spark.read.schema(spark_schema_def).parquet(resource_path)
                        else:
                            new_df = spark.read.parquet(resource_path)
                    else:
                        if schema_def:
                            new_df = spark.read.load(resource_path, format=input['data_format'], sep=input['delimiter'], schema=spark_schema_def, header=input['data_header_exists'])
                        else:
                            new_df = spark.read.load(resource_path, format=input['data_format'], sep=input['delimiter'], inferSchema='true', header=input['data_header_exists'])
                # Path not found exception is considered Analysis Exception within Spark code. Please check the
                # link below for reference.
                # https://github.com/apache/spark/blob/1b609c7dcfc3a30aefff12a71aac5c1d6273b2c0/sql/catalyst/src/main/scala/org/apache/spark/sql/errors/QueryCompilationErrors.scala#L977
                except AnalysisException as e:
                    if not input['range_check_required']:
                        if str(e).find('Path does not exist') != -1:
                            logger.error("{0} path does not exist. Since range check is not required. Continuing with next available path".format(resource_path))
                            continue
                    raise e
            if input_df and len(input_df.columns) > 0: 
                if len(new_df.columns) > 0: # skip empty "partition" 
                    input_df = input_df.unionAll(new_df)
            else:
                input_df = new_df
            if input['nearest_the_tip_in_range']:
                break
        if input_df is None:
            logger.error("Looks like none of the input materialised path exist for this input: {0}. Check input details below".format(repr(input)))
            logger.error("Refer input materialised paths: {0}".format(' '.join(input['resource_materialized_paths'])))
            raise RuntimeError("Input is None. Couldnt find any materialised path for this input: {0}".format(repr(input)))
        return input_df

    # validation for encryption_key
    encryption_keys = {input['encryption_key'] for input in input_map['data_inputs'] if input['encryption_key']}
    if len(encryption_keys) > 1:
        raise ValueError("Only one input should have 'encryption_key' defined!")

    # assign input dataframes to user provided alias'
    for i, input in enumerate(input_map['data_inputs']):
        input_df = load_input_df(input, sc, aws_region)
        input_signal = Signal.deserialize(input['serialized_signal'])
        if input['alias']:
            exec('{0} = input_df'.format(input['alias']))
            exec('{0}_signal = input_signal'.format(input['alias']))
            if input_df is not None:
                input_df.registerTempTable(input['alias'])
        exec('input{0} = input_df'.format(i))
        exec('input{0}_signal = input_signal'.format(i))
        if input_df is not None:
            input_df.registerTempTable('input{0}'.format(i))

    # assign timers to user provided alias'
    for i, input in enumerate(input_map['timers']):
        input_signal = Signal.deserialize(input['serialized_signal'])
        if input['alias']:
            exec('{0} = "{1}"'.format(input['alias'], input['time']))
            exec('{0}_signal = input_signal'.format(input['alias']))
        exec('timer{0} = "{1}"'.format(i, input['time']))
        exec('timer{0}_signal = input_signal'.format(i))
        
    # assign other signals to user provided alias'
    for i, input in enumerate(input_map['other_signals']):
        input_signal = Signal.deserialize(input['serialized_signal'])
        if input['alias']:
            exec('{0} = "{1}"'.format(input['alias'], input['time']))
            exec('{0}_signal = input_signal'.format(input['alias']))

    output_param = json.loads(output_str)
    output_signal = Signal.deserialize(output_param['serialized_signal'])

    dimensions = output_param['dimension_map']

    output = None

    completion_path = output_param['completion_indicator_materialized_path']
    uses_hadoop_success_file = False
    if completion_path and completion_path.endswith("/_SUCCESS"):
        sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true")
        uses_hadoop_success_file = True
    else:
        sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    _locals = locals() 
    # local closure catch when exec in a function
    # https://stackoverflow.com/questions/23168282/setting-variables-with-exec-inside-a-function

    exec(prologue_code, globals(), _locals)

    exec(client_code, globals(), _locals)
    output = _locals.get('output', _locals.get(output_param['alias'], None))

    exec(epilogue_code, globals(), _locals)

    if not output:
        print('Client script could not set the output properly. Aborting execution...')
        raise ValueError("Client script could not set the 'output' or '{}' variable properly. Please assign the output dataframe to either of those variables.".format(output_param['alias']))

    output = output.write\\
             .format(output_param['data_format'])\\
             .option("header", output_param['data_header_exists'])\\
             .option("delimiter", output_param['delimiter'])\\
             .mode("overwrite")
             
    if 'partition_by' in args:
        partition_cols = ast.literal_eval(args['partition_by'])
        output = output.partitionBy(partition_cols)
    
    output.save(output_param['resource_materialized_path'])

    # save schema
    try:
        output_signal_internal = runtime_platform.storage.map_materialized_signal(output_signal)
        schema_file = output_signal_internal.resource_access_spec.data_schema_file
        if schema_file:
            # this implicitly calls _jdf.schema().json() and then json.loads, so avoiding it now directly using _jdf.
            # schema_data = json.dumps(output.schema.jsonValue())
            schema_data = output._jdf.schema().json()
            runtime_platform.storage.save(schema_data, [], output_signal_internal.get_materialized_resource_paths()[0].strip("/") + "/" + schema_file)
    except Exception as error:
        # note: critical is not supported via logger
        logger.error("IntelliFlow: An error occurred while trying to create schema! Error: " + str(error))

    if completion_path and not uses_hadoop_success_file:
        # Below line added due to "ClassNotFoundException for org.apache.hadoop.mapred.DirectOutputCommitter" in saveAsTextFile
        sc._jsc.hadoopConfiguration().set("mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
        try:
            spark.sparkContext.parallelize([], 1).saveAsTextFile(output_param['completion_indicator_materialized_path'])
        except:
            # try to delete the file and create again
            try:
                URI = sc._gateway.jvm.java.net.URI
                Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
                FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
                fs = Path(output_param['completion_indicator_materialized_path']).getFileSystem(sc._jsc.hadoopConfiguration())
                fs.delete(Path(output_param['completion_indicator_materialized_path']), True)
                spark.sparkContext.parallelize([], 1).saveAsTextFile(output_param['completion_indicator_materialized_path'])
            except:
                # TODO
                print('Could not save completion indicator resource! Not a problem for the same app but it might impact downstream applications.')
"""
            + f"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args_list = ['--{JOB_NAME_PARAM}', '--{CLIENT_CODE_BUCKET}', '--{CLIENT_CODE_PARAM}', '--{INPUT_MAP_PARAM}', '--{OUTPUT_PARAM}',
                 '--{BOOTSTRAPPER_PLATFORM_KEY_PARAM}', '--{AWS_REGION}', '--{WORKING_SET_OBJECT_PARAM}', 
                 '--{USER_EXTRA_PARAMS_PARAM}', '--{IGNORED_BUNDLE_MODULES_PARAM}', '--{EXECUTION_ID}']
    for arg in args_list:
        parser.add_argument(arg)
    args, _ = parser.parse_known_args()
    
    user_arg_keys = json.loads(args.{USER_EXTRA_PARAMS_PARAM})
    for arg in user_arg_keys:
        parser.add_argument(arg)
    args, _ = parser.parse_known_args()

    prologue_code_base64_str = '{base64.b64encode(self._prologue_code.encode('utf-8')).decode('ascii')}'
    epilogue_code_base64_str = '{base64.b64encode(self._epilogue_code.encode('utf-8')).decode('ascii')}'

    prologue_code = b64.b64decode(prologue_code_base64_str).decode('utf-8')
    epilogue_code = b64.b64decode(epilogue_code_base64_str).decode('utf-8')
    # use base64 for json support in cli

    ignored_bundle_modules = json.loads(args.{IGNORED_BUNDLE_MODULES_PARAM})
    install_if_bundle(args.{WORKING_SET_OBJECT_PARAM}, ignored_bundle_modules)
    run_emr_bootstrap(args.{JOB_NAME_PARAM},
                      args.{CLIENT_CODE_BUCKET},
                      args.{CLIENT_CODE_PARAM},
                      args.{INPUT_MAP_PARAM},
                      args.{OUTPUT_PARAM},
                      args.{BOOTSTRAPPER_PLATFORM_KEY_PARAM},
                      args.{AWS_REGION},
                      prologue_code,
                      epilogue_code,
                      args.{WORKING_SET_OBJECT_PARAM},
                      vars(args))
"""
        )
