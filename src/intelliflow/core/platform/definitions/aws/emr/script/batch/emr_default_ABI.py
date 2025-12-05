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
from typing import ClassVar, List, Optional, Set

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

    RESERVED_KEYWORDS: ClassVar[List[str]] = [
        "runtime_platform",
        "output",
        "output_param",
        "output_signal",
        "output_metadata",
        "load_input_df",
        "load_input_content",
        "create_query",
        "input_map",
        "args",
        "dimensions",
        "spark",
        "sc",
        "glue",
        "job",
        "boto",
        "boto3",
        "s3",
        "completion_path",
        "exec",
    ]

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
from pyspark.sql import SQLContext, SparkSession, DataFrame
from pyspark.sql.functions import col, lit
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
            
    # Some nodes cannot pick up EMR bootstrapper installed dependencies
    from intelliflow.core.runtime import PYTHON_VERSION_MAJOR, PYTHON_VERSION_MINOR
    sys.path.append(f'/usr/local/lib/python{PYTHON_VERSION_MAJOR}.{PYTHON_VERSION_MINOR}/site-packages')

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
                        such as 's3://IF-application-123456789012-us-east-1/'.
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
    from intelliflow.core.signal_processing.signal_source import InternalDatasetSignalSourceAccessSpec, MANAGED_RAW_CONTENT_OUTPUT_FILE_NAME, DatasetMetadata
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
    
    def load_input_content(input, file_name, input_signal = None):
        # designed to be called by users for their unmanaged content as well so expecting data file name as an input
        if input.get("data_type", "dataset") not in ["content"]:
            return None
        
        if input_signal is None:  
            input_signal = Signal.deserialize(input['serialized_signal'])
            
        from intelliflow.core.serialization import loads

        input_internal_signal = runtime_platform.storage.map_materialized_signal(input_signal)
        path: str = input_internal_signal.get_materialized_resource_paths()[0]
        folder = path[path.find(InternalDatasetSignalSourceAccessSpec.FOLDER) :]
        data = runtime_platform.storage.load([folder], file_name)
        return loads(data) 

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
        s3_uri_list_parquet = []
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
                            if input['nearest_the_tip_in_range'] or not input['range_check_required'] or len(input['resource_materialized_paths']) < 2:
                                # don't use batch S3 read when `nearest` semantics enabled on input
                                new_df = spark.read.parquet(resource_path)
                            else:
                                s3_uri_list_parquet.append(resource_path)
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

        if len(s3_uri_list_parquet) > 0:
            input_df = spark.read.parquet(*s3_uri_list_parquet)
            if len(input_df.columns) == 0:
                input_df = None

        # always raise unless the input is "ref.range_check(False)"
        if input_df is None and (input['nearest_the_tip_in_range'] or not (input['is_reference'] and not input['range_check_required'])):
            logger.error("Looks like none of the input materialised path exist for this input: {0}. Check input details below".format(repr(input)))
            logger.error("Refer input materialised paths: {0}".format(' '.join(input['resource_materialized_paths'])))
            raise RuntimeError("Input is None. Couldnt find any materialised path for this input: {0}".format(repr(input)))
        return input_df

    # validation for encryption_key
    encryption_keys = {input['encryption_key'] for input in input_map['data_inputs'] if input['encryption_key']}
    if len(encryption_keys) > 1:
        raise ValueError("Only one input should have 'encryption_key' defined!")

    # assign loaded/deserialized input dataframes/variables to user provided alias'
    for i, input in enumerate(input_map['data_inputs']):
        input_signal = Signal.deserialize(input['serialized_signal'])
        input_data_type = input.get("data_type", "dataset")
        input_df = None
        if input_data_type in ["dataset", None]:
            input_df = load_input_df(input, sc, aws_region)
        elif input_data_type in ["content"]:
            if input.get("data_persistence", None) in ["managed"]:
                from intelliflow.core.signal_processing.signal_source import MANAGED_RAW_CONTENT_OUTPUT_FILE_NAME
                input_df = load_input_content(input, MANAGED_RAW_CONTENT_OUTPUT_FILE_NAME, input_signal)
                
        if input['alias']:
            exec('{0} = input_df'.format(input['alias']))
            exec('{0}_signal = input_signal'.format(input['alias']))
            if input_df is not None and input_data_type in ["dataset", None]:
                input_df.registerTempTable(input['alias'])
        exec('input{0} = input_df'.format(i))
        exec('input{0}_signal = input_signal'.format(i))
        if input_df is not None and input_data_type in ["dataset", None]:
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

    # TODO remove completion logic from this module. disabled now.
    # Drivers handle this in terminate_session callback now
    completion_path = None # output_param['completion_indicator_materialized_path'] if not output_param['has_metadata_actions'] else None
    uses_hadoop_success_file = False
    if completion_path and completion_path.endswith("/_SUCCESS"):
        sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "true")
        uses_hadoop_success_file = True
    else:
        sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    _locals = locals() 
    # local closure catch when exec in a function
    # https://stackoverflow.com/questions/23168282/setting-variables-with-exec-inside-a-function
    
    # why we pass single dict into exec?
    # https://stackoverflow.com/questions/39647566/why-does-python-3-exec-fail-when-specifying-locals
    _if_exec_scope = globals().copy()
    _if_exec_scope.update(_locals)

    exec(prologue_code, _if_exec_scope)

    exec(client_code, _if_exec_scope)
    output = _if_exec_scope.get('output', _if_exec_scope.get(output_param['alias'], None))

    exec(epilogue_code, _if_exec_scope)

    try:
        _final_output_metadata = output_metadata
    except NameError:
        _final_output_metadata = dict()

    output_data_persistence = output_param.get("data_persistence", None)
    if output_data_persistence in ["managed", None]:
        try:
            output
        except NameError:
            print('Client script could not set the output properly. Aborting execution...')
            raise ValueError("Client script could not set the 'output' or '{}' variable properly. Please assign the output data to either of those variables.".format(output_param['alias']))
            
        output_data_type = output_param.get("data_type", "dataset")
        if output_data_type in ["dataset", None]:
            if  isinstance(output, DataFrame):
                output_df = output
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
                        if getattr(output_df, "_jdf", None):
                            schema_data = output_df._jdf.schema().json()
                        else:
                            schema_data = output_df.schema.json()
                        runtime_platform.storage.save(schema_data, [], output_signal_internal.get_materialized_resource_paths()[0].strip("/") + "/" + schema_file)
                except Exception as error:
                    # note: critical is not supported via logger
                    logger.error("IntelliFlow: An error occurred while trying to create schema! Error: " + str(error))
                    
                # save metadata
                if output_param['has_metadata_actions'] and DatasetMetadata.RECORD_COUNT.value not in _final_output_metadata:
                    _final_output_metadata[DatasetMetadata.RECORD_COUNT.value] = output_df.rdd.countApprox(10000, confidence=0.50)
            else:
                uses_hadoop_success_file = False
                try:
                    import pandas as pd
                    output_data_format = output_param['data_format'].lower()
                    output_signal_internal = runtime_platform.storage.map_materialized_signal(output_signal)
                    output_file_name = output_param['resource_materialized_path'].strip("/") + "/" + MANAGED_RAW_CONTENT_OUTPUT_FILE_NAME
                    if isinstance(output, pd.DataFrame):
                        if output_data_format == "csv":
                            output.to_csv(output_file_name + ".csv", index=False, sep=output_param['delimiter'], header=output_param['data_header_exists'])
                        elif output_data_format == "parquet":
                            output.to_parquet(output_file_name)
                        else:
                            raise ValueError("Output data format '{}' is not supported for Pandas DataFrames in managed persistence!".format(output_data_format))
                        # save metadata
                        if output_param['has_metadata_actions'] and DatasetMetadata.RECORD_COUNT.value not in _final_output_metadata:
                            _final_output_metadata[DatasetMetadata.RECORD_COUNT.value] = len(output.index)
                except Exception as pe: 
                    raise ValueError("'output' variable of type '{}' could not be persisted!".format(type(output))) from pe
        elif output_data_type in ["content"] and output_data_persistence in ["managed"]:
            # managed persistence for RAW_CONTENT when explicitly set as "managed"
            from intelliflow.core.serialization import dumps

            output_internal_signal = runtime_platform.storage.map_materialized_signal(output_signal)
            path: str = output_internal_signal.get_materialized_resource_paths()[0]
            folder = path[path.find(InternalDatasetSignalSourceAccessSpec.FOLDER) :]
            data = dumps(output)
            runtime_platform.storage.save(data, [folder], MANAGED_RAW_CONTENT_OUTPUT_FILE_NAME)
            uses_hadoop_success_file = False
            
    # save output metadata even if it is empty (overwrite)
    runtime_platform.storage.save_internal_metadata(output_signal, _final_output_metadata)

    if completion_path and not uses_hadoop_success_file:
        try:
            empty_file_prefix = completion_path[completion_path.find(InternalDatasetSignalSourceAccessSpec.FOLDER) :]
            runtime_platform.storage.save("", [empty_file_prefix], "")
        except:
            logger.error('Could not save completion indicator resource! Not a problem for the same app but it might impact downstream applications.')
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
