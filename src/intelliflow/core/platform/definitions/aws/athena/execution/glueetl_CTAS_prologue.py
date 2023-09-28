# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Module to provide Athena CTAS input materialization Glue ETL script
"""
from typing import ClassVar, Optional, Set

from intelliflow.core.platform.definitions.aws.athena.common import ATHENA_RESERVED_DDL_KEYWORDS, ATHENA_RESERVED_SELECT_STATEMENT_KEYWORDS
from intelliflow.core.platform.definitions.aws.athena.execution.common import (
    AWS_REGION,
    BOOTSTRAPPER_PLATFORM_KEY_PARAM,
    CLIENT_CODE_BUCKET,
    DATABASE_NAME_PARAM,
    EXECUTION_ID_PARAM,
    HADOOP_PARAMS_PARAM,
    INPUT_MAP_PARAM,
    JOB_NAME_PARAM,
    OUTPUT_PARAM,
    WORKGROUP_ID_PARAM,
)


class GlueAthenaCTASPrologue:
    """This class dynamically creates the glue ETL script that would used as a prologue to an
    RheocerOS Athena execution.
    Its main purpose is to create materialized views (ATHENA EXTERNAL TABLES) specific to the supported
    input data of the execution.
    These tables are temporary and isolated for an execution.

    The following are not the responsibilities of this prologue step:
    - Actual execution post materialization
    - Clean-up for these tables/views post execution
    """

    NONOVERRIDABLE_PARAMS: ClassVar[Set[str]] = {
        JOB_NAME_PARAM,
        INPUT_MAP_PARAM,
        CLIENT_CODE_BUCKET,
        OUTPUT_PARAM,
        EXECUTION_ID_PARAM,
        DATABASE_NAME_PARAM,
        WORKGROUP_ID_PARAM,
        HADOOP_PARAMS_PARAM,
        BOOTSTRAPPER_PLATFORM_KEY_PARAM,
        AWS_REGION,
    }

    def __init__(self, var_args: Optional[Set[str]] = None, prologue_code: Optional[str] = None, epilogue_code: Optional[str] = None):
        """
        Instantiate a different version of this script.

        Parameters
        ----------
        var_args: extra arguments on top of non-overridable GlueDefaultABI::NONOVERRIDABLE_PARAMs, they can be
        either app-developer provided or Glue construct impl specific. These are exposed to embedded client do as well.

        prologue_code: A Glue based construct can provide prologue code to support more enhanced ABIs or to evaluate
        some of the 'var_args' which are construct specific (neither user provided nor NONOVERRIDABLE).

        epilogue_code: A Glue based construct can provide epilogue code to be executed at the end of the script
        to take a set of final actions based on Contruct impl and some of the 'var_args'. Ex: Any epilogue functionality
        can be exposed to app-developers as a high-level feature (such as dumping data to different medium that
        authorized IF_EXEC_ROLE already).
        """
        self._args = self.NONOVERRIDABLE_PARAMS.update(var_args) if var_args else self.NONOVERRIDABLE_PARAMS
        self._prologue_code = prologue_code if prologue_code else ""
        self._epilogue_code = epilogue_code if epilogue_code else ""

    def generate_glue_script(self) -> str:
        return f"""import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.utils import *
import datetime
import json
import boto3

from intelliflow.core.signal_processing import Signal
# make sure full-stack platform API (including driver interfaces and related entities) are exposed.
from intelliflow.core.platform.constructs import *
from intelliflow.core.platform.development import RuntimePlatform
from intelliflow.core.platform.definitions.aws.glue import catalog as glue_catalog
from intelliflow.core.platform.definitions.aws.athena.client_wrapper import query, ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST
from intelliflow.core.platform.definitions.aws.athena.common import check_name_for_DDL, check_name_for_DML
from intelliflow.core.platform.definitions.aws.athena.execution.common import create_input_table_name, create_input_filtered_view_name, convert_spark_datatype_to_presto

args = getResolvedOptions(sys.argv, {list(self._args)!r})

execution_id = args['{EXECUTION_ID_PARAM}']
workgroup_id = args['{WORKGROUP_ID_PARAM}']
database_name = args['{DATABASE_NAME_PARAM}']

aws_region = args['{AWS_REGION}']
code_bucket = args['{CLIENT_CODE_BUCKET}']

s3 = boto3.resource('s3', region_name=aws_region)
glue = boto3.client('glue', region_name=aws_region)
athena = boto3.client('athena', region_name=aws_region)

input_map_obj = s3.Object(code_bucket, args['{INPUT_MAP_PARAM}'])
input_map_str = input_map_obj.get()['Body'].read().decode('utf-8') 

output_obj = s3.Object(code_bucket, args['{OUTPUT_PARAM}'])
output_str = output_obj.get()['Body'].read().decode('utf-8') 

bootstrapper_obj = s3.Object(code_bucket, args['{BOOTSTRAPPER_PLATFORM_KEY_PARAM}'])
serialized_bootstrapper_str = bootstrapper_obj.get()['Body'].read().decode('utf-8') 
runtime_platform: RuntimePlatform = RuntimePlatform.deserialize(serialized_bootstrapper_str)
runtime_platform.runtime_init(runtime_platform.batch_compute)

sc = SparkContext()

hadoop_param_dic = json.loads(args['{HADOOP_PARAMS_PARAM}'])
for key, value in hadoop_param_dic.items():
    sc._jsc.hadoopConfiguration().set(key, value)

glueContext = GlueContext(sc)
spark = glueContext.spark_session

logger = glueContext.get_logger()

def create_query(database: str, table_name: str, partition_keys, all_partitions) -> str:
    pr_tb = '`{{0}}`.`{{1}}`'.format(database, table_name)
    partition_conditions = []
    for i, partition_key in enumerate(partition_keys):
        partition_values = [partition[i] for partition in all_partitions]
        condition = f'{{partition_key}} in ({{",".join([repr(val) for val in partition_values])}})'
        partition_conditions.append(condition)

    where_clause = f'{{" AND ".join(partition_conditions)}}' if partition_conditions else ''
    return f'''
              SELECT * FROM {{pr_tb}}
                  {{" WHERE " + where_clause if where_clause else ""}}
            '''
        
job = Job(glueContext)
job.init(args['{JOB_NAME_PARAM}'], args)

input_map = json.loads(input_map_str)

output_param = json.loads(output_str)
dimensions = output_param['dimension_map']

# TODO move to a separate module under 'execution' (we now bootstrap RheocerOS).
def load_input_df(input, sc, aws_region):
    if input['encryption_key']:
        # first set encryption (if defined)
        if input['resource_type'] == 'S3':
            sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3.cse.enabled", "true")
            sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3.cse.kms.keyId", input['encryption_key'])
            sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3.cse.kms.region", aws_region)
            sc._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3.cse.encryptionMaterialsProvider",
                                              "com.amazon.ws.emr.hadoop.fs.cse.KMSEncryptionMaterialsProvider")
        else:
            raise NotImplementedError("Dont know how to use encryption_key for resource type {{0}}!".format(input['resource_type']))
        
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

    nearest_the_tip_index_in_range = None
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
                        resource_path = glue_catalog.get_location(glue, database, table_name, next_partition_values)
                        if not resource_path:
                            if not input['range_check_required']:
                                continue
                            else:
                                raise RuntimeError("{{0}} path does not exist! Dimensions: {{1}}"
                                                   "Either there is a problem with range_check mechanism or the partition has been deleted "
                                                   "after the execution has started.".format(resource_path, next_partition_values))
                else:
                    new_df = spark.sql(create_query(database, table_name, input['partition_keys'], next_partitions))
            elif not input['range_check_required']:
                logger.error("{{0}} path does not exist. Since range check is not required. Continuing with next available path".format(resource_path))
                continue
            else:
                raise RuntimeError("{{0}} path does not exist! "
                                   "Either there is a problem with range_check mechanism or the partition has been deleted "
                                   "after the execution has started.".format(resource_path))
        if not new_df:
            # native access via input materialized paths
            try:
                if input['data_format'] == 'parquet':
                    new_df = spark.read.parquet(resource_path)
                else:
                    # TODO read IF 'data_schema_file' for internal datasets when 'data_header_exists' is False
                    #      - mandatory for Athena CSV based CTAS outputs (which lack header)
                    #  if header is false and schema does not exist, fail!
                    new_df = spark.read.load(resource_path, format=input['data_format'], sep=input['delimiter'], inferSchema='true', header='true')
            # Path not found exception is considered Analysis Exception within Spark code. Please check the
            # link below for reference.
            # https://github.com/apache/spark/blob/1b609c7dcfc3a30aefff12a71aac5c1d6273b2c0/sql/catalyst/src/main/scala/org/apache/spark/sql/errors/QueryCompilationErrors.scala#L977
            except AnalysisException as e:
                if not input['range_check_required']:
                    if str(e).find('Path does not exist') != -1:
                        logger.error("{{0}} path does not exist. Since range check is not required. Continuing with next available path".format(resource_path))
                        continue
                raise e
        # since prologue tries to capture the schema, don't union all of the partitions but use the TIP
        # Trade off: if there is going to be a schema mismatch among partitions;
        #      - this approach postpones it to Athena execution (so no consistency issue), but it is captured late.
        #      - and if do a union, then this job will always take a lot of time.
        # so we heuristically choose single-partition option assuming that schema mismatch will be rare.
        #input_df = input_df.unionAll(new_df) if input_df else new_df
        input_df = input_df if input_df else new_df
        if input['nearest_the_tip_in_range']:
            nearest_the_tip_index_in_range = i
            break
    if input_df is None:
        logger.error("Looks like none of the input materialised path exist for this input: {{0}}. Check input details below".format(repr(input)))
        logger.error("Refer input materialised paths: {{0}}".format(' '.join(input['resource_materialized_paths'])))
        raise RuntimeError("Input is None. Couldnt find any materialised path for this input: {{0}}".format(repr(input)))
    return input_df, nearest_the_tip_index_in_range 

# validation for encryption_key
encryption_keys = {{input['encryption_key'] for input in input_map['data_inputs'] if input['encryption_key']}}
if len(encryption_keys) > 1:
    raise ValueError("Only one input should have 'encryption_key' defined!")

# assign input dataframes to user provided alias'
for i, input in enumerate(input_map['data_inputs']):
    input_df = None
    input_partition_index_from_tip = None
    if not input['alias']:
        raise ValueError('Alias must be provided to all of the inputs for an Athena execution!')
    alias = input['alias']

    if input['resource_type'] not in ['S3', 'GLUE_TABLE']:
        raise ValueError("Input data type {{}} not supported in Athena executions yet! Input alias: '{{}}'".format(input['resource_type'], input['alias']))
    else:
        input_df, input_partition_index_from_tip  = load_input_df(input, sc, aws_region)
    # TODO use input_signal for more elegant path extraction below
    # input_signal = Signal.deserialize(input['serialized_signal'])

    all_partitions = input['partitions']
    partition_keys = input['partition_keys']
    unmaterialized_path_format = input['path_format']
    path_root = unmaterialized_path_format 
    if partition_keys:
        path_root = path_root.split('{{}}')[0]
        path_root = path_root[:path_root.rfind('/')]
    if not path_root.endswith('/'):
        # Athena location requires trailing slash
        path_root = path_root + '/'

    # CTAS INPUT MATERIALIZATION
    # 1- SCHEMA EXTRACTION from Spark DF ('input_df' variable): 
    # 2- CREATE EXTERNAL TABLE for INPUT 
    #    2.1- use the alias as the table name
    #    2.2- specify SERDE
    #    2.3- Use 'Partition Projection'

    #  eliminate partition_keys from columns if they already exist (otherwise Athena will raise SemanticException:
    #    "Column repeated in partitioning columns")
    cols = [col for col in input_df.schema.fields if not partition_keys or check_name_for_DDL(col.name.lower()) not in [check_name_for_DDL(part_key.lower()) for part_key in partition_keys]]
    table_query= f'''CREATE EXTERNAL TABLE IF NOT EXISTS {{create_input_table_name(alias, execution_id)}}
                    (
                     {{(",").join([f'{{check_name_for_DDL(col.name.lower())}} {{convert_spark_datatype_to_presto(col.dataType.typeName())}}' for col in cols])}}
                    )
                    '''
    table_properties = dict()
    if input['encryption_key']:
        # refer https://docs.aws.amazon.com/athena/latest/ug/creating-tables-based-on-encrypted-datasets-in-s3.html
        table_properties[f'"has_encrypted_data"'] = '"true"'

    # partition projection!
    if partition_keys:
        table_query = table_query +  \\
                f'''PARTITIONED BY
                    (
                     {{(",").join([f'{{check_name_for_DDL(key.lower())}} STRING' for key in partition_keys])}}
                    )
                    '''
        table_properties['"projection.enabled"'] = '"true"'
        for i, key in enumerate(partition_keys):
            table_properties[f'"projection.{{key.lower()}}.type"'] = '"enum"'
            partition_values = [partition[i] for partition in all_partitions] if input_partition_index_from_tip is None else [all_partitions[input_partition_index_from_tip][i]]
            table_properties[f'"projection.{{key.lower()}}.values"'] = f'"{{",".join([str(val) for val in partition_values])}}"'
        table_properties['"storage.location.template"'] = '"' + unmaterialized_path_format.format(*['${{'+ key + '}}' for key in partition_keys]) + '"'

    # SERDE
    if input['data_format'].lower() == 'parquet':
        table_query = table_query +  \\
                     f'''STORED AS PARQUET
                     '''
        if input['data_compression']:
            table_properties['"parquet.compression"'] = '"' + input['data_compression'].upper() + '"'
    elif input['data_format'].lower() == 'orc':
        table_query = table_query +  \\
                     f'''STORED AS ORC
                     '''
        if input['data_compression']:
            table_properties['"orc.compress"'] = '"' + input['data_compression'].upper() + '"'
    else:
        table_query = table_query +  \\
                     f'''ROW FORMAT DELIMITED
                           FIELDS TERMINATED BY '{{input['delimiter']}}'
                           ESCAPED BY '\\x5c\\x5c'
                           LINES TERMINATED BY '\\n'
                     '''

    table_query = table_query +  \\
                    f'''
                    LOCATION "{{path_root}}"
                    '''

    if table_properties:
        table_query = table_query +  \\
                f'''TBLPROPERTIES
                    ('''
        key_index = 0
        for key, value in table_properties.items():
            table_query = table_query +  \\
                        f'''
                        {{key}} = {{value + (',' if key_index < len(table_properties) - 1 else '')}}
                        '''
            key_index = key_index + 1

        table_query = table_query +  \\
                f'''
                    )
                '''

    table_query = table_query + ";"
    #print(f"TABLE_QUERY2: {{table_query}}")
    query(athena,
          table_query,
          database_name,
          workgroup_id,
          wait=True,
          poll_interval_in_secs=3)

    # 3- CREATE VIEW using partition values
    partition_conditions = []
    for i, partition_key in enumerate(partition_keys):
        partition_values = [partition[i] for partition in all_partitions] if input_partition_index_from_tip is None else [all_partitions[input_partition_index_from_tip][i]]
        condition = f'{{check_name_for_DML(partition_key.lower())}} in ({{",".join([repr(str(val)) for val in partition_values])}})'
        partition_conditions.append(condition)

    where_clause = f'{{" AND ".join(partition_conditions)}}' if partition_conditions else ''
    view_query = f'''CREATE OR REPLACE VIEW {{create_input_filtered_view_name(alias, execution_id)}} AS
                      SELECT * FROM {{create_input_table_name(alias, execution_id)}}
                          {{" WHERE " + where_clause if where_clause else ""}};
                    '''
    query(athena,
          view_query,
          database_name,
          workgroup_id,
          wait=True,
          poll_interval_in_secs=3)
        
# TODO support TIMER, NOTIFICATION, CW ALARM and METRIC inputs as lookup tables in Athena executions
# input_map['timers'], etc

job.commit()
"""


class GlueDefaultABIScala:
    pass
