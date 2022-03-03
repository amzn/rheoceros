# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from intelliflow.core.platform.definitions.aws.athena.common import to_athena_format
from intelliflow.core.platform.definitions.compute import BasicBatchDataInputMap, BasicBatchDataOutput

JOB_NAME_PARAM: str = "JOB_NAME"
INPUT_MAP_PARAM: str = "INPUT_MAP"
CLIENT_CODE_PARAM: str = "CLIENT_CODE"
CLIENT_CODE_BUCKET: str = "CLIENT_CODE_BUCKET"
CLIENT_CODE_METADATA: str = "CLIENT_CODE_METADATA"
CLIENT_CODE_ABI: str = "CLIENT_CODE_ABI"
OUTPUT_PARAM: str = "OUTPUT"
BOOTSTRAPPER_PLATFORM_KEY_PARAM: str = "BOOTSTRAPPER_PATH"
USER_EXTRA_PARAMS_PARAM: str = "USER_EXTRA_PARAM"
AWS_REGION: str = "AWS_REGION"

EXECUTION_ID_PARAM: str = "EXECUTION_ID"
DATABASE_NAME_PARAM: str = "DATABASE_NAME"
WORKGROUP_ID_PARAM: str = "WORKGROUP_ID"
HADOOP_PARAMS_PARAM: str = "HADOOP_PARAMS"

FILTERED_VIEW_SUFFIX: str = "filtered_view"

BatchInputMap = BasicBatchDataInputMap
BatchOutput = BasicBatchDataOutput


INTELLIFLOW_ATHENA_EXECUTION_INPUT_TABLE_NAME_FORMAT = "input_{}_{}"  # input_{ALIAS}_{EXEC_ID}
INTELLIFLOW_ATHENA_EXECUTION_OUTPUT_TABLE_NAME_FORMAT = "output_{}_{}"  # input_{ALIAS}_{EXEC_ID}


def create_input_table_name(alias: str, execution_ctx_id: str) -> str:
    """Refer
    https://docs.aws.amazon.com/athena/latest/ug/tables-databases-columns-names.html

    Out of all the concerns listed there, we are concerned about;
      - lower case restriction
      - and special chars other than '_'
    and to_athena_format is taking care of that.
    """
    return INTELLIFLOW_ATHENA_EXECUTION_INPUT_TABLE_NAME_FORMAT.format(to_athena_format(alias), to_athena_format(execution_ctx_id))


def create_input_filtered_view_name(alias: str, execution_ctx_id: str) -> str:
    return create_input_table_name(to_athena_format(alias), execution_ctx_id) + "_" + FILTERED_VIEW_SUFFIX


def create_output_table_name(alias: str, execution_ctx_id: str) -> str:
    return INTELLIFLOW_ATHENA_EXECUTION_OUTPUT_TABLE_NAME_FORMAT.format(to_athena_format(alias), to_athena_format(execution_ctx_id))


def convert_spark_datatype_to_presto(spark_datatype: str) -> str:
    """
    Map from
     https://spark.apache.org/docs/latest/sql-ref-datatypes.html

    to

     https://docs.aws.amazon.com/athena/latest/ug/create-table.html
    """
    spark_datatype_lower = spark_datatype.lower()
    # e.g varchar(100), decimal(10, 4), char(5)
    if spark_datatype_lower.startswith("varchar") or spark_datatype_lower.startswith("decimal") or spark_datatype_lower.startswith("char"):
        return spark_datatype_lower.upper()
    else:
        return {
            "boolean": "BOOLEAN",
            "float": "FLOAT",
            "double": "DOUBLE",
            "string": "STRING",
            "date": "DATE",
            "timestamp": "TIMESTAMP",
            "long": "BIGINT",
            "integer": "INT",
            "binary": "BINARY",
            "byte": "TINYINT",
            "short": "SMALLINT",
        }.get(spark_datatype_lower, "VARCHAR(65535)")
