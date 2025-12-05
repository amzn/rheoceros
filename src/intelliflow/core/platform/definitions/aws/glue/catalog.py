# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
import json
import logging
import math
from enum import Enum, unique
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
from botocore.exceptions import ClientError

from intelliflow.core.entity import CoreData
from intelliflow.core.platform.definitions.aws.common import exponential_retry
from intelliflow.core.signal_processing import DimensionSpec, Signal
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.dimension_constructs import AnyVariant, Dimension, DimensionFilter
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import (
    DATA_FORMAT_KEY,
    ENCRYPTION_KEY_KEY,
    PARTITION_KEYS_KEY,
    DatasetSignalSourceFormat,
    GlueTableSignalSourceAccessSpec,
    DatasetType,
    GlueTableSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
    SignalSource,
    SignalSourceAccessSpec,
    SignalSourceType,
)

logger = logging.getLogger(__name__)


CATALOG_COMMON_RETRYABLE_ERRORS = {"OperationTimeoutException", "InternalServiceException"}


class GlueTableDesc(CoreData):
    def __init__(self, signal: Signal, default_compute_params: Dict[str, Any], _test_bypass_checks: Optional[bool] = False) -> None:
        self.signal = signal
        self.default_compute_params = default_compute_params
        self._test_bypass_checks = _test_bypass_checks


def _map_column_type(col_type: str) -> Type:
    type = col_type.lower()
    if (
        type.startswith("decimal")
        or type.startswith("bigint")
        or type.startswith("int")
        or type.startswith("tinyint")
        or type.startswith("smallint")
        or type.startswith("long")
    ):
        return Type.LONG
    elif type.startswith("date") or type.startswith("timestamp"):
        return Type.DATETIME
    elif type.startswith("string") or type.startswith("varchar"):
        return Type.STRING
    else:
        raise TypeError(f"Column type {col_type} from AWS Glue Catalog cannot be mapped to an RheocerOS type!")


def _create_dimension_spec(partition_keys: List[Dict[str, Any]]) -> DimensionSpec:
    dim_spec = DimensionSpec()
    if partition_keys:
        partition_key = partition_keys[0]
        dim_name = partition_key["Name"]
        dim_type = _map_column_type(partition_key["Type"])
        dim_spec.add_dimension(
            Dimension(dim_name, dim_type), _create_dimension_spec(partition_keys[1:]) if len(partition_keys) > 1 else None
        )

    return dim_spec


def get_data_format(output_format: str, serde: str) -> Optional[DatasetSignalSourceFormat]:
    if output_format == "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat":
        # refer
        # https://hive.apache.org/javadocs/r2.2.0/api/org/apache/hadoop/hive/ql/io/HiveIgnoreKeyTextOutputFormat.html
        if serde in ["org.apache.hadoop.hive.serde2.OpenCSVSerde", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"]:
            return DatasetSignalSourceFormat.CSV
        elif serde in ["org.apache.hive.hcatalog.data.JsonSerDe", "org.openx.data.jsonserde.JsonSerDe"]:
            return DatasetSignalSourceFormat.JSON
    elif output_format == "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat":
        # refer
        # https://hive.apache.org/javadocs/r1.2.2/api/org/apache/hadoop/hive/ql/io/avro/AvroContainerOutputFormat.html
        return DatasetSignalSourceFormat.AVRO
    elif output_format == "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat":
        # refer
        # https://hive.apache.org/javadocs/r1.2.2/api/org/apache/hadoop/hive/ql/io/orc/OrcOutputFormat.html
        return DatasetSignalSourceFormat.ORC
    elif output_format == "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat":
        # refer
        # https://hive.apache.org/javadocs/r1.2.2/api/org/apache/hadoop/hive/ql/io/parquet/MapredParquetOutputFormat.html
        return DatasetSignalSourceFormat.PARQUET


def create_signal(glue_client, database_name: str, table_name: str) -> Optional[GlueTableDesc]:
    try:
        table = exponential_retry(
            glue_client.get_table,
            # temp access denied for dev-role (for a new app)
            CATALOG_COMMON_RETRYABLE_ERRORS.union({"AccessDeniedException"}),
            DatabaseName=database_name,
            Name=table_name,
        )
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "EntityNotFoundException":
            return None
        raise

    storage_desc: str = table["Table"].get("StorageDescriptor", {})
    location: str = storage_desc.get("Location", "")
    output_format: str = storage_desc.get("OutputFormat", "")
    serde_name: str = storage_desc.get("SerdeInfo", {}).get("Name", "")
    serde: str = storage_desc.get("SerdeInfo", {}).get("SerializationLibrary", "")

    partition_keys = table["Table"].get("PartitionKeys", None)
    partition_key_names = None
    dim_spec = None

    compute_param_hints = {}
    source_type: SignalSourceType = None
    if location.lower().startswith("s3://"):
        source_type = SignalSourceType.S3
    else:
        raise ValueError(
            f"Location for table {table_name!r} (in DB:{database_name!r}) is not recognized by RheocerOS! " f"Location: {location!r}"
        )

    access_spec: SignalSourceAccessSpec = None
    if source_type == SignalSourceType.S3:
        partition_key_names = [key["Name"] for key in partition_keys]
        dim_spec = _create_dimension_spec(partition_keys)
        s3_path_root = storage_desc["Location"]
        s3_spec = S3SignalSourceAccessSpec.from_url(account_id=None, url=s3_path_root)

        has_encrypted_data = table["Table"]["Parameters"].get("has_encrypted_data", "false")
        encryption_key_placeholder = "NEEDS_ENCRYPTION_KEY" if has_encrypted_data == "true" else None

        params = {
            # @USER_PARAM depends on this key (if not None)
            ENCRYPTION_KEY_KEY: encryption_key_placeholder,
            PARTITION_KEYS_KEY: partition_key_names,
        }
        data_format = get_data_format(output_format, serde)
        if data_format:
            params.update({DATA_FORMAT_KEY: data_format})
        else:
            logger.warning(
                f"Data format could not be inferred for table: {table_name!r} (in DB: {database_name!r})! "
                f"If the following metadata maps to one of the formats from {DatasetSignalSourceFormat!r}, "
                f"then please specify it. OutputFormat: {output_format!r}, Serde {serde!r}"
            )

        access_spec = S3SignalSourceAccessSpec(
            None,
            s3_spec.bucket,
            s3_spec.folder,
            *partition_key_names,
            # @USER_PARAM depends on this key (if not None)
            **params,
        )

    try:
        partitions = exponential_retry(
            glue_client.get_partitions, CATALOG_COMMON_RETRYABLE_ERRORS, DatabaseName=database_name, TableName=table_name, MaxResults=1
        )
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "EntityNotFoundException":
            partitions = None
        else:
            raise

    if not partitions or "Partitions" not in partitions or not partitions["Partitions"]:
        logger.critical(f"There no partitions in Glue catalog for table: {table_name} in DB: {database_name}!")
    else:
        partition = partitions["Partitions"][0]
        if partition_keys:
            partition_values = partition["Values"][: len(partition_key_names)]
            # IMPORTANT: we force load of partitions using the spec (from partition_keys) with error unwinding enabled.
            # This will enable inconsistency check against partition_keys definition and the actual partition values
            # in the catalog.
            # TODO DateVariant should auto-resolve format, until then <timestamp> should be mapped to string.
            #   without the format, DATETIME will rendered in default datetime string format and this might not be OK
            #   with S3 paths, etc.
            try:
                new_dim_spec = DimensionFilter.load_raw(partition_values, cast=dim_spec, error_out=True).get_spec()
            except (ValueError, TypeError) as error:
                logger.warning(
                    f"Glue catalog has inconsistent partition_keys definition for table {table_name!r} "
                    f"in database {database_name!r}!\n Inferred partition (dimension) spec from actual "
                    f"partition values ({partition_values!r}) does not match "
                    f"the spec from table metadata: {dim_spec!r}\n"
                    f"Cast error: {error!r}\n"
                    f"IntelliFlow will prefer actual partition values to infer the dimension spec."
                )
                new_dim_spec = DimensionFilter.load_raw(partition_values).get_spec()
        else:
            partition_values = partition["Values"]
            new_dim_spec = DimensionFilter.load_raw(partition_values).get_spec()
        # transfer names from the original spec
        new_dim_spec.compensate(other_spec=dim_spec)
        dim_spec = new_dim_spec

    if dim_spec is not None:
        # all pass filter
        dim_filter_spec = DimensionFilter.all_pass(dim_spec)
        dim_filter_spec.set_spec(dim_spec)
    else:
        dim_filter_spec = None

    signal = Signal(
        type=SignalType.EXTERNAL_GLUE_TABLE_PARTITION_UPDATE,
        resource_access_spec=access_spec,
        domain_spec=(
            SignalDomainSpec(
                dimension_spec=dim_spec,
                dimension_filter_spec=dim_filter_spec,
                # @USER_PARAM ALWAYS!
                integrity_check_protocol=None,
            )
            if (dim_spec is not None)
            else None
        ),
        alias=table_name,
    )

    return GlueTableDesc(signal, compute_param_hints)


def is_partition_present(glue_client, database_name: str, table_name: str, values: List[str]) -> bool:
    try:
        partition = exponential_retry(
            glue_client.get_partition,
            CATALOG_COMMON_RETRYABLE_ERRORS,
            DatabaseName=database_name,
            TableName=table_name,
            PartitionValues=values,
        )
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "EntityNotFoundException":
            return False
        else:
            raise

    return partition and partition["Partition"]


def get_location(glue_client, database_name: str, table_name: str, values: List[str]) -> Optional[str]:
    """Return the 'Location' (e.g S3 full path) for partition from the catalog"""
    try:
        partition = exponential_retry(
            glue_client.get_partition,
            CATALOG_COMMON_RETRYABLE_ERRORS,
            DatabaseName=database_name,
            TableName=table_name,
            PartitionValues=values,
        )
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "EntityNotFoundException":
            return None
        else:
            raise

    if partition and ("Partition" in partition) and partition["Partition"]:
        return partition["Partition"]["StorageDescriptor"]["Location"]


def check_table(session: boto3.Session, region: str, database: str, table_name: str) -> bool:
    """
    Checks the Glue Catalog for existence of a table within a particular database.
    :param glue_client: AWS Glue Client
    :param database: Glue Catalog database name
    :param table_name: Glue Catalog table name
    :return Boolean response. True if the table exists
    """

    try:
        glue_client = session.client(service_name="glue", region_name=region)
        response_get_tables = exponential_retry(
            glue_client.get_tables,
            ["AccessDeniedException"],  # IAM propagation (dev-role hits this in frontend)
            DatabaseName=database,
            MaxResults=1000,
        )

        table_list = response_get_tables["TableList"]
        for table in table_list:
            if table["Name"] == table_name or table["Name"] == f"{database.lower()}.{table_name.lower()}":
                return True
    except ClientError:
        logger.exception("Couldnt fetch table %s from %s database", table_name, database)
        raise

    return False


def query_table_spec(database_name: str, table_name: str) -> Optional[Tuple[GlueTableSignalSourceAccessSpec, DimensionSpec]]:
    """Tries to fetch the full description of target table.

    Extended details for partition keys and primary keys are encapsulated by the returned access spec object, whereas
    the detailed (typed) description and again the order of the partition keys are to be found in the DimensionSpec object.

    Parameters
    ----------
    database: provider name
    table: table name

    Returns
    -------
    if table exists, then returns a pair of GlueTableSourceAccessSpec and DimensionSpec, otherwise None.
    Partition Keys are contained in both but DimensionSpec contains the type information. If there are no
    partition keys, the DimensionSpec is empty (newly instantiated with 'DimensionSpec()').
    """
    # TODO will be implemented for more convenient/automatic glue table signal creation (during App development).
    pass


MAX_PUT_RULE_LIMIT = 64


class CatalogCWEventResource(CoreData):
    def __init__(self, cw_event_name: str, cw_event_arn: str, function_target_id: str, function_target_arn: str) -> None:
        self.cw_event_name = cw_event_name
        self.cw_event_arn = cw_event_arn
        self.function_target_id = function_target_id
        self.function_target_arn = function_target_arn

    @property
    def principal(self):
        return "events.amazonaws.com"

    @property
    def action(self):
        return "lambda:InvokeFunction"

    class LambdaCWEventTriggerPermission(CoreData):
        def __init__(self, lambda_func_name: str, action: str, principal: str, source_arn: str) -> None:
            self.lambda_func_name = lambda_func_name
            self.action = action
            self.principal = principal
            self.source_arn = source_arn

    def get_lambda_trigger_permission(self) -> LambdaCWEventTriggerPermission:
        return self.LambdaCWEventTriggerPermission(
            lambda_func_name=self.function_target_id, action=self.action, principal=self.principal, source_arn=self.cw_event_arn
        )


def _create_glue_event_rule(cw_events_client: boto3.session.Session.client, cw_event_name: str):
    event_pattern = {"source": ["aws.glue"], "detail-type": ["Glue Data Catalog Table State Change"]}

    # Put rule Name should be 64 characters in length
    resp = cw_events_client.put_rule(Name=cw_event_name, EventPattern=json.dumps(event_pattern), State="ENABLED")
    glue_event_arn = resp["RuleArn"]
    return glue_event_arn


def _add_target_to_event(cw_events_client: boto3.session.Session.client, cw_event_name: str, lambda_function_name: str, lambda_arn: str):

    response = cw_events_client.put_targets(Rule=cw_event_name, Targets=[{"Arn": lambda_arn, "Id": lambda_function_name}])
    if "FailedEntryCount" in response and response["FailedEntryCount"] > 0:
        raise RuntimeError(f"Cannot create timer {cw_event_name} for Lambda {lambda_function_name}. Response: {response!r}")

    return response


def provision_cw_event_rule(
    session: boto3.Session, region: str, cw_event_name: str, lambda_function_name: str, lambda_arn: str
) -> CatalogCWEventResource:
    cw_events_client = session.client(service_name="events", region_name=region)
    cw_event_rule_arn = _create_glue_event_rule(cw_events_client, cw_event_name)
    _add_target_to_event(cw_events_client, cw_event_name, lambda_function_name, lambda_arn)
    cw_resource_collection = CatalogCWEventResource(cw_event_name, cw_event_rule_arn, lambda_function_name, lambda_arn)
    return cw_resource_collection


def _remove_rule_targets(cw_events_client: boto3.session.Session.client, cw_resource_collection: CatalogCWEventResource):

    return cw_events_client.remove_targets(
        Rule=cw_resource_collection.cw_event_name,
        Ids=[
            cw_resource_collection.function_target_id,
        ],
    )


def _delete_cw_rule(cw_events_client: boto3.session.Session.client, cw_resource_collection: CatalogCWEventResource):

    return cw_events_client.delete_rule(Name=cw_resource_collection.cw_event_name)


def release_cw_event_rule(session: boto3.session, region: str, cw_resource_collection: CatalogCWEventResource):
    cw_events_client = session.client(service_name="events", region_name=region)
    if cw_resource_collection is None:
        raise Exception("Catalog Cloudwatch Event Rule does not exist")

    _remove_rule_targets(cw_events_client, cw_resource_collection)
    _delete_cw_rule(cw_events_client, cw_resource_collection)
