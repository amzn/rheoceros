# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AWS Athena driver that provide big-data workload execution abstraction to a Processor.
"""
import json
import logging
import uuid
from datetime import datetime
from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple, Type, cast

import boto3
from botocore.exceptions import ClientError

from intelliflow.core.deployment import get_working_set_as_zip_stream, is_environment_immutable
from intelliflow.core.permission import PermissionContext
from intelliflow.core.signal_processing import DimensionFilter, DimensionSpec, Signal, Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import (
    DATA_FORMAT_KEY,
    DATA_TYPE_KEY,
    DATASET_FORMAT_KEY,
    DATASET_HEADER_KEY,
    DATASET_SCHEMA_TYPE_KEY,
    ENCRYPTION_KEY_KEY,
    CWMetricSignalSourceAccessSpec,
    DatasetSchemaType,
    DatasetSignalSourceAccessSpec,
    DatasetSignalSourceFormat,
    DataType,
    GlueTableSignalSourceAccessSpec,
    InternalDatasetSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
    SignalSourceAccessSpec,
    SignalSourceType,
)
from intelliflow.core.signal_processing.slot import SlotCodeMetadata, SlotCodeType, SlotType

from ...constructs import (
    BatchCompute,
    ConstructInternalMetricDesc,
    ConstructParamsDict,
    ConstructPermission,
    ConstructSecurityConf,
    EncryptionKeyAllocationLevel,
    RoutingTable,
    Storage,
)
from ...definitions.aws.athena.client_wrapper import (
    ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST,
    ATHENA_MAX_QUERY_STRING_LENGTH_IN_BYTES,
    INTELLIFLOW_ATHENA_INPUT_VIEW_LENGTH,
    create_or_update_workgroup,
    delete_workgroup,
    get_athena_query_execution_failure_type,
    get_athena_query_execution_state_type,
    get_table_metadata,
    query,
)
from ...definitions.aws.athena.common import to_athena_format
from ...definitions.aws.athena.execution.common import (
    AWS_REGION,
    BOOTSTRAPPER_PLATFORM_KEY_PARAM,
    CLIENT_CODE_BUCKET,
    DATABASE_NAME_PARAM,
    EXECUTION_ID_PARAM,
    HADOOP_PARAMS_PARAM,
    INPUT_MAP_PARAM,
    OUTPUT_PARAM,
    USER_EXTRA_PARAMS_PARAM,
    WORKGROUP_ID_PARAM,
    BatchInputMap,
    BatchOutput,
    create_input_filtered_view_name,
    create_input_table_name,
    create_output_table_name,
)
from ...definitions.aws.athena.execution.glueetl_CTAS_prologue import GlueAthenaCTASPrologue
from ...definitions.aws.common import AWS_COMMON_RETRYABLE_ERRORS
from ...definitions.aws.common import CommonParams as AWSCommonParams
from ...definitions.aws.common import exponential_retry, get_code_for_exception
from ...definitions.aws.glue import catalog as glue_catalog
from ...definitions.aws.glue.client_wrapper import (
    PYTHON_MODULES_TO_BE_AVOIDED_IN_GLUE_BUNDLE,
    GlueJobCommandType,
    GlueJobLanguage,
    GlueWorkerType,
    create_glue_job,
    delete_glue_job,
    get_glue_job,
    get_glue_job_run,
    get_glue_job_run_failure_type,
    get_glue_job_run_state_type,
    start_glue_job,
    update_glue_job,
)
from ...definitions.aws.s3.bucket_wrapper import MAX_BUCKET_LEN, bucket_exists, create_bucket, delete_bucket, get_bucket, put_policy
from ...definitions.aws.s3.object_wrapper import build_object_key, delete_folder, empty_bucket, object_exists, put_object
from ...definitions.common import ActivationParams
from ...definitions.compute import (
    ComputeExecutionDetails,
    ComputeFailedResponse,
    ComputeFailedResponseType,
    ComputeFailedSessionState,
    ComputeFailedSessionStateType,
    ComputeLogQuery,
    ComputeResourceDesc,
    ComputeResponse,
    ComputeResponseType,
    ComputeSessionDesc,
    ComputeSessionState,
    ComputeSessionStateType,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from ..aws_common import AWSConstructMixin

module_logger = logging.getLogger(__file__)


class AWSAthenaBatchCompute(AWSConstructMixin, BatchCompute):
    """AWS Athena based BatchCompute impl that provides PrestoSQL big-data workload execution
    abstraction to a Platform (to its Processor particularly).

    Thanks to callbacks from RheocerOS orchestration, this driver is able to manage the full life-cycle of
    Athena execution and provide;
      - auto-schema management for external and internal data
      - plumbing for data formats,
         - Temporal CTAS based execution
      - auto 'Partition Projection' for external and internal data and other exploitations around optimal use of Athena:
      - hence seamless integration with downstream compute nodes (Athena or other Compute types [Spark, etc]).
      - leverage internal Storage

    Supports both Lang.PrestoSQL and ABI.PARAMETRIZED_QUERY

    Trade-offs:

        Pros:
            - N/A. Designed and impl'd to be the most balanced impl around AWS Athena.
        Cons:
            - N/A.

    """

    GLUE_JOB_NAME_FORMAT: ClassVar[str] = "IntelliFlow-{0}-{1}-{2}-{3}-{4}"
    GLUE_CLIENT_RETRYABLE_EXCEPTION_LIST: Set[str] = {
        "ConcurrentRunsExceededException",
        "OperationTimeoutException",
        "InternalServiceException",
    }

    @classmethod
    def driver_spec(cls) -> DimensionFilter:
        return DimensionFilter.load_raw(
            {
                Lang.PRESTO_SQL: {
                    ABI.PARAMETRIZED_QUERY: {
                        "*": {"*": {}},  # does not matter what extra params are
                    }
                }
            }
        )

    def __init__(self, params: ConstructParamsDict) -> None:
        """Called the first time this construct is added/configured within a platform.

        Subsequent sessions maintain the state of a construct, so the following init
        operations occur in the very beginning of a construct's life-cycle within an app.
        """
        super().__init__(params)
        self._athena = self._session.client("athena", region_name=self._region)
        self._glue = self._session.client("glue", region_name=self._region)
        self._database_name = None
        self._workgroup_id = None
        self._output_location = None

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._athena = self._session.client("athena", region_name=self._region)
        self._glue = self._session.client("glue", region_name=self._region)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._athena = None
        self._glue = None

    def provide_output_attributes(self, inputs: List[Signal], slot: Slot, user_attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if user_attrs.get(DATA_TYPE_KEY, DataType.DATASET) != DataType.DATASET:
            raise ValueError(
                f"{DATA_TYPE_KEY!r} must be defined as {DataType.DATASET} or left undefined for {self.__class__.__name__} output!"
            )

        data_format_value = user_attrs.get(DATASET_FORMAT_KEY, user_attrs.get(DATA_FORMAT_KEY, None))
        # default to PARQUET
        data_format = DatasetSignalSourceFormat.PARQUET if data_format_value is None else DatasetSignalSourceFormat(data_format_value)
        header_not_supported = data_format == DatasetSignalSourceFormat.CSV
        # Athena CTAS output does not have header (IF compensates it by using SCHEMAS in downstream node executions)
        # so if the user wants to have header appended
        if header_not_supported:
            user_header_key = user_attrs.get(DATASET_HEADER_KEY, None)
            if user_header_key is not None:
                if (isinstance(user_header_key, str) and user_header_key.lower() == "true") or (
                    isinstance(user_header_key, bool) and user_header_key
                ):
                    raise ValueError(
                        f"{DATASET_HEADER_KEY} cannot be set True for Athena PrestoSQL output partitions with {data_format_value!r} format!"
                    )

        return {
            DATA_TYPE_KEY: DataType.DATASET,
            DATASET_HEADER_KEY: not header_not_supported,  # we cannot control CTAS header generation (so ignore usr setting to False)
            DATASET_SCHEMA_TYPE_KEY: DatasetSchemaType.ATHENA_CTAS_SCHEMA_JSON,
            DATASET_FORMAT_KEY: data_format,
        }

    def query_external_source_spec(
        self, ext_signal_source: SignalSourceAccessSpec
    ) -> Optional[Tuple[SignalSourceAccessSpec, DimensionSpec]]:
        raise NotImplementedError(
            f"This external signal source ({ext_signal_source.source!r}) cannot be queried"
            f" by BatchCompute driver: {self.__class__.__name__}"
        )

    def compute(
        self,
        route: Route,
        materialized_inputs: List[Signal],
        slot: Slot,
        materialized_output: Signal,
        execution_ctx_id: str,
        retry_session_desc: Optional[ComputeSessionDesc] = None,
    ) -> ComputeResponse:

        if retry_session_desc:
            # 0- check which stage should be retried. if it has passed the prologue stage already,
            # then retry on the 2nd stage directly (OPTIMIZATION).
            prologue_job_arn: str = self._glue_job_lang_map[GlueJobLanguage.from_slot_lang(Lang.PYTHON)]["2.0"]["job_arn"]
            if retry_session_desc.resource_desc.resource_path != prologue_job_arn:
                # execution is already in the second (Athena CTAS) stage
                return self._ctas_compute(materialized_inputs, slot, materialized_output, execution_ctx_id, session_dec=retry_session_desc)

        # 1- check if we need prologue Glue stage to materialize inputs into Athena filtered views
        if any([signal for signal in materialized_inputs if signal.type.is_data()]):
            return self._prologue_compute(materialized_inputs, slot, materialized_output, execution_ctx_id, retry_session_desc)
        else:
            # 2- directly move into the second (Athena CTAS) stage and run the query
            return self._ctas_compute(materialized_inputs, slot, materialized_output, execution_ctx_id, session_dec=None)

    def _prologue_compute(
        self,
        materialized_inputs: List[Signal],
        slot: Slot,
        materialized_output: Signal,
        execution_ctx_id: str,
        retry_session_desc: Optional[ComputeSessionDesc] = None,
    ) -> ComputeResponse:
        """Create a temporary CTAS context for the materialized output partition.

        Algo:
            - materialize input tables
                - read a source partition, then create an exec specific 'Athena external table'
            - replace table names in the user query with new (auto-generated) table names.
            - initiate CTAS query
        """
        # validation for;
        #  - when this driver is the sole BatchCompute (user provides an unsupported compute decl),
        #  - spec filterin/routing issues in CompositeBatchCompute.
        if slot.code_lang != Lang.PRESTO_SQL or slot.code_abi != ABI.PARAMETRIZED_QUERY:
            raise ValueError(f"{self.__class__.__name__!r} cannot compute the slot: {slot!r}!")

        if not execution_ctx_id:
            raise ValueError(f'{self.__class__.__name__!r}::compute requires "execution_id" argument!')

        code_metadata: SlotCodeMetadata = slot.code_metadata
        extra_params: Dict[str, Any] = dict(slot.extra_params)
        user_extra_param_keys = list(set(extra_params.keys()))

        # RUN CTAS PROLOGUE to create materialized Athena views for each input
        lang = GlueJobLanguage.from_slot_lang(Lang.PYTHON)
        glue_version = "2.0"
        lang_spec = self._glue_job_lang_map[lang]
        version_spec = lang_spec[glue_version]
        job_name: str = version_spec["job_name"]
        job_arn: str = version_spec["job_arn"]
        # Add Glue Job execution related params (workers, glue version)
        extra_params.update(version_spec["params"])

        # PROCESSOR must have materialized the signals in the exec context
        # so even for internal signals, paths should be absolute, fully materialized.
        input_map = BatchInputMap(materialized_inputs)
        output = BatchOutput(materialized_output)

        lang_code = str(Lang.PYTHON)
        unique_compute_id: str = str(uuid.uuid1())
        # push inputs to S3
        input_map_key = build_object_key(
            ["driver_data", self.__class__.__name__, "glue", lang_code, job_name, unique_compute_id], f"input_map.json"
        )
        exponential_retry(
            put_object,
            {"ServiceException", "TooManyRequestsException"},
            self.get_platform().storage._bucket,
            input_map_key,
            input_map.dumps().encode("utf-8"),
        )

        output_param_key = build_object_key(
            ["driver_data", self.__class__.__name__, "glue", lang_code, job_name, unique_compute_id], f"output_param.json"
        )
        exponential_retry(
            put_object,
            {"ServiceException", "TooManyRequestsException"},
            self.get_platform().storage._bucket,
            output_param_key,
            output.dumps().encode("utf-8"),
        )

        ##extra_jars = self._bundle_s3_paths
        # if slot.code_lang == Lang.SCALA:
        #    extra_jars = extra_jars + (
        #        code_metadata.external_library_paths if code_metadata.external_library_paths else [])

        extra_params.update(
            {
                # --enable-glue-datacatalog
                "enable-glue-datacatalog": ""
            }
        )

        # pass extra_params as job args first
        args = {f"--{key}": str(value) for key, value in extra_params.items()}
        args.update(
            {
                f"--{INPUT_MAP_PARAM}": input_map_key,
                f"--{CLIENT_CODE_BUCKET}": self.get_platform().storage.bucket_name,
                f"--{OUTPUT_PARAM}": output_param_key,
                f"--{EXECUTION_ID_PARAM}": execution_ctx_id,
                f"--{AWS_REGION}": self.region,
                f"--{DATABASE_NAME_PARAM}": self._database_name,
                f"--{WORKGROUP_ID_PARAM}": self._workgroup_id,
                f"--{HADOOP_PARAMS_PARAM}": json.dumps({})
                # f"--{USER_EXTRA_PARAMS_PARAM}": json.dumps(user_extra_param_keys)
            }
        )
        try:
            # Start the prologue job to setup execution specific input data views in our Athena DB.
            job_id = exponential_retry(
                start_glue_job,
                self.GLUE_CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._glue,
                GlueJobCommandType.BATCH,
                lang,
                job_name,
                args,
                # evaluate if any of the extra_params map to Glue job params
                extra_params,
                prev_job_run_id=retry_session_desc.session_id if retry_session_desc else None,
            )
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in self.GLUE_CLIENT_RETRYABLE_EXCEPTION_LIST or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                failed_response_type = ComputeFailedResponseType.TRANSIENT
            elif error_code in ["InvalidInputException", "EntityNotFoundException", "ResourceNumberLimitExceededException"]:
                failed_response_type = ComputeFailedResponseType.BAD_SLOT

            return ComputeFailedResponse(
                failed_response_type,
                ComputeResourceDesc(job_name, job_arn, driver=self.__class__),
                error_code,
                # TODO
                str(error.response["Error"]),
            )

        return ComputeSuccessfulResponse(
            ComputeSuccessfulResponseType.PROCESSING,
            ComputeSessionDesc(job_id, ComputeResourceDesc(job_name, job_arn, driver=self.__class__)),
        )

    def get_session_state(self, session_desc: ComputeSessionDesc, active_compute_record: RoutingTable.ComputeRecord) -> ComputeSessionState:
        """
        Checks the state of two-stage RheocerOS Athena execution, and does the state transition implicitly if necessary
        and irrespective of whether a state-transition occurs or not it always returns the session state of
        the execution back to the orchestration.

        Algo:

        First check if still in CTAS Prologue Glue job execution state.
        yes;
            is Glue job session COMPLETE?
                yes:
                    - initiate Athena CTAS query for the actual execution, using the views created in the prologue jobs.
                    - return the initial session state of the Athena execution.
                no:
                   return prologue Glue job state
        no;
            - return the session state of the Athena execution.
        """
        prologue_job_arn: str = self._glue_job_lang_map[GlueJobLanguage.from_slot_lang(Lang.PYTHON)]["2.0"]["job_arn"]
        if session_desc.resource_desc.resource_path == prologue_job_arn:
            # STATE: Glue Prologue Job
            job_name = session_desc.resource_desc.resource_name
            job_run_id = session_desc.session_id
            job_run = exponential_retry(get_glue_job_run, self.GLUE_CLIENT_RETRYABLE_EXCEPTION_LIST, self._glue, job_name, job_run_id)
            # now let's interpret the response in our Compute model
            start = job_run["StartedOn"]
            end = job_run["CompletedOn"] if "CompletedOn" in job_run else None
            execution_details = ComputeExecutionDetails(start, end, dict(job_run))

            session_state = get_glue_job_run_state_type(job_run)
            if session_state == ComputeSessionStateType.FAILED:
                failure_type = get_glue_job_run_failure_type(job_run)
                return ComputeFailedSessionState(failure_type, session_desc, [execution_details])
            elif session_state == ComputeSessionStateType.COMPLETED:
                # STATE CHANGE: Glue Prologue -> CTAS query
                compute_response = self._ctas_compute(
                    active_compute_record.materialized_inputs,
                    active_compute_record.slot,
                    active_compute_record.materialized_output,
                    active_compute_record.execution_context_id,
                    session_desc,
                )
                if compute_response.response_type == ComputeResponseType.SUCCESS:
                    return ComputeSessionState(compute_response.session_desc, ComputeSessionStateType.PROCESSING, [execution_details])
                else:
                    failed_state: ComputeFailedResponse = cast(ComputeFailedResponse, compute_response)
                    if failed_state.failed_response_type == ComputeFailedResponseType.TRANSIENT:
                        # ignore the completion and force PROCESSING,
                        # in the next cycle CTAS state transition will be attempted again
                        return ComputeSessionState(session_desc, ComputeSessionStateType.PROCESSING, [execution_details])
                    else:
                        failure_time = datetime.utcnow()
                        return ComputeFailedSessionState(
                            ComputeFailedSessionStateType.UNKNOWN,
                            session_desc,
                            [execution_details]
                            + [
                                ComputeExecutionDetails(
                                    str(failure_time),
                                    str(failure_time),
                                    {"ErrorMessage": f"Athena CTAS Query execution could not be started! Response: {compute_response!r}"},
                                )
                            ],
                        )

            # 1st stage (Glue Prologue) is still running, return job run session state
            return ComputeSessionState(session_desc, session_state, [execution_details])
        else:
            # STATE: CTAS compute
            return self._get_ctas_session_state(session_desc, active_compute_record)

    def terminate_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        if active_compute_record.session_state.state_type == ComputeSessionStateType.COMPLETED:
            # CTAS EPILOGUE
            # first map materialized output into internal signal form
            output = self.get_platform().storage.map_materialized_signal(active_compute_record.materialized_output)
            path = output.get_materialized_resource_paths()[0]
            path_sep = output.resource_access_spec.path_delimiter()

            # 1- save CTAS schema (CTAS output does not have HEADER), so that downtream nodes will have a chance to
            # reconstruct.
            ctas_table_name = create_output_table_name(
                active_compute_record.materialized_output.alias, active_compute_record.execution_context_id
            )
            schema_file = output.resource_access_spec.data_schema_file
            if schema_file:
                response = get_table_metadata(self._athena, self._database_name, ctas_table_name)
                schema_data = json.dumps(response["TableMetadata"]["Columns"])
                self.get_platform().storage.save(schema_data, [], path.strip(path_sep) + path_sep + schema_file)

            # 2- activate completion, etc
            if output.domain_spec.integrity_check_protocol:
                from intelliflow.core.signal_processing.analysis import INTEGRITY_CHECKER_MAP

                integrity_checker = INTEGRITY_CHECKER_MAP[output.domain_spec.integrity_check_protocol.type]
                completion_resource_name = integrity_checker.get_required_resource_name(
                    output.resource_access_spec, output.domain_spec.integrity_check_protocol
                )
                if completion_resource_name:  # ex: _SUCCESS file/object
                    # TODO evaluate Storage::add_to_internal(signal, sub_folder, data, object_name)
                    #  see 'data_emulation::add_test_data' also.
                    folder = path[path.find(output.resource_access_spec.FOLDER) :]
                    self.get_platform().storage.save("", [folder], completion_resource_name)

        # cleanup no matter what the state is
        self._cleanup_ctas_table(active_compute_record)
        self._cleanup_ctas_prologue_artifacts(active_compute_record)

    def _ctas_compute(
        self,
        materialized_inputs: List[Signal],
        slot: Slot,
        materialized_output: Signal,
        execution_ctx_id: str,
        session_desc: Optional[ComputeSessionDesc] = None,
    ) -> ComputeResponse:
        ctas_query_name: str = f"IntelliFlow_Athena_CTAS_{materialized_output.alias}_{execution_ctx_id}"

        # 0- Clean up the output partition
        #    always try to do this since even after a retry on ctas_compute there might be some partial
        # data left over from previous attempt (so don't check session_desc or existence of resource_path in it).
        output_as_internal = self.get_platform().storage.map_materialized_signal(materialized_output)
        partition_completely_wiped_out = exponential_retry(self.get_platform().storage.delete_internal, [], output_as_internal)
        if not partition_completely_wiped_out:
            # return TRANSIENT error and let the orchestration retry in the next cycle.
            return ComputeFailedResponse(
                ComputeFailedResponseType.TRANSIENT,
                ComputeResourceDesc(ctas_query_name, None, driver=self.__class__),
                "PARTITION CANNOT BE DELETED",
                f"The following output ({materialized_output.alias!r}) partition could not be deleted "
                f"before Athena CTAS query stage: {materialized_output.get_materialized_resource_paths()[0]}",
            )

        user_query = slot.code

        # 1- parametrize the query with user provided params
        #    but more importantly with dynamic dimensions of the output
        #    Example:
        #     select * from athena_input where region_name = {region}
        #       ->
        #     select * from athena_input where region_name =
        extra_params: Dict[str, Any] = dict(slot.extra_params)
        extra_params.update(
            {
                dimension.name: dimension.value
                for dimension in materialized_output.domain_spec.dimension_filter_spec.get_flattened_dimension_map().values()
            }
        )
        if extra_params:
            user_query = user_query.format_map(extra_params)

        # 2- if any inputs, then replace their alias with view names from the prologue stage
        for input_signal in materialized_inputs:
            athena_view_name_from_prologue = create_input_filtered_view_name(input_signal.alias, execution_ctx_id)
            user_query = user_query.replace(input_signal.alias, athena_view_name_from_prologue)

        ctas_table_name = create_output_table_name(materialized_output.alias, execution_ctx_id)

        # 3- turn user query into a CTAS query
        dataset_access_spec = cast(DatasetSignalSourceAccessSpec, materialized_output.resource_access_spec)
        data_format: DatasetSignalSourceFormat = dataset_access_spec.data_format
        data_delimiter: str = dataset_access_spec.data_delimiter
        data_encoding: str = dataset_access_spec.data_encoding
        data_header_exists: bool = dataset_access_spec.data_header_exists
        data_compression: Optional[str] = dataset_access_spec.data_compression
        output_partition_path = materialized_output.get_materialized_resource_paths()[0]
        if not output_partition_path.rstrip().endswith("/"):
            output_partition_path = output_partition_path + "/"

        # Refer
        #  https://docs.aws.amazon.com/athena/latest/ug/create-table-as.html
        ctas_query: str = f"""CREATE TABLE {ctas_table_name}
                          WITH (
                            external_location = '{output_partition_path}'"""
        if data_format == DatasetSignalSourceFormat.PARQUET:
            ctas_query = (
                ctas_query
                + f""",
                    format='PARQUET'"""
            )
            if data_compression:
                ctas_query = (
                    ctas_query
                    + f""",
                    parquet_compression = '{data_compression.upper()}'
                    """
                )
        elif data_format == DatasetSignalSourceFormat.ORC:
            ctas_query = (
                ctas_query
                + f""",
                    format='ORC'"""
            )
            if data_compression:
                ctas_query = (
                    ctas_query
                    + f""",
                    orc_compression = '{data_compression.upper()}'
                    """
                )
        elif data_format == DatasetSignalSourceFormat.CSV:
            ctas_query = (
                ctas_query
                + f""",
                    format='TEXTFILE'"""
            )
            if data_delimiter:
                ctas_query = (
                    ctas_query
                    + f""",
                            field_delimiter = '{data_delimiter}'
                            """
                )
        elif data_format == DatasetSignalSourceFormat.JSON:
            ctas_query = (
                ctas_query
                + f""",
                            format='JSON'"""
            )

        elif data_format == DatasetSignalSourceFormat.AVRO:
            ctas_query = (
                ctas_query
                + f""",
                            format='AVRO'"""
            )
        else:
            # inform user about Athena's default behaviour
            module_logger.critical(
                f"Using PARQUET by default as the output data format for Athena execution" f" output: {output_partition_path!r}."
            )

        # close the WITH block
        ctas_query = (
            ctas_query
            + f"""
            )
            """
        )

        # finally append the tranformed user query
        ctas_query = (
            ctas_query
            + f""" AS
            {user_query}
            """
        )

        if not user_query.rstrip().endswith(";"):
            ctas_query = ctas_query + ";"

        try:
            # make sure that CTAS table is dropped (safeguard against a failure in a previous cleanup)
            query(self._athena, f"DROP TABLE IF EXISTS {ctas_table_name}", self._database_name, self._workgroup_id, wait=True)

            # run the query!
            ctas_execution_id = query(self._athena, ctas_query, self._database_name, self._workgroup_id, wait=False)
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                failed_response_type = ComputeFailedResponseType.TRANSIENT
            else:
                failed_response_type = ComputeFailedResponseType.BAD_SLOT

            return ComputeFailedResponse(
                failed_response_type,
                ComputeResourceDesc(ctas_query_name, ctas_table_name, driver=self.__class__),
                error_code,
                str(error.response["Error"]),
            )

        return ComputeSuccessfulResponse(
            ComputeSuccessfulResponseType.PROCESSING,
            ComputeSessionDesc(ctas_execution_id, ComputeResourceDesc(ctas_query_name, ctas_table_name, driver=self.__class__)),
        )

    def _get_ctas_session_state(
        self, session_desc: ComputeSessionDesc, active_compute_record: RoutingTable.ComputeRecord
    ) -> ComputeSessionState:
        """
        Check the state of CTAS query from _ctas_compute and do a clean up if completion is detected.
        """
        ctas_query_execution_id = session_desc.session_id
        response = exponential_retry(
            self._athena.get_query_execution, ATHENA_CLIENT_RETRYABLE_EXCEPTION_LIST, QueryExecutionId=ctas_query_execution_id
        )
        # state: QUEUED | RUNNING | SUCCEEDED | FAILED | CANCELLED
        status = response["QueryExecution"]["Status"]

        # now let's interpret the response in our Compute model
        start = status.get("SubmissionDateTime", None)
        end = status.get("CompletionDateTime", None)
        execution_details = ComputeExecutionDetails(start, end, dict(response))

        state = status["State"]
        session_state = get_athena_query_execution_state_type(state)
        # dont do cleanups blindly here, do it terminate_session which signals the end of the entire session.
        #  especially in failure case, here we don't know whether orchestration will retry
        #  even with non-transient failure type (based on user retry conf).
        #  so we cannot cleanup prologue artifacts (ctas table cleanup is safe since each compute should recreate it).
        if session_state == ComputeSessionStateType.COMPLETED:
            # See terminate_session
            # self._cleanup_ctas_table(active_compute_record)
            # self._cleanup_ctas_prologue_artifacts(active_compute_record)
            pass
        elif session_state == ComputeSessionStateType.FAILED:
            failure_type = get_athena_query_execution_failure_type(response)
            if failure_type != ComputeFailedSessionStateType.TRANSIENT:
                # See terminate session
                # self._cleanup_ctas_table(active_compute_record)
                # we cannot do this here since we don't know if it is going to be retried (e.g user set retry_count)
                # #self._cleanup_ctas_prologue_artifacts(active_compute_record)
                pass
            return ComputeFailedSessionState(failure_type, session_desc, [execution_details])

        return ComputeSessionState(session_desc, session_state, [execution_details])

    def _cleanup_ctas_table(self, active_compute_record: RoutingTable.ComputeRecord) -> None:
        ctas_table_name = create_output_table_name(
            active_compute_record.materialized_output.alias, active_compute_record.execution_context_id
        )
        query(self._athena, f"DROP TABLE IF EXISTS {ctas_table_name}", self._database_name, self._workgroup_id, wait=False)

    def _cleanup_ctas_prologue_artifacts(self, active_compute_record: RoutingTable.ComputeRecord) -> None:
        execution_ctx_id = active_compute_record.execution_context_id

        for input_signal in active_compute_record.materialized_inputs:
            athena_table_name_from_prologue = create_input_table_name(input_signal.alias, execution_ctx_id)
            query(
                self._athena, f"DROP TABLE IF EXISTS {athena_table_name_from_prologue}", self._database_name, self._workgroup_id, wait=False
            )
            athena_view_name_from_prologue = create_input_filtered_view_name(input_signal.alias, execution_ctx_id)
            query(
                self._athena, f"DROP VIEW IF EXISTS {athena_view_name_from_prologue}", self._database_name, self._workgroup_id, wait=False
            )

    @classmethod
    def _build_database_name(cls, params: ConstructParamsDict):
        return to_athena_format(params[ActivationParams.UNIQUE_ID_FOR_CONTEXT])

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)

        # OPTIMIZATION: This impl relies on S3 based Storage for seamless integration other upstream/downstream
        # internal data.
        storage_path: str = platform.storage.get_storage_resource_path()
        if not (storage_path.startswith("arn:aws:s3") or storage_path.startswith("s3:")):
            raise TypeError(f"{self.__class__.__name__} driver should be used with an S3 based Storage!")

        self._database_name = self._build_database_name(self._params)
        # Refer
        #  https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html
        # Database, Table, and Column Names
        # When you create schema in AWS Glue to query in Athena, consider the following:
        #   - A database name cannot be longer than 252 characters.
        #   - A table name cannot be longer than 255 characters.
        #   - A column name cannot be longer than 128 characters.
        # The only acceptable characters for database names, table names, and column names are lowercase letters, numbers, and the underscore character.
        if len(self._database_name) > 252:
            raise ValueError(f"Long application name {platform.context_id!r} caused internal Athena database name to exceed 252!")

        # workgroup primitives
        self._workgroup_id = self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]
        internal_data_bucket_name = self.get_platform().storage.bucket_name
        self._output_location = f"s3://{internal_data_bucket_name}/driver_data/{self.__class__.__name__}/query_location/"

        # CTAS prologue Glue job
        self._glue_job_lang_map: Dict[GlueJobLanguage, Dict[str, Dict[str, Any]]] = {
            GlueJobLanguage.PYTHON: {
                "2.0": {
                    "job_name": "",
                    "job_arn": "",
                    "boilerplate": GlueAthenaCTASPrologue,
                    "suffix": "v2_0",
                    "ext": "py",
                    "params": {
                        "WorkerType": GlueWorkerType.G_1X.value,
                        "NumberOfWorkers": 20,  # heuristical: optimal # of instances to read 'schema' operation in Spark.
                        "GlueVersion": "2.0",  # FUTURE analyze advantages of 3.0 over 2.0 as our athena prologue
                    },
                }
            }
        }
        for lang, lang_spec in self._glue_job_lang_map.items():
            for version, version_spec in lang_spec.items():
                boilerplate_module = version_spec["boilerplate"]
                version_suffix = version_spec["suffix"]
                job_name: str = self.GLUE_JOB_NAME_FORMAT.format(
                    self.__class__.__name__, boilerplate_module.__name__, self._dev_platform.context_id, version_suffix, self._region
                )
                if len(job_name) > 255:
                    raise ValueError(
                        f"Cannot dev_init {self.__class__.__name__} due to very long"
                        f" AWS Glue Job Name {job_name} (limit < 255),"
                        f" as a result of very long context_id '{self._dev_platform.context_id}'."
                    )

                self._glue_job_lang_map[lang][version].update({"job_name": job_name})

                self._glue_job_lang_map[lang][version].update({"job_arn": f"arn:aws:glue:{self._region}:{self._account_id}:job/{job_name}"})

        self._intelliflow_python_workingset_key = build_object_key(["driver_data", self.__class__.__name__, "glue"], "bundle.zip")

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        """Whole platform got bootstrapped at runtime. For other runtime services, this
        construct should be initialized (ex: context_owner: Processor [Lambda], etc)"""
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._athena = boto3.client("athena", region_name=self._region)
        self._glue = boto3.client("glue", region_name=self._region)

    def provide_runtime_trusted_entities(self) -> List[str]:
        return ["athena.amazonaws.com"]

    def provide_runtime_default_policies(self) -> List[str]:
        # For testing purposes add;
        #   arn:aws:iam::aws:policy/service-role/AmazonAthenaFullAccess
        return ["service-role/AWSGlueServiceRole"]  # Glue required for Prologue job

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        # allow exec-role (post-activation, cumulative list of all trusted entities [AWS services]) to do the following;
        # Based on:
        #  https://docs.aws.amazon.com/athena/latest/ug/managed-policies.html

        # If there is no <Route> detected (see hook_internal) for this driver,
        # then no need to provide extra permissions.
        # If they were added previously, they will be wiped out in the current activation.
        if not self._pending_internal_routes:
            return []

        internal_data_bucket_name = self.get_platform().storage.bucket_name
        permissions = [
            # Athena specific permissions for output location since this impl relies on Storage
            ConstructPermission(
                [
                    f"arn:aws:s3:::{internal_data_bucket_name}",
                    f"arn:aws:s3:::{internal_data_bucket_name}/{InternalDatasetSignalSourceAccessSpec.FOLDER}/*",
                    # we don't use the default (auto-generated) bucket, but in case the user tries to use console
                    # to manually trigger something within the same workgroup.
                    "arn:aws:s3:::aws-athena-query-results-*",
                ],
                [
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:ListMultipartUploadParts",
                    "s3:AbortMultipartUpload",
                    "s3:CreateBucket",
                    "s3:PutObject",
                    "s3:PutBucketPublicAccessBlock",
                ],
            ),
            ConstructPermission(
                [
                    f"arn:aws:glue:{self._region}:{self._account_id}:catalog",
                    f"arn:aws:glue:{self._region}:{self._account_id}:database/{self._database_name}",
                    f"arn:aws:glue:{self._region}:{self._account_id}:table/{self._database_name}/*",
                    f"arn:aws:glue:{self._region}:{self._account_id}:userDefinedFunction/{self._database_name}/*",
                ],
                [
                    # TODO remove Get* actions since in the subsequent permission, we allow EXEC ROLE to get from
                    # all of the entities in the same catalog.
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateTable",
                    "glue:DeleteTable",
                    "glue:BatchDeleteTable",
                    "glue:UpdateTable",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:BatchCreatePartition",
                    "glue:CreatePartition",
                    "glue:DeletePartition",
                    "glue:BatchDeletePartition",
                    "glue:UpdatePartition",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                    "glue:CreateUserDefinedFunction",
                    "glue:UpdateUserDefinedFunction",
                    "glue:DeleteUserDefinedFunction",
                    "glue:GetUserDefinedFunction",
                    "glue:GetUserDefinedFunctions",
                ],
            ),
            ConstructPermission(
                [
                    # TODO specify
                    #  - databases and tables from external S3 signals with GlueTable Proxy
                    f"arn:aws:glue:{self._region}:{self._account_id}:catalog",
                    f"arn:aws:glue:{self._region}:{self._account_id}:database/*",
                    f"arn:aws:glue:{self._region}:{self._account_id}:table/*/*",
                    f"arn:aws:glue:{self._region}:{self._account_id}:userDefinedFunction/*/*",
                ],
                [
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                    "glue:GetUserDefinedFunction",
                    "glue:GetUserDefinedFunctions",
                ],
            ),
            # can do anything on its own CTAS Prologue Glue Job
            ConstructPermission(
                [
                    f"arn:aws:glue:{self._region}:{self._account_id}:job/{self._glue_job_lang_map[lang][version]['job_name']}"
                    for lang in self._glue_job_lang_map.keys()
                    for version in self._glue_job_lang_map[lang].keys()
                ],
                ["glue:*"],
            ),
            # Refer
            #  https://docs.aws.amazon.com/athena/latest/ug/example-policies-workgroup.html
            # for workgroup based policy (for our EXEC role)
            # Modified version of 'Example Policy for Running Queries in a Specified Workgroup'
            ConstructPermission(
                ["*"],
                [
                    "athena:ListWorkGroups",
                    "athena:GetExecutionEngine",
                    "athena:GetExecutionEngines",
                    "athena:GetCatalogs",
                    "athena:GetNamespace",
                    "athena:GetNamespaces",
                    "athena:GetTables",
                    "athena:GetTable",
                    "athena:GetTableMetadata",
                ],
            ),
            ConstructPermission(
                [
                    f"arn:aws:athena:{self._region}:{self._account_id}:workgroup/{self._workgroup_id}",
                    f"arn:aws:athena:{self._region}:{self._account_id}:workgroup/primary",
                ],
                [
                    "athena:StartQueryExecution",
                    "athena:StopQueryExecution",
                    "athena:CancelQueryExecution",
                    "athena:RunQuery",
                    "athena:GetQueryResults",
                    "athena:DeleteNamedQuery",
                    "athena:GetNamedQuery",
                    "athena:ListQueryExecutions",
                    "athena:GetQueryResultsStream",
                    "athena:ListNamedQueries",
                    "athena:CreateNamedQuery",
                    "athena:GetQueryExecution",
                    "athena:GetQueryExecutions",
                    "athena:BatchGetNamedQuery",
                    "athena:BatchGetQueryExecution",
                    "athena:GetWorkGroup",
                ],
            ),
            # CW Logs (might look redundant, but please forget about other drivers while declaring these),
            # deduping is handled automatically.
            ConstructPermission([f"arn:aws:logs:{self._region}:{self._account_id}:*"], ["logs:*"]),
            # must add a policy to allow your users the iam:PassRole permission for IAM roles to match your naming convention
            ConstructPermission([self._params[AWSCommonParams.IF_EXE_ROLE]], ["iam:PassRole"]),
        ]

        for route in self._pending_internal_routes:
            for slot in route.slots:
                if slot.type.is_batch_compute() and slot.permissions:
                    for compute_perm in slot.permissions:
                        # Just provide permission flexibility despite that this should not be supported with Athena.
                        if compute_perm.context != PermissionContext.DEVTIME:
                            permissions.append(ConstructPermission(compute_perm.resource, compute_perm.action))

        # might look familiar (from Processor impl maybe), but please forget about other drivers while declaring these),
        # deduping is handled automatically.
        ext_s3_signals = [
            ext_signal for ext_signal in self._pending_external_signals if ext_signal.resource_access_spec.source == SignalSourceType.S3
        ]
        if ext_s3_signals:
            # External S3 access
            permissions.append(
                ConstructPermission(
                    [
                        f"arn:aws:s3:::{ext_signal.resource_access_spec.bucket}{'/' + ext_signal.resource_access_spec.folder if ext_signal.resource_access_spec.folder else ''}/*"
                        for ext_signal in ext_s3_signals
                    ]
                    + [
                        f"arn:aws:s3:::{ext_signal.resource_access_spec.bucket}/{ext_signal.resource_access_spec.folder if ext_signal.resource_access_spec.folder else ''}"
                        for ext_signal in ext_s3_signals
                    ],
                    ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket"],
                )
            )

            encryption_key_list: Set[str] = {
                ext_signal.resource_access_spec.encryption_key
                for ext_signal in ext_s3_signals
                if ext_signal.resource_access_spec.encryption_key
            }

            if encryption_key_list:
                permissions.append(
                    ConstructPermission(
                        list(encryption_key_list),
                        [
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:GenerateDataKey",
                            "kms:DescribeCustomKeyStores",
                            "kms:ListKeys",
                            "kms:ListAliases",
                        ],
                    )
                )

        return permissions

    @classmethod
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        # dev-role permissions (things this construct would do during development)
        # dev-role should be able to do the following.
        database_name: str = cls._build_database_name(params)
        workgroup_id = params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]
        return [
            # TODO after Athena driver MVP
            #   - use params to narrow down resources,
            #   - move glue actions (from runtime here) and remove database related actions from runtime.
            # Refer
            #  https://docs.aws.amazon.com/athena/latest/ug/fine-grained-access-to-glue-resources.html
            ConstructPermission(
                [
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:catalog",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:database/*",
                ],
                ["glue:GetDatabases"],
            ),
            ConstructPermission(
                [
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:catalog",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:database/default",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:database/{database_name}",
                ],
                ["glue:CreateDatabase", "glue:GetDatabase"],
            ),
            ConstructPermission(
                [
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:catalog",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:database/{database_name}",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:table/{database_name}/*",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:userDefinedFunction/{database_name}/*",
                ],
                [
                    "glue:DeleteDatabase",
                    "glue:GetDatabase",
                    "glue:UpdateDatabase",
                    "glue:CreateTable",
                    "glue:DeleteTable",
                    "glue:BatchDeleteTable",
                    "glue:UpdateTable",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:BatchCreatePartition",
                    "glue:CreatePartition",
                    "glue:DeletePartition",
                    "glue:BatchDeletePartition",
                    "glue:UpdatePartition",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                    "glue:GetUserDefinedFunction",
                    "glue:GetUserDefinedFunctions",
                    "glue:UpdateUserDefinedFunction",
                    "glue:DeleteUserDefinedFunction",
                ],
            ),
            # Read-access into everything else in the same catalog
            # Refer
            #   https://docs.aws.amazon.com/glue/latest/dg/glue-specifying-resource-arns.html
            ConstructPermission(
                [
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:catalog",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:database/*",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:table/*/*",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:userDefinedFunction/*/*",
                ],
                [
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                    "glue:Get*",
                    "glue:BatchGet*",
                ],
            ),
            ConstructPermission(
                ["*"],
                [
                    "athena:ListWorkGroups",
                    "athena:GetExecutionEngine",
                    "athena:GetExecutionEngines",
                    "athena:GetCatalogs",
                    "athena:GetNamespace",
                    "athena:GetNamespaces",
                    "athena:GetTables",
                    "athena:GetTable",
                    "athena:GetTableMetadata",
                ],
            ),
            ConstructPermission(
                [f"arn:aws:athena:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:workgroup/{workgroup_id}"],
                ["athena:CreateWorkGroup", "athena:UpdateWorkGroup", "athena:DeleteWorkGroup", "athena:TagResource"],
            ),
            ConstructPermission(
                [
                    f"arn:aws:athena:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:workgroup/{workgroup_id}",
                    f"arn:aws:athena:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:workgroup/primary",
                ],
                [
                    "athena:StartQueryExecution",
                    "athena:StopQueryExecution",
                    "athena:CancelQueryExecution",
                    "athena:RunQuery",
                    "athena:GetQueryResults",
                    "athena:DeleteNamedQuery",
                    "athena:GetNamedQuery",
                    "athena:ListQueryExecutions",
                    "athena:GetQueryResultsStream",
                    "athena:ListNamedQueries",
                    "athena:CreateNamedQuery",
                    "athena:GetQueryExecution",
                    "athena:GetQueryExecutions",
                    "athena:BatchGetNamedQuery",
                    "athena:BatchGetQueryExecution",
                    "athena:GetWorkGroup",
                ],
            ),
            # and finally: full-authorization on activation and (local) compute time permissions (on its own resources)
            ConstructPermission(
                [
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:job/{cls.GLUE_JOB_NAME_FORMAT.format(cls.__name__, '*', '*', '*', params[AWSCommonParams.REGION])}"
                ],
                ["glue:*"],
            ),
        ]

    def _provide_route_metrics(self, route: Route) -> List[ConstructInternalMetricDesc]:
        # TODO
        return []

    def _provide_internal_metrics(self) -> List[ConstructInternalMetricDesc]:
        """Provide internal metrics (of type INTERNAL_METRIC) that should be managed by RheocerOS and emitted by this
        driver via Diagnostics::emit.
        These metrics are logical metrics generated by the driver (with no assumption on other drivers and other details
        about the underlying platform). So as a driver impl, you want Diagnostics driver to manage those metrics and
        bind them to alarms, etc. Example: Routing metrics.
        """
        return []

    def _provide_internal_alarms(self) -> List[Signal]:
        """Provide internal alarms (of type INTERNAL_ALARM OR INTERNAL_COMPOSITE_ALARM) managed/emitted
        by this driver impl"""
        return []

    # overrides
    def _provide_system_metrics(self) -> List[Signal]:
        """Expose system generated metrics to the rest of the platform in a consolidated, filtered and
        well-defined RheocerOS metric signal format.
        """
        # TODO
        return []

    def build_bootstrapper_object_key(self) -> str:
        return build_object_key(
            ["driver_data", self.__class__.__name__, "glue", "bootstrapper"], f"{self.__class__.__name__.lower()}_RuntimePlatform.data"
        )

    def _update_bootstrapper(self, bootstrapper: "RuntimePlatform") -> None:
        # Use bootstrapper for CTAS Prologue Glue job.
        # uploading it to S3 and passing S3 link as job arg.
        bootstrapped_platform = bootstrapper.serialize()

        bootstrapper_object_key = self.build_bootstrapper_object_key()
        exponential_retry(
            put_object,
            {"ServiceException", "TooManyRequestsException"},
            self.get_platform().storage._bucket,
            bootstrapper_object_key,
            bootstrapped_platform,
        )

    def activate(self) -> None:
        # this driver uses smart activation by analyzing the existence of Athena computes and their changeset
        # in '_process_internal' callback

        # 1- Create/Update Workgroup
        create_or_update_workgroup(
            self._athena,
            self._workgroup_id,
            self._output_location,
            enforce=False,  # due to dynamic and full path use of 'external_location' in CTAS based compute
            description=f"Athena Workgroup for RheocerOS app {self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]!r}",
            **{"context_id": self.get_platform().context_id},
        )

        # 2- check the DB
        storage_bucket = self.get_platform().storage.bucket_name
        query(
            self._athena,
            f"CREATE DATABASE IF NOT EXISTS {self._database_name} LOCATION 's3://{storage_bucket}/driver_data/{self.__class__.__name__}/{self._database_name}/'",
            self._database_name,
            self._workgroup_id,
            wait=True,
            poll_interval_in_secs=3,
        )

        # 3- provision CTAS Prologue job
        if not is_environment_immutable():
            for lang, lang_spec in self._glue_job_lang_map.items():
                working_set_s3_location = None
                if lang == GlueJobLanguage.PYTHON:
                    # Upload the bundle (working set) to its own bucket.
                    exponential_retry(
                        put_object,
                        {"ServiceException", "TooManyRequestsException"},
                        self.get_platform().storage._bucket,
                        self._intelliflow_python_workingset_key,
                        get_working_set_as_zip_stream(extra_folders_to_avoid=PYTHON_MODULES_TO_BE_AVOIDED_IN_GLUE_BUNDLE),
                    )

                    working_set_s3_location = f"s3://{storage_bucket}/{self._intelliflow_python_workingset_key}"
                for version, version_spec in lang_spec.items():
                    batch = version_spec["boilerplate"]()
                    file_ext = version_spec["ext"]
                    batch_script_file_key = build_object_key(
                        ["driver_data", self.__class__.__name__, "glue"], f"glueetl_{batch.__class__.__name__.lower()}.{file_ext}"
                    )
                    exponential_retry(
                        put_object,
                        {"ServiceException", "TooManyRequestsException"},
                        self.get_platform().storage._bucket,
                        batch_script_file_key,
                        batch.generate_glue_script().encode("utf-8"),
                    )

                    lang_abi_job_name = version_spec["job_name"]
                    job_name = exponential_retry(get_glue_job, self.GLUE_CLIENT_RETRYABLE_EXCEPTION_LIST, self._glue, lang_abi_job_name)

                    if lang == GlueJobLanguage.PYTHON:
                        # it will be uploaded within "_update_bootstrapper"
                        # only available in GlueJobLanguage.PYTHON, so in Scala 'RheocerOS' runtime and
                        # active platform object (drivers) are not available.
                        default_args = {f"--{BOOTSTRAPPER_PLATFORM_KEY_PARAM}": self.build_bootstrapper_object_key()}
                    elif lang == GlueJobLanguage.SCALA:
                        default_args = {f"--class": batch.CLASS_NAME}

                    description = f"RheocerOS {lang.value}, Glue Version {version} batch-compute driver for the application {self._dev_platform.context_id}"
                    create_or_update_func = create_glue_job if not job_name else update_glue_job
                    exponential_retry(
                        create_or_update_func,
                        self.GLUE_CLIENT_RETRYABLE_EXCEPTION_LIST.union("AccessDeniedException"),
                        self._glue,
                        lang_abi_job_name,
                        description,
                        self._params[AWSCommonParams.IF_EXE_ROLE],
                        GlueJobCommandType.BATCH,
                        lang,
                        f"s3://{storage_bucket}/{batch_script_file_key}",
                        glue_version=version,
                        working_set_s3_location=working_set_s3_location,
                        default_args=default_args,
                    )

        super().activate()

    def rollback(self) -> None:
        # roll back activation, something bad has happened (probably in another Construct) during app launch
        super().rollback()
        # TODO

    def terminate(self) -> None:
        """Designed to be resilient against repetitive calls in case of retries in the high-level
        termination work-flow.
        """
        # Delete CTAS Prologue Glue jobs
        for lang, lang_spec in self._glue_job_lang_map.items():
            for version, version_spec in lang_spec.items():
                lang_abi_job_name = version_spec["job_name"]
                exponential_retry(delete_glue_job, self.GLUE_CLIENT_RETRYABLE_EXCEPTION_LIST, self._glue, lang_abi_job_name)

        # Wipe Database (any left-over [zombie] execution views, tables)
        # https://docs.aws.amazon.com/athena/latest/ug/drop-database.html
        # Use cascade to wipe residual tables
        try:
            query(
                self._athena,
                f"DROP DATABASE IF EXISTS {self._database_name} CASCADE",
                self._database_name,
                self._workgroup_id,
                wait=True,
                poll_interval_in_secs=3,
            )
        except ClientError as error:
            if get_code_for_exception(error) not in ["InvalidRequestException"]:  # workgroup_id does not exist
                raise error

        # delete database metadata files
        metadata_prefix = f"driver_data/{self.__class__.__name__}"
        delete_folder(self.get_platform().storage._bucket, metadata_prefix)

        # delete the workgroup
        delete_workgroup(self._athena, self._workgroup_id)

        self._database_name = None
        self._workgroup_id = None
        self._output_location = None

        super().terminate()

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)

    def hook_internal(self, route: Route) -> None:
        """Early stage check on a new route, so that we can fail fast before the whole activation.
        RheocerOS guarantees that <Route>s depending on this driver will be passed via this callback.
        So the driver can raise with no concern as to composite use of this driver along with other
        BatchCompute drivers."""
        super().hook_internal(route)
        if len([slot.type == SlotType.ASYNC_BATCH_COMPUTE for slot in route.slots]) > 1:
            raise ValueError(f"Multiple batch compute assigned to route {route.route_id!r}!")

        for slot in route.slots:
            if (
                slot.type == SlotType.ASYNC_BATCH_COMPUTE and slot.code_lang == Lang.PRESTO_SQL
            ):  # enough condition to make sure that this slot will be owned by this driver.
                code_metadata = slot.code_metadata
                if code_metadata.code_type != SlotCodeType.EMBEDDED_SCRIPT:
                    raise NotImplementedError(f"Code script type {code_metadata.code_type!r} is not supported!")

                # Validate early (during dev-time, before the activation) if input alias' conform to Athena expectations.
                # An input alias is used during execution as part of the auto-created external table/view that represents
                # the materialized view of the input within the CTAS query for the actual code.
                # Here we are applying the validation to all signal types (not only datasets) since we have
                # a future plan to support all of the input signals as queryable temp lookup views/tables, yes
                # even the timers, notifications (which have 'time' dimension similar to timers), alarms, etc (as virtual tables with one data row).
                dummy_exec_id = str(uuid.uuid4())
                for signal in route.link_node.signals:
                    # this raise ValueError if alias has a bad format (bad chars). check the impl to understand more.
                    as_exec_table_name = create_input_filtered_view_name(signal.alias, dummy_exec_id)
                    # Refer https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html
                    if len(as_exec_table_name) > 255:
                        raise ValueError(
                            f"Size of input alias {signal.alias!r} for route {route.route_id!r} "
                            f"is exceeding RheocerOS allowed limit by {len(as_exec_table_name) - 255}!"
                        )

                allowed_query_str_length = ATHENA_MAX_QUERY_STRING_LENGTH_IN_BYTES - (
                    len(route.link_node.signals) * INTELLIFLOW_ATHENA_INPUT_VIEW_LENGTH
                )
                query_str_length = len(slot.code.encode("utf-8"))
                if query_str_length > allowed_query_str_length:
                    raise ValueError(
                        f"PrestoSQL query string length for Athena based compute is too long (by extra {(query_str_length - allowed_query_str_length)!r} bytes)!"
                        f" Route/Node ID: {route.route_id!r}\n"
                        f" Slot code: '{slot.code[:100]} ... <abbreviated>'"
                    )

    def hook_external(self, signals: List[Signal]) -> None:
        # hook early during app activation so that we can fail fast, without letting
        # whole platform (and other constructs) activate themselves and cause a more
        # complicated rollback.
        # RheocerOS guarantees that external signals that will be used by this driver are passed in.
        super().hook_external(signals)

        if any(
            s.resource_access_spec.source
            not in [
                SignalSourceType.S3,
                SignalSourceType.GLUE_TABLE,
                # FUTURE support non-data external signals as (auto-generated) virtual tables (views) as well.
                SignalSourceType.CW_ALARM,
                SignalSourceType.CW_COMPOSITE_ALARM,
                SignalSourceType.CW_METRIC,
                # notifications
                SignalSourceType.SNS,
            ]
            for s in signals
        ):
            raise NotImplementedError(
                f"External signal source type for one of " f"'{signals!r} is not supported by {self.__class__.__name__}'"
            )

        for s in signals:
            if s.resource_access_spec.source == SignalSourceType.GLUE_TABLE:
                glue_access_spec: GlueTableSignalSourceAccessSpec = cast("GlueTableSignalSourceAccessSpec", s.resource_access_spec)
                if not glue_catalog.check_table(self._session, self._region, glue_access_spec.database, glue_access_spec.table_name):
                    raise ValueError(
                        f"Either the database ({glue_access_spec.database!r}) or table ({glue_access_spec.table_name!r}) "
                        f" could not be found in Glue catalog (account: {self._account_id}, region: {self._region})."
                    )
            elif s.resource_access_spec.source == SignalSourceType.S3:
                key: str = s.resource_access_spec.attrs.get("encryption_key", None)
                if key and not key.startswith("arn:aws:kms"):
                    # TODO not %100 sure about this, check Athena table properties for supported encryption schemes.
                    raise NotImplementedError(
                        f"{key} is not currently supported. {self.__class__.__name__}" f" currently supports KMS key arns."
                    )
                # If has Glue table has a proxy
                if s.resource_access_spec.proxy and s.resource_access_spec.proxy.source == SignalSourceType.GLUE_TABLE:
                    glue_access_spec: GlueTableSignalSourceAccessSpec = cast(
                        "GlueTableSignalSourceAccessSpec", s.resource_access_spec.proxy
                    )
                    if not glue_catalog.check_table(self._session, self._region, glue_access_spec.database, glue_access_spec.table_name):
                        raise ValueError(
                            f"Either the database ({glue_access_spec.database!r}) or the table ({glue_access_spec.table_name!r}) "
                            f" could not be found in Glue catalog (account: {self._account_id}, region: {self._region})."
                        )

    def hook_internal_signal(self, signal: "Signal") -> None:
        # currently not interested in doing any checks on other signal types (such as TIMER_EVENT)
        # FUTURE support internal signals as (auto-generated) virtual tables (views) as well.
        pass

    def hook_security_conf(
        self, security_conf: ConstructSecurityConf, platform_security_conf: Dict[Type["BaseConstruct"], ConstructSecurityConf]
    ) -> None:
        if security_conf:
            raise NotImplementedError(f"Extra security for {self.__class__.__name__} is not supported.")

        storage_security_conf = platform_security_conf.get(Storage, None)
        if (
            storage_security_conf
            and storage_security_conf.persisting.encryption.key_allocation_level == EncryptionKeyAllocationLevel.PER_RESOURCE
        ):
            # TODO support via workgroup encryption configuration
            raise NotImplementedError(f"Security/Encryption/Resource level encryption for {self.__class__.__name__} not supported.")

        super().hook_security_conf(security_conf, platform_security_conf)

    def _process_security_conf(self, new_security_conf: ConstructSecurityConf, current_security_conf: ConstructSecurityConf) -> None:
        pass

    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        # This driver relies on the utilization of execution based temporal:
        #  - auto view generation (implicit schema extraction) for inputs (internal or external)
        #  - CTAS for actual ETL
        #
        # This is completely managed in 'compute' and 'get_session_state' (or completion [success/failure] execution hooks)
        #
        # So we don't need changeset management here, since under circumstances when all executions are done
        # no artifacts should be found in app database.
        pass

    def _process_internal_signals(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_construct_connections(
        self, new_construct_conns: Set["_PendingConnRequest"], current_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _revert_external(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        pass

    def _revert_internal(self, routes: Set[Route], prev_routes: Set[Route]) -> None:
        pass

    def _revert_internal_signals(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        pass

    def _revert_construct_connections(
        self, construct_conns: Set["_PendingConnRequest"], prev_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _revert_security_conf(selfs, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        pass

    # overrides
    def describe_compute_record(self, active_compute_record: "RoutingTable.ComputeRecord") -> Optional[Dict[str, Any]]:
        execution_details = dict()
        if active_compute_record.session_state and active_compute_record.session_state.executions:
            final_execution_details = active_compute_record.session_state.executions[::-1][0].details
            if "QueryExecution" in final_execution_details and final_execution_details["QueryExecution"]:
                execution_details["details"] = final_execution_details["QueryExecution"]
                query_execution_id = execution_details["details"]["QueryExecutionId"]
                execution_details["details"][
                    "QueryEditorURL"
                ] = f"https://{self.region}.console.aws.amazon.com/athena/home?region={self.region}#/query-editor/history/{query_execution_id}"

        # Extract SLOT info
        if active_compute_record.slot:
            slot = dict()
            slot["type"] = active_compute_record.slot.type
            slot["lang"] = active_compute_record.slot.code_lang
            slot["code"] = active_compute_record.slot.code
            slot["code_abi"] = active_compute_record.slot.code_abi

            execution_details.update({"slot": slot})

        return execution_details

    # overrides
    def get_compute_record_logs(
        self,
        compute_record: "RoutingTable.ComputeRecord",
        error_only: bool = True,
        filter_pattern: Optional[str] = None,
        time_range: Optional[Tuple[int, int]] = None,
        limit: Optional[int] = None,
        next_token: Optional[str] = None,
    ) -> Optional[ComputeLogQuery]:
        if compute_record.session_state:
            if compute_record.session_state.executions:
                final_execution_details = compute_record.session_state.executions[::-1][0].details
                if final_execution_details:
                    query_execution_id = final_execution_details["QueryExecutionId"]

                    output_url = f"https://{self.region}.console.aws.amazon.com/athena/home?region={self.region}#/query-editor/history/{query_execution_id}"
                    return ComputeLogQuery(None, [output_url])
