# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Constructs that provide big-data workload execution abstraction to a Processor,
achieving a simple RPC dispatch, micro-service feel from an integration point of view.
 
They generally constitute other AWS resources to control request-buffering, de-duping, execution status check and other integration
related aspects. Request-buffering would be required to abstract other Platform components (aka the Processor) from
the internal details
"""
import json
import logging
import uuid
from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple, Type, cast

import boto3
from botocore.exceptions import ClientError

from intelliflow.core.deployment import get_working_set_as_zip_stream, is_environment_immutable
from intelliflow.core.permission import PermissionContext
from intelliflow.core.platform.definitions.aws.glue.script.batch.glueetl_scala_all_ABI import GlueAllABIScala
from intelliflow.core.signal_processing import DimensionFilter, DimensionSpec, Signal, Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.definitions.dimension_defs import Type as DimensionType
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import (
    DATASET_HEADER_KEY,
    DATASET_SCHEMA_TYPE_KEY,
    CWMetricSignalSourceAccessSpec,
    DatasetSchemaType,
    GlueTableSignalSourceAccessSpec,
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
    Storage,
)
from ...definitions.aws.common import AWS_COMMON_RETRYABLE_ERRORS, MAX_SLEEP_INTERVAL_PARAM
from ...definitions.aws.common import CommonParams as AWSCommonParams
from ...definitions.aws.common import exponential_retry
from ...definitions.aws.glue import catalog as glue_catalog
from ...definitions.aws.glue.catalog import check_table, query_table_spec
from ...definitions.aws.glue.client_wrapper import (
    JOB_ARN_FORMAT,
    PYTHON_MODULES_TO_BE_AVOIDED_IN_GLUE_BUNDLE,
    GlueJobCommandType,
    GlueJobLanguage,
    GlueVersion,
    create_glue_job,
    delete_glue_job,
    evaluate_execution_params,
    get_bundles,
    get_glue_job,
    get_glue_job_run,
    get_glue_job_run_failure_type,
    get_glue_job_run_state_type,
    start_glue_job,
    update_glue_job,
)
from ...definitions.aws.glue.script.batch.common import (
    AWS_REGION,
    BOOTSTRAPPER_PLATFORM_KEY_PARAM,
    CLIENT_CODE_BUCKET,
    CLIENT_CODE_PARAM,
    INPUT_MAP_PARAM,
    OUTPUT_PARAM,
    USER_EXTRA_PARAMS_PARAM,
    BatchInputMap,
    BatchOutput,
)
from ...definitions.aws.glue.script.batch.glueetl_default_ABI import GlueDefaultABIPython
from ...definitions.aws.s3.bucket_wrapper import MAX_BUCKET_LEN, bucket_exists, create_bucket, delete_bucket, get_bucket, put_policy
from ...definitions.aws.s3.object_wrapper import build_object_key, empty_bucket, object_exists, put_object
from ...definitions.compute import (
    ComputeExecutionDetails,
    ComputeFailedResponse,
    ComputeFailedResponseType,
    ComputeFailedSessionState,
    ComputeFailedSessionStateType,
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


class AWSGlueBatchComputeBasic(AWSConstructMixin, BatchCompute):
    """AWS Glue based BatchCompute impl that provides an easy-to-implement (and easy-to-manage)
     big-data workload execution abstraction to a Platform (to its Processor particularly).

    Requires no high-level Cluster life-cycle management, orchestration and concurrent execution control for RheocerOS'
    sequential or parallel branches.
    """

    GLUE_JOB_NAME_FORMAT: ClassVar[str] = "IntelliFlow-{0}-{1}-{2}-{3}"
    SCRIPTS_ROOT_FORMAT: ClassVar[str] = "if-{0}-{1}-{2}-{3}"
    CLIENT_RETRYABLE_EXCEPTION_LIST: Set[str] = {"ConcurrentRunsExceededException", "OperationTimeoutException", "InternalServiceException"}
    GLUE_DEFAULT_VERSION: ClassVar[GlueVersion] = GlueVersion.VERSION_2_0

    @classmethod
    def driver_spec(cls) -> DimensionFilter:
        return DimensionFilter.load_raw(
            {
                Lang.PYTHON: {ABI.GLUE_EMBEDDED: {"GlueVersion": {GlueVersion.AUTO.value: {}, "1.0": {}, "2.0": {}, "3.0": {}}}},
                Lang.SCALA: {ABI.GLUE_EMBEDDED: {"GlueVersion": {GlueVersion.AUTO.value: {}, "1.0": {}, "2.0": {}, "3.0": {}}}},
            }
        )

    def __init__(self, params: ConstructParamsDict) -> None:
        """Called the first time this construct is added/configured within a platform.

        Subsequent sessions maintain the state of a construct, so the following init
        operations occur in the very beginning of a construct's life-cycle within an app.
        """
        super().__init__(params)
        self._glue = self._session.client("glue", region_name=self._region)
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._bucket = None
        self._bucket_name = None
        self._intelliflow_python_workingset_key = None

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._glue = self._session.client("glue", region_name=self._region)
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._bucket = get_bucket(self._s3, self._bucket_name)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._glue = None
        self._s3 = None
        self._bucket = None

    def provide_output_attributes(self, slot: Slot, user_attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # header: supports both so it is up to user input. but default to True if not set.
        return {DATASET_HEADER_KEY: user_attrs.get(DATASET_HEADER_KEY, True), DATASET_SCHEMA_TYPE_KEY: DatasetSchemaType.SPARK_SCHEMA_JSON}

    def query_external_source_spec(
        self, ext_signal_source: SignalSourceAccessSpec
    ) -> Optional[Tuple[SignalSourceAccessSpec, DimensionSpec]]:
        if ext_signal_source.source == SignalSourceType.GLUE_TABLE:
            return query_table_spec(ext_signal_source.database, ext_signal_source.table_name)

        raise NotImplementedError(
            f"This external signal source ({ext_signal_source.source!r}) cannot be queried"
            f" by BatchCompute driver: {self.__class__.__name__}"
        )

    def compute(
        self,
        materialized_inputs: List[Signal],
        slot: Slot,
        materialized_output: Signal,
        execution_ctx_id: str,
        retry_session_desc: Optional[ComputeSessionDesc] = None,
    ) -> ComputeResponse:
        """Find the respective job for the slot based on
        - Lang
        - JobCommandType (ignore since this construct is BATCH specific)
        - ABI ( TODO post-MVP)
        """
        code_metadata: SlotCodeMetadata = slot.code_metadata
        lang = GlueJobLanguage.from_slot_lang(slot.code_lang)
        extra_params: Dict[str, Any] = dict(slot.extra_params)
        self._resolve_glue_version(extra_params, materialized_inputs)
        user_extra_param_keys = list(set(extra_params.keys()))
        glue_version = extra_params.get("GlueVersion")

        lang_spec = self._glue_job_lang_map[lang]
        version_spec = lang_spec[glue_version]
        job_name: str = version_spec["job_name"]
        job_arn: str = version_spec["job_arn"]
        # PROCESSOR must have materialized the signals in the exec context
        # so even for internal signals, paths should be absolute, fully materialized.
        input_map = BatchInputMap(materialized_inputs)
        output = BatchOutput(materialized_output)

        lang_code = str(slot.code_lang.value)
        unique_compute_id: str = str(uuid.uuid1())
        # push code to S3
        code_key = build_object_key(["batch", lang_code, job_name, unique_compute_id], f"slot_code.{version_spec['ext']}")
        exponential_retry(put_object, {"ServiceException", "TooManyRequestsException"}, self._bucket, code_key, slot.code.encode("utf-8"))

        input_map_key = build_object_key(["batch", lang_code, job_name, unique_compute_id], f"input_map.json")
        exponential_retry(
            put_object, {"ServiceException", "TooManyRequestsException"}, self._bucket, input_map_key, input_map.dumps().encode("utf-8")
        )

        output_param_key = build_object_key(["batch", lang_code, job_name, unique_compute_id], f"output_param.json")
        exponential_retry(
            put_object, {"ServiceException", "TooManyRequestsException"}, self._bucket, output_param_key, output.dumps().encode("utf-8")
        )

        extra_jars = self._bundle_s3_paths
        if slot.code_lang == Lang.SCALA:
            extra_jars = extra_jars + (code_metadata.external_library_paths if code_metadata.external_library_paths else [])

        extra_params.update(
            {
                # --enable-glue-datacatalog
                "enable-glue-datacatalog": "",
                # --extra-jars
                "extra-jars": ",".join(extra_jars),
            }
        )

        # TODO investigate why enabling this causes 'JOB_ID invalid argument failure' during glue init.
        # if glue_version == "3.0":
        #    extra_params.update({
        #        # refer
        #        #   https://docs.aws.amazon.com/glue/latest/dg/migrating-version-30.html#migrating-version-30-from-20
        #        # --user-jars-first
        #        "user-jars-first": ""
        #    })

        if slot.code_lang == Lang.PYTHON and code_metadata.external_library_paths:
            if glue_version == "1.0":
                # in version 1.0, job run uses the same param as create_job and 'overrides' so we have to keep
                # the bundle path and add compute specific libraries.
                extra_params.update(
                    {
                        # --extra-py-files
                        "extra-py-files": ",".join(
                            [f"s3://{self._bucket_name}/{self._intelliflow_python_workingset_key}"] + code_metadata.external_library_paths
                        )
                    }
                )
            else:
                # refer the section "Specifying Additional Python Modules with AWS Glue Version 2.0"
                #   https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html
                extra_params.update(
                    {
                        # --additional-python-modules
                        "additional-python-modules": ",".join(code_metadata.external_library_paths)
                    }
                )

                # pass extra_params as job args first
        args = {f"--{key}": str(value) for key, value in extra_params.items()}
        args.update(
            {
                f"--{INPUT_MAP_PARAM}": input_map_key,
                f"--{CLIENT_CODE_PARAM}": code_key,
                f"--{CLIENT_CODE_BUCKET}": self._bucket_name,
                f"--{AWS_REGION}": self.region,
                f"--{OUTPUT_PARAM}": output_param_key,
                f"--{USER_EXTRA_PARAMS_PARAM}": json.dumps(user_extra_param_keys),
            }
        )
        try:
            # DONOT retry on transients for compute. on orchestration critical path.
            # transients should be immediately returned back to orchestration. they will be retried in a diff context.
            # also orchestration will call BatchCompute::compute in a temporal context (with timeout) in the future,
            # marking timed-out compute efforts as failures. so retries will eventually be bogus here in the long
            # term causing transients to be evaluated as failures. In the short term, long wait time here might cause
            # orchestration core (Processor) timeouts (e.g in AWS Lambda impl) especially in large-scale operations
            # such as backfilling.
            # Why do we still have exponential_retry active for get_session_state but not here? It's heuristical to
            # avoid retries here as batch compute is very expensive and very likely to cause transient errors
            # (e.g ConcurrentRunsExceeded or capacity related errors from Glue).
            job_id = start_glue_job(
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
            if error_code in self.CLIENT_RETRYABLE_EXCEPTION_LIST or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                failed_response_type = ComputeFailedResponseType.TRANSIENT
            elif error_code in ["InvalidInputException", "EntityNotFoundException", "ResourceNumberLimitExceededException"]:
                failed_response_type = ComputeFailedResponseType.BAD_SLOT
            else:
                failed_response_type = ComputeFailedResponseType.UNKNOWN

            return ComputeFailedResponse(
                failed_response_type,
                ComputeResourceDesc(job_name, job_arn, driver=self.__class__),
                error_code,
                # TODO
                str(error.response["Error"]),
            )

        # if this construct will implement queueing for request buffering, then
        # in stead of a direct call to glue, a QUEUED type response can be returned.
        return ComputeSuccessfulResponse(
            ComputeSuccessfulResponseType.PROCESSING,
            ComputeSessionDesc(job_id, ComputeResourceDesc(job_name, job_arn, driver=self.__class__)),
        )

    def get_session_state(
        self, session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
    ) -> ComputeSessionState:
        job_name = session_desc.resource_desc.resource_name
        job_run_id = session_desc.session_id
        execution_details = None
        try:
            # We are actually advised to avoid retries in critical orchestration paths which will eventually be
            # retried as long as right session state (TRANSIENT) is returned. But we still do exp retry (with a max time
            # than usual) as an optimization based on unlikeliness of issues with this api (relative to compute).
            job_run = exponential_retry(
                get_glue_job_run, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._glue, job_name, job_run_id, **{MAX_SLEEP_INTERVAL_PARAM: 16}
            )
            # now let's interpret the response in our Compute model
            start = job_run["StartedOn"]
            end = job_run["CompletedOn"] if "CompletedOn" in job_run else None
            execution_details = ComputeExecutionDetails(start, end, dict(job_run))

            session_state = get_glue_job_run_state_type(job_run)
            if session_state == ComputeSessionStateType.FAILED:
                failure_type = get_glue_job_run_failure_type(job_run)
                return ComputeFailedSessionState(failure_type, session_desc, [execution_details])
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in self.CLIENT_RETRYABLE_EXCEPTION_LIST or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                # don't mark it as failed but let orchestration know about that session state could not be retrieved
                session_state = ComputeSessionStateType.TRANSIENT_UNKNOWN
            else:
                failure_type = (
                    ComputeFailedSessionStateType.NOT_FOUND
                    if error_code in ["EntityNotFoundException"]
                    else ComputeFailedSessionStateType.UNKNOWN
                )
                # provide information to orchestration
                return ComputeFailedSessionState(failure_type, session_desc, [execution_details])

        return ComputeSessionState(session_desc, session_state, [execution_details])

    def kill_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        # skip if initial response ('state') is not SUCCESS to 'compute' call
        if active_compute_record.state.response_type == ComputeResponseType.SUCCESS:
            state = cast(ComputeSuccessfulResponse, active_compute_record.state)
            # now find most recent session desc returned for the compute record
            if active_compute_record.session_state and active_compute_record.session_state.session_desc:
                most_recent_session_desc: ComputeSessionDesc = active_compute_record.session_state.session_desc
            else:
                most_recent_session_desc: ComputeSessionDesc = state.session_desc
            job_name = most_recent_session_desc.resource_desc.resource_name
            job_run_id = most_recent_session_desc.session_id
            response = exponential_retry(
                self._glue.batch_stop_job_run, self.CLIENT_RETRYABLE_EXCEPTION_LIST, JobName=job_name, JobRunIds=[job_run_id]
            )
            if "Errors" in response and response["Errors"]:
                raise RuntimeError(
                    f"An error occurred while trying to stop AWS Glue job run! " f"Error: {response['Errors']!r}"
                )  # Errors will contain job name and run id.

    # overrides
    def get_max_wait_time_for_next_retry_in_secs(self) -> int:
        """Owerwrite the maximum interval used by the default retry strategy in
        BatchCompute::can_retry
        """
        # enough to get out of Glue's 'resource unavailable' cycle?
        # retry with increasing probability as wait time gets close to this
        return 2 * 60 * 60

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)

        # TODO introduce ABI as a sub-key to lang in this dict
        # we currently support compute_defs.ABI.GLUE_EMBEDDED
        self._glue_job_lang_map: Dict[GlueJobLanguage, Dict[str, Dict[str, Any]]] = {
            GlueJobLanguage.PYTHON: {
                "1.0": {"job_name": "", "job_arn": "", "boilerplate": GlueDefaultABIPython, "suffix": "", "ext": "py"},
                "2.0": {"job_name": "", "job_arn": "", "boilerplate": GlueDefaultABIPython, "suffix": "v2_0", "ext": "py"},
                "3.0": {"job_name": "", "job_arn": "", "boilerplate": GlueDefaultABIPython, "suffix": "v3_0", "ext": "py"},
            },
            GlueJobLanguage.SCALA: {
                "1.0": {"job_name": "", "job_arn": "", "boilerplate": GlueAllABIScala, "suffix": "", "ext": "scala"},
                "2.0": {"job_name": "", "job_arn": "", "boilerplate": GlueAllABIScala, "suffix": "v2_0", "ext": "scala"},
                "3.0": {"job_name": "", "job_arn": "", "boilerplate": GlueAllABIScala, "suffix": "v3_0", "ext": "scala"},
            },
        }

        # prepare job-names
        for lang, lang_spec in self._glue_job_lang_map.items():
            for version, version_spec in lang_spec.items():
                boilerplate_module = version_spec["boilerplate"]
                version_suffix = version_spec["suffix"]
                job_name: str = self.GLUE_JOB_NAME_FORMAT.format(
                    self.__class__.__name__, boilerplate_module.__name__, self._dev_platform.context_id + version_suffix, self._region
                )
                if len(job_name) > 255:
                    raise ValueError(
                        f"Cannot dev_init {self.__class__.__name__} due to very long"
                        f" AWS Glue Job Name {job_name} (limit < 255),"
                        f" as a result of very long context_id '{self._dev_platform.context_id}'."
                    )

                self._glue_job_lang_map[lang][version].update({"job_name": job_name})

                self._glue_job_lang_map[lang][version].update({"job_arn": f"arn:aws:glue:{self._region}:{self._account_id}:job/{job_name}"})

        self._intelliflow_python_workingset_key = build_object_key(["batch"], "bundle.zip")
        self._bucket_name: str = self.SCRIPTS_ROOT_FORMAT.format(
            "awsglue".lower(), self._dev_platform.context_id.lower(), self._account_id, self._region
        )
        bucket_len_diff = len(self._bucket_name) - MAX_BUCKET_LEN
        if bucket_len_diff > 0:
            msg = (
                f"Platform context_id '{self._dev_platform.context_id}' is too long (by {bucket_len_diff}!"
                f" {self.__class__.__name__} needs to use it create {self._bucket_name} bucket in S3."
                f" Please refer https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html"
                f" to align your naming accordingly in order to be able to use this driver."
            )
            module_logger.error(msg)
            raise ValueError(msg)

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        """Whole platform got bootstrapped at runtime. For other runtime services, this
        construct should be initialized (ex: context_owner: Lambda, etc)"""
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._glue = boto3.client("glue", region_name=self._region)
        # TODO comment the following, probably won't need at runtime
        self._s3 = boto3.resource("s3")
        self._bucket = get_bucket(self._s3, self._bucket_name)

    def provide_runtime_trusted_entities(self) -> List[str]:
        return ["glue.amazonaws.com"]

    def provide_runtime_default_policies(self) -> List[str]:
        # arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole
        return ["service-role/AWSGlueServiceRole"]

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        # allow exec-role (post-activation, cumulative list of all trusted entities [AWS services]) to do the following;
        permissions = [
            ConstructPermission([f"arn:aws:s3:::{self._bucket_name}", f"arn:aws:s3:::{self._bucket_name}/*"], ["s3:*"]),
            # TODO be more picky.
            # allow other service assuming our role to call the jobs here
            ConstructPermission(
                [
                    f"arn:aws:glue:{self._region}:{self._account_id}:job/{self._glue_job_lang_map[lang][version]['job_name']}"
                    for lang in self._glue_job_lang_map.keys()
                    for version in self._glue_job_lang_map[lang].keys()
                ],
                ["glue:*"],
            ),
            # CW Logs (might look redundant, but please forget about other drivers while declaring these),
            # deduping is handled automatically.
            ConstructPermission([f"arn:aws:logs:{self._region}:{self._account_id}:*"], ["logs:*"]),
            # must add a policy to allow your users the iam:PassRole permission for IAM roles to match your naming convention
            ConstructPermission([self._params[AWSCommonParams.IF_EXE_ROLE]], ["iam:PassRole"]),
            # TODO post-MVP evaluate External output support.
            #  Currently we dont support it. So no write related permission should be granted for external signals.
            #  And, for internals, it is obvious that Storage impl should have already granted our role with the
            #  necessary permissions.
        ]

        external_library_resource_arns = set()
        for route in self._pending_internal_routes:
            for slot in route.slots:
                if slot.code_metadata.external_library_paths:
                    for s3_path in slot.code_metadata.external_library_paths:
                        try:
                            s3_spec = S3SignalSourceAccessSpec.from_url(account_id=None, url=s3_path)
                        except Exception:
                            module_logger.error(
                                f"External library path {s3_path} attached to route {route.route_id!r} "
                                f" via slot: {(slot.type, slot.code_lang)!r} is not supported by "
                                f" BatchCompute driver {self.__class__.__name__!r}. "
                            )
                            raise
                        # exact resource (JARs, zips)
                        external_library_resource_arns.add(f"arn:aws:s3:::{s3_spec.bucket}/{s3_path[len(f's3://{s3_spec.bucket}/'):]}")

                # TODO Move into <BatchCompute>
                # TODO evalute moving is_batch_compute check even before the external library paths extraction.
                if slot.type.is_batch_compute() and slot.permissions:
                    for compute_perm in slot.permissions:
                        # TODO check compute_perm feasibility in AWS Glue (check ARN, resource type, etc)
                        if compute_perm.context != PermissionContext.DEVTIME:
                            permissions.append(ConstructPermission(compute_perm.resource, compute_perm.action))

        if external_library_resource_arns:
            permissions.append(
                ConstructPermission(list(external_library_resource_arns), ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket"])
            )

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
        bucket_name_format: str = cls.SCRIPTS_ROOT_FORMAT.format(
            "awsglue".lower(), "*", params[AWSCommonParams.ACCOUNT_ID], params[AWSCommonParams.REGION]
        )
        return [
            ConstructPermission([f"arn:aws:s3:::{bucket_name_format}", f"arn:aws:s3:::{bucket_name_format}/*"], ["s3:*"]),
            # ConstructPermission(["*"], ["glue:*"]),
            ConstructPermission(
                [
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:catalog",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:database/*",
                ],
                ["glue:GetDatabases"],
            ),
            # Read-access into everything else in the same catalog
            # Refer
            #   https://docs.aws.amazon.com/glue/latest/dg/glue-specifying-resource-arns.html
            ConstructPermission(
                [
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:catalog",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:database/default",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:database/*",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:table/*/*",
                    # f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:userDefinedFunction/*/*",
                    # f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:connection/*",
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
            # More permissive read access on other non-catalog entities
            ConstructPermission(
                ["*"],
                [
                    "glue:ListCrawlers",
                    "glue:BatchGetCrawlers",
                    "glue:ListDevEndpoints",
                    "glue:BatchGetDevEndpoints",
                    "glue:GetJob",
                    "glue:GetJobs",
                    "glue:ListJobs",
                    "glue:BatchGetJobs",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:GetJobBookmark",
                    "glue:GetJobBookmarks",
                    "glue:GetTrigger",
                    "glue:GetTriggers",
                    "glue:ListTriggers",
                    "glue:BatchGetTriggers",
                ],
            ),
            # and finally: full-authorization on activation and (local) compute time permissions (on its own resources)
            ConstructPermission(
                [
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:job/{cls.GLUE_JOB_NAME_FORMAT.format(cls.__name__, '*', '*', params[AWSCommonParams.REGION])}"
                ],
                ["glue:*"],
            ),
            # TODO dev-role should have the right to do BucketNotification on external signals
            # this would require post-MVP design change on how dev-role is used and when it is updated.
            # probably during the activation again (switching to the admin credentails if authorization is given).
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
        # where dimension is Type='count'
        job_level_COUNT_metrics = [
            "glue.driver.aggregate.bytesRead",
            "glue.driver.aggregate.elapsedTime",
            "glue.driver.aggregate.numCompletedStages",
            "glue.driver.aggregate.numCompletedTasks",
            "glue.driver.aggregate.numFailedTask",
            "glue.driver.aggregate.numKilledTasks",
            "glue.driver.aggregate.recordsRead",
            "glue.driver.aggregate.shuffleBytesWritten",
            "glue.driver.aggregate.shuffleLocalBytesRead",
        ]

        job_level_GAUGE_metrics = [
            "glue.driver.BlockManager.disk.diskSpaceUsed_MB",
            "glue.driver.jvm.heap.usage",
            "glue.driver.jvm.heap.used",
            "glue.driver.s3.filesystem.read_bytes",
            "glue.driver.s3.filesystem.write_bytes",
            "glue.driver.system.cpuSystemLoad",
            "glue.ALL.jvm.heap.usage",
            "glue.ALL.jvm.heap.used",
            "glue.ALL.s3.filesystem.read_bytes",
            "glue.ALL.s3.filesystem.write_bytes",
            "glue.ALL.system.cpuSystemLoad",
        ]
        return [
            Signal(
                SignalType.CW_METRIC_DATA_CREATION,
                CWMetricSignalSourceAccessSpec(
                    "Glue",
                    {"JobName": self._glue_job_lang_map[lang][version]["job_name"], "Type": "count", "JobRunId": "ALL"},
                    # metadata (should be visible in front-end as well)
                    **{"Notes": "Supports 1 min period"},
                ),
                SignalDomainSpec(
                    dimension_filter_spec=DimensionFilter.load_raw(
                        {
                            metric_name: {  # Name  (overwrite in filter spec to make them visible to the user. otherwise user should specify the name,
                                #           in that case platform abstraction is broken since user should have a very clear idea about what is
                                #           providing these metrics).
                                "*": {  # Statistic
                                    "*": {  # Period  (AWS emits with 1 min by default), let user decide.
                                        "*": {}  # Time  always leave it 'any' if not experimenting.
                                    }
                                }
                            }
                            for metric_name in job_level_COUNT_metrics
                        }
                    ),
                    dimension_spec=None,
                    integrity_check_protocol=None,
                ),
                # make sure that default metric alias/ID complies with CW expectation (first letter lower case).
                f"batchCompute.typeCount.{lang.value}.{version}",
            )
            for lang in self._glue_job_lang_map.keys()
            for version in self._glue_job_lang_map[lang].keys()
        ] + [
            Signal(
                SignalType.CW_METRIC_DATA_CREATION,
                CWMetricSignalSourceAccessSpec(
                    "Glue", {"JobName": self._glue_job_lang_map[lang][version]["job_name"], "Type": "gauge", "JobRunId": "ALL"}
                ),
                SignalDomainSpec(
                    dimension_filter_spec=DimensionFilter.load_raw(
                        {metric_name: {"*": {"*": {"*": {}}}} for metric_name in job_level_GAUGE_metrics}
                    ),
                    dimension_spec=None,
                    integrity_check_protocol=None,
                ),
                # make sure that default metric alias/ID complies with CW expectation (first letter lower case).
                f"batchCompute.typeGauge.{lang.value}.{version}",
            )
            for lang in self._glue_job_lang_map.keys()
            for version in self._glue_job_lang_map[lang].keys()
        ]

    def _setup_scripts_bucket(self):
        """Initial setup of storage bucket. Enforces policy for access from dev and exec roles."""
        try:
            self._bucket = create_bucket(self._s3, self._bucket_name, self._region)
        except ClientError as error:
            if error.response["Error"]["Code"] == "InvalidBucketName":
                msg = (
                    f"Platform context_id '{self._dev_platform.context_id}' is not valid!"
                    f" {self.__class__.__name__} needs to use it create {self._bucket_name} bucket in S3."
                    f" Please refer https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html"
                    f" to align your naming accordingly in order to be able to use this driver."
                )
                module_logger.error(msg)
                raise ValueError(msg)
            elif error.response["Error"]["Code"] == "BucketAlreadyExists":
                msg = (
                    f"Bucket {self._bucket_name!r} has been taken by some other application. Cannot "
                    f"proceed with activation until S3 bucket is retained by same account "
                    f" (AWS Entity: {self._params[AWSCommonParams.IF_DEV_ROLE]!r}, Region: {self.region})."
                )
                module_logger.error(msg)
                raise RuntimeError(msg, error)
            else:
                raise

        self._setup_activated_bucket_policy()

    def _setup_activated_bucket_policy(self) -> None:
        put_policy_desc = {
            "Version": "2012-10-17",
            "Id": str(uuid.uuid1()),
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": self._params[AWSCommonParams.IF_DEV_ROLE]},
                    "Action": ["s3:*"],
                    "Resource": [f"arn:aws:s3:::{self._bucket.name}/*", f"arn:aws:s3:::{self._bucket.name}"],
                },
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": self._params[AWSCommonParams.IF_EXE_ROLE]},
                    "Action": ["s3:*"],
                    # TODO post-MVP
                    # see https://docs.aws.amazon.com/glue/latest/dg/create-an-iam-role.html
                    # the following is the complete list for both Data sources + targets combined.
                    # 'Action': [ 's3:GetObject', 's3:PutObject', 's3:DeleteObject', 's3:GetObjectVersion' 's3:ListBucket' ],
                    "Resource": [f"arn:aws:s3:::{self._bucket.name}/*", f"arn:aws:s3:::{self._bucket.name}"],
                },
            ],
        }
        try:
            exponential_retry(put_policy, ["MalformedPolicy"], self._s3, self._bucket.name, put_policy_desc)
        except ClientError as error:
            if error.response["Error"]["Code"] == "MalformedPolicy":
                module_logger.error("Couldn't put the policy for Glue scripts folder! Error:", str(error))
            else:
                raise

    def build_bootstrapper_object_key(self) -> str:
        return build_object_key(["bootstrapper"], f"{self.__class__.__name__.lower()}_RuntimePlatform.data")

    def _update_bootstrapper(self, bootstrapper: "RuntimePlatform") -> None:
        # uploading it to S3 and passing S3 link as job arg.
        bootstrapped_platform = bootstrapper.serialize()

        bootstrapper_object_key = self.build_bootstrapper_object_key()
        exponential_retry(
            put_object, {"ServiceException", "TooManyRequestsException"}, self._bucket, bootstrapper_object_key, bootstrapped_platform
        )

    def activate(self) -> None:
        if not bucket_exists(self._s3, self._bucket_name):
            # TODO consider moving to activate?
            # storage might be required to read stuff even during dev-time.
            self._setup_scripts_bucket()
        else:
            self._bucket = get_bucket(self._s3, self._bucket_name)

        if not is_environment_immutable():
            # check driver specific pre-compiled dependencies
            #
            # call with actual job version if bundles are version specific
            bundles: List[Tuple[str, "Path"]] = get_bundles(glue_version="2.0")
            self._bundle_s3_keys = []
            self._bundle_s3_paths = []
            for bundle_name, bundle_path in bundles:
                bundle_s3_key = build_object_key(["batch", "lib"], bundle_name)
                self._bundle_s3_keys.append(bundle_s3_key)
                self._bundle_s3_paths.append(f"s3://{self._bucket_name}/{bundle_s3_key}")

                if not object_exists(self._s3, self._bucket, bundle_s3_key):
                    exponential_retry(
                        put_object,
                        {"ServiceException", "TooManyRequestsException"},
                        self._bucket,
                        bundle_s3_key,
                        open(bundle_path, "rb").read(),
                    )

            for lang, lang_spec in self._glue_job_lang_map.items():
                working_set_s3_location = None
                if lang == GlueJobLanguage.PYTHON:
                    # Upload the bundle (working set) to its own bucket.
                    exponential_retry(
                        put_object,
                        {"ServiceException", "TooManyRequestsException"},
                        self._bucket,
                        self._intelliflow_python_workingset_key,
                        get_working_set_as_zip_stream(extra_folders_to_avoid=PYTHON_MODULES_TO_BE_AVOIDED_IN_GLUE_BUNDLE),
                    )

                    working_set_s3_location = f"s3://{self._bucket_name}/{self._intelliflow_python_workingset_key}"
                for version, version_spec in lang_spec.items():
                    batch = version_spec["boilerplate"]()
                    file_ext = version_spec["ext"]
                    batch_script_file_key = build_object_key(["batch"], f"glueetl_{batch.__class__.__name__.lower()}.{file_ext}")
                    exponential_retry(
                        put_object,
                        {"ServiceException", "TooManyRequestsException"},
                        self._bucket,
                        batch_script_file_key,
                        batch.generate_glue_script().encode("utf-8"),
                    )

                    lang_abi_job_name = version_spec["job_name"]
                    job_name = exponential_retry(get_glue_job, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._glue, lang_abi_job_name)

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
                        self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                        self._glue,
                        lang_abi_job_name,
                        description,
                        self._params[AWSCommonParams.IF_EXE_ROLE],
                        GlueJobCommandType.BATCH,
                        lang,
                        f"s3://{self._bucket_name}/{batch_script_file_key}",
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

        # 1- remove all of the jobs
        # Note: delete_glue_job does not raise, it is already retry friendly.

        for lang, lang_spec in self._glue_job_lang_map.items():
            for version, version_spec in lang_spec.items():
                lang_abi_job_name = version_spec["job_name"]
                exponential_retry(delete_glue_job, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._glue, lang_abi_job_name)

        # 2- delete bucket
        if self._bucket_name and exponential_retry(bucket_exists, [], self._s3, self._bucket_name):
            bucket = get_bucket(self._s3, self._bucket_name)
            exponential_retry(empty_bucket, [], bucket)
            exponential_retry(delete_bucket, [], bucket)
            self._bucket_name = None
            self._bucket = None

        super().terminate()

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)

    def _resolve_glue_version(self, extra_params: Dict[str, Any], materialized_inputs: List[Signal]) -> None:
        if "GlueVersion" not in extra_params or extra_params.get("GlueVersion", GlueVersion.AUTO.value) in [
            GlueVersion.AUTO,
            GlueVersion.AUTO.value,
        ]:
            extra_params["GlueVersion"] = self.GLUE_DEFAULT_VERSION.value

    def hook_internal(self, route: Route) -> None:
        """Early stage check on a new route, so that we can fail fast before the whole activation."""
        super().hook_internal(route)

        # capture one of the most common scenarios early (':' char output path causing issues on old Hadoop envs)
        output_path_might_have_colon = any(
            [
                (not dim.params or not dim.params.get("format", None) or ":" in dim.params.get("format", None))
                for dim in route.output.domain_spec.dimension_spec.get_flattened_dimension_map().values()
                if dim.type == DimensionType.DATETIME
            ]
        )

        if not output_path_might_have_colon:
            # check all of the material dimensions to see if user inputted colon in filter values
            output_path_might_have_colon = any(
                [
                    isinstance(dim.value, str) and ":" in dim.value
                    for dim in route.output.domain_spec.dimension_filter_spec.get_flattened_dimension_map().values()
                    if dim.is_material_value()
                ]
            )

        for slot in route.slots:
            if slot.type == SlotType.ASYNC_BATCH_COMPUTE:
                code_metadata = slot.code_metadata
                if code_metadata.code_type != SlotCodeType.EMBEDDED_SCRIPT:
                    raise NotImplementedError(f"Code script type {code_metadata.code_type!r} is not supported yet!")
                    # FUTURE / TODO when other code types are supported warn user about target_entity and target_method
                    #  will be inferred by the driver (e.g Scala quasiquotes based object/class name and method name extraction)

                extra_params: Dict[str, Any] = dict(slot.extra_params)
                self._resolve_glue_version(extra_params, route.link_node.signals)
                evaluate_execution_params(GlueJobCommandType.BATCH, GlueJobLanguage.from_slot_lang(slot.code_lang), extra_params, True)

                glue_version = extra_params.get("GlueVersion", None)

                # special handling of Hadoop 2.4 relative path exception
                if output_path_might_have_colon and glue_version == GlueVersion.VERSION_1_0:
                    module_logger.warning(
                        f"Route {route.output.alias!r} might not be executed on Glue version {GlueVersion.VERSION_1_0!r}"
                        f" because its output path would contain ':' character. If it is due to missing 'format' in"
                        f" one of the DATETIME dimensions of the output then define its format (e.g '%Y-%m-%d') without"
                        f" that character."
                    )

    def hook_external(self, signals: List[Signal]) -> None:
        # hook early during app activation so that we can fail fast, without letting
        # whole platform (and other constructs) activate themselves and cause a more
        # complicated rollback.
        super().hook_external(signals)

        if any(
            s.resource_access_spec.source
            not in [
                SignalSourceType.S3,
                SignalSourceType.GLUE_TABLE,
                SignalSourceType.CW_ALARM,
                SignalSourceType.CW_COMPOSITE_ALARM,
                SignalSourceType.CW_METRIC,
            ]
            for s in signals
        ):
            raise NotImplementedError(
                f"External signal source type for one of " f"'{signals!r} is not supported by {self.__class__.__name__}'"
            )

        for s in signals:
            if s.resource_access_spec.source == SignalSourceType.GLUE_TABLE:
                gt_access_spec: GlueTableSignalSourceAccessSpec = cast("GlueTableSignalSourceAccessSpec", s.resource_access_spec)
                if not glue_catalog.check_table(self._session, self._region, gt_access_spec.database, gt_access_spec.table_name):
                    raise ValueError(
                        f"Either the database ({gt_access_spec.database!r}) or table ({gt_access_spec.table_name!r}) "
                        f" could not be found in the account ({self._account_id}) region ({self._region})."
                    )
            elif s.resource_access_spec.source == SignalSourceType.S3:
                key: str = s.resource_access_spec.attrs.get("encryption_key", None)
                if key and not key.startswith("arn:aws:kms"):
                    raise NotImplementedError(
                        f"{key} is not currently supported. {self.__class__.__name__}" f" currently supports KMS key arns."
                    )

    def hook_internal_signal(self, signal: "Signal") -> None:
        # currently not interested in doing any checks on other signal types (such as TIMER_EVENT)
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
            raise NotImplementedError(f"Security/Encryption/Resource level encryption for {self.__class__.__name__} not supported.")

        super().hook_security_conf(security_conf, platform_security_conf)

    def _process_security_conf(self, new_security_conf: ConstructSecurityConf, current_security_conf: ConstructSecurityConf) -> None:
        pass

    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        # TODO compile BATCH_COMPUTE type Slots before to see if they are valid
        # otherwise app-developer will get the error at runtime, in glue (bad exp).
        # why don't we store compiled object along with the Slot code?
        # Answer is "to be safe", as we currently dont know the compatibility of our ecosystem
        # and glue side. Glue does not let us specify minor version of Python. We found it a
        # risk in terms of compiled code (byte-code) compatibility.

        # allow it to raise SyntaxError
        # Currently blocked since we dont want to introduce Glue dependencies into RheocerOS.
        # compile(codeBlock, '<string>', 'exec')
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

    def describe_compute_record(self, active_compute_record: "RoutingTable.ComputeRecord") -> Optional[Dict[str, Any]]:
        execution_details = dict()
        if active_compute_record.session_state:
            # TODO: Create presigned url. Find a way to redirect th user to jobrun
            if active_compute_record.session_state.executions:
                # We only check and gather details for the final execution
                final_execution_details = active_compute_record.session_state.executions[::-1][0].details
                details = dict()
                details["JobId"] = final_execution_details.get("Id", None)
                details["JobName"] = final_execution_details.get("JobName", None)
                details[
                    "JobURL"
                ] = f"https://{self.region}.console.aws.amazon.com/gluestudio/home?region={self.region}#/editor/job/{details['JobName']}/details"
                details[
                    "JobRunURL"
                ] = f"https://{self.region}.console.aws.amazon.com/gluestudio/home?region={self.region}#/job/{details['JobName']}/run/{details['JobId']}"
                details["StartedOn"] = final_execution_details.get("StartedOn", None)
                details["CompletedOn"] = final_execution_details.get("CompletedOn", None)
                details["JobRunState"] = final_execution_details.get("JobRunState", None)
                details["Attempt"] = final_execution_details.get("Attempt", None)
                details["ExecutionTime"] = final_execution_details.get("ExecutionTime", None)
                details["WorkerType"] = final_execution_details.get("WorkerType", None)
                details["NumberOfWorkers"] = final_execution_details.get("NumberOfWorkers", None)
                details["GlueVersion"] = final_execution_details.get("GlueVersion", None)

                job_run_state = final_execution_details.get("JobRunState", None)
                # Adding error message if the JobRun FAILED OR TIMEDOUT
                if job_run_state == "FAILED" or job_run_state == "TIMEOUT":
                    details["ErrorMessage"] = final_execution_details["ErrorMessage"]
                    details["Timeout"] = final_execution_details["Timeout"]

                execution_details.update({"details": details})

            # Extract SLOT info
            if active_compute_record.slot:
                slot = dict()
                slot["type"] = active_compute_record.slot.type
                slot["lang"] = active_compute_record.slot.code_lang
                slot["code"] = active_compute_record.slot.code
                slot["code_abi"] = active_compute_record.slot.code_abi

                execution_details.update({"slot": slot})

            return execution_details
