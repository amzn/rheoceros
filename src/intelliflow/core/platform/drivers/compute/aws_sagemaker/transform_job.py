# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Any, Dict, List, Optional, Set, Tuple, cast

import shortuuid
from botocore.exceptions import ClientError
from overrides import overrides

from intelliflow.core.permission import PermissionContext
from intelliflow.core.signal_processing import DimensionFilter, DimensionSpec, Signal, Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal_source import (
    DATA_FORMAT_KEY,
    DATA_TYPE_KEY,
    DATASET_FORMAT_KEY,
    DATASET_HEADER_KEY,
    MODEL_FORMAT_KEY,
    MODEL_METADATA,
    DatasetSignalSourceAccessSpec,
    DatasetSignalSourceFormat,
    DataType,
    ModelSignalSourceFormat,
    S3SignalSourceAccessSpec,
    SignalSourceAccessSpec,
    SignalSourceType,
)

from ....constructs import (
    BatchCompute,
    ConstructInternalMetricDesc,
    ConstructParamsDict,
    ConstructPermission,
    ConstructSecurityConf,
    Storage,
)
from ....definitions.aws.common import AWS_COMMON_RETRYABLE_ERRORS, MAX_SLEEP_INTERVAL_PARAM
from ....definitions.aws.common import CommonParams as AWSCommonParams
from ....definitions.aws.common import exponential_retry
from ....definitions.aws.s3.bucket_wrapper import get_bucket
from ....definitions.aws.s3.object_wrapper import list_objects
from ....definitions.aws.sagemaker.client_wrapper import (
    BUILTIN_ALGO_MODEL_ARTIFACT_RESOURCE_NAME,
    ImageScope,
    create_sagemaker_model,
    describe_sagemaker_transform_job,
    get_image_for_builtin,
    get_transform_job_run_failure_state_type,
    get_transform_job_run_state_type,
    is_builtin_algo_name,
    start_batch_transform_job,
    stop_batch_transform_job,
    validate_job_name,
)
from ....definitions.compute import (
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
from ...aws_common import AWSConstructMixin

module_logger = logging.getLogger(__file__)


def get_data_iterator(model_signal: Signal, storage: Storage):
    return storage.load_internal(model_signal, None)


def get_s3_bucket(s3, model_signal_specs: SignalSourceAccessSpec):
    s3_spec = cast(S3SignalSourceAccessSpec, model_signal_specs)
    bucket_name = s3_spec.bucket
    return get_bucket(s3, bucket_name)


def get_s3_bucket_name(model_signal_specs: SignalSourceAccessSpec):
    s3_spec = cast(S3SignalSourceAccessSpec, model_signal_specs)
    return s3_spec.bucket


def get_objects_in_folder(model_signal: Signal, bucket_name: str, bucket):
    # enforcing this -- multiple models from multiple partitions not supported
    model_materialized_folder_path = model_signal.get_materialized_resource_paths()[0]
    folder = model_materialized_folder_path.replace(f"s3://{bucket_name}/", "")
    objects_in_folder = list_objects(bucket, folder)
    return objects_in_folder


class AWSSagemakerTransformJobBatchCompute(AWSConstructMixin, BatchCompute):
    """AWS Sagemaker Transform Job BatchCompute impl"""

    CLIENT_RETRYABLE_EXCEPTION_LIST: Set[str] = {"ResourceInUse", "InternalServerException", "ResourceLimitExceeded"}

    TRANSFORM_JOB_NAME_PATTERN = "IF-Transform-{}-{}"
    TRANSFORM_MODEL_NAME_PATTERN = "IF-{}-{}"

    @classmethod
    @overrides
    def driver_spec(cls) -> DimensionFilter:
        return DimensionFilter.load_raw(
            {
                Lang.AWS_SAGEMAKER_TRANSFORM_JOB: {ABI.NONE: {"*": {"*": {}}}},
            }
        )

    def __init__(self, params: ConstructParamsDict) -> None:
        super().__init__(params)
        self._sagemaker = self._session.client("sagemaker", region_name=self._region)
        self._s3 = self._session.resource("s3", region_name=self._region)

    @overrides
    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)

        if not platform.storage.get_storage_resource_path().lower().startswith("arn:aws:s3"):
            raise TypeError(f"Internal storage should be based on S3 for {self.__class__.__name__!r} driver to work!")

        self.validate_job_names()

    @overrides
    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._sagemaker = self._session.client("sagemaker", region_name=self._region)
        self._s3 = self._session.resource("s3", region_name=self._region)

    @overrides
    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._sagemaker = self._session.client("sagemaker", region_name=self._region)
        self._s3 = self._session.resource("s3", region_name=self._region)

    @overrides
    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._sagemaker = None
        self._s3 = None

    @overrides
    def provide_output_attributes(self, inputs: List[Signal], slot: Slot, user_attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Do very early checks and fill missing metadata based on
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_transform_job

        note: These checks and metadata hints will run even before activation when new nodes are added to the application.
        """
        # 1- Find the model data input (e.g output of training job for example). for that use the metadata
        model_data_signals = [input for input in inputs if input.resource_access_spec.data_type == DataType.MODEL_ARTIFACT]
        if not model_data_signals:
            raise ValueError(f"One of the inputs to {self.__class__.__name__} compute must be {DataType.MODEL_ARTIFACT.value!r}!r")
        if len(model_data_signals) > 1:
            raise ValueError(f"Only one input to {self.__class__.__name__} compute must be {DataType.MODEL_ARTIFACT.value!r}!r")
        # Note: we don't check model_format to make sure it is AWS Training Job. Sagemaker model can still be created other artifacts from
        # other sources.
        model_data_signal = model_data_signals[0]
        # TODO check for multiple partition for model data -- get materialized resource path won't here as
        # data[: -2] is logical and will not actually give multiple paths during build time
        # and the path would still be single

        model_format = model_data_signal.resource_access_spec.attrs.get(MODEL_FORMAT_KEY, None)
        if model_format != ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB:
            raise ValueError(
                f"Input model {model_data_signal.alias!r} should have the metadata in {ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB!r} format!"
                f" Please refer the following API on expected metadata"
                f" https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_training_job"
            )

        if not model_data_signal.resource_access_spec.attrs.get(MODEL_METADATA, None):
            raise ValueError(
                f"Input model {model_data_signal.alias!r} does not have {MODEL_METADATA!r} defined!"
                f" If it is an external input, please refer the following API on expected metadata"
            )

        # Adding check for if Algorithm Specification exists for model metadata
        model_metadata = model_data_signal.resource_access_spec.attrs.get(MODEL_METADATA, None)
        if not model_metadata.get("AlgorithmSpecification", None):
            raise ValueError(f"Algorithm specification not defined for {MODEL_METADATA!r}!")

        # Adding check for if Training Image exists for model metadata -- Done in compute()
        # algorithm_specification = model_metadata.get("AlgorithmSpecification", None)
        # if not algorithm_specification.get("TrainingImage", None):
        #     raise ValueError(f"Training Image not defined for {MODEL_METADATA!r}!")

        # validate model data is in the same region
        # refer note in create_model API
        if isinstance(model_data_signal.resource_access_spec, S3SignalSourceAccessSpec):
            if not model_data_signal.resource_access_spec.bucket.region == self.region:
                raise ValueError(
                    f" Model data {model_data_signal.alias!r} region not the same as {self.region!r}!"
                    f" If it is an external input, please refer the following API on expected metadata"
                )

        # 2- Find datasets
        dataset_signals = [
            input
            for input in inputs
            if input.resource_access_spec.data_type == DataType.DATASET
            or (isinstance(input.resource_access_spec, DatasetSignalSourceAccessSpec) and input != model_data_signal)
        ]

        if len(dataset_signals) == 0:
            raise ValueError(f"There is no dataset input to {self.__class__.__name__} compute!")
        # FUTURE/TODO support multiple datasets (via 'manifest' in compute)
        if len(dataset_signals) > 1:
            raise ValueError(f"There should only be one dataset input to {self.__class__.__name__} compute!")

        # TODO check for multiple partition for datasets -- get materialized resource path won't here as
        # data[: -2] is logical and will not actually give multiple paths during build time
        # and the path would still be single

        # 2.1- data inputs should be all same
        content_types = [dataset.resource_access_spec.content_type for dataset in dataset_signals]
        if len(set(content_types)) > 1:
            raise ValueError(
                f"Input datasets to {self.__class__.__name__} compute must have the same 'content_type' (or 'dataset_format')! Current types: {content_types!r}"
            )

        # validating if the input dataset(s) have a header then raise
        for dataset in dataset_signals:
            if dataset.resource_access_spec.data_header_exists:
                raise ValueError(
                    f"{DATASET_HEADER_KEY!r} must be defined as False or left undefined on an {self.__class__.__name__!r} input!"
                )

        if user_attrs.get(DATA_TYPE_KEY, DataType.DATASET) != DataType.DATASET:
            raise ValueError(
                f"{DATA_TYPE_KEY!r} must be defined as {DataType.DATASET} or left undefined for {self.__class__.__name__} output!"
            )

        # validating if the output dataset has a header then raise
        if user_attrs.get(DATASET_HEADER_KEY, False) != False:
            raise ValueError(f"{DATASET_HEADER_KEY!r} must be defined as False or left undefined on an {self.__class__.__name__!r} output!")

        common_input_format = DatasetSignalSourceFormat.from_content_type(content_types[0])
        data_format = user_attrs.get(DATASET_FORMAT_KEY, user_attrs.get(DATA_FORMAT_KEY, common_input_format))

        # header: supports both so it is up to user input. but default to True if not set.
        return {
            DATA_TYPE_KEY: DataType.DATASET,
            DATASET_HEADER_KEY: False,
            # TODO how to dump schema? at the end of the compute this driver can actually do that (see Athena driver)
            # DATASET_SCHEMA_TYPE_KEY: DatasetSchemaType.SPARK_SCHEMA_JSON,
            DATASET_FORMAT_KEY: data_format,
        }

    @overrides
    def query_external_source_spec(
        self, ext_signal_source: SignalSourceAccessSpec
    ) -> Optional[Tuple[SignalSourceAccessSpec, DimensionSpec]]:
        if ext_signal_source.source == SignalSourceType.S3:
            # FUTURE
            #  iterate over all of the training jobs in the same account, region and return a new S3 access spec with
            #  all of the necessary metadata
            pass

        raise NotImplementedError(
            f"This external signal source ({ext_signal_source.source!r}) cannot be queried"
            f" by BatchCompute driver: {self.__class__.__name__}"
        )

    def _build_job_name(self, uuid: str):
        return self.TRANSFORM_JOB_NAME_PATTERN.format(self.context_id, uuid).lower()

    def validate_job_names(self):
        AWS_SAGEMAKER_TRANSFORM_JOB_NAME_MAX_LENGTH = 63
        shortuuid.uuid()
        job_name = self._build_job_name(str(shortuuid.uuid()[:12]))
        if len(job_name) > 63:
            raise ValueError(
                f"Cannot dev_init {self.__class__.__name__} due to very long" f" AWS Sagemaker Transform Job Name {job_name} (limit < 64)."
            )
        if not validate_job_name(job_name):
            raise ValueError(
                f"Cannot dev_init {self.__class__.__name__} due to invalid job name {job_name} doesn't meet Sagemaker transform job name "
                f"pattern"
            )

    def _get_model_artifacts_path(self, model_signal: Signal):
        model_artifacts_path = None
        model_source_specs = model_signal.get_materialized_access_specs()

        if model_signal.resource_access_spec.source == SignalSourceType.INTERNAL:

            storage: Storage = self.get_platform().storage
            data_iterator = get_data_iterator(model_signal, storage)

            for object_path_and_data_tuple in data_iterator:
                path = object_path_and_data_tuple[0]
                if path.endswith(BUILTIN_ALGO_MODEL_ARTIFACT_RESOURCE_NAME):
                    model_artifacts_path = path
                    return model_artifacts_path
        elif model_signal.resource_access_spec.source == SignalSourceType.S3:
            bucket_name = get_s3_bucket_name(model_signal.resource_access_spec)
            bucket = get_s3_bucket(self._s3, model_signal.resource_access_spec)

            objects_in_folder = get_objects_in_folder(model_signal, bucket_name, bucket)

            for object in objects_in_folder:
                key = object.key
                if key.endswith(BUILTIN_ALGO_MODEL_ARTIFACT_RESOURCE_NAME):
                    model_artifacts_path = f"s3://{bucket_name}/{key}"
                    return model_artifacts_path
        return model_artifacts_path

    @overrides
    def compute(
        self,
        route: Route,
        materialized_inputs: List[Signal],
        slot: Slot,
        materialized_output: Signal,
        execution_ctx_id: str,
        retry_session_desc: Optional[ComputeSessionDesc] = None,
    ) -> ComputeResponse:
        api_params: Dict[str, Any] = dict(slot.extra_params)

        unique_compute_id: str = shortuuid.uuid()[:12]

        transform_job_name = self._build_job_name(unique_compute_id)
        transform_job_arn = self._build_transform_job_arn(self.account_id, self.region, transform_job_name)
        model_name = self.TRANSFORM_MODEL_NAME_PATTERN.format(self.context_id, unique_compute_id).lower()

        model_signal: Signal = [
            input
            for input in materialized_inputs
            if input.resource_access_spec.data_type == DataType.MODEL_ARTIFACT
            or input.resource_access_spec.data_type == DataType.RAW_CONTENT
        ][0]

        model_artifacts_path = self._get_model_artifacts_path(model_signal)

        model_metadata = model_signal.resource_access_spec.attrs.get(MODEL_METADATA, None)
        algo_specification = model_metadata["AlgorithmSpecification"]
        training_image = algo_specification.get("TrainingImage", None)
        # TODO Support more builtin algorithm and custom algorithms
        is_builtin_algo = False
        if not training_image:
            algo_name = algo_specification.get("AlgorithmName", None)
            if is_builtin_algo_name(algo_name):
                is_builtin_algo = True
                algo_version = algo_specification.get("AlgorithmVersion", None)
                training_image = get_image_for_builtin(algo_name, self.region, ImageScope.INFERENCE, algo_version)
                if not model_artifacts_path:
                    return ComputeFailedResponse(
                        ComputeFailedResponseType.UNKNOWN,
                        ComputeResourceDesc(transform_job_name, transform_job_arn, driver=self.__class__),
                        "ResourceNotFound",  # model artifact path not found
                        f"Could not find any model artifact object {BUILTIN_ALGO_MODEL_ARTIFACT_RESOURCE_NAME!r} in the input path {model_signal.get_materialized_resource_paths()[0]!r}",
                    )
            else:
                # TODO fetch algo
                raise NotImplementedError(f"Custom algorithm {algo_name!r} cannot be retrieved!")

        try:
            model_params = {
                "Image": training_image,
            }
            if model_artifacts_path:
                model_params.update(
                    {
                        "ModelDataUrl": model_artifacts_path,
                    }
                )

            create_sagemaker_model(
                self._sagemaker,
                model_name,
                model_params,
                self._params[AWSCommonParams.IF_EXE_ROLE],
                [
                    {"Key": "TransformJobName", "Value": transform_job_name},
                ],
            )
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in ["ResourceLimitExceeded"]:
                failed_response_type = ComputeFailedResponseType.TRANSIENT
            else:
                failed_response_type = ComputeFailedResponseType.UNKNOWN

            return ComputeFailedResponse(
                failed_response_type,
                ComputeResourceDesc(transform_job_name, transform_job_arn, driver=self.__class__),
                error_code,
                str(error.response["Error"]),
            )

        dataset_signals = [
            input
            for input in materialized_inputs
            if input.resource_access_spec.data_type == DataType.DATASET
            or (isinstance(input.resource_access_spec, DatasetSignalSourceAccessSpec) and input != model_signal)
        ]
        materialized_dataset_signal = dataset_signals[0]
        transform_resources = api_params["TransformResources"] if "TransformResources" in api_params else None
        if not transform_resources:
            # TODO get this from algo module based on algo type
            transform_resources = {"InstanceType": "ml.m4.xlarge", "InstanceCount": 1}

        try:
            transform_job_arn = start_batch_transform_job(
                self._sagemaker,
                transform_job_name,
                model_name,
                # FUTURE/TODO "MaxConcurrentTransforms": 16,
                # FUTURE/TODO "MaxPayloadInMB": 6,
                # FUTURE/TODO "BatchStrategy": "SingleRecord",
                {
                    "S3OutputPath": materialized_output.get_materialized_resource_paths()[0],
                    "Accept": materialized_output.resource_access_spec.content_type.value,
                    "AssembleWith": "Line",
                },
                {
                    # FUTURE / TODO support multiple partitions via ManifestFile
                    "DataSource": {
                        "S3DataSource": {
                            "S3DataType": "S3Prefix",
                            "S3Uri": materialized_dataset_signal.get_materialized_resource_paths()[0],
                        }
                    },
                    "ContentType": materialized_dataset_signal.resource_access_spec.content_type.value,
                    "SplitType": "Line",
                    "CompressionType": "None",
                },
                transform_resources,
            )
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in self.CLIENT_RETRYABLE_EXCEPTION_LIST or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                failed_response_type = ComputeFailedResponseType.TRANSIENT
            elif error_code in ["ResourceNotFound"]:
                failed_response_type = ComputeFailedResponseType.BAD_SLOT
            else:
                failed_response_type = ComputeFailedResponseType.UNKNOWN

            return ComputeFailedResponse(
                failed_response_type,
                ComputeResourceDesc(transform_job_name, transform_job_arn, driver=self.__class__),
                error_code,
                str(error.response["Error"]),
            )

        return ComputeSuccessfulResponse(
            ComputeSuccessfulResponseType.PROCESSING,
            ComputeSessionDesc(transform_job_name, ComputeResourceDesc(transform_job_name, transform_job_arn, driver=self.__class__)),
        )

    @overrides
    def get_session_state(
        self, session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
    ) -> ComputeSessionState:
        transform_job_name = session_desc.session_id
        execution_details = None
        try:
            job_run = exponential_retry(
                describe_sagemaker_transform_job,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._sagemaker,
                transform_job_name,
                **{MAX_SLEEP_INTERVAL_PARAM: 8},
            )
            start = job_run.get("TransformStartTime", None)
            end = job_run.get("TransformEndTime", None)
            execution_details = ComputeExecutionDetails(start, end, dict(job_run))

            session_state = get_transform_job_run_state_type(job_run)
            if session_state == ComputeSessionStateType.FAILED:
                failure_type = get_transform_job_run_failure_state_type(job_run)
                execution_details.details.update(
                    {"ErrorMessage": job_run.get("FailureReason", "Unknown: failureReason for transform job not provided by Sagemaker.")}
                )
                return ComputeFailedSessionState(failure_type, session_desc, [execution_details])
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in self.CLIENT_RETRYABLE_EXCEPTION_LIST or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                session_state = ComputeSessionStateType.TRANSIENT_UNKNOWN
            else:
                failure_type = (
                    ComputeFailedSessionStateType.NOT_FOUND
                    if error in ["ResourceNotFound", "EntityNotFoundException"]
                    else ComputeFailedSessionStateType.UNKNOWN
                )
                return ComputeFailedSessionState(failure_type, session_desc, [execution_details])

        return ComputeSessionState(session_desc, session_state, [execution_details])

    @overrides
    def terminate_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        """Compute is terminated. Irrespective of compute state, this will always be called. Add completion file."""
        if active_compute_record.session_state.state_type == ComputeSessionStateType.COMPLETED:
            # first map materialized output into internal signal form
            output = self.get_platform().storage.map_materialized_signal(active_compute_record.materialized_output)
            # e.g /internal_data/{DATA_ID]/[PARTITION_1]/.../[PARTITION_N]
            path = output.get_materialized_resource_paths()[0]
            # 1- activate completion, etc
            if output.domain_spec.integrity_check_protocol:
                from intelliflow.core.signal_processing.analysis import INTEGRITY_CHECKER_MAP

                integrity_checker = INTEGRITY_CHECKER_MAP[output.domain_spec.integrity_check_protocol.type]
                # e.g completion_resource_name -> _SUCCESS
                completion_resource_name = integrity_checker.get_required_resource_name(
                    output.resource_access_spec, output.domain_spec.integrity_check_protocol
                )
                if completion_resource_name:  # ex: _SUCCESS file/object
                    # remove anything before (internal_data) folder (do it in a decoupled way)
                    # e.g /internal_data/1/2023-09-22 -> internal_data/1/2023-09-22
                    folder = path[path.find(output.resource_access_spec.FOLDER) :]
                    self.get_platform().storage.save("", [folder], completion_resource_name)

    @overrides
    def kill_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        # no need to deal with model, as terminate_session will be called in subsequent cycles automatically
        # skip if initial response ('state') is not SUCCESS to 'compute' call
        if active_compute_record.state.response_type != ComputeResponseType.SUCCESS:
            return

        # ComputeRecord.state is the "initial state" of the execution, which is the response of compute()
        # ComputeRecord.session_state is the "running state" of the execution, which is the response of
        # get_session_state()
        # The compute driver will have session_state IFF "initial state" was successful, meaning the execution started
        # without an issue (e.g. wrong arguments, missing permission, etc)
        if active_compute_record.session_state and active_compute_record.session_state.session_desc:
            most_recent_session_desc: ComputeSessionDesc = active_compute_record.session_state.session_desc
        else:
            state = cast(ComputeSuccessfulResponse, active_compute_record.state)
            most_recent_session_desc: ComputeSessionDesc = state.session_desc
        transform_job_name = most_recent_session_desc.session_id
        exponential_retry(
            stop_batch_transform_job,
            # ValidationException happens when the BatchTransform job has already been completed when we stop it.
            self.CLIENT_RETRYABLE_EXCEPTION_LIST.union({"ValidationException"}),
            self._sagemaker,
            transform_job_name,
        )

    @overrides
    def provide_runtime_trusted_entities(self) -> List[str]:
        return ["sagemaker.amazonaws.com"]

    @overrides
    def provide_runtime_default_policies(self) -> List[str]:
        # https://docs.aws.amazon.com/sagemaker/latest/dg/security-iam-awsmanpol.html
        # To keep the minimalist privilege, we don't use managed policy here
        # We set up permissions explicitly in inlined policies, see provide_runtime_permissions()
        return []

    @classmethod
    def _build_transform_job_arn(cls, region: str, account_id: str, transform_job_name: str):
        return f"arn:aws:sagemaker:{region}:{account_id}:transform-job/{transform_job_name.lower()}"

    @classmethod
    def _build_model_arn(cls, region: str, account_id: str, model_name: str):
        return f"arn:aws:sagemaker:{region}:{account_id}:model/{model_name.lower()}"

    @overrides
    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        # If there is no <Route> detected (see hook_internal) for this driver,
        # then no need to provide extra permissions.
        # If they were added previously, they will be wiped out in the current activation.
        if not self._pending_internal_routes:
            return []
        # https://docs.aws.amazon.com/sagemaker/latest/dg/api-permissions-reference.html refer
        # create transform jobs for permissions
        permissions = [
            ConstructPermission(
                [
                    self._build_transform_job_arn(
                        self._region, self._account_id, self.TRANSFORM_JOB_NAME_PATTERN.format(self.context_id, "*")
                    )
                ],
                ["sagemaker:*"],
            ),
            ConstructPermission(
                [self._build_model_arn(self._region, self._account_id, self.TRANSFORM_MODEL_NAME_PATTERN.format(self.context_id, "*"))],
                ["sagemaker:*"],
            ),
            ConstructPermission(
                [f"arn:aws:ecr:{self._region}:{self._account_id}:repository/*"],
                [
                    "ecr:Describe*",
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchGetImage",
                ],
            ),
            ConstructPermission(
                ["*"],
                [
                    "cloudwatch:PutMetricData",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:CreateLogGroup",
                    "logs:DescribeLogStreams",
                    "ecr:GetAuthorizationToken",
                ],
            ),
            ConstructPermission(["*"], ["sagemaker:Get*", "sagemaker:List*", "sagemaker:Describe*"]),
            ConstructPermission([self._params[AWSCommonParams.IF_EXE_ROLE]], ["iam:PassRole"]),
        ]

        # pending_internal_routes is up-to-date as hook_internal has been called before this method
        # Copied from other drivers (lambda, training_job) etc.
        for route in self._pending_internal_routes:
            for slot in route.slots:
                if slot.type.is_batch_compute() and slot.permissions:
                    for compute_perm in slot.permissions:
                        # Just provide permission flexibility despite that this should not be supported with Athena.
                        if compute_perm.context != PermissionContext.DEVTIME:
                            permissions.append(ConstructPermission(compute_perm.resource, compute_perm.action))

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
    @overrides
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        """Provide permissions required by development role.
        These are going to be kind of a superset of the ones provide in provide_runtime_permissions (exec role perms)
        Rather than focusing only on the activation time operations (create, update, etc) we need to capture the case
        for "local development" where Application::execute, Application::process can trigger executions locally using
        local orchestrator and (depending on use-case) even subsequent tracking (the rest of orchestrator state-machine
        around an execution) can happen locally on a dev endpoint. These are basically what exec role would normally need
        at runtime.

        So dev-role needs both resource creation/update + exec role permissions.
        """
        region = params[AWSCommonParams.REGION]
        account_id = params[AWSCommonParams.ACCOUNT_ID]
        # TODO adding context_id to devtime_permissions.
        return [
            ConstructPermission(
                [cls._build_transform_job_arn(region, account_id, cls.TRANSFORM_JOB_NAME_PATTERN.format("*", "*"))], ["sagemaker:*"]
            ),
            ConstructPermission(
                [cls._build_model_arn(region, account_id, cls.TRANSFORM_MODEL_NAME_PATTERN.format("*", "*"))], ["sagemaker:*"]
            ),
            ConstructPermission(["*"], ["sagemaker:Get*", "sagemaker:List*", "sagemaker:Describe*"]),
            ConstructPermission([f"arn:aws:ecr:{region}:{account_id}:repository/*"], ["ecr:Describe*"]),
            ConstructPermission(
                ["*"],
                [
                    "s3:List*",
                    "s3:Get*",
                    "s3:Head*",
                ],
            ),
        ]

    @overrides
    def _provide_system_metrics(self) -> List[Signal]:
        return []

    @overrides
    def _provide_internal_metrics(self) -> List[ConstructInternalMetricDesc]:
        return []

    @overrides
    def _provide_route_metrics(self, route: Route) -> List[ConstructInternalMetricDesc]:
        return []

    @overrides
    def _provide_internal_alarms(self) -> List[Signal]:
        return []

    @overrides
    def activate(self) -> None:
        # TODO anything to set up during activation?
        #  if any, do not forget to clean up in "terminate"

        super().activate()

    @overrides
    def terminate(self) -> None:
        super().terminate()
        # TODO wipe-out resources created in "activate"

        # TODO how to cleanup dynamic compute based resources? e.g models (we cannot wait for 'terminate_session')

    @overrides
    def check_update(self, prev_construct: "BaseConstruct") -> None:
        """Change set management"""
        super().check_update(prev_construct)

    @overrides
    def hook_internal(self, route: "Route") -> None:
        """Early stage check on a new route, so that we can fail fast before the whole activation.

        At this point, it is guaranteed that this driver will be used to execute at least one of the slots of this route
        at runtime.
        """
        super().hook_internal(route)

        # TODO

    @overrides
    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    @overrides
    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        pass

    @overrides
    def _process_internal_signals(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    @overrides
    def _process_construct_connections(
        self, new_construct_conns: Set["_PendingConnRequest"], current_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    @overrides
    def _process_security_conf(self, new_security_conf: ConstructSecurityConf, current_security_conf: ConstructSecurityConf) -> None:
        pass

    @overrides
    def rollback(self) -> None:
        super().rollback()

    @overrides
    def _revert_external(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        pass

    @overrides
    def _revert_internal(self, routes: Set[Route], prev_routes: Set[Route]) -> None:
        pass

    @overrides
    def _revert_internal_signals(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        pass

    @overrides
    def _revert_construct_connections(
        self, construct_conns: Set["_PendingConnRequest"], prev_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    @overrides
    def _revert_security_conf(self, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        pass

    @property
    def context_id(self) -> str:
        return self.get_platform().context_id
