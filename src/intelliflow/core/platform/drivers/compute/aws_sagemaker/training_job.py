# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple, cast

from botocore.exceptions import ClientError

from intelliflow.core.permission import PermissionContext
from intelliflow.core.signal_processing import DimensionFilter, DimensionSpec, Signal, Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal_source import (
    DATA_TYPE_KEY,
    DATASET_HEADER_KEY,
    DATASET_SCHEMA_FILE_KEY,
    DATASET_SCHEMA_TYPE_KEY,
    MODEL_FORMAT_KEY,
    MODEL_METADATA,
    DatasetSignalSourceAccessSpec,
    DataType,
    ModelSignalSourceFormat,
    SignalSourceAccessSpec,
    SignalSourceType,
)
from intelliflow.core.signal_processing.slot import SlotType

from ....constructs import BatchCompute, ConstructInternalMetricDesc, ConstructParamsDict, ConstructPermission, ConstructSecurityConf
from ....definitions.aws.common import AWS_COMMON_RETRYABLE_ERRORS, MAX_SLEEP_INTERVAL_PARAM
from ....definitions.aws.common import CommonParams as AWSCommonParams
from ....definitions.aws.common import exponential_retry
from ....definitions.aws.sagemaker.client_wrapper import (
    ImageScope,
    SagemakerTrainingInputMode,
    convert_image_uri_to_arn,
    get_builtin_algo_content_types,
    get_default_hyper_parameters_for_builtin_algo,
    get_default_input_mode_for_builtin_algo,
    get_image_for_builtin,
    get_training_job_run_failure_type,
    get_training_job_run_state_type,
    is_builtin_algo_name,
    map_training_image_to_builtin_algo,
    validate_hyper_parameters_for_builtin_algo,
    validate_inputs_for_builtin_algo,
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


class AWSSagemakerTrainingJobBatchCompute(AWSConstructMixin, BatchCompute):
    """AWS Sagemaker Training Job BatchCompute impl"""

    CLIENT_RETRYABLE_EXCEPTION_LIST: Set[str] = {"ResourceInUse", "InternalServerException"}

    # TODO change to IntelliFlow-Training-{APP ID}-{UNIQUE ID}
    TRAINING_JOB_NAME_PATTERN = "IntelliFlow-Training-{}"

    @classmethod
    def driver_spec(cls) -> DimensionFilter:
        return DimensionFilter.load_raw(
            {
                Lang.AWS_SAGEMAKER_TRAINING_JOB: {ABI.NONE: {"*": {"*": {}}}},
            }
        )

    def __init__(self, params: ConstructParamsDict) -> None:
        super().__init__(params)
        self._sagemaker = self._session.client("sagemaker", region_name=self._region)

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._sagemaker = self._session.client("sagemaker", region_name=self._region)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._sagemaker = None

    def _validate_route(self, inputs: List[Signal], slot: Slot, user_attrs: Dict[str, Any]) -> None:
        # inputs must be of supported types
        # INTERNAL at this point is guaranteed to be on S3 as we make sure that Storage is S3 based in dev_init
        if not inputs:
            raise ValueError(f"Number of inputs must be greater than zero for driver {self.__class__.__name__!r}!")
        # TODO remove when algo specific validation is enabled below
        if not all(
            [
                input.resource_access_spec.source in [SignalSourceType.INTERNAL, SignalSourceType.S3]
                for input in inputs
                if isinstance(input.resource_access_spec, DatasetSignalSourceAccessSpec)
            ]
        ):
            raise ValueError(f"Inputs must reside in AWS S3 for driver {self.__class__.__name__!r}!")

        # inputs should not have range (e.g should have only one partition).
        # check "compute" and see how we use input materialized path as S3Uri with no ability to do filtering on S3Prefix.
        # this is different than create_transform_job when partitions root path can be given and then DataProcessing can be used to capture input range via filtering.
        for input in inputs:
            if len(input.get_materialized_resource_paths()) > 1:
                raise ValueError(
                    f"Input dataset {input.alias!r} cannot have multiple partitions for training using driver {self.__class__.__name__!r}!"
                )

        # now check Slot parameters
        extra_params: Dict[str, Any] = dict(slot.extra_params)
        algo_specification = extra_params["AlgorithmSpecification"]

        # Validation based on "AlgorithmSpecification"
        training_image = algo_specification.get("TrainingImage", None)
        # - custom algo or from marketplace
        # - we support builtin Sagemaker algos to be provided using this parameter as well
        algo_name = algo_specification.get("AlgorithmName", None)
        builtin_algo_name = None
        if algo_name and training_image:
            raise ValueError(f"Cannot declare 'AlgorithmName' and 'TrainingName' at the same time for an AWS Sagemaker training job!")
        elif not (algo_name or training_image):
            raise ValueError(f"Either 'AlgorithmName' or 'TrainingName' must be declared for an AWS Sagemaker training job!")
        elif training_image:
            # https://docs.aws.amazon.com/sagemaker/latest/dg/ecr-us-east-1.html#xgboost-us-east-1.title
            builtin_algo_name = map_training_image_to_builtin_algo(training_image)
        elif is_builtin_algo_name(algo_name):
            builtin_algo_name = algo_name
            algo_version = algo_specification.get("AlgorithmVersion", None)
            if not get_image_for_builtin(algo_name, self.region, ImageScope.TRAINING, algo_version):
                raise NotImplementedError(f"Cannot use unsupported algorithm {algo_name!r} in region {self.region!r}!")

        if builtin_algo_name:
            # TODO validate inputs based on builtin algo
            validate_inputs_for_builtin_algo(inputs, builtin_algo_name)

            # do validations based on
            #   https://docs.aws.amazon.com/sagemaker/latest/dg/cdf-training.html
            supported_content_types = get_builtin_algo_content_types(builtin_algo_name)
            for input in inputs:
                if input.resource_access_spec.content_type and input.resource_access_spec.content_type not in supported_content_types:
                    raise ValueError(
                        f"Input {input.alias!r} must be of one of the following content types {supported_content_types!r} for driver {self.__class__.__name__!r}!"
                    )

            if "HyperParameters" in extra_params:
                validate_hyper_parameters_for_builtin_algo(builtin_algo_name, extra_params["HyperParameters"])
            else:
                default_hyper_parameters = get_default_hyper_parameters_for_builtin_algo(builtin_algo_name)
                module_logger.critical(
                    f"'HyperParameters' ({default_hyper_parameters!r}) will be automatically set for builtin algo {builtin_algo_name!r}."
                )
        else:
            module_logger.critical(f"Algorithm provided is not builtin! Inputs cannot be validated for compatibility.")
            if "HyperParameters" not in extra_params:
                raise ValueError(f"'HyperParameters' for custom algorithm must be provided!")

    def provide_output_attributes(self, inputs: List[Signal], slot: Slot, user_attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # this callback is received during node creation, even before activation
        # so doing validations here early would provide the best development experience
        self._validate_route(inputs, slot, user_attrs)

        if user_attrs.get(DATA_TYPE_KEY, DataType.MODEL_ARTIFACT) != DataType.MODEL_ARTIFACT:
            raise ValueError(
                f"{DATA_TYPE_KEY!r} must be defined as {DataType.MODEL_ARTIFACT} or left undefined on an {self.__class__.__name__!r} output!"
            )

        if user_attrs.get(DATASET_SCHEMA_FILE_KEY, None) != None:
            raise ValueError(
                f"{DATASET_SCHEMA_FILE_KEY!r} must be defined as None or left undefined on an {self.__class__.__name__!r} output!"
            )

        if user_attrs.get(DATASET_SCHEMA_TYPE_KEY, None) != None:
            raise ValueError(
                f"{DATASET_SCHEMA_TYPE_KEY!r} must be defined as None or left undefined on an {self.__class__.__name__!r} output!"
            )

        if user_attrs.get(DATASET_HEADER_KEY, False) != False:
            raise ValueError(f"{DATASET_HEADER_KEY!r} must be defined as False or left undefined on an {self.__class__.__name__!r} output!")

        return {
            DATA_TYPE_KEY: DataType.MODEL_ARTIFACT,
            MODEL_FORMAT_KEY: ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB,
            MODEL_METADATA: dict(slot.extra_params),
            DATASET_SCHEMA_FILE_KEY: None,
            DATASET_SCHEMA_TYPE_KEY: None,
            DATASET_HEADER_KEY: False,
        }

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

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)

        if not platform.storage.get_storage_resource_path().lower().startswith("arn:aws:s3"):
            raise TypeError(f"Internal storage should be based on S3 for {self.__class__.__name__!r} driver to work!")

        self.validate_job_names()

    def validate_job_names(self):
        # TODO
        # check if app-name is ok, both length and format (if used as part of internal training-job naming scheme here)
        # raise ValueError(
        #     f"Cannot dev_init {self.__class__.__name__} due to invalid app name {job_name} doesn't meet Sagemaker training job name "
        #     f"pattern"
        # )
        pass

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._sagemaker = self._session.client("sagemaker", region_name=self._region)

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

        unique_compute_id: str = str(uuid.uuid1())
        training_job_name = self.TRAINING_JOB_NAME_PATTERN.format(unique_compute_id).lower()
        training_job_arn = self._build_training_job_arn(self.region, self.account_id, training_job_name)

        # refer
        #  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_training_job

        # 1- TrainingJobName
        api_params.update({"TrainingJobName": training_job_name})

        # 2- AlgorithmSpecification
        algo_specification = api_params["AlgorithmSpecification"]
        training_image = algo_specification.get("TrainingImage", None)
        builtin_algo_name = None
        if not training_image:
            # - custom algo or from marketplace
            # - we support builtin Sagemaker algos to be provided using this parameter as well
            algo_name = algo_specification.get("AlgorithmName", None)
            if is_builtin_algo_name(algo_name):
                algo_version = algo_specification.get("AlgorithmVersion", None)
                training_image = get_image_for_builtin(algo_name, self.region, ImageScope.TRAINING, algo_version)
                algo_specification.update({"TrainingImage": training_image})
                # end up replacing builtin algo with training image
                del algo_specification["AlgorithmName"]
                builtin_algo_name = algo_name
        else:
            builtin_algo_name = map_training_image_to_builtin_algo(training_image)

        # TODO validate in hook_internal?
        if "TrainingInputMode" not in algo_specification:
            input_mode = (
                get_default_input_mode_for_builtin_algo(builtin_algo_name) if builtin_algo_name else SagemakerTrainingInputMode.FILE
            )
            algo_specification.update({"TrainingInputMode": input_mode.value})

        # 3- RoleArn
        api_params.update({"RoleArn": self._params[AWSCommonParams.IF_EXE_ROLE]})

        # 4-
        if "ResourceConfig" not in api_params:
            # TODO
            api_params.update({"ResourceConfig": {"InstanceCount": 1, "InstanceType": "ml.m5.xlarge", "VolumeSizeInGB": 30}})

        desired_instance_count = api_params["ResourceConfig"]["InstanceCount"]

        # 5- InputDataConfig
        input_data_config = []
        for materialized_input in materialized_inputs:
            if materialized_input.resource_access_spec.data_type == DataType.DATASET or isinstance(
                materialized_input.resource_access_spec, DatasetSignalSourceAccessSpec
            ):
                # TODO validate 'desired_instance_count <= number of part objects'. part objects = total # objects - (protocol file + schema file)
                #  we can / should do it in compute time (cannot do it during activation)
                input_data_config.append(
                    {
                        "ChannelName": materialized_input.alias,
                        "DataSource": {
                            "S3DataSource": {
                                "S3DataType": "S3Prefix",
                                "S3Uri": materialized_input.get_materialized_resource_paths()[0],
                                "S3DataDistributionType": "ShardedByS3Key" if desired_instance_count > 1 else "FullyReplicated",
                            },
                        },
                        "ContentType": materialized_input.resource_access_spec.content_type.value,
                        "CompressionType": "None",
                    }
                )

        api_params.update({"InputDataConfig": input_data_config})

        # 6- output
        api_params.update(
            {
                "OutputDataConfig": {"S3OutputPath": materialized_output.get_materialized_resource_paths()[0]},
            }
        )

        # 7- HyperParameters
        if "HyperParameters" not in api_params:
            builtin_algo_name = map_training_image_to_builtin_algo(training_image)
            api_params.update({"HyperParameters": get_default_hyper_parameters_for_builtin_algo(builtin_algo_name)})

        # StoppingCondition
        if "StoppingCondition" not in api_params:
            api_params.update(
                {
                    "StoppingCondition": {
                        # TODO
                        "MaxRuntimeInSeconds": 24
                        * 60
                        * 60
                    }
                }
            )

        # Retry
        if "RetryStrategy" not in api_params:
            api_params.update(
                {
                    "RetryStrategy": {
                        # TODO
                        "MaximumRetryAttempts": 2
                    }
                }
            )

        # TODO check other parameters

        # TODO declare input_fn and output_fn for PARQUET
        #    https://stackoverflow.com/questions/62415237/aws-sagemaker-using-parquet-file-for-batch-transform-job

        # now clean the output partition (delete previous traning job runs)
        # entire_domain=False just uses the material dimension values to target the current output partition only
        output_internal_view = self.get_platform().storage.map_materialized_signal(materialized_output)
        self.get_platform().storage.delete_internal(output_internal_view, entire_domain=False)

        # call create_training_job
        try:
            # we need to keep the exp-retry-count low due to being on orchestration critical-path here
            api_params.update({MAX_SLEEP_INTERVAL_PARAM: 8})
            exponential_retry(self._sagemaker.create_training_job, ["AccessDeniedException"], **api_params)  # IAM propagation for exec role
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in self.CLIENT_RETRYABLE_EXCEPTION_LIST or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                failed_response_type = ComputeFailedResponseType.TRANSIENT
            elif error_code in ["ResourceNotFound", "ResourceLimitExceeded"]:
                failed_response_type = ComputeFailedResponseType.BAD_SLOT
            else:
                failed_response_type = ComputeFailedResponseType.UNKNOWN

            return ComputeFailedResponse(
                failed_response_type,
                ComputeResourceDesc(training_job_name, training_job_arn, driver=self.__class__),
                error_code,
                # TODO
                str(error.response["Error"]),
            )

        return ComputeSuccessfulResponse(
            ComputeSuccessfulResponseType.PROCESSING,
            ComputeSessionDesc(training_job_name, ComputeResourceDesc(training_job_name, training_job_arn, driver=self.__class__)),
        )

    def get_session_state(
        self, session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
    ) -> ComputeSessionState:
        training_job_name = session_desc.session_id
        execution_details = None
        try:
            # We are actually advised to avoid retries in critical orchestration paths which will eventually be
            # retried as long as right session state (TRANSIENT) is returned. But we still do exp retry (with a max time
            # less than usual) as an optimization based on unlikeliness of issues with this api (relative to compute).
            job_run = exponential_retry(
                self._sagemaker.describe_training_job,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                **{"TrainingJobName": training_job_name, MAX_SLEEP_INTERVAL_PARAM: 8},
            )
            # now let's interpret the response in our Compute model
            start = job_run.get("TrainingStartTime", None)
            end = job_run.get("TrainingEndTime", None)
            execution_details = ComputeExecutionDetails(start, end, dict(job_run))

            session_state = get_training_job_run_state_type(job_run)
            if session_state == ComputeSessionStateType.FAILED:
                failure_type = get_training_job_run_failure_type(job_run)
                execution_details.details.update(
                    {"ErrorMessage": job_run.get("FailureReason", "Unknown: failureReason for training job not provided by Sagemaker.")}
                )
                return ComputeFailedSessionState(failure_type, session_desc, [execution_details])
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in self.CLIENT_RETRYABLE_EXCEPTION_LIST or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                # don't mark it as failed but let orchestration know about that session state could not be retrieved
                session_state = ComputeSessionStateType.TRANSIENT_UNKNOWN
            else:
                failure_type = (
                    ComputeFailedSessionStateType.NOT_FOUND
                    if error_code in ["ResourceNotFound", "EntityNotFoundException"]
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
            training_job_name = most_recent_session_desc.session_id
            exponential_retry(self._sagemaker.stop_training_job, self.CLIENT_RETRYABLE_EXCEPTION_LIST, TrainingJobName=training_job_name)

    def provide_runtime_trusted_entities(self) -> List[str]:
        # TODO
        return ["sagemaker.amazonaws.com"]

    def provide_runtime_default_policies(self) -> List[str]:
        # TODO probably won't need any default policies for training job?
        return []

    @classmethod
    def _build_training_job_arn(cls, region: str, account_id: str, training_job_name: str):
        return f"arn:aws:sagemaker:{region}:{account_id}:training-job/{training_job_name.lower()}"

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        # If there is no <Route> detected (see hook_internal) for this driver,
        # then no need to provide extra permissions.
        # If they were added previously, they will be wiped out in the current activation.
        if not self._pending_internal_routes:
            return []

        # allow exec-role (post-activation, cumulative list of all trusted entities [AWS services]) to do the following;
        # what kind of permissions Sagemaker will require once it assumes our execution role to run the training job?
        # also refer
        #  https://docs.aws.amazon.com/sagemaker/latest/dg/sagemaker-roles.html#sagemaker-roles-createtrainingjob-perms
        permissions = [
            # allow our role to create/describe the jobs here
            ConstructPermission(
                [self._build_training_job_arn(self._region, self._account_id, self.TRAINING_JOB_NAME_PATTERN.format("*"))], ["sagemaker:*"]
            ),
            ConstructPermission(["*"], ["sagemaker:Get*", "sagemaker:List*", "sagemaker:Describe*"]),
            # this will also be needed by Sagemaker when the exec role is assumed
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
            # must add a policy to allow your users the iam:PassRole permission for IAM roles to match your naming convention
            ConstructPermission([self._params[AWSCommonParams.IF_EXE_ROLE]], ["iam:PassRole"]),
        ]

        permissions.extend(self._provide_runtime_permissions_for_training_images())

        # should not be a case for Sagemaker training job.
        # but still we can offer this as a convenience to user.
        # e.g extra KMS permissions?
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
        return [
            ConstructPermission(
                [cls._build_training_job_arn(region, account_id, cls.TRAINING_JOB_NAME_PATTERN.format("*"))], ["sagemaker:*"]
            ),
            ConstructPermission(["*"], ["sagemaker:Get*", "sagemaker:List*", "sagemaker:Describe*"]),
            ConstructPermission([f"arn:aws:ecr:{region}:{account_id}:repository/*"], ["ecr:Describe*"]),
            # we cannot assume that the following will be provided by AWS based Processor, BatchCompute implementations.
            # we favor redundancy over coupling
            ConstructPermission(
                ["*"],
                [
                    "s3:List*",
                    "s3:Get*",
                    "s3:Head*",
                ],
            ),
        ]

    def _provide_system_metrics(self) -> List[Signal]:
        return []

    def _provide_internal_metrics(self) -> List[ConstructInternalMetricDesc]:
        return []

    def _provide_route_metrics(self, route: Route) -> List[ConstructInternalMetricDesc]:
        return []

    def _provide_internal_alarms(self) -> List[Signal]:
        return []

    def activate(self) -> None:
        # FUTURE anything to setup during activation?
        #  if any, donot forget to clean up in "terminate"

        super().activate()

    def rollback(self) -> None:
        super().rollback()

    def terminate(self) -> None:
        super().terminate()
        # FUTURE wipe-out resources created in "activate"

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)

    def hook_internal(self, route: "Route") -> None:
        """Early stage check on a new route during the activation, so that we can fail fast before the whole activation.

        At this point, it is guaranteed that this driver will be used to execute at least one of the slots of this route
        at runtime.
        """
        super().hook_internal(route)

    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        pass

    def _process_internal_signals(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_construct_connections(
        self, new_construct_conns: Set["_PendingConnRequest"], current_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _process_security_conf(self, new_security_conf: ConstructSecurityConf, current_security_conf: ConstructSecurityConf) -> None:
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

    def _revert_security_conf(self, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        pass

    def _provide_runtime_permissions_for_training_images(self):
        # iterate over each route and extract TrainingImage from slots
        custom_training_images = []
        builtin_training_images = []
        for route in self._pending_internal_routes:
            for slot in route.slots:
                if slot.type != SlotType.ASYNC_BATCH_COMPUTE or slot.code_lang != Lang.AWS_SAGEMAKER_TRAINING_JOB:
                    continue
                api_params = dict(slot.extra_params)
                # At this point, the route had been validated early before, we can assume it's valid
                algo_specification = api_params["AlgorithmSpecification"]
                algo_name = algo_specification.get("AlgorithmName", None)
                training_image = algo_specification.get("TrainingImage", None)
                if training_image:
                    custom_training_images.append(training_image)
                elif is_builtin_algo_name(algo_name):
                    algo_version = algo_specification.get("AlgorithmVersion", None)
                    training_image = get_image_for_builtin(algo_name, self.region, ImageScope.TRAINING, algo_version)
                    builtin_training_images.append(training_image)

        permissions = []
        for image_uris in [custom_training_images, builtin_training_images]:
            if image_uris:
                permissions.append(
                    ConstructPermission(
                        [convert_image_uri_to_arn(uri) for uri in image_uris],
                        [
                            "ecr:Describe*",
                            "ecr:BatchCheckLayerAvailability",
                            "ecr:GetDownloadUrlForLayer",
                            "ecr:BatchGetImage",
                            "ecr:GetAuthorizationToken",
                        ],
                    )
                )
        return permissions
