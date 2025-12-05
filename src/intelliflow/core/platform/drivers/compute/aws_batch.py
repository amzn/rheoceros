import copy
import logging
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple, cast

from botocore.exceptions import ClientError, WaiterError

from intelliflow.core.permission import PermissionContext
from intelliflow.core.signal_processing import DimensionFilter, DimensionSpec, Signal, Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal_source import (
    DATA_FORMAT_KEY,
    DATA_TYPE_KEY,
    DATASET_FORMAT_KEY,
    DATASET_HEADER_KEY,
    DATASET_SCHEMA_FILE_KEY,
    DATASET_SCHEMA_TYPE_KEY,
    DatasetSchemaType,
    DatasetSignalSourceFormat,
    DataType,
    SignalSourceAccessSpec,
    SignalSourceType,
)
from intelliflow.core.signal_processing.slot import SlotType

from ...constructs import BatchCompute, ConstructInternalMetricDesc, ConstructParamsDict, ConstructPermission, ConstructSecurityConf
from ...definitions.aws.batch.client_wrapper import (
    INTELLIFLOW_AWS_BATCH_CANCELLATION_REASON,
    create_compute_environment_waiter,
    create_job_queue_waiter,
    describe_compute_environments,
    describe_job_definitions,
    describe_job_queues,
    describe_jobs,
    get_batch_job_run_failure_type,
    get_batch_job_run_state_type,
    submit_job,
)
from ...definitions.aws.batch.common import AWS_BATCH_ORCHESTRATOR_ROLE_ARN_PARAM
from ...definitions.aws.common import AWS_COMMON_RETRYABLE_ERRORS, IF_EXE_ROLE_NAME_FORMAT, MAX_SLEEP_INTERVAL_PARAM
from ...definitions.aws.common import CommonParams as AWSCommonParams
from ...definitions.aws.common import exponential_retry, get_assumed_role_session
from ...definitions.aws.emr.client_wrapper import create_job_flow_instance_profile, delete_instance_profile
from ...definitions.common import ActivationParams
from ...definitions.compute import (
    ComputeExecutionDetails,
    ComputeFailedResponse,
    ComputeFailedResponseType,
    ComputeFailedSessionState,
    ComputeFailedSessionStateType,
    ComputeResourceDesc,
    ComputeResponse,
    ComputeResponseType,
    ComputeRuntimeTemplateRenderer,
    ComputeSessionDesc,
    ComputeSessionState,
    ComputeSessionStateType,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from ..aws_common import AWSConstructMixin

module_logger = logging.getLogger(__file__)


class AWSBatchCompute(AWSConstructMixin, BatchCompute):
    """AWS Batch BatchCompute impl"""

    CLIENT_RETRYABLE_EXCEPTION_LIST: Set[str] = {"ServerException"}

    # IntelliFlow-Batch-{APP ID}-{UNIQUE ID}
    BATCH_JOB_NAME_PATTERN = "IntelliFlow-Batch-{}-{}"
    # IntelliFlow-Batch-{APP ID}-{USER_PROVIDED_NAME}
    BATCH_COMPUTE_ENV_NAME_PATTERN = "IntelliFlow-Batch-{}-{}"
    # IntelliFlow-Batch-{APP ID}-{USER_PROVIDED_NAME}
    BATCH_JOB_QUEUE_NAME_PATTERN = "IntelliFlow-Batch-{}-{}"
    # IntelliFlow-Batch-{APP ID}-{USER_PROVIDED_NAME}
    BATCH_JOB_DEFINITION_NAME_PATTERN = "IntelliFlow-Batch-{}-{}"

    COMMAND_EVAL_KEYWORD = "^[eval]"

    @classmethod
    def driver_spec(cls) -> DimensionFilter:
        return DimensionFilter.load_raw(
            {
                Lang.AWS_BATCH_JOB: {ABI.NONE: {"*": {"*": {}}}},
            }
        )

    def __init__(self, params: ConstructParamsDict) -> None:
        super().__init__(params)
        self._batch = self._session.client("batch", region_name=self._region)

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._batch = self._session.client("batch", region_name=self._region)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._batch = None

    def _validate_route(self, inputs: List[Signal], slot: Slot, user_attrs: Dict[str, Any]) -> None:
        # 1- we let no-input runs with batch jobs, so no need to check input count

        # 2- check job parameters
        # now check Slot parameters
        extra_params: Dict[str, Any] = dict(slot.extra_params)

        # "command'
        containerOverrides = extra_params.get("containerOverrides", None)
        if containerOverrides:
            command = containerOverrides.get("command", None)
            if not command or not isinstance(command, list):
                raise ValueError(
                    f"'command' parameter of type <list> is mandated by driver {self.__class__.__name__!r} for a AWS Bath job compute!"
                )

        job_def = extra_params.get("jobDefinition", None)
        if not job_def or not (isinstance(job_def, str) or isinstance(job_def, dict)):
            raise ValueError(
                f"'jobDefinition' parameter of type <str> (for an ARN) or type <dict> (for a managed job definition) is mandated by driver {self.__class__.__name__!r} for a AWS Bath job compute!"
            )
        elif isinstance(job_def, dict):
            # Refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/register_job_definition.html
            if "jobDefinitionName" not in job_def or "type" not in job_def:
                raise ValueError(
                    f"Managed 'jobDefinition' parametrization must be based on 'https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/register_job_definition.html'"
                )

        job_queue = extra_params.get("jobQueue", None)
        if not job_queue or not (isinstance(job_queue, str) or isinstance(job_queue, dict)):
            raise ValueError(
                f"'jobQueue' parameter of type <str> (for an ARN) or type <dict> (for a managed job queue) is mandated by driver {self.__class__.__name__!r} for a AWS Bath job compute!"
            )
        elif isinstance(job_queue, dict):
            # Refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/create_job_queue.html
            if "jobQueueName" not in job_queue or "priority" not in job_queue or "computeEnvironmentOrder" not in job_queue:
                raise ValueError(
                    f"Managed 'jobQueue' parametrization must be based on 'https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/create_job_queue.html'"
                )
            compute_env_order = job_queue["computeEnvironmentOrder"]
            for comp_item in compute_env_order:
                compute_env = comp_item["computeEnvironment"]
                if not compute_env or not (isinstance(compute_env, str) or isinstance(compute_env, dict)):
                    raise ValueError(
                        f"'computeEnvironment' sub-parameter of a framework managed AWS Batch 'jobQueue' must be of type <str> (for an env name) or type <dict> (for framework managed env)!"
                    )
                elif isinstance(compute_env, dict):
                    if "computeEnvironmentName" not in compute_env or "type" not in compute_env:
                        raise ValueError(
                            f"Framework managed 'computeEnvironment' parametrization dict must be based on https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/create_compute_environment.html"
                        )
                    if compute_env["type"] == "MANAGED" and "computeResources" not in compute_env:
                        raise ValueError(
                            f"'computeResources' must be defined for a computeEnvironment {compute_env['computeEnvironmentName']!r} when type=='MANAGED'!"
                        )
                    compute_resources = compute_env["computeResources"]
                    if "type" not in compute_resources:
                        raise ValueError(
                            f"'computeResources' must be have 'type' parameter defined for a computeEnvironment {compute_env['computeEnvironmentName']!r}!"
                        )
                    if compute_resources["type"] in ["EC2", "SPOT"] and "instanceRole" not in compute_resources:
                        module_logger.critical(
                            f"'instanceRole' for computeEnvironment {compute_env['computeEnvironmentName']!r} will be IF_EXEC_ROLE as it was left undefined."
                        )
                elif isinstance(compute_env, str):
                    # job queue mandates it to be an ARN (add this validation for better UX)
                    if not compute_env.startswith("arn:"):
                        raise ValueError(
                            f"'computeEnvironment' parameter {compute_env!r} used in job qeueu {job_queue['jobQueueName']!r} must be in ARN format!"
                        )

    def provide_output_attributes(self, inputs: List[Signal], slot: Slot, user_attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # this callback is received during node creation, even before activation
        # so doing validations here early would provide the best development experience
        self._validate_route(inputs, slot, user_attrs)

        # default to PARQUET
        data_format_value = user_attrs.get(DATASET_FORMAT_KEY, user_attrs.get(DATA_FORMAT_KEY, None))
        data_format = DatasetSignalSourceFormat.PARQUET if data_format_value is None else DatasetSignalSourceFormat(data_format_value)

        return {
            DATA_TYPE_KEY: user_attrs.get(DATA_TYPE_KEY, DataType.DATASET),
            DATASET_HEADER_KEY: user_attrs.get(DATASET_HEADER_KEY, True),
            DATASET_SCHEMA_TYPE_KEY: user_attrs.get(DATASET_SCHEMA_TYPE_KEY, DatasetSchemaType.SPARK_SCHEMA_JSON),
            DATASET_SCHEMA_FILE_KEY: user_attrs.get(DATASET_SCHEMA_FILE_KEY, None),
            DATASET_FORMAT_KEY: data_format,
        }

    def query_external_source_spec(
        self, ext_signal_source: SignalSourceAccessSpec
    ) -> Optional[Tuple[SignalSourceAccessSpec, DimensionSpec]]:
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
        # check if app-name is ok, both length and format (if used as part of internal training-job naming scheme here)
        if len(self._build_batch_job_name(self._dev_platform.context_id, str(uuid.uuid1()))) > 128:
            raise ValueError(
                f"Cannot dev_init {self.__class__.__name__} due to invalid app name {self._dev_platform.context_id} does not meet AWS Batch job name pattern"
            )

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._batch = self._session.client("batch", region_name=self._region)

    def _get_batch_client(self, slot):
        orchestrator_role = slot.extra_params.get(AWS_BATCH_ORCHESTRATOR_ROLE_ARN_PARAM, None)
        batch_client = self._batch
        if orchestrator_role:
            # assume into this role to
            batch_client = get_assumed_role_session(
                role_arn=orchestrator_role,
                duration=60 * 60,
                base_session=self._session,
                context_id=self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT],
            ).client("batch", region_name=self._region)
        return batch_client

    def compute(
        self,
        route: Route,
        materialized_inputs: List[Signal],
        slot: Slot,
        materialized_output: Signal,
        execution_ctx_id: str,
        retry_session_desc: Optional[ComputeSessionDesc] = None,
    ) -> ComputeResponse:
        api_params: Dict[str, Any] = copy.deepcopy(slot.extra_params)

        unique_compute_id: str = str(uuid.uuid1())
        job_name = self._build_batch_job_name(self.get_platform().context_id, unique_compute_id)
        job_arn = self._build_batch_job_arn(self.region, self.account_id, job_name)

        # refer
        #  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/submit_job.html

        # 1- jobName
        api_params.update({"jobName": job_name})

        # 2- process "command" with inputs', output's alias and dimensions
        # replace input / output paths
        renderer = ComputeRuntimeTemplateRenderer(materialized_inputs, materialized_output, self._params)
        # TODO might wanna do it against other parameters as well?
        containerOverrides = api_params.get("containerOverrides", None)
        if containerOverrides:
            command = containerOverrides["command"]
            rendered_command = []
            for arg in command:
                if arg.startswith(self.COMMAND_EVAL_KEYWORD):
                    try:
                        rendered_command.append(eval(renderer.render(arg[len(self.COMMAND_EVAL_KEYWORD) :])))
                    except (ValueError, SyntaxError) as err:
                        raise err
                else:
                    rendered_command.append(renderer.render(arg))

            containerOverrides.update({"command": rendered_command})

        # 3- replace dictionaries in parametrization with arns for managed job definitions and queues
        job_def = api_params["jobDefinition"]
        job_queue = api_params["jobQueue"]
        if isinstance(job_def, dict):
            # IF managed definition, set the provisioned def name
            api_params.update(
                {"jobDefinition": self._build_batch_job_definition_name(self.get_platform().context_id, job_def["jobDefinitionName"])}
            )
            # handles when batch script needs provisioned def name too
            if containerOverrides and "{jobDefinition}" in containerOverrides["command"]:
                containerOverrides["command"] = [
                    api_params["jobDefinition"] if arg == "{jobDefinition}" else arg for arg in containerOverrides["command"]
                ]

        if isinstance(job_queue, dict):
            # IF managed queue, set the provisioned queue name
            api_params.update({"jobQueue": self._build_batch_job_queue_name(self.get_platform().context_id, job_queue["jobQueueName"])})
            # handles when batch script needs provisioned queue name too
            if containerOverrides and "{jobQueue}" in containerOverrides["command"]:
                containerOverrides["command"] = [
                    api_params["jobQueue"] if arg == "{jobQueue}" else arg for arg in containerOverrides["command"]
                ]

        batch_client = self._get_batch_client(slot)
        if AWS_BATCH_ORCHESTRATOR_ROLE_ARN_PARAM in api_params:
            # remove it from params (IF specific param)
            api_params.pop(AWS_BATCH_ORCHESTRATOR_ROLE_ARN_PARAM)

        if not retry_session_desc:
            if route.output.resource_access_spec.overwrites:
                # overwrite logic:
                # now clean the output partition (delete previous batch job runs)
                # entire_domain=False just uses the material dimension values to target the current output partition only
                # IMPORTANT: do it the first time! not during retries as we'd like to keep the partial container outputs
                # Example: and array size 100 job fails due to some childs having transient issues. we want to let them
                # (batch logic) be idempotent.
                output_internal_view = self.get_platform().storage.map_materialized_signal(materialized_output)
                self.get_platform().storage.delete_internal(output_internal_view, entire_domain=False)

        # call submit_job
        job_id = None
        try:
            # we need to keep the exp-retry-count low due to being on orchestration critical-path here
            api_params.update({MAX_SLEEP_INTERVAL_PARAM: 8})
            response = submit_job(batch_client, self.CLIENT_RETRYABLE_EXCEPTION_LIST, api_params)
            job_id = response["jobId"]
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in self.CLIENT_RETRYABLE_EXCEPTION_LIST or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                failed_response_type = ComputeFailedResponseType.TRANSIENT
            elif error_code in ["ClientException"]:
                failed_response_type = ComputeFailedResponseType.BAD_SLOT
            else:
                failed_response_type = ComputeFailedResponseType.UNKNOWN

            return ComputeFailedResponse(
                failed_response_type,
                ComputeResourceDesc(job_name, job_arn, driver=self.__class__),
                error_code,
                str(error.response["Error"]),
            )

        return ComputeSuccessfulResponse(
            ComputeSuccessfulResponseType.PROCESSING,
            ComputeSessionDesc(job_id, ComputeResourceDesc(job_name, job_arn, driver=self.__class__)),
        )

    def get_session_state(
        self, session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
    ) -> ComputeSessionState:
        job_id = session_desc.session_id
        execution_details = None

        # extract AWS_BATCH_ORCHESTRATOR_ROLE_ARN_PARAM  from active_compute_record.slot.extra_params
        # and assume into the role
        batch_client = self._get_batch_client(active_compute_record.slot)
        try:
            # We are actually advised to avoid retries in critical orchestration paths which will eventually be
            # retried as long as right session state (TRANSIENT) is returned. But we still do exp retry (with a max time
            # less than usual) as an optimization based on unlikeliness of issues with this api (relative to compute).
            response = describe_jobs(batch_client, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **{"jobs": [job_id], MAX_SLEEP_INTERVAL_PARAM: 8})
            # this statement is not a must but it will provide a better explanation to the orchestrator. otherwise it
            # will cause a dict access error and obfuscate the actual problem for diagnosis. also we don't expect this
            # to happen. we don't do this in other drivers.
            if not response or not response["jobs"]:
                raise RuntimeError(f"AWS Batch Job ID {job_id} could not be found!")

            # now let's interpret the response in our Compute model
            job_details = response["jobs"][0]
            start = job_details.get("startedAt", None)
            end = job_details.get("stoppedAt", None)
            execution_details = ComputeExecutionDetails(start, end, dict(job_details))

            session_state = get_batch_job_run_state_type(job_details)
            if session_state == ComputeSessionStateType.FAILED:
                failure_type = get_batch_job_run_failure_type(job_details)
                execution_details.details.update(
                    {
                        "ErrorMessage": job_details.get(
                            "statusReason", "Unknown: FAILED 'statusReason' for batch job not provided by AWS Batch."
                        )
                    }
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

    def terminate_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        """Compute is terminated. Irrespective of compute state, this will always be called. Add completion file."""
        if active_compute_record.session_state and active_compute_record.session_state.state_type == ComputeSessionStateType.COMPLETED:
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

    def kill_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        # skip if initial response ('state') is not SUCCESS to 'compute' call
        if active_compute_record.state.response_type == ComputeResponseType.SUCCESS:
            state = cast(ComputeSuccessfulResponse, active_compute_record.state)
            # now find most recent session desc returned for the compute record
            if active_compute_record.session_state and active_compute_record.session_state.session_desc:
                most_recent_session_desc: ComputeSessionDesc = active_compute_record.session_state.session_desc
            else:
                most_recent_session_desc: ComputeSessionDesc = state.session_desc
            job_id = most_recent_session_desc.session_id
            batch_client = self._get_batch_client(active_compute_record.slot)
            exponential_retry(
                batch_client.terminate_job,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                jobId=job_id,
                # later, `reason` will show up as "statusReason" for FAILED state (in describe_job)
                # and help us to mark failure as STOPPED. unfortunately, the service does not have
                # well defined states for cancellation/termination and also for failure reasons.
                reason=INTELLIFLOW_AWS_BATCH_CANCELLATION_REASON,
            )

    def provide_runtime_trusted_entities(self) -> List[str]:
        # TODO make this contingent based on the use of IF EXEC role as instancerole in managed compute env (if any)
        #  or execution roles in managed job def (if any)
        #  - use pending routes for this
        return ["batch.amazonaws.com", "ec2.amazonaws.com", "ecs-tasks.amazonaws.com"]

    def provide_runtime_default_policies(self) -> List[str]:
        # TODO make this contingent based on the use of IF EXEC role as instancerole in managed compute env (if any)
        #  or execution roles in managed job def (if any)
        #  - use pending routes for this
        return ["service-role/AWSBatchServiceRole", "AWSBatchFullAccess", "service-role/AmazonEC2ContainerServiceforEC2Role"]

    @classmethod
    def _build_batch_job_name(cls, app_id: str, uuid: str):
        return cls.BATCH_JOB_NAME_PATTERN.format(app_id, uuid).lower()

    @classmethod
    def _build_batch_job_arn(cls, region: str, account_id: str, job_name: str):
        return f"arn:aws:batch:{region}:{account_id}:job/*"  # jobId is used so we have to set it "*"

    @classmethod
    def _build_batch_compute_env_name(cls, app_id: str, name: str):
        return cls.BATCH_COMPUTE_ENV_NAME_PATTERN.format(app_id, name).lower()

    @classmethod
    def _build_batch_compute_env_arn(cls, region: str, account_id: str, name: str):
        return f"arn:aws:batch:{region}:{account_id}:compute-environment/{name}"

    @classmethod
    def _build_batch_job_queue_name(cls, app_id: str, name: str):
        return cls.BATCH_JOB_QUEUE_NAME_PATTERN.format(app_id, name).lower()

    @classmethod
    def _build_batch_job_queue_arn(cls, region: str, account_id: str, name: str):
        return f"arn:aws:batch:{region}:{account_id}:job-queue/{name}"

    @classmethod
    def _build_batch_job_definition_name(cls, app_id: str, name: str):
        return cls.BATCH_JOB_DEFINITION_NAME_PATTERN.format(app_id, name).lower()

    @classmethod
    def _build_batch_job_definition_arn(cls, region: str, account_id: str, name: str):
        return f"arn:aws:batch:{region}:{account_id}:job-definition/{name}"

    @classmethod
    def _build_instance_profile_name(cls, exec_role_name: str):
        return f"{exec_role_name}-{cls.__name__}-Profile"

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        # If there is no <Route> detected (see hook_internal) for this driver,
        # then no need to provide extra permissions.
        # If they were added previously, they will be wiped out in the current activation.
        if not self._pending_internal_routes:
            return []

        permissions = []

        # first add AWS Batch orchestration related permissions
        resources = []
        for route in self._pending_internal_routes:
            for slot in route.slots:
                if slot.type != SlotType.ASYNC_BATCH_COMPUTE or slot.code_lang != Lang.AWS_BATCH_JOB:
                    continue
                orchestrator_role = slot.extra_params.get(AWS_BATCH_ORCHESTRATOR_ROLE_ARN_PARAM, None)
                if orchestrator_role:
                    # exec role just needs to assume unmanaged role provided by the user.
                    # permissions and trust policy on that role must be managed by the user.
                    permissions.append(ConstructPermission([orchestrator_role], ["sts:AssumeRole"]))
                else:
                    # exec role will orchestrate directly, so need the following for jobs
                    # these resources must be in the same account, so we will build the ARNs accordingly
                    job_def = slot.extra_params["jobDefinition"]
                    job_queue = slot.extra_params["jobQueue"]
                    if isinstance(job_def, str):
                        resources.append(
                            self._build_batch_job_definition_arn(self._region, self._account_id, job_def),
                        )
                    if isinstance(job_queue, str):
                        resources.append(
                            self._build_batch_job_queue_arn(self._region, self._account_id, job_queue),
                        )

        # Add IF managed resources (don't be too granular, like using def and queue names in ARNs) to save policy space.
        resources.extend(
            [
                self._build_batch_job_arn(self._region, self._account_id, self._build_batch_job_name(self._dev_platform.context_id, "*")),
                # not needed at runtime for job management
                # self._build_batch_compute_env_arn(self._region, self._account_id, self._build_batch_compute_env_name(self._dev_platform.context_id, "*")),
                self._build_batch_job_queue_arn(
                    self._region, self._account_id, self._build_batch_job_queue_name(self._dev_platform.context_id, "*")
                ),
                self._build_batch_job_definition_arn(
                    self._region, self._account_id, self._build_batch_job_queue_name(self._dev_platform.context_id, "*")
                ),
            ]
        )

        permissions.append(
            # allow our role to create/describe the jobs here
            ConstructPermission(
                resources,
                [
                    "batch:SubmitJob",
                    "batch:TerminateJob",
                    # AWS Batch Describe and List APIs don't support resource-level permissions:
                    # https://docs.aws.amazon.com/batch/latest/userguide/security_iam_service-with-iam.html
                    # https://docs.aws.amazon.com/service-authorization/latest/reference/list_awsbatch.html#awsbatch-actions-as-permissions
                    # "batch:DescribeJobs"
                ],
            ),
        )

        # AWS Batch Describe and List APIs don't support resource-level permissions:
        # https://docs.aws.amazon.com/batch/latest/userguide/security_iam_service-with-iam.html
        # https://docs.aws.amazon.com/service-authorization/latest/reference/list_awsbatch.html#awsbatch-actions-as-permissions
        permissions.append(ConstructPermission(["*"], ["batch:Describe*"]))

        # extra permissions
        for route in self._pending_internal_routes:
            for slot in route.slots:
                if slot.type.is_batch_compute() and slot.permissions:
                    for compute_perm in slot.permissions:
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
        context_id = params[ActivationParams.CONTEXT_ID]

        return [
            # dev-role should be able to orchestrate in local mode
            # local execution support (Application::execute)
            ConstructPermission(
                [
                    cls._build_batch_job_arn(region, account_id, "*"),
                    cls._build_batch_job_queue_arn(region, account_id, "*"),
                    cls._build_batch_job_definition_arn(region, account_id, "*"),
                ],
                [
                    "batch:SubmitJob",
                    "batch:TerminateJob",
                    # Describe and List APIs don't allow resource-level persmissions (see the one with "*" below)
                    #    , "batch:DescribeJobs"
                ],
            ),
            # should be able to provision resources
            ConstructPermission(
                [
                    cls._build_batch_compute_env_arn(region, account_id, cls._build_batch_compute_env_name(context_id, "*")),
                    cls._build_batch_job_queue_arn(region, account_id, cls._build_batch_job_queue_name(context_id, "*")),
                    cls._build_batch_job_definition_arn(region, account_id, cls._build_batch_job_definition_name(context_id, "*")),
                ],
                ["batch:*"],
            ),
            # WARNING why we need to use "*" for Describe* ? Because
            #  https://docs.aws.amazon.com/service-authorization/latest/reference/list_awsbatch.html#awsbatch-actions-as-permissions
            ConstructPermission(
                ["*"],
                ["batch:Describe*"],
            ),
            # create_compute_environment requires serviceRole to be passed by the dev-role
            # for user managed serviceRole provided to compute env, app level permission (DEVTIME scope) can be added.
            ConstructPermission(
                [f"arn:aws:iam::{account_id}:role/aws-service-role/batch.amazonaws.com/AWSServiceRoleForBatch"],
                ["iam:PassRole"],
            ),
            # should be able to assume orchestrator roles (if provided by the user for unmanaged definitions and queues)
            # local execution support (Application::execute)
            # TODO not happy with this, this is due to execute API using process API with the local Processor hitting
            #   `compute` and `get_session_state` impls here locally (using dev-role)
            #  SOLUTION: WE SHOULD change orchestrator core (RoutingTable) to get exec-role session and pass it around.
            #  REMOVED will be pending on SOLUTION to be implemented till the use-case arises (local execution in unmanaged mode)
            # ConstructPermission(["*"], ["sts:AssumeRole"]),
            # instance-profile for ec2 instance in cluster
            ConstructPermission(
                [
                    f"arn:aws:iam::{account_id}:instance-profile/{cls._build_instance_profile_name(IF_EXE_ROLE_NAME_FORMAT.format(context_id, region))}"
                ],
                [
                    "iam:CreateInstanceProfile",
                    "iam:AddRoleToInstanceProfile",
                    "iam:DeleteInstanceProfile",
                    "iam:RemoveRoleFromInstanceProfile",
                ],
            ),
            # FUTURE uncomment this and add more ECS / ECR permissions for provisioning of container image
            # ConstructPermission([f"arn:aws:ecr:{region}:{account_id}:repository/*"], ["ecr:Describe*"]),
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
        # create the resources driver would need to operate

        super().activate()

    def rollback(self) -> None:
        super().rollback()

    def terminate(self) -> None:
        """wipe-out resources created in "activation" (via "_process_internal" callback in this driver).
        During "terminate" sequence, Application calls _process_internal with an empty topology causing cleanup
        logic to be effective in there however we keep termination logic for the cases where:
        - driver might have a direct call:
            - when the driver is removed from the app or replaced (most likely scenario)
            - user dispatches this directly via driver instance (not recommended though)
        """
        super().terminate()
        compute_envs = dict()
        job_queues = dict()
        job_definitions = dict()
        if_exe_role_name = IF_EXE_ROLE_NAME_FORMAT.format(self.get_platform().context_id, self._params[AWSCommonParams.REGION])
        instance_profile_name = self._build_instance_profile_name(if_exe_role_name)
        instance_profile_arn = f"arn:aws:iam::{self._params[AWSCommonParams.ACCOUNT_ID]}:instance-profile/{instance_profile_name}"
        # - fill the maps
        for route in self._processed_internal_routes:
            for slot in route.slots:
                if slot.type != SlotType.ASYNC_BATCH_COMPUTE or slot.code_lang != Lang.AWS_BATCH_JOB:
                    continue
                extra_params = slot.extra_params
                job_queue = extra_params["jobQueue"]
                if isinstance(job_queue, dict):
                    job_queue_name = job_queue["jobQueueName"]
                    job_queues[job_queue_name] = job_queue

                    compute_env_order = job_queue["computeEnvironmentOrder"]
                    for comp_item in compute_env_order:
                        compute_env = comp_item["computeEnvironment"]
                        if isinstance(compute_env, dict):
                            compute_env_name = compute_env["computeEnvironmentName"]
                            compute_envs[compute_env_name] = compute_env

                job_def = extra_params["jobDefinition"]
                if isinstance(job_def, dict):
                    job_def_name = job_def["jobDefinitionName"]
                    job_definitions[job_def_name] = job_def

        compute_env_names = set(compute_envs.keys())
        job_queue_names = set(job_queues.keys())
        job_definition_names = set(job_definitions.keys())

        # 1. terminate removed job definitions
        # refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/deregister_job_definition.html
        for job_definition_name in job_definition_names:
            managed_job_definition_name = self._build_batch_job_definition_name(self.get_platform().context_id, job_definition_name)
            response = describe_job_definitions(
                self._batch, self.CLIENT_RETRYABLE_EXCEPTION_LIST, jobDefinitionName=managed_job_definition_name
            )
            if response and not response.get("jobDefinitions", []):
                exponential_retry(
                    self._batch.deregister_job_definition, self.CLIENT_RETRYABLE_EXCEPTION_LIST, jobDefinition=managed_job_definition_name
                )

        # 2. TERMINATION SEQUENCE for compute envs and job_queues
        # Refer the following link for a description of this deletion sequence:
        #  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/delete_compute_environment.html
        #
        # 2.1- terminate job queues
        # refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/delete_job_queue.html
        for job_queue_name in job_queue_names:
            managed_job_queue_name = self._build_batch_job_queue_name(self.get_platform().context_id, job_queue_name)
            response = describe_job_queues(self._batch, self.CLIENT_RETRYABLE_EXCEPTION_LIST, jobQueues=[managed_job_queue_name])
            if not response or not response.get("jobQueues", []):
                # already cleanup in _process_internal (via Application::terminate)
                continue

            job_queue = copy.deepcopy(job_queues[job_queue_name])
            # inconsistent variable naming between create and update APIs
            job_queue["jobQueue"] = managed_job_queue_name
            job_queue.pop("jobQueueName")
            job_queue["state"] = "DISABLED"
            # remove "tags" to conform to update API
            if "tags" in job_queue:
                job_queue.pop("tags")
            # then we need to conform to the APIs and replace compute env dicts with their names only
            compute_env_order = job_queue["computeEnvironmentOrder"]
            for comp_item in compute_env_order:
                compute_env = comp_item["computeEnvironment"]
                if isinstance(compute_env, dict):
                    comp_item["computeEnvironment"] = self._build_batch_compute_env_arn(
                        self._region, self._account_id, compute_env["computeEnvironmentName"]
                    )

            try:
                # disable it
                exponential_retry(self._batch.update_job_queue, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **job_queue)

                self._wait_on_job_queue(managed_job_queue_name)  # or the delete will fail due to queue "resource is being modified"

                # delete it
                exponential_retry(self._batch.delete_job_queue, self.CLIENT_RETRYABLE_EXCEPTION_LIST, jobQueue=managed_job_queue_name)
            except ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code in ["ClientException"] and ("does not exist".lower() in error.response["Error"]["Message"].lower()):
                    pass
                else:
                    raise error
        #
        # 2.2- terminate removed compute envs
        # refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/delete_compute_environment.html
        for compute_env_name in compute_env_names:
            managed_compute_env_name = self._build_batch_compute_env_name(self.get_platform().context_id, compute_env_name)
            response = describe_compute_environments(
                self._batch, self.CLIENT_RETRYABLE_EXCEPTION_LIST, computeEnvironments=[managed_compute_env_name]
            )
            if not response or not response.get("computeEnvironments", []):
                # already cleanup in _process_internal (via Application::terminate)
                continue

            compute_env = copy.deepcopy(compute_envs[compute_env_name])
            # inconsistent variable naming between update and create APIs
            compute_env["computeEnvironment"] = managed_compute_env_name
            compute_env.pop("computeEnvironmentName")
            # disable it!
            compute_env["state"] = "DISABLED"
            # conform to update API remove the parameters used only in creation
            compute_env.pop("type")
            if "tags" in compute_env:
                compute_env.pop("tags")
            if "eksConfiguration" in compute_env:
                compute_env.pop("eksConfiguration")
            if "updatePolicy" in compute_env:
                compute_env.pop("updatePolicy")
            #
            compute_resources = compute_env["computeResources"]
            if compute_resources["type"] in ["EC2", "SPOT"]:
                compute_resources["instanceRole"] = instance_profile_arn

                if "updateToLatestImageVersion" not in compute_resources:
                    compute_resources["updateToLatestImageVersion"] = True

                if "desiredvCpus" in compute_resources:
                    # will cause complication during the update as it should be equal to or greated than the current
                    compute_resources.pop("desiredvCpus")
            elif compute_resources["type"] in ["FARGATE"]:
                if "updateToLatestImageVersion" in compute_resources:
                    compute_resources.pop("updateToLatestImageVersion")
                compute_resources.pop("type")

            try:
                # disable via update
                exponential_retry(self._batch.update_compute_environment, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **compute_env)

                self._wait_on_compute_env(managed_compute_env_name)

                # now delete it!
                exponential_retry(
                    self._batch.delete_compute_environment,
                    self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                    computeEnvironment=managed_compute_env_name,
                )
            except ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code in ["ClientException"] and ("does not exist".lower() in error.response["Error"]["Message"].lower()):
                    pass
                else:
                    raise error

        # 3. lastly delete the instance role based on exec role
        iam = self._session.client("iam", region_name=self._region)
        delete_instance_profile(iam, if_exe_role_name, instance_profile_name)

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

    def _wait_on_compute_env(self, compute_env_name):
        try:
            module_logger.critical(
                f"Waiting on updated AWS Batch Compute Env {compute_env_name!r} till status becomes 'VALID' to"
                f" be able to use it in dependent job queues..."
            )
            waiter = create_compute_environment_waiter(self._batch)
            waiter.wait(computeEnvironments=[compute_env_name])
        except (WaiterError, TypeError) as e:  # TODO TEST MOTO handle "TypeError: Object of type Role is not JSON serializable"
            module_logger.warning(
                f"Waiter has exited prematurely before AWS Batch compute env {compute_env_name!r} status becomes 'VALID'. "
                f"If the dependent job queues will fail because of this, please simply re-activate the application. "
                f"Error: {e!r}"
            )

    def _wait_on_job_queue(self, job_queue_name):
        try:
            module_logger.critical(
                f"Waiting on updated AWS Batch Job Queue {job_queue_name!r} till status becomes 'VALID' to"
                f" be able to use it in dependent job queues..."
            )
            waiter = create_job_queue_waiter(self._batch)
            waiter.wait(jobQueues=[job_queue_name])
        except (WaiterError, TypeError) as e:  # TODO TEST MOTO handle "TypeError: Object of type Role is not JSON serializable"
            module_logger.warning(
                f"Waiter has exited prematurely before AWS Batch job queue {job_queue_name!r} status becomes 'VALID'. "
                f"If the following actions will fail because of this, please simply re-activate the application. "
                f"Error: {e!r}"
            )

    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        """Changeset logic for all sorts of the AWS Batch resources (compute envs, queues and definitions) that user
        wants the application to manage.

        Dictionary definitions for these resources will be replaced by their name only in `compute` operation to comply
        with the API. So we are using the originally much leaner AWS Batch Job definition in a nested, more enriched
        way to define all its dependencies on other resources from AWS Batch, IAM, containers, etc.

        TODO/FUTURE Resources for a job could have been added to the framework as <Extension>s. Each one of them could be
          implemented as independent AWS extension to the platform (e.g DynamoDBTable) but this driver impl has decided
          to keep resource management encapsulated. Extension based approach would have an advantage in separating
          concerns and also minimizing the conditional logic we need in this driver as in permissions related APIs
          (dev-time, runtime permissions depending on EXEC role use in compute envs for example) or in this particular
          callback to manage resources.
        """

        new_compute_envs = dict()
        new_job_queues = dict()
        new_job_definitions = dict()
        # - fill the maps
        # - also check whether jobDefs, jobQueues and compute envs (<str>s) provided by user do exist
        for new_route in new_routes:
            for slot in new_route.slots:
                if slot.type != SlotType.ASYNC_BATCH_COMPUTE or slot.code_lang != Lang.AWS_BATCH_JOB:
                    continue
                extra_params = slot.extra_params
                # we cannot assume that dev-role will be able to describe the entity, we need to assume into the orchesrator
                # role (if defined by the user). resource might even be in a different account.
                batch_client = self._get_batch_client(slot)
                job_queue = extra_params["jobQueue"]
                if isinstance(job_queue, dict):
                    job_queue_name = job_queue["jobQueueName"]
                    if job_queue_name in new_job_queues and new_job_queues[job_queue_name] != job_queue:
                        raise ValueError(f"Conflicting definitions for AWS Batch jobQueue dict {job_queue_name!r}!")
                    new_job_queues[job_queue_name] = job_queue

                    compute_env_order = job_queue["computeEnvironmentOrder"]
                    for comp_item in compute_env_order:
                        compute_env = comp_item["computeEnvironment"]
                        if isinstance(compute_env, dict):
                            compute_env_name = compute_env["computeEnvironmentName"]
                            if compute_env_name in new_compute_envs and new_compute_envs[compute_env_name] != compute_env:
                                raise ValueError(f"Conflicting definitions for AWS Batch computeEnvironment dict {compute_env_name!r}!")
                            new_compute_envs[compute_env_name] = compute_env
                        else:  # str
                            response = describe_compute_environments(
                                batch_client, self.CLIENT_RETRYABLE_EXCEPTION_LIST, computeEnvironments=[compute_env]
                            )
                            if not response or not response.get("computeEnvironments", []):
                                raise ValueError(f"AWS Batch computeEnvironment {job_queue} could not be found!")
                else:  # str
                    response = describe_job_queues(batch_client, self.CLIENT_RETRYABLE_EXCEPTION_LIST, jobQueues=[job_queue])
                    if not response or not response.get("jobQueues", []):
                        raise ValueError(f"AWS Batch jobQueue {job_queue} could not be found!")

                job_def = extra_params["jobDefinition"]
                if isinstance(job_def, dict):
                    job_def_name = job_def["jobDefinitionName"]
                    if job_def_name in new_job_definitions and new_job_definitions[job_def_name] != job_def:
                        raise ValueError(f"Conflicting definitions for AWS Batch jobDefinition dict {job_def_name!r}!")
                    new_job_definitions[job_def_name] = job_def
                else:  # str
                    response = describe_job_definitions(batch_client, self.CLIENT_RETRYABLE_EXCEPTION_LIST, jobDefinitionName=job_def)
                    if not response or not response.get("jobDefinitions", []):
                        raise ValueError(f"AWS Batch jobDefinition {job_def} could not be found!")

        compute_envs = dict()
        job_queues = dict()
        job_definitions = dict()
        # - fill the maps
        for route in current_routes:
            for slot in route.slots:
                if slot.type != SlotType.ASYNC_BATCH_COMPUTE or slot.code_lang != Lang.AWS_BATCH_JOB:
                    continue
                extra_params = slot.extra_params
                job_queue = extra_params["jobQueue"]
                if isinstance(job_queue, dict):
                    job_queue_name = job_queue["jobQueueName"]
                    job_queues[job_queue_name] = job_queue

                    compute_env_order = job_queue["computeEnvironmentOrder"]
                    for comp_item in compute_env_order:
                        compute_env = comp_item["computeEnvironment"]
                        if isinstance(compute_env, dict):
                            compute_env_name = compute_env["computeEnvironmentName"]
                            compute_envs[compute_env_name] = compute_env

                job_def = extra_params["jobDefinition"]
                if isinstance(job_def, dict):
                    job_def_name = job_def["jobDefinitionName"]
                    job_definitions[job_def_name] = job_def

        # 1- compute envs
        new_compute_env_names = set(new_compute_envs.keys())
        compute_env_names = set(compute_envs.keys())
        # first we will need the instance role based on exec role to be ready for use in compute envs with type EC2
        iam = self._session.client("iam", region_name=self._region)
        if_exe_role_name = IF_EXE_ROLE_NAME_FORMAT.format(self.get_platform().context_id, self._params[AWSCommonParams.REGION])
        instance_profile_name = self._build_instance_profile_name(if_exe_role_name)
        create_job_flow_instance_profile(iam, if_exe_role_name, instance_profile_name)
        instance_profile_arn = f"arn:aws:iam::{self._params[AWSCommonParams.ACCOUNT_ID]}:instance-profile/{instance_profile_name}"
        # 1.1- add brand new compute env resources
        # refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/create_compute_environment.html
        compute_envs_to_be_created = new_compute_env_names - compute_env_names
        for compute_env_name in compute_envs_to_be_created:
            compute_env = copy.deepcopy(new_compute_envs[compute_env_name])
            managed_compute_env_name = self._build_batch_compute_env_name(self.get_platform().context_id, compute_env_name)
            compute_env["computeEnvironmentName"] = managed_compute_env_name
            # conform to create API, remove the parameters used only in update
            if "updatePolicy" in compute_env:
                compute_env.pop("updatePolicy")
            compute_resources = compute_env["computeResources"]
            if compute_resources["type"] in ["EC2", "SPOT"] and "instanceRole" not in compute_resources:
                module_logger.critical(
                    f"Using EXEC role {if_exe_role_name!r} as the 'instanceRole' for AWS Batch computeEnvironment {compute_env_name!r} ({managed_compute_env_name!r})!"
                )
                compute_resources["instanceRole"] = instance_profile_arn
            # conform to create API, remove the parameters used only in update
            if "updateToLatestImageVersion" in compute_resources:
                compute_resources.pop("updateToLatestImageVersion")

            try:
                exponential_retry(self._batch.create_compute_environment, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **compute_env)
            except ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code in ["ClientException"] and ("Object already exists".lower() in error.response["Error"]["Message"].lower()):
                    # corner case: retried creation effort (e.g compute env fails and halts the activation due to IAM permission on serviceRole
                    # but the env is created.
                    pass
                else:
                    raise error

            # wait for compute env to be `status=VALID`otherwise it cannot be used in a JobQueue,
            # refer definition of "computeEnvironmentOrder" in the following job queue API to see why:
            #  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/create_job_queue.html
            self._wait_on_compute_env(managed_compute_env_name)

        # 1.2- update survivor compute envs
        # refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/update_compute_environment.html
        # The following parameters can\'t be updated for Fargate compute environment:
        #   allocationStrategy, bidPercentage, ec2Configuration, ec2KeyPair, imageId, instanceRole, instanceTypes,
        #   launchTemplate, placementGroup, tags, type, updatePolicy, updateToLatestImageVersion.
        compute_envs_to_be_updated = new_compute_env_names.intersection(compute_env_names)
        for compute_env_name in compute_envs_to_be_updated:
            compute_env = copy.deepcopy(new_compute_envs[compute_env_name])
            managed_compute_env_name = self._build_batch_compute_env_name(self.get_platform().context_id, compute_env_name)
            # inconsistent variable naming between update and create APIs
            compute_env["computeEnvironment"] = managed_compute_env_name
            compute_env.pop("computeEnvironmentName")
            # conform to update API remove the parameters used only in creation
            compute_env.pop("type")
            if "tags" in compute_env:
                compute_env.pop("tags")
            if "eksConfiguration" in compute_env:
                compute_env.pop("eksConfiguration")
            if "updatePolicy" in compute_env:
                compute_env.pop("updatePolicy")
            #
            compute_resources = compute_env["computeResources"]
            if compute_resources["type"] in ["EC2", "SPOT"]:
                if "instanceRole" not in compute_resources:
                    module_logger.critical(
                        f"Using EXEC role {if_exe_role_name!r} as the 'instanceRole' for AWS Batch computeEnvironment {compute_env_name!r} (managed name: {managed_compute_env_name!r})!"
                    )
                    compute_resources["instanceRole"] = instance_profile_arn

                if "updateToLatestImageVersion" not in compute_resources:
                    compute_resources["updateToLatestImageVersion"] = True

                if "desiredvCpus" in compute_resources:
                    # will cause complication during the update as it should be equal to or greated than the current
                    compute_resources.pop("desiredvCpus")
            elif compute_resources["type"] in ["FARGATE"]:
                if "updateToLatestImageVersion" in compute_resources:
                    compute_resources.pop("updateToLatestImageVersion")
                compute_resources.pop("type")

            exponential_retry(self._batch.update_compute_environment, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **compute_env)
            # again we need to wait to make sure that job queues won't complain
            self._wait_on_compute_env(managed_compute_env_name)

        # 2- job queues
        new_job_queue_names = set(new_job_queues.keys())
        job_queue_names = set(job_queues.keys())
        # 2.1- add new job queues
        # refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/create_job_queue.html
        job_queues_to_be_created = new_job_queue_names - job_queue_names
        for job_queue_name in job_queues_to_be_created:
            job_queue = copy.deepcopy(new_job_queues[job_queue_name])
            managed_job_queue_name = self._build_batch_job_queue_name(self.get_platform().context_id, job_queue_name)
            job_queue["jobQueueName"] = managed_job_queue_name
            # first we need to conform to the APIs and replace compute env dicts with their names only
            compute_env_order = job_queue["computeEnvironmentOrder"]
            for comp_item in compute_env_order:
                compute_env = comp_item["computeEnvironment"]
                if isinstance(compute_env, dict):
                    comp_item["computeEnvironment"] = self._build_batch_compute_env_arn(
                        self._region,
                        self._account_id,
                        self._build_batch_compute_env_name(self.get_platform().context_id, compute_env["computeEnvironmentName"]),
                    )

            try:
                exponential_retry(self._batch.create_job_queue, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **job_queue)
            except ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code in ["ClientException"] and ("Object already exists".lower() in error.response["Error"]["Message"].lower()):
                    # corner case: retried creation effort (before activation completes and gets persisted due to a previous error)
                    pass
                else:
                    raise error

        # 2.2- update survivor job queues
        # refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/update_job_queue.html
        job_queues_to_be_updated = new_job_queue_names.intersection(job_queue_names)
        for job_queue_name in job_queues_to_be_updated:
            job_queue = copy.deepcopy(new_job_queues[job_queue_name])
            managed_job_queue_name = self._build_batch_job_queue_name(self.get_platform().context_id, job_queue_name)
            # inconsistent variable naming between create and update APIs
            job_queue["jobQueue"] = managed_job_queue_name
            job_queue.pop("jobQueueName")
            # first remove "tags" to conform to update API
            if "tags" in job_queue:
                job_queue.pop("tags")
            # then we need to conform to the APIs and replace compute env dicts with their names only
            compute_env_order = job_queue["computeEnvironmentOrder"]
            for comp_item in compute_env_order:
                compute_env = comp_item["computeEnvironment"]
                if isinstance(compute_env, dict):
                    comp_item["computeEnvironment"] = self._build_batch_compute_env_arn(
                        self._region,
                        self._account_id,
                        self._build_batch_compute_env_name(self.get_platform().context_id, compute_env["computeEnvironmentName"]),
                    )

            exponential_retry(self._batch.update_job_queue, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **job_queue)

        # 3- job definitions
        new_job_definition_names = set(new_job_definitions.keys())
        job_definition_names = set(job_definitions.keys())
        # 3.1 - add or update job definitions
        # refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/register_job_definition.html
        for job_definition_name in new_job_definition_names:
            job_definition = copy.deepcopy(new_job_definitions[job_definition_name])
            managed_job_definition_name = self._build_batch_job_definition_name(self.get_platform().context_id, job_definition_name)
            job_definition["jobDefinitionName"] = managed_job_definition_name
            # update roles with EXEC role if they are not defined by the user
            container_props = job_definition.get("containerProperties", None)
            if container_props:
                if "jobRoleArn" not in container_props:
                    container_props["jobRoleArn"] = self._params[AWSCommonParams.IF_EXE_ROLE]
                if "executionRoleArn" not in container_props:
                    container_props["executionRoleArn"] = self._params[AWSCommonParams.IF_EXE_ROLE]
            node_props = job_definition.get("nodeProperties", None)
            if node_props:
                node_range_props = node_props.get("nodeRangeProperties", None)
                if node_range_props:
                    for node_range_prop in node_range_props:
                        container = node_range_prop.get("container", None)
                        if container:
                            if "jobRoleArn" not in container:
                                container["jobRoleArn"] = self._params[AWSCommonParams.IF_EXE_ROLE]
                            if "executionRoleArn" not in container:
                                container["executionRoleArn"] = self._params[AWSCommonParams.IF_EXE_ROLE]

            exponential_retry(self._batch.register_job_definition, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **job_definition)

        # 3.2- terminate removed job definitions
        # refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/deregister_job_definition.html
        job_Definitions_to_be_removed = job_definition_names - new_job_definition_names
        for job_definition_name in job_Definitions_to_be_removed:
            managed_job_definition_name = self._build_batch_job_definition_name(self.get_platform().context_id, job_definition_name)
            exponential_retry(
                self._batch.deregister_job_definition, self.CLIENT_RETRYABLE_EXCEPTION_LIST, jobDefinition=managed_job_definition_name
            )

        # 4. TERMINATION SEQUENCE for compute envs and job_queues
        # Intentionally moved down to the end because it needs a particular workflow to clean-up these resources.
        # Refer the following link for a description of why this is needed:
        #  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/delete_compute_environment.html
        #
        # 4.1- terminate removed job queues
        # refer
        #    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/update_job_queue.html
        #    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/delete_job_queue.html
        job_queues_to_be_removed = job_queue_names - new_job_queue_names
        for job_queue_name in job_queues_to_be_removed:
            job_queue = copy.deepcopy(job_queues[job_queue_name])
            managed_job_queue_name = self._build_batch_job_queue_name(self.get_platform().context_id, job_queue_name)
            # inconsistent variable naming between create and update APIs
            job_queue["jobQueue"] = managed_job_queue_name
            job_queue.pop("jobQueueName")
            job_queue["state"] = "DISABLED"
            # remove "tags" to conform to update API
            if "tags" in job_queue:
                job_queue.pop("tags")
            # then we need to conform to the APIs and replace compute env dicts with their names only
            compute_env_order = job_queue["computeEnvironmentOrder"]
            for comp_item in compute_env_order:
                compute_env = comp_item["computeEnvironment"]
                if isinstance(compute_env, dict):
                    comp_item["computeEnvironment"] = self._build_batch_compute_env_arn(
                        self._region,
                        self._account_id,
                        self._build_batch_compute_env_name(self.get_platform().context_id, compute_env["computeEnvironmentName"]),
                    )

            try:
                # disable it
                exponential_retry(self._batch.update_job_queue, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **job_queue)

                self._wait_on_job_queue(managed_job_queue_name)  # or the delete will fail due to queue "resource is being modified"

                # delete it
                exponential_retry(self._batch.delete_job_queue, self.CLIENT_RETRYABLE_EXCEPTION_LIST, jobQueue=managed_job_queue_name)
            except ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code in ["ClientException"] and ("does not exist".lower() in error.response["Error"]["Message"].lower()):
                    pass
                else:
                    raise error

        #
        # 4.2- terminate removed compute envs
        # refer https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/delete_compute_environment.html
        compute_envs_to_be_removed = compute_env_names - new_compute_env_names
        for compute_env_name in compute_envs_to_be_removed:
            compute_env = copy.deepcopy(compute_envs[compute_env_name])
            managed_compute_env_name = self._build_batch_compute_env_name(self.get_platform().context_id, compute_env_name)
            # inconsistent variable naming between update and create APIs
            compute_env["computeEnvironment"] = managed_compute_env_name
            compute_env.pop("computeEnvironmentName")
            # disable it!
            compute_env["state"] = "DISABLED"
            # conform to update API remove the parameters used only in creation
            compute_env.pop("type")
            if "tags" in compute_env:
                compute_env.pop("tags")
            if "eksConfiguration" in compute_env:
                compute_env.pop("eksConfiguration")
            if "updatePolicy" in compute_env:
                compute_env.pop("updatePolicy")
            #
            compute_resources = compute_env["computeResources"]
            if compute_resources["type"] in ["EC2", "SPOT"]:
                compute_resources["instanceRole"] = instance_profile_arn

                if "updateToLatestImageVersion" not in compute_resources:
                    compute_resources["updateToLatestImageVersion"] = True

                if "desiredvCpus" in compute_resources:
                    # will cause complication during the update as it should be equal to or greated than the current
                    compute_resources.pop("desiredvCpus")
            elif compute_resources["type"] in ["FARGATE"]:
                if "updateToLatestImageVersion" in compute_resources:
                    compute_resources.pop("updateToLatestImageVersion")
                compute_resources.pop("type")

            try:
                # disable via update
                exponential_retry(self._batch.update_compute_environment, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **compute_env)
                # we need to wait on VALID status (with state=DISABLED)
                self._wait_on_compute_env(managed_compute_env_name)

                # now delete it!
                exponential_retry(
                    self._batch.delete_compute_environment,
                    self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                    computeEnvironment=managed_compute_env_name,
                )
            except ClientError as error:
                error_code = error.response["Error"]["Code"]
                if error_code in ["ClientException"] and ("does not exist".lower() in error.response["Error"]["Message"].lower()):
                    pass
                else:
                    raise error

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
