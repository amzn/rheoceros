# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
import json
import logging
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
    DatasetSchemaType,
    DataType,
    SignalSourceAccessSpec,
    SignalSourceType,
)
from intelliflow.core.signal_processing.slot import SlotType

from ...constructs import BatchCompute, ConstructInternalMetricDesc, ConstructParamsDict, ConstructPermission, ConstructSecurityConf
from ...definitions.aws.athena.execution.common import BatchInputMap, BatchOutput
from ...definitions.aws.common import AWS_COMMON_RETRYABLE_ERRORS, MAX_SLEEP_INTERVAL_PARAM
from ...definitions.aws.common import CommonParams as AWSCommonParams
from ...definitions.aws.common import exponential_retry
from ...definitions.aws.sfn.client_wrapper import (
    INTELLIFLOW_AWS_SFN_CANCELLATION_REASON,
    get_sfn_state_machine_failure_type,
    get_sfn_state_machine_state_type,
)
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


class AWSSFNCompute(AWSConstructMixin, BatchCompute):
    """AWS SFN BatchCompute impl"""

    CLIENT_RETRYABLE_EXCEPTION_LIST: Set[str] = {"StateMachineDeleting", "ServiceQuotaExceededException"}

    # IntelliFlow-SFN-{APP ID}-{NODE ID}
    SFN_STATE_MACHINE_NAME_PATTERN = "IntelliFlow-{}-{}-{}"

    @classmethod
    def driver_spec(cls) -> DimensionFilter:
        return DimensionFilter.load_raw(
            {
                Lang.AWS_SFN_STATE_MACHINE: {ABI.NONE: {"*": {"*": {}}}},
            }
        )

    def __init__(self, params: ConstructParamsDict) -> None:
        super().__init__(params)
        self._sfn = self._session.client("stepfunctions", region_name=self._region)

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._sfn = self._session.client("stepfunctions", region_name=self._region)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._sfn = None

    def _validate_route(self, inputs: List[Signal], slot: Slot, user_attrs: Dict[str, Any]) -> None:
        # 1- we let no-input runs, so no need to check input count

        # 2- check job parameters
        # now check Slot parameters
        extra_params: Dict[str, Any] = dict(slot.extra_params)

        sm_def = extra_params.get("definition", None)
        if not sm_def or not (isinstance(sm_def, str) or isinstance(sm_def, dict)):
            raise ValueError(
                f"'definition' parameter of type <str> (jsonified string) or type <dict> (to be jsonified by the framework) is mandated by driver {self.__class__.__name__!r}!"
            )

        if "name" in extra_params:
            raise ValueError(f"'name' parameter cannot be defined for the state-machine!")

        if "publish" in extra_params:
            raise ValueError(f"Use of `publish` parameter for the state-machine is not allowed!")

    def provide_output_attributes(self, inputs: List[Signal], slot: Slot, user_attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # this callback is received during node creation, even before activation
        # so doing validations here early would provide the best development experience
        self._validate_route(inputs, slot, user_attrs)

        return {
            DATA_TYPE_KEY: user_attrs.get(DATA_TYPE_KEY, DataType.DATASET),
            DATASET_HEADER_KEY: user_attrs.get(DATASET_HEADER_KEY, True),
            DATASET_SCHEMA_TYPE_KEY: user_attrs.get(DATASET_SCHEMA_TYPE_KEY, DatasetSchemaType.SPARK_SCHEMA_JSON),
            DATASET_SCHEMA_FILE_KEY: user_attrs.get(DATASET_SCHEMA_FILE_KEY, None),
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

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._sfn = self._session.client("stepfunctions", region_name=self._region)

    def compute(
        self,
        route: Route,
        materialized_inputs: List[Signal],
        slot: Slot,
        materialized_output: Signal,
        execution_ctx_id: str,
        retry_session_desc: Optional[ComputeSessionDesc] = None,
    ) -> ComputeResponse:
        extra_params: Dict[str, Any] = copy.deepcopy(slot.extra_params)
        slot_index: str = str([i for i, s in enumerate(route.slots) if s == slot][0])

        sm_name = self._build_sm_name(self.get_platform().context_id, route.route_id, slot_index)
        sm_arn = self._build_sm_arn(self.region, self.account_id, self.get_platform().context_id, route.route_id, slot_index)

        # refer
        #  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions/client/start_execution.html

        # 1- stateMachineArn
        api_params = {}
        api_params.update({"stateMachineArn": sm_arn})

        # 2- process "input" with inputs', output's alias and dimensions
        inputs: Dict[str, Any] = BatchInputMap(materialized_inputs).create()
        output: Dict[str, Any] = BatchOutput(materialized_output).create()

        input_json = json.dumps({"inputs": inputs, "output": output}, default=repr)
        api_params.update({"input": input_json})

        # overwrite logic:
        # now clean the output partition (delete previous state-machine execution outputs on the same dimensions)
        # entire_domain=False just uses the material dimension values to target the current output partition only
        output_internal_view = self.get_platform().storage.map_materialized_signal(materialized_output)
        self.get_platform().storage.delete_internal(output_internal_view, entire_domain=False)

        job_id = None
        retryables = self.CLIENT_RETRYABLE_EXCEPTION_LIST.union({"ExecutionLimitExceeded"})
        try:
            # we need to keep the exp-retry-count low due to being on orchestration critical-path here
            api_params.update({MAX_SLEEP_INTERVAL_PARAM: 8})
            response = exponential_retry(self._sfn.start_execution, retryables, **api_params)
            job_id = response["executionArn"]
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in retryables or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                failed_response_type = ComputeFailedResponseType.TRANSIENT
            elif error_code in ["ClientException"]:
                failed_response_type = ComputeFailedResponseType.BAD_SLOT
            else:
                failed_response_type = ComputeFailedResponseType.UNKNOWN

            return ComputeFailedResponse(
                failed_response_type,
                ComputeResourceDesc(sm_name, sm_arn, driver=self.__class__),
                error_code,
                str(error.response["Error"]),
            )

        return ComputeSuccessfulResponse(
            ComputeSuccessfulResponseType.PROCESSING,
            ComputeSessionDesc(job_id, ComputeResourceDesc(sm_name, sm_arn, driver=self.__class__)),
        )

    def get_session_state(
        self, session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
    ) -> ComputeSessionState:
        job_id = session_desc.session_id
        execution_details = None

        try:
            # We are actually advised to avoid retries in critical orchestration paths which will eventually be
            # retried as long as right session state (TRANSIENT) is returned. But we still do exp retry (with a max time
            # less than usual) as an optimization based on unlikeliness of issues with this api (relative to compute).
            response = exponential_retry(
                self._sfn.describe_execution, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **{"executionArn": job_id, MAX_SLEEP_INTERVAL_PARAM: 8}
            )
            # this statement is not a must but it will provide a better explanation to the orchestrator. otherwise it
            # will cause a dict access error and obfuscate the actual problem for diagnosis. also we don't expect this
            # to happen. we don't do this in other drivers.
            if not response or not response.get("executionArn", None):
                raise RuntimeError(f"AWS SFN state-machine {job_id!r} could not be found!")

            # now let's interpret the response in our Compute model
            job_details = response
            start = job_details.get("startDate", None)
            end = job_details.get("stopDate", None)
            execution_details = ComputeExecutionDetails(start, end, dict(job_details))

            session_state = get_sfn_state_machine_state_type(job_details)
            if session_state == ComputeSessionStateType.FAILED:
                failure_type = get_sfn_state_machine_failure_type(job_details)
                execution_details.details.update(
                    {
                        "ErrorMessage": job_details.get(
                            "error", "Unknown: FAILED 'error' for state-machine execution not provided by AWS SFN."
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
            exponential_retry(
                self._sfn.stop_execution,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                executionArn=job_id,
                # later, `reason` will show up as "statusReason" for FAILED state (in describe_job)
                # and help us to mark failure as STOPPED. unfortunately, the service does not have
                # well defined states for cancellation/termination and also for failure reasons.
                error=INTELLIFLOW_AWS_SFN_CANCELLATION_REASON,
                cause=f"{self.__class__.__name__}::kill_session",
            )

    def provide_runtime_trusted_entities(self) -> List[str]:
        # TODO
        #  - use pending routes for making this conditional
        return [f"states.{self.region}.amazonaws.com"]

    def provide_runtime_default_policies(self) -> List[str]:
        return []

    @classmethod
    def _build_sm_name(cls, app_id: str, route_id: str, slot_index: str):
        return cls.SFN_STATE_MACHINE_NAME_PATTERN.format(app_id, route_id, slot_index).lower()

    @classmethod
    def _build_sm_arn(cls, region: str, account_id: str, app_id: str, route_id: str, slot_index: str):
        return f"arn:aws:states:{region}:{account_id}:stateMachine:{cls._build_sm_name(app_id, route_id, slot_index)}"

    @classmethod
    def _build_sm_execution_arn(cls, region: str, account_id: str, app_id: str, route_id: str, slot_index: str, execution_id: str):
        return f"arn:aws:states:{region}:{account_id}:execution:{cls._build_sm_name(app_id, route_id, slot_index)}:{execution_id}"

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        # If there is no <Route> detected (see hook_internal) for this driver,
        # then no need to provide extra permissions.
        # If they were added previously, they will be wiped out in the current activation.
        if not self._pending_internal_routes:
            return []

        # Add IF managed resources (don't be too granular, like using def and queue names in ARNs) to save policy space.
        permissions = [
            # allow our role to create the executions here (please note that the resource is the SM)
            ConstructPermission(
                [
                    self._build_sm_arn(self._region, self._account_id, self._dev_platform.context_id, "*", "*"),
                ],
                [
                    "states:StartExecution",
                ],
            ),
            # executions (please note that the resource is the execution itself)
            ConstructPermission(
                [
                    self._build_sm_execution_arn(self._region, self._account_id, self._dev_platform.context_id, "*", "*", "*"),
                ],
                [
                    "states:DescribeExecution",
                    "states:StopExecution",
                ],
            ),
        ]

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
            #  - allow our role to create the executions here (please note that the resource is the SM)
            ConstructPermission(
                [
                    cls._build_sm_arn(region, account_id, context_id, "*", "*"),
                ],
                [
                    "states:*",
                ],
            ),
            # - executions (please note that the resource is the execution itself)
            ConstructPermission(
                [
                    cls._build_sm_execution_arn(region, account_id, context_id, "*", "*", "*"),
                ],
                [
                    "states:DescribeExecution",
                    "states:StopExecution",
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
        for route in self._processed_internal_routes:
            for i, slot in enumerate(route.slots):
                if slot.type != SlotType.ASYNC_BATCH_COMPUTE or slot.code_lang != Lang.AWS_SFN_STATE_MACHINE:
                    continue

                sm_arn = self._build_sm_arn(self.region, self.account_id, self.get_platform().context_id, route.route_id, str(i))
                exponential_retry(self._sfn.delete_state_machine, self.CLIENT_RETRYABLE_EXCEPTION_LIST, stateMachineArn=sm_arn)

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
        """Changeset logic for all of the state-machines that user wants the application to manage.

        Each node containing a state-machine compute will have its own SFN state-machine created or updated here.
        """
        deleted_routes = current_routes - new_routes
        for route in deleted_routes:
            for i, slot in enumerate(route.slots):
                if slot.type != SlotType.ASYNC_BATCH_COMPUTE or slot.code_lang != Lang.AWS_SFN_STATE_MACHINE:
                    continue
                sm_arn = self._build_sm_arn(self.region, self.account_id, self.get_platform().context_id, route.route_id, str(i))
                exponential_retry(self._sfn.delete_state_machine, self.CLIENT_RETRYABLE_EXCEPTION_LIST, stateMachineArn=sm_arn)

        for route in new_routes:
            for i, slot in enumerate(route.slots):
                if slot.type != SlotType.ASYNC_BATCH_COMPUTE or slot.code_lang != Lang.AWS_SFN_STATE_MACHINE:
                    continue
                sm_arn = self._build_sm_arn(self.region, self.account_id, self.get_platform().context_id, route.route_id, str(i))
                update: bool = False
                try:
                    response = exponential_retry(
                        self._sfn.describe_state_machine, self.CLIENT_RETRYABLE_EXCEPTION_LIST, stateMachineArn=sm_arn
                    )
                    if response:
                        if response["status"] == "ACTIVE":
                            update = True
                        else:  # DELETING
                            # wait for the deletion to finish and then create, we will handle this by retrying on
                            # SM deleting exception in the next step (creation)
                            update = False
                except ClientError as error:
                    error_code = error.response["Error"]["Code"]
                    if error_code not in ["StateMachineDoesNotExist"]:
                        raise error

                # common params for both update/create cases
                extra_params: Dict[str, Any] = dict(slot.extra_params)
                sm_def = extra_params["definition"]
                if isinstance(sm_def, dict):
                    extra_params.update({"definition": json.dumps(sm_def)})
                extra_params.update({"publish": True})
                if "roleArn" not in extra_params:
                    extra_params.update({"roleArn": self._params[AWSCommonParams.IF_EXE_ROLE]})
                if update:
                    extra_params.update({"stateMachineArn": sm_arn})
                    if "type" in extra_params:
                        del extra_params["type"]
                    if "tags" in extra_params:
                        del extra_params["tags"]
                    exponential_retry(self._sfn.update_state_machine, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **extra_params)
                else:  # create
                    sm_name = self._build_sm_name(self.get_platform().context_id, route.route_id, str(i))
                    # more user friendly error, rather than leaving it to SFN
                    name_diff = len(sm_name) - 80
                    if name_diff > 0:
                        raise ValueError(
                            f"Shorten the node id {route.output.alias!r} by {name_diff!r} characters to be able to create AWS SFN state-machine compute!"
                        )
                    extra_params.update({"name": sm_name})

                    if "type" not in extra_params:
                        extra_params.update({"type": "STANDARD"})

                    # we will retry here if SFN raises StateMachineDeleting
                    # TODO create a boto waiter (see aws batch custom waiters)
                    exponential_retry(self._sfn.create_state_machine, self.CLIENT_RETRYABLE_EXCEPTION_LIST, **extra_params)

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
