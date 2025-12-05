# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import re
from datetime import datetime
from enum import Enum, unique
from typing import Any, ClassVar, Dict, List, Optional, Sequence, Type, Union

from intelliflow.core.entity import CoreData
from intelliflow.core.signal_processing import Signal
from intelliflow.core.signal_processing.signal import SignalType
from intelliflow.core.signal_processing.signal_source import TimerSignalSourceAccessSpec

# Constructs / Compute Defs
# ------------
# Compute here stands for both inlined, batch_compute and any sync, async variants of them.
# Definitions below should be common across those variants and be independent of construct impl.


@unique
class ComputeResponseType(str, Enum):
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@unique
class ComputeSuccessfulResponseType(str, Enum):
    QUEUED = "QUEUED"  # based on construct impl, request can just be in pending / queued state
    PROCESSING = "PROCESSING"  # underlying compute technology has accepted the request, just started working on it.
    COMPLETED = "COMPLETED"  # request got synchronuously completed (ex: Inlined Compute request)


@unique
class ComputeFailedResponseType(str, Enum):
    BAD_SLOT = "BAD_SLOT"  # permanent, don't try with the same workload
    TRANSIENT = "TRANSIENT"
    TRANSIENT_FORCE_STOPPED = "TRANSIENT_FORCE_STOPPED"
    TIMEOUT = "TIMEOUT"  # cannot decide BAD or TRANSIENT, but workload took too much time
    STOPPED = "STOPPED"  # IntelliFlow assumes that a stopped execution is a failed one
    FORCE_STOPPED = "FORCE_STOPPED"
    APP_INTERNAL = "APP_INTERNAL"  # app session/workload logic failed internally
    METADATA_ACTION = "METADATA_ACTION"  # one of metadata actions failed normally successful execution
    COMPUTE_INTERNAL = "COMPUTE_INTERNAL"  # compute impl failed internally (no comm/transient issues but smthg else)
    NOT_FOUND = "NOT_FOUND"  # orchestration and compute backend state inconsistency
    UNKNOWN = "UNKNOWN"


class ComputeResourceDesc(CoreData):
    """Ex: a glue job"""

    def __init__(
        self,
        # short form representation of an underlying resource (ex: glue job_name)
        resource_name: str,
        # a full path identifier such as arn, uri for the underlying resource
        # (this might contain the resource name as well)
        resource_path: str,
        # type info about the driver initiated this compute
        driver: Type = None,
    ) -> None:
        self.resource_name = resource_name
        self.resource_path = resource_path
        self.driver = driver

    @property
    def driver_type(self) -> Optional[Type]:
        # Backwards compatibility (for previously saved inactive records or concurrent executions running while
        #  batch_compute_overhaul is being applied the very first time.
        # TODO remove when batch_compute_overhaul is applied to all of the active IF apps.
        return getattr(self, "driver", None)


class ComputeResponse:
    def __init__(self, response_type: ComputeResponseType, resource_desc: ComputeResourceDesc) -> None:
        self._response_type = response_type
        self._resource_desc = resource_desc

    @property
    def response_type(self) -> ComputeResponseType:
        return self._response_type

    @response_type.setter
    def response_type(self, val: ComputeResponseType) -> None:
        self._response_type = val

    @property
    def resource_desc(self) -> ComputeResourceDesc:
        return self._resource_desc

    @resource_desc.setter
    def resource_desc(self, val: ComputeResourceDesc) -> None:
        self._resource_desc = val

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(response_type={self._response_type}, resource_desc={self._resource_desc})"


class ComputeSessionDesc(CoreData):
    def __init__(self, session_id: str, resource_desc: ComputeResourceDesc) -> None:
        self.session_id = session_id
        self.resource_desc = resource_desc


class ComputeSuccessfulResponse(ComputeResponse):
    def __init__(self, successful_response_type: ComputeSuccessfulResponseType, session_desc: ComputeSessionDesc) -> None:
        super().__init__(ComputeResponseType.SUCCESS, session_desc.resource_desc)
        self._successful_response_type = successful_response_type
        self._session_desc = session_desc

    @property
    def successful_response_type(self) -> ComputeSuccessfulResponseType:
        return self._successful_response_type

    @successful_response_type.setter
    def successful_response_type(self, val: ComputeSuccessfulResponseType) -> None:
        self._successful_response_type = val

    @property
    def session_desc(self) -> ComputeSessionDesc:
        return self._session_desc

    @session_desc.setter
    def session_desc(self, val: ComputeSessionDesc) -> None:
        self._session_desc = val


class ComputeFailedResponse(ComputeResponse):
    def __init__(
        self, failed_response_type: ComputeFailedResponseType, resource_desc: ComputeResourceDesc, error_code: str, error_msg: str
    ) -> None:
        super().__init__(ComputeResponseType.FAILED, resource_desc)
        self._failed_response_type = failed_response_type
        self._error_code = error_code
        self._error_msg = error_msg

    @property
    def failed_response_type(self) -> ComputeFailedResponseType:
        return self._failed_response_type

    @failed_response_type.setter
    def failed_response_type(self, val: ComputeFailedResponseType) -> None:
        self._failed_response_type = val

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(failed_response_type={self.failed_response_type}, resource_desc={self._resource_desc}, "
            f"error_code={self._error_code}, error_msg={self._error_msg})"
        )


# generic compute session/job status types
@unique
class ComputeSessionStateType(str, Enum):
    QUEUED = "QUEUED"  # based on construct impl, session has not even been activated on the target resource / compute
    PROCESSING = "PROCESSING"  # underlying compute technology is working on it
    TRANSIENT_UNKNOWN = "TRANSIENT_UNKNOWN"  # session state could not be retrieved due to transient issues
    COMPLETED = "COMPLETED"  # ultimate desired state, everything
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"  # for future incompatibility detections

    def is_active(self) -> bool:
        return self in [ComputeSessionStateType.QUEUED, ComputeSessionStateType.PROCESSING, ComputeSessionStateType.TRANSIENT_UNKNOWN]


ComputeFailedSessionStateType = ComputeFailedResponseType


class ComputeExecutionDetails(CoreData):
    """Represents each execution/job-run/attempt within the underlying technology that a compute session uses"""

    # these time fields are expected to be contained by `details` dictionary as well.
    # similar attributes out of the details should be brought up to this level to abstract the clients
    # from the underlying construct impl (dict structure [see response for Glue::get_job_run]).
    def __init__(self, start_time: Union[datetime, str, int], end_time: Union[datetime, str, int], details: Dict[str, Any]) -> None:
        self.start_time = start_time
        self.end_time = end_time
        self.details = details

    def total_time_elapsed_in_milliseconds(self) -> int:
        # TODO
        pass


class ComputeSessionState:
    EXECUTION_DETAILS_DEPTH: ClassVar[int] = 5

    def __init__(
        self,
        session_desc: ComputeSessionDesc,
        state_type: ComputeSessionStateType,
        # detailed / impl specific dump of execution/runs [retries, etc] happened within this session
        executions: List[ComputeExecutionDetails],
    ) -> None:
        self._session_desc = session_desc
        self._state_type = state_type
        # detailed / impl specific dump of execution/runs [retries, etc] happened within this session
        self._executions = executions

    @property
    def session_desc(self) -> ComputeSessionDesc:
        return self._session_desc

    @property
    def state_type(self) -> ComputeSessionStateType:
        return self._state_type

    @property
    def executions(self) -> List[ComputeExecutionDetails]:
        return self._executions

    @executions.setter
    def executions(self, value: List[ComputeExecutionDetails]) -> None:
        self._executions = value

    def total_time_elapsed_in_milliseconds(self) -> int:
        # TODO
        pass

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(session_desc={self.session_desc!r},"
            f"state_type={self._state_type},"
            f"executions={self._executions})"
        )


class ComputeFailedSessionState(ComputeSessionState):
    def __init__(
        self, failed_type: ComputeFailedSessionStateType, session_desc: ComputeSessionDesc, executions: Sequence[ComputeExecutionDetails]
    ) -> ComputeSessionState:
        super().__init__(session_desc, ComputeSessionStateType.FAILED, executions)
        self._failed_type = failed_type

    @property
    def failed_type(self) -> ComputeFailedSessionStateType:
        return self._failed_type

    @failed_type.setter
    def failed_type(self, failed_type: ComputeFailedSessionStateType) -> None:
        self._failed_type = failed_type

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(session_desc={self.session_desc!r},"
            f"state_type={self._state_type},"
            f"executions={self._executions},"
            f"failed_type={self._failed_type})"
        )


class ComputeInternalError(Exception):
    pass


class ComputeRetryableInternalError(ComputeInternalError):
    pass


# Compute generic IO Defs
# -----------------------
class BasicBatchDataInputMap(CoreData):
    """Abstract clients from the internal details of common input_map map serialization (as an arg).

    This is helpful in platform construct impls that cannot bundle RheocerOS dependencies,
    and process over basic / primitive representations of inputs and outputs which are of
    type Signal within the framework.
    """

    def __init__(self, input_signals: Sequence[Signal]) -> None:
        self.input_signals = input_signals

    def create(self) -> Dict[str, Any]:
        return {
            "data_inputs": [
                {
                    "alias": signal.alias,
                    "resource_materialized_paths": [path for path in signal.get_materialized_resource_paths()],
                    "resource_type": signal.resource_access_spec.source.value,
                    "path_format": signal.resource_access_spec.path_format,
                    "data_format": signal.resource_access_spec.data_format.value,
                    "data_type": (signal.resource_access_spec.data_type.value if signal.resource_access_spec.data_type else None),
                    "data_persistence": (
                        signal.resource_access_spec.data_persistence.value if signal.resource_access_spec.data_persistence else None
                    ),
                    "dataset_type": (signal.resource_access_spec.dataset_type.value if signal.resource_access_spec.dataset_type else None),
                    "data_compression": signal.resource_access_spec.data_compression,
                    "data_folder": signal.resource_access_spec.data_folder,
                    "encryption_key": signal.resource_access_spec.encryption_key,
                    "data_header_exists": signal.resource_access_spec.data_header_exists,
                    "data_schema_file": signal.resource_access_spec.data_schema_file,
                    "data_schema_type": (
                        signal.resource_access_spec.data_schema_type.value if signal.resource_access_spec.data_schema_type else None
                    ),
                    "data_schema_def": signal.resource_access_spec.data_schema_definition,
                    "partition_keys": signal.resource_access_spec.partition_keys,
                    "partitions": signal.resource_access_spec.get_partitions(signal.domain_spec.dimension_filter_spec),
                    "primary_keys": signal.resource_access_spec.primary_keys,
                    "delimiter": signal.resource_access_spec.data_delimiter,
                    "attrs": signal.resource_access_spec.__dict__,
                    "proxy": (
                        {
                            "resource_type": signal.resource_access_spec.proxy.source.value,
                            "attrs": signal.resource_access_spec.proxy.__dict__,
                        }
                        if signal.resource_access_spec.proxy
                        else None
                    ),
                    "if_signal_type": signal.type.value,
                    "serialized_signal": signal.serialize(),
                    "range_check_required": signal.range_check_required,
                    "nearest_the_tip_in_range": signal.nearest_the_tip_in_range,
                    "is_reference": signal.is_reference,
                }
                for signal in self.input_signals
                if signal.type.is_data()
            ],
            "timers": [
                {
                    "alias": signal.alias,
                    #'formatted_time': next(iter(signal.domain_spec.dimension_filter_spec.get_root_dimensions())).value,
                    #'raw_time': next(iter(signal.domain_spec.dimension_filter_spec.get_root_dimensions())).date,
                    # note: formatted_time and time should be identical
                    "time": TimerSignalSourceAccessSpec.extract_time(signal.get_materialized_resource_paths()[0]),
                    "resource_type": signal.resource_access_spec.source.value,
                    "serialized_signal": signal.serialize(),
                }
                for signal in self.input_signals
                if signal.type == SignalType.TIMER_EVENT
            ],
            "other_signals": [
                {
                    "alias": signal.alias,
                    "resource_type": signal.resource_access_spec.source.value,
                    "serialized_signal": signal.serialize(),
                }
                for signal in self.input_signals
                if not signal.type.is_data() and signal.type != SignalType.TIMER_EVENT
            ],
        }

    def dumps(self) -> str:
        return json.dumps(self.create(), default=repr)


def create_output_dimension_map(materialized_output: Signal, raw_value: bool = False) -> Dict[str, Any]:
    return {
        dimension.name: (dimension.transform().value if not raw_value else dimension.transform().raw_value)
        for dimension in materialized_output.domain_spec.dimension_filter_spec.get_flattened_dimension_map().values()
    }


def create_pending_output_dimension_map(
    pending_node: "RuntimeLinkNode", output: "Signal", output_dim_matrix: "DimensionLinkMatrix"
) -> Dict[str, Any]:
    materialized_output: Signal = None
    try:
        # unmaterialized dimensions will like '*' (for Any) and/or ':+/-range' (for Relative) thanks to 'force'
        materialized_output = pending_node.materialize_output(output, output_dim_matrix, force=True)
    except:
        pass
    return create_output_dimension_map(materialized_output) if materialized_output else {}


class BasicBatchDataOutput(CoreData):
    def __init__(self, output: Signal, route: Optional["Route"] = None) -> None:
        self.output = output
        self.route = route

    def create(self) -> Dict[str, Any]:
        completion_indicator_paths = self.output.get_materialized_resources()
        return {
            "alias": self.output.alias,
            "resource_materialized_path": self.output.get_materialized_resource_paths()[0],
            "resource_type": self.output.resource_access_spec.source.value,
            "completion_indicator_materialized_path": completion_indicator_paths[0] if completion_indicator_paths else None,
            "data_format": self.output.resource_access_spec.data_format.value,
            "data_type": (self.output.resource_access_spec.data_type.value if self.output.resource_access_spec.data_type else None),
            "data_persistence": (
                self.output.resource_access_spec.data_persistence.value if self.output.resource_access_spec.data_persistence else None
            ),
            "data_header_exists": self.output.resource_access_spec.data_header_exists,
            "data_schema_file": self.output.resource_access_spec.data_schema_file,
            "data_schema_type": (
                self.output.resource_access_spec.data_schema_type.value if self.output.resource_access_spec.data_schema_type else None
            ),
            "data_schema_def": self.output.resource_access_spec.data_schema_definition,
            "delimiter": self.output.resource_access_spec.data_delimiter,
            "if_signal_type": self.output.type.value,
            "serialized_signal": self.output.serialize(),
            "dimension_map": create_output_dimension_map(self.output),
            "has_metadata_actions": (self.route.has_execution_metadata_actions() if self.route else False),
        }

    def dumps(self) -> str:
        return json.dumps(self.create())


ComputeLogRow = Dict[str, Any]


class ComputeLogQuery(CoreData):
    def __init__(self, records: List[ComputeLogRow], resource_urls: List[str], next_token: Optional[str] = None) -> None:
        self.records = records
        self.resource_urls = resource_urls
        self.next_token = next_token


class ComputeRuntimeTemplateRenderer:
    PARAMETRIZATION_PATTERN = re.compile(r"\$\{(.+?)\}")

    def __init__(
        self,
        materialized_inputs: List[Signal],
        materialized_output: Signal,
        scope: Optional[Dict[str, str]] = None,
    ):
        self._scope = dict(scope) if scope is not None else dict()
        # input alias' mapped to input paths
        self._scope.update({input.alias: input.get_materialized_resource_paths()[0] for input in materialized_inputs})
        # inputs as indexes mapped to input paths
        self._scope.update({f"input{i}": input.get_materialized_resource_paths()[0] for i, input in enumerate(materialized_inputs)})
        # output alias mapped to the output path
        self._scope.update({"output": materialized_output.get_materialized_resource_paths()[0]})
        # output dimension names mapped to runtime values (e.g {"region": "NA", "date": "2024-03-28"})
        dimensions_map = create_output_dimension_map(materialized_output)
        self._scope.update(dimensions_map)

    # logic adapted from "new_app_template.template_engine" module
    def _replacement(self, match: re.Match) -> str:
        code = match.group(1)
        try:
            return str(eval(code, self._scope))
        except SyntaxError:
            # Means code is assignment statement, just execute it and return empty string
            return ""
        except NameError as err:
            raise NameError(
                f"Error in {self.__class__.__name__} while rendering template param {code!r}! It does not exist in runtime scope."
            ) from err

    def render(self, template: str) -> str:
        return self.PARAMETRIZATION_PATTERN.sub(self._replacement, template)


def validate_compute_runtime_identifiers(
    inputs: List[Signal],
    output: Optional[Signal] = None,
    extra_reserved_keywords: Optional[List[str]] = None,
) -> None:
    from keyword import iskeyword

    for input in inputs:
        if input.alias:
            if not input.alias.isidentifier():
                raise ValueError(f"Input alias {input.alias!r} is not a valid Python identifier!")

            if iskeyword(input.alias):
                raise ValueError(f"Input alias {input.alias!r} is a reserved Python identifier/keyword!")

            if extra_reserved_keywords and (input.alias in extra_reserved_keywords):
                raise ValueError(f"Input alias {input.alias!r} cannot be any of the reserved keywords: {extra_reserved_keywords}!")

    if output and output.alias:
        if not output.alias.isidentifier():
            raise ValueError(f"Node ID {output.alias!r} is not a valid Python identifier!")

        if iskeyword(output.alias):
            raise ValueError(f"Node ID {output.alias!r} is a reserved Python identifier/keyword!")

        if extra_reserved_keywords and (output.alias in extra_reserved_keywords):
            raise ValueError(f"Node ID {output.alias!r} cannot be any of the reserved keywords: {extra_reserved_keywords}!")
