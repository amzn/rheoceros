from typing import overload, List, Union, Any

from intelliflow.core.signal_processing.analysis import INTEGRITY_CHECKER_MAP
from intelliflow.core.signal_processing.signal import Signal


@overload
def create_incoming_signal(from_signal: Signal, dimension_values: List[Any]) -> Signal:
    ...


def create_incoming_signal(from_signal: Signal, materialized_path_or_dimension_values: Union[str, List[Any]]) -> Signal:
    resource_path = None
    if isinstance(materialized_path_or_dimension_values, list):
        dimension_values = materialized_path_or_dimension_values
        resource_path = from_signal.resource_access_spec.path_format.format(*dimension_values)
        # feed-back signals should be mapped to real resources for routing to work.
        # we get help from the integrity_check_protocol attached to the signal.
        # we mimic real event notification.
        required_resource_name = "_resource"
        if from_signal.domain_spec.integrity_check_protocol:
            integrity_checker = INTEGRITY_CHECKER_MAP[from_signal.domain_spec.integrity_check_protocol.type]
            required_resource_name = integrity_checker.get_required_resource_name(
                from_signal.resource_access_spec, from_signal.domain_spec.integrity_check_protocol
            )

        resource_path = resource_path + from_signal.resource_access_spec.path_delimiter() + required_resource_name
    else:
        resource_path = materialized_path_or_dimension_values

    return from_signal.create(from_signal.type, from_signal.resource_access_spec.source, resource_path)
