# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
import logging
from abc import abstractmethod
from enum import Enum, unique
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple, Union, cast, overload

from intelliflow.core.serialization import Serializable, dumps, loads
from intelliflow.core.signal_processing.analysis import INTEGRITY_CHECKER_MAP, SignalIntegrityChecker
from intelliflow.core.signal_processing.definitions.dimension_defs import NameType as OutputDimensionNameType
from intelliflow.core.signal_processing.dimension_constructs import (
    DIMENSION_VARIANT_IDENTICAL_MAP_FUNC,
    AnyVariant,
    Dimension,
    DimensionAsKey,
    DimensionFilter,
    DimensionSpec,
    DimensionVariant,
    DimensionVariantMapFunc,
    DimensionVariantMapper,
    DimensionVariantReader,
    RawDimensionFilterInput,
)
from intelliflow.core.signal_processing.signal_source import (
    SignalSource,
    SignalSourceAccessSpec,
    SignalSourceType,
    SystemSignalSourceAccessSpec,
)

logger = logging.getLogger(__name__)


@unique
class SignalType(Enum):
    """Enum for all of the Signal Types that RheocerOS supports.

    This establishes the common language across different modules of RheocerOS.
    Allows Signal producer/owner entities to be more specific on what type of
    events will map to their Signal domain, along with other fields such as resource
    format and dimension descriptions (which would not be enough to remove ambiguity
    in some cases.
    """

    # As in "Signal Ground", it represents the overall system from within an internal entity with no input signals.
    # This signal "tethers" entities with no inputs to the system and allows their control/trigger/executions.
    INTERNAL_SYSTEM_GROUND_TETHER = 0

    # Trivial/Two state Signals that can be represented by CREATION/EXISTS wave.
    # Related resources do exist or not from IF's perspective and for routing.
    # Their signals (of interest for RheocerOS) are simply represented with
    # these types and when they are terminated we understand that creation action should be reverted (it was undone).
    # Most of the data (datasets, etc) fall into these category.
    EXTERNAL_S3_OBJECT_CREATION = 1
    _AMZN_RESERVED_1 = 2
    _AMZN_RESERVED_2 = 6
    EXTERNAL_GLUE_TABLE_PARTITION_UPDATE = 7
    EXTERNAL_SNS_NOTIFICATION = 8
    INTERNAL_PARTITION_CREATION = 3
    LOCAL_FOLDER_CREATION = 4

    TIMER_EVENT = 5

    # BACKFILLING SUPPORT for internal marshaled data coming from the same application
    # requesting trigger on upstream routes/records.
    INTERNAL_PARTITION_CREATION_REQUEST = 10
    INTERNAL_PSEUDO = 15

    # BACKFILLING SUPPORT for External Marshaled Data (from other RheocerOS apps)
    # Cross Application Types
    #
    # RheocerOS uses the following types to enable an Application to
    # request action on a resource owned by another Application.
    # Currently, we a legitimate scenario of supporting auto-backfilling.
    #   When an "external data" needs to be
    #   backfilled RheocerOS cannot resolve upstream dependencies of
    #   that particular source and trigger related Route(s) by auto-emitting
    #   "Inner Application Types" on the upstream dependencies of that source.
    #
    #    So it sends out the following to another Application (<RemoteApplication>)
    #    (that owns a source that we use) via accessing its ProcessorQueue and
    #    registering these (a Signal actually) depending on the source type.
    EXTERNAL_S3_OBJECT_CREATION_REQUEST = 20
    _AMZN_RESERVED_3 = 21

    # Metrics and Alarming Types
    INTERNAL_METRIC_DATA_CREATION = 30
    CW_METRIC_DATA_CREATION = 31

    # Multiple state Signals that cannot be trivially represented by CREATION - TERMINATION states
    INTERNAL_ALARM_STATE_CHANGE = 35
    CW_ALARM_STATE_CHANGE = 36

    def is_data(self) -> bool:
        return self in [
            SignalType.EXTERNAL_S3_OBJECT_CREATION,
            SignalType.EXTERNAL_GLUE_TABLE_PARTITION_UPDATE,
            SignalType.INTERNAL_PARTITION_CREATION,
            SignalType.LOCAL_FOLDER_CREATION,
        ]

    def is_data_dependent(self) -> bool:
        return self in [
            SignalType.INTERNAL_PARTITION_CREATION,
            SignalType.LOCAL_FOLDER_CREATION,
            # ex: SignalType.INTERNAL_MODEL_CREATION
        ]

    def is_metric(self) -> bool:
        return self in [SignalType.INTERNAL_METRIC_DATA_CREATION, SignalType.CW_METRIC_DATA_CREATION]

    def is_alarm(self) -> bool:
        return self in [SignalType.INTERNAL_ALARM_STATE_CHANGE, SignalType.CW_ALARM_STATE_CHANGE]


class SignalIntegrityProtocol:
    """Immutable pritimitive to encapsulate the necessary information for resource-specific analysis, range-checks (completion, etc)

    Required for runtime mostly. Please check :module:`.analysis` for a glimpse of how protocols might be
    used in runtime.

    For External unmarshaled entities (external raw datasets, etc):
        Provided by app-developers to give a hint to RheocerOS so that it can do runtime analysis.

    For RheocerOS managed entities:
        Not required during app-development, since RheocerOS' protocol for internal entities (datasets,etc)
        relies on a version-specific implementation.

    Example and contract lock-down test:
    >>> my_dataset_completion_check_protol = SignalIntegrityProtocol("MANIFEST", {"file": "manifest.json"})
    >>> print(my_dataset_completion_check_protol )
    SignalIntegrityProtocol(type='MANIFEST', args={'file': 'manifest.json'})
    """

    def __init__(self, type: str, args: Dict[str, Any]) -> None:
        self.type = type
        self.args = args

    def __eq__(self, other) -> bool:
        return type(self) is type(other) and (self.type == other.type and self.args == other.args)

    def __hash__(self) -> int:
        return hash((self.type, self.args))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(type={self.type!r}, args={self.args!r})"


class SignalDomainSpec:
    def __init__(
        self, dimension_spec: DimensionSpec, dimension_filter_spec: DimensionFilter, integrity_check_protocol: SignalIntegrityProtocol
    ) -> None:
        self.dimension_spec = dimension_spec
        self.dimension_filter_spec = dimension_filter_spec
        self.integrity_check_protocol = integrity_check_protocol

    def __eq__(self, other) -> bool:
        return (
            self.dimension_spec == other.dimension_spec
            and self.dimension_filter_spec == other.dimension_filter_spec
            and self.integrity_check_protocol == other.integrity_check_protocol
        )

    def __hash__(self) -> int:
        return hash((self.dimension_spec, self.dimension_filter_spec, self.integrity_check_protocol))

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(dimension_spec={self.dimension_spec!r}, "
            f" dimension_filter_spec={self.dimension_filter_spec!r},"
            f" integrity_check_protocol={self.integrity_check_protocol!r})"
        )


class SignalUniqueKey:
    def __init__(self, alias: str, type: SignalType, source_type: SignalSourceType, path_format: str) -> None:
        self.alias = alias
        self.type = type
        self.source_type = source_type
        self.path_format = path_format

    def __eq__(self, other) -> bool:
        return (
            self.alias == other.alias
            and self.type == other.type
            and self.source_type == other.source_type
            and self.path_format == other.path_format
        )

    def __hash__(self) -> int:
        return hash((self.alias, self.type, self.source_type, self.path_format))

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(alias={self.alias!r}, "
            f" type={self.type!r},"
            f" source_type={self.source_type!r},"
            f" path_format={self.path_format!r})"
        )


class Signal(Serializable["Signal"]):
    """Doc: TODO"""

    def __init__(
        self,
        type: SignalType,
        resource_access_spec: SignalSourceAccessSpec,
        domain_spec: SignalDomainSpec,
        alias: Optional[str] = None,
        is_reference: Optional[bool] = False,  # used by routing runtime to understand if incoming events are strictly expected
        range_check_required: Optional[bool] = False,  # used by routing runtime to see if domain (multiple paths) represented
        # by this signal should be checked or not.
        nearest_the_tip_in_range: Optional[bool] = False,  # routing parameter, satisfies linking condition to use the
        # latest/soonest resource with a range. if range is not satisfied
        # waits and adapts the first incoming signal.
        is_termination: Optional[bool] = False,  # whether this object represents the end/termination of the signal
        is_inverted: Optional[bool] = False,  # whether this object represents the inversion of domain_spec
    ) -> None:
        self.type = type
        self.resource_access_spec = resource_access_spec
        self.domain_spec = domain_spec
        self.alias = alias
        self.is_reference = is_reference
        self.range_check_required = range_check_required
        self.nearest_the_tip_in_range = nearest_the_tip_in_range
        self.is_termination = is_termination
        self.is_inverted = is_inverted

    def _key(self):
        return self.type, self.resource_access_spec.source, self.resource_access_spec.path_format

    def unique_key(self) -> SignalUniqueKey:
        return SignalUniqueKey(self.alias, self.type, self.resource_access_spec.source, self.resource_access_spec.path_format)

    @property
    def is_dependent(self) -> bool:
        return self.is_reference or self.nearest_the_tip_in_range

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(type={self.type!r},"
            f"resource_access_spec={self.resource_access_spec!r},"
            f"domain_spec={self.domain_spec!r},"
            f"alias={self.alias!r},"
            f"is_reference={self.is_reference!r},"
            f"range_check_required={self.range_check_required!r},"
            f"nearest_the_tip_in_range={self.nearest_the_tip_in_range},"
            f"is_termination={self.is_termination},"
            f"is_inverted={self.is_inverted})"
        )

    def __hash__(self):
        return hash(self._key())

    def __eq__(self, other: "Signal"):
        return (
            self._key() == other._key() and (not self.alias or not other.alias or (self.alias == other.alias))
            if other is not None
            else False
        )

    def __ne__(self, other: "Signal"):
        return not self == other

    def __invert__(self) -> Any:
        """Overwrite bitwise not for high-level convenient operations between Signals.
        Outcome depends on underlying resource_access_spec.
        """
        return Signal(
            self.type,
            self.resource_access_spec,
            self.domain_spec,
            self.alias,
            self.is_reference,
            self.range_check_required,
            self.nearest_the_tip_in_range,
            self.is_termination,
            True,
        )

    def __and__(self, other) -> Any:
        """Overwrite bitwise & for high-level operations operations between Signals.
        Outcome depends on underlying resource_access_spec.
        """
        return self.resource_access_spec & (self, other)

    def __or__(self, other) -> Any:
        """Overwrite bitwise | for high-level operations operations between Signals.
        Outcome depends on underlying resource_access_spec.
        """
        return self.resource_access_spec | (self, other)

    def check_integrity(self, other: "Signal") -> bool:
        if self != other:
            return False

        # let access spec do the integrity check. Ex: if there is only a metadata change that would not impact
        # runtime behaviour, then do not return False.
        if not self.resource_access_spec.check_integrity(other.resource_access_spec):
            return False

        # we don't tolerate a bit of change in the domain spec. use equals directly.
        if self.domain_spec != other.domain_spec:
            return False

        if self.alias != other.alias:
            return False

        if self.is_reference != other.is_reference:
            return False

        if self.range_check_required != other.range_check_required:
            return False

        if self.nearest_the_tip_in_range != other.nearest_the_tip_in_range:
            return False

        if self.is_termination != other.is_termination:
            return False

        if self.is_inverted != other.is_inverted:
            return False

        return True

    @overload
    @classmethod
    def ground(cls, alias: str, dimension_spec: Optional[DimensionSpec] = None) -> "Signal": ...

    @classmethod
    def ground(cls, alias_or_other_signal: "Signal", dimension_spec: Optional[DimensionSpec] = None) -> "Signal":
        """Convenience method to provide a 'ground signal' that can be used to tether any other entity/signal
        in the system so that it can be triggered, controlled seamlessly.

        It mimics the target signal, adapts its alias and also the dimension spec.
        """
        if isinstance(alias_or_other_signal, Signal):
            alias = alias_or_other_signal.alias
            dimension_spec = alias_or_other_signal.domain_spec.dimension_spec
            dimension_filter = alias_or_other_signal.domain_spec.dimension_filter_spec
        else:
            alias = alias_or_other_signal
            if dimension_spec is None:
                raise ValueError(f"DimensionSpec must be provided for ground signal {alias!r}!")
            dimension_filter = DimensionFilter.load_raw(
                [AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR for d in range(len(dimension_spec.get_flattened_dimension_map().keys()))]
            )
            dimension_filter.set_spec(dimension_spec)

        alias = alias + "_GROUND"

        return Signal(
            SignalType.INTERNAL_SYSTEM_GROUND_TETHER,
            SystemSignalSourceAccessSpec(tether_id=alias, target_entity_dimension_spec=dimension_spec),
            SignalDomainSpec(dimension_spec, dimension_filter, integrity_check_protocol=None),
            alias,
        )

    def clone(self, alias: str, deep: bool = True) -> "Signal":
        clone = copy.deepcopy(self) if deep else self
        return Signal(
            clone.type,
            clone.resource_access_spec,
            clone.domain_spec,
            alias,
            clone.is_reference,
            clone.range_check_required,
            clone.nearest_the_tip_in_range,
            clone.is_termination,
            clone.is_inverted,
        )

    def check_termination(self, incoming_signal: "Signal") -> bool:
        # if singals already know the answer, use it.
        if self.is_termination or incoming_signal.is_termination:
            return True

        # do resource specific analysis that goes through dimensions to detect termination
        return self.resource_access_spec.check_termination(
            current=self.domain_spec.dimension_filter_spec, new=incoming_signal.domain_spec.dimension_filter_spec
        )

    def apply(self, other: "Signal") -> Optional["Signal"]:
        new_filter = self.domain_spec.dimension_filter_spec.apply(other.domain_spec.dimension_filter_spec)
        if new_filter is not None:
            return self.filter(new_filter)
        return None

    def create_from_spec(self, access_spec: SignalSourceAccessSpec, signal_type: Optional[SignalType] = None) -> "Signal":
        return Signal(
            self.type if not signal_type else signal_type,
            access_spec,
            self.domain_spec,
            self.alias,
            self.is_reference,
            self.range_check_required,
            self.nearest_the_tip_in_range,
            self.is_termination,
            self.is_inverted,
        )

    def create(
        self,
        signal_type: SignalType,
        source_type: SignalSourceType,
        materialized_resource_path: str,
        is_termination: bool = False,
        from_transformed: bool = False,
        transform_source: bool = False,
    ) -> Optional["Signal"]:
        if not self.resource_access_spec.proxy:
            # if there is no proxy linked to the resource access spec, then this simple check is enough to reject.
            if signal_type != self.type or source_type != self.resource_access_spec.source:
                return None

        # extract actual/pyhsical resource
        signal_source: SignalSource = self.resource_access_spec.extract_source(
            materialized_resource_path, self.get_required_resource_name()
        )

        if signal_source:
            # check protocol
            if self.domain_spec.integrity_check_protocol and signal_source.name is not None:
                integrity_checker: SignalIntegrityChecker = INTEGRITY_CHECKER_MAP[self.domain_spec.integrity_check_protocol.type]
                if not integrity_checker.check(signal_source, self.domain_spec.integrity_check_protocol):
                    return None

            # allow empty lists, this will support Signals with path_formats without any variants and no filter specs.
            if self.domain_spec.dimension_filter_spec.check_compatibility(signal_source.dimension_values, False):
                dimension_filter = DimensionFilter.load_raw(
                    signal_source.dimension_values, cast=self.domain_spec.dimension_spec, error_out=False
                )
                if dimension_filter is not None:  # bad cast -> None
                    self_filter = self.domain_spec.dimension_filter_spec
                    if from_transformed:
                        # TODO drop transformation metadata from dimension_filter. if returned signal is used again for
                        #   path rendering then transformation would be applied again. Currently "from_transformed" is
                        #   only used by analyzer in a very limited way.
                        self_filter = self_filter.transform()
                    chained_filter = self_filter.chain(dimension_filter)
                    if chained_filter or (not self.domain_spec.dimension_filter_spec and chained_filter is not None):
                        # Do not use the chained filter but use it to do the following
                        # param transfer:
                        #
                        # transfer full type information + params
                        # ex: transfer 'format', 'granularity' params from the origin
                        # we have to chain because these information (i.e params) might change based on breadth.
                        # ex: datetime params for a region might be different than another region
                        dimension_filter.set_spec(chained_filter)
                        if transform_source:
                            dimension_filter = dimension_filter.transform()
                        return Signal(
                            self.type,
                            self.resource_access_spec,
                            SignalDomainSpec(self.domain_spec.dimension_spec, dimension_filter, self.domain_spec.integrity_check_protocol),
                            None,  # alias
                            self.is_reference,
                            self.range_check_required,
                            self.nearest_the_tip_in_range,
                            is_termination,
                            self.is_inverted,
                        )
        return None

    @overload
    def chain(self, other: "Signal", new_alias: str = None) -> Optional["Signal"]: ...

    @overload
    def chain(self, other: DimensionFilter, new_alias: str = None) -> Optional["Signal"]: ...

    @overload
    def chain(self, other: RawDimensionFilterInput, new_alias: str = None) -> Optional["Signal"]: ...

    def chain(self, other: Union["Signal", Union[DimensionFilter, RawDimensionFilterInput]], new_alias: str = None) -> Optional["Signal"]:
        other_filter = other.domain_spec.dimension_filter_spec if isinstance(other, Signal) else other
        new_filter = self.domain_spec.dimension_filter_spec.chain(other_filter, False)

        if new_filter is not None:
            return self.filter(new_filter, new_alias)
        return None

    @overload
    def filter(self, new_filter: RawDimensionFilterInput, new_alias: str = None, transfer_spec: bool = False) -> "Signal": ...

    @overload
    def filter(self, new_filter: DimensionFilter, new_alias: str = None, transfer_spec: bool = False) -> "Signal": ...

    def filter(
        self, new_filter: Union[DimensionFilter, RawDimensionFilterInput], new_alias: str = None, transfer_spec: bool = False
    ) -> "Signal":
        filter = (
            new_filter
            if isinstance(new_filter, DimensionFilter)
            else DimensionFilter.load_raw(new_filter, cast=self.domain_spec.dimension_spec)
        )
        if transfer_spec:
            filter.set_spec(self.domain_spec.dimension_spec)
        return Signal(
            self.type,
            self.resource_access_spec,
            SignalDomainSpec(self.domain_spec.dimension_spec, filter, self.domain_spec.integrity_check_protocol),
            self.alias if not new_alias else new_alias,
            self.is_reference,
            self.range_check_required,
            self.nearest_the_tip_in_range,
            self.is_termination,
            self.is_inverted,
        )

    def as_reference(self) -> "Signal":
        return Signal(
            self.type,
            self.resource_access_spec,
            self.domain_spec,
            self.alias,
            True,
            self.range_check_required,
            self.nearest_the_tip_in_range,
            self.is_termination,
            self.is_inverted,
        )

    def range_check(self, enabled: Optional[bool] = True) -> "Signal":
        if self.nearest_the_tip_in_range:
            raise ValueError("Cannot set 'range_check' as True while 'nearest_the_tip_in_range' is True!")

        return Signal(
            self.type,
            self.resource_access_spec,
            self.domain_spec,
            self.alias,
            self.is_reference,
            enabled,
            self.nearest_the_tip_in_range,
            self.is_termination,
            self.is_inverted,
        )

    def as_nearest_in_range(self) -> "Signal":
        if self.range_check_required:
            raise ValueError("Cannot set 'nearest_the_tip_in_range' as True while range_check_required is True!")

        return Signal(
            self.type,
            self.resource_access_spec,
            self.domain_spec,
            self.alias,
            self.is_reference,
            self.range_check_required,
            True,
            self.is_termination,
            self.is_inverted,
        )

    def invert(self) -> "Signal":
        return ~self

    def feed(self, dim_readers: Sequence[DimensionVariantReader]) -> None:
        return self.domain_spec.dimension_filter_spec.feed(dim_readers)

    def map(self, dim_mappers: Sequence[DimensionVariantMapper]) -> "Signal":
        return self.filter(self.domain_spec.dimension_filter_spec.map(dim_mappers))

    def materialize(self, dim_mappers: Sequence[DimensionVariantMapper], allow_partial: bool = False) -> "Signal":
        new_filter: DimensionFilter = (
            DimensionFilter.materialize(self.domain_spec.dimension_spec, dim_mappers, allow_partial=allow_partial)
            if self.domain_spec.dimension_spec
            else DimensionFilter()
        )

        # sanity check
        result = self.domain_spec.dimension_filter_spec.apply(new_filter)
        if result is None or (self.domain_spec.dimension_filter_spec and not result):
            raise TypeError(
                f"Signal cannot be materialized with dimension values which are not compatible"
                f" with its filter spec. Signal: {self!r}, dim_mappers: {dim_mappers!r}"
            )

        return self.filter(new_filter)

    def get_materialized_access_specs(self, transform: bool = True) -> List[SignalSourceAccessSpec]:
        return self.resource_access_spec.materialize_for_filter(self.domain_spec.dimension_filter_spec, transform)

    def get_materialized_resource_paths(self) -> List[str]:
        return [spec.path_format for spec in self.get_materialized_access_specs()]

    def get_materialized_resources(self) -> List[str]:
        if self.domain_spec.integrity_check_protocol:
            return [
                source_spec.path_format
                + source_spec.path_delimiter()
                + INTEGRITY_CHECKER_MAP[self.domain_spec.integrity_check_protocol.type].get_required_resource_name(
                    source_spec, self.domain_spec.integrity_check_protocol
                )
                for source_spec in self.get_materialized_access_specs()
            ]

    def get_required_resource_name(self) -> Optional[str]:
        required_resource_name: Optional[str] = None
        if self.domain_spec.integrity_check_protocol:
            integrity_checker = INTEGRITY_CHECKER_MAP[self.domain_spec.integrity_check_protocol.type]
            required_resource_name = integrity_checker.get_required_resource_name(
                self.resource_access_spec, self.domain_spec.integrity_check_protocol
            )
        return required_resource_name

    def extract_dimensions(self, materialized_path: str) -> List[Any]:
        full_path = materialized_path
        required_resource_name = self.get_required_resource_name()
        if required_resource_name:
            full_path = full_path + self.resource_access_spec.path_delimiter() + required_resource_name
        elif self.resource_access_spec.path_format_requires_resource_name():
            full_path = full_path + self.resource_access_spec.path_delimiter() + "_resource"
        return self.resource_access_spec.extract_source(full_path, self.get_required_resource_name()).dimension_values

    def dimension_values(self, dimension_name: str) -> List[Any]:
        """Gets the entire range of values for the input dimension from the underlying dimension_filter_spec"""
        if not self.domain_spec.dimension_spec.find_dimension_by_name(dimension_name):
            raise ValueError(
                f"Dimension {dimension_name!r} cannot be found on signal {self.alias!r}! It should be one of these dimensions: {[key for key in self.domain_spec.dimension_spec.get_flattened_dimension_map().keys()]!r}"
            )
        dimension_variants: List["DimensionsVariant"] = self.domain_spec.dimension_filter_spec.get_dimension_variants(dimension_name)
        return [dim.transform().value for dim in dimension_variants]

    def dimension_values_map(self) -> Dict[str, List[Any]]:
        """Get range of values for each dimension from the underlying dimension_filter_spec"""
        from collections import OrderedDict

        return OrderedDict(
            (key, self.dimension_values(key)) for key in self.domain_spec.dimension_spec.get_flattened_dimension_map().keys()
        )

    def dimension_type_map(self) -> Dict[str, List[Any]]:
        """Get type for each dimension from the underlying dimension_spec"""
        from collections import OrderedDict

        return OrderedDict((key, dim.type.value) for key, dim in self.domain_spec.dimension_spec.get_flattened_dimension_map().items())

    def tip_value(self, dimension_name: str) -> Any:
        """Gets the TIP value from a range of variants of input dimension"""
        return self.dimension_values(dimension_name)[0]

    def __getitem__(self, dimension_name: str) -> Any:
        """Convenience wrapper around "tip_value" method to provide the TIP value of a dimension from the underlying dimension_filter_spec"""
        return self.tip_value(dimension_name)

    def tip(self) -> "Signal":
        """Return the TIP of the signal as a new Signal if its dimension_filter represents a range.
        TIP here corresponds to the first branch of the dimension_filter.
        """
        return self.filter(new_filter=self.domain_spec.dimension_filter_spec.tip())

    def serialize(self) -> str:
        return dumps(self)

    @classmethod
    def deserialize(cls, serialized_str: str) -> "Signal":
        return loads(serialized_str)


class SignalProvider:
    @abstractmethod
    def get_signal(self, alias: str = None) -> Signal: ...


class SignalDimensionTuple:
    def __init__(self, signal: Signal, *dimension: Dimension) -> None:
        self.signal = signal
        self._dimensions = dimension

    @property
    def dimensions(self) -> Tuple[Dimension]:
        # TODO remove after 1.0 release (backwards compatibility with older versions)
        if hasattr(self, "dimension"):
            return tuple([self.dimension])
        else:
            return self._dimensions

    def __eq__(self, other) -> bool:
        return self.signal == other.signal and self.dimensions == other.dimensions

    def __hash__(self) -> int:
        return hash((self.signal, self.dimensions))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(signal={self.signal!r}, dimensions={self.dimensions!r})"


class SignalDimensionLink:
    """1-1 mapping of Signal:Dimension (lhs_dim) <- func <- Signal:Dimension (rhs_dim)"""

    def __init__(self, lhs_dim: SignalDimensionTuple, dim_link_func: DimensionVariantMapFunc, rhs_dim: SignalDimensionTuple) -> bool:
        assert len(lhs_dim.dimensions) == 1, (
            f"Left hand side operand in dimension link has multiple " f"dimensions {lhs_dim.dimensions!r}! Link {self!r}"
        )
        self.lhs_dim = lhs_dim
        self.dim_link_func = dim_link_func
        self.rhs_dim = rhs_dim
        if len(rhs_dim.dimensions) > 1:
            assert dim_link_func, (
                f"Mapping/link function must be defined when right hand side operand has multiple " f"dimensions {rhs_dim.dimensions!r}"
            )
        link_func_arg_count = self.dim_link_func.__code__.co_argcount
        assert len(rhs_dim.dimensions) == link_func_arg_count, (
            f"Mapping/link function argument count {link_func_arg_count!r} should be"
            f" compatible with the right hand side operand dimensions {rhs_dim.dimensions!r}."
            f" Dimension link: {self!r}"
        )

    def is_equality(self):
        try:
            if self.dim_link_func(self.rhs_dim) is self.rhs_dim:
                return True
        except:
            pass
        return False

    def __eq__(self, other) -> bool:
        return (
            self.lhs_dim == other.lhs_dim and self.dim_link_func == other.dim_link_func and self.rhs_dim == other.rhs_dim
        ) or self.check_integrity(other)

    def __hash__(self) -> int:
        return hash((self.lhs_dim, self.dim_link_func, self.rhs_dim))

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(lhs_dim={self.lhs_dim!r}, dim_link_func={self.dim_link_func.__code__!r}, rhs_dim={self.rhs_dim!r})"
        )

    def check_integrity(self, other: "SignalDimensionLink") -> bool:
        # note: byte-code based check on the link function
        return (
            self.lhs_dim == other.lhs_dim
            and self.dim_link_func.__code__.co_code == other.dim_link_func.__code__.co_code
            and self.dim_link_func.__code__.co_consts == other.dim_link_func.__code__.co_consts
            and self.rhs_dim == other.rhs_dim
        )


"""
N-N Look-up matrix for Signal:Dimension -> Signal:Dimension matching/transformation
"""
DimensionLinkMatrix = List[SignalDimensionLink]


class SignalLinkNode:
    """A link group that consists of one or more input Signals and the dim link matrix that
    shows how to connect them to each other.

    This class provides basic encapsulation and primitives to understand completion/trigger logic
    for an output at runtime. In dev-time, it resolves the filter spec for the output, from the input
    Signal combination.

    See :class:`RuntimeLinkNode` for its runtime utilization.

    Note: Each data-node/transformation in RheocerOS captures its state as a SignalLinkNode + Output Signal.
          See `Application::create` to see the utilization of this class in dev-time.
    """

    def __init__(self, input_signals: Sequence[Signal]) -> None:
        self._link_matrix: DimensionLinkMatrix = []
        self._signals: List[Signal] = list(input_signals)
        # This class is designed to be decoupled from aliasing so both modes are supported.
        # First check the ones that have alias.
        if len({i.alias for i in input_signals if i.alias is not None}) < len([s.alias for s in self._signals if s.alias is not None]):
            raise ValueError(
                f"Input signals should have unique alias'! "
                f"Duplicates exist within {[s.alias for s in input_signals if s is not None]!r}"
            )
        # now check the ones with no alias.
        # we should be able to resolve them uniquely (ex: eliminate cases where same signal is used [irrespective of
        #  diff in their domain_spec since it does not contribute to final equality check].
        input_signal_set_without_alias: Set[Signal] = {i for i in input_signals if i.alias is None}
        if len(input_signal_set_without_alias) != len([s for s in self._signals if s.alias is None]):
            raise ValueError(
                f"Cannot resolve some of the input signals due to missing alias' and identical Signal"
                f" structures. Impacted signal set: "
                f"{[{'type': i.type, 'resource_access_spec': {'source': i.resource_access_spec.source, 'path_format': i.resource_access_spec.path_format}} for i in input_signal_set_without_alias]}"
            )

        if self._signals and len(self.dependent_signals) == len(self._signals):
            raise ValueError(
                "All of the input signals are dependents (e.g 'reference', 'nearest')! At least one of the inputs must poll for an event."
            )

    @property
    def link_matrix(self) -> DimensionLinkMatrix:
        return self._link_matrix

    @property
    def signals(self) -> Iterable[Signal]:
        return self._signals

    @property
    def reference_signals(self) -> List[Signal]:
        return [s for s in self._signals if s.is_reference]

    @property
    def dependent_signals(self) -> List[Signal]:
        return [s for s in self._signals if s.is_reference or s.nearest_the_tip_in_range]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(link_matrix={self._link_matrix}, signals={self._signals})"

    def __hash__(self) -> int:
        return hash(self._signals)

    def __eq__(self, other: "SignalLinkNode") -> bool:
        if len(self._signals) != len(other.signals):
            return False
        return not [signal for signal in self._signals if signal not in other.signals]

    def check_integrity(self, other: "SignalLinkNode") -> bool:
        if self != other:
            return False

        for i, signal in enumerate(self.signals):
            if not signal.check_integrity(other.signals[i]):
                return False

        if len(self.link_matrix) != len(other.link_matrix):
            return False

        for link in self.link_matrix:
            if not any(link.check_integrity(other_link) for other_link in other.link_matrix):
                return False

        return True

    def add_link(self, link: SignalDimensionLink) -> None:
        if not (link.lhs_dim.signal in self._signals and link.rhs_dim.signal in self._signals):
            raise ValueError(f"Cannot add the dimension link {link!r} for a Signal that does not exist within" f" {self!r}")

        self._link_matrix.append(link)

    def check_dangling_dependents(self, materialized_output: Signal, output_dim_matrix: DimensionLinkMatrix) -> None:
        """A similar graph analysis as SignalLinkNode::get_materialized_inputs_for_output, this time focusing on
        the possibility of retrieval/materialization of dependent signals (ref, nearest) from the output and other inputs.

        So the general algo layout is similar but this one is much simpler.
        """
        if not self.dependent_signals:
            return

        # consistent with the runtime behaviour for references we enable this behaviour by default.
        auto_bind_unlinked_input_dimensions: bool = True

        materialized_inputs: List[Signal] = [s for s in self.signals if not s.is_dependent]
        transitive_check_list: List[Tuple[Signal, List[DimensionVariantMapper]]] = []
        for input_signal in self._signals:
            if input_signal.is_dependent and input_signal not in materialized_inputs:
                if input_signal.domain_spec.dimension_filter_spec.is_material():
                    # if it is a materialized reference then we don't need to worry about anything.
                    materialized_inputs.append(input_signal)
                else:
                    # High-level cyclic-dependency check will take care of problematic dependent -> output links.
                    # So while checking dangling dependencies, we utilize all of the possible links.
                    signal_mappers = self._get_dim_mappers(None, input_signal, output_dim_matrix)
                    if signal_mappers:
                        materialized_output.feed(signal_mappers)
                    transitive_check_list.append((input_signal, signal_mappers))

        # TODO refactor this method and SignalLinkNode::get_materialized_inputs_for_output to have the remaining
        #  graph analysis in a separate method. Currently the only difference is in 'c' where we dont check violation
        #  against the output for existing links during auto-binding (since for references it is already verified
        #  above that references cannot have direct links to outputs). And also error messages are different.

        # if transitive_check_list is not empty, then it means that we have a reference that needs to be checked against
        # other inputs and the output (for auto-binding only).

        # So go over those problematic inputs and apply the following logic uniformly:
        #   a- do a (transitive) check against fully materialized inputs (%100 linked with output)
        #      we do 'a' first to capture incompatibilities with other inputs (even if 'b' turns out to be true).
        #       a.1- also check partially satisfied inputs to see if they satisfy any of the dimensions we need.
        #   b- check if those unlinked dimensions are material or not
        #   c- and finally (depending on param 'auto_bind_unlinked_input_dimensions') check if we can create
        #   mappings from materialized inputs and the output for the completely detached dimensions.
        # TODO this graph algorithm should be iterated len(transitive_check_list) times. So the exceptions
        #  should be raised after a final check following the Nth iteration.
        #  - First iteration should have this long 'for' loop doing 'a,b,c'
        #  - Remaining (n - 1) iterations should only do 'a'.
        for input_signal, current_signal_mappers in transitive_check_list:
            signal_mappers: List[DimensionVariantMapper] = current_signal_mappers
            # gather all of the mappers from the materialized list (redundants won't be a problem).
            # 'a'
            for materialized_input in materialized_inputs:
                # transitive lookup (use input dim link matrix this time)
                mappers = self._get_dim_mappers(materialized_input, input_signal, self.link_matrix)
                if mappers:
                    materialized_input.feed(mappers)
                    signal_mappers.extend(mappers)

            satisfied_dimensions = {DimensionAsKey(mapper.target) for mapper in signal_mappers}
            required_dimensions = {
                DimensionAsKey(dim) for dim in input_signal.domain_spec.dimension_spec.get_flattened_dimension_map().values()
            }
            # these dimensions might be intentionally left unlinked by the user (all pass logic)
            unlinked_dimensions = required_dimensions - satisfied_dimensions

            if unlinked_dimensions:
                # 'a.1' check partially satisfied inputs to see if they satisfy any of the dimensions we need.
                for other_signal, other_current_signal_mappers in transitive_check_list:
                    if other_signal != input_signal and other_current_signal_mappers:
                        mappers = self._get_dim_mappers(other_signal, input_signal, self.link_matrix)
                        for mapper in mappers:
                            if mapper.target in unlinked_dimensions:
                                # we need to find if the materialized values for each 'source' of this mapper can be
                                # found in the 'target's of materialized mappers.
                                source_values: List[List[Any]] = []
                                for source in mapper.source:
                                    for other_materialized_mapper in other_current_signal_mappers:
                                        if other_materialized_mapper.target == source:
                                            source_values.append(other_materialized_mapper.map(source))
                                            break
                                if len(source_values) == len(mapper.source):
                                    mapper.set_source_values(source_values)
                                    signal_mappers.append(mapper)
                                    satisfied_dimensions.add(DimensionAsKey(mapper.target))

                # check again
                unlinked_dimensions = required_dimensions - satisfied_dimensions

                # first check if these unlinked dimensions are already materialized.
                # 'b'
                for unlinked_dim in unlinked_dimensions:
                    # FUTURE consider following logic if we observe materialized ref dim not being detected here
                    # (e.g "*" being detected on top of a range of materialized values). this should not occur anymore (but legit though).
                    # already_materialized_variants = [variant for variant in input_signal.domain_spec.dimension_filter_spec.get_dimension_variants(required.dimension.name)
                    #                                  if variant.is_material_value()]
                    # #then use the first variant
                    # if already_materialized_variants
                    #  dimension_variant = already_materialized_variants[0]

                    dimension_variant = cast(
                        DimensionVariant, input_signal.domain_spec.dimension_filter_spec.find_dimension_by_name(unlinked_dim.dimension.name)
                    )
                    if dimension_variant.is_material_value():
                        # good news! this dimension is already materialized. maybe that was the reason it was left
                        # unlinked as well.
                        mapper_from_own_filter = DimensionVariantMapper(
                            unlinked_dim.dimension, unlinked_dim.dimension, DIMENSION_VARIANT_IDENTICAL_MAP_FUNC
                        )
                        input_signal.feed([mapper_from_own_filter])
                        signal_mappers.append(mapper_from_own_filter)
                        satisfied_dimensions.add(unlinked_dim)

                # check again
                unlinked_dimensions = required_dimensions - satisfied_dimensions
                if unlinked_dimensions:
                    # 'c'
                    if auto_bind_unlinked_input_dimensions:
                        # now it is for sure that this input has some unlinked & immaterial dimensions.
                        # if the client wants us to force-link over dimensions, then attempt to do that by checking
                        # Dimension equality (name + type). But while doing that, we have to make sure that the dim
                        # is completely disconnected (no from/to edges from the rest of the input/output network).
                        for unlinked_dim in unlinked_dimensions:
                            # first check the output
                            materialized_match = materialized_output.domain_spec.dimension_spec.find_dimension_by_name(
                                unlinked_dim.dimension.name
                            )
                            if materialized_match:
                                # we can immediately assume that this dim is satisfied since it has been already checked
                                # in the beginning of this routine that output cannot have link with this reference.
                                auto_equality_mapper = DimensionVariantMapper(
                                    materialized_match, unlinked_dim.dimension, DIMENSION_VARIANT_IDENTICAL_MAP_FUNC
                                )
                                materialized_output.feed([auto_equality_mapper])
                                signal_mappers.append(auto_equality_mapper)
                                satisfied_dimensions.add(unlinked_dim)
                                continue

                            for materialized_input in materialized_inputs:
                                materialized_match = materialized_input.domain_spec.dimension_spec.find_dimension_by_name(
                                    unlinked_dim.dimension.name
                                )
                                if materialized_match:
                                    # now check we don't have an outbound edge from this signal to the other input (over the same dim),
                                    # if there is any and we still force this equality, then it is an absurd violation.
                                    mappers_from_input = self._get_dim_mappers(input_signal, materialized_input, self.link_matrix)
                                    if not mappers_from_input or all(unlinked_dim.dimension not in m.source for m in mappers_from_input):
                                        auto_equality_mapper = DimensionVariantMapper(
                                            materialized_match, unlinked_dim.dimension, DIMENSION_VARIANT_IDENTICAL_MAP_FUNC
                                        )
                                        materialized_input.feed([auto_equality_mapper])
                                        signal_mappers.append(auto_equality_mapper)
                                        satisfied_dimensions.add(unlinked_dim)
                                        break
                    else:
                        unmapped_input_dimensions = {dim.dimension.name for dim in unlinked_dimensions}
                        error_msg: str = (
                            f"Cannot do reverse lookup for dependent input: {input_signal.alias!r} from output: {materialized_output.alias} "
                            f"and other inputs! "
                            f"Dependent input dimensions {unmapped_input_dimensions!r} cannot be mapped from "
                            f"independent input dimensions. You must add links to those "
                            f"dimensions from independent inputs: {[s.alias for s in materialized_inputs]!r}"
                        )
                        logger.error(error_msg)
                        raise ValueError(error_msg)

            # recalculate unlinked dimensions to see if we still have any missing.
            unlinked_dimensions = required_dimensions - satisfied_dimensions

            if not unlinked_dimensions:
                # if any of the dimensions of the input signal is not mapped, then this will fail (raise).
                new_input_filter = DimensionFilter.materialize(
                    input_signal.domain_spec.dimension_spec, signal_mappers, coalesce_relatives=True
                )
                test_filter = input_signal.domain_spec.dimension_filter_spec.chain(new_input_filter)
                if input_signal.domain_spec.dimension_filter_spec and not test_filter:
                    raise ValueError(
                        f"Cannot do reverse lookup/materialize input {input_signal.alias!r} because the"
                        f" result of the lookup from the output and other inputs is out of its filter spec."
                        f" inferred materialized filter: {new_input_filter!r}"
                        f" input original filter: {input_signal.domain_spec.dimension_filter_spec!r}"
                    )
                materialized_inputs.append(input_signal.filter(new_input_filter, transfer_spec=True))
            else:
                unmapped_input_dimensions = {dim.dimension.name for dim in unlinked_dimensions}
                error_msg: str = (
                    f"Cannot do reverse lookup for dependent input: {input_signal.alias!r} from output: {materialized_output.alias} "
                    f"and other inputs! "
                    f"Dependent input dimensions {unmapped_input_dimensions!r} cannot be mapped from "
                    f"independent input dimensions. You must add links to those "
                    f"dimensions from independent inputs: {[s.alias for s in materialized_inputs]!r}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

    def get_dim_mappers(self, from_signal: Signal, to_signal: Signal) -> List[DimensionVariantMapper]:
        return self._get_dim_mappers(from_signal, to_signal, self._link_matrix)

    @classmethod
    def _get_dim_mappers(cls, from_signal: Signal, to_signal: Signal, dim_matrix: DimensionLinkMatrix) -> List[DimensionVariantMapper]:
        dim_mappers: List[DimensionVariantMapper] = []
        for dim_link in dim_matrix:
            if not dim_link.lhs_dim.signal and not dim_link.rhs_dim.signal:
                if not from_signal and not to_signal:
                    dim_mappers.append(
                        DimensionVariantMapper(dim_link.rhs_dim.dimensions, dim_link.lhs_dim.dimensions[0], dim_link.dim_link_func)
                    )
            elif from_signal or to_signal:
                is_identical: bool = dim_link.is_equality()

                # check Signal -> None case first
                if (not dim_link.lhs_dim.signal or dim_link.lhs_dim.signal == to_signal) and dim_link.rhs_dim.signal == from_signal:
                    dim_mappers.append(
                        DimensionVariantMapper(dim_link.rhs_dim.dimensions, dim_link.lhs_dim.dimensions[0], dim_link.dim_link_func)
                    )
                elif is_identical:
                    # check None->Signal and Signal->Signal reversible cases
                    if (
                        (not dim_link.lhs_dim.signal or dim_link.lhs_dim.signal == from_signal) and dim_link.rhs_dim.signal == to_signal
                    ) or (
                        (dim_link.lhs_dim.signal and from_signal and dim_link.lhs_dim.signal == from_signal)
                        and (to_signal and dim_link.rhs_dim.signal == to_signal)
                    ):
                        dim_mappers.append(
                            DimensionVariantMapper(dim_link.lhs_dim.dimensions, dim_link.rhs_dim.dimensions[0], dim_link.dim_link_func)
                        )

        return dim_mappers

    def get_output_filter(self, output_desc: DimensionSpec, dim_links: DimensionLinkMatrix) -> DimensionFilter:
        """Get the output representation / filter based on the current input signals and
        the client provided input-output dimension mappings (dim_links)."""
        if not output_desc:
            return DimensionFilter()

        mappers: List[DimensionVariantMapper] = []
        for input_signal in self._signals:
            signal_mappers = self._get_dim_mappers(input_signal, None, dim_links)
            if signal_mappers:
                input_signal.feed(signal_mappers)
                mappers.extend(signal_mappers)

        # literal values assigned to output filter dimensions
        literal_value_mappers = self._get_dim_mappers(None, None, dim_links)
        if literal_value_mappers:
            for mapper in literal_value_mappers:
                if len(mapper.source) > 1:
                    raise ValueError(f"Cannot assign multiple literal values to output dimension! Mapper: {mapper!r}")
                mapper.read(cast("DimensionVariant", mapper.source[0]))
            mappers.extend(literal_value_mappers)

        # -  since range expansion (or user provided multiple values) is possible
        # for the same dimension (also from different inputs that output has `links` with),
        # the following materialization, filter generation (depending on the impl) might contain duplicate variants
        # for the same value (from different mappers) `at the same level` (within its tree structure).
        # This is not a problem, because this is not against the UNION semantics that we want to
        # get from the final output filter here. Duplicate values on a filter (at a specific level)
        # won't effect the overall filtering logic.
        # And also during the access_spec materialization of a Signal, the output paths are all unique, eliminating
        # possible duplicate paths that would arise from within the output `Signal` (with duplicate/identical variants
        # at the same level).
        # - Secondly, we don't want RelativeVariants being mapped to the output filter. This would cause
        # real problems for downstream dependencies of this filter. 'coalesce_relatives' param
        # causes relatives to be replaced with an 'AnyVariant'. The 'tip' of the relative variant can easily be
        # represented with AnyVariant.
        return DimensionFilter.materialize(output_desc, mappers, coalesce_relatives=True)

    def get_materialized_inputs_for_output(
        self,
        output: Signal,
        dim_links: DimensionLinkMatrix,
        auto_bind_unlinked_input_dimensions: Optional[bool] = True,
        already_materialized_inputs: List[Signal] = [],
        enforce_materialization_on_all_inputs: Optional[bool] = True,
    ) -> List[Signal]:
        """Returns the materialized version of inputs if their dimensions are all linked to the output in trival equality.
            Transitive links are also considered over the inputs which have direct connection with the output.
            It will cause ValueError if any of the inputs cannot be materialized from the material dimension values
            from the output. To change this error behaviour and get only as many materialized inputs as possible,
            'enforce_materialization_on_all_inputs' parameter can be set to False.
        :param output: An output signal that has a (fully or partially) material dimension filter spec from which input signals
            can possibly be materialized.
        :param dim_links: Output dimension link matrix that contains the mappings / links between the output and inputs.
        :param auto_bind_unlinked_input_dimensions: use dimensional equality (name + type) as a means to force direct
        mapping onto dangling dimensions of inputs which are not fully mapped to the rest of the node (other inputs + output).
        :param already_materialized_inputs: use the materialized inputs provided by the client as a feed to the graph
        analysis.
        :param enforce_materialization_on_all_inputs: whether the method should error out if any of the inputs cannot be fully
        materialized.
        :return: A list of materialized input signals. Signal::apply semantics applied during materialization which
            finalizes the signal and might cause range expansion.
        """
        materialized_inputs: List[Signal] = list(already_materialized_inputs)
        transitive_check_list: List[Tuple[Signal, List[DimensionVariantMapper]]] = []
        for input_signal in self._signals:
            if input_signal not in materialized_inputs:
                # reverse lookup
                signal_mappers = self._get_dim_mappers(None, input_signal, dim_links)
                # check mappers that require output dimension to be set. _get_dim_mappers cannot assign output dimension
                # to mappers that maps from OUTPUT to an INPUT. Here we set those dimensions using the output signal.
                for mapper in signal_mappers:
                    if all([isinstance(source, OutputDimensionNameType) for source in mapper.source]):
                        mapper.source = [output.domain_spec.dimension_spec.find_dimension_by_name(source) for source in mapper.source]
                if signal_mappers:
                    output.feed(signal_mappers)
                    # since output can be partially materialized, we need to eliminate non-material mappings.
                    signal_mappers = [mapper for mapper in signal_mappers if mapper.is_material()]

                if signal_mappers:
                    # if multiple_values are mapped from a linked source (ex: date range, etc)
                    # choose the most recent, highest (aka the TIP).
                    # Mapper will use materialized value while mapping the output Signal.
                    for mapper in signal_mappers:
                        mapper.set_materialized_value(mapper.mapped_values[0])

                    satisfied_dimensions = {DimensionAsKey(mapper.target) for mapper in signal_mappers}
                    required_dimensions = {
                        DimensionAsKey(dim) for dim in input_signal.domain_spec.dimension_spec.get_flattened_dimension_map().values()
                    }
                    unlinked_dimensions = required_dimensions - satisfied_dimensions
                    if not unlinked_dimensions:
                        # if any of the dimensions of the input signal is not mapped, then this will fail.
                        # but at this it should not happen since unlinked dims are empty.
                        new_input_filter = DimensionFilter.materialize(
                            input_signal.domain_spec.dimension_spec, signal_mappers, coalesce_relatives=True
                        )
                        test_filter = input_signal.domain_spec.dimension_filter_spec.apply(new_input_filter)
                        if input_signal.domain_spec.dimension_filter_spec and not test_filter:
                            raise TypeError(
                                f"Cannot do reverse lookup/materialize input {input_signal.alias!r} because the"
                                f" result of the lookup from the output is out of its filter spec."
                                f" input materialized filter: {new_input_filter!r}"
                                f" input original filter: {input_signal.domain_spec.dimension_filter_spec!r}"
                            )
                        materialized_inputs.append(input_signal.filter(test_filter, transfer_spec=True))
                    else:
                        transitive_check_list.append((input_signal, signal_mappers))
                elif input_signal.domain_spec.dimension_filter_spec.is_material():
                    materialized_inputs.append(input_signal)
                else:
                    transitive_check_list.append((input_signal, []))

        # if transitive_check_list is not empty, then it means that we either have
        # - some inputs that has no direct link with the output (and their dim filter is not fully materialized)
        # - or some other that have dimensions with no direct link with the output

        # So go over those problematic inputs and apply the following logic uniformly:
        #   a- do a (transitive) check against fully materialized inputs (%100 linked with output)
        #      we do 'a' first to capture incompatibilities with other inputs (even if 'b' turns out to be true).
        #       a.1- also check partially satisfied inputs to see if they satisfy any of the dimensions we need.
        #   b- check if those unlinked dimensions are actually material or not
        #   c- and finally (depending on param 'auto_bind_unlinked_input_dimensions') check if we can create
        #   mappings from materialized inputs and the output for the completely detached dimensions.
        # TODO this graph algorithm should be iterated len(transitive_check_list) times. So the exceptions
        #  should be raised after a final check following the Nth iteration.
        #  - First iteration should have this long 'for' loop doing 'a,b,c'
        #  - Remaining (n - 1) iterations should only do 'a'.
        for input_signal, current_signal_mappers in transitive_check_list:
            signal_mappers: List[DimensionVariantMapper] = current_signal_mappers
            own_signal_mappers: List[DimensionVariantMapper] = []
            # gather all of the mappers from the materialized list (redundants won't be a problem).
            # 'a'
            for materialized_input in materialized_inputs:
                # transitive lookup (use input dim link matrix this time)
                mappers = self._get_dim_mappers(materialized_input, input_signal, self.link_matrix)
                if mappers:
                    materialized_input.feed(mappers)
                    signal_mappers.extend(mappers)

            satisfied_dimensions = {DimensionAsKey(mapper.target) for mapper in signal_mappers}
            required_dimensions = {
                DimensionAsKey(dim) for dim in input_signal.domain_spec.dimension_spec.get_flattened_dimension_map().values()
            }
            # these dimensions might be intentionally left unlinked by the user (all pass logic)
            unlinked_dimensions = required_dimensions - satisfied_dimensions

            if unlinked_dimensions:
                # 'a.1' check partially satisfied inputs to see if they satisfy any of the dimensions we need.
                for other_signal, other_current_signal_mappers in transitive_check_list:
                    if other_signal != input_signal and other_current_signal_mappers:
                        mappers = self._get_dim_mappers(other_signal, input_signal, self.link_matrix)
                        for mapper in mappers:
                            if mapper.target in unlinked_dimensions:
                                # we need to find if the materialized values for each 'source' of this mapper can be
                                # found in the 'target's of materialized mappers.
                                source_values: List[List[Any]] = []
                                for source in mapper.source:
                                    for other_materialized_mapper in other_current_signal_mappers:
                                        if other_materialized_mapper.target == source:
                                            source_values.append(other_materialized_mapper.map(source))
                                            break
                                if len(source_values) == len(mapper.source):
                                    mapper.set_source_values(source_values)
                                    signal_mappers.append(mapper)
                                    satisfied_dimensions.add(DimensionAsKey(mapper.target))

                # check again
                unlinked_dimensions = required_dimensions - satisfied_dimensions

                # first check if these unlinked dimensions are already materialized.
                # 'b'
                for unlinked_dim in unlinked_dimensions:
                    dimension_variant = cast(
                        DimensionVariant, input_signal.domain_spec.dimension_filter_spec.find_dimension_by_name(unlinked_dim.dimension.name)
                    )
                    if dimension_variant.is_material_value():
                        # good news! this dimension is already materialized. maybe that was the reason it was left
                        # unlinked as well.
                        mapper_from_own_filter = DimensionVariantMapper(
                            unlinked_dim.dimension, unlinked_dim.dimension, DIMENSION_VARIANT_IDENTICAL_MAP_FUNC
                        )
                        input_signal.feed([mapper_from_own_filter])
                        own_signal_mappers.append(mapper_from_own_filter)
                        satisfied_dimensions.add(unlinked_dim)

                # check again
                unlinked_dimensions = required_dimensions - satisfied_dimensions
                if unlinked_dimensions:
                    # 'c'
                    if auto_bind_unlinked_input_dimensions:
                        # now it is for sure that this input has some unlinked & immaterial dimensions.
                        # if the client wants us to force-link over dimensions, then attempt to do that by checking
                        # Dimension equality (name + type). But while doing that, we have to make sure that the dim
                        # is completely disconnected (no from/to edges from the rest of the input/output network).
                        for unlinked_dim in unlinked_dimensions:
                            # first check the output (output can be partially materialized), so first check if the
                            # found dim variant is material.
                            materialized_match = output.domain_spec.dimension_filter_spec.find_dimension_by_name(
                                unlinked_dim.dimension.name
                            )
                            if materialized_match and materialized_match.is_material_value():
                                # now check we don't have an outbound edge from this signal to the output (over the same dim),
                                # if there is any and we still force this equality, then it is an absurd violation.
                                mappers_from_input = self._get_dim_mappers(input_signal, None, dim_links)
                                if not mappers_from_input or all(unlinked_dim.dimension not in m.source for m in mappers_from_input):
                                    auto_equality_mapper = DimensionVariantMapper(
                                        materialized_match, unlinked_dim.dimension, DIMENSION_VARIANT_IDENTICAL_MAP_FUNC
                                    )
                                    output.feed([auto_equality_mapper])
                                    signal_mappers.append(auto_equality_mapper)
                                    satisfied_dimensions.add(unlinked_dim)
                                    continue

                            for materialized_input in materialized_inputs:
                                materialized_match = materialized_input.domain_spec.dimension_spec.find_dimension_by_name(
                                    unlinked_dim.dimension.name
                                )
                                if materialized_match:
                                    # now check we don't have an outbound edge from this signal to the other input (over the same dim),
                                    # if there is any and we still force this equality, then it is an absurd violation.
                                    mappers_from_input = self._get_dim_mappers(input_signal, materialized_input, self.link_matrix)
                                    if not mappers_from_input or all(unlinked_dim.dimension not in m.source for m in mappers_from_input):
                                        auto_equality_mapper = DimensionVariantMapper(
                                            materialized_match, unlinked_dim.dimension, DIMENSION_VARIANT_IDENTICAL_MAP_FUNC
                                        )
                                        materialized_input.feed([auto_equality_mapper])
                                        signal_mappers.append(auto_equality_mapper)
                                        satisfied_dimensions.add(unlinked_dim)
                                        break
                    elif enforce_materialization_on_all_inputs:
                        unmapped_input_dimensions = {dim.dimension.name for dim in unlinked_dimensions}
                        error_msg: str = (
                            f"Cannot do reverse lookup for input: {input_signal.alias!r} from output: {output.alias}! "
                            f"Input dimensions {unmapped_input_dimensions!r} cannot be mapped from "
                            f"output dimensions: {output.domain_spec.dimension_spec.get_flattened_dimension_map().keys()!r}"
                        )
                        logger.error(error_msg)
                        raise ValueError(error_msg)

            # recalculate unlinked dimensions to see if we still have any missing.
            unlinked_dimensions = required_dimensions - satisfied_dimensions

            if not unlinked_dimensions:
                # enforce the TIP on mappers against possible ranges (on values mapped from other signals).
                for mapper in signal_mappers:
                    mapper.set_materialized_value(mapper.mapped_values[0])
                signal_mappers.extend(own_signal_mappers)

                # if any of the dimensions of the input signal is not mapped, then this will fail.
                new_input_filter = DimensionFilter.materialize(
                    input_signal.domain_spec.dimension_spec, signal_mappers, coalesce_relatives=True
                )
                test_filter = input_signal.domain_spec.dimension_filter_spec.apply(new_input_filter)
                if input_signal.domain_spec.dimension_filter_spec and not test_filter:
                    raise TypeError(
                        f"Cannot do reverse lookup/materialize input {input_signal.alias!r} because the"
                        f" result of the lookup from the output is out of its filter spec."
                        f" input materialized filter: {new_input_filter!r}"
                        f" input original filter: {input_signal.domain_spec.dimension_filter_spec!r}"
                    )
                materialized_inputs.append(input_signal.filter(test_filter, transfer_spec=True))
            elif enforce_materialization_on_all_inputs:
                unmapped_input_dimensions = {dim.dimension.name for dim in unlinked_dimensions}
                error_msg: str = (
                    f"Cannot do reverse lookup for input: {input_signal.alias!r} from output: {output.alias}! "
                    f"Input dimensions {unmapped_input_dimensions!r} cannot be mapped from "
                    f"output dimensions: {output.domain_spec.dimension_spec.get_flattened_dimension_map().keys()!r}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

        return materialized_inputs

    def compensate_missing_links(self) -> None:
        """Make sure that self._link_matrix satisfies all N-N dimension lookup among self._signals.

        Equates same dimensions from different signals if there is no dimension link connecting them already.
        So based on this assumption, if the expected dimension is not found this raises ValueError indicating
        that referential integrity cannot be established.

        :return: None
        """
        for i, src_signal in enumerate(self._signals):
            for j, dst_signal in enumerate(self._signals):
                if j > i:
                    src_dims = {key for key in src_signal.domain_spec.dimension_spec.get_flattened_dimension_map().keys()}
                    for src_dim in src_dims:
                        satisfied = False
                        # scan existing links
                        for dim_link in self._link_matrix:
                            if (
                                dim_link.rhs_dim.signal == src_signal
                                and dim_link.lhs_dim.signal == dst_signal
                                and any(rhs_dim.name == src_dim for rhs_dim in dim_link.rhs_dim.dimensions)
                            ):
                                satisfied = True
                                break
                        if not satisfied:
                            # lhs = dst_signal.clone(None)
                            left_dim: Optional[Dimension] = dst_signal.domain_spec.dimension_spec.find_dimension_by_name(src_dim)
                            if left_dim:
                                # make sure there are no existing non-trivial reverse links from left_dim to src_dim
                                # Note/Reminder: in the follow if-statement, each dim_link object is of type SignalDimensionLink
                                #  rhs_dim: means the source for that link. it is a pair of source Signal and Dimension(s)
                                #  lhs_dim: means the destination for that link. it is a pair of destination/target Signal and Dimension.
                                avoid_auto_link = False
                                for dim_link in self._link_matrix:
                                    if (
                                        # reset alias to capture non-trivial links from other versions of both lhs and rhs
                                        dim_link.rhs_dim.signal == dst_signal.clone(None, deep=False)
                                        and dim_link.lhs_dim.signal == src_signal.clone(None, deep=False)
                                        and any(lhs_dim.name == src_dim for lhs_dim in dim_link.lhs_dim.dimensions)
                                        and any(rhs_dim.name == left_dim.name for rhs_dim in dim_link.rhs_dim.dimensions)
                                    ) or (
                                        # and other direction too
                                        dim_link.lhs_dim.signal == dst_signal.clone(None, deep=False)
                                        and dim_link.rhs_dim.signal == src_signal.clone(None, deep=False)
                                        and any(rhs_dim.name == src_dim for rhs_dim in dim_link.rhs_dim.dimensions)
                                        and any(lhs_dim.name == left_dim.name for lhs_dim in dim_link.lhs_dim.dimensions)
                                    ):
                                        # - when dst_signal has any other dim in the mapping then it is still a non-trivial mapping
                                        # - when the mapper function is not equality
                                        if len(dim_link.rhs_dim.dimensions) > 1 or not dim_link.is_equality():
                                            logger.critical(
                                                f"Cannot auto-link dimension {src_dim!r} from source: {src_signal.alias!r} to: {dst_signal.alias!r}! "
                                                f"The reason is an existing non-trivial reverse link from {dst_signal.alias!r} to {src_signal.alias!r} "
                                                f"over the same dimension. Link {dim_link!r}"
                                            )
                                            avoid_auto_link = True
                                            break

                                if not avoid_auto_link:
                                    logger.info(
                                        f"Auto-linking dimension {src_dim!r} from source: {src_signal.alias!r} to: {dst_signal.alias!r}"
                                    )
                                    right_dim = src_signal.domain_spec.dimension_spec.find_dimension_by_name(src_dim)
                                    self.add_link(
                                        SignalDimensionLink(
                                            SignalDimensionTuple(dst_signal, left_dim),
                                            DIMENSION_VARIANT_IDENTICAL_MAP_FUNC,
                                            SignalDimensionTuple(src_signal, right_dim),
                                        )
                                    )

    def can_receive(self, signal: Signal):
        return signal in self._signals
