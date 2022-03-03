# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Module Doc:

  TODO

"""
import json
from abc import ABC, abstractmethod
from typing import Any, List, Optional

from intelliflow.core.signal_processing import DimensionFilter, Signal
from intelliflow.core.signal_processing.definitions.dimension_defs import DatetimeGranularity
from intelliflow.core.signal_processing.dimension_constructs import (
    DateVariant,
    DimensionVariantFactory,
    DimensionVariantResolverScore,
    LongVariant,
    ParamsDictType,
    RawDimensionFilterInput,
    RelativeVariant,
    StringVariant,
)
from intelliflow.core.signal_processing.signal import SignalProvider


class _FilterViewMeta(type(ABC), type(SignalProvider)):
    pass


class FilteredView(ABC, SignalProvider, metaclass=_FilterViewMeta):
    """Base class for user-level entities aiding in the incremental build of new filters for a Signal via indexing.

    Each indexing action yields another view by chaining the filtering level information to the current filter.

    When the filter is compatible with the underlying Signal, this view can be used to take concrete actions.
    """

    # TODO evaluate interface change for
    #  'filtering_levels -> RawDimensionFilterInput (Union[List[RawVariantType], Dict[RawVariantType, Any]])'
    # so basically this view init interface should better be compatible with
    #   `DimensionFilter.load_raw`
    #
    # But currently we enforce this interface so that FilteredView s will always yield
    # a single Filter branch that would be materialized for the concrete purposes that these view
    # classes are designed for (see :class:`MonitoringView` and :class:`MarshalingView`)
    # However there are cases where we would still be doing operations on a dimension range (multiple branched filter)
    # while using those views (such as MonitoringView).
    def __init__(self, signal: Signal, filtering_levels: RawDimensionFilterInput) -> None:
        self._signal: Signal = signal
        self._filtering_levels = filtering_levels
        if filtering_levels:
            try:
                self._new_filter: DimensionFilter = DimensionFilter.load_raw(
                    self._filtering_levels, cast=self._signal.domain_spec.dimension_spec, error_out=True
                )
            except (ValueError, TypeError) as bad_cast:
                raise ValueError(
                    f"Filtering values {self._filtering_levels!r} not compatible with signal ({self._signal.alias!r})"
                    f"\ndimension spec: {self._signal.domain_spec.dimension_spec!r}."
                    f"\nError: {bad_cast!r}"
                )
        else:
            self._new_filter = None

    @property
    def signal(self) -> Signal:
        return self._signal

    @property
    def filtering_levels(self) -> RawDimensionFilterInput:
        return self._filtering_levels

    @property
    def new_filter(self) -> DimensionFilter:
        return self._new_filter if self._new_filter is not None else self._signal.domain_spec.dimension_filter_spec

    def as_reference(self) -> "FilteredView":
        self._signal = self._signal.as_reference()
        return self

    def reference(self) -> "FilteredView":
        return self.as_reference()

    @property
    def ref(self) -> "FilteredView":
        return self.as_reference()

    @property
    def no_trigger(self) -> "FilteredView":
        return self.as_reference()

    @property
    def no_event(self) -> "FilteredView":
        return self.as_reference()

    @property
    def is_reference(self) -> bool:
        return self._signal.is_reference

    @property
    def is_dependent(self) -> bool:
        return self._signal.is_dependent

    def range_check(self, enabled: Optional[bool] = True) -> "FilteredView":
        self._signal = self._signal.range_check(enabled)
        return self

    def check_range(self) -> "FilteredView":
        return self.range_check(True)

    def nearest(self) -> "FilteredView":
        self._signal = self._signal.as_nearest_in_range()
        return self

    def latest(self) -> "FilteredView":
        return self.nearest()

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(signal={self._signal!r}, "
            f" filtering_levels={self._filtering_levels}, "
            f" is_reference={self.is_reference}, "
            f" range_check={self._signal.range_check_required}, "
            f" nearest={self._signal.nearest_the_tip_in_range})"
        )

    def __getitem__(self, slice_filter: Any) -> "FilteredView":

        """Convenience indexing method for high-level modules to build new filters for a Signal.

        This tries to complete the hierarchy (expected) structure of the filter by appending a new
        breadth to its filter. Expected structure is determined by the source Signal's dimension spec.

        Examples

        1-
        ["2020-05-06":"2020-05-30"]

        yields an interim filter of;

        {
            "2020-05-06": {}
            "2020-05-07": {}
            ...
            "2020-05-30": {}
        }

        and this gets applied to the current filter. If current filter has "*" at its root level, the
        final filter has the Variants shown above, rather than all-pass "*".

        Type inference is in action. So the date-strings above should
        match the type of DimensionVariants. If there is no type information, then they would cause StringVariant
        instantiation and go through (-/+) operations, which would cause undefined behaviour.

        But when a filter is instantiated from a spec (as in development, as part of a Signal for example), type
        information should always be there. So in all of the cases where this API would make any sense, the type
        information will exist.

        2-
        Assume that the filter is already like this:

        <filter>
         {
            "NA": { ... }
            "EU": { ... }
        }

        then

           filter["NA"]

            will yield

           {
             "NA": {...}
           }


        TODO doctest


        Parameters
        ----------
        slice_filter : filter auto-retrieved from Python __getitem__ call

        Returns
        -------
        :class:`FilteredView`: a new filtered view (wrapping the same source `Signal`) is returned (copy semantics)
        """
        if self._filtering_levels is None:
            if not self._signal.domain_spec.dimension_filter_spec:
                raise ValueError(f"Cannot create a filtered view on a Signal with no dimensions! Signal: {self._signal!r}")
            self._filtering_levels = []

        # get the dimension on which filtering is applied now
        dimensions = list(self._signal.domain_spec.dimension_spec.get_flattened_dimension_map().keys())
        if len(dimensions) <= len(self._filtering_levels):
            raise ValueError(f"Extra dimension value {slice_filter!r} provided for {self._signal.alias!r}")
        dimension = dimensions[len(self._filtering_levels)]

        new_filter_raw_input: RawDimensionFilterInput = self._filtering_levels + self.get_raw_filtering_input(slice_filter, dimension)

        return self._chain(self._signal, new_filter_raw_input)

    @abstractmethod
    def _chain(self, signal: Signal, filtering_levels: RawDimensionFilterInput) -> "FilterView":
        ...

    @classmethod
    def get_raw_filtering_input(cls, slice_filter: Any, dimension: "Dimension") -> RawDimensionFilterInput:
        filter_raw_input: RawDimensionFilterInput
        if isinstance(slice_filter, slice):
            # do filtering on the root level DimensionVariants
            # - use variant's -/+ to create a range
            # - check "_" to map the input to RelativeVariant's expected format.
            #
            # raise ValueError if given.step is defined

            # print("slice", slice_filter.start, slice_filter.stop, slice_filter.step)
            variant_values: List[Any] = []
            if not slice_filter.start or slice_filter.start == RelativeVariant.DATUM_DIMENSION_VALUE_SPECIAL_CHAR:
                variant_values.append(RelativeVariant.build_value(int(slice_filter.stop)))
            else:
                params: ParamsDictType = {} if not dimension.params else dimension.params
                step = slice_filter.step
                if isinstance(slice_filter.step, DatetimeGranularity):
                    params.update({DateVariant.GRANULARITY_PARAM: slice_filter.step})
                    step = 1
                elif DatetimeGranularity.has_value(slice_filter.step):
                    params.update({DateVariant.GRANULARITY_PARAM: DatetimeGranularity[slice_filter.step]})
                    step = 1

                variant_range = DimensionVariantFactory.create_variant_range(slice_filter.start, slice_filter.stop, step, params)
                variant_values.extend([variant.value for variant in variant_range])

            filter_raw_input = variant_values
        elif isinstance(slice_filter, tuple):
            # ex: [1:, ::2]
            # We don't support this currently.
            # if this complex scheme is required, `Signal::filter` API should be preferred.

            raise NotImplementedError(
                f"{cls.__name__}::__getitem__ does not support tuple (double slice) access yet."
                f"Might want to check Signal::filter API instead, to create a complex filter."
            )

            # print("multidim", given)
        else:
            # Ex1: Special variant values such as "*" or "_"
            # Ex2: Explicit variant values such as "1", "2020-05-20", etc

            filter_raw_input = [slice_filter]

        return filter_raw_input

    def is_complete(self) -> bool:
        return self._new_filter is None or self._signal.domain_spec.dimension_filter_spec.check_spec_match(self._new_filter)

    # overrides
    def get_signal(self, alias: str = None) -> Signal:
        if self._new_filter is None:
            # ex: MarshalerNode creates the filter only to get a reference with no extra filtering
            return self._signal

        if not self.is_complete():
            self._new_filter = self._signal.resource_access_spec.auto_complete_filter(
                self.filtering_levels, cast=self._signal.domain_spec.dimension_spec
            )

            if self._new_filter is None or not self.is_complete():
                raise ValueError(
                    f"Dimension type mismatch or premature utilization of FilteredView ({self.filtering_levels}"
                    f" on signal (id={self._signal.alias!r}, alias={alias!r})! It is not complete yet,"
                    f" not compatible with the dimension spec of underlying Signal. "
                    f" Right dimension types or further indexing might be required on this FilteredView: {self!r}"
                )

        if self.is_complete():
            new_filter = self._signal.domain_spec.dimension_filter_spec.chain(self._new_filter)
            if self._signal.domain_spec.dimension_filter_spec and not new_filter:
                raise ValueError(
                    f"New filter ({self.filtering_levels!r}) on new signal(id={self._signal.alias!r}, alias={alias!r}) yields empty"
                    f" result against source signal's filter: "
                    f" {self._signal.domain_spec.dimension_filter_spec!r}, "
                    f" Source signal: {self._signal!r}"
                )

            return self._signal.filter(new_filter, alias)

    def __invert__(self) -> Any:
        """Overwrite bitwise not for high-level operations between Signals.
        Finalizes the filtering.
        """
        return ~self.get_signal()

    def __and__(self, other) -> Any:
        """Overwrite bitwise & for high-level operations between Signals.
        Finalizes the filtering.
        """
        return self.get_signal() & other

    def __or__(self, other) -> Any:
        """Overwrite bitwise | for high-level operations between Signals.
        Finalizes the filtering.
        """
        return self.get_signal() | other

    def paths(self) -> List[str]:
        return self.get_signal().get_materialized_resource_paths()

    def path_format(self) -> str:
        return self.get_signal().resource_access_spec.path_format

    def describe(self) -> str:
        signal: Signal = self.get_signal()
        stats = {"id/alias": signal.alias, "source_type": signal.resource_access_spec.source.value}
        if signal.type.is_metric():
            stats.update({"metric_stats": signal.resource_access_spec.create_stats_from_filter(signal.domain_spec.dimension_filter_spec)})
        else:
            stats.update(
                {
                    "dimensions": [
                        (dim_name, dim.type, dim.params)
                        for dim_name, dim in signal.domain_spec.dimension_spec.get_flattened_dimension_map().items()
                    ],
                    "current_filter_values": signal.domain_spec.dimension_filter_spec.pretty(),
                }
            )
        return json.dumps(stats, indent=10, default=repr)

    def dimensions(self) -> str:
        """render human-readable dimension spec in name:type pairs and also render dimension filter."""
        return json.dumps(
            {
                "dimensions": [
                    (dim_name, dim.type, dim.params)
                    for dim_name, dim in self.get_signal().domain_spec.dimension_spec.get_flattened_dimension_map().items()
                ],
                "current_filter_values": self.get_signal().domain_spec.dimension_filter_spec.pretty(),
            },
            indent=11,
        )

    def partitions(self) -> str:
        return self.dimensions()

    def dimension_values(self) -> str:
        return json.dumps(self.get_signal().domain_spec.dimension_filter_spec.pretty(), indent=10)

    def partition_values(self) -> str:
        return self.dimension_values()
