# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Module Doc:

  TODO

"""

import copy
import logging
import sys
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any, Callable, ClassVar, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple
from typing import Type as _Type
from typing import Union, cast, overload

import dateutil
import dateutil.parser
from dateutil.relativedelta import relativedelta
from dateutil.tz import tzoffset

from intelliflow.core.entity import CoreData
from intelliflow.core.signal_processing.definitions.dimension_defs import (
    DEFAULT_DATETIME_GRANULARITY,
    DatetimeGranularity,
    NameType,
    Timezone,
    Type,
)

module_logger: logging.Logger = logging.getLogger(__name__)


class Dimension:
    """RheocerOS conceptualization starts from the dynamic attributes of entities (a distribute resource, datasets, training-jobs, etc)
    which are used to control their high-level (inbound/outbound) relationship with the rest of the world.

    How do they get partitioned? What is the classification as a resource (i.e ARN attributes)?
    How can we differ it from other similar resources/entities and also within the domain of
    entity itself, how can we differentiate one part of it from its other parts/ranges?

    So a dimension is the lowest level concept that RheocerOS uses when trying to manage
    the flow/relationship between entities (datasets, training-jobs, etc).

    That's why we model the entities we are interested in by using Signals, not only for
    facilitating our dev efforts (as a pattern) but also as a consequence of this point of view.

    We are not interested in the entities themselves. We use other frameworks to deal with
    the actual actions to be taken against them.

    We are interested in the entropy, causes & effects around a distributed resource. Therefore,
    It is assumed that the combinations of RheocerOS Dimensions (see :class:`DimensionSpec`) should
    be enough to generically define a distributed resource (an RheocerOS entity).
    """

    NAME_FIELD_ID: ClassVar[str] = "name"
    TYPE_FIELD_ID: ClassVar[str] = "type"

    def __init__(self, name: Optional[NameType] = None, typ: Optional[Type] = None, params: Optional[Dict[str, Any]] = None) -> None:
        self._name = name
        self._type = typ
        self._params = params

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @property
    def type(self) -> Type:
        return self._type

    @type.setter
    def type(self, value: str) -> None:
        self._type = value

    @property
    def params(self) -> Optional[Dict[str, Any]]:
        return self._params

    @params.setter
    def params(self, val: Dict[str, Any]) -> None:
        self._params = val

    def __hash__(self) -> int:
        return hash(id(self))

    def __eq__(self, other: "Dimension"):
        return self._name == other._name and self._type == other._type

    def __ne__(self, other):
        return not self == other

    def _repr_params(self) -> Dict[str, Any]:
        if self.params:
            params = dict()
            for key, value in self.params.items():
                if isinstance(value, Callable):
                    # avoid memory address print-out
                    params[key] = value.__code__.co_code
                else:
                    params[key] = value
        else:
            params = self.params

        return params

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self._name!r}, typ={self._type!r}, params={self._repr_params()!r})"

    def is_identical(self, other: "Dimension") -> bool:
        if self != other:
            return False

        if self.params == other.params:
            return True

        left_params = dict(self.params) if self.params else dict()
        right_params = dict(other.params) if other.params else dict()
        left_params.update({self.NAME_FIELD_ID: self.name, self.TYPE_FIELD_ID: self.type, type: self.type})
        right_params.update({self.NAME_FIELD_ID: other.name, self.TYPE_FIELD_ID: other.type, type: other.type})
        return left_params == right_params

    @staticmethod
    def is_dimension(raw_key_value_pair: Tuple[Any, Any]) -> bool:
        item, value = raw_key_value_pair
        return item != type and item != Dimension.TYPE_FIELD_ID and isinstance(value, Dict)

    @staticmethod
    def is_dimension_param(raw_key_value_pair: Tuple[Any, Any]) -> bool:
        item, value = raw_key_value_pair
        return item != type and item != Dimension.TYPE_FIELD_ID and not isinstance(value, Dict)

    @staticmethod
    def extract_type(params_dict: Dict[Any, Any]) -> Type:
        if Dimension.TYPE_FIELD_ID in params_dict:
            return Type[params_dict[Dimension.TYPE_FIELD_ID]]
        elif type in params_dict:
            return Type[params_dict[type]]
        else:
            return None


class DimensionAsKey(CoreData):
    def __init__(self, dimension: Dimension) -> None:
        self.dimension = dimension

    def __hash__(self) -> int:
        return hash(self.dimension.name)

    def __eq__(self, other):
        if isinstance(other, Dimension):
            return self.dimension == other
        return self.dimension == other.dimension

    def __ne__(self, other):
        return not self == other

    def __repr__(self) -> str:
        return repr(self.dimension)


_DimensionComparator = Callable[[Dimension, Dimension], bool]


class DimensionSpec:
    """Data structure to hold nested dimension spec format.

     We are interested in the entropy, causes & effects around a distributed resource. Therefore,
     It is assumed that the combinations of RheocerOS Dimensions should
     be enough to generically define a distributed resource (an RheocerOS entity).

     So, this class lies in the core of RheocerOS' type-system. A DimensionSpec can be
     thought of as an "uninstantiated" template, generic representation of a resource.

     This class is closely related with :class:`DimensionFilter` which is used to
     "instantiate" / "specialize" (in metaprogramming terminology) a DimensionSpec.

     Layout:
     At each level, it is allowed to have one or more Dimensions.
     For each dimension, again it is possible to declare sub-dimension specs.

     Examples

     { "hyper_param1": {
         "type": STRING
         "sub_param": {
             "type": LONG
         }
       },
       "hyper_param2": {
         "type": STRING
       }
     }

     can be initialized as follows:
     >>> DimensionSpec([Dimension("hyper_param1", Type.LONG), Dimension("hyper_param2", Type.STRING)],\
                       [DimensionSpec([Dimension("sub_param", Type.LONG)],\
                                      [None]),\
                        None])
     <__main__.DimensionSpec object at 0x...>
    """

    def __init__(self, dimensions: List[Dimension] = None, sub_dim_specs: List["DimensionSpec"] = None):
        if (dimensions and not sub_dim_specs) or (not dimensions and sub_dim_specs):
            raise ValueError("Cannot initiate DimensionSpec due to mismatch in dimension and spec lists.")

        self._dict: Dict[Dimension, "DimensionSpec"] = OrderedDict()

        if dimensions and sub_dim_specs:
            if len(dimensions) != len(sub_dim_specs):
                raise ValueError("Cannot initiate DimensionSpec due to mismatch in dimension and spec lists.")
            for index, dim in enumerate(dimensions):
                self.add_dimension(dim, sub_dim_specs[index])

    def add_dimension(self, dimension: Dimension, sub_dimensions: "DimensionSpec", check_duplicate_names: bool = True) -> "DimensionSpec":
        if dimension is None:
            raise ValueError("Cannot add dimension of NoneType to spec!")
        if check_duplicate_names:
            if self.find_dimension_by_name(dimension.name) or (sub_dimensions and sub_dimensions.find_dimension_by_name(dimension.name)):
                raise ValueError(f"Cannot add dimension! Duplicate dimension name {dimension.name!r}")

        self._dict[dimension] = sub_dimensions
        return self

    def get_dimensions(self) -> Iterable[Tuple[Dimension, "DimensionSpec"]]:
        return self._dict.items()

    def get_root_dimensions(self) -> Iterable[Dimension]:
        return self._dict.keys()

    def get_all_sub_dimensions(self) -> Iterable["DimensionSpec"]:
        return self._dict.values()

    def get_flattened_dimension_map(self) -> Dict[str, Dimension]:
        """Returns the packed/sequential form of the dimension hierarchy.
        Branches follow each other according to the order within this spec.

        'dim1'
         |_'dim1_1'
         |
        'dim2
         |_'dim_2_1'

         :returns an ordered 1 level dictionary of all of the dimensions.

         {'dim1': <dim1>, 'dim_1_1': <dim_1_1>, 'dim_2': <dim_2>, 'dim_2_1': <dim_2_1>}
        """
        return self._get_flattened_dimension_map(self)

    @classmethod
    def _get_flattened_dimension_map(cls, spec: "DimensionSpec") -> Dict[str, Dimension]:
        dimensions: Dict[str, Dimension] = OrderedDict()
        if spec:
            for dim, sub_spec in spec.get_dimensions():
                dimensions[dim.name] = dim
                dimensions.update(cls._get_flattened_dimension_map(sub_spec))
        return dimensions

    def get_total_dimension_count(self) -> int:
        return self._get_total_dimension_count(self)

    @classmethod
    def _get_total_dimension_count(cls, spec: "DimensionSpec") -> int:
        total_count = 0
        if spec:
            for dim, sub_spec in spec.get_dimensions():
                total_count = total_count + 1 + cls._get_total_dimension_count(sub_spec)
        return total_count

    def find_dimension_by_name(self, name: str) -> Optional[Dimension]:
        return self._find_dimension_by_name(name, self) if name is not None else None

    @classmethod
    def _find_dimension_by_name(cls, name: str, spec: "DimensionSpec") -> Optional[Dimension]:
        if not spec:
            return None

        for dim, spec in spec.get_dimensions():
            if dim.name == name:
                return dim

            sub_dim = cls._find_dimension_by_name(name, spec)
            if sub_dim:
                return sub_dim

        return None

    @overload
    def check_compatibility(self, dim_hierarchy: List[Any], enable_breadth_check: bool = True) -> bool: ...

    @overload
    def check_compatibility(self, dim_spec: "DimensionSpec", enable_breadth_check: bool = True, enable_type_check: bool = True) -> bool: ...

    def check_compatibility(
        self, arg1: Union["DimensionSpec", List[Any]], enable_breadth_check: bool = True, enable_type_check: bool = True
    ) -> bool:
        if isinstance(arg1, DimensionSpec):
            return self._check_spec_compatibility(arg1, enable_breadth_check, enable_type_check)
        else:
            return self._check_list_compatibility(arg1, enable_breadth_check)

    def _check_list_compatibility(self, nested_dim_list: List[Any], enable_breadth_check: bool = True) -> bool:
        return self._check_list_compatibility_recursive(self, nested_dim_list, enable_breadth_check)

    @classmethod
    def _check_list_compatibility_recursive(
        cls, dim_spec: "DimensionSpec", nested_dim_list: List[Any], enable_breadth_check: bool = True
    ) -> bool:
        if not nested_dim_list and not dim_spec:
            return True

        if (not nested_dim_list and dim_spec) or (nested_dim_list and not dim_spec):
            return False

        if enable_breadth_check and len(dim_spec.get_root_dimensions()) != 1:
            return False

        return cls._check_list_compatibility_recursive(
            next(iter(dim_spec.get_all_sub_dimensions())), nested_dim_list[1:], enable_breadth_check
        )

    def _check_spec_compatibility(
        self, other_spec: "DimensionSpec", enable_breadth_check: bool = True, enable_type_check: bool = True
    ) -> bool:
        """Check if two specs have the same structure

        Examples

        >>> d1 = DimensionSpec([Dimension("region", Type.STRING)], [DimensionSpec([Dimension("day", Type.DATETIME)], [None])])
        >>> d2 = DimensionSpec([Dimension("foo", Type.TIMESTAMP)], [DimensionSpec([Dimension("bar", Type.STRING)], [None])])
        >>> d1._check_spec_compatibility(d2)
        True
        """
        source_spec: DimensionSpec = self
        target_spec: DimensionSpec = other_spec
        return DimensionSpec._check_spec_structure_recursive(source_spec, target_spec, enable_breadth_check, enable_type_check)

    @classmethod
    def _check_spec_structure_recursive(
        cls,
        source_spec: "DimensionSpec",
        target_spec: "DimensionSpec",
        enable_breadth_check: bool = True,
        enable_type_check: bool = False,
        comparator: _DimensionComparator = lambda dim1, dim2: True,
    ) -> bool:
        """Check DimensionSpec structure/layout without considering dimension names.

        Whether the check will be strictly typed (using Dimension:Type) can be controlled by
        'enable_type_check' param.
        """
        if not source_spec and not target_spec:
            return True

        if (
            (source_spec is None and target_spec is not None)
            or (source_spec is not None and target_spec is None)
            or (enable_breadth_check and len(source_spec.get_root_dimensions()) != len(target_spec.get_root_dimensions()))
        ):
            return False

        # go through sub-dimensions using the same order on both sides.
        # if breadth check is not enabled, then skip checking other branches.
        for (src_dim, src_desc), (tgt_dim, tgt_desc) in zip(source_spec.get_dimensions(), target_spec.get_dimensions()):
            if (not src_dim and tgt_dim) or (src_dim and not tgt_dim):
                return False

            if enable_type_check and (src_dim.type != tgt_dim.type):
                return False

            if not comparator(src_dim, tgt_dim):
                return False

            if not cls._check_spec_structure_recursive(src_desc, tgt_desc, enable_breadth_check, enable_type_check, comparator):
                return False

            if not enable_breadth_check:
                break

        return True

    def compensate(self, other_spec: "DimensionSpec", overwrite: bool = False) -> None:
        source_spec: DimensionSpec = self
        target_spec: DimensionSpec = other_spec
        DimensionSpec._compensate_recursive(source_spec, target_spec, overwrite)

    @classmethod
    def _compensate_recursive(cls, source_spec: "DimensionSpec", target_spec: "DimensionSpec", overwrite: bool = False) -> None:
        if not source_spec and not target_spec:
            return

        if (
            (source_spec is None and target_spec is not None)
            or (source_spec is not None and target_spec is None)
            or (len(source_spec.get_root_dimensions()) != len(target_spec.get_root_dimensions()))
        ):
            raise TypeError("Cannot transfer names between incompatible DimensionSpecs!")

        # go through sub-dimensions using the same order on both sides.
        for (src_dim, src_desc), (tgt_dim, tgt_desc) in zip(source_spec.get_dimensions(), target_spec.get_dimensions()):
            if (not src_dim and tgt_dim) or (src_dim and not tgt_dim):
                raise TypeError("Cannot transfer names between incompatible DimensionSpecs!")

            if overwrite or src_dim.name is None:
                src_dim.name = tgt_dim.name

            if overwrite or src_dim.type is None:
                src_dim.type = tgt_dim.type

            cls._compensate_recursive(src_desc, tgt_desc, overwrite)

    @classmethod
    def load_from_pretty(cls, raw_pretty_dict: Dict[str, Any], enable_type_check: bool = True) -> "DimensionSpec":
        """Convenience method to load a spec from a raw dict.

        This also formalizes our front-end (pretty) spec format as well.

        Examples
        >>> pretty_dict = dict({\
            "dim1": {\
                "type": "STRING",\
                "dim2":{"type":"LONG"},\
            }\
        })
        >>> DimensionSpec.load_from_pretty(pretty_dict)
        """
        if raw_pretty_dict:
            return cls._load_from_pretty(raw_pretty_dict, enable_type_check)
        else:
            return DimensionSpec()

    @classmethod
    def _load_from_pretty(cls, raw_pretty_dict: Dict[str, Any], enable_type_check: bool = True) -> Optional["DimensionSpec"]:
        """Recursive/internal loader (that sets specs for leaf nodes as None)"""

        if not (raw_pretty_dict and isinstance(raw_pretty_dict, dict)):
            return None

        new_spec: DimensionSpec = DimensionSpec()

        try:
            for item, value in raw_pretty_dict.items():
                if item != type and item != Dimension.TYPE_FIELD_ID and isinstance(value, Dict):
                    # special handling of special type
                    type_str: str = None
                    if type in value:
                        type_str = value[type]
                    # causes AttributeError if type check is enabled and 'type' does not exist

                    if not type_str:
                        type_str = value[Dimension.TYPE_FIELD_ID] if enable_type_check or Dimension.TYPE_FIELD_ID in value else None
                    typ: Type = Type[type_str] if type_str else None
                    sub_dim_specs: DimensionSpec = cls._load_from_pretty(value, enable_type_check)

                    extra_params: Dict[str, Any] = {i: v for i, v in value.items() if Dimension.is_dimension_param((i, v))}

                    dim: Dimension = Dimension(item, typ, extra_params if extra_params else None)
                    new_spec.add_dimension(dim, sub_dim_specs)
        except Exception as e:
            raise type(e)("Error loading DimensionSpec from raw dict {}".format(raw_pretty_dict) + "\n Reason: " + repr(e))

        return new_spec if new_spec else None

    def pretty(self) -> Dict[str, Any]:
        pass

    def __repr__(self) -> str:
        return repr(self._dict)

    def __str__(self) -> str:
        return str(self._dict)

    def __eq__(self, other: Union["DimensionSpec", Dict, OrderedDict]) -> bool:
        # TODO figure out the problem with ordereddict::__eq__ (don't use repr)
        if isinstance(other, OrderedDict):
            return repr(self._dict) == repr(other)
        elif isinstance(other, dict):
            return repr(dict(self._dict)) == repr(other)
        else:
            return repr(self._dict) == repr(other._dict)

    def __ne__(self, other):
        return not self == other

    def __hash__(self) -> int:
        return hash(repr(self._dict))

    def __bool__(self) -> bool:
        return bool(self._dict)


# internal "type aliases" that represent raw input types for variants
# Any -> any primitive value that will be mapped to Type
# Tuple[Any, Dic[str, Any]] -> any primitive along with its params
ParamsDictType = Dict[str, Any]
RawVariantType = Union[Any, Tuple[Any, ParamsDictType]]


class DimensionVariant(Dimension):
    """This class provides the variant type that would represent the instantiated version of :class:`Dimension`s.

    It holds the actual values of the :class:`Type` s that we support.

    It is more of a runtime concept where raw/external events are interpreted and then mapped to resource paths
    and Signal instances. So for Signal instances to exist, we have to first get specialized/instantiated
    version of Dimensions (variants) and then map them to filters (see :class:`DimensionFilter`).

    Resource paths + filters (of an incoming Signal) can now be mapped to development time Signals.
    This establishes the core semantics of our Signal handling at runtime.

    Mandatory field on a DimensionVariant is the "value" field.
    """

    CAST_PARAMS_FIELD_ID: ClassVar[str] = "_cast_params"
    RANGE_SHIFT_FIELD_ID: ClassVar[str] = "_range_shift"
    MAX_RANGE_SHIFT_VALUE: ClassVar[int] = sys.maxsize

    def __init__(
        self, value: Any, name: Optional[NameType] = None, typ: Optional[Type] = None, params: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(name, typ, params)
        self._value = value

    @property
    def raw_value(self) -> Any:
        return self.value

    @property
    def value(self) -> Any:
        return self._value

    @value.setter
    def value(self, val: Any) -> None:
        self._value = val

    def is_material_value(self) -> bool:
        return True

    def is_special_value(self) -> bool:
        return not self.is_material_value()

    def transform(self) -> "DimensionVariant":
        """Apply user provided range shift or custom transforms on the value of the variant to get the final value.

        Transformation is required whenever a variant value is to be used in high-level compute code when resource
        access is necessary. Untransformed value of the variant is its actual value that must be used to link high-level
        entities such as Signals to others in the context of SignalLinkNodes for example. Actual unmaterialized value
        represents either a direct user input or the value from an incoming event.

        DimensionVariant materialization can be best resembled to Scene Graph rendering where 3D entities (their
        location, dimensions, etc) go through chain of transformations based on the scene hierarchy. Here we have a
        more trivial, one level hierarchy (Signal -> DimensionFilter -> DimensionVariant) but technically limitless
        type of transformations (shift and custom transformations provided by the user).
        """
        return self._shift()._transform() if self.is_material_value() else self

    def _shift(self) -> "DimensionVariant":
        """apply range shift if defined by user as part of dimension variant metadata.
        Shift can be applied as a special transform to materialize the final value of a dimension variant.
        """
        shifted_variant = self
        if self.params:
            shift = self.params.get(self.RANGE_SHIFT_FIELD_ID, None)
            if shift:
                if shift >= 0:
                    shifted_variant = shifted_variant + abs(shift)
                else:
                    shifted_variant = shifted_variant - abs(shift)
        return shifted_variant

    def _transform(self) -> "DimensionVariant":
        """Apply user provided transform <Callable>s (if any) in a chain.
        Transforms are used to materialize the final value of a dimension variant.
        """
        # FUTURE/TODO
        return self

    def apply(self, other: "DimensionVariant", finalize: bool) -> Optional[List["DimensionVariant"]]:
        """Apply another variant to this one in the context of chaining two filters.

        Semantics is self [LHS operand, Left] -> other [RHS operand, Right], as if other is applied to 'self' here.

        But the operation is not in-place, returned object uses 'copy semantics'.

        Variants encapsulate N-N complex interactions during filtering based on their specialization
        and provide a great deal of abstraction to high-level entities (i.e :class:`DimensionFilter`).

        Parameters

        other: a variant evaluated as RHS operand.

        Returns

        Optional[List[DimensionVariant]] : return zero or more variants as the result of this operation (copy semantics)
        """
        results = self._apply(other, finalize)
        if not results:  # try other direction now. to enable N-N polymorphic check
            results = other._apply(self, finalize)

        if results:
            # preserve attributes in case cloned `other`
            # should not be critical since the over all compatibility should still be intact.
            # but it is better to preserve user provided specifics anywhere possible.
            for result in results:
                if not result.name:
                    result.name = self.name
                if not result.type:
                    result.type = self.type
                if not result.params:
                    result.params = self.params
                elif self.params:
                    result.params.update(self.params)
                    result.params = result.params

        return results

    def _apply(self, other: "DimensionVariant", finalize: bool) -> Optional[List["DimensionVariant"]]:
        return [copy.deepcopy(self)] if self == other or other == self else None

    def check_type(self, other: Dimension) -> bool:
        """Enforce type if it is specified only"""
        return not self._type or not other.type or (self._type == other.type)

    def check_value(self, value: Any) -> bool:
        return self._value == value

    def __hash__(self) -> int:
        return super().__hash__()

    def __eq__(self, other: Any):
        if isinstance(other, DimensionVariant):
            return self.check_type(other) and self._value == other._value
        elif isinstance(other, Dimension):
            return self.check_type(other)
        else:
            return self.check_value(other)

    def __repr__(self) -> str:
        return f"""{self.__class__.__name__}(name={self._name!r},
                                             typ={self._type!r},
                                             value={self._value!r},
                                             params={self._repr_params()!r})"""

    def __add__(self, value: Any) -> "DimensionVariant":
        new_variant: DimensionVariant = copy.deepcopy(self)
        new_variant.value = self.value + value
        return new_variant

    def __sub__(self, value: Any) -> "DimensionVariant":
        new_variant: DimensionVariant = copy.deepcopy(self)
        new_variant.value = self.value - value
        return new_variant


DimensionVariantMapFunc = Callable[[Any], Any]

DIMENSION_VARIANT_IDENTICAL_MAP_FUNC = lambda dim: dim


class DimensionVariantReader:
    def __init__(self, source: Union[Union[Dimension, str], Tuple[Union[Dimension, str]]]) -> None:
        self.source = source
        self._read_variants: Dict[str, List[DimensionVariant]] = {self._get_source_name(source): [] for source in self._source}

    @classmethod
    def _get_source_name(cls, src: Union[Dimension, str]):
        return src if isinstance(src, str) else src.name

    def read(self, source_variant: DimensionVariant) -> bool:
        if any([self._get_source_name(source) == source_variant.name for source in self._source]):
            self._read_variants.setdefault(source_variant.name, []).append(source_variant)
            return True
        return False

    @property
    def source(self) -> Tuple[Union[Dimension, str]]:
        return self._source

    @source.setter
    def source(self, source: Union[Union[Dimension, str], Tuple[Union[Dimension, str]]]) -> None:
        self._source = tuple(source) if isinstance(source, (tuple, list)) else tuple([source])

    @property
    def values(self) -> Dict[str, List[Any]]:
        return {
            self._get_source_name(source): [variant.value for variant in self._read_variants.get(self._get_source_name(source), [])]
            for source in self._source
        }

    @property
    def variants(self) -> Dict[str, List[DimensionVariant]]:
        return self._read_variants


class DimensionVariantMapper(DimensionVariantReader):
    def __init__(
        self,
        source: Union[Union[Dimension, str], Tuple[Union[Dimension, str]]],
        target: Union[Dimension, str],
        func: DimensionVariantMapFunc,
    ) -> None:
        super().__init__(source)
        self._target = target if isinstance(target, Dimension) else Dimension(target, None)
        self._func = func
        self._mapped_target_index = 0
        self._mapped_values: List[Any] = []
        self._materialized_value = None

    def __repr__(self) -> str:
        return repr(
            (
                self._source,
                self._target,
                self._func.__code__.co_code,
                {"read_variants": self._read_variants, "mapped_values": self._mapped_values},
            )
        )

    @property
    def target(self) -> Dimension:
        return self._target

    @property
    def func(self) -> DimensionVariantMapFunc:
        return self._func

    @property
    def mapped_values(self) -> List[Any]:
        return self._mapped_values

    def set_materialized_value(self, value: Any):
        self._materialized_value = value

    def set_source_values(self, raw_values: List[List[Any]]):
        self._mapped_values.extend([self._func(*source_args) for source_args in zip(raw_values)])

    def is_material(self) -> bool:
        return all([all(variant.is_material_value() for variant in source_variants) for source_variants in self._read_variants.values()])

    def _check_if_mapping_possible(self) -> bool:
        """Check source dimensions all have variants read for the next mapping (as args to next func call)"""
        return all([len(source_variants) > self._mapped_target_index for source_variants in self._read_variants.values()])

    # overrides
    def read(self, source_variant: DimensionVariant) -> bool:
        is_read = super().read(source_variant)
        if is_read and self._check_if_mapping_possible():
            # extract the variants for each source for the func call, strictly obeying the 'source' order
            source_variants_as_args = [
                self._read_variants[self._get_source_name(source)][self._mapped_target_index] for source in self.source
            ]
            self._mapped_target_index = self._mapped_target_index + 1
            if all([source_var.is_material_value() for source_var in source_variants_as_args]):
                try:
                    self._mapped_values.append(self._func(*[source_var.raw_value for source_var in source_variants_as_args]))
                except (
                    Exception
                ) as error:  # map errors in user mapper funcs as to RuntimeErrors! avoid error swallowing at high-evel orchestration code.
                    raise RuntimeError(error)
            else:
                # too early for mapping (not finalized/materialized), read the 'special' value from either
                # (actually the first source) as is. not big deal. just to inform the user and for compatibility.
                first_non_materialized_var = [source_var for source_var in source_variants_as_args if not source_var.is_material_value()][0]
                self._mapped_values.append(first_non_materialized_var.value)
        return is_read

    def map(self, target: Dimension) -> List[Any]:
        if self._target.name == target.name:
            return [self._materialized_value] if self._materialized_value else self._mapped_values
        return []


class DimensionVariantCreatorMethod(ABC):
    @classmethod
    @abstractmethod
    def create(cls, raw_value: Any, params_dict: ParamsDictType) -> DimensionVariant:
        pass


class DimensionVariantResolverScore(Enum):
    MATCH = 1
    TENTATIVE = 2
    UNMATCH = 3


class DimensionVariantResolver(ABC):
    class Result(CoreData):
        def __init__(self, score: DimensionVariantResolverScore, creator: DimensionVariantCreatorMethod) -> None:
            self.score = score
            self.creator = creator

    @classmethod
    @abstractmethod
    def resolve(cls, raw_value: Any) -> Result:
        pass


class DimensionVariantFactory:
    """Factory class that provides registration API for new Variants and also an instantiation API
    to help variant creation from raw data or type info.

    This factory is designed to be generic (a central registrar) to allow future expansions in the variant
    type system.

    >>> type(DimensionVariantFactory.create_variant("2020-05-22"))
    <class '...DateVariant'>
    """

    _RESOLVERS: ClassVar[List[_Type[DimensionVariantResolver]]] = []
    _TYPE_TO_CREATORS_MAP: ClassVar[Dict[Type, _Type[DimensionVariantCreatorMethod]]] = {}
    _logger: ClassVar[logging.Logger] = logging.getLogger("DimensionVariantFactory")

    MAX_RANGE_LIMIT: ClassVar[int] = 9999

    def __new__(cls, *args, **kwargs):
        raise TypeError(f"{cls.__class__.__name__} cannot be instantiated!")

    @classmethod
    def register_resolver(cls, resolver: DimensionVariantResolver) -> None:
        cls._RESOLVERS.append(resolver)

    @classmethod
    def register_creator(cls, typ: Type, creator: DimensionVariantCreatorMethod) -> None:
        cls._TYPE_TO_CREATORS_MAP[typ] = creator

    @classmethod
    def get_creator(cls, typ: Type) -> DimensionVariantCreatorMethod:
        return cls._TYPE_TO_CREATORS_MAP[typ]

    @classmethod
    def create_variant_range(cls, start: Any, stop: Any, step: int, params_dict: ParamsDictType) -> List[DimensionVariant]:
        start_variant = cls.create_variant(start, params_dict)
        stop_variant = cls.create_variant(stop, params_dict)

        range: List[DimensionVariant] = []
        if type(start_variant) == type(stop_variant):
            while True:
                range.append(start_variant)
                for i in range(step):
                    if start_variant.value != stop_variant.value:
                        start_variant = start_variant + 1
                    else:
                        break

                if start_variant.value == stop_variant.value:
                    break

                if len(range) > cls.MAX_RANGE_LIMIT:
                    module_logger.critical(
                        f"Range creation for (start: {start}, stop: {stop},"
                        f" step: {step}, params_dict: {params_dict}) exceeded limit {cls.MAX_RANGE_LIMIT}"
                    )
                    break
        else:
            raise ValueError(
                f"Cannot create dimension range due to type mismatch between "
                f"start: {start} (variant: {start_variant}) and stop: {stop} (variant: {stop_variant}"
            )

        return range

    @classmethod
    def create_variant(cls, raw_variant: Any, params_dict: ParamsDictType) -> DimensionVariant:
        result_map: Dict[DimensionVariantResolverScore, List[DimensionVariantCreatorMethod]] = {}
        for resolver in cls._RESOLVERS:
            result: DimensionVariantResolver.Result = resolver.resolve(raw_variant)
            if result:
                result_map.setdefault(result.score, []).append(result.creator)

        inferred_variant: DimensionVariant = None
        decisive_matches = result_map.get(DimensionVariantResolverScore.MATCH, None)
        decisive_match_count = 0
        if decisive_matches:
            decisive_match_count = len(decisive_matches)
            if decisive_match_count == 1:
                inferred_variant = decisive_matches[0].create(raw_variant, params_dict)

        typ: Type = None
        if params_dict:
            typ = Dimension.extract_type(params_dict)

        if inferred_variant:
            if not typ or inferred_variant.type == typ:
                # we honor resolver matches unless there is a type mismatch.
                # this behaviour enables special variant types (such as any, relative, etc)
                # to decorate dimension variants.
                return inferred_variant

        if not typ and not decisive_matches:
            tentative_matches = result_map.get(DimensionVariantResolverScore.TENTATIVE, [])
            if not tentative_matches:
                raise ValueError(f"Cannot resolve DimensionVariant for raw dimension value: '{raw_variant}' (params_dict: '{params_dict}'")
            else:
                # pick the first tentative match
                tentative_variant = tentative_matches[0].create(raw_variant, params_dict)
                cls._logger.debug(
                    f"Tentatively resolved DimensionVariant ({tentative_variant}) for raw value '{raw_variant}'"
                    f" (params_dict: '{params_dict}'. type: {typ} and there is no decisive match."
                )
                return tentative_variant
        elif decisive_match_count > 1:
            raise TypeError(f"Type mismatch between {decisive_matches} for raw value: '{raw_variant}' (params_dict: '{params_dict}'")
        else:
            cls._logger.debug(
                f"Could not resolve DimensionVariant for raw value '{raw_variant}' (params_dict: '{params_dict}' "
                f". Creating the variant using type: {typ}."
            )

        return cls._TYPE_TO_CREATORS_MAP[typ].create(raw_variant, params_dict)


class AnyVariant(DimensionVariant, DimensionVariantResolver, DimensionVariantCreatorMethod):
    """Represents any value for a specific type"""

    ANY_DIMENSION_VALUE_SPECIAL_CHAR: ClassVar[str] = "*"

    def __init__(self, name: Optional[NameType] = None, typ: Optional[Type] = None, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(None, name, typ)
        self._value = self.ANY_DIMENSION_VALUE_SPECIAL_CHAR
        self._params = params

    # overrides
    def is_material_value(self) -> bool:
        return False

    def _apply(self, other: DimensionVariant, finalize: bool) -> Optional[List[DimensionVariant]]:
        if self == other:
            if isinstance(other, AnyVariant):
                return [copy.deepcopy(self)]
            else:
                return [copy.deepcopy(other)]
        else:
            return None

    def __hash__(self) -> int:
        return Dimension.__hash__(self)

    def __eq__(self, other: Any):
        if isinstance(other, Dimension):
            return self.check_type(other)
        else:
            return self._value == other

    def __add__(self, value: Any) -> DimensionVariant:
        raise ValueError("Bad operation '__add__' on AnyVariant")

    def __sub__(self, value: Any) -> DimensionVariant:
        raise ValueError("Bad operation '__sub__' on AnyVariant")

    @classmethod
    def create(cls, raw_value: Any, params_dict: ParamsDictType) -> DimensionVariant:
        return (
            AnyVariant()
            if not params_dict
            else AnyVariant(params_dict.get(Dimension.NAME_FIELD_ID, None), Dimension.extract_type(params_dict), params_dict)
        )

    @classmethod
    def resolve(cls, raw_value: Any) -> DimensionVariantResolver.Result:
        if raw_value == cls.ANY_DIMENSION_VALUE_SPECIAL_CHAR:
            return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.MATCH, creator=cls)

        return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.UNMATCH, creator=None)


# register this new variant to the factory
DimensionVariantFactory.register_resolver(AnyVariant)


class RelativeVariant(DimensionVariant, DimensionVariantResolver, DimensionVariantCreatorMethod):
    """Represents Variants which don't have a concrete value yet but with a relative index.

    In development, we are capable of declaring "range"s for a dimension. This ranges will map to
    actual values at runtime only, depending on an incoming Signal's (corresponding/compatible)
    dimension values.

    Examples

    RelativeVariant(with relative_index=-30, "last 30 days into the past"):
        {
            "_:-30": { "type": DATETIME }
        }

    Assume that this simple filter (with only one dimension) is as part of the final state of
    one of the Signals in the RoutingTable, then an incoming Signal (that has the same type, etc)
    with the following dimension value shows up.

        {
            "2020-07-30": {}
        }

    It matches the "compatibility" check with the Signal and applied to it.

    Now the Signal in the RoutingTable (which is an input for a Route) is now assumed to be:

    {
        "2020-07-01": {},
        "2020-07-02": {},
        ...
        "2020-07-30": {},
    }

    Following actions (completion checks [aka Integrity Checks, other Signal Analysis] on that Signal will be taken based on this final filter
    """

    DATUM_DIMENSION_VALUE_SPECIAL_CHAR: ClassVar[str] = "_"
    INDEX_SEPARATOR: ClassVar[str] = ":"
    MAX_RELATIVE_INDEX_VALUE: ClassVar[int] = sys.maxsize

    def __init__(
        self, relative_index: int, name: Optional[NameType] = None, typ: Optional[Type] = None, params: Optional[Dict[str, Any]] = None
    ) -> None:
        super().__init__(None, name, typ, params)
        self._value = self.build_value(relative_index)
        self._relative_index = relative_index

    @classmethod
    def build_value(cls, relative_index: int, shift: Optional[int] = None) -> str:
        value = cls.DATUM_DIMENSION_VALUE_SPECIAL_CHAR + cls.INDEX_SEPARATOR + str(relative_index)
        if shift:
            value = value + cls.INDEX_SEPARATOR + str(shift)
        return value

    # overrides
    def is_material_value(self) -> bool:
        return False

    @property
    def relative_index(self) -> int:
        return self._relative_index

    def __hash__(self) -> int:
        return Dimension.__hash__(self)

    def __eq__(self, other: Any) -> bool:
        return DimensionVariant.__eq__(self, other)

    # overrides
    def _apply(self, other: DimensionVariant, finalize: bool) -> Optional[List["DimensionVariant"]]:
        if isinstance(other, RelativeVariant):
            if finalize:
                raise RuntimeError(
                    f"Cannot apply a RelativeVariant {other} to another RelativeVariant {self} " f"as value (finalize = True)."
                )
            else:
                return [self.intersect(other)]
        else:
            if self.check_type(other):
                # end of life-cycle for this relative variant. some concrete value applied down the chain.
                try:
                    # ex: runtime expansion of a whole range upon an incoming event.
                    return self.range(other)
                except ValueError:
                    # let the other variant decide on what to do.
                    pass
        return None

    def intersect(self, other: "RelativeVariant") -> "RelativeVariant":
        """Return the intersection of two relative variants.

        Examples

        "last 30 days"

        {
            "_:-30": {}
        }

        intersects with

        {
            "_:-15": {}
        }

        result:

        {
            "_:-15": {}
        }

        Parameters
        ----------
        other

        Returns
        -------
        RelativeVariant (copy semantics)
        """

        if self._type and other.type and self._type != other._type:
            raise RuntimeError(f"Cannot intersect RelativeVariants {self} and {other} due to type mismatch!")

        new_relative_index: int = 0
        if self._relative_index < 0 and other._relative_index < 0:
            new_relative_index = max(self._relative_index, other._relative_index)
        elif self._relative_index > 0 and other._relative_index > 0:
            new_relative_index = min(self._relative_index, other._relative_index)

        return RelativeVariant(new_relative_index, self._name, self._type, self._params)

    def __add__(self, value: Any) -> DimensionVariant:
        raise RuntimeError("Bad operation '__add__' on RelativeVariant")

    def __sub__(self, value: Any) -> DimensionVariant:
        raise RuntimeError("Bad operation '__sub__' on RelativeVariant")

    def range(self, datum: DimensionVariant) -> List[DimensionVariant]:
        """Expand/explode the logical relative range into concrete/finalized values by replacing
        RELATIVE_DIMENSION_VALUE ("_") with the value received on runtime.

        Parameters
        ----------
        datum: variant value that will become the main reference (starting point)

        Returns
        -------
        List[DimensionVariant]: exploded list of variants with concrete/finalized values.

        Examples

        >>> RelativeVariant(-5).range(LongVariant(10))
        [LongVariant(value=10,...),...,LongVariant(value=5)]
        """
        range_result: List[DimensionVariant] = []
        range_dim: DimensionVariant = datum
        for relative_index in range(abs(self._relative_index)):
            if self._relative_index >= 0:
                range_dim = datum + relative_index
            else:
                range_dim = datum - relative_index
            range_result.append(range_dim)

        return range_result

    @classmethod
    def resolve(cls, raw_value: Any) -> DimensionVariantResolver.Result:
        if raw_value == cls.DATUM_DIMENSION_VALUE_SPECIAL_CHAR:
            return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.MATCH, creator=cls)

        # noinspection PyBroadException
        try:
            slice = raw_value.split(cls.INDEX_SEPARATOR)
            if len(slice) in [2, 3]:
                datum = slice[0]
                if datum == cls.DATUM_DIMENSION_VALUE_SPECIAL_CHAR:
                    relative_index = slice[1]
                    if abs(int(relative_index)) <= cls.MAX_RELATIVE_INDEX_VALUE:
                        if len(slice) == 3:
                            shift = slice[2]
                            if abs(int(shift)) > cls.MAX_RANGE_SHIFT_VALUE:
                                raise ValueError(f"RelativeVariant (value: {raw_value}) shift ({shift}) exceeds max limit!")
                        return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.MATCH, creator=cls)
                    else:
                        raise ValueError(f"RelativeVariant (value: {raw_value}) relative_index exceeds max limit!")
        except Exception:
            pass

        return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.UNMATCH, creator=None)

    @classmethod
    def create(cls, raw_value: Any, params_dict: ParamsDictType) -> DimensionVariant:
        """Create variant from a string value previously resolved to be a RelativeVariant by
        RelativeVariant::resolve function. In other terms, DimensionVariantFactory is expected
        to call this for a raw_value that was previously resolved to be RelativeVariant.

        So its format can be any of the following:

        "_": NOW where _ is DATUM_DIMENSION_VALUE_SPECIAL_CHAR
        "_:R": Range of [NOW ... NOW -/+ abs(R)] where R is an integer
        "_:R:S": Range of [(NOW - S) ... (NOW - S) -/+ abs(R)] where R and S are integers
        """
        if raw_value == RelativeVariant.DATUM_DIMENSION_VALUE_SPECIAL_CHAR:
            relative_index = -1
            shift = None
        else:
            range_parts = raw_value.split(cls.INDEX_SEPARATOR)
            relative_index = range_parts[1]
            shift = range_parts[2] if len(range_parts) == 3 else None
            if shift:
                params_dict = dict() if params_dict is None else dict(params_dict)
                params_dict.update({DimensionVariant.RANGE_SHIFT_FIELD_ID: int(shift)})
        return (
            RelativeVariant(int(relative_index))
            if not params_dict
            else RelativeVariant(
                int(relative_index), params_dict.get(Dimension.NAME_FIELD_ID, None), Dimension.extract_type(params_dict), params_dict
            )
        )


# register this new variant to the factory
DimensionVariantFactory.register_resolver(RelativeVariant)


class StringVariant(DimensionVariant, DimensionVariantResolver, DimensionVariantCreatorMethod):
    FORMAT_PARAM: ClassVar[str] = "format"
    CASE_INSENSITIVE: ClassVar[str] = "insensitive"

    UPPER_FUNC: ClassVar[DimensionVariantMapFunc] = lambda dim: dim.upper()
    LOWER_FUNC: ClassVar[DimensionVariantMapFunc] = lambda dim: dim.lower()

    def __init__(self, value: Any, name: Optional[NameType] = None, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(value, name, Type.STRING, params)
        self._update_params(params)
        self.value = value

    # overrides
    @DimensionVariant.value.setter
    def value(self, val: Any) -> None:
        self.raw_string = str(val)

    # overrides
    @Dimension.params.setter
    def params(self, val: Dict[str, Any]) -> None:
        self._params = val
        self._update_params(val)
        if self.formatter:
            # load format func
            try:
                self._value = self._formatter(self.raw_string)
            except Exception as ex:
                raise ValueError(
                    f"Formatting error for String dimension {self!r}! Error: {str(ex)}. "
                    f"Please also note that formatter signature should be Callable[[Any], Any]."
                )
            self._validate_format()

    def _update_params(self, params: Dict[str, Any]) -> None:
        if params:
            self._formatter: DimensionVariantMapFunc = params.get(self.FORMAT_PARAM, None)
            case_insensitive_param = params.get(self.CASE_INSENSITIVE, False)
            if not isinstance(case_insensitive_param, bool):
                raise ValueError(
                    f"Case-sensitivity param {case_insensitive_param!r} for StringVariant {self!r} should " f"be of bool type."
                )
            self._is_case_sensitive = not case_insensitive_param
        else:
            self._formatter = None
            self._is_case_sensitive = True

    @property
    def formatter(self):
        return getattr(self, "_formatter", None)

    @property
    def case_sensitive(self):
        return getattr(self, "_is_case_sensitive", True)

    @property
    def raw_string(self) -> str:
        return getattr(self, "_raw_string", None)

    @raw_string.setter
    def raw_string(self, new_str: str) -> None:
        self._raw_string = new_str
        if self.formatter:
            self._value = self._formatter(self._raw_string)
            self._validate_format()
        else:
            self._value = self._raw_string

    def _validate_format(self):
        if self._value is None or not isinstance(self._value, str):
            raise ValueError(
                f"Formatter {self._formatter.__code__.co_code!r} assigned to dimension {self!r} is yielding a bad output! "
                f"It transformed the raw dimension value {self.raw_string!r} to {self._value!r}."
            )

        # also make sure that it won't be resolved to Long, Datetime, etc \
        resolved_variant = DimensionVariantFactory.create_variant(self._value, None)
        if not isinstance(resolved_variant, StringVariant):
            raise ValueError(
                f"Formatter {self._formatter!r} assigned to dimension {self!r} is yielding a different"
                f" type {resolved_variant.type!r}! It transformed the raw dimension value "
                f"{self.raw_string!r} to {self._value!r} which would resolve to {resolved_variant!r} "
                f"at runtime."
            )

    def __hash__(self) -> int:
        return Dimension.__hash__(self)

    def __eq__(self, other: Any) -> bool:
        is_equals: bool = DimensionVariant.__eq__(self, other)
        if not is_equals and not self.case_sensitive:
            if isinstance(other, DimensionVariant):
                return self.check_type(other) and self._value.lower() == other._value.lower()
            elif isinstance(other, str):
                return self._value.lower() == other.lower()
            else:
                return self._value.lower() == other
        return is_equals

    _OVERFLOW_UNDERFLOW_TYPE = bool

    @classmethod
    def _increment_alphanum_char(cls, c: str) -> Tuple[str, _OVERFLOW_UNDERFLOW_TYPE]:
        if c.isnumeric() and c == "9":
            return "0", True

        if c == "Z":
            return "A", True
        if c == "z":
            return "a", True

        return chr(ord(c) + 1), False

    @classmethod
    def _increment_alphanum_string(cls, alphanum_str: str) -> str:
        low_char: str = alphanum_str[-1]
        next_char, overflown = cls._increment_alphanum_char(low_char)
        high_part: str = alphanum_str[:-1]

        if overflown:
            if high_part:  # increment the remainder
                high_part = cls._increment_alphanum_string(high_part)
            else:
                # overflow prepending a new char to the input string
                # new char will depend on the current high-ranking char (low_char
                # ex: 9Z -> increment -> 10A, or Z9 -> increment -> AA0
                if low_char.isnumeric():
                    high_part = "1"
                else:
                    high_part = "A" if low_char.isupper() else "a"

        return high_part + next_char

    @classmethod
    def _decrement_alphanum_char(cls, c: str) -> Tuple[str, _OVERFLOW_UNDERFLOW_TYPE]:
        if c.isnumeric() and c == "0":
            return "9", True

        if c == "A":
            return "Z", True
        if c == "a":
            return "z", True

        return chr(ord(c) - 1), False

    @classmethod
    def _decrement_alphanum_string(cls, alphanum_str: str) -> str:
        low_char: str = alphanum_str[-1]
        prev_char, underflown = cls._decrement_alphanum_char(low_char)
        high_part: str = alphanum_str[:-1]

        if underflown:
            if high_part:  # decrement the remainder
                high_part = cls._decrement_alphanum_string(high_part)
            else:
                raise OverflowError("Decrement operation caused underflow on alphanumeric string!")

        return high_part + prev_char

    def __add__(self, val: Any) -> DimensionVariant:
        """For outmost user convenience when defining range operations on dimensions
        (particularly for different type of entities other than datasets), support string add/sub.

        And also, in cases where type is forced to be string, this will enable high-level APIs to offer
        range declarations. See :class:`RelativeVariant.explode` for implicit runtime utilization of this method.

        Support more flexible type of partition naming and auto increment them.

        if the input is of type int, then string value should be alpha numeric. And also, if the input is string,
        then the addition is basic string concatenation.

        Parameters

            value : <int> or <str>

        Returns

            `DimensionVariant` : A new StringVariant

        Raises

            `ValueError` : if `value` param is of type int but string value is not alpha-numeric or `value` is
                           neither `string` nor `int`.

        Examples:

        >>> (StringVariant("NA") + "01").value
        'NA01'
        >>> (StringVariant("NA01") + 1).value
        'NA02'
        >>> (StringVariant("99") + 1).value
        '100'
        >>> (StringVariant("AA") + 2).value
        'AC'
        >>> (StringVariant("trainingjob99") + 1).value
        'trainingjoc00'
        """
        if isinstance(val, str):
            new_variant: StringVariant = copy.deepcopy(self)
            new_variant.raw_string = self.raw_string + val
            return new_variant

        if not isinstance(val, int):
            raise ValueError(f"Bad input type (value={val}) for '__add__' on StringVariant(value={self!r}")

        if not cast(str, self.raw_string).isalnum():
            raise ValueError(f"Bad operation '__add__'(value={val}) on non-alphanumeric StringVariant(value={self!r}")

        new_variant: DimensionVariant = copy.deepcopy(self)

        for _ in range(val):
            new_variant.raw_string = self._increment_alphanum_string(new_variant.raw_string)

        return new_variant

    def __sub__(self, val: Any) -> DimensionVariant:
        """Decrement action on a StringVariant.

        More restrictive than __add__ operation. Only supports decrement action with an integer input type.

        If the input is of type int, then string value should be alpha numeric.

        Keeps the length of the underlying value string of StringVariant (ex: '100' - 1 = '099')

        See :meth:`__add__` for other details.

        Parameters

            `val` : <int>

        Returns

            `DimensionVariant` : A new StringVariant

        Raises

            `ValueError` : if `val` param is of type int but string value is not alpha-numeric or `val` is not `int`.
            `OverflowError` : if decrement operation causes underflow on the alpha-numeric string (ex: '0', 'aaa' or 'Aa').

        Examples:

        >>> (StringVariant("NA02") - 1).value
        'NA01'
        >>> (StringVariant("100") - 1).value
        '099'
        >>> (StringVariant("Ab") - 1).value
        'Aa'
        >>> (StringVariant("000") - 1).value
        ...
        OverflowError: ...
        """

        if not isinstance(val, int):
            raise ValueError(f"Bad input type (value={val}) for '__sub__' on StringVariant(value={self!r}")

        if not cast(str, self._value).isalnum():
            raise ValueError(f"Bad operation '__sub__'(value={val}) on non-alphanumeric StringVariant(value={self!r}")

        new_variant: StringVariant = copy.deepcopy(self)

        for _ in range(val):
            try:
                new_variant.raw_string = self._decrement_alphanum_string(new_variant.raw_string)
            except OverflowError as overflow_error:
                raise type(overflow_error)(
                    f"Underflow happened on alpha-numeric StringVariant(value={self!r})"
                    f" while decrementing it by {val}. Exception: {str(overflow_error)}"
                )

        return new_variant

    @classmethod
    def resolve(cls, raw_value: Any) -> DimensionVariantResolver.Result:
        if isinstance(raw_value, str):
            return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.TENTATIVE, creator=cls)
        return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.UNMATCH, creator=cls)

    @classmethod
    def create(cls, raw_value: Any, params_dict: ParamsDictType) -> DimensionVariant:
        return (
            StringVariant(raw_value)
            if not params_dict
            else StringVariant(raw_value, params_dict.get(Dimension.NAME_FIELD_ID, None), params_dict)
        )


# register this new variant to the factory
DimensionVariantFactory.register_resolver(StringVariant)
DimensionVariantFactory.register_creator(Type.STRING, StringVariant)


class DateVariant(DimensionVariant, DimensionVariantResolver, DimensionVariantCreatorMethod):
    GRANULARITY_PARAM: ClassVar[str] = "granularity"
    TIMEZONE_PARAM: ClassVar[str] = "timezone"
    FORMAT_PARAM: ClassVar[str] = "format"
    MIN_DATETIME: ClassVar[str] = "min"
    RELATIVE_MIN_DATETIME: ClassVar[str] = "relative_min"

    _SUPPORTED_DATE_TIME_SEPARATORS: ClassVar[List[str]] = ["T", " ", "-", "_", "@", "/"]

    _FORMAT_EXCEPTION_LIST: ClassVar[Set[str]] = {"%Y%m%d", "%d%m%Y", "%m%d%Y", "%Y", "%d"}

    def __init__(self, value: Any, name: Optional[NameType] = None, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(value, name, Type.DATETIME, params)
        # self._value = value
        self._update_params(params)
        self.value = value

    # overrides
    @property
    def raw_value(self) -> Any:
        return self.date

    # overrides
    @property
    def value(self) -> Any:
        return self._value

    # overrides
    @DimensionVariant.value.setter
    def value(self, val: Any) -> None:
        if isinstance(val, datetime):
            self.date = val
        elif isinstance(val, date):
            self.date = datetime(val.year, val.month, val.day)
        else:
            # see https://dateutil.readthedocs.io/en/stable/parser.html
            #
            # We have to allow fuzzy parsing since the 'val' must have been 'resolve'd into DateVariant
            # and might not be perfect.  DateVariant::resolve also uses fuzzy parsing.
            # Allowing this (being not strictly-typed) at this level is not a problem since type checking
            # is a high-level (DimensionFilter/Spec level) concern and also resolve operation enforces the type
            # (if specified by the user). Even when format is provided, we favor dateutil.parser over strptime
            # (as of 12/2021) based on our own # observation that when it succeeds it yields more reliable results than
            # strptime, if it fails then we use strptime with 'format'.
            # Example: '2020-03-03-01' might have been fuzzily resolved into DateVariant, then RheocerOS
            # attempts to instantiate the variant, which ends up in this method. So if we do strictly-typed
            # parsing, this would raise a ValueError which would be inconsistent with high-level logic.
            try:
                if not isinstance(val, str):  # e.g 23, 2021 as int
                    val = str(val)
                # the underlying date information: we always want to keep it as close to the raw value as possible,
                # extra value (like hours ,etc) might be used for linking, etc. That is why we don't directly use
                # format here (and just use it as a fallback).
                self.date = dateutil.parser.parse(val, fuzzy_with_tokens=True)[0]
                # resolve the discrepancies between dateutil and expected value from format
                # e.g '23' -> resolved as day but format might be %H, %M, %m, %S
                if len(val) <= 2 and self._format != "%d":
                    resolved_val = self.date.day
                    if self._format == "%H":
                        self.date = datetime(self.date.year, self.date.month, self.date.day, resolved_val)
                    elif self._format == "%m":
                        self.date = datetime(self.date.year, resolved_val, 1)
                    elif self._format in ["%M", "%S"]:
                        # dateutil: when x > 32 then it is read as year (e.g 32 -> 2032) we cannot use it.
                        self.date = datetime.strptime(val, self._format)
            except ValueError:
                # ex: val = "2021012223" in format "%Y%m%d%H" which causes dateutil to fail but can still be loaded
                # using the format as a hint.
                if self._format:
                    self.date = datetime.strptime(val, self._format)
                else:
                    raise ValueError(
                        f"DateVariant cannot be created from {val!r} without a format. Please specify"
                        f" {self.FORMAT_PARAM} for dimension {self.name!r}"
                    )

    # overrides
    @Dimension.params.setter
    def params(self, val: Dict[str, Any]) -> None:
        self._params = val
        self._update_params(val)
        self._validate_naive()
        if self._format:
            self._value = self._date.strftime(self._format)
            self._validate_format()

    def _update_params(self, params: Dict[str, Any]) -> None:
        if params:
            self._datetime_granularity = (
                DatetimeGranularity[params[self.GRANULARITY_PARAM]] if self.GRANULARITY_PARAM in params else DEFAULT_DATETIME_GRANULARITY
            )
            if self.TIMEZONE_PARAM in params:
                timezone_param = params[self.TIMEZONE_PARAM]
                # TODO use dateutil.tz module (tzoffset, tzlocal)
                self._timezone: Union[Timezone, datetime.tzinfo] = (
                    Timezone[timezone_param] if isinstance(timezone_param, str) else timezone_param
                )
            else:
                self._timezone = None
            # ex: %Y-%m-%d
            self._format = params[self.FORMAT_PARAM] if self.FORMAT_PARAM in params else None
            if self._format is None and params.get(self.CAST_PARAMS_FIELD_ID, None):
                self._format = (
                    params[self.CAST_PARAMS_FIELD_ID][self.FORMAT_PARAM] if self.FORMAT_PARAM in params[self.CAST_PARAMS_FIELD_ID] else None
                )

            # TODO check format and granularity dispcepancies!
            # e.g if self._format == "%H" and self._datetime_granularity != DatetimeGranularity.HOUR

            self._update_min(params)
            self._update_relative_min(params)
        else:
            self._datetime_granularity = DEFAULT_DATETIME_GRANULARITY
            self._timezone = None
            self._format = None
            self._min = None
            self._relative_min = None

    def _update_min(self, params: Dict[str, Any]):
        self._min = None
        if self.MIN_DATETIME in params:
            min_datetime = params[self.MIN_DATETIME]
            if isinstance(min_datetime, date):
                self._min = datetime(min_datetime.year, min_datetime.month, min_datetime.day)
            elif isinstance(min_datetime, datetime):
                self._min = min_datetime
            else:
                # use our own mechanism to infer from user provided value. create a DateVariant
                # to get the datetime value
                min_params = copy.deepcopy(params)
                # eliminate any recursive min/relative_min action on min value
                del min_params[self.MIN_DATETIME]
                if self.RELATIVE_MIN_DATETIME in min_params:
                    del min_params[self.RELATIVE_MIN_DATETIME]
                if self.CAST_PARAMS_FIELD_ID in min_params:
                    cast_params = min_params[self.CAST_PARAMS_FIELD_ID]
                    if self.MIN_DATETIME in cast_params:
                        del cast_params[self.MIN_DATETIME]
                    if self.RELATIVE_MIN_DATETIME in min_params[self.CAST_PARAMS_FIELD_ID]:
                        del cast_params[self.RELATIVE_MIN_DATETIME]
                self._min = DimensionVariantFactory.create_variant(min_datetime, min_params).date
        elif params.get(self.CAST_PARAMS_FIELD_ID, None):
            cast_params = params[self.CAST_PARAMS_FIELD_ID]
            min_datetime = cast_params.get(self.MIN_DATETIME, None)
            if min_datetime:
                min_params = dict(params)
                del min_params[self.CAST_PARAMS_FIELD_ID]
                min_params.update(cast_params)
                del min_params[self.MIN_DATETIME]
                if self.RELATIVE_MIN_DATETIME in min_params:
                    del min_params[self.RELATIVE_MIN_DATETIME]
                self._min = DimensionVariantFactory.create_variant(min_datetime, min_params).date

    def _update_relative_min(self, params: Dict[str, Any]):
        if self.RELATIVE_MIN_DATETIME in params:
            self._relative_min = params[self.RELATIVE_MIN_DATETIME]
        elif params.get(self.CAST_PARAMS_FIELD_ID, None):
            self._relative_min = params[self.CAST_PARAMS_FIELD_ID].get(self.RELATIVE_MIN_DATETIME, None)
        else:
            self._relative_min = None

        if self._relative_min is not None and not isinstance(self._relative_min, timedelta):
            self._relative_min = self._delta(self._relative_min)

    @property
    def date(self) -> datetime:
        return self._date

    @date.setter
    def date(self, new_dt: datetime) -> None:
        self._date = new_dt
        self._validate_naive()
        if self._format:
            self._value = self._date.strftime(self._format)
            self._validate_format()
        else:
            self._value = str(self._date)

    def _validate_format(self):
        pass
        # if self._format in self._FORMAT_EXCEPTION_LIST:
        #    return
        # if self.resolve(self._value).score != DimensionVariantResolverScore.MATCH:
        #    raise ValueError(f"Format {self._format!r} is a bad datetime format! It should be parsed back to "
        #                     f"a valid datetime object with no ambiguity.")

    def _validate_naive(self):
        # TODO/FUTURE if timezone identifiers are allowed, then use this to check against timezone parameter
        #  of the dimension. But we are strictly avoiding timezone information in string representations since
        #  it is currently a very ambigous topic (see support in ISO 8601).
        #  And we anticipate our users would make the mistake of creating their own customer formats that would
        #  be parsed back to timezone offets when reading from string (ex: "2020-03-05 01-03") here the intent is
        #  to declare the minutes but dateutil will interpret this as tzoffet. We avoid this situation for the sake
        #  of devtime / runtime data consistency.
        # tzlocal is OK! we just use the absolute datetime value as naive in that case.
        if self._date.tzinfo and isinstance(self._date.tzinfo, tzoffset):
            raise ValueError(
                f"DateVariant datetime object {self.name!r} cannot contain timezone info!"
                " Please use the 'timezone' attribute of the dimension."
            )

        if self._min is not None and self._date < self._min:
            raise ValueError(
                f"DateVariant {self.name!r} cannot be created from {self._date!r} which is earlier than 'min' datetime {self._min!r}!"
            )
        if self._relative_min is not None:
            shifted_now = datetime.utcnow() - self._relative_min
            if self._date < shifted_now:
                raise ValueError(
                    f"DateVariant {self.name!r} cannot be created from {self._date!r} which is earlier than 'relative_min' datetime {shifted_now!r} (calculated as [UTC_NOW - {self._relative_min}])!"
                )

    @property
    def timezone(self) -> datetime.tzinfo:
        """Currently timezone information is reserved for compatibility check purposes (Signal-Signal)

        We are not using it for the instantiation of the datetime object.
        TODO we have to check and apply the timezone to the datetime object (make it 'aware').
        #  this = self.date.replace(tzinfo=self.timezone)
        #  or (pytz::timezone::localize)
        #  'raw_value' and 'value' of this variant can be utilized in other places (linking, etc) where naive datetime
        #  objects might violate the intention of declaring timezone.
        #  This problem is very important and a blocker for higher granularities (HOUR, etc).
        #  RheocerOS should handle this everywhere automatically
        #  (yyyy:mm:19 20:00:00 PST) == (yyyy:mm:20 04:00:00 UTC)
        """
        if isinstance(self._timezone, datetime.tzinfo):
            return self._timezone
        else:
            return self._timezone.tzinfo

    def __hash__(self) -> int:
        return Dimension.__hash__(self)

    def __eq__(self, other: Any) -> bool:
        try:
            if isinstance(other, DimensionVariant):
                return self._type == other._type and (
                    self._value == other._value
                    or (
                        isinstance(other, DateVariant)
                        and (self._date == other.date or (self._format and self._value == other.date.strftime(self._format)))
                    )
                )
            elif isinstance(other, Dimension):
                return self._type == other._type
            else:
                return (
                    self._value == other
                    or (isinstance(other, date) and self.date == datetime(other.year, other.month, other.day))
                    or (isinstance(other, datetime) and self.date == other)
                    or (isinstance(other, datetime) and self._format and self._date.strftime(self._format) == other.strftime(self._format))
                    or (isinstance(other, str) and self.date == dateutil.parser.parse(other, fuzzy_with_tokens=True)[0])
                )
        except Exception:
            return False

    def _delta(self, value: Any) -> Union[timedelta, relativedelta]:
        delta: timedelta
        if self._datetime_granularity == DatetimeGranularity.MINUTE:
            delta = timedelta(minutes=value)
        elif self._datetime_granularity == DatetimeGranularity.HOUR:
            delta = timedelta(hours=value)
        elif self._datetime_granularity == DatetimeGranularity.DAY:
            delta = timedelta(days=value)
        elif self._datetime_granularity == DatetimeGranularity.WEEK:
            delta = timedelta(weeks=value)
        elif self._datetime_granularity == DatetimeGranularity.MONTH:
            delta = relativedelta(months=value)
        elif self._datetime_granularity == DatetimeGranularity.YEAR:
            delta = relativedelta(years=value)
        else:
            raise ValueError(f"Datetime granularity {self._datetime_granularity!r} defined on {self.name!r} is not supported!")
        return delta

    def __add__(self, value: Any) -> "DimensionVariant":
        """Encapsulate the logic for incremental change / shifts on this variant.

        Deep-copy and return a shifted (date modified) DateVariant.

        See RelativeVariant for a generic use of this facility.

        Parameters
        ----------
        value

        Returns
        -------
        DimensionVariant (copy semantics)

        """
        delta: timedelta = self._delta(value)

        new_variant: DimensionVariant = copy.deepcopy(self)
        new_variant.date = new_variant.date + delta
        return new_variant

    def __sub__(self, value: Any) -> "DimensionVariant":
        """see :meth:`__add__`"""
        return self + (-value)

    @classmethod
    def resolve(cls, raw_value: Any) -> DimensionVariantResolver.Result:
        if isinstance(raw_value, (datetime, date)):
            return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.MATCH, creator=cls)

        if isinstance(raw_value, str) and len(raw_value) >= 6:  # length check to narrow down the scope to formats such as "DDMMYY"
            try:
                fuzzy_datetime, ignored_tokens = dateutil.parser.parse(raw_value, fuzzy_with_tokens=True)
                if fuzzy_datetime:
                    if ignored_tokens:
                        if not (
                            len(ignored_tokens) == 1 and all([token in cls._SUPPORTED_DATE_TIME_SEPARATORS for token in ignored_tokens[0]])
                        ):
                            # a very interesting case of a string containing datetime.
                            # module_logger.info(f"Tentatively resolving dimension value '{raw_value}' as DatetimeVariant,"\
                            #                   f"since it contains ({fuzzy_datetime}) along with ignored tokens ({ignored_tokens})")
                            return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.TENTATIVE, creator=cls)

                    # DONOT allow external datetime string to have timezone!
                    #  see the comment in DateVariant::_validate_naive
                    if fuzzy_datetime.tzinfo and isinstance(fuzzy_datetime.tzinfo, tzoffset):
                        # ex: "2020-03-03 01-38" -> datetime(2020, 3, 3, 1, 0, tzinfo=(tzoffset(None, -136800))
                        #      intent here is to provide minutes however dateutil will evaluate as timezone.
                        # And more importantly, we should enforce RheocerOS' restriction to avoid datetime embedded
                        # timezone information. It should be managed by this variant (via 'timezone' attribute).
                        module_logger.info(
                            f"Problematic dimension value {raw_value!r}. It has datetime string"
                            f" but could not be resolved to DateVariant successfully because some "
                            f" part of it got interpreted as timezone. This means that a datetime"
                            f" component is lost. Lossy datetime object: {fuzzy_datetime}."
                            f" Resolving to UNMATCH for DateVariant."
                        )
                        return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.UNMATCH, creator=cls)

                    if not raw_value.isnumeric():
                        # ex: "2020-05-22", "2020-03-03____0138" (2020, 3, 3, 1, 38), "2020-03-03-01" (2020, 3, 3, 1, 0)
                        return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.MATCH, creator=cls)
                    else:
                        # let the high-level resolver decide (along with user provided 'cast' spec, if any)
                        return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.TENTATIVE, creator=cls)
            except Exception:
                pass

        return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.UNMATCH, creator=cls)

    @classmethod
    def create(cls, raw_value: Any, params_dict: ParamsDictType) -> DimensionVariant:
        if not params_dict:
            return DateVariant(raw_value)
        else:
            return DateVariant(raw_value, params_dict.get(Dimension.NAME_FIELD_ID, None), params_dict)


# register this new variant to the factory
DimensionVariantFactory.register_resolver(DateVariant)
DimensionVariantFactory.register_creator(Type.DATETIME, DateVariant)


class LongVariant(DimensionVariant, DimensionVariantResolver, DimensionVariantCreatorMethod):
    DIGITS_PARAM: ClassVar[str] = "digits"

    def __init__(self, value: Any, name: Optional[NameType] = None, params: Optional[Dict[str, Any]] = None) -> None:
        try:
            assert isinstance(value, int) or int(value) or int(value) == 0
        except Exception as error:
            raise type(error)(
                f"Please provide a numeric value for dimension (name={name!r}, params={params!r}), not {value!r}! Error: {error!r}"
            )
        super().__init__(None, name, Type.LONG, params)
        self._update_params(params)
        self._value = str(int(value)).zfill(self._digits)

    # overrides
    @Dimension.params.setter
    def params(self, val: Dict[str, Any]) -> None:
        self._params = val
        self._update_params(val)
        self._value = self._value.zfill(self._digits)

    def _update_params(self, params: Dict[str, Any]) -> None:
        self._digits = None
        if params:
            self._digits = params[self.DIGITS_PARAM] if self.DIGITS_PARAM in params else None
            if self._digits is None and params.get(self.CAST_PARAMS_FIELD_ID, None):
                self._digits = (
                    params[self.CAST_PARAMS_FIELD_ID][self.DIGITS_PARAM] if self.DIGITS_PARAM in params[self.CAST_PARAMS_FIELD_ID] else None
                )

        try:
            # str::zfill used for digits and 0 is noop for it
            self._digits = int(self._digits) if self._digits else 0
        except:
            raise ValueError(f"Cannot cast {self.DIGITS_PARAM!r} parameter {self._digits} provided for dimension {self!r} to an integer!")

    # overrides
    @property
    def value(self) -> Any:
        # return int when digits not making any difference, for user convenience and also for backwards compatibility.
        return int(self._value) if len(str(int(self._value))) >= self.digits else self._value

    @value.setter
    def value(self, val: Any) -> None:
        self._value = str(val).zfill(self._digits)

    @property
    def digits(self) -> int:
        # FUTURE remove, added for backwards compatibility
        return getattr(self, "_digits", 0)

    # overrides
    def check_value(self, value: Any) -> bool:
        return super().check_value(value) or self.value == value or int(self.value) == value or int(self.value) == int(value)

    def __hash__(self) -> int:
        return Dimension.__hash__(self)

    def __eq__(self, other: Any) -> bool:
        if DimensionVariant.__eq__(self, other):
            return True

        try:
            # exceptional check for 'digits' support
            if isinstance(other, DimensionVariant) and self.check_type(other):
                return int(self._value) == int(other._value)
        except:
            pass

        return False

    def _check_operand(self, val: Any) -> Any:
        if isinstance(val, str):
            if val.isnumeric():
                return int(val)
            else:
                raise ValueError(f"Bad input value {val} for operation on {self!r}")

        return val

    def __add__(self, val: Any) -> DimensionVariant:
        """Create a new LongVariant and add the numeric input (`val`) to its value.

        For a string input, LongVariant converts it to `int` implicitly as long as it is numeric.

        Parameters

            value : should be numeric or should support "+" (protocol) with an `int`

        Returns

            `DimensionVariant` : A new LongVariant

        Raises

            `ValueError` : if `val` param is not numeric
            `TypeError` : if type(`val`) is not ok for int::__add__

        Examples:

        >>> (LongVariant(100) + 2).value
        102
        >>> (LongVariant(10) + "1" ).value
        11
        """
        new_variant: DimensionVariant = copy.deepcopy(self)
        # might raise TypeError if type(val) is not supported
        new_variant.value = int(self.value) + self._check_operand(val)
        return new_variant

    def __sub__(self, val: Any) -> DimensionVariant:
        """See :meth:`__add__`"""
        new_variant: DimensionVariant = copy.deepcopy(self)
        # might raise TypeError if type(val) is not supported
        new_variant.value = int(self.value) - self._check_operand(val)
        return new_variant

    @classmethod
    def resolve(cls, raw_value: Any) -> DimensionVariantResolver.Result:
        try:
            if isinstance(raw_value, int) or int(raw_value) == 0:
                return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.MATCH, creator=cls)
            elif int(raw_value):
                if DateVariant.resolve(raw_value).score in [DimensionVariantResolverScore.MATCH, DimensionVariantResolverScore.TENTATIVE]:
                    return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.TENTATIVE, creator=cls)
                else:
                    return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.MATCH, creator=cls)
        except (ValueError, TypeError):
            pass

        return DimensionVariantResolver.Result(score=DimensionVariantResolverScore.UNMATCH, creator=cls)

    @classmethod
    def create(cls, raw_value: Any, params_dict: ParamsDictType) -> DimensionVariant:
        return (
            LongVariant(raw_value)
            if not params_dict
            else LongVariant(raw_value, params_dict.get(Dimension.NAME_FIELD_ID, None), params_dict)
        )


# register this new variant to the factory
DimensionVariantFactory.register_resolver(LongVariant)
DimensionVariantFactory.register_creator(Type.LONG, LongVariant)


RawDimensionFilterInput = Union[List[RawVariantType], Dict[RawVariantType, Any]]


class DimensionFilter(DimensionSpec):
    """Data structure to hold nested dimension filters as a specialization of DimensionSpec with
    actual/concrete values (as :class:`DimensionVariant`).

    Two major structural differences that can occur during this specialization:
     - a dimension can have one or more values (variants) at the same level,
     - "name" and "type" field is not mandated. we rely on type inference during instantiation of filters.

    In some cases filters are created without spec information (spec). And also in dev-time,
    it is likely that filter and spec information can be instantiated separately and then
    merged (see :meth:`set_spec`). So DimensionFilters are supposed to create the representation
    of their type (aka the Spec) based on the following rules:

    - Order of dimensions
    - Group dimensions by 'name' and 'type'. Dont lose the ordering among the groups as well.
    - If name and type are both None, treat them as placeholders for a new spec-dim (type dim).

    This logic will be applied universally in all of the scenarios where we'll have a filter vs filter
    operation (ex: input signal:filter vs a signal:filter in routing table) and also when compatibility
    checks are necessary in ambiguous scenarios.

    Examples

    For a spec like:
    {
        "region": {...}
    }

    A filter (specialization) might look like;

    {
        "NA: {...}
        "EU": {...}
    }

    >>> DimensionFilter([DimensionVariant("NA"), DimensionVariant("EU")], [None, None])
    """

    def __init__(self, dim_variants: List[DimensionVariant] = None, sub_dim_filters: List["DimensionFilter"] = None) -> None:
        super().__init__(dim_variants, sub_dim_filters)

    def add_dimension(self, dimension: Dimension, sub_dimensions: "DimensionSpec", eliminate_identicals=False) -> "DimensionSpec":
        if eliminate_identicals:
            for dim, sub_spec in self.get_dimensions():
                if dim.is_identical(dimension) and sub_spec == sub_dimensions:
                    return self
        return super().add_dimension(dimension, sub_dimensions, False)

    # override
    def pretty(self) -> Dict[Any, Any]:
        dimensions: Dict[Any, Any] = OrderedDict()
        for dim, sub_filter in self.get_dimensions():
            value_key = str(dim.value)
            raw_value = str(dim.raw_value)
            dimensions[value_key] = {}
            if value_key != raw_value:
                dimensions[value_key]["raw_value"] = raw_value
            dimensions[value_key][Dimension.NAME_FIELD_ID] = dim.name
            dimensions[value_key][Dimension.TYPE_FIELD_ID] = dim.type
            if dim.params:
                dimensions[value_key].update(dim.params)
            if sub_filter:
                dimensions[value_key].update(sub_filter.pretty())
        return dimensions

    def feed(self, dim_readers: Sequence[DimensionVariantReader]) -> None:
        self._feed_recursive(self, dim_readers)

    @classmethod
    def _feed_recursive(cls, sub_filter: "DimensionFilter", dim_readers: Sequence[DimensionVariantReader]) -> None:
        if sub_filter:
            for dim, sub_spec in sub_filter.get_dimensions():
                # map(lambda reader: reader.read(dim), dim_readers)
                for reader in dim_readers:
                    reader.read(dim)
                cls._feed_recursive(sub_spec, dim_readers)

    def map(self, dim_mappers: Sequence[DimensionVariantMapper]) -> "DimensionFilter":
        # TODO
        pass

    def merge(self, other_filter: "DimensionFilter") -> "DimensionFilter":
        """Use UNION logic to merge this filter with another"""
        pass

    def apply(self, value_filter: "DimensionFilter") -> Optional["DimensionFilter"]:
        return self.chain(value_filter, True)

    @overload
    def chain(self, other_filter: "DimensionFilter", finalize: bool = False) -> Optional["DimensionFilter"]: ...

    @overload
    def chain(self, other_filter: RawDimensionFilterInput, finalize: bool = False) -> Optional["DimensionFilter"]: ...

    def chain(
        self, other_filter_or_raw: Union["DimensionFilter", RawDimensionFilterInput], finalize: bool = False
    ) -> Optional["DimensionFilter"]:
        """Further filter this with another filter, narrowing down the value(dimension) range,
         hence the scope of specialization.

         Multiple filters can be chained using this method. So this method is a core facility for
         high-level filter-chaining, hence Signal binding in dev-time, and also in runtime
         (against incoming events).

         This operation also causes special variants (Any, Relative, etc) to use corresponding dimensions
         from the other filter as datums to expand themselves.

         This filter and the input filter should have the same type (spec). Otherwise, the operation
         will yield None.

         Example 1:

             Filter1:
             { "NA": {}
               "EU": {}
               "IN": {}
             }

             Filter2:
             {
               "NA": {}
             }

             Filter1.chain(Filter2) yields

             result:
             {
               "NA": {}
             }

         Example 2:

            Filter1 (RelativeVariant):
            {
                "_:3": {}
            }

            Filter2 (LongVariant):
            {
                "5": {}
            }

            Filter1.chain(Filter2, finalize=True)  [runtime behaviour]

            result:
            {
                "5": {}
                "4": {}
                "3": {}
            }


        Parameters
        ----------
        other_filter:DimensionFilter
        finalize: if this is true, then variants (such as RelativeVariant) are asked to finalize
                  their value based on the corresponding value.

        Returns
        -------
        Optional[DimensionFilter]: a new filter (copy semantics)
        """

        other_filter = (
            other_filter_or_raw if isinstance(other_filter_or_raw, DimensionFilter) else DimensionFilter.load_raw(other_filter_or_raw)
        )

        if not self.check_spec_match(other_filter):
            module_logger.critical(f"Cannot chain the filter {other_filter} with {self} due to spec mismatch.")
            return None

        return self._chain_recursive(self, other_filter, finalize) if other_filter else copy.deepcopy(self)

    @classmethod
    def _chain_recursive(
        cls, left_filter: "DimensionFilter", right_filter: "DimensionFilter", finalize: bool
    ) -> Optional["DimensionFilter"]:
        if not left_filter or not right_filter:
            return None

        new_filter: DimensionFilter = DimensionFilter()

        # represents the head of a dimension sequence (of same type&name) at this level
        current_level_head_dim: Dimension = None

        # Ordering of dims should be same at this level.
        left_root_dims: Iterable[Tuple[DimensionVariant, DimensionFilter]] = left_filter.get_dimensions()
        left_filter_dim_block: List[Tuple[DimensionVariant, DimensionFilter]] = None
        for right_dim, right_sub_spec in right_filter.get_dimensions():
            # if name and type are both None, treat it as unique (ex: Special Variants, etc)
            if (
                (not right_dim.name and not right_dim.type)
                or current_level_head_dim is None
                or (current_level_head_dim.type != right_dim.type or current_level_head_dim.name != right_dim.name)
            ):
                # whenever this condition occurs in the filter, we assume
                # that we moved into another dim in the spec.
                current_level_head_dim = Dimension(right_dim.name, right_dim.type)

                # populate all of the dims from the left side.
                left_filter_dim_block = []  # [(d,s) for d,s in next(left_root_dims) if d == right_dim]

                for d, s in left_root_dims:
                    # check type and name compatibility
                    if d.check_type(right_dim) and (not d.name or not right_dim.name or d.name == right_dim.name):
                        # if right_dim == d or (d == right_dim and (d.name or d.type)):
                        left_filter_dim_block.append((d, s))

            # apply a right-dimvariant to left-dimvariants which are of the same type.
            # if right-dimvariant can survive, it will probably yield one or more dimvariants
            # (depending on whether finalize=True and also the types of the variants. For example:
            # RelativeVariant (as a left-dim) can produce a range of variants if finalize=true, for
            # a new value from the right-dimvariant.
            for left_dim, left_sub_spec in left_filter_dim_block:
                result: List[DimensionVariant] = left_dim.apply(right_dim, finalize)
                if result:  # right_dim does not get eliminated (overlaps with left-filter)
                    if left_sub_spec and right_sub_spec:
                        new_spec: DimensionSpec = cls._chain_recursive(left_sub_spec, right_sub_spec, finalize)
                        if new_spec:  # check if sub_specs yield a match as well, otherwise skip the whole branch.
                            for r in result:
                                new_filter.add_dimension(r, copy.deepcopy(new_spec), eliminate_identicals=True)
                    else:  # leaf dimension
                        for r in result:
                            new_filter.add_dimension(r, None, eliminate_identicals=True)

        return new_filter

    def check_spec_match(self, type_spec: Union[DimensionSpec, "DimensionFilter"]) -> bool:
        """Check whether this filter has a spec equivalent to input spec.

        Parameters
        ----------
        type_spec

        Returns
        -------
        bool
        """
        spec: DimensionSpec = self.get_spec()
        target_spec: DimensionSpec = type_spec.get_spec() if isinstance(type_spec, DimensionFilter) else type_spec

        def _filter_spec_equality_comparator(src_dim: Dimension, tgt_dim: Dimension) -> bool:
            # handle special DimensionVariants where type information is not known,
            # they are placeholders for anything if type is not specified. So, it is
            # safe to accept equality here if "type"s match.
            if not src_dim.type or not tgt_dim.type:
                return True
            # we cannot use "Dimension::name" when comparing two different specs
            return src_dim.type == tgt_dim.type

        # noinspection PyBroadException
        try:
            # enable "breadth check", don't use Type check since it is being handled by special comparator here.
            return self._check_spec_structure_recursive(spec, target_spec, True, False, _filter_spec_equality_comparator)
        except Exception as ex:
            pass

        return False

    def is_equivalent(self, other_filter: "DimensionFilter") -> bool:
        """See 'DimensionFilter::check_equivalence"""
        return DimensionFilter.check_equivalence(self, other_filter)

    @classmethod
    def check_equivalence(cls, left_filter: "DimensionFilter", right_filter: "DimensionFilter") -> bool:
        """Checks the hierarchy and also values on each DimensionVariant.

        Ignores attached metadata ('params') to each variant

        :param right_filter:
        :return: True if filters are equivalent in terms of structure and values at each node.
        """
        if not left_filter and not right_filter:
            return True

        if not left_filter or not right_filter:
            return False

        left_dimensions = left_filter.get_dimensions()
        right_dimensions = right_filter.get_dimensions()

        if len(left_dimensions) != len(right_dimensions):
            return False
        try:
            left_it = iter(left_dimensions)
            right_it = iter(right_dimensions)
            while True:
                l_d, l_spec = next(left_it)
                r_d, r_spec = next(right_it)
                if l_d != r_d or not cls.check_equivalence(l_spec, r_spec):
                    return False
        except StopIteration:
            pass

        return True

    def set_spec(self, type_spec: DimensionSpec) -> None:
        """Type specification for this filter.

        Particularly important in development when we have access to basic type spec for a filter. The reason
        specs and filters are described separately in dev-time is mostly due to user-convenience reasons. For
        elimination of redundant type specific details mentioned in multiple places. The separation itself
        has many other advantages that make filter/spec management for multiple entities look more like
        type-systems in high-level languages.

        Generally this operation on a filter is optional. But it can be used to support implicit "type
        inference" that happens during the instantiation of a filter. For special variant types such
        as :class:`AnyVariant` and :class:`RelativeVariant`, this operation can guarantee dev-time and runtime
        checks.

        Examples

        filter:
        { "NA" : { *: {'timezone': 'PST'}},
          "EU" : { *: {'timezone': 'GMT'}}
        }

        spec:
        { "region": {
                "type": STRING,
                "day": {
                    "type": DATETIME
                }
        }

        result:
        { "NA" : {"type": STRING, *: {"type": DATETIME, 'timezone': 'PST'}},
          "EU" : {"type": STRING, *: {'timezone': 'GMT'}}
        }

        Parameters
        ----------
        type_spec

        Returns
        -------
        None

        Raises
        ------
        TypeError : if the current filter and input type_spec are not compatible
        """
        try:
            self._set_spec_recursive(self, type_spec)
        except TypeError as te:
            raise te
        except Exception as ex:
            raise TypeError(f"Filter is not compatible with the spec! Exception: {str(ex)}")

    @classmethod
    def _set_spec_recursive(cls, target_filter: "DimensionFilter", source_type_spec: DimensionSpec) -> None:
        if not target_filter and not source_type_spec:
            return

        # represents the head of a dimension sequence (of same type&name) at this level
        current_level_head_dim: Dimension = None

        # Ordering of dims should be same at this level.
        spec_root_dims: Iterator[Tuple[Dimension, DimensionSpec]] = iter(source_type_spec.get_dimensions())
        for dim, sub_spec in target_filter.get_dimensions():
            # if name and type are both None, treat it as unique (ex: Special Variants, etc)
            if (
                (not dim.name and not dim.type)
                or current_level_head_dim is None
                or (current_level_head_dim.type != dim.type or current_level_head_dim.name != dim.name)
            ):
                # whenever this condition occurs in the filter, we assume that we moved into another
                # dim (sequence/block) in the spec.
                spec_root_dim, spec_root_sub_spec = next(spec_root_dims)
                current_level_head_dim = Dimension(dim.name, dim.type)

            if spec_root_dim.type:
                if dim.type and dim.type != spec_root_dim.type:
                    err_msg = (
                        f"Incompatible dimensions (target: {dim!r}, source: {spec_root_dim!r} detected "
                        f"while trying to apply source spec {source_type_spec!r} to target_filter {target_filter!r}"
                    )
                    module_logger.error(err_msg)
                    raise TypeError(err_msg)

                dim.type = spec_root_dim.type

            dim.name = spec_root_dim.name
            if dim.params:
                if spec_root_dim.params:
                    dim.params.update(spec_root_dim.params)
                    dim.params = dim.params
            else:
                dim.params = spec_root_dim.params
            cls._set_spec_recursive(sub_spec, spec_root_sub_spec)

    def get_spec(self) -> DimensionSpec:
        """Extract the type (specific layout/structure) information for this filter.

        - Order sensitive
        - Differentiates dimensions (at the same order) by Dimension::type and Dimension::name
        - Special variants map to unique type dimensions (since type and name might be None on them)

        Returns
        -------
        DimensionSpec : a spec that would represent the type of its filter
        """
        if not self:
            return DimensionSpec()
        return self._get_spec_recursive(self)

    @classmethod
    def _get_spec_recursive(cls, current_level: "DimensionFilter") -> DimensionSpec:
        if not current_level:
            return None

        spec: DimensionSpec = DimensionSpec()

        # represents the head of the current dimension sequence (of same type)
        current_level_head_dim: Dimension = None
        for item, value in current_level.get_dimensions():
            # if name and type are both None, treat it as unique (ex: Special Variants, etc)
            if (
                (not item.name and not item.type)
                or current_level_head_dim is None
                or (item.name != current_level_head_dim.name or item.type != current_level_head_dim.type)
            ):
                # beginning of a new dimension block (multiple variant seq)
                sub_dim_specs: DimensionSpec = cls._get_spec_recursive(cast(DimensionFilter, value))

                dim: Dimension = Dimension(item.name, item.type, item.params)
                spec.add_dimension(dim, sub_dim_specs)
                current_level_head_dim = dim

        return spec

    @classmethod
    def all_pass(cls, for_spec: Optional[DimensionSpec] = None) -> Optional["DimensionFilter"]:
        if for_spec is None:
            return None

        filter = DimensionFilter()

        for dim, sub_spec in for_spec.get_dimensions():
            filter.add_dimension(AnyVariant(dim.name, dim.type, dim.params), cls.all_pass(sub_spec))

        return filter

    @classmethod
    @overload
    def load_raw(
        cls, raw_value_list: List[RawVariantType], cast: DimensionSpec = None, error_out: bool = False
    ) -> Optional["DimensionFilter"]: ...

    @classmethod
    @overload
    def load_raw(
        cls, raw_value_dict: Dict[RawVariantType, Any], cast: DimensionSpec = None, error_out: bool = False
    ) -> Optional["DimensionFilter"]: ...

    @classmethod
    def load_raw(
        cls, raw_values: RawDimensionFilterInput, cast: DimensionSpec = None, error_out: bool = False
    ) -> Optional["DimensionFilter"]:
        """Load a filter most probably from external resources where even the underlying type information (spec)
        is not known.

        A filter initiated like this will heavily rely on "type inference / resolve" operations provided by
        Variant impls.

        Mostly used in runtime (for example upon a new event, we might need to rely on load operations like this).

        Parameters
        ----------
        raw_values : Provide a raw list or dictionary to create a filter with DimensionVariants with value and inferred types.

        cast: DimensionSpec to be used for casting when there is type ambiguity.

        Returns
        -------
        DimensionFilter : a new filter
        """
        if not raw_values:
            return DimensionFilter()

        try:
            if isinstance(raw_values, List):
                return cls._load_raw_list(raw_values, cast)
            else:
                return cls._load_raw_dict(raw_values, cast)
        except (ValueError, TypeError) as bad_cast:
            if error_out:
                raise bad_cast
            return None

    @classmethod
    def _load_raw_list(cls, raw_list: List[RawVariantType], cast: DimensionSpec = None) -> "DimensionFilter":
        if not raw_list:
            return None

        new_filter = DimensionFilter()

        raw_value = raw_list[0]
        new_dim_var: DimensionVariant = None
        cast_dim, sub_cast = next(iter(cast.get_dimensions())) if cast else (None, None)

        if isinstance(raw_value, Tuple):
            params_dict = dict(raw_value[1])
            if cast_dim:
                params_dict.update({Dimension.TYPE_FIELD_ID: cast_dim.type, DimensionVariant.CAST_PARAMS_FIELD_ID: cast_dim.params})
            new_dim_var = DimensionVariantFactory.create_variant(raw_value[0], params_dict)
            if cast_dim:
                del params_dict[DimensionVariant.CAST_PARAMS_FIELD_ID]
        else:
            params_dict = (
                {Dimension.TYPE_FIELD_ID: cast_dim.type, DimensionVariant.CAST_PARAMS_FIELD_ID: cast_dim.params} if cast_dim else None
            )
            new_dim_var = DimensionVariantFactory.create_variant(raw_value, params_dict)
            if cast_dim:
                del params_dict[DimensionVariant.CAST_PARAMS_FIELD_ID]

        sub_filters = cls._load_raw_list(raw_list[1:], sub_cast)
        new_filter.add_dimension(new_dim_var, sub_filters)

        return new_filter

    @classmethod
    def _load_raw_dict(cls, raw_dict: Dict[RawVariantType, Any], cast: DimensionSpec = None) -> "DimensionFilter":
        if not (raw_dict and isinstance(raw_dict, dict)):
            return None

        new_filter = DimensionFilter()
        cast_it = iter(cast.get_dimensions()) if cast else None

        if cast_it:
            # check cast ambiguity
            #   - cast dimension spec type count is more than 1 and not equal to raw variant count.
            raw_variant_count = len(raw_dict.keys())
            cast_type_count = len(cast.get_root_dimensions())
            if cast and cast_type_count > 1 and raw_variant_count > cast_type_count:
                raise TypeError(
                    f"Cannot cast dict-type filter to spec!"
                    f"\nReason: cast_type_count > 1 and cast_type_count({cast_type_count}) < raw_filter_variant_count({raw_variant_count})."
                    f"\nFilter: {raw_dict!r}, Cast Spec: {cast!r}"
                )

        for raw_key, raw_value in raw_dict.items():
            new_dim_var: DimensionVariant = None
            raw_variant = None
            params_dict = None
            if isinstance(raw_key, Tuple):
                # variant params are provided along with the param
                # example: ("NA", {"name": "region", "type": STRING, ...})
                raw_variant = raw_key[0]
                params_dict = dict(raw_value[1])
            elif isinstance(raw_value, dict):
                # variant params are embedded within the nested map along with child filters
                # example: {"NA": {"name":...}
                # TODO remove all 'dict' type sub-fields.
                #      currently, we keep the whole sub-tree as part of the param.
                raw_variant = raw_key
                params_dict = dict(raw_value)

            if raw_variant is not None:
                try:
                    cast_dim, sub_cast = next(cast_it) if cast_it else (None, None)
                except StopIteration:
                    pass  # keep using the previous type (representing this level)
                if cast_dim:
                    params_dict.update({Dimension.TYPE_FIELD_ID: cast_dim.type, DimensionVariant.CAST_PARAMS_FIELD_ID: cast_dim.params})
                    new_dim_var = DimensionVariantFactory.create_variant(raw_variant, params_dict)
                    del params_dict[DimensionVariant.CAST_PARAMS_FIELD_ID]
                else:
                    new_dim_var = DimensionVariantFactory.create_variant(raw_variant, params_dict)

            if new_dim_var:
                sub_filters = cls._load_raw_dict(raw_value, sub_cast)
                new_filter.add_dimension(new_dim_var, sub_filters)

        return new_filter if new_filter else None

    @classmethod
    def materialize(
        cls,
        dim_spec: DimensionSpec,
        dim_mappers: Sequence[DimensionVariantMapper],
        coalesce_relatives: bool = False,
        allow_partial: bool = False,
    ) -> "DimensionFilter":
        """Create a new filter using mappers that binds external filters (variants) with different specs to `dim_spec`.

        So this method uses values from mappers for each of the dimension of `dim_spec` to instantiate a
        new filter.
        """
        if not dim_spec:
            return None

        new_filter = DimensionFilter()

        for dim, sub_spec in dim_spec.get_dimensions():
            new_dim_var: DimensionVariant = None
            # eliminate duplicates. not so critical, as our overall scheme is not effected by
            # the presence of duplicates. but it is reasonable for memory, cosmetic reasons and
            # also against any future change in our core modules that would make dimension or signaling
            # scheme to be effected by duplicates.
            dim_all_mapped_values: Set[Any] = set()
            for mapper in dim_mappers:
                mapped_values: List[Any] = mapper.map(dim)
                for value in mapped_values:
                    if value in dim_all_mapped_values:
                        # enforce UNION logic
                        # skip the same value for the same dim at the same level
                        continue

                    params_dict: ParamsDictType = {Dimension.NAME_FIELD_ID: dim.name, Dimension.TYPE_FIELD_ID: dim.type}
                    if dim.params:
                        params_dict.update(dim.params)

                    new_dim_var = DimensionVariantFactory.create_variant(value, params_dict)
                    if coalesce_relatives and isinstance(new_dim_var, RelativeVariant):
                        if AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR in dim_all_mapped_values:
                            continue
                        new_dim_var = DimensionVariantFactory.create_variant(AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR, params_dict)
                        dim_all_mapped_values.add(AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR)

                    if new_dim_var:
                        sub_filters = cls.materialize(sub_spec, dim_mappers, coalesce_relatives, allow_partial)
                        new_filter.add_dimension(new_dim_var, sub_filters)
                        dim_all_mapped_values.add(value)
                    else:
                        raise RuntimeError(
                            f"Cannot materialize dimension ({dim!r}) for value({value!r}) due to"
                            f"factory error! dim_spec: {dim_spec!r}, dim_mappers: {dim_mappers!r}"
                        )

            if not new_dim_var:
                if not allow_partial:
                    raise ValueError(
                        f"Cannot materialize dimension ({dim!r}) using mappers ({dim_mappers!r})!"
                        f" dim_spec: {dim_spec!r}, dim_all_mapped_values {dim_all_mapped_values!r}"
                    )
                # use AnyVariant as the place-holder for the unmapped dimension.
                params_dict: ParamsDictType = {Dimension.NAME_FIELD_ID: dim.name, Dimension.TYPE_FIELD_ID: dim.type}
                if dim.params:
                    params_dict.update(dim.params)
                new_dim_var = DimensionVariantFactory.create_variant(AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR, params_dict)
                sub_filters = cls.materialize(sub_spec, dim_mappers, coalesce_relatives, allow_partial)
                new_filter.add_dimension(new_dim_var, sub_filters)

        return new_filter

    def is_material(self) -> bool:
        if any(cast("DimensionVariant", dim).is_special_value() for dim in self.get_flattened_dimension_map().values()):
            return False
        return True

    def transform(self) -> "DimensionFilter":
        new_filter: DimensionFilter = DimensionFilter()

        for dim, sub_spec in self.get_dimensions():
            tip_dim = copy.deepcopy(dim.transform())
            tip_sub_spec = cast(DimensionFilter, sub_spec).transform() if sub_spec else None
            new_filter.add_dimension(tip_dim, tip_sub_spec)

        return new_filter

    def tip(self) -> "DimensionFilter":
        new_filter: DimensionFilter = DimensionFilter()

        for dim, sub_spec in self.get_dimensions():
            tip_dim = copy.deepcopy(dim)
            tip_sub_spec = cast(DimensionFilter, sub_spec).tip() if sub_spec else None
            new_filter.add_dimension(tip_dim, tip_sub_spec)
            break

        return new_filter

    def get_dimension_variants(self, dimension_name: str) -> List[DimensionVariant]:
        if not self.find_dimension_by_name(dimension_name):
            raise ValueError(f"Dimension {dimension_name!r} cannot be found!")
        return self._get_dimension_variants(self, dimension_name)

    @classmethod
    def _get_dimension_variants(cls, filter: "DimensionFilter", dimension_name: str) -> List[DimensionVariant]:
        variants: List[DimensionVariant] = []
        if filter:
            for dim, sub_filter in filter.get_dimensions():
                if dim.name == dimension_name:
                    variants.append(dim)
                variants.extend(cls._get_dimension_variants(sub_filter, dimension_name))
        return variants


if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=False, optionflags=doctest.ELLIPSIS)
