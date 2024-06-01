# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from enum import Enum, unique
from typing import Any, ClassVar, Dict, List, Optional, Sequence, Set, Tuple, Union, cast, overload

from intelliflow.core.entity import CoreData
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import (
    AlarmComparisonOperator,
    AlarmDimension,
    AlarmParams,
    AlarmRule,
    AlarmRuleOperator,
    AlarmState,
    AlarmTreatMissingData,
    CompositeAlarmParams,
    MetricDimension,
    MetricDimensionValues,
    MetricExpression,
    MetricPeriod,
    MetricStatistic,
    MetricStatType,
    MetricSubDimensionMapType,
)

from .definitions.dimension_defs import Type
from .dimension_constructs import (
    AnyVariant,
    DateVariant,
    Dimension,
    DimensionFilter,
    DimensionSpec,
    DimensionVariant,
    RawDimensionFilterInput,
)

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse


logger = logging.getLogger(__name__)


@unique
class SignalSourceType(str, Enum):
    """High-level grouping of Signals (<SignalType>s)

    Aims to keep external modules from being tightly coupled with <SignalType>s.
    """

    INTERNAL = "INTERNAL"  # depending on Platform:Storage impl
    TIMER = "TIMER"
    S3 = "S3"
    GLUE_TABLE = "GLUE_TABLE"
    _AMZN_RESERVED_1 = "EDX"
    LOCAL_FS = "LOCAL_FS"
    SAGEMAKER = "SAGEMAKER"
    SNS = "SNS"

    INTERNAL_METRIC = "INTERNAL_METRIC"  # depending on Platform::MetricStore impl
    CW_METRIC = "CW_METRIC"

    INTERNAL_ALARM = "INTERNAL_ALARM"  # depending on Platform:MetricStore impl
    CW_ALARM = "CW_ALARM"
    INTERNAL_COMPOSITE_ALARM = "INTERNAL_COMPOSITE_ALARM"
    CW_COMPOSITE_ALARM = "CW_COMPOSITE_ALARM"
    # FUTURE based on the feasibility/research for "agent state update hooks from AWS"
    # CARNAVAL_ALARM = "CARNAVAL_ALARM"

    # Represents RheocerOS system as a signal source (for example; ground signals use this type)
    INTERNAL_SYSTEM = "INTERNAL_SYSTEM"

    def is_internal_signal(self) -> bool:
        return self in [
            SignalSourceType.TIMER,
            SignalSourceType.INTERNAL_METRIC,
            SignalSourceType.INTERNAL_ALARM,
            SignalSourceType.INTERNAL_COMPOSITE_ALARM,
        ]

    def is_external(self) -> bool:
        return self in [
            SignalSourceType.S3,
            SignalSourceType.GLUE_TABLE,
            SignalSourceType.LOCAL_FS,
            SignalSourceType.SAGEMAKER,
            SignalSourceType.CW_METRIC,
            SignalSourceType.CW_ALARM,
            SignalSourceType.CW_COMPOSITE_ALARM,
        ]


DIMENSION_PLACEHOLDER_FORMAT = "{}"

ENCRYPTION_KEY_KEY = "encryption_key"

COMPUTE_HINTS_KEY = "COMPUTE_HINTS"

DATA_TYPE_KEY = "data_type"


@unique
class DataType(str, Enum):
    MODEL_ARTIFACT = "model"
    DATASET = "dataset"
    RAW_CONTENT = "content"


CONTENT_TYPE_KEY = "content_type"


@unique
class ContentType(str, Enum):
    """Refer
    https://docs.aws.amazon.com/sagemaker/latest/dg/cdf-training.html
    """

    X_IMAGE = "application/x-image"
    X_RECORDIO = "application/x-recordio"
    X_RECORDIO_PROTOBUF = "application/x-recordio-protobuf"
    JSONLINES = "application/jsonlines"
    PARQUET = "application/x-parquet"
    JPEG = "image/jpeg"
    PNG = "image/png"
    CSV = "text/csv"
    LIBSVM = "text/libsvm"


MODEL_FORMAT_KEY = "model_format"


@unique
class ModelSignalSourceFormat(str, Enum):
    SAGEMAKER_TRAINING_JOB = "sagemaker_training_job"


MODEL_METADATA = "model_metadata"


class SignalSource(CoreData):
    def __init__(
        self,
        type: SignalSourceType,
        dimension_values: List[Any],
        # an empty name means that its source access spec does not require an extra resource name
        # as part of its path format (for which any event can be mapped to a valid Signal).
        # For that type of objects, SignalIntegrityProtocol based check is skipped (see Signal::create).
        # Example: AWS Glue Table Events which can be unambigously mapped to partition changed signals.
        name: Optional[str] = None,
    ) -> None:
        self.type = type
        self.dimension_values = dimension_values
        self.name = name


class SignalSourceAccessSpec:
    OWNER_CONTEXT_UUID: ClassVar[str] = "owner_context_uuid"
    IS_MAPPED_FROM_UPSTREAM: ClassVar[str] = "is_mapped_from_upstream"

    IS_PROJECTION: ClassVar[str] = "__is_projection"

    def __init__(self, source: SignalSourceType, path_format: str, attrs: Dict[str, Any]) -> None:
        self._source = source
        self._path_format = path_format
        self._attrs = attrs
        self._proxy: SignalSourceAccessSpec = None

    @property
    def source(self) -> SignalSourceType:
        return self._source

    @property
    def path_format(self) -> str:
        return self._path_format

    @property
    def attrs(self) -> Dict[str, Any]:
        return self._attrs

    @property
    def data_type(self) -> Optional[DataType]:
        return DataType(self.attrs[DATA_TYPE_KEY]) if DATA_TYPE_KEY in self.attrs else None

    @property
    def encryption_key(self) -> Optional[str]:
        return self._attrs.get(ENCRYPTION_KEY_KEY, None)

    @property
    def content_type(self) -> Optional[ContentType]:
        c_type = self._attrs.get(CONTENT_TYPE_KEY, None)
        return ContentType(c_type) if c_type else None

    @property
    def proxy(self) -> "SignalSourceAccessSpec":
        return getattr(self, "_proxy", None)

    @classmethod
    def path_delimiter(cls) -> str:
        # override in subclasses if necessary
        return "/"

    def get_owner_context_uuid(self) -> str:
        if SignalSourceAccessSpec.OWNER_CONTEXT_UUID in self._attrs:
            return self._attrs[SignalSourceAccessSpec.OWNER_CONTEXT_UUID]
        return None

    def is_mapped_from_upstream(self) -> Optional[bool]:
        return self._attrs.get(SignalSourceAccessSpec.IS_MAPPED_FROM_UPSTREAM, None)

    def set_as_mapped_from_upstream(self) -> None:
        self._attrs[SignalSourceAccessSpec.IS_MAPPED_FROM_UPSTREAM] = True

    @property
    def is_projection(self) -> bool:
        return self._attrs.get(self.IS_PROJECTION, False)

    def __hash__(self) -> int:
        return hash((self._source, self._path_format))

    def __eq__(self, other) -> bool:
        return self._source == other.source and self._path_format == other.path_format and self._attrs == other.attrs

    def __ne__(self, other):
        return not self == other

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(source={self._source!r}, path_format={self._path_format}, attrs={self._attrs})"

    def check_integrity(self, other: "SignalSourceAccessSpec") -> bool:
        """Check the runtime compatibility of another spec with this one.

        This method can be used to understand whether a diff in spec (due to re-activation/deployment)
         would cause a change in runtime behaviour, particularly in the context of routing/orchestration.

         Sub-classes are expected to do checks on relevant keys within the '_attrs' metadata dict.
        """
        return (
            self._source == other.source
            and self._path_format == other.path_format
            and ((not self.proxy and not other.proxy) or (self.proxy and other.proxy and self.proxy.check_integrity(other.proxy)))
            and self.encryption_key == other.encryption_key
        )

    def link(self, proxy: "SignalSourceAccessSpec") -> None:
        """Link another source access spec to this one.

        They will collectively determine the runtime mapping of incoming events to a valid signal, in other terms
        represent the same logical resource (aka Signal source).
        """
        self._proxy = proxy

    @classmethod
    def _allow_redundant_dimension_values(cls) -> bool:
        return False

    @classmethod
    def path_format_requires_resource_name(cls) -> bool:
        return True

    def extract_source(self, materialized_resource_path: str, required_resource_name: Optional[str] = None) -> Optional[SignalSource]:
        """Extract dimensions (i.e partition keys) from a full/physical resource path using this spec and also
        the entire chain of proxy specs attached to it.
        """
        source = self._extract_source(materialized_resource_path, required_resource_name)
        if not source and getattr(self, "_proxy", None):
            source = self._proxy.extract_source(materialized_resource_path, required_resource_name)
        return source

    def _extract_source(self, materialized_resource_path: str, required_resource_name: Optional[str] = None) -> Optional[SignalSource]:
        """Extract dimensions (i.e partition keys) from a full/physical resource path.

        Can be specialized in sub-classes if this impl cannot satisfy a particular source type.
        TODO add 'root_path' as an attribute to this class and make the impl simpler, more reliable here.
        subclasses already know/extract the root path during creation and then its lost.
        """
        path_format_partitioned = self.path_format.split(self.path_delimiter())
        resource_path_partitioned = materialized_resource_path.split(self.path_delimiter())
        if self.path_format_requires_resource_name():
            required_resource_name_parts_len = len(required_resource_name.split(self.path_delimiter())) if required_resource_name else 1
            # drop the actual resource name (i.e partition name for datasets) from the resource path
            resource_name = self.path_delimiter().join(resource_path_partitioned[-required_resource_name_parts_len:])
            resource_path_partitioned = resource_path_partitioned[:-required_resource_name_parts_len]
        else:
            resource_name = None
            resource_path_partitioned = resource_path_partitioned

        if not self._allow_redundant_dimension_values():
            if len(path_format_partitioned) != len(resource_path_partitioned):
                return None
        elif len(resource_path_partitioned) < len(path_format_partitioned):
            # no matter what the case is (how tolerant spec is), we need all dimension values.
            return None

        dimension_list: List[Any] = []
        resource_iterator = iter(resource_path_partitioned)
        for path_part in path_format_partitioned:
            if DIMENSION_PLACEHOLDER_FORMAT not in path_part:
                # parts should match
                # TODO regex here (currently the whole compartment should be '*')
                if path_part != "*" and path_part != next(resource_iterator):
                    return None
            else:
                start = path_part.find("{")
                end_shift = len(path_part) - path_part.rfind("}") - 1
                next_dim_value = next(resource_iterator)
                if end_shift:
                    dimension_list.append(next_dim_value[start:-end_shift])
                else:
                    dimension_list.append(next_dim_value[start:])

        return SignalSource(self._source, dimension_list, resource_name)

    def materialize_for_filter(self, dim_filter: DimensionFilter) -> List["SignalSourceAccessSpec"]:
        """Returns empty array if 'dim_filter' is not compatible with the path_format of the spec"""
        materialized_paths: List[str] = self.create_paths_from_filter(dim_filter)
        materialized_specs: List["SignalSourceAccessSpec"] = []

        for path in materialized_paths:
            materialized_specs.append(SignalSourceAccessSpec(self.source, path, self.attrs))

        return materialized_specs

    def create_paths_from_filter(self, dim_filter: DimensionFilter) -> List[str]:
        """Returns empty array if 'dim_filter' is not compatible with the path_format of the spec"""
        paths: List[str] = []
        current_path_values: List[str] = []
        try:
            self._create_path_from_filter(self.path_format, paths, current_path_values, dim_filter)
        except IndexError as error:
            # return empty array if dim_filter is not compatible with the expected path_format
            pass

        return paths

    # TODO make this abstract and move into impls. Impl below suits Datasets more, for example.
    @classmethod
    def _create_path_from_filter(
        cls, path_format: str, paths: List[str], current_path_values: List[str], dim_filter: DimensionFilter
    ) -> None:
        if not dim_filter:
            new_path = path_format.format(*current_path_values)
            if new_path not in paths:
                paths.append(new_path)
            else:
                logger.critical(f"Detected duplicate materialized path: {new_path!r}!")
            return

        # this logic suits datasets, for other resource type dimensions from the same level can contribute together,
        # with different schemes to materialize a path.
        for dim, sub_filter in dim_filter.get_dimensions():
            path_values: List[str] = list(current_path_values) + [str(cast(DimensionVariant, dim).transform().value)]
            cls._create_path_from_filter(path_format, paths, path_values, sub_filter)

    @classmethod
    def get_dimensions(cls, dim_filter: DimensionFilter) -> List[List[Any]]:
        """Interpret the dimension values using the specific context of access spec impls, so that they can
        support different dimension filter structures and map the final dimension values."""
        if not dim_filter:
            return []
        dimensions: List[List[Any]] = []
        current_dim_values: List[Any] = []
        cls._create_dim_from_filter(dimensions, current_dim_values, dim_filter)
        return dimensions

    @classmethod
    def _create_dim_from_filter(cls, dimensions: List[List[Any]], current_dim_values: List[Any], dim_filter: DimensionFilter) -> None:
        if not dim_filter:
            if current_dim_values not in dimensions:
                dimensions.append(current_dim_values)
            else:
                logger.critical(
                    f"{cls.__name__} detected duplicate dimensions list!" f"{current_dim_values!r} already exists in {dimensions!r}!"
                )
            return

        for dim, sub_filter in dim_filter.get_dimensions():
            dimension_values: List[Any] = list(current_dim_values) + [cls._get_dimension_value(cast(DimensionVariant, dim))]
            cls._create_dim_from_filter(dimensions, dimension_values, sub_filter)

    @classmethod
    def _get_dimension_value(cls, dim_variant: DimensionVariant) -> Any:
        return dim_variant.transform().value

    def check_termination(self, current: DimensionFilter, new: DimensionFilter) -> bool:
        """
        Returns True when this spec detects the end/termination of the host Signal based on its new dimension values.

        This analysis is important for Signal types where Signal domain does not have binary nature and new states
        of it might nullify previous states. These states are determined by the domain_spec (or in this case, particularly
        by the dimension values). So a resource aware (comparative) analysis of dimension values is required to
        understand a Signal has been terminated (for example should be discarding in routing).
        """
        return False

    def auto_complete_filter(
        self, filtering_levels: RawDimensionFilterInput, cast: Optional[DimensionSpec] = None
    ) -> Optional[DimensionFilter]:
        return None

    def __and__(self, operands: Tuple["Signal", Union["Signal", Any]]) -> Any:
        """Overwrite bitwise & for high-level operations operations between Signals."""
        raise NotImplementedError(f"Operator & not implemented for {self.source!r} resources!")

    def __or__(self, operands: Tuple["Signal", Union["Signal", Any]]) -> Any:
        """Overwrite bitwise | for high-level operations operations between Signals."""
        raise NotImplementedError(f"Operator | not implemented for {self.source!r} resources!")


class SystemSignalSourceAccessSpec(SignalSourceAccessSpec):
    """Represents the system as a signal source based on a connection between the system and another RheocerOS
    node/entity (ex: data/model node which would use an input signal that would adapt this spec internally).

    Most common use-case is to bind this to a 'ground signal' for a dangling node that has no inputs.

    This trivial spec impl provides the flexibility/convenience to create a spec from other signal (mostly an output
    signal) basically.
    """

    def __init__(self, tether_id: str, target_entity_dimension_spec: DimensionSpec, *partition_key: str, **kwargs: Dict[str, Any]) -> None:
        """Input params to this spec should be determined by the id and the dimension spec of the target entity.

        For example: when used for a ground signal to tether a dangling data signal/node to the system, this interface
        expects the dimension_spec of the output signal of that node.
        """
        path_format = self.create_path_format_from_spec(tether_id, target_entity_dimension_spec)
        super().__init__(SignalSourceType.INTERNAL_SYSTEM, path_format, kwargs)
        self._tether_id = tether_id

    # overrides
    def check_integrity(self, other: SignalSourceAccessSpec) -> bool:
        if not super().check_integrity(other):
            return False

        if not isinstance(other, SystemSignalSourceAccessSpec):
            return False

        return True

    @property
    def tether_id(self) -> str:
        return self._tether_id

    @classmethod
    def create_path_format_root(cls, tether_id: str) -> str:
        return f"/internal_system_tether/{tether_id}"

    @classmethod
    def create_path_format_from_spec(cls, tether_id: str, spec: DimensionSpec) -> str:
        path_format = cls.create_path_format_root(tether_id)
        if spec:
            path_format = (
                path_format
                + "/"
                + f"{'/'.join(map(lambda x: DIMENSION_PLACEHOLDER_FORMAT, range(len(spec.get_flattened_dimension_map().keys()))))}"
            )
        return path_format


DATASET_DELIMITER_KEY = "delimiter"
DATASET_DELIMITER_DEFAULT_VALUE = "|"
DATASET_ENCODING_KEY = "encoding"
DATASET_ENCODING_DEFAULT_VALUE = "utf-8"
DATASET_HEADER_KEY = "header"  # boolean, identical type & utilization with Spark
DATASET_HEADER_DEFAULT_VALUE = True
DATASET_COMPRESSION_KEY = "compression"
DATASET_COMPRESSION_DEFAULT_VALUE = None

# schema output
DATASET_SCHEMA_FILE_KEY = "schema"
DATASET_SCHEMA_FILE_DEFAULT_VALUE = None
DATASET_SCHEMA_TYPE_KEY = "schema_type"
DATASET_SCHEMA_TYPE_DEFAULT_VALUE = None

# user provided schema data
DATASET_SCHEMA_DEFINITION_KEY = "schema_def"
DATASET_SCHEMA_DEFINITION_DEFAULT_VALUE = None
SchemaField = Tuple[str, Union[str, List], bool]
SchemaDefinition = List[SchemaField]


@unique
class DatasetSchemaType(str, Enum):
    SPARK_SCHEMA_JSON = "spark.json"
    ATHENA_CTAS_SCHEMA_JSON = "athena_ctas.json"


@unique
class DatasetSignalSourceFormat(str, Enum):
    CSV = "csv"
    PARQUET = "parquet"
    ORC = "orc"
    JSON = "json"
    AVRO = "avro"
    LIBSVM = "libsvm"

    @classmethod
    def from_content_type(cls, content_type: ContentType) -> Optional["DatasetSignalSourceFormat"]:
        return {
            ContentType.CSV: DatasetSignalSourceFormat.CSV,
            ContentType.LIBSVM: DatasetSignalSourceFormat.LIBSVM,
            ContentType.PARQUET: DatasetSignalSourceFormat.PARQUET,
            ContentType.JSONLINES: DatasetSignalSourceFormat.JSON,
        }.get(content_type, None)

    def to_content_type(self) -> Optional[ContentType]:
        return {
            DatasetSignalSourceFormat.CSV: ContentType.CSV,
            DatasetSignalSourceFormat.LIBSVM: ContentType.LIBSVM,
            DatasetSignalSourceFormat.PARQUET: ContentType.PARQUET,
            DatasetSignalSourceFormat.JSON: ContentType.JSONLINES,
        }.get(self, None)


DATASET_FORMAT_KEY = "dataset_format"
DATA_FORMAT_KEY = "data_format"
# TODO use parquet as default (similar to what Spark does?)
DEFAULT_DATASET_FORMAT = DatasetSignalSourceFormat.CSV


@unique
class DatasetType(str, Enum):
    REPLACE = "REPLACE"
    APPEND = "APPEND"


DATASET_TYPE_KEY = "dataset_type"
DEFAULT_DATASET_TYPE = DatasetType.REPLACE

PARTITION_KEYS_KEY = "partition_keys"
PRIMARY_KEYS_KEY = "primary_keys"


@unique
class DataFrameFormat(str, Enum):
    SPARK = "SPARK"
    PANDAS = "PANDAS"
    ORIGINAL = "ORIGINAL"


# TODO extract core data spec into a base class (DataSignalSourceAccessSpec)
class DatasetSignalSourceAccessSpec(SignalSourceAccessSpec):
    def __init__(self, source: SignalSourceType, path_format: str, attrs: Dict[str, Any]) -> None:
        super().__init__(source, path_format, attrs)
        data_format_value = self.attrs.get(DATASET_FORMAT_KEY, self.attrs.get(DATA_FORMAT_KEY, None))
        # TODO remove this defaulting after "DataSignalSourceAccessSpec"
        self._data_format = DatasetSignalSourceFormat(data_format_value) if data_format_value else DEFAULT_DATASET_FORMAT
        self._dataset_type = DatasetType(self.attrs[DATASET_TYPE_KEY]) if DATASET_TYPE_KEY in attrs else DEFAULT_DATASET_TYPE
        self._partition_keys = attrs.get(PARTITION_KEYS_KEY, [])
        self._primary_keys = attrs.get(PRIMARY_KEYS_KEY, [])
        schema_def = self.data_schema_definition
        if schema_def is not None:
            self.validate_schema_definition(schema_def)

    # overrides
    def check_integrity(self, other: "SignalSourceAccessSpec") -> bool:
        if not super().check_integrity(other):
            return False

        if (
            not isinstance(other, DatasetSignalSourceAccessSpec)
            or self.data_format != other.data_format
            or self.dataset_type != other.dataset_type
            or self.data_delimiter != other.data_delimiter
            or self.data_compression != other.data_compression
            or self.data_header_exists != other.data_header_exists
            or self.data_schema_file != other.data_schema_file
            or self.data_schema_type != other.data_schema_type
            or self.data_schema_definition != other.data_schema_definition
            or self.partition_keys != other.partition_keys
            or self.primary_keys != other.primary_keys
        ):
            return False

        return True

    # overrides
    @property
    def content_type(self) -> Optional[ContentType]:
        c_type = super().content_type
        if not c_type and self.data_format:
            c_type = self.data_format.to_content_type()
        return c_type

    @property
    def data_format(self) -> DatasetSignalSourceFormat:
        return self._data_format

    @property
    def dataset_type(self) -> DatasetType:
        # TODO remove in the future. added for backwards compatibility
        return getattr(self, "_dataset_type", getattr(self, "_data_type", None))

    @property
    def data_delimiter(self) -> str:
        return self._attrs.get(DATASET_DELIMITER_KEY, DATASET_DELIMITER_DEFAULT_VALUE)

    @property
    def data_encoding(self) -> str:
        return self._attrs.get(DATASET_ENCODING_KEY, DATASET_ENCODING_DEFAULT_VALUE)

    @property
    def data_header_exists(self) -> bool:
        header_exists = self._attrs.get(DATASET_HEADER_KEY, DATASET_HEADER_DEFAULT_VALUE)
        if isinstance(header_exists, str):
            header_exists = True if header_exists.lower() == "true" else False

        return header_exists

    @property
    def data_compression(self) -> Optional[str]:
        return self._attrs.get(DATASET_COMPRESSION_KEY, DATASET_COMPRESSION_DEFAULT_VALUE)

    @property
    def data_schema_file(self) -> str:
        return self._attrs.get(DATASET_SCHEMA_FILE_KEY, DATASET_SCHEMA_FILE_DEFAULT_VALUE)

    @property
    def data_schema_type(self) -> DatasetSchemaType:
        return self._attrs.get(DATASET_SCHEMA_TYPE_KEY, DATASET_SCHEMA_TYPE_DEFAULT_VALUE)

    @property
    def data_schema_definition(self) -> str:
        return self._attrs.get(DATASET_SCHEMA_DEFINITION_KEY, DATASET_SCHEMA_DEFINITION_DEFAULT_VALUE)

    @classmethod
    def validate_schema_definition(cls, schema_def: SchemaDefinition) -> None:
        if not isinstance(schema_def, list):
            raise ValueError(f"Schema definition must be of type <list> not {type(schema_def)}!")

        if not schema_def:
            raise ValueError(
                f"Schema definition cannot be an empty list! Either undefine it or set as 'None' if you don't want to specify it."
            )

        keys = set()
        for schema_field in schema_def:
            if not isinstance(schema_field, tuple):
                raise ValueError(
                    f"A schema field must be a tuple 'Tuple[name: str, type: Union[str, List], nullable: bool]' not {type(schema_field)}! "
                    f"Define the following field as a tuple: {schema_field!r}. "
                    f"Example: ('column_name', 'StringType', True)"
                )

            if len(schema_field) != 3:
                raise ValueError(
                    f"A schema field tuple 'Tuple[str, Union[str, List], nullable: bool]' must be of size 3, not {len(schema_field)}! "
                    f"Fix the following field definition as a tuple of (name, type, nullable): {schema_field!r}"
                )

            name, data_type, nullable = schema_field
            if not name or not isinstance(name, str):
                raise ValueError(f"Schema field 'name' must be a valid string")

            if data_type:
                if isinstance(data_type, list):
                    cls.validate_schema_definition(data_type)
                elif not isinstance(data_type, str):
                    raise ValueError(
                        f"Schema field 'type' is invalid! It must be a valid string or an array of nested schema fields. "
                        f"Fix the data 'type' of following field: {schema_field!r}"
                    )
            else:
                raise ValueError(
                    f"Schema field 'type' must be a valid string or an array of nested schema fields! "
                    f"Fix the data 'type' of following field: {schema_field!r}"
                )

            if not isinstance(nullable, bool):
                raise ValueError(
                    f"Schema field 'nullable' must of type bool! " f"Fix the 'nullable' value of following field: {schema_field!r}"
                )

            keys.add(name)

        if len(keys) != len(schema_def):
            raise ValueError("There are duplicate field names in schema definition!")

    @property
    def partition_keys(self) -> List[str]:
        if self._proxy and isinstance(self._proxy, GlueTableSignalSourceAccessSpec):
            return self._proxy.partition_keys
        return self._partition_keys

    @property
    def primary_keys(self) -> List[str]:
        return self._primary_keys

    @staticmethod
    def get_partition_key_count(dimension_spec: DimensionSpec) -> int:
        """Extract partition key count from generic dimension spec.

        Dataset path format expects one dim at each level.

        Example:
            from
            {
                "region": {
                    type: ...
                    "day: {
                        type: ...
                     }
                }
            }

            get
            2

        >>> DatasetSignalSourceAccessSpec.get_partition_key_count(dim_spec1)
        2
        >>> DatasetSignalSourceAccessSpec.get_partition_key_count(None)
        0
        """

        depth: int = 0
        curr_spec: DimensionSpec = dimension_spec
        while curr_spec:
            depth += 1
            sub_dimensions = curr_spec.get_all_sub_dimensions()
            if sub_dimensions:
                curr_spec = list(sub_dimensions)[0]
            else:
                curr_spec = None
        return depth

    @classmethod
    def get_partitions(cls, dim_filter: DimensionFilter) -> List[List[Any]]:
        return cls.get_dimensions(dim_filter)

    # overrides
    @classmethod
    def _get_dimension_value(cls, dim_variant: DimensionVariant) -> Any:
        return dim_variant.transform().value


INTERNAL_DATA_ROUTE_ID_KEY = "_internal_data_route_id"
INTERNAL_SLOT_TYPES_KEY = "_internal_data_slot_types"


class InternalDatasetSignalSourceAccessSpec(DatasetSignalSourceAccessSpec):
    FOLDER: ClassVar[str] = "internal_data"
    SCHEMA_FILE_DEFAULT_VALUE = "_SCHEMA"
    SCHEMA_FILE_DEFAULT_TYPE = DatasetSchemaType.SPARK_SCHEMA_JSON

    def __init__(
        self, data_id: str, dimension_spec_or_key: Union[str, DimensionSpec], *partition_key: str, **kwargs: Dict[str, Any]
    ) -> None:
        if isinstance(dimension_spec_or_key, str):
            path_format = self.create_path_format(data_id, [dimension_spec_or_key] + list(partition_key))
        else:
            path_format = self.create_path_format_from_spec(data_id, dimension_spec_or_key)
        if DATASET_SCHEMA_TYPE_KEY not in kwargs:
            kwargs[DATASET_SCHEMA_TYPE_KEY] = self.SCHEMA_FILE_DEFAULT_TYPE
        if DATASET_SCHEMA_FILE_KEY not in kwargs or kwargs[DATASET_SCHEMA_FILE_KEY] is not None:
            kwargs[DATASET_SCHEMA_FILE_KEY] = self.build_schema_file_name(kwargs[DATASET_SCHEMA_TYPE_KEY])
        super().__init__(SignalSourceType.INTERNAL, path_format, kwargs)
        self._data_id = data_id

    @classmethod
    def build_schema_file_name(cls, schema_type: DatasetSchemaType = DatasetSchemaType.SPARK_SCHEMA_JSON) -> str:
        return cls.SCHEMA_FILE_DEFAULT_VALUE + "." + schema_type.value

    @classmethod
    def create_from_external(
        cls, other: SignalSourceAccessSpec, converted_spec: SignalSourceAccessSpec
    ) -> Optional["InternalDatasetSignalSourceAccessSpec"]:
        if converted_spec.source == SignalSourceType.INTERNAL:
            data_id_and_partitions = converted_spec.path_format[len(f"/{cls.FOLDER}/") :].split("/")
            data_id = data_id_and_partitions[0]
            partitions = data_id_and_partitions[1:]
            if partitions:
                return InternalDatasetSignalSourceAccessSpec(data_id, *partitions, **other.attrs)
            else:
                return InternalDatasetSignalSourceAccessSpec(data_id, DimensionSpec(), **other.attrs)

    # overrides
    def check_integrity(self, other: SignalSourceAccessSpec) -> bool:
        if not super().check_integrity(other):
            return False

        if not isinstance(other, InternalDatasetSignalSourceAccessSpec):
            return False

        return True

    @property
    def data_id(self) -> str:
        return self._data_id

    @property
    def route_id(self) -> Optional[str]:
        return self._attrs.get(INTERNAL_DATA_ROUTE_ID_KEY, None)

    @route_id.setter
    def route_id(self, val: str) -> None:
        self._attrs[INTERNAL_DATA_ROUTE_ID_KEY] = val

    @property
    def slot_types(self) -> Optional[List["SlotType"]]:
        return self._attrs.get(INTERNAL_SLOT_TYPES_KEY, None)

    @slot_types.setter
    def slot_types(self, val: List["SlotType"]) -> None:
        self._attrs[INTERNAL_SLOT_TYPES_KEY] = val

    @property
    def folder(self) -> str:
        return self.create_path_format_root(self.data_id).strip("/")

    @classmethod
    def create_path_format_root(cls, data_id: str) -> str:
        """Underlying storage is abstracted by Platform::Storage. Providing the relative path for an internal dataset is enough."""
        return f"/{cls.FOLDER}/{data_id}"

    @classmethod
    def create_path_format(cls, data_id: str, partition_keys: List[str]) -> str:
        path_format = cls.create_path_format_root(data_id)
        if partition_keys:
            path_format = path_format + "/" + f"{'/'.join([DIMENSION_PLACEHOLDER_FORMAT for part in partition_keys])}"
        return path_format

    @classmethod
    def create_path_format_from_spec(cls, data_id: str, spec: DimensionSpec) -> str:
        path_format = cls.create_path_format_root(data_id)
        if spec:
            path_format = (
                path_format + "/" + f"{'/'.join(map(lambda x: DIMENSION_PLACEHOLDER_FORMAT, range(cls.get_partition_key_count(spec))))}"
            )
        return path_format


class S3SignalSourceAccessSpec(DatasetSignalSourceAccessSpec):
    @overload
    def __init__(cls, account_id: str, bucket: str, other_spec: SignalSourceAccessSpec) -> None:
        ...

    @overload
    def __init__(cls, account_id: str, bucket: str, folder: str, dimension_spec: DimensionSpec, **kwargs: Dict[str, Any]) -> None:
        ...

    @overload
    def __init__(cls, account_id: str, bucket: str, folder: str, *partition_key: Sequence[str], **kwargs: Dict[str, Any]) -> None:
        ...

    def __init__(
        self,
        account_id: str,
        bucket: str,
        folder_or_other_spec: Union[str, SignalSourceAccessSpec],
        spec_or_partition_key: Optional[Union[str, DimensionSpec]] = None,
        *other_partition_keys: Sequence[str],
        **kwargs: Dict[str, Any],
    ) -> None:
        if not bucket:
            raise ValueError(f"bucket should be provided for {self.__class__.__name__}")

        if folder_or_other_spec is None or isinstance(folder_or_other_spec, str):
            folder: str = folder_or_other_spec
            path_format: str
            if isinstance(spec_or_partition_key, DimensionSpec):
                path_format = self.create_path_format_from_spec(bucket, folder, spec_or_partition_key)
            else:
                partition_keys = [spec_or_partition_key] + list(other_partition_keys) if spec_or_partition_key else []
                path_format = self.create_path_format(bucket, folder, partition_keys)

            super().__init__(SignalSourceType.S3, path_format, kwargs)
            self._folder = folder
        else:
            other_spec: SignalSourceAccessSpec = folder_or_other_spec
            super().__init__(SignalSourceType.S3, self.create_path_format_root(bucket, other_spec.path_format), other_spec.attrs)
            self._folder = other_spec.folder
        self._bucket = bucket
        self._account_id = account_id

    # overrides
    def check_integrity(self, other: "SignalSourceAccessSpec") -> bool:
        if not super().check_integrity(other):
            return False

        # other attributes are already as part of the path_format (checked by super)
        # we just need to check account_id separately.
        if not isinstance(other, S3SignalSourceAccessSpec) or self.account_id != other.account_id:
            return False

        return True

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def folder(self) -> str:
        return self._folder

    @classmethod
    def from_url(cls, account_id: str, url: str) -> "S3SignalSourceAccessSpec":
        s3_url = urlparse(url, allow_fragments=False)
        bucket = s3_url.netloc
        keys = s3_url.path.lstrip("/").split("/")
        return S3SignalSourceAccessSpec(account_id, bucket, keys[0], *keys[1:-1])

    @classmethod
    def create_path_format_root(cls, bucket, folder) -> str:
        if not folder:
            return f"s3://{bucket}"
        return f"s3://{bucket}/{folder.lstrip('/')}"

    @classmethod
    def create_path_format(cls, bucket, folder, partition_keys: List[str]) -> str:
        path_format = cls.create_path_format_root(bucket, folder)
        if partition_keys:
            path_format = path_format + "/" + f"{'/'.join(partition_keys)}"
        return path_format

    @classmethod
    def create_path_format_from_spec(cls, bucket, folder, spec: DimensionSpec) -> str:
        path_format = cls.create_path_format_root(bucket, folder)
        if spec:
            path_format = (
                path_format + "/" + f"{'/'.join(map(lambda x: DIMENSION_PLACEHOLDER_FORMAT, range(cls.get_partition_key_count(spec))))}"
            )
        return path_format

    def get_partition_keys(self) -> List[str]:
        return self.get_partition_keys_from_url(self.path_format)

    @classmethod
    def get_partition_keys_from_url(cls, url: str) -> List[str]:
        s3_url = urlparse(url, allow_fragments=False)
        key = s3_url.path.lstrip("/")
        return key.split("/")


class GlueTableSignalSourceAccessSpec(DatasetSignalSourceAccessSpec):
    # resource name to be used to create a resource path at runtime, representing the resource that caused an
    # incoming event, if this information is missing. this is closely related with signal level integrity check protocol
    # which might rely on this to decide on processing.
    # This is not critical for GlueTable updates since integrity
    # protocol is generally not required on other table types. If it is required, then it is better to specialize that
    # spec. Then, why would we still need this definition? The reason is to comply
    # with RheocerOS' resource path format which requires a resource identifier at the end of a resource path.
    # See SignalSourceAccessSpec::create_source on how this resource path format is used during the extraction process
    # against incoming event resource paths.
    GENERIC_EVENT_RESOURCE_NAME: ClassVar[str] = "table_update"
    # Events that map to valid signals within RheocerOS routing
    # ref
    #    https://docs.aws.amazon.com/glue/latest/dg/automating-awsglue-with-cloudwatch-events.html
    #    see "Glue Data Catalog Table State Change"  "detail-type"
    SIGNAL_GENERATOR_TABLE_EVENTS: ClassVar[List[str]] = ["UpdateTable", "CreatePartition", "BatchCreatePartition"]

    def __init__(
        self, database: str, table_name: str, partition_keys_: List[str], dimension_spec: DimensionSpec, **kwargs: Dict[str, Any]
    ) -> None:
        args = dict(kwargs)
        if not database:
            raise ValueError(f"Database should be provided for {self.__class__.__name__}")

        if not table_name:
            raise ValueError(f"Table name should be provided for {self.__class__.__name__}." f" Database: {database}")

        if partition_keys_ is None and dimension_spec is None:
            raise ValueError(
                f"Partition keys or a dimension spec should be provided for {self.__class__.__name__}"
                f" Database: {database}"
                f" Table name: {table_name}"
            )

        if partition_keys_ and PARTITION_KEYS_KEY not in args:
            args[PARTITION_KEYS_KEY] = partition_keys_

        self._database = database
        self._table_name = table_name

        path_format = self.create_path_format(database, table_name, partition_keys_, dimension_spec)

        super().__init__(SignalSourceType.GLUE_TABLE, path_format, args)

    # overrides
    def check_integrity(self, other: "SignalSourceAccessSpec") -> bool:
        if not super().check_integrity(other):
            return False

        if not isinstance(other, GlueTableSignalSourceAccessSpec):
            return False

        return True

    @classmethod
    def path_format_requires_resource_name(cls) -> bool:
        """GlueTable Signal always yields a valid signal and by-default does not require an extra resource
        name to be declared along with database and the table itself.
        Glue partition change events dont require any high-level resource based analysis to understand event to
        signal mapping.
        A SignalSource object returned by this spec should be used for further processing/trigger withhout any
        need for SignalIntegrityProtocol based check.
        """
        return False

    # overrides
    @classmethod
    def _allow_redundant_dimension_values(cls) -> bool:
        # allow extra dimensions in the path of incoming signals to support implicit partitions.
        # downstream low-level components such as BatchCompute drivers will know how to disregard them,
        # when they are accessing the data.
        return True

    @property
    def database(self) -> str:
        return self._database

    @property
    def table_name(self) -> str:
        return self._table_name

    @classmethod
    def create_materialized_path(cls, database: str, table_name: str, partition_values: List[str]) -> str:
        return cls.create_path_format(database, table_name, partition_values, None).format(*partition_values)

    @classmethod
    def create_path_format_root(cls, database, table_name) -> str:
        return f"glue_table://{database}/{table_name.lstrip('/')}"

    @classmethod
    def create_path_format(cls, provider, table_name, partition_keys: List[str], spec: DimensionSpec) -> str:
        path_format = cls.create_path_format_root(provider, table_name)
        if partition_keys:
            path_format = path_format + "/" + f"{'/'.join(map(lambda x: DIMENSION_PLACEHOLDER_FORMAT, partition_keys))}"
        elif spec:
            path_format = (
                path_format + "/" + f"{'/'.join(map(lambda x: DIMENSION_PLACEHOLDER_FORMAT, range(cls.get_partition_key_count(spec))))}"
            )
        return path_format

    # overrides
    @classmethod
    def _get_partition_value(cls, dim_variant: DimensionVariant) -> Any:
        if isinstance(dim_variant, DateVariant):
            # Enforce the following format, otherwise some Java stacks might complain
            # about it against TIMESTAMP typed columns. It should still be fine even
            # if the partition's catalog type is another date related type.
            return dim_variant.raw_value.strftime("%Y-%m-%d %H:%M:%S")
        return dim_variant.value


class TimerSignalSourceAccessSpec(SignalSourceAccessSpec):
    # needed to conform with IF's resource_path expectation
    INTERNAL_RESOURCE_SUFFIX: ClassVar[str] = "timer"

    def __init__(self, timer_id: str, schedule_expression: str, context_id: str, **kwargs: Dict[str, Any]) -> None:
        self._timer_id = context_id + "-" + timer_id
        path_format = self.create_path_format(self._timer_id)
        super().__init__(SignalSourceType.TIMER, path_format, kwargs)
        self._schedule_expression = schedule_expression
        self._context_id = context_id

    def __eq__(self, other) -> bool:
        return super().__eq__(other) and self.schedule_expression == other.schedule_expression

    def __ne__(self, other):
        return not self == other

    # overrides
    def check_integrity(self, other: "SignalSourceAccessSpec") -> bool:
        if not super().check_integrity(other):
            return False

        # other fields are already as part of the path_format (checked by super above),
        # we just need to check the schedule expression.
        if not isinstance(other, TimerSignalSourceAccessSpec) or self.schedule_expression != other.schedule_expression:
            return False

        return True

    @property
    def timer_id(self) -> str:
        return self._timer_id

    @property
    def schedule_expression(self) -> str:
        return self._schedule_expression

    @property
    def context_id(self) -> str:
        return self._context_id

    @classmethod
    def create_path_format(cls, timer_id: str) -> str:
        return timer_id + cls.path_delimiter() + DIMENSION_PLACEHOLDER_FORMAT

    @classmethod
    def create_resource_path(cls, timer_id: str, time: str) -> str:
        return cls.create_path_format(timer_id).format(time) + cls.path_delimiter() + cls.INTERNAL_RESOURCE_SUFFIX

    @classmethod
    def extract_time(cls, materialized_resource_path: str) -> str:
        parts = materialized_resource_path.rsplit(cls.path_delimiter(), 2)
        if parts[-1] == cls.INTERNAL_RESOURCE_SUFFIX:
            return parts[-2]

        return parts[-1]


class SNSSignalSourceAccessSpec(SignalSourceAccessSpec):
    # needed to conform with IF's resource_path expectation
    TOPIC_RESOURCE_SUFFIX: ClassVar[str] = "topic"

    def __init__(self, topic: str, account_id: str, region: str, retain_ownership: bool, **kwargs: Dict[str, Any]) -> None:
        self._topic = topic
        self._account_id = account_id
        self._region = region
        self._retain_ownership = retain_ownership
        path_format = self.create_path_format(self._topic, account_id, region)
        super().__init__(SignalSourceType.SNS, path_format, kwargs)

    def __eq__(self, other) -> bool:
        return super().__eq__(other) and self.retain_ownership == other.retain_ownership

    def __ne__(self, other):
        return not self == other

    # overrides
    def check_integrity(self, other: "SignalSourceAccessSpec") -> bool:
        if not super().check_integrity(other):
            return False

        # other fields are already as part of the path_format (checked by super above),
        # we just need to check extra fields that are not part of the path_format
        if not isinstance(other, SNSSignalSourceAccessSpec) or self.retain_ownership != other.retain_ownership:
            return False

        return True

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def topic_arn(self) -> str:
        return f"arn:aws:sns:{self.region}:{self.account_id}:{self.topic}"

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def region(self) -> str:
        return self._region

    @property
    def retain_ownership(self) -> bool:
        return self._retain_ownership

    @classmethod
    def create_path_format(cls, topic_id: str, account_id: str, region: str) -> str:
        return (
            "sns://"
            + account_id
            + cls.path_delimiter()
            + region
            + cls.path_delimiter()
            + topic_id
            + cls.path_delimiter()
            + DIMENSION_PLACEHOLDER_FORMAT
        )

    @classmethod
    def create_resource_path(cls, topic_id: str, account_id: str, region: str, time: str) -> str:
        return cls.create_path_format(topic_id, account_id, region).format(time) + cls.path_delimiter() + cls.TOPIC_RESOURCE_SUFFIX

    @classmethod
    def extract_time(cls, materialized_resource_path: str) -> str:
        parts = materialized_resource_path.rsplit(cls.path_delimiter(), 2)
        if parts[-1] == cls.TOPIC_RESOURCE_SUFFIX:
            return parts[-2]

        return parts[-1]


METRIC_VISUALIZATION_TYPE_HINT: str = "__metric_vis_view_hint"
METRIC_VISUALIZATION_STAT_HINT: MetricStatistic = "__metric_vis_stat_hint"
METRIC_VISUALIZATION_PERIOD_HINT: MetricPeriod = "__metric_vis_period_hint"


class MetricSignalSourceAccessSpec(SignalSourceAccessSpec):
    SUB_DIMENSION_NAME_VALUE_SEPARATOR: ClassVar[str] = "@_@"

    def __init__(
        self, source: SignalSourceType, context_id: str, sub_dimensions: MetricSubDimensionMapType, **kwargs: Dict[str, Any]
    ) -> None:
        self._context_id = context_id
        self._sub_dimensions = sub_dimensions
        path_format = self.create_path_format(self._context_id, sub_dimensions)
        super().__init__(source, path_format, kwargs)

    # overrides
    def check_integrity(self, other: "SignalSourceAccessSpec") -> bool:
        if not super().check_integrity(other):
            return False

        # other fields are already as part of the path_format (checked by super above),
        # we just need to check impl specific attributes which are not as part of the path format.
        # currently we don't have anything that does not go into the path format.
        if not isinstance(other, MetricSignalSourceAccessSpec):
            return False

        return True

    @classmethod
    def path_format_requires_resource_name(cls) -> bool:
        return False

    @property
    def sub_dimensions(self) -> MetricSubDimensionMapType:
        return self._sub_dimensions

    @property
    def context_id(self) -> str:
        return self._context_id

    @classmethod
    def extract_context_id(cls, resource_path: str) -> str:
        return resource_path.split(cls.path_delimiter())[0]

    @classmethod
    def create_path_format(cls, context_id: str, sub_dimensions: MetricSubDimensionMapType) -> str:
        if any(
            [
                (cls.SUB_DIMENSION_NAME_VALUE_SEPARATOR in name or cls.SUB_DIMENSION_NAME_VALUE_SEPARATOR in value)
                for name, value in sub_dimensions.items()
            ]
        ):
            raise ValueError(f"Metric sub-dimensions ({sub_dimensions!r}) cannot contain {cls.SUB_DIMENSION_NAME_VALUE_SEPARATOR}!")

        return (
            context_id
            + cls.path_delimiter().join([name + cls.SUB_DIMENSION_NAME_VALUE_SEPARATOR + value for name, value in sub_dimensions.items()])
            + cls.path_delimiter()
            + cls.path_delimiter().join([DIMENSION_PLACEHOLDER_FORMAT for _ in MetricDimension.__members__.keys()])
        )

    @classmethod
    def create_resource_path(
        cls, metric_id: str, sub_dimensions: MetricSubDimensionMapType, metric_dim_values: MetricDimensionValues
    ) -> str:
        return cls.create_path_format(metric_id, sub_dimensions).format(*metric_dim_values)

    def create_stat(self, metric_dim_values: MetricDimensionValues) -> MetricStatType:
        """Sub-classes can override this. Default impl adapts AWS CW Metric Stat structure."""
        return {
            "Metric": {
                "Namespace": self._context_id,
                "MetricName": metric_dim_values.name,
                "Dimensions": [{"Name": name, "Value": value} for name, value in self._sub_dimensions.items()],
            },
            "Period": metric_dim_values.period,
            "Stat": metric_dim_values.statistic,
            "Unit": "None",
        }

    def create_stats_from_filter(self, dim_filter: DimensionFilter) -> List[MetricStatType]:
        """Returns empty array if 'dim_filter' is not compatible with the path_format of the spec"""
        stats: List[MetricStatType] = []
        current_metric_dim_values: List[str] = []
        try:
            self._create_stats_from_filter(stats, current_metric_dim_values, dim_filter)
        except IndexError as error:
            # return empty array if dim_filter is not compatible with the expected path_format
            pass

        return stats

    def _create_stats_from_filter(
        self, stats: List[MetricStatType], current_metric_dim_values: List[str], dim_filter: DimensionFilter
    ) -> None:
        if not dim_filter:
            new_stat = self.create_stat(MetricDimensionValues.from_raw_values(current_metric_dim_values))
            if new_stat not in stats:
                stats.append(new_stat)
            return

        # this logic suits datasets, for other resource type dimensions from the same level can contribute together,
        # with different schemes to materialize a path.
        for dim, sub_filter in dim_filter.get_dimensions():
            metric_dim_values: List[str] = list(current_metric_dim_values) + [str(cast(DimensionVariant, dim).transform().value)]
            self._create_stats_from_filter(stats, metric_dim_values, sub_filter)

    # overrides
    def auto_complete_filter(
        self, filtering_levels: RawDimensionFilterInput, cast: Optional[DimensionSpec] = None
    ) -> Optional[DimensionFilter]:
        if isinstance(filtering_levels, list):
            missing_metric_dimension_count = len(MetricDimension.__members__.keys()) - len(filtering_levels)
            if missing_metric_dimension_count > 0:
                return DimensionFilter.load_raw(
                    filtering_levels + [AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR for i in range(missing_metric_dimension_count)],
                    cast=cast,
                )
        return None


class InternalMetricSignalSourceAccessSpec(MetricSignalSourceAccessSpec):
    # In order to separate metric groups based on alias/id for internal signals, we add 'id' as the metric group id.
    # This is not a concern for external signals, since with internal signals emission is from within RheocerOS we
    # have to make sure that internal metrics with same 'Name' and other sub-dimensions will not conflict.
    METRIC_GROUP_ID_SUB_DIMENSION_KEY: ClassVar[str] = "MetricGroupId"

    def __init__(self, sub_dimensions: MetricSubDimensionMapType, **kwargs: Dict[str, Any]) -> None:
        context_id: str = kwargs[SignalSourceAccessSpec.OWNER_CONTEXT_UUID]
        super().__init__(SignalSourceType.INTERNAL_METRIC, context_id, sub_dimensions, **kwargs)


class CWMetricSignalSourceAccessSpec(MetricSignalSourceAccessSpec):
    def __init__(self, namespace: str, sub_dimensions: MetricSubDimensionMapType, **kwargs: Dict[str, Any]) -> None:
        super().__init__(SignalSourceType.CW_METRIC, namespace, sub_dimensions, **kwargs)


class AlarmSignalSourceAccessSpec(SignalSourceAccessSpec):
    """Refer
    TODO
    """

    def __init__(
        self, source: SignalSourceType, path_format: str, alarm_id: str, alarm_params: AlarmParams, **kwargs: Dict[str, Any]
    ) -> None:
        self._alarm_id = alarm_id
        self._alarm_params = alarm_params
        super().__init__(source, path_format, kwargs)

    @property
    def alarm_id(self) -> str:
        return self._alarm_id

    @property
    def alarm_params(self) -> AlarmParams:
        return self._alarm_params

    # overrides
    def check_integrity(self, other: "SignalSourceAccessSpec") -> bool:
        if not super().check_integrity(other):
            return False

        # other fields are already as part of the path_format (checked by super above),
        # we just need to check impl specific attributes which are not as part of the path format.
        # currently we don't have anything that does not go into the path format.
        if not isinstance(other, AlarmSignalSourceAccessSpec):
            return False

        if (
            (self.alarm_params and not other.alarm_params)
            or (not self.alarm_params and other.alarm_params)
            or not self.alarm_params.check_integrity(other.alarm_params)
        ):
            return False

        return True

    @classmethod
    def path_format_requires_resource_name(cls) -> bool:
        return False

    def check_termination(self, current: DimensionFilter, new: DimensionFilter) -> bool:
        # OK -> ALARM, ALARM -> OK, OK -> INSUFFICIENT_DATA
        current_alarm_state: str = current.find_dimension_by_name(AlarmDimension.STATE_TRANSITION).value
        new_alarm_state: str = new.find_dimension_by_name(AlarmDimension.STATE_TRANSITION).value
        if current_alarm_state != new_alarm_state:
            return True

        return False

    # overrides
    def auto_complete_filter(
        self, filtering_levels: RawDimensionFilterInput, cast: Optional[DimensionSpec] = None
    ) -> Optional[DimensionFilter]:
        if isinstance(filtering_levels, list):
            missing_alarm_dimension_count = len(AlarmDimension.__members__.keys()) - len(filtering_levels)
            if missing_alarm_dimension_count > 0:
                return DimensionFilter.load_raw(
                    filtering_levels + [AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR for i in range(missing_alarm_dimension_count)],
                    cast=cast,
                )
        return None

    # overrides
    def __and__(self, operands: Tuple["Signal", Union["Signal", Any]]) -> Any:
        """Overwrite bitwise & for high-level operations operations between Signals."""
        from intelliflow.core.signal_processing.signal import SignalProvider

        rhs = operands[1].get_signal() if isinstance(operands[1], SignalProvider) else operands[1]
        alarm_rule = AlarmRule(inverted=False, lhs=operands[0], operator=AlarmRuleOperator.AND, rhs=rhs)
        alarm_rule.validate()
        return alarm_rule

    # overrides
    def __or__(self, operands: Tuple["Signal", Union["Signal", Any]]) -> Any:
        """Overwrite bitwise | for high-level operations operations between Signals."""
        from intelliflow.core.signal_processing.signal import SignalProvider

        rhs = operands[1].get_signal() if isinstance(operands[1], SignalProvider) else operands[1]
        alarm_rule = AlarmRule(inverted=False, lhs=operands[0], operator=AlarmRuleOperator.OR, rhs=rhs)
        alarm_rule.validate()
        return alarm_rule


class InternalAlarmSignalSourceAccessSpec(AlarmSignalSourceAccessSpec):
    def __init__(self, alarm_id: str, alarm_params: AlarmParams, **kwargs: Dict[str, Any]) -> None:
        path_format: str = self.create_path_format(alarm_id)
        super().__init__(SignalSourceType.INTERNAL_ALARM, path_format, alarm_id, alarm_params, **kwargs)

    @classmethod
    def create_path_format(cls, alarm_id: str) -> str:
        return (
            alarm_id
            + cls.path_delimiter()
            + cls.path_delimiter().join([DIMENSION_PLACEHOLDER_FORMAT for _ in AlarmDimension.__members__.keys()])
        )


class CWAlarmSignalSourceAccessSpec(AlarmSignalSourceAccessSpec):
    def __init__(self, name: str, account_id: str, region_id: str, alarm_params: AlarmParams, **kwargs: Dict[str, Any]) -> None:
        self._account_id = account_id
        self._region_id = region_id
        path_format: str = self.create_path_format(name, account_id, region_id)
        super().__init__(SignalSourceType.CW_ALARM, path_format, name, alarm_params, **kwargs)

    @property
    def name(self) -> str:
        return self._alarm_id

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def region_id(self) -> str:
        return self._region_id

    @property
    def arn(self) -> str:
        return f"arn:aws:cloudwatch:{self.region_id}:{self.account_id}:alarm:{self.name}"

    @classmethod
    def create_path_format(cls, name: str, account_id: str, region_id: str) -> str:
        return (
            f"arn:aws:cloudwatch:{region_id}:{account_id}:alarm:{name}"
            + cls.path_delimiter()
            + cls.path_delimiter().join([DIMENSION_PLACEHOLDER_FORMAT for _ in AlarmDimension.__members__.keys()])
        )

    @classmethod
    def create_resource_path(cls, name: str, account_id: str, region_id: str, new_state: AlarmState, time: str) -> str:
        return cls.create_path_format(name, account_id, region_id).format(*(new_state.value, time))

    @classmethod
    def from_resource_path(cls, materialized_path: str) -> "CWAlarmSignalSourceAccessSpec":
        arn: str = materialized_path.split(cls.path_delimiter())[0]
        arn_parts: List[str] = arn.split(":")
        region_id = arn_parts[3]
        account_id = arn_parts[4]
        name = arn_parts[6]
        return CWAlarmSignalSourceAccessSpec(name, account_id, region_id, None)


class CompositeAlarmSignalSourceAccessSpec(SignalSourceAccessSpec):
    """Refer
    TODO
    """

    def __init__(
        self, source: SignalSourceType, path_format: str, alarm_id: str, alarm_params: CompositeAlarmParams, **kwargs: Dict[str, Any]
    ) -> None:
        self._alarm_id = alarm_id
        self._alarm_params = alarm_params
        super().__init__(source, path_format, kwargs)

    @property
    def alarm_id(self) -> str:
        return self._alarm_id

    @property
    def alarm_params(self) -> CompositeAlarmParams:
        return self._alarm_params

    # overrides
    def check_integrity(self, other: "SignalSourceAccessSpec") -> bool:
        if not super().check_integrity(other):
            return False

        # other fields are already as part of the path_format (checked by super above),
        # we just need to check impl specific attributes which are not as part of the path format.
        # currently we don't have anything that does not go into the path format.
        if not isinstance(other, CompositeAlarmSignalSourceAccessSpec) or self.alarm_params != other.alarm_params:
            return False

        return True

    @classmethod
    def path_format_requires_resource_name(cls) -> bool:
        return False

    def check_termination(self, current: DimensionFilter, new: DimensionFilter) -> bool:
        # OK -> ALARM, ALARM -> OK, OK -> INSUFFICIENT_DATA
        current_alarm_state: str = current.find_dimension_by_name(AlarmDimension.STATE_TRANSITION).value
        new_alarm_state: str = new.find_dimension_by_name(AlarmDimension.STATE_TRANSITION).value
        if current_alarm_state != new_alarm_state:
            return True

        return False

    # overrides
    def auto_complete_filter(
        self, filtering_levels: RawDimensionFilterInput, cast: Optional[DimensionSpec] = None
    ) -> Optional[DimensionFilter]:
        if isinstance(filtering_levels, list):
            missing_alarm_dimension_count = len(AlarmDimension.__members__.keys()) - len(filtering_levels)
            if missing_alarm_dimension_count > 0:
                return DimensionFilter.load_raw(
                    filtering_levels + [AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR for i in range(missing_alarm_dimension_count)],
                    cast=cast,
                )
        return None

    # overrides
    def __and__(self, operands: Tuple["Signal", Union["Signal", Any]]) -> Any:
        """Overwrite bitwise & for high-level operations operations between Signals."""
        from intelliflow.core.signal_processing.signal import SignalProvider

        rhs = operands[1].get_signal() if isinstance(operands[1], SignalProvider) else operands[1]
        alarm_rule = AlarmRule(inverted=False, lhs=operands[0], operator=AlarmRuleOperator.AND, rhs=rhs)
        alarm_rule.validate()
        return alarm_rule

    # overrides
    def __or__(self, operands: Tuple["Signal", Union["Signal", Any]]) -> Any:
        """Overwrite bitwise | for high-level operations operations between Signals."""
        from intelliflow.core.signal_processing.signal import SignalProvider

        rhs = operands[1].get_signal() if isinstance(operands[1], SignalProvider) else operands[1]
        alarm_rule = AlarmRule(inverted=False, lhs=operands[0], operator=AlarmRuleOperator.OR, rhs=rhs)
        alarm_rule.validate()
        return alarm_rule


class InternalCompositeAlarmSignalSourceAccessSpec(CompositeAlarmSignalSourceAccessSpec):
    PATH_PREFIX: ClassVar[str] = "internal_composite_alarm"

    def __init__(self, alarm_id: str, alarm_params: CompositeAlarmParams, **kwargs: Dict[str, Any]) -> None:
        path_format: str = self.create_path_format(alarm_id)
        super().__init__(SignalSourceType.INTERNAL_COMPOSITE_ALARM, path_format, alarm_id, alarm_params, **kwargs)

    @classmethod
    def create_path_format(cls, alarm_id: str) -> str:
        return (
            cls.PATH_PREFIX
            + cls.path_delimiter()
            + alarm_id
            + cls.path_delimiter()
            + cls.path_delimiter().join([DIMENSION_PLACEHOLDER_FORMAT for _ in AlarmDimension.__members__.keys()])
        )


class CWCompositeAlarmSignalSourceAccessSpec(CompositeAlarmSignalSourceAccessSpec):
    def __init__(self, name: str, account_id: str, region_id: str, alarm_params: CompositeAlarmParams, **kwargs: Dict[str, Any]) -> None:
        self._account_id = account_id
        self._region_id = region_id
        path_format: str = self.create_path_format(name, account_id, region_id)
        super().__init__(SignalSourceType.CW_COMPOSITE_ALARM, path_format, name, alarm_params, **kwargs)

    @property
    def name(self) -> str:
        return self._alarm_id

    @property
    def account_id(self) -> str:
        return self._account_id

    @property
    def region_id(self) -> str:
        return self._region_id

    @property
    def arn(self) -> str:
        return f"arn:aws:cloudwatch:{self.region_id}:{self.account_id}:alarm:{self.name}"

    @classmethod
    def create_path_format(cls, name: str, account_id: str, region_id: str) -> str:
        """CW Composite ARN is no different than a metric Alarm ARN"""
        return (
            f"arn:aws:cloudwatch:{region_id}:{account_id}:alarm:{name}"
            + cls.path_delimiter()
            + cls.path_delimiter().join([DIMENSION_PLACEHOLDER_FORMAT for _ in AlarmDimension.__members__.keys()])
        )

    @classmethod
    def create_resource_path(cls, name: str, account_id: str, region_id: str, new_state: AlarmState, time: str) -> str:
        return cls.create_path_format(name, account_id, region_id).format(*(new_state.value, time))

    @classmethod
    def from_resource_path(cls, materialized_path: str) -> "CWCompositeAlarmSignalSourceAccessSpec":
        arn: str = materialized_path.split(cls.path_delimiter())[0]
        arn_parts: List[str] = arn.split(":")
        region_id = arn_parts[3]
        account_id = arn_parts[4]
        name = arn_parts[6]
        return CWCompositeAlarmSignalSourceAccessSpec(name, account_id, region_id, None)


if __name__ == "__main__":
    import doctest

    test_dim_spec1: DimensionSpec = DimensionSpec(
        [Dimension("region", Type.STRING)], [DimensionSpec([Dimension("day", Type.DATE)], [None])]
    )
    doctest.testmod(extraglobs={"dim_spec1": test_dim_spec1}, verbose=False)
