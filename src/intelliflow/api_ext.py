# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Core API extensions for specialization in specific use-cases and for simplified experience.

For finer granular control, more flexibility core API should be used.
"""
import copy
import json
import logging
import uuid
from enum import Enum
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union, cast, overload

from intelliflow.core.application.context.node.filtered_views import FilteredView
from intelliflow.core.application.context.node.marshaling.nodes import MarshalingView
from intelliflow.core.entity import CoreData
from intelliflow.core.platform.compute_targets.descriptor import ComputeDescriptor
from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
from intelliflow.core.platform.definitions.aws.glue.client_wrapper import GlueVersion, GlueWorkerType
from intelliflow.core.platform.definitions.aws.s3.bucket_wrapper import get_bucket
from intelliflow.core.platform.definitions.aws.s3.object_wrapper import list_objects
from intelliflow.core.signal_processing.signal import Signal, SignalDimensionTuple
from intelliflow.core.signal_processing.signal_source import (
    PRIMARY_KEYS_KEY,
    DataType,
    ModelSignalSourceFormat,
    DatasetSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
    SignalSourceType,
)
from intelliflow.core.signal_processing.slot import SlotCode, SlotCodeMetadata, SlotCodeType, SlotType

from .api import *
from .core.application.context.node.base import DataNode
from .core.platform.constructs import RoutingTable
from .core.platform.definitions.aws.sagemaker.client_wrapper import (
    BUILTIN_ALGO_MODEL_ARTIFACT_RESOURCE_NAME,
    SagemakerBuiltinAlgo,
    convert_image_uri_to_arn,
)
from .core.platform.definitions.compute import ComputeExecutionDetails, ComputeSessionStateType
from .core.platform.drivers.compute.aws_emr import InstanceConfig, RuntimeConfig
from .core.signal_processing.definitions.dimension_defs import DEFAULT_DATETIME_GRANULARITY, DatetimeGranularity, Type
from .core.signal_processing.definitions.metric_alarm_defs import AlarmType, MetricSubDimensionMap, MetricSubDimensionMapType
from .core.signal_processing.dimension_constructs import (
    AnyVariant,
    DateVariant,
    Dimension,
    DimensionFilter,
    DimensionSpec,
    DimensionVariant,
    DimensionVariantMapFunc,
)
from .core.signal_processing.routing_runtime_constructs import Route, RouteID, RuntimeLinkNode

logger = logging.getLogger(__name__)


class S3DatasetExt(CoreData):
    def __init__(self, account_id: str, bucket: str, folder: str, dimension_vars: List[DimensionVariant], attrs: Dict[str, Any]) -> None:
        self.account_id = account_id
        self.bucket = bucket
        self.folder = folder
        self.dimension_vars = dimension_vars
        self.attrs = attrs


def S3(account_id: str, bucket: str, key_prefix: str, *dimension: DimensionVariant, **kwargs):
    bucket = bucket[len("s3://") :] if bucket.startswith("s3://") else bucket
    return S3DatasetExt(account_id, bucket, key_prefix, list(dimension), kwargs)


class AnyDate(AnyVariant):
    def __init__(self, name: DimensionNameType, params: Optional[Dict[str, Any]] = None) -> None:
        if params:
            if DateVariant.FORMAT_PARAM not in params:
                params.update({DateVariant.FORMAT_PARAM: "%Y-%m-%d"})

            if DateVariant.GRANULARITY_PARAM not in params:
                # DateVariant already takes care of the granulurity but at this level, we should
                # still make sure for user convenience (and to be immune from future changes in that core module).
                params.update({DateVariant.GRANULARITY_PARAM: DEFAULT_DATETIME_GRANULARITY})

        super().__init__(name, Type.DATETIME, params)
        self._value = self.ANY_DIMENSION_VALUE_SPECIAL_CHAR


class AnyString(AnyVariant):
    def __init__(self, name: DimensionNameType, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(name, Type.STRING, params)
        self._value = self.ANY_DIMENSION_VALUE_SPECIAL_CHAR


class AnyLong(AnyVariant):
    def __init__(self, name: DimensionNameType, params: Optional[Dict[str, Any]] = None) -> None:
        super().__init__(name, Type.Long, params)
        self._value = self.ANY_DIMENSION_VALUE_SPECIAL_CHAR


# protocols
HADOOP_SUCCESS_FILE = completion_file("_SUCCESS")


class GlueBatchCompute(InternalDataNode.BatchComputeDescriptor):
    def __init__(
        self,
        code: str,
        lang: Lang = Lang.PYTHON,
        abi: ABI = ABI.GLUE_EMBEDDED,
        extra_permissions: List[Permission] = None,
        retry_count: int = 0,
        **kwargs,
    ) -> None:
        """Does basic check on AWS Glue Parametrization

        Defaults:
        Workers -> 25
        Timeout -> 10 hours
        """
        if not kwargs:
            kwargs = dict()

        if "Timeout" not in kwargs:
            kwargs["Timeout"] = 600

        if "GlueVersion" not in kwargs:
            # let BatchCompute driver choose the right version.
            # existence of this definition provides a hint to the platform as to what runtime conf is desired, impacting
            # which BatchCompute to be used eventually.
            kwargs["GlueVersion"] = GlueVersion.AUTO.value

        if "MaxCapacity" not in kwargs:
            if "WorkerType" not in kwargs:
                kwargs["WorkerType"] = GlueWorkerType.G_1X.value

            if "NumberOfWorkers" not in kwargs:
                kwargs["NumberOfWorkers"] = 25

        super().__init__(code, lang, abi, extra_permissions, retry_count, **kwargs)


Glue = GlueBatchCompute


class EmrBatchCompute(InternalDataNode.BatchComputeDescriptor):
    def __init__(
        self,
        code: str,
        extra_permissions: List[Permission] = None,
        retry_count: int = 0,
        **kwargs,
    ) -> None:
        """Does basic check on AWS EMR Parametrization

        Defaults:
        Workers -> 25 m5.xlarge
        """

        lang: Lang = Lang.PYTHON
        abi: ABI = ABI.GLUE_EMBEDDED
        if not kwargs:
            kwargs = dict()

        if "InstanceConfig" not in kwargs:
            kwargs["InstanceConfig"] = InstanceConfig(25)

        if "RuntimeConfig" not in kwargs:
            kwargs["RuntimeConfig"] = RuntimeConfig.AUTO

        super().__init__(code, lang, abi, extra_permissions, retry_count, **kwargs)


EMR = EmrBatchCompute


class Spark(InternalDataNode.BatchComputeDescriptor):
    def __init__(
        self,
        code: str,
        lang: Lang = Lang.PYTHON,
        abi: ABI = ABI.GLUE_EMBEDDED,
        extra_permissions: List[Permission] = None,
        retry_count: int = 0,
        **kwargs,
    ) -> None:
        super().__init__(code, lang, abi, extra_permissions, retry_count, **kwargs)


class SparkSQL(InternalDataNode.BatchComputeDescriptor):
    def __init__(self, code: str, extra_permissions: List[Permission] = None, retry_count: int = 0, **kwargs) -> None:
        # Temporarily use a typical PySpark conf so that any of the drivers would pick this up (till SPARK_SQL support)
        #  The following defaulting is not actually a coupling with Glue based BatchCompute drivers.
        #  AWS Glue and its API parametrization is adapted as a good basis for overall serverless model in IF
        #  We expect the following parameters to be supported by all of the BatchCompute drivers.
        #  They are hints to the drivers which still can overwrite or map them.
        if "WorkerType" not in kwargs:
            kwargs["WorkerType"] = GlueWorkerType.G_1X.value
        if "NumberOfWorkers" not in kwargs:
            kwargs["NumberOfWorkers"] = 25
        if "GlueVersion" not in kwargs and "RuntimeConfig" not in kwargs:
            kwargs["GlueVersion"] = GlueVersion.AUTO.value

        ## means "run in PySpark as inlined/embedded code"
        super().__init__(
            f'''
import re
queries = [query.strip() for query in re.split(";\\s+|;$", """{code}""")]
for query in queries:
    if query:
        output = spark.sql(query)
                ''',
            Lang.PYTHON,
            ABI.GLUE_EMBEDDED,
            extra_permissions,
            retry_count,
            **kwargs,
        )

        ## TODO support this Lang + ABI pair in Spark based BatchCompute drivers (Glue, EMR).
        # we need this for two things:
        #  - parametrization of code with runtime 'dimensions' in BatchCompute::compute
        #  - enforcing language and ABI as PYTHON and EMBEDDED at this level keeps drivers from handling SQL code in a
        # flexible way
        # super().__init__(code, Lang.SPARK_SQL, ABI.PARAMETRIZED_QUERY, extra_permissions, retry_count, **kwargs)


class PrestoSQL(InternalDataNode.BatchComputeDescriptor):
    def __init__(self, code: str, extra_permissions: List[Permission] = None, retry_count: int = 0, **kwargs) -> None:
        super().__init__(code, Lang.PRESTO_SQL, ABI.PARAMETRIZED_QUERY, extra_permissions, retry_count, **kwargs)


class SagemakerTrainingJob(InternalDataNode.BatchComputeDescriptor):
    def __init__(
        self,
        extra_permissions: List[Permission] = None,
        retry_count: int = 0,
        **kwargs,
    ) -> None:
        if not kwargs:
            raise ValueError(
                "AWS Sagemaker training job parameters cannot be empty! " "Please provide them as keyword args to this compute descriptor."
            )

        # TODO do validations on kwargs based on
        #   https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.create_training_job

        super().__init__("", Lang.AWS_SAGEMAKER_TRAINING_JOB, ABI.NONE, extra_permissions, retry_count, **kwargs)


class SagemakerTransformJob(InternalDataNode.BatchComputeDescriptor):
    def __init__(
        self,
        extra_permissions: List[Permission] = None,
        retry_count: int = 0,
        **kwargs,
    ) -> None:
        if not kwargs:
            raise ValueError(
                "AWS Sagemaker transform job parameters cannot be empty! " "Please provide them as keyword args to this compute descriptor."
            )

        super().__init__("", Lang.AWS_SAGEMAKER_TRANSFORM_JOB, ABI.NONE, extra_permissions, retry_count, **kwargs)


class ApplicationExt(Application):
    @classmethod
    def _create_spec(cls, variants: List[DimensionVariant]) -> DimensionSpec:
        if not variants:
            return None
        spec: DimensionSpec = DimensionSpec()
        dimension_variant = variants[0]
        sub_spec = cls._create_spec(variants[1:])
        spec.add_dimension(Dimension(dimension_variant.name, dimension_variant.type, dimension_variant.params), sub_spec)
        return spec

    @classmethod
    def _create_filter(cls, variants: List[DimensionVariant]) -> DimensionFilter:
        if not variants:
            return None
        filter: DimensionFilter = DimensionFilter()
        dimension_variant = variants[0]
        sub_filter = cls._create_filter(variants[1:])
        filter.add_dimension(dimension_variant, sub_filter)
        return filter

    def add_external_data(self, data_id: str, s3_dataset: S3DatasetExt, completion_file: str = "_SUCCESS"):
        # use the same decl order to setup hierarchy for DimensionSpec and DimensionFilter
        spec: DimensionSpec = self._create_spec(s3_dataset.dimension_vars)
        filter: DimensionFilter = self._create_filter(s3_dataset.dimension_vars)

        return self.marshal_external_data(
            S3Dataset(
                s3_dataset.account_id,
                s3_dataset.bucket,
                s3_dataset.folder,
                *["{}" for dim in s3_dataset.dimension_vars],
                **s3_dataset.attrs,
            ),
            data_id,
            spec,
            filter,
            SignalIntegrityProtocol("FILE_CHECK", {"file": completion_file}) if completion_file else None,
        )

    def add_timer(
        self,
        id: str,
        schedule_expression: str,
        time_dimension_id: str = "time",
        time_dimension_format: str = "%Y-%m-%d",
        time_dimension_granularity: DatetimeGranularity = None,
        **kwargs,
    ) -> MarshalerNode:
        """Create new timer signal within this application. The signal can be bound to other Application APIs most
        notably to create_data to achieve time based scheduling, etc.

        This API is actually a convenience wrapper around "Application::create_timer".

        :param id: internal ID of the new signal, can be used to retrieve this signal using get_timer API. It will be
        used as the default alias for this signal if it is used as an input in downstream create_data calls.
        :param schedule_expression: expression supported by underlying platform configuration. E.g in AWSConfiguration
        this parameter is AWS CloudWatch scheduled expressions that can be in either CRON format or "rate(x [minute(s)|day(s)|...])
        :param time_dimension_id: Just as other RheocerOS signals, this timer will have a DimensionSpec with only one
        dimension of DATETIME type implicitly. Provide the 'id' ('name') of the dimension (e.g dataset partition) using
        this parameter. Partition name will be defaulted to 'time' if not defined by the user.
        :param time_dimension_format: Just as other RheocerOS signals, this timer will have a DimensionSpec with only one
        dimension of DATETIME type implicitly. Provide datetime 'format' of the dimension (e.g '%Y-%m-%d %H-%M') using
        this parameter. Partition format will be set as '%Y-%m-%d' if not defined by the user.
        :param time_dimension_granularity: Just as other RheocerOS signals, this timer will have a DimensionSpec with only one
        dimension of DATETIME type implicitly. Provide datetime 'granularity' of the dimension (e.g DatatimeGranularity.DAY)
        using this parameter. Partition granularity will be set as DatetimeGranularity.DAY implicitly if not defined by
        the user.
        :param kwargs: user provided metadata (for purposes such as cataloguing, etc)
        :return: A new MarshalerNode that encapsulates the timer in development time. Returned value can be used
        as an input to many other Application APIs in a convenient way. Most important of them is 'create_data'
        which can use MarshalerNode and its filtered version (FilteredView) as inputs.
        """
        time_dim_params = {DateVariant.FORMAT_PARAM: time_dimension_format, DateVariant.TIMEZONE_PARAM: "UTC"}

        if time_dimension_granularity:
            time_dim_params.update({DateVariant.GRANULARITY_PARAM: time_dimension_granularity.value})
        time_dim = AnyDate(time_dimension_id, time_dim_params)
        return self.create_timer(id, schedule_expression, time_dim, **kwargs)

    def data(self, data_id: str) -> Optional[MarshalerNode]:
        # check ALL applications (self + upstreams) from the active context
        marshalers: List[MarshalerNode] = self.get_data(data_id)
        if marshalers:
            if len(marshalers) > 1:
                raise ValueError(
                    f"Cannot resolve DataNode! Application::data API cannot be used when the ID maps to multiple data nodes!"
                    f" Entity id: {data_id}, type: <data>, # of mapped entities: {len(marshalers)}, entities: {marshalers}"
                )
            return marshalers[0]

    def timer(self, timer_id: str) -> Optional[MarshalerNode]:
        marshalers: List[MarshalerNode] = self.get_timer(timer_id)
        if marshalers:
            if len(marshalers) > 1:
                raise ValueError(
                    f"Application::timer API cannot be used when the ID maps to multiple nodes of same type!"
                    f" Entity id: {timer_id}, type: <timer>, # of mapped entities: {len(marshalers)}"
                )
            return marshalers[0]

    def metric(self, metric_id: str, sub_dimensions: Optional[MetricSubDimensionMapType] = None) -> Optional[MarshalerNode]:
        marshalers: List[MarshalerNode] = self.get_metric(metric_id, sub_dimensions)
        if marshalers:
            if len(marshalers) > 1:
                raise ValueError(
                    f"Application::metric API cannot be used when the ID (and sub_dimensions) map to "
                    f"multiple metric nodes!"
                    f" Metric id: {metric_id}, sub_dimensions: {sub_dimensions!r},"
                    f" type: <metric>, # of mapped entities: {len(marshalers)}"
                )
            return marshalers[0]

    def alarm(self, alarm_id: str, alarm_type: AlarmType = AlarmType.ALL) -> Optional[MarshalerNode]:
        marshalers: List[MarshalerNode] = self.get_alarm(alarm_id, alarm_type)
        if marshalers:
            if len(marshalers) > 1:
                raise ValueError(
                    f"Application::alarm API cannot be used when the ID (and alarm_type) map to "
                    f"multiple alarm nodes!"
                    f" Alarm id: {alarm_id}, alarm_type: {alarm_type!r}"
                    f" type: <alarm>, # of mapped entities: {len(marshalers)}"
                )
            return marshalers[0]

    def __getitem__(self, entity_id_or_slice) -> MarshalerNode:
        # FUTURE when other node types are introduced call their respective retrieval API
        # ex: model(model_id)
        if isinstance(entity_id_or_slice, str):
            entity_id = entity_id_or_slice
            marshaler = self.data(entity_id)
            if marshaler:
                return marshaler

            marshaler = self.timer(entity_id)
            if marshaler:
                return marshaler

            marshaler = self.metric(entity_id)
            if marshaler:
                return marshaler

            marshaler = self.alarm(entity_id)
            if marshaler:
                return marshaler
        elif isinstance(entity_id_or_slice, slice):
            # exceptional treatment for metrics since 'sub_dimensions' are very likely to be used to uniquely identify
            # a metric. in other terms, it is allowed among metrics nodes to share the same ID.
            entity_id = entity_id_or_slice.start
            resolver_param = entity_id_or_slice.stop
            if not (isinstance(entity_id, str) and isinstance(resolver_param, (MetricSubDimensionMap, AlarmType))):
                raise ValueError(
                    f"Wrong types in <slice> ({entity_id_or_slice!r}) for Application::__getitem__!"
                    f" Must be in the form of [<METRIC_ID | ALARM_ID> : <SUB_DIMENSIONS_MAP | AlarmType>],"
                    f" Metric example: ['my_custom_metric', {{'sub_dim': 'value'}}], "
                    f" Alarm example: ['my_alarm' : AlarmType.COMPOSITE]"
                )

            if isinstance(resolver_param, MetricSubDimensionMap):
                marshaler = self.metric(entity_id, sub_dimensions=resolver_param)
                if marshaler:
                    return marshaler
            elif isinstance(resolver_param, AlarmType):
                marshaler = self.alarm(entity_id, alarm_type=resolver_param)
                if marshaler:
                    return marshaler
        else:
            raise ValueError(
                f"Please provide a <str> or a <slice> (e.g app['metric_id':{{'sub_dim1': 'value'}}]) for"
                f" Application::__getitem__ API! Parameter {entity_id_or_slice!r} is not valid."
            )

        raise ValueError(
            f"Cannot find any active node with ID '{entity_id}' within the application '{self._id}'."
            f" If you are looking for a node from the dev context (which was added to the application in"
            f" this development session but not activated yet), then you cannot use this API. PLease"
            f" use respective getter APIs depending on the node type (e.g get_data for data nodes, "
            f"get_timer for timer nodes, get_metric for metric nodes, etc). These APIs allow you to"
            f"specify 'context' parameter which can be set as either QueryContext.DEV_CONTEXT or "
            f"QueryContext.ALL."
        )


class AWSApplication(ApplicationExt):
    def __init__(
        self,
        app_name: str,
        region_or_platform: Union[str, HostPlatform] = None,
        dev_role_account_id: str = None,
        access_id: str = None,
        access_key: str = None,
        enforce_runtime_compatibility: bool = True,
        **kwargs,
    ) -> None:
        from intelliflow.core.deployment import is_on_remote_dev_env

        # limiting application name length to fail quickly and to avoid creation of dev role and storage account.
        # this is a temporary fix and should eventually be handled in the respective cloud services drivers
        assert len(app_name) <= 16, "App name should be of length 16 or less"

        conf_builder = AWSConfiguration.builder()

        if isinstance(region_or_platform, str) or "region" in kwargs:
            region = region_or_platform if isinstance(region_or_platform, str) else kwargs["region"]
            if dev_role_account_id and not is_on_remote_dev_env():
                conf_builder = conf_builder.with_dev_role_credentials(dev_role_account_id)
            elif access_id is not None and access_key is not None:
                conf_builder = conf_builder.with_admin_access_pair(access_id, access_key)
            else:
                conf_builder = conf_builder.with_default_credentials(as_admin=True)

            for k, v in kwargs.items():
                conf_builder.with_param(k, v)

            super().__init__(app_name, HostPlatform(conf_builder.with_region(region).build()), enforce_runtime_compatibility)
        elif isinstance(region_or_platform, HostPlatform):
            platform = region_or_platform
            for k, v in kwargs.items():
                platform.conf.add_param(k, v)
            super().__init__(app_name, platform, enforce_runtime_compatibility)
        else:
            raise ValueError(f"Please provide HostPlatform object or 'region' parameter for AWSApplication!")

    def get_upstream(
        self, remote_app_name: str, aws_acc_id: str, region: str
    ) -> Mapping[Application.QueryContext, List["RemoteApplication"]]:
        return self.get_upstream_applications(
            remote_app_name,
            AWSConfiguration.builder().with_account_id(aws_acc_id).with_region(region).build(),
            Application.QueryContext.ALL,
        )

    def import_upstream(self, remote_app_name: str, aws_acc_id: str, region: str) -> "RemoteApplication":
        """Make a remote application's artifacts (data, model, etc) available to this application.

        Remote app must have authorized this caller app before via 'authorize_downstream' API.
        """
        return self.import_upstream_application(
            remote_app_name, AWSConfiguration.builder().with_account_id(aws_acc_id).with_region(region).build()
        )

    def authorize_downstream(self, remote_app_name, aws_acc_id: str, region: str) -> None:
        """Authorize another RheocerOS application so that it will be able to do
        'import_upstream' for this application and be able to use/connect to the artifacts
        (such as data, model, etc) seamlessly."""
        self.export_to_downstream_application(
            remote_app_name, AWSConfiguration.builder().with_account_id(aws_acc_id).with_region(region).build()
        )

    def glue_table(
        self,
        database: str,
        table_name: str,
        source_account_id: str = None,  # might be required or recommended to provide for S3 datasets
        table_type: Optional[Union["DatasetType", str]] = None,
        primary_keys: Optional[List[str]] = None,
        id: Optional[str] = None,
        dimension_spec: Optional[Union[DimensionSpec, Dict[str, Any]]] = None,
        dimension_filter: Optional[Union[DimensionFilter, Dict[str, Any]]] = None,
        protocol: SignalIntegrityProtocol = None,
        tags: str = None,
        **metadata,  # catalog metadata, etc
    ) -> MarshalerNode:
        """Convenience API that flattens out all of the parameters that *might* be required based on the information
        available on catalog and table type.

        If stars are aligned, this API provides the best UX as shown below:

           ucsi_table= app.glue_table('booker', 'unified_customer_shipment_items')
        """
        if source_account_id is not None:
            metadata["account_id"] = source_account_id

        if table_type is not None:
            metadata["table_type"] = table_type

        if primary_keys is not None:
            metadata[PRIMARY_KEYS_KEY] = primary_keys

        return self.marshal_external_data(GlueTable(database, table_name, **metadata), id, dimension_spec, dimension_filter, protocol, tags)

    def create_data(
        self,
        id: str,
        inputs: Union[List[Union[FilteredView, MarshalerNode]], Dict[str, Union[FilteredView, MarshalerNode]]] = None,
        input_dim_links: Optional[Sequence[Tuple[SignalDimensionTuple, DimensionVariantMapFunc, SignalDimensionTuple]]] = None,
        output_dimension_spec: Optional[Union[Dict[str, Any], DimensionSpec]] = None,
        output_dim_links: Optional[
            Sequence[
                Tuple[
                    Union[OutputDimensionNameType, SignalDimensionTuple],
                    DimensionVariantMapFunc,
                    Union[OutputDimensionNameType, Tuple[OutputDimensionNameType, ...], SignalDimensionTuple],
                ]
            ]
        ] = None,
        compute_targets: Optional[Union[Sequence[ComputeDescriptor], str]] = None,
        execution_hook: RouteExecutionHook = None,
        pending_node_hook: RoutePendingNodeHook = None,
        pending_node_expiration_ttl_in_secs: int = None,
        auto_input_dim_linking_enabled=True,
        auto_output_dim_linking_enabled=True,
        auto_backfilling_enabled=False,
        protocol: SignalIntegrityProtocol = InternalDataNode.DEFAULT_DATA_COMPLETION_PROTOCOL,
        **kwargs,
    ) -> MarshalerNode:
        """Overwrites Application::create_data in order to provide defaulting on parameters, specific to AWS use-case.

        :param id: id/route_id/data_id of the new internal data node to be created. this ID will be used to find/get this
        node while using the other APIs of the Application. It is also used as part of the route_id to be used in runtime.
        :param inputs: Filtered or unfiltered references of other data nodes which are the return values of previous calls
        to node generating APIs such as marshal_external_data or again the same API 'create_data'.
        :param input_dim_links: How should the 'inputs' be linked to each other over their dimensions? This is important
        to determine executions at runtime. DEFAULT value is EMPTY. While empty, if 'auto_input_dim_linking_enabled' is set False,
        then any combination of input signals would yield an execution. Input dimensions on either side of the link must be referred
        using the input signals from 'inputs' ( e.g input('input_dimension_name') or input.dimension('dim_name')).
        For multiple dimension use of inputs on the right hand side, add extra dimensions as new arguments to __call__
        call or 'dimension' method call on the input signal object (e.g input('dim1', 'dim2') or input.dimension('dim1', 'dim2')).
        Multiple dimensions can only be used on the right hand side. Signature (argument count) of the mapper function
        for the link should be compatible with the number of dimensions used on the right hand side.
        :param output_dimension_spec: What are the dimensions of the signal that would represent this new node? And what is the
        structure/order? If left unset, then the DEFAULT value is equivalent to the spec of the first input.
        :param output_dim_links: How should the output and the inputs relate each other? Which dimensions of the output can
        be retrieved from which input dimensions at runtime? if output_dimension_spec input is empty, then the DEFAULT value
        is (aligned with the default behaviour for the spec) equality/linking on the dimensions of the first input. If
        'output_dimension_spec' is not empty but this param is left empty, then the outcome is determined by
        'auto_output_dim_linking_enabled' param. Output dimensions on either side of the link must be referred using
        type str aliased as <OutputDimensionNameType>. Input dimensions on either side of the link must be referred
        using the input signals from 'inputs' ( e.g input('input_dimension_name') or input.dimension('dim_name')).
        To use multiple dimensions on the right hand side for output use a tuple of <OutputDimensionNameType>s. But for
        multiple dimension use of inputs on the right hand side, add extra dimensions as new arguments to __call__ call
        or 'dimension' method call on the input signal object (e.g input('dim1', 'dim2') or
        input.dimension('dim1', 'dim2')). Multiple dimensions can only be used on the right hand side.
        Signature (argument count) of the mapper function for the link should be compatible with the number of
        dimensions used on the right hand side.
        :param compute_targets: When incoming signals for the inputs are linked successfully and a new execution context is created
        at runtime, which compute targets should be run using those signals and their material dimension values? For AWS case,
        user can optionally provide the code. In that case, this API will by DEFAULT wrap with GlueBatchCompute.
        :param execution_hook: Provide an instance of <ExecutionHook> (or <RouteExecutionHook>) to have runtime hooks
        into your own code along with remote execution and compute actions. Each callback/hook can either be pure Python
        Callable or a Callable wrapped by InlinedCompute type. RheocerOS provides interfaces for each hook type. Please
        see the internal types from class <RoutingHookInterface.Execution>: <IExecutionBeginHook>, <IExecutionSkippedHook>,
        <IExecutionSuccessHook>, <IExecutionFailureHook>, ...
        :param pending_node_hook: Provide an instance of <PendingNodeHook> (or <RoutePendingNodeHook>) to have runtime hooks
        into your own code when pending event-trigger groups (pending nodes) are created (first ever event is received), expired or
        when a checkpoint is hit. For expiration hook to be called, the next param 'pending_node_expiration_ttl_in_secs' must be
        defined. Defining expiration hook without an expiration TTL is not allowed. Each callback/hook can either be pure Python
        Callable or a Callable wrapped by InlinedCompute type. RheocerOS provides interfaces for each hook type. Please
        see the internal types from class <RoutingHookInterface.PendingNode>: <IPendingNodeCreationHook>, <IPendingNodeExpirationHook>,
        <IPendingCheckpointHook>
        :param pending_node_expiration_ttl_in_secs: Determine how long the system should keep track of a pending event trigger
        group. For example: an event was received a week ago on a particular dimension values (e.g date partition), but for the
        other inputs of your data node, there has been no events so far. This forms a Pending Node and without a TTL RheocerOS
        persists and tracks them forever until routing data reset (incompatible update), terminate or internal error occurs.
        :param auto_input_dim_linking_enabled: Enables the convenience functionality to link inputs to each other over
        same 'dimensions'. Unique dimensions are still left unlinked.
        :param auto_output_dim_linking_enabled: Enables the convenience functionality to link output dimensions to any
        of the inputs based on the assumption of dimension name equality.
        :param auto_backfilling_enabled: TODO
        :param protocol: completition protocol for the output. default value if "_SUCCESS" file based pritimitive
        protocol (also used by Hadoop, etc).
        :param kwargs: Provide metadata. Format and content are up to the client and they are guaranteed to be preserved.

        :return: A new MarshalerNode that encapsulates the internal data on the client side. Returned value can be used
        as an input to many other Application APIs in a convenient way. Most important of them is again this same API
        'create_data' which can use MarshalerNode and its filtered version (FilteredView) as inputs.
        """
        return self._create_or_update_data_with_defaults(
            id,
            inputs,
            input_dim_links,
            output_dimension_spec,
            output_dim_links,
            compute_targets,
            execution_hook,
            pending_node_hook,
            pending_node_expiration_ttl_in_secs,
            auto_input_dim_linking_enabled,
            auto_output_dim_linking_enabled,
            auto_backfilling_enabled,
            protocol,
            is_update=False,
            **kwargs,
        )

    def update_data(
        self,
        id: str,
        inputs: Union[List[Union[FilteredView, MarshalerNode]], Dict[str, Union[FilteredView, MarshalerNode]]] = None,
        input_dim_links: Optional[Sequence[Tuple[SignalDimensionTuple, DimensionVariantMapFunc, SignalDimensionTuple]]] = None,
        output_dimension_spec: Optional[Union[Dict[str, Any], DimensionSpec]] = None,
        output_dim_links: Optional[
            Sequence[
                Tuple[
                    Union[OutputDimensionNameType, SignalDimensionTuple],
                    DimensionVariantMapFunc,
                    Union[OutputDimensionNameType, Tuple[OutputDimensionNameType, ...], SignalDimensionTuple],
                ]
            ]
        ] = None,
        compute_targets: Optional[Union[Sequence[ComputeDescriptor], str]] = None,
        execution_hook: RouteExecutionHook = None,
        pending_node_hook: RoutePendingNodeHook = None,
        pending_node_expiration_ttl_in_secs: int = None,
        auto_input_dim_linking_enabled=True,
        auto_output_dim_linking_enabled=True,
        auto_backfilling_enabled=False,
        protocol: SignalIntegrityProtocol = InternalDataNode.DEFAULT_DATA_COMPLETION_PROTOCOL,
        enforce_referential_integrity=True,
        **kwargs,
    ) -> MarshalerNode:
        """See AWSApplication::create_data for parametrization.

        Updates an existing data node using Application::update_data after applying some defaulting specific to AWS.
        """
        return self._create_or_update_data_with_defaults(
            id,
            inputs,
            input_dim_links,
            output_dimension_spec,
            output_dim_links,
            compute_targets,
            execution_hook,
            pending_node_hook,
            pending_node_expiration_ttl_in_secs,
            auto_input_dim_linking_enabled,
            auto_output_dim_linking_enabled,
            auto_backfilling_enabled,
            protocol,
            enforce_referential_integrity,
            is_update=True,
            **kwargs,
        )

    def _create_or_update_data_with_defaults(
        self,
        id: str,
        inputs: Union[List[Union[FilteredView, MarshalerNode]], Dict[str, Union[FilteredView, MarshalerNode]]] = None,
        input_dim_links: Optional[Sequence[Tuple[SignalDimensionTuple, DimensionVariantMapFunc, SignalDimensionTuple]]] = None,
        output_dimension_spec: Optional[Union[Dict[str, Any], DimensionSpec]] = None,
        output_dim_links: Optional[
            Sequence[
                Tuple[
                    Union[OutputDimensionNameType, SignalDimensionTuple],
                    DimensionVariantMapFunc,
                    Union[OutputDimensionNameType, Tuple[OutputDimensionNameType, ...], SignalDimensionTuple],
                ]
            ]
        ] = None,
        compute_targets: Optional[Union[Sequence[ComputeDescriptor], str]] = None,
        execution_hook: RouteExecutionHook = None,
        pending_node_hook: RoutePendingNodeHook = None,
        pending_node_expiration_ttl_in_secs: int = None,
        auto_input_dim_linking_enabled=True,
        auto_output_dim_linking_enabled=True,
        auto_backfilling_enabled=False,
        protocol: SignalIntegrityProtocol = InternalDataNode.DEFAULT_DATA_COMPLETION_PROTOCOL,
        enforce_referential_integrity=True,
        is_update=False,
        **kwargs,
    ) -> MarshalerNode:
        if output_dimension_spec is None:
            if inputs:
                # set output dimension spec as first input's spec
                if isinstance(inputs, Dict):
                    first_signal = self._get_input_signal(next(iter(inputs.items()))[1])
                    all_signals = [self._get_input_signal(input) for input in inputs.values()]
                else:
                    first_signal = self._get_input_signal(inputs[0])
                    all_signals = [self._get_input_signal(input) for input in inputs]

                output_dimension_spec = first_signal.domain_spec.dimension_spec
                if not output_dim_links:
                    # if output dim links are not provided either, then again prefer linking to the first input. But
                    # check if that input is dependent or not. If dependent, then only already materialized dimensions
                    # from it can be mapped to the output, for other dimensions go over independent inputs and try to find
                    # the same dimension (with the same name) in them.
                    output_signal = first_signal.clone(None)
                    if not output_signal.is_dependent:  # first signal -> reference, nearest, etc
                        output_dim_links = [
                            (
                                dim_name,
                                None,
                                SignalDimensionTuple(
                                    output_signal, output_signal.domain_spec.dimension_spec.find_dimension_by_name(dim_name)
                                ),
                            )
                            for dim_name in output_signal.domain_spec.dimension_spec.get_flattened_dimension_map().keys()
                        ]
                    else:
                        # we have to make sure non-materialized dimensions are mapped from other signals.
                        # we cannot allow non-materialized dimension mapping to the output from a dependent signal.
                        output_dim_links = []
                        independent_signals = [input for input in all_signals if not input.is_dependent]
                        for dim_name, dim in output_signal.domain_spec.dimension_spec.get_flattened_dimension_map().items():
                            dim_variant = output_signal.domain_spec.dimension_filter_spec.find_dimension_by_name(dim_name)
                            if dim_variant.is_material_value():
                                # assign as literal value, otherwise output would require dependent signal at runtime to be
                                # materialized (until RuntimeLinkNode::materialized_output would support already materialized dimensions).
                                # RuntimeLinkNode::_check_dependents rely on materialized_output to compensate dependents,
                                # which in turn would rely on a dependent if we don't use literal assignment.
                                # output_dim_links.append((dim_name, None, SignalDimensionTuple(output_signal, dim)))
                                output_dim_links.append((dim_name, None, dim_variant.value))
                            else:
                                # if not material, then try to link the dimension from any of the independent signals
                                link = None
                                # TODO once the graph analyzer is added, use it retrieve to even from dependent signals
                                #  just make sure that directly or transitively the link leads to a source dimension from
                                #  a independent signal
                                for other_indep_input in independent_signals:
                                    other_dim = other_indep_input.domain_spec.dimension_spec.find_dimension_by_name(dim_name)
                                    if other_dim:
                                        link = (dim_name, None, SignalDimensionTuple(other_indep_input.clone(None), other_dim))
                                        break
                                if link is None:
                                    raise ValueError(
                                        f"Cannot link unmaterialized dimension {dim_name!r} from dependent "
                                        f"input {first_signal.alias!r} to output {id!r}"
                                    )
                                output_dim_links.append(link)

            else:
                output_dimension_spec = DimensionSpec()
                output_dim_links = []

        if not compute_targets:
            raise ValueError(f"Cannot create data node {id!r} without compute targets! " f"Please define the param compute_targets!")

        if isinstance(compute_targets, str):
            # TODO add this default batch-compute generation to batch_compute driver as an abstract classmethod
            # then remove this overwrite (keep everything in Application::create_data)
            compute_targets = [
                GlueBatchCompute(compute_targets, WorkerType=GlueWorkerType.G_1X.value, NumberOfWorkers=100, Timeout=12 * 60)  # 72 hours
            ]

        if not is_update:
            return super(AWSApplication, self).create_data(
                id,
                inputs,
                input_dim_links if input_dim_links else [],
                output_dimension_spec,
                output_dim_links if output_dim_links else [],
                compute_targets,
                execution_hook,
                pending_node_hook,
                pending_node_expiration_ttl_in_secs,
                auto_input_dim_linking_enabled,
                auto_output_dim_linking_enabled,
                auto_backfilling_enabled,
                protocol,
                **kwargs,
            )
        else:
            return super(AWSApplication, self).update_data(
                id,
                inputs,
                input_dim_links if input_dim_links else [],
                output_dimension_spec,
                output_dim_links if output_dim_links else [],
                compute_targets,
                execution_hook,
                pending_node_hook,
                pending_node_expiration_ttl_in_secs,
                auto_input_dim_linking_enabled,
                auto_output_dim_linking_enabled,
                auto_backfilling_enabled,
                protocol,
                enforce_referential_integrity,
                **kwargs,
            )

    def _load_external_data(
        self,
        external_data_node: ExternalDataNode,
        output: Union[MarshalingView, MarshalerNode],
        materialized_output: Signal,
        materialized_path: str,
        limit: int = None,
    ) -> Iterator[Tuple[str, bytes]]:
        """Returns an iterator of Tuple[physical_path, BLOB] for the external data, in a platform specific way"""
        platform = self._get_platform_for(external_data_node.signal())

        aws_configuration: AWSConfiguration = cast(AWSConfiguration, platform.conf)
        session = aws_configuration.get_param(AWSCommonParams.BOTO_SESSION)
        region = aws_configuration.get_param(AWSCommonParams.REGION)

        if materialized_output.resource_access_spec.source == SignalSourceType.S3:
            s3_spec = cast(S3SignalSourceAccessSpec, materialized_output.resource_access_spec)
            bucket_name = s3_spec.bucket
            s3 = session.resource("s3", region_name=region)
            bucket = get_bucket(s3, bucket_name)

            materialized_paths = materialized_output.get_materialized_resource_paths()
            for materialized_path in materialized_paths:
                prefix = materialized_path.replace(f"s3://{bucket_name}/", "")
                objects_in_folder = list_objects(bucket, prefix)
                for object in objects_in_folder:
                    key = object.key
                    body = object.get()["Body"].read()
                    yield (f"{materialized_path}/{key}", body)
        elif materialized_output.resource_access_spec.source == SignalSourceType.GLUE_TABLE:
            if not self.get_data(external_data_node.data_id, self.QueryApplicationScope.ALL, self.QueryContext.DEV_CONTEXT):
                error_str = (
                    f"Data node {external_data_node.data_id!r} does not exist in the current development "
                    f"context! If you refer it from the already active context of the application, try "
                    f"to attach it via Application::attach to pull the active nodes into your dev context."
                )
                logger.error(error_str)
                raise ValueError(error_str)

            # since we are about to modify the DAG temporarily push the dev-state.
            self.save_dev_state()

            # we will be modifying the active DAG and at the end of this operation restore it, so save it for later.
            active_state = copy.deepcopy(self._active_context) if self._active_context else None
            temp_data = self.create_data(
                id=str(uuid.uuid1()),
                inputs=[output],
                compute_targets=f"output={materialized_output.alias}{f'.limit({limit})' if limit else ''}",
            )
            try:
                # 'output' is materialized, so is 'temp_data' (since it by default adapts its spec and filter.
                self.execute(temp_data)

                # data should be ready now
                # since it is internal, we can do the following conveniently (same logic as internal data handling
                # within 'load_data')
                data_it = self.platform.storage.load_internal(temp_data.signal())
                for data in data_it:
                    path, _ = data
                    schema_file = temp_data.signal().resource_access_spec.data_schema_file
                    if not schema_file or not path.lower().endswith(schema_file.lower()):
                        success_file = temp_data.signal().get_required_resource_name().lower()
                        if not success_file or not path.lower().endswith(success_file):
                            yield data
            finally:
                # restore previous active state
                # TODO support Application::delete_data (with dependency check)
                #  after the the following two operations won't be necessary
                complete_cleanup = True
                try:
                    # at this point, temp is created off of output. their specs are same. cloning output's materialized
                    # filter into the temp signals filter will make it materialized as well.
                    materialized_temp_output = temp_data.signal().filter(materialized_output.domain_spec.dimension_filter_spec)
                    if not platform.storage.delete_internal(materialized_temp_output):
                        complete_cleanup = False
                except Exception:
                    complete_cleanup = False

                if not complete_cleanup:
                    logger.critical(
                        f"There has been problems during the clean-up after the load data operation on {external_data_node.data_id!r}"
                    )
                    logger.critical(
                        f"Please try to clean-up following resources manually: {temp_data.signal().get_materialized_resource_paths()!r}"
                    )
                if active_state:
                    self._dev_context = active_state
                    self.activate()
                else:
                    # was not active before, we have just launched it for this operation. don't keep it running at least.
                    self.pause()

                # pop the dev-state now and restore the application state
                self.load_dev_state()

    def train_xgboost(
        self,
        id: str,
        training_data: Union[FilteredView, MarshalerNode],
        validation_data: Union[FilteredView, MarshalerNode],
        extra_inputs: Union[List[Union[FilteredView, MarshalerNode]], Dict[str, Union[FilteredView, MarshalerNode]]] = None,
        input_dim_links: Optional[Sequence[Tuple[SignalDimensionTuple, DimensionVariantMapFunc, SignalDimensionTuple]]] = None,
        output_dimension_spec: Optional[Union[Dict[str, Any], DimensionSpec]] = None,
        output_dim_links: Optional[
            Sequence[
                Tuple[
                    Union[OutputDimensionNameType, SignalDimensionTuple],
                    DimensionVariantMapFunc,
                    Union[OutputDimensionNameType, Tuple[OutputDimensionNameType, ...], SignalDimensionTuple],
                ]
            ]
        ] = None,
        training_job_params: Dict[str, Any] = None,
        extra_compute_targets: Optional[Union[Sequence[ComputeDescriptor], str]] = None,
        execution_hook: RouteExecutionHook = None,
        pending_node_hook: RoutePendingNodeHook = None,
        pending_node_expiration_ttl_in_secs: int = None,
        retry_count=0,
        **kwargs,
    ) -> MarshalerNode:
        """Convenience method to facilitate training using AWS Sagemaker builtin 'xgboost' algorithm"""
        if extra_compute_targets and any(
            [compute_target.create_slot(None).type.is_batch_compute() for compute_target in extra_compute_targets]
        ):
            raise ValueError(f"training node {id!r} cannot have a BatchCompute target in 'extra_compute_targets' param!")
        else:
            extra_compute_targets = []

        compute_params = dict(training_job_params) if training_job_params else dict()
        if "AlgorithmSpecification" not in compute_params:
            compute_params["AlgorithmSpecification"] = {}
        if "TrainingImage" not in compute_params["AlgorithmSpecification"]:
            compute_params["AlgorithmSpecification"].update({"AlgorithmName": SagemakerBuiltinAlgo.XGBOOST.value})

        inputs = {"train": training_data, "validation": validation_data}
        if extra_inputs:
            if isinstance(extra_inputs, Dict):
                input_signals = [self._get_input_signal(filtered_view, alias) for alias, filtered_view in extra_inputs.items()]
            else:
                input_signals = [self._get_input_signal(filtered_view) for filtered_view in extra_inputs]

            extra_signals = self._check_upstream(input_signals)

            for extra_input in extra_signals:
                inputs.update({extra_input.alias: extra_input})

        return self.create_data(
            id=id,
            inputs=inputs,
            input_dim_links=input_dim_links,
            output_dimension_spec=output_dimension_spec,
            output_dim_links=output_dim_links,
            compute_targets=[SagemakerTrainingJob(retry_count=retry_count, **compute_params)] + extra_compute_targets,
            execution_hook=execution_hook,
            pending_node_hook=pending_node_hook,
            pending_node_expiration_ttl_in_secs=pending_node_expiration_ttl_in_secs,
            # Sagemaker adds a folder before the model output file using the training job name
            # e.g s3://.../internal_data/data/{dim1}/.../{dimN}/IntelliFlow-TrainingJob-{UUID/output/model.tar.gz
            #   IntelliFlow-TrainingJob-{UUID/output/ is added as a prefix by SM
            protocol=SignalIntegrityProtocol(
                "FILE_CHECK",
                {
                    "file": "*"
                    + S3SignalSourceAccessSpec.path_delimiter()
                    + "*"
                    + S3SignalSourceAccessSpec.path_delimiter()
                    + BUILTIN_ALGO_MODEL_ARTIFACT_RESOURCE_NAME
                },
            ),
            **kwargs,
        )

    def marshal_external_sagemaker_model(
        self,
        id: str,
        training_image_uri: str,
        **kwargs,
    ) -> MarshalerNode:
        """
        Import a custom model created by the user. We can say that model concept here maps to Sagemaker <Model> entity.

        Please refer
          https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateModel.html
          https://docs.aws.amazon.com/sagemaker/latest/dg/realtime-endpoints-deployment.html#realtime-endpoints-deployment-create-model

        Also the `training_image_uri` argument here maps to `Image` parameter in that API.
        """
        return self.marshal_external_model(
            id,
            model_metadata={
                "AlgorithmSpecification": {"TrainingImage": training_image_uri},
            },
            model_format=ModelSignalSourceFormat.SAGEMAKER_TRAINING_JOB,
            permissions=[
                Permission(
                    [convert_image_uri_to_arn(training_image_uri)],
                    [
                        "ecr:Describe*",
                        "ecr:BatchCheckLayerAvailability",
                        "ecr:GetDownloadUrlForLayer",
                        "ecr:BatchGetImage",
                        "ecr:GetAuthorizationToken",
                    ],
                )
            ],
            **kwargs,
        )

    def marshal_external_model(
        self,
        id: str,
        model_metadata: Dict[str, Any],
        model_format: ModelSignalSourceFormat,
        permissions: Optional[List[Permission]] = None,
        **kwargs,
    ) -> MarshalerNode:
        noop_compute_with_perms = InlinedCompute(lambda input_map, output, params: ..., permissions=permissions)
        return self.create_data(
            id,
            compute_targets=[noop_compute_with_perms],
            model_metadata=model_metadata,
            data_type=DataType.MODEL_ARTIFACT,
            model_format=model_format,
            schema=None,
            header=False,
            **kwargs,
        )


# Other Convenience Methods
def has_session_failed(compute_record: RoutingTable.ComputeRecord) -> bool:
    return compute_record.session_state and compute_record.session_state.state_type == ComputeSessionStateType.FAILED


def has_started(compute_record: RoutingTable.ComputeRecord) -> bool:
    return bool(compute_record.session_state)


def get_execution_details(compute_record: RoutingTable.ComputeRecord) -> Sequence[ComputeExecutionDetails]:
    return compute_record.session_state.executions


class EscapeExistingJobParams(dict):
    # refer
    #  https://docs.python.org/3/library/stdtypes.html#str.format_map
    def __missing__(self, key):
        return "{" + key + "}"


def python_module(module_path: str, external_library_paths: Optional[List[str]] = None, **kwargs) -> str:
    import inspect
    import importlib

    code_spec = importlib.util.find_spec(module_path)
    if not code_spec:
        raise ValueError(f"{module_path!r} cannot be resolved to be a valid Python module path.")
    mod = importlib.util.module_from_spec(code_spec)
    if not inspect.ismodule(mod):
        raise ValueError(f"{module_path} is not a valid Python module!")
    module_src = inspect.getsource(mod)

    params = {key: repr(value) for key, value in kwargs.items()}
    code = module_src if not params else module_src.format_map(EscapeExistingJobParams(params))

    if external_library_paths:
        code = SlotCode(code, SlotCodeMetadata(SlotCodeType.EMBEDDED_SCRIPT, external_library_paths=external_library_paths))

    return code


# provide convenience methods for users to capture two possible cases for Scala code.
def scala_module(class_path: str, external_library_paths: Optional[List[str]], entity: str, method: str) -> str:
    return SlotCode(class_path, SlotCodeMetadata(SlotCodeType.MODULE_PATH, entity, method, external_library_paths))


def scala_script(
    code: str, entity: Optional[str] = None, method: Optional[str] = None, external_library_paths: Optional[List[str]] = None
) -> str:
    """Convenience method to help users with specifying entity + method even for an embedded script.
    When entity and method are none, this is equivalent to user providing a script that has no Classes/Objects
    and a target method in it.
    But it should be remembered that even in that case (only 'code' is defined), then final compute driver is
    determined by user's ABI choice and other compute parameters for a node. Particularly, ABI determines the calling
    convention and which boilerplate to be used at runtime.
    """
    return SlotCode(code, SlotCodeMetadata(SlotCodeType.EMBEDDED_SCRIPT, entity, method, external_library_paths))


def sql_module(module_root: str, file_name: str) -> str:
    import inspect
    import importlib
    from importlib.resources import contents
    from importlib.resources import path

    code_spec = importlib.util.find_spec(module_root)

    if not code_spec:
        raise ValueError(f"{module_root!r} cannot be resolved to be a valid Python module path.")

    root_mod = importlib.util.module_from_spec(code_spec)

    if not inspect.ismodule(root_mod):
        raise ValueError(f"{module_root} is not a valid Python module!")

    for resource in contents(root_mod):
        with path(root_mod, resource) as resource_path:
            if resource == file_name:
                data = resource_path.read_text()
                return data

    raise ValueError(f"File({file_name}) not found in {module_root}!")
