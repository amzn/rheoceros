# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import ClassVar, Dict, List, Optional, Set, Type

import boto3
from botocore.exceptions import ClientError

from intelliflow.core.platform.constructs import Extension
from intelliflow.core.signal_processing import DimensionFilter, Signal
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import CWMetricSignalSourceAccessSpec

from .....constructs import ConstructInternalMetricDesc, ConstructParamsDict, ConstructPermission, ConstructSecurityConf
from .....definitions.aws.common import CommonParams as AWSCommonParams
from .....definitions.aws.common import exponential_retry
from .....definitions.aws.ddb.client_wrapper import BillingMode, create_table, delete_table, update_table, update_ttl
from .....definitions.common import ActivationParams
from ....aws_common import AWSConstructMixin

module_logger = logging.getLogger(__name__)


class AWSDDBTableExtension(AWSConstructMixin, Extension):
    """A basic resource extension to manage the lifecycle of a single DynamoDB table."""

    DEFAULT_TABLE_NAME_FORMAT: ClassVar[str] = "IntelliFlow-{0}-{1}-{2}-{3}"

    class Descriptor(Extension.Descriptor):
        def __init__(
            self,
            extension_id: str,
            table_name: Optional[str] = None,
            key_schema: List[Dict[str, str]] = [{"AttributeName": "id", "KeyType": "HASH"}],
            attribute_def: List[Dict[str, str]] = [{"AttributeName": "id", "AttributeType": "S"}],
            ttl_attribute_name: Optional[str] = None,
            **extra_boto_args,
        ) -> None:
            super().__init__(extension_id, **extra_boto_args)
            self._table_name = table_name
            self._key_schema = key_schema
            self._attribute_def = attribute_def
            self._ttl_attribute_name = ttl_attribute_name
            self._billing_mode = BillingMode.PAY_PER_REQUEST

            if "TableName" in extra_boto_args:
                if self._table_name:
                    raise ValueError(f"{self.__class__.__name__}: 'table_name' and 'TableName' cannot be defined at the same time!")
                self._table_name = extra_boto_args["TableName"]

            billing_mode_in_kwargs = extra_boto_args.get("BillingMode", None)
            if billing_mode_in_kwargs and billing_mode_in_kwargs != self._billing_mode.value:
                raise ValueError(f"{self.__class__.__name__}: BillingMode must be {self._billing_mode.value!r}!")

        @property
        def table_name(self) -> str:
            return self._table_name

        @property
        def key_schema(self) -> List[Dict[str, str]]:
            return self._key_schema

        @property
        def attribute_def(self) -> List[Dict[str, str]]:
            return self._attribute_def

        @property
        def ttl_attribute_name(self) -> Optional[str]:
            return self._ttl_attribute_name

        @property
        def billing_mode(self) -> BillingMode:
            return self._billing_mode

        # overrides
        # because we allow 'table_name' to be specified by user, so when it changes we expect this extension to be invalidated
        # by `CompositeExtension::_deserialized_init`
        def __eq__(self, other):
            return type(self) == type(other) and self.table_name == other.table_name

        def __hash__(self) -> int:
            return hash(self.__class__) + hash(self.table_name)

        # overrides
        def provide_extension_type(cls) -> Type["Extension"]:
            return AWSDDBTableExtension

    def __init__(self, params: ConstructParamsDict) -> None:
        """Called the first time this construct is added/configured within a platform.

        Subsequent sessions maintain the state of a construct, so the following init
        operations occur in the very beginning of a construct's life-cycle within an app.
        """
        super().__init__(params)
        self._ddb = self._session.resource("dynamodb", region_name=self._region)
        self._ddb_scaling = self._session.client("application-autoscaling", region_name=self._region)
        self._table_name = None
        self._table = None

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def table_arn(self) -> str:
        return f"arn:aws:dynamodb:{self._region}:{self._account_id}:table/{self._table_name}"

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._ddb = self._session.resource("dynamodb", region_name=self._region)
        self._ddb_scaling = self._session.client("application-autoscaling", region_name=self._region)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._ddb = None
        self._ddb_scaling = None
        self._table = None

    def _delete_table(self, table_name: str) -> None:
        table = self._ddb.Table(table_name)
        try:
            exponential_retry(
                delete_table,
                [
                    "LimitExceededException",
                    "InternalServerError",
                    # this one was capture during resilience integ-test.
                    # repetitive activate->terminate cycle, or a very quick activate->terminate transition might
                    # cause the following:
                    # "Attempt to change a resource which is still in use: Table: <TABLE> is in the process of being updated."
                    "ResourceInUseException",
                ],
                table=table,
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                raise

    def terminate(self) -> None:
        """Designed to be resilient against repetitive calls in case of retries in the high-level
        termination work-flow.
        """
        module_logger.info(f"{self.__class__.__name__}: Deleting the extension table...")
        if self._table_name:
            self._delete_table(self._table_name)
            self._table_name = None
        module_logger.info(f"Extension table {self._table_name!r} has been deleted!")
        super().terminate()

    @classmethod
    def generate_default_table_name(cls, extension_id: str, context_id: str, region: str):
        return cls.DEFAULT_TABLE_NAME_FORMAT.format(cls.__class__.__name__, extension_id, context_id, region)

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)
        # default table name if the user left it empty on the descriptor
        if self.desc.table_name is None:
            default_table_name: str = self.generate_default_table_name(
                self.desc.extension_id, self._dev_platform.context_id.lower(), self._region
            )
            self._table_name = default_table_name
        else:
            self._table_name = self.desc.table_name

        self._validate_table_name_on_dev_init(self._table_name)
        self._table = self._ddb.Table(self._table_name)

    def _validate_table_name_on_dev_init(self, table_name: str):
        if len(table_name) > 255:
            raise ValueError(
                f"Cannot dev_init {self.__class__.__name__} due to very long" f" AWS DynamoDB Name {table_name} (limit <= 255)."
            )

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        """Whole platform got bootstrapped at runtime. For other runtime services, this
        construct should be initialized (ex: context_owner: Lambda, Glue, etc)"""
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._ddb = boto3.resource("dynamodb", region_name=self._region)
        self._table = self._ddb.Table(self._table_name)

    def provide_runtime_trusted_entities(self) -> List[str]:
        return []
        # return ["dynamodb.amazonaws.com", "dynamodb.application-autoscaling.amazonaws.com"]

    def provide_runtime_default_policies(self) -> List[str]:
        return []

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        return [
            ConstructPermission(
                [
                    f"arn:aws:dynamodb:{self._region}:{self._account_id}:table/{self._table_name}*",
                ],
                [
                    "dynamodb:Describe*",
                    "dynamodb:List*",
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:PutItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:BatchWriteItem",
                    "dynamodb:BatchGetItem",
                ],
            ),
        ]

    # overrides
    @classmethod
    def provide_devtime_permissions_ext(
        cls, extension_desc: "Extension.Descriptor", params: ConstructParamsDict
    ) -> List[ConstructPermission]:
        region: str = params[AWSCommonParams.REGION]
        account_id: str = params[AWSCommonParams.ACCOUNT_ID]
        context_id: str = params[ActivationParams.CONTEXT_ID].lower()
        table_name = extension_desc.table_name
        if table_name is None:
            table_name = cls.generate_default_table_name(extension_desc.extension_id, context_id, region)

        return [
            ConstructPermission(
                [
                    # add asterisk to support table sub-resources such as indexes (for Query operation)
                    # e.g arn:aws:dynamodb:<REGION>:<ACCOUNT>:table/<TABLE_NAME>/index/compute_trigger_index
                    f"arn:aws:dynamodb:{region}:{account_id}:table/{table_name}*",
                ],
                ["dynamodb:*"],
            )
        ]

    # overrides
    @classmethod
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        return []

    # overrides
    def _provide_system_metrics(self) -> List[Signal]:
        """Expose system generated metrics to the rest of the platform in a consolidated, filtered and
        well-defined IntelliFlow metric signal format.

        Refer
            https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/metrics-dimensions.html

        for remarks on supported periods and statistics.

        At application level, these metrics will be retrieved using the "alias'" of each signal provided here.
        Each metric signal here actually represents a metric group, because its dimension_filter contains
        all of the possible metric names, etc. So users will retrieve these signals and then specific final
        metric names, statistic, etc on them.

        E.g

        ```
        system_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.SYSTEM)
        extension_metrics_map = system_metrics_map[CompositeExtension]

        # this is where the "alias" generated below is used
        table_metric_signal = extension_metrics_map['myDDBExtensionTable']

        metric_that_can_be_an_alarmn_input= table_metric_signal['WriteThrottleEvents'][MetricStatistic.SUM][MetricPeriod.MINUTES(15)]
        ```
        """
        table_names_and_alias = {
            self._table_name: f"{self.desc.extension_id.lower()}Table",
        }
        # where dimension is Type='count'
        table_metrics_and_supported_statistics = {
            "ConsumedWriteCapacityUnits": ["*"],
            # [MetricStatistic.MINIMUM, MetricStatistic.MAXIMUM, MetricStatistic.SUM, MetricStatistic.AVERAGE, MetricStatistic.SAMPLE_COUNT],
            "ConsumedReadCapacityUnits": ["*"],
            "ProvisionedWriteCapacityUnits": ["*"],
            "ProvisionedReadCapacityUnits": ["*"],
            "TransactionConflict": ["*"],
            "ReadThrottleEvents": ["*"],
            "WriteThrottleEvents": ["*"]
            # TODO enable when default CW dashboard max widget limit issue is handled
            # "ConditionalCheckFailedRequests": []
        }

        supported_operations = ["BatchWriteItem", "DeleteItem", "GetItem", "PutItem", "Query", "Scan"]
        operation_metrics_and_supported_statistics = {"SuccessfulRequestLatency": ["*"], "ThrottledRequests": ["*"]}

        return [
            Signal(
                SignalType.CW_METRIC_DATA_CREATION,
                CWMetricSignalSourceAccessSpec(
                    "AWS/DynamoDB",
                    {"TableName": table_name},
                    # metadata (should be visible in front-end as well)
                    **{"Notes": "Supports 1 min period"},
                ),
                SignalDomainSpec(
                    dimension_filter_spec=DimensionFilter.load_raw(
                        {
                            metric_name: {  # Name  (overwrite in filter spec to make them visible to the user. otherwise user should specify the name,
                                #          in that case platform abstraction is broken since user should have a very clear idea about what is
                                #          providing these metrics).
                                statistic: {  # Statistic
                                    "*": {  # Period  (AWS emits with 1 min by default), let user decide.
                                        "*": {}  # Time  always leave it 'any' if not experimenting.
                                    }
                                }
                                for statistic in table_metrics_and_supported_statistics[metric_name]
                            }
                            for metric_name in table_metrics_and_supported_statistics.keys()
                        }
                    ),
                    dimension_spec=None,
                    integrity_check_protocol=None,
                ),
                # make sure that default metric alias/ID complies with CW expectation (first letter lower case).
                table_alias,
            )
            for table_name, table_alias in table_names_and_alias.items()
        ] + [
            Signal(
                SignalType.CW_METRIC_DATA_CREATION,
                CWMetricSignalSourceAccessSpec(
                    "AWS/DynamoDB",
                    {"TableName": table_name, "Operation": operation},
                    # metadata (should be visible in front-end as well)
                    **{"Notes": "Supports 1 min period"},
                ),
                SignalDomainSpec(
                    dimension_filter_spec=DimensionFilter.load_raw(
                        {
                            metric_name: {  # Name  (overwrite in filter spec to make them visible to the user. otherwise user should specify the name,
                                #          in that case platform abstraction is broken since user should have a very clear idea about what is
                                #          providing these metrics).
                                statistic: {  # Statistic
                                    "*": {  # Period  (AWS emits with 1 min by default), let user decide.
                                        "*": {}  # Time  always leave it 'any' if not experimenting.
                                    }
                                }
                                for statistic in operation_metrics_and_supported_statistics[metric_name]
                            }
                            for metric_name in operation_metrics_and_supported_statistics.keys()
                        }
                    ),
                    dimension_spec=None,
                    integrity_check_protocol=None,
                ),
                # make sure that default metric alias/ID complies with CW expectation (first letter lower case).
                f"{table_alias}.{operation}",
            )
            for table_name, table_alias in table_names_and_alias.items()
            for operation in supported_operations
        ]

    def _create_table(self):
        table = exponential_retry(
            create_table,
            ["LimitExceededException", "InternalServerError"],
            ddb_resource=self._ddb,
            table_name=self._table_name,
            key_schema=self.desc.key_schema,
            attribute_def=self.desc.attribute_def,
            provisioned_throughput=None,  # enforcing PAY_PER_REQUEST / ON_DEMAND
            billing_mode=self.desc.billing_mode,
            **self.desc.extension_params,
        )
        return table

    def _update_table(self):
        table = exponential_retry(
            update_table,
            ["LimitExceededException", "InternalServerError"],
            ddb_table=self._table,
            attribute_def=self.desc.attribute_def,
            provisioned_throughput=None,  # enforcing PAY_PER_REQUEST / ON_DEMAND
            billing_mode=self.desc.billing_mode,
            **self.desc.extension_params,
        )
        return table

    def activate(self) -> None:
        """Platform is being activated, now it is time to provision the resources and setup all necessary wiring."""
        try:
            self._create_table()
        except ClientError as error:
            if error.response["Error"]["Code"] == "ResourceInUseException":
                self._update_table()
            else:
                module_logger.error(f"An error occurred while trying to create/reset {self._table_name}")
                raise

        update_ttl(self._table, self.desc.ttl_attribute_name)

        super().activate()

    def rollback(self) -> None:
        super().rollback()

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)
        # TODO

    def hook_security_conf(
        self, security_conf: ConstructSecurityConf, platform_security_conf: Dict[Type["BaseConstruct"], ConstructSecurityConf]
    ) -> None:
        super().hook_security_conf(security_conf, platform_security_conf)

    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        pass

    def _revert_internal(self, routes: Set[Route], prev_routes: Set[Route]) -> None:
        pass

    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
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

    def _revert_construct_connections(
        self, construct_conns: Set["_PendingConnRequest"], prev_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _revert_internal_signals(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        # TODO
        pass

    def _revert_security_conf(selfs, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        pass
