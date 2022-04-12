# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import uuid
from datetime import datetime
from typing import ClassVar, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Type, Union, cast

import boto3
from botocore.exceptions import ClientError

from intelliflow.core.platform.definitions.aws.cw.client_wrapper import put_composite_alarm
from intelliflow.core.platform.definitions.aws.cw.dashboard import (
    CW_DASHBOARD_WIDTH_MAX,
    create_alarm_status_widget,
    create_metric_widget,
    create_text_widget,
)
from intelliflow.core.platform.definitions.aws.sns.client_wrapper import find_subscription
from intelliflow.core.platform.definitions.common import ActivationParams
from intelliflow.core.signal_processing import Signal
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import (
    AlarmDimension,
    AlarmRule,
    AlarmRuleOperator,
    AlarmState,
    CompositeAlarmParams,
    MetricDimension,
    MetricExpression,
    MetricStatisticData,
    MetricStatType,
    MetricValueCountPairData,
)
from intelliflow.core.signal_processing.dimension_constructs import AnyVariant, DimensionVariant
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal import SignalType
from intelliflow.core.signal_processing.signal_source import (
    CWAlarmSignalSourceAccessSpec,
    CWCompositeAlarmSignalSourceAccessSpec,
    CWMetricSignalSourceAccessSpec,
    InternalAlarmSignalSourceAccessSpec,
    InternalCompositeAlarmSignalSourceAccessSpec,
    InternalDatasetSignalSourceAccessSpec,
    InternalMetricSignalSourceAccessSpec,
    MetricSignalSourceAccessSpec,
    SignalSourceAccessSpec,
    SignalSourceType,
)

from ...constructs import (
    BaseConstruct,
    ConstructInternalMetricDesc,
    ConstructParamsDict,
    ConstructPermission,
    ConstructPermissionGroup,
    ConstructSecurityConf,
    Diagnostics,
    EncryptionKeyAllocationLevel,
    UpstreamGrants,
    UpstreamRevokes,
)
from ...definitions.aws.aws_lambda.client_wrapper import add_permission, remove_permission
from ...definitions.aws.common import CommonParams as AWSCommonParams
from ...definitions.aws.common import exponential_retry, generate_statement_id
from ...definitions.aws.kms.client_wrapper import (
    KMS_MIN_DELETION_WAITING_PERIOD_IN_DAYS,
    create_alias,
    create_cmk,
    create_default_policy,
    delete_alias,
    enable_key_rotation,
    get_cmk,
    put_cmk_policy,
    schedule_cmk_deletion,
    update_alias,
)
from ...definitions.aws.s3.bucket_wrapper import (
    bucket_exists,
    create_bucket,
    delete_bucket,
    get_bucket,
    put_notification,
    put_policy,
    update_policy,
)
from ...definitions.aws.s3.object_wrapper import (
    build_object_key,
    delete_objects,
    empty_bucket,
    get_object,
    list_objects,
    object_exists,
    put_object,
)
from ..aws_common import AWSConstructMixin

module_logger = logging.getLogger(__name__)


class AWSCloudWatchDiagnostics(AWSConstructMixin, Diagnostics):
    INTERNAL_ALARM_NAME_PREFIX: ClassVar[str] = "if-{0}-"
    TOPIC_NAME_FORMAT: ClassVar[str] = "if-{0}-{1}"
    """Diagnostics hub impl based on CloudWatch.

    Trade-offs:

        Pros:

        Cons:
    """

    def __init__(self, params: ConstructParamsDict) -> None:
        super().__init__(params)
        self._cw = self._session.client(service_name="cloudwatch", region_name=self._region)
        self._sns = self._session.client(service_name="sns", region_name=self._region)
        self._topic_name = None
        self._topic_arn = None
        self._topic_root_policy = None

    def get_event_channel_type(self) -> str:
        return "SNS"

    def get_event_channel_resource_path(self) -> str:
        return self._topic_arn

    def _get_internal_alarm_name_prefix(self) -> str:
        unique_context_id = self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]
        return self.INTERNAL_ALARM_NAME_PREFIX.format(unique_context_id)

    # overrides
    def get_unique_internal_alarm_name(self, alarm_name: str) -> str:
        """To guarantee that a given alarm_name for an internal alarm will have no conflict within the same account,
        we use context_uuid as a prefix to make it unique.
        """
        return self._get_internal_alarm_name_prefix() + alarm_name

    def map_incoming_event(self, source_type: SignalSourceType, resource_path: str) -> Optional[SignalSourceAccessSpec]:
        """Map an external diagnostics event to its internal representation (if it is actually governed by this driver).
        This is necessary for the abstraction of diagnostics impl from routing where internal signals are always
        kept in generic internal format.

        Returns a new access spec with the internal version of the 'source_type' and the internal representation of
        the resource_path (if different). If the external event does not map to an internal signal, then this method
        return None.
        """
        if source_type == SignalSourceType.CW_METRIC:
            context_id = MetricSignalSourceAccessSpec.extract_context_id(resource_path)
            if context_id == self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]:
                return SignalSourceAccessSpec(SignalSourceType.INTERNAL_METRIC, resource_path, None)
        elif source_type == SignalSourceType.CW_ALARM:
            cw_alarm_spec = CWAlarmSignalSourceAccessSpec.from_resource_path(resource_path)
            if cw_alarm_spec.account_id == self.account_id and cw_alarm_spec.region_id == self.region:
                if cw_alarm_spec.name.startswith(self._get_internal_alarm_name_prefix()):
                    mapped_resource_path = cw_alarm_spec.name.replace(self._get_internal_alarm_name_prefix(), "") + resource_path.replace(
                        cw_alarm_spec.arn, ""
                    )
                    return SignalSourceAccessSpec(SignalSourceType.INTERNAL_ALARM, mapped_resource_path, None)
        elif source_type == SignalSourceType.CW_COMPOSITE_ALARM:
            cw_alarm_spec = CWCompositeAlarmSignalSourceAccessSpec.from_resource_path(resource_path)
            if cw_alarm_spec.account_id == self.account_id and cw_alarm_spec.region_id == self.region:
                if cw_alarm_spec.name.startswith(self._get_internal_alarm_name_prefix()):
                    mapped_resource_path = (
                        InternalCompositeAlarmSignalSourceAccessSpec.PATH_PREFIX
                        + InternalAlarmSignalSourceAccessSpec.path_delimiter()
                        + cw_alarm_spec.name.replace(self._get_internal_alarm_name_prefix(), "")
                        + resource_path.replace(cw_alarm_spec.arn, "")
                    )
                    return SignalSourceAccessSpec(SignalSourceType.INTERNAL_COMPOSITE_ALARM, mapped_resource_path, None)
        return None

    def map_external_access_spec(self, access_spec: Optional[SignalSourceAccessSpec] = None) -> Optional[SignalSourceAccessSpec]:
        """Map an external diagnostic signal to its internal version (if it is actually internal and governed by
        this driver)."""
        if access_spec.source == SignalSourceType.CW_METRIC:
            if access_spec.context_id == self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]:
                return InternalMetricSignalSourceAccessSpec(access_spec.sub_dimensions, **access_spec.attrs)
        elif access_spec.source == SignalSourceType.CW_ALARM:
            if access_spec.account_id == self.account_id and access_spec.region_id == self.region:
                if access_spec.name.startswith(self._get_internal_alarm_name_prefix()):
                    mapped_id = access_spec.name.replace(self._get_internal_alarm_name_prefix(), "")
                    return InternalAlarmSignalSourceAccessSpec(mapped_id, access_spec.alarm_params, **access_spec.attrs)
        elif access_spec.source == SignalSourceType.CW_COMPOSITE_ALARM:
            if access_spec.account_id == self.account_id and access_spec.region_id == self.region:
                if access_spec.name.startswith(self._get_internal_alarm_name_prefix()):
                    mapped_id = access_spec.name.replace(self._get_internal_alarm_name_prefix(), "")
                    return InternalCompositeAlarmSignalSourceAccessSpec(mapped_id, access_spec.alarm_params, **access_spec.attrs)
        return None

    def map_internal_access_spec(self, data_access_spec: SignalSourceAccessSpec) -> SignalSourceAccessSpec:
        """Map an internal diagnostic signal to its external/raw version"""
        if data_access_spec.source == SignalSourceType.INTERNAL_ALARM:
            return CWAlarmSignalSourceAccessSpec(
                self.get_unique_internal_alarm_name(data_access_spec.alarm_id),
                self.account_id,
                self.region,
                data_access_spec.alarm_params,
                **data_access_spec.attrs,
            )
        elif data_access_spec.source == SignalSourceType.INTERNAL_COMPOSITE_ALARM:
            return CWAlarmSignalSourceAccessSpec(
                self.get_unique_internal_alarm_name(data_access_spec.alarm_id),
                self.account_id,
                self.region,
                data_access_spec.alarm_params,
                **data_access_spec.attrs,
            )
        elif data_access_spec.source == SignalSourceType.INTERNAL_METRIC:
            return CWMetricSignalSourceAccessSpec(data_access_spec.context_id, data_access_spec.sub_dimensions, **data_access_spec.attrs)
        else:
            raise ValueError(
                f"Input diagnostic data access spec is not of type Internal! Problematic access spec: " f" {data_access_spec!r}"
            )

    def map_internal_signal(self, signal: Signal) -> Signal:
        mapped_resource_spec = self.map_internal_access_spec(signal.resource_access_spec)
        signal_type = None
        if signal.resource_access_spec.source in [SignalSourceType.INTERNAL_ALARM, SignalSourceType.INTERNAL_COMPOSITE_ALARM]:
            signal_type = SignalType.CW_ALARM_STATE_CHANGE
        elif signal.resource_access_spec.source == SignalSourceType.INTERNAL_METRIC:
            signal_type = SignalType.CW_METRIC_DATA_CREATION
        return Signal(
            signal_type,
            mapped_resource_spec,
            signal.domain_spec,
            signal.alias,
            signal.is_reference,
            signal.range_check_required,
            signal.nearest_the_tip_in_range,
            signal.is_termination,
            signal.is_inverted,
        )

    def _do_emit(
        self,
        metric_signal: Signal,
        value: Union[float, MetricStatisticData, List[Union[float, MetricValueCountPairData]]],
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Refer
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html#CloudWatch.Client.put_metric_data

        - dynamic handling of 'StorageResolution' based on the period value of the metric to be emitted. If it is
        less than 60 seconds, set it as 1 or 60. Also, if it is not specified, then we default to 1.
        - Use 'alias' as the MetricName if left as '*'.
        """
        period = metric_signal.domain_spec.dimension_filter_spec.find_dimension_by_name(MetricDimension.PERIOD).value
        storage_resolution = 60
        if period == AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR or period < 60:
            storage_resolution = 1

        metric_access_spec = cast("MetricSignalSourceAccessSpec", metric_signal.resource_access_spec)
        # Metric emission in IF chooses the first materialized path.
        metric_stat: MetricStatType = metric_access_spec.create_stats_from_filter(metric_signal.domain_spec.dimension_filter_spec)[0]

        metric_data = {
            "MetricName": metric_stat["Metric"]["MetricName"],
            "Dimensions": metric_stat["Metric"]["Dimensions"],
            "Timestamp": timestamp if timestamp else datetime.utcnow(),
            # RheocerOS does not use Unit !
            "Unit": "None",
            "StorageResolution": storage_resolution,
        }

        if isinstance(value, float) or isinstance(value, int):
            metric_data.update({"Value": float(value)})
        elif isinstance(value, MetricStatisticData):
            metric_data.update(
                {
                    "StatisticValues": {
                        "SampleCount": float(value.SampleCount),
                        "Sum": float(value.Sum),
                        "Minimum": float(value.Minimum),
                        "Maximum": float(value.Maximum),
                    }
                }
            )
        elif isinstance(value, list):
            if not value or any([not isinstance(pair, (MetricValueCountPairData, float)) for pair in value]):
                raise ValueError(
                    f"Cannot emit metric {metric_signal.alias!r}! Value list should not be empty and "
                    f"it should contain only entities of type float or "
                    f"{MetricValueCountPairData.__class__.__name__!r}"
                )
            metric_data.update(
                {
                    "Values": [float(val) if isinstance(val, float) or isinstance(val, int) else val.Value for val in value],
                    "Counts": [1.0 if isinstance(val, float) or isinstance(val, int) else val.Count for val in value],
                }
            )
        else:
            raise ValueError(f"Value type {type(value)!r} is not supported for metric emission! MetricData={metric_data!r}")

        exponential_retry(
            self._cw.put_metric_data, {"InternalServiceFault"}, Namespace=metric_stat["Metric"]["Namespace"], MetricData=[metric_data]
        )

    def is_internal(self, source_type: SignalSourceType, resource_path: str) -> bool:
        if source_type == SignalSourceType.CW_METRIC:
            context_id = MetricSignalSourceAccessSpec.extract_context_id(resource_path)
            if context_id == self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]:
                return True
        elif source_type in SignalSourceType.CW_ALARM:
            cw_alarm_spec = CWAlarmSignalSourceAccessSpec.from_resource_path(resource_path)
            if cw_alarm_spec.account_id == self.account_id and cw_alarm_spec.region_id == self.region:
                if cw_alarm_spec.name.startswith(self._get_internal_alarm_name_prefix()):
                    return True
        elif source_type in SignalSourceType.CW_COMPOSITE_ALARM:
            cw_alarm_spec = CWCompositeAlarmSignalSourceAccessSpec.from_resource_path(resource_path)
            if cw_alarm_spec.account_id == self.account_id and cw_alarm_spec.region_id == self.region:
                if cw_alarm_spec.name.startswith(self._get_internal_alarm_name_prefix()):
                    return True

        return False

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)
        self._topic_name = self.TOPIC_NAME_FORMAT.format(self._dev_platform.context_id, self.__class__.__name__)
        self._topic_arn = f"arn:aws:sns:{self._region}:{self._account_id}:{self._topic_name}"

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._cw = self._session.client(service_name="cloudwatch", region_name=self._region)
        self._sns = self._session.client(service_name="sns", region_name=self._region)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._cw = None
        self._sns = None

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        """Whole platform got bootstrapped at runtime. For other runtime services, this
        construct should be initialized (ex: context_owner: Lambda, Glue, etc)"""
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._cw = boto3.client(service_name="cloudwatch", region_name=self._region)
        self._sns = boto3.client(service_name="sns", region_name=self._region)

    def provide_runtime_trusted_entities(self) -> List[str]:
        # Is there any scenario when any of the AWS services (s3) from this impl should assume our exec role?
        # No
        return []

    def provide_runtime_default_policies(self) -> List[str]:
        return []

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        # allow exec-role (post-activation, cumulative list of all trusted entities [AWS services]) to do the following;
        permissions = [
            ConstructPermission(
                [self._topic_arn],
                # our exec-role might need to reply back to SNS once a token is received at runtime
                # following a 'subscribe' call.
                ["sns:ConfirmSubscription", "sns:Receive"],
            ),
            # no need to be picky about resources here since the only action that requires 'resource type' is
            # DescribeAlarmHistory and we are ok to allow '*' with it.
            #  ref
            #   https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazoncloudwatch.html
            ConstructPermission(["*"], ["cloudwatch:DescribeAlarmHistory", "cloudwatch:PutMetricData", "cloudwatch:GetMetricData"]),
        ]

        return permissions

    @classmethod
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        # dev-role should be able to do the following.
        return [
            ConstructPermission(["*"], ["sns:*"]),
            ConstructPermission(
                ["*"],
                [
                    "cloudwatch:DeleteAlarms",
                    "cloudwatch:DescribeAlarmHistory",
                    "cloudwatch:DescribeAlarms",
                    "cloudwatch:PutCompositeAlarm",
                    "cloudwatch:PutMetricAlarm",
                    "cloudwatch:PutMetricData",  # particular for testing (integ-tests, etc)
                    "cloudwatch:GetMetricData",
                    "cloudwatch:TagResource",
                    "cloudwatch:PutDashboard",
                ],
            ),
        ]

    def _provide_route_metrics(self, route: Route) -> List[ConstructInternalMetricDesc]:
        # TODO
        return []

    def _provide_internal_metrics(self) -> List[ConstructInternalMetricDesc]:
        """Provide internal metrics (of type INTERNAL_METRIC) that should be managed by RheocerOS and emitted by this
        driver via Diagnostics::emit.
        These metrics are logical metrics generated by the driver (with no assumption on other drivers and other details
        about the underlying platform). So as a driver impl, you want Diagnostics driver to manage those metrics and
        bind them to alarms, etc. Example: Routing metrics.
        """
        return []

    def _provide_internal_alarms(self) -> List[Signal]:
        """Provide internal alarms (of type INTERNAL_ALARM OR INTERNAL_COMPOSITE_ALARM) managed/emitted
        by this driver impl"""
        return []

    def _provide_system_metrics(self) -> List[Signal]:
        """Provide metrics auto-generated by the underlying system components/resources. These metrics types are
        explicit and internally represented as 'external' metric types such as CW_METRIC, etc."""
        return []

    # overrides
    def hook_internal(self, route: "Route") -> None:
        # not interested in data routes, should keep a track
        pass

    def hook_internal_signal(self, signal: "Signal") -> None:
        """A change to do the early validations for better user-exp before activation (before resources provisioned).
        These specific validations can only be performed here since they are impl based and apply expectations from
        AWS CW APIs mostly.
        """
        super().hook_internal_signal(signal)

        if signal.resource_access_spec.source == SignalSourceType.INTERNAL_ALARM:
            # validate for user defined / NEW / internal alarms metrics all have the same period
            alarm_params = signal.resource_access_spec.alarm_params
            if isinstance(alarm_params.target_metric_or_expression, MetricExpression):
                id: str = alarm_params.target_metric_or_expression.alias
                if id is not None and (not id or not id[0].islower()):
                    raise ValueError(
                        f"Alias {id!r} for the target expression of alarm {signal.alias!r} should not be empty and its "
                        f"first character must be lower case. Problematic expression: {alarm_params.target_metric_or_expression!r}"
                    )

                # check if all of the metrics share the same period!
                # please note that we don't need to check for materialization of the dimensions here since such a
                # generic check is supposed to be done by application front-end and also in framework core already.
                metric_periods: Dict[str, int] = dict()
                for metric_signal in alarm_params.metric_signals:
                    if not metric_signal.alias or not metric_signal.alias[0].islower():
                        raise ValueError(
                            f"Alias {metric_signal.alias!r} for input metric of alarm {signal.alias!r} should not be empty and its "
                            f"first character must be lower case. Problematic metric: {metric_signal!r}"
                        )
                    metric_periods[metric_signal.alias] = metric_signal.domain_spec.dimension_filter_spec.find_dimension_by_name(
                        MetricDimension.PERIOD
                    ).value
                if len(set(metric_periods.values())) > 1:
                    raise ValueError(
                        f"All metrics in the alarm {signal.alias!r} should have the same period!\n" f"Metric periods: \n{metric_periods!r}"
                    )

                for expression in alarm_params.metric_expressions:
                    if not expression.alias or not expression.alias[0].islower():
                        raise ValueError(
                            f"Alias {expression.alias!r} for intermediate metric expression of alarm {signal.alias!r} should not be empty and its "
                            f"first character must be lower case. Problematic expression: {alarm_params.target_metric_or_expression!r}"
                        )

    @classmethod
    def _generate_cw_alarm_to_sns_publish_permissin_label(cls, alarm_acc_id: str, alarm_region_id: str, alarm_name: str):
        return generate_statement_id(f"{alarm_acc_id}_{alarm_region_id}_{alarm_name}_external_alarm_publish")

    # overrides
    def _process_raw_external_alarms(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        """Register own topic as an action to this alarm from the same or an different account.
        Skips upstream external alarms. Processor should be able to register them directly. We are avoiding an extra
        hop in connection from upstream down to the Processor. And that direct connection is guaranteed to work.

        Normally Processor can register to both but:
            - Processor registration to an alarm is not guaranteed (ex: Lambda registration for CW composite alarms)
            - And there is too much alarming tech related details involved for which Diagnostics impl would be the right
        place for encapsulation.

        So for external (non IF-governed) Alarms, Diagnostics driver is taking the responsibility.
        """
        processed_resources: Set[str] = {s.resource_access_spec.path_format for s in current_signals}
        new_processed_resources: Set[str] = {s.resource_access_spec.path_format for s in new_signals}

        resources_to_be_deleted = processed_resources - new_processed_resources

        # 1 - REMOVALS
        for ext_signal in current_signals:
            removed_alarm = ext_signal.resource_access_spec.path_format
            if removed_alarm in resources_to_be_deleted:
                cw = self._get_session_for(ext_signal, self._dev_platform).client(
                    service_name="cloudwatch", region_name=ext_signal.resource_access_spec.region_id
                )
                # 1- Remove this SNS from external alarm's action lists
                try:
                    # best effort (auto-disconnection) action against external alarm (not owned by current plat/app)
                    cw_alarm_resp = exponential_retry(cw.describe_alarms, {}, AlarmNames=[ext_signal.resource_access_spec.name])
                    alarm_type, alarms = next(iter(cw_alarm_resp.items()))
                    alarm = alarms[0]
                    # remove self._topic_arn from action lists.
                    alarm_actions = alarm["AlarmActions"]
                    ok_actions = alarm["OKActions"]
                    insuffficient_data_actions = alarm["InsufficientDataActions"]
                    changed = False
                    if self._topic_arn in alarm_actions:
                        alarm_actions.remove(self._topic_arn)
                        changed = True
                    if self._topic_arn in insuffficient_data_actions:
                        insuffficient_data_actions.remove(self._topic_arn)
                        changed = True
                    if self._topic_arn in ok_actions:
                        ok_actions.remove(self._topic_arn)
                        changed = True
                    if changed:
                        if alarm_type == "CompositeAlarms":
                            exponential_retry(cw.put_composite_alarm, {}, **alarm)
                        elif alarm_type == "MetricAlarms":
                            exponential_retry(cw.put_metric_alarm, {}, **alarm)
                except Exception as err:
                    # swallow so that the following actions can be taken by the developer to unblock the app.
                    module_logger.critical(
                        f"RheocerOS could not update alarm actions for the removal of SNS: {self._topic_arn}"
                        f" due to {str(err)}. Please manually make sure that external alarm {ext_signal.resource_access_spec.arn}"
                        f" does not have this topic in its alarm actions."
                    )

                # 2- Remove external alarm from SNS policy (remove publish permission)
                permission_label: str = self._generate_cw_alarm_to_sns_publish_permissin_label(
                    ext_signal.resource_access_spec.account_id, ext_signal.resource_access_spec.region_id, ext_signal.name
                )

                try:
                    exponential_retry(
                        self._sns.remove_permission, {"InternalErrorException"}, TopicArn=self._topic_arn, Label=permission_label
                    )
                except self._sns.exceptions.NotFoundException:
                    pass

        # 2 - SURVIVORS + NEW ONES (always doing the check/update even against the existing [common] signals)
        for ext_signal in new_signals:
            # 1- add permission to publish to this SNS topic
            # TODO replace the following (acc based) permission control code with a more granular policy based version.
            # similar to what we do in '_setup_event_channel' using 'self._topic_root_policy', and then
            # calling 'self._sns.set_topic_attributes'
            # AWSAccountId based permission won't work since we need to authorize the service:
            #   "Principal": {"Service": "cloudwatch.amazonaws.com"},
            # again similar to what we do in _setup_event_channel.
            # So in stead of manipulating permissions in this method, we can move extract the policy update logic from
            # _setup_event_channel and add external (processed) signals to the policy at the end of this method.
            permission_label: str = self._generate_cw_alarm_to_sns_publish_permissin_label(
                ext_signal.resource_access_spec.account_id, ext_signal.resource_access_spec.region_id, ext_signal.name
            )
            try:
                exponential_retry(self._sns.remove_permission, {"InternalErrorException"}, TopicArn=self._topic_arn, Label=permission_label)
            except self._sns.exceptions.NotFoundException:
                pass

            exponential_retry(
                self._sns.add_permission,
                {"InternalErrorException"},
                TopicArn=self._topic_arn,
                Label=permission_label,
                AWSAccountId=[ext_signal.resource_access_spec.account_id],
                ActionName=["Publish"],
            )

            # 2- register this topic to external raw alarm's lists.
            cw = self._get_session_for(ext_signal, self._dev_platform).client(
                service_name="cloudwatch", region_name=ext_signal.resource_access_spec.region_id
            )
            try:
                cw_alarm_resp = exponential_retry(cw.describe_alarms, {}, AlarmNames=[ext_signal.resource_access_spec.name])
                alarm_type, alarms = next(iter(cw_alarm_resp.items()))
                alarm = alarms[0]
                # add self._topic_arn to action lists.
                alarm_actions = alarm["AlarmActions"]
                ok_actions = alarm["OKActions"]
                insuffficient_data_actions = alarm["InsufficientDataActions"]
                changed = False
                if self._topic_arn not in alarm_actions:
                    alarm_actions.append(self._topic_arn)
                    changed = True
                if self._topic_arn not in insuffficient_data_actions:
                    insuffficient_data_actions.append(self._topic_arn)
                    changed = True
                if self._topic_arn not in ok_actions:
                    ok_actions.append(self._topic_arn)
                    changed = True
                if changed:
                    if alarm_type == "CompositeAlarms":
                        # - add self._topic_arn to action lists.
                        exponential_retry(put_composite_alarm, {}, cw, **alarm)
                    elif alarm_type == "MetricAlarms":
                        exponential_retry(cw.put_metric_alarm, {}, **alarm)
            except Exception as err:
                # swallow so that the following actions can be taken by the developer to unblock the app.
                module_logger.critical(
                    f"RheocerOS could update alarm actions for alarm {ext_signal.resource_access_spec.arn} to "
                    f"register SNS topic {self._topic_arn} due to {str(err)}. Please make sure that this external "
                    f"alarm contains this topic in its alarm actions."
                )

    # overrides
    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        # for connections, it is expected that other constructs should send a conn request
        # to this Diagnostics impl (see _process_construct_connections).
        # according to the needs of their underlying (AWS) resources.
        #
        # But for this Diagnostics impl, we don't need to track internal routes.
        #
        pass

    def _remove_internal_alarm(self, int_signal: Signal, is_terminating=False) -> None:
        # 1- Remove the alarm from account
        internal_alarm_name = self.get_unique_internal_alarm_name(int_signal.resource_access_spec.alarm_id)
        try:
            exponential_retry(self._cw.delete_alarms, {}, AlarmNames=[internal_alarm_name])
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFound", "ResourceNotFoundException"]:
                raise

        # 2- Remove external alarm from SNS policy (remove publish permission)
        permission_label: str = self._generate_cw_alarm_to_sns_publish_permissin_label(self.account_id, self.region, internal_alarm_name)

        if not is_terminating:
            # OPTIMIZATION: ignore the state in the topic since it must also be going down as part of the termination.
            try:
                exponential_retry(self._sns.remove_permission, {"InternalErrorException"}, TopicArn=self._topic_arn, Label=permission_label)
            except self._sns.exceptions.NotFoundException:
                pass

    # overrides
    def _process_internal_alarms(
        self, new_alarms: Set[Signal], current_alarms: Set[Signal], resource_paths_to_be_deleted: Set[str]
    ) -> None:
        # no need to check alarm inputs type (metric for metric alarms and alarms for composite alarms)
        # and their materialization. Application layer makes sure that compilation/front-end takes care of
        # necessary validations.
        # Logic:
        # - create/update alarms as CW metric alarms or composite alarms.
        # - register self._topic_arn to their action lists

        # 1 - REMOVALS
        # 1.1- Remove Composite alarms first (hard condition in case they might depend on some of the metric alarms
        # which are also to be deleted)
        for int_signal in current_alarms:
            if (
                int_signal.resource_access_spec.path_format in resource_paths_to_be_deleted
                and int_signal.resource_access_spec.source == SignalSourceType.INTERNAL_COMPOSITE_ALARM
            ):
                self._remove_internal_alarm(int_signal)

        # 1.2- Remove metric alarms
        for int_signal in current_alarms:
            if (
                int_signal.resource_access_spec.path_format in resource_paths_to_be_deleted
                and int_signal.resource_access_spec.source == SignalSourceType.INTERNAL_ALARM
            ):
                self._remove_internal_alarm(int_signal)

        # 2 - SURVIVORS + NEW ONES (always doing the check/update even against the existing [common] signals)
        for int_signal in new_alarms:
            internal_alarm_name = self.get_unique_internal_alarm_name(int_signal.resource_access_spec.alarm_id)
            # 1- add permission to publish to this SNS topic
            # TODO replace the following (acc based) permission control code with a more granular policy based version.
            # similar to what we do in '_setup_event_channel' using 'self._topic_root_policy', and then
            # calling 'self._sns.set_topic_attributes'
            permission_label: str = self._generate_cw_alarm_to_sns_publish_permissin_label(
                self.account_id, self.region, internal_alarm_name
            )
            try:
                exponential_retry(self._sns.remove_permission, {"InternalErrorException"}, TopicArn=self._topic_arn, Label=permission_label)
            except self._sns.exceptions.NotFoundException:
                pass

            exponential_retry(
                self._sns.add_permission,
                {"InternalErrorException"},
                TopicArn=self._topic_arn,
                Label=permission_label,
                AWSAccountId=[self.account_id],  # this is redundant but kept here until the policy based update (above TODO is handled)
                ActionName=["Publish"],
            )

        # create metric alarms first (composite alarm fails on validation if it depends on any of these new metric alarms).
        for int_signal in new_alarms:
            if int_signal.resource_access_spec.source == SignalSourceType.INTERNAL_ALARM:
                self._create_or_update_internal_alarm(int_signal)

        for int_signal in new_alarms:
            if int_signal.resource_access_spec.source == SignalSourceType.INTERNAL_COMPOSITE_ALARM:
                self._create_or_update_internal_alarm(int_signal)

    def _create_or_update_internal_alarm(self, int_alarm_signal: Signal) -> None:
        # Now create/update the Alarm!
        if isinstance(int_alarm_signal.resource_access_spec, InternalAlarmSignalSourceAccessSpec):
            self._create_or_update_internal_metric_alarm(int_alarm_signal)
        elif isinstance(int_alarm_signal.resource_access_spec, InternalCompositeAlarmSignalSourceAccessSpec):
            self._create_or_update_internal_composite_alarm(int_alarm_signal)
        else:
            raise ValueError(
                f"Internal alarm {int_alarm_signal.alias!r} is not supported by driver "
                f"{self.__class__.__name__!r} due to its unrecognized access spec "
                f"{int_alarm_signal.resource_access_spec.__class__.__name__!r}"
            )

    def _create_or_update_internal_metric_alarm(self, int_alarm_signal: Signal) -> None:
        """Refer
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html#CloudWatch.Client.put_metric_alarm

        Please note that at this point alarm must have passed all of the validations (generic validations in front
        end, core and hook_internal_signal within this driver).
        """
        internal_alarm_spec: InternalAlarmSignalSourceAccessSpec = int_alarm_signal.resource_access_spec
        alarm_params = internal_alarm_spec.alarm_params

        target_dict = {}
        if isinstance(alarm_params.target_metric_or_expression, MetricExpression):
            metrics_array = []
            id: str = alarm_params.target_metric_or_expression.alias
            if id is None:
                id = MetricExpression.DEFAULT_ALIAS

            metrics_array.append({"Id": id, "Expression": alarm_params.target_metric_or_expression, "ReturnData": True})

            for metric_signal in alarm_params.metric_signals:
                metric_stat_dict = {"Id": metric_signal.alias, "MetricStat": {}, "ReturnData": False}
                metric_access_spec = cast("MetricSignalSourceAccessSpec", metric_signal.resource_access_spec)
                metric_stat: MetricStatType = metric_access_spec.create_stats_from_filter(metric_signal.domain_spec.dimension_filter_spec)[
                    0
                ]
                metric_stat_dict["MetricStat"].update(metric_stat)
                metrics_array.append(metric_stat_dict)

            for expression in alarm_params.metric_expressions:
                metrics_array.append({"Id": id, "Expression": expression, "ReturnData": False})

            target_dict.update({"Metrics": metrics_array})
        else:  # Signal (target is single metric)
            metric_signal = alarm_params.target_metric_or_expression
            metric_access_spec = cast("MetricSignalSourceAccessSpec", metric_signal.resource_access_spec)
            metric_stat: MetricStatType = metric_access_spec.create_stats_from_filter(metric_signal.domain_spec.dimension_filter_spec)[0]

            target_dict.update(
                {
                    "MetricName": metric_stat["Metric"]["MetricName"],
                    "Namespace": metric_stat["Metric"]["Namespace"],
                    "Dimensions": metric_stat["Metric"]["Dimensions"],
                    "Statistic": metric_stat["Stat"],
                    # "ExtendedStatistic"= ,
                    "Period": metric_stat["Period"],
                    # RheocerOS does not use Unit !
                    "Unit": "None",
                }
            )

        exponential_retry(
            self._cw.put_metric_alarm,
            [],
            AlarmName=self.get_unique_internal_alarm_name(internal_alarm_spec.alarm_id),
            AlarmDescription=internal_alarm_spec.alarm_params.description if internal_alarm_spec.alarm_params.description else "",
            ActionsEnabled=True,
            # forward all of the actions to Diagnostics HUB / event-channel + default actions.
            OKActions=[self._topic_arn] + [action.uri(alarm_params) for action in alarm_params.default_actions.OK_ACTIONS],
            AlarmActions=[self._topic_arn] + [action.uri(alarm_params) for action in alarm_params.default_actions.ALARM_ACTIONS],
            InsufficientDataActions=[self._topic_arn]
            + [action.uri(alarm_params) for action in alarm_params.default_actions.INSUFFICIENT_DATA_ACTIONS],
            EvaluationPeriods=alarm_params.number_of_evaluation_periods,
            DatapointsToAlarm=alarm_params.number_of_datapoint_periods,
            Threshold=alarm_params.threshold,
            ComparisonOperator=alarm_params.comparison_operator.value,
            TreatMissingData=alarm_params.treat_missing_data.value,
            # EvaluateLowSampleCountPercentile='string',
            # TODO Pass this if alarm is new
            # Tags=[
            #    {
            #        'Key': 'IntelliFlow_Context_UUID',
            #        'Value': internal_alarm_spec.get_owner_context_uuid()
            #    }
            # ],
            **target_dict,
        )

    def _render_cw_alarm_rule(self, alarm_rule: Union[AlarmRule, "Signal"], nested: bool = False) -> str:
        """Recursively render the rule into AWS CloudWatch composite alarm alarm rule string."""
        cw_alarm_rule_part = ""
        if isinstance(alarm_rule, AlarmRule):
            if alarm_rule.inverted:
                cw_alarm_rule_part += "NOT "

            if nested:
                cw_alarm_rule_part += "("

            # LHS
            cw_alarm_rule_part += self._render_cw_alarm_rule(alarm_rule.lhs, nested=True)
            # OPERATOR
            if alarm_rule.operator == AlarmRuleOperator.AND:
                cw_alarm_rule_part += " AND "
            elif alarm_rule.operator == AlarmRuleOperator.OR:
                cw_alarm_rule_part += " OR "
            else:
                raise ValueError(f"Operator {alarm_rule.operator} is not supported for AWS CW Alarm Rule rendering!")
            # RHS
            cw_alarm_rule_part += self._render_cw_alarm_rule(alarm_rule.rhs, nested=True)

            if nested:
                cw_alarm_rule_part += ")"  # close inversion paranthesis
        else:  # Signal
            rule_alarm_signal: Signal = alarm_rule
            if rule_alarm_signal.is_inverted:
                cw_alarm_rule_part += "NOT "

            alarm_state_dimension: DimensionVariant = rule_alarm_signal.domain_spec.dimension_filter_spec.find_dimension_by_name(
                AlarmDimension.STATE_TRANSITION
            )
            # default to alarm if not specialized (materialized)
            current_alarm_state: str = alarm_state_dimension.value if alarm_state_dimension.is_material_value() else AlarmState.ALARM.value
            cw_alarm_rule_part += current_alarm_state
            cw_alarm_rule_part += "("
            # we use ALARM ARN
            if rule_alarm_signal.resource_access_spec.source in [
                SignalSourceType.INTERNAL_ALARM,
                SignalSourceType.INTERNAL_COMPOSITE_ALARM,
            ]:
                # internal alarm
                # use this platform's own AWS region, account id and mapped, unique alarm ID
                cw_alarm_rule_part += f"arn:aws:cloudwatch:{self.region}:{self.account_id}:alarm:{self.get_unique_internal_alarm_name(rule_alarm_signal.resource_access_spec.alarm_id)}"
            elif rule_alarm_signal.resource_access_spec.source in [SignalSourceType.CW_ALARM, SignalSourceType.CW_COMPOSITE_ALARM]:
                # TODO actually CW does not allow alarms from other accounts or regions to be bound to composite
                #   alarms. So we can proactively add a validation to make the API error more human readable.
                # external alarm
                account_id = rule_alarm_signal.resource_access_spec.account_id
                region_id = rule_alarm_signal.resource_access_spec.region_id
                # use the name "as is" for externals
                cw_alarm_rule_part += f"arn:aws:cloudwatch:{region_id}:{account_id}:alarm:{rule_alarm_signal.resource_access_spec.name}"
            else:
                raise ValueError(
                    f"Input signal (alias: {rule_alarm_signal.alias!r}, "
                    f"source : {rule_alarm_signal.resource_access_spec.source.value!r}) is not "
                    f"supported as a composite alarm input! Only alarms must be used as an input."
                )
            cw_alarm_rule_part += ")"

        return cw_alarm_rule_part

    def _create_or_update_internal_composite_alarm(self, int_alarm_signal: Signal) -> None:
        """Refer
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch.html#CloudWatch.Client.put_composite_alarm
        """
        internal_alarm_spec: InternalCompositeAlarmSignalSourceAccessSpec = int_alarm_signal.resource_access_spec
        alarm_params: CompositeAlarmParams = internal_alarm_spec.alarm_params

        exponential_retry(
            put_composite_alarm,
            {},
            self._cw,
            ActionsEnabled=True,
            AlarmName=self.get_unique_internal_alarm_name(internal_alarm_spec.alarm_id),
            AlarmDescription=alarm_params.description if alarm_params.description else "",
            AlarmRule=self._render_cw_alarm_rule(alarm_params.alarm_rule),
            # forward all of the actions to Diagnostics HUB / event-channel + default actions.
            OKActions=[self._topic_arn] + [action.uri(alarm_params) for action in alarm_params.default_actions.OK_ACTIONS],
            AlarmActions=[self._topic_arn] + [action.uri(alarm_params) for action in alarm_params.default_actions.ALARM_ACTIONS],
            InsufficientDataActions=[self._topic_arn]
            + [action.uri(alarm_params) for action in alarm_params.default_actions.INSUFFICIENT_DATA_ACTIONS]
            # TODO Pass this if alarm is new
            # , Tags=[
            #    {
            #        'Key': 'IntelliFlow_Context_UUID',
            #        'Value': internal_alarm_spec.get_owner_context_uuid()
            #    }
            # ]
        )

    # overrides
    def _process_construct_connections(
        self, new_construct_conns: Set["_PendingConnRequest"], current_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        """Got dev-time connection request from another Construct.
        That construct needs modifications in the resources that this construct encapsulates.
        Ex: from Lambda, to set up event notification from internal data S3 bucket.
        """
        lambda_arns: Set[str] = set()
        queue_arns: Set[str] = set()
        topic_arns: Set[str] = set()
        for conn in new_construct_conns:
            if conn.resource_type == "lambda":
                lambda_arns.add(conn.resource_path)
            elif conn.resource_type == "sns":
                topic_arns.add(conn.resource_path)
            elif conn.resource_type == "sqs":
                queue_arns.add(conn.resource_path)
            else:
                err: str = f"{self.__class__.__name__} driver cannot process the connection request: {conn!r}"
                module_logger.error(err)
                raise ValueError(err)

        # s3 notification setup requires target resources to be alive and with right permissions.
        # so let's setup client permissions first.
        lambda_client = self._session.client(service_name="lambda", region_name=self._region)
        for lambda_arn in lambda_arns:
            statement_id: str = self.__class__.__name__ + "_lambda_Invoke_Permission"
            # consider switching to following way of generating the statement id. this is not critical since within
            # same application/platform only this impl can retain this statement_id (upstream ones use the specific one)
            # generate_statement_id(f"{self.__class__.__name__}_{self._dev_platform.context_id}_internal_invoke")
            try:
                exponential_retry(
                    remove_permission,
                    {"InvalidParameterValueException", "ServiceException", "TooManyRequestsException"},
                    lambda_client,
                    lambda_arn,
                    statement_id,
                )
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                    raise

            # give permission to SNS to call this lambda resource owned by our platform.
            exponential_retry(
                add_permission,
                {"InvalidParameterValueException", "ServiceException", "TooManyRequestsException"},
                lambda_client,
                lambda_arn,
                statement_id,
                "lambda:InvokeFunction",
                "sns.amazonaws.com",
                self._topic_arn,
            )
            # WARNING: DO NOT use account_id for any other trigger than S3.
            # self._account_id)

            exponential_retry(
                self._sns.subscribe, {"InternalErrorException"}, TopicArn=self._topic_arn, Protocol="lambda", Endpoint=lambda_arn
            )

        #  TODO/FUTURE go over queue_arns and topic_arns

    def subscribe_downstream_resource(self, downstream_platform: "DevelopmentPlatform", resource_type: str, resource_path: str):
        if resource_type == "lambda":
            downstream_session = downstream_platform.conf.get_param(AWSCommonParams.BOTO_SESSION)
            downstream_region = downstream_platform.conf.get_param(AWSCommonParams.REGION)
            # alternatively the session can be retrieved as below:
            #  current platform (self._dev_platform) should be alive as an upstream platform within
            #  the dev context of the caller  downstream platform
            # downstream_session = self._dev_platform.conf.get_param(AWSCommonParams.HOST_BOTO_SESSION)
            lambda_client = downstream_session.client(
                service_name="lambda",
                # have to use downstream's own region (cannot use self._region)
                region_name=downstream_region,
            )
            lambda_arn = resource_path

            statement_id: str = generate_statement_id(f"{self.__class__.__name__}_{self._dev_platform.context_id}_upstream_invoke")
            try:
                exponential_retry(
                    remove_permission,
                    {"InvalidParameterValueException", "ServiceException", "TooManyRequestsException"},
                    lambda_client,
                    lambda_arn,
                    statement_id,
                )
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                    raise

            # give permission to SNS to call this lambda resource owned by our platform.
            exponential_retry(
                add_permission,
                {"InvalidParameterValueException", "ServiceException", "TooManyRequestsException"},
                lambda_client,
                lambda_arn,
                statement_id,
                "lambda:InvokeFunction",
                "sns.amazonaws.com",
                self._topic_arn,
            )

            # Setup against the downstream resource (Lambda) is complete.
            # Now start configuring upstream event-channel (SNS).
            downstream_account_id = downstream_platform.conf.get_param(AWSCommonParams.ACCOUNT_ID)
            if self._account_id != downstream_account_id:
                # 1 - check permission to downstream resource
                permission_label: str = generate_statement_id(
                    f"{self.__class__.__name__}_{downstream_platform.context_id}_downstream_invoke"
                )

                # TODO replace the following (acc based) permission control code with a more granular policy based version.
                # similar to what we do in '_setup_event_channel' using 'self._topic_root_policy', and then
                # calling 'self._sns.set_topic_attributes'
                try:
                    exponential_retry(
                        self._sns.remove_permission, {"InternalErrorException"}, TopicArn=self._topic_arn, Label=permission_label
                    )
                except self._sns.exceptions.NotFoundException:
                    pass

                exponential_retry(
                    self._sns.add_permission,
                    {"InternalErrorException"},
                    TopicArn=self._topic_arn,
                    Label=permission_label,
                    AWSAccountId=[downstream_account_id],
                    ActionName=["Receive", "Subscribe"],
                )

            # and finally the actual subscribe call can now be made from the downstream session.
            # if upstream makes this call on behalf of downstream, then downstream lambda should
            # catch SNS notification with a 'token' and then call 'confirm_subscription' (we avoid this).
            downstream_sns = downstream_session.client(
                service_name="sns",
                # here we have to use self._region again since we just want to
                # hit this SNS from downstream acc only.
                region_name=self._region,
            )

            exponential_retry(
                downstream_sns.subscribe, {"InternalErrorException"}, TopicArn=self._topic_arn, Protocol="lambda", Endpoint=lambda_arn
            )

    def unsubscribe_downstream_resource(self, downstream_platform: "DevelopmentPlatform", resource_type: str, resource_path: str):
        """Remove the resource owned by a downstream platform from event-channel gracefully, following a reverse order
        as compared to 'subscribe_downstream_resource'.
        """
        if resource_type == "lambda":
            downstream_session = downstream_platform.conf.get_param(AWSCommonParams.BOTO_SESSION)
            downstream_region = downstream_platform.conf.get_param(AWSCommonParams.REGION)
            # alternatively the session can be retrieved as below:
            #  current platform (self._dev_platform) should be alive as an upstream platform within
            #  the dev context of the caller  downstream platform
            # downstream_session = self._dev_platform.conf.get_param(AWSCommonParams.HOST_BOTO_SESSION)
            lambda_client = downstream_session.client(
                service_name="lambda",
                # have to use downstream's own region (cannot use self._region)
                region_name=downstream_region,
            )
            lambda_arn = resource_path

            statement_id: str = generate_statement_id(f"{self.__class__.__name__}_{self._dev_platform.context_id}_upstream_invoke")

            # 1- check if subscribed (don't rely on state-machine, be resilient against manual modifications, etc).
            subscription_arn: Optional[str] = find_subscription(self._sns, topic_arn=self._topic_arn, endpoint=lambda_arn)

            if subscription_arn:
                # 1.1- if already subscribed, then unsubscribe call from the downstream session.
                downstream_sns = downstream_session.client(
                    service_name="sns",
                    # here we have to use self._region again since we just want to
                    # hit this SNS from downstream acc only.
                    region_name=self._region,
                )

                exponential_retry(downstream_sns.unsubscribe, {"InternalErrorException"}, SubscriptionArn=subscription_arn)

            # 2 - check permission to downstream resource and remove (if cross-account)
            downstream_account_id = downstream_platform.conf.get_param(AWSCommonParams.ACCOUNT_ID)
            if self._account_id != downstream_account_id:
                permission_label: str = generate_statement_id(
                    f"{self.__class__.__name__}_{downstream_platform.context_id}_downstream_invoke"
                )

                try:
                    exponential_retry(
                        self._sns.remove_permission, {"InternalErrorException"}, TopicArn=self._topic_arn, Label=permission_label
                    )
                except self._sns.exceptions.NotFoundException:
                    pass

            # 3- now downstream lambda can remove the permission
            try:
                exponential_retry(
                    remove_permission,
                    {"InvalidParameterValueException", "ServiceException", "TooManyRequestsException"},
                    lambda_client,
                    lambda_arn,
                    statement_id,
                )
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                    raise

    def _revert_external(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        pass

    def _revert_internal(self, routes: Set[Route], prev_routes: Set[Route]) -> None:
        pass

    def _revert_internal_signals(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        # TODO
        pass

    def _revert_construct_connections(
        self, construct_conns: Set["_PendingConnRequest"], prev_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _revert_security_conf(selfs, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        pass

    def activate(self) -> None:
        super().activate()
        # app is being launched. our last chance to take runtime related actions
        # before our code becomes available to other constructs at runtime (ex: in AWS).
        self._setup_event_channel()

    def _setup_event_channel(self) -> None:
        """Make sure that the SNS topic which is central for Alarm forwarding is setup well.
        Create it (if it does not exist) in a way that it allows publish from all of the internal alarms by default.
        """
        topic_exists: bool = False
        try:
            exponential_retry(self._sns.get_topic_attributes, ["InternalErrorException"], TopicArn=self._topic_arn)
            topic_exists = True
        except self._sns.exceptions.NotFoundException:
            topic_exists = False

        if not topic_exists:
            try:
                response = exponential_retry(
                    self._sns.create_topic, ["InternalErrorException", "ConcurrentAccessException"], Name=self._topic_name
                )
                self._topic_arn = response["TopicArn"]
            except ClientError as err:
                module_logger.error("Couldn't create event channel (SNS topic) for the diagnostics! Error: %s", str(err))
                raise

            self._topic_root_policy = {
                "Version": "2012-10-17",
                "Id": str(uuid.uuid1()),
                "Statement": [
                    {
                        "Sid": str(uuid.uuid1()),
                        "Effect": "Allow",
                        # https://aws.amazon.com/premiumsupport/knowledge-center/cloudwatch-receive-sns-for-alarm-trigger/
                        "Principal": {"Service": "cloudwatch.amazonaws.com"},
                        "Action": "sns:Publish",
                        "Resource": self._topic_arn,
                        "Condition": {
                            "ArnLike": {
                                "AWS:SourceArn": f"arn:aws:cloudwatch:{self.region}:{self.account_id}:alarm:{self.get_unique_internal_alarm_name('*')}"
                            }
                            # "StringEquals": { "AWS:SourceOwner": f"{self._account_id}" }
                        },
                    }
                ],
            }

            # update SNS internal policy so that storage bucket can publish to it
            try:
                exponential_retry(
                    self._sns.set_topic_attributes,
                    ["InternalErrorException", "NotFoundException"],
                    TopicArn=self._topic_arn,
                    AttributeName="Policy",
                    AttributeValue=json.dumps(self._topic_root_policy),
                )
            except ClientError as err:
                module_logger.error(
                    f"Could not update policy for Diagnostics event-channel to enable publish for "
                    f"internal alarms! topic: {self._topic_arn}, err: {str(err)}"
                )
                raise

    def activation_completed(self) -> None:
        super().activation_completed()
        # now that the system is up & running (alarms have been created), we can setup the dashboard
        self._setup_default_dashboard()

    def _setup_default_dashboard(self) -> None:
        """
        Creates a default CW dashboard with the processed internal/external alarms and metrics

        H1/2/3: Represents a new Horizontal Layout with initial Header text widget (w=24)

         - H1 App UUID Diagnostic Dashboard
         - H2 Alarms (if any)
            - H3 All Internal Alarms Graph
            - H3 All Internal Alarms Status
            - H3 All External Alarms Graph
            - H3 All External Alarms Status
         - H2 Metrics
            - H3 internal metrics all (orchestration + routing)
            - H3 external metrics all
            - H3 system metrics
        """
        platform = self.get_platform()
        unique_context_id = self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]
        dashboard_name = f"RheocerOS-{unique_context_id}-Default-Dashboard"
        module_logger.critical(f"Creating default diagnostics dashboard {dashboard_name!r} ...")
        dashboard = {
            "start": "-PT2W",  # two weeks by default. parametrize for non-default
            "periodOverride": "inherit",
        }

        widgets = []
        widgets.append(
            create_text_widget(
                markdown=f"""
## Dashboard for RheocerOS Application **'{unique_context_id!r}'**
This dashboard is auto-updated every time the application is activated.

Layout of the dashboard:
 - **Alarms (if any)**
      - **All Internal Alarms Status**: using alarm status widget for all of the alarms
      - **All Internal Alarms Graph**: using separate Metric type widgets
      - **All External Alarms Graph**: 
      - **All External Alarms Status**: 
 - **Metrics**
      - **All internal metrics (orchestration + routing)**: using Metric type widgets for each Metric signal (group). Each metric 'Name' corresponds to a line.
      - **All external metrics**: these are the metric signals imported into the application via 'marshal_external_metric' API. Each metric 'Name' corresponds to a line.
      - **All system metrics**: metrics managed/emitted by underlying system resources. Each metric 'Name' corresponds to a line.
                              """,
                width=CW_DASHBOARD_WIDTH_MAX,
                height=4,
            )
        )  # default height = 6
        widgets.append(create_text_widget(markdown="## Alarms", width=CW_DASHBOARD_WIDTH_MAX, height=1))
        all_internal_alarms = [
            s for s in self._processed_internal_signals if s.type.is_alarm() and s.resource_access_spec.source.is_internal_signal()
        ]
        if all_internal_alarms:
            widgets.append(
                create_text_widget(
                    markdown=f"### Internal Alarms Status - All (all of the alarms created by this application)",
                    width=CW_DASHBOARD_WIDTH_MAX,
                    height=1,
                )
            )
            widgets.append(
                create_alarm_status_widget(
                    platform=platform,
                    title="State of all of the internal alarms together",
                    alarms=all_internal_alarms,
                    width=CW_DASHBOARD_WIDTH_MAX,
                    height=6,
                )
            )
            widgets.append(
                create_text_widget(
                    markdown=f"### Internal Alarm Graphs - All (all of the alarms created by this application)",
                    width=CW_DASHBOARD_WIDTH_MAX,
                    height=1,
                )
            )
            for alarm in all_internal_alarms:
                widgets.append(
                    create_metric_widget(
                        platform=platform, title=f"Graph for alarm {alarm.alias!r}", metrics_or_alarm=[alarm], width=12, height=6
                    )
                )

        all_external_alarms = [
            s for s in self._processed_external_signals if s.type.is_alarm() and s.resource_access_spec.source.is_external()
        ]
        if all_external_alarms:
            widgets.append(
                create_text_widget(
                    markdown=f"### External Alarms Status - All (all of the external alarms this application depends on)",
                    width=CW_DASHBOARD_WIDTH_MAX,
                    height=1,
                )
            )
            widgets.append(
                create_alarm_status_widget(
                    platform=platform,
                    title="State of all of the external alarms together",
                    alarms=all_external_alarms,
                    width=CW_DASHBOARD_WIDTH_MAX,
                    height=6,
                )
            )
            widgets.append(
                create_text_widget(
                    markdown=f"### External Alarm Graphs - All (all of the external alarms this application depends on)",
                    width=CW_DASHBOARD_WIDTH_MAX,
                    height=1,
                )
            )
            for alarm in all_external_alarms:
                widgets.append(
                    create_metric_widget(
                        platform=platform, title=f"Graph for alarm {alarm.alias!r}", metrics_or_alarm=[alarm], width=12, height=6
                    )
                )

        widgets.append(create_text_widget(markdown="## Metrics", width=CW_DASHBOARD_WIDTH_MAX, height=1))

        widgets.append(
            create_text_widget(
                markdown=f"### Internal Metrics - All (emitted by RheocerOS or user code)", width=CW_DASHBOARD_WIDTH_MAX, height=1
            )
        )
        all_internal_metrics = [
            s for s in self._processed_internal_signals if s.type.is_metric() and s.resource_access_spec.source.is_internal_signal()
        ]
        for metric_signal in all_internal_metrics:
            widget = create_metric_widget(
                platform=platform,
                title=f"Internal metric signal {metric_signal.alias!r} " f"({metric_signal.resource_access_spec.sub_dimensions!r})",
                metrics_or_alarm=[metric_signal],
                width=8,
            )
            # some of the internal metrics (user defined ones) might not have materialized 'Name' dimensions
            if widget["properties"]["metrics"]:
                widgets.append(widget)

        all_external_metrics = [
            s for s in self._processed_external_signals if s.type.is_metric() and s.resource_access_spec.source.is_external()
        ]
        if all_external_metrics:
            widgets.append(
                create_text_widget(
                    markdown=f"### External Metrics - All (emitted by imported external metrics or upstream RheocerOS applications)",
                    width=CW_DASHBOARD_WIDTH_MAX,
                    height=1,
                )
            )
            for metric_signal in all_external_metrics:
                widget = create_metric_widget(
                    platform=platform,
                    title=f"External metric signal " f"({metric_signal.resource_access_spec.sub_dimensions!r})",
                    metrics_or_alarm=[metric_signal],
                    width=8,
                )
                # some of the external metrics (user defined ones) might not have materialized 'Name' dimensions
                if widget["properties"]["metrics"]:
                    widgets.append(widget)

        widgets.append(
            create_text_widget(
                markdown=f"### System Metrics - All (emitted by underlying system resources)", width=CW_DASHBOARD_WIDTH_MAX, height=1
            )
        )
        system_metrics_map: Dict[Type[BaseConstruct], List[Signal]] = platform.get_system_metrics()
        for construct_type in system_metrics_map.keys():
            metric_signals = system_metrics_map[construct_type]
            construct = platform.conf.get_active_construct(construct_type)
            if metric_signals:
                widgets.append(
                    create_text_widget(
                        markdown=f"**{construct.__class__.__name__}** Metrics (emitted by its underlying resources)",
                        width=CW_DASHBOARD_WIDTH_MAX,
                        height=1,
                    )
                )

                for metric_signal in metric_signals:
                    widgets.append(
                        create_metric_widget(
                            platform=platform,
                            title=f"System metric {metric_signal.alias!r}, " f"({metric_signal.resource_access_spec.sub_dimensions!r})",
                            metrics_or_alarm=[metric_signal],
                            width=8,
                        )
                    )

        dashboard.update({"widgets": widgets})

        if len(widgets) > 100:
            # refer https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/CloudWatch-Dashboard-Body-Structure.html
            # for max limit
            module_logger.critical(
                f"Application topology contains too many nodes for default diagnostics CW dashboard. "
                f"Creation of {dashboard_name!r} might fail. Widget count: {len(widgets)}"
            )

        try:
            response = exponential_retry(
                self._cw.put_dashboard, {"InternalServiceFault"}, DashboardName=dashboard_name, DashboardBody=json.dumps(dashboard)
            )
            validation_messages = response["DashboardValidationMessages"]
            if validation_messages:
                module_logger.warning(f"Dashboard has been generated with validation errors: {validation_messages!r}")
            else:
                module_logger.critical(f"Default diagnostics CW dashboard {dashboard_name!r} has been created successfully!")
        except Exception as error:
            logging.critical(
                f"Default diagnostics CW dashboard {dashboard_name!r} could not be created due to error: {error!r}!"
                f" Widget count: {len(widgets)}"
            )
            logging.critical(f"By-passing the error as it has no direct impact on the runtime of this application.")

    def rollback(self) -> None:
        # roll back activation, something bad has happened (probably in another Construct) during app launch
        super().rollback()
        # TODO revert the structural change (keep track of other actions that just has been taken within 'activate')

    def terminate(self) -> None:
        """Designed to be resilient against repetitive calls in case of retries in the high-level
        termination work-flow.
        """
        # 1- Delete the topic
        if self._topic_arn:
            try:
                # This action is idempotent, so deleting a topic that does not exist does not result in an error.
                exponential_retry(
                    self._sns.delete_topic,
                    ["InternalError", "InternalErrorException", "ConcurrentAccess", "ConcurrentAccessException"],
                    TopicArn=self._topic_arn,
                )
            except KeyError:
                # TODO moto sns::delete_topic bug. they maintain an OrderDict for topics which causes KeyError for
                #   a non-existent key, rather than emulating SNS NotFound exception.
                pass
            except ClientError as err:
                if err.response["Error"]["Code"] not in ["NotFound", "ResourceNotFoundException"]:
                    module_logger.error(
                        f"Couldn't delete the event channel {self._topic_arn} for diagnostics "
                        f"driver {self.__class__.__name__!r}! Error: {str(err)}"
                    )
                    raise
            self._topic_arn = None

        # 2- Delete alarms that were created
        #  2.1- Remove Composite alarms first (hard condition in case they might depend on some of the metric alarms
        # which are also to be deleted next)
        for int_signal in self._processed_internal_signals:
            if int_signal.resource_access_spec.source == SignalSourceType.INTERNAL_COMPOSITE_ALARM:
                self._remove_internal_alarm(int_signal, is_terminating=True)

        #  2.2- Remove metric alarms
        for int_signal in self._processed_internal_signals:
            if int_signal.resource_access_spec.source == SignalSourceType.INTERNAL_ALARM:
                self._remove_internal_alarm(int_signal, is_terminating=True)

        super().terminate()

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)
        # update detected. this driver is replacing the prev diagnostics impl.
        # take necessary actions, compatibility checks, data copy, etc.
        # if it is not possible, raise

    def _process_security_conf(self, new_security_conf: ConstructSecurityConf, current_security_conf: ConstructSecurityConf) -> None:
        pass
