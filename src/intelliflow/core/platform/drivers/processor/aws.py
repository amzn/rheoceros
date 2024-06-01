# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import time
import uuid
from typing import Any, ClassVar, Dict, List, Optional, Set, Type, Union, cast
from urllib.parse import unquote_plus

import boto3

from intelliflow._logging_config import init_basic_logging
from intelliflow.core.deployment import PYTHON_VERSION_MAJOR, PYTHON_VERSION_MINOR, get_working_set_as_zip_stream, is_environment_immutable
from intelliflow.core.permission import PermissionContext
from intelliflow.core.signal_processing import DimensionFilter, Signal, Slot
from intelliflow.core.signal_processing.analysis import INTEGRITY_CHECKER_MAP
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import AlarmState, MetricPeriod
from intelliflow.core.signal_processing.routing_runtime_constructs import Route, RouteID, RoutingSession
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import (
    CWAlarmSignalSourceAccessSpec,
    CWCompositeAlarmSignalSourceAccessSpec,
    CWMetricSignalSourceAccessSpec,
    GlueTableSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
    SignalSourceType,
    SNSSignalSourceAccessSpec,
    TimerSignalSourceAccessSpec,
)
from intelliflow.utils.digest import calculate_bytes_sha256

from ...constructs import (
    FEEDBACK_SIGNAL_PROCESSING_MODE_KEY,
    FEEDBACK_SIGNAL_TYPE_EVENT_KEY,
    SIGNAL_IS_BLOCKED_KEY,
    SIGNAL_TARGET_ROUTE_ID_KEY,
    ConstructInternalMetricDesc,
    ConstructParamsDict,
    ConstructPermission,
    ConstructSecurityConf,
    FeedBackSignalProcessingMode,
    ProcessingUnit,
    RoutingTable,
)
from ...definitions.aws.aws_lambda.client_wrapper import *
from ...definitions.aws.common import CommonParams as AWSCommonParams
from ...definitions.aws.common import exponential_retry, generate_statement_id
from ...definitions.aws.glue import catalog as glue_catalog
from ...definitions.aws.glue.catalog import MAX_PUT_RULE_LIMIT, CatalogCWEventResource, provision_cw_event_rule
from ...definitions.aws.s3.bucket_wrapper import (
    MAX_BUCKET_LEN,
    bucket_exists,
    create_bucket,
    delete_bucket,
    get_bucket,
    put_notification,
    put_policy,
    remove_notification,
    update_policy,
)
from ...definitions.aws.s3.object_wrapper import (
    build_object_key,
    empty_bucket,
    get_object,
    get_object_metadata,
    get_object_sha256_hash,
    put_object,
)
from ...definitions.aws.sns.client_wrapper import (
    SNSPublisherType,
    add_publisher_to_topic,
    find_subscription,
    get_topic_policy,
    remove_publisher_from_topic,
    topic_exists,
)
from ...definitions.aws.sqs.client_wrapper import *
from ...definitions.common import ActivationParams
from ..aws_common import AWSConstructMixin

module_logger = logging.getLogger(__name__)


class AWSLambdaProcessorBasic(AWSConstructMixin, ProcessingUnit):
    """Processor construct impl that uses Lambda in its core, surrounded by auxiliary resources (queues [DLQ, etc])

    Trade-offs:

        Pros:
        - Cost
        - Performance

        Cons:
        - Reliability
        - Security (Critical data)

    """

    LAMBDA_NAME_FORMAT: ClassVar[str] = "if-{0}-{1}-{2}"
    DLQ_NAME_FORMAT: ClassVar[str] = "if-{0}-{1}-{2}"
    BOOTSTRAPPER_ROOT_FORMAT: ClassVar[str] = "if-{0}-{1}-{2}-{3}"
    # Lambda specific retryables
    CLIENT_RETRYABLE_EXCEPTION_LIST: ClassVar[Set[str]] = {
        "InvalidParameterValueException",
        "ServiceException",
        "503",
        "ResourceConflictException",
    }
    MAIN_LOOP_INTERVAL_IN_MINUTES: ClassVar[int] = 1
    REPLAY_LOOP_INTERVAL_IN_MINUTES: ClassVar[int] = 5

    CORE_LAMBDA_CONCURRENCY_PARAM: ClassVar[str] = "CORE_LAMBDA_CONCURRENCY"
    MAX_CORE_LAMBDA_CONCURRENCY: ClassVar[int] = 15  # set to None to remove limit
    MAX_REPLAY_LAMBDA_CONCURRENCY: ClassVar[int] = 1

    def __init__(self, params: ConstructParamsDict) -> None:
        """Called the first time this construct is added/configured within a platform.

        Subsequent sessions maintain the state of a construct, so the following init
        operations occur in the very beginning of a construct's life-cycle within an app.
        """
        super().__init__(params)
        self._lambda = self._session.client(service_name="lambda", region_name=self._region)
        self._lambda_name = None
        self._lambda_arn = None
        self._filter_lambda_name = None
        self._filter_lambda_arn = None
        # for stuff like bootstrapper bucket and code
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._bucket = None
        self._bucket_name = None
        # SNS proxies
        self._sns = self._session.client(service_name="sns", region_name=self._region)
        # DLQ
        self._sqs = self._session.client(service_name="sqs", region_name=self._region)
        self._dlq_name = None
        self._dlq_url = None
        self._dlq_arn = None

        self._main_loop_timer_id = None
        self._replay_loop_timer_id = None
        self._update_desired_core_lambda_concurrency()

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._lambda = self._session.client(service_name="lambda", region_name=self._region)
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._bucket = get_bucket(self._s3, self._bucket_name)
        self._sqs = self._session.client(service_name="sqs", region_name=self._region)
        self._sns = self._session.client(service_name="sns", region_name=self._region)
        self._update_desired_core_lambda_concurrency()

    def _update_desired_core_lambda_concurrency(self) -> None:
        self._desired_core_lambda_concurrency = self._params.get(self.CORE_LAMBDA_CONCURRENCY_PARAM, self.MAX_CORE_LAMBDA_CONCURRENCY)
        if self._desired_core_lambda_concurrency <= 0:
            raise ValueError(f"{self.CORE_LAMBDA_CONCURRENCY_PARAM!r} must be greater than zero!")

    @property
    def desired_core_lambda_concurrency(self) -> int:
        return self._desired_core_lambda_concurrency

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._lambda = None
        self._s3 = None
        self._bucket = None
        self._sqs = None
        self._sns = None

    def process(
        self,
        signal_or_event: Union[Signal, Dict[str, Any]],
        use_activated_instance=False,
        processing_mode=FeedBackSignalProcessingMode.ONLY_HEAD,
        target_route_id: Optional[RouteID] = None,
        is_async=True,
        filter=False,
        is_blocked=False,
    ) -> Optional[List[RoutingTable.Response]]:
        if isinstance(signal_or_event, Signal):
            lambda_event = {
                FEEDBACK_SIGNAL_TYPE_EVENT_KEY: signal_or_event.serialize(),
                FEEDBACK_SIGNAL_PROCESSING_MODE_KEY: processing_mode.value,
            }
        else:
            lambda_event = signal_or_event

        if target_route_id:
            lambda_event.update({SIGNAL_TARGET_ROUTE_ID_KEY: target_route_id})

        if is_blocked:
            lambda_event.update({SIGNAL_IS_BLOCKED_KEY: is_blocked})

        if use_activated_instance or not self._dev_platform:
            while True:
                try:
                    target_lambda_name: str = self._lambda_name if not filter else self._filter_lambda_name
                    exponential_retry(
                        invoke_lambda_function,
                        # KMSAccessDeniedException is caused by quick terminate->activate cycles
                        # where Lambda encryption store might have problems with IAM propagation probably.
                        ["ServiceException", "KMSAccessDeniedException"],
                        self._lambda,
                        target_lambda_name,
                        lambda_event,
                        is_async,
                    )
                    break
                except ClientError as error:
                    if error.response["Error"]["Code"] != "KMSAccessDeniedException":
                        raise
                    else:
                        logger.critical(
                            f"Lambda still needs time to use role {self._params[AWSCommonParams.IF_EXE_ROLE]}"
                            f" for the encryption of environment variables. If this error persists, then"
                            f" there must be another problem and this should be treated as fatal."
                        )

            return None
        else:
            return self.event_handler(self._dev_platform, lambda_event, None, filter_only=False)

    # overrides
    def pause(self) -> None:
        """
        Pause the entire application by pausing event consumption.
        Sync, manual invocations are still allowed at this layer. However, high-level application logic can still
        block even those type of attempts while being paused.
        The behaviour we expect is totally provided by AWS Lambda event-queue, as cool as it gets.
        WARNING: if pause state exceeds default async invocation conf limit of 6 hours
        then some of the events will be dropped and pause-resume cycle won't basically be
        graceful.
        """
        try:
            exponential_retry(put_function_concurrency, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name, 0)
        except ClientError as error:
            if error.response["Error"]["Code"] == "ResourceNotFoundException":
                logger.warning(
                    f"Ignoring PAUSE request on non-existent Processor core lambda {self._lambda_name!r}! "
                    f"If this is not a retry on a previous unsuccessful termination attempt, then should "
                    f"be treated as a system error."
                )
        super().pause()

    def resume(self) -> None:
        concurreny_limit = self.concurrency_limit
        if concurreny_limit is None:
            concurreny_limit = self._desired_core_lambda_concurrency

        if concurreny_limit:
            exponential_retry(
                put_function_concurrency, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name, concurreny_limit
            )
        else:
            # delete the limit
            exponential_retry(delete_function_concurrency, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name)
        super().resume()

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)
        self._lambda_name: str = self.LAMBDA_NAME_FORMAT.format(self.__class__.__name__, self._dev_platform.context_id, self._region)

        self._lambda_arn = f"arn:aws:lambda:{self._region}:{self._account_id}:function:{self._lambda_name}"

        self._filter_lambda_name: str = self._lambda_name + "-FILTER"
        self._filter_lambda_arn = f"arn:aws:lambda:{self._region}:{self._account_id}:function:{self._filter_lambda_name}"

        self._replay_lambda_name: str = self._lambda_name + "-REPLAY"
        self._replay_lambda_arn = f"arn:aws:lambda:{self._region}:{self._account_id}:function:{self._replay_lambda_name}"

        self._dlq_name = self._lambda_name + "-DLQ"
        queue_name_len_diff = len(self._dlq_name) - MAX_QUEUE_NAME_SIZE
        if queue_name_len_diff > 0:
            msg = (
                f"Platform context_id '{self._dev_platform.context_id}' is too long (by {queue_name_len_diff}!"
                f" {self.__class__.__name__} needs to use it create {self._dlq_name} SQS queue."
                f" Please refer https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-queues.html"
                f" to align your naming accordingly in order to be able to use this driver."
            )
            module_logger.error(msg)
            raise ValueError(msg)

        self._bucket_name: str = self.BOOTSTRAPPER_ROOT_FORMAT.format(
            "awslambda".lower(), self._dev_platform.context_id.lower(), self._account_id, self._region
        )
        bucket_len_diff = len(self._bucket_name) - MAX_BUCKET_LEN
        if bucket_len_diff > 0:
            msg = (
                f"Platform context_id '{self._dev_platform.context_id}' is too long (by {bucket_len_diff}!"
                f" {self.__class__.__name__} needs to use it create {self._bucket_name} bucket in S3."
                f" Please refer https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html"
                f" to align your naming accordingly in order to be able to use this driver."
            )
            module_logger.error(msg)
            raise ValueError(msg)

        self._main_loop_timer_id = self._lambda_name + "-loop"
        if (len(self._main_loop_timer_id) - MAX_PUT_RULE_LIMIT) > 0:
            msg = f"Main Loop Timer Id ({self._main_loop_timer_id}) length needs to be less than or equal to {str(MAX_PUT_RULE_LIMIT)}"
            module_logger.error(msg)
            raise ValueError(msg)

        self._replay_loop_timer_id = self._lambda_name + "-replay"
        if (len(self._replay_loop_timer_id) - MAX_PUT_RULE_LIMIT) > 0:
            msg = f"Replay Loop Timer Id ({self._replay_loop_timer_id}) length needs to be less than or equal to {str(MAX_PUT_RULE_LIMIT)}"
            module_logger.error(msg)
            raise ValueError(msg)

        # TODO easily hits event-bridge PutRule 64 limit (should be <= 64)
        #   re-evaluate 'using unique but abbreviated IDs of drivers', e.g AWS_Lambda_1
        self._glue_catalog_event_channel_rule_id = self._lambda_name + "-gluec-id"
        if (len(self._glue_catalog_event_channel_rule_id) - MAX_PUT_RULE_LIMIT) > 0:
            msg = f"Glue Catalog Event Channel Rule Id ({self._glue_catalog_event_channel_rule_id}) length needs to be less than or equal to {str(MAX_PUT_RULE_LIMIT)}"
            module_logger.error(msg)
            raise ValueError(msg)

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        """Whole platform got bootstrapped at runtime. For other runtime services, this
        construct should be initialized (ex: context_owner: Lambda, Glue, etc)"""
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        # TODO if not self.__class__ == context_owner.__class__
        # so that cyclic invocations wont happen. there should be a scenario where this
        # lambda would be calling itself.
        self._lambda = boto3.client(service_name="lambda", region_name=self._region)
        # TODO comment the following, probably won't need at runtime
        self._s3 = boto3.resource("s3")
        self._bucket = get_bucket(self._s3, self._bucket_name)
        self._sqs = boto3.client(service_name="sqs", region_name=self._region)
        self._sns = boto3.client(service_name="sns", region_name=self._region)

    def provide_runtime_trusted_entities(self) -> List[str]:
        return ["lambda.amazonaws.com", "events.amazonaws.com"]

    def provide_runtime_default_policies(self) -> List[str]:
        return []

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        # allow exec-role (post-activation, cumulative list of all trusted entities [AWS services]) to do the following;
        permissions = [
            ConstructPermission([f"arn:aws:s3:::{self._bucket_name}", f"arn:aws:s3:::{self._bucket_name}/*"], ["s3:*"]),
            # CW Logs
            ConstructPermission([f"arn:aws:logs:{self._region}:{self._account_id}:*"], ["logs:*"]),
            #   - allow other platform components to call us via invoke
            #   - Or recursive calls to the same func
            # then add permission with "lambda:InvokeFunction" using self._lambda_arn
            # (for that, we should build lambda_arn during dev_init, not as a response to create_function)
            ConstructPermission([self._lambda_arn], ["lambda:InvokeFunction"]),
            ConstructPermission([self._filter_lambda_arn], ["lambda:InvokeFunction"]),
            # DLQ (let exec-role send messages to DLQ)
            ConstructPermission(
                [
                    # since this callback is received right before the activation (but after dev_init),
                    # we cannot use the retrieved self._dlq_arn. should manually create it here.
                    # not a big deal since self._dlq_name should already be created.
                    f"arn:aws:sqs:{self._region}:{self._account_id}:{self._dlq_name}"
                ],
                ["sqs:SendMessage", "sqs:SendMessageBatch", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ReceiveMessage"],
            ),
        ]

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

        # TODO Move into <RoutingTable>
        for route in self._pending_internal_routes:
            for slot in route.slots:
                if slot.type.is_inlined_compute() and slot.permissions:
                    for compute_perm in slot.permissions:
                        # TODO check compute_perm feasibility in AWS Lambda (check ARN, resource type, etc)
                        if compute_perm.context != PermissionContext.DEVTIME:
                            permissions.append(ConstructPermission(compute_perm.resource, compute_perm.action))
            # check Slots from hooks
            for hook in (route.execution_hook, route.pending_node_hook):
                if hook:
                    for callback in hook.callbacks():
                        if isinstance(callback, Slot) and callback.permissions:
                            for compute_perm in callback.permissions:
                                if compute_perm.context != PermissionContext.DEVTIME:
                                    permissions.append(ConstructPermission(compute_perm.resource, compute_perm.action))

        return permissions

    @classmethod
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        # dev-role permissions (things this construct would do during development)
        # dev-role should be able to do the following.
        bucket_name_format: str = cls.BOOTSTRAPPER_ROOT_FORMAT.format(
            "awslambda".lower(), "*", params[AWSCommonParams.ACCOUNT_ID], params[AWSCommonParams.REGION]
        )

        lambda_name_format: str = cls.LAMBDA_NAME_FORMAT.format(cls.__name__, "*", params[AWSCommonParams.REGION])
        filter_lambda_name_format: str = lambda_name_format + "-FILTER"
        replay_lambda_name_format: str = lambda_name_format + "-REPLAY"
        dlq_name_format: str = lambda_name_format + "-DLQ"
        return [
            # full permission on its own bucket
            ConstructPermission([f"arn:aws:s3:::{bucket_name_format}", f"arn:aws:s3:::{bucket_name_format}/*"], ["s3:*"]),
            # on everything else, still try to minimize
            ConstructPermission(
                ["*"],
                [
                    "s3:List*",
                    "s3:Get*",
                    "s3:Head*",
                    # required for external S3 signal registration (from the same account)
                    "s3:PutBucketNotificationConfiguration",
                    "s3:PutBucketPolicy",
                ],
            ),
            # full permission on its own resources
            # 1- Lambda
            ConstructPermission(
                [
                    f"arn:aws:lambda:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:function:{lambda_name_format}",
                    f"arn:aws:lambda:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:function:{filter_lambda_name_format}",
                    f"arn:aws:lambda:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:function:{replay_lambda_name_format}",
                ],
                ["lambda:*"],
            ),
            # 2- DLQ (let exec-role send messages to DLQ)
            ConstructPermission(
                [f"arn:aws:sqs:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:{dlq_name_format}"],
                ["sqs:*"],
            ),
            # read-only on other resources
            # 1- lambda
            ConstructPermission(["*"], ["lambda:Get*", "lambda:List*"]),
            # 2- SQS
            ConstructPermission(["*"], ["sqs:Get*", "sqs:List*"]),
            ConstructPermission(["*"], ["events:*"]),
            # 3- SNS (proxy registrations)
            ConstructPermission(["*"], ["sns:*"]),
            # Glue catalog access (see "glue_catalog.check_table" call below)
            ConstructPermission(
                [f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:catalog"],
                ["glue:GetTable", "glue:GetTables"],
            ),
        ]

    # overrides
    def _provide_system_metrics(self) -> List[Signal]:
        """Expose system generated metrics to the rest of the platform in a consolidated, filtered and
        well-defined RheocerOS metric signal format.

        Refer
            Lambda:
            https://docs.aws.amazon.com/lambda/latest/dg/monitoring-metrics.html
            SQS:
            https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-available-cloudwatch-metrics.html
        """
        function_names = [self._lambda_name, self._filter_lambda_name, self._replay_lambda_name]
        function_types = ["core", "filter", "replay"]
        function_level_metrics = [
            "Throttles",
            "Errors",
            "Duration",
            "Invocations",
            "ConcurrentExecutions",
            "DeadLetterErrors",
        ]
        _unused_function_level_metrics = [
            "DestinationDeliveryFailures",
            "ProvisionedConcurrencyInvocations",
            "ProvisionedConcurrencySpilloverInvocations",
        ]

        dlq_names = [self._dlq_name]
        dlq_metrics = [
            "NumberOfMessagesSent",
            "NumberOfMessagesReceived",
            "ApproximateNumberOfMessagesVisible",
            "ApproximateAgeOfOldestMessage",
        ]  # these two are relevant for users actually
        return [
            Signal(
                SignalType.CW_METRIC_DATA_CREATION,
                CWMetricSignalSourceAccessSpec(
                    "AWS/Lambda",
                    {"FunctionName": function_name},
                    # metadata (should be visible in front-end)
                    **{"Notes": "Supports 1 min period"},
                ),
                SignalDomainSpec(
                    dimension_filter_spec=DimensionFilter.load_raw(
                        {
                            metric_name: {  # Name  (overwrite in filter spec to make them visible to the user. otherwise user should specify the name,
                                #           in that case platform abstraction is broken since user should have a very clear idea about what is
                                #           providing these metrics).
                                "*": {  # Statistic
                                    "*": {  # Period  (AWS emits with 1 min by default), let user decide.
                                        "*": {}  # Time  always leave it 'any' if not experimenting.
                                    }
                                }
                            }
                            for metric_name in function_level_metrics
                        }
                    ),
                    dimension_spec=None,
                    integrity_check_protocol=None,
                ),
                # make sure that default metric alias/ID complies with CW expectation (first letter lower case).
                f"processor.{function_types[i]}",
            )
            for i, function_name in enumerate(function_names)
        ] + [
            Signal(
                SignalType.CW_METRIC_DATA_CREATION,
                CWMetricSignalSourceAccessSpec(
                    "AWS/SQS",
                    {"QueueName": dlq_name},
                    # metadata (should be visible in front-end)
                    **{"Notes": "Supports 1 min period"},
                ),
                SignalDomainSpec(
                    dimension_filter_spec=DimensionFilter.load_raw(
                        {
                            metric_name: {  # Name  (overwrite in filter spec to make them visible to the user. otherwise user should specify the name,
                                #           in that case platform abstraction is broken since user should have a very clear idea about what is
                                #           providing these metrics).
                                "*": {  # Statistic
                                    "*": {  # Period  (AWS emits with 1 min by default), let user decide.
                                        "*": {}  # Time  always leave it 'any' if not experimenting.
                                    }
                                }
                            }
                            for metric_name in dlq_metrics
                        }
                    ),
                    dimension_spec=None,
                    integrity_check_protocol=None,
                ),
                # make sure that default metric alias/ID complies with CW expectation (first letter lower case).
                f"processor.dlq",
            )
            for i, dlq_name in enumerate(dlq_names)
        ]

    def _provide_route_metrics(self, route: Route) -> List[ConstructInternalMetricDesc]:
        return []

    def _provide_internal_metrics(self) -> List[ConstructInternalMetricDesc]:
        # return internal signals that will be emitted by this driver from within event_handler
        #  where emission via Diagnostics::emit will be done.
        #  please note that this metrics can be retrieved via Diagnostics::__getitem__ or get_internal_metric APIs,
        #  so there is no need to main this list in a global scope.
        return [
            ConstructInternalMetricDesc(
                id="processor.event.type",
                metric_names=["Internal", "ProcessorQueue", "NextCycle", "S3", "SNS", "CW_Alarm", "Glue", "EventBridge", "Unrecognized"],
            ),
            ConstructInternalMetricDesc(
                id="processor.filter.event.type",
                metric_names=["Internal", "ProcessorQueue", "NextCycle", "S3", "SNS", "CW_Alarm", "Glue", "EventBridge", "Unrecognized"],
                # since metric names are shared with the other event type, we have to
                # discriminate using a sub-dimension.
                extra_sub_dimensions={"filter_mode": "True"},
            ),
            ConstructInternalMetricDesc(
                id="processor.replay.event.type",
                metric_names=["Internal", "ProcessorQueue", "NextCycle", "S3", "SNS", "CW_Alarm", "Glue", "EventBridge", "Unrecognized"],
                # since metric names are shared with the other event type, we have to
                # discriminate using a sub-dimension.
                extra_sub_dimensions={"replay_mode": "True"},
            ),
            ConstructInternalMetricDesc(id="processor.event.error.type", metric_names=["NameError", "Exception"]),
        ]

    def _provide_internal_alarms(self) -> List[Signal]:
        """Provide internal alarms (of type INTERNAL_ALARM OR INTERNAL_COMPOSITE_ALARM) managed/emitted
        by this driver impl"""
        return []

    def _setup_scripts_bucket(self):
        """Initial setup of storage bucket. Enforces policy for access from dev/runtime roles."""
        try:
            self._bucket = create_bucket(self._s3, self._bucket_name, self._region)
        except ClientError as error:
            if error.response["Error"]["Code"] == "InvalidBucketName":
                msg = (
                    f"Platform context_id '{self._dev_platform.context_id}' is not valid!"
                    f" {self.__class__.__name__} needs to use it create {self._bucket_name} bucket in S3."
                    f" Please refer https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html"
                    f" to align your naming accordingly in order to be able to use this driver."
                )
                module_logger.error(msg)
                raise ValueError(msg)
            else:
                raise

        self._setup_activated_bucket_policy()

    def _setup_activated_bucket_policy(self) -> None:
        put_policy_desc = {
            "Version": "2012-10-17",
            "Id": str(uuid.uuid1()),
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": self._params[AWSCommonParams.IF_DEV_ROLE]},
                    "Action": ["s3:*"],
                    "Resource": [f"arn:aws:s3:::{self._bucket.name}/*", f"arn:aws:s3:::{self._bucket.name}"],
                },
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": self._params[AWSCommonParams.IF_EXE_ROLE]},
                    # TODO post-MVP narrow down
                    "Action": ["s3:*"],
                    "Resource": [f"arn:aws:s3:::{self._bucket.name}/*", f"arn:aws:s3:::{self._bucket.name}"],
                },
            ],
        }
        try:
            exponential_retry(put_policy, ["MalformedPolicy"], self._s3, self._bucket.name, put_policy_desc)
        except ClientError as error:
            if error.response["Error"]["Code"] == "MalformedPolicy":
                module_logger.error(
                    f"Could not put the policy for {self.__class__.__name__} resources folder!"
                    f" A reactivation might be required. Error:",
                    str(error),
                )
            else:
                raise

    def activate(self) -> None:
        super().activate()
        if not bucket_exists(self._s3, self._bucket_name):
            self._setup_scripts_bucket()
        else:
            self._bucket = get_bucket(self._s3, self._bucket_name)

        # check DLQ
        dlq_url = None
        try:
            response = exponential_retry(self._sqs.get_queue_url, [""], QueueName=self._dlq_name)
            dlq_url = response["QueueUrl"]
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["AWS.SimpleQueueService.NonExistentQueue"]:
                raise

        if not dlq_url:
            self._dlq_url = exponential_retry(
                create_queue,
                ["AWS.SimpleQueueService.QueueDeletedRecently"],
                self._sqs,
                self._dlq_name,
                1209600,  # 14 days for msg retention period (MAX for SQS)
                15 * 60,  # 15 mins for visibility timeout for future support of consumption from DLQ.
                # in that case the consumer will depend on max IF event processing time
                # before it can actually delete the message from the Queue.
            )
        else:
            self._dlq_url = dlq_url
            exponential_retry(set_queue_attributes, [], self._sqs, self._dlq_url, 1209600, 15 * 60)
        self._dlq_arn = get_queue_arn(self._sqs, self._dlq_url)

        if not is_environment_immutable():
            working_set_stream = get_working_set_as_zip_stream()
            # Sync working set to S3
            deployment_package = self._upload_working_set_to_s3(working_set_stream, self._bucket, "WorkingSet.zip")
            # lambda
            self._activate_core_lambda(deployment_package)
            # filter lambda
            self._activate_filter_lambda(deployment_package)
            # filter lambda
            self._activate_replay_lambda(deployment_package)
            # So what about ASYNC invocation configuration?
            # Why not use https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.put_function_event_invoke_config
            # - Because, default settings (6 hours event age, 2 retries, etc) are perfectly ok for this driver impl and
            # based on how IF internal routing works.
            # - Also we already set DLQ via function creation and conf update APIs and don't need to use event invoke config
            # to alternatively set our DLQ as a failure destination.

        self._activate_main_loop()
        self._activate_replay_loop()

    def _upload_working_set_to_s3(self, working_set_stream: bytes, bucket, s3_object_key: str) -> Dict[str, str]:
        # Upload working set to S3 if has not been loaded yet, return bucket name and object key as dict.
        # Try getting metadata which contains hash of the workspace from S3, compare it with local hash.
        remote_object_hash = exponential_retry(
            get_object_sha256_hash, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._s3, self._bucket_name, s3_object_key
        )
        local_object_hash = calculate_bytes_sha256(working_set_stream)
        if remote_object_hash and remote_object_hash == local_object_hash:
            # If the remote version is consistent with local
            logger.info("Workspace already uploaded to S3, skipped upload")
        else:
            # If the remote version needs upload
            logger.info("Uploading working set to S3")
            exponential_retry(put_object, self.CLIENT_RETRYABLE_EXCEPTION_LIST, bucket, s3_object_key, working_set_stream)
        return {"S3Bucket": self._bucket_name, "S3Key": s3_object_key, "sha256": local_object_hash}

    def _activate_core_lambda(self, deployment_package):
        lambda_arn = exponential_retry(get_lambda_arn, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name)
        if lambda_arn and lambda_arn != self._lambda_arn:
            raise RuntimeError(f"AWS Lambda returned an ARN in an unexpected format." f" Expected: {self._lambda_arn}. Got: {lambda_arn}")

        if not lambda_arn:
            # create the function without the 'bootstrapper' first so that connections can be established,
            # bootstrapper is later on set within _update_bootstrapper below as an env param.
            self._lambda_arn = exponential_retry(
                create_lambda_function,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._lambda,
                self._lambda_name,
                "RheocerOS application Processor construct main loop in Lambda",
                # TODO
                "intelliflow.core.platform.drivers.processor.aws.AWSLambdaProcessorBasic_lambda_handler",
                self._params[AWSCommonParams.IF_EXE_ROLE],
                deployment_package,
                PYTHON_VERSION_MAJOR,
                PYTHON_VERSION_MINOR,
                self._dlq_arn,
            )

            # see '_update_bootstrapper' callback below to see how bootstrapper is set
            # at the end of the activation cycle. we cannot set it now since activation across
            # other drivers might happen in parallel and they might impact the final state of
            # the platform which needs to be serialized as the bootstrapper.
        elif not self._lambda_code_source_in_sync(deployment_package, self._lambda_name):  # update
            # code should be updated first, as the workset should naturally be backward compatible
            # with the conf (which carries the bootstrapper [RuntimePlatform]).
            exponential_retry(
                update_lambda_function_code, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name, deployment_package
            )
        else:
            logger.info("Same working set already uploaded, upload skipped")

        concurreny_limit = self.concurrency_limit
        if concurreny_limit is None:
            concurreny_limit = self._desired_core_lambda_concurrency
        if concurreny_limit:
            exponential_retry(
                put_function_concurrency,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._lambda,
                self._lambda_name,
                concurreny_limit if not self.is_paused() else 0,
            )
        else:
            # delete the limit
            exponential_retry(delete_function_concurrency, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name)

    def _activate_filter_lambda(self, deployment_package):
        lambda_arn = exponential_retry(get_lambda_arn, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._filter_lambda_name)
        if lambda_arn and lambda_arn != self._filter_lambda_arn:
            raise RuntimeError(
                f"AWS Lambda returned an ARN for filter Lambda in an unexpected format."
                f" Expected: {self._filter_lambda_arn}. Got: {lambda_arn}"
            )

        if not lambda_arn:
            # create the function without the 'bootstrapper' first so that connections can be established,
            # bootstrapper is later on set within _update_bootstrapper below as an env param.
            self._filter_lambda_arn = exponential_retry(
                create_lambda_function,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._lambda,
                self._filter_lambda_name,
                "RheocerOS application Processor construct filterer/forwarder in Lambda",
                "intelliflow.core.platform.drivers.processor.aws.AWSLambdaProcessorBasic_lambda_handler",
                self._params[AWSCommonParams.IF_EXE_ROLE],
                deployment_package,
                PYTHON_VERSION_MAJOR,
                PYTHON_VERSION_MINOR,
                self._dlq_arn,
                is_filter="True",
            )
        elif not self._lambda_code_source_in_sync(deployment_package, self._filter_lambda_name):  # update
            # code should be updated first, as the workset should naturally be backward compatible
            # with the conf (which carries the bootstrapper [RuntimePlatform]).
            exponential_retry(
                update_lambda_function_code,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._lambda,
                self._filter_lambda_name,
                deployment_package,
            )
        else:
            logger.info("Same working set already uploaded, upload skipped")

    def _activate_replay_lambda(self, deployment_package):
        lambda_arn = exponential_retry(get_lambda_arn, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._replay_lambda_name)
        if lambda_arn and lambda_arn != self._replay_lambda_arn:
            raise RuntimeError(
                f"AWS Lambda returned an ARN in an unexpected format." f" Expected: {self._replay_lambda_arn}. Got: {lambda_arn}"
            )

        if not lambda_arn:
            # create the function without the 'bootstrapper' first so that connections can be established,
            # bootstrapper is later on set within _update_bootstrapper below as an env param.
            self._replay_lambda_arn = exponential_retry(
                create_lambda_function,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._lambda,
                self._replay_lambda_name,
                "IntelliFlow application Processor construct throttled (or retryable) events replay Lambda",
                "intelliflow.core.platform.drivers.processor.aws.AWSLambdaProcessorBasic_replay_lambda_handler",
                self._params[AWSCommonParams.IF_EXE_ROLE],
                deployment_package,
                PYTHON_VERSION_MAJOR,
                PYTHON_VERSION_MINOR,
                None,  # DLQ
            )

            # see '_update_bootstrapper' callback below to see how bootstrapper is set
            # at the end of the activation cycle. we cannot set it now since activation across
            # other drivers might happen in parallel and they might impact the final state of
            # the platform which needs to be serialized as the bootstrapper.
        elif not self._lambda_code_source_in_sync(deployment_package, self._replay_lambda_name):  # update
            # code should be updated first, as the workset should naturally be backward compatible
            # with the conf (which carries the bootstrapper [RuntimePlatform]).
            exponential_retry(
                update_lambda_function_code,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._lambda,
                self._replay_lambda_name,
                deployment_package,
            )
        else:
            logger.info("Same working set already uploaded to replay Lambda, upload skipped")

        concurreny_limit = self.MAX_REPLAY_LAMBDA_CONCURRENCY
        if concurreny_limit:
            exponential_retry(
                put_function_concurrency,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._lambda,
                self._replay_lambda_name,
                concurreny_limit,
            )
        else:
            # delete the limit
            exponential_retry(delete_function_concurrency, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._replay_lambda_name)

    def build_bootstrapper_object_key(self) -> str:
        return build_object_key(["bootstrapper"], f"{self.__class__.__name__.lower()}_RuntimePlatform.data")

    def _update_bootstrapper(self, bootstrapper: "RuntimePlatform") -> None:
        # now uploading it to S3 and passing S3 link as Env param instead of the
        # whole bootsrapper. Lambda has a limit of 4KB for the all of the Env params.
        # (current bootstrapper size is 2 KB, still has a lot of room but it is better not to
        # have this constraint / future risk).
        bootstrapped_platform = bootstrapper.serialize()

        bootstrapper_object_key = self.build_bootstrapper_object_key()
        exponential_retry(
            put_object, {"ServiceException", "TooManyRequestsException"}, self._bucket, bootstrapper_object_key, bootstrapped_platform
        )

        # core router
        exponential_retry(
            update_lambda_function_conf,
            self.CLIENT_RETRYABLE_EXCEPTION_LIST,
            self._lambda,
            self._lambda_name,
            "RheocerOS application Processor construct main loop in Lambda",
            "intelliflow.core.platform.drivers.processor.aws.AWSLambdaProcessorBasic_lambda_handler",
            self._params[AWSCommonParams.IF_EXE_ROLE],
            PYTHON_VERSION_MAJOR,
            PYTHON_VERSION_MINOR,
            self._dlq_arn,
            bootstrapper_bucket=self._bucket_name,
            bootstrapper_key=bootstrapper_object_key,
        )

        # filter
        exponential_retry(
            update_lambda_function_conf,
            self.CLIENT_RETRYABLE_EXCEPTION_LIST,
            self._lambda,
            self._filter_lambda_name,
            "RheocerOS application Processor construct filterer/forwarder in Lambda",
            "intelliflow.core.platform.drivers.processor.aws.AWSLambdaProcessorBasic_lambda_handler",
            self._params[AWSCommonParams.IF_EXE_ROLE],
            PYTHON_VERSION_MAJOR,
            PYTHON_VERSION_MINOR,
            self._dlq_arn,
            bootstrapper_bucket=self._bucket_name,
            bootstrapper_key=bootstrapper_object_key,
            is_filter="True",
        )

        # replay
        exponential_retry(
            update_lambda_function_conf,
            self.CLIENT_RETRYABLE_EXCEPTION_LIST,
            self._lambda,
            self._replay_lambda_name,
            "IntelliFlow application Processor construct throttled (retryable) event replay Lambda",
            "intelliflow.core.platform.drivers.processor.aws.AWSLambdaProcessorBasic_replay_lambda_handler",
            self._params[AWSCommonParams.IF_EXE_ROLE],
            PYTHON_VERSION_MAJOR,
            PYTHON_VERSION_MINOR,
            None,
            bootstrapper_bucket=self._bucket_name,
            bootstrapper_key=bootstrapper_object_key,
        )

    def _lambda_code_source_in_sync(self, deployment_package, lambda_name):
        lambda_digest = exponential_retry(get_lambda_digest, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, lambda_name)
        logger.info("Remote lambda's code SHA256 is %s, local source %s", lambda_digest, deployment_package["sha256"])
        return lambda_digest is not None and deployment_package["sha256"] == lambda_digest

    def _activate_main_loop(self) -> None:
        events = self._session.client(service_name="events", region_name=self._region)

        interval = self.MAIN_LOOP_INTERVAL_IN_MINUTES
        main_loop_schedule_expression = f"rate({interval} minute{'s' if interval > 1 else ''})"

        statement_id: str = self._get_unique_statement_id_for_cw(self._main_loop_timer_id)
        try:
            exponential_retry(remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name, statement_id)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                raise

        response = exponential_retry(
            events.put_rule,
            ["ConcurrentModificationException", "InternalException"],
            Name=self._main_loop_timer_id,
            ScheduleExpression=main_loop_schedule_expression,
            State="ENABLED",
        )
        rule_arn = response["RuleArn"]

        try:
            exponential_retry(
                add_permission,
                # on platforms where related waiters are not supported, func might not be ready yet.
                self.CLIENT_RETRYABLE_EXCEPTION_LIST.union({"ResourceNotFoundException"}),
                self._lambda,
                self._lambda_name,
                statement_id,
                "lambda:InvokeFunction",
                "events.amazonaws.com",
                rule_arn,
            )
            # DO NOT provide account_id for anything other than S3
            # self._account_id)
        except ClientError as error:
            # TODO remove this special handling since we remove and add every time.
            if error.response["Error"]["Code"] not in ["ResourceConflictException"]:
                raise

        # now connect rule & lambda (add lamda as target on the CW side)
        response = exponential_retry(
            events.put_targets,
            ["ConcurrentModificationException", "InternalException"],
            Rule=self._main_loop_timer_id,
            Targets=[
                {
                    "Arn": self._lambda_arn,
                    "Id": self._lambda_name,
                }
            ],
        )
        if "FailedEntryCount" in response and response["FailedEntryCount"] > 0:
            raise RuntimeError(f"Cannot create timer {self._main_loop_timer_id} for Lambda {self._lambda_arn}. Response: {response}")

    def _deactivate_main_loop(self) -> None:
        """Retry friendly. Be resilient against retries during the high-level termination workflow."""
        if not self._main_loop_timer_id:
            return
        events = self._session.client(service_name="events", region_name=self._region)

        statement_id: str = self._get_unique_statement_id_for_cw(self._main_loop_timer_id)
        try:
            exponential_retry(
                events.remove_targets,
                ["ConcurrentModificationException", "InternalException"],
                Rule=self._main_loop_timer_id,
                Ids=[
                    self._lambda_name,
                ],
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                raise

        try:
            exponential_retry(remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name, statement_id)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                raise

        try:
            exponential_retry(events.delete_rule, ["ConcurrentModificationException", "InternalException"], Name=self._main_loop_timer_id)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                logger.critical(f"Cannot delete timer {self._main_loop_timer_id} for Lambda {self._lambda_arn}!")
                raise

        self._main_loop_timer_id = None

    def _activate_replay_loop(self) -> None:
        events = self._session.client(service_name="events", region_name=self._region)

        interval = self.REPLAY_LOOP_INTERVAL_IN_MINUTES
        replay_loop_schedule_expression = f"rate({interval} minute{'s' if interval > 1 else ''})"

        statement_id: str = self._get_unique_statement_id_for_cw(self._replay_loop_timer_id)
        try:
            exponential_retry(remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._replay_lambda_name, statement_id)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                raise

        response = exponential_retry(
            events.put_rule,
            ["ConcurrentModificationException", "InternalException"],
            Name=self._replay_loop_timer_id,
            ScheduleExpression=replay_loop_schedule_expression,
            State="ENABLED",
        )
        rule_arn = response["RuleArn"]

        try:
            exponential_retry(
                add_permission,
                # on platforms where related waiters are not supported, func might not be ready yet.
                self.CLIENT_RETRYABLE_EXCEPTION_LIST.union({"ResourceNotFoundException"}),
                self._lambda,
                self._replay_lambda_name,
                statement_id,
                "lambda:InvokeFunction",
                "events.amazonaws.com",
                rule_arn,
            )
        except ClientError as error:
            # TODO remove this special handling since we remove and add every time.
            if error.response["Error"]["Code"] not in ["ResourceConflictException"]:
                raise

        # now connect rule & lambda (add lamda as target on the CW side)
        response = exponential_retry(
            events.put_targets,
            ["ConcurrentModificationException", "InternalException"],
            Rule=self._replay_loop_timer_id,
            Targets=[
                {
                    "Arn": self._replay_lambda_arn,
                    "Id": self._replay_lambda_name,
                }
            ],
        )
        if "FailedEntryCount" in response and response["FailedEntryCount"] > 0:
            raise RuntimeError(
                f"Cannot create timer {self._replay_loop_timer_id} for Lambda {self._replay_lambda_arn}. Response: {response}"
            )

    def _deactivate_replay_loop(self) -> None:
        """Retry friendly. Be resilient against retries during the high-level termination workflow."""
        if not self._replay_loop_timer_id:
            return
        events = self._session.client(service_name="events", region_name=self._region)

        statement_id: str = self._get_unique_statement_id_for_cw(self._replay_loop_timer_id)
        try:
            exponential_retry(
                events.remove_targets,
                ["ConcurrentModificationException", "InternalException"],
                Rule=self._replay_loop_timer_id,
                Ids=[
                    self._replay_lambda_name,
                ],
            )
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                raise

        try:
            exponential_retry(remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._replay_lambda_name, statement_id)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                raise

        try:
            exponential_retry(events.delete_rule, ["ConcurrentModificationException", "InternalException"], Name=self._replay_loop_timer_id)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                logger.critical(f"Cannot delete timer {self._replay_loop_timer_id} for Lambda {self._replay_lambda_arn}!")
                raise

        self._replay_loop_timer_id = None

    def rollback(self) -> None:
        # roll back activation, something bad has happened (probably in another Construct) during app launch
        super().rollback()
        # TODO

    def terminate(self) -> None:
        """Designed to be resilient against repetitive calls in case of retries in the high-level
        termination work-flow.
        """
        # 1- remove main-loop 'event bridge' rule and detach from the lambda.
        if self._main_loop_timer_id:
            self._deactivate_main_loop()

        if self._replay_loop_timer_id:
            self._deactivate_replay_loop()

        # 2- remove lambda
        if self._lambda_name:
            try:
                exponential_retry(delete_lambda_function, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name)
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["ResourceNotFoundException", "404"]:
                    raise
            self._lambda_name = None

        # 2.1- remove filter lambda
        if self._filter_lambda_name:
            try:
                exponential_retry(delete_lambda_function, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._filter_lambda_name)
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["ResourceNotFoundException", "404"]:
                    raise
            self._filter_lambda_name = None

        # 2.2- remove replay lambda
        if self._replay_lambda_name:
            try:
                exponential_retry(delete_lambda_function, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._replay_lambda_name)
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["ResourceNotFoundException", "404"]:
                    raise
            self._replay_lambda_name = None

        # 3- remove the resources that lambda relies on
        # 3.1- DLQ
        if self._dlq_url:
            try:
                exponential_retry(self._sqs.delete_queue, [""], QueueUrl=self._dlq_url)
            except ClientError as error:
                if error.response["Error"]["Code"] not in [
                    "AWS.SimpleQueueService.NonExistentQueue",
                    "AWS.SimpleQueueService.QueueDeletedRecently",
                ]:
                    raise
            self._dlq_url = None

        # 3.2- Its own S3 bucket
        if self._bucket_name and exponential_retry(bucket_exists, [], self._s3, self._bucket_name):
            exponential_retry(empty_bucket, [], self._bucket)
            exponential_retry(delete_bucket, [], self._bucket)
            self._bucket_name = None
            self._bucket = None

        # 4- Detach external connections (simplified version of removal part of _process_external_S3)
        #    TODO refactor along with _process_external_S3
        removed_buckets: Set[str] = set()
        removed_s3_signal_source_access_specs: Set[S3SignalSourceAccessSpec] = set()
        for ext_signal in self._processed_external_signals:
            if ext_signal.resource_access_spec.source == SignalSourceType.S3:
                removed_bucket = ext_signal.resource_access_spec.bucket
                if removed_bucket in removed_buckets:
                    continue
                removed_buckets.add(removed_bucket)
                removed_s3_signal_source_access_specs.add(ext_signal.resource_access_spec)
                # upstream deregistration
                #  - check if signal is from another IF app, if so then let the upstream platform handle it.
                #  - if not, use direct de-registration against the bucket
                upstream_platform = self._get_upstream_platform_for(ext_signal, self._dev_platform)
                if upstream_platform and ext_signal.resource_access_spec.is_mapped_from_upstream():
                    upstream_platform.storage.unsubscribe_downstream_resource(self._dev_platform, "lambda", self._filter_lambda_arn)
                else:
                    # external bucket is raw (not from an RheocerOS app), let's try to do our best to de-register
                    #
                    s3 = self._get_session_for(ext_signal, self._dev_platform).resource("s3")
                    try:
                        # best effort (auto-disconnection) action against external bucket (not owned by current plat/app)
                        remove_notification(s3, removed_bucket, set(), set(), {self._filter_lambda_arn})
                    except Exception as err:
                        # swallow so that the following actions can be taken by the developer to unblock the app.
                        module_logger.critical(
                            f"RheocerOS could not update bucket notification for the removal of lambda: {self._filter_lambda_arn}"
                            f" due to {str(err)}."
                            f" Please manually make sure that external bucket {removed_bucket}"
                            f" BucketNotification does not have the following statement:"
                            f" (LambdaFunctionArn={self._filter_lambda_arn}, events='s3:ObjectCreated:*')"
                            f" or owners of that bucket can grant the following to "
                            f" IF_DEV_ROLE ({self._params[AWSCommonParams.IF_DEV_ROLE]})"
                            f" ['s3:GetBucketNotificationConfiguration', 's3:PutBucketNotificationConfiguration']."
                        )

        if removed_s3_signal_source_access_specs:
            self._remove_from_remote_bucket_policy(removed_s3_signal_source_access_specs)

        # note: no need to revert SNS bindings, during the termination sequence _process_external_SNS will be called
        # with current and new signals swapped. That will do the trick.

        super().terminate()

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)

    @classmethod
    def _get_unique_statement_id_for_s3(cls, bucket: str) -> str:
        return "s3Invoke" + generate_statement_id(bucket)

    @classmethod
    def _get_unique_statement_id_for_cw(cls, timer_id: str) -> str:
        return "cwInvoke" + generate_statement_id(timer_id)

    @classmethod
    def _get_unique_statement_id_for_sns(cls, region: str, account: str, topic: str) -> str:
        return "ext_snsInvoke" + generate_statement_id(region + account + topic)

    def hook_internal(self, route: "Route") -> None:
        super().hook_internal(route)

    def hook_external(self, signals: List[Signal]) -> None:
        # hook early during app activation so that we can fail fast, without letting
        # whole platform (and other constructs) activate themselves and cause a more
        # complicated rollback.
        super().hook_external(signals)

        if any(
            s.resource_access_spec.source
            not in [
                SignalSourceType.S3,
                SignalSourceType.GLUE_TABLE,
                # notifications
                SignalSourceType.SNS,
                # this driver supports the consumption of the following (use-case:
                # external CW signals from the same account).
                SignalSourceType.CW_ALARM,
                SignalSourceType.CW_COMPOSITE_ALARM,
                SignalSourceType.CW_METRIC,
            ]
            for s in signals
        ):
            raise NotImplementedError(
                f"External signal source type for one of " f"'{signals!r} is not supported by {self.__class__.__name__}'"
            )

        # S3 specific check
        for s in signals:
            if s.resource_access_spec.source == SignalSourceType.S3 and not s.resource_access_spec.account_id:
                raise ValueError(
                    f"Account_id for S3 source {s!r} should be provided for {self.__class__.__name__}."
                    f"This is required for RheocerOS' auto-connection mechanism using this resource."
                )
            # TODO this validation is now responsibility of application layer. we can remove this.
            # convenience check to help users to avoid typos, etc in the most common scenario of s3 + glue_table
            if s.resource_access_spec.proxy and s.resource_access_spec.proxy.source == SignalSourceType.GLUE_TABLE:
                glue_access_spec: GlueTableSignalSourceAccessSpec = cast("GlueTableSignalSourceAccessSpec", s.resource_access_spec.proxy)
                if not glue_catalog.check_table(self._session, self._region, glue_access_spec.database, glue_access_spec.table_name):
                    raise ValueError(
                        f"Glue table {glue_access_spec!r} could not be found in account " f"{self._account_id!r} region {self._region!r}!"
                    )

        # note: external notifications (SNS, etc)
        # we assume that application layer should validate them early pre-activation

    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        # we dont need to check each one of those `Route`s,
        # we can send a one-time conn request to Storage.
        # Ex: if Storage is based on S3, it should setup notification for this func.
        #
        # Remark: Storage should take care of everything so that this construct would be totally
        # decoupled from its internal state and future updates.
        # So it can be said that, among constructs, there should not be collaboration in setting up a connection.
        # Example; what if our storage will move from S3 to something else? if that happens, Storage impl
        # has a chance to align/migrate prev state/permissions to new underlying tech. But if responsibilities
        # are distributed among multiple construct, then our update management would be very complicated.
        if new_routes:
            # IMPORTANT this connection has been cancelled:
            #   We realized that this bridge is way too aggressive and actually violating the way orchestrator
            #   has been designed to handle propagation and dependency checks.
            #   In a typical AWS configuration, this bridge means S3 object events from internal storage into orchestrator
            #   core (filter Lambda). When an internal compute is underway, new partition object events for example
            #   might be received by the orchestrator due to this bridge.
            #   But those events (such as completion file resource event) from internal storage are actually redundant
            #   for generic Routing impl since completion is detected via queries to BatchCompute::get_session_state
            #   and then injected into the system as synthetic events (for downstream nodes).
            #   So we currently have no reason to have event-bridge between the storage (e.g S3) and the processor (Lambda)
            #   Besides, it turns out to be problematic in terms of idempotency since for some executions, orchestration
            #   detects the completion on some output (via BatchCompute interface) early and starts executions on
            #   downstream which might get completed before raw (S3) event ingestion for the same output. In this case,
            #   that raw event is basically out of the idempotency check window as active compute is used for that check.
            #   It might end up being evaluated as new incoming signal and might actually cause redundant triggers.
            #   It is also better not to have this to lower inbound event frequency on the (first stage) filter Lambda.
            #
            # self._dev_platform.storage.connect_construct("lambda", self._filter_lambda_arn, self)
            pass
        elif current_routes:
            # TODO support inter-construct disconnection requests
            # self._dev_platform.storage.disconnect_construct("lambda", self._filter_lambda_arn, self)
            pass

    def _process_internal_signals(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        # 1- Timers
        self._process_internal_timers_signals(new_signals, current_signals)
        # add other signal types here

        # 2- Internal notifications? (probably we'll never have them)
        # self._process_internal_notification_signals(new_signals, current_signals)

        new_alarms: List[Signal] = [
            s
            for s in new_signals
            if s.resource_access_spec.source in [SignalSourceType.INTERNAL_ALARM, SignalSourceType.INTERNAL_COMPOSITE_ALARM]
        ]
        current_alarms: List[Signal] = [
            s
            for s in current_signals
            if s.resource_access_spec.source in [SignalSourceType.INTERNAL_ALARM, SignalSourceType.INTERNAL_COMPOSITE_ALARM]
        ]
        if new_alarms:
            self._dev_platform.diagnostics.connect_construct("lambda", self._filter_lambda_arn, self)
        elif current_alarms:
            # TODO support inter-construct disconnection requests
            # self._dev_platform.diagnostics.disconnect_construct("lambda", self._filter_lambda_arn, self)
            pass

    def _process_internal_timers_signals(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        processed_timers: List[Signal] = [s for s in current_signals if s.resource_access_spec.source == SignalSourceType.TIMER]
        new_processed_timers: List[Signal] = [s for s in new_signals if s.resource_access_spec.source == SignalSourceType.TIMER]
        processed_resources: Set[str] = set([s.resource_access_spec.timer_id for s in processed_timers])
        new_processed_resources: Set[str] = set([s.resource_access_spec.timer_id for s in new_processed_timers])

        # resources_to_be_added = new_processed_resources - processed_resources
        resources_to_be_deleted = processed_resources - new_processed_resources

        events = self._session.client(service_name="events", region_name=self._region)

        for processed_signal in processed_timers:
            removed_timer_id = processed_signal.resource_access_spec.timer_id
            if removed_timer_id in resources_to_be_deleted:
                try:
                    exponential_retry(
                        remove_permission,
                        self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                        self._lambda,
                        self._lambda_name,
                        self._get_unique_statement_id_for_cw(removed_timer_id),
                    )
                except ClientError as error:
                    if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                        raise

                try:
                    exponential_retry(
                        events.remove_targets,
                        ["ConcurrentModificationException", "InternalException"],
                        Rule=removed_timer_id,
                        Ids=[
                            self._lambda_name,
                        ],
                    )
                except ClientError as error:
                    if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                        raise

                if processed_signal.resource_access_spec.context_id == self._dev_platform.context_id:
                    # we should delete it only when it is not owned by another app within the same account.
                    try:
                        exponential_retry(
                            events.delete_rule,
                            # refer
                            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/events.html#EventBridge.Client.delete_rule
                            ["ConcurrentModificationException", "InternalException"],
                            Name=removed_timer_id,
                        )
                    except ClientError as error:
                        if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                            raise

        for new_signal in new_processed_timers:
            new_timer_id = new_signal.resource_access_spec.timer_id
            # if new_timer_id in resources_to_be_added:
            statement_id: str = self._get_unique_statement_id_for_cw(new_timer_id)
            try:
                exponential_retry(remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name, statement_id)
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                    raise

            # create the cw rule
            if new_signal.resource_access_spec.context_id == self._dev_platform.context_id:
                # timer is owner by the same context (application)
                response = exponential_retry(
                    events.put_rule,
                    ["ConcurrentModificationException", "InternalException"],
                    Name=new_timer_id,
                    ScheduleExpression=new_signal.resource_access_spec.schedule_expression,
                    State="ENABLED",
                )
                rule_arn = response["RuleArn"]
            else:
                try:
                    response = exponential_retry(events.describe_rule, ["InternalException"], Name=new_timer_id)
                    rule_arn = response["Arn"]
                except ClientError as error:
                    raise

            try:
                exponential_retry(
                    add_permission,
                    self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                    self._lambda,
                    self._lambda_name,
                    statement_id,
                    "lambda:InvokeFunction",
                    "events.amazonaws.com",
                    rule_arn,
                )
                # DO NOT provide account_id for anything other than S3
                # self._account_id)
            except ClientError as error:
                # TODO remove this special handling since we remove and add every time.
                if error.response["Error"]["Code"] not in ["ResourceConflictException"]:
                    raise

            # now connect rule & lambda (add lamda as target on the CW side)
            response = exponential_retry(
                events.put_targets,
                ["ConcurrentModificationException", "InternalException"],
                Rule=new_timer_id,
                Targets=[
                    {
                        "Arn": self._lambda_arn,
                        "Id": self._lambda_name,
                    }
                ],
            )
            if "FailedEntryCount" in response and response["FailedEntryCount"] > 0:
                raise RuntimeError(f"Cannot create timer {new_timer_id} for Lambda {self._lambda_arn}! Response: {response}")

    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        self._process_external_S3(new_signals, current_signals)
        self._process_external_glue_table(new_signals, current_signals)
        self._process_external_SNS(new_signals, current_signals)

    def _process_external_S3(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        processed_resources: Set[str] = set(
            [s.resource_access_spec.bucket for s in current_signals if s.resource_access_spec.source == SignalSourceType.S3]
        )
        new_processed_resources: Set[str] = set(
            [s.resource_access_spec.bucket for s in new_signals if s.resource_access_spec.source == SignalSourceType.S3]
        )

        # resources_to_be_added = new_processed_resources - processed_resources
        resources_to_be_deleted = processed_resources - new_processed_resources

        # 1 - REMOVALS
        removed_buckets: Set[str] = set()
        removed_s3_signal_source_access_specs: Set[S3SignalSourceAccessSpec] = set()
        # for removed_bucket in resources_to_be_deleted:
        for ext_signal in current_signals:
            if ext_signal.resource_access_spec.source == SignalSourceType.S3:
                removed_bucket = ext_signal.resource_access_spec.bucket
                if removed_bucket in resources_to_be_deleted:
                    if removed_bucket in removed_buckets:
                        continue
                    removed_buckets.add(removed_bucket)
                    removed_s3_signal_source_access_specs.add(ext_signal.resource_access_spec)

                    statement_id: str = self._get_unique_statement_id_for_s3(removed_bucket)
                    try:
                        exponential_retry(
                            remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._filter_lambda_name, statement_id
                        )
                    except ClientError as error:
                        if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                            raise

                    # upstream deregistration
                    #  - check if signal is from another IF app, if so then let the upstream platform handle it.
                    #  - if not, use direct de-registration against the bucket
                    upstream_platform = self._get_upstream_platform_for(ext_signal, self._dev_platform)
                    if upstream_platform and ext_signal.resource_access_spec.is_mapped_from_upstream():
                        upstream_platform.storage.unsubscribe_downstream_resource(self._dev_platform, "lambda", self._filter_lambda_arn)
                    else:
                        # external bucket is raw (not from an RheocerOS app), let's try to do our best to de-register
                        #
                        # - remove bucket notification
                        # (dev-role should have permission to do this, otherwise raise and inform the user)
                        # By default, only the bucket owner can configure notifications on a bucket.
                        # However, bucket owners can use a bucket policy to grant permission to other users to set
                        # this configuration with
                        #   s3:GetBucketNotificationConfiguration
                        #   s3:PutBucketNotificationConfiguration
                        # permissions.
                        s3 = self._get_session_for(ext_signal, self._dev_platform).resource("s3")
                        try:
                            # best effort (auto-disconnection) action against external bucket (not owned by current plat/app)
                            remove_notification(s3, removed_bucket, set(), set(), {self._filter_lambda_arn})
                        except Exception as err:
                            # swallow so that the following actions can be taken by the developer to unblock the app.
                            module_logger.critical(
                                f"RheocerOS could not update bucket notification for the removal of lambda: {self._filter_lambda_arn}"
                                f" due to {str(err)}."
                                f" You have two options to setup the event channel:"
                                f""
                                f" 1- Create an SNS topic and add it as an proxy to the external S3 signal. Add the following lambda as a "
                                f" target to the topic: {self._filter_lambda_arn}."
                                f""
                                f" 2- Manually make sure that external bucket {removed_bucket}"
                                f" has the following BucketNotification setup for lambda."
                                f" (LambdaFunctionArn={self._filter_lambda_arn}, events='s3:ObjectCreated:*')"
                                f" or owners of that bucket can grant the following to "
                                f" IF_DEV_ROLE ({self._params[AWSCommonParams.IF_DEV_ROLE]})"
                                f" ['s3:GetBucketNotificationConfiguration', 's3:PutBucketNotificationConfiguration']."
                            )

        if removed_s3_signal_source_access_specs:
            self._remove_from_remote_bucket_policy(removed_s3_signal_source_access_specs)

        # 2 - SURVIVORS + NEW ONES (always doing the check/update even against the existing [common] signals)
        updated_buckets: Set[str] = set()
        s3_signal_source_access_specs: Set[S3SignalSourceAccessSpec] = set()
        for ext_signal in new_signals:
            if ext_signal.resource_access_spec.source == SignalSourceType.S3:
                s3_signal_source_access_specs.add(ext_signal.resource_access_spec)
                ext_bucket = ext_signal.resource_access_spec.bucket

                # Don't do the rest of the loop if bucket is already processed
                # Ex: Two different external signals from the same bucket
                if ext_bucket in updated_buckets:
                    continue
                updated_buckets.add(ext_bucket)

                # 1. add lambda permission
                # TODO post-MVP wipe out previously added permissions
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.remove_permission
                # we need ping-pong pattern for construct buffers (for removal and also for rollbacks).

                statement_id: str = self._get_unique_statement_id_for_s3(ext_bucket)
                try:
                    exponential_retry(
                        remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._filter_lambda_name, statement_id
                    )
                except ClientError as error:
                    if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                        raise

                try:
                    exponential_retry(
                        add_permission,
                        self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                        self._lambda,
                        self._filter_lambda_name,
                        statement_id,
                        "lambda:InvokeFunction",
                        "s3.amazonaws.com",
                        f"arn:aws:s3:::{ext_bucket}",
                        ext_signal.resource_access_spec.account_id,
                    )
                except ClientError as error:
                    if error.response["Error"]["Code"] not in ["ResourceConflictException"]:
                        raise

                # upstream registration
                #  - check if signal is from another IF app, if so then let the upstream platform handle it.
                #  - if not, use direct registration against the bucket
                upstream_platform = self._get_upstream_platform_for(ext_signal, self._dev_platform)
                if upstream_platform and ext_signal.resource_access_spec.is_mapped_from_upstream():
                    upstream_platform.storage.subscribe_downstream_resource(self._dev_platform, "lambda", self._filter_lambda_arn)
                else:
                    # external bucket is raw (not from an RheocerOS app), let's try to do our best
                    # to register hoping that bucket notification conf is still pristine. If not, then
                    # S3 will reject our notification registration due to 'overlapped' conf.
                    # Please a sample arch like: https://aws.amazon.com/blogs/compute/fanout-s3-event-notifications-to-multiple-endpoints/
                    # on the ideal registration scheme (which is what IF uses in between diff apps by default).

                    # check if SNS notification is added as a proxy
                    proxy = ext_signal.resource_access_spec.proxy
                    if not proxy or proxy.source != SignalSourceType.SNS:
                        #
                        # 2. add bucket notification
                        # (dev-role should have permission to do this, otherwise raise and inform the user)
                        # By default, only the bucket owner can configure notifications on a bucket.
                        # However, bucket owners can use a bucket policy to grant permission to other users to set
                        # this configuration with
                        #   s3:GetBucketNotificationConfiguration
                        #   s3:PutBucketNotificationConfiguration
                        # permissions.
                        s3 = self._get_session_for(ext_signal, self._dev_platform).resource("s3")
                        try:
                            # best effort (auto-connection) action against external bucket (not owned by current plat/app)
                            # TODO register notification with 'event filter's so that we won't have conflict with
                            #  other existing notifications that use filters already. If they are also bucket level, then
                            #  that won't work either. This will increase the likelihood of this automation.
                            put_notification(s3, ext_bucket, None, None, [self._filter_lambda_arn], events=["s3:ObjectCreated:*"])
                        except Exception as err:
                            # swallow so that the following actions can be taken by the developer to unblock the app.
                            module_logger.critical(
                                f"IntelliFlow could not setup bucket notification for lambda: {self._filter_lambda_arn}"
                                f" due to {str(err)}."
                                f" Please manually make sure that external bucket {ext_bucket}"
                                f" has the following BucketNotification setup for lambda."
                                f" (LambdaFunctionArn={self._filter_lambda_arn}, events='s3:ObjectCreated:*'"
                                f" or owners of that bucket can grant the following to "
                                f" IF_DEV_ROLE ({self._params[AWSCommonParams.IF_DEV_ROLE]})"
                                f" ['s3:GetBucketNotificationConfiguration', 's3:PutBucketNotificationConfiguration']."
                                f" Also make sure that an overlapping BucketNotification against the same event type"
                                f" does not already exist in the notification configuration."
                            )
            elif ext_signal.resource_access_spec.source == SignalSourceType.GLUE_TABLE:
                continue

        self._update_remote_bucket_policy(s3_signal_source_access_specs)

    def _update_remote_bucket_policy(self, all_s3_signal_source_access_specs: Set[S3SignalSourceAccessSpec]) -> None:
        bucket_to_specs_map: Dict[str, List[S3SignalSourceAccessSpec]] = {}
        for s3_signal_source in all_s3_signal_source_access_specs:
            bucket_to_specs_map.setdefault(s3_signal_source.bucket, []).append(s3_signal_source)

        unique_context_id = self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]
        for bucket, s3_signal_source_access_specs in bucket_to_specs_map.items():
            updated_policy_desc = {
                "Version": "2012-10-17",
                "Id": str(uuid.uuid1()),
                "Statement": [
                    {
                        "Sid": f"{unique_context_id}_dev_role_bucket_access",
                        "Effect": "Allow",
                        "Principal": {"AWS": self._params[AWSCommonParams.IF_DEV_ROLE]},
                        "Action": ["s3:List*", "s3:Put*", "s3:Get*"],
                        "Resource": [f"arn:aws:s3:::{bucket}"],
                    },
                    {
                        "Sid": f"{unique_context_id}_dev_role_object_access",
                        "Effect": "Allow",
                        "Principal": {"AWS": self._params[AWSCommonParams.IF_DEV_ROLE]},
                        "Action": ["s3:Get*"],
                        "Resource": [
                            f'arn:aws:s3:::{bucket}{"/" + s3_access_spec.folder if s3_access_spec.folder else ""}/*'
                            for s3_access_spec in s3_signal_source_access_specs
                        ],
                    },
                    {
                        "Sid": f"{unique_context_id}_exec_role_bucket_access",
                        "Effect": "Allow",
                        "Principal": {"AWS": self._params[AWSCommonParams.IF_EXE_ROLE]},
                        # TODO post-MVP narrow down
                        "Action": ["s3:List*", "s3:Get*"],
                        "Resource": [f"arn:aws:s3:::{bucket}"],
                    },
                    {
                        "Sid": f"{unique_context_id}_exec_role_object_access",
                        "Effect": "Allow",
                        "Principal": {"AWS": self._params[AWSCommonParams.IF_EXE_ROLE]},
                        "Action": ["s3:Get*"],
                        "Resource": [
                            f'arn:aws:s3:::{bucket}{"/" + s3_access_spec.folder if s3_access_spec.folder else ""}/*'
                            for s3_access_spec in s3_signal_source_access_specs
                        ],
                    },
                ],
            }

            # opportunistic loop to find the spec with context info
            # tolerating a scenario where data from an IF app is explicitly marshalled as external data,
            # whereas some others are pulled in via collaboration (from an upstream app).
            access_spec_with_context_info = None
            for s3_access_spec in s3_signal_source_access_specs:
                if not access_spec_with_context_info or s3_access_spec.get_owner_context_uuid():
                    access_spec_with_context_info = s3_access_spec

            s3 = self._get_session_for(access_spec_with_context_info, self._dev_platform).resource("s3")
            try:
                exponential_retry(update_policy, ["MalformedPolicy"], s3, bucket, updated_policy_desc)
            except ClientError as err:
                # swallow so that the following actions can be taken by the developer to unblock the app.
                module_logger.critical(
                    f"RheocerOS could not update the policy of the remote bucket {bucket} for lambda: {self._lambda_arn}"
                    f" due to {str(err)}."
                    f" Please manually make sure that external s3 signal sources {s3_signal_source_access_specs!r}"
                    f" have the following bucket policy: {updated_policy_desc}."
                )

    def _remove_from_remote_bucket_policy(self, all_s3_signal_source_access_specs: Set[S3SignalSourceAccessSpec]) -> None:
        bucket_to_specs_map: Dict[str, List[S3SignalSourceAccessSpec]] = {}
        for s3_signal_source in all_s3_signal_source_access_specs:
            bucket_to_specs_map.setdefault(s3_signal_source.bucket, []).append(s3_signal_source)

        unique_context_id = self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]
        for bucket, s3_signal_source_access_specs in bucket_to_specs_map.items():
            removed_statements = [
                {"Sid": f"{unique_context_id}_dev_role_bucket_access"},
                {"Sid": f"{unique_context_id}_dev_role_object_access"},
                {"Sid": f"{unique_context_id}_exec_role_bucket_access"},
                {"Sid": f"{unique_context_id}_exec_role_object_access"},
            ]

            # opportunistic loop to find the spec with context info
            # tolerating a scenario where data from an IF app is explicitly marshalled as external data,
            # whereas some others are pulled in via collaboration (from an upstream app).
            access_spec_with_context_info = None
            for s3_access_spec in s3_signal_source_access_specs:
                if not access_spec_with_context_info or s3_access_spec.get_owner_context_uuid():
                    access_spec_with_context_info = s3_access_spec

            s3 = self._get_session_for(access_spec_with_context_info, self._dev_platform).resource("s3")
            try:
                exponential_retry(update_policy, ["MalformedPolicy"], s3, bucket, None, removed_statements)
            except ClientError as err:
                # swallow so that the following actions can be taken by the developer to unblock the app.
                module_logger.critical(
                    f"RheocerOS could not update the policy of the remote bucket {bucket} for lambda: {self._lambda_arn}"
                    f" due to {str(err)}."
                )
                module_logger.critical(
                    f" Please manually make sure that external s3 signal sources {s3_signal_source_access_specs!r}"
                    f" does not have the following statements: {removed_statements}."
                )

    @classmethod
    def _get_sns_spec(cls, signal: Signal) -> Optional[SNSSignalSourceAccessSpec]:
        if signal.resource_access_spec.source == SignalSourceType.SNS:
            return signal.resource_access_spec
        elif signal.resource_access_spec.proxy and signal.resource_access_spec.proxy.source == SignalSourceType.SNS:
            return signal.resource_access_spec.proxy

    def _revert_S3_SNS_connections(
        self, current_signals: Set[Signal], unique_context_id: str, survivor_buckets: Set[str], topics_to_be_deleted: Set[str]
    ) -> None:
        # update a bucket if it is removed or its topic is removed. Or update remaining topics for removed publishers.
        for ext_signal in current_signals:
            sns_access_spec: SNSSignalSourceAccessSpec = self._get_sns_spec(ext_signal)
            if sns_access_spec:
                topic = sns_access_spec.topic_arn
                if sns_access_spec.retain_ownership and ext_signal.resource_access_spec.get_owner_context_uuid() == unique_context_id:
                    # attempt to remove the link from the bucket (if bucket is also in the same account)
                    if ext_signal.resource_access_spec.source == SignalSourceType.S3:
                        source_bucket = ext_signal.resource_access_spec.bucket
                        if source_bucket not in survivor_buckets or topic in topics_to_be_deleted:
                            # in both cases, attempt to remove the topic from the bucket
                            s3 = self._get_session_for(ext_signal, self._dev_platform).resource("s3")
                            try:
                                remove_notification(s3, source_bucket, {topic}, None, None)
                            except Exception as err:
                                # swallow so that the following actions can be taken by the developer to unblock the app.
                                module_logger.critical(
                                    f"IntelliFlow could not update bucket notification for the removel of SNS topic: {topic!r}"
                                    f" due to {str(err)}."
                                    f" Please manually make sure that external bucket {source_bucket!r}"
                                    f" BucketNotification does not have the following statement:"
                                    f" (TopicArn={topic}, events='s3:ObjectCreated:*')"
                                    f" or owners of that bucket can grant the following to "
                                    f" IF_DEV_ROLE ({self._params[AWSCommonParams.IF_DEV_ROLE]})"
                                    f" ['s3:GetBucketNotificationConfiguration', 's3:PutBucketNotificationConfiguration']."
                                    f" Also make sure that an overlapping BucketNotification against the same event type"
                                    f" does not already exist in the notification configuration."
                                )

                            # if topic is still around but bucket is gone, then update topic policy
                            if topic not in topics_to_be_deleted:
                                if sns_access_spec.region == self.region:
                                    sns = self._sns
                                else:
                                    sns = self.session.client("sns", region_name=sns_access_spec.region)
                                remove_publisher_from_topic(sns, topic, SNSPublisherType.S3_BUCKET, source_bucket)

    def _remove_SNS_topics(self, current_signals: Set[Signal], unique_context_id: str, topics_to_be_deleted: Set[str]):
        """
        - Break the link between the topic and the lambda
        - Nuke the topic if we own it
        """
        removed_topics: Set[str] = set()
        for ext_signal in current_signals:
            sns_access_spec: SNSSignalSourceAccessSpec = self._get_sns_spec(ext_signal)
            if sns_access_spec:
                removed_topic = sns_access_spec.topic_arn
                removed_topic_name = sns_access_spec.topic
                if removed_topic in topics_to_be_deleted:
                    if removed_topic in removed_topics:
                        continue
                    removed_topics.add(removed_topic)

                    # client from the region that owns the topic
                    sns = self._get_session_for(ext_signal, self._dev_platform).client(
                        service_name="sns", region_name=sns_access_spec.region
                    )
                    # app side client (should be created based on topic region)
                    if sns_access_spec.region == self.region:
                        downstream_sns = self._sns
                    else:
                        # within app's account but using ext signal's region
                        downstream_sns = self._session.client(service_name="sns", region_name=sns_access_spec.region)

                    # 1- attempt to clean-up the topic
                    # Convenience feature for faster data-science experimentation.
                    # Topic management is actually expected from the user.
                    try:
                        # 1- check if subscribed (don't rely on state-machine, be resilient against manual modifications, etc).
                        subscription_arn: Optional[str] = find_subscription(sns, topic_arn=removed_topic, endpoint=self._filter_lambda_arn)
                        if subscription_arn:
                            # if already subscribed, then unsubscribe call from the app session.
                            # please note that we are using the sns client from the app side (app account + region)
                            exponential_retry(downstream_sns.unsubscribe, {"InternalErrorException"}, SubscriptionArn=subscription_arn)

                        if self._account_id != sns_access_spec.account_id:
                            permission_label: str = generate_statement_id(
                                f"{self.__class__.__name__}_{self.get_platform().context_id}_notification_channel"
                            )

                            try:
                                exponential_retry(
                                    sns.remove_permission, {"InternalErrorException"}, TopicArn=removed_topic, Label=permission_label
                                )
                            except sns.exceptions.NotFoundException:
                                pass

                    except ClientError as error:
                        # swallow and report
                        logger.error(
                            f"Could not deregister from the external topic {removed_topic!r} due to error {error!r}!"
                            f" Application filter lambda {self._filter_lambda_arn!r} should be removed from the topic if it is not"
                            f" going to be used/imported again."
                        )

                    # 2- now we can remove the topic related permission from the filter lambda
                    statement_id: str = self._get_unique_statement_id_for_sns(
                        sns_access_spec.region, sns_access_spec.account_id, removed_topic_name
                    )
                    try:
                        exponential_retry(
                            remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._filter_lambda_name, statement_id
                        )
                    except ClientError as error:
                        if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                            raise

                    if sns_access_spec.retain_ownership and ext_signal.resource_access_spec.get_owner_context_uuid() == unique_context_id:
                        # delete the topic
                        try:
                            # This action is idempotent, so deleting a topic that does not exist does not result in an error.
                            exponential_retry(
                                sns.delete_topic,
                                ["InternalError", "InternalErrorException", "ConcurrentAccess", "ConcurrentAccessException"],
                                TopicArn=removed_topic,
                            )
                        except KeyError:
                            # TODO moto sns::delete_topic bug. they maintain an OrderDict for topics which causes KeyError for
                            #   a non-existent key, rather than emulating SNS NotFound exception.
                            pass
                        except ClientError as err:
                            if err.response["Error"]["Code"] not in ["NotFound", "ResourceNotFoundException"]:
                                module_logger.error(f"Couldn't delete SNS topic {removed_topic!r}! Error:" f"{str(err)}")
                                raise

    def _add_or_update_SNS_topics(self, new_signals: Set[Signal], unique_context_id: str) -> None:
        """
        Go over all of the topics that exist in the new version of the app topology. Here the param "new_signals" contains
        brand new external signals or the ones that exist in both the previous and new versions of the app.

        - Create the topic if we own it
        - Establish the link between the topic and the lambda, so that events will be received by the app at runtime
        """
        updated_topics: Set[str] = set()
        for ext_signal in new_signals:
            sns_access_spec: SNSSignalSourceAccessSpec = self._get_sns_spec(ext_signal)
            if sns_access_spec:
                updated_topic = sns_access_spec.topic_arn
                updated_topic_name = sns_access_spec.topic
                if updated_topic in updated_topics:
                    continue
                updated_topics.add(updated_topic)

                sns = self._get_session_for(ext_signal, self._dev_platform).client(service_name="sns", region_name=sns_access_spec.region)

                # 0- check the existence if the app retains the topic
                if sns_access_spec.retain_ownership and ext_signal.resource_access_spec.get_owner_context_uuid() == unique_context_id:
                    if not topic_exists(sns, updated_topic):
                        try:
                            exponential_retry(
                                sns.create_topic, ["InternalErrorException", "ConcurrentAccessException"], Name=updated_topic_name
                            )
                        except ClientError as err:
                            module_logger.error(f"Couldn't create SNS topic {updated_topic!r}! Error: %s", str(err))
                            raise

                # 1- we can add the topic related permission to the filter lambda
                statement_id: str = self._get_unique_statement_id_for_sns(
                    sns_access_spec.region, sns_access_spec.account_id, updated_topic_name
                )
                try:
                    exponential_retry(
                        remove_permission,
                        self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                        self._lambda,
                        self._filter_lambda_name,
                        statement_id,
                    )
                except ClientError as error:
                    if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                        raise

                exponential_retry(
                    add_permission,
                    self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                    self._lambda,
                    self._filter_lambda_arn,
                    statement_id,
                    "lambda:InvokeFunction",
                    "sns.amazonaws.com",
                    updated_topic,
                )

                # 2- attempt to give permission to SNS to call this lambda resource owned by our platform.
                #    convenience feature to avoid extra configuration on the topic side in most cases (where topic resides in the same account)
                try:
                    if self._account_id != sns_access_spec.account_id:
                        # 1 - check permission to downstream resource
                        permission_label: str = generate_statement_id(
                            f"{self.__class__.__name__}_{self.get_platform().context_id}_notification_channel"
                        )

                        # TODO replace the following (acc based) permission control code with a more granular policy based version.
                        # similar to what we do in 'AWSS3StorageBasic::_setup_event_channel' using 'self._topic_root_policy', and then
                        # calling 'self._sns.set_topic_attributes'
                        try:
                            exponential_retry(
                                sns.remove_permission, {"InternalErrorException"}, TopicArn=updated_topic, Label=permission_label
                            )
                        except sns.exceptions.NotFoundException:
                            pass

                        exponential_retry(
                            sns.add_permission,
                            {"InternalErrorException"},
                            TopicArn=updated_topic,
                            Label=permission_label,
                            AWSAccountId=[self.account_id],
                            ActionName=["Receive", "Subscribe"],
                        )

                    # and finally the actual subscribe call can now be made from app side session.
                    # if upstream makes this call on behalf of downstream, then downstream lambda should
                    # catch SNS notification with a 'token' and then call 'confirm_subscription' (we avoid this).
                    if sns_access_spec.region == self.region:
                        downstream_sns = self._sns
                    else:
                        # within app's account but using ext signal's region
                        downstream_sns = self._session.client(service_name="sns", region_name=sns_access_spec.region)

                    exponential_retry(
                        downstream_sns.subscribe,
                        {"InternalErrorException"},
                        TopicArn=updated_topic,
                        Protocol="lambda",
                        Endpoint=self._filter_lambda_arn,
                    )
                except ClientError as error:
                    # swallow and report
                    logger.error(
                        f"Could not register to the external topic {updated_topic!r} due to error {error!r}!"
                        f" Application filter lambda {self._filter_lambda_arn!r} should be manually added to the topic if event-based"
                        f" ingestion/triggers are expected within this application."
                    )

    def _add_or_update_S3_SNS_connections(self, new_signals: Set[Signal], unique_context_id: str):
        """Go over all of the S3 external signals (buckets) that exist in the new version of the app topology. Here the param "new_signals" contains
        brand new external signals or the ones that exist in both the previous and new versions of the app.

        Here we establish an S3-SNS connection or update it if the user wants the framework manage the topic (via "retain_ownership").

        A connection has two ends:
        - Update the bucket notification to channel S3 events into the topic
        - Update the topic to grant S3 bucket to "sns:Publish"
        """
        updated_resources: Set[str] = set()
        for ext_signal in new_signals:
            sns_access_spec: SNSSignalSourceAccessSpec = self._get_sns_spec(ext_signal)
            if sns_access_spec:
                updated_topic = sns_access_spec.topic_arn
                updated_topic_name = sns_access_spec.topic
                if sns_access_spec.retain_ownership and ext_signal.resource_access_spec.get_owner_context_uuid() == unique_context_id:
                    # special treatment for S3 bucket
                    # 1- we have to do policy update all the time because:
                    #    - same topic might be associated with different buckets
                    #    - in between different versions of the application, same topic can switch to a different bucket
                    #    so we need to make policy update incremental as well, to accommodate multiple buckets.
                    # 2- update the newly created topic with notifications out of the bucket
                    if ext_signal.resource_access_spec.source == SignalSourceType.S3:
                        source_bucket = ext_signal.resource_access_spec.bucket
                        if source_bucket in updated_resources:
                            continue
                        updated_resources.add(source_bucket)

                        if sns_access_spec.region == self.region:
                            sns = self._sns
                        else:
                            sns = self.session.client("sns", region_name=sns_access_spec.region)

                        # 1- update the policy
                        add_publisher_to_topic(sns, updated_topic, SNSPublisherType.S3_BUCKET, source_bucket)

                        # 2- always try to update the bucket notification even if the topic is not new
                        s3 = self._get_session_for(ext_signal, self._dev_platform).resource("s3")
                        source_bucket = ext_signal.resource_access_spec.bucket
                        try:
                            exponential_retry(
                                put_notification,
                                {"ServiceException", "TooManyRequestsException"},
                                s3,
                                source_bucket,
                                {updated_topic},
                                {},
                                {},
                                events=["s3:ObjectCreated:*"],
                            )
                        except Exception as err:
                            # swallow so that the following actions can be taken by the developer to unblock the app.
                            module_logger.critical(
                                f"IntelliFlow could not setup bucket notification for topic: {updated_topic}"
                                f" due to {str(err)}."
                                f" Please manually make sure that external bucket {source_bucket}"
                                f" has the following BucketNotification setup for the topic:"
                                f" (TopicArn={updated_topic}, events='s3:ObjectCreated:*')"
                                f" or owners of that bucket can grant the following to "
                                f" IF_DEV_ROLE ({self._params[AWSCommonParams.IF_DEV_ROLE]})"
                                f" ['s3:GetBucketNotificationConfiguration', 's3:PutBucketNotificationConfiguration']."
                                f" Also make sure that an overlapping BucketNotification against the same event type"
                                f" does not already exist in the notification configuration."
                            )

    def _process_external_SNS(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        """
        Here we evaluate two types of SNS signals:
         - SNS signals linked with upstream AWS resources such as S3 bucket (aka proxy signals)
         - Pure / standalone imports of SNS topics into the app

         In the most simple case, we just make sure that those topics can publish into the Lambdas managed by this driver and
         we will be able to ingest events from those topics. In the most sophisticated case (where we retain the ownership of the topic),
         we try to control the first hop/link between source AWS resource (S3 bucket -> SNS), create the topic if non existent and then again
         link the topic to the lambda here.

         So there are four categories of operations here. They can be grouped as below:

        1- removed topics
           1.1- effect of removed topics on upstream buckets when we "retain ownership" (do the cleanup in upstream bucket policies)
           1.2- cleanup Lambda's permission for the topic (invoke permission in Lambda granted to the topic)
           1.3- delete the topic when we "retain ownership"

        2- removed buckets (on a survivor topic). a topic can be reused for multiple buckets. so this is the case when one of the buckets is removed
        from the app. In this case:
           2.1- update the topic that forwards the bucket. drop permission in topic policy (sns:publish permission in topic granted to the bucket)
           2.2- attempt to update bucket's notification if we "retain owership". clean up bucket's notification and remove the topic as the notification target.

        3- added topics (brand new topics)
           3.1- if "retained", create the topic and attempt to register it to upstream bucket's notification list
           3.2- update permission registry of topic (allow bucket to publish)
           3.3- create the link between the topic and the lambda here so that it can publish into the app (so that we can ingest events from the topic,
           see event_handler method in this driver)

        4- new buckets (no change on topics). like a new bucket being registered on a topic that was used in another "marshal_external_data" call and linked to another bucket already.
           4.1- if "retain_ownership", then update the notification
           4.2- update permission registry of topic (allow bucket to publish)


        In all of those main operations, sns clients are instantiated specifically for each signal. Reason:
        - driver level sns client (self._sns) cannot be used if the external signal (topic) is from a different account or region
        - if from a different account, we try to retrieve the session from upstream platform. we check if the topic is imported from an upstream IF app (IF collaboration feature).
        - and when we are about to finalize the link between the topic and the lambda here, we need to make the "sns:subscribe" call from the lambda account (app account) but
        using the region of the topic (another reason why self._sns cannot be used).
        """
        processed_resources: Set[str] = set()
        old_bucket_to_sns_map: Dict[str, str] = dict()
        for s in current_signals:
            sns_spec = self._get_sns_spec(s)
            if sns_spec:
                processed_resources.add(sns_spec.topic_arn)
                if s.resource_access_spec.source == SignalSourceType.S3:
                    # SNS topic given as proxy to a bucket, let's see if the same bucket has any other
                    old_bucket_to_sns_map[s.resource_access_spec.bucket] = sns_spec.topic_arn

        new_processed_resources: Set[str] = set()
        # while extracting new topics, also validate if application misconfigures a bucket with multiple topics
        new_bucket_to_sns_map: Dict[str, Set[str]] = dict()
        for s in new_signals:
            sns_spec = self._get_sns_spec(s)
            if sns_spec:
                new_processed_resources.add(sns_spec.topic_arn)
                if s.resource_access_spec.source == SignalSourceType.S3:
                    # SNS topic given as proxy to a bucket, let's see if the same bucket has any other
                    new_bucket_to_sns_map.setdefault(s.resource_access_spec.bucket, set()).add(sns_spec.topic_arn)

        # now check if same bucket has multiple notifications defined
        bucket_with_sns_overlap = {bucket: topics for bucket, topics in new_bucket_to_sns_map.items() if len(topics) > 1}
        if bucket_with_sns_overlap:
            raise ValueError(
                f"There are overlapping SNS notifications on one or more buckets! Buckets with conflicting topics: {bucket_with_sns_overlap}"
            )

        # resources_to_be_added = new_processed_resources - processed_resources
        resources_to_be_deleted = processed_resources - new_processed_resources

        unique_context_id = self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]

        # 1 - REMOVALS
        # 1.1
        self._revert_S3_SNS_connections(current_signals, unique_context_id, set(new_bucket_to_sns_map.keys()), resources_to_be_deleted)

        # 1.2
        self._remove_SNS_topics(current_signals, unique_context_id, resources_to_be_deleted)

        # 2 - SURVIVORS + NEW ONES (always doing the check/update even against the existing [common] signals)
        # 2.1 establish links between topics and the lambda
        self._add_or_update_SNS_topics(new_signals, unique_context_id)

        # 2.2
        # establish links for publishers (e.g s3 bucket) for the topics that this app owns
        self._add_or_update_S3_SNS_connections(new_signals, unique_context_id)

    def _process_external_glue_table(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        def _is_gt(s: Signal) -> bool:
            return (
                s.resource_access_spec.source == SignalSourceType.GLUE_TABLE
                or s.resource_access_spec.proxy
                and s.resource_access_spec.proxy.source == SignalSourceType.GLUE_TABLE
            )

        has_new_gt_signal = any([_is_gt(s) for s in new_signals])
        has_gt_signal_already = any([_is_gt(s) for s in current_signals])

        if not has_new_gt_signal and has_gt_signal_already:
            # we used to have glue table signal in this application but not anymore
            # we need to delete the glue catalog event channel
            try:
                exponential_retry(
                    remove_permission,
                    self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                    self._lambda,
                    self._filter_lambda_name,
                    self._get_unique_statement_id_for_cw(self._glue_catalog_event_channel_rule_id),
                )
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                    raise
        elif new_signals and not current_signals:
            # we did not have any glue table signal before, but we do now
            # provision the event channel now
            self._activate_glue_catalog_event_channel()

    def _process_external_upstream_alarms(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        """Only alarms from upstream platforms need to be supported. The rest should be taken care of by Diagnostics driver."""
        supported_external_alarm_sources = [SignalSourceType.CW_ALARM, SignalSourceType.CW_COMPOSITE_ALARM]
        processed_resources: Set[str] = set(
            [
                s.resource_access_spec.path_format
                for s in current_signals
                if s.resource_access_spec.source in supported_external_alarm_sources
            ]
        )
        new_processed_resources: Set[str] = set(
            [s.resource_access_spec.path_format for s in new_signals if s.resource_access_spec.source in supported_external_alarm_sources]
        )

        # resources_to_be_added = new_processed_resources - processed_resources
        resources_to_be_deleted = processed_resources - new_processed_resources

        # 1 - REMOVALS
        removed_buckets: Set[str] = set()
        removed_s3_signal_source_access_specs: Set[S3SignalSourceAccessSpec] = set()
        # for removed_bucket in resources_to_be_deleted:
        for ext_signal in current_signals:
            if ext_signal.resource_access_spec.source == SignalSourceType.S3:
                removed_bucket = ext_signal.resource_access_spec.bucket
                if removed_bucket in resources_to_be_deleted:
                    if removed_bucket in removed_buckets:
                        continue
                    removed_buckets.add(removed_bucket)
                    removed_s3_signal_source_access_specs.add(ext_signal.resource_access_spec)

                    statement_id: str = self._get_unique_statement_id_for_s3(removed_bucket)
                    try:
                        exponential_retry(
                            remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._filter_lambda_name, statement_id
                        )
                    except ClientError as error:
                        if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                            raise

                    # upstream deregistration
                    #  - check if signal is from another IF app, if so then let the upstream platform handle it.
                    #  - if not, use direct de-registration against the bucket
                    upstream_platform = self._get_upstream_platform_for(ext_signal, self._dev_platform)
                    if upstream_platform and ext_signal.resource_access_spec.is_mapped_from_upstream():
                        upstream_platform.storage.unsubscribe_downstream_resource(self._dev_platform, "lambda", self._filter_lambda_arn)
                    else:
                        # external bucket is raw (not from an RheocerOS app), let's try to do our best to de-register
                        #
                        # - remove bucket notification
                        # (dev-role should have permission to do this, otherwise raise and inform the user)
                        # By default, only the bucket owner can configure notifications on a bucket.
                        # However, bucket owners can use a bucket policy to grant permission to other users to set
                        # this configuration with
                        #   s3:GetBucketNotificationConfiguration
                        #   s3:PutBucketNotificationConfiguration
                        # permissions.
                        s3 = self._get_session_for(ext_signal, self._dev_platform).resource("s3")
                        try:
                            # best effort (auto-disconnection) action against external bucket (not owned by current plat/app)
                            remove_notification(s3, removed_bucket, set(), set(), {self._filter_lambda_arn})
                        except Exception as err:
                            # swallow so that the following actions can be taken by the developer to unblock the app.
                            module_logger.critical(
                                f"RheocerOS could not update bucket notification for the removal of lambda: {self._filter_lambda_arn}"
                                f" due to {str(err)}."
                                f" Please manually make sure that external bucket {removed_bucket}"
                                f" has the following BucketNotification setup for lambda."
                                f" (LambdaFunctionArn={self._filter_lambda_arn}, events='s3:ObjectCreated:*'"
                                f" or owners of that bucket can grant the following to "
                                f" IF_DEV_ROLE ({self._params[AWSCommonParams.IF_DEV_ROLE]})"
                                f" ['s3:GetBucketNotificationConfiguration', 's3:PutBucketNotificationConfiguration']."
                            )

        if removed_s3_signal_source_access_specs:
            self._remove_from_remote_bucket_policy(removed_s3_signal_source_access_specs)

        # 2 - SURVIVORS + NEW ONES (always doing the check/update even against the existing [common] signals)
        updated_buckets: Set[str] = set()
        s3_signal_source_access_specs: Set[S3SignalSourceAccessSpec] = set()
        for ext_signal in new_signals:
            if ext_signal.resource_access_spec.source == SignalSourceType.S3:
                ext_bucket = ext_signal.resource_access_spec.bucket
                # Don't do the rest of the loop if bucket is already processed
                # Ex: Two different external signals from the same bucket
                if ext_bucket in updated_buckets:
                    continue
                updated_buckets.add(ext_bucket)
                s3_signal_source_access_specs.add(ext_signal.resource_access_spec)

                # 1. add lambda permission
                # TODO post-MVP wipe out previously added permissions
                # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.remove_permission
                # we need ping-pong pattern for construct buffers (for removal and also for rollbacks).

                statement_id: str = self._get_unique_statement_id_for_s3(ext_bucket)
                try:
                    exponential_retry(
                        remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._filter_lambda_name, statement_id
                    )
                except ClientError as error:
                    if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                        raise

                try:
                    exponential_retry(
                        add_permission,
                        self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                        self._lambda,
                        self._filter_lambda_name,
                        statement_id,
                        "lambda:InvokeFunction",
                        "s3.amazonaws.com",
                        f"arn:aws:s3:::{ext_bucket}",
                        ext_signal.resource_access_spec.account_id,
                    )
                except ClientError as error:
                    if error.response["Error"]["Code"] not in ["ResourceConflictException"]:
                        raise

                # upstream registration
                #  - check if signal is from another IF app, if so then let the upstream platform handle it.
                #  - if not, use direct registration against the bucket
                upstream_platform = self._get_upstream_platform_for(ext_signal, self._dev_platform)
                if upstream_platform and ext_signal.resource_access_spec.is_mapped_from_upstream():
                    upstream_platform.storage.subscribe_downstream_resource(self._dev_platform, "lambda", self._filter_lambda_arn)
                else:
                    # external bucket is raw (not from an RheocerOS app), let's try to do our best
                    # to register hoping that bucket notification conf is still pristine. If not, then
                    # S3 will reject our notification registration due to 'overlapped' conf.
                    # Please a sample arch like: https://aws.amazon.com/blogs/compute/fanout-s3-event-notifications-to-multiple-endpoints/
                    # on the ideal registration scheme (which is what IF uses in between diff apps by default).
                    #
                    # 2. add bucket notification
                    # (dev-role should have permission to do this, otherwise raise and inform the user)
                    # By default, only the bucket owner can configure notifications on a bucket.
                    # However, bucket owners can use a bucket policy to grant permission to other users to set
                    # this configuration with
                    #   s3:GetBucketNotificationConfiguration
                    #   s3:PutBucketNotificationConfiguration
                    # permissions.
                    s3 = self._get_session_for(ext_signal, self._dev_platform).resource("s3")
                    try:
                        # best effort (auto-connection) action against external bucket (not owned by current plat/app)
                        # TODO register notification with 'event filter's so that we won't have conflict with
                        #  other existing notifications that use filters already. If they are also bucket level, then
                        #  that won't work either. This will increase the likelihood of this automation.
                        put_notification(s3, ext_bucket, None, None, [self._filter_lambda_arn], events=["s3:ObjectCreated:*"])
                    except Exception as err:
                        # swallow so that the following actions can be taken by the developer to unblock the app.
                        module_logger.critical(
                            f"RheocerOS could not setup bucket notification for lambda: {self._filter_lambda_arn}"
                            f" due to {str(err)}."
                            f" Please manually make sure that external bucket {ext_bucket}"
                            f" has the following BucketNotification setup for lambda."
                            f" (LambdaFunctionArn={self._filter_lambda_arn}, events='s3:ObjectCreated:*'"
                            f" or owners of that bucket can grant the following to "
                            f" IF_DEV_ROLE ({self._params[AWSCommonParams.IF_DEV_ROLE]})"
                            f" ['s3:GetBucketNotificationConfiguration', 's3:PutBucketNotificationConfiguration']."
                            f" Also make sure that an overlapping BucketNotification against the same event type"
                            f" does not already exist in the notification configuration."
                        )
            elif ext_signal.resource_access_spec.source == SignalSourceType.GLUE_TABLE:
                # TODO placeholder for other data technologies
                continue
            else:
                raise NotImplementedError(
                    f"External signal source type "
                    f"'{ext_signal.resource_access_spec.source} is not supported by {self.__class__.__name__}'"
                )

        self._update_remote_bucket_policy(s3_signal_source_access_specs)

    def _activate_glue_catalog_event_channel(self) -> None:
        # Need to fix the statement id limit here. Max 100 characters in the statement id
        # As a temporary fix, have reduced the suffix to the catalog event channel rule id
        statement_id: str = self._get_unique_statement_id_for_cw(self._glue_catalog_event_channel_rule_id)
        try:
            exponential_retry(remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._filter_lambda_name, statement_id)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                raise

        try:
            cw_event_source: CatalogCWEventResource = exponential_retry(
                provision_cw_event_rule,
                ["ConcurrentModificationException", "InternalException"],
                self._session,
                self._region,
                self._glue_catalog_event_channel_rule_id,
                self._filter_lambda_name,
                self._filter_lambda_arn,
            )
        except ClientError as error:
            raise

        try:
            lambda_permission = cw_event_source.get_lambda_trigger_permission()
            exponential_retry(
                add_permission,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._lambda,
                self._filter_lambda_name,
                statement_id,
                lambda_permission.action,
                lambda_permission.principal,
                lambda_permission.source_arn,
            )
        except ClientError as error:
            # TODO remove this special handling since we remove and add every time.
            if error.response["Error"]["Code"] not in ["ResourceConflictException"]:
                raise

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

    def _revert_security_conf(selfs, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        pass

    @staticmethod
    def runtime_bootstrap() -> "RuntimePlatform":
        # dropped due to 4KB limit
        # serialized_encoded_compressed_bootstrapper: str = os.environ['bootstrapper']
        # serialized_compressed_bootstrapper = codecs.decode(serialized_encoded_compressed_bootstrapper.encode(), 'base64')
        # serialized_bootstrapper = zlib.decompress(serialized_compressed_bootstrapper).decode()
        bootstrapper_bucket = os.environ["bootstrapper_bucket"]
        bootstrapper_key = os.environ["bootstrapper_key"]
        _s3 = boto3.resource("s3")
        _bucket = get_bucket(_s3, bootstrapper_bucket)
        serialized_bootstrapper = get_object(_bucket, bootstrapper_key).decode()

        from intelliflow.core.platform.development import RuntimePlatform

        runtime_platform: RuntimePlatform = RuntimePlatform.deserialize(serialized_bootstrapper)
        runtime_platform.runtime_init(runtime_platform.processor)
        return runtime_platform

    def event_handler(
        self, runtime_platform: "Platform", event, context, filter_only: bool = False, routing_session: RoutingSession = RoutingSession()
    ) -> List[RoutingTable.Response]:
        # TODO check processor_queue
        #  - based on another CW event
        # currently we check the queue optimistically at the end of each cycle
        routing_responses: List[RoutingTable.Response] = []
        target_route_id = event.get(SIGNAL_TARGET_ROUTE_ID_KEY, None)
        is_blocked = event.get(SIGNAL_IS_BLOCKED_KEY, False)
        serialized_feed_back_signal: str = event.get(FEEDBACK_SIGNAL_TYPE_EVENT_KEY, None)

        if not serialized_feed_back_signal:
            module_logger.info(f"Processing event: {event!r}")

        if serialized_feed_back_signal:
            feed_back_signal: Signal = Signal.deserialize(serialized_feed_back_signal)
            module_logger.info(f"Processing internal event: {feed_back_signal.unique_key()!r}")
            processing_mode: FeedBackSignalProcessingMode = FeedBackSignalProcessingMode(
                event.get(FEEDBACK_SIGNAL_PROCESSING_MODE_KEY, FeedBackSignalProcessingMode.FULL_DOMAIN.value)
            )
            # TODO move this block to RoutingTable::receive(feed_back: Signal)
            if feed_back_signal.type not in [
                SignalType.INTERNAL_PARTITION_CREATION_REQUEST,
                SignalType.EXTERNAL_S3_OBJECT_CREATION_REQUEST,
            ]:
                self.metric(f"processor{'.filter' if filter_only else ''}.event.type")["Internal"].emit(1)
                for source_spec in feed_back_signal.get_materialized_access_specs():
                    resource_path = source_spec.path_format
                    if feed_back_signal.resource_access_spec.path_format_requires_resource_name():
                        # feed-back signals should be mapped to real resources for routing to work.
                        # we get help from the integrity_check_protocol attached to the signal.
                        # we mimic real event notification.
                        required_resource_name = "_resource"
                        if feed_back_signal.domain_spec.integrity_check_protocol:
                            integrity_checker = INTEGRITY_CHECKER_MAP[feed_back_signal.domain_spec.integrity_check_protocol.type]
                            required_resource_name = integrity_checker.get_required_resource_name(
                                source_spec, feed_back_signal.domain_spec.integrity_check_protocol
                            )

                        resource_path = resource_path + source_spec.path_delimiter() + required_resource_name
                    routing_response = runtime_platform.routing_table.receive(
                        feed_back_signal.type, source_spec.source, resource_path, target_route_id, filter_only, routing_session, is_blocked
                    )
                    routing_responses.append(routing_response)
                    if processing_mode == FeedBackSignalProcessingMode.ONLY_HEAD:
                        break
            elif feed_back_signal.type in [SignalType.INTERNAL_PARTITION_CREATION_REQUEST]:
                # TODO
                # - Ask routingtable about the "Dependencies" of the internal partition.
                # - Convert the signal into a multi-cast to dependencies. However;
                #   - use integrity_check_protocol of dependencies to see if they are ready.
                #       - if dependency is ready, change the signal type (i.e remove REQUEST suffix)
                #       - if not ready, keep the signal type.
                # - Send the signals into ProcessorQueue.
                pass
            else:
                # FUTURE auto-handling of inter-application feed-back, access remoteapplication platform
                # convert event type to respective OBJECT_CREATION events and then dispatch them.
                pass
        elif "Records" in event and event["Records"]:
            for raw_record in event["Records"]:
                s3_records = []
                alarm_records = []
                if "Sns" in raw_record:
                    self.metric(f"processor{'.filter' if filter_only else ''}.event.type")["SNS"].emit(1)
                    sns_message = json.loads(raw_record["Sns"]["Message"])
                    if "Records" in sns_message:
                        sns_records = json.loads(raw_record["Sns"]["Message"])
                        for record in sns_records["Records"]:
                            if "s3" in record:
                                s3_records.append(record)
                            elif (
                                "source" in record
                                and record["source"] == "aws.cloudwatch"
                                and "detail" in record
                                and "alarmName" in record["detail"]
                            ):
                                # refer the following for CW alarms:
                                #  https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch-and-eventbridge.html
                                alarm_records.append(record)
                    elif "AlarmName" in sns_message and "AlarmArn" in sns_message:
                        alarm_records.append(sns_message)
                    else:
                        self.metric(f"processor{'.filter' if filter_only else ''}.event.type")["Unrecognized"].emit(1)
                        msg = f"Unrecognized message {sns_message!r} in SNS event!"
                        module_logger.error(msg)
                        return routing_responses
                elif "s3" in raw_record:
                    s3_records.append(raw_record)
                elif (
                    "source" in raw_record
                    and raw_record["source"] == "aws.cloudwatch"
                    and "detail" in raw_record
                    and "alarmName" in raw_record["detail"]
                ):
                    alarm_records.append(raw_record)

                if s3_records:
                    self.metric(f"processor{'.filter' if filter_only else ''}.event.type")["S3"].emit(len(s3_records))
                if alarm_records:
                    self.metric(f"processor{'.filter' if filter_only else ''}.event.type")["CW_Alarm"].emit(len(alarm_records))

                for record in s3_records:
                    bucket = record["s3"]["bucket"]["name"]
                    key = unquote_plus(record["s3"]["object"]["key"])
                    # FIXME (temp) special handling of completion indicator for batch-compute jobs running in Spark
                    # if we don't do this, batch compute drivers should handle this in a more complicated manner.
                    # see
                    # https://code.dblock.org/2017/04/27/writing-data-to-amazon-s3-from-apache-spark.html
                    # https://stackoverflow.com/questions/24371259/how-to-make-saveastextfile-not-split-output-into-multiple-file
                    res_head, res_sep, res_tail = key.rpartition("/part-00000")
                    if not res_tail:  # match in the end (in other cases res_tail == key or some other tail value)
                        module_logger.critical(
                            f"Detected completion indicator partition on bucket: {bucket}, key: {key}!"
                            f"Will use {res_head} as the new resource key for the indicator."
                        )
                        key = res_head
                    resource_path: str = f"s3://{bucket}/{key}"

                    # check if it is internal, so that
                    # signal_type reflects right INTERNAL signal type.
                    if runtime_platform.storage.is_internal(SignalSourceType.S3, resource_path):
                        signal_type = SignalType.INTERNAL_PARTITION_CREATION
                    else:
                        signal_type = SignalType.EXTERNAL_S3_OBJECT_CREATION

                    # routing table will take care of the rest.
                    # reactions are expected to be put in processor queue.
                    # let router will take care of them as well.
                    routing_response = runtime_platform.routing_table.receive(
                        signal_type, SignalSourceType.S3, resource_path, target_route_id, filter_only, routing_session, is_blocked
                    )
                    routing_responses.append(routing_response)

                for record in alarm_records:
                    if "detail" in record and "alarmName" in record["detail"]:
                        # https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch-and-eventbridge.html
                        name = record["detail"]["alarmName"]
                        account = record["account"]
                        region = record["region"]

                        new_state = AlarmState(record["detail"]["state"]["value"])
                        time_str = record["detail"]["state"]["timestamp"]

                        is_composite = "configuration" not in record["detail"] or "metrics" not in record["detail"]["configuration"]
                    else:
                        name = record["AlarmName"]
                        account = record["AWSAccountId"]
                        region = record["AlarmArn"].split(":")[3]

                        new_state = AlarmState(record["NewStateValue"])
                        time_str = record["StateChangeTime"]

                        is_composite = True if "AlarmRule" in record else False

                    if not is_composite:
                        resource_path = CWAlarmSignalSourceAccessSpec.create_resource_path(name, account, region, new_state, time_str)
                        source_type = SignalSourceType.CW_ALARM
                    else:
                        resource_path = CWCompositeAlarmSignalSourceAccessSpec.create_resource_path(
                            name, account, region, new_state, time_str
                        )
                        source_type = SignalSourceType.CW_COMPOSITE_ALARM

                    # check if it is internal, so that
                    # signal_type reflects right INTERNAL signal type.
                    if runtime_platform.diagnostics.is_internal(source_type, resource_path):
                        signal_type = SignalType.INTERNAL_ALARM_STATE_CHANGE
                    else:
                        signal_type = SignalType.CW_ALARM_STATE_CHANGE

                    routing_response = runtime_platform.routing_table.receive(
                        signal_type, source_type, resource_path, target_route_id, filter_only, routing_session, is_blocked
                    )
                    routing_responses.append(routing_response)
        elif "source" in event and event["source"] == "aws.glue":
            self.metric(f"processor{'.filter' if filter_only else ''}.event.type")["Glue"].emit(1)
            type_of_change = event["detail"]["typeOfChange"]
            # this check also makes sure that "detail-type" / "Glue Data Catalog Table State Change"
            # for generic Glue Table events that this Processor is interested in.
            #   see https://docs.aws.amazon.com/glue/latest/dg/automating-awsglue-with-cloudwatch-events.html
            if type_of_change in GlueTableSignalSourceAccessSpec.SIGNAL_GENERATOR_TABLE_EVENTS:
                provider = event["detail"]["databaseName"]
                table_name = event["detail"]["tableName"]
                changed_partitions = event["detail"]["changedPartitions"]

                for changed_partition in changed_partitions:
                    changed_partition_list = [part.strip() for part in changed_partition.strip("][").split(",")]

                    # now we let the incoming partitions into the RoutingTable and each route is now supposed to filter
                    # out unnecessary partition values based on their metadata.
                    partition_key_list = changed_partition_list

                    # Evaluate as generic Glue Table event
                    resource_path: str = GlueTableSignalSourceAccessSpec.create_materialized_path(provider, table_name, partition_key_list)
                    if GlueTableSignalSourceAccessSpec.path_format_requires_resource_name():
                        # be adaptive in case Glue access spec starts supporting
                        resource_path = resource_path + GlueTableSignalSourceAccessSpec.path_delimiter() + type_of_change

                    routing_response = runtime_platform.routing_table.receive(
                        SignalType.EXTERNAL_GLUE_TABLE_PARTITION_UPDATE,
                        SignalSourceType.GLUE_TABLE,
                        resource_path,
                        target_route_id,
                        filter_only,
                        routing_session,
                        is_blocked,
                    )
                    routing_responses.append(routing_response)
        elif "source" in event and event["source"] == "aws.events":
            self.metric(f"processor{'.filter' if filter_only else ''}.event.type")["EventBridge"].emit(1)
            for resource in event["resources"]:
                event_time = event["time"]
                try:
                    # expecting 'resource' to be a rule arn now
                    timer_resource_partitioned = unquote_plus(resource).rsplit("/", 1)
                    timer_id = timer_resource_partitioned[-1]
                    if timer_id != self._main_loop_timer_id:
                        # if timer_id is not internal poller
                        resource_path: str = TimerSignalSourceAccessSpec.create_resource_path(timer_id, event_time)
                        routing_response = runtime_platform.routing_table.receive(
                            SignalType.TIMER_EVENT,
                            SignalSourceType.TIMER,
                            resource_path,
                            target_route_id,
                            filter_only,
                            routing_session,
                            is_blocked,
                        )
                        routing_responses.append(routing_response)
                    else:
                        self.metric(f"processor{'.filter' if filter_only else ''}.event.type")["NextCycle"].emit(1)
                        # TODO see the comment for '_activate_main_loop' call within 'activate'
                        # NEXT CYCLE IN MAIN LOOP
                        runtime_platform.routing_table.check_active_routes(routing_session)
                except NameError as bad_code_path:
                    module_logger.critical(
                        f"Processor failed to process resource: {resource!r} due to a missing internal "
                        f" dependency or symbol!"
                        f" Signal Type: {SignalType.TIMER_EVENT},"
                        f" Resource Path: {resource_path!r}."
                        f" Error: {bad_code_path!r}"
                    )
                    # we cannot bail out at this point, since we need to iterate over other signals
                    self.metric("processor.event.failure.type")["NameError"].emit(1)
                except Exception as error:
                    module_logger.critical(
                        f"Received event: {event} contains an unrecognized resource: {resource}!"
                        f" Error: '{str(error)}'."
                        f" Ignoring and moving to the next event resource..."
                    )
                    self.metric("processor.event.failure.type")["Exception"].emit(1)
        else:
            self.metric(f"processor{'.filter' if filter_only else ''}.event.type")["Unrecognized"].emit(1)
            msg = "Unrecognized event: {}".format(event)
            module_logger.error(msg)
            return routing_responses

        # pull queued feed-back signals into Lambda (recursive call)
        # TODO do this within a CW event context only, once ProcessorQueue becomes distributed.
        queued_events = runtime_platform.processor_queue.receive(90, 10)
        if queued_events:
            module_logger.critical(f"Consuming {len(queued_events)} events from queue.")
            self.metric(f"processor{'.filter' if filter_only else ''}.event.type")["ProcessorQueue"].emit(len(queued_events))
            for event in queued_events:
                # recursive call through filter lambda (create another lambda context for the handling of this event)
                runtime_platform.processor.process(event, True, filter=True)
                module_logger.critical("Recursive invocation successful!")
            runtime_platform.processor_queue.delete(queued_events)

        return routing_responses

    def replay_event_handler(self, runtime_platform: "Platform", context, routing_session: RoutingSession = RoutingSession()):
        self.metric(f"processor.replay.event.type")["NextCycle"].emit(1)
        response = self._sqs.receive_message(
            QueueUrl=self._dlq_url,
            # AttributeNames=['All'],
            MessageAttributeNames=["All"],
            MaxNumberOfMessages=10,
            VisibilityTimeout=1
            * self.REPLAY_LOOP_INTERVAL_IN_MINUTES
            * 60,  # skip next cycle for these events in case we cannot process and delete them
            WaitTimeSeconds=10,
        )
        messages = response.get("Messages", None)

        if messages:
            # first delete. we cannot take the risk of replaying and then deleting in which case duplicate signalling
            # might occur due to a failed deletion upon successful replay into core lambda. because in a subsequent
            # replay cycle that message can be picked again.
            response = self._sqs.delete_message_batch(
                QueueUrl=self._dlq_url,
                Entries=[{"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]} for message in messages],
            )
            deleted_messages_ids = {msg["Id"] for msg in response.get("Successful", [])}
            replay_messages = [message for message in messages if message["MessageId"] in deleted_messages_ids]
            replay_count = 0
            for replay_msg in replay_messages:
                body = json.loads(replay_msg["Body"])
                # filter out if it is a throttled timer event (no need to replay the core lambda for a missed ping)
                if any(self._main_loop_timer_id.lower() in resource.lower() for resource in body.get("resources", [])):
                    # self.metric(f"processor.replay.event.type")["EventBridge"].emit(1)
                    continue

                msg_attributes = replay_msg.get("MessageAttributes", None)
                if msg_attributes and (
                    msg_attributes.get("ErrorCode", {}).get("StringValue", "") == "429"
                    or "Rate Exceeded".lower() in msg_attributes.get("ErrorMessage", {}).get("StringValue", "").lower()
                ):
                    # replay: send back to "filter" lambda's queue asynchronously (DLQ is shared so we cannot bombard
                    #  core lambda with events throttled by filter lambda)
                    runtime_platform.processor.process(body, True, filter=True)
                    replay_count = replay_count + 1
                    time.sleep(1)

            if replay_count:
                module_logger.critical(f"{replay_count} events have been sent back to core processor Lambda!")
                self.metric(f"processor.replay.event.type")["ProcessorQueue"].emit(replay_count)

    @staticmethod
    def lambda_handler(event, context):
        import traceback

        try:
            init_basic_logging(None, False, logging.CRITICAL)
            is_filter = True if os.getenv("is_filter", "False") == "True" else False

            runtime_platform = AWSLambdaProcessorBasic.runtime_bootstrap()
            routing_responses = cast("AWSLambdaProcessorBasic", runtime_platform.processor).event_handler(
                runtime_platform,
                event,
                context,
                is_filter,
                RoutingSession(max_duration_in_secs=12 * 60),
            )
            if is_filter:
                if any([response for response in routing_responses if response.routes]):
                    runtime_platform.processor.process(event, True)
                    module_logger.critical("Forwarded the event into the core processor!")
            else:
                # send deferred signals back to processor queue
                # set their target_route_id s before sending it
                for routing_response in routing_responses:
                    deferred_signals: Dict[Route, Set[Signal]] = routing_response.deferred_signals
                    for route, signals in deferred_signals.items():
                        module_logger.critical(f"Will put {len(signals)} deferred signals back to queue on route {route.route_id!r}...")
                        for signal in signals:
                            # this will cause the deferred signal to hit the target route only (not the other routes that have processed it already)
                            runtime_platform.processor.process(signal, True, target_route_id=route.route_id)
            lambda_response = {"routing_responses": [response.simple_dump() for response in routing_responses if response.routes]}
            lambda_response.update({"code": "200", "message": "SUCCESS"})
            try:
                # make sure that Lambda response is json serializable
                json.dumps(lambda_response)
                return lambda_response
            except Exception as err:
                # everything went smoothly, but the response is not JSON friendly so in order to avoid Lambda regarding
                # this transaction erroneous and retry later on.
                return {"routing_responses": None, "response_rendering_error": str(err), "code": "200", "message": "SUCCESS"}
        except (RuntimeError, ValueError, TypeError, AssertionError, KeyError) as nonretryable_error:
            # swallow (by returning to Lambda), this will disable implicit retries for ASYNC requests.
            # for SYNC ones (most probably our own manual invocations via process API), returned JSON and our 'code'
            # should be descriptive enough.
            # TODO ALARM metric (will be addressed as part of the overall IF error handling/conf support)
            msg = "Non-retryable processor exception occurred while processing event: {}, exception: {}".format(
                event, str(nonretryable_error)
            )
            print(msg)
            traceback.print_exc()
            return {"message": msg, "code": "500"}
        except Exception as retryable_error:
            # A high-level safe-guard against missing an event. This is on top of internal retries, etc.
            # ex: AWS ClientErrors [timeouts, etc]
            msg = "Retryable processor exception occurred while processing event: {}, exception: {}".format(event, str(retryable_error))
            print(msg)
            traceback.print_exc()
            # 1- ASYNC: SNS, S3, EventBridge, (async invocation via 'process')
            #           Lambda will retry (up to 2 [check function event config used above during activation]
            #           waiting 1 min in between each try. This might add up to 5 mins and cause retrigger attempts
            #           on some routes (if failure happens halfway scanning the table). This is ok thanks to internal
            #           idempotency checks.
            #           if this was the last attempt, then Lambda will send this to the DLQ.
            #           What are the other scenarios for incoming events to be sent to the DLQ?
            #           - Somehow (even with intermittent retries or not [while total retry count <= 2), Lambda
            #           itself has not been available or this function is being "throttled" for up to 6 hours.
            # 2- SYNC: Lambda wraps the error and returns StatusCode: 200 back to the client.
            #           https://docs.aws.amazon.com/lambda/latest/dg/invocation-sync.html
            #           "If Lambda was able to run the function, the status code is 200, even if the function returned an error."
            #
            # see https://docs.aws.amazon.com/lambda/latest/dg/python-exceptions.html for more details specific to Python.
            # TODO SETUP ALARM ON DLQ (#2)
            raise retryable_error

    @staticmethod
    def replay_lambda_handler(event, context):
        import traceback

        try:
            init_basic_logging(None, False, logging.CRITICAL)

            runtime_platform = AWSLambdaProcessorBasic.runtime_bootstrap()
            cast("AWSLambdaProcessorBasic", runtime_platform.processor).replay_event_handler(
                runtime_platform,
                context,
                RoutingSession(max_duration_in_secs=13 * 60),
            )

            lambda_response = {"code": "200", "message": "SUCCESS"}
            # make sure that Lambda response is json serializable
            return json.dumps(lambda_response)
        except Exception as swallow:
            # swallow (by returning to Lambda), this will disable implicit retries for ASYNC requests.
            # TODO ALARM metric (will be addressed as part of the overall IF error handling/conf support)
            msg = "Replay lambda raised an exception {}".format(event, str(swallow))
            print(msg)
            traceback.print_exc()
            return {"message": msg, "code": "500"}


AWSLambdaProcessorBasic_lambda_handler = AWSLambdaProcessorBasic.lambda_handler
AWSLambdaProcessorBasic_replay_lambda_handler = AWSLambdaProcessorBasic.replay_lambda_handler


# avoid cyclic module dependency
# from ...development import DevelopmentPlatform, RuntimePlatform
