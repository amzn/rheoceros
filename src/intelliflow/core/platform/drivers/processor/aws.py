# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
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
from intelliflow.core.signal_processing.routing_runtime_constructs import Route, RouteID
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import (
    CWAlarmSignalSourceAccessSpec,
    CWCompositeAlarmSignalSourceAccessSpec,
    CWMetricSignalSourceAccessSpec,
    GlueTableSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
    SignalSourceType,
    TimerSignalSourceAccessSpec,
)

from ...constructs import (
    FEEDBACK_SIGNAL_PROCESSING_MODE_KEY,
    FEEDBACK_SIGNAL_TYPE_EVENT_KEY,
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
from ...definitions.aws.s3.object_wrapper import build_object_key, empty_bucket, get_object, put_object
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
    CLIENT_RETRYABLE_EXCEPTION_LIST: ClassVar[Set[str]] = {"InvalidParameterValueException", "ServiceException", "503"}
    MAIN_LOOP_INTERVAL_IN_MINUTES: ClassVar[int] = 1

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
        # DLQ
        self._sqs = self._session.client(service_name="sqs", region_name=self._region)
        self._dlq_name = None
        self._dlq_url = None
        self._dlq_arn = None

        self._main_loop_timer_id = None

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._lambda = self._session.client(service_name="lambda", region_name=self._region)
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._bucket = get_bucket(self._s3, self._bucket_name)
        self._sqs = self._session.client(service_name="sqs", region_name=self._region)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._lambda = None
        self._s3 = None
        self._bucket = None
        self._sqs = None

    def process(
        self,
        signal_or_event: Union[Signal, Dict[str, Any]],
        use_activated_instance=False,
        processing_mode=FeedBackSignalProcessingMode.ONLY_HEAD,
        target_route_id: Optional[RouteID] = None,
        is_async=True,
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

        if use_activated_instance or not self._dev_platform:
            while True:
                try:
                    exponential_retry(
                        invoke_lambda_function,
                        # KMSAccessDeniedException is caused by quick terminate->activate cycles
                        # where Lambda encryption store might have problems with IAM propagation probably.
                        ["ServiceException", "KMSAccessDeniedException"],
                        self._lambda,
                        self._lambda_name,
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
        exponential_retry(put_function_concurrency, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name, 0)
        super().pause()

    def resume(self) -> None:
        concurreny_limit = self.concurrency_limit
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
            # DLQ (let exec-role send messages to DLQ)
            ConstructPermission(
                [
                    # since this callback is received right before the activation (but after dev_init),
                    # we cannot use the retrieved self._dlq_arn. should manually create it here.
                    # not a big deal since self._dlq_name should already be created.
                    f"arn:aws:sqs:{self._region}:{self._account_id}:{self._dlq_name}"
                ],
                ["sqs:SendMessage", "sqs:SendMessageBatch"],
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
        # TODO post-MVP narrow it down to whatever Lambda APIs we rely on within this impl (at dev-time).
        return [
            ConstructPermission(["*"], ["lambda:*"]),
            ConstructPermission(["*"], ["events:*"]),
            ConstructPermission(["*"], ["sqs:*"])
            # TODO dev-role should have the right to do BucketNotification on external signals
            # this would require post-MVP design change on how dev-role is used and when it is updated.
            # probably during the activation again (switching to the admin credentails if authorization is given).
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
        function_names = [self._lambda_name, self._filter_lambda_name]
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
                f"processor.{'core' if i == 0 else 'filter'}",
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
                15 * 60  # 15 mins for visibility timeout for future support of consumption from DLQ.
                # in that case the consumer will depend on max IF event processing time
                # before it can actually delete the message from the Queue.
            )
        else:
            self._dlq_url = dlq_url
            exponential_retry(set_queue_attributes, [], self._sqs, self._dlq_url, 1209600, 15 * 60)
        self._dlq_arn = get_queue_arn(self._sqs, self._dlq_url)

        if not is_environment_immutable():
            working_set_stream = get_working_set_as_zip_stream()
            # lambda
            self._activate_core_lambda(working_set_stream)
            # filter lambda
            self._activate_filter_lambda(working_set_stream)

            # So what about ASYNC invocation configuration?
            # Why not use https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.put_function_event_invoke_config
            # - Because, default settings (6 hours event age, 2 retries, etc) are perfectly ok for this driver impl and
            # based on how IF internal routing works.
            # - Also we already set DLQ via function creation and conf update APIs and don't need to use event invoke config
            # to alternatively set our DLQ as a failure destination.

        self._activate_main_loop()

    def _activate_core_lambda(self, working_set_stream):
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
                working_set_stream,
                PYTHON_VERSION_MAJOR,
                PYTHON_VERSION_MINOR,
                self._dlq_arn,
            )

            # see '_update_bootstrapper' callback below to see how bootstrapper is set
            # at the end of the activation cycle. we cannot set it now since activation across
            # other drivers might happen in parallel and they might impact the final state of
            # the platform which needs to be serialized as the bootstrapper.
        else:  # update
            # code should be updated first, as the workset should naturally be backward compatible
            # with the conf (which carries the bootstrapper [RuntimePlatform]).
            exponential_retry(
                update_lambda_function_code, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name, working_set_stream
            )

        concurreny_limit = self.concurrency_limit
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

    def _activate_filter_lambda(self, working_set_stream):
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
                working_set_stream,
                PYTHON_VERSION_MAJOR,
                PYTHON_VERSION_MINOR,
                self._dlq_arn,
                is_filter="True",
            )
        else:  # update
            # code should be updated first, as the workset should naturally be backward compatible
            # with the conf (which carries the bootstrapper [RuntimePlatform]).
            exponential_retry(
                update_lambda_function_code,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._lambda,
                self._filter_lambda_name,
                working_set_stream,
            )

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

        try:
            response = exponential_retry(
                events.put_rule,
                ["ConcurrentModificationException", "InternalException"],
                Name=self._main_loop_timer_id,
                ScheduleExpression=main_loop_schedule_expression,
                State="ENABLED",
            )
            rule_arn = response["RuleArn"]
        except ClientError as error:
            raise

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
                            f" has the following BucketNotification setup for lambda."
                            f" (LambdaFunctionArn={self._filter_lambda_arn}, events='s3:ObjectCreated:*'"
                            f" or owners of that bucket can grant the following to "
                            f" IF_DEV_ROLE ({self._params[AWSCommonParams.IF_DEV_ROLE]})"
                            f" ['s3:GetBucketNotificationConfiguration', 's3:PutBucketNotificationConfiguration']."
                        )

        if removed_s3_signal_source_access_specs:
            self._remove_from_remote_bucket_policy(removed_s3_signal_source_access_specs)

        super().terminate()

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)

    @classmethod
    def _get_unique_statement_id_for_s3(cls, bucket: str) -> str:
        return "s3Invoke" + generate_statement_id(bucket)

    @classmethod
    def _get_unique_statement_id_for_cw(cls, timer_id: str) -> str:
        return "cwInvoke" + generate_statement_id(timer_id)

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
            # convenience check to help users to avoid typos, etc in the most common scenario of s3 + glue_table
            if s.resource_access_spec.proxy and s.resource_access_spec.proxy.source == SignalSourceType.GLUE_TABLE:
                glue_access_spec: GlueTableSignalSourceAccessSpec = cast("GlueTableSignalSourceAccessSpec", s.resource_access_spec.proxy)
                if not glue_catalog.check_table(self._session, self._region, glue_access_spec.database, glue_access_spec.table_name):
                    raise ValueError(
                        f"Glue table {glue_access_spec!r} could not be found in account " f"{self._account_id!r} region {self._region!r}!"
                    )

        # Glue table specific check
        for s in signals:
            if s.resource_access_spec.source == SignalSourceType.GLUE_TABLE:
                access_spec: GlueTableSignalSourceAccessSpec = cast("GlueTableSignalSourceAccessSpec", s.resource_access_spec)
                if not glue_catalog.check_table(self._session, self._region, access_spec.database, access_spec.table_name):
                    raise ValueError(
                        f"Either the database ({access_spec.database!r}) or table ({access_spec.table_name!r}) "
                        f" could not be found in account {self._account_id!r} region {self._region!r}."
                    )

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
                try:
                    response = exponential_retry(
                        events.put_rule,
                        ["ConcurrentModificationException", "InternalException"],
                        Name=new_timer_id,
                        ScheduleExpression=new_signal.resource_access_spec.schedule_expression,
                        State="ENABLED",
                    )
                    rule_arn = response["RuleArn"]
                except ClientError as error:
                    raise
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

    def event_handler(self, runtime_platform: "Platform", event, context, filter_only: bool = False) -> List[RoutingTable.Response]:
        # TODO check processor_queue
        #  - based on another CW event
        # currently we check the queue optimistically at the end of each cycle
        routing_responses: List[RoutingTable.Response] = []
        target_route_id = event.get(SIGNAL_TARGET_ROUTE_ID_KEY, None)
        module_logger.info(f"Processing event: {event!r}")

        serialized_feed_back_signal: str = event.get(FEEDBACK_SIGNAL_TYPE_EVENT_KEY, None)
        if serialized_feed_back_signal:
            feed_back_signal: Signal = Signal.deserialize(serialized_feed_back_signal)
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
                        feed_back_signal.type, source_spec.source, resource_path, target_route_id, filter_only
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
                        signal_type, SignalSourceType.S3, resource_path, target_route_id, filter_only
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
                        signal_type, source_type, resource_path, target_route_id, filter_only
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
                            SignalType.TIMER_EVENT, SignalSourceType.TIMER, resource_path, target_route_id, filter_only
                        )
                        routing_responses.append(routing_response)
                    else:
                        self.metric(f"processor{'.filter' if filter_only else ''}.event.type")["NextCycle"].emit(1)
                        # TODO see the comment for '_activate_main_loop' call within 'activate'
                        # NEXT CYCLE IN MAIN LOOP
                        runtime_platform.routing_table.check_active_routes()
                except NameError as bad_code_path:
                    module_logger.error(
                        f"Processor failed to process resource: {resource!r} due to a missing internal "
                        f" dependency or symbol!"
                        f" Signal Type: {SignalType.TIMER_EVENT},"
                        f" Resource Path: {resource_path!r}."
                        f" Error: {bad_code_path!r}"
                    )
                    # we cannot bail out at this point, since we need to iterate over other signals
                    self.metric("processor.event.failure.type")["NameError"].emit(1)
                except Exception as error:
                    module_logger.error(
                        f"Received event: {event} contains an unrecognized resource: {resource}!"
                        f" Error: {str(error)}"
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
                # recursive call (create another lambda context for the handling of this event)
                runtime_platform.processor.process(event, True)
                module_logger.critical("Recursive invocation successful!")
            runtime_platform.processor_queue.delete(queued_events)

        return routing_responses

    @staticmethod
    def lambda_handler(event, context):
        import traceback

        try:
            init_basic_logging(None, False, logging.CRITICAL)
            is_filter = True if os.getenv("is_filter", "False") == "True" else False

            runtime_platform = AWSLambdaProcessorBasic.runtime_bootstrap()
            routing_responses = cast("AWSLambdaProcessorBasic", runtime_platform.processor).event_handler(
                runtime_platform, event, context, is_filter
            )
            if is_filter:
                if any([response for response in routing_responses if response.routes]):
                    runtime_platform.processor.process(event, True)
                    module_logger.critical("Forwarded the event into the core processor!")
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


AWSLambdaProcessorBasic_lambda_handler = AWSLambdaProcessorBasic.lambda_handler


# avoid cyclic module dependency
# from ...development import DevelopmentPlatform, RuntimePlatform
