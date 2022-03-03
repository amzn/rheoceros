# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import codecs
import json
import logging
import uuid
from typing import ClassVar, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Type, Union, cast

import boto3
from botocore.exceptions import ClientError

from intelliflow.core.platform.definitions.aws.sns.client_wrapper import find_subscription
from intelliflow.core.platform.definitions.common import ActivationParams
from intelliflow.core.signal_processing import Signal
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal import SignalType
from intelliflow.core.signal_processing.signal_source import (
    InternalDatasetSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
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
    EncryptionKeyAllocationLevel,
    Storage,
    UpstreamGrants,
    UpstreamRevokes,
)
from ...definitions.aws.aws_lambda.client_wrapper import add_permission, remove_permission
from ...definitions.aws.common import CommonParams as AWSCommonParams
from ...definitions.aws.common import exponential_retry, generate_statement_id, update_role
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


class AWSS3StorageBasic(AWSConstructMixin, Storage):
    STORAGE_ROOT_FORMAT: ClassVar[str] = "if-{0}-{1}-{2}"
    TOPIC_NAME_FORMAT: ClassVar[str] = "if-{0}-{1}"
    CMK_ALIAS_NAME_FORMAT: ClassVar[str] = "alias/if-{0}-{1}"
    """Storage based on S3.

    Trade-offs:

        Pros:

        Cons:

    """

    def __init__(self, params: ConstructParamsDict) -> None:
        super().__init__(params)
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._bucket = None
        self._bucket_name = None
        self._sns = self._session.client(service_name="sns", region_name=self._region)
        self._topic_name = None
        self._topic_arn = None
        self._topic_root_policy = None
        self._kms = self._session.client(service_name="kms", region_name=self._region)
        self._cmk_alias = None
        self._cmk_id = None
        self._cmk_arn = None

    @property
    def bucket_name(self) -> str:
        return self._bucket_name

    def get_storage_resource_path(self) -> str:
        return f"arn:aws:s3:::{self._bucket.name}"

    def get_event_channel_type(self) -> str:
        return "SNS"

    def get_event_channel_resource_path(self) -> str:
        return self._topic_arn

    def get_object_uri(self, folders: Sequence[str], object_name: str) -> str:
        object_key: str = build_object_key(folders, object_name)
        return f"s3://{self._bucket_name}/{object_key}"

    def check_object(self, folders: Sequence[str], object_name: str) -> bool:
        object_key: str = build_object_key(folders, object_name)
        return object_exists(self._s3, self._bucket, object_key)

    def load_object(self, folders: Sequence[str], object_name: str) -> bytes:
        return get_object(self._bucket, build_object_key(folders, object_name))

    def save_object(self, data: bytes, folders: Sequence[str], object_name: str) -> None:
        put_object(self._bucket, build_object_key(folders, object_name), data)

    def delete(self, folders: Sequence[str], object_name: str) -> None:
        delete_objects(self._bucket, [build_object_key(folders, object_name)])

    def check_folder(self, prefix_or_folders: Union[str, Sequence[str]]) -> bool:
        folder = prefix_or_folders if isinstance(prefix_or_folders, str) else prefix_or_folders.join("/")
        return len(list(list_objects(self._bucket, folder, 1))) >= 1

    def load_folder(self, prefix_or_folders: Union[str, Sequence[str]], limit: int = None) -> Iterator[Tuple[str, bytes]]:
        """returns (materialized full path ~ data) pairs"""
        folder = prefix_or_folders if isinstance(prefix_or_folders, str) else prefix_or_folders.join("/")
        objects_in_folder = list_objects(self._bucket, folder, limit)
        for object in objects_in_folder:
            key = object.key
            body = object.get()["Body"].read()
            yield (f"s3://{self._bucket_name}/{key}", body)

    def load_internal(self, internal_signal: Signal, limit: int = None) -> Iterator[Tuple[str, bytes]]:
        """returns (materialized full path ~ data) pairs of all of the objects represented by this signal

        If internal signal represents a domain (multiple folder for example), then 'limit' param is applied
        to each one of them (not as limit to the cumulative sum of number of object from each folder).
        """
        if internal_signal.resource_access_spec.source != SignalSourceType.INTERNAL:
            raise ValueError("Input data access spec is not of type Internal! ")
        materialized_paths = internal_signal.get_materialized_resource_paths()
        for materialized_path in materialized_paths:
            path_it = self.load_folder(materialized_path.lstrip("/"), limit)
            yield from path_it

    def delete_internal(self, internal_signal: Signal, entire_domain: bool = True) -> bool:
        """Deletes the internal data represented by this signal.

        If 'entire_domain' is True (by default), then internal data represented by the all possible dimension values
        for the signal (the domain) will be wiped out. For that case, the behaviour does not depend on whether the
        signal is materialized or not (uses the 'folder' / root information for the deletion).

        if 'entire_domain' is False, then the signal should be materialized. A specific part of the internal data
        (aka partition for datasets) gets wiped out and the remaining parts of the data stays intact along with folder
        structure.

        returns True if the data is successfully (not partially) wiped out.
        """
        if internal_signal.resource_access_spec.source != SignalSourceType.INTERNAL:
            raise ValueError("Input data access spec is not of type Internal! ")

        prefixes = []
        if entire_domain:
            prefixes.append(internal_signal.resource_access_spec.folder if internal_signal.resource_access_spec.folder else "")
        else:
            if not internal_signal.domain_spec.dimension_filter_spec.is_material():
                raise ValueError("Input to Storage::delete_internal should be materialized while " "'entire_domain' is false")

            for path in internal_signal.get_materialized_resource_paths():
                prefixes.append(path.lstrip(internal_signal.resource_access_spec.path_delimiter()))

        completely_wiped_out = True
        for prefix in prefixes:
            objects_in_folder = list_objects(self._bucket, prefix)
            keys = []
            for object in objects_in_folder:
                key = object.key
                keys.append(key)
                if len(keys) == 50:
                    if not delete_objects(self._bucket, keys):
                        completely_wiped_out = False
                    keys = []
            if keys:
                if not delete_objects(self._bucket, keys):
                    completely_wiped_out = False
        return completely_wiped_out

    def is_internal(self, source_type: SignalSourceType, resource_path: str) -> bool:
        if source_type == SignalSourceType.S3 and S3SignalSourceAccessSpec.from_url("", resource_path).bucket == self._bucket_name:
            return True
        return False

    def map_incoming_event(self, source_type: SignalSourceType, resource_path: str) -> Optional[SignalSourceAccessSpec]:
        if source_type == SignalSourceType.S3:
            s3_access_spec = S3SignalSourceAccessSpec.from_url("", resource_path)
            if s3_access_spec.bucket == self._bucket_name:
                if s3_access_spec.folder == InternalDatasetSignalSourceAccessSpec.FOLDER:
                    mapped_path = resource_path.replace(f"s3://{self._bucket_name}", "")
                    return SignalSourceAccessSpec(SignalSourceType.INTERNAL, mapped_path, None)
                    # return InternalDatasetSignalSourceAccessSpec(s3_access_spec.get_partition_keys()[1],
                    #                                             *s3_access_spec.get_partition_keys()[2:])
        return None

    def map_internal_data_access_spec(self, data_access_spec: SignalSourceAccessSpec) -> SignalSourceAccessSpec:
        if data_access_spec.source == SignalSourceType.INTERNAL:
            return S3SignalSourceAccessSpec(self._account_id, self._bucket_name, data_access_spec)
        else:
            raise ValueError("Input data access spec is not of type Internal!")

    def map_internal_signal(self, signal: Signal) -> Signal:
        mapped_resource_spec = self.map_internal_data_access_spec(signal.resource_access_spec)
        return Signal(
            SignalType.EXTERNAL_S3_OBJECT_CREATION,
            mapped_resource_spec,
            signal.domain_spec,
            signal.alias,
            signal.is_reference,
            signal.range_check_required,
            signal.nearest_the_tip_in_range,
            signal.is_termination,
            signal.is_inverted,
        )

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)
        self._bucket_name: str = self.STORAGE_ROOT_FORMAT.format(self._dev_platform.context_id.lower(), self._account_id, self._region)
        if not bucket_exists(self._s3, self._bucket_name):
            # TODO consider moving to activate?
            # storage might be required to read stuff even during dev-time.
            self._setup_bucket()
        else:
            self._bucket = get_bucket(self._s3, self._bucket_name)

        self._topic_name = self.TOPIC_NAME_FORMAT.format(self._dev_platform.context_id, self.__class__.__name__)
        self._topic_arn = f"arn:aws:sns:{self._region}:{self._account_id}:{self._topic_name}"

        # TODO remove (added for backwards compatibility)
        if not getattr(self, "_cmk_alias", None):
            self._cmk_alias = self.CMK_ALIAS_NAME_FORMAT.format(self._dev_platform.context_id, self.__class__.__name__)

        # the reason we create the cmk (before the activation) is unpredictability of key id, ARN which will
        # be used in calls such as 'provide_runtime_permissions'.
        # we paranoidly check to reflect id&arn on the CMK (that was probably forced manually by admin, etc).
        self._cmk_id, self._cmk_arn = exponential_retry(
            get_cmk,
            ["DependencyTimeoutException", "KMSInternalException", "AccessDeniedException"],  # permission propagation
            self._kms,
            self._cmk_alias,
        )

        if not self._cmk_id:
            self._cmk_id, self._cmk_arn = exponential_retry(
                create_cmk,
                [
                    "DependencyTimeoutException",
                    "KMSInternalException",
                    # handle due to IAM propagation (eventual consistency).
                    # roles might not still be ready for a brand new app.
                    "MalformedPolicyDocumentException",  # role still seems to be non-existent
                    "AccessDeniedException",
                ],  # permission propagation delayed
                self._kms,
                create_default_policy(
                    account_id=self._account_id,
                    # cannot add EXE ROLE since the platform might be new and
                    # it might not even exist yet (would cause MalformedPolicy error)
                    users_to_be_added=set(),
                    admins={self._params[AWSCommonParams.IF_DEV_ROLE]},
                    trust_same_account=True,
                ),
                f"RheocerOS {self.__class__.__name__} driver encryption key for {self._dev_platform.context_id!r}",
            )
            try:
                exponential_retry(
                    create_alias, ["DependencyTimeoutException", "KMSInternalException"], self._kms, self._cmk_alias, self._cmk_id
                )
            except ClientError as error:
                if error.response["Error"]["Code"] != "AlreadyExistsException":
                    raise
                exponential_retry(
                    update_alias, ["DependencyTimeoutException", "KMSInternalException"], self._kms, self._cmk_alias, self._cmk_id
                )

    def _setup_bucket(self):
        """Initial setup of storage bucket. Enforces policy for access from IF_DEV_ROLE."""
        try:
            self._bucket = exponential_retry(
                create_bucket,
                # For a brand-new app, IAM propagation delay causes the following:
                ["AccessDenied"],
                self._s3,
                self._bucket_name,
                self._region,
            )
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

        put_policy_desc = {
            "Version": "2012-10-17",
            "Id": str(uuid.uuid1()),
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": self._params[AWSCommonParams.IF_DEV_ROLE]},
                    "Action": ["s3:*"],
                    "Resource": [f"arn:aws:s3:::{self._bucket.name}/*", f"arn:aws:s3:::{self._bucket.name}"],
                }
            ],
        }
        try:
            exponential_retry(put_policy, ["MalformedPolicy", "AccessDenied"], self._s3, self._bucket.name, put_policy_desc)
        except ClientError as error:
            if error.response["Error"]["Code"] == "MalformedPolicy":
                module_logger.error("Couldn't put the policy for storage! Error: %s", str(error))
            raise

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._sns = self._session.client(service_name="sns", region_name=self._region)
        self._kms = self._session.client(service_name="kms", region_name=self._region)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._s3 = None
        self._bucket = None
        self._sns = None
        self._kms = None

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        """Whole platform got bootstrapped at runtime. For other runtime services, this
        construct should be initialized (ex: context_owner: Lambda, Glue, etc)"""
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._s3 = boto3.resource("s3", region_name=self._region)
        self._bucket = get_bucket(self._s3, self._bucket_name)
        self._sns = boto3.client(service_name="sns", region_name=self._region)
        self._kms = boto3.client(service_name="kms", region_name=self._region)

    def provide_runtime_trusted_entities(self) -> List[str]:
        # Is there any scenario when any of the AWS services (s3) from this impl should assume our exec role?
        # No
        return []

    def provide_runtime_default_policies(self) -> List[str]:
        return []

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        # allow exec-role (post-activation, cumulative list of all trusted entities [AWS services]) to do the following;
        permissions = [
            ConstructPermission([f"arn:aws:s3:::{self._bucket.name}", f"arn:aws:s3:::{self._bucket.name}/*"], ["s3:*"]),
            # TODO enable post-MVP (might need more due to subscriptions to S3 from other Constructs
            # and also due to polling (i.e Lambda polling)
            # ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:GetObjectVersion", "s3:ListBucket"]),
            ConstructPermission(
                [self._topic_arn],
                # our exec-role might need to reply back to SNS once a token is received at runtime
                # following a 'subscribe' call.
                ["sns:ConfirmSubscription", "sns:Receive"],
            ),
        ]

        if self._pending_security_conf and self._pending_security_conf.persisting.encryption:
            ConstructPermission(
                [self._cmk_arn], ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"]
            )

        return permissions

    @classmethod
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        # dev-role should be able to do the following.
        # TODO post-MVP narrow it down to whatever S3 APIs we rely on within this impl (at dev-time).
        # # ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:GetObjectVersion"]),
        # # ["s3:GetBucketNotificationConfiguration", "s3:PutBucketNotificationConfiguration"]
        # # ["s3:GetBucketPolicy", "s3:PutBucketPolicy", "s3:GetBucketPolicyStatus"]
        # # ["s3:ListBucket", "s3:HeadBucket"]
        # also specify its bucket
        return [
            ConstructPermission(["*"], ["s3:*"]),
            # TODO limit to sns:CreateTopic, sns:Subscribe, sns:SetTopicAttributes, sns:Unsubscribe, sns:DeleteTopic
            ConstructPermission(["*"], ["sns:*"]),
            # TODO limit to kms:CreateKey, kms:PutKeyPolicy, kms:GetKeyPolicy, kms:Encrypt, kms:Decrypt,
            #  kms:ReEncrypt*, kms:GenerateDataKey*, kms:DescribeKey
            ConstructPermission(["*"], ["kms:*"]),
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

    def hook_external(self, signals: List[Signal]) -> None:
        # demo purpose optimization (not mandatory), avoid buffering of external events for activation.
        # skip buffering it for activation. this construct cannot do anything with external signals
        # super().hook_external(signals)
        pass

    def hook_internal(self, route: "Route") -> None:
        # we are interested to check on all of the internal events.
        super().hook_internal(route)

    def hook_internal_signal(self, signal: "Signal") -> None:
        # currently, not interested in other signals
        pass

    def hook_security_conf(
        self, security_conf: ConstructSecurityConf, platform_security_conf: Dict[Type["BaseConstruct"], ConstructSecurityConf]
    ) -> None:
        if security_conf:
            if security_conf.persisting.encryption.is_hard_rotation:
                raise NotImplementedError("Security/Encryption/Hard rotation for AWS S3 based Storage not implemented yet.")
            if security_conf.persisting.encryption.key_allocation_level == EncryptionKeyAllocationLevel.PER_RESOURCE:
                raise NotImplementedError("Security/Encryption/PER_RESOURCE key allocation not implemented yet.")
            if security_conf.persisting.encryption.key_rotation_cycle_in_days < 365:
                raise NotImplementedError("Security/Encryption/Rotation Cycle intervals less than 365 days not supported yet.")
            if security_conf.persisting.encryption.reencrypt_old_data_during_hard_rotation:
                raise NotImplementedError("Security/Encryption/Reencrypt old data during hard rotations not supported yet.")

        super().hook_security_conf(security_conf, platform_security_conf)

    def _process_security_conf(self, new_security_conf: ConstructSecurityConf, current_security_conf: ConstructSecurityConf) -> None:
        # set bucket encryption
        if new_security_conf and new_security_conf.persisting.encryption:
            if new_security_conf.persisting.encryption.key_rotation_cycle_in_days:
                exponential_retry(enable_key_rotation, ["DependencyTimeoutException", "KMSInternalException"], self._kms, self._cmk_id)
            # if not current_security_conf or (new_security_conf.persisting.encryption.trust_access_from_same_root !=
            #                                current_security_conf.persisting.encryption.trust_access_from_same_root):
            exponential_retry(
                put_cmk_policy,
                [
                    "DependencyTimeoutException",
                    # hand due to IAM propagation (eventual consistency),
                    # exec role might not still be ready for a brand new app.
                    "MalformedPolicyDocumentException",
                    "KMSInternalException",
                ],
                self._kms,
                self._cmk_id,
                self._account_id,
                # this might be our first time activating security, add exec role.
                # this method has necessary deduping.
                users_to_be_added={self._params[AWSCommonParams.IF_EXE_ROLE]},
                users_to_be_removed=set(),  # leave as is
                trust_same_account=new_security_conf.persisting.encryption.trust_access_from_same_root,
            )

            exponential_retry(
                self._s3.meta.client.put_bucket_encryption,
                [],
                Bucket=self._bucket_name,
                ServerSideEncryptionConfiguration={
                    "Rules": [
                        {
                            "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms", "KMSMasterKeyID": self._cmk_arn}
                            # ,
                            # see https://docs.aws.amazon.com/AmazonS3/latest/dev/bucket-key.html
                            # why this is much needed in our case.
                            # TODO raising botocore.exceptions.ParamValidationError with 1.16.25
                            #  need to upgrade to at least 1.16.27
                            #  see
                            #  https://boto3.amazonaws.com/v1/documentation/api/1.16.27/reference/services/s3.html#S3.Client.put_bucket_encryption
                            #'BucketKeyEnabled': True
                        }
                    ]
                },
                ExpectedBucketOwner=self._account_id,
            )

        elif current_security_conf and current_security_conf.persisting.encryption:
            exponential_retry(
                self._s3.meta.client.delete_bucket_encryption, [], Bucket=self._bucket_name, ExpectedBucketOwner=self._account_id
            )

    def process_downstream_connection(self, downstream_platform: "DevelopmentPlatform") -> UpstreamGrants:
        """Called by downstream platform to establish/update the connection"""
        downstream_dev_role = downstream_platform.conf.get_param(AWSCommonParams.IF_DEV_ROLE)
        downstream_exe_role = downstream_platform.conf.get_param(AWSCommonParams.IF_EXE_ROLE)

        # 1- authorize downstream roles in KMS policy of upstream
        exponential_retry(
            put_cmk_policy,
            ["DependencyTimeoutException", "KMSInternalException"],
            self._kms,
            self._cmk_id,
            self._account_id,
            users_to_be_added={downstream_dev_role, downstream_exe_role},
            users_to_be_removed=set(),  # leave as is
            # leave this flag as is (None)
            trust_same_account=None,
        )

        # 2- send back policy change requirements from downstream
        cmk_use_statement = ConstructPermission(
            [self._cmk_arn], ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"]
        )
        return UpstreamGrants(
            {
                ConstructPermissionGroup(downstream_dev_role, {cmk_use_statement}),
                ConstructPermissionGroup(downstream_exe_role, {cmk_use_statement}),
            }
        )

    def terminate_downstream_connection(self, downstream_platform: "DevelopmentPlatform") -> UpstreamRevokes:
        """Called by downstream platform to request the termination of the connection"""
        downstream_dev_role = downstream_platform.conf.get_param(AWSCommonParams.IF_DEV_ROLE)
        downstream_exe_role = downstream_platform.conf.get_param(AWSCommonParams.IF_EXE_ROLE)

        # 1- unauthorize downstream roles in KMS policy of upstream
        exponential_retry(
            put_cmk_policy,
            ["DependencyTimeoutException", "KMSInternalException"],
            self._kms,
            self._cmk_id,
            self._account_id,
            users_to_be_added=set(),  # leave as is
            users_to_be_removed={downstream_dev_role, downstream_exe_role},
            # leave this flag as is (None)
            trust_same_account=None,
        )

        # 2- send back the changes to be deleted from downstream policies
        cmk_use_statement = ConstructPermission(
            [self._cmk_arn], ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"]
        )
        return UpstreamRevokes(
            {
                ConstructPermissionGroup(downstream_dev_role, {cmk_use_statement}),
                ConstructPermissionGroup(downstream_exe_role, {cmk_use_statement}),
            }
        )

    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        # has nothing to do with external events (since hook_external is overwritten and events are bypassed)
        # this method should be called with an empty list.
        pass

    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        # TODO anything to check or setup for new routes?
        # for connections, it is expected that other constructs should send a conn request (see _process_construct_connections)
        # according to the needs of their underlying (AWS) resources.
        pass

    def _process_internal_signals(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

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
                err: str = f"{self.__class__.__name__} Construct cannot process the connection request: {conn!r}"
                module_logger.error(err)
                raise ValueError(err)

        # s3 notification setup requires target resources to be alive and with right permissions.
        # so let's setup client permissions first.
        lambda_client = self._session.client(service_name="lambda", region_name=self._region)
        for lambda_arn in lambda_arns:
            statement_id: str = self.__class__.__name__ + "_lambda_Invoke_Permission"
            # TODO switch to following way of generating the statement id
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
            # DO NOT use account_id for any other trigger than S3.
            # self._account_id)

            exponential_retry(
                self._sns.subscribe, {"InternalErrorException"}, TopicArn=self._topic_arn, Protocol="lambda", Endpoint=lambda_arn
            )

        """
        # FUTURE might enable this for internal Queues and SNS topics where direct registration to S3 might be ok
        # (on a 1-1 basis), which is obviously not possible with Lambda.
        #
        # we cannot tolerate problems in setting up our internal bucket.
        # dev-role should be able to do this and if this fails, fail the whole activation.
        exponential_retry(put_notification,
                          {'ServiceException', 'TooManyRequestsException'},
                          self._s3, self._bucket_name,
                          topic_arns, queue_arns, [], events=["s3:ObjectCreated:*"])
        """

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
        # TODO
        pass

    def _revert_internal(self, routes: Set[Route], prev_routes: Set[Route]) -> None:
        # TODO
        pass

    def _revert_internal_signals(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        # TODO
        pass

    def _revert_construct_connections(
        self, construct_conns: Set["_PendingConnRequest"], prev_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        # TODO
        pass

    def _revert_security_conf(selfs, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        # TODO
        pass

    def activate(self) -> None:
        super().activate()
        # app is being lauched. our last chance to take runtime related actions
        # before our code becomes available to other constructs at runtime (ex: in AWS).
        self._setup_activated_policy()
        self._setup_event_channel()

    def _setup_activated_policy(self) -> None:
        unique_context_id = self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]
        put_policy_desc = {
            "Version": "2012-10-17",
            "Id": str(uuid.uuid1()),
            "Statement": [
                {
                    "Sid": f"{unique_context_id}_self_dev_role_bucket_access",
                    "Effect": "Allow",
                    "Principal": {"AWS": self._params[AWSCommonParams.IF_DEV_ROLE]},
                    "Action": ["s3:*"],
                    "Resource": [f"arn:aws:s3:::{self._bucket.name}/*", f"arn:aws:s3:::{self._bucket.name}"],
                },
                {
                    "Sid": f"{unique_context_id}_self_exec_role_bucket_access",
                    "Effect": "Allow",
                    "Principal": {"AWS": self._params[AWSCommonParams.IF_EXE_ROLE]},
                    "Action": ["s3:*"],
                    # TODO post-MVP
                    # 'Action': [ 's3:GetObject', 's3:GetObjectVersion' 's3:ListBucket' ],
                    "Resource": [f"arn:aws:s3:::{self._bucket.name}/*", f"arn:aws:s3:::{self._bucket.name}"],
                },
            ],
        }
        try:
            exponential_retry(
                update_policy,
                {"MalformedPolicy", "AccessDenied", "ServiceException", "TooManyRequestsException"},
                self._s3,
                self._bucket.name,
                put_policy_desc,
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "MalformedPolicy":
                module_logger.error("Couldn't put the policy for storage! Error: %s", str(error))
            raise

    def _setup_event_channel(self) -> None:
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
                module_logger.error("Couldn't create event channel (SNS topic) for the storage! Error: %s", str(err))
                raise

            self._topic_root_policy = {
                "Version": "2012-10-17",
                "Id": str(uuid.uuid1()),
                "Statement": [
                    {
                        "Sid": str(uuid.uuid1()),
                        "Effect": "Allow",
                        "Principal": {"Service": "s3.amazonaws.com"},
                        "Action": "sns:Publish",
                        "Resource": self._topic_arn,
                        "Condition": {
                            "ArnLike": {"AWS:SourceArn": f"arn:aws:s3:*:*:{self._bucket_name}"}
                            # "StringEquals": { "AWS:SourceOwner": f"{self._account_id}" }
                        },
                    },
                    # dev-role policy already has super-set IAM statements to control the management of this SNS topic
                    # alternatively SNS policy here can also control / specify dev-role access.
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
                    f"Could not update policy for storage event-channel to enable publish! topic: {self._topic_arn}, err: {str(err)}"
                )
                raise

        # now register event-channel (topic) to storage bucket's notification conf
        exponential_retry(
            put_notification,
            {"ServiceException", "TooManyRequestsException"},
            self._s3,
            self._bucket_name,
            [self._topic_arn],
            [],
            [],
            events=["s3:ObjectCreated:*"],
        )

    def rollback(self) -> None:
        # roll back activation, something bad has happened (probably in another Construct) during app launch
        super().rollback()
        # TODO revert the structural change (keep track of other actions that just has been taken within 'activate')

    def deactivate(self) -> None:
        """The rest of the system has been terminated, use this callback to revert whatever has been done
        in Storage::activate.

        Why is this important? For example, if we leave the bucket policy as is, then the IF_EXE_ROLE that was added
        during the activation would remain in the policy. But that role is actually delete (by Platform) at the end of
        the termination sequence. So we would risk leaving this Storage in a bad state with a deleted (invaliad) IAM
        role in its policy which would cause MalformedPolicy errors in any of the subsequent update calls.
        So in a case like that, when you fetch your bucket policy it would actually contain a weird principle like
        'AROAWHG3....' in stead of the ARN of the deleted role.

        And theoretically, to be data consistent according to the overall system state.
        """
        unique_context_id = self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]
        # DELETE the exec role
        removed_statements = [{"Sid": f"{unique_context_id}_self_exec_role_bucket_access"}]

        exponential_retry(
            update_policy,
            {"MalformedPolicy", "ServiceException", "TooManyRequestsException"},
            self._s3,
            self._bucket.name,
            None,
            removed_statements,
        )

        exponential_retry(
            put_cmk_policy,
            ["DependencyTimeoutException", "KMSInternalException"],
            self._kms,
            self._cmk_id,
            self._account_id,
            users_to_be_added=set(),  # leave as is
            users_to_be_removed={self._params[AWSCommonParams.IF_EXE_ROLE]},
        )

    def terminate(self) -> None:
        """Designed to be resilient against repetitive calls in case of retries in the high-level
        termination work-flow.
        """
        # 1- Delete bucket
        if self._bucket_name and exponential_retry(bucket_exists, [], self._s3, self._bucket_name):
            while True:
                try:
                    module_logger.critical(f"Emptying bucket {self._bucket_name!r}...")
                    exponential_retry(empty_bucket, [], self._bucket)
                    module_logger.critical(f"Deleting emptied bucket {self._bucket_name!r}...")
                    exponential_retry(delete_bucket, [], self._bucket)
                except ClientError as err:
                    if err.response["Error"]["Code"] in ["BucketNotEmpty"]:
                        # detected in integ-tests.
                        # culprits:
                        # - eventual consistency
                        # - an active computation might have just written data to internal storage. system must be
                        # paused but in some drivers active compute resources might not have been forced to shut down.
                        # Example: Glue driver not forcing existing job-runs to finish off.
                        module_logger.critical(
                            "New data has been found during bucket deletion, will attempt to " "empty and delete the bucket again..."
                        )
                        continue
                    raise
                break
            self._bucket_name = None
            self._bucket = None
            module_logger.critical("Application data has been permanently deleted!")

        # 2- Delete the topic
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
                    module_logger.error(f"Couldn't delete the event channel {self._topic_arn} for the storage! Error:" f"{str(err)}")
                    raise
            self._topic_arn = None

        # 3- Delete the CMK
        # First make sure that the key is accessible by other entities within the same account.
        # This should not be a concern at this point as we have just wiped out the entire storage.
        # Avoid a deadlock (need to reach out to AWS support) in case of issues in the rest of termination flow,
        # and also not to leave an about-to-be-deleted entity (dev role) within its policy. The latter is enough of
        # a concern to be consistent with what we always do in termination to revert whatever was done in the activation.
        exponential_retry(
            put_cmk_policy,
            ["DependencyTimeoutException", "KMSInternalException"],
            self._kms,
            self._cmk_id,
            self._account_id,
            users_to_be_added=set(),  # leave as is
            users_to_be_removed={self._params[AWSCommonParams.IF_DEV_ROLE]},
            trust_same_account=True,
        )  # allow access from all entites, console from the same account.

        # 3.1- CMK Alias
        if self._cmk_alias:
            try:
                exponential_retry(
                    delete_alias,
                    ["DependencyTimeoutException", "KMSInternalException", "AccessDeniedException"],
                    self._kms,
                    self._cmk_alias,
                )
            except ClientError as err:
                if err.response["Error"]["Code"] not in ["NotFoundException", "NotFound", "ResourceNotFoundException"]:
                    module_logger.error(f"Couldn't delete KMS alias {self._cmk_alias} for the storage! Error: " f"{str(err)}")
                    raise
            self._cmk_alias = None

        # 3.2- CMK itself
        if self._cmk_id:
            try:
                deletion_date = exponential_retry(
                    schedule_cmk_deletion,
                    ["DependencyTimeoutException", "KMSInternalException"],
                    self._kms,
                    self._cmk_id,
                    KMS_MIN_DELETION_WAITING_PERIOD_IN_DAYS,
                )
                logging.critical(f"Storage symmetric CMK {self._cmk_arn} has been scheduled for deletion on {deletion_date!r}")
                logging.critical("RheocerOS can no longer be used to retrieve or manage data encrypted using this key.")
                logging.critical(
                    f"If you have copied your data somewhere else, still want to be able to do decryption "
                    f"using this key, then use KMS:cancel_key_deletion(KeyId: {self._cmk_id!r}) API Call."
                )
                logging.critical(
                    f"But if you have disabled 'root access' to your key as part of the Storage security "
                    f"config, then this operation will only be possible with your application's "
                    f"development IAM ROLE {self._params[AWSCommonParams.IF_DEV_ROLE]!r}."
                )
            except ClientError as err:
                if err.response["Error"]["Code"] not in ["NotFoundException", "NotFound", "ResourceNotFoundException"]:
                    module_logger.error(f"Couldn't schedule deletion for KMS CMK {self._cmk_arn} for the storage! Error:" f"{str(err)}")
                    raise
            self._cmk_id = None
            self._cmk_arn = None

        super().terminate()

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)
        # update detected. this storage is replacing the prev storage impl.
        # take necessary actions, compatibility checks, data copy, etc.
        # if it is not possible, raise
