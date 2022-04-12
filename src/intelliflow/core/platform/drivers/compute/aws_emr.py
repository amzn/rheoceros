# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import uuid
from enum import Enum, unique
from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple, Type, Union, cast

import boto3
from botocore.exceptions import ClientError

from intelliflow.core.deployment import get_working_set_as_zip_stream, is_environment_immutable
from intelliflow.core.permission import PermissionContext
from intelliflow.core.platform.definitions.aws.glue.script.batch.glueetl_scala_all_ABI import GlueAllABIScala
from intelliflow.core.signal_processing import DimensionFilter, DimensionSpec, Signal, Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import (
    DATASET_HEADER_KEY,
    DATASET_SCHEMA_TYPE_KEY,
    ENCRYPTION_KEY_KEY,
    CWMetricSignalSourceAccessSpec,
    DatasetSchemaType,
    S3SignalSourceAccessSpec,
    SignalSourceAccessSpec,
    SignalSourceType,
)
from intelliflow.core.signal_processing.slot import SlotCodeMetadata, SlotCodeType, SlotType

from ...constructs import (
    BatchCompute,
    ConstructInternalMetricDesc,
    ConstructParamsDict,
    ConstructPermission,
    ConstructSecurityConf,
    EncryptionKeyAllocationLevel,
    Storage,
)
from ...definitions.aws.common import AWS_COMMON_RETRYABLE_ERRORS, MAX_SLEEP_INTERVAL_PARAM
from ...definitions.aws.common import CommonParams as AWSCommonParams
from ...definitions.aws.common import exponential_retry
from ...definitions.aws.emr.client_wrapper import EmrJobLanguage, EmrReleaseLabel, build_job_arn, validate_job_name
from ...definitions.aws.emr.script.batch.emr_default_ABI import EmrDefaultABIPython
from ...definitions.aws.emr.script.batch.emr_scala_all_ABI import EmrAllABIScala
from ...definitions.aws.glue import catalog as glue_catalog
from ...definitions.aws.glue.client_wrapper import GlueVersion, get_bundles
from ...definitions.aws.glue.script.batch.common import (
    AWS_REGION,
    BOOTSTRAPPER_PLATFORM_KEY_PARAM,
    CLIENT_CODE_BUCKET,
    CLIENT_CODE_PARAM,
    INPUT_MAP_PARAM,
    OUTPUT_PARAM,
    USER_EXTRA_PARAMS_PARAM,
    BatchInputMap,
    BatchOutput,
)
from ...definitions.aws.glue.script.batch.glueetl_default_ABI import GlueDefaultABIPython
from ...definitions.aws.s3.bucket_wrapper import MAX_BUCKET_LEN, bucket_exists, create_bucket, delete_bucket, get_bucket, put_policy
from ...definitions.aws.s3.object_wrapper import build_object_key, empty_bucket, object_exists, put_object
from ...definitions.compute import (
    ComputeExecutionDetails,
    ComputeFailedResponse,
    ComputeFailedResponseType,
    ComputeFailedSessionState,
    ComputeFailedSessionStateType,
    ComputeResourceDesc,
    ComputeResponse,
    ComputeResponseType,
    ComputeSessionDesc,
    ComputeSessionState,
    ComputeSessionStateType,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from ..aws_common import AWSConstructMixin

module_logger = logging.getLogger(__file__)


@unique
class RuntimeConfig(Enum):
    GlueVersion_0_9 = "GlueVersion0.9"
    GlueVersion_1_0 = "GlueVersion1.0"
    GlueVersion_2_0 = "GlueVersion2.0"
    GlueVersion_3_0 = "GlueVersion3.0"

    @classmethod
    def from_glue_version(cls, glue_version: GlueVersion):
        return {
            GlueVersion.VERSION_0_9: RuntimeConfig.GlueVersion_0_9,
            GlueVersion.VERSION_1_0: RuntimeConfig.GlueVersion_1_0,
            GlueVersion.VERSION_2_0: RuntimeConfig.GlueVersion_2_0,
            GlueVersion.VERSION_3_0: RuntimeConfig.GlueVersion_3_0,
        }[glue_version]


class AWSEMRBatchCompute(AWSConstructMixin, BatchCompute):
    """AWS EMR based BatchCompute impl"""

    @classmethod
    def driver_spec(cls) -> DimensionFilter:
        return DimensionFilter.load_raw(
            {
                Lang.SPARK_SQL: {ABI.PARAMETRIZED_QUERY: {"*": {"*": {}}}},  # irrespective of extra params
                Lang.PYTHON: {
                    ABI.GLUE_EMBEDDED: {
                        # also a RuntimeConfig, supported as an explicit param for compatibility with Glue driver
                        "GlueVersion": {GlueVersion.AUTO.value: {}, "1.0": {}, "2.0": {}, "3.0": {}},
                        "InstanceConfig": {"*": {}},
                        # for other EMR specific runtime configurations
                        "RuntimeConfig": {"*": {}},
                    }
                },
                Lang.SCALA: {
                    ABI.GLUE_EMBEDDED: {
                        "GlueVersion": {GlueVersion.AUTO.value: {}, "1.0": {}, "2.0": {}, "3.0": {}},
                        "InstanceConfig": {"*": {}},
                        "RuntimeConfig": {"*": {}},
                    }
                },
            }
        )

    @classmethod
    def runtime_config_mapping(cls) -> Dict[EmrJobLanguage, Dict[ABI, Dict[RuntimeConfig, Dict]]]:
        # TODO: Using Glue 3.0 spark version as it's the only tested one so far
        return {
            EmrJobLanguage.PYTHON: {
                ABI.GLUE_EMBEDDED: {
                    RuntimeConfig.GlueVersion_0_9: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_3_0),
                        "boilerplate": EmrDefaultABIPython,
                    },
                    RuntimeConfig.GlueVersion_1_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_3_0),
                        "boilerplate": EmrDefaultABIPython,
                    },
                    RuntimeConfig.GlueVersion_2_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_3_0),
                        "boilerplate": EmrDefaultABIPython,
                    },
                    RuntimeConfig.GlueVersion_3_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_3_0),
                        "boilerplate": EmrDefaultABIPython,
                    },
                }
            },
            EmrJobLanguage.SCALA: {
                ABI.GLUE_EMBEDDED: {
                    RuntimeConfig.GlueVersion_0_9: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_3_0),
                        "boilerplate": EmrAllABIScala,
                    },
                    RuntimeConfig.GlueVersion_1_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_3_0),
                        "boilerplate": EmrAllABIScala,
                    },
                    RuntimeConfig.GlueVersion_2_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_3_0),
                        "boilerplate": EmrAllABIScala,
                    },
                    RuntimeConfig.GlueVersion_3_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_3_0),
                        "boilerplate": EmrAllABIScala,
                    },
                }
            },
        }

    def __init__(self, params: ConstructParamsDict) -> None:
        super().__init__(params)
        self._emr = self._session.client("emr", region_name=self._region)
        self._ec2 = self._session.resource("ec2", region_name=self._region)
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._bucket = None
        self._bucket_name = None
        self._iam = self._session.resource("iam", region_name=self._region)
        self._intelliflow_python_workingset_key = None

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._emr = self._session.client("emr", region_name=self._region)
        self._ec2 = self._session.resource("ec2", region_name=self._region)
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._bucket = get_bucket(self._s3, self._bucket_name)
        self._iam = self._session.resource("iam", region_name=self._region)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._emr = None
        self._ec2 = None
        self._s3 = None
        self._bucket = None
        self._iam = None

    def provide_output_attributes(self, slot: Slot, user_attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # header: supports both so it is up to user input. but default to True if not set.
        return {DATASET_HEADER_KEY: user_attrs.get(DATASET_HEADER_KEY, True), DATASET_SCHEMA_TYPE_KEY: DatasetSchemaType.SPARK_SCHEMA_JSON}

    def query_external_source_spec(
        self, ext_signal_source: SignalSourceAccessSpec
    ) -> Optional[Tuple[SignalSourceAccessSpec, DimensionSpec]]:
        if ext_signal_source.source == SignalSourceType.GLUE_TABLE:
            return glue_catalog.query_table_spec(ext_signal_source.database, ext_signal_source.table_name)

        raise NotImplementedError(
            f"This external signal source ({ext_signal_source.source!r}) cannot be queried"
            f" by BatchCompute driver: {self.__class__.__name__}"
        )

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)

        # construct lang -> runtime_version -> {name, arn, boilerplate, suffix, ext}
        # how to de-dup?? each run will create a new folder with uuid
        # arn format https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazonelasticmapreduce.html#amazonelasticmapreduce-resources-for-iam-policies
        self._intelliflow_python_workingset_key = build_object_key(["batch"], "bundle.zip")
        self._bucket_name = self.build_bucket_name()
        # eagerly validate all possible job names
        self.validate_job_names()

    def validate_job_names(self):
        for lang, lang_spec in self.runtime_config_mapping().items():
            for abi, abi_spec in lang_spec.items():
                for runtime_config, runtime_config_spec in abi_spec.items():
                    boilerplate_type = runtime_config_spec["boilerplate"]
                    if not boilerplate_type:
                        raise ValueError(f"No boilerplate defined for lang: {lang}, abi: {abi}, runtime_config: {runtime_config}")
                    job_name = self.build_job_name(lang, abi, runtime_config)
                    if len(job_name) > 255:
                        raise ValueError(
                            f"Cannot dev_init {self.__class__.__name__} due to very long"
                            f" AWS EMR Job Name {job_name} (limit < 255),"
                            f" as a result of very long context_id '{self._dev_platform.context_id}'."
                        )
                    if not validate_job_name(job_name):
                        raise ValueError(
                            f"Cannot dev_init {self.__class__.__name__} due to invalid job name {job_name} doesn't meet EMR job name "
                            f"pattern"
                        )

    def build_bucket_name(self) -> str:
        bucket_name: str = f"if-awsemr-{self._dev_platform.context_id.lower()}-{self._account_id}-{self._region}"
        bucket_len_diff = len(bucket_name) - MAX_BUCKET_LEN
        if bucket_len_diff > 0:
            msg = (
                f"Platform context_id '{self._dev_platform.context_id}' is too long (by {bucket_len_diff}!"
                f" {self.__class__.__name__} needs to use it create {bucket_name} bucket in S3."
                f" Please refer https://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html"
                f" to align your naming accordingly in order to be able to use this driver."
            )
            module_logger.error(msg)
            raise ValueError(msg)
        return bucket_name

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._emr = boto3.client("emr", region_name=self._region)
        self._ec2 = boto3.resource("ec2", region_name=self._region)
        # TODO comment the following, probably won't need at runtime
        self._s3 = boto3.resource("s3")
        self._bucket = get_bucket(self._s3, self._bucket_name)

    def compute(
        self,
        materialized_inputs: List[Signal],
        slot: Slot,
        materialized_output: Signal,
        execution_ctx_id: str,
        retry_session_desc: Optional[ComputeSessionDesc] = None,
    ) -> ComputeResponse:
        return ComputeFailedResponse(
            ComputeFailedSessionStateType.COMPUTE_INTERNAL,
            ComputeResourceDesc("placeholder", "placeholder", driver=self.__class__),
            "NotImplemented",
            "NotImplemented",
        )

    def get_session_state(
        self, session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
    ) -> ComputeSessionState:
        return ComputeFailedSessionState(ComputeFailedSessionStateType.COMPUTE_INTERNAL, session_desc, [])

    def kill_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        pass

    def provide_runtime_trusted_entities(self) -> List[str]:
        return ["elasticmapreduce.amazonaws.com", "ec2.amazonaws.com"]

    def provide_runtime_default_policies(self) -> List[str]:
        return [
            # TODO: fill policies
            # "service-role/AmazonEMRServicePolicy_v2",
            # "service-role/AmazonElasticMapReduceRole",
            # "service-role/AmazonElasticMapReduceforEC2Role",
        ]

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        # allow exec-role (post-activation, cumulative list of all trusted entities [AWS services]) to do the following;
        permissions = [
            ConstructPermission([f"arn:aws:s3:::{self._bucket_name}", f"arn:aws:s3:::{self._bucket_name}/*"], ["s3:*"]),
            # TODO be more picky.
            # allow other service assuming our role to call the jobs here
            ConstructPermission([build_job_arn(self._region, self._account_id, "*")], ["ec2:*", "elasticmapreduce:*"]),
            # CW Logs (might look redundant, but please forget about other drivers while declaring these),
            # deduping is handled automatically.
            ConstructPermission([f"arn:aws:logs:{self._region}:{self._account_id}:*"], ["logs:*"]),
            # must add a policy to allow your users the iam:PassRole permission for IAM roles to match your naming convention
            ConstructPermission([self._params[AWSCommonParams.IF_EXE_ROLE]], ["iam:PassRole"]),
        ]

        external_library_resource_arns = set()
        for route in self._pending_internal_routes:
            for slot in route.slots:
                if slot.code_metadata.external_library_paths:
                    for s3_path in slot.code_metadata.external_library_paths:
                        try:
                            s3_spec = S3SignalSourceAccessSpec.from_url(account_id=None, url=s3_path)
                        except Exception:
                            module_logger.error(
                                f"External library path {s3_path} attached to route {route.route_id!r} "
                                f" via slot: {(slot.type, slot.code_lang)!r} is not supported by "
                                f" BatchCompute driver {self.__class__.__name__!r}. "
                            )
                            raise
                        # exact resource (JARs, zips)
                        external_library_resource_arns.add(f"arn:aws:s3:::{s3_spec.bucket}/{s3_path[len(f's3://{s3_spec.bucket}/'):]}")

                # TODO Move into <BatchCompute>
                # TODO evalute moving is_batch_compute check even before the external library paths extraction.
                if slot.type.is_batch_compute() and slot.permissions:
                    for compute_perm in slot.permissions:
                        # TODO check compute_perm feasibility in AWS EMR (check ARN, resource type, etc)
                        if compute_perm.context != PermissionContext.DEVTIME:
                            permissions.append(ConstructPermission(compute_perm.resource, compute_perm.action))

        if external_library_resource_arns:
            permissions.append(
                ConstructPermission(list(external_library_resource_arns), ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket"])
            )

        # might look familiar (from Processor impl maybe), but please forget about other drivers while declaring these),
        # deduping is handled automatically.
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

            encryption_key_list: Set[str] = {
                ext_signal.resource_access_spec.encryption_key
                for ext_signal in ext_s3_signals
                if ext_signal.resource_access_spec.encryption_key
            }

            if encryption_key_list:
                permissions.append(
                    ConstructPermission(
                        list(encryption_key_list),
                        [
                            "kms:Decrypt",
                            "kms:DescribeKey",
                            "kms:GenerateDataKey",
                            "kms:DescribeCustomKeyStores",
                            "kms:ListKeys",
                            "kms:ListAliases",
                        ],
                    )
                )

        return permissions

    @classmethod
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        return [
            # TODO narrow down to exact operations and resources
            ConstructPermission(["*"], ["elasticmapreduce:*"]),
            ConstructPermission(["*"], ["ec2:*"]),
            # instance-profile for ec2 instance in cluster
            ConstructPermission(
                [f"arn:aws:iam::{params[AWSCommonParams.ACCOUNT_ID]}:instance-profile/*"],
                ["iam:CreateInstanceProfile", "iam:AddRoleToInstanceProfile"],
            ),
        ]

    def _provide_system_metrics(self) -> List[Signal]:
        return []

    def _provide_internal_metrics(self) -> List[ConstructInternalMetricDesc]:
        return []

    def _provide_route_metrics(self, route: Route) -> List[ConstructInternalMetricDesc]:
        return []

    def _provide_internal_alarms(self) -> List[Signal]:
        return []

    def build_bootstrapper_object_key(self) -> str:
        return build_object_key(["bootstrapper"], f"{self.__class__.__name__.lower()}_RuntimePlatform.data")

    def _update_bootstrapper(self, bootstrapper: "RuntimePlatform") -> None:
        # uploading it to S3 and passing S3 link as job arg.
        bootstrapped_platform = bootstrapper.serialize()

        self.bootstrapper_object_key = self.build_bootstrapper_object_key()
        exponential_retry(
            put_object, {"ServiceException", "TooManyRequestsException"}, self._bucket, self.bootstrapper_object_key, bootstrapped_platform
        )

    def activate(self) -> None:
        if not bucket_exists(self._s3, self._bucket_name):
            self._setup_scripts_bucket()
        else:
            self._bucket = get_bucket(self._s3, self._bucket_name)


        bundles: List[Tuple[str, "Path"]] = get_bundles("1.0")
        self._bundle_s3_keys = []
        self._bundle_s3_paths = []
        for bundle_name, bundle_path in bundles:
            bundle_s3_key = build_object_key(["batch", "lib"], bundle_name)
            self._bundle_s3_keys.append(bundle_s3_key)
            self._bundle_s3_paths.append(f"s3://{self._bucket_name}/{bundle_s3_key}")

            if not object_exists(self._s3, self._bucket, bundle_s3_key):
                exponential_retry(
                    put_object,
                    {"ServiceException", "TooManyRequestsException"},
                    self._bucket,
                    bundle_s3_key,
                    open(bundle_path, "rb").read(),
                )

        for lang, lang_spec in self.runtime_config_mapping().items():
            if lang == EmrJobLanguage.PYTHON:
                # Upload the bundle (working set) to its own bucket.
                exponential_retry(
                    put_object,
                    {"ServiceException", "TooManyRequestsException"},
                    self._bucket,
                    self._intelliflow_python_workingset_key,
                    get_working_set_as_zip_stream(),
                )
            for abi, abi_spec in lang_spec.items():
                for runtime_config, runtime_config_spec in abi_spec.items():
                    batch = runtime_config_spec["boilerplate"]()
                    file_ext = lang.extension
                    batch_script_file_key = build_object_key(["batch"], f"emretl_{batch.__class__.__name__.lower()}.{file_ext}")
                    exponential_retry(
                        put_object,
                        {"ServiceException", "TooManyRequestsException"},
                        self._bucket,
                        batch_script_file_key,
                        batch.generate_emr_script().encode("utf-8"),
                    )

        super().activate()

    def rollback(self) -> None:
        super().rollback()

    def terminate(self) -> None:
        super().terminate()
        # TODO eliminate emr instance profile

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)

    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
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

    def _revert_internal(self, routes: Set[Route], prev_routes: Set[Route]) -> None:
        pass

    def _revert_internal_signals(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        pass

    def _revert_construct_connections(
        self, construct_conns: Set["_PendingConnRequest"], prev_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _revert_security_conf(self, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        pass

    def build_job_name(self, lang: EmrJobLanguage, abi: ABI, runtime_config: RuntimeConfig):
        return (
            f"IntelliFlow-{self._dev_platform.context_id}-{self._region}-{self.__class__.__name__}-{lang.extension}-{abi.name.lower()}-"
            f"{runtime_config.name}"
        )

    def _setup_scripts_bucket(self):
        """Initial setup of storage bucket. Enforces policy for access from dev and exec roles."""
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
            elif error.response["Error"]["Code"] == "BucketAlreadyExists":
                msg = (
                    f"Bucket {self._bucket_name!r} has been taken by some other application. Cannot "
                    f"proceed with activation until S3 bucket is retained by same account "
                    f" (AWS Entity: {self._params[AWSCommonParams.IF_DEV_ROLE]!r}, Region: {self.region})."
                )
                module_logger.error(msg)
                raise RuntimeError(msg, error)
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
                    "Action": ["s3:*"],
                    # TODO post-MVP
                    # the following is the complete list for both Data sources + targets combined.
                    # 'Action': [ 's3:GetObject', 's3:PutObject', 's3:DeleteObject', 's3:GetObjectVersion' 's3:ListBucket' ],
                    "Resource": [f"arn:aws:s3:::{self._bucket.name}/*", f"arn:aws:s3:::{self._bucket.name}"],
                },
            ],
        }
        try:
            exponential_retry(put_policy, ["MalformedPolicy"], self._s3, self._bucket.name, put_policy_desc)
        except ClientError as error:
            if error.response["Error"]["Code"] == "MalformedPolicy":
                module_logger.error("Couldn't put the policy for EMR scripts folder! Error:", str(error))
            else:
                raise
