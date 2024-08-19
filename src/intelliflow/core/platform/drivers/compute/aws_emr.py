# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import base64
import json
import logging
import re
import uuid
from datetime import datetime
from enum import Enum, unique
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Set, Tuple, cast

import boto3
from botocore.exceptions import ClientError

from intelliflow.core.deployment import get_working_set_as_zip_stream
from intelliflow.core.permission import PermissionContext
from intelliflow.core.platform.definitions.aws.emr.script.batch.common import (
    APPLICATIONS,
    AWS_REGION,
    BOOTSTRAPPER_PLATFORM_KEY_PARAM,
    CLIENT_CODE_BUCKET,
    CLIENT_CODE_PARAM,
    EMR_BOOTSTRAP_ACTIONS,
    EMR_CONFIGURATIONS,
    EMR_INSTANCES_SPECS,
    EXTRA_JARS,
    IGNORED_BUNDLE_MODULES_PARAM,
    INPUT_MAP_PARAM,
    INSTANCE_CONFIG_KEY,
    JOB_NAME_PARAM,
    OUTPUT_PARAM,
    RESERVED_INSTANCES_SPECS,
    RESERVED_SPARK_CLI_ARGS,
    RUNTIME_CONFIG_KEY,
    SECURITY_CONFIGURATION,
    SPARK_CLI_ARGS,
    USER_EXTRA_PARAMS_PARAM,
    WORKING_SET_OBJECT_PARAM,
)
from intelliflow.core.platform.definitions.common import ActivationParams
from intelliflow.core.signal_processing import DimensionFilter, DimensionSpec, Signal, Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal_source import (
    DATA_FORMAT_KEY,
    DATA_TYPE_KEY,
    DATASET_FORMAT_KEY,
    DATASET_HEADER_KEY,
    DATASET_SCHEMA_TYPE_KEY,
    DatasetSchemaType,
    DatasetSignalSourceFormat,
    DataType,
    S3SignalSourceAccessSpec,
    SignalSourceAccessSpec,
    SignalSourceType,
)
from intelliflow.core.signal_processing.slot import SlotCodeMetadata, SlotCodeType, SlotType
from intelliflow.utils.algorithm import chunk_iter

from ...constructs import BatchCompute, ConstructInternalMetricDesc, ConstructParamsDict, ConstructPermission, ConstructSecurityConf
from ...definitions.aws.common import AWS_COMMON_RETRYABLE_ERRORS, MAX_SLEEP_INTERVAL_PARAM
from ...definitions.aws.common import CommonParams as AWSCommonParams
from ...definitions.aws.common import exponential_retry, has_aws_managed_policy
from ...definitions.aws.emr.client_wrapper import (
    EmrJobLanguage,
    EmrReleaseLabel,
    build_capacity_params,
    build_glue_catalog_configuration,
    build_job_arn,
    create_job_flow_instance_profile,
    delete_instance_profile,
    describe_emr_cluster,
    get_emr_cluster_failure_type,
    get_emr_cluster_state_type,
    get_emr_step,
    list_emr_clusters,
    start_emr_job_flow,
    terminate_emr_job_flow,
    translate_glue_worker_type,
    validate_job_name,
)
from ...definitions.aws.emr.script.batch.emr_default_ABI import EmrDefaultABIPython
from ...definitions.aws.emr.script.batch.emr_scala_all_ABI import EmrAllABIScala
from ...definitions.aws.glue import catalog as glue_catalog
from ...definitions.aws.glue.client_wrapper import GlueVersion, get_bundles
from ...definitions.aws.glue.script.batch.common import (
    AWS_REGION,
    BOOTSTRAPPER_PLATFORM_KEY_PARAM,
    CLIENT_CODE_BUCKET,
    CLIENT_CODE_PARAM,
    EXECUTION_ID,
    INPUT_MAP_PARAM,
    OUTPUT_PARAM,
    USER_EXTRA_PARAMS_PARAM,
    BatchInputMap,
    BatchOutput,
)
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
    create_output_dimension_map,
)
from ..aws_common import AWSConstructMixin

module_logger = logging.getLogger(__file__)


@unique
class RuntimeConfig(Enum):
    GlueVersion_0_9 = "GlueVersion0.9"
    GlueVersion_1_0 = "GlueVersion1.0"
    GlueVersion_2_0 = "GlueVersion2.0"
    GlueVersion_3_0 = "GlueVersion3.0"
    GlueVersion_4_0 = "GlueVersion4.0"
    EMR_6_4_0 = "EMR_6_4_0"
    EMR_6_6_0 = "EMR_6_6_0"
    EMR_6_8_0 = "EMR_6_8_0"
    EMR_6_10_0 = "EMR_6_10_0"
    AUTO = "AUTO"

    @classmethod
    def from_glue_version(cls, glue_version: GlueVersion):
        return {
            GlueVersion.VERSION_0_9: RuntimeConfig.GlueVersion_0_9,
            GlueVersion.VERSION_1_0: RuntimeConfig.GlueVersion_1_0,
            GlueVersion.VERSION_2_0: RuntimeConfig.GlueVersion_2_0,
            GlueVersion.VERSION_3_0: RuntimeConfig.GlueVersion_3_0,
            GlueVersion.VERSION_4_0: RuntimeConfig.GlueVersion_4_0,
        }[glue_version]


class InstanceConfig:
    def __init__(self, instance_count: int, instance_type: str = "m5.xlarge"):
        if instance_count <= 1:
            raise ValueError(f"instance_count={instance_count}, which should be >= 2")
        self._instance_count: int = instance_count
        self._instance_type: str = instance_type

    @property
    def instance_count(self):
        return self._instance_count

    @property
    def instance_type(self):
        return self._instance_type

    def __eq__(self, other):
        if isinstance(other, InstanceConfig):
            return self.instance_count == other.instance_count and self.instance_type == other.instance_type
        return False

    def __repr__(self):
        return f"InstanceConfig(instance_count={self.instance_count},instance_type={self.instance_type})"


class AWSEMRBatchCompute(AWSConstructMixin, BatchCompute):
    """AWS EMR based BatchCompute impl"""

    # https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html#API_RunJobFlow_Errors
    CLIENT_RETRYABLE_EXCEPTION_LIST: Set[str] = {"InternalServerException", "InternalServerError"}
    GLUE_DEFAULT_VERSION: ClassVar[GlueVersion] = GlueVersion.VERSION_4_0
    DEFAULT_RUNTIME_CONFIG: ClassVar[RuntimeConfig] = RuntimeConfig.GlueVersion_4_0
    DEFAULT_INSTANCE_CONFIG: ClassVar[InstanceConfig] = InstanceConfig(25)
    EMR_CLUSTER_SUBNET_ID: ClassVar[str] = "EmrClusterSubnetId"

    @classmethod
    def driver_spec(cls) -> DimensionFilter:
        return DimensionFilter.load_raw(
            {
                Lang.SPARK_SQL: {ABI.PARAMETRIZED_QUERY: {"*": {"*": {}}}},  # irrespective of extra params
                Lang.PYTHON: {
                    ABI.GLUE_EMBEDDED: {
                        # also a RuntimeConfig, supported as an explicit param for compatibility with Glue driver
                        "GlueVersion": {GlueVersion.AUTO.value: {}, "1.0": {}, "2.0": {}, "3.0": {}, "4.0": {}},
                        INSTANCE_CONFIG_KEY: {"*": {}},
                        # for other EMR specific runtime configurations
                        RUNTIME_CONFIG_KEY: {"*": {}},
                        SPARK_CLI_ARGS: {"*": {}},
                    }
                },
                # TODO: Scala support
                # Lang.SCALA: {
                #     ABI.GLUE_EMBEDDED: {
                #         "GlueVersion": {GlueVersion.AUTO.value: {}, "1.0": {}, "2.0": {}, "3.0": {}, "4.0": {}},
                #         INSTANCE_CONFIG_KEY: {"*": {}},
                #         RUNTIME_CONFIG_KEY: {"*": {}},
                #         SPARK_CLI_ARGS: {"*": {}},
                #     }
                # },
            }
        )

    @classmethod
    def runtime_config_mapping(cls) -> Dict[EmrJobLanguage, Dict[ABI, Dict[RuntimeConfig, Dict]]]:
        return {
            EmrJobLanguage.PYTHON: {
                ABI.GLUE_EMBEDDED: {
                    RuntimeConfig.GlueVersion_0_9: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_0_9),
                        "boilerplate": EmrDefaultABIPython,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                    RuntimeConfig.GlueVersion_1_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_1_0),
                        "boilerplate": EmrDefaultABIPython,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                    RuntimeConfig.GlueVersion_2_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_2_0),
                        "boilerplate": EmrDefaultABIPython,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                    RuntimeConfig.GlueVersion_3_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_3_0),
                        "boilerplate": EmrDefaultABIPython,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                    RuntimeConfig.GlueVersion_4_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_4_0),
                        "boilerplate": EmrDefaultABIPython,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                    RuntimeConfig.EMR_6_4_0: {
                        "runtime_version": EmrReleaseLabel.VERSION_6_4_0,
                        "boilerplate": EmrDefaultABIPython,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                    RuntimeConfig.EMR_6_6_0: {
                        "runtime_version": EmrReleaseLabel.VERSION_6_6_0,
                        "boilerplate": EmrDefaultABIPython,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                    RuntimeConfig.EMR_6_8_0: {
                        "runtime_version": EmrReleaseLabel.VERSION_6_8_0,
                        "boilerplate": EmrDefaultABIPython,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                    RuntimeConfig.EMR_6_10_0: {
                        "runtime_version": EmrReleaseLabel.VERSION_6_10_0,
                        "boilerplate": EmrDefaultABIPython,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                }
            },
            EmrJobLanguage.SCALA: {
                ABI.GLUE_EMBEDDED: {
                    RuntimeConfig.GlueVersion_0_9: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_0_9),
                        "boilerplate": EmrAllABIScala,
                        "applications": [],
                    },
                    RuntimeConfig.GlueVersion_1_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_1_0),
                        "boilerplate": EmrAllABIScala,
                        "applications": [],
                    },
                    RuntimeConfig.GlueVersion_2_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_2_0),
                        "boilerplate": EmrAllABIScala,
                        "applications": [],
                    },
                    RuntimeConfig.GlueVersion_3_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_3_0),
                        "boilerplate": EmrAllABIScala,
                        "applications": [],
                    },
                    RuntimeConfig.GlueVersion_4_0: {
                        "runtime_version": EmrReleaseLabel.resolve_from_glue_version(GlueVersion.VERSION_4_0),
                        "boilerplate": EmrAllABIScala,
                        "applications": [],
                    },
                    RuntimeConfig.EMR_6_4_0: {
                        "runtime_version": EmrReleaseLabel.VERSION_6_4_0,
                        "boilerplate": EmrAllABIScala,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                    RuntimeConfig.EMR_6_6_0: {
                        "runtime_version": EmrReleaseLabel.VERSION_6_6_0,
                        "boilerplate": EmrAllABIScala,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                    RuntimeConfig.EMR_6_8_0: {
                        "runtime_version": EmrReleaseLabel.VERSION_6_8_0,
                        "boilerplate": EmrAllABIScala,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
                    },
                    RuntimeConfig.EMR_6_10_0: {
                        "runtime_version": EmrReleaseLabel.VERSION_6_10_0,
                        "boilerplate": EmrAllABIScala,
                        "applications": ["Hadoop", "Pig", "Hue", "Spark"],
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
        self._iam = self._session.client("iam", region_name=self._region)
        self._intelliflow_python_workingset_key = None

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._emr = self._session.client("emr", region_name=self._region)
        self._ec2 = self._session.resource("ec2", region_name=self._region)
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._bucket = get_bucket(self._s3, self._bucket_name)
        self._iam = self._session.client("iam", region_name=self._region)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._emr = None
        self._ec2 = None
        self._s3 = None
        self._bucket = None
        self._iam = None

    def provide_output_attributes(self, inputs: List[Signal], slot: Slot, user_attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # early validation for compute params that will be used at runtime
        # TODO: before adding as a mainline feature, document 'partition_by' as a keyword in `args` dict: https://sim.amazon.com/issues/SVEN-438
        if slot.extra_params and "partition_by" in slot.extra_params:
            partition_cols = slot.extra_params["partition_by"]
            is_valid_input = partition_cols and isinstance(partition_cols, list) and all(isinstance(item, str) for item in partition_cols)
            if not is_valid_input:
                raise ValueError("`partition_by` param must be a nonempty List[str]!")

        if user_attrs.get(DATA_TYPE_KEY, DataType.DATASET) != DataType.DATASET:
            raise ValueError(
                f"{DATA_TYPE_KEY!r} must be defined as {DataType.DATASET} or left undefined for {self.__class__.__name__} output!"
            )

        # default to CSV
        data_format_value = user_attrs.get(DATASET_FORMAT_KEY, user_attrs.get(DATA_FORMAT_KEY, None))
        data_format = DatasetSignalSourceFormat.CSV if data_format_value is None else DatasetSignalSourceFormat(data_format_value)

        # header: supports both so it is up to user input. but default to True if not set.
        return {
            DATA_TYPE_KEY: DataType.DATASET,
            DATASET_HEADER_KEY: user_attrs.get(DATASET_HEADER_KEY, True),
            DATASET_SCHEMA_TYPE_KEY: DatasetSchemaType.SPARK_SCHEMA_JSON,
            DATASET_FORMAT_KEY: data_format,
        }

    def query_external_source_spec(
        self, ext_signal_source: SignalSourceAccessSpec
    ) -> Optional[Tuple[SignalSourceAccessSpec, DimensionSpec]]:
        raise NotImplementedError(
            f"This external signal source ({ext_signal_source.source!r}) cannot be queried"
            f" by BatchCompute driver: {self.__class__.__name__}"
        )

    # overrides
    def get_max_wait_time_for_next_retry_in_secs(self) -> int:
        """Owerwrite the maximum interval used by the default retry strategy in
        BatchCompute::can_retry
        """
        # retry with increasing probability as wait time gets close to this
        return 100 * 60

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)

        # construct lang -> runtime_version -> {name, arn, boilerplate, suffix, ext}
        # how to de-dup?? each run will create a new folder with uuid
        # arn format https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazonelasticmapreduce.html#amazonelasticmapreduce-resources-for-iam-policies
        self._intelliflow_python_workingset_key = build_object_key(["batch"], "bundle.zip")
        self._bucket_name = self.build_bucket_name(self.unique_context_id)
        # eagerly validate all possible job names
        self.validate_job_names()

    def validate_job_names(self):
        for lang, lang_spec in self.runtime_config_mapping().items():
            for abi, abi_spec in lang_spec.items():
                for runtime_config, runtime_config_spec in abi_spec.items():
                    boilerplate_type = runtime_config_spec["boilerplate"]
                    if not boilerplate_type:
                        raise ValueError(f"No boilerplate defined for lang: {lang}, abi: {abi}, runtime_config: {runtime_config}")
                    job_name = self._build_job_name(lang, abi, runtime_config, str(uuid.uuid1()))
                    if len(job_name) > 255:
                        raise ValueError(
                            f"Cannot dev_init {self.__class__.__name__} due to very long"
                            f" AWS EMR Job Name {job_name} (limit < 255),"
                            f" as a result of very long unique_context_id '{self.unique_context_id}'."
                        )
                    if not validate_job_name(job_name):
                        raise ValueError(
                            f"Cannot dev_init {self.__class__.__name__} due to invalid job name {job_name} doesn't meet EMR job name "
                            f"pattern"
                        )

    def build_bucket_name(self, unique_context_id: str) -> str:
        # https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
        bucket_name: str = re.sub(r"[^a-z0-9.-]", "-", f"if-awsemr-{unique_context_id.lower()}")
        bucket_len_diff = len(bucket_name) - MAX_BUCKET_LEN
        if bucket_len_diff > 0:
            msg = (
                f"Platform context_id '{self.context_id}' is too long (by {bucket_len_diff}!"
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
        route: Route,
        materialized_inputs: List[Signal],
        slot: Slot,
        materialized_output: Signal,
        execution_ctx_id: str,
        retry_session_desc: Optional[ComputeSessionDesc] = None,
    ) -> ComputeResponse:
        code_metadata: SlotCodeMetadata = slot.code_metadata
        lang = EmrJobLanguage.from_slot_lang(slot.code_lang)
        abi = slot.code_abi
        extra_params: Dict[str, Any] = dict(slot.extra_params)

        runtime_config: RuntimeConfig = self._translate_runtime_config(extra_params, materialized_inputs)
        instance_config: InstanceConfig = self._translate_instance_config(extra_params)
        runtime_spec = self.runtime_config_mapping()[lang][abi][runtime_config]
        applications = list(set(runtime_spec["applications"]) | set(extra_params.get(APPLICATIONS, {})))

        input_map = BatchInputMap(materialized_inputs)
        output = BatchOutput(materialized_output)

        compute_start_time = str(datetime.utcnow())
        output_dimensions_map = create_output_dimension_map(materialized_output)
        output_dimension_values = [str(i) for i in [materialized_output.alias] + list(output_dimensions_map.values())]

        lang_code = str(slot.code_lang.value)
        unique_compute_id: str = str(uuid.uuid1())

        object_path = ["batch", "jobs"] + output_dimension_values + [compute_start_time, unique_compute_id]
        code_key = build_object_key(object_path, f"slot_code.{lang.extension}")
        exponential_retry(put_object, {"ServiceException", "TooManyRequestsException"}, self._bucket, code_key, slot.code.encode("utf-8"))

        input_map_key = build_object_key(object_path, f"input_map.json")
        exponential_retry(
            put_object, {"ServiceException", "TooManyRequestsException"}, self._bucket, input_map_key, input_map.dumps().encode("utf-8")
        )

        output_param_key = build_object_key(object_path, f"output_param.json")
        exponential_retry(
            put_object, {"ServiceException", "TooManyRequestsException"}, self._bucket, output_param_key, output.dumps().encode("utf-8")
        )

        extra_jars = self._common_bundle_s3_paths + list(extra_params.get(EXTRA_JARS, []))

        job_name = self._build_job_name(lang, abi, runtime_config, unique_compute_id)
        boilerplate_path = (
            f"s3://{self._bucket_name}/" f'{self._build_boilerplate_code_path(abi, runtime_spec["boilerplate"](), lang, runtime_config)}'
        )

        # spark submit allows multiple --conf flag, using list here
        spark_cli_args: List[str] = extra_params.get(SPARK_CLI_ARGS, [])
        extra_params.pop(SPARK_CLI_ARGS, None)

        ignored_bundle_modules = code_metadata.ignored_bundle_modules if code_metadata.ignored_bundle_modules else []

        emr_cli_args = self._build_emr_cli_arg(
            self.unique_context_id,
            boilerplate_path,
            code_key,
            extra_jars,
            input_map_key,
            output_param_key,
            spark_cli_args,
            f"s3://{self._bucket_name}/{self._intelliflow_python_workingset_key}",
            extra_params,
            execution_ctx_id,
            ignored_bundle_modules,
        )
        module_logger.info(f"Job run id: {unique_compute_id} is using spark cli args: {emr_cli_args!r}")

        configurations: List = list(extra_params.get(EMR_CONFIGURATIONS, []))
        extra_params.pop(EMR_CONFIGURATIONS, None)
        configurations.append(build_glue_catalog_configuration())

        bootstrap_actions: List = self._common_bootstrap_actions + list(extra_params.get(EMR_BOOTSTRAP_ACTIONS, []))
        extra_params.pop(EMR_BOOTSTRAP_ACTIONS, None)

        emr_instances_specs = dict(extra_params.get(EMR_INSTANCES_SPECS, {}))
        app_subnet_id = self._params.get(self.EMR_CLUSTER_SUBNET_ID, None)
        if "Ec2SubnetId" not in emr_instances_specs and app_subnet_id:
            emr_instances_specs["Ec2SubnetId"] = app_subnet_id
        extra_params.pop(EMR_INSTANCES_SPECS, None)
        capacity_params = build_capacity_params(instance_config)
        emr_instances_specs.update(capacity_params)

        security_config = extra_params.get(SECURITY_CONFIGURATION, None)
        extra_params.pop(SECURITY_CONFIGURATION, None)

        try:
            cluster_id = start_emr_job_flow(
                self._emr,
                job_name,
                runtime_spec["runtime_version"].aws_label,
                f"s3://{self._bucket_name}/emr_logs/{build_object_key(object_path, '')}",
                applications,
                emr_cli_args,
                self._params[AWSCommonParams.IF_EXE_ROLE],
                configurations,
                emr_instances_specs,
                security_config,
                bootstrap_actions,
            )
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in self.CLIENT_RETRYABLE_EXCEPTION_LIST or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                failed_response_type = ComputeFailedResponseType.TRANSIENT
            else:
                failed_response_type = ComputeFailedResponseType.UNKNOWN

            return ComputeFailedResponse(
                failed_response_type,
                ComputeResourceDesc("N/A", "N/A", driver=self.__class__),
                error_code,
                str(error.response["Error"]),
            )

        return ComputeSuccessfulResponse(
            ComputeSuccessfulResponseType.PROCESSING,
            ComputeSessionDesc(
                cluster_id,
                ComputeResourceDesc(cluster_id, build_job_arn(self._region, self._account_id, cluster_id), driver=self.__class__),
            ),
        )

    def _build_emr_cli_arg(
        self,
        app_name: str,
        boilerplate_path: str,
        code_key: str,
        extra_jars: List[str],
        input_map_key: str,
        output_param_key: str,
        user_spark_args: List[str],
        working_set_key: str,
        extra_params_key: Dict[str, Any],
        execution_ctx_id: str,
        ignored_bundle_modules: List[str],
    ) -> List[str]:
        emr_cli_arg = [
            "spark-submit",
            "--deploy-mode",
            "cluster",
            "--jars",
            ",".join(extra_jars),
            "--conf",
            "spark.sql.catalogImplementation=hive",
        ]
        emr_cli_arg.extend(user_spark_args if user_spark_args else [])
        emr_cli_arg.append(boilerplate_path)

        boilerplate_cli_arg: Dict[str, str] = {f"--{key}": str(value) for key, value in extra_params_key.items()}
        user_params_keys = list(set(boilerplate_cli_arg.keys()))
        boilerplate_cli_arg.update(
            {
                f"--{JOB_NAME_PARAM}": f"if-{app_name}-emr-job",
                f"--{CLIENT_CODE_BUCKET}": self._bucket_name,
                f"--{CLIENT_CODE_PARAM}": code_key,
                f"--{INPUT_MAP_PARAM}": input_map_key,
                f"--{OUTPUT_PARAM}": output_param_key,
                f"--{BOOTSTRAPPER_PLATFORM_KEY_PARAM}": self._build_bootstrapper_object_key(),
                f"--{AWS_REGION}": self.region,
                # TODO: maybe we can use spark-submit --pyfiles directly
                f"--{WORKING_SET_OBJECT_PARAM}": working_set_key,
                f"--{EXECUTION_ID}": execution_ctx_id,
                f"--{USER_EXTRA_PARAMS_PARAM}": json.dumps(user_params_keys),
                f"--{IGNORED_BUNDLE_MODULES_PARAM}": json.dumps(ignored_bundle_modules),
            }
        )
        for k, v in boilerplate_cli_arg.items():
            emr_cli_arg.append(k)
            emr_cli_arg.append(v)
        return emr_cli_arg

    def get_session_state(
        self, session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord"
    ) -> ComputeSessionState:
        cluster_id = session_desc.session_id
        execution_details = None
        try:
            cluster_response = exponential_retry(
                describe_emr_cluster, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._emr, cluster_id, **{MAX_SLEEP_INTERVAL_PARAM: 16}
            )["Cluster"]
            cluster_status = cluster_response["Status"]
            timeline = cluster_status["Timeline"]
            start_time = timeline.get("CreationDateTime", None)
            end_time = timeline.get("EndDateTime", None)
            details = dict(cluster_response)
            execution_details = ComputeExecutionDetails(start_time, end_time, details)

            session_state = get_emr_cluster_state_type(cluster_status)

            if session_state == ComputeSessionStateType.FAILED:
                failure_type = get_emr_cluster_failure_type(cluster_status)
                # get step details (add EMR step description, note one cluster only has one EMR step by design)
                step_details = self._retrieve_step_details(cluster_id, 8)
                details.update({"step_details": step_details})
                return ComputeFailedSessionState(failure_type, session_desc, [execution_details])
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code in self.CLIENT_RETRYABLE_EXCEPTION_LIST or error_code in AWS_COMMON_RETRYABLE_ERRORS:
                # don't mark it as failed but let orchestration know about that session state could not be retrieved
                session_state = ComputeSessionStateType.TRANSIENT_UNKNOWN
            else:
                failure_type = (
                    ComputeFailedSessionStateType.NOT_FOUND
                    if error_code in ["EntityNotFoundException"]
                    else ComputeFailedSessionStateType.UNKNOWN
                )
                # provide information to orchestration
                return ComputeFailedSessionState(failure_type, session_desc, [execution_details])

        return ComputeSessionState(session_desc, session_state, [execution_details])

    def _retrieve_step_details(self, cluster_id: str, max_sleep_interval: int) -> Dict[str, Any]:
        try:
            return exponential_retry(
                get_emr_step, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._emr, cluster_id, **{MAX_SLEEP_INTERVAL_PARAM: max_sleep_interval}
            )
        except Exception as error:
            return {"error": f"Could not retrieve step details due to error: {error!r}"}

    def describe_compute_record(self, active_compute_record: "RoutingTable.ComputeRecord") -> Optional[Dict[str, Any]]:
        execution_details = super().describe_compute_record(active_compute_record)
        if "details" in execution_details:
            # In default implementation, execution_details['details'] comes from `get_session_state()`, i.e. cluster description
            cluster_details = execution_details["details"]
            execution_details["details"] = {"cluster_details": cluster_details}
            if "step_details" in cluster_details:
                # in get_session_state currently step_details is retrieved for FAILED computes, let's extract it out and
                # keep it at the root level for better presentation
                step_details = cluster_details.pop("step_details")
                execution_details["details"].update({"step_details": step_details})
        return execution_details

    def provide_runtime_trusted_entities(self) -> List[str]:
        return ["elasticmapreduce.amazonaws.com", "ec2.amazonaws.com"]

    def provide_runtime_default_policies(self) -> List[str]:
        managed_policies = [
            # https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-managed-iam-policies.html
            "AmazonElasticMapReduceFullAccess",
            "service-role/AmazonElasticMapReduceRole",
            "service-role/AmazonElasticMapReduceforEC2Role",
            # "aws-service-role/AmazonEMRCleanupPolicy",
            "service-role/AmazonElasticMapReduceforAutoScalingRole",
            "service-role/AmazonElasticMapReduceEditorsRole",
        ]

        infosec_policy_arn: str = f"arn:aws:iam::{self._account_id}:policy/CloudRanger/InfoSecHostMonitoringPolicy-DO-NOT-DELETE"
        if has_aws_managed_policy(infosec_policy_arn, self.session):
            managed_policies.append(infosec_policy_arn)

        return managed_policies

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
                    for path in slot.code_metadata.external_library_paths:
                        try:
                            s3_spec = S3SignalSourceAccessSpec.from_url(account_id=None, url=path)
                            # exact resource (JARs, zips)
                            external_library_resource_arns.add(f"arn:aws:s3:::{s3_spec.bucket}/{path[len(f's3://{s3_spec.bucket}/'):]}")
                        except Exception:
                            module_logger.warning(
                                f"External library path {path} attached to route {route.route_id!r} "
                                f" via slot: {(slot.type, slot.code_lang)!r} is not an S3 path, assuming that it is just"
                                f" a PyPI library name, BatchCompute driver {self.__class__.__name__!r} won't add it to"
                                f" runtime permissions for exec role."
                            )

                extra_jars = slot.extra_params.get(EXTRA_JARS, [])
                for path in extra_jars:
                    try:
                        s3_spec = S3SignalSourceAccessSpec.from_url(account_id=None, url=path)
                        external_library_resource_arns.add(f"arn:aws:s3:::{s3_spec.bucket}/{path[len(f's3://{s3_spec.bucket}/'):]}")
                    except Exception:
                        raise ValueError(
                            f"External JAR path {path!r} attached to route {route.route_id!r} "
                            f" via slot: {(slot.type, slot.code_lang)!r} is not an S3 path!"
                        )

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
            # Glue catalog access
            ConstructPermission(
                [
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:catalog",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:database/*",
                ],
                ["glue:GetDatabases"],
            ),
            # Read-access into everything else in the same catalog
            # Refer
            #   https://docs.aws.amazon.com/glue/latest/dg/glue-specifying-resource-arns.html
            ConstructPermission(
                [
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:catalog",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:database/default",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:database/*",
                    f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:table/*/*",
                    # f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:userDefinedFunction/*/*",
                    # f"arn:aws:glue:{params[AWSCommonParams.REGION]}:{params[AWSCommonParams.ACCOUNT_ID]}:connection/*",
                ],
                [
                    "glue:GetDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:BatchGetPartition",
                    "glue:Get*",
                    "glue:BatchGet*",
                ],
            ),
            # More permissive read access on other non-catalog entities
            ConstructPermission(
                ["*"],
                [
                    "glue:ListCrawlers",
                    "glue:BatchGetCrawlers",
                    "glue:ListDevEndpoints",
                    "glue:BatchGetDevEndpoints",
                    "glue:GetJob",
                    "glue:GetJobs",
                    "glue:ListJobs",
                    "glue:BatchGetJobs",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:GetJobBookmark",
                    "glue:GetJobBookmarks",
                    "glue:GetTrigger",
                    "glue:GetTriggers",
                    "glue:ListTriggers",
                    "glue:BatchGetTriggers",
                ],
            ),
            # instance-profile for ec2 instance in cluster
            ConstructPermission(
                [f"arn:aws:iam::{params[AWSCommonParams.ACCOUNT_ID]}:instance-profile/*IntelliFlowExeRole"],
                [
                    "iam:CreateInstanceProfile",
                    "iam:AddRoleToInstanceProfile",
                    "iam:DeleteInstanceProfile",
                    "iam:RemoveRoleFromInstanceProfile",
                ],
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

    def _build_bootstrapper_object_key(self) -> str:
        return build_object_key(["bootstrapper"], f"{self.__class__.__name__.lower()}_RuntimePlatform.data")

    def _update_bootstrapper(self, bootstrapper: "RuntimePlatform") -> None:
        # uploading it to S3 and passing S3 link as job arg.
        bootstrapped_platform = bootstrapper.serialize()

        bootstrapper_object_key = self._build_bootstrapper_object_key()
        exponential_retry(
            put_object, {"ServiceException", "TooManyRequestsException"}, self._bucket, bootstrapper_object_key, bootstrapped_platform
        )

    def activate(self) -> None:
        if not bucket_exists(self._s3, self._bucket_name):
            self._setup_scripts_bucket()
        else:
            self._bucket = get_bucket(self._s3, self._bucket_name)

        self._common_bootstrap_actions = []

        self._common_bundle_s3_paths = []

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
                    batch_script_file_key = self._build_boilerplate_code_path(abi, batch, lang, runtime_config)
                    exponential_retry(
                        put_object,
                        {"ServiceException", "TooManyRequestsException"},
                        self._bucket,
                        batch_script_file_key,
                        batch.generate_emr_script().encode("utf-8"),
                    )

        if_exe_role_name = self._get_if_exe_role_name()
        create_job_flow_instance_profile(self._iam, if_exe_role_name)

        super().activate()

    def _get_if_exe_role_name(self):
        return self._params[AWSCommonParams.IF_EXE_ROLE].split("/")[-1]

    @classmethod
    def _build_boilerplate_code_path(cls, abi, batch, lang, runtime_config):
        return build_object_key(
            ["batch", lang.name, abi.name, runtime_config.name], f"emretl_{batch.__class__.__name__.lower()}.{lang.extension}"
        )

    def rollback(self) -> None:
        super().rollback()

    def kill_session(self, active_compute_record: "RoutingTable.ComputeRecord") -> None:
        if active_compute_record.state.response_type != ComputeResponseType.SUCCESS:
            return
        state = cast(ComputeSuccessfulResponse, active_compute_record.state)
        cluster_id = state.session_desc.session_id
        response = terminate_emr_job_flow(self._emr, [cluster_id])
        if "Errors" in response and response["Errors"]:
            raise RuntimeError(
                f"An error occurred while trying to stop AWS EMR job run! " f"Error: {response['Errors']!r}"
            )  # Errors will contain job name and run id.

    def terminate(self) -> None:
        """Designed to be resilient against repetitive calls in case of retries in the high-level
        termination work-flow.
        """

        # 1- cancel all cluster started by IF
        unfinished_clusters = exponential_retry(
            list_emr_clusters,
            self.CLIENT_RETRYABLE_EXCEPTION_LIST,
            self._emr,
            ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"],
        )

        unfinished_clusters_started_by_app = map(
            lambda cluster: cluster["Id"],
            filter(lambda cluster: cluster["Name"].startswith(self._build_job_prefix()), unfinished_clusters),
        )

        # Chunk job_flow_ids because terminate_job_flow API supports up to 10 cluster id at a time.
        for chunk in chunk_iter(unfinished_clusters_started_by_app, 10):
            terminate_emr_job_flow(self._emr, chunk)

        # 2- delete bucket
        if self._bucket_name and exponential_retry(bucket_exists, [], self._s3, self._bucket_name):
            bucket = get_bucket(self._s3, self._bucket_name)
            exponential_retry(empty_bucket, [], bucket)
            exponential_retry(delete_bucket, [], bucket)
            self._bucket_name = None
            self._bucket = None

        # 3. delete instance profile
        delete_instance_profile(self._iam, self._get_if_exe_role_name())

        super().terminate()

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)

    def hook_internal(self, route: "Route") -> None:
        """Early stage check on a new route, so that we can fail fast before the whole activation."""
        super().hook_internal(route)
        # GlueVersion early validation

        for slot in route.slots:
            if slot.type == SlotType.ASYNC_BATCH_COMPUTE:
                code_metadata = slot.code_metadata
                lang = EmrJobLanguage.from_slot_lang(slot.code_lang)
                abi = slot.code_abi
                if code_metadata.code_type != SlotCodeType.EMBEDDED_SCRIPT:
                    raise NotImplementedError(f"Code script type {code_metadata.code_type!r} is not supported yet!")
                    # FUTURE / TODO when other code types are supported warn user about target_entity and target_method
                    #  will be inferred by the driver (e.g Scala quasiquotes based object/class name and method name extraction)
                if lang != EmrJobLanguage.PYTHON or abi != ABI.GLUE_EMBEDDED:
                    raise NotImplementedError(
                        f"Expect lang={EmrJobLanguage.PYTHON.name} and abi={ABI.GLUE_EMBEDDED.name}, "
                        f"({lang.name}, {abi.name}) is not supported yet"
                    )

                extra_params: Dict[str, Any] = dict(slot.extra_params)
                if RUNTIME_CONFIG_KEY in extra_params:
                    runtime_config = extra_params[RUNTIME_CONFIG_KEY]
                    if not isinstance(runtime_config, RuntimeConfig):
                        raise ValueError(
                            f"Expect RuntimeConfig to be a instance of RuntimeConfig Enum, "
                            f"found {runtime_config.__class__.__name__} instead"
                        )
                if INSTANCE_CONFIG_KEY in extra_params:
                    instance_config = extra_params[INSTANCE_CONFIG_KEY]
                    if not isinstance(instance_config, InstanceConfig):
                        raise ValueError(
                            f"Expect InstanceConfig to be a instance of InstanceConfig, "
                            f"found {instance_config.__class__.__name__} instead"
                        )

                instance_config = self._translate_instance_config(extra_params)
                if instance_config.instance_count <= 1:
                    raise ValueError("Expect instance_count > 1, " f"found: {instance_config!r}")

                runtime_config = self._translate_runtime_config(extra_params, route.link_node.signals)

                self._validate_spark_cli_args(extra_params.get(SPARK_CLI_ARGS))

                if APPLICATIONS in extra_params.keys():
                    applications = extra_params[APPLICATIONS]
                    if not isinstance(applications, Iterable) or isinstance(applications, str):
                        raise ValueError(f"Expecting an Iterable[str] for {APPLICATIONS} config, got {type(applications).__name__} instead")
                    for app in applications:
                        if not isinstance(app, str):
                            raise ValueError(
                                f"Expecting an Iterable[str] for {APPLICATIONS} config, but one of the elements is {type(app).__name__}"
                            )

                if EXTRA_JARS in extra_params.keys():
                    extra_jars = extra_params[EXTRA_JARS]
                    if not isinstance(extra_jars, Iterable) or isinstance(extra_jars, str):
                        raise ValueError(f"Expecting an Iterable[str] for {EXTRA_JARS} config, got {type(extra_jars).__name__} instead")
                    for jar_path in extra_jars:
                        if not isinstance(jar_path, str):
                            raise ValueError(
                                f"Expecting an Iterable[str] for {EXTRA_JARS} config, but one of the elements is {type(jar_path).__name__}"
                            )

                if SECURITY_CONFIGURATION in extra_params.keys():
                    security_config = extra_params[SECURITY_CONFIGURATION]
                    if not isinstance(security_config, str):
                        raise ValueError(f"Expecting a str for {SECURITY_CONFIGURATION}, got {type(security_config).__name__} instead")
                    max_name_length = 10280
                    if len(security_config) > max_name_length:
                        raise ValueError(f"Security configuration name is too long, exceeding maximum of {max_name_length}")

                if EMR_INSTANCES_SPECS in extra_params.keys():
                    instances_specs = extra_params[EMR_INSTANCES_SPECS]
                    if not isinstance(instances_specs, dict):
                        raise ValueError(f"Expecting a dict for {EMR_INSTANCES_SPECS}, got {type(instances_specs).__name__} instead")
                    for key in instances_specs.keys():
                        if key in RESERVED_INSTANCES_SPECS:
                            raise ValueError(f"'{key}' is a reserved key in {RESERVED_INSTANCES_SPECS}")

                if EMR_CONFIGURATIONS in extra_params.keys():
                    configurations = extra_params[EMR_CONFIGURATIONS]
                    if not isinstance(configurations, list):
                        raise ValueError(f"Expecting a list for {EMR_CONFIGURATIONS}, got {type(configurations).__name__} instead")

                if EMR_BOOTSTRAP_ACTIONS in extra_params.keys():
                    bootstrap_actions = extra_params[EMR_BOOTSTRAP_ACTIONS]
                    if not isinstance(bootstrap_actions, list):
                        raise ValueError(f"Expecting a list for {EMR_BOOTSTRAP_ACTIONS}, got {type(bootstrap_actions).__name__} instead")

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

    def _build_job_name(self, lang: EmrJobLanguage, abi: ABI, runtime_config: RuntimeConfig, uuid: str):
        return f"{self._build_job_prefix()}-{lang.extension}-{abi.name.lower()}" f"-{runtime_config.name}-{uuid}"

    def _build_job_prefix(self):
        return f"IntelliFlow-{self.unique_context_id}-{self.__class__.__name__}"

    def _setup_scripts_bucket(self):
        """Initial setup of storage bucket. Enforces policy for access from dev and exec roles."""
        try:
            self._bucket = create_bucket(self._s3, self._bucket_name, self._region)
        except ClientError as error:
            if error.response["Error"]["Code"] == "InvalidBucketName":
                msg = (
                    f"Platform context_id '{self.context_id}' is not valid!"
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

    def _translate_runtime_config(self, extra_params: Dict[str, Any], materialized_inputs: List[Signal]) -> RuntimeConfig:
        # validated in hook_internal
        if RUNTIME_CONFIG_KEY in extra_params:
            runtime_config = extra_params[RUNTIME_CONFIG_KEY]
            if runtime_config in [RuntimeConfig.AUTO, RuntimeConfig.AUTO.value]:
                return RuntimeConfig.EMR_6_10_0
            return extra_params[RUNTIME_CONFIG_KEY]
        elif extra_params.get("GlueVersion", GlueVersion.AUTO.value) in [GlueVersion.AUTO, GlueVersion.AUTO.value]:
            return RuntimeConfig.EMR_6_10_0
        return RuntimeConfig.from_glue_version(self._resolve_glue_version(extra_params, materialized_inputs))

    def _translate_instance_config(self, extra_params: Dict[str, Any]):
        # validated in hook_internal
        if INSTANCE_CONFIG_KEY in extra_params:
            return extra_params[INSTANCE_CONFIG_KEY]
        instance_count = extra_params.get("NumberOfWorkers", self.DEFAULT_INSTANCE_CONFIG.instance_count)
        instance_type = self.DEFAULT_INSTANCE_CONFIG.instance_type
        if "WorkerType" in extra_params:
            instance_type = translate_glue_worker_type(extra_params["WorkerType"])
        return InstanceConfig(instance_count, instance_type)

    def _resolve_glue_version(self, extra_params: Dict[str, Any], materialized_inputs: List[Signal]) -> GlueVersion:
        glue_version = extra_params.get("GlueVersion", GlueVersion.AUTO.value)
        if glue_version in [
            GlueVersion.AUTO,
            GlueVersion.AUTO.value,
        ]:
            glue_version = self.GLUE_DEFAULT_VERSION

        if isinstance(glue_version, GlueVersion):
            return glue_version
        # formalize to enum
        for e in GlueVersion:
            if e == glue_version:
                return e

        raise ValueError(f"Unknown glue version: {glue_version}")

    @classmethod
    def _validate_spark_cli_args(cls, spark_cli_args: Optional[List[str]]):
        if not spark_cli_args:
            return

        if not isinstance(spark_cli_args, list):
            raise ValueError(f"Expect {SPARK_CLI_ARGS} to be a list")

        for idx, arg in enumerate(spark_cli_args):
            if not isinstance(arg, str):
                raise ValueError(f"Invalid spark cli args: {arg!r}, it's not a str")
            if arg in RESERVED_SPARK_CLI_ARGS:
                raise ValueError(f"Invalid spark cli args: {arg}, it's a reserved spark cli argument name")
            if not arg.startswith("--") and (idx == 0 or not spark_cli_args[idx - 1].startswith("--")):
                raise ValueError(f"Invalid spark cli args: {arg}, it doesn't start with '--' and not following a key that starts with '--'")

    @property
    def context_id(self) -> str:
        return self.get_platform().context_id

    @property
    def unique_context_id(self) -> str:
        return self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT]
