# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import uuid
from typing import Any, ClassVar, Dict, List, Optional, Set, Tuple, Type, cast

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
from ...definitions.aws.glue import catalog as glue_catalog
from ...definitions.aws.glue.catalog import check_table, query_table_spec
from ...definitions.aws.glue.client_wrapper import GlueVersion
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
        return []

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        # managed policy provided via provide_runtime_default_policies will not cover the requirements of exec role
        # used on EC2 instances.
        # Use the following guide to decorate exec-role:
        # TODO
        #  https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role-for-ec2.html
        # - should be similar to Glue drivers runtime permissions.
        # - what AWS suggests here is very well aligned with how IF drivers are expected to provide these permissions
        return []

    @classmethod
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        return []

    def _provide_system_metrics(self) -> List[Signal]:
        return []

    def _provide_internal_metrics(self) -> List[ConstructInternalMetricDesc]:
        return []

    def _provide_route_metrics(self, route: Route) -> List[ConstructInternalMetricDesc]:
        return []

    def _provide_internal_alarms(self) -> List[Signal]:
        return []

    def activate(self) -> None:
        super().activate()

    def rollback(self) -> None:
        super().rollback()

    def terminate(self) -> None:
        super().terminate()

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

    def _revert_security_conf(selfs, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        pass
