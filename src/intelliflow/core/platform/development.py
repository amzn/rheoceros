# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
import logging
from abc import abstractmethod
from enum import Enum
from typing import Any, ClassVar, Dict, List, Optional, Sequence, Set, Type

from intelliflow.core.deployment import is_on_remote_dev_env
from intelliflow.core.platform.definitions.aws.sagemaker import notebook
from intelliflow.core.platform.definitions.aws.sagemaker.common import SAGEMAKER_NOTEBOOK_INSTANCE_ASSUMED_ROLE_USER
from intelliflow.core.platform.endpoint import DevEndpoint

from ..serialization import DeserializationError, Serializable, SerializationError
from ..signal_processing.routing_runtime_constructs import Route
from ..signal_processing.signal import Signal
from ..signal_processing.signal_source import SignalSourceType
from .constructs import (
    BaseConstruct,
    BatchCompute,
    CompositeBatchCompute,
    CompositeExtension,
    ConstructParamsDict,
    ConstructPermission,
    ConstructPermissionGroup,
    ConstructSecurityConf,
    Diagnostics,
    Extension,
    ProcessingUnit,
    ProcessorQueue,
    RoutingTable,
    Storage,
    UpstreamGrants,
    UpstreamRevokes,
)
from .definitions.aws.common import (
    AWS_ASSUMED_ROLE_ARN_ROOT,
    AWS_MAX_ROLE_NAME_SIZE,
    IF_DEV_POLICY_NAME_FORMAT,
    IF_DEV_REMOVED_DRIVERS_TEMP_POLICY_NAME_FORMAT,
    IF_DEV_ROLE_FORMAT,
    IF_DEV_ROLE_NAME_FORMAT,
    IF_EXE_POLICY_NAME_FORMAT,
    IF_EXE_ROLE_FORMAT,
    IF_EXE_ROLE_NAME_FORMAT,
    MAX_CHAINED_ROLE_DURATION,
    AWSAccessPair,
)
from .definitions.aws.common import CommonParams as AWSCommonParams
from .definitions.aws.common import (
    attach_aws_managed_policy,
    create_role,
    delete_inlined_policy,
    delete_role,
    exponential_retry,
    get_aws_account_id,
    get_caller_identity,
    get_session,
    get_session_with_account,
    has_role,
    is_assumed_role_session,
    is_in_role_trust_policy,
    put_inlined_policy,
    update_role,
    update_role_trust_policy,
)
from .definitions.aws.refreshable_session import RefreshableBotoSession
from .definitions.common import ActivationParams
from .drivers.compute.aws import AWSGlueBatchComputeBasic
from .drivers.compute.aws_athena import AWSAthenaBatchCompute
from .drivers.compute.aws_emr import AWSEMRBatchCompute
from .drivers.compute.aws_sagemaker.training_job import AWSSagemakerTrainingJobBatchCompute
from .drivers.compute.aws_sagemaker.transform_job import AWSSagemakerTransformJobBatchCompute
from .drivers.compute.local import LocalSparkBatchComputeImpl, LocalSparkIPCBatchComputeImpl
from .drivers.diagnostics.aws import AWSCloudWatchDiagnostics
from .drivers.diagnostics.local import LocalDiagnosticsImpl
from .drivers.processor.aws import AWSLambdaProcessorBasic
from .drivers.processor.local import LocalProcessorImpl
from .drivers.processor_queue.aws import AWSProcessorQueueBasic
from .drivers.processor_queue.local import LocalProcessorQueueImpl
from .drivers.routing.aws import AWSDDBRoutingTable
from .drivers.routing.local import LocalRoutingTableImpl
from .drivers.storage.aws import AWSS3StorageBasic
from .drivers.storage.local import LocalStorageImpl
from .platform import Platform

module_logger = logging.getLogger(__file__)


class RuntimePlatform(Platform):
    def __init__(
        self,
        context_id: str,
        storage: Storage,
        processor: ProcessingUnit,
        processor_queue: ProcessorQueue,
        batch_compute: BatchCompute,
        routing_table: RoutingTable,
        diagnostics: Diagnostics,
        extensions: CompositeExtension,
    ) -> None:
        self._context_id = context_id
        self._storage = storage
        self._processor = processor
        self._processor_queue = processor_queue
        self._batch_compute = batch_compute
        self._routing_table = routing_table
        self._diagnostics = diagnostics
        self._extensions = extensions

        self._serialization_cache = None

    def _serializable_copy_init(self, org_instance: "RuntimePlatform") -> None:
        super()._serializable_copy_init(org_instance)

        original_to_serialized_map: Dict[BaseConstruct, BaseConstruct] = dict()
        original_to_serialized_map[org_instance.storage] = self._storage
        original_to_serialized_map[org_instance._processor] = self._processor
        original_to_serialized_map[org_instance._processor_queue] = self._processor_queue
        original_to_serialized_map[org_instance._batch_compute] = self._batch_compute
        original_to_serialized_map[org_instance._routing_table] = self._routing_table
        original_to_serialized_map[org_instance._diagnostics] = self._diagnostics
        original_to_serialized_map[org_instance._extensions] = self._extensions

        self._storage.update_serialized_cross_refs(original_to_serialized_map)
        self._processor.update_serialized_cross_refs(original_to_serialized_map)
        self._processor_queue.update_serialized_cross_refs(original_to_serialized_map)
        self._batch_compute.update_serialized_cross_refs(original_to_serialized_map)
        self._routing_table.update_serialized_cross_refs(original_to_serialized_map)
        self._diagnostics.update_serialized_cross_refs(original_to_serialized_map)
        self._extensions.update_serialized_cross_refs(original_to_serialized_map)

    def runtime_init(self, context_owner: BaseConstruct) -> None:
        self._storage.runtime_init(self, context_owner)
        self._processor.runtime_init(self, context_owner)
        self._processor_queue.runtime_init(self, context_owner)
        self._batch_compute.runtime_init(self, context_owner)
        self._routing_table.runtime_init(self, context_owner)
        self._diagnostics.runtime_init(self, context_owner)
        self._extensions.runtime_init(self, context_owner)

    # overrides
    def serialize(self) -> str:
        if not self._serialization_cache:
            self._serialization_cache = super().serialize(True)
        return self._serialization_cache


class Configuration(Serializable):
    """A Class to provide consistency in the utilization of drivers in the context of
    a particular `DevelopmentPlatform` impl (and of course its logical configuration).

    Subclasses should provide the information of valid construct impls (for each construct
    category) and also a default assignment for them.

    Different `DevelopmentPlatform` impls using the same configuration should be compatible.

    It also sets up the fluent API for subclasses.
    """

    class _Builder:
        def __init__(self, conf_class: Type["Configuration"]) -> None:
            self._new_conf: Configuration = conf_class()

        def _with(self, impl: Type[BaseConstruct], category: Type[BaseConstruct]) -> "_Builder":
            supported_impls = self._new_conf.get_supported_constructs_map()[category]
            if impl not in supported_impls:
                raise ValueError(
                    f"{impl} is not supported by this configuration {self._new_conf.__class__.__name__} as a {category}."
                    f" Supported constructs list: {supported_impls}."
                )

            self._new_conf._active_construct_map[category] = impl
            return self

        def with_processor(self, processor: Type[ProcessingUnit]) -> "_Builder":
            return self._with(processor, ProcessingUnit)

        def with_processor_input_queue(self, processor_queue: Type[ProcessorQueue]) -> "_Builder":
            return self._with(processor_queue, ProcessorQueue)

        def with_batch_compute(self, batch_compute: Type[BatchCompute]) -> "_Builder":
            return self._with(batch_compute, BatchCompute)

        def with_routing_table(self, routing_table: Type[RoutingTable]) -> "_Builder":
            return self._with(routing_table, RoutingTable)

        def with_diagnostics(self, diagnostics: Type[Diagnostics]) -> "_Builder":
            return self._with(diagnostics, Diagnostics)

        def with_extensions(self, extensions: List[Extension.Descriptor]):
            return self.with_param(CompositeExtension.EXTENSIONS_PARAM, extensions)

        def with_param(self, key: str, value: Any) -> "_Builder":
            self._new_conf.add_param(key, value)
            return self

        def build(self) -> "Configuration":
            return self._new_conf

    @classmethod
    def builder(cls) -> _Builder:
        return Configuration._Builder(cls)

    def __init__(
        self,
        storage: Type[Storage],
        default_processor: Type[ProcessingUnit],
        default_processor_queue: Type[ProcessorQueue],
        default_batch_compute: Type[BatchCompute],
        default_routing_table: Type[RoutingTable],
        default_diagnostics: Type[Diagnostics],
        default_extensions: Type[CompositeExtension],
    ) -> None:
        self._active_construct_map: Dict[Type[BaseConstruct], Type[BaseConstruct]] = dict()
        self._active_construct_map[Storage] = storage
        self._active_construct_map[ProcessingUnit] = default_processor
        self._active_construct_map[ProcessorQueue] = default_processor_queue
        self._active_construct_map[BatchCompute] = default_batch_compute
        self._active_construct_map[RoutingTable] = default_routing_table
        self._active_construct_map[Diagnostics] = default_diagnostics
        self._active_construct_map[CompositeExtension] = default_extensions

        self._activated_construct_instance_map: Dict[Type[BaseConstruct], BaseConstruct] = dict()

        self._params: ConstructParamsDict = dict()
        self._is_upstream = False
        self._is_downstream = False
        self._context_id: Optional[str] = None

    # overrides
    def _serializable_copy_init(self, org_instance: "Configuration") -> None:
        super()._serializable_copy_init(org_instance)

        original_to_serialized_map: Dict[BaseConstruct, BaseConstruct] = dict()
        # constructs are assumed to be not safe, we have to get their safe clones.
        self._activated_construct_instance_map: Dict[Type[BaseConstruct], BaseConstruct] = dict()
        for const_type, const in org_instance._activated_construct_instance_map.items():
            serialized_const: BaseConstruct = const.serializable_copy()
            original_to_serialized_map[const] = serialized_const
            self._activated_construct_instance_map[const_type] = serialized_const

        # now let constructs to update their cross-refs (if any)
        for const_ins in self._activated_construct_instance_map.values():
            const_ins.update_serialized_cross_refs(original_to_serialized_map)

    @property
    def context_id(self) -> str:
        return self._context_id

    def _set_context(self, id: str) -> None:
        self._context_id = id

    def set_upstream(self, host_conf: "Configuration") -> None:
        if self.__class__ != host_conf.__class__:
            raise ValueError(
                f"Cannot create a remote configuration {self.__class__.__name__} "
                f"from an incompatible host configuration {host_conf.__class__.__name__}"
            )
        self._is_upstream = True
        self.add_param(ActivationParams.UNIQUE_ID_FOR_HOST_CONTEXT, host_conf._generate_unique_id_for_context())

    def set_downstream(self, host_conf: "Configuration", context_id: str) -> None:
        if self.__class__ != host_conf.__class__:
            raise ValueError(
                f"Cannot create a downstream configuration {self.__class__.__name__} "
                f"from an incompatible host configuration {host_conf.__class__.__name__}"
            )
        self._is_downstream = True
        self._context_id = context_id
        self.add_param(ActivationParams.UNIQUE_ID_FOR_CONTEXT, self._generate_unique_id_for_context())

    def authorize(self, downstream_conf: Sequence["Configuration"]) -> None:
        pass

    def unauthorize(self, downstream_conf: Sequence["Configuration"]) -> None:
        pass

    def process_downstream_connection(self, downstream_platform: "DevelopmentPlatform") -> UpstreamGrants:
        """Called by downstream platform to establish/update the connection"""
        return UpstreamGrants(set())

    def terminate_downstream_connection(self, downstream_platform: "DevelopmentPlatform") -> UpstreamRevokes:
        """Called by downstream platform to request the termination of the connection"""
        return UpstreamRevokes(set())

    @abstractmethod
    def process_upstream_grants(
        self, upstream_grants: UpstreamGrants, unique_context_id: str, remote_platform: "DevelopmentPlatform"
    ) -> None:
        pass

    @abstractmethod
    def process_upstream_revokes(
        self, upstream_revokes: UpstreamRevokes, unique_context_id: str, remote_platform: "DevelopmentPlatform"
    ) -> None:
        pass

    def get_param(self, key: str) -> Any:
        return self._params[key]

    def add_param(self, key: str, value: Any) -> None:
        self._params[key] = value

    def remove_param(self, key: str) -> None:
        if key in self._params:
            del self._params[key]

    def dev_init(self, context_id: str):
        if self._is_downstream:
            raise TypeError(f"A downstream configuration cannot be initiated for development!")
        self._context_id = context_id
        self.add_param(ActivationParams.CONTEXT_ID, context_id)
        self._add_common_params()
        self.add_param(ActivationParams.UNIQUE_ID_FOR_CONTEXT, self._generate_unique_id_for_context())

    def activate(self) -> None:
        if self._is_upstream or self._is_downstream:
            raise TypeError(f"Downstream/upstream configuration (context_id={self._context_id}) cannot be activated remotely!")
        self._setup_runtime_params()

    def terminate(self) -> None:
        if self._is_upstream or self._is_downstream:
            raise TypeError(f"Downstream/upstream configuration (context_id={self._context_id}) cannot be terminated remotely!")
        self._clean_runtime_params()

    def delete(self) -> None:
        if self._is_upstream or self._is_downstream:
            raise TypeError(f"Downstream/upstream configuration (context_id={self._context_id}) cannot be deleted remotely!")
        self._clean_common_params()
        self.remove_param(ActivationParams.UNIQUE_ID_FOR_CONTEXT)
        self._context_id = None

    @abstractmethod
    def _add_common_params(self) -> None:
        pass

    @abstractmethod
    def _clean_common_params(self) -> None:
        pass

    def add_permissions_for_removed_drivers(self, removed_drivers: Set[BaseConstruct]) -> None:
        pass

    def clean_permissions_for_removed_drivers(self, removed_drivers: Set[BaseConstruct]) -> None:
        pass

    @abstractmethod
    def _setup_runtime_params(self) -> None:
        pass

    @abstractmethod
    def _clean_runtime_params(self) -> None:
        pass

    @abstractmethod
    def _generate_unique_id_for_context(self) -> str:
        """Generate the universally unique_id for the context (i.e app) using the params from application
        and also from the internals (common params) of this Configuration.

        So this ID should uniquely identify an app among all of the other apps using the same Platform/Configuration.
        """
        pass

    @classmethod
    @abstractmethod
    def get_supported_constructs_map(cls) -> Dict[Type[BaseConstruct], Set[Type[BaseConstruct]]]:
        ...

    def get_active_construct_type(self, construct_type: Type[BaseConstruct]) -> Type[BaseConstruct]:
        return self._active_construct_map[construct_type]

    def get_active_construct(self, construct_type: Type[BaseConstruct]) -> BaseConstruct:
        return self._activated_construct_instance_map[construct_type]

    def restore_active_construct(self, construct_type: Type[BaseConstruct], persisted_construct: BaseConstruct) -> None:
        persisted_construct._deserialized_init(self._params)
        self._activated_construct_instance_map[construct_type] = persisted_construct

    def provide_construct(self, construct_type: Type[BaseConstruct]) -> BaseConstruct:
        construct_instance: BaseConstruct = self.get_active_construct_type(construct_type)(self._params)
        self._activated_construct_instance_map[construct_type] = construct_instance
        return construct_instance

    def provide_storage(self) -> Storage:
        return self.provide_construct(Storage)

    def provide_processor(self) -> ProcessingUnit:
        return self.provide_construct(ProcessingUnit)

    def provide_processor_queue(self) -> ProcessorQueue:
        return self.provide_construct(ProcessorQueue)

    def provide_batch_compute(self) -> BatchCompute:
        return self.provide_construct(BatchCompute)

    def provide_routing_table(self) -> RoutingTable:
        return self.provide_construct(RoutingTable)

    def provide_diagnostics(self) -> Diagnostics:
        return self.provide_construct(Diagnostics)

    def provide_extensions(self) -> CompositeExtension:
        return self.provide_construct(CompositeExtension)

    def provide_runtime_trusted_entities(self) -> List[str]:
        return [
            trusted_entity
            for cons in self._activated_construct_instance_map.values()
            for trusted_entity in cons.provide_runtime_trusted_entities()
            if trusted_entity
        ]

    def provide_runtime_default_policies(self) -> Set[str]:
        return {
            policy
            for cons in self._activated_construct_instance_map.values()
            for policy in cons.provide_runtime_default_policies()
            if policy
        }

    def provide_runtime_construct_permissions(self) -> List[ConstructPermission]:
        return [
            construct_permission
            for cons in self._activated_construct_instance_map.values()
            for construct_permission in cons.provide_runtime_permissions()
            if construct_permission
        ]

    def provide_devtime_construct_permissions(self) -> List[ConstructPermission]:
        return [
            dev_time_permission
            for cons_type in self._active_construct_map.values()
            for dev_time_permission in cons_type.provide_devtime_permissions(self._params)
            if dev_time_permission
        ]

    @abstractmethod
    def provision_remote_dev_env(self, bundle_uri: str, endpoint_attrs: Dict[str, Any]) -> DevEndpoint:
        """Provisions and returns the URIs for remote dev endpoint"""
        pass

    @abstractmethod
    def deprovision_remote_dev_env(self) -> bool:
        """Check and delete the existing remote dev env.

        :returns : True if the deletion happens.
        """
        pass

    @abstractmethod
    def get_remote_dev_env(self) -> DevEndpoint:
        pass

    @abstractmethod
    def is_on_remote_dev_env(self) -> bool:
        pass


class LocalConfiguration(Configuration):
    """Default Local platform configuration

    Default constructs can be overwritten based on the supported constructs map
    (see :meth:`get_supported_constructs_map`).
    """

    def __init__(self, root_dir: Optional[str] = None) -> None:
        super().__init__(
            LocalStorageImpl,
            LocalProcessorImpl,
            LocalProcessorQueueImpl,
            LocalSparkBatchComputeImpl,
            LocalRoutingTableImpl,
            LocalDiagnosticsImpl,
            CompositeExtension,
        )

    def _generate_unique_id_for_context(self) -> str:
        pass

    # override
    def _add_common_params(self) -> None:
        # add common params to be used by local constructs into self._params
        pass

    def _clean_common_params(self) -> None:
        pass

    # override
    def _setup_runtime_params(self) -> None:
        # setup common params to be used by local constructs during activtion
        # into self._params
        pass

    # override
    def _clean_runtime_params(self) -> None:
        pass

    # overrides
    def process_upstream_grants(
        self, upstream_grants: UpstreamGrants, unique_context_id: str, remote_platform: "DevelopmentPlatform"
    ) -> None:
        pass

    # overrides
    def process_upstream_revokes(
        self, upstream_revokes: UpstreamRevokes, unique_context_id: str, remote_platform: "DevelopmentPlatform"
    ) -> None:
        pass

    # overrides
    @classmethod
    def get_supported_constructs_map(cls) -> Dict[Type[BaseConstruct], Set[Type[BaseConstruct]]]:
        return {
            ProcessingUnit: {LocalProcessorImpl},
            ProcessorQueue: {LocalProcessorQueueImpl},
            BatchCompute: {LocalSparkBatchComputeImpl, LocalSparkIPCBatchComputeImpl},
            RoutingTable: {LocalRoutingTableImpl},
            Diagnostics: {LocalDiagnosticsImpl},
            CompositeExtension: {CompositeExtension},
        }

    # overrides
    def _serializable_copy_init(self, org_instance: "LocalConfiguration") -> None:
        super()._serializable_copy_init(org_instance)

    # overrides
    def provision_remote_dev_env(self, bundle_uri: str, endpoint_attrs: Dict[str, Any]) -> DevEndpoint:
        """Provisions and returns the URI or local file path for a dev-env outside of this host-env
        Ex: local Jupyter notebook, etc
        """
        pass

    # overrides
    def deprovision_remote_dev_env(self):
        pass

    # overrides
    def get_remote_dev_env(self) -> DevEndpoint:
        pass

    # overrides
    def is_on_remote_dev_env(self) -> bool:
        return False


class AWSConfiguration(Configuration):
    """Default AWS platform configuration.

    Default constructs can be overwritten based on the supported constructs map
    (see :meth:`get_supported_constructs_map`).
    """

    class _Builder(Configuration._Builder):
        def __init__(self, conf_class: Type["Configuration"]) -> None:
            super().__init__(conf_class)

        def with_region(self, region: str) -> "_Builder":
            self._new_conf.add_param(AWSCommonParams.REGION, region)
            return self

        def with_account_id(self, account_id: str) -> "_Builder":
            self._new_conf.add_param(AWSCommonParams.ACCOUNT_ID, account_id)
            self._new_conf.add_param(AWSCommonParams.AUTHENTICATE_WITH_ACCOUNT_ID, True)
            return self

        def with_dev_role_credentials(self, account_id: str, auto_update_dev_role: bool = True) -> "_Builder":
            self._new_conf.add_param(AWSCommonParams.ACCOUNT_ID, account_id)
            self._new_conf.add_param(AWSCommonParams.AUTHENTICATE_WITH_DEV_ROLE, True)
            self._new_conf.add_param(AWSCommonParams.AUTO_UPDATE_DEV_ROLE, auto_update_dev_role)
            return self

        def with_dev_access_pair(self, aws_access_key_id: str, aws_secret_access_key: str) -> "_Builder":
            self._new_conf.add_param(AWSCommonParams.IF_ADMIN_ACCESS_PAIR, AWSAccessPair(aws_access_key_id, aws_secret_access_key))
            return self

        def with_default_credentials(self, as_admin: bool = False):
            self._new_conf.add_param(AWSCommonParams.DEFAULT_CREDENTIALS_AS_ADMIN, as_admin)
            self._new_conf.add_param(AWSCommonParams.AUTO_UPDATE_DEV_ROLE, as_admin)
            return self

        def with_admin_role(self, role: str, auto_update_dev_role: bool = True) -> "_Builder":
            self._new_conf.add_param(AWSCommonParams.IF_ADMIN_ROLE, role)
            self._new_conf.add_param(AWSCommonParams.AUTO_UPDATE_DEV_ROLE, auto_update_dev_role)
            return self

        def with_admin_access_pair(
            self, aws_access_key_id: str, aws_secret_access_key: str, auto_update_dev_role: bool = True
        ) -> "_Builder":
            self._new_conf.add_param(AWSCommonParams.IF_ADMIN_ACCESS_PAIR, AWSAccessPair(aws_access_key_id, aws_secret_access_key))
            self._new_conf.add_param(AWSCommonParams.AUTO_UPDATE_DEV_ROLE, auto_update_dev_role)
            return self

    @classmethod
    def builder(cls) -> _Builder:
        return AWSConfiguration._Builder(cls)

    def __init__(self) -> None:
        super().__init__(
            AWSS3StorageBasic,
            AWSLambdaProcessorBasic,
            AWSProcessorQueueBasic,
            CompositeBatchCompute,
            AWSDDBRoutingTable,
            AWSCloudWatchDiagnostics,
            CompositeExtension,
        )
        # Default BatchCompute driver priority list:
        #   - it determines which computes will be implicitly supported by the platform,
        #   - the order here determines which driver to choose when there is an overlap for a Slot,
        #     e.g PrestoSQL supported by Athena and EMR.
        # please note that users are still able to change the BatchCompute driver type or
        # again for CompositeBatchCompute this piority list via
        #    AWSConfiguration.builder().with_param(CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM, [...])
        self.add_param(
            CompositeBatchCompute.BATCH_COMPUTE_DRIVERS_PARAM,
            [
                AWSAthenaBatchCompute,
                AWSGlueBatchComputeBasic,
                AWSEMRBatchCompute,
                AWSSagemakerTrainingJobBatchCompute,
                AWSSagemakerTransformJobBatchCompute,
            ],
        )

    # overrides
    def _serializable_copy_init(self, org_instance: "AWSConfiguration") -> None:
        super()._serializable_copy_init(org_instance)
        # we are about to modify _params, we have to create a new shallow copy of it.
        self._params = copy.copy(org_instance._params)
        self.remove_param(AWSCommonParams.BOTO_SESSION)
        # self.remove_param(AWSCommonParams.ASSUMED_ROLE_USER)
        self.remove_param(AWSCommonParams.IF_ADMIN_ACCESS_PAIR)
        self.remove_param(AWSCommonParams.IF_ACCESS_PAIR)

    def _generate_unique_id_for_context(self) -> str:
        """Universally unique representation of an IF application using this configuration"""
        return f"{self._context_id}_{self._params[AWSCommonParams.ACCOUNT_ID]}_{self._params[AWSCommonParams.REGION]}"

    # overrides
    def set_upstream(self, host_conf: "Configuration") -> None:
        super().set_upstream(host_conf)
        if AWSCommonParams.ACCOUNT_ID not in self._params:
            raise ValueError(
                f"Please create the remote configuration with AWS Account ID. "
                f" Ex: AWSConfiguration.builder().with_account_id('111222333444')"
            )
        if AWSCommonParams.REGION not in self._params:
            raise ValueError(
                f"Please create the remote configuration with AWS Region. " f" Ex: AWSConfiguration.builder().with_region('us-east-1')"
            )
        self.add_param(AWSCommonParams.AUTO_UPDATE_DEV_ROLE, False)

        self.add_param(AWSCommonParams.HOST_BOTO_SESSION, host_conf._params[AWSCommonParams.BOTO_SESSION])

    def set_downstream(self, host_conf: "Configuration", context_id: str) -> None:
        super().set_downstream(host_conf, context_id)
        if AWSCommonParams.ACCOUNT_ID not in self._params:
            raise ValueError(
                f"Please create the remote configuration with AWS Account ID. "
                f" Ex: AWSConfiguration.builder().with_account_id('111222333444')"
            )
        if AWSCommonParams.REGION not in self._params:
            raise ValueError(
                f"Please create the remote configuration with AWS Region. " f" Ex: AWSConfiguration.builder().with_region('us-east-1')"
            )

        if AWSCommonParams.IF_DEV_ROLE not in self._params:
            self._params[AWSCommonParams.IF_DEV_ROLE] = IF_DEV_ROLE_FORMAT.format(
                self._params[AWSCommonParams.ACCOUNT_ID], self._context_id, self._params[AWSCommonParams.REGION]
            )

    # overrides
    def authorize(self, downstream_confs: Sequence["Configuration"]) -> None:
        if downstream_confs:
            session = self._params[AWSCommonParams.BOTO_SESSION]
            if_dev_role_name: str = self._build_dev_role_name()

            downstream_dev_roles = {downstream_conf.get_param(AWSCommonParams.IF_DEV_ROLE) for downstream_conf in downstream_confs}

            # let them assume our dev role
            exponential_retry(
                update_role_trust_policy,
                ["MalformedPolicyDocument", "MalformedPolicyDocumentException"],
                if_dev_role_name,
                session,
                self._params[AWSCommonParams.REGION],
                downstream_dev_roles,
            )

    # overrides
    def unauthorize(self, downstream_confs: Sequence["Configuration"]) -> None:
        if downstream_confs:
            session = self._params[AWSCommonParams.BOTO_SESSION]
            if_dev_role_name: str = self._build_dev_role_name()

            downstream_dev_roles = {downstream_conf.get_param(AWSCommonParams.IF_DEV_ROLE) for downstream_conf in downstream_confs}

            # let them assume our dev role
            exponential_retry(
                update_role_trust_policy,
                ["MalformedPolicyDocument"],
                if_dev_role_name,
                session,
                self._params[AWSCommonParams.REGION],
                set(),
                downstream_dev_roles,
            )

    def _build_dev_role(self) -> str:
        return IF_DEV_ROLE_FORMAT.format(self._params[AWSCommonParams.ACCOUNT_ID], self._context_id, self._params[AWSCommonParams.REGION])

    def _build_dev_role_name(self) -> str:
        return IF_DEV_ROLE_NAME_FORMAT.format(self._context_id, self._params[AWSCommonParams.REGION])

    def _build_exe_role(self) -> str:
        return IF_EXE_ROLE_FORMAT.format(self._params[AWSCommonParams.ACCOUNT_ID], self._context_id, self._params[AWSCommonParams.REGION])

    def _build_exe_role_name(self) -> str:
        return IF_EXE_ROLE_NAME_FORMAT.format(self._context_id, self._params[AWSCommonParams.REGION])

    # override
    def _add_common_params(self) -> None:
        # add common params to be used by local constructs into self._params

        if_dev_role_name: str = self._build_dev_role_name()
        session_duration = None

        needs_to_assume_role, needs_to_check_role, session = self._setup_session(if_dev_role_name)

        if AWSCommonParams.ACCOUNT_ID not in self._params:
            self._params[AWSCommonParams.ACCOUNT_ID] = get_aws_account_id(session, self._params[AWSCommonParams.REGION])

        if AWSCommonParams.IF_DEV_ROLE not in self._params:
            self._params[AWSCommonParams.IF_DEV_ROLE] = self._build_dev_role()

        if AWSCommonParams.IF_EXE_ROLE not in self._params:
            self._params[AWSCommonParams.IF_EXE_ROLE] = self._build_exe_role()

        # just make sure that this is updated early before DEV-ROLE is setup (this conf relies on acc ID so, at this
        # point it is guaranteed to have it independent of how the conf/session was initiated). drivers might need this
        # for role permissions, etc.
        self._params[ActivationParams.UNIQUE_ID_FOR_CONTEXT] = self._generate_unique_id_for_context()

        # now make sure that the current session does not belong to the dev-role already
        if not self._is_upstream:
            is_assumed_dev_role = False
            caller_identity: str = get_caller_identity(session, self._params[AWSCommonParams.REGION])
            if is_assumed_role_session(caller_identity):
                module_logger.info(
                    f"Session belongs to an assumed role: {caller_identity}. "
                    f"This will determine RheocerOS dev role chaining or session duration."
                )
                session_duration = MAX_CHAINED_ROLE_DURATION
                self._params[AWSCommonParams.ASSUMED_ROLE_USER] = caller_identity.rsplit("/", 1)[-1]
                if caller_identity.startswith(AWS_ASSUMED_ROLE_ARN_ROOT.format(self._params[AWSCommonParams.ACCOUNT_ID], if_dev_role_name)):
                    is_assumed_dev_role = True
            elif AWSCommonParams.ASSUMED_ROLE_USER in self._params:
                del self._params[AWSCommonParams.ASSUMED_ROLE_USER]

            if caller_identity == self._params[AWSCommonParams.IF_DEV_ROLE] or is_assumed_dev_role:
                needs_to_check_role = False
                needs_to_assume_role = False
                module_logger.info(
                    f"Skipping update on the development role,"
                    f" since the caller identity is already {self._params[AWSCommonParams.IF_DEV_ROLE]}"
                )

        if needs_to_check_role or needs_to_assume_role:
            if_dev_role: str = self._params[AWSCommonParams.IF_DEV_ROLE]

            update_dev_role: bool = self._params.get(AWSCommonParams.AUTO_UPDATE_DEV_ROLE, False)
            if needs_to_check_role:
                new_role: bool = False
                if not has_role(if_dev_role_name, session):
                    assert len(if_dev_role_name) <= AWS_MAX_ROLE_NAME_SIZE, (
                        f"Cannot create dev role with long context ID {self._context_id!r}."
                        f"Try to shorten it by {len(self._context_id) - 64}"
                    )
                    new_role = True
                    # initially no "trusted entities" (except the caller entity and AWS services with dev endpoints)
                    create_role(
                        if_dev_role_name,
                        session,
                        self._params[AWSCommonParams.REGION],
                        True,
                        ["sagemaker.amazonaws.com", "glue.amazonaws.com"],
                    )
                    exponential_retry(
                        attach_aws_managed_policy, ["ServiceFailureException"], if_dev_role_name, "AmazonSageMakerFullAccess", session
                    )
                else:
                    # TODO support restoration of trusted-services as well (e.g. if role's trust policy is not manually restored after Conduit wipes it out).
                    # so basically support 'services' in 'update_role_trust_policy' (add 'sagemaker.amazonaws.com').
                    update_role_trust_policy(if_dev_role_name, session, self._params[AWSCommonParams.REGION], set(), set(), True)

                if update_dev_role or new_role:
                    # dynamically adapt the policy to the current configuration
                    # baseline policy for dev-mode (independent of construct configuration_
                    # (1) make sure that dev role can create/update exec role
                    # (2) make sure that dev role can add/update policy fir exec role
                    devrole_permissions = self.provide_devtime_construct_permissions() + [
                        ConstructPermission(
                            [self._params[AWSCommonParams.IF_EXE_ROLE]],
                            [
                                "iam:CreateRole",
                                "iam:DeleteRole",
                                "iam:GetRole",
                                "iam:PassRole",
                                "iam:PutRolePolicy",
                                "iam:GetRolePolicy",
                                "iam:DeleteRolePolicy",
                                "iam:ListRolePolicies",
                                "iam:ListAttachedRolePolicies",
                                "iam:UpdateAssumeRolePolicy",
                                "iam:AttachRolePolicy",
                                "iam:DetachRolePolicy",
                            ],
                        ),
                        # should be able to get itself, and update its own policies.
                        ConstructPermission(
                            [self._params[AWSCommonParams.IF_DEV_ROLE]],
                            ["iam:GetRole", "iam:UpdateAssumeRolePolicy", "iam:PutRolePolicy", "iam:DeleteRolePolicy"],
                        ),
                        # allow upstream connections (granular control is from upstream trust policy)
                        ConstructPermission([IF_DEV_ROLE_FORMAT.format("*", "*", "*")], ["sts:AssumeRole"]),
                    ]
                    put_inlined_policy(
                        if_dev_role_name, IF_DEV_POLICY_NAME_FORMAT.format(self._context_id), set(devrole_permissions), session
                    )

            # switch IF_ROLE (raise if not possible)
            if needs_to_assume_role:
                if self._is_upstream or get_caller_identity(session, self._params[AWSCommonParams.REGION]) != if_dev_role:
                    module_logger.info("Assuming role: {0}".format(if_dev_role))
                    # when used in a downstream app (wnen this conf is upstream), downstream app will be the host. so
                    # downstream app will have its own unique session name for assumed-upstream-role
                    context_id_as_session_name = (
                        self._generate_unique_id_for_context()
                        if not self._is_upstream
                        else self._params[ActivationParams.UNIQUE_ID_FOR_HOST_CONTEXT]
                    )
                    session = RefreshableBotoSession(
                        role_arn=if_dev_role,
                        session_name=context_id_as_session_name,
                        base_session=session,
                    ).refreshable_session()
                else:
                    module_logger.info(
                        f"Skipping assume_role call for the development role," f" since the caller identity is already {if_dev_role}"
                    )
            else:
                # This means the session is already created by DEV_ROLE, we can make it refreshable
                session = RefreshableBotoSession(
                    # TODO: Shall we pass rola_arn and session_name here? both args are optional,
                    #  not sure whether they are needed in collaboration?
                    # role_arn=self._params[AWSCommonParams.IF_DEV_ROLE],
                    # session_name="""??????""",
                    base_session=session,
                    duration_seconds=session_duration,
                ).refreshable_session()

        self.add_param(AWSCommonParams.BOTO_SESSION, session)

    def _setup_session(self, if_dev_role_name):
        session = None
        needs_to_check_role: bool = False
        needs_to_assume_role: bool = False
        if self._is_upstream:
            session = self._params[AWSCommonParams.HOST_BOTO_SESSION]
            # Remote conf/app must have already given permission to us (probably via App::export_to_remote_app) or directly.
            # so the role switch will succeed and the owner context/app of this app will be loaded seamlessly.
            needs_to_assume_role = True
        elif AWSCommonParams.AUTHENTICATE_WITH_DEV_ROLE in self._params:
            # first always get the root session (account based one, also an assumed-role per user)
            # - to check the existence of the role first
            # - then either enforce role based retrieval (if exists and also the admin role is not already a trustee)
            # - or create the role using the root credentials and then assume it (if not exists)
            self._params[AWSCommonParams.IF_DEV_ROLE] = self._build_dev_role()
            admin_session = get_session_with_account(self._params[AWSCommonParams.ACCOUNT_ID], self._params[AWSCommonParams.REGION])

            dev_role_exists: bool = False
            already_in_trust_policy: bool = False
            try:
                if has_role(if_dev_role_name, admin_session):
                    dev_role_exists = True
                    admin_identity: str = get_caller_identity(admin_session, self._params[AWSCommonParams.REGION])
                    if is_in_role_trust_policy(if_dev_role_name, admin_session, admin_identity):
                        already_in_trust_policy = True
            except Exception as err:
                module_logger.error(
                    f"Could not get credentials for account {self._params[AWSCommonParams.ACCOUNT_ID]}. "
                    f"Please check your permissions with the account. If you think that this is temporary"
                    f" then try again in a moment. Error: {str(err)}"
                )
                raise err

            if dev_role_exists and not already_in_trust_policy:
                # enforce role-based credentials which would not require trust-policy modifications
                # (handled by IIBS broker service). But necessary set-up against the role must have been completed
                # via Conduit (bindle based delegation to broker service).
                session = get_session(self._params[AWSCommonParams.IF_DEV_ROLE], self._params[AWSCommonParams.REGION])
                # check the new session and fail early with a proper message
                try:
                    self._params[AWSCommonParams.ACCOUNT_ID] = get_aws_account_id(session, self._params[AWSCommonParams.REGION])
                except Exception as err:
                    module_logger.error(
                        f"Could not get credentials for role {self._params[AWSCommonParams.IF_DEV_ROLE]!r}."
                        f"If you think that this is temporary then try again in a moment. Error: {str(err)}"
                    )
                    raise err
            else:
                # use the session as admin credentials to set things up and then assume.
                # note: the reason we cannot use this as the default mode is that
                # we cannot keep assuming on behalf of different users since they need to be added to the dev-role
                # trust-policy which has an ACL (2 KB) limit. so for common applications to be accessed by different
                # users, this mode is not possible.
                # one exception: if the admin role is already in the trust policy of the dev-role (e.g the original
                # creator of the app/role, where 'already_in_trust_policy = True'), then we use it as is.
                session = admin_session
                needs_to_check_role = True
                needs_to_assume_role = True
                session_duration = MAX_CHAINED_ROLE_DURATION
        elif AWSCommonParams.IF_ACCESS_PAIR in self._params:
            # session that can assume IF_DEV_ROLE
            session = get_session(self._params[AWSCommonParams.IF_ACCESS_PAIR], self._params[AWSCommonParams.REGION])
            needs_to_assume_role = True
        elif AWSCommonParams.IF_ADMIN_ROLE in self._params:
            # session that can check/create/delete IF_DEV_ROLE and then assume it
            session = get_session(self._params[AWSCommonParams.IF_ADMIN_ROLE], self._params[AWSCommonParams.REGION])
            needs_to_check_role = True
            needs_to_assume_role = True
        elif AWSCommonParams.IF_ADMIN_ACCESS_PAIR in self._params:
            # session that can check/create/delete IF_ROLE and then assume it
            session = get_session(self._params[AWSCommonParams.IF_ADMIN_ACCESS_PAIR], self._params[AWSCommonParams.REGION])
            needs_to_check_role = True
            needs_to_assume_role = True
        elif AWSCommonParams.ACCOUNT_ID in self._params and self._params.get(AWSCommonParams.AUTHENTICATE_WITH_ACCOUNT_ID, False):
            session = get_session(
                IF_DEV_ROLE_FORMAT.format(self._params[AWSCommonParams.ACCOUNT_ID], self._context_id, self._params[AWSCommonParams.REGION]),
                self._params[AWSCommonParams.REGION],
            )
        else:  # use system defaults
            # treat the credentials as a
            session = get_session(None, self._params[AWSCommonParams.REGION], None)
            needs_to_check_role = self._params.get(AWSCommonParams.DEFAULT_CREDENTIALS_AS_ADMIN, False)
            # we should still obey the grand permission scope determined by configuration (i.e dev-time
            # permissions declared by drivers).
            # also we have to make sure that everything we do during dev is doable by dev-role
            # this is important for cross-app collaboration scheme to work.
            needs_to_assume_role = True
        return needs_to_assume_role, needs_to_check_role, session

    def get_admin_session(self) -> "boto3.Session":
        if_dev_role_name: str = self._build_dev_role_name()
        if AWSCommonParams.AUTHENTICATE_WITH_DEV_ROLE in self._params:
            # this means that previously the conf was gotten up with this mode.
            # so it should be ok to get the credentials root (account based one) to do the clean-up.
            admin_session = get_session_with_account(self._params[AWSCommonParams.ACCOUNT_ID], self._params[AWSCommonParams.REGION])

            try:
                # make an exceptional test call to see if the session retrieval was successful.
                has_role(if_dev_role_name, admin_session)
            except Exception as err:
                module_logger.error(
                    f"Could not get credentials for account {self._params[AWSCommonParams.ACCOUNT_ID]}. "
                    f"Please check your permissions with the account. If you think that this is temporary"
                    f" then try again in a moment. Error: {str(err)}"
                )
                raise err
        elif AWSCommonParams.IF_ACCESS_PAIR in self._params:
            # session that can assume IF_DEV_ROLE
            module_logger.critical("Attempting to use initial AWS Credentials pair as admin...")
            admin_session = get_session(self._params[AWSCommonParams.IF_ACCESS_PAIR], self._params[AWSCommonParams.REGION])
        elif AWSCommonParams.IF_ADMIN_ROLE in self._params:
            # session that can check/create/delete IF_DEV_ROLE
            admin_session = get_session(self._params[AWSCommonParams.IF_ADMIN_ROLE], self._params[AWSCommonParams.REGION])
        elif AWSCommonParams.IF_ADMIN_ACCESS_PAIR in self._params:
            # session that can check/create/delete IF_ROLE
            admin_session = get_session(self._params[AWSCommonParams.IF_ADMIN_ACCESS_PAIR], self._params[AWSCommonParams.REGION])
        elif AWSCommonParams.ACCOUNT_ID in self._params and self._params.get(AWSCommonParams.AUTHENTICATE_WITH_ACCOUNT_ID, False):
            admin_session = get_session(self._build_dev_role(), self._params[AWSCommonParams.REGION])
        else:  # use system defaults
            # treat the credentials as an admin
            admin_session = get_session(None, self._params[AWSCommonParams.REGION], None)
            if not self._params.get(AWSCommonParams.DEFAULT_CREDENTIALS_AS_ADMIN, False):
                module_logger.critical(f"Attempting to use default AWS Credentials as admin...")

        return admin_session

    def _clean_common_params(self) -> None:
        """Clean up the resources allocated in _add_common_params.
        Most important thing to clean up (for this impl) is the dev-role.

        So the sequence here is quite similar to _add_common_params to get a potent session to handle
        the necessary deletion against dev role, etc.
        """
        if_dev_role: str = self._params[AWSCommonParams.IF_DEV_ROLE]
        if_dev_role_name: str = self._build_dev_role_name()

        admin_session = self.get_admin_session()

        from botocore.exceptions import ClientError

        try:
            delete_role(if_dev_role_name, admin_session)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["NoSuchEntityException", "NoSuchEntity"]:
                module_logger.critical(f"Could not delete role {if_dev_role} due to error: {error.response['Error']!r}.")
                module_logger.critical(f"You might consider removing it manually.")

    # override
    def add_permissions_for_removed_drivers(self, removed_drivers: Set[BaseConstruct]) -> None:
        if removed_drivers:
            devrole_permissions = set(p for r in removed_drivers for p in r.provide_devtime_permissions(self._params))

            if devrole_permissions:
                session = self._params[AWSCommonParams.BOTO_SESSION]
                if_dev_role_name: str = self._build_dev_role_name()
                put_inlined_policy(
                    if_dev_role_name,
                    IF_DEV_REMOVED_DRIVERS_TEMP_POLICY_NAME_FORMAT.format(self._context_id),
                    set(devrole_permissions),
                    session,
                )

    # override
    def clean_permissions_for_removed_drivers(self, removed_drivers: Set[BaseConstruct]) -> None:
        if removed_drivers:
            session = self._params[AWSCommonParams.BOTO_SESSION]
            if_dev_role_name: str = self._build_dev_role_name()
            delete_inlined_policy(if_dev_role_name, IF_DEV_REMOVED_DRIVERS_TEMP_POLICY_NAME_FORMAT.format(self._context_id), session)

    # override
    def _setup_runtime_params(self) -> None:
        # setup common params to be used by constructs during activation
        # into self._params
        if_exe_role_name: str = self._build_exe_role_name()
        if_exe_role: str = self._build_exe_role()
        self._params[AWSCommonParams.IF_EXE_ROLE] = if_exe_role

        session = self._params[AWSCommonParams.BOTO_SESSION]
        # check the role first
        if not has_role(if_exe_role_name, session):
            create_role(
                if_exe_role_name,
                session,
                self._params[AWSCommonParams.REGION],
                False,
                self.provide_runtime_trusted_entities(),
                # will cause malformed entity if the role is new
                # [if_exe_role],
            )
        update_role(
            if_exe_role_name,
            session,
            self._params[AWSCommonParams.REGION],
            False,
            self.provide_runtime_trusted_entities(),
            [if_exe_role],
        )

        for managed_policy in self.provide_runtime_default_policies():
            exponential_retry(attach_aws_managed_policy, ["ServiceFailureException"], if_exe_role_name, managed_policy, session)

        # always update
        put_inlined_policy(
            if_exe_role_name, IF_EXE_POLICY_NAME_FORMAT.format(self._context_id), set(self.provide_runtime_construct_permissions()), session
        )

    # override
    def _clean_runtime_params(self) -> None:
        from botocore.exceptions import ClientError

        if_exe_role_name: str = self._build_exe_role_name()
        session = self._params[AWSCommonParams.BOTO_SESSION]
        try:
            delete_role(if_exe_role_name, session)
        except ClientError as error:
            if error.response["Error"]["Code"] not in ["NoSuchEntityException", "NoSuchEntity"]:
                raise

    # overrides
    def process_upstream_grants(
        self, upstream_grants: UpstreamGrants, unique_context_id: str, remote_platform: "DevelopmentPlatform"
    ) -> None:
        """Implementation for method is quite trivial for AWSConfiguration because here we exploit AWS IAM provided segmentation
        for each inline policy and make them specific to an upstream platform.

        So we set each upstream grant / permission list as an isolated inlined policy to the roles.

         - You can add "as many inline policies as you" want to an IAM user, role, or group. But the total aggregate
         policy size (the sum size of all inline policies) per entity cannot exceed the following quotas:
           Role policy size cannot exceed 10,240 characters.

        refer
            https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_iam-quotas.html
        """
        session = self._params[AWSCommonParams.BOTO_SESSION]
        for permission_group in upstream_grants.permission_groups:
            if permission_group.group_id == self._params[AWSCommonParams.IF_EXE_ROLE]:
                put_inlined_policy(
                    self._build_exe_role_name(), IF_EXE_POLICY_NAME_FORMAT.format(unique_context_id), permission_group.permissions, session
                )
            elif permission_group.group_id == self._params[AWSCommonParams.IF_DEV_ROLE]:
                put_inlined_policy(
                    self._build_dev_role_name(), IF_DEV_POLICY_NAME_FORMAT.format(unique_context_id), permission_group.permissions, session
                )

    # overrides
    def process_upstream_revokes(
        self, upstream_revokes: UpstreamRevokes, unique_context_id: str, remote_platform: "DevelopmentPlatform"
    ) -> None:
        """Please refer 'process_upstream_grants' for detailes on this implementation."""
        session = self._params[AWSCommonParams.BOTO_SESSION]
        for permission_group in upstream_revokes.permission_groups:
            if permission_group.group_id == self._params[AWSCommonParams.IF_EXE_ROLE]:
                delete_inlined_policy(self._build_exe_role_name(), IF_EXE_POLICY_NAME_FORMAT.format(unique_context_id), session)
            elif permission_group.group_id == self._params[AWSCommonParams.IF_DEV_ROLE]:
                delete_inlined_policy(self._build_dev_role_name(), IF_EXE_POLICY_NAME_FORMAT.format(unique_context_id), session)

    # overrides
    @classmethod
    def get_supported_constructs_map(cls) -> Dict[Type[BaseConstruct], Set[Type[BaseConstruct]]]:
        return {
            ProcessingUnit: {AWSLambdaProcessorBasic},
            ProcessorQueue: {AWSProcessorQueueBasic},
            BatchCompute: {
                CompositeBatchCompute,
                AWSAthenaBatchCompute,
                AWSGlueBatchComputeBasic,
                AWSEMRBatchCompute,
                AWSSagemakerTrainingJobBatchCompute,
                AWSSagemakerTransformJobBatchCompute,
            },
            RoutingTable: {AWSDDBRoutingTable},
            Diagnostics: {AWSCloudWatchDiagnostics},
            CompositeExtension: {CompositeExtension},
        }

    # overrides
    def get_remote_dev_env(self) -> DevEndpoint:
        notebook_instance = notebook.Instance(self._context_id, self._params)
        notebook_instance.sync()
        return notebook_instance

    # overrides
    def provision_remote_dev_env(self, bundle_uri: str, endpoint_attrs: Dict[str, Any]) -> DevEndpoint:
        """Provisions and returns the DevEndPoint for a remote Sagemaker notebook instance.

        This is the default (non-configurable) remote dev-endpoint for AWS configuration.
        """
        notebook_instance = notebook.Instance(self._context_id, self._params, bundle_uri, endpoint_attrs)
        notebook_instance.provision()
        return notebook_instance

    # overrides
    def deprovision_remote_dev_env(self):
        # TODO
        pass

    # overrides
    def is_on_remote_dev_env(self) -> bool:
        # TODO remove IAM user based check (should be redundant after the intro of method call to deployment module)
        return (
            is_on_remote_dev_env()
            or self._params.get(AWSCommonParams.ASSUMED_ROLE_USER, "") == SAGEMAKER_NOTEBOOK_INSTANCE_ASSUMED_ROLE_USER
        )


class DevelopmentPlatform(Platform):
    def __init__(self, conf: Configuration):
        assert conf, f"Configuration is not valid!"
        self._enforce_runtime_compatibility = True
        self._incompatible_runtime = False
        self._context_id = None
        self._conf = conf

    def _serializable_copy_init(self, org_instance: "DevelopmentPlatform") -> None:
        # no need to call super since we will let conf to handle construct serialization
        # first and then assign our attributes accordingly.
        self._conf = org_instance._conf.serializable_copy()
        self._storage = self._conf.get_active_construct(Storage)
        self._processor = self._conf.get_active_construct(ProcessingUnit)
        self._processor_queue = self._conf.get_active_construct(ProcessorQueue)
        self._batch_compute = self._conf.get_active_construct(BatchCompute)
        self._routing_table = self._conf.get_active_construct(RoutingTable)
        self._diagnostics = self._conf.get_active_construct(Diagnostics)
        self._extensions = self._conf.get_active_construct(CompositeExtension)

    @property
    def enforce_runtime_compatibility(self) -> bool:
        return getattr(self, "_enforce_runtime_compatibility", True)

    @enforce_runtime_compatibility.setter
    def enforce_runtime_compatibility(self, enforce: bool) -> None:
        self._enforce_runtime_compatibility = enforce

    @property
    def incompatible_runtime(self) -> bool:
        return getattr(self, "_incompatible_runtime", False)

    @property
    def conf(self) -> Configuration:
        return self._conf

    @Platform.context_id.setter
    def context_id(self, context_id: str) -> None:
        self._context_id = context_id
        self._conf.dev_init(context_id)
        self._init_storage_construct()
        synced = False
        if self.should_load_constructs() and self._check_platform_exists():
            try:
                self._load_constructs()
                synced = True
            except (ModuleNotFoundError, AttributeError, SerializationError, DeserializationError) as error:
                self._incompatible_runtime = True
                module_logger.critical(f"Active platform state is not compatible! Error: {error!r}")
                if self.enforce_runtime_compatibility:
                    raise error
        if not synced:
            self._provide_constructs()
        self._init_constructs()

    def _init_storage_construct(self) -> None:
        self._storage = self._conf.provide_storage()
        self._storage.dev_init(self)

    def _check_platform_exists(self) -> bool:
        return self._storage.check_object([Platform.__name__], DevelopmentPlatform.__name__)

    def _load_constructs(self) -> None:
        # load/deserialize from the storage
        serialized_platform = self._storage.load([Platform.__name__], DevelopmentPlatform.__name__)
        curr_platform: Platform = DevelopmentPlatform.deserialize(serialized_platform)
        self.bind(curr_platform)

    def _provide_constructs(self) -> None:
        self._processor = self._conf.provide_processor()
        self._processor_queue = self._conf.provide_processor_queue()
        self._batch_compute = self._conf.provide_batch_compute()
        self._routing_table = self._conf.provide_routing_table()
        self._diagnostics = self._conf.provide_diagnostics()
        self._extensions = self._conf.provide_extensions()

    def _init_constructs(self) -> None:
        self._processor.dev_init(self)
        self._processor_queue.dev_init(self)
        self._batch_compute.dev_init(self)
        self._routing_table.dev_init(self)
        self._diagnostics.dev_init(self)
        self._extensions.dev_init(self)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(conf={repr(self._conf)})"

    @abstractmethod
    def bind(self, persisted_platform: "Platform") -> None:
        """DevelopmentPlatform impls should provide the logic to handle compatibility checks ,etc against an already active platform"""
        self._processor = persisted_platform.processor
        self._processor_queue = persisted_platform.processor_queue
        self._batch_compute = persisted_platform.batch_compute
        self._routing_table = persisted_platform.routing_table
        self._diagnostics = persisted_platform.diagnostics
        self._extensions = persisted_platform.extensions

    @abstractmethod
    def should_load_constructs(self) -> bool:
        ...

    def process_downstream_connection(self, downstream_platform: "DevelopmentPlatform") -> UpstreamGrants:
        conf_grants: UpstreamGrants = self._conf.process_downstream_connection(downstream_platform)
        for const_ins in self._conf._activated_construct_instance_map.values():
            conf_grants << const_ins.process_downstream_connection(downstream_platform)
        return conf_grants

    def terminate_downstream_connection(self, downstream_platform: "DevelopmentPlatform") -> UpstreamRevokes:
        conf_revokes: UpstreamRevokes = self._conf.terminate_downstream_connection(downstream_platform)
        for const_ins in self._conf._activated_construct_instance_map.values():
            conf_revokes << const_ins.terminate_downstream_connection(downstream_platform)
        return conf_revokes


class HostPlatform(DevelopmentPlatform):
    class MetricType(Enum):
        ORCHESTRATION = 1
        SYSTEM = 2
        ALL = 3

    def __init__(self, conf: Configuration):
        super().__init__(conf)
        self._terminated = False
        # keep a track of replaced or updated constructs
        # during the activation, old constructs are auto-terminated and replaced with
        # new/active constructs from the conf
        self._replaced_constructs: Set[BaseConstruct] = set()

        # keep a track of upstream connections for the current development session
        # it will be wiped out post-activation (during the serialization).
        # Connections are tracked/persisted at Application level.
        self._upstream_connections: Dict[str, DevelopmentPlatform] = dict()
        self._removed_upstream_connections: Dict[str, DevelopmentPlatform] = dict()
        self._downstream_connections: Dict[str, Configuration] = dict()
        self._removed_downstream_connections: Dict[str, Configuration] = dict()

    def _serializable_copy_init(self, org_instance: "HostPlatform") -> None:
        super()._serializable_copy_init(org_instance)
        self._replaced_constructs = set()
        self._upstream_connections = dict()
        self._removed_upstream_connections = dict()
        self._downstream_connections = dict()
        self._removed_downstream_connections = dict()

    def _check_update(self, construct: BaseConstruct, typ: Type[BaseConstruct]) -> BaseConstruct:
        if not construct:
            # very rare case of architectural change/extension in RheocerOS Platform abstraction.
            # a new construct is introduced or an existing one being discarded.
            module_logger.critical(
                f"Detected {typ!r} does not exist on this platform! Will add a new driver and "
                f"activate it for the first time during platform activation."
            )
            return self._conf.provide_construct(typ)

        active_construct_type: Type[BaseConstruct] = self._conf.get_active_construct_type(typ)
        if construct.__class__ != active_construct_type:
            module_logger.critical(
                f"Detected update on {typ!r}! Will replace {construct.__class__!r} with {active_construct_type!r} during activation."
            )
            self._replaced_constructs.add(construct)
            active_construct: BaseConstruct = self._conf.provide_construct(typ)
            active_construct.check_update(construct)
            return active_construct
        else:
            self._conf.restore_active_construct(typ, construct)

        return construct

    # overrides
    def bind(self, persisted_platform: "Platform") -> None:
        if not isinstance(persisted_platform, HostPlatform):
            raise TypeError(f"HostPlatform {self!r} cannot bind to existing platform: {persisted_platform!r} due to type mismatch!")

        # core of our seamless update mechanism (as long as confs are compatible)
        #
        # storage is different since at this point we are already using the active instance
        # from the conf. Its impl might need to check if it has read the persisted data from
        # another Storage impl's output.
        if self._storage.__class__ != persisted_platform.storage.__class__:
            # there has been update, let the prev one be notified about the switch
            # (and possibly terminate itself).
            self._replaced_constructs.add(persisted_platform.storage)
        # storage should be able to check and transfer state
        self._storage.check_update(persisted_platform.storage)

        self._processor = self._check_update(persisted_platform.processor, ProcessingUnit)
        self._processor_queue = self._check_update(persisted_platform.processor_queue, ProcessorQueue)
        self._batch_compute = self._check_update(persisted_platform.batch_compute, BatchCompute)
        self._routing_table = self._check_update(persisted_platform.routing_table, RoutingTable)
        self._diagnostics = self._check_update(persisted_platform.diagnostics, Diagnostics)
        self._extensions = self._check_update(persisted_platform.extensions, CompositeExtension)

    # overrides
    def should_load_constructs(self) -> bool:
        return True

    @property
    def terminated(self) -> bool:
        return self._terminated

    @terminated.setter
    def terminated(self, val: bool) -> None:
        self._terminated = val

    @property
    def upstream_connections(self) -> Dict[str, DevelopmentPlatform]:
        return self._upstream_connections

    @property
    def removed_upstream_connections(self) -> Dict[str, DevelopmentPlatform]:
        return self._removed_upstream_connections

    def connect_upstream(self, remote_platform: DevelopmentPlatform) -> None:
        # buffer the connection for this session, during the activation
        # they will be served to low-level entites such as drivers via this map.
        # See '.drivers.processor.aws.AWSLambdaProcessorBasic' for a basic
        # utilization of this map for external/upstream Signals.
        self._upstream_connections[remote_platform.conf._generate_unique_id_for_context()] = remote_platform

    def disconnect_upstream(self, remote_platform: DevelopmentPlatform) -> None:
        # low-level disconnection is the responsibility of underlying Constructs.
        # so even for a disconnect request we just let the remote_platform
        # added to the 'upstream connections' list so that constructs can still
        # use it to handle their cleanups (impl specific requests to upstream drivers).
        self.connect_upstream(remote_platform)
        # this is used for high-level clean-up
        self._removed_upstream_connections[remote_platform.conf._generate_unique_id_for_context()] = remote_platform

    def _process_upstream_connections(self) -> None:
        """Called at the end of the activation cycle of this downstream platform, to let its upstream
        dependencies finalize/establish the connection or terminate it."""
        # FUTURE consider changing sts:AssumeRole resources here for IF_DEV_ROLE. But it is not a problem now.
        for unique_ctx_id, remote_platform in self._upstream_connections.items():
            if unique_ctx_id not in self._removed_upstream_connections:
                upstream_grants: UpstreamGrants = remote_platform.process_downstream_connection(self)
                self._conf.process_upstream_grants(upstream_grants, unique_ctx_id, remote_platform)

        for unique_ctx_id, remote_platform in self._removed_upstream_connections.items():
            upstream_revokes: UpstreamRevokes = remote_platform.terminate_downstream_connection(self)
            self._conf.process_upstream_revokes(upstream_revokes, unique_ctx_id, remote_platform)

    def connect_downstream(self, context_id: str, remote_conf: Configuration):
        self._downstream_connections[remote_conf._generate_unique_id_for_context()] = remote_conf

    def disconnect_downstream(self, context_id: str, remote_conf: Configuration):
        self._removed_downstream_connections[remote_conf._generate_unique_id_for_context()] = remote_conf

    def add_security_conf(self, security_conf: Dict[Type[BaseConstruct], ConstructSecurityConf]):
        for const_type, const_ins in self._conf._activated_construct_instance_map.items():
            const_conf = security_conf.get(const_type, None)
            const_ins.hook_security_conf(const_conf, security_conf)

    def add_dashboards(self, dashboards: Dict[str, Any]):
        self.diagnostics.hook_dashboards(dashboards)

    def connect_external(self, signals: List[Signal]) -> None:
        for const_ins in self._conf._activated_construct_instance_map.values():
            const_ins.hook_external(signals)

    def connect_internal(self, route: Route) -> None:
        for const_ins in self._conf._activated_construct_instance_map.values():
            const_ins.hook_internal(route)

        # FUTURE platform wide early checks / validations (which are not driver dependent)
        # Do validations here which cannot be done in application layer (pre-activation)

        # now re-direct upstream input signals as external (detect & marshal as external)
        # this is the only place where we can do this since app layer would never call connect_external for those
        # signals mapped from (and owned by) other applications.
        upstream_signals: List[Signal] = []
        for signal in route.link_node.signals:
            context_uuid = signal.resource_access_spec.get_owner_context_uuid()
            if context_uuid and context_uuid != self.conf._generate_unique_id_for_context():
                if signal.resource_access_spec.source == SignalSourceType.TIMER:
                    self.connect_internal_signal(signal)
                else:
                    upstream_signals.append(signal)

        if upstream_signals:
            self.connect_external(upstream_signals)

    def connect_internal_signal(self, signal: Signal) -> None:
        for const_ins in self._conf._activated_construct_instance_map.values():
            const_ins.hook_internal_signal(signal)

    def get_route_metrics(self, route: Route) -> Dict[Type[BaseConstruct], List[Signal]]:
        """Provide metrics for a prospective route to the client. Describe what metrics would be emitted by the platform
        should this route is activated by the application.

        The reason this platform cannot provide get_route_metrics for all of the routes nor include them as part of the
        response to 'get_metrics' is that platform actually does not know (in development) which routes will make it
        to the end (activation) yet. It does not  / can not know about the routes/nodes that were not yet added, either.

        However, during the development (from new application code) user might want to set up metrics and alarming for
        routes/nodes that platform does not know about yet. Platform gets notified about new routes and other types of
        signals during the application activation.

        Platform object guarantees that the metrics from the response of this API will be emitted at runtime as long
        as the route will be kept as part of the application code and activated. Other than that, this API does not
        mean that

        So if this route is still as part of the application in the beginning of the activation, then it will be
        injected into the system (and provided to MetricsStore) via _connect_auto_internal_signals.
        """
        return {
            const_type: self._conf.get_active_construct(const_type).provide_route_metrics(route)
            for const_type in self._conf.get_supported_constructs_map().keys()
        }

    def get_internal_metrics(self) -> Dict[Type[BaseConstruct], List[Signal]]:
        """Provide all of the internal orchestration metrics that might be emitted by this
        platform (its internal drivers) post-activation. Response does not / can not contain route specific metrics.
        Please refer 'get_route_metrics" API for the reason behind exclusion of route metrics.

        For route metrics, call 'get_route_metrics' using the new route/node to be added to the platform.

        Returns a collective set of internal orchestration metrics grouped by each driver type.
        """
        return {
            const_type: self._conf.get_active_construct(const_type).provide_internal_metrics()
            for const_type in self._conf.get_supported_constructs_map().keys()
        }

    def get_system_metrics(self) -> Dict[Type[BaseConstruct], List[Signal]]:
        """Provide all of the system resource metrics that might be emitted by this
        platform (its internal drivers) post-activation. Response does not / can not contain route specific metrics and
        other internal metrics to be emitted by the orchestration logic of the Platform.

        Please use 'get_metrics" API to get a combined set of metrics from the platform, containing both system and
        orchestration internal metrics.

        For route metrics, call 'get_route_metrics' using the new route/node to be added to the platform.

        Returns a collective set of system metrics grouped by each driver type.
        """
        return {
            const_type: self._conf.get_active_construct(const_type).provide_system_metrics()
            for const_type in self._conf.get_supported_constructs_map().keys()
        }

    def get_metrics(self) -> Dict[Type[BaseConstruct], List[Signal]]:
        """Provide all of the high-level (system resource and orchestration) metrics that might be emitted by this
        platform (its internal drivers) post-activation. Response does not / can not contain route specific metrics.
        Please refer 'get_route_metrics" API for the reason behind exclusion of route metrics.

        For route metrics, call 'get_route_metrics' using the new route/node to be added to the platform.

        Returns a collective set of system and internal orchestration metrics grouped by each driver type.
        """
        return {
            const_type: self._conf.get_active_construct(const_type).provide_system_metrics()
            + self._conf.get_active_construct(const_type).provide_internal_metrics()
            for const_type in self._conf.get_supported_constructs_map().keys()
        }

    def _get_all_metrics_before_activation(self) -> Dict[Type[BaseConstruct], List[Signal]]:
        """Gathers route-specific and other (system + orchestration) metrics from all of the drivers.

        At this point (right in the beginning of platform activation and after all of the signals, routes have been
        gathered/collected from application development context), we can call 'provide_metrics_for_new_routes' on
        each driver since 'pending_internal_routes' on each one of them can now represent the final set of routes
        that will be activated.

        This private method is supposed to be used only in the beginning of a new platform activation.
        see '_connect_auto_internal_signals' for more details on its use.
        """
        return {
            const_type: self._conf.get_active_construct(const_type).provide_internal_metrics()
            + self._conf.get_active_construct(const_type).provide_metrics_for_new_routes()
            for const_type in self._conf.get_supported_constructs_map().keys()
        }

    def _connect_auto_internal_signals(self) -> None:
        """Inject driver provided internal signals (such as internal / routing metrics) into the platform.

        Other type of signals provided by application-level user logic must all have been injected into the platform
        already via application level activation at this point (see Plaform::connect_<signal_type> APIs and trace
        high-level clients such as Application context and node classes for a better understanding).

        So to mimic the same process, we retrieve internal signals from each driver and broadcast them within the
        platform (e.g internal metrics are retained by MetricsStore [added to MetricsStore::_pending_internal_signals).
        After this call, these internal signals will be added to _pending_internal_signals of each driver for further
        process (depending on their own impl), even the pending signals of the driver that provides them here.
        """
        # Inject RheocerOS own metrics just like user custom (internal) metrics
        for internal_signal_list in self._get_all_metrics_before_activation().values():
            for signal in internal_signal_list:
                self.connect_internal_signal(signal)

    def _terminate_replaced_drivers(self) -> None:
        for replaced_construct in self._replaced_constructs:
            replaced_construct.terminate()
        self._replaced_constructs.clear()

    def _get_all_removed_drivers(self) -> Set[BaseConstruct]:
        removed_drivers = set(self._replaced_constructs)
        if isinstance(self.batch_compute, CompositeBatchCompute):
            removed_drivers = removed_drivers.union(set(self.batch_compute.removed_drivers))

        removed_drivers = removed_drivers.union(set(self.extensions.removed_extensions))

        return removed_drivers

    def activate(self) -> None:
        self._connect_auto_internal_signals()

        self._conf.activate()
        all_removed_drivers = self._get_all_removed_drivers()
        self._conf.add_permissions_for_removed_drivers(all_removed_drivers)
        self._terminate_replaced_drivers()
        self._conf.authorize(list(self._downstream_connections.values()))
        self._conf.unauthorize(list(self._removed_downstream_connections.values()))

        activated_constructs: List[BaseConstruct] = []
        try:
            # TODO begin PARALLEL activation
            self.storage.activate()
            activated_constructs.append(self.storage)
            self.processor_queue.activate()
            activated_constructs.append(self.processor_queue)
            self.batch_compute.activate()
            activated_constructs.append(self.batch_compute)
            self.routing_table.activate()
            activated_constructs.append(self.routing_table)
            self.processor.activate()
            activated_constructs.append(self.processor)
            self.diagnostics.activate()
            activated_constructs.append(self.diagnostics)
            self.extensions.activate()
            activated_constructs.append(self.extensions)
            # end PARALLEL EXEC

            self._process_signals()
            self._process_connections()
            self._process_security_conf()
            self._process_dashboards()

            self._process_upstream_connections()

            runtime_bootstrapper: RuntimePlatform = RuntimePlatform(
                self.context_id,
                self.storage,
                self.processor,
                self.processor_queue,
                self.batch_compute,
                self.routing_table,
                self.diagnostics,
                self.extensions,
            )
            self._update_bootstrapper(runtime_bootstrapper)
        except Exception as ex:
            module_logger.error(f"Activation error: {str(ex)}. Rolling back {activated_constructs}...")
            self._rollback(activated_constructs)
            raise

        self._conf.clean_permissions_for_removed_drivers(all_removed_drivers)
        self._activation_completed()

        # everything went well. we can capture this platform as the active one
        self._storage.save(self.serialize(True), [Platform.__name__], DevelopmentPlatform.__name__)

    def _rollback(self, rollback_list: Sequence[BaseConstruct]):
        for active_construct in rollback_list:
            active_construct.rollback()

    def _activation_completed(self):
        for const_ins in self._conf._activated_construct_instance_map.values():
            const_ins.activation_completed()

    def _update_bootstrapper(self, bootstrapper: "RuntimePlatform") -> None:
        for const_ins in self._conf._activated_construct_instance_map.values():
            const_ins._update_bootstrapper(bootstrapper)

    def _process_signals(self):
        for const_ins in self._conf._activated_construct_instance_map.values():
            const_ins.process_signals()

    def _process_connections(self):
        for const_ins in self._conf._activated_construct_instance_map.values():
            const_ins.process_connections()

    def _process_security_conf(self):
        for const_ins in self._conf._activated_construct_instance_map.values():
            const_ins.process_security_conf()

    def _process_dashboards(self):
        self.diagnostics.process_dashboards()

    def pause(self) -> None:
        for const_ins in self._conf._activated_construct_instance_map.values():
            const_ins.pause()

    def terminate(self) -> None:
        """Execute a logically reverse, but technically a much simpler version of 'activate'

        Why? To destroy is easier than to cre... wow.

        We just try to rely on the existing mechanisms and use them in a symmetrical manner to achieve reuse,
        even during the tear-down to make it graceful, fully consistent with a previous activation.
        """
        # 1- reuse activation sequence
        self._conf.unauthorize(list(self._removed_downstream_connections.values()))

        # 2- reuse the regular activation process but this time with changesets of X -> None on
        #    signals, inter-driver connections, upstream connections. This is the key to graceful,
        #    comprehensive tear-down operation.
        self._process_signals()
        self._process_connections()
        self._process_upstream_connections()

        # 3- let drivers do the clean-up now.
        # Warning: Storage should be terminated separately. See 'HostPlatform::delete'.
        #
        # Drivers are expected to be idempotent against repetitive terminate calls, but let's not bother the ones
        # with 'terminated' flag set (terminated already).
        if not self.processor.terminated:
            self.processor.terminate()
        if not self.processor_queue.terminated:
            self.processor_queue.terminate()
        if not self.batch_compute.terminated:
            self.batch_compute.terminate()
        if not self.routing_table.terminated:
            self.routing_table.terminate()
        if not self.diagnostics.terminated:
            self.diagnostics.terminate()
        if not self.extensions.terminated:
            self.extensions.terminate()
        self.storage.deactivate()
        self._conf.terminate()

        self._storage.delete([Platform.__name__], DevelopmentPlatform.__name__)
        self._terminated = True

    def delete(self) -> None:
        if self._conf.is_on_remote_dev_env():
            err_str = "Cannot delete application on remote development endpoint!"
            module_logger.critical(err_str)
            raise RuntimeError(err_str)

        self.deprovision_remote_dev_env()
        self.storage.terminate()
        # final nail on the coffin
        self._conf.delete()

    def provision_remote_dev_env(self, endpoint_attrs: Dict[str, Any]) -> DevEndpoint:
        """Provisions and returns the reference for remote dev endpoint"""
        # move it into conf provision_remote_dev_env.
        #  remote env type will decide on how to generate the bundle.
        self.sync_bundle()
        return self._conf.provision_remote_dev_env(self.get_bundle_uri(), endpoint_attrs)

    def deprovision_remote_dev_env(self) -> bool:
        """Deprovisions the remote dev endpoint previously created by this platform.

        :returns: True if a remote dev endpoint existed and was just cleaned by this call.
        """
        return self._conf.deprovision_remote_dev_env()

    def get_remote_dev_env(self) -> DevEndpoint:
        return self._conf.get_remote_dev_env()

    _BUNDLE_FOLDER: ClassVar[str] = "Bundle"
    _BUNDLE_ZIP_FILE: ClassVar[str] = "bundle.zip"

    def get_bundle_uri(self) -> str:
        return self._storage.get_object_uri([self._BUNDLE_FOLDER], self._BUNDLE_ZIP_FILE)

    def sync_bundle(self) -> None:
        """Syncs/updates bundle in the storage.

        Relies on 'deployment' module to pack the whole dependency closure / working set
        for this app runtime.o

        Bundle can be used to feed other development environments or (AWS) services in a consistent/compatible
        manner.
        """
        if not self._conf.is_on_remote_dev_env():
            from intelliflow.core.deployment import get_working_set_as_zip_stream

            module_logger.critical("Updating bundle in the storage...")
            bundle: bytes = get_working_set_as_zip_stream(include_resources=True)
            self._storage.save_object(bundle, [self._BUNDLE_FOLDER], self._BUNDLE_ZIP_FILE)
            module_logger.critical("Bundle update is successful!")
            module_logger.critical(f"Bundle size: {len(bundle)} bytes")
            module_logger.critical(f"Bundle URI: {self.get_bundle_uri()}")
        else:
            raise RuntimeError("Cannot sync bundle using this development environment!")

    def is_bundle_already_synced(self) -> bool:
        return self._storage.check_object(["Bundle"], "bundle.zip")

    def should_use_bundle(self) -> bool:
        return self._conf.is_on_remote_dev_env()

    def fetch_bundle(self) -> bytes:
        return self._storage.load_object(["Bundle"], "bundle.zip")


class RemotePlatform(DevelopmentPlatform):
    def __init__(self, conf: Configuration):
        super().__init__(conf)

    def refresh(self) -> None:
        # swap & reset so that DevelopmentPlatform re-loads
        context_id = self._context_id
        self._context_id = None

        super().context_id = context_id

    # overrides
    def should_load_constructs(self) -> bool:
        return True

    # overrides
    def bind(self, persisted_platform: Platform) -> None:
        if not persisted_platform:
            raise RuntimeError(f"RemotePlatform for {self.context_id} cannot be loaded!")

        if not isinstance(persisted_platform, HostPlatform) or not persisted_platform.storage:
            raise RuntimeError(f"RemotePlatform is corrupted. Type or storage cannot be retrieved")

        super().bind(persisted_platform)
        # remote binding is skipping check_update routine and getting directly down to conf update.
        self._conf.restore_active_construct(ProcessingUnit, persisted_platform.processor)
        self._conf.restore_active_construct(ProcessorQueue, persisted_platform.processor_queue)
        self._conf.restore_active_construct(BatchCompute, persisted_platform.batch_compute)
        self._conf.restore_active_construct(RoutingTable, persisted_platform.routing_table)
        self._conf.restore_active_construct(Diagnostics, persisted_platform.diagnostics)
        # FUTURE leave restore_active_construct only when "platform extensions" release is complete
        # case: when the parent platform has been activated with the extensions support, then we will set None here
        if persisted_platform.extensions:
            self._conf.restore_active_construct(CompositeExtension, persisted_platform.extensions)
        else:  # dummy "extensions" to unblock remote platform loading
            self._conf._active_construct_map[CompositeExtension] = CompositeExtension
            self._extensions = self._conf.provide_extensions()

    # overrides
    def _provide_constructs(self) -> None:
        raise RuntimeError(
            f"Bad operation on RemotePlatform for {self.context_id}!"
            f" RemotePlatform is meant to bind to another one, not supposed to create a new context."
        )
