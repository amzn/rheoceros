# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Sequence, cast

from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
from intelliflow.core.platform.definitions.aws.glue import catalog as glue_catalog
from intelliflow.core.platform.definitions.aws.glue.catalog import GlueTableDesc
from intelliflow.core.platform.definitions.aws.sns.client_wrapper import create_topic_arn, topic_exists
from intelliflow.core.platform.development import HostPlatform
from intelliflow.core.serialization import Serializable
from intelliflow.core.signal_processing import DimensionFilter, DimensionSpec, Signal, SignalDomainSpec, Slot
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.signal import SignalIntegrityProtocol, SignalType
from intelliflow.core.signal_processing.signal_source import (
    COMPUTE_HINTS_KEY,
    DATA_FORMAT_KEY,
    DATASET_FORMAT_KEY,
    DATASET_TYPE_KEY,
    DIMENSION_PLACEHOLDER_FORMAT,
    ENCRYPTION_KEY_KEY,
    PARTITION_KEYS_KEY,
    PRIMARY_KEYS_KEY,
    DatasetSignalSourceFormat,
    GlueTableSignalSourceAccessSpec,
    S3SignalSourceAccessSpec,
    SignalSourceAccessSpec,
    SignalSourceType,
    SNSSignalSourceAccessSpec,
)

from ..base import DataNode

logger = logging.getLogger(__name__)


class ExternalDataNode(DataNode, ABC):
    """Common base class external data providers."""

    class Descriptor(Serializable):
        def __init__(self, **kwargs) -> None:
            self._data_kwargs: Dict[str, Any] = dict(kwargs)
            self._proxy: ExternalDataNode.Descriptor = None

        def add(self, key: str, value: Any) -> None:
            self._data_kwargs[key] = value

        def link(self, proxy: "ExternalDataNode.Descriptor") -> "ExternalDataNode.Descriptor":
            """Link another source representation/descriptor of this node to another one.

            They will collectively determine the runtime mapping of incoming events to a valid signal
            for this node.
            """
            self._proxy = proxy
            return self

        @classmethod
        @abstractmethod
        def requires_dimension_spec(cls) -> bool: ...

        @classmethod
        def provide_default_dimension_spec(cls) -> DimensionSpec:
            return None

        @abstractmethod
        def provide_default_id(self) -> str: ...

        def set_platform(self, platform: HostPlatform) -> None:
            self._set_platform(platform)
            if self._proxy:
                self._proxy._set_platform(platform)

        def _set_platform(self, platform: HostPlatform) -> None:
            pass

        @abstractmethod
        def create_node(self, data_id: str, domain_spec: SignalDomainSpec, platform: HostPlatform) -> "ExternalDataNode":
            pass

        def create_source_spec(self, data_id: str, domain_spec: SignalDomainSpec) -> SignalSourceAccessSpec:
            own_spec = self._create_source_spec(data_id, domain_spec)
            if self._proxy:
                proxy_spec = self._proxy.create_source_spec(data_id, domain_spec)
                own_spec.link(proxy_spec)
            return own_spec

        @abstractmethod
        def _create_source_spec(self, data_id: str, domain_spec: SignalDomainSpec) -> SignalSourceAccessSpec:
            pass

    def __init__(self, desc: Descriptor, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._descriptor = desc

    # overrides
    def _slots(self) -> Optional[List[Slot]]:
        """Default behaviour is None.

        see :meth:`intelliflow.core.platform.context.node.base.Node._slots`
        """
        raise NotImplemented

    # overrides
    def do_activate(self, platform: HostPlatform) -> None:
        """Default implementation for ExternalDataNode is only about informing the platform about the Signal.

        Once informed, it is up to the Platform impl, its current Construct configurations and permissions to setup
        the necessary connections for this external data node.

        A sub-class can still provide an overwrite if it is absolutely necessary.
        We currently cannot envision a scenario where we would like to register a slot
        or take any other action in runtime, purely against a state change in an external
        source, within the context of this Node. So an ExternalDataNode is not expected to
        register a new Route to a Platform.

        Our current approach is to bind a child module such as
        :class:`intelliflow_core.core.platform.context.node.marshaling.nodes.MarshalerNode`
        for tracking external data nodes and for RheocerOS conformance.
        """
        platform.connect_external([self.signal().clone(None)])


class S3DataNode(ExternalDataNode):
    class Descriptor(ExternalDataNode.Descriptor):
        def __init__(self, account_id: str, bucket: str, folder: str, *partition_key: Sequence[str], **kwargs) -> None:
            super().__init__(**kwargs)
            self._account_id = account_id
            self._bucket = bucket
            self._folder = folder
            self._partition_key: List[str] = list(partition_key) if partition_key else []

        # overrides
        @classmethod
        def requires_dimension_spec(cls) -> bool:
            return True

        @classmethod
        def provide_default_dimension_spec(cls) -> DimensionSpec:
            return DimensionSpec()

        # overrides
        def provide_default_id(self) -> str:
            """If use did not provide an ID for the data node, then we create unique ID using bucket and folder name
            representing the underlying storage unique to the data.

            In order to make sure that bucket + folder combination can be used as an RheocerOS data ID, we transform
            AWS S3 bucket format to BASE64 ASCII format, so that it won't cause any trouble in aliasing in compute code, etc
            Please note that RheocerOS uses data_ids as variable names in compute runtimes (Python, Scala, PrestoSQL, etc).
            Additional char replacements for '/', '=', '+' are coming from Base64 table. Again they cannot be used as
            variable names in compute runtimes that we support.

            Why do we need this?
            To allow more user convenience in high-level APIs where a node with less verbosity can be bound to a node
            as an input. For that node, reuse might not be a concern for the user.
            Ex:
                app.create_data("foo", inputs=[app.marshal_external_data(S3Dataset('acc_id', 'bucket', 'folder'))], ...)

                as you can see ID field is skipped in marshal_external_data call.
            """
            import codecs

            default_id = f"{self._bucket}_{self._folder}"
            return codecs.encode(default_id.encode("utf-8"), "base64").decode().replace("/", "_").replace("=", "_").replace("+", "_")

        # overrides
        def create_node(self, data_id: str, domain_spec: SignalDomainSpec, platform: HostPlatform) -> ExternalDataNode:
            return S3DataNode(self, data_id, domain_spec)

        # overrides
        def _create_source_spec(self, data_id: str, domain_spec: SignalDomainSpec) -> SignalSourceAccessSpec:
            s3_source_access_spec: S3SignalSourceAccessSpec = None

            # extract partition keys as dimension names from the dimension spec
            dataset_args = dict(self._data_kwargs)
            dataset_args[PARTITION_KEYS_KEY] = [key for key in domain_spec.dimension_spec.get_flattened_dimension_map().keys()]

            if len(self._partition_key) == 0:
                # partition_keys will be extracted from domain_spec.dimension_spec
                # client opted out from specifying the underlying resource_format within the access spec
                s3_source_access_spec = S3SignalSourceAccessSpec(
                    self._account_id, self._bucket, self._folder, domain_spec.dimension_spec, **dataset_args
                )
            else:
                if not domain_spec.dimension_spec.check_compatibility(self._partition_key):
                    raise ValueError(
                        f"S3 {self.__class__.__name__}(data_id={data_id}, account_id={self._account_id}, bucket={self._bucket}, folder={self._folder}, ...) init failed "
                        f"due to incompatible partition keys and dimension_spec"
                    )
                s3_source_access_spec = S3SignalSourceAccessSpec(
                    self._account_id, self._bucket, self._folder, *self._partition_key, **dataset_args
                )
            return s3_source_access_spec

    def __init__(self, desc: Descriptor, data_id: str, domain_spec: SignalDomainSpec) -> None:
        s3_source_access_spec = desc.create_source_spec(data_id, domain_spec)

        super().__init__(
            desc, data_id, s3_source_access_spec, domain_spec, None, None, None  # parent_node  # child_nodes
        )  # node_id (will automatically set)

    # overrides
    def signal(self) -> Signal:
        return Signal(SignalType.EXTERNAL_S3_OBJECT_CREATION, self._source_access_spec, self._domain_spec, self._data_id)  # alias


class GlueTableDataNode(ExternalDataNode):
    class Descriptor(ExternalDataNode.Descriptor):
        def __init__(self, database: str, table_name: str, partition_keys: Optional[List[str]] = None, **kwargs) -> None:
            super().__init__(**kwargs)
            self._database = database
            self._table_name = table_name

            if partition_keys is not None:
                self._partition_keys = partition_keys
                self.add(PARTITION_KEYS_KEY, partition_keys)
            elif PARTITION_KEYS_KEY in kwargs:
                self._partition_keys = kwargs[PARTITION_KEYS_KEY]
            else:
                self._partition_keys = None

            if self._partition_keys and not isinstance(self._partition_keys, list):
                raise TypeError(f"Please provide a list for {PARTITION_KEYS_KEY}")

            self._table_desc: Optional[GlueTableDesc] = None
            self._new_kwargs = None

        # overrides
        @classmethod
        def requires_dimension_spec(cls) -> bool:
            return False

        # overrides
        def provide_default_id(self) -> str:
            return self._table_name

        # overrides
        def _set_platform(self, platform: HostPlatform) -> None:
            # marshal glue catalog data in the form of S3.
            #  use information from catalog to compensate metadata normally expected from the user.
            region: str = platform.conf.get_param(AWSCommonParams.REGION)
            glue = platform.conf.get_param(AWSCommonParams.BOTO_SESSION).client("glue", region_name=region)
            self._table_desc: Optional[GlueTableDesc] = glue_catalog.create_signal(
                glue_client=glue, database_name=self._database, table_name=self._table_name
            )
            if not self._table_desc:
                raise ValueError(
                    f"Table {self._table_name!r} in database: {self._database!r} does not exist in Glue" f"catalog [region={region!r}]"
                )

            if self._table_desc._test_bypass_checks:
                return

            table_as_signal: Signal = self._table_desc.signal
            compute_hints = self._table_desc.default_compute_params

            self._new_kwargs = dict(self._data_kwargs)
            self._new_kwargs.update({COMPUTE_HINTS_KEY: compute_hints})

            if table_as_signal.resource_access_spec.source == SignalSourceType.S3:
                # check encryption
                s3_access_spec = cast("S3SignalSourceAccessSpec", table_as_signal.resource_access_spec)
                if s3_access_spec.encryption_key and not self._data_kwargs.get(ENCRYPTION_KEY_KEY, None):
                    # TODO this metadata does not mean that a client side encryption is required, so we will be ok with
                    # a warning for now (E.g S3 SSE)
                    logger.warning(
                        f"'encryption_key' should be provided for table {self._table_name!r} in database: {self._database!r}!"
                        f" You can ignore this if SSE encryption is used but even in that case you might"
                        f" need to provide compute 'extra_permissions' for KMS related permissions in"
                        f" execution role policy."
                    )

                data_format = s3_access_spec.attrs.get(DATA_FORMAT_KEY, None)
                if data_format:
                    if DATA_FORMAT_KEY in self._new_kwargs:
                        user_data_format = self._new_kwargs[DATA_FORMAT_KEY]
                        if user_data_format is not None and DatasetSignalSourceFormat(user_data_format) != data_format:
                            raise ValueError(
                                f"Data format does not match the value for S3 dataset {self._table_name!r}"
                                f" in database: {self._database!r}!"
                                f" User provided format : {user_data_format!r},"
                                f" keys in the catalog: {data_format!r}"
                            )
                    else:
                        self._new_kwargs.update({DATA_FORMAT_KEY: data_format})

                if self._partition_keys is not None and self._partition_keys != s3_access_spec.partition_keys:
                    raise ValueError(
                        f"Partition keys don't match the value for S3 dataset {self._table_name!r}"
                        f" in database: {self._database!r}!"
                        f" User keys: {self._partition_keys!r},"
                        f" keys in the catalog: {s3_access_spec.partition_keys!r}"
                    )
                self._partition_keys = s3_access_spec.partition_keys
            else:
                raise NotImplementedError(
                    f"Table {self._table_name!r} in database: {self._database!r} is not supported by"
                    f" any application layer external node implementation! "
                    f"Table type: {table_as_signal.resource_access_spec.source!r}"
                )

        @classmethod
        def _create_domain_spec(cls, user_domain_spec: SignalDomainSpec, catalog_domain_spec: SignalDomainSpec) -> SignalDomainSpec:
            dim_spec: DimensionSpec = None
            dim_filter_spec: DimensionFilter = None
            if user_domain_spec.dimension_spec is None:
                if user_domain_spec.dimension_filter_spec is None:
                    dim_spec = catalog_domain_spec.dimension_spec
                    dim_filter_spec = catalog_domain_spec.dimension_filter_spec
                else:
                    # honor user provided filter for both (e.g user knows '20210910' is a date but in catalog it is STRING)
                    dim_spec = user_domain_spec.dimension_filter_spec.get_spec()
                    try:
                        dim_spec.compensate(catalog_domain_spec.dimension_spec, overwrite=False)
                    except TypeError:
                        raise ValueError(
                            f"DimensionSpec is not compatible with partition keys from the catalog! \n"
                            f"user spec: {dim_spec.get_flattened_dimension_map()!r} \n"
                            f"catalog: {catalog_domain_spec.dimension_spec.get_flattened_dimension_map()!r}"
                        )
                    dim_filter_spec = user_domain_spec.dimension_filter_spec
                    dim_filter_spec.set_spec(dim_spec)
            else:
                # honor user spec but do checks on cardinality, etc
                dim_spec = user_domain_spec.dimension_spec
                dim_filter_spec = user_domain_spec.dimension_filter_spec
                if len(set(dim_spec.get_flattened_dimension_map().keys())) != len(
                    set(catalog_domain_spec.dimension_spec.get_flattened_dimension_map().keys())
                ):
                    raise ValueError(
                        f"Number of partition keys does not match between "
                        f"user provided keys: {dim_spec.get_flattened_dimension_map()!r} and keys from "
                        f"catalog: {catalog_domain_spec.dimension_spec.get_flattened_dimension_map()!r}"
                    )

                # make sure that user provided spec has all of the Dimension names
                try:
                    dim_spec.compensate(catalog_domain_spec.dimension_spec, overwrite=False)
                except TypeError:
                    raise ValueError(
                        f"DimensionSpec is not compatible with partition keys from the catalog! \n"
                        f"user spec: {dim_spec.get_flattened_dimension_map()!r} \n"
                        f"catalog: {catalog_domain_spec.dimension_spec.get_flattened_dimension_map()!r}"
                    )

                # this should not happen since Application layer is expected to do the same. but let's be safe.
                if dim_filter_spec is None:
                    dim_filter_spec = DimensionFilter.all_pass(for_spec=dim_spec)

            protocol = (
                user_domain_spec.integrity_check_protocol
                if user_domain_spec.integrity_check_protocol
                else catalog_domain_spec.integrity_check_protocol
            )

            return SignalDomainSpec(dim_spec, dim_filter_spec, protocol)

        # overrides
        def create_node(self, data_id: str, domain_spec: SignalDomainSpec, platform: HostPlatform) -> ExternalDataNode:
            if self._table_desc._test_bypass_checks:
                return GlueTableDataNode(self, data_id, domain_spec)

            table_as_signal: Signal = self._table_desc.signal

            # check user provided dim_spec and dim_filter
            new_domain_spec = self._create_domain_spec(user_domain_spec=domain_spec, catalog_domain_spec=table_as_signal.domain_spec)
            if table_as_signal.resource_access_spec.source == SignalSourceType.S3:
                # check encryption
                s3_access_spec = cast("S3SignalSourceAccessSpec", table_as_signal.resource_access_spec)
                # if the user has provided the account_id (as metadata, etc), honor it otherwise always default to
                #  'catalog' module retrieved account_id (which does not exist for S3 datasets right now [None),
                #  then fall back to the account_id of the current session.
                account_id: str = self._data_kwargs.get(
                    "account_id",
                    s3_access_spec.account_id if s3_access_spec.account_id else platform.conf.get_param(AWSCommonParams.ACCOUNT_ID),
                )

                return (
                    S3DataNode.Descriptor(
                        account_id,
                        s3_access_spec.bucket,
                        s3_access_spec.folder,
                        # add '{}' for each partition key
                        *[DIMENSION_PLACEHOLDER_FORMAT for key in s3_access_spec.partition_keys],
                        **self._new_kwargs,
                    )
                    .link(self)
                    .create_node(data_id, new_domain_spec, platform)
                )
            else:
                raise NotImplementedError(
                    f"Table {self._table_name!r} in database: {self._database!r} is not supported by"
                    f" any application layer external node implementation! "
                    f"Table type: {table_as_signal.resource_access_spec.source!r}"
                )

            # Now glue data node is not even a front-end entity, so this node never becomes as part of the topology,
            # descriptor creates S3 nodes (or other storage specific nodes in the future).
            # TODO  remove this node class
            # return GlueTableDataNode(self, data_id, domain_spec)

        # overrides
        def _create_source_spec(self, data_id: str, domain_spec: SignalDomainSpec) -> SignalSourceAccessSpec:
            if self._partition_keys and not domain_spec.dimension_spec.check_compatibility(self._partition_keys):
                raise ValueError(
                    f"{self.__class__.__name__}(data_id={data_id}, database={self._database}, table_name={self._table_name}, ...) init failed "
                    f"due to incompatible partition keys {self._partition_keys!r} and dimension_spec {domain_spec.dimension_spec!r}"
                )
            return GlueTableSignalSourceAccessSpec(
                self._database, self._table_name, self._partition_keys, domain_spec.dimension_spec, **self._data_kwargs
            )

    def __init__(self, desc: Descriptor, data_id: str, domain_spec: SignalDomainSpec) -> None:
        if domain_spec.integrity_check_protocol is not None:
            logger.critical(
                f"Signal integrity protocol defined for {data_id!r} will be ignored on events coming from "
                f"Glue since it has no effect on Glue Table event ingestion. Each incoming event will be "
                f"interpreted as a valid signal."
            )
            domain_spec = SignalDomainSpec(domain_spec.dimension_spec, domain_spec.dimension_filter_spec, None)

        glue_table_source_access_spec = desc.create_source_spec(data_id, domain_spec)

        super().__init__(
            desc, data_id, glue_table_source_access_spec, domain_spec, None, None, None  # parent_node  # child_nodes
        )  # node_id (will automatically set)

    # overrides
    def signal(self) -> Signal:
        return Signal(SignalType.EXTERNAL_GLUE_TABLE_PARTITION_UPDATE, self._source_access_spec, self._domain_spec, self._data_id)  # alias


class SNSNode(ExternalDataNode):
    class Descriptor(ExternalDataNode.Descriptor):
        def __init__(
            self, topic_name: str, region: Optional[str] = None, account_id: Optional[str] = None, retain_ownership: bool = True, **kwargs
        ) -> None:
            super().__init__(**kwargs)
            if not topic_name:
                raise ValueError(f"'topic_name' must be defined!")
            self._topic_name = topic_name
            self._region = region
            self._account_id = account_id
            self._retain_ownership = retain_ownership

        # overrides
        @classmethod
        def requires_dimension_spec(cls) -> bool:
            return False

        @classmethod
        def provide_default_dimension_spec(cls) -> DimensionSpec:
            return DimensionSpec.load_from_pretty({"time": {type: Type.DATETIME}})

        # overrides
        def provide_default_id(self) -> str:
            # convert SNS allowed hyphen to underscore to make it a valid data node id
            return self._topic_name.replace("-", "_")

        # overrides
        def _set_platform(self, platform: HostPlatform) -> None:
            """use platform to do validation and defaulting"""
            app_account_id: str = platform.conf.get_param(AWSCommonParams.ACCOUNT_ID)
            if not self._account_id:
                self._account_id = app_account_id
                logger.warning(
                    f"Undefined 'account_id' for SNS topic {self._topic_name!r} will be defaulted to application account {self._account_id!r}."
                )

            if not self._region:
                self._region = platform.conf.get_param(AWSCommonParams.REGION)
                logger.warning(
                    f"Undefined 'region' for SNS topic {self._topic_name!r} will be defaulted to application region {self._region!r}."
                )

            topic_arn: str = create_topic_arn(self._region, self._account_id, self._topic_name)

            if not self._retain_ownership:
                sns = platform.conf.get_param(AWSCommonParams.BOTO_SESSION).client("sns", region_name=self._region)
                if not topic_exists(sns, topic_arn):
                    raise ValueError(
                        f"Topic {self._topic_name!r} could not be found in account: {self._account_id!r}, region: {self._region!r}."
                    )
            elif self._account_id != app_account_id:
                raise ValueError(f"Cannot retain the ownership of topic {self._topic_name!r} from another account {self._account_id!r}!")
            else:
                logger.info(
                    f"Topic {self._topic_name} will be retained and its connections will be managed by framework."
                    f" If it does not exist already, it will be created during activation."
                )

        # overrides
        def create_node(self, data_id: str, domain_spec: SignalDomainSpec, platform: HostPlatform) -> ExternalDataNode:
            return SNSNode(self, data_id, domain_spec)

        # overrides
        def _create_source_spec(self, data_id: str, domain_spec: SignalDomainSpec) -> SignalSourceAccessSpec:
            return SNSSignalSourceAccessSpec(self._topic_name, self._account_id, self._region, self._retain_ownership, **self._data_kwargs)

    def __init__(self, desc: Descriptor, data_id: str, domain_spec: SignalDomainSpec) -> None:
        if domain_spec.integrity_check_protocol is not None:
            logger.critical(
                f"Signal integrity protocol defined for {data_id!r} will be ignored on events coming from "
                f"SNS. Each incoming event will be interpreted as a valid signal."
            )
            domain_spec = SignalDomainSpec(domain_spec.dimension_spec, domain_spec.dimension_filter_spec, None)

        sns_source_access_spec = desc.create_source_spec(data_id, domain_spec)

        super().__init__(desc, data_id, sns_source_access_spec, domain_spec, None, None, None)

    # overrides
    def signal(self) -> Signal:
        return Signal(SignalType.EXTERNAL_SNS_NOTIFICATION, self._source_access_spec, self._domain_spec, self._data_id)
