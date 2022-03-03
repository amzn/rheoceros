# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List

from intelliflow.core.permission import Permission
from intelliflow.core.platform.constructs import BatchCompute as BatchComputeDriver
from intelliflow.core.platform.constructs import (
    ConstructEncryption,
    ConstructPersistenceSecurityDef,
    ConstructSecurityConf,
    EncryptionKeyAllocationLevel,
    ProcessingUnit,
    ProcessorQueue,
    RoutingComputeInterface,
    RoutingHookInterface,
    RoutingTable,
    Storage,
)
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.definitions.metric_alarm_defs import AlarmDefaultActionsMap, AlarmState, MetricExpression
from intelliflow.core.signal_processing.dimension_constructs import DIMENSION_VARIANT_IDENTICAL_MAP_FUNC, StringVariant

from ._logging_config import init_basic_logging
from .core.application.application import Application, OutputDimensionNameType
from .core.application.context.node.base import Node
from .core.application.context.node.external.alarm.nodes import CWAlarmNode
from .core.application.context.node.external.metric.nodes import CWMetricNode
from .core.application.context.node.external.nodes import ExternalDataNode, GlueTableDataNode, S3DataNode
from .core.application.context.node.internal.nodes import InternalDataNode
from .core.application.context.node.marshaling.nodes import MarshalerNode
from .core.deployment import set_deployment_conf
from .core.platform.definitions.aws.common import IF_DEV_ROLE_FORMAT
from .core.platform.development import AWSConfiguration, HostPlatform, LocalConfiguration
from .core.platform.endpoint import DevEndpoint
from .core.signal_processing.definitions.dimension_defs import NameType as DimensionNameType
from .core.signal_processing.definitions.dimension_defs import Type as DimensionType
from .core.signal_processing.definitions.metric_alarm_defs import (
    AlarmComparisonOperator,
    MetricPeriod,
    MetricStatistic,
    MetricStatisticData,
    MetricValueCountPairData,
)
from .core.signal_processing.routing_runtime_constructs import Route, RouteCheckpoint, RouteExecutionHook, RouteID, RoutePendingNodeHook
from .core.signal_processing.signal import SignalIntegrityProtocol
from .core.signal_processing.signal_source import DatasetSignalSourceFormat, GlueTableSignalSourceAccessSpec

S3Dataset = S3DataNode.Descriptor
GlueTable = GlueTableDataNode.Descriptor
DataFormat = DatasetSignalSourceFormat

CWMetric = CWMetricNode.Descriptor
CWAlarm = CWAlarmNode.Descriptor

GLUE_TABLE_CHANGE_EVENT = GlueTableSignalSourceAccessSpec.GENERIC_EVENT_RESOURCE_NAME

# Linking typedefs
EQUALS = DIMENSION_VARIANT_IDENTICAL_MAP_FUNC
EQUAL_TO = DIMENSION_VARIANT_IDENTICAL_MAP_FUNC
UPPER = StringVariant.UPPER_FUNC
LOWER = StringVariant.LOWER_FUNC

# dimension param defs
INSENSITIVE = StringVariant.CASE_INSENSITIVE

InlinedCompute = InternalDataNode.InlinedComputeDescriptor


# protocols
def completion_file(file_name: str) -> SignalIntegrityProtocol:
    return SignalIntegrityProtocol("FILE_CHECK", {"file": file_name})


IInlinedCompute = RoutingComputeInterface.IInlinedCompute
IExecutionBeginHook = RoutingHookInterface.Execution.IExecutionBeginHook
IExecutionSkippedHook = RoutingHookInterface.Execution.IExecutionSkippedHook
IComputeSuccessHook = RoutingHookInterface.Execution.IComputeSuccessHook
IComputeFailureHook = RoutingHookInterface.Execution.IComputeFailureHook
IComputeRetryHook = RoutingHookInterface.Execution.IComputeRetryHook
IExecutionSuccessHook = RoutingHookInterface.Execution.IExecutionSuccessHook
IExecutionFailureHook = RoutingHookInterface.Execution.IExecutionFailureHook
IExecutionCheckpointHook = RoutingHookInterface.Execution.IExecutionCheckpointHook

IPendingNodeCreationHook = RoutingHookInterface.PendingNode.IPendingNodeCreationHook
IPendingNodeExpirationHook = RoutingHookInterface.PendingNode.IPendingNodeExpirationHook
IPendingCheckpointHook = RoutingHookInterface.PendingNode.IPendingCheckpointHook


class BatchCompute(InternalDataNode.BatchComputeDescriptor):
    """Convenience class to facilitate default batch compute parametrization"""

    def __init__(
        self,
        code: str,
        lang: Lang = Lang.PYTHON,
        abi: ABI = ABI.GLUE_EMBEDDED,
        extra_permissions: List[Permission] = None,
        retry_count: int = 0,
        **kwargs
    ) -> None:
        super().__init__(code, lang, abi, extra_permissions, retry_count, **kwargs)


def create_or_update_application(id: str, platform: HostPlatform) -> Application:
    return Application(id, platform)
