# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from ...constructs import BatchCompute


class LocalSparkBatchComputeImpl(BatchCompute):
    """In-process execution of batch Slot using Spark in its local mode"""

    pass


class LocalSparkIPCBatchComputeImpl(BatchCompute):
    """IPC based Spark execution in a separate process that conforms to the expectations of this driver"""

    pass
