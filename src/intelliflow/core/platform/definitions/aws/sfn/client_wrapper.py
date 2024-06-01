# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import Optional

import botocore

from intelliflow.core.platform.definitions.aws.common import exponential_retry
from intelliflow.core.platform.definitions.compute import ComputeFailedSessionStateType, ComputeSessionStateType

logger = logging.getLogger(__name__)

# Refer
#  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions/client/stop_execution.html
# see how this is used in `get_batch_job_run_failure_type`
# Driver must use this reason for a better status reporting by this module.
INTELLIFLOW_AWS_SFN_CANCELLATION_REASON = "STOPPED_BY_INTELLIFLOW"


def get_sfn_state_machine_state_type(job_run) -> ComputeSessionStateType:
    """
    Refer
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions/client/describe_execution.html

    Use "status" from response to map state to one of framework's session states.
    """
    run_state = job_run["status"]
    if run_state in ["RUNNING", "PENDING_REDRIVE"]:  # we don't redrive but still a must to support it here
        return ComputeSessionStateType.PROCESSING
    elif run_state in ["SUCCEEDED"]:
        return ComputeSessionStateType.COMPLETED
    elif run_state in ["FAILED", "TIMED_OUT", "ABORTED"]:
        return ComputeSessionStateType.FAILED
    else:
        logger.critical(
            f"AWS Step Functions introduced a new state type {run_state} for state-machine executions!"
            f" Marking it as {ComputeSessionStateType.UNKNOWN}. "
            f" Please upgrade framework to a newer version."
        )
        return ComputeSessionStateType.UNKNOWN


def get_sfn_state_machine_failure_type(job_run) -> ComputeFailedSessionStateType:
    """
    Refer
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions/client/describe_execution.html
    """
    run_state = job_run["status"]
    if run_state in ["FAILED", "TIMED_OUT", "ABORTED"]:
        error: Optional[str] = job_run.get("error", None)
        cause: Optional[str] = job_run.get("cause", None)
        # TODO add logic for TRANSIENT (if possible)
        if error and error.strip().lower() in [INTELLIFLOW_AWS_SFN_CANCELLATION_REASON.lower()]:
            return ComputeFailedSessionStateType.STOPPED
        elif run_state in ["ABORTED"]:
            return ComputeFailedSessionStateType.STOPPED
        elif run_state in ["TIMED_OUT"]:
            return ComputeFailedSessionStateType.TIMEOUT
        else:
            return ComputeFailedSessionStateType.APP_INTERNAL
    else:
        logger.critical(
            f"AWS Step Functions introduced a new state type {run_state} for state-machine executions!"
            f" Marking failure type as {ComputeFailedSessionStateType.UNKNOWN}. "
            f" Please upgrade framework to a newer version."
        )
        return ComputeFailedSessionStateType.UNKNOWN
