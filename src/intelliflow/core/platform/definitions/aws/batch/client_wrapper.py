import logging

import botocore

from intelliflow.core.platform.definitions.aws.common import exponential_retry
from intelliflow.core.platform.definitions.compute import ComputeFailedSessionStateType, ComputeSessionStateType

logger = logging.getLogger(__name__)


# Refer
#  https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/terminate_job.html
# see how this is used in `get_batch_job_run_failure_type`
# Batch driver must use this reason for a better status reporting by this module.
INTELLIFLOW_AWS_BATCH_CANCELLATION_REASON = "STOPPED_BY_INTELLIFLOW"


def get_batch_job_run_state_type(job_run) -> ComputeSessionStateType:
    """
    Refer
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/describe_jobs.html

    Use "status" from response to map state to one of framework's session states.
    """
    run_state = job_run["status"]
    if run_state in ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]:
        return ComputeSessionStateType.PROCESSING
    elif run_state in ["SUCCEEDED"]:
        return ComputeSessionStateType.COMPLETED
    elif run_state in ["FAILED"]:
        return ComputeSessionStateType.FAILED
    else:
        logger.critical(
            f"AWS Batch introduced a new state type {run_state} for batch jobs!"
            f" Marking it as {ComputeSessionStateType.UNKNOWN}. "
            f" Please upgrade framework to a newer version."
        )
        return ComputeSessionStateType.UNKNOWN


def get_batch_job_run_failure_type(job_run) -> ComputeFailedSessionStateType:
    """
    Refer
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/batch/client/describe_jobs.html

    Use "statusReason" from response to map failure state to one of framework's failed session states.

    Warning: valid values for "statusReason" are subject to change. So we will default to UNKNOWN for new states.
    """
    run_state = job_run["status"]
    if run_state in ["FAILED"]:
        secondary_state = job_run["statusReason"]
        if secondary_state in [INTELLIFLOW_AWS_BATCH_CANCELLATION_REASON]:
            return ComputeFailedSessionStateType.STOPPED
        # TODO Other reliable ways to extract timeout and some transient failures?
        # if secondary_state in ["MaxRuntimeExceeded", "MaxWaitTimeExceeded"]:
        #    return ComputeFailedSessionStateType.TIMEOUT
        elif "Host EC2".lower() in secondary_state.lower():
            # https://aws.amazon.com/blogs/compute/introducing-retry-strategies-for-aws-batch/
            return ComputeFailedSessionStateType.TRANSIENT
        else:
            return ComputeFailedSessionStateType.APP_INTERNAL
    else:
        logger.critical(
            f"AWS Batch introduced a new state type {run_state} for training jobs!"
            f" Marking training job failure type as {ComputeFailedSessionStateType.UNKNOWN}. "
            f" Please upgrade framework to a newer version."
        )
        return ComputeFailedSessionStateType.UNKNOWN


def create_compute_environment_waiter(batch_client):
    waiter_id = "ComputeEnvironmentWaiter"
    model = botocore.waiter.WaiterModel(
        {
            "version": 2,
            "waiters": {
                waiter_id: {
                    "delay": 2,
                    "operation": "DescribeComputeEnvironments",
                    "maxAttempts": 20,
                    "acceptors": [
                        {"expected": "VALID", "matcher": "pathAll", "state": "success", "argument": "computeEnvironments[].status"},
                        {"expected": "INVALID", "matcher": "pathAny", "state": "failure", "argument": "computeEnvironments[].status"},
                    ],
                }
            },
        }
    )
    return botocore.waiter.create_waiter_with_client(waiter_id, model, batch_client)


def create_job_queue_waiter(batch_client):
    waiter_id = "JobQueueWaiter"
    model = botocore.waiter.WaiterModel(
        {
            "version": 2,
            "waiters": {
                waiter_id: {
                    "delay": 2,
                    "operation": "DescribeJobQueues",
                    "maxAttempts": 20,
                    "acceptors": [
                        {"expected": "VALID", "matcher": "pathAll", "state": "success", "argument": "jobQueues[].status"},
                        {"expected": "INVALID", "matcher": "pathAny", "state": "failure", "argument": "jobQueues[].status"},
                    ],
                }
            },
        }
    )
    return botocore.waiter.create_waiter_with_client(waiter_id, model, batch_client)


def submit_job(batch_client, retryable_exceptions, api_params):
    """wrapped for better testability"""
    return exponential_retry(
        batch_client.submit_job, list(retryable_exceptions) + ["AccessDeniedException"], **api_params  # IAM propagation for exec role
    )


def describe_jobs(batch_client, retryable_exceptions, **api_params):
    return exponential_retry(batch_client.describe_jobs, list(retryable_exceptions), **api_params)


def describe_compute_environments(batch_client, retryable_exceptions, **api_params):
    """wrapped for better testability"""
    return exponential_retry(batch_client.describe_compute_environments, list(retryable_exceptions), **api_params)


def describe_job_queues(batch_client, retryable_exceptions, **api_params):
    """wrapped for better testability"""
    return exponential_retry(batch_client.describe_job_queues, list(retryable_exceptions), **api_params)


def describe_job_definitions(batch_client, retryable_exceptions, **api_params):
    """wrapped for better testability"""
    return exponential_retry(batch_client.describe_job_definitions, list(retryable_exceptions), **api_params)
