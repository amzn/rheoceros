# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import re
from enum import Enum, unique
from typing import Any, Dict, Iterator, List, Optional, Set

from botocore.exceptions import ClientError

from intelliflow.core.platform.definitions.compute import ComputeFailedSessionStateType, ComputeSessionStateType
from intelliflow.core.signal_processing.signal_source import ContentType, DatasetSignalSourceAccessSpec, DataType

logger = logging.getLogger(__name__)


@unique
class ImageScope(str, Enum):
    TRAINING = "training"
    INFERENCE = "inference"


@unique
class SagemakerBuiltinAlgo(str, Enum):
    """
    Refer
    https://docs.aws.amazon.com/sagemaker/latest/dg/ecr-us-east-1.html
    to find algo names that would be compatible with Sagemaker Python SDK
    """

    XGBOOST = "xgboost"
    PRINCIPLE_COMPONENT_ANALYSIS = "pca"


BUILTIN_ALGO_MODEL_ARTIFACT_RESOURCE_NAME = "model.tar.gz"


BUILTIN_ALGOS = {
    SagemakerBuiltinAlgo.XGBOOST.value: {
        "us-east-1": {
            # https://docs.aws.amazon.com/sagemaker/latest/dg/ecr-us-east-1.html
            ImageScope.TRAINING: {
                "default_version": "1.5-1",
                "images": {
                    "1.5-1": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.5-1",
                    "1.3-1": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.3-1",
                    "1.2-2": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.2-2",
                    "1.2-1": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.2-1",
                },
            },
            ImageScope.INFERENCE: {
                "default_version": "1.5-1",
                "images": {
                    "1.5-1": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.5-1",
                    "1.3-1": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.3-1",
                    "1.2-2": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.2-2",
                    "1.2-1": "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.2-1",
                },
            },
        },
        # TODO other regions (if we won't start using sagemaker SDK)
    }
}

BUILTIN_ALGO_CONTENT_TYPE_MAP = {
    # refer
    #   https://docs.aws.amazon.com/sagemaker/latest/dg/cdf-training.html
    SagemakerBuiltinAlgo.XGBOOST.value: {ContentType.CSV, ContentType.LIBSVM}
}


def map_training_image_to_builtin_algo(training_image_uri: str) -> str:
    # TODO
    def nested_contains(to_search, value):
        if isinstance(to_search, dict):
            return any(nested_contains(v, value) for v in to_search.values())
        elif isinstance(to_search, list):
            return any(nested_contains(v, value) for v in to_search)
        else:
            return to_search == value

    for algo_name, val in BUILTIN_ALGOS.items():
        if nested_contains(val, training_image_uri):
            return algo_name

    return None


def is_builtin_algo_name(algo_name: str) -> bool:
    return algo_name in BUILTIN_ALGOS


def get_builtin_algo_content_types(algo_name: str) -> Set[str]:
    return BUILTIN_ALGO_CONTENT_TYPE_MAP[algo_name]


def get_image_for_builtin(algo_name: str, region: str, image_scope: ImageScope, version: Optional[str] = None) -> Optional[str]:
    # TODO use sagemaker Python SDK
    #   https://sagemaker.readthedocs.io/en/stable/api/utility/image_uris.html

    ## - https://docs.aws.amazon.com/sagemaker/latest/dg/ecr-us-east-1.html
    ##  Example: xgboost
    ##     - https://docs.aws.amazon.com/sagemaker/latest/dg/ecr-us-east-1.html#xgboost-us-east-1.title
    # from sagemaker import image_uris
    # image_uris.retrieve(framework='algo_name', region='us-east-1', version='1.2-1')
    ## Output path
    ## '683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.2-1'
    scope = BUILTIN_ALGOS[algo_name][region][image_scope]
    scope_version = version if version else scope["default_version"]
    return scope["images"].get(scope_version, None)


def convert_image_uri_to_arn(image_uri: str) -> str:
    match = re.search(r"(?P<account>\d+).dkr.ecr.(?P<region>.*?).amazonaws.com/(?P<repo>.*?):(?P<tag>.*)", image_uri)
    return f'arn:aws:ecr:{match["region"]}:{match["account"]}:repository/{match["repo"]}'


def validate_inputs_for_builtin_algo(inputs: List["Signal"], algo_name: str):
    # Refer "Content Types Supported by Built-In Algorithms"
    #   https://docs.aws.amazon.com/sagemaker/latest/dg/cdf-training.html
    #   for 'content_type' check for each builtin algo
    if algo_name == SagemakerBuiltinAlgo.XGBOOST:
        dataset_inputs = [
            input
            for input in inputs
            if input.resource_access_spec.data_type == DataType.DATASET
            or isinstance(input.resource_access_spec, DatasetSignalSourceAccessSpec)
        ]
        if len(dataset_inputs) < 2:
            raise ValueError(f"There must be at least 2 dataset inputs for {algo_name!r}")

        alias_set = set([d.alias for d in dataset_inputs])
        if not {"train", "validation"}.issubset(alias_set):
            raise ValueError(
                f"Sagemaker builtin {algo_name!r} algo requires input datasets with 'train' and 'validation' alias'! "
                f"Current input alias': {alias_set!r}"
            )

        #  check if inputs don't have protocol (e.g _SUCCESS file)
        for d in dataset_inputs:
            if d.domain_spec.integrity_check_protocol:
                raise ValueError(
                    f"Input dataset {d.alias!r} must have 'protocol' set None for training in Sagemaker builtin {algo_name!r} algo! Folder should not contain anything other than partition files."
                )

            if d.resource_access_spec.data_schema_file:
                raise ValueError(
                    f"Input dataset {d.alias!r} must have 'schema' set None for training in Sagemaker builtin {algo_name!r} algo! Folder should not contain anything other than partition files."
                )

            #  check if content_type is ContentType.CSV ?
            content_type = d.resource_access_spec.content_type
            if content_type not in [ContentType.CSV, ContentType.LIBSVM]:
                raise ValueError(
                    f"Input dataset {d.alias!r} must have 'content_type' CSV or LIBSVM (not {content_type!r}) for training in Sagemaker builtin {algo_name!r} algo!"
                )
            #  check if inputs don't have HEADER

            if d.resource_access_spec.data_header_exists:
                raise ValueError(
                    f"Input dataset {d.alias!r} must have 'header' set to False for training in Sagemaker builtin {algo_name!r} algo!"
                )


def validate_hyper_parameters_for_builtin_algo(algo_name: str, hyper_parameters: Dict[str, str]):
    # TODO
    pass


@unique
class SagemakerTrainingInputMode(str, Enum):
    FILE = "File"
    PIPE = "Pipe"
    FAST_FILE = "FastFile"


def get_default_input_mode_for_builtin_algo(algo_name: str) -> SagemakerTrainingInputMode:
    """
    Refer "Pipe Mode" in https://docs.aws.amazon.com/sagemaker/latest/dg/cdf-training.html
    See the list of builtin algos that support PIPE mode:
       https://aws.amazon.com/blogs/machine-learning/now-use-pipe-mode-with-csv-datasets-for-faster-training-on-amazon-sagemaker-built-in-algorithms/
    """
    if algo_name == SagemakerBuiltinAlgo.XGBOOST:
        return SagemakerTrainingInputMode.FILE
    elif algo_name == SagemakerBuiltinAlgo.PRINCIPLE_COMPONENT_ANALYSIS:
        return SagemakerTrainingInputMode.PIPE

    return SagemakerTrainingInputMode.FILE


def get_default_hyper_parameters_for_builtin_algo(algo_name: str):
    # TODO
    return {
        SagemakerBuiltinAlgo.XGBOOST.value: {
            "max_depth": "5",
            "eta": "0.2",
            "gamma": "4",
            "min_child_weight": "6",
            "silent": "0",
            "objective": "multi:softmax",
            "num_class": "10",
            "num_round": "10",
        }
    }[algo_name]


def get_training_job_run_state_type(job_run) -> ComputeSessionStateType:
    """
    Refer
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_training_job

    Use "TrainingJobStatus" from response to map state to one of framework's session states.
    """
    run_state = job_run["TrainingJobStatus"]
    if run_state in ["InProgress", "Stopping"]:
        return ComputeSessionStateType.PROCESSING
    elif run_state in ["Completed"]:
        return ComputeSessionStateType.COMPLETED
    elif run_state in ["Stopped", "Failed"]:
        return ComputeSessionStateType.FAILED
    else:
        logger.critical(
            f"AWS Sagemaker introduced a new state type {run_state} for training jobs!"
            f" Marking it as {ComputeSessionStateType.UNKNOWN}. "
            f" Please upgrade framework to a newer version."
        )
        return ComputeSessionStateType.UNKNOWN


def get_training_job_run_failure_type(job_run) -> ComputeFailedSessionStateType:
    """
    Refer
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_training_job

    Use "SecondaryStatus" from response to map failure state to one of framework's failed session states.

    Warning: valid values for "SecondaryStatus" are subject to change. So we will default to UNKNOWN for new states.
    """
    run_state = job_run["TrainingJobStatus"]
    if run_state in ["Stopping", "Stopped"]:
        return ComputeFailedSessionStateType.STOPPED
    elif run_state in ["Failed"]:
        secondary_state = job_run["SecondaryStatus"]
        # TODO/FUTURE evaluate the use of failure reason to detect other transient cases
        # failure_reason = job_run.get("FailureReason", )
        if secondary_state in ["MaxRuntimeExceeded", "MaxWaitTimeExceeded"]:
            return ComputeFailedSessionStateType.TIMEOUT
        elif secondary_state in ["Interrupted"]:
            return ComputeFailedSessionStateType.TRANSIENT
        else:
            return ComputeFailedSessionStateType.APP_INTERNAL
    else:
        logger.critical(
            f"AWS Sagemaker introduced a new state type {run_state} for training jobs!"
            f" Marking training job failure type as {ComputeFailedSessionStateType.UNKNOWN}. "
            f" Please upgrade framework to a newer version."
        )
        return ComputeFailedSessionStateType.UNKNOWN


def get_transform_job_run_state_type(job_run) -> ComputeSessionStateType:
    """
    Refer
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker.html#SageMaker.Client.describe_transform_job

    Use "TransformJobStatus" from response to map state to one of framework's session states.
    """
    run_state = job_run["TransformJobStatus"]
    if run_state in ["InProgress"]:
        return ComputeSessionStateType.PROCESSING
    elif run_state in ["Completed"]:
        return ComputeSessionStateType.COMPLETED
    elif run_state in ["Stopping", "Stopped", "Failed"]:
        return ComputeSessionStateType.FAILED
    else:
        logger.critical(
            f"AWS Sagemaker introduced a new state type {run_state} for transform jobs!"
            f" Marking it as {ComputeSessionStateType.UNKNOWN}. "
            f" Please upgrade framework to a newer version."
        )
        return ComputeSessionStateType.UNKNOWN


def get_transform_job_run_failure_state_type(job_description) -> ComputeFailedSessionStateType:
    run_state = job_description["TransformJobStatus"]
    if run_state in ["Stopping", "Stopped"]:
        return ComputeFailedSessionStateType.STOPPED
    elif run_state in ["Failed"]:
        # TODO map failure reason to TRANSIENT, however this is tricky since we don't know what values it can be
        job_name = job_description.get("TransformJobName", None)
        failure_reason = job_description.get("FailureReason", None)
        logger.critical(f"AWS Sagemaker batch transform job {job_name} has failed, failure reason: {failure_reason}!")
        return ComputeFailedSessionStateType.APP_INTERNAL
    else:
        logger.critical(
            f"AWS Sagemaker introduced a new state type {run_state} for transform jobs!"
            f" Marking transform job failure type as {ComputeFailedSessionStateType.UNKNOWN}. "
            f" Please upgrade framework to a newer version."
        )
        return ComputeFailedSessionStateType.UNKNOWN


def describe_sagemaker_transform_job(
    sagemaker_client,
    job_name: str,
):
    try:
        res = sagemaker_client.describe_transform_job(TransformJobName=job_name)
        return res
    except ClientError as e:
        logger.exception(f"Couldn't describe transform job with job name: {job_name}")
        raise


def stop_batch_transform_job(sagemaker_client, job_name):
    try:
        sagemaker_client.stop_transform_job(TransformJobName=job_name)
    except:
        logger.exception("Couldn't stop transform job %s.", job_name)
        raise


def start_batch_transform_job(
    sagemaker_client,
    job_name: str,
    model_name: str,
    transform_output: Dict[str, Any],
    transform_input: Dict[str, Any],
    transform_resources: Dict[str, Any],
):
    try:
        response = sagemaker_client.create_transform_job(
            TransformJobName=job_name,
            ModelName=model_name,
            TransformInput=transform_input,
            TransformOutput=transform_output,
            TransformResources=transform_resources,
        )
        transform_job_arn = response["TransformJobArn"]
    except ClientError:
        logger.exception("Couldn't run Sagemaker batch transform job %s.", job_name)
        raise
    else:
        return transform_job_arn


def validate_job_name(job_name: str) -> bool:
    """
    Sagemaker job name pattern from: https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateTransformJob.html#sagemaker-CreateTransformJob-request-TransformJobName
    """
    return re.compile(r"^[a-zA-Z0-9](-*[a-zA-Z0-9]){0,62}").match(job_name) is not None


def create_sagemaker_model(
    sagemaker_client,
    model_name: str,
    primary_container: Dict[str, Any],
    execution_role_arn: str,
    tags: Dict[str, Any],
):
    try:
        response = sagemaker_client.create_model(
            ModelName=model_name,
            PrimaryContainer=primary_container,
            ExecutionRoleArn=execution_role_arn,
            Tags=tags,
        )
        model_arn = response["ModelArn"]
    except ClientError:
        logger.exception("Couldn't run Sagemaker create model for %s.", model_name)
        raise
    else:
        return model_arn
