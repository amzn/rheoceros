# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import math
from enum import Enum, unique
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

import boto3
from botocore.exceptions import ClientError
from packaging import version
from packaging.version import Version

from intelliflow.core.platform.definitions.compute import ComputeFailedSessionStateType, ComputeSessionStateType
from intelliflow.core.signal_processing.definitions.compute_defs import Lang

logger = logging.getLogger(__name__)

JOB_ARN_FORMAT = "arn:aws:glue:{}:{}:job/{}"


# refer for this list
#  https://docs.aws.amazon.com/glue/latest/dg/reduced-start-times-spark-etl-jobs.html#reduced-start-times-limitations
#  RheocerOS module uses these as a prefix to avoid all of the related modules (ex: boto is enough to avoid boto3 and botocore).
#  We mostly avoid critical components such as boto and the ones with 'native' (C, etc) dependencies (most of the popular
#  data science libraries).
PYTHON_MODULES_TO_BE_AVOIDED_IN_GLUE_BUNDLE: Set[str] = {
    "setuptools",
    "subprocess32",
    "ptvsd",
    "pydevd",
    "PyMySQL",
    "docutils",
    "jmespath",
    "six",
    "python_dateutil",
    "urllib3",
    "botocore",
    "s3transfer",
    "boto3",
    "certifi",
    "chardet",
    "idna",
    "requests",
    "pyparsing",
    "enum34",
    "pytz",
    "numpy",
    "cycler",
    "kiwisolver",
    "scipy",
    "pandas",
    "pyarrow",
    "matplotlib",
    "pyhocon",
    "mpmath",
    "sympy",
    "patsy",
    "statsmodels",
    "fsspec",
    "s3fs",
    "Cython",
    "joblib",
    "pmdarima",
    "scikit-learn",
    "tbats",
}


@unique
class GlueJobCommandType(str, Enum):
    BATCH = "glueetl"
    NORMAL = "pythonshell"


@unique
class GlueJobLanguage(str, Enum):
    PYTHON = "python"
    SCALA = "scala"

    @classmethod
    def from_slot_lang(cls, lang: Lang):
        if lang in [Lang.PYTHON, Lang.SPARK_SQL]:
            return cls.PYTHON
        elif lang == Lang.SCALA:
            return cls.SCALA
        else:
            raise ValueError(f"Slot lang '{lang!r}' is not supported by AWS Glue!")


@unique
class GlueVersion(str, Enum):
    AUTO = "auto"
    VERSION_0_9 = "0.9"
    VERSION_1_0 = "1.0"
    VERSION_2_0 = "2.0"
    VERSION_3_0 = "3.0"


def glue_spark_version_map() -> Dict[GlueVersion, Version]:
    """
    Source: https://docs.aws.amazon.com/glue/latest/dg/release-notes.html
    """
    return {
        GlueVersion.VERSION_0_9: version.parse("2.2.1"),
        GlueVersion.VERSION_1_0: version.parse("2.4.3"),
        GlueVersion.VERSION_2_0: version.parse("2.4.3"),
        GlueVersion.VERSION_3_0: version.parse("3.1.1"),
    }


@unique
class GlueWorkerType(str, Enum):
    STANDARD = "Standard"
    G_1X = "G.1X"
    G_2X = "G.2X"


def _create_job_params(
    description,
    role,
    job_command_type: GlueJobCommandType,
    job_language: GlueJobLanguage,
    script_s3_location,
    max_concurrent_runs=20,
    max_capacity_in_DPU=None,
    # https://docs.aws.amazon.com/glue/latest/dg/add-job.html
    glue_version=None,
    working_set_s3_location=None,
    default_args: Dict[str, Any] = None,
) -> Dict[str, Any]:

    default_arguments = {"--enable-metrics": "", "--job-language": job_language.value}
    if default_args:
        default_arguments.update(default_args)
    capacity_params = dict()
    if glue_version == "1.0":
        if not max_capacity_in_DPU:
            max_capacity_in_DPU = 20 if job_command_type == GlueJobCommandType.BATCH else 0.0625
        capacity_params.update({"MaxCapacity": max_capacity_in_DPU})
        if job_language == GlueJobLanguage.PYTHON:
            default_arguments.update({"--extra-py-files": working_set_s3_location})
    elif glue_version in ["2.0", "3.0"]:
        # with 2.0 cannot even use MaxCapacity
        capacity_params.update({"WorkerType": GlueWorkerType.G_1X.value})
        if job_command_type == GlueJobCommandType.BATCH:
            capacity_params.update({"NumberOfWorkers": 100})
        else:
            capacity_params.update({"NumberOfWorkers": 1})
        if job_language == GlueJobLanguage.PYTHON:
            default_arguments.update({"--extra-py-files": working_set_s3_location})
    else:
        raise ValueError(f"Unsupported glue_version: {glue_version!r}.")

    params = {
        "Description": description,
        "Role": role,
        "ExecutionProperty": {"MaxConcurrentRuns": max_concurrent_runs},
        "Command": {"Name": job_command_type.value, "ScriptLocation": script_s3_location, "PythonVersion": "3"},
        "DefaultArguments": default_arguments,
        # RheocerOS controls the retry logic, so implicit retries should be avoided!
        "MaxRetries": 0,
        "Timeout": 600,  # 60 mins x 10 = 10 hours
        "NotificationProperty": {"NotifyDelayAfter": 123},
        "GlueVersion": glue_version,
    }

    params.update(capacity_params)

    return params


def create_glue_job(
    glue_client,
    glue_job_name,
    description,
    role,
    job_command_type: GlueJobCommandType,
    job_language: GlueJobLanguage,
    script_s3_location,
    max_concurrent_runs=20,
    max_capacity_in_DPU=None,
    # https://docs.aws.amazon.com/glue/latest/dg/add-job.html
    glue_version=None,
    working_set_s3_location=None,
    default_args: Dict[str, Any] = None,
):

    job_params = _create_job_params(
        description,
        role,
        job_command_type,
        job_language,
        script_s3_location,
        max_concurrent_runs,
        max_capacity_in_DPU,
        glue_version,
        working_set_s3_location,
        default_args,
    )

    job_params.update({"Name": glue_job_name})

    try:
        response = glue_client.create_job(**job_params)
        job_name = response["Name"]
    except ClientError:
        logger.exception("Couldn't create glue job %s.", glue_job_name)
        raise
    else:
        return job_name


def delete_glue_job(glue_client, glue_job_name):
    """Refer
    https://boto3.amazonaws.com/v1/documentation/api/1.9.42/reference/services/glue.html#Glue.Client.delete_job

    Note: If the job definition is not found, no exception is thrown.
    """
    try:
        glue_client.delete_job(JobName=glue_job_name)
    except ClientError:
        logger.exception("Couldn't delete glue job %s.", glue_job_name)
        raise


def update_glue_job(
    glue_client,
    glue_job_name,
    description,
    role,
    job_command_type: GlueJobCommandType,
    job_language: GlueJobLanguage,
    script_s3_location,
    max_concurrent_runs=20,
    max_capacity_in_DPU=None,
    # https://docs.aws.amazon.com/glue/latest/dg/add-job.html
    glue_version=None,
    working_set_s3_location=None,
    default_args: Dict[str, Any] = None,
):

    job_update_params = _create_job_params(
        description,
        role,
        job_command_type,
        job_language,
        script_s3_location,
        max_concurrent_runs,
        max_capacity_in_DPU,
        glue_version,
        working_set_s3_location,
        default_args,
    )

    job_params = {"JobName": glue_job_name, "JobUpdate": job_update_params}

    try:
        response = glue_client.update_job(**job_params)
        job_name = response["JobName"]
    except ClientError:
        logger.exception("Couldn't update glue job %s.", job_name)
        raise
    else:
        return job_name


def evaluate_execution_params(
    job_command: GlueJobCommandType,
    job_lang: GlueJobLanguage,
    org_params: Dict[str, Any],
    fail_on_misconfiguration=False,
    keep_unrecognized_params=True,
) -> Dict[str, Any]:
    # Convenience method to catch invalid params early in development or to be more tolerant against type mismatches (by doing
    # necessary conversions for compatible values before the actual API call [not relying on boto3's internal impl).
    #
    # This is generally not a good practice since these checks are creating a coupling with Glue backend. So, some of the checks
    # and type conversions are ignored to avoid further coupling.
    #
    # The ones addressed here are for better customer/developer exp and due to complicated/hidden parametrization of Glue jobs.
    # Please check the impl below to understand more.
    #
    # Warning: These are job run parameters not the sub "Arguments" which is also a job run parameter
    # of type Dict[str, str].
    # ref
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glue.html#Glue.Client.start_job_run

    params = dict(org_params) if keep_unrecognized_params else dict()

    glue_version = org_params.get("GlueVersion")

    if glue_version == "1.0":
        max_capacity = None
        if "MaxCapacity" in org_params:
            if job_command == GlueJobCommandType.BATCH:
                max_capacity = int(org_params["MaxCapacity"])
                if fail_on_misconfiguration and (max_capacity < 2 or max_capacity > 100):
                    raise ValueError(f"MaxCapacity for {job_command} is not defined correctly! Valid range is between 2 and 100")
                if max_capacity < 2:
                    max_capacity = 2
                if max_capacity > 100:
                    max_capacity = 100
            elif job_command == GlueJobCommandType.NORMAL:
                max_capacity = float(org_params["MaxCapacity"])
                if math.isclose(max_capacity, 0.0625):
                    max_capacity = 0.0625
                elif math.isclose(max_capacity, 1.0):
                    max_capacity = 1.0
                else:
                    if fail_on_misconfiguration:
                        raise ValueError(f"MaxCapacity for {job_command} is not defined correctly! Valid values are either 0.0625 or 1.0")

            if max_capacity:
                params.update({"MaxCapacity": float(max_capacity)})

        if fail_on_misconfiguration and max_capacity and ("WorkerType" in org_params or "NumberOfWorkers" in org_params):
            raise ValueError(f"Do not set Max Capacity for an AWS Glue Job if using WorkerType and NumberOfWorkers.")
    elif glue_version in ["2.0", "3.0"]:
        if fail_on_misconfiguration and ("WorkerType" not in org_params or "NumberOfWorkers" not in org_params):
            raise ValueError(f"AWS Glue Version {glue_version} jobs require 'WorkerType' and 'NumberOfWorkers' to be defined.")
    else:
        raise ValueError(f"Unsupported glue_version: {glue_version!r}.")

    if "Timeout" in org_params:
        time_out = int(org_params["Timeout"])
        if fail_on_misconfiguration and time_out < 1:
            raise ValueError(f"Timeout value {time_out} for AWS Glue job is too low or not valid!")
        params.update({"Timeout": time_out})

    if "WorkerType" in org_params:
        worker_type = org_params["WorkerType"]
        # if not any([worker_type == type.value for type in GlueWorkerType]):
        if worker_type not in GlueWorkerType.__members__.values():
            raise ValueError(
                f"WorkerType value {worker_type!r} for AWS Glue job is not valid!" f" Valid values: {GlueWorkerType.__members__.values()}"
            )
        params.update({"WorkerType": worker_type})

    if "NumberOfWorkers" in org_params:
        number_of_workers = int(org_params["NumberOfWorkers"])
        if fail_on_misconfiguration:
            if number_of_workers < 1:
                raise ValueError("NumberOfWorkers value '{number_of_workers}' is not valid for AWS Glue Job!")

            if org_params["WorkerType"] == GlueWorkerType.G_1X.value and number_of_workers > 299:
                raise ValueError(
                    f"NumberOfWorkers value '{number_of_workers}' is out of bounds for AWS Glue Job. "
                    f"The maximum number of workers you can define are 299 for G.1X."
                )
            elif org_params["WorkerType"] == GlueWorkerType.G_2X.value and number_of_workers > 149:
                raise ValueError(
                    f"NumberOfWorkers value '{number_of_workers}' is out of bounds for AWS Glue Job. "
                    f"The maximum number of workers you can define are 149 for G.2X."
                )

        params.update({"NumberOfWorkers": number_of_workers})

    return params


def start_glue_job(
    glue_client,
    job_command: GlueJobCommandType,
    job_lang: GlueJobLanguage,
    job_name,
    args: Dict[str, str],
    extra_params: Dict[str, Any],
    prev_job_run_id: str = None,
):
    kwargs = dict()
    kwargs.update({"JobName": job_name})
    kwargs.update({"Arguments": args})
    if prev_job_run_id:  # enables retry on prev run
        kwargs.update({"JobRunId": prev_job_run_id})

    new_extra_params = evaluate_execution_params(job_command, job_lang, extra_params, False, False)

    kwargs.update(new_extra_params)

    try:
        response = glue_client.start_job_run(**kwargs)
        job_id = response["JobRunId"]
    except ClientError:
        logger.exception("Couldn't create glue job %s.", job_name)
        raise
    else:
        return job_id


def get_glue_job(glue_client, job_name):
    try:
        response = glue_client.get_job(JobName=job_name)
        return response["Job"]["Name"]
    except ClientError as ex:
        # TODO
        if ex.response["Error"]["Code"] == "EntityNotFoundException":
            return None
        logger.error("Couldn't check glue job '%s'! Error: %s", job_name, str(ex))
        raise


def get_glue_job_run(glue_client, job_name, job_run_id):
    try:
        response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id, PredecessorsIncluded=False)
        job_run = response["JobRun"]
    except ClientError:
        logger.exception("Couldn't get glue run job run [job_name: %s, run_id: %s].", job_name, job_run_id)
        raise
    else:
        return job_run


def get_glue_job_run_state_type(job_run) -> ComputeSessionStateType:
    run_state = job_run["JobRunState"]
    if run_state in ["STARTING", "RUNNING"]:
        return ComputeSessionStateType.PROCESSING
    elif run_state in ["SUCCEEDED"]:
        return ComputeSessionStateType.COMPLETED
    elif run_state in ["STOPPING", "STOPPED", "FAILED", "TIMEOUT"]:
        return ComputeSessionStateType.FAILED
    else:
        logger.critical(
            f"AWS Glue introduced a new state type {run_state}!"
            f" Marking it as {ComputeSessionStateType.UNKNOWN}. "
            f" This should be addressed ASAP by RheocerOS Core,"
            f" or your app should upgrade to a newer RheocerOS version."
        )
        return ComputeSessionStateType.UNKNOWN


def get_glue_job_run_failure_type(job_run) -> ComputeFailedSessionStateType:
    run_state = job_run["JobRunState"]
    if run_state in ["STOPPING", "STOPPED"]:
        return ComputeFailedSessionStateType.STOPPED
    elif run_state in ["TIMEOUT"]:
        return ComputeFailedSessionStateType.TIMEOUT
    elif run_state in ["FAILED"]:
        error_message = job_run["ErrorMessage"]
        # TODO AWS Glue should provide better support for concurrent capacity exceeding
        # the account limit. Or we should find it if there is a better way.
        if "Exceeded maximum concurrent compute capacity" in error_message or "Resource unavailable" in error_message:
            return ComputeFailedSessionStateType.TRANSIENT
        else:
            return ComputeFailedSessionStateType.APP_INTERNAL
    else:
        logger.critical(
            f"AWS Glue introduced a new state type {run_state}!"
            f" Marking it as {ComputeFailedSessionStateType.UNKNOWN}. "
            f" This should be addressed ASAP by RheocerOS Core,"
            f" or your app should upgrade to a newer RheocerOS version."
        )
        return ComputeFailedSessionStateType.UNKNOWN


def get_bundles(glue_version: str) -> List[Tuple[str, Path]]:
    from importlib.resources import contents, path

    bundles = []
    if glue_version in ["1.0", "2.0", "3.0"]:
        from .lib import v1_0 as bundle

        for resource in contents(bundle):
            with path(bundle, resource) as resource_path:
                if Path(resource_path).suffix.lower() == ".jar":
                    bundles.append((resource, Path(resource_path)))
    return bundles
