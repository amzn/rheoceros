# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime
import logging

import re
from enum import Enum, unique
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Union

from botocore.exceptions import ClientError
from packaging import version
from packaging.version import Version

from intelliflow.core.platform.definitions.aws.common import MAX_SLEEP_INTERVAL_PARAM, exponential_retry, get_code_for_exception
from intelliflow.core.platform.definitions.aws.glue.client_wrapper import GlueVersion, GlueWorkerType, glue_spark_version_map
from intelliflow.core.platform.definitions.aws.s3.object_wrapper import build_object_key
from intelliflow.core.platform.definitions.compute import ComputeFailedSessionStateType, ComputeSessionStateType
from intelliflow.core.signal_processing.definitions.compute_defs import Lang
from intelliflow.utils.algorithm import chunk_iter

logger = logging.getLogger(__name__)


@unique
class EmrJobLanguage(str, Enum):
    PYTHON = "py"
    SCALA = "scala"

    def __init__(self, extension="unknown"):
        self._extension = extension

    @property
    def extension(self) -> str:
        return self._extension

    @classmethod
    def from_slot_lang(cls, lang: Lang):
        if lang in [Lang.PYTHON, Lang.SPARK_SQL]:
            return cls.PYTHON
        elif lang == Lang.SCALA:
            return cls.SCALA
        else:
            raise ValueError(f"Slot lang '{lang!r}' is not supported by AWS EMR!")


@unique
class EmrReleaseLabel(Enum):
    """
    Versions have to be placed in ASC order
    """

    AUTO = None, None, None
    VERSION_5_12_3 = version.parse("5.12.3"), version.parse("2.2.1"), version.parse("2.8.3")
    VERSION_5_36_0 = version.parse("5.36.0"), version.parse("2.4.8"), version.parse("2.10.1")
    VERSION_6_3_1 = version.parse("6.3.1"), version.parse("3.1.1"), version.parse("3.2.1")
    VERSION_6_4_0 = version.parse("6.4.0"), version.parse("3.1.2"), version.parse("3.2.1")

    def __init__(self, emr_version: Version, spark_version: Version, hadoop_version: Version):
        """
        EMR release label with corresponding software versions
        https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-6.x.html
        https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-app-versions-5.x.html
        """
        self._emr_version = emr_version
        self._spark_version = spark_version
        self._hadoop_version = hadoop_version

    @property
    def display_name(self) -> str:
        return self._emr_version.__str__()

    @property
    def extension(self) -> str:
        return f"emr_{self._emr_version.major}_{self._emr_version.minor}_{self._emr_version.patch}"

    @property
    def aws_label(self) -> str:
        """
        AWS EMR release label format: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html
        """
        return f"emr-{self.display_name}"

    @property
    def spark_version(self) -> Version:
        return self._spark_version

    @property
    def hadoop_version(self) -> Version:
        return self._hadoop_version

    @classmethod
    def resolve_from_glue_version(cls, glue_version: GlueVersion) -> Optional["EmrReleaseLabel"]:
        """
        Find the lowest possible EMR release label to satisfy software versions from the input glue version
        """
        least_spark_version = glue_spark_version_map()[glue_version]
        if not least_spark_version:
            return None
        for release_label in EmrReleaseLabel:
            spark_version = release_label.spark_version
            if not spark_version:
                continue
            if least_spark_version <= spark_version:
                return release_label
        return None


def build_job_arn(region: str, account_id: str, job_id: str, partition: str = "aws") -> str:
    return f"arn:{partition}:elasticmapreduce:{region}:{account_id}:cluster/{job_id}"


def validate_job_name(job_name: str) -> bool:
    """
    EMR job name pattern from: https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html#EMR-RunJobFlow-request-Name
    """
    return re.compile(r"^[\u0020-\uD7FF\uE000-\uFFFD\uD800\uDBFF-\uDC00\uDFFF\r\n\t]*$").match(job_name) is not None


def get_emr_step(emr_client, cluster_id: str):
    try:
        res = emr_client.list_steps(ClusterId=cluster_id)
        steps = res["Steps"]
    except ClientError:
        raise
    else:
        # Assuming there's only one step in the cluster and the cluster will terminate once the step finishes
        if len(steps) != 1:
            raise ValueError(f"Cluster with id {cluster_id} doesn't have any step or it has more than 1 steps")
        return steps[0]


def get_emr_step_state_type(step_state: str) -> ComputeSessionStateType:
    if step_state in ["PENDING", "RUNNING"]:
        return ComputeSessionStateType.PROCESSING
    elif step_state in ["COMPLETED"]:
        return ComputeSessionStateType.COMPLETED
    elif step_state in ["CANCEL_PENDING", "CANCELLED", "FAILED", "INTERRUPTED"]:
        return ComputeSessionStateType.FAILED
    else:
        logger.critical(
            f"AWS Glue introduced a new state type {step_state}!"
            f" Marking it as {ComputeSessionStateType.UNKNOWN}. "
            f" This should be addressed ASAP by IntelliFlow Core,"
            f" or your app should upgrade to a newer IntelliFlow version."
        )
        return ComputeSessionStateType.UNKNOWN


def describe_emr_cluster(emr_client, cluster_id: str):
    return emr_client.describe_cluster(ClusterId=cluster_id)


def get_emr_cluster_state_type(cluster_status: Dict[str, Any]):
    state = cluster_status["State"]
    state_change_reason = cluster_status["StateChangeReason"]
    if state in ["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING", "TERMINATING"]:
        return ComputeSessionStateType.PROCESSING
    elif state in ["TERMINATED_WITH_ERRORS"]:
        return ComputeSessionStateType.FAILED
    elif state in ["TERMINATED"]:
        if state_change_reason["Code"] in ["ALL_STEPS_COMPLETED"]:
            return ComputeSessionStateType.COMPLETED
        else:
            return ComputeSessionStateType.FAILED
    else:
        logger.critical(
            f"AWS Glue introduced a new state type {state}!"
            f" Marking it as {ComputeSessionStateType.UNKNOWN}. "
            f" This should be addressed ASAP by IntelliFlow Core,"
            f" or your app should upgrade to a newer IntelliFlow version."
        )
        return ComputeSessionStateType.UNKNOWN


def get_emr_cluster_failure_type(cluster_status: Dict[str, Any]):
    state_change_reason = cluster_status["StateChangeReason"]
    code = state_change_reason["Code"]
    if code in ["USER_REQUEST"]:
        return ComputeFailedSessionStateType.STOPPED
    elif code in ["INTERNAL_ERROR", "VALIDATION_ERROR", "INSTANCE_FAILURE", "BOOTSTRAP_FAILURE", "STEP_FAILURE"]:
        msg = state_change_reason["Message"]
        if code == "INTERNAL_ERROR" and "exceeds the EC2 service quota for that type." in msg:
            return ComputeFailedSessionStateType.TRANSIENT
        elif code == "VALIDATION_ERROR" and "The EBS volume limit was exceeded" in msg:
            return ComputeFailedSessionStateType.TRANSIENT
        elif code == "VALIDATION_ERROR" and "The subnet is not large enough" in msg:
            return ComputeFailedSessionStateType.TRANSIENT
        elif code == "BOOTSTRAP_FAILURE" and "application provisioning failed" in msg:
            return ComputeFailedSessionStateType.TRANSIENT
        else:
            return ComputeFailedSessionStateType.APP_INTERNAL
    else:
        logger.critical(
            f"AWS EMR introduced a new state type {code}!"
            f" Marking it as {ComputeFailedSessionStateType.UNKNOWN}. "
            f" This should be addressed ASAP by IntelliFlow Core,"
            f" or your app should upgrade to a newer IntelliFlow version."
        )
        return ComputeFailedSessionStateType.UNKNOWN


def translate_glue_worker_type(worker_type: Union[GlueWorkerType, str]) -> str:
    return {
        "STANDARD": "m5.large",
        "G.1X": "m5.xlarge",
        "G.2X": "m5.2xlarge",
    }[worker_type.value if isinstance(worker_type, GlueWorkerType) else worker_type]


def start_emr_job_flow(
    emr_client: Any,
    job_name: str,
    emr_release_label: str,
    log_path: str,
    applications: List[str],
    emr_cli_args: List[str],
    if_exe_role_arn: str,
    emr_configurations: List,
    emr_instances_specs: Dict[str, Any],
    security_config: str,
    bootstrap_actions: Optional[List] = None,
):

    step_params = [
        {"Name": job_name, "ActionOnFailure": "TERMINATE_CLUSTER", "HadoopJarStep": {"Jar": "command-runner.jar", "Args": emr_cli_args}},
    ]

    instance_profile_arn = if_exe_role_arn.replace(":role/", ":instance-profile/")
    kwargs = {
        "Name": job_name,
        "LogUri": log_path,
        "ReleaseLabel": emr_release_label,
        "Instances": emr_instances_specs,
        "Steps": step_params,
        "Applications": [{"Name": app} for app in applications],
        "JobFlowRole": instance_profile_arn,
        "ServiceRole": if_exe_role_arn,
        "VisibleToAllUsers": True,
        "Configurations": emr_configurations,
    }
    if security_config:
        kwargs.update({"SecurityConfiguration": security_config})
    if bootstrap_actions:
        kwargs.update({"BootstrapActions": bootstrap_actions})
    try:
        response = emr_client.run_job_flow(**kwargs)
        cluster_id = response["JobFlowId"]
    except ClientError:
        logger.exception("Couldn't run EMR job flow %s.", job_name)
        raise
    else:
        return cluster_id


# TODO: move to a separate file for SPOT market bidding
def build_capacity_params(instance_config):
    return {
        "InstanceGroups": [
            {"InstanceRole": "MASTER", "InstanceType": instance_config.instance_type, "Market": "ON_DEMAND", "InstanceCount": 1},
            {
                "InstanceRole": "CORE",
                "InstanceType": instance_config.instance_type,
                "InstanceCount": instance_config.instance_count - 1,
                # TODO: future options: ON_DEMAND | SPOT | AUTO
                "Market": "ON_DEMAND",
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
    }


def build_glue_catalog_configuration(account_id: Optional[str] = None):
    glue_catalog_id_key = "hive.metastore.glue.catalogid"
    config = {
        "Classification": "spark-hive-site",
        "Properties": {
            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            # TODO support cross account glue catalog
            # https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-glue.html
            glue_catalog_id_key: account_id,
        },
    }
    if not account_id:
        config["Properties"].pop(glue_catalog_id_key, None)
    return config


def list_emr_clusters(
    emr_client,
    **kwargs,
) -> Iterator[str]:
    try:
        paginator = emr_client.get_paginator("list_clusters")
        response_iterator = paginator.paginate(**kwargs)
        for res in response_iterator:
            for cluster in res["Clusters"]:
                yield cluster
    except ClientError as e:
        logger.exception(f"Couldn't list emr clusters with kwargs: {kwargs}")
        raise


def terminate_emr_job_flow(emr_client, job_flow_ids: List[str]) -> Dict[Any, Any]:
    if len(job_flow_ids) > 10:
        raise ValueError(f"EMR terminate_emr_job_flow only support <= 10 clusters, got {len(job_flow_ids)} items instead")
    return exponential_retry(
        emr_client.terminate_job_flows, ["InternalServerError"], **{MAX_SLEEP_INTERVAL_PARAM: 16, "JobFlowIds": job_flow_ids}
    )


def create_job_flow_instance_profile(iam_client, if_exec_role):
    try:
        create_instance_profile_res = exponential_retry(
            iam_client.create_instance_profile,
            ["AccessDenied", "LimitExceededException", "ConcurrentModificationException", "ServiceFailureException"],
            InstanceProfileName=if_exec_role,
        )
        exponential_retry(
            iam_client.add_role_to_instance_profile,
            ["LimitExceededException", "ServiceFailureException"],
            InstanceProfileName=if_exec_role,
            RoleName=if_exec_role,
        )
        return create_instance_profile_res
    except ClientError as err:
        if "EntityAlreadyExists" not in get_code_for_exception(err):
            logger.exception("Couldn't create instance profile %s.", if_exec_role)
            raise


def delete_instance_profile(iam_client, if_exec_role):
    try:
        exponential_retry(
            iam_client.remove_role_from_instance_profile,
            ["LimitExceededException", "ServiceFailureException"],
            InstanceProfileName=if_exec_role,
            RoleName=if_exec_role,
        )
        exponential_retry(
            iam_client.delete_instance_profile,
            ["LimitExceededException", "ServiceFailureException", "DeleteConflictException"],
            InstanceProfileName=if_exec_role,
        )
    except ClientError as err:
        if "NoSuchEntity" not in get_code_for_exception(err):
            logger.exception("Couldn't delete instance profile %s.", if_exec_role)
            raise

