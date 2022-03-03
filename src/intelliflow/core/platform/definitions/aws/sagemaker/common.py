# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# refer https://docs.aws.amazon.com/sagemaker/latest/APIReference/CommonErrors.html
SAGEMAKER_COMMON_RETRYABLE_ERRORS = ["ServiceUnavailable", "InternalFailure", "ThrottlingException"]

SAGEMAKER_NOTEBOOK_INSTANCE_ASSUMED_ROLE_USER: str = "SageMaker"

SAGEMAKER_NOTEBOOK_INSTANCE_IF_MINICONDA_NAME: str = "intelliflow-miniconda"

SAGEMAKER_NOTEBOOK_INSTANCE_IF_KERNEL_NAME: str = "RheocerOS"

_SAGEMAKER_NOTEBOOK_INSTANCE_IF_ENV_PACKAGE_ROOT_FORMAT: str = "/home/ec2-user/SageMaker/{}/miniconda/envs/{}/lib/python{}.{}/site-packages"


def get_sagemaker_notebook_instance_pkg_root(python_major_ver: int, python_minor_ver) -> str:
    return _SAGEMAKER_NOTEBOOK_INSTANCE_IF_ENV_PACKAGE_ROOT_FORMAT.format(
        SAGEMAKER_NOTEBOOK_INSTANCE_IF_MINICONDA_NAME, SAGEMAKER_NOTEBOOK_INSTANCE_IF_KERNEL_NAME, python_major_ver, python_minor_ver
    )
