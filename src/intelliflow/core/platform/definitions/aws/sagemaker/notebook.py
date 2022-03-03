# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import codecs
import logging
from typing import Any, ClassVar, Dict, Optional

from botocore.exceptions import ClientError

from intelliflow.core.platform.endpoint import DevEndpoint, DevEndpointType
from intelliflow.core.runtime import PYTHON_VERSION_MAJOR, PYTHON_VERSION_MINOR

from ..common import CommonParams as AWSCommonParams
from ..common import exponential_retry
from .common import (
    SAGEMAKER_COMMON_RETRYABLE_ERRORS,
    SAGEMAKER_NOTEBOOK_INSTANCE_IF_KERNEL_NAME,
    SAGEMAKER_NOTEBOOK_INSTANCE_IF_MINICONDA_NAME,
    get_sagemaker_notebook_instance_pkg_root,
)

logger = logging.getLogger(__name__)


class LifeCycleConfiguration:
    def __init__(self, instance_name: str, bundle_uri: str) -> None:
        self._name = instance_name + "-lifecycle"
        self._bundle_uri = bundle_uri
        if not self._bundle_uri.startswith("s3://"):
            raise NotImplementedError(
                f"Bundle storage {self._bundle_uri} is not supported for sync into an RheocerOS"
                f" provisioned Sagemaker notebook instance."
            )
        self._start_script = self._generate_start_script()
        self._create_script = self._generate_creation_script()

    @property
    def name(self) -> str:
        return self._name

    def _generate_start_script(self) -> str:
        start_script: str = f"""#!/bin/bash

set -e

# OVERVIEW
# This script bootstraps RheocerOS environment for a Sagemaker notebook instance.

sudo -u ec2-user -i <<'EOF'
unset SUDO_UID

WORKING_DIR=/home/ec2-user/SageMaker/{SAGEMAKER_NOTEBOOK_INSTANCE_IF_MINICONDA_NAME}/
source "$WORKING_DIR/miniconda/bin/activate"

for env in $WORKING_DIR/miniconda/envs/*; do
    BASENAME=$(basename "$env")
    source activate "$BASENAME"

    #pip install --quiet --upgrade boto3
    #pip install --quiet --upgrade dill
    # TODO required for pandas PARQUET load operation but causing deployment
    #  bundle to exceed AWS lambda size during activation in current driver conf. 
    #pip install --quiet --upgrade pyarrow

    python -m ipykernel install --user --name "$BASENAME" --display-name "$BASENAME"
done


# Optionally, uncomment these lines to disable SageMaker-provided Conda functionality.
# echo 'c.EnvironmentKernelSpecManager.use_conda_directly = False' >> /home/ec2-user/.jupyter/jupyter_notebook_config.py
# rm /home/ec2-user/.condarc

# Most important line in this configuration: sync the notebook instance with the bundle of this application
aws s3 cp {self._bundle_uri} {get_sagemaker_notebook_instance_pkg_root(PYTHON_VERSION_MAJOR, PYTHON_VERSION_MINOR)}

cd {get_sagemaker_notebook_instance_pkg_root(PYTHON_VERSION_MAJOR, PYTHON_VERSION_MINOR)}
unzip -o bundle.zip

# clean-up
rm -f bundle.zip
rm -rf rip_python_helper

EOF

echo 'Restarting the Jupyter server..'
#restart jupyter-server
sudo initctl restart jupyter-server --no-wait
"""
        return start_script

    def _generate_creation_script(self) -> str:
        # safety belt: this will raise if an upgrade in IF (like into 3.8+) will ignore this module.
        miniconda_url = {"3.7": "https://repo.anaconda.com/miniconda/Miniconda3-py37_4.8.3-Linux-x86_64.sh"}[
            f"{PYTHON_VERSION_MAJOR}.{PYTHON_VERSION_MINOR}"
        ]

        create_script = f"""
#!/bin/bash

set -e

# OVERVIEW
# This script installs a custom, persistent installation of RheocerOS compatible/tested Conda on the Notebook Instance's EBS volume, and ensures
# that these custom environments are available as kernels in Jupyter.
# 
# The on-create script downloads and installs a custom conda installation to the EBS volume via Miniconda. Any relevant
# packages can be installed here.
#   1. ipykernel is installed to ensure that the custom environment can be used as a Jupyter kernel   
#   2. Ensure the Notebook Instance has internet connectivity to download the Miniconda installer

sudo -u ec2-user -i <<'EOF'
unset SUDO_UID

# Install a separate conda installation via Miniconda
WORKING_DIR=/home/ec2-user/SageMaker/{SAGEMAKER_NOTEBOOK_INSTANCE_IF_MINICONDA_NAME}
mkdir -p "$WORKING_DIR"
wget {miniconda_url} -O "$WORKING_DIR/miniconda.sh"
bash "$WORKING_DIR/miniconda.sh" -b -u -p "$WORKING_DIR/miniconda" 
rm -rf "$WORKING_DIR/miniconda.sh"


# Create a custom conda environment
source "$WORKING_DIR/miniconda/bin/activate"
KERNEL_NAME="{SAGEMAKER_NOTEBOOK_INSTANCE_IF_KERNEL_NAME}"
PYTHON="{PYTHON_VERSION_MAJOR}.{PYTHON_VERSION_MINOR}"

conda create --yes --name "$KERNEL_NAME" python="$PYTHON"
conda activate "$KERNEL_NAME"

pip install --quiet ipykernel

# Customize these lines as necessary to install the required packages
conda install --yes numpy
conda install --yes pandas
#pip install --quiet boto3

# To install the full Anaconda distribution:
# nohup conda install --yes anaconda

EOF
"""
        return create_script

    def provision(self, sagemaker) -> str:
        """Typical Sagemaker notebook instance provisioning with a dedicated life-cycle configuration.

        Life-cycle configuration exploits all of the distinctive advantages of RheocerOS' cross-platform and tested
        compatibility with chosen miniconda env here.
        Refer './doc/DEVELOPMENT' doc for a similar PyCharm + Miniconda quick setup for RheocerOS development.

        TODO / FUTURE integ-tests will have notebook execution as well to make sure that specific miniconda compatibility
        is captured in the release pipeline.
        """

        # Error Handling:
        #  https://docs.aws.amazon.com/sagemaker/latest/APIReference/CommonErrors.html

        response = None
        try:
            response = exponential_retry(
                sagemaker.describe_notebook_instance_lifecycle_config,
                SAGEMAKER_COMMON_RETRYABLE_ERRORS,
                NotebookInstanceLifecycleConfigName=self._name,
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "ValidationException":
                raise error

        if response and "NotebookInstanceLifecycleConfigArn" in response:
            self._arn = response["NotebookInstanceLifecycleConfigArn"]
            logger.info(f"Notebook instance lifecycle config {self._arn} already exists. Updating...")
            exponential_retry(
                sagemaker.update_notebook_instance_lifecycle_config,
                SAGEMAKER_COMMON_RETRYABLE_ERRORS,
                NotebookInstanceLifecycleConfigName=self.name,
                OnCreate=[
                    {"Content": codecs.encode(self._create_script.encode(), "base64").decode()},
                ],
                OnStart=[
                    {"Content": codecs.encode(self._start_script.encode(), "base64").decode()},
                ],
            )
        else:
            response = exponential_retry(
                sagemaker.create_notebook_instance_lifecycle_config,
                SAGEMAKER_COMMON_RETRYABLE_ERRORS,
                NotebookInstanceLifecycleConfigName=self.name,
                OnCreate=[
                    {"Content": codecs.encode(self._create_script.encode(), "base64").decode()},
                ],
                OnStart=[
                    {"Content": codecs.encode(self._start_script.encode(), "base64").decode()},
                ],
            )
            self._arn = response["NotebookInstanceLifecycleConfigArn"]
            logger.info(f"New notebook instance lifecycle config {self._arn} is successfully created!")

        return self._arn


class Instance(DevEndpoint):
    DEFAULT_INSTANCE_TYPE: ClassVar[str] = "ml.t3.medium"
    DEFAULT_VOLUME_SIZE_IN_GB: ClassVar[int] = 50

    def __init__(self, context_id: str, conf_params: Dict[str, Any], bundle_uri=None, attrs: Dict[str, Any] = dict()) -> None:
        super().__init__(DevEndpointType.SAGEMAKER_NOTEBOOK_INSTANCE, context_id + "-notebook", attrs)
        self._session = conf_params[AWSCommonParams.BOTO_SESSION]
        self._region = conf_params[AWSCommonParams.REGION]
        self._account_id = conf_params[AWSCommonParams.ACCOUNT_ID]
        self._dev_role = conf_params[AWSCommonParams.IF_DEV_ROLE]
        self._arn: str = None
        self._sagemaker = self._session.client(service_name="sagemaker", region_name=self._region)
        self._life_cycle_conf = LifeCycleConfiguration(self.name, bundle_uri) if bundle_uri else None

    def sync(self) -> None:
        response = exponential_retry(
            self._sagemaker.describe_notebook_instance, SAGEMAKER_COMMON_RETRYABLE_ERRORS, NotebookInstanceName=self.name
        )

        self._attrs.update(response)

    def provision(self) -> None:
        self._life_cycle_conf.provision(self._sagemaker)

        # Error Handling:
        #  https://docs.aws.amazon.com/sagemaker/latest/APIReference/CommonErrors.html

        response = None
        try:
            response = exponential_retry(
                self._sagemaker.describe_notebook_instance, SAGEMAKER_COMMON_RETRYABLE_ERRORS, NotebookInstanceName=self.name
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "ValidationException":
                raise error

        if response and "NotebookInstanceArn" in response:
            self._arn = response["NotebookInstanceArn"]

            logger.critical(f"Notebook instance {self._arn} already exists. Updating...")
            was_already_in_service: bool = False
            if response["NotebookInstanceStatus"] == "InService":
                was_already_in_service = True
                # update cannot happen while in service
                logger.critical("Notebook is InService. Stopping before initiating the update...")
                self.stop(True)
            elif response["NotebookInstanceStatus"] not in ["Stopped", "Failed"]:
                raise RuntimeError(
                    f"Bad notebook state! Notebook instance is in {response['NotebookInstanceStatus']!r} state. "
                    f"RheocerOS can initiate the update only when the state is among 'InService, Stopped, Failed'"
                )

            exponential_retry(
                self._sagemaker.update_notebook_instance,
                SAGEMAKER_COMMON_RETRYABLE_ERRORS,
                NotebookInstanceName=self.name,
                InstanceType=self._attrs.get("InstanceType", response["InstanceType"]),
                RoleArn=self._dev_role,
                LifecycleConfigName=self._life_cycle_conf.name,
                # DisassociateLifecycleConfig=True,
                VolumeSizeInGB=self._attrs.get("VolumeSizeInGB", response["VolumeSizeInGB"]),
                RootAccess="Enabled",
            )

            # use the stop waiter to check the transition from 'Updating' -> 'Stopped'
            waiter = self._sagemaker.get_waiter("notebook_instance_stopped")
            waiter.wait(NotebookInstanceName=self.name)

            # check the state to make sure that it is not 'Failed'
            response = exponential_retry(
                self._sagemaker.describe_notebook_instance, SAGEMAKER_COMMON_RETRYABLE_ERRORS, NotebookInstanceName=self.name
            )

            if response["NotebookInstanceStatus"] == "Stopped":
                logger.critical(f"Notebook instance {self._arn} is successfully updated!")
            else:
                # raise early to capture failures in this context. alternative is bad customer exp or inability to
                # capture failures in automated pipelines.
                raise RuntimeError(f"Notebook instance update resulted in {response['NotebookInstanceStatus']!r} state.")

            """
            if was_already_in_service:
                logger.critical(f"Restoring the instance state back to 'InService'")
                self.start(True)
            """
        else:
            logger.critical(f"Notebook instance {self.name} does not exist. Creating...")
            try:
                response = exponential_retry(
                    self._sagemaker.create_notebook_instance,
                    SAGEMAKER_COMMON_RETRYABLE_ERRORS,
                    NotebookInstanceName=self.name,
                    InstanceType=self._attrs.get("InstanceType", self.DEFAULT_INSTANCE_TYPE),
                    RoleArn=self._dev_role,
                    Tags=[{"Key": key, "Value": str(self.attrs[key])} for key in self._attrs],
                    LifecycleConfigName=self._life_cycle_conf.name,
                    DirectInternetAccess="Enabled",
                    VolumeSizeInGB=self._attrs.get("VolumeSizeInGB", self.DEFAULT_VOLUME_SIZE_IN_GB),
                    # AcceleratorTypes=[
                    #'ml.eia1.medium' | 'ml.eia1.large' | 'ml.eia1.xlarge' | 'ml.eia2.medium' | 'ml.eia2.large' | 'ml.eia2.xlarge',
                    # ],
                    RootAccess="Enabled",
                )
            except ClientError as error:
                if error.response["Error"]["Code"] in ["ResourceLimitExceeded"]:
                    logger.error(
                        f"A Sagemaker resource limit is hit in this account {self._account_id}."
                        f"Defails: {error.response['Error']['Message']!r}. "
                        f"Please request increase on this limit and try again. "
                        f"\n"
                        f"If it is related to 'InstanceType', then you might alternatively want to switch to a different type among: "
                        f"https://docs.aws.amazon.com/sagemaker/latest/dg/notebooks-available-instance-types.html"
                    )
                raise

            self._arn = response["NotebookInstanceArn"]

            # use the stop waiter to check the transition from 'Updating' -> 'InService'
            waiter = self._sagemaker.get_waiter("notebook_instance_in_service")
            waiter.wait(NotebookInstanceName=self.name)

            # check the state to make sure that it is not 'Failed'
            response = exponential_retry(
                self._sagemaker.describe_notebook_instance, SAGEMAKER_COMMON_RETRYABLE_ERRORS, NotebookInstanceName=self.name
            )

            if response["NotebookInstanceStatus"] == "InService":
                logger.critical(f"Notebook instance {self._arn} is successfully created and active now!")
            else:
                # if response['NotebookInstanceStatus'] != 'Pending':
                # raise early to capture failures in this context. alternative is bad customer exp or inability to
                # capture failures in automated pipelines.
                raise RuntimeError(f"Notebook instance was created with bad state {response['NotebookInstanceStatus']!r}.")
            # else:
            # logger.critical("Wait time on InService check was probably not enough."
            #                " Please check active state on this instance.")

        self._set_uri(self._arn)

    # overrides
    def is_active(self) -> bool:
        response = self._sagemaker.describe_notebook_instance(NotebookInstanceName=self.name)

        if response and "NotebookInstanceArn" in response:
            self._arn = response["NotebookInstanceArn"]
            return True if response["NotebookInstanceStatus"] == "InService" else False

    # overrides
    def start(self, wait_till_active=False) -> None:
        self._sagemaker.start_notebook_instance(NotebookInstanceName=self.name)
        if wait_till_active:
            logger.critical(f"Waiting for {self.name} to start...")
            waiter = self._sagemaker.get_waiter("notebook_instance_in_service")
            waiter.wait(NotebookInstanceName=self.name, WaiterConfig={"Delay": 15, "MaxAttempts": 90})  # default is 30  # default is 60
            logger.critical(f"{self.name} is in service now.")

    # overrides
    def stop(self, wait_till_inactive=False) -> None:
        exponential_retry(self._sagemaker.stop_notebook_instance, SAGEMAKER_COMMON_RETRYABLE_ERRORS, NotebookInstanceName=self.name)

        if wait_till_inactive:
            logger.critical(f"Waiting for {self.name} to stop...")
            waiter = self._sagemaker.get_waiter("notebook_instance_stopped")
            waiter.wait(NotebookInstanceName=self.name)
            logger.critical(f"{self.name} has stopped!")

    def generate_url(self, session_duration_in_secs: Optional[int] = 3600) -> str:
        """Generate a pre-signed URL for the notebook instance.

        Returned URL should be used within 5 minutes, othwerwise a redirection to AWS Console will happen.
        """
        response = self._sagemaker.create_presigned_notebook_instance_url(
            NotebookInstanceName=self._name, SessionExpirationDurationInSeconds=session_duration_in_secs
        )
        return response["AuthorizedUrl"]
