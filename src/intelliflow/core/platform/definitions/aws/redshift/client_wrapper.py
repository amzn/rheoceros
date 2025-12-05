# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import re
import time
from typing import Any, Dict, List, Optional, Sequence, Set, Union

import boto3
from botocore.exceptions import ClientError

from ..common import exponential_retry
from ..s3.bucket_wrapper import get_bucket

module_logger = logging.getLogger(__name__)


class RedshiftServerlessManager:
    """Manager class for Redshift Serverless operations."""

    def __init__(self, session: boto3.Session, region: str):
        self._session = session
        self._region = region
        self._redshift_serverless = session.client("redshift-serverless", region_name=region)
        self._redshift_data = session.client("redshift-data", region_name=region)
        self._ec2 = session.resource("ec2", region_name=region)

    @staticmethod
    def build_workgroup_name(unique_context_id: str) -> str:
        """Build Redshift serverless workgroup name.
        Workgroup names must be 3-64 characters long and can contain alphanumeric characters and hyphens.
        """
        workgroup_name = f"if-{unique_context_id.lower()}"
        # Ensure name meets Redshift serverless workgroup naming requirements
        workgroup_name = re.sub(r"[^a-z0-9-]", "-", workgroup_name)
        if len(workgroup_name) > 64:
            workgroup_name = workgroup_name[:64]
        return workgroup_name

    @staticmethod
    def build_namespace_name(unique_context_id: str) -> str:
        """Build Redshift serverless namespace name.
        Namespace names must be 3-64 characters long and can contain alphanumeric characters and hyphens.
        """
        namespace_name = f"if-ns-{unique_context_id.lower()}"
        # Ensure name meets Redshift serverless namespace naming requirements
        namespace_name = re.sub(r"[^a-z0-9-]", "-", namespace_name)
        if len(namespace_name) > 64:
            namespace_name = namespace_name[:64]
        return namespace_name

    @staticmethod
    def build_workgroup_arn(region: str, account_id: str, workgroup_name: str) -> str:
        """Build Redshift serverless workgroup ARN."""
        return f"arn:aws:redshift-serverless:{region}:{account_id}:workgroup/{workgroup_name}"

    def get_workgroup_endpoint(self, workgroup_name: str, account_id: Optional[str] = None) -> str:
        """Get Redshift serverless workgroup endpoint.

        Args:
            workgroup_name: Name of the Redshift serverless workgroup
            account_id: AWS account ID (optional, will be retrieved via STS if not provided)

        Returns:
            Endpoint string in format: workgroup.account_id.region.redshift-serverless.amazonaws.com:port
        """
        try:
            response = exponential_retry(
                self._redshift_serverless.get_workgroup, {"InternalServerException", "ThrottlingException"}, workgroupName=workgroup_name
            )

            workgroup = response["workgroup"]
            endpoint = workgroup.get("endpoint", {})

            if endpoint:
                address = endpoint.get("address", "")
                port = endpoint.get("port", 5439)
                return f"{address}:{port}"
            else:
                # Construct endpoint based on naming convention with account ID
                # CRITICAL FIX: Include account ID in endpoint construction for proper Redshift authentication
                if not account_id:
                    sts = self._session.client("sts", region_name=self._region)
                    account_id = sts.get_caller_identity()["Account"]
                return f"{workgroup_name}.{account_id}.{self._region}.redshift-serverless.amazonaws.com:5439"

        except Exception as e:
            module_logger.warning(f"Could not get workgroup endpoint, using default format: {e}")
            # CRITICAL FIX: Include account ID in fallback endpoint construction
            # This fixes "To get cluster credentials, you must provide either the cluster identifier" error
            if not account_id:
                try:
                    sts = self._session.client("sts", region_name=self._region)
                    account_id = sts.get_caller_identity()["Account"]
                except Exception as sts_error:
                    module_logger.error(f"Could not get account ID for endpoint construction: {sts_error}")
                    # Last resort - return without account ID (will likely fail but at least provides diagnostics)
                    return f"{workgroup_name}.{self._region}.redshift-serverless.amazonaws.com:5439"

            return f"{workgroup_name}.{account_id}.{self._region}.redshift-serverless.amazonaws.com:5439"

    def namespace_exists(self, namespace_name: str) -> bool:
        """Check if Redshift serverless namespace exists."""
        try:
            response = exponential_retry(
                self._redshift_serverless.get_namespace, {"InternalServerException", "ThrottlingException"}, namespaceName=namespace_name
            )
            return response["namespace"]["status"] in ["AVAILABLE", "CREATING"]
        except ClientError as error:
            if error.response["Error"]["Code"] == "ResourceNotFoundException":
                return False
            raise

    def create_namespace(
        self, namespace_name: str, admin_username: str = "admin", admin_password: str = None, default_iam_role_arn: str = None
    ) -> None:
        """Create Redshift serverless namespace."""
        try:
            # Generate a secure random password but store it for consistency across activations
            # Note: This admin password is not used by AndesSpark (which uses IAM auth)
            if not admin_password:
                import secrets
                import string

                # AWS Redshift password requirements:
                # - 8-64 characters long
                # - At least one uppercase letter
                # - At least one lowercase letter
                # - At least one decimal digit
                # - Forbidden characters: '/', '@', '"', ' ', '\', '''
                # Build character sets
                uppercase = string.ascii_uppercase
                lowercase = string.ascii_lowercase
                digits = string.digits
                special_chars = "!#$%^&*()-_=+[]{}|;:,.<>?"

                # Ensure password contains at least one from each required category
                admin_password = (
                    secrets.choice(uppercase) + secrets.choice(lowercase) + secrets.choice(digits) + secrets.choice(special_chars)
                )

                # Fill remaining length with random characters from all allowed characters
                all_chars = uppercase + lowercase + digits + special_chars
                for _ in range(28):  # 32 total - 4 required characters
                    admin_password += secrets.choice(all_chars)

                # Shuffle the password to randomize the order
                password_list = list(admin_password)
                secrets.SystemRandom().shuffle(password_list)
                admin_password = "".join(password_list)

                # Log that password is generated (but not the password itself for security)
                module_logger.info(
                    f"Generated secure admin password for namespace {namespace_name} (meets all AWS requirements, not used by AndesSpark)"
                )

            namespace_config = {
                "namespaceName": namespace_name,
                "adminUsername": admin_username,
                "adminUserPassword": admin_password,
            }

            # Associate IAM role with namespace for AndesSpark access
            if default_iam_role_arn:
                # Must add role to iamRoles list first, then set as default
                namespace_config["iamRoles"] = [default_iam_role_arn]
                namespace_config["defaultIamRoleArn"] = default_iam_role_arn
                module_logger.info(
                    f"Associating IAM role {default_iam_role_arn} with namespace {namespace_name} (both in iamRoles and as default)"
                )

            module_logger.info(f"Creating Redshift serverless namespace: {namespace_name}")

            exponential_retry(
                self._redshift_serverless.create_namespace, {"InternalServerException", "ThrottlingException"}, **namespace_config
            )

            # Wait for namespace to become available
            self.wait_for_namespace_available(namespace_name)

        except ClientError as error:
            if error.response["Error"]["Code"] != "ConflictException":  # Already exists
                raise

    def update_namespace_iam_role(self, namespace_name: str, default_iam_role_arn: str) -> None:
        """Update existing namespace with default IAM role if not already set."""
        try:
            # Get current namespace configuration
            response = exponential_retry(
                self._redshift_serverless.get_namespace,
                {"InternalServerException", "ThrottlingException"},
                namespaceName=namespace_name,
            )

            current_namespace = response["namespace"]
            current_default_role = current_namespace.get("defaultIamRoleArn")
            current_iam_roles_raw = current_namespace.get("iamRoles", [])

            # Extract plain ARN strings from role objects in current_iam_roles
            # AWS API returns role objects like 'IamRole(applyStatus=in-sync, iamRoleArn=arn:aws:iam::...)'
            current_iam_roles_clean = []
            for role in current_iam_roles_raw:
                if isinstance(role, str) and role.startswith("arn:aws:iam::"):
                    # Already a plain ARN string
                    current_iam_roles_clean.append(role)
                elif hasattr(role, "iamRoleArn"):
                    # Role object with iamRoleArn attribute
                    current_iam_roles_clean.append(role.iamRoleArn)
                elif hasattr(role, "arn"):
                    # Role object with arn attribute
                    current_iam_roles_clean.append(role.arn)
                else:
                    # Try to extract from string representation
                    role_str = str(role)
                    if "iamRoleArn=" in role_str:
                        import re

                        match = re.search(r"iamRoleArn=([^,)]+)", role_str)
                        if match:
                            current_iam_roles_clean.append(match.group(1))
                        else:
                            # Fallback: use as-is and let AWS handle it
                            current_iam_roles_clean.append(role_str)
                    else:
                        current_iam_roles_clean.append(role_str)

            module_logger.info(f"Current IAM roles (cleaned): {current_iam_roles_clean}")

            # Check if update is needed using clean ARN strings
            needs_update = current_default_role != default_iam_role_arn or default_iam_role_arn not in current_iam_roles_clean

            if needs_update:
                module_logger.info(f"Updating namespace {namespace_name} with IAM role {default_iam_role_arn}")

                # Ensure the role is in the iamRoles list (use clean strings only)
                updated_iam_roles = list(current_iam_roles_clean)
                if default_iam_role_arn not in updated_iam_roles:
                    updated_iam_roles.append(default_iam_role_arn)

                # Update namespace with both iamRoles and defaultIamRoleArn (all clean strings)
                exponential_retry(
                    self._redshift_serverless.update_namespace,
                    {"InternalServerException", "ThrottlingException"},
                    namespaceName=namespace_name,
                    iamRoles=updated_iam_roles,  # Clean string ARNs only
                    defaultIamRoleArn=default_iam_role_arn,
                )

                # Wait for namespace to be available after update
                self.wait_for_namespace_available(namespace_name)
                module_logger.info(f"Successfully updated namespace {namespace_name} with IAM role (added to iamRoles and set as default)")
            else:
                module_logger.info(f"Namespace {namespace_name} already has correct IAM role configuration")

        except ClientError as error:
            module_logger.error(f"Failed to update namespace {namespace_name} with IAM role: {error}")
            raise

    def wait_for_namespace_available(self, namespace_name: str, timeout_minutes: int = 10) -> None:
        """Wait for Redshift namespace to become available."""
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            try:
                response = exponential_retry(
                    self._redshift_serverless.get_namespace,
                    {"InternalServerException", "ThrottlingException"},
                    namespaceName=namespace_name,
                )
                status = response["namespace"]["status"]
                if status == "AVAILABLE":
                    module_logger.info(f"Redshift namespace {namespace_name} is now available")
                    return
                elif status == "DELETING":
                    raise RuntimeError(f"Redshift namespace {namespace_name} is being deleted")

                module_logger.info(f"Waiting for Redshift namespace {namespace_name}, status: {status}")
                time.sleep(30)

            except ClientError as error:
                if error.response["Error"]["Code"] == "ResourceNotFoundException":
                    raise RuntimeError(f"Redshift namespace {namespace_name} was not found")
                raise

        raise RuntimeError(f"Timeout waiting for Redshift namespace {namespace_name} to become available")

    def workgroup_exists(self, workgroup_name: str) -> bool:
        """Check if Redshift serverless workgroup exists."""
        try:
            response = exponential_retry(
                self._redshift_serverless.get_workgroup, {"InternalServerException", "ThrottlingException"}, workgroupName=workgroup_name
            )
            return response["workgroup"]["status"] in ["AVAILABLE", "CREATING"]
        except ClientError as error:
            if error.response["Error"]["Code"] == "ResourceNotFoundException":
                return False
            raise

    def create_workgroup(
        self,
        workgroup_name: str,
        namespace_name: str,
        subnet_ids: Optional[List[str]] = None,
        security_group_ids: Optional[List[str]] = None,
        base_capacity: int = 32,
    ) -> None:
        """Create Redshift serverless workgroup with appropriate configuration."""

        # Create workgroup configuration
        workgroup_config = {
            "workgroupName": workgroup_name,
            "namespaceName": namespace_name,  # Required parameter
            "baseCapacity": base_capacity,
            "enhancedVpcRouting": True,
            "publiclyAccessible": True,
        }

        # Add subnet configuration if available - use all provided subnets
        if subnet_ids:
            workgroup_config["subnetIds"] = subnet_ids

        # Add security group configuration
        if security_group_ids:
            workgroup_config["securityGroupIds"] = security_group_ids

        module_logger.info(
            f"Creating Redshift serverless workgroup: {workgroup_name} in namespace: {namespace_name} "
            f"(publiclyAccessible=True for EMR access)"
        )

        try:
            exponential_retry(
                self._redshift_serverless.create_workgroup, {"InternalServerException", "ThrottlingException"}, **workgroup_config
            )

            # Wait for workgroup to become available
            self.wait_for_workgroup_available(workgroup_name)

        except ClientError as error:
            if error.response["Error"]["Code"] != "ConflictException":  # Already exists
                raise

    def get_or_create_security_group(self, vpc_id: str, unique_context_id: str) -> str:
        """Get or create security group for Redshift-EMR connectivity."""
        sg_name = f"if-redshift-emr-{unique_context_id}"

        try:
            # Try to find existing security group
            response = self._ec2.meta.client.describe_security_groups(
                Filters=[{"Name": "group-name", "Values": [sg_name]}, {"Name": "vpc-id", "Values": [vpc_id]}]
            )
            if response["SecurityGroups"]:
                sg_id = response["SecurityGroups"][0]["GroupId"]

                # DIAGNOSTIC: Log existing security group rules for debugging
                sg_details = response["SecurityGroups"][0]
                inbound_rules = sg_details.get("IpPermissions", [])
                module_logger.info(f"Found existing Redshift-EMR security group {sg_id} with {len(inbound_rules)} inbound rules")

                # Check if self-referencing rule exists
                has_self_ref = any(
                    rule.get("IpProtocol") == "-1" and any(pair.get("GroupId") == sg_id for pair in rule.get("UserIdGroupPairs", []))
                    for rule in inbound_rules
                )

                if not has_self_ref:
                    module_logger.warning(f"Security group {sg_id} missing self-referencing rule! Adding it now...")
                    try:
                        self._ec2.meta.client.authorize_security_group_ingress(
                            GroupId=sg_id, IpPermissions=[{"IpProtocol": "-1", "UserIdGroupPairs": [{"GroupId": sg_id}]}]
                        )
                        module_logger.info(f"Added self-referencing rule to security group {sg_id}")
                    except ClientError as e:
                        if e.response.get("Error", {}).get("Code") != "InvalidPermission.Duplicate":
                            module_logger.warning(f"Could not add self-referencing rule: {e}")
                else:
                    module_logger.info(f"Security group {sg_id} has proper self-referencing rule")

                return sg_id
        except ClientError:
            pass

        # Create new security group
        try:
            module_logger.info(f"Creating new Redshift-EMR security group: {sg_name} in VPC {vpc_id}")
            sg_response = self._ec2.meta.client.create_security_group(
                GroupName=sg_name, Description=f"IntelliFlow EMR-Redshift connectivity for {unique_context_id}", VpcId=vpc_id
            )
            security_group_id = sg_response["GroupId"]

            # Add inbound rule allowing all traffic from itself (self-referencing rule)
            # This allows any resource using this SG to communicate with any other resource using the same SG
            self._ec2.meta.client.authorize_security_group_ingress(
                GroupId=security_group_id, IpPermissions=[{"IpProtocol": "-1", "UserIdGroupPairs": [{"GroupId": security_group_id}]}]
            )

            module_logger.info(
                f"Created security group {security_group_id} with self-referencing rule " f"(allows all traffic between EMR and Redshift)"
            )

            return security_group_id
        except ClientError as error:
            module_logger.error(f"Failed to create security group for Redshift: {error}")
            raise

    def update_workgroup(
        self,
        workgroup_name: str,
        subnet_ids: Optional[List[str]] = None,
        security_group_ids: Optional[List[str]] = None,
        base_capacity: Optional[int] = None,
    ) -> None:
        """Update Redshift serverless workgroup configuration (subnets/security groups/base_capacity).

        CRITICAL: This fixes connectivity issues after VPC recovery/idempotency changes
        by ensuring workgroup uses same subnets as EMR clusters.
        """
        try:
            # Get current workgroup configuration
            response = exponential_retry(
                self._redshift_serverless.get_workgroup,
                {"InternalServerException", "ThrottlingException"},
                workgroupName=workgroup_name,
            )

            current_workgroup = response["workgroup"]
            current_subnet_ids = set(current_workgroup.get("subnetIds", []))
            current_sg_ids = set(current_workgroup.get("securityGroupIds", []))
            current_publicly_accessible = current_workgroup.get("publiclyAccessible", True)
            current_base_capacity = current_workgroup.get("baseCapacity", 32)

            # AWS Redshift Serverless limitation: Can't update multiple configurations at the same time
            # We need to split updates into separate API calls:
            # 1. First update networking configuration (subnets, security groups, publiclyAccessible)
            # 2. Then update base_capacity separately if needed

            # Phase 1: Update networking configuration
            networking_update_params = {"workgroupName": workgroup_name}
            needs_networking_update = False

            if subnet_ids:
                new_subnet_ids = set(subnet_ids)
                if new_subnet_ids != current_subnet_ids:
                    networking_update_params["subnetIds"] = subnet_ids
                    needs_networking_update = True
                    module_logger.info(
                        f"Workgroup subnet change detected: {current_subnet_ids} → {new_subnet_ids}. "
                        f"This update fixes connectivity after VPC recovery."
                    )

            if security_group_ids:
                new_sg_ids = set(security_group_ids)
                if new_sg_ids != current_sg_ids:
                    networking_update_params["securityGroupIds"] = security_group_ids
                    needs_networking_update = True
                    module_logger.info(f"Workgroup security group change detected: {current_sg_ids} → {new_sg_ids}")

            # CRITICAL FIX: Ensure publiclyAccessible is True for VPC-only EMR access
            if not current_publicly_accessible:
                networking_update_params["publiclyAccessible"] = True
                needs_networking_update = True
                module_logger.warning(
                    f"Workgroup {workgroup_name} is not publicly accessible! Changing to VPC-only access (publiclyAccessible=True). "
                    f"This is CRITICAL for EMR connectivity from private subnets."
                )

            if needs_networking_update:
                module_logger.info(f"Updating Redshift workgroup {workgroup_name} networking configuration...")
                exponential_retry(
                    self._redshift_serverless.update_workgroup,
                    {"InternalServerException", "ThrottlingException", "ConflictException"},
                    **networking_update_params,
                )

                # Wait for update to complete before proceeding
                self.wait_for_workgroup_available(workgroup_name)
                module_logger.info(f"Successfully updated workgroup {workgroup_name} networking configuration")

                # Additional wait to ensure no background operations are in progress
                # AWS Redshift Serverless may still have operations running even after status is AVAILABLE
                if base_capacity is not None and base_capacity != current_base_capacity:
                    module_logger.info("Waiting 30 seconds before base_capacity update to ensure workgroup is fully ready...")
                    time.sleep(30)

            # Phase 2: Update base_capacity separately if needed
            needs_capacity_update = False
            if base_capacity is not None and base_capacity != current_base_capacity:
                needs_capacity_update = True
                module_logger.info(f"Workgroup base_capacity change detected: {current_base_capacity} → {base_capacity}")

                capacity_update_params = {
                    "workgroupName": workgroup_name,
                    "baseCapacity": base_capacity,
                }

                # Retry with ConflictException handling for cases where background operations are still in progress
                exponential_retry(
                    self._redshift_serverless.update_workgroup,
                    {"InternalServerException", "ThrottlingException", "ConflictException"},
                    **capacity_update_params,
                )

                # Wait for update to complete
                self.wait_for_workgroup_available(workgroup_name)
                module_logger.info(f"Successfully updated workgroup {workgroup_name} base_capacity to {base_capacity}")

            if not needs_networking_update and not needs_capacity_update:
                module_logger.info(
                    f"Workgroup {workgroup_name} configuration already matches desired state "
                    f"(publiclyAccessible=True, subnets/SGs/base_capacity match) - no update needed"
                )

        except ClientError as error:
            module_logger.error(f"Failed to update workgroup {workgroup_name}: {error}")
            raise

    def wait_for_workgroup_available(self, workgroup_name: str, timeout_minutes: int = 10) -> None:
        """Wait for Redshift workgroup to become available."""
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            try:
                response = exponential_retry(
                    self._redshift_serverless.get_workgroup,
                    {"InternalServerException", "ThrottlingException"},
                    workgroupName=workgroup_name,
                )
                status = response["workgroup"]["status"]
                if status == "AVAILABLE":
                    module_logger.info(f"Redshift workgroup {workgroup_name} is now available")
                    return
                elif status == "DELETING":
                    raise RuntimeError(f"Redshift workgroup {workgroup_name} is being deleted")

                module_logger.info(f"Waiting for Redshift workgroup {workgroup_name}, status: {status}")
                time.sleep(30)

            except ClientError as error:
                if error.response["Error"]["Code"] == "ResourceNotFoundException":
                    raise RuntimeError(f"Redshift workgroup {workgroup_name} was not found")
                raise

        raise RuntimeError(f"Timeout waiting for Redshift workgroup {workgroup_name} to become available")

    def setup_glue_catalog_integration(self, workgroup_name: str, database_name: str, execution_role_arn: str) -> None:
        """Set up Glue Data Catalog as 'andes' database in Redshift and grant permissions."""

        module_logger.info(f"Setting up Glue Data Catalog integration for workgroup {workgroup_name}")

        # Extract role name for IAM username format
        role_name = execution_role_arn.split("/")[-1]
        iam_username = f"IAMR:{role_name}"

        # Create external schema for Glue catalog using 'andes' database and EXE role ARN
        # Extract account ID from execution role ARN for CATALOG_ID parameter
        account_id = execution_role_arn.split(":")[4]  # ARN format: arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME

        # schema_statement = f"CREATE EXTERNAL SCHEMA IF NOT EXISTS andes FROM DATA CATALOG DATABASE 'andes' IAM_ROLE '{execution_role_arn}';"
        schema_statement = (
            f"CREATE EXTERNAL SCHEMA IF NOT EXISTS andes FROM DATA CATALOG DATABASE 'andes' IAM_ROLE 'SESSION' CATALOG_ID '{account_id}';"
        )

        try:
            # Use extended timeout (7 minutes instead of 5) for first-time Redshift Serverless provisioning
            # Glue catalog integration can take longer than normal operations in new accounts
            module_logger.info(
                f"Executing Glue catalog integration with extended timeout (7 minutes) for potential first-time provisioning"
            )
            self.execute_statement_with_timeout(workgroup_name, database_name, schema_statement, timeout_minutes=7)
            module_logger.info(f"Successfully created external schema 'andes' from Glue Data Catalog")
        except Exception as e:
            module_logger.error(f"Failed to create external schema 'andes': {e}")
            raise  # This is critical, fail if we can't create the schema

        # Establish IAM user in Redshift by assuming execution role and connecting
        try:
            module_logger.info(f"Establishing IAM user {iam_username} in Redshift by assuming execution role")
            self._establish_iam_user_in_redshift(workgroup_name, database_name, execution_role_arn)

            # Now proceed with GRANT statements
            grant_statements = [
                f'GRANT USAGE ON SCHEMA andes TO "{iam_username}";',
                f'GRANT SELECT ON ALL TABLES IN SCHEMA andes TO "{iam_username}";',
            ]

            for statement in grant_statements:
                try:
                    # Use standard timeout for GRANT statements as they should be fast
                    self.execute_statement(workgroup_name, database_name, statement)
                    module_logger.info(f"Successfully granted permissions to {iam_username}")
                except Exception as e:
                    module_logger.warning(f"Could not grant permissions to {iam_username}: {e}")

        except Exception as e:
            module_logger.warning(f"Could not establish IAM user during activation: {e}")
            module_logger.info(f"External schema 'andes' is ready, permissions will be available on first EMR connection")

    def _establish_iam_user_in_redshift(self, workgroup_name: str, database_name: str, execution_role_arn: str) -> None:
        """Assume execution role and run a query to establish IAM user in Redshift."""

        # Assume the execution role
        sts = self._session.client("sts", region_name=self._region)

        try:
            # Assume the execution role to establish IAM user in Redshift
            assume_response = exponential_retry(
                sts.assume_role,
                {"AccessDenied", "ServiceUnavailable", "ThrottlingException", "RequestLimitExceeded"},
                RoleArn=execution_role_arn,
                RoleSessionName="RedshiftIAMUserEstablishment",
            )

            credentials = assume_response["Credentials"]

            # Create a new session with the assumed role credentials
            import boto3

            assumed_session = boto3.Session(
                aws_access_key_id=credentials["AccessKeyId"],
                aws_secret_access_key=credentials["SecretAccessKey"],
                aws_session_token=credentials["SessionToken"],
                region_name=self._region,
            )

            # Create Redshift Data client with assumed role credentials
            assumed_redshift_data = assumed_session.client("redshift-data")

            # Execute a simple query to establish IAM user
            module_logger.info("Executing 'SELECT current_user' to establish IAM user in Redshift")

            response = exponential_retry(
                assumed_redshift_data.execute_statement,
                {"InternalServerException", "ThrottlingException"},
                WorkgroupName=workgroup_name,
                Database=database_name,
                Sql="SELECT current_user;",
            )

            # Wait for completion
            statement_id = response["Id"]
            self._wait_for_assumed_statement_completion(assumed_redshift_data, statement_id)

            module_logger.info("Successfully established IAM user in Redshift")

        except Exception as e:
            module_logger.warning(f"Could not establish IAM user via role assumption: {e}")
            raise

    def _wait_for_assumed_statement_completion(self, redshift_data_client, statement_id: str, timeout_minutes: int = 2) -> None:
        """Wait for statement completion using assumed role credentials."""
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            try:
                response = redshift_data_client.describe_statement(Id=statement_id)
                status = response["Status"]

                if status == "FINISHED":
                    return
                elif status == "FAILED":
                    error_msg = response.get("Error", "Unknown error")
                    raise RuntimeError(f"IAM user establishment query failed: {error_msg}")
                elif status == "ABORTED":
                    raise RuntimeError("IAM user establishment query was aborted")

                time.sleep(3)

            except ClientError as error:
                raise RuntimeError(f"Error checking IAM establishment query status: {error}")

        raise RuntimeError(f"Timeout waiting for IAM user establishment query to complete")

    def execute_statement(self, workgroup_name: str, database: str, statement: str) -> Dict[str, Any]:
        """Execute a SQL statement in Redshift using the Data API."""
        return self.execute_statement_with_timeout(workgroup_name, database, statement, timeout_minutes=7)

    def execute_statement_with_timeout(
        self, workgroup_name: str, database: str, statement: str, timeout_minutes: int = 7
    ) -> Dict[str, Any]:
        """Execute a SQL statement in Redshift using the Data API with configurable timeout."""
        try:
            # Add user-friendly logging for long-running operations
            module_logger.info(f"Executing Redshift SQL statement in workgroup {workgroup_name} (timeout: {timeout_minutes} minutes)")
            module_logger.info(f"Statement: {statement[:100]}{'...' if len(statement) > 100 else ''}")

            # Retry logic for endpoint availability - handles cases where workgroup is AVAILABLE
            # but endpoint is not yet ready (can happen in new accounts or after modifications)
            max_endpoint_wait_attempts = 4  # Up to ~1 minute of retries
            endpoint_wait_delay = 15  # seconds between attempts

            for attempt in range(max_endpoint_wait_attempts):
                try:
                    response = exponential_retry(
                        self._redshift_data.execute_statement,
                        {"InternalServerException", "ThrottlingException"},
                        WorkgroupName=workgroup_name,
                        Database=database,
                        Sql=statement,
                    )
                    # Successfully submitted statement
                    break
                except ClientError as error:
                    error_code = error.response.get("Error", {}).get("Code", "")
                    error_message = error.response.get("Error", {}).get("Message", "")

                    # Check if this is an endpoint availability error
                    if error_code == "ValidationException" and "endpoint is not available" in error_message.lower():
                        if attempt < max_endpoint_wait_attempts - 1:
                            module_logger.info(
                                f"Redshift endpoint not yet available (attempt {attempt + 1}/{max_endpoint_wait_attempts}), "
                                f"waiting {endpoint_wait_delay} seconds before retry... "
                                f"(This is normal for new accounts or during AWS provisioning)"
                            )
                            time.sleep(endpoint_wait_delay)
                            continue
                        else:
                            # Final attempt failed - log helpful message but let it fail
                            module_logger.error(
                                f"Redshift endpoint still not available after {max_endpoint_wait_attempts} attempts "
                                f"({max_endpoint_wait_attempts * endpoint_wait_delay} seconds). "
                                f"The workgroup may still be provisioning in AWS. Please check AWS Redshift console "
                                f"for workgroup status and retry activation later if workgroup is stuck in 'Modifying' state."
                            )
                            raise
                    else:
                        # Different error, re-raise immediately
                        raise

            statement_id = response["Id"]
            module_logger.info(f"Statement submitted successfully (ID: {statement_id}), waiting for completion...")

            # Wait for statement completion with configurable timeout and progress logging
            self.wait_for_statement_completion_with_progress(statement_id, timeout_minutes=timeout_minutes)

            module_logger.info(f"Statement completed successfully")
            return response

        except ClientError as error:
            module_logger.error(f"Failed to execute Redshift statement: {error}")
            raise

    def wait_for_statement_completion(self, statement_id: str, timeout_minutes: int = 7) -> None:
        """Wait for Redshift Data API statement to complete."""
        return self.wait_for_statement_completion_with_progress(statement_id, timeout_minutes)

    def wait_for_statement_completion_with_progress(self, statement_id: str, timeout_minutes: int = 7) -> None:
        """Wait for Redshift Data API statement to complete with enhanced progress logging."""
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()
        last_progress_log = 0
        progress_log_interval = 60  # Log progress every minute

        module_logger.info(f"Waiting for Redshift statement {statement_id} to complete (timeout: {timeout_minutes} minutes)")

        while time.time() - start_time < timeout_seconds:
            try:
                response = exponential_retry(
                    self._redshift_data.describe_statement, {"InternalServerException", "ThrottlingException"}, Id=statement_id
                )

                status = response["Status"]
                if status == "FINISHED":
                    elapsed_time = time.time() - start_time
                    module_logger.info(f"Redshift statement {statement_id} completed successfully in {elapsed_time:.1f} seconds")
                    return
                elif status == "FAILED":
                    error_msg = response.get("Error", "Unknown error")
                    raise RuntimeError(f"Redshift statement failed: {error_msg}")
                elif status == "ABORTED":
                    raise RuntimeError("Redshift statement was aborted")

                # Progress logging for long-running operations
                elapsed_time = time.time() - start_time
                if elapsed_time - last_progress_log >= progress_log_interval:
                    remaining_time = timeout_seconds - elapsed_time
                    module_logger.info(
                        f"Redshift statement {statement_id} still running (status: {status}, elapsed: {elapsed_time:.0f}s, remaining: {remaining_time:.0f}s)"
                    )
                    last_progress_log = elapsed_time

                time.sleep(5)

            except ClientError as error:
                module_logger.error(f"Error checking statement status: {error}")
                raise

        # Enhanced timeout error with more context
        elapsed_time = time.time() - start_time
        raise RuntimeError(
            f"Timeout waiting for Redshift statement {statement_id} to complete after {elapsed_time:.0f} seconds "
            f"(timeout: {timeout_minutes} minutes). This may indicate first-time Redshift Serverless provisioning "
            f"or Glue catalog integration taking longer than expected."
        )

    def delete_workgroup(self, workgroup_name: str) -> None:
        """Delete Redshift serverless workgroup."""
        # Check if workgroup exists before attempting to delete
        if self.workgroup_exists(workgroup_name):
            module_logger.info(f"Deleting Redshift serverless workgroup {workgroup_name}")

            exponential_retry(
                self._redshift_serverless.delete_workgroup,
                {"InternalServerException", "ThrottlingException"},
                workgroupName=workgroup_name,
            )

            # Wait for workgroup to be deleted
            self.wait_for_workgroup_deleted(workgroup_name)

            module_logger.info(f"Successfully deleted Redshift serverless workgroup {workgroup_name}")

    def delete_namespace(self, namespace_name: str) -> None:
        """Delete Redshift serverless namespace."""
        # Check if namespace exists before attempting to delete
        if self.namespace_exists(namespace_name):
            module_logger.info(f"Deleting Redshift serverless namespace {namespace_name}")

            exponential_retry(
                self._redshift_serverless.delete_namespace, {"InternalServerException", "ThrottlingException"}, namespaceName=namespace_name
            )

            # Wait for namespace to be deleted
            self.wait_for_namespace_deleted(namespace_name)

            module_logger.info(f"Successfully deleted Redshift serverless namespace {namespace_name}")

    def wait_for_namespace_deleted(self, namespace_name: str, timeout_minutes: int = 15) -> None:
        """Wait for Redshift namespace to be deleted."""
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            try:
                response = exponential_retry(
                    self._redshift_serverless.get_namespace,
                    {"InternalServerException", "ThrottlingException"},
                    namespaceName=namespace_name,
                )
                status = response["namespace"]["status"]
                if status == "DELETING":
                    module_logger.info(f"Waiting for Redshift namespace {namespace_name} deletion, status: {status}")
                    time.sleep(30)
                else:
                    # If not in deleting state, continue waiting
                    time.sleep(30)

            except ClientError as error:
                if error.response["Error"]["Code"] == "ResourceNotFoundException":
                    module_logger.info(f"Redshift namespace {namespace_name} has been deleted")
                    return
                raise

        module_logger.warning(f"Timeout waiting for Redshift namespace {namespace_name} to be deleted")

    def wait_for_workgroup_deleted(self, workgroup_name: str, timeout_minutes: int = 10) -> None:
        """Wait for Redshift workgroup to be deleted."""
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            try:
                response = exponential_retry(
                    self._redshift_serverless.get_workgroup,
                    {"InternalServerException", "ThrottlingException"},
                    workgroupName=workgroup_name,
                )
                status = response["workgroup"]["status"]
                if status == "DELETING":
                    module_logger.info(f"Waiting for Redshift workgroup {workgroup_name} deletion, status: {status}")
                    time.sleep(30)
                else:
                    # If not in deleting state, continue waiting
                    time.sleep(30)

            except ClientError as error:
                if error.response["Error"]["Code"] == "ResourceNotFoundException":
                    module_logger.info(f"Redshift workgroup {workgroup_name} has been deleted")
                    return
                raise

        module_logger.warning(f"Timeout waiting for Redshift workgroup {workgroup_name} to be deleted")

    def cleanup_security_group(self, unique_context_id: str) -> None:
        """Clean up the security group created for Redshift-EMR connectivity."""
        sg_name = f"if-redshift-emr-{unique_context_id}"

        # Find the security group
        response = self._ec2.meta.client.describe_security_groups(Filters=[{"Name": "group-name", "Values": [sg_name]}])

        for sg in response["SecurityGroups"]:
            sg_id = sg["GroupId"]
            try:
                # Delete the security group
                self._ec2.meta.client.delete_security_group(GroupId=sg_id)
                module_logger.info(f"Deleted Redshift security group {sg_id}")
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["InvalidGroup.NotFound", "DependencyViolation"]:
                    module_logger.warning(f"Could not delete Redshift security group {sg_id}: {error}")

    def get_subnets_in_vpc(self, vpc_id: str) -> List[str]:
        """Get all subnets in the specified VPC."""
        try:
            response = self._ec2.meta.client.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
            return [subnet["SubnetId"] for subnet in response["Subnets"]]
        except Exception as e:
            module_logger.error(f"Failed to get subnets for VPC {vpc_id}: {e}")
            return []

    def ensure_all_subnets_supported(self, subnet_ids: Union[str, List[str], Set[str]]) -> List[str]:
        """Ensure all provided subnets are in the same VPC and return them as a list.
        This addresses the issue where EMR might pick different subnets at runtime.
        """
        if isinstance(subnet_ids, str):
            subnet_ids = [subnet_ids]
        elif isinstance(subnet_ids, (set, tuple)):
            subnet_ids = list(subnet_ids)

        if not subnet_ids:
            return []

        # Get VPC ID from the first subnet
        try:
            first_subnet = self._ec2.Subnet(subnet_ids[0])
            vpc_id = first_subnet.vpc_id

            # Verify all subnets are in the same VPC
            for subnet_id in subnet_ids:
                subnet = self._ec2.Subnet(subnet_id)
                if subnet.vpc_id != vpc_id:
                    raise ValueError(f"Subnet {subnet_id} is in VPC {subnet.vpc_id}, but expected VPC {vpc_id}")

            return subnet_ids
        except Exception as e:
            module_logger.error(f"Failed to validate subnets: {e}")
            raise

    def setup_andes_fgac_s3_lifecycle_policy(self, bucket_name: str) -> str:
        """Set up S3 lifecycle policy for AndesSpark temporary files and return the temp dir path."""
        andes_fgac_prefix = "andes-fgac-temp/"

        # Get S3 client to manage lifecycle policies
        s3_client = self._session.client("s3", region_name=self._region)

        # Define lifecycle rule for AndesSpark temporary files
        lifecycle_rule = {
            "Rules": [
                {"ID": "AndesFgacTempCleanup", "Status": "Enabled", "Filter": {"Prefix": andes_fgac_prefix}, "Expiration": {"Days": 3}}
            ]
        }

        # Check if bucket has existing lifecycle configuration
        existing_rules = []
        try:
            # Don't retry NoSuchLifecycleConfiguration - it's an expected normal state
            response = exponential_retry(s3_client.get_bucket_lifecycle_configuration, {}, Bucket=bucket_name)
            existing_rules = response.get("Rules", [])
        except ClientError as error:
            if error.response["Error"]["Code"] != "NoSuchLifecycleConfiguration":
                raise

        # Check if our rule already exists
        rule_exists = any(rule.get("ID") == "AndesFgacTempCleanup" for rule in existing_rules)

        if not rule_exists:
            # Add our rule to existing rules
            all_rules = existing_rules + lifecycle_rule["Rules"]

            # Apply updated lifecycle configuration
            exponential_retry(
                s3_client.put_bucket_lifecycle_configuration,
                {"ServiceException"},
                Bucket=bucket_name,
                LifecycleConfiguration={"Rules": all_rules},
            )

            module_logger.info(f"Set up S3 lifecycle policy for AndesSpark temp files in bucket {bucket_name}")
        else:
            module_logger.info(f"S3 lifecycle policy for AndesSpark temp files already exists in bucket {bucket_name}")

        return f"s3://{bucket_name}/{andes_fgac_prefix}"

    def get_andes_fgac_temp_dir(self, bucket_name: str) -> str:
        """Get the S3 path for AndesSpark temporary directory."""
        return f"s3://{bucket_name}/andes-fgac-temp/"


def get_redshift_permissions_for_workgroup(region: str, account_id: str, workgroup_name: str) -> List[str]:
    """Get specific Redshift resource ARNs for permissions instead of using '*'."""
    workgroup_arn = RedshiftServerlessManager.build_workgroup_arn(region, account_id, workgroup_name)

    return [
        workgroup_arn,
        # Data API doesn't have specific resource ARNs, it's controlled by workgroup access
    ]
