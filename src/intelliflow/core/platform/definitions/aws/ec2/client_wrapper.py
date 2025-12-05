# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import time
from typing import Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from ..common import exponential_retry

module_logger = logging.getLogger(__name__)


class VPCManager:
    """Manager class for VPC and subnet operations."""

    def __init__(self, session: boto3.Session, region: str):
        self._session = session
        self._region = region
        self._ec2 = session.resource("ec2", region_name=region)
        self._ec2_client = session.client("ec2", region_name=region)

    def get_available_azs(self) -> List[str]:
        """Get available AZs in the region."""
        response = exponential_retry(
            self._ec2_client.describe_availability_zones,
            {"RequestLimitExceeded", "ServiceUnavailable"},
            Filters=[{"Name": "state", "Values": ["available"]}],
        )
        return [az["ZoneName"] for az in response["AvailabilityZones"]]

    def create_vpc(self, unique_context_id: str, cidr_block: str = "10.0.0.0/16") -> str:
        """Create VPC with DNS support."""
        vpc_name = f"IntelliFlow-{unique_context_id}-VPC"

        try:
            # Create VPC
            vpc_response = exponential_retry(
                self._ec2_client.create_vpc, {"RequestLimitExceeded", "ServiceUnavailable"}, CidrBlock=cidr_block
            )
            vpc_id = vpc_response["Vpc"]["VpcId"]

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "VpcLimitExceeded":
                module_logger.error(
                    f"VPC limit exceeded when creating VPC: {e}\n"
                    f"RECOVERY STEPS:\n"
                    f"1. Go to AWS Console → VPC → Your VPCs\n"
                    f"2. Delete any unused IntelliFlow VPCs (check for 'IntelliFlow-' prefix)\n"
                    f"3. Or request VPC limit increase from AWS Support\n"
                    f"4. Check if previous app termination left orphaned VPCs"
                )
            raise

        # Tag VPC
        exponential_retry(
            self._ec2_client.create_tags,
            {"RequestLimitExceeded", "InvalidVpcID.NotFound"},
            Resources=[vpc_id],
            Tags=[{"Key": "Name", "Value": vpc_name}],
        )

        # Enable DNS hostnames and resolution
        exponential_retry(self._ec2_client.modify_vpc_attribute, {"RequestLimitExceeded"}, VpcId=vpc_id, EnableDnsHostnames={"Value": True})
        exponential_retry(self._ec2_client.modify_vpc_attribute, {"RequestLimitExceeded"}, VpcId=vpc_id, EnableDnsSupport={"Value": True})

        module_logger.info(f"Created VPC {vpc_id} ({vpc_name})")
        return vpc_id

    def create_permissive_network_acl(self, vpc_id: str, unique_context_id: str) -> str:
        """Create a permissive Network ACL that allows all outbound internet access for pip/bootstrap actions."""
        try:
            # Create custom Network ACL
            acl_response = exponential_retry(
                self._ec2_client.create_network_acl,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                VpcId=vpc_id,
            )
            acl_id = acl_response["NetworkAcl"]["NetworkAclId"]

            # Tag Network ACL
            exponential_retry(
                self._ec2_client.create_tags,
                {"RequestLimitExceeded", "InvalidNetworkAclID.NotFound"},
                Resources=[acl_id],
                Tags=[
                    {"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-NetworkACL"},
                    {"Key": "IntelliFlow", "Value": unique_context_id},
                ],
            )

            # Create inbound rules (allow all return traffic)
            exponential_retry(
                self._ec2_client.create_network_acl_entry,
                {"RequestLimitExceeded"},
                NetworkAclId=acl_id,
                RuleNumber=100,
                Protocol="-1",  # All protocols
                RuleAction="allow",
                CidrBlock="0.0.0.0/0",
                Egress=False,  # Inbound rule
            )

            # Create outbound rules (allow all outbound traffic for pip/bootstrap)
            exponential_retry(
                self._ec2_client.create_network_acl_entry,
                {"RequestLimitExceeded"},
                NetworkAclId=acl_id,
                RuleNumber=100,
                Protocol="-1",  # All protocols
                RuleAction="allow",
                CidrBlock="0.0.0.0/0",
                Egress=True,  # Outbound rule
            )

            module_logger.info(f"Created permissive Network ACL {acl_id} for internet access")
            return acl_id

        except ClientError as e:
            module_logger.error(f"Failed to create Network ACL: {e}")
            raise

    def ensure_permissive_network_acl(self, vpc_id: str, unique_context_id: str) -> Optional[str]:
        """Ensure permissive Network ACL exists for the VPC, creating if necessary."""
        try:
            # Check if we already have a permissive IntelliFlow Network ACL
            acl_response = exponential_retry(
                self._ec2_client.describe_network_acls,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "tag:IntelliFlow", "Values": [unique_context_id]},
                    {"Name": "default", "Values": ["false"]},
                ],
            )

            if acl_response["NetworkAcls"]:
                acl_id = acl_response["NetworkAcls"][0]["NetworkAclId"]
                module_logger.info(f"Found existing IntelliFlow Network ACL: {acl_id}")
                return acl_id

            # Create new permissive Network ACL if none exists
            acl_id = self.create_permissive_network_acl(vpc_id, unique_context_id)
            module_logger.info(f"Created new permissive Network ACL {acl_id} for internet access")
            return acl_id

        except Exception as e:
            module_logger.warning(f"Could not ensure permissive Network ACL for VPC {vpc_id}: {e}")
            raise e

    def ensure_private_subnets_internet_access(self, vpc_id: str, private_subnet_ids: List[str], unique_context_id: str) -> None:
        """Ensure private subnets have permissive Network ACL for outbound internet access."""
        try:
            # Get or create permissive Network ACL
            acl_id = self.ensure_permissive_network_acl(vpc_id, unique_context_id)
            if not acl_id:
                module_logger.warning("Could not get permissive Network ACL - private subnets may have internet access issues")
                return

            # Apply permissive Network ACL to private subnets if they don't already have it
            for private_subnet_id in private_subnet_ids:
                try:
                    # Check current Network ACL for this subnet
                    current_acl_response = exponential_retry(
                        self._ec2_client.describe_network_acls,
                        {"RequestLimitExceeded", "ServiceUnavailable"},
                        Filters=[
                            {"Name": "vpc-id", "Values": [vpc_id]},
                            {"Name": "association.subnet-id", "Values": [private_subnet_id]},
                        ],
                    )

                    # Check if subnet is already using our permissive ACL
                    current_acl_id = None
                    association_id = None
                    for acl in current_acl_response["NetworkAcls"]:
                        for association in acl.get("Associations", []):
                            if association.get("SubnetId") == private_subnet_id:
                                current_acl_id = acl["NetworkAclId"]
                                association_id = association["NetworkAclAssociationId"]
                                break

                    if current_acl_id == acl_id:
                        module_logger.info(f"Private subnet {private_subnet_id} already has permissive Network ACL")
                        continue

                    # Replace with permissive Network ACL
                    if association_id:
                        exponential_retry(
                            self._ec2_client.replace_network_acl_association,
                            {"RequestLimitExceeded", "ServiceUnavailable"},
                            AssociationId=association_id,
                            NetworkAclId=acl_id,
                        )
                        module_logger.info(f"Applied permissive Network ACL to private subnet {private_subnet_id} for internet access")
                    else:
                        module_logger.warning(f"Could not find Network ACL association for subnet {private_subnet_id}")

                except Exception as e:
                    module_logger.warning(f"Could not apply Network ACL to private subnet {private_subnet_id}: {e}")

        except Exception as e:
            module_logger.warning(f"Could not ensure internet access for private subnets: {e}")

    def create_internet_gateway(self, unique_context_id: str, vpc_id: str) -> str:
        """Create and attach Internet Gateway to VPC."""
        igw_response = exponential_retry(
            self._ec2_client.create_internet_gateway,
            {"RequestLimitExceeded", "ServiceUnavailable"},
        )
        igw_id = igw_response["InternetGateway"]["InternetGatewayId"]

        # Attach to VPC
        exponential_retry(self._ec2_client.attach_internet_gateway, {"RequestLimitExceeded"}, InternetGatewayId=igw_id, VpcId=vpc_id)

        # Tag Internet Gateway
        exponential_retry(
            self._ec2_client.create_tags,
            {"RequestLimitExceeded", "InvalidInternetGatewayID.NotFound"},
            Resources=[igw_id],
            Tags=[{"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-IGW"}],
        )

        module_logger.info(f"Created and attached Internet Gateway {igw_id}")
        return igw_id

    def create_subnets_across_azs(
        self, vpc_id: str, unique_context_id: str, subnet_count: int, start_index: int = 0, exclude_azs: List[str] = None
    ) -> Tuple[List[str], List[str]]:
        """Create public and private subnets across AZs.

        CIDR Allocation Strategy for Large EMR Clusters:
        - Private subnets use /18 CIDR blocks (16,379 usable IPs) to support very large EMR clusters
        - Public subnets use /24 CIDR blocks (251 usable IPs) sufficient for NAT gateways
        - VPC uses 10.0.0.0/16 providing 65,536 total addresses

        Allocation scheme:
        - Private subnet 0: 10.0.0.0/18   (10.0.0.0 - 10.0.63.255)
        - Private subnet 1: 10.0.64.0/18  (10.0.64.0 - 10.0.127.255)
        - Private subnet 2: 10.0.128.0/18 (10.0.128.0 - 10.0.191.255)
        - Public subnets:   10.0.192.0/24, 10.0.193.0/24, 10.0.194.0/24, etc.

        Args:
            vpc_id: VPC ID
            unique_context_id: Unique context ID for naming
            subnet_count: Number of subnet pairs to create
            start_index: Starting index for CIDR block calculation
            exclude_azs: List of AZs to exclude (already used by existing subnets)
        """
        available_azs = self.get_available_azs()

        # Filter out excluded AZs to ensure new subnets go to different AZs
        if exclude_azs:
            available_azs = [az for az in available_azs if az not in exclude_azs]
            module_logger.info(f"Creating subnets in AZs (excluding {exclude_azs}): {available_azs}")

        if len(available_azs) < subnet_count:
            module_logger.warning(f"Only {len(available_azs)} AZs available, reducing subnet count to {len(available_azs)}")
            subnet_count = len(available_azs)

        public_subnet_ids = []
        private_subnet_ids = []

        for i in range(subnet_count):
            az = available_azs[i % len(available_azs)]  # Cycle through AZs if more subnets than AZs
            cidr_index = start_index + i

            # Create private subnet with /18 CIDR (16,379 usable IPs for large EMR clusters)
            # Each /18 block spans 64 /24 blocks (e.g., 10.0.0.0/18 = 10.0.0.0 - 10.0.63.255)
            private_cidr = f"10.0.{cidr_index * 64}.0/18"
            private_subnet_response = exponential_retry(
                self._ec2_client.create_subnet,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                VpcId=vpc_id,
                CidrBlock=private_cidr,
                AvailabilityZone=az,
            )
            private_subnet_id = private_subnet_response["Subnet"]["SubnetId"]
            private_subnet_ids.append(private_subnet_id)

            # Tag private subnet with IP pool size for visibility
            exponential_retry(
                self._ec2_client.create_tags,
                {"RequestLimitExceeded", "InvalidSubnetID.NotFound"},
                Resources=[private_subnet_id],
                Tags=[
                    {"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-Private-{az}-{cidr_index}"},
                    {"Key": "IPPoolSize", "Value": "16379"},
                ],
            )

            # Create public subnet with /24 CIDR (251 usable IPs, sufficient for NAT gateways)
            # Allocate public subnets in the 10.0.192.0/20 range (10.0.192.0 - 10.0.207.255)
            # This avoids overlap with private /18 subnets (10.0.0.0/18, 10.0.64.0/18, 10.0.128.0/18)
            public_cidr = f"10.0.{192 + cidr_index}.0/24"
            public_subnet_response = exponential_retry(
                self._ec2_client.create_subnet,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                VpcId=vpc_id,
                CidrBlock=public_cidr,
                AvailabilityZone=az,
            )
            public_subnet_id = public_subnet_response["Subnet"]["SubnetId"]
            public_subnet_ids.append(public_subnet_id)

            # Tag public subnet
            exponential_retry(
                self._ec2_client.create_tags,
                {"RequestLimitExceeded", "InvalidSubnetID.NotFound"},
                Resources=[public_subnet_id],
                Tags=[{"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-Public-{az}-{cidr_index}"}],
            )

        module_logger.info(
            f"Created {subnet_count} subnet pairs across AZs: "
            f"Private subnets with /18 CIDR (~16,379 IPs each for large EMR clusters), "
            f"Public subnets with /24 CIDR (~251 IPs each)"
        )
        return public_subnet_ids, private_subnet_ids

    def _find_available_intelliflow_eips(self, unique_context_id: str) -> List[str]:
        """Find available IntelliFlow Elastic IPs that can be reused."""
        try:
            response = exponential_retry(
                self._ec2_client.describe_addresses,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "tag:IntelliFlow", "Values": [unique_context_id]}, {"Name": "domain", "Values": ["vpc"]}],
            )

            available_eips = []
            for address in response["Addresses"]:
                allocation_id = address["AllocationId"]
                # Check if EIP is not associated with any resource
                if not address.get("AssociationId") and not address.get("NetworkInterfaceId"):
                    module_logger.info(f"Found available orphaned EIP for reuse: {allocation_id}")
                    available_eips.append(allocation_id)

            return available_eips

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code not in ["InvalidAddress.NotFound", "InvalidFilter"]:
                module_logger.warning(f"Error searching for available EIPs: {e}")
            return []

    def create_nat_gateways(self, public_subnet_ids: List[str], unique_context_id: str) -> List[str]:
        """Create NAT gateways in public subnets, reusing available Elastic IPs when possible."""
        nat_gateway_ids = []

        # First, try to find any available IntelliFlow EIPs that can be reused
        available_eips = self._find_available_intelliflow_eips(unique_context_id)
        module_logger.info(f"Found {len(available_eips)} available EIPs for potential reuse")

        for i, public_subnet_id in enumerate(public_subnet_ids):
            try:
                # Try to reuse existing EIP first, then allocate new if needed
                if i < len(available_eips):
                    allocation_id = available_eips[i]
                    module_logger.info(f"Reusing existing EIP {allocation_id} for NAT gateway {i}")

                    # Update tags for the reused EIP
                    try:
                        exponential_retry(
                            self._ec2_client.create_tags,
                            {"RequestLimitExceeded", "InvalidAllocationID.NotFound"},
                            Resources=[allocation_id],
                            Tags=[
                                {"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-EIP-{i}"},
                                {"Key": "IntelliFlow", "Value": unique_context_id},
                                {"Key": "Purpose", "Value": "NAT-Gateway"},
                                {"Key": "Status", "Value": "Reused"},
                            ],
                        )
                    except ClientError as tag_error:
                        module_logger.warning(f"Could not update tags for reused Elastic IP {allocation_id}: {tag_error}")
                else:
                    # Allocate new Elastic IP
                    eip_response = exponential_retry(
                        self._ec2_client.allocate_address, {"RequestLimitExceeded", "ServiceUnavailable"}, Domain="vpc"
                    )
                    allocation_id = eip_response["AllocationId"]
                    module_logger.info(f"Allocated new EIP {allocation_id} for NAT gateway {i}")

                    # Tag new Elastic IP for tracking
                    try:
                        exponential_retry(
                            self._ec2_client.create_tags,
                            {"RequestLimitExceeded", "InvalidAllocationID.NotFound"},
                            Resources=[allocation_id],
                            Tags=[
                                {"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-EIP-{i}"},
                                {"Key": "IntelliFlow", "Value": unique_context_id},
                                {"Key": "Purpose", "Value": "NAT-Gateway"},
                                {"Key": "Status", "Value": "New"},
                            ],
                        )
                    except ClientError as tag_error:
                        module_logger.warning(f"Could not tag new Elastic IP {allocation_id}: {tag_error}")

                # Create NAT Gateway
                nat_response = exponential_retry(
                    self._ec2_client.create_nat_gateway,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    SubnetId=public_subnet_id,
                    AllocationId=allocation_id,
                )
                nat_gateway_id = nat_response["NatGateway"]["NatGatewayId"]
                nat_gateway_ids.append(nat_gateway_id)

                # Tag NAT Gateway
                exponential_retry(
                    self._ec2_client.create_tags,
                    {"RequestLimitExceeded", "InvalidNatGatewayID.NotFound"},
                    Resources=[nat_gateway_id],
                    Tags=[
                        {"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-NAT-{i}"},
                        {"Key": "IntelliFlow", "Value": unique_context_id},
                    ],
                )

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "AddressLimitExceeded":
                    available_eips_info = f" Found {len(available_eips)} available EIPs for reuse, needed {len(public_subnet_ids)} total."
                    module_logger.error(
                        f"Failed to allocate Elastic IP for NAT gateway: {e}{available_eips_info}\n"
                        f"RECOVERY STEPS:\n"
                        f"1. Go to AWS Console → EC2 → Elastic IPs\n"
                        f"2. Look for EIPs tagged with 'IntelliFlow' or 'Not associated'\n"
                        f"3. Release any unassociated Elastic IPs\n"
                        f"4. Check if previous VPC cleanup left orphaned EIPs\n"
                        f"5. Retry activation after cleanup"
                    )
                raise

        module_logger.info(
            f"Created {len(nat_gateway_ids)} NAT gateways ({len([eip for eip in available_eips[:len(public_subnet_ids)]])} reused EIPs, {max(0, len(public_subnet_ids) - len(available_eips))} new EIPs)"
        )
        return nat_gateway_ids

    def create_s3_vpc_endpoint(self, vpc_id: str, route_table_ids: List[str], unique_context_id: str) -> str:
        """Create S3 VPC endpoint for reliable S3 access from private subnets."""
        try:
            # Create S3 VPC endpoint
            endpoint_response = exponential_retry(
                self._ec2.meta.client.create_vpc_endpoint,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                VpcId=vpc_id,
                ServiceName=f"com.amazonaws.{self._region}.s3",
                VpcEndpointType="Gateway",
                RouteTableIds=route_table_ids,
            )
            endpoint_id = endpoint_response["VpcEndpoint"]["VpcEndpointId"]

            # Tag S3 VPC endpoint
            exponential_retry(
                self._ec2_client.create_tags,
                {"RequestLimitExceeded", "InvalidVpcEndpointId.NotFound"},
                Resources=[endpoint_id],
                Tags=[
                    {"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-S3-Endpoint"},
                    {"Key": "IntelliFlow", "Value": unique_context_id},
                    {"Key": "Service", "Value": "S3"},
                ],
            )

            module_logger.info(f"Created S3 VPC endpoint {endpoint_id} for VPC {vpc_id}")
            return endpoint_id

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "RouteAlreadyExists":
                # S3 endpoint might already exist with these route tables
                module_logger.info(f"S3 VPC endpoint route already exists for VPC {vpc_id}")
                # Try to find existing S3 endpoint
                try:
                    existing_endpoints = self.get_vpc_s3_endpoints(vpc_id)
                    if existing_endpoints:
                        module_logger.info(f"Using existing S3 VPC endpoint {existing_endpoints[0]}")
                        return existing_endpoints[0]
                except Exception as lookup_error:
                    module_logger.warning(f"Could not find existing S3 endpoint: {lookup_error}")
            raise

    def get_vpc_s3_endpoints(self, vpc_id: str) -> List[str]:
        """Get S3 VPC endpoint IDs for the given VPC."""
        try:
            response = exponential_retry(
                self._ec2.meta.client.describe_vpc_endpoints,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "service-name", "Values": [f"com.amazonaws.{self._region}.s3"]},
                ],
            )
            return [endpoint["VpcEndpointId"] for endpoint in response["VpcEndpoints"]]

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ["InvalidVpcEndpointId.NotFound", "InvalidFilter"]:
                return []
            module_logger.error(f"Failed to get S3 VPC endpoints for {vpc_id}: {e}")
            raise

    def ensure_s3_vpc_endpoint(self, vpc_id: str, unique_context_id: str) -> Optional[str]:
        """Ensure S3 VPC endpoint exists for the given VPC with correct route table associations.

        IDEMPOTENT: Checks and updates route table associations if endpoint exists.
        """
        try:
            # Get all IntelliFlow private route tables for this VPC
            rt_response = exponential_retry(
                self._ec2_client.describe_route_tables,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "tag:Name", "Values": [f"IntelliFlow-{unique_context_id}-Private-RT*"]},
                ],
            )

            private_route_table_ids = [rt["RouteTableId"] for rt in rt_response["RouteTables"]]

            if not private_route_table_ids:
                module_logger.warning(f"No IntelliFlow private route tables found in VPC {vpc_id}")
                return None

            # Check if S3 endpoint already exists
            existing_endpoints = self.get_vpc_s3_endpoints(vpc_id)

            if existing_endpoints:
                endpoint_id = existing_endpoints[0]
                module_logger.info(f"S3 VPC endpoint already exists: {endpoint_id}")

                # IDEMPOTENCY FIX: Verify and update route table associations
                try:
                    endpoint_details = exponential_retry(
                        self._ec2.meta.client.describe_vpc_endpoints,
                        {"RequestLimitExceeded", "ServiceUnavailable"},
                        VpcEndpointIds=[endpoint_id],
                    )

                    if endpoint_details["VpcEndpoints"]:
                        current_rt_ids = set(endpoint_details["VpcEndpoints"][0].get("RouteTableIds", []))
                        required_rt_ids = set(private_route_table_ids)

                        # Find route tables that need to be added
                        missing_rt_ids = required_rt_ids - current_rt_ids

                        if missing_rt_ids:
                            module_logger.info(
                                f"Adding {len(missing_rt_ids)} missing route tables to S3 endpoint {endpoint_id}: {missing_rt_ids}"
                            )
                            exponential_retry(
                                self._ec2.meta.client.modify_vpc_endpoint,
                                {"RequestLimitExceeded", "ServiceUnavailable"},
                                VpcEndpointId=endpoint_id,
                                AddRouteTableIds=list(missing_rt_ids),
                            )
                            module_logger.info(f"Updated S3 VPC endpoint {endpoint_id} with missing route tables")
                        else:
                            module_logger.info(f"S3 VPC endpoint {endpoint_id} already has all required route tables")

                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["InvalidVpcEndpointId.NotFound"]:
                        module_logger.warning(f"S3 endpoint {endpoint_id} not found, will create new one")
                        # Fall through to create new endpoint
                    else:
                        raise
                else:
                    # Successfully verified/updated associations
                    return endpoint_id

            # Create new S3 VPC endpoint with private route tables
            endpoint_id = self.create_s3_vpc_endpoint(vpc_id, private_route_table_ids, unique_context_id)
            module_logger.info(f"Created S3 VPC endpoint {endpoint_id} for reliable EMR bootstrap downloads")
            return endpoint_id

        except Exception as e:
            module_logger.warning(f"Failed to ensure S3 VPC endpoint for VPC {vpc_id}: {e}")
            return None

    def wait_for_nat_gateways_available(self, nat_gateway_ids: List[str], timeout_minutes: int = 10) -> None:
        """Wait for NAT gateways to become available."""
        if not nat_gateway_ids:
            return

        module_logger.info("Waiting for NAT gateways to become available...")

        timeout_seconds = timeout_minutes * 60
        start_time = time.time()

        while time.time() - start_time < timeout_seconds:
            try:
                response = exponential_retry(
                    self._ec2_client.describe_nat_gateways, {"RequestLimitExceeded"}, NatGatewayIds=nat_gateway_ids
                )

                all_available = all(nat["State"] == "available" for nat in response["NatGateways"])
                if all_available:
                    module_logger.info("All NAT gateways are now available")
                    return

                module_logger.info("Waiting for NAT gateways to be ready...")
                time.sleep(30)

            except Exception as e:
                module_logger.error(f"Error checking NAT gateway status: {e}")
                raise

        # raise RuntimeError("Timeout waiting for NAT gateways to become available")
        module_logger.error("Timeout waiting for NAT gateways to become available! Please retry if routing setup fails.")

    def configure_routing(
        self,
        vpc_id: str,
        igw_id: str,
        public_subnet_ids: List[str],
        private_subnet_ids: List[str],
        nat_gateway_ids: List[str],
        unique_context_id: str,
    ) -> None:
        """Configure route tables for public and private subnets."""
        # Create route table for public subnets
        public_rt_response = exponential_retry(
            self._ec2_client.create_route_table, {"RequestLimitExceeded", "ServiceUnavailable"}, VpcId=vpc_id
        )
        public_route_table_id = public_rt_response["RouteTable"]["RouteTableId"]

        # Tag public route table
        exponential_retry(
            self._ec2_client.create_tags,
            {"RequestLimitExceeded", "InvalidRouteTableID.NotFound"},
            Resources=[public_route_table_id],
            Tags=[{"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-Public-RT"}],
        )

        # Add route to Internet Gateway for public subnets
        exponential_retry(
            self._ec2_client.create_route,
            {"RequestLimitExceeded"},
            RouteTableId=public_route_table_id,
            DestinationCidrBlock="0.0.0.0/0",
            GatewayId=igw_id,
        )

        # Associate public subnets with public route table
        for public_subnet_id in public_subnet_ids:
            exponential_retry(
                self._ec2_client.associate_route_table,
                {"RequestLimitExceeded"},
                SubnetId=public_subnet_id,
                RouteTableId=public_route_table_id,
            )

        # Create route table for each private subnet (for NAT gateway routing)
        private_route_table_ids = []
        for i, (private_subnet_id, nat_gateway_id) in enumerate(zip(private_subnet_ids, nat_gateway_ids)):
            private_rt_response = exponential_retry(
                self._ec2_client.create_route_table, {"RequestLimitExceeded", "ServiceUnavailable"}, VpcId=vpc_id
            )
            private_route_table_id = private_rt_response["RouteTable"]["RouteTableId"]
            private_route_table_ids.append(private_route_table_id)

            # Tag private route table
            exponential_retry(
                self._ec2_client.create_tags,
                {"RequestLimitExceeded", "InvalidRouteTableID.NotFound"},
                Resources=[private_route_table_id],
                Tags=[{"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-Private-RT-{i}"}],
            )

            # Add route to NAT Gateway for private subnet
            exponential_retry(
                self._ec2_client.create_route,
                {"RequestLimitExceeded"},
                RouteTableId=private_route_table_id,
                DestinationCidrBlock="0.0.0.0/0",
                NatGatewayId=nat_gateway_id,
            )

            # Associate private subnet with private route table
            exponential_retry(
                self._ec2_client.associate_route_table,
                {"RequestLimitExceeded"},
                SubnetId=private_subnet_id,
                RouteTableId=private_route_table_id,
            )

        module_logger.info("VPC routing configuration completed")

        # Create S3 VPC endpoint for reliable S3 access from private subnets using the route table IDs we just created
        if private_route_table_ids:
            try:
                self.create_s3_vpc_endpoint(vpc_id, private_route_table_ids, unique_context_id)
            except Exception as e:
                module_logger.warning(f"Could not create S3 VPC endpoint (EMR will use NAT gateway): {e}")

    def _configure_additional_routing(
        self,
        vpc_id: str,
        igw_id: str,
        new_public_subnet_ids: List[str],
        new_private_subnet_ids: List[str],
        new_nat_gateway_ids: List[str],
        unique_context_id: str,
    ) -> None:
        """
        Configure routing for additional subnets added to an existing VPC.

        CRITICAL FIX: This method is called when adding subnets during idempotent operations.
        The bug was that new public subnets didn't get IGW routes, causing NAT gateways
        to be unable to forward traffic (connection timeout).
        """

        # CRITICAL: Ensure new public subnets have IGW routes FIRST
        # This MUST happen before NAT gateways are created or they will fail
        if new_public_subnet_ids and igw_id:
            module_logger.info(
                f"CRITICAL FIX: Ensuring {len(new_public_subnet_ids)} new public subnets "
                f"have Internet Gateway routes before NAT gateway creation..."
            )
            self._ensure_public_subnets_have_internet_gateway_routes(vpc_id, igw_id, new_public_subnet_ids, unique_context_id)
            module_logger.info("✓ New public subnets now have IGW routes for NAT gateway connectivity")

        # Get existing public route table to associate new public subnets
        # Note: The above IGW route check handles route creation, this just ensures association
        try:
            public_rt_response = exponential_retry(
                self._ec2_client.describe_route_tables,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "tag:Name", "Values": [f"IntelliFlow-{unique_context_id}-Public-RT"]},
                ],
            )

            if public_rt_response["RouteTables"]:
                public_route_table_id = public_rt_response["RouteTables"][0]["RouteTableId"]

                # Associate new public subnets with existing public route table (if not already done by IGW route check)
                for public_subnet_id in new_public_subnet_ids:
                    try:
                        exponential_retry(
                            self._ec2_client.associate_route_table,
                            {"RequestLimitExceeded", "Resource.AlreadyAssociated"},
                            SubnetId=public_subnet_id,
                            RouteTableId=public_route_table_id,
                        )
                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code", "")
                        if error_code == "Resource.AlreadyAssociated":
                            module_logger.info(f"Public subnet {public_subnet_id} already associated with route table")
                        else:
                            raise
            else:
                module_logger.warning("Could not find existing public route table, will create routing for new subnets")
                # Fall back to full routing configuration
                self.configure_routing(
                    vpc_id, igw_id, new_public_subnet_ids, new_private_subnet_ids, new_nat_gateway_ids, unique_context_id
                )
                return

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code not in ["Resource.AlreadyAssociated"]:
                module_logger.warning(f"Error finding existing public route table: {e}")
                self.configure_routing(
                    vpc_id, igw_id, new_public_subnet_ids, new_private_subnet_ids, new_nat_gateway_ids, unique_context_id
                )
                return

        # Create route tables for new private subnets
        for i, (private_subnet_id, nat_gateway_id) in enumerate(zip(new_private_subnet_ids, new_nat_gateway_ids)):
            private_rt_response = exponential_retry(
                self._ec2_client.create_route_table, {"RequestLimitExceeded", "ServiceUnavailable"}, VpcId=vpc_id
            )
            private_route_table_id = private_rt_response["RouteTable"]["RouteTableId"]

            # Tag private route table
            exponential_retry(
                self._ec2_client.create_tags,
                {"RequestLimitExceeded", "InvalidRouteTableID.NotFound"},
                Resources=[private_route_table_id],
                Tags=[{"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-Private-RT-Additional-{i}"}],
            )

            # Add route to NAT Gateway for private subnet
            exponential_retry(
                self._ec2_client.create_route,
                {"RequestLimitExceeded"},
                RouteTableId=private_route_table_id,
                DestinationCidrBlock="0.0.0.0/0",
                NatGatewayId=nat_gateway_id,
            )

            # Associate private subnet with private route table
            exponential_retry(
                self._ec2_client.associate_route_table,
                {"RequestLimitExceeded"},
                SubnetId=private_subnet_id,
                RouteTableId=private_route_table_id,
            )

        module_logger.info(
            f"Configured routing for {len(new_private_subnet_ids)} additional subnet pairs "
            f"(public subnets have IGW routes, private subnets have NAT routes)"
        )

    def _get_or_create_public_route_table(self, vpc_id: str, igw_id: str, unique_context_id: str) -> Optional[str]:
        """Get existing public route table or create one with Internet Gateway route."""
        try:
            # Try to find existing IntelliFlow public route table
            rt_response = exponential_retry(
                self._ec2_client.describe_route_tables,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "tag:Name", "Values": [f"IntelliFlow-{unique_context_id}-Public-RT"]},
                ],
            )

            if rt_response["RouteTables"]:
                public_route_table_id = rt_response["RouteTables"][0]["RouteTableId"]
                module_logger.info(f"Found existing public route table {public_route_table_id}")
                return public_route_table_id

            # Create new public route table with Internet Gateway route
            module_logger.info("Creating new public route table with Internet Gateway route")
            public_rt_response = exponential_retry(
                self._ec2_client.create_route_table, {"RequestLimitExceeded", "ServiceUnavailable"}, VpcId=vpc_id
            )
            public_route_table_id = public_rt_response["RouteTable"]["RouteTableId"]

            # Tag public route table
            exponential_retry(
                self._ec2_client.create_tags,
                {"RequestLimitExceeded", "InvalidRouteTableID.NotFound"},
                Resources=[public_route_table_id],
                Tags=[{"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-Public-RT"}],
            )

            # Add route to Internet Gateway
            exponential_retry(
                self._ec2_client.create_route,
                {"RequestLimitExceeded"},
                RouteTableId=public_route_table_id,
                DestinationCidrBlock="0.0.0.0/0",
                GatewayId=igw_id,
            )

            module_logger.info(f"Created public route table {public_route_table_id} with Internet Gateway route")
            return public_route_table_id

        except Exception as e:
            module_logger.error(f"Failed to get or create public route table: {e}")
            return None

    def _ensure_public_subnets_have_internet_gateway_routes(
        self, vpc_id: str, igw_id: str, public_subnet_ids: List[str], unique_context_id: str
    ) -> None:
        """CRITICAL FIX: Ensure public subnets have Internet Gateway routes for NAT gateway connectivity.

        This addresses the 'Network is unreachable' error when NAT gateways can't reach internet
        due to missing Internet Gateway routes in public subnets.
        """
        if not public_subnet_ids or not igw_id:
            module_logger.warning("Missing public subnets or Internet Gateway ID, cannot ensure IGW routes")
            return

        # Try to get or create a shared public route table
        public_route_table_id = self._get_or_create_public_route_table(vpc_id, igw_id, unique_context_id)
        if not public_route_table_id:
            module_logger.error("Could not get or create public route table, cannot ensure Internet Gateway routes")
            return

        for public_subnet_id in public_subnet_ids:
            try:
                # Check current route table for this public subnet
                subnet_rt_response = exponential_retry(
                    self._ec2_client.describe_route_tables,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "association.subnet-id", "Values": [public_subnet_id]}, {"Name": "vpc-id", "Values": [vpc_id]}],
                )

                if not subnet_rt_response["RouteTables"]:
                    # No route table found - associate with the public route table (fixes the bug!)
                    module_logger.info(f"No route table found for public subnet {public_subnet_id}, associating with public route table")

                    exponential_retry(
                        self._ec2_client.associate_route_table,
                        {"RequestLimitExceeded"},
                        SubnetId=public_subnet_id,
                        RouteTableId=public_route_table_id,
                    )
                    module_logger.info(
                        f"Successfully associated public subnet {public_subnet_id} with public route table {public_route_table_id}"
                    )
                    continue

                # Check if Internet Gateway route exists
                route_table = subnet_rt_response["RouteTables"][0]
                route_table_id = route_table["RouteTableId"]

                has_igw_route = any(
                    route.get("DestinationCidrBlock") == "0.0.0.0/0" and route.get("GatewayId") == igw_id
                    for route in route_table.get("Routes", [])
                )

                if has_igw_route:
                    module_logger.info(f"Public subnet {public_subnet_id} already has Internet Gateway route")
                    continue

                # Add missing Internet Gateway route to public subnet
                module_logger.info(f"Adding missing Internet Gateway route to public subnet {public_subnet_id}")
                exponential_retry(
                    self._ec2_client.create_route,
                    {"RequestLimitExceeded", "RouteAlreadyExists"},
                    RouteTableId=route_table_id,
                    DestinationCidrBlock="0.0.0.0/0",
                    GatewayId=igw_id,
                )
                module_logger.info(f"Successfully added Internet Gateway route 0.0.0.0/0 → {igw_id} to public subnet {public_subnet_id}")

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "RouteAlreadyExists":
                    module_logger.info(f"Internet Gateway route already exists for public subnet {public_subnet_id}")
                elif error_code == "Resource.AlreadyAssociated":
                    module_logger.info(f"Public subnet {public_subnet_id} already associated with a route table")
                else:
                    module_logger.error(f"Failed to add Internet Gateway route to public subnet {public_subnet_id}: {e}")
                    raise
            except Exception as e:
                module_logger.error(f"Unexpected error ensuring IGW route for public subnet {public_subnet_id}: {e}")
                raise

    def _apply_complete_network_connectivity(
        self,
        vpc_id: str,
        igw_id: str,
        public_subnet_ids: List[str],
        private_subnet_ids: List[str],
        unique_context_id: str,
    ) -> None:
        """
        Apply COMPLETE network connectivity setup for subnets.

        This is the central method that ensures subnets have ALL required connectivity:
        1. Public subnets get Internet Gateway routes (MUST happen before NAT setup)
        2. NAT gateways are created in public subnets
        3. Private subnets get route tables with NAT gateway routes
        4. S3 VPC endpoint is added to private route tables for bootstrap reliability
        5. Permissive Network ACL is applied to private subnets for internet access

        IDEMPOTENT: Can be called multiple times safely - checks existing setup first.

        Args:
            vpc_id: VPC ID
            igw_id: Internet Gateway ID
            public_subnet_ids: List of public subnet IDs
            private_subnet_ids: List of private subnet IDs
            unique_context_id: Unique context ID for resource naming
        """
        if not public_subnet_ids or not private_subnet_ids:
            module_logger.warning("No subnets provided for network connectivity setup")
            return

        module_logger.info(
            f"Applying complete network connectivity for {len(public_subnet_ids)} public " f"and {len(private_subnet_ids)} private subnets"
        )

        # STEP 1: CRITICAL - Ensure public subnets have IGW routes FIRST
        # NAT gateways need internet connectivity via IGW to function properly
        module_logger.info("Step 1: Ensuring public subnets have Internet Gateway routes...")
        self._ensure_public_subnets_have_internet_gateway_routes(vpc_id, igw_id, public_subnet_ids, unique_context_id)

        # STEP 2: Create NAT gateways in public subnets
        module_logger.info("Step 2: Creating NAT gateways in public subnets...")
        nat_gateway_ids = self.create_nat_gateways(public_subnet_ids, unique_context_id)

        # STEP 3: Wait for NAT gateways to become fully operational
        module_logger.info("Step 3: Waiting for NAT gateways to become operational...")
        self.wait_for_nat_gateways_available(nat_gateway_ids, timeout_minutes=5)

        # STEP 4: Configure routing for private subnets (NAT gateway routes)
        module_logger.info("Step 4: Configuring private subnet routing with NAT gateways...")
        private_route_table_ids = []
        for i, (private_subnet_id, nat_gateway_id) in enumerate(zip(private_subnet_ids, nat_gateway_ids)):
            # Create route table for private subnet
            private_rt_response = exponential_retry(
                self._ec2_client.create_route_table,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                VpcId=vpc_id,
            )
            private_route_table_id = private_rt_response["RouteTable"]["RouteTableId"]
            private_route_table_ids.append(private_route_table_id)

            # Tag private route table
            exponential_retry(
                self._ec2_client.create_tags,
                {"RequestLimitExceeded", "InvalidRouteTableID.NotFound"},
                Resources=[private_route_table_id],
                Tags=[{"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-Private-RT-New-{i}"}],
            )

            # Add route to NAT Gateway
            exponential_retry(
                self._ec2_client.create_route,
                {"RequestLimitExceeded"},
                RouteTableId=private_route_table_id,
                DestinationCidrBlock="0.0.0.0/0",
                NatGatewayId=nat_gateway_id,
            )

            # Associate with private subnet
            exponential_retry(
                self._ec2_client.associate_route_table,
                {"RequestLimitExceeded"},
                SubnetId=private_subnet_id,
                RouteTableId=private_route_table_id,
            )

        module_logger.info(f"Created {len(private_route_table_ids)} route tables for private subnets")

        # STEP 5: Add S3 VPC endpoint to private route tables for bootstrap reliability
        if private_route_table_ids:
            module_logger.info("Step 5: Configuring S3 VPC endpoint for bootstrap reliability...")
            try:
                existing_endpoints = self.get_vpc_s3_endpoints(vpc_id)
                if existing_endpoints:
                    # Update existing S3 endpoint to include new route tables
                    endpoint_id = existing_endpoints[0]
                    exponential_retry(
                        self._ec2.meta.client.modify_vpc_endpoint,
                        {"RequestLimitExceeded", "ServiceUnavailable"},
                        VpcEndpointId=endpoint_id,
                        AddRouteTableIds=private_route_table_ids,
                    )
                    module_logger.info(f"Added {len(private_route_table_ids)} route tables to existing S3 endpoint")
                else:
                    # Create new S3 endpoint
                    self.create_s3_vpc_endpoint(vpc_id, private_route_table_ids, unique_context_id)
                    module_logger.info(f"Created S3 VPC endpoint for {len(private_route_table_ids)} route tables")
            except Exception as s3_error:
                module_logger.warning(f"Could not configure S3 VPC endpoint (EMR will use NAT): {s3_error}")

        # STEP 6: Apply permissive Network ACL to private subnets for internet access
        module_logger.info("Step 6: Applying permissive Network ACL for internet access...")
        try:
            self.ensure_private_subnets_internet_access(vpc_id, private_subnet_ids, unique_context_id)
            module_logger.info("Applied permissive Network ACL to private subnets")
        except Exception as acl_error:
            module_logger.warning(f"Could not apply Network ACL (may affect pip/bootstrap): {acl_error}")

        module_logger.info(
            f"✓ Complete network connectivity applied successfully: "
            f"{len(public_subnet_ids)} public + {len(private_subnet_ids)} private subnets, "
            f"{len(nat_gateway_ids)} NAT gateways"
        )

    def _configure_nat_gateway_routing_for_existing_subnets(
        self, vpc_id: str, private_subnet_ids: List[str], nat_gateway_ids: List[str], unique_context_id: str
    ) -> None:
        """Configure NAT gateway routing for existing private subnets that lack NAT gateway connectivity."""

        if not private_subnet_ids or not nat_gateway_ids:
            return

        module_logger.info(f"Configuring NAT gateway routing for {len(private_subnet_ids)} existing private subnets")

        # Create route tables for existing private subnets that need NAT gateway connectivity
        for i, (private_subnet_id, nat_gateway_id) in enumerate(zip(private_subnet_ids, nat_gateway_ids)):
            # First, check if the subnet already has a custom route table association
            try:
                subnet_rt_response = exponential_retry(
                    self._ec2_client.describe_route_tables,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "association.subnet-id", "Values": [private_subnet_id]}, {"Name": "vpc-id", "Values": [vpc_id]}],
                )

                existing_route_tables = [
                    rt
                    for rt in subnet_rt_response["RouteTables"]
                    if not any(assoc.get("Main", False) for assoc in rt.get("Associations", []))
                ]

                if existing_route_tables:
                    # Subnet already has custom routing - skip it
                    module_logger.info(f"Subnet {private_subnet_id} already has custom routing, skipping NAT gateway configuration")
                    continue

            except ClientError as e:
                module_logger.warning(f"Could not check existing routing for subnet {private_subnet_id}: {e}")

            # Create new route table for this private subnet
            private_rt_response = exponential_retry(
                self._ec2_client.create_route_table, {"RequestLimitExceeded", "ServiceUnavailable"}, VpcId=vpc_id
            )
            private_route_table_id = private_rt_response["RouteTable"]["RouteTableId"]

            # Tag private route table
            exponential_retry(
                self._ec2_client.create_tags,
                {"RequestLimitExceeded", "InvalidRouteTableID.NotFound"},
                Resources=[private_route_table_id],
                Tags=[{"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-Private-RT-Existing-{i}"}],
            )

            # Add route to NAT Gateway for private subnet
            exponential_retry(
                self._ec2_client.create_route,
                {"RequestLimitExceeded"},
                RouteTableId=private_route_table_id,
                DestinationCidrBlock="0.0.0.0/0",
                NatGatewayId=nat_gateway_id,
            )

            # Associate private subnet with new private route table
            exponential_retry(
                self._ec2_client.associate_route_table,
                {"RequestLimitExceeded"},
                SubnetId=private_subnet_id,
                RouteTableId=private_route_table_id,
            )

        module_logger.info(f"Configured NAT gateway routing for {len(private_subnet_ids)} existing private subnets")

    def vpc_exists(self, unique_context_id: str) -> Optional[str]:
        """Check if VPC with IntelliFlow naming pattern exists."""
        vpc_name = f"IntelliFlow-{unique_context_id}-VPC"
        try:
            # First try with exact name and available state
            response = exponential_retry(
                self._ec2_client.describe_vpcs,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "tag:Name", "Values": [vpc_name]}, {"Name": "state", "Values": ["available"]}],
            )
            if response["Vpcs"]:
                # If multiple VPCs with same name exist, pick the first available one
                vpc_id = response["Vpcs"][0]["VpcId"]
                if len(response["Vpcs"]) > 1:
                    all_vpc_ids = [vpc["VpcId"] for vpc in response["Vpcs"]]
                    module_logger.warning(f"Found {len(response['Vpcs'])} VPCs with name '{vpc_name}': {all_vpc_ids}")
                    module_logger.warning(f"Selecting first available VPC: {vpc_id} (others may need manual cleanup)")
                else:
                    module_logger.info(f"Found existing available VPC: {vpc_id} ({vpc_name})")
                return vpc_id

            # If not found, also check for VPCs in other states that might be usable
            response = exponential_retry(
                self._ec2_client.describe_vpcs,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "tag:Name", "Values": [vpc_name]}],
            )
            if response["Vpcs"]:
                available_vpcs = [vpc for vpc in response["Vpcs"] if vpc["State"] == "available"]
                if available_vpcs:
                    vpc_id = available_vpcs[0]["VpcId"]
                    if len(response["Vpcs"]) > 1:
                        all_vpc_ids = [vpc["VpcId"] for vpc in response["Vpcs"]]
                        module_logger.warning(f"Found {len(response['Vpcs'])} VPCs with name '{vpc_name}': {all_vpc_ids}")
                    module_logger.info(f"Found available VPC among {len(response['Vpcs'])} total: {vpc_id}")
                    return vpc_id

                # Log all non-available VPCs for troubleshooting
                for vpc in response["Vpcs"]:
                    vpc_id = vpc["VpcId"]
                    state = vpc["State"]
                    module_logger.warning(f"Found existing VPC {vpc_id} in state '{state}' - may need manual cleanup")

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code not in ["InvalidVpcID.NotFound", "InvalidFilter"]:
                module_logger.error(f"Error checking VPC existence: {e}")
                raise
            # VPC not found or invalid filter - expected case
            module_logger.debug(f"VPC {vpc_name} not found: {error_code}")
        return None

    def get_vpc_subnets(self, vpc_id: str, unique_context_id: str = None) -> Tuple[List[str], List[str]]:
        """Get public and private subnet IDs from VPC, filtering to IntelliFlow-managed only if context provided.

        Args:
            vpc_id: VPC ID to query
            unique_context_id: If provided, only return IntelliFlow-managed subnets for this context
        """
        try:
            response = exponential_retry(
                self._ec2_client.describe_subnets,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
            )

            public_subnets = []
            private_subnets = []

            for subnet in response["Subnets"]:
                subnet_id = subnet["SubnetId"]
                # Check tags to determine subnet type
                tags = {tag["Key"]: tag["Value"] for tag in subnet.get("Tags", [])}
                name = tags.get("Name", "")

                # CRITICAL: Filter to IntelliFlow-managed subnets only if context provided
                # This prevents EMR from randomly picking up non-IF managed subnets
                if unique_context_id:
                    expected_prefix = f"IntelliFlow-{unique_context_id}"
                    if not name.startswith(expected_prefix):
                        module_logger.debug(f"Skipping non-IntelliFlow subnet {subnet_id} with name '{name}'")
                        continue

                if "Public" in name:
                    public_subnets.append(subnet_id)
                elif "Private" in name:
                    private_subnets.append(subnet_id)

            return public_subnets, private_subnets

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "InvalidVpcID.NotFound":
                module_logger.warning(f"VPC {vpc_id} not found")
                return [], []
            module_logger.error(f"Failed to get VPC subnets for {vpc_id}: {e}")
            raise

    def get_subnet_azs(self, subnet_ids: List[str]) -> List[str]:
        """Get list of Availability Zones used by given subnets.

        Returns unique AZs, preserving order of first occurrence.
        """
        if not subnet_ids:
            return []

        try:
            response = exponential_retry(
                self._ec2_client.describe_subnets,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                SubnetIds=subnet_ids,
            )

            azs = []
            seen_azs = set()
            for subnet in response["Subnets"]:
                az = subnet["AvailabilityZone"]
                if az not in seen_azs:
                    azs.append(az)
                    seen_azs.add(az)

            return azs

        except ClientError as e:
            module_logger.error(f"Failed to get AZs for subnets {subnet_ids}: {e}")
            raise

    def select_subnets_across_different_azs(self, subnet_ids: List[str], required_count: int = 3) -> List[str]:
        """Select subnets ensuring they are in different Availability Zones.

        CRITICAL FIX: Redshift Serverless requires subnets in different AZs.
        This method ensures the selected subnets meet this requirement.

        Args:
            subnet_ids: List of candidate subnet IDs
            required_count: Number of subnets needed (default 3 for Redshift)

        Returns:
            List of subnet IDs in different AZs, up to required_count

        Raises:
            ValueError: If insufficient subnets across different AZs are available
        """
        if not subnet_ids:
            raise ValueError(f"No subnet IDs provided - cannot select {required_count} subnets across different AZs")

        try:
            # Get subnet details including AZ information
            response = exponential_retry(
                self._ec2_client.describe_subnets,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                SubnetIds=subnet_ids,
            )

            # Group subnets by AZ
            az_to_subnets = {}
            for subnet in response["Subnets"]:
                subnet_id = subnet["SubnetId"]
                az = subnet["AvailabilityZone"]
                if az not in az_to_subnets:
                    az_to_subnets[az] = []
                az_to_subnets[az].append(subnet_id)

            # Select one subnet from each different AZ
            selected_subnets = []
            for az, subnets in az_to_subnets.items():
                if len(selected_subnets) >= required_count:
                    break
                # Take first subnet from this AZ
                selected_subnets.append(subnets[0])

            if len(selected_subnets) < required_count:
                selected_azs = list(az_to_subnets.keys())
                az_distribution = {az: len(subnets) for az, subnets in az_to_subnets.items()}
                error_msg = (
                    f"INSUFFICIENT AVAILABILITY ZONE DISTRIBUTION: Only found {len(selected_subnets)} subnets "
                    f"across {len(selected_azs)} different AZs: {selected_azs}.\n"
                    f"Redshift Serverless requires at least {required_count} subnets in {required_count} different Availability Zones.\n"
                    f"Current AZ distribution: {az_distribution}\n"
                    f"Total candidate subnets: {len(subnet_ids)}\n\n"
                    f"RESOLUTION:\n"
                    f"1. Ensure VPC has at least {required_count} private subnets in different AZs\n"
                    f"2. Delete the VPC and let IntelliFlow recreate it properly\n"
                    f"3. Or manually create subnets in missing AZs"
                )
                module_logger.error(error_msg)
                raise ValueError(error_msg)

            selected_azs = [az for az in az_to_subnets.keys() if any(sid in selected_subnets for sid in az_to_subnets[az])]
            module_logger.info(
                f"Selected {len(selected_subnets)} subnets across {len(selected_azs)} different AZs for Redshift: {selected_azs}"
            )

            return selected_subnets

        except ClientError as e:
            module_logger.error(f"AWS API error selecting subnets across different AZs: {e}")
            raise
        except ValueError:
            # Re-raise ValueError as-is (it's our custom error with clear message)
            raise
        except Exception as e:
            error_msg = f"Unexpected error selecting subnets across different AZs: {e}"
            module_logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def get_vpc_nat_gateways(self, vpc_id: str) -> List[str]:
        """Get available NAT gateway IDs from VPC."""
        try:
            response = exponential_retry(
                self._ec2_client.describe_nat_gateways,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}, {"Name": "state", "Values": ["available"]}],
            )

            nat_gateway_ids = [nat["NatGatewayId"] for nat in response["NatGateways"]]
            return nat_gateway_ids

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ["InvalidNatGatewayID.NotFound", "NatGatewayNotFound", "InvalidFilter"]:
                module_logger.debug(f"No NAT gateways found in VPC {vpc_id}")
                return []
            module_logger.error(f"Failed to get NAT gateways for VPC {vpc_id}: {e}")
            raise

    def _delete_non_local_routes_from_table(self, route_table_id: str, routes: List[Dict]) -> None:
        """Delete all non-local routes from a route table before deletion.

        CRITICAL FIX: This removes stale routes to deleted NAT gateways, IGWs, etc.
        that would otherwise cause DependencyViolation or leave orphaned routes.

        Args:
            route_table_id: Route table ID to clean up
            routes: List of routes from the route table
        """
        for route in routes:
            destination = route.get("DestinationCidrBlock")

            # Skip local routes (AWS doesn't allow deletion of local routes)
            if route.get("GatewayId") == "local":
                continue

            # Delete any route with a destination (NAT gateway, IGW, etc.)
            if destination:
                try:
                    exponential_retry(
                        self._ec2_client.delete_route,
                        {"RequestLimitExceeded", "InvalidRoute.NotFound"},
                        RouteTableId=route_table_id,
                        DestinationCidrBlock=destination,
                    )
                    module_logger.info(f"Deleted route {destination} from route table {route_table_id}")
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code == "InvalidRoute.NotFound":
                        module_logger.debug(f"Route {destination} already deleted from {route_table_id}")
                    else:
                        module_logger.warning(f"Could not delete route {destination} from {route_table_id}: {e}")

    def _validate_private_subnet_routing(self, vpc_id: str, private_subnet_ids: List[str]) -> List[Dict[str, str]]:
        """Validate that all private subnets have proper NAT gateway routes.

        Returns list of issues found: [{"subnet_id": "...", "issue": "...", "route_table_id": "..."}]

        ENHANCED: Now also detects routes to non-existent NAT gateways (the bug!)
        """
        issues = []

        for subnet_id in private_subnet_ids:
            try:
                rt_response = exponential_retry(
                    self._ec2_client.describe_route_tables,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "association.subnet-id", "Values": [subnet_id]}, {"Name": "vpc-id", "Values": [vpc_id]}],
                )

                if not rt_response["RouteTables"]:
                    issues.append(
                        {"subnet_id": subnet_id, "issue": "No route table associated (using VPC main route table)", "route_table_id": None}
                    )
                    continue

                route_table = rt_response["RouteTables"][0]
                route_table_id = route_table["RouteTableId"]
                is_main = any(assoc.get("Main", False) for assoc in route_table.get("Associations", []))

                if is_main:
                    issues.append(
                        {
                            "subnet_id": subnet_id,
                            "issue": "Using VPC main route table (no custom route table)",
                            "route_table_id": route_table_id,
                        }
                    )
                    continue

                # Check for NAT gateway route
                nat_route = None
                for route in route_table.get("Routes", []):
                    if route.get("DestinationCidrBlock") == "0.0.0.0/0" and route.get("NatGatewayId", "").startswith("nat-"):
                        nat_route = route
                        break

                if not nat_route:
                    issues.append(
                        {"subnet_id": subnet_id, "issue": "Missing NAT gateway route (0.0.0.0/0)", "route_table_id": route_table_id}
                    )
                else:
                    # CRITICAL FIX: Verify the NAT gateway actually exists (detects the bug!)
                    nat_gw_id = nat_route["NatGatewayId"]
                    try:
                        nat_check = exponential_retry(
                            self._ec2_client.describe_nat_gateways,
                            {"RequestLimitExceeded", "ServiceUnavailable"},
                            NatGatewayIds=[nat_gw_id],
                        )
                        nat_state = nat_check["NatGateways"][0]["State"] if nat_check["NatGateways"] else "not-found"

                        if nat_state in ["deleted", "deleting", "failed"]:
                            issues.append(
                                {
                                    "subnet_id": subnet_id,
                                    "issue": f"Route references non-existent NAT gateway {nat_gw_id} (state: {nat_state})",
                                    "route_table_id": route_table_id,
                                    "stale_nat_id": nat_gw_id,
                                }
                            )
                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code", "")
                        if error_code in ["InvalidNatGatewayID.NotFound", "NatGatewayNotFound"]:
                            issues.append(
                                {
                                    "subnet_id": subnet_id,
                                    "issue": f"Route references non-existent NAT gateway {nat_gw_id}",
                                    "route_table_id": route_table_id,
                                    "stale_nat_id": nat_gw_id,
                                }
                            )

            except Exception as e:
                module_logger.warning(f"Could not validate routing for subnet {subnet_id}: {e}")
                issues.append({"subnet_id": subnet_id, "issue": f"Validation error: {str(e)}", "route_table_id": None})

        return issues

    def _sweep_and_fix_stale_nat_routes(self, vpc_id: str, unique_context_id: str) -> int:
        """Sweep all route tables in VPC and fix routes pointing to non-existent NAT gateways.

        RECOVERY LOGIC: This is the key fix for the bug - it detects and repairs
        route tables that have routes to deleted NAT gateways.

        CRITICAL: Replaces stale routes with valid NAT gateway routes to maintain connectivity,
        consistent with initial setup where private subnets have 0.0.0.0/0 → NAT routes.

        Returns: Number of stale routes fixed
        """
        module_logger.info(f"Sweeping VPC {vpc_id} for stale NAT gateway routes...")

        try:
            # First, get available NAT gateways in the VPC for replacement routes
            available_nat_gateways = self.get_vpc_nat_gateways(vpc_id)

            if not available_nat_gateways:
                module_logger.warning(
                    f"No available NAT gateways found in VPC {vpc_id}. "
                    f"Will delete stale routes but cannot replace them. "
                    f"Private subnets may lose internet connectivity."
                )

            # Get ALL route tables in the VPC (including orphaned ones)
            rt_response = exponential_retry(
                self._ec2_client.describe_route_tables,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
            )

            stale_routes_fixed = 0
            route_table_nat_index = 0  # For round-robin NAT assignment

            for route_table in rt_response["RouteTables"]:
                rt_id = route_table["RouteTableId"]
                is_main = any(assoc.get("Main", False) for assoc in route_table.get("Associations", []))

                # Check each route for stale NAT gateway references
                for route in route_table.get("Routes", []):
                    nat_gw_id = route.get("NatGatewayId")
                    destination = route.get("DestinationCidrBlock")

                    if not nat_gw_id or not destination:
                        continue

                    # Verify NAT gateway exists
                    try:
                        nat_check = exponential_retry(
                            self._ec2_client.describe_nat_gateways,
                            {"RequestLimitExceeded", "ServiceUnavailable"},
                            NatGatewayIds=[nat_gw_id],
                        )

                        if not nat_check["NatGateways"]:
                            # NAT doesn't exist - replace stale route with valid NAT gateway route
                            module_logger.warning(
                                f"Found stale route to non-existent NAT {nat_gw_id} in route table {rt_id} "
                                f"(main={is_main}). Replacing with valid NAT gateway..."
                            )

                            try:
                                # Delete stale route
                                exponential_retry(
                                    self._ec2_client.delete_route,
                                    {"RequestLimitExceeded", "InvalidRoute.NotFound"},
                                    RouteTableId=rt_id,
                                    DestinationCidrBlock=destination,
                                )

                                # CRITICAL: Replace with valid NAT gateway route (consistent with initial setup)
                                if available_nat_gateways:
                                    # Use round-robin to distribute across available NAT gateways
                                    replacement_nat_id = available_nat_gateways[route_table_nat_index % len(available_nat_gateways)]
                                    route_table_nat_index += 1

                                    exponential_retry(
                                        self._ec2_client.create_route,
                                        {"RequestLimitExceeded", "RouteAlreadyExists"},
                                        RouteTableId=rt_id,
                                        DestinationCidrBlock=destination,
                                        NatGatewayId=replacement_nat_id,
                                    )
                                    stale_routes_fixed += 1
                                    module_logger.info(
                                        f"✓ Replaced stale route {destination} → {nat_gw_id} with {destination} → {replacement_nat_id} in {rt_id}"
                                    )
                                else:
                                    # No NAT gateways available - just delete (subnet loses connectivity)
                                    stale_routes_fixed += 1
                                    module_logger.warning(
                                        f"✓ Deleted stale route {destination} → {nat_gw_id} from {rt_id} "
                                        f"(no NAT gateways available for replacement)"
                                    )
                            except ClientError as delete_error:
                                error_code = delete_error.response.get("Error", {}).get("Code", "")
                                if error_code == "InvalidRoute.NotFound":
                                    module_logger.debug(f"Route already deleted from {rt_id}")
                                    stale_routes_fixed += 1
                                elif error_code == "RouteAlreadyExists":
                                    module_logger.info(f"Valid route already exists in {rt_id}")
                                    stale_routes_fixed += 1
                                else:
                                    module_logger.error(f"Failed to replace stale route in {rt_id}: {delete_error}")
                        else:
                            nat_state = nat_check["NatGateways"][0]["State"]
                            if nat_state in ["deleted", "deleting", "failed"]:
                                module_logger.warning(
                                    f"Found route to deleted/failed NAT {nat_gw_id} (state: {nat_state}) "
                                    f"in route table {rt_id}. Replacing with valid NAT gateway..."
                                )

                                try:
                                    # Delete stale route
                                    exponential_retry(
                                        self._ec2_client.delete_route,
                                        {"RequestLimitExceeded", "InvalidRoute.NotFound"},
                                        RouteTableId=rt_id,
                                        DestinationCidrBlock=destination,
                                    )

                                    # CRITICAL: Replace with valid NAT gateway route
                                    if available_nat_gateways:
                                        replacement_nat_id = available_nat_gateways[route_table_nat_index % len(available_nat_gateways)]
                                        route_table_nat_index += 1

                                        exponential_retry(
                                            self._ec2_client.create_route,
                                            {"RequestLimitExceeded", "RouteAlreadyExists"},
                                            RouteTableId=rt_id,
                                            DestinationCidrBlock=destination,
                                            NatGatewayId=replacement_nat_id,
                                        )
                                        stale_routes_fixed += 1
                                        module_logger.info(
                                            f"✓ Replaced stale route {destination} → {nat_gw_id} with {destination} → {replacement_nat_id} in {rt_id}"
                                        )
                                    else:
                                        stale_routes_fixed += 1
                                        module_logger.warning(
                                            f"✓ Deleted stale route {destination} → {nat_gw_id} from {rt_id} (no NAT available)"
                                        )
                                except ClientError as delete_error:
                                    error_code = delete_error.response.get("Error", {}).get("Code", "")
                                    if error_code == "InvalidRoute.NotFound":
                                        module_logger.debug(f"Route already deleted from {rt_id}")
                                        stale_routes_fixed += 1
                                    elif error_code == "RouteAlreadyExists":
                                        module_logger.info(f"Valid route already exists in {rt_id}")
                                        stale_routes_fixed += 1
                                    else:
                                        module_logger.error(f"Failed to replace stale route in {rt_id}: {delete_error}")

                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code", "")
                        if error_code in ["InvalidNatGatewayID.NotFound", "NatGatewayNotFound"]:
                            # NAT doesn't exist - replace stale route with valid NAT gateway route
                            module_logger.warning(
                                f"Found stale route to non-existent NAT {nat_gw_id} in route table {rt_id}. "
                                f"Replacing with valid NAT gateway..."
                            )

                            try:
                                # Delete stale route
                                exponential_retry(
                                    self._ec2_client.delete_route,
                                    {"RequestLimitExceeded", "InvalidRoute.NotFound"},
                                    RouteTableId=rt_id,
                                    DestinationCidrBlock=destination,
                                )

                                # CRITICAL: Replace with valid NAT gateway route (consistent with initial setup)
                                if available_nat_gateways:
                                    replacement_nat_id = available_nat_gateways[route_table_nat_index % len(available_nat_gateways)]
                                    route_table_nat_index += 1

                                    exponential_retry(
                                        self._ec2_client.create_route,
                                        {"RequestLimitExceeded", "RouteAlreadyExists"},
                                        RouteTableId=rt_id,
                                        DestinationCidrBlock=destination,
                                        NatGatewayId=replacement_nat_id,
                                    )
                                    stale_routes_fixed += 1
                                    module_logger.info(
                                        f"✓ Replaced stale route {destination} → {nat_gw_id} with {destination} → {replacement_nat_id} in {rt_id}"
                                    )
                                else:
                                    stale_routes_fixed += 1
                                    module_logger.warning(
                                        f"✓ Deleted stale route {destination} → {nat_gw_id} from {rt_id} (no NAT available)"
                                    )
                            except ClientError as delete_error:
                                error_code = delete_error.response.get("Error", {}).get("Code", "")
                                if error_code == "InvalidRoute.NotFound":
                                    module_logger.debug(f"Route already deleted from {rt_id}")
                                    stale_routes_fixed += 1
                                elif error_code == "RouteAlreadyExists":
                                    module_logger.info(f"Valid route already exists in {rt_id}")
                                    stale_routes_fixed += 1
                                else:
                                    module_logger.error(f"Failed to replace stale route in {rt_id}: {delete_error}")
                        else:
                            module_logger.warning(f"Error checking NAT gateway {nat_gw_id}: {e}")

            if stale_routes_fixed > 0:
                module_logger.info(f"✓ Fixed {stale_routes_fixed} stale NAT gateway routes in VPC {vpc_id}")
            else:
                module_logger.info(f"✓ No stale NAT gateway routes found in VPC {vpc_id}")

            return stale_routes_fixed

        except Exception as e:
            module_logger.error(f"Failed to sweep and fix stale NAT routes in VPC {vpc_id}: {e}")
            return 0

    def _fix_private_subnet_routing(
        self, vpc_id: str, private_subnet_id: str, nat_gateway_id: str, unique_context_id: str, index: int
    ) -> str:
        """Fix broken private subnet routing by creating proper route table with NAT gateway route.

        This automatically fixes subnets using main route table or missing NAT routes.
        Returns the new route table ID for S3 endpoint association.

        ENHANCED: Now also handles routes to non-existent NAT gateways.
        """
        try:
            # Check current route table association
            rt_response = exponential_retry(
                self._ec2_client.describe_route_tables,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "association.subnet-id", "Values": [private_subnet_id]}, {"Name": "vpc-id", "Values": [vpc_id]}],
            )

            # Disassociate from current route table if needed
            if rt_response["RouteTables"]:
                route_table = rt_response["RouteTables"][0]
                is_main = any(assoc.get("Main", False) for assoc in route_table.get("Associations", []))

                if is_main:
                    # Using main RT - need to create custom RT and associate
                    pass  # Will create new RT below
                else:
                    # Has custom RT but wrong/missing routes - disassociate
                    for assoc in route_table.get("Associations", []):
                        if assoc.get("SubnetId") == private_subnet_id:
                            assoc_id = assoc.get("RouteTableAssociationId")
                            if assoc_id:
                                exponential_retry(
                                    self._ec2_client.disassociate_route_table,
                                    {"RequestLimitExceeded"},
                                    AssociationId=assoc_id,
                                )
                                module_logger.info(f"Disassociated broken route table from subnet {private_subnet_id}")

            # Create new route table with proper NAT gateway route
            new_rt_response = exponential_retry(
                self._ec2_client.create_route_table,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                VpcId=vpc_id,
            )
            new_rt_id = new_rt_response["RouteTable"]["RouteTableId"]

            # Tag the route table
            exponential_retry(
                self._ec2_client.create_tags,
                {"RequestLimitExceeded", "InvalidRouteTableID.NotFound"},
                Resources=[new_rt_id],
                Tags=[{"Key": "Name", "Value": f"IntelliFlow-{unique_context_id}-Private-RT-Fixed-{index}"}],
            )

            # Add NAT gateway route
            exponential_retry(
                self._ec2_client.create_route,
                {"RequestLimitExceeded"},
                RouteTableId=new_rt_id,
                DestinationCidrBlock="0.0.0.0/0",
                NatGatewayId=nat_gateway_id,
            )

            # Associate with subnet
            exponential_retry(
                self._ec2_client.associate_route_table,
                {"RequestLimitExceeded"},
                SubnetId=private_subnet_id,
                RouteTableId=new_rt_id,
            )

            module_logger.info(f"Fixed routing for subnet {private_subnet_id} with NAT gateway {nat_gateway_id}")

            return new_rt_id

        except Exception as e:
            module_logger.error(f"Failed to fix routing for subnet {private_subnet_id}: {e}")
            raise

    def _disconnect_and_cleanup_subnet_dependencies(self, vpc_id: str, subnets_to_delete: List[str]) -> None:
        """Disconnect and cleanup all dependencies for subnets to be deleted.

        This follows the correct idempotent sequence consistent with initial setup:

        INITIAL SETUP ORDER: subnets → route tables → network resources (NAT, IGW, etc.)
        CLEANUP ORDER (reverse): network resources → route tables → subnets

        Detailed cleanup sequence:
        1. Disassociate ALL subnets from route tables
        2. SWEEP all route tables to find orphans (no subnet associations)
        3. CLEAN routes in orphaned tables (delete routes to NAT/IGW) - CRITICAL FIX!
        4. Disconnect orphaned route tables from VPC endpoints
        5. SWEEP to find orphaned NAT gateways (in deleted subnets, not used elsewhere)
        6. Delete in order: NAT gateways → EIPs → route tables → subnets

        IDEMPOTENT: Uses sweep approach to detect orphans, not session-based tracking.
        This handles previous failed cleanup attempts correctly.

        Args:
            vpc_id: VPC ID containing the subnets
            subnets_to_delete: List of subnet IDs to delete

        Raises:
            ClientError: If critical AWS API operations fail
        """
        if not subnets_to_delete:
            return

        # STEP 1: Disassociate ALL subnets from their route tables
        # (Following reverse of initial setup where subnets are associated with route tables)
        module_logger.info(f"Step 1: Disassociating {len(subnets_to_delete)} subnets from route tables")
        for subnet_id in subnets_to_delete:
            rt_response = exponential_retry(
                self._ec2_client.describe_route_tables,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "association.subnet-id", "Values": [subnet_id]}, {"Name": "vpc-id", "Values": [vpc_id]}],
            )

            for route_table in rt_response["RouteTables"]:
                rt_id = route_table["RouteTableId"]

                # Skip main route table
                is_main = any(assoc.get("Main", False) for assoc in route_table.get("Associations", []))
                if is_main:
                    continue

                # Disassociate this subnet
                for assoc in route_table.get("Associations", []):
                    if assoc.get("SubnetId") == subnet_id and not assoc.get("Main", False):
                        assoc_id = assoc.get("RouteTableAssociationId")
                        if assoc_id:
                            try:
                                exponential_retry(
                                    self._ec2_client.disassociate_route_table,
                                    {"RequestLimitExceeded"},
                                    AssociationId=assoc_id,
                                )
                                module_logger.info(f"Disassociated subnet {subnet_id} from route table {rt_id}")
                            except ClientError as e:
                                error_code = e.response.get("Error", {}).get("Code", "")
                                if error_code == "InvalidAssociationID.NotFound":
                                    module_logger.debug(f"Association already removed for subnet {subnet_id}")
                                else:
                                    raise

        # Small delay to allow AWS to process disassociations
        time.sleep(2)

        # STEP 2: SWEEP all route tables in VPC to identify orphans (IDEMPOTENT)
        # This finds orphans from current AND previous failed cleanup attempts
        module_logger.info(f"Step 2: Sweeping ALL route tables in VPC to identify orphans (idempotent)")

        all_rt_response = exponential_retry(
            self._ec2_client.describe_route_tables,
            {"RequestLimitExceeded", "ServiceUnavailable"},
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
        )

        orphaned_route_tables = []
        nat_gateway_ids_in_orphaned_rts = set()

        for route_table in all_rt_response["RouteTables"]:
            rt_id = route_table["RouteTableId"]

            # Skip main route table
            is_main = any(assoc.get("Main", False) for assoc in route_table.get("Associations", []))
            if is_main:
                continue

            # Check if this route table has any subnet associations (excluding main)
            has_subnet_associations = any(
                assoc.get("SubnetId") for assoc in route_table.get("Associations", []) if not assoc.get("Main", False)
            )

            if not has_subnet_associations:
                orphaned_route_tables.append(route_table)
                module_logger.info(f"Route table {rt_id} is orphaned (no subnet associations)")

                # Collect NAT gateway IDs from this orphaned route table
                for route in route_table.get("Routes", []):
                    if route.get("NatGatewayId"):
                        nat_gateway_ids_in_orphaned_rts.add(route["NatGatewayId"])

        # STEP 3: CLEAN routes from orphaned route tables (CRITICAL FIX for the bug!)
        # This removes stale routes to NAT gateways/IGWs before attempting deletion
        # Follows reverse of initial setup where routes are created in route tables
        if orphaned_route_tables:
            module_logger.info(f"Step 3: Cleaning routes from {len(orphaned_route_tables)} orphaned route tables")
            for route_table in orphaned_route_tables:
                rt_id = route_table["RouteTableId"]
                routes = route_table.get("Routes", [])

                # Delete all non-local routes (NAT gateway, IGW routes, etc.)
                self._delete_non_local_routes_from_table(rt_id, routes)

            module_logger.info(f"✓ Cleaned routes from {len(orphaned_route_tables)} orphaned route tables")

        # STEP 4: Disconnect orphaned route tables from VPC endpoints
        # (Follows reverse of initial setup where route tables are added to endpoints)
        if orphaned_route_tables:
            module_logger.info(f"Step 4: Disconnecting {len(orphaned_route_tables)} orphaned route tables from VPC endpoints")
            orphaned_rt_ids = [rt["RouteTableId"] for rt in orphaned_route_tables]

            endpoint_response = exponential_retry(
                self._ec2_client.describe_vpc_endpoints,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
            )

            for endpoint in endpoint_response["VpcEndpoints"]:
                endpoint_id = endpoint["VpcEndpointId"]

                # For gateway endpoints (like S3), remove orphaned route tables
                if endpoint.get("VpcEndpointType") == "Gateway" and endpoint.get("RouteTableIds"):
                    rts_to_remove = [rt_id for rt_id in endpoint["RouteTableIds"] if rt_id in orphaned_rt_ids]

                    if rts_to_remove:
                        try:
                            exponential_retry(
                                self._ec2_client.modify_vpc_endpoint,
                                {"RequestLimitExceeded"},
                                VpcEndpointId=endpoint_id,
                                RemoveRouteTableIds=rts_to_remove,
                            )
                            module_logger.info(f"Removed {len(rts_to_remove)} orphaned route tables from VPC endpoint {endpoint_id}")
                        except ClientError as e:
                            error_code = e.response.get("Error", {}).get("Code", "")
                            if error_code in ["InvalidVpcEndpointId.NotFound", "InvalidParameter"]:
                                module_logger.debug(f"VPC endpoint already updated or doesn't exist")
                            else:
                                raise

        # STEP 5: SWEEP to identify orphaned NAT gateways (IDEMPOTENT)
        # NAT is orphaned if: in subnet to delete AND only referenced by orphaned route tables
        # (Follows reverse of initial setup where NATs are created in subnets)
        module_logger.info(f"Step 5: Sweeping NAT gateways to identify orphans (idempotent)")
        orphaned_nat_gateways = []
        orphaned_rt_ids = [rt["RouteTableId"] for rt in orphaned_route_tables]

        # Get ALL NAT gateways in subnets we're deleting
        for subnet_id in subnets_to_delete:
            try:
                nat_response = exponential_retry(
                    self._ec2_client.describe_nat_gateways,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[
                        {"Name": "subnet-id", "Values": [subnet_id]},
                        {"Name": "state", "Values": ["available", "pending"]},
                    ],
                )

                for nat in nat_response["NatGateways"]:
                    nat_id = nat["NatGatewayId"]

                    # NAT is orphaned if it's ONLY referenced by orphaned route tables
                    # Check all route tables in VPC to see if any non-orphaned RT uses this NAT
                    used_by_active_rt = False

                    for rt in all_rt_response["RouteTables"]:
                        # Skip orphaned route tables
                        if rt["RouteTableId"] in orphaned_rt_ids:
                            continue

                        # Check if this active RT uses this NAT gateway
                        for route in rt.get("Routes", []):
                            if route.get("NatGatewayId") == nat_id:
                                used_by_active_rt = True
                                module_logger.info(f"NAT gateway {nat_id} is still used by active route table {rt['RouteTableId']}")
                                break

                        if used_by_active_rt:
                            break

                    if not used_by_active_rt:
                        orphaned_nat_gateways.append(nat)
                        module_logger.info(f"NAT gateway {nat_id} in subnet {subnet_id} is orphaned")

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code in ["InvalidNatGatewayID.NotFound", "NatGatewayNotFound", "InvalidFilter"]:
                    module_logger.debug(f"No NAT gateways in subnet {subnet_id}")
                else:
                    raise

        # STEP 6: Delete orphaned resources in correct order (reverse of creation)
        # Creation order was: subnets → route tables → NAT gateways
        # Deletion order is: NAT gateways → route tables → subnets

        # 6a. Delete orphaned NAT gateways first (reverse of creation)
        if orphaned_nat_gateways:
            module_logger.info(f"Step 6a: Deleting {len(orphaned_nat_gateways)} orphaned NAT gateways")
            elastic_ips_to_release = []

            for nat in orphaned_nat_gateways:
                nat_id = nat["NatGatewayId"]
                try:
                    exponential_retry(
                        self._ec2_client.delete_nat_gateway,
                        {"RequestLimitExceeded"},
                        NatGatewayId=nat_id,
                    )
                    module_logger.info(f"Deleted orphaned NAT gateway {nat_id}")

                    # Collect Elastic IPs for later release
                    for addr in nat.get("NatGatewayAddresses", []):
                        if addr.get("AllocationId"):
                            elastic_ips_to_release.append(addr["AllocationId"])

                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code == "InvalidNatGatewayID.NotFound":
                        module_logger.debug(f"NAT gateway {nat_id} already deleted")
                    else:
                        raise

            # Wait for NAT gateways to delete before releasing EIPs
            if elastic_ips_to_release:
                module_logger.info(f"Waiting for NAT gateways to delete before releasing {len(elastic_ips_to_release)} Elastic IPs...")
                time.sleep(10)

                for allocation_id in elastic_ips_to_release:
                    try:
                        exponential_retry(
                            self._ec2_client.release_address,
                            {"RequestLimitExceeded"},
                            AllocationId=allocation_id,
                        )
                        module_logger.info(f"Released Elastic IP {allocation_id}")
                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code", "")
                        if error_code in ["InvalidAllocationID.NotFound", "AuthFailure"]:
                            module_logger.debug(f"Elastic IP {allocation_id} already released")
                        else:
                            raise

        # 6b. Delete orphaned route tables (reverse of creation)
        # Now safe to delete since routes are cleaned and NATs are deleted
        if orphaned_route_tables:
            module_logger.info(f"Step 6b: Deleting {len(orphaned_route_tables)} orphaned route tables")
            for route_table in orphaned_route_tables:
                rt_id = route_table["RouteTableId"]
                try:
                    # Routes already cleaned in Step 3, just delete the route table
                    exponential_retry(
                        self._ec2_client.delete_route_table,
                        {"RequestLimitExceeded", "DependencyViolation"},
                        RouteTableId=rt_id,
                    )
                    module_logger.info(f"Deleted orphaned route table {rt_id}")
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["InvalidRouteTableID.NotFound"]:
                        module_logger.debug(f"Route table {rt_id} already deleted")
                    elif error_code == "DependencyViolation":
                        module_logger.warning(f"Route table {rt_id} still has dependencies, skipping")
                    else:
                        raise

        module_logger.info("Step 6c: Ready to delete subnets (will be done by caller)")
        module_logger.info(
            "✓ Cleanup sequence completed following reverse order of initial setup: " "NAT gateways → route tables → subnets"
        )

    def _cleanup_duplicate_subnets(self, vpc_id: str, unique_context_id: str, keep_subnet_ids: List[str]) -> None:
        """Delete duplicate IntelliFlow subnets that are not in the keep list.

        This cleans up residual subnets from before idempotency features were added.
        Uses proper sequence: disconnect all → identify orphans → delete orphaned resources → delete subnets.

        Raises:
            ClientError: If critical AWS API operations fail (non-recoverable errors)
        """
        # Get all IntelliFlow-managed subnets in this VPC
        all_if_subnets_response = exponential_retry(
            self._ec2_client.describe_subnets,
            {"RequestLimitExceeded", "ServiceUnavailable"},
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "tag:Name", "Values": [f"IntelliFlow-{unique_context_id}-*"]},
            ],
        )

        # Find duplicates to delete
        subnets_to_delete = [
            subnet["SubnetId"] for subnet in all_if_subnets_response["Subnets"] if subnet["SubnetId"] not in keep_subnet_ids
        ]

        if not subnets_to_delete:
            return

        module_logger.info(f"Cleaning up {len(subnets_to_delete)} duplicate/unused subnets: {subnets_to_delete}")
        # CRITICAL FIX 1: Ensure kept public subnets have IGW routes BEFORE any cleanup
        # This must happen BEFORE NAT migration to ensure new NATs can function properly
        try:
            # Find kept public subnets
            kept_public_subnets = []
            for subnet_id in keep_subnet_ids:
                try:
                    subnet_response = exponential_retry(
                        self._ec2_client.describe_subnets,
                        {"RequestLimitExceeded", "ServiceUnavailable"},
                        SubnetIds=[subnet_id],
                    )

                    for subnet in subnet_response["Subnets"]:
                        tags = {tag["Key"]: tag["Value"] for tag in subnet.get("Tags", [])}
                        name = tags.get("Name", "")
                        if "Public" in name:
                            kept_public_subnets.append(subnet_id)
                except Exception as e:
                    module_logger.warning(f"Could not check if subnet {subnet_id} is public: {e}")

            # Get IGW for the VPC
            if kept_public_subnets:
                igw_response = exponential_retry(
                    self._ec2_client.describe_internet_gateways,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}],
                )
                igw_id = igw_response["InternetGateways"][0]["InternetGatewayId"] if igw_response["InternetGateways"] else None

                if igw_id:
                    module_logger.info(
                        f"Ensuring {len(kept_public_subnets)} kept public subnets have IGW routes "
                        f"before subnet cleanup (prevents NAT connectivity issues)"
                    )
                    self._ensure_public_subnets_have_internet_gateway_routes(vpc_id, igw_id, kept_public_subnets, unique_context_id)
                else:
                    module_logger.warning("No Internet Gateway found for VPC - kept public subnets may have connectivity issues")
        except Exception as igw_error:
            module_logger.warning(f"Could not ensure IGW routes for kept public subnets: {igw_error}")

        # CRITICAL FIX 2: Migrate NAT gateways from public subnets being deleted
        # This prevents DependencyViolation and ensures NATs are in kept subnets with proper IGW routes
        self._migrate_nat_gateways_from_deleted_subnets(vpc_id, unique_context_id, subnets_to_delete, keep_subnet_ids)

        # CRITICAL FIX: Use proper sequence - disconnect all subnets, identify orphans, delete orphans, then delete subnets
        try:
            self._disconnect_and_cleanup_subnet_dependencies(vpc_id, subnets_to_delete)
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            module_logger.error(f"Failed to disconnect and cleanup dependencies for subnets: {e}")
            raise

        # Small delay to allow AWS to process all the disconnections and deletions
        module_logger.info("Waiting for dependency cleanup to complete...")
        time.sleep(5)

        # Now delete the subnets (all dependencies should be cleaned up)
        module_logger.info(f"Deleting {len(subnets_to_delete)} subnets after dependency cleanup")
        for subnet_id in subnets_to_delete:
            try:
                exponential_retry(
                    self._ec2_client.delete_subnet,
                    {"RequestLimitExceeded"},
                    SubnetId=subnet_id,
                )
                module_logger.info(f"Successfully deleted duplicate subnet {subnet_id}")
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "DependencyViolation":
                    # This should be very rare now after comprehensive dependency cleanup
                    # Log as warning but don't raise - will retry in next activation
                    module_logger.warning(
                        f"Subnet {subnet_id} still has dependencies after comprehensive cleanup. "
                        f"This likely indicates an EMR cluster or other external resource is actively using it. "
                        f"Will retry in next activation. Error: {e}"
                    )
                elif error_code in ["InvalidSubnetID.NotFound", "InvalidSubnet.NotFound"]:
                    module_logger.debug(f"Subnet {subnet_id} already deleted")
                else:
                    # Unexpected error - re-raise it
                    module_logger.error(f"Unexpected error deleting subnet {subnet_id}: {e}")
                    raise

    def _migrate_nat_gateways_from_deleted_subnets(
        self,
        vpc_id: str,
        unique_context_id: str,
        subnets_to_delete: List[str],
        keep_subnet_ids: List[str],
    ) -> None:
        """
        Migrate NAT gateways from public subnets being deleted to kept public subnets.

        CRITICAL: This prevents DependencyViolation when trying to delete public subnets that have NAT gateways.
        Without this, NAT gateways remain in deleted subnets without IGW routes, causing connectivity failures.

        Process:
        1. Find NAT gateways in subnets being deleted
        2. Identify kept public subnets that can host NATs
        3. Create new NAT gateways in kept public subnets (with proper IGW routes)
        4. Update private subnet route tables to use new NAT IDs
        5. Delete old NAT gateways

        Args:
            vpc_id: VPC ID
            unique_context_id: Context ID for naming
            subnets_to_delete: List of subnet IDs being deleted
            keep_subnet_ids: List of subnet IDs being kept
        """
        try:
            # Step 1: Find NAT gateways in subnets being deleted
            nats_to_migrate = []
            for subnet_id in subnets_to_delete:
                try:
                    nat_response = exponential_retry(
                        self._ec2_client.describe_nat_gateways,
                        {"RequestLimitExceeded", "ServiceUnavailable"},
                        Filters=[
                            {"Name": "subnet-id", "Values": [subnet_id]},
                            {"Name": "state", "Values": ["available", "pending"]},
                        ],
                    )

                    for nat in nat_response["NatGateways"]:
                        nats_to_migrate.append(
                            {
                                "nat_id": nat["NatGatewayId"],
                                "old_subnet_id": subnet_id,
                                "allocation_ids": [
                                    addr["AllocationId"] for addr in nat.get("NatGatewayAddresses", []) if addr.get("AllocationId")
                                ],
                            }
                        )

                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code not in ["InvalidNatGatewayID.NotFound", "NatGatewayNotFound", "InvalidFilter"]:
                        module_logger.warning(f"Could not check NAT gateways in subnet {subnet_id}: {e}")

            if not nats_to_migrate:
                module_logger.info("No NAT gateways to migrate from deleted subnets")
                return

            module_logger.warning(
                f"CRITICAL: Found {len(nats_to_migrate)} NAT gateways in subnets being deleted. "
                f"Migrating to kept public subnets to prevent connectivity issues..."
            )

            # Step 2: Find kept public subnets that can host NATs
            kept_public_subnets = []
            for subnet_id in keep_subnet_ids:
                try:
                    subnet_response = exponential_retry(
                        self._ec2_client.describe_subnets,
                        {"RequestLimitExceeded", "ServiceUnavailable"},
                        SubnetIds=[subnet_id],
                    )

                    for subnet in subnet_response["Subnets"]:
                        tags = {tag["Key"]: tag["Value"] for tag in subnet.get("Tags", [])}
                        name = tags.get("Name", "")
                        if "Public" in name:
                            kept_public_subnets.append(subnet_id)

                except Exception as e:
                    module_logger.warning(f"Could not check if subnet {subnet_id} is public: {e}")

            if not kept_public_subnets:
                module_logger.error(
                    f"Cannot migrate {len(nats_to_migrate)} NAT gateways - no kept public subnets available! "
                    f"This will cause DependencyViolation when trying to delete their subnets."
                )
                return

            module_logger.info(f"Will migrate NAT gateways to {len(kept_public_subnets)} kept public subnets")

            # Step 3: Get IGW for ensuring kept public subnets have proper routes
            igw_response = exponential_retry(
                self._ec2_client.describe_internet_gateways,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}],
            )
            igw_id = igw_response["InternetGateways"][0]["InternetGatewayId"] if igw_response["InternetGateways"] else None

            if not igw_id:
                module_logger.error("Cannot migrate NAT gateways - no Internet Gateway found!")
                return

            # Ensure kept public subnets have IGW routes BEFORE creating new NATs
            module_logger.info("Ensuring kept public subnets have IGW routes before NAT migration...")
            self._ensure_public_subnets_have_internet_gateway_routes(vpc_id, igw_id, kept_public_subnets, unique_context_id)

            # Step 4: Create new NAT gateways in kept public subnets
            new_nat_mappings = {}  # old_nat_id -> new_nat_id

            for i, nat_info in enumerate(nats_to_migrate):
                old_nat_id = nat_info["nat_id"]
                target_public_subnet = kept_public_subnets[i % len(kept_public_subnets)]

                module_logger.info(
                    f"Migrating NAT gateway {old_nat_id} from {nat_info['old_subnet_id']} " f"to kept public subnet {target_public_subnet}"
                )

                try:
                    # Create new NAT gateway
                    new_nats = self.create_nat_gateways([target_public_subnet], unique_context_id)
                    if new_nats:
                        new_nat_id = new_nats[0]
                        new_nat_mappings[old_nat_id] = new_nat_id
                        module_logger.info(f"Created new NAT gateway {new_nat_id} to replace {old_nat_id}")

                except Exception as create_error:
                    module_logger.error(f"Failed to create replacement NAT gateway for {old_nat_id}: {create_error}")

            if not new_nat_mappings:
                module_logger.error("Failed to create any replacement NAT gateways - cannot migrate")
                return

            # Wait for new NAT gateways to be available
            module_logger.info("Waiting for new NAT gateways to become available...")
            self.wait_for_nat_gateways_available(list(new_nat_mappings.values()), timeout_minutes=5)

            # Step 5: Update private subnet route tables to use new NAT IDs
            module_logger.info("Updating private subnet route tables to use new NAT gateways...")
            rt_response = exponential_retry(
                self._ec2_client.describe_route_tables,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
            )

            routes_updated = 0
            for route_table in rt_response["RouteTables"]:
                rt_id = route_table["RouteTableId"]

                # Check if this route table uses an old NAT gateway
                for route in route_table.get("Routes", []):
                    old_nat_id = route.get("NatGatewayId")
                    if old_nat_id in new_nat_mappings:
                        new_nat_id = new_nat_mappings[old_nat_id]

                        try:
                            # Delete old route
                            exponential_retry(
                                self._ec2_client.delete_route,
                                {"RequestLimitExceeded"},
                                RouteTableId=rt_id,
                                DestinationCidrBlock="0.0.0.0/0",
                            )

                            # Add new route with new NAT
                            exponential_retry(
                                self._ec2_client.create_route,
                                {"RequestLimitExceeded", "RouteAlreadyExists"},
                                RouteTableId=rt_id,
                                DestinationCidrBlock="0.0.0.0/0",
                                NatGatewayId=new_nat_id,
                            )

                            routes_updated += 1
                            module_logger.info(f"Updated route table {rt_id}: {old_nat_id} → {new_nat_id}")

                        except Exception as update_error:
                            module_logger.error(f"Failed to update route in {rt_id}: {update_error}")

            module_logger.info(f"Updated {routes_updated} route tables to use new NAT gateways")

            # Step 6: Delete old NAT gateways (now safe since routes updated)
            module_logger.info("Deleting old NAT gateways from subnets being deleted...")
            for old_nat_id in new_nat_mappings.keys():
                try:
                    exponential_retry(
                        self._ec2_client.delete_nat_gateway,
                        {"RequestLimitExceeded"},
                        NatGatewayId=old_nat_id,
                    )
                    module_logger.info(f"Deleted old NAT gateway {old_nat_id}")
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["InvalidNatGatewayID.NotFound", "NatGatewayNotFound"]:
                        module_logger.info(f"NAT gateway {old_nat_id} already deleted")
                    else:
                        module_logger.warning(f"Could not delete old NAT gateway {old_nat_id}: {e}")

            # Wait for old NATs to delete before releasing EIPs
            time.sleep(10)

            # Release old Elastic IPs
            for nat_info in nats_to_migrate:
                for allocation_id in nat_info.get("allocation_ids", []):
                    try:
                        exponential_retry(
                            self._ec2_client.release_address,
                            {"RequestLimitExceeded"},
                            AllocationId=allocation_id,
                        )
                        module_logger.info(f"Released Elastic IP {allocation_id}")
                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code", "")
                        if error_code in ["InvalidAllocationID.NotFound", "AuthFailure"]:
                            module_logger.info(f"Elastic IP {allocation_id} already released")
                        else:
                            module_logger.warning(f"Could not release Elastic IP {allocation_id}: {e}")

            module_logger.info(
                f"✓ Successfully migrated {len(new_nat_mappings)} NAT gateways to kept public subnets. "
                f"Old subnets can now be safely deleted."
            )

        except Exception as e:
            module_logger.error(f"NAT gateway migration failed: {e}")
            module_logger.warning(
                "NAT gateways were not migrated. Subnet deletion may fail with DependencyViolation. " "Manual intervention may be required."
            )

    def _cleanup_orphaned_route_tables(self, vpc_id: str, unique_context_id: str, keep_subnet_ids: List[str]) -> None:
        """Delete orphaned IntelliFlow route tables not associated with kept subnets.

        Cleans up route tables created during previous failed activations or from obsolete subnets.
        Follows proper sequence: disassociate → clean routes → delete route tables.
        """
        try:
            # Get all IntelliFlow-managed route tables (excludes main RT)
            rt_response = exponential_retry(
                self._ec2_client.describe_route_tables,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "tag:Name", "Values": [f"IntelliFlow-{unique_context_id}-Private-RT*"]},
                ],
            )

            route_tables_to_check = []
            for rt in rt_response["RouteTables"]:
                # Skip main route table
                is_main = any(assoc.get("Main", False) for assoc in rt.get("Associations", []))
                if not is_main:
                    route_tables_to_check.append(rt)

            if not route_tables_to_check:
                return

            # Identify orphaned route tables (not associated with kept subnets)
            orphaned_rts = []
            for rt in route_tables_to_check:
                rt_id = rt["RouteTableId"]
                associations = rt.get("Associations", [])

                # Check if associated with any of our kept subnets
                associated_with_kept_subnet = any(
                    assoc.get("SubnetId") in keep_subnet_ids for assoc in associations if assoc.get("SubnetId")
                )

                if not associated_with_kept_subnet:
                    orphaned_rts.append(rt)

            if not orphaned_rts:
                module_logger.info("No orphaned route tables to clean up")
                return

            module_logger.info(f"Cleaning up {len(orphaned_rts)} orphaned route tables")

            for rt in orphaned_rts:
                rt_id = rt["RouteTableId"]
                try:
                    # First: Disassociate if needed
                    for assoc in rt.get("Associations", []):
                        if assoc.get("SubnetId") and not assoc.get("Main", False):
                            assoc_id = assoc.get("RouteTableAssociationId")
                            if assoc_id:
                                try:
                                    exponential_retry(
                                        self._ec2_client.disassociate_route_table,
                                        {"RequestLimitExceeded"},
                                        AssociationId=assoc_id,
                                    )
                                except ClientError as e:
                                    if e.response.get("Error", {}).get("Code") not in ["InvalidAssociationID.NotFound"]:
                                        module_logger.warning(f"Could not disassociate RT {rt_id}: {e}")

                    # Second: CLEAN all non-local routes (CRITICAL FIX!)
                    # This removes stale routes to deleted NAT gateways, preventing DependencyViolation
                    self._delete_non_local_routes_from_table(rt_id, rt.get("Routes", []))

                    # Third: Delete the route table (now safe since routes are cleaned)
                    exponential_retry(
                        self._ec2_client.delete_route_table,
                        {"RequestLimitExceeded", "DependencyViolation"},
                        RouteTableId=rt_id,
                    )
                    module_logger.info(f"Deleted orphaned route table {rt_id}")

                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code == "DependencyViolation":
                        module_logger.warning(f"Deferring route table {rt_id} deletion - has dependencies")
                    elif error_code in ["InvalidRouteTableID.NotFound"]:
                        module_logger.debug(f"Route table {rt_id} already deleted")
                    else:
                        module_logger.warning(f"Failed to delete route table {rt_id}: {e}")

        except Exception as e:
            module_logger.warning(f"Could not cleanup orphaned route tables: {e}")

    def _cleanup_orphaned_nat_gateways(self, vpc_id: str, unique_context_id: str, keep_nat_gateway_ids: List[str]) -> None:
        """Delete orphaned IntelliFlow NAT gateways not in the keep list.

        Cleans up NAT gateways from previous configurations no longer in use.
        """
        try:
            # Get all NAT gateways in VPC
            nat_response = exponential_retry(
                self._ec2_client.describe_nat_gateways,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "state", "Values": ["available", "pending"]},
                ],
            )

            # Filter to IntelliFlow-managed NAT gateways
            orphaned_nats = []
            for nat in nat_response["NatGateways"]:
                nat_id = nat["NatGatewayId"]
                tags = {tag["Key"]: tag["Value"] for tag in nat.get("Tags", [])}

                # Check if it's IntelliFlow-managed and not in keep list
                if tags.get("IntelliFlow") == unique_context_id and nat_id not in keep_nat_gateway_ids:
                    orphaned_nats.append(nat)

            if not orphaned_nats:
                module_logger.info("No orphaned NAT gateways to clean up")
                return

            module_logger.info(f"Cleaning up {len(orphaned_nats)} orphaned NAT gateways")

            for nat in orphaned_nats:
                nat_id = nat["NatGatewayId"]
                try:
                    exponential_retry(
                        self._ec2_client.delete_nat_gateway,
                        {"RequestLimitExceeded"},
                        NatGatewayId=nat_id,
                    )
                    module_logger.info(f"Deleted orphaned NAT gateway {nat_id}")

                    # Also track Elastic IP for later release
                    for addr in nat.get("NatGatewayAddresses", []):
                        if addr.get("AllocationId"):
                            allocation_id = addr["AllocationId"]
                            module_logger.info(f"Will release Elastic IP {allocation_id} after NAT gateway deletion")

                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["InvalidNatGatewayID.NotFound", "NatGatewayNotFound"]:
                        module_logger.debug(f"NAT gateway {nat_id} already deleted")
                    else:
                        module_logger.warning(f"Failed to delete NAT gateway {nat_id}: {e}")

        except Exception as e:
            module_logger.warning(f"Could not cleanup orphaned NAT gateways: {e}")

    def _get_intelliflow_managed_subnets_per_az(self, vpc_id: str, unique_context_id: str, subnet_type: str) -> Dict[str, str]:
        """Get one IntelliFlow-managed subnet per AZ for given type (Public/Private).

        Returns: {az: subnet_id} mapping with at most one subnet per AZ.
        If multiple IF-managed subnets exist in same AZ, prefer the first one found.
        """
        try:
            # Get all IntelliFlow-managed subnets of this type
            response = exponential_retry(
                self._ec2_client.describe_subnets,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "tag:Name", "Values": [f"IntelliFlow-{unique_context_id}-{subnet_type}-*"]},
                ],
            )

            az_to_subnet = {}
            for subnet in response["Subnets"]:
                az = subnet["AvailabilityZone"]
                subnet_id = subnet["SubnetId"]

                # Only keep first subnet per AZ for idempotency
                if az not in az_to_subnet:
                    az_to_subnet[az] = subnet_id
                else:
                    # Log duplicate for visibility
                    module_logger.warning(
                        f"Found duplicate IntelliFlow {subnet_type} subnet in {az}: {subnet_id} "
                        f"(keeping {az_to_subnet[az]}). This indicates previous activation created duplicates."
                    )

            return az_to_subnet

        except ClientError as e:
            module_logger.error(f"Failed to get IntelliFlow-managed {subnet_type} subnets: {e}")
            return {}

    def provision_complete_vpc_infrastructure(self, unique_context_id: str, subnet_count: int = 3) -> Dict[str, any]:
        """Provision complete VPC infrastructure with subnets and NAT gateways using create-or-update semantics.

        IDEMPOTENCY: This method can be called multiple times and will:
        - Reuse existing IntelliFlow-managed infrastructure
        - Only filter and use IntelliFlow-managed subnets (ignore customer-created subnets in same VPC)
        - Validate and fix broken routing configurations
        - Ensure minimum 3 AZs for Redshift Serverless compatibility
        """
        module_logger.info(
            f"Provisioning complete VPC infrastructure with {subnet_count} subnet pairs (minimum {subnet_count} AZs required)"
        )

        # Check if VPC already exists and can be reused/extended
        existing_vpc_id = self.vpc_exists(unique_context_id)
        if existing_vpc_id:
            module_logger.info(f"Found existing VPC {existing_vpc_id}, checking configuration compatibility")

            # CRITICAL: Get ONLY IntelliFlow-managed subnets to avoid picking up customer subnets
            public_subnets, private_subnets = self.get_vpc_subnets(existing_vpc_id, unique_context_id)

            if not public_subnets and not private_subnets:
                module_logger.warning(
                    f"VPC {existing_vpc_id} exists but has NO IntelliFlow-managed subnets. "
                    f"This VPC may have been created outside IntelliFlow. Will provision subnets."
                )

            # IDEMPOTENCY: Use helper to get one IF-managed subnet per AZ
            # This prevents duplicate subnets in same AZ from causing issues
            public_azs_to_subnets = self._get_intelliflow_managed_subnets_per_az(existing_vpc_id, unique_context_id, "Public")
            private_azs_to_subnets = self._get_intelliflow_managed_subnets_per_az(existing_vpc_id, unique_context_id, "Private")

            # Extract subnet lists (now guaranteed one per AZ)
            public_subnets = list(public_azs_to_subnets.values())
            private_subnets = list(private_azs_to_subnets.values())

            module_logger.info(
                f"Found IntelliFlow-managed subnets: {len(public_subnets)} public across {len(public_azs_to_subnets)} AZs, "
                f"{len(private_subnets)} private across {len(private_azs_to_subnets)} AZs"
            )

            # Get infrastructure components early (needed for routing fixes)
            igw_id = None
            nat_gateway_ids = []
            try:
                # Get Internet Gateway
                igw_response = exponential_retry(
                    self._ec2_client.describe_internet_gateways,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "attachment.vpc-id", "Values": [existing_vpc_id]}],
                )
                igw_id = igw_response["InternetGateways"][0]["InternetGatewayId"] if igw_response["InternetGateways"] else None

                # Get NAT gateways using existing method
                nat_gateway_ids = self.get_vpc_nat_gateways(existing_vpc_id)

            except ClientError as e:
                module_logger.warning(f"Could not retrieve existing infrastructure details: {e}")

            # AUTOMATIC FIXING: Check and fix route table configuration for private subnets
            fixed_route_table_ids = []
            if private_subnets and nat_gateway_ids:
                routing_issues = self._validate_private_subnet_routing(existing_vpc_id, private_subnets)
                if routing_issues:
                    module_logger.warning(f"Detected {len(routing_issues)} routing issues - fixing automatically...")

                    # Fix each broken subnet by creating proper route table with NAT gateway
                    for i, issue in enumerate(routing_issues):
                        subnet_id = issue["subnet_id"]
                        # Use corresponding NAT gateway (round-robin if not enough)
                        nat_gw_id = nat_gateway_ids[i % len(nat_gateway_ids)]

                        try:
                            new_rt_id = self._fix_private_subnet_routing(existing_vpc_id, subnet_id, nat_gw_id, unique_context_id, i)
                            fixed_route_table_ids.append(new_rt_id)
                            module_logger.info(f"✓ Fixed routing for subnet {subnet_id}")
                        except Exception as fix_error:
                            module_logger.error(f"✗ Failed to fix subnet {subnet_id}: {fix_error}")

                    # Apply complete network connectivity for fixed subnets
                    if fixed_route_table_ids:
                        # 1. Add S3 VPC endpoint to newly created route tables for bootstrap reliability
                        try:
                            existing_endpoints = self.get_vpc_s3_endpoints(existing_vpc_id)
                            if existing_endpoints:
                                # Update existing S3 endpoint to include new route tables
                                endpoint_id = existing_endpoints[0]
                                exponential_retry(
                                    self._ec2.meta.client.modify_vpc_endpoint,
                                    {"RequestLimitExceeded", "ServiceUnavailable"},
                                    VpcEndpointId=endpoint_id,
                                    AddRouteTableIds=fixed_route_table_ids,
                                )
                                module_logger.info(f"Added {len(fixed_route_table_ids)} fixed route tables to S3 endpoint")
                            else:
                                # Create new S3 endpoint with all route tables
                                self.create_s3_vpc_endpoint(existing_vpc_id, fixed_route_table_ids, unique_context_id)
                                module_logger.info(f"Created S3 endpoint for {len(fixed_route_table_ids)} fixed route tables")
                        except Exception as s3_error:
                            module_logger.warning(f"Could not add S3 endpoint to fixed route tables: {s3_error}")

                        # 2. Apply permissive Network ACL to fixed private subnets for internet access
                        fixed_subnet_ids = [issue["subnet_id"] for issue in routing_issues]
                        try:
                            self.ensure_private_subnets_internet_access(existing_vpc_id, fixed_subnet_ids, unique_context_id)
                            module_logger.info(f"Applied Network ACL to {len(fixed_subnet_ids)} fixed subnets")
                        except Exception as acl_error:
                            module_logger.warning(f"Could not apply Network ACL to fixed subnets: {acl_error}")

                    module_logger.info(f"Routing fixes completed for {len(routing_issues)} subnets")

            # Track if we need to create new subnets for AZ coverage
            az_coverage_fixed = False
            new_public_for_az_fix = []
            new_private_for_az_fix = []

            # REDSHIFT REQUIREMENT: Check AZ distribution (need at least subnet_count unique AZs, not equal distribution)
            # We only need to ensure all required AZs are covered, not that subnets are evenly distributed
            try:
                if private_subnets:
                    covered_azs = set(private_azs_to_subnets.keys())
                    unique_az_count = len(covered_azs)

                    if unique_az_count < subnet_count:
                        # Missing AZ coverage - need to create subnets in additional AZs
                        azs_needed = subnet_count - unique_az_count
                        module_logger.warning(
                            f"INSUFFICIENT AZ COVERAGE: {len(private_subnets)} private subnets cover only "
                            f"{unique_az_count} AZs {list(covered_azs)}, but Redshift Serverless requires {subnet_count} unique AZs. "
                            f"Creating {azs_needed} additional subnet pair(s) in different AZ(s)."
                        )

                        # Create additional subnets in new AZs
                        new_public_for_az_fix, new_private_for_az_fix = self.create_subnets_across_azs(
                            existing_vpc_id,
                            unique_context_id,
                            azs_needed,
                            start_index=subnet_count,  # Use fixed index to avoid collisions
                            exclude_azs=list(covered_azs),  # Exclude AZs that already have IntelliFlow subnets
                        )

                        # Update subnet lists and mappings
                        public_subnets.extend(new_public_for_az_fix)
                        private_subnets.extend(new_private_for_az_fix)
                        az_coverage_fixed = True

                        module_logger.info(
                            f"Fixed AZ coverage: created {len(new_private_for_az_fix)} additional subnet pairs. "
                            f"Now have {len(private_subnets)} private subnets covering {len(covered_azs) + azs_needed} AZs."
                        )

                        # CRITICAL: Apply COMPLETE network connectivity to new subnets
                        # This includes: IGW routes for public subnets, NAT gateways, private routing, S3 endpoints, Network ACLs
                        try:
                            self._apply_complete_network_connectivity(
                                existing_vpc_id,
                                igw_id,
                                new_public_for_az_fix,
                                new_private_for_az_fix,
                                unique_context_id,
                            )
                            module_logger.info("Applied complete network connectivity to new subnets for AZ coverage")
                        except Exception as network_error:
                            module_logger.error(f"Failed to apply complete network connectivity: {network_error}")
                            raise
                    else:
                        module_logger.info(
                            f"AZ coverage sufficient: {unique_az_count} AZs covered {list(covered_azs)} " f"(required: {subnet_count})"
                        )

            except Exception as e:
                module_logger.warning(f"Could not check/fix AZ coverage for existing subnets: {e}")

            # Check if we have sufficient subnets and NAT gateways
            current_private_count = len(private_subnets)
            current_public_count = len(public_subnets)
            current_nat_gateway_count = len(nat_gateway_ids)

            # CRITICAL: Ensure Internet Gateway exists (this fixes incomplete VPC setups)
            if not igw_id:
                module_logger.info("Internet Gateway missing - creating new one (this fixes incomplete VPC setup)")
                igw_id = self.create_internet_gateway(unique_context_id, existing_vpc_id)

            # If we fixed AZ coverage, we need to create NAT gateways and configure routing
            if az_coverage_fixed and new_public_for_az_fix:
                module_logger.info(
                    f"Creating NAT gateways and routing for {len(new_public_for_az_fix)} new subnets added to fix AZ distribution"
                )

                # CRITICAL: Ensure new public subnets have IGW routes FIRST
                # NAT gateways need internet connectivity via IGW to function
                module_logger.info("Ensuring new public subnets have Internet Gateway routes for NAT gateway connectivity...")
                self._ensure_public_subnets_have_internet_gateway_routes(existing_vpc_id, igw_id, new_public_for_az_fix, unique_context_id)

                # Create NAT gateways for the new public subnets
                new_nat_gateways = self.create_nat_gateways(new_public_for_az_fix, unique_context_id)
                nat_gateway_ids.extend(new_nat_gateways)

                # CRITICAL: Wait longer for NAT gateways to become fully operational
                module_logger.info("Waiting for new NAT gateways to become fully operational...")
                self.wait_for_nat_gateways_available(new_nat_gateways, timeout_minutes=5)

                # Configure routing for the new private subnets (connect to NAT gateways)
                self._configure_additional_routing(
                    existing_vpc_id,
                    igw_id,
                    new_public_for_az_fix,
                    new_private_for_az_fix,
                    new_nat_gateways,
                    unique_context_id,
                )

                # Apply complete network connectivity (S3 endpoint, Network ACL)
                try:
                    # Get route table IDs for new private subnets
                    new_rt_ids = []
                    for subnet_id in new_private_for_az_fix:
                        rt_response = exponential_retry(
                            self._ec2_client.describe_route_tables,
                            {"RequestLimitExceeded", "ServiceUnavailable"},
                            Filters=[
                                {"Name": "association.subnet-id", "Values": [subnet_id]},
                                {"Name": "vpc-id", "Values": [existing_vpc_id]},
                            ],
                        )
                        if rt_response["RouteTables"]:
                            new_rt_ids.append(rt_response["RouteTables"][0]["RouteTableId"])

                    # Add S3 endpoint to new route tables
                    if new_rt_ids:
                        existing_endpoints = self.get_vpc_s3_endpoints(existing_vpc_id)
                        if existing_endpoints:
                            exponential_retry(
                                self._ec2.meta.client.modify_vpc_endpoint,
                                {"RequestLimitExceeded", "ServiceUnavailable"},
                                VpcEndpointId=existing_endpoints[0],
                                AddRouteTableIds=new_rt_ids,
                            )
                            module_logger.info(f"Added {len(new_rt_ids)} new route tables to S3 endpoint")

                    # Apply Network ACL to new private subnets
                    self.ensure_private_subnets_internet_access(existing_vpc_id, new_private_for_az_fix, unique_context_id)

                except Exception as network_error:
                    module_logger.warning(f"Could not apply full network connectivity to new subnets: {network_error}")

                module_logger.info(f"Completed NAT gateway and routing configuration for {len(new_private_for_az_fix)} new subnets")

            # IDEMPOTENCY: Check if we have sufficient AZ coverage (not count)
            # We need at least subnet_count unique AZs covered
            unique_az_count = len(set(private_azs_to_subnets.keys()) | set(public_azs_to_subnets.keys()))

            if unique_az_count >= subnet_count and current_nat_gateway_count >= subnet_count and igw_id:
                # Sufficient AZ coverage and NAT gateways - reuse as-is but validate/fix routing
                module_logger.info(
                    f"Reusing existing VPC {existing_vpc_id} with sufficient AZ coverage "
                    f"(private: {current_private_count} across {len(private_azs_to_subnets)} AZs, "
                    f"public: {current_public_count} across {len(public_azs_to_subnets)} AZs, "
                    f"NAT gateways: {current_nat_gateway_count}, required AZs: {subnet_count})"
                )

                # Select subnets: take one per AZ up to subnet_count AZs (already deduplicated by helper)
                selected_private_subnets = private_subnets[:subnet_count]
                selected_public_subnets = public_subnets[:subnet_count]

                # AUTOMATIC FIXING: Fix any broken routing before returning
                fixed_route_table_ids = []
                routing_issues = self._validate_private_subnet_routing(existing_vpc_id, selected_private_subnets)
                if routing_issues:
                    module_logger.warning(f"Detected {len(routing_issues)} routing issues - fixing automatically...")

                    for i, issue in enumerate(routing_issues):
                        subnet_id = issue["subnet_id"]
                        nat_gw_id = nat_gateway_ids[i % len(nat_gateway_ids)]

                        try:
                            new_rt_id = self._fix_private_subnet_routing(existing_vpc_id, subnet_id, nat_gw_id, unique_context_id, i)
                            fixed_route_table_ids.append(new_rt_id)
                            module_logger.info(f"✓ Fixed routing for subnet {subnet_id}")
                        except Exception as fix_error:
                            module_logger.error(f"✗ Failed to fix subnet {subnet_id}: {fix_error}")

                    # Apply complete network connectivity for fixed subnets
                    if fixed_route_table_ids:
                        try:
                            # Add S3 endpoint
                            existing_endpoints = self.get_vpc_s3_endpoints(existing_vpc_id)
                            if existing_endpoints:
                                exponential_retry(
                                    self._ec2.meta.client.modify_vpc_endpoint,
                                    {"RequestLimitExceeded", "ServiceUnavailable"},
                                    VpcEndpointId=existing_endpoints[0],
                                    AddRouteTableIds=fixed_route_table_ids,
                                )
                                module_logger.info(f"Added fixed route tables to S3 endpoint")

                            # Apply Network ACL
                            fixed_subnet_ids = [issue["subnet_id"] for issue in routing_issues]
                            self.ensure_private_subnets_internet_access(existing_vpc_id, fixed_subnet_ids, unique_context_id)
                            module_logger.info(f"Applied Network ACL to fixed subnets")
                        except Exception as network_error:
                            module_logger.warning(f"Could not apply full network connectivity: {network_error}")

                # Ensure public subnets have proper Internet Gateway routes (fixes incomplete setups)
                self._ensure_public_subnets_have_internet_gateway_routes(
                    existing_vpc_id, igw_id, selected_public_subnets, unique_context_id
                )

                # CLEANUP: Delete duplicate/unused resources after fixing routing
                all_selected = selected_public_subnets + selected_private_subnets
                self._cleanup_duplicate_subnets(existing_vpc_id, unique_context_id, all_selected)
                self._cleanup_orphaned_route_tables(existing_vpc_id, unique_context_id, all_selected)
                self._cleanup_orphaned_nat_gateways(existing_vpc_id, unique_context_id, nat_gateway_ids[:subnet_count])

                return {
                    "vpc_id": existing_vpc_id,
                    "internet_gateway_id": igw_id,
                    "public_subnet_ids": selected_public_subnets,
                    "private_subnet_ids": selected_private_subnets,
                    "nat_gateway_ids": nat_gateway_ids[:subnet_count],
                }

            elif current_private_count > 0 and current_public_count > 0:
                # We have some subnets but need more - extend the existing VPC
                module_logger.info(
                    f"Extending existing VPC {existing_vpc_id} with additional infrastructure "
                    f"(current private: {current_private_count}, public: {current_public_count}, "
                    f"NAT gateways: {current_nat_gateway_count}, required: {subnet_count})"
                )

                # CRITICAL FIX: Check AZ distribution of existing subnets
                # Redshift requires subnets in different AZs
                try:
                    existing_private_azs = self.get_subnet_azs(private_subnets)
                    existing_public_azs = self.get_subnet_azs(public_subnets)
                    unique_az_count = len(set(existing_private_azs + existing_public_azs))

                    module_logger.info(
                        f"Existing subnet AZ distribution: private={existing_private_azs}, "
                        f"public={existing_public_azs}, unique_azs={unique_az_count}"
                    )

                    # Determine if we need additional subnets based on AZ requirements
                    # We need at least subnet_count subnets in different AZs
                    if unique_az_count < subnet_count:
                        # Need to create subnets in new AZs
                        additional_azs_needed = subnet_count - unique_az_count
                        additional_subnet_pairs_needed = max(
                            additional_azs_needed, subnet_count - current_private_count, subnet_count - current_public_count
                        )
                        module_logger.info(
                            f"Need {additional_subnet_pairs_needed} additional subnet pairs to meet "
                            f"AZ distribution requirement ({unique_az_count} -> {subnet_count} unique AZs)"
                        )
                    else:
                        # Calculate based on total count
                        additional_subnet_pairs_needed = max(subnet_count - current_private_count, subnet_count - current_public_count)
                        module_logger.info(f"Need {additional_subnet_pairs_needed} additional subnet pairs based on count")

                except Exception as e:
                    module_logger.warning(f"Could not check AZ distribution, falling back to count-based calculation: {e}")
                    additional_subnet_pairs_needed = max(subnet_count - current_private_count, subnet_count - current_public_count)
                    existing_private_azs = []

                # Calculate how many additional NAT gateways we need
                additional_nat_gateways_needed = max(0, subnet_count - current_nat_gateway_count)

                if additional_subnet_pairs_needed > 0:
                    # Create additional subnets in AZs that don't have existing subnets
                    # This ensures proper AZ distribution for Redshift Serverless
                    new_public_subnets, new_private_subnets = self.create_subnets_across_azs(
                        existing_vpc_id,
                        unique_context_id,
                        additional_subnet_pairs_needed,
                        start_index=max(current_private_count, current_public_count),
                        exclude_azs=existing_private_azs,  # Exclude AZs already used
                    )

                    # Combine with existing subnets
                    all_public_subnets = public_subnets + new_public_subnets
                    all_private_subnets = private_subnets + new_private_subnets
                else:
                    all_public_subnets = public_subnets
                    all_private_subnets = private_subnets
                    new_public_subnets = []
                    new_private_subnets = []

                # Create additional NAT gateways if needed
                if additional_nat_gateways_needed > 0:
                    # Use new public subnets first, then existing ones if needed
                    available_public_subnets = new_public_subnets + public_subnets
                    public_subnets_for_nat = available_public_subnets[:additional_nat_gateways_needed]

                    additional_nat_gateways = self.create_nat_gateways(public_subnets_for_nat, unique_context_id)
                    nat_gateway_ids.extend(additional_nat_gateways)

                    # Wait for additional NAT gateways to be available before configuring routing
                    module_logger.info("Waiting for additional NAT gateways to be available before configuring routing...")
                    self.wait_for_nat_gateways_available(additional_nat_gateways, timeout_minutes=2)

                    # Configure routing for additional NAT gateways
                    if additional_nat_gateways:
                        if new_private_subnets or new_public_subnets:
                            # We have new subnets - use existing additional routing method
                            routing_public_subnets = public_subnets_for_nat if new_public_subnets else []
                            self._configure_additional_routing(
                                existing_vpc_id,
                                igw_id,
                                routing_public_subnets,
                                new_private_subnets,
                                additional_nat_gateways,
                                unique_context_id,
                            )
                        else:
                            # No new subnets, but we have new NAT gateways - need to connect existing private subnets
                            # that don't have NAT gateway routing to the new NAT gateways
                            existing_private_subnets_needing_nat = all_private_subnets[current_nat_gateway_count:subnet_count]
                            self._configure_nat_gateway_routing_for_existing_subnets(
                                existing_vpc_id, existing_private_subnets_needing_nat, additional_nat_gateways, unique_context_id
                            )

                # CRITICAL FIX: Select subnets across different AZs for Redshift Serverless compatibility
                selected_public_subnets = self.select_subnets_across_different_azs(all_public_subnets, subnet_count)
                selected_private_subnets = self.select_subnets_across_different_azs(all_private_subnets, subnet_count)

                # AUTOMATIC FIXING: Fix any broken routing in selected subnets
                fixed_route_table_ids = []
                routing_issues = self._validate_private_subnet_routing(existing_vpc_id, selected_private_subnets)
                if routing_issues:
                    module_logger.warning(f"Detected {len(routing_issues)} routing issues - fixing automatically...")

                    for i, issue in enumerate(routing_issues):
                        subnet_id = issue["subnet_id"]
                        nat_gw_id = nat_gateway_ids[i % len(nat_gateway_ids)]

                        try:
                            new_rt_id = self._fix_private_subnet_routing(existing_vpc_id, subnet_id, nat_gw_id, unique_context_id, i)
                            fixed_route_table_ids.append(new_rt_id)
                            module_logger.info(f"✓ Fixed routing for subnet {subnet_id}")
                        except Exception as fix_error:
                            module_logger.error(f"✗ Failed to fix subnet {subnet_id}: {fix_error}")

                    # Apply complete network connectivity for fixed subnets
                    if fixed_route_table_ids:
                        try:
                            # Add S3 endpoint
                            existing_endpoints = self.get_vpc_s3_endpoints(existing_vpc_id)
                            if existing_endpoints:
                                exponential_retry(
                                    self._ec2.meta.client.modify_vpc_endpoint,
                                    {"RequestLimitExceeded", "ServiceUnavailable"},
                                    VpcEndpointId=existing_endpoints[0],
                                    AddRouteTableIds=fixed_route_table_ids,
                                )
                                module_logger.info(f"Added fixed route tables to S3 endpoint")

                            # Apply Network ACL
                            fixed_subnet_ids = [issue["subnet_id"] for issue in routing_issues]
                            self.ensure_private_subnets_internet_access(existing_vpc_id, fixed_subnet_ids, unique_context_id)
                            module_logger.info(f"Applied Network ACL to fixed subnets")
                        except Exception as network_error:
                            module_logger.warning(f"Could not apply full network connectivity: {network_error}")

                # Ensure all public subnets have proper Internet Gateway routes
                self._ensure_public_subnets_have_internet_gateway_routes(
                    existing_vpc_id, igw_id, selected_public_subnets, unique_context_id
                )

                # CLEANUP: Delete duplicate/unused resources after fixing routing
                all_selected = selected_public_subnets + selected_private_subnets
                self._cleanup_duplicate_subnets(existing_vpc_id, unique_context_id, all_selected)
                self._cleanup_orphaned_route_tables(existing_vpc_id, unique_context_id, all_selected)
                self._cleanup_orphaned_nat_gateways(existing_vpc_id, unique_context_id, nat_gateway_ids[:subnet_count])

                return {
                    "vpc_id": existing_vpc_id,
                    "internet_gateway_id": igw_id,
                    "public_subnet_ids": selected_public_subnets,
                    "private_subnet_ids": selected_private_subnets,
                    "nat_gateway_ids": nat_gateway_ids[:subnet_count],
                }
            else:
                # VPC exists but has insufficient subnets - treat as new VPC setup, but reuse existing NAT gateways if any
                module_logger.info(
                    f"Existing VPC {existing_vpc_id} has insufficient infrastructure, setting up complete infrastructure "
                    f"(existing NAT gateways: {current_nat_gateway_count})"
                )
                vpc_id = existing_vpc_id

                # Create all subnets
                public_subnet_ids, private_subnet_ids = self.create_subnets_across_azs(vpc_id, unique_context_id, subnet_count)

                # Create NAT gateways, accounting for existing ones
                needed_nat_gateways = max(0, subnet_count - current_nat_gateway_count)
                if needed_nat_gateways > 0:
                    new_nat_gateways = self.create_nat_gateways(public_subnet_ids[:needed_nat_gateways], unique_context_id)
                    nat_gateway_ids.extend(new_nat_gateways)

                    # Wait for new NAT gateways to be available before configuring routing
                    module_logger.info("Waiting for new NAT gateways to be available before configuring routing...")
                    self.wait_for_nat_gateways_available(new_nat_gateways, timeout_minutes=2)

                # Configure routing
                self.configure_routing(vpc_id, igw_id, public_subnet_ids, private_subnet_ids, nat_gateway_ids, unique_context_id)
        else:
            # No existing VPC - create new infrastructure
            module_logger.info("No existing VPC found, creating new VPC infrastructure")

            # Create VPC
            vpc_id = self.create_vpc(unique_context_id)

            # Create Internet Gateway
            igw_id = self.create_internet_gateway(unique_context_id, vpc_id)

            # Create subnets across AZs
            public_subnet_ids, private_subnet_ids = self.create_subnets_across_azs(vpc_id, unique_context_id, subnet_count)

            # Create NAT gateways
            nat_gateway_ids = self.create_nat_gateways(public_subnet_ids, unique_context_id)

            # Wait for NAT gateways to be available before configuring routing
            if nat_gateway_ids:
                module_logger.info("Waiting for NAT gateways to be available before configuring routing...")
                self.wait_for_nat_gateways_available(nat_gateway_ids, timeout_minutes=2)

            # Configure routing
            self.configure_routing(vpc_id, igw_id, public_subnet_ids, private_subnet_ids, nat_gateway_ids, unique_context_id)

        result = {
            "vpc_id": vpc_id,
            "internet_gateway_id": igw_id,
            "public_subnet_ids": public_subnet_ids,
            "private_subnet_ids": private_subnet_ids,
            "nat_gateway_ids": nat_gateway_ids,
        }

        module_logger.info(
            f"Successfully provisioned VPC infrastructure: VPC {vpc_id}, "
            f"Private subnets {private_subnet_ids}, Public subnets {public_subnet_ids}, "
            f"NAT gateways {nat_gateway_ids}"
        )

        return result

    def _cleanup_network_interfaces(self, vpc_id: str, max_wait_time: int = 180) -> None:
        """Clean up lingering network interfaces in the VPC before security group deletion.

        CRITICAL FIX: EMR clusters create network interfaces (ENIs) that can remain in
        'detaching' or 'deleting' state for several minutes after cluster termination.
        These ENIs prevent security group deletion with DependencyViolation errors.

        This method:
        1. Lists all ENIs in the VPC
        2. Detaches any still-attached ENIs
        3. Deletes all ENIs (with retry logic)
        4. Waits for ENI deletion to complete

        Args:
            vpc_id: VPC ID to clean up ENIs from
            max_wait_time: Maximum time in seconds to wait for ENI deletion (default 180s)
        """
        try:
            module_logger.info(f"Cleaning up network interfaces in VPC {vpc_id}")

            # Get all network interfaces in the VPC
            eni_response = exponential_retry(
                self._ec2_client.describe_network_interfaces,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
            )

            enis = eni_response.get("NetworkInterfaces", [])

            if not enis:
                module_logger.info("No network interfaces found in VPC")
                return

            module_logger.info(f"Found {len(enis)} network interface(s) to clean up")

            # Track ENIs that need deletion
            enis_to_delete = []

            for eni in enis:
                eni_id = eni["NetworkInterfaceId"]
                eni_status = eni["Status"]
                eni_type = eni.get("InterfaceType", "interface")
                attachment = eni.get("Attachment", {})

                module_logger.info(f"Processing ENI {eni_id}: status={eni_status}, type={eni_type}, " f"attached={bool(attachment)}")

                # Detach ENI if it's still attached
                if attachment and attachment.get("AttachmentId"):
                    attachment_id = attachment["AttachmentId"]

                    # Skip ENIs that are required by AWS services (e.g., NAT gateway ENIs)
                    # NAT gateway ENIs have InterfaceType='nat_gateway' and are automatically
                    # deleted when NAT gateway is deleted
                    if eni_type in ["nat_gateway", "gateway_load_balancer_endpoint", "vpc_endpoint"]:
                        module_logger.info(
                            f"Skipping AWS-managed ENI {eni_id} (type={eni_type}) - " f"will be deleted automatically with parent resource"
                        )
                        continue

                    try:
                        module_logger.info(f"Detaching ENI {eni_id} (attachment: {attachment_id})")
                        exponential_retry(
                            self._ec2_client.detach_network_interface,
                            {"RequestLimitExceeded"},
                            AttachmentId=attachment_id,
                            Force=True,  # Force detachment to handle stuck ENIs
                        )
                        module_logger.info(f"Successfully initiated detachment of ENI {eni_id}")
                        enis_to_delete.append(eni_id)
                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code", "")
                        if error_code in ["InvalidAttachmentID.NotFound", "InvalidParameterValue"]:
                            module_logger.info(f"ENI {eni_id} already detached or attachment not found")
                            enis_to_delete.append(eni_id)
                        else:
                            module_logger.warning(f"Could not detach ENI {eni_id}: {e}")
                            # Still try to delete it
                            enis_to_delete.append(eni_id)
                elif eni_type not in ["nat_gateway", "gateway_load_balancer_endpoint", "vpc_endpoint"]:
                    # Not attached and not AWS-managed - safe to delete
                    enis_to_delete.append(eni_id)
                else:
                    module_logger.info(f"Skipping AWS-managed ENI {eni_id} (type={eni_type}) - " f"will be deleted automatically")

            if not enis_to_delete:
                module_logger.info("No ENIs require explicit deletion (all are AWS-managed)")
                return

            # Wait a bit for detachments to process
            if enis_to_delete:
                module_logger.info(f"Waiting 10 seconds for {len(enis_to_delete)} ENI detachment(s) to process...")
                time.sleep(10)

            # Delete ENIs with retry logic
            deleted_count = 0
            start_time = time.time()

            while enis_to_delete and (time.time() - start_time) < max_wait_time:
                remaining_enis = []

                for eni_id in enis_to_delete:
                    try:
                        exponential_retry(
                            self._ec2_client.delete_network_interface,
                            {"RequestLimitExceeded"},
                            NetworkInterfaceId=eni_id,
                        )
                        module_logger.info(f"Deleted network interface {eni_id}")
                        deleted_count += 1
                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code", "")
                        if error_code in ["InvalidNetworkInterfaceID.NotFound"]:
                            module_logger.info(f"Network interface {eni_id} already deleted")
                            deleted_count += 1
                        elif error_code in ["InvalidParameterValue", "InvalidNetworkInterface.InUse"]:
                            # ENI still in use or detaching - retry later
                            module_logger.info(f"ENI {eni_id} still in use or detaching, will retry...")
                            remaining_enis.append(eni_id)
                        else:
                            module_logger.warning(f"Failed to delete ENI {eni_id}: {e}")
                            remaining_enis.append(eni_id)

                enis_to_delete = remaining_enis

                if enis_to_delete:
                    module_logger.info(
                        f"Waiting for {len(enis_to_delete)} ENI(s) to become deletable... " f"(elapsed: {int(time.time() - start_time)}s)"
                    )
                    time.sleep(10)

            if enis_to_delete:
                module_logger.warning(
                    f"Could not delete {len(enis_to_delete)} ENI(s) after {max_wait_time}s: {enis_to_delete}. "
                    f"These may be managed by active EMR clusters or other AWS services. "
                    f"Security group deletion may fail with DependencyViolation."
                )
            else:
                module_logger.info(f"Successfully cleaned up {deleted_count} network interface(s)")

        except Exception as e:
            module_logger.warning(
                f"Error during network interface cleanup: {e}. " f"Security group deletion may fail with DependencyViolation."
            )

    def _cleanup_security_groups(self, vpc_id: str) -> None:
        """Clean up security groups with comprehensive rule removal to handle cross-references.

        CRITICAL FIX: EMR-managed security groups reference each other in their ingress/egress rules,
        causing DependencyViolation errors. This method:
        1. Lists all security groups in VPC
        2. Removes all ingress and egress rules (breaks cross-references)
        3. Deletes security groups with retries
        4. Allows VPC deletion to proceed even if EMR-managed groups can't be deleted

        Args:
            vpc_id: VPC ID to clean up security groups from
        """
        try:
            module_logger.info(f"Cleaning up security groups in VPC {vpc_id}")

            # Get all security groups in the VPC
            sg_response = exponential_retry(
                self._ec2_client.describe_security_groups,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
            )

            security_groups = sg_response.get("SecurityGroups", [])

            if not security_groups:
                module_logger.info("No security groups found in VPC")
                return

            # Filter out default security group (cannot be deleted)
            sg_to_cleanup = [sg for sg in security_groups if sg["GroupName"] != "default"]

            if not sg_to_cleanup:
                module_logger.info("Only default security group found - no cleanup needed")
                return

            module_logger.info(f"Found {len(sg_to_cleanup)} security group(s) to clean up")

            # STEP 1: Remove all ingress and egress rules to break cross-references
            # This is critical for EMR-managed security groups that reference each other
            module_logger.info("Step 1: Removing all ingress/egress rules from security groups to break cross-references")
            for sg in sg_to_cleanup:
                sg_id = sg["GroupId"]
                sg_name = sg["GroupName"]

                try:
                    # Remove all ingress rules
                    if sg.get("IpPermissions"):
                        exponential_retry(
                            self._ec2_client.revoke_security_group_ingress,
                            {"RequestLimitExceeded"},
                            GroupId=sg_id,
                            IpPermissions=sg["IpPermissions"],
                        )
                        module_logger.info(f"Removed {len(sg['IpPermissions'])} ingress rule(s) from {sg_id} ({sg_name})")

                    # Remove all egress rules
                    if sg.get("IpPermissionsEgress"):
                        exponential_retry(
                            self._ec2_client.revoke_security_group_egress,
                            {"RequestLimitExceeded"},
                            GroupId=sg_id,
                            IpPermissions=sg["IpPermissionsEgress"],
                        )
                        module_logger.info(f"Removed {len(sg['IpPermissionsEgress'])} egress rule(s) from {sg_id} ({sg_name})")

                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["InvalidGroup.NotFound", "InvalidGroupId.NotFound"]:
                        module_logger.info(f"Security group {sg_id} already deleted")
                    else:
                        module_logger.warning(f"Could not remove rules from security group {sg_id}: {e}")

            # STEP 2: Delete security groups with retries and exponential backoff
            # Some groups may still have dependencies even after rule removal
            module_logger.info("Step 2: Deleting security groups (with retry logic for remaining dependencies)")

            emr_managed_groups_with_dependencies = []
            deleted_count = 0

            for sg in sg_to_cleanup:
                sg_id = sg["GroupId"]
                sg_name = sg["GroupName"]

                try:
                    exponential_retry(
                        self._ec2_client.delete_security_group,
                        {"RequestLimitExceeded"},
                        GroupId=sg_id,
                    )
                    module_logger.info(f"Deleted security group {sg_id} ({sg_name})")
                    deleted_count += 1
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["InvalidGroup.NotFound", "InvalidGroupId.NotFound"]:
                        module_logger.info(f"Security group {sg_id} already deleted")
                        deleted_count += 1
                    elif error_code == "DependencyViolation":
                        # Track EMR-managed groups that still have dependencies
                        if sg_name.startswith("ElasticMapReduce-"):
                            emr_managed_groups_with_dependencies.append(f"{sg_id} ({sg_name})")
                            module_logger.warning(
                                f"EMR-managed security group {sg_id} ({sg_name}) still has dependencies. "
                                f"This is expected - EMR service manages these groups. VPC deletion will proceed."
                            )
                        else:
                            module_logger.warning(f"Cannot delete security group {sg_id} ({sg_name}) due to dependencies: {e}")
                    else:
                        module_logger.warning(f"Failed to delete security group {sg_id} ({sg_name}): {e}")

            if emr_managed_groups_with_dependencies:
                module_logger.info(
                    f"Note: {len(emr_managed_groups_with_dependencies)} EMR-managed security group(s) remain "
                    f"with dependencies: {emr_managed_groups_with_dependencies}. "
                    f"These are managed by AWS EMR service and will be cleaned up automatically by AWS. "
                    f"VPC deletion can still proceed."
                )

            module_logger.info(
                f"Security group cleanup completed: {deleted_count} deleted, "
                f"{len(emr_managed_groups_with_dependencies)} EMR-managed groups deferred to AWS"
            )

        except Exception as e:
            module_logger.warning(
                f"Error during security group cleanup: {e}. "
                f"VPC deletion may fail, but this is expected if EMR-managed groups still exist."
            )

    def _cleanup_orphaned_elastic_ips(self, vpc_id: str) -> None:
        """Clean up ALL orphaned Elastic IPs associated with the VPC.

        CRITICAL FIX: Previous cleanup only released EIPs attached to NAT gateways.
        This method finds ALL EIPs that:
        - Are not currently associated with any resource (NetworkInterfaceId is None)
        - Belong to VPC domain
        - Optionally have IntelliFlow tags (but also catches untagged orphans)

        This handles EIPs left behind from:
        - Deleted NAT gateways that weren't properly cleaned up
        - Failed cleanup operations
        - Manual resource deletions

        Args:
            vpc_id: VPC ID to check for orphaned EIPs
        """
        try:
            module_logger.info(f"Cleaning up orphaned Elastic IPs for VPC {vpc_id}")

            # Get all VPC addresses (not filtered by association status)
            address_response = exponential_retry(
                self._ec2_client.describe_addresses,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "domain", "Values": ["vpc"]}],
            )

            # Find orphaned EIPs (not associated with any resource)
            # We need to check which ones belong to this VPC's resources
            orphaned_eips = []

            for address in address_response.get("Addresses", []):
                allocation_id = address["AllocationId"]
                network_interface_id = address.get("NetworkInterfaceId")
                association_id = address.get("AssociationId")
                public_ip = address.get("PublicIp", "N/A")

                # EIP is orphaned if not associated with any network interface
                if not network_interface_id and not association_id:
                    # Check if this EIP was previously used in this VPC by checking tags
                    tags = {tag["Key"]: tag["Value"] for tag in address.get("Tags", [])}

                    # Consider it orphaned if:
                    # 1. Has no tags (likely from deleted resource), OR
                    # 2. Has IntelliFlow/NAT-Gateway tags (definitely ours), OR
                    # 3. Has Purpose=NAT-Gateway tag (legacy from older code)
                    is_orphaned_intelliflow_eip = (
                        not tags  # No tags at all (orphaned)
                        or "IntelliFlow" in tags  # IntelliFlow tagged
                        or tags.get("Purpose") == "NAT-Gateway"  # NAT purpose
                    )

                    if is_orphaned_intelliflow_eip:
                        orphaned_eips.append({"allocation_id": allocation_id, "public_ip": public_ip, "tags": tags})
                        module_logger.info(f"Found orphaned EIP {allocation_id} ({public_ip}) - " f"not associated with any resource")

            if not orphaned_eips:
                module_logger.info("No orphaned Elastic IPs found for this VPC")
                return

            module_logger.info(f"Releasing {len(orphaned_eips)} orphaned Elastic IP(s)")

            released_count = 0
            for eip_info in orphaned_eips:
                allocation_id = eip_info["allocation_id"]
                public_ip = eip_info["public_ip"]

                try:
                    exponential_retry(
                        self._ec2_client.release_address,
                        {"RequestLimitExceeded"},
                        AllocationId=allocation_id,
                    )
                    module_logger.info(f"Released orphaned Elastic IP {allocation_id} ({public_ip})")
                    released_count += 1
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["InvalidAllocationID.NotFound", "AuthFailure"]:
                        module_logger.info(f"Elastic IP {allocation_id} already released or not accessible")
                        released_count += 1
                    elif error_code == "InvalidAddress.InUse":
                        module_logger.warning(
                            f"Elastic IP {allocation_id} is still in use - " f"may be associated with resource not yet fully deleted"
                        )
                    else:
                        module_logger.warning(f"Failed to release Elastic IP {allocation_id}: {e}")

            module_logger.info(f"Orphaned EIP cleanup completed: {released_count}/{len(orphaned_eips)} released")

        except Exception as e:
            module_logger.warning(f"Error during orphaned Elastic IP cleanup: {e}. " f"Some EIPs may remain allocated.")

    def cleanup_vpc_infrastructure(self, vpc_id: str) -> None:
        """Clean up complete VPC infrastructure."""
        if not vpc_id:
            return

        try:
            module_logger.info(f"Cleaning up VPC infrastructure for {vpc_id}")

            # Get and delete S3 VPC endpoints first
            try:
                s3_endpoint_ids = self.get_vpc_s3_endpoints(vpc_id)
                for endpoint_id in s3_endpoint_ids:
                    try:
                        # Use correct AWS API method name (plural) and parameter format
                        exponential_retry(self._ec2_client.delete_vpc_endpoints, {"RequestLimitExceeded"}, VpcEndpointIds=[endpoint_id])
                        module_logger.info(f"Deleted S3 VPC endpoint {endpoint_id}")
                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code", "")
                        if error_code in ["InvalidVpcEndpointId.NotFound"]:
                            module_logger.info(f"S3 VPC endpoint {endpoint_id} already deleted")
                        else:
                            module_logger.warning(f"Failed to delete S3 VPC endpoint {endpoint_id}: {e}")
            except Exception as e:
                module_logger.warning(f"Could not retrieve or delete S3 VPC endpoints: {e}")

            # Get all resources in VPC
            subnets_response = exponential_retry(
                self._ec2_client.describe_subnets,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
            )
            subnet_ids = [subnet["SubnetId"] for subnet in subnets_response["Subnets"]]

            # Get NAT gateways
            try:
                nat_response = exponential_retry(
                    self._ec2_client.describe_nat_gateways,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
                )
                nat_gateway_ids = [
                    nat["NatGatewayId"] for nat in nat_response["NatGateways"] if nat["State"] not in ["deleted", "deleting"]
                ]
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code in ["InvalidNatGatewayID.NotFound", "NatGatewayNotFound", "InvalidFilter"]:
                    nat_gateway_ids = []
                else:
                    module_logger.error(f"Error retrieving NAT gateways for VPC {vpc_id}: {e}")
                    raise

            # Get Internet Gateway
            try:
                igw_response = exponential_retry(
                    self._ec2_client.describe_internet_gateways,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}],
                )
                igw_ids = [igw["InternetGatewayId"] for igw in igw_response["InternetGateways"]]
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code in ["InvalidInternetGatewayID.NotFound", "InvalidFilter"]:
                    igw_ids = []
                else:
                    module_logger.error(f"Error retrieving Internet gateways for VPC {vpc_id}: {e}")
                    raise

            # Get ALL Route Tables (we'll filter out main route table during deletion)
            try:
                rt_response = exponential_retry(
                    self._ec2_client.describe_route_tables,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
                )
                all_route_tables = rt_response["RouteTables"]
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code in ["InvalidRouteTableID.NotFound", "InvalidFilter"]:
                    all_route_tables = []
                else:
                    module_logger.error(f"Error retrieving route tables for VPC {vpc_id}: {e}")
                    raise

            # Get Elastic IPs associated with NAT gateways in this VPC
            elastic_ip_allocation_ids = []
            if nat_gateway_ids:
                try:
                    # Get EIPs from NAT gateway details
                    nat_details_response = exponential_retry(
                        self._ec2_client.describe_nat_gateways,
                        {"RequestLimitExceeded", "ServiceUnavailable"},
                        NatGatewayIds=nat_gateway_ids,
                    )
                    for nat_gateway in nat_details_response["NatGateways"]:
                        for nat_gateway_address in nat_gateway.get("NatGatewayAddresses", []):
                            if nat_gateway_address.get("AllocationId"):
                                elastic_ip_allocation_ids.append(nat_gateway_address["AllocationId"])
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code not in ["InvalidNatGatewayID.NotFound", "NatGatewayNotFound"]:
                        module_logger.warning(f"Could not retrieve EIP details from NAT gateways: {e}")

            # Delete NAT Gateways first (they need to be deleted before subnets)
            for nat_id in nat_gateway_ids:
                try:
                    exponential_retry(self._ec2_client.delete_nat_gateway, {"RequestLimitExceeded"}, NatGatewayId=nat_id)
                    module_logger.info(f"Deleted NAT gateway {nat_id}")
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["InvalidNatGatewayID.NotFound", "NatGatewayNotFound"]:
                        module_logger.info(f"NAT gateway {nat_id} already deleted")
                    else:
                        module_logger.error(f"Failed to delete NAT gateway {nat_id}: {e}")
                        # Don't raise here, continue with cleanup of other resources

            # Wait for NAT gateways deletion if any existed
            if nat_gateway_ids:
                self._wait_for_nat_gateways_deleted(nat_gateway_ids)

                # After NAT gateways are deleted, explicitly release any associated Elastic IPs
                for allocation_id in elastic_ip_allocation_ids:
                    try:
                        exponential_retry(self._ec2_client.release_address, {"RequestLimitExceeded"}, AllocationId=allocation_id)
                        module_logger.info(f"Released Elastic IP {allocation_id}")
                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code", "")
                        if error_code in ["InvalidAllocationID.NotFound", "AuthFailure"]:
                            module_logger.info(f"Elastic IP {allocation_id} already released or not found")
                        else:
                            module_logger.warning(f"Failed to release Elastic IP {allocation_id}: {e}")
                            # Continue with cleanup - don't let EIP release failures block everything

            # CRITICAL: Delete all custom route tables comprehensively
            # Following proper sequence: disassociate → clean routes → delete route tables
            route_tables_to_delete = []
            for route_table in all_route_tables:
                rt_id = route_table["RouteTableId"]

                # Skip main route table (AWS won't allow deletion of main route table)
                is_main = any(assoc.get("Main", False) for assoc in route_table.get("Associations", []))
                if is_main:
                    module_logger.info(f"Skipping main route table {rt_id}")
                    continue

                route_tables_to_delete.append(route_table)

            module_logger.info(f"Found {len(route_tables_to_delete)} custom route tables to delete")

            # First: disassociate ALL route table associations
            for route_table in route_tables_to_delete:
                rt_id = route_table["RouteTableId"]
                try:
                    for association in route_table.get("Associations", []):
                        if not association.get("Main", False) and association.get("SubnetId"):
                            association_id = association.get("RouteTableAssociationId")
                            if association_id:
                                try:
                                    exponential_retry(
                                        self._ec2_client.disassociate_route_table,
                                        {"RequestLimitExceeded"},
                                        AssociationId=association_id,
                                    )
                                    module_logger.info(f"Disassociated route table {rt_id} from subnet {association['SubnetId']}")
                                except ClientError as e:
                                    error_code = e.response.get("Error", {}).get("Code", "")
                                    if error_code in ["InvalidAssociationID.NotFound"]:
                                        module_logger.info(f"Association {association_id} already deleted")
                                    else:
                                        module_logger.warning(f"Failed to disassociate route table {rt_id}: {e}")
                except Exception as e:
                    module_logger.warning(f"Error during route table disassociation for {rt_id}: {e}")

            # Second: CLEAN all non-local routes from route tables (CRITICAL FIX!)
            # This removes stale routes to deleted NAT gateways, IGWs, etc.
            module_logger.info(f"Cleaning routes from {len(route_tables_to_delete)} route tables before deletion")
            for route_table in route_tables_to_delete:
                rt_id = route_table["RouteTableId"]
                try:
                    self._delete_non_local_routes_from_table(rt_id, route_table.get("Routes", []))
                except Exception as e:
                    module_logger.warning(f"Error cleaning routes from route table {rt_id}: {e}")

            # Third: delete all custom route tables (now safe since routes are cleaned)
            for route_table in route_tables_to_delete:
                rt_id = route_table["RouteTableId"]
                try:
                    exponential_retry(
                        self._ec2_client.delete_route_table, {"RequestLimitExceeded", "DependencyViolation"}, RouteTableId=rt_id
                    )
                    module_logger.info(f"Deleted route table {rt_id}")
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["InvalidRouteTableID.NotFound"]:
                        module_logger.info(f"Route table {rt_id} already deleted")
                    elif error_code == "DependencyViolation":
                        module_logger.warning(f"Cannot delete route table {rt_id} due to dependencies: {e}")
                    else:
                        module_logger.warning(f"Failed to delete route table {rt_id}: {e}")

            # Delete subnets
            for subnet_id in subnet_ids:
                try:
                    exponential_retry(self._ec2_client.delete_subnet, {"RequestLimitExceeded", "DependencyViolation"}, SubnetId=subnet_id)
                    module_logger.info(f"Deleted subnet {subnet_id}")
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["InvalidSubnetID.NotFound", "InvalidSubnet.NotFound"]:
                        module_logger.info(f"Subnet {subnet_id} already deleted")
                    elif error_code == "DependencyViolation":
                        module_logger.warning(f"Cannot delete subnet {subnet_id} due to dependencies, will retry after other resources")
                    else:
                        module_logger.error(f"Failed to delete subnet {subnet_id}: {e}")
                        # Don't raise here, continue with cleanup of other resources

            # Detach and delete Internet Gateways
            for igw_id in igw_ids:
                try:
                    exponential_retry(
                        self._ec2_client.detach_internet_gateway, {"RequestLimitExceeded"}, InternetGatewayId=igw_id, VpcId=vpc_id
                    )
                    exponential_retry(self._ec2_client.delete_internet_gateway, {"RequestLimitExceeded"}, InternetGatewayId=igw_id)
                    module_logger.info(f"Deleted Internet gateway {igw_id}")
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code in ["InvalidInternetGatewayID.NotFound", "Gateway.NotAttached"]:
                        module_logger.info(f"Internet gateway {igw_id} already detached/deleted")
                    else:
                        module_logger.error(f"Failed to delete Internet gateway {igw_id}: {e}")
                        # Don't raise here, continue with cleanup of other resources

            # CRITICAL FIX: Clean up network interfaces before security group deletion
            # EMR-managed ENIs can remain in "detaching" state after cluster termination,
            # preventing security group deletion with DependencyViolation errors
            self._cleanup_network_interfaces(vpc_id)

            # CRITICAL FIX: Clean up security groups with comprehensive rule removal
            # EMR-managed security groups have cross-references causing DependencyViolation
            self._cleanup_security_groups(vpc_id)

            # CRITICAL FIX: Release all orphaned Elastic IPs in the VPC
            # Previous cleanup may have left orphaned EIPs not associated with NAT gateways
            self._cleanup_orphaned_elastic_ips(vpc_id)

            # Final comprehensive cleanup of remaining route tables
            try:
                # Re-fetch all route tables one more time to ensure we clean up everything
                rt_response_final = exponential_retry(
                    self._ec2_client.describe_route_tables,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
                )

                for route_table in rt_response_final["RouteTables"]:
                    rt_id = route_table["RouteTableId"]

                    # Skip main route table (AWS doesn't allow deletion of main route table)
                    is_main = any(assoc.get("Main", False) for assoc in route_table.get("Associations", []))
                    if is_main:
                        continue

                    try:
                        # First disassociate any remaining associations
                        for association in route_table.get("Associations", []):
                            if not association.get("Main", False) and association.get("SubnetId"):
                                try:
                                    exponential_retry(
                                        self._ec2_client.disassociate_route_table,
                                        {"RequestLimitExceeded"},
                                        AssociationId=association["RouteTableAssociationId"],
                                    )
                                    module_logger.info(f"Final disassociation: route table {rt_id} from subnet {association['SubnetId']}")
                                except ClientError as e:
                                    error_code = e.response.get("Error", {}).get("Code", "")
                                    if error_code not in ["InvalidAssociationID.NotFound"]:
                                        module_logger.warning(f"Failed final disassociation of route table {rt_id}: {e}")

                        # Then delete the route table
                        exponential_retry(
                            self._ec2_client.delete_route_table, {"RequestLimitExceeded", "DependencyViolation"}, RouteTableId=rt_id
                        )
                        module_logger.info(f"Final cleanup: deleted route table {rt_id}")
                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code", "")
                        if error_code in ["InvalidRouteTableID.NotFound"]:
                            module_logger.info(f"Route table {rt_id} already deleted")
                        elif error_code == "DependencyViolation":
                            module_logger.warning(f"Cannot delete route table {rt_id}, has remaining dependencies: {e}")
                        else:
                            module_logger.warning(f"Failed to delete route table {rt_id}: {e}")

            except Exception as e:
                module_logger.warning(f"Could not perform final comprehensive route table cleanup: {e}")

            # Delete custom Network ACLs (after all associations are cleaned up)
            try:
                acl_response = exponential_retry(
                    self._ec2_client.describe_network_acls,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[
                        {"Name": "vpc-id", "Values": [vpc_id]},
                        {"Name": "tag:IntelliFlow", "Values": ["*"]},
                        {"Name": "default", "Values": ["false"]},  # Only custom ACLs
                    ],
                )

                for acl in acl_response["NetworkAcls"]:
                    acl_id = acl["NetworkAclId"]
                    try:
                        exponential_retry(
                            self._ec2_client.delete_network_acl, {"RequestLimitExceeded", "DependencyViolation"}, NetworkAclId=acl_id
                        )
                        module_logger.info(f"Deleted custom Network ACL {acl_id}")
                    except ClientError as e:
                        error_code = e.response.get("Error", {}).get("Code", "")
                        if error_code in ["InvalidNetworkAclID.NotFound"]:
                            module_logger.info(f"Network ACL {acl_id} already deleted")
                        elif error_code == "DependencyViolation":
                            module_logger.warning(f"Cannot delete Network ACL {acl_id} due to dependencies: {e}")
                        else:
                            module_logger.warning(f"Failed to delete Network ACL {acl_id}: {e}")
            except Exception as e:
                module_logger.warning(f"Could not retrieve or delete custom Network ACLs: {e}")

            # Delete VPC (should work now that all dependencies are cleaned up)
            try:
                exponential_retry(self._ec2_client.delete_vpc, {"RequestLimitExceeded", "DependencyViolation"}, VpcId=vpc_id)
                module_logger.info(f"Deleted VPC {vpc_id}")
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code in ["InvalidVpcID.NotFound"]:
                    module_logger.info(f"VPC {vpc_id} already deleted")
                elif error_code == "DependencyViolation":
                    module_logger.error(f"Cannot delete VPC {vpc_id} due to remaining dependencies: {e}")
                    # Log remaining resources for debugging
                    self._log_remaining_vpc_dependencies(vpc_id)
                else:
                    module_logger.error(f"Failed to delete VPC {vpc_id}: {e}")

        except ClientError as e:
            module_logger.error(f"Failed to cleanup VPC infrastructure for {vpc_id}: {e}")
            raise
        except Exception as e:
            module_logger.error(f"Unexpected error during VPC cleanup for {vpc_id}: {e}")
            raise

    def _wait_for_nat_gateways_deleted(self, nat_gateway_ids: List[str]) -> None:
        """Wait for NAT gateways to be deleted."""
        module_logger.info("Waiting for NAT gateways to be deleted...")

        max_wait_time = 300  # 5 minutes
        start_time = time.time()

        while time.time() - start_time < max_wait_time:
            try:
                response = exponential_retry(
                    self._ec2_client.describe_nat_gateways, {"RequestLimitExceeded"}, NatGatewayIds=nat_gateway_ids
                )

                all_deleted = all(nat["State"] in ["deleted", "deleting"] for nat in response["NatGateways"])
                if all_deleted:
                    module_logger.info("All NAT gateways have been deleted")
                    return

                time.sleep(15)

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code in ["InvalidNatGatewayID.NotFound", "NatGatewayNotFound"]:
                    # NAT gateways already deleted
                    module_logger.info("NAT gateways already deleted")
                    return
                module_logger.warning(f"Error checking NAT gateway deletion status: {e}")
                time.sleep(15)
            except Exception as e:
                module_logger.error(f"Unexpected error checking NAT gateway deletion status: {e}")
                raise

        module_logger.warning("Timeout waiting for NAT gateways to be deleted")

    def _log_remaining_vpc_dependencies(self, vpc_id: str) -> None:
        """Log remaining VPC dependencies for debugging DependencyViolation errors."""
        try:
            module_logger.error(f"Logging remaining dependencies for VPC {vpc_id} to help debug DependencyViolation:")

            # Check for remaining security groups
            try:
                sg_response = exponential_retry(
                    self._ec2_client.describe_security_groups,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
                )
                remaining_sgs = [f"{sg['GroupId']} ({sg['GroupName']})" for sg in sg_response["SecurityGroups"]]
                if remaining_sgs:
                    module_logger.error(f"  Remaining security groups: {remaining_sgs}")
            except Exception as e:
                module_logger.error(f"  Could not check security groups: {e}")

            # Check for remaining network interfaces
            try:
                eni_response = exponential_retry(
                    self._ec2_client.describe_network_interfaces,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
                )
                remaining_enis = [
                    f"{eni['NetworkInterfaceId']} (status: {eni['Status']}, type: {eni.get('InterfaceType', 'unknown')})"
                    for eni in eni_response["NetworkInterfaces"]
                ]
                if remaining_enis:
                    module_logger.error(f"  Remaining network interfaces: {remaining_enis}")
            except Exception as e:
                module_logger.error(f"  Could not check network interfaces: {e}")

            # Check for remaining route tables
            try:
                rt_response = exponential_retry(
                    self._ec2_client.describe_route_tables,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
                )
                remaining_rts = [
                    f"{rt['RouteTableId']} (main: {any(assoc.get('Main', False) for assoc in rt.get('Associations', []))})"
                    for rt in rt_response["RouteTables"]
                ]
                if remaining_rts:
                    module_logger.error(f"  Remaining route tables: {remaining_rts}")
            except Exception as e:
                module_logger.error(f"  Could not check route tables: {e}")

            # Check for remaining subnets
            try:
                subnet_response = exponential_retry(
                    self._ec2_client.describe_subnets,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
                )
                remaining_subnets = [f"{subnet['SubnetId']} (state: {subnet['State']})" for subnet in subnet_response["Subnets"]]
                if remaining_subnets:
                    module_logger.error(f"  Remaining subnets: {remaining_subnets}")
            except Exception as e:
                module_logger.error(f"  Could not check subnets: {e}")

        except Exception as e:
            module_logger.error(f"Error logging VPC dependencies: {e}")

    def find_orphaned_intelliflow_vpcs(self) -> List[Dict[str, str]]:
        """Find potentially orphaned IntelliFlow VPCs that can be cleaned up."""
        try:
            response = exponential_retry(
                self._ec2_client.describe_vpcs,
                {"RequestLimitExceeded", "ServiceUnavailable"},
                Filters=[
                    {"Name": "tag:Name", "Values": ["IntelliFlow-*"]},
                    {"Name": "is-default", "Values": ["false"]},  # Never touch default VPC
                ],
            )

            orphaned_vpcs = []
            for vpc in response["Vpcs"]:
                vpc_id = vpc["VpcId"]
                vpc_tags = {tag["Key"]: tag["Value"] for tag in vpc.get("Tags", [])}
                vpc_name = vpc_tags.get("Name", "")
                state = vpc["State"]

                # Check if this VPC has any running EMR clusters
                try:
                    # Get subnets in this VPC
                    subnet_response = exponential_retry(
                        self._ec2_client.describe_subnets,
                        {"RequestLimitExceeded", "ServiceUnavailable"},
                        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}],
                    )
                    subnet_ids = [subnet["SubnetId"] for subnet in subnet_response["Subnets"]]

                    # Check if any subnets have running EMR clusters
                    # This would require EMR client, so for now just list the VPC info
                    orphaned_vpcs.append({"vpc_id": vpc_id, "vpc_name": vpc_name, "state": state, "subnet_count": len(subnet_ids)})

                except ClientError as e:
                    module_logger.warning(f"Could not check subnets for VPC {vpc_id}: {e}")
                    orphaned_vpcs.append({"vpc_id": vpc_id, "vpc_name": vpc_name, "state": state, "subnet_count": "unknown"})

            return orphaned_vpcs

        except ClientError as e:
            module_logger.error(f"Error finding orphaned VPCs: {e}")
            return []

    def validate_and_fix_vpc_connectivity(
        self,
        vpc_id: str,
        private_subnet_ids: List[str],
        public_subnet_ids: List[str],
        nat_gateway_ids: List[str],
        igw_id: Optional[str],
        unique_context_id: str,
    ) -> None:
        """
        Validate VPC connectivity and fix any issues found.

        This method is called during every activation to ensure that:
        1. Private subnets have proper NAT gateway routes
        2. Public subnets have Internet Gateway routes
        3. S3 VPC endpoints are configured
        4. Network ACLs allow internet access

        IDEMPOTENT: Safe to call multiple times - only fixes actual issues.

        Args:
            vpc_id: VPC ID to validate
            private_subnet_ids: List of private subnet IDs
            public_subnet_ids: List of public subnet IDs
            nat_gateway_ids: List of NAT gateway IDs
            igw_id: Internet Gateway ID (optional)
            unique_context_id: Unique context ID for resource naming
        """
        if not vpc_id or not private_subnet_ids:
            module_logger.info("No VPC infrastructure to validate")
            return

        module_logger.info(
            f"Validating VPC connectivity for {len(private_subnet_ids)} private and {len(public_subnet_ids)} public subnets..."
        )

        # CRITICAL FIX: First sweep and clean up any stale NAT gateway routes
        # This fixes the bug where route tables have routes to non-existent NAT gateways
        try:
            stale_routes_fixed = self._sweep_and_fix_stale_nat_routes(vpc_id, unique_context_id)
            if stale_routes_fixed > 0:
                module_logger.info(
                    f"✓ RECOVERY: Cleaned up {stale_routes_fixed} stale NAT gateway routes "
                    f"(routes pointing to deleted/non-existent NAT gateways)"
                )
        except Exception as sweep_error:
            module_logger.warning(f"Could not sweep for stale NAT routes (will continue with validation): {sweep_error}")

        # CRITICAL: Validate NAT gateways have proper EIP associations and public subnet connectivity
        # This catches the case where NAT gateways were created without IGW routes in public subnets
        nat_gateway_issues = []
        if nat_gateway_ids:
            module_logger.info("Checking NAT gateway health and EIP associations...")
            try:
                nat_response = exponential_retry(
                    self._ec2_client.describe_nat_gateways,
                    {"RequestLimitExceeded", "ServiceUnavailable"},
                    NatGatewayIds=nat_gateway_ids,
                )

                for nat in nat_response["NatGateways"]:
                    nat_id = nat["NatGatewayId"]
                    nat_state = nat["State"]
                    nat_subnet_id = nat["SubnetId"]

                    # Check if NAT has EIP association
                    nat_addresses = nat.get("NatGatewayAddresses", [])
                    has_eip = any(addr.get("AllocationId") for addr in nat_addresses)

                    if nat_state == "failed":
                        nat_gateway_issues.append(
                            {"nat_id": nat_id, "subnet_id": nat_subnet_id, "issue": "NAT gateway in FAILED state", "has_eip": has_eip}
                        )
                        module_logger.error(
                            f"NAT gateway {nat_id} is in FAILED state (likely due to missing IGW route in public subnet {nat_subnet_id})"
                        )
                    elif not has_eip:
                        nat_gateway_issues.append(
                            {"nat_id": nat_id, "subnet_id": nat_subnet_id, "issue": "No EIP associated", "has_eip": False}
                        )
                        module_logger.error(f"NAT gateway {nat_id} has no Elastic IP associated!")
                    elif nat_state == "pending":
                        module_logger.info(f"NAT gateway {nat_id} is still pending (may become available)")
                    elif nat_state == "available":
                        module_logger.info(f"✓ NAT gateway {nat_id} is healthy with EIP")

            except Exception as nat_check_error:
                module_logger.error(f"Could not validate NAT gateway health: {nat_check_error}")

        if nat_gateway_issues:
            module_logger.error(
                f"CRITICAL: {len(nat_gateway_issues)} NAT gateways have issues. "
                f"This is typically caused by public subnets lacking Internet Gateway routes when NATs were created. "
                f"Root cause: Public subnets must have IGW routes BEFORE creating NAT gateways."
            )

            # First ensure public subnets have IGW routes to prevent recreating the problem
            if public_subnet_ids and igw_id:
                module_logger.info("Ensuring public subnets have IGW routes before fixing NAT gateways...")
                try:
                    self._ensure_public_subnets_have_internet_gateway_routes(vpc_id, igw_id, public_subnet_ids, unique_context_id)
                    module_logger.info("✓ Public subnets now have proper IGW routes")
                except Exception as igw_error:
                    module_logger.error(f"Failed to add IGW routes to public subnets: {igw_error}")
                    raise RuntimeError(
                        "Cannot fix NAT gateways without proper IGW routes in public subnets. "
                        "Please manually add 0.0.0.0/0 -> IGW routes to public subnet route tables."
                    )

            module_logger.warning(
                "NAT gateway issues detected. These NAT gateways were likely created without proper "
                "public subnet connectivity. They need to be deleted and recreated. "
                "Please terminate this app and reactivate - the fix will prevent this issue going forward."
            )

        # Validate private subnet routing
        routing_issues = self._validate_private_subnet_routing(vpc_id, private_subnet_ids)

        if routing_issues:
            module_logger.warning(
                f"CONNECTIVITY ISSUES DETECTED: {len(routing_issues)} private subnets have routing problems. "
                f"Applying automatic fixes..."
            )

            fixed_count = 0
            for i, issue in enumerate(routing_issues):
                subnet_id = issue["subnet_id"]
                issue_desc = issue["issue"]

                module_logger.info(f"Fixing subnet {subnet_id}: {issue_desc}")

                # Use corresponding NAT gateway (round-robin if not enough)
                if nat_gateway_ids:
                    nat_gw_id = nat_gateway_ids[i % len(nat_gateway_ids)]

                    try:
                        new_rt_id = self._fix_private_subnet_routing(vpc_id, subnet_id, nat_gw_id, unique_context_id, i)

                        # Add S3 endpoint to the new route table
                        try:
                            existing_endpoints = self.get_vpc_s3_endpoints(vpc_id)
                            if existing_endpoints:
                                exponential_retry(
                                    self._ec2.meta.client.modify_vpc_endpoint,
                                    {"RequestLimitExceeded", "ServiceUnavailable"},
                                    VpcEndpointId=existing_endpoints[0],
                                    AddRouteTableIds=[new_rt_id],
                                )
                        except Exception as s3_error:
                            module_logger.warning(f"Could not add S3 endpoint to fixed route table: {s3_error}")

                        # Apply Network ACL
                        try:
                            self.ensure_private_subnets_internet_access(vpc_id, [subnet_id], unique_context_id)
                        except Exception as acl_error:
                            module_logger.warning(f"Could not apply Network ACL to fixed subnet: {acl_error}")

                        fixed_count += 1
                        module_logger.info(f"✓ Fixed connectivity for subnet {subnet_id}")

                    except Exception as fix_error:
                        module_logger.error(f"✗ Failed to fix subnet {subnet_id}: {fix_error}")
                else:
                    module_logger.error(f"Cannot fix subnet {subnet_id} - no NAT gateways available")

            if fixed_count > 0:
                module_logger.info(f"Successfully fixed connectivity for {fixed_count}/{len(routing_issues)} subnets")
            else:
                module_logger.error("Failed to fix any subnet connectivity issues - EMR clusters may not have internet access")
        else:
            module_logger.info("✓ All private subnets have proper NAT gateway routing")

        # CRITICAL: Validate and fix public subnet Internet Gateway routes
        # This is THE most common cause of "Connection timed out" errors
        # Symptoms: NAT has public IP but cannot forward traffic
        if public_subnet_ids and igw_id:
            module_logger.info("Validating public subnet Internet Gateway routes...")

            missing_igw_routes = []
            for public_subnet_id in public_subnet_ids:
                try:
                    # Check if this public subnet's route table has IGW route
                    rt_response = exponential_retry(
                        self._ec2_client.describe_route_tables,
                        {"RequestLimitExceeded", "ServiceUnavailable"},
                        Filters=[{"Name": "association.subnet-id", "Values": [public_subnet_id]}, {"Name": "vpc-id", "Values": [vpc_id]}],
                    )

                    has_igw_route = False
                    if rt_response["RouteTables"]:
                        route_table = rt_response["RouteTables"][0]
                        rt_id = route_table["RouteTableId"]

                        # Check routes for IGW
                        for route in route_table.get("Routes", []):
                            if route.get("DestinationCidrBlock") == "0.0.0.0/0" and route.get("GatewayId", "").startswith("igw-"):
                                has_igw_route = True
                                break

                        if not has_igw_route:
                            missing_igw_routes.append(
                                {"subnet_id": public_subnet_id, "route_table_id": rt_id, "issue": "Missing 0.0.0.0/0 → IGW route"}
                            )
                    else:
                        missing_igw_routes.append(
                            {"subnet_id": public_subnet_id, "route_table_id": None, "issue": "No route table associated (using main RT)"}
                        )

                except Exception as check_error:
                    module_logger.warning(f"Could not validate IGW route for subnet {public_subnet_id}: {check_error}")

            if missing_igw_routes:
                module_logger.error(
                    f"CRITICAL ISSUE DETECTED: {len(missing_igw_routes)} public subnets missing Internet Gateway routes!\n"
                    f"This causes 'Connection timed out' errors in EMR clusters.\n"
                    f"Root cause: Public subnets created during AZ fix or idempotent operations didn't get IGW routes.\n"
                    f"Applying automatic fix..."
                )

                for issue in missing_igw_routes:
                    subnet_id = issue["subnet_id"]
                    module_logger.info(f"Fixing public subnet {subnet_id}: {issue['issue']}")

                # Fix all public subnets at once
                try:
                    self._ensure_public_subnets_have_internet_gateway_routes(vpc_id, igw_id, public_subnet_ids, unique_context_id)
                    module_logger.info(f"✓ Fixed IGW routes for {len(missing_igw_routes)} public subnets")

                    # Log clear resolution message
                    module_logger.info(
                        "RESOLUTION: Public subnets now have Internet Gateway routes. "
                        "NAT gateways can now forward traffic properly. "
                        "EMR clusters should connect to internet successfully."
                    )
                except Exception as fix_error:
                    module_logger.error(f"Failed to fix public subnet IGW routes: {fix_error}")
                    raise RuntimeError(
                        f"Cannot fix public subnet IGW routes: {fix_error}. "
                        f"Manual fix required: Add route 0.0.0.0/0 → {igw_id} to public subnet route tables."
                    )
            else:
                module_logger.info("✓ All public subnets have proper Internet Gateway routes")

        # Ensure S3 VPC endpoint exists and is properly configured
        try:
            s3_endpoint_id = self.ensure_s3_vpc_endpoint(vpc_id, unique_context_id)
            if s3_endpoint_id:
                module_logger.info(f"✓ S3 VPC endpoint configured: {s3_endpoint_id}")
        except Exception as s3_error:
            module_logger.warning(f"Could not ensure S3 VPC endpoint: {s3_error}")

        # Ensure Network ACL is properly configured
        try:
            self.ensure_private_subnets_internet_access(vpc_id, private_subnet_ids, unique_context_id)
            module_logger.info("✓ Network ACL configured for internet access")
        except Exception as acl_error:
            module_logger.warning(f"Could not ensure Network ACL configuration: {acl_error}")

        module_logger.info("VPC connectivity validation and fixes completed")


class SubnetConfig:
    """Configuration for subnet setup."""

    def __init__(self, subnet_count: int = 3):
        self.subnet_count = subnet_count

    def __eq__(self, other):
        if isinstance(other, SubnetConfig):
            return self.subnet_count == other.subnet_count
        return False

    def __repr__(self):
        return f"SubnetConfig(subnet_count={self.subnet_count})"
