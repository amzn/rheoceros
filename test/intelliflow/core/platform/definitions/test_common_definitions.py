# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from unittest.mock import MagicMock, patch
import pytest

from intelliflow.core.platform.definitions.aws.common import (
    AWS_INLINE_POLICY_SIZE_LIMIT,
    AWS_MANAGED_POLICY_SIZE_LIMIT,
    AWS_MAX_MANAGED_POLICIES_PER_ROLE,
    _build_managed_policy_arn,
    _build_managed_policy_name,
    _check_statement_field_equality,
    _estimate_policy_size,
    _is_role_managed_policy_name,
    _parse_managed_policy_base_name,
    _partition_statements_by_size,
    delete_managed_policy,
    delete_role,
    normalize_policy_arn,
    put_inlined_policy,
    put_managed_policy,
    segregate_trusted_entities,
    update_role_trust_policy,
)


class TestPlatformCommonDefinitions:
    def test_check_statement_field_equality(self):
        assert not _check_statement_field_equality({"Principal": {"Service": "foo"}}, {}, "Principal")
        assert not _check_statement_field_equality({"Principal": {"Service": "foo"}}, {"Principal": {"AWS": "foo"}}, "Principal")
        assert _check_statement_field_equality({"Principal": {"Service": "foo"}}, {"Principal": {"Service": "foo"}}, "Principal")
        assert not _check_statement_field_equality({"Principal": {"Service": "foo"}}, {"Principal": {"Service": "foo"}}, "WRONG_FIELD")
        assert _check_statement_field_equality({"Principal": {"Service": ["foo"]}}, {"Principal": {"Service": "foo"}}, "Principal")
        assert not _check_statement_field_equality(
            {"Principal": {"Service": ["foo", "bar"]}}, {"Principal": {"Service": "foo"}}, "Principal"
        )

    def test_update_role_trust_policy_with_new_services(self):
        """Test adding new AWS services to trust policy"""
        mock_session = MagicMock()
        mock_iam = MagicMock()
        mock_session.client.return_value = mock_iam

        # Mock existing trust policy with only AWS entities
        existing_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::123456789012:user/test-user"}, "Action": "sts:AssumeRole"}
            ],
        }

        mock_iam.get_role.return_value = {"Role": {"AssumeRolePolicyDocument": existing_trust_policy}}

        # Test adding new services
        new_services = {"sagemaker.amazonaws.com", "lambda.amazonaws.com"}

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, **kwargs: func(**kwargs)

            update_role_trust_policy("test-role", mock_session, "us-east-1", new_aws_services=new_services)

            # Verify that update_assume_role_policy was called
            mock_iam.update_assume_role_policy.assert_called_once()
            call_args = mock_iam.update_assume_role_policy.call_args

            updated_policy = json.loads(call_args[1]["PolicyDocument"])

            # Should have original AWS statement plus new Service statement
            assert len(updated_policy["Statement"]) == 2

            # Check that new service statement was added
            service_statement = None
            for stmt in updated_policy["Statement"]:
                if "Service" in stmt["Principal"]:
                    service_statement = stmt
                    break

            assert service_statement is not None
            assert service_statement["Effect"] == "Allow"
            assert service_statement["Action"] == "sts:AssumeRole"
            assert set(service_statement["Principal"]["Service"]) == new_services

    def test_update_role_trust_policy_remove_services(self):
        """Test removing AWS services from trust policy"""
        mock_session = MagicMock()
        mock_iam = MagicMock()
        mock_session.client.return_value = mock_iam

        # Mock existing trust policy with services
        existing_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": ["sagemaker.amazonaws.com", "lambda.amazonaws.com", "glue.amazonaws.com"]},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

        mock_iam.get_role.return_value = {"Role": {"AssumeRolePolicyDocument": existing_trust_policy}}

        # Test removing one service
        removed_services = {"lambda.amazonaws.com"}

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, **kwargs: func(**kwargs)

            update_role_trust_policy("test-role", mock_session, "us-east-1", removed_aws_services=removed_services)

            # Verify that update_assume_role_policy was called
            mock_iam.update_assume_role_policy.assert_called_once()
            call_args = mock_iam.update_assume_role_policy.call_args

            updated_policy = json.loads(call_args[1]["PolicyDocument"])

            # Should still have one statement with remaining services
            assert len(updated_policy["Statement"]) == 1
            remaining_services = set(updated_policy["Statement"][0]["Principal"]["Service"])
            assert remaining_services == {"sagemaker.amazonaws.com", "glue.amazonaws.com"}

    def test_update_role_trust_policy_mixed_principals(self):
        """Test handling both AWS entities and services in the same policy"""
        mock_session = MagicMock()
        mock_iam = MagicMock()
        mock_session.client.return_value = mock_iam

        # Mock existing trust policy with both AWS and Service principals
        existing_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::123456789012:user/test-user"}, "Action": "sts:AssumeRole"},
                {"Effect": "Allow", "Principal": {"Service": "sagemaker.amazonaws.com"}, "Action": "sts:AssumeRole"},
            ],
        }

        mock_iam.get_role.return_value = {"Role": {"AssumeRolePolicyDocument": existing_trust_policy}}

        # Test adding new services and entities
        new_services = {"lambda.amazonaws.com"}
        new_entities = {"arn:aws:iam::123456789012:user/another-user"}

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, **kwargs: func(**kwargs)

            update_role_trust_policy("test-role", mock_session, "us-east-1", new_aws_entities=new_entities, new_aws_services=new_services)

            # Verify that update_assume_role_policy was called
            mock_iam.update_assume_role_policy.assert_called_once()
            call_args = mock_iam.update_assume_role_policy.call_args

            updated_policy = json.loads(call_args[1]["PolicyDocument"])

            # Should have statements for both AWS and Service principals
            aws_statements = [stmt for stmt in updated_policy["Statement"] if "AWS" in stmt["Principal"]]
            service_statements = [stmt for stmt in updated_policy["Statement"] if "Service" in stmt["Principal"]]

            assert len(aws_statements) >= 1
            assert len(service_statements) >= 1

            # Check that new entities and services were added
            all_aws_principals = set()
            for stmt in aws_statements:
                aws_principals = stmt["Principal"]["AWS"]
                if isinstance(aws_principals, list):
                    all_aws_principals.update(aws_principals)
                else:
                    all_aws_principals.add(aws_principals)

            all_service_principals = set()
            for stmt in service_statements:
                service_principals = stmt["Principal"]["Service"]
                if isinstance(service_principals, list):
                    all_service_principals.update(service_principals)
                else:
                    all_service_principals.add(service_principals)

            assert "arn:aws:iam::123456789012:user/another-user" in all_aws_principals
            assert "lambda.amazonaws.com" in all_service_principals
            assert "sagemaker.amazonaws.com" in all_service_principals

    def test_update_role_trust_policy_deduplication(self):
        """Test service deduplication logic"""
        mock_session = MagicMock()
        mock_iam = MagicMock()
        mock_session.client.return_value = mock_iam

        # Mock existing trust policy with existing services
        existing_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": {"Service": "sagemaker.amazonaws.com"}, "Action": "sts:AssumeRole"}],
        }

        mock_iam.get_role.return_value = {"Role": {"AssumeRolePolicyDocument": existing_trust_policy}}

        # Test adding service that already exists (should be deduplicated)
        new_services = {"sagemaker.amazonaws.com", "lambda.amazonaws.com"}

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, **kwargs: func(**kwargs)

            update_role_trust_policy("test-role", mock_session, "us-east-1", new_aws_services=new_services)

            # Verify that update_assume_role_policy was called
            mock_iam.update_assume_role_policy.assert_called_once()
            call_args = mock_iam.update_assume_role_policy.call_args

            updated_policy = json.loads(call_args[1]["PolicyDocument"])

            # Should have only one statement with both services
            assert len(updated_policy["Statement"]) == 1
            service_principals = set(updated_policy["Statement"][0]["Principal"]["Service"])
            assert service_principals == {"sagemaker.amazonaws.com", "lambda.amazonaws.com"}

    def test_update_role_trust_policy_backward_compatibility(self):
        """Test that existing calls without new parameters still work"""
        mock_session = MagicMock()
        mock_iam = MagicMock()
        mock_session.client.return_value = mock_iam

        # Mock existing trust policy
        existing_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::123456789012:user/test-user"}, "Action": "sts:AssumeRole"}
            ],
        }

        mock_iam.get_role.return_value = {"Role": {"AssumeRolePolicyDocument": existing_trust_policy}}

        # Test calling with only original parameters (backward compatibility)
        new_entities = {"arn:aws:iam::123456789012:user/another-user"}

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, **kwargs: func(**kwargs)

            # This should work without any service-related parameters
            update_role_trust_policy("test-role", mock_session, "us-east-1", new_aws_entities=new_entities)

            # Verify that update_assume_role_policy was called
            mock_iam.update_assume_role_policy.assert_called_once()
            call_args = mock_iam.update_assume_role_policy.call_args

            updated_policy = json.loads(call_args[1]["PolicyDocument"])

            # Should have AWS entities but no service statements
            assert len(updated_policy["Statement"]) == 1
            assert "AWS" in updated_policy["Statement"][0]["Principal"]
            assert "Service" not in updated_policy["Statement"][0]["Principal"]

    def test_update_role_trust_policy_remove_all_services(self):
        """Test removing all services from a statement"""
        mock_session = MagicMock()
        mock_iam = MagicMock()
        mock_session.client.return_value = mock_iam

        # Mock existing trust policy with only services
        existing_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": ["sagemaker.amazonaws.com", "lambda.amazonaws.com"]},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

        mock_iam.get_role.return_value = {"Role": {"AssumeRolePolicyDocument": existing_trust_policy}}

        # Test removing all services
        removed_services = {"sagemaker.amazonaws.com", "lambda.amazonaws.com"}

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, **kwargs: func(**kwargs)

            update_role_trust_policy("test-role", mock_session, "us-east-1", removed_aws_services=removed_services)

            # Verify that update_assume_role_policy was called
            mock_iam.update_assume_role_policy.assert_called_once()
            call_args = mock_iam.update_assume_role_policy.call_args

            updated_policy = json.loads(call_args[1]["PolicyDocument"])

            # Should have no statements left (empty policy)
            assert len(updated_policy["Statement"]) == 0
        assert not _check_statement_field_equality({"Effect": "Allow"}, {"Effect": "Deny"}, "Principal")
        assert _check_statement_field_equality({"Effect": "Allow"}, {"Effect": "Allow"}, "Effect")
        assert _check_statement_field_equality({"Effect": "Allow"}, {"Effect": ["Allow"]}, "Effect")
        assert not _check_statement_field_equality({"Effect": "Allow"}, {"Effect": ["Allow"]}, "Principal")
        assert _check_statement_field_equality({"Resource": "resource1"}, {"Resource": ["resource1"]}, "Resource")
        assert not _check_statement_field_equality({"Resource": "resource1"}, {"Resource": ["resource1", "resource2"]}, "Resource")

        assert _check_statement_field_equality({"Resource": "resource1"}, {"Resource": ["resource1", "resource1"]}, "Resource")

    def test_normalize_policy_arn(self):
        assert (
            normalize_policy_arn("service-role/AmazonElasticMapReduceEditorsRole")
            == "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceEditorsRole"
        )
        assert (
            normalize_policy_arn("arn:aws:iam::12345678910:policy/CloudRanger/InfoSecHostMonitoringPolicy-DO-NOT-DELETE")
            == "arn:aws:iam::12345678910:policy/CloudRanger/InfoSecHostMonitoringPolicy-DO-NOT-DELETE"
        )

    def test_segregate_trusted_entities_mixed_list(self):
        """Test segregating a mixed list of AWS services and entities"""
        mixed_entities = [
            "lambda.amazonaws.com",
            "arn:aws:iam::123456789012:role/MyRole",
            "sagemaker.amazonaws.com",
            "arn:aws:iam::123456789012:user/MyUser",
            "glue.amazonaws.com",
        ]

        services, entities = segregate_trusted_entities(mixed_entities)

        expected_services = ["lambda.amazonaws.com", "sagemaker.amazonaws.com", "glue.amazonaws.com"]
        expected_entities = ["arn:aws:iam::123456789012:role/MyRole", "arn:aws:iam::123456789012:user/MyUser"]

        assert services == expected_services
        assert entities == expected_entities

    def test_segregate_trusted_entities_only_services(self):
        """Test segregating a list with only AWS services"""
        service_entities = ["lambda.amazonaws.com", "sagemaker.amazonaws.com", "glue.amazonaws.com", "ec2.amazonaws.com"]

        services, entities = segregate_trusted_entities(service_entities)

        assert services == service_entities
        assert entities == []

    def test_segregate_trusted_entities_only_entities(self):
        """Test segregating a list with only AWS entities"""
        aws_entities = [
            "arn:aws:iam::123456789012:role/MyRole",
            "arn:aws:iam::123456789012:user/MyUser",
            "arn:aws:iam::123456789012:root",
            "arn:aws:sts::123456789012:assumed-role/MyRole/session-name",
        ]

        services, entities = segregate_trusted_entities(aws_entities)

        assert services == []
        assert entities == aws_entities

    def test_segregate_trusted_entities_empty_list(self):
        """Test segregating an empty list"""
        services, entities = segregate_trusted_entities([])

        assert services == []
        assert entities == []

    def test_segregate_trusted_entities_edge_cases(self):
        """Test segregating edge cases and unusual formats"""
        edge_case_entities = [
            "custom-service.amazonaws.com",  # Custom service
            "123456789012",  # Account ID only
            "arn:aws:iam::*:root",  # Wildcard entity
            "events.amazonaws.com",  # Another AWS service
            "AIDACKCEVSQ6C2EXAMPLE",  # IAM unique ID
        ]

        services, entities = segregate_trusted_entities(edge_case_entities)

        expected_services = ["custom-service.amazonaws.com", "events.amazonaws.com"]
        expected_entities = ["123456789012", "arn:aws:iam::*:root", "AIDACKCEVSQ6C2EXAMPLE"]

        assert services == expected_services
        assert entities == expected_entities

    def test_managed_policy_naming_functions(self):
        """Test role-specific managed policy naming functions"""
        role_name = "test-app-us-east-1-IntelliFlowDevRole"
        policy_name = "DevelopmentPolicy"
        account_id = "123456789012"

        # Test single policy name
        single_name = _build_managed_policy_name(role_name, policy_name)
        expected_single = f"{role_name}-MP-{policy_name}"
        assert single_name == expected_single

        # Test partitioned policy name
        part_name = _build_managed_policy_name(role_name, policy_name, 2)
        expected_part = f"{role_name}-MP-{policy_name}-Part-2"
        assert part_name == expected_part

        # Test ARN building
        single_arn = _build_managed_policy_arn(account_id, role_name, policy_name)
        expected_arn = f"arn:aws:iam::{account_id}:policy/{expected_single}"
        assert single_arn == expected_arn

        part_arn = _build_managed_policy_arn(account_id, role_name, policy_name, 3)
        expected_part_arn = f"arn:aws:iam::{account_id}:policy/{role_name}-MP-{policy_name}-Part-3"
        assert part_arn == expected_part_arn

        # Test role ownership checking
        assert _is_role_managed_policy_name(role_name, single_name)
        assert _is_role_managed_policy_name(role_name, part_name)

        other_role = "other-app-us-east-1-IntelliFlowDevRole"
        other_policy = _build_managed_policy_name(other_role, policy_name)
        assert not _is_role_managed_policy_name(role_name, other_policy)

        # Test base name parsing
        parsed_base = _parse_managed_policy_base_name(role_name, single_name)
        assert parsed_base == policy_name

        parsed_base_part = _parse_managed_policy_base_name(role_name, part_name)
        assert parsed_base_part == policy_name

        # Test parsing returns None for other role's policies
        parsed_other = _parse_managed_policy_base_name(role_name, other_policy)
        assert parsed_other is None

    def test_policy_size_estimation(self):
        """Test policy size estimation function"""
        # Small policy
        small_statements = [{"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::bucket/*"]}]
        small_size = _estimate_policy_size(small_statements)
        assert small_size < AWS_INLINE_POLICY_SIZE_LIMIT

        # Large policy with many resources
        large_statements = []
        for i in range(100):
            large_statements.append(
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    "Resource": [f"arn:aws:s3:::bucket{i}/*", f"arn:aws:s3:::bucket{i}"],
                }
            )

        large_size = _estimate_policy_size(large_statements)
        assert large_size > AWS_INLINE_POLICY_SIZE_LIMIT

    def test_policy_partitioning(self):
        """Test policy statement partitioning logic"""
        # Create statements that need partitioning
        statements = []
        for i in range(10):
            statements.append(
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject"],
                    "Resource": [f"arn:aws:s3:::very-long-bucket-name-{i}/*" for _ in range(20)],
                }
            )

        partitions = _partition_statements_by_size(statements, AWS_MANAGED_POLICY_SIZE_LIMIT)

        # Verify each partition is within size limits
        for partition in partitions:
            partition_size = _estimate_policy_size(partition)
            assert partition_size <= AWS_MANAGED_POLICY_SIZE_LIMIT

        # Verify all statements are included
        total_statements = sum(len(partition) for partition in partitions)
        assert total_statements == len(statements)

        # Test single oversized statement raises error
        oversized_statement = {
            "Effect": "Allow",
            "Action": ["s3:*"],
            "Resource": [f"arn:aws:s3:::bucket{i}/*" for i in range(1000)],  # Very large
        }

        with pytest.raises(ValueError, match="Single policy statement exceeds"):
            _partition_statements_by_size([oversized_statement], AWS_MANAGED_POLICY_SIZE_LIMIT)

    def test_put_inlined_policy_automatic_fallback(self):
        """Test put_inlined_policy automatically falls back to managed policies"""
        mock_session = MagicMock()
        mock_iam = MagicMock()
        mock_sts = MagicMock()
        mock_session.client.side_effect = lambda service: mock_iam if service == "iam" else mock_sts
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

        # Mock ConstructPermission-like objects
        class MockConstructPermission:
            def __init__(self, resource, action):
                self.resource = resource
                self.action = action

            def __iter__(self):
                return iter([self.resource, self.action])

        # Small policy should use inline policy
        small_permissions = {MockConstructPermission(["arn:aws:s3:::test/*"], ["s3:GetObject"])}

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, *args, **kwargs: func(*args, **kwargs)

            put_inlined_policy("test-role", "TestPolicy", small_permissions, mock_session)

            # Should call put_role_policy for small policy
            mock_iam.put_role_policy.assert_called_once()
            mock_iam.create_policy.assert_not_called()

        # Reset mocks
        mock_iam.reset_mock()

        # Large policy should use managed policies - create a policy that definitely exceeds 10240 bytes
        large_permissions = set()
        for i in range(200):  # More permissions
            # Use unique actions for each permission to prevent consolidation
            unique_actions = [f"s3:GetObject{i}", f"s3:PutObject{i}", f"s3:DeleteObject{i}"]
            large_permissions.add(
                MockConstructPermission(
                    [
                        f"arn:aws:s3:::very-long-bucket-name-that-makes-the-policy-large-{i}/*",
                        f"arn:aws:s3:::very-long-bucket-name-that-makes-the-policy-large-{i}",
                    ],
                    unique_actions,
                )
            )

        def mock_get_policy_small_not_found(*args, **kwargs):
            from botocore.exceptions import ClientError

            raise ClientError(error_response={"Error": {"Code": "NoSuchEntity", "Message": "Policy not found"}}, operation_name="GetPolicy")

        mock_iam.get_policy.side_effect = mock_get_policy_small_not_found  # Policy doesn't exist
        mock_iam.list_attached_role_policies.return_value = {"AttachedPolicies": []}

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, *args, **kwargs: func(*args, **kwargs)

            put_inlined_policy("test-role", "TestPolicy", large_permissions, mock_session)

            # Should NOT call put_role_policy for large policy
            mock_iam.put_role_policy.assert_not_called()
            # Should create managed policy instead
            mock_iam.create_policy.assert_called()

    def test_put_managed_policy_single_policy(self):
        """Test put_managed_policy with policy that fits in single managed policy"""
        mock_session = MagicMock()
        mock_iam = MagicMock()
        mock_sts = MagicMock()
        mock_session.client.side_effect = lambda service: mock_iam if service == "iam" else mock_sts
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

        class MockConstructPermission:
            def __init__(self, resource, action):
                self.resource = resource
                self.action = action

            def __iter__(self):
                return iter([self.resource, self.action])

        permissions = {MockConstructPermission(["arn:aws:s3:::test/*"], ["s3:GetObject", "s3:PutObject"])}

        def mock_get_policy_not_found(*args, **kwargs):
            from botocore.exceptions import ClientError

            raise ClientError(error_response={"Error": {"Code": "NoSuchEntity", "Message": "Policy not found"}}, operation_name="GetPolicy")

        mock_iam.get_policy.side_effect = mock_get_policy_not_found  # Policy doesn't exist

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, *args, **kwargs: func(*args, **kwargs)

            put_managed_policy("test-role", "TestPolicy", permissions, mock_session)

            # Should create single managed policy
            mock_iam.create_policy.assert_called_once()
            mock_iam.attach_role_policy.assert_called_once()

            # Verify policy name follows role-specific naming
            call_args = mock_iam.create_policy.call_args
            policy_name = call_args[1]["PolicyName"]
            assert "test-role-MP-TestPolicy" == policy_name

    def test_put_managed_policy_partitioned(self):
        """Test put_managed_policy with large policy that requires partitioning"""
        mock_session = MagicMock()
        mock_iam = MagicMock()
        mock_sts = MagicMock()
        mock_session.client.side_effect = lambda service: mock_iam if service == "iam" else mock_sts
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

        class MockConstructPermission:
            def __init__(self, resource, action):
                self.resource = resource
                self.action = action

            def __iter__(self):
                return iter([self.resource, self.action])

        # Create large policy that needs partitioning - make it much larger
        large_permissions = set()
        for i in range(150):
            # Use unique actions for each permission to prevent consolidation
            unique_actions = [f"s3:GetObject{i}", f"s3:PutObject{i}", f"s3:DeleteObject{i}", f"s3:ListBucket{i}"]
            large_permissions.add(
                MockConstructPermission(
                    [
                        f"arn:aws:s3:::very-long-bucket-name-that-makes-the-policy-really-large-{i}/*",
                        f"arn:aws:s3:::very-long-bucket-name-that-makes-the-policy-really-large-{i}",
                    ],
                    unique_actions,
                )
            )

        def mock_get_policies_not_found(*args, **kwargs):
            from botocore.exceptions import ClientError

            raise ClientError(error_response={"Error": {"Code": "NoSuchEntity", "Message": "Policy not found"}}, operation_name="GetPolicy")

        mock_iam.get_policy.side_effect = mock_get_policies_not_found  # Policies don't exist
        mock_iam.list_attached_role_policies.return_value = {"AttachedPolicies": []}

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, *args, **kwargs: func(*args, **kwargs)

            put_managed_policy("test-role", "TestPolicy", large_permissions, mock_session)

            # Should create multiple managed policies
            assert mock_iam.create_policy.call_count > 1
            assert mock_iam.attach_role_policy.call_count > 1

            # Verify partitioned policy names
            create_calls = mock_iam.create_policy.call_args_list
            for i, call in enumerate(create_calls, 1):
                policy_name = call[1]["PolicyName"]
                expected_name = f"test-role-MP-TestPolicy-Part-{i}"
                assert policy_name == expected_name

    @patch("boto3.Session")
    def test_delete_managed_policy(self, mock_session_class):
        """Test deletion of managed policies (single and partitioned)"""
        mock_session = MagicMock()
        mock_iam = MagicMock()
        mock_sts = MagicMock()
        mock_session.client.side_effect = lambda service: mock_iam if service == "iam" else mock_sts
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

        # Mock policy exists for main policy and first partition
        def mock_get_policy(*args, **kwargs):
            policy_arn = kwargs["PolicyArn"]
            if "TestPolicy-Part-1" in policy_arn or (not "Part" in policy_arn and "TestPolicy" in policy_arn):
                return {"Policy": {"Arn": policy_arn}}
            else:
                from botocore.exceptions import ClientError

                raise ClientError(
                    error_response={"Error": {"Code": "NoSuchEntity", "Message": "Policy not found"}}, operation_name="GetPolicy"
                )

        mock_iam.get_policy.side_effect = mock_get_policy
        mock_iam.list_entities_for_policy.return_value = {"PolicyRoles": [], "PolicyUsers": [], "PolicyGroups": []}
        mock_iam.list_policy_versions.return_value = {"Versions": [{"VersionId": "v1", "IsDefaultVersion": True}]}

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, *args, **kwargs: func(*args, **kwargs)

            delete_managed_policy("test-role", "TestPolicy", mock_session)

            # Should attempt to delete main policy and partitions
            assert mock_iam.delete_policy.call_count >= 1

    def test_delete_role_with_managed_policies(self):
        """Test delete_role properly cleans up managed policies"""
        mock_session = MagicMock()
        mock_iam = MagicMock()
        mock_sts = MagicMock()
        mock_session.client.side_effect = lambda service: mock_iam if service == "iam" else mock_sts
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

        # Mock inline policies
        mock_iam.list_role_policies.return_value = {"PolicyNames": ["InlinePolicy1"]}

        # Mock attached managed policies including IntelliFlow ones
        mock_iam.list_attached_role_policies.return_value = {
            "AttachedPolicies": [
                {"PolicyArn": "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess", "PolicyName": "AmazonS3ReadOnlyAccess"},
                {
                    "PolicyArn": "arn:aws:iam::123456789012:policy/test-role-MP-ExecutionPolicy",
                    "PolicyName": "test-role-MP-ExecutionPolicy",
                },
                {
                    "PolicyArn": "arn:aws:iam::123456789012:policy/test-role-MP-DevelopmentPolicy-Part-1",
                    "PolicyName": "test-role-MP-DevelopmentPolicy-Part-1",
                },
                {"PolicyArn": "arn:aws:iam::123456789012:policy/other-role-MP-SomePolicy", "PolicyName": "other-role-MP-SomePolicy"},
            ]
        }

        # Mock managed policy details for IntelliFlow policies
        def mock_list_entities(*args, **kwargs):
            return {"PolicyRoles": [], "PolicyUsers": [], "PolicyGroups": []}

        def mock_list_versions(*args, **kwargs):
            return {"Versions": [{"VersionId": "v1", "IsDefaultVersion": True}]}

        mock_iam.list_entities_for_policy.side_effect = mock_list_entities
        mock_iam.list_policy_versions.side_effect = mock_list_versions

        with patch("intelliflow.core.platform.definitions.aws.common.exponential_retry") as mock_retry:
            mock_retry.side_effect = lambda func, errors, *args, **kwargs: func(*args, **kwargs)

            delete_role("test-role", mock_session)

            # Should delete inline policy
            mock_iam.delete_role_policy.assert_called_with(RoleName="test-role", PolicyName="InlinePolicy1")

            # Should detach all policies (including AWS managed and other role's policies)
            assert mock_iam.detach_role_policy.call_count == 4

            # Should only delete IntelliFlow managed policies belonging to this role (2 policies)
            delete_policy_calls = [call for call in mock_iam.delete_policy.call_args_list]
            assert len(delete_policy_calls) == 2  # Only test-role's managed policies

            # Should delete the role itself
            mock_iam.delete_role.assert_called_once_with(RoleName="test-role")

    def test_policy_size_exceeds_max_partitions(self):
        """Test error when policy requires more partitions than AWS allows"""

        class MockConstructPermission:
            def __init__(self, resource, action):
                self.resource = resource
                self.action = action

            def __iter__(self):
                return iter([self.resource, self.action])

        # Create policy with many individual statements that would require too many partitions
        # Each permission has unique actions to prevent consolidation by _get_size_optimized_statements
        huge_permissions = set()
        for i in range(500):  # Much larger to ensure we exceed 10 partitions
            # Use unique actions for each permission to prevent consolidation
            unique_actions = [f"s3:GetObject{i}", f"s3:PutObject{i}", f"s3:DeleteObject{i}"]
            huge_permissions.add(
                MockConstructPermission(
                    [
                        f"arn:aws:s3:::extremely-long-bucket-name-that-will-make-the-policy-exceed-size-limits-{i}/*",
                        f"arn:aws:s3:::extremely-long-bucket-name-that-will-make-the-policy-exceed-size-limits-{i}",
                    ],
                    unique_actions,
                )
            )

        mock_session = MagicMock()
        mock_sts = MagicMock()
        mock_session.client.return_value = mock_sts
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

        with pytest.raises(ValueError, match="Policy requires .* managed policies, but AWS allows maximum"):
            put_managed_policy("test-role", "HugePolicy", huge_permissions, mock_session)

    def test_single_statement_too_large(self):
        """Test error when a single policy statement exceeds managed policy size limit"""

        class MockConstructPermission:
            def __init__(self, resource, action):
                self.resource = resource
                self.action = action

            def __iter__(self):
                return iter([self.resource, self.action])

        # Create a single permission with many resources that creates an oversized statement
        huge_permission = MockConstructPermission(
            [f"arn:aws:s3:::extremely-long-bucket-name-{i}/*" for i in range(200)],  # Many resources in one statement
            ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"],
        )

        mock_session = MagicMock()
        mock_sts = MagicMock()
        mock_session.client.return_value = mock_sts
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

        with pytest.raises(ValueError, match="Single policy statement exceeds AWS managed policy size limit"):
            put_managed_policy("test-role", "OversizedPolicy", {huge_permission}, mock_session)

    def test_segregate_trusted_entities_preserve_order(self):
        """Test that segregation preserves the original order within each category"""
        mixed_entities = [
            "zeta.amazonaws.com",
            "arn:aws:iam::999:role/ZRole",
            "alpha.amazonaws.com",
            "arn:aws:iam::111:role/ARole",
            "beta.amazonaws.com",
        ]

        services, entities = segregate_trusted_entities(mixed_entities)

        # Should preserve order within each category
        expected_services = ["zeta.amazonaws.com", "alpha.amazonaws.com", "beta.amazonaws.com"]
        expected_entities = ["arn:aws:iam::999:role/ZRole", "arn:aws:iam::111:role/ARole"]

        assert services == expected_services
        assert entities == expected_entities
