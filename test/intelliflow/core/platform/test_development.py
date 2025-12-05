# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock, patch

import intelliflow.core.platform.development as development
from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
from intelliflow.core.platform.development import AWSConfiguration


class TestDevelopment:
    def test_setup_session(self):
        config = (
            AWSConfiguration.builder()
            .with_default_credentials(as_admin=True)
            .with_region("us-east-1")
            .build()
        )

        _real_get_session = development.get_session

        def mock_get_session(credential_config, region, duration=0):
            return MagicMock()

        development.get_session = MagicMock(side_effect=mock_get_session)

        needs_to_assume_role, needs_to_check_role, session = config._setup_session("devRole")
        assert needs_to_assume_role
        assert needs_to_check_role
        assert session

    @patch("intelliflow.core.platform.development.segregate_trusted_entities")
    @patch("intelliflow.core.platform.development.create_role")
    @patch("intelliflow.core.platform.development.update_role")
    @patch("intelliflow.core.platform.development.has_role")
    @patch("intelliflow.core.platform.development.exponential_retry")
    @patch("intelliflow.core.platform.development.put_inlined_policy")
    def test_setup_runtime_params_segregation(
        self, mock_put_policy, mock_retry, mock_has_role, mock_update_role, mock_create_role, mock_segregate
    ):
        """Test that _setup_runtime_params properly segregates trusted entities"""

        # Setup test configuration
        config = AWSConfiguration()
        config._context_id = "test-app"
        config._params = {
            AWSCommonParams.REGION: "us-east-1",
            AWSCommonParams.ACCOUNT_ID: "123456789012",
            AWSCommonParams.BOTO_SESSION: MagicMock(),
            AWSCommonParams.IF_EXE_ROLE: "arn:aws:iam::123456789012:role/test-app-us-east-1-IntelliFlowExeRole",
        }

        # Mock the segregate_trusted_entities function
        mixed_entities = [
            "lambda.amazonaws.com",
            "arn:aws:iam::123456789012:role/MyRole",
            "sagemaker.amazonaws.com",
            "arn:aws:iam::123456789012:user/MyUser",
        ]
        expected_services = ["lambda.amazonaws.com", "sagemaker.amazonaws.com"]
        expected_entities = ["arn:aws:iam::123456789012:role/MyRole", "arn:aws:iam::123456789012:user/MyUser"]

        mock_segregate.return_value = (expected_services, expected_entities)

        # Mock provide_runtime_trusted_entities to return mixed list
        config.provide_runtime_trusted_entities = MagicMock(return_value=mixed_entities)
        config.provide_runtime_default_policies = MagicMock(return_value=set())
        config.provide_runtime_construct_permissions = MagicMock(return_value=[])

        # Mock has_role to return False (new role)
        mock_has_role.return_value = False

        # Mock retry function to just call the original function
        mock_retry.side_effect = lambda func, errors, *args, **kwargs: func(*args, **kwargs)

        # Call _setup_runtime_params
        config._setup_runtime_params()

        # Verify segregate_trusted_entities was called with the mixed list
        mock_segregate.assert_called_once_with(mixed_entities)

        # Verify create_role was called with segregated services and entities
        mock_create_role.assert_called_once()
        create_args = mock_create_role.call_args[0]
        create_kwargs = mock_create_role.call_args[1] if mock_create_role.call_args[1] else {}

        # Check the arguments passed to create_role
        assert len(create_args) >= 6  # role_name, session, region, auto_trust, services, entities
        assert create_args[4] == expected_services  # allowed_services
        assert create_args[5] == expected_entities  # allowed_aws_entities

        # Verify update_role was called with segregated services and entities + exe_role
        mock_update_role.assert_called_once()
        update_args = mock_update_role.call_args[0]

        # Check the arguments passed to update_role
        assert len(update_args) >= 6  # role_name, session, region, auto_trust, services, entities
        assert update_args[4] == expected_services  # allowed_services
        assert update_args[5] == expected_entities + [config._params[AWSCommonParams.IF_EXE_ROLE]]  # new_allowed_aws_entities

    @patch("intelliflow.core.platform.development.segregate_trusted_entities")
    @patch("intelliflow.core.platform.development.create_role")
    @patch("intelliflow.core.platform.development.update_role")
    @patch("intelliflow.core.platform.development.has_role")
    @patch("intelliflow.core.platform.development.exponential_retry")
    @patch("intelliflow.core.platform.development.put_inlined_policy")
    def test_setup_runtime_params_existing_role(
        self, mock_put_policy, mock_retry, mock_has_role, mock_update_role, mock_create_role, mock_segregate
    ):
        """Test that _setup_runtime_params skips create_role when role exists"""

        # Setup test configuration
        config = AWSConfiguration()
        config._context_id = "test-app"
        config._params = {
            AWSCommonParams.REGION: "us-east-1",
            AWSCommonParams.ACCOUNT_ID: "123456789012",
            AWSCommonParams.BOTO_SESSION: MagicMock(),
            AWSCommonParams.IF_EXE_ROLE: "arn:aws:iam::123456789012:role/test-app-us-east-1-IntelliFlowExeRole",
        }

        # Mock the segregate_trusted_entities function
        mixed_entities = ["lambda.amazonaws.com", "arn:aws:iam::123456789012:role/MyRole"]
        expected_services = ["lambda.amazonaws.com"]
        expected_entities = ["arn:aws:iam::123456789012:role/MyRole"]

        mock_segregate.return_value = (expected_services, expected_entities)

        # Mock provide_runtime_trusted_entities
        config.provide_runtime_trusted_entities = MagicMock(return_value=mixed_entities)
        config.provide_runtime_default_policies = MagicMock(return_value=set())
        config.provide_runtime_construct_permissions = MagicMock(return_value=[])

        # Mock has_role to return True (role exists)
        mock_has_role.return_value = True

        # Mock retry function
        mock_retry.side_effect = lambda func, errors, *args, **kwargs: func(*args, **kwargs)

        # Call _setup_runtime_params
        config._setup_runtime_params()

        # Verify segregate_trusted_entities was called
        mock_segregate.assert_called_once_with(mixed_entities)

        # Verify create_role was NOT called (role already exists)
        mock_create_role.assert_not_called()

        # Verify update_role was still called with correct parameters
        mock_update_role.assert_called_once()
        update_args = mock_update_role.call_args[0]
        assert update_args[4] == expected_services  # allowed_services
        assert update_args[5] == expected_entities + [config._params[AWSCommonParams.IF_EXE_ROLE]]  # new_allowed_aws_entities

    @patch("intelliflow.core.platform.development.segregate_trusted_entities")
    @patch("intelliflow.core.platform.development.create_role")
    @patch("intelliflow.core.platform.development.update_role")
    @patch("intelliflow.core.platform.development.has_role")
    @patch("intelliflow.core.platform.development.exponential_retry")
    @patch("intelliflow.core.platform.development.put_inlined_policy")
    def test_setup_runtime_params_empty_segregation(
        self, mock_put_policy, mock_retry, mock_has_role, mock_update_role, mock_create_role, mock_segregate
    ):
        """Test _setup_runtime_params with empty segregated lists"""

        # Setup test configuration
        config = AWSConfiguration()
        config._context_id = "test-app"
        config._params = {
            AWSCommonParams.REGION: "us-east-1",
            AWSCommonParams.ACCOUNT_ID: "123456789012",
            AWSCommonParams.BOTO_SESSION: MagicMock(),
            AWSCommonParams.IF_EXE_ROLE: "arn:aws:iam::123456789012:role/test-app-us-east-1-IntelliFlowExeRole",
        }

        # Mock segregation to return empty lists
        mock_segregate.return_value = ([], [])

        # Mock provide_runtime_trusted_entities to return empty list
        config.provide_runtime_trusted_entities = MagicMock(return_value=[])
        config.provide_runtime_default_policies = MagicMock(return_value=set())
        config.provide_runtime_construct_permissions = MagicMock(return_value=[])

        # Mock has_role to return False
        mock_has_role.return_value = False

        # Mock retry function
        mock_retry.side_effect = lambda func, errors, *args, **kwargs: func(*args, **kwargs)

        # Call _setup_runtime_params
        config._setup_runtime_params()

        # Verify segregate_trusted_entities was called
        mock_segregate.assert_called_once_with([])

        # Verify create_role was called with empty services and entities
        mock_create_role.assert_called_once()
        create_args = mock_create_role.call_args[0]
        assert create_args[4] == []  # allowed_services (empty)
        assert create_args[5] == []  # allowed_aws_entities (empty)

        # Verify update_role was called with empty services but exe_role in entities
        mock_update_role.assert_called_once()
        update_args = mock_update_role.call_args[0]
        assert update_args[4] == []  # allowed_services (empty)
        assert update_args[5] == [config._params[AWSCommonParams.IF_EXE_ROLE]]  # only exe_role
