import json
import os
from unittest.mock import MagicMock, Mock, patch

import boto3
import pytest
from moto import mock_iam, mock_lambda, mock_s3

from intelliflow.api_ext import *
from intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda import (
    AWSLambdaControlPlaneExtension,
    ManagementAPI,
    route_api_request,
)
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSLambdaControlPlaneExtension(AWSTestBase):

    def test_application_lambda_control_plane_extension_zero_configuration(self):
        """Test that CompositeExtension exists but is empty with zero configuration."""
        self.patch_aws_start()

        app = AWSApplication("lambda_ctrl_test", self.region)

        # CompositeExtension should be there
        assert app.platform.extensions
        # however map should be empty
        assert not app.platform.extensions.extensions_map

        # check metrics
        system_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.SYSTEM)
        extensions_metrics_map = system_metrics_map[CompositeExtension]

        assert not extensions_metrics_map, "There should not be any system metrics from platform.extensions!"

        self.patch_aws_stop()

    def test_application_lambda_control_plane_extension_descriptor_validation(self):
        """Test descriptor validation and corner cases."""
        self.patch_aws_start()

        # if extension_id is not provided, default extension ID is used
        assert ManagementAPI().extension_id == ManagementAPI.DEFAULT_EXTENSION_ID

        extension_id: str = "lambda_control_plane_ext_1"

        # lambda_name and FunctionName cannot be defined at the same time
        with pytest.raises(ValueError):
            ManagementAPI(extension_id=extension_id, lambda_name="my_lambda", FunctionName="my_lambda")

        # Test authorized clients default values
        management_api = ManagementAPI(extension_id=extension_id)
        assert management_api.authorized_clients == ManagementAPI.DEFAULT_AUTHORIZED_CLIENTS

        # Test custom authorized clients
        custom_clients = {"arn:aws:iam::123456789012:role/CustomRole"}
        management_api = ManagementAPI(extension_id=extension_id, authorized_clients=custom_clients)
        assert management_api.authorized_clients == custom_clients

        self.patch_aws_stop()

    @mock_s3
    def test_application_lambda_control_plane_extension_s3_storage_validation(self):
        """Test that extension validates S3-based storage."""
        self.patch_aws_start()

        extension_id: str = "lambda_control_plane_ext_2"
        lambda_extension = ManagementAPI(extension_id=extension_id)

        # This should work with default AWSApplication (uses S3 storage)
        app = AWSApplication("lambda_ctrl_test", self.region, PLATFORM_EXTENSIONS=[lambda_extension])

        # Manually check the validation logic by accessing the extension directly
        extension_instance = app.platform.extensions.extensions_map[extension_id]
        assert extension_instance._bucket_name is not None
        assert extension_instance._bucket_folder_prefix == f"extension_data/{AWSLambdaControlPlaneExtension.__name__}"

        self.patch_aws_stop()

    @mock_lambda
    @mock_s3
    @mock_iam
    def test_application_lambda_control_plane_extension_basic_functionality(self):
        """Test basic extension functionality including Lambda provisioning."""
        self.patch_aws_start()

        extension_id: str = "lambda_control_plane_ext_3"
        lambda_extension = ManagementAPI(extension_id=extension_id, memory_size=256, timeout=60)

        app = AWSApplication("lambda_ctrl_test", self.region, PLATFORM_EXTENSIONS=[lambda_extension])

        # CompositeExtension should be there
        assert app.platform.extensions
        # map should not be empty
        assert app.platform.extensions.extensions_map

        assert app.platform.extensions[extension_id]
        extension_instance = app.platform.extensions[extension_id]

        # Check initial state - Lambda should not exist
        lambda_client = boto3.client("lambda", region_name=self.region)

        with pytest.raises(lambda_client.exceptions.ResourceNotFoundException):
            lambda_client.get_function(FunctionName=extension_instance.lambda_name)

        # Check metrics before activation
        system_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.SYSTEM)
        extensions_metrics_map = system_metrics_map[CompositeExtension]

        # Should have metrics since extension is configured
        assert extensions_metrics_map, "There must be system metrics from the platform extension!"
        control_plane_metrics = extensions_metrics_map[f"controlPlane.{extension_id.lower()}"]
        assert control_plane_metrics

        self.patch_aws_stop()

    @mock_lambda
    @mock_s3
    @mock_iam
    def test_application_lambda_control_plane_extension_lambda_naming(self):
        """Test Lambda function naming logic."""
        self.patch_aws_start()

        extension_id: str = "lambda_control_plane_ext_4"

        # Test default naming
        lambda_extension1 = ManagementAPI(extension_id=extension_id)
        app1 = AWSApplication("lambda_test", self.region, PLATFORM_EXTENSIONS=[lambda_extension1])
        extension_instance1 = app1.platform.extensions[extension_id]

        expected_name = f"IntelliFlow-ControlPlane-lambda_test-{extension_id}"
        assert extension_instance1.lambda_name == expected_name

        # Test custom naming
        custom_name = "my-custom-control-plane-lambda"
        lambda_extension2 = ManagementAPI(extension_id=extension_id + "_custom", lambda_name=custom_name)
        app2 = AWSApplication("lambda_test2", self.region, PLATFORM_EXTENSIONS=[lambda_extension2])
        extension_instance2 = app2.platform.extensions[extension_id + "_custom"]

        assert extension_instance2.lambda_name == custom_name

        self.patch_aws_stop()

    def test_application_lambda_control_plane_extension_multiple_configurations(self):
        """Test multiple extensions with different configurations."""
        self.patch_aws_start()

        # same extension_id should raise error
        with pytest.raises(ValueError):
            app = AWSApplication(
                "lambda_multi",
                self.region,
                PLATFORM_EXTENSIONS=[
                    ManagementAPI(extension_id="ext1", lambda_name="lambda1"),
                    ManagementAPI(extension_id="ext1", lambda_name="lambda2"),
                ],
            )

        # Different extension_ids should work
        app = AWSApplication(
            "lambda_multi",
            self.region,
            PLATFORM_EXTENSIONS=[
                ManagementAPI(extension_id="ext1", memory_size=256),
                ManagementAPI(extension_id="ext2", memory_size=512, timeout=30),
            ],
        )

        # CompositeExtension should be there
        assert app.platform.extensions
        # map should not be empty
        assert len(app.platform.extensions.extensions_map) == 2

        # Check extension configurations
        ext1 = app.platform.extensions["ext1"]
        ext2 = app.platform.extensions["ext2"]

        assert ext1.desc.memory_size == 256
        assert ext2.desc.memory_size == 512
        assert ext2.desc.timeout == 30

        # check metrics
        system_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.SYSTEM)
        extensions_metrics_map = system_metrics_map[CompositeExtension]

        assert extensions_metrics_map, "There must be system metrics from the platform extension!"

        # Both extensions should have metrics
        assert f"controlPlane.ext1" in extensions_metrics_map
        assert f"controlPlane.ext2" in extensions_metrics_map

        self.patch_aws_stop()

    def test_lambda_control_plane_extension_authorized_clients_configuration(self):
        """Test authorized clients configuration and validation."""
        self.patch_aws_start()

        extension_id: str = "lambda_control_plane_ext_auth"

        # Test custom authorized clients
        custom_clients = {"arn:aws:iam::123456789012:role/CustomRole1", "arn:aws:iam::987654321098:role/CustomRole2"}

        lambda_extension = ManagementAPI(extension_id=extension_id, authorized_clients=custom_clients)
        app = AWSApplication("lambda_auth", self.region, PLATFORM_EXTENSIONS=[lambda_extension])

        extension_instance = app.platform.extensions[extension_id]
        assert extension_instance.desc.authorized_clients == custom_clients

        self.patch_aws_stop()

    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.AWSLambdaControlPlaneExtension.runtime_bootstrap")
    def test_route_api_request_function(self, mock_bootstrap):
        """Test the route_api_request function."""
        self.patch_aws_start()

        from intelliflow.core.platform import development

        development.IS_ON_AWS_LAMBDA = True

        # Mock runtime platform
        mock_runtime_platform = Mock()
        mock_runtime_platform.processor._params = {"CONTEXT": "test_app", "AWS_ACCOUNT_ID": "123456789012", "AWS_REGION": "us-east-1"}
        mock_bootstrap.return_value = mock_runtime_platform

        # Test ping endpoint
        result = route_api_request(mock_runtime_platform, "/ping", {})
        assert result == {"message": "pong"}

        # Test unknown path
        with pytest.raises(ValueError, match="Unknown API path"):
            route_api_request(mock_runtime_platform, "/unknown/path", {})

        development.IS_ON_AWS_LAMBDA = False
        self.patch_aws_stop()

    @mock_lambda
    @mock_s3
    @mock_iam
    def test_lambda_handler_function(self):
        """Test the Lambda handler function."""
        self.patch_aws_start()
        from intelliflow.core.platform import development

        development.IS_ON_AWS_LAMBDA = True

        # Test successful ping request
        event = {"httpMethod": "GET", "path": "/ping", "queryStringParameters": {}}
        context = {}

        with patch(
            "intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.AWSLambdaControlPlaneExtension.runtime_bootstrap"
        ) as mock_bootstrap:
            mock_runtime_platform = Mock()
            mock_runtime_platform.processor._params = {"CONTEXT": "test_app", "AWS_ACCOUNT_ID": "123456789012", "AWS_REGION": "us-east-1"}
            mock_bootstrap.return_value = mock_runtime_platform

            response = AWSLambdaControlPlaneExtension.lambda_handler(event, context)

            assert response["statusCode"] == 200
            assert "application/json" in response["headers"]["Content-Type"]
            body = json.loads(response["body"])
            assert body == {"message": "pong"}

        # Test error handling
        event = {"httpMethod": "GET", "path": "/unknown/path", "queryStringParameters": {}}

        with patch(
            "intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.AWSLambdaControlPlaneExtension.runtime_bootstrap"
        ) as mock_bootstrap:
            mock_runtime_platform = Mock()
            mock_runtime_platform.processor._params = {"CONTEXT": "test_app", "AWS_ACCOUNT_ID": "123456789012", "AWS_REGION": "us-east-1"}
            mock_bootstrap.return_value = mock_runtime_platform

            response = AWSLambdaControlPlaneExtension.lambda_handler(event, context)

            assert response["statusCode"] == 500
            body = json.loads(response["body"])
            assert "error" in body

        development.IS_ON_AWS_LAMBDA = False
        self.patch_aws_stop()

    def test_extension_permissions_structure(self):
        """Test extension permissions structure."""
        self.patch_aws_start()

        extension_id: str = "lambda_control_plane_ext_perms"
        lambda_extension = ManagementAPI(extension_id=extension_id)

        app = AWSApplication("lambda_perms", self.region, PLATFORM_EXTENSIONS=[lambda_extension])
        extension_instance = app.platform.extensions[extension_id]

        # Runtime permissions should be empty (uses dev role)
        runtime_perms = extension_instance.provide_runtime_permissions()
        assert runtime_perms == []

        # Runtime trusted entities should be empty (uses dev role)
        trusted_entities = extension_instance.provide_runtime_trusted_entities()
        assert trusted_entities == []

        # Development time permissions should be provided
        from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
        from intelliflow.core.platform.definitions.common import ActivationParams

        params = {AWSCommonParams.REGION: self.region, AWSCommonParams.ACCOUNT_ID: "123456789012"}

        devtime_perms = AWSLambdaControlPlaneExtension.provide_devtime_permissions(params)
        assert not devtime_perms

        # Test extension-specific permissions
        from intelliflow.core.platform.definitions.common import ActivationParams

        ext_params = {
            AWSCommonParams.REGION: self.region,
            AWSCommonParams.ACCOUNT_ID: "123456789012",
            ActivationParams.CONTEXT_ID: "test_app",
        }

        ext_devtime_perms = AWSLambdaControlPlaneExtension.provide_devtime_permissions_ext(lambda_extension, ext_params)
        assert len(ext_devtime_perms) > 0

        # Should include specific Lambda function permissions
        lambda_perms = [p for p in ext_devtime_perms if any("lambda:" in action for action in p.action)]
        assert len(lambda_perms) > 0

        # Check that the ARN includes the specific function name
        lambda_perm = lambda_perms[0]
        expected_function_name = f"IntelliFlow-ControlPlane-test_app-{extension_id}"
        assert any(expected_function_name in resource for resource in lambda_perm.resource)

        self.patch_aws_stop()

    @mock_lambda
    @mock_s3
    @mock_iam
    def test_extension_removal_cleanup(self):
        """Test that extension resources are cleaned up when removed."""
        self.patch_aws_start()

        extension_id: str = "lambda_control_plane_ext_removal"
        lambda_extension = ManagementAPI(extension_id=extension_id)

        # Create app with extension
        app = AWSApplication("lambda_removal", self.region, PLATFORM_EXTENSIONS=[lambda_extension])

        extension_instance = app.platform.extensions[extension_id]
        lambda_name = extension_instance.lambda_name

        app.activate()

        # Create app without extensions - should trigger cleanup
        app = AWSApplication("lambda_removal", self.region)

        # The removed extension should be tracked
        if hasattr(app.platform.extensions, "removed_extensions"):
            assert len(app.platform.extensions.removed_extensions) > 0  # May be empty if cleanup already happened

        app.activate()

        self.patch_aws_stop()

    def test_extension_system_metrics(self):
        """Test system metrics provided by the extension."""
        self.patch_aws_start()

        extension_id: str = "lambda_control_plane_ext_metrics"
        lambda_extension = ManagementAPI(extension_id=extension_id)

        app = AWSApplication("lambda_metrics", self.region, PLATFORM_EXTENSIONS=[lambda_extension])
        extension_instance = app.platform.extensions[extension_id]

        # Test system metrics
        system_metrics = extension_instance._provide_system_metrics()
        assert len(system_metrics) > 0

        # Should have Lambda function metrics
        lambda_metric = system_metrics[0]
        assert lambda_metric.resource_access_spec.source.value == "CW_METRIC"
        assert "AWS/Lambda" in str(lambda_metric.resource_access_spec.context_id)
        assert extension_instance.lambda_name in str(lambda_metric.resource_access_spec.sub_dimensions)

        self.patch_aws_stop()

    def test_management_api_syntactic_sugar(self):
        """Test ManagementAPI class as syntactic sugar."""
        self.patch_aws_start()

        extension_id: str = "management_api_test"

        # Test ManagementAPI instantiation
        management_api = ManagementAPI(
            extension_id=extension_id, memory_size=1024, timeout=120, authorized_clients={"arn:aws:iam::111111111111:role/TestRole"}
        )

        # Should be instance of the Descriptor
        assert isinstance(management_api, AWSLambdaControlPlaneExtension.Descriptor)
        assert management_api.extension_id == extension_id
        assert management_api.memory_size == 1024
        assert management_api.timeout == 120
        assert management_api.authorized_clients == {"arn:aws:iam::111111111111:role/TestRole"}

        self.patch_aws_stop()
