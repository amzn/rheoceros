"""
Test module for AWS Lambda Control Plane S3 large response handling.

This module tests S3 large response handling without truncation fallback.
"""

import json
import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda import (
    MAX_SAFE_RESPONSE_SIZE,
    cleanup_expired_s3_responses,
    handle_large_response,
)


class TestLambdaLargeResponseHandling(unittest.TestCase):
    """Test cases for Lambda S3 large response handling functionality."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_bucket = "test-intelliflow-bucket"
        self.test_context_id = "test-app-123"
        self.test_path = "/intelli-flow/api/getNodesSummary"

        # Create a large response that exceeds the limit
        self.large_response = {"nodes": [{"id": f"node_{i}", "data": "x" * 1000} for i in range(6000)], "metadata": {"total_nodes": 6000}}

        # Create a small response within limits
        self.small_response = {"nodes": [{"id": "node_1", "data": "small"}], "metadata": {"total_nodes": 1}}

    @patch("boto3.resource")
    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.get_bucket")
    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.exponential_retry")
    def test_handle_small_response(self, mock_retry, mock_get_bucket, mock_boto3):
        """Test that small responses are returned directly."""
        result = handle_large_response(self.small_response, self.test_bucket, self.test_path, self.test_context_id)

        # Should return direct response for small data
        self.assertEqual(result["type"], "direct")
        self.assertEqual(result["data"], self.small_response)
        self.assertLess(result["size_bytes"], MAX_SAFE_RESPONSE_SIZE)

        # S3 operations should not be called for small responses
        mock_get_bucket.assert_not_called()

    @patch("boto3.resource")
    @patch("boto3.client")
    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.get_bucket")
    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.exponential_retry")
    def test_handle_large_response_s3_storage(self, mock_retry, mock_get_bucket, mock_boto3_client, mock_boto3_resource):
        """Test that large responses are stored in S3."""
        # Mock S3 operations
        mock_bucket = MagicMock()
        mock_get_bucket.return_value = mock_bucket

        mock_s3_client = MagicMock()
        mock_s3_client.generate_presigned_url.return_value = "https://presigned-url.com"
        mock_boto3_client.return_value = mock_s3_client

        os.environ["AWS_REGION"] = "us-east-1"

        result = handle_large_response(self.large_response, self.test_bucket, self.test_path, self.test_context_id)

        # Should return S3 reference for large data
        self.assertEqual(result["type"], "s3_reference")
        self.assertEqual(result["s3_bucket"], self.test_bucket)
        self.assertIn("s3_key", result)
        self.assertGreater(result["size_bytes"], MAX_SAFE_RESPONSE_SIZE)
        self.assertIn("download_instructions", result)
        self.assertIn("presigned_url", result)
        self.assertEqual(result["presigned_url"], "https://presigned-url.com")

        # S3 operations should be called
        mock_get_bucket.assert_called_once()
        mock_retry.assert_called_once()
        mock_s3_client.generate_presigned_url.assert_called_once()

    @patch("boto3.resource")
    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.get_bucket")
    def test_handle_large_response_s3_failure_raises_error(self, mock_get_bucket, mock_boto3):
        """Test that S3 storage failure raises error instead of truncating."""
        # Mock S3 failure
        mock_get_bucket.side_effect = Exception("S3 operation failed")

        # Should raise RuntimeError when S3 fails
        with self.assertRaises(RuntimeError) as context:
            handle_large_response(self.large_response, self.test_bucket, self.test_path, self.test_context_id)

        self.assertIn("Response too large", str(context.exception))
        self.assertIn("S3 storage failed", str(context.exception))

    @patch("boto3.resource")
    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.get_bucket")
    def test_cleanup_expired_s3_responses(self, mock_get_bucket, mock_boto3):
        """Test S3 cleanup of expired response objects."""
        # Mock S3 bucket and objects
        mock_bucket = MagicMock()
        mock_get_bucket.return_value = mock_bucket

        # Create mock objects - some expired, some current
        mock_expired_obj = MagicMock()
        mock_expired_obj.key = "test_key_expired"
        mock_expired_obj.metadata = {"expiration": "2023-01-01T00:00:00"}
        mock_expired_obj.last_modified = datetime(2023, 1, 1)

        mock_current_obj = MagicMock()
        mock_current_obj.key = "test_key_current"
        mock_current_obj.metadata = {"expiration": "2025-12-31T23:59:59"}
        mock_current_obj.last_modified = datetime.now()

        mock_bucket.objects.filter.return_value = [mock_expired_obj, mock_current_obj]

        # Run cleanup
        cleanup_expired_s3_responses(self.test_bucket, self.test_context_id)

        # Verify cleanup was attempted
        mock_get_bucket.assert_called_once()
        mock_bucket.objects.filter.assert_called_once()

    def test_response_size_constants(self):
        """Test that response size constants are properly defined."""
        # Verify constants are reasonable
        self.assertEqual(MAX_SAFE_RESPONSE_SIZE, 6 * 1024 * 1024 - 256 * 1024)
        self.assertGreater(MAX_SAFE_RESPONSE_SIZE, 5 * 1024 * 1024)  # Should be > 5MB
        self.assertLess(MAX_SAFE_RESPONSE_SIZE, 6 * 1024 * 1024)  # Should be < 6MB


class TestLambdaHandlerIntegration(unittest.TestCase):
    """Integration tests for the Lambda handler with S3 large response handling."""

    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.AWSLambdaControlPlaneExtension.runtime_bootstrap")
    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.route_api_request")
    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.handle_large_response")
    def test_lambda_handler_with_s3_response(self, mock_handle_large, mock_route_api, mock_bootstrap):
        """Test Lambda handler with S3 large response handling."""
        from intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda import AWSLambdaControlPlaneExtension

        # Mock runtime platform
        mock_platform = MagicMock()
        mock_platform.processor._params = {
            "ActivationParams.CONTEXT_ID": "test-app",
            "AWSCommonParams.ACCOUNT_ID": "123456789012",
            "AWSCommonParams.REGION": "us-east-1",
        }
        mock_platform.storage.bucket_name = "test-bucket"
        mock_bootstrap.return_value = mock_platform

        # Mock API response
        large_response = {"large_data": "x" * 1000000}
        mock_route_api.return_value = large_response

        # Mock large response handling to return S3 reference
        mock_handle_large.return_value = {
            "type": "s3_reference",
            "s3_bucket": "test-bucket",
            "s3_key": "test-key",
            "size_bytes": 7000000,
            "presigned_url": "https://presigned-url.com",
            "download_instructions": {"message": "Use S3 reference"},
        }

        # Set up Lambda event
        event = {"httpMethod": "GET", "path": "/intelli-flow/api/getNodesSummary", "queryStringParameters": {}}
        context = MagicMock()

        # Set environment variable for S3 bucket
        os.environ["bootstrapper_bucket"] = "test-bucket"

        try:
            # Call Lambda handler
            response = AWSLambdaControlPlaneExtension.lambda_handler(event, context)

            # Verify response
            self.assertEqual(response["statusCode"], 200)
            self.assertIn("X-Response-Type", response["headers"])
            self.assertEqual(response["headers"]["X-Response-Type"], "s3_reference")

            # Parse response body and verify S3 reference
            response_body = json.loads(response["body"])
            self.assertEqual(response_body["type"], "s3_reference")
            self.assertIn("presigned_url", response_body)

            # Verify large response handler was called
            mock_handle_large.assert_called_once()

        finally:
            # Cleanup environment
            if "bootstrapper_bucket" in os.environ:
                del os.environ["bootstrapper_bucket"]

    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.AWSLambdaControlPlaneExtension.runtime_bootstrap")
    @patch("intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.route_api_request")
    def test_lambda_handler_direct_response(self, mock_route_api, mock_bootstrap):
        """Test Lambda handler returns direct response for small data."""
        from intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda import AWSLambdaControlPlaneExtension

        # Mock runtime platform
        mock_platform = MagicMock()
        mock_platform.processor._params = {
            "ActivationParams.CONTEXT_ID": "test-app",
            "AWSCommonParams.ACCOUNT_ID": "123456789012",
            "AWSCommonParams.REGION": "us-east-1",
        }
        mock_bootstrap.return_value = mock_platform

        # Mock small API response
        test_response = {"nodes": [{"id": "node_1", "data": "test"}], "metadata": {"total_nodes": 1}}
        mock_route_api.return_value = test_response

        # Set up Lambda event
        event = {"httpMethod": "GET", "path": "/intelli-flow/api/getNodesSummary", "queryStringParameters": {}}
        context = MagicMock()

        # Set environment variable for S3 bucket
        os.environ["bootstrapper_bucket"] = "test-bucket"

        try:
            # Call Lambda handler
            response = AWSLambdaControlPlaneExtension.lambda_handler(event, context)

            # Verify response
            self.assertEqual(response["statusCode"], 200)
            self.assertEqual(response["headers"]["Content-Type"], "application/json")
            self.assertIn("Access-Control-Allow-Origin", response["headers"])
            self.assertIn("X-Response-Size", response["headers"])  # Should have size header for direct response

            # Parse response body - for direct responses, the body contains the unwrapped data
            response_body = json.loads(response["body"])
            self.assertEqual(response_body, test_response)

        finally:
            # Cleanup environment
            if "bootstrapper_bucket" in os.environ:
                del os.environ["bootstrapper_bucket"]
