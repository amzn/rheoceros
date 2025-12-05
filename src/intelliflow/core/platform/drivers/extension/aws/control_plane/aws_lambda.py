import json
import logging
import os
import sys
import uuid
from datetime import datetime, timedelta
from typing import ClassVar, Dict, List, Optional, Set, Type
from urllib.parse import unquote_plus

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from intelliflow.core.deployment import PYTHON_VERSION_MAJOR, PYTHON_VERSION_MINOR, get_working_set_as_zip_stream, is_environment_immutable
from intelliflow.core.platform.constructs import Extension
from intelliflow.core.signal_processing import DimensionFilter, Signal
from intelliflow.core.signal_processing.routing_runtime_constructs import Route
from intelliflow.core.signal_processing.signal import SignalDomainSpec, SignalType
from intelliflow.core.signal_processing.signal_source import CWMetricSignalSourceAccessSpec

from .....constructs import ConstructInternalMetricDesc, ConstructParamsDict, ConstructPermission, ConstructSecurityConf
from .....definitions.aws.aws_lambda.client_wrapper import *
from .....definitions.aws.common import CommonParams as AWSCommonParams
from .....definitions.aws.common import exponential_retry, generate_statement_id
from .....definitions.aws.s3.bucket_wrapper import bucket_exists, get_bucket
from .....definitions.aws.s3.object_wrapper import build_object_key, get_object, put_object
from .....definitions.common import ActivationParams
from ....aws_common import AWSConstructMixin
from .json_utils import intelliflow_jsonify

module_logger = logging.getLogger(__name__)


# Response size constants (AWS Lambda limits)
MAX_LAMBDA_RESPONSE_SIZE_BYTES: int = 6 * 1024 * 1024  # 6MB limit for synchronous invocations
RESPONSE_SIZE_BUFFER_BYTES: int = 256 * 1024  # 256KB buffer for headers and metadata
MAX_SAFE_RESPONSE_SIZE: int = MAX_LAMBDA_RESPONSE_SIZE_BYTES - RESPONSE_SIZE_BUFFER_BYTES  # ~5.75MB


def handle_large_response(response_data: dict, s3_bucket: str, path: str, context_id: str) -> dict:
    """
    Handle large responses by storing them in S3 and returning a reference.
    If S3 storage fails, the function will raise an error instead of truncating.

    Args:
        response_data: The original response data
        s3_bucket: S3 bucket name for storage
        path: API path for naming the S3 object
        context_id: Context ID for unique object naming

    Returns:
        Dict containing either the original response or S3 reference

    Raises:
        Exception: If response is too large and S3 storage fails
    """
    try:
        # First, try to serialize the response to check its size
        response_json = intelliflow_jsonify(response_data)
        response_size = len(response_json.encode("utf-8"))

        # If response is within limits, return as normal
        if response_size <= MAX_SAFE_RESPONSE_SIZE:
            return {"type": "direct", "data": response_data, "size_bytes": response_size}

        # Response is too large, store in S3
        module_logger.warning(f"Large response detected ({response_size} bytes), storing in S3")

        s3 = boto3.resource("s3")
        bucket = get_bucket(s3, s3_bucket)

        # Create unique S3 key with timestamp for cache management
        timestamp = datetime.utcnow().isoformat()
        s3_key = build_object_key(
            ["extension_data", "AWSLambdaControlPlaneExtension", "large_responses"],
            f"{context_id}_{path.replace('/', '_')}_{timestamp}_{uuid.uuid4().hex[:8]}.json",
        )

        # Store response in S3 with expiration metadata
        expiration_time = (datetime.utcnow() + timedelta(hours=24)).isoformat()

        exponential_retry(
            put_object,
            {"ServiceException", "TooManyRequestsException"},
            bucket,
            s3_key,
            response_json,
            metadata={"expiration": expiration_time, "api_path": path},
        )

        # Generate presigned URL for secure frontend access
        s3_client = boto3.client("s3", config=Config(signature_version="s3v4"), region_name=os.environ["AWS_REGION"])

        presigned_url = s3_client.generate_presigned_url(
            "get_object", Params={"Bucket": s3_bucket, "Key": s3_key}, ExpiresIn=86400  # 24 hours in seconds
        )

        # Return S3 reference with presigned URL for secure access
        return {
            "type": "s3_reference",
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
            "size_bytes": response_size,
            "expiration": expiration_time,
            "presigned_url": presigned_url,
            "download_instructions": {
                "message": "Response too large for direct return. Use the presigned URL to download the full response.",
                "presigned_download_url": presigned_url,
                "expires_at": expiration_time,
                "security_note": "This URL provides temporary secure access and will expire in 24 hours.",
            },
        }

    except Exception as e:
        module_logger.error(f"Failed to handle large response: {str(e)}")
        # Instead of truncating, raise an error
        raise RuntimeError(
            f"Response too large ({response_size if 'response_size' in locals() else 'unknown'} bytes) and S3 storage failed: {str(e)}"
        )


def get_s3_bucket_for_large_responses(runtime_platform=None) -> Optional[str]:
    """
    Get S3 bucket for large response storage with consistent fallback logic.

    Args:
        runtime_platform: Optional runtime platform for storage bucket fallback

    Returns:
        S3 bucket name or None if not available
    """
    # Primary: Check environment variable (set during Lambda bootstrap)
    s3_bucket = os.environ.get("bootstrapper_bucket")

    if not s3_bucket and runtime_platform:
        # Fallback: Try to get bucket from platform storage if available
        try:
            storage_path = runtime_platform.storage.get_storage_resource_path()
            if storage_path.startswith("arn:aws:s3") or storage_path.startswith("s3:"):
                s3_bucket = runtime_platform.storage.bucket_name
                module_logger.info(f"Using platform storage bucket as fallback: {s3_bucket}")
        except Exception as e:
            module_logger.warning(f"Failed to get storage bucket as fallback: {str(e)}")

    return s3_bucket


class AWSLambdaControlPlaneExtension(AWSConstructMixin, Extension):
    """AWS Lambda Control Plane Extension to expose IntelliFlow management APIs via Lambda function."""

    LAMBDA_NAME_FORMAT: ClassVar[str] = "IntelliFlow-ControlPlane-{0}-{1}"
    CLIENT_RETRYABLE_EXCEPTION_LIST: ClassVar[Set[str]] = {
        "InvalidParameterValueException",
        "ServiceException",
        "503",
        "ResourceConflictException",
    }

    class Descriptor(Extension.Descriptor):
        DEFAULT_AUTHORIZED_CLIENTS = {
            # "arn:aws:iam::{YOUR_ACCOUNT}:role/{UI_BACKEND_ROLE_NAME}",
        }

        DEFAULT_EXTENSION_ID = "IDE-API"

        def __init__(
            self,
            extension_id: Optional[str] = DEFAULT_EXTENSION_ID,
            lambda_name: Optional[str] = None,
            memory_size: int = 4096,
            timeout: int = 900,
            authorized_clients: Optional[Set[str]] = None,
            **extra_boto_args,
        ) -> None:
            super().__init__(extension_id, **extra_boto_args)
            self._lambda_name = lambda_name
            self._memory_size = memory_size
            self._timeout = timeout
            self._authorized_clients = authorized_clients or self.DEFAULT_AUTHORIZED_CLIENTS.copy()

            if "FunctionName" in extra_boto_args:
                if self._lambda_name:
                    raise ValueError(f"{self.__class__.__name__}: 'lambda_name' and 'FunctionName' cannot be defined at the same time!")
                self._lambda_name = extra_boto_args["FunctionName"]

        @property
        def lambda_name(self) -> Optional[str]:
            return self._lambda_name

        @property
        def memory_size(self) -> int:
            return self._memory_size

        @property
        def timeout(self) -> int:
            return self._timeout

        @property
        def authorized_clients(self) -> Set[str]:
            return self._authorized_clients

        # overrides
        def __eq__(self, other):
            return type(self) == type(other) and (
                (not self.lambda_name and not other.lambda_name and self.extension_id == other._extension_id)
                or ((self.lambda_name or other.lambda_name) and self.lambda_name == other.lambda_name)
            )

        def __hash__(self) -> int:
            return hash(self.__class__) + hash(self.lambda_name)

        @classmethod
        def provide_extension_type(cls) -> Type["Extension"]:
            return AWSLambdaControlPlaneExtension

    def __init__(self, params: ConstructParamsDict) -> None:
        """Called the first time this construct is added/configured within a platform."""
        super().__init__(params)
        self._lambda = self._session.client(service_name="lambda", region_name=self._region)
        self._lambda_name = None
        self._lambda_arn = None
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._bucket = None
        self._bucket_name = None

    @property
    def lambda_name(self) -> str:
        return self._lambda_name

    @property
    def lambda_arn(self) -> str:
        return self._lambda_arn

    def _deserialized_init(self, params: ConstructParamsDict) -> None:
        super()._deserialized_init(params)
        self._lambda = self._session.client(service_name="lambda", region_name=self._region)
        self._s3 = self._session.resource("s3", region_name=self._region)
        self._bucket = get_bucket(self._s3, self._bucket_name)

    def _serializable_copy_init(self, org_instance: "BaseConstruct") -> None:
        AWSConstructMixin._serializable_copy_init(self, org_instance)
        self._lambda = None
        self._s3 = None
        self._bucket = None

    def dev_init(self, platform: "DevelopmentPlatform") -> None:
        super().dev_init(platform)

        # Set default lambda name if not provided
        if self.desc.lambda_name is None:
            self._lambda_name = self.LAMBDA_NAME_FORMAT.format(self._dev_platform.context_id, self.desc.extension_id)
        else:
            self._lambda_name = self.desc.lambda_name

        self._lambda_arn = f"arn:aws:lambda:{self._region}:{self._account_id}:function:{self._lambda_name}"

        # Check that platform uses S3-based Storage
        storage_path: str = platform.storage.get_storage_resource_path()
        if not (storage_path.startswith("arn:aws:s3") or storage_path.startswith("s3:")):
            raise TypeError(f"{self.__class__.__name__} extension should be used with an S3 based Storage!")

        # Use the platform's storage bucket instead of creating our own
        self._bucket_name = platform.storage.bucket_name
        self._bucket_folder_prefix = f"extension_data/{self.__class__.__name__}"

    def runtime_init(self, platform: "RuntimePlatform", context_owner: "BaseConstruct") -> None:
        """Initialize runtime platform components."""
        AWSConstructMixin.runtime_init(self, platform, context_owner)
        self._lambda = boto3.client(service_name="lambda", region_name=self._region)
        self._s3 = boto3.resource("s3")
        self._bucket = get_bucket(self._s3, self._bucket_name)

    def provide_runtime_trusted_entities(self) -> List[str]:
        return []

    def provide_runtime_default_policies(self) -> List[str]:
        return []

    def provide_runtime_permissions(self) -> List[ConstructPermission]:
        """Lambda function uses development role which already has necessary permissions."""
        return []

    @classmethod
    def provide_devtime_permissions_ext(
        cls, extension_desc: "Extension.Descriptor", params: ConstructParamsDict
    ) -> List[ConstructPermission]:
        """Provide development time permissions for specific extension instance."""
        region: str = params[AWSCommonParams.REGION]
        account_id: str = params[AWSCommonParams.ACCOUNT_ID]
        context_id: str = params[ActivationParams.CONTEXT_ID]

        # Determine lambda function name from descriptor or generate default
        lambda_name = extension_desc.lambda_name
        if lambda_name is None:
            lambda_name = cls.LAMBDA_NAME_FORMAT.format(context_id, extension_desc.extension_id)

        return [
            ConstructPermission(
                [
                    # Lambda function permissions
                    f"arn:aws:lambda:{region}:{account_id}:function:{lambda_name}",
                ],
                ["lambda:*"],
            ),
            ConstructPermission(["*"], ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]),
            # "development" module by default add this to dev-role
            # ConstructPermission(
            #    [
            #       params[AWSCommonParams.IF_DEV_ROLE]
            #    ],
            #    ["iam:PassRole"]
            # )
        ]

    @classmethod
    def provide_devtime_permissions(cls, params: ConstructParamsDict) -> List[ConstructPermission]:
        """Provide development time permissions."""
        return []

    def _provide_system_metrics(self) -> List[Signal]:
        """Expose Lambda function metrics."""
        lambda_metrics = [
            "Invocations",
            "Errors",
            "Duration",
            "Throttles",
            "ConcurrentExecutions",
            "DeadLetterErrors",
        ]

        return (
            [
                Signal(
                    SignalType.CW_METRIC_DATA_CREATION,
                    CWMetricSignalSourceAccessSpec(
                        "AWS/Lambda",
                        {"FunctionName": self._lambda_name},
                        **{"Notes": "Supports 1 min period"},
                    ),
                    SignalDomainSpec(
                        dimension_filter_spec=DimensionFilter.load_raw(
                            {metric_name: {"*": {"*": {"*": {}}}} for metric_name in lambda_metrics}
                        ),
                        dimension_spec=None,
                        integrity_check_protocol=None,
                    ),
                    f"controlPlane.{self.desc.extension_id.lower()}",
                )
            ]
            if self._lambda_name
            else []
        )

    def _upload_working_set_to_s3(self, working_set_stream: bytes, bucket, s3_object_key: str) -> Dict[str, str]:
        """Upload working set to S3 for Lambda deployment."""
        exponential_retry(put_object, self.CLIENT_RETRYABLE_EXCEPTION_LIST, bucket, s3_object_key, working_set_stream)
        return {"S3Bucket": self._bucket_name, "S3Key": s3_object_key}

    def build_bootstrapper_object_key(self) -> str:
        """Build S3 object key for bootstrapper."""
        return build_object_key([self._bucket_folder_prefix, "bootstrapper"], f"{self.__class__.__name__.lower()}_RuntimePlatform.data")

    def _update_bootstrapper(self, bootstrapper: "RuntimePlatform") -> None:
        """Update Lambda function with new bootstrapper."""
        bootstrapped_platform = bootstrapper.serialize()
        bootstrapper_object_key = self.build_bootstrapper_object_key()

        exponential_retry(
            put_object, {"ServiceException", "TooManyRequestsException"}, self._bucket, bootstrapper_object_key, bootstrapped_platform
        )

        # Update Lambda function configuration
        exponential_retry(
            update_lambda_function_conf,
            self.CLIENT_RETRYABLE_EXCEPTION_LIST,
            self._lambda,
            self._lambda_name,
            f"IntelliFlow Control Plane API Lambda for {self._dev_platform.context_id}",
            "intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.lambda_handler",
            self._params[AWSCommonParams.IF_DEV_ROLE],
            PYTHON_VERSION_MAJOR,
            PYTHON_VERSION_MINOR,
            None,  # No DLQ for control plane
            self.desc.memory_size,
            self.desc.timeout,
            bootstrapper_bucket=self._bucket_name,
            bootstrapper_key=bootstrapper_object_key,
        )

    def activate(self) -> None:
        """Activate the extension - provision Lambda function and supporting resources."""
        super().activate()

        # Ensure storage bucket exists (managed by Storage driver)
        if not bucket_exists(self._s3, self._bucket_name):
            raise RuntimeError(f"Storage S3 bucket {self._bucket_name} does not exist!")

        self._bucket = get_bucket(self._s3, self._bucket_name)

        # Deploy Lambda function if environment is not immutable
        if not is_environment_immutable():
            working_set_stream = get_working_set_as_zip_stream()
            s3_object_key = build_object_key([self._bucket_folder_prefix], "ControlPlaneWorkingSet.zip")
            deployment_package = self._upload_working_set_to_s3(working_set_stream, self._bucket, s3_object_key)
            self._activate_lambda(deployment_package)

    def _activate_lambda(self, deployment_package: Dict[str, str]):
        """Activate the control plane Lambda function."""
        lambda_arn = exponential_retry(get_lambda_arn, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name)

        if lambda_arn and lambda_arn != self._lambda_arn:
            raise RuntimeError(f"AWS Lambda returned an ARN in an unexpected format. Expected: {self._lambda_arn}. Got: {lambda_arn}")

        if not lambda_arn:
            # Create new Lambda function
            self._lambda_arn = exponential_retry(
                create_lambda_function,
                self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                self._lambda,
                self._lambda_name,
                f"IntelliFlow Control Plane API Lambda for {self._dev_platform.context_id}",
                "intelliflow.core.platform.drivers.extension.aws.control_plane.aws_lambda.lambda_handler",
                self._params[AWSCommonParams.IF_DEV_ROLE],
                deployment_package,
                PYTHON_VERSION_MAJOR,
                PYTHON_VERSION_MINOR,
                None,  # No DLQ for control plane
                memory_size=self.desc.memory_size,
                timeout=self.desc.timeout,
            )
        else:
            # Update existing Lambda function
            exponential_retry(
                update_lambda_function_code, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name, deployment_package
            )

        # Grant invoke permissions to authorized clients
        self._grant_invoke_permissions()

    def _grant_invoke_permissions(self):
        """Grant invoke permissions to authorized clients (IntelliFlow IDE backend roles)."""
        for client_arn in self.desc.authorized_clients:
            # Extract account ID and role name from ARN for unique statement ID
            statement_id = generate_statement_id(f"InvokePermission_{client_arn}")

            try:
                # Remove existing permission first (idempotent)
                exponential_retry(remove_permission, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name, statement_id)
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["ResourceNotFoundException"]:
                    raise

            # Add permission for the authorized client to invoke the Lambda
            try:
                exponential_retry(
                    add_permission,
                    self.CLIENT_RETRYABLE_EXCEPTION_LIST,
                    self._lambda,
                    self._lambda_name,
                    statement_id,
                    "lambda:InvokeFunction",
                    client_arn,  # client_arn.split(":")[4],  # Extract account ID from ARN
                )
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["ResourceConflictException"]:
                    raise

    def terminate(self) -> None:
        """Terminate the extension - cleanup Lambda function and resources."""
        # Delete Lambda function
        if self._lambda_name:
            try:
                exponential_retry(delete_lambda_function, self.CLIENT_RETRYABLE_EXCEPTION_LIST, self._lambda, self._lambda_name)
            except ClientError as error:
                if error.response["Error"]["Code"] not in ["ResourceNotFoundException", "404"]:
                    raise
            self._lambda_name = None

        # Note: We don't delete the S3 bucket as it's managed by the Storage driver
        # Only clean up our extension-specific files if needed
        self._bucket_name = None
        self._bucket = None

        super().terminate()

    def rollback(self) -> None:
        super().rollback()

    def check_update(self, prev_construct: "BaseConstruct") -> None:
        super().check_update(prev_construct)

    def hook_security_conf(
        self, security_conf: ConstructSecurityConf, platform_security_conf: Dict[Type["BaseConstruct"], ConstructSecurityConf]
    ) -> None:
        super().hook_security_conf(security_conf, platform_security_conf)

    # Required abstract method implementations
    def _process_internal(self, new_routes: Set[Route], current_routes: Set[Route]) -> None:
        pass

    def _revert_internal(self, routes: Set[Route], prev_routes: Set[Route]) -> None:
        pass

    def _process_external(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_internal_signals(self, new_signals: Set[Signal], current_signals: Set[Signal]) -> None:
        pass

    def _process_construct_connections(
        self, new_construct_conns: Set["_PendingConnRequest"], current_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _process_security_conf(self, new_security_conf: ConstructSecurityConf, current_security_conf: ConstructSecurityConf) -> None:
        pass

    def _revert_external(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        pass

    def _revert_construct_connections(
        self, construct_conns: Set["_PendingConnRequest"], prev_construct_conns: Set["_PendingConnRequest"]
    ) -> None:
        pass

    def _revert_internal_signals(self, signals: Set[Signal], prev_signals: Set[Signal]) -> None:
        pass

    def _revert_security_conf(self, security_conf: ConstructSecurityConf, prev_security_conf: ConstructSecurityConf) -> None:
        pass

    @staticmethod
    def runtime_bootstrap() -> "RuntimePlatform":
        """Bootstrap runtime platform from S3 stored serialization."""
        bootstrapper_bucket = os.environ["bootstrapper_bucket"]
        bootstrapper_key = os.environ["bootstrapper_key"]
        _s3 = boto3.resource("s3")
        _bucket = get_bucket(_s3, bootstrapper_bucket)
        serialized_bootstrapper = get_object(_bucket, bootstrapper_key).decode()

        from intelliflow.core.platform.development import RuntimePlatform

        runtime_platform: RuntimePlatform = RuntimePlatform.deserialize(serialized_bootstrapper)
        runtime_platform.runtime_init(runtime_platform.processor)
        return runtime_platform

    @staticmethod
    def lambda_handler(event, context):
        """Lambda handler for control plane API requests with large response handling."""
        import traceback

        from intelliflow._logging_config import init_basic_logging

        try:
            init_basic_logging(None, False, logging.INFO)

            # Bootstrap runtime platform
            runtime_platform = AWSLambdaControlPlaneExtension.runtime_bootstrap()

            # Extract API path and parameters from event
            http_method = event.get("httpMethod", "GET")
            path = event.get("path", "")
            query_string_parameters = event.get("queryStringParameters") or {}

            # Get platform parameters for S3 bucket and context info
            platform_params = runtime_platform.processor._params
            context_id = platform_params.get("ActivationParams.CONTEXT_ID", "unknown")
            s3_bucket = get_s3_bucket_for_large_responses(runtime_platform)

            # Route to appropriate control API function
            response_body = route_api_request(runtime_platform, path, query_string_parameters)

            # Handle potentially large responses
            if s3_bucket:
                try:
                    response_result = handle_large_response(response_body, s3_bucket, path, context_id)

                    # Check response type and handle accordingly
                    if response_result["type"] == "direct":
                        # Response is small enough, return directly
                        return {
                            "statusCode": 200,
                            "headers": {
                                "Content-Type": "application/json",
                                "Access-Control-Allow-Origin": "*",
                                "Access-Control-Allow-Headers": "Content-Type",
                                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                                "X-Response-Size": str(response_result["size_bytes"]),
                            },
                            "body": intelliflow_jsonify(response_result["data"]),
                        }
                    else:
                        # Response is stored in S3
                        return {
                            "statusCode": 200,
                            "headers": {
                                "Content-Type": "application/json",
                                "Access-Control-Allow-Origin": "*",
                                "Access-Control-Allow-Headers": "Content-Type",
                                "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                                "X-Response-Type": response_result["type"],
                                "X-Response-Size": str(response_result.get("size_bytes", 0)),
                            },
                            "body": intelliflow_jsonify(response_result),
                        }

                except Exception as large_response_error:
                    module_logger.error(f"Large response handling failed: {str(large_response_error)}")
                    # Re-raise the error instead of falling back to truncation
                    raise large_response_error
            else:
                # No S3 bucket available, use direct response (may fail if too large)
                module_logger.warning("No S3 bucket available for large response handling")
                return {
                    "statusCode": 200,
                    "headers": {
                        "Content-Type": "application/json",
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Headers": "Content-Type",
                        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                    },
                    "body": intelliflow_jsonify(response_body),
                }

        except Exception as error:
            module_logger.error(f"Control plane Lambda error: {str(error)}")
            traceback.print_exc()

            # Enhanced error response with diagnostic information
            error_details = {
                "error": str(error),
                "error_type": error.__class__.__name__,
                "message": "Internal server error",
                "path": path if "path" in locals() else "unknown",
                "diagnostic_info": {},
            }

            # Add specific diagnostic information based on error type
            if isinstance(error, TypeError) and "not JSON serializable" in str(error):
                # JSON serialization error - provide detailed diagnostic
                error_details.update(
                    {
                        "error_category": "SERIALIZATION_ERROR",
                        "diagnostic_info": {
                            "issue": "Response contains non-serializable objects",
                            "technical_details": f"Object type causing issue: {str(error)}",
                            "solution": "Objects like Enum, datetime, or custom classes need custom serialization",
                            "hint": "Check if new data types were added to the response that aren't handled by the JSON encoder",
                        },
                    }
                )
                module_logger.error(f"JSON Serialization Error detected for path {path if 'path' in locals() else 'unknown'}: {str(error)}")

            elif isinstance(error, ValueError):
                # Application/validation error
                error_details.update(
                    {
                        "error_category": "VALIDATION_ERROR",
                        "diagnostic_info": {
                            "issue": "Invalid input parameters or application state",
                            "solution": "Check API parameters and application configuration",
                            "parameters": query_string_parameters if "query_string_parameters" in locals() else {},
                        },
                    }
                )

            elif isinstance(error, AttributeError):
                # Missing attribute/method error
                error_details.update(
                    {
                        "error_category": "ATTRIBUTE_ERROR",
                        "diagnostic_info": {
                            "issue": "Missing expected attribute or method on object",
                            "technical_details": str(error),
                            "solution": "Check if application state or object structure has changed",
                        },
                    }
                )

            elif "bootstrap" in str(error).lower():
                # Bootstrap/initialization error
                error_details.update(
                    {
                        "error_category": "INITIALIZATION_ERROR",
                        "diagnostic_info": {
                            "issue": "Runtime platform bootstrap failure",
                            "solution": "Check application deployment status and retry",
                        },
                    }
                )

            elif "413" in str(error) or "payload too large" in str(error).lower():
                # Payload too large error
                error_details.update(
                    {
                        "error_category": "PAYLOAD_TOO_LARGE",
                        "diagnostic_info": {
                            "issue": "Lambda response exceeded 6MB limit",
                            "solution": "Response is too large and S3 fallback is not available or failed",
                            "technical_details": "AWS Lambda synchronous invocations have a 6MB response size limit",
                        },
                    }
                )

            else:
                # Generic system error
                error_details.update(
                    {
                        "error_category": "SYSTEM_ERROR",
                        "diagnostic_info": {
                            "issue": "Unexpected system error occurred",
                            "solution": "Check CloudWatch logs for detailed stack trace",
                        },
                    }
                )

            # Add stack trace for debugging (but truncated for response size)
            stack_trace = traceback.format_exc()
            error_details["stack_trace_preview"] = stack_trace.split("\n")[-10:]  # Last 10 lines

            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
                "body": intelliflow_jsonify(error_details),
            }


def cleanup_expired_s3_responses(s3_bucket: str, context_id: str) -> None:
    """
    Clean up expired S3 response objects to prevent storage buildup.

    Args:
        s3_bucket: S3 bucket name
        context_id: Context ID to scope the cleanup
    """
    try:
        s3 = boto3.resource("s3")
        bucket = get_bucket(s3, s3_bucket)

        # List objects in the large_responses folder for this context
        prefix = f"extension_data/AWSLambdaControlPlaneExtension/large_responses/{context_id}_"

        current_time = datetime.utcnow()
        objects_to_delete = []

        for obj in bucket.objects.filter(Prefix=prefix):
            try:
                # Check object metadata for expiration
                metadata = obj.metadata or {}
                expiration_str = metadata.get("expiration")

                if expiration_str:
                    expiration_time = datetime.fromisoformat(expiration_str.replace("Z", "+00:00").replace("+00:00", ""))
                    if current_time > expiration_time:
                        objects_to_delete.append(obj.key)
                else:
                    # If no expiration metadata, check object age (default 24h cleanup)
                    if current_time - obj.last_modified.replace(tzinfo=None) > timedelta(hours=24):
                        objects_to_delete.append(obj.key)

            except Exception as e:
                module_logger.warning(f"Failed to check expiration for S3 object {obj.key}: {str(e)}")
                continue

        # Delete expired objects in batches
        if objects_to_delete:
            for i in range(0, len(objects_to_delete), 1000):  # S3 delete limit is 1000 per batch
                batch = objects_to_delete[i : i + 1000]
                try:
                    bucket.delete_objects(Delete={"Objects": [{"Key": key} for key in batch]})
                    module_logger.info(f"Deleted {len(batch)} expired S3 response objects")
                except Exception as e:
                    module_logger.warning(f"Failed to delete S3 objects batch: {str(e)}")

    except Exception as e:
        module_logger.warning(f"S3 cleanup failed: {str(e)}")


def route_api_request(runtime_platform, path: str, params: Dict[str, str]):
    """Route API requests to appropriate control API functions."""
    from intelliflow.api_ext import AWSApplication
    from intelliflow.control import api as control_api
    from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
    from intelliflow.core.platform.definitions.common import ActivationParams

    # Extract parameters from runtime platform
    platform_params = runtime_platform.processor._params
    context_id = platform_params[ActivationParams.CONTEXT_ID]
    account_id = platform_params[AWSCommonParams.ACCOUNT_ID]
    region = platform_params[AWSCommonParams.REGION]

    # Cleanup expired S3 responses periodically (10% chance to avoid overhead)
    import random

    if random.random() < 0.1:  # 10% chance
        s3_bucket = get_s3_bucket_for_large_responses(runtime_platform)
        if s3_bucket:
            try:
                cleanup_expired_s3_responses(s3_bucket, context_id)
            except Exception as e:
                module_logger.warning(f"S3 cleanup failed: {str(e)}")

    # Create AWSApplication with use_activated_platform_params parameter
    app = AWSApplication(
        app_name=context_id,
        region_or_platform=region,
        dev_role_account_id=account_id,
        use_activated_platform_params=True,  # New parameter, ineffective for now
    )

    if path == "/intelli-flow/api/getNodesSummary":
        return control_api.get_nodes_summary(app)

    elif path == "/intelli-flow/api/getNodesExecSummary":
        return control_api.get_nodes_exec_summary(app)

    elif path == "/intelli-flow/api/getNodesExecDimensionsSummary":
        limit = int(params.get("limit", 300))
        return control_api.get_nodes_exec_dimensions_summary(app, limit)

    elif path == "/intelli-flow/api/getNodesFilteredExecSummary":
        dimension_value_map_str = params.get("dimensionValueMap", "")
        dimension_value_map = {}
        if dimension_value_map_str:
            dimension_value_map_array = dimension_value_map_str.split("@_:dim@")
            for value in dimension_value_map_array:
                dim_key_value_pair = value.split("=", 1)
                key = dim_key_value_pair[0]
                values_str = dim_key_value_pair[1]
                values = values_str.split("@_:value@")
                dimension_value_map[key] = values

        state_type_str = params.get("state_type", None)
        return control_api.get_nodes_filtered_exec_summary(app, dimension_value_map, state_type_str)

    elif path == "/intelli-flow/api/getNodesList":
        data = control_api.compute_node_list(app)
        return {"nodes": data}

    elif path == "/intelli-flow/api/getApplicationState":
        app_state = app._state if hasattr(app, "_state") else "UNKNOWN"
        app_id = app.id if hasattr(app, "id") else "unknown"
        app_uuid = app.uuid if hasattr(app, "uuid") else "unknown"
        return {"app_state": app_state.name if hasattr(app_state, "name") else str(app_state), "app_id": app_id, "app_uuid": app_uuid}

    elif path == "/intelli-flow/api/getActiveNodeState":
        node_id = params.get("nodeId")
        if not node_id:
            raise ValueError("nodeId parameter is required")
        return control_api.get_active_node_state(app, node_id)

    elif path == "/intelli-flow/api/getInactiveNodeState":
        node_id = params.get("nodeId")
        if not node_id:
            raise ValueError("nodeId parameter is required")
        limit = int(params.get("limit", 200))
        return control_api.get_inactive_node_state(app, node_id, limit)

    elif path == "/intelli-flow/api/getCloudWatchLogs":
        node_id = params.get("nodeId")
        if not node_id:
            raise ValueError("nodeId parameter is required")
        dimension_value_map_str = params.get("dimensionValueMap", "")
        dimension_values = dimension_value_map_str.split(",") if dimension_value_map_str else None
        return control_api.get_cloud_watch_logs(app, node_id, dimension_values)

    elif path == "/intelli-flow/api/getExecutionDiagnostics":
        node_id = params.get("nodeId")
        if not node_id:
            raise ValueError("nodeId parameter is required")
        dimension_value_map_str = params.get("dimensionValueMap", "")
        dimension_values = dimension_value_map_str.split(",") if dimension_value_map_str else None
        filter_pattern = params.get("filterPattern", None)
        limit = int(params.get("limit", 1000))
        return control_api.get_execution_diagnostics(app, node_id, dimension_values, filter_pattern, limit)

    elif path == "/intelli-flow/api/createExecution":
        node_id = params.get("nodeId")
        if not node_id:
            raise ValueError("nodeId parameter is required")
        dimension_value_map_str = params.get("dimensionValueMap", "")
        dimension_values = dimension_value_map_str.split(",") if dimension_value_map_str else None
        wait = params.get("wait", "true").lower() == "true"
        recursive = params.get("recursive", "true").lower() == "true"
        update_tree = params.get("updateTree", "true").lower() == "true"
        return control_api.create_execution(app, node_id, dimension_values, wait, recursive, update_tree)

    elif path == "/intelli-flow/api/killExecution":
        node_id = params.get("nodeId")
        if not node_id:
            raise ValueError("nodeId parameter is required")
        dimension_value_map_str = params.get("dimensionValueMap", "")
        dimension_values = dimension_value_map_str.split(",") if dimension_value_map_str else None
        return control_api.kill_execution(app, node_id, dimension_values)

    elif path == "/intelli-flow/api/deletePending":
        node_id = params.get("nodeId")
        pending_node_id = params.get("pendingNodeId")
        if not node_id or not pending_node_id:
            raise ValueError("nodeId and pendingNodeId parameters are required")
        return control_api.delete_pending(app, node_id, pending_node_id)

    elif path == "/intelli-flow/api/loadData":
        node_id = params.get("nodeId")
        if not node_id:
            raise ValueError("nodeId parameter is required")
        dimension_value_map_str = params.get("dimensionValueMap", "")
        dimension_values = dimension_value_map_str.split(",") if dimension_value_map_str else None
        limit = int(params.get("limit", 10))
        return control_api.load_data(app, node_id, dimension_values, limit)

    elif path == "/intelli-flow/api/refresh":
        return control_api.refresh_app(app)

    elif path == "/ping":
        return {"message": "pong"}

    else:
        raise ValueError(f"Unknown API path: {path}")


# Export the Lambda handler for external reference
lambda_handler = AWSLambdaControlPlaneExtension.lambda_handler

# High-level syntactic sugar class for user-facing API (similar to ManagementAPI mentioned in design doc)
ManagementAPI = AWSLambdaControlPlaneExtension.Descriptor
