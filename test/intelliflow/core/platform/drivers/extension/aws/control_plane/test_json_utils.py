#!/usr/bin/env python3
"""
Test module for JSON utils datetime compatibility.

This module tests that our intelliflow_jsonify function produces the same datetime
serialization format as Flask's jsonify, ensuring frontend compatibility.
"""

import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal

from intelliflow.core.platform.drivers.extension.aws.control_plane.json_utils import IntelliFlowJSONEncoder, intelliflow_jsonify

# Add the src directory to the Python path for testing
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../../../../src"))


class TestIntelliFlowJSONUtils:
    """Test class for IntelliFlow JSON utilities."""

    def test_datetime_serialization(self):
        """Test datetime object serialization"""
        test_datetime = datetime(2023, 12, 25, 14, 30, 0)

        # Test our implementation
        result = intelliflow_jsonify({"timestamp": test_datetime})
        expected = '{"timestamp": "2023-12-25T14:30:00"}'

        assert result == expected

    def test_date_serialization(self):
        """Test date object serialization"""
        test_date = date(2023, 12, 25)

        # Test our implementation
        result = intelliflow_jsonify({"date": test_date})
        expected = '{"date": "2023-12-25"}'

        assert result == expected

    def test_mixed_data_serialization(self):
        """Test complex data structure with datetime objects"""
        complex_data = {
            "app_id": "test-app",
            "timestamp": datetime(2023, 12, 25, 14, 30, 0),
            "date": date(2023, 12, 25),
            "nodes": [
                {"node_id": "node_1", "created_at": datetime(2023, 12, 24, 10, 15, 30), "status": "active"},
                {"node_id": "node_2", "created_at": datetime(2023, 12, 25, 9, 45, 0), "status": "pending"},
            ],
            "metadata": {"last_updated": datetime(2023, 12, 25, 14, 30, 0), "version": "1.0.0"},
        }

        # Test our implementation
        result = intelliflow_jsonify(complex_data, indent=2)

        # Verify all datetime objects are properly serialized
        assert '"2023-12-25T14:30:00"' in result
        assert '"2023-12-25"' in result
        assert '"2023-12-24T10:15:30"' in result
        assert '"2023-12-25T09:45:00"' in result

    def test_backwards_compatibility(self):
        """Test that non-datetime objects still work as expected"""
        regular_data = {
            "string": "hello",
            "number": 42,
            "float": 3.14159,
            "boolean": True,
            "null": None,
            "list": [1, 2, 3],
            "nested": {"key": "value"},
        }

        # Test our implementation
        our_result = intelliflow_jsonify(regular_data)

        # Test standard json.dumps
        standard_result = json.dumps(regular_data)

        assert our_result == standard_result

    def test_decimal_serialization(self):
        """Test Decimal object serialization"""
        test_data = {"price": Decimal("19.99"), "tax": Decimal("1.60")}

        result = intelliflow_jsonify(test_data)
        expected = '{"price": 19.99, "tax": 1.6}'

        assert result == expected

    def test_encoder_datetime_handling(self):
        """Test the encoder class directly"""
        encoder = IntelliFlowJSONEncoder()

        # Test datetime encoding
        test_datetime = datetime(2023, 12, 25, 14, 30, 0)
        result = encoder.default(test_datetime)
        assert result == "2023-12-25T14:30:00"

        # Test date encoding
        test_date = date(2023, 12, 25)
        result = encoder.default(test_date)
        assert result == "2023-12-25"

    def test_compare_with_standard_json_dumps(self):
        """Compare behavior with standard json.dumps using default=repr"""
        test_datetime = datetime(2023, 12, 25, 14, 30, 0)
        test_data = {"timestamp": test_datetime}

        # Current Lambda behavior (problematic)
        current_result = json.dumps(test_data, default=repr)

        # Our new behavior
        new_result = intelliflow_jsonify(test_data)

        # Verify they are different and ours is better
        assert current_result != new_result
        assert '"2023-12-25T14:30:00"' in new_result
        assert "datetime.datetime" in current_result

    def test_lambda_response_simulation(self):
        """Simulate actual Lambda API response format"""
        # Sample response data that might contain datetime objects
        response_data = {
            "nodes": [
                {
                    "id": "eureka_training_data",
                    "node_type": "INTERNAL_DATA_NODE",
                    "execution_data": {
                        "activeRecords": [
                            {"trigger_timestamp": datetime(2023, 12, 25, 14, 30, 0), "deactivated_timestamp": None, "state": "COMPLETED"}
                        ],
                        "inactiveRecords": [
                            {
                                "trigger_timestamp": datetime(2023, 12, 24, 10, 15, 30),
                                "deactivated_timestamp": datetime(2023, 12, 24, 11, 45, 0),
                                "state": "COMPLETED",
                            }
                        ],
                    },
                }
            ],
            "app_state": "ACTIVE",
            "last_updated": datetime(2023, 12, 25, 14, 30, 0),
        }

        # Format as Lambda would
        lambda_response = {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": intelliflow_jsonify(response_data, indent=4),
        }

        # Verify frontend can parse this
        parsed = json.loads(lambda_response["body"])

        # Verify datetime fields are strings
        assert isinstance(parsed["last_updated"], str)
        assert isinstance(parsed["nodes"][0]["execution_data"]["activeRecords"][0]["trigger_timestamp"], str)

    def test_indent_parameter(self):
        """Test that indent parameter works correctly"""
        test_data = {"timestamp": datetime(2023, 12, 25, 14, 30, 0)}

        # Test with indent
        result_with_indent = intelliflow_jsonify(test_data, indent=2)
        result_without_indent = intelliflow_jsonify(test_data)

        # Should be different formats but same data
        assert result_with_indent != result_without_indent
        assert '"2023-12-25T14:30:00"' in result_with_indent
        assert '"2023-12-25T14:30:00"' in result_without_indent

    def test_custom_class_serialization(self):
        """Test serialization of custom classes with __dict__"""

        class TestClass:
            def __init__(self):
                self.name = "test"
                self.timestamp = datetime(2023, 12, 25, 14, 30, 0)

        test_obj = TestClass()
        test_data = {"obj": test_obj}

        result = intelliflow_jsonify(test_data)

        # Should serialize the __dict__ of the object
        assert '"name": "test"' in result
        assert '"2023-12-25T14:30:00"' in result
