"""
JSON utility module providing Flask-compatible JSON serialization for the control plane.

This module provides a minimalist replacement for Flask's jsonify functionality,
specifically designed to handle datetime serialization issues in the IntelliFlow
Lambda backend that previously relied on Flask's automatic datetime conversion.
"""

import json
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, Union


class IntelliFlowJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that mimics Flask's jsonify behavior for datetime objects.

    This encoder handles the most common non-serializable Python objects that
    Flask's jsonify would automatically convert, particularly datetime and date
    objects which are converted to ISO format strings.
    """

    def default(self, obj: Any) -> Any:
        """
        Convert non-serializable objects to JSON-serializable formats.

        Args:
            obj: The object to serialize

        Returns:
            JSON-serializable representation of the object

        Raises:
            TypeError: If the object cannot be serialized
        """
        if isinstance(obj, datetime):
            # Flask's default: ISO format with 'T' separator
            # Example: datetime(2023, 12, 25, 14, 30) -> "2023-12-25T14:30:00"
            return obj.isoformat()
        elif isinstance(obj, date):
            # Convert date to ISO format string
            # Example: date(2023, 12, 25) -> "2023-12-25"
            return obj.isoformat()
        elif isinstance(obj, Enum):
            # Handle Enum objects (like SlotType, SignalType, etc.) by returning their value
            # This is what Flask's jsonify does with enums
            # Example: SlotType.SYNC_INLINED -> 1, ComputeResponseType.SUCCESS -> "SUCCESS"
            return obj.value
        elif isinstance(obj, Decimal):
            # Handle Decimal objects commonly used in financial data
            return float(obj)
        elif hasattr(obj, "__dict__") and not isinstance(obj, type):
            # Handle objects with __dict__ (custom class instances)
            # Avoid classes themselves (type objects)
            return obj.__dict__
        else:
            # Fallback to repr for other non-serializable objects
            # This maintains compatibility with the current default=repr behavior
            return repr(obj)


def intelliflow_jsonify(obj: Any, **kwargs: Any) -> str:
    """
    Minimalist replacement for Flask's jsonify that handles datetime serialization.

    This function provides Flask-compatible JSON serialization, specifically
    addressing the datetime/date serialization issues that occur when moving
    from Flask's jsonify to standard json.dumps.

    Args:
        obj: Object to serialize to JSON
        **kwargs: Additional arguments passed to json.dumps
                 (e.g., indent, ensure_ascii, etc.)

    Returns:
        JSON string with proper datetime serialization

    Example:
        >>> from datetime import datetime
        >>> data = {"timestamp": datetime(2023, 12, 25, 14, 30)}
        >>> intelliflow_jsonify(data)
        '{"timestamp": "2023-12-25T14:30:00"}'

        >>> intelliflow_jsonify(data, indent=2)
        '{\\n  "timestamp": "2023-12-25T14:30:00"\\n}'
    """
    # Set default kwargs that match Flask's behavior
    default_kwargs = {"ensure_ascii": False, "cls": IntelliFlowJSONEncoder, "sort_keys": False}  # Flask's default

    # Allow user kwargs to override defaults
    default_kwargs.update(kwargs)

    return json.dumps(obj, **default_kwargs)


def get_flask_compatible_datetime_format(dt: Union[datetime, date]) -> str:
    """
    Helper function to get Flask-compatible datetime format.

    Args:
        dt: datetime or date object

    Returns:
        ISO format string compatible with Flask's jsonify

    Example:
        >>> from datetime import datetime, date
        >>> get_flask_compatible_datetime_format(datetime(2023, 12, 25, 14, 30))
        '2023-12-25T14:30:00'
        >>> get_flask_compatible_datetime_format(date(2023, 12, 25))
        '2023-12-25'
    """
    return dt.isoformat()


# Export public interface
__all__ = ["intelliflow_jsonify", "IntelliFlowJSONEncoder", "get_flask_compatible_datetime_format"]
