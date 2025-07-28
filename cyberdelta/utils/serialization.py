"""Serialization utilities for CyberDeltaEngine.

This module provides custom JSON encoding and decoding functionality
for handling Decimal types and other CyberDelta-specific data structures.
"""

import json
from datetime import datetime
from decimal import Decimal
from typing import Any

import numpy as np
from pydantic import BaseModel


# Type alias for JSON-compatible values
JSONValue = str | int | float | bool | dict[str, "JSONValue"] | list["JSONValue"] | None


class JSONSerializationError(TypeError):
    """Raised when an object cannot be serialized to JSON."""

    def __init__(self, obj_type: type) -> None:
        """Initialize JSON serialization error.

        Args:
            obj_type: The type of object that failed serialization
        """
        self.obj_type = obj_type
        message = f"Object of type {obj_type.__name__} is not JSON serializable"
        super().__init__(message)


class CyberDeltaJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for CyberDeltaEngine data types.

    Handles serialization of Decimal and other types that aren't natively
    supported by the standard JSON encoder.
    """

    def default(self, o: object) -> str | int | float | dict[str, Any]:
        """Serialize object to JSON-compatible type.

        Args:
            o: The object to serialize.

        Returns:
            JSON-serializable representation of the object.

        Raises:
            JSONSerializationError: If the object type is not serializable.

        """
        if isinstance(o, Decimal):
            # Convert Decimal to string to preserve precision
            return str(o)
        if isinstance(o, datetime):
            # Convert datetime to ISO 8601 format string
            return o.isoformat()
        if isinstance(o, np.integer):
            # Convert numpy integer to standard Python int using .item()
            return int(o.item())
        if isinstance(o, np.floating):
            # Convert numpy float to standard Python float using .item()
            return float(o.item())
        # Handle Pydantic models
        if isinstance(o, BaseModel):
            # Use Pydantic's built-in serialization
            return o.model_dump(mode="json")
        # Let the base class default method raise the TypeError for other types
        # Note: super().default(o) returns Any but we need to handle this
        # Since this is a fallback for unknown types, we'll raise TypeError explicitly
        raise JSONSerializationError(type(o))


# Helper function to easily dump JSON with the custom encoder
def dump_json(
    data: object,
    *,
    skipkeys: bool = False,
    ensure_ascii: bool = True,
    check_circular: bool = True,
    allow_nan: bool = True,
    indent: int | str | None = None,
    separators: tuple[str, str] | None = None,
    sort_keys: bool = False,
) -> str:
    """Dump data to JSON string using the custom CyberDeltaJSONEncoder.

    Args:
        data: Data to serialize to JSON
        skipkeys: Skip keys that are not basic types
        ensure_ascii: Ensure output is ASCII
        check_circular: Check for circular references
        allow_nan: Allow NaN values
        indent: Indentation for pretty printing
        separators: Separators for JSON output
        default: Default function for non-serializable objects
        sort_keys: Sort dictionary keys

    Returns:
        JSON string representation of the data

    """
    # Use the custom encoder with explicit parameters
    return json.dumps(
        data,
        cls=CyberDeltaJSONEncoder,
        skipkeys=skipkeys,
        ensure_ascii=ensure_ascii,
        check_circular=check_circular,
        allow_nan=allow_nan,
        indent=indent,
        separators=separators,
        sort_keys=sort_keys,
    )


# Optionally, a helper to load JSON (though standard json.loads often works fine
# unless specific object hooks are needed for complex deserialization)
def load_json(json_str: str) -> object:
    """Load data from JSON string.

    Args:
        json_str: JSON string to parse
        **kwargs: Additional keyword arguments for json.loads

    Returns:
        Parsed JSON data structure

    """
    # Load JSON with standard handling
    return json.loads(json_str)
