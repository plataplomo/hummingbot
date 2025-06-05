"""Serialization utilities for CyberDeltaEngine.

This module provides custom JSON encoding and decoding functionality
for handling Decimal types and other CyberDelta-specific data structures.
"""

import json
from datetime import datetime
from decimal import Decimal

import numpy as np
from pydantic import BaseModel

# Type alias for JSON-compatible values
JSONValue = str | int | float | bool | None | dict[str, "JSONValue"] | list["JSONValue"]


class CyberDeltaJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for CyberDeltaEngine data types.

    Handles serialization of Decimal and other types that aren't natively
    supported by the standard JSON encoder.
    """

    def default(self, o: object) -> JSONValue:
        """Serialize object to JSON-compatible type.

        Args:
            o: The object to serialize.

        Returns:
            JSON-serializable representation of the object.

        Raises:
            TypeError: If the object type is not serializable.

        """
        if isinstance(o, Decimal):
            # Convert Decimal to string to preserve precision
            return str(o)
        if isinstance(o, datetime):
            # Convert datetime to ISO 8601 format string
            return o.isoformat()
        if isinstance(o, np.integer):
            # Convert numpy integer to standard Python int using .item()
            return o.item()
        if isinstance(o, np.floating):
            # Convert numpy float to standard Python float using .item()
            return o.item()
        # Handle Pydantic models
        if isinstance(o, BaseModel):
            # Use Pydantic's built-in serialization
            result = o.model_dump(mode="json")
            # Ensure the result is a valid JSONValue
            return result
        # Let the base class default method raise the TypeError for other types
        return super().default(o)


# Helper function to easily dump JSON with the custom encoder
def dump_json(data: object, **kwargs: object) -> str:
    """Dump data to JSON string using the custom CyberDeltaJSONEncoder.

    Args:
        data: Data to serialize to JSON
        **kwargs: Additional keyword arguments for json.dumps

    Returns:
        JSON string representation of the data

    """
    # Convert kwargs to any type to avoid mypy issues
    return json.dumps(data, cls=CyberDeltaJSONEncoder, **kwargs)  # type: ignore[misc]


# Optionally, a helper to load JSON (though standard json.loads often works fine
# unless specific object hooks are needed for complex deserialization)
def load_json(json_str: str, **kwargs: object) -> JSONValue:
    """Load data from JSON string.

    Args:
        json_str: JSON string to parse
        **kwargs: Additional keyword arguments for json.loads

    Returns:
        Parsed JSON data structure

    """
    # Convert kwargs to any type to avoid mypy issues
    result = json.loads(json_str, **kwargs)  # type: ignore[misc]
    return result  # type: ignore[return-value]
