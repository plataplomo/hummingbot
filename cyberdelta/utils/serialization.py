import json
from datetime import datetime
from decimal import Decimal
from typing import Any

import numpy as np
from pydantic import BaseModel


class CyberDeltaJSONEncoder(json.JSONEncoder):
    """Custom JSON Encoder for CyberDeltaEngine objects.

    Handles:
    - Decimal objects (converts to string)
    - datetime objects (converts to ISO 8601 string)
    - numpy integer types (converts to standard int)
    - numpy float types (converts to standard float)
    - Pydantic models (uses Pydantic's built-in serialization)
    """

    def default(self, obj: object) -> str | float | int | dict[str, Any]:
        if isinstance(obj, Decimal):
            # Convert Decimal to string to preserve precision
            return str(obj)
        if isinstance(obj, datetime):
            # Convert datetime to ISO 8601 format string
            return obj.isoformat()
        if isinstance(obj, np.integer):
            # Convert numpy integer to standard Python int using .item()
            return obj.item()
        if isinstance(obj, np.floating):
            # Convert numpy float to standard Python float using .item()
            return obj.item()
        # Handle Pydantic models
        if isinstance(obj, BaseModel):
            # Use Pydantic's built-in serialization
            return obj.model_dump(mode="json")
        # Let the base class default method raise the TypeError for other types
        return super().default(obj)


# Helper function to easily dump JSON with the custom encoder
def dump_json(data: object, **kwargs: dict[str, Any]) -> str:
    """Dump data to JSON string using the custom CyberDeltaJSONEncoder."""
    return json.dumps(data, cls=CyberDeltaJSONEncoder, **kwargs)


# Optionally, a helper to load JSON (though standard json.loads often works fine
# unless specific object hooks are needed for complex deserialization)
def load_json(json_str: str, **kwargs: dict[str, Any]) -> Any:  # Any needed for JSON types
    """Load data from JSON string."""
    return json.loads(json_str, **kwargs)
