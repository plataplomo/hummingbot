import json
from datetime import datetime
from decimal import Decimal
from typing import Any


class CyberDeltaJSONEncoder(json.JSONEncoder):
    """Custom JSON Encoder for CyberDeltaEngine objects.

    Handles:
    - Decimal objects (converts to string)
    - datetime objects (converts to ISO 8601 string)
    """

    def default(self, obj: Any) -> str | Any:  # noqa: ANN401
        if isinstance(obj, Decimal):
            # Convert Decimal to string to preserve precision
            return str(obj)
        if isinstance(obj, datetime):
            # Convert datetime to ISO 8601 format string
            return obj.isoformat()
        # Let the base class default method raise the TypeError for other types
        return super().default(obj)


# Helper function to easily dump JSON with the custom encoder
def dump_json(data: Any, **kwargs: Any) -> str:  # noqa: ANN401
    """Dump data to JSON string using the custom CyberDeltaJSONEncoder."""
    return json.dumps(data, cls=CyberDeltaJSONEncoder, **kwargs)


# Optionally, a helper to load JSON (though standard json.loads often works fine
# unless specific object hooks are needed for complex deserialization)
def load_json(json_str: str, **kwargs: Any) -> Any:  # noqa: ANN401
    """Load data from JSON string."""
    return json.loads(json_str, **kwargs)
