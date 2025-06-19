"""Hyperliquid-specific payload serialization strategy.

This module provides the serialization strategy for Hyperliquid exchange,
which requires special handling of field names during serialization.
"""

from typing import Any

from pydantic import BaseModel


class HyperliquidSerializationStrategy:
    """Hyperliquid-specific serialization strategy.

    Hyperliquid requires by_alias=False because the model field names
    (a, b, p, etc.) are the actual API field names, while aliases
    are used for developer convenience.
    """

    def serialize_model(self, model: BaseModel, serialize_none_as_null: bool) -> dict[str, Any]:
        """Serialize model using Hyperliquid's requirements.

        Args:
            model: The Pydantic model to serialize
            serialize_none_as_null: If True, include None values as null

        Returns:
            Dictionary with original field names (not aliases)
        """
        return model.model_dump(
            by_alias=False,  # Critical for Hyperliquid
            exclude_none=not serialize_none_as_null,
        )
