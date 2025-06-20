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
        # Special handling for funding history requests which require camelCase
        if model.__class__.__name__ == "HyperliquidRawFundingHistoryRequestPayload":
            return model.model_dump(
                by_alias=True,  # Use camelCase aliases for funding history
                exclude_none=True,  # Exclude None values
                mode="python",  # Ensure python types
            )

        # For Hyperliquid, we exclude None values to match SDK behavior
        # The 'c' field should not be included when it's None
        return model.model_dump(
            by_alias=False,  # Critical for Hyperliquid - use actual field names (a, b, p, etc.)
            exclude_none=True,  # Exclude None values to match SDK behavior
            mode="python",  # Ensure python types
        )
