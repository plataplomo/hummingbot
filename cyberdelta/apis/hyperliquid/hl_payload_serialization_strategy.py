"""Hyperliquid-specific payload serialization strategy.

This module provides the serialization strategy for Hyperliquid exchange,
which requires special handling of field names during serialization.
"""

from typing import Any

from pydantic import BaseModel

from cyberdelta.apis.base.infrastructure_config_domain import RequestConfiguration


class HyperliquidSerializationStrategy:
    """Hyperliquid-specific serialization strategy.

    Hyperliquid requires by_alias=False because the model field names
    (a, b, p, etc.) are the actual API field names, while aliases
    are used for developer convenience.
    """

    def serialize_model(
        self,
        model: BaseModel,
        request_config: RequestConfiguration,
    ) -> dict[str, Any]:
        """Serialize model using Hyperliquid's requirements.

        Args:
            model: The Pydantic model to serialize
            request_config: Request configuration including serialization settings

        Returns:
            Dictionary with original field names (not aliases)
        """
        # Determine if we should include None values based on serialization mode
        exclude_none = not request_config.serialization_mode.should_serialize_none

        # Special handling for funding history requests which require camelCase
        if model.__class__.__name__ == "HyperliquidRawFundingHistoryRequestPayload":
            return model.model_dump(
                by_alias=True,  # Use camelCase aliases for funding history
                exclude_none=exclude_none,
                mode="json",  # Use JSON mode for proper serialization of custom types
            )

        # For Hyperliquid, we use actual field names (not aliases) for most requests
        return model.model_dump(
            by_alias=False,  # Critical for Hyperliquid - use actual field names (a, b, p, etc.)
            exclude_none=exclude_none,  # Based on serialization mode
            mode="json",  # Use JSON mode for proper serialization of custom types
        )
