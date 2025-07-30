"""Payload serialization strategy interface and implementations.

This module provides a strategy pattern for handling exchange-specific
serialization requirements when converting Pydantic models to dictionaries
for API requests.
"""

from typing import Any, Protocol

from pydantic import BaseModel

from cyberdelta.apis.base.infrastructure_config_domain import RequestConfiguration


class PayloadSerializationStrategy(Protocol):
    """Protocol defining the interface for payload serialization strategies.

    Each exchange can implement its own strategy to handle specific
    serialization requirements (e.g., by_alias behavior, null handling).
    """

    def serialize_model(
        self,
        model: BaseModel,
        request_config: RequestConfiguration,
    ) -> dict[str, Any]:
        """Serialize a Pydantic model according to exchange requirements.

        Args:
            model: The Pydantic model to serialize
            request_config: Request configuration including serialization settings

        Returns:
            Dictionary representation of the model suitable for the exchange's API
        """
        ...


class DefaultSerializationStrategy:
    """Default serialization strategy that maintains backward compatibility.

    This strategy uses by_alias=True which is the standard behavior
    for most exchanges like Backpack.
    """

    def serialize_model(
        self,
        model: BaseModel,
        request_config: RequestConfiguration,
    ) -> dict[str, Any]:
        """Serialize model using standard Pydantic behavior with aliases.

        Args:
            model: The Pydantic model to serialize
            request_config: Request configuration including serialization settings

        Returns:
            Dictionary with aliased field names
        """
        return model.model_dump(
            by_alias=True,
            exclude_none=not request_config.serialization_mode.should_serialize_none,
        )
