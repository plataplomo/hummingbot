"""Payload serialization strategy interface and implementations.

This module provides a strategy pattern for handling exchange-specific
serialization requirements when converting Pydantic models to dictionaries
for API requests.
"""

from typing import Any, Protocol

from pydantic import BaseModel


class PayloadSerializationStrategy(Protocol):
    """Protocol defining the interface for payload serialization strategies.
    
    Each exchange can implement its own strategy to handle specific
    serialization requirements (e.g., by_alias behavior, null handling).
    """

    def serialize_model(self, model: BaseModel, serialize_none_as_null: bool) -> dict[str, Any]:
        """Serialize a Pydantic model according to exchange requirements.
        
        Args:
            model: The Pydantic model to serialize
            serialize_none_as_null: If True, serialize None values as null instead of excluding them
            
        Returns:
            Dictionary representation of the model suitable for the exchange's API
        """
        ...


class DefaultSerializationStrategy:
    """Default serialization strategy that maintains backward compatibility.
    
    This strategy uses by_alias=True which is the standard behavior
    for most exchanges like Backpack.
    """

    def serialize_model(self, model: BaseModel, serialize_none_as_null: bool) -> dict[str, Any]:
        """Serialize model using standard Pydantic behavior with aliases.
        
        Args:
            model: The Pydantic model to serialize
            serialize_none_as_null: If True, include None values as null
            
        Returns:
            Dictionary with aliased field names
        """
        return model.model_dump(
            by_alias=True,
            exclude_none=not serialize_none_as_null,
        )