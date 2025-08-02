"""State serialization service with type safety."""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import TypeVar

from pydantic import BaseModel

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.models.base import StateWrapper
from cyberdelta.core.data_management.persistence.persistence_models import (
    TypedStatePersistenceError,
)


logger = get_logger(__name__)

# Generic type variable bound to BaseModel
T = TypeVar("T", bound=BaseModel)


class StateSerializer(ABC):
    """Abstract base class for type-safe state serializers."""

    @abstractmethod
    def serialize(self, wrapper: StateWrapper[T]) -> bytes:
        """Serialize StateWrapper to bytes with full type safety.
        
        Args:
            wrapper: StateWrapper containing typed data to serialize.
            
        Returns:
            Serialized bytes representation of the state wrapper.
        """
        ...

    @abstractmethod
    def deserialize(self, data: bytes, model_class: type[T]) -> StateWrapper[T]:
        """Deserialize bytes to StateWrapper with type preservation.
        
        Args:
            data: Serialized bytes to deserialize.
            model_class: Expected model class for type safety.
            
        Returns:
            StateWrapper with preserved type information.
        """
        ...

    @abstractmethod
    def get_extension(self) -> str:
        """Get file extension for this serializer.
        
        Returns:
            File extension string for this serializer type.
        """
        ...


class PydanticJSONSerializer:
    """Type-safe JSON serializer using Pydantic."""

    def serialize(self, wrapper: StateWrapper[T]) -> bytes:
        """Serialize StateWrapper using Pydantic JSON serialization.

        Args:
            wrapper: StateWrapper containing typed data

        Returns:
            JSON bytes with full type safety

        Raises:
            TypedStatePersistenceError: If serialization fails
        """
        try:
            # Pydantic handles all serialization automatically
            json_str = wrapper.model_dump_json(indent=2)
            return json_str.encode("utf-8")
        except Exception as e:
            raise TypedStatePersistenceError("serialize", wrapper.state_id, e) from e

    def deserialize(self, data: bytes, model_class: type[T]) -> StateWrapper[T]:
        """Deserialize bytes to typed StateWrapper.

        Args:
            data: JSON bytes to deserialize
            model_class: Expected model class for type safety

        Returns:
            StateWrapper[T] with preserved type information

        Raises:
            TypedStatePersistenceError: If deserialization fails
        """
        try:
            json_str = data.decode("utf-8")
            json_data = json.loads(json_str)

            # Reconstruct StateWrapper with proper typing
            return StateWrapper[T](
                state_id=json_data["state_id"],
                timestamp=json_data["timestamp"],
                datetime_iso=json_data["datetime_iso"],
                metadata=json_data["metadata"],
                data=model_class.model_validate(json_data["data"]),
            )
        except Exception as e:
            state_id = "unknown"
            try:
                json_str = data.decode("utf-8")
                json_data = json.loads(json_str)
                state_id = json_data.get("state_id", "unknown")
            except (json.JSONDecodeError, UnicodeDecodeError, AttributeError):
                logger.debug("Failed to extract state_id from corrupted data")
            raise TypedStatePersistenceError("deserialize", state_id, e) from e

    def get_extension(self) -> str:
        """Get file extension.

        Returns:
            File extension string for JSON files
        """
        return ".json"