"""Secure serialization utilities for portfolio services.

This module provides secure alternatives to pickle for internal data persistence.
"""

from __future__ import annotations

import json
from typing import Any, cast

from pydantic import BaseModel, Field


# Type alias for JSON-serializable data structures
type JsonValue = str | int | float | bool | None
type SerializableType = dict[str, JsonValue] | list[JsonValue] | JsonValue


class SerializedPortfolioData(BaseModel):
    """Model for serialized portfolio data."""

    balances: dict[str, Any] = Field(default_factory=dict)
    positions: dict[str, Any] = Field(default_factory=dict)
    orders: dict[str, Any] = Field(default_factory=dict)
    trades: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    timestamp: float = 0.0
    version: str = "1.0"


class SecureSerializer:
    """Secure serializer using JSON for internal data persistence."""

    @staticmethod
    def dumps(obj: SerializableType) -> bytes:
        """Serialize object to bytes using JSON.

        Args:
            obj: Object to serialize

        Returns:
            Serialized bytes
        """
        return json.dumps(obj, default=str, ensure_ascii=False).encode("utf-8")

    @staticmethod
    def loads(data: bytes) -> SerializableType:
        """Deserialize bytes to object using JSON.

        Args:
            data: Bytes to deserialize

        Returns:
            Deserialized object
        """
        # Parse JSON with explicit type annotation
        raw_data = data.decode("utf-8")
        parsed = json.loads(raw_data)

        # Type narrowing after isinstance check
        if isinstance(parsed, dict):
            typed_dict = cast(dict[Any, Any], parsed)  # type: ignore[redundant-cast]
            result: dict[str, JsonValue] = {}
            for k, v in typed_dict.items():
                if isinstance(v, (str, int, float, bool)) or v is None:
                    result[str(k)] = v
            return result

        if isinstance(parsed, list):
            typed_list = cast(list[Any], parsed)  # type: ignore[redundant-cast]
            result_list: list[JsonValue] = [
                item
                for item in typed_list
                if isinstance(item, (str, int, float, bool)) or item is None
            ]
            return result_list

        if isinstance(parsed, (str, int, float, bool)) or parsed is None:
            return parsed

        return str(parsed)

    @staticmethod
    def loads_portfolio_data(data: bytes) -> SerializedPortfolioData:
        """Deserialize portfolio-specific data with type safety.

        Args:
            data: Bytes to deserialize

        Returns:
            Typed portfolio data
        """
        parsed = json.loads(data.decode("utf-8"))
        return SerializedPortfolioData.model_validate(parsed)


# Compatibility interface matching pickle API
secure_serializer = SecureSerializer()
dumps = secure_serializer.dumps
loads = secure_serializer.loads
