"""Secure serialization utilities for portfolio services.

This module provides secure alternatives to pickle for internal data persistence.
"""

from __future__ import annotations

import json
import time
from decimal import Decimal

from pydantic import BaseModel, Field


# Type alias for JSON-serializable data structures
type JsonValue = str | int | float | bool | None
type SerializableType = dict[str, JsonValue] | list[JsonValue] | JsonValue


class SerializedBalanceData(BaseModel):
    """Model for serialized balance data."""

    exchange_id: str
    asset: str
    amount: Decimal
    locked_amount: Decimal = Decimal(0)
    available_amount: Decimal = Decimal(0)
    timestamp: float


class SerializedPositionData(BaseModel):
    """Model for serialized position data."""

    position_id: str
    exchange_id: str
    symbol: str
    side: str
    size: Decimal
    entry_price: Decimal
    current_price: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    margin_used: Decimal = Decimal(0)
    leverage: Decimal = Decimal(1)
    timestamp: float


class SerializedOrderData(BaseModel):
    """Model for serialized order data."""

    order_id: str
    exchange_id: str
    symbol: str
    side: str
    order_type: str
    quantity: Decimal
    price: Decimal
    filled_quantity: Decimal = Decimal(0)
    status: str
    timestamp: float


class SerializedTradeData(BaseModel):
    """Model for serialized trade data."""

    trade_id: str
    order_id: str
    exchange_id: str
    symbol: str
    side: str
    quantity: Decimal
    price: Decimal
    fee: Decimal = Decimal(0)
    fee_currency: str = ""
    timestamp: float


class SerializedMetadata(BaseModel):
    """Model for serialized metadata."""

    created_at: float
    updated_at: float
    version: str = "1.0"
    source_component: str = ""
    notes: str = ""
    tags: dict[str, str] = Field(default_factory=dict)


class SerializedPortfolioData(BaseModel):
    """Model for serialized portfolio data with proper typing."""

    balances: dict[str, SerializedBalanceData] = Field(default_factory=dict)
    positions: dict[str, SerializedPositionData] = Field(default_factory=dict)
    orders: dict[str, SerializedOrderData] = Field(default_factory=dict)
    trades: dict[str, SerializedTradeData] = Field(default_factory=dict)
    metadata: SerializedMetadata = Field(
        default_factory=lambda: SerializedMetadata(created_at=time.time(), updated_at=time.time())
    )
    timestamp: float = Field(default_factory=time.time)
    version: str = "1.0"


class SerializableData(BaseModel):
    """Model for generic serializable data."""

    data: dict[str, JsonValue] | list[JsonValue] | JsonValue


class SecureSerializer:
    """Secure serializer using JSON for internal data persistence."""

    @staticmethod
    def dumps(obj: SerializableType | BaseModel) -> bytes:
        """Serialize object to bytes using JSON.

        Args:
            obj: Object to serialize

        Returns:
            Serialized bytes
        """
        # If it's a Pydantic model, use model_dump
        if isinstance(obj, BaseModel):
            return obj.model_dump_json().encode("utf-8")
        # For other objects, use standard JSON serialization
        return json.dumps(obj, default=str, ensure_ascii=False).encode("utf-8")

    @staticmethod
    def loads(data: bytes) -> SerializableType:
        """Deserialize bytes to object using JSON.

        Args:
            data: Bytes to deserialize

        Returns:
            Deserialized object
        """
        # Parse JSON and validate with Pydantic
        raw_data = data.decode("utf-8")
        parsed = json.loads(raw_data)

        # Use Pydantic to validate the structure
        validated = SerializableData(data=parsed)
        return validated.data

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
