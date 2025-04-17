"""
Strategy Models for CyberDeltaEngine

This module contains models representing trading strategy intent, signals, and arbitrage
opportunities. These are used by the strategy layer to communicate actionable ideas and
instructions to the execution and portfolio layers.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cyberdelta.core.models.enums import OrderSide, SignalType
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value


class ArbitrageOpportunity(BaseModel):
    """
    ArbitrageOpportunity represents a funding rate arbitrage opportunity between two exchanges for a
    given symbol. This model is mutable because it may be updated with analytics, sizing, or
    confidence scores after initial creation.

    Fields:
        symbol (str): Trading symbol.
        long_exchange (str): Exchange to go long.
        short_exchange (str): Exchange to go short.
        long_price (Decimal): Long entry price (must be positive).
        short_price (Decimal): Short entry price (must be positive).
        long_funding_rate (Decimal): Funding rate on long exchange.
        short_funding_rate (Decimal): Funding rate on short exchange.
        net_funding_differential (Decimal): Net funding advantage (long - short).
        timestamp (datetime): UTC timestamp of opportunity detection.
        expected_profit (Decimal | None): Optional expected profit.
        basis_volatility (float | None): Optional basis volatility metric.
        utility_score (float | None): Optional utility score for ranking.
        optimal_size (Decimal | None): Optional optimal trade size.
        confidence (float | None): Optional confidence score.
        expiration_timestamp (float | None): Optional expiry (epoch seconds).
        id (str): Unique identifier (UUID).

    Notes:
        - All financial fields use Decimal for accuracy.
        - Use this model for opportunity tracking, analytics, and strategy input.
        - This model is mutable to allow enrichment after creation.
    """

    symbol: str
    long_exchange: str
    short_exchange: str
    long_price: Decimal = Field(gt=0, description="Long price must be positive.")
    short_price: Decimal = Field(gt=0, description="Short price must be positive.")
    long_funding_rate: Decimal
    short_funding_rate: Decimal
    net_funding_differential: Decimal
    timestamp: datetime
    # Optional fields: may not be available at opportunity creation
    expected_profit: Decimal | None = None
    basis_volatility: float | None = None
    utility_score: float | None = None
    optimal_size: Decimal | None = Field(
        default=None, gt=0, description="Optimal size, if calculated by risk/position sizing."
    )
    confidence: float | None = None  # Optional: model-derived or subjective score
    expiration_timestamp: float | None = None  # Optional: may be set by strategy or downstream
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator(
        "long_price",
        "short_price",
        "long_funding_rate",
        "short_funding_rate",
        "net_funding_differential",
        "optimal_size",
        "expected_profit",
        mode="before",
    )
    @classmethod
    def parse_decimal_fields(
        cls, v: str | int | float | Decimal | None, info: object
    ) -> Decimal | None:
        return parse_decimal_value(v)

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: str | int | float | datetime | None, info: object) -> datetime:
        dt = parse_datetime_utc(v)
        if dt is None:
            raise ValueError("timestamp cannot be None")
        return dt

    @model_validator(mode="after")
    def set_expiration(self) -> Self:
        """
        Set expiration_timestamp to 1 hour after timestamp (UTC).
        """
        if self.timestamp.tzinfo:
            object.__setattr__(self, "expiration_timestamp", self.timestamp.timestamp() + 3600)
        else:
            object.__setattr__(
                self, "expiration_timestamp", self.timestamp.replace(tzinfo=UTC).timestamp() + 3600
            )
        return self

    @model_validator(mode="after")
    def validate_required_fields(self) -> Self:
        # All required fields are enforced by Pydantic; no need to check for None.
        return self

    @model_validator(mode="after")
    def check_arbitrage_logic(self) -> Self:
        """
        Ensure all required financial fields are positive where appropriate.
        """
        if self.long_price <= 0:
            raise ValueError("Long price must be positive.")
        if self.short_price <= 0:
            raise ValueError("Short price must be positive.")
        if self.optimal_size is not None and self.optimal_size <= 0:
            raise ValueError("Optimal size must be positive if present.")
        # No need to check for None or always-true conditions on required fields
        return self


class TradeSignal(BaseModel):
    """
    TradeSignal represents a decision or actionable signal generated by a trading strategy. It is
    mutable to allow enrichment with metadata, tracking, or downstream annotations.

    Fields:
        symbol (str): Trading symbol.
        signal_type (SignalType): Type of signal (e.g., ENTRY, EXIT).
        side (OrderSide): Buy or sell.
        price (Decimal): Signal price (must be positive).
        quantity (Decimal): Signal quantity (must be positive).
        timestamp (datetime | None): Optional signal creation time.
        confidence (float | None): Optional confidence score.
        source_strategy (str | None): Optional strategy identifier.
        stop_loss (Decimal | None): Optional stop loss price.
        take_profit (Decimal | None): Optional take profit price.
        expiration (datetime | None): Optional expiry time.
        metadata (dict[str, Any] | None): Optional extra metadata.
        signal_id (str): Unique identifier (UUID).

    Notes:
        - All financial fields use Decimal for accuracy.
        - Use is_valid() to check if the signal is still actionable.
        - This model is mutable to support enrichment and tracking.
    """

    symbol: str
    signal_type: SignalType
    side: OrderSide
    price: Decimal  # Required if always provided for actionable signals
    quantity: Decimal  # Required if always provided for actionable signals
    # Optional fields
    timestamp: datetime | None = None  # Optional: can default to now if not set
    confidence: float | None = None  # Optional: model-derived or subjective score
    source_strategy: str | None = None  # Optional: for tracking
    stop_loss: Decimal | None = Field(
        default=None, gt=0, description="Stop loss, if set by strategy."
    )
    take_profit: Decimal | None = Field(
        default=None, gt=0, description="Take profit, if set by strategy."
    )
    expiration: datetime | None = None  # Optional: signal expiry
    metadata: dict[str, Any] | None = None  # Optional: extra info
    signal_id: str = Field(
        default_factory=lambda: str(uuid.uuid4())
    )  # Always present after creation

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator("price", "quantity", "stop_loss", "take_profit", mode="before")
    @classmethod
    def parse_decimal_fields(
        cls, v: str | int | float | Decimal | None, info: object
    ) -> Decimal | None:
        return parse_decimal_value(v)

    @field_validator("timestamp", "expiration", mode="before")
    @classmethod
    def parse_datetime_fields(
        cls, v: str | int | float | datetime | None, info: object
    ) -> datetime | None:
        return parse_datetime_utc(v)

    @model_validator(mode="after")
    def ensure_signal_id(self) -> Self:
        if not self.signal_id:
            object.__setattr__(self, "signal_id", str(uuid.uuid4()))
        return self

    @model_validator(mode="after")
    def check_signal_logic(self) -> Self:
        """
        Ensure price, quantity, stop_loss, and take_profit are positive if present.
        Enforce that actionable signals require a price.
        """
        if self.price <= 0:
            raise ValueError("Signal price must be positive.")
        if self.quantity <= 0:
            raise ValueError("Signal quantity must be positive.")
        if self.stop_loss is not None and self.stop_loss <= 0:
            raise ValueError("Stop loss must be positive if present.")
        if self.take_profit is not None and self.take_profit <= 0:
            raise ValueError("Take profit must be positive if present.")
        return self

    def is_valid(self) -> bool:
        """
        Check if the signal is still valid (e.g., not expired).
        Returns:
            bool: True if not expired, False otherwise.
        """
        if self.expiration is None:
            return True
        return datetime.now(UTC) < self.expiration
