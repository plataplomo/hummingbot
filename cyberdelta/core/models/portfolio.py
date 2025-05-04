"""
Portfolio Models for CyberDeltaEngine

This module contains models representing the user's portfolio state, including balances and
positions. These are used for risk management, PnL tracking, and reporting.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.core.models.enums import OrderSide
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field


class SpotBalance(BaseModel):
    """
    Represents an immutable snapshot of a spot asset balance on a specific exchange.

    This model reflects simple asset ownership, typically sourced from account balance endpoints.
    It uses Decimal for financial precision and is immutable (`frozen=True`) to ensure
    data integrity after creation.

    Fields:
        exchange (str): The name of the exchange (e.g., 'backpack', 'hyperliquid').
        asset (str): The asset/currency symbol (e.g., 'USDC', 'BTC').
        total (Decimal): Total balance for the asset (non-negative).
        available (Decimal): Amount available for trading (non-negative).

    Validators ensure required fields are non-empty strings and financial values are
    non-negative, finite Decimals.
    """

    exchange: str
    asset: str
    total: Decimal = Field(ge=Decimal("0"))
    available: Decimal = Field(ge=Decimal("0"))

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("exchange", "asset", mode="before")
    @classmethod
    def validate_string_fields(cls, v: object, info: ValidationInfo) -> str:
        """Validate exchange and asset fields are non-empty, reasonable length strings."""
        # DEFENSIVE CHECK: Ensures runtime type at boundary. Mypy=[unreachable]
        if info.field_name is None:
            # This should be practically unreachable due to Pydantic's validation flow
            raise ValueError("Field name is unexpectedly None during validation.")
        return validate_str_field(v, field_name=info.field_name, max_length=64)

    @field_validator("total", "available", mode="before")
    @classmethod
    def parse_and_validate_decimal(
        cls, v: str | int | float | Decimal | None, info: ValidationInfo
    ) -> Decimal:
        """Parse and validate decimal fields, ensuring they are non-None and finite."""
        # DEFENSIVE CHECK: Ensures runtime type at boundary. Mypy=[unreachable]
        if info.field_name is None:
            # This should be practically unreachable due to Pydantic's validation flow
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed_value = parse_decimal_value(v, allow_none=False, field_name=info.field_name)
        # DEFENSIVE CHECK: Explicitly require finite values post-parse. Mypy=[unreachable]
        if parsed_value is None:
            # parse_decimal_value should raise if allow_none=False, but belt-and-suspenders
            raise ValueError(f"{info.field_name}: Value cannot be None")
        # DEFENSIVE CHECK: Ensure value is finite. Mypy=[possibly-undefined]
        if not parsed_value.is_finite():
            raise ValueError(f"{info.field_name}: Value must be finite (not NaN or Infinity)")
        return parsed_value


class Position(BaseModel):
    """
    Position represents the current net holding or exposure in a specific asset or contract on an
    exchange. Unlike most models here, Position is mutable because it aggregates and updates state
    as new trades occur, reflecting the live position for risk and PnL tracking.

    Fields:
        symbol (str): Trading symbol (e.g., 'BTC-PERP').
        side (OrderSide): Net side (long/buy or short/sell).
        size (Decimal): Net quantity held (positive for long, negative for short).
        entry_price (Decimal): Average entry price for the current position
            (must be positive if open).
        leverage (Decimal | None): Leverage used, if applicable.
        id (str | None): Optional unique identifier.
        status (str | None): Optional status string.
        mark_price (Decimal | None): Current mark price.
        liquidation_price (Decimal | None): Liquidation price.
        unrealized_pnl (Decimal | None): Current unrealized PnL.
        realized_pnl (Decimal | None): Realized PnL from closed portions.
        margin_type (str | None): Margin type (e.g., 'cross', 'isolated').
        margin_used (Decimal | None): Margin used for the position.
        timestamp (int | None): Optional timestamp.
        strategy_name (str | None): Optional strategy identifier.
        close_price (Decimal | None): Price at which the position was closed.
        close_time (datetime | None): Time at which the position was closed.
        pnl (Decimal | None): Total PnL for the position.

    Notes:
        - All financial fields use Decimal for precision.
        - Use is_active() to check if the position is open.
        - Use calculate_unrealized_pnl() for live PnL updates.
        - This model is mutable by design for real-time state tracking.
    """

    symbol: str
    side: OrderSide
    size: Decimal
    entry_price: Decimal = Field(
        gt=0, description="Entry price must be positive if position is open."
    )
    leverage: Decimal | None = None
    id: str | None = None
    status: str | None = None
    mark_price: Decimal | None = None
    liquidation_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    realized_pnl: Decimal | None = None
    margin_type: str | None = None
    margin_used: Decimal | None = None
    timestamp: int | None = None
    strategy_name: str | None = None
    close_price: Decimal | None = None
    close_time: datetime | None = None
    pnl: Decimal | None = None

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    @field_validator(
        "size",
        "entry_price",
        "leverage",
        "mark_price",
        "liquidation_price",
        "unrealized_pnl",
        "realized_pnl",
        "margin_used",
        "close_price",
        "pnl",
        mode="before",
    )
    @classmethod
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: object) -> Decimal | None:
        return parse_decimal_value(v)

    @field_validator("close_time", mode="before")
    @classmethod
    def parse_datetime(
        cls, v: str | int | float | datetime | None, info: object
    ) -> datetime | None:
        return parse_datetime_utc(v)

    def is_active(self) -> bool:
        """
        Returns True if the position is currently open (size != 0), False otherwise.
        """
        return self.size != Decimal("0")

    def calculate_unrealized_pnl(self, current_mark_price: Decimal | None = None) -> Decimal | None:
        """
        Calculate the unrealized PnL for the position based on the current mark price.
        If no mark price is provided, returns the stored unrealized_pnl.

        Args:
            current_mark_price (Decimal | None): The current mark price for the symbol.
        Returns:
            Decimal | None: The calculated or stored unrealized PnL.
        """
        if current_mark_price is None:
            return self.unrealized_pnl
        if self.size == Decimal("0"):
            return Decimal("0.0")
        if self.side == OrderSide.BUY:
            pnl = (current_mark_price - self.entry_price) * self.size
        else:
            pnl = (self.entry_price - current_mark_price) * self.size
        object.__setattr__(self, "unrealized_pnl", pnl)
        return pnl

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the Position to a dictionary, serializing Decimals, Enums, and
        datetimes appropriately.
        Returns:
            dict[str, Any]: Dictionary representation of the position.
        """
        d = self.model_dump()
        for k, v in d.items():
            if isinstance(v, Decimal):
                d[k] = str(v)
            elif isinstance(v, OrderSide):
                d[k] = v.value
            elif isinstance(v, datetime):
                d[k] = v.isoformat()
        return d

    @model_validator(mode="after")
    def check_position_logic(self) -> Self:
        """
        Ensure entry_price is positive if size is not zero. Optionally, check side/size consistency.
        """
        if self.size != Decimal("0") and self.entry_price <= 0:
            raise ValueError("Entry price must be positive if position is open.")
        # Example: if self.side == OrderSide.BUY and self.size < 0: ...
        return self
