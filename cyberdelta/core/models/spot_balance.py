"""
Portfolio Models for CyberDeltaEngine

This module contains models representing the user's portfolio state, including balances,
derivative positions, and margin account summaries. These are used for risk management,
PnL tracking, and reporting.
"""

from __future__ import annotations

from decimal import Decimal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


class SpotBalance(BaseModel):
    """
    Represents an immutable snapshot of a spot asset balance on a specific exchange.

    This model reflects detailed asset ownership and its collateral value, primarily derived
    from sources like Backpack's collateral endpoint. It uses Decimal for financial precision
    and is immutable (`frozen=True`) to ensure data integrity after creation.

    Fields:
        exchange (str): The name of the exchange (e.g., 'backpack', 'hyperliquid'). Required.
        asset (str): The asset/currency symbol (e.g., 'USDC', 'BTC'). Required.
        total_quantity (Decimal): Total balance quantity for the asset (non-negative). Required.
        available_quantity (Decimal): Quantity available for trading/withdrawal (non-negative). Required.
        mark_price (Decimal | None): Current mark price of the asset (positive if provided). Optional.
        balance_notional (Decimal | None): Notional value of the total balance (non-negative if provided). Optional.
        collateral_weight (Decimal | None): Collateral weight assigned by the exchange (non-negative if provided). Optional.
        collateral_value (Decimal | None): Calculated collateral value (non-negative if provided). Optional.
        open_order_quantity (Decimal | None): Quantity tied up in open orders (non-negative if provided). Optional.
        lend_quantity (Decimal | None): Quantity currently being lent out (non-negative if provided). Optional.

    Validators ensure required fields are non-empty strings and financial values meet their
    constraints (e.g., non-negative, positive, finite Decimals).
    """

    exchange: str
    asset: str
    total_quantity: Decimal = Field(ge=Decimal("0"))  # Renamed from total
    available_quantity: Decimal = Field(ge=Decimal("0"))  # Renamed from available
    # --- New Optional Fields ---
    mark_price: Decimal | None = Field(default=None, gt=Decimal("0"))  # Must be > 0 if present
    balance_notional: Decimal | None = Field(default=None, ge=Decimal("0"))
    collateral_weight: Decimal | None = Field(default=None, ge=Decimal("0"))
    collateral_value: Decimal | None = Field(default=None, ge=Decimal("0"))
    open_order_quantity: Decimal | None = Field(default=None, ge=Decimal("0"))
    lend_quantity: Decimal | None = Field(default=None, ge=Decimal("0"))

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("exchange", "asset", mode="before")
    @classmethod
    def validate_required_strings(cls, v: object, info: ValidationInfo) -> str:
        """Validate required string fields are non-empty, reasonable length."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("total_quantity", "available_quantity", mode="before")
    @classmethod
    def parse_required_decimal(
        cls, v: str | int | float | Decimal | None, info: ValidationInfo
    ) -> Decimal:
        """Parse required decimals, ensuring non-None and finite."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed_value = parse_decimal_value(v, field_name=field_name)
        if parsed_value is None:
            raise ValueError(f"{field_name}: Required value parsed as None or was invalid.")
        if not parsed_value.is_finite():
            raise ValueError(f"{field_name}: Value must be finite (not NaN or Infinity)")
        # Non-negative check handled by Field(ge=0)
        return parsed_value

    @field_validator(
        "mark_price",
        "balance_notional",
        "collateral_weight",
        "collateral_value",
        "open_order_quantity",
        "lend_quantity",
        mode="before",
    )
    @classmethod
    def parse_optional_decimal(
        cls, v: str | int | float | Decimal | None, info: ValidationInfo
    ) -> Decimal | None:
        """Parse optional decimals, allowing None but ensuring finite if present."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        if v is None:
            return None
        parsed_value = parse_decimal_value(
            v, field_name=field_name, allow_none=True
        )  # Allow None parsing
        if parsed_value is not None and not parsed_value.is_finite():
            raise ValueError(
                f"{field_name}: Value must be finite if provided (not NaN or Infinity)"
            )
        # Positivity/Non-negative checks handled by Field constraints (gt=0, ge=0)
        return parsed_value
