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
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            # This should be practically unreachable due to Pydantic's validation flow
            raise ValueError("Field name is unexpectedly None during validation.")
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("total", "available", mode="before")
    @classmethod
    def parse_and_validate_decimal(
        cls, v: str | int | float | Decimal | None, info: ValidationInfo
    ) -> Decimal:
        """Parse and validate decimal fields, ensuring they are non-None and finite."""
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed_value = parse_decimal_value(v, field_name=field_name)
        # DEFENSIVE CHECK: Ensure parsed_value is not None first (should be caught by parse_decimal if input was invalid/None)
        if parsed_value is None:
            # This path implies parse_decimal_value allowed None despite required field context
            raise ValueError(f"{field_name}: Required value parsed as None.")
        # DEFENSIVE CHECK: Ensure value is finite. Mypy=[possibly-undefined]
        if not parsed_value.is_finite():
            raise ValueError(f"{field_name}: Value must be finite (not NaN or Infinity)")
        return parsed_value
