"""
Portfolio Models for CyberDeltaEngine

This module contains models representing the user's portfolio state, including balances,
derivative positions, and margin account summaries. These are used for risk management,
PnL tracking, and reporting.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.utils.parsing import (
    parse_datetime_utc,
    parse_decimal_value,
    validate_str_field,
)

# Type alias for values Pydantic passes to validators
ValidatorInput = Any


# --- Spot Balance Details Sub-Models (INTERNAL, IMMUTABLE) ---


class HyperliquidSpotBalanceDetails(BaseModel):
    """Immutable exchange-specific details for a Hyperliquid spot balance. (Currently empty)."""

    # Config: Immutable, ignore extra fields during creation
    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=False)


class BackpackSpotBalanceDetails(BaseModel):
    """Immutable exchange-specific details for a Backpack spot balance."""

    open_order_quantity: Decimal | None = Field(default=None, ge=Decimal("0"))
    lend_quantity: Decimal | None = Field(default=None, ge=Decimal("0"))
    collateral_weight: Decimal | None = Field(default=None, ge=Decimal("0"))

    # Config: Immutable, ignore extra fields during creation
    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=False)

    @field_validator("open_order_quantity", "lend_quantity", "collateral_weight", mode="before")
    @classmethod
    def parse_optional_decimal_finite(
        cls, v: ValidatorInput, info: ValidationInfo
    ) -> Decimal | None:
        """Parse optional decimal, allowing None but ensuring finite if present."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        if v is None:
            return None
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:  # Input format was invalid
            return None
        # Check finiteness if a valid Decimal was parsed. ge=0 handled by Field.
        if not parsed.is_finite():
            raise ValueError(f"{field_name}: Value must be finite if provided")
        return parsed


# --- Spot Balance Core Model (IMMUTABLE SNAPSHOT) ---


class SpotBalance(BaseModel):
    """
    Represents an immutable snapshot of a spot asset balance.
    Follows the "Core + Typed Extension Slots" pattern (Idea 5).

    Core Fields:
        exchange (str): Required exchange name.
        asset (str): Required asset symbol.
        timestamp (datetime): Required snapshot timestamp (UTC).
        total_quantity (Decimal): Required total quantity (>= 0).
        available_quantity (Decimal): Required available quantity (>= 0).

    Extension Slots:
        hl_details (HyperliquidSpotBalanceDetails | None): Details if exchange is 'hyperliquid'.
        bp_details (BackpackSpotBalanceDetails | None): Details if exchange is 'backpack'.

    Notes:
        - IMMUTABLE (`frozen=True`).
        - Logic consistency (e.g., exchange vs. details slot) enforced by instantiation logic.
    """

    # --- Core Required Fields ---
    exchange: str
    asset: str
    timestamp: datetime
    total_quantity: Decimal = Field(ge=Decimal("0"))
    available_quantity: Decimal = Field(ge=Decimal("0"))
    # --- Extension Slots ---
    hl_details: HyperliquidSpotBalanceDetails | None = Field(default=None)
    bp_details: BackpackSpotBalanceDetails | None = Field(default=None)

    # Config: IMMUTABLE, forbid extra fields, validate on assignment (though immutable)
    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    # --- Field Validators ---
    @field_validator("exchange", "asset", mode="before")
    @classmethod
    def validate_required_strings(cls, v: ValidatorInput, info: ValidationInfo) -> str:
        """Validate required string fields are non-empty, reasonable length."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_required_datetime_utc(cls, v: ValidatorInput, info: ValidationInfo) -> datetime:
        """Parse required datetime, ensuring UTC."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        dt = parse_datetime_utc(v, field_name=field_name)
        if dt is None:
            raise ValueError(
                f"{field_name}: Required datetime value parsed as None or was invalid."
            )
        return dt

    @field_validator("total_quantity", "available_quantity", mode="before")
    @classmethod
    def parse_required_decimal_finite(cls, v: ValidatorInput, info: ValidationInfo) -> Decimal:
        """Parse required decimal, ensuring finite and non-negative via Field."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        if parsed is None:
            raise ValueError(f"{field_name}: Required value parsed as None or was invalid.")
        # Check finiteness. ge=0 handled by Field constraint.
        if not parsed.is_finite():
            raise ValueError(f"{field_name}: Value must be finite")
        return parsed
