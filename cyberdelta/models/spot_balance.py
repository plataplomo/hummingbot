"""Portfolio Models for CyberDeltaEngine.

This module contains models representing the user's portfolio state, including balances,
derivative positions, and margin account summaries. These are used for risk management,
PnL tracking, and reporting.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import Field

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models.base_validators import (
    ExchangeValidationMixin,
    ExtensionSlotModel,
    StandardModel,
    optional_decimal_validator,
    required_datetime_validator,
    required_decimal_validator,
)
from cyberdelta.symbols.models import Symbol


# --- Spot Balance Details Sub-Models (INTERNAL, IMMUTABLE) ---


class HyperliquidSpotBalanceDetails(ExtensionSlotModel):
    """Immutable exchange-specific details for a Hyperliquid spot balance. (Currently empty)."""

    # Config: Extension slot (inherited from ExtensionSlotModel)


class BackpackSpotBalanceDetails(ExtensionSlotModel):
    """Immutable exchange-specific details for a Backpack spot balance."""

    open_order_quantity: Decimal | None = Field(default=None, ge=Decimal(0))
    lend_quantity: Decimal | None = Field(default=None, ge=Decimal(0))
    collateral_weight: Decimal | None = Field(default=None, ge=Decimal(0))

    # Config: Extension slot (inherited from ExtensionSlotModel)
    # Use centralized validator for optional decimals
    _validate_optional_decimals = optional_decimal_validator(
        "open_order_quantity", "lend_quantity", "collateral_weight"
    )


# --- Spot Balance Core Model (IMMUTABLE SNAPSHOT) ---


class SpotBalance(ExchangeValidationMixin, StandardModel):
    """Represents an immutable snapshot of a spot asset balance.

    Follows the "Core + Typed Extension Slots" pattern (Idea 5).

    Core Fields:
        exchange (str): Required exchange name.
        asset (Symbol): Exchange-specific asset symbol domain object.
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
    exchange: ExchangeName
    asset: Symbol
    timestamp: datetime
    total_quantity: Decimal = Field(ge=Decimal(0))
    available_quantity: Decimal = Field(ge=Decimal(0))
    # --- Extension Slots ---
    hl_details: HyperliquidSpotBalanceDetails | None = Field(default=None)
    bp_details: BackpackSpotBalanceDetails | None = Field(default=None)

    # Config: IMMUTABLE, forbid extra fields, validate on assignment (though immutable)
    # Validation provided by mixins and convenience validators
    # Exchange validation: ExchangeValidationMixin
    # DateTime validation: required_datetime_validator
    # Decimal validation: required_decimal_validator

    _validate_timestamp = required_datetime_validator("timestamp")
    _validate_decimals = required_decimal_validator("total_quantity", "available_quantity")
