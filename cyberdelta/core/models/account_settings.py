"""Internal Core Model: Account Settings.

Represents mutable account configuration settings for a specific exchange.
This module provides models for tracking account settings information across different exchanges,
including core fields common to all exchanges and exchange-specific extension slots for
detailed trading and margin settings.

This model differs from MarginAccountSummary by focusing on configuration/preferences
rather than financial state.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.exceptions.field_validation import DecimalFiniteError, FieldNameMissingError
from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


# --- Account Settings Core Model (MUTABLE CONFIGURATION) ---


class AccountSettings(BaseModel):
    """Represents mutable account configuration settings.

    Follows the "Core + Typed Extension Slots" pattern.

    PURPOSE: Configuration/preferences (leverage limits, auto-trading flags)
    DIFFERS FROM MarginAccountSummary: This is mutable configuration vs immutable financial state

    Core Fields:
        exchange (str): Required exchange name.
        timestamp (datetime): Last update timestamp (UTC).
        leverage_limit (Decimal | None): Optional account leverage limit (>= 1).
        auto_borrow_settlements (bool | None): Optional auto borrow settlements setting.
        auto_lend (bool | None): Optional auto lending setting.
        auto_realize_pnl (bool | None): Optional auto PnL realization setting.
        auto_repay_borrows (bool | None): Optional auto borrow repayment setting.

    Extension Slots:
        hl_details (HyperliquidAccountSettingsDetails | None): Details if exchange is 'hyperliquid'.
        bp_details (BackpackAccountSettingsDetails | None): Details if exchange is 'backpack'.

    Notes:
        - MUTABLE (not frozen) to allow settings updates.
        - Validates on assignment for safety.

    """

    # --- Core Required Fields ---
    exchange: str
    timestamp: datetime

    # --- Core Optional Fields ---
    leverage_limit: Decimal | None = Field(default=None, ge=Decimal(1))
    auto_borrow_settlements: bool | None = None
    auto_lend: bool | None = None
    auto_realize_pnl: bool | None = None
    auto_repay_borrows: bool | None = None

    # --- Extension Slots ---
    hl_details: HyperliquidAccountSettingsDetails | None = Field(default=None)
    bp_details: BackpackAccountSettingsDetails | None = Field(default=None)

    # Config: MUTABLE (not frozen), forbid extra fields, validate on assignment
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    # --- Field Validators (Simplified - reuse validation from parsing utils) ---
    def update_leverage_limit(self, new_limit: Decimal | None) -> None:
        """Update leverage limit with validation.

        Raises:
            ValueError: If leverage limit is not finite or less than 1.
        """
        if new_limit is not None:
            parsed = parse_decimal_value(new_limit, field_name="leverage_limit", allow_none=False)
            if not parsed.is_finite():
                raise DecimalFiniteError(
                    field_name="leverage_limit",
                    value=parsed,
                    context="and must be >= 1",
                )
            if parsed < Decimal(1):
                raise DecimalFiniteError(
                    field_name="leverage_limit",
                    value=parsed,
                    context="(must be >= 1)",
                )
        self.leverage_limit = new_limit

        self.timestamp = datetime.now(UTC)


# --- Account Settings Details Sub-Models (INTERNAL, IMMUTABLE) ---


class HyperliquidAccountSettingsDetails(BaseModel):
    """Immutable exchange-specific details for Hyperliquid account settings.

    Note:
        Hyperliquid uses per-asset leverage settings rather than global account leverage.
        This model captures that difference and any future Hyperliquid-specific settings.
    """

    # Per-asset leverage settings (asset index -> leverage value)
    asset_leverage_settings: dict[int, int] | None = Field(
        default=None,
        description="Per-asset leverage settings mapped by asset index",
    )

    # Cross vs isolated margin preferences
    cross_margin_enabled: bool | None = Field(
        default=None,
        description="Whether cross margin is enabled by default",
    )

    # Config: Immutable, ignore extra fields during creation
    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=False)


class BackpackAccountSettingsDetails(BaseModel):
    """Backpack-specific account settings enrichment data.

    Maps to Backpack's account settings fields providing enhanced
    trading configuration and risk management options.

    CONSISTENCY NOTE: Structure mirrors HyperliquidAccountSettingsDetails
    but contains Backpack's unique margin and auto-trading settings.
    """

    # Raw leverage limit value from API for debugging
    leverage_limit_raw: str | None = Field(
        default=None,
        description="Raw leverage limit string from API response",
    )

    # Subaccount information if applicable
    subaccount_id: int | None = Field(
        default=None,
        ge=0,
        le=65535,
        description="Subaccount ID for these settings (uint16)",
    )

    # Additional Backpack-specific auto-trading settings
    auto_liquidation_enabled: bool | None = Field(
        default=None,
        description="Whether auto-liquidation is enabled",
    )

    auto_margin_call_enabled: bool | None = Field(
        default=None,
        description="Whether auto margin calls are enabled",
    )

    # Source endpoint metadata for debugging
    source_endpoint: str | None = Field(
        default=None,
        exclude=True,
        description="Source endpoint for debugging",
    )

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        frozen=True,
    )

    @field_validator("leverage_limit_raw", mode="before")
    @classmethod
    def validate_optional_string(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate optional string field if present."""
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError(context="BackpackAccountSettingsDetails validation")
        if v is None:
            return None
        return validate_str_field(v, field_name=field_name, max_length=256)
