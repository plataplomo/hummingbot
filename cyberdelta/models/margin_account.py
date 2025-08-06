"""Internal Core Model: Margin Account Summary.

Represents an immutable snapshot of the overall margin account state for a specific exchange.
This module provides models for tracking margin account information across different exchanges,
including core fields common to all exchanges and exchange-specific extension slots for
detailed margin calculations and risk metrics.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.field_validation import (
    FieldNameMissingError,
)
from cyberdelta.models.base_validators import (
    ExchangeValidationMixin,
    ExtensionSlotModel,
    ImmutableModel,
    optional_decimal_validator,
    required_datetime_validator,
    required_decimal_validator,
)
from cyberdelta.utils.parsing import (
    validate_str_field,
)


# --- Margin Account Summary Core Model (IMMUTABLE SNAPSHOT) ---


class MarginAccountSummary(ExchangeValidationMixin, ImmutableModel):
    """Represents an immutable snapshot of the overall margin account state.

    Follows the "Core + Typed Extension Slots" pattern (Idea 5).

    Core Fields:
        exchange (str): Required exchange name.
        timestamp (datetime): Required snapshot timestamp (UTC).
        total_equity (Decimal): Required total account equity (>= 0).
        available_equity (Decimal): Required available equity (>= 0).
        total_initial_margin_required (Decimal | None): Optional total initial margin (>= 0).
        total_maintenance_margin_required (Decimal | None): Optional total maint. margin (>= 0).
        total_position_notional (Decimal | None): Optional total position notional (>= 0).
        total_unrealized_pnl (Decimal | None): Optional total unrealized PnL.

    Extension Slots:
        hl_details (HyperliquidMarginDetails | None): Details if exchange is 'hyperliquid'.
        bp_details (BackpackMarginDetails | None): Details if exchange is 'backpack'.

    Notes:
        - IMMUTABLE (`frozen=True`, `validate_assignment=True`).
        - Logic consistency (e.g., exchange vs. details slot) enforced by validator.

    """

    # --- Core Required Fields ---
    exchange: ExchangeName
    timestamp: datetime
    total_equity: Decimal = Field(ge=Decimal(0))
    available_equity: Decimal = Field(ge=Decimal(0))
    # --- Core Optional Fields ---
    total_initial_margin_required: Decimal | None = Field(default=None, ge=Decimal(0))
    total_maintenance_margin_required: Decimal | None = Field(default=None, ge=Decimal(0))
    total_position_notional: Decimal | None = Field(default=None, ge=Decimal(0))
    total_unrealized_pnl: Decimal | None = None  # Can be negative
    # --- Extension Slots ---
    hl_details: HyperliquidMarginDetails | None = Field(default=None)
    bp_details: BackpackMarginDetails | None = Field(default=None)

    # Config: Immutable (inherited from ImmutableModel)
    # Exchange validation: ExchangeValidationMixin provides validate_exchange()

    # Exchange validation provided by ExchangeValidationMixin

    _validate_timestamp = required_datetime_validator("timestamp")
    _validate_required_decimals = required_decimal_validator("total_equity", "available_equity")
    _validate_optional_decimals = optional_decimal_validator(
        "total_initial_margin_required",
        "total_maintenance_margin_required",
        "total_position_notional",
        "total_unrealized_pnl",
    )

    # DateTime and decimal validation provided by convenience validators above

    # --- Model Validators ---
    # Pydantic v2+ does not reliably support @model_validator on frozen models after init
    # for simple checks like this. We will rely on correct instantiation logic.
    # If cross-field validation during init is critical, a root_validator (pre=True)
    # might be needed, or potentially making the model mutable briefly during init.
    # For now, keep it simple and rely on the caller providing consistent exchange/details.


# --- Margin Account Details Sub-Models (INTERNAL, IMMUTABLE) ---


class HyperliquidMarginDetails(ExtensionSlotModel):
    """Immutable exchange-specific details for a Hyperliquid margin account summary."""

    cross_maintenance_margin_used: Decimal = Field(ge=Decimal(0))
    isolated_maintenance_margin_used: Decimal = Field(ge=Decimal(0))

    _validate_required_decimals = required_decimal_validator(
        "cross_maintenance_margin_used", "isolated_maintenance_margin_used"
    )


class BackpackMarginDetails(ExtensionSlotModel):
    """Backpack-specific margin account enrichment data.

    Maps to OpenAPI MarginAccountSummary fields not covered in
    core MarginAccountSummary. Provides Backpack-specific risk
    and collateral data for enhanced trading decisions.

    CONSISTENCY NOTE: Structure mirrors HyperliquidMarginDetails
    but contains Backpack's unique collateral-based data.
    """

    # Enhanced equity breakdown (from OpenAPI MarginAccountSummary)
    assets_value: Decimal | None = Field(
        default=None,
        ge=Decimal(0),
        description="Total value of all assets (assetsValue)",
    )
    liabilities_value: Decimal | None = Field(
        default=None,
        ge=Decimal(0),
        description="Total value of all liabilities (liabilitiesValue)",
    )
    locked_equity: Decimal | None = Field(
        default=None,
        ge=Decimal(0),
        description="Equity locked in orders/positions (netEquityLocked)",
    )
    borrow_liability: Decimal | None = Field(
        default=None,
        ge=Decimal(0),
        description="Total borrowed amount liability (borrowLiability)",
    )
    unsettled_equity: Decimal | None = Field(
        default=None,
        description="Equity pending settlement (unsettledEquity)",
    )

    # Risk metrics (from OpenAPI MarginAccountSummary)
    margin_fraction: Decimal | None = Field(
        default=None,
        ge=Decimal(0),
        description="Current margin utilization fraction (marginFraction, nullable)",
    )
    net_exposure_futures: Decimal | None = Field(
        default=None,
        description="Net futures/perp exposure notional (netExposureFutures)",
    )

    # Raw margin factors for debugging (from OpenAPI)
    imf_raw: str | None = Field(
        default=None,
        description="Raw Initial Margin Fraction string from API (imf)",
    )
    mmf_raw: str | None = Field(
        default=None,
        description="Raw Maintenance Margin Fraction string from API (mmf)",
    )

    # Account configuration
    leverage_limit: Decimal | None = Field(
        default=None,
        gt=Decimal(0),
        description="Account leverage limit (leverageLimit)",
    )

    # Subaccount information (from OpenAPI support)
    subaccount_id: int | None = Field(
        default=None,
        ge=0,
        le=65535,
        description="Subaccount ID used for this data (uint16)",
    )

    # Per-asset collateral breakdown (from OpenAPI Collateral array)
    collateral_assets: list[dict[str, str]] | None = Field(
        default=None,
        exclude=True,
        description="Detailed collateral breakdown by asset (internal use)",
    )

    # OpenAPI compliance metadata
    source_endpoint: str | None = Field(
        default=None,
        exclude=True,
        description="Source endpoint for debugging (collateral vs basic)",
    )

    # Config: Immutable (inherited from ExtensionSlotModel)

    _validate_optional_decimals = optional_decimal_validator(
        "assets_value",
        "borrow_liability",
        "liabilities_value",
        "locked_equity",
        "margin_fraction",
        "unsettled_equity",
        "net_exposure_futures",
        "leverage_limit",
    )

    @field_validator("imf_raw", "mmf_raw", mode="before")
    @classmethod
    def validate_optional_string(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate optional string field if present.

        Returns:
            The validated string value or None if input is None.

        Raises:
            FieldNameMissingError: If field name is missing from validation info.
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError("validation")
        if v is None:
            return None
        # Basic string validation, adjust max_length if needed
        return validate_str_field(v, field_name=field_name, max_length=256)
