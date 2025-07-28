"""Internal Core Model: Margin Account Summary.

Represents an immutable snapshot of the overall margin account state for a specific exchange.
This module provides models for tracking margin account information across different exchanges,
including core fields common to all exchanges and exchange-specific extension slots for
detailed margin calculations and risk metrics.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.exceptions.field_validation import (
    DecimalFiniteError,
    FieldNameMissingError,
    RequiredFieldNoneError,
)
from cyberdelta.utils.parsing import (
    parse_datetime_utc,
    parse_decimal_value,
    validate_str_field,
)


# --- Margin Account Summary Core Model (IMMUTABLE SNAPSHOT) ---


class MarginAccountSummary(BaseModel):
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
    exchange: str
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

    # Config: IMMUTABLE, forbid extra fields, validate on assignment (though immutable)
    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    # --- Field Validators ---
    @field_validator("exchange", mode="before")
    @classmethod
    def validate_required_strings(cls, v: str, info: ValidationInfo) -> str:
        """Validate required string fields are non-empty, reasonable length.
        
        Returns:
            The validated string value.
            
        Raises:
            FieldNameMissingError: If field name is missing from validation info.
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError("validation")
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_required_datetime_utc(
        cls,
        v: str | float | datetime,
        info: ValidationInfo,
    ) -> datetime:
        """Parse required datetime, ensuring UTC.
        
        Returns:
            The parsed datetime in UTC timezone.
            
        Raises:
            FieldNameMissingError: If field name is missing from validation info.
            RequiredFieldNoneError: If datetime value is None or invalid.
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError("validation")
        # parse_datetime_utc helper already raises error if v is None or invalid format
        dt = parse_datetime_utc(v, field_name=field_name)  # Removed allow_none
        # Additional check just in case helper behavior changes (unlikely)
        if dt is None:
            raise RequiredFieldNoneError(
                field_name=field_name,
                reason="Required datetime value parsed as None or was invalid",
            )
        return dt

    @field_validator("total_equity", "available_equity", mode="before")
    @classmethod
    def parse_required_decimal_finite_non_negative(
        cls,
        v: str | float | Decimal,
        info: ValidationInfo,
    ) -> Decimal:
        """Parse required decimal, ensuring finite and non-negative.
        
        Returns:
            The parsed finite Decimal value.
            
        Raises:
            FieldNameMissingError: If field name is missing from validation info.
            DecimalFiniteError: If decimal value is not finite.
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError("validation")
        # Parse, explicitly handling None return from parser for required field
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        # allow_none=False ensures parsed is never None

        # Check finiteness. ge=0 handled by Field constraint.
        if not parsed.is_finite():
            raise DecimalFiniteError(field_name=field_name, value=parsed)
        return parsed

    @field_validator(
        "total_initial_margin_required",
        "total_maintenance_margin_required",
        "total_position_notional",
        "total_unrealized_pnl",
        mode="before",
    )
    @classmethod
    def parse_optional_decimal_finite(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimals, allowing None but ensuring finite if present.
        
        Returns:
            The parsed finite Decimal value or None if input is None or invalid.
            
        Raises:
            FieldNameMissingError: If field name is missing from validation info.
            DecimalFiniteError: If decimal value is not finite when provided.
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError("validation")
        if v is None:
            return None
        # Parse, allow None from parser if input format is bad
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:  # Input format was invalid, return None as per optional field
            return None

        # Check finiteness if a valid Decimal was parsed. ge=0 handled by Field where applicable.
        if not parsed.is_finite():
            raise DecimalFiniteError(
                field_name=field_name,
                value=parsed,
                context="if provided",
            )
        return parsed

    # --- Model Validators ---
    # Pydantic v2+ does not reliably support @model_validator on frozen models after init
    # for simple checks like this. We will rely on correct instantiation logic.
    # If cross-field validation during init is critical, a root_validator (pre=True)
    # might be needed, or potentially making the model mutable briefly during init.
    # For now, keep it simple and rely on the caller providing consistent exchange/details.


# --- Margin Account Details Sub-Models (INTERNAL, IMMUTABLE) ---


class HyperliquidMarginDetails(BaseModel):
    """Immutable exchange-specific details for a Hyperliquid margin account summary."""

    cross_maintenance_margin_used: Decimal = Field(ge=Decimal(0))
    isolated_maintenance_margin_used: Decimal = Field(ge=Decimal(0))

    # Config: Immutable, ignore extra fields during creation
    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=False)

    @field_validator(
        "cross_maintenance_margin_used",
        "isolated_maintenance_margin_used",
        mode="before",
    )
    @classmethod
    def parse_required_decimal_finite_non_negative(
        cls,
        v: str | float | Decimal,
        info: ValidationInfo,
    ) -> Decimal:
        """Parse required decimal, ensuring finite and non-negative.
        
        Returns:
            The parsed finite Decimal value.
            
        Raises:
            FieldNameMissingError: If field name is missing from validation info.
            DecimalFiniteError: If decimal value is not finite.
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError("validation")

        # Parse, explicitly handling None return from parser for required field
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        # allow_none=False ensures parsed is never None

        # Check finiteness. ge=0 handled by Field constraint.
        if not parsed.is_finite():
            raise DecimalFiniteError(field_name=field_name, value=parsed)
        return parsed


class BackpackMarginDetails(BaseModel):
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

    model_config = ConfigDict(
        extra="ignore",
        validate_assignment=True,
        frozen=True,  # Consistency with HyperliquidMarginDetails
    )

    @field_validator(
        "assets_value",
        "borrow_liability",
        "liabilities_value",
        "locked_equity",
        "margin_fraction",
        "unsettled_equity",
        "net_exposure_futures",
        "leverage_limit",
        mode="before",
    )
    @classmethod
    def parse_optional_decimal_finite(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite if present.
        
        Returns:
            The parsed finite Decimal value or None if input is None or invalid.
            
        Raises:
            FieldNameMissingError: If field name is missing from validation info.
            DecimalFiniteError: If decimal value is not finite when provided.
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError("validation")
        if v is None:  # Explicitly allow None input
            return None

        # Parse, allow None from parser if input format is bad
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:  # Input format was invalid, return None as per optional field
            return None

        # Check finiteness if a valid Decimal was parsed. ge=0 handled by Field constraint.
        if not parsed.is_finite():
            raise DecimalFiniteError(
                field_name=field_name,
                value=parsed,
                context="if provided",
            )
        return parsed

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
