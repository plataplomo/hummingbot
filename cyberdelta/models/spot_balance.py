"""Portfolio Models for CyberDeltaEngine.

This module contains models representing the user's portfolio state, including balances,
derivative positions, and margin account summaries. These are used for risk management,
PnL tracking, and reporting.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)
from pydantic_core.core_schema import ValidationInfo

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.field_validation import (
    DateTimeFieldError,
    DecimalFiniteError,
    FieldNameMissingError,
    InvalidExchangeNameError,
    TypeFieldError,
)
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.parsing import (
    parse_datetime_utc,
    parse_decimal_value,
)


# --- Spot Balance Details Sub-Models (INTERNAL, IMMUTABLE) ---


class HyperliquidSpotBalanceDetails(BaseModel):
    """Immutable exchange-specific details for a Hyperliquid spot balance. (Currently empty)."""

    # Config: Immutable, ignore extra fields during creation
    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=False)


class BackpackSpotBalanceDetails(BaseModel):
    """Immutable exchange-specific details for a Backpack spot balance."""

    open_order_quantity: Decimal | None = Field(default=None, ge=Decimal(0))
    lend_quantity: Decimal | None = Field(default=None, ge=Decimal(0))
    collateral_weight: Decimal | None = Field(default=None, ge=Decimal(0))

    # Config: Immutable, ignore extra fields during creation
    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=False)

    @field_validator("open_order_quantity", "lend_quantity", "collateral_weight", mode="before")
    @classmethod
    def parse_optional_decimal_finite(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, allowing None but ensuring finite if present.

        Args:
            v: Value to parse.
            info: Pydantic validation context.

        Returns:
            Decimal | None: Parsed decimal if valid, None if input is None.

        Raises:
            FieldNameMissingError: If field name is not available in validation context.
            DecimalFiniteError: If parsed decimal is not finite.
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        if v is None:
            return None
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        if parsed is None:  # Input format was invalid
            return None
        # Check finiteness if a valid Decimal was parsed. ge=0 handled by Field.
        if not parsed.is_finite():
            raise DecimalFiniteError(
                field_name=field_name,
                value=parsed,
            )
        return parsed


# --- Spot Balance Core Model (IMMUTABLE SNAPSHOT) ---


class SpotBalance(BaseModel):
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
    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    # --- Field Validators ---
    @field_validator("exchange", mode="before")
    @classmethod
    def validate_exchange_string(cls, v: object, info: ValidationInfo) -> ExchangeName:
        """Validate exchange field is a valid ExchangeName.

        Returns:
            ExchangeName: Validated exchange name.

        Raises:
            InvalidExchangeNameError: If exchange name is invalid.
            TypeFieldError: If value is not a string or ExchangeName.
        """
        if isinstance(v, ExchangeName):
            return v
        if isinstance(v, str):
            try:
                return ExchangeName(v.lower())
            except ValueError as e:
                raise InvalidExchangeNameError(
                    value=v,
                    valid_exchanges=[ex.value for ex in ExchangeName],
                ) from e
        raise TypeFieldError(
            field_name="exchange",
            expected_type="string or ExchangeName",
            actual_type=type(v).__name__,
            actual_value=v,
        )

    # Symbol validation is handled by Pydantic's type system
    # No need for a custom validator since Symbol is always valid

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_required_datetime_utc(
        cls,
        v: str | float | datetime,
        info: ValidationInfo,
    ) -> datetime:
        """Parse required datetime, ensuring UTC.

        Args:
            v: Value to parse as datetime.
            info: Pydantic validation context.

        Returns:
            datetime: Parsed UTC datetime.

        Raises:
            FieldNameMissingError: If field name is not available in validation context.
            DateTimeFieldError: If datetime parsing fails or returns None.
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        dt = parse_datetime_utc(v, field_name=field_name)
        if dt is None:
            raise DateTimeFieldError(
                field_name=field_name,
                value=v,
                reason="Required datetime value parsed as None or was invalid",
            )
        return dt

    @field_validator("total_quantity", "available_quantity", mode="before")
    @classmethod
    def parse_required_decimal_finite(
        cls,
        v: str | float | Decimal,
        info: ValidationInfo,
    ) -> Decimal:
        """Parse required decimal, ensuring finite and non-negative via Field.

        Args:
            v: Value to parse as decimal.
            info: Pydantic validation context.

        Returns:
            Decimal: Parsed finite decimal.

        Raises:
            FieldNameMissingError: If field name is not available in validation context.
            DecimalFiniteError: If parsed decimal is not finite.
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        # allow_none=False ensures parsed is never None
        # Check finiteness. ge=0 handled by Field constraint.
        if not parsed.is_finite():
            raise DecimalFiniteError(
                field_name=field_name,
                value=parsed,
            )
        return parsed
