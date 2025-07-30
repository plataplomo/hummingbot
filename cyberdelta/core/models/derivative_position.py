"""CyberDeltaEngine derivative position models.

Provides the DerivativePosition model for tracking leveraged positions across exchanges,
following the "Core + Typed Extension Slots" pattern for exchange-specific details.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)
from pydantic_core.core_schema import ValidationInfo

# Correctly import the Raw model ONLY for transformation logic, not direct use in internal models
# (Although for Details, we usually transform *before* creating Details)
from cyberdelta.core.symbols.models import Symbol, BaseSymbol
from cyberdelta.enums import OrderSide
from cyberdelta.exceptions import (
    DecimalFiniteError,
    FieldNameMissingError,
    PositionLogicError,
    RequiredFieldNoneError,
    TypeFieldError,
)
from cyberdelta.utils.parsing import (
    parse_datetime_utc,
    parse_decimal_value,
    validate_enum_field,
    validate_str_field,
)


# --- Derivative Position Core Model ---


class DerivativePosition(BaseModel):
    """Represents the mutable state of a single derivative position on a specific exchange.

    This model tracks the core aspects of a leveraged position, allowing for updates
    as trades occur or market data changes. It follows the "Core + Typed Extension Slots"
    pattern, separating common fields from exchange-specific details.

    Core Fields:
        exchange (str): The name of the exchange (e.g., 'backpack', 'hyperliquid').
        symbol (Symbol): Exchange-specific trading symbol domain object.
        side (OrderSide): Net side (long/buy or short/sell) of the position.
        size (Decimal): Net quantity held (positive for long, negative for short, zero if flat).
        entry_price (Decimal | None): Average entry price if the position is open (> 0),
                                       None if the position is flat (size == 0).
        timestamp (datetime): Timestamp of the last update to this position state (UTC).
        mark_price (Decimal | None): Current mark price (non-negative if present).
        liquidation_price (Decimal | None): Estimated liquidation price (non-negative if present).
        unrealized_pnl (Decimal | None): Current unrealized PnL (can be negative).
        realized_pnl (Decimal | None): Accumulated realized PnL (can be negative).
        strategy_name (str | None): Optional identifier for the strategy managing this position.
        signal_id (str | None): Optional identifier for the signal
                                that originated the position/trade.

    Extension Slots:
        hl_details (HyperliquidPositionDetails | None): Specific details for Hyperliquid.
        bp_details (BackpackPositionDetails | None): Specific details for Backpack.

    Notes:
        - This model is MUTABLE (`frozen=False`, `validate_assignment=True`) by design.
        - All financial fields use Decimal for precision. Validators ensure finiteness.
        - Use `is_active()` to check if the position size is non-zero.
        - Core logic consistency (e.g., entry_price validity, side vs. size) is enforced by
          the `check_position_logic` model validator.

    """

    exchange: str
    symbol: Symbol
    side: OrderSide
    size: Decimal  # Can be positive, negative, or zero
    entry_price: Decimal | None = Field(default=None)  # Validated > 0 if size != 0 later
    timestamp: datetime  # Required, UTC
    # --- Optional Core Fields ---
    mark_price: Decimal | None = Field(default=None, ge=Decimal(0))
    liquidation_price: Decimal | None = Field(default=None, ge=Decimal(0))
    unrealized_pnl: Decimal | None = None  # Can be negative
    realized_pnl: Decimal | None = None  # Can be negative
    strategy_name: str | None = None
    signal_id: str | None = None
    # --- Extension Slots ---
    hl_details: HyperliquidPositionDetails | None = Field(default=None)
    bp_details: BackpackPositionDetails | None = Field(default=None)

    # IMPORTANT: MUTABLE MODEL - DO NOT SET frozen=True
    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    # --- Field Validators ---

    @field_validator("exchange", mode="before")
    @classmethod
    def validate_exchange_string(cls, v: object, info: ValidationInfo) -> str:
        """Validate exchange field is non-empty, reasonable length.

        Args:
            v: The value to validate
            info: Validation context containing field information

        Returns:
            Validated string value

        Raises:
            FieldNameMissingError: If field name is None
        """
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            # This should be practically unreachable due to Pydantic's validation flow
            raise FieldNameMissingError
        # Assuming validate_str_field internally handles None check if required
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol_domain(cls, v: object) -> Symbol:
        """Validate symbol field is Symbol domain object.

        Args:
            v: The value to validate

        Returns:
            Validated Symbol

        Raises:
            TypeError: If not a Symbol
        """
        if not isinstance(v, BaseSymbol):
            raise TypeFieldError(
                field_name="symbol", expected_type="Symbol", actual_type=type(v).__name__
            )
        return v

    @field_validator("strategy_name", "signal_id", mode="before")
    @classmethod
    def validate_optional_strings(cls, v: object, info: ValidationInfo) -> str | None:
        """Validate optional string fields if provided.

        Args:
            v: The value to validate (optional string)
            info: Validation context containing field information

        Returns:
            Validated string value or None if not provided

        Raises:
            FieldNameMissingError: If field name is None
        """
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        # Allow None, but if a string is passed, validate it
        if v is None:
            return None
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        return validate_str_field(v, field_name=field_name, max_length=128)

    @field_validator("size", mode="before")
    @classmethod
    def parse_required_decimal(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal:
        """Parse required decimal ('size'), ensuring finite.

        Args:
            v: The value to parse (string, float, Decimal, or None)
            info: Validation context containing field information

        Returns:
            Parsed finite decimal value

        Raises:
            FieldNameMissingError: If field name is None
            RequiredFieldNoneError: If parsed value is None
            DecimalFiniteError: If parsed value is not finite
        """
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        parsed = parse_decimal_value(v, field_name=field_name)
        # DEFENSIVE CHECK: Explicitly require non-None and finite values post-parse.
        if parsed is None:
            raise RequiredFieldNoneError(field_name)
        # DEFENSIVE CHECK: Ensure value is finite. Mypy=[possibly-undefined]
        if not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed)
        return parsed

    @field_validator(
        "entry_price",
        "mark_price",
        "liquidation_price",
        "unrealized_pnl",
        "realized_pnl",
        mode="before",
    )
    @classmethod
    def parse_optional_decimal(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimals, ensuring finite if present.

        Args:
            v: The value to parse (string, float, Decimal, or None)
            info: Validation context containing field information

        Returns:
            Parsed finite decimal value or None if not provided

        Raises:
            FieldNameMissingError: If field name is None
            DecimalFiniteError: If parsed value is not finite
        """
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        parsed = parse_decimal_value(v, field_name=field_name)
        # DEFENSIVE CHECK: Ensure finite if not None. Mypy=[redundant-expr]
        if parsed is not None and not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed, context="if provided")
        return parsed

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_required_datetime(
        cls,
        v: str | float | datetime | None,
        info: ValidationInfo,
    ) -> datetime:
        """Parse required datetime, ensuring UTC.

        Args:
            v: The value to parse (string, float, datetime, or None)
            info: Validation context containing field information

        Returns:
            Parsed UTC datetime

        Raises:
            FieldNameMissingError: If field name is None
            RequiredFieldNoneError: If parsed datetime is None
        """
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        dt = parse_datetime_utc(v, field_name=field_name)
        # DEFENSIVE CHECK: Explicitly require non-None. Mypy=[unreachable]
        if dt is None:
            raise RequiredFieldNoneError(field_name, "Required datetime parsed as None or invalid")
        return dt

    # --- Instance Methods ---

    def is_active(self) -> bool:
        """Check if the position has a non-zero size.

        Returns:
            True if position size is non-zero, False otherwise
        """
        return self.size != Decimal(0)

    # --- Model Validators ---

    @model_validator(mode="after")
    def check_position_logic(self) -> Self:
        """Validate cross-field consistency (entry_price, side/size, details slots).

        Returns:
            Self instance after validation
        """
        self._validate_entry_price_logic()
        self._validate_side_size_logic()
        self._validate_extension_slot_consistency()
        return self

    def _validate_entry_price_logic(self) -> None:
        """Validate entry price consistency with position size.

        Raises:
            PositionLogicError: If entry price logic is inconsistent with position size
        """
        if self.size != Decimal(0):
            if self.entry_price is None:
                raise PositionLogicError(
                    "entry_price_required",
                    "entry_price must be provided if size is non-zero",
                    exchange=self.exchange,
                    fields={"size": self.size, "entry_price": self.entry_price},
                )
            if self.entry_price <= Decimal(0):
                raise PositionLogicError(
                    "entry_price_positive",
                    "entry_price must be positive (> 0) if size is non-zero",
                    exchange=self.exchange,
                    fields={"size": self.size, "entry_price": self.entry_price},
                )
        elif self.entry_price is not None:
            raise PositionLogicError(
                "entry_price_none_when_flat",
                "entry_price must be None if size is zero",
                exchange=self.exchange,
                fields={"size": self.size, "entry_price": self.entry_price},
            )

    def _validate_side_size_logic(self) -> None:
        """Validate side consistency with position size.

        Raises:
            PositionLogicError: If side is inconsistent with position size
        """
        if self.size > Decimal(0) and self.side != OrderSide.BUY:
            raise PositionLogicError(
                "side_size_consistency",
                "side must be BUY if size is positive",
                exchange=self.exchange,
                fields={"size": self.size, "side": self.side.value},
            )
        if self.size < Decimal(0) and self.side != OrderSide.SELL:
            raise PositionLogicError(
                "side_size_consistency",
                "side must be SELL if size is negative",
                exchange=self.exchange,
                fields={"size": self.size, "side": self.side.value},
            )

    def _validate_extension_slot_consistency(self) -> None:
        """Validate exchange-specific details consistency (Idea 5).

        Raises:
            PositionLogicError: If exchange-specific details are inconsistent
        """
        known_exchanges_with_details = {"hyperliquid", "backpack"}

        if self.exchange == "hyperliquid" and self.bp_details is not None:
            raise PositionLogicError(
                "exchange_details_consistency",
                "Backpack details (bp_details) must be None for a Hyperliquid position",
                exchange=self.exchange,
                fields={"exchange": self.exchange, "bp_details": self.bp_details},
            )
        if self.exchange == "backpack" and self.hl_details is not None:
            raise PositionLogicError(
                "exchange_details_consistency",
                "Hyperliquid details (hl_details) must be None for a Backpack position",
                exchange=self.exchange,
                fields={"exchange": self.exchange, "hl_details": self.hl_details},
            )

        # Add check for unrecognized exchanges having details
        if (self.exchange not in known_exchanges_with_details) and (
            self.hl_details is not None or self.bp_details is not None
        ):
            raise PositionLogicError(
                "unrecognized_exchange_details",
                f"Exchange-specific details provided for unrecognized exchange: {self.exchange}",
                exchange=self.exchange,
                fields={
                    "exchange": self.exchange,
                    "hl_details": self.hl_details,
                    "bp_details": self.bp_details,
                },
            )


# --- Derivative Position Details & Sub-Models (INTERNAL) ---


class HyperliquidPositionDetails(BaseModel):
    """Immutable exchange-specific details for a Hyperliquid position (Internal)."""

    leverage_type: str = Field(...)  # 'cross' or 'isolated'
    leverage_value: int = Field(..., ge=0)
    max_leverage: int = Field(..., ge=0)
    margin_used: Decimal | None = Field(default=None, ge=Decimal(0))

    # Config: Immutable, ignore extra fields during creation
    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=False)

    @field_validator("leverage_type", mode="before")
    @classmethod
    def validate_leverage_type(cls, v: object, info: ValidationInfo) -> str:
        """Validate leverage_type is 'cross' or 'isolated'.

        Args:
            v: The value to validate
            info: Validation context containing field information

        Returns:
            Validated leverage type string

        Raises:
            FieldNameMissingError: If validation fails due to field errors
        """
        field_name = info.field_name or "leverage_type"
        allowed_values: set[str] = {"cross", "isolated"}
        try:
            s = validate_str_field(v, field_name=field_name, max_length=16)
            # Use helper for enum check
            return validate_enum_field(s, allowed=allowed_values, field_name=field_name)
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            raise FieldNameMissingError from e

    @field_validator("leverage_value", "max_leverage", mode="before")
    @classmethod
    def validate_leverage_int(cls, v: object, info: ValidationInfo) -> int:
        """Validate leverage values are non-negative integers.

        Args:
            v: The value to validate
            info: Validation context containing field information

        Returns:
            Validated non-negative integer

        Raises:
            FieldNameMissingError: If field name is None
            TypeFieldError: If value is not an integer
            RequiredFieldNoneError: If value is negative
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        if not isinstance(v, int):
            raise TypeFieldError(field_name, "int", type(v).__name__, actual_value=v)
        if v < 0:
            raise RequiredFieldNoneError(field_name, "Must be non-negative")
        return v

    @field_validator("margin_used", mode="before")
    @classmethod
    def parse_optional_decimal_finite(  # Renamed for clarity
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite if present.

        Args:
            v: The value to parse (string, float, Decimal, or None)
            info: Validation context containing field information

        Returns:
            Parsed finite decimal value or None if not provided

        Raises:
            FieldNameMissingError: If field name is None
            DecimalFiniteError: If parsed value is not finite
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        # Check finiteness if not None. ge=0 handled by Field constraint.
        if parsed is not None and not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed, context="if provided")
        return parsed


class BackpackPositionDetails(BaseModel):
    """Immutable exchange-specific details for a Backpack position (Internal)."""

    leverage: int | None = Field(default=None, ge=0)  # Current leverage value
    imf_base: Decimal | None = Field(default=None)
    imf_factor: Decimal | None = Field(default=None)
    mmf_base: Decimal | None = Field(default=None)
    mmf_factor: Decimal | None = Field(default=None)
    cumulative_funding: Decimal | None = Field(default=None)

    # Config: Immutable, ignore extra fields during creation
    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=False)

    # Use single validator for all optional decimals
    @field_validator(
        "imf_base",
        "imf_factor",
        "mmf_base",
        "mmf_factor",
        "cumulative_funding",
        mode="before",
    )
    @classmethod
    def parse_optional_decimal_finite(  # Renamed for clarity and consistency
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite if present.

        Args:
            v: The value to parse (string, float, Decimal, or None)
            info: Validation context containing field information

        Returns:
            Parsed finite decimal value or None if not provided

        Raises:
            FieldNameMissingError: If field name is None
            DecimalFiniteError: If parsed value is not finite
        """
        field_name = info.field_name
        if field_name is None:
            raise FieldNameMissingError
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        # Check finiteness if not None
        if parsed is not None and not parsed.is_finite():
            raise DecimalFiniteError(field_name, parsed, context="if provided")
        return parsed
