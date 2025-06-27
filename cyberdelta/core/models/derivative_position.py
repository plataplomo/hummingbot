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
from cyberdelta.core.models.enums import OrderSide
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
        symbol (str): Trading symbol (e.g., 'BTC-PERP').
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
    symbol: str
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

    @field_validator("exchange", "symbol", mode="before")
    @classmethod
    def validate_required_strings(cls, v: object, info: ValidationInfo) -> str:
        """Validate required string fields are non-empty, reasonable length.

        Args:
            v: The value to validate
            info: Validation context containing field information

        Returns:
            Validated string value

        Raises:
            ValueError: If field name is None or string validation fails
        """
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            # This should be practically unreachable due to Pydantic's validation flow
            raise ValueError("Field name is unexpectedly None during validation.")
        # Assuming validate_str_field internally handles None check if required
        return validate_str_field(v, field_name=field_name, max_length=64)

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
            ValueError: If field name is None or string validation fails
        """
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        # Allow None, but if a string is passed, validate it
        if v is None:
            return None
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        return validate_str_field(v, field_name=field_name, max_length=128)

    @field_validator("size", mode="before")
    @classmethod
    def parse_required_decimal(
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal:
        """Parse required decimal ('size'), ensuring finite."""
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name)
        # DEFENSIVE CHECK: Explicitly require non-None and finite values post-parse.
        if parsed is None:
            raise ValueError(f"{field_name}: Value cannot be None")
        # DEFENSIVE CHECK: Ensure value is finite. Mypy=[possibly-undefined]
        if not parsed.is_finite():
            raise ValueError(f"{field_name}: Value must be finite")
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
        """Parse optional decimals, ensuring finite if present."""
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name)
        # DEFENSIVE CHECK: Ensure finite if not None. Mypy=[redundant-expr]
        if parsed is not None and not parsed.is_finite():
            raise ValueError(f"{field_name}: Value must be finite if provided")
        return parsed

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_required_datetime(
        cls,
        v: str | float | datetime | None,
        info: ValidationInfo,
    ) -> datetime:
        """Parse required datetime, ensuring UTC."""
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        dt = parse_datetime_utc(v, field_name=field_name)
        # DEFENSIVE CHECK: Explicitly require non-None. Mypy=[unreachable]
        if dt is None:
            raise ValueError(f"{field_name}: Value cannot be None")
        return dt

    # --- Instance Methods ---

    def is_active(self) -> bool:
        """Check if the position has a non-zero size."""
        return self.size != Decimal(0)

    # --- Model Validators ---

    @model_validator(mode="after")
    def check_position_logic(self) -> Self:
        """Validate cross-field consistency (entry_price, side/size, details slots)."""
        self._validate_entry_price_logic()
        self._validate_side_size_logic()
        self._validate_extension_slot_consistency()
        return self

    def _validate_entry_price_logic(self) -> None:
        """Validate entry price consistency with position size."""
        if self.size != Decimal(0):
            if self.entry_price is None:
                raise ValueError("entry_price must be provided if size is non-zero")
            if self.entry_price <= Decimal(0):
                raise ValueError("entry_price must be positive (> 0) if size is non-zero")
        elif self.entry_price is not None:
            raise ValueError("entry_price must be None if size is zero")

    def _validate_side_size_logic(self) -> None:
        """Validate side consistency with position size."""
        if self.size > Decimal(0) and self.side != OrderSide.BUY:
            raise ValueError("side must be BUY if size is positive")
        if self.size < Decimal(0) and self.side != OrderSide.SELL:
            raise ValueError("side must be SELL if size is negative")

    def _validate_extension_slot_consistency(self) -> None:
        """Validate exchange-specific details consistency (Idea 5)."""
        known_exchanges_with_details = {"hyperliquid", "backpack"}

        if self.exchange == "hyperliquid" and self.bp_details is not None:
            raise ValueError(
                "Backpack details (bp_details) must be None for a Hyperliquid position",
            )
        if self.exchange == "backpack" and self.hl_details is not None:
            raise ValueError(
                "Hyperliquid details (hl_details) must be None for a Backpack position",
            )

        # Add check for unrecognized exchanges having details
        if (self.exchange not in known_exchanges_with_details) and (
            self.hl_details is not None or self.bp_details is not None
        ):
            raise ValueError(
                f"Exchange-specific details provided for unrecognized exchange: {self.exchange}",
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
        """Validate leverage_type is 'cross' or 'isolated'."""
        field_name = info.field_name or "leverage_type"
        allowed_values: set[str] = {"cross", "isolated"}
        try:
            s = validate_str_field(v, field_name=field_name, max_length=16)
            # Use helper for enum check
            return validate_enum_field(s, allowed=allowed_values, field_name=field_name)
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e

    @field_validator("leverage_value", "max_leverage", mode="before")
    @classmethod
    def validate_leverage_int(cls, v: object, info: ValidationInfo) -> int:
        """Validate leverage values are non-negative integers."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        if not isinstance(v, int):
            raise ValueError(f"{field_name}: Expected int, got {type(v).__name__}")
        if v < 0:
            raise ValueError(f"{field_name}: Must be non-negative")
        return v

    @field_validator("margin_used", mode="before")
    @classmethod
    def parse_optional_decimal_finite(  # Renamed for clarity
        cls,
        v: str | float | Decimal | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite if present."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        # Check finiteness if not None. ge=0 handled by Field constraint.
        if parsed is not None and not parsed.is_finite():
            raise ValueError(f"{field_name}: Value must be finite if provided")
        return parsed


class BackpackPositionDetails(BaseModel):
    """Immutable exchange-specific details for a Backpack position (Internal)."""

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
        """Parse optional decimal, ensuring finite if present."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name, allow_none=True)
        # Check finiteness if not None
        if parsed is not None and not parsed.is_finite():
            raise ValueError(f"{field_name}: Value must be finite if provided")
        return parsed
