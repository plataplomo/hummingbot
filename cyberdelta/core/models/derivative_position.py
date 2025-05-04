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

from cyberdelta.core.models.enums import OrderSide
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field

# --- Derivative Position Details & Sub-Models ---

# Note: These sub-models represent raw data structures often received from APIs.
# They should be validated but kept simple.


class HyperliquidRawLeverage(BaseModel):
    """Raw leverage details from Hyperliquid API."""

    type: str  # e.g., "cross", "isolated"
    value: int = Field(ge=0)

    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type_str(cls, v: object) -> str:
        return validate_str_field(v, field_name="type", max_length=32)


class HyperliquidPositionDetails(BaseModel):
    """Immutable exchange-specific details for a Hyperliquid position."""

    leverage: HyperliquidRawLeverage
    max_leverage: int = Field(ge=0)
    margin_used: Decimal | None = Field(default=None, ge=Decimal("0"))

    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=True)

    @field_validator("margin_used", mode="before")
    @classmethod
    def parse_optional_decimal(
        cls, v: str | int | float | Decimal | None, info: ValidationInfo
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite if present."""
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name)
        # DEFENSIVE CHECK: Ensure finite if not None.
        if parsed is not None and not parsed.is_finite():
            # This should be practically unreachable due to Pydantic's validation flow
            # if info.field_name is None: # Check already performed
            #     raise ValueError("Field name is unexpectedly None during validation.")
            raise ValueError(f"{field_name}: Value must be finite if provided")
        return parsed


class BackpackRawImfFunction(BaseModel):
    """Raw Initial Margin Fraction (IMF) function parameters from Backpack API."""

    a: Decimal
    b: Decimal
    c: Decimal

    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=True)

    @field_validator("a", "b", "c", mode="before")
    @classmethod
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: ValidationInfo) -> Decimal:
        """Parse required decimal, ensuring finite."""
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name)
        # DEFENSIVE CHECK: Explicitly require finite values post-parse. Mypy=[unreachable]
        if parsed is None:
            # parse_decimal_value should raise if allow_none=False is implied by return type Decimal
            # if info.field_name is None: # Check already performed
            #     raise ValueError("Field name is unexpectedly None during validation.")
            raise ValueError(f"{field_name}: Value cannot be None")
        # DEFENSIVE CHECK: Ensure value is finite. Mypy=[possibly-undefined]
        if not parsed.is_finite():
            # This should be practically unreachable due to Pydantic's validation flow
            # if info.field_name is None: # Check already performed
            #     raise ValueError("Field name is unexpectedly None during validation.")
            raise ValueError(f"{field_name}: Value must be finite")
        return parsed


class BackpackRawMmfFunction(BaseModel):
    """Raw Maintenance Margin Fraction (MMF) function parameters from Backpack API."""

    base: Decimal
    factor: Decimal

    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=True)

    @field_validator("base", "factor", mode="before")
    @classmethod
    def parse_decimal(cls, v: str | int | float | Decimal | None, info: ValidationInfo) -> Decimal:
        """Parse required decimal, ensuring finite."""
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name)
        # DEFENSIVE CHECK: Explicitly require finite values post-parse. Mypy=[unreachable]
        if parsed is None:
            # parse_decimal_value should raise if allow_none=False is implied by return type Decimal
            # if info.field_name is None: # Check already performed
            #     raise ValueError("Field name is unexpectedly None during validation.")
            raise ValueError(f"{field_name}: Value cannot be None")
        # DEFENSIVE CHECK: Ensure value is finite. Mypy=[possibly-undefined]
        if not parsed.is_finite():
            # This should be practically unreachable due to Pydantic's validation flow
            # if info.field_name is None: # Check already performed
            #     raise ValueError("Field name is unexpectedly None during validation.")
            raise ValueError(f"{field_name}: Value must be finite")
        return parsed


class BackpackPositionDetails(BaseModel):
    """Immutable exchange-specific details for a Backpack position."""

    cumulative_funding: Decimal | None = Field(default=None)
    imf_function: BackpackRawImfFunction | None = None
    mmf_function: BackpackRawMmfFunction | None = None

    model_config = ConfigDict(extra="ignore", frozen=True, validate_assignment=True)

    @field_validator("cumulative_funding", mode="before")
    @classmethod
    def parse_optional_decimal(
        cls, v: str | int | float | Decimal | None, info: ValidationInfo
    ) -> Decimal | None:
        """Parse optional decimal, ensuring finite if present."""
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name)
        # DEFENSIVE CHECK: Ensure finite if not None.
        if parsed is not None and not parsed.is_finite():
            # This should be practically unreachable due to Pydantic's validation flow
            # if info.field_name is None: # Check already performed
            #     raise ValueError("Field name is unexpectedly None during validation.")
            raise ValueError(f"{field_name}: Value must be finite if provided")
        return parsed


# --- Derivative Position Core Model ---


class DerivativePosition(BaseModel):
    """
    Represents the mutable state of a single derivative (margin/futures/perp) position
    on a specific exchange.

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
        signal_id (str | None): Optional identifier for the signal that originated the position/trade.

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
    mark_price: Decimal | None = Field(default=None, ge=Decimal("0"))
    liquidation_price: Decimal | None = Field(default=None, ge=Decimal("0"))
    unrealized_pnl: Decimal | None = None  # Can be negative
    realized_pnl: Decimal | None = None  # Can be negative
    strategy_name: str | None = None
    signal_id: str | None = None
    # --- Extension Slots ---
    hl_details: HyperliquidPositionDetails | None = Field(default=None)
    bp_details: BackpackPositionDetails | None = Field(default=None)

    model_config = ConfigDict(extra="forbid", validate_assignment=True)  # Mutable

    # --- Field Validators ---

    @field_validator("exchange", "symbol", mode="before")
    @classmethod
    def validate_required_strings(cls, v: object, info: ValidationInfo) -> str:
        """Validate required string fields are non-empty, reasonable length."""
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
        """Validate optional string fields if provided."""
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
        cls, v: str | int | float | Decimal | None, info: ValidationInfo
    ) -> Decimal:
        """Parse required decimal fields, ensuring finite."""
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            # This should be practically unreachable due to Pydantic's validation flow
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name)
        # DEFENSIVE CHECK: Explicitly require finite values post-parse. Mypy=[unreachable]
        if parsed is None:
            # parse_decimal_value should raise if None based on return type Decimal
            # if info.field_name is None: # Check already performed
            #     raise ValueError("Field name is unexpectedly None during validation.")
            raise ValueError(f"{field_name}: Value cannot be None")
        # DEFENSIVE CHECK: Ensure value is finite. Mypy=[possibly-undefined]
        if not parsed.is_finite():
            # This should be practically unreachable due to Pydantic's validation flow
            # if info.field_name is None: # Check already performed
            #     raise ValueError("Field name is unexpectedly None during validation.")
            raise ValueError(f"{field_name}: Value must be finite (not NaN or Infinity)")
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
        cls, v: str | int | float | Decimal | None, info: ValidationInfo
    ) -> Decimal | None:
        """Parse optional decimal fields, ensuring finite if present."""
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            # This should be practically unreachable due to Pydantic's validation flow
            raise ValueError("Field name is unexpectedly None during validation.")
        parsed = parse_decimal_value(v, field_name=field_name)
        # DEFENSIVE CHECK: Ensure finite if not None.
        if parsed is not None and not parsed.is_finite():
            # This should be practically unreachable due to Pydantic's validation flow
            # if info.field_name is None: # Check already performed
            #     raise ValueError("Field name is unexpectedly None during validation.")
            raise ValueError(f"{field_name}: Value must be finite if provided")
        # Additional check for fields that must be non-negative if provided
        if field_name in ("mark_price", "liquidation_price"):
            if parsed is not None and parsed < Decimal("0"):
                raise ValueError(f"{field_name}: Value cannot be negative")
        # Note: entry_price > 0 check happens in model_validator based on size
        return parsed

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_required_datetime(
        cls, v: str | int | float | datetime | None, info: ValidationInfo
    ) -> datetime:
        """Parse required datetime field, ensuring it's not None."""
        # DEFENSIVE CHECK: Explicitly validate field_name is not None before use.
        field_name = info.field_name
        if field_name is None:
            # This should be practically unreachable due to Pydantic's validation flow
            raise ValueError("Field name is unexpectedly None during validation.")
        dt = parse_datetime_utc(v, field_name=field_name)
        # DEFENSIVE CHECK: parse_datetime_utc should raise if None based on return type datetime
        if dt is None:
            # This should be practically unreachable due to Pydantic's validation flow
            # if info.field_name is None: # Check already performed
            #     raise ValueError("Field name is unexpectedly None during validation.")
            raise ValueError(f"{field_name} cannot be None")
        return dt

    # --- Properties ---

    def is_active(self) -> bool:
        """
        Returns True if the position is currently open (size is non-zero), False otherwise.
        """
        # DEFENSIVE CHECK: Ensure size is finite before comparison. Should be guaranteed by validator. Mypy=[redundant-expr]
        if not self.size.is_finite():
            raise ValueError("Position size is not finite, cannot determine active status.")
        return self.size != Decimal("0")

    # --- Model Validator ---

    @model_validator(mode="after")
    def check_position_logic(self) -> Self:
        """
        Ensures logical consistency between size, entry_price, and side.
        1. If size is non-zero, entry_price must be present and positive.
        2. If size is zero, entry_price must be None.
        3. Side must align with the sign of the size (BUY for > 0, SELL for < 0).
           (Size == 0 can technically have either side state depending on last trade,
            but often reset to BUY/None; we allow either if size is zero).
        """
        # Check 1 & 2: Entry price logic vs. size
        if self.size != Decimal("0"):
            if self.entry_price is None or self.entry_price <= Decimal("0"):
                raise ValueError(
                    "Entry price must be provided and positive if position size is non-zero."
                )
        else:  # size == 0
            if self.entry_price is not None:
                # Optionally force entry_price to None when flat, or just warn/allow?
                # Forcing consistency:
                # object.__setattr__(self, "entry_price", None)
                # Raising error:
                raise ValueError("Entry price must be None if position size is zero.")

        # Check 3: Side vs. Size consistency
        if self.size > Decimal("0") and self.side != OrderSide.BUY:
            raise ValueError("Position side must be BUY if size is positive.")
        if self.size < Decimal("0") and self.side != OrderSide.SELL:
            raise ValueError("Position side must be SELL if size is negative.")

        # Check details match exchange (simple check)
        if self.exchange == "hyperliquid" and self.bp_details is not None:
            raise ValueError("Backpack details (bp_details) provided for a Hyperliquid position.")
        if self.exchange == "backpack" and self.hl_details is not None:
            raise ValueError("Hyperliquid details (hl_details) provided for a Backpack position.")
        # Could add checks: If exchange is X, details X should not be None?
        # Depends on whether we always expect details. For now, allow None details.

        return self
