"""CyberDeltaEngine derivative position models.

Provides the DerivativePosition model for tracking leveraged positions across exchanges,
following the "Core + Typed Extension Slots" pattern for exchange-specific details.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Self


if TYPE_CHECKING:
    from cyberdelta.models.market.fill import Fill

from pydantic import (
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from cyberdelta.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions import (
    FieldNameMissingError,
    PositionLogicError,
    RequiredFieldNoneError,
    TypeFieldError,
)
from cyberdelta.models.base_validators import (
    ExchangeValidationMixin,
    ExtensionSlotModel,
    StandardModel,
    optional_decimal_validator,
    required_datetime_validator,
    required_decimal_validator,
)

# Correctly import the Raw model ONLY for transformation logic, not direct use in internal models
# (Although for Details, we usually transform *before* creating Details)
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.parsing import (
    validate_enum_field,
    validate_str_field,
)


# --- Derivative Position Core Model ---


class DerivativePosition(ExchangeValidationMixin, StandardModel):
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

    exchange: ExchangeName
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

    # Config: Mutable (inherited from StandardModel with validate_assignment=True)
    # Exchange validation: ExchangeValidationMixin provides validate_exchange()

    # --- Field Validators ---

    # Exchange validation provided by ExchangeValidationMixin
    # Symbol validation is handled by Pydantic's type system

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

    # Validation using convenience validators from base_validators.py
    _validate_size = required_decimal_validator("size")
    _validate_optional_decimals = optional_decimal_validator(
        "entry_price", "mark_price", "liquidation_price", "unrealized_pnl", "realized_pnl"
    )
    _validate_timestamp = required_datetime_validator("timestamp")

    # --- Instance Methods ---

    def is_active(self) -> bool:
        """Check if the position has a non-zero size.

        Returns:
            True if position size is non-zero, False otherwise
        """
        return self.size != Decimal(0)

    def apply_fill(self, fill: Fill) -> tuple[Decimal | None, Decimal]:
        """Apply a fill to this position and calculate realized PnL.

        This method encapsulates the business logic for updating a position based on a fill,
        including calculating realized PnL when reducing/closing positions and updating
        the average entry price.

        Args:
            fill: The fill to apply to this position

        Returns:
            Tuple of (realized_pnl, new_average_price)
            - realized_pnl: Realized PnL if position was reduced/closed, None otherwise
            - new_average_price: Updated average entry price after the fill

        Note:
            This method calculates but does not update the position. The caller is responsible
            for updating the position fields based on the returned values.
        """
        # Calculate position change and realized PnL
        new_quantity, realized_pnl = self._calculate_position_change(fill)

        # Calculate new average price
        new_average_price = self._calculate_average_price(fill, new_quantity)

        return realized_pnl, new_average_price

    def _calculate_position_change(self, fill: Fill) -> tuple[Decimal, Decimal | None]:
        """Calculate position change from fill.

        Args:
            fill: New fill to apply

        Returns:
            Tuple of (new_quantity, realized_pnl)
            - new_quantity: New position quantity (signed)
            - realized_pnl: Realized PnL if reducing/closing, None otherwise
        """
        # Current position quantity (signed)
        current_qty = self.size
        if self.side == OrderSide.SELL:
            current_qty = -current_qty

        # Fill quantity (signed)
        fill_qty = fill.quantity
        if fill.side == OrderSide.SELL:
            fill_qty = -fill_qty

        # New position quantity
        new_qty = current_qty + fill_qty

        # Calculate realized PnL if reducing/closing position
        realized_pnl = None
        if current_qty != 0 and abs(new_qty) < abs(current_qty):
            # Position is being reduced
            reduced_qty = abs(current_qty) - abs(new_qty)
            if self.entry_price:
                if current_qty > 0:  # Was long
                    realized_pnl = reduced_qty * (fill.price - self.entry_price)
                else:  # Was short
                    realized_pnl = reduced_qty * (self.entry_price - fill.price)

        return new_qty, realized_pnl

    def _calculate_average_price(self, fill: Fill, new_quantity: Decimal) -> Decimal:
        """Calculate new average price after fill.

        Args:
            fill: New fill
            new_quantity: New position quantity (signed)

        Returns:
            New average price
        """
        # If position flipped sides, use trade price
        old_signed_qty = self.size
        if self.side == OrderSide.SELL:
            old_signed_qty = -old_signed_qty

        if (old_signed_qty > 0 and new_quantity < 0) or (old_signed_qty < 0 and new_quantity > 0):
            return fill.price

        # Calculate weighted average for same-side fills
        if not self.entry_price:
            return fill.price

        old_value = abs(old_signed_qty) * self.entry_price
        fill_value = fill.quantity * fill.price
        total_value = old_value + fill_value
        total_quantity = abs(old_signed_qty) + fill.quantity

        if total_quantity == 0:
            return fill.price

        return total_value / total_quantity

    def calculate_unrealized_pnl(self, mark_price: Decimal) -> Decimal | None:
        """Calculate current unrealized PnL based on mark price.

        Args:
            mark_price: Current mark price

        Returns:
            Unrealized PnL or None if position is flat
        """
        if self.size == Decimal(0) or not self.entry_price:
            return None

        if self.side == OrderSide.BUY:
            return self.size * (mark_price - self.entry_price)
        # SELL
        return abs(self.size) * (self.entry_price - mark_price)

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
        known_exchanges_with_details = {ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK}

        if self.exchange == ExchangeName.HYPERLIQUID and self.bp_details is not None:
            raise PositionLogicError(
                "exchange_details_consistency",
                "Backpack details (bp_details) must be None for a Hyperliquid position",
                exchange=self.exchange,
                fields={"exchange": self.exchange, "bp_details": self.bp_details},
            )
        if self.exchange == ExchangeName.BACKPACK and self.hl_details is not None:
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


class HyperliquidPositionDetails(ExtensionSlotModel):
    """Immutable exchange-specific details for a Hyperliquid position (Internal)."""

    leverage_type: str = Field(...)  # 'cross' or 'isolated'
    leverage_value: int = Field(..., ge=0)
    max_leverage: int = Field(..., ge=0)
    margin_used: Decimal | None = Field(default=None, ge=Decimal(0))

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

    _validate_margin_used = optional_decimal_validator("margin_used")


class BackpackPositionDetails(ExtensionSlotModel):
    """Immutable exchange-specific details for a Backpack position (Internal)."""

    leverage: int | None = Field(default=None, ge=0)  # Current leverage value
    imf_base: Decimal | None = Field(default=None)
    imf_factor: Decimal | None = Field(default=None)
    mmf_base: Decimal | None = Field(default=None)
    mmf_factor: Decimal | None = Field(default=None)
    cumulative_funding: Decimal | None = Field(default=None)

    _validate_optional_decimals = optional_decimal_validator(
        "imf_base", "imf_factor", "mmf_base", "mmf_factor", "cumulative_funding"
    )
