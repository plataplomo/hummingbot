"""Core fill models for CyberDeltaEngine.

This module defines the internal Fill model and exchange-specific enrichment details
for representing executed fills across all supported exchanges. The Fill model follows
the "Core + Typed Extension Slots" pattern for exchange-specific data.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Self

from pydantic import (
    Field,
    computed_field,
    field_validator,
    model_validator,
)

from cyberdelta.enums import MakerTaker, OrderSide
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.field_validation import FillLogicError
from cyberdelta.models.base_validators import (
    ExchangeValidationMixin,
    ExtensionSlotModel,
    ImmutableModel,
    optional_decimal_validator,
    required_datetime_validator,
    required_decimal_validator,
)
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.parsing import validate_str_field


class Fill(ExchangeValidationMixin, ImmutableModel):
    """Lean core internal model for a single execution event (fill) across all supported exchanges.

    Contains only essential, universal fields. Immutable, robust, and validated.

    Fields:
        id (str): Fill ID (string, unique per exchange fill)
        symbol (Symbol): Trading symbol domain object
        executed_at (datetime): UTC timestamp of execution
        side (OrderSide): Buy or sell
        order_id (str): Exchange order ID
        exchange (str): Exchange name (internal, required)
        client_order_id (Optional[str]): Client-generated order ID
        price (Decimal): Execution price (must be positive)
        quantity (Decimal): Executed quantity (must be positive)
        fee (Decimal): Fee paid for this fill (can be negative for rebates/promotions)
        fee_asset (Optional[str]): Asset in which the fee was paid (required if fee != 0)
        maker_taker (Optional[MakerTaker]): MAKER or TAKER, None if unknown
        hl_details (Optional[HyperliquidFillDetails]): Hyperliquid-specific enrichment slot
        bp_details (Optional[BackpackFillDetails]): Backpack-specific enrichment slot
    """

    id: str
    symbol: Symbol
    executed_at: datetime
    side: OrderSide
    order_id: str
    exchange: ExchangeName
    price: Decimal = Field(gt=Decimal(0))
    quantity: Decimal = Field(gt=Decimal(0))
    client_order_id: str | None = Field(default=None)
    fee: Decimal = Field(default=Decimal(0))
    fee_asset: str | None = Field(default=None)
    maker_taker: MakerTaker | None = Field(default=None)
    hl_details: HyperliquidFillDetails | None = Field(default=None)
    bp_details: BackpackFillDetails | None = Field(default=None)

    # Config: Immutable (inherited from ImmutableModel)
    # Exchange validation: ExchangeValidationMixin provides validate_exchange()

    @field_validator("id", "order_id", mode="before")
    @classmethod
    def validate_id_fields(cls, v: str, info: object) -> str:
        """Validate fill and order ID fields with appropriate length limits.

        Args:
            v: ID field value to validate
            info: Pydantic field validation context

        Returns:
            Validated ID string
        """
        field_name = getattr(info, "field_name", "id")
        # max_length=128 is a generous default; revisit if stricter limits are found in
        # exchange specs
        return validate_str_field(v, field_name=str(field_name), max_length=128)

    # Exchange validation provided by ExchangeValidationMixin

    _validate_executed_at = required_datetime_validator("executed_at")
    _validate_required_decimals = required_decimal_validator("price", "quantity", "fee")

    @field_validator("client_order_id", "fee_asset", mode="before")
    @classmethod
    def validate_optional_str(cls, v: str | None, info: object) -> str | None:
        """Validate optional string fields with length limits.

        Ensures optional string fields are properly validated when present,
        with appropriate length constraints for database and API compatibility.

        Args:
            v: Raw string value or None
            info: Pydantic field validation context

        Returns:
            Validated string or None if not provided

        """
        field_name = getattr(info, "field_name", None)
        if v is None:
            return None
        return validate_str_field(v, field_name=str(field_name), max_length=64)

    @model_validator(mode="after")
    def check_fee_logic(self) -> Self:
        """Validate fee-related business logic constraints.

        Ensures that when a fee is charged (non-zero), the fee asset is specified.
        This maintains data integrity for fee tracking and accounting.

        Returns:
            Self for method chaining

        Raises:
            FillLogicError: If fee is non-zero but fee_asset is not provided

        """
        if self.fee != Decimal(0) and not self.fee_asset:
            raise FillLogicError(
                validation_type="fee_asset_required",
                message="fee_asset must be provided if fee is nonzero",
                fill_id=self.id,
                fields={"fee": self.fee, "fee_asset": self.fee_asset},
            )
        return self

    @computed_field
    def cost(self) -> Decimal:
        """Total cost (price * quantity) for this fill.

        Returns:
            Total cost as Decimal
        """
        return self.price * self.quantity

    def to_dict(self) -> dict[str, Any]:
        """Subject to deprecation: Prefer model_dump(mode='json') for future serialization.

        Returns:
            Dictionary representation with serialized values
        """
        data = self.model_dump()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = str(value)
            elif isinstance(value, (OrderSide, MakerTaker)):
                data[key] = value.value
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data


class HyperliquidFillDetails(ExtensionSlotModel):
    """Hyperliquid-specific fill enrichment fields for extension slot on Fill.

    Fields:
        fill_hash (str): Unique fill hash (ApiUserFill.hash)
        liquidation_mark_px (Optional[Decimal]): Mark price at liquidation
            (ApiUserFill.liquidationMarkPx)
        start_position (Optional[Decimal]): Position size before fill
            (ApiUserFill.startPosition)
        dir (Optional[str]): Direction of fill (ApiUserFill.dir).
            Enum validation to be added if values are known.
    """

    fill_hash: str
    liquidation_mark_px: Decimal | None = None
    start_position: Decimal | None = None
    dir: str | None = None

    # Use centralized validators
    _validate_optional_decimals = optional_decimal_validator(
        "liquidation_mark_px", "start_position"
    )

    @field_validator("fill_hash", mode="before")
    @classmethod
    def validate_fill_hash(cls, v: str, info: object) -> str:
        """Validate and sanitize the Hyperliquid fill hash field.

        Ensures the fill hash is a non-empty string with reasonable length limits
        to prevent malformed or excessively long hash values from external APIs.

        Args:
            v: Raw fill hash value from external source
            info: Pydantic field validation context

        Returns:
            Validated fill hash string

        """
        return validate_str_field(v, field_name="fill_hash", max_length=128)

    @field_validator("dir", mode="before")
    @classmethod
    def validate_dir(cls, v: str | None, info: object) -> str | None:
        """Validate and sanitize the Hyperliquid direction field.

        Validates the optional direction field from Hyperliquid's ApiUserFill.dir.
        This field indicates the direction of the fill relative to the position.

        Args:
            v: Raw direction value from external source (can be None)
            info: Pydantic field validation context

        Returns:
            Validated direction string or None if not provided

        """
        if v is None:
            return None
        # TODO: Replace with enum validation if/when values are known
        return validate_str_field(v, field_name="dir", max_length=32)


class BackpackFillDetails(ExtensionSlotModel):
    """Backpack-specific fill enrichment fields for extension slot on Fill.

    Fields:
        system_order_type (Optional[str]): Type of system order that triggered the fill
            (OrderFill.systemOrderType). Enum validation to be added if values are known.
    """

    system_order_type: str | None = None

    @field_validator("system_order_type", mode="before")
    @classmethod
    def validate_system_order_type(cls, v: str | None, info: object) -> str | None:
        """Validate and sanitize the Backpack system order type field.

        Validates the optional system order type from Backpack's OrderFill.systemOrderType.
        This field indicates the type of system order that triggered the fill
        (e.g., stop-loss, take-profit, liquidation).

        Args:
            v: Raw system order type value from external source (can be None)
            info: Pydantic field validation context

        Returns:
            Validated system order type string or None if not provided

        """
        if v is None:
            return None
        # TODO: Replace with enum validation if/when values are known
        return validate_str_field(v, field_name="system_order_type", max_length=32)
