"""Core trade models for CyberDeltaEngine.

This module defines the internal Trade model and exchange-specific enrichment details
for representing executed trades across all supported exchanges. The Trade model follows
the "Core + Typed Extension Slots" pattern for exchange-specific data.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)

from cyberdelta.enums import OrderSide
from cyberdelta.exceptions.field_validation import (
    DecimalFiniteError,
    RequiredFieldNoneError,
    TradeLogicError,
)
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field


class Trade(BaseModel):
    """Lean core internal model for a single execution event (fill) across all supported exchanges.

    Contains only essential, universal fields. Immutable, robust, and validated.

    Fields:
        id (str): Trade ID (string, unique per exchange fill)
        symbol (str): Trading symbol (e.g., 'BTC-PERP')
        executed_at (datetime): UTC timestamp of execution
        side (OrderSide): Buy or sell
        order_id (str): Exchange order ID
        exchange (str): Exchange name (internal, required)
        client_order_id (Optional[str]): Client-generated order ID
        price (Decimal): Execution price (must be positive)
        quantity (Decimal): Executed quantity (must be positive)
        fee (Decimal): Fee paid for this trade (can be negative for rebates/promotions)
        fee_asset (Optional[str]): Asset in which the fee was paid (required if fee != 0)
        is_maker (Optional[bool]): True if maker fill, False if taker, None if unknown
        hl_details (Optional[HyperliquidTradeDetails]): Hyperliquid-specific enrichment slot
        bp_details (Optional[BackpackTradeDetails]): Backpack-specific enrichment slot
    """

    id: str
    symbol: str
    executed_at: datetime
    side: OrderSide
    order_id: str
    exchange: str
    price: Decimal = Field(gt=Decimal(0))
    quantity: Decimal = Field(gt=Decimal(0))
    client_order_id: str | None = Field(default=None)
    fee: Decimal = Field(default=Decimal(0))
    fee_asset: str | None = Field(default=None)
    is_maker: bool | None = Field(default=None)
    hl_details: HyperliquidTradeDetails | None = Field(default=None)
    bp_details: BackpackTradeDetails | None = Field(default=None)

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("id", "order_id", mode="before")
    @classmethod
    def validate_id_fields(cls, v: str, info: object) -> str:
        """Validate trade and order ID fields with appropriate length limits.
        
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

    @field_validator("symbol", "exchange", mode="before")
    @classmethod
    def validate_symbol_exchange(cls, v: str, info: object) -> str:
        """Validate symbol and exchange fields with shorter length limits.
        
        Args:
            v: Symbol or exchange field value to validate
            info: Pydantic field validation context
            
        Returns:
            Validated string value
        """
        field_name = getattr(info, "field_name", None)
        return validate_str_field(v, field_name=str(field_name), max_length=64)

    @field_validator("executed_at", mode="before")
    @classmethod
    def parse_executed_at(
        cls,
        raw_value: str | float | datetime | None,
        info: object,
    ) -> datetime:
        """Parse and validate the execution timestamp field.

        Converts various timestamp formats to a UTC datetime object for consistent
        internal representation. Ensures the timestamp is not None.

        Args:
            raw_value: Raw timestamp value from external source
            info: Pydantic field validation context

        Returns:
            Validated UTC datetime object

        Raises:
            RequiredFieldNoneError: If timestamp cannot be parsed or is None

        """
        dt = parse_datetime_utc(raw_value, field_name="executed_at")
        if dt is None:
            raise RequiredFieldNoneError(
                field_name="executed_at",
                reason="Trade execution timestamp is required and cannot be None",
            )
        return dt

    @field_validator("price", "quantity", "fee", mode="before")
    @classmethod
    def parse_decimal_fields(
        cls,
        raw_value: str | float | Decimal | None,
        info: object,
    ) -> Decimal:
        """Parse and validate decimal fields for financial precision.

        Converts various numeric formats to finite Decimal objects for precise
        financial calculations. Ensures all values are finite and valid.

        Args:
            raw_value: Raw numeric value from external source
            info: Pydantic field validation context

        Returns:
            Validated finite Decimal object

        Raises:
            DecimalFiniteError: If value cannot be parsed to finite Decimal

        """
        field_name = getattr(info, "field_name", None)
        d = parse_decimal_value(raw_value, allow_none=False, field_name=str(field_name))
        # allow_none=False ensures d is never None
        if not d.is_finite():
            raise DecimalFiniteError(
                field_name=str(field_name),
                value=d,
                context="for trade financial calculations",
            )
        return d

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
            TradeLogicError: If fee is non-zero but fee_asset is not provided

        """
        if self.fee != Decimal(0) and not self.fee_asset:
            raise TradeLogicError(
                validation_type="fee_asset_required",
                message="fee_asset must be provided if fee is nonzero",
                trade_id=self.id,
                fields={"fee": self.fee, "fee_asset": self.fee_asset},
            )
        return self

    @computed_field
    def cost(self) -> Decimal:
        """Total cost (price * quantity) for this trade.
        
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
            elif isinstance(value, OrderSide):
                data[key] = value.value
            elif isinstance(value, datetime):
                data[key] = value.isoformat()
        return data


class HyperliquidTradeDetails(BaseModel):
    """Hyperliquid-specific trade enrichment fields for extension slot on Trade.

    Fields:
        trade_hash (str): Unique trade hash (ApiUserFill.hash)
        liquidation_mark_px (Optional[Decimal]): Mark price at liquidation
            (ApiUserFill.liquidationMarkPx)
        start_position (Optional[Decimal]): Position size before fill
            (ApiUserFill.startPosition)
        dir (Optional[str]): Direction of fill (ApiUserFill.dir).
            Enum validation to be added if values are known.
    """

    trade_hash: str
    liquidation_mark_px: Decimal | None = None
    start_position: Decimal | None = None
    dir: str | None = None

    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("trade_hash", mode="before")
    @classmethod
    def validate_trade_hash(cls, v: str, info: object) -> str:
        """Validate and sanitize the Hyperliquid trade hash field.

        Ensures the trade hash is a non-empty string with reasonable length limits
        to prevent malformed or excessively long hash values from external APIs.

        Args:
            v: Raw trade hash value from external source
            info: Pydantic field validation context

        Returns:
            Validated trade hash string

        """
        return validate_str_field(v, field_name="trade_hash", max_length=128)

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

    @field_validator("liquidation_mark_px", "start_position", mode="before")
    @classmethod
    def validate_decimals(
        cls,
        v: str | float | Decimal | None,
        info: object,
    ) -> Decimal | None:
        """Validate and parse Hyperliquid decimal fields to ensure financial precision.

        Converts raw numeric values from external APIs to validated Decimal objects
        for precise financial calculations. Handles liquidation mark price and
        starting position size fields from Hyperliquid's ApiUserFill structure.

        Args:
            v: Raw numeric value from external source (can be None)
            info: Pydantic field validation context containing field name

        Returns:
            Validated finite Decimal or None if not provided

        Raises:
            DecimalFiniteError: If value cannot be parsed to finite Decimal

        """
        if v is None:
            return None
        field_name = getattr(info, "field_name", "unknown")
        d = parse_decimal_value(v, allow_none=False, field_name=field_name)
        # allow_none=False ensures d is never None
        if not d.is_finite():
            raise DecimalFiniteError(
                field_name=str(field_name),
                value=d,
                context="for Hyperliquid trade details",
            )
        return d


class BackpackTradeDetails(BaseModel):
    """Backpack-specific trade enrichment fields for extension slot on Trade.

    Fields:
        system_order_type (Optional[str]): Type of system order that triggered the fill
            (OrderFill.systemOrderType). Enum validation to be added if values are known.
    """

    system_order_type: str | None = None

    model_config = ConfigDict(extra="ignore", frozen=True)

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
