"""
Backpack API Trade Models
------------------------

Strict Pydantic models for validating trade and trade event responses from the Backpack
Exchange API.
These models are used for boundary validation and transformation, not for internal business
logic.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BackpackRawTrade(BaseModel):
    """
    Pydantic model for a raw trade/fill from `/api/v1/trades` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse trade payloads received from the exchange.

    Attributes:
        id (str): Trade ID.
        order_id (str): Associated order ID.
        symbol (str): Trading symbol.
        price (str): Execution price (as string).
        quantity (str): Executed quantity (as string).
        time (int | str | float | None): Execution timestamp.
    """

    id: str = Field(..., alias="id", max_length=64)
    order_id: str = Field(..., alias="orderId", max_length=64)
    symbol: str = Field(..., alias="symbol", max_length=64)
    price: str = Field(..., alias="price", max_length=64)
    quantity: str = Field(..., alias="qty", max_length=64)
    time: int | str | float | None = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("id", "order_id", "symbol", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if type(v) is not str:
            raise ValueError(f"Must be a string (got {type(v).__name__})")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        if len(v) > 64:
            raise ValueError("String value too long (max 64 chars)")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if type(v) is not str:
            raise ValueError("Must be a string representing a decimal value")
        if not v.strip():
            raise ValueError("Must be a non-empty string representing a decimal value")
        try:
            dec_val = Decimal(v)
        except Exception as err:
            raise ValueError("Must be a string representing a decimal value") from err
        if not dec_val.is_finite():
            raise ValueError("Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("time", mode="before")
    @classmethod
    def validate_timestamp(cls, v: int | float | str | None) -> int | float | str | None:
        if v is None:
            return v
        if isinstance(v, int | float):
            return v
        # If not int or float, treat as string
        if v.isdigit():
            return int(v)
        # Accept ISO8601, but do not parse here
        return v


class BackpackRawTradeEvent(BaseModel):
    """
    Pydantic model for a raw trade event from the Backpack WebSocket stream (`trade`).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse trade event payloads received from the exchange.

    Attributes:
        event_type (str): Event type ('trade').
        event_time (int | str | float | None): Event time.
        symbol (str): Trading symbol.
        price (str): Price (as string).
        quantity (str): Quantity (as string).
        buyer_order_id (str): Buyer order ID.
        seller_order_id (str): Seller order ID.
        trade_id (str): Trade ID.
        engine_timestamp (int | str | float | None): Engine timestamp.
        is_buyer_the_maker (bool): Is buyer the maker?
    """

    event_type: str = Field(..., alias="e")
    event_time: int | str | float | None = Field(..., alias="E")
    symbol: str = Field(..., alias="s")
    price: str = Field(..., alias="p")
    quantity: str = Field(..., alias="q")
    buyer_order_id: str = Field(..., alias="b")
    seller_order_id: str = Field(..., alias="a")
    trade_id: str = Field(..., alias="t")
    engine_timestamp: int | str | float | None = Field(..., alias="T")
    is_buyer_the_maker: bool = Field(..., alias="m")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator(
        "event_type",
        "symbol",
        "price",
        "quantity",
        "buyer_order_id",
        "seller_order_id",
        "trade_id",
        mode="before",
    )
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if type(v) is not str:
            raise ValueError(f"Must be a string (got {type(v).__name__})")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"Must be a valid unicode string (got {type(v).__name__})") from err
        return v

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        if type(v) is not str:
            raise ValueError("Must be a string representing a decimal value")
        if not v.strip():
            raise ValueError("Must be a non-empty string representing a decimal value")
        try:
            dec_val = Decimal(v)
        except Exception as err:
            raise ValueError("Must be a string representing a decimal value") from err
        if not dec_val.is_finite():
            raise ValueError("Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("event_time", "engine_timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: int | float | str | None) -> int | float | str | None:
        if v is None:
            return v
        if isinstance(v, int | float):
            return v
        # If not int or float, treat as string
        if v.isdigit():
            return int(v)
        # Accept ISO8601, but do not parse here
        return v
