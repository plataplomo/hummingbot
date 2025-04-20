"""
Backpack API Market, Ticker, and Open Interest Models
"""

from decimal import Decimal, InvalidOperation

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BackpackRawMarket(BaseModel):
    """
    Raw market metadata object from `/api/v1/markets`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /markets)
    Fields:
        symbol: Trading symbol (str)
        base_asset: Base asset symbol (str)
        quote_asset: Quote asset symbol (str)
    """

    symbol: str = Field(..., alias="symbol", max_length=64)
    base_asset: str = Field(..., alias="baseAsset", max_length=64)
    quote_asset: str = Field(..., alias="quoteAsset", max_length=64)
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("symbol", "base_asset", "quote_asset", mode="before")
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


class BackpackRawTicker(BaseModel):
    """
    Raw ticker object from `/api/v1/ticker`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /ticker)
    Fields:
        symbol: Trading symbol (str)
        price: Last traded price (as string, optional)
        bid: Best bid price (as string, optional)
        ask: Best ask price (as string, optional)
        volume: 24h trading volume (as string, optional)
        time: Ticker timestamp (int | str | float | None)
    """

    symbol: str = Field(..., alias="symbol")
    price: str | None = Field(None, alias="price")
    bid: str | None = Field(None, alias="bid")
    ask: str | None = Field(None, alias="ask")
    volume: str | None = Field(None, alias="volume")
    time: int | str | float | None = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if type(v) is not str:
            raise ValueError(f"Must be a string (got {type(v).__name__})")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        v.encode("utf-8", "strict")
        return v

    @field_validator("price", "bid", "ask", "volume", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            Decimal(v)
        except (InvalidOperation, TypeError) as err:
            raise ValueError("Must be a string representing a decimal value") from err
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


class BackpackRawOpenInterest(BaseModel):
    """
    Raw open interest data from `/api/v1/openInterest`.
    Mirrors the schema in the Backpack OpenAPI spec.

    Reference: https://docs.backpack.exchange/ (see OpenAPI spec for /openInterest)
    Fields:
        symbol: Trading symbol (str)
        open_interest: Open interest (as string)
    """

    symbol: str = Field(..., alias="symbol")
    open_interest: str = Field(..., alias="openInterest")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("symbol", "open_interest", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: str | None) -> str | None:
        if v is None:
            raise ValueError("Must be a non-empty string (got None)")
        if type(v) is not str:
            raise ValueError(f"Must be a string (got {type(v).__name__})")
        if not v.strip():
            raise ValueError("Must be a non-empty string")
        return v

    @field_validator("open_interest", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            Decimal(v)
        except (InvalidOperation, TypeError) as err:
            raise ValueError("Must be a string representing a decimal value") from err
        return v
