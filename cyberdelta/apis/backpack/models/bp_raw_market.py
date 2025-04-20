"""
Backpack API Market, Ticker, and Open Interest Models
"""

from pydantic import BaseModel, ConfigDict, Field


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

    symbol: str = Field(..., alias="symbol")
    base_asset: str = Field(..., alias="baseAsset")
    quote_asset: str = Field(..., alias="quoteAsset")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


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
