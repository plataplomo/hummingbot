"""
Backpack API Market, Ticker, and Open Interest Models
"""

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


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
    def validate_non_empty_str(cls, v: Any, info: ValidationInfo) -> str:  # noqa: ANN401
        field_name = getattr(info, "field_name", None) or "field"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(f"{field_name}: String must be non-empty and not just whitespace")
        if len(v) > 64:
            raise ValueError(f"{field_name}: String exceeds max length of 64")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"{field_name}: String must be valid UTF-8: {err}") from err
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
    def validate_non_empty_str(cls, v: Any, info: ValidationInfo) -> str:  # noqa: ANN401
        field_name = getattr(info, "field_name", None) or "field"
        if not isinstance(v, str):
            field_info = getattr(cls, "model_fields", {}).get(field_name) if field_name else None
            allow_none = field_info and (str(field_info.annotation).find("| None") != -1)
            if v is None and allow_none:
                raise ValueError(f"{field_name}: Field is required, cannot be None.")
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v:
            raise ValueError("String must be non-empty")
        try:
            v.encode("utf-8")
        except UnicodeError as err:
            raise ValueError("String must be valid UTF-8") from err
        return v

    @field_validator("price", "bid", "ask", "volume", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: Any, info: ValidationInfo) -> str | None:  # noqa: ANN401
        field_name = getattr(info, "field_name", None) or "field"
        if v is None:
            return v
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(
                f"{field_name}: Input decimal string cannot be empty or just whitespace."
            )
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"{field_name}: String must be valid UTF-8: {err}") from err
        try:
            dec_val = Decimal(v)
        except Exception as err:
            raise ValueError(f"{field_name}: Must be a valid decimal string (got {v!r})") from err
        if not dec_val.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return v

    @field_validator("time", mode="before")
    @classmethod
    def validate_timestamp(cls, v: Any) -> int | float | str:  # noqa: ANN401
        """
        Accepts int, float, or digit-only string. Returns as int/float if possible, else string.
        Accepts any type due to Pydantic's mode="before"; type is checked at runtime.
        """
        if isinstance(v, int | float):
            return v
        if isinstance(v, str):
            if not v.strip():
                raise ValueError("String must be non-empty")
            try:
                # Try parsing as int or float
                if v.isdigit():
                    return int(v)
                return float(v)
            except Exception:
                # If not parseable as a number, treat as ISO8601 or raise
                try:
                    v.encode("utf-8", "strict")
                except UnicodeEncodeError as err:
                    raise ValueError(f"String must be valid UTF-8: {err}") from err
                # Accept as string if it looks like ISO8601 (basic check)
                if ("T" in v or "-" in v or ":" in v) and any(c.isdigit() for c in v):
                    return v
                raise ValueError("Invalid timestamp format")
        raise ValueError("Invalid timestamp format")


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
    def validate_non_empty_str(cls, v: Any, info: ValidationInfo) -> str:  # noqa: ANN401
        field_name = getattr(info, "field_name", None) or "field"
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(f"{field_name}: String must be non-empty and not just whitespace")
        if len(v) > 64:
            raise ValueError(f"{field_name}: String exceeds max length of 64")
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"{field_name}: String must be valid UTF-8: {err}") from err
        return v

    @field_validator("open_interest", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: Any, info: ValidationInfo) -> str | None:  # noqa: ANN401
        field_name = getattr(info, "field_name", None) or "field"
        if v is None:
            return v
        if not isinstance(v, str):
            raise ValueError(f"{field_name}: Input must be a string, got {type(v).__name__}")
        if not v.strip():
            raise ValueError(
                f"{field_name}: Input decimal string cannot be empty or just whitespace."
            )
        try:
            v.encode("utf-8", "strict")
        except UnicodeEncodeError as err:
            raise ValueError(f"{field_name}: String must be valid UTF-8: {err}") from err
        try:
            dec_val = Decimal(v)
        except Exception as err:
            raise ValueError(f"{field_name}: Must be a valid decimal string (got {v!r})") from err
        if not dec_val.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return v
