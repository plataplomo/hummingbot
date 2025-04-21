"""
Backpack API Market, Ticker, and Open Interest Models
----------------------------------------------------

This module defines strict Pydantic models for validating market metadata, ticker, and open interest
responses from the Backpack Exchange API. These models are used for boundary validation and transformation,
not for internal business logic.

Models:
    - BackpackRawMarket: Validates market metadata (symbol, base/quote asset).
    - BackpackRawTicker: Validates ticker data (symbol, price, bid, ask, volume, time).
    - BackpackRawOpenInterest: Validates open interest data (symbol, open interest).

Validation Pattern:
    - All string fields are strictly validated for type, non-emptiness, max length, and valid UTF-8.
    - Decimal fields are validated for parseability and finiteness.
    - Timestamps accept int, float, or ISO8601-like strings.
    - All extra fields are forbidden.

These models act as a strict shield between external API data and internal business logic, ensuring
robustness and security at the data ingestion boundary.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


class BackpackRawMarket(BaseModel):
    """
    Pydantic model for a raw market metadata object from `/api/v1/markets` (Backpack REST API).

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse market metadata payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        base_asset (str): Base asset symbol.
        quote_asset (str): Quote asset symbol.
    """

    symbol: str = Field(..., alias="symbol", max_length=64)
    base_asset: str = Field(..., alias="baseAsset", max_length=64)
    quote_asset: str = Field(..., alias="quoteAsset", max_length=64)
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("symbol", "base_asset", "quote_asset", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty UTF-8 string of max 64 chars.
        Raises ValueError if not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
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
    Pydantic model for a raw ticker object from `/api/v1/ticker` (Backpack REST API).

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse ticker payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        price (str | None): Last traded price (as string, optional).
        bid (str | None): Best bid price (as string, optional).
        ask (str | None): Best ask price (as string, optional).
        volume (str | None): 24h trading volume (as string, optional).
        time (int | str | float | None): Ticker timestamp.
    """

    symbol: str = Field(..., alias="symbol")
    price: str | None = Field(None, alias="price")
    bid: str | None = Field(None, alias="bid")
    ask: str | None = Field(None, alias="ask")
    volume: str | None = Field(None, alias="volume")
    time: int | str | float | None = Field(..., alias="time")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the symbol is a non-empty UTF-8 string.
        Raises ValueError if not a string, is empty, or is not valid UTF-8.
        """
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
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str | None:
        """
        Validates that the value is a non-empty string representing a finite decimal.
        Raises ValueError if not a string, not parseable as decimal, or not finite.
        """
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
    def validate_timestamp(cls, v: object) -> int | float | str:
        """
        Validates that the value is a valid timestamp (int, float, or ISO8601-like string).
        Raises ValueError if not a valid type, not parseable, or not valid UTF-8.
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
            except Exception as err:
                # If not parseable as a number, treat as ISO8601 or raise
                try:
                    v.encode("utf-8", "strict")
                except UnicodeEncodeError as err2:
                    raise ValueError(f"String must be valid UTF-8: {err2}") from err2
                # Accept as string if it looks like ISO8601 (basic check)
                if ("T" in v or "-" in v or ":" in v) and any(c.isdigit() for c in v):
                    return v
                raise ValueError("Invalid timestamp format") from err
        raise ValueError("Invalid timestamp format")


class BackpackRawOpenInterest(BaseModel):
    """
    Pydantic model for a raw open interest object from `/api/v1/openInterest` (Backpack REST API).

    This model mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse open interest payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        open_interest (str): Open interest (as string).
    """

    symbol: str = Field(..., alias="symbol")
    open_interest: str = Field(..., alias="openInterest")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    @field_validator("symbol", "open_interest", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the value is a non-empty UTF-8 string of max 64 chars.
        Raises ValueError if not a string, is empty, exceeds max length, or is not valid UTF-8.
        """
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
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str | None:
        """
        Validates that the value is a non-empty string representing a finite decimal (max 64 chars).
        Raises ValueError if not a string, not parseable as decimal, not finite, or exceeds max length.
        """
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
