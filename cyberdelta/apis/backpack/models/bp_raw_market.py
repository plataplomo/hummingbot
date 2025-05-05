"""
Backpack API Market, Ticker, and Open Interest Models
----------------------------------------------------

This module defines strict Pydantic models for validating market metadata, ticker, and open
interest responses from the Backpack Exchange API. These models are used for boundary validation
and transformation, not for internal business logic.

Models:
    - BackpackRawMarket: Validates market metadata (symbol, base/quote asset).
    - BackpackRawTicker: Validates ticker data (symbol, price, bid, ask, volume, time).
    - BackpackRawOpenInterest: Validates open interest data (symbol, open interest).
    - BackpackRawBookLevel: Validates a single price level in the raw order book response.
    - BackpackRawOrderBook: Validates the raw order book depth object.

Validation Pattern:
    - All string fields are strictly validated for type, non-emptiness, max length, and valid UTF-8.
    - Decimal fields are validated for parseability and finiteness.
    - Timestamps accept int, float, or ISO8601-like strings.
    - All extra fields are forbidden.

These models act as a strict shield between external API data and internal business logic, ensuring
robustness and security at the data ingestion boundary.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


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
        field_name = getattr(info, "field_name", None) or "field"
        return validate_str_field(v, field_name=field_name, max_length=64)


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
    def validate_symbol(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="symbol", max_length=64)

    @field_validator("price", "bid", "ask", "volume", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str | None:
        field_name = getattr(info, "field_name", None) or "field"
        if v is None:
            return v
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=True, field_name=field_name)
        if d is not None and not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s

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
        field_name = getattr(info, "field_name", None) or "field"
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("open_interest", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = getattr(info, "field_name", None) or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s


class BackpackRawOrderBook(BaseModel):
    """
    Pydantic model for the raw order book depth object from `/api/v1/depth` (Backpack REST API).

    This model mirrors the Backpack OpenAPI Depth schema exactly, enforcing strict field validation.
    Use this model to validate and parse depth payloads received from the exchange.

    Attributes:
        asks (List[Tuple[str, str]]): List of asks [price, quantity].
        bids (List[Tuple[str, str]]): List of bids [price, quantity].
        last_update_id (str): ID of the last update that changed the book.
        timestamp (int): Matching engine timestamp in microseconds.
    """

    asks: list[tuple[str, str]] = Field(..., alias="asks")
    bids: list[tuple[str, str]] = Field(..., alias="bids")
    last_update_id: str = Field(..., alias="lastUpdateId")
    timestamp: int = Field(..., alias="timestamp")
    model_config = ConfigDict(
        populate_by_name=True, extra="forbid", validate_by_name=True, frozen=True
    )

    @field_validator("asks", "bids", mode="before")
    @classmethod
    def validate_levels(cls, v: object, info: ValidationInfo) -> list[tuple[str, str]]:
        """Validates bids/asks are lists of [price_str, quantity_str] pairs.

        Ensures:
            - Input `v` is a list.
            - Each item in `v` is a list/tuple of exactly 2 elements.
            - Both elements are non-empty strings (max_length=64).
            - Both elements parse to finite Decimals.
            - Quantity string parses to a non-negative Decimal.

        Returns:
            List[Tuple[str, str]]: The validated list of string pairs.

        Raises:
            ValueError: If validation fails at any step.
        """
        field_name = info.field_name
        if not isinstance(v, list):
            raise ValueError(f"{field_name}: Must be a list of [price, quantity] pairs")

        # Removed cast - perform runtime checks inside loop
        validated_levels: list[tuple[str, str]] = []
        for i, level_raw in enumerate(v):
            # Runtime check for structure
            if not isinstance(level_raw, list | tuple) or len(level_raw) != 2:
                raise ValueError(
                    f"{field_name}[{i}]: Each level must be a list/tuple of [price, quantity]"
                )

            price_raw: Any = level_raw[0]
            quantity_raw: Any = level_raw[1]

            try:
                # Validate price string and its content
                price_str = validate_str_field(price_raw, f"{field_name}[{i}].price", max_length=64)
                price_dec = parse_decimal_value(price_str, allow_none=False)
                if price_dec is None or not price_dec.is_finite():
                    raise ValueError("Price must be a finite decimal string")

                # Validate quantity string and its content (non-negative)
                quantity_str = validate_str_field(
                    quantity_raw, f"{field_name}[{i}].quantity", max_length=64
                )
                quantity_dec = parse_decimal_value(quantity_str, allow_none=False)
                if quantity_dec is None or not quantity_dec.is_finite() or quantity_dec < 0:
                    raise ValueError("Quantity must be a non-negative finite decimal string")

                validated_levels.append((price_str, quantity_str))
            except (ValueError, TypeError) as e:
                # Catch errors from helpers or checks above
                raise ValueError(
                    f"{field_name}[{i}]: Invalid level format [{level_raw}]: {e}"
                ) from e
        return validated_levels

    @field_validator("last_update_id", mode="before")
    @classmethod
    def validate_last_update_id(cls, v: object, info: ValidationInfo) -> str:
        return validate_str_field(v, field_name="last_update_id", max_length=64)

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: object, info: ValidationInfo) -> int:
        """Validate timestamp is strictly an integer."""
        if not isinstance(v, int):
            raise ValueError("timestamp: Must be an integer (microseconds)")
        return v
