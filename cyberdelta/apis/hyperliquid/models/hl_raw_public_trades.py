"""
CyberDeltaEngine: Hyperliquid API Raw Models (Public Trades Group)
-----------------------------------------------------------------

This module provides strict Pydantic models for validating the *raw* structure of all major
Hyperliquid Exchange API (REST and WebSocket) responses related to public trades. It is a core part of CyberDeltaEngine's boundary validation layer for real-time and historical trade data.

**Scope & Rationale:**
- Models in this file are used to validate and parse the *external* data structures returned by Hyperliquid's public trade endpoints, including individual trades, batch trade responses, and trade request payloads.
- All models enforce strict schema validation (`extra="forbid"`), ensuring that any unexpected or malformed fields in upstream data are immediately rejected. This is critical for robust, secure, and predictable operation in a financial system.
- These models are the *first step* in the "validate first, then transform" pattern: validate external data at the boundary, then map to internal business models with type conversions and business logic.

**References:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawPublicTrade.model_validate(api_response_dict)
    # ...then transform to internal trade model

**Note:**
Do not use these models for internal business logic—use your core models for that. These are for boundary validation only.
"""

from pydantic import BaseModel, ConfigDict, Field, RootModel, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_enum_field, validate_str_field


# --- Core Public Trade Model ---
class HyperliquidRawPublicTrade(BaseModel):
    """
    Strict boundary model for a public trade object as returned in recent trades endpoints.

    This model validates the structure and content of individual public trade entries, enforcing strict
    type and format constraints for all fields. Never use for internal business logic.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        side (str): Side of the trade ('B' for buy, 'A' for ask/sell).
        px (str): Price at which the trade occurred as a decimal string.
        sz (str): Size of the trade as a decimal string.
        time (int): Timestamp of the trade event (epoch ms).
        hash (str): Unique trade hash.
    """

    coin: str = Field(..., alias="coin")
    side: str = Field(..., alias="side")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    time: int = Field(..., alias="time")
    hash: str = Field(..., alias="hash")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("coin", mode="before")
    @classmethod
    def validate_coin(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'coin' field to ensure it is a string of max length 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated asset symbol string.
        Raises:
            ValueError: If the input is not a valid string.
        """
        return validate_str_field(v, field_name="coin", max_length=64)

    @field_validator("side", mode="before")
    @classmethod
    def validate_side(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'side' field to ensure it is either 'B' (buy) or 'A' (ask/sell).

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated side string.
        Raises:
            ValueError: If the input is not a valid side value.
        """
        return validate_enum_field(v, allowed={"B", "A"}, field_name="side")

    @field_validator("px", "sz", mode="before")
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the field is a string representing a finite decimal (not NaN/inf),
        with a maximum length of 64. This is critical for financial data integrity.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated decimal string.
        Raises:
            ValueError: If the input is not a valid decimal string.
        """
        field_name = info.field_name or "field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
        return s


# --- Batch/Array Response ---
class HyperliquidRawRecentTradesResponse(RootModel[list[HyperliquidRawPublicTrade]]):
    """
    Strict boundary model for an array of public trades as returned in the 'recentTrades' endpoint response.

    This model validates the structure and content of the batch response, enforcing strict type and
    format constraints for all fields. Never use for internal business logic.

    Fields:
        root (List[HyperliquidRawPublicTrade]): List of public trade objects.
    """

    @property
    def items(self) -> list[HyperliquidRawPublicTrade]:
        """
        Returns the validated list of public trades with full type safety.
        This is the preferred way to access the root data in Pydantic v2.

        Returns:
            list[HyperliquidRawPublicTrade]: The validated list of public trade objects.
        """
        return self.root


# --- Request Payload ---
class HyperliquidRawRecentTradesRequestPayload(BaseModel):
    """
    Strict boundary model for the request payload for the 'recentTrades' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when requesting
    recent public trades for a specific asset. Enforces strict type and format constraints for all fields.
    Never use for internal business logic.

    Fields:
        type (str): Must be 'recentTrades'.
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
    """

    type: str = Field("recentTrades", alias="type")
    coin: str = Field(..., alias="coin")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("coin", mode="before")
    @classmethod
    def validate_coin(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'coin' field to ensure it is a string of max length 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated asset symbol string.
        Raises:
            ValueError: If the input is not a valid string.
        """
        return validate_str_field(v, field_name="coin", max_length=64)
