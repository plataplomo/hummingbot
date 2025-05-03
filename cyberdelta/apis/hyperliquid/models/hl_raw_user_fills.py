"""
CyberDeltaEngine: Hyperliquid API Raw Models (User Fills Group)
--------------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of all major Hyperliquid Exchange API (REST and WebSocket) responses related to
user fills. It is a core part of CyberDeltaEngine's boundary validation layer for user
trade execution and fill data.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data
  structures returned by Hyperliquid's user fills endpoints, including individual fills,
  batch fill responses, and fill request payloads.
- All models enforce strict schema validation (`extra="forbid"`), strict type checking,
  and robust format validation (e.g., max length, finite decimals, valid UTF-8).
- Any unexpected, malformed, or ambiguous fields in upstream data are immediately
  rejected. This is critical for robust, secure, and predictable operation in a
  financial system.
- These models are the *first step* in the "validate first, then transform" pattern:
  validate external data at the boundary, then map to internal business models with
  type conversions and business logic.
- **Never use these models for internal business logic.**

**References:**
- Official Hyperliquid API documentation: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api
- Reverse-engineered OpenAPI spec: see openapi_hl.json
- Official SDK: https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**Usage Example:**
    raw = HyperliquidRawUserFill.model_validate(api_response_dict)
    # ...then transform to internal fill model

**Note:**
Do not use these models for internal business logic—use your core models for that. These are for
boundary validation only.
"""

from pydantic import BaseModel, ConfigDict, Field, RootModel, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_enum_field, validate_str_field


# --- Core User Fill Model ---
class HyperliquidRawUserFill(BaseModel):
    """
    Strict boundary model for a user fill/trade object as returned in user fills endpoints.

    This model validates the structure and content of individual user fill entries, enforcing strict
    type and format constraints for all fields. Never use for internal business logic.

    Fields:
        tid (int): Trade ID.
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        px (str): Price at which the fill occurred as a decimal string.
        sz (str): Size of the fill as a decimal string.
        time (int): Timestamp of the fill event (epoch ms).
        side (str): Side of the trade ('B' for buy, 'A' for ask/sell).
        oid (int): Order ID associated with the fill.
        start_position (str): Start position before the fill.
        dir (str): Direction of the fill.
        hash (str): Unique trade hash.
        fee (str): Fee paid for the fill as a decimal string.
        is_maker (bool): True if the user was the maker in this trade.
        liquidation_mark_px (Optional[str]): Liquidation mark price as a decimal string, if present.
        cloid (Optional[str]): Client order ID, if present.
    """

    tid: int = Field(..., alias="tid")
    coin: str = Field(..., alias="coin")
    px: str = Field(..., alias="px")
    sz: str = Field(..., alias="sz")
    time: int = Field(..., alias="time")
    side: str = Field(..., alias="side")
    oid: int = Field(..., alias="oid")
    start_position: str = Field(..., alias="startPosition")
    dir: str = Field(..., alias="dir")
    hash: str = Field(..., alias="hash")
    fee: str = Field(..., alias="fee")
    is_maker: bool = Field(..., alias="isMaker")
    liquidation_mark_px: str | None = Field(None, alias="liquidationMarkPx")
    cloid: str | None = Field(None, alias="cloid")
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

    @field_validator("px", "sz", "fee", mode="before")
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

    @field_validator("start_position", "dir", "hash", mode="before")
    @classmethod
    def validate_non_empty_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates that the field is a non-empty string of max length 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated string.
        Raises:
            ValueError: If the input is not a valid string.
        """
        field_name = info.field_name or "field"
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("liquidation_mark_px", "cloid", mode="before")
    @classmethod
    def validate_optional_str(cls, v: object, info: ValidationInfo) -> str | None:
        """
        Validates that the field is either None or a string of max length 64.

        Args:
            v (object): The value to validate (should be a string or None).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            Optional[str]: The validated string or None.
        Raises:
            ValueError: If the input is not a valid string or None.
        """
        if v is None:
            return v
        field_name = info.field_name or "field"
        return validate_str_field(v, field_name=field_name, max_length=64)

    @field_validator("is_maker", mode="before")
    @classmethod
    def validate_is_maker_bool(cls, v: object, info: ValidationInfo) -> bool:
        """
        Strictly enforce that is_maker is a bool (no coercion). This is required by the raw
        model policy.

        Args:
            v (object): The value to validate (should be a bool).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            bool: The validated boolean value.
        Raises:
            ValueError: If the input is not a bool.
        """
        if not isinstance(v, bool):
            raise ValueError(f"is_maker: Expected bool, got {type(v).__name__}")
        return v

    @field_validator("cloid", "hash", mode="before")
    @classmethod
    def validate_no_null_bytes(cls, v: object, info: ValidationInfo) -> str | None:
        """
        Reject null bytes (\x00) in cloid and hash fields for safety and robustness.

        Args:
            v (object): The value to validate (should be a string or None).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            Optional[str]: The validated string or None.
        Raises:
            ValueError: If the input contains null bytes or is not a valid string or None.
        """
        if v is None:
            return v
        s = validate_str_field(v, field_name=info.field_name or "field", max_length=64)
        if "\x00" in s:
            raise ValueError(f"{info.field_name}: Null byte (\\x00) not allowed in string")
        return s

    @field_validator("liquidation_mark_px", mode="before")
    @classmethod
    def validate_optional_decimal_str(cls, v: object, info: ValidationInfo) -> str | None:
        """
        Ensure optional decimal string is valid if present (finite decimal, not NaN/inf).

        Args:
            v (object): The value to validate (should be a string or None).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            Optional[str]: The validated decimal string or None.
        Raises:
            ValueError: If the input is not a valid decimal string or None.
        """
        if v is None:
            return v
        s = validate_str_field(v, field_name=info.field_name or "field", max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=info.field_name or "field")
        if d is None or not d.is_finite():
            raise ValueError(f"{info.field_name}: Value must be a finite decimal (not NaN or inf)")
        return s


# --- Batch/Array Response ---
class HyperliquidRawUserFillsResponse(RootModel[list[HyperliquidRawUserFill]]):
    """
    Strict boundary model for an array of user fills as returned in the 'userFills'
    endpoint response.

    This model validates the structure and content of the batch response, enforcing strict
    type and format constraints for all fields. Never use for internal business logic.

    Fields:
        root (List[HyperliquidRawUserFill]): List of user fill objects.
    """

    pass


# --- Request Payload ---
class HyperliquidRawUserFillsRequestPayload(BaseModel):
    """
    Strict boundary model for the request payload for the 'userFills' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting user fills for a specific wallet address. Enforces strict type and format
    constraints for all fields. Never use for internal business logic.

    Fields:
        type (str): Must be 'userFills'.
        user (str): Wallet address of the user.
    """

    type: str = Field("userFills", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("user", mode="before")
    @classmethod
    def validate_user_eth_address(cls, v: object, info: ValidationInfo) -> str:
        """
        Enforce Ethereum address pattern ^0x[0-9a-fA-F]{40}$ for user field.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated Ethereum address string.
        Raises:
            ValueError: If the input is not a valid Ethereum address string.
        """
        s = validate_str_field(v, field_name="user", max_length=64)
        import re

        if not re.fullmatch(r"^0x[0-9a-fA-F]{40}$", s):
            raise ValueError("user: Must be a valid Ethereum address (0x + 40 hex chars)")
        return s
