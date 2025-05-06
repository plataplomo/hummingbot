"""
CyberDeltaEngine: Hyperliquid API Raw Models (User State Group)
--------------------------------------------------------------

This module provides strict, security-focused Pydantic models for validating the *raw*
structure of all major Hyperliquid Exchange API (REST and WebSocket) responses related to
user state. It is a core part of CyberDeltaEngine's boundary validation layer for user
account, margin, and position data.

**Boundary Validation Policy:**
- Models in this file are used exclusively to validate and parse the *external* data
  structures returned by Hyperliquid's user state endpoints, including leverage,
  position info, margin summary, and clearinghouse state.
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
    raw = HyperliquidRawClearinghouseState.model_validate(api_response_dict)
    # ...then transform to internal user state model
"""

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_enum_field, validate_str_field


# --- Leverage Submodel ---
class HyperliquidRawLeverage(BaseModel):
    """
    Strict boundary model for leverage settings as returned in user state endpoints.

    This model validates the leverage type and value for a position, enforcing strict type and
    format constraints. Never use for internal business logic.

    Fields:
        type (str): Leverage type ('cross' or 'isolated').
        value (int): Leverage value (must be non-negative integer).
    """

    type: str = Field(..., alias="type")
    value: int = Field(..., alias="value")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("type", mode="before")
    @classmethod
    def validate_type(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'type' field to ensure it is either 'cross' or 'isolated' and a string
        of max length 16.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated leverage type string.
        Raises:
            ValueError: If the input is not a valid leverage type string.
        """
        field_name = info.field_name or "type"
        try:
            s = validate_str_field(v, field_name=field_name, max_length=16)
            return validate_enum_field(s, allowed={"cross", "isolated"}, field_name=field_name)
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e

    @field_validator("value", mode="before")
    @classmethod
    def validate_value(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates the 'value' field to ensure it is a non-negative integer.

        Args:
            v (object): The value to validate (should be an integer).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            int: The validated leverage value.
        Raises:
            ValueError: If the input is not a non-negative integer.
        """
        field_name = info.field_name or "value"
        if not isinstance(v, int):
            raise ValueError(f"{field_name}: Expected int, got {type(v).__name__}")
        if v < 0:
            raise ValueError(f"{field_name}: Leverage value must be non-negative")
        return v


# --- Position Info Submodel ---
class HyperliquidRawPositionInfo(BaseModel):
    """
    Strict boundary model for detailed user position information as returned
    in user state endpoints.

    This model validates the structure and content of a user's position for a given asset,
    enforcing strict type and format constraints. Never use for internal business logic.

    Fields:
        coin (str): Asset symbol (e.g., 'ETH', 'BTC').
        entry_px (Optional[str]): Entry price as a decimal string, if present.
        leverage (HyperliquidRawLeverage): Leverage settings for this position.
        liquidation_px (Optional[str]): Liquidation price as a decimal string, if present.
        margin_used (str): Margin used for this position as a decimal string.
        max_leverage (int): Maximum leverage allowed for this asset.
        position_value (str): Value of the position as a decimal string.
        return_on_equity (str): Return on equity (ROE) as a decimal string.
        szi (str): Size of the position as a decimal string.
        unrealized_pnl (str): Unrealized profit and loss as a decimal string.
    """

    coin: str = Field(..., alias="coin")
    entry_px: str | None = Field(None, alias="entryPx")
    leverage: HyperliquidRawLeverage = Field(..., alias="leverage")
    liquidation_px: str | None = Field(None, alias="liquidationPx")
    margin_used: str = Field(..., alias="marginUsed")
    max_leverage: int = Field(..., alias="maxLeverage")
    position_value: str = Field(..., alias="positionValue")
    return_on_equity: str = Field(..., alias="returnOnEquity")
    szi: str = Field(..., alias="szi")
    unrealized_pnl: str = Field(..., alias="unrealizedPnl")
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
        field_name = info.field_name or "coin"
        try:
            return validate_str_field(v, field_name=field_name, max_length=64)
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e

    @field_validator("entry_px", "liquidation_px", mode="before")
    @classmethod
    def validate_optional_decimal_str(cls, v: object, info: ValidationInfo) -> str | None:
        """
        Validates an optional decimal string field (entry_px or liquidation_px).
        Ensures the value is either None or a valid decimal string of max length 64.

        Args:
            v (object): The value to validate (should be a string or None).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            Optional[str]: The validated decimal string or None.
        Raises:
            ValueError: If the input is not a valid decimal string or None.
        """
        field_name = info.field_name or "field"
        if v is None:
            return v
        try:
            s = validate_str_field(v, field_name=field_name, max_length=64)
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            if d is None or not d.is_finite():
                raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
            return s
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e

    @field_validator(
        "margin_used", "position_value", "return_on_equity", "szi", "unrealized_pnl", mode="before"
    )
    @classmethod
    def validate_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates a required decimal string field, ensuring it is a valid
        decimal string of max length 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated decimal string.
        Raises:
            ValueError: If the input is not a valid decimal string.
        """
        field_name = info.field_name or "field"
        try:
            s = validate_str_field(v, field_name=field_name, max_length=64)
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            if d is None or not d.is_finite():
                raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
            return s
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e

    @field_validator("max_leverage", mode="before")
    @classmethod
    def validate_max_leverage(cls, v: object, info: ValidationInfo) -> int:
        """
        Validates the 'max_leverage' field to ensure it is a non-negative integer.

        Args:
            v (object): The value to validate (should be an integer).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            int: The validated max leverage value.
        Raises:
            ValueError: If the input is not a non-negative integer.
        """
        field_name = info.field_name or "max_leverage"
        if not isinstance(v, int):
            raise ValueError(f"{field_name}: Expected int, got {type(v).__name__}")
        if v < 0:
            raise ValueError(f"{field_name}: max_leverage must be non-negative")
        return v


# --- Asset Position Submodel ---
class HyperliquidRawAssetPosition(BaseModel):
    """
    Strict boundary model for a user's position details for a specific asset
    as returned in user state endpoints.

    This model validates the structure and content of a user's asset position,
    enforcing strict type and format constraints.
    Never use for internal business logic.

    Fields:
        asset (str): Asset symbol (e.g., 'ETH', 'BTC').
        position (HyperliquidRawPositionInfo): Detailed position information for this asset.
    """

    asset: str = Field(..., alias="asset")
    position: HyperliquidRawPositionInfo = Field(..., alias="position")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("asset", mode="before")
    @classmethod
    def validate_asset(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'asset' field to ensure it is a string of max length 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated asset symbol string.
        Raises:
            ValueError: If the input is not a valid string.
        """
        field_name = info.field_name or "asset"
        try:
            return validate_str_field(v, field_name=field_name, max_length=64)
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e


# --- Margin Summary Submodel ---
class HyperliquidRawMarginSummary(BaseModel):
    """
    Strict boundary model for a margin summary as returned in user state endpoints.

    This model validates the structure and content of a user's margin summary,
    enforcing strict type and format constraints for all fields.
    Never use for internal business logic.

    Fields:
        account_value (str): Account value for the user as a decimal string.
        total_margin_used (str): Total margin used across all positions as a decimal string.
        total_ntl_pos (str): Total notional position size as a decimal string.
        total_raw_usd (str): Total raw USD value as a decimal string.
    """

    account_value: str = Field(..., alias="accountValue")
    total_margin_used: str = Field(..., alias="totalMarginUsed")
    total_ntl_pos: str = Field(..., alias="totalNtlPos")
    total_raw_usd: str = Field(..., alias="totalRawUsd")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("account_value", mode="before")
    @classmethod
    def validate_account_value(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'account_value' field to ensure it is a valid decimal string of max length 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated account value string.
        Raises:
            ValueError: If the input is not a valid decimal string.
        """
        field_name = info.field_name or "account_value"
        try:
            s = validate_str_field(v, field_name=field_name, max_length=64)
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            if d is None or not d.is_finite():
                raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
            return s
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e

    @field_validator("total_margin_used", mode="before")
    @classmethod
    def validate_total_margin_used(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'total_margin_used' field to ensure it is a valid
        decimal string of max length 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated total margin used string.
        Raises:
            ValueError: If the input is not a valid decimal string.
        """
        field_name = info.field_name or "total_margin_used"
        try:
            s = validate_str_field(v, field_name=field_name, max_length=64)
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            if d is None or not d.is_finite():
                raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
            return s
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e

    @field_validator("total_ntl_pos", mode="before")
    @classmethod
    def validate_total_ntl_pos(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'total_ntl_pos' field to ensure it is a valid decimal string of max length 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated total notional position string.
        Raises:
            ValueError: If the input is not a valid decimal string.
        """
        field_name = info.field_name or "total_ntl_pos"
        try:
            s = validate_str_field(v, field_name=field_name, max_length=64)
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            if d is None or not d.is_finite():
                raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
            return s
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e

    @field_validator("total_raw_usd", mode="before")
    @classmethod
    def validate_total_raw_usd(cls, v: object, info: ValidationInfo) -> str:
        """
        Validates the 'total_raw_usd' field to ensure it is a valid decimal string of max length 64.

        Args:
            v (object): The value to validate (should be a string).
            info (ValidationInfo): Pydantic validation context.
        Returns:
            str: The validated total raw USD string.
        Raises:
            ValueError: If the input is not a valid decimal string.
        """
        field_name = info.field_name or "total_raw_usd"
        try:
            s = validate_str_field(v, field_name=field_name, max_length=64)
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            if d is None or not d.is_finite():
                raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
            return s
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e


# --- Clearinghouse State Model ---
class HyperliquidRawClearinghouseState(BaseModel):
    """
    Represents the user's clearinghouse state, including positions and margin, as returned in
    user state endpoints.

    This model is used to validate the structure of the clearinghouse state response, which
    includes asset positions, margin summaries, and withdrawable amounts.

    Fields:
        asset_positions (List[HyperliquidRawAssetPosition]): List of asset positions for the user.
        margin_summary (HyperliquidRawMarginSummary): Margin summary for the user.
        cross_maintenance_margin_used (str): Cross maintenance margin used.
        cross_margin_summary (HyperliquidRawMarginSummary): Cross margin summary for the user.
        isolated_maintenance_margin_used (str): Isolated maintenance margin used.
        isolated_margin_summary (HyperliquidRawMarginSummary): Isolated margin summary for the user.
        withdrawable (str): Amount withdrawable by the user.
    """

    asset_positions: list[HyperliquidRawAssetPosition] = Field(..., alias="assetPositions")
    margin_summary: HyperliquidRawMarginSummary = Field(..., alias="marginSummary")
    cross_maintenance_margin_used: str = Field(..., alias="crossMaintenanceMarginUsed")
    cross_margin_summary: HyperliquidRawMarginSummary = Field(..., alias="crossMarginSummary")
    isolated_maintenance_margin_used: str = Field(..., alias="isolatedMaintenanceMarginUsed")
    isolated_margin_summary: HyperliquidRawMarginSummary = Field(..., alias="isolatedMarginSummary")
    withdrawable: str = Field(..., alias="withdrawable")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("cross_maintenance_margin_used", mode="before")
    @classmethod
    def validate_cross_maintenance_margin_used(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "cross_maintenance_margin_used"
        try:
            s = validate_str_field(v, field_name=field_name, max_length=64)
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            if d is None or not d.is_finite():
                raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
            return s
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e

    @field_validator("isolated_maintenance_margin_used", mode="before")
    @classmethod
    def validate_isolated_maintenance_margin_used(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "isolated_maintenance_margin_used"
        try:
            s = validate_str_field(v, field_name=field_name, max_length=64)
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            if d is None or not d.is_finite():
                raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
            return s
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e

    @field_validator("withdrawable", mode="before")
    @classmethod
    def validate_withdrawable(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "withdrawable"
        try:
            s = validate_str_field(v, field_name=field_name, max_length=64)
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            if d is None or not d.is_finite():
                raise ValueError(f"{field_name}: Value must be a finite decimal (not NaN or inf)")
            return s
        except Exception as e:
            raise ValueError(f"{field_name}: Validation failed - {e}") from e


# --- Request Payload ---
class HyperliquidRawUserStateRequestPayload(BaseModel):
    """
    Represents the request payload for the 'clearinghouseState' info type.

    This model is used to construct and validate the payload sent to the Hyperliquid API when
    requesting the user's clearinghouse state.

    Fields:
        type (str): Must be 'clearinghouseState'.
        user (str): Wallet address of the user.
    """

    type: str = Field("clearinghouseState", alias="type")
    user: str = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    @field_validator("user", mode="before")
    @classmethod
    def validate_user_eth_address(cls, v: object, info: ValidationInfo) -> str:
        """
        Enforce Ethereum address pattern ^0x[0-9a-fA-F]{40}$ for user field.
        """
        s = validate_str_field(v, field_name="user", max_length=64)
        import re

        if not re.fullmatch(r"^0x[0-9a-fA-F]{40}$", s):
            raise ValueError("user: Must be a valid Ethereum address (0x + 40 hex chars)")
        return s
