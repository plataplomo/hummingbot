"""CyberDeltaEngine: Hyperliquid API Raw Models (User Fills Group)
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

from typing import Annotated, Literal, cast

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    RootModel,
    ValidationInfo,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawAssetString64HL,
    RawDefaultString,
    RawFiniteDecimalStr,
    RawNonNegativeInt,
    RawOptionalNonEmptyString128HL,
    RawSideStr,
    RawStrictBool,
    RawStrictEthereumAddressStrHL,
    RawTimestampMsInt,
    RawTradeHashStringHL,
)
from cyberdelta.utils.parsing import validate_str_field


# --- Core User Fill Model ---
class HyperliquidRawUserFill(BaseModel):
    """Strict boundary model for a user fill/trade object as returned in user fills endpoints.
    Validation handled by Annotated types from common_raw_types.
    """

    tid: RawNonNegativeInt = Field(..., alias="tid")
    coin: RawAssetString64HL = Field(..., alias="coin")
    px: RawFiniteDecimalStr = Field(..., alias="px")
    sz: RawFiniteDecimalStr = Field(..., alias="sz")
    time: RawTimestampMsInt = Field(..., alias="time")
    side: RawSideStr = Field(..., alias="side")
    oid: RawNonNegativeInt = Field(..., alias="oid")
    start_position: RawFiniteDecimalStr = Field(..., alias="startPosition")
    dir: RawDefaultString = Field(..., alias="dir", max_length=64)
    hash: RawTradeHashStringHL = Field(..., alias="hash")
    fee: RawFiniteDecimalStr = Field(..., alias="fee")
    is_maker: RawStrictBool = Field(..., alias="isMaker")
    liquidation_mark_px: RawFiniteDecimalStr | None = Field(None, alias="liquidationMarkPx")
    cloid: RawOptionalNonEmptyString128HL = Field(None, alias="cloid")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


# --- Batch/Array Response ---
class HyperliquidRawUserFillsResponse(RootModel[list[HyperliquidRawUserFill]]):
    """Raw boundary model for a list of user fills."""

    root: list[HyperliquidRawUserFill]
    model_config = ConfigDict(frozen=True)

    @field_validator("root", mode="before")
    @classmethod
    def validate_user_fills_list(cls, v: object, info: ValidationInfo) -> list[dict[str, object]]:
        """Ensures the root input is a list of dictionaries for user fills."""
        field_name = info.field_name or "user_fills_list"
        if not isinstance(v, list):
            raise ValueError(f"Field '{field_name}': Expected a list, got {type(v).__name__}.")

        # CAST 1: For type checker, v is already confirmed list by runtime check
        list_of_objects = cast(list[object], v)
        assert isinstance(list_of_objects, list)

        validated_items: list[dict[str, object]] = []
        for item_idx, item_obj in enumerate(list_of_objects):
            if not isinstance(item_obj, dict):
                item_type = type(item_obj).__name__
                raise ValueError(
                    f"Field '{field_name}', Item {item_idx}: Expected a dictionary, "
                    f"got {item_type}.",
                )

            # CAST 2: For type checker, item_obj is already confirmed dict by runtime check
            item_dict = cast(dict[str, object], item_obj)
            assert isinstance(item_dict, dict)

            validated_items.append(item_dict)
        return validated_items


# --- Request Payload ---
class HyperliquidRawUserFillsRequestPayload(BaseModel):
    """Strict boundary model for the request payload for the 'userFills' info type.
    """

    type: Annotated[
        Literal["userFills"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32, allow_empty=False)),
    ] = Field("userFills", alias="type")
    user: RawStrictEthereumAddressStrHL = Field(..., alias="user")
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)
