"""
CyberDeltaEngine: Hyperliquid API Raw Models (Exchange Action Response)
----------------------------------------------------------------------

This module defines Pydantic models for validating the *raw* structure of responses from
the Hyperliquid Exchange API's `/exchange` endpoint, which is used for actions like
placing, modifying, or canceling orders.

Models:
    - HyperliquidRawExchangeStatusResting: Validates a 'resting' order status object.
    - HyperliquidRawExchangeStatusFilled: Validates a 'filled' order status object.
    - HyperliquidRawExchangeStatusObject: Validates a general status object which can be one of
      `resting`, `filled`, or an `error` string. Uses `extra="ignore"` as these are sub-components.
    - HyperliquidRawExchangeResponseData: Validates the `data` field within a successful exchange
      response, expecting a `type` (e.g., "order") and a `statuses` list. The `statuses` list
      can contain simple strings (e.g., "canceled") or `HyperliquidRawExchangeStatusObject`
      instances.     Uses `extra="ignore"`.
    - HyperliquidRawExchangeResponse: Validates the top-level exchange action response, expecting
      a `status` (typically "ok") and an optional `data` field of type
      `HyperliquidRawExchangeResponseData`. Uses `extra="forbid"` for strict top-level validation.

These models adhere to the Raw Model Policy, focusing on validating the external contract,
raw data types, and basic formats without incorporating business logic.
"""

from typing import Literal, cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)

from cyberdelta.utils.parsing import validate_str_field

# ... (Existing models like HyperliquidRawOrder, HyperliquidRawFill remain) ...


# --- Exchange Action Response Models ---


class HyperliquidRawExchangeStatusResting(BaseModel):
    """Raw model for a 'resting' order status within an exchange response."""

    oid: int = Field(..., ge=0)  # Ensure non-negative
    # Add other fields observed in resting order status if needed
    # Example: remainingSz: str | None = None
    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawExchangeStatusFilled(BaseModel):
    """Raw model for a 'filled' order status within an exchange response."""

    oid: int = Field(..., ge=0)  # Ensure non-negative
    total_sz: str = Field(..., alias="totalSz", max_length=64)
    avg_px: str = Field(..., alias="avgPx", max_length=64)
    # Potentially add fills list if present
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawExchangeStatusObject(BaseModel):
    """Raw model for complex status objects in exchange responses."""

    resting: HyperliquidRawExchangeStatusResting | None = Field(None)
    filled: HyperliquidRawExchangeStatusFilled | None = Field(None)
    error: str | None = Field(None, max_length=1024)
    withdrawal_submitted: str | None = Field(
        None, alias="WithdrawalSubmitted", max_length=128
    )  # e.g. 0x... tx hash
    success: str | None = Field(
        None, alias="Success", max_length=1024
    )  # e.g. "L2 USDC Transfer successful."

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawExchangeResponseData(BaseModel):
    """Raw model for the 'data' part of an exchange action response."""

    type: str = Field(..., max_length=32)
    # Corrected return type hint in validator below reflects the actual possible validated types
    statuses: list[str | HyperliquidRawExchangeStatusObject] = Field(...)
    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("statuses", mode="before")
    @classmethod
    def validate_statuses_list(
        cls,
        v: object,
        info: ValidationInfo,
    ) -> list[str | dict[str, object]]:
        """Validate list containing strings or dicts for status objects."""
        field_name = info.field_name or "statuses"
        if not isinstance(v, list):
            raise TypeError(f"{field_name}: Must be a list, got {type(v).__name__}.")

        # #[CAST-REVIEW-REQUIRED] - Casting input list for type checker item inference
        list_of_objects = cast(list[object], v)
        assert isinstance(list_of_objects, list)

        validated_items: list[str | dict[str, object]] = []
        for i, item_obj in enumerate(list_of_objects):
            current_item_desc = f"{field_name}[{i}]"
            if isinstance(item_obj, str):
                # Validate the string status (e.g., "canceled")
                # Using basic string validation logic here, adjust max_length as needed
                validated_str = validate_str_field(
                    item_obj, field_name=current_item_desc, max_length=64, allow_empty=False
                )
                validated_items.append(validated_str)
            elif isinstance(item_obj, dict):
                # Pass dict for Pydantic to validate against HyperliquidRawExchangeStatusObject
                # #[CAST-REVIEW-REQUIRED] - Casting dict item for type checker compatibility
                item_dict = cast(dict[str, object], item_obj)
                assert isinstance(item_dict, dict)
                validated_items.append(item_dict)
            else:
                raise TypeError(
                    f"{current_item_desc}: Item must be a string or a dictionary, "
                    f"got {type(item_obj).__name__}."
                )
        return validated_items


class HyperliquidRawExchangeResponse(BaseModel):
    """Raw model for the top-level response from the /exchange endpoint."""

    status: Literal["ok"] = Field(...)
    data: HyperliquidRawExchangeResponseData | None = Field(None)
    # Sometimes 'data' might be missing or structured differently on error/simple success?
    # Making data optional and handling its absence might be safer.
    model_config = ConfigDict(extra="forbid", frozen=True)  # Set extra='forbid' and frozen=True

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_string(cls, v: object, info: ValidationInfo) -> str:
        """Validates the 'status' field is a valid string."""
        # Literal["ok"] check happens after this.
        return validate_str_field(v, field_name="status", max_length=16, allow_empty=False)
