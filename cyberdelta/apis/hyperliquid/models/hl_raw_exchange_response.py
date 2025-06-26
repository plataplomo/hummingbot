"""CyberDeltaEngine: Hyperliquid API Raw Models (Exchange Action Response).

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

from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
)

# Import specific common types
from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawDefaultString,
    RawFiniteDecimalStr,
    RawNonNegativeFiniteDecimalStr,
    RawNonNegativeInt,
    RawOptionalCloidHL,
    RawOptionalNonEmptyString1024HL,
    RawStatusStringHL,
    RawTxHashStr,
)
from cyberdelta.utils.parsing import validate_str_field


# ... (Existing models like HyperliquidRawOrder, HyperliquidRawFill remain) ...


# --- Exchange Action Response Models ---


class HyperliquidRawExchangeStatusResting(BaseModel):
    """Raw model for a 'resting' order status within an exchange response."""

    oid: RawNonNegativeInt = Field(...)
    cloid: RawOptionalCloidHL = Field(
        default=None,
        description="Client order ID if provided in the original order",
    )
    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawExchangeStatusFilled(BaseModel):
    """Raw model for a 'filled' order status within an exchange response."""

    oid: RawNonNegativeInt = Field(...)
    total_sz: RawNonNegativeFiniteDecimalStr = Field(..., alias="totalSz")
    avg_px: RawFiniteDecimalStr = Field(..., alias="avgPx")
    cloid: RawOptionalCloidHL = Field(
        default=None,
        description="Client order ID if provided in the original order",
    )
    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawExchangeStatusObject(BaseModel):
    """Raw model for complex status objects in exchange responses."""

    resting: HyperliquidRawExchangeStatusResting | None = Field(None)
    filled: HyperliquidRawExchangeStatusFilled | None = Field(None)
    error: RawOptionalNonEmptyString1024HL = Field(
        None,
        alias="error",
        description="Error message if any",
    )
    withdrawal_submitted: RawTxHashStr | None = Field(
        default=None,
        alias="WithdrawalSubmitted",
        description="Withdrawal tx hash if submitted",
    )
    success: RawOptionalNonEmptyString1024HL = Field(
        default=None,
        alias="Success",
        description="Success message if any",
    )

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawExchangeResponseDataInner(BaseModel):
    """Raw model for the inner 'data' object containing statuses."""

    statuses: list[RawStatusStringHL | HyperliquidRawExchangeStatusObject] = Field(...)
    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawExchangeResponseData(BaseModel):
    """Raw model for the 'data' part of an exchange action response."""

    type: RawDefaultString = Field(..., description="Type of response data", max_length=32)
    statuses: list[RawStatusStringHL | HyperliquidRawExchangeStatusObject] = Field(...)
    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawExchangeResponseNested(BaseModel):
    """Raw model for the nested response structure when status is 'ok'."""

    type: RawDefaultString = Field(..., description="Type of response data", max_length=32)
    data: HyperliquidRawExchangeResponseDataInner = Field(...)
    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawExchangeResponse(BaseModel):
    """Raw model for the top-level response from the /exchange endpoint."""

    status: Annotated[
        Literal["ok", "err"],
        BeforeValidator(
            lambda x: validate_str_field(x, field_name="status", max_length=16, allow_empty=False),
        ),
    ] = Field(...)
    data: HyperliquidRawExchangeResponseData | None = Field(None)
    response: (
        RawOptionalNonEmptyString1024HL
        | HyperliquidRawExchangeResponseData
        | HyperliquidRawExchangeResponseNested
        | None
    ) = Field(
        None,
        description="Error message when status is 'err' or response data when status is 'ok'",
    )
    model_config = ConfigDict(extra="forbid", frozen=True)

    @property
    def response_data(self) -> HyperliquidRawExchangeResponseData | None:
        """Get the response data in a normalized format, handling flat and nested structures."""
        if self.status == "ok":
            if isinstance(self.response, HyperliquidRawExchangeResponseData):
                return self.response
            if isinstance(self.response, HyperliquidRawExchangeResponseNested):
                # Return a flattened version
                return HyperliquidRawExchangeResponseData(
                    type=self.response.type,
                    statuses=self.response.data.statuses,
                )
            if self.data:
                return self.data
        return None
