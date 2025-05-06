"""
CyberDeltaEngine: Hyperliquid API Raw Models (Order Action Payloads)
-------------------------------------------------------------------

This module defines Pydantic models for constructing parts of the raw
Hyperliquid Exchange API request when placing orders, specifically the
`trigger` object and the `orderType` object within an order action.
"""

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawLimitOrderTypeDetails(BaseModel):
    """
    Details for a limit order type.
    """

    tif: Literal["Gtc", "Ioc", "Alo"]  # Add other TIFs if HL supports more

    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawMarketOrderTypeDetails(BaseModel):
    """
    Details for a market order type (currently empty as per Hyperliquid spec).
    """

    # Hyperliquid market order type is just an empty object: {"market": {}}
    pass

    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawOrderTypeUnion(BaseModel):
    """
    Represents the 'orderType' field which can be a limit or market type.
    Uses a dictionary structure as per Hyperliquid's format, e.g., {"limit": {...}} or {"market": {}}.
    """

    limit: HyperliquidRawLimitOrderTypeDetails | None = None
    market: HyperliquidRawMarketOrderTypeDetails | None = None

    # Validate that exactly one of limit or market is set.
    # This would typically be done with a model_validator, but for constructing the payload,
    # we ensure this in the calling code.
    model_config = ConfigDict(extra="forbid", frozen=True)


# Placeholder for the full HyperliquidRawOrderAction if needed for other contexts,
# for now, place_order will construct the dict directly using these components.


class HyperliquidRawQueryOrderHistoryRequestPayload(BaseModel):
    """
    Request payload for the 'queryOrderHistory' info type.
    Timestamps are in milliseconds.
    """

    type: Literal["queryOrderHistory"] = Field("queryOrderHistory")
    start_time: int = Field(..., alias="startTime", ge=0)
    end_time: int = Field(..., alias="endTime", ge=0)

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type_literal(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "type"
        s = validate_str_field(v, field_name=field_name, max_length=32)
        if s != "queryOrderHistory":
            raise ValueError(f"{field_name} must be 'queryOrderHistory', got '{s}'")
        return s

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_timestamp(cls, value: int) -> int:
        if value < 0:
            raise ValueError("Timestamp must be non-negative.")
        return value

    @model_validator(mode="after")
    def check_start_end_time(self) -> Self:
        if self.end_time < self.start_time:
            raise ValueError(
                f"endTime ({self.end_time}) cannot be before startTime ({self.start_time})."
            )
        return self
