"""
CyberDeltaEngine: Hyperliquid API Raw Models (Order Action Payloads)
-------------------------------------------------------------------

This module defines Pydantic models for constructing parts of the raw
Hyperliquid Exchange API request when placing orders, specifically the
`trigger` object and the `orderType` object within an order action.
"""

from typing import Annotated, Literal, Self, cast

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawTifStr,
    RawTimestampMsInt,
)
from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawLimitOrderTypeDetails(BaseModel):
    """
    Details for a limit order type.
    """

    tif: RawTifStr

    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawMarketOrderTypeDetails(BaseModel):
    """
    Details for a market order type (currently empty as per Hyperliquid spec).
    """

    # Hyperliquid market order type is just an empty object: {"market": {}}
    pass

    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawOrderType(BaseModel):
    """
    Represents the 'orderType' field which can be a limit or market type.
    Uses a dictionary structure as per Hyperliquid's format, e.g., {"limit": {...}}
    or {"market": {}}.
    """

    limit: HyperliquidRawLimitOrderTypeDetails | None = Field(default=None)
    market: HyperliquidRawMarketOrderTypeDetails | None = Field(default=None)

    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="before")
    @classmethod
    def check_exclusive_order_type(cls, data: object) -> dict[str, object]:
        if not isinstance(data, dict):
            raise TypeError("orderType must be a dictionary")

        data_dict = cast(dict[str, object], data)

        has_limit = "limit" in data_dict and data_dict["limit"] is not None
        has_market = "market" in data_dict and data_dict["market"] is not None
        if not (has_limit ^ has_market):
            raise ValueError("Exactly one of 'limit' or 'market' must be provided in orderType")

        return data_dict


# Placeholder for the full HyperliquidRawOrderAction if needed for other contexts,
# for now, place_order will construct the dict directly using these components.


class HyperliquidRawQueryOrderHistoryRequestPayload(BaseModel):
    """
    Request payload for the 'queryOrderHistory' info type.
    Timestamps are in milliseconds.
    """

    type: Annotated[
        Literal["queryOrderHistory"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32, allow_empty=False)),
    ] = Field("queryOrderHistory")
    start_time: RawTimestampMsInt = Field(..., alias="startTime")
    end_time: RawTimestampMsInt = Field(..., alias="endTime")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @model_validator(mode="after")
    def check_start_end_time(self) -> Self:
        if self.end_time < self.start_time:
            raise ValueError(
                f"endTime ({self.end_time}) cannot be before startTime ({self.start_time})."
            )
        return self
