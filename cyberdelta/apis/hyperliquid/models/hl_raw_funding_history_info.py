"""
Pydantic Models for Hyperliquid Raw Info Endpoint Responses
---------------------------------------------------------

This module defines Pydantic models that represent the raw structure of
responses from Hyperliquid's INFO endpoints (e.g., /info).
These models are used for initial validation of the external API contract.
"""

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawHlCoinName,
    RawHlParsableFiniteDecimalString,
    RawHlTimestampMsInt,
)


class HyperliquidRawFundingHistoryItem(BaseModel):
    """
    Represents a single item in the historical funding rates response array
    from Hyperliquid's `/info` endpoint (type: "fundingHistory").
    Example: {"coin": "ETH", "fundingRate": "-0.00022196", "premium": "-0.00052196", "time": 1683849600076}
    """

    model_config = ConfigDict(
        extra="forbid",  # No extra fields allowed
        frozen=True,  # Raw models are immutable snapshots of API response
        populate_by_name=True,  # Allows use of aliases if defined (none here yet)
    )

    coin: RawHlCoinName
    funding_rate: RawHlParsableFiniteDecimalString = Field(alias="fundingRate")
    premium: RawHlParsableFiniteDecimalString  # Optional, as some exchanges might not have it for funding
    time: RawHlTimestampMsInt


# If there were other raw info responses, they would be defined here too.
# For example:
# class HyperliquidRawAnotherInfoResponse(BaseModel):
#     ...
