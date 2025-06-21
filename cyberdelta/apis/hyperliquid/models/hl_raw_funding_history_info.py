"""Pydantic Models for Hyperliquid Raw Info Endpoint Responses.

---------------------------------------------------------

This module defines Pydantic models that represent the raw structure of
responses from Hyperliquid's INFO endpoints (e.g., /info).
These models are used for initial validation of the external API contract.
"""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, RootModel, field_validator

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawHlCoinName,
    RawHlParsableFiniteDecimalString,
    RawHlTimestampMsInt,
)
from cyberdelta.utils.parsing import validate_str_field
from cyberdelta.utils.typing import is_dict_str_any, is_list_any


class HyperliquidRawFundingHistoryItem(BaseModel):
    """Represents a single item in the historical funding rates response array.

    This model validates individual funding history entries from Hyperliquid's `/info`
    endpoint (type: "fundingHistory"). Each item contains funding rate, premium, and
    timestamp information for a specific coin.

    Example: {"coin": "ETH", "fundingRate": "-0.00022196", "premium": "-0.00052196",
              "time": 1683849600076}
    """

    model_config = ConfigDict(
        extra="forbid",  # No extra fields allowed
        frozen=True,  # Raw models are immutable snapshots of API response
        populate_by_name=True,  # Allows use of aliases if defined (none here yet)
    )

    coin: RawHlCoinName
    funding_rate: RawHlParsableFiniteDecimalString = Field(alias="fundingRate")
    # Optional, as some exchanges might not have it for funding
    premium: RawHlParsableFiniteDecimalString
    time: RawHlTimestampMsInt


class HyperliquidRawFundingHistoryRequestPayload(BaseModel):
    """Strict boundary model for the request payload for the 'fundingHistory' info type.

    Used only for constructing and validating the payload sent to the Hyperliquid API when
    requesting historical funding rates for a specific coin. Never use for internal business logic.

    Fields:
        type (Literal['fundingHistory']): Must be 'fundingHistory'.
        coin (RawHlCoinName): The coin symbol (e.g., "ETH").
        startTime (RawHlTimestampMsInt): Start time in milliseconds since Unix epoch.
        endTime (RawHlTimestampMsInt | None): End time in milliseconds since Unix epoch (optional).
    """

    model_config = ConfigDict(
        extra="forbid",  # No extra fields allowed
        frozen=True,  # Request models are immutable
        populate_by_name=True,  # Allows use of aliases
    )

    type: Annotated[
        Literal["fundingHistory"],
        BeforeValidator(
            lambda v: validate_str_field(v, field_name="type", max_length=32, allow_empty=False),
        ),
    ] = Field(default="fundingHistory")

    coin: RawHlCoinName

    start_time: RawHlTimestampMsInt = Field(
        alias="startTime",
        description="Start time in milliseconds since Unix epoch",
    )

    end_time: RawHlTimestampMsInt | None = Field(
        alias="endTime",
        default=None,
        description="End time in milliseconds since Unix epoch (optional)",
    )


class HyperliquidRawFundingHistoryResponse(RootModel[list[HyperliquidRawFundingHistoryItem]]):
    """Response model for list of historical funding rates.

    This RootModel validates an array of funding history items, handling
    all preprocessing and validation for the list structure.
    """

    root: list[HyperliquidRawFundingHistoryItem]

    @property
    def items(self) -> list[HyperliquidRawFundingHistoryItem]:
        """Return the validated list of funding history items."""
        return self.root

    model_config = ConfigDict(frozen=True)

    @field_validator("root", mode="before")
    @classmethod
    def validate_funding_list(cls, v: object) -> list[dict[str, Any]]:
        """Validate and preprocess the list of funding history items.

        This validator handles:
        - Type checking that input is a list
        - Validating each item is a dictionary
        - Providing detailed error messages for malformed items
        """
        if not is_list_any(v):
            raise ValueError(
                f"Unexpected historical_funding_rates response format: expected list, "
                f"got {type(v).__name__}"
            )

        validated_items: list[dict[str, Any]] = []
        for i, item in enumerate(v):
            if not is_dict_str_any(item):
                raise ValueError(
                    f"Expected dict for historical funding rate item, "
                    f"got {type(item).__name__} at index {i}"
                )

            # item is now properly typed as dict[str, Any] due to TypeGuard
            validated_items.append(item)

        return validated_items
