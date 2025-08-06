"""Funding rate data model for perpetual futures markets.

This module provides models for representing funding rate information across
different exchanges. Funding rates are periodic payments between long and short
position holders in perpetual futures contracts.

The models follow the "Core + Typed Extension Slots" pattern, providing:
- Core fields common across all exchanges (funding rate, mark price, etc.)
- Exchange-specific extension slots for additional data
- Immutable design with strict validation using Decimal for financial precision
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import Field

from cyberdelta.models.base_validators import (
    ExtensionSlotModel,
    ImmutableModel,
    optional_datetime_validator,
    optional_decimal_validator,
    required_datetime_validator,
)
from cyberdelta.symbols.models import Symbol


class HyperliquidFundingDetails(ExtensionSlotModel):
    """Hyperliquid-specific funding rate enrichment fields for extension slot on FundingRate.

    Fields:
        hl_funding_hourly (Decimal | None): Hourly funding rate representation (ApiAssetCtx.funding)
        hl_prev_day_px (Decimal | None): Previous day price (ApiAssetCtx.prevDayPx)
        hl_day_ntl_vlm (Decimal | None): Daily notional volume (ApiAssetCtx.dayNtlVlm)
        hl_impact_px (Decimal | None): Impact price (ApiAssetCtx.impactPx)
        premium (Decimal | None): Premium component from historical funding data.
    """

    hl_funding_hourly: Decimal | None = None
    hl_prev_day_px: Decimal | None = None
    hl_day_ntl_vlm: Decimal | None = None
    hl_impact_px: Decimal | None = None
    premium: Decimal | None = None

    # Config: Extension slot (inherited from ExtensionSlotModel)
    # Use centralized validators
    _validate_optional_decimals = optional_decimal_validator(
        "hl_funding_hourly", "hl_prev_day_px", "hl_day_ntl_vlm", "hl_impact_px", "premium"
    )


class BackpackFundingDetails(ExtensionSlotModel):
    """Backpack-specific funding rate enrichment fields for extension slot on FundingRate.

    # TODO: Add BP-specific enrichment fields if identified later.
    """

    # Config: Extension slot (inherited from ExtensionSlotModel)


class FundingRate(ImmutableModel):
    """Core internal model for funding rate information across all supported exchanges.

    Contains only essential, universal fields with exchange-specific details in extension slots.
    Immutable (frozen=True) to ensure funding data is not altered after retrieval.

    Fields:
        symbol (Symbol): Exchange-specific trading symbol domain object.
        timestamp (datetime): Data snapshot timestamp (UTC-aware).
        funding_rate (Decimal | None): Current funding rate (8hr basis for perpetuals).
        predicted_rate (Decimal | None): Predicted next funding rate.
        mark_price (Decimal | None): Current mark price. Must be positive if provided.
        index_price (Decimal | None): Current index price. Must be positive if provided.
        next_funding_time (datetime | None): Timestamp of next funding event (UTC-aware).
        hl_details (HyperliquidFundingDetails | None): Hyperliquid-specific enrichment slot.
        bp_details (BackpackFundingDetails | None): Backpack-specific enrichment slot.

    Notes:
        - All rate/price fields use Decimal for precision.
        - All timestamp fields use timezone-aware UTC datetime objects.
        - This model and its extension slots are immutable after creation.

    """

    symbol: Symbol
    timestamp: datetime
    funding_rate: Decimal | None = Field(default=None)
    predicted_rate: Decimal | None = Field(default=None)
    mark_price: Decimal | None = Field(default=None, gt=0)
    index_price: Decimal | None = Field(default=None, gt=0)
    next_funding_time: datetime | None = Field(default=None)
    hl_details: HyperliquidFundingDetails | None = Field(default=None)
    bp_details: BackpackFundingDetails | None = Field(default=None)

    # Config: Immutable (inherited from ImmutableModel)
    # Use centralized validators
    _validate_timestamp = required_datetime_validator("timestamp")
    _validate_optional_decimals = optional_decimal_validator(
        "funding_rate", "predicted_rate", "mark_price", "index_price"
    )
    _validate_next_funding = optional_datetime_validator("next_funding_time")

    # Symbol validation is handled by Pydantic's type system
    # No need for a custom validator since Symbol is always valid
