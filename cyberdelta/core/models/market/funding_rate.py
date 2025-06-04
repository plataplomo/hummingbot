from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value, validate_str_field


class HyperliquidFundingDetails(BaseModel):
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

    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator(
        "hl_funding_hourly",
        "hl_prev_day_px",
        "hl_day_ntl_vlm",
        "hl_impact_px",
        "premium",
        mode="before",
    )
    @classmethod
    def parse_decimal_fields(
        cls, raw_value: str | int | float | Decimal | None, info: object,
    ) -> Decimal | None:
        """Parse and validate decimal fields to ensure they are valid finite Decimal objects."""
        value = parse_decimal_value(raw_value)
        if value is not None and not value.is_finite():
            raise ValueError(f"Value must be a finite decimal, got {value}")
        return value


class BackpackFundingDetails(BaseModel):
    """Backpack-specific funding rate enrichment fields for extension slot on FundingRate.

    # TODO: Add BP-specific enrichment fields if identified later.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)


class FundingRate(BaseModel):
    """Core internal model for funding rate information across all supported exchanges.
    Contains only essential, universal fields with exchange-specific details in extension slots.
    Immutable (frozen=True) to ensure funding data is not altered after retrieval.

    Fields:
        symbol (str): Trading symbol.
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

    symbol: str
    timestamp: datetime
    funding_rate: Decimal | None = Field(default=None)
    predicted_rate: Decimal | None = Field(default=None)
    mark_price: Decimal | None = Field(default=None, gt=0)
    index_price: Decimal | None = Field(default=None, gt=0)
    next_funding_time: datetime | None = Field(default=None)
    hl_details: HyperliquidFundingDetails | None = Field(default=None)
    bp_details: BackpackFundingDetails | None = Field(default=None)

    model_config = ConfigDict(extra="forbid", validate_assignment=True, frozen=True)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol(cls, value: object) -> str:
        """Validate that symbol is a non-empty string."""
        return validate_str_field(value, field_name="symbol", max_length=64, allow_empty=False)

    @field_validator("funding_rate", "predicted_rate", "mark_price", "index_price", mode="before")
    @classmethod
    def parse_decimal_fields(
        cls, raw_value: str | int | float | Decimal | None, info: object,
    ) -> Decimal | None:
        """Parse and validate decimal fields to ensure they are valid finite Decimal objects."""
        value = parse_decimal_value(raw_value)
        if value is not None and not value.is_finite():
            raise ValueError(f"Value must be a finite decimal, got {value}")
        # The Field(gt=0) constraint will handle ensuring positive values for prices
        return value

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(
        cls, raw_value: datetime | int | float | str | None, info: object,
    ) -> datetime:
        """Parse and validate timestamp to ensure it is a UTC-aware datetime object."""
        value = parse_datetime_utc(raw_value, field_name="timestamp")
        if value is None:
            raise ValueError("timestamp must not be None")
        return value

    @field_validator("next_funding_time", mode="before")
    @classmethod
    def parse_next_funding_time(
        cls, raw_value: datetime | int | float | str | None, info: object,
    ) -> datetime | None:
        """Parse and validate next_funding_time to ensure it is a UTC-aware datetime object."""
        return parse_datetime_utc(raw_value, field_name="next_funding_time")
