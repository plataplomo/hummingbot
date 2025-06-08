"""Backpack API Funding Rate and Mark Price Models.

------------------------------------------

This module defines strict Pydantic models for validating funding rate and mark
price responses from the Backpack Exchange API. These models are used for boundary
validation and transformation, not for internal business logic.

Models:
    - BackpackRawFundingRate: Validates funding rate objects.
    - BackpackRawMarkPrice: Validates mark price and funding info objects.
    - BackpackRawFundingIntervalRate: Validates funding interval rate objects.

These models use common raw types for validation, ensuring robustness and security
at the data ingestion boundary.
"""

from pydantic import BaseModel, ConfigDict, Field

# Removed direct imports from cyberdelta.utils.parsing
from cyberdelta.apis.backpack.models.bp_common_raw_types import (
    RawBpFundingRateTimestamp,
    RawBpIsoTimestampString,
    RawBpNonEmptyStringMax64,
    RawBpParsableFiniteDecimalString,
)


class BackpackRawFundingRate(BaseModel):
    """Pydantic model for a raw funding rate object from `/api/v1/funding` (Backpack REST API).

    This model mirrors the Backpack OpenAPI schema, using common raw types for validation.

    Attributes:
        symbol (str): The trading symbol (e.g., 'SOL_USDC').
        rate (str): The funding rate as a string, validated to be parsable to a finite decimal.
        mark_price (str): The mark price as a string, validated to be parsable to a finite decimal.
        index_price (str): The index price (string), validated as parsable to a finite decimal.
        time (int | float | str): The timestamp of the funding rate data, validated for a
                                  specific range.

    """

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    funding_rate: RawBpParsableFiniteDecimalString = Field(..., alias="rate")
    mark_price: RawBpParsableFiniteDecimalString = Field(..., alias="markPrice")
    index_price: RawBpParsableFiniteDecimalString = Field(..., alias="indexPrice")
    time: RawBpFundingRateTimestamp = Field(..., alias="time")

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    # All @field_validator methods removed


class BackpackRawMarkPrice(BaseModel):
    """Pydantic model for a raw mark price and funding info object from Backpack API.

    Validates responses from `/api/v1/markPrice` (Backpack REST API).
    This model mirrors the Backpack OpenAPI schema, using common raw types for validation.

    Attributes:
        symbol (str): Trading symbol.
        mark_price (str): Mark price (validated as a parsable decimal string).
        funding_rate (str): Estimated next funding rate (validated as a parsable decimal string).
        # funding_time: (Removed from attributes as it's not in the original model fields)

    """

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    mark_price: RawBpParsableFiniteDecimalString = Field(..., alias="markPrice")
    funding_rate: RawBpParsableFiniteDecimalString = Field(..., alias="fundingRate")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    # All @field_validator methods removed


class BackpackRawFundingIntervalRate(BaseModel):
    """Pydantic model for a single raw funding interval rate object from Backpack API.

    Validates objects from the list returned by `/api/v1/fundingRates` (Backpack REST API).
    The API returns:
    - "fundingRate": "-0.000015513" (maps to rate field)
    - "intervalEndTimestamp": "2025-06-09T00:00:00" (maps to time field)
    - "symbol": "SOL_USDC_PERP"

    Attributes:
        symbol (str): The trading symbol (e.g., 'SOL_USDC_PERP').
        rate (str): The funding rate for the interval, validated as parsable to a finite decimal.
        time (str): The timestamp for the funding interval as ISO datetime string.

    """

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    rate: RawBpParsableFiniteDecimalString = Field(..., alias="fundingRate")
    time: RawBpIsoTimestampString = Field(..., alias="intervalEndTimestamp")

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)
    # No @field_validator methods here; validation is by the common raw types.
