"""
Backpack API Funding Rate and Mark Price Models
------------------------------------------

This module defines strict Pydantic models for validating funding rate and mark
price responses from the Backpack Exchange API. These models are used for boundary
validation and transformation, not for internal business logic.

Models:
    - BackpackRawFundingRate: Validates funding rate objects.
    - BackpackRawMarkPrice: Validates mark price and funding info objects.

These models use common raw types for validation, ensuring robustness and security
at the data ingestion boundary.
"""

from pydantic import BaseModel, ConfigDict, Field

# Removed direct imports from cyberdelta.utils.parsing
from .bp_common_raw_types import (
    RawBpFlexibleTimestamp,
    RawBpNonEmptyStringMax64,
    RawBpParsableFiniteDecimalString,
)


class BackpackRawFundingRate(BaseModel):
    """
    Pydantic model for a raw funding rate object from `/api/v1/funding` (Backpack REST API).

    This model mirrors the Backpack OpenAPI schema, using common raw types for validation.

    Attributes:
        symbol (str): Trading symbol.
        funding_rate (str): Current funding rate (validated as a parsable decimal string).
        mark_price (str): Mark price (validated as a parsable decimal string).
        index_price (str): Index price (validated as a parsable decimal string).
        time (Union[int, str, float]): Data timestamp (validated, cannot be None).
    """

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    funding_rate: RawBpParsableFiniteDecimalString = Field(..., alias="rate")
    mark_price: RawBpParsableFiniteDecimalString = Field(..., alias="markPrice")
    index_price: RawBpParsableFiniteDecimalString = Field(..., alias="indexPrice")
    # The field type remains Union[int, str, float] as RawBpFlexibleTimestamp's validator returns the original valid type.
    # The original model had `time: int | str | float | None`, but the validator rejected None.
    # So, the effective type after validation is `Union[int, str, float]`.
    time: RawBpFlexibleTimestamp = Field(..., alias="time")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", validate_by_name=True)

    # All @field_validator methods removed


class BackpackRawMarkPrice(BaseModel):
    """
    Pydantic model for a raw mark price and funding info object
    from `/api/v1/markPrice` (Backpack REST API).

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
