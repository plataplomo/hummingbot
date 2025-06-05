"""Backpack API Position Models (RAW).

----------------------------------

This module defines strict Pydantic models for validating position responses from the Backpack
Exchange API. These models are used for boundary validation and transformation, not for internal
business logic.

Models:
    - BackpackRawPosition: Validates open position objects (all required fields, strict schema).
    - BackpackRawPositionUpdate: Validates position update events from the WebSocket stream.

Validation Pattern:
    - All string fields are strictly validated for type, non-emptiness, max length, and valid UTF-8.
    - Decimal fields are validated for parseability and finiteness.
    - Timestamps accept int, float, or ISO8601-like strings.
    - All extra fields are forbidden.

These models act as a strict shield between external API data and internal business logic, ensuring
robustness and security at the data ingestion boundary.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.backpack.models.bp_common_raw_types import (
    RawBpNonEmptyStringMax64,
    RawBpNonNegativeInt,
    RawBpOptionalFlexibleTimestamp,
    RawBpOptionalParsableFiniteDecimalString,
    RawBpParsableFiniteDecimalString,
)
from cyberdelta.apis.backpack.models.bp_raw_margin_functions import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
)


class BackpackRawPosition(BaseModel):
    """Pydantic model for a raw position object from `/api/v1/position` or WebSocket events.

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse position payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        position_side (str): Position side ('LONG', 'SHORT').
        quantity (str): Position size.
        entry_price (str): Entry price.
        mark_price (str): Mark price.
        leverage (str): Leverage used.
        unrealized_pnl (str): Unrealized PnL.
        liquidation_price (str): Liquidation price.
        margin_type (str): Margin type ('ISOLATED', 'CROSS').
        margin (str): Margin allocated.
        imf (PositionImfFunction | None): Initial margin function parameters.
        sqrt_func (SqrtFunction | None): Square root function parameters.
        update_time (int | float | str | None): Last update time.

    """

    break_even_price: RawBpParsableFiniteDecimalString = Field(..., alias="breakEvenPrice")
    entry_price: RawBpParsableFiniteDecimalString = Field(..., alias="entryPrice")
    est_liquidation_price: RawBpParsableFiniteDecimalString = Field(
        ...,
        alias="estLiquidationPrice",
    )
    imf: RawBpParsableFiniteDecimalString = Field(..., alias="imf")
    imf_function: BackpackRawImfFunction = Field(..., alias="imfFunction")
    mark_price: RawBpParsableFiniteDecimalString = Field(..., alias="markPrice")
    mmf: RawBpParsableFiniteDecimalString = Field(..., alias="mmf")
    mmf_function: BackpackRawMmfFunction = Field(..., alias="mmfFunction")
    net_cost: RawBpParsableFiniteDecimalString = Field(..., alias="netCost")
    net_quantity: RawBpParsableFiniteDecimalString = Field(..., alias="netQuantity")
    net_exposure_quantity: RawBpParsableFiniteDecimalString = Field(
        ...,
        alias="netExposureQuantity",
    )
    net_exposure_notional: RawBpParsableFiniteDecimalString = Field(
        ...,
        alias="netExposureNotional",
    )
    pnl_realized: RawBpParsableFiniteDecimalString = Field(..., alias="pnlRealized")
    pnl_unrealized: RawBpParsableFiniteDecimalString = Field(..., alias="pnlUnrealized")
    cumulative_funding_payment: RawBpParsableFiniteDecimalString = Field(
        ...,
        alias="cumulativeFundingPayment",
    )
    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    user_id: RawBpNonNegativeInt = Field(..., alias="userId")
    position_id: RawBpNonEmptyStringMax64 = Field(..., alias="positionId")
    cumulative_interest: RawBpParsableFiniteDecimalString = Field(..., alias="cumulativeInterest")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )


class BackpackRawPositionUpdate(BaseModel):
    """Pydantic model for a raw position update event from Backpack WebSocket positionUpdate stream.

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse position update events received from the exchange.

    Attributes:
        event_type (str): Event type (e.g., 'positionUpdate').
        event_time (int | str | float | None): Event time.
        symbol (str): Trading symbol.
        position_side (str): Position side ('LONG', 'SHORT').
        quantity (str): Position size.
        entry_price (str): Entry price.
        mark_price (str): Mark price.
        leverage (str): Leverage used.
        unrealized_pnl (str): Unrealized PnL.
        liquidation_price (str): Liquidation price.
        margin_type (str): Margin type ('ISOLATED', 'CROSS').
        margin (str): Margin allocated.
        imf (PositionImfFunction | None): Initial margin function parameters.
        sqrt_func (SqrtFunction | None): Square root function parameters.

    """

    event_type: Literal["positionUpdate"] = Field(..., alias="e")
    event_time: RawBpOptionalFlexibleTimestamp = Field(..., alias="E")
    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="s")
    break_event_price: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="b")
    entry_price: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="B")
    liquidation_price: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="l")
    initial_margin_fraction: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="f")
    mark_price: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="M")
    maintenance_margin_fraction: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="m")
    net_quantity: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="q")
    net_exposure_quantity: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="Q")
    net_exposure_notional: RawBpOptionalParsableFiniteDecimalString = Field(None, alias="n")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
        validate_assignment=True,
    )
