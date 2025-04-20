"""
Backpack API Position Models (RAW)
----------------------------------

Strict Pydantic models for validating position responses from the
Backpack Exchange API. These models are for boundary validation only:
- 1:1 contract with the Backpack OpenAPI schema (no optionality, no business logic)
- All fields required, types and structure must match the OpenAPI spec exactly
- Used only for parsing/validating raw API responses
"""

from pydantic import BaseModel, ConfigDict, Field


class SqrtFunction(BaseModel):
    """
    Pydantic model for the 'SqrtFunction' used in PositionImfFunction.
    """

    base: str = Field(..., alias="base")
    factor: str = Field(..., alias="factor")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class PositionImfFunction(BaseModel):
    """
    Pydantic model for the 'PositionImfFunction' (currently only supports 'sqrt').
    """

    type: str = Field(..., alias="type")  # Must be 'sqrt'
    base: str = Field(..., alias="base")
    factor: str = Field(..., alias="factor")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawPosition(BaseModel):
    """
    Strict Pydantic model for a raw open position from `/api/v1/positions` (Backpack REST API).

    This model mirrors the Backpack OpenAPI 'FuturePositionWithMargin' schema exactly.
    All fields are required and must match the API contract. No business logic or optionality.
    """

    break_even_price: str = Field(..., alias="breakEvenPrice")
    entry_price: str = Field(..., alias="entryPrice")
    est_liquidation_price: str = Field(..., alias="estLiquidationPrice")
    imf: str = Field(..., alias="imf")
    imf_function: PositionImfFunction = Field(..., alias="imfFunction")
    mark_price: str = Field(..., alias="markPrice")
    mmf: str = Field(..., alias="mmf")
    mmf_function: PositionImfFunction = Field(..., alias="mmfFunction")
    net_cost: str = Field(..., alias="netCost")
    net_quantity: str = Field(..., alias="netQuantity")
    net_exposure_quantity: str = Field(..., alias="netExposureQuantity")
    net_exposure_notional: str = Field(..., alias="netExposureNotional")
    pnl_realized: str = Field(..., alias="pnlRealized")
    pnl_unrealized: str = Field(..., alias="pnlUnrealized")
    cumulative_funding_payment: str = Field(..., alias="cumulativeFundingPayment")
    symbol: str = Field(..., alias="symbol")
    user_id: int = Field(..., alias="userId")
    position_id: str = Field(..., alias="positionId")
    cumulative_interest: str = Field(..., alias="cumulativeInterest")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawPositionUpdate(BaseModel):
    """
    Pydantic model for a raw position update event from the Backpack WebSocket stream
    (`positionUpdate`).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse position update events received from the exchange.

    Attributes:
        event_type (str): Event type (e.g., 'positionOpened', ...).
        event_time (int | str | float | None): Event time.
        symbol (str): Trading symbol.
        break_event_price (str | None): Break event price.
        entry_price (str | None): Entry price.
        liquidation_price (str | None): Estimated liquidation price.
        initial_margin_fraction (str | None): Initial margin fraction.
        mark_price (str | None): Mark price.
        maintenance_margin_fraction (str | None): Maintenance margin fraction.
        net_quantity (str | None): Net quantity.
        net_exposure_quantity (str | None): Net exposure quantity.
        net_exposure_notional (str | None): Net exposure notional.
    """

    event_type: str = Field(..., alias="e")
    event_time: int | str | float | None = Field(..., alias="E")
    symbol: str = Field(..., alias="s")
    break_event_price: str | None = Field(None, alias="b")
    entry_price: str | None = Field(None, alias="B")
    liquidation_price: str | None = Field(None, alias="l")
    initial_margin_fraction: str | None = Field(None, alias="f")
    mark_price: str | None = Field(None, alias="M")
    maintenance_margin_fraction: str | None = Field(None, alias="m")
    net_quantity: str | None = Field(None, alias="q")
    net_exposure_quantity: str | None = Field(None, alias="Q")
    net_exposure_notional: str | None = Field(None, alias="n")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
