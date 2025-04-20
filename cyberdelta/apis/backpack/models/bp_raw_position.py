"""
Backpack API Position Models
--------------------------

Strict Pydantic models for validating position and position update responses from the Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business logic.
"""

from pydantic import BaseModel, ConfigDict, Field


class BackpackRawPosition(BaseModel):
    """
    Pydantic model for a raw open position from `/api/v1/positions` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse position payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol (e.g., 'BTC_USDC_PERP').
        size (str): Net position size (as string).
        entry_price (str): Average entry price (as string).
        mark_price (str): Current mark price (as string).
    """

    symbol: str = Field(..., alias="symbol")
    size: str = Field(..., alias="positionSize")
    entry_price: str = Field(..., alias="entryPrice")
    mark_price: str = Field(..., alias="markPrice")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawPositionUpdate(BaseModel):
    """
    Pydantic model for a raw position update event from the Backpack WebSocket stream (`positionUpdate`).

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
