"""
Backpack API Transfer, Deposit, and Liquidation Models
-----------------------------------------------------

Strict Pydantic models for validating withdrawal, deposit, and liquidation responses from
the Backpack Exchange API.
These models are used for boundary validation and transformation, not for internal business
logic.
"""

from pydantic import BaseModel, ConfigDict, Field


class BackpackRawWithdrawal(BaseModel):
    """
    Pydantic model for a raw withdrawal object from `/api/v1/withdrawals` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse withdrawal payloads received from the exchange.

    Attributes:
        id (str): Withdrawal ID.
        asset (str): Asset symbol.
        amount (str): Withdrawal amount (as string).
        status (str): Withdrawal status (e.g., 'pending', 'completed').
    """

    id: str = Field(..., alias="id")
    asset: str = Field(..., alias="asset")
    amount: str = Field(..., alias="amount")
    status: str = Field(..., alias="status")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawDeposit(BaseModel):
    """
    Pydantic model for a raw deposit object from `/api/v1/deposits` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse deposit payloads received from the exchange.

    Attributes:
        id (str): Deposit ID.
        asset (str): Asset symbol.
        amount (str): Deposit amount (as string).
        status (str): Deposit status (e.g., 'pending', 'completed').
    """

    id: str = Field(..., alias="id")
    asset: str = Field(..., alias="asset")
    amount: str = Field(..., alias="amount")
    status: str = Field(..., alias="status")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class BackpackRawLiquidation(BaseModel):
    """
    Pydantic model for a raw liquidation event from `/api/v1/liquidations` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse liquidation event payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol.
        price (str): Liquidation price (as string).
        quantity (str): Liquidated quantity (as string).
        side (str): Side ('buy', 'sell', etc.).
    """

    symbol: str = Field(..., alias="symbol")
    price: str = Field(..., alias="price")
    quantity: str = Field(..., alias="quantity")
    side: str = Field(..., alias="side")
    model_config = ConfigDict(populate_by_name=True, extra="forbid")
