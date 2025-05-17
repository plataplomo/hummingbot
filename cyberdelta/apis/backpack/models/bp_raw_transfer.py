"""
Backpack API Transfer (Deposit/Withdrawal) Models
------------------------------------------------

Defines strict Pydantic models for validating deposit and withdrawal responses from
the Backpack Exchange API. These models are used for boundary validation and
transformation, not for internal business logic.

Models:
    - BackpackRawDeposit: Validates deposit objects.
    - BackpackRawWithdrawal: Validates withdrawal objects.
    - BackpackRawLiquidation: Validates liquidation event objects.

Validation Pattern:
    - Strict type, format, and constraint checks on all fields.
    - `extra='forbid'` to reject unknown fields.
    - `frozen=True` to ensure immutability after validation.

These models act as a strict shield between external API data and internal business
logic, ensuring robustness and security at the data ingestion boundary.
"""

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.backpack.models.bp_common_raw_types import (
    RawBpExtendedOrderSideString,
    RawBpNonEmptyStringMax32,
    RawBpNonEmptyStringMax64,
    RawBpParsableNonNegativeFiniteDecimalString,
    RawBpTransferStatusString,
)


class BackpackRawWithdrawal(BaseModel):
    """
    Pydantic model for a raw withdrawal object from `/api/v1/withdrawals` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse withdrawal payloads received from the exchange.

    Attributes:
        id (str): Withdrawal ID.
        asset (str): Asset symbol.
        amount (str): Withdrawal amount (as string, non-negative finite decimal).
        status (str): Withdrawal status (e.g., 'pending', 'completed').
    """

    id: RawBpNonEmptyStringMax64 = Field(..., alias="id")
    asset: RawBpNonEmptyStringMax32 = Field(..., alias="asset")
    amount: RawBpParsableNonNegativeFiniteDecimalString = Field(..., alias="amount")
    status: RawBpTransferStatusString = Field(..., alias="status")
    model_config = ConfigDict(
        populate_by_name=True, extra="forbid", validate_by_name=True, frozen=True
    )


class BackpackRawDeposit(BaseModel):
    """
    Pydantic model for a raw deposit object from `/api/v1/deposits` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse deposit payloads received from the exchange.

    Attributes:
        id (str): Deposit ID.
        asset (str): Asset symbol.
        amount (str): Deposit amount (as string, non-negative finite decimal).
        status (str): Deposit status (e.g., 'pending', 'completed').
    """

    id: RawBpNonEmptyStringMax64 = Field(..., alias="id")
    asset: RawBpNonEmptyStringMax32 = Field(..., alias="asset")
    amount: RawBpParsableNonNegativeFiniteDecimalString = Field(..., alias="amount")
    status: RawBpTransferStatusString = Field(..., alias="status")
    model_config = ConfigDict(
        populate_by_name=True, extra="forbid", validate_by_name=True, frozen=True
    )


class BackpackRawLiquidation(BaseModel):
    """
    Pydantic model for a raw liquidation event from `/api/v1/liquidations` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse liquidation event payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol. (max_length=64, per OpenAPI spec)
        price (str): Liquidation price (as string, non-negative finite decimal).
        quantity (str): Liquidated quantity (as string, non-negative finite decimal).
        side (str): Side ('buy', 'sell').
    """

    symbol: RawBpNonEmptyStringMax64 = Field(..., alias="symbol")
    price: RawBpParsableNonNegativeFiniteDecimalString = Field(..., alias="price")
    quantity: RawBpParsableNonNegativeFiniteDecimalString = Field(..., alias="quantity")
    side: RawBpExtendedOrderSideString = Field(..., alias="side")
    model_config = ConfigDict(
        populate_by_name=True, extra="forbid", validate_by_name=True, frozen=True
    )
