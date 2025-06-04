"""Backpack API Transfer (Deposit/Withdrawal) Models
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
    RawBpDepositAmountString,
    RawBpExtendedOrderSideString,
    RawBpLiquidationPriceString,
    RawBpLiquidationQuantityString,
    RawBpNonEmptyStringMax32,
    RawBpNonEmptyStringMax64,
    RawBpOptionalNonEmptyString,
    RawBpParsablePositiveFiniteDecimalString,
    RawBpStringToDatetime,
    RawBpTransferStatusString,
    RawBpWithdrawalAmountString,
)


class BackpackRawWithdrawal(BaseModel):
    """Pydantic model for a raw withdrawal object from `/api/v1/withdrawals` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse withdrawal payloads received from the exchange.

    Attributes:
        id (str): Withdrawal ID.
        asset (str): Asset symbol.
        amount (str): Withdrawal amount (as string, non-negative finite decimal).
        status (str): Withdrawal status (e.g., 'pending', 'completed', 'failed', 'cancelled').
        time (datetime | None): Timestamp of the withdrawal.

    """

    id: RawBpNonEmptyStringMax64 = Field(..., alias="id")
    asset: RawBpNonEmptyStringMax32 = Field(..., alias="asset")
    amount: RawBpWithdrawalAmountString = Field(..., alias="amount")
    status: RawBpTransferStatusString = Field(..., alias="status")
    time: RawBpStringToDatetime | None = Field(None, alias="time")

    # Optional fields
    address: RawBpOptionalNonEmptyString = Field(None, alias="address")
    fee: RawBpParsablePositiveFiniteDecimalString | None = Field(None, alias="fee")
    method: RawBpNonEmptyStringMax32 | None = Field(None, alias="method")
    network: RawBpOptionalNonEmptyString = Field(None, alias="network")
    subaccount: int | None = Field(None, alias="subaccount")
    to_address: RawBpOptionalNonEmptyString = Field(None, alias="to_address")
    transaction_hash: RawBpOptionalNonEmptyString = Field(None, alias="transaction_hash")
    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        validate_by_name=True,
        frozen=True,
    )


class BackpackRawDeposit(BaseModel):
    """Pydantic model for a raw deposit object from `/api/v1/deposits` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse deposit payloads received from the exchange.

    Attributes:
        id (str): Deposit ID.
        asset (str): Asset symbol.
        amount (str): Deposit amount (as string, non-negative finite decimal).
        status (str): Deposit status (e.g., 'pending', 'completed').
        time (datetime | None): Timestamp of the deposit.

    """

    id: RawBpNonEmptyStringMax64 = Field(..., alias="id")
    asset: RawBpNonEmptyStringMax32 = Field(..., alias="asset")
    amount: RawBpDepositAmountString = Field(..., alias="amount")
    status: RawBpTransferStatusString = Field(..., alias="status")
    time: RawBpStringToDatetime | None = Field(None, alias="time")

    # Optional fields
    address: RawBpOptionalNonEmptyString = Field(None, alias="address")
    fee: RawBpOptionalNonEmptyString = Field(None, alias="fee")
    method: RawBpNonEmptyStringMax32 | None = Field(None, alias="method")
    network: RawBpOptionalNonEmptyString = Field(None, alias="network")
    subaccount: int | None = Field(None, alias="subaccount")
    to_address: RawBpOptionalNonEmptyString = Field(None, alias="to_address")
    transaction_hash: RawBpOptionalNonEmptyString = Field(None, alias="transaction_hash")
    confirmation_block_number: int | None = Field(None, alias="confirmation_block_number")
    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        validate_by_name=True,
        frozen=True,
    )


class BackpackRawLiquidation(BaseModel):
    """Pydantic model for a raw liquidation event from `/api/v1/liquidations` (Backpack REST API).

    Mirrors the Backpack OpenAPI schema exactly, enforcing strict field validation.
    Use this model to validate and parse liquidation event payloads received from the exchange.

    Attributes:
        symbol (str): Trading symbol. (max_length=64, per OpenAPI spec)
        price (str): Liquidation price (as string, non-negative finite decimal).
        quantity (str): Liquidated quantity (as string, non-negative finite decimal).
        side (str): Side ('buy', 'sell').
        time (datetime | None): Timestamp of the liquidation.
        liquidation_id (str | None): Unique ID for the liquidation event.

    """

    symbol: RawBpNonEmptyStringMax32 = Field(..., alias="symbol")
    quantity: RawBpLiquidationQuantityString = Field(..., alias="quantity")
    price: RawBpLiquidationPriceString = Field(..., alias="price")
    side: RawBpExtendedOrderSideString = Field(..., alias="side")
    time: RawBpStringToDatetime | None = Field(None, alias="time")
    liquidation_id: RawBpNonEmptyStringMax64 | None = Field(None, alias="liquidation_id")
    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        validate_by_name=True,
        frozen=True,
    )
