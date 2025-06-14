"""Raw models for Backpack account limits endpoints.

These models are for INTERNAL USE ONLY within BackpackAccountService
for risk calculation validation and reconciliation purposes. They are
NOT exposed through the public API to maintain exchange agnosticism.

Based on OpenAPI specification for:
- GET /api/v1/account/limits/borrow
- GET /api/v1/account/limits/order
- GET /api/v1/account/limits/withdrawal
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from .bp_common_raw_types import (
    RawBpNonEmptyStringMax64,
    RawBpOptionalStrictBool,
    RawBpStringToFiniteDecimal,
)


class BackpackRawMaxBorrowQuantity(BaseModel):
    """Raw response from /api/v1/account/limits/borrow endpoint."""

    max_borrow_quantity: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="maxBorrowQuantity",
        description="Maximum quantity that can be borrowed for this symbol",
    )
    symbol: RawBpNonEmptyStringMax64 = Field(
        ..., alias="symbol", description="Asset symbol for the borrow limit"
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        validate_assignment=True,
    )


class BackpackRawMaxOrderQuantity(BaseModel):
    """Raw response from /api/v1/account/limits/order endpoint."""

    auto_borrow: RawBpOptionalStrictBool = Field(
        None, alias="autoBorrow", description="Whether auto-borrow is enabled"
    )
    auto_borrow_repay: RawBpOptionalStrictBool = Field(
        None, alias="autoBorrowRepay", description="Whether auto-borrow repay is enabled"
    )
    auto_lend_redeem: RawBpOptionalStrictBool = Field(
        None, alias="autoLendRedeem", description="Whether auto-lend redeem is enabled"
    )
    max_order_quantity: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="maxOrderQuantity",
        description="Maximum order quantity for the specified parameters",
    )
    price: str | None = Field(
        None, alias="price", description="Price used for the calculation (if provided)"
    )
    reduce_only: RawBpOptionalStrictBool = Field(
        None, alias="reduceOnly", description="Whether this is a reduce-only order"
    )
    side: RawBpNonEmptyStringMax64 = Field(..., alias="side", description="Order side (Bid/Ask)")
    symbol: RawBpNonEmptyStringMax64 = Field(
        ..., alias="symbol", description="Trading symbol for the order limit"
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        validate_assignment=True,
    )


class BackpackRawMaxWithdrawalQuantity(BaseModel):
    """Raw response from /api/v1/account/limits/withdrawal endpoint."""

    auto_borrow: RawBpOptionalStrictBool = Field(
        None, alias="autoBorrow", description="Whether auto-borrow is enabled for withdrawal"
    )
    auto_lend_redeem: RawBpOptionalStrictBool = Field(
        None,
        alias="autoLendRedeem",
        description="Whether auto-lend redeem is enabled for withdrawal",
    )
    max_withdrawal_quantity: RawBpStringToFiniteDecimal = Field(
        ..., alias="maxWithdrawalQuantity", description="Maximum quantity that can be withdrawn"
    )
    symbol: RawBpNonEmptyStringMax64 = Field(
        ..., alias="symbol", description="Asset symbol for the withdrawal limit"
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        validate_assignment=True,
    )
