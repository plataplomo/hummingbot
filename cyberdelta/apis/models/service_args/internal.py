"""Internal service argument models.

This module contains Pydantic models marked for INTERNAL USE ONLY,
including various limit and constraint queries.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.enums import OrderSide


class GetMaxBorrowQuantityArgs(BaseModel):
    """Args for fetching max borrow quantity from exchange (INTERNAL VALIDATION)."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(..., min_length=1, max_length=64)


class GetMaxOrderQuantityArgs(BaseModel):
    """Args for fetching max order quantity from exchange (INTERNAL VALIDATION)."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(..., min_length=1, max_length=64)
    side: OrderSide
    price: Decimal | None = Field(default=None, gt=Decimal(0))
    reduce_only: bool | None = Field(default=None)
    auto_borrow: bool | None = Field(default=None)
    auto_borrow_repay: bool | None = Field(default=None)
    auto_lend_redeem: bool | None = Field(default=None)


class GetMaxWithdrawalQuantityArgs(BaseModel):
    """Args for fetching max withdrawal quantity from exchange (INTERNAL VALIDATION)."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(..., min_length=1, max_length=64)
    auto_borrow: bool | None = Field(default=None)
    auto_lend_redeem: bool | None = Field(default=None)
