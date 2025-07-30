"""Internal service argument models.

This module contains Pydantic models marked for INTERNAL USE ONLY,
including various limit and constraint queries.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import OrderSide


class GetMaxBorrowQuantityArgs(BaseModel):
    """Args for fetching max borrow quantity from exchange (INTERNAL VALIDATION)."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: Symbol = Field(...)


class GetMaxOrderQuantityArgs(BaseModel):
    """Args for fetching max order quantity from exchange (INTERNAL VALIDATION)."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: Symbol = Field(...)
    side: OrderSide
    price: Decimal | None = Field(default=None, gt=Decimal(0))
    reduce_only: bool | None = Field(default=None)
    auto_borrow: bool | None = Field(default=None)
    auto_borrow_repay: bool | None = Field(default=None)
    auto_lend_redeem: bool | None = Field(default=None)


class GetMaxWithdrawalQuantityArgs(BaseModel):
    """Args for fetching max withdrawal quantity from exchange (INTERNAL VALIDATION)."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: Symbol = Field(...)
    auto_borrow: bool | None = Field(default=None)
    auto_lend_redeem: bool | None = Field(default=None)
