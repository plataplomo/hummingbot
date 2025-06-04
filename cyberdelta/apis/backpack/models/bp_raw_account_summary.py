"""CyberDeltaEngine: Backpack API Raw Models (Account Summary)
----------------------------------------------------------

This module defines the Pydantic model for validating the *raw* structure
of the Backpack Exchange API response when querying for account summary details.
"""

from pydantic import BaseModel, ConfigDict, Field

from .bp_common_raw_types import (
    RawBpNonNegativeInt,
    RawBpStrictBool,
    RawBpStringToFiniteDecimal,
)


class BackpackRawAccountSummary(BaseModel):
    """Pydantic model for the raw account summary data from Backpack.

    Corresponds to the `AccountSummary` schema in Backpack's OpenAPI specification.
    Ensures that all fields from the API response are correctly typed and validated
    at the raw data boundary using common raw types.
    """

    auto_borrow_settlements: RawBpStrictBool = Field(..., alias="autoBorrowSettlements")
    auto_lend: RawBpStrictBool = Field(..., alias="autoLend")
    auto_realize_pnl: RawBpStrictBool = Field(..., alias="autoRealizePnl")
    auto_repay_borrows: RawBpStrictBool = Field(..., alias="autoRepayBorrows")

    borrow_limit: RawBpStringToFiniteDecimal = Field(..., alias="borrowLimit")
    futures_maker_fee: RawBpStringToFiniteDecimal = Field(..., alias="futuresMakerFee")
    futures_taker_fee: RawBpStringToFiniteDecimal = Field(..., alias="futuresTakerFee")
    leverage_limit: RawBpStringToFiniteDecimal = Field(..., alias="leverageLimit")

    limit_orders: RawBpNonNegativeInt = Field(..., alias="limitOrders")
    liquidating: RawBpStrictBool

    position_limit: RawBpStringToFiniteDecimal = Field(..., alias="positionLimit")
    spot_maker_fee: RawBpStringToFiniteDecimal = Field(..., alias="spotMakerFee")
    spot_taker_fee: RawBpStringToFiniteDecimal = Field(..., alias="spotTakerFee")

    trigger_orders: RawBpNonNegativeInt = Field(..., alias="triggerOrders")

    model_config = ConfigDict(
        populate_by_name=True, extra="forbid", frozen=True, validate_assignment=True,
    )
