"""
CyberDeltaEngine: Backpack API Raw Models (Account Summary)
----------------------------------------------------------

This module defines the Pydantic model for validating the *raw* structure
of the Backpack Exchange API response when querying for account summary details.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


class BackpackRawAccountSummary(BaseModel):
    """
    Pydantic model for the raw account summary data from Backpack.

    Corresponds to the `AccountSummary` schema in Backpack's OpenAPI specification.
    Ensures that all fields from the API response are correctly typed and validated
    at the raw data boundary.
    """

    auto_borrow_settlements: bool = Field(..., alias="autoBorrowSettlements")
    auto_lend: bool = Field(..., alias="autoLend")
    auto_realize_pnl: bool = Field(..., alias="autoRealizePnl")
    auto_repay_borrows: bool = Field(..., alias="autoRepayBorrows")
    borrow_limit: Decimal = Field(..., alias="borrowLimit")
    futures_maker_fee: Decimal = Field(..., alias="futuresMakerFee")
    futures_taker_fee: Decimal = Field(..., alias="futuresTakerFee")
    leverage_limit: Decimal = Field(..., alias="leverageLimit")
    limit_orders: int = Field(..., alias="limitOrders")
    liquidating: bool
    position_limit: Decimal = Field(..., alias="positionLimit")
    spot_maker_fee: Decimal = Field(..., alias="spotMakerFee")
    spot_taker_fee: Decimal = Field(..., alias="spotTakerFee")
    trigger_orders: int = Field(..., alias="triggerOrders")

    model_config = ConfigDict(
        populate_by_name=True, extra="forbid", frozen=True, validate_assignment=True
    )

    @field_validator(
        "borrow_limit",
        "futures_maker_fee",
        "futures_taker_fee",
        "leverage_limit",
        "position_limit",
        "spot_maker_fee",
        "spot_taker_fee",
        mode="before",
    )
    @classmethod
    def _validate_decimal_strings(cls, v: object, info: ValidationInfo) -> Decimal:
        """Validates string fields that should be finite Decimals."""
        field_name_str = info.field_name
        if field_name_str is None:
            # This should ideally not happen if Pydantic provides a field name.
            raise TypeError("Field name is unexpectedly None during validation.")

        if not isinstance(v, str):
            raise ValueError(f"Field {field_name_str} must be a string, got {type(v)}")

        validated_str = validate_str_field(v, field_name=field_name_str)

        # The parse_decimal_value helper should raise ValueError on failure or non-finite.
        decimal_value = parse_decimal_value(validated_str, field_name=field_name_str)
        if decimal_value is None:  # Defensive check if parse_decimal_value can return None
            raise ValueError(f"Field {field_name_str} could not be parsed to a valid Decimal.")
        return decimal_value

    @field_validator("limit_orders", "trigger_orders", mode="before")
    @classmethod
    def _validate_positive_integer(cls, v: object, info: ValidationInfo) -> int:
        """Validates that integer fields are non-negative."""
        field_name_str = info.field_name
        if field_name_str is None:
            raise TypeError("Field name is unexpectedly None during validation.")

        if not isinstance(v, int):
            # Backpack OpenAPI specifies integer, not string.
            raise ValueError(f"Field {field_name_str} must be an integer, got {type(v)}")
        if v < 0:
            raise ValueError(f"Field {field_name_str} must be non-negative, got {v}")
        return v

    @field_validator(
        "auto_borrow_settlements",
        "auto_lend",
        "auto_realize_pnl",
        "auto_repay_borrows",
        "liquidating",
        mode="before",
    )
    @classmethod
    def _validate_boolean_types(cls, v: object, info: ValidationInfo) -> bool:
        """Validates boolean fields are actual booleans."""
        field_name_str = info.field_name
        if field_name_str is None:
            raise TypeError("Field name is unexpectedly None during validation.")

        if not isinstance(v, bool):
            raise ValueError(f"Field {field_name_str} must be a boolean, got {type(v)}")
        return v
