"""Trading service argument models.

This module contains Pydantic models for trading operations including order placement,
cancellation, and trade history queries.
"""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.apis.base.trading_execution_domain import LiquidityRequirement, OrderExecution
from cyberdelta.apis.exceptions.field_validation import (
    TypeFieldError,
)
from cyberdelta.apis.models.service_args.common import validate_api_str_field
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.exceptions.field_validation import (
    DecimalFieldError,
    RequiredFieldError,
)
from cyberdelta.exceptions.service_validation import (
    IntegerConversionError,
    MissingPriceError,
    MissingStopPriceError,
    PostOnlyLimitError,
    TimeRangeError,
)
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.typing import PotentialDecimalInput, is_potential_decimal_input


class PlaceOrderArgs(BaseModel):
    """Encapsulates all arguments for placing an order.

    This model centralizes input validation for order placement across all exchanges,
    including type checks, value constraints, and inter-parameter dependencies.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: Symbol
    side: OrderSide
    order_type: OrderType
    quantity: Decimal = Field(gt=Decimal(0))
    time_in_force: TimeInForce
    price: Decimal | None = Field(default=None, gt=Decimal(0))
    stop_price: Decimal | None = Field(default=None, gt=Decimal(0))
    client_order_id: str | None = Field(default=None)
    execution: OrderExecution = Field(default_factory=OrderExecution)

    @field_validator("client_order_id", mode="before")
    @classmethod
    def validate_client_order_id_str(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate client_order_id is None or a non-empty string with max length 64.

        Returns:
            Validated client order ID string or None if input was None.
        """
        if v is None:
            return None
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("quantity", "price", "stop_price", mode="before")
    @classmethod
    def parse_decimal_fields(
        cls,
        v: PotentialDecimalInput | None,
        info: ValidationInfo,
    ) -> Decimal | None:
        """Parse decimal fields and ensure they are finite.

        Returns:
            Parsed and validated Decimal value or None for optional fields.

        Raises:
            TypeFieldError: If input is not a valid decimal type.
            DecimalFieldError: If value is not finite or cannot be parsed.
        """
        field_name = str(info.field_name)
        is_required = field_name == "quantity"

        # Use TypeGuard for better type safety
        if v is not None and not is_potential_decimal_input(v):
            raise TypeFieldError(
                field_name=field_name,
                expected_type="string, int, float, or Decimal",
                actual_type=type(v).__name__,
            )

        parsed: Decimal | None
        if is_required:
            parsed = parse_decimal_value(v, allow_none=False, field_name=field_name)
        else:
            parsed = parse_decimal_value(v, allow_none=True, field_name=field_name)
        if parsed is not None and not parsed.is_finite():
            raise DecimalFieldError(
                field_name=field_name,
                value=v,
                reason="must be a finite decimal",
            )
            # Positivity (gt=0) is handled by Field constraint AFTER this validator.
        return parsed

    @model_validator(mode="after")
    def check_parameter_dependencies(self) -> "PlaceOrderArgs":
        """Validate inter-parameter dependencies.

        Returns:
            Self for method chaining.

        Raises:
            MissingPriceError: If price is required but not provided.
            MissingStopPriceError: If stop price is required but not provided.
            PostOnlyLimitError: If post_only is used with non-limit order type.
        """
        if self.order_type in {OrderType.LIMIT, OrderType.STOP_LIMIT} and self.price is None:
            raise MissingPriceError(order_type=self.order_type.value)
        if (
            self.order_type in {OrderType.STOP_MARKET, OrderType.STOP_LIMIT}
            and self.stop_price is None
        ):
            raise MissingStopPriceError(order_type=self.order_type.value)
        # Validate post_only compatibility with order type
        if (
            self.execution.liquidity_requirement == LiquidityRequirement.POST_ONLY
            and self.order_type != OrderType.LIMIT
        ):
            raise PostOnlyLimitError(order_type=self.order_type.value)
        # Note: Specific client_order_id format checks (e.g., Backpack int conversion)
        # should be handled within the exchange-specific RequestBuilder or service,
        # not in this generic Args model.
        return self


class CancelOrderArgs(BaseModel):
    """Encapsulates arguments for cancelling an order.

    This model centralizes input validation for order cancellation across all exchanges,
    ensuring order_id is always a valid non-empty string and handling optional parameters
    like symbol and client_order_id gracefully with validation.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    order_id: str  # Usually the exchange-generated order ID
    symbol: Symbol | None = Field(default=None)  # Often required by exchanges
    client_order_id: str | None = Field(default=None)  # Alternative identifier

    @field_validator("order_id", "client_order_id", mode="before")
    @classmethod
    def validate_string_fields(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate string fields with appropriate requirements.

        Returns:
            str | None: Validated string or None

        Raises:
            RequiredFieldError: If required field is None
        """
        field_name = str(info.field_name)
        is_required = field_name == "order_id"  # order_id is always required

        if v is None:
            if is_required:
                raise RequiredFieldError(field_name=field_name, context="order cancellation")
            return None  # For optional fields

        # Assuming generic string validation, max_length can be adjusted
        # allow_empty should be False for IDs and symbols if they are provided
        return validate_api_str_field(v, field_name=field_name, max_length=128, allow_empty=False)

    @model_validator(mode="after")
    def check_identifiers_logic(self) -> "CancelOrderArgs":
        """Validate identifier logic.

        Example: Some exchanges might require symbol if not using client_order_id,
        or only one of order_id/client_order_id.
        For Backpack, 'symbol' is required, and one of 'orderId' or 'clientId'.
        For Hyperliquid, 'asset' (derived from symbol) and 'oid' (order_id) are needed.
        This generic model ensures order_id is present. Exchange-specific services
        will need to ensure `symbol` is also provided if their RequestBuilder requires it.

        Returns:
            Self for method chaining.
        """
        if self.symbol is None:
            # Depending on exchange specifics, this might be an error for some.
            # For now, allow symbol to be optional in the generic model.
            # The service/builder for a specific exchange will enforce if it's needed.
            pass
        return self


class CancelAllOrdersArgs(BaseModel):
    """Arguments for canceling all orders.

    This model handles cancellation of all open orders for a symbol or all symbols,
    with optional filtering by order side.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: Symbol | None = Field(default=None)
    side: OrderSide | None = Field(default=None)


class GetOrderArgs(BaseModel):
    """Encapsulates arguments for fetching a specific order.

    This model centralizes validation for fetching order details, ensuring consistent
    handling of order_id (primary identifier) and optional parameters like symbol
    and client_order_id which may be required by some exchanges.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    order_id: str  # Primary identifier, usually exchange-generated
    symbol: Symbol | None = Field(default=None)  # Often required or recommended by exchanges
    client_order_id: str | None = Field(default=None)  # Alternative identifier

    @field_validator("order_id", "client_order_id", mode="before")
    @classmethod
    def validate_string_fields(cls, v: object, info: ValidationInfo) -> str | None:
        """Validate string fields with appropriate requirements.

        Returns:
            str | None: Validated string or None

        Raises:
            RequiredFieldError: If required field is None
        """
        field_name = str(info.field_name)
        is_required = field_name == "order_id"

        if v is None:
            if is_required:
                raise RequiredFieldError(field_name=field_name, context="order query")
            return None  # For optional fields

        # Max length for order_id can be quite long for some exchanges (e.g. UUIDs)
        max_len = 128 if field_name in {"order_id", "client_order_id"} else 64
        return validate_api_str_field(
            v,
            field_name=field_name,
            max_length=max_len,
            allow_empty=False,
        )

    @model_validator(mode="after")
    def check_identifier_logic(self) -> "GetOrderArgs":
        """Validate identifier logic.

        While order_id is primary, some exchanges might heavily rely on symbol.
        Backpack requires symbol for its GET /order/{id} endpoint as a query param.
        Hyperliquid's orderStatus needs user + oid, symbol is not directly part of request.
        For a generic model, ensuring order_id is primary.
        If symbol becomes strictly required for all exchanges, it can be made non-optional.

        Returns:
            Self for method chaining.
        """
        if self.symbol is None:
            # Log a debug message or warning if symbol is often needed but not provided.
            pass
        return self


# Alias GetOrderStatusArgs to GetOrderArgs since they have identical fields
GetOrderStatusArgs: type[GetOrderArgs] = GetOrderArgs  # Alias for clarity in signatures


class GetAllOpenOrdersArgs(BaseModel):
    """Encapsulates arguments for fetching all open orders.

    This model centralizes validation for open orders requests, ensuring
    the optional symbol filter is correctly validated if provided.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: Symbol | None = Field(default=None)  # Optional symbol to filter by


class GetOrderHistoryArgs(BaseModel):
    """Encapsulates arguments for fetching order history.

    This model centralizes validation for order history requests, including
    time range validation, positive limit constraints, and string field validation.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: Symbol | None = Field(default=None)
    start_time: datetime | None = Field(default=None)
    end_time: datetime | None = Field(default=None)
    limit: int | None = Field(default=None, gt=0)  # Limit must be positive if provided
    order_id: str | None = Field(default=None)
    client_order_id: str | None = Field(default=None)

    @field_validator("order_id", "client_order_id", mode="before")
    @classmethod
    def validate_optional_strings(
        cls,
        v: str | float | None,
        info: ValidationInfo,
    ) -> str | None:
        """Validate optional string fields are non-empty with reasonable max length.

        Returns:
            Validated string value or None if input was None.
        """
        if v is None:
            return None
        # Assuming generic string validation for these, max_length can be adjusted
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("start_time", "end_time", mode="before")
    @classmethod
    def parse_optional_datetime_utc(
        cls,
        v: str | float | datetime | None,
        info: ValidationInfo,
    ) -> datetime | None:
        """Parse optional datetime fields to UTC.

        Returns:
            Parsed UTC datetime or None if input was None.
        """
        if v is None:
            return None
        # parse_datetime_utc will return None if parsing fails, which is acceptable
        # for optional fields
        return parse_datetime_utc(v, field_name=str(info.field_name))

    @field_validator("limit", mode="before")
    @classmethod
    def parse_optional_int(cls, v: object, info: ValidationInfo) -> int | None:
        """Parse optional integer fields.

        Returns:
            Parsed integer value or None if input was None.

        Raises:
            TypeFieldError: If input is not convertible to integer.
            IntegerConversionError: If value cannot be converted to integer.
        """
        if v is None:
            return None
        if not isinstance(v, int | str | float):  # Allow int, or str/float that can be int
            field_name = str(info.field_name)
            raise TypeFieldError(
                field_name=field_name,
                expected_type="integer or convertible to one",
                actual_type=type(v).__name__,
            )
        try:
            return int(v)
            # Positivity (gt=0) is handled by Field constraint
        except ValueError as e:
            field_name = str(info.field_name)
            raise IntegerConversionError(field_name=field_name, value=v) from e

    @model_validator(mode="after")
    def check_time_range(self) -> "GetOrderHistoryArgs":
        """Validate time range logic.

        Returns:
            Self for method chaining.

        Raises:
            TimeRangeError: If start_time is greater than or equal to end_time.
        """
        if self.start_time and self.end_time and self.start_time >= self.end_time:
            raise TimeRangeError(
                start_field="start_time",
                end_field="end_time",
                start_value=self.start_time,
                end_value=self.end_time,
            )
        return self


class GetTradeHistoryArgs(BaseModel):
    """Encapsulates arguments for fetching trade history (fills).

    This model centralizes validation for trade history requests, ensuring consistent
    handling of optional symbol filters and limit constraints.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: Symbol | None = Field(default=None)
    limit: int | None = Field(default=100, gt=0)  # Default matches BackpackAPI

    @field_validator("limit", mode="before")
    @classmethod
    def parse_optional_positive_int(cls, v: object, info: ValidationInfo) -> int | None:
        """Parse optional positive integer fields.

        Returns:
            Parsed positive integer or None if input was None.

        Raises:
            TypeFieldError: If input is not convertible to integer.
            IntegerConversionError: If value cannot be converted to integer.
        """
        if v is None:
            return None
        if not isinstance(v, int | str | float):
            field_name = str(info.field_name)
            raise TypeFieldError(
                field_name=field_name,
                expected_type="integer or convertible",
                actual_type=type(v).__name__,
            )
        try:
            return int(v)
            # Positivity (gt=0) is handled by Field constraint.
        except ValueError as e:
            field_name = str(info.field_name)
            raise IntegerConversionError(field_name=field_name, value=v) from e
