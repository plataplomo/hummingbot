"""Common service argument models for internal service layer interfaces.

This module contains generic Pydantic models that encapsulate arguments for various service methods,
centralizing validation logic and improving API clarity across all exchanges.

Exception Import Pattern:
Service args models are a special case that legitimately need both API and core exceptions:
- API exceptions (cyberdelta.apis.exceptions/) for basic validations (EmptyStringFieldError,
  TypeFieldError)
- Core exceptions (cyberdelta.exceptions/) for rich validation features (DecimalFieldError with
  constraints, RequiredFieldError with context, service validation exceptions)

This is because service args models bridge between API layer and business logic layer, performing
application-level validation beyond simple API concerns.
"""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.apis.base.trading_execution_domain import LiquidityRequirement, OrderExecution
from cyberdelta.apis.exceptions.field_validation import (
    EmptyStringFieldError,
    TypeFieldError,
)
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.exceptions.field_validation import (
    DecimalFieldError,
    RequiredFieldError,
)
from cyberdelta.exceptions.service_validation import (
    IntegerConversionError,
    MissingPriceError,
    MissingStopPriceError,
    NegativeValueError,
    PostOnlyLimitError,
    TimeRangeError,
    TransferAccountError,
)
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.typing import PotentialDecimalInput, is_potential_decimal_input


def validate_api_str_field(
    value: object,
    *,
    field_name: str,
    max_length: int | None = None,
    allow_empty: bool = True,
) -> str:
    """Validate string field using API-specific exceptions.

    Args:
        value: Value to validate
        field_name: Name of the field being validated
        max_length: Maximum allowed length
        allow_empty: Whether empty strings are allowed

    Returns:
        Validated string value

    Raises:
        TypeFieldError: If value is not a string or exceeds max_length
        EmptyStringFieldError: If value is empty and allow_empty is False
    """
    if not isinstance(value, str):
        raise TypeFieldError(
            field_name=field_name,
            expected_type="str",
            actual_type=type(value).__name__,
        )

    if not allow_empty and not value.strip():
        raise EmptyStringFieldError(field_name=field_name)

    if max_length is not None and len(value) > max_length:
        raise TypeFieldError(
            field_name=field_name,
            expected_type=f"string with max length {max_length}",
            actual_type=f"string with length {len(value)}",
        )

    try:
        value.encode("utf-8", "strict")
    except UnicodeEncodeError as e:
        raise TypeFieldError(
            field_name=field_name,
            expected_type="valid UTF-8 string",
            actual_type="string with invalid UTF-8",
        ) from e

    return value


class PlaceOrderArgs(BaseModel):
    """Encapsulates all arguments for placing an order.

    This model centralizes input validation for order placement across all exchanges,
    including type checks, value constraints, and inter-parameter dependencies.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal = Field(gt=Decimal(0))
    time_in_force: TimeInForce
    price: Decimal | None = Field(default=None, gt=Decimal(0))
    stop_price: Decimal | None = Field(default=None, gt=Decimal(0))
    client_order_id: str | None = Field(default=None)
    execution: OrderExecution = Field(default_factory=OrderExecution)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol_str(cls, v: str, info: ValidationInfo) -> str:
        """Validate symbol is a non-empty string with max length 64."""
        # field_name is guaranteed by Pydantic to be correct here.
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("client_order_id", mode="before")
    @classmethod
    def validate_client_order_id_str(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate client_order_id is None or a non-empty string with max length 64."""
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
        """Parse decimal fields and ensure they are finite."""
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
        """Validate inter-parameter dependencies."""
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


class TransferArgs(BaseModel):
    """Encapsulates arguments for internal fund transfers between account types within an exchange.

    This model centralizes validation for transfer operations, ensuring consistent
    handling of asset, amount, and account type parameters.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    asset: str
    amount: Decimal = Field(gt=Decimal(0))
    from_account_type: str  # Specific validation might depend on exchange
    to_account_type: str  # Specific validation might depend on exchange
    client_transfer_id: str | None = Field(default=None)

    @field_validator("asset", "from_account_type", "to_account_type", mode="before")
    @classmethod
    def validate_required_strings(cls, v: str, info: ValidationInfo) -> str:
        """Validate required string fields are non-empty with max length 64."""
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("client_transfer_id", mode="before")
    @classmethod
    def validate_optional_string(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate optional string fields."""
        if v is None:
            return None
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=128,
            allow_empty=False,
        )

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount_decimal(cls, v: PotentialDecimalInput, info: ValidationInfo) -> Decimal:
        """Parse and validate amount as a positive finite decimal."""
        field_name = str(info.field_name)

        # Use TypeGuard for better type safety
        if not is_potential_decimal_input(v):
            raise TypeFieldError(
                field_name=field_name,
                expected_type="string, int, float, or Decimal",
                actual_type=type(v).__name__,
            )

        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        if not parsed.is_finite():
            raise DecimalFieldError(
                field_name=field_name,
                value=v,
                reason="must be a finite decimal",
            )
        # Positivity (gt=0) is handled by Field constraint.
        return parsed

    @model_validator(mode="after")
    def check_account_types_differ(self) -> "TransferArgs":
        """Ensure from and to account types are different."""
        if self.from_account_type == self.to_account_type:
            raise TransferAccountError(
                from_account=self.from_account_type,
                to_account=self.to_account_type,
            )
        # Note: Exchange-specific validation for from/to_account_type values would ideally
        # be handled by derived Args models or within the service implementation.
        return self


class WithdrawArgs(BaseModel):
    """Encapsulates arguments for fund withdrawals.

    This model handles withdrawal parameters including asset, amount, address, network,
    and optional tags or IDs, with support for exchange-specific extra parameters.
    """

    model_config = ConfigDict(extra="allow", validate_assignment=True)  # extra="allow" for **kwargs

    asset: str
    amount: Decimal = Field(gt=Decimal(0))
    address: str
    network: str | None = Field(default=None)  # Optional for some exchanges
    tag: str | None = Field(default=None)  # e.g., memo for XRP, destination tag for others
    client_withdrawal_id: str | None = Field(default=None)
    two_factor_token: str | None = Field(default=None)  # If 2FA is handled at this level

    @field_validator("asset", "address", mode="before")
    @classmethod
    def validate_required_strings(cls, v: str, info: ValidationInfo) -> str:
        """Validate required string fields."""
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=128,
            allow_empty=False,
        )

    @field_validator("network", "tag", "client_withdrawal_id", "two_factor_token", mode="before")
    @classmethod
    def validate_optional_strings(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate optional string fields."""
        if v is None:
            return None
        # Shorter max_length for network/tag unless specific exchanges require longer
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("amount", mode="before")
    @classmethod
    def parse_amount_decimal(cls, v: PotentialDecimalInput, info: ValidationInfo) -> Decimal:
        """Parse and validate amount as a positive finite decimal."""
        field_name = str(info.field_name)

        # Use TypeGuard for better type safety
        if not is_potential_decimal_input(v):
            raise TypeFieldError(
                field_name=field_name,
                expected_type="string, int, float, or Decimal",
                actual_type=type(v).__name__,
            )

        parsed = parse_decimal_value(v, field_name=field_name, allow_none=False)
        if not parsed.is_finite():
            raise DecimalFieldError(
                field_name=field_name,
                value=v,
                reason="must be a finite decimal",
            )
        # Positivity (gt=0) is handled by Field constraint.
        return parsed

    # No model_validator needed for basic WithdrawArgs unless inter-dependencies
    # are identified that are universal. Exchange-specific checks would go into
    # the service method or a derived model.


class GetOrderHistoryArgs(BaseModel):
    """Encapsulates arguments for fetching order history.

    This model centralizes validation for order history requests, including
    time range validation, positive limit constraints, and string field validation.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str | None = Field(default=None)
    start_time: datetime | None = Field(default=None)
    end_time: datetime | None = Field(default=None)
    limit: int | None = Field(default=None, gt=0)  # Limit must be positive if provided
    order_id: str | None = Field(default=None)
    client_order_id: str | None = Field(default=None)

    @field_validator("symbol", "order_id", "client_order_id", mode="before")
    @classmethod
    def validate_optional_strings(
        cls,
        v: str | float | None,
        info: ValidationInfo,
    ) -> str | None:
        """Validate optional string fields are non-empty with reasonable max length."""
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
        """Parse optional datetime fields to UTC."""
        if v is None:
            return None
        # parse_datetime_utc will return None if parsing fails, which is acceptable
        # for optional fields
        return parse_datetime_utc(v, field_name=str(info.field_name))

    @field_validator("limit", mode="before")
    @classmethod
    def parse_optional_int(cls, v: object, info: ValidationInfo) -> int | None:
        """Parse optional integer fields."""
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
        """Validate time range logic."""
        if self.start_time and self.end_time and self.start_time >= self.end_time:
            raise TimeRangeError(
                start_field="start_time",
                end_field="end_time",
                start_value=self.start_time,
                end_value=self.end_time,
            )
        return self


class GetMarketDataArgs(BaseModel):
    """Encapsulates arguments for fetching market data (candlesticks/OHLCV).

    This model centralizes validation for market data requests, including
    symbol validation, timeframe validation, and time range constraints.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str
    timeframe: str
    limit: int = Field(default=100, gt=0)  # Limit must be positive
    start_time_ms: int | None = Field(default=None)
    end_time_ms: int | None = Field(default=None)

    @field_validator("symbol", "timeframe", mode="before")
    @classmethod
    def validate_required_strings(cls, v: object, info: ValidationInfo) -> str:
        """Validate required string fields are non-empty with reasonable max length."""
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("limit", mode="before")
    @classmethod
    def parse_limit_int(cls, v: object, info: ValidationInfo) -> int:
        """Parse limit field as positive integer."""
        if not isinstance(v, int | str | float):
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

    @field_validator("start_time_ms", "end_time_ms", mode="before")
    @classmethod
    def parse_optional_timestamp_ms(cls, v: object, info: ValidationInfo) -> int | None:
        """Parse optional timestamp milliseconds fields."""
        if v is None:
            return None
        if not isinstance(v, int | str | float):
            field_name = str(info.field_name)
            raise TypeFieldError(
                field_name=field_name,
                expected_type="integer or convertible to one",
                actual_type=type(v).__name__,
            )
        try:
            int_val = int(v)
            if int_val < 0:
                field_name = str(info.field_name)
                raise NegativeValueError(field_name=field_name, value=int_val)
        except ValueError as e:
            field_name = str(info.field_name)
            raise IntegerConversionError(field_name=field_name, value=v) from e
        else:
            return int_val

    @model_validator(mode="after")
    def check_time_range(self) -> "GetMarketDataArgs":
        """Validate time range logic."""
        if (
            self.start_time_ms is not None
            and self.end_time_ms is not None
            and self.start_time_ms >= self.end_time_ms
        ):
            raise TimeRangeError(
                start_field="start_time_ms",
                end_field="end_time_ms",
                start_value=self.start_time_ms,
                end_value=self.end_time_ms,
            )
        return self


class CancelOrderArgs(BaseModel):
    """Encapsulates arguments for cancelling an order.

    This model centralizes input validation for order cancellation across all exchanges,
    ensuring order_id is always a valid non-empty string and handling optional parameters
    like symbol and client_order_id gracefully with validation.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    order_id: str  # Usually the exchange-generated order ID
    symbol: str | None = Field(default=None)  # Often required by exchanges
    client_order_id: str | None = Field(default=None)  # Alternative identifier

    @field_validator("order_id", "symbol", "client_order_id", mode="before")
    @classmethod
    def validate_strings(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate string fields with appropriate requirements."""
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
        """
        if self.symbol is None:
            # Depending on exchange specifics, this might be an error for some.
            # For now, allow symbol to be optional in the generic model.
            # The service/builder for a specific exchange will enforce if it's needed.
            pass
        return self


class GetFundingRatesArgs(BaseModel):
    """Encapsulates arguments for fetching funding rates.

    This model centralizes validation for funding rate requests, ensuring that if symbols
    are provided, it's a list of valid, non-empty strings, and that the list itself is
    not empty if provided.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbols: list[str] | None = Field(default=None)  # List of symbols, or None for all

    @field_validator("symbols", mode="before")
    @classmethod
    def validate_symbols_list(cls, v: list[str] | None, info: ValidationInfo) -> list[str] | None:
        """Validate symbols list contains valid non-empty strings."""
        if v is None:
            return None  # Allowed

        # v is already typed as list[str] so no isinstance check needed
        if not v:  # Empty list is passed through, service must decide if "all" or error
            return []

        validated_symbols: list[str] = []
        for i, item in enumerate(v):
            # Ensure item is a non-empty string
            item_str = validate_api_str_field(
                str(item),
                field_name=f"{info.field_name!s}[{i}]",
                max_length=64,
                allow_empty=False,
            )
            validated_symbols.append(item_str)
        return validated_symbols


class GetTradeHistoryArgs(BaseModel):
    """Encapsulates arguments for fetching trade history (fills).

    This model centralizes validation for trade history requests, ensuring consistent
    handling of optional symbol filters and limit constraints.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str | None = Field(default=None)
    limit: int | None = Field(default=100, gt=0)  # Default matches BackpackAPI

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_optional_strings(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate optional string fields."""
        if v is None:
            return None
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("limit", mode="before")
    @classmethod
    def parse_optional_positive_int(cls, v: object, info: ValidationInfo) -> int | None:
        """Parse optional positive integer fields."""
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


class GetAllOpenOrdersArgs(BaseModel):
    """Encapsulates arguments for fetching all open orders.

    This model centralizes validation for open orders requests, ensuring
    the optional symbol filter is correctly validated if provided.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str | None = Field(default=None)  # Optional symbol to filter by

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_optional_symbol(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate optional symbol field."""
        if v is None:
            return None
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )


class GetOrderArgs(BaseModel):
    """Encapsulates arguments for fetching a specific order.

    This model centralizes validation for fetching order details, ensuring consistent
    handling of order_id (primary identifier) and optional parameters like symbol
    and client_order_id which may be required by some exchanges.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    order_id: str  # Primary identifier, usually exchange-generated
    symbol: str | None = Field(default=None)  # Often required or recommended by exchanges
    client_order_id: str | None = Field(default=None)  # Alternative identifier

    @field_validator("order_id", "symbol", "client_order_id", mode="before")
    @classmethod
    def validate_strings(cls, v: object, info: ValidationInfo) -> str | None:
        """Validate string fields with appropriate requirements."""
        field_name = str(info.field_name)
        is_required = field_name == "order_id"

        if v is None:
            if is_required:
                raise RequiredFieldError(field_name=field_name, context="order query")
            return None  # For optional fields

        # Max length for order_id can be quite long for some exchanges (e.g. UUIDs)
        max_len = 128 if field_name in {"order_id", "client_order_id"} else 64
        return validate_api_str_field(
            v, field_name=field_name, max_length=max_len, allow_empty=False
        )

    @model_validator(mode="after")
    def check_identifier_logic(self) -> "GetOrderArgs":
        """Validate identifier logic.

        While order_id is primary, some exchanges might heavily rely on symbol.
        Backpack requires symbol for its GET /order/{id} endpoint as a query param.
        Hyperliquid's orderStatus needs user + oid, symbol is not directly part of request.
        For a generic model, ensuring order_id is primary.
        If symbol becomes strictly required for all exchanges, it can be made non-optional.
        """
        if self.symbol is None:
            # Log a debug message or warning if symbol is often needed but not provided.
            pass
        return self


# Alias GetOrderStatusArgs to GetOrderArgs since they have identical fields
GetOrderStatusArgs = GetOrderArgs  # Alias for clarity in signatures


class GetHistoricalFundingRatesArgs(BaseModel):
    """Encapsulates arguments for fetching historical funding rates.

    This model provides structured access to funding rate history with optional
    time range filtering and limit constraints.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str  # Symbol is required for this endpoint on Backpack
    start_time: datetime | None = Field(default=None)
    end_time: datetime | None = Field(default=None)
    limit: int | None = Field(default=None, gt=0)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol_str(cls, v: object, info: ValidationInfo) -> str:
        """Validate symbol is a non-empty string with max length."""
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
        v: datetime | float | str | None,
        info: ValidationInfo,
    ) -> datetime | None:
        """Parse optional datetime fields to UTC."""
        if v is None:
            return None
        return parse_datetime_utc(v, field_name=str(info.field_name))

    @field_validator("limit", mode="before")
    @classmethod
    def parse_optional_positive_int(cls, v: object, info: ValidationInfo) -> int | None:
        """Parse optional positive integer fields."""
        if v is None:
            return None
        if not isinstance(v, int | str | float):
            raise TypeFieldError(
                field_name=str(info.field_name),
                expected_type="integer or convertible",
                actual_type=type(v).__name__,
            )
        try:
            return int(v)
        except ValueError as e:
            raise IntegerConversionError(field_name=str(info.field_name), value=v) from e

    @model_validator(mode="after")
    def check_time_range_logic(self) -> "GetHistoricalFundingRatesArgs":
        """Validate time range logic."""
        if self.start_time and self.end_time and self.start_time >= self.end_time:
            raise TimeRangeError(
                start_field="start_time",
                end_field="end_time",
                start_value=self.start_time,
                end_value=self.end_time,
            )
        return self


class GetMarketArgs(BaseModel):
    """Encapsulates arguments for fetching a specific market's metadata.

    This model centralizes validation for fetching market configuration including
    tick sizes, step sizes, trading limits, and other market-specific rules.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol_str(cls, v: str, info: ValidationInfo) -> str:
        """Validate symbol is a non-empty string with max length 64."""
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )


class GetMarketsArgs(BaseModel):
    """Encapsulates arguments for fetching all available markets metadata.

    This model provides a consistent interface for fetching market configuration
    for all tradable symbols, even though most implementations require no parameters.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    # Currently no parameters needed, but model provides consistency and future extensibility
    # Could potentially add filters like market_type, status, etc. in the future


class GetTickerArgs(BaseModel):
    """Encapsulates arguments for fetching ticker data for a specific symbol.

    This model centralizes validation for ticker requests, ensuring consistent
    handling of symbol parameters across all exchanges.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol_str(cls, v: str, info: ValidationInfo) -> str:
        """Validate symbol is a non-empty string with max length 64."""
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )


class GetOrderBookArgs(BaseModel):
    """Encapsulates arguments for fetching order book data for a specific symbol.

    This model centralizes validation for order book requests, ensuring consistent
    handling of symbol and optional depth parameters across all exchanges.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str
    depth: int | None = Field(default=None, gt=0)
    limit: int | None = Field(default=None, gt=0)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol_str(cls, v: str, info: ValidationInfo) -> str:
        """Validate symbol is a non-empty string with max length 64."""
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("depth", "limit", mode="before")
    @classmethod
    def parse_optional_depth_int(cls, v: object, info: ValidationInfo) -> int | None:
        """Parse optional depth field as positive integer."""
        if v is None:
            return None
        if not isinstance(v, int | str | float):
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


class GetAllMidsArgs(BaseModel):
    """Encapsulates arguments for fetching mid prices for all available symbols.

    This model provides a consistent interface for fetching mid prices across
    all symbols, even though most implementations require no parameters.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    # Currently no parameters needed, but model provides consistency and future extensibility
    # Could potentially add filters like market_type, active_only, etc. in the future


# --- Account Limits Args Models (INTERNAL USE ONLY) ---


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


class UpdateAccountSettingsArgs(BaseModel):
    """Arguments for updating account settings."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    auto_borrow_settlements: bool | None = Field(default=None)
    auto_lend: bool | None = Field(default=None)
    auto_realize_pnl: bool | None = Field(default=None)
    auto_repay_borrows: bool | None = Field(default=None)
    leverage_limit: Decimal | None = Field(default=None, gt=Decimal(0))


# --- Additional Args Models for RequestBuilder Architecture Compliance ---


class GetRecentTradesArgs(BaseModel):
    """Arguments for fetching recent public trades."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(..., min_length=1, max_length=64)
    limit: int | None = Field(default=100, gt=0)


class CancelAllOrdersArgs(BaseModel):
    """Arguments for canceling all orders.

    This model handles cancellation of all open orders for a symbol or all symbols,
    with optional filtering by order side.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str | None = Field(default=None, max_length=64)
    side: OrderSide | None = Field(default=None)

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol_str(cls, v: str | None, info: ValidationInfo) -> str | None:
        """Validate symbol is None or a non-empty string with max length 64."""
        if v is None:
            return None
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )


class GetL2BookArgs(BaseModel):
    """Arguments for fetching L2 order book data."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(..., min_length=1, max_length=64)
