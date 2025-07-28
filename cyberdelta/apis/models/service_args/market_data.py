"""Market data service argument models.

This module contains Pydantic models for market data queries including tickers,
order books, funding rates, and historical data.
"""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from cyberdelta.apis.exceptions.field_validation import TypeFieldError
from cyberdelta.apis.models.service_args.common import validate_api_str_field
from cyberdelta.exceptions.service_validation import (
    IntegerConversionError,
    NegativeValueError,
    TimeRangeError,
)
from cyberdelta.utils.parsing import parse_datetime_utc


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
        """Validate required string fields are non-empty with reasonable max length.
        
        Returns:
            Validated string value.
        """
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("limit", mode="before")
    @classmethod
    def parse_limit_int(cls, v: object, info: ValidationInfo) -> int:
        """Parse limit field as positive integer.
        
        Returns:
            Parsed integer value.
            
        Raises:
            TypeFieldError: If input is not convertible to integer.
            IntegerConversionError: If value cannot be converted to integer.
        """
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
        """Parse optional timestamp milliseconds fields.
        
        Returns:
            Parsed integer timestamp or None if input was None.
            
        Raises:
            TypeFieldError: If input is not convertible to integer.
            IntegerConversionError: If value cannot be converted to integer.
            NegativeValueError: If converted value is negative.
        """
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
        """Validate time range logic.
        
        Returns:
            Self for method chaining.
            
        Raises:
            TimeRangeError: If start_time_ms is greater than or equal to end_time_ms.
        """
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
        """Validate symbols list contains valid non-empty strings.
        
        Returns:
            Validated list of symbol strings or None if input was None.
        """
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
        """Validate symbol is a non-empty string with max length.
        
        Returns:
            Validated symbol string.
        """
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
        """Parse optional datetime fields to UTC.
        
        Returns:
            Parsed UTC datetime or None if input was None.
        """
        if v is None:
            return None
        return parse_datetime_utc(v, field_name=str(info.field_name))

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
        """Validate symbol is a non-empty string with max length 64.
        
        Returns:
            Validated symbol string.
        """
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
        """Validate symbol is a non-empty string with max length 64.
        
        Returns:
            Validated symbol string.
        """
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
        """Validate symbol is a non-empty string with max length 64.
        
        Returns:
            Validated symbol string.
        """
        return validate_api_str_field(
            v,
            field_name=str(info.field_name),
            max_length=64,
            allow_empty=False,
        )

    @field_validator("depth", "limit", mode="before")
    @classmethod
    def parse_optional_depth_int(cls, v: object, info: ValidationInfo) -> int | None:
        """Parse optional depth field as positive integer.
        
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


class GetL2BookArgs(BaseModel):
    """Arguments for fetching L2 order book data."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(..., min_length=1, max_length=64)


class GetRecentTradesArgs(BaseModel):
    """Arguments for fetching recent public trades."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: str = Field(..., min_length=1, max_length=64)
    limit: int | None = Field(default=100, gt=0)
