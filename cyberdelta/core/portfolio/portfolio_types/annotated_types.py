"""Annotated types providing reusable type constraints and validation."""

from __future__ import annotations

import re
import time
from decimal import Decimal
from typing import Annotated, Any

from pydantic import Field, StringConstraints
from pydantic.functional_validators import AfterValidator

from cyberdelta.core.portfolio.exceptions import (
    EmptyAssetError,
    EmptyListError,
    EmptySymbolError,
    InvalidAssetCharactersError,
    InvalidAssetLengthError,
    InvalidCacheSizeError,
    InvalidExchangeError,
    InvalidMarketHoursFormatError,
    InvalidSymbolCharactersError,
    InvalidSymbolLengthError,
    InvalidTimestampRangeError,
    InvalidTTLError,
    InvalidURLFormatError,
    NonUniqueListError,
    PositionSizeTooLargeError,
)


# Constants for validation
SYMBOL_MIN_LENGTH = 2
SYMBOL_MAX_LENGTH = 20
ASSET_MIN_LENGTH = 1
ASSET_MAX_LENGTH = 10
MAX_POSITION_SIZE = 1_000_000_000  # 1 billion
MIN_CACHE_SIZE = 10
MAX_CACHE_SIZE = 1_000_000
MIN_TTL_SECONDS = 1
MAX_TTL_SECONDS = 86400 * 30  # 30 days
MIN_TIMESTAMP = 1577836800  # 2020-01-01
TIMESTAMP_FUTURE_YEARS = 100


# Numeric constraints
type PositiveFloat = Annotated[float, Field(gt=0, description="Must be a positive number")]

type NonNegativeFloat = Annotated[float, Field(ge=0, description="Must be non-negative")]

type PositiveInt = Annotated[int, Field(gt=0, description="Must be a positive integer")]

type NonNegativeInt = Annotated[int, Field(ge=0, description="Must be non-negative integer")]

type PositiveDecimal = Annotated[
    Decimal, Field(gt=Decimal(0), description="Must be a positive decimal")
]

type NonNegativeDecimal = Annotated[
    Decimal, Field(ge=Decimal(0), description="Must be non-negative decimal")
]

type Percentage = Annotated[
    float, Field(ge=0, le=100, description="Percentage value between 0 and 100")
]

type Ratio = Annotated[float, Field(ge=0, le=1, description="Ratio value between 0 and 1")]

type BasisPoints = Annotated[int, Field(ge=0, le=10000, description="Basis points (0-10000)")]


# Financial constraints
type Price = Annotated[
    Decimal,
    Field(gt=Decimal(0), decimal_places=8, description="Trading price with up to 8 decimal places"),
]

type Quantity = Annotated[
    Decimal,
    Field(
        gt=Decimal(0), decimal_places=8, description="Trading quantity with up to 8 decimal places"
    ),
]

type Volume = Annotated[
    Decimal,
    Field(
        ge=Decimal(0), decimal_places=8, description="Trading volume with up to 8 decimal places"
    ),
]

type PnL = Annotated[
    Decimal, Field(decimal_places=8, description="Profit and Loss with up to 8 decimal places")
]

type Leverage = Annotated[
    float, Field(gt=0, le=1000, description="Leverage multiplier (1x to 1000x)")
]

type MarginRatio = Annotated[
    float, Field(ge=0, le=1, description="Margin ratio as decimal (0.0 to 1.0)")
]


# String constraints
def validate_symbol(value: str) -> str:
    """Validate trading symbol format.

    Returns:
        str: The validated symbol in uppercase format.

    Raises:
        EmptySymbolError: If the symbol is empty or None.
        InvalidSymbolLengthError: If symbol length is outside allowed range.
        InvalidSymbolCharactersError: If symbol contains invalid characters.
    """
    if not value:
        raise EmptySymbolError
    if len(value) < SYMBOL_MIN_LENGTH or len(value) > SYMBOL_MAX_LENGTH:
        raise InvalidSymbolLengthError(value)
    if not value.replace("-", "").replace("/", "").replace("_", "").isalnum():
        raise InvalidSymbolCharactersError(value)
    return value.upper()


def validate_asset(value: str) -> str:
    """Validate asset name format.

    Returns:
        str: The validated asset name in uppercase format.

    Raises:
        EmptyAssetError: If the asset name is empty or None.
        InvalidAssetLengthError: If asset name length is outside allowed range.
        InvalidAssetCharactersError: If asset name contains invalid characters.
    """
    if not value:
        raise EmptyAssetError
    if len(value) < ASSET_MIN_LENGTH or len(value) > ASSET_MAX_LENGTH:
        raise InvalidAssetLengthError(value)
    if not value.isalnum():
        raise InvalidAssetCharactersError(value)
    return value.upper()


def validate_exchange_name(value: str) -> str:
    """Validate exchange name.

    Returns:
        str: The validated exchange name in lowercase format.

    Raises:
        InvalidExchangeError: If exchange name is not in the allowed list.
    """
    valid_exchanges = {"hyperliquid", "backpack"}
    if value.lower() not in valid_exchanges:
        raise InvalidExchangeError(value, valid_exchanges)
    return value.lower()


type TradingSymbol = Annotated[
    str,
    AfterValidator(validate_symbol),
    StringConstraints(min_length=2, max_length=20, strip_whitespace=True),
    Field(description="Trading symbol (e.g., BTC-USD, ETH/USDT)"),
]

type AssetName = Annotated[
    str,
    AfterValidator(validate_asset),
    StringConstraints(min_length=1, max_length=10, strip_whitespace=True),
    Field(description="Asset name (e.g., BTC, ETH, USDT)"),
]

type ExchangeName = Annotated[
    str,
    AfterValidator(validate_exchange_name),
    StringConstraints(min_length=3, max_length=20, strip_whitespace=True),
    Field(description="Exchange name (hyperliquid, backpack)"),
]

type OrderId = Annotated[
    str,
    StringConstraints(min_length=1, max_length=100, strip_whitespace=True),
    Field(description="Unique order identifier"),
]

type TradeId = Annotated[
    str,
    StringConstraints(min_length=1, max_length=100, strip_whitespace=True),
    Field(description="Unique trade identifier"),
]

type WalletAddress = Annotated[
    str,
    StringConstraints(pattern=r"^0x[a-fA-F0-9]{40}$", strip_whitespace=True),
    Field(description="Ethereum wallet address (0x followed by 40 hex characters)"),
]

type Username = Annotated[
    str,
    StringConstraints(
        min_length=3, max_length=50, pattern=r"^[a-zA-Z0-9_-]+$", strip_whitespace=True
    ),
    Field(description="Username (alphanumeric, underscore, dash)"),
]


# Time constraints
def validate_timestamp(value: float) -> float:
    """Validate timestamp is reasonable.

    Returns:
        float: The validated timestamp value.

    Raises:
        InvalidTimestampRangeError: If timestamp is outside the allowed range.
    """
    current_time = time.time()
    # Allow timestamps from 2020 to 100 years in the future
    min_timestamp = MIN_TIMESTAMP
    max_timestamp = current_time + (TIMESTAMP_FUTURE_YEARS * 365 * 24 * 60 * 60)

    if value < min_timestamp or value > max_timestamp:
        raise InvalidTimestampRangeError(value)
    return value


type Timestamp = Annotated[
    float,
    AfterValidator(validate_timestamp),
    Field(description="Unix timestamp (seconds since epoch)"),
]

type TimestampMs = Annotated[
    int, Field(gt=1577836800000, description="Unix timestamp in milliseconds")
]

type Duration = Annotated[float, Field(ge=0, description="Duration in seconds")]

type DurationMs = Annotated[int, Field(ge=0, description="Duration in milliseconds")]


# Collection constraints
def validate_non_empty_list(value: list[Any]) -> list[Any]:
    """Validate that a list is not empty.

    Returns:
        list[Any]: The validated non-empty list.

    Raises:
        EmptyListError: If the list is empty.
    """
    if not value:
        raise EmptyListError
    return value


def validate_unique_list(value: list[Any]) -> list[Any]:
    """Validate that a list contains unique elements.

    Returns:
        list[Any]: The validated list with unique elements.

    Raises:
        NonUniqueListError: If the list contains duplicate elements.
    """
    if len(value) != len(set(value)):
        raise NonUniqueListError(value)
    return value


type NonEmptyList = Annotated[
    list[Any],
    AfterValidator(validate_non_empty_list),
    Field(min_length=1, description="Non-empty list"),
]

type UniqueList = Annotated[
    list[Any], AfterValidator(validate_unique_list), Field(description="List with unique elements")
]

type LimitedList = Annotated[
    list[Any], Field(max_length=1000, description="List with maximum 1000 items")
]


# Portfolio-specific constraints
def validate_position_size(value: float) -> float:
    """Validate position size (can be negative for short positions).

    Returns:
        float: The validated position size value.

    Raises:
        PositionSizeTooLargeError: If position size exceeds maximum allowed value.
    """
    if abs(value) > MAX_POSITION_SIZE:
        raise PositionSizeTooLargeError(value)
    return value


type PositionSize = Annotated[
    float,
    AfterValidator(validate_position_size),
    Field(description="Position size (positive for long, negative for short)"),
]

type BalanceAmount = Annotated[
    Decimal, Field(ge=Decimal(0), decimal_places=8, description="Balance amount (non-negative)")
]

type TradingFee = Annotated[
    Decimal,
    Field(
        ge=Decimal(0),
        le=Decimal(1),
        decimal_places=6,
        description="Trading fee as decimal (0.0-1.0)",
    ),
]

type FundingRate = Annotated[
    Decimal,
    Field(
        ge=Decimal(-1), le=Decimal(1), decimal_places=8, description="Funding rate (-1.0 to 1.0)"
    ),
]


# Error and validation constraints
type ErrorCode = Annotated[
    str,
    StringConstraints(
        min_length=3, max_length=50, pattern=r"^[A-Z][A-Z0-9_]*$", strip_whitespace=True
    ),
    Field(description="Error code (uppercase, alphanumeric with underscores)"),
]

type ErrorMessage = Annotated[
    str,
    StringConstraints(min_length=1, max_length=500, strip_whitespace=True),
    Field(description="Human-readable error message"),
]

type LogLevel = Annotated[
    str, Field(pattern=r"^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$", description="Log level")
]


# Configuration constraints
def validate_cache_size(value: int) -> int:
    """Validate cache size is reasonable.

    Returns:
        int: The validated cache size value.

    Raises:
        InvalidCacheSizeError: If cache size is outside the allowed range.
    """
    if value < MIN_CACHE_SIZE or value > MAX_CACHE_SIZE:
        raise InvalidCacheSizeError(value)
    return value


def validate_ttl_seconds(value: float) -> float:
    """Validate TTL is reasonable.

    Returns:
        float: The validated TTL value in seconds.

    Raises:
        InvalidTTLError: If TTL is outside the allowed range.
    """
    if value < MIN_TTL_SECONDS or value > MAX_TTL_SECONDS:
        raise InvalidTTLError(value)
    return value


type CacheSize = Annotated[
    int, AfterValidator(validate_cache_size), Field(description="Cache size (10 to 1,000,000)")
]

type TTLSeconds = Annotated[
    float,
    AfterValidator(validate_ttl_seconds),
    Field(description="Time to live in seconds (1 second to 30 days)"),
]

type RetryCount = Annotated[int, Field(ge=0, le=10, description="Retry count (0-10)")]

type TimeoutSeconds = Annotated[
    float, Field(gt=0, le=300, description="Timeout in seconds (max 5 minutes)")
]


# Version and identifier constraints
type Version = Annotated[int, Field(ge=0, description="Version number (non-negative)")]

type UUID = Annotated[
    str,
    StringConstraints(
        pattern=r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
        strip_whitespace=True,
    ),
    Field(description="UUID format"),
]


# Network and URL constraints
def validate_url(value: str) -> str:
    """Validate URL format.

    Returns:
        str: The validated URL string.

    Raises:
        InvalidURLFormatError: If URL format is invalid or missing required protocol.
    """
    if not value.startswith(("http://", "https://", "ws://", "wss://")):
        raise InvalidURLFormatError(value)
    return value


type URL = Annotated[
    str,
    AfterValidator(validate_url),
    StringConstraints(min_length=10, max_length=2000, strip_whitespace=True),
    Field(description="Valid URL"),
]

type IPAddress = Annotated[
    str,
    StringConstraints(
        pattern=r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
        strip_whitespace=True,
    ),
    Field(description="IPv4 address"),
]

type Port = Annotated[int, Field(ge=1, le=65535, description="Network port (1-65535)")]


# Business logic constraints
def validate_market_hours(value: str) -> str:
    """Validate market hours format (HH:MM-HH:MM).

    Returns:
        str: The validated market hours string.

    Raises:
        InvalidMarketHoursFormatError: If market hours format is invalid.
    """
    pattern = r"^([01]?[0-9]|2[0-3]):[0-5][0-9]-([01]?[0-9]|2[0-3]):[0-5][0-9]$"
    if not re.match(pattern, value):
        raise InvalidMarketHoursFormatError(value)
    return value


type MarketHours = Annotated[
    str,
    AfterValidator(validate_market_hours),
    Field(description="Market hours in HH:MM-HH:MM format"),
]

type Currency = Annotated[
    str,
    StringConstraints(min_length=3, max_length=3, pattern=r"^[A-Z]{3}$", strip_whitespace=True),
    Field(description="Three-letter currency code (ISO 4217)"),
]

type CountryCode = Annotated[
    str,
    StringConstraints(min_length=2, max_length=2, pattern=r"^[A-Z]{2}$", strip_whitespace=True),
    Field(description="Two-letter country code (ISO 3166-1)"),
]


# Type aliases for common combinations
SymbolPrice = tuple[TradingSymbol, Price]
AssetBalance = tuple[AssetName, BalanceAmount]
Symbol = tuple[ExchangeName, TradingSymbol]
TimestampedValue = tuple[Timestamp, Any]


# Factory functions for creating constrained types
def create_bounded_float(
    min_val: float | None = None,
    max_val: float | None = None,
    description: str = "Bounded float value",
) -> type[float]:
    """Create a float type with custom bounds.

    Returns:
        type[float]: The base float type (annotated types cannot be returned from functions).
    """
    constraints = {}
    if min_val is not None:
        constraints["ge"] = min_val
    if max_val is not None:
        constraints["le"] = max_val

    # Note: Can't return annotated types from functions, return base type
    return float


def create_bounded_int(
    min_val: int | None = None,
    max_val: int | None = None,
    description: str = "Bounded integer value",
) -> type[int]:
    """Create an int type with custom bounds.

    Returns:
        type[int]: The base int type (annotated types cannot be returned from functions).
    """
    constraints = {}
    if min_val is not None:
        constraints["ge"] = min_val
    if max_val is not None:
        constraints["le"] = max_val

    # Note: Can't return annotated types from functions, return base type
    return int


def create_constrained_string(
    min_length: int | None = None,
    max_length: int | None = None,
    pattern: str | None = None,
    description: str = "Constrained string value",
) -> type[str]:
    """Create a string type with custom constraints.

    Returns:
        type[str]: The base str type (annotated types cannot be returned from functions).
    """
    StringConstraints(
        min_length=min_length, max_length=max_length, pattern=pattern, strip_whitespace=True
    )

    # Note: Can't return annotated types from functions, return base type
    return str
