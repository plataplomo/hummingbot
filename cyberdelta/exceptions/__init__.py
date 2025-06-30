"""CyberDelta Exception Classes.

This module extends the existing exception infrastructure with specific
exception classes that fix TRY003/TRY301 Ruff violations while maintaining
100% backward compatibility.

All new exceptions inherit from either APIError or TransformationError,
preserving existing functionality like retry logic, error mapping, and metadata.
"""

# Re-export existing base exceptions for convenience
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError

# Authentication exceptions
from .authentication import (
    AuthenticationError,
    AuthenticationPreparationError,
    AuthenticatorNotConfiguredError,
    InvalidAPIKeyError,
    InvalidPrivateKeyError,
    UnknownEndpointError,
    WebSocketSignatureError,
)

# Import new specific exception classes as we create them
# Configuration exceptions
from .configuration import (
    ConfigurationError,
    RateLimitConfigurationError,
    RequiredParameterError,
    TestnetConfigurationError,
)

# Data transformation exceptions
from .data_transformation import (
    CollateralTransformationError,
    DataTransformationError,
    InvalidMappingError,
    MappingError,
    MissingRequiredFieldError,
    OrderTransformationError,
    UnknownEnumError,
)

# Field exceptions
from .field_validation import (
    BooleanFieldError,
    DecimalFieldError,
    EnumFieldError,
    FieldError,
    InvalidFormatError,
    PassphraseFieldError,
    RangeFieldError,
    RequiredFieldError,
    TimestampFieldError,
    TypeFieldError,
)

# Market data exceptions
from .market_data import (
    DataUnavailableError,
    FundingRateUnavailableError,
    MarketDataError,
    OrderBookError,
    SymbolNotFoundError,
    TickerError,
)

# Parsing exceptions
from .parsing import (
    ActionHashError,
    ClientIdFormatError,
    DateTimeParsingError,
    EmptyStringError,
    MsgpackSerializationError,
    NonNullableFieldError,
    ParsingError,
    TimestampFormatError,
    TimestampYearRangeError,
)

# Service validation exceptions
from .service_validation import (
    IntegerConversionError,
    MissingPriceError,
    MissingStopPriceError,
    NegativeValueError,
    OrderParameterError,
    PostOnlyLimitError,
    ServiceValidationError,
    TimeRangeError,
    TransferAccountError,
)

# Strategy exceptions
from .strategy import (
    ArbitrageError,
    DeltaNeutralError,
    FundingRateArbitrageError,
    PositionSyncError,
    RebalanceError,
    RiskLimitError,
    StrategyError,
)

# Trading exceptions
from .trading import (
    InsufficientBalanceError,
    MarketClosedError,
    OrderError,
    OrderNotFoundError,
    OrderSizeError,
    PositionNotFoundError,
    TradingError,
)


__all__ = [
    # Re-exported existing classes
    "APIError",
    "APIErrorCode",
    # Strategy exceptions
    "ArbitrageError",
    # Authentication exceptions
    "AuthenticationError",
    "AuthenticationPreparationError",
    "AuthenticatorNotConfiguredError",
    # Configuration exceptions
    "ConfigurationError",
    # Market data exceptions
    "DataUnavailableError",
    "DeltaNeutralError",
    # Field exceptions
    "BooleanFieldError",
    "DecimalFieldError",
    "EnumFieldError",
    "FieldError",
    "FundingRateArbitrageError",
    "FundingRateUnavailableError",
    # Trading exceptions
    "InsufficientBalanceError",
    "InvalidAPIKeyError",
    "InvalidFormatError",
    "InvalidPrivateKeyError",
    "MarketClosedError",
    "MarketDataError",
    "OrderBookError",
    "OrderError",
    "OrderNotFoundError",
    "OrderSizeError",
    "PassphraseFieldError",
    "PositionNotFoundError",
    "PositionSyncError",
    "RangeFieldError",
    "RateLimitConfigurationError",
    "RebalanceError",
    "RequiredFieldError",
    "RequiredParameterError",
    "RiskLimitError",
    "StrategyError",
    "SymbolNotFoundError",
    "TestnetConfigurationError",
    "TickerError",
    "TimestampFieldError",
    "TradingError",
    "TransformationError",
    "TypeFieldError",
    "UnknownEndpointError",
    "WebSocketSignatureError",
    # Service validation exceptions
    "ServiceValidationError",
    "OrderParameterError",
    "PostOnlyLimitError",
    "MissingPriceError",
    "MissingStopPriceError",
    "TimeRangeError",
    "TransferAccountError",
    "IntegerConversionError",
    "NegativeValueError",
    # Parsing exceptions
    "ActionHashError",
    "ClientIdFormatError",
    "DateTimeParsingError",
    "EmptyStringError",
    "MsgpackSerializationError",
    "NonNullableFieldError",
    "ParsingError",
    "TimestampFormatError",
    "TimestampYearRangeError",
    # Data transformation exceptions
    "MappingError",
    "UnknownEnumError",
    "MissingRequiredFieldError",
    "DataTransformationError",
    "InvalidMappingError",
    "CollateralTransformationError",
    "OrderTransformationError",
]
