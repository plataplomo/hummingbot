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
    CandleTransformationError,
    CollateralTransformationError,
    DataTransformationError,
    FundingRateTransformationError,
    InvalidMappingError,
    MappingError,
    MarketTransformationError,
    MissingRequiredFieldError,
    OrderBookTransformationError,
    OrderTransformationError,
    TickerTransformationError,
    TradeTransformationError,
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
    KlineTypeError,
    KlineValueError,
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
    "ActionHashError",
    "ArbitrageError",
    "AuthenticationError",
    "AuthenticationPreparationError",
    "AuthenticatorNotConfiguredError",
    "BooleanFieldError",
    "CandleTransformationError",
    "ClientIdFormatError",
    "CollateralTransformationError",
    "ConfigurationError",
    "DataTransformationError",
    "DataUnavailableError",
    "DateTimeParsingError",
    "DecimalFieldError",
    "DeltaNeutralError",
    "EmptyStringError",
    "EnumFieldError",
    "FieldError",
    "FundingRateArbitrageError",
    "FundingRateTransformationError",
    "FundingRateUnavailableError",
    "InsufficientBalanceError",
    "IntegerConversionError",
    "InvalidAPIKeyError",
    "InvalidFormatError",
    "InvalidMappingError",
    "InvalidPrivateKeyError",
    "KlineTypeError",
    "KlineValueError",
    "MappingError",
    "MarketClosedError",
    "MarketDataError",
    "MarketTransformationError",
    "MissingPriceError",
    "MissingRequiredFieldError",
    "MissingStopPriceError",
    "MsgpackSerializationError",
    "NegativeValueError",
    "NonNullableFieldError",
    "OrderBookError",
    "OrderBookTransformationError",
    "OrderError",
    "OrderNotFoundError",
    "OrderParameterError",
    "OrderSizeError",
    "OrderTransformationError",
    "ParsingError",
    "PassphraseFieldError",
    "PositionNotFoundError",
    "PositionSyncError",
    "PostOnlyLimitError",
    "RangeFieldError",
    "RateLimitConfigurationError",
    "RebalanceError",
    "RequiredFieldError",
    "RequiredParameterError",
    "RiskLimitError",
    "ServiceValidationError",
    "StrategyError",
    "SymbolNotFoundError",
    "TestnetConfigurationError",
    "TickerError",
    "TickerTransformationError",
    "TimeRangeError",
    "TimestampFieldError",
    "TimestampFormatError",
    "TimestampYearRangeError",
    "TradeTransformationError",
    "TradingError",
    "TransferAccountError",
    "TransformationError",
    "TypeFieldError",
    "UnknownEndpointError",
    "UnknownEnumError",
    "WebSocketSignatureError",
]
