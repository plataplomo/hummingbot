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
    DecimalFiniteError,
    EnumFieldError,
    FieldError,
    FieldNameMissingError,
    InvalidFormatError,
    OrderFieldError,
    OrderLogicError,
    PassphraseFieldError,
    PositionLogicError,
    RangeFieldError,
    RequiredFieldError,
    RequiredFieldNoneError,
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

# Reconciliation exceptions
from .reconciliation import (
    NonFinitePositionValueError,
    PositionDiscrepancyError,
    PositionFieldError,
    ReconciliationError,
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
    InvalidBatchResponseError,
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
    "DecimalFiniteError",
    "DeltaNeutralError",
    "EmptyStringError",
    "EnumFieldError",
    "FieldError",
    "FieldNameMissingError",
    "FundingRateArbitrageError",
    "FundingRateTransformationError",
    "FundingRateUnavailableError",
    "InsufficientBalanceError",
    "IntegerConversionError",
    "InvalidAPIKeyError",
    "InvalidBatchResponseError",
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
    "NonFinitePositionValueError",
    "NonNullableFieldError",
    "OrderBookError",
    "OrderBookTransformationError",
    "OrderError",
    "OrderFieldError",
    "OrderLogicError",
    "OrderNotFoundError",
    "OrderParameterError",
    "OrderSizeError",
    "OrderTransformationError",
    "ParsingError",
    "PassphraseFieldError",
    "PositionDiscrepancyError",
    "PositionFieldError",
    "PositionLogicError",
    "PositionNotFoundError",
    "PositionSyncError",
    "PostOnlyLimitError",
    "RangeFieldError",
    "RateLimitConfigurationError",
    "RebalanceError",
    "ReconciliationError",
    "RequiredFieldError",
    "RequiredFieldNoneError",
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
