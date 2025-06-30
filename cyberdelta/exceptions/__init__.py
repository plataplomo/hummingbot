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

# Decorator exceptions
from .decorators import (
    AsyncDecoratorError,
    DecoratorError,
    NoExceptionCapturedError,
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
    DictStructureError,
    EmptyDictionaryError,
    EmptyStringError,
    KlineTypeError,
    KlineValueError,
    MsgpackSerializationError,
    NonNullableFieldError,
    ParsingError,
    SequenceLengthError,
    StructureTypeError,
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

# Request validation exceptions
from .request_validation import (
    DecimalFormatError,
    DecimalRangeError,
    InvalidEnumValueError,
    InvalidParameterTypeError,
    MissingRequiredParameterError,
    PrecisionLossError,
    RequestValidationError,
)

# Response validation exceptions
from .response_validation import (
    EmptyResponseError,
    InvalidLeverageError,
    NotImplementedOperationError,
    ResponseValidationError,
    UnreachableCodeError,
)

# Security exceptions
from .security import (
    FieldConstraintError,
    FieldTypeError,
    FinancialFieldError,
    InvalidMapperResultError,
    MapperNotFoundError,
    SecurityValidationError,
)

# Service validation exceptions
from .service_validation import (
    EmptyStringParameterError,
    IntegerConversionError,
    InvalidAccountTypeError,
    MissingPriceError,
    MissingStopPriceError,
    NegativeValueError,
    NetworkRequiredError,
    OrderParameterError,
    PostOnlyLimitError,
    ServiceValidationError,
    TimeRangeError,
    TransferAccountError,
    UnsupportedNetworkError,
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

# Trading transformation exceptions
from .trading_transformation import (
    InvalidQuantityError,
    MissingQuantityError,
    MissingTimestampError,
    OrderTransformationFailedError,
    UnknownOrderSideError,
)

# WebSocket exceptions
from .websocket import (
    InvalidWebSocketDataError,
    UnsupportedWebSocketTopicError,
    UserEventsSubscriptionError,
    WebSocketError,
    WebSocketSubscriptionError,
)


__all__ = [
    # Re-exported existing classes
    "APIError",
    "APIErrorCode",
    "ActionHashError",
    "ArbitrageError",
    "AsyncDecoratorError",
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
    "DecimalFormatError",
    "DecimalRangeError",
    "DecoratorError",
    "DeltaNeutralError",
    "DictStructureError",
    "EmptyDictionaryError",
    "EmptyResponseError",
    "EmptyStringError",
    "EmptyStringParameterError",
    "EnumFieldError",
    "FieldConstraintError",
    "FieldError",
    "FieldNameMissingError",
    "FieldTypeError",
    "FinancialFieldError",
    "FundingRateArbitrageError",
    "FundingRateTransformationError",
    "FundingRateUnavailableError",
    "InsufficientBalanceError",
    "IntegerConversionError",
    "InvalidAPIKeyError",
    "InvalidAccountTypeError",
    "InvalidBatchResponseError",
    "InvalidEnumValueError",
    "InvalidFormatError",
    "InvalidLeverageError",
    "InvalidMapperResultError",
    "InvalidMappingError",
    "InvalidParameterTypeError",
    "InvalidPrivateKeyError",
    "InvalidQuantityError",
    "InvalidWebSocketDataError",
    "KlineTypeError",
    "KlineValueError",
    "MapperNotFoundError",
    "MappingError",
    "MarketClosedError",
    "MarketDataError",
    "MarketTransformationError",
    "MissingPriceError",
    "MissingQuantityError",
    "MissingRequiredFieldError",
    "MissingRequiredParameterError",
    "MissingStopPriceError",
    "MissingTimestampError",
    "MsgpackSerializationError",
    "NegativeValueError",
    "NetworkRequiredError",
    "NoExceptionCapturedError",
    "NonFinitePositionValueError",
    "NonNullableFieldError",
    "NotImplementedOperationError",
    "OrderBookError",
    "OrderBookTransformationError",
    "OrderError",
    "OrderFieldError",
    "OrderLogicError",
    "OrderNotFoundError",
    "OrderParameterError",
    "OrderSizeError",
    "OrderTransformationError",
    "OrderTransformationFailedError",
    "ParsingError",
    "PassphraseFieldError",
    "PositionDiscrepancyError",
    "PositionFieldError",
    "PositionLogicError",
    "PositionNotFoundError",
    "PositionSyncError",
    "PostOnlyLimitError",
    "PrecisionLossError",
    "RangeFieldError",
    "RateLimitConfigurationError",
    "RebalanceError",
    "ReconciliationError",
    "RequestValidationError",
    "RequiredFieldError",
    "RequiredFieldNoneError",
    "RequiredParameterError",
    "ResponseValidationError",
    "RiskLimitError",
    "SecurityValidationError",
    "SequenceLengthError",
    "ServiceValidationError",
    "StrategyError",
    "StructureTypeError",
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
    "UnknownOrderSideError",
    "UnreachableCodeError",
    "UnsupportedNetworkError",
    "UnsupportedWebSocketTopicError",
    "UserEventsSubscriptionError",
    "WebSocketError",
    "WebSocketSignatureError",
    "WebSocketSubscriptionError",
]
