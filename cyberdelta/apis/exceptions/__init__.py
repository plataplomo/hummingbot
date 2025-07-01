"""API-specific exceptions for CyberDelta.

This module contains exceptions that inherit from APIError or TransformationError
and are used exclusively within the API layer.
"""

# Authentication exceptions
# Parsing exceptions - import base ones from core
from cyberdelta.exceptions.parsing import (
    DateTimeParsingError,
    EmptyStringError,
    ParsingError,
    TimestampFormatError,
)

from .authentication import (
    AuthenticationError,
    AuthenticationPreparationError,
    AuthenticatorNotConfiguredError,
    InvalidAPIKeyError,
    InvalidPrivateKeyError,
    UnknownEndpointError,
    WebSocketSignatureError,
)

# Connectivity exceptions
from .connectivity import (
    ConnectivityError,
    ContentTypeValidationError,
    EmptyResponseError as ConnectivityEmptyResponseError,
    HttpClientError,
    HttpTimeoutError,
    InvalidContentTypeError,
    ResponseParsingError,
    WebSocketConnectionClosedError,
    WebSocketError as ConnectivityWebSocketError,
    WebSocketNotConnectedError,
    WhitespaceContentTypeError,
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

# Market data exceptions
from .market_data import (
    DataUnavailableError,
    FundingRateUnavailableError,
    MarketDataError,
    OrderBookError,
    SymbolNotFoundError,
    TickerError,
)

# Market data service exceptions
from .market_data_service import (
    MarketDataServiceError,
)

# Import API-specific parsing exceptions from local module
from .parsing import (
    ActionHashError,
    ClientIdFormatError,
    DictStructureError,
    EmptyDictionaryError,
    KlineTypeError,
    KlineValueError,
    MsgpackSerializationError,
    NonNullableFieldError,
    SequenceLengthError,
    StructureTypeError,
    TimestampYearRangeError,
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
    "ActionHashError",
    "ArbitrageError",
    "AuthenticationError",
    "AuthenticationPreparationError",
    "AuthenticatorNotConfiguredError",
    "CandleTransformationError",
    "ClientIdFormatError",
    "CollateralTransformationError",
    "ConnectivityEmptyResponseError",
    "ConnectivityError",
    "ConnectivityWebSocketError",
    "ContentTypeValidationError",
    "DataTransformationError",
    "DataUnavailableError",
    "DateTimeParsingError",
    "DecimalFormatError",
    "DecimalRangeError",
    "DeltaNeutralError",
    "DictStructureError",
    "EmptyDictionaryError",
    "EmptyResponseError",
    "EmptyStringError",
    "FieldConstraintError",
    "FieldTypeError",
    "FinancialFieldError",
    "FundingRateArbitrageError",
    "FundingRateTransformationError",
    "FundingRateUnavailableError",
    "HttpClientError",
    "HttpTimeoutError",
    "InsufficientBalanceError",
    "InvalidAPIKeyError",
    "InvalidBatchResponseError",
    "InvalidContentTypeError",
    "InvalidEnumValueError",
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
    "MarketDataServiceError",
    "MarketTransformationError",
    "MissingQuantityError",
    "MissingRequiredFieldError",
    "MissingRequiredParameterError",
    "MissingTimestampError",
    "MsgpackSerializationError",
    "NonNullableFieldError",
    "NotImplementedOperationError",
    "OrderBookError",
    "OrderBookTransformationError",
    "OrderError",
    "OrderNotFoundError",
    "OrderSizeError",
    "OrderTransformationError",
    "OrderTransformationFailedError",
    "ParsingError",
    "PositionNotFoundError",
    "PositionSyncError",
    "PrecisionLossError",
    "RebalanceError",
    "RequestValidationError",
    "ResponseParsingError",
    "ResponseValidationError",
    "RiskLimitError",
    "SecurityValidationError",
    "SequenceLengthError",
    "StrategyError",
    "StructureTypeError",
    "SymbolNotFoundError",
    "TickerError",
    "TickerTransformationError",
    "TimestampFormatError",
    "TimestampYearRangeError",
    "TradeTransformationError",
    "TradingError",
    "UnknownEndpointError",
    "UnknownEnumError",
    "UnknownOrderSideError",
    "UnreachableCodeError",
    "UnsupportedWebSocketTopicError",
    "UserEventsSubscriptionError",
    "WebSocketConnectionClosedError",
    "WebSocketError",
    "WebSocketNotConnectedError",
    "WebSocketSignatureError",
    "WebSocketSubscriptionError",
    "WhitespaceContentTypeError",
]
