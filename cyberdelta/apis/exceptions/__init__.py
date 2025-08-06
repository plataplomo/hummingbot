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
    AuthenticationPreparationError,
    AuthenticatorNotConfiguredError,
    InvalidAPIKeyError,
    InvalidPrivateKeyError,
    UnknownEndpointError,
    WebSocketSignatureError,
)

# Connectivity exceptions
from .connectivity import (
    ContentTypeValidationError,
    HttpTimeoutError,
    ResponseParsingError,
    WebSocketConnectionClosedError,
)

# Data transformation exceptions
from .data_transformation import (
    CandleTransformationError,
    CollateralTransformationError,
    DataTransformationError,
    FillTransformationError,
    FundingRateTransformationError,
    InvalidMappingError,
    MarketTransformationError,
    MissingRequiredFieldError,
    OrderBookTransformationError,
    OrderTransformationError,
    TickerTransformationError,
    TradeTransformationError,
    UnknownEnumError,
)

# Market data service exceptions
from .market_data_service import (
    MarketDataServiceError,
    SymbolNotFoundError,
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
)

# Response validation exceptions
from .response_validation import (
    EmptyResponseError,
    InvalidLeverageError,
    NotImplementedOperationError,
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

# Service exceptions
from .service import ServiceParameterError

# Trading exceptions
from .trading import (
    InvalidBatchResponseError,
    MarketClosedError,
    OrderError,
    OrderNotFoundError,
)

# Trading transformation exceptions
from .trading_transformation import (
    InvalidQuantityError,
    MissingQuantityError,
    MissingTimestampError,
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
    "AuthenticationPreparationError",
    "AuthenticatorNotConfiguredError",
    "CandleTransformationError",
    "ClientIdFormatError",
    "CollateralTransformationError",
    "ContentTypeValidationError",
    "DataTransformationError",
    "DateTimeParsingError",
    "DecimalFormatError",
    "DecimalRangeError",
    "DictStructureError",
    "EmptyDictionaryError",
    "EmptyResponseError",
    "EmptyStringError",
    "FieldConstraintError",
    "FieldTypeError",
    "FillTransformationError",
    "FinancialFieldError",
    "FundingRateTransformationError",
    "HttpTimeoutError",
    "InvalidAPIKeyError",
    "InvalidBatchResponseError",
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
    "MarketClosedError",
    "MarketDataServiceError",
    "MarketTransformationError",
    "MissingQuantityError",
    "MissingRequiredFieldError",
    "MissingRequiredParameterError",
    "MissingTimestampError",
    "MsgpackSerializationError",
    "NonNullableFieldError",
    "NotImplementedOperationError",
    "OrderBookTransformationError",
    "OrderError",
    "OrderNotFoundError",
    "OrderTransformationError",
    "ParsingError",
    "PrecisionLossError",
    "ResponseParsingError",
    "SecurityValidationError",
    "SequenceLengthError",
    "ServiceParameterError",
    "StructureTypeError",
    "SymbolNotFoundError",
    "TickerTransformationError",
    "TimestampFormatError",
    "TimestampYearRangeError",
    "TradeTransformationError",
    "UnknownEndpointError",
    "UnknownEnumError",
    "UnknownOrderSideError",
    "UnreachableCodeError",
    "UnsupportedWebSocketTopicError",
    "UserEventsSubscriptionError",
    "WebSocketConnectionClosedError",
    "WebSocketError",
    "WebSocketSignatureError",
    "WebSocketSubscriptionError",
]
