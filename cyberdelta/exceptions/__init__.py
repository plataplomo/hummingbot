"""CyberDelta Exception Classes.

This module extends the existing exception infrastructure with specific
exception classes that fix TRY003/TRY301 Ruff violations while maintaining
100% backward compatibility.

All new exceptions inherit from either APIError or TransformationError,
preserving existing functionality like retry logic, error mapping, and metadata.
"""

# Note: APIError, APIErrorCode, and TransformationError are not imported here
# to avoid circular imports. Import them directly from cyberdelta.apis.common when needed.

# Authentication exceptions moved to cyberdelta.apis.exceptions

# Import new specific exception classes as we create them
# Configuration exceptions - core config exceptions moved to .base
# API-specific configuration exceptions remain in .configuration

# Connectivity exceptions moved to cyberdelta.apis.exceptions

# Data transformation exceptions moved to cyberdelta.apis.exceptions
# Decorator exceptions moved to cyberdelta.apis.exceptions

# Field exceptions
from .field_validation import (
    BooleanFieldError,
    DecimalFieldError,
    DecimalFiniteError,
    EnumFieldError,
    FieldError,
    FieldNameMissingError,
    InvalidFormatError,
    ListFieldError,
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

# Market data exceptions moved to cyberdelta.apis.exceptions
# Parsing exceptions (core utilities only)
from .parsing import (
    DateTimeParsingError,
    EmptyStringError,
    ParsingError,
    TimestampFormatError,
)

# Reconciliation exceptions
from .reconciliation import (
    NonFinitePositionValueError,
    PositionDiscrepancyError,
    PositionFieldError,
    ReconciliationError,
)

# Request validation exceptions moved to cyberdelta.apis.exceptions
# Response validation exceptions moved to cyberdelta.apis.exceptions
# Security exceptions moved to cyberdelta.apis.exceptions
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


# Strategy exceptions moved to cyberdelta.apis.exceptions

# Trading exceptions moved to cyberdelta.apis.exceptions

# Trading transformation exceptions moved to cyberdelta.apis.exceptions

# WebSocket exceptions moved to cyberdelta.apis.exceptions


__all__ = [
    # Note: APIError, APIErrorCode, TransformationError are not exported here
    # Import them directly from cyberdelta.apis.common when needed
    "BooleanFieldError",
    "DateTimeParsingError",
    "DecimalFieldError",
    "DecimalFiniteError",
    "EmptyStringError",
    "EmptyStringParameterError",
    "EnumFieldError",
    "FieldError",
    "FieldNameMissingError",
    "IntegerConversionError",
    "InvalidAccountTypeError",
    "InvalidFormatError",
    "ListFieldError",
    "MissingPriceError",
    "MissingStopPriceError",
    "NegativeValueError",
    "NetworkRequiredError",
    "NonFinitePositionValueError",
    "OrderFieldError",
    "OrderLogicError",
    "OrderParameterError",
    "ParsingError",
    "PassphraseFieldError",
    "PositionDiscrepancyError",
    "PositionFieldError",
    "PositionLogicError",
    "PostOnlyLimitError",
    "RangeFieldError",
    "ReconciliationError",
    "RequiredFieldError",
    "RequiredFieldNoneError",
    "ServiceValidationError",
    "TimeRangeError",
    "TimestampFieldError",
    "TimestampFormatError",
    "TransferAccountError",
    "TypeFieldError",
    "UnsupportedNetworkError",
]
