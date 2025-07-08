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

# Funding exceptions
from .funding import (
    AllSourcesFailedError,
    ArbitrageFieldError,
    FundingError,
    FundingRateSourceError,
    NegativeLongPriceError,
    NegativeShortPriceError,
    NegativeSizeError,
    NoFallbackSourceError,
    NoFundingDataError,
    NoValidWeightedDataError,
    NullTimestampError,
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

# Symbol mapping exceptions
from .symbol_mapping import (
    ExchangeNotSupportedError,
    InvalidSymbolFormatError,
    SymbolMappingConfigurationError,
    SymbolMappingError,
    SymbolMappingErrorMessages,
    SymbolMappingFieldError,
    SymbolNotFoundError,
)


# Strategy exceptions moved to cyberdelta.apis.exceptions

# Trading exceptions moved to cyberdelta.apis.exceptions

# Trading transformation exceptions moved to cyberdelta.apis.exceptions

# WebSocket exceptions moved to cyberdelta.apis.exceptions


__all__ = [
    # Note: APIError, APIErrorCode, TransformationError are not exported here
    # Import them directly from cyberdelta.apis.common when needed
    "AllSourcesFailedError",
    "ArbitrageFieldError",
    "BooleanFieldError",
    "DateTimeParsingError",
    "DecimalFieldError",
    "DecimalFiniteError",
    "EmptyStringError",
    "EmptyStringParameterError",
    "EnumFieldError",
    "ExchangeNotSupportedError",
    "FieldError",
    "FieldNameMissingError",
    "FundingError",
    "FundingRateSourceError",
    "IntegerConversionError",
    "InvalidAccountTypeError",
    "InvalidFormatError",
    "InvalidSymbolFormatError",
    "ListFieldError",
    "MissingPriceError",
    "MissingStopPriceError",
    "NegativeLongPriceError",
    "NegativeShortPriceError",
    "NegativeSizeError",
    "NegativeValueError",
    "NetworkRequiredError",
    "NoFallbackSourceError",
    "NoFundingDataError",
    "NoValidWeightedDataError",
    "NonFinitePositionValueError",
    "NullTimestampError",
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
    "SymbolMappingConfigurationError",
    "SymbolMappingError",
    "SymbolMappingErrorMessages",
    "SymbolMappingFieldError",
    "SymbolNotFoundError",
    "TimeRangeError",
    "TimestampFieldError",
    "TimestampFormatError",
    "TransferAccountError",
    "TypeFieldError",
    "UnsupportedNetworkError",
]
