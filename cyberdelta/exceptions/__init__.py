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

# Financial exceptions
from .financial import (
    BalanceLockTimeoutError,
    CurrencyMismatchError,
    DivisionByZeroError,
    InvalidAmountError,
)

# Funding exceptions - removed as they're not used anymore
# The APIs have their own funding exceptions
# Market data exceptions
from .market import (
    MarketDataError,
    MarketDataMissingPriceError,
    MissingOrderbookError,
    MissingQuoteError,
    MissingVolumeError,
)

# Monitoring exceptions
from .monitoring import (
    InvalidMetricError,
    MetricCalculationError,
    MonitoringError,
)

# Parsing exceptions (core utilities only)
from .parsing import (
    DateTimeParsingError,
    EmptyStringError,
    ParsingError,
    TimestampFormatError,
)

# Portfolio exceptions
from .portfolio import (
    ExchangeNotSupportedError as PortfolioExchangeNotSupportedError,
    InvalidPositionDataError,
    MissingBalanceDataError,
    MissingClosePriceError,
    MissingEntryPriceError,
    MissingRealizedPnLError,
    PortfolioError,
    PortfolioNotInitializedError,
    PortfolioStateError,
    ReconciliationError,
    StorageError,
)

# Reconciliation exceptions - removed as they're not used anymore
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

# System exceptions
from .system import (
    CyberDeltaSystemError,
    SystemHealthError,
)

# Trading exceptions
from .trading import (
    BalanceLockError,
    OrderDataError,
    PositionDataError,
    PositionNoneError,
    RealizedPnLNoneError,
    RiskAssessmentError,
    SignalDataError,
    TotalEquityNoneError,
    TradingError,
)


# Strategy exceptions moved to cyberdelta.apis.exceptions

# Trading exceptions moved to cyberdelta.apis.exceptions

# Trading transformation exceptions moved to cyberdelta.apis.exceptions

# WebSocket exceptions moved to cyberdelta.apis.exceptions


__all__ = [
    "BalanceLockError",
    "BalanceLockTimeoutError",
    # Note: APIError, APIErrorCode, TransformationError are not exported here
    # Import them directly from cyberdelta.apis.common when needed
    "BooleanFieldError",
    "CurrencyMismatchError",
    "CyberDeltaSystemError",
    "DateTimeParsingError",
    "DecimalFieldError",
    "DecimalFiniteError",
    "DivisionByZeroError",
    "EmptyStringError",
    "EmptyStringParameterError",
    "EnumFieldError",
    "ExchangeNotSupportedError",
    "FieldError",
    "FieldNameMissingError",
    "IntegerConversionError",
    "InvalidAccountTypeError",
    "InvalidAmountError",
    "InvalidFormatError",
    "InvalidMetricError",
    "InvalidPositionDataError",
    "InvalidSymbolFormatError",
    "ListFieldError",
    "MarketDataError",
    "MarketDataMissingPriceError",
    "MetricCalculationError",
    "MissingBalanceDataError",
    "MissingClosePriceError",
    "MissingEntryPriceError",
    "MissingOrderbookError",
    "MissingPriceError",
    "MissingQuoteError",
    "MissingRealizedPnLError",
    "MissingStopPriceError",
    "MissingVolumeError",
    "MonitoringError",
    "NegativeValueError",
    "NetworkRequiredError",
    "OrderDataError",
    "OrderFieldError",
    "OrderLogicError",
    "OrderParameterError",
    "ParsingError",
    "PassphraseFieldError",
    "PortfolioError",
    "PortfolioExchangeNotSupportedError",
    "PortfolioNotInitializedError",
    "PortfolioStateError",
    "PositionDataError",
    "PositionLogicError",
    "PositionNoneError",
    "PostOnlyLimitError",
    "RangeFieldError",
    "RealizedPnLNoneError",
    "ReconciliationError",
    "RequiredFieldError",
    "RequiredFieldNoneError",
    "RiskAssessmentError",
    "ServiceValidationError",
    "SignalDataError",
    "StorageError",
    "SymbolMappingConfigurationError",
    "SymbolMappingError",
    "SymbolMappingErrorMessages",
    "SymbolMappingFieldError",
    "SymbolNotFoundError",
    "SystemHealthError",
    "TimeRangeError",
    "TimestampFieldError",
    "TimestampFormatError",
    "TotalEquityNoneError",
    "TradingError",
    "TransferAccountError",
    "TypeFieldError",
    "UnsupportedNetworkError",
]
