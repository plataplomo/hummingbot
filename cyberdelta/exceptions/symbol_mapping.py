"""Symbol mapping exceptions for CyberDelta.

These exceptions handle symbol mapping validation errors and follow
the standard CyberDelta three-layer exception architecture:
- Layer 1: API Operations → APIError
- Layer 2: Input Validation → FieldError + ValueError/TypeError (Multiple inheritance)
- Layer 3: Data Transformation → TransformationError

Symbol mapping errors are primarily Layer 2 (Input Validation) errors.
"""

from typing import Any


class SymbolMappingError(Exception):
    """Base exception for symbol mapping errors.

    This is the base class for all symbol mapping related errors.
    It provides consistent metadata and context handling.
    """

    def __init__(
        self,
        message: str,
        *,
        exchange_id: str | None = None,
        symbol: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize symbol mapping error.

        Args:
            message: Human-readable error description
            exchange_id: Exchange identifier where error occurred
            symbol: Symbol involved in the error
            metadata: Additional error context
            original_exception: The underlying exception
        """
        super().__init__(message)
        self.exchange_id = exchange_id
        self.symbol = symbol
        self.metadata = metadata or {}
        self.original_exception = original_exception


class SymbolMappingConfigurationError(ValueError, SymbolMappingError):
    """Configuration validation errors during symbol mapper initialization.

    Raised when symbol mapper configuration is invalid, missing, or malformed.
    Inherits ValueError semantics for configuration validation failures.
    """

    def __init__(
        self,
        message: str,
        *,
        exchange_id: str | None = None,
        config_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize configuration error.

        Args:
            message: Human-readable error description
            exchange_id: Exchange identifier where config error occurred
            config_type: Type of configuration that failed
            metadata: Additional error context
            original_exception: The underlying exception
        """
        # Initialize ValueError with the message
        ValueError.__init__(self, message)

        # Initialize SymbolMappingError with metadata
        SymbolMappingError.__init__(
            self,
            message,
            exchange_id=exchange_id,
            metadata=metadata or {},
            original_exception=original_exception,
        )

        self.config_type = config_type

        # Add config-specific metadata
        if config_type:
            self.metadata["config_type"] = config_type


class SymbolMappingFieldError(ValueError, SymbolMappingError):
    """Field validation errors for symbol mapping operations.

    Raised when symbol or exchange ID fields fail validation.
    Inherits ValueError semantics for field validation failures.
    """

    def __init__(
        self,
        message: str,
        *,
        field_name: str | None = None,
        field_value: object = None,
        exchange_id: str | None = None,
        symbol: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize field validation error.

        Args:
            message: Human-readable error description
            field_name: Name of the field that failed validation
            field_value: The value that failed validation
            exchange_id: Exchange identifier where error occurred
            symbol: Symbol involved in the error
            metadata: Additional error context
            original_exception: The underlying exception
        """
        # Initialize ValueError with the message
        ValueError.__init__(self, message)

        # Initialize SymbolMappingError with metadata
        SymbolMappingError.__init__(
            self,
            message,
            exchange_id=exchange_id,
            symbol=symbol,
            metadata=metadata or {},
            original_exception=original_exception,
        )

        self.field_name = field_name
        self.field_value = field_value

        # Add field-specific metadata
        if field_name:
            self.metadata["field_name"] = field_name
        if field_value is not None:
            self.metadata["field_value"] = field_value


class SymbolNotFoundError(SymbolMappingError):
    """Symbol not found in mappings.

    Raised when a symbol lookup fails because the symbol is not
    configured in the symbol mapper.
    """

    def __init__(
        self,
        message: str,
        *,
        symbol: str | None = None,
        exchange_id: str | None = None,
        lookup_type: str = "symbol_lookup",
        available_symbols: list[str] | None = None,
        available_exchanges: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize symbol not found error.

        Args:
            message: Human-readable error description
            symbol: Symbol that was not found
            exchange_id: Exchange identifier where lookup failed
            lookup_type: Type of lookup that failed
            available_symbols: List of available symbols
            available_exchanges: List of available exchanges
            metadata: Additional error context
            original_exception: The underlying exception
        """
        super().__init__(
            message,
            exchange_id=exchange_id,
            symbol=symbol,
            metadata=metadata or {},
            original_exception=original_exception,
        )

        self.lookup_type = lookup_type
        self.available_symbols = available_symbols or []
        self.available_exchanges = available_exchanges or []

        # Add lookup-specific metadata
        self.metadata.update({
            "lookup_type": lookup_type,
            "available_symbols": self.available_symbols,
            "available_exchanges": self.available_exchanges,
        })


class ExchangeNotSupportedError(SymbolMappingError):
    """Exchange not supported by symbol mapper.

    Raised when an operation is attempted on an exchange that
    is not configured in the symbol mapper.
    """

    def __init__(
        self,
        message: str,
        *,
        exchange_id: str | None = None,
        supported_exchanges: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize exchange not supported error.

        Args:
            message: Human-readable error description
            exchange_id: Exchange identifier that is not supported
            supported_exchanges: List of supported exchanges
            metadata: Additional error context
            original_exception: The underlying exception
        """
        super().__init__(
            message,
            exchange_id=exchange_id,
            metadata=metadata or {},
            original_exception=original_exception,
        )

        self.supported_exchanges = supported_exchanges or []

        # Add exchange-specific metadata
        self.metadata["supported_exchanges"] = self.supported_exchanges


class InvalidSymbolFormatError(ValueError, SymbolMappingError):
    """Invalid symbol format error.

    Raised when a symbol does not match the expected format pattern.
    Inherits ValueError semantics for format validation failures.
    """

    def __init__(
        self,
        message: str,
        *,
        symbol: str | None = None,
        expected_pattern: str | None = None,
        symbol_type: str = "symbol",
        exchange_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize invalid symbol format error.

        Args:
            message: Human-readable error description
            symbol: Symbol that has invalid format
            expected_pattern: Expected regex pattern
            symbol_type: Type of symbol (internal/exchange)
            exchange_id: Exchange identifier where error occurred
            metadata: Additional error context
            original_exception: The underlying exception
        """
        # Initialize ValueError with the message
        ValueError.__init__(self, message)

        # Initialize SymbolMappingError with metadata
        SymbolMappingError.__init__(
            self,
            message,
            exchange_id=exchange_id,
            symbol=symbol,
            metadata=metadata or {},
            original_exception=original_exception,
        )

        self.expected_pattern = expected_pattern
        self.symbol_type = symbol_type

        # Add format-specific metadata
        if expected_pattern:
            self.metadata["expected_pattern"] = expected_pattern
        self.metadata["symbol_type"] = symbol_type


class SymbolMappingErrorMessages:
    """Error message constants for symbol mapping.

    These constants satisfy TRY003 linting requirements by centralizing
    error messages outside of exception constructors.
    """

    # Configuration errors
    CONFIG_NOT_DICT = "Configuration must be a dictionary"
    CONFIG_EMPTY = "Configuration cannot be empty"
    EXCHANGE_ID_INVALID = "Exchange ID must be a non-empty string"
    EXCHANGE_DATA_INVALID = "Exchange data must be a dictionary"
    SYMBOLS_MISSING = "Exchange missing required symbols section"
    SYMBOLS_NOT_DICT = "Symbols must be a dictionary"
    SYMBOLS_EMPTY = "Exchange has no symbol mappings"
    NO_EXCHANGES = "No enabled exchanges found"
    NO_SYMBOLS = "No symbol mappings found"

    # Field validation errors
    INTERNAL_SYMBOL_INVALID = "Internal symbol must be a non-empty string"
    EXCHANGE_SYMBOL_INVALID = "Exchange symbol must be a non-empty string"
    SYMBOL_REQUIRED = "Symbol must be a non-empty string"
    EXCHANGE_ID_REQUIRED = "Exchange ID must be a non-empty string"

    # Format validation errors
    INTERNAL_SYMBOL_PATTERN = "Internal symbol must be 2-10 uppercase alphanumeric characters"
    EXCHANGE_SYMBOL_PATTERN = "Exchange symbol must be 2-20 alphanumeric/underscore/dash characters"

    # Lookup errors
    EXCHANGE_NOT_SUPPORTED = "Exchange not supported"
    INTERNAL_NOT_FOUND = "Internal symbol not found"
    EXCHANGE_SYMBOL_NOT_FOUND = "Exchange symbol not found"
    SYMBOL_NOT_ON_EXCHANGE = "Symbol not available on exchange"

    # Validation errors
    DUPLICATE_INTERNAL = "Duplicate internal symbol for exchange"
    DUPLICATE_EXCHANGE = "Exchange symbol already maps to different internal symbol"
    SYMBOL_PAIR_FAILED = "Symbol pair validation failed"

    # Metadata validation errors
    SYMBOL_ID_INVALID = "Invalid symbol_id"
    SYMBOL_ID_REQUIRED_BACKPACK = "Backpack symbols require symbol_id"
