"""Symbol system exceptions.

This module defines custom exceptions for the unified symbol system,
providing clear error messages and structured error information.
"""

from typing import Any


class SymbolError(Exception):
    """Base exception for all symbol-related errors."""

    def __init__(
        self,
        message: str,
        error_code: str = "SYMBOL_ERROR",
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize symbol error.

        Args:
            message: Error message
            error_code: Unique error code for this error type
            details: Additional error details
        """
        super().__init__(message)
        self.error_code = error_code
        self.details = details or {}


class SymbolValidationError(SymbolError):
    """Raised when symbol validation fails."""

    def __init__(
        self,
        symbol: str,
        reason: str,
        expected_format: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize symbol validation error.

        Args:
            symbol: The invalid symbol
            reason: Reason for validation failure
            expected_format: Expected symbol format (if applicable)
            details: Additional error details
        """
        message = f"Symbol validation failed for '{symbol}': {reason}"
        if expected_format:
            message += f" (expected format: {expected_format})"

        error_details = {"symbol": symbol, "reason": reason}
        if expected_format:
            error_details["expected_format"] = expected_format
        if details:
            error_details.update(details)

        super().__init__(
            message=message,
            error_code="SYMBOL_VALIDATION_ERROR",
            details=error_details,
        )


class SymbolNotFoundError(SymbolError):
    """Raised when a symbol cannot be found."""

    def __init__(
        self,
        symbol: str,
        context: str,
        available_symbols: list[str] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize symbol not found error.

        Args:
            symbol: The symbol that was not found
            context: Context where symbol was searched (e.g., "registry", "exchange")
            available_symbols: List of available symbols (for suggestions)
            details: Additional error details
        """
        message = f"Symbol '{symbol}' not found in {context}"

        error_details: dict[str, Any] = {"symbol": symbol, "context": context}
        if available_symbols:
            error_details["available_symbols"] = available_symbols[:10]  # Limit suggestions

        if details:
            error_details.update(details)

        super().__init__(
            message=message,
            error_code="SYMBOL_NOT_FOUND",
            details=error_details,
        )


class SymbolMappingError(SymbolError):
    """Raised when symbol mapping fails."""

    def __init__(
        self,
        internal_symbol: str,
        exchange: str,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize symbol mapping error.

        Args:
            internal_symbol: The internal symbol
            exchange: The exchange where mapping failed
            reason: Reason for mapping failure
            details: Additional error details
        """
        message = f"Failed to map symbol '{internal_symbol}' for exchange '{exchange}': {reason}"

        error_details = {
            "internal_symbol": internal_symbol,
            "exchange": exchange,
            "reason": reason,
        }
        if details:
            error_details.update(details)

        super().__init__(
            message=message,
            error_code="SYMBOL_MAPPING_ERROR",
            details=error_details,
        )


class SymbolRegistryError(SymbolError):
    """Raised when symbol registry operations fail."""

    def __init__(
        self,
        operation: str,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize symbol registry error.

        Args:
            operation: The operation that failed
            reason: Reason for failure
            details: Additional error details
        """
        message = f"Symbol registry operation '{operation}' failed: {reason}"

        error_details = {"operation": operation, "reason": reason}
        if details:
            error_details.update(details)

        super().__init__(
            message=message,
            error_code="SYMBOL_REGISTRY_ERROR",
            details=error_details,
        )


class SymbolCacheError(SymbolError):
    """Raised when symbol cache operations fail."""

    def __init__(
        self,
        operation: str,
        reason: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Initialize symbol cache error.

        Args:
            operation: The cache operation that failed
            reason: Reason for failure
            details: Additional error details
        """
        message = f"Symbol cache operation '{operation}' failed: {reason}"

        error_details = {"operation": operation, "reason": reason}
        if details:
            error_details.update(details)

        super().__init__(
            message=message,
            error_code="SYMBOL_CACHE_ERROR",
            details=error_details,
        )


