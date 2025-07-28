"""Migration utilities for converting from string-based to domain model usage.

This module provides helper functions and patterns to make the migration
from primitive string handling to rich domain objects easier and safer.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.exceptions import SymbolValidationError
from cyberdelta.core.symbols.helpers import SymbolDomainHelpers, get_domain_helpers
from cyberdelta.core.symbols.models import ExchangeSymbol, InternalSymbol, UnifiedSymbol


logger = get_logger(__name__)

T = TypeVar("T")


class LegacyBridge:
    """Bridge for legacy code that expects string returns."""

    def __init__(self, helpers: SymbolDomainHelpers) -> None:
        """Initialize legacy bridge with domain helpers.

        Args:
            helpers: Configured domain helpers instance
        """
        self.helpers = helpers

    def get_exchange_symbol_value(self, internal: str, exchange: str) -> str | None:
        """Get exchange symbol value as string (legacy compatibility).

        Args:
            internal: Internal symbol name
            exchange: Exchange identifier

        Returns:
            Exchange symbol value as string, None if not found
        """
        exchange_symbol = self.helpers.resolve_for_exchange(internal, exchange)
        result = exchange_symbol.value if exchange_symbol else None

        logger.debug(
            "legacy_exchange_symbol_resolved",
            internal=internal,
            exchange=exchange,
            result=result,
            found=result is not None,
        )

        return result

    def get_internal_symbol_value(self, exchange_symbol: str, exchange: str) -> str | None:
        """Get internal symbol value as string (legacy compatibility).

        Args:
            exchange_symbol: Exchange-specific symbol
            exchange: Exchange identifier

        Returns:
            Internal symbol value as string, None if not found
        """
        internal_symbol = self.helpers.resolve_from_exchange(exchange_symbol, exchange)
        result = internal_symbol.value if internal_symbol else None

        logger.debug(
            "legacy_internal_symbol_resolved",
            exchange_symbol=exchange_symbol,
            exchange=exchange,
            result=result,
            found=result is not None,
        )

        return result

    def is_symbol_supported(self, internal: str, exchange: str) -> bool:
        """Check if symbol is supported (legacy compatibility).

        Args:
            internal: Internal symbol name
            exchange: Exchange identifier

        Returns:
            True if supported, False otherwise
        """
        return self.helpers.is_symbol_supported_on_exchange(internal, exchange)

    def validate_symbol_pair(self, internal: str, long_exchange: str, short_exchange: str) -> None:
        """Validate symbol pair (legacy compatibility).

        Args:
            internal: Internal symbol name
            long_exchange: Long exchange identifier
            short_exchange: Short exchange identifier

        Raises:
            SymbolValidationError: If validation fails
        """
        is_valid, errors = self.helpers.validate_arbitrage_pair(
            internal, long_exchange, short_exchange
        )
        if not is_valid:
            error_details = "; ".join(errors)
            raise SymbolValidationError(
                symbol=internal,
                reason="Symbol pair validation failed",
                details={"error_details": error_details},
            )


def safe_domain_call(
    domain_func: Callable[..., T],
    *args: object,
    fallback: T | None = None,
    log_errors: bool = True,
    **kwargs: object,
) -> T | None:
    """Safely call a domain function with error handling.

    Args:
        domain_func: Function that works with domain objects
        *args: Positional arguments for the function
        fallback: Value to return on error
        log_errors: Whether to log errors
        **kwargs: Keyword arguments for the function

    Returns:
        Function result or fallback value on error
    """
    try:
        return domain_func(*args, **kwargs)
    except (KeyError, AttributeError, ValueError, TypeError) as e:
        if log_errors:
            logger.warning(
                "domain_function_call_failed",
                function_name=domain_func.__name__,
                args=str(args)[:200],  # Truncate for logging
                error=str(e),
                error_type=type(e).__name__,
            )
        return fallback


def migrate_string_to_domain(
    string_value: str | None,
    resolver_func: Callable[[str], ExchangeSymbol | InternalSymbol | None],
    context: str = "unknown",
) -> ExchangeSymbol | InternalSymbol | None:
    """Migrate from string value to domain object.

    Args:
        string_value: Legacy string value
        resolver_func: Function to resolve string to domain object
        context: Context for logging

    Returns:
        Domain object or None if resolution fails
    """
    if not string_value:
        return None

    try:
        result = resolver_func(string_value)
    except (KeyError, AttributeError, ValueError, TypeError) as e:
        logger.warning(
            "string_to_domain_migration_failed",
            context=context,
            string_value=string_value,
            error=str(e),
            error_type=type(e).__name__,
        )
        return None
    else:
        logger.debug(
            "string_to_domain_migration",
            context=context,
            string_value=string_value,
            resolved=result is not None,
            domain_type=type(result).__name__ if result else None,
        )
        return result


def create_migration_wrapper(legacy_function: Callable[..., Any]) -> Callable[..., Any]:
    """Create a wrapper that logs migration usage.

    Args:
        legacy_function: Function that still uses string-based approach

    Returns:
        Wrapped function with migration logging
    """

    def wrapper(*args: object, **kwargs: object) -> object:
        logger.debug(
            "legacy_function_usage",
            function_name=legacy_function.__name__,
            module=legacy_function.__module__,
            args_count=len(args),
            kwargs_keys=list(kwargs.keys()),
        )
        return legacy_function(*args, **kwargs)

    return wrapper


def extract_symbol_value(symbol_obj: ExchangeSymbol | InternalSymbol | str | None) -> str | None:
    """Extract string value from symbol object or string.

    Args:
        symbol_obj: Symbol object or string

    Returns:
        String value or None
    """
    if symbol_obj is None:
        return None
    if isinstance(symbol_obj, str):
        return symbol_obj
    if hasattr(symbol_obj, "value"):
        return symbol_obj.value
    logger.warning(
        "unknown_symbol_object_type",
        type_name=type(symbol_obj).__name__,
        str_representation=str(symbol_obj)[:100],
    )
    return str(symbol_obj)


def batch_migrate_symbols(
    symbol_strings: list[str],
    resolver_func: Callable[[str], ExchangeSymbol | InternalSymbol | None],
    context: str = "batch_migration",
) -> tuple[list[ExchangeSymbol | InternalSymbol | UnifiedSymbol], list[str]]:
    """Migrate a batch of string symbols to domain objects.

    Args:
        symbol_strings: List of string symbols to migrate
        resolver_func: Function to resolve each string
        context: Context for logging

    Returns:
        Tuple of (successful_objects, failed_strings)
    """
    successful: list[InternalSymbol | ExchangeSymbol | UnifiedSymbol] = []
    failed: list[str] = []

    for symbol_str in symbol_strings:
        domain_obj = migrate_string_to_domain(symbol_str, resolver_func, context)
        if domain_obj:
            successful.append(domain_obj)
        else:
            failed.append(symbol_str)

    logger.info(
        "batch_symbol_migration_completed",
        context=context,
        total_symbols=len(symbol_strings),
        successful_count=len(successful),
        failed_count=len(failed),
        success_rate=(
            f"{len(successful) / len(symbol_strings) * 100:.1f}%" if symbol_strings else "0%"
        ),
    )

    return successful, failed


class ValidationHelper:
    """Helper for validating during migration."""

    @staticmethod
    def validate_domain_object_integrity(obj: ExchangeSymbol | InternalSymbol) -> bool:
        """Validate that domain object has required fields.

        Args:
            obj: Domain object to validate

        Returns:
            True if object is valid
        """
        try:
            if isinstance(obj, ExchangeSymbol):
                return bool(obj.value) and (
                    obj.internal_symbol is None or bool(obj.internal_symbol.value)
                )
            # Must be InternalSymbol based on type hints
            return bool(obj.value) and bool(obj.base_asset)
        except (AttributeError, ValueError):
            return False

    @staticmethod
    def compare_string_and_domain(
        string_value: str, domain_obj: ExchangeSymbol | InternalSymbol, context: str = "comparison"
    ) -> bool:
        """Compare string value with domain object value.

        Args:
            string_value: Legacy string value
            domain_obj: Domain object to compare
            context: Context for logging

        Returns:
            True if values match
        """
        domain_value = extract_symbol_value(domain_obj)
        match = string_value == domain_value

        if not match:
            logger.warning(
                "string_domain_value_mismatch",
                context=context,
                string_value=string_value,
                domain_value=domain_value,
                domain_type=type(domain_obj).__name__,
            )

        return match


# Convenience functions for common migration patterns
def create_legacy_bridge(helpers: SymbolDomainHelpers | None = None) -> LegacyBridge:
    """Create a legacy bridge with optional helpers.

    Args:
        helpers: Optional pre-configured helpers

    Returns:
        Configured legacy bridge
    """
    return LegacyBridge(helpers or get_domain_helpers())


def log_migration_progress(component_name: str, step: str, success: bool = True) -> None:
    """Log migration progress for tracking.

    Args:
        component_name: Name of component being migrated
        step: Description of migration step
        success: Whether step was successful
    """
    logger.info(
        "migration_progress",
        component=component_name,
        step=step,
        success=success,
        level="INFO" if success else "WARNING",
    )
