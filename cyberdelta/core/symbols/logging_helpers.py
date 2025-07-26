"""Domain-aware structured logging utilities for symbol operations.

This module provides specialized logging helpers that understand symbol domain objects
and can produce rich, structured log entries for better observability and debugging.
"""

from __future__ import annotations

from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import ExchangeSymbol, InternalSymbol, UnifiedSymbol


logger = get_logger(__name__)


class SymbolLogContextBuilder:
    """Builds rich logging context from symbol domain objects."""

    @staticmethod
    def from_internal_symbol(symbol: InternalSymbol, prefix: str = "internal") -> dict[str, Any]:
        """Extract logging context from InternalSymbol.

        Args:
            symbol: Internal symbol domain object
            prefix: Prefix for log field names

        Returns:
            Dictionary of structured log fields
        """
        return {
            f"{prefix}_symbol": symbol.value,
            f"{prefix}_base_asset": symbol.base_asset,
            f"{prefix}_quote_asset": symbol.quote_asset,
            f"{prefix}_market_type": symbol.market_type.value,
            f"{prefix}_canonical_name": symbol.canonical_name,
            f"{prefix}_is_pair": symbol.is_pair,
        }

    @staticmethod
    def from_exchange_symbol(symbol: ExchangeSymbol, prefix: str = "exchange") -> dict[str, Any]:
        """Extract logging context from ExchangeSymbol.

        Args:
            symbol: Exchange symbol domain object
            prefix: Prefix for log field names

        Returns:
            Dictionary of structured log fields
        """
        context: dict[str, Any] = {
            f"{prefix}_symbol": symbol.value,
            f"{prefix}_id": symbol.exchange_id.value,
            f"{prefix}_has_asset_index": symbol.asset_index is not None,
            f"{prefix}_has_symbol_id": symbol.symbol_id is not None,
            f"{prefix}_is_indexed": symbol.is_indexed,
        }

        # Add asset index if available
        if symbol.asset_index is not None:
            context[f"{prefix}_asset_index"] = str(symbol.asset_index)

        # Add symbol ID if available
        if symbol.symbol_id is not None:
            context[f"{prefix}_symbol_id"] = str(symbol.symbol_id)

        # Add internal symbol context if available
        if symbol.internal_symbol:
            internal_context = SymbolLogContextBuilder.from_internal_symbol(
                symbol.internal_symbol, f"{prefix}_internal"
            )
            context.update(internal_context)

        return context

    @staticmethod
    def from_unified_symbol(symbol: UnifiedSymbol, prefix: str = "unified") -> dict[str, Any]:
        """Extract logging context from UnifiedSymbol.

        Args:
            symbol: Unified symbol domain object
            prefix: Prefix for log field names

        Returns:
            Dictionary of structured log fields
        """
        context: dict[str, Any] = {
            f"{prefix}_exchanges": list(symbol.exchange_mappings.keys()),
            f"{prefix}_exchange_count": len(symbol.exchange_mappings),
            f"{prefix}_is_active": symbol.is_active,
            f"{prefix}_is_tradeable": symbol.is_tradeable,
            f"{prefix}_has_tick_size": symbol.tick_size is not None,
            f"{prefix}_has_min_order_size": symbol.min_order_size is not None,
            f"{prefix}_last_updated": symbol.updated_at.isoformat() if symbol.updated_at else None,
        }

        # Add internal symbol context
        internal_context = SymbolLogContextBuilder.from_internal_symbol(
            symbol.internal, f"{prefix}_internal"
        )
        context.update(internal_context)

        # Add trading specifications if available
        if symbol.tick_size is not None:
            context[f"{prefix}_tick_size"] = float(symbol.tick_size)

        if symbol.min_order_size is not None:
            context[f"{prefix}_min_order_size"] = float(symbol.min_order_size)

        if symbol.max_order_size is not None:
            context[f"{prefix}_max_order_size"] = float(symbol.max_order_size)

        return context

    @staticmethod
    def from_symbol_pair(
        long_symbol: ExchangeSymbol,
        short_symbol: ExchangeSymbol,
        internal_symbol: InternalSymbol | None = None,
    ) -> dict[str, Any]:
        """Extract logging context from arbitrage symbol pair.

        Args:
            long_symbol: Long position exchange symbol
            short_symbol: Short position exchange symbol
            internal_symbol: Optional internal symbol for context

        Returns:
            Dictionary of structured log fields
        """
        context: dict[str, Any] = {}

        # Add long symbol context
        long_context = SymbolLogContextBuilder.from_exchange_symbol(long_symbol, "long")
        context.update(long_context)

        # Add short symbol context
        short_context = SymbolLogContextBuilder.from_exchange_symbol(short_symbol, "short")
        context.update(short_context)

        # Add internal symbol context if provided
        if internal_symbol:
            internal_context = SymbolLogContextBuilder.from_internal_symbol(
                internal_symbol, "internal"
            )
            context.update(internal_context)

        # Add pair-specific metadata
        context.update({
            "arbitrage_pair": True,
            "long_exchange": long_symbol.exchange_id.value,
            "short_exchange": short_symbol.exchange_id.value,
            "cross_exchange": long_symbol.exchange_id != short_symbol.exchange_id,
            "both_indexed": long_symbol.is_indexed and short_symbol.is_indexed,
        })

        return context


class SymbolOperationLogger:
    """High-level logging utilities for symbol operations."""

    # Success rate thresholds for batch operations
    HIGH_SUCCESS_THRESHOLD = 95
    GOOD_SUCCESS_THRESHOLD = 80
    PARTIAL_SUCCESS_THRESHOLD = 50

    def __init__(self, logger_name: str | None = None) -> None:
        """Initialize with optional custom logger name."""
        self.logger = get_logger(logger_name or __name__)

    def log_symbol_resolution(
        self,
        operation: str,
        symbol_input: str,
        exchange: str | None = None,
        result: InternalSymbol | ExchangeSymbol | None = None,
        error: Exception | None = None,
        duration_ms: float | None = None,
    ) -> None:
        """Log symbol resolution operation with rich context.

        Args:
            operation: Type of operation (e.g., "exchange_to_internal", "internal_to_exchange")
            symbol_input: Input symbol string
            exchange: Exchange name if applicable
            result: Resolved symbol object if successful
            error: Exception if operation failed
            duration_ms: Operation duration in milliseconds
        """
        base_context: dict[str, Any] = {
            "operation": operation,
            "symbol_input": symbol_input,
            "exchange": exchange,
            "success": error is None,
            "duration_ms": duration_ms,
        }

        if result:
            if isinstance(result, InternalSymbol):
                symbol_context = SymbolLogContextBuilder.from_internal_symbol(result, "result")
            else:  # Must be ExchangeSymbol based on type hint
                symbol_context = SymbolLogContextBuilder.from_exchange_symbol(result, "result")

            base_context.update(symbol_context)

            self.logger.info("symbol_resolution_completed", **base_context)

        elif error:
            base_context.update({
                "error": str(error),
                "error_type": type(error).__name__,
            })

            self.logger.warning("symbol_resolution_failed", **base_context)

        else:
            self.logger.debug("symbol_resolution_attempted", **base_context)

    def log_arbitrage_validation(
        self,
        internal_symbol: str,
        exchanges: list[str],
        compatible_exchanges: list[str],
        incompatible_exchanges: list[str],
        is_compatible: bool,
        validation_errors: list[str] | None = None,
    ) -> None:
        """Log arbitrage compatibility validation with detailed context.

        Args:
            internal_symbol: Internal symbol being validated
            exchanges: All exchanges being tested
            compatible_exchanges: Exchanges where symbol is available
            incompatible_exchanges: Exchanges where symbol is unavailable
            is_compatible: Whether symbol supports arbitrage across all exchanges
            validation_errors: Optional list of validation errors
        """
        context: dict[str, Any] = {
            "internal_symbol": internal_symbol,
            "requested_exchanges": exchanges,
            "compatible_exchanges": compatible_exchanges,
            "incompatible_exchanges": incompatible_exchanges,
            "total_exchanges": len(exchanges),
            "compatible_count": len(compatible_exchanges),
            "incompatible_count": len(incompatible_exchanges),
            "is_arbitrage_compatible": is_compatible,
            "compatibility_percentage": (
                (len(compatible_exchanges) / len(exchanges)) * 100 if exchanges else 0
            ),
        }

        if validation_errors:
            context["validation_errors"] = validation_errors

        if is_compatible:
            self.logger.info("arbitrage_validation_passed", **context)
        else:
            self.logger.warning("arbitrage_validation_failed", **context)

    def log_batch_operation(
        self,
        operation: str,
        total_items: int,
        successful_items: int,
        failed_items: int,
        duration_ms: float | None = None,
        exchange: str | None = None,
    ) -> None:
        """Log batch symbol operation with statistics.

        Args:
            operation: Type of batch operation
            total_items: Total number of items processed
            successful_items: Number of successful items
            failed_items: Number of failed items
            duration_ms: Total operation duration in milliseconds
            exchange: Exchange name if applicable
        """
        success_rate = (successful_items / total_items) * 100 if total_items > 0 else 0

        context: dict[str, Any] = {
            "operation": operation,
            "total_items": total_items,
            "successful_items": successful_items,
            "failed_items": failed_items,
            "success_rate": round(success_rate, 2),
            "duration_ms": duration_ms,
            "exchange": exchange,
        }

        if duration_ms and total_items > 0:
            context["avg_duration_per_item_ms"] = round(duration_ms / total_items, 2)

        if success_rate >= self.HIGH_SUCCESS_THRESHOLD:
            self.logger.info("batch_operation_highly_successful", **context)
        elif success_rate >= self.GOOD_SUCCESS_THRESHOLD:
            self.logger.info("batch_operation_mostly_successful", **context)
        elif success_rate >= self.PARTIAL_SUCCESS_THRESHOLD:
            self.logger.warning("batch_operation_partially_successful", **context)
        else:
            self.logger.error("batch_operation_mostly_failed", **context)

    def log_symbol_registration(self, symbol: UnifiedSymbol, operation: str = "register") -> None:
        """Log symbol registration with full context.

        Args:
            symbol: Unified symbol being registered
            operation: Type of registration operation
        """
        context = SymbolLogContextBuilder.from_unified_symbol(symbol)
        context["operation"] = operation

        self.logger.info("symbol_registration", **context)

    def log_cache_operation(
        self,
        operation: str,
        hit: bool,
        key: str,
        cache_size: int | None = None,
        duration_ms: float | None = None,
    ) -> None:
        """Log cache operation for symbol lookups.

        Args:
            operation: Type of cache operation
            hit: Whether it was a cache hit
            key: Cache key used
            cache_size: Current cache size
            duration_ms: Operation duration in milliseconds
        """
        context: dict[str, Any] = {
            "operation": operation,
            "cache_hit": hit,
            "cache_key": key,
            "cache_size": cache_size,
            "duration_ms": duration_ms,
        }

        if hit:
            self.logger.debug("symbol_cache_hit", **context)
        else:
            self.logger.debug("symbol_cache_miss", **context)


# Convenience functions for direct usage
def log_symbol_context(
    message: str,
    symbol: InternalSymbol | ExchangeSymbol | UnifiedSymbol,
    level: str = "info",
    logger_name: str | None = None,
    **additional_context: str | float | bool | None,
) -> None:
    """Log a message with rich symbol context.

    Args:
        message: Log message
        symbol: Symbol domain object
        level: Log level (debug, info, warning, error)
        logger_name: Optional logger name
        **additional_context: Additional context fields
    """
    log = get_logger(logger_name or __name__)

    # Build context based on symbol type
    if isinstance(symbol, InternalSymbol):
        context = SymbolLogContextBuilder.from_internal_symbol(symbol)
    elif isinstance(symbol, ExchangeSymbol):
        context = SymbolLogContextBuilder.from_exchange_symbol(symbol)
    else:  # Must be UnifiedSymbol based on function signature
        context = SymbolLogContextBuilder.from_unified_symbol(symbol)

    # Add any additional context
    context.update(additional_context)

    # Log at appropriate level
    log_method = getattr(log, level, log.info)
    log_method(message, **context)


def create_operation_logger(component_name: str) -> SymbolOperationLogger:
    """Create a symbol operation logger for a specific component.

    Args:
        component_name: Name of the component using the logger

    Returns:
        Configured SymbolOperationLogger instance
    """
    logger_name = f"symbols.{component_name}"
    return SymbolOperationLogger(logger_name)
