"""Domain-aware helpers for working with symbol models.

This module provides utilities that bridge between string-based legacy code
and the rich domain models, while encouraging migration to proper domain usage.
"""

from __future__ import annotations

from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.exceptions import SymbolError
from cyberdelta.core.symbols.models import ExchangeSymbol, InternalSymbol, UnifiedSymbol
from cyberdelta.core.symbols.service import SymbolService


logger = get_logger(__name__)


class SymbolDomainHelpers:
    """Domain-aware helpers for working with symbol models.

    This class provides a bridge between legacy string-based code and
    the rich domain models, with proper error handling and logging.
    """

    def __init__(self, service: SymbolService | None = None) -> None:
        """Initialize domain helpers.

        Args:
            service: SymbolService instance, creates new one if None
        """
        self.service = service or SymbolService()

    def resolve_for_exchange(self, internal: str, exchange: str) -> ExchangeSymbol | None:
        """Get exchange symbol with proper error handling.

        Args:
            internal: Internal symbol name (e.g., "BTC")
            exchange: Exchange identifier (e.g., "hyperliquid")

        Returns:
            ExchangeSymbol object if found, None otherwise
        """
        try:
            exchange_symbol = self.service.get_exchange_symbol(internal, exchange.lower())
        except (KeyError, AttributeError, ValueError, SymbolError) as e:
            logger.debug(
                "symbol_resolution_failed",
                internal=internal,
                exchange=exchange,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None
        else:
            logger.debug(
                "symbol_resolved_for_exchange",
                internal=internal,
                exchange=exchange,
                exchange_symbol=exchange_symbol.value,
                has_asset_index=exchange_symbol.asset_index is not None,
                has_symbol_id=exchange_symbol.symbol_id is not None,
            )
            return exchange_symbol

    def resolve_from_exchange(self, exchange_symbol: str, exchange: str) -> InternalSymbol | None:
        """Get internal symbol with proper error handling.

        Args:
            exchange_symbol: Exchange-specific symbol (e.g., "BTC-PERP")
            exchange: Exchange identifier (e.g., "hyperliquid")

        Returns:
            InternalSymbol object if found, None otherwise
        """
        try:
            internal_symbol = self.service.get_internal_symbol(exchange_symbol, exchange.lower())
        except (KeyError, AttributeError, ValueError, SymbolError) as e:
            logger.debug(
                "symbol_resolution_failed",
                exchange_symbol=exchange_symbol,
                exchange=exchange,
                error=str(e),
                error_type=type(e).__name__,
            )
            return None
        else:
            logger.debug(
                "symbol_resolved_from_exchange",
                exchange_symbol=exchange_symbol,
                exchange=exchange,
                internal=internal_symbol.value,
                base_asset=internal_symbol.base_asset,
                quote_asset=internal_symbol.quote_asset,
                market_type=internal_symbol.market_type.value,
            )
            return internal_symbol

    def validate_arbitrage_pair(
        self, internal: str, long_ex: str, short_ex: str
    ) -> tuple[bool, list[str]]:
        """Validate arbitrage pair with detailed feedback.

        Args:
            internal: Internal symbol name
            long_ex: Long exchange identifier
            short_ex: Short exchange identifier

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors: list[str] = []

        long_symbol = self.resolve_for_exchange(internal, long_ex)
        if not long_symbol:
            errors.append(f"Symbol {internal} not available on {long_ex} (long exchange)")

        short_symbol = self.resolve_for_exchange(internal, short_ex)
        if not short_symbol:
            errors.append(f"Symbol {internal} not available on {short_ex} (short exchange)")

        is_valid = len(errors) == 0

        if is_valid:
            logger.debug(
                "arbitrage_pair_validated",
                internal=internal,
                long_exchange=long_ex,
                short_exchange=short_ex,
                long_symbol=long_symbol.value if long_symbol else None,
                short_symbol=short_symbol.value if short_symbol else None,
            )
        else:
            logger.warning(
                "arbitrage_pair_validation_failed",
                internal=internal,
                long_exchange=long_ex,
                short_exchange=short_ex,
                errors=errors,
            )

        return is_valid, errors

    def get_symbol_overview(self, internal: str) -> dict[str, Any] | None:
        """Get comprehensive symbol information.

        Args:
            internal: Internal symbol name

        Returns:
            Dictionary with complete symbol information, None if not found
        """
        unified = self.service.store.get_by_internal(internal)
        if not unified:
            logger.debug("symbol_overview_not_found", internal=internal)
            return None

        overview: dict[str, Any] = {
            "internal": {
                "value": unified.internal.value,
                "base_asset": unified.internal.base_asset,
                "quote_asset": unified.internal.quote_asset,
                "market_type": unified.internal.market_type.value,
                "is_pair": unified.internal.is_pair,
                "canonical_name": unified.internal.canonical_name,
            },
            "exchanges": {
                exchange_id: {
                    "value": exchange_symbol.value,
                    "exchange_id": exchange_symbol.exchange_id.value,
                    "asset_index": exchange_symbol.asset_index,
                    "symbol_id": exchange_symbol.symbol_id,
                    "is_indexed": exchange_symbol.is_indexed,
                }
                for exchange_id, exchange_symbol in unified.exchange_mappings.items()
            },
            "trading_specs": {
                "tick_size": unified.tick_size,
                "min_order_size": unified.min_order_size,
                "max_order_size": unified.max_order_size,
                "lot_size": unified.lot_size,
            },
            "status": {
                "is_active": unified.is_active,
                "is_tradeable": unified.is_tradeable,
            },
            "timestamps": {
                "created_at": unified.created_at.isoformat(),
                "updated_at": unified.updated_at.isoformat(),
            },
        }

        logger.debug(
            "symbol_overview_generated",
            internal=internal,
            exchange_count=len(overview["exchanges"]),
            is_tradeable=overview["status"]["is_tradeable"],
        )

        return overview

    def get_supported_exchanges_for_symbol(self, internal: str) -> list[str]:
        """Get list of exchanges that support this symbol.

        Args:
            internal: Internal symbol name

        Returns:
            List of exchange identifiers that support this symbol
        """
        unified = self.service.store.get_by_internal(internal)
        if not unified:
            return []

        exchanges = list(unified.exchange_mappings.keys())
        logger.debug(
            "supported_exchanges_retrieved",
            internal=internal,
            exchanges=exchanges,
            count=len(exchanges),
        )
        return exchanges

    def is_symbol_supported_on_exchange(self, internal: str, exchange: str) -> bool:
        """Check if symbol is supported on specific exchange.

        Args:
            internal: Internal symbol name
            exchange: Exchange identifier

        Returns:
            True if supported, False otherwise
        """
        result = self.resolve_for_exchange(internal, exchange) is not None
        logger.debug(
            "symbol_support_checked",
            internal=internal,
            exchange=exchange,
            supported=result,
        )
        return result

    def get_exchange_symbols_mapping(self, internal: str) -> dict[str, str]:
        """Get mapping of exchange IDs to exchange symbol values.

        Args:
            internal: Internal symbol name

        Returns:
            Dictionary mapping exchange_id -> exchange_symbol_value
        """
        unified = self.service.store.get_by_internal(internal)
        if not unified:
            return {}

        mapping = {
            exchange_id: exchange_symbol.value
            for exchange_id, exchange_symbol in unified.exchange_mappings.items()
        }

        logger.debug(
            "exchange_symbols_mapping_generated",
            internal=internal,
            mapping=mapping,
        )

        return mapping

    def get_symbol_coverage(self, internal: str) -> dict[str, bool]:
        """Get symbol availability across all exchanges.

        Args:
            internal: Internal symbol name

        Returns:
            Dictionary mapping exchange_id -> is_supported
        """
        all_exchanges = self.service.get_supported_exchanges()
        coverage: dict[str, bool] = {}

        for exchange_id in all_exchanges:
            coverage[exchange_id] = self.is_symbol_supported_on_exchange(internal, exchange_id)

        supported_count = sum(1 for supported in coverage.values() if supported)

        logger.debug(
            "symbol_coverage_calculated",
            internal=internal,
            total_exchanges=len(coverage),
            supported_exchanges=supported_count,
            coverage_percentage=(
                f"{(supported_count / len(coverage) * 100):.1f}%" if coverage else "0%"
            ),
        )

        return coverage

    def format_symbol_for_logging(
        self, symbol: ExchangeSymbol | InternalSymbol | UnifiedSymbol
    ) -> dict[str, Any]:
        """Format symbol object for structured logging.

        Args:
            symbol: Any symbol domain object

        Returns:
            Dictionary with structured symbol data for logging
        """
        if isinstance(symbol, ExchangeSymbol):
            return {
                "type": "exchange",
                "value": symbol.value,
                "exchange_id": symbol.exchange_id.value,
                "asset_index": symbol.asset_index,
                "symbol_id": symbol.symbol_id,
                "is_indexed": symbol.is_indexed,
                "internal_value": symbol.internal_symbol.value if symbol.internal_symbol else None,
            }
        if isinstance(symbol, InternalSymbol):
            return {
                "type": "internal",
                "value": symbol.value,
                "base_asset": symbol.base_asset,
                "quote_asset": symbol.quote_asset,
                "market_type": symbol.market_type.value,
                "is_pair": symbol.is_pair,
                "canonical_name": symbol.canonical_name,
            }
        # Must be UnifiedSymbol based on type hints
        return {
            "type": "unified",
            "internal_value": symbol.internal.value,
            "exchange_count": len(symbol.exchange_mappings),
            "supported_exchanges": list(symbol.exchange_mappings.keys()),
            "is_active": symbol.is_active,
            "is_tradeable": symbol.is_tradeable,
        }


# Convenience function for quick access
def get_domain_helpers(service: SymbolService | None = None) -> SymbolDomainHelpers:
    """Get a configured SymbolDomainHelpers instance.

    Args:
        service: Optional SymbolService instance

    Returns:
        Configured SymbolDomainHelpers instance
    """
    return SymbolDomainHelpers(service)
