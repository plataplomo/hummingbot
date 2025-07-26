"""Symbol application service - orchestrates all symbol operations with clean API.

This module provides the main service interface for all symbol operations,
coordinating between storage and transformation layers using protocols
for dependency injection and testing.
"""

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols.exceptions import SymbolNotFoundError
from cyberdelta.core.symbols.models import (
    ExchangeSymbol,
    InternalSymbol,
    UnifiedSymbol,
    create_exchange_symbol,
    create_internal_symbol,
)
from cyberdelta.core.symbols.operation_results import (
    SymbolArbitrageCompatibility,
    SymbolBatchTransformResult,
)
from cyberdelta.core.symbols.protocols import SymbolStoreProtocol
from cyberdelta.core.symbols.store import SymbolStore
from cyberdelta.core.symbols.transformers import SYMBOL_TRANSFORMERS
from cyberdelta.enums.exchange_names import ExchangeName


logger = get_logger(__name__)


class SymbolService:
    """Symbol service - orchestrates all symbol operations with clean API."""

    # Constants for symbol parsing
    MIN_SYMBOL_PARTS = 2

    def __init__(self, store: SymbolStoreProtocol | None = None) -> None:
        """Initialize SymbolService with optional store dependency."""
        # Protocol-based dependency injection for maximum testability
        self.store: SymbolStoreProtocol = store or SymbolStore()

    def get_internal_symbol(self, exchange_symbol: str, exchange_name: str) -> InternalSymbol:
        """Transform exchange symbol to internal canonical format.

        Args:
            exchange_symbol: Exchange-specific symbol (e.g., "BTC-PERP", "BTC_PERP")
            exchange_name: Exchange name (e.g., "hyperliquid", "backpack")

        Returns:
            InternalSymbol: Canonical internal representation

        Raises:
            SymbolNotFoundError: If exchange not supported or transformation fails
        """
        logger.debug(
            "symbol_resolution_started",
            operation="exchange_to_internal",
            exchange_symbol=exchange_symbol,
            exchange_name=exchange_name,
            supported_exchanges=list(SYMBOL_TRANSFORMERS.keys()),
        )

        # 1. Try registered symbols first (fastest path for known symbols)
        unified = self.store.get_by_exchange(exchange_symbol, exchange_name.lower())
        if unified:
            logger.debug(
                "symbol_resolved_from_cache",
                exchange_symbol=exchange_symbol,
                exchange_name=exchange_name,
                internal_symbol=unified.internal.value,
                base_asset=unified.internal.base_asset,
                market_type=unified.internal.market_type.value,
            )
            return unified.internal

        # 2. Transform using symbol transformer (extensible fallback)
        transformer = SYMBOL_TRANSFORMERS.get(exchange_name.lower())
        if not transformer:
            error_msg = (
                f"Exchange '{exchange_name}' is not supported for symbol transformation. "
                f"Available exchanges: {sorted(SYMBOL_TRANSFORMERS.keys())}. "
                f"Symbol: '{exchange_symbol}'"
            )
            logger.warning(
                "unsupported_exchange_for_symbol",
                exchange_symbol=exchange_symbol,
                exchange_name=exchange_name,
                supported_exchanges=list(SYMBOL_TRANSFORMERS.keys()),
                error_context="transformer_not_found",
            )
            raise SymbolNotFoundError(
                symbol=exchange_symbol,
                context="transformer_not_found",
                details={"error_msg": error_msg},
            )

        try:
            internal_symbol = transformer.exchange_to_internal(exchange_symbol)
        except (ValueError, KeyError, AttributeError) as e:
            error_msg = f"Failed to transform symbol '{exchange_symbol}' from {exchange_name}"
            logger.exception(
                "symbol_transformation_failed",
                exchange_symbol=exchange_symbol,
                exchange_name=exchange_name,
                transformer_class=transformer.__class__.__name__,
                error=str(e),
                error_type=type(e).__name__,
                error_context="exchange_to_internal_transform",
            )
            raise SymbolNotFoundError(
                symbol=exchange_symbol,
                context="exchange_to_internal_transform",
                details={"error_msg": error_msg},
            ) from e
        else:
            logger.debug(
                "symbol_transformed_to_internal",
                exchange_symbol=exchange_symbol,
                exchange_name=exchange_name,
                internal_symbol=internal_symbol.value,
                base_asset=internal_symbol.base_asset,
                quote_asset=internal_symbol.quote_asset,
                market_type=internal_symbol.market_type.value,
                transformer_class=transformer.__class__.__name__,
            )
            return internal_symbol

    def get_exchange_symbol(self, internal_symbol: str, exchange_name: str) -> ExchangeSymbol:
        """Transform internal symbol to exchange-specific format.

        Args:
            internal_symbol: Internal canonical symbol (e.g., "BTC_USD")
            exchange_name: Target exchange (e.g., "hyperliquid", "backpack")

        Returns:
            ExchangeSymbol: Exchange-specific symbol with metadata

        Raises:
            SymbolNotFoundError: If exchange not supported or transformation fails
        """
        logger.debug(
            "symbol_resolution_started",
            operation="internal_to_exchange",
            internal_symbol=internal_symbol,
            exchange_name=exchange_name,
            supported_exchanges=list(SYMBOL_TRANSFORMERS.keys()),
        )

        # 1. Try registered symbols first (fastest path)
        unified = self.store.get_by_internal(internal_symbol)
        if unified and exchange_name.lower() in unified.exchange_mappings:
            exchange_symbol = unified.exchange_mappings[exchange_name.lower()]
            logger.debug(
                "symbol_resolved_from_cache",
                internal_symbol=internal_symbol,
                exchange_name=exchange_name,
                exchange_symbol=exchange_symbol.value,
                exchange_id=exchange_symbol.exchange_id.value,
                has_asset_index=exchange_symbol.asset_index is not None,
                has_symbol_id=exchange_symbol.symbol_id is not None,
            )
            return exchange_symbol

        # 2. Transform using symbol transformer
        transformer = SYMBOL_TRANSFORMERS.get(exchange_name.lower())
        if not transformer:
            error_msg = (
                f"Exchange '{exchange_name}' is not supported for symbol transformation. "
                f"Available exchanges: {sorted(SYMBOL_TRANSFORMERS.keys())}. "
                f"Internal symbol: '{internal_symbol}'"
            )
            logger.warning(
                "unsupported_exchange_for_symbol",
                internal_symbol=internal_symbol,
                exchange_name=exchange_name,
                supported_exchanges=list(SYMBOL_TRANSFORMERS.keys()),
                error_context="transformer_not_found",
            )
            raise SymbolNotFoundError(
                symbol=internal_symbol,
                context="transformer_not_found",
                details={"error_msg": error_msg},
            )

        try:
            # Parse internal symbol back to domain object for transformation
            internal_obj = self._parse_internal_symbol(internal_symbol)
            exchange_value = transformer.internal_to_exchange(internal_obj)

            exchange_symbol = create_exchange_symbol(
                value=exchange_value,
                exchange_id=ExchangeName(exchange_name.upper()),
                internal_symbol=internal_obj,
            )
        except Exception as e:
            error_msg = (
                f"Failed to transform internal symbol '{internal_symbol}' to "
                f"{exchange_name} format. Error: {e!s}. "
                f"Transformer: {transformer.__class__.__name__}"
            )
            logger.exception(
                "symbol_transformation_failed",
                internal_symbol=internal_symbol,
                exchange_name=exchange_name,
                transformer_class=transformer.__class__.__name__,
                error=str(e),
                error_type=type(e).__name__,
                error_context="internal_to_exchange_transform",
            )
            raise SymbolNotFoundError(
                symbol=internal_symbol,
                context="internal_to_exchange_transform",
                details={"error_msg": error_msg},
            ) from e
        else:
            logger.debug(
                "symbol_transformed_to_exchange",
                internal_symbol=internal_symbol,
                exchange_name=exchange_name,
                exchange_symbol=exchange_symbol.value,
                exchange_id=exchange_symbol.exchange_id.value,
                base_asset=internal_obj.base_asset,
                quote_asset=internal_obj.quote_asset,
                market_type=internal_obj.market_type.value,
                transformer_class=transformer.__class__.__name__,
            )

            return exchange_symbol

    def batch_transform_symbols(
        self, exchange_symbols: list[str], exchange_name: str
    ) -> SymbolBatchTransformResult:
        """Transform multiple symbols in batch for better performance.

        Returns type-safe result with success/failure statistics.
        """
        logger.info(
            "batch_symbol_transformation_started",
            symbol_count=len(exchange_symbols),
            exchange_name=exchange_name,
            operation="batch_exchange_to_internal",
        )

        result = SymbolBatchTransformResult()

        for symbol in exchange_symbols:
            try:
                internal = self.get_internal_symbol(symbol, exchange_name)
                result.successful_transforms.append((symbol, internal))
            except SymbolNotFoundError as e:
                result.failed_transforms.append((symbol, str(e)))

        logger.info(
            "batch_symbol_transformation_completed",
            total_symbols=len(exchange_symbols),
            successful_count=len(result.successful_transforms),
            failed_count=len(result.failed_transforms),
            success_rate=result.success_rate,
            exchange_name=exchange_name,
        )

        return result

    def validate_arbitrage_compatibility(
        self, internal_symbol: str, exchange_names: list[str]
    ) -> SymbolArbitrageCompatibility:
        """Validate if symbol supports arbitrage across multiple exchanges."""
        logger.info(
            "arbitrage_compatibility_validation_started",
            internal_symbol=internal_symbol,
            exchange_names=exchange_names,
            exchange_count=len(exchange_names),
        )

        result = SymbolArbitrageCompatibility(
            is_arbitrage_compatible=True, exchange_availability={}, compatibility_warnings=[]
        )

        for exchange_name in exchange_names:
            try:
                exchange_symbol = self.get_exchange_symbol(internal_symbol, exchange_name)
                result.exchange_availability[exchange_name] = {
                    "available": True,
                    "symbol": exchange_symbol.value,
                    "exchange_id": exchange_symbol.exchange_id.value,
                }
                logger.debug(
                    "arbitrage_exchange_compatible",
                    internal_symbol=internal_symbol,
                    exchange_name=exchange_name,
                    exchange_symbol=exchange_symbol.value,
                )
            except SymbolNotFoundError as e:
                result.is_arbitrage_compatible = False
                result.exchange_availability[exchange_name] = {"available": False, "error": str(e)}
                logger.warning(
                    "arbitrage_exchange_incompatible",
                    internal_symbol=internal_symbol,
                    exchange_name=exchange_name,
                    error=str(e),
                    error_type=type(e).__name__,
                )

        logger.info(
            "arbitrage_compatibility_validation_completed",
            internal_symbol=internal_symbol,
            is_compatible=result.is_arbitrage_compatible,
            compatible_exchanges=len([
                ex for ex, data in result.exchange_availability.items() if data["available"]
            ]),
            total_exchanges=len(exchange_names),
        )

        return result

    def register_symbol(self, symbol: UnifiedSymbol) -> None:
        """Register a unified symbol for fast lookup."""
        logger.debug(
            "symbol_registration_started",
            internal_symbol=symbol.internal.value,
            base_asset=symbol.internal.base_asset,
            market_type=symbol.internal.market_type.value,
            exchange_mappings=list(symbol.exchange_mappings.keys()),
            exchange_count=len(symbol.exchange_mappings),
        )

        self.store.store(symbol)

        logger.info(
            "symbol_registered",
            internal_symbol=symbol.internal.value,
            exchange_mappings=list(symbol.exchange_mappings.keys()),
            is_active=symbol.is_active,
            is_tradeable=symbol.is_tradeable,
        )

    def get_all_symbols(self) -> list[UnifiedSymbol]:
        """Get all registered symbols."""
        return self.store.get_all()

    def clear(self) -> None:
        """Clear all stored symbols."""
        self.store.clear()

    def get_supported_exchanges(self) -> list[str]:
        """Get list of supported exchanges."""
        return list(SYMBOL_TRANSFORMERS.keys())

    def _parse_internal_symbol(self, internal_symbol: str) -> InternalSymbol:
        """Parse internal symbol string back to domain object with type safety.

        Args:
            internal_symbol: Internal symbol format (e.g., "BTC_USD", "ETH_USDC")

        Returns:
            InternalSymbol: Parsed domain object

        Raises:
            ValueError: If symbol format is invalid
        """
        if "_" in internal_symbol:
            parts = internal_symbol.split("_", 1)
            if len(parts) >= self.MIN_SYMBOL_PARTS:
                base, quote = parts[0], parts[1]
                # Heuristic: assume PERP if quote is USD, otherwise SPOT
                market_type = MarketType.PERP if quote == "USD" else MarketType.SPOT
                return create_internal_symbol(
                    value=internal_symbol,
                    base_asset=base,
                    quote_asset=quote,
                    market_type=market_type,
                )

        # Single asset (e.g., "BTC" for spot)
        return create_internal_symbol(
            value=internal_symbol, base_asset=internal_symbol, market_type=MarketType.SPOT
        )
