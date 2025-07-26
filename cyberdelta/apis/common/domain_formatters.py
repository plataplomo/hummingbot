"""Domain-aware API formatters for exchange-specific symbol formatting.

This module provides formatters that work with rich domain objects
instead of primitive strings, enabling type-safe API integration.
"""

from __future__ import annotations

from typing import Any, ClassVar, Protocol

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import ExchangeSymbol
from cyberdelta.enums.exchange_names import ExchangeName


logger = get_logger(__name__)


class FormatterProtocol(Protocol):
    """Protocol defining the interface for exchange formatters."""

    @staticmethod
    def format_symbol_for_api(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format symbol for API."""
        ...

    @staticmethod
    def format_for_market_data(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format symbol for market data."""
        ...

    @staticmethod
    def format_for_order_placement(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format symbol for order placement."""
        ...


class InvalidExchangeSymbolError(ValueError):
    """Raised when a symbol is provided for the wrong exchange."""

    def __init__(self, expected_exchange: ExchangeName, actual_exchange: ExchangeName) -> None:
        """Initialize with exchange details."""
        super().__init__(f"Expected {expected_exchange.value} symbol, got {actual_exchange.value}")
        self.expected_exchange = expected_exchange
        self.actual_exchange = actual_exchange


class UnsupportedExchangeError(ValueError):
    """Raised when no formatter is available for an exchange."""

    def __init__(self, exchange: ExchangeName, supported_exchanges: list[ExchangeName]) -> None:
        """Initialize with exchange details."""
        supported_names = [ex.value for ex in supported_exchanges]
        super().__init__(
            f"No formatter available for exchange {exchange.value}. "
            f"Supported exchanges: {supported_names}"
        )
        self.exchange = exchange
        self.supported_exchanges = supported_exchanges


class BinanceNotSupportedError(ValueError):
    """Raised when Binance exchange is not yet supported."""

    def __init__(self, actual_exchange: str) -> None:
        """Initialize with exchange details."""
        super().__init__(
            f"BinanceApiFormatter requires Binance exchange symbol, got {actual_exchange}"
        )
        self.actual_exchange = actual_exchange


class ExchangeApiFormatter:
    """Base class for exchange-specific API formatters."""

    @staticmethod
    def format_symbol_for_api(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format ExchangeSymbol for API usage.

        Args:
            exchange_symbol: Domain object to format

        Returns:
            Dictionary formatted for specific exchange API

        Raises:
            ValueError: If exchange_symbol is not compatible
        """
        raise NotImplementedError("Subclasses must implement format_symbol_for_api")


class HyperliquidApiFormatter(ExchangeApiFormatter):
    """Hyperliquid-specific API formatting."""

    @staticmethod
    def format_symbol_for_api(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format ExchangeSymbol for Hyperliquid API.

        Args:
            exchange_symbol: Domain object for Hyperliquid

        Returns:
            Dictionary with Hyperliquid API format

        Raises:
            ValueError: If symbol is not for Hyperliquid
        """
        if exchange_symbol.exchange_id != ExchangeName.HYPERLIQUID:
            raise InvalidExchangeSymbolError(ExchangeName.HYPERLIQUID, exchange_symbol.exchange_id)

        api_format: dict[str, Any] = {"symbol": exchange_symbol.value}

        # Add asset index if available (for spot symbols)
        if exchange_symbol.asset_index is not None:
            api_format["assetIndex"] = exchange_symbol.asset_index

        logger.debug(
            "hyperliquid_symbol_formatted",
            internal_symbol=(
                exchange_symbol.internal_symbol.value if exchange_symbol.internal_symbol else None
            ),
            exchange_symbol=exchange_symbol.value,
            asset_index=exchange_symbol.asset_index,
            has_asset_index=exchange_symbol.asset_index is not None,
        )

        return api_format

    @staticmethod
    def format_for_market_data(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format symbol for Hyperliquid market data requests.

        Args:
            exchange_symbol: Domain object for Hyperliquid

        Returns:
            Dictionary formatted for market data API
        """
        base_format = HyperliquidApiFormatter.format_symbol_for_api(exchange_symbol)

        # Add market data specific fields if needed
        if exchange_symbol.internal_symbol:
            base_format["marketType"] = exchange_symbol.internal_symbol.market_type.value

        return base_format

    @staticmethod
    def format_for_order_placement(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format symbol for Hyperliquid order placement.

        Args:
            exchange_symbol: Domain object for Hyperliquid

        Returns:
            Dictionary formatted for order placement API
        """
        return HyperliquidApiFormatter.format_symbol_for_api(exchange_symbol)


class BackpackApiFormatter(ExchangeApiFormatter):
    """Backpack-specific API formatting."""

    @staticmethod
    def format_symbol_for_api(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format ExchangeSymbol for Backpack API.

        Args:
            exchange_symbol: Domain object for Backpack

        Returns:
            Dictionary with Backpack API format

        Raises:
            ValueError: If symbol is not for Backpack
        """
        if exchange_symbol.exchange_id != ExchangeName.BACKPACK:
            raise InvalidExchangeSymbolError(ExchangeName.BACKPACK, exchange_symbol.exchange_id)

        api_format: dict[str, Any] = {"symbol": exchange_symbol.value}

        # Add symbol ID if available
        if exchange_symbol.symbol_id is not None:
            api_format["symbolId"] = exchange_symbol.symbol_id

        logger.debug(
            "backpack_symbol_formatted",
            internal_symbol=(
                exchange_symbol.internal_symbol.value if exchange_symbol.internal_symbol else None
            ),
            exchange_symbol=exchange_symbol.value,
            symbol_id=exchange_symbol.symbol_id,
            has_symbol_id=exchange_symbol.symbol_id is not None,
        )

        return api_format

    @staticmethod
    def format_for_market_data(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format symbol for Backpack market data requests.

        Args:
            exchange_symbol: Domain object for Backpack

        Returns:
            Dictionary formatted for market data API
        """
        return BackpackApiFormatter.format_symbol_for_api(exchange_symbol)

    @staticmethod
    def format_for_order_placement(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format symbol for Backpack order placement.

        Args:
            exchange_symbol: Domain object for Backpack

        Returns:
            Dictionary formatted for order placement API
        """
        return BackpackApiFormatter.format_symbol_for_api(exchange_symbol)


class BinanceApiFormatter(ExchangeApiFormatter):
    """Binance-specific API formatting (future implementation)."""

    @staticmethod
    def format_symbol_for_api(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format ExchangeSymbol for Binance API.

        Args:
            exchange_symbol: Domain object for Binance

        Returns:
            Dictionary with Binance API format

        Raises:
            ValueError: If symbol is not for Binance
            NotImplementedError: Binance support not yet implemented
        """
        # Create a placeholder for Binance since it's not in ExchangeName enum yet
        if exchange_symbol.exchange_id.value.lower() != "binance":
            raise BinanceNotSupportedError(exchange_symbol.exchange_id.value)

        # Placeholder for future Binance implementation
        raise NotImplementedError("Binance API formatting not implemented yet")


class DomainAwareApiFormatter:
    """Unified formatter that routes to exchange-specific formatters."""

    # Mapping of exchange names to formatter classes
    _FORMATTERS: ClassVar[dict[ExchangeName, type[FormatterProtocol]]] = {
        ExchangeName.HYPERLIQUID: HyperliquidApiFormatter,
        ExchangeName.BACKPACK: BackpackApiFormatter,
        # ExchangeName.BINANCE: BinanceApiFormatter,  # Uncomment when ready
    }

    @classmethod
    def format_for_api(cls, exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format symbol for appropriate exchange API.

        Args:
            exchange_symbol: Domain object to format

        Returns:
            Dictionary formatted for the symbol's exchange

        Raises:
            ValueError: If exchange is not supported
        """
        formatter_class = cls._FORMATTERS.get(exchange_symbol.exchange_id)
        if not formatter_class:
            supported_exchanges = list(cls._FORMATTERS.keys())
            raise UnsupportedExchangeError(exchange_symbol.exchange_id, supported_exchanges)

        try:
            result: dict[str, Any] = formatter_class.format_symbol_for_api(exchange_symbol)
        except Exception as e:
            logger.exception(
                "symbol_formatting_failed",
                exchange=exchange_symbol.exchange_id.value,
                exchange_symbol=exchange_symbol.value,
                formatter_class=formatter_class.__name__,
                error=str(e),
                error_type=type(e).__name__,
            )
            raise
        else:
            logger.debug(
                "domain_symbol_formatted",
                exchange=exchange_symbol.exchange_id.value,
                internal_symbol=(
                    exchange_symbol.internal_symbol.value
                    if exchange_symbol.internal_symbol
                    else None
                ),
                formatter_class=formatter_class.__name__,
            )
            return result

    @classmethod
    def format_for_market_data(cls, exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format symbol for market data requests.

        Args:
            exchange_symbol: Domain object to format

        Returns:
            Dictionary formatted for market data API
        """
        formatter_class = cls._FORMATTERS.get(exchange_symbol.exchange_id)
        if not formatter_class:
            # Fallback to basic formatting
            return cls.format_for_api(exchange_symbol)

        # Use specialized market data formatter
        market_result: dict[str, Any] = formatter_class.format_for_market_data(exchange_symbol)
        return market_result

    @classmethod
    def format_for_order_placement(cls, exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
        """Format symbol for order placement requests.

        Args:
            exchange_symbol: Domain object to format

        Returns:
            Dictionary formatted for order placement API
        """
        formatter_class = cls._FORMATTERS.get(exchange_symbol.exchange_id)
        if not formatter_class:
            # Fallback to basic formatting
            return cls.format_for_api(exchange_symbol)

        # Use specialized order placement formatter
        order_result: dict[str, Any] = formatter_class.format_for_order_placement(exchange_symbol)
        return order_result

    @classmethod
    def get_supported_exchanges(cls) -> list[str]:
        """Get list of supported exchange identifiers.

        Returns:
            List of supported exchange names
        """
        return [exchange.value for exchange in cls._FORMATTERS]


# Convenience functions for direct usage
def format_symbol_for_api(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
    """Format ExchangeSymbol for its appropriate exchange API.

    Args:
        exchange_symbol: Domain object to format

    Returns:
        Dictionary formatted for the symbol's exchange API
    """
    return DomainAwareApiFormatter.format_for_api(exchange_symbol)


def format_symbol_for_market_data(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
    """Format ExchangeSymbol for market data requests.

    Args:
        exchange_symbol: Domain object to format

    Returns:
        Dictionary formatted for market data API
    """
    return DomainAwareApiFormatter.format_for_market_data(exchange_symbol)


def format_symbol_for_order_placement(exchange_symbol: ExchangeSymbol) -> dict[str, Any]:
    """Format ExchangeSymbol for order placement requests.

    Args:
        exchange_symbol: Domain object to format

    Returns:
        Dictionary formatted for order placement API
    """
    return DomainAwareApiFormatter.format_for_order_placement(exchange_symbol)
