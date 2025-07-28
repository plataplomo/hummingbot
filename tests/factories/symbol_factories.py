"""Factory functions for creating symbol domain objects for testing.

This module provides convenient factory functions to create valid symbol domain objects
with realistic test data, supporting both simple and complex test scenarios.
"""

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols.models import (
    ExchangeSymbol,
    InternalSymbol,
    UnifiedSymbol,
    create_exchange_symbol,
    create_internal_symbol,
)
from cyberdelta.enums.exchange_names import ExchangeName


class InternalSymbolFactory:
    """Factory for creating InternalSymbol test objects."""

    @staticmethod
    def create_btc_usd_perp() -> InternalSymbol:
        """Create BTC/USD perpetual internal symbol.
        
        Returns:
            InternalSymbol for BTC/USD perpetual contract.
        """
        return create_internal_symbol(
            value="BTC_USD", base_asset="BTC", quote_asset="USD", market_type=MarketType.PERP
        )

    @staticmethod
    def create_eth_usd_perp() -> InternalSymbol:
        """Create ETH/USD perpetual internal symbol.
        
        Returns:
            InternalSymbol for ETH/USD perpetual contract.
        """
        return create_internal_symbol(
            value="ETH_USD", base_asset="ETH", quote_asset="USD", market_type=MarketType.PERP
        )

    @staticmethod
    def create_btc_usdc_spot() -> InternalSymbol:
        """Create BTC/USDC spot internal symbol.
        
        Returns:
            InternalSymbol for BTC/USDC spot market.
        """
        return create_internal_symbol(
            value="BTC_USDC", base_asset="BTC", quote_asset="USDC", market_type=MarketType.SPOT
        )

    @staticmethod
    def create_eth_usdc_spot() -> InternalSymbol:
        """Create ETH/USDC spot internal symbol.
        
        Returns:
            InternalSymbol for ETH/USDC spot market.
        """
        return create_internal_symbol(
            value="ETH_USDC", base_asset="ETH", quote_asset="USDC", market_type=MarketType.SPOT
        )

    @staticmethod
    def create_sol_usd_perp() -> InternalSymbol:
        """Create SOL/USD perpetual internal symbol.
        
        Returns:
            InternalSymbol for SOL/USD perpetual contract.
        """
        return create_internal_symbol(
            value="SOL_USD", base_asset="SOL", quote_asset="USD", market_type=MarketType.PERP
        )

    @staticmethod
    def create_custom(
        base_asset: str, quote_asset: str | None = None, market_type: MarketType = MarketType.PERP
    ) -> InternalSymbol:
        """Create custom internal symbol.

        Args:
            base_asset: Base asset symbol
            quote_asset: Quote asset symbol (defaults to USD for PERP, USDC for SPOT)
            market_type: Market type

        Returns:
            InternalSymbol with specified parameters
        """
        if quote_asset is None:
            quote_asset = "USD" if market_type == MarketType.PERP else "USDC"

        value = f"{base_asset}_{quote_asset}" if quote_asset else base_asset

        return create_internal_symbol(
            value=value, base_asset=base_asset, quote_asset=quote_asset, market_type=market_type
        )


class ExchangeSymbolFactory:
    """Factory for creating ExchangeSymbol test objects."""

    @staticmethod
    def create_hyperliquid_btc_perp() -> ExchangeSymbol:
        """Create Hyperliquid BTC perpetual exchange symbol.
        
        Returns:
            ExchangeSymbol for BTC perpetual on Hyperliquid.
        """
        internal = InternalSymbolFactory.create_btc_usd_perp()
        return create_exchange_symbol(
            value="BTC-PERP", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=internal
        )

    @staticmethod
    def create_hyperliquid_eth_perp() -> ExchangeSymbol:
        """Create Hyperliquid ETH perpetual exchange symbol.
        
        Returns:
            ExchangeSymbol for ETH perpetual on Hyperliquid.
        """
        internal = InternalSymbolFactory.create_eth_usd_perp()
        return create_exchange_symbol(
            value="ETH-PERP", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=internal
        )

    @staticmethod
    def create_hyperliquid_btc_spot() -> ExchangeSymbol:
        """Create Hyperliquid BTC spot exchange symbol.
        
        Returns:
            ExchangeSymbol for BTC spot on Hyperliquid.
        """
        internal = InternalSymbolFactory.create_btc_usdc_spot()
        return create_exchange_symbol(
            value="BTC/USDC",
            exchange_id=ExchangeName.HYPERLIQUID,
            internal_symbol=internal,
            asset_index=0,  # BTC asset index
        )

    @staticmethod
    def create_backpack_btc_perp() -> ExchangeSymbol:
        """Create Backpack BTC perpetual exchange symbol.
        
        Returns:
            ExchangeSymbol for BTC perpetual on Backpack.
        """
        internal = InternalSymbolFactory.create_btc_usd_perp()
        return create_exchange_symbol(
            value="BTC_PERP",
            exchange_id=ExchangeName.BACKPACK,
            internal_symbol=internal,
            symbol_id=1001,  # Example symbol ID
        )

    @staticmethod
    def create_backpack_eth_perp() -> ExchangeSymbol:
        """Create Backpack ETH perpetual exchange symbol.
        
        Returns:
            ExchangeSymbol for ETH perpetual on Backpack.
        """
        internal = InternalSymbolFactory.create_eth_usd_perp()
        return create_exchange_symbol(
            value="ETH_PERP",
            exchange_id=ExchangeName.BACKPACK,
            internal_symbol=internal,
            symbol_id=1002,  # Example symbol ID
        )

    @staticmethod
    def create_backpack_btc_spot() -> ExchangeSymbol:
        """Create Backpack BTC spot exchange symbol.
        
        Returns:
            ExchangeSymbol for BTC spot on Backpack.
        """
        internal = InternalSymbolFactory.create_btc_usdc_spot()
        return create_exchange_symbol(
            value="BTC_USDC",
            exchange_id=ExchangeName.BACKPACK,
            internal_symbol=internal,
            symbol_id=2001,  # Example symbol ID
        )

    @staticmethod
    def create_custom(
        value: str,
        exchange_id: ExchangeName,
        internal_symbol: InternalSymbol | None = None,
        asset_index: int | None = None,
        symbol_id: int | None = None,
    ) -> ExchangeSymbol:
        """Create custom exchange symbol.

        Args:
            value: Exchange-specific symbol value
            exchange_id: Exchange identifier
            internal_symbol: Optional internal symbol (auto-generated if not provided)
            asset_index: Optional asset index (Hyperliquid)
            symbol_id: Optional symbol ID (Backpack)

        Returns:
            ExchangeSymbol with specified parameters
        """
        if internal_symbol is None:
            # Auto-generate internal symbol based on exchange conventions
            if exchange_id == ExchangeName.HYPERLIQUID:
                if value.endswith("-PERP"):
                    base = value.replace("-PERP", "")
                    internal_symbol = InternalSymbolFactory.create_custom(
                        base, "USD", MarketType.PERP
                    )
                elif "/" in value:
                    base, quote = value.split("/")
                    internal_symbol = InternalSymbolFactory.create_custom(
                        base, quote, MarketType.SPOT
                    )
                else:
                    internal_symbol = InternalSymbolFactory.create_custom(
                        value, "USD", MarketType.PERP
                    )

            elif exchange_id == ExchangeName.BACKPACK:
                if value.endswith("_PERP"):
                    base = value.replace("_PERP", "")
                    internal_symbol = InternalSymbolFactory.create_custom(
                        base, "USD", MarketType.PERP
                    )
                elif "_" in value:
                    base, quote = value.split("_")
                    internal_symbol = InternalSymbolFactory.create_custom(
                        base, quote, MarketType.SPOT
                    )
                else:
                    internal_symbol = InternalSymbolFactory.create_custom(
                        value, "USD", MarketType.PERP
                    )

        return create_exchange_symbol(
            value=value,
            exchange_id=exchange_id,
            internal_symbol=internal_symbol,
            asset_index=asset_index,
            symbol_id=symbol_id,
        )


class UnifiedSymbolFactory:
    """Factory for creating UnifiedSymbol test objects."""

    @staticmethod
    def create_btc_usd_perp_unified() -> UnifiedSymbol:
        """Create BTC/USD perpetual unified symbol with multiple exchanges.
        
        Returns:
            UnifiedSymbol for BTC/USD perpetual with both Hyperliquid and Backpack mappings.
        """
        internal = InternalSymbolFactory.create_btc_usd_perp()

        hyperliquid_symbol = create_exchange_symbol(
            value="BTC-PERP", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=internal
        )

        backpack_symbol = create_exchange_symbol(
            value="BTC_PERP",
            exchange_id=ExchangeName.BACKPACK,
            internal_symbol=internal,
            symbol_id=1001,
        )

        exchange_mappings = {"hyperliquid": hyperliquid_symbol, "backpack": backpack_symbol}

        return UnifiedSymbol(
            internal=internal,
            exchange_mappings=exchange_mappings,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0.001"),
            max_order_size=Decimal("1000.0"),
            is_active=True,
            is_tradeable=True,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

    @staticmethod
    def create_eth_usd_perp_unified() -> UnifiedSymbol:
        """Create ETH/USD perpetual unified symbol with multiple exchanges.
        
        Returns:
            UnifiedSymbol for ETH/USD perpetual with both Hyperliquid and Backpack mappings.
        """
        internal = InternalSymbolFactory.create_eth_usd_perp()

        hyperliquid_symbol = create_exchange_symbol(
            value="ETH-PERP", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=internal
        )

        backpack_symbol = create_exchange_symbol(
            value="ETH_PERP",
            exchange_id=ExchangeName.BACKPACK,
            internal_symbol=internal,
            symbol_id=1002,
        )

        exchange_mappings = {"hyperliquid": hyperliquid_symbol, "backpack": backpack_symbol}

        return UnifiedSymbol(
            internal=internal,
            exchange_mappings=exchange_mappings,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0.01"),
            max_order_size=Decimal("500.0"),
            is_active=True,
            is_tradeable=True,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

    @staticmethod
    def create_btc_usdc_spot_unified() -> UnifiedSymbol:
        """Create BTC/USDC spot unified symbol with multiple exchanges.
        
        Returns:
            UnifiedSymbol for BTC/USDC spot with both Hyperliquid and Backpack mappings.
        """
        internal = InternalSymbolFactory.create_btc_usdc_spot()

        hyperliquid_symbol = create_exchange_symbol(
            value="BTC/USDC",
            exchange_id=ExchangeName.HYPERLIQUID,
            internal_symbol=internal,
            asset_index=0,
        )

        backpack_symbol = create_exchange_symbol(
            value="BTC_USDC",
            exchange_id=ExchangeName.BACKPACK,
            internal_symbol=internal,
            symbol_id=2001,
        )

        exchange_mappings = {"hyperliquid": hyperliquid_symbol, "backpack": backpack_symbol}

        return UnifiedSymbol(
            internal=internal,
            exchange_mappings=exchange_mappings,
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0.001"),
            max_order_size=Decimal("100.0"),
            is_active=True,
            is_tradeable=True,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

    @staticmethod
    def create_custom(
        internal_symbol: InternalSymbol,
        exchange_mappings: dict[str, ExchangeSymbol],
        tick_size: Decimal | None = None,
        min_order_size: Decimal | None = None,
        max_order_size: Decimal | None = None,
        is_active: bool = True,
        is_tradeable: bool = True,
    ) -> UnifiedSymbol:
        """Create custom unified symbol.

        Args:
            internal_symbol: Internal symbol
            exchange_mappings: Dictionary of exchange mappings
            tick_size: Optional tick size
            min_order_size: Optional minimum order size
            max_order_size: Optional maximum order size
            is_active: Whether symbol is active
            is_tradeable: Whether symbol is tradeable

        Returns:
            UnifiedSymbol with specified parameters
        """
        return UnifiedSymbol(
            internal=internal_symbol,
            exchange_mappings=exchange_mappings,
            tick_size=tick_size or Decimal("0.01"),
            min_order_size=min_order_size or Decimal("0.001"),
            max_order_size=max_order_size or Decimal("1000.0"),
            is_active=is_active,
            is_tradeable=is_tradeable,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

    @staticmethod
    def create_single_exchange(
        internal_symbol: InternalSymbol,
        exchange_symbol: ExchangeSymbol,
        tick_size: Decimal | None = None,
        min_order_size: Decimal | None = None,
        max_order_size: Decimal | None = None,
        is_active: bool = True,
        is_tradeable: bool = True,
    ) -> UnifiedSymbol:
        """Create unified symbol with single exchange mapping.

        Args:
            internal_symbol: Internal symbol
            exchange_symbol: Exchange symbol
            tick_size: Minimum price increment for the symbol
            min_order_size: Minimum order size allowed
            max_order_size: Maximum order size allowed
            is_active: Whether the symbol is active
            is_tradeable: Whether the symbol is tradeable

        Returns:
            UnifiedSymbol with single exchange mapping
        """
        exchange_mappings = {exchange_symbol.exchange_id.value.lower(): exchange_symbol}

        return UnifiedSymbolFactory.create_custom(
            internal_symbol=internal_symbol,
            exchange_mappings=exchange_mappings,
            tick_size=tick_size,
            min_order_size=min_order_size,
            max_order_size=max_order_size,
            is_active=is_active,
            is_tradeable=is_tradeable,
        )


class ArbitrageSymbolFactory:
    """Factory for creating arbitrage-ready symbol pairs."""

    @staticmethod
    def create_btc_arbitrage_pair() -> tuple[ExchangeSymbol, ExchangeSymbol]:
        """Create BTC arbitrage pair (Hyperliquid long, Backpack short).

        Returns:
            Tuple of (long_symbol, short_symbol) for arbitrage
        """
        internal = InternalSymbolFactory.create_btc_usd_perp()

        long_symbol = create_exchange_symbol(
            value="BTC-PERP", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=internal
        )

        short_symbol = create_exchange_symbol(
            value="BTC_PERP",
            exchange_id=ExchangeName.BACKPACK,
            internal_symbol=internal,
            symbol_id=1001,
        )

        return long_symbol, short_symbol

    @staticmethod
    def create_eth_arbitrage_pair() -> tuple[ExchangeSymbol, ExchangeSymbol]:
        """Create ETH arbitrage pair (Hyperliquid long, Backpack short).

        Returns:
            Tuple of (long_symbol, short_symbol) for arbitrage
        """
        internal = InternalSymbolFactory.create_eth_usd_perp()

        long_symbol = create_exchange_symbol(
            value="ETH-PERP", exchange_id=ExchangeName.HYPERLIQUID, internal_symbol=internal
        )

        short_symbol = create_exchange_symbol(
            value="ETH_PERP",
            exchange_id=ExchangeName.BACKPACK,
            internal_symbol=internal,
            symbol_id=1002,
        )

        return long_symbol, short_symbol

    @staticmethod
    def create_custom_arbitrage_pair(
        base_asset: str,
        long_exchange: ExchangeName = ExchangeName.HYPERLIQUID,
        short_exchange: ExchangeName = ExchangeName.BACKPACK,
        market_type: MarketType = MarketType.PERP,
    ) -> tuple[ExchangeSymbol, ExchangeSymbol]:
        """Create custom arbitrage pair.

        Args:
            base_asset: Base asset for the pair
            long_exchange: Exchange for long position
            short_exchange: Exchange for short position
            market_type: Market type

        Returns:
            Tuple of (long_symbol, short_symbol) for arbitrage
        """
        internal = InternalSymbolFactory.create_custom(base_asset, market_type=market_type)

        # Generate exchange-specific symbol values
        if long_exchange == ExchangeName.HYPERLIQUID:
            long_value = (
                f"{base_asset}-PERP" if market_type == MarketType.PERP else f"{base_asset}/USDC"
            )
        else:
            long_value = (
                f"{base_asset}_PERP" if market_type == MarketType.PERP else f"{base_asset}_USDC"
            )

        if short_exchange == ExchangeName.HYPERLIQUID:
            short_value = (
                f"{base_asset}-PERP" if market_type == MarketType.PERP else f"{base_asset}/USDC"
            )
        else:
            short_value = (
                f"{base_asset}_PERP" if market_type == MarketType.PERP else f"{base_asset}_USDC"
            )

        long_symbol = create_exchange_symbol(
            value=long_value,
            exchange_id=long_exchange,
            internal_symbol=internal,
            asset_index=(
                0
                if long_exchange == ExchangeName.HYPERLIQUID and market_type == MarketType.SPOT
                else None
            ),
            symbol_id=1000 if long_exchange == ExchangeName.BACKPACK else None,
        )

        short_symbol = create_exchange_symbol(
            value=short_value,
            exchange_id=short_exchange,
            internal_symbol=internal,
            asset_index=(
                0
                if short_exchange == ExchangeName.HYPERLIQUID and market_type == MarketType.SPOT
                else None
            ),
            symbol_id=2000 if short_exchange == ExchangeName.BACKPACK else None,
        )

        return long_symbol, short_symbol


class SymbolListFactory:
    """Factory for creating lists of symbols for testing."""

    @staticmethod
    def create_crypto_perp_list() -> list[InternalSymbol]:
        """Create list of major cryptocurrency perpetual symbols.
        
        Returns:
            List of InternalSymbol objects for major perpetual contracts.
        """
        return [
            InternalSymbolFactory.create_btc_usd_perp(),
            InternalSymbolFactory.create_eth_usd_perp(),
            InternalSymbolFactory.create_sol_usd_perp(),
            InternalSymbolFactory.create_custom("DOGE", "USD", MarketType.PERP),
            InternalSymbolFactory.create_custom("AVAX", "USD", MarketType.PERP),
        ]

    @staticmethod
    def create_crypto_spot_list() -> list[InternalSymbol]:
        """Create list of major cryptocurrency spot symbols.
        
        Returns:
            List of InternalSymbol objects for major spot markets.
        """
        return [
            InternalSymbolFactory.create_btc_usdc_spot(),
            InternalSymbolFactory.create_eth_usdc_spot(),
            InternalSymbolFactory.create_custom("SOL", "USDC", MarketType.SPOT),
            InternalSymbolFactory.create_custom("DOGE", "USDC", MarketType.SPOT),
            InternalSymbolFactory.create_custom("AVAX", "USDC", MarketType.SPOT),
        ]

    @staticmethod
    def create_unified_symbol_list() -> list[UnifiedSymbol]:
        """Create list of unified symbols for testing.
        
        Returns:
            List of UnifiedSymbol objects for comprehensive testing scenarios.
        """
        return [
            UnifiedSymbolFactory.create_btc_usd_perp_unified(),
            UnifiedSymbolFactory.create_eth_usd_perp_unified(),
            UnifiedSymbolFactory.create_btc_usdc_spot_unified(),
        ]

    @staticmethod
    def create_arbitrage_pairs_list() -> list[tuple[ExchangeSymbol, ExchangeSymbol]]:
        """Create list of arbitrage pairs for testing.
        
        Returns:
            List of (long_symbol, short_symbol) tuples for arbitrage testing.
        """
        return [
            ArbitrageSymbolFactory.create_btc_arbitrage_pair(),
            ArbitrageSymbolFactory.create_eth_arbitrage_pair(),
            ArbitrageSymbolFactory.create_custom_arbitrage_pair("SOL"),
            ArbitrageSymbolFactory.create_custom_arbitrage_pair("DOGE"),
        ]


# Convenience functions for quick symbol creation
def create_btc_symbols() -> tuple[InternalSymbol, ExchangeSymbol, ExchangeSymbol, UnifiedSymbol]:
    """Create complete BTC symbol set for testing.

    Returns:
        Tuple of (internal, hyperliquid_exchange, backpack_exchange, unified)
    """
    internal = InternalSymbolFactory.create_btc_usd_perp()
    hyperliquid = ExchangeSymbolFactory.create_hyperliquid_btc_perp()
    backpack = ExchangeSymbolFactory.create_backpack_btc_perp()
    unified = UnifiedSymbolFactory.create_btc_usd_perp_unified()

    return internal, hyperliquid, backpack, unified


def create_eth_symbols() -> tuple[InternalSymbol, ExchangeSymbol, ExchangeSymbol, UnifiedSymbol]:
    """Create complete ETH symbol set for testing.

    Returns:
        Tuple of (internal, hyperliquid_exchange, backpack_exchange, unified)
    """
    internal = InternalSymbolFactory.create_eth_usd_perp()
    hyperliquid = ExchangeSymbolFactory.create_hyperliquid_eth_perp()
    backpack = ExchangeSymbolFactory.create_backpack_eth_perp()
    unified = UnifiedSymbolFactory.create_eth_usd_perp_unified()

    return internal, hyperliquid, backpack, unified


def create_test_symbol_service_data() -> list[UnifiedSymbol]:
    """Create comprehensive test data for SymbolService testing.

    Returns:
        List of unified symbols covering various scenarios
    """
    symbols: list[UnifiedSymbol] = []

    # Add major cryptocurrencies
    symbols.extend([
        UnifiedSymbolFactory.create_btc_usd_perp_unified(),
        UnifiedSymbolFactory.create_eth_usd_perp_unified(),
        UnifiedSymbolFactory.create_btc_usdc_spot_unified(),
    ])

    # Add single-exchange symbols
    sol_internal = InternalSymbolFactory.create_sol_usd_perp()
    sol_hyperliquid = ExchangeSymbolFactory.create_custom(
        "SOL-PERP", ExchangeName.HYPERLIQUID, sol_internal
    )
    symbols.append(UnifiedSymbolFactory.create_single_exchange(sol_internal, sol_hyperliquid))

    # Add inactive symbol
    doge_internal = InternalSymbolFactory.create_custom("DOGE", "USD", MarketType.PERP)
    doge_backpack = ExchangeSymbolFactory.create_custom(
        "DOGE_PERP", ExchangeName.BACKPACK, doge_internal
    )
    inactive_unified = UnifiedSymbolFactory.create_single_exchange(
        doge_internal, doge_backpack, is_active=False, is_tradeable=False
    )
    symbols.append(inactive_unified)

    return symbols


# Export factory classes and convenience functions
__all__ = [
    "ArbitrageSymbolFactory",
    "ExchangeSymbolFactory",
    "InternalSymbolFactory",
    "SymbolListFactory",
    "UnifiedSymbolFactory",
    "create_btc_symbols",
    "create_eth_symbols",
    "create_test_symbol_service_data",
]
