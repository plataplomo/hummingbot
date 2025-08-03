"""Enhanced Symbol Test Factory for comprehensive symbol testing.

This module provides advanced factory methods for creating test symbols
with full metadata support, equivalence relationships, and arbitrage scenarios.
"""

from typing import Any

from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols import Symbol, symbol
from cyberdelta.core.symbols.models import (
    BackpackMetadata,
    HyperliquidMetadata,
    SymbolComponents,
)
from cyberdelta.enums.exchange_names import ExchangeName


class SymbolTestFactory:
    """Enhanced factory for creating test symbols with full metadata support."""

    @staticmethod
    def create_with_metadata(value: str, exchange: ExchangeName, **metadata_kwargs: Any) -> Symbol:
        """Create symbol with specific metadata.

        Args:
            value: Symbol value (e.g., "BTC-PERP")
            exchange: Exchange name
            **metadata_kwargs: Exchange-specific metadata fields
                - For Hyperliquid: asset_index
                - For Backpack: symbol_id

        Returns:
            Symbol with proper metadata
        """
        if exchange == ExchangeName.HYPERLIQUID:
            asset_index = metadata_kwargs.get("asset_index")
            return symbol(value, exchange, asset_index=asset_index)
        if exchange == ExchangeName.BACKPACK:
            symbol_id = metadata_kwargs.get("symbol_id")
            return symbol(value, exchange, symbol_id=symbol_id)
        # For future exchanges
        return symbol(value, exchange)

    @staticmethod
    def create_equivalent_pair(
        base_asset: str,
        exchanges: tuple[ExchangeName, ExchangeName] = (
            ExchangeName.HYPERLIQUID,
            ExchangeName.BACKPACK,
        ),
        market_type: MarketType = MarketType.PERP,
    ) -> tuple[Symbol, Symbol]:
        """Create equivalent symbols across exchanges.

        Args:
            base_asset: Base asset (e.g., "BTC", "ETH")
            exchanges: Tuple of two exchange names
            market_type: Market type (PERP or SPOT)

        Returns:
            Tuple of equivalent symbols
        """
        if market_type == MarketType.PERP:
            # Perpetual symbols
            if exchanges[0] == ExchangeName.HYPERLIQUID:
                symbol1 = symbol(f"{base_asset}-PERP", exchanges[0])
            else:
                symbol1 = symbol(f"{base_asset}_PERP", exchanges[0])

            if exchanges[1] == ExchangeName.HYPERLIQUID:
                symbol2 = symbol(f"{base_asset}-PERP", exchanges[1])
            else:
                symbol2 = symbol(f"{base_asset}_PERP", exchanges[1])
        else:
            # Spot symbols
            if exchanges[0] == ExchangeName.HYPERLIQUID:
                symbol1 = symbol(f"{base_asset}-USDC", exchanges[0])
            else:
                symbol1 = symbol(f"{base_asset}_USDC", exchanges[0])

            if exchanges[1] == ExchangeName.HYPERLIQUID:
                symbol2 = symbol(f"{base_asset}-USDC", exchanges[1])
            else:
                symbol2 = symbol(f"{base_asset}_USDC", exchanges[1])

        return (symbol1, symbol2)

    @staticmethod
    def create_arbitrage_set(
        assets: list[str],
        include_spot: bool = False,
    ) -> dict[str, dict[str, tuple[Symbol, Symbol]]]:
        """Create complete arbitrage symbol set.

        Args:
            assets: List of base assets (e.g., ["BTC", "ETH", "SOL"])
            include_spot: Whether to include spot pairs

        Returns:
            Dictionary mapping asset to market type to symbol pairs
        """
        result = {}

        for asset in assets:
            result[asset] = {}

            # Always include perpetuals
            result[asset]["perp"] = SymbolTestFactory.create_equivalent_pair(
                asset, market_type=MarketType.PERP
            )

            # Optionally include spot
            if include_spot:
                result[asset]["spot"] = SymbolTestFactory.create_equivalent_pair(
                    asset, market_type=MarketType.SPOT
                )

        return result

    @staticmethod
    def create_with_components(
        value: str,
        exchange: ExchangeName,
        base_asset: str,
        quote_asset: str | None = None,
        market_type: MarketType = MarketType.PERP,
        **metadata_kwargs: Any,
    ) -> Symbol:
        """Create symbol and ensure components are set.

        Args:
            value: Symbol value
            exchange: Exchange name
            base_asset: Base asset
            quote_asset: Quote asset (optional for perps)
            market_type: Market type
            **metadata_kwargs: Exchange-specific metadata

        Returns:
            Symbol with components pre-set
        """
        sym = SymbolTestFactory.create_with_metadata(value, exchange, **metadata_kwargs)

        # Set components for testing
        components = SymbolComponents(
            base_asset=base_asset,
            quote_asset=quote_asset,
            market_type=market_type,
        )
        sym.set_components(components)

        return sym

    @staticmethod
    def create_test_portfolio_symbols() -> dict[str, Symbol]:
        """Create a standard test portfolio of symbols.

        Returns:
            Dictionary of commonly used test symbols
        """
        return {
            # Hyperliquid perpetuals
            "btc_perp_hl": symbol("BTC-PERP", ExchangeName.HYPERLIQUID, asset_index=0),
            "eth_perp_hl": symbol("ETH-PERP", ExchangeName.HYPERLIQUID, asset_index=1),
            "sol_perp_hl": symbol("SOL-PERP", ExchangeName.HYPERLIQUID, asset_index=4),
            # Backpack perpetuals
            "btc_perp_bp": symbol("BTC_PERP", ExchangeName.BACKPACK, symbol_id=1001),
            "eth_perp_bp": symbol("ETH_PERP", ExchangeName.BACKPACK, symbol_id=1002),
            "sol_perp_bp": symbol("SOL_PERP", ExchangeName.BACKPACK, symbol_id=1003),
            # Hyperliquid spot
            "btc_spot_hl": symbol("BTC-USDC", ExchangeName.HYPERLIQUID),
            "eth_spot_hl": symbol("ETH-USDC", ExchangeName.HYPERLIQUID),
            # Backpack spot
            "btc_spot_bp": symbol("BTC_USDC", ExchangeName.BACKPACK, symbol_id=2001),
            "eth_spot_bp": symbol("ETH_USDC", ExchangeName.BACKPACK, symbol_id=2002),
            # Single assets
            "usdc_hl": symbol("USDC", ExchangeName.HYPERLIQUID),
            "usdc_bp": symbol("USDC", ExchangeName.BACKPACK, symbol_id=3001),
        }

    @staticmethod
    def create_invalid_test_cases() -> dict[str, dict[str, Any]]:
        """Create invalid symbol test cases for error handling.

        Returns:
            Dictionary of invalid test cases with expected errors
        """
        return {
            "empty_value": {
                "value": "",
                "exchange": ExchangeName.HYPERLIQUID,
                "expected_error": "Symbol value cannot be empty",
            },
            "too_long": {
                "value": "A" * 31,  # Exceeds max length
                "exchange": ExchangeName.HYPERLIQUID,
                "expected_error": "Symbol value too long",
            },
            "invalid_chars": {
                "value": "BTC$USD",
                "exchange": ExchangeName.BACKPACK,
                "expected_error": "Invalid characters in symbol",
            },
            "missing_metadata": {
                "value": "BTC_PERP",
                "exchange": ExchangeName.BACKPACK,
                "metadata": {},  # Missing required symbol_id
                "expected_error": "Missing required metadata",
            },
        }


class MetadataTestFactory:
    """Factory for creating test metadata objects."""

    @staticmethod
    def create_hyperliquid_metadata(
        asset_index: int | None = None,
    ) -> HyperliquidMetadata:
        """Create Hyperliquid metadata for testing.

        Args:
            asset_index: Optional asset index

        Returns:
            HyperliquidMetadata instance
        """
        return HyperliquidMetadata(asset_index=asset_index)

    @staticmethod
    def create_backpack_metadata(
        symbol_id: int | None = None,
    ) -> BackpackMetadata:
        """Create Backpack metadata for testing.

        Args:
            symbol_id: Optional symbol ID

        Returns:
            BackpackMetadata instance
        """
        return BackpackMetadata(symbol_id=symbol_id)

    @staticmethod
    def create_test_metadata_sets() -> dict[str, dict[ExchangeName, Any]]:
        """Create standard test metadata sets.

        Returns:
            Dictionary mapping asset to exchange to metadata
        """
        return {
            "BTC": {
                ExchangeName.HYPERLIQUID: {"asset_index": 0},
                ExchangeName.BACKPACK: {"symbol_id": 1001},
            },
            "ETH": {
                ExchangeName.HYPERLIQUID: {"asset_index": 1},
                ExchangeName.BACKPACK: {"symbol_id": 1002},
            },
            "SOL": {
                ExchangeName.HYPERLIQUID: {"asset_index": 4},
                ExchangeName.BACKPACK: {"symbol_id": 1003},
            },
            "AVAX": {
                ExchangeName.HYPERLIQUID: {"asset_index": 7},
                ExchangeName.BACKPACK: {"symbol_id": 1004},
            },
        }


class ComponentsTestFactory:
    """Factory for creating test symbol components."""

    @staticmethod
    def create_perp_components(base_asset: str) -> SymbolComponents:
        """Create perpetual components.

        Args:
            base_asset: Base asset

        Returns:
            SymbolComponents for perpetual
        """
        return SymbolComponents(
            base_asset=base_asset,
            quote_asset="USD",  # Perps typically quote in USD
            market_type=MarketType.PERP,
        )

    @staticmethod
    def create_spot_components(
        base_asset: str,
        quote_asset: str = "USDC",
    ) -> SymbolComponents:
        """Create spot components.

        Args:
            base_asset: Base asset
            quote_asset: Quote asset

        Returns:
            SymbolComponents for spot
        """
        return SymbolComponents(
            base_asset=base_asset,
            quote_asset=quote_asset,
            market_type=MarketType.SPOT,
        )

    @staticmethod
    def create_test_component_sets() -> dict[str, SymbolComponents]:
        """Create standard test component sets.

        Returns:
            Dictionary of test components
        """
        return {
            "btc_perp": ComponentsTestFactory.create_perp_components("BTC"),
            "eth_perp": ComponentsTestFactory.create_perp_components("ETH"),
            "sol_perp": ComponentsTestFactory.create_perp_components("SOL"),
            "btc_spot": ComponentsTestFactory.create_spot_components("BTC"),
            "eth_spot": ComponentsTestFactory.create_spot_components("ETH"),
            "btc_usdt": ComponentsTestFactory.create_spot_components("BTC", "USDT"),
        }
