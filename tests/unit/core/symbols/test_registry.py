"""Test scalable registry pattern."""

import pytest

from cyberdelta.core.symbols import exchanges, get_registry, symbol, symbols
from cyberdelta.core.symbols.models import BackpackMetadata, BaseSymbol
from cyberdelta.enums.exchange_names import ExchangeName
from tests.common_symbols import AVAX_HL


class TestSymbolRegistry:
    """Test scalable registry pattern."""

    def test_direct_symbol_creation(self) -> None:
        """Test direct symbol creation function."""
        # Create symbols using direct function
        btc_hl = symbol("BTC-PERP", ExchangeName.HYPERLIQUID)
        btc_bp = symbol("BTC_USD_PERP", ExchangeName.BACKPACK, symbol_id=12345)

        assert isinstance(btc_hl, BaseSymbol)
        assert btc_hl.value == "BTC-PERP"
        assert btc_hl.exchange == ExchangeName.HYPERLIQUID

        assert isinstance(btc_bp, BaseSymbol)
        assert btc_bp.value == "BTC_USD_PERP"
        assert btc_bp.exchange == ExchangeName.BACKPACK
        assert isinstance(btc_bp.metadata, BackpackMetadata)
        assert btc_bp.metadata.symbol_id == 12345

    def test_exchange_namespace(self) -> None:
        """Test exchange namespace for cleaner API."""
        # Create symbols using exchange namespace
        btc_hl = exchanges.hyperliquid("BTC-PERP")
        btc_bp = exchanges.backpack("BTC_USD_PERP", symbol_id=12345)

        assert isinstance(btc_hl, BaseSymbol)
        assert btc_hl.value == "BTC-PERP"
        assert btc_hl.exchange == ExchangeName.HYPERLIQUID

        assert isinstance(btc_bp, BaseSymbol)
        assert btc_bp.value == "BTC_USD_PERP"
        assert btc_bp.exchange == ExchangeName.BACKPACK

    def test_common_symbols(self) -> None:
        """Test common symbol constants."""
        # Get BTC symbols
        btc_hl = symbols.BTC.hyperliquid()
        btc_bp = symbols.BTC.backpack()

        assert btc_hl.value == "BTC-PERP"
        assert btc_hl.exchange == ExchangeName.HYPERLIQUID

        assert btc_bp.value == "BTC_USD_PERP"
        assert btc_bp.exchange == ExchangeName.BACKPACK
        assert isinstance(btc_bp.metadata, BackpackMetadata)
        assert btc_bp.metadata.symbol_id == 12345

        # Get all BTC symbols
        all_btc = symbols.BTC.all()
        assert len(all_btc) == 2
        assert all(isinstance(s, BaseSymbol) for s in all_btc)

    def test_eth_symbols(self) -> None:
        """Test ETH symbols."""
        eth_hl = symbols.ETH.hyperliquid()
        eth_bp = symbols.ETH.backpack()

        assert eth_hl.value == "ETH-PERP"
        assert eth_bp.value == "ETH_USD_PERP"
        assert isinstance(eth_bp.metadata, BackpackMetadata)
        assert eth_bp.metadata.symbol_id == 67890

    def test_invalid_exchange(self) -> None:
        """Test error handling for invalid exchange."""
        with pytest.raises(AttributeError, match="No factory for exchange"):
            exchanges.invalid_exchange("BTC")

    def test_registry_caching(self) -> None:
        """Test that symbol creation is cached."""
        # Create same symbol twice
        btc1 = exchanges.hyperliquid("BTC-PERP")
        btc2 = exchanges.hyperliquid("BTC-PERP")

        # Should be the same cached object
        assert btc1 is btc2

    def test_all_exchanges_registered(self) -> None:
        """Test that both exchanges are properly registered."""
        registry = get_registry()

        # Should have factories for both exchanges
        hl_factory = registry.get_factory(ExchangeName.HYPERLIQUID)
        bp_factory = registry.get_factory(ExchangeName.BACKPACK)

        assert hl_factory is not None
        assert bp_factory is not None

        # Should be able to create symbols
        hl_sym = hl_factory("TEST-PERP")
        bp_sym = bp_factory("TEST_USD_PERP", symbol_id=999)

        assert hl_sym.exchange == ExchangeName.HYPERLIQUID
        assert bp_sym.exchange == ExchangeName.BACKPACK

    def test_dynamic_asset_addition(self) -> None:
        """Test adding new assets dynamically."""
        # Add AVAX dynamically
        symbols.add_asset(
            "AVAX",
            {
                ExchangeName.HYPERLIQUID: {"value": AVAX_HL.value},
                ExchangeName.BACKPACK: {"value": "AVAX_USD_PERP", "symbol_id": 11111},
            },
        )

        # Should now be able to use AVAX
        # Note: Dynamic attribute access - mypy can't verify this
        # Use getattr for dynamic access instead of direct attribute access
        avax_asset = getattr(symbols, "AVAX", None)
        assert avax_asset is not None
        avax_hl = avax_asset.hyperliquid()
        avax_bp = avax_asset.backpack()

        assert avax_hl.value == AVAX_HL.value
        assert avax_bp.value == "AVAX_USD_PERP"
        assert avax_bp.metadata.symbol_id == 11111

    def test_for_exchanges_filter(self) -> None:
        """Test filtering symbols for specific exchanges."""
        # Get BTC only for Hyperliquid
        btc_hl_only = symbols.BTC.for_exchanges([ExchangeName.HYPERLIQUID])
        assert len(btc_hl_only) == 1
        assert btc_hl_only[0].exchange == ExchangeName.HYPERLIQUID

        # Get BTC for both exchanges
        btc_both = symbols.BTC.for_exchanges([ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK])
        assert len(btc_both) == 2
