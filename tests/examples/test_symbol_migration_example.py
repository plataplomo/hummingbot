"""Example test demonstrating Symbol migration patterns.

This file shows how to migrate from string-based tests to
fully Symbol-aware tests using the new infrastructure.
"""

import pytest
from decimal import Decimal
from unittest.mock import Mock

from cyberdelta.core.symbols import Symbol, symbols
from cyberdelta.core.models import Order
from cyberdelta.core.enums.enums import OrderSide, OrderType, OrderStatus
from cyberdelta.enums.exchange_names import ExchangeName

from tests.helpers.symbol_validators import SymbolTestValidator
from tests.helpers.symbol_scenarios import SymbolTestScenarios


class TestSymbolMigrationExample:
    """Example test class showing migration patterns."""
    
    # ===== OLD PATTERN: String-based testing =====
    
    def test_order_creation_old_pattern(self):
        """OLD: Test order creation with string symbols."""
        # String-based symbol
        symbol = "BTC-PERP"
        exchange = "hyperliquid"
        
        # Create order with strings
        order = Order(
            id="test_order",
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("50000"),
            size=Decimal("0.1"),
            status=OrderStatus.OPEN,
        )
        
        # Basic assertions
        assert order.symbol == symbol
        assert order.side == OrderSide.BUY
        
        # No type safety, no metadata, no validation
    
    # ===== NEW PATTERN: Symbol-aware testing =====
    
    def test_order_creation_new_pattern(self, btc_symbols):
        """NEW: Test order creation with Symbol objects."""
        # Use Symbol object from fixture
        symbol = btc_symbols.perp_hl
        
        # Create order with Symbol
        order = Order(
            id="test_order",
            symbol=symbol.value,  # Use symbol.value for string
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("50000"),
            size=Decimal("0.1"),
            status=OrderStatus.OPEN,
        )
        
        # Rich assertions with metadata
        assert order.symbol == symbol.value
        assert symbol.exchange == ExchangeName.HYPERLIQUID
        assert symbol.metadata.asset_index == 0  # Type-safe metadata access
        
        # Validate symbol properties
        SymbolTestValidator.assert_components_valid(
            symbol,
            expected_base="BTC",
            expected_quote="USD",
        )
    
    # ===== ARBITRAGE TESTING: Old vs New =====
    
    def test_arbitrage_old_pattern(self):
        """OLD: Test arbitrage with string symbols."""
        hl_symbol = "BTC"
        bp_symbol = "BTC_PERP"
        
        # Manual setup
        hl_price = Decimal("50000")
        bp_price = Decimal("50010")
        
        # Check arbitrage
        spread = bp_price - hl_price
        assert spread > 0
        
        # No validation of symbol compatibility
    
    def test_arbitrage_new_pattern(self, arbitrage_pairs, symbol_service):
        """NEW: Test arbitrage with Symbol pairs."""
        # Use pre-configured arbitrage pair
        btc_pair = arbitrage_pairs["BTC"]
        
        # Validate pair compatibility
        SymbolTestValidator.assert_valid_arbitrage_pair(
            btc_pair.long,
            btc_pair.short,
            symbol_service,
        )
        
        # Test with real Symbol objects
        assert btc_pair.long.exchange == ExchangeName.HYPERLIQUID
        assert btc_pair.short.exchange == ExchangeName.BACKPACK
        assert btc_pair.spread_threshold == Decimal("0.001")
        
        # Check equivalence
        assert symbol_service.are_equivalent(btc_pair.long, btc_pair.short)
    
    # ===== MOCK TESTING: Old vs New =====
    
    def test_symbol_conversion_old_pattern(self):
        """OLD: Test with basic mocks."""
        mock_mapper = Mock()
        mock_mapper.get_exchange_symbol.return_value = "BTC-PERP"
        
        result = mock_mapper.get_exchange_symbol("BTC", "hyperliquid")
        assert result == "BTC-PERP"
        
        # No type safety, manual mock setup
    
    def test_symbol_conversion_new_pattern(self, mock_symbol_service):
        """NEW: Test with builder-pattern mocks."""
        # Create symbols
        btc_hl = symbols.BTC.hyperliquid()
        btc_bp = symbols.BTC.backpack()
        
        # Configure mock with builder
        service = (
            mock_symbol_service
            .with_conversion(btc_hl, ExchangeName.BACKPACK, btc_bp)
            .with_equivalence([btc_hl, btc_bp])
            .build()
        )
        
        # Test conversion
        result = service.convert_symbol(btc_hl, ExchangeName.BACKPACK)
        
        # Type-safe assertions
        assert result == btc_bp
        assert isinstance(result.metadata, btc_bp.metadata.__class__)
        assert service.are_equivalent(btc_hl, result)
    
    # ===== SCENARIO TESTING: Using pre-built scenarios =====
    
    def test_funding_arbitrage_scenario(self, symbol_service):
        """Test using pre-built funding arbitrage scenario."""
        # Create complete scenario
        scenario = SymbolTestScenarios.create_funding_arbitrage_scenario()
        
        # Test BTC arbitrage
        btc_hl, btc_bp = scenario.symbols["BTC"]
        
        # Validate symbols
        assert symbol_service.are_equivalent(btc_hl, btc_bp)
        
        # Check funding rates
        hl_rate = scenario.funding_rates[btc_hl]
        bp_rate = scenario.funding_rates[btc_bp]
        rate_diff = hl_rate - bp_rate
        
        assert rate_diff > 0  # Arbitrage opportunity
        assert rate_diff == Decimal("0.015")  # 1.5% spread
        
        # Check positions
        assert scenario.positions[btc_hl] == -scenario.positions[btc_bp]
    
    # ===== BUILDER PATTERN: Complex test setup =====
    
    def test_with_arbitrage_builder(self, symbol_builder):
        """Test using ArbitrageSymbolBuilder."""
        # Build complex scenario
        test_data = (
            symbol_builder
            .add_perpetual_pair(
                "BTC",
                hl_metadata={"asset_index": 0},
                bp_metadata={"symbol_id": 1001},
            )
            .add_perpetual_pair(
                "ETH",
                hl_metadata={"asset_index": 1},
                bp_metadata={"symbol_id": 1002},
            )
            .with_price_discrepancy("BTC", Decimal("50000"), Decimal("50020"))
            .with_funding_rates("BTC", Decimal("0.01"), Decimal("-0.005"))
            .build()
        )
        
        # Use built data
        btc_pair = test_data.pairs["BTC"]
        assert btc_pair.long.metadata.asset_index == 0
        assert btc_pair.short.metadata.symbol_id == 1001
        
        # Check prices
        assert test_data.prices[btc_pair.long] == Decimal("50000")
        assert test_data.prices[btc_pair.short] == Decimal("50020")
    
    # ===== PARAMETRIZED TESTING: With Symbol support =====
    
    @pytest.mark.parametrize("asset", ["BTC", "ETH", "SOL"])
    def test_multi_asset_symbols(self, asset, symbol_service):
        """Test multiple assets with Symbol API."""
        # Get symbols for asset
        hl_symbol = getattr(symbols, asset).hyperliquid()
        bp_symbol = getattr(symbols, asset).backpack()
        
        # Validate
        assert hl_symbol.exchange == ExchangeName.HYPERLIQUID
        assert bp_symbol.exchange == ExchangeName.BACKPACK
        assert symbol_service.are_equivalent(hl_symbol, bp_symbol)
        
        # Check format
        if hl_symbol.exchange == ExchangeName.HYPERLIQUID:
            assert "-PERP" in hl_symbol.value
        else:
            assert "_PERP" in bp_symbol.value