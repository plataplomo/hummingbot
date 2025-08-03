"""Unit tests for safe mode wrapper module."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
import uuid

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.logic.trading.safe_mode_wrapper import (
    SafeModeWrapper,
    SimulatedFill,
    SimulatedBalance,
    SimulatedPosition
)
from cyberdelta.models.market.order import Order, OrderStatus
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import ExchangeName, OrderSide, OrderType


@pytest.fixture
def mock_config():
    """Create mock configuration for testing."""
    config = MagicMock(spec=AppSettings)
    
    # General config
    config.general.safe_mode = True
    
    # Paper trading config
    config.testing.paper_trading.initial_balance = Decimal("10000")
    config.testing.paper_trading.fill_probability = 0.95
    config.testing.paper_trading.slippage_range = Decimal("0.001")
    config.testing.paper_trading.limit_fill_delay_seconds = 1.0
    
    # Exchange config
    config.exchanges = {
        'hyperliquid': MagicMock(
            maker_fee_rate=Decimal("0.0002"),
            taker_fee_rate=Decimal("0.0005")
        )
    }
    
    # Calculation config
    config.calculation.base_currency = "USDT"
    
    return config


@pytest.fixture
def mock_real_api():
    """Create mock real API for testing."""
    api = AsyncMock()
    api.place_order = AsyncMock(return_value={
        "order_id": "REAL_123",
        "status": "open"
    })
    api.cancel_order = AsyncMock(return_value={
        "order_id": "REAL_123",
        "status": "cancelled"
    })
    api.get_order = AsyncMock(return_value={
        "order_id": "REAL_123",
        "status": "filled"
    })
    api.get_balances = AsyncMock(return_value=[
        {"asset": "USDT", "total": 10000.0, "available": 10000.0}
    ])
    api.get_positions = AsyncMock(return_value=[])
    return api


@pytest.fixture
def safe_mode_wrapper(mock_config, mock_real_api):
    """Create safe mode wrapper instance."""
    return SafeModeWrapper(
        mock_config,
        mock_real_api,
        ExchangeName.HYPERLIQUID
    )


@pytest.fixture
def safe_mode_disabled_wrapper(mock_config, mock_real_api):
    """Create wrapper with safe mode disabled."""
    mock_config.general.safe_mode = False
    return SafeModeWrapper(
        mock_config,
        mock_real_api,
        ExchangeName.HYPERLIQUID
    )


class TestSafeModeWrapper:
    """Test safe mode wrapper functionality."""
    
    def test_init_safe_mode_enabled(self, mock_config, mock_real_api):
        """Test initialization with safe mode enabled."""
        wrapper = SafeModeWrapper(
            mock_config,
            mock_real_api,
            ExchangeName.HYPERLIQUID
        )
        
        assert wrapper._safe_mode is True
        assert wrapper._initial_balance == Decimal("10000")
        assert wrapper._fill_probability == 0.95
        assert wrapper._slippage_range == Decimal("0.001")
        assert wrapper._maker_fee_rate == Decimal("0.0002")
        assert wrapper._taker_fee_rate == Decimal("0.0005")
        
        # Check initial balance
        assert "USDT" in wrapper._simulated_balances
        balance = wrapper._simulated_balances["USDT"]
        assert balance.total == Decimal("10000")
        assert balance.available == Decimal("10000")
    
    def test_init_safe_mode_disabled(self, mock_config, mock_real_api):
        """Test initialization with safe mode disabled."""
        mock_config.general.safe_mode = False
        wrapper = SafeModeWrapper(
            mock_config,
            mock_real_api,
            ExchangeName.HYPERLIQUID
        )
        
        assert wrapper._safe_mode is False
    
    @pytest.mark.asyncio
    async def test_place_order_safe_mode_market(self, safe_mode_wrapper):
        """Test placing market order in safe mode."""
        with patch('random.random', return_value=0.5):  # Will fill
            result = await safe_mode_wrapper.place_order(
                symbol="BTC_USD",
                side="buy",
                order_type="market",
                price=50000.0,
                quantity=0.1
            )
        
        assert "SIM_" in result["order_id"]
        assert result["status"] == "open"
        assert result["safe_mode"] is True
        
        # Check order was stored
        order_id = result["order_id"]
        assert order_id in safe_mode_wrapper._simulated_orders
        
        order = safe_mode_wrapper._simulated_orders[order_id]
        assert order.symbol == Symbol("BTC_USD")
        assert order.side == OrderSide.BUY
        assert order.order_type == OrderType.MARKET
        
        # Wait a bit for async fill
        await asyncio.sleep(0.1)
        
        # Should be filled for market order
        assert order.status == OrderStatus.FILLED
    
    @pytest.mark.asyncio
    async def test_place_order_safe_mode_limit(self, safe_mode_wrapper):
        """Test placing limit order in safe mode."""
        result = await safe_mode_wrapper.place_order(
            symbol="BTC_USD",
            side="sell",
            order_type="limit",
            price=51000.0,
            quantity=0.05,
            time_in_force="GTC"
        )
        
        assert "SIM_" in result["order_id"]
        assert result["safe_mode"] is True
        
        order_id = result["order_id"]
        order = safe_mode_wrapper._simulated_orders[order_id]
        
        assert order.order_type == OrderType.LIMIT
        assert order.price == Decimal("51000")
        assert order.quantity == Decimal("0.05")
        assert order.status == OrderStatus.OPEN  # Not immediately filled
    
    @pytest.mark.asyncio
    async def test_place_order_pass_through(self, safe_mode_disabled_wrapper, mock_real_api):
        """Test that orders pass through when safe mode is disabled."""
        result = await safe_mode_disabled_wrapper.place_order(
            symbol="BTC_USD",
            side="buy",
            order_type="market",
            price=50000.0,
            quantity=0.1
        )
        
        # Should call real API
        mock_real_api.place_order.assert_called_once_with(
            "BTC_USD", "buy", "market", 50000.0, 0.1
        )
        
        assert result["order_id"] == "REAL_123"
        assert "safe_mode" not in result or result["safe_mode"] is False
    
    @pytest.mark.asyncio
    async def test_cancel_order_safe_mode(self, safe_mode_wrapper):
        """Test cancelling order in safe mode."""
        # First place an order
        place_result = await safe_mode_wrapper.place_order(
            symbol="BTC_USD",
            side="buy",
            order_type="limit",
            price=49000.0,
            quantity=0.1
        )
        
        order_id = place_result["order_id"]
        
        # Cancel it
        cancel_result = await safe_mode_wrapper.cancel_order(order_id)
        
        assert cancel_result["order_id"] == order_id
        assert cancel_result["status"] == "cancelled"
        assert cancel_result["safe_mode"] is True
        
        # Check order status updated
        order = safe_mode_wrapper._simulated_orders[order_id]
        assert order.status == OrderStatus.CANCELLED
    
    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order(self, safe_mode_wrapper):
        """Test cancelling non-existent order in safe mode."""
        result = await safe_mode_wrapper.cancel_order("FAKE_ORDER")
        
        assert result["error"] == "Order not found"
        assert result["safe_mode"] is True
    
    @pytest.mark.asyncio
    async def test_get_order_safe_mode(self, safe_mode_wrapper):
        """Test getting order status in safe mode."""
        # Place an order
        place_result = await safe_mode_wrapper.place_order(
            symbol="ETH_USD",
            side="sell",
            order_type="limit",
            price=3000.0,
            quantity=1.0
        )
        
        order_id = place_result["order_id"]
        
        # Get order status
        status = await safe_mode_wrapper.get_order(order_id)
        
        assert status["order_id"] == order_id
        assert status["symbol"] == "ETH_USD"
        assert status["side"] == "sell"
        assert status["price"] == 3000.0
        assert status["quantity"] == 1.0
        assert status["safe_mode"] is True
    
    @pytest.mark.asyncio
    async def test_get_balances_safe_mode(self, safe_mode_wrapper):
        """Test getting balances in safe mode."""
        balances = await safe_mode_wrapper.get_balances()
        
        assert len(balances) == 1
        assert balances[0]["asset"] == "USDT"
        assert balances[0]["total"] == 10000.0
        assert balances[0]["available"] == 10000.0
        assert balances[0]["safe_mode"] is True
    
    @pytest.mark.asyncio
    async def test_get_positions_safe_mode(self, safe_mode_wrapper):
        """Test getting positions in safe mode."""
        positions = await safe_mode_wrapper.get_positions()
        
        # Should be empty initially
        assert len(positions) == 0
    
    @pytest.mark.asyncio
    async def test_simulate_fill_buy_order(self, safe_mode_wrapper):
        """Test simulating a buy order fill."""
        order = Order(
            order_id="TEST_123",
            exchange=ExchangeName.HYPERLIQUID,
            symbol=Symbol("BTC_USD"),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("50000"),
            quantity=Decimal("0.1"),
            status=OrderStatus.OPEN,
            timestamp=datetime.now(UTC)
        )
        
        safe_mode_wrapper._simulated_orders[order.order_id] = order
        
        with patch('random.random', return_value=0.5), \
             patch('random.uniform', return_value=0.0005):  # 0.05% slippage
            await safe_mode_wrapper._simulate_fill(order, immediate=True)
        
        # Check order filled
        assert order.status == OrderStatus.FILLED
        assert order.filled_quantity == Decimal("0.1")
        
        # Check fill recorded
        assert len(safe_mode_wrapper._simulated_fills) == 1
        fill = safe_mode_wrapper._simulated_fills[0]
        assert fill.order_id == "TEST_123"
        assert fill.side == OrderSide.BUY
        
        # Check balance updated (should decrease)
        balance = safe_mode_wrapper._simulated_balances["USDT"]
        # Cost = 0.1 * 50025 (with slippage) + fee
        expected_cost = Decimal("0.1") * Decimal("50025") + fill.fee
        assert balance.total < Decimal("10000")
    
    @pytest.mark.asyncio
    async def test_simulate_fill_sell_order(self, safe_mode_wrapper):
        """Test simulating a sell order fill."""
        # First create a position
        safe_mode_wrapper._simulated_positions["BTC_USD"] = SimulatedPosition(
            symbol=Symbol("BTC_USD"),
            side=OrderSide.BUY,
            size=Decimal("0.1"),
            entry_price=Decimal("50000"),
            unrealized_pnl=Decimal("0"),
            timestamp=datetime.now(UTC)
        )
        
        order = Order(
            order_id="TEST_456",
            exchange=ExchangeName.HYPERLIQUID,
            symbol=Symbol("BTC_USD"),
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal("51000"),
            quantity=Decimal("0.05"),
            status=OrderStatus.OPEN,
            timestamp=datetime.now(UTC)
        )
        
        safe_mode_wrapper._simulated_orders[order.order_id] = order
        
        with patch('random.random', return_value=0.5), \
             patch('random.uniform', return_value=-0.0005):  # -0.05% slippage
            await safe_mode_wrapper._simulate_fill(order)
        
        # Check position reduced
        position = safe_mode_wrapper._simulated_positions.get("BTC_USD")
        assert position is not None
        assert position.size == Decimal("0.05")  # 0.1 - 0.05
        
        # Check balance increased
        balance = safe_mode_wrapper._simulated_balances["USDT"]
        assert balance.total > Decimal("10000")  # Sold at profit
    
    @pytest.mark.asyncio
    async def test_limit_order_delayed_fill(self, safe_mode_wrapper, mock_config):
        """Test that limit orders have delayed fills."""
        mock_config.testing.paper_trading.limit_fill_delay_seconds = 0.1
        
        order = Order(
            order_id="TEST_789",
            exchange=ExchangeName.HYPERLIQUID,
            symbol=Symbol("ETH_USD"),
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("3000"),
            quantity=Decimal("1"),
            status=OrderStatus.OPEN,
            timestamp=datetime.now(UTC)
        )
        
        safe_mode_wrapper._simulated_orders[order.order_id] = order
        
        with patch('random.random', return_value=0.5):  # Will fill
            # Start fill task
            task = asyncio.create_task(
                safe_mode_wrapper._simulate_limit_fill(order)
            )
            
            # Order should still be open immediately
            assert order.status == OrderStatus.OPEN
            
            # Wait for delay
            await asyncio.sleep(0.15)
            
            # Now should be filled
            assert order.status == OrderStatus.FILLED
            
            await task
    
    @pytest.mark.asyncio
    async def test_update_position_from_fill_new(self, safe_mode_wrapper):
        """Test creating new position from fill."""
        fill = SimulatedFill(
            order_id="TEST_001",
            symbol=Symbol("SOL_USD"),
            side=OrderSide.BUY,
            price=Decimal("100"),
            quantity=Decimal("10"),
            fee=Decimal("0.05"),
            timestamp=datetime.now(UTC)
        )
        
        await safe_mode_wrapper._update_position_from_fill(fill)
        
        assert "SOL_USD" in safe_mode_wrapper._simulated_positions
        position = safe_mode_wrapper._simulated_positions["SOL_USD"]
        assert position.side == OrderSide.BUY
        assert position.size == Decimal("10")
        assert position.entry_price == Decimal("100")
    
    @pytest.mark.asyncio
    async def test_update_position_from_fill_add(self, safe_mode_wrapper):
        """Test adding to existing position."""
        # Create initial position
        safe_mode_wrapper._simulated_positions["BTC_USD"] = SimulatedPosition(
            symbol=Symbol("BTC_USD"),
            side=OrderSide.BUY,
            size=Decimal("0.1"),
            entry_price=Decimal("50000"),
            unrealized_pnl=Decimal("0"),
            timestamp=datetime.now(UTC)
        )
        
        # Add to position
        fill = SimulatedFill(
            order_id="TEST_002",
            symbol=Symbol("BTC_USD"),
            side=OrderSide.BUY,
            price=Decimal("51000"),
            quantity=Decimal("0.1"),
            fee=Decimal("25"),
            timestamp=datetime.now(UTC)
        )
        
        await safe_mode_wrapper._update_position_from_fill(fill)
        
        position = safe_mode_wrapper._simulated_positions["BTC_USD"]
        assert position.size == Decimal("0.2")  # 0.1 + 0.1
        # Average entry: (0.1 * 50000 + 0.1 * 51000) / 0.2 = 50500
        assert position.entry_price == Decimal("50500")
    
    @pytest.mark.asyncio
    async def test_update_position_from_fill_close(self, safe_mode_wrapper):
        """Test closing position completely."""
        # Create initial position
        safe_mode_wrapper._simulated_positions["ETH_USD"] = SimulatedPosition(
            symbol=Symbol("ETH_USD"),
            side=OrderSide.BUY,
            size=Decimal("1"),
            entry_price=Decimal("3000"),
            unrealized_pnl=Decimal("0"),
            timestamp=datetime.now(UTC)
        )
        
        # Close position
        fill = SimulatedFill(
            order_id="TEST_003",
            symbol=Symbol("ETH_USD"),
            side=OrderSide.SELL,
            price=Decimal("3100"),
            quantity=Decimal("1"),
            fee=Decimal("1.5"),
            timestamp=datetime.now(UTC)
        )
        
        await safe_mode_wrapper._update_position_from_fill(fill)
        
        # Position should be closed
        assert "ETH_USD" not in safe_mode_wrapper._simulated_positions
    
    def test_get_simulated_stats(self, safe_mode_wrapper):
        """Test getting simulation statistics."""
        # Add some test data
        safe_mode_wrapper._simulated_orders["O1"] = Order(
            order_id="O1",
            exchange=ExchangeName.HYPERLIQUID,
            symbol=Symbol("BTC_USD"),
            side=OrderSide.BUY,
            status=OrderStatus.FILLED,
            timestamp=datetime.now(UTC)
        )
        safe_mode_wrapper._simulated_orders["O2"] = Order(
            order_id="O2",
            exchange=ExchangeName.HYPERLIQUID,
            symbol=Symbol("ETH_USD"),
            side=OrderSide.SELL,
            status=OrderStatus.OPEN,
            timestamp=datetime.now(UTC)
        )
        
        safe_mode_wrapper._simulated_fills.append(SimulatedFill(
            order_id="O1",
            symbol=Symbol("BTC_USD"),
            side=OrderSide.BUY,
            price=Decimal("50000"),
            quantity=Decimal("0.1"),
            fee=Decimal("25"),
            timestamp=datetime.now(UTC)
        ))
        
        stats = safe_mode_wrapper.get_simulated_stats()
        
        assert stats["safe_mode"] is True
        assert stats["total_orders"] == 2
        assert stats["filled_orders"] == 1
        assert stats["fill_rate"] == 0.5
        assert stats["total_fills"] == 1
        assert stats["total_volume"] == 5000.0  # 0.1 * 50000
        assert stats["total_fees"] == 25.0
        assert "current_balance" in stats
    
    def test_is_safe_mode(self, safe_mode_wrapper, safe_mode_disabled_wrapper):
        """Test checking if safe mode is active."""
        assert safe_mode_wrapper.is_safe_mode() is True
        assert safe_mode_disabled_wrapper.is_safe_mode() is False