"""Unit tests for safe mode wrapper module."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.symbols import exchanges
from cyberdelta.enums import ExchangeName
from cyberdelta.logic.trading.safe_mode_wrapper import SafeModeWrapper


@pytest.fixture
def mock_config() -> MagicMock:
    """Create mock configuration for testing.

    Returns:
        MagicMock: Mock configuration object with safe mode settings.
    """
    config = MagicMock(spec=AppSettings)

    # General config
    config.general.safe_mode = True

    # Paper trading config
    config.testing.paper_trading.initial_balance = Decimal(10000)
    config.testing.paper_trading.fill_probability = 0.95
    config.testing.paper_trading.slippage_range = Decimal("0.001")
    config.testing.paper_trading.limit_fill_delay_seconds = 1.0

    # Exchange config
    config.exchanges = {
        "hyperliquid": MagicMock(maker_fee_rate=Decimal("0.0002"), taker_fee_rate=Decimal("0.0005"))
    }

    # Calculation config
    config.calculation.base_currency = "USDT"

    return config


@pytest.fixture
def mock_real_api() -> AsyncMock:
    """Create mock real API for testing.

    Returns:
        AsyncMock: Mock API object with standard trading methods.
    """
    api = AsyncMock()
    api.place_order = AsyncMock(return_value={"order_id": "REAL_123", "status": "open"})
    api.cancel_order = AsyncMock(return_value={"order_id": "REAL_123", "status": "cancelled"})
    api.get_order = AsyncMock(return_value={"order_id": "REAL_123", "status": "filled"})
    api.get_balances = AsyncMock(
        return_value=[{"asset": "USDT", "total": 10000.0, "available": 10000.0}]
    )
    api.place_order = AsyncMock(return_value={"order_id": "REAL_123", "status": "open"})
    api.cancel_order = AsyncMock(return_value={"order_id": "REAL_123", "status": "cancelled"})
    api.get_order = AsyncMock(return_value={"order_id": "REAL_123", "status": "filled"})
    api.get_balances = AsyncMock(
        return_value=[{"asset": "USDT", "total": 10000.0, "available": 10000.0}]
    )
    api.get_positions = AsyncMock(return_value=[])
    return api


@pytest.fixture
def safe_mode_wrapper(mock_config: MagicMock, mock_real_api: AsyncMock) -> SafeModeWrapper:
    """Create safe mode wrapper instance.

    Returns:
        SafeModeWrapper: Wrapper instance with safe mode enabled.
    """
    return SafeModeWrapper(mock_config, mock_real_api, ExchangeName.HYPERLIQUID)


@pytest.fixture
def safe_mode_disabled_wrapper(mock_config: MagicMock, mock_real_api: AsyncMock) -> SafeModeWrapper:
    """Create wrapper with safe mode disabled.

    Returns:
        SafeModeWrapper: Wrapper instance with safe mode disabled.
    """
    mock_config.general.safe_mode = False
    return SafeModeWrapper(mock_config, mock_real_api, ExchangeName.HYPERLIQUID)


class TestSafeModeWrapper:
    """Test safe mode wrapper functionality."""

    def test_init_safe_mode_enabled(self, mock_config, mock_real_api):
        """Test initialization with safe mode enabled."""
        wrapper = SafeModeWrapper(mock_config, mock_real_api, ExchangeName.HYPERLIQUID)

        # Test safe mode is enabled
        assert wrapper.is_safe_mode() is True

        # Check initial balance through public API
        balances = wrapper.get_simulated_balances()
        assert "USDT" in balances
        balance_info = balances["USDT"]
        assert balance_info["total"] == 10000.0
        assert balance_info["available"] == 10000.0

    def test_init_safe_mode_disabled(self, mock_config, mock_real_api):
        """Test initialization with safe mode disabled."""
        mock_config.general.safe_mode = False
        wrapper = SafeModeWrapper(mock_config, mock_real_api, ExchangeName.HYPERLIQUID)

        assert wrapper.is_safe_mode() is False

    @pytest.mark.asyncio
    async def test_place_order_safe_mode_market(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test placing market order in safe mode."""
        with patch("random.random", return_value=0.5):  # Will fill
            result = await safe_mode_wrapper.place_order(
                symbol="BTC_USD", side="buy", order_type="market", price=50000.0, quantity=0.1
            )

        assert "SIM_" in result["order_id"]
        assert result["status"] == "open"
        assert result["safe_mode"] is True

        # Check order was created
        order_id = result["order_id"]
        simulated_orders = safe_mode_wrapper.get_simulated_orders()
        assert order_id in simulated_orders

        order_data = simulated_orders[order_id]
        assert order_data["symbol"] == "BTC_USD"
        assert order_data["side"] == "BUY"

        # Wait a bit for async fill
        await asyncio.sleep(0.1)

        # Check if order was filled through public API
        updated_orders = safe_mode_wrapper.get_simulated_orders()
        assert updated_orders[order_id]["status"] == "FILLED"

    @pytest.mark.asyncio
    async def test_place_order_safe_mode_limit(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test placing limit order in safe mode."""
        btc_symbol = exchanges.hyperliquid("BTC")
        result = await safe_mode_wrapper.place_order(
            symbol=btc_symbol.value,
            side="sell",
            order_type="limit",
            price=51000.0,
            quantity=0.05,
            time_in_force="GTC",
        )

        assert "SIM_" in result["order_id"]
        assert result["safe_mode"] is True

        order_id = result["order_id"]
        simulated_orders = safe_mode_wrapper.get_simulated_orders()
        order_data = simulated_orders[order_id]

        assert order_data["price"] == 51000.0
        assert order_data["quantity"] == 0.05
        assert order_data["status"] == "OPEN"  # Not immediately filled

    @pytest.mark.asyncio
    async def test_place_order_pass_through(
        self, safe_mode_disabled_wrapper: SafeModeWrapper, mock_real_api: AsyncMock
    ) -> None:
        """Test that orders pass through when safe mode is disabled."""
        btc_symbol = exchanges.hyperliquid("BTC")
        result = await safe_mode_disabled_wrapper.place_order(
            symbol="BTC_USD", side="buy", order_type="market", price=50000.0, quantity=0.1
        )

        # Should call real API
        mock_real_api.place_order.assert_called_once_with(
            btc_symbol.value, "buy", "market", 50000.0, 0.1
        )

        assert result["order_id"] == "REAL_123"
        assert "safe_mode" not in result or result["safe_mode"] is False

    @pytest.mark.asyncio
    async def test_cancel_order_safe_mode(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test cancelling order in safe mode."""
        # First place an order
        btc_symbol = exchanges.hyperliquid("BTC")
        place_result = await safe_mode_wrapper.place_order(
            symbol="BTC_USD", side="buy", order_type="limit", price=49000.0, quantity=0.1
        )

        order_id = place_result["order_id"]

        # Cancel it
        cancel_result = await safe_mode_wrapper.cancel_order(order_id)

        assert cancel_result["order_id"] == order_id
        assert cancel_result["status"] == "cancelled"
        assert cancel_result["safe_mode"] is True

        # Check order status updated
        simulated_orders = safe_mode_wrapper.get_simulated_orders()
        assert simulated_orders[order_id]["status"] == "CANCELED"

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test cancelling non-existent order in safe mode."""
        result = await safe_mode_wrapper.cancel_order("FAKE_ORDER")

        assert result["error"] == "Order not found"
        assert result["safe_mode"] is True

    @pytest.mark.asyncio
    async def test_get_order_safe_mode(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test getting order status in safe mode."""
        # Place an order
        eth_symbol = exchanges.hyperliquid("ETH")
        place_result = await safe_mode_wrapper.place_order(
            symbol="ETH_USD", side="sell", order_type="limit", price=3000.0, quantity=1.0
        )

        order_id = place_result["order_id"]

        # Get order status
        status = await safe_mode_wrapper.get_order(order_id)

        assert status["order_id"] == order_id
        assert status["symbol"] == "ETH_USD"  # SafeModeWrapper converts ETH to ETH_USD
        assert status["side"] == "sell"
        assert status["price"] == 3000.0
        assert status["quantity"] == 1.0
        assert status["safe_mode"] is True

    @pytest.mark.asyncio
    async def test_get_balances_safe_mode(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test getting balances in safe mode."""
        balances = await safe_mode_wrapper.get_balances()

        assert len(balances) == 1
        assert balances[0]["asset"] == "USDT"
        assert balances[0]["total"] == 10000.0
        assert balances[0]["available"] == 10000.0
        assert balances[0]["safe_mode"] is True

    @pytest.mark.asyncio
    async def test_get_positions_safe_mode(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test getting positions in safe mode."""
        positions = await safe_mode_wrapper.get_positions()

        # Should be empty initially
        assert len(positions) == 0

    @pytest.mark.asyncio
    async def test_simulate_fill_buy_order(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test simulating a buy order fill using public API."""
        # Place an order through the public API
        with (
            patch("random.random", return_value=0.5),
            patch("random.uniform", return_value=0.0005),
        ):  # 0.05% slippage
            result = await safe_mode_wrapper.place_order(
                symbol="BTC_USD",
                side="BUY",
                order_type="MARKET",
                price=50000.0,
                quantity=0.1,
            )

        # Check that order was created
        assert "order_id" in result
        order_id = result["order_id"]

        # Get order status through public API
        order_data = await safe_mode_wrapper.get_order(order_id)
        assert order_data["status"] == "FILLED"
        assert order_data["filled_quantity"] == 0.1

        # Check balances through public API
        balances = await safe_mode_wrapper.get_balances()
        usdt_balance = next((b for b in balances if b["asset"] == "USDT"), None)
        assert usdt_balance is not None
        # Balance should be less than initial amount due to trade cost and fees
        assert Decimal(str(usdt_balance["total"])) < Decimal(10000)

        # Get simulation stats to verify fill was recorded
        stats = safe_mode_wrapper.get_simulated_stats()
        assert stats["total_orders"] == 1
        assert stats["filled_orders"] == 1

    @pytest.mark.asyncio
    async def test_simulate_fill_sell_order(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test simulating a sell order fill."""
        # First create a position
        # First create a position by placing a buy order
        await safe_mode_wrapper.place_order(
            symbol="BTC_USD",
            side="BUY",
            order_type="MARKET",
            price=50000.0,
            quantity=0.1,
        )

        # Get the current position
        positions_list = await safe_mode_wrapper.get_positions()
        btc_position = next((p for p in positions_list if p["symbol"] == "BTC_USD"), None)
        assert btc_position is not None
        assert Decimal(str(btc_position["size"])) == Decimal("0.1")

        # Instead of manually inserting orders, place a sell order through public API
        with (
            patch("random.random", return_value=0.5),
            patch("random.uniform", return_value=-0.0005),
        ):  # -0.05% slippage
            result = await safe_mode_wrapper.place_order(
                symbol="BTC_USD",
                side="SELL",
                order_type="LIMIT",
                price=51000.0,
                quantity=0.05,
            )

            # Wait for processing
            await asyncio.sleep(0.1)

            # Check order status through public API
            order_data = await safe_mode_wrapper.get_order(result["order_id"])
            assert order_data["status"] == "FILLED"

        # Check position through public API
        positions: dict[str, Any] = safe_mode_wrapper.get_simulated_positions()
        assert "BTC_USD" in positions
        position = positions["BTC_USD"]
        assert position["size"] == 0.05  # Reduced from 0.1

        # Check balance through public API
        balances = safe_mode_wrapper.get_simulated_balances()
        assert balances["USDT"]["total"] > 10000.0  # Sold at profit

    @pytest.mark.asyncio
    async def test_limit_order_delayed_fill(
        self, safe_mode_wrapper: SafeModeWrapper, mock_config: MagicMock
    ) -> None:
        """Test that limit orders have delayed fills."""
        mock_config.testing.paper_trading.limit_fill_delay_seconds = 0.1

        # Test limit order delayed fill by placing through the public API
        with (
            patch("random.random", return_value=0.5),  # Will fill
            patch("asyncio.sleep", return_value=None),  # Skip delay for faster test
        ):
            result = await safe_mode_wrapper.place_order(
                symbol="ETH_USD",
                side="BUY",
                order_type="LIMIT",
                price=3000.0,
                quantity=1.0,
            )

            # Check that order was created
            assert "order_id" in result
            order_id = result["order_id"]

            # Give time for async processing
            await asyncio.sleep(0.15)

            # Check final status through public API
            order_data = await safe_mode_wrapper.get_order(order_id)
            assert order_data["status"] == "filled"

    @pytest.mark.asyncio
    async def test_update_position_from_fill_new(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test creating new position from fill by placing order through public API."""
        # Place a buy order to create a new position
        await safe_mode_wrapper.place_order(
            symbol="SOL_USD",
            side="BUY",
            order_type="MARKET",
            price=100.0,
            quantity=10.0,
        )

        # Wait for order processing
        await asyncio.sleep(0.1)

        # Check position through public API
        positions = safe_mode_wrapper.get_simulated_positions()
        assert "SOL_USD" in positions
        position = positions["SOL_USD"]
        assert position["side"] == "BUY"
        assert position["size"] == 10.0
        assert position["entry_price"] == 100.0

    @pytest.mark.asyncio
    async def test_update_position_from_fill_add(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test adding to existing position by placing multiple orders."""
        # Create initial position
        await safe_mode_wrapper.place_order(
            symbol="BTC_USD",
            side="BUY",
            order_type="MARKET",
            price=50000.0,
            quantity=0.1,
        )

        # Wait for processing
        await asyncio.sleep(0.1)

        # Add to position with another order
        await safe_mode_wrapper.place_order(
            symbol="BTC_USD",
            side="BUY",
            order_type="MARKET",
            price=51000.0,
            quantity=0.1,
        )

        # Wait for processing
        await asyncio.sleep(0.1)

        # Check position through public API
        positions = safe_mode_wrapper.get_simulated_positions()
        assert "BTC_USD" in positions
        position = positions["BTC_USD"]
        assert position["size"] == 0.2  # 0.1 + 0.1
        # Average entry should be around 50500: (0.1 * 50000 + 0.1 * 51000) / 0.2
        assert abs(position["entry_price"] - 50500.0) < 1000  # Allow for slippage

    @pytest.mark.asyncio
    async def test_update_position_from_fill_close(
        self, safe_mode_wrapper: SafeModeWrapper
    ) -> None:
        """Test closing position completely by placing opposite orders."""
        # Create initial position with a buy order
        await safe_mode_wrapper.place_order(
            symbol="ETH-PERP",
            side="BUY",
            order_type="MARKET",
            price=3000.0,
            quantity=1.0,
        )

        # Wait for processing
        await asyncio.sleep(0.1)

        # Close position with a sell order
        await safe_mode_wrapper.place_order(
            symbol="ETH-PERP",
            side="SELL",
            order_type="MARKET",
            price=3100.0,
            quantity=1.0,
        )

        # Wait for processing
        await asyncio.sleep(0.1)

        # Position should be closed (not in positions list)
        positions = safe_mode_wrapper.get_simulated_positions()
        assert "ETH-PERP" not in positions

    def test_get_simulated_stats(self, safe_mode_wrapper):
        """Test getting simulation statistics."""
        # Test empty stats initially
        stats = safe_mode_wrapper.get_simulated_stats()
        assert stats["safe_mode"] is True
        assert stats["total_orders"] == 0
        assert stats["filled_orders"] == 0
        assert stats["total_fills"] == 0
        assert "current_balance" in stats

    def test_is_safe_mode(self, safe_mode_wrapper, safe_mode_disabled_wrapper):
        """Test checking if safe mode is active."""
        assert safe_mode_wrapper.is_safe_mode() is True
        assert safe_mode_disabled_wrapper.is_safe_mode() is False
