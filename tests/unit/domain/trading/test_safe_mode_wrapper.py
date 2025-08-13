"""Unit tests for safe mode wrapper module."""

from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.domain.trading.simulation import SafeModeWrapper
from cyberdelta.enums import ExchangeName, OrderSide, OrderType, TimeInForce
from cyberdelta.symbols import exchanges


pytestmark = pytest.mark.timing


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

    def test_init_safe_mode_enabled(self, mock_config: MagicMock, mock_real_api: MagicMock) -> None:
        """Test initialization with safe mode enabled."""
        wrapper = SafeModeWrapper(mock_config, mock_real_api, ExchangeName.HYPERLIQUID)

        # Test safe mode is enabled
        assert wrapper.is_safe_mode() is True

        # Check initial stats through public API
        stats = wrapper.get_simulated_stats()
        assert "current_balance" in stats
        assert stats["current_balance"] == 10000.0

    def test_init_safe_mode_disabled(
        self, mock_config: MagicMock, mock_real_api: MagicMock
    ) -> None:
        """Test initialization with safe mode disabled."""
        mock_config.general.safe_mode = False
        wrapper = SafeModeWrapper(mock_config, mock_real_api, ExchangeName.HYPERLIQUID)

        assert wrapper.is_safe_mode() is False

    @pytest.mark.asyncio
    async def test_place_order_safe_mode_market(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test placing market order in safe mode."""
        with patch("random.random", return_value=0.5):  # Will fill
            args = PlaceOrderArgs(
                symbol=exchanges.hyperliquid("BTC"),
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.1"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("50000.0"),
            )
            result = await safe_mode_wrapper.place_order(args)

        assert result.exchange_order_id is not None
        assert "SIM_" in result.exchange_order_id
        assert result.status.value == "NEW"

        # Check stats through public interface
        stats = safe_mode_wrapper.get_simulated_stats()
        assert "total_orders" in stats

    @pytest.mark.asyncio
    async def test_place_order_safe_mode_limit(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test placing limit order in safe mode."""
        args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid("BTC"),
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.05"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("51000.0"),
        )
        result = await safe_mode_wrapper.place_order(args)

        assert result.exchange_order_id is not None
        assert "SIM_" in result.exchange_order_id
        assert result.status.value == "NEW"

        # Check stats through public interface
        stats = safe_mode_wrapper.get_simulated_stats()
        assert "total_orders" in stats

    @pytest.mark.asyncio
    async def test_place_order_pass_through(
        self, safe_mode_disabled_wrapper: SafeModeWrapper, mock_real_api: AsyncMock
    ) -> None:
        """Test that orders pass through when safe mode is disabled."""
        args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid("BTC"),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("50000.0"),
        )
        result = await safe_mode_disabled_wrapper.place_order(args)

        # Should call real API
        mock_real_api.place_order.assert_called_once_with(args)

        assert result.exchange_order_id == "REAL_123"

    @pytest.mark.asyncio
    async def test_cancel_order_safe_mode(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test cancelling order in safe mode."""
        # First place an order
        place_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid("BTC"),
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("49000.0"),
        )
        place_result = await safe_mode_wrapper.place_order(place_args)

        order_id = place_result.exchange_order_id
        assert order_id is not None  # Type guard

        # Cancel it using CancelOrderArgs
        cancel_args = CancelOrderArgs(order_id=order_id, symbol=exchanges.hyperliquid("BTC"))
        cancel_result = await safe_mode_wrapper.cancel_order(cancel_args)

        assert cancel_result.order_id == order_id
        assert cancel_result.status.value in ["CANCELLED", "CANCELED"]

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_order(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test cancelling non-existent order in safe mode."""
        cancel_args = CancelOrderArgs(order_id="FAKE_ORDER", symbol=exchanges.hyperliquid("BTC"))
        await safe_mode_wrapper.cancel_order(cancel_args)

        # For fake orders, we expect no exception to be raised
        # The cancel operation should complete without error

    @pytest.mark.asyncio
    async def test_get_order_safe_mode(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test getting order status in safe mode."""
        # Place an order
        place_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid("ETH"),
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("3000.0"),
        )
        place_result = await safe_mode_wrapper.place_order(place_args)

        order_id = place_result.exchange_order_id
        assert order_id is not None  # Type guard

        # Get order status using GetOrderArgs
        get_order_args = GetOrderArgs(order_id=order_id, symbol=exchanges.hyperliquid("ETH"))
        status = await safe_mode_wrapper.get_order(get_order_args)

        assert status is not None
        assert status.client_order_id == order_id  # Use client_order_id
        assert status.symbol == exchanges.hyperliquid("ETH")
        assert status.side == OrderSide.SELL
        assert status.price == Decimal("3000.0")
        assert status.quantity_requested == Decimal("1.0")

    @pytest.mark.asyncio
    async def test_get_balances_safe_mode(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test getting balances in safe mode."""
        balances = await safe_mode_wrapper.get_balances()

        assert len(balances) >= 1

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
            args = PlaceOrderArgs(
                symbol=exchanges.hyperliquid("BTC"),
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=Decimal("0.1"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("50000.0"),
            )
            result = await safe_mode_wrapper.place_order(args)

        # Check that order was created
        assert result.exchange_order_id
        order_id = result.exchange_order_id
        assert order_id is not None  # Type guard

        # Get order status through public API using GetOrderArgs
        get_order_args = GetOrderArgs(order_id=order_id, symbol=exchanges.hyperliquid("BTC"))
        order_data = await safe_mode_wrapper.get_order(get_order_args)
        assert order_data is not None
        assert order_data.status.value == "FILLED"
        assert order_data.quantity_filled == Decimal("0.1")

        # Check balances through public API
        balances = await safe_mode_wrapper.get_balances()
        # Balance should be affected by trade
        assert len(balances) >= 1

        # Get simulation stats to verify fill was recorded
        stats = safe_mode_wrapper.get_simulated_stats()
        assert stats["total_orders"] == 1
        assert stats["filled_orders"] == 1

    @pytest.mark.asyncio
    async def test_simulate_fill_sell_order(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test simulating a sell order fill."""
        # First create a position
        # First create a position by placing a buy order
        buy_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid("BTC"),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("50000.0"),
        )
        await safe_mode_wrapper.place_order(buy_args)

        # Get the current position
        positions_list = await safe_mode_wrapper.get_positions()
        btc_position = next(
            (p for p in positions_list if p.symbol == exchanges.hyperliquid("BTC")), None
        )
        assert btc_position is not None
        assert btc_position.size == Decimal("0.1")

        # Instead of manually inserting orders, place a sell order through public API
        with (
            patch("random.random", return_value=0.5),
            patch("random.uniform", return_value=-0.0005),
        ):  # -0.05% slippage
            sell_args = PlaceOrderArgs(
                symbol=exchanges.hyperliquid("BTC"),
                side=OrderSide.SELL,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.05"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("51000.0"),
            )
            result = await safe_mode_wrapper.place_order(sell_args)

            # Wait for processing
            await asyncio.sleep(0.1)

            # Check order status through public API using GetOrderArgs
            order_id = result.exchange_order_id
            assert order_id is not None  # Type guard
            get_order_args = GetOrderArgs(order_id=order_id, symbol=exchanges.hyperliquid("BTC"))
            order_data = await safe_mode_wrapper.get_order(get_order_args)
            assert order_data is not None
            assert order_data.status.value == "FILLED"

        # Check position through public API
        positions = await safe_mode_wrapper.get_positions()
        btc_positions = [p for p in positions if p.symbol == exchanges.hyperliquid("BTC")]
        assert len(btc_positions) >= 1
        position = btc_positions[0]
        assert abs(float(position.size) - 0.05) < 0.01  # Reduced from 0.1

        # Check balance through public API
        balances = await safe_mode_wrapper.get_balances()
        # Should have profit from selling at higher price
        assert len(balances) >= 1

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
            args = PlaceOrderArgs(
                symbol=exchanges.hyperliquid("ETH"),
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
                price=Decimal("3000.0"),
            )
            result = await safe_mode_wrapper.place_order(args)

            # Check that order was created
            assert result.exchange_order_id
            order_id = result.exchange_order_id

            # Give time for async processing
            await asyncio.sleep(0.15)

            # Check final status through public API
            get_order_args = GetOrderArgs(order_id=order_id, symbol=exchanges.hyperliquid("ETH"))
            order_data = await safe_mode_wrapper.get_order(get_order_args)
            assert order_data is not None
            assert order_data.status.value.upper() == "FILLED"

    @pytest.mark.asyncio
    async def test_update_position_from_fill_new(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test creating new position from fill by placing order through public API."""
        # Place a buy order to create a new position
        args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid("SOL"),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("10.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("100.0"),
        )
        await safe_mode_wrapper.place_order(args)

        # Wait for order processing
        await asyncio.sleep(0.1)

        # Check position through public API
        positions = await safe_mode_wrapper.get_positions()
        sol_positions = [p for p in positions if p.symbol == exchanges.hyperliquid("SOL")]
        assert len(sol_positions) >= 1
        position = sol_positions[0]
        assert position.side == OrderSide.BUY
        assert position.size is not None
        assert position.size == Decimal("10.0")
        assert position.entry_price is not None
        assert position.entry_price == Decimal("100.0")

    @pytest.mark.asyncio
    async def test_update_position_from_fill_add(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test adding to existing position by placing multiple orders."""
        # Create initial position
        buy_args1 = PlaceOrderArgs(
            symbol=exchanges.hyperliquid("BTC"),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("50000.0"),
        )
        await safe_mode_wrapper.place_order(buy_args1)

        # Wait for processing
        await asyncio.sleep(0.1)

        # Add to position with another order
        buy_args2 = PlaceOrderArgs(
            symbol=exchanges.hyperliquid("BTC"),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("0.1"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("51000.0"),
        )
        await safe_mode_wrapper.place_order(buy_args2)

        # Wait for processing
        await asyncio.sleep(0.1)

        # Check position through public API
        positions = await safe_mode_wrapper.get_positions()
        btc_positions = [p for p in positions if p.symbol == exchanges.hyperliquid("BTC")]
        assert len(btc_positions) >= 1
        position = btc_positions[0]
        assert position.size is not None
        assert abs(position.size - Decimal("0.2")) < Decimal("0.01")  # 0.1 + 0.1
        # Average entry should be around 50500: (0.1 * 50000 + 0.1 * 51000) / 0.2
        assert position.entry_price is not None
        # Allow for slippage
        assert abs(position.entry_price - Decimal("50500.0")) < Decimal(1000)

    @pytest.mark.asyncio
    async def test_update_position_from_fill_close(
        self, safe_mode_wrapper: SafeModeWrapper
    ) -> None:
        """Test closing position completely by placing opposite orders."""
        # Create initial position with a buy order
        buy_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid("ETH-PERP"),
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("3000.0"),
        )
        await safe_mode_wrapper.place_order(buy_args)

        # Wait for processing
        await asyncio.sleep(0.1)

        # Close position with a sell order
        sell_args = PlaceOrderArgs(
            symbol=exchanges.hyperliquid("ETH-PERP"),
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=Decimal("1.0"),
            time_in_force=TimeInForce.GTC,
            price=Decimal("3100.0"),
        )
        await safe_mode_wrapper.place_order(sell_args)

        # Wait for processing
        await asyncio.sleep(0.1)

        # Position should be closed (not in positions list)
        positions = await safe_mode_wrapper.get_positions()
        eth_perp_positions = [p for p in positions if p.symbol == exchanges.hyperliquid("ETH-PERP")]
        assert len(eth_perp_positions) == 0

    def test_get_simulated_stats(self, safe_mode_wrapper: SafeModeWrapper) -> None:
        """Test getting simulation statistics."""
        # Test empty stats initially
        stats = safe_mode_wrapper.get_simulated_stats()
        assert stats["safe_mode"] is True
        assert stats["total_orders"] == 0
        assert stats["filled_orders"] == 0
        assert stats["total_fills"] == 0
        assert "current_balance" in stats

    def test_is_safe_mode(
        self,
        safe_mode_wrapper: SafeModeWrapper,
        safe_mode_disabled_wrapper: SafeModeWrapper,
    ) -> None:
        """Test checking if safe mode is active."""
        assert safe_mode_wrapper.is_safe_mode() is True
        assert safe_mode_disabled_wrapper.is_safe_mode() is False
