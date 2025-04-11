"""
Tests for the PortfolioTracker class.
"""

import pytest
from unittest.mock import MagicMock, AsyncMock
from datetime import datetime, timedelta

from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.models import Position, Order, OrderSide, OrderType, OrderStatus


class TestPortfolioTracker:
    """Test suite for the PortfolioTracker class."""

    @pytest.fixture
    def config(self):
        """Create a mock config for testing."""
        config = MagicMock()
        config.get.side_effect = lambda key, default=None: {
            "exchanges": {"hyperliquid": {}, "backpack": {}},
            "exchanges.hyperliquid.enabled": True,
            "exchanges.backpack.enabled": True,
            "portfolio.reconciliation_interval": 300,
        }.get(key, default)
        return config

    @pytest.fixture
    def api_clients(self):
        """Create mock API clients for testing."""
        hyperliquid_client = AsyncMock()
        backpack_client = AsyncMock()

        return {"hyperliquid": hyperliquid_client, "backpack": backpack_client}

    @pytest.fixture
    def portfolio_tracker(self, config, api_clients):
        """Create a PortfolioTracker instance for testing."""
        tracker = PortfolioTracker(config)

        # Register API clients
        for exchange_id, client in api_clients.items():
            tracker.register_api_client(exchange_id, client)

        return tracker

    @pytest.fixture
    def sample_positions(self):
        """Create sample positions for testing."""
        return {
            "hyperliquid": [
                Position(
                    symbol="BTC",
                    size=1.0,
                    entry_price=50000.0,
                    mark_price=51000.0,
                    liquidation_price=45000.0,
                    unrealized_pnl=1000.0,
                    leverage=2.0,
                    side=OrderSide.BUY,
                )
            ],
            "backpack": [
                Position(
                    symbol="ETH",
                    size=10.0,
                    entry_price=3000.0,
                    mark_price=3100.0,
                    liquidation_price=2800.0,
                    unrealized_pnl=1000.0,
                    leverage=1.0,
                    side=OrderSide.SELL,
                )
            ],
        }

    @pytest.fixture
    def sample_orders(self):
        """Create sample orders for testing."""
        return {
            "hyperliquid": [
                Order(
                    id="hl-order-1",
                    symbol="BTC",
                    side=OrderSide.BUY,
                    type=OrderType.LIMIT,
                    price=49000.0,
                    quantity=0.5,
                    filled_quantity=0.0,
                    status=OrderStatus.NEW.value,
                    time=int(datetime.now().timestamp() * 1000),
                    client_order_id="client-order-1",
                )
            ],
            "backpack": [
                Order(
                    id="bp-order-1",
                    symbol="ETH",
                    side=OrderSide.SELL,
                    type=OrderType.MARKET,
                    price=3050.0,
                    quantity=5.0,
                    filled_quantity=5.0,
                    status=OrderStatus.FILLED.value,
                    time=int(datetime.now().timestamp() * 1000),
                    client_order_id="client-order-2",
                )
            ],
        }

    @pytest.fixture
    def sample_balances(self):
        """Create sample balances for testing."""
        return {
            "hyperliquid": {"USDC": 100000.0, "BTC": 2.0},
            "backpack": {"USDC": 50000.0, "ETH": 20.0},
        }

    @pytest.mark.asyncio
    async def test_initialize(
        self,
        portfolio_tracker,
        api_clients,
        sample_positions,
        sample_orders,
        sample_balances,
    ):
        """Test initializing the portfolio tracker."""
        # Setup mock responses
        for exchange_id, client in api_clients.items():
            client.get_balances.return_value = sample_balances[exchange_id]
            client.get_positions.return_value = sample_positions[exchange_id]
            client.get_open_orders.return_value = sample_orders[exchange_id]

        # Initialize the portfolio tracker
        await portfolio_tracker.initialize()

        # Verify API clients were called
        for exchange_id, client in api_clients.items():
            client.get_balances.assert_called_once()
            client.get_positions.assert_called_once()
            client.get_open_orders.assert_called_once()

        # Verify internal state was updated
        assert (
            portfolio_tracker._balances["hyperliquid"] == sample_balances["hyperliquid"]
        )
        assert portfolio_tracker._balances["backpack"] == sample_balances["backpack"]

        # Verify positions were stored properly
        for exchange_id, positions in sample_positions.items():
            for position in positions:
                stored_position = portfolio_tracker._positions[exchange_id].get(
                    position.symbol
                )
                assert stored_position is not None

        # Verify orders were stored properly
        for exchange_id, orders in sample_orders.items():
            for order in orders:
                stored_order = portfolio_tracker._orders[exchange_id].get(order.id)
                assert stored_order is not None

    @pytest.mark.asyncio
    async def test_update(
        self,
        portfolio_tracker,
        api_clients,
        sample_positions,
        sample_orders,
        sample_balances,
    ):
        """Test updating the portfolio tracker."""
        # Setup mock responses
        for exchange_id, client in api_clients.items():
            client.get_balances.return_value = sample_balances[exchange_id]
            client.get_positions.return_value = sample_positions[exchange_id]
            client.get_open_orders.return_value = sample_orders[exchange_id]

        # Initialize the tracker first
        await portfolio_tracker.initialize()

        # Reset the mock call counts
        for exchange_id, client in api_clients.items():
            client.get_balances.reset_mock()
            client.get_positions.reset_mock()
            client.get_open_orders.reset_mock()

        # Update without reconciliation (update only positions and orders)
        await portfolio_tracker.update()

        # Verify only positions and orders were updated
        for exchange_id, client in api_clients.items():
            client.get_balances.assert_not_called()
            client.get_positions.assert_called_once()
            client.get_open_orders.assert_called_once()

        # Force reconciliation by setting last reconciliation time to past
        for exchange_id in api_clients.keys():
            portfolio_tracker._last_reconciliation_time[exchange_id] = (
                datetime.now() - timedelta(seconds=600)
            )

        # Reset the mock call counts
        for exchange_id, client in api_clients.items():
            client.get_balances.reset_mock()
            client.get_positions.reset_mock()
            client.get_open_orders.reset_mock()

        # Update with reconciliation (update balances, positions, and orders)
        await portfolio_tracker.update()

        # Verify all data was updated
        for exchange_id, client in api_clients.items():
            client.get_balances.assert_called_once()
            client.get_positions.assert_called_once()
            client.get_open_orders.assert_called_once()

    def test_update_order(self, portfolio_tracker):
        """Test updating an order in the portfolio tracker."""
        # Create a new order
        order = Order(
            id="test-order-1",
            symbol="BTC",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=50000.0,
            quantity=1.0,
            filled_quantity=0.0,
            status=OrderStatus.NEW.value,
            time=int(datetime.now().timestamp() * 1000),
            client_order_id="client-order-3",
        )

        # Update the order in the tracker
        portfolio_tracker.update_order("hyperliquid", order)

        # Verify the order was stored
        stored_order = portfolio_tracker.get_order("hyperliquid", "test-order-1")
        assert stored_order is not None
        assert stored_order.id == "test-order-1"
        assert stored_order.symbol == "BTC"
        assert stored_order.side == OrderSide.BUY

        # Update the order status to filled
        order.status = OrderStatus.FILLED.value
        order.filled_quantity = 1.0
        portfolio_tracker.update_order("hyperliquid", order)

        # Verify the order was updated
        stored_order = portfolio_tracker.get_order("hyperliquid", "test-order-1")
        assert stored_order.status == OrderStatus.FILLED.value
        assert stored_order.filled_quantity == 1.0

    def test_update_position(self, portfolio_tracker):
        """Test updating a position in the portfolio tracker."""
        # Create a new position
        position = Position(
            symbol="BTC",
            size=1.0,
            entry_price=50000.0,
            mark_price=51000.0,
            liquidation_price=45000.0,
            unrealized_pnl=1000.0,
            leverage=2.0,
            side=OrderSide.BUY,
        )

        # Update the position in the tracker
        portfolio_tracker.update_position("hyperliquid", position)

        # Verify the position was stored
        stored_position = portfolio_tracker.get_position("hyperliquid", "BTC")
        assert stored_position is not None
        assert stored_position.symbol == "BTC"
        assert stored_position.size == 1.0
        assert stored_position.entry_price == 50000.0

        # Update the position mark price
        position.mark_price = 52000.0
        position.unrealized_pnl = 2000.0
        portfolio_tracker.update_position("hyperliquid", position)

        # Verify the position was updated
        stored_position = portfolio_tracker.get_position("hyperliquid", "BTC")
        assert stored_position.mark_price == 52000.0
        assert stored_position.unrealized_pnl == 2000.0

    def test_update_balance(self, portfolio_tracker):
        """Test updating a balance in the portfolio tracker."""
        # Update a balance in the tracker
        portfolio_tracker.update_balance("hyperliquid", "USDC", 100000.0)

        # Verify the balance was stored
        balance = portfolio_tracker.get_exchange_balance("hyperliquid", "USDC")
        assert balance == 100000.0

        # Update the balance
        portfolio_tracker.update_balance("hyperliquid", "USDC", 90000.0)

        # Verify the balance was updated
        balance = portfolio_tracker.get_exchange_balance("hyperliquid", "USDC")
        assert balance == 90000.0

    def test_get_exchange_balance(self, portfolio_tracker, sample_balances):
        """Test getting an exchange balance."""
        # Set up some balances
        for exchange_id, balances in sample_balances.items():
            for asset, amount in balances.items():
                portfolio_tracker.update_balance(exchange_id, asset, amount)

        # Test getting balances
        assert portfolio_tracker.get_exchange_balance("hyperliquid", "USDC") == 100000.0
        assert portfolio_tracker.get_exchange_balance("backpack", "ETH") == 20.0

        # Test getting a non-existent balance
        assert portfolio_tracker.get_exchange_balance("hyperliquid", "ETH") == 0.0

    def test_get_total_capital(self, portfolio_tracker, sample_balances):
        """Test getting the total capital across all exchanges."""
        # Set up some balances
        for exchange_id, balances in sample_balances.items():
            for asset, amount in balances.items():
                portfolio_tracker.update_balance(exchange_id, asset, amount)

        # Expected total capital: 100000 + 50000 = 150000 USDC
        assert portfolio_tracker.get_total_capital() == 150000.0

    def test_get_position(self, portfolio_tracker, sample_positions):
        """Test getting a position by ID."""
        # Set up some positions
        for exchange_id, positions in sample_positions.items():
            for position in positions:
                portfolio_tracker.update_position(exchange_id, position)

        # Test getting positions
        btc_position = portfolio_tracker.get_position("hyperliquid", "BTC")
        assert btc_position is not None
        assert btc_position.symbol == "BTC"
        assert btc_position.size == 1.0

        eth_position = portfolio_tracker.get_position("backpack", "ETH")
        assert eth_position is not None
        assert eth_position.symbol == "ETH"
        assert eth_position.size == 10.0

        # Test getting a non-existent position
        assert portfolio_tracker.get_position("hyperliquid", "ETH") is None

    def test_get_positions_by_symbol(self, portfolio_tracker, sample_positions):
        """Test getting positions by symbol."""
        # Set up some positions
        for exchange_id, positions in sample_positions.items():
            for position in positions:
                portfolio_tracker.update_position(exchange_id, position)

        # Add another BTC position to backpack
        btc_position = Position(
            symbol="BTC",
            size=0.5,
            entry_price=49000.0,
            mark_price=51000.0,
            liquidation_price=45000.0,
            unrealized_pnl=1000.0,
            leverage=1.0,
            side=OrderSide.BUY,
        )
        portfolio_tracker.update_position("backpack", btc_position)

        # Test getting positions by symbol
        btc_positions = portfolio_tracker.get_positions_by_symbol("backpack", "BTC")
        assert len(btc_positions) == 1
        assert btc_positions[0].symbol == "BTC"
        assert btc_positions[0].size == 0.5

    def test_get_all_positions(self, portfolio_tracker, sample_positions):
        """Test getting all positions across exchanges."""
        # Set up some positions
        for exchange_id, positions in sample_positions.items():
            for position in positions:
                portfolio_tracker.update_position(exchange_id, position)

        # Test getting all positions
        all_positions = portfolio_tracker.get_all_positions()
        assert len(all_positions) == 2

        # Verify positions are returned correctly
        exchanges = [exchange for exchange, _ in all_positions]
        symbols = [position.symbol for _, position in all_positions]

        assert "hyperliquid" in exchanges
        assert "backpack" in exchanges
        assert "BTC" in symbols
        assert "ETH" in symbols

    def test_get_order(self, portfolio_tracker, sample_orders):
        """Test getting an order by ID."""
        # Set up some orders
        for exchange_id, orders in sample_orders.items():
            for order in orders:
                portfolio_tracker.update_order(exchange_id, order)

        # Test getting orders
        hl_order = portfolio_tracker.get_order("hyperliquid", "hl-order-1")
        assert hl_order is not None
        assert hl_order.id == "hl-order-1"
        assert hl_order.symbol == "BTC"

        bp_order = portfolio_tracker.get_order("backpack", "bp-order-1")
        assert bp_order is not None
        assert bp_order.id == "bp-order-1"
        assert bp_order.symbol == "ETH"

        # Test getting a non-existent order
        assert portfolio_tracker.get_order("hyperliquid", "non-existent") is None

    def test_get_open_orders(self, portfolio_tracker, sample_orders):
        """Test getting open orders."""
        # Set up some orders
        for exchange_id, orders in sample_orders.items():
            for order in orders:
                portfolio_tracker.update_order(exchange_id, order)

        # Add more orders with different statuses
        cancelled_order = Order(
            id="hl-order-2",
            symbol="BTC",
            side=OrderSide.SELL,
            type=OrderType.LIMIT,
            price=53000.0,
            quantity=0.3,
            filled_quantity=0.0,
            status=OrderStatus.CANCELED.value,
            time=int(datetime.now().timestamp() * 1000),
            client_order_id="client-order-4",
        )
        portfolio_tracker.update_order("hyperliquid", cancelled_order)

        # Test getting open orders (only NEW and PARTIALLY_FILLED should be included)
        open_orders = portfolio_tracker.get_open_orders("hyperliquid")
        assert len(open_orders) == 1
        assert open_orders[0].id == "hl-order-1"
        assert open_orders[0].status == OrderStatus.NEW.value

        # Test getting open orders by symbol
        open_btc_orders = portfolio_tracker.get_open_orders("hyperliquid", "BTC")
        assert len(open_btc_orders) == 1
        assert open_btc_orders[0].id == "hl-order-1"

        # Test getting open orders from an exchange with only filled orders
        open_bp_orders = portfolio_tracker.get_open_orders("backpack")
        assert len(open_bp_orders) == 0

    def test_to_dict_and_from_dict(
        self, portfolio_tracker, sample_balances, sample_positions, sample_orders
    ):
        """Test serialization and deserialization of the portfolio tracker state."""
        # Set up some test data
        for exchange_id, balances in sample_balances.items():
            for asset, amount in balances.items():
                portfolio_tracker.update_balance(exchange_id, asset, amount)

        for exchange_id, positions in sample_positions.items():
            for position in positions:
                portfolio_tracker.update_position(exchange_id, position)

        for exchange_id, orders in sample_orders.items():
            for order in orders:
                portfolio_tracker.update_order(exchange_id, order)

        # Serialize the state
        state_dict = portfolio_tracker.to_dict()

        # Create a new portfolio tracker
        new_tracker = PortfolioTracker(portfolio_tracker.config)

        # Deserialize the state
        new_tracker.from_dict(state_dict)

        # Verify balances were restored
        for exchange_id, balances in sample_balances.items():
            for asset, amount in balances.items():
                assert new_tracker.get_exchange_balance(exchange_id, asset) == amount

        # Verify positions were restored
        for exchange_id, positions in sample_positions.items():
            for position in positions:
                restored_position = new_tracker.get_position(
                    exchange_id, position.symbol
                )
                assert restored_position is not None
                assert restored_position.symbol == position.symbol
                assert restored_position.size == position.size

        # Verify orders were restored
        for exchange_id, orders in sample_orders.items():
            for order in orders:
                restored_order = new_tracker.get_order(exchange_id, order.id)
                assert restored_order is not None
                assert restored_order.id == order.id
                assert restored_order.symbol == order.symbol
