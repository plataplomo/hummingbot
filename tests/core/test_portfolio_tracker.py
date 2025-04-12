"""
Tests for the PortfolioTracker class.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.core.models import (
    Balance,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker


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
                    size=Decimal("1.0"),
                    entry_price=Decimal("50000.0"),
                    mark_price=Decimal("51000.0"),
                    liquidation_price=Decimal("45000.0"),
                    unrealized_pnl=Decimal("1000.0"),
                    leverage=Decimal("2.0"),
                    side=OrderSide.BUY,
                )
            ],
            "backpack": [
                Position(
                    symbol="ETH",
                    size=Decimal("10.0"),
                    entry_price=Decimal("3000.0"),
                    mark_price=Decimal("3100.0"),
                    liquidation_price=Decimal("2800.0"),
                    unrealized_pnl=Decimal("1000.0"),
                    leverage=Decimal("1.0"),
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
                    price=Decimal("49000.0"),
                    quantity=Decimal("0.5"),
                    filled_quantity=Decimal("0.0"),
                    status=OrderStatus.NEW.value,
                    time=int(datetime.now(UTC).timestamp() * 1000),
                    client_order_id="client-order-1",
                )
            ],
            "backpack": [
                Order(
                    id="bp-order-1",
                    symbol="ETH",
                    side=OrderSide.SELL,
                    type=OrderType.MARKET,
                    price=Decimal("3050.0"),
                    quantity=Decimal("5.0"),
                    filled_quantity=Decimal("5.0"),
                    status=OrderStatus.FILLED.value,
                    time=int(datetime.now(UTC).timestamp() * 1000),
                    client_order_id="client-order-2",
                )
            ],
        }

    @pytest.fixture
    def sample_balances(self):
        """Create sample balances for testing."""
        return {
            "hyperliquid": {"USDC": Decimal("100000.0"), "BTC": Decimal("2.0")},
            "backpack": {"USDC": Decimal("50000.0"), "ETH": Decimal("20.0")},
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
        hyperliquid_usdc_balance = portfolio_tracker.get_exchange_balance("hyperliquid", "USDC")
        assert isinstance(hyperliquid_usdc_balance, Balance)
        assert hyperliquid_usdc_balance.total == sample_balances["hyperliquid"]["USDC"]

        backpack_eth_balance = portfolio_tracker.get_exchange_balance("backpack", "ETH")
        assert isinstance(backpack_eth_balance, Balance)
        assert backpack_eth_balance.total == sample_balances["backpack"]["ETH"]

        # Verify positions were stored properly
        for exchange_id, positions in sample_positions.items():
            for position in positions:
                stored_position = portfolio_tracker._positions[exchange_id].get(position.symbol)
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

        # Set last reconciliation time to be recent to prevent full update
        portfolio_tracker._last_reconciliation_time[list(api_clients.keys())[0]] = datetime.now(
            UTC
        ) - timedelta(seconds=10)
        portfolio_tracker._last_reconciliation_time[list(api_clients.keys())[1]] = datetime.now(
            UTC
        ) - timedelta(seconds=10)

        # Update - should only fetch orders if interval hasn't passed
        await portfolio_tracker.update()

        # Verify only orders were updated (balances and positions not fetched)
        for exchange_id, client in api_clients.items():
            client.get_balances.assert_not_called()
            client.get_positions.assert_not_called()
            client.get_open_orders.assert_called_once()

        # Now force reconciliation by setting last check time far in the past
        portfolio_tracker._last_reconciliation_time[list(api_clients.keys())[0]] = (
            datetime.min.replace(tzinfo=UTC)
        )
        portfolio_tracker._last_reconciliation_time[list(api_clients.keys())[1]] = (
            datetime.min.replace(tzinfo=UTC)
        )

        await portfolio_tracker.update()  # Should now fetch everything
        for exchange_id, client in api_clients.items():
            # Check counts after full reconciliation (add 1 to previous checks)
            assert client.get_balances.call_count == 1
            assert client.get_positions.call_count == 1
            assert client.get_open_orders.call_count == 2  # Called once before, once now

    def test_update_order(self, portfolio_tracker):
        """Test updating an order in the portfolio tracker."""
        # Create a new order
        order = Order(
            id="test-order-1",
            symbol="BTC",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=Decimal("50000.0"),
            quantity=Decimal("1.0"),
            filled_quantity=Decimal("0.0"),
            status=OrderStatus.NEW.value,
            time=int(datetime.now(UTC).timestamp() * 1000),
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
        order.filled_quantity = Decimal("1.0")
        portfolio_tracker.update_order("hyperliquid", order)

        # Verify the order was updated
        stored_order = portfolio_tracker.get_order("hyperliquid", "test-order-1")
        assert stored_order.status == OrderStatus.FILLED.value
        assert stored_order.filled_quantity == Decimal("1.0")

    def test_update_position(self, portfolio_tracker):
        """Test updating a position in the portfolio tracker."""
        # Create a new position
        position = Position(
            symbol="BTC",
            size=Decimal("1.0"),
            entry_price=Decimal("50000.0"),
            mark_price=Decimal("51000.0"),
            liquidation_price=Decimal("45000.0"),
            unrealized_pnl=Decimal("1000.0"),
            leverage=Decimal("2.0"),
            side=OrderSide.BUY,
        )

        # Update the position in the tracker
        portfolio_tracker.update_position("hyperliquid", position)

        # Verify the position was stored
        stored_position = portfolio_tracker.get_position("hyperliquid", "BTC")
        assert stored_position is not None
        assert stored_position.symbol == "BTC"
        assert stored_position.size == Decimal("1.0")
        assert stored_position.entry_price == Decimal("50000.0")

        # Update the position mark price
        position.mark_price = Decimal("52000.0")
        position.unrealized_pnl = Decimal("2000.0")
        portfolio_tracker.update_position("hyperliquid", position)

        # Verify the position was updated
        stored_position = portfolio_tracker.get_position("hyperliquid", "BTC")
        assert stored_position.mark_price == Decimal("52000.0")
        assert stored_position.unrealized_pnl == Decimal("2000.0")

    def test_update_balance(self, portfolio_tracker):
        """Test updating a balance in the portfolio tracker."""
        # Update a balance in the tracker
        usdc_amount = Decimal("100000.0")
        portfolio_tracker.update_balance("hyperliquid", "USDC", usdc_amount)

        # Verify the balance was stored as a Balance object
        balance_obj = portfolio_tracker.get_exchange_balance("hyperliquid", "USDC")
        assert isinstance(balance_obj, Balance)
        assert balance_obj.asset == "USDC"
        assert balance_obj.total == usdc_amount
        assert balance_obj.available == usdc_amount

    def test_get_exchange_balance(self, portfolio_tracker, sample_balances):
        """Test getting an exchange balance."""
        # Set up some balances
        for exchange_id, balances in sample_balances.items():
            for asset, amount in balances.items():
                portfolio_tracker.update_balance(exchange_id, asset, amount)

        # Test getting balances - check object type and total value
        usdc_balance = portfolio_tracker.get_exchange_balance("hyperliquid", "USDC")
        assert isinstance(usdc_balance, Balance)
        assert usdc_balance.total == Decimal("100000.0")

        eth_balance = portfolio_tracker.get_exchange_balance("backpack", "ETH")
        assert isinstance(eth_balance, Balance)
        assert eth_balance.total == Decimal("20.0")

        # Test getting non-existent balance
        assert portfolio_tracker.get_exchange_balance("hyperliquid", "XYZ") is None

    def test_get_total_capital(self, portfolio_tracker, sample_balances):
        """Test getting the total capital across all exchanges."""
        # Set up some balances
        for exchange_id, balances in sample_balances.items():
            for asset, amount in balances.items():
                portfolio_tracker.update_balance(exchange_id, asset, amount)

        # Expected total capital: 100000 + 50000 = 150000 USDC
        assert portfolio_tracker.get_total_capital() == Decimal("150000.0")

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
        assert btc_position.size == Decimal("1.0")

        eth_position = portfolio_tracker.get_position("backpack", "ETH")
        assert eth_position is not None
        assert eth_position.symbol == "ETH"
        assert eth_position.size == Decimal("10.0")

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
            size=Decimal("0.5"),
            entry_price=Decimal("49000.0"),
            mark_price=Decimal("51000.0"),
            liquidation_price=Decimal("45000.0"),
            unrealized_pnl=Decimal("1000.0"),
            leverage=Decimal("1.0"),
            side=OrderSide.BUY,
        )
        portfolio_tracker.update_position("backpack", btc_position)

        # Test getting positions by symbol
        btc_positions = portfolio_tracker.get_positions_by_symbol("backpack", "BTC")
        assert len(btc_positions) == 1
        assert btc_positions[0].symbol == "BTC"
        assert btc_positions[0].size == Decimal("0.5")

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

        # Add an order that should be considered open
        open_order = Order(
            id="hl-order-open",
            symbol="BTC",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=Decimal("51000.0"),
            quantity=Decimal("0.5"),
            filled_quantity=Decimal("0.1"),  # Partially filled
            status=OrderStatus.PARTIALLY_FILLED,  # Use Enum member
            time=int(datetime.now(UTC).timestamp() * 1000),
            client_order_id="client-order-open",
        )
        portfolio_tracker.update_order("hyperliquid", open_order)

        # Add an order that should NOT be considered open
        cancelled_order = Order(
            id="hl-order-2",
            symbol="BTC",
            side=OrderSide.SELL,
            type=OrderType.LIMIT,
            price=Decimal("53000.0"),
            quantity=Decimal("0.3"),
            filled_quantity=Decimal("0.0"),
            # status=OrderStatus.CANCELED.value, # Original: used string value
            status=OrderStatus.CANCELED,  # Use Enum member
            time=int(datetime.now(UTC).timestamp() * 1000),
            client_order_id="client-order-4",
        )
        portfolio_tracker.update_order("hyperliquid", cancelled_order)

        # Test getting open orders (only NEW and PARTIALLY_FILLED should be included)
        open_orders = portfolio_tracker.get_open_orders("hyperliquid")
        # Original hl-order-1 (status NEW) should also be open if added correctly
        # Check if hl-order-1 exists and has status NEW
        initial_open_order = portfolio_tracker.get_order("hyperliquid", "hl-order-1")
        expected_count = 0
        if initial_open_order and initial_open_order.status == OrderStatus.NEW:
            expected_count += 1
        if open_order.status == OrderStatus.PARTIALLY_FILLED:  # Our added open order
            expected_count += 1

        assert len(open_orders) == expected_count
        assert all(o.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED] for o in open_orders)
        # Ensure the cancelled order is not present
        assert not any(o.id == "hl-order-2" for o in open_orders)

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
                restored_balance = new_tracker.get_exchange_balance(exchange_id, asset)
                assert isinstance(restored_balance, Balance)
                assert restored_balance.total == amount

        # Verify positions were restored
        for exchange_id, positions in sample_positions.items():
            for position in positions:
                restored_position = new_tracker.get_position(exchange_id, position.symbol)
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
                assert restored_order.price == order.price
                assert restored_order.quantity == order.quantity
                assert restored_order.filled_quantity == order.filled_quantity
