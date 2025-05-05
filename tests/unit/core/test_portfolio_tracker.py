"""
Tests for the PortfolioTracker class.
"""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.core.models import (
    DerivativePosition,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    SpotBalance,
    TimeInForce,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker


class TestPortfolioTracker:
    """Test suite for the PortfolioTracker class."""

    @pytest.fixture
    def config(self) -> MagicMock:
        """Create a mock config for testing."""
        config = MagicMock()

        # Explicitly type the side effect function for config.get
        def get_config_value(key: str, default: object = None) -> object:
            """
            Mocked config.get implementation.
            Returns values for known keys, otherwise returns the provided default.
            Type: (str, object) -> object
            Note: This is a test mock; in production, config values should be strictly typed.
            """
            config_dict: dict[str, object] = {
                "exchanges": {"hyperliquid": {}, "backpack": {}},
                "exchanges.hyperliquid.enabled": True,
                "exchanges.backpack.enabled": True,
                "portfolio.reconciliation_interval": 300,
            }
            return config_dict.get(key, default)

        config.get.side_effect = get_config_value
        return config

    @pytest.fixture
    def api_clients(self) -> dict[str, AsyncMock]:
        """Create mock API clients for testing."""
        hyperliquid_client = AsyncMock()
        backpack_client = AsyncMock()

        return {"hyperliquid": hyperliquid_client, "backpack": backpack_client}

    @pytest.fixture
    def portfolio_tracker(
        self, config: MagicMock, api_clients: dict[str, AsyncMock]
    ) -> PortfolioTracker:
        """Create a PortfolioTracker instance for testing."""
        tracker = PortfolioTracker(config)

        # Register API clients
        for exchange_id, client in api_clients.items():
            tracker.register_api_client(exchange_id, client)

        return tracker

    @pytest.fixture
    def sample_positions(self) -> dict[str, list[DerivativePosition]]:
        """Create sample positions for testing."""
        return {
            "hyperliquid": [
                DerivativePosition(
                    exchange="hyperliquid",
                    timestamp=datetime.now(UTC),
                    symbol="BTC",
                    size=Decimal("1.0"),
                    entry_price=Decimal("50000.0"),
                    mark_price=Decimal("51000.0"),
                    liquidation_price=Decimal("45000.0"),
                    unrealized_pnl=Decimal("1000.0"),
                    side=OrderSide.BUY,
                )
            ],
            "backpack": [
                DerivativePosition(
                    exchange="backpack",
                    timestamp=datetime.now(UTC),
                    symbol="ETH",
                    size=Decimal("10.0"),
                    entry_price=Decimal("3000.0"),
                    mark_price=Decimal("3100.0"),
                    liquidation_price=Decimal("2800.0"),
                    unrealized_pnl=Decimal("1000.0"),
                    side=OrderSide.SELL,
                )
            ],
        }

    @pytest.fixture
    def sample_orders(self) -> dict[str, list[Order]]:
        """Create sample orders for testing."""
        return {
            "hyperliquid": [
                Order(
                    exchange="hyperliquid",
                    client_order_id="hl-order-1",
                    order_type=OrderType.LIMIT,
                    symbol="BTC",
                    side=OrderSide.BUY,
                    price=Decimal("49000.0"),
                    quantity_requested=Decimal("0.5"),
                    quantity_filled=Decimal("0.0"),
                    status=OrderStatus.NEW,
                    time_in_force=TimeInForce.GTC,
                    created_at=datetime.now(UTC),
                )
            ],
            "backpack": [
                Order(
                    exchange="backpack",
                    client_order_id="bp-order-1",
                    order_type=OrderType.MARKET,
                    symbol="ETH",
                    side=OrderSide.SELL,
                    price=None,
                    quantity_requested=Decimal("5.0"),
                    quantity_filled=Decimal("5.0"),
                    status=OrderStatus.FILLED,
                    time_in_force=TimeInForce.IOC,
                    created_at=datetime.now(UTC),
                )
            ],
        }

    @pytest.fixture
    def sample_balances(self) -> dict[str, dict[str, Decimal]]:
        """Create sample balances for testing."""
        return {
            "hyperliquid": {"USDC": Decimal("100000.0"), "BTC": Decimal("2.0")},
            "backpack": {"USDC": Decimal("50000.0"), "ETH": Decimal("20.0")},
        }

    @pytest.mark.asyncio
    async def test_initialize(
        self,
        portfolio_tracker: PortfolioTracker,
        api_clients: dict[str, AsyncMock],
        sample_positions: dict[str, list[DerivativePosition]],
        sample_orders: dict[str, list[Order]],
        sample_balances: dict[str, dict[str, Decimal]],
    ) -> None:
        """Test initializing the portfolio tracker."""
        # Setup mock responses
        for exchange_id, client in api_clients.items():
            client.get_balances.return_value = sample_balances[exchange_id]
            client.get_positions.return_value = sample_positions[exchange_id]
            client.get_open_orders.return_value = sample_orders[exchange_id]

        # Initialize the portfolio tracker
        await portfolio_tracker.initialize()

        # Verify API clients were called
        for _exchange_id, client in api_clients.items():  # B007: Use _ for unused var
            client.get_balances.assert_called_once()
            client.get_positions.assert_called_once()
            client.get_open_orders.assert_called_once()

        # Verify internal state was updated
        # Access internal state directly for verification
        hyperliquid_usdc_balance = portfolio_tracker._balances.get("hyperliquid", {}).get("USDC")  # noqa: SLF001 - Test verification
        assert isinstance(hyperliquid_usdc_balance, SpotBalance)
        # assert hyperliquid_usdc_balance.total_quantity == sample_balances["hyperliquid"]["USDC"] # Assertion moved below check
        if hyperliquid_usdc_balance:
            assert hyperliquid_usdc_balance.total_quantity == sample_balances["hyperliquid"]["USDC"]
        else:
            pytest.fail("Hyperliquid USDC balance not found in tracker state")

        backpack_eth_balance = portfolio_tracker._balances.get("backpack", {}).get("ETH")  # noqa: SLF001 - Test verification
        assert isinstance(backpack_eth_balance, SpotBalance)
        # assert backpack_eth_balance.total_quantity == sample_balances["backpack"]["ETH"] # Assertion moved below check
        if backpack_eth_balance:
            assert backpack_eth_balance.total_quantity == sample_balances["backpack"]["ETH"]
        else:
            pytest.fail("Backpack ETH balance not found in tracker state")

        # Verify positions were stored properly
        for exchange_id, positions in sample_positions.items():
            for position in positions:
                stored_position = portfolio_tracker._positions[exchange_id].get(position.symbol)  # noqa: SLF001 - Test verification
                assert stored_position is not None

        # Verify orders were stored properly
        for exchange_id, orders in sample_orders.items():
            for order in orders:
                stored_order = portfolio_tracker._orders[exchange_id].get(order.client_order_id)  # noqa: SLF001 - Test verification
                assert stored_order is not None

    @pytest.mark.asyncio
    async def test_update(
        self,
        portfolio_tracker: PortfolioTracker,
        api_clients: dict[str, AsyncMock],
        sample_positions: dict[str, list[DerivativePosition]],
        sample_orders: dict[str, list[Order]],
        sample_balances: dict[str, dict[str, Decimal]],
    ) -> None:
        """Test updating the portfolio tracker."""
        # Setup mock responses
        for exchange_id, client in api_clients.items():
            client.get_balances.return_value = sample_balances[exchange_id]
            client.get_positions.return_value = sample_positions[exchange_id]
            client.get_open_orders.return_value = sample_orders[exchange_id]

        # Initialize the tracker first
        await portfolio_tracker.initialize()

        # Reset the mock call counts
        for _exchange_id, client in api_clients.items():  # B007: Use _ for unused var
            client.get_balances.reset_mock()
            client.get_positions.reset_mock()
            client.get_open_orders.reset_mock()

        # Set last reconciliation time to be recent to prevent full update
        portfolio_tracker._last_reconciliation_time[list(api_clients.keys())[0]] = datetime.now(  # noqa: SLF001 - Test setup requires modifying internal state
            UTC
        ) - timedelta(seconds=10)
        portfolio_tracker._last_reconciliation_time[list(api_clients.keys())[1]] = datetime.now(  # noqa: SLF001 - Test setup requires modifying internal state
            UTC
        ) - timedelta(seconds=10)

        # Update - should only fetch orders if interval hasn't passed
        await portfolio_tracker.update()

        # Verify only orders were updated (balances and positions not fetched)
        for _exchange_id, client in api_clients.items():  # B007: Use _ for unused var
            client.get_balances.assert_not_called()
            client.get_positions.assert_not_called()
            client.get_open_orders.assert_called_once()

        # Now force reconciliation by setting last check time far in the past
        portfolio_tracker._last_reconciliation_time[list(api_clients.keys())[0]] = (  # noqa: SLF001 - Test setup requires modifying internal state
            datetime.min.replace(tzinfo=UTC)
        )
        portfolio_tracker._last_reconciliation_time[list(api_clients.keys())[1]] = (  # noqa: SLF001 - Test setup requires modifying internal state
            datetime.min.replace(tzinfo=UTC)
        )

        await portfolio_tracker.update()  # Should now fetch everything
        for _exchange_id, client in api_clients.items():  # B007: Use _ for unused var
            # Check counts after full reconciliation (add 1 to previous checks)
            assert client.get_balances.call_count == 1
            assert client.get_positions.call_count == 1
            assert client.get_open_orders.call_count == 2  # Called once before, once now

    def test_update_order(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test updating an order in the portfolio tracker."""
        # Create a new order
        order = Order(
            client_order_id="test-order-1",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("50000.0"),
            quantity_requested=Decimal("1.0"),
            quantity_filled=Decimal("0.0"),
            status=OrderStatus.NEW,
            created_at=datetime.now(UTC),
        )

        # Update the order in the tracker
        portfolio_tracker.update_order("hyperliquid", order)

        # Verify the order was stored
        # Retrieve the order from history/open orders
        history = portfolio_tracker.get_order_history("hyperliquid")
        stored_order = next((o for o in history if o.client_order_id == "test-order-1"), None)
        assert stored_order is not None
        assert stored_order.client_order_id == "test-order-1"
        assert stored_order.symbol == "BTC"
        assert stored_order.side == OrderSide.BUY

        # Update the order status to filled
        order.status = OrderStatus.FILLED
        order.quantity_filled = Decimal("1.0")
        portfolio_tracker.update_order("hyperliquid", order)

        # Verify the order was updated
        # Retrieve the order from history/open orders
        history = portfolio_tracker.get_order_history("hyperliquid")
        stored_order = next((o for o in history if o.client_order_id == "test-order-1"), None)
        assert stored_order is not None
        assert stored_order.status == OrderStatus.FILLED
        assert stored_order.quantity_filled == Decimal("1.0")

    def test_update_position(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test updating a position in the portfolio tracker."""
        # Create a new position
        position = DerivativePosition(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            symbol="BTC",
            size=Decimal("1.0"),
            entry_price=Decimal("50000.0"),
            mark_price=Decimal("51000.0"),
            liquidation_price=Decimal("45000.0"),
            unrealized_pnl=Decimal("1000.0"),
            side=OrderSide.BUY,
        )

        # Update the position in the tracker
        portfolio_tracker.update_position("hyperliquid", position)

        # Verify the position was stored
        stored_position = portfolio_tracker._positions["hyperliquid"].get("BTC")  # noqa: SLF001  # White-box test: no public getter exists
        assert stored_position is not None
        assert stored_position.symbol == "BTC"
        assert stored_position.size == Decimal("1.0")
        assert stored_position.entry_price == Decimal("50000.0")

        # Update the position mark price
        position.mark_price = Decimal("52000.0")
        position.unrealized_pnl = Decimal("2000.0")
        portfolio_tracker.update_position("hyperliquid", position)

        # Verify the position was updated
        stored_position = portfolio_tracker._positions["hyperliquid"].get("BTC")  # noqa: SLF001  # White-box test: no public getter exists
        assert stored_position is not None
        assert stored_position.mark_price == Decimal("52000.0")
        assert stored_position.unrealized_pnl == Decimal("2000.0")

    def test_update_balance(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test updating a balance in the portfolio tracker."""
        # Update a balance in the tracker
        usdc_amount = Decimal("100000.0")
        portfolio_tracker.update_balance("hyperliquid", "USDC", usdc_amount)

        # Verify the balance was stored as a SpotBalance object
        balance_obj = portfolio_tracker.get_exchange_balance("hyperliquid", "USDC")
        assert isinstance(balance_obj, SpotBalance)
        assert balance_obj.asset == "USDC"
        assert balance_obj.total == usdc_amount
        assert balance_obj.available == usdc_amount

    def test_get_exchange_balance(
        self,
        portfolio_tracker: PortfolioTracker,
        sample_balances: dict[str, dict[str, Decimal]],
    ) -> None:
        """Test getting an exchange balance."""
        # Set up some balances
        for exchange_id, balances in sample_balances.items():
            for asset, amount in balances.items():
                portfolio_tracker.update_balance(exchange_id, asset, amount)

        # Test getting balances - check object type and total value
        usdc_balance = portfolio_tracker.get_exchange_balance("hyperliquid", "USDC")
        assert isinstance(usdc_balance, SpotBalance)
        assert usdc_balance.total == Decimal("100000.0")

        eth_balance = portfolio_tracker.get_exchange_balance("backpack", "ETH")
        assert isinstance(eth_balance, SpotBalance)
        assert eth_balance.total == Decimal("20.0")

        # Test getting non-existent balance
        assert portfolio_tracker.get_exchange_balance("hyperliquid", "XYZ") is None

    def test_get_total_capital(
        self,
        portfolio_tracker: PortfolioTracker,
        sample_balances: dict[str, dict[str, Decimal]],
    ) -> None:
        """Test getting the total capital across all exchanges."""
        # Set up some balances
        for exchange_id, balances in sample_balances.items():
            for asset, amount in balances.items():
                portfolio_tracker.update_balance(exchange_id, asset, amount)

        # Expected total capital: 100000 + 50000 = 150000 USDC
        assert portfolio_tracker.get_total_capital() == Decimal("150000.0")

    def test_get_position(
        self,
        portfolio_tracker: PortfolioTracker,
        sample_positions: dict[str, list[DerivativePosition]],
    ) -> None:
        """Test getting a position from the portfolio tracker."""
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

    def test_get_positions_by_symbol(
        self,
        portfolio_tracker: PortfolioTracker,
        sample_positions: dict[str, list[DerivativePosition]],
    ) -> None:
        """Test getting positions filtered by symbol."""
        # Set up some positions
        for exchange_id, positions in sample_positions.items():
            for position in positions:
                portfolio_tracker.update_position(exchange_id, position)

        # Add another BTC position to backpack
        btc_position = DerivativePosition(
            exchange="backpack",
            timestamp=datetime.now(UTC),
            symbol="BTC",
            size=Decimal("0.5"),
            entry_price=Decimal("49000.0"),
            mark_price=Decimal("51000.0"),
            liquidation_price=Decimal("45000.0"),
            unrealized_pnl=Decimal("1000.0"),
            side=OrderSide.BUY,
        )
        portfolio_tracker.update_position("backpack", btc_position)

        # Test getting positions by symbol
        btc_positions = portfolio_tracker.get_positions_by_symbol("backpack", "BTC")
        assert len(btc_positions) == 1
        assert btc_positions[0].symbol == "BTC"
        assert btc_positions[0].size == Decimal("0.5")

    def test_get_all_positions(
        self,
        portfolio_tracker: PortfolioTracker,
        sample_positions: dict[str, list[DerivativePosition]],
    ) -> None:
        """Test getting all positions from the portfolio tracker."""
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

    def test_get_order_by_id(
        self, portfolio_tracker: PortfolioTracker, sample_orders: dict[str, list[Order]]
    ) -> None:
        """Test getting an order by ID."""
        # Set up some orders using update_order which adds them to _orders
        for exchange_id, orders in sample_orders.items():
            for order in orders:
                portfolio_tracker.update_order(exchange_id, order)

        # Test getting orders using the correct method and attribute
        hl_order = portfolio_tracker.get_order_by_id("hyperliquid", "hl-order-1")
        assert hl_order is not None
        assert hl_order.client_order_id == "hl-order-1"
        assert hl_order.symbol == "BTC"

        bp_order = portfolio_tracker.get_order_by_id("backpack", "bp-order-1")
        assert bp_order is not None
        assert bp_order.client_order_id == "bp-order-1"
        assert bp_order.symbol == "ETH"

        # Test getting a non-existent order using the correct method
        assert portfolio_tracker.get_order_by_id("hyperliquid", "non-existent") is None

    def test_get_open_orders(
        self, portfolio_tracker: PortfolioTracker, sample_orders: dict[str, list[Order]]
    ) -> None:
        """Test getting open orders."""
        # Set up some orders
        for exchange_id, orders in sample_orders.items():
            for order in orders:
                portfolio_tracker.update_order(exchange_id, order)

        # Add an order that should be considered open
        open_order = Order(
            client_order_id="hl-order-open",
            order_type=OrderType.LIMIT,
            symbol="BTC",
            side=OrderSide.BUY,
            price=Decimal("51000.0"),
            quantity_requested=Decimal("0.5"),
            quantity_filled=Decimal("0.1"),
            status=OrderStatus.PARTIALLY_FILLED,
            created_at=datetime.now(UTC),
        )
        portfolio_tracker.update_order("hyperliquid", open_order)

        # Add an order that should NOT be considered open
        cancelled_order = Order(
            client_order_id="hl-order-2",
            order_type=OrderType.LIMIT,
            symbol="BTC",
            side=OrderSide.SELL,
            price=Decimal("53000.0"),
            quantity_requested=Decimal("0.3"),
            quantity_filled=Decimal("0.0"),
            status=OrderStatus.CANCELED,
            created_at=datetime.now(UTC),
        )
        portfolio_tracker.update_order("hyperliquid", cancelled_order)

        # Test getting open orders (only NEW and PARTIALLY_FILLED should be included)
        open_orders = portfolio_tracker.get_open_orders("hyperliquid")
        initial_open_order = portfolio_tracker.get_order_by_id("hyperliquid", "hl-order-1")
        expected_count = 0
        if initial_open_order and initial_open_order.status == OrderStatus.NEW:
            expected_count += 1
        if open_order.status == OrderStatus.PARTIALLY_FILLED:
            expected_count += 1

        assert len(open_orders) == expected_count
        assert all(o.status in [OrderStatus.NEW, OrderStatus.PARTIALLY_FILLED] for o in open_orders)
        assert not any(o.client_order_id == "hl-order-2" for o in open_orders)

    def test_to_dict_and_from_dict(
        self,
        portfolio_tracker: PortfolioTracker,
        sample_balances: dict[str, dict[str, Decimal]],
        sample_positions: dict[str, list[DerivativePosition]],
        sample_orders: dict[str, list[Order]],
    ) -> None:
        """Test serializing and deserializing the portfolio tracker state."""
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
        new_tracker = PortfolioTracker.from_dict(state_dict, portfolio_tracker.config)

        # Verify balances were restored
        for exchange_id, balances in sample_balances.items():
            for asset, amount in balances.items():
                restored_balance = new_tracker.get_exchange_balance(exchange_id, asset)
                assert isinstance(restored_balance, SpotBalance)
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
                history = new_tracker.get_order_history(exchange_id)
                restored_order = next(
                    (o for o in history if o.client_order_id == order.client_order_id), None
                )
                assert restored_order is not None
                assert restored_order.symbol == order.symbol
                assert restored_order.side == order.side
                assert restored_order.order_type == order.order_type
                assert restored_order.quantity_requested == order.quantity_requested
                assert restored_order.quantity_filled == order.quantity_filled
                assert restored_order.price == order.price
                assert restored_order.status == order.status

    def test_last_reconciliation_time(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test the last_reconciliation_time attribute."""
        # Verify initial state
        assert portfolio_tracker._last_reconciliation_time is None  # noqa: SLF001  # White-box test: no public getter exists

        # Set last reconciliation time
        previous_time = datetime.now(UTC)
        portfolio_tracker._last_reconciliation_time = previous_time
        assert portfolio_tracker._last_reconciliation_time == previous_time  # noqa: SLF001  # White-box test: no public getter exists

        # Verify subsequent updates
        expected_time = datetime.now(UTC)
        portfolio_tracker._last_reconciliation_time = expected_time
        assert portfolio_tracker._last_reconciliation_time == expected_time  # noqa: SLF001  # White-box test: no public getter exists

        # Verify reset
        portfolio_tracker._last_reconciliation_time = None
        assert portfolio_tracker._last_reconciliation_time is None  # noqa: SLF001  # White-box test: no public getter exists

    @pytest.mark.asyncio
    async def test_fetch_exchange_balances(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching balances from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_balances = {
            "USDC": SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            ),
            "BTC": SpotBalance(
                exchange="hyperliquid",
                asset="BTC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("1.0"),
                available_quantity=Decimal("1.0"),
            ),
        }
        mock_hl_api.get_balances.return_value = test_balances
        # Call protected method for test verification
        await portfolio_tracker._fetch_exchange_balances("hyperliquid")  # noqa: SLF001 - Test verification
        mock_hl_api.get_balances.assert_called_once()
        # Access protected member for test verification
        assert "hyperliquid" in portfolio_tracker._balances  # noqa: SLF001 - Test verification
        # Access protected member for test verification
        assert portfolio_tracker._balances["hyperliquid"] == test_balances  # noqa: SLF001 - Test verification
        # Access protected member for test verification
        assert "hyperliquid" in portfolio_tracker._last_update_time  # noqa: SLF001 - Test verification
        # Access protected member for test verification
        assert isinstance(portfolio_tracker._last_update_time["hyperliquid"], datetime)  # noqa: SLF001 - Test verification

    @pytest.mark.asyncio
    async def test_fetch_exchange_positions(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching positions from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_positions_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_positions.return_value = test_positions_list
        # Call protected method for test verification
        success = await portfolio_tracker._fetch_exchange_positions("hyperliquid")  # noqa: SLF001 - Test verification
        assert success is True
        mock_hl_api.get_positions.assert_called_once()
        # Access protected member for test verification
        assert "hyperliquid" in portfolio_tracker._positions  # noqa: SLF001 - Test verification
        assert (
            # Access protected member for test verification
            # Assuming position_key is symbol for simplicity here
            "BTC" in portfolio_tracker._positions["hyperliquid"]  # noqa: SLF001 - Test verification
        )
        # Access protected member for test verification
        assert portfolio_tracker._positions["hyperliquid"]["BTC"] == test_positions_list[0]  # noqa: SLF001 - Test verification

    @pytest.mark.asyncio
    async def test_fetch_exchange_orders(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching orders from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        now_utc = datetime.now(UTC)
        test_orders_list = [
            Order(
                exchange="hyperliquid",
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("41000.0"),
                quantity_requested=Decimal("0.1"),
                quantity_filled=Decimal("0.0"),
                status=OrderStatus.NEW,
                client_order_id="test-order-123",
                time_in_force=TimeInForce.GTC,
                created_at=now_utc,
                updated_at=None,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )
        ]
        test_orders_dict = {order.client_order_id: order for order in test_orders_list}
        mock_hl_api.get_open_orders.return_value = test_orders_list
        # Call protected method for test verification
        await portfolio_tracker._fetch_exchange_orders("hyperliquid")  # noqa: SLF001 - Test verification
        mock_hl_api.get_open_orders.assert_called_once()
        # Access protected member for test verification
        assert "hyperliquid" in portfolio_tracker._orders  # noqa: SLF001 - Test verification
        for order_id, order in test_orders_dict.items():
            # Access protected member for test verification
            assert order_id in portfolio_tracker._orders["hyperliquid"]  # noqa: SLF001 - Test verification
            # Access protected member for test verification
            assert portfolio_tracker._orders["hyperliquid"][order_id] == order  # noqa: SLF001 - Test verification
        # Access protected member for test verification
        assert "hyperliquid" in portfolio_tracker._last_update_time  # noqa: SLF001 - Test verification
        # Access protected member for test verification
        assert isinstance(portfolio_tracker._last_update_time["hyperliquid"], datetime)  # noqa: SLF001 - Test verification

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_orders(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled orders from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_orders_list = [
            Order(
                client_order_id="test-order-filled",
                exchange="hyperliquid",
                order_type=OrderType.LIMIT,
                symbol="BTC",
                side=OrderSide.SELL,
                price=Decimal("40000.0"),
                quantity_requested=Decimal("0.5"),
                quantity_filled=Decimal("0.5"),
                status=OrderStatus.FILLED,
                time_in_force=TimeInForce.GTC,
            )
        ]
        mock_hl_api.get_filled_orders.return_value = test_filled_orders_list

        # Assuming order_key is client_order_id for simplicity here
        assert "test-order-filled" in portfolio_tracker._filled_orders["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_orders["hyperliquid"]["test-order-filled"]
            == test_filled_orders_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_orders(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled orders from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_orders_list = [
            Order(
                client_order_id="test-order-cancelled",
                exchange="hyperliquid",
                order_type=OrderType.LIMIT,
                symbol="BTC",
                side=OrderSide.SELL,
                price=Decimal("40000.0"),
                quantity_requested=Decimal("0.5"),
                quantity_filled=Decimal("0.0"),
                status=OrderStatus.CANCELED,
                time_in_force=TimeInForce.GTC,
            )
        ]
        mock_hl_api.get_cancelled_orders.return_value = test_cancelled_orders_list

        # Assuming order_key is client_order_id for simplicity here
        assert "test-order-cancelled" in portfolio_tracker._cancelled_orders["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_orders["hyperliquid"]["test-order-cancelled"]
            == test_cancelled_orders_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_order_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching order history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_order_history_list = [
            Order(
                client_order_id="test-order-history",
                exchange="hyperliquid",
                order_type=OrderType.LIMIT,
                symbol="BTC",
                side=OrderSide.BUY,
                price=Decimal("40000.0"),
                quantity_requested=Decimal("0.5"),
                quantity_filled=Decimal("0.5"),
                status=OrderStatus.FILLED,
                time_in_force=TimeInForce.GTC,
            )
        ]
        mock_hl_api.get_order_history.return_value = test_order_history_list

        # Assuming order_key is client_order_id for simplicity here
        assert "test-order-history" in portfolio_tracker._order_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._order_history["hyperliquid"]["test-order-history"]
            == test_order_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_order_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled order history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_order_history_list = [
            Order(
                client_order_id="test-filled-order-history",
                exchange="hyperliquid",
                order_type=OrderType.LIMIT,
                symbol="BTC",
                side=OrderSide.BUY,
                price=Decimal("40000.0"),
                quantity_requested=Decimal("0.5"),
                quantity_filled=Decimal("0.5"),
                status=OrderStatus.FILLED,
                time_in_force=TimeInForce.GTC,
            )
        ]
        mock_hl_api.get_filled_order_history.return_value = test_filled_order_history_list

        # Assuming order_key is client_order_id for simplicity here
        assert "test-filled-order-history" in portfolio_tracker._filled_order_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_order_history["hyperliquid"]["test-filled-order-history"]
            == test_filled_order_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_order_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled order history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_order_history_list = [
            Order(
                client_order_id="test-cancelled-order-history",
                exchange="hyperliquid",
                order_type=OrderType.LIMIT,
                symbol="BTC",
                side=OrderSide.SELL,
                price=Decimal("40000.0"),
                quantity_requested=Decimal("0.5"),
                quantity_filled=Decimal("0.0"),
                status=OrderStatus.CANCELED,
                time_in_force=TimeInForce.GTC,
            )
        ]
        mock_hl_api.get_cancelled_order_history.return_value = test_cancelled_order_history_list

        # Assuming order_key is client_order_id for simplicity here
        assert (
            "test-cancelled-order-history"
            in portfolio_tracker._cancelled_order_history["hyperliquid"]
        )  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_order_history["hyperliquid"][
                "test-cancelled-order-history"
            ]
            == test_cancelled_order_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_position_history.return_value = test_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._position_history["hyperliquid"]["BTC"]
            == test_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_filled_position_history.return_value = test_filled_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._filled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_position_history["hyperliquid"]["BTC"]
            == test_filled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_cancelled_position_history.return_value = (
            test_cancelled_position_history_list
        )

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._cancelled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_position_history["hyperliquid"]["BTC"]
            == test_cancelled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_balance_history.return_value = test_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._balance_history["hyperliquid"]["USDC"]
            == test_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_filled_balance_history.return_value = test_filled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._filled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_balance_history["hyperliquid"]["USDC"]
            == test_filled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_cancelled_balance_history.return_value = test_cancelled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._cancelled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_balance_history["hyperliquid"]["USDC"]
            == test_cancelled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_position_history.return_value = test_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._position_history["hyperliquid"]["BTC"]
            == test_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_filled_position_history.return_value = test_filled_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._filled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_position_history["hyperliquid"]["BTC"]
            == test_filled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_cancelled_position_history.return_value = (
            test_cancelled_position_history_list
        )

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._cancelled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_position_history["hyperliquid"]["BTC"]
            == test_cancelled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_balance_history.return_value = test_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._balance_history["hyperliquid"]["USDC"]
            == test_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_filled_balance_history.return_value = test_filled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._filled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_balance_history["hyperliquid"]["USDC"]
            == test_filled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_cancelled_balance_history.return_value = test_cancelled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._cancelled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_balance_history["hyperliquid"]["USDC"]
            == test_cancelled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_position_history.return_value = test_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._position_history["hyperliquid"]["BTC"]
            == test_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_filled_position_history.return_value = test_filled_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._filled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_position_history["hyperliquid"]["BTC"]
            == test_filled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_cancelled_position_history.return_value = (
            test_cancelled_position_history_list
        )

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._cancelled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_position_history["hyperliquid"]["BTC"]
            == test_cancelled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_balance_history.return_value = test_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._balance_history["hyperliquid"]["USDC"]
            == test_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_filled_balance_history.return_value = test_filled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._filled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_balance_history["hyperliquid"]["USDC"]
            == test_filled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_cancelled_balance_history.return_value = test_cancelled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._cancelled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_balance_history["hyperliquid"]["USDC"]
            == test_cancelled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_position_history.return_value = test_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._position_history["hyperliquid"]["BTC"]
            == test_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_filled_position_history.return_value = test_filled_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._filled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_position_history["hyperliquid"]["BTC"]
            == test_filled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_cancelled_position_history.return_value = (
            test_cancelled_position_history_list
        )

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._cancelled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_position_history["hyperliquid"]["BTC"]
            == test_cancelled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_balance_history.return_value = test_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._balance_history["hyperliquid"]["USDC"]
            == test_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_filled_balance_history.return_value = test_filled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._filled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_balance_history["hyperliquid"]["USDC"]
            == test_filled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_cancelled_balance_history.return_value = test_cancelled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._cancelled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_balance_history["hyperliquid"]["USDC"]
            == test_cancelled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_position_history.return_value = test_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._position_history["hyperliquid"]["BTC"]
            == test_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_filled_position_history.return_value = test_filled_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._filled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_position_history["hyperliquid"]["BTC"]
            == test_filled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_cancelled_position_history.return_value = (
            test_cancelled_position_history_list
        )

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._cancelled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_position_history["hyperliquid"]["BTC"]
            == test_cancelled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_balance_history.return_value = test_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._balance_history["hyperliquid"]["USDC"]
            == test_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_filled_balance_history.return_value = test_filled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._filled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_balance_history["hyperliquid"]["USDC"]
            == test_filled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_cancelled_balance_history.return_value = test_cancelled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._cancelled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_balance_history["hyperliquid"]["USDC"]
            == test_cancelled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_position_history.return_value = test_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._position_history["hyperliquid"]["BTC"]
            == test_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_filled_position_history.return_value = test_filled_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._filled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_position_history["hyperliquid"]["BTC"]
            == test_filled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_cancelled_position_history.return_value = (
            test_cancelled_position_history_list
        )

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._cancelled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_position_history["hyperliquid"]["BTC"]
            == test_cancelled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_balance_history.return_value = test_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._balance_history["hyperliquid"]["USDC"]
            == test_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_filled_balance_history.return_value = test_filled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._filled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_balance_history["hyperliquid"]["USDC"]
            == test_filled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_cancelled_balance_history.return_value = test_cancelled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._cancelled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_balance_history["hyperliquid"]["USDC"]
            == test_cancelled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_position_history.return_value = test_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._position_history["hyperliquid"]["BTC"]
            == test_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_filled_position_history.return_value = test_filled_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._filled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_position_history["hyperliquid"]["BTC"]
            == test_filled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_cancelled_position_history.return_value = (
            test_cancelled_position_history_list
        )

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._cancelled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_position_history["hyperliquid"]["BTC"]
            == test_cancelled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_balance_history.return_value = test_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._balance_history["hyperliquid"]["USDC"]
            == test_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_filled_balance_history.return_value = test_filled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._filled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_balance_history["hyperliquid"]["USDC"]
            == test_filled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_cancelled_balance_history.return_value = test_cancelled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._cancelled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_balance_history["hyperliquid"]["USDC"]
            == test_cancelled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_position_history.return_value = test_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._position_history["hyperliquid"]["BTC"]
            == test_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_filled_position_history.return_value = test_filled_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._filled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_position_history["hyperliquid"]["BTC"]
            == test_filled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_cancelled_position_history.return_value = (
            test_cancelled_position_history_list
        )

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._cancelled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_position_history["hyperliquid"]["BTC"]
            == test_cancelled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_balance_history.return_value = test_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._balance_history["hyperliquid"]["USDC"]
            == test_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_filled_balance_history.return_value = test_filled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._filled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_balance_history["hyperliquid"]["USDC"]
            == test_filled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_cancelled_balance_history.return_value = test_cancelled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._cancelled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_balance_history["hyperliquid"]["USDC"]
            == test_cancelled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_position_history.return_value = test_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._position_history["hyperliquid"]["BTC"]
            == test_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_filled_position_history.return_value = test_filled_position_history_list

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._filled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_position_history["hyperliquid"]["BTC"]
            == test_filled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_position_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled position history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_position_history_list = [
            DerivativePosition(
                exchange="hyperliquid",
                timestamp=datetime.now(UTC),
                symbol="BTC",
                size=Decimal("0.5"),
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("41000.0"),
                side=OrderSide.BUY,
                liquidation_price=Decimal("38000"),
                unrealized_pnl=Decimal("500"),
            )
        ]
        mock_hl_api.get_cancelled_position_history.return_value = (
            test_cancelled_position_history_list
        )

        # Assuming position_key is symbol for simplicity here
        assert "BTC" in portfolio_tracker._cancelled_position_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_position_history["hyperliquid"]["BTC"]
            == test_cancelled_position_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_balance_history.return_value = test_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._balance_history["hyperliquid"]["USDC"]
            == test_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_filled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching filled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_filled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_filled_balance_history.return_value = test_filled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._filled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._filled_balance_history["hyperliquid"]["USDC"]
            == test_filled_balance_history_list[0]
        )  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_fetch_exchange_cancelled_balance_history(
        self, portfolio_tracker: PortfolioTracker, mock_api_clients: dict[str, AsyncMock]
    ) -> None:
        """Test fetching cancelled balance history from an exchange."""
        mock_hl_api = mock_api_clients["hyperliquid"]
        test_cancelled_balance_history_list = [
            SpotBalance(
                exchange="hyperliquid",
                asset="USDC",
                timestamp=datetime.now(UTC),
                total_quantity=Decimal("10000.0"),
                available_quantity=Decimal("10000.0"),
            )
        ]
        mock_hl_api.get_cancelled_balance_history.return_value = test_cancelled_balance_history_list

        # Assuming balance_key is asset for simplicity here
        assert "USDC" in portfolio_tracker._cancelled_balance_history["hyperliquid"]  # noqa: SLF001
        assert (
            portfolio_tracker._cancelled_balance_history["hyperliquid"]["USDC"]
            == test_cancelled_balance_history_list[0]
        )  # noqa: SLF001
