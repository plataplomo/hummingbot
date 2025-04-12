import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

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
    """Test suite for PortfolioTracker component."""

    @pytest.fixture
    def portfolio_tracker(self, mock_config, mock_exchange_api):
        """Create a PortfolioTracker instance with mocked dependencies."""
        # Use the mock_config fixture to create a Config instance with a dictionary
        config = mock_config(
            {
                "exchanges": {
                    "hyperliquid": {"enabled": True},
                    "backpack": {"enabled": True},
                },
                "portfolio": {"reconciliation_interval": 300},
            }
        )

        tracker = PortfolioTracker(config)

        # Register API clients
        tracker.register_api_client("hyperliquid", mock_exchange_api)
        tracker.register_api_client("backpack", mock_exchange_api)

        return tracker

    @pytest.mark.asyncio
    async def test_register_api_client(self, portfolio_tracker, mock_exchange_api):
        """Test that API clients can be registered."""
        # Register a new API client
        portfolio_tracker.register_api_client("test_exchange", mock_exchange_api)

        # Verify the client was registered
        assert "test_exchange" in portfolio_tracker.api_clients
        assert portfolio_tracker.api_clients["test_exchange"] == mock_exchange_api

    @pytest.mark.asyncio
    async def test_initialize(self, portfolio_tracker):
        """Test initialization of the PortfolioTracker."""
        # Patch the fetch methods
        with (
            patch.object(
                portfolio_tracker, "_fetch_exchange_balances", AsyncMock()
            ) as mock_fetch_balances,
            patch.object(
                portfolio_tracker, "_fetch_exchange_positions", AsyncMock()
            ) as mock_fetch_positions,
            patch.object(
                portfolio_tracker, "_fetch_exchange_orders", AsyncMock()
            ) as mock_fetch_orders,
        ):
            # Initialize the tracker
            await portfolio_tracker.initialize()

            # Verify methods were called for both exchanges
            assert mock_fetch_balances.call_count == 2
            assert mock_fetch_positions.call_count == 2
            # assert mock_fetch_orders.call_count == 2 # Commented out: Order fetching removed from init

            # Verify calls for hyperliquid
            mock_fetch_balances.assert_any_call("hyperliquid")
            mock_fetch_positions.assert_any_call("hyperliquid")
            # mock_fetch_orders.assert_any_call("hyperliquid") # Commented out as orders aren't fetched in init

            # Verify calls for backpack
            mock_fetch_balances.assert_any_call("backpack")
            mock_fetch_positions.assert_any_call("backpack")
            # mock_fetch_orders.assert_any_call("backpack") # Commented out as orders aren't fetched in init

    @pytest.mark.asyncio
    async def test_fetch_exchange_balances(self, portfolio_tracker, mock_exchange_api):
        """Test fetching balances from an exchange."""
        # Set up test balances
        test_balances = {
            "USDC": Balance(asset="USDC", free=10000.0, locked=0.0, total=10000.0),
            "BTC": Balance(asset="BTC", free=1.0, locked=0.0, total=1.0),
        }

        # Mock the get_balances method
        mock_exchange_api.get_balances.return_value = test_balances

        # Fetch balances
        await portfolio_tracker._fetch_exchange_balances("hyperliquid")

        # Verify API client was called
        mock_exchange_api.get_balances.assert_called_once()

        # Check that balances were stored correctly
        assert "hyperliquid" in portfolio_tracker._balances
        assert portfolio_tracker._balances["hyperliquid"] == test_balances

        # Check that the last update time was set
        assert "hyperliquid" in portfolio_tracker._last_update_time
        assert isinstance(portfolio_tracker._last_update_time["hyperliquid"], datetime)

    @pytest.mark.asyncio
    async def test_fetch_exchange_positions(self, portfolio_tracker, mock_exchange_api):
        """Test fetching positions from an exchange."""
        # Set up test positions as a LIST of Position objects
        test_positions_list = [
            Position(
                symbol="BTC",
                size=Decimal("0.5"),  # Use Decimal
                entry_price=Decimal("40000.0"),
                mark_price=Decimal("42000.0"),
                liquidation_price=Decimal("30000.0"),
                unrealized_pnl=Decimal("1000.0"),
                leverage=Decimal("5.0"),
                side=OrderSide.BUY,
            )
        ]

        # Mock the get_positions method to return the LIST
        mock_exchange_api.get_positions.return_value = test_positions_list

        # Fetch positions
        success = await portfolio_tracker._fetch_exchange_positions("hyperliquid")

        # Verify fetch succeeded
        assert success is True

        # Verify API client was called
        mock_exchange_api.get_positions.assert_called_once()

        # Check that positions were stored correctly (keyed by symbol)
        assert "hyperliquid" in portfolio_tracker._positions
        assert "BTC" in portfolio_tracker._positions["hyperliquid"]
        # Compare the stored object with the first item in the test list
        assert portfolio_tracker._positions["hyperliquid"]["BTC"] == test_positions_list[0]

    @pytest.mark.asyncio
    async def test_fetch_exchange_orders(self, portfolio_tracker, mock_exchange_api):
        """Test fetching orders from an exchange."""
        # Set up test orders
        test_orders = {
            "order123": Order(
                id="order123",
                symbol="BTC",
                side=OrderSide.BUY,
                type=OrderType.LIMIT,
                price=41000.0,
                quantity=0.1,
                filled_quantity=0.0,
                status="NEW",
                time=int(datetime.now().timestamp() * 1000),
                client_order_id="test-order-123",
            )
        }

        # Mock the get_open_orders method
        mock_exchange_api.get_open_orders.return_value = list(test_orders.values())

        # Fetch orders
        await portfolio_tracker._fetch_exchange_orders("hyperliquid")

        # Verify API client was called
        mock_exchange_api.get_open_orders.assert_called_once()

        # Check that orders were stored correctly
        assert "hyperliquid" in portfolio_tracker._orders
        for order_id, order in test_orders.items():
            assert order_id in portfolio_tracker._orders["hyperliquid"]
            assert portfolio_tracker._orders["hyperliquid"][order_id] == order

        # Check that the last update time was set
        assert "hyperliquid" in portfolio_tracker._last_update_time
        assert isinstance(portfolio_tracker._last_update_time["hyperliquid"], datetime)

    @pytest.mark.asyncio
    async def test_update(self, portfolio_tracker):
        """Test updating portfolio state."""
        # Enable exchanges in mock config for the test
        portfolio_tracker.config.get = MagicMock(
            side_effect=lambda key, default=None: {
                "exchanges.hyperliquid.enabled": True,
                "exchanges.backpack.enabled": True,
                # Add other config gets if needed by the method, otherwise return default
            }.get(key, default)
        )

        # Patch the fetch methods
        with (
            patch.object(
                portfolio_tracker, "_fetch_exchange_balances", AsyncMock()
            ) as mock_fetch_balances,
            patch.object(
                portfolio_tracker, "_fetch_exchange_positions", AsyncMock()
            ) as mock_fetch_positions,
            patch.object(
                portfolio_tracker, "_fetch_exchange_orders", AsyncMock()
            ) as mock_fetch_orders,
        ):
            # Set up reconciliation timestamps
            now = datetime.now(UTC)  # Use UTC
            # Make last reconciliation older than the interval
            portfolio_tracker._last_reconciliation_time = {
                "hyperliquid": now
                - timedelta(seconds=portfolio_tracker.reconciliation_interval + 1),
                "backpack": now - timedelta(seconds=portfolio_tracker.reconciliation_interval + 1),
            }
            # Ensure interval is positive
            assert portfolio_tracker.reconciliation_interval > 0

            # Update the portfolio state
            await portfolio_tracker.update()

            # Verify all methods were called for both exchanges since reconciliation is needed
            assert mock_fetch_balances.call_count == 2
            assert mock_fetch_positions.call_count == 2
            assert mock_fetch_orders.call_count == 2

            # Reset the mocks
            mock_fetch_balances.reset_mock()
            mock_fetch_positions.reset_mock()
            mock_fetch_orders.reset_mock()

            # Set up reconciliation timestamps to be recent
            portfolio_tracker._last_reconciliation_time = {
                "hyperliquid": now - timedelta(seconds=10),
                "backpack": now - timedelta(seconds=10),
            }

            # Update the portfolio state again
            await portfolio_tracker.update()

            # Only orders should be fetched if not reconciling
            assert mock_fetch_balances.call_count == 0
            assert (
                mock_fetch_positions.call_count == 0
            )  # Positions also fetched only on reconciliation interval
            assert mock_fetch_orders.call_count == 2

    def test_update_order(self, portfolio_tracker):
        """Test updating an order."""
        # Create a test order
        test_order = Order(
            id="order123",
            symbol="BTC",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=41000.0,
            quantity=0.1,
            filled_quantity=0.0,
            status="NEW",
            time=int(datetime.now().timestamp() * 1000),
            client_order_id="test-order-123",
        )

        # Update the order
        portfolio_tracker.update_order("hyperliquid", test_order)

        # Check that the order was stored correctly
        assert "hyperliquid" in portfolio_tracker._orders
        assert "order123" in portfolio_tracker._orders["hyperliquid"]
        assert portfolio_tracker._orders["hyperliquid"]["order123"] == test_order

        # Test updating a filled order
        filled_order = Order(
            id="order123",
            symbol="BTC",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=41000.0,
            quantity=0.1,
            filled_quantity=0.1,
            status=OrderStatus.FILLED,
            time=int(datetime.now().timestamp() * 1000),
            client_order_id="test-order-123",
        )

        portfolio_tracker.update_order("hyperliquid", filled_order)

        # Check that the order was updated
        assert portfolio_tracker._orders["hyperliquid"]["order123"] == filled_order

    def test_update_position(self, portfolio_tracker):
        """Test updating a position."""
        # Create a test position
        test_position = Position(
            symbol="BTC",
            size=0.5,
            entry_price=40000.0,
            mark_price=42000.0,
            liquidation_price=30000.0,
            unrealized_pnl=1000.0,
            leverage=5.0,
            side=OrderSide.BUY,
        )

        # Update the position
        portfolio_tracker.update_position("hyperliquid", test_position)

        # Check that the position was added/updated correctly by symbol
        assert "hyperliquid" in portfolio_tracker._positions
        assert test_position.symbol in portfolio_tracker._positions["hyperliquid"]
        assert portfolio_tracker._positions["hyperliquid"][test_position.symbol] == test_position

    def test_update_balance(self, portfolio_tracker):
        """Test updating a balance."""
        # Update a balance - Use Decimal
        portfolio_tracker.update_balance("hyperliquid", "USDC", Decimal("10000.0"))

        # Check that the balance was stored correctly
        assert "hyperliquid" in portfolio_tracker._balances
        assert "USDC" in portfolio_tracker._balances["hyperliquid"]
        # Assert the Balance object's total attribute
        assert portfolio_tracker._balances["hyperliquid"]["USDC"].total == Decimal("10000.0")

    def test_get_exchange_balance(self, portfolio_tracker):
        """Test getting an exchange balance."""
        # Set up test balances using Balance objects
        portfolio_tracker._balances = {
            "hyperliquid": {
                "USDC": Balance(
                    asset="USDC",
                    free=Decimal("10000.0"),
                    locked=Decimal("0.0"),
                    total=Decimal("10000.0"),
                ),
                "BTC": Balance(
                    asset="BTC", free=Decimal("1.0"), locked=Decimal("0.0"), total=Decimal("1.0")
                ),
            }
        }

        # Get a balance object
        balance_obj = portfolio_tracker.get_exchange_balance("hyperliquid", "USDC")

        # Check the result - assuming get_exchange_balance returns the Balance object
        assert isinstance(balance_obj, Balance)
        assert balance_obj.total == Decimal("10000.0")

        # Test getting a nonexistent balance
        balance_obj_none = portfolio_tracker.get_exchange_balance("hyperliquid", "ETH")
        assert balance_obj_none is None  # Should return None if not found

        # Test getting a balance from a nonexistent exchange
        balance_obj_none_exchange = portfolio_tracker.get_exchange_balance("nonexistent", "USDC")
        assert balance_obj_none_exchange is None  # Should return None

    def test_get_total_capital(self, portfolio_tracker):
        """Test calculating total capital."""
        # Set up test balances
        portfolio_tracker._balances = {
            "hyperliquid": {"USDC": Balance(asset="USDC", free=10000.0, locked=0.0, total=10000.0)},
            "backpack": {"USDC": Balance(asset="USDC", free=5000.0, locked=0.0, total=5000.0)},
        }

        # Calculate total capital
        total = portfolio_tracker.get_total_capital()

        # Check the result
        assert total == 15000.0

    def test_get_exchange_exposure(self, portfolio_tracker):
        """Test calculating exchange exposure."""
        # Set up test positions
        portfolio_tracker._positions = {
            "hyperliquid": {
                "position1": Position(
                    symbol="BTC",
                    size=0.5,
                    entry_price=40000.0,
                    mark_price=42000.0,
                    liquidation_price=30000.0,
                    unrealized_pnl=1000.0,
                    leverage=5.0,
                    side=OrderSide.BUY,
                ),
                "position2": Position(
                    symbol="ETH",
                    size=5.0,
                    entry_price=2000.0,
                    mark_price=2100.0,
                    liquidation_price=1500.0,
                    unrealized_pnl=500.0,
                    leverage=3.0,
                    side=OrderSide.BUY,
                ),
            }
        }

        # Calculate exchange exposure
        exposure = portfolio_tracker.get_exchange_exposure("hyperliquid")

        # Check the result - should be BTC position (0.5 * 42000) + ETH position (5 * 2100)
        assert exposure == 0.5 * 42000 + 5.0 * 2100

    def test_get_total_exposure(self, portfolio_tracker):
        """Test calculating total exposure."""
        # Set up test positions
        portfolio_tracker._positions = {
            "hyperliquid": {
                "position1": Position(
                    symbol="BTC",
                    size=0.5,
                    entry_price=40000.0,
                    mark_price=42000.0,
                    liquidation_price=30000.0,
                    unrealized_pnl=1000.0,
                    leverage=5.0,
                    side=OrderSide.BUY,
                )
            },
            "backpack": {
                "position2": Position(
                    symbol="ETH",
                    size=5.0,
                    entry_price=2000.0,
                    mark_price=2100.0,
                    liquidation_price=1500.0,
                    unrealized_pnl=500.0,
                    leverage=3.0,
                    side=OrderSide.BUY,
                )
            },
        }

        # Calculate total exposure
        exposure = portfolio_tracker.get_total_exposure()

        # Check the result - should be BTC position (0.5 * 42000) + ETH position (5 * 2100)
        # Convert expected result to Decimal for comparison
        expected_exposure = Decimal(str(0.5 * 42000 + 5.0 * 2100))
        assert exposure == expected_exposure

    def test_get_pnl(self, portfolio_tracker):
        """Test calculating PNL."""
        # Set up test positions
        portfolio_tracker._positions = {
            "hyperliquid": {
                "position1": Position(
                    symbol="BTC",
                    size=0.5,
                    entry_price=40000.0,
                    mark_price=42000.0,
                    liquidation_price=30000.0,
                    unrealized_pnl=1000.0,
                    leverage=5.0,
                    side=OrderSide.BUY,
                )
            },
            "backpack": {
                "position2": Position(
                    symbol="ETH",
                    size=5.0,
                    entry_price=2000.0,
                    mark_price=2100.0,
                    liquidation_price=1500.0,
                    unrealized_pnl=500.0,
                    leverage=3.0,
                    side=OrderSide.BUY,
                )
            },
        }

        # Calculate PNL
        realized, unrealized = portfolio_tracker.get_pnl()

        # Check the result
        # Convert expected result to Decimal
        assert realized == Decimal("0.0")  # No realized PNL in our test setup
        expected_unrealized = Decimal("1000.0") + Decimal(
            "500.0"
        )  # Sum of unrealized PNL from positions
        assert unrealized == expected_unrealized

    def test_get_position(self, portfolio_tracker):
        """Test getting a position by ID."""
        # Create a test position
        test_position = Position(
            symbol="BTC",
            size=0.5,
            entry_price=40000.0,
            mark_price=42000.0,
            liquidation_price=30000.0,
            unrealized_pnl=1000.0,
            leverage=5.0,
            side=OrderSide.BUY,
        )
        test_position.id = "position123"

        # Store the position
        portfolio_tracker._positions = {"hyperliquid": {"position123": test_position}}

        # Get the position
        position = portfolio_tracker.get_position("hyperliquid", "position123")

        # Check the result
        assert position == test_position

        # Test getting a nonexistent position
        position = portfolio_tracker.get_position("hyperliquid", "nonexistent")
        assert position is None

    def test_get_positions_by_symbol(self, portfolio_tracker):
        """Test getting positions by symbol."""
        # Create test positions
        position1 = Position(
            symbol="BTC",
            size=Decimal("0.5"),  # Use Decimal
            entry_price=Decimal("40000.0"),  # Use Decimal
            mark_price=Decimal("42000.0"),  # Use Decimal
            liquidation_price=Decimal("30000.0"),  # Use Decimal
            unrealized_pnl=Decimal("1000.0"),  # Use Decimal
            leverage=Decimal("5.0"),  # Use Decimal
            side=OrderSide.BUY,
        )
        # Assuming update_position stores by symbol, no need for explicit ID here for the test

        position2 = Position(
            symbol="BTC",
            size=Decimal("0.3"),  # Use Decimal
            entry_price=Decimal("41000.0"),  # Use Decimal
            mark_price=Decimal("42000.0"),  # Use Decimal
            liquidation_price=Decimal("35000.0"),  # Use Decimal
            unrealized_pnl=Decimal("300.0"),  # Use Decimal
            leverage=Decimal("3.0"),  # Use Decimal
            side=OrderSide.BUY,
        )

        position_eth = Position(
            symbol="ETH",
            size=Decimal("10.0"),
            entry_price=Decimal("2000.0"),
            mark_price=Decimal("2100.0"),
            liquidation_price=Decimal("1800.0"),
            unrealized_pnl=Decimal("1000.0"),
            leverage=Decimal("2.0"),
            side=OrderSide.BUY,
        )

        # Store the positions using update_position or by setting _positions correctly
        # Assuming _positions structure is exchange -> symbol -> Position
        portfolio_tracker._positions = {
            "hyperliquid": {
                "BTC": position1,  # Simulate storing the first BTC position
                # Storing multiple positions for the same symbol might overwrite
                # or require a list. Let's assume update_position handles this
                # or the test focuses on retrieving what's currently stored under the key.
                # For testing get_positions_by_symbol, let's structure _positions
                # to contain multiple positions if the method aggregates them.
                # If PortfolioTracker only stores ONE position per symbol per exchange,
                # this test needs rethinking or the implementation needs changing.
                # Let's assume the method should find all matching symbols if stored correctly.
                # A more realistic storage might be exchange -> position_id -> Position,
                # and get_positions_by_symbol iterates and filters.
                # Let's adjust the storage mock to reflect symbol->Position for simplicity
                # and assume get_positions_by_symbol returns a list containing that one position.
                # OR, adjust storage to be exchange -> symbol -> list[Position] if intended.
                # Let's assume exchange -> symbol -> Position for now. We'll add ETH too.
                "ETH": position_eth,
                # If we add position2 here, it overwrites position1.
                # Let's test retrieving the ONE stored BTC position first.
            }
        }
        # Add position2 to simulate an update overwriting position1
        portfolio_tracker._positions["hyperliquid"]["BTC"] = position2

        # Get positions by symbol "BTC"
        # This should now return a list containing only position2
        positions_btc = portfolio_tracker.get_positions_by_symbol("hyperliquid", "BTC")

        # Check the result - should contain only position2
        assert len(positions_btc) == 1, (
            "Expected only one position if storage is symbol -> Position"
        )
        assert positions_btc[0] == position2  # Check it's the latest one stored

        # Get positions by symbol "ETH"
        positions_eth_list = portfolio_tracker.get_positions_by_symbol("hyperliquid", "ETH")
        assert len(positions_eth_list) == 1
        assert positions_eth_list[0] == position_eth

        # Get positions for a non-existent symbol
        positions_none = portfolio_tracker.get_positions_by_symbol("hyperliquid", "SOL")
        assert len(positions_none) == 0

    def test_to_dict(self, portfolio_tracker):
        """Test serializing the portfolio state to a dictionary."""
        # Set up test data
        portfolio_tracker._balances = {
            "hyperliquid": {"USDC": Balance(asset="USDC", free=10000.0, locked=0.0, total=10000.0)}
        }

        test_position = Position(
            symbol="BTC",
            size=0.5,
            entry_price=40000.0,
            mark_price=42000.0,
            liquidation_price=30000.0,
            unrealized_pnl=1000.0,
            leverage=5.0,
            side=OrderSide.BUY,
        )
        test_position.id = "position1"

        portfolio_tracker._positions = {"hyperliquid": {"position1": test_position}}

        test_order = Order(
            id="order1",
            symbol="BTC",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=41000.0,
            quantity=0.1,
            filled_quantity=0.0,
            status="NEW",
            time=int(datetime.now().timestamp() * 1000),
            client_order_id="test-order-1",
        )

        portfolio_tracker._orders = {"hyperliquid": {"order1": test_order}}

        # Convert to dictionary
        state_dict = portfolio_tracker.to_dict()

        # Check the structure
        assert "balances" in state_dict
        assert "positions" in state_dict
        assert "orders" in state_dict

        # Check the content
        assert "hyperliquid" in state_dict["balances"]
        assert "USDC" in state_dict["balances"]["hyperliquid"]

        assert "hyperliquid" in state_dict["positions"]
        assert "position1" in state_dict["positions"]["hyperliquid"]

        assert "hyperliquid" in state_dict["orders"]
        assert "order1" in state_dict["orders"]["hyperliquid"]

        # Check that the dictionary can be serialized to JSON using the new method
        try:
            json_str = portfolio_tracker.to_json()  # Use the new method without indent
            assert isinstance(json_str, str)
            # Attempt to load it back to ensure it's valid JSON
            loaded_data = json.loads(json_str)
            assert isinstance(loaded_data, dict)
            # Check a Decimal value was converted to string by encoder
            assert isinstance(loaded_data["balances"]["hyperliquid"]["USDC"]["total"], str)
            assert (
                loaded_data["balances"]["hyperliquid"]["USDC"]["total"] == "10000.0"
            )  # Compare as string
        except Exception as e:
            pytest.fail(f"Serialization using to_json failed: {e}")
