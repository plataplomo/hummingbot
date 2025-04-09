import pytest
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from datetime import datetime, timedelta
import json

from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.models import Balance, Position, Order, OrderStatus, OrderSide, OrderType

class TestPortfolioTracker:
    """Test suite for PortfolioTracker component."""

    @pytest.fixture
    def portfolio_tracker(self, mock_config, mock_exchange_api):
        """Create a PortfolioTracker instance with mocked dependencies."""
        tracker = PortfolioTracker(mock_config)
        
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
        with patch.object(portfolio_tracker, '_fetch_exchange_balances', AsyncMock()) as mock_fetch_balances, \
             patch.object(portfolio_tracker, '_fetch_exchange_positions', AsyncMock()) as mock_fetch_positions, \
             patch.object(portfolio_tracker, '_fetch_exchange_orders', AsyncMock()) as mock_fetch_orders:
            
            # Initialize the tracker
            await portfolio_tracker.initialize()
            
            # Verify methods were called for both exchanges
            assert mock_fetch_balances.call_count == 2
            assert mock_fetch_positions.call_count == 2
            assert mock_fetch_orders.call_count == 2
            
            # Verify calls for hyperliquid
            mock_fetch_balances.assert_any_call("hyperliquid")
            mock_fetch_positions.assert_any_call("hyperliquid")
            mock_fetch_orders.assert_any_call("hyperliquid")
            
            # Verify calls for backpack
            mock_fetch_balances.assert_any_call("backpack")
            mock_fetch_positions.assert_any_call("backpack")
            mock_fetch_orders.assert_any_call("backpack")

    @pytest.mark.asyncio
    async def test_fetch_exchange_balances(self, portfolio_tracker, mock_exchange_api):
        """Test fetching balances from an exchange."""
        # Set up test balances
        test_balances = {
            "USDC": Balance(asset="USDC", free=10000.0, locked=0.0, total=10000.0),
            "BTC": Balance(asset="BTC", free=1.0, locked=0.0, total=1.0)
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
        # Set up test positions
        test_positions = {
            "BTC": Position(
                symbol="BTC", 
                size=0.5, 
                entry_price=40000.0, 
                mark_price=42000.0,
                liquidation_price=30000.0,
                unrealized_pnl=1000.0,
                leverage=5.0,
                side=OrderSide.BUY
            )
        }
        
        # Mock the get_positions method
        mock_exchange_api.get_positions.return_value = test_positions
        
        # Fetch positions
        await portfolio_tracker._fetch_exchange_positions("hyperliquid")
        
        # Verify API client was called
        mock_exchange_api.get_positions.assert_called_once()
        
        # Check that positions were stored correctly
        assert "hyperliquid" in portfolio_tracker._positions
        assert portfolio_tracker._positions["hyperliquid"] == test_positions
        
        # Check that the last update time was set
        assert "hyperliquid" in portfolio_tracker._last_update_time
        assert isinstance(portfolio_tracker._last_update_time["hyperliquid"], datetime)

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
                client_order_id="test-order-123"
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
        # Patch the fetch methods
        with patch.object(portfolio_tracker, '_fetch_exchange_balances', AsyncMock()) as mock_fetch_balances, \
             patch.object(portfolio_tracker, '_fetch_exchange_positions', AsyncMock()) as mock_fetch_positions, \
             patch.object(portfolio_tracker, '_fetch_exchange_orders', AsyncMock()) as mock_fetch_orders:
            
            # Set up reconciliation timestamps
            now = datetime.now()
            # Make last reconciliation older than the interval
            portfolio_tracker._last_reconciliation_time = {
                "hyperliquid": now - timedelta(seconds=600),  # 10 minutes ago
                "backpack": now - timedelta(seconds=600)
            }
            
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
                "hyperliquid": now - timedelta(seconds=60),  # 1 minute ago
                "backpack": now - timedelta(seconds=60)
            }
            
            # Update the portfolio state again
            await portfolio_tracker.update()
            
            # Only positions and orders should be updated, not balances
            assert mock_fetch_balances.call_count == 0
            assert mock_fetch_positions.call_count == 2
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
            client_order_id="test-order-123"
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
            client_order_id="test-order-123"
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
            side=OrderSide.BUY
        )
        
        # Assign a position ID
        test_position.id = "position123"
        
        # Update the position
        portfolio_tracker.update_position("hyperliquid", test_position)
        
        # Check that the position was stored correctly
        assert "hyperliquid" in portfolio_tracker._positions
        assert "position123" in portfolio_tracker._positions["hyperliquid"]
        assert portfolio_tracker._positions["hyperliquid"]["position123"] == test_position

    def test_update_balance(self, portfolio_tracker):
        """Test updating a balance."""
        # Update a balance
        portfolio_tracker.update_balance("hyperliquid", "USDC", 10000.0)
        
        # Check that the balance was stored correctly
        assert "hyperliquid" in portfolio_tracker._balances
        assert "USDC" in portfolio_tracker._balances["hyperliquid"]
        assert portfolio_tracker._balances["hyperliquid"]["USDC"] == 10000.0

    def test_get_exchange_balance(self, portfolio_tracker):
        """Test getting an exchange balance."""
        # Set up test balances
        portfolio_tracker._balances = {
            "hyperliquid": {
                "USDC": Balance(asset="USDC", free=10000.0, locked=0.0, total=10000.0),
                "BTC": Balance(asset="BTC", free=1.0, locked=0.0, total=1.0)
            }
        }
        
        # Get a balance
        balance = portfolio_tracker.get_exchange_balance("hyperliquid", "USDC")
        
        # Check the result
        assert balance == 10000.0
        
        # Test getting a nonexistent balance
        balance = portfolio_tracker.get_exchange_balance("hyperliquid", "ETH")
        assert balance == 0.0
        
        # Test getting a balance from a nonexistent exchange
        balance = portfolio_tracker.get_exchange_balance("nonexistent", "USDC")
        assert balance == 0.0

    def test_get_total_capital(self, portfolio_tracker):
        """Test calculating total capital."""
        # Set up test balances
        portfolio_tracker._balances = {
            "hyperliquid": {
                "USDC": Balance(asset="USDC", free=10000.0, locked=0.0, total=10000.0)
            },
            "backpack": {
                "USDC": Balance(asset="USDC", free=5000.0, locked=0.0, total=5000.0)
            }
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
                    side=OrderSide.BUY
                ),
                "position2": Position(
                    symbol="ETH", 
                    size=5.0, 
                    entry_price=2000.0, 
                    mark_price=2100.0,
                    liquidation_price=1500.0,
                    unrealized_pnl=500.0,
                    leverage=3.0,
                    side=OrderSide.BUY
                )
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
                    side=OrderSide.BUY
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
                    side=OrderSide.BUY
                )
            }
        }
        
        # Calculate total exposure
        exposure = portfolio_tracker.get_total_exposure()
        
        # Check the result - should be BTC position (0.5 * 42000) + ETH position (5 * 2100)
        assert exposure == 0.5 * 42000 + 5.0 * 2100

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
                    side=OrderSide.BUY
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
                    side=OrderSide.BUY
                )
            }
        }
        
        # Calculate PNL
        realized, unrealized = portfolio_tracker.get_pnl()
        
        # Check the result
        assert realized == 0.0  # No realized PNL in our test setup
        assert unrealized == 1000.0 + 500.0  # Sum of unrealized PNL from both positions

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
            side=OrderSide.BUY
        )
        test_position.id = "position123"
        
        # Store the position
        portfolio_tracker._positions = {
            "hyperliquid": {
                "position123": test_position
            }
        }
        
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
            size=0.5, 
            entry_price=40000.0, 
            mark_price=42000.0,
            liquidation_price=30000.0,
            unrealized_pnl=1000.0,
            leverage=5.0,
            side=OrderSide.BUY
        )
        position1.id = "position1"
        
        position2 = Position(
            symbol="BTC", 
            size=0.3, 
            entry_price=41000.0, 
            mark_price=42000.0,
            liquidation_price=35000.0,
            unrealized_pnl=300.0,
            leverage=3.0,
            side=OrderSide.BUY
        )
        position2.id = "position2"
        
        # Store the positions
        portfolio_tracker._positions = {
            "hyperliquid": {
                "position1": position1,
                "position2": position2
            }
        }
        
        # Get positions by symbol
        positions = portfolio_tracker.get_positions_by_symbol("hyperliquid", "BTC")
        
        # Check the result
        assert len(positions) == 2
        assert position1 in positions
        assert position2 in positions

    def test_to_dict(self, portfolio_tracker):
        """Test serializing the portfolio state to a dictionary."""
        # Set up test data
        portfolio_tracker._balances = {
            "hyperliquid": {
                "USDC": Balance(asset="USDC", free=10000.0, locked=0.0, total=10000.0)
            }
        }
        
        test_position = Position(
            symbol="BTC", 
            size=0.5, 
            entry_price=40000.0, 
            mark_price=42000.0,
            liquidation_price=30000.0,
            unrealized_pnl=1000.0,
            leverage=5.0,
            side=OrderSide.BUY
        )
        test_position.id = "position1"
        
        portfolio_tracker._positions = {
            "hyperliquid": {
                "position1": test_position
            }
        }
        
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
            client_order_id="test-order-1"
        )
        
        portfolio_tracker._orders = {
            "hyperliquid": {
                "order1": test_order
            }
        }
        
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
        
        # Check that the dictionary can be serialized to JSON
        json_str = json.dumps(state_dict)
        assert isinstance(json_str, str) 