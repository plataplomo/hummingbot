import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

# Removed unused: from typing import Any
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
from cyberdelta.utils.serialization import CyberDeltaJSONEncoder  # Import needed for test_to_dict


class TestPortfolioTracker:
    """Test suite for PortfolioTracker component."""

    @pytest.fixture()
    def portfolio_tracker(
        self, mock_config: MagicMock, mock_exchange_api: AsyncMock
    ) -> PortfolioTracker:
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

    @pytest.mark.asyncio()
    async def test_register_api_client(
        self, portfolio_tracker: PortfolioTracker, mock_exchange_api: AsyncMock
    ) -> None:
        """Test that API clients can be registered."""
        # Register a new API client
        portfolio_tracker.register_api_client("test_exchange", mock_exchange_api)

        # Verify the client was registered
        assert "test_exchange" in portfolio_tracker.api_clients
        assert portfolio_tracker.api_clients["test_exchange"] == mock_exchange_api

    @pytest.mark.asyncio()
    async def test_initialize(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test initialization of the PortfolioTracker."""
        # Patch the fetch methods
        with (
            patch.object(
                portfolio_tracker, "_fetch_exchange_balances", AsyncMock()
            ) as mock_fetch_balances,
            patch.object(
                portfolio_tracker, "_fetch_exchange_positions", AsyncMock()
            ) as mock_fetch_positions,
            # patch.object( # mock_fetch_orders is unused in this test scope
            #     portfolio_tracker, "_fetch_exchange_orders", AsyncMock()
            # ), # Removed assignment to unused mock_fetch_orders
        ):
            # Initialize the tracker
            await portfolio_tracker.initialize()

            # Verify methods were called for both exchanges
            assert mock_fetch_balances.call_count == 2
            assert mock_fetch_positions.call_count == 2
            # assert mock_fetch_orders.call_count == 2 # Order fetching removed from init logic

            # Verify calls for hyperliquid
            mock_fetch_balances.assert_any_call("hyperliquid")
            mock_fetch_positions.assert_any_call("hyperliquid")
            # mock_fetch_orders.assert_any_call("hyperliquid") # Orders not fetched in init logic

            # Verify calls for backpack
            mock_fetch_balances.assert_any_call("backpack")
            mock_fetch_positions.assert_any_call("backpack")
            # mock_fetch_orders.assert_any_call("backpack") # Orders not fetched in init logic

    @pytest.mark.asyncio()
    async def test_fetch_exchange_balances(
        self, portfolio_tracker: PortfolioTracker, mock_exchange_api: AsyncMock
    ) -> None:
        """Test fetching balances from an exchange."""
        # Set up test balances
        test_balances: dict[str, Balance] = {
            "USDC": Balance(
                asset="USDC",
                free=Decimal("10000.0"),
                locked=Decimal("0.0"),
                total=Decimal("10000.0"),
            ),
            "BTC": Balance(
                asset="BTC",
                free=Decimal("1.0"),
                locked=Decimal("0.0"),
                total=Decimal("1.0"),
            ),
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

    @pytest.mark.asyncio()
    async def test_fetch_exchange_positions(
        self, portfolio_tracker: PortfolioTracker, mock_exchange_api: AsyncMock
    ) -> None:
        """Test fetching positions from an exchange."""
        # Set up test positions as a LIST of Position objects
        test_positions_list = [
            Position(
                symbol="BTC",
                size=Decimal("0.5"),
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

    @pytest.mark.asyncio()
    async def test_fetch_exchange_orders(
        self, portfolio_tracker: PortfolioTracker, mock_exchange_api: AsyncMock
    ) -> None:
        """Test fetching orders from an exchange."""
        # Set up test orders
        test_orders = {
            # Mypy flagged id, type, time as unexpected kwargs. Assuming they are set differently.
            "order123": Order(
                order_id="order123",  # Assuming order_id is the correct field name
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,  # Assuming order_type is the correct field name
                price=Decimal("41000.0"),
                quantity=Decimal("0.1"),
                filled_quantity=Decimal("0.0"),
                status=OrderStatus.NEW,  # Use Enum
                timestamp=datetime.now(
                    UTC
                ),  # Assuming timestamp is the correct field name and type
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

    @pytest.mark.asyncio()
    async def test_update(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test updating portfolio state."""
        # Patch the config.get method for this test's scope
        with patch.object(
            portfolio_tracker.config,
            "get",
            MagicMock(
                side_effect=lambda key, default=None: {
                    "exchanges.hyperliquid.enabled": True,
                    "exchanges.backpack.enabled": True,
                    # Add other config gets if needed by the method, otherwise return default
                }.get(key, default)
            ),
        ) as mock_config_get:
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
                    "backpack": now
                    - timedelta(seconds=portfolio_tracker.reconciliation_interval + 1),
                }
                # Ensure interval is positive
                assert portfolio_tracker.reconciliation_interval > 0

                # Update the portfolio state
                await portfolio_tracker.update()

                # Verify config.get was called (e.g., to check enabled exchanges)
                mock_config_get.assert_called()  # Basic check

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

    def test_update_order(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test updating an order."""
        # Create a test order
        test_order = Order(
            order_id="order123",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("41000.0"),
            quantity=Decimal("0.1"),
            filled_quantity=Decimal("0.0"),
            status=OrderStatus.NEW,  # Use Enum
            timestamp=datetime.now(UTC),
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
            order_id="order123",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("41000.0"),
            quantity=Decimal("0.1"),
            filled_quantity=Decimal("0.1"),
            status=OrderStatus.FILLED,
            timestamp=datetime.now(UTC),
            client_order_id="test-order-123",
        )

        portfolio_tracker.update_order("hyperliquid", filled_order)

        # Check that the order was updated
        assert portfolio_tracker._orders["hyperliquid"]["order123"] == filled_order

    def test_update_position(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test updating a position."""
        # Create a test position
        test_position = Position(
            symbol="BTC",
            size=Decimal("0.5"),
            entry_price=Decimal("40000.0"),
            mark_price=Decimal("42000.0"),
            liquidation_price=Decimal("30000.0"),
            unrealized_pnl=Decimal("1000.0"),
            leverage=Decimal("5.0"),
            side=OrderSide.BUY,
        )
        test_position.id = "position123"  # Assuming positions have an ID attribute

        # Update the position
        portfolio_tracker.update_position("hyperliquid", test_position)

        # Check that the position was stored correctly
        assert "hyperliquid" in portfolio_tracker._positions
        assert "BTC" in portfolio_tracker._positions["hyperliquid"]  # Assuming storage by symbol
        assert portfolio_tracker._positions["hyperliquid"]["BTC"] == test_position

    def test_update_balance(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test updating a balance."""
        # Update a balance - Use Decimal
        portfolio_tracker.update_balance("hyperliquid", "USDC", Decimal("10000.0"))

        # Check that the balance was stored correctly
        assert "hyperliquid" in portfolio_tracker._balances
        assert "USDC" in portfolio_tracker._balances["hyperliquid"]
        assert portfolio_tracker._balances["hyperliquid"]["USDC"].total == Decimal("10000.0")

    def test_get_exchange_balance(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test getting an exchange balance."""
        # Set up test balances using Balance objects
        test_balance_usdc = Balance(
            asset="USDC",
            free=Decimal("10000.0"),
            locked=Decimal("0.0"),
            total=Decimal("10000.0"),
        )
        test_balance_btc = Balance(
            asset="BTC", free=Decimal("1.0"), locked=Decimal("0.0"), total=Decimal("1.0")
        )
        portfolio_tracker._balances = {
            "hyperliquid": {"USDC": test_balance_usdc, "BTC": test_balance_btc},
            "backpack": {},
        }

        # Get balance for USDC on hyperliquid
        balance_obj = portfolio_tracker.get_exchange_balance("hyperliquid", "USDC")
        assert balance_obj == test_balance_usdc
        assert balance_obj.total == Decimal("10000.0")

        # Get balance for BTC on hyperliquid
        balance_obj_btc = portfolio_tracker.get_exchange_balance("hyperliquid", "BTC")
        assert balance_obj_btc == test_balance_btc
        assert balance_obj_btc.total == Decimal("1.0")

        # Get balance for nonexistent asset
        balance_obj_none = portfolio_tracker.get_exchange_balance("hyperliquid", "ETH")
        assert balance_obj_none is None

        # Get balance for nonexistent exchange
        balance_obj_none_exchange = portfolio_tracker.get_exchange_balance("nonexistent", "USDC")
        assert balance_obj_none_exchange is None  # Should return None

    def test_get_total_capital(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test calculating total capital, assuming balances are in base currency (e.g., USD)."""
        # Set up test balances (assuming all 'total' are in USD or equivalent)
        portfolio_tracker._balances = {
            "hyperliquid": {
                "USDC": Balance(
                    asset="USDC",
                    free=Decimal("10000.0"),
                    locked=Decimal("0.0"),
                    total=Decimal("10000.0"),
                ),
                "BTC": Balance(
                    asset="BTC", free=Decimal("0.0"), locked=Decimal("0.0"), total=Decimal("5000.0")
                ),  # Value in USD
            },
            "backpack": {
                "USDC": Balance(
                    asset="USDC",
                    free=Decimal("5000.0"),
                    locked=Decimal("0.0"),
                    total=Decimal("5000.0"),
                )
            },
        }

        # Calculate total capital by summing 'total' from all balances
        total = portfolio_tracker.get_total_capital()

        # Expected = 10000 (hyper USDC) + 5000 (hyper BTC value) + 5000 (back USDC) = 20000
        assert total == Decimal("20000.0")

    def test_get_exchange_exposure(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test calculating exchange exposure."""
        # Set up test positions
        portfolio_tracker._positions = {
            "hyperliquid": {
                "BTC": Position(
                    symbol="BTC",
                    size=Decimal("0.5"),
                    entry_price=Decimal("40000.0"),
                    mark_price=Decimal("42000.0"),  # Use mark price for exposure
                    liquidation_price=Decimal("30000.0"),
                    unrealized_pnl=Decimal("1000.0"),
                    leverage=Decimal("5.0"),
                    side=OrderSide.BUY,
                ),
                "ETH": Position(
                    symbol="ETH",
                    size=Decimal("5.0"),
                    entry_price=Decimal("2000.0"),
                    mark_price=Decimal("2100.0"),  # Use mark price for exposure
                    liquidation_price=Decimal("1500.0"),
                    unrealized_pnl=Decimal("500.0"),
                    leverage=Decimal("10.0"),
                    side=OrderSide.BUY,
                ),
            },
            "backpack": {},
        }

        # Calculate exposure for hyperliquid
        exposure = portfolio_tracker.get_exchange_exposure("hyperliquid")

        # Expected = Dec('0.5')*Dec('42000') + Dec('5.0')*Dec('2100') = 31500
        assert exposure == Decimal("31500.0")

        # Calculate exposure for backpack (no positions)
        exposure_bp = portfolio_tracker.get_exchange_exposure("backpack")
        assert exposure_bp == Decimal("0.0")

        # Calculate exposure for nonexistent exchange
        exposure_none = portfolio_tracker.get_exchange_exposure("nonexistent")
        assert exposure_none == Decimal("0.0")

    def test_get_total_exposure(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test calculating total exposure."""
        # Set up test positions
        portfolio_tracker._positions = {
            "hyperliquid": {
                "BTC": Position(
                    symbol="BTC",
                    size=Decimal("0.5"),
                    entry_price=Decimal("40000.0"),
                    mark_price=Decimal("42000.0"),
                    liquidation_price=Decimal("30000.0"),
                    unrealized_pnl=Decimal("1000.0"),
                    leverage=Decimal("5.0"),
                    side=OrderSide.BUY,
                ),
                "ETH": Position(
                    symbol="ETH",
                    size=Decimal("5.0"),
                    entry_price=Decimal("2000.0"),
                    mark_price=Decimal("2100.0"),
                    liquidation_price=Decimal("1500.0"),
                    unrealized_pnl=Decimal("500.0"),
                    leverage=Decimal("10.0"),
                    side=OrderSide.BUY,
                ),
            },
            "backpack": {
                "BTC": Position(
                    symbol="BTC",
                    size=Decimal("-1.0"),  # Short position
                    entry_price=Decimal("43000.0"),
                    mark_price=Decimal("42000.0"),
                    liquidation_price=Decimal("50000.0"),
                    unrealized_pnl=Decimal("1000.0"),
                    leverage=Decimal("5.0"),
                    side=OrderSide.SELL,
                ),
                "SOL": Position(
                    symbol="SOL",
                    size=Decimal("10.0"),
                    entry_price=Decimal("1.0"),
                    mark_price=Decimal("1.5"),  # Use mark price
                    liquidation_price=Decimal("0.5"),
                    unrealized_pnl=Decimal("5.0"),
                    leverage=Decimal("20.0"),
                    side=OrderSide.BUY,
                ),
            },
        }

        # Calculate total exposure
        exposure = portfolio_tracker.get_total_exposure()

        # Expected = (0.5*42k)+(5.0*2.1k)+(-1.0*42k)+(10.0*1.5) = 21k+10.5k-42k+15 = -10485
        expected_exposure = Decimal("-10485.0")
        assert exposure == expected_exposure

    def test_get_pnl(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test calculating PNL."""
        # Set up test positions
        portfolio_tracker._positions = {
            "hyperliquid": {
                "BTC": Position(
                    symbol="BTC",
                    size=Decimal("0.5"),
                    entry_price=Decimal("40000.0"),
                    mark_price=Decimal("42000.0"),
                    liquidation_price=Decimal("30000.0"),
                    unrealized_pnl=Decimal("1000.0"),
                    leverage=Decimal("5.0"),
                    side=OrderSide.BUY,
                ),
                "ETH": Position(
                    symbol="ETH",
                    size=Decimal("5.0"),
                    entry_price=Decimal("2000.0"),
                    mark_price=Decimal("2100.0"),
                    liquidation_price=Decimal("1500.0"),
                    unrealized_pnl=Decimal("500.0"),
                    leverage=Decimal("10.0"),
                    side=OrderSide.BUY,
                ),
            },
            "backpack": {
                "BTC": Position(
                    symbol="BTC",
                    size=Decimal("-1.0"),
                    entry_price=Decimal("43000.0"),
                    mark_price=Decimal("42000.0"),
                    liquidation_price=Decimal("50000.0"),
                    unrealized_pnl=Decimal("1000.0"),  # PNL is positive for short if price drops
                    leverage=Decimal("5.0"),
                    side=OrderSide.SELL,
                ),
                "SOL": Position(
                    symbol="SOL",
                    size=Decimal("10.0"),
                    entry_price=Decimal("1.0"),
                    mark_price=Decimal("1.5"),
                    liquidation_price=Decimal("0.5"),
                    unrealized_pnl=Decimal("5.0"),
                    leverage=Decimal("20.0"),
                    side=OrderSide.BUY,
                ),
            },
        }

        # Calculate total unrealized PNL
        unrealized = portfolio_tracker.get_pnl()

        # Expected = 1000 + 500 + 1000 + 5 = 2505
        expected_unrealized = Decimal("2505.0")
        assert unrealized == expected_unrealized

    def test_get_position(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test getting a position by ID."""
        # Create a test position
        test_position = Position(
            symbol="BTC",
            size=Decimal("0.5"),
            entry_price=Decimal("40000.0"),
            mark_price=Decimal("42000.0"),
            liquidation_price=Decimal("30000.0"),
            unrealized_pnl=Decimal("1000.0"),
            leverage=Decimal("5.0"),
            side=OrderSide.BUY,
        )
        test_position.id = "position123"  # Assuming positions have an ID attribute

        # Store the position
        # Assuming storage is exchange -> position_id -> Position
        portfolio_tracker._positions = {"hyperliquid": {"position123": test_position}}

        # Get the position
        position = portfolio_tracker.get_position("hyperliquid", "position123")

        # Check the result
        assert position == test_position

        # Test getting a nonexistent position
        position = portfolio_tracker.get_position("hyperliquid", "nonexistent")
        assert position is None

    def test_get_positions_by_symbol(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test getting positions by symbol."""
        # Create test positions
        position1 = Position(
            symbol="BTC",
            size=Decimal("0.5"),
            entry_price=Decimal("40000.0"),
            mark_price=Decimal("42000.0"),
            liquidation_price=Decimal("30000.0"),
            unrealized_pnl=Decimal("1000.0"),
            leverage=Decimal("5.0"),
            side=OrderSide.BUY,
        )
        position1.id = "btc_pos_1"

        position2 = Position(
            symbol="BTC",
            size=Decimal("0.3"),
            entry_price=Decimal("41000.0"),
            mark_price=Decimal("42000.0"),
            liquidation_price=Decimal("35000.0"),
            unrealized_pnl=Decimal("300.0"),
            leverage=Decimal("3.0"),
            side=OrderSide.BUY,
        )
        position2.id = "btc_pos_2"

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
        position_eth.id = "eth_pos_1"

        # Store the positions using a structure like exchange -> position_id -> Position
        portfolio_tracker._positions = {
            "hyperliquid": {
                "btc_pos_1": position1,
                "btc_pos_2": position2,
                "eth_pos_1": position_eth,
            },
            "backpack": {
                "btc_pos_3": Position(  # Add a BTC position on another exchange
                    symbol="BTC",
                    size=Decimal("-0.1"),
                    entry_price=Decimal("42500"),
                    mark_price=Decimal("42000"),
                    liquidation_price=Decimal("45000"),
                    unrealized_pnl=Decimal("50.0"),
                    leverage=Decimal("10.0"),
                    side=OrderSide.SELL,
                    id="btc_pos_3",
                )
            },
        }

        # Get positions by symbol "BTC" for hyperliquid
        positions_btc_hyper = portfolio_tracker.get_positions_by_symbol("hyperliquid", "BTC")

        # Check the result - should contain position1 and position2
        assert len(positions_btc_hyper) == 2
        assert position1 in positions_btc_hyper
        assert position2 in positions_btc_hyper

        # Get positions by symbol "ETH" for hyperliquid
        positions_eth_hyper = portfolio_tracker.get_positions_by_symbol("hyperliquid", "ETH")
        assert len(positions_eth_hyper) == 1
        assert position_eth in positions_eth_hyper

        # Get positions by symbol "BTC" for backpack
        positions_btc_bp = portfolio_tracker.get_positions_by_symbol("backpack", "BTC")
        assert len(positions_btc_bp) == 1
        assert portfolio_tracker._positions["backpack"]["btc_pos_3"] in positions_btc_bp

        # Test getting positions for a symbol with no positions
        positions_none = portfolio_tracker.get_positions_by_symbol("hyperliquid", "SOL")
        assert len(positions_none) == 0

    def test_to_dict(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test serializing the portfolio state to a dictionary."""
        # Set up test data
        portfolio_tracker._balances = {
            "hyperliquid": {
                "USDC": Balance(
                    asset="USDC",
                    free=Decimal("10000.0"),
                    locked=Decimal("0.0"),
                    total=Decimal("10000.0"),
                )
            }
        }

        test_position = Position(
            symbol="BTC",
            size=Decimal("0.5"),
            entry_price=Decimal("40000.0"),
            mark_price=Decimal("42000.0"),
            liquidation_price=Decimal("30000.0"),
            unrealized_pnl=Decimal("1000.0"),
            leverage=Decimal("5.0"),
            side=OrderSide.BUY,
        )
        test_position.id = "position1"

        portfolio_tracker._positions = {"hyperliquid": {"position1": test_position}}

        test_order = Order(
            order_id="order1",
            symbol="BTC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("41000.0"),
            quantity=Decimal("0.1"),
            filled_quantity=Decimal("0.0"),
            status=OrderStatus.NEW,
            timestamp=datetime.now(UTC),
            client_order_id="test-order-1",
        )

        portfolio_tracker._orders = {"hyperliquid": {"order1": test_order}}
        portfolio_tracker._last_update_time["hyperliquid"] = datetime.now(UTC)
        portfolio_tracker._last_reconciliation_time["hyperliquid"] = datetime.now(UTC)

        # Convert to dictionary
        state_dict = portfolio_tracker.to_dict()

        # Check the structure
        assert "balances" in state_dict
        assert "positions" in state_dict
        assert "orders" in state_dict
        assert "last_update_time" in state_dict
        assert "last_reconciliation_time" in state_dict

        # Check the content
        assert "hyperliquid" in state_dict["balances"]
        assert "USDC" in state_dict["balances"]["hyperliquid"]

        assert "hyperliquid" in state_dict["positions"]
        assert "position1" in state_dict["positions"]["hyperliquid"]

        assert "hyperliquid" in state_dict["orders"]
        assert "order1" in state_dict["orders"]["hyperliquid"]

        assert "hyperliquid" in state_dict["last_update_time"]
        assert "hyperliquid" in state_dict["last_reconciliation_time"]

        # Check that the dictionary can be serialized to JSON
        # Note: to_json method seems removed or renamed, testing standard json.dumps with encoder
        try:
            # Assuming CyberDeltaJSONEncoder is available via portfolio_tracker or globally
            # If not, this part needs adjustment based on actual encoder access
            # from cyberdelta.utils.serialization import CyberDeltaJSONEncoder # Imported top
            json_str = json.dumps(state_dict, cls=CyberDeltaJSONEncoder)
            assert isinstance(json_str, str)
            # Attempt to load it back to ensure it's valid JSON
            loaded_data = json.loads(json_str)
            assert isinstance(loaded_data, dict)
            # Check a Decimal value was converted to string by encoder
            assert isinstance(loaded_data["balances"]["hyperliquid"]["USDC"]["total"], str)
            assert (
                loaded_data["balances"]["hyperliquid"]["USDC"]["total"] == "10000.0"
            )  # Compare as string
            # Check datetime was converted to ISO string
            assert isinstance(loaded_data["last_update_time"]["hyperliquid"], str)
        except ImportError:
            pytest.skip("Skipping JSON serialization test: CyberDeltaJSONEncoder not found.")
        except Exception as e:
            pytest.fail(f"Serialization using json.dumps failed: {e}")
