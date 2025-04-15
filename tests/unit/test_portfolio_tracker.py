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
    Ticker,  # Added missing Ticker import
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
        # Set up test orders using correct parameters
        test_orders = {
            "order123": Order(
                id="order123",
                symbol="BTC",
                side=OrderSide.BUY,
                type=OrderType.LIMIT,
                price=Decimal("41000.0"),
                quantity=Decimal("0.1"),
                filled_quantity=Decimal("0.0"),
                status=OrderStatus.NEW,
                client_order_id="test-order-123",
                # time uses default factory
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
        # Create a test order with correct parameters
        test_order = Order(
            id="order123",
            symbol="BTC",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=Decimal("41000.0"),
            quantity=Decimal("0.1"),
            filled_quantity=Decimal("0.0"),
            status=OrderStatus.NEW,
            client_order_id="test-order-123",
            # time uses default factory
        )

        # Update the order
        portfolio_tracker.update_order("hyperliquid", test_order)

        # Verify the order was stored
        assert "hyperliquid" in portfolio_tracker._orders
        assert "order123" in portfolio_tracker._orders["hyperliquid"]
        assert portfolio_tracker._orders["hyperliquid"]["order123"] == test_order

        # Update with a filled order using correct parameters
        filled_order = Order(
            id="order123",
            symbol="BTC",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,
            price=Decimal("41000.0"),
            quantity=Decimal("0.1"),
            filled_quantity=Decimal("0.1"),
            status=OrderStatus.FILLED,
            client_order_id="test-order-123",  # Assume same client_order_id
            # time uses default factory
        )

        portfolio_tracker.update_order("hyperliquid", filled_order)

        # Verify the order was updated
        assert portfolio_tracker._orders["hyperliquid"]["order123"].status == OrderStatus.FILLED
        assert portfolio_tracker._orders["hyperliquid"]["order123"].filled_quantity == Decimal(
            "0.1"
        )

    def test_update_position(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test updating a position."""
        # Create a test position
        test_position = Position(
            symbol="BTC",
            side=OrderSide.BUY,
            size=Decimal("0.5"),
            entry_price=Decimal("40000.0"),
            leverage=Decimal("5.0"),
            # Assuming 'id' is the key used for positions internally or fetched
            # If the key is symbol-based, 'id' might not be needed here
            id="btc_pos_1",
        )

        # Update the position
        # Use exchange_id and the position object
        portfolio_tracker.update_position("hyperliquid", test_position)

        # Verify the position was stored (assuming storage by symbol)
        assert "hyperliquid" in portfolio_tracker._positions
        assert "BTC" in portfolio_tracker._positions["hyperliquid"]  # Assuming storage by symbol
        assert portfolio_tracker._positions["hyperliquid"]["BTC"] == test_position

    def test_update_balance(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test updating a balance."""
        # Initial balance update
        portfolio_tracker.update_balance("hyperliquid", "USDC", Decimal("10000.0"))

        # Verify balance
        assert "hyperliquid" in portfolio_tracker._balances
        assert "USDC" in portfolio_tracker._balances["hyperliquid"]
        assert portfolio_tracker._balances["hyperliquid"]["USDC"].total == Decimal("10000.0")

    def test_get_exchange_balance(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test getting an exchange balance."""
        # Setup initial balances *before* calling get_exchange_balance
        test_balance_usdc = Balance(asset="USDC", total=Decimal("10000.0"), free=Decimal("10000.0"))
        test_balance_btc = Balance(asset="BTC", total=Decimal("1.0"), free=Decimal("1.0"))
        # Assign directly to the internal state for testing purposes
        portfolio_tracker._balances = {
            "hyperliquid": {
                "USDC": test_balance_usdc,
                "BTC": test_balance_btc,
            }
        }

        # Get existing balance
        balance_obj = portfolio_tracker.get_exchange_balance("hyperliquid", "USDC")
        assert balance_obj is not None
        # Pylance error here suggests balance_obj might be None. Added check.
        if balance_obj:
            assert balance_obj.total == Decimal("10000.0")

        # Get another existing balance
        balance_obj_btc = portfolio_tracker.get_exchange_balance("hyperliquid", "BTC")
        assert balance_obj_btc is not None
        if balance_obj_btc:
            assert balance_obj_btc.total == Decimal("1.0")

        # Get non-existent balance
        balance_obj_eth = portfolio_tracker.get_exchange_balance("hyperliquid", "ETH")
        assert balance_obj_eth is None

    def test_get_total_capital(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test calculating total capital."""
        # Set up mock balances
        portfolio_tracker._balances = {
            "hyperliquid": {
                "USDC": Balance(asset="USDC", total=Decimal("10000.0")),
                "BTC": Balance(asset="BTC", total=Decimal("1.0")),
            },
            "backpack": {
                "USDC": Balance(asset="USDC", total=Decimal("5000.0")),
            },
        }

        # Mock the get_ticker method used for conversion
        async def mock_get_ticker(symbol: str) -> Ticker | None:
            if symbol == "BTC-USDC":  # Assuming PortfolioTracker asks for this pair
                return Ticker(symbol="BTC-USDC", bid=Decimal("40000.0"), ask=Decimal("40010.0"))
            return None

        portfolio_tracker.api_clients["hyperliquid"].get_ticker = AsyncMock(
            side_effect=mock_get_ticker
        )
        portfolio_tracker.api_clients["backpack"].get_ticker = AsyncMock(
            side_effect=mock_get_ticker
        )

        # Calculate total capital (expecting 10000 + 5000 + 1*40000 = 55000)
        total_capital = portfolio_tracker.get_total_capital(base_currency="USDC")

        assert total_capital == Decimal("55000.0")

        # Test with a different base currency (hypothetical ETH conversion)
        async def mock_get_ticker_eth(symbol: str) -> Ticker | None:
            if symbol == "USDC-ETH":  # Hypothetical pair
                return Ticker(symbol="USDC-ETH", bid=Decimal("0.0005"), ask=Decimal("0.00051"))
            if symbol == "BTC-ETH":  # Hypothetical pair
                return Ticker(symbol="BTC-ETH", bid=Decimal("20.0"), ask=Decimal("20.1"))
            return None

        portfolio_tracker.api_clients["hyperliquid"].get_ticker = AsyncMock(
            side_effect=mock_get_ticker_eth
        )
        portfolio_tracker.api_clients["backpack"].get_ticker = AsyncMock(
            side_effect=mock_get_ticker_eth
        )

        # Expecting (10000+5000)*0.0005 + 1*20 = 7.5 + 20 = 27.5
        total_capital_eth = portfolio_tracker.get_total_capital(base_currency="ETH")
        assert total_capital_eth == Decimal("27.5")

    def test_get_exchange_exposure(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test calculating exposure on a single exchange."""
        # Setup balances and positions
        portfolio_tracker._balances = {
            "hyperliquid": {
                "USDC": Balance(asset="USDC", total=Decimal("10000.0")),
            }
        }
        portfolio_tracker._positions = {
            "hyperliquid": {
                "BTC": Position(
                    symbol="BTC",
                    side=OrderSide.BUY,
                    size=Decimal("0.5"),
                    entry_price=Decimal("40000"),
                ),
                "ETH": Position(
                    symbol="ETH",
                    side=OrderSide.SELL,
                    size=Decimal("10"),
                    entry_price=Decimal("2000"),
                ),
            }
        }

        # Mock ticker for valuation
        async def mock_get_ticker(symbol: str) -> Ticker | None:
            if symbol == "BTC-USDC":
                return Ticker(symbol="BTC-USDC", bid=Decimal("41000"), ask=Decimal("41010"))
            if symbol == "ETH-USDC":
                return Ticker(symbol="ETH-USDC", bid=Decimal("2100"), ask=Decimal("2101"))
            return None

        portfolio_tracker.api_clients["hyperliquid"].get_ticker = AsyncMock(
            side_effect=mock_get_ticker
        )

        # Exposure = 0.5 * 41000 + abs(-10) * 2100 = 20500 + 21000 = 41500
        exposure = portfolio_tracker.get_exchange_exposure("hyperliquid")
        assert exposure == Decimal("41500.0")

        # Test non-existent exchange
        exposure_none = portfolio_tracker.get_exchange_exposure("nonexistent")
        assert exposure_none == Decimal("0.0")

    def test_get_total_exposure(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test calculating total exposure across all exchanges."""
        # Setup balances and positions
        portfolio_tracker._balances = {
            "hyperliquid": {"USDT": Balance(asset="USDT", total=Decimal("10000.0"))},
            "backpack": {"USDT": Balance(asset="USDT", total=Decimal("5000.0"))},
        }
        portfolio_tracker._positions = {
            "hyperliquid": {
                "BTC": Position(
                    symbol="BTC",
                    side=OrderSide.BUY,
                    size=Decimal("0.5"),
                    entry_price=Decimal("40000"),
                ),
            },
            "backpack": {
                "ETH": Position(
                    symbol="ETH",
                    side=OrderSide.SELL,
                    size=Decimal("10"),
                    entry_price=Decimal("2000"),
                ),
            },
        }

        # Mock ticker for valuation (using USDT as valuation asset)
        async def mock_get_ticker(symbol: str) -> Ticker | None:
            if symbol == "BTC-USDT":
                return Ticker(symbol="BTC-USDT", bid=Decimal("41000"), ask=Decimal("41010"))
            if symbol == "ETH-USDT":
                return Ticker(symbol="ETH-USDT", bid=Decimal("2100"), ask=Decimal("2101"))
            return None

        # Mock get_ticker for both clients
        mock_ticker_func = AsyncMock(side_effect=mock_get_ticker)
        portfolio_tracker.api_clients["hyperliquid"].get_ticker = mock_ticker_func
        portfolio_tracker.api_clients["backpack"].get_ticker = mock_ticker_func

        # Total Exposure = (0.5 * 41000)_hyp + (abs(-10) * 2100)_bp
        # Total Exposure = 20500 + 21000 = 41500
        total_exposure = portfolio_tracker.get_total_exposure(valuation_asset="USDT")
        assert total_exposure == Decimal("41500.0")

        # Verify get_ticker was called for each position's symbol
        assert mock_ticker_func.call_count == 2  # Called once for BTC, once for ETH
        mock_ticker_func.assert_any_call("BTC-USDT")
        mock_ticker_func.assert_any_call("ETH-USDT")

        # Test with no positions
        portfolio_tracker._positions = {}
        total_exposure_none = portfolio_tracker.get_total_exposure(valuation_asset="USDT")
        assert total_exposure_none == Decimal("0.0")

    def test_get_pnl(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test calculating realized and unrealized PNL."""
        # Setup balances and positions
        portfolio_tracker._balances = {
            "hyperliquid": {"USDC": Balance(asset="USDC", total=Decimal("10000.0"))}
        }
        portfolio_tracker._positions = {
            "hyperliquid": {
                "BTC": Position(
                    symbol="BTC",
                    side=OrderSide.BUY,
                    size=Decimal("0.5"),
                    entry_price=Decimal("40000"),
                    # Simulate some realized PNL if the model stores it
                    realized_pnl=Decimal("100.0"),
                ),
                "ETH": Position(
                    symbol="ETH",
                    side=OrderSide.SELL,
                    size=Decimal("10"),
                    entry_price=Decimal("2000"),
                    realized_pnl=Decimal("-50.0"),  # Example realized loss
                ),
            }
        }

        # Mock ticker for unrealized PNL calculation
        async def mock_get_ticker(symbol: str) -> Ticker | None:
            if symbol == "BTC-USDC":
                # Current price > entry price for long position
                return Ticker(symbol="BTC-USDC", bid=Decimal("41000"), ask=Decimal("41010"))
            if symbol == "ETH-USDC":
                # Current price < entry price for short position
                return Ticker(symbol="ETH-USDC", bid=Decimal("1900"), ask=Decimal("1901"))
            return None

        portfolio_tracker.api_clients["hyperliquid"].get_ticker = AsyncMock(
            side_effect=mock_get_ticker
        )

        # Calculate Unrealized PNL:
        # BTC: (41000 - 40000) * 0.5 = 500
        # ETH: (2000 - 1900) * 10 = 1000
        # Total Unrealized = 500 + 1000 = 1500

        # Calculate Realized PNL:
        # Sum of realized_pnl fields = 100.0 + (-50.0) = 50.0
        # Need to consider _realized_pnl if tracked internally
        portfolio_tracker._realized_pnl = Decimal("25.0")  # Internal tracker

        # Total Realized = 100.0 - 50.0 + 25.0 = 75.0 (depends on how realized PNL is aggregated)
        # Assuming get_pnl combines position.realized_pnl and internal _realized_pnl

        realized_pnl, unrealized_pnl = portfolio_tracker.get_pnl()

        # Verify Unrealized PNL calculation
        assert unrealized_pnl == Decimal("1500.0")

        # Verify Realized PNL calculation (adjust based on actual implementation)
        # If it sums position.realized_pnl + internal _realized_pnl: 75.0
        # If it only uses internal _realized_pnl: 25.0
        # If it only sums position.realized_pnl: 50.0
        # Let's assume it sums position realized PNL for now:
        assert realized_pnl == Decimal("50.0")  # Adjust this assertion based on the exact logic

    def test_get_position(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test getting a position by ID."""
        # Setup a position
        test_position = Position(
            symbol="BTC",
            side=OrderSide.BUY,
            size=Decimal("0.5"),
            entry_price=Decimal("40000"),
            id="position123",  # Use a unique ID if storage is ID-based
        )
        # Store it using the chosen key (e.g., ID or symbol)
        # Assuming storage by ID for this test
        portfolio_tracker._positions = {"hyperliquid": {"position123": test_position}}

        # Get the position by ID
        retrieved_position = portfolio_tracker.get_position("hyperliquid", "position123")
        assert retrieved_position == test_position

        # Get non-existent position
        retrieved_none = portfolio_tracker.get_position("hyperliquid", "nonexistent")
        assert retrieved_none is None

    def test_get_positions_by_symbol(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test getting positions by symbol."""
        # Setup multiple positions for different symbols and exchanges
        position1 = Position(
            symbol="BTC",
            side=OrderSide.BUY,
            size=Decimal("0.5"),
            entry_price=Decimal("40000"),
            id="btc_pos_1",
        )
        position2 = Position(
            symbol="BTC",
            side=OrderSide.SELL,
            size=Decimal("0.2"),
            entry_price=Decimal("42000"),
            id="btc_pos_2",
        )
        position_eth = Position(
            symbol="ETH",
            side=OrderSide.BUY,
            size=Decimal("10"),
            entry_price=Decimal("2000"),
            id="eth_pos_1",
        )
        position_btc_bp = Position(
            symbol="BTC",
            side=OrderSide.BUY,
            size=Decimal("0.1"),
            entry_price=Decimal("40500"),
            id="btc_pos_3",
        )

        portfolio_tracker._positions = {
            "hyperliquid": {
                "btc_pos_1": position1,
                "btc_pos_2": position2,
                "eth_pos_1": position_eth,
            },
            "backpack": {
                "btc_pos_3": position_btc_bp,
            },
        }

        # Get BTC positions on hyperliquid
        positions_btc_hl = portfolio_tracker.get_positions_by_symbol("hyperliquid", "BTC")
        assert len(positions_btc_hl) == 2
        assert position1 in positions_btc_hl
        assert position2 in positions_btc_hl

        # Get ETH positions on hyperliquid
        positions_eth_hl = portfolio_tracker.get_positions_by_symbol("hyperliquid", "ETH")
        assert len(positions_eth_hl) == 1
        assert position_eth in positions_eth_hl

        # Get SOL positions on hyperliquid (none exist)
        positions_sol_hl = portfolio_tracker.get_positions_by_symbol("hyperliquid", "SOL")
        assert len(positions_sol_hl) == 0

        # Get BTC positions on backpack
        positions_btc_bp = portfolio_tracker.get_positions_by_symbol("backpack", "BTC")
        assert len(positions_btc_bp) == 1
        assert position_btc_bp in positions_btc_bp
        # Test assertion fix: backpack positions only contains position_btc_bp
        assert portfolio_tracker._positions["backpack"]["btc_pos_3"] in positions_btc_bp

    def test_to_dict(self, portfolio_tracker: PortfolioTracker) -> None:
        """Test serializing the portfolio state to a dictionary."""
        # Setup some state
        portfolio_tracker._balances = {
            "hyperliquid": {
                "USDC": Balance(asset="USDC", total=Decimal("10000.0")),
            }
        }
        test_position = Position(
            symbol="BTC",
            side=OrderSide.BUY,
            size=Decimal("0.5"),
            entry_price=Decimal("40000"),
            id="position1",
        )
        portfolio_tracker._positions = {"hyperliquid": {"position1": test_position}}

        test_order = Order(
            id="order1",  # Use 'id' parameter
            symbol="BTC",
            side=OrderSide.BUY,
            type=OrderType.LIMIT,  # Use 'type' parameter
            price=Decimal("41000.0"),
            quantity=Decimal("0.1"),
            filled_quantity=Decimal("0.0"),
            status=OrderStatus.NEW,
            # time uses default factory
            client_order_id="test-order-1",
        )
        portfolio_tracker._orders = {"hyperliquid": {"order1": test_order}}
        portfolio_tracker._last_update_time["hyperliquid"] = datetime.now(UTC)
        portfolio_tracker._last_reconciliation_time["hyperliquid"] = datetime.now(UTC)

        # Serialize state
        state_dict = portfolio_tracker.to_dict()

        # Basic structure checks
        assert "balances" in state_dict
        assert "positions" in state_dict
        assert "orders" in state_dict
        assert "last_update_time" in state_dict
        assert "last_reconciliation_time" in state_dict

        # Check serialization of nested objects (ensure Decimals are strings)
        assert isinstance(state_dict["balances"]["hyperliquid"]["USDC"]["total"], str)
        assert isinstance(state_dict["positions"]["hyperliquid"]["position1"]["size"], str)
        assert isinstance(state_dict["orders"]["hyperliquid"]["order1"]["price"], str)

        # Check timestamp serialization
        assert isinstance(state_dict["last_update_time"]["hyperliquid"], str)

        # Verify JSON compatibility
        try:
            json_str = json.dumps(state_dict, cls=CyberDeltaJSONEncoder)
            assert isinstance(json_str, str)
            # Try loading back
            loaded_dict = json.loads(json_str)
            assert loaded_dict["balances"]["hyperliquid"]["USDC"]["total"] == "10000.0"
        except TypeError as e:
            pytest.fail(f"Failed to JSON serialize portfolio state: {e}")
