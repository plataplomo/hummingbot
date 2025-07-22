"""Exchange API mocks and test data fixtures.

This module provides mock exchange APIs, trading data, and other
exchange-specific test fixtures.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    SpotBalance,
    Ticker,
    TimeInForce,
)
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Import path setup to ensure cyberdelta can be imported

# --- Mock Exchange API ---


@pytest.fixture
def mock_exchange_api() -> AsyncMock:
    """Create a mock ExchangeAPI for testing.

    Returns:
        AsyncMock: A mock exchange API configured with test data and methods.
    """
    mock_api = AsyncMock()
    now = datetime.now(UTC)

    # Configure common methods
    mock_api.get_balances.return_value = {
        "USDC": SpotBalance(
            asset="USDC",
            exchange="hyperliquid",
            total_quantity=Decimal(10000),
            available_quantity=Decimal(10000),
            timestamp=datetime.now(UTC),
        ),
        "BTC": SpotBalance(
            asset="BTC",
            exchange="hyperliquid",
            total_quantity=Decimal(1),
            available_quantity=Decimal(1),
            timestamp=datetime.now(UTC),
        ),
    }

    mock_api.get_positions.return_value = {
        "BTC": DerivativePosition(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            symbol="BTC",
            size=Decimal("0.5"),
            entry_price=Decimal(60000),
            mark_price=Decimal(61000),
            side=OrderSide.BUY,
            unrealized_pnl=Decimal(500),
        ),
        "ETH": DerivativePosition(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            symbol="ETH",
            size=Decimal(-10),
            entry_price=Decimal(3000),
            mark_price=Decimal(2950),
            side=OrderSide.SELL,
            unrealized_pnl=Decimal(500),
        ),
    }

    mock_api.get_ticker.return_value = Ticker(
        symbol="BTC",
        exchange="test_exchange",
        bid=Decimal("40000.0"),
        ask=Decimal("40002.0"),
        price=Decimal("40001.0"),
        timestamp=now,  # Ticker expects datetime
    )

    mock_api.get_funding_rate.return_value = FundingRate(
        symbol="BTC",
        funding_rate=Decimal("0.0001"),
        mark_price=Decimal("41500.0"),
        index_price=Decimal("41450.0"),
        timestamp=now,
        next_funding_time=now + timedelta(hours=1),
    )

    mock_api.place_order.return_value = Order(
        exchange="hyperliquid",
        exchange_order_id="order123",
        symbol="BTC",
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        status=OrderStatus.NEW,
        price=Decimal("41000.0"),
        quantity_requested=Decimal("0.1"),
        quantity_filled=Decimal("0.0"),
        created_at=now,
        updated_at=now,
        client_order_id="test-order-123",
        related_order_id=None,
        time_in_force=TimeInForce.GTC,
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )

    mock_api.get_open_orders.return_value = [
        # Assuming get_open_orders returns a list of orders
    ]

    return mock_api


# --- Mock Trading Components ---


@pytest.fixture
def mock_portfolio_tracker() -> MagicMock:
    """Create a mock PortfolioTracker for testing.

    Returns:
        MagicMock: A mock portfolio tracker with preconfigured test values.
    """
    mock_tracker = MagicMock()

    # Configure mock methods
    mock_tracker.get_total_capital.return_value = 20000.0
    mock_tracker.get_exchange_balance.return_value = 10000.0
    mock_tracker.get_exchange_exposure.return_value = 5000.0
    mock_tracker.get_total_exposure.return_value = 10000.0

    return mock_tracker


@pytest.fixture
def mock_data_handler() -> MagicMock:
    """Create a mock DataHandler for testing.

    Returns:
        MagicMock: A mock data handler with preconfigured market data.
    """
    mock_handler = MagicMock()
    now = datetime.now(UTC)

    # Configure mock methods
    mock_handler.get_ticker.return_value = Ticker(
        symbol="BTC",
        exchange="test_exchange",
        bid=Decimal("40000.0"),
        ask=Decimal("40002.0"),
        price=Decimal("40001.0"),
        timestamp=now,
    )

    # Funding rate from handler returns tuple (rate, timestamp)
    mock_handler.get_funding_rate.return_value = (Decimal("0.0001"), now)

    return mock_handler


# --- Mock Trading Opportunities ---


@pytest.fixture
def mock_arbitrage_opportunity() -> MagicMock:
    """Create a mock ArbitrageOpportunity for testing.

    Returns:
        MagicMock: A mock arbitrage opportunity with test trading data.
    """
    opportunity = MagicMock(spec=ArbitrageOpportunity)
    opportunity.symbol = "BTC"
    opportunity.long_exchange = "hyperliquid"
    opportunity.short_exchange = "backpack"
    opportunity.net_funding_differential = 0.05  # 5 basis points
    opportunity.basis_volatility = 0.01
    opportunity.expected_profit = 10.0
    opportunity.confidence = 0.8
    opportunity.timestamp = datetime.now(UTC)
    opportunity.long_price = Decimal(30000)
    opportunity.short_price = Decimal(29999)
    opportunity.long_size = Decimal("0.1")
    opportunity.short_size = Decimal("0.1")

    return opportunity
