"""Exchange API mocks and test data fixtures.

This module provides mock exchange APIs, trading data, and other
exchange-specific test fixtures.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import (
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
from cyberdelta.symbols import exchanges
from tests.common_symbols import BTC_HL, ETH_HL


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
            asset=exchanges.hyperliquid("USD"),
            exchange=ExchangeName.HYPERLIQUID,
            total_quantity=Decimal(10000),
            available_quantity=Decimal(10000),
            timestamp=datetime.now(UTC),
        ),
        BTC_HL.value: SpotBalance(
            asset=BTC_HL,
            exchange=ExchangeName.HYPERLIQUID,
            total_quantity=Decimal(1),
            available_quantity=Decimal(1),
            timestamp=datetime.now(UTC),
        ),
    }

    mock_api.get_positions.return_value = {
        BTC_HL.value: DerivativePosition(
            exchange=ExchangeName.HYPERLIQUID,
            timestamp=datetime.now(UTC),
            symbol=BTC_HL,
            size=Decimal("0.5"),
            entry_price=Decimal(60000),
            mark_price=Decimal(61000),
            side=OrderSide.BUY,
            unrealized_pnl=Decimal(500),
        ),
        ETH_HL.value: DerivativePosition(
            exchange=ExchangeName.HYPERLIQUID,
            timestamp=datetime.now(UTC),
            symbol=ETH_HL,
            size=Decimal(-10),
            entry_price=Decimal(3000),
            mark_price=Decimal(2950),
            side=OrderSide.SELL,
            unrealized_pnl=Decimal(500),
        ),
    }

    mock_api.get_ticker.return_value = Ticker(
        symbol=exchanges.hyperliquid("BTC-PERP"),
        exchange=ExchangeName.HYPERLIQUID,
        bid=Decimal("40000.0"),
        ask=Decimal("40002.0"),
        price=Decimal("40001.0"),
        timestamp=now,  # Ticker expects datetime
    )

    mock_api.get_funding_rate.return_value = FundingRate(
        symbol=exchanges.hyperliquid("BTC-PERP"),
        funding_rate=Decimal("0.0001"),
        mark_price=Decimal("41500.0"),
        index_price=Decimal("41450.0"),
        timestamp=now,
        next_funding_time=now + timedelta(hours=1),
    )

    mock_api.place_order.return_value = Order(
        exchange=ExchangeName.HYPERLIQUID,
        exchange_order_id="order123",
        symbol=exchanges.hyperliquid("BTC-PERP"),
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
def mock_portfolio_state_manager() -> MagicMock:
    """Create a mock PortfolioStateManager for testing.

    Returns:
        MagicMock: A mock portfolio state manager with preconfigured test values.
    """
    mock_manager = MagicMock()

    # Configure mock methods for PortfolioStateManager interface
    mock_manager.get_total_capital.return_value = 20000.0
    mock_manager.get_exchange_balance.return_value = 10000.0
    mock_manager.get_exchange_exposure.return_value = 5000.0
    mock_manager.get_total_exposure.return_value = 10000.0

    # Additional methods for PortfolioStateManager
    mock_manager.get_current_state.return_value = MagicMock()
    mock_manager.get_positions.return_value = {}
    mock_manager.get_balances.return_value = {}
    mock_manager.process_trade.return_value = None

    return mock_manager


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
        symbol=exchanges.hyperliquid("BTC-PERP"),
        exchange=ExchangeName.HYPERLIQUID,
        bid=Decimal("40000.0"),
        ask=Decimal("40002.0"),
        price=Decimal("40001.0"),
        timestamp=now,
    )

    # Funding rate from handler returns tuple (rate, timestamp)
    mock_handler.get_funding_rate.return_value = (Decimal("0.0001"), now)

    return mock_handler


# --- Mock Trading Opportunities ---
