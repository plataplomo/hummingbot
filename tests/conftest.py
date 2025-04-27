import time
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from cyberdelta.core.models import (
    Balance,
    FundingRate,
    MarketData,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
)

# Import the classes we need to test
from cyberdelta.utils.config import Config
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Mock aiohttp ClientSession and Response for API testing
class MockResponse:
    def __init__(
        self,
        data: object,
        status: int = 200,
        headers: dict[str, str] | None = None,
        content_type: str = "application/json",
    ):
        self._data = data
        self.status = status
        self.headers = headers or {}
        self.content_type = content_type
        self._raise_for_status_called = False

    async def json(self):
        return self._data

    async def text(self):
        return str(self._data)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object):
        pass

    def raise_for_status(self):
        self._raise_for_status_called = True
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=self.status
            )


class MockClientSession:
    def __init__(
        self,
        responses: dict[tuple[str, str], MockResponse] | None = None,
    ):
        self.responses = responses or {}
        self.requests: list[dict[str, object]] = []
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object):
        pass

    async def close(self):
        self.closed = True

    async def _request(self, method: str, url: str, **kwargs: object) -> MockResponse:
        self.requests.append({"method": method, "url": url, "kwargs": kwargs})

        # Find match in responses
        for pattern, response in self.responses.items():
            if (
                (method, url) == pattern
                or (method, pattern[1]) == pattern
                and url.startswith(pattern[1])
            ):
                return response

        # Default response if no match
        return MockResponse({}, status=404)

    async def get(self, url: str, **kwargs: object) -> MockResponse:
        return await self._request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: object) -> MockResponse:
        return await self._request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: object) -> MockResponse:
        return await self._request("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs: object) -> MockResponse:
        return await self._request("DELETE", url, **kwargs)


@pytest.fixture
def mock_client_session():
    """Fixture to provide a mock aiohttp ClientSession."""

    def create_session(
        responses: dict[tuple[str, str], MockResponse] | None = None,
    ) -> MockClientSession:
        return MockClientSession(responses)

    return create_session


@pytest.fixture
def hyperliquid_config():
    """Fixture to provide Hyperliquid API configuration."""
    return {
        "rest_endpoint": "https://api.hyperliquid.xyz",
        "ws_endpoint": "wss://api.hyperliquid.xyz/ws",
        "rate_limits": {
            "default_rate": 10.0,
            "default_bucket": 50,
            "endpoints": {"POST:/user": {"rate": 5.0, "bucket": 20}},
        },
    }


@pytest.fixture
def backpack_config():
    """Fixture to provide Backpack API configuration."""
    return {
        "rest_endpoint": "https://api.backpack.exchange",
        "ws_endpoint": "wss://ws.backpack.exchange",
        "rate_limits": {
            "default_rate": 10.0,
            "default_bucket": 50,
            "endpoints": {"GET:/api/v1/depth": {"rate": 5.0, "bucket": 20}},
        },
    }


@pytest.fixture
def hyperliquid_secrets():
    """Fixture to provide Hyperliquid API secrets."""
    return {
        "HYPERLIQUID_WALLET_PRIVATE_KEY": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        "HYPERLIQUID_WALLET_ADDRESS": "0xabcdef1234567890abcdef1234567890abcdef12",
    }


@pytest.fixture
def backpack_secrets():
    """Fixture to provide Backpack API secrets."""
    return {
        "BACKPACK_API_KEY": "backpack-api-key-123456",
        "BACKPACK_API_SECRET": "backpack-api-secret-123456",
    }


# The event_loop fixture is now provided by pytest-asyncio
# No need to define it ourselves

# Additional fixtures for component testing


@pytest.fixture
def mock_config():
    """Create a mock Config object with test settings."""
    config_data = {
        "exchanges": {
            "mock_hl": {
                "enabled": True,
                "symbols": {"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
                "websocket": {
                    "reconnect_delay": 1,
                    "max_reconnect_delay": 5,
                    "ping_interval": 10,
                },
                "risk_modifier": 0.9,
            },
            "mock_bp": {
                "enabled": True,
                "symbols": {"BTC": "BTCUSDC", "ETH": "ETHUSDC"},
                "websocket": {
                    "reconnect_delay": 1,
                    "max_reconnect_delay": 5,
                    "ping_interval": 10,
                },
                "risk_modifier": 1.0,
            },
        },
        "portfolio": {
            "reconciliation_interval": 300  # 5 minutes
        },
        "risk": {
            "max_position_size": 1000.0,
            "max_total_exposure": 5000.0,
            "kelly_fraction": 0.5,
            "max_collateral_per_exchange": 0.8,
            "max_leverage": 5.0,
            "min_liquidation_buffer": 0.2,
        },
        "execution": {
            "max_slippage": 0.002,
            "max_retries": 3,
            "retry_delay_base": 1.0,
            "circuit_breaker": {"loss_threshold": 100.0, "failed_trades": 3},
        },
        "validation": {
            "circuit_breaker": {
                "enabled": True,
                "global": {
                    "api_errors": {
                        "enabled": True,
                        "threshold": 5,
                        "window_seconds": 120,
                        "cooldown_seconds": 600,
                    }
                },
                "exchanges": {
                    "mock_hl": {
                        "enabled": True,
                        "api_errors": {
                            "enabled": True,
                            "threshold": 3,
                            "window_seconds": 60,
                            "cooldown_seconds": 300,
                        },
                        "drawdown": {"enabled": False},
                        "volatility": {"enabled": False},
                        "liquidity": {"enabled": False},
                    },
                    "mock_bp": {
                        "enabled": True,
                        "api_errors": {
                            "enabled": True,
                            "threshold": 3,
                            "window_seconds": 60,
                            "cooldown_seconds": 300,
                        },
                        "drawdown": {"enabled": False},
                        "volatility": {"enabled": False},
                        "liquidity": {"enabled": False},
                    },
                },
            },
            "position_reconciliation": {
                "enabled": True,
                "check_interval": 300,
                "reconciliation_threshold": 0.01,
                "auto_correct": False,
            },
        },
        "data": {"staleness_thresholds": {"ticker": 60, "funding_rate": 300, "orderbook": 60}},
    }
    return Config(config_data)


@pytest.fixture
def mock_exchange_api():
    """Create a mock ExchangeAPI for testing."""
    mock_api = AsyncMock()

    # Configure common methods
    mock_api.get_balances.return_value = {
        "USDC": Balance(
            asset="USDC", free=Decimal("10000.0"), locked=Decimal("0.0"), total=Decimal("10000.0")
        ),
        "BTC": Balance(
            asset="BTC", free=Decimal("1.0"), locked=Decimal("0.0"), total=Decimal("1.0")
        ),
    }

    mock_api.get_positions.return_value = {
        "BTC": Position(
            symbol="BTC",
            size=Decimal("0.5"),
            entry_price=Decimal("40000.0"),
            mark_price=Decimal("42000.0"),
            liquidation_price=Decimal("30000.0"),
            unrealized_pnl=Decimal("1000.0"),
            leverage=Decimal("5.0"),
            side=OrderSide.BUY,
        )
    }

    mock_api.get_ticker.return_value = MarketData(
        symbol="BTC",
        timestamp=datetime.now(),
        open=Decimal("40000.0"),
        high=Decimal("42000.0"),
        low=Decimal("39000.0"),
        close=Decimal("41500.0"),
        volume=Decimal("100.0"),
    )

    mock_api.get_funding_rate.return_value = FundingRate(
        symbol="BTC",
        funding_rate=Decimal("0.0001"),
        predicted_rate=Decimal("0.00012"),
        mark_price=Decimal("41500.0"),
        index_price=Decimal("41450.0"),
        next_funding_time=int(time.time() * 1000) + 3600000,
    )

    mock_api.place_order.return_value = Order(
        id="order123",
        symbol="BTC",
        side=OrderSide.BUY,
        type=OrderType.LIMIT,
        price=Decimal("41000.0"),
        quantity=Decimal("0.1"),
        filled_quantity=Decimal("0.0"),
        status=OrderStatus.NEW,
        time=datetime.now(),
        client_order_id="test-order-123",
    )

    return mock_api


@pytest.fixture
def circuit_breaker_system(mock_config: Config):
    """Create a CircuitBreakerSystem instance using mock config."""
    from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem

    system = CircuitBreakerSystem(mock_config)
    return system


@pytest.fixture
def mock_portfolio_tracker():
    """Create a mock PortfolioTracker for testing."""
    mock_tracker = MagicMock()

    # Configure mock methods
    mock_tracker.get_total_capital.return_value = 20000.0
    mock_tracker.get_exchange_balance.return_value = 10000.0
    mock_tracker.get_exchange_exposure.return_value = 5000.0
    mock_tracker.get_total_exposure.return_value = 10000.0

    return mock_tracker


@pytest.fixture
def mock_data_handler():
    """Create a mock DataHandler for testing."""
    mock_handler = MagicMock()

    # Configure mock methods
    mock_handler.get_ticker.return_value = MarketData(
        symbol="BTC",
        timestamp=datetime.now(),
        open=Decimal("40000.0"),
        high=Decimal("42000.0"),
        low=Decimal("39000.0"),
        close=Decimal("41500.0"),
        volume=Decimal("100.0"),
    )

    mock_handler.get_funding_rate.return_value = (Decimal("0.0001"), datetime.now())

    return mock_handler


@pytest.fixture
def mock_arbitrage_opportunity():
    """Create a mock ArbitrageOpportunity for testing."""
    opportunity = MagicMock(spec=ArbitrageOpportunity)
    opportunity.symbol = "BTC"
    opportunity.long_exchange = "hyperliquid"
    opportunity.short_exchange = "backpack"
    opportunity.net_funding_differential = 0.05  # 5 basis points
    opportunity.basis_volatility = 0.01
    opportunity.expected_profit = 10.0
    opportunity.confidence = 0.8
    opportunity.timestamp = datetime.now()

    return opportunity
