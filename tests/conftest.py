from __future__ import annotations  # Enable postponed evaluation

import os
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import TracebackType  # Import TracebackType
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from cyberdelta.config.secrets_manager import SecretsManager

# Correct paths for utils and core
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
from cyberdelta.utils.config import Config
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem  # Import CB system
from cyberdelta.validation.funding_data import ArbitrageOpportunity

# Adjust path for fixtures
SCRIPT_DIR = os.path.dirname(__file__)
# Add project root to sys.path to allow imports like from cyberdelta.apis...
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
sys.path.insert(0, PROJECT_ROOT)

# Ensure tests can import from src


# Mock aiohttp ClientSession and Response for API testing
class MockResponse:
    def __init__(
        self,
        data: Any,  # noqa: ANN401 - Mock data can be anything for tests
        status: int = 200,
        headers: dict[str, str] | None = None,
        content_type: str = "application/json",
    ) -> None:
        self._data = data
        self.status = status
        self.headers = headers or {}
        self.content_type = content_type
        self._raise_for_status_called = False

    async def json(self) -> Any:  # noqa: ANN401 - Mock data can be anything for tests
        return self._data

    async def text(self) -> str:
        return str(self._data)

    async def __aenter__(self) -> MockResponse:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    def raise_for_status(self) -> None:
        self._raise_for_status_called = True
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=self.status
            )


class MockClientSession:
    def __init__(
        self,
        responses: dict[tuple[str, str], MockResponse] | None = None,
    ) -> None:
        self.responses = responses or {}
        self.requests: list[dict[str, Any]] = []  # Flexible for test requests
        self.closed = False

    async def __aenter__(self) -> MockClientSession:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        pass

    async def close(self) -> None:
        self.closed = True

    async def _request(
        self, method: str, url: str, **kwargs: dict[str, Any]
    ) -> MockResponse:  # Accepts any kwargs
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

    async def get(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:  # Accepts any kwargs
        return await self._request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:  # Accepts any kwargs
        return await self._request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:  # Accepts any kwargs
        return await self._request("PUT", url, **kwargs)

    async def delete(
        self, url: str, **kwargs: dict[str, Any]
    ) -> MockResponse:  # Accepts any kwargs
        return await self._request("DELETE", url, **kwargs)


@pytest.fixture
def mock_client_session() -> Callable[
    [dict[tuple[str, str], MockResponse] | None], MockClientSession
]:
    """Fixture to provide a mock aiohttp ClientSession."""

    def create_session(
        responses: dict[tuple[str, str], MockResponse] | None = None,
    ) -> MockClientSession:
        return MockClientSession(responses)

    return create_session


@pytest.fixture
def hyperliquid_config() -> dict[str, Any]:
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
def backpack_config() -> dict[str, Any]:
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
def hyperliquid_secrets() -> dict[str, str]:
    """Fixture to provide Hyperliquid API secrets."""
    return {
        "HYPERLIQUID_WALLET_PRIVATE_KEY": (
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        ),
        "HYPERLIQUID_WALLET_ADDRESS": "0xabcdef1234567890abcdef1234567890abcdef12",
    }


@pytest.fixture
def backpack_secrets() -> dict[str, str]:
    """Fixture to provide Backpack API secrets."""
    return {
        "BACKPACK_API_KEY": "backpack-api-key-123456",
        "BACKPACK_API_SECRET": "backpack-api-secret-123456",
    }


# The event_loop fixture is now provided by pytest-asyncio
# No need to define it ourselves

# Additional fixtures for component testing


@pytest.fixture
def mock_config() -> MagicMock:
    """Create a mock Config object with test settings."""
    config_data = {
        "exchanges": {
            "hyperliquid": {
                "enabled": True,
                "symbols": {"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
                "websocket": {
                    "reconnect_delay": 1,
                    "max_reconnect_delay": 5,
                    "ping_interval": 10,
                },
                "risk_modifier": 0.9,
            },
            "backpack": {
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
                    "hyperliquid": {
                        "enabled": True,
                        "api_errors": {
                            "enabled": True,
                            "type": "api_error",
                            "threshold": 3,
                            "window_seconds": 60,
                            "cooldown_seconds": 300,
                        },
                        "drawdown": {"enabled": False},
                        "volatility": {"enabled": False},
                        "liquidity": {"enabled": False},
                    },
                    "backpack": {
                        "enabled": True,
                        "api_errors": {
                            "enabled": True,
                            "type": "api_error",
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

    def getter(key: str, default: Any | None = None) -> Any | None:  # noqa: ANN401 - config getter can return Any
        return _deep_get(config_data, key, default)

    mock_cfg = MagicMock(spec=Config)
    mock_cfg.get = getter
    mock_cfg.config_data = config_data
    return mock_cfg


@pytest.fixture
def mock_exchange_api() -> AsyncMock:
    """Create a mock ExchangeAPI for testing."""
    mock_api = AsyncMock()
    now = datetime.now(UTC)

    # Configure common methods
    mock_api.get_balances.return_value = {
        "USDC": SpotBalance(
            asset="USDC",
            exchange="hyperliquid",
            total_quantity=Decimal("10000"),
            available_quantity=Decimal("10000"),
            timestamp=datetime.now(UTC),
        ),
        "BTC": SpotBalance(
            asset="BTC",
            exchange="hyperliquid",
            total_quantity=Decimal("1"),
            available_quantity=Decimal("1"),
            timestamp=datetime.now(UTC),
        ),
    }

    mock_api.get_positions.return_value = {
        "BTC": DerivativePosition(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            symbol="BTC",
            size=Decimal("0.5"),
            entry_price=Decimal("60000"),
            mark_price=Decimal("61000"),
            side=OrderSide.BUY,
            unrealized_pnl=Decimal("500"),
        ),
        "ETH": DerivativePosition(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            symbol="ETH",
            size=Decimal("-10"),
            entry_price=Decimal("3000"),
            mark_price=Decimal("2950"),
            side=OrderSide.SELL,
            unrealized_pnl=Decimal("500"),
        ),
    }

    mock_api.get_ticker.return_value = Ticker(
        symbol="BTC",
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


@pytest.fixture
def circuit_breaker_system(mock_config: Config) -> CircuitBreakerSystem:
    """Create a CircuitBreakerSystem instance using mock config."""
    system = CircuitBreakerSystem(mock_config)
    return system


@pytest.fixture
def mock_portfolio_tracker() -> MagicMock:
    """Create a mock PortfolioTracker for testing."""
    mock_tracker = MagicMock()

    # Configure mock methods
    mock_tracker.get_total_capital.return_value = 20000.0
    mock_tracker.get_exchange_balance.return_value = 10000.0
    mock_tracker.get_exchange_exposure.return_value = 5000.0
    mock_tracker.get_total_exposure.return_value = 10000.0

    return mock_tracker


@pytest.fixture
def mock_data_handler() -> MagicMock:
    """Create a mock DataHandler for testing."""
    mock_handler = MagicMock()
    now = datetime.now(UTC)

    # Configure mock methods
    mock_handler.get_ticker.return_value = Ticker(
        symbol="BTC",
        bid=Decimal("40000.0"),
        ask=Decimal("40002.0"),
        price=Decimal("40001.0"),
        timestamp=now,
    )

    # Funding rate from handler returns tuple (rate, timestamp)
    mock_handler.get_funding_rate.return_value = (Decimal("0.0001"), now)

    return mock_handler


@pytest.fixture
def mock_arbitrage_opportunity() -> MagicMock:
    """Create a mock ArbitrageOpportunity for testing."""
    opportunity = MagicMock(spec=ArbitrageOpportunity)
    opportunity.symbol = "BTC"
    opportunity.long_exchange = "hyperliquid"
    opportunity.short_exchange = "backpack"
    opportunity.net_funding_differential = 0.05  # 5 basis points
    opportunity.basis_volatility = 0.01
    opportunity.expected_profit = 10.0
    opportunity.confidence = 0.8
    opportunity.timestamp = datetime.now(UTC)
    opportunity.long_price = Decimal("30000")
    opportunity.short_price = Decimal("29999")
    opportunity.long_size = Decimal("0.1")
    opportunity.short_size = Decimal("0.1")

    return opportunity


def _deep_get(d: dict[str, Any], keys: str, default: Any | None = None) -> Any | None:  # noqa: ANN401 - deep_get can return Any
    """Helper to get nested dictionary values."""
    keys_list = keys.split(".")
    value: Any = d
    for k in keys_list:
        if isinstance(value, dict):
            value = value.get(k, default)
            if value is default and k != keys_list[-1]:  # Key not found before the last part
                return default
        else:  # value is not a dict, cannot go deeper
            return default
    return value


@pytest.fixture
def mock_secrets_manager_with_missing() -> MagicMock:
    """Fixture for SecretsManager where some keys are missing."""
    manager = MagicMock(spec=SecretsManager)

    # Configure get method to return None for specific keys
    # Simulate missing optional keys
    def mock_get(
        key: str,
        default: Any = None,  # noqa: ANN401 - default can be Any for this flexible mock
        *,
        _deep_get: bool = False,
        getter: Callable[..., Any] | None = None,  # Changed Any to Callable[..., Any] | None
    ) -> Any:  # noqa: ANN401 - mock_get can return Any
        if key == "OPTIONAL_SETTING":
            return None
        elif key == "REQUIRED_DB_PASSWORD":
            return "fake_password"  # Assume this one exists
        elif _deep_get:  # Simulate deep_get if needed for structure
            return default
        else:
            return default

    manager.get.side_effect = mock_get
    return manager


# --- Async Mocking Helpers ---


def mock_get_config() -> dict[str, Any]:  # noqa: ANN401 - Test fixture returns dict
    """Fixture to provide a mock configuration dictionary."""
    return {
        "exchanges": {
            "hyperliquid": {
                "enabled": True,
                "symbols": {"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
                "websocket": {
                    "reconnect_delay": 1,
                    "max_reconnect_delay": 5,
                    "ping_interval": 10,
                },
                "risk_modifier": 0.9,
            },
            "backpack": {
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
                    "hyperliquid": {
                        "enabled": True,
                        "api_errors": {
                            "enabled": True,
                            "type": "api_error",
                            "threshold": 3,
                            "window_seconds": 60,
                            "cooldown_seconds": 300,
                        },
                        "drawdown": {"enabled": False},
                        "volatility": {"enabled": False},
                        "liquidity": {"enabled": False},
                    },
                    "backpack": {
                        "enabled": True,
                        "api_errors": {
                            "enabled": True,
                            "type": "api_error",
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


def create_mock_response(
    status: int = 200,
    json_data: Any | None = None,  # noqa: ANN401 - JSON data can be Any for tests
    text_data: str | None = None,
    headers: dict[str, str] | None = None,
) -> MockResponse:
    # Simplified mock logic
    mock_resp = MockResponse(json_data, status, headers, "application/json")
    # Assign AsyncMock instances directly to the attributes
    mock_resp.text = AsyncMock(return_value=text_data if text_data is not None else "")
    mock_resp.raise_for_status = MagicMock()  # Assign MagicMock to the attribute
    if status >= 400:
        # Configure the mock to raise if needed
        mock_resp.raise_for_status.side_effect = aiohttp.ClientResponseError(
            MagicMock(), (), status=status
        )
    return mock_resp


async def mock_request(
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,  # noqa: ANN401 - Params can be Any for mock
    data: Any | None = None,  # noqa: ANN401 - Data can be Any for mock
    json: Any | None = None,  # noqa: ANN401 - Json can be Any for mock
    headers: dict[str, Any] | None = None,  # noqa: ANN401 - Headers can be Any for mock
    status_code: int = 200,  # Added status_code parameter
    **kwargs: Any,  # noqa: ANN401 - Allow Any extra kwargs for flexibility
) -> MockResponse:
    # Simplified mock logic
    text_data = str(json) if json else ""  # Define text_data based on json
    mock_resp = MockResponse(json, status_code, headers, "application/json")  # Use status_code
    # Assign AsyncMock instances directly to the attributes
    mock_resp.text = AsyncMock(return_value=text_data)
    mock_resp.raise_for_status = MagicMock()  # Assign MagicMock to the attribute
    if status_code >= 400:  # Use status_code
        # Configure the mock to raise if needed
        mock_resp.raise_for_status.side_effect = aiohttp.ClientResponseError(
            MagicMock(), (), status=status_code
        )
    return mock_resp
