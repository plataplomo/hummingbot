"""Test configuration and fixtures for CyberDeltaEngine test suite.

This module provides pytest fixtures and configuration for testing the CyberDelta trading engine.
"""

from __future__ import annotations  # Enable postponed evaluation

import os
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import TracebackType  # Import TracebackType
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from pydantic import AnyUrl, HttpUrl

from cyberdelta.config.config_manager import ConfigManager, ConfigurationError
from cyberdelta.config.config_models import (
    AppSettings,
    BalanceMonitoringSettings,
    CircuitBreakerSettings,
    ExchangeSpecificConfig,
    ExecutionCompensationSettings,
    ExecutionSettings,
    GeneralSettings,
    GlobalRiskSettings,
    MonitoringSettings,
    PortfolioTrackerConfig,
    PositionReconciliationSettings,
    RiskSettings,
    SafetySystemsSettings,
    StrategiesSettings,
    StrategyConfigHLPerpBPSpot,
    StrategyParamsHLPerpBPSpot,
)
from cyberdelta.config.secrets_manager import SecretsManager
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets, SecretsConfig
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
from cyberdelta.enums.exchange_names import ExchangeName
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
    """Mock aiohttp response for testing API clients."""

    def __init__(
        self,
        data: object,  # Test data can be any JSON-serializable object
        status: int = 200,
        headers: dict[str, str] | None = None,
        content_type: str = "application/json",
        text_data: str | None = None,  # Added for direct initialization
    ) -> None:
        """Initialize mock response with test data and status.

        Args:
            data: JSON-serializable test data
            status: HTTP status code (default: 200)
            headers: HTTP response headers
            content_type: Response content type
            text_data: Raw text data for response

        """
        self._data = data
        self.status = status
        self.headers = headers if headers is not None else {}  # Ensure headers is a dict
        self.content_type = content_type
        self._raise_for_status_called = False

        # Attributes for mocking
        effective_text_data = text_data if text_data is not None else str(self._data)
        self.text: AsyncMock = AsyncMock(return_value=effective_text_data)
        self.raise_for_status: MagicMock = MagicMock()
        if self.status >= 400:
            # aiohttp's ClientResponseError headers expect a MultiMapping or None.
            # For simplicity in mock, we pass our dict; aiohttp might handle basic dicts.
            # Or, pass `None` if type issues persist: `headers=None`
            minimal_request_info = MagicMock()
            minimal_request_info.url = "mock://url"
            minimal_request_info.method = "GET"
            minimal_request_info.headers = self.headers
            minimal_request_info.real_url = "mock://real_url"

            self.raise_for_status.side_effect = aiohttp.ClientResponseError(
                request_info=minimal_request_info,
                history=(),
                status=self.status,
                message="Mock ResponseError",
                headers=cast("Any", self.headers),
            )

    async def json(self) -> object:  # JSON data can be any serializable object
        """Return JSON data from response."""
        return self._data

    async def __aenter__(self) -> MockResponse:
        """Enter async context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context manager."""
        pass


class MockClientSession:
    """Mock aiohttp ClientSession for testing HTTP clients."""

    def __init__(
        self,
        responses: dict[tuple[str, str], MockResponse] | None = None,
    ) -> None:
        """Initialize mock session with predefined responses.

        Args:
            responses: Mapping of (method, url) tuples to mock responses

        """
        self.responses = responses or {}
        self.requests: list[dict[str, Any]] = []  # Flexible for test requests
        self.closed = False

    async def __aenter__(self) -> MockClientSession:
        """Enter async context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context manager."""
        pass

    async def close(self) -> None:
        """Close the session."""
        self.closed = True

    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: dict[str, Any],
    ) -> MockResponse:  # Accepts any kwargs
        self.requests.append({"method": method, "url": url, "kwargs": kwargs})

        # Find match in responses
        for pattern, response in self.responses.items():
            if (method, url) == pattern or (
                (method, pattern[1]) == pattern and url.startswith(pattern[1])
            ):
                return response

        # Default response if no match
        return MockResponse({}, status=404)

    async def get(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:  # Accepts any kwargs
        """Execute GET request."""
        return await self._request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:  # Accepts any kwargs
        """Execute POST request."""
        return await self._request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:  # Accepts any kwargs
        """Execute PUT request."""
        return await self._request("PUT", url, **kwargs)

    async def delete(
        self,
        url: str,
        **kwargs: dict[str, Any],
    ) -> MockResponse:  # Accepts any kwargs
        """Execute DELETE request."""
        return await self._request("DELETE", url, **kwargs)


@pytest.fixture
def mock_client_session() -> Callable[
    [dict[tuple[str, str], MockResponse] | None],
    MockClientSession,
]:
    """Fixture to provide a mock aiohttp ClientSession."""

    def create_session(
        responses: dict[tuple[str, str], MockResponse] | None = None,
    ) -> MockClientSession:
        """Create session for testing."""
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
def mock_config() -> AppSettings:
    """Create a mock AppSettings object with test settings."""
    # Create a test AppSettings instance
    return AppSettings(
        general=GeneralSettings(
            log_level="INFO",
            log_file="logs/test.log",
            module_log_levels={},
            safe_mode=True,
            state_file="data/test_state.json",
            state_backup_directory="data/test_backups",
            state_save_interval=300,
            state_backup_count=5,
        ),
        exchanges={
            "hyperliquid": ExchangeSpecificConfig(
                exchange_name=ExchangeName.HYPERLIQUID,
                enabled=True,
                api_base_url_mainnet=HttpUrl("https://api.hyperliquid.xyz"),
                ws_url_mainnet=AnyUrl("wss://api.hyperliquid.xyz/ws"),
                api_base_url_testnet=HttpUrl("https://api.hyperliquid-testnet.xyz"),
                ws_url_testnet=AnyUrl("wss://api.hyperliquid-testnet.xyz/ws"),
                is_mainnet_environment=False,  # Default to testnet for testing
                chain_id=1337,
                rate_limit_per_minute=120,
                symbols={"BTC": "BTC", "ETH": "ETH"},
            ),
            "backpack": ExchangeSpecificConfig(
                exchange_name=ExchangeName.BACKPACK,
                enabled=True,
                api_base_url_mainnet=HttpUrl("https://api.backpack.exchange"),
                ws_url_mainnet=AnyUrl("wss://ws.backpack.exchange"),
                is_mainnet_environment=True,  # Backpack only has mainnet
                rate_limit_per_minute=120,
                symbols={"BTC": "BTC_USDC", "ETH": "ETH_USDC"},
            ),
        },
        strategies=StrategiesSettings(
            hl_perp_bp_spot=StrategyConfigHLPerpBPSpot(
                enabled=True,
                long_exchange="backpack",
                short_exchange="hyperliquid",
                symbol_long="BTC",
                symbol_short="BTC",
                params=StrategyParamsHLPerpBPSpot(
                    funding_threshold=Decimal("0.0001"),
                    max_price_spread_pct=Decimal("0.002"),
                    min_profit_usd=Decimal("1.0"),
                ),
            ),
        ),
        risk=RiskSettings(
            **{
                "global": GlobalRiskSettings(
                    max_position_usd=Decimal("200.0"),
                    max_total_exposure_usd=Decimal("1000.0"),
                ),
            },
            use_simple_sizing_path=True,
            simple_sizing_method="fixed_fraction",
            simple_fixed_fraction=Decimal("0.1"),
            simple_fixed_usd_size=Decimal("10.0"),
        ),
        execution=ExecutionSettings(
            max_slippage_pct=Decimal("0.001"),
            max_retries=3,
            retry_delay_base_sec=Decimal("1.0"),
            settlement_delay=Decimal("2.0"),
            compensation=ExecutionCompensationSettings(
                use_limit_orders=True,
                limit_price_offset_pct=Decimal("0.05"),
            ),
        ),
        safety_systems=SafetySystemsSettings(
            circuit_breakers=CircuitBreakerSettings(
                enabled=True,
                global_consecutive_failures=5,
                global_reset_timeout_sec=300,
                exchange_consecutive_failures=3,
                exchange_reset_timeout_sec=180,
            ),
            position_reconciliation=PositionReconciliationSettings(
                enabled=True,
                check_interval_sec=600,
                max_discrepancy_pct=Decimal("0.01"),
            ),
            balance_monitoring=BalanceMonitoringSettings(
                enabled=True,
                check_interval_sec=300,
                min_balance_thresholds_usd={
                    "hyperliquid": Decimal("100.0"),
                    "backpack": Decimal("100.0"),
                },
            ),
        ),
        monitoring=MonitoringSettings(
            notifications_enabled=True,
            alert_methods=["log"],
        ),
        portfolio_tracker=PortfolioTrackerConfig(
            data_freshness_seconds=60,
            initial_balances={},
            initial_positions=[],
        ),
    )


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
def circuit_breaker_system(mock_config: AppSettings) -> CircuitBreakerSystem:
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


@pytest.fixture
def mock_secrets_manager_with_missing() -> MagicMock:
    """Fixture for SecretsManager where some keys are missing."""
    manager = MagicMock(spec=SecretsManager)

    # Configure get method to return None for specific keys
    # Simulate missing optional keys
    def mock_get(
        key: str,
        default: object = None,
        *,
        _deep_get: bool = False,
        getter: Callable[..., object] | None = None,
    ) -> object:
        """Return mock get for testing."""
        # Simulate missing keys for testing
        missing_keys = ["optional_key_1", "optional_key_2"]
        if key in missing_keys:
            return default
        return f"mock_value_for_{key}"

    manager.get.side_effect = mock_get
    return manager


# --- Environment-Aware Hyperliquid Test Fixtures ---


@pytest.fixture(scope="session")
def hl_test_environment() -> str:
    """Fixture to determine Hyperliquid test environment.

    Defaults to 'testnet' but can be overridden with CYBERDELTA_TEST_ENV_HL environment variable.
    """
    return os.environ.get("CYBERDELTA_TEST_ENV_HL", "testnet")


@pytest.fixture(scope="session")
def active_hl_config(hl_test_environment: str) -> ExchangeSpecificConfig:
    """Environment-aware ExchangeSpecificConfig fixture for Hyperliquid.

    Configures the exchange for mainnet or testnet based on hl_test_environment.
    Always includes both mainnet and testnet URLs.
    """
    is_mainnet_env_flag = hl_test_environment == "mainnet"

    return ExchangeSpecificConfig.model_validate(
        {
            "exchange_name": ExchangeName.HYPERLIQUID,
            "api_base_url_mainnet": "https://api.hyperliquid.xyz",
            "ws_url_mainnet": "wss://api.hyperliquid.xyz/ws",
            "api_base_url_testnet": "https://api.hyperliquid-testnet.xyz",
            "ws_url_testnet": "wss://api.hyperliquid-testnet.xyz/ws",
            "is_mainnet_environment": is_mainnet_env_flag,
            "chain_id": 1337,
            "rate_limit_per_minute": 300,
            "symbols": {"BTC": "BTC", "ETH": "ETH"},
            # Hyperliquid-specific rate limiting configuration
            "ip_weight_limit_per_minute": 1200,
            "info_request_type_ip_weights": {
                "l2Book": 2,
                "allMids": 2,
                "meta": 2,
                "userRole": 60,
                "clearinghouseState": 10,
                "openOrders": 1,
            },
            "default_info_weight": 20,
            "exchange_action_base_ip_weight": 1,
            "address_action_safety_net": {"rate_per_minute": 300},
            "websocket_send_rate_per_minute": 1800,
        },
    )


@pytest.fixture(scope="session")
def active_hl_secrets() -> PrivateKeyAuthSecrets:
    """Environment-aware PrivateKeyAuthSecrets fixture for Hyperliquid.

    Uses environment variables if available, otherwise provides test placeholders.
    Supports both dedicated testnet credentials and main credentials.
    """
    # Main private key (always required)
    main_private_key = os.environ.get(
        "HL_PRIVATE_KEY",
        "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
    )

    # Optional testnet-specific private key
    testnet_private_key = os.environ.get("HL_TESTNET_PRIVATE_KEY")

    # Optional testnet seed passphrase
    testnet_seed = os.environ.get("HL_TESTNET_SEED_PASSPHRASE")

    # Optional passphrase for main key encryption
    passphrase = os.environ.get("HL_PASSPHRASE")

    return PrivateKeyAuthSecrets.model_validate(
        {
            "private_key": main_private_key,
            "passphrase": passphrase,
            "private_key_testnet": testnet_private_key,
            "testnet_seed_passphrase": testnet_seed,
        },
    )


# --- Async Mocking Helpers ---


def mock_get_config() -> dict[str, Any]:
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
            "reconciliation_interval": 300,  # 5 minutes
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
                    },
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
    json_data: object | None = None,  # JSON data can be any serializable object
    text_data: str | None = None,
    headers: dict[str, str] | None = None,
) -> MockResponse:
    """Create mock response for testing."""
    mock_resp = MockResponse(
        json_data,
        status,
        headers,
        "application/json",
        text_data=text_data if text_data is not None else str(json_data),
    )
    # Attributes are now set in MockResponse.__init__
    if status >= 400 and mock_resp.raise_for_status.side_effect is None:
        minimal_request_info = MagicMock()
        minimal_request_info.url = "mock://url"
        minimal_request_info.method = "GET"
        minimal_request_info.headers = headers
        minimal_request_info.real_url = "mock://real_url"
        mock_resp.raise_for_status.side_effect = aiohttp.ClientResponseError(
            minimal_request_info,
            (),
            status=status,
            message="Mock Response Error",
            headers=cast("Any", headers),
        )
    return mock_resp


async def mock_request(
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    data: object | None = None,  # Request data can be any serializable object
    json: object | None = None,  # JSON data can be any serializable object
    headers: dict[str, Any] | None = None,
    status_code: int = 200,
    **kwargs: object,  # Additional kwargs for flexibility
) -> MockResponse:
    """Create mock HTTP request for testing."""
    text_data = str(json) if json else ""
    actual_headers = headers if headers else {}
    mock_resp = MockResponse(
        json,
        status_code,
        actual_headers,
        "application/json",
        text_data=text_data,
    )
    # Attributes are now set in MockResponse.__init__
    if status_code >= 400 and mock_resp.raise_for_status.side_effect is None:
        minimal_request_info = MagicMock()
        minimal_request_info.url = "mock://url"
        minimal_request_info.method = "GET"
        minimal_request_info.headers = actual_headers
        minimal_request_info.real_url = "mock://real_url"
        mock_resp.raise_for_status.side_effect = aiohttp.ClientResponseError(
            minimal_request_info,
            (),
            status=status_code,
            message="Mock Response Error",
            headers=cast("Any", actual_headers),
        )
    return mock_resp


# --- New Test Configuration Fixtures ---


@pytest.fixture(scope="session")
def test_config_file_path() -> Path:
    """Path to the test configuration file."""
    # Assumes test_config.yaml is in tests/config/ relative to project root
    return Path(__file__).parent / "config" / "test_config.yaml"


@pytest.fixture(scope="session")
def test_secrets_file_path() -> Path:
    """Path to the test secrets file."""
    return Path(__file__).parent / "config" / "test_secrets.yaml"


@pytest.fixture(scope="session")
def test_app_settings(test_config_file_path: Path) -> AppSettings:
    """Load test-specific AppSettings from test_config.yaml."""
    if not test_config_file_path.exists():
        pytest.skip(
            f"Test config file not found at {test_config_file_path}, skipping tests that need it.",
        )
    try:
        manager = ConfigManager(str(test_config_file_path))
        if manager.settings is None:  # Should be caught by ConfigManager raising ConfigurationError
            raise ConfigurationError("ConfigManager loaded but settings are None.")
        return manager.settings
    except ConfigurationError as e:
        pytest.fail(f"Failed to load test AppSettings from {test_config_file_path}: {e}")
    # Add a default return to satisfy linters, though pytest.fail should exit
    # This path should ideally not be reached if pytest.fail works as expected.
    raise RuntimeError("test_app_settings fixture failed unexpectedly.")


@pytest.fixture(scope="session")
def test_secrets_config(test_secrets_file_path: Path) -> SecretsConfig:
    """Load test-specific SecretsConfig from test_secrets.yaml."""
    if not test_secrets_file_path.exists():
        pytest.skip(
            f"Test secrets file not found at {test_secrets_file_path}, "
            "skipping tests that need it.",
        )
    try:
        manager = SecretsManager(str(test_secrets_file_path))
        if (
            manager.secrets_data is None
        ):  # Should be caught by SecretsManager raising ConfigurationError
            raise ConfigurationError("SecretsManager loaded but secrets_data is None.")
        return manager.secrets_data
    except ConfigurationError as e:
        pytest.fail(f"Failed to load test SecretsConfig from {test_secrets_file_path}: {e}")
    # Add a default return to satisfy linters
    raise RuntimeError("test_secrets_config fixture failed unexpectedly.")


@pytest.fixture(scope="session")
def hl_test_environment_from_config(test_app_settings: AppSettings) -> str:
    """Get the default Hyperliquid test environment from test_config.yaml.

    Can be overridden with CYBERDELTA_TEST_ENV_HL environment variable.
    """
    hl_config = test_app_settings.exchanges.get("hyperliquid")
    is_mainnet_from_config = False  # Default to testnet
    if hl_config and hasattr(hl_config, "is_mainnet_environment"):
        is_mainnet_from_config = hl_config.is_mainnet_environment

    # Allow override via environment variable
    env_override = os.environ.get("CYBERDELTA_TEST_ENV_HL")
    if env_override:
        return env_override.lower()
    return "mainnet" if is_mainnet_from_config else "testnet"


# --- VCR Configuration for pytest-recording ---


@pytest.fixture(scope="module")
def vcr_config() -> dict[str, Any]:
    """VCR.py configuration for pytest-recording cassette-based integration testing.

    Provides comprehensive configuration for recording and playing back HTTP interactions,
    with robust filtering for sensitive data including authentication tokens, signatures,
    timestamps, and personal information.
    """

    def filter_request_body(request: object) -> object:
        """Filter and sanitize request body content for VCR cassette recording."""
        if hasattr(request, "body") and getattr(request, "body", None):
            # Filter known sensitive patterns in request bodies
            request_body = request.body
            body_str = (
                request_body.decode("utf-8")
                if isinstance(request_body, bytes)
                else str(request_body)
            )

            # Replace common sensitive patterns
            import re

            # Filter private keys (hex strings that look like private keys)
            body_str = re.sub(
                r'"private_key":\s*"0x[a-fA-F0-9]{64}"',
                '"private_key": "FILTERED_PRIVATE_KEY"',
                body_str,
            )
            # Filter API keys
            body_str = re.sub(r'"api_key":\s*"[^"]*"', '"api_key": "FILTERED_API_KEY"', body_str)
            # Filter signatures
            body_str = re.sub(
                r'"signature":\s*"[^"]*"',
                '"signature": "FILTERED_SIGNATURE"',
                body_str,
            )
            # Filter timestamps to make tests more deterministic
            body_str = re.sub(r'"timestamp":\s*\d+', '"timestamp": 1234567890', body_str)

            request.body = body_str.encode("utf-8") if isinstance(request_body, bytes) else body_str
        return request

    def filter_response_body(response: object) -> object:
        """Filter and sanitize response body content for VCR cassette recording."""
        if hasattr(response, "body") and getattr(response, "body", None):
            # For now, we don't filter response bodies as they typically don't contain
            # user credentials, but this hook is available for future use
            pass
        return response

    return {
        "filter_headers": [
            # ===== GLOBAL HEADERS =====
            # Standard authentication headers
            ("Authorization", "FILTERED_AUTHORIZATION_HEADER"),
            ("Bearer", "FILTERED_BEARER_TOKEN"),
            ("Cookie", "FILTERED_COOKIE"),
            ("Set-Cookie", "FILTERED_SET_COOKIE"),
            # API key headers (various formats)
            ("X-API-Key", "FILTERED_API_KEY"),
            ("X-Api-Key", "FILTERED_API_KEY"),  # Case variation
            ("API-Key", "FILTERED_API_KEY"),
            ("Api-Key", "FILTERED_API_KEY"),
            ("X-Auth-Token", "FILTERED_AUTH_TOKEN"),
            ("X-Access-Token", "FILTERED_ACCESS_TOKEN"),
            # Signature headers (for HMAC-based auth)
            ("X-Signature", "FILTERED_SIGNATURE"),
            ("X-Sig", "FILTERED_SIGNATURE"),
            ("Signature", "FILTERED_SIGNATURE"),
            # Timestamp headers (for replay protection)
            ("X-Timestamp", "FILTERED_TIMESTAMP"),
            ("X-Time", "FILTERED_TIMESTAMP"),
            ("Timestamp", "FILTERED_TIMESTAMP"),
            # Window headers (for time-based auth)
            ("X-Window", "FILTERED_WINDOW"),
            ("X-Time-Window", "FILTERED_WINDOW"),
            # User agent (normalize for consistency)
            ("User-Agent", "CyberDeltaEngine-Test-Suite/1.0"),
            # ===== EXCHANGE-SPECIFIC HEADERS =====
            # Backpack Exchange headers
            ("X-BP-API-Key", "FILTERED_BACKPACK_API_KEY"),
            ("X-BP-Signature", "FILTERED_BACKPACK_SIGNATURE"),
            ("X-BP-Timestamp", "FILTERED_BACKPACK_TIMESTAMP"),
            # Hyperliquid Exchange headers
            ("X-HL-Agent", "FILTERED_HYPERLIQUID_AGENT"),
            ("X-HL-Signature", "FILTERED_HYPERLIQUID_SIGNATURE"),
            # Common exchange headers that might contain sensitive data
            ("X-Nonce", "FILTERED_NONCE"),
            ("X-Request-Id", "FILTERED_REQUEST_ID"),
            ("X-Client-Id", "FILTERED_CLIENT_ID"),
            # Session and tracking headers
            ("X-Session-Id", "FILTERED_SESSION_ID"),
            ("X-Trace-Id", "FILTERED_TRACE_ID"),
            ("X-Correlation-Id", "FILTERED_CORRELATION_ID"),
        ],
        "filter_query_parameters": [
            # ===== AUTHENTICATION PARAMETERS =====
            ("api_key", "FILTERED_QUERY_API_KEY"),
            ("apikey", "FILTERED_QUERY_API_KEY"),
            ("key", "FILTERED_QUERY_KEY"),
            ("token", "FILTERED_QUERY_TOKEN"),
            ("auth", "FILTERED_QUERY_AUTH"),
            ("authorization", "FILTERED_QUERY_AUTHORIZATION"),
            # ===== SIGNATURE PARAMETERS =====
            ("signature", "FILTERED_QUERY_SIGNATURE"),
            ("sig", "FILTERED_QUERY_SIGNATURE"),
            ("sign", "FILTERED_QUERY_SIGNATURE"),
            ("hmac", "FILTERED_QUERY_HMAC"),
            # ===== TIMESTAMP PARAMETERS =====
            ("timestamp", "FILTERED_QUERY_TIMESTAMP"),
            ("ts", "FILTERED_QUERY_TIMESTAMP"),
            ("time", "FILTERED_QUERY_TIMESTAMP"),
            ("nonce", "FILTERED_QUERY_NONCE"),
            # ===== SESSION PARAMETERS =====
            ("session", "FILTERED_QUERY_SESSION"),
            ("session_id", "FILTERED_QUERY_SESSION_ID"),
            ("request_id", "FILTERED_QUERY_REQUEST_ID"),
            # ===== USER IDENTIFICATION =====
            ("user_id", "FILTERED_QUERY_USER_ID"),
            ("client_id", "FILTERED_QUERY_CLIENT_ID"),
            ("wallet", "FILTERED_QUERY_WALLET"),
            ("address", "FILTERED_QUERY_ADDRESS"),
        ],
        "filter_post_data_parameters": [
            # ===== POST BODY PARAMETERS =====
            # Same patterns as query parameters but for POST body
            ("api_key", "FILTERED_POST_API_KEY"),
            ("signature", "FILTERED_POST_SIGNATURE"),
            ("timestamp", "FILTERED_POST_TIMESTAMP"),
            ("private_key", "FILTERED_POST_PRIVATE_KEY"),
            ("secret", "FILTERED_POST_SECRET"),
            ("password", "FILTERED_POST_PASSWORD"),
            ("passphrase", "FILTERED_POST_PASSPHRASE"),
            ("mnemonic", "FILTERED_POST_MNEMONIC"),
            ("seed", "FILTERED_POST_SEED"),
        ],
        # ===== CUSTOM FILTERS =====
        "before_record_request": filter_request_body,
        "before_record_response": filter_response_body,
        # ===== MATCHING CONFIGURATION =====
        # Match on method, URI components, but NOT on filtered query params
        "match_on": ["method", "scheme", "host", "port", "path"],
        # ===== CASSETTE CONFIGURATION =====
        "cassette_library_dir": "tests/cassettes",
        # Record mode can be controlled via environment variable
        # - 'once': Record if cassette doesn't exist, otherwise replay (default)
        # - 'new_episodes': Record new interactions, replay existing ones
        # - 'all': Always record (overwrite cassettes)
        # - 'none': Never record, only replay (fail if cassette missing)
        "record_mode": os.environ.get("VCR_RECORD_MODE", "once"),
        # ===== RESPONSE PROCESSING =====
        "decode_compressed_response": True,  # Handle gzipped responses
        # ===== SECURITY OPTIONS =====
        # Ignore certain hosts that shouldn't be recorded (if any)
        "ignore_hosts": [],
        # Ignore localhost/development endpoints that might contain secrets
        "ignore_localhost": True,
    }
