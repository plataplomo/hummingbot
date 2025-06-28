"""Module docstring."""

import types
from collections.abc import Callable
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest
from pydantic import AnyUrl, HttpUrl, SecretStr
from web3.auto import w3  # Import w3

from cyberdelta.config import AppSettings
from cyberdelta.config.models.config_models import (
    AddressActionSafetyNetConfig,
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
)
from cyberdelta.config.models.funding_strategy_models import (
    StrategiesSettings,
    StrategyConfigHLPerpBPSpot,
    StrategyParamsHLPerpBPSpot,
)
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName


logger = get_logger(__name__)


def create_test_http_url(url_str: str) -> HttpUrl:
    """Create an HTTP URL for tests.

    Returns:
        HttpUrl: HttpUrl instance for testing.
    """
    return HttpUrl(url_str)


def create_test_any_url(url_str: str) -> AnyUrl:
    """Create an Any URL for tests.

    Returns:
        AnyUrl: AnyUrl instance for testing.
    """
    return AnyUrl(url_str)


# Mock aiohttp ClientSession and Response for API testing
class MockResponse:
    """Mock implementation of aiohttp ClientResponse for testing."""

    def __init__(
        self,
        data: dict[str, Any] | list[Any] | str,  # More specific than Any
        status: int = 200,
        headers: dict[str, str] | None = None,
        content_type: str = "application/json",
    ) -> None:
        """Initialize MockResponse with test data and HTTP status."""
        self._data = data
        self.status = status
        self.headers = headers or {}
        self.content_type = content_type
        self._raise_for_status_called = False

    async def json(self) -> dict[str, Any] | list[Any] | str:  # Match data type hint
        """Return the mock response data as JSON."""
        return self._data

    async def text(self) -> str:
        """Return the mock response data as text."""
        return str(self._data)

    async def __aenter__(self) -> "MockResponse":
        """Enter async context manager.

        Returns:
            Self for use as async context manager.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Exit async context manager."""

    def raise_for_status(self) -> None:  # Add return type hint
        """Raise an exception for HTTP error status codes.

        Raises:
            ClientResponseError: When HTTP status code is 400 or higher.
        """
        self._raise_for_status_called = True
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=MagicMock(),
                history=(),
                status=self.status,
            )


class MockClientSession:
    """Mock implementation of aiohttp ClientSession for testing."""

    def __init__(self, responses: dict[tuple[str, str], MockResponse] | None = None) -> None:
        """Initialize MockClientSession with optional response mappings."""
        self.responses = responses or {}
        self.requests: list[dict[str, Any]] = []
        self.closed = False

    async def __aenter__(self) -> "MockClientSession":
        """Enter async context manager.

        Returns:
            Self for use as async context manager.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        """Exit async context manager."""

    async def close(self) -> None:
        """Close the mock session."""
        self.closed = True

    async def _request(self, method: str, url: str, **kwargs: dict[str, Any]) -> MockResponse:
        """Execute an HTTP request and return a mock response.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            url: Request URL
            **kwargs: Additional request parameters

        Returns:
            MockResponse for the request or a 404 response if no match found.
        """
        self.requests.append({"method": method, "url": url, "kwargs": kwargs})

        # Find match in responses
        for pattern, response in (self.responses or {}).items():
            if (method, url) == pattern or (
                (method, pattern[1]) == pattern and url.startswith(pattern[1])
            ):
                return response

        # Default response if no match
        return MockResponse({}, status=404)

    async def get(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:
        """Send a GET request to the specified URL.

        Returns:
            MockResponse with the result of the GET request.
        """
        return await self._request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:
        """Send a POST request to the specified URL.

        Returns:
            MockResponse with the result of the POST request.
        """
        return await self._request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:
        """Send a PUT request to the specified URL.

        Returns:
            MockResponse with the result of the PUT request.
        """
        return await self._request("PUT", url, **kwargs)

    async def delete(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:
        """Send a DELETE request to the specified URL.

        Returns:
            MockResponse with the result of the DELETE request.
        """
        return await self._request("DELETE", url, **kwargs)


@pytest.fixture
def mock_client_session() -> Callable[..., MockClientSession]:
    """Fixture to provide a mock aiohttp ClientSession.

    Returns:
        Factory function that creates MockClientSession instances.
    """

    def create_session(
        responses: dict[tuple[str, str], MockResponse] | None = None,
    ) -> MockClientSession:
        """Create session for testing.

        Returns:
            MockClientSession: MockClientSession instance for testing.
        """
        return MockClientSession(responses)

    return create_session


@pytest.fixture
def hyperliquid_config() -> dict[str, Any]:
    """Fixture to provide Hyperliquid API configuration.

    Returns:
        Dictionary containing Hyperliquid API configuration settings.
    """
    return {
        "base_url": "https://api.hyperliquid.xyz",
        "ws_endpoint": "wss://api.hyperliquid.xyz/ws",
        "rate_limits": {
            "default_rate": 10.0,
            "default_bucket_size": 50,
            "endpoints": {"POST:/user": {"rate": 5.0, "bucket_size": 20}},
        },
    }


@pytest.fixture
def backpack_config() -> dict[str, Any]:
    """Fixture to provide Backpack API configuration.

    Returns:
        Dictionary containing Backpack API configuration settings.
    """
    return {
        "base_url": "https://api.backpack.exchange",
        "ws_endpoint": "wss://ws.backpack.exchange",
        "rate_limits": {
            "default_rate": 10.0,
            "default_bucket_size": 50,
            "endpoints": {"GET:/api/v1/depth": {"rate": 5.0, "bucket_size": 20}},
        },
    }


@pytest.fixture
def hyperliquid_secrets() -> dict[str, str]:
    """Fixture to provide Hyperliquid API secrets with a VALID derived address.

    Returns:
        Dictionary containing Hyperliquid API secrets with valid derived address.
    """
    # Use a fixed dummy private key for reproducibility in tests
    dummy_private_key = "0x1111111111111111111111111111111111111111111111111111111111111111"
    try:
        account = w3.eth.account.from_key(dummy_private_key)
        derived_address = account.address
    except Exception as e:
        # Fallback if w3 or account generation fails unexpectedly
        logger.warning(
            "hyperliquid_mock_account_generation_failed",
            action="generate_mock_account",
            error=str(e),
            message=f"Error generating Hyperliquid mock account: {e}",
        )
        derived_address = "0xMockAddressCreationFailed"  # Provide a fallback

    return {
        "private_key": dummy_private_key,
        "wallet_address": derived_address,
    }


@pytest.fixture
def backpack_secrets() -> dict[str, str | None]:
    """Provide default secrets for BackpackAPI testing.

    Returns:
        Dictionary containing Backpack API secrets for testing.
    """
    return {
        "BACKPACK_API_KEY": "test_api_key",
        "BACKPACK_API_SECRET": "test_api_secret",
    }


@pytest.fixture
def mock_config() -> Callable[..., AppSettings]:
    """Fixture to create an AppSettings object with the provided data dictionary.

    Returns:
        Factory function that creates AppSettings instances for testing.
    """

    def _create_config(config_data: dict[str, Any] | None = None) -> AppSettings:
        """Create AppSettings for testing.

        Returns:
            AppSettings: Configured AppSettings instance for testing.
        """
        # For now, return a basic AppSettings instance
        # This is a simplified version for unit tests
        return AppSettings(
            general=GeneralSettings(
                log_level="INFO",
                safe_mode=True,
                state_file="data/test_state.json",
                state_backup_directory="data/test_backups",
                state_save_interval=300,
                state_backup_count=5,
            ),
            exchanges={
                "hyperliquid": ExchangeSpecificConfig.model_validate({
                    "exchange_name": ExchangeName.HYPERLIQUID,
                    "enabled": True,
                    "api_base_url_mainnet": "https://api.hyperliquid.xyz",
                    "ws_url_mainnet": "wss://api.hyperliquid.xyz/ws",
                    "rate_limit_per_minute": 120,
                    "symbols": {"BTC": "BTC", "ETH": "ETH"},
                    "chain_id": 1337,
                    "ip_weight_limit_per_minute": 1200,
                    "info_request_type_ip_weights": {"meta": 2, "orderStatus": 1},
                    "default_info_weight": 2,
                    "exchange_action_base_ip_weight": 10,
                    "address_action_safety_net": AddressActionSafetyNetConfig(rate_per_minute=60),
                    "request_timeout_seconds": 20.0,
                    "ws_ping_interval_seconds": 25.0,
                }),
                "backpack": ExchangeSpecificConfig.model_validate({
                    "exchange_name": ExchangeName.BACKPACK,
                    "enabled": True,
                    "api_base_url_mainnet": "https://api.backpack.exchange",
                    "ws_url_mainnet": "wss://api.backpack.exchange/ws",
                    "rate_limit_per_minute": 100,
                    "symbols": {"BTC": "BTC-USDC", "ETH": "ETH-USDC"},
                    "request_timeout_seconds": 15.0,
                    "ws_ping_interval_seconds": 30.0,
                }),
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
                        min_funding_differential=Decimal("0.0001"),
                        check_interval=10,
                        risk_aversion=Decimal("1.0"),
                        rebalance_threshold=Decimal("0.05"),
                        perp_exchange="hyperliquid",
                        spot_exchange="backpack",
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
                max_slippage_pct=Decimal("0.002"),
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

    return _create_config


@pytest.fixture
def mock_hl_http_client() -> MagicMock:
    """Mock HyperliquidHttpClient.

    Returns:
        MagicMock instance configured for HyperliquidHttpClient testing.
    """
    mock_client = MagicMock()
    mock_client.close_session = AsyncMock()
    return mock_client


@pytest.fixture
def mock_bp_http_client() -> MagicMock:
    """Mock BackpackHttpClient.

    Returns:
        MagicMock instance configured for BackpackHttpClient testing.
    """
    mock_client = MagicMock()
    mock_client.close_session = AsyncMock()
    return mock_client


@pytest.fixture(scope="session")
def test_app_settings() -> AppSettings:
    """Provides a complete AppSettings instance for testing.

    This replaces the old Config class usage in tests with proper Pydantic models.

    Returns:
        Complete AppSettings instance configured for testing.
    """
    return AppSettings(
        general=GeneralSettings(
            log_level="INFO",
            safe_mode=True,
            state_file="data/test_state.json",
            state_backup_directory="data/test_state_backups",
            state_save_interval=300,
            state_backup_count=5,
        ),
        exchanges={
            "hyperliquid": ExchangeSpecificConfig.model_validate({
                "exchange_name": ExchangeName.HYPERLIQUID,
                "enabled": True,
                "api_base_url_mainnet": "https://api.hyperliquid.xyz",
                "ws_url_mainnet": "wss://api.hyperliquid.xyz/ws",
                "symbols": {"BTC": "BTC", "ETH": "ETH"},
                "chain_id": 1337,
                "ip_weight_limit_per_minute": 1200,
                "info_request_type_ip_weights": {"meta": 2, "orderStatus": 1},
                "default_info_weight": 2,
                "exchange_action_base_ip_weight": 10,
                "address_action_safety_net": AddressActionSafetyNetConfig(rate_per_minute=60),
            }),
            "backpack": ExchangeSpecificConfig.model_validate({
                "exchange_name": ExchangeName.BACKPACK,
                "enabled": True,
                "api_base_url_mainnet": "https://api.backpack.exchange",
                "ws_url_mainnet": "wss://api.backpack.exchange/ws",
                "rate_limit_per_minute": 100,
                "symbols": {"BTC": "BTC-USDC", "ETH": "ETH-USDC"},
            }),
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
                    min_funding_differential=Decimal("0.0001"),
                    check_interval=10,
                    risk_aversion=Decimal("1.0"),
                    rebalance_threshold=Decimal("0.05"),
                    perp_exchange="hyperliquid",
                    spot_exchange="backpack",
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
def active_bp_config() -> ExchangeSpecificConfig:
    """Fixture providing an active Backpack exchange configuration.

    Returns:
        ExchangeSpecificConfig instance for active Backpack testing.
    """
    return ExchangeSpecificConfig.model_validate({
        "exchange_name": ExchangeName.BACKPACK,
        "enabled": True,
        "api_base_url_mainnet": "https://api.backpack.exchange",
        "ws_url_mainnet": "wss://api.backpack.exchange/ws",
        "rate_limit_per_minute": 100,
        "symbols": {"BTC": "BTC-USDC", "ETH": "ETH-USDC"},
        "request_timeout_seconds": 15.0,
        "ws_ping_interval_seconds": 30.0,
    })


@pytest.fixture
def active_bp_secrets() -> ApiKeyAuthSecrets:
    """Fixture providing active Backpack secrets for testing.

    Returns:
        ApiKeyAuthSecrets instance with test credentials for Backpack.
    """
    return ApiKeyAuthSecrets(
        auth_type="api_key",
        api_key=SecretStr("test_active_api_key"),
        api_secret=SecretStr("test_active_api_secret"),
    )


@pytest.fixture
def test_config_dict() -> dict[str, Any]:
    """Provides a dictionary representation of test configuration for legacy test compatibility.

    This helps transition tests that expect dictionary-style config access.

    Returns:
        Dictionary containing legacy-style configuration for testing.
    """
    return {
        "exchanges": {
            "hyperliquid": {
                "enabled": True,
                "symbols": {"BTC": "BTC", "ETH": "ETH"},
                "fee_rate": "0.0004",
                "exchange_name": "hyperliquid",
                "api_base_url": "https://api.hyperliquid.xyz",
                "ws_url": "wss://api.hyperliquid.xyz/ws",
                "rate_limit_per_minute": 120,
                "chain_id": 1337,
            },
            "backpack": {
                "enabled": True,
                "symbols": {"BTC": "BTC_USDC", "ETH": "ETH_USDC"},
                "fee_rate": "0.0006",
                "exchange_name": "backpack",
                "api_base_url": "https://api.backpack.exchange",
                "ws_url": "wss://ws.backpack.exchange",
                "rate_limit_per_minute": 120,
            },
        },
        "strategy": {
            "funding_rate": {
                "min_funding_differential": "0.0002",
                "min_profit_threshold": "3.0",
                "funding_sample_period": 3600,
                "funding_sample_count": 24,
                "risk_aversion": 1.0,
                "default_slippage": "0.001",
                "slippage_sensitivity": "0.5",
                "liquidity_threshold_usd": "10000",
                "max_slippage_percent": "0.01",
            },
        },
        "risk": {
            "global": {
                "max_position_usd": "200.0",
                "max_total_exposure_usd": "1000.0",
            },
            "use_simple_sizing_path": True,
            "simple_sizing_method": "fixed_fraction",
            "simple_fixed_fraction": "0.1",
            "simple_fixed_usd_size": "10.0",
        },
        "execution": {
            "max_slippage_pct": "0.001",
            "max_retries": 3,
            "retry_delay_base_sec": "1.0",
            "settlement_delay": "2.0",
        },
    }
