import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import create_autospec, patch

import pytest
import pytest_asyncio

from cyberdelta.config import AppSettings
from cyberdelta.core.data_handler import DataHandler, Ticker
from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models import SpotBalance
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import (
    FundingRateValidatorProtocol,
    PortfolioTrackerProtocol,
)
from cyberdelta.core.signal_generator import SignalGenerator
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity
from cyberdelta.validation.position_reconciliation import PositionReconciliationSystem
from tests.integration.mocks.mock_exchange import MockExchangeAPI

logger = logging.getLogger(__name__)

# --- Integration Test Specific Helpers & Fixtures ---


# Moved from test_core_workflow.py
def create_mock_ticker(
    symbol: str,
    bid: str | float | Decimal,  # Allow various inputs
    ask: str | float | Decimal,
    price: str | float | Decimal,
    timestamp: datetime,  # Expect datetime object
) -> Ticker:  # Return Ticker object
    """Helper to create a Ticker object with Decimal conversion."""
    return Ticker(
        symbol=symbol,
        bid=Decimal(str(bid)),
        ask=Decimal(str(ask)),
        price=Decimal(str(price)),
        timestamp=timestamp,  # Pass datetime directly
    )


@pytest.fixture(scope="function")
def basic_opportunity() -> ArbitrageOpportunity:
    """Provides a basic ArbitrageOpportunity instance for integration tests."""
    # Note: basis_volatility is set after creation currently, which is fine.
    # Ensure all required fields are present.
    opp = ArbitrageOpportunity(
        symbol="BTC",
        long_exchange="mock_bp",
        short_exchange="mock_hl",
        long_price=Decimal("30001"),  # Already correct
        short_price=Decimal("30010"),  # Already correct
        long_funding_rate=Decimal("0.0001"),  # Already correct
        short_funding_rate=Decimal("-0.00005"),  # Already correct
        net_funding_differential=Decimal("0.00015"),  # Already correct
        timestamp=datetime.now(UTC),  # Already correct
        # Add missing optional args if needed, or ensure they are None
        basis_volatility=0.01,  # Increased for conservative, safe sizing
        utility_score=None,  # Add optional float
        expected_profit=Decimal("0.01"),  # Set via constructor, not as attribute
    )
    return opp


@pytest_asyncio.fixture(scope="function")
async def real_portfolio_tracker(mock_config: AppSettings) -> AsyncGenerator[PortfolioTracker]:
    """Provides a real PortfolioTracker instance initialized with mock config."""
    tracker = PortfolioTracker(mock_config)
    # DO NOT call await tracker.initialize() here.
    # Initialization should happen in the test or a more specific fixture
    # after API clients are registered.
    yield tracker
    # No specific teardown needed for PortfolioTracker itself unless it holds resources
    # that need explicit async closing beyond what its components (like api_clients) handle.


# Add other integration-specific fixtures here if needed


# Define needed secrets locally for integration tests
@pytest.fixture
def mock_secrets() -> dict[str, dict[str, str | None]]:
    """Provides dummy secrets needed by integration mock APIs."""
    return {
        "mock_hl": {"api_key": "integ_hl_key", "api_secret": "integ_hl_secret"},
        "mock_bp": {"api_key": "integ_bp_key", "api_secret": "integ_bp_secret"},
    }


@pytest_asyncio.fixture(scope="function")
async def mock_hl_api(
    mock_config: AppSettings, mock_secrets: dict[str, dict[str, str | None]]
) -> AsyncGenerator[MockExchangeAPI]:
    """Function-scoped mock HyperLiquid API with patched clients."""
    exchange_name = "mock_hl"
    # For AppSettings, we need to access exchange config differently
    exchange_config_dict: dict[str, Any] = {}  # Simplified for mock

    exchange_secrets = mock_secrets[exchange_name]

    with (
        patch("cyberdelta.apis.connectivity.http_client.HttpClient.__init__", return_value=None),
        patch(
            "cyberdelta.apis.connectivity.ws_manager.WebSocketManager.__init__", return_value=None
        ),
    ):
        api = MockExchangeAPI(
            exchange_name=exchange_name,
            config=exchange_config_dict,
            secrets=exchange_secrets,
            config_obj=mock_config,
        )
        try:
            yield api
        finally:
            await api.close()


@pytest_asyncio.fixture(scope="function")
async def mock_bp_api(
    mock_config: AppSettings, mock_secrets: dict[str, dict[str, str | None]]
) -> AsyncGenerator[MockExchangeAPI]:
    """Function-scoped mock Backpack API with patched clients."""
    exchange_name = "mock_bp"
    # For AppSettings, we need to access exchange config differently
    exchange_config_dict_bp: dict[str, Any] = {}  # Simplified for mock

    exchange_secrets = mock_secrets[exchange_name]

    with (
        patch(
            "cyberdelta.apis.connectivity.http_client.HttpClient.__init__",
            return_value=None,
        ),
        patch(
            "cyberdelta.apis.connectivity.ws_manager.WebSocketManager.__init__",
            return_value=None,
        ),
    ):
        api = MockExchangeAPI(
            exchange_name=exchange_name,
            config=exchange_config_dict_bp,
            secrets=exchange_secrets,
            config_obj=mock_config,
        )
        try:
            yield api
        finally:
            await api.close()


# --- Core Component Fixtures ---


@pytest.fixture
def data_handler(
    mock_config: AppSettings,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    symbol_mapper: SymbolMapper,
    real_portfolio_tracker: PortfolioTracker,
) -> DataHandler:
    """Data Handler instance with mock APIs registered."""
    api_clients = {
        "mock_hl": mock_hl_api,
        "mock_bp": mock_bp_api,
    }
    dh = DataHandler(
        app_settings=mock_config,
        api_clients=api_clients,
        portfolio_tracker=real_portfolio_tracker,
        symbol_mapper=symbol_mapper,
    )
    return dh


# Define symbol_mapper fixture
@pytest.fixture
def symbol_mapper(mock_config: AppSettings) -> SymbolMapper:
    """Provides a SymbolMapper instance initialized with mock config."""
    # For AppSettings, provide empty dict for exchanges config
    config_data_for_mapper = {}
    return SymbolMapper(config_data_for_mapper)


@pytest.fixture(scope="function")
def signal_generator(
    mock_config: AppSettings, data_handler: DataHandler, symbol_mapper: SymbolMapper
) -> SignalGenerator:
    """Fixture for a SignalGenerator instance with mock data handler."""
    return SignalGenerator(mock_config, data_handler, symbol_mapper)


@pytest.fixture
def risk_manager(
    mock_config: AppSettings,
) -> object:  # Keep as object to avoid circular dependency if RiskManager imports protocols
    """
    Risk Manager instance using protocol-compliant mocks for portfolio tracker and
    funding rate validator.
    """
    from cyberdelta.core.risk_manager import RiskManager  # Local import

    mock_portfolio_tracker = create_autospec(PortfolioTrackerProtocol, instance=True)
    mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
    mock_portfolio_tracker.get_total_exposure_usd.return_value = Decimal("0")
    mock_portfolio_tracker.get_current_drawdown.return_value = Decimal("0")
    mock_spot_balance = SpotBalance(
        exchange="mock_generic",
        asset="USDC",
        total_quantity=Decimal("1000.0"),
        available_quantity=Decimal("1000.0"),
        timestamp=datetime.now(UTC),
    )
    mock_portfolio_tracker.get_exchange_balance.return_value = mock_spot_balance
    mock_funding_validator = create_autospec(FundingRateValidatorProtocol, instance=True)
    mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.0, "bias": 0.0}
    return RiskManager(
        app_settings=mock_config,
        portfolio_tracker=mock_portfolio_tracker,
        funding_rate_validator=mock_funding_validator,
    )


@pytest.fixture
def execution_handler(
    mock_config: AppSettings,
    real_portfolio_tracker: PortfolioTracker,  # Will use the async real_portfolio_tracker
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    circuit_breaker_system: CircuitBreakerSystem,
) -> ExecutionHandler:
    """Execution Handler instance with real tracker, mock APIs, CB system, and SymbolMapper."""
    from cyberdelta.core.execution_handler import ExecutionHandler  # Local import

    # For AppSettings, provide empty dict for exchanges config
    config_data_for_mapper_eh = {}
    symbol_mapper_instance = SymbolMapper(config_data_for_mapper_eh)
    eh = ExecutionHandler(
        app_settings=mock_config,
        portfolio_tracker=real_portfolio_tracker,
        symbol_mapper=symbol_mapper_instance,
        circuit_breaker_system=circuit_breaker_system,
    )
    eh.register_api_client("mock_hl", mock_hl_api)
    eh.register_api_client("mock_bp", mock_bp_api)
    return eh


# --- Safety System Specific Fixtures ---


@pytest.fixture
def funding_rate_validator() -> FundingRateValidatorProtocol:
    """Provides a protocol-compliant mock for the FundingRateValidator."""
    from unittest.mock import create_autospec  # Local import

    mock_validator = create_autospec(FundingRateValidatorProtocol, instance=True)
    mock_validator.get_symbol_metrics.return_value = {"rmse": 0.0, "bias": 0.0}
    return mock_validator


@pytest_asyncio.fixture(scope="function")  # Changed to async fixture
async def position_reconciler(
    mock_config: AppSettings,
    # real_portfolio_tracker: PortfolioTracker, # No longer directly used, will create its own
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
) -> AsyncGenerator[PositionReconciliationSystem]:  # Changed return type
    """Provides a PositionReconciliationSystem instance with mock APIs."""
    # Create a fresh PortfolioTracker for this fixture
    portfolio_tracker = PortfolioTracker(mock_config)
    await portfolio_tracker.initialize()  # Initialize it
    portfolio_tracker.register_api_client("mock_hl", mock_hl_api)
    portfolio_tracker.register_api_client("mock_bp", mock_bp_api)

    reconciler = PositionReconciliationSystem(
        app_settings=mock_config,
        portfolio_tracker=portfolio_tracker,
    )
    try:
        yield reconciler
    finally:
        # Clean up if needed
        await portfolio_tracker.shutdown()


@pytest.fixture
def circuit_breaker_system(mock_config: AppSettings) -> CircuitBreakerSystem:
    """Provides a CircuitBreakerSystem instance initialized with mock config."""
    global_cb_path_parts = ["validation", "circuit_breaker", "global", "api_errors"]
    # Create a mock config structure for circuit breaker
    from unittest.mock import MagicMock

    mock_cb_config = MagicMock()
    mock_cb_config.failure_threshold = 5
    mock_cb_config.recovery_timeout_seconds = 60
    mock_cb_config.half_open_max_calls = 3

    # Mock the nested config access
    mock_config.validation = MagicMock()
    mock_config.validation.circuit_breaker = MagicMock()
    mock_config.validation.circuit_breaker.global_config = MagicMock()
    mock_config.validation.circuit_breaker.global_config.api_errors = mock_cb_config

    return CircuitBreakerSystem(mock_config)


# Find opportunity creation/mocking
@pytest.fixture
def mock_opportunity() -> ArbitrageOpportunity:
    return ArbitrageOpportunity(
        symbol="BTC-PERP",
        long_exchange="mock_hl",
        short_exchange="mock_bp",
        long_price=Decimal("30000"),  # Already correct
        short_price=Decimal("30050"),  # Already correct
        long_funding_rate=Decimal("0.0001"),  # Already correct
        short_funding_rate=Decimal("-0.0001"),  # Already correct
        net_funding_differential=Decimal("0.0002"),  # Already correct
        timestamp=datetime.now(UTC),  # Already correct
        expected_profit=Decimal("5.0"),  # Already correct
        # Add missing optional args
        basis_volatility=0.002,  # Example float value
        utility_score=0.6,  # Example float value
    )
