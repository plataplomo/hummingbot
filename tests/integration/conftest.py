import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from unittest.mock import create_autospec, patch

import pytest
import pytest_asyncio

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
from cyberdelta.utils.config import Config  # Assuming Config class is used
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
        long_exchange="backpack",
        short_exchange="hyperliquid",
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


@pytest.fixture
def real_portfolio_tracker(mock_config: Config) -> PortfolioTracker:
    """Provides a real PortfolioTracker instance initialized with mock config."""
    # Assumes mock_config fixture is available from parent conftest.py
    tracker = PortfolioTracker(mock_config)
    return tracker


# Add other integration-specific fixtures here if needed


# Define needed secrets locally for integration tests
@pytest.fixture
def mock_secrets() -> dict[str, dict[str, str | None]]:
    """Provides dummy secrets needed by integration mock APIs."""
    return {
        "hyperliquid": {"api_key": "integ_hl_key", "api_secret": "integ_hl_secret"},
        "backpack": {"api_key": "integ_bp_key", "api_secret": "integ_bp_secret"},
    }


@pytest_asyncio.fixture(scope="function")
async def mock_hl_api(
    mock_config: Config, mock_secrets: dict[str, dict[str, str | None]]
) -> AsyncGenerator[MockExchangeAPI]:
    """Function-scoped mock HyperLiquid API with patched clients."""
    exchange_name = "hyperliquid"
    exchange_config_dict = mock_config.config_data["exchanges"][exchange_name]
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
    mock_config: Config, mock_secrets: dict[str, dict[str, str | None]]
) -> AsyncGenerator[MockExchangeAPI]:
    """Function-scoped mock Backpack API with patched clients."""
    exchange_name = "backpack"
    exchange_config_dict = mock_config.config_data["exchanges"][exchange_name]
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
            config=exchange_config_dict,
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
    mock_config: Config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    symbol_mapper: SymbolMapper,
) -> DataHandler:
    """Data Handler instance with mock APIs registered."""
    dh = DataHandler(mock_config, symbol_mapper)
    dh.register_api_client("hyperliquid", mock_hl_api)
    dh.register_api_client("backpack", mock_bp_api)
    return dh


# Define symbol_mapper fixture
@pytest.fixture
def symbol_mapper(mock_config: Config) -> SymbolMapper:
    """Provides a SymbolMapper instance initialized with mock config."""
    return SymbolMapper(mock_config.config_data)


@pytest.fixture(scope="function")
def signal_generator(
    mock_config: Config, data_handler: DataHandler, symbol_mapper: SymbolMapper
) -> SignalGenerator:
    """Fixture for a SignalGenerator instance with mock data handler."""
    return SignalGenerator(mock_config, data_handler, symbol_mapper)


@pytest.fixture
def risk_manager(
    mock_config: Config,
    # Use protocol-compliant mocks for both dependencies
) -> object:
    """
    Risk Manager instance using protocol-compliant mocks for portfolio tracker and
    funding rate validator.
    """
    from cyberdelta.core.risk_manager import RiskManager

    # Create a protocol-compliant mock for PortfolioTrackerProtocol
    mock_portfolio_tracker = create_autospec(PortfolioTrackerProtocol, instance=True)
    mock_portfolio_tracker.get_total_capital.return_value = Decimal("100000.0")
    # mock_balance = type("ExchangeBalance", (), {"available": Decimal("1000.0")})() # OLD
    # mock_portfolio_tracker.get_exchange_balance.return_value = mock_balance # OLD
    # NEW: Use SpotBalance
    mock_spot_balance = SpotBalance(
        exchange="mock_generic",  # Generic mock exchange name
        asset="USDC",  # Common asset
        total_quantity=Decimal("1000.0"),
        available_quantity=Decimal("1000.0"),
        timestamp=datetime.now(UTC),  # Add required timestamp
    )
    mock_portfolio_tracker.get_exchange_balance.return_value = mock_spot_balance
    # Create a protocol-compliant mock for FundingRateValidatorProtocol
    mock_funding_validator = create_autospec(FundingRateValidatorProtocol, instance=True)
    mock_funding_validator.get_symbol_metrics.return_value = {"rmse": 0.0, "bias": 0.0}
    return RiskManager(
        mock_config,
        mock_portfolio_tracker,
        funding_rate_validator=mock_funding_validator,
    )


@pytest.fixture
def execution_handler(
    mock_config: Config,
    real_portfolio_tracker: PortfolioTracker,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    circuit_breaker_system: CircuitBreakerSystem,
) -> ExecutionHandler:
    """Execution Handler instance with real tracker, mock APIs, CB system, and SymbolMapper."""
    from cyberdelta.core.execution_handler import ExecutionHandler

    symbol_mapper = SymbolMapper(mock_config.config_data)
    eh = ExecutionHandler(
        config=mock_config,
        portfolio_tracker=real_portfolio_tracker,
        symbol_mapper=symbol_mapper,
        circuit_breaker_system=circuit_breaker_system,
    )
    eh.register_api_client("hyperliquid", mock_hl_api)
    eh.register_api_client("backpack", mock_bp_api)
    return eh


# --- Safety System Specific Fixtures ---


@pytest.fixture
def funding_rate_validator() -> FundingRateValidatorProtocol:
    """Provides a protocol-compliant mock for the FundingRateValidator."""
    from unittest.mock import create_autospec

    mock_validator = create_autospec(FundingRateValidatorProtocol, instance=True)
    mock_validator.get_symbol_metrics.return_value = {"rmse": 0.0, "bias": 0.0}
    return cast(FundingRateValidatorProtocol, mock_validator)


@pytest.fixture
def position_reconciler(
    mock_config: Config,
    real_portfolio_tracker: PortfolioTracker,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
) -> PositionReconciliationSystem:
    """Provides a PositionReconciliationSystem instance using the shared mock_config."""
    from cyberdelta.validation.position_reconciliation import PositionReconciliationSystem

    if "hyperliquid" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("hyperliquid", mock_hl_api)
    if "backpack" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("backpack", mock_bp_api)
    reconciler = PositionReconciliationSystem(mock_config, real_portfolio_tracker)
    return reconciler


@pytest.fixture
def circuit_breaker_system(mock_config: Config) -> CircuitBreakerSystem:
    """Provides a CircuitBreakerSystem instance."""

    # Pass mock_config to ensure it uses the test configuration
    return CircuitBreakerSystem(mock_config)


# Find opportunity creation/mocking
@pytest.fixture
def mock_opportunity() -> ArbitrageOpportunity:
    return ArbitrageOpportunity(
        symbol="BTC-PERP",
        long_exchange="hyperliquid",
        short_exchange="backpack",
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
