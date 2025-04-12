from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cyberdelta.core.models import ArbitrageOpportunity, Ticker
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.signal_generator import SignalGenerator
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.utils.config import Config  # Assuming Config class is used
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem

# --- Integration Test Specific Helpers & Fixtures ---


# Moved from test_core_workflow.py
def create_mock_ticker(symbol, bid, ask, price, timestamp):
    """Helper to create a Ticker object with Decimal conversion."""
    # This should ideally be moved from test_core_workflow.py
    # For now, defining it here if not already moved.
    return Ticker(
        symbol=symbol,
        bid=Decimal(str(bid)),  # Ensure conversion from potential float/int
        ask=Decimal(str(ask)),
        price=Decimal(str(price)),
        timestamp=timestamp,
    )


@pytest.fixture(scope="function")
def basic_opportunity():
    """Provides a basic ArbitrageOpportunity instance for integration tests."""
    # Note: basis_volatility is set after creation currently, which is fine.
    # Ensure all required fields are present.
    return ArbitrageOpportunity(
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
        basis_volatility=0.001,  # Add optional float
        utility_score=None,  # Add optional float
    )


@pytest.fixture
def real_portfolio_tracker(mock_config: Config):
    """Provides a real PortfolioTracker instance initialized with mock config."""
    # Assumes mock_config fixture is available from parent conftest.py
    tracker = PortfolioTracker(mock_config)
    return tracker


# Add other integration-specific fixtures here if needed


# Define needed secrets locally for integration tests
@pytest.fixture
def mock_secrets():
    """Provides dummy secrets needed by integration mock APIs."""
    return {
        "mock_hl": {"api_key": "integ_hl_key", "api_secret": "integ_hl_secret"},
        "mock_bp": {"api_key": "integ_bp_key", "api_secret": "integ_bp_secret"},
    }


@pytest.fixture
def mock_hl_api(mock_config, mock_secrets):
    """Mock API for Hyperliquid, passes full config."""
    from tests.integration.mocks.mock_exchange import MockExchangeAPI

    # Ensure necessary keys exist, *especially* collateral_asset
    if "mock_hl" not in mock_config.config_data["exchanges"]:
        mock_config.config_data["exchanges"]["mock_hl"] = {}
    # Explicitly set collateral_asset if missing, even if mock_hl key exists
    if "collateral_asset" not in mock_config.config_data["exchanges"]["mock_hl"]:
        mock_config.config_data["exchanges"]["mock_hl"]["collateral_asset"] = "USD"
    # Set fee rate if missing (optional, but good practice for mocks)
    if "fee_rate" not in mock_config.config_data["exchanges"]["mock_hl"]:
        mock_config.config_data["exchanges"]["mock_hl"]["fee_rate"] = 0.0005

    if "mock_hl" not in mock_secrets:
        mock_secrets["mock_hl"] = {"api_key": "hl_key", "api_secret": "hl_secret"}
    return MockExchangeAPI(
        "mock_hl",
        mock_config.config_data["exchanges"]["mock_hl"],
        mock_secrets["mock_hl"],
        config_obj=mock_config,
    )


@pytest.fixture
def mock_bp_api(mock_config, mock_secrets):
    """Mock API for Backpack, passes full config."""
    from tests.integration.mocks.mock_exchange import MockExchangeAPI

    # Ensure necessary keys exist, *especially* collateral_asset
    if "mock_bp" not in mock_config.config_data["exchanges"]:
        mock_config.config_data["exchanges"]["mock_bp"] = {}
    # Explicitly set collateral_asset if missing
    if "collateral_asset" not in mock_config.config_data["exchanges"]["mock_bp"]:
        mock_config.config_data["exchanges"]["mock_bp"]["collateral_asset"] = "USDC"
    # Set fee rate if missing
    if "fee_rate" not in mock_config.config_data["exchanges"]["mock_bp"]:
        mock_config.config_data["exchanges"]["mock_bp"]["fee_rate"] = 0.0005

    if "mock_bp" not in mock_secrets:
        mock_secrets["mock_bp"] = {"api_key": "bp_key", "api_secret": "bp_secret"}
    return MockExchangeAPI(
        "mock_bp",
        mock_config.config_data["exchanges"]["mock_bp"],
        mock_secrets["mock_bp"],
        config_obj=mock_config,
    )


# --- Core Component Fixtures ---


@pytest.fixture
def data_handler(mock_config, mock_hl_api, mock_bp_api):
    """Data Handler instance with mock APIs registered."""
    from cyberdelta.core.data_handler import DataHandler

    dh = DataHandler(mock_config)
    # Use the correct mock API fixtures defined in this conftest
    dh.register_api_client("mock_hl", mock_hl_api)
    dh.register_api_client("mock_bp", mock_bp_api)
    return dh


# Define symbol_mapper fixture
@pytest.fixture
def symbol_mapper(mock_config: Config) -> SymbolMapper:
    """Provides a SymbolMapper instance initialized with mock config."""
    return SymbolMapper(mock_config.config_data)


@pytest.fixture(scope="function")
def signal_generator(mock_config, data_handler, symbol_mapper):
    """Fixture for a SignalGenerator instance with mock data handler."""
    return SignalGenerator(mock_config, data_handler, symbol_mapper)


@pytest.fixture
def risk_manager(mock_config, real_portfolio_tracker, funding_rate_validator):
    """Risk Manager instance using the real portfolio tracker and mock validator."""
    from cyberdelta.core.risk_manager import RiskManager

    # Pass the mock validator fixture during initialization
    return RiskManager(
        mock_config,
        real_portfolio_tracker,
        funding_rate_validator=funding_rate_validator,
    )


@pytest.fixture
def execution_handler(
    mock_config,
    real_portfolio_tracker,
    mock_hl_api,
    mock_bp_api,
    circuit_breaker_system,
):
    """Execution Handler instance with real tracker, mock APIs, CB system, and SymbolMapper."""
    from cyberdelta.core.execution_handler import ExecutionHandler

    # Instantiate SymbolMapper using the mock_config
    symbol_mapper = SymbolMapper(mock_config.config_data)

    eh = ExecutionHandler(
        config=mock_config,
        portfolio_tracker=real_portfolio_tracker,
        symbol_mapper=symbol_mapper,
        circuit_breaker_system=circuit_breaker_system,
    )

    # Register APIs with the execution handler
    eh.register_api_client("mock_hl", mock_hl_api)
    eh.register_api_client("mock_bp", mock_bp_api)
    return eh


# --- Safety System Specific Fixtures ---


@pytest.fixture
def funding_rate_validator():
    """Provides a MagicMock for the FundingRateValidator."""
    from unittest.mock import MagicMock

    # Mock the validator used by RiskManager
    validator = MagicMock()
    # Set default return values if needed for tests not specifically configuring it
    validator.get_validation_metrics.return_value = {
        "rmse": 0.0,
        "bias": 0.0,
    }  # Assume good metrics initially
    return validator


@pytest.fixture
def position_reconciler(mock_config, real_portfolio_tracker, mock_hl_api, mock_bp_api):
    """Provides a PositionReconciliationSystem instance using the shared mock_config."""
    from cyberdelta.validation.position_reconciliation import (
        PositionReconciliationSystem,
    )
    # Removed creation of separate integration_config

    # Ensure mock APIs are registered on the tracker
    if "mock_hl" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_hl", mock_hl_api)
    if "mock_bp" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_bp", mock_bp_api)

    # Pass the shared mock_config object directly
    reconciler = PositionReconciliationSystem(mock_config, real_portfolio_tracker)
    return reconciler


@pytest.fixture
def circuit_breaker_system(mock_config):
    """Provides a CircuitBreakerSystem instance."""

    # Pass mock_config to ensure it uses the test configuration
    return CircuitBreakerSystem(mock_config)


# Find opportunity creation/mocking
@pytest.fixture
def mock_opportunity():
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
