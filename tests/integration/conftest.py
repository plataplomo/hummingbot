import pytest
from decimal import Decimal
from datetime import datetime, timezone

from cyberdelta.core.models import Ticker, FundingRate, ArbitrageOpportunity, OrderSide
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config # Assuming Config class is used

# --- Integration Test Specific Helpers & Fixtures ---

# Moved from test_core_workflow.py
def create_mock_ticker(symbol, bid, ask, price, timestamp):
    """Helper to create a Ticker object with Decimal conversion."""
    # This should ideally be moved from test_core_workflow.py
    # For now, defining it here if not already moved.
    return Ticker(
        symbol=symbol,
        bid=Decimal(str(bid)), # Ensure conversion from potential float/int
        ask=Decimal(str(ask)),
        price=Decimal(str(price)),
        timestamp=timestamp
    )

@pytest.fixture(scope="function")
def basic_opportunity():
    """Provides a basic ArbitrageOpportunity instance for integration tests."""
    opp = ArbitrageOpportunity(
        symbol="BTC-PERP",
        long_exchange="mock_bp",
        short_exchange="mock_hl",
        long_price=Decimal("30001"),
        short_price=Decimal("30010"),
        long_funding_rate=Decimal("0.0001"),
        short_funding_rate=Decimal("-0.00005"),
        net_funding_differential=Decimal("0.00015"),
        timestamp=datetime.now(timezone.utc)
    )
    opp.basis_volatility = 0.001 
    return opp

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
        "mock_bp": {"api_key": "integ_bp_key", "api_secret": "integ_bp_secret"}
    }

@pytest.fixture
def mock_hl_api(mock_config, mock_secrets):
    """Mock API for Hyperliquid, passes full config."""
    # Assumes mock_config and mock_secrets fixtures are available from parent conftest.py
    from tests.integration.mocks.mock_exchange import MockExchangeAPI
    # Ensure necessary keys exist in mock_config fixture
    if 'mock_hl' not in mock_config.config_data['exchanges']:
        mock_config.config_data['exchanges']['mock_hl'] = {"fee_rate": 0.0005, "collateral_asset": "USD"}
    if 'mock_hl' not in mock_secrets:
         mock_secrets['mock_hl'] = {"api_key": "hl_key", "api_secret": "hl_secret"}
    return MockExchangeAPI("mock_hl", mock_config.config_data['exchanges']['mock_hl'], mock_secrets['mock_hl'], config_obj=mock_config)

@pytest.fixture
def mock_bp_api(mock_config, mock_secrets):
    """Mock API for Backpack, passes full config."""
    # Assumes mock_config and mock_secrets fixtures are available from parent conftest.py
    from tests.integration.mocks.mock_exchange import MockExchangeAPI
    # Ensure necessary keys exist in mock_config fixture
    if 'mock_bp' not in mock_config.config_data['exchanges']:
        mock_config.config_data['exchanges']['mock_bp'] = {"fee_rate": 0.0005, "collateral_asset": "USDC"}
    if 'mock_bp' not in mock_secrets:
         mock_secrets['mock_bp'] = {"api_key": "bp_key", "api_secret": "bp_secret"}
    return MockExchangeAPI("mock_bp", mock_config.config_data['exchanges']['mock_bp'], mock_secrets['mock_bp'], config_obj=mock_config)

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

@pytest.fixture
def signal_generator(mock_config: Config, data_handler):
    """Signal Generator instance. Uses the mock_config fixture."""
    # Removed internal recreation of config, using the injected mock_config directly
    from cyberdelta.core.signal_generator import SignalGenerator
    # Assumes data_handler fixture provides a handler with registered APIs
    return SignalGenerator(mock_config, data_handler)

@pytest.fixture
def risk_manager(mock_config, real_portfolio_tracker, funding_rate_validator):
    """Risk Manager instance using the real portfolio tracker and mock validator."""
    from cyberdelta.core.risk_manager import RiskManager
    # Pass the mock validator fixture during initialization
    return RiskManager(mock_config, real_portfolio_tracker, funding_rate_validator=funding_rate_validator)

@pytest.fixture
def execution_handler(mock_config, real_portfolio_tracker, mock_hl_api, mock_bp_api, circuit_breaker_system):
    """Execution Handler instance with real tracker, mock APIs, and main circuit breaker."""
    from cyberdelta.core.execution_handler import ExecutionHandler
    eh = ExecutionHandler(mock_config, real_portfolio_tracker, circuit_breaker_system=circuit_breaker_system)
    # Ensure APIs are registered on the tracker if not already done by its fixture
    if "mock_hl" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_hl", mock_hl_api)
    if "mock_bp" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_bp", mock_bp_api)
    # Register APIs directly with the execution handler as well 
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
    validator.get_validation_metrics.return_value = {"rmse": 0.0, "bias": 0.0} # Assume good metrics initially
    return validator

@pytest.fixture
def position_reconciler(mock_config, real_portfolio_tracker, mock_hl_api, mock_bp_api):
    """Provides a PositionReconciliationSystem instance using the shared mock_config."""
    from cyberdelta.validation.position_reconciliation import PositionReconciliationSystem
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
    from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem
    # Pass mock_config to ensure it uses the test configuration
    return CircuitBreakerSystem(mock_config) 