from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from unittest.mock import create_autospec

import pytest

from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models import ArbitrageOpportunity, Ticker
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import FundingRateValidatorProtocol, PortfolioTrackerProtocol
from cyberdelta.core.signal_generator import SignalGenerator
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.utils.config import Config  # Assuming Config class is used
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem
from cyberdelta.validation.position_reconciliation import PositionReconciliationSystem
from tests.integration.mocks.mock_exchange import MockExchangeAPI

# --- Integration Test Specific Helpers & Fixtures ---


# Moved from test_core_workflow.py
def create_mock_ticker(
    symbol: str,
    bid: float,
    ask: float,
    price: float,
    timestamp: int,
) -> Ticker:
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


@pytest.fixture
def real_portfolio_tracker(mock_config: Config) -> PortfolioTracker:
    """Provides a real PortfolioTracker instance initialized with mock config."""
    # Assumes mock_config fixture is available from parent conftest.py
    tracker = PortfolioTracker(mock_config)
    return tracker


# Add other integration-specific fixtures here if needed


# Define needed secrets locally for integration tests
@pytest.fixture
def mock_secrets() -> dict[str, dict[str, str]]:
    """Provides dummy secrets needed by integration mock APIs."""
    return {
        "mock_hl": {"api_key": "integ_hl_key", "api_secret": "integ_hl_secret"},
        "mock_bp": {"api_key": "integ_bp_key", "api_secret": "integ_bp_secret"},
    }


@pytest.fixture
def mock_hl_api(mock_config: Config, mock_secrets: dict[str, dict[str, str]]) -> MockExchangeAPI:
    """Mock API for Hyperliquid, passes full config."""
    # Ensure necessary keys exist, *especially* collateral_asset
    if "mock_hl" not in mock_config.config_data["exchanges"]:
        mock_config.config_data["exchanges"]["mock_hl"] = {}
    # Explicitly set collateral_asset if missing, even if mock_hl key exists
    if "collateral_asset" not in mock_config.config_data["exchanges"]["mock_hl"]:
        mock_config.config_data["exchanges"]["mock_hl"]["collateral_asset"] = "USD"
    # Set fee rate if missing (optional, but good practice for mocks)
    if "fee_rate" not in mock_config.config_data["exchanges"]["mock_hl"]:
        mock_config.config_data["exchanges"]["mock_hl"]["fee_rate"] = 0.0005

    # Convert secrets to dict[str, Optional[str]] for compatibility
    secrets_hl: dict[str, str | None] = {k: v for k, v in mock_secrets["mock_hl"].items()}
    return MockExchangeAPI(
        "mock_hl",
        mock_config.config_data["exchanges"]["mock_hl"],
        secrets_hl,
        config_obj=mock_config,
    )


@pytest.fixture
def mock_bp_api(mock_config: Config, mock_secrets: dict[str, dict[str, str]]) -> MockExchangeAPI:
    """Mock API for Backpack, passes full config."""
    # Ensure necessary keys exist, *especially* collateral_asset
    if "mock_bp" not in mock_config.config_data["exchanges"]:
        mock_config.config_data["exchanges"]["mock_bp"] = {}
    # Explicitly set collateral_asset if missing
    if "collateral_asset" not in mock_config.config_data["exchanges"]["mock_bp"]:
        mock_config.config_data["exchanges"]["mock_bp"]["collateral_asset"] = "USDC"
    # Set fee rate if missing
    if "fee_rate" not in mock_config.config_data["exchanges"]["mock_bp"]:
        mock_config.config_data["exchanges"]["mock_bp"]["fee_rate"] = 0.0005

    # Convert secrets to dict[str, Optional[str]] for compatibility
    secrets_bp: dict[str, str | None] = {k: v for k, v in mock_secrets["mock_bp"].items()}
    return MockExchangeAPI(
        "mock_bp",
        mock_config.config_data["exchanges"]["mock_bp"],
        secrets_bp,
        config_obj=mock_config,
    )


# --- Core Component Fixtures ---


@pytest.fixture
def data_handler(
    mock_config: Config, mock_hl_api: MockExchangeAPI, mock_bp_api: MockExchangeAPI
) -> DataHandler:
    """Data Handler instance with mock APIs registered."""
    dh = DataHandler(mock_config)
    dh.register_api_client("mock_hl", mock_hl_api)
    dh.register_api_client("mock_bp", mock_bp_api)
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
    mock_balance = type("ExchangeBalance", (), {"available": Decimal("1000.0")})()
    mock_portfolio_tracker.get_exchange_balance.return_value = mock_balance
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
    eh.register_api_client("mock_hl", mock_hl_api)
    eh.register_api_client("mock_bp", mock_bp_api)
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

    if "mock_hl" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_hl", mock_hl_api)
    if "mock_bp" not in real_portfolio_tracker.api_clients:
        real_portfolio_tracker.register_api_client("mock_bp", mock_bp_api)
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
