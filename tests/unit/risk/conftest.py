# Fixtures for RiskManager tests
from datetime import datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

from pytest import fixture

from cyberdelta.core.models import ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.utils.config import Config
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem


@fixture
def mock_config_values() -> dict[str, Any]:
    """Return the dictionary of default mock config values."""
    # Centralize the default mock values
    return {
        "risk.global.max_position_usd": "1000.0",  # Use the correct key RiskManager expects
        "risk.global.max_total_exposure_usd": "20000.0",  # Use the correct key RiskManager expects
        "risk.global.max_portfolio_leverage": "3.0",  # Use the correct key RiskManager expects
        "risk.max_collateral_per_exchange": "0.8",
        "risk.max_exposure_per_asset": "0.2",
        "risk.max_exposure_per_exchange": "0.5",
        # "risk.target_leverage": "2.0", # Key likely not used directly in current tests/code
        # "risk.max_exposure_per_strategy": "0.4", # Key likely not used
        # "risk.max_exchange_concentration": "0.6", # Key likely not used
        "risk.kelly_fraction": "0.5",
        "exchanges": {
            "hyperliquid": {"enabled": True},
            "backpack": {"enabled": True},
        },  # Provide structure
        "risk.min_liquidation_buffer": "0.2",
        "risk_manager.min_exchange_balance": "10.0",  # Corrected key prefix
        "risk.strategy.max_single_position_exposure_ratio": "0.1",  # Use the correct key
        "risk.global.max_drawdown_limit_ratio": "0.2",  # Use the correct key
        "strategy.min_net_funding_differential": "0.0001",  # Use the correct key
        "risk.strategy.max_leverage_per_trade": "5.0",  # Use the correct key
        "risk.max_acceptable_rmse": "0.05",
        "risk.max_acceptable_bias": "0.02",
        "risk.min_validation_factor": "0.2",
        "strategy.volatility_period_days": 14,  # Use the correct key
        "risk.circuit_breaker_recovery_factor": "0.3",  # Add default
        # --- Default values for simple path (can be overridden in tests) ---
        "risk.use_simple_sizing_path": False,
        "risk.simple_sizing_method": "fixed_fraction",
        "risk.simple_fixed_fraction": "0.01",
        "risk.simple_fixed_usd_size": "100",
    }


@fixture
def mock_config(mock_config_values: dict[str, Any]) -> MagicMock:
    """Create a mock config using the default values."""
    cfg = MagicMock(spec=Config)
    # Store the default values dictionary for reference in tests
    cfg.default_values = mock_config_values

    # The side_effect function now just looks up from the stored defaults
    def config_get_side_effect(key: str, default: object | None = None) -> Any:
        return cfg.default_values.get(key, default)

    cfg.get.side_effect = config_get_side_effect
    return cfg


@fixture
def mock_portfolio_tracker() -> MagicMock:
    """Create a mock portfolio tracker for testing."""
    tracker = MagicMock()
    # --- Set DEFAULT return values ---
    tracker.get_total_capital.return_value = Decimal("100000.0")
    mock_balance = MagicMock()
    mock_balance.available = Decimal("1000.0")
    tracker.get_exchange_balance.return_value = mock_balance

    def collateral_balance_side_effect(*args: Any) -> Decimal:
        return Decimal("1000.0")

    tracker.get_exchange_collateral_balance.side_effect = collateral_balance_side_effect
    tracker.get_total_exposure.return_value = Decimal("1000.0")
    tracker.get_exchange_exposure.return_value = Decimal("0.0")  # Method name correction
    tracker.get_symbol_exposure.return_value = Decimal(
        "0.0"
    )  # Method likely not used directly, keep for now
    tracker.get_portfolio_drawdown.return_value = Decimal("0.0")  # Method name correction
    tracker.get_exchange_drawdown.return_value = Decimal("0.0")
    tracker.get_current_drawdown.return_value = Decimal("0.0")

    # Patch get_all_positions to return a valid tuple for exposure calculations
    mock_position = MagicMock()
    mock_position.symbol = "BTC"
    mock_position.size = Decimal("0.1")
    mock_position.entry_price = Decimal("50000.0")
    mock_position.is_active.return_value = True
    mock_position.mark_price = Decimal("50000.0")
    mock_position.liquidation_price = Decimal("40000.0")
    tracker.get_all_positions.return_value = [("hyperliquid", mock_position)]

    tracker.get_active_exchanges.return_value = [
        "hyperliquid",
        "backpack",
    ]  # Default active exchanges

    # Mocks for methods used in complex adjustments (defaults)
    tracker.get_asset_volatility.return_value = Decimal("0.02")
    tracker.get_historical_volatility.return_value = Decimal("0.015")
    tracker.get_asset_correlation.return_value = 0.5  # Correlation likely float

    # Add mocks for get_position used by calculate_position_exposure
    mock_position_active = MagicMock()
    mock_position_active.is_active.return_value = True
    mock_position_active.symbol = "BTC"
    mock_position_active.mark_price = Decimal("50000.0")
    mock_position_active.size = Decimal("0.1")
    mock_position_active.liquidation_price = Decimal("40000.0")

    mock_position_inactive = MagicMock()
    mock_position_inactive.is_active.return_value = False
    mock_position_inactive.symbol = "ETH"

    def mock_get_position(exchange_id: str, symbol: str) -> MagicMock:
        if symbol == "BTC":
            return mock_position_active
        else:
            return mock_position_inactive  # Or None if preferred

    tracker.get_position.side_effect = mock_get_position
    # Mock get_all_positions to return the active one for correlation/exposure tests if needed
    tracker.get_all_positions.return_value = [("hyperliquid", mock_position_active)]

    return tracker


@fixture
def mock_circuit_breaker() -> MagicMock:
    """Create a mock CircuitBreakerSystem."""
    # Define the methods expected by RiskManager based on CircuitBreakerSystem definition
    # Use autospec=True to automatically create the spec from the class
    cb = MagicMock(spec=CircuitBreakerSystem, autospec=True)
    # Default behavior: execution is allowed
    cb.can_execute.return_value = (True, None)
    return cb


@fixture
def mock_funding_validator() -> MagicMock:
    """Create a mock FundingRateValidator."""
    from cyberdelta.validation.funding_rate_validator import FundingRateValidator

    fv = MagicMock(spec=FundingRateValidator)

    # Always return high-confidence metrics for any call
    def symbol_metrics_side_effect(exchange: str, symbol: str) -> dict[str, float]:
        return {"rmse": 0.0, "bias": 0.0}

    fv.get_symbol_metrics.side_effect = symbol_metrics_side_effect
    return fv


@fixture
def mock_data_handler() -> MagicMock:
    """Create a mock data handler for testing."""
    # Import locally
    from cyberdelta.core.data_handler import DataHandler

    dh = MagicMock(spec=DataHandler)
    # Setup default return values if needed for specific tests
    dh.get_recent_volatility.return_value = 0.02  # Example default
    dh.get_historical_volatility.return_value = 0.015  # Example default
    dh.get_correlation.return_value = 0.5  # Example default
    return dh


@fixture
def risk_manager(
    mock_config: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_circuit_breaker: MagicMock,
    mock_funding_validator: MagicMock,
) -> RiskManager:
    """Create a RiskManager instance with mocked dependencies."""
    rm = RiskManager(
        mock_config, mock_portfolio_tracker, mock_circuit_breaker, mock_funding_validator
    )
    return rm


@fixture
def sample_opportunity() -> ArbitrageOpportunity:
    """Create a sample arbitrage opportunity using the correct signature."""
    # Match the signature from signal_generator.py
    opp = ArbitrageOpportunity(
        symbol="BTC",
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_price=Decimal("50001.0"),
        short_price=Decimal("50004.0"),
        long_funding_rate=Decimal("0.0002"),
        short_funding_rate=Decimal("-0.0003"),
        net_funding_differential=Decimal("0.0005"),
        timestamp=datetime.now(),
        expected_profit=Decimal("10.0"),  # Ensure Decimal
        utility_score=0.8,  # float is ok here
        basis_volatility=0.002,  # float ok for Kelly input
    )
    return opp
