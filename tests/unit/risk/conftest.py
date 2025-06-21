"""Test fixtures for risk management tests.

Provides common fixtures and utilities for testing risk management components.
"""

# Fixtures for RiskManager tests
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from pytest import fixture

from cyberdelta.core.models import SpotBalance
from cyberdelta.core.risk_manager import (
    CircuitBreakerSystemProtocol,
    FundingRateValidatorProtocol,
    PortfolioTrackerProtocol,
    RiskManager,
)
from cyberdelta.validation.circuit_breaker import BreakerState, CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity


@fixture
def mock_config_dict() -> dict[str, Any]:
    """Return mock config dict for testing."""
    return {
        "risk": {
            "global": {
                "max_position_usd": "5000.0",
                "max_total_exposure_usd": "10000.0",
                "min_position_usd": "10.0",
                "max_portfolio_leverage": "5.0",
                "max_drawdown_limit_ratio": "0.2",
            },
            "strategy": {
                "max_single_position_exposure_ratio": "0.25",
                "max_leverage_per_trade": "5.0",
                "min_net_funding_differential": "0.0001",
            },
            "kelly": {
                "fraction": "0.5",
                "min_acceptable_fraction": "0.01",
                "max_acceptable_fraction": "0.25",
                "min_volatility": "0.0001",
            },
            "simple_sizing_method": "fixed_fraction",
            "simple_fixed_fraction": "0.01",  # 1% of capital
            "simple_fixed_usd_size": "100.0",
            "max_acceptable_rmse": "0.05",  # 5% RMSE
            "max_acceptable_bias": "0.02",  # 2% bias
            "min_validation_factor": "0.2",  # Minimum factor to apply if validation fails
            "min_liquidation_buffer": "0.15",  # 15%
        },
        "exchanges": {
            "exchange_a": {
                "enabled": True,
                "collateral_asset": "USD",
                "risk_modifier": 1.0,
            },
            "exchange_b": {
                "enabled": True,
                "collateral_asset": "USD",
                "risk_modifier": 1.0,
            },
        },
        "balance": {"min_exchange_balance": "50.0"},  # Added for RiskManager's _load_config
        "risk_manager": {"min_exchange_balance": "10.0"},  # Used by RiskManager
    }


@fixture
def mock_config(mock_config_dict: dict[str, Any]) -> MagicMock:
    """Create a mock AppSettings object for testing."""
    # Don't use spec=AppSettings since we need to mock nested attributes
    mock = MagicMock()

    # Create nested mock structure to match AppSettings attribute access pattern
    # Based on how RiskManager accesses config: app_settings.risk.global_risk.max_position_usd

    # Set up risk configuration
    mock.risk.global_risk.max_position_usd = Decimal(
        mock_config_dict["risk"]["global"]["max_position_usd"]
    )
    mock.risk.global_risk.max_total_exposure_usd = Decimal(
        mock_config_dict["risk"]["global"]["max_total_exposure_usd"]
    )
    mock.risk.global_risk.min_position_usd = Decimal(
        mock_config_dict["risk"]["global"]["min_position_usd"]
    )
    mock.risk.global_risk.max_portfolio_leverage = Decimal(
        mock_config_dict["risk"]["global"]["max_portfolio_leverage"]
    )
    mock.risk.global_risk.max_drawdown_limit_ratio = Decimal(
        mock_config_dict["risk"]["global"]["max_drawdown_limit_ratio"]
    )

    mock.risk.strategy.max_single_position_exposure_ratio = Decimal(
        mock_config_dict["risk"]["strategy"]["max_single_position_exposure_ratio"]
    )
    mock.risk.strategy.max_leverage_per_trade = Decimal(
        mock_config_dict["risk"]["strategy"]["max_leverage_per_trade"]
    )
    mock.risk.strategy.min_net_funding_differential = Decimal(
        mock_config_dict["risk"]["strategy"]["min_net_funding_differential"]
    )

    mock.risk.kelly.fraction = Decimal(mock_config_dict["risk"]["kelly"]["fraction"])
    mock.risk.kelly.min_acceptable_fraction = Decimal(
        mock_config_dict["risk"]["kelly"]["min_acceptable_fraction"]
    )
    mock.risk.kelly.max_acceptable_fraction = Decimal(
        mock_config_dict["risk"]["kelly"]["max_acceptable_fraction"]
    )
    mock.risk.kelly.min_volatility = Decimal(mock_config_dict["risk"]["kelly"]["min_volatility"])

    mock.risk.use_simple_sizing_path = False  # Default
    mock.risk.simple_sizing_method = mock_config_dict["risk"]["simple_sizing_method"]
    mock.risk.simple_fixed_fraction = Decimal(mock_config_dict["risk"]["simple_fixed_fraction"])
    mock.risk.simple_fixed_usd_size = Decimal(mock_config_dict["risk"]["simple_fixed_usd_size"])
    mock.risk.max_acceptable_rmse = Decimal(mock_config_dict["risk"]["max_acceptable_rmse"])
    mock.risk.max_acceptable_bias = Decimal(mock_config_dict["risk"]["max_acceptable_bias"])
    mock.risk.min_validation_factor = Decimal(mock_config_dict["risk"]["min_validation_factor"])
    mock.risk.min_liquidation_buffer = Decimal(mock_config_dict["risk"]["min_liquidation_buffer"])

    # Set up exchanges configuration
    mock.exchanges = {}
    for exchange_name, exchange_config in mock_config_dict["exchanges"].items():
        exchange_mock = MagicMock()
        exchange_mock.enabled = exchange_config["enabled"]
        exchange_mock.collateral_asset = exchange_config["collateral_asset"]
        exchange_mock.risk_modifier = exchange_config["risk_modifier"]
        mock.exchanges[exchange_name] = exchange_mock

    return mock


@fixture
def mock_portfolio_tracker() -> MagicMock:
    """Return mock portfolio tracker for testing."""
    tracker = MagicMock(spec=PortfolioTrackerProtocol)
    tracker.get_total_capital.return_value = Decimal("10000")
    # Update to return SpotBalance
    tracker.get_exchange_balance.return_value = SpotBalance(
        exchange="mock_exchange",
        asset="USD",
        timestamp=datetime.now(UTC),
        total_quantity=Decimal("1000"),
        available_quantity=Decimal("1000"),
    )
    tracker.get_all_positions.return_value = []  # Default to no positions
    tracker.get_current_drawdown.return_value = Decimal("0.05")  # 5% drawdown
    tracker.get_total_exposure_usd = AsyncMock(
        return_value=Decimal("0.0"),
    )  # Use AsyncMock for async method
    return tracker


@fixture
def mock_circuit_breaker_system() -> MagicMock:
    """Return mock circuit breaker system for testing."""
    system = MagicMock(spec=CircuitBreakerSystemProtocol)
    system.can_execute.return_value = (True, None)  # Default to can execute
    # Mock get_exchange_breaker to return a MagicMock with a state attribute
    mock_breaker = MagicMock()
    mock_breaker.state = BreakerState.CLOSED  # Default to closed
    system.get_exchange_breaker.return_value = mock_breaker
    return system


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
        """Handle symbol metrics side effect for testing."""
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
    mock_portfolio_tracker: PortfolioTrackerProtocol,
    mock_circuit_breaker_system: CircuitBreakerSystemProtocol,
    mock_funding_validator: FundingRateValidatorProtocol,
) -> RiskManager:
    """Create a RiskManager instance with mocked dependencies."""
    rm = RiskManager(
        app_settings=mock_config,
        portfolio_tracker=mock_portfolio_tracker,
        circuit_breaker_system=mock_circuit_breaker_system,
        funding_rate_validator=mock_funding_validator,
    )
    return rm


@fixture
def sample_opportunity_dict() -> dict[str, Any]:
    """Create sample opportunity dict for testing."""
    now = datetime.now(UTC)
    return {
        "symbol": "BTC-PERP",
        "long_exchange": "exchange_a",
        "short_exchange": "exchange_b",
        "long_price": Decimal("30000"),
        "short_price": Decimal("29900"),
        "long_funding_rate": Decimal("0.0001"),
        "short_funding_rate": Decimal("-0.00005"),
        "net_funding_differential": Decimal("0.00015"),  # (0.0001 - (-0.00005))
        "timestamp": now,
        "utility_score": Decimal("0.8"),
        "expected_profit": Decimal(
            "0.00015",
        ),  # Changed from expected_return, using NFD value for simplicity
        "basis_volatility": Decimal("0.005"),  # Example 0.5% volatility
        "confidence_score": Decimal("0.9"),
    }


@fixture
def sample_opportunity(sample_opportunity_dict: dict[str, Any]) -> ArbitrageOpportunity:
    """Create sample opportunity for testing."""
    return ArbitrageOpportunity(**sample_opportunity_dict)


def mock_get_config(key: str, default: object = None) -> object | None:
    """Mock function for Config.get."""
    config_values = {
        # Global Risk
        "risk.global.max_position_usd": "1000.0",
        "risk.global.max_total_exposure_usd": "20000.0",
        "risk.global.max_portfolio_leverage": "3.0",
        "risk.max_collateral_per_exchange": "0.8",
        "risk.max_exposure_per_asset": "0.2",
        "risk.max_exposure_per_exchange": "0.5",
        "risk.kelly_fraction": "0.5",
        "exchanges": {
            "hyperliquid": {"enabled": True, "assets": {"BTC": {"quote_asset": "USDC"}}},
            "backpack": {"enabled": True, "assets": {"BTC": {"quote_asset": "USDC"}}},
        },
        "risk.min_liquidation_buffer": "0.2",
        "risk_manager.min_exchange_balance": "10.0",
        "risk.strategy.max_single_position_exposure_ratio": "0.1",
        "risk.global.max_drawdown_limit_ratio": "0.2",
        "strategy.min_net_funding_differential": "0.0001",
        "risk.strategy.max_leverage_per_trade": "5.0",
        "risk.max_acceptable_rmse": "0.05",
        "risk.max_acceptable_bias": "0.02",
        "risk.min_validation_factor": "0.2",
        "strategy.volatility_period_days": 14,
        "risk.circuit_breaker_recovery_factor": "0.3",
        "risk.use_simple_sizing_path": False,
        "risk.simple_sizing_method": "fixed_fraction",
        "risk.simple_fixed_fraction": "0.01",
        "risk.simple_fixed_usd_size": "100",
    }
    return config_values.get(key, default)
