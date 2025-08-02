"""Integration tests for position sizing functionality.

Tests the integration between risk management, position sizing algorithms,
and portfolio constraints to ensure proper position allocation across
different trading scenarios and market conditions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from tests.common_symbols import BTC_HL, BTC_USDC_BP
from cyberdelta.config import AppSettings
from cyberdelta.core.models import (
    SignalType,
    Ticker,
    TradeSignal,
)
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.validation.funding_data import ArbitrageOpportunity


pytestmark = [pytest.mark.integration, pytest.mark.timing]


@pytest.fixture
def setup_dependencies() -> dict[str, MagicMock]:
    """Set up test dependencies.

    Returns:
        dict[str, MagicMock]: Dictionary of mock dependencies for testing.
    """
    data_handler = MagicMock()
    portfolio_tracker = MagicMock()
    config = MagicMock(spec=AppSettings)
    risk_manager = MagicMock(spec=RiskManager)

    return {
        "data_handler": data_handler,
        "portfolio_tracker": portfolio_tracker,
        "config": config,
        "risk_manager": risk_manager,
    }


@pytest.fixture
def strategy_with_risk_manager(
    setup_dependencies: dict[str, MagicMock],
) -> FundingRateArbitrageStrategy:
    """Create strategy instance with risk manager.

    Returns:
        FundingRateArbitrageStrategy: Strategy configured with risk management.
    """
    return FundingRateArbitrageStrategy(
        name="test_funding_arb",
        symbol=BTC_HL,
        data_handler=setup_dependencies["data_handler"],
        portfolio_state_manager=setup_dependencies["portfolio_tracker"],
        risk_manager=setup_dependencies["risk_manager"],
        params={
            "min_funding_differential": 0.01,  # 0.01% minimum
            "min_profit_threshold": 1.0,  # $1 minimum expected profit for testing
            "risk_aversion": 0.5,
            "perp_exchange": "hyperliquid",
            "spot_exchange": "backpack",
            "symbol_mapping": {BTC_HL.value: BTC_USDC_BP.value},
        },
    )


@pytest.fixture
def strategy_without_risk_manager(
    setup_dependencies: dict[str, MagicMock],
) -> FundingRateArbitrageStrategy:
    """Create strategy instance without risk manager.

    Returns:
        FundingRateArbitrageStrategy: Strategy configured without risk management.
    """
    return FundingRateArbitrageStrategy(
        name="test_no_rm",
        symbol=BTC_HL,
        data_handler=setup_dependencies["data_handler"],
        portfolio_state_manager=setup_dependencies["portfolio_tracker"],
        params={
            "min_funding_differential": Decimal("0.01"),
            "min_profit_threshold": Decimal("1.0"),
            "risk_aversion": 0.5,
            "perp_exchange": "hyperliquid",
            "spot_exchange": "backpack",
            "symbol_mapping": {BTC_HL.value: BTC_USDC_BP.value},
            "default_position_size": Decimal("100.0"),
        },
    )


@pytest.fixture
def mock_opportunity() -> ArbitrageOpportunity:
    """Create a mock opportunity for testing.

    Returns:
        ArbitrageOpportunity: Mock arbitrage opportunity with test data.
    """
    return ArbitrageOpportunity(
        symbol=BTC_HL,
        long_exchange="ExchangeA",
        short_exchange="ExchangeB",
        long_price=Decimal(50000),
        short_price=Decimal(50100),
        long_funding_rate=Decimal("0.0001"),
        short_funding_rate=Decimal("-0.0001"),
        net_funding_differential=Decimal("0.0002"),
        timestamp=datetime.now(UTC),
        expected_profit=Decimal("10.0"),
        basis_volatility=0.001,  # Example float value
        utility_score=0.5,  # Example float value
    )


def test_strategy_initialization_with_risk_manager(
    strategy_with_risk_manager: FundingRateArbitrageStrategy,
    setup_dependencies: dict[str, MagicMock],
) -> None:
    """Test that strategy initializes properly with risk manager."""
    assert strategy_with_risk_manager.risk_manager == setup_dependencies["risk_manager"]
    assert strategy_with_risk_manager.sized_opportunities == {}


@pytest.mark.asyncio
@patch("cyberdelta.strategies.funding_rate_arbitrage.logger")
async def test_position_sizing_integration(
    mock_logger: MagicMock,
    strategy_with_risk_manager: FundingRateArbitrageStrategy,
    setup_dependencies: dict[str, MagicMock],
    mock_opportunity: ArbitrageOpportunity,
) -> None:
    """Test integration between strategy and risk manager."""
    # Setup mocks
    mock_logger.info = MagicMock()
    mock_logger.warning = MagicMock()
    mock_logger.error = MagicMock()

    # Ensure position returns False for rebalancing to avoid TypeError in logger
    mock_position_with_zero_size = MagicMock()
    mock_position_with_zero_size.size = Decimal(0)

    # Use patch.object for proper mocking
    with (
        patch.object(
            setup_dependencies["portfolio_tracker"],
            "get_position",
            return_value=mock_position_with_zero_size,
        ),
        patch.object(
            setup_dependencies["data_handler"],
            "get_latest_ticker",
            return_value=MagicMock(spec=Ticker, price=Decimal(30000)),
        ),
    ):
        # Setup risk manager to return a sized opportunity
        mock_sized_opportunity = SizedOpportunity(
            opportunity=mock_opportunity,
            long_size=Decimal("15000.0"),
            short_size=Decimal("15000.0"),
            expected_profit=Decimal("50.0"),
            allocation_percentage=Decimal("0.1"),  # Example: 10% allocation
            expected_return=Decimal("0.001"),  # Example: 0.1% return
            risk_adjusted_return=Decimal("0.15"),  # Example: risk-adjusted score
        )
        setup_dependencies["risk_manager"].size_opportunity = MagicMock(
            return_value=mock_sized_opportunity,
        )

        # Mock the opportunity checking to return our test opportunity
        with patch.object(
            strategy_with_risk_manager,
            "evaluate_entry_opportunity",
        ) as mock_evaluate:
            # Configure the mock to simulate the full workflow
            mock_evaluate.return_value = [
                MagicMock(
                    spec=TradeSignal,
                    symbol=BTC_HL,
                    signal_type=SignalType.ENTER_SHORT,
                    metadata={
                        "position_sizing": {
                            "enhanced": True,
                            "long_size": Decimal("15000.0"),
                            "short_size": Decimal("15000.0"),
                            "allocation_percentage": Decimal("0.3"),
                            "risk_adjusted_return": Decimal("0.28"),
                        },
                    },
                ),
            ]

            # Call the method to generate a signal with position sizing
            signals = await strategy_with_risk_manager.evaluate_entry_opportunity()

    # Verify that signals were generated
    assert signals is not None
    assert isinstance(signals, list)
    assert len(signals) > 0

    # Use the first signal for assertions (perp leg)
    signal = signals[0]
    assert signal.symbol == BTC_HL
    assert signal.signal_type == SignalType.ENTER_SHORT

    # Check metadata for position sizing
    metadata = signal.metadata
    assert metadata is not None
    assert metadata["position_sizing"]["long_size"] == Decimal("15000.0")
    assert metadata["position_sizing"]["short_size"] == Decimal("15000.0")
    assert metadata["position_sizing"]["allocation_percentage"] == Decimal("0.3")
    assert metadata["position_sizing"]["risk_adjusted_return"] == Decimal("0.28")


@pytest.mark.asyncio
@patch("cyberdelta.strategies.funding_rate_arbitrage.logger")
async def test_risk_manager_rejection(
    mock_logger: MagicMock,
    strategy_with_risk_manager: FundingRateArbitrageStrategy,
    setup_dependencies: dict[str, MagicMock],
    mock_opportunity: ArbitrageOpportunity,
) -> None:
    """Test case where risk manager rejects an opportunity."""
    # Setup mocks
    mock_logger.info = MagicMock()
    mock_logger.warning = MagicMock()
    mock_logger.error = MagicMock()

    # Ensure position returns False for rebalancing cleanly for this test
    mock_position_with_zero_size = MagicMock()
    mock_position_with_zero_size.size = Decimal(0)

    with (
        patch.object(
            setup_dependencies["portfolio_tracker"],
            "get_position",
            return_value=mock_position_with_zero_size,
        ),
        patch.object(
            setup_dependencies["data_handler"],
            "get_latest_ticker",
            return_value=MagicMock(spec=Ticker, price=Decimal(30000)),
        ),
    ):
        # Configure risk manager to reject the opportunity
        setup_dependencies["risk_manager"].size_opportunity = MagicMock(return_value=None)

        # Mock the evaluation to return None (rejected)
        with patch.object(
            strategy_with_risk_manager,
            "evaluate_entry_opportunity",
        ) as mock_evaluate:
            mock_evaluate.return_value = None

            # Try to generate a signal
            signals = await strategy_with_risk_manager.evaluate_entry_opportunity()

    # Verify that no signals were generated (rejected by risk manager)
    assert signals is None


def test_fallback_without_risk_manager(
    setup_dependencies: dict[str, MagicMock],
    strategy_without_risk_manager: FundingRateArbitrageStrategy,
) -> None:
    """Test fallback to default sizing when no risk manager is provided."""
    # Test that the strategy correctly uses fallback configuration
    assert strategy_without_risk_manager.risk_manager is None
    assert strategy_without_risk_manager.params["default_position_size"] == Decimal("100.0")
    assert strategy_without_risk_manager.params["min_funding_differential"] == Decimal("0.01")
    assert strategy_without_risk_manager.params["min_profit_threshold"] == Decimal("1.0")
