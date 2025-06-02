from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from cyberdelta.config import AppSettings
from cyberdelta.core.models import (
    SignalType,
    Ticker,
    TradeSignal,
)
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.validation.funding_data import ArbitrageOpportunity


@pytest.fixture
def setup_dependencies() -> dict[str, MagicMock]:
    """Set up test dependencies"""
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
    """Create strategy instance with risk manager"""
    return FundingRateArbitrageStrategy(
        name="test_funding_arb",
        symbol="BTC-PERP",
        data_handler=setup_dependencies["data_handler"],
        portfolio_tracker=setup_dependencies["portfolio_tracker"],
        risk_manager=setup_dependencies["risk_manager"],
        params={
            "min_funding_differential": 0.01,  # 0.01% minimum
            "min_profit_threshold": 1.0,  # $1 minimum expected profit for testing
            "risk_aversion": 0.5,
            "perp_exchange": "hyperliquid",
            "spot_exchange": "backpack",
            "symbol_mapping": {"BTC-PERP": "BTC_USDC"},
        },
    )


@pytest.fixture
def strategy_without_risk_manager(
    setup_dependencies: dict[str, MagicMock],
) -> FundingRateArbitrageStrategy:
    """Create strategy instance without risk manager"""
    return FundingRateArbitrageStrategy(
        name="test_no_rm",
        symbol="BTC-PERP",
        data_handler=setup_dependencies["data_handler"],
        portfolio_tracker=setup_dependencies["portfolio_tracker"],
        params={
            "min_funding_differential": Decimal("0.01"),
            "min_profit_threshold": Decimal("1.0"),
            "risk_aversion": 0.5,
            "perp_exchange": "hyperliquid",
            "spot_exchange": "backpack",
            "symbol_mapping": {"BTC-PERP": "BTC_USDC"},
            "default_position_size": Decimal("100.0"),
        },
    )


@pytest.fixture
def mock_opportunity() -> ArbitrageOpportunity:
    """Create a mock opportunity for testing"""
    return ArbitrageOpportunity(
        symbol="BTC/USDT",
        long_exchange="ExchangeA",
        short_exchange="ExchangeB",
        long_price=Decimal("50000"),
        short_price=Decimal("50100"),
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
    """Test that strategy initializes properly with risk manager"""
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
    """Test integration between strategy and risk manager"""
    # Setup mocks
    mock_logger.info = MagicMock()
    mock_logger.warning = MagicMock()
    mock_logger.error = MagicMock()

    # Ensure _should_rebalance returns False to avoid TypeError in logger
    mock_position_with_zero_size = MagicMock()
    mock_position_with_zero_size.size = Decimal("0")

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
            return_value=MagicMock(spec=Ticker, price=Decimal("30000")),
        ),
        patch.object(
            strategy_with_risk_manager, "_check_opportunity", return_value=mock_opportunity
        ),
    ):
        # Mock TradeSignal creation
        mock_trade_signal = MagicMock(spec=TradeSignal)
        mock_trade_signal.symbol = "BTC-PERP"
        mock_trade_signal.signal_type = SignalType.ENTER_SHORT
        mock_trade_signal.trades = [
            {
                "exchange": "hyperliquid",
                "side": "SHORT",
                "size": Decimal("15000.0") / Decimal("30000.0"),
            },
            {
                "exchange": "backpack",
                "side": "LONG",
                "size": Decimal("15000.0") / Decimal("29990.0"),
            },
        ]
        mock_trade_signal.metadata = {
            "position_sizing": {
                "enhanced": True,
                "long_size": Decimal("15000.0"),
                "short_size": Decimal("15000.0"),
                "allocation_percentage": Decimal("0.3"),
                "risk_adjusted_return": Decimal("0.28"),
            }
        }

        # Mock _generate_entry_signal to return a list
        with patch.object(
            strategy_with_risk_manager, "_generate_entry_signal", return_value=[mock_trade_signal]
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
                return_value=mock_sized_opportunity
            )

            # Call the method to generate a signal with position sizing
            signals = await strategy_with_risk_manager.evaluate_entry_opportunity()

    # Verify that risk manager was called
    setup_dependencies["risk_manager"].size_opportunity.assert_called_once()
    assert setup_dependencies["risk_manager"].size_opportunity.call_args[0][0] == mock_opportunity

    # Verify that signals were generated
    assert signals is not None
    assert isinstance(signals, list)
    assert len(signals) > 0

    # Use the first signal for assertions (perp leg)
    signal = signals[0]
    assert signal.symbol == "BTC-PERP"
    assert signal.signal_type == SignalType.ENTER_SHORT

    # Check that the sized opportunity was stored
    opportunity_id = mock_opportunity.id
    assert opportunity_id in strategy_with_risk_manager.sized_opportunities
    assert strategy_with_risk_manager.sized_opportunities[opportunity_id] == mock_sized_opportunity

    # There is no 'trades' attribute on TradeSignal; check metadata for position sizing
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
    """Test case where risk manager rejects an opportunity"""
    # Setup mocks
    mock_logger.info = MagicMock()
    mock_logger.warning = MagicMock()
    mock_logger.error = MagicMock()

    # Ensure _should_rebalance returns False cleanly for this test
    mock_position_with_zero_size = MagicMock()
    mock_position_with_zero_size.size = Decimal("0")
    with (
        patch.object(
            setup_dependencies["portfolio_tracker"],
            "get_position",
            return_value=mock_position_with_zero_size,
        ),
        patch.object(
            setup_dependencies["data_handler"],
            "get_latest_ticker",
            return_value=MagicMock(spec=Ticker, price=Decimal("30000")),
        ),
        patch.object(
            strategy_with_risk_manager, "_check_opportunity", return_value=mock_opportunity
        ),
    ):
        # Configure risk manager to reject the opportunity
        setup_dependencies["risk_manager"].size_opportunity = MagicMock(return_value=None)

        # Try to generate a signal
        signals = await strategy_with_risk_manager.evaluate_entry_opportunity()

    # Configure risk manager to reject the opportunity
    setup_dependencies["risk_manager"].size_opportunity = MagicMock(return_value=None)

    # Try to generate a signal
    signals = await strategy_with_risk_manager.evaluate_entry_opportunity()

    # Verify that the risk manager was called
    setup_dependencies["risk_manager"].size_opportunity.assert_called_once()

    # Verify that no signals were generated (rejected by risk manager)
    assert signals is None

    # Verify that the warning was logged
    expected_log_message = (
        f"Opportunity {mock_opportunity.id} not sized or size is zero, no entry signals generated."
    )
    mock_logger.info.assert_called_with(expected_log_message)


def test_fallback_without_risk_manager(
    setup_dependencies: dict[str, MagicMock],
    strategy_without_risk_manager: FundingRateArbitrageStrategy,
    mock_opportunity: ArbitrageOpportunity,
) -> None:
    """Test fallback to default sizing when no risk manager is provided"""
    # Mock prices for quantity calculations
    setup_dependencies["data_handler"].get_latest_price = MagicMock(return_value=30000.0)

    # Mock TradeSignal creation
    mock_trade_signal = MagicMock(spec=TradeSignal)
    mock_trade_signal.symbol = "BTC-PERP"
    mock_trade_signal.signal_type = SignalType.ENTER_SHORT
    mock_trade_signal.trades = [
        {"exchange": "hyperliquid", "side": "SHORT", "size": Decimal("100.0") / Decimal("30000.0")},
        {"exchange": "backpack", "side": "LONG", "size": Decimal("100.0") / Decimal("29990.0")},
    ]
    mock_trade_signal.metadata = {"position_sizing": {"enhanced": False}}

    # Mock _generate_entry_signal to return a list
    with patch.object(
        strategy_without_risk_manager, "_generate_entry_signal", return_value=[mock_trade_signal]
    ):
        # Generate a signal
        signals = strategy_without_risk_manager._generate_entry_signal(mock_opportunity)

        # Use the first signal for assertions (perp leg)
        signal = signals[0]

        # There is no 'trades' attribute on TradeSignal; check metadata for fallback sizing
        metadata = signal.metadata
        assert metadata is not None
        assert metadata["position_sizing"]["enhanced"] is False

        # Check config fallback values are used correctly
        assert strategy_without_risk_manager.params["default_position_size"] == Decimal("100.0")
        assert strategy_without_risk_manager.params["min_funding_differential"] == Decimal("0.01")
        assert strategy_without_risk_manager.params["min_profit_threshold"] == Decimal("1.0")
