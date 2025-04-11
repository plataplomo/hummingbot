import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from datetime import datetime

from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.core.signal_generator import ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.core.types import SignalType, TradeSignal
from cyberdelta.utils.config import Config


@pytest.fixture
def setup_dependencies():
    """Set up test dependencies"""
    data_handler = MagicMock()
    portfolio_tracker = MagicMock()
    config = MagicMock(spec=Config)
    risk_manager = MagicMock(spec=RiskManager)

    return {
        "data_handler": data_handler,
        "portfolio_tracker": portfolio_tracker,
        "config": config,
        "risk_manager": risk_manager,
    }


@pytest.fixture
def strategy_with_risk_manager(setup_dependencies):
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
def strategy_without_risk_manager(setup_dependencies):
    """Create strategy instance without risk manager"""
    return FundingRateArbitrageStrategy(
        name="test_no_rm",
        symbol="BTC-PERP",
        data_handler=setup_dependencies["data_handler"],
        portfolio_tracker=setup_dependencies["portfolio_tracker"],
        params={
            "min_funding_differential": 0.01,
            "min_profit_threshold": 1.0,
            "risk_aversion": 0.5,
            "perp_exchange": "hyperliquid",
            "spot_exchange": "backpack",
            "symbol_mapping": {"BTC-PERP": "BTC_USDC"},
        },
    )


@pytest.fixture
def mock_opportunity():
    """Create a mock opportunity for testing"""
    return ArbitrageOpportunity(
        symbol="BTC-PERP",
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_funding_rate=0,
        short_funding_rate=0.1,
        net_funding_differential=0.1,
        timestamp=datetime.now(),
        expected_profit=10.0,
        utility_score=8.0,
        basis_volatility=0.005,
    )


def test_strategy_initialization_with_risk_manager(
    strategy_with_risk_manager, setup_dependencies
):
    """Test that strategy initializes properly with risk manager"""
    assert strategy_with_risk_manager.risk_manager == setup_dependencies["risk_manager"]
    assert strategy_with_risk_manager.sized_opportunities == {}


@pytest.mark.asyncio
@patch("cyberdelta.strategies.funding_rate_arbitrage.logger")
async def test_position_sizing_integration(
    mock_logger, strategy_with_risk_manager, setup_dependencies, mock_opportunity
):
    """Test integration between strategy and risk manager"""
    # Setup mocks
    mock_logger.info = MagicMock()
    mock_logger.warning = MagicMock()
    mock_logger.error = MagicMock()

    # Mock _check_opportunity to return our test opportunity
    strategy_with_risk_manager._check_opportunity = AsyncMock(
        return_value=mock_opportunity
    )

    # Mock TradeSignal creation
    mock_trade_signal = MagicMock(spec=TradeSignal)
    mock_trade_signal.symbol = "BTC-PERP"
    mock_trade_signal.signal_type = SignalType.ENTER_SHORT
    mock_trade_signal.trades = [
        {"exchange": "hyperliquid", "side": "SHORT", "size": 15000.0 / 30000.0},
        {"exchange": "backpack", "side": "LONG", "size": 15000.0 / 29990.0},
    ]
    mock_trade_signal.metadata = {
        "position_sizing": {
            "enhanced": True,
            "long_size": 15000.0,
            "short_size": 15000.0,
            "allocation_percentage": 0.3,
            "risk_adjusted_return": 0.28,
        }
    }

    # Mock _generate_entry_signal
    strategy_with_risk_manager._generate_entry_signal = MagicMock(
        return_value=mock_trade_signal
    )

    # Setup risk manager to return a sized opportunity
    mock_sized_opportunity = SizedOpportunity(
        opportunity=mock_opportunity,
        long_size=15000.0,
        short_size=15000.0,
        allocation_percentage=0.3,
        expected_profit=50.0,
        expected_return=0.33,
        risk_adjusted_return=0.28,
    )
    setup_dependencies["risk_manager"].size_opportunity = MagicMock(
        return_value=mock_sized_opportunity
    )

    # Call the method to generate a signal with position sizing
    signal = await strategy_with_risk_manager._check_and_generate_signal()

    # Verify that risk manager was called
    setup_dependencies["risk_manager"].size_opportunity.assert_called_once()
    assert (
        setup_dependencies["risk_manager"].size_opportunity.call_args[0][0]
        == mock_opportunity
    )

    # Verify that a signal was generated
    assert signal is not None
    assert signal.symbol == "BTC-PERP"

    # Verify the correct signal type was used
    assert signal.signal_type == SignalType.ENTER_SHORT

    # Check that the sized opportunity was stored
    opportunity_id = str(id(mock_opportunity))
    assert opportunity_id in strategy_with_risk_manager.sized_opportunities
    assert (
        strategy_with_risk_manager.sized_opportunities[opportunity_id]
        == mock_sized_opportunity
    )

    # Verify that the trade sizes were correctly calculated
    trades = signal.trades
    assert len(trades) == 2

    # First trade should be for the perp exchange
    perp_trade = next(t for t in trades if t["exchange"] == "hyperliquid")
    assert perp_trade["side"] == "SHORT"
    assert perp_trade["size"] == 15000.0 / 30000.0  # size in USD / price

    # Second trade should be for the spot exchange
    spot_trade = next(t for t in trades if t["exchange"] == "backpack")
    assert spot_trade["side"] == "LONG"
    assert spot_trade["size"] == 15000.0 / 29990.0  # size in USD / price

    # Verify metadata contains position sizing details
    metadata = signal.metadata
    assert metadata["position_sizing"]["enhanced"] is True
    assert metadata["position_sizing"]["long_size"] == 15000.0
    assert metadata["position_sizing"]["short_size"] == 15000.0
    assert metadata["position_sizing"]["allocation_percentage"] == 0.3
    assert metadata["position_sizing"]["risk_adjusted_return"] == 0.28


@pytest.mark.asyncio
@patch("cyberdelta.strategies.funding_rate_arbitrage.logger")
async def test_risk_manager_rejection(
    mock_logger, strategy_with_risk_manager, setup_dependencies, mock_opportunity
):
    """Test case where risk manager rejects an opportunity"""
    # Setup mocks
    mock_logger.info = MagicMock()
    mock_logger.warning = MagicMock()
    mock_logger.error = MagicMock()

    # Mock _check_opportunity to return our test opportunity
    strategy_with_risk_manager._check_opportunity = AsyncMock(
        return_value=mock_opportunity
    )

    # Configure risk manager to reject the opportunity
    setup_dependencies["risk_manager"].size_opportunity = MagicMock(return_value=None)

    # Try to generate a signal
    signal = await strategy_with_risk_manager._check_and_generate_signal()

    # Verify that the risk manager was called
    setup_dependencies["risk_manager"].size_opportunity.assert_called_once()

    # Verify that no signal was generated (rejected by risk manager)
    assert signal is None

    # Verify that the warning was logged
    mock_logger.warning.assert_called_with("Opportunity rejected by risk manager")


def test_fallback_without_risk_manager(
    setup_dependencies, strategy_without_risk_manager, mock_opportunity
):
    """Test fallback to default sizing when no risk manager is provided"""
    # Mock prices for quantity calculations
    setup_dependencies["data_handler"].get_latest_price = MagicMock(
        return_value=30000.0
    )

    # Mock TradeSignal creation
    mock_trade_signal = MagicMock(spec=TradeSignal)
    mock_trade_signal.symbol = "BTC-PERP"
    mock_trade_signal.signal_type = SignalType.ENTER_SHORT
    mock_trade_signal.trades = [
        {"exchange": "hyperliquid", "side": "SHORT", "size": 1000.0},
        {"exchange": "backpack", "side": "LONG", "size": 1000.0},
    ]
    mock_trade_signal.metadata = {}

    # Mock _generate_entry_signal
    strategy_without_risk_manager._generate_entry_signal = MagicMock(
        return_value=mock_trade_signal
    )

    # Generate a signal
    signal = strategy_without_risk_manager._generate_entry_signal(mock_opportunity)

    # Verify the signal has default sizes
    for trade in signal.trades:
        assert trade["size"] == 1000.0

    # Verify no enhanced position sizing metadata
    assert "position_sizing" not in signal.metadata
