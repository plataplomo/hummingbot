from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytz

from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.models import OrderSide, SignalType, TradeSignal
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.strategy_manager import StrategyManager
from tests.unit.mocks.mock_strategy import MockStrategy

# Define UTC timezone
UTC = pytz.UTC


# Mock configuration object
@pytest.fixture
def mock_config_dict() -> dict[str, Any]:
    """Fixture for a mock config dictionary."""
    return {
        "strategy_paths": [],
        "strategies": {},
    }


# Mock dependencies needed by StrategyManager
@pytest.fixture
def mock_execution_handler() -> MagicMock:
    return MagicMock(spec=ExecutionHandler)


@pytest.fixture
def mock_portfolio_tracker() -> MagicMock:
    return MagicMock(spec=PortfolioTracker)


@pytest.fixture
def mock_risk_manager() -> MagicMock:
    return MagicMock(spec=RiskManager)


@pytest.fixture
def mock_signal_queue() -> MagicMock:
    return MagicMock(spec=PrioritySignalQueue)


@patch("cyberdelta.core.strategy_manager.importlib.import_module")
@patch("cyberdelta.core.strategy_manager.inspect.getmembers")
def test_register_strategy(
    mock_getmembers: MagicMock,
    mock_import_module: MagicMock,
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test registering a new strategy."""
    config_data_with_strategy = mock_config_dict
    config_data_with_strategy["strategy_paths"] = ["tests.unit.mocks.mock_strategy.MockStrategy"]
    strategy_manager_for_test = StrategyManager(
        config=config_data_with_strategy,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(name="MockStrategy", symbol="MOCK/SYMBOL")
    strategy_manager_for_test.register_strategy(mock_strategy_instance)
    assert "MockStrategy" in strategy_manager_for_test.strategies


def test_unregister_strategy(
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test unregistering an existing strategy."""
    strategy_manager_for_test = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(name="MockStrategy", symbol="MOCK/SYMBOL")
    strategy_manager_for_test.strategies = {"MockStrategy": mock_strategy_instance}
    assert "MockStrategy" in strategy_manager_for_test.strategies
    strategy_manager_for_test.unregister_strategy("MockStrategy")
    assert "MockStrategy" not in strategy_manager_for_test.strategies


def test_enable_disable_strategy(
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test enabling and disabling a strategy."""
    strategy_manager_for_test = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(name="TestStrategy", symbol="MOCK/SYMBOL", enabled=False)
    strategy_manager_for_test.strategies = {"TestStrategy": mock_strategy_instance}

    strategy_manager_for_test.enable_strategy("TestStrategy")
    assert "TestStrategy" in strategy_manager_for_test.enabled_strategies
    assert strategy_manager_for_test.strategies["TestStrategy"].enabled

    strategy_manager_for_test.disable_strategy("TestStrategy")
    assert "TestStrategy" not in strategy_manager_for_test.enabled_strategies
    assert not strategy_manager_for_test.strategies["TestStrategy"].enabled


def test_get_strategies_for_symbol(
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test retrieving strategies relevant to a symbol."""
    strategy_manager_for_test = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_btc_instance = MockStrategy(name="BTCStrategy", symbol="BTC/USDT", enabled=True)
    mock_strategy_eth_instance = MockStrategy(name="ETHStrategy", symbol="ETH/USDT", enabled=True)
    mock_strategy_disabled_instance = MockStrategy(
        name="DisabledBTCStrategy", symbol="BTC/USDT", enabled=False
    )

    strategy_manager_for_test.strategies = {
        "BTCStrategy": mock_strategy_btc_instance,
        "ETHStrategy": mock_strategy_eth_instance,
        "DisabledBTCStrategy": mock_strategy_disabled_instance,
    }
    strategy_manager_for_test.enabled_strategies = {"BTCStrategy", "ETHStrategy"}

    btc_strategies = strategy_manager_for_test.get_strategies_for_symbol("BTC/USDT")
    eth_strategies = strategy_manager_for_test.get_strategies_for_symbol("ETH/USDT")
    sol_strategies = strategy_manager_for_test.get_strategies_for_symbol("SOL/USDT")

    assert len(btc_strategies) == 1
    assert mock_strategy_btc_instance in btc_strategies
    assert len(eth_strategies) == 1
    assert mock_strategy_eth_instance in eth_strategies
    assert len(sol_strategies) == 0


@patch("cyberdelta.core.strategy_manager.asyncio.gather")
@pytest.mark.asyncio
async def test_start_stop_all(
    mock_gather: MagicMock,
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test starting and stopping all strategies."""
    strategy_manager_for_test = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy1_instance = MockStrategy(name="Strategy1", symbol="S1/USDT")
    mock_strategy2_instance = MockStrategy(name="Strategy2", symbol="S2/USDT")

    strategy_manager_for_test.strategies = {
        "Strategy1": mock_strategy1_instance,
        "Strategy2": mock_strategy2_instance,
    }
    strategy_manager_for_test.enable_strategy("Strategy1")
    strategy_manager_for_test.enable_strategy("Strategy2")

    strategy_manager_for_test.start_all()
    assert strategy_manager_for_test.strategies["Strategy1"].enabled
    assert strategy_manager_for_test.strategies["Strategy2"].enabled
    assert "Strategy1" in strategy_manager_for_test.enabled_strategies
    assert "Strategy2" in strategy_manager_for_test.enabled_strategies

    mock_gather.reset_mock()

    strategy_manager_for_test.stop_all()
    assert not strategy_manager_for_test.strategies["Strategy1"].enabled
    assert not strategy_manager_for_test.strategies["Strategy2"].enabled
    assert len(strategy_manager_for_test.enabled_strategies) == 0


@patch("cyberdelta.core.strategy_manager.asyncio.create_task")
@pytest.mark.asyncio
async def test_process_market_data(
    mock_create_task: MagicMock,
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test processing market data and generating signals."""
    strategy_manager_for_test = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(name="TestStrategy", symbol="BTC/USDT", enabled=True)

    mock_signal = TradeSignal(
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        metadata={"utility_score": 0.8, "origin_strategy": mock_strategy_instance.name},
        exchange="mock_exchange",
    )
    strategy_manager_for_test.register_strategy(mock_strategy_instance)
    strategy_manager_for_test.enable_strategy("TestStrategy")

    market_data = Candle(
        symbol="BTC/USDT",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("49990"),
        high=Decimal("50001"),
        low=Decimal("49980"),
        close=Decimal("49999"),
        volume=Decimal("10"),
    )

    with patch.object(
        mock_strategy_instance,
        "process_data",
        new_callable=AsyncMock,
        return_value=[mock_signal],
    ) as mock_process_data:
        await strategy_manager_for_test.process_market_data(market_data)

    mock_process_data.assert_called_once_with(market_data)
    mock_signal_queue.add_signal.assert_called_once_with(mock_signal)


@pytest.mark.asyncio
async def test_process_market_data_no_enabled_strategies(
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test processing market data when no strategies are enabled."""
    strategy_manager_for_test = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(name="TestStrategy", symbol="BTC/USDT", enabled=False)
    strategy_manager_for_test.register_strategy(mock_strategy_instance)

    market_data = Candle(
        symbol="BTC/USDT",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("49990"),
        high=Decimal("50001"),
        low=Decimal("49980"),
        close=Decimal("49999"),
        volume=Decimal("10"),
    )

    with patch.object(
        mock_strategy_instance, "process_data", new_callable=AsyncMock
    ) as mock_process_data:
        await strategy_manager_for_test.process_market_data(market_data)

    mock_process_data.assert_not_called()
    mock_signal_queue.add_signal.assert_not_called()


@patch("cyberdelta.core.strategy_manager.asyncio.create_task")
@pytest.mark.asyncio
async def test_process_market_data_exception(
    mock_create_task: MagicMock,
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test that exceptions during signal processing are handled."""
    strategy_manager_for_test = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(name="TestStrategy", symbol="BTC/USDT", enabled=True)
    strategy_manager_for_test.register_strategy(mock_strategy_instance)
    strategy_manager_for_test.enable_strategy("TestStrategy")

    market_data = Candle(
        symbol="BTC/USDT",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("49990"),
        high=Decimal("50001"),
        low=Decimal("49980"),
        close=Decimal("49999"),
        volume=Decimal("10"),
    )

    with patch.object(
        mock_strategy_instance,
        "process_data",
        new_callable=AsyncMock,
        side_effect=ValueError("Test Error"),
    ) as mock_process_data:
        await strategy_manager_for_test.process_market_data(market_data)

    mock_process_data.assert_called_once_with(market_data)
    mock_signal_queue.add_signal.assert_not_called()


@patch("cyberdelta.core.strategy_manager.logger")
@pytest.mark.asyncio
async def test_process_market_data_signal_handler_raises(
    mock_logger: MagicMock,
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test exception handling when signal_queue.add_signal raises an error."""
    strategy_manager_for_test = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(name="TestStrategy", symbol="BTC/USDT", enabled=True)
    mock_signal = TradeSignal(
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        metadata={"utility_score": 0.8, "origin_strategy": mock_strategy_instance.name},
        exchange="mock_exchange",
    )
    strategy_manager_for_test.register_strategy(mock_strategy_instance)
    strategy_manager_for_test.enable_strategy("TestStrategy")

    mock_signal_queue.add_signal.side_effect = ValueError("Signal Queue Error")

    market_data = Candle(
        symbol="BTC/USDT",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("49990"),
        high=Decimal("50001"),
        low=Decimal("49980"),
        close=Decimal("49999"),
        volume=Decimal("10"),
    )

    with patch.object(
        mock_strategy_instance,
        "process_data",
        new_callable=AsyncMock,
        return_value=[mock_signal],
    ) as mock_process_data:
        await strategy_manager_for_test.process_market_data(market_data)

    mock_process_data.assert_called_once_with(market_data)
    mock_signal_queue.add_signal.assert_called_once_with(mock_signal)
    mock_logger.error.assert_any_call(
        "Signal queue failed to add signal: Signal Queue Error", exc_info=True
    )


@pytest.mark.asyncio
async def test_process_market_data_duplicate_signals(
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test passing duplicate signals from strategy.
    Note: StrategyManager itself doesn't deduplicate; this is likely handled
    downstream (e.g., SignalQueue, ExecutionHandler).
    """
    strategy_manager_for_test = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(name="TestStrategy", symbol="BTC/USDT", enabled=True)
    signal1 = TradeSignal(
        signal_id="dup_signal_123",
        symbol="BTC/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000"),
        quantity=Decimal("1"),
        metadata={"utility_score": 0.8, "origin_strategy": mock_strategy_instance.name},
        exchange="mock_exchange",
    )
    strategy_manager_for_test.register_strategy(mock_strategy_instance)
    strategy_manager_for_test.enable_strategy("TestStrategy")

    market_data = Candle(
        symbol="BTC/USDT",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("49990"),
        high=Decimal("50001"),
        low=Decimal("49980"),
        close=Decimal("49999"),
        volume=Decimal("10"),
    )

    with patch.object(
        mock_strategy_instance,
        "process_data",
        new_callable=AsyncMock,
        return_value=[signal1, signal1],
    ) as mock_process_data:
        await strategy_manager_for_test.process_market_data(market_data)

    mock_process_data.assert_called_once_with(market_data)
    assert mock_signal_queue.add_signal.call_count == 2
    mock_signal_queue.add_signal.assert_any_call(signal1)


@pytest.mark.asyncio
async def test_process_market_data_mixed_valid_invalid(
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test processing a mix of valid and invalid signals."""
    strategy_manager_for_test = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(name="TestStrategy", symbol="ETH/USDT", enabled=True)
    valid_signal = TradeSignal(
        signal_id="valid_sig_789",
        symbol="ETH/USDT",
        signal_type=SignalType.ENTER_SHORT,
        side=OrderSide.SELL,
        price=Decimal("3000"),
        quantity=Decimal("5"),
        metadata={"utility_score": 0.9, "origin_strategy": mock_strategy_instance.name},
        exchange="another_exchange",
    )
    invalid_signal_object = {"data": "not a trade signal"}

    strategy_manager_for_test.register_strategy(mock_strategy_instance)
    strategy_manager_for_test.enable_strategy("TestStrategy")

    market_data = Candle(
        symbol="ETH/USDT",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("2990"),
        high=Decimal("3001"),
        low=Decimal("2980"),
        close=Decimal("2999"),
        volume=Decimal("12"),
    )

    with (
        patch.object(
            mock_strategy_instance,
            "process_data",
            new_callable=AsyncMock,
            return_value=[valid_signal, invalid_signal_object],
        ) as mock_process_data,
        patch("cyberdelta.core.strategy_manager.logger") as mock_logger_local,
    ):
        await strategy_manager_for_test.process_market_data(market_data)

    mock_process_data.assert_called_once_with(market_data)
    mock_signal_queue.add_signal.assert_called_once_with(valid_signal)

    invalid_msg = (
        f"Non-TradeSignal object returned by {mock_strategy_instance.name}: {invalid_signal_object}"
    )
    mock_logger_local.error.assert_any_call(invalid_msg)


@pytest.mark.asyncio
async def test_signal_handler_risk_manager_exception(
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test process_market_data when risk_manager.size_signal raises."""
    strategy_manager = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(name="TestStrategyRMEx", symbol="SYM/USDT", enabled=True)
    mock_signal = TradeSignal(
        signal_id="test_signal_rm_ex",
        symbol="SYM/USDT",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("100"),
        quantity=Decimal("1"),
        exchange="test_exchange",
    )
    strategy_manager.register_strategy(mock_strategy_instance)
    strategy_manager.enable_strategy("TestStrategyRMEx")

    mock_risk_manager.size_signal.side_effect = ValueError("Risk Eval Error")

    market_data = Candle(
        symbol="SYM/USDT",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("99"),
        high=Decimal("101"),
        low=Decimal("98"),
        close=Decimal("100"),
        volume=Decimal("100"),
    )

    with (
        patch.object(
            mock_strategy_instance,
            "process_data",
            new_callable=AsyncMock,
            return_value=[mock_signal],
        ) as mock_process_data,
        patch("cyberdelta.core.strategy_manager.logger") as mock_logger,
    ):
        await strategy_manager.process_market_data(market_data)

    mock_process_data.assert_called_once_with(market_data)
    mock_risk_manager.size_signal.assert_called_once_with(mock_signal)
    mock_signal_queue.add_signal.assert_not_called()
    error_msg = (
        f"Error during signal processing (post-generation) in "
        f"{mock_strategy_instance.name}: Risk Eval Error"
    )
    mock_logger.error.assert_any_call(error_msg, exc_info=True)


@pytest.mark.asyncio
async def test_signal_handler_update_historical_data_exception(
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test process_market_data when strategy.update_historical_data raises an exception."""
    strategy_manager = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(
        name="TestStrategyHistEx", symbol="SYM/USDT", enabled=True
    )
    strategy_manager.register_strategy(mock_strategy_instance)
    strategy_manager.enable_strategy("TestStrategyHistEx")

    market_data = Candle(
        symbol="SYM/USDT",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("99"),
        high=Decimal("101"),
        low=Decimal("98"),
        close=Decimal("100"),
        volume=Decimal("100"),
    )

    with (
        patch.object(
            mock_strategy_instance,
            "update_historical_data",
            side_effect=ValueError("Hist Data Error"),
        ) as mock_update_hist,
        patch.object(
            mock_strategy_instance, "process_data", new_callable=AsyncMock
        ) as mock_process_data,
    ):
        with pytest.raises(ValueError, match="Hist Data Error"):
            await strategy_manager.process_market_data(market_data)

    mock_update_hist.assert_called_once_with(market_data)
    mock_process_data.assert_not_called()
    mock_signal_queue.add_signal.assert_not_called()


@pytest.mark.asyncio
async def test_process_market_data_malformed_signal(
    mock_config_dict: dict[str, Any],
    mock_execution_handler: MagicMock,
    mock_portfolio_tracker: MagicMock,
    mock_risk_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test process_market_data when strategy returns malformed signal data."""
    strategy_manager = StrategyManager(
        config=mock_config_dict,
        execution_handler=mock_execution_handler,
        portfolio_tracker=mock_portfolio_tracker,
        risk_manager=mock_risk_manager,
        signal_queue=mock_signal_queue,
    )
    mock_strategy_instance = MockStrategy(
        name="TestStrategyMalformed", symbol="SYM/USDT", enabled=True
    )
    malformed_signal_dict: dict[str, Any] = {
        "signal_id": "malformed_123",
        "symbol": "SYM/USDT",
        "signal_type": SignalType.ENTER_LONG,
        "side": OrderSide.BUY,
        "quantity": Decimal("1"),
        "exchange": "test",
        "metadata": {},
    }

    strategy_manager.register_strategy(mock_strategy_instance)
    strategy_manager.enable_strategy("TestStrategyMalformed")

    market_data = Candle(
        symbol="SYM/USDT",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("99"),
        high=Decimal("101"),
        low=Decimal("98"),
        close=Decimal("100"),
        volume=Decimal("100"),
    )

    with (
        patch.object(
            mock_strategy_instance,
            "process_data",
            new_callable=AsyncMock,
            return_value=[malformed_signal_dict],
        ) as mock_process_data,
        patch("cyberdelta.core.strategy_manager.logger") as mock_logger,
    ):
        await strategy_manager.process_market_data(market_data)

    mock_process_data.assert_called_once_with(market_data)
    mock_signal_queue.add_signal.assert_not_called()
    error_msg = (
        f"Non-TradeSignal object returned by {mock_strategy_instance.name}: {malformed_signal_dict}"
    )
    mock_logger.error.assert_any_call(error_msg)


# Removed the __main__ block as tests are run via pytest
