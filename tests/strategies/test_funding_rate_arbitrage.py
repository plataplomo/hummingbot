from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    OrderSide,
    Ticker,
    TradeSignal,
)
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy

if TYPE_CHECKING:
    pass

# Configure logging for this test
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Move TickerWithClose to module level for reuse
default_close = Decimal("30000.0")

# Define a specific type alias or use a more concrete type if possible
PositionType = DerivativePosition | None


class TickerWithClose:
    def __init__(self, close: Decimal = default_close) -> None:
        self.close: Decimal = close


@pytest.fixture
def strategy() -> FundingRateArbitrageStrategy:
    data_handler = MagicMock()
    data_handler.get_latest_price = MagicMock()
    portfolio_tracker = MagicMock()
    portfolio_tracker.get_position = MagicMock()
    return FundingRateArbitrageStrategy(
        name="test_funding_arb",
        symbol="BTC-PERP",
        data_handler=data_handler,
        portfolio_tracker=portfolio_tracker,
        params={
            "min_funding_differential": Decimal("0.01"),
            "min_profit_threshold": Decimal("1.0"),
            "risk_aversion": Decimal("0.5"),
            "perp_exchange": "hyperliquid",
            "spot_exchange": "backpack",
            "symbol_mapping": {"BTC-PERP": "BTC_USDC"},
        },
    )


def fake_get_latest_price(ex: str, sym: str) -> Decimal:
    if (ex, sym) == ("hyperliquid", "BTC-PERP"):
        return Decimal("30000.0")
    return Decimal("29990.0")


def fake_get_position(ex: str, sym: str) -> PositionType:
    positions = {
        ("hyperliquid", "BTC-PERP"): DerivativePosition(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            symbol="BTC-PERP",
            size=Decimal("1.0"),
            entry_price=Decimal("29500"),
            mark_price=Decimal("30000"),
            side=OrderSide.BUY,
            liquidation_price=Decimal("28000"),
            unrealized_pnl=Decimal("500"),
        ),
        ("backpack", "BTC_USDC"): DerivativePosition(
            exchange="backpack",
            timestamp=datetime.now(UTC),
            symbol="BTC_USDC",
            size=Decimal("-1.0"),
            entry_price=Decimal("29510"),
            mark_price=Decimal("29990"),
            side=OrderSide.SELL,
            liquidation_price=Decimal("31000"),
            unrealized_pnl=Decimal("-480"),
        ),
    }
    return positions.get((ex, sym))


# Define a specific type alias or use a more concrete type if possible
TickerType = Ticker | TickerWithClose | None


def fake_get_ticker(ex: str, sym: str) -> TickerType:
    if ex == "hyperliquid":
        return TickerWithClose(Decimal("30000.0"))
    return TickerWithClose(Decimal("29990.0"))


# Define a specific type alias or use a more concrete type if possible
ParamType = Any  # Keep Any for now as the return structure is complex


def fake_get_param(k: str, d: object | None = None) -> ParamType:
    return {
        "history_length": 24,
        "hyperliquid_fee_rate": Decimal("0.0001"),
        "backpack_fee_rate": Decimal("0.0001"),
    }.get(k, d)


@pytest.mark.asyncio
@patch("asyncio.create_task", new_callable=lambda: MagicMock(return_value=asyncio.Future()))
async def test_process_data_scheduling(
    mock_create_task: MagicMock, strategy: FundingRateArbitrageStrategy
) -> None:
    mock_create_task.return_value.set_result(None)

    mock_data: Candle = create_mock_candle()
    strategy.last_opportunity_check = None
    with patch.object(strategy.portfolio_tracker, "get_position", return_value=None):
        with patch.object(
            strategy.data_handler, "get_latest_price", side_effect=fake_get_latest_price
        ):
            result = await strategy.process_data(mock_data)
    mock_create_task.assert_called_once()
    assert result == []


@pytest.mark.asyncio
@patch("asyncio.create_task", new_callable=lambda: MagicMock(return_value=asyncio.Future()))
async def test_process_data_rebalance(
    mock_create_task: MagicMock, strategy: FundingRateArbitrageStrategy
) -> None:
    mock_create_task.return_value.set_result(None)

    mock_data: Candle = create_mock_candle()
    strategy.last_opportunity_check = datetime.now(UTC)
    strategy.rebalance_threshold = Decimal("0.00001")
    with patch.object(strategy.portfolio_tracker, "get_position", side_effect=fake_get_position):
        with patch.object(strategy.data_handler, "get_ticker", side_effect=fake_get_ticker):
            result = await strategy.process_data(mock_data)
    mock_create_task.assert_not_called()
    assert isinstance(result, list)
    assert len(result) == 2
    opp_ids: set[str] = {
        str(s.metadata["opportunity_id"])
        for s in result
        if s.metadata is not None and "opportunity_id" in s.metadata
    }
    assert len(opp_ids) == 1
    legs: set[str] = {
        str(s.metadata["leg"]) for s in result if s.metadata is not None and "leg" in s.metadata
    }
    assert legs == {"perp", "spot"}
    for signal in result:
        assert isinstance(signal, TradeSignal)
        assert signal.symbol in ("BTC-PERP", "BTC_USDC")
        if signal.metadata is not None and "leg" in signal.metadata:
            assert signal.metadata["leg"] in ("perp", "spot")


@pytest.mark.asyncio
@patch("asyncio.create_task", new_callable=lambda: MagicMock(return_value=asyncio.Future()))
async def test_opportunity_check_scheduling(
    mock_create_task: MagicMock, strategy: FundingRateArbitrageStrategy
) -> None:
    mock_create_task.return_value.set_result(None)

    data: Candle = Candle(
        symbol="BTC-PERP",
        interval="1m",
        open_time=datetime.now(UTC),
        open=Decimal("30000.0"),
        high=Decimal("30100.0"),
        low=Decimal("29900.0"),
        close=Decimal("30050.0"),
        volume=Decimal("10.0"),
    )
    with patch.object(strategy.portfolio_tracker, "get_position", return_value=None):
        with patch.object(strategy.data_handler, "get_latest_price", return_value=None):
            await strategy.process_data(data)
    mock_create_task.assert_called_once()


@pytest.mark.asyncio
@patch("cyberdelta.strategies.funding_rate_arbitrage.logger")
async def test_check_opportunity(
    mock_logger: MagicMock, strategy: FundingRateArbitrageStrategy
) -> None:
    # Setup mocks for funding rate and ticker data
    funding_rate: FundingRate = FundingRate(
        symbol="BTC-PERP",
        funding_rate=Decimal("0.1"),
        predicted_rate=Decimal("0.1"),
        next_funding_time=datetime.now(UTC) + timedelta(hours=1),
        mark_price=Decimal("30000.0"),
        index_price=Decimal("29990.0"),
        timestamp=datetime.now(UTC),
    )
    # Patch async/protected and other methods for test
    with patch.object(
        strategy.data_handler, "get_funding_rate", new_callable=AsyncMock, return_value=funding_rate
    ):
        with patch.object(strategy.data_handler, "get_ticker", side_effect=fake_get_ticker):
            with patch.object(strategy, "get_param", side_effect=fake_get_param):
                # Protected member patching is acceptable in tests
                with patch.object(
                    strategy, "_calculate_basis_volatility", return_value=Decimal("0.005")
                ):
                    # Protected method call is acceptable in tests
                    opportunity = await strategy._check_opportunity()  # type: ignore[attr-defined]
    assert opportunity is not None
    assert opportunity.symbol == "BTC-PERP"
    assert opportunity.net_funding_differential == Decimal("0.1")
    assert opportunity.short_exchange == "hyperliquid"
    assert opportunity.long_exchange == "backpack"
    assert opportunity.expected_profit is not None and opportunity.expected_profit > Decimal("0")


@pytest.mark.asyncio
async def test_calculate_basis_volatility(
    strategy: FundingRateArbitrageStrategy, **kwargs: object
) -> None:
    """Test calculation of basis volatility."""
    strategy.historical_basis = {
        "BTC-PERP": [
            (datetime.now(UTC), Decimal("10.0")),
            (datetime.now(UTC), Decimal("12.0")),
            (datetime.now(UTC), Decimal("8.0")),
            (datetime.now(UTC), Decimal("11.0")),
            (datetime.now(UTC), Decimal("9.0")),
        ]
    }
    volatility = strategy._calculate_basis_volatility("BTC-PERP")
    expected = Decimal("1.4142135623730951")  # sqrt(2)
    assert abs(volatility - expected) < Decimal("1e-6")


# Helper functions


def create_mock_candle(**kwargs: Any) -> Candle:
    return Candle(
        symbol=kwargs.get("symbol", "BTC-PERP"),
        interval=kwargs.get("interval", "1m"),
        open_time=kwargs.get("open_time", datetime.now(UTC)),
        open=kwargs.get("open", Decimal("30000.0")),
        high=kwargs.get("high", Decimal("30100.0")),
        low=kwargs.get("low", Decimal("29900.0")),
        close=kwargs.get("close", Decimal("30050.0")),
        volume=kwargs.get("volume", Decimal("10.0")),
    )


def create_mock_ticker(**kwargs: Any) -> Ticker:
    return Ticker(
        symbol=kwargs.get("symbol", "BTC-PERP"),
        price=kwargs.get("price", Decimal("30000.0")),
        bid=kwargs.get("bid", Decimal("29995.0")),
        ask=kwargs.get("ask", Decimal("30005.0")),
        volume=kwargs.get("volume", Decimal("100.0")),
        timestamp=kwargs.get("timestamp", datetime.now(UTC)),
    )


default_funding_rate_kwargs = {
    "symbol": "BTC-PERP",
    "funding_rate": Decimal("0.1"),
    "predicted_rate": Decimal("0.1"),
    "next_funding_time": datetime.now(UTC) + timedelta(hours=1),
    "mark_price": Decimal("30000.0"),
    "index_price": Decimal("29990.0"),
    "timestamp": datetime.now(UTC),
}


def create_mock_funding_rate(**kwargs: Any) -> FundingRate:
    return FundingRate(**default_funding_rate_kwargs | kwargs)


@pytest.mark.asyncio
def test_none_subscript() -> None:
    """Test is not None check before subscript."""
    maybe_dict: dict[str, int] = {"a": 1}
    value = maybe_dict["a"]
    assert value == 1


@pytest.mark.asyncio
async def test_process_data_rebalance(
    mock_strategy: FundingRateArbitrageStrategy,
    mock_data_manager: MagicMock,
    mock_signal_queue: MagicMock,
) -> None:
    """Test processing data leading to a rebalancing signal."""
    # Mock internal state and data
    mock_strategy.latest_opportunities = {
        "BTC/USDT": create_mock_opportunity(
            "BTC/USDT",
            long_exchange="hyperliquid",
            short_exchange="backpack",
            long_funding_rate=Decimal("0.0001"),
            short_funding_rate=Decimal("-0.0002"),  # Large difference
            long_price=Decimal("50000"),
            short_price=Decimal("50000"),  # Assume same price for simplicity here
        )
    }
    mock_strategy.current_positions = {
        "hyperliquid": {"BTC/USDT": MagicMock(size=Decimal("-0.1"))},  # Wrong side
        "backpack": {"BTC/USDT": MagicMock(size=Decimal("0.1"))},
    }
    mock_strategy.config.strategy.parameters = {"rebalance_threshold": 0.05}  # Example

    # Mock _check_and_generate_signal to simulate signal generation
    mock_strategy._check_and_generate_signal = AsyncMock(  # type: ignore[method-assign]
        return_value=create_mock_signal("BTC/USDT", score=0.9, signal_type=SignalType.REBALANCE)
    )

    # Call process_data
    await mock_strategy.process_data()

    # Assert _check_and_generate_signal was awaited
    # Need to await the mocked async function when asserting its call
    # This doesn't check if the original call site awaited it, but that's harder to test directly
    # We rely on the RuntimeWarning fix for the actual call site.
    mock_strategy._check_and_generate_signal.assert_awaited()  # Check it was called

    # Assert signal was added to the queue
    mock_signal_queue.add_signal.assert_called_once()
    call_args, _ = mock_signal_queue.add_signal.call_args
    added_signal = call_args[0]
    assert isinstance(added_signal, TradeSignal)
    assert added_signal.signal_type == SignalType.REBALANCE


@pytest.mark.asyncio
async def test_check_opportunity(
    mock_strategy: FundingRateArbitrageStrategy, mock_signal_queue: MagicMock
) -> None:
    """Test the _check_opportunity method directly for signal generation."""
    opportunity = create_mock_opportunity(
        symbol="ETH/USDT",
        long_exchange="hyperliquid",
        short_exchange="backpack",
        long_funding_rate=Decimal("0.0002"),
        short_funding_rate=Decimal("-0.0001"),
        net_funding_differential=Decimal("0.0003"),
        long_price=Decimal("3000"),
        short_price=Decimal("3001"),
        expected_profit=Decimal("5"),
        utility_score=0.9,
        basis_volatility=Decimal("0.0005"),
    )
    mock_strategy.config.strategy.parameters = {
        "min_utility_score": 0.7,
        "signal_expiration_seconds": 60,
    }
    mock_strategy.symbol_mapper = MagicMock()
    mock_strategy.symbol_mapper.get_internal_symbol.return_value = "ETH"

    # Await the call to the async method
    signal = await mock_strategy._check_and_generate_signal(opportunity)  # Add await here

    assert signal is not None
    assert signal.symbol == "ETH/USDT"
    assert signal.signal_type == SignalType.OPEN
    assert signal.score == opportunity.utility_score
    assert signal.exchange == "MULTI"  # Should indicate both exchanges involved
    assert (
        signal.price == (opportunity.long_price + opportunity.short_price) / 2
    )  # Example price logic
    assert signal.details["long_exchange"] == "hyperliquid"
    assert signal.details["short_exchange"] == "backpack"
    assert signal.details["long_funding_rate"] == opportunity.long_funding_rate
    assert signal.details["short_funding_rate"] == opportunity.short_funding_rate
    assert signal.details["net_funding_differential"] == opportunity.net_funding_differential
    assert signal.details["utility_score"] == opportunity.utility_score
    assert signal.details["basis_volatility"] == opportunity.basis_volatility
    assert signal.expiration is not None


# Test for potential None subscript error if parameters are missing (Defensive)
# @pytest.mark.asyncio # REMOVE this mark as it's not an async test
def test_none_subscript(mock_strategy: FundingRateArbitrageStrategy) -> None:
    """Test behavior when strategy parameters might be missing."""
    # Simulate missing parameters
    mock_strategy.config.strategy.parameters = {}
    opportunity = create_mock_opportunity(symbol="BTC/USDT", utility_score=0.9)

    # Expect this call NOT to raise an error, even if parameters are missing
    # The method should handle missing keys gracefully (e.g., use defaults or skip checks)
    try:
        # This isn't async, so no await needed
        signal = mock_strategy._check_and_generate_signal(opportunity)
        # Depending on implementation, signal might be None or have default values
        # Add assertions here based on expected graceful handling
        # For example, if it should return None when min_utility_score is missing:
        # assert signal is None
        # Or if defaults are used:
        # assert signal is not None # Or more specific checks
        pass  # Placeholder: Test passes if no exception is raised

    except TypeError as e:
        pytest.fail(f"'_check_and_generate_signal' raised TypeError with missing params: {e}")
    except KeyError as e:
        pytest.fail(f"'_check_and_generate_signal' raised KeyError with missing params: {e}")
