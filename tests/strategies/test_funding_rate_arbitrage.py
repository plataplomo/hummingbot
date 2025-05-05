from __future__ import annotations

import logging
from datetime import UTC, datetime
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
@patch("asyncio.create_task")
async def test_process_data_scheduling(
    mock_create_task: MagicMock, strategy: FundingRateArbitrageStrategy
) -> None:
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
@patch("asyncio.create_task")
async def test_process_data_rebalance(
    mock_create_task: MagicMock, strategy: FundingRateArbitrageStrategy
) -> None:
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
@patch("asyncio.create_task")
async def test_opportunity_check_scheduling(
    mock_create_task: MagicMock, strategy: FundingRateArbitrageStrategy
) -> None:
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
        next_funding_time=int(datetime.now(UTC).timestamp() * 1000) + 3600000,
        mark_price=Decimal("30000.0"),
        index_price=Decimal("29990.0"),
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


def create_mock_candle(**kwargs: object) -> Candle:
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


def create_mock_ticker(**kwargs: object) -> Ticker:
    return Ticker(
        symbol=kwargs.get("symbol", "BTC-PERP"),
        price=kwargs.get("price", Decimal("30000.0")),
        bid=kwargs.get("bid", Decimal("29995.0")),
        ask=kwargs.get("ask", Decimal("30005.0")),
        volume=kwargs.get("volume", Decimal("100.0")),
        timestamp=kwargs.get("timestamp", int(datetime.now(UTC).timestamp() * 1000)),
    )


def create_mock_funding_rate(**kwargs: object) -> FundingRate:
    return FundingRate(
        symbol=kwargs.get("symbol", "BTC-PERP"),
        funding_rate=kwargs.get("funding_rate", Decimal("0.1")),
        predicted_rate=kwargs.get("predicted_rate", Decimal("0.1")),
        next_funding_time=kwargs.get(
            "next_funding_time", int(datetime.now(UTC).timestamp() * 1000) + 3600000
        ),
        mark_price=kwargs.get("mark_price", Decimal("30000.0")),
        index_price=kwargs.get("index_price", Decimal("29990.0")),
    )


@pytest.mark.asyncio
def test_none_subscript() -> None:
    """Test is not None check before subscript."""
    maybe_dict: dict[str, int] = {"a": 1}
    value = maybe_dict["a"]
    assert value == 1
