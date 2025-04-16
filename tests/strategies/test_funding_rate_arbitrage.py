from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.core.models import (
    FundingRate,
    MarketData,
    OrderSide,
    Position,
    Ticker,
    TradeSignal,
)
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy

# Configure logging for this test
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


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


@pytest.mark.asyncio
@patch("asyncio.create_task")
async def test_process_data_scheduling(
    mock_create_task: MagicMock, strategy: FundingRateArbitrageStrategy
) -> None:
    mock_data: MarketData = create_mock_market_data()
    strategy.last_opportunity_check = None
    strategy.portfolio_tracker.get_position = MagicMock(return_value=None)  # type: ignore[attr-defined]
    strategy.data_handler.get_latest_price = MagicMock(
        side_effect=lambda ex: Decimal("30000.0")
        if ex == ("hyperliquid", "BTC-PERP")
        else Decimal("29990.0")
    )  # type: ignore[attr-defined]

    # Should schedule a check and return None
    result = await strategy.process_data(mock_data)
    mock_create_task.assert_called_once()
    assert result == []


@pytest.mark.asyncio
@patch("asyncio.create_task")
async def test_process_data_rebalance(
    mock_create_task: MagicMock, strategy: FundingRateArbitrageStrategy
) -> None:
    mock_data: MarketData = create_mock_market_data()
    strategy.last_opportunity_check = datetime.now(UTC)
    # Lower the rebalance threshold to always trigger rebalance
    strategy.rebalance_threshold = Decimal("0.00001")
    # Simulate positions for rebalancing
    mock_perp_pos: Position = Position(
        symbol="BTC-PERP",
        size=Decimal("1.0"),
        entry_price=Decimal("29500"),
        side=OrderSide.BUY,
        leverage=Decimal("1"),
    )
    mock_spot_pos: Position = Position(
        symbol="BTC_USDC",
        size=Decimal("-1.0"),
        entry_price=Decimal("29510"),
        side=OrderSide.SELL,
        leverage=Decimal("1"),
    )
    strategy.portfolio_tracker.get_position = MagicMock(
        side_effect=lambda ex, sym: {
            ("hyperliquid", "BTC-PERP"): mock_perp_pos,
            ("backpack", "BTC_USDC"): mock_spot_pos,
        }.get((ex, sym))
    )

    # Mock get_ticker to return Ticker with .close attribute
    class TickerWithClose:
        def __init__(self, close: Decimal):
            self.close: Decimal = close

    strategy.data_handler.get_ticker = MagicMock(
        side_effect=lambda ex, sym: TickerWithClose(Decimal("30000.0"))
        if ex == "hyperliquid"
        else TickerWithClose(Decimal("29990.0"))
    )
    result = await strategy.process_data(mock_data)
    # Should not schedule a check (too soon)
    mock_create_task.assert_not_called()
    # Should return a list of two signals (perp and spot legs)
    assert isinstance(result, list)
    assert len(result) == 2
    opp_ids: set[str] = {
        str(s.metadata["opportunity_id"])
        for s in result
        if s.metadata and "opportunity_id" in s.metadata
    }
    assert len(opp_ids) == 1  # Both legs share the same opportunity_id
    legs: set[str] = {str(s.metadata["leg"]) for s in result if s.metadata and "leg" in s.metadata}
    assert legs == {"perp", "spot"}
    for signal in result:
        assert isinstance(signal, TradeSignal)
        assert signal.symbol in ("BTC-PERP", "BTC_USDC")
        assert signal.metadata["leg"] in ("perp", "spot")


@pytest.mark.asyncio
@patch("asyncio.create_task")
async def test_opportunity_check_scheduling(
    mock_create_task: MagicMock, strategy: FundingRateArbitrageStrategy
) -> None:
    data: MarketData = MarketData(
        symbol="BTC-PERP",
        timestamp=datetime.now(UTC),
        open=Decimal("30000.0"),
        high=Decimal("30100.0"),
        low=Decimal("29900.0"),
        close=Decimal("30050.0"),
        volume=Decimal("10.0"),
    )
    strategy.portfolio_tracker.get_position = MagicMock(return_value=None)  # type: ignore[attr-defined]
    strategy.data_handler.get_latest_price = MagicMock(return_value=None)  # type: ignore[attr-defined]
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

    # Ticker mock with .close attribute
    class TickerWithClose:
        def __init__(self, close: Decimal):
            self.close: Decimal = close

    perp_ticker = TickerWithClose(Decimal("30000.0"))
    spot_ticker = TickerWithClose(Decimal("29990.0"))
    strategy.data_handler.get_funding_rate = AsyncMock(return_value=funding_rate)
    strategy.data_handler.get_ticker = MagicMock(
        side_effect=lambda ex, sym: TickerWithClose(Decimal("30000.0"))
        if ex == "hyperliquid"
        else TickerWithClose(Decimal("29990.0"))
    )
    # Patch get_param for fee rates
    strategy.get_param = MagicMock(
        side_effect=lambda k, d=None: {
            "history_length": 24,
            "hyperliquid_fee_rate": Decimal("0.0001"),
            "backpack_fee_rate": Decimal("0.0001"),
        }.get(k, d)
    )
    # Patch basis volatility
    strategy._calculate_basis_volatility = MagicMock(return_value=Decimal("0.005"))
    opportunity = await strategy._check_opportunity()
    assert opportunity is not None
    assert opportunity.symbol == "BTC-PERP"
    assert opportunity.net_funding_differential == Decimal("0.1")
    assert opportunity.short_exchange == "hyperliquid"
    assert opportunity.long_exchange == "backpack"
    assert opportunity.expected_profit > Decimal("0")


@pytest.mark.asyncio
async def test_calculate_basis_volatility(strategy):
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


# Helper functions remain unchanged


def create_mock_market_data(**kwargs) -> MarketData:
    return MarketData(
        symbol=kwargs.get("symbol", "BTC-PERP"),
        timestamp=kwargs.get("timestamp", datetime.now(UTC)),
        open=kwargs.get("open", Decimal("30000.0")),
        high=kwargs.get("high", Decimal("30100.0")),
        low=kwargs.get("low", Decimal("29900.0")),
        close=kwargs.get("close", Decimal("30050.0")),
        volume=kwargs.get("volume", Decimal("10.0")),
    )


def create_mock_ticker(**kwargs) -> Ticker:
    return Ticker(
        symbol=kwargs.get("symbol", "BTC-PERP"),
        price=kwargs.get("price", Decimal("30000.0")),
        bid=kwargs.get("bid", Decimal("29995.0")),
        ask=kwargs.get("ask", Decimal("30005.0")),
        volume=kwargs.get("volume", Decimal("100.0")),
        timestamp=kwargs.get("timestamp", int(datetime.now(UTC).timestamp() * 1000)),
    )


def create_mock_funding_rate(**kwargs) -> FundingRate:
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
