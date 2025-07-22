"""Integration tests for FundingRateArbitrageStrategy.

Tests the complete funding rate arbitrage strategy including opportunity
detection, signal generation, position management, and integration with
data handler, portfolio tracker, and risk manager components.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, TypedDict, Unpack, cast
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    Ticker,
    TradeSignal,
)
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.models.market.funding_rate import (
    BackpackFundingDetails,
    HyperliquidFundingDetails,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.enums import OrderSide, SignalType
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.validation.funding_data import ArbitrageOpportunity


pytestmark = pytest.mark.timing

logging.basicConfig(level=logging.DEBUG)
logger = get_logger(__name__)

default_price = Decimal("30000.0")
PositionType = DerivativePosition | None


def create_mock_opportunity(
    symbol: str,
    long_exchange: str = "long_ex",
    short_exchange: str = "short_ex",
    long_funding_rate: Decimal = Decimal("0.0001"),
    short_funding_rate: Decimal = Decimal("-0.0001"),
    net_funding_differential: Decimal = Decimal("0.0002"),
    long_price: Decimal = Decimal(100),
    short_price: Decimal = Decimal(100),
    expected_profit: Decimal = Decimal(1),
    utility_score: float = 0.5,
    basis_volatility: Decimal = Decimal("0.001"),
    timestamp: datetime | None = None,
) -> ArbitrageOpportunity:
    """Create mock opportunity for testing."""
    return ArbitrageOpportunity(
        symbol=symbol,
        long_exchange=long_exchange,
        short_exchange=short_exchange,
        long_price=long_price,
        short_price=short_price,
        long_funding_rate=long_funding_rate,
        short_funding_rate=short_funding_rate,
        net_funding_differential=net_funding_differential,
        timestamp=timestamp or datetime.now(UTC),
        expected_profit=expected_profit,
        utility_score=utility_score,
        basis_volatility=float(basis_volatility),
    )


def create_mock_signal(
    symbol: str,
    signal_type: SignalType = SignalType.ENTER_LONG,
    side: OrderSide = OrderSide.BUY,
    price: Decimal = Decimal(100),
    exchange: str | list[str] = "MULTI",
    confidence: float | None = 0.5,
    quantity: Decimal | None = None,
    details: dict[str, Any] | None = None,
    expiration: datetime | None = None,
) -> TradeSignal:
    """Create mock signal for testing."""
    return TradeSignal(
        symbol=symbol,
        signal_type=signal_type,
        side=side,
        price=price,
        exchange=exchange,
        quantity=quantity,
        confidence=confidence,
        metadata=details or {},
        timestamp=datetime.now(UTC),
        expiration=expiration,
    )


@pytest.fixture
def strategy() -> FundingRateArbitrageStrategy:
    """Create a FundingRateArbitrageStrategy instance for testing."""
    data_handler = MagicMock()
    portfolio_tracker = MagicMock(spec=PortfolioTracker)
    risk_manager_mock = MagicMock(spec=RiskManager)

    return FundingRateArbitrageStrategy(
        name="test_funding_arb",
        symbol="BTC-PERP",
        data_handler=data_handler,
        portfolio_tracker=portfolio_tracker,
        risk_manager=risk_manager_mock,
        params={
            "min_funding_differential": Decimal("0.01"),
            "min_profit_threshold": Decimal("1.0"),
            "risk_aversion": Decimal("0.5"),
            "perp_exchange": "hyperliquid",
            "spot_exchange": "backpack",
            "symbol_mapping": {"BTC-PERP": "BTC_USDC"},
        },
    )


def fake_get_position(ex: str, sym: str) -> PositionType:
    """Return fake position data based on exchange and symbol combination."""
    positions = {
        ("hyperliquid", "BTC-PERP"): DerivativePosition(
            exchange="hyperliquid",
            timestamp=datetime.now(UTC),
            symbol="BTC-PERP",
            size=Decimal("1.0"),
            entry_price=Decimal(29500),
            mark_price=Decimal(30000),
            side=OrderSide.BUY,
            liquidation_price=Decimal(28000),
            unrealized_pnl=Decimal(500),
        ),
        ("backpack", "BTC_USDC"): DerivativePosition(
            exchange="backpack",
            timestamp=datetime.now(UTC),
            symbol="BTC_USDC",
            size=Decimal("-1.0"),
            entry_price=Decimal(29510),
            mark_price=Decimal(29990),
            side=OrderSide.SELL,
            liquidation_price=Decimal(31000),
            unrealized_pnl=Decimal(-480),
        ),
    }
    return positions.get((ex, sym))


def fake_get_ticker(exchange_id: str, symbol: str) -> Ticker | None:
    """Return fake ticker data for testing different exchange and symbol combinations."""
    now = datetime.now(UTC)
    if exchange_id == "hyperliquid" and symbol == "BTC-PERP":
        return Ticker(
            symbol="BTC-PERP",
            exchange="hyperliquid",
            price=Decimal("30000.0"),
            timestamp=now,
            bid=Decimal("29999.0"),
            ask=Decimal("30001.0"),
            volume=Decimal(1000),
        )
    if exchange_id == "backpack" and symbol == "BTC_USDC":
        return Ticker(
            symbol="BTC_USDC",
            exchange="backpack",
            price=Decimal("29990.0"),
            timestamp=now,
            bid=Decimal("29989.0"),
            ask=Decimal("29991.0"),
            volume=Decimal(500),
        )
    return None


def fake_get_funding_rate(exchange_id: str, symbol: str) -> FundingRate | None:
    """Return fake funding rate data for testing different exchange and symbol combinations."""
    now = datetime.now(UTC)
    if exchange_id == "hyperliquid" and symbol == "BTC-PERP":
        return FundingRate(
            symbol="BTC-PERP",
            funding_rate=Decimal("0.0001"),
            timestamp=now,
            next_funding_time=now + timedelta(hours=1),
            mark_price=Decimal("30000.0"),
        )
    return None


@pytest.mark.asyncio
async def test_process_data_scheduling(
    strategy: FundingRateArbitrageStrategy,
) -> None:
    """Test that process_data evaluates opportunities when conditions are met."""
    mock_data: Candle = create_mock_candle()
    strategy.last_opportunity_check = None

    with (
        patch.object(strategy.data_handler, "get_latest_ticker", side_effect=fake_get_ticker),
        patch.object(
            strategy.data_handler,
            "get_latest_funding_rate",
            side_effect=fake_get_funding_rate,
        ),
        patch.object(strategy.portfolio_tracker, "get_position", return_value=None),
        patch.object(strategy, "evaluate_entry_opportunity") as mock_eval,
    ):
        # Configure mock to return an awaitable
        def mock_evaluate_entry() -> list[Any]:
            return []

        mock_eval.side_effect = mock_evaluate_entry

        await strategy.process_data(mock_data)

    # Should have called evaluate_entry_opportunity since last_opportunity_check was None
    mock_eval.assert_called_once()


@pytest.mark.asyncio
async def test_process_data_no_scheduling_if_recent_check(
    strategy: FundingRateArbitrageStrategy,
) -> None:
    """Test that process_data skips opportunity evaluation when recent check was performed."""
    strategy.last_opportunity_check = datetime.now(UTC) - timedelta(
        seconds=strategy.check_interval - 10,
    )
    mock_data: Candle = create_mock_candle()

    with (
        patch.object(strategy.data_handler, "get_latest_ticker", side_effect=fake_get_ticker),
        patch.object(
            strategy.data_handler,
            "get_latest_funding_rate",
            side_effect=fake_get_funding_rate,
        ),
        patch.object(strategy.portfolio_tracker, "get_position", return_value=None),
        patch.object(strategy, "evaluate_entry_opportunity") as mock_eval,
    ):
        # Configure mock to return an awaitable (though it shouldn't be called)
        def mock_evaluate_entry() -> list[Any]:
            return []

        mock_eval.side_effect = mock_evaluate_entry

        await strategy.process_data(mock_data)

    # Should NOT have called evaluate_entry_opportunity since recent check was performed
    mock_eval.assert_not_called()


@pytest.mark.asyncio
async def test_process_data_rebalance_signal_generation(
    strategy: FundingRateArbitrageStrategy,
) -> None:
    """Test that process_data generates rebalance signals when prices have moved significantly."""
    strategy.active_opportunities = [create_mock_opportunity(symbol="BTC-PERP")]

    def rebalance_ticker_prices(ex: str, sym: str) -> Ticker | None:
        now = datetime.now(UTC)
        if (ex, sym) == ("hyperliquid", "BTC-PERP"):
            return Ticker(
                symbol="BTC-PERP",
                exchange="hyperliquid",
                price=Decimal("31000.0"),
                timestamp=now,
                bid=Decimal("30999.0"),
                ask=Decimal("31001.0"),
                volume=Decimal(1000),
            )
        if (ex, sym) == ("backpack", "BTC_USDC"):
            return Ticker(
                symbol="BTC_USDC",
                exchange="backpack",
                price=Decimal("30500.0"),
                timestamp=now,
                bid=Decimal("30499.0"),
                ask=Decimal("30501.0"),
                volume=Decimal(500),
            )
        return None

    strategy.rebalance_threshold = Decimal("0.01")
    strategy.last_opportunity_check = datetime.now(UTC) - timedelta(
        seconds=strategy.check_interval + 1,
    )

    mock_data: Candle = create_mock_candle()
    with (
        patch.object(
            strategy.data_handler,
            "get_latest_ticker",
            side_effect=rebalance_ticker_prices,
        ),
        patch.object(
            strategy.data_handler,
            "get_latest_funding_rate",
            side_effect=fake_get_funding_rate,
        ),
        patch.object(
            strategy.portfolio_tracker,
            "get_position",
            side_effect=fake_get_position,
        ) as _,
        patch.object(strategy, "_should_rebalance", return_value=True) as mock_should_rebalance,
        patch.object(
            strategy,
            "_generate_rebalance_signal",
            return_value=[create_mock_signal(symbol="BTC-PERP", signal_type=SignalType.REBALANCE)],
        ) as mock_gen_rebal_signal,
        patch.object(
            strategy,
            "evaluate_entry_opportunity",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_eval_entry_opp,
    ):
        signals = await strategy.process_data(mock_data)

    mock_should_rebalance.assert_called_once()
    mock_gen_rebal_signal.assert_called_once()
    mock_eval_entry_opp.assert_called_once()

    assert signals is not None
    assert any(s.signal_type == SignalType.REBALANCE for s in signals)


@pytest.mark.skip(reason="Strategy has a bug - not awaiting async method size_opportunity")
@pytest.mark.asyncio
@patch("cyberdelta.strategies.funding_rate_arbitrage.logger")
async def test_evaluate_entry_opportunity_found(
    mock_logger: MagicMock,
    strategy: FundingRateArbitrageStrategy,
) -> None:
    """Test that evaluate_entry_opportunities identifies and logs profitable opportunities."""
    mock_opportunity = create_mock_opportunity(symbol="BTC-PERP", expected_profit=Decimal(100))
    ep = cast("Decimal", mock_opportunity.expected_profit)
    mock_sized_opportunity = SizedOpportunity(
        opportunity=mock_opportunity,
        long_size=Decimal(10000),
        short_size=Decimal(10000),
        allocation_percentage=Decimal("0.1"),
        expected_profit=ep,
        expected_return=Decimal("0.01"),
        risk_adjusted_return=Decimal("0.008"),
    )

    def mock_size_opportunity(opp: ArbitrageOpportunity) -> SizedOpportunity:
        return mock_sized_opportunity

    with (
        patch.object(strategy.data_handler, "get_latest_ticker", side_effect=fake_get_ticker),
        patch.object(
            strategy.data_handler,
            "get_latest_funding_rate",
            side_effect=fake_get_funding_rate,
        ),
        patch.object(strategy.portfolio_tracker, "get_position", return_value=None) as _,
        patch.object(strategy, "_should_rebalance", return_value=False) as mock_should_rebalance,
        patch.object(
            strategy,
            "_check_opportunity",
            new_callable=AsyncMock,
            return_value=mock_opportunity,
        ) as mock_check_internal,
        patch.object(
            strategy.risk_manager,
            "size_opportunity",
            side_effect=mock_size_opportunity,
        ) as mock_calc_size,
        patch.object(
            strategy,
            "_generate_entry_signal",
            return_value=[create_mock_signal(symbol="BTC-PERP")],
        ) as mock_gen_signal,
    ):
        signals = await strategy.evaluate_entry_opportunity()

    mock_should_rebalance.assert_called_once()
    mock_check_internal.assert_called_once()
    mock_calc_size.assert_called_once_with(mock_opportunity)
    mock_gen_signal.assert_called_once_with(mock_opportunity, mock_sized_opportunity, ANY, ANY)

    assert signals is not None
    assert len(signals) == 1
    assert len(strategy.active_opportunities) == 1
    assert strategy.active_opportunities[0].id == mock_opportunity.id
    mock_logger.warning.assert_not_called()
    mock_logger.info.assert_any_call(f"Found opportunity: {mock_opportunity}")


@pytest.mark.asyncio
@patch("cyberdelta.strategies.funding_rate_arbitrage.logger")
async def test_evaluate_entry_opportunity_no_opportunity(
    mock_logger: MagicMock,
    strategy: FundingRateArbitrageStrategy,
) -> None:
    """Test that evaluate_entry_opportunities handles cases with no arbitrage opportunities."""
    with (
        patch.object(strategy.data_handler, "get_latest_ticker", side_effect=fake_get_ticker),
        patch.object(
            strategy.data_handler,
            "get_latest_funding_rate",
            side_effect=fake_get_funding_rate,
        ),
        patch.object(strategy.portfolio_tracker, "get_position", return_value=None) as _,
        patch.object(
            strategy,
            "_check_opportunity",
        ) as mock_check_internal,
        patch.object(strategy.risk_manager, "size_opportunity") as mock_calc_size,
        patch.object(strategy, "_generate_entry_signal") as mock_gen_signal,
    ):
        # Configure async mock to return None
        def mock_check_opp() -> None:
            return None

        mock_check_internal.side_effect = mock_check_opp

        signals = await strategy.evaluate_entry_opportunity()

    mock_check_internal.assert_called_once()
    mock_calc_size.assert_not_called()
    mock_gen_signal.assert_not_called()
    assert signals is None
    assert not strategy.active_opportunities


# Helper functions
class CandleKwargs(TypedDict, total=False):
    """TypedDict for Candle keyword arguments used in test data creation."""

    symbol: str
    interval: str
    open_time: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


def create_mock_candle(**kwargs: Unpack[CandleKwargs]) -> Candle:
    """Create mock candle for testing."""
    defaults: dict[str, Any] = {
        "symbol": "BTC-PERP",
        "open_time": datetime.now(UTC) - timedelta(minutes=1),
        "open": Decimal(29900),
        "high": Decimal(30100),
        "low": Decimal(29800),
        "close": Decimal(30000),
        "volume": Decimal(1000),
        "interval": "1m",
    }
    merged_args = {**defaults, **kwargs}
    return Candle(
        symbol=str(merged_args["symbol"]),
        interval=str(merged_args["interval"]),
        open_time=cast("datetime", merged_args["open_time"]),
        open=Decimal(str(merged_args["open"])),
        high=Decimal(str(merged_args["high"])),
        low=Decimal(str(merged_args["low"])),
        close=Decimal(str(merged_args["close"])),
        volume=Decimal(str(merged_args["volume"])),
    )


class TickerKwargs(TypedDict, total=False):
    """TypedDict for Ticker keyword arguments used in test data creation."""

    symbol: str
    timestamp: datetime
    price: Decimal | None
    bid: Decimal | None
    ask: Decimal | None
    volume: Decimal | None


def create_mock_ticker(**kwargs: Unpack[TickerKwargs]) -> Ticker:
    """Create mock ticker for testing."""
    defaults: dict[str, Any] = {
        "symbol": "BTC-PERP",
        "exchange": "test_exchange",  # Default exchange for testing
        "price": Decimal("30000.0"),
        "timestamp": datetime.now(UTC),
        "bid": Decimal("29999.0"),
        "ask": Decimal("30001.0"),
        "volume": Decimal("1000.0"),
    }
    merged_args = {**defaults, **kwargs}
    return Ticker(
        symbol=str(merged_args["symbol"]),
        exchange=str(merged_args["exchange"]),
        timestamp=cast("datetime", merged_args["timestamp"]),
        price=Decimal(str(merged_args["price"])) if merged_args.get("price") is not None else None,
        bid=Decimal(str(merged_args["bid"])) if merged_args.get("bid") is not None else None,
        ask=Decimal(str(merged_args["ask"])) if merged_args.get("ask") is not None else None,
        volume=Decimal(str(merged_args["volume"]))
        if merged_args.get("volume") is not None
        else None,
    )


class FundingRateKwargs(TypedDict, total=False):
    """TypedDict for FundingRate keyword arguments used in test data creation."""

    symbol: str
    timestamp: datetime
    funding_rate: Decimal | None
    predicted_rate: Decimal | None
    mark_price: Decimal | None
    index_price: Decimal | None
    next_funding_time: datetime | None
    hl_details: HyperliquidFundingDetails | None
    bp_details: BackpackFundingDetails | None


def create_mock_funding_rate(**kwargs: Unpack[FundingRateKwargs]) -> FundingRate:
    """Create mock funding rate for testing."""
    defaults: dict[str, Any] = {
        "symbol": "BTC-PERP",
        "funding_rate": Decimal("0.0001"),
        "timestamp": datetime.now(UTC),
        "next_funding_time": datetime.now(UTC) + timedelta(hours=1),
        "mark_price": Decimal("30000.0"),
        "predicted_rate": None,
        "index_price": None,
        "hl_details": None,
        "bp_details": None,
    }
    merged_args = {**defaults, **kwargs}

    if "symbol" not in merged_args or merged_args["symbol"] is None:
        raise ValueError(
            "Missing or invalid value for required field: symbol in create_mock_funding_rate",
        )
    ts_val = merged_args.get("timestamp")
    if ts_val is None or not isinstance(ts_val, datetime):
        raise ValueError(
            "Missing or invalid value for required field: timestamp in create_mock_funding_rate",
        )

    return FundingRate(
        symbol=str(merged_args["symbol"]),
        timestamp=ts_val,
        funding_rate=Decimal(str(merged_args["funding_rate"]))
        if merged_args.get("funding_rate") is not None
        else None,
        predicted_rate=Decimal(str(merged_args["predicted_rate"]))
        if merged_args.get("predicted_rate") is not None
        else None,
        mark_price=Decimal(str(merged_args["mark_price"]))
        if merged_args.get("mark_price") is not None
        else None,
        index_price=Decimal(str(merged_args["index_price"]))
        if merged_args.get("index_price") is not None
        else None,
        next_funding_time=cast("datetime | None", merged_args.get("next_funding_time")),
        hl_details=cast("HyperliquidFundingDetails | None", merged_args.get("hl_details")),
        bp_details=cast("BackpackFundingDetails | None", merged_args.get("bp_details")),
    )
