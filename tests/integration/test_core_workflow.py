import asyncio
import copy  # Import copy module
import logging  # Import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from cyberdelta.apis.base import APIErrorCode, ExchangeAPI  # <--- Added ExchangeAPI here

# Core Components
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.execution_handler import ExecutionHandler, ExecutionStatus

# Models
from cyberdelta.core.models import (
    ArbitrageOpportunity,
    Balance,
    FundingRate,
    OrderSide,
    OrderStatus,
    Ticker,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.core.signal_generator import SignalGenerator
from cyberdelta.utils.config import Config  # Assuming Config class is used

# Mocks & Config
from tests.integration.mocks.mock_exchange import MockExchangeAPI, MockAPIError  # Import MockAPIError (Fix 30)

# Helper Functions
# def create_mock_ticker(symbol, bid, ask, price, timestamp): # Moved to integration/conftest.py


def create_mock_funding_rate(symbol, rate, next_time):
    # Convert rate to Decimal
    return FundingRate(
        symbol=symbol, funding_rate=Decimal(rate), next_funding_time=next_time
    )


# Helper function for creating mock tickers
def create_mock_ticker(symbol, bid, ask, price, timestamp) -> Ticker:
    return Ticker(
        symbol=symbol,
        bid=Decimal(str(bid)),
        ask=Decimal(str(ask)),
        price=Decimal(str(price)),
        timestamp=int(timestamp.timestamp() * 1000)
    )


# Initialize logger for this module
logger = logging.getLogger(__name__)

# --- Test Fixtures ---


@pytest.fixture(scope="module")
def mock_config_dict():
    """Provides a base configuration dictionary for integration tests."""
    return {
        "general": {
            "log_level": "DEBUG",
            "safe_mode": False,
        },
        "exchanges": {
            "mock_hl": {
                "enabled": True,
                "api_key": "mock_hl_key",
                "api_secret": "mock_hl_secret",
                "api_base_url": "mock",
                "ws_url": "mock",
                "symbols": {"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
                "fee_rate": 0.0005,
                "collateral_asset": "USD",
            },
            "mock_bp": {
                "enabled": True,
                "api_key": "mock_bp_key",
                "api_secret": "mock_bp_secret",
                "api_base_url": "mock",
                "ws_url": "mock",
                "symbols": {"BTC": "BTC-USDC", "ETH": "ETH-USDC"},
                "fee_rate": 0.0005,
                "collateral_asset": "USDC",
            },
        },
        "strategy": {
            "hl_perp_bp_perp": {
                "type": "funding_rate",
                "exchanges": ["mock_hl", "mock_bp"],
                "symbol": "BTC",
            },
            "funding_rate": {
                "min_profit_threshold": 0.1,
                "min_funding_differential": 0.0001,
                "risk_aversion": 1.0,
            },
        },
        "portfolio": {"reconciliation_interval": 300},
        "risk_manager": {
            "max_total_exposure": 20000.0,
            "max_exchange_exposure": 10000.0,
            "max_position_size": 5000.0,
            "circuit_breaker_threshold": 0.1,
            "min_exchange_balance": 10.0,
        },
        "execution_handler": {
            "order_timeout_seconds": 30,
            "max_slippage_pct": 0.001,
            "max_retries": 2,
            "retry_delay_base_sec": 0.05,
            "compensation": {  # Add compensation config section
                "use_limit_orders": True,
                "limit_price_offset_pct": 0.05,
            },
        },
        "data": {
            "staleness_thresholds": {
                "ticker": 60,
                "funding_rate": 3600,
                "orderbook": 30,
            }
        },
        "safety_systems": {
            "circuit_breakers": {"enabled": False},
            "position_reconciliation": {"enabled": False},
            "balance_monitoring": {"enabled": False},
        },
    }


@pytest.fixture
def mock_config(mock_config_dict):
    """Provides a Config object based on the dictionary."""
    cfg = MagicMock(spec=Config)
    cfg.get = lambda key, default=None: _deep_get(mock_config_dict, key, default)
    cfg.config_data = mock_config_dict
    return cfg


def _deep_get(d: dict, keys: str, default=None):
    """Helper to access nested keys using dot notation."""
    key_parts = keys.split(".")
    val = d
    try:
        for key in key_parts:
            if isinstance(val, dict):
                val = val[key]
            else:
                if key_parts.index(key) < len(key_parts) - 1:
                    return default
                return default
        return val
    except (KeyError, TypeError):
        return default


@pytest.fixture
def mock_secrets():
    """Provides dummy secrets (using placeholders)."""
    return {
        "mock_hl": {"api_key": "hl_key", "api_secret": "hl_secret"},
        "mock_bp": {"api_key": "bp_key", "api_secret": "bp_secret"},
    }


@pytest.fixture
def mock_hl_api(mock_config, mock_secrets):
    """Mock API for Hyperliquid using MagicMock spec."""
    api_mock = MagicMock(spec=ExchangeAPI)
    api_mock.exchange_name = "mock_hl"
    # Configure necessary return values or side effects here or in tests
    api_mock.get_ticker = AsyncMock(return_value=None)
    api_mock.get_funding_rate = MagicMock(return_value=None)
    api_mock.place_order = MagicMock()
    api_mock.cancel_order = MagicMock()
    # Add get_order_status mock
    api_mock.get_order_status = AsyncMock(return_value=None)
    # Make get_balances and get_positions return awaitables (Fix 24)
    api_mock.get_balances = AsyncMock(return_value={})
    api_mock.get_positions = AsyncMock(return_value=[])
    api_mock.reset = MagicMock()
    # Add test helper mocks identified from failures
    api_mock.reset_failure = MagicMock()
    api_mock.set_mock_ticker = MagicMock()
    api_mock.clear_error = MagicMock()
    api_mock.set_mock_funding_rate = MagicMock()
    api_mock.set_open_orders_behavior = MagicMock()
    api_mock.set_mock_balance = MagicMock()
    api_mock.configure_error = MagicMock()
    # Mock other methods as needed by tests
    return api_mock


@pytest.fixture
def mock_bp_api(mock_config, mock_secrets):
    """Mock API for Backpack using MagicMock spec."""
    api_mock = MagicMock(spec=ExchangeAPI)
    api_mock.exchange_name = "mock_bp"
    # Configure necessary return values or side effects here or in tests
    api_mock.get_ticker = AsyncMock(return_value=None)
    api_mock.get_funding_rate = MagicMock(return_value=None)
    api_mock.place_order = MagicMock()
    api_mock.cancel_order = MagicMock()
    # Add get_order_status mock
    api_mock.get_order_status = AsyncMock(return_value=None)
    # Make get_balances and get_positions return awaitables (Fix 24)
    api_mock.get_balances = AsyncMock(return_value={})
    api_mock.get_positions = AsyncMock(return_value=[])
    api_mock.reset = MagicMock()
    # Add test helper mocks identified from failures
    api_mock.reset_failure = MagicMock()
    api_mock.set_mock_ticker = MagicMock()
    api_mock.clear_error = MagicMock()
    api_mock.set_mock_funding_rate = MagicMock()
    api_mock.set_open_orders_behavior = MagicMock()
    # Mock other methods as needed by tests
    api_mock.set_mock_balance = MagicMock()
    api_mock.configure_error = MagicMock()
    return api_mock


@pytest.fixture
def portfolio_tracker(mock_config):
    """Portfolio Tracker instance."""
    return PortfolioTracker(mock_config)


@pytest.fixture
def data_handler(mock_config, mock_hl_api, mock_bp_api):
    """Data Handler instance with mock APIs registered."""
    dh = DataHandler(mock_config)
    dh.register_api_client("mock_hl", mock_hl_api)
    dh.register_api_client("mock_bp", mock_bp_api)
    return dh


@pytest.fixture
def signal_generator(mock_config, data_handler):
    """Signal Generator instance."""
    return SignalGenerator(mock_config, data_handler)


@pytest.fixture
def risk_manager(mock_config, portfolio_tracker):
    """Risk Manager instance."""
    return RiskManager(mock_config, portfolio_tracker)


@pytest.fixture
def execution_handler(mock_config, portfolio_tracker, mock_hl_api, mock_bp_api):
    """Execution Handler instance with mock APIs registered."""
    eh = ExecutionHandler(mock_config, portfolio_tracker)
    portfolio_tracker.register_api_client("mock_hl", mock_hl_api)
    portfolio_tracker.register_api_client("mock_bp", mock_bp_api)
    eh.register_api_client("mock_hl", mock_hl_api)
    eh.register_api_client("mock_bp", mock_bp_api)
    return eh


# --- Integration Test ---


@pytest.mark.asyncio
async def test_happy_path_full_cycle(
    mock_config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    portfolio_tracker: PortfolioTracker,
    data_handler: DataHandler,
    signal_generator: SignalGenerator,
    risk_manager: RiskManager,
    execution_handler: ExecutionHandler,
):
    """Tests the full cycle - data -> signal -> risk -> execution -> portfolio."""

    # 1. Setup
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()
    assert mock_config.get("strategy.funding_rate.min_profit_threshold") == 0.1

    hl_symbol = mock_config.get("exchanges.mock_hl.symbols.BTC")
    bp_symbol = mock_config.get("exchanges.mock_bp.symbols.BTC")
    assert hl_symbol == "BTC-PERP"
    assert bp_symbol == "BTC-USDC"

    initial_usd_balance = Decimal("10000.0")
    mock_hl_api.set_mock_balance(
        Balance(
            asset="USD",
            total=float(initial_usd_balance),
            available=float(initial_usd_balance),
        )
    )
    mock_bp_api.set_mock_balance(
        Balance(
            asset="USDC",
            total=float(initial_usd_balance),
            available=float(initial_usd_balance),
        )
    )
    await portfolio_tracker.initialize()

    # Set mock data
    start_time = datetime.now(UTC)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)
    mock_hl_ticker_obj = create_mock_ticker(
        hl_symbol, 40099.0, 40101.0, 40100.0, start_time
    )
    mock_bp_ticker_obj = create_mock_ticker(
        bp_symbol, 40079.0, 40081.0, 40080.0, start_time
    )
    mock_hl_api.set_mock_ticker(mock_hl_ticker_obj)
    mock_bp_api.set_mock_ticker(mock_bp_ticker_obj)
    mock_hl_api.set_mock_funding_rate(
        create_mock_funding_rate(hl_symbol, 0.002, next_funding_ts)
    )
    mock_bp_api.set_mock_funding_rate(
        create_mock_funding_rate(bp_symbol, -0.002, next_funding_ts)
    )

    # Manually update DataHandler using INTERNAL symbol as key
    internal_symbol = "BTC"
    if "mock_hl" not in data_handler.funding_rates:
        data_handler.funding_rates["mock_hl"] = {}
    if "mock_bp" not in data_handler.funding_rates:
        data_handler.funding_rates["mock_bp"] = {}
    # Use internal_symbol for the key
    data_handler.funding_rates["mock_hl"][internal_symbol] = (Decimal("0.002"), start_time)
    data_handler.funding_rates["mock_bp"][internal_symbol] = (Decimal("-0.002"), start_time)
    if "mock_hl" not in data_handler.last_update_time:
        data_handler.last_update_time["mock_hl"] = {}
    if "funding_rate" not in data_handler.last_update_time["mock_hl"]:
        data_handler.last_update_time["mock_hl"]["funding_rate"] = {}
    # Use internal_symbol for the key
    data_handler.last_update_time["mock_hl"]["funding_rate"][internal_symbol] = start_time
    if "mock_bp" not in data_handler.last_update_time:
        data_handler.last_update_time["mock_bp"] = {}
    if "funding_rate" not in data_handler.last_update_time["mock_bp"]:
        data_handler.last_update_time["mock_bp"]["funding_rate"] = {}
    # Use internal_symbol for the key
    data_handler.last_update_time["mock_bp"]["funding_rate"][internal_symbol] = start_time
    if "mock_hl" not in data_handler.tickers:
        data_handler.tickers["mock_hl"] = {}
    if "mock_bp" not in data_handler.tickers:
        data_handler.tickers["mock_bp"] = {}
    # Use internal_symbol for the key
    data_handler.tickers["mock_hl"][internal_symbol] = mock_hl_ticker_obj
    data_handler.tickers["mock_bp"][internal_symbol] = mock_bp_ticker_obj
    if "ticker" not in data_handler.last_update_time["mock_hl"]:
        data_handler.last_update_time["mock_hl"]["ticker"] = {}
    # Use internal_symbol for the key
    data_handler.last_update_time["mock_hl"]["ticker"][internal_symbol] = start_time
    if "ticker" not in data_handler.last_update_time["mock_bp"]:
        data_handler.last_update_time["mock_bp"]["ticker"] = {}
    # Use internal_symbol for the key
    data_handler.last_update_time["mock_bp"]["ticker"][internal_symbol] = start_time

    # Lower profit threshold for this test to ensure signal generation
    original_profit_threshold = signal_generator.min_profit_threshold
    signal_generator.min_profit_threshold = Decimal("0.0")

    # --- Fix 39: Refresh last_update_time before generating --- 
    now_utc = datetime.now(UTC)
    # Use internal_symbol for the key
    data_handler.last_update_time["mock_hl"]["ticker"][internal_symbol] = now_utc
    data_handler.last_update_time["mock_bp"]["ticker"][internal_symbol] = now_utc
    data_handler.last_update_time["mock_hl"]["funding_rate"][internal_symbol] = now_utc
    data_handler.last_update_time["mock_bp"]["funding_rate"][internal_symbol] = now_utc
    # --- End Fix 39 ---

    opportunities = signal_generator.generate_opportunities()

    # Restore original threshold
    signal_generator.min_profit_threshold = original_profit_threshold

    # 4. Validate Signal
    assert len(opportunities) >= 1, "Should generate at least one opportunity"
    opportunity = opportunities[0]
    assert isinstance(opportunity, ArbitrageOpportunity)
    assert opportunity.symbol == internal_symbol
    assert opportunity.long_exchange == "mock_bp"
    assert opportunity.short_exchange == "mock_hl"
    assert opportunity.long_funding_rate == Decimal("-0.002")
    assert opportunity.short_funding_rate == Decimal("0.002")
    assert opportunity.net_funding_differential == pytest.approx(Decimal("0.004"))
    assert opportunity.expected_profit == pytest.approx(Decimal("4.0"))

    # 5. Validate & Size
    await portfolio_tracker.update() # Ensure tracker has latest balances from mocks
    # Manually calculate and set total capital from mock balances
    manual_total_capital = Decimal("0.0")
    if "mock_hl" in portfolio_tracker._balances and "USD" in portfolio_tracker._balances["mock_hl"]:
        manual_total_capital += portfolio_tracker._balances["mock_hl"]["USD"].total
    if "mock_bp" in portfolio_tracker._balances and "USDC" in portfolio_tracker._balances["mock_bp"]:
         manual_total_capital += portfolio_tracker._balances["mock_bp"]["USDC"].total
    portfolio_tracker._total_capital = manual_total_capital
    logger.info(f"Manually set portfolio_tracker._total_capital to: {portfolio_tracker._total_capital}") # Add log

    sized_opportunities = risk_manager.validate_opportunities([opportunity])
    assert len(sized_opportunities) == 1, "Opportunity should be valid and sized"
    sized_opportunity = sized_opportunities[0]
    assert isinstance(sized_opportunity, SizedOpportunity)
    assert sized_opportunity.long_size > 0
    assert sized_opportunity.short_size > 0
    assert sized_opportunity.long_size <= mock_config.get(
        "risk_manager.max_position_size"
    )
    assert sized_opportunity.short_size <= mock_config.get(
        "risk_manager.max_position_size"
    )

    # 6. Execute
    execution = await execution_handler.execute_opportunity(sized_opportunity)

    # 7. Verify Execution
    assert execution.status == ExecutionStatus.COMPLETED, (
        f"Execution failed: {execution.error_message}"
    )
    assert execution.long_order_id is not None
    assert execution.short_order_id is not None
    long_order = await mock_bp_api.get_order(execution.long_order_id)
    short_order = await mock_hl_api.get_order(execution.short_order_id)
    assert long_order is not None
    assert short_order is not None
    assert long_order.status == OrderStatus.FILLED
    assert short_order.status == OrderStatus.FILLED
    assert long_order.side == OrderSide.BUY
    assert short_order.side == OrderSide.SELL
    assert long_order.symbol == bp_symbol
    assert short_order.symbol == hl_symbol
    assert (
        long_order.filled_quantity
        == pytest.approx(
            Decimal(sized_opportunity.long_size) / long_order.price, rel=Decimal("1e-5")
        )
        if long_order.price
        else False
    )
    assert (
        short_order.filled_quantity
        == pytest.approx(
            Decimal(sized_opportunity.short_size) / short_order.price,
            rel=Decimal("1e-5"),
        )
        if short_order.price
        else False
    )

    # 8. Verify Portfolio State
    await portfolio_tracker.update()
    bp_positions = portfolio_tracker.get_positions_by_symbol("mock_bp", bp_symbol)
    hl_positions = portfolio_tracker.get_positions_by_symbol("mock_hl", hl_symbol)
    assert len(bp_positions) >= 1, "Backpack position not found in tracker"
    assert len(hl_positions) >= 1, "HyperLiquid position not found in tracker"
    bp_pos = bp_positions[0]
    hl_pos = hl_positions[0]
    assert bp_pos.side == OrderSide.BUY
    assert hl_pos.side == OrderSide.SELL
    assert bp_pos.size == pytest.approx(long_order.filled_quantity)
    assert hl_pos.size == pytest.approx(-short_order.filled_quantity)
    assert bp_pos.entry_price == pytest.approx(long_order.price)
    assert hl_pos.entry_price == pytest.approx(short_order.price)
    logger.info("Happy path integration test completed successfully.")


@pytest.mark.asyncio
async def test_api_error_during_placement(
    mock_config: Config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    data_handler: DataHandler,
    signal_generator: SignalGenerator,
    risk_manager: RiskManager,
    portfolio_tracker: PortfolioTracker,
    execution_handler: ExecutionHandler,
    caplog,
):
    """Tests the workflow when one exchange API fails during order placement."""
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f"exchanges.mock_hl.symbols.{symbol_key}", "BTC-PERP")
    bp_symbol = mock_config.get(f"exchanges.mock_bp.symbols.{symbol_key}", "BTC-PERP")
    start_time = datetime.now(UTC)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()
    mock_hl_api.reset_failure()
    mock_bp_api.reset_failure()
    mock_hl_api.set_mock_ticker(
        create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, start_time)
    )
    mock_hl_api.set_mock_funding_rate(
        create_mock_funding_rate(hl_symbol, 0.002, next_funding_ts)
    )
    mock_hl_api.set_mock_balance(
        Balance(asset="USD", total=10000.0, available=10000.0)
    )
    mock_hl_api.set_open_orders_behavior("fill_immediately")
    mock_bp_api.set_mock_ticker(
        create_mock_ticker(bp_symbol, 10004.0, 10006.0, 10005.0, start_time)
    )
    mock_bp_api.set_mock_funding_rate(
        create_mock_funding_rate(bp_symbol, -0.002, next_funding_ts)
    )
    mock_bp_api.set_mock_balance(
        Balance(asset="USDC", total=10000.0, available=10000.0)
    )
    mock_bp_api.configure_error("place_order", MockAPIError("BP connection failed"))

    # Manually set data in DataHandler for the test using INTERNAL symbol key
    internal_symbol = "BTC"
    hl_ticker = create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, start_time)
    bp_ticker = create_mock_ticker(bp_symbol, 10004.0, 10006.0, 10005.0, start_time)
    # Use internal_symbol for the key
    data_handler.tickers = {
        "mock_hl": {internal_symbol: hl_ticker},
        "mock_bp": {internal_symbol: bp_ticker}
    }
    # Use internal_symbol for the key
    data_handler.last_update_time = {
        "mock_hl": {"ticker": {internal_symbol: start_time}},
        "mock_bp": {"ticker": {internal_symbol: start_time}}
    }
    # Add funding rates too if needed by signal generator
    # Use internal_symbol for the key
    data_handler.funding_rates = {
        "mock_hl": {internal_symbol: (Decimal("0.002"), start_time)},
        "mock_bp": {internal_symbol: (Decimal("-0.002"), start_time)}
    }
    if "mock_hl" not in data_handler.last_update_time:
        data_handler.last_update_time["mock_hl"] = {}
    if "mock_bp" not in data_handler.last_update_time:
        data_handler.last_update_time["mock_bp"] = {}
    # Use internal_symbol for the key
    data_handler.last_update_time["mock_hl"]["funding_rate"] = {internal_symbol: start_time}
    data_handler.last_update_time["mock_bp"]["funding_rate"] = {internal_symbol: start_time}

    await portfolio_tracker.initialize()
    await portfolio_tracker.update()
    # Manually calculate and set total capital from mock balances
    manual_total_capital = Decimal("0.0")
    if "mock_hl" in portfolio_tracker._balances and "USD" in portfolio_tracker._balances["mock_hl"]:
        manual_total_capital += portfolio_tracker._balances["mock_hl"]["USD"].total
    if "mock_bp" in portfolio_tracker._balances and "USDC" in portfolio_tracker._balances["mock_bp"]:
        manual_total_capital += portfolio_tracker._balances["mock_bp"]["USDC"].total
    portfolio_tracker._total_capital = manual_total_capital
    logger.info(f"Manually set portfolio_tracker._total_capital to: {portfolio_tracker._total_capital}") # Add log

    # --- Generate Opportunities (was accidentally removed) ---
    # --- Fix 39: Refresh last_update_time before generating ---
    now_utc = datetime.now(UTC)
    data_handler.last_update_time["mock_hl"]["ticker"][internal_symbol] = now_utc
    data_handler.last_update_time["mock_bp"]["ticker"][internal_symbol] = now_utc
    data_handler.last_update_time["mock_hl"]["funding_rate"][internal_symbol] = now_utc
    data_handler.last_update_time["mock_bp"]["funding_rate"][internal_symbol] = now_utc
    # --- End Fix 39 ---
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1
    opportunity = opportunities[0]
    assert opportunity.long_exchange == "mock_bp"
    assert opportunity.short_exchange == "mock_hl"
    # --- End Generate Opportunities ---

    initial_balances = copy.deepcopy(portfolio_tracker._balances)
    initial_positions = copy.deepcopy(portfolio_tracker._positions)
    sized_opportunities = risk_manager.validate_opportunities(opportunities)
    assert len(sized_opportunities) >= 1
    sized_opportunity = sized_opportunities[0]
    caplog.set_level(logging.ERROR)
    with patch.object(
        mock_bp_api, "place_order", side_effect=MockAPIError("BP connection failed")
    ) as mock_place_long:
        trade_execution_result = await execution_handler.execute_opportunity(
            sized_opportunity
        )
        mock_place_long.assert_called()
    assert trade_execution_result.status == ExecutionStatus.FAILED, (
        "Execution should have failed"
    )
    assert trade_execution_result.error_message is not None
    assert "Failed to place long order" in trade_execution_result.error_message
    assert any(
        "Unexpected error placing order on mock_bp" in record.message
        for record in caplog.records
    )
    assert any("BP connection failed" in record.message for record in caplog.records)
    await asyncio.sleep(0.1)
    await portfolio_tracker.update()
    final_balances = copy.deepcopy(portfolio_tracker._balances)
    final_positions = copy.deepcopy(portfolio_tracker._positions)
    assert portfolio_tracker.get_position("mock_bp", bp_symbol) is None
    assert portfolio_tracker.get_position("mock_hl", hl_symbol) is None
    assert trade_execution_result.short_order_id is None
    mock_hl_final_balances = await mock_hl_api.get_balances()
    mock_bp_final_balances = await mock_bp_api.get_balances()
    mock_hl_initial_balances = initial_balances.get("mock_hl", {})
    mock_bp_initial_balances = initial_balances.get("mock_bp", {})
    assert mock_hl_final_balances.get("USD") == mock_hl_initial_balances.get("USD")
    assert mock_bp_final_balances.get("USDC") == mock_bp_initial_balances.get("USDC")
    mock_bp_api.clear_error()


@pytest.mark.asyncio
async def test_insufficient_balance(
    mock_config: Config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    data_handler: DataHandler,
    signal_generator: SignalGenerator,
    risk_manager: RiskManager,
    portfolio_tracker: PortfolioTracker,
    caplog,
):
    """Tests that an opportunity is rejected if there's insufficient balance."""
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f"exchanges.mock_hl.symbols.{symbol_key}", "BTC-PERP")
    bp_symbol = mock_config.get(f"exchanges.mock_bp.symbols.{symbol_key}", "BTC-PERP")
    start_time = datetime.now(UTC)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()

    # --- Define Mock Data Objects ---
    mock_hl_ticker = create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, start_time)
    mock_bp_ticker = create_mock_ticker(bp_symbol, 10004.0, 10006.0, 10005.0, start_time)
    mock_hl_funding = create_mock_funding_rate(hl_symbol, 0.002, next_funding_ts)
    mock_bp_funding = create_mock_funding_rate(bp_symbol, -0.002, next_funding_ts)

    # --- Configure Mock API Responses ---
    mock_hl_api.get_ticker.return_value = mock_hl_ticker
    mock_bp_api.get_ticker.return_value = mock_bp_ticker
    mock_hl_api.get_funding_rate.return_value = mock_hl_funding
    mock_bp_api.get_funding_rate.return_value = mock_bp_funding

    mock_hl_api.set_mock_ticker(mock_hl_ticker) # Keep this if MockExchangeAPI uses it internally
    mock_hl_api.set_mock_funding_rate(mock_hl_funding)
    mock_bp_api.set_mock_ticker(mock_bp_ticker)
    mock_bp_api.set_mock_funding_rate(mock_bp_funding)

    # --- Set Mock Balances (Low for BP) ---
    mock_hl_api.set_mock_balance(
        Balance(asset="USD", total=10000.0, available=10000.0)
    )
    mock_bp_api.set_mock_balance(
        Balance(asset="USDC", total=1.0, available=1.0)
    )

    # --- Manually Populate DataHandler for Test ---
    now = datetime.now(UTC) # Use consistent timestamp
    internal_symbol = "BTC" # Use internal symbol
    # Use internal_symbol for the key
    data_handler.tickers[mock_hl_api.exchange_name] = {internal_symbol: mock_hl_ticker}
    data_handler.tickers[mock_bp_api.exchange_name] = {internal_symbol: mock_bp_ticker}
    data_handler.funding_rates[mock_hl_api.exchange_name] = {internal_symbol: (mock_hl_funding.funding_rate, mock_hl_funding.next_funding_time)}
    data_handler.funding_rates[mock_bp_api.exchange_name] = {internal_symbol: (mock_bp_funding.funding_rate, mock_bp_funding.next_funding_time)}
    data_handler.last_update_time[mock_hl_api.exchange_name]["ticker"] = {internal_symbol: now}
    data_handler.last_update_time[mock_bp_api.exchange_name]["ticker"] = {internal_symbol: now}
    data_handler.last_update_time[mock_hl_api.exchange_name]["funding_rate"] = {internal_symbol: now}
    data_handler.last_update_time[mock_bp_api.exchange_name]["funding_rate"] = {internal_symbol: now}
    # --- End DataHandler Population ---

    # --- Fix 39: Refresh last_update_time before generating ---
    now_utc = datetime.now(UTC)
    data_handler.last_update_time["mock_hl"]["ticker"][internal_symbol] = now_utc
    data_handler.last_update_time["mock_bp"]["ticker"][internal_symbol] = now_utc
    data_handler.last_update_time["mock_hl"]["funding_rate"][internal_symbol] = now_utc
    data_handler.last_update_time["mock_bp"]["funding_rate"][internal_symbol] = now_utc
    # --- End Fix 39 ---

    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1
    opportunity = opportunities[0]
    assert opportunity.long_exchange == "mock_bp"
    assert opportunity.short_exchange == "mock_hl"
    await portfolio_tracker.initialize()
    await portfolio_tracker.update()
    caplog.set_level(logging.WARNING)
    sized_opportunities = risk_manager.validate_opportunities(opportunities)
    assert len(sized_opportunities) == 0, (
        "Opportunity should be rejected due to insufficient balance"
    )
    assert any(
        "Cannot size opportunity: total capital is zero or negative" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_partial_fill(
    mock_config: Config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    data_handler: DataHandler,
    signal_generator: SignalGenerator,
    risk_manager: RiskManager,
    portfolio_tracker: PortfolioTracker,
    execution_handler: ExecutionHandler,
    caplog,
):
    """
    Test scenario where one leg of the trade fills partially initially,
    requiring compensation logic (which should eventually fully fill or handle).
    """
    caplog.set_level(logging.DEBUG)  # Set log level to DEBUG

    # --- Setup Mock Data ---
    now = datetime.now(UTC)
    symbol = "BTC"
    hl_symbol = mock_config.get(f"exchanges.mock_hl.symbols.{symbol}")
    bp_symbol = mock_config.get(f"exchanges.mock_bp.symbols.{symbol}")

    mock_hl_ticker = create_mock_ticker(hl_symbol, 40000, 40001, 40000.5, now)
    mock_bp_ticker = create_mock_ticker(bp_symbol, 40005, 40006, 40005.5, now)
    mock_hl_funding = create_mock_funding_rate(hl_symbol, 0.0001, now + timedelta(hours=1))
    mock_bp_funding = create_mock_funding_rate(bp_symbol, -0.0001, now + timedelta(hours=1))

    # Configure mocks to return the data
    mock_hl_api.get_ticker.return_value = mock_hl_ticker
    mock_bp_api.get_ticker.return_value = mock_bp_ticker
    mock_hl_api.get_funding_rate.return_value = mock_hl_funding
    mock_bp_api.get_funding_rate.return_value = mock_bp_funding

    # Configure initial balances (ensure sufficient funds)
    mock_hl_api.get_balances.return_value = {"USD": Balance(asset="USD", total=Decimal("10000"), available=Decimal("10000"))}
    mock_bp_api.get_balances.return_value = {"USDC": Balance(asset="USDC", total=Decimal("10000"), available=Decimal("10000"))}
    await portfolio_tracker.update() # Ensure tracker has balances

    # --- Manually Populate DataHandler for Test ---
    now = datetime.now(UTC) # Use consistent timestamp
    internal_symbol = "BTC" # Use internal symbol
    # Use internal_symbol for the key
    data_handler.tickers[mock_hl_api.exchange_name] = {internal_symbol: mock_hl_ticker}
    data_handler.tickers[mock_bp_api.exchange_name] = {internal_symbol: mock_bp_ticker}
    data_handler.funding_rates[mock_hl_api.exchange_name] = {internal_symbol: (mock_hl_funding.funding_rate, mock_hl_funding.next_funding_time)}
    data_handler.funding_rates[mock_bp_api.exchange_name] = {internal_symbol: (mock_bp_funding.funding_rate, mock_bp_funding.next_funding_time)}
    data_handler.last_update_time[mock_hl_api.exchange_name]["ticker"] = {internal_symbol: now}
    data_handler.last_update_time[mock_bp_api.exchange_name]["ticker"] = {internal_symbol: now}
    data_handler.last_update_time[mock_hl_api.exchange_name]["funding_rate"] = {internal_symbol: now}
    data_handler.last_update_time[mock_bp_api.exchange_name]["funding_rate"] = {internal_symbol: now}
    # --- End DataHandler Population ---

    # Configure order placement responses
    # Long leg (HL) - Simulate partial fill initially
    partial_fill_qty = Decimal("0.05") # Half of the typical 0.1 size
    long_order_id_hl = "hl_long_partial"
    mock_hl_api.place_order.side_effect = [
        # First attempt: Partial Fill
        {"id": long_order_id_hl, "status": OrderStatus.PARTIALLY_FILLED, "filledQty": partial_fill_qty, "avgFillPrice": Decimal("40001.0"), "symbol": hl_symbol, "side": OrderSide.BUY, "type": "MARKET"},
        # Subsequent status check simulation (can refine if needed)
        {"id": long_order_id_hl, "status": OrderStatus.FILLED, "filledQty": Decimal("0.1"), "avgFillPrice": Decimal("40001.0"), "symbol": hl_symbol, "side": OrderSide.BUY, "type": "MARKET"} # Assume it fills later
    ]
    # Short leg (BP) - Simulate immediate full fill (Needs awaitable)
    short_order_id_bp = "bp_short_full"
    bp_order_response = {"id": short_order_id_bp, "status": OrderStatus.FILLED, "filledQty": Decimal("0.1"), "avgFillPrice": Decimal("40005.0"), "symbol": bp_symbol, "side": OrderSide.SELL, "type": "MARKET"}
    mock_bp_api.place_order.return_value = AsyncMock(return_value=bp_order_response) # Wrap dict in AsyncMock

    # Configure get_order_status mocks
    mock_hl_api.get_order_status.side_effect = [
         # First check: still partially filled
        {"id": long_order_id_hl, "status": OrderStatus.PARTIALLY_FILLED, "filledQty": partial_fill_qty, "avgFillPrice": Decimal("40001.0"), "symbol": hl_symbol, "side": OrderSide.BUY, "type": "MARKET"},
         # Second check: now fully filled
        {"id": long_order_id_hl, "status": OrderStatus.FILLED, "filledQty": Decimal("0.1"), "avgFillPrice": Decimal("40001.0"), "symbol": hl_symbol, "side": OrderSide.BUY, "type": "MARKET"}
    ]
    mock_bp_api.get_order_status.return_value = {"id": short_order_id_bp, "status": OrderStatus.FILLED, "filledQty": Decimal("0.1"), "avgFillPrice": Decimal("40005.0"), "symbol": bp_symbol, "side": OrderSide.SELL, "type": "MARKET"}


    # --- Execute Test ---
    # 1. Scan for opportunities
    logger.info("Scanning for opportunities...")
    opportunities = signal_generator.generate_opportunities()
    logger.info(f"Found {len(opportunities)} opportunities.")
    assert len(opportunities) >= 1, "Should find at least one opportunity with mock data"
    opportunity = opportunities[0]

    # 2. Validate Signal
    assert isinstance(opportunity, ArbitrageOpportunity)
    assert opportunity.symbol == internal_symbol
    assert opportunity.long_exchange == "mock_bp"
    assert opportunity.short_exchange == "mock_hl"
    assert opportunity.long_funding_rate == mock_bp_funding.funding_rate
    assert opportunity.short_funding_rate == mock_hl_funding.funding_rate
    expected_nfd = mock_hl_funding.funding_rate - mock_bp_funding.funding_rate
    assert opportunity.net_funding_differential == pytest.approx(expected_nfd)
    expected_profit_calc = expected_nfd * Decimal("1000")
    assert opportunity.expected_profit == pytest.approx(expected_profit_calc)

    # 3. Validate & Size
    await portfolio_tracker.initialize()
    sized_opportunities = risk_manager.validate_opportunities([opportunity])
    assert len(sized_opportunities) == 1, "Opportunity should be valid and sized"
    sized_opportunity = sized_opportunities[0]
    assert isinstance(sized_opportunity, SizedOpportunity)
    assert sized_opportunity.long_size > 0
    assert sized_opportunity.short_size > 0
    assert sized_opportunity.long_size <= mock_config.get(
        "risk_manager.max_position_size"
    )
    assert sized_opportunity.short_size <= mock_config.get(
        "risk_manager.max_position_size"
    )

    # 4. Execute
    execution = await execution_handler.execute_opportunity(sized_opportunity)

    # 5. Verify Execution
    assert execution.status == ExecutionStatus.COMPLETED, (
        f"Execution failed: {execution.error_message}"
    )
    assert execution.long_order_id is not None
    assert execution.short_order_id is not None
    long_order = await mock_bp_api.get_order(execution.long_order_id)
    short_order = await mock_hl_api.get_order(execution.short_order_id)
    assert long_order is not None
    assert short_order is not None
    assert long_order.status == OrderStatus.FILLED
    assert short_order.status == OrderStatus.FILLED
    assert long_order.side == OrderSide.BUY
    assert short_order.side == OrderSide.SELL
    assert long_order.symbol == bp_symbol
    assert short_order.symbol == hl_symbol
    assert (
        long_order.filled_quantity
        == pytest.approx(
            Decimal(sized_opportunity.long_size) / long_order.price, rel=Decimal("1e-5")
        )
        if long_order.price
        else False
    )
    assert (
        short_order.filled_quantity
        == pytest.approx(
            Decimal(sized_opportunity.short_size) / short_order.price,
            rel=Decimal("1e-5"),
        )
        if short_order.price
        else False
    )

    # 6. Verify Portfolio State
    await portfolio_tracker.update()
    bp_positions = portfolio_tracker.get_positions_by_symbol("mock_bp", bp_symbol)
    hl_positions = portfolio_tracker.get_positions_by_symbol("mock_hl", hl_symbol)
    assert len(bp_positions) >= 1, "Backpack position not found in tracker"
    assert len(hl_positions) >= 1, "HyperLiquid position not found in tracker"
    bp_pos = bp_positions[0]
    hl_pos = hl_positions[0]
    assert bp_pos.side == OrderSide.BUY
    assert hl_pos.side == OrderSide.SELL
    assert bp_pos.size == pytest.approx(long_order.filled_quantity)
    assert hl_pos.size == pytest.approx(-short_order.filled_quantity)
    assert bp_pos.entry_price == pytest.approx(long_order.price)
    assert hl_pos.entry_price == pytest.approx(short_order.price)
    logger.info("Partial fill integration test completed successfully.")


@pytest.mark.asyncio
async def test_execution_failure_compensation(
    mock_config: Config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    data_handler: DataHandler,
    signal_generator: SignalGenerator,
    risk_manager: RiskManager,
    portfolio_tracker: PortfolioTracker,
    execution_handler: ExecutionHandler,
    caplog,
):
    """
    Tests compensation logic when one leg fails entirely during execution.
    Ensures the successfully executed leg is reversed.
    """
    caplog.set_level(logging.DEBUG)

    # --- Setup Mock Data ---
    now = datetime.now(UTC)
    symbol = "BTC"
    hl_symbol = mock_config.get(f"exchanges.mock_hl.symbols.{symbol}")
    bp_symbol = mock_config.get(f"exchanges.mock_bp.symbols.{symbol}")

    mock_hl_ticker = create_mock_ticker(hl_symbol, 40000, 40001, 40000.5, now)
    mock_bp_ticker = create_mock_ticker(bp_symbol, 40005, 40006, 40005.5, now)
    mock_hl_funding = create_mock_funding_rate(hl_symbol, 0.0001, now + timedelta(hours=1))
    mock_bp_funding = create_mock_funding_rate(bp_symbol, -0.0001, now + timedelta(hours=1))

    # Configure mocks to return the data
    mock_hl_api.get_ticker.return_value = mock_hl_ticker
    mock_bp_api.get_ticker.return_value = mock_bp_ticker
    mock_hl_api.get_funding_rate.return_value = mock_hl_funding
    mock_bp_api.get_funding_rate.return_value = mock_bp_funding

    # Configure initial balances (ensure sufficient funds)
    initial_hl_balance = Balance(asset="USD", total=Decimal("10000"), available=Decimal("10000"))
    initial_bp_balance = Balance(asset="USDC", total=Decimal("10000"), available=Decimal("10000"))
    mock_hl_api.get_balances.return_value = {"USD": initial_hl_balance}
    mock_bp_api.get_balances.return_value = {"USDC": initial_bp_balance}
    await portfolio_tracker.update() # Ensure tracker has balances

    # --- Manually Populate DataHandler for Test ---
    now = datetime.now(UTC) # Use consistent timestamp
    internal_symbol = "BTC" # Use internal symbol
    # Use internal_symbol for the key
    data_handler.tickers[mock_hl_api.exchange_name] = {internal_symbol: mock_hl_ticker}
    data_handler.tickers[mock_bp_api.exchange_name] = {internal_symbol: mock_bp_ticker}
    data_handler.funding_rates[mock_hl_api.exchange_name] = {internal_symbol: (mock_hl_funding.funding_rate, mock_hl_funding.next_funding_time)}
    data_handler.funding_rates[mock_bp_api.exchange_name] = {internal_symbol: (mock_bp_funding.funding_rate, mock_bp_funding.next_funding_time)}
    data_handler.last_update_time[mock_hl_api.exchange_name]["ticker"] = {internal_symbol: now}
    data_handler.last_update_time[mock_bp_api.exchange_name]["ticker"] = {internal_symbol: now}
    data_handler.last_update_time[mock_hl_api.exchange_name]["funding_rate"] = {internal_symbol: now}
    data_handler.last_update_time[mock_bp_api.exchange_name]["funding_rate"] = {internal_symbol: now}
    # --- End DataHandler Population ---

    # --- Configure Mocks for Failure Scenario ---
    # Long leg (HL) - Simulate successful fill
    long_order_id_hl = "hl_long_success"
    long_fill_qty = Decimal("0.1")
    long_fill_price = Decimal("40001.0")
    mock_hl_api.place_order.return_value = {"id": long_order_id_hl, "status": OrderStatus.FILLED, "filledQty": long_fill_qty, "avgFillPrice": long_fill_price, "symbol": hl_symbol, "side": OrderSide.BUY, "type": "MARKET"}

    # Short leg (BP) - Simulate failure (e.g., API error)
    # mock_bp_api.place_order.side_effect = MockAPIError("Simulated BP execution failure", APIErrorCode.EXCHANGE_SPECIFIC)
    # Side effect needs to be an awaitable that raises the error
    bp_error_mock = AsyncMock()
    bp_error_mock.side_effect = MockAPIError("Simulated BP execution failure", APIErrorCode.EXCHANGE_SPECIFIC)
    mock_bp_api.place_order = bp_error_mock # Assign the async mock raising the error

    # Mock get_order_status for the successful long leg
    mock_hl_api.get_order_status.return_value = {"id": long_order_id_hl, "status": OrderStatus.FILLED, "filledQty": long_fill_qty, "avgFillPrice": long_fill_price, "symbol": hl_symbol, "side": OrderSide.BUY, "type": "MARKET"}
    # BP order status won't be checked as placement failed
    mock_bp_api.get_order_status.return_value = None

    # Mock compensation order placement (reversing the long HL leg)
    compensating_order_id_hl = "hl_compensate_sell"
    # Use AsyncMock for the side_effect if the target method is async
    # We need to simulate placing the *compensation* order successfully after the initial short fails.
    # Reset the side effect for place_order on hl_api specifically for the compensation call.
    async def place_order_side_effect_hl(*args, **kwargs):
        # First call is the initial long, which succeeds
        if kwargs.get('side') == OrderSide.BUY:
            return {"id": long_order_id_hl, "status": OrderStatus.FILLED, "filledQty": long_fill_qty, "avgFillPrice": long_fill_price, "symbol": hl_symbol, "side": OrderSide.BUY, "type": "MARKET"}
        # Second call is the compensation sell, which also succeeds
        elif kwargs.get('side') == OrderSide.SELL:
             return {"id": compensating_order_id_hl, "status": OrderStatus.FILLED, "filledQty": long_fill_qty, "avgFillPrice": mock_hl_ticker.ask, "symbol": hl_symbol, "side": OrderSide.SELL, "type": "LIMIT" if mock_config.get("execution_handler.compensation.use_limit_orders") else "MARKET"}
        else:
            raise ValueError("Unexpected order side in mock")

    mock_hl_api.place_order.side_effect = place_order_side_effect_hl

    # Mock get_order_status for the compensation order
    # Need to handle multiple calls to get_order_status for HL
    async def get_order_status_side_effect_hl(*args, **kwargs):
        order_id = args[1] # Assuming order_id is the second arg
        if order_id == long_order_id_hl:
             return {"id": long_order_id_hl, "status": OrderStatus.FILLED, "filledQty": long_fill_qty, "avgFillPrice": long_fill_price, "symbol": hl_symbol, "side": OrderSide.BUY, "type": "MARKET"}
        elif order_id == compensating_order_id_hl:
            return {"id": compensating_order_id_hl, "status": OrderStatus.FILLED, "filledQty": long_fill_qty, "avgFillPrice": mock_hl_ticker.ask, "symbol": hl_symbol, "side": OrderSide.SELL, "type": "LIMIT"}
        else:
            return None # Or raise error for unexpected id

    mock_hl_api.get_order_status.side_effect = get_order_status_side_effect_hl

    # --- Execute Test ---
    # 1. Scan for opportunities
    logger.info("Scanning for opportunities...")
    opportunities = signal_generator.generate_opportunities()
    logger.info(f"Found {len(opportunities)} opportunities.")
    assert len(opportunities) >= 1, "Should find at least one opportunity with mock data"
    opportunity = opportunities[0]

    # 2. Validate Signal
    assert isinstance(opportunity, ArbitrageOpportunity)
    assert opportunity.symbol == internal_symbol
    assert opportunity.long_exchange == "mock_bp"
    assert opportunity.short_exchange == "mock_hl"
    assert opportunity.long_funding_rate == mock_bp_funding.funding_rate
    assert opportunity.short_funding_rate == mock_hl_funding.funding_rate
    expected_nfd = mock_hl_funding.funding_rate - mock_bp_funding.funding_rate
    assert opportunity.net_funding_differential == pytest.approx(expected_nfd)
    expected_profit_calc = expected_nfd * Decimal("1000")
    assert opportunity.expected_profit == pytest.approx(expected_profit_calc)

    # 3. Validate & Size
    sized_opportunities = risk_manager.validate_opportunities([opportunity])
    assert len(sized_opportunities) == 1, "Opportunity should be valid and sized"
    sized_opportunity = sized_opportunities[0]
    assert isinstance(sized_opportunity, SizedOpportunity)
    assert sized_opportunity.long_size > 0
    assert sized_opportunity.short_size > 0
    assert sized_opportunity.long_size <= mock_config.get(
        "risk_manager.max_position_size"
    )
    assert sized_opportunity.short_size <= mock_config.get(
        "risk_manager.max_position_size"
    )

    # 4. Execute
    execution = await execution_handler.execute_opportunity(sized_opportunity)

    # 5. Verify Execution
    assert execution.status == ExecutionStatus.COMPLETED, (
        f"Execution failed: {execution.error_message}"
    )
    assert execution.long_order_id is not None
    assert execution.short_order_id is not None
    long_order = await mock_bp_api.get_order(execution.long_order_id)
    short_order = await mock_hl_api.get_order(execution.short_order_id)
    assert long_order is not None
    assert short_order is not None
    assert long_order.status == OrderStatus.FILLED
    assert short_order.status == OrderStatus.FILLED
    assert long_order.side == OrderSide.BUY
    assert short_order.side == OrderSide.SELL
    assert long_order.symbol == bp_symbol
    assert short_order.symbol == hl_symbol
    assert (
        long_order.filled_quantity
        == pytest.approx(
            Decimal(sized_opportunity.long_size) / long_order.price, rel=Decimal("1e-5")
        )
        if long_order.price
        else False
    )
    assert (
        short_order.filled_quantity
        == pytest.approx(
            Decimal(sized_opportunity.short_size) / short_order.price,
            rel=Decimal("1e-5"),
        )
        if short_order.price
        else False
    )

    # 6. Verify Portfolio State
    await portfolio_tracker.update()
    bp_positions = portfolio_tracker.get_positions_by_symbol("mock_bp", bp_symbol)
    hl_positions = portfolio_tracker.get_positions_by_symbol("mock_hl", hl_symbol)
    assert len(bp_positions) >= 1, "Backpack position not found in tracker"
    assert len(hl_positions) >= 1, "HyperLiquid position not found in tracker"
    bp_pos = bp_positions[0]
    hl_pos = hl_positions[0]
    assert bp_pos.side == OrderSide.BUY
    assert hl_pos.side == OrderSide.SELL
    assert bp_pos.size == pytest.approx(long_order.filled_quantity)
    assert hl_pos.size == pytest.approx(-short_order.filled_quantity)
    assert bp_pos.entry_price == pytest.approx(long_order.price)
    assert hl_pos.entry_price == pytest.approx(short_order.price)
    logger.info("Execution failure compensation test completed successfully.")


# --- Add More Failure Test Cases Below ---
pass  # Ensure file doesn't end abruptly
