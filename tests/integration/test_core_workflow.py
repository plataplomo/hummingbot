import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, AsyncMock, patch, call
from typing import Dict, List, Any, Tuple
import logging # Import logging
import copy # Import copy module

# Core Components
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.signal_generator import SignalGenerator, ArbitrageOpportunity
from cyberdelta.core.risk_manager import RiskManager, SizedOpportunity
from cyberdelta.core.execution_handler import ExecutionHandler, ExecutionStatus
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.strategy_manager import StrategyManager

# Models
from cyberdelta.core.models import (
    Ticker, FundingRate, Balance, Position, Order, OrderSide, OrderType, OrderStatus
)

# Mocks & Config
from tests.integration.mocks.mock_exchange import MockExchangeAPI, MockAPIError # Import MockAPIError
from cyberdelta.utils.config import Config # Assuming Config class is used
from cyberdelta.apis.base import APIError, APIErrorCode # <--- Added APIErrorCode here

# Helper Functions
def create_mock_ticker(symbol, bid, ask, price, timestamp): # Corrected price arg
    # Convert inputs to Decimal
    return Ticker(symbol=symbol, bid=Decimal(bid), ask=Decimal(ask), price=Decimal(price), timestamp=timestamp)

def create_mock_funding_rate(symbol, rate, next_time):
    # Convert rate to Decimal
    return FundingRate(symbol=symbol, funding_rate=Decimal(rate), next_funding_time=next_time)

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
                "symbols": {"BTC": "BTC-PERP"},
                "fee_rate": 0.0005,
                "collateral_asset": "USD"
            },
            "mock_bp": {
                "enabled": True,
                "api_key": "mock_bp_key",
                "api_secret": "mock_bp_secret",
                "api_base_url": "mock",
                "ws_url": "mock",
                "symbols": {"BTC": "BTC-PERP"},
                "fee_rate": 0.0005,
                "collateral_asset": "USDC"
            }
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
                "risk_aversion": 1.0
            }
        },
        "portfolio": {
            "reconciliation_interval": 300
        },
        "risk_manager": {
            "max_total_exposure": 20000.0,
            "max_exchange_exposure": 10000.0,
            "max_position_size": 5000.0,
            "circuit_breaker_threshold": 0.1,
            "min_exchange_balance": 10.0
        },
        "execution_handler": {
            "order_timeout_seconds": 30,
            "max_slippage_pct": 0.001,
            "max_retries": 2,
            "retry_delay_base_sec": 0.05,
            "compensation": { # Add compensation config section
                "use_limit_orders": True,
                "limit_price_offset_pct": 0.05
            }
        },
        "data": {
            "staleness_thresholds": {
                "ticker": 60,
                "funding_rate": 3600,
                "orderbook": 30
            }
        },
         "safety_systems": {
             "circuit_breakers": {"enabled": False},
             "position_reconciliation": {"enabled": False},
             "balance_monitoring": {"enabled": False}
        }
    }

@pytest.fixture
def mock_config(mock_config_dict):
    """Provides a Config object based on the dictionary."""
    cfg = MagicMock(spec=Config)
    cfg.get = lambda key, default=None: _deep_get(mock_config_dict, key, default)
    cfg.config_data = mock_config_dict 
    return cfg

def _deep_get(d: Dict, keys: str, default=None):
    """Helper to access nested keys using dot notation."""
    key_parts = keys.split('.')
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
        "mock_bp": {"api_key": "bp_key", "api_secret": "bp_secret"}
    }

@pytest.fixture
def mock_hl_api(mock_config, mock_secrets):
    """Mock API for Hyperliquid, passes full config."""
    return MockExchangeAPI("mock_hl", mock_config.config_data['exchanges']['mock_hl'], mock_secrets['mock_hl'], config_obj=mock_config)

@pytest.fixture
def mock_bp_api(mock_config, mock_secrets):
    """Mock API for Backpack, passes full config."""
    return MockExchangeAPI("mock_bp", mock_config.config_data['exchanges']['mock_bp'], mock_secrets['mock_bp'], config_obj=mock_config)

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
    execution_handler: ExecutionHandler
):
    """Tests the full cycle - data -> signal -> risk -> execution -> portfolio."""
    
    # 1. Setup
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset() 
    assert mock_config.get('strategy.funding_rate.min_profit_threshold') == 0.1
    
    symbol = "BTC"
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol}')
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol}')
    assert hl_symbol == "BTC-PERP"
    assert bp_symbol == "BTC-PERP"
    
    initial_usd_balance = Decimal("10000.0")
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=float(initial_usd_balance), free=float(initial_usd_balance)))
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=float(initial_usd_balance), free=float(initial_usd_balance)))
    await portfolio_tracker.initialize()

    # Set mock data
    now_aware = datetime.now(timezone.utc)
    now_naive = datetime.now()
    ts_int = int(now_aware.timestamp() * 1000)
    mock_hl_ticker_obj = create_mock_ticker(hl_symbol, 40099.0, 40101.0, 40100.0, ts_int)
    mock_bp_ticker_obj = create_mock_ticker(bp_symbol, 40079.0, 40081.0, 40080.0, ts_int)
    mock_hl_api.set_mock_ticker(mock_hl_ticker_obj)
    mock_bp_api.set_mock_ticker(mock_bp_ticker_obj)
    next_funding_ts = int((now_aware + timedelta(hours=1)).timestamp()*1000)
    mock_hl_api.set_mock_funding_rate(create_mock_funding_rate(hl_symbol, 0.002, next_funding_ts))
    mock_bp_api.set_mock_funding_rate(create_mock_funding_rate(bp_symbol, -0.002, next_funding_ts))

    # Manually update DataHandler
    if "mock_hl" not in data_handler.funding_rates: data_handler.funding_rates["mock_hl"] = {}
    if "mock_bp" not in data_handler.funding_rates: data_handler.funding_rates["mock_bp"] = {}
    data_handler.funding_rates["mock_hl"][hl_symbol] = (Decimal("0.002"), now_naive)
    data_handler.funding_rates["mock_bp"][bp_symbol] = (Decimal("-0.002"), now_naive)
    if "mock_hl" not in data_handler.last_update_time: data_handler.last_update_time["mock_hl"] = {}
    if "funding_rate" not in data_handler.last_update_time["mock_hl"]: data_handler.last_update_time["mock_hl"]["funding_rate"] = {}
    data_handler.last_update_time["mock_hl"]["funding_rate"][hl_symbol] = now_naive
    if "mock_bp" not in data_handler.last_update_time: data_handler.last_update_time["mock_bp"] = {}
    if "funding_rate" not in data_handler.last_update_time["mock_bp"]: data_handler.last_update_time["mock_bp"]["funding_rate"] = {}
    data_handler.last_update_time["mock_bp"]["funding_rate"][bp_symbol] = now_naive
    if "mock_hl" not in data_handler.tickers: data_handler.tickers["mock_hl"] = {}
    if "mock_bp" not in data_handler.tickers: data_handler.tickers["mock_bp"] = {}
    data_handler.tickers["mock_hl"][hl_symbol] = mock_hl_ticker_obj
    data_handler.tickers["mock_bp"][bp_symbol] = mock_bp_ticker_obj
    if "ticker" not in data_handler.last_update_time["mock_hl"]: data_handler.last_update_time["mock_hl"]["ticker"] = {}
    data_handler.last_update_time["mock_hl"]["ticker"][hl_symbol] = now_naive
    if "ticker" not in data_handler.last_update_time["mock_bp"]: data_handler.last_update_time["mock_bp"]["ticker"] = {}
    data_handler.last_update_time["mock_bp"]["ticker"][bp_symbol] = now_naive

    # Lower profit threshold for this test to ensure signal generation
    original_profit_threshold = signal_generator.min_profit_threshold
    signal_generator.min_profit_threshold = Decimal("0.0")
    
    opportunities = signal_generator.generate_opportunities()
    
    # Restore original threshold
    signal_generator.min_profit_threshold = original_profit_threshold
    
    # 4. Validate Signal
    assert len(opportunities) >= 1, "Should generate at least one opportunity"
    opportunity = opportunities[0]
    assert isinstance(opportunity, ArbitrageOpportunity)
    assert opportunity.symbol == symbol
    assert opportunity.long_exchange == "mock_bp" 
    assert opportunity.short_exchange == "mock_hl"
    assert opportunity.long_funding_rate == Decimal("-0.002")
    assert opportunity.short_funding_rate == Decimal("0.002")
    assert opportunity.net_funding_differential == pytest.approx(Decimal("0.004")) 
    assert opportunity.expected_profit == pytest.approx(Decimal("1.0"))

    # 5. Validate & Size
    sized_opportunities = risk_manager.validate_opportunities([opportunity])
    assert len(sized_opportunities) == 1, "Opportunity should be valid and sized"
    sized_opportunity = sized_opportunities[0]
    assert isinstance(sized_opportunity, SizedOpportunity)
    assert sized_opportunity.long_size > 0
    assert sized_opportunity.short_size > 0
    assert sized_opportunity.long_size <= mock_config.get("risk_manager.max_position_size")
    assert sized_opportunity.short_size <= mock_config.get("risk_manager.max_position_size")

    # 6. Execute
    execution = await execution_handler.execute_opportunity(sized_opportunity)

    # 7. Verify Execution
    assert execution.status == ExecutionStatus.COMPLETED, f"Execution failed: {execution.error_message}"
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
    assert long_order.filled_quantity == pytest.approx(Decimal(sized_opportunity.long_size) / long_order.price, rel=Decimal("1e-5")) if long_order.price else False
    assert short_order.filled_quantity == pytest.approx(Decimal(sized_opportunity.short_size) / short_order.price, rel=Decimal("1e-5")) if short_order.price else False

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
    caplog
):
    """Tests the workflow when one exchange API fails during order placement."""
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol_key}', "BTC-PERP")
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol_key}', "BTC-PERP")
    start_time = datetime.now(timezone.utc)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()
    mock_hl_api.reset_failure()
    mock_bp_api.reset_failure()
    mock_hl_api.set_mock_ticker(create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, ts_int))
    mock_hl_api.set_mock_funding_rate(create_mock_funding_rate(hl_symbol, 0.002, next_funding_ts))
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=10000.0, free=10000.0))
    mock_hl_api.set_open_orders_behavior("fill_immediately")
    mock_bp_api.set_mock_ticker(create_mock_ticker(bp_symbol, 10004.0, 10006.0, 10005.0, ts_int))
    mock_bp_api.set_mock_funding_rate(create_mock_funding_rate(bp_symbol, -0.002, next_funding_ts))
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=10000.0, free=10000.0))
    mock_bp_api.configure_failure("place_order", MockAPIError("BP connection failed")) 
    data_handler.tickers["mock_hl"] = {hl_symbol: mock_hl_api._mock_tickers[hl_symbol]}
    data_handler.tickers["mock_bp"] = {bp_symbol: mock_bp_api._mock_tickers[bp_symbol]}
    data_handler.funding_rates["mock_hl"] = {hl_symbol: (Decimal("0.002"), datetime.now())}
    data_handler.funding_rates["mock_bp"] = {bp_symbol: (Decimal("-0.002"), datetime.now())}
    data_handler.last_update_time["mock_hl"] = {"ticker": {hl_symbol: datetime.now()}, "funding_rate": {hl_symbol: datetime.now()}}
    data_handler.last_update_time["mock_bp"] = {"ticker": {bp_symbol: datetime.now()}, "funding_rate": {bp_symbol: datetime.now()}}
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1
    opportunity = opportunities[0]
    assert opportunity.long_exchange == "mock_bp" 
    assert opportunity.short_exchange == "mock_hl"
    await portfolio_tracker.initialize()
    initial_balances = copy.deepcopy(portfolio_tracker._balances) 
    initial_positions = copy.deepcopy(portfolio_tracker._positions)
    sized_opportunities = risk_manager.validate_opportunities(opportunities)
    assert len(sized_opportunities) >= 1
    sized_opportunity = sized_opportunities[0]
    caplog.set_level(logging.ERROR) 
    with patch.object(mock_bp_api, 'place_order', side_effect=MockAPIError("BP connection failed")) as mock_place_long:
        trade_execution_result = await execution_handler.execute_opportunity(sized_opportunity)
        mock_place_long.assert_called() 
    assert trade_execution_result.status == ExecutionStatus.FAILED, "Execution should have failed"
    assert trade_execution_result.error_message is not None
    assert "Failed to place long order" in trade_execution_result.error_message 
    assert any("Unexpected error placing order on mock_bp" in record.message for record in caplog.records)
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
    caplog
):
    """Tests that an opportunity is rejected if there's insufficient balance."""
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol_key}', "BTC-PERP")
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol_key}', "BTC-PERP")
    start_time = datetime.now(timezone.utc)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()
    mock_hl_api.set_mock_ticker(create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, ts_int))
    mock_hl_api.set_mock_funding_rate(create_mock_funding_rate(hl_symbol, 0.002, next_funding_ts))
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=10000.0, free=10000.0))
    mock_bp_api.set_mock_ticker(create_mock_ticker(bp_symbol, 10004.0, 10006.0, 10005.0, ts_int))
    mock_bp_api.set_mock_funding_rate(create_mock_funding_rate(bp_symbol, -0.002, next_funding_ts))
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=1.0, free=1.0)) 
    data_handler.tickers["mock_hl"] = {hl_symbol: mock_hl_api._mock_tickers[hl_symbol]}
    data_handler.tickers["mock_bp"] = {bp_symbol: mock_bp_api._mock_tickers[bp_symbol]}
    data_handler.funding_rates["mock_hl"] = {hl_symbol: (Decimal("0.002"), datetime.now())}
    data_handler.funding_rates["mock_bp"] = {bp_symbol: (Decimal("-0.002"), datetime.now())}
    data_handler.last_update_time["mock_hl"] = {"ticker": {hl_symbol: datetime.now()}, "funding_rate": {hl_symbol: datetime.now()}}
    data_handler.last_update_time["mock_bp"] = {"ticker": {bp_symbol: datetime.now()}, "funding_rate": {bp_symbol: datetime.now()}}
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1
    opportunity = opportunities[0]
    assert opportunity.long_exchange == "mock_bp" 
    assert opportunity.short_exchange == "mock_hl"
    await portfolio_tracker.initialize() 
    caplog.set_level(logging.WARNING)
    sized_opportunities = risk_manager.validate_opportunities(opportunities)
    assert len(sized_opportunities) == 0, "Opportunity should be rejected due to insufficient balance"
    assert any("Cannot size opportunity: total capital is zero or negative" in record.message for record in caplog.records)

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
    caplog
):
    """Tests the workflow when one leg partially fills, triggering immediate compensation."""
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol_key}', "BTC-PERP")
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol_key}', "BTC-PERP")
    start_time = datetime.now(timezone.utc)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()
    mock_hl_api.set_mock_ticker(create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, ts_int))
    mock_hl_api.set_mock_funding_rate(create_mock_funding_rate(hl_symbol, 0.002, next_funding_ts))
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=10000.0, free=10000.0))
    mock_hl_api.set_open_orders_behavior("fill_immediately")
    mock_bp_api.set_mock_ticker(create_mock_ticker(bp_symbol, 10004.0, 10006.0, 10005.0, ts_int))
    mock_bp_api.set_mock_funding_rate(create_mock_funding_rate(bp_symbol, -0.002, next_funding_ts))
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=10000.0, free=10000.0))
    mock_bp_api.set_open_orders_behavior("partial_fill") 
    data_handler.tickers["mock_hl"] = {hl_symbol: mock_hl_api._mock_tickers[hl_symbol]}
    data_handler.tickers["mock_bp"] = {bp_symbol: mock_bp_api._mock_tickers[bp_symbol]}
    data_handler.funding_rates["mock_hl"] = {hl_symbol: (Decimal("0.002"), datetime.now())}
    data_handler.funding_rates["mock_bp"] = {bp_symbol: (Decimal("-0.002"), datetime.now())}
    data_handler.last_update_time["mock_hl"] = {"ticker": {hl_symbol: datetime.now()}, "funding_rate": {hl_symbol: datetime.now()}}
    data_handler.last_update_time["mock_bp"] = {"ticker": {bp_symbol: datetime.now()}, "funding_rate": {bp_symbol: datetime.now()}}
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1
    opportunity = opportunities[0]
    assert opportunity.long_exchange == "mock_bp"
    assert opportunity.short_exchange == "mock_hl"
    await portfolio_tracker.initialize()
    sized_opportunities = risk_manager.validate_opportunities(opportunities)
    assert len(sized_opportunities) >= 1
    sized_opportunity = sized_opportunities[0]
    initial_bp_balances_direct = await mock_bp_api.get_balances()
    initial_hl_balances_direct = await mock_hl_api.get_balances()
    mock_bp_logger = logging.getLogger("tests.integration.mocks.mock_exchange")
    original_level = mock_bp_logger.level
    mock_bp_logger.setLevel(logging.DEBUG)
    caplog.set_level(logging.DEBUG) 
    trade_execution_result = await execution_handler.execute_opportunity(sized_opportunity)
    mock_bp_logger.setLevel(original_level)
    assert trade_execution_result.status == ExecutionStatus.FAILED, \
        f"Expected FAILED status after partial fill compensation, got {trade_execution_result.status.name}"
    assert trade_execution_result.error_message is not None
    assert "partially completed and compensation attempted" in trade_execution_result.error_message
    assert trade_execution_result.long_order_id is not None
    assert trade_execution_result.short_order_id is not None
    original_long_order = await mock_bp_api.get_order(trade_execution_result.long_order_id)
    original_short_order = await mock_hl_api.get_order(trade_execution_result.short_order_id)
    assert original_long_order is not None
    assert original_short_order is not None
    assert original_long_order.status == OrderStatus.PARTIALLY_FILLED 
    assert original_short_order.status == OrderStatus.FILLED 
    assert any("Attempting partial fill compensation for long leg" in record.message for record in caplog.records)
    assert any("Attempting partial fill compensation for short leg" in record.message for record in caplog.records)
    assert any(f"Compensating position on {opportunity.long_exchange}" in record.message for record in caplog.records)
    assert any(f"Compensating position on {opportunity.short_exchange}" in record.message for record in caplog.records)
    all_bp_orders = mock_bp_api.get_orders()
    all_hl_orders = mock_hl_api.get_orders()
    assert len(all_bp_orders) == 2
    assert len(all_hl_orders) == 2
    compensating_bp_order = None
    for order_id, order in all_bp_orders.items():
        if order_id != original_long_order.id:
            compensating_bp_order = order
            break
    assert compensating_bp_order is not None and compensating_bp_order.side == OrderSide.SELL
    compensating_hl_order = None
    for order_id, order in all_hl_orders.items():
        if order_id != original_short_order.id:
            compensating_hl_order = order
            break
    assert compensating_hl_order is not None and compensating_hl_order.side == OrderSide.BUY
    await asyncio.sleep(0.1)
    await portfolio_tracker.update()
    bp_final_pos = portfolio_tracker.get_position("mock_bp", bp_symbol)
    hl_final_pos = portfolio_tracker.get_position("mock_hl", hl_symbol)
    assert bp_final_pos is None or bp_final_pos.size == pytest.approx(Decimal("0.0"), abs=Decimal("1e-9"))
    assert hl_final_pos is None or hl_final_pos.size == pytest.approx(Decimal("0.0"), abs=Decimal("1e-9"))
    mock_bp_final_balances = await mock_bp_api.get_balances()
    mock_hl_final_balances = await mock_hl_api.get_balances()
    assert mock_bp_final_balances["USDC"].total < initial_bp_balances_direct["USDC"].total
    assert mock_hl_final_balances["USD"].total < initial_hl_balances_direct["USD"].total
    assert mock_bp_final_balances.get("BTC", Balance(asset="BTC", total=Decimal("0"))).total == pytest.approx(Decimal("0.0"), abs=Decimal("1e-9"))
    assert mock_hl_final_balances.get("BTC", Balance(asset="BTC", total=Decimal("0"))).total == pytest.approx(Decimal("0.0"), abs=Decimal("1e-9"))

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
    caplog
):
    """Tests the LIMIT order compensation logic when the second leg (short) fails."""
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol_key}', "BTC-PERP") 
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol_key}', "BTC-PERP") 
    start_time = datetime.now(timezone.utc)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)
    mock_hl_api.reset() 
    mock_bp_api.reset()
    portfolio_tracker.reset()
    bp_ticker_price = 10005.0
    bp_bid = 10004.0
    bp_ask = 10006.0
    mock_bp_api.set_mock_ticker(create_mock_ticker(bp_symbol, bp_bid, bp_ask, bp_ticker_price, ts_int)) 
    mock_bp_api.set_mock_funding_rate(create_mock_funding_rate(bp_symbol, -0.002, next_funding_ts)) 
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=10000.0, free=10000.0)) 
    mock_bp_api.set_open_orders_behavior("fill_immediately") 
    mock_hl_api.set_mock_ticker(create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, ts_int)) 
    mock_hl_api.set_mock_funding_rate(create_mock_funding_rate(hl_symbol, 0.002, next_funding_ts)) 
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=10000.0, free=10000.0)) 
    mock_hl_api.configure_error("place_order", APIErrorCode.INSUFFICIENT_FUNDS, "Simulated insufficient funds") 
    data_handler.tickers["mock_hl"] = {hl_symbol: mock_hl_api._mock_tickers[hl_symbol]}
    data_handler.tickers["mock_bp"] = {bp_symbol: mock_bp_api._mock_tickers[bp_symbol]}
    data_handler.funding_rates["mock_hl"] = {hl_symbol: (Decimal("0.002"), datetime.now())}
    data_handler.funding_rates["mock_bp"] = {bp_symbol: (Decimal("-0.002"), datetime.now())}
    data_handler.last_update_time["mock_hl"] = {"ticker": {hl_symbol: datetime.now()}, "funding_rate": {hl_symbol: datetime.now()}}
    data_handler.last_update_time["mock_bp"] = {"ticker": {bp_symbol: datetime.now()}, "funding_rate": {bp_symbol: datetime.now()}}
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1
    opportunity = opportunities[0]
    await portfolio_tracker.initialize()
    initial_bp_balances = await mock_bp_api.get_balances()
    initial_hl_balances = await mock_hl_api.get_balances()
    sized_opportunities = risk_manager.validate_opportunities(opportunities)
    assert len(sized_opportunities) >= 1
    sized_opportunity = sized_opportunities[0]
    caplog.set_level(logging.INFO) 
    trade_execution_result = None 
    trade_execution_result = await execution_handler.execute_opportunity(sized_opportunity)
    logger.info(f"Execution result (short fail, comp success): {trade_execution_result}")
    assert trade_execution_result is not None 
    assert trade_execution_result.status == ExecutionStatus.FAILED, \
        f"Expected FAILED status after initial failure and compensation, got {trade_execution_result.status.name}"
    assert trade_execution_result.error_message is not None
    assert "Failed to place short order" in trade_execution_result.error_message and \
           "compensating long" in trade_execution_result.error_message, \
           f"Unexpected error message: {trade_execution_result.error_message}"
    assert trade_execution_result.long_order_id is not None 
    assert trade_execution_result.short_order_id is None 
    original_long_order = await mock_bp_api.get_order(trade_execution_result.long_order_id)
    assert original_long_order is not None
    assert original_long_order.status == OrderStatus.FILLED
    assert any("Compensating position on mock_bp" in record.message for record in caplog.records if record.levelno >= logging.INFO)
    all_bp_orders = mock_bp_api.get_orders() 
    assert len(all_bp_orders) == 2, "Expected 2 orders on BP (original + compensation)" 
    compensating_order_id = None
    for order_id in all_bp_orders:
        if order_id != original_long_order.id:
            compensating_order_id = order_id
            break
    assert compensating_order_id is not None, "Could not find compensation order ID"
    compensating_order = await mock_bp_api.get_order(compensating_order_id)
    assert compensating_order is not None
    assert compensating_order.side == OrderSide.SELL 
    assert compensating_order.status == OrderStatus.FILLED, "Compensation order should have filled" 
    await asyncio.sleep(0.1)
    await portfolio_tracker.update()
    bp_final_pos = portfolio_tracker.get_position("mock_bp", bp_symbol)
    hl_final_pos = portfolio_tracker.get_position("mock_hl", hl_symbol)
    assert bp_final_pos is None or bp_final_pos.size == 0, f"BP position should be zero or None after compensation, but is {bp_final_pos}"
    assert hl_final_pos is None, "HL position should not exist as short order failed"
    mock_bp_final_balances = await mock_bp_api.get_balances()
    mock_hl_final_balances = await mock_hl_api.get_balances()
    assert mock_hl_final_balances == initial_hl_balances, "HL balances should not change"
    assert mock_bp_final_balances["USDC"].total < initial_bp_balances["USDC"].total, \
        f"BP USDC balance should be lower after fees from original and compensating trades. Initial: {initial_bp_balances['USDC'].total}, Final: {mock_bp_final_balances['USDC'].total}"
    assert mock_bp_final_balances.get("BTC", Balance(asset="BTC", total=Decimal("0"))).total == pytest.approx(Decimal("0.0"), abs=Decimal("1e-9")), \
        f"BP BTC balance should be near zero after compensation, but is {mock_bp_final_balances.get('BTC', Balance(asset='BTC', total=Decimal("0"))).total}"
    mock_hl_api.clear_error()
    # --- Cleared errors for BP too -----
    mock_bp_api.clear_error()
    mock_bp_api.set_open_orders_behavior("fill_immediately")
    mock_hl_api.set_open_orders_behavior("fill_immediately")
    # ------------------------------------

# +++ NEW TEST CASE: Failure during compensation +++
@pytest.mark.asyncio
async def test_failure_during_compensation(
    mock_config: Config,
    mock_bp_api: MockExchangeAPI,
    mock_hl_api: MockExchangeAPI,
    portfolio_tracker: PortfolioTracker,
    execution_handler: ExecutionHandler,
    data_handler: DataHandler, 
    caplog
):
    """
    Tests the scenario where the initial order on one exchange fails,
    and the subsequent compensation order on the other exchange also fails.
    Expected: ExecutionStatus.FAILED, portfolio left with the initial filled position.
    """
    caplog.set_level(logging.INFO)
    symbol_key = "BTC"
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol_key}', "BTC-PERP")
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol_key}', "BTC-PERP")
    mock_bp_api.reset()
    mock_hl_api.reset()
    portfolio_tracker.reset()
    mock_bp_api.clear_error()
    mock_hl_api.clear_error()
    mock_bp_api.set_open_orders_behavior("fill_immediately")
    mock_hl_api.configure_error("place_order", APIErrorCode.INSUFFICIENT_FUNDS, "Insufficient funds")
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=10000.0, free=10000.0))
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=10000.0, free=10000.0))
    await portfolio_tracker.initialize()
    initial_bp_balances = await mock_bp_api.get_balances()
    initial_hl_balances = await mock_hl_api.get_balances()
    ts_int = int(datetime.now(timezone.utc).timestamp()*1000)
    mock_bp_ticker = create_mock_ticker(bp_symbol, 29995, 30000, 30000, ts_int)
    mock_hl_ticker = create_mock_ticker(hl_symbol, 30050, 30055, 30050, ts_int)
    
    # --- ADDED: Set tickers on mock APIs --- 
    mock_bp_api.set_mock_ticker(mock_bp_ticker)
    mock_hl_api.set_mock_ticker(mock_hl_ticker)
    # -------------------------------------
    
    if "mock_bp" not in data_handler.tickers: data_handler.tickers["mock_bp"] = {}
    if "mock_hl" not in data_handler.tickers: data_handler.tickers["mock_hl"] = {}
    data_handler.tickers["mock_bp"][bp_symbol] = mock_bp_ticker
    data_handler.tickers["mock_hl"][hl_symbol] = mock_hl_ticker
    now_naive = datetime.now()
    if "mock_bp" not in data_handler.last_update_time: data_handler.last_update_time["mock_bp"] = {}
    if "ticker" not in data_handler.last_update_time["mock_bp"]: data_handler.last_update_time["mock_bp"]["ticker"] = {}
    data_handler.last_update_time["mock_bp"]["ticker"][bp_symbol] = now_naive
    if "mock_hl" not in data_handler.last_update_time: data_handler.last_update_time["mock_hl"] = {}
    if "ticker" not in data_handler.last_update_time["mock_hl"]: data_handler.last_update_time["mock_hl"]["ticker"] = {}
    data_handler.last_update_time["mock_hl"]["ticker"][hl_symbol] = now_naive
    base_opportunity = ArbitrageOpportunity(
        symbol=symbol_key,
        long_exchange="mock_bp",
        short_exchange="mock_hl",
        long_funding_rate=Decimal("-0.002"), 
        short_funding_rate=Decimal("0.002"), 
        net_funding_differential=Decimal("0.004"),
        timestamp=datetime.now(timezone.utc),
        expected_profit=Decimal("1.0"), 
        utility_score=1.0, 
        basis_volatility=0.001 
    )
    sized_opportunity = SizedOpportunity(
        opportunity=base_opportunity,
        long_size=Decimal("1500.0"),
        short_size=Decimal("1500.0"),
        allocation_percentage=15.0,
        expected_profit=Decimal("1.0"),
        expected_return=0.004,
        risk_adjusted_return=0.003
    )
    sized_opportunity.quantity = Decimal("0.05")
    sized_opportunity.long_entry_price = Decimal("30000")
    sized_opportunity.short_entry_price = Decimal("30050")
    original_place_order_bp = mock_bp_api.place_order
    call_count = 0
    async def failing_compensation_place_order(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        logger.info(f"Mock BP place_order call #{call_count} intercepted. Args: {args}, Kwargs: {kwargs}")
        if call_count == 1:
            logger.info("Executing original place_order for call #1 (initial BUY)")
            mock_bp_api.clear_error()
            mock_bp_api.set_open_orders_behavior("fill_immediately")
            result = await original_place_order_bp(*args, **kwargs)
            logger.info(f"Original place_order call #1 returned: {result}")
            return result
        else:
            logger.info("Configuring error for place_order call #2 (compensation SELL)")
            mock_bp_api.configure_error("place_order", APIErrorCode.CONNECTION_ERROR, "Simulated connection error during compensation")
            return await original_place_order_bp(*args, **kwargs)
    mock_bp_api.place_order = failing_compensation_place_order
    execution = await execution_handler.execute_opportunity(sized_opportunity)
    logger.info(f"Execution result: {execution}")
    mock_bp_api.place_order = original_place_order_bp
    assert execution.status == ExecutionStatus.FAILED, f"Expected FAILED, got {execution.status}"
    assert execution.error_message is not None
    assert "Failed to place short order" in execution.error_message, f"Error message missing short leg failure: {execution.error_message}"
    assert "compensating long" in execution.error_message, f"Error message missing compensation indicator: {execution.error_message}"
    assert "Compensation attempt also failed" in execution.error_message, f"Error message missing compensation failure indicator: {execution.error_message}"
    
    # Check logs for specific error reasons
    log_text = caplog.text
    # Check for logs indicating the short leg failure reason
    assert "API Error placing order on mock_hl: Insufficient funds" in log_text, "Missing log for HL insufficient funds API error"
    assert "Non-retryable API error or max retries reached for order placement on mock_hl" in log_text, "Missing log for aborting HL order attempt"
    # Check compensation logs
    # assert "Attempting to compensate filled long leg (mock_bp)" in log_text, "Missing log for BP compensation attempt" # This log message doesn't exist
    assert "Compensating position on mock_bp for BTC-PERP" in log_text, "Missing log for BP compensation placement initiation" # Correct log message
    assert "Error placing compensation order on mock_bp" in log_text, "Missing log for BP compensation failure reason" # Correct log message
    assert "Simulated connection error during compensation" in log_text or "APIErrorCode.CONNECTION_ERROR" in log_text, "Missing log for BP compensation connection error"
    await asyncio.sleep(0.1)
    await portfolio_tracker.update()
    final_bp_balances = await mock_bp_api.get_balances()
    final_hl_balances = await mock_hl_api.get_balances()
    logger.info(f"Final BP Balances: {final_bp_balances}")
    logger.info(f"Final HL Balances: {final_hl_balances}")
    initial_bp_btc = initial_bp_balances.get("BTC", Balance(asset="BTC", total=Decimal("0.0"), free=Decimal("0.0"))).total
    initial_bp_usdc = initial_bp_balances.get("USDC", Balance(asset="USDC", total=Decimal("10000.0"), free=Decimal("10000.0"))).total
    original_long_order_id = execution.long_order_id
    assert original_long_order_id is not None, "Long order ID was not recorded"
    original_long_order = await mock_bp_api.get_order(original_long_order_id)
    assert original_long_order is not None, "Could not retrieve original long order"
    assert original_long_order.status == OrderStatus.FILLED, "Original long order not filled"
    filled_long_qty = original_long_order.filled_quantity
    filled_long_price = original_long_order.price
    expected_bp_btc = initial_bp_btc + filled_long_qty
    expected_bp_usdc_change = -(filled_long_qty * filled_long_price)
    # Use taker_fee as market orders were used
    fee_bp = abs(expected_bp_usdc_change * mock_bp_api.taker_fee) 
    expected_bp_usdc = initial_bp_usdc + expected_bp_usdc_change - fee_bp
    final_bp_btc = final_bp_balances.get("BTC", Balance(asset="BTC", total=Decimal("0.0"), free=Decimal("0.0"))).total
    final_bp_usdc = final_bp_balances.get("USDC", Balance(asset="USDC", total=Decimal("0.0"), free=Decimal("0.0"))).total
    assert final_bp_btc == pytest.approx(expected_bp_btc), f"Final BP BTC mismatch"
    assert final_bp_usdc == pytest.approx(expected_bp_usdc), f"Final BP USDC mismatch"
    initial_hl_btc = initial_hl_balances.get("BTC", Balance(asset="BTC", total=Decimal("0.0"), free=Decimal("0.0"))).total
    initial_hl_usd = initial_hl_balances.get("USD", Balance(asset="USD", total=Decimal("10000.0"), free=Decimal("10000.0"))).total
    final_hl_btc = final_hl_balances.get("BTC", Balance(asset="BTC", total=Decimal("0.0"), free=Decimal("0.0"))).total
    final_hl_usd = final_hl_balances.get("USD", Balance(asset="USD", total=Decimal("0.0"), free=Decimal("0.0"))).total
    assert final_hl_btc == pytest.approx(initial_hl_btc), f"Final HL BTC mismatch"
    assert final_hl_usd == pytest.approx(initial_hl_usd), f"Final HL USD mismatch"
    mock_bp_api.clear_error()
    mock_hl_api.clear_error()
    mock_bp_api.set_open_orders_behavior("fill_immediately")
    mock_hl_api.set_open_orders_behavior("fill_immediately")

# --- Add More Failure Test Cases Below ---
pass # Ensure file doesn't end abruptly 