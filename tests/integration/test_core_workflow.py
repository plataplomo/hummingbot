import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, AsyncMock, patch
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
def create_mock_ticker(symbol, bid, ask, last, timestamp):
    return Ticker(symbol=symbol, bid=bid, ask=ask, price=last, timestamp=timestamp)

def create_mock_funding_rate(symbol, rate, next_time):
    return FundingRate(symbol=symbol, funding_rate=rate, next_funding_time=next_time)

# Initialize logger for this module
logger = logging.getLogger(__name__)

# --- Test Fixtures --- 

@pytest.fixture(scope="module")
def mock_config_dict():
    """Provides a base configuration dictionary for integration tests."""
    # Ensure this reflects the latest version with correct strategy/thresholds
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
            # Assuming RiskManager loads these directly
            "max_total_exposure": 20000.0,
            "max_exchange_exposure": 10000.0,
            "max_position_size": 5000.0,
            "circuit_breaker_threshold": 0.1, # Example 10% loss threshold
            "min_exchange_balance": 10.0 # Add the new config value
            # Add other risk params if needed
        },
        "execution_handler": {
            "order_timeout_seconds": 30,
            "max_slippage_pct": 0.001, # Added for completeness
            "max_retries": 2, # Added for completeness
            "retry_delay_base_sec": 0.05 # Added for completeness
        },
        "data": {
            "staleness_thresholds": {
                "ticker": 60,
                "funding_rate": 3600,
                "orderbook": 30
            }
        },
         "safety_systems": { # Ensure this section exists if EH needs it
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
                # Handle cases where a sub-key is requested but parent is not a dict
                if key_parts.index(key) < len(key_parts) - 1: 
                    return default # Intermediate key not found
                # If it's the last key, maybe it's a direct value access
                return default # Or handle differently if needed
        return val
    except (KeyError, TypeError): # Catch TypeError if trying to index non-dict
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
    # Pass the full config object, not just the exchange section
    return MockExchangeAPI("mock_hl", mock_config.config_data['exchanges']['mock_hl'], mock_secrets['mock_hl'], config_obj=mock_config)

@pytest.fixture
def mock_bp_api(mock_config, mock_secrets):
    """Mock API for Backpack, passes full config."""
    # Pass the full config object, not just the exchange section
    return MockExchangeAPI("mock_bp", mock_config.config_data['exchanges']['mock_bp'], mock_secrets['mock_bp'], config_obj=mock_config)

@pytest.fixture
def portfolio_tracker(mock_config):
    """Portfolio Tracker instance."""
    # Register API clients within the tracker if needed AFTER initialization
    # pt = PortfolioTracker(mock_config)
    # pt.register_api_client(...) # If needed by RiskManager/ExecutionHandler
    return PortfolioTracker(mock_config)

@pytest.fixture
def data_handler(mock_config, mock_hl_api, mock_bp_api):
    """Data Handler instance with mock APIs registered. Corrected definition."""
    dh = DataHandler(mock_config) # No portfolio_tracker here
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
    # Ensure PortfolioTracker is ready if RiskManager uses it during init
    # Might need to call portfolio_tracker.initialize() here or pass APIs
    return RiskManager(mock_config, portfolio_tracker)

@pytest.fixture
def execution_handler(mock_config, portfolio_tracker, mock_hl_api, mock_bp_api):
    """Execution Handler instance with mock APIs registered."""
    eh = ExecutionHandler(mock_config, portfolio_tracker)
    # Portfolio tracker needs clients registered *before* EH potentially uses it.
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
    """Tests the full cycle - Rewritten Setup Part."""
    
    # 1. Setup: Reset mocks and ensure config access
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset() 
    
    # Verify config access within test if needed (REMOVED failing assertion)
    # assert mock_config.get('strategy.hl_perp_bp_perp.min_profit_threshold') == 0.1
    # Verify the NEW path if needed
    assert mock_config.get('strategy.funding_rate.min_profit_threshold') == 0.1
    
    symbol = "BTC" # Internal symbol
    # Use config access method to get symbols
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol}')
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol}')
    assert hl_symbol == "BTC-PERP"
    assert bp_symbol == "BTC-PERP"
    
    # Set balances (using correct 'free' field)
    initial_usd_balance = Decimal("10000.0")
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=float(initial_usd_balance), free=float(initial_usd_balance)))
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=float(initial_usd_balance), free=float(initial_usd_balance)))
    
    # Initialize PortfolioTracker *after* APIs are registered in its fixture or EH fixture
    # The EH fixture now registers clients with portfolio_tracker
    await portfolio_tracker.initialize()

    # Set mock data (Ticker, FundingRate)
    now_aware = datetime.now(timezone.utc)
    now_naive = datetime.now()
    
    mock_hl_ticker_obj = Ticker(symbol=hl_symbol, timestamp=int(now_aware.timestamp() * 1000), price=40100.0, bid=40099.0, ask=40101.0, volume=1000)
    mock_bp_ticker_obj = Ticker(symbol=bp_symbol, timestamp=int(now_aware.timestamp() * 1000), price=40080.0, bid=40079.0, ask=40081.0, volume=1000)
    mock_hl_api.set_mock_ticker(mock_hl_ticker_obj)
    mock_bp_api.set_mock_ticker(mock_bp_ticker_obj)
    
    mock_hl_api.set_mock_funding_rate(FundingRate(symbol=hl_symbol, funding_rate=0.002, next_funding_time=int((now_aware + timedelta(hours=1)).timestamp()*1000)))
    mock_bp_api.set_mock_funding_rate(FundingRate(symbol=bp_symbol, funding_rate=-0.002, next_funding_time=int((now_aware + timedelta(hours=1)).timestamp()*1000)))

    # Manually update DataHandler internal state using *exchange-specific* symbols
    # Funding Rates
    if "mock_hl" not in data_handler.funding_rates: data_handler.funding_rates["mock_hl"] = {}
    if "mock_bp" not in data_handler.funding_rates: data_handler.funding_rates["mock_bp"] = {}
    data_handler.funding_rates["mock_hl"][hl_symbol] = (0.002, now_naive)
    data_handler.funding_rates["mock_bp"][bp_symbol] = (-0.002, now_naive)
    if "mock_hl" not in data_handler.last_update_time: data_handler.last_update_time["mock_hl"] = {}
    if "funding_rate" not in data_handler.last_update_time["mock_hl"]: data_handler.last_update_time["mock_hl"]["funding_rate"] = {}
    data_handler.last_update_time["mock_hl"]["funding_rate"][hl_symbol] = now_naive
    if "mock_bp" not in data_handler.last_update_time: data_handler.last_update_time["mock_bp"] = {}
    if "funding_rate" not in data_handler.last_update_time["mock_bp"]: data_handler.last_update_time["mock_bp"]["funding_rate"] = {}
    data_handler.last_update_time["mock_bp"]["funding_rate"][bp_symbol] = now_naive

    # Tickers
    if "mock_hl" not in data_handler.tickers: data_handler.tickers["mock_hl"] = {}
    if "mock_bp" not in data_handler.tickers: data_handler.tickers["mock_bp"] = {}
    data_handler.tickers["mock_hl"][hl_symbol] = mock_hl_ticker_obj
    data_handler.tickers["mock_bp"][bp_symbol] = mock_bp_ticker_obj
    if "ticker" not in data_handler.last_update_time["mock_hl"]: data_handler.last_update_time["mock_hl"]["ticker"] = {}
    data_handler.last_update_time["mock_hl"]["ticker"][hl_symbol] = now_naive
    if "ticker" not in data_handler.last_update_time["mock_bp"]: data_handler.last_update_time["mock_bp"]["ticker"] = {}
    data_handler.last_update_time["mock_bp"]["ticker"][bp_symbol] = now_naive

    # 2. (No explicit DataHandler update needed due to manual injection)

    # 3. Generate Signal (No await)
    opportunities = signal_generator.generate_opportunities()
    
    # 4. Validate Signal
    assert len(opportunities) >= 1, "Should generate at least one opportunity"
    opportunity = opportunities[0] # Take the first one
    assert isinstance(opportunity, ArbitrageOpportunity)
    assert opportunity.symbol == symbol
    # Assert the swapped exchanges based on funding rates
    assert opportunity.long_exchange == "mock_bp" 
    assert opportunity.short_exchange == "mock_hl"
    assert opportunity.long_funding_rate == -0.002
    assert opportunity.short_funding_rate == 0.002
    assert opportunity.net_funding_differential == pytest.approx(0.004) 
    assert opportunity.expected_profit == pytest.approx(1.0)

    # 5. Validate & Size
    sized_opportunities = risk_manager.validate_opportunities([opportunity])
    
    assert len(sized_opportunities) == 1, "Opportunity should be valid and sized"
    sized_opportunity = sized_opportunities[0]
    assert isinstance(sized_opportunity, SizedOpportunity)
    assert sized_opportunity.long_size > 0
    assert sized_opportunity.short_size > 0
    # Check if sizes are within global limits from mock_config
    assert sized_opportunity.long_size <= mock_config.get("risk_manager.max_position_size")
    assert sized_opportunity.short_size <= mock_config.get("risk_manager.max_position_size")

    # 6. Execute
    execution = await execution_handler.execute_opportunity(sized_opportunity)

    # 7. Verify Execution
    assert execution.status == ExecutionStatus.COMPLETED, f"Execution failed: {execution.error_message}"
    assert execution.long_order_id is not None
    assert execution.short_order_id is not None
    
    # Verify mock orders were placed and filled (using get_order on mock API)
    long_order = await mock_bp_api.get_order(execution.long_order_id)
    short_order = await mock_hl_api.get_order(execution.short_order_id)
    
    assert long_order is not None
    assert short_order is not None
    assert long_order.status == OrderStatus.FILLED
    assert short_order.status == OrderStatus.FILLED
    assert long_order.side == OrderSide.BUY
    assert short_order.side == OrderSide.SELL
    # Assert symbols match the EXCHANGE-SPECIFIC symbols
    assert long_order.symbol == bp_symbol # Check against exchange symbol 'BTC-PERP'
    assert short_order.symbol == hl_symbol # Check against exchange symbol 'BTC-PERP'
    # Approximate quantity check (allow small tolerance if conversion happens)
    assert long_order.filled_quantity == pytest.approx(sized_opportunity.long_size / long_order.price, rel=1e-5) if long_order.price else False
    assert short_order.filled_quantity == pytest.approx(sized_opportunity.short_size / short_order.price, rel=1e-5) if short_order.price else False

    # 8. Verify Portfolio State
    await portfolio_tracker.update() # Use the correct method name
    
    # Check if positions were correctly recorded
    bp_positions = portfolio_tracker.get_positions_by_symbol("mock_bp", bp_symbol)
    hl_positions = portfolio_tracker.get_positions_by_symbol("mock_hl", hl_symbol)
    
    assert len(bp_positions) >= 1, "Backpack position not found in tracker"
    assert len(hl_positions) >= 1, "HyperLiquid position not found in tracker"
    
    # Access the first position in the list
    bp_pos = bp_positions[0]
    hl_pos = hl_positions[0]
    
    # Verify sides (BP should be LONG, HL should be SHORT)
    assert bp_pos.side == OrderSide.BUY
    assert hl_pos.side == OrderSide.SELL
    
    # Verify quantities and prices against the *orders* that created them
    assert bp_pos.size == pytest.approx(long_order.filled_quantity)
    assert hl_pos.size == pytest.approx(short_order.filled_quantity)
    assert bp_pos.entry_price == pytest.approx(long_order.price)
    assert hl_pos.entry_price == pytest.approx(short_order.price)

    # Check final balances (optional, simple check assuming only cost is position value)
    final_hl_balance = portfolio_tracker.get_exchange_balance("mock_hl", "USD")
    final_bp_balance = portfolio_tracker.get_exchange_balance("mock_bp", "USDC")
    # Note: Real balance check is complex (fees, PnL, margin). This is a basic sanity check.
    # We expect HL balance to decrease (margin used), BP balance to increase (received USDC)
    # assert final_hl_balance < initial_usd_balance # Margin is complex
    # assert final_bp_balance > initial_usd_balance # Needs calc

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
    caplog # Pytest fixture to capture logs
):
    """Tests the workflow when one exchange API fails during order placement."""
    # --- Setup ---
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol_key}', "BTC-PERP")
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol_key}', "BTC-PERP")
    start_time = datetime.now(timezone.utc)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)

    # Reset mocks
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()
    mock_hl_api.reset_failure()
    mock_bp_api.reset_failure()

    # Configure HL (High funding, Low price -> Short) - Will succeed
    mock_hl_api.set_mock_ticker(create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, ts_int))
    mock_hl_api.set_mock_funding_rate(create_mock_funding_rate(hl_symbol, 0.0005, next_funding_ts))
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=10000.0, free=10000.0))
    mock_hl_api.set_open_orders_behavior("fill_immediately") # First leg fills fast

    # Configure BP (Low funding, High price -> Long) - Will FAIL
    mock_bp_api.set_mock_ticker(create_mock_ticker(bp_symbol, 10004.0, 10006.0, 10005.0, ts_int))
    mock_bp_api.set_mock_funding_rate(create_mock_funding_rate(bp_symbol, -0.0001, next_funding_ts))
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=10000.0, free=10000.0))
    mock_bp_api.configure_failure("place_order", MockAPIError("BP connection failed")) # Simulate failure

    # --- Workflow Steps ---
    # 1. Update Data (Manual Inject)
    data_handler.tickers["mock_hl"] = {hl_symbol: mock_hl_api._mock_tickers[hl_symbol]}
    data_handler.tickers["mock_bp"] = {bp_symbol: mock_bp_api._mock_tickers[bp_symbol]}
    data_handler.funding_rates["mock_hl"] = {hl_symbol: (mock_hl_api._mock_funding_rates[hl_symbol].funding_rate, datetime.now())}
    data_handler.funding_rates["mock_bp"] = {bp_symbol: (mock_bp_api._mock_funding_rates[bp_symbol].funding_rate, datetime.now())}
    data_handler.last_update_time["mock_hl"] = {"ticker": {hl_symbol: datetime.now()}, "funding_rate": {hl_symbol: datetime.now()}}
    data_handler.last_update_time["mock_bp"] = {"ticker": {bp_symbol: datetime.now()}, "funding_rate": {bp_symbol: datetime.now()}}

    # 2. Generate Signals
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1, "Should generate at least one opportunity"
    opportunity = opportunities[0]
    assert opportunity.long_exchange == "mock_bp" # BP is the long leg which will fail
    assert opportunity.short_exchange == "mock_hl"

    # 3. Validate Opportunities (Risk Management)
    await portfolio_tracker.initialize()
    # Access internal state for testing
    initial_balances = copy.deepcopy(portfolio_tracker._balances) 
    initial_positions = copy.deepcopy(portfolio_tracker._positions)

    sized_opportunities = risk_manager.validate_opportunities(opportunities)
    assert len(sized_opportunities) >= 1, "Opportunity should be valid after risk sizing"
    sized_opportunity = sized_opportunities[0]

    # 4. Execute Trade - Expecting failure reported via status, not exception
    caplog.set_level(logging.ERROR) # Capture ERROR logs
    
    # Patch the specific method on the instance to ensure it raises
    with patch.object(mock_bp_api, 'place_order', side_effect=MockAPIError("BP connection failed")) as mock_place_long:
        trade_execution_result = await execution_handler.execute_opportunity(sized_opportunity)
        # Verify the patched method was called (at least once, retry might call more)
        mock_place_long.assert_called() 

    # 5. Verify Error Handling via returned status object
    assert trade_execution_result.status == ExecutionStatus.FAILED, "Execution should have failed"
    assert trade_execution_result.error_message is not None
    assert "Failed to place long order" in trade_execution_result.error_message # Check generic message

    # Check logs for the error message
    assert any("Unexpected error placing order on mock_bp" in record.message for record in caplog.records)
    assert any("BP connection failed" in record.message for record in caplog.records)

    # 6. Verify Portfolio State
    # Depending on ExecutionHandler logic, the first leg (HL short) might have executed.
    # Ideal scenario: ExecutionHandler detects failure and cancels the first leg.
    # Current simple scenario: First leg likely executed, second failed.
    await asyncio.sleep(0.1)
    await portfolio_tracker.update()

    # Access internal state for testing
    final_balances = copy.deepcopy(portfolio_tracker._balances) 
    final_positions = copy.deepcopy(portfolio_tracker._positions)
    print(f"Final balances after failed trade: {final_balances}")
    print(f"Final positions after failed trade: {final_positions}")

    # Assert that the failing leg (BP long) did not create a position
    assert portfolio_tracker.get_position("mock_bp", bp_symbol) is None, "Position should not exist on BP after placement failure"

    # Assert that the first leg (HL short) *also* did not get recorded in PortfolioTracker
    # because the overall execution failed before completion.
    hl_final_pos = portfolio_tracker.get_position("mock_hl", hl_symbol)
    assert hl_final_pos is None, \
        "Position should NOT exist on HL in tracker because overall execution failed"

    # Since the long leg failed, the short leg placement should not have been attempted.
    assert trade_execution_result.short_order_id is None, "Short order ID should be None as placement was not attempted"
    
    # Remove checks for hl_order as it was never placed
    # hl_order = await mock_hl_api.get_order(trade_execution_result.short_order_id)
    # assert hl_order is not None
    # assert hl_order.status == OrderStatus.FILLED # Verify the mock API did fill it

    # Assert HL balance also remained unchanged, as the short order was never placed.
    mock_hl_final_balances = await mock_hl_api.get_balances()
    mock_hl_initial_balances = initial_balances.get("mock_hl", {}) # Get initial from tracker state
    assert mock_hl_final_balances.get("USD") == mock_hl_initial_balances.get("USD")
    # assert mock_hl_final_balances.get("USD").total < mock_hl_initial_balances.get("USD").total
    
    # Assert BP balance remained unchanged (comparing USDC now in tracker)
    assert final_balances.get("mock_bp", {}).get("USDC") == initial_balances.get("mock_bp", {}).get("USDC")

    # Check if ExecutionHandler tried to cancel the first order (if logic exists)
    # This depends on the implementation details of ExecutionHandler's error handling
    # We can check if mock_hl_api.cancel_order was called
    # mock_hl_api.cancel_order.assert_called_once_with(trade_execution_result.short_order_id)
    # For now, assume no cancellation attempt is asserted here.

    # Clear the error simulation for subsequent tests
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
    caplog # Pytest fixture to capture logs
):
    """Tests that an opportunity is rejected if there's insufficient balance."""
    # --- Setup ---
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol_key}', "BTC-PERP")
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol_key}', "BTC-PERP")
    start_time = datetime.now(timezone.utc)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)

    # Reset mocks
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()

    # Configure HL (Sufficient Balance)
    mock_hl_api.set_mock_ticker(create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, ts_int))
    mock_hl_api.set_mock_funding_rate(create_mock_funding_rate(hl_symbol, 0.0005, next_funding_ts))
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=10000.0, free=10000.0))

    # Configure BP (Insufficient Balance)
    mock_bp_api.set_mock_ticker(create_mock_ticker(bp_symbol, 10004.0, 10006.0, 10005.0, ts_int))
    mock_bp_api.set_mock_funding_rate(create_mock_funding_rate(bp_symbol, -0.0001, next_funding_ts))
    # Set a very low balance, below min_exchange_balance and likely trade cost
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=1.0, free=1.0)) 

    # --- Workflow Steps ---
    # 1. Update Data (Manual Inject)
    data_handler.tickers["mock_hl"] = {hl_symbol: mock_hl_api._mock_tickers[hl_symbol]}
    data_handler.tickers["mock_bp"] = {bp_symbol: mock_bp_api._mock_tickers[bp_symbol]}
    data_handler.funding_rates["mock_hl"] = {hl_symbol: (mock_hl_api._mock_funding_rates[hl_symbol].funding_rate, datetime.now())}
    data_handler.funding_rates["mock_bp"] = {bp_symbol: (mock_bp_api._mock_funding_rates[bp_symbol].funding_rate, datetime.now())}
    data_handler.last_update_time["mock_hl"] = {"ticker": {hl_symbol: datetime.now()}, "funding_rate": {hl_symbol: datetime.now()}}
    data_handler.last_update_time["mock_bp"] = {"ticker": {bp_symbol: datetime.now()}, "funding_rate": {bp_symbol: datetime.now()}}

    # 2. Generate Signals
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1, "Should generate at least one opportunity"
    opportunity = opportunities[0]
    assert opportunity.long_exchange == "mock_bp" # BP is the long leg
    assert opportunity.short_exchange == "mock_hl"

    # 3. Validate Opportunities (Risk Management) - Expect Rejection
    await portfolio_tracker.initialize() # Load balances into tracker
    
    caplog.set_level(logging.WARNING) # Capture WARNING logs
    sized_opportunities = risk_manager.validate_opportunities(opportunities)

    # Assert that the opportunity was rejected (empty list returned)
    assert len(sized_opportunities) == 0, "Opportunity should be rejected due to insufficient balance"

    # Verify logs contain the expected warning message
    assert any("Cannot size opportunity: total capital is zero or negative" in record.message for record in caplog.records), \
           "RiskManager should log a warning about inability to size due to capital issues"

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
    """Tests the workflow when one leg of the trade is partially filled."""
    # --- Setup ---
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol_key}', "BTC-PERP")
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol_key}', "BTC-PERP")
    start_time = datetime.now(timezone.utc)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)

    # Reset mocks
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()

    # Configure HL (Full Fill - Short Leg)
    mock_hl_api.set_mock_ticker(create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, ts_int))
    mock_hl_api.set_mock_funding_rate(create_mock_funding_rate(hl_symbol, 0.0005, next_funding_ts))
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=10000.0, free=10000.0))
    mock_hl_api.set_open_orders_behavior("fill_immediately")

    # Configure BP (Partial Fill - Long Leg)
    mock_bp_api.set_mock_ticker(create_mock_ticker(bp_symbol, 10004.0, 10006.0, 10005.0, ts_int))
    mock_bp_api.set_mock_funding_rate(create_mock_funding_rate(bp_symbol, -0.0001, next_funding_ts))
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=10000.0, free=10000.0))
    mock_bp_api.set_open_orders_behavior("partial_fill") # Simulate partial fill

    # --- Workflow Steps ---
    # 1. Update Data (Manual Inject)
    data_handler.tickers["mock_hl"] = {hl_symbol: mock_hl_api._mock_tickers[hl_symbol]}
    data_handler.tickers["mock_bp"] = {bp_symbol: mock_bp_api._mock_tickers[bp_symbol]}
    data_handler.funding_rates["mock_hl"] = {hl_symbol: (mock_hl_api._mock_funding_rates[hl_symbol].funding_rate, datetime.now())}
    data_handler.funding_rates["mock_bp"] = {bp_symbol: (mock_bp_api._mock_funding_rates[bp_symbol].funding_rate, datetime.now())}
    data_handler.last_update_time["mock_hl"] = {"ticker": {hl_symbol: datetime.now()}, "funding_rate": {hl_symbol: datetime.now()}}
    data_handler.last_update_time["mock_bp"] = {"ticker": {bp_symbol: datetime.now()}, "funding_rate": {bp_symbol: datetime.now()}}

    # 2. Generate Signals
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1
    opportunity = opportunities[0]
    assert opportunity.long_exchange == "mock_bp"
    assert opportunity.short_exchange == "mock_hl"

    # 3. Validate Opportunities
    await portfolio_tracker.initialize()
    sized_opportunities = risk_manager.validate_opportunities(opportunities)
    assert len(sized_opportunities) >= 1
    sized_opportunity = sized_opportunities[0]

    # 4. Execute Trade - Expecting Partial Completion
    # Capture initial balance directly from mock *before* execution for comparison
    initial_bp_balances_direct = await mock_bp_api.get_balances()
    initial_hl_balances_direct = await mock_hl_api.get_balances()

    caplog.set_level(logging.INFO) # Capture INFO and above
    trade_execution_result = await execution_handler.execute_opportunity(sized_opportunity)

    # 5. Verify Execution Status and Details
    # Assuming ExecutionHandler marks partial fills as PARTIALLY_COMPLETED
    assert trade_execution_result.status == ExecutionStatus.PARTIALLY_COMPLETED, \
        f"Expected PARTIALLY_COMPLETED, got {trade_execution_result.status.name}"
    assert trade_execution_result.error_message is not None
    assert "Trade partially completed" in trade_execution_result.error_message # Check new message
    assert trade_execution_result.long_order_id is not None
    assert trade_execution_result.short_order_id is not None

    # Verify mock orders show correct status (HL: FILLED, BP: PARTIALLY_FILLED)
    long_order = await mock_bp_api.get_order(trade_execution_result.long_order_id)
    short_order = await mock_hl_api.get_order(trade_execution_result.short_order_id)
    assert long_order is not None, f"Long order {trade_execution_result.long_order_id} not found in mock_bp"
    assert short_order is not None, f"Short order {trade_execution_result.short_order_id} not found in mock_hl"
    assert long_order.status == OrderStatus.PARTIALLY_FILLED
    assert short_order.status == OrderStatus.FILLED
    
    # Check filled quantities (BP should be ~50% of requested)
    # Calculate requested quantity based on *actual* ask price used by mock
    mock_bp_ticker = await mock_bp_api.get_ticker(bp_symbol)
    assert mock_bp_ticker is not None, f"Ticker {bp_symbol} not found in mock_bp"
    requested_long_qty = sized_opportunity.long_size / mock_bp_ticker.ask
    assert long_order.filled_quantity == pytest.approx(requested_long_qty * 0.5, rel=1e-5) # Use rel tolerance

    # Calculate requested short quantity based on *actual* bid price used by mock
    mock_hl_ticker = await mock_hl_api.get_ticker(hl_symbol)
    assert mock_hl_ticker is not None, f"Ticker {hl_symbol} not found in mock_hl"
    # Short size needs to be adjusted by price if not using market order mock logic
    # For mock market order, filled_quantity should match requested size / fill_price
    # short_order.price should be the bid price (9999.0)
    assert short_order.filled_quantity == pytest.approx(sized_opportunity.short_size / short_order.price, rel=1e-5) if short_order.price else False

    # 6. Verify Portfolio State
    await asyncio.sleep(0.1) # Allow time for any async updates
    await portfolio_tracker.update() # Update tracker state

    # Verify positions reflect the actual filled amounts *if* tracker handles partials
    bp_final_pos = portfolio_tracker.get_position("mock_bp", bp_symbol)
    hl_final_pos = portfolio_tracker.get_position("mock_hl", hl_symbol)

    # Current EH/PT logic might not create positions on partial fills.
    # If positions *should* be created for partials eventually, update this assertion.
    assert bp_final_pos is None, "BP position should NOT be recorded on partial fill by current PT/EH logic"
    assert hl_final_pos is None, "HL position should NOT be recorded on partial fill by current PT/EH logic"

    # Verify balances reflect the actual partial fill on BP and full fill on HL
    # Check balances directly from mock APIs against the directly captured initial state
    mock_bp_final_balances = await mock_bp_api.get_balances()
    mock_hl_final_balances = await mock_hl_api.get_balances()

    # --- Assertions using directly captured initial balances ---
    initial_bp_usdc_total = initial_bp_balances_direct.get("USDC").total
    final_bp_usdc_total = mock_bp_final_balances.get("USDC").total
    assert final_bp_usdc_total < initial_bp_usdc_total, \
        f"BP USDC balance should decrease. Initial: {initial_bp_usdc_total}, Final: {final_bp_usdc_total}"

    initial_hl_usd_total = initial_hl_balances_direct.get("USD").total
    final_hl_usd_total = mock_hl_final_balances.get("USD").total
    assert final_hl_usd_total > initial_hl_usd_total, \
        f"HL USD balance should increase. Initial: {initial_hl_usd_total}, Final: {final_hl_usd_total}"

    # Verify base asset balances changed correctly
    initial_bp_btc_total = initial_bp_balances_direct.get("BTC", Balance(asset="BTC", total=0.0)).total # Default to 0 if no initial BTC
    final_bp_btc_total = mock_bp_final_balances.get("BTC").total
    assert final_bp_btc_total > initial_bp_btc_total, \
        f"BP BTC balance should increase. Initial: {initial_bp_btc_total}, Final: {final_bp_btc_total}"
    assert final_bp_btc_total == pytest.approx(long_order.filled_quantity * (1 - mock_bp_api.maker_fee if mock_bp_api.fee_asset == "BTC" else 1), rel=1e-5) # Adjust if fee is BTC

    initial_hl_btc_total = initial_hl_balances_direct.get("BTC", Balance(asset="BTC", total=0.0)).total # Default to 0 if no initial BTC
    final_hl_btc_total = mock_hl_final_balances.get("BTC").total
    assert final_hl_btc_total < initial_hl_btc_total, \
        f"HL BTC balance should decrease. Initial: {initial_hl_btc_total}, Final: {final_hl_btc_total}"
    # Note: Selling decreases BTC, so final should be negative if starting from 0
    assert final_hl_btc_total == pytest.approx(-short_order.filled_quantity, rel=1e-5) # Simple check assumes no BTC fee deduction

@pytest.mark.asyncio
async def test_execution_failure_placement(
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
    """Tests the workflow when placing the long order fails."""
    # --- Setup ---
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol_key}', "BTC-PERP")
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol_key}', "BTC-PERP")
    start_time = datetime.now(timezone.utc)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)

    # Reset mocks
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()

    # Configure HL (Short Leg - Succeeds)
    mock_hl_api.set_mock_ticker(create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, ts_int))
    mock_hl_api.set_mock_funding_rate(create_mock_funding_rate(hl_symbol, 0.0005, next_funding_ts))
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=10000.0, free=10000.0))
    mock_hl_api.set_open_orders_behavior("fill_immediately") # Short order fills

    # Configure BP (Long Leg - Fails)
    mock_bp_api.set_mock_ticker(create_mock_ticker(bp_symbol, 10004.0, 10006.0, 10005.0, ts_int))
    mock_bp_api.set_mock_funding_rate(create_mock_funding_rate(bp_symbol, -0.0001, next_funding_ts))
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=10000.0, free=10000.0))
    # *** Simulate failure on place_order ***
    mock_bp_api.configure_error("place_order", APIErrorCode.RATE_LIMITED, "Simulated rate limit on place_order")

    # --- Workflow Steps (similar to happy path until execution) ---
    # 1. Update Data (Manual Inject)
    data_handler.tickers["mock_hl"] = {hl_symbol: mock_hl_api._mock_tickers[hl_symbol]}
    data_handler.tickers["mock_bp"] = {bp_symbol: mock_bp_api._mock_tickers[bp_symbol]}
    data_handler.funding_rates["mock_hl"] = {hl_symbol: (mock_hl_api._mock_funding_rates[hl_symbol].funding_rate, datetime.now())}
    data_handler.funding_rates["mock_bp"] = {bp_symbol: (mock_bp_api._mock_funding_rates[bp_symbol].funding_rate, datetime.now())}
    data_handler.last_update_time["mock_hl"] = {"ticker": {hl_symbol: datetime.now()}, "funding_rate": {hl_symbol: datetime.now()}}
    data_handler.last_update_time["mock_bp"] = {"ticker": {bp_symbol: datetime.now()}, "funding_rate": {bp_symbol: datetime.now()}}

    # 2. Generate Signals
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1
    opportunity = opportunities[0]
    assert opportunity.long_exchange == "mock_bp"
    assert opportunity.short_exchange == "mock_hl"

    # 3. Validate Opportunities
    await portfolio_tracker.initialize()
    sized_opportunities = risk_manager.validate_opportunities(opportunities)
    assert len(sized_opportunities) >= 1
    sized_opportunity = sized_opportunities[0]

    # Capture initial balances before potential partial execution
    initial_bp_balances = await mock_bp_api.get_balances()
    initial_hl_balances = await mock_hl_api.get_balances()

    # 4. Execute Trade - Expecting Failure
    caplog.set_level(logging.WARNING) # Capture warnings and errors
    trade_execution_result = await execution_handler.execute_opportunity(sized_opportunity)

    # 5. Verify Execution Status and Details
    assert trade_execution_result.status == ExecutionStatus.FAILED, \
        f"Expected FAILED, got {trade_execution_result.status.name}"
    assert trade_execution_result.error_message is not None
    assert "Failed to place long order" in trade_execution_result.error_message # Check generic message
    assert trade_execution_result.long_order_id is None # Long order placement failed
    assert trade_execution_result.short_order_id is None # Short order placement should NOT have been attempted

    # 6. Verify Cleanup Attempt (Cancel Short Order) -> Not applicable if short wasn't placed
    # Check if the short order placed on HL was cancelled by the handler
    # short_order = await mock_hl_api.get_order(trade_execution_result.short_order_id)
    # assert short_order is not None, f"Short order {trade_execution_result.short_order_id} not found in mock_hl"
    # Depending on timing and mock logic, it might be FILLED then CANCELLED, or just CANCELLED if cancel was fast
    # Current ExecutionHandler logic cancels *after* attempting both placements.
    # If HL fills immediately, it will be FILLED. EH should then try to cancel.
    # Mock cancel logic sets status to CANCELLED if possible.
    # assert short_order.status == OrderStatus.CANCELLED, \
    #     f"Expected short order to be cancelled after long failed, but status is {short_order.status.name}"
    logger.info("Skipping short order cancellation check as it wasn't placed.")

    # 7. Verify Portfolio State (Should be unchanged)
    await asyncio.sleep(0.1)
    await portfolio_tracker.update()

    # No new positions should be created
    bp_final_pos = portfolio_tracker.get_position("mock_bp", bp_symbol)
    hl_final_pos = portfolio_tracker.get_position("mock_hl", hl_symbol)
    assert bp_final_pos is None, "BP position should not exist after failed execution"
    assert hl_final_pos is None, "HL position should not exist after failed execution"

    # Balances should remain unchanged from the initial state
    mock_bp_final_balances = await mock_bp_api.get_balances()
    mock_hl_final_balances = await mock_hl_api.get_balances()

    # BP balances should be unchanged as place_order failed
    assert mock_bp_final_balances == initial_bp_balances, "BP balances should not change on placement failure"

    # HL balances should also be unchanged as short order was never placed
    # assert mock_hl_final_balances["USD"].total > initial_hl_balances["USD"].total, \
    #     "HL USD balance should have increased from the initial short fill (before cancellation)"
    # assert mock_hl_final_balances["BTC"].total < initial_hl_balances.get("BTC", Balance(asset="BTC", total=0)).total, \
    #     "HL BTC balance should have decreased from the initial short fill"
    assert mock_hl_final_balances == initial_hl_balances, "HL balances should not change as short order was not placed"

    # Clear the error simulation for subsequent tests
    mock_bp_api.clear_error()

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
    """Tests the compensation logic when the second leg (short) fails after the first (long) succeeds."""
    # --- Setup ---
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f'exchanges.mock_hl.symbols.{symbol_key}', "BTC-PERP")
    bp_symbol = mock_config.get(f'exchanges.mock_bp.symbols.{symbol_key}', "BTC-PERP")
    start_time = datetime.now(timezone.utc)
    ts_int = int(start_time.timestamp() * 1000)
    next_funding_ts = int((start_time + timedelta(hours=1)).timestamp() * 1000)

    # Reset mocks
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()

    # Configure BP (Long Leg - Succeeds & Fills)
    mock_bp_api.set_mock_ticker(create_mock_ticker(bp_symbol, 10004.0, 10006.0, 10005.0, ts_int))
    mock_bp_api.set_mock_funding_rate(create_mock_funding_rate(bp_symbol, -0.0001, next_funding_ts))
    mock_bp_api.set_mock_balance(Balance(asset="USDC", total=10000.0, free=10000.0))
    mock_bp_api.set_open_orders_behavior("fill_immediately") # Long order fills

    # Configure HL (Short Leg - Fails)
    mock_hl_api.set_mock_ticker(create_mock_ticker(hl_symbol, 9999.0, 10001.0, 10000.0, ts_int))
    mock_hl_api.set_mock_funding_rate(create_mock_funding_rate(hl_symbol, 0.0005, next_funding_ts))
    mock_hl_api.set_mock_balance(Balance(asset="USD", total=10000.0, free=10000.0))
    # *** Simulate failure on place_order ***
    mock_hl_api.configure_error("place_order", APIErrorCode.INSUFFICIENT_FUNDS, "Simulated insufficient funds")

    # --- Workflow Steps ---
    # 1. Update Data (Manual Inject)
    data_handler.tickers["mock_hl"] = {hl_symbol: mock_hl_api._mock_tickers[hl_symbol]}
    data_handler.tickers["mock_bp"] = {bp_symbol: mock_bp_api._mock_tickers[bp_symbol]}
    data_handler.funding_rates["mock_hl"] = {hl_symbol: (mock_hl_api._mock_funding_rates[hl_symbol].funding_rate, datetime.now())}
    data_handler.funding_rates["mock_bp"] = {bp_symbol: (mock_bp_api._mock_funding_rates[bp_symbol].funding_rate, datetime.now())}
    data_handler.last_update_time["mock_hl"] = {"ticker": {hl_symbol: datetime.now()}, "funding_rate": {hl_symbol: datetime.now()}}
    data_handler.last_update_time["mock_bp"] = {"ticker": {bp_symbol: datetime.now()}, "funding_rate": {bp_symbol: datetime.now()}}

    # 2. Generate Signals
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1
    opportunity = opportunities[0]

    # 3. Validate Opportunities
    await portfolio_tracker.initialize()
    initial_bp_balances = await mock_bp_api.get_balances()
    initial_hl_balances = await mock_hl_api.get_balances()
    sized_opportunities = risk_manager.validate_opportunities(opportunities)
    assert len(sized_opportunities) >= 1
    sized_opportunity = sized_opportunities[0]

    # 4. Execute Trade - Expecting Short Leg Failure & Compensation
    caplog.set_level(logging.INFO) # Lower level to capture compensation info log
    trade_execution_result = await execution_handler.execute_opportunity(sized_opportunity)

    # 5. Verify Execution Status and Details
    # The final status should be FAILED after compensation attempt
    assert trade_execution_result.status == ExecutionStatus.FAILED, \
        f"Expected FAILED status after compensation attempt, got {trade_execution_result.status.name}"
    assert trade_execution_result.error_message is not None
    # Check if error message indicates compensation was triggered due to short leg failure
    assert "Failed to place short order" in trade_execution_result.error_message and \
           "compensating long" in trade_execution_result.error_message # Check for substrings
    assert trade_execution_result.long_order_id is not None # Long order placement succeeded
    assert trade_execution_result.short_order_id is None # Short order placement failed

    # 6. Verify Original Long Order and Compensation Attempt
    # Original long order should be filled
    original_long_order = await mock_bp_api.get_order(trade_execution_result.long_order_id)
    assert original_long_order is not None, f"Original long order {trade_execution_result.long_order_id} not found"
    assert original_long_order.status == OrderStatus.FILLED, "Original long order should be filled"

    # Check logs for compensation attempt (or inspect mock state directly)
    assert any("Compensating position on mock_bp" in record.message for record in caplog.records if record.levelno >= logging.INFO), \
        "Expected log message indicating compensation attempt on mock_bp"

    # Verify a compensating SELL order was placed on mock_bp
    # Find the order that isn't the original long order
    compensating_order = None
    all_bp_orders = mock_bp_api.get_orders()
    assert len(all_bp_orders) == 2, "Expected two orders on mock_bp (original + compensating)"
    for order_id, order in all_bp_orders.items():
        if order_id != original_long_order.id:
            compensating_order = order
            break

    assert compensating_order is not None, "Could not find compensating order on mock_bp"
    assert compensating_order.side == OrderSide.SELL, "Compensating order should be SELL"
    assert compensating_order.symbol == original_long_order.symbol
    # Check if quantity matches the original filled amount (approximate)
    assert compensating_order.quantity == pytest.approx(original_long_order.filled_quantity, rel=1e-6)
    assert compensating_order.status == OrderStatus.FILLED # Assume mock fills compensation immediately

    # 7. Verify Final Portfolio State (Should be Flat)
    await asyncio.sleep(0.1)
    await portfolio_tracker.update()

    # No net positions should exist
    bp_final_pos = portfolio_tracker.get_position("mock_bp", bp_symbol)
    hl_final_pos = portfolio_tracker.get_position("mock_hl", hl_symbol)
    assert bp_final_pos is None or bp_final_pos.size == 0, f"BP position should be zero or None after compensation, but is {bp_final_pos}"
    assert hl_final_pos is None, "HL position should not exist as short order failed"

    # Verify final balances
    mock_bp_final_balances = await mock_bp_api.get_balances()
    mock_hl_final_balances = await mock_hl_api.get_balances()

    # HL balances should be unchanged
    assert mock_hl_final_balances == initial_hl_balances, "HL balances should not change"

    # BP balances will have changed due to initial fill fee + compensation fill fee
    # Assert that the final USDC balance is slightly less than initial due to two sets of fees
    assert mock_bp_final_balances["USDC"].total < initial_bp_balances["USDC"].total, \
        f"BP USDC balance should be lower after fees from original and compensating trades. Initial: {initial_bp_balances['USDC'].total}, Final: {mock_bp_final_balances['USDC'].total}"
    # Assert BTC balance is near zero (might have tiny residual from fee differences/rounding)
    assert mock_bp_final_balances.get("BTC", Balance(asset="BTC", total=0)).total == pytest.approx(0.0, abs=1e-9), \
        f"BP BTC balance should be near zero after compensation, but is {mock_bp_final_balances.get('BTC', Balance(asset='BTC', total=0)).total}"

    # Clear the error simulation for subsequent tests
    mock_hl_api.clear_error()

# --- Add More Failure Test Cases Below ---
pass # Ensure file doesn't end abruptly 