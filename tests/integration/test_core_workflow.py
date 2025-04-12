import asyncio
import logging  # Import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from _pytest.logging import LogCaptureFixture  # Added for caplog typing

# from cyberdelta.apis.base import APIErrorCode, ExchangeAPI # Removed unused import
from cyberdelta.apis.base import APIErrorCode  # Kept APIErrorCode

# Core Components
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.execution_handler import (  # Added TradeExecution
    ExecutionHandler,
    TradeExecution,
    ExecutionStatus,
)

# Models
from cyberdelta.core.models import (
    Balance,
    FundingRate,
    Order,  # Added Order import
    OrderBook,  # Ensure OrderBook is imported
    OrderSide,
    OrderStatus,
    OrderType,  # Added OrderType import
    Position,  # Ensure Position is imported
    Ticker,  # Added Trade import
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.signal_generator import SignalGenerator
from cyberdelta.utils.config import Config  # Assuming Config class is used

# Mocks & Config
from tests.integration.mocks.mock_exchange import MockAPIError, MockExchangeAPI

# Helper Functions


def create_mock_funding_rate(symbol: str, rate: str | Decimal, next_time: datetime) -> FundingRate:
    # Convert rate to Decimal, ensuring string conversion for floats/others
    # Convert next_time to integer timestamp (milliseconds)
    next_funding_timestamp = int(next_time.timestamp() * 1000)
    return FundingRate(
        symbol=symbol,
        funding_rate=Decimal(str(rate)),
        next_funding_time=next_funding_timestamp,  # Pass integer timestamp
    )


# Helper function for creating mock tickers
def create_mock_ticker(
    symbol: str,
    bid: str | float | Decimal,
    ask: str | float | Decimal,
    price: str | float | Decimal,
    timestamp: datetime,
) -> Ticker:
    return Ticker(
        symbol=symbol,
        bid=Decimal(str(bid)),
        ask=Decimal(str(ask)),
        price=Decimal(str(price)),
        timestamp=int(timestamp.timestamp() * 1000),
    )


# Helper function for creating mock order books
def create_mock_orderbook(
    symbol: str,
    bids: list[tuple[str | float | Decimal, str | float | Decimal]],
    asks: list[tuple[str | float | Decimal, str | float | Decimal]],
    timestamp: datetime,
) -> OrderBook:
    return OrderBook(
        symbol=symbol,
        bids=[(Decimal(str(p)), Decimal(str(q))) for p, q in bids],
        asks=[(Decimal(str(p)), Decimal(str(q))) for p, q in asks],
        timestamp=int(timestamp.timestamp() * 1000),
    )


# Initialize logger for this module
logger = logging.getLogger(__name__)

# --- Test Fixtures ---


@pytest.fixture(scope="module")
def mock_config_dict() -> dict[str, Any]:
    """Provides a base configuration dictionary for integration tests."""
    # Combined and cleaned config from previous versions
    return {
        "general": {
            "log_level": "DEBUG",
            "safe_mode": False,
        },
        "cyberdelta": {
            "base_currency": "USD",
            "logging": {"level": "DEBUG"},
            "performance": {"update_interval": 60},
        },
        "exchanges": {
            "mock_hl": {
                "enabled": True,
                "api_key": "mock_hl_key",
                "api_secret": "mock_hl_secret",
                "api_base_url": "mock",
                "ws_url": "mock",
                "symbols": {"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
                "collateral_asset": "USD",
                "fee_asset": "USD",  # Added for consistency
                "maker_fee": "0.0002",  # Use string for Decimal init
                "taker_fee": "0.0005",  # Use string for Decimal init
                "rate_limit": 10,  # Added from previous config
            },
            "mock_bp": {
                "enabled": True,
                "api_key": "mock_bp_key",
                "api_secret": "mock_bp_secret",
                "api_base_url": "mock",
                "ws_url": "mock",
                "symbols": {"BTC": "BTC-USDC", "ETH": "ETH-USDC"},
                "collateral_asset": "USDC",
                "fee_asset": "USDC",  # Added for consistency
                "maker_fee": "0.0001",  # Use string for Decimal init
                "taker_fee": "0.0004",  # Use string for Decimal init
                "rate_limit": 10,  # Added from previous config
            },
        },
        "strategy": {
            "name": "funding_rate_arbitrage",  # Added from previous
            "update_interval": 5,  # Added from previous
            "rebalance_interval": 3600,  # Added from previous
            "symbols": ["BTC", "ETH"],  # Added internal symbols list
            "funding_rate": {
                "min_funding_differential": 0.0001,  # Float ok if SignalGenerator handles conversion
                "min_profit_threshold": 0.1,  # Float ok if SignalGenerator handles conversion
                "max_basis_volatility": 0.02,  # Added from previous
                "staleness_threshold_seconds": 60,
                "risk_aversion": 1.0,  # Moved from old location
            },
        },
        "portfolio_tracker": {
            "update_interval": 5,
            "reconciliation_interval": 60,
            "initial_capital": 100000.0,
        },
        "risk_manager": {
            "enabled": True,  # Added from previous
            "update_interval": 10,  # Added from previous
            "max_total_exposure": 20000.0,
            "max_exchange_exposure": 10000.0,  # Added from previous
            "max_position_size": 5000.0,
            "max_drawdown": 0.10,  # Added from previous
            "capital_allocation_pct": 0.80,
            "min_required_balance": 100.0,
            "slippage_factor": 0.001,
            "circuit_breaker_threshold": 0.1,  # Moved from old location
            "min_exchange_balance": 10.0,  # Moved from old location
        },
        "execution_handler": {
            "update_interval": 1,  # Added from previous
            "order_timeout_seconds": 30,
            "max_retries": 3,  # Changed from 2
            "retry_delay_seconds": 5,  # Changed from 0.05 base
            "max_slippage_pct": 0.001,  # Moved from old location
            "compensation": {
                "enabled": True,
                "max_attempts": 5,
                "check_interval_seconds": 10,
                "use_limit_orders": True,  # Changed from False in happy path
                "limit_order_offset_bps": 5,
                "limit_price_offset_pct": 0.05,  # Kept from old config
            },
        },
        "data_handler": {
            "update_interval": 2,
            "staleness_threshold": {  # Renamed from staleness_thresholds
                "ticker": 15,  # Changed from 60
                "orderbook": 10,  # Changed from 30
                "funding_rate": 60,  # Changed from 3600
                "balance": 30,
                "position": 30,
            },
        },
        "safety_systems": {
            "circuit_breakers": {"enabled": False},
            "position_reconciliation": {"enabled": False},
            "balance_monitoring": {"enabled": False},
        },
    }


@pytest.fixture
def mock_config(mock_config_dict: dict[str, Any]) -> Config:
    """Provides a Config object based on the dictionary."""
    cfg = MagicMock(spec=Config)

    # Define the getter function separately for clarity
    def getter(key: str, default: Any | None = None) -> Any | None:
        return _deep_get(mock_config_dict, key, default)

    cfg.get = getter
    cfg.config_data = mock_config_dict
    return cfg  # type: ignore # Ignore MagicMock return type issue


def _deep_get(d: dict[str, Any], keys: str, default: Any | None = None) -> Any | None:
    """Helper to access nested keys using dot notation."""
    key_parts = keys.split(".")
    val: Any = d
    try:
        for key in key_parts:
            if isinstance(val, dict):
                val = val[key]
            else:
                if key != key_parts[-1]:
                    return default
                return val
        return val
    except (KeyError, TypeError, IndexError):
        return default


@pytest.fixture
def mock_secrets() -> dict[str, dict[str, str]]:
    """Provides dummy secrets (using placeholders)."""
    return {
        "mock_hl": {"api_key": "hl_key", "api_secret": "hl_secret"},
        "mock_bp": {"api_key": "bp_key", "api_secret": "bp_secret"},
    }


@pytest.fixture
def mock_hl_api(mock_config: Config, mock_secrets: dict[str, dict[str, str]]) -> MockExchangeAPI:
    """Instantiate the actual Mock API for Hyperliquid."""
    # Provide default {} and cast config
    exchange_config = cast(
        dict[str, Any], _deep_get(mock_config.config_data, "exchanges.mock_hl", default={}) or {}
    )
    # Access secrets directly and cast
    secrets_raw = mock_secrets.get("mock_hl", {})
    secrets_typed = cast(dict[str, str | None], secrets_raw)  # Explicit cast
    api_mock = MockExchangeAPI(
        exchange_name="mock_hl",
        config=exchange_config,
        secrets=secrets_typed,  # Pass the casted dict
        config_obj=mock_config,
    )
    return api_mock


@pytest.fixture
def mock_bp_api(mock_config: Config, mock_secrets: dict[str, dict[str, str]]) -> MockExchangeAPI:
    """Instantiate the actual Mock API for Backpack."""
    exchange_config = cast(
        dict[str, Any], _deep_get(mock_config.config_data, "exchanges.mock_bp", default={}) or {}
    )
    # Access secrets directly and cast
    secrets_raw = mock_secrets.get("mock_bp", {})
    secrets_typed = cast(dict[str, str | None], secrets_raw)  # Explicit cast
    api_mock = MockExchangeAPI(
        exchange_name="mock_bp",
        config=exchange_config,
        secrets=secrets_typed,  # Pass the casted dict
        config_obj=mock_config,
    )
    return api_mock


@pytest.fixture
def portfolio_tracker(mock_config: Config) -> PortfolioTracker:
    """Portfolio Tracker instance."""
    pt = PortfolioTracker(mock_config)
    # Rely on pt.reset() called in tests to ensure clean state
    return pt


@pytest.fixture
def data_handler(
    mock_config: Config, mock_hl_api: MockExchangeAPI, mock_bp_api: MockExchangeAPI
) -> DataHandler:
    """Data Handler instance with mock APIs registered."""
    dh = DataHandler(mock_config)
    dh.register_api_client("mock_hl", mock_hl_api)
    dh.register_api_client("mock_bp", mock_bp_api)
    return dh


@pytest.fixture
def signal_generator(mock_config: Config, data_handler: DataHandler) -> SignalGenerator:
    """Signal Generator instance."""
    return SignalGenerator(mock_config, data_handler)


@pytest.fixture
def risk_manager(mock_config: Config, portfolio_tracker: PortfolioTracker) -> RiskManager:
    """Risk Manager instance."""
    return RiskManager(mock_config, portfolio_tracker)


@pytest.fixture
def execution_handler(
    mock_config: Config,
    portfolio_tracker: PortfolioTracker,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
) -> ExecutionHandler:
    """Execution Handler instance with mock APIs registered."""
    eh = ExecutionHandler(mock_config, portfolio_tracker)
    eh.register_api_client("mock_hl", mock_hl_api)
    eh.register_api_client("mock_bp", mock_bp_api)
    return eh


# --- Integration Test --- NEW STRUCTURE BELOW ---


# Helper function to ensure nested dict structure exists in DataHandler
def _ensure_dh_structure(dh: DataHandler, exchange: str, symbol: str):
    if exchange not in dh.tickers:
        dh.tickers[exchange] = {}
    if exchange not in dh.funding_rates:
        dh.funding_rates[exchange] = {}
    if exchange not in dh.orderbooks:
        dh.orderbooks[exchange] = {}
    if exchange not in dh.last_update_time:
        dh.last_update_time[exchange] = {}
    for data_type in ["ticker", "funding_rate", "orderbook"]:
        if data_type not in dh.last_update_time[exchange]:
            dh.last_update_time[exchange][data_type] = {}
    # Ensure the symbol exists in the last update time sub-dicts
    if symbol not in dh.last_update_time[exchange].get("ticker", {}):
        dh.last_update_time[exchange].setdefault("ticker", {})[symbol] = datetime.fromtimestamp(
            0, UTC
        )
    if symbol not in dh.last_update_time[exchange].get("funding_rate", {}):
        dh.last_update_time[exchange].setdefault("funding_rate", {})[symbol] = (
            datetime.fromtimestamp(0, UTC)
        )
    if symbol not in dh.last_update_time[exchange].get("orderbook", {}):
        dh.last_update_time[exchange].setdefault("orderbook", {})[symbol] = datetime.fromtimestamp(
            0, UTC
        )


@pytest.mark.asyncio
async def test_happy_path_full_cycle(
    mock_config: Config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    portfolio_tracker: PortfolioTracker,
    data_handler: DataHandler,
    signal_generator: SignalGenerator,
    risk_manager: RiskManager,
    execution_handler: ExecutionHandler,
    caplog: LogCaptureFixture,
):
    """Tests the full arbitrage cycle: data -> signal -> validation -> execution -> portfolio update."""
    # --- Force DEBUG logging for this test --- 
    caplog.set_level(logging.DEBUG)
    # Also configure the specific loggers if needed (optional)
    logging.getLogger("cyberdelta.core.signal_generator").setLevel(logging.DEBUG)
    logging.getLogger("cyberdelta.core.data_handler").setLevel(logging.DEBUG)
    # --- End Log Setup ---
    
    logger.info("Starting happy path integration test...")

    # --- Setup ---
    symbol_base = "BTC"
    symbol_hl = "BTC-PERP"
    symbol_bp = "BTC-USDC"
    start_time = datetime.now(UTC)
    initial_usdc_balance = Decimal("100000.0")

    # 1. Initialize PortfolioTracker with balances
    await portfolio_tracker.initialize()
    portfolio_tracker.update_balance(
        "mock_hl", "USD", Balance(asset="USD", total=initial_usdc_balance, available=initial_usdc_balance)
    )
    portfolio_tracker.update_balance(
        "mock_bp", "USDC", Balance(asset="USDC", total=initial_usdc_balance, available=initial_usdc_balance)
    )

    # 2. Set mock data in APIs and DataHandler
    # Tickers
    mock_hl_ticker = create_mock_ticker(symbol_hl, "29999.0", "30001.0", "30000.0", start_time)
    mock_bp_ticker = create_mock_ticker(symbol_bp, "29998.0", "30000.0", "29999.0", start_time)
    mock_hl_api.set_mock_ticker(mock_hl_ticker)
    mock_bp_api.set_mock_ticker(mock_bp_ticker)

    # Funding Rates
    mock_hl_funding = create_mock_funding_rate(
        symbol_hl, "-0.0002", start_time + timedelta(hours=1)
    )
    mock_bp_funding = create_mock_funding_rate(
        symbol_bp, "0.0001", start_time + timedelta(hours=1)
    )
    mock_hl_api.set_mock_funding_rate(mock_hl_funding)
    mock_bp_api.set_mock_funding_rate(mock_bp_funding)

    # Order Books (NEW - Fix 40)
    mock_hl_ob = create_mock_orderbook(
        symbol_hl,
        bids=[("29999.0", "10.0"), ("29998.0", "5.0")],
        asks=[("30001.0", "8.0"), ("30002.0", "6.0")],
        timestamp=start_time,
    )
    mock_bp_ob = create_mock_orderbook(
        symbol_bp,
        bids=[("29998.0", "12.0"), ("29997.0", "7.0")],
        asks=[("30000.0", "9.0"), ("30001.0", "4.0")],
        timestamp=start_time,
    )
    # Configure mock APIs to return these order books
    mock_hl_api.get_order_book = AsyncMock(return_value=mock_hl_ob)
    mock_bp_api.get_order_book = AsyncMock(return_value=mock_bp_ob)

    # Manually populate DataHandler's internal state
    # Use NESTED dictionary structure matching DataHandler.__init__
    if "mock_hl" not in data_handler.tickers: data_handler.tickers["mock_hl"] = {}
    if "mock_bp" not in data_handler.tickers: data_handler.tickers["mock_bp"] = {}
    if "mock_hl" not in data_handler.funding_rates: data_handler.funding_rates["mock_hl"] = {}
    if "mock_bp" not in data_handler.funding_rates: data_handler.funding_rates["mock_bp"] = {}
    if "mock_hl" not in data_handler.orderbooks: data_handler.orderbooks["mock_hl"] = {}
    if "mock_bp" not in data_handler.orderbooks: data_handler.orderbooks["mock_bp"] = {}
    
    data_handler.tickers["mock_hl"][symbol_hl] = mock_hl_ticker
    data_handler.tickers["mock_bp"][symbol_bp] = mock_bp_ticker
    # Store funding rates as tuple (rate, timestamp) - assuming timestamp needed later
    data_handler.funding_rates["mock_hl"][symbol_hl] = (mock_hl_funding.funding_rate, mock_hl_funding.timestamp)
    data_handler.funding_rates["mock_bp"][symbol_bp] = (mock_bp_funding.funding_rate, mock_bp_funding.timestamp)
    data_handler.orderbooks["mock_hl"][symbol_hl] = mock_hl_ob
    data_handler.orderbooks["mock_bp"][symbol_bp] = mock_bp_ob

    # Update last update times (using nested structure)
    if "mock_hl" not in data_handler.last_update_time: data_handler.last_update_time["mock_hl"] = {"ticker": {}, "funding_rate": {}, "orderbook": {}}
    if "mock_bp" not in data_handler.last_update_time: data_handler.last_update_time["mock_bp"] = {"ticker": {}, "funding_rate": {}, "orderbook": {}}
    data_handler.last_update_time["mock_hl"]["ticker"][symbol_hl] = start_time
    data_handler.last_update_time["mock_bp"]["ticker"][symbol_bp] = start_time
    data_handler.last_update_time["mock_hl"]["funding_rate"][symbol_hl] = start_time
    data_handler.last_update_time["mock_bp"]["funding_rate"][symbol_bp] = start_time
    data_handler.last_update_time["mock_hl"]["orderbook"][symbol_hl] = start_time
    data_handler.last_update_time["mock_bp"]["orderbook"][symbol_bp] = start_time

    # --- Log DataHandler state BEFORE generating opportunities ---
    logger.debug("--- DataHandler State Check Before Generate --- ")
    logger.debug(f"Tickers Keys: {list(data_handler.tickers.keys())}")
    logger.debug(f"Funding Keys: {list(data_handler.funding_rates.keys())}")
    # Log nested structure
    logger.debug(f"HL Ticker Data (Nested): {data_handler.tickers.get('mock_hl', {}).get(symbol_hl)}")
    logger.debug(f"BP Ticker Data (Nested): {data_handler.tickers.get('mock_bp', {}).get(symbol_bp)}")
    logger.debug(f"HL Funding Data (Nested): {data_handler.funding_rates.get('mock_hl', {}).get(symbol_hl)}")
    logger.debug(f"BP Funding Data (Nested): {data_handler.funding_rates.get('mock_bp', {}).get(symbol_bp)}")
    logger.debug("--- End DataHandler State Check --- ")
    # -----------------------------------------------------------

    # 3. Generate Opportunities
    logger.info("Generating opportunities...")
    opportunities = signal_generator.generate_opportunities()
    logger.info(f"Generated opportunities: {opportunities}")
    assert len(opportunities) >= 1, "No opportunities generated"
    opportunity = opportunities[0]

    # 4. Validate Opportunity Details (Basic Checks)
    # Note: SignalGenerator now uses the *internal* symbol for the opportunity
    assert opportunity.symbol == symbol_base # Check against internal symbol
    assert opportunity.long_exchange == "mock_bp" # Check opportunity details
    assert opportunity.short_exchange == "mock_hl"
    assert opportunity.long_price == mock_bp_ticker.ask # Price to buy on long exchange
    assert opportunity.short_price == mock_hl_ticker.bid # Price to sell on short exchange
    assert opportunity.long_funding_rate == mock_bp_funding.funding_rate
    assert opportunity.short_funding_rate == mock_hl_funding.funding_rate
    expected_nfd = mock_bp_funding.funding_rate - mock_hl_funding.funding_rate
    assert opportunity.net_funding_differential == pytest.approx(expected_nfd)
    assert opportunity.expected_profit is not None
    assert opportunity.expected_profit > 0

    # 5. Validate & Size Opportunity with RiskManager
    # RM needs portfolio state (balances mainly)
    # Note: initialize() fetches balances, manually set ones are overridden unless initialize called *after*
    # Let's ensure tracker has the intended balances before RM runs
    portfolio_tracker.update_balance("mock_hl", "USD", Balance(asset="USD", total=initial_usdc_balance, available=initial_usdc_balance))
    portfolio_tracker.update_balance("mock_bp", "USDC", Balance(asset="USDC", total=initial_usdc_balance, available=initial_usdc_balance))
    # Manually update derived metrics if needed (depends on RM implementation)
    # await portfolio_tracker.update() # Assuming update calculates total capital etc.
    # Let's assume RM uses get_total_capital directly from balances for now
    logger.info(f"Portfolio Total Capital for Sizing (from balances): {portfolio_tracker.get_total_capital()}")

    logger.info("Validating and sizing opportunities with RiskManager...")
    from cyberdelta.core.risk_manager import SizedOpportunity # Corrected import path
    sized_opportunities: list[SizedOpportunity] = risk_manager.validate_opportunities([opportunity])
    logger.info(f"Validated {len(sized_opportunities)} opportunities.")
    assert len(sized_opportunities) == 1, "Opportunity should be valid and sized by RiskManager"
    sized_opportunity = sized_opportunities[0]

    # Check sizing logic results
    assert sized_opportunity.long_size > 0
    assert sized_opportunity.short_size > 0
    assert sized_opportunity.long_size == sized_opportunity.short_size # Should be delta neutral
    # Check against max position size config
    max_size_usd = mock_config.get("risk_manager.max_position_size")
    assert max_size_usd is not None
    # Use the sized opportunity's USD sizes directly
    assert sized_opportunity.long_size <= Decimal(str(max_size_usd)), "Long size exceeds max position size"
    assert sized_opportunity.short_size <= Decimal(str(max_size_usd)), "Short size exceeds max position size"

    # 6. Execute Sized Opportunity
    logger.info(
        f"Executing opportunity: {sized_opportunity.opportunity.long_exchange} LONG {sized_opportunity.long_size} {symbol_bp}, "
        f"{sized_opportunity.opportunity.short_exchange} SHORT {sized_opportunity.short_size} {symbol_hl}"
    )
    trade_execution_result: TradeExecution = await execution_handler.execute_opportunity(
        sized_opportunity
    )

    # 7. Verify Execution Result from ExecutionHandler
    logger.info(f"Execution result: {trade_execution_result}")
    assert trade_execution_result.status == ExecutionStatus.COMPLETED, (
        f"Execution failed or incomplete: Status={trade_execution_result.status.name}, "
        f"Error='{trade_execution_result.error_message}'"
    )
    assert trade_execution_result.long_order_id is not None
    assert trade_execution_result.short_order_id is not None

    # --- Wait briefly for mocks to process --- 
    await asyncio.sleep(0.1)

    # 8. Verify Portfolio State (Check *internal* tracker state directly - Fix 43)
    logger.info("Verifying portfolio state post-execution...")

    # Get final balances (should be updated by ExecutionHandler via PortfolioTracker.record_trade)
    hl_balance = portfolio_tracker.get_asset_balance("mock_hl", "USD")
    bp_balance = portfolio_tracker.get_asset_balance("mock_bp", "USDC")
    assert hl_balance is not None
    assert bp_balance is not None
    logger.debug(f"Final HL Balance: {hl_balance.total} (Available: {hl_balance.available})")
    logger.debug(f"Final BP Balance: {bp_balance.total} (Available: {bp_balance.available})")
    # Add assertions about balance changes if fees/costs are accurately simulated

    # Get final positions (should be updated by ExecutionHandler via PortfolioTracker.record_trade)
    hl_pos = portfolio_tracker.get_position("mock_hl", symbol_base)
    bp_pos = portfolio_tracker.get_position("mock_bp", symbol_base)

    logger.debug(f"Final HL Position: {hl_pos}")
    logger.debug(f"Final BP Position: {bp_pos}")

    assert hl_pos is not None, f"Hyperliquid position ({symbol_base}) not found in tracker"
    assert bp_pos is not None, f"Backpack position ({symbol_base}) not found in tracker"

    # Verify position details (size, side, entry price)
    assert hl_pos.symbol == symbol_base # Check internal symbol stored
    assert hl_pos.side == OrderSide.SELL # Short on HL
    hl_order = await mock_hl_api.get_order_status(trade_execution_result.short_order_id)
    assert hl_pos.size == pytest.approx(hl_order.filled_quantity)
    # Use average fill price if available, otherwise order price
    hl_entry = hl_order.avg_fill_price if hl_order.avg_fill_price else hl_order.price
    assert hl_pos.entry_price == pytest.approx(hl_entry)

    assert bp_pos.symbol == symbol_base # Check internal symbol stored
    assert bp_pos.side == OrderSide.BUY # Long on BP
    bp_order = await mock_bp_api.get_order_status(trade_execution_result.long_order_id)
    assert bp_pos.size == pytest.approx(bp_order.filled_quantity)
    # Use average fill price if available, otherwise order price
    bp_entry = bp_order.avg_fill_price if bp_order.avg_fill_price else bp_order.price
    assert bp_pos.entry_price == pytest.approx(bp_entry)

    # Check overall net position (should be delta neutral for the base symbol)
    # This requires a method like get_net_position or manual calculation
    # net_position_btc = portfolio_tracker.get_net_position(symbol_base) # Example
    # assert net_position_btc == Decimal("0.0") # Example assertion

    logger.info("Happy path integration test completed successfully.")


# --- test_api_error_during_placement needs significant rework ---
@pytest.mark.skip(reason="Rework needed for new structure and error handling")
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
    caplog: LogCaptureFixture,
):
    pass  # Rework required


# --- test_insufficient_balance needs rework ---
@pytest.mark.skip(reason="Rework needed for new structure and balance checks")
@pytest.mark.asyncio
async def test_insufficient_balance(
    mock_config: Config,
    mock_hl_api: MockExchangeAPI,
    mock_bp_api: MockExchangeAPI,
    data_handler: DataHandler,
    signal_generator: SignalGenerator,
    risk_manager: RiskManager,
    portfolio_tracker: PortfolioTracker,
    caplog: LogCaptureFixture,
):
    pass  # Rework required


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
    caplog: LogCaptureFixture,
):
    """
    Test scenario where one leg of the trade fills partially initially,
    requiring compensation logic (which should eventually fully fill or handle).
    """
    caplog.set_level(logging.DEBUG)

    # --- Setup Mock Data ---
    now = datetime.now(UTC)
    symbol_key = "BTC"
    hl_symbol = mock_config.get(f"exchanges.mock_hl.symbols.{symbol_key}")
    bp_symbol = mock_config.get(f"exchanges.mock_bp.symbols.{symbol_key}")
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()

    # Tickers
    mock_hl_ticker = create_mock_ticker(hl_symbol, 40000.0, 40002.0, 40001.0, now)
    mock_bp_ticker = create_mock_ticker(bp_symbol, 40004.0, 40006.0, 40005.0, now)
    mock_hl_api.set_mock_ticker(mock_hl_ticker)
    mock_bp_api.set_mock_ticker(mock_bp_ticker)

    # Funding Rates
    next_funding_dt = now + timedelta(hours=1)
    mock_hl_funding = create_mock_funding_rate(
        hl_symbol, "0.0002", next_funding_dt
    )  # Short HL (pay)
    mock_bp_funding = create_mock_funding_rate(
        bp_symbol, "-0.0003", next_funding_dt
    )  # Long BP (receive)
    mock_hl_api.set_mock_funding_rate(mock_hl_funding)
    mock_bp_api.set_mock_funding_rate(mock_bp_funding)

    # Order Books
    mock_hl_ob = create_mock_orderbook(hl_symbol, [(40000.0, 1.0)], [(40002.0, 1.5)], now)
    mock_bp_ob = create_mock_orderbook(bp_symbol, [(40004.0, 0.5)], [(40006.0, 2.0)], now)
    mock_hl_api.get_order_book = AsyncMock(return_value=mock_hl_ob)
    mock_bp_api.get_order_book = AsyncMock(return_value=mock_bp_ob)

    # Configure initial balances
    mock_hl_api.set_mock_balance(
        Balance(asset="USD", total=Decimal("10000"), free=Decimal("10000"))
    )
    mock_bp_api.set_mock_balance(
        Balance(asset="USDC", total=Decimal("10000"), free=Decimal("10000"))
    )
    await portfolio_tracker.initialize()

    # --- Manually Populate DataHandler ---
    internal_symbol = symbol_key
    _ensure_dh_structure(data_handler, mock_hl_api.exchange_name, internal_symbol)
    _ensure_dh_structure(data_handler, mock_bp_api.exchange_name, internal_symbol)

    data_handler.tickers[mock_hl_api.exchange_name][internal_symbol] = mock_hl_ticker  # type: ignore
    data_handler.tickers[mock_bp_api.exchange_name][internal_symbol] = mock_bp_ticker  # type: ignore

    # Populate Funding Rates (Convert to float rate and datetime object for DataHandler)
    assert mock_hl_funding.next_funding_time is not None
    hl_rate_float = float(mock_hl_funding.funding_rate)
    hl_next_funding_dt = datetime.fromtimestamp(mock_hl_funding.next_funding_time / 1000, tz=UTC)
    data_handler.funding_rates[mock_hl_api.exchange_name][internal_symbol] = (
        hl_rate_float,
        hl_next_funding_dt,
    )

    assert mock_bp_funding.next_funding_time is not None
    bp_rate_float = float(mock_bp_funding.funding_rate)
    bp_next_funding_dt = datetime.fromtimestamp(mock_bp_funding.next_funding_time / 1000, tz=UTC)
    data_handler.funding_rates[mock_bp_api.exchange_name][internal_symbol] = (
        bp_rate_float,
        bp_next_funding_dt,
    )

    data_handler.orderbooks[mock_hl_api.exchange_name][internal_symbol] = mock_hl_ob
    data_handler.orderbooks[mock_bp_api.exchange_name][internal_symbol] = mock_bp_ob
    data_handler.last_update_time[mock_hl_api.exchange_name]["ticker"][internal_symbol] = now
    data_handler.last_update_time[mock_bp_api.exchange_name]["ticker"][internal_symbol] = now
    data_handler.last_update_time[mock_hl_api.exchange_name]["funding_rate"][internal_symbol] = now
    data_handler.last_update_time[mock_bp_api.exchange_name]["funding_rate"][internal_symbol] = now
    data_handler.last_update_time[mock_hl_api.exchange_name]["orderbook"][internal_symbol] = now
    data_handler.last_update_time[mock_bp_api.exchange_name]["orderbook"][internal_symbol] = now

    # --- Configure Mock Behavior for Partial Fill ---
    target_qty = Decimal("0.1")
    partial_fill_qty = target_qty / 2
    long_order_id_bp = "bp_long_partial"
    short_order_id_hl = "hl_short_full"

    # BP (Long) - Simulate partial fill then full fill
    bp_place_call_count = 0

    async def place_order_side_effect_bp(*args: Any, **kwargs: Any) -> Order:
        nonlocal bp_place_call_count
        bp_place_call_count += 1
        if bp_place_call_count == 1:
            return Order(
                id=long_order_id_bp,
                status=OrderStatus.PARTIALLY_FILLED,
                filled_quantity=partial_fill_qty,
                avg_fill_price=Decimal("40006.0"),
                symbol=bp_symbol,
                side=OrderSide.BUY,
                type=OrderType.MARKET,
                quantity=target_qty,
                time=int(now.timestamp() * 1000),
            )
        else:
            raise MockAPIError(f"place_order on BP called unexpectedly {bp_place_call_count} times")

    mock_bp_api.place_order = AsyncMock(side_effect=place_order_side_effect_bp)

    bp_status_call_count = 0

    async def get_order_status_side_effect_bp(*args: Any, **kwargs: Any) -> Order | None:
        nonlocal bp_status_call_count
        order_id = kwargs.get("order_id") or (args[1] if len(args) > 1 else None)
        if order_id != long_order_id_bp:
            return None
        bp_status_call_count += 1
        if bp_status_call_count == 1:
            logger.debug(
                f"MOCK BP get_order_status (call {bp_status_call_count}): Returning PARTIALLY_FILLED"
            )
            return Order(
                id=long_order_id_bp,
                status=OrderStatus.PARTIALLY_FILLED,
                filled_quantity=partial_fill_qty,
                avg_fill_price=Decimal("40006.0"),
                symbol=bp_symbol,
                side=OrderSide.BUY,
                type=OrderType.MARKET,
                quantity=target_qty,
                time=int(now.timestamp() * 1000),
            )
        else:
            logger.debug(
                f"MOCK BP get_order_status (call {bp_status_call_count}): Returning FILLED"
            )
            return Order(
                id=long_order_id_bp,
                status=OrderStatus.FILLED,
                filled_quantity=target_qty,
                avg_fill_price=Decimal("40006.0"),
                symbol=bp_symbol,
                side=OrderSide.BUY,
                type=OrderType.MARKET,
                quantity=target_qty,
                time=int(now.timestamp() * 1000),
            )

    mock_bp_api.get_order_status = AsyncMock(side_effect=get_order_status_side_effect_bp)

    # HL (Short) - Fills completely
    hl_order_response = Order(
        id=short_order_id_hl,
        status=OrderStatus.FILLED,
        filled_quantity=target_qty,
        avg_fill_price=Decimal("40000.0"),
        symbol=hl_symbol,
        side=OrderSide.SELL,
        type=OrderType.MARKET,
        quantity=target_qty,
        time=int(now.timestamp() * 1000),
    )
    mock_hl_api.place_order = AsyncMock(return_value=hl_order_response)
    mock_hl_api.get_order_status = AsyncMock(return_value=hl_order_response)

    # --- Execute Test ---
    # 1. Generate Signal
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1
    opportunity = opportunities[0]
    assert opportunity.long_exchange == "mock_bp"
    assert opportunity.short_exchange == "mock_hl"
    assert opportunity.long_funding_rate == mock_bp_funding.funding_rate
    assert opportunity.short_funding_rate == mock_hl_funding.funding_rate
    expected_nfd = mock_bp_funding.funding_rate - mock_hl_funding.funding_rate
    assert opportunity.net_funding_differential == pytest.approx(expected_nfd)  # type: ignore

    # 2. Validate & Size
    sized_opportunities = risk_manager.validate_opportunities([opportunity])
    assert len(sized_opportunities) == 1
    sized_opportunity = sized_opportunities[0]
    sized_opportunity.long_size = target_qty
    sized_opportunity.short_size = target_qty

    # 3. Execute
    logger.info("Executing partially filling opportunity...")
    trade_execution_result: TradeExecution = await execution_handler.execute_opportunity(
        sized_opportunity
    )
    logger.info(f"Execution result: {trade_execution_result}")

    # 4. Verification
    assert trade_execution_result.status == ExecutionStatus.COMPLETED  # type: ignore
    assert trade_execution_result.long_order_id == long_order_id_bp
    assert trade_execution_result.short_order_id == short_order_id_hl

    # Check logs
    await asyncio.sleep(0.5)
    assert any("Partially filled order detected" in record.message for record in caplog.records), (
        "Compensation log for partial fill not found"
    )
    assert any("Compensation check for order" in record.message for record in caplog.records), (
        "Compensation check log not found"
    )
    assert any(
        "successfully filled via compensation check" in record.message for record in caplog.records
    ), "Compensation success log not found"

    # 5. Verify Final Portfolio State
    logger.info("Verifying final portfolio state after partial fill...")
    long_order = portfolio_tracker.get_order("mock_bp", long_order_id_bp)
    short_order = portfolio_tracker.get_order("mock_hl", short_order_id_hl)

    assert long_order is not None
    assert short_order is not None
    assert long_order.status == OrderStatus.FILLED, (
        f"Long order final status was {long_order.status}"
    )
    assert short_order.status == OrderStatus.FILLED

    # Assert final filled quantities
    assert long_order.filled_quantity == pytest.approx(target_qty)  # type: ignore
    assert short_order.filled_quantity == pytest.approx(target_qty)  # type: ignore

    # Check final positions
    bp_pos = portfolio_tracker.get_position("mock_bp", internal_symbol)
    hl_pos = portfolio_tracker.get_position("mock_hl", internal_symbol)

    assert bp_pos is not None, "BP Position not found"
    assert hl_pos is not None, "HL Position not found"
    assert bp_pos.side == OrderSide.BUY
    assert hl_pos.side == OrderSide.SELL
    assert bp_pos.size == pytest.approx(long_order.filled_quantity)  # type: ignore
    assert hl_pos.size == pytest.approx(short_order.filled_quantity)  # type: ignore
    assert bp_pos.entry_price == pytest.approx(long_order.avg_fill_price)  # type: ignore
    assert hl_pos.entry_price == pytest.approx(short_order.avg_fill_price)  # type: ignore

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
    caplog: LogCaptureFixture,
):
    """
    Tests compensation logic when one leg fails entirely during execution.
    Ensures the successfully executed leg is reversed.
    """
    caplog.set_level(logging.DEBUG)

    # --- Setup Mock Data ---
    now = datetime.now(UTC)
    symbol_key = "ETH"
    hl_symbol = mock_config.get(f"exchanges.mock_hl.symbols.{symbol_key}")
    bp_symbol = mock_config.get(f"exchanges.mock_bp.symbols.{symbol_key}")
    mock_hl_api.reset()
    mock_bp_api.reset()
    portfolio_tracker.reset()

    # Tickers
    mock_hl_ticker = create_mock_ticker(hl_symbol, 2000.0, 2000.5, 2000.25, now)
    mock_bp_ticker = create_mock_ticker(bp_symbol, 2001.0, 2001.5, 2001.25, now)
    mock_hl_api.set_mock_ticker(mock_hl_ticker)
    mock_bp_api.set_mock_ticker(mock_bp_ticker)

    # Funding Rates
    next_funding_dt = now + timedelta(hours=1)
    mock_hl_funding = create_mock_funding_rate(hl_symbol, "0.0001", next_funding_dt)
    mock_bp_funding = create_mock_funding_rate(bp_symbol, "-0.0002", next_funding_dt)
    mock_hl_api.set_mock_funding_rate(mock_hl_funding)
    mock_bp_api.set_mock_funding_rate(mock_bp_funding)

    # Order Books
    mock_hl_ob = create_mock_orderbook(hl_symbol, [(2000.0, 5.0)], [(2000.5, 3.0)], now)
    mock_bp_ob = create_mock_orderbook(bp_symbol, [(2001.0, 2.0)], [(2001.5, 4.0)], now)
    mock_hl_api.get_order_book = AsyncMock(return_value=mock_hl_ob)
    mock_bp_api.get_order_book = AsyncMock(return_value=mock_bp_ob)

    # Configure initial balances
    initial_hl_balance = Balance(asset="USD", total=Decimal("10000"), free=Decimal("10000"))
    initial_bp_balance = Balance(asset="USDC", total=Decimal("10000"), free=Decimal("10000"))
    mock_hl_api.set_mock_balance(initial_hl_balance)
    mock_bp_api.set_mock_balance(initial_bp_balance)
    await portfolio_tracker.initialize()

    # --- Manually Populate DataHandler ---
    internal_symbol = symbol_key
    _ensure_dh_structure(data_handler, mock_hl_api.exchange_name, internal_symbol)
    _ensure_dh_structure(data_handler, mock_bp_api.exchange_name, internal_symbol)

    data_handler.tickers[mock_hl_api.exchange_name][internal_symbol] = mock_hl_ticker  # type: ignore
    data_handler.tickers[mock_bp_api.exchange_name][internal_symbol] = mock_bp_ticker  # type: ignore

    # Populate Funding Rates (Convert to float rate and datetime object for DataHandler)
    assert mock_hl_funding.next_funding_time is not None
    hl_rate_float = float(mock_hl_funding.funding_rate)
    hl_next_funding_dt = datetime.fromtimestamp(mock_hl_funding.next_funding_time / 1000, tz=UTC)
    data_handler.funding_rates[mock_hl_api.exchange_name][internal_symbol] = (
        hl_rate_float,
        hl_next_funding_dt,
    )

    assert mock_bp_funding.next_funding_time is not None
    bp_rate_float = float(mock_bp_funding.funding_rate)
    bp_next_funding_dt = datetime.fromtimestamp(mock_bp_funding.next_funding_time / 1000, tz=UTC)
    data_handler.funding_rates[mock_bp_api.exchange_name][internal_symbol] = (
        bp_rate_float,
        bp_next_funding_dt,
    )

    data_handler.orderbooks[mock_hl_api.exchange_name][internal_symbol] = mock_hl_ob
    data_handler.orderbooks[mock_bp_api.exchange_name][internal_symbol] = mock_bp_ob
    data_handler.last_update_time[mock_hl_api.exchange_name]["ticker"][internal_symbol] = now
    data_handler.last_update_time[mock_bp_api.exchange_name]["ticker"][internal_symbol] = now
    data_handler.last_update_time[mock_hl_api.exchange_name]["funding_rate"][internal_symbol] = now
    data_handler.last_update_time[mock_bp_api.exchange_name]["funding_rate"][internal_symbol] = now
    data_handler.last_update_time[mock_hl_api.exchange_name]["orderbook"][internal_symbol] = now
    data_handler.last_update_time[mock_bp_api.exchange_name]["orderbook"][internal_symbol] = now

    # --- Configure Mock Behavior for Execution Failure ---
    target_qty = Decimal("1.0")
    long_order_id_bp = "bp_long_success"
    short_order_id_hl = "hl_short_fails"
    compensating_order_id_bp = "bp_compensate_sell"

    # BP (Long) - Succeeds initially
    long_fill_price = mock_bp_ticker.ask
    bp_initial_order = Order(
        id=long_order_id_bp,
        status=OrderStatus.FILLED,
        filled_quantity=target_qty,
        avg_fill_price=long_fill_price,
        symbol=bp_symbol,
        side=OrderSide.BUY,
        type=OrderType.MARKET,
        quantity=target_qty,
        time=int(now.timestamp() * 1000),
    )

    # HL (Short) - Fails
    hl_api_error = MockAPIError("Simulated HL execution failure", APIErrorCode.EXCHANGE_SPECIFIC)

    # BP Compensation order (Sell) - Should succeed
    compensation_fill_price = mock_bp_ticker.bid
    bp_compensation_order = Order(
        id=compensating_order_id_bp,
        status=OrderStatus.FILLED,
        filled_quantity=target_qty,
        avg_fill_price=compensation_fill_price,
        symbol=bp_symbol,
        side=OrderSide.SELL,
        type=OrderType.MARKET,
        quantity=target_qty,
        time=int(now.timestamp() * 1000),
    )

    # --- Setup Mock `place_order` Side Effects ---
    bp_place_call_num = 0

    async def place_order_bp_side_effect(*args: Any, **kwargs: Any) -> Order:
        nonlocal bp_place_call_num
        bp_place_call_num += 1
        side = kwargs.get("side")
        logger.debug(f"MOCK BP place_order (call {bp_place_call_num}, side={side}) called")
        if bp_place_call_num == 1 and side == OrderSide.BUY:
            logger.debug("MOCK BP place_order: Returning successful initial long order.")
            return bp_initial_order
        elif bp_place_call_num == 2 and side == OrderSide.SELL:
            logger.debug("MOCK BP place_order: Returning successful compensation sell order.")
            return bp_compensation_order
        else:
            logger.error(
                f"MOCK BP place_order: Unexpected call {bp_place_call_num} with side {side}"
            )
            raise MockAPIError(
                f"Unexpected BP place_order call {bp_place_call_num} with side {side}"
            )

    hl_place_call_num = 0

    async def place_order_hl_side_effect(*args: Any, **kwargs: Any):
        nonlocal hl_place_call_num
        hl_place_call_num += 1
        side = kwargs.get("side")
        logger.debug(f"MOCK HL place_order (call {hl_place_call_num}, side={side}) called")
        if hl_place_call_num == 1 and side == OrderSide.SELL:
            logger.debug("MOCK HL place_order: Raising simulated API error.")
            raise hl_api_error
        else:
            logger.error(
                f"MOCK HL place_order: Unexpected call {hl_place_call_num} with side {side}"
            )
            raise MockAPIError(
                f"Unexpected HL place_order call {hl_place_call_num} with side {side}"
            )

    mock_bp_api.place_order = AsyncMock(side_effect=place_order_bp_side_effect)
    mock_hl_api.place_order = AsyncMock(side_effect=place_order_hl_side_effect)

    # Mock `get_order_status`
    async def get_order_status_bp_side_effect(*args: Any, **kwargs: Any) -> Order | None:
        order_id = kwargs.get("order_id") or (args[1] if len(args) > 1 else None)
        logger.debug(f"MOCK BP get_order_status called for ID: {order_id}")
        if order_id == long_order_id_bp:
            logger.debug("MOCK BP get_order_status: Returning status for initial long order.")
            return bp_initial_order
        elif order_id == compensating_order_id_bp:
            logger.debug("MOCK BP get_order_status: Returning status for compensation order.")
            return bp_compensation_order
        else:
            logger.warning(f"MOCK BP get_order_status: Unknown order ID {order_id}")
            return None

    mock_bp_api.get_order_status = AsyncMock(side_effect=get_order_status_bp_side_effect)
    mock_hl_api.get_order_status = AsyncMock(return_value=None)

    # --- Execute Test ---
    # 1. Generate Signal
    opportunities = signal_generator.generate_opportunities()
    assert len(opportunities) >= 1
    opportunity = opportunities[0]
    assert opportunity.symbol == internal_symbol
    assert opportunity.long_exchange == "mock_bp"
    assert opportunity.short_exchange == "mock_hl"

    # 2. Validate & Size
    sized_opportunities = risk_manager.validate_opportunities([opportunity])
    assert len(sized_opportunities) == 1
    sized_opportunity = sized_opportunities[0]
    sized_opportunity.long_size = target_qty
    sized_opportunity.short_size = target_qty

    # 3. Execute
    logger.info("Executing failing opportunity (HL short fails)...")
    trade_execution_result: TradeExecution = await execution_handler.execute_opportunity(
        sized_opportunity
    )
    logger.info(f"Execution result: {trade_execution_result}")

    # 4. Verification
    assert trade_execution_result.status == ExecutionStatus.COMPLETED  # type: ignore
    assert trade_execution_result.long_order_id == long_order_id_bp
    assert trade_execution_result.short_order_id is None
    assert trade_execution_result.error_message is not None
    assert "Failed to place short order" in trade_execution_result.error_message
    assert "Simulated HL execution failure" in trade_execution_result.error_message

    # Check logs
    await asyncio.sleep(0.5)
    assert any(
        "Executing compensation for failed leg" in record.message for record in caplog.records
    )
    assert any(
        f"Attempting to reverse successful leg on mock_bp for order {long_order_id_bp}"
        in record.message
        for record in caplog.records
    )
    assert any(
        f"Successfully placed compensation order {compensating_order_id_bp}" in record.message
        for record in caplog.records
    )

    # 5. Verify Final Portfolio State
    logger.info("Verifying final portfolio state after execution failure compensation...")
    initial_long_order = portfolio_tracker.get_order("mock_bp", long_order_id_bp)
    compensation_order = portfolio_tracker.get_order("mock_bp", compensating_order_id_bp)
    failed_short_order = portfolio_tracker.get_order("mock_hl", short_order_id_hl)

    assert initial_long_order is not None
    assert initial_long_order.status == OrderStatus.FILLED
    assert compensation_order is not None
    assert compensation_order.status == OrderStatus.FILLED
    assert compensation_order.side == OrderSide.SELL
    assert failed_short_order is None

    # Check final positions
    bp_pos = portfolio_tracker.get_position("mock_bp", internal_symbol)
    hl_pos = portfolio_tracker.get_position("mock_hl", internal_symbol)

    assert bp_pos is None or bp_pos.size == Decimal("0"), (
        f"BP position should be flat after compensation, but is {bp_pos}"
    )
    assert hl_pos is None or hl_pos.size == Decimal("0"), (
        f"HL position should be flat (was never opened), but is {hl_pos}"
    )

    # Optional: Check balances
    final_bp_balance = portfolio_tracker.get_asset_balance("mock_bp", "USDC")
    taker_fee_rate_str = mock_config.get("exchanges.mock_bp.taker_fee", "0")
    taker_fee_rate = Decimal(str(taker_fee_rate_str))
    fee1 = (
        bp_initial_order.filled_quantity * bp_initial_order.avg_fill_price * taker_fee_rate
        if bp_initial_order.avg_fill_price
        else Decimal(0)
    )
    fee2 = (
        compensation_order.filled_quantity * compensation_order.avg_fill_price * taker_fee_rate
        if compensation_order.avg_fill_price
        else Decimal(0)
    )
    expected_final_bp_total = initial_bp_balance.total - fee1 - fee2
    assert final_bp_balance is not None
    assert final_bp_balance.total == pytest.approx(expected_final_bp_total, rel=Decimal("1e-6"))  # type: ignore

    logger.info("Execution failure compensation test completed successfully.")
