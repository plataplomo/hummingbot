"""Shared fixtures and utilities for core unit tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, Mock
from uuid import uuid4

import pytest
from pydantic import AnyUrl, HttpUrl

from cyberdelta.config.models.exchange_config import (
    AddressActionSafetyNetConfig,
    ExchangeSpecificConfig,
)
from cyberdelta.core.enums import SignalType
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import (
    DerivativePosition,
    Fill,
    FundingRate,
    MarginAccountSummary,
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    SpotBalance,
    Ticker,
    TimeInForce,
    TradeSignal,
)
from cyberdelta.models.market.candle import Candle

# PortfolioStateManager import removed - replaced by modular portfolio system
from cyberdelta.symbols import Symbol, exchanges
from cyberdelta.symbols.service import SymbolService
from tests.common_symbols import BTC_BP, BTC_HL, ETH_BP, ETH_HL, SOL_BP, SOL_HL
from tests.fixtures.symbol_domain_fixtures import SymbolSet


# Common time fixtures
@pytest.fixture
def now() -> datetime:
    """Current UTC datetime for testing.

    Returns:
        datetime: Current UTC datetime.
    """
    return datetime.now(UTC)


@pytest.fixture
def mock_clock() -> Mock:
    """Mock clock for time-dependent tests.

    Returns:
        Mock: Mock clock object with now() method.
    """
    mock = Mock()
    mock.now.return_value = datetime.now(UTC)
    return mock


# Common configuration fixtures
@pytest.fixture
def mock_app_settings() -> MagicMock:
    """Mock application settings.

    Returns:
        MagicMock: Mock application settings object.
    """
    settings = MagicMock()
    settings.get.return_value = {}
    return settings


@pytest.fixture
def mock_config() -> MagicMock:
    """Mock configuration object.

    Returns:
        MagicMock: Mock configuration object.
    """
    config = MagicMock()
    config.get.return_value = {}
    return config


# Symbol mapper fixtures
@pytest.fixture
def mock_symbol_mapper() -> Mock:
    """Mock symbol mapper with standard mappings.

    Returns:
        Mock: A mock symbol mapper instance for testing.
    """
    mapper = Mock(spec=SymbolService)
    btc_symbol = BTC_HL
    mapper.get_exchange_symbol.return_value = btc_symbol.value
    mapper.get_internal_symbol.return_value = "BTC"
    mapper.get_all_internal_symbols.return_value = ["BTC", "ETH", "SOL"]
    mapper.get_exchange_symbols_for_internal.return_value = {
        "hyperliquid": btc_symbol.value,
        "backpack": BTC_BP.value,
    }
    return mapper


def create_test_exchange_config(
    exchange_name: ExchangeName, symbol_mapping: dict[str, str] | None = None, **overrides: object
) -> ExchangeSpecificConfig:
    """Create a test ExchangeSpecificConfig with minimal required fields.

    Args:
        exchange_name: The exchange name to configure.
        symbol_mapping: Optional symbol mappings.
        **overrides: Additional configuration overrides.

    Returns:
        ExchangeSpecificConfig: Test exchange configuration.

    Args:
        exchange_name: The exchange name enum
        symbol_mapping: Symbol mappings (defaults to BTC/ETH/SOL)
        **overrides: Additional fields to override

    Returns:
        ExchangeSpecificConfig: Properly configured exchange config
    """
    if symbol_mapping is None:
        symbol_mapping = {
            "BTC": BTC_HL.value,
            "ETH": ETH_HL.value,
            "SOL": SOL_HL.value,
        }

    base_config: dict[str, Any] = {
        "exchange_name": exchange_name,
        "enabled": True,
        "api_base_url_mainnet": HttpUrl("https://api.example.com"),
        "ws_url_mainnet": AnyUrl("wss://ws.example.com"),
        "symbols": symbol_mapping,
    }

    # Add exchange-specific required fields
    if exchange_name == ExchangeName.HYPERLIQUID:
        base_config.update({
            "chain_id": 1337,
            "ip_weight_limit_per_minute": 1200,
            "info_request_type_ip_weights": {"meta": 1, "allMids": 2},
            "default_info_weight": 1,
            "exchange_action_base_ip_weight": 1,
            "address_action_safety_net": AddressActionSafetyNetConfig(rate_per_minute=600),
        })
    elif exchange_name == ExchangeName.BACKPACK:
        base_config.update({
            "rate_limit_per_minute": 120,
        })

    # Apply overrides
    base_config.update(overrides)

    return ExchangeSpecificConfig.model_validate(base_config)


@pytest.fixture
def hyperliquid_exchange_config() -> ExchangeSpecificConfig:
    """Standard Hyperliquid exchange configuration.

    Returns:
        ExchangeSpecificConfig: Standard Hyperliquid exchange configuration.
    """
    return create_test_exchange_config(ExchangeName.HYPERLIQUID)


@pytest.fixture
def backpack_exchange_config() -> ExchangeSpecificConfig:
    """Standard Backpack exchange configuration.

    Returns:
        ExchangeSpecificConfig: Standard Backpack exchange configuration.
    """
    return create_test_exchange_config(ExchangeName.BACKPACK)


@pytest.fixture
def symbol_mapper_config() -> dict[str, ExchangeSpecificConfig]:
    """Standard symbol mapper configuration with typed exchanges.

    Returns:
        dict[str, ExchangeSpecificConfig]: Mapping of exchange names to their configurations.
    """
    return {
        "hyperliquid": create_test_exchange_config(ExchangeName.HYPERLIQUID),
        "backpack": create_test_exchange_config(ExchangeName.BACKPACK),
    }

    # Portfolio tracker fixtures disabled - replaced by modular portfolio system


# Sample data fixtures
@pytest.fixture
def sample_order(btc_symbols: SymbolSet) -> Order:
    """Standard test order.

    Returns:
        Order: Test order with standard BTC-PERP buy configuration.
    """
    btc_symbol = btc_symbols.perp_hl
    return Order(
        client_order_id=str(uuid4()),
        exchange=ExchangeName.HYPERLIQUID,
        symbol=btc_symbol,
        side=OrderSide.BUY,
        order_type=OrderType.LIMIT,
        quantity_requested=Decimal("1.0"),
        price=Decimal("50000.0"),
        status=OrderStatus.NEW,
        time_in_force=TimeInForce.GTC,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )


@pytest.fixture
def sample_spot_balance() -> SpotBalance:
    """Standard test spot balance.

    Returns:
        SpotBalance: Test USDC balance with 10k total, 8k available.
    """
    return SpotBalance(
        asset=exchanges.hyperliquid("USD"),
        total_quantity=Decimal("10000.0"),
        available_quantity=Decimal("8000.0"),
        exchange=ExchangeName.HYPERLIQUID,
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_derivative_position(btc_symbols: SymbolSet) -> DerivativePosition:
    """Standard test derivative position.

    Returns:
        DerivativePosition: Test BTC-PERP long position with unrealized PnL.
    """
    btc_symbol = btc_symbols.perp_hl
    return DerivativePosition(
        symbol=btc_symbol,
        side=OrderSide.BUY,
        size=Decimal("1.0"),
        entry_price=Decimal("50000.0"),
        mark_price=Decimal("51000.0"),
        unrealized_pnl=Decimal("1000.0"),
        realized_pnl=Decimal("0.0"),
        exchange=ExchangeName.HYPERLIQUID,
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_fill(btc_symbols: SymbolSet) -> Fill:
    """Standard test fill.

    Returns:
        Fill: Test BTC-PERP buy fill at $50k.
    """
    btc_symbol = btc_symbols.perp_hl
    return Fill(
        id=str(uuid4()),
        symbol=btc_symbol,
        side=OrderSide.BUY,
        quantity=Decimal("1.0"),
        price=Decimal("50000.0"),
        order_id=str(uuid4()),
        executed_at=datetime.now(UTC),
        exchange=ExchangeName.HYPERLIQUID,
    )


@pytest.fixture
def sample_ticker(btc_symbols: SymbolSet) -> Ticker:
    """Standard test ticker.

    Returns:
        Ticker: Test BTC-PERP ticker with bid/ask spread around $50k.
    """
    btc_symbol = btc_symbols.perp_hl
    return Ticker(
        symbol=btc_symbol,
        exchange=ExchangeName.HYPERLIQUID,
        bid=Decimal("49950.0"),
        ask=Decimal("50050.0"),
        price=Decimal("50000.0"),
        volume=Decimal("1000.0"),
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_order_book(btc_symbols: SymbolSet) -> OrderBook:
    """Standard test order book.

    Returns:
        OrderBook: Test BTC-PERP order book with bid/ask levels.
    """
    btc_symbol = btc_symbols.perp_hl
    return OrderBook(
        symbol=btc_symbol,
        bids=[(Decimal("49950.0"), Decimal("10.0"))],
        asks=[(Decimal("50050.0"), Decimal("10.0"))],
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_funding_rate(btc_symbols: SymbolSet) -> FundingRate:
    """Standard test funding rate.

    Returns:
        FundingRate: Test BTC-PERP funding rate of 0.01% with 8h next funding.
    """
    btc_symbol = btc_symbols.perp_hl
    return FundingRate(
        symbol=btc_symbol,
        funding_rate=Decimal("0.0001"),
        next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_candle(btc_symbols: SymbolSet) -> Candle:
    """Standard test candle.

    Returns:
        Candle: Test BTC-PERP 1m candle from $50k to $50.5k.
    """
    btc_symbol = btc_symbols.perp_hl
    now_time = datetime.now(UTC)
    return Candle(
        symbol=btc_symbol,
        interval="1m",
        open_time=now_time,
        open=Decimal("50000.0"),
        high=Decimal("51000.0"),
        low=Decimal("49000.0"),
        close=Decimal("50500.0"),
        volume=Decimal("1000.0"),
    )


@pytest.fixture
def sample_trade_signal(btc_symbols: SymbolSet) -> TradeSignal:
    """Standard test trade signal.

    Returns:
        TradeSignal: Test BTC-PERP enter long signal at $50k with 80% confidence.
    """
    btc_symbol = btc_symbols.perp_hl
    return TradeSignal(
        symbol=btc_symbol,
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000.0"),
        exchange=ExchangeName.HYPERLIQUID,
        confidence=0.8,
        metadata={"strategy": "test_strategy"},
    )


@pytest.fixture
def sample_margin_summary() -> MarginAccountSummary:
    """Standard test margin account summary.

    Returns:
        MarginAccountSummary: Test margin account with $100k total, $90k available equity.
    """
    return MarginAccountSummary(
        exchange=ExchangeName.HYPERLIQUID,
        timestamp=datetime.now(UTC),
        total_equity=Decimal("100000.0"),
        available_equity=Decimal("90000.0"),
    )


# Type aliases for cleaner test signatures
ExchangeBalances = dict[str, dict[str, SpotBalance]]
ExchangeOrders = dict[str, dict[str, Order]]
ExchangePositions = dict[str, dict[str, DerivativePosition]]


# Collection fixtures for tests that need multiple objects
@pytest.fixture(params=["default", "large", "minimal"])
def sample_orders(request: pytest.FixtureRequest) -> dict[str, dict[str, Order]]:
    """Sample orders organized by exchange - parametrized for different scenarios.

    Returns:
        dict[str, dict[str, Order]]: Orders grouped by exchange and order ID for test scenarios.
    """
    scenario = str(request.param)
    return create_sample_orders(scenario)


@pytest.fixture(params=["default", "large", "minimal"])
def sample_balances_state(request: pytest.FixtureRequest) -> dict[str, dict[str, SpotBalance]]:
    """Sample balances organized by exchange - parametrized for different scenarios.

    Returns:
        Balances grouped by exchange and asset for test scenarios.
    """
    scenario = str(request.param)
    return create_sample_balances(scenario)


# Legacy name for backward compatibility
sample_balances_parametrized = sample_balances_state


# Test data scenarios as fixtures that can be used with indirect parametrization
@pytest.fixture
def balance_scenario(request: pytest.FixtureRequest) -> dict[str, dict[str, SpotBalance]]:
    """Fixture that creates balance data based on indirect parametrization.

    Returns:
        dict[str, dict[str, SpotBalance]]: Balance data structured by exchange and asset.
    """
    scenario = getattr(request, "param", "default")
    return create_sample_balances(scenario)


@pytest.fixture
def order_scenario(request: pytest.FixtureRequest) -> dict[str, dict[str, Order]]:
    """Fixture that creates order data based on indirect parametrization.

    Returns:
        dict[str, dict[str, Order]]: Order data structured by exchange and order ID.
    """
    scenario = getattr(request, "param", "default")
    return create_sample_orders(scenario)


@pytest.fixture
def position_scenario(request: pytest.FixtureRequest) -> dict[str, dict[str, DerivativePosition]]:
    """Fixture that creates position data based on indirect parametrization.

    Returns:
        dict[str, dict[str, DerivativePosition]]: Position data structured by exchange and symbol.
    """
    scenario = getattr(request, "param", "default")
    return create_sample_positions(scenario)


@pytest.fixture
def sample_balances_large() -> dict[str, dict[str, SpotBalance]]:
    """Sample balances organized by exchange - large capital scenario.

    Returns:
        dict[str, dict[str, SpotBalance]]: Large capital balances for portfolio stress testing.
    """
    return create_sample_balances("large")


@pytest.fixture
def sample_balances_minimal() -> dict[str, dict[str, SpotBalance]]:
    """Sample balances organized by exchange - minimal amounts scenario.

    Returns:
        dict[str, dict[str, SpotBalance]]: Minimal balances for edge case testing.
    """
    return create_sample_balances("minimal")


@pytest.fixture(params=["default", "large", "minimal"])
def sample_positions(request: pytest.FixtureRequest) -> dict[str, dict[str, DerivativePosition]]:
    """Sample positions organized by exchange - parametrized for different scenarios.

    Returns:
        Positions grouped by exchange and symbol for test scenarios.
    """
    scenario = str(request.param)
    return create_sample_positions(scenario)


# Non-parametrized versions for specific test cases
@pytest.fixture
def sample_orders_default() -> dict[str, dict[str, Order]]:
    """Sample orders organized by exchange - default scenario only.

    Returns:
        dict[str, dict[str, Order]]: Default test orders without parametrization.
    """
    return create_sample_orders("default")


@pytest.fixture
def sample_positions_default() -> dict[str, dict[str, DerivativePosition]]:
    """Sample positions organized by exchange - default scenario only.

    Returns:
        dict[str, dict[str, DerivativePosition]]: Default test positions without parametrization.
    """
    return create_sample_positions("default")


# Common exchange names for parametrized tests
@pytest.fixture(params=["hyperliquid", "backpack"])
def exchange_name(request: pytest.FixtureRequest) -> str:
    """Parametrized exchange names.

    Returns:
        str: Exchange name for parametrized testing across supported exchanges.
    """
    return str(request.param)


@pytest.fixture(params=[OrderSide.BUY, OrderSide.SELL])
def order_side(request: pytest.FixtureRequest) -> OrderSide:
    """Parametrized order sides.

    Returns:
        OrderSide: Order side (BUY or SELL) for parametrized testing.
    """
    return OrderSide(request.param)


@pytest.fixture(params=["BTC", "ETH", "SOL"])
def symbol(request: pytest.FixtureRequest) -> str:
    """Parametrized symbol names.

    Returns:
        str: Symbol name for parametrized testing across major cryptocurrencies.
    """
    return str(request.param)


# Utility functions
def create_test_order(
    exchange: str = "hyperliquid",
    symbol: Symbol | None = None,
    side: OrderSide = OrderSide.BUY,
    quantity: Decimal = Decimal("1.0"),
    price: Decimal = Decimal("50000.0"),
    status: OrderStatus = OrderStatus.NEW,
) -> Order:
    """Create a test order with customizable parameters.

    Returns:
        Order: Test order with specified or default parameters.
    """
    if symbol is None:
        symbol = BTC_HL
    return Order(
        client_order_id=str(uuid4()),
        exchange=ExchangeName.HYPERLIQUID if exchange == "hyperliquid" else ExchangeName.BACKPACK,
        symbol=symbol,
        side=side,
        order_type=OrderType.LIMIT,
        quantity_requested=quantity,
        price=price,
        status=status,
        time_in_force=TimeInForce.GTC,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        triggered_at=None,
        strategy_name=None,
        signal_id=None,
    )


def create_test_balance(
    exchange: str = "hyperliquid",
    asset: str = "USDC",
    total_quantity: Decimal = Decimal("10000.0"),
    available_quantity: Decimal | None = None,
) -> SpotBalance:
    """Create a test balance with customizable parameters.

    Returns:
        SpotBalance: Test balance with specified or default parameters.
    """
    if available_quantity is None:
        available_quantity = total_quantity * Decimal("0.8")

    return SpotBalance(
        asset=exchanges.hyperliquid(asset) if asset != "USDC" else exchanges.hyperliquid("USD"),
        total_quantity=total_quantity,
        available_quantity=available_quantity,
        exchange=ExchangeName.HYPERLIQUID if exchange == "hyperliquid" else ExchangeName.BACKPACK,
        timestamp=datetime.now(UTC),
    )


def create_test_signal(
    symbol: Symbol | None = None,
    signal_type: SignalType = SignalType.ENTER_LONG,
    side: OrderSide = OrderSide.BUY,
    price: Decimal = Decimal("50000.0"),
    exchange: str = "hyperliquid",
    confidence: float = 0.8,
    metadata: dict[str, Any] | None = None,
) -> TradeSignal:
    """Create a test signal with customizable parameters.

    Returns:
        TradeSignal: Test trade signal with specified or default parameters.
    """
    if symbol is None:
        symbol = BTC_HL
    if metadata is None:
        metadata = {"strategy": "test_strategy"}

    return TradeSignal(
        symbol=symbol,
        signal_type=signal_type,
        side=side,
        price=price,
        exchange=ExchangeName.HYPERLIQUID if exchange == "hyperliquid" else ExchangeName.BACKPACK,
        confidence=confidence,
        metadata=metadata,
    )


def create_sample_balances(scenario: str = "default") -> dict[str, dict[str, SpotBalance]]:
    """Create sample balances for different test scenarios.

    Args:
        scenario: The balance scenario to create
            - "default": Standard test amounts
            - "large": Large capital amounts for portfolio tests
            - "minimal": Small amounts for edge case testing

    Returns:
        Nested dictionary of balances organized by exchange and asset.

    Raises:
        ValueError: If scenario is not one of the supported scenarios.
    """
    scenarios = {
        "default": {
            "hyperliquid": {
                "USDC": (Decimal("10000.0"), Decimal("8000.0")),
                "BTC": (Decimal("0.2"), Decimal("0.15")),
            },
            "backpack": {
                "USDC": (Decimal("5000.0"), Decimal("4500.0")),
                "ETH": (Decimal("2.0"), Decimal("1.8")),
            },
        },
        "large": {
            "hyperliquid": {
                "USDC": (Decimal("100000.0"), Decimal("90000.0")),
                "ETH": (Decimal("5.0"), Decimal("4.5")),
            },
            "backpack": {
                "USDC": (Decimal("5000.0"), Decimal("4500.0")),
                "BTC": (Decimal("0.1"), Decimal("0.08")),
            },
        },
        "minimal": {
            "hyperliquid": {
                "USDC": (Decimal("100.0"), Decimal("80.0")),
                "BTC": (Decimal("0.001"), Decimal("0.0008")),
            },
            "backpack": {
                "USDC": (Decimal("50.0"), Decimal("45.0")),
                "ETH": (Decimal("0.01"), Decimal("0.008")),
            },
        },
    }

    if scenario not in scenarios:
        raise ValueError(f"Unknown scenario: {scenario}. Available: {list(scenarios.keys())}")

    scenario_data = scenarios[scenario]
    result: dict[str, dict[str, SpotBalance]] = {}

    for exchange_id, assets in scenario_data.items():
        result[exchange_id] = {}
        for asset, (total_qty, available_qty) in assets.items():
            result[exchange_id][asset] = SpotBalance(
                asset=exchanges.hyperliquid("USD")
                if asset == "USDC"
                else exchanges.hyperliquid(asset)
                if exchange_id == "hyperliquid"
                else exchanges.backpack(asset),
                total_quantity=total_qty,
                available_quantity=available_qty,
                exchange=ExchangeName.HYPERLIQUID
                if exchange_id == "hyperliquid"
                else ExchangeName.BACKPACK,
                timestamp=datetime.now(UTC),
            )

    return result


def create_sample_orders(scenario: str = "default") -> dict[str, dict[str, Order]]:
    """Create sample orders for different test scenarios.

    Returns:
        dict[str, dict[str, Order]]: Nested dictionary of orders organized by exchange and order ID.
    """
    base_time = datetime.now(UTC)

    scenarios = {
        "default": {
            "hyperliquid": [
                (
                    "hl-order-1",
                    BTC_HL.value,
                    OrderSide.BUY,
                    Decimal("1.0"),
                    Decimal("50000.0"),
                    OrderStatus.NEW,
                ),
                (
                    "hl-order-2",
                    ETH_HL.value,
                    OrderSide.SELL,
                    Decimal("5.0"),
                    Decimal("3000.0"),
                    OrderStatus.NEW,
                ),
            ],
            "backpack": [
                (
                    "bp-order-1",
                    SOL_BP.value,
                    OrderSide.BUY,
                    Decimal("10.0"),
                    Decimal("100.0"),
                    OrderStatus.PARTIALLY_FILLED,
                ),
            ],
        },
        "large": {
            "hyperliquid": [
                (
                    "hl-order-1",
                    BTC_HL.value,
                    OrderSide.BUY,
                    Decimal("1.0"),
                    Decimal("50000.0"),
                    OrderStatus.NEW,
                ),
                (
                    "hl-order-2",
                    ETH_HL.value,
                    OrderSide.SELL,
                    Decimal("5.0"),
                    Decimal("3000.0"),
                    OrderStatus.NEW,
                ),
                (
                    "hl-order-3",
                    SOL_HL.value,
                    OrderSide.BUY,
                    Decimal("100.0"),
                    Decimal("100.0"),
                    OrderStatus.FILLED,
                ),
            ],
            "backpack": [
                (
                    "bp-order-1",
                    SOL_BP.value,
                    OrderSide.BUY,
                    Decimal("10.0"),
                    Decimal("100.0"),
                    OrderStatus.PARTIALLY_FILLED,
                ),
                (
                    "bp-order-2",
                    BTC_BP.value,
                    OrderSide.SELL,
                    Decimal("0.5"),
                    Decimal("51000.0"),
                    OrderStatus.NEW,
                ),
            ],
        },
        "minimal": {
            "hyperliquid": [
                (
                    "hl-order-1",
                    BTC_HL.value,
                    OrderSide.BUY,
                    Decimal("0.001"),
                    Decimal("50000.0"),
                    OrderStatus.NEW,
                ),
            ],
            "backpack": [],
        },
    }

    scenario_data = scenarios.get(scenario, scenarios["default"])
    result: dict[str, dict[str, Order]] = {}

    for exchange_id, orders_data in scenario_data.items():
        result[exchange_id] = {}
        for order_id, symbol, side, qty, price, status in orders_data:
            result[exchange_id][order_id] = Order(
                client_order_id=order_id,
                exchange=ExchangeName.HYPERLIQUID
                if exchange_id == "hyperliquid"
                else ExchangeName.BACKPACK,
                symbol=exchanges.hyperliquid(symbol)
                if exchange_id == "hyperliquid"
                else exchanges.backpack(symbol),
                side=side,
                order_type=OrderType.LIMIT,
                quantity_requested=qty,
                price=price,
                status=status,
                time_in_force=TimeInForce.GTC,
                created_at=base_time,
                updated_at=base_time,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

    return result


def create_sample_positions(scenario: str = "default") -> dict[str, dict[str, DerivativePosition]]:
    """Create sample positions for different test scenarios.

    Returns:
        Nested dictionary of positions organized by exchange and symbol.
    """
    base_time = datetime.now(UTC)

    scenarios = {
        "default": {
            "hyperliquid": [
                (
                    BTC_HL.value,
                    OrderSide.BUY,
                    Decimal("1.0"),
                    Decimal("50000.0"),
                    Decimal("51000.0"),
                    Decimal("1000.0"),
                ),
                (
                    ETH_HL.value,
                    OrderSide.SELL,
                    Decimal("-2.0"),
                    Decimal("3000.0"),
                    Decimal("2950.0"),
                    Decimal("100.0"),
                ),
            ],
            "backpack": [
                (
                    SOL_BP.value,
                    OrderSide.BUY,
                    Decimal("10.0"),
                    Decimal("100.0"),
                    Decimal("105.0"),
                    Decimal("50.0"),
                ),
            ],
        },
        "large": {
            "hyperliquid": [
                (
                    BTC_HL.value,
                    OrderSide.BUY,
                    Decimal("5.0"),
                    Decimal("50000.0"),
                    Decimal("51000.0"),
                    Decimal("5000.0"),
                ),
                (
                    ETH_HL.value,
                    OrderSide.SELL,
                    Decimal("-10.0"),
                    Decimal("3000.0"),
                    Decimal("2950.0"),
                    Decimal("500.0"),
                ),
                (
                    SOL_HL.value,
                    OrderSide.BUY,
                    Decimal("100.0"),
                    Decimal("100.0"),
                    Decimal("105.0"),
                    Decimal("500.0"),
                ),
            ],
            "backpack": [
                (
                    BTC_BP.value,
                    OrderSide.BUY,
                    Decimal("2.0"),
                    Decimal("49000.0"),
                    Decimal("51000.0"),
                    Decimal("4000.0"),
                ),
                (
                    ETH_BP.value,
                    OrderSide.SELL,
                    Decimal("-5.0"),
                    Decimal("3100.0"),
                    Decimal("2950.0"),
                    Decimal("750.0"),
                ),
            ],
        },
        "minimal": {
            "hyperliquid": [
                (
                    BTC_HL.value,
                    OrderSide.BUY,
                    Decimal("0.001"),
                    Decimal("50000.0"),
                    Decimal("51000.0"),
                    Decimal("1.0"),
                ),
            ],
            "backpack": [],
        },
    }

    scenario_data = scenarios.get(scenario, scenarios["default"])
    result: dict[str, dict[str, DerivativePosition]] = {}

    for exchange_id, positions_data in scenario_data.items():
        result[exchange_id] = {}
        for symbol, side, size, entry_price, mark_price, unrealized_pnl in positions_data:
            result[exchange_id][symbol] = DerivativePosition(
                symbol=exchanges.hyperliquid(symbol)
                if exchange_id == "hyperliquid"
                else exchanges.backpack(symbol),
                side=side,
                size=size,
                entry_price=entry_price,
                mark_price=mark_price,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=Decimal("0.0"),
                exchange=ExchangeName.HYPERLIQUID
                if exchange_id == "hyperliquid"
                else ExchangeName.BACKPACK,
                timestamp=base_time,
            )

    return result


def populate_nested_dict(target_dict: Any, source_dict: dict[str, dict[str, Any]]) -> None:  # noqa: ANN401
    """Helper to populate nested defaultdict structures from regular dicts."""
    for exchange_id, nested_items in source_dict.items():
        for item_key, item_value in nested_items.items():
            target_dict[exchange_id][item_key] = item_value
