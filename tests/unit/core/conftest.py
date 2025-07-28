"""Shared fixtures and utilities for core unit tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, Mock
from uuid import uuid4

import pytest
from pydantic import AnyUrl, HttpUrl

from cyberdelta.config.models.config_models import (
    AddressActionSafetyNetConfig,
    ExchangeSpecificConfig,
    PortfolioStateSettings,
    PortfolioTrackerConfig,
    PortfolioValidationSettings,
)
from cyberdelta.core.enums import SignalType
from cyberdelta.core.models import (
    DerivativePosition,
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
    Trade,
    TradeSignal,
)
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.enums.exchange_names import ExchangeName


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


@pytest.fixture
def pt_config() -> PortfolioTrackerConfig:
    """Standard portfolio tracker configuration.

    Returns:
        PortfolioTrackerConfig: Standard portfolio tracker configuration.
    """
    return PortfolioTrackerConfig(
        data_freshness_seconds=60,
        # Ensure validation timeout <= state update timeout to satisfy business logic
        validation=PortfolioValidationSettings(
            validation_timeout=5.0,  # Set to 5.0 to be <= state.update_timeout default (5.0)
        ),
        state=PortfolioStateSettings(
            update_timeout=10.0,  # Increase to 10.0 to provide buffer above validation_timeout
        ),
    )


# Symbol mapper fixtures
@pytest.fixture
def mock_symbol_mapper() -> Mock:
    """Mock symbol mapper with standard mappings.
    
    Returns:
        Mock: A mock symbol mapper instance for testing.
    """
    mapper = Mock(spec=SymbolService)
    mapper.get_exchange_symbol.return_value = "BTC-PERP"
    mapper.get_internal_symbol.return_value = "BTC"
    mapper.get_all_internal_symbols.return_value = ["BTC", "ETH", "SOL"]
    mapper.get_exchange_symbols_for_internal.return_value = {
        "hyperliquid": "BTC-PERP",
        "backpack": "BTC-PERP",
    }
    return mapper


def create_test_exchange_config(
    exchange_name: ExchangeName, symbols: dict[str, str] | None = None, **overrides: object
) -> ExchangeSpecificConfig:
    """Create a test ExchangeSpecificConfig with minimal required fields.

    Args:
        exchange_name: The exchange name to configure.
        symbols: Optional symbol mappings.
        **overrides: Additional configuration overrides.

    Returns:
        ExchangeSpecificConfig: Test exchange configuration.

    Args:
        exchange_name: The exchange name enum
        symbols: Symbol mappings (defaults to BTC/ETH/SOL)
        **overrides: Additional fields to override

    Returns:
        ExchangeSpecificConfig: Properly configured exchange config
    """
    if symbols is None:
        symbols = {
            "BTC": "BTC-PERP",
            "ETH": "ETH-PERP",
            "SOL": "SOL-PERP",
        }

    base_config: dict[str, Any] = {
        "exchange_name": exchange_name,
        "enabled": True,
        "api_base_url_mainnet": HttpUrl("https://api.example.com"),
        "ws_url_mainnet": AnyUrl("wss://ws.example.com"),
        "symbols": symbols,
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


# Portfolio tracker fixtures
@pytest.fixture
def mock_portfolio_tracker() -> Mock:
    """Mock portfolio tracker with standard methods.

    Returns:
        Mock: Mock portfolio tracker with configured method return values.
    """
    tracker = Mock(spec=PortfolioTracker)
    tracker.get_open_orders.return_value = []
    tracker.get_order_history.return_value = []
    tracker.get_all_positions.return_value = []
    tracker.get_exchange_balance.return_value = None
    tracker.get_position.return_value = None
    tracker.get_order_by_id.return_value = None
    return tracker


# Sample data fixtures
@pytest.fixture
def sample_order() -> Order:
    """Standard test order.

    Returns:
        Order: Test order with standard BTC-PERP buy configuration.
    """
    return Order(
        client_order_id=str(uuid4()),
        exchange="hyperliquid",
        symbol="BTC-PERP",
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
        asset="USDC",
        total_quantity=Decimal("10000.0"),
        available_quantity=Decimal("8000.0"),
        exchange="hyperliquid",
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_derivative_position() -> DerivativePosition:
    """Standard test derivative position.

    Returns:
        DerivativePosition: Test BTC-PERP long position with unrealized PnL.
    """
    return DerivativePosition(
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        size=Decimal("1.0"),
        entry_price=Decimal("50000.0"),
        mark_price=Decimal("51000.0"),
        unrealized_pnl=Decimal("1000.0"),
        realized_pnl=Decimal("0.0"),
        exchange="hyperliquid",
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_trade() -> Trade:
    """Standard test trade.

    Returns:
        Trade: Test BTC-PERP buy trade at $50k.
    """
    return Trade(
        id=str(uuid4()),
        symbol="BTC-PERP",
        side=OrderSide.BUY,
        quantity=Decimal("1.0"),
        price=Decimal("50000.0"),
        order_id=str(uuid4()),
        executed_at=datetime.now(UTC),
        exchange="hyperliquid",
    )


@pytest.fixture
def sample_ticker() -> Ticker:
    """Standard test ticker.

    Returns:
        Ticker: Test BTC-PERP ticker with bid/ask spread around $50k.
    """
    return Ticker(
        symbol="BTC-PERP",
        exchange="test_exchange",
        bid=Decimal("49950.0"),
        ask=Decimal("50050.0"),
        price=Decimal("50000.0"),
        volume=Decimal("1000.0"),
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_order_book() -> OrderBook:
    """Standard test order book.

    Returns:
        OrderBook: Test BTC-PERP order book with bid/ask levels.
    """
    return OrderBook(
        symbol="BTC-PERP",
        bids=[(Decimal("49950.0"), Decimal("10.0"))],
        asks=[(Decimal("50050.0"), Decimal("10.0"))],
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_funding_rate() -> FundingRate:
    """Standard test funding rate.

    Returns:
        FundingRate: Test BTC-PERP funding rate of 0.01% with 8h next funding.
    """
    return FundingRate(
        symbol="BTC-PERP",
        funding_rate=Decimal("0.0001"),
        next_funding_time=datetime.now(UTC) + timedelta(hours=8),
        timestamp=datetime.now(UTC),
    )


@pytest.fixture
def sample_candle() -> Candle:
    """Standard test candle.

    Returns:
        Candle: Test BTC-PERP 1m candle from $50k to $50.5k.
    """
    now_time = datetime.now(UTC)
    return Candle(
        symbol="BTC-PERP",
        interval="1m",
        open_time=now_time,
        open=Decimal("50000.0"),
        high=Decimal("51000.0"),
        low=Decimal("49000.0"),
        close=Decimal("50500.0"),
        volume=Decimal("1000.0"),
    )


@pytest.fixture
def sample_trade_signal() -> TradeSignal:
    """Standard test trade signal.

    Returns:
        TradeSignal: Test BTC-PERP enter long signal at $50k with 80% confidence.
    """
    return TradeSignal(
        symbol="BTC-PERP",
        signal_type=SignalType.ENTER_LONG,
        side=OrderSide.BUY,
        price=Decimal("50000.0"),
        exchange="hyperliquid",
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
        exchange="hyperliquid",
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
    symbol: str = "BTC-PERP",
    side: OrderSide = OrderSide.BUY,
    quantity: Decimal = Decimal("1.0"),
    price: Decimal = Decimal("50000.0"),
    status: OrderStatus = OrderStatus.NEW,
) -> Order:
    """Create a test order with customizable parameters.

    Returns:
        Order: Test order with specified or default parameters.
    """
    return Order(
        client_order_id=str(uuid4()),
        exchange=exchange,
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
        asset=asset,
        total_quantity=total_quantity,
        available_quantity=available_quantity,
        exchange=exchange,
        timestamp=datetime.now(UTC),
    )


def create_test_signal(
    symbol: str = "BTC-PERP",
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
    if metadata is None:
        metadata = {"strategy": "test_strategy"}

    return TradeSignal(
        symbol=symbol,
        signal_type=signal_type,
        side=side,
        price=price,
        exchange=exchange,
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
                asset=asset,
                total_quantity=total_qty,
                available_quantity=available_qty,
                exchange=exchange_id,
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
                    "BTC-PERP",
                    OrderSide.BUY,
                    Decimal("1.0"),
                    Decimal("50000.0"),
                    OrderStatus.NEW,
                ),
                (
                    "hl-order-2",
                    "ETH-PERP",
                    OrderSide.SELL,
                    Decimal("5.0"),
                    Decimal("3000.0"),
                    OrderStatus.NEW,
                ),
            ],
            "backpack": [
                (
                    "bp-order-1",
                    "SOL-PERP",
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
                    "BTC-PERP",
                    OrderSide.BUY,
                    Decimal("1.0"),
                    Decimal("50000.0"),
                    OrderStatus.NEW,
                ),
                (
                    "hl-order-2",
                    "ETH-PERP",
                    OrderSide.SELL,
                    Decimal("5.0"),
                    Decimal("3000.0"),
                    OrderStatus.NEW,
                ),
                (
                    "hl-order-3",
                    "SOL-PERP",
                    OrderSide.BUY,
                    Decimal("100.0"),
                    Decimal("100.0"),
                    OrderStatus.FILLED,
                ),
            ],
            "backpack": [
                (
                    "bp-order-1",
                    "SOL-PERP",
                    OrderSide.BUY,
                    Decimal("10.0"),
                    Decimal("100.0"),
                    OrderStatus.PARTIALLY_FILLED,
                ),
                (
                    "bp-order-2",
                    "BTC-PERP",
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
                    "BTC-PERP",
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
                exchange=exchange_id,
                symbol=symbol,
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
                    "BTC",
                    OrderSide.BUY,
                    Decimal("1.0"),
                    Decimal("50000.0"),
                    Decimal("51000.0"),
                    Decimal("1000.0"),
                ),
                (
                    "ETH",
                    OrderSide.SELL,
                    Decimal("-2.0"),
                    Decimal("3000.0"),
                    Decimal("2950.0"),
                    Decimal("100.0"),
                ),
            ],
            "backpack": [
                (
                    "SOL",
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
                    "BTC",
                    OrderSide.BUY,
                    Decimal("5.0"),
                    Decimal("50000.0"),
                    Decimal("51000.0"),
                    Decimal("5000.0"),
                ),
                (
                    "ETH",
                    OrderSide.SELL,
                    Decimal("-10.0"),
                    Decimal("3000.0"),
                    Decimal("2950.0"),
                    Decimal("500.0"),
                ),
                (
                    "SOL",
                    OrderSide.BUY,
                    Decimal("100.0"),
                    Decimal("100.0"),
                    Decimal("105.0"),
                    Decimal("500.0"),
                ),
            ],
            "backpack": [
                (
                    "BTC",
                    OrderSide.BUY,
                    Decimal("2.0"),
                    Decimal("49000.0"),
                    Decimal("51000.0"),
                    Decimal("4000.0"),
                ),
                (
                    "ETH",
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
                    "BTC",
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
                symbol=symbol,
                side=side,
                size=size,
                entry_price=entry_price,
                mark_price=mark_price,
                unrealized_pnl=unrealized_pnl,
                realized_pnl=Decimal("0.0"),
                exchange=exchange_id,
                timestamp=base_time,
            )

    return result


def populate_nested_dict(target_dict: Any, source_dict: dict[str, dict[str, Any]]) -> None:  # noqa: ANN401
    """Helper to populate nested defaultdict structures from regular dicts."""
    for exchange_id, nested_items in source_dict.items():
        for item_key, item_value in nested_items.items():
            target_dict[exchange_id][item_key] = item_value
