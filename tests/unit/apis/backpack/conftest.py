"""Shared fixtures for BackpackRequestBuilder tests."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce


@pytest.fixture
def symbol_spot() -> str:
    """Standard spot trading symbol."""
    return "SOL_USDC"


@pytest.fixture
def symbol_perp() -> str:
    """Standard perpetual trading symbol."""
    return "SOL-PERP"


@pytest.fixture
def symbol_btc_spot() -> str:
    """Bitcoin spot trading symbol."""
    return "BTC_USDT"


@pytest.fixture
def symbol_eth_spot() -> str:
    """Ethereum spot trading symbol."""
    return "ETH_USDC"


@pytest.fixture
def order_id() -> str:
    """Standard order ID."""
    return "987654321"


@pytest.fixture
def client_order_id() -> str:
    """Standard client order ID."""
    return "myOrder1"


@pytest.fixture
def buy_order_side() -> OrderSide:
    """Buy order side."""
    return OrderSide.BUY


@pytest.fixture
def sell_order_side() -> OrderSide:
    """Sell order side."""
    return OrderSide.SELL


@pytest.fixture
def limit_order_type() -> OrderType:
    """Limit order type."""
    return OrderType.LIMIT


@pytest.fixture
def market_order_type() -> OrderType:
    """Market order type."""
    return OrderType.MARKET


@pytest.fixture
def stop_market_order_type() -> OrderType:
    """Stop market order type."""
    return OrderType.STOP_MARKET


@pytest.fixture
def stop_limit_order_type() -> OrderType:
    """Stop limit order type."""
    return OrderType.STOP_LIMIT


@pytest.fixture
def gtc_time_in_force() -> TimeInForce:
    """Good Till Cancelled time in force."""
    return TimeInForce.GTC


@pytest.fixture
def ioc_time_in_force() -> TimeInForce:
    """Immediate or Cancel time in force."""
    return TimeInForce.IOC


@pytest.fixture
def standard_quantity() -> Decimal:
    """Standard order quantity."""
    return Decimal("10.5")


@pytest.fixture
def standard_price() -> Decimal:
    """Standard order price."""
    return Decimal("140.00")


@pytest.fixture
def trigger_price() -> Decimal:
    """Standard trigger price for stop orders."""
    return Decimal("28.00")


@pytest.fixture
def withdrawal_amount() -> Decimal:
    """Standard withdrawal amount."""
    return Decimal("100.0")


@pytest.fixture
def withdrawal_address() -> str:
    """Standard withdrawal address."""
    return "xyzAddress"


@pytest.fixture
def solana_network() -> str:
    """Solana network name."""
    return "Solana"


@pytest.fixture
def ethereum_network() -> str:
    """Ethereum network name."""
    return "Ethereum"


@pytest.fixture
def current_timestamp_ms() -> int:
    """Current timestamp in milliseconds."""
    return int(datetime.now(UTC).timestamp() * 1000)


@pytest.fixture
def past_timestamp_ms(current_timestamp_ms: int) -> int:
    """Past timestamp in milliseconds."""
    return current_timestamp_ms - 100000


@pytest.fixture
def usdc_asset() -> str:
    """USDC asset symbol."""
    return "USDC"


@pytest.fixture
def sol_asset() -> str:
    """SOL asset symbol."""
    return "SOL"


@pytest.fixture
def eth_asset() -> str:
    """ETH asset symbol."""
    return "ETH"
