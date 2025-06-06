"""Shared fixtures for BackpackRequestBuilder tests."""

from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce


@pytest.fixture
def symbol_spot() -> str:
    """Return standard spot trading symbol."""
    return "SOL_USDC"


@pytest.fixture
def symbol_perp() -> str:
    """Return standard perpetual trading symbol."""
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
    """Return a standard order ID for testing."""
    return "987654321"


@pytest.fixture
def client_order_id() -> str:
    """Return a standard client order ID for testing."""
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
    """Return a standard order quantity for testing."""
    return Decimal("10.5")


@pytest.fixture
def standard_price() -> Decimal:
    """Return standard order price for testing."""
    return Decimal("140.00")


@pytest.fixture
def trigger_price() -> Decimal:
    """Return standard trigger price for stop orders."""
    return Decimal("28.00")


@pytest.fixture
def withdrawal_amount() -> Decimal:
    """Return standard withdrawal amount for testing."""
    return Decimal("100.0")


@pytest.fixture
def withdrawal_address() -> str:
    """Return standard withdrawal address for testing."""
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
    """Return current timestamp in milliseconds."""
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


# Removed hardcoded active_bp_config fixture - now using centralized fixture from tests/conftest.py


@pytest.fixture
def bp_api_with_di(
    active_bp_config: ExchangeSpecificConfig,
    active_bp_secrets: ApiKeyAuthSecrets,
) -> Callable[..., BackpackAPI]:
    """Create BackpackAPI instances with all dependencies mocked.

    This enables unit testing without accessing protected members.
    """

    def _create_api(
        config: ExchangeSpecificConfig | None = None,
        secrets: ApiKeyAuthSecrets | None = None,
        **overrides: MagicMock,
    ) -> BackpackAPI:
        """Create BackpackAPI with mocked dependencies."""
        final_config = config or active_bp_config

        # Use active secrets as default if not provided
        if secrets is None:
            secrets = active_bp_secrets

        # Create BackpackAPI with standard configuration
        api = BackpackAPI(exchange_config=final_config, exchange_secrets=secrets)

        # Replace services with mocks
        if "account_service" in overrides:
            api.account_service = overrides["account_service"]
        else:
            api.account_service = MagicMock()
            api.account_service.get_balances = AsyncMock()
            api.account_service.get_account_info = AsyncMock()
            api.account_service.get_positions = AsyncMock()
            api.account_service.get_order_history = AsyncMock()
            api.account_service.get_trade_history = AsyncMock()
            api.account_service.transfer = AsyncMock()
            api.account_service.withdraw = AsyncMock()

        if "market_data_service" in overrides:
            api.market_data_service = overrides["market_data_service"]
        else:
            api.market_data_service = MagicMock()
            api.market_data_service.get_ticker = AsyncMock()
            api.market_data_service.get_order_book = AsyncMock()
            api.market_data_service.get_recent_trades = AsyncMock()
            api.market_data_service.get_funding_rate = AsyncMock()
            api.market_data_service.get_funding_rates = AsyncMock()
            api.market_data_service.get_market_data = AsyncMock()
            api.market_data_service.get_historical_funding_rates = AsyncMock()
            # Note: These are being set for test purposes
            # Consider using dependency injection in the actual service
            # to avoid accessing protected members in tests

        if "trading_service" in overrides:
            api.trading_service = overrides["trading_service"]
        else:
            api.trading_service = MagicMock()
            api.trading_service.place_order = AsyncMock()
            api.trading_service.cancel_order = AsyncMock()
            api.trading_service.cancel_all_orders = AsyncMock()
            api.trading_service.get_open_orders = AsyncMock()
            api.trading_service.get_order = AsyncMock()
            api.trading_service.get_order_status = AsyncMock()
            api.trading_service.get_all_open_orders = AsyncMock()
            # Note: Setting protected member for test purposes
            # Consider refactoring to use dependency injection

        return api

    return _create_api
