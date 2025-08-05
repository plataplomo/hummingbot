"""Shared fixtures for BackpackRequestBuilder tests."""

from collections.abc import Callable, Generator
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from tests.common_symbols import BTC_USDT_BP, ETH_USDT_BP, SOL_USDC_BP, SOL_USDC_PERP_BP, USDC_BP


@pytest.fixture
def symbol_spot() -> Symbol:
    """Return standard spot trading symbol.

    Returns:
        Symbol: The standard spot trading symbol.
    """
    return SOL_USDC_BP


@pytest.fixture
def symbol_perp() -> Symbol:
    """Return standard perpetual trading symbol.

    Returns:
        Symbol: The standard perpetual trading symbol.
    """
    return SOL_USDC_PERP_BP


@pytest.fixture
def symbol_btc_spot() -> Symbol:
    """Bitcoin spot trading symbol.

    Returns:
        Symbol: The Bitcoin spot trading symbol.
    """
    return BTC_USDT_BP


@pytest.fixture
def symbol_eth_spot() -> Symbol:
    """Ethereum spot trading symbol.

    Returns:
        Symbol: The Ethereum spot trading symbol.
    """
    return ETH_USDT_BP


@pytest.fixture
def order_id() -> str:
    """Return a standard order ID for testing.

    Returns:
        str: A standard order ID for testing.
    """
    return "987654321"


@pytest.fixture
def client_order_id() -> str:
    """Return a standard client order ID for testing.

    Returns:
        str: A standard client order ID for testing.
    """
    return "myOrder1"


@pytest.fixture
def buy_order_side() -> OrderSide:
    """Buy order side.

    Returns:
        OrderSide: The buy order side enum value.
    """
    return OrderSide.BUY


@pytest.fixture
def sell_order_side() -> OrderSide:
    """Sell order side.

    Returns:
        OrderSide: The sell order side enum value.
    """
    return OrderSide.SELL


@pytest.fixture
def limit_order_type() -> OrderType:
    """Limit order type.

    Returns:
        OrderType: The limit order type enum value.
    """
    return OrderType.LIMIT


@pytest.fixture
def market_order_type() -> OrderType:
    """Market order type.

    Returns:
        OrderType: The market order type enum value.
    """
    return OrderType.MARKET


@pytest.fixture
def stop_market_order_type() -> OrderType:
    """Stop market order type.

    Returns:
        OrderType: The stop market order type enum value.
    """
    return OrderType.STOP_MARKET


@pytest.fixture
def stop_limit_order_type() -> OrderType:
    """Stop limit order type.

    Returns:
        OrderType: The stop limit order type enum value.
    """
    return OrderType.STOP_LIMIT


@pytest.fixture
def gtc_time_in_force() -> TimeInForce:
    """Good Till Cancelled time in force.

    Returns:
        TimeInForce: The GTC time in force enum value.
    """
    return TimeInForce.GTC


@pytest.fixture
def ioc_time_in_force() -> TimeInForce:
    """Immediate or Cancel time in force.

    Returns:
        TimeInForce: The IOC time in force enum value.
    """
    return TimeInForce.IOC


@pytest.fixture
def standard_quantity() -> Decimal:
    """Return a standard order quantity for testing.

    Returns:
        Decimal: A standard order quantity for testing.
    """
    return Decimal("10.5")


@pytest.fixture
def standard_price() -> Decimal:
    """Return standard order price for testing.

    Returns:
        Decimal: A standard order price for testing.
    """
    return Decimal("140.00")


@pytest.fixture
def trigger_price() -> Decimal:
    """Return standard trigger price for stop orders.

    Returns:
        Decimal: A standard trigger price for stop orders.
    """
    return Decimal("28.00")


@pytest.fixture
def withdrawal_amount() -> Decimal:
    """Return standard withdrawal amount for testing.

    Returns:
        Decimal: A standard withdrawal amount for testing.
    """
    return Decimal("100.0")


@pytest.fixture
def withdrawal_address() -> str:
    """Return standard withdrawal address for testing.

    Returns:
        str: A standard withdrawal address for testing.
    """
    return "xyzAddress"


@pytest.fixture
def solana_network() -> str:
    """Solana network name.

    Returns:
        str: The Solana network name.
    """
    return "Solana"


@pytest.fixture
def ethereum_network() -> str:
    """Ethereum network name.

    Returns:
        str: The Ethereum network name.
    """
    return "Ethereum"


@pytest.fixture
def current_timestamp_ms() -> int:
    """Return current timestamp in milliseconds.

    Returns:
        int: Current timestamp in milliseconds.
    """
    return int(datetime.now(UTC).timestamp() * 1000)


@pytest.fixture
def past_timestamp_ms(current_timestamp_ms: int) -> int:
    """Past timestamp in milliseconds.

    Returns:
        int: Past timestamp in milliseconds.
    """
    return current_timestamp_ms - 100000


@pytest.fixture
def usdc_asset() -> Symbol:
    """USDC asset symbol.

    Returns:
        Symbol: The USDC asset symbol.
    """
    return USDC_BP


@pytest.fixture
def sol_asset() -> Symbol:
    """SOL asset symbol.

    Returns:
        Symbol: The SOL asset symbol.
    """
    return exchanges.backpack("SOL")


@pytest.fixture
def eth_asset() -> Symbol:
    """ETH asset symbol.

    Returns:
        Symbol: The ETH asset symbol.
    """
    return exchanges.backpack("ETH")


# Removed hardcoded active_bp_config fixture - now using centralized fixture from tests/conftest.py


def _create_mock_account_service() -> MagicMock:
    """Create a mock account service with async methods.

    Returns:
        MagicMock: A mock account service with mocked async methods.
    """
    mock_service = MagicMock()
    mock_service.get_balances = AsyncMock()
    mock_service.get_account_info = AsyncMock()
    mock_service.get_positions = AsyncMock()
    mock_service.get_order_history = AsyncMock()
    mock_service.get_trade_history = AsyncMock()
    mock_service.transfer = AsyncMock()
    mock_service.withdraw = AsyncMock()
    return mock_service


def _create_mock_market_data_service() -> MagicMock:
    """Create a mock market data service with async methods.

    Returns:
        MagicMock: A mock market data service with mocked async methods.
    """
    mock_service = MagicMock()
    mock_service.get_ticker = AsyncMock()
    mock_service.get_order_book = AsyncMock()
    mock_service.get_recent_trades = AsyncMock()
    mock_service.get_funding_rate = AsyncMock()
    mock_service.get_funding_rates = AsyncMock()
    mock_service.get_market_data = AsyncMock()
    mock_service.get_historical_funding_rates = AsyncMock()
    return mock_service


def _create_mock_trading_service() -> MagicMock:
    """Create a mock trading service with async methods.

    Returns:
        MagicMock: A mock trading service with mocked async methods.
    """
    mock_service = MagicMock()
    mock_service.place_order = AsyncMock()
    mock_service.cancel_order = AsyncMock()
    mock_service.cancel_all_orders = AsyncMock()
    mock_service.get_open_orders = AsyncMock()
    mock_service.get_order = AsyncMock()
    mock_service.get_order_status = AsyncMock()
    mock_service.get_all_open_orders = AsyncMock()
    return mock_service


@pytest.fixture(autouse=True)
def patch_http_websocket_for_tests() -> Generator[None]:
    """Auto-use fixture to patch HTTP client and WebSocket manager for all tests in this module."""
    with (
        patch("cyberdelta.apis.connectivity.http_client.HttpClient") as mock_http_class,
        patch("cyberdelta.apis.connectivity.ws_manager.WebSocketManager") as mock_ws_class,
        patch("aiohttp.ClientSession") as mock_session_class,
    ):
        # Configure mock HTTP client
        mock_http_client = MagicMock()
        mock_http_client.close = AsyncMock()
        mock_http_client.request = AsyncMock()
        mock_http_class.return_value = mock_http_client

        # Configure mock aiohttp session
        mock_session = MagicMock()
        mock_session.close = AsyncMock()
        mock_session.request = AsyncMock()
        mock_session_class.return_value = mock_session

        # Configure mock WebSocket manager
        mock_ws_manager = MagicMock()
        mock_ws_manager.close = AsyncMock()
        mock_ws_class.return_value = mock_ws_manager

        yield


@pytest.fixture
def bp_api_with_di(
    active_bp_config: ExchangeSpecificConfig,
    active_bp_secrets: ApiKeyAuthSecrets,
) -> Generator[Callable[..., BackpackAPI]]:
    """Create BackpackAPI instances with all dependencies mocked.

    This enables unit testing without accessing protected members.

    Yields:
        Callable[..., BackpackAPI]: Factory function for creating BackpackAPI instances with mocks.
    """
    created_apis: list[BackpackAPI] = []

    def _create_api(
        config: ExchangeSpecificConfig | None = None,
        secrets: ApiKeyAuthSecrets | None = None,
        **overrides: MagicMock,
    ) -> BackpackAPI:
        """Create BackpackAPI with mocked dependencies.

        Returns:
            BackpackAPI: Configured BackpackAPI instance with mocked dependencies.
        """
        final_config = config or active_bp_config
        final_secrets = secrets or active_bp_secrets

        # Create BackpackAPI - HTTP client and WebSocket manager are patched globally
        api = BackpackAPI(exchange_config=final_config, exchange_secrets=final_secrets)
        created_apis.append(api)

        # Set up account service
        api.account_service = overrides.get("account_service", _create_mock_account_service())

        # Set up market data service
        api.market_data_service = overrides.get(
            "market_data_service", _create_mock_market_data_service()
        )

        # Set up trading service
        api.trading_service = overrides.get("trading_service", _create_mock_trading_service())

        return api

    yield _create_api

    # Cleanup: Since we're using mocked HTTP clients and WS managers,
    # no explicit cleanup is needed as mocks handle their own cleanup
    created_apis.clear()
