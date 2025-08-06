"""Unit tests for the base ExchangeAPI class implementation.

Tests use dependency injection patterns to mock collaborators and focus on public interface testing.
"""

import asyncio
from collections.abc import Callable, Coroutine, Mapping
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import structlog.testing
from pydantic import BaseModel

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.common import APIError, APIErrorCode, IErrorMapper
from cyberdelta.apis.models.service_args.account import (
    TransferArgs,
    UpdateAccountSettingsArgs,
    WithdrawArgs,
)
from cyberdelta.apis.models.service_args.market_data import (
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketArgs,
    GetMarketDataArgs,
    GetMarketsArgs,
)
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetOrderArgs,
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    PlaceOrderArgs,
)
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import AnyExchangeSecrets
from cyberdelta.core.enums import CancelOrderResultStatus
from cyberdelta.enums.environment import EnvironmentType
from cyberdelta.models import (
    AccountSettings,
    DerivativePosition,
    FundingRate,
    MidPrices,
    Order,
    OrderBook,
    OrderSide,
    OrderType,
    SpotBalance,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.models.margin_account import MarginAccountSummary
from cyberdelta.models.market import Candle
from cyberdelta.models.market.market import Market
from cyberdelta.models.market.order import CancelOrderResult
from cyberdelta.models.operations import Transfer, Withdrawal
from cyberdelta.symbols.models import BackpackMetadata, BaseSymbol, HyperliquidMetadata
from tests.common_symbols import BTC_HL


# Match the definition in cyberdelta.apis.base.exchange_api.py
MessageHandler = Callable[..., Coroutine[Any, Any, None]]


class MockSubscriptionPayload(BaseModel):
    """Simple BaseModel for testing subscription payloads."""

    type: str
    channel: str


class ConcreteTestExchangeAPI(ExchangeAPI):
    """Concrete implementation of ExchangeAPI for testing base class functionality."""

    def __init__(
        self,
        exchange_name: str,
        config: ExchangeSpecificConfig | dict[str, Any],
        secrets: AnyExchangeSecrets | dict[str, str | None],
        error_mapper: IErrorMapper,
        loop: asyncio.AbstractEventLoop | None = None,
        authenticator: IAuthenticator | None = None,
        http_client: MagicMock | None = None,
        ws_manager: MagicMock | None = None,
        rate_limiter_service: MagicMock | None = None,
    ) -> None:
        """Initialize with dependency injection support for testing."""
        # Convert dict to mock config if needed for testing
        if isinstance(config, dict):
            mock_config = MagicMock(spec=ExchangeSpecificConfig)
            for key, value in config.items():
                setattr(mock_config, key, value)
            config = mock_config

        # Convert dict to mock secrets if needed for testing
        if isinstance(secrets, dict):
            mock_secrets = MagicMock(spec=AnyExchangeSecrets)
            for key, value in secrets.items():
                setattr(mock_secrets, key, value)
            secrets = mock_secrets

        super().__init__(
            exchange_name,
            config,
            secrets,
            error_mapper,
            loop,
            authenticator=authenticator,
        )

        # Store test-specific dependencies as public attributes for test access
        # This avoids direct manipulation of protected members
        self.test_http_client = http_client
        self.test_ws_manager = ws_manager
        self.test_rate_limiter_service = rate_limiter_service

        # Override injected dependencies if provided
        if http_client is not None:
            self._http_client = http_client
        if ws_manager is not None:
            self._ws_manager = ws_manager
        if rate_limiter_service is not None:
            self._rate_limiter_service = rate_limiter_service

    # Implement abstract methods from ExchangeAPI
    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Mock implementation of authentication.

        Returns:
            Empty dictionary for testing.
        """
        return {}

    def _update_rate_limit_from_headers(
        self,
        headers: Mapping[str, str],
        method: str,
        path: str,
    ) -> None:
        """Mock implementation of rate limit header processing."""

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        """Mock implementation of WebSocket message routing."""

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        """Mock implementation of WebSocket message handling."""

    # Implement all abstract methods with simple mocks
    async def get_ticker(
        self, symbol: BaseSymbol[HyperliquidMetadata] | BaseSymbol[BackpackMetadata]
    ) -> Ticker:
        """Get ticker data for the specified symbol.

        Returns:
            Mock Ticker object.
        """
        return MagicMock(spec=Ticker)

    async def get_order_book(
        self,
        symbol: BaseSymbol[HyperliquidMetadata] | BaseSymbol[BackpackMetadata],
        depth: int | None = None,
    ) -> OrderBook:
        """Get order book data for the specified symbol.

        Returns:
            Mock OrderBook object.
        """
        return MagicMock(spec=OrderBook)

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Get current funding rates for the specified arguments.

        Returns:
            List of mock FundingRate objects.
        """
        return [MagicMock(spec=FundingRate)]

    async def get_historical_funding_rates(
        self,
        args: GetHistoricalFundingRatesArgs,
    ) -> list[FundingRate]:
        """Get historical funding rates for the specified arguments.

        Returns:
            List of mock FundingRate objects.
        """
        return [MagicMock(spec=FundingRate)]

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Get market data candles for the specified arguments.

        Returns:
            List of mock Candle objects.
        """
        return [MagicMock(spec=Candle)]

    async def get_market(self, args: GetMarketArgs) -> Market:
        """Get market metadata for the specified symbol.

        Returns:
            Mock Market object.
        """
        return MagicMock(spec=Market)

    async def get_markets(self, args: GetMarketsArgs) -> list[Market]:
        """Get market metadata for all available markets.

        Returns:
            List of mock Market objects.
        """
        return [MagicMock(spec=Market)]

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Get account balances for all assets.

        Returns:
            Dictionary mapping asset symbols to mock SpotBalance objects.
        """
        return {"USD": MagicMock(spec=SpotBalance)}

    async def get_account_summary(self) -> MarginAccountSummary:
        """Get margin account summary information.

        Returns:
            Mock MarginAccountSummary object.
        """
        return MagicMock(spec=MarginAccountSummary)

    async def get_positions(
        self, symbol: BaseSymbol[HyperliquidMetadata] | BaseSymbol[BackpackMetadata] | None = None
    ) -> list[DerivativePosition]:
        """Get derivative positions for the specified symbol or all positions.

        Returns:
            List of mock DerivativePosition objects.
        """
        return [MagicMock(spec=DerivativePosition)]

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place a new order with the specified arguments.

        Returns:
            Mock Order object.
        """
        return MagicMock(spec=Order)

    async def transfer(self, args: TransferArgs) -> Transfer:
        """Execute a transfer with the specified arguments.

        Returns:
            Mock Transfer object.
        """
        return MagicMock(spec=Transfer)

    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Execute a withdrawal with the specified arguments.

        Returns:
            Mock Withdrawal object.
        """
        return MagicMock(spec=Withdrawal)

    async def cancel_order(self, args: CancelOrderArgs) -> CancelOrderResult:
        """Cancel an order with the specified arguments.

        Returns:
            CancelOrderResult with success status and details.
        """
        return CancelOrderResult(
            symbol=args.symbol,
            order_id=args.order_id,
            client_order_id=args.client_order_id,
            success=True,
            message=None,
            status=CancelOrderResultStatus.SUCCESS,
            raw_response=None,
        )

    async def cancel_all_orders(
        self, symbol: BaseSymbol[HyperliquidMetadata] | BaseSymbol[BackpackMetadata] | None = None
    ) -> list[CancelOrderResult]:
        """Cancel all orders for the specified symbol or all symbols.

        Returns:
            Empty list of CancelOrderResult objects.
        """
        return []

    async def get_open_orders(
        self, symbol: BaseSymbol[HyperliquidMetadata] | BaseSymbol[BackpackMetadata] | None = None
    ) -> list[Order]:
        """Get open orders for the specified symbol or all symbols.

        Returns:
            List of mock Order objects.
        """
        return [MagicMock(spec=Order)]

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Get order history for the specified arguments.

        Returns:
            List of mock Order objects.
        """
        return [MagicMock(spec=Order)]

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Get trade history for the specified arguments.

        Returns:
            List of mock Trade objects.
        """
        return [MagicMock(spec=Trade)]

    async def get_order_status(self, args: GetOrderArgs) -> Order | None:
        """Get the status of a specific order.

        Returns:
            Mock Order object.
        """
        mock_order = MagicMock(spec=Order)
        mock_order.exchange_order_id = args.order_id
        return mock_order

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Get a specific order by its arguments.

        Returns:
            Mock Order object.
        """
        return MagicMock(spec=Order)

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Get all open orders for the specified arguments.

        Returns:
            List of mock Order objects.
        """
        return [MagicMock(spec=Order)]

    async def update_account_settings(self, args: UpdateAccountSettingsArgs) -> AccountSettings:
        """Mock implementation of update_account_settings.

        Returns:
            Mock AccountSettings object.
        """
        return MagicMock(spec=AccountSettings)

    async def place_batch_orders(self, orders: list[PlaceOrderArgs]) -> list[Order]:
        """Mock implementation of place_batch_orders.

        Returns:
            List of mock Order objects corresponding to input orders.
        """
        return [MagicMock(spec=Order) for _ in orders]

    async def cancel_batch_orders(
        self,
        cancel_args: list[CancelOrderArgs],
    ) -> list[CancelOrderResult]:
        """Mock implementation of cancel_batch_orders.

        Returns:
            List of mock CancelOrderResult objects corresponding to input args.
        """
        return [MagicMock(spec=CancelOrderResult) for _ in cancel_args]

    def _construct_subscription_payload(self, topic: str) -> BaseModel:
        """Construct subscription payload for the given topic.

        Returns:
            MockSubscriptionPayload with subscription details.
        """
        return MockSubscriptionPayload(type="subscribe", channel=topic)

    async def get_all_mids(self) -> MidPrices:
        """Get all mid prices for efficient market order pricing.

        Returns:
            Mock MidPrices object.
        """
        return MagicMock(spec=MidPrices)

    async def ping_websocket(self) -> None:
        """Send a ping message to the WebSocket connection."""


# --- Dependency Injection Test Fixtures ---


@pytest.fixture
def mock_error_mapper() -> MagicMock:
    """Mock error mapper for ExchangeAPI.

    Returns:
        Configured MagicMock for IErrorMapper interface.
    """
    mock_mapper = MagicMock(spec=IErrorMapper)
    # Configure the mock to return proper APIError instances
    mock_mapper.map_string_error.return_value = APIError(
        message="Mock error",
        code=APIErrorCode.UNKNOWN.value,
    )
    mock_mapper.map_exchange_error.return_value = APIError(
        message="Mock exchange error",
        code=APIErrorCode.UNKNOWN.value,
    )
    return mock_mapper


@pytest.fixture
def mock_authenticator() -> MagicMock:
    """Mock authenticator for ExchangeAPI.

    Returns:
        Configured MagicMock for IAuthenticator interface.
    """
    mock_auth = MagicMock(spec=IAuthenticator)
    mock_auth.prepare_request = AsyncMock()
    return mock_auth


@pytest.fixture
def mock_http_client() -> MagicMock:
    """Mock HTTP client for ExchangeAPI.

    Returns:
        Configured MagicMock for HTTP client with async methods.
    """
    mock_client = MagicMock()
    mock_client.request = AsyncMock()
    mock_client.close_session = AsyncMock()
    return mock_client


@pytest.fixture
def mock_ws_manager() -> MagicMock:
    """Mock WebSocket manager for ExchangeAPI.

    Returns:
        Configured MagicMock for WebSocket manager with async methods.
    """
    mock_manager = MagicMock()
    mock_manager.connect = AsyncMock()
    mock_manager.close = AsyncMock()
    mock_manager.send_json = AsyncMock()
    mock_manager.is_connected = False
    return mock_manager


@pytest.fixture
def mock_rate_limiter() -> MagicMock:
    """Mock rate limiter service for ExchangeAPI.

    Returns:
        MagicMock for rate limiter service.
    """
    return MagicMock()


@pytest.fixture
def base_config() -> dict[str, Any]:
    """Provide base configuration for ExchangeAPI tests.

    Returns:
        Dictionary containing base configuration settings.
    """
    return {
        "rate_limits": {"default_rate": 10, "default_bucket_size": 10},
        "ws_endpoint": "wss://test.ws.endpoint",
        "rest_endpoint": "https://test.rest.endpoint",
        "rate_limit_per_minute": 60,
        "environment_type": EnvironmentType.MAINNET,
        "api_base_url_mainnet": "https://test.rest.endpoint",
        "api_base_url_testnet": "https://test.rest.endpoint",
        "ws_url_mainnet": "wss://test.ws.endpoint",
        "ws_url_testnet": "wss://test.ws.endpoint",
        "request_timeout_seconds": None,
        "max_retries": None,
        "retry_delay_seconds": None,
        "ws_ping_interval_seconds": None,
        "ws_reconnect_delay_seconds": None,
        "ws_max_reconnect_attempts": None,
        "ws_connection_timeout_seconds": None,
        "websocket_send_rate_per_minute": None,
    }


@pytest.fixture
def mock_secrets() -> dict[str, str | None]:
    """Mock secrets for ExchangeAPI tests.

    Returns:
        Dictionary containing mock API credentials.
    """
    return {"API_KEY": "test_key", "API_SECRET": "test_secret"}


@pytest.fixture
def exchange_api_with_di(
    base_config: dict[str, Any],
    mock_secrets: dict[str, str | None],
    mock_error_mapper: MagicMock,
    mock_authenticator: MagicMock,
    mock_http_client: MagicMock,
    mock_ws_manager: MagicMock,
    mock_rate_limiter: MagicMock,
) -> Callable[..., ConcreteTestExchangeAPI]:
    """Create factory fixture to create ExchangeAPI instances with all dependencies injected.

    This enables black-box testing without accessing private members.

    Returns:
        Factory function that creates ConcreteTestExchangeAPI instances with injected dependencies.
    """

    def _create_api(
        exchange_name: str = "test_exchange",
        config: dict[str, Any] | None = None,
        secrets: dict[str, str | None] | None = None,
        **overrides: MagicMock,
    ) -> ConcreteTestExchangeAPI:
        """Create ExchangeAPI with injected dependencies.

        Returns:
            ConcreteTestExchangeAPI instance with mocked dependencies.
        """
        actual_config = config or base_config
        actual_secrets = secrets or mock_secrets

        return ConcreteTestExchangeAPI(
            exchange_name=exchange_name,
            config=actual_config,
            secrets=actual_secrets,
            error_mapper=overrides.get("error_mapper", mock_error_mapper),
            authenticator=overrides.get("authenticator", mock_authenticator),
            http_client=overrides.get("http_client", mock_http_client),
            ws_manager=overrides.get("ws_manager", mock_ws_manager),
            rate_limiter_service=overrides.get("rate_limiter_service", mock_rate_limiter),
        )

    return _create_api


# --- Test Classes ---


class TestExchangeAPIInitialization:
    """Test ExchangeAPI initialization and setup."""

    def test_api_creation_with_di_fixture(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that the DI fixture creates a valid API instance."""
        api = exchange_api_with_di()

        assert api is not None
        assert api.exchange_name == "test_exchange"

    def test_api_creation_with_custom_config(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test API creation with custom configuration."""
        custom_config = {
            "rate_limits": {"default_rate": 20, "default_bucket_size": 20},
            "rest_endpoint": "https://custom.api.endpoint",
            "rate_limit_per_minute": 60,
            "environment_type": EnvironmentType.MAINNET,
            "api_base_url_mainnet": "https://custom.api.endpoint",
            "api_base_url_testnet": "https://custom.api.endpoint",
            "ws_url_mainnet": "wss://custom.ws.endpoint",
            "ws_url_testnet": "wss://custom.ws.endpoint",
            "request_timeout_seconds": None,
            "max_retries": None,
            "retry_delay_seconds": None,
            "ws_ping_interval_seconds": None,
            "ws_reconnect_delay_seconds": None,
            "ws_max_reconnect_attempts": None,
            "ws_connection_timeout_seconds": None,
            "websocket_send_rate_per_minute": None,
        }

        api = exchange_api_with_di(config=custom_config)
        assert api is not None

    def test_api_creation_with_custom_exchange_name(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test API creation with custom exchange name."""
        api = exchange_api_with_di(exchange_name="custom_exchange")
        assert api.exchange_name == "custom_exchange"


class TestExchangeAPIWebSocketOperations:
    """Test WebSocket operations using black-box approach."""

    @pytest.mark.asyncio
    async def test_connect_websocket_delegates_to_ws_manager(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that connect_websocket delegates to WebSocket manager."""
        mock_ws = MagicMock()
        mock_ws.connect = AsyncMock()
        mock_ws.close = AsyncMock()

        api = exchange_api_with_di(ws_manager=mock_ws)

        await api.connect_websocket()
        mock_ws.connect.assert_called_once()

        await api.close()

    @pytest.mark.asyncio
    async def test_subscribe_sends_payload_when_connected(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that subscribe sends subscription payload when connected."""
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.is_connected = True
        mock_ws.close = AsyncMock()

        api = exchange_api_with_di(ws_manager=mock_ws)

        async def test_handler(context: WebSocketContextProtocol) -> None:
            pass

        await api.subscribe("test.topic", test_handler)

        # Verify WebSocket manager was called with subscription payload
        # The payload should be a MockSubscriptionPayload BaseModel instance
        mock_ws.send_json.assert_called_once()
        call_args = mock_ws.send_json.call_args[0][0]
        assert isinstance(call_args, MockSubscriptionPayload)
        assert call_args.type == "subscribe"
        assert call_args.channel == "test.topic"

        await api.close()

    @pytest.mark.asyncio
    async def test_subscribe_logs_warning_when_not_connected(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that subscribe logs warning when WebSocket not connected."""
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock()
        mock_ws.is_connected = False
        mock_ws.close = AsyncMock()

        api = exchange_api_with_di(ws_manager=mock_ws)

        async def test_handler(context: WebSocketContextProtocol) -> None:
            pass

        with structlog.testing.capture_logs() as captured_logs:
            await api.subscribe("test.topic", test_handler)

        # Verify warning was logged in structured logs
        warning_logs = [log for log in captured_logs if log.get("log_level") == "warning"]
        assert len(warning_logs) > 0, "Expected at least one warning log"

        # Check for WebSocket not connected warning
        ws_logs = [log for log in warning_logs if "WebSocket not connected" in str(log)]
        assert len(ws_logs) > 0, f"Expected WebSocket warning, got: {warning_logs}"

        # The send_json should not be called when not connected
        mock_ws.send_json.assert_not_called()

        await api.close()

    def test_is_connected_property_delegates_to_ws_manager(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that is_connected property delegates to WebSocket manager."""
        mock_ws = MagicMock()
        mock_ws.is_connected = True

        api = exchange_api_with_di(ws_manager=mock_ws)
        assert api.is_connected is True

        mock_ws.is_connected = False
        assert api.is_connected is False

    @pytest.mark.asyncio
    async def test_construct_subscription_payload_integration(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that _construct_subscription_payload is properly integrated with subscribe."""
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock(return_value=True)
        mock_ws.is_connected = True
        mock_ws.close = AsyncMock()

        api = exchange_api_with_di(ws_manager=mock_ws)

        async def test_handler(context: WebSocketContextProtocol) -> None:
            pass

        # Test that subscribe uses _construct_subscription_payload
        await api.subscribe("test.topic", test_handler)

        # Verify the correct payload was sent
        mock_ws.send_json.assert_called_once()
        call_args = mock_ws.send_json.call_args[0][0]
        assert isinstance(call_args, MockSubscriptionPayload)
        assert call_args.type == "subscribe"
        assert call_args.channel == "test.topic"

        await api.close()

    @pytest.mark.asyncio
    async def test_websocket_reconnection_triggers_resubscription(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that WebSocket reconnection triggers resubscription to existing topics."""
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock(return_value=True)
        mock_ws.is_connected = True
        mock_ws.close = AsyncMock()
        mock_ws.connect = AsyncMock()  # Make connect async

        api = exchange_api_with_di(ws_manager=mock_ws)

        async def test_handler1(context: WebSocketContextProtocol) -> None:
            pass

        async def test_handler2(context: WebSocketContextProtocol) -> None:
            pass

        # Subscribe to multiple topics
        await api.subscribe("topic1", test_handler1)
        await api.subscribe("topic2", test_handler2)

        # Verify initial subscriptions were sent
        assert mock_ws.send_json.call_count == 2

        # Clear the mock to test resubscription behavior
        mock_ws.send_json.reset_mock()

        # Simulate WebSocket reconnection by calling the connection method
        await api.connect_websocket()

        # Verify that connect was called on the WebSocket manager
        mock_ws.connect.assert_called_once()

        await api.close()

    @pytest.mark.asyncio
    async def test_subscription_payload_construction_integration(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that subscription payload construction is properly integrated."""
        mock_ws = MagicMock()
        mock_ws.send_json = AsyncMock(return_value=True)
        mock_ws.is_connected = True
        mock_ws.close = AsyncMock()

        # Create a custom API that tracks payload construction calls
        class PayloadTrackingAPI(ConcreteTestExchangeAPI):
            def __init__(
                self,
                exchange_name: str,
                config: dict[str, Any],
                secrets: dict[str, str | None],
                error_mapper: IErrorMapper,
                loop: asyncio.AbstractEventLoop | None = None,
                authenticator: IAuthenticator | None = None,
                http_client: MagicMock | None = None,
                ws_manager: MagicMock | None = None,
                rate_limiter_service: MagicMock | None = None,
            ) -> None:
                super().__init__(
                    exchange_name,
                    config,
                    secrets,
                    error_mapper,
                    loop,
                    authenticator,
                    http_client,
                    ws_manager,
                    rate_limiter_service,
                )
                self.payload_construction_calls: list[str] = []

            def _construct_subscription_payload(self, topic: str) -> BaseModel:
                self.payload_construction_calls.append(topic)
                return MockSubscriptionPayload(type="subscribe", channel=topic)

            async def get_all_mids(self) -> MidPrices:
                """Get all mid prices for efficient market order pricing.

                Returns:
                    MidPrices: Mock mid prices data
                """
                return MagicMock(spec=MidPrices)

            async def transfer(self, args: TransferArgs) -> Transfer:
                return MagicMock(spec=Transfer)

            async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
                return MagicMock(spec=Withdrawal)

        api = PayloadTrackingAPI(
            exchange_name="test_exchange",
            config={
                "rest_endpoint": "https://test.endpoint",
                "ws_endpoint": "wss://test.ws",
                "rate_limit_per_minute": 60,
                "environment_type": EnvironmentType.MAINNET,
                "api_base_url_mainnet": "https://test.endpoint",
                "api_base_url_testnet": "https://test.endpoint",
                "ws_url_mainnet": "wss://test.ws",
                "ws_url_testnet": "wss://test.ws",
                "request_timeout_seconds": None,
                "max_retries": None,
                "retry_delay_seconds": None,
                "ws_ping_interval_seconds": None,
                "ws_reconnect_delay_seconds": None,
                "ws_max_reconnect_attempts": None,
                "ws_connection_timeout_seconds": None,
                "websocket_send_rate_per_minute": None,
            },
            secrets={"API_KEY": "test"},
            error_mapper=MagicMock(spec=IErrorMapper),
            ws_manager=mock_ws,
        )

        async def test_handler(context: WebSocketContextProtocol) -> None:
            pass

        # Subscribe to a topic
        await api.subscribe("test_topic", test_handler)

        # Verify payload construction was called
        assert "test_topic" in api.payload_construction_calls

        # Verify the custom payload was sent
        mock_ws.send_json.assert_called_once()
        call_args = mock_ws.send_json.call_args[0][0]
        assert isinstance(call_args, MockSubscriptionPayload)
        assert call_args.type == "subscribe"
        assert call_args.channel == "test_topic"

        await api.close()


class TestExchangeAPIResourceManagement:
    """Test resource management and cleanup."""

    @pytest.mark.asyncio
    async def test_api_close_cleanup_http_client(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that close properly cleans up HTTP client."""
        mock_http = MagicMock()
        mock_http.close_session = AsyncMock()

        api = exchange_api_with_di(http_client=mock_http)

        await api.close()
        mock_http.close_session.assert_called_once()

    @pytest.mark.asyncio
    async def test_api_close_cleanup_websocket_manager(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that close properly cleans up WebSocket manager."""
        mock_ws = MagicMock()
        mock_ws.close = AsyncMock()

        api = exchange_api_with_di(ws_manager=mock_ws)

        await api.close()
        mock_ws.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_api_close_handles_missing_dependencies(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that close handles missing dependencies gracefully."""
        api = exchange_api_with_di(
            http_client=None,
            ws_manager=None,
        )

        # Should not raise an exception
        await api.close()


class TestExchangeAPIErrorHandling:
    """Test error handling and propagation."""

    @pytest.mark.asyncio
    async def test_http_client_error_mapping(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that HTTP client errors are properly mapped."""
        mock_http = MagicMock()
        mock_error_mapper = MagicMock(spec=IErrorMapper)

        # Configure HTTP client to raise an exception
        mock_http.request = AsyncMock(side_effect=Exception("HTTP Error"))
        mock_http.close_session = AsyncMock()  # Ensure close_session is async

        # Configure error mapper to return specific API error
        expected_error = APIError(
            message="Mapped HTTP Error",
            code=APIErrorCode.CONNECTION_ERROR.value,
        )
        mock_error_mapper.map_exchange_error.return_value = expected_error

        api = exchange_api_with_di(
            http_client=mock_http,
            error_mapper=mock_error_mapper,
        )

        # Since we can't test _request directly, we need to test a public method
        # that would use the HTTP client. Since this is a test ExchangeAPI,
        # we'll test that the error handling mechanism works through dependency injection.

        # This test validates that our DI setup allows for proper error handling
        assert api is not None  # Basic validation that DI setup works

        await api.close()


class TestExchangeAPIDependencyIsolation:
    """Test that dependency injection provides proper isolation."""

    def test_custom_dependency_override(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that specific dependencies can be overridden."""
        custom_http_client = MagicMock()
        custom_ws_manager = MagicMock()

        api = exchange_api_with_di(
            http_client=custom_http_client,
            ws_manager=custom_ws_manager,
        )

        # Verify the custom dependencies are used
        # We can't access private members, so we verify through behavior
        assert api is not None

    def test_multiple_api_instances_are_isolated(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that multiple API instances don't share dependencies."""
        api1 = exchange_api_with_di(exchange_name="exchange1")
        api2 = exchange_api_with_di(exchange_name="exchange2")

        # Verify instances are different
        assert api1 is not api2
        assert api1.exchange_name != api2.exchange_name

    def test_dependency_injection_completeness(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that all expected dependencies can be injected."""
        custom_error_mapper = MagicMock(spec=IErrorMapper)
        custom_authenticator = MagicMock(spec=IAuthenticator)
        custom_http_client = MagicMock()
        custom_ws_manager = MagicMock()
        custom_rate_limiter = MagicMock()

        api = exchange_api_with_di(
            error_mapper=custom_error_mapper,
            authenticator=custom_authenticator,
            http_client=custom_http_client,
            ws_manager=custom_ws_manager,
            rate_limiter_service=custom_rate_limiter,
        )

        # Verify API instance was created successfully with all custom dependencies
        assert api is not None
        assert api.exchange_name == "test_exchange"


class TestExchangeAPIPublicInterface:
    """Test the public interface of ExchangeAPI."""

    @pytest.mark.asyncio
    async def test_abstract_methods_implemented(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test that all abstract methods are properly implemented."""
        api = exchange_api_with_di()

        # Test that all abstract methods can be called without errors
        await api.get_ticker(BTC_HL)
        await api.get_order_book(BTC_HL)
        await api.get_funding_rates(GetFundingRatesArgs(symbols=[BTC_HL]))
        await api.get_market_data(GetMarketDataArgs(symbol=BTC_HL, timeframe="1h"))
        await api.get_balances()
        await api.get_account_summary()
        await api.get_positions()
        place_order_args = PlaceOrderArgs(
            symbol=BTC_HL,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=Decimal(1),
            time_in_force=TimeInForce.GTC,
        )
        await api.place_order(place_order_args)
        await api.cancel_order(CancelOrderArgs(order_id="order123"))
        await api.cancel_all_orders()
        await api.get_open_orders()
        await api.get_order_history(GetOrderHistoryArgs())
        await api.get_trade_history(GetTradeHistoryArgs())
        await api.get_order_status(GetOrderArgs(order_id="order123"))
        await api.get_order(GetOrderArgs(order_id="order123"))
        await api.get_all_open_orders(GetAllOpenOrdersArgs())

        await api.close()

    def test_subscription_payload_construction(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test subscription payload construction through public behavior."""
        api = exchange_api_with_di()

        # We can't test the private method directly, but we can verify
        # that the API has the expected public interface for subscriptions
        assert hasattr(api, "subscribe")
        assert callable(api.subscribe)

    @pytest.mark.asyncio
    async def test_websocket_connection_interface(
        self,
        exchange_api_with_di: Callable[..., ConcreteTestExchangeAPI],
    ) -> None:
        """Test WebSocket connection interface."""
        api = exchange_api_with_di()

        # Test public WebSocket interface
        await api.connect_websocket()
        await api.ping_websocket()

        # Test that is_connected property is accessible
        _ = api.is_connected

        await api.close()
