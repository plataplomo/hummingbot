import asyncio
from collections.abc import Callable, Coroutine, Mapping
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.base.error_mapper_interface import IErrorMapper
from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.connectivity.http_client import HttpRequestFailedError
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import (
    DerivativePosition,
    FundingRate,
    Order,
    OrderBook,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.models.market import Candle

MessageHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


@pytest.fixture
def mock_error_mapper() -> MagicMock:
    return MagicMock(spec=IErrorMapper)


class ConcreteTestExchangeAPI(ExchangeAPI):
    def __init__(
        self,
        exchange_name: str,
        config: dict[str, Any],
        secrets: dict[str, str | None],
        error_mapper: IErrorMapper,
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(exchange_name, config, secrets, error_mapper, loop)
        # Mock specific attributes if needed for tests, not entire methods here
        self.mock_auth_method = AsyncMock(return_value={})
        self.mock_route_ws_method = AsyncMock()
        self.mock_subscribe_method = AsyncMock()
        self.mock_resubscribe_method = AsyncMock()
        self.mock_handle_websocket_message_method = AsyncMock()
        self.mock_update_rate_limit_method = MagicMock()
        self.mock_construct_subscription_payload_method = MagicMock(return_value={"sub": "payload"})
        self.mock_on_ws_connected_method = AsyncMock()

    async def _authenticate(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self.mock_auth_method(method, path, params, data)

    async def _route_ws_message(self, message: dict[str, Any]) -> None:
        await self.mock_route_ws_method(message)

    async def subscribe(self, topic: str, handler: MessageHandler) -> None:
        await self.mock_subscribe_method(topic, handler)

    async def _resubscribe(self) -> None:
        await self.mock_resubscribe_method()

    async def _handle_websocket_message(self, message: dict[str, Any]) -> None:
        await self.mock_handle_websocket_message_method(message)

    def _update_rate_limit_from_headers(
        self, headers: Mapping[str, str], method: str, path: str
    ) -> None:
        self.mock_update_rate_limit_method(headers, method, path)

    async def get_ticker(self, symbol: str) -> Ticker:
        return MagicMock(spec=Ticker)

    async def get_order_book(self, symbol: str, depth: int | None = None) -> OrderBook:
        return MagicMock(spec=OrderBook)

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        return [MagicMock(spec=FundingRate)]

    async def get_market_data(self, symbol: str, timeframe: str, limit: int = 100) -> list[Candle]:
        return [MagicMock(spec=Candle)]

    async def get_balances(self) -> dict[str, SpotBalance]:
        return {"USD": MagicMock(spec=SpotBalance)}

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        return [MagicMock(spec=DerivativePosition)]

    async def place_order(
        self,
        *args: Any,  # noqa: ANN401
        **kwargs: Any,  # noqa: ANN401
    ) -> Order:
        return MagicMock(spec=Order)

    async def cancel_order(self, order_id: str, symbol: str | None = None) -> bool:
        return True

    async def cancel_all_orders(self, symbol: str | None = None) -> None:
        pass

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        return [MagicMock(spec=Order)]

    async def get_order_history(
        self,
        symbol: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int | None = None,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> list[Order]:
        return [MagicMock(spec=Order)]

    async def get_trade_history(self, symbol: str | None = None, limit: int = 100) -> list[Trade]:
        return [MagicMock(spec=Trade)]

    async def get_order_status(
        self, order_id: str, symbol: str | None = None, client_order_id: str | None = None
    ) -> Order | None:
        mock_order: MagicMock = MagicMock(spec=Order)
        mock_order.exchange_order_id = order_id
        return mock_order

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        return MagicMock(spec=Order)

    async def connect_websocket(self) -> None:
        pass  # Mocked behavior

    async def ping_websocket(self) -> None:
        pass  # Mocked behavior

    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        return [MagicMock(spec=Order)]

    # --- Abstract method implementations for WebSocketManager integration ---
    def _construct_subscription_payload(self, topic: str) -> dict[str, Any] | None:
        # Returning Any from function declared to return "dict[str, Any] | None"
        mock_payload: MagicMock = MagicMock()
        mock_payload.topic = topic
        return mock_payload

    async def _on_ws_connected(self) -> None:
        await self.mock_on_ws_connected_method()


@pytest.fixture
def default_config() -> dict[str, Any]:
    return {
        "rate_limits": {"default_rate": 10, "default_bucket_size": 10},
        "ws_endpoint": "wss://test.ws.endpoint",
        "rest_endpoint": "https://test.rest.endpoint",
    }


@pytest.fixture
def mock_loop() -> MagicMock:
    return MagicMock(spec=asyncio.AbstractEventLoop)


@pytest.fixture
def exchange_name() -> str:
    return "test_exchange"


@pytest.fixture
def mock_secrets() -> dict[str, str | None]:
    return {"API_KEY": "test_key", "API_SECRET": "test_secret"}


@patch("cyberdelta.apis.connectivity.rate_limiter_service.RateLimiterService")
def test_exchange_api_initialization_creates_rate_limiter_service(
    MockRateLimiterService: MagicMock,
    exchange_name: str,
    default_config: dict[str, Any],
    mock_secrets: dict[str, str | None],
    mock_error_mapper: MagicMock,
    mock_loop: MagicMock,
) -> None:
    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name,
        config=default_config,
        secrets=mock_secrets,
        error_mapper=mock_error_mapper,
        loop=mock_loop,
    )
    MockRateLimiterService.assert_called_once_with(
        exchange_name=exchange_name, config=default_config, loop=mock_loop
    )
    assert api._rate_limiter_service == MockRateLimiterService.return_value  # noqa: SLF001


@pytest.mark.asyncio
@patch("cyberdelta.apis.connectivity.http_client.HttpClient.request", new_callable=AsyncMock)
async def test_exchange_api_request_delegates_to_http_client_and_handles_response(
    mock_http_client_request: AsyncMock,
    exchange_name: str,
    default_config: dict[str, Any],
    mock_secrets: dict[str, str | None],
    mock_error_mapper: MagicMock,
    mock_loop: MagicMock,
) -> None:
    """
    Tests that ExchangeAPI._request correctly calls HttpClient.request
    and processes its successful response (content and headers).
    """
    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name,
        config=default_config,
        secrets=mock_secrets,
        error_mapper=mock_error_mapper,
        loop=mock_loop,
    )

    # Mock return value of HttpClient.request: (content, headers_multidict)
    mock_response_content = {"data": "success_payload"}
    # CIMultiDictProxy is tricky to mock directly if not imported; use MagicMock with items()
    mock_response_headers = MagicMock()
    mock_response_headers.items.return_value = [
        ("X-Response-ID", "123"),
        ("Content-Type", "application/json"),
    ]
    mock_http_client_request.return_value = (mock_response_content, mock_response_headers)

    method = "POST"
    endpoint = "/submit_data"
    params = {"query_param": "test"}
    data_payload = {"request_body": "payload"}
    custom_headers = {"X-Client-Specific": "value"}
    mock_authenticator = AsyncMock(spec=IAuthenticator)  # For signed request part
    api.authenticator = mock_authenticator

    # Call _request (as a signed request for this test part)
    result = await api._request(  # type: ignore # SLF001 for _request
        method, endpoint, params=params, data=data_payload, headers=custom_headers, is_signed=True
    )

    assert result == mock_response_content
    mock_http_client_request.assert_called_once_with(
        method=method,
        endpoint_path=endpoint.lstrip("/"),  # Ensure leading slash is removed for http_client
        rate_limiter_service=api._rate_limiter_service,  # noqa: SLF001
        authenticator=mock_authenticator,
        params=params,
        data=data_payload,
        headers=custom_headers,
        is_signed=True,
    )
    # Check that _update_rate_limit_from_headers was called with the processed headers
    api.mock_update_rate_limit_method.assert_called_once_with(
        mock_response_headers,  # Pass the multidict proxy directly
        method,
        endpoint.lstrip("/"),  # Ensure consistent path format
    )


# Test for HttpRequestFailedError (copied and adapted from previous attempt)
@pytest.mark.asyncio
@patch("cyberdelta.apis.connectivity.http_client.HttpClient.request", new_callable=AsyncMock)
async def test_request_error_mapping_from_http_request_failed_error(
    mock_http_client_request: AsyncMock,
    exchange_name: str,
    default_config: dict[str, Any],
    mock_secrets: dict[str, str | None],
    mock_error_mapper: MagicMock,
    mock_loop: MagicMock,
) -> None:
    """Test that HttpRequestFailedError from HttpClient is mapped by error_mapper."""
    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name,
        config=default_config,
        secrets=mock_secrets,
        error_mapper=mock_error_mapper,
        loop=mock_loop,
    )

    # Configure HttpClient.request to raise HttpRequestFailedError
    http_status_from_exchange = 400
    error_body_from_exchange = '{"error": "Specific exchange error", "code": 1234}'
    parsed_error_data_from_exchange = {"error": "Specific exchange error", "code": 1234}
    request_path_sent = "/test/error"

    http_failure = HttpRequestFailedError(
        message=f"HTTP {http_status_from_exchange} Error",  # Generic message
        http_status_code=http_status_from_exchange,
        response_body=error_body_from_exchange,
        # api_error_code can be specified if HttpClient determines one, else default
    )
    mock_http_client_request.side_effect = http_failure

    # Configure the (mocked) error_mapper.map_exchange_error to return a specific APIError
    expected_mapped_api_error = APIError(
        message="Mapped: Bad Request",
        code=APIErrorCode.INVALID_REQUEST.value,
        http_status=http_status_from_exchange,
        exchange_message=error_body_from_exchange,
    )
    # api.error_mapper is already the mock_error_mapper instance
    mock_error_mapper.map_exchange_error.return_value = expected_mapped_api_error

    with pytest.raises(APIError) as exc_info:
        await api._request(method="POST", endpoint=request_path_sent.lstrip("/"))  # type: ignore

    # Assert that the raised exception is the one returned by our mocked map_exchange_error
    assert exc_info.value is expected_mapped_api_error

    # Assert that map_exchange_error was called correctly
    mock_error_mapper.map_exchange_error.assert_called_once_with(
        status_code=http_status_from_exchange,
        error_body=error_body_from_exchange,
        error_data=parsed_error_data_from_exchange,
        request_path=request_path_sent,
    )


# Test for aiohttp.ClientError (copied and adapted)
@pytest.mark.asyncio
@patch("cyberdelta.apis.connectivity.http_client.HttpClient.request", new_callable=AsyncMock)
async def test_request_handles_client_error_from_http_client(
    mock_http_client_request: AsyncMock,
    exchange_name: str,
    default_config: dict[str, Any],
    mock_secrets: dict[str, str | None],
    mock_error_mapper: MagicMock,
    mock_loop: MagicMock,
) -> None:
    """Test that other client errors (e.g., aiohttp.ClientConnectionError) are also mapped."""
    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name,
        config=default_config,
        secrets=mock_secrets,
        error_mapper=mock_error_mapper,
        loop=mock_loop,
    )

    original_client_error = aiohttp.ClientConnectionError("Connection refused")
    mock_http_client_request.side_effect = (
        original_client_error  # HttpClient would wrap this or _request catches it
    )

    expected_mapped_api_error = APIError(
        message="Mapped: Connection Error",
        code=APIErrorCode.CONNECTION_ERROR.value,
        http_status=503,  # Default for connection issues if not otherwise specified
    )
    mock_error_mapper.map_exchange_error.return_value = expected_mapped_api_error

    request_path_sent = "/test/conn_error"
    with pytest.raises(APIError) as exc_info:
        await api._request(method="GET", endpoint=request_path_sent.lstrip("/"))  # type: ignore

    assert exc_info.value is expected_mapped_api_error
    mock_error_mapper.map_exchange_error.assert_called_once_with(
        status_code=503,  # This is how ExchangeAPI._request currently translates ClientError
        error_body=str(original_client_error),
        error_data=None,
        request_path=request_path_sent,
    )


# Test for asyncio.TimeoutError (copied and adapted)
@pytest.mark.asyncio
@patch("cyberdelta.apis.connectivity.http_client.HttpClient.request", new_callable=AsyncMock)
async def test_request_handles_timeout_error_from_http_client(
    mock_http_client_request: AsyncMock,
    exchange_name: str,
    default_config: dict[str, Any],
    mock_secrets: dict[str, str | None],
    mock_error_mapper: MagicMock,
    mock_loop: MagicMock,
) -> None:
    """Test that TimeoutError from HttpClient is mapped."""
    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name,
        config=default_config,
        secrets=mock_secrets,
        error_mapper=mock_error_mapper,
        loop=mock_loop,
    )

    original_timeout_error = TimeoutError("Request timed out")
    # HttpClient.request might raise asyncio.TimeoutError directly or wrap it.
    # Let's assume it can propagate or be caught by _request's general Exception handler.
    mock_http_client_request.side_effect = original_timeout_error

    expected_mapped_api_error = APIError(
        message="Mapped: Timeout",
        code=APIErrorCode.TIMEOUT.value,
        http_status=504,  # Default for timeout issues
    )
    mock_error_mapper.map_exchange_error.return_value = expected_mapped_api_error

    request_path_sent = "/test/timeout"
    with pytest.raises(APIError) as exc_info:
        await api._request(method="GET", endpoint=request_path_sent.lstrip("/"))  # type: ignore

    assert exc_info.value is expected_mapped_api_error
    # The generic exception handler in _request would call map_exchange_error
    mock_error_mapper.map_exchange_error.assert_called_once_with(
        status_code=500,
        error_body=str(original_timeout_error),
        error_data=None,
        request_path=request_path_sent,
    )


# Start of new Test Class for WebSocketManager integration
@patch(
    "cyberdelta.apis.base.exchange_api.WebSocketManager"
)  # Patch WebSocketManager where it's imported in exchange_api
class TestExchangeAPIWebSocketIntegration:
    @pytest.fixture
    def mock_ws_manager_class(self, MockWebSocketManager: MagicMock) -> MagicMock:
        """Fixture to get the patched WebSocketManager class."""
        return MockWebSocketManager

    @pytest.fixture
    def mock_ws_manager_instance(self, mock_ws_manager_class: MagicMock) -> MagicMock:
        """Fixture to get the instance of the patched WebSocketManager."""
        return mock_ws_manager_class.return_value

    def test_initialization_with_ws_endpoint(
        self,
        mock_ws_manager_class: MagicMock,
        mock_ws_manager_instance: MagicMock,
        default_config: dict[str, Any],
        mock_error_mapper: MagicMock,
    ) -> None:
        api = ConcreteTestExchangeAPI("test_ws_exchange", default_config, {}, mock_error_mapper)
        mock_ws_manager_class.assert_called_once_with(
            exchange_name="test_ws_exchange",
            ws_url=default_config["ws_endpoint"],
            message_handler=api._handle_websocket_message,  # Check callback
            on_connected_callback=api._on_ws_connected,  # Check callback
            ping_interval=None,  # Assuming default_config doesn't specify these
            reconnect_delay=None,
            max_reconnect_attempts=None,
            connection_timeout=None,
        )
        assert api._ws_manager is mock_ws_manager_instance  # noqa: SLF001

    def test_initialization_without_ws_endpoint(
        self,
        mock_ws_manager_class: MagicMock,
        mock_error_mapper: MagicMock,
        default_config: dict[str, Any],
    ) -> None:
        config_no_ws = default_config.copy()
        del config_no_ws["ws_endpoint"]
        api = ConcreteTestExchangeAPI("test_no_ws_exchange", config_no_ws, {}, mock_error_mapper)
        mock_ws_manager_class.assert_not_called()
        assert api._ws_manager is None  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_connect_websocket_delegates_to_ws_manager(
        self,
        mock_ws_manager_instance: MagicMock,
        mock_error_mapper: MagicMock,
        default_config: dict[str, Any],
    ) -> None:
        api = ConcreteTestExchangeAPI("test_exchange", default_config, {}, mock_error_mapper)
        mock_ws_manager_instance.connect = AsyncMock()  # Ensure connect is an AsyncMock
        await api.connect_websocket()
        mock_ws_manager_instance.connect.assert_called_once()

    def test_is_connected_property_delegates_to_ws_manager(
        self,
        mock_ws_manager_instance: MagicMock,
        mock_error_mapper: MagicMock,
        default_config: dict[str, Any],
    ) -> None:
        api = ConcreteTestExchangeAPI("test_exchange", default_config, {}, mock_error_mapper)
        mock_ws_manager_instance.is_connected = True
        assert api.is_connected is True
        mock_ws_manager_instance.is_connected = False
        assert api.is_connected is False

    @pytest.mark.asyncio
    async def test_close_delegates_to_ws_manager(
        self,
        mock_ws_manager_instance: MagicMock,
        mock_error_mapper: MagicMock,
        default_config: dict[str, Any],
    ) -> None:
        api = ConcreteTestExchangeAPI("test_exchange", default_config, {}, mock_error_mapper)
        mock_ws_manager_instance.close = AsyncMock()
        await api.close()  # ExchangeAPI.close also closes http_client, test ws part here
        mock_ws_manager_instance.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribe_registers_handler_and_sends_if_connected(
        self,
        mock_ws_manager_instance: MagicMock,
        mock_error_mapper: MagicMock,
        default_config: dict[str, Any],
    ) -> None:
        api = ConcreteTestExchangeAPI("test_exchange", default_config, {}, mock_error_mapper)
        mock_handler = AsyncMock()
        topic = "test.topic"
        payload = {"type": "subscribe", "channel": topic}
        api.mock_construct_subscription_payload_method.return_value = payload

        # Test when connected
        mock_ws_manager_instance.is_connected = True
        mock_ws_manager_instance.send_json = AsyncMock(return_value=True)

        await api.subscribe(
            topic, mock_handler
        )  # ConcreteTestExchangeAPI uses its own mock_subscribe_method
        # We need to test the actual ExchangeAPI.subscribe logic.
        # This requires calling super().subscribe or testing it directly.
        # Let's call the actual ExchangeAPI.subscribe behavior
        # To test ExchangeAPI.subscribe, we call it directly as it's not abstract anymore
        # Re-evaluate how ConcreteTestExchangeAPI handles subscribe vs ExchangeAPI.subscribe
        # For now, let's assume ConcreteTestExchangeAPI.subscribe was a mock for other tests
        # and we want to test the actual base class ExchangeAPI.subscribe logic.
        # We can do this by calling it through the instance, as it is no longer abstract.

        # Reset mocks for ExchangeAPI.subscribe test
        api._ws_handlers.clear()  # Clear handlers from previous calls if any # noqa: SLF001
        api.mock_construct_subscription_payload_method.reset_mock()
        mock_ws_manager_instance.send_json.reset_mock()

        await ExchangeAPI.subscribe(api, topic, mock_handler)  # Call the actual base method

        assert topic in api._ws_handlers  # noqa: SLF001
        assert api._ws_handlers[topic] is mock_handler  # noqa: SLF001
        api.mock_construct_subscription_payload_method.assert_called_once_with(topic)
        mock_ws_manager_instance.send_json.assert_called_once_with(payload)

    @pytest.mark.asyncio
    async def test_subscribe_logs_warning_if_not_connected(
        self,
        mock_ws_manager_instance: MagicMock,
        mock_error_mapper: MagicMock,
        default_config: dict[str, Any],
        caplog: pytest.LogCaptureFixture,  # Added caplog fixture
    ) -> None:
        api = ConcreteTestExchangeAPI("test_exchange", default_config, {}, mock_error_mapper)
        mock_handler = AsyncMock()
        topic = "test.topic.notconnected"

        mock_ws_manager_instance.is_connected = False  # Simulate not connected
        mock_ws_manager_instance.send_json = AsyncMock()

        await ExchangeAPI.subscribe(api, topic, mock_handler)  # Call actual base method

        assert topic in api._ws_handlers
        mock_ws_manager_instance.send_json.assert_not_called()
        assert (
            f"[{api.exchange_name}] WebSocket not connected. "  # Broke long line
            f"Subscription to {topic} will be attempted upon connection." in caplog.text
        )

    @pytest.mark.asyncio
    async def test_resubscribe_sends_for_all_handlers_if_connected(
        self,
        mock_ws_manager_instance: MagicMock,
        mock_error_mapper: MagicMock,
        default_config: dict[str, Any],
    ) -> None:
        api = ConcreteTestExchangeAPI("test_exchange", default_config, {}, mock_error_mapper)
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        topic1, topic2 = "topic1", "topic2"
        payload1, payload2 = {"sub": topic1}, {"sub": topic2}

        # Setup handlers directly for testing ExchangeAPI._resubscribe
        api._ws_handlers = {topic1: handler1, topic2: handler2}  # noqa: SLF001

        # Mock _construct_subscription_payload to return different payloads for different topics
        def side_effect_construct_payload(topic_arg: str) -> dict[str, Any] | None:
            if topic_arg == topic1:
                return payload1
            if topic_arg == topic2:
                return payload2
            return None

        api.mock_construct_subscription_payload_method.side_effect = side_effect_construct_payload

        mock_ws_manager_instance.is_connected = True
        mock_ws_manager_instance.send_json = AsyncMock(return_value=True)

        await ExchangeAPI._resubscribe(api)  # Call actual base method

        api.mock_construct_subscription_payload_method.assert_any_call(topic1)
        api.mock_construct_subscription_payload_method.assert_any_call(topic2)
        mock_ws_manager_instance.send_json.assert_any_call(payload1)
        mock_ws_manager_instance.send_json.assert_any_call(payload2)
        assert mock_ws_manager_instance.send_json.call_count == 2

    @pytest.mark.asyncio
    async def test_on_ws_connected_calls_resubscribe(
        self, mock_error_mapper: MagicMock, default_config: dict[str, Any]
    ) -> None:
        api = ConcreteTestExchangeAPI("test_exchange", default_config, {}, mock_error_mapper)
        # Patch _resubscribe on the instance to check if it's called
        api._resubscribe = AsyncMock()  # type: ignore # noqa: SLF001

        await ExchangeAPI._on_ws_connected(api)  # Call actual base method # noqa: SLF001
        api._resubscribe.assert_called_once()  # noqa: SLF001


# End of new Test Class
