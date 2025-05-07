import asyncio
from collections.abc import Callable, Coroutine, Mapping
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.base_api import ExchangeAPI
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


class ConcreteTestExchangeAPI(ExchangeAPI):
    def __init__(
        self,
        exchange_name: str,
        config: dict[str, Any],
        secrets: dict[str, str | None],
        loop: asyncio.AbstractEventLoop | None = None,
    ) -> None:
        super().__init__(exchange_name, config, secrets, loop)
        # Mock specific attributes if needed for tests, not entire methods here
        self.mock_auth_method = AsyncMock(return_value={})
        self.mock_route_ws_method = AsyncMock()
        self.mock_subscribe_method = AsyncMock()
        self.mock_resubscribe_method = AsyncMock()
        self.mock_handle_websocket_message_method = AsyncMock()
        self.mock_update_rate_limit_method = MagicMock()

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
        *args: Any,
        **kwargs: Any,  # type: ignore[misc]
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
        return MagicMock(spec=Order)

    async def get_order(self, order_id: str, symbol: str | None = None) -> Order | None:
        return MagicMock(spec=Order)

    async def connect_websocket(self) -> None:
        pass  # Mocked behavior

    async def ping_websocket(self) -> None:
        pass  # Mocked behavior

    async def get_all_open_orders(self, symbol: str | None = None) -> list[Order]:
        return [MagicMock(spec=Order)]


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
    mock_loop: MagicMock,
) -> None:
    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name, config=default_config, secrets=mock_secrets, loop=mock_loop
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
    mock_loop: MagicMock,
) -> None:
    """
    Tests that ExchangeAPI._request correctly calls HttpClient.request
    and processes its successful response (content and headers).
    """
    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name, config=default_config, secrets=mock_secrets, loop=mock_loop
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
        endpoint_path=endpoint,
        rate_limiter_service=api._rate_limiter_service,  # type: ignore # SLF001
        authenticator=mock_authenticator,
        params=params,
        data=data_payload,
        headers=custom_headers,
        is_signed=True,
    )
    # Check that _update_rate_limit_from_headers was called with the processed headers
    api.mock_update_rate_limit_method.assert_called_once_with(
        {"X-Response-ID": "123", "Content-Type": "application/json"},  # Expected dict from headers
        method,
        endpoint,
    )


# Test for HttpRequestFailedError (copied and adapted from previous attempt)
@pytest.mark.asyncio
@patch("cyberdelta.apis.connectivity.http_client.HttpClient.request", new_callable=AsyncMock)
async def test_request_error_mapping_from_http_request_failed_error(
    mock_http_client_request: AsyncMock,
    exchange_name: str,
    default_config: dict[str, Any],
    mock_secrets: dict[str, str | None],
    mock_loop: MagicMock,
) -> None:
    """Test that HttpRequestFailedError from HttpClient is mapped by _map_error_response."""
    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name, config=default_config, secrets=mock_secrets, loop=mock_loop
    )

    http_error = HttpRequestFailedError(
        message="HTTP 404 Not Found",
        http_status_code=404,
        response_body="Resource not here",
        api_error_code=APIErrorCode.NETWORK_ISSUE,  # Or any other appropriate default for http_client
    )
    mock_http_client_request.side_effect = http_error

    mapped_api_error = APIError(
        "Mapped Not Found by test", code=APIErrorCode.SYMBOL_NOT_FOUND.value
    )
    # Patch the instance method _map_error_response
    with patch.object(api, "_map_error_response", return_value=mapped_api_error) as mock_map_error:
        with pytest.raises(APIError) as excinfo:
            await api._request("GET", "/notfound")  # type: ignore # SLF001

        assert excinfo.value is mapped_api_error
        mock_map_error.assert_called_once_with(
            status_code=404, error_body="Resource not here", error_data=None
        )


# Test for aiohttp.ClientError (copied and adapted)
@pytest.mark.asyncio
@patch("cyberdelta.apis.connectivity.http_client.HttpClient.request", new_callable=AsyncMock)
async def test_request_handles_client_error_from_http_client(
    mock_http_client_request: AsyncMock,
    exchange_name: str,
    default_config: dict[str, Any],
    mock_secrets: dict[str, str | None],
    mock_loop: MagicMock,
) -> None:
    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name, config=default_config, secrets=mock_secrets, loop=mock_loop
    )
    original_client_error = aiohttp.ClientConnectorError(
        MagicMock(), OSError("Connection failed OS")
    )
    mock_http_client_request.side_effect = original_client_error

    with pytest.raises(APIError) as excinfo:
        await api._request("GET", "/clienterr")  # type: ignore # SLF001

    assert excinfo.value.code == APIErrorCode.NETWORK_ISSUE.value
    assert excinfo.value.original_exception is original_client_error


# Test for asyncio.TimeoutError (copied and adapted)
@pytest.mark.asyncio
@patch("cyberdelta.apis.connectivity.http_client.HttpClient.request", new_callable=AsyncMock)
async def test_request_handles_timeout_error_from_http_client(
    mock_http_client_request: AsyncMock,
    exchange_name: str,
    default_config: dict[str, Any],
    mock_secrets: dict[str, str | None],
    mock_loop: MagicMock,
) -> None:
    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name, config=default_config, secrets=mock_secrets, loop=mock_loop
    )
    original_timeout_error = TimeoutError("Request really really timed out")
    mock_http_client_request.side_effect = original_timeout_error

    with pytest.raises(APIError) as excinfo:
        await api._request("GET", "/timeout")  # type: ignore # SLF001

    assert excinfo.value.code == APIErrorCode.TIMEOUT.value
    assert excinfo.value.original_exception is original_timeout_error
