import asyncio
from collections.abc import Callable, Coroutine, Mapping
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.base_api import ExchangeAPI
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
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


@patch("cyberdelta.apis.connectivity.rate_limiter_service.RateLimiterService")
def test_get_rate_limiter_delegates_to_service(
    MockRateLimiterService: MagicMock,
    exchange_name: str,
    default_config: dict[str, Any],
    mock_secrets: dict[str, str | None],
    mock_loop: MagicMock,
) -> None:
    mock_service_instance = MockRateLimiterService.return_value
    mock_target_limiter = MagicMock(spec=TokenBucketRateLimiterRuntime)
    mock_service_instance.get_limiter.return_value = mock_target_limiter

    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name, config=default_config, secrets=mock_secrets, loop=mock_loop
    )
    # Ensure the service instance from RateLimiterService() is used by api
    api._rate_limiter_service = mock_service_instance  # noqa: SLF001

    method = "GET"
    path = "/test/path"
    actual_limiter = asyncio.run(
        api._get_rate_limiter(method, path)  # noqa: SLF001
    )  # _get_rate_limiter is async

    mock_service_instance.get_limiter.assert_called_once_with(method, path)
    assert actual_limiter == mock_target_limiter


@pytest.mark.asyncio
@patch("aiohttp.ClientSession")
@patch("cyberdelta.apis.connectivity.rate_limiter_service.RateLimiterService")
async def test_request_acquires_limiter_before_request(
    MockRateLimiterService: MagicMock,
    MockClientSession: MagicMock,
    exchange_name: str,
    default_config: dict[str, Any],
    mock_secrets: dict[str, str | None],
    mock_loop: MagicMock,
) -> None:
    mock_service_instance = MockRateLimiterService.return_value
    mock_actual_limiter = AsyncMock(spec=TokenBucketRateLimiterRuntime)
    mock_service_instance.get_limiter.return_value = mock_actual_limiter

    api = ConcreteTestExchangeAPI(
        exchange_name=exchange_name, config=default_config, secrets=mock_secrets, loop=mock_loop
    )
    api._rate_limiter_service = mock_service_instance  # noqa: SLF001

    # Use the MockClientSession from the patch
    mock_session_instance = MockClientSession.return_value
    mock_session_instance.request.return_value.__aenter__.return_value.status = 200
    mock_session_instance.request.return_value.__aenter__.return_value.json = AsyncMock(
        return_value={"data": "success"}
    )
    api._session = mock_session_instance  # noqa: SLF001

    method = "GET"
    endpoint = "/test/endpoint"
    await api._request(method, endpoint)  # noqa: SLF001

    # Assert that get_limiter was called correctly on the service instance
    mock_service_instance.get_limiter.assert_called_once_with(method, endpoint)
    mock_actual_limiter.acquire.assert_called_once()
