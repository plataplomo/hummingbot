import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import LogCaptureFixture

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.bp_auth import BackpackHmacAuthenticator
from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.connectivity.http_client import HttpRequestFailedError
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce


@pytest.fixture
def mock_loop() -> MagicMock:
    return MagicMock(spec=asyncio.AbstractEventLoop)


@pytest.fixture
def default_bp_config() -> dict[str, Any]:
    return {
        "exchange_name": "backpack",
        "rest_endpoint": "https://api.backpack.test",
        "ws_endpoint": "wss://ws.backpack.test",
        "rate_limits": {"default_rate": 10, "default_bucket_size": 10},
    }


@pytest.fixture
def bp_secrets_valid() -> dict[str, str | None]:
    return {"BACKPACK_API_KEY": "test_key", "BACKPACK_API_SECRET": "test_secret"}


@pytest.fixture
def bp_secrets_invalid() -> dict[str, str | None]:
    return {"BACKPACK_API_KEY": None, "BACKPACK_API_SECRET": None}


@pytest.fixture
def mock_bp_authenticator_instance() -> MagicMock:
    mock_auth = MagicMock(spec=BackpackHmacAuthenticator)
    mock_auth.prepare_request = AsyncMock(
        return_value=AuthenticatedRequestComponents(
            headers={"X-Test-Signed": "true"}, params=None, data=None
        )
    )
    return mock_auth


class TestBackpackAPI_Authentication:
    def test_backpack_api_initialization_with_valid_secrets(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
        mock_loop: MagicMock,
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_valid)
        assert api._bp_authenticator is not None  # noqa: SLF001
        assert isinstance(api._bp_authenticator, BackpackHmacAuthenticator)  # noqa: SLF001
        assert api._api_key == "test_key"  # noqa: SLF001
        assert api._api_secret == "test_secret"  # noqa: SLF001

    def test_backpack_api_initialization_with_invalid_secrets(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_invalid: dict[str, str | None],
        mock_loop: MagicMock,
        caplog: LogCaptureFixture,
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_invalid)
        assert api._bp_authenticator is None  # noqa: SLF001
        assert "Authenticator not initialized" in caplog.text

    @pytest.mark.asyncio
    async def test_authenticate_method_uses_bp_authenticator(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
        mock_loop: MagicMock,
        mock_bp_authenticator_instance: MagicMock,
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_valid)
        # Replace the actual authenticator with our mock for this test
        api._bp_authenticator = mock_bp_authenticator_instance  # noqa: SLF001

        method = "GET"
        path = "/api/v1/capital"
        params = {"test_param": "value"}
        data = {"test_data": "value"}

        expected_auth_components = AuthenticatedRequestComponents(
            headers={
                "X-Test-Signed": "true",
                "Content-Type": "application/json; charset=utf-8",
                "Accept": "application/json",
            },
            params=params,
            data=data,
        )
        mock_bp_authenticator_instance.prepare_request.return_value = expected_auth_components

        auth_dict = await api._authenticate(method, path, params, data)  # noqa: SLF001

        mock_bp_authenticator_instance.prepare_request.assert_called_once_with(
            method=method, path=path, params=params, data=data, headers=api.default_headers.copy()
        )
        assert auth_dict["headers"] == expected_auth_components["headers"]
        assert auth_dict["params"] == expected_auth_components["params"]
        assert auth_dict["data"] == expected_auth_components["data"]

    @pytest.mark.asyncio
    async def test_authenticate_method_raises_if_authenticator_not_initialized(
        self,
        default_bp_config: dict[str, Any],
        bp_secrets_invalid: dict[str, str | None],
        mock_loop: MagicMock,
    ) -> None:
        api = BackpackAPI(
            default_bp_config, bp_secrets_invalid
        )  # Initializes with no authenticator
        with pytest.raises(APIError) as exc_info:
            await api._authenticate("GET", "/test", None, None)  # noqa: SLF001
        assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
        assert "Backpack authenticator not initialized" in exc_info.value.message

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.base_api.ExchangeAPI._request")  # Patch the base _request
    async def test_signed_request_flow_uses_authenticate(
        self,
        mock_base_request: AsyncMock,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
        mock_loop: MagicMock,
        mock_bp_authenticator_instance: MagicMock,
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_valid)
        api._bp_authenticator = mock_bp_authenticator_instance  # noqa: SLF001
        mock_base_request.return_value = {
            "status": "success"
        }  # Mock a successful response from _request

        endpoint = "/api/v1/order"
        # req_data removed as it was unused here

        # This is what _bp_authenticator.prepare_request is mocked to return
        mocked_auth_headers = {
            "X-Test-Signed": "true",
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
        }
        # Assuming authenticator doesn't change params for this test
        mocked_auth_params = None  # Params are in body for POST
        mocked_auth_data = {  # Data constructed by place_order
            "symbol": "SOL_USDC",
            "side": "buy",
            "orderType": "limit",
            "quantity": "1",
            "price": "100",
            "timeInForce": "GTC",
            "clientId": "myorder123",
        }

        mock_bp_authenticator_instance.prepare_request.return_value = (
            AuthenticatedRequestComponents(
                headers=mocked_auth_headers, params=mocked_auth_params, data=mocked_auth_data
            )
        )

        # Call a method that uses _request with is_signed=True, e.g., place_order
        await api.place_order(
            symbol="SOL_USDC",
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=Decimal("1"),
            price=Decimal("100"),
            time_in_force=TimeInForce.GTC,
            client_order_id="myorder123",
        )

        # Check that _authenticate was called (implicitly by _request when is_signed=True)
        # This is hard to check directly without further mocking _authenticate itself.
        # The more important check is that BackpackHmacAuthenticator.prepare_request was called.
        mock_bp_authenticator_instance.prepare_request.assert_called_once_with(
            method="POST",
            path=endpoint,  # place_order uses "/api/v1/order"
            params=None,  # place_order sends params in data body for POST
            data=mocked_auth_data,  # Check against the data that place_order would construct
            headers=api.default_headers.copy(),
        )

        # Verify that the base _request was called with the headers from the authenticator
        mock_base_request.assert_called_once()
        _args, kwargs = mock_base_request.call_args
        assert kwargs["method"] == "POST"
        assert kwargs["endpoint"] == endpoint
        assert kwargs["data"] == mocked_auth_data
        assert kwargs["headers"] == mocked_auth_headers
        assert kwargs["is_signed"] is True


class TestBackpackAPIMethodErrors:
    @pytest.mark.asyncio
    @patch(
        "cyberdelta.apis.base_api.ExchangeAPI._request"
    )  # Patch the base _request used by BackpackAPI
    async def test_get_ticker_handles_mapped_invalid_symbol_error(
        self,
        mock_base_request: AsyncMock,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
        # mock_loop: MagicMock, # Not strictly needed here unless something uses it
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_valid)
        # The api instance will have BackpackErrorMapper as its self.error_mapper

        # Simulate that HttpClient.request (called by ExchangeAPI._request) raises HttpRequestFailedError
        # This error will then be processed by ExchangeAPI._request, which calls self.error_mapper.map_exchange_error
        http_status_from_exchange = 400
        error_body_from_exchange = '{"error":"Invalid symbol: XYZ_USDC","code":10001}'
        # parsed_error_data_from_exchange = {"error":"Invalid symbol: XYZ_USDC","code":10001}
        request_path_sent = "/api/v1/ticker/XYZ_USDC"

        # This is the error that ExchangeAPI._request would receive from HttpClient
        http_failure = HttpRequestFailedError(
            message=f"HTTP {http_status_from_exchange} Error",
            http_status_code=http_status_from_exchange,
            response_body=error_body_from_exchange,
        )
        mock_base_request.side_effect = http_failure

        with pytest.raises(APIError) as exc_info:
            await api.get_ticker(symbol="XYZ_USDC")

        # Now we assert on the error that BackpackErrorMapper would produce for this scenario
        assert exc_info.value.code == APIErrorCode.INVALID_SYMBOL.value
        assert exc_info.value.http_status == http_status_from_exchange
        assert "Invalid symbol" in exc_info.value.message
        assert exc_info.value.exchange_message == error_body_from_exchange

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.base_api.ExchangeAPI._request")
    async def test_place_order_handles_mapped_insufficient_funds_error(
        self,
        mock_base_request: AsyncMock,
        default_bp_config: dict[str, Any],
        bp_secrets_valid: dict[str, str | None],
    ) -> None:
        api = BackpackAPI(default_bp_config, bp_secrets_valid)

        http_status_from_exchange = 400
        error_body_from_exchange = (
            '{"error":"Account has insufficient balance for requested action.","code":10004}'
        )

        http_failure = HttpRequestFailedError(
            message=f"HTTP {http_status_from_exchange} Error for order placement",
            http_status_code=http_status_from_exchange,
            response_body=error_body_from_exchange,
        )
        mock_base_request.side_effect = http_failure

        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="SOL_USDC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("100000"),  # Large quantity likely to fail
                price=Decimal("1"),
                time_in_force=TimeInForce.GTC,
            )

        assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value
        assert exc_info.value.http_status == http_status_from_exchange
        assert "Account has insufficient balance" in exc_info.value.message
        assert exc_info.value.exchange_message == error_body_from_exchange

    # TODO: Add more error tests for other methods and error types


class TestBackpackAPIWebSocketRouting:
    @pytest.fixture
    def api_for_ws_tests(
        self, default_bp_config: dict[str, Any], bp_secrets_valid: dict[str, str | None]
    ) -> BackpackAPI:
        # We won't actually connect, so ws_manager interaction is minimal here, focus on routing logic
        # Ensure it has a ws_manager instance for subscribe to try to use though.
        with patch("cyberdelta.apis.base.exchange_api.WebSocketManager") as mock_ws_mgr_class:
            mock_ws_mgr_instance = MagicMock()
            mock_ws_mgr_class.return_value = mock_ws_mgr_instance
            api = BackpackAPI(default_bp_config, bp_secrets_valid)
            api._ws_manager = mock_ws_mgr_instance  # noqa: SLF001 - for testing
            return api

    def test_construct_subscription_payload(self, api_for_ws_tests: BackpackAPI) -> None:
        topic = "depth.SOL_USDC"
        payload = api_for_ws_tests._construct_subscription_payload(topic)  # noqa: SLF001
        assert payload == {
            "op": "subscribe",
            "channel": topic,
            "args": {},
        }

    @pytest.mark.asyncio
    async def test_route_ws_message_public_topic(self, api_for_ws_tests: BackpackAPI) -> None:
        mock_depth_handler = AsyncMock()
        topic = "depth.SOL_USDC"
        # Register handler using the actual subscribe method (which populates _ws_handlers)
        # We mock out ws_manager.send_json to prevent actual sending during this registration.
        if api_for_ws_tests._ws_manager:  # noqa: SLF001
            api_for_ws_tests._ws_manager.send_json = AsyncMock(return_value=True)  # noqa: SLF001
            api_for_ws_tests._ws_manager.is_connected = True  # noqa: SLF001

        await api_for_ws_tests.subscribe(topic, mock_depth_handler)

        test_message_data = {"bids": [["100", "1"]], "asks": [["101", "2"]]}
        test_message = {"topic": topic, "data": test_message_data}

        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001

        mock_depth_handler.assert_called_once_with(test_message_data)

    @pytest.mark.asyncio
    async def test_route_ws_message_private_topic_fills(
        self, api_for_ws_tests: BackpackAPI
    ) -> None:
        mock_fills_handler = AsyncMock()
        # Backpack private streams might use a different topic structure, e.g.,
        # based on message type
        # As per bp_api.py _route_ws_message, it uses "fills" as topic from "type":"fills"
        topic_internal = "fills"
        # Directly add to _ws_handlers for testing routing, bypassing full subscribe logic.
        if api_for_ws_tests._ws_manager:  # noqa: SLF001
            api_for_ws_tests._ws_manager.send_json = AsyncMock(return_value=True)  # noqa: SLF001
            api_for_ws_tests._ws_manager.is_connected = True  # noqa: SLF001

        await api_for_ws_tests.subscribe(topic_internal, mock_fills_handler)

        # Example fill message structure (adapt if known structure is different)
        test_fill_data = {"id": "fill123", "price": "150", "qty": "0.5"}
        test_message = {"type": "fills", "data": test_fill_data}

        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001

        # _route_ws_message in BackpackAPI passes message["data"] to handler
        # for topic like "depth.SOL_USDC"
        # but for type "fills", it passes the *entire message* if type is used as topic
        # Let's check bp_api.py _route_ws_message: for type "fills", topic becomes "fills",
        # data_payload = message
        mock_fills_handler.assert_called_once_with(test_message)

    @pytest.mark.asyncio
    async def test_route_ws_message_no_handler(
        self, api_for_ws_tests: BackpackAPI, caplog: LogCaptureFixture
    ) -> None:
        test_message = {"topic": "unhandled.topic", "data": {"key": "value"}}

        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001

        assert "No handler registered for topic: unhandled.topic" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_no_topic_or_type(
        self, api_for_ws_tests: BackpackAPI, caplog: LogCaptureFixture
    ) -> None:
        test_message = {
            "event": "random_event",
            "data": {"key": "value"},
        }  # No 'topic' or known 'type'

        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001

        assert "Received unroutable message (no clear string topic/type)" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_topic_no_data(
        self, api_for_ws_tests: BackpackAPI, caplog: LogCaptureFixture
    ) -> None:
        mock_handler = AsyncMock()
        topic = "data_missing.topic"
        if api_for_ws_tests._ws_manager:  # noqa: SLF001
            api_for_ws_tests._ws_manager.send_json = AsyncMock(return_value=True)  # noqa: SLF001
            api_for_ws_tests._ws_manager.is_connected = True  # noqa: SLF001
        await api_for_ws_tests.subscribe(topic, mock_handler)

        test_message = {"topic": topic, "action": "update"}  # Missing "data" field

        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001

        mock_handler.assert_not_called()
        assert f"Received message with topic '{topic}' but no data" in caplog.text
