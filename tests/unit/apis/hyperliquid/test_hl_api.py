"""
Unit tests for the HyperliquidAPI class, focusing on authenticator integration.
"""

from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _pytest.logging import LogCaptureFixture

from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.connectivity.http_client import HttpRequestFailedError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce

# Constants for testing
TEST_WALLET_ADDRESS = "0xTestWalletAddress000000000000000000000000"
TEST_PRIVATE_KEY = "0xTestPrivateKey00000000000000000000000000000000000000000000000"
TEST_CHAIN_ID = 1337

BASE_API_CONFIG = {
    "rest_endpoint": "https://api.hyperliquid.xyz",
    "ws_endpoint": "wss://api.hyperliquid.xyz/ws",
    "rate_limits": {  # Example, content doesn't matter much for these tests
        "default": {"rate": 10, "bucket_size": 10},
        "endpoints": {"POST /exchange": {"rate": 5, "bucket_size": 5}},
    },
}

SECRETS_WITH_KEY: dict[str, str | None] = {
    "wallet_address": TEST_WALLET_ADDRESS,
    "private_key": TEST_PRIVATE_KEY,
}

SECRETS_NO_KEY: dict[str, str | None] = {
    "wallet_address": TEST_WALLET_ADDRESS,
    "private_key": None,
}

SECRETS_NO_ADDRESS: dict[str, str | None] = {
    "wallet_address": None,
    "private_key": TEST_PRIVATE_KEY,
}


@pytest.fixture
def mock_hl_auth_init() -> Generator[tuple[MagicMock, MagicMock], Any]:
    """Mocks the HyperliquidEip712Authenticator initialization."""
    with patch(
        "cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"
    ) as mock_auth_class:
        mock_instance = MagicMock(spec=HyperliquidEip712Authenticator)
        mock_instance.prepare_request = AsyncMock()  # Add async mock for prepare_request
        mock_auth_class.return_value = mock_instance
        yield mock_auth_class, mock_instance  # Return class and instance mock


# --- Initialization Tests --- #


def test_hl_api_init_with_key(
    mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any],
) -> None:
    """Test successful initialization when private key is provided."""
    mock_auth_class, mock_instance = next(mock_hl_auth_init)
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    mock_auth_class.assert_called_once_with(
        private_key_hex=TEST_PRIVATE_KEY,
        wallet_address=TEST_WALLET_ADDRESS,
        chain_id=HyperliquidAPI.CHAIN_ID,
    )
    assert api.authenticator is mock_instance
    assert api._hl_authenticator is mock_instance  # noqa: SLF001


def test_hl_api_init_without_key(
    mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any],
) -> None:
    """Test initialization when private key is None."""
    mock_auth_class, _ = next(mock_hl_auth_init)
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_NO_KEY)

    mock_auth_class.assert_not_called()
    assert api.authenticator is None
    assert api._hl_authenticator is None  # noqa: SLF001


def test_hl_api_init_auth_init_fails(
    mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any], caplog: LogCaptureFixture
) -> None:
    """Test initialization when HyperliquidEip712Authenticator fails to initialize."""
    mock_auth_class, _ = next(mock_hl_auth_init)
    mock_auth_class.side_effect = ValueError("Bad key format")

    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    mock_auth_class.assert_called_once()  # Still attempted
    assert api.authenticator is None
    assert api._hl_authenticator is None  # noqa: SLF001
    assert "Failed to init HL authenticator: Bad key format" in caplog.text


def test_hl_api_init_no_address(
    mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any], caplog: LogCaptureFixture
) -> None:
    """Test initialization logs error if wallet address is missing."""
    mock_auth_class, _ = next(mock_hl_auth_init)
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_NO_ADDRESS)

    mock_auth_class.assert_not_called()  # Authenticator shouldn't be called without address
    assert api.authenticator is None
    assert api._hl_authenticator is None  # noqa: SLF001
    assert "HLAPI: Wallet address required" in caplog.text


# --- _authenticate Method Tests --- #


@pytest.mark.asyncio
async def test_authenticate_success(
    mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any],
) -> None:
    """Test successful call to _authenticate delegates to authenticator."""
    _, mock_instance = next(mock_hl_auth_init)
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    method = "POST"
    path = "/exchange"
    params = {"p": 1}
    data = {"d": 2}
    expected_components = AuthenticatedRequestComponents(
        headers={"X-HL-Signature": "sig123"}, params=params, data=data
    )
    mock_instance.prepare_request.return_value = expected_components

    result = await api._authenticate(method, path, params, data)  # noqa: SLF001

    mock_instance.prepare_request.assert_awaited_once_with(
        method, path, params, data, api.default_headers.copy()
    )
    # Verify returned dict structure matches what current _request expects
    assert result == {
        "headers": expected_components["headers"],
        "params": expected_components["params"],
        "data": expected_components["data"],
    }


@pytest.mark.asyncio
async def test_authenticate_no_authenticator(
    mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any],
) -> None:
    """Test _authenticate raises APIError if no authenticator is configured."""
    _, _ = next(mock_hl_auth_init)
    # Initialize without key so authenticator is None
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_NO_KEY)
    assert api.authenticator is None

    with pytest.raises(APIError, match="HL authenticator not initialized") as excinfo:
        await api._authenticate("POST", "/exchange", None, {"d": 1})  # noqa: SLF001
    assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value


@pytest.mark.asyncio
async def test_authenticate_prepare_request_fails(
    mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any],
) -> None:
    """Test _authenticate propagates APIError from prepare_request."""
    _, mock_instance = next(mock_hl_auth_init)
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    mock_instance.prepare_request.side_effect = APIError(
        "Signing failed internally", code=APIErrorCode.AUTHENTICATION_FAILED.value
    )

    with pytest.raises(APIError, match="Signing failed internally") as excinfo:
        await api._authenticate("POST", "/exchange", None, {"d": 1})  # noqa: SLF001
    assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value


# --- Signed Endpoint Test Example (place_order) --- #


@pytest.mark.asyncio
@patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAPI._request", new_callable=AsyncMock)
async def test_place_order_calls_authenticate_and_request(
    mock_request: AsyncMock, mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any]
) -> None:
    """Verify place_order uses the authenticator flow."""
    _, mock_instance = next(mock_hl_auth_init)
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    with patch.object(api, "_get_asset_index", new_callable=AsyncMock) as mock_get_index:
        mock_get_index.return_value = 0

        mock_order_response = {
            "status": "ok",
            "response": {"type": "order", "data": {"statuses": [{"resting": {"oid": 12345}}]}},
        }
        mock_request.return_value = mock_order_response

        mock_final_order = MagicMock()
        mock_final_order.exchange_order_id = "12345"
        with patch.object(api, "get_order_status", new_callable=AsyncMock) as mock_get_status:
            mock_get_status.return_value = mock_final_order

            auth_headers = {"X-HL-Signature": "sig123", "X-HL-Timestamp": "ts", "X-HL-Nonce": "1"}
            auth_params = None
            order_action_data = {
                "type": "order",
                "actions": [
                    {
                        "asset": 0,
                        "isBuy": True,
                        "sz": "1.0",
                        "limitPx": "30000",
                        "orderType": {"limit": {"tif": "Gtc"}},
                        "reduceOnly": False,
                    }
                ],
            }
            auth_data = order_action_data

            expected_components = AuthenticatedRequestComponents(
                headers=auth_headers, params=auth_params, data=auth_data
            )
            mock_instance.prepare_request.return_value = expected_components

            from decimal import Decimal

            final_order = await api.place_order(
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1.0"),
                price=Decimal("30000"),
                time_in_force=TimeInForce.GTC,
            )

            mock_instance.prepare_request.assert_awaited_once()
            # Unpack only call_args as call_kwargs is not used
            call_args = mock_instance.prepare_request.call_args[0]
            # call_args, _ = mock_instance.prepare_request.call_args # Alternative unpacking
            assert call_args[0] == "POST"
            assert call_args[1] == "/exchange"
            assert call_args[2] is None
            assert call_args[3]["type"] == "order"
            assert len(call_args[3]["actions"]) == 1
            # More detailed checks for action payload can be added if necessary

            mock_request.assert_awaited_once_with(
                method="POST", endpoint="/exchange", data=auth_data, is_signed=True
            )
            assert final_order is mock_final_order


class TestHyperliquidAPIMethodErrors:
    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAPI._request", new_callable=AsyncMock)
    async def test_get_ticker_handles_mapped_http_error(
        self,
        mock_hl_request: AsyncMock,
        mock_hl_auth_init: Any,  # Corrected type to Any for fixture
    ) -> None:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
        # api.error_mapper is HyperliquidErrorMapper by default

        http_status_from_exchange = 503
        error_body_from_exchange = "Service Unavailable - Gateway Error"

        http_failure = HttpRequestFailedError(
            message=f"HTTP {http_status_from_exchange} Error",
            http_status_code=http_status_from_exchange,
            response_body=error_body_from_exchange,
        )
        mock_hl_request.side_effect = http_failure

        with pytest.raises(APIError) as exc_info:
            await api.get_ticker(
                symbol="BTC"
            )  # get_ticker uses /info which might be mocked by _request

        assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
        assert exc_info.value.http_status == http_status_from_exchange
        assert "Service Unavailable" in exc_info.value.message  # Mapper might generalize
        assert exc_info.value.exchange_message == error_body_from_exchange

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAPI._request", new_callable=AsyncMock)
    @patch(
        "cyberdelta.apis.hyperliquid.hl_api.HyperliquidAPI._get_asset_index", new_callable=AsyncMock
    )
    async def test_place_order_handles_hl_string_error_in_response(
        self, mock_get_asset_index: AsyncMock, mock_hl_request: AsyncMock, mock_hl_auth_init: Any
    ) -> None:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
        mock_get_asset_index.return_value = 0  # Mock asset index

        # Simulate Hyperliquid returning 200 OK but with an error string in the response body
        # This is a common HL pattern that place_order needs to handle internally
        error_string_from_hl = "User has insufficient margin"
        mock_hl_response_with_internal_error = {
            "status": "ok",  # Note: status is ok, but error is in statuses[0]
            "response": {
                "type": "order",
                "data": {"statuses": [error_string_from_hl]},  # HL error string
            },
        }
        mock_hl_request.return_value = mock_hl_response_with_internal_error

        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="ETH",
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=Decimal("1"),
                time_in_force=TimeInForce.IOC,
            )

        # place_order should detect this error string and use the mapper
        assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value
        # HTTP status would be 200 as per HL's response, but the semantic error is critical
        assert exc_info.value.http_status == 200
        assert "Insufficient margin" in exc_info.value.message
        assert exc_info.value.exchange_message == error_string_from_hl

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAPI._request", new_callable=AsyncMock)
    @patch(
        "cyberdelta.apis.hyperliquid.hl_api.HyperliquidAPI._get_asset_index", new_callable=AsyncMock
    )
    async def test_place_order_handles_hl_error_object_in_response(
        self, mock_get_asset_index: AsyncMock, mock_hl_request: AsyncMock, mock_hl_auth_init: Any
    ) -> None:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
        mock_get_asset_index.return_value = 1

        error_message_from_hl = "Order size too small"
        mock_hl_response_with_error_obj = {
            "status": "ok",
            "response": {
                "type": "order",
                "data": {"statuses": [{"error": error_message_from_hl}]},  # HL error object
            },
        }
        mock_hl_request.return_value = mock_hl_response_with_error_obj

        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="ARB",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.001"),
                price=Decimal("1"),
                time_in_force=TimeInForce.GTC,
            )

        assert exc_info.value.code == APIErrorCode.INVALID_ORDER_SIZE.value
        assert exc_info.value.http_status == 200
        assert "Order size too small" in exc_info.value.message
        assert exc_info.value.exchange_message == error_message_from_hl


class TestHyperliquidAPIWebSocketRouting:
    @pytest.fixture
    def api_for_ws_tests(self, mock_hl_auth_init: Any) -> HyperliquidAPI:
        # mock_hl_auth_init ensures authenticator is mocked if needed
        # We are primarily testing routing, not live connection
        with patch("cyberdelta.apis.base.exchange_api.WebSocketManager") as mock_ws_mgr_class:
            mock_ws_mgr_instance = MagicMock()
            mock_ws_mgr_class.return_value = mock_ws_mgr_instance
            # Provide a wallet address for userEvents subscription testing
            secrets_with_addr = SECRETS_WITH_KEY.copy()
            api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=secrets_with_addr)
            api._ws_manager = mock_ws_mgr_instance  # noqa: SLF001 - for testing
            return api

    @pytest.mark.parametrize(
        "topic, expected_sub_details",
        [
            ("l2Book:BTC", {"type": "l2Book", "coin": "BTC"}),
            ("trades:ETH", {"type": "trades", "coin": "ETH"}),
            ("userEvents", {"type": "userEvents", "user": TEST_WALLET_ADDRESS}),
            ("candle:SOL:1m", {"type": "candle", "coin": "SOL", "interval": "1m"}),
        ],
    )
    def test_construct_subscription_payload_valid_topics(
        self, api_for_ws_tests: HyperliquidAPI, topic: str, expected_sub_details: dict[str, Any]
    ):
        payload = api_for_ws_tests._construct_subscription_payload(topic)  # noqa: SLF001
        assert payload == {"method": "subscribe", "subscription": expected_sub_details}

    def test_construct_subscription_payload_invalid_topic(self, api_for_ws_tests: HyperliquidAPI):
        payload = api_for_ws_tests._construct_subscription_payload("invalidTopicFormat")  # noqa: SLF001
        assert payload is None

    def test_construct_subscription_payload_user_event_no_address(self, mock_hl_auth_init: Any):
        # Test userEvents subscription when API is initialized without wallet address
        with patch(
            "cyberdelta.apis.base.exchange_api.WebSocketManager"
        ):  # Mock WS manager for init
            api_no_addr = HyperliquidAPI(
                api_config=BASE_API_CONFIG, secrets=SECRETS_NO_KEY
            )  # No address
            # Manually set _wallet_address to None to be sure for this test case
            api_no_addr._wallet_address = None  # noqa: SLF001
            payload = api_no_addr._construct_subscription_payload("userEvents")  # noqa: SLF001
            assert payload is None

    @pytest.mark.asyncio
    async def test_route_ws_message_known_channel(self, api_for_ws_tests: HyperliquidAPI):
        mock_l2book_handler = AsyncMock()
        channel = "l2Book"
        # For Hyperliquid, the subscription topic like "l2Book:BTC" maps to a handler keyed by "l2Book"
        # This means api.subscribe("l2Book:BTC", handler) would internally register handler for "l2Book"
        # if the routing logic derives channel from topic. Current HLAPI._route_ws_message uses message["channel"]
        # So, handler should be registered with the channel name.
        # Let's assume subscribe sets up self._ws_handlers with the channel name directly or via mapping.
        # For this test, directly populate _ws_handlers for simplicity of testing routing.
        api_for_ws_tests._ws_handlers[channel] = mock_l2book_handler  # noqa: SLF001

        test_data_payload = {"coin": "BTC", "levels": [[], []]}
        test_message = {"channel": channel, "data": test_data_payload}

        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001

        mock_l2book_handler.assert_called_once_with(test_data_payload)

    @pytest.mark.asyncio
    async def test_route_ws_message_pong(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ):
        test_message = {"channel": "pong"}  # Pong messages might not have 'data'
        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001
        assert "Received pong" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_error_channel(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ):
        error_payload = "Something went wrong on WS"
        test_message = {"channel": "error", "data": error_payload}
        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001
        assert f"Received WS error message: {error_payload}" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_subscription_response(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ):
        sub_response_payload = {"type": "l2Book", "coin": "ETH"}
        test_message = {"channel": "subscriptionResponse", "data": sub_response_payload}
        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001
        assert f"Received subscription response: {sub_response_payload}" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_no_handler(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ):
        test_message = {"channel": "unknownChannel", "data": {"info": "payload"}}
        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001
        assert "No handler registered for channel: unknownChannel" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_no_channel(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ):
        test_message = {"some_other_key": "value", "data": {"info": "payload"}}
        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001
        assert "Received WS message without channel" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_channel_no_data(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ):
        # This scenario (channel present but no data) is unlikely for most HL messages but good to test.
        # The _route_ws_message has a check for data_payload is None.
        channel_name = "someChannelWithNoData"
        api_for_ws_tests._ws_handlers[channel_name] = (
            AsyncMock()
        )  # Register a handler # noqa: SLF001
        test_message = {"channel": channel_name}  # No "data" field

        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001

        assert f"Received message on channel '{channel_name}' but no data" in caplog.text
        api_for_ws_tests._ws_handlers[channel_name].assert_not_called()  # noqa: SLF001


from decimal import (
    Decimal,  # Already imported earlier, but ensures visibility if needed by pytest execution
)
