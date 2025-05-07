"""
Unit tests for the HyperliquidAPI client implementation.
"""

import logging
from collections.abc import Generator
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _pytest.logging import LogCaptureFixture
from multidict import CIMultiDictProxy

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
TEST_API_KEY = "test_key"

BASE_API_CONFIG = {
    "rest_endpoint": "https://api.hyperliquid.xyz",
    "ws_endpoint": "wss://api.hyperliquid.xyz/ws",
    "rate_limits": {
        "default_rate": 10,
        "default_bucket_size": 10,
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
def mock_hl_auth_init() -> Generator[tuple[MagicMock, MagicMock], Any]:  # noqa: ANN401
    """Mocks the HyperliquidEip712Authenticator initialization."""
    with patch(
        "cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"
    ) as mock_auth_class:
        mock_instance = MagicMock(spec=HyperliquidEip712Authenticator)
        mock_instance.prepare_request = AsyncMock()
        mock_auth_class.return_value = mock_instance
        yield mock_auth_class, mock_instance


# --- Initialization Tests --- #


def test_hl_api_init_with_key(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Test successful initialization when private key is provided."""
    mock_auth_class, mock_instance = mock_hl_auth_init
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    mock_auth_class.assert_called_once_with(
        private_key_hex=TEST_PRIVATE_KEY,
        wallet_address=TEST_WALLET_ADDRESS,
        chain_id=HyperliquidAPI.CHAIN_ID,
    )
    assert api.authenticator is mock_instance
    assert api._hl_authenticator is mock_instance  # noqa: SLF001 - Test verification of internal state


def test_hl_api_init_without_key(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Test initialization when private key is None."""
    mock_auth_class, _ = mock_hl_auth_init
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_NO_KEY)

    mock_auth_class.assert_not_called()
    assert api.authenticator is None
    assert api._hl_authenticator is None  # noqa: SLF001 - Test verification of internal state


def test_hl_api_init_auth_init_fails(
    mock_hl_auth_init: tuple[MagicMock, MagicMock], caplog: LogCaptureFixture
) -> None:
    """Test initialization when HyperliquidEip712Authenticator fails to initialize."""
    mock_auth_class, _ = mock_hl_auth_init
    mock_auth_class.side_effect = ValueError("Bad key format")

    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    mock_auth_class.assert_called_once()  # Still attempted
    assert api.authenticator is None
    assert api._hl_authenticator is None  # noqa: SLF001 - Test verification of internal state
    assert "Failed to init HL authenticator: Bad key format" in caplog.text


def test_hl_api_init_no_address(
    mock_hl_auth_init: tuple[MagicMock, MagicMock], caplog: LogCaptureFixture
) -> None:
    """Test initialization logs error if wallet address is missing."""
    mock_auth_class, _ = mock_hl_auth_init
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_NO_ADDRESS)

    mock_auth_class.assert_not_called()  # Authenticator shouldn't be called without address
    assert api.authenticator is None
    assert api._hl_authenticator is None  # noqa: SLF001 - Test verification of internal state
    assert "HLAPI: Wallet address required" in caplog.text


# --- _authenticate Method Tests --- #


@pytest.mark.asyncio
async def test_authenticate_success(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Test successful call to _authenticate delegates to authenticator."""
    _, mock_instance = mock_hl_auth_init
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    method = "POST"
    path = "/exchange"
    params = {"p": 1}
    data = {"d": 2}
    expected_components = AuthenticatedRequestComponents(
        headers={"X-HL-Signature": "sig123"}, params=params, data=data
    )
    mock_instance.prepare_request.return_value = expected_components

    result = await api._authenticate(method, path, params, data)  # noqa: SLF001 - Testing protected method directly

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
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Test _authenticate raises APIError if no authenticator is configured."""
    _, _ = mock_hl_auth_init
    # Initialize without key so authenticator is None
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_NO_KEY)
    assert api.authenticator is None

    with pytest.raises(APIError, match="HL authenticator not initialized") as excinfo:
        await api._authenticate("POST", "/exchange", None, {"d": 1})  # noqa: SLF001 - Testing protected method directly
    assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value


@pytest.mark.asyncio
async def test_authenticate_prepare_request_fails(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Test _authenticate propagates APIError from prepare_request."""
    _, mock_instance = mock_hl_auth_init
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    mock_instance.prepare_request.side_effect = APIError(
        "Signing failed internally", code=APIErrorCode.AUTHENTICATION_FAILED.value
    )

    with pytest.raises(APIError, match="Signing failed internally") as excinfo:
        await api._authenticate("POST", "/exchange", None, {"d": 1})  # noqa: SLF001 - Testing protected method directly
    assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value


# --- Signed Endpoint Test Example (place_order) --- #


@pytest.mark.asyncio
# Removed top-level patch for HttpClient.request
async def test_place_order_calls_authenticate_and_request(
    # mock_http_client_request: AsyncMock, # No longer a param
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Verify place_order uses the authenticator flow including _authenticate."""
    # Create a mock authenticator instance FOR THIS TEST
    mock_auth_for_test = MagicMock(spec=HyperliquidEip712Authenticator)
    auth_headers = {"X-HL-Signature": "sig123", "X-HL-Timestamp": "ts", "X-HL-Nonce": "1"}
    auth_params = None
    expected_builder_payload = {  # Define here for clarity
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
    auth_prepared_components = AuthenticatedRequestComponents(
        headers=auth_headers, params=auth_params, data=expected_builder_payload
    )
    # Make prepare_request an AsyncMock *on the instance*
    mock_auth_for_test.prepare_request = AsyncMock(return_value=auth_prepared_components)

    # Expected response content and headers tuple returned by _request
    mock_http_response_content = {
        "status": "ok",
        "data": {"type": "order", "statuses": [{"resting": {"oid": 12345}}]},
    }
    mock_response_headers = MagicMock(spec=CIMultiDictProxy)

    # Patch the Authenticator constructor within hl_api module scope
    with patch(
        "cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator",
        return_value=mock_auth_for_test,
    ) as mock_auth_constructor:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
        # Assert API instance uses our mock authenticator
        mock_auth_constructor.assert_called_once_with(
            private_key_hex=TEST_PRIVATE_KEY,
            wallet_address=TEST_WALLET_ADDRESS,
            chain_id=HyperliquidAPI.CHAIN_ID,
        )
        assert api.authenticator is mock_auth_for_test
        assert api._hl_authenticator is mock_auth_for_test  # noqa: SLF001

        # Define a side effect for the mocked _request
        async def mock_request_side_effect(*args, **kwargs):
            # Simulate the internal call to prepare_request
            if kwargs.get("is_signed") is True and api.authenticator:
                await api.authenticator.prepare_request(
                    method=kwargs.get("method", args[0] if args else None),
                    path=kwargs.get(
                        "endpoint", args[1] if len(args) > 1 else None
                    ),  # Base _request uses 'endpoint'
                    params=kwargs.get("params"),
                    data=kwargs.get("data"),
                    headers=dict(api.default_headers),  # Simulate passing headers
                )
            # Return ONLY the expected content, matching ExchangeAPI._request signature
            return mock_http_response_content  # NOT the tuple

        # Patch the _request method on the API instance
        with patch.object(
            api, "_request", side_effect=mock_request_side_effect, spec=True
        ) as mock_api_request:
            with patch.object(api, "_get_asset_index", new_callable=AsyncMock) as mock_get_index:
                mock_get_index.return_value = 0  # Asset index for BTC

                # Patch the HyperliquidRequestBuilder.build_place_order_payload
                with patch(
                    "cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder.build_place_order_payload"
                ) as mock_build_payload:
                    # Define parameters for the place_order call
                    symbol_val: str = "BTC"
                    side_val: OrderSide = OrderSide.BUY
                    order_type_val: OrderType = OrderType.LIMIT
                    quantity_val: Decimal = Decimal("1.0")
                    price_val: Decimal = Decimal("30000")
                    time_in_force_val: TimeInForce = TimeInForce.GTC
                    client_order_id_val: str | None = None
                    reduce_only_val: bool = False
                    post_only_val: bool = False
                    stop_price_val: Decimal | None = None

                    mock_build_payload.return_value = expected_builder_payload

                    mock_final_order = MagicMock()
                    mock_final_order.exchange_order_id = "12345"
                    with patch.object(
                        api, "get_order_status", new_callable=AsyncMock
                    ) as mock_get_status:
                        mock_get_status.return_value = mock_final_order

                        final_order = await api.place_order(
                            symbol=symbol_val,
                            side=side_val,
                            order_type=order_type_val,
                            quantity=quantity_val,
                            price=price_val,
                            time_in_force=time_in_force_val,
                            client_order_id=client_order_id_val,
                            reduce_only=reduce_only_val,
                            post_only=post_only_val,
                            stop_price=stop_price_val,
                        )

                    # Verify builder was called correctly
                    mock_build_payload.assert_called_once_with(
                        asset_index=0,
                        side=side_val,
                        order_type=order_type_val,
                        quantity=quantity_val,
                        time_in_force=time_in_force_val,
                        price=price_val,
                        stop_price=None,
                        client_order_id=None,
                        reduce_only=False,
                        post_only=False,
                    )

                    # Verify prepare_request (on our specific mock instance) was awaited via the side_effect
                    mock_auth_for_test.prepare_request.assert_awaited_once()

                    # Verify _request itself was called correctly by place_order
                    mock_api_request.assert_awaited_once_with(
                        method="POST",
                        endpoint="/exchange",  # _request expects endpoint, not endpoint_path
                        data=expected_builder_payload,
                        is_signed=True,
                    )

                    assert final_order == mock_final_order

    # --- END OF PRE-SETUP PATCH CONTEXT --- #


class TestHyperliquidAPIMethodErrors:
    @pytest.mark.asyncio
    @patch("cyberdelta.apis.base.exchange_api.HttpClient.request", new_callable=AsyncMock)
    async def test_get_ticker_handles_mapped_http_error(
        self,
        mock_http_client_request: AsyncMock,
        mock_hl_auth_init: tuple[MagicMock, MagicMock],
    ) -> None:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
        # api.error_mapper is HyperliquidErrorMapper by default

        http_status_from_exchange = 503
        error_body_from_exchange = "Service Unavailable - Gateway Error"

        http_failure = HttpRequestFailedError(
            message=f"HTTP {http_status_from_exchange} Error from HttpClient",
            http_status_code=http_status_from_exchange,
            response_body=error_body_from_exchange,
        )
        mock_http_client_request.side_effect = http_failure

        with pytest.raises(APIError) as exc_info:
            await api.get_ticker(symbol="BTC")

        assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
        assert exc_info.value.http_status == http_status_from_exchange
        assert exc_info.value.message == (error_body_from_exchange or "Service Unavailable (503)")
        assert exc_info.value.exchange_message == error_body_from_exchange

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAPI._request", new_callable=AsyncMock)
    @patch(
        "cyberdelta.apis.hyperliquid.hl_api.HyperliquidAPI._get_asset_index", new_callable=AsyncMock
    )
    async def test_place_order_handles_hl_string_error_in_response(
        self,
        mock_get_asset_index: AsyncMock,
        mock_hl_request: AsyncMock,
        mock_hl_auth_init: tuple[MagicMock, MagicMock],
    ) -> None:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
        mock_get_asset_index.return_value = 0  # Mock asset index

        error_string_from_hl = "User has insufficient margin"
        mock_hl_response_with_internal_error = {
            "status": "ok",
            "data": {"type": "order", "statuses": [error_string_from_hl]},
        }
        mock_hl_request.return_value = mock_hl_response_with_internal_error

        # Patch the HyperliquidRequestBuilder.build_place_order_payload
        with patch(
            "cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder.build_place_order_payload"
        ) as mock_build_payload:
            # Define parameters for the place_order call
            symbol_val: str = "ETH"
            side_val: OrderSide = OrderSide.SELL
            order_type_val: OrderType = OrderType.MARKET
            quantity_val: Decimal = Decimal("1")
            price_val: Decimal | None = None  # Market order
            time_in_force_val: TimeInForce = TimeInForce.IOC
            # These are the specific args for the builder for this test case
            # expected_builder_args = { # Commented out as unused
            #     "symbol": symbol_val,
            #     "side": side_val,
            #     "order_type": order_type_val,
            # }
            # This is the payload the builder would create for the _request method
            expected_builder_payload = {
                "type": "order",
                "actions": [
                    {
                        "asset": 0,  # From mock_get_asset_index
                        "isBuy": False,  # Sell
                        "sz": "1",
                        "limitPx": "0",  # Standard for market orders if required by API
                        "orderType": {"ioc": None},  # IOC has no tif value in HL struct
                        "reduceOnly": False,
                    }
                ],
            }
            mock_build_payload.return_value = expected_builder_payload

            with pytest.raises(APIError) as exc_info:
                await api.place_order(
                    symbol=symbol_val,
                    side=side_val,
                    order_type=order_type_val,
                    quantity=quantity_val,
                    price=price_val,  # For market, this might be None or not passed
                    time_in_force=time_in_force_val,
                )

            # Verify builder was called correctly
            mock_build_payload.assert_called_once_with(
                asset_index=0,
                side=side_val,
                order_type=order_type_val,
                quantity=quantity_val,
                time_in_force=time_in_force_val,
                price=price_val,
                stop_price=None,
                client_order_id=None,
                reduce_only=False,
                post_only=False,
            )
            # Verify api._request was called with the payload from the builder
            mock_hl_request.assert_awaited_once_with(
                "POST", "/exchange", data=expected_builder_payload, is_signed=True
            )

        # place_order should detect this error string and use the mapper
        assert exc_info.value.code == APIErrorCode.INSUFFICIENT_FUNDS.value
        # HTTP status would be 200 as per HL's response, but the semantic error is critical
        assert exc_info.value.http_status == 200
        assert exc_info.value.message == error_string_from_hl
        assert exc_info.value.exchange_message == error_string_from_hl

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAPI._request", new_callable=AsyncMock)
    @patch(
        "cyberdelta.apis.hyperliquid.hl_api.HyperliquidAPI._get_asset_index", new_callable=AsyncMock
    )
    async def test_place_order_handles_hl_error_object_in_response(
        self,
        mock_get_asset_index: AsyncMock,
        mock_hl_request: AsyncMock,
        mock_hl_auth_init: tuple[MagicMock, MagicMock],
    ) -> None:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
        mock_get_asset_index.return_value = 1  # Mock asset index

        error_message_from_hl = "Order size too small"
        mock_hl_response_with_error_obj = {
            "status": "ok",
            "data": {"type": "order", "statuses": [{"error": error_message_from_hl}]},
        }
        mock_hl_request.return_value = mock_hl_response_with_error_obj

        # Patch the HyperliquidRequestBuilder.build_place_order_payload
        with patch(
            "cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder.build_place_order_payload"
        ) as mock_build_payload:
            # Define parameters for the place_order call
            symbol_val: str = "ARB"
            side_val: OrderSide = OrderSide.BUY
            order_type_val: OrderType = OrderType.LIMIT
            quantity_val: Decimal = Decimal("0.001")
            price_val: Decimal = Decimal("1")
            time_in_force_val: TimeInForce = TimeInForce.GTC
            # These are the specific args for the builder for this test case
            # expected_builder_args = { # Commented out as unused
            #     "symbol": symbol_val,
            #     "side": side_val,
            #     "order_type": order_type_val,
            # }
            # This is the payload the builder would create for the _request method
            expected_builder_payload = {
                "type": "order",
                "actions": [
                    {
                        "asset": 1,  # From mock_get_asset_index
                        "isBuy": True,
                        "sz": "0.001",
                        "limitPx": "1",
                        "orderType": {"limit": {"tif": "Gtc"}},
                        "reduceOnly": False,
                    }
                ],
            }
            mock_build_payload.return_value = expected_builder_payload

            with pytest.raises(APIError) as exc_info:
                await api.place_order(
                    symbol=symbol_val,
                    side=side_val,
                    order_type=order_type_val,
                    quantity=quantity_val,
                    price=price_val,
                    time_in_force=time_in_force_val,
                )

            # Verify builder was called correctly
            mock_build_payload.assert_called_once_with(
                asset_index=1,
                side=side_val,
                order_type=order_type_val,
                quantity=quantity_val,
                time_in_force=time_in_force_val,
                price=price_val,
                stop_price=None,
                client_order_id=None,
                reduce_only=False,
                post_only=False,
            )
            # Verify api._request was called with the payload from the builder
            mock_hl_request.assert_awaited_once_with(
                "POST", "/exchange", data=expected_builder_payload, is_signed=True
            )

        assert exc_info.value.code == APIErrorCode.INVALID_ORDER_SIZE.value
        assert exc_info.value.http_status == 200
        assert "Order size too small" in exc_info.value.message
        assert exc_info.value.exchange_message == error_message_from_hl


class TestHyperliquidAPIWebSocketRouting:
    @pytest.fixture
    def api_for_ws_tests(self, mock_hl_auth_init: tuple[MagicMock, MagicMock]) -> HyperliquidAPI:
        # mock_hl_auth_init ensures authenticator is mocked if needed
        # We are primarily testing routing, not live connection
        with patch("cyberdelta.apis.base.exchange_api.WebSocketManager") as mock_ws_mgr_class:
            mock_ws_mgr_instance = MagicMock()
            mock_ws_mgr_class.return_value = mock_ws_mgr_instance
            # Provide a wallet address for userEvents subscription testing
            secrets_with_addr = SECRETS_WITH_KEY.copy()
            api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=secrets_with_addr)
            api._ws_manager = mock_ws_mgr_instance  # noqa: SLF001 - Setting internal state for test isolation
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
    ) -> None:
        payload = api_for_ws_tests._construct_subscription_payload(topic)  # noqa: SLF001 - Testing protected method directly
        assert payload is not None
        assert payload["method"] == "subscribe"
        assert payload["subscription"] == expected_sub_details

    def test_construct_subscription_payload_invalid_topic(
        self, api_for_ws_tests: HyperliquidAPI
    ) -> None:
        payload = api_for_ws_tests._construct_subscription_payload("invalidTopicFormat")  # noqa: SLF001 - Testing protected method directly
        assert payload is None

    def test_construct_subscription_payload_user_event_no_address(
        self, mock_hl_auth_init: tuple[MagicMock, MagicMock]
    ) -> None:
        # Test userEvents subscription when API is initialized without wallet address
        with patch.object(
            HyperliquidAPI, "__init__", return_value=None
        ):  # Patch __init__ to control instance state
            api_no_addr = HyperliquidAPI(
                BASE_API_CONFIG, SECRETS_NO_ADDRESS
            )  # This won't run real init
            api_no_addr._wallet_address = None  # noqa: SLF001 - Setting internal state for specific test case
            api_no_addr.exchange_name = "hyperliquid"  # Manually set for logger

            payload = api_no_addr._construct_subscription_payload("userEvents")  # noqa: SLF001 - Testing protected method with altered state
            assert payload is None

    @pytest.mark.asyncio
    async def test_route_ws_message_known_channel(self, api_for_ws_tests: HyperliquidAPI) -> None:
        mock_handler: AsyncMock = AsyncMock()
        channel_name = "l2Book"
        api_for_ws_tests._ws_handlers[channel_name] = mock_handler  # noqa: SLF001 - Manipulating internal state for test setup

        test_data_payload: dict[str, Any] = {
            "coin": "BTC",
            "levels": [[], []],  # bids, asks
            "time": 1234567890,
        }
        test_message: dict[str, Any] = {"channel": channel_name, "data": test_data_payload}

        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001 - Testing protected method directly
        mock_handler.assert_awaited_once_with(test_data_payload, test_message)

    @pytest.mark.asyncio
    async def test_route_ws_message_pong(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        test_message = {"channel": "pong"}
        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001 - Testing protected method directly
        assert "Received pong" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_error_channel(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        error_payload = {"error": "Subscription failed", "reason": "Invalid coin"}
        test_message = {"channel": "error", "data": error_payload}
        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001 - Testing protected method directly
        assert f"Received WS error message: {error_payload}" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_subscription_response(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.INFO, logger="cyberdelta.apis.hyperliquid.hl_api")
        response_payload = {"subscription": {"type": "l2Book", "coin": "ETH"}, "status": "ok"}
        test_message = {"channel": "subscriptionResponse", "data": response_payload}
        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001 - Testing protected method directly
        assert "Received subscription response:" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_no_handler(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        test_message = {"channel": "unknownChannel", "data": {"some": "payload"}}
        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001 - Testing protected method directly
        assert "No handler registered for channel: unknownChannel" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_no_channel(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        test_message = {"type": "someType", "data": {"other": "data"}}
        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001 - Testing protected method directly
        assert "Received WS message without channel" in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_channel_no_data(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        # This scenario (channel present but no data) is unlikely for most HL messages
        # but good to test. The _route_ws_message has a check for data_payload is None.
        mock_handler: AsyncMock = AsyncMock()
        channel_name = "dataCheckChannel"
        api_for_ws_tests._ws_handlers[channel_name] = mock_handler  # noqa: SLF001 - Manipulating internal state for test setup
        test_message: dict[str, Any] = {"channel": channel_name}  # No 'data' field

        await api_for_ws_tests._handle_websocket_message(test_message)  # noqa: SLF001 - Testing protected method directly
        mock_handler.assert_not_called()
        assert f"Received message on channel '{channel_name}' but no data" in caplog.text
