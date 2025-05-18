"""
Unit tests for the HyperliquidAPI client implementation.
"""

import logging
from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from _pytest.logging import LogCaptureFixture
from pydantic import ValidationError

from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.connectivity.http_client import HttpRequestFailedError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.hyperliquid.hl_mapper import (
    HyperliquidCandleMapper,
    HyperliquidMapper,
    HyperliquidOrderMapper,
)
from cyberdelta.apis.hyperliquid.hl_response_handler import RawJsonResponse
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import HyperliquidRawAssetCtx
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawOrder
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawLimitOrderTypeDetails,
    HyperliquidRawMarketOrderTypeDetails,
    HyperliquidRawOrderType,
    HyperliquidRawPlaceOrderAction,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawLeverage,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsPositionUpdateEvent,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import FundingRate, MarginAccountSummary
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.margin_account import HyperliquidMarginDetails
from cyberdelta.core.models.market.order import Order

# Define HL_API_PATH at the module level
HL_API_PATH = "cyberdelta.apis.hyperliquid.hl_api"

# Constants for testing
TEST_WALLET_ADDRESS = "0x0000000000000000000000000000000000000000"  # Corrected length (42 chars)
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


@pytest.mark.asyncio
async def test_hl_api_init_with_key(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Test successful initialization when private key is provided."""
    mock_auth_class, mock_instance = mock_hl_auth_init
    api = None
    try:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

        mock_auth_class.assert_called_once_with(
            wallet_private_key=TEST_PRIVATE_KEY,
            chain_id=HyperliquidAPI.CHAIN_ID,
        )
        assert api._authenticator is mock_instance
        assert isinstance(api._hl_mapper, HyperliquidMapper)
        assert isinstance(api._hl_order_mapper, HyperliquidOrderMapper)
        assert isinstance(api._hl_candle_mapper, HyperliquidCandleMapper)
    finally:
        if api:
            await api.close()


@pytest.mark.asyncio
async def test_hl_api_init_without_key(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Test initialization when private key is None."""
    mock_auth_class, _ = mock_hl_auth_init
    api = None
    try:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_NO_KEY)

        mock_auth_class.assert_not_called()
        assert api._authenticator is None
        assert api._hl_authenticator is None
        assert isinstance(api._hl_mapper, HyperliquidMapper)
        assert isinstance(api._hl_order_mapper, HyperliquidOrderMapper)
        assert isinstance(api._hl_candle_mapper, HyperliquidCandleMapper)
    finally:
        if api:
            await api.close()


@pytest.mark.asyncio
async def test_hl_api_init_auth_init_fails(
    mock_hl_auth_init: tuple[MagicMock, MagicMock], caplog: LogCaptureFixture
) -> None:
    """Test initialization when HyperliquidEip712Authenticator fails to initialize."""
    mock_auth_class, _ = mock_hl_auth_init
    mock_auth_class.side_effect = ValueError("Bad key format")
    api = None
    try:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

        mock_auth_class.assert_called_once()  # Still attempted
        assert api._authenticator is None
        assert api._hl_authenticator is None
        assert "Failed to init HL authenticator: Bad key format" in caplog.text
        assert isinstance(api._hl_mapper, HyperliquidMapper)
        assert isinstance(api._hl_order_mapper, HyperliquidOrderMapper)
        assert isinstance(api._hl_candle_mapper, HyperliquidCandleMapper)
    finally:
        if api:
            await api.close()


@pytest.mark.asyncio
async def test_hl_api_init_no_address(
    mock_hl_auth_init: tuple[MagicMock, MagicMock], caplog: LogCaptureFixture
) -> None:
    """Test initialization logs error if wallet address is missing."""
    mock_auth_class, _ = mock_hl_auth_init
    api = None
    try:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_NO_ADDRESS)

        mock_auth_class.assert_not_called()  # Authenticator shouldn't be called without address
        assert api._authenticator is None
        assert api._hl_authenticator is None
        assert "HLAPI: Wallet address required" in caplog.text
        assert isinstance(api._hl_mapper, HyperliquidMapper)
        assert isinstance(api._hl_order_mapper, HyperliquidOrderMapper)
        assert isinstance(api._hl_candle_mapper, HyperliquidCandleMapper)
    finally:
        if api:
            await api.close()


# --- _authenticate Method Tests --- #


@pytest.mark.asyncio
async def test_authenticate_success(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Test successful call to _authenticate delegates to authenticator."""
    _mock_auth_class, mock_auth_instance = mock_hl_auth_init
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_WITH_KEY)
    # Ensure the instance created by API init is replaced by our mock for this test
    api._authenticator = mock_auth_instance  # pyright: ignore[reportPrivateUsage]

    method = "POST"
    path = "/exchange"
    params = {"p": 1}
    data_payload = {"d": 2}
    expected_components = AuthenticatedRequestComponents(
        headers={"X-HL-Signature": "sig123"}, params=params, data=data_payload
    )
    mock_auth_instance.prepare_request.return_value = expected_components

    result = await api._authenticate(method, path, params, data_payload)  # pyright: ignore[reportPrivateUsage]

    mock_auth_instance.prepare_request.assert_awaited_once_with(
        method,
        path,
        params,
        data_payload,
        api.default_headers.copy(),
    )
    assert result["headers"] == expected_components["headers"]
    assert result["params"] == expected_components["params"]
    assert result["data"] == expected_components["data"]
    # No direct is_authenticated flag to check, success is implied by no exception
    # and correct delegation to authenticator.prepare_request


@pytest.mark.asyncio
async def test_authenticate_no_authenticator(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Test _authenticate raises APIError if no authenticator is configured."""
    _mock_auth_class, _mock_auth_instance = mock_hl_auth_init
    # Initialize without key so authenticator is None
    api = HyperliquidAPI(
        BASE_API_CONFIG, SECRETS_NO_KEY
    )  # SECRETS_NO_KEY ensures _authenticator is None
    assert api._authenticator is None  # pyright: ignore[reportPrivateUsage]

    with pytest.raises(APIError, match="HL authenticator not initialized") as excinfo:
        await api._authenticate("POST", "/exchange", None, {"d": 1})  # pyright: ignore[reportPrivateUsage]
    assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value


@pytest.mark.asyncio
async def test_authenticate_prepare_request_fails(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Test _authenticate propagates APIError from prepare_request."""
    _mock_auth_class, mock_auth_instance = mock_hl_auth_init
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_WITH_KEY)
    api._authenticator = mock_auth_instance  # pyright: ignore[reportPrivateUsage]

    mock_auth_instance.prepare_request.side_effect = APIError(
        "Signing failed internally", code=APIErrorCode.AUTHENTICATION_FAILED.value
    )

    with pytest.raises(APIError, match="Signing failed internally") as excinfo:
        await api._authenticate("POST", "/exchange", None, {"d": 1})  # pyright: ignore[reportPrivateUsage]
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
    # Define the data that the builder is expected to produce as a Pydantic model
    expected_action_payload = HyperliquidRawPlaceOrderAction(
        asset=0,
        isBuy=True,
        sz="1.0",
        limitPx="30000",
        orderType=HyperliquidRawOrderType(limit=HyperliquidRawLimitOrderTypeDetails(tif="Gtc")),
        reduceOnly=False,
        cloid=None,
        trigger=None,
    )
    expected_request_model = HyperliquidApiPlaceOrderRequest(
        type="order", actions=[expected_action_payload]
    )
    # This is what _request will receive after .model_dump()
    expected_data_for_request = expected_request_model.model_dump(by_alias=True, exclude_none=True)

    auth_prepared_components = AuthenticatedRequestComponents(
        headers=auth_headers, params=auth_params, data=expected_data_for_request
    )
    # Make prepare_request an AsyncMock *on the instance*
    mock_auth_for_test.prepare_request = AsyncMock(return_value=auth_prepared_components)

    # Expected response content returned by _request
    mock_http_response_content = {
        "status": "ok",
        "data": {"type": "order", "statuses": [{"resting": {"oid": 12345}}]},
    }
    # mock_response_headers = MagicMock(spec=CIMultiDictProxy) # F841 - Removed

    # Patch the Authenticator constructor within hl_api module scope
    with patch(
        "cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator",
        return_value=mock_auth_for_test,
    ) as mock_auth_constructor:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
        # Assert API instance uses our mock authenticator
        mock_auth_constructor.assert_called_once_with(
            wallet_private_key=TEST_PRIVATE_KEY,
            chain_id=HyperliquidAPI.CHAIN_ID,
        )
        assert api._authenticator is mock_auth_for_test

        # Define a side effect for the mocked _request
        async def mock_request_side_effect(
            *_args: Any,  # noqa: ANN401
            **kwargs: Any,  # noqa: ANN401
        ) -> dict[str, Any]:
            # When api._request("POST", "/exchange", data=..., is_signed=True) is called,
            # the side_effect receives _args = ("POST", "/exchange") and
            # kwargs = {"data": ..., "is_signed": True}.
            # `self` is not part of _args for a side_effect function.

            if kwargs.get("is_signed") is True and api._authenticator:
                actual_method_from_args = _args[0] if _args else None
                actual_path_from_args = _args[1] if len(_args) > 1 else None

                # Ensure actual_method_from_args and actual_path_from_args are strings
                if not isinstance(actual_method_from_args, str):
                    pytest.fail(
                        f"prepare_request: method from _args[0] not str: "
                        f"{actual_method_from_args=} ({type(actual_method_from_args)})"
                    )
                if not isinstance(actual_path_from_args, str):
                    pytest.fail(
                        f"prepare_request: path from _args[1] not str: "
                        f"{actual_path_from_args=} ({type(actual_path_from_args)})"
                    )

                # The data passed to prepare_request should be the Pydantic model's dump
                # Use mock_auth_for_test as api._authenticator points to it in this test context
                await mock_auth_for_test.prepare_request(
                    method=actual_method_from_args,
                    path=actual_path_from_args,
                    params=kwargs.get("params"),
                    data=kwargs.get("data"),  # This data is already model_dumped by api.place_order
                    headers=dict(api.default_headers),
                )
            return mock_http_response_content

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

                    mock_build_payload.return_value = (
                        expected_request_model  # Builder returns the Pydantic model
                    )

                    # Mock the Order object that get_order_status is expected to return
                    # after successful parsing and mapping by the response handler.
                    mock_mapped_order_obj = MagicMock(
                        spec=Order
                    )  # Order from cyberdelta.core.models
                    mock_mapped_order_obj.exchange_order_id = "12345"
                    mock_mapped_order_obj.symbol = symbol_val
                    # ... add other attributes if place_order uses them

                    with patch.object(
                        api, "get_order_status", new_callable=AsyncMock
                    ) as mock_get_status:
                        mock_get_status.return_value = mock_mapped_order_obj

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

                    # Verify prepare_request (on our specific mock instance) was awaited
                    # via the side_effect
                    mock_auth_for_test.prepare_request.assert_awaited_once()

                    # Verify _request itself was called correctly by place_order
                    mock_api_request.assert_awaited_once_with(
                        method="POST",
                        endpoint="/exchange",  # _request expects endpoint, not endpoint_path
                        data=expected_data_for_request,  # Assert with the dumped dict
                        is_signed=True,
                    )

                    assert final_order == mock_mapped_order_obj

    # --- END OF PRE-SETUP PATCH CONTEXT --- #


class TestHyperliquidAPIMethodErrors:
    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.HttpClient.request", new_callable=AsyncMock)
    async def test_get_ticker_handles_mapped_http_error(
        self,
        mock_http_client_request: AsyncMock,
        mock_hl_auth_init: tuple[MagicMock, MagicMock],
    ) -> None:
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

        http_status_from_exchange = 503
        error_body_from_exchange = "Service Unavailable - Gateway Error"

        http_failure = HttpRequestFailedError(
            message=f"HTTP {http_status_from_exchange} Error from HttpClient",
            http_status_code=http_status_from_exchange,
            response_body=error_body_from_exchange,
            api_error_code=APIErrorCode.SERVICE_UNAVAILABLE,  # Set the expected final code
        )
        mock_http_client_request.side_effect = http_failure

        with pytest.raises(APIError) as exc_info:
            await api.get_ticker(symbol="BTC")

        assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
        assert exc_info.value.http_status == http_status_from_exchange
        assert exc_info.value.message is not None
        # Check that the original HttpRequestFailedError is preserved
        assert exc_info.value.original_exception is http_failure

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
            # This is the payload the builder would create (as a Pydantic model)
            expected_action_payload_error_str = HyperliquidRawPlaceOrderAction(
                asset=0,
                isBuy=False,
                sz="1",
                limitPx="0",
                orderType=HyperliquidRawOrderType(market=HyperliquidRawMarketOrderTypeDetails()),
                reduceOnly=False,
                cloid=None,
                trigger=None,
            )
            expected_request_model_error_str = HyperliquidApiPlaceOrderRequest(
                type="order", actions=[expected_action_payload_error_str]
            )
            # This is what _request will receive after .model_dump()
            expected_data_for_request_error_str = expected_request_model_error_str.model_dump(
                by_alias=True, exclude_none=True
            )

            mock_build_payload.return_value = expected_request_model_error_str

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
                "POST", "/exchange", data=expected_data_for_request_error_str, is_signed=True
            )

        # The handler should raise INVALID_RESPONSE because the string
        # "User has insufficient margin" is not a valid item in the 'statuses' list
        # according to HyperliquidRawExchangeResponse model.
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "User has insufficient margin" in str(exc_info.value.original_exception)
        assert "statuses.0" in str(exc_info.value.original_exception)  # Check path to error

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

            expected_action_payload_error_obj = HyperliquidRawPlaceOrderAction(
                asset=1,
                isBuy=True,
                sz="0.001",
                limitPx="1",
                orderType=HyperliquidRawOrderType(
                    limit=HyperliquidRawLimitOrderTypeDetails(tif="Gtc")
                ),
                reduceOnly=False,
                cloid=None,
                trigger=None,
            )
            expected_request_model_error_obj = HyperliquidApiPlaceOrderRequest(
                type="order", actions=[expected_action_payload_error_obj]
            )
            expected_data_for_request_error_obj = expected_request_model_error_obj.model_dump(
                by_alias=True, exclude_none=True
            )

            mock_build_payload.return_value = expected_request_model_error_obj

            with pytest.raises(APIError) as exc_info:
                await api.place_order(
                    symbol=symbol_val,
                    side=side_val,
                    order_type=order_type_val,
                    quantity=quantity_val,
                    price=price_val,
                    time_in_force=time_in_force_val,
                )

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
            mock_hl_request.assert_awaited_once_with(
                "POST", "/exchange", data=expected_data_for_request_error_obj, is_signed=True
            )

        assert exc_info.value.code == APIErrorCode.INVALID_ORDER_SIZE.value
        assert exc_info.value.http_status == 200
        assert error_message_from_hl in exc_info.value.message
        assert exc_info.value.exchange_message == error_message_from_hl


class TestHyperliquidAPIWebSocketRouting:
    @pytest_asyncio.fixture
    async def api_for_ws_tests(
        self, mock_hl_auth_init: tuple[MagicMock, MagicMock]
    ) -> AsyncGenerator[HyperliquidAPI]:
        # mock_hl_auth_init ensures authenticator is mocked if needed
        # We are primarily testing routing, not live connection
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
        # Mock the ws_manager for these tests
        mock_ws_manager_instance = AsyncMock()
        mock_ws_manager_instance.close = AsyncMock()  # Ensure ws_manager.close() is awaitable
        api._ws_manager = mock_ws_manager_instance  # pyright: ignore[reportPrivateUsage]

        yield api

        await api.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "topic, expected_sub_details",
        [
            ("l2Book:BTC", {"type": "l2Book", "coin": "BTC"}),
            ("trades:ETH", {"type": "trades", "coin": "ETH"}),
            ("userEvents", {"type": "userEvents", "user": TEST_WALLET_ADDRESS}),
            ("candle:SOL:1m", {"type": "candle", "coin": "SOL", "interval": "1m"}),
        ],
    )
    async def test_construct_subscription_payload_valid_topics(
        self, api_for_ws_tests: HyperliquidAPI, topic: str, expected_sub_details: dict[str, Any]
    ) -> None:
        payload = api_for_ws_tests._construct_subscription_payload(topic)  # pyright: ignore[reportPrivateUsage]
        assert payload is not None
        assert payload.get("method") == "subscribe"

        actual_subscription_raw = payload.get("subscription")
        assert actual_subscription_raw is not None, "Subscription data is missing"
        assert isinstance(actual_subscription_raw, dict), "Subscription data is not a dictionary"

        # Cast after assert isinstance to help Pylance with key types
        typed_subscription_dict = cast(dict[str, Any], actual_subscription_raw)

        typed_actual_subscription: dict[str, str] = {}
        k: str
        v_raw_from_items: Any  # Value from dict[str, Any] can be Any
        for k, v_raw_from_items in typed_subscription_dict.items():  # Use the casted dict
            key_str: str = k
            v_raw_any: object = v_raw_from_items

            value_str: str
            if isinstance(v_raw_any, str):
                value_str = v_raw_any
            else:
                pytest.fail(
                    f"Actual_raw: v !str for k '{key_str}'. "
                    f"T={type(v_raw_any)}, R={repr(v_raw_any)}"
                )
            typed_actual_subscription[key_str] = value_str

        typed_expected_sub_details: dict[str, str] = {}
        for key, value in expected_sub_details.items():
            if isinstance(value, str):
                typed_expected_sub_details[key] = value
            else:
                pytest.fail(f"Expected_sub: val type {type(value)} for key {key!r}. Val={value!r}")

        assert sorted(typed_actual_subscription.items()) == sorted(
            typed_expected_sub_details.items()
        )

    @pytest.mark.asyncio
    async def test_construct_subscription_payload_invalid_topic(
        self, api_for_ws_tests: HyperliquidAPI
    ) -> None:
        payload = api_for_ws_tests._construct_subscription_payload("invalidTopicFormat")  # pyright: ignore[reportPrivateUsage]
        assert payload is None

    @pytest.mark.asyncio
    async def test_construct_subscription_payload_user_event_no_address(
        self, api_for_ws_tests: HyperliquidAPI, mock_hl_auth_init: tuple[MagicMock, MagicMock]
    ) -> None:
        # Test userEvents subscription when API is initialized without wallet address
        # The api_for_ws_tests fixture already gives an API instance.
        # For this specific test, we need to simulate the condition of _wallet_address being None
        # on THAT INSTANCE, or an instance created specifically for this test.
        # Re-using api_for_ws_tests and patching its _wallet_address is simpler.

        # Ensure authenticator mock is not influencing this part (it's part of mock_hl_auth_init)
        # mock_auth_class, _ = mock_hl_auth_init

        # Simulate no wallet address on the provided API instance for this test's scope
        with patch.object(api_for_ws_tests, "_wallet_address", None):
            # mock_auth_class.assert_not_called() # This assertion is tricky with shared fixture

            payload = api_for_ws_tests._construct_subscription_payload("userEvents")  # pyright: ignore[reportPrivateUsage]
            assert payload is None, "Should not construct userEvents payload without address"

    @pytest.mark.asyncio
    async def test_route_ws_message_known_channel(self, api_for_ws_tests: HyperliquidAPI) -> None:
        mock_handler: AsyncMock = AsyncMock()
        channel_name = "l2Book:ETH"  # Example specific channel name
        api_for_ws_tests._ws_handlers[channel_name] = mock_handler  # pyright: ignore[reportPrivateUsage]

        test_data_payload: dict[str, Any] = {
            "coin": "ETH",
            "levels": [["100", "1"], ["101", "2"]],  # bids, asks
            "time": 1234567890,
        }
        test_message: dict[str, Any] = {"channel": channel_name, "data": test_data_payload}

        await api_for_ws_tests._handle_websocket_message(test_message)  # pyright: ignore[reportPrivateUsage]
        mock_handler.assert_awaited_once_with(test_data_payload, test_message)

    @pytest.mark.asyncio
    async def test_route_ws_message_pong(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        test_message = {"channel": "pong"}  # Data for pong is often None or just the channel
        await api_for_ws_tests._handle_websocket_message(test_message)  # pyright: ignore[reportPrivateUsage]

        # Check for key components in the log message
        assert f"[{api_for_ws_tests.exchange_name}]" in caplog.text
        assert "Control message on 'pong'" in caplog.text
        assert str(test_message) in caplog.text  # Ensure the message dict representation is there

    @pytest.mark.asyncio
    async def test_route_ws_message_error_channel(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        error_payload = "Connection timed out"
        test_message = {"channel": "error", "data": error_payload}
        await api_for_ws_tests._handle_websocket_message(test_message)  # pyright: ignore[reportPrivateUsage]
        # The log message should be: "[hyperliquid] No WS handler for 'error'. Msg: ..."
        # Since topic_key_for_handler == channel ('error'), the "(or base ...)" part is skipped.
        expected_log = (
            f"[{api_for_ws_tests.exchange_name}] No WS handler for 'error'. Msg: {test_message}"
        )
        assert expected_log in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_subscription_response(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        response_payload = {"subscription": {"type": "l2Book", "coin": "ETH"}, "status": "ok"}
        test_message = {"channel": "subscriptionResponse", "data": response_payload}
        await api_for_ws_tests._handle_websocket_message(test_message)  # pyright: ignore[reportPrivateUsage]

        # Check for key components in the log message
        assert f"[{api_for_ws_tests.exchange_name}]" in caplog.text
        assert "Control message on 'subscriptionResponse'" in caplog.text
        assert str(test_message) in caplog.text  # Ensure the message dict representation is there

    @pytest.mark.asyncio
    async def test_route_ws_message_no_handler(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        test_message = {"channel": "unknownChannel", "data": {"some": "payload"}}
        await api_for_ws_tests._handle_websocket_message(test_message)  # pyright: ignore[reportPrivateUsage]
        # The log message should be: "[hyperliquid] No WS handler for 'unknownChannel'. Msg: ..."
        # Since topic_key_for_handler == channel ('unknownChannel'), the "(or base ...)" part is skipped.
        expected_log = f"[{api_for_ws_tests.exchange_name}] No WS handler for 'unknownChannel'. Msg: {test_message}"
        assert expected_log in caplog.text

    @pytest.mark.asyncio
    async def test_route_ws_message_no_channel(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.WARNING, logger="cyberdelta.apis.hyperliquid.hl_api")
        mock_handler: AsyncMock = AsyncMock()
        channel_name = "dataCheckChannel"
        # Register a handler so it doesn't fall into "No WS handler" path
        api_for_ws_tests._ws_handlers[channel_name] = mock_handler  # pyright: ignore[reportPrivateUsage]
        test_message: dict[str, Any] = {"channel": channel_name}  # No 'data' field

        await api_for_ws_tests._handle_websocket_message(test_message)  # pyright: ignore[reportPrivateUsage]
        mock_handler.assert_not_called()  # Handler should not be called if no data

        expected_log_part = f"[{api_for_ws_tests.exchange_name}] WS '{channel_name}' has no data. Msg: {test_message}"
        assert expected_log_part in caplog.text, (
            f'Expected log substring "{expected_log_part}" not found. caplog.text: {caplog.text!r}'
        )

    @pytest.mark.asyncio
    async def test_route_ws_message_unknown_channel(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        test_message = {"type": "someType", "data": {"other": "data"}}  # No channel
        await api_for_ws_tests._handle_websocket_message(test_message)  # pyright: ignore[reportPrivateUsage]
        assert (
            f"[{api_for_ws_tests.exchange_name}] Unroutable WS message (no channel): {test_message}"
        ) in caplog.text

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidWsRawMessageHandler")
    async def test_route_ws_message_l2book_calls_handler(
        self, mock_ws_handler_class: MagicMock, api_for_ws_tests: HyperliquidAPI
    ) -> None:
        """Test _route_ws_message calls handle_l2book_payload for l2Book channel."""
        mock_app_handler = AsyncMock()
        topic = "l2Book:ETH"
        api_for_ws_tests._ws_handlers[topic] = mock_app_handler  # pyright: ignore[reportPrivateUsage]

        raw_l2_data = {"coin": "ETH", "levels": [[], []], "time": 123}  # pyright: ignore [reportUnknownVariableType]
        # Test data; type checker struggles with inline dict structure for nested lists.
        # Actual validation is done by Pydantic in the (mocked) handler.
        ws_message = {"channel": "l2Book", "data": raw_l2_data}  # pyright: ignore [reportUnknownVariableType]
        # Test data; type checker struggles with inline dict structure.

        mock_validated_l2_model = MagicMock(spec=HyperliquidRawWsBookUpdate)
        mock_dumped_l2_model = {"validated": "l2book_data"}
        mock_validated_l2_model.model_dump.return_value = mock_dumped_l2_model
        mock_ws_handler_class.handle_l2book_payload.return_value = mock_validated_l2_model

        await api_for_ws_tests._route_ws_message(ws_message)
        # Testing protected routing method directly. Arg-type ignore for ws_message due to
        # test data structure.

        mock_ws_handler_class.handle_l2book_payload.assert_called_once_with(raw_l2_data)
        mock_app_handler.assert_awaited_once_with(mock_dumped_l2_model, ws_message)

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidWsRawMessageHandler")
    async def test_route_ws_message_trades_calls_handler(
        self, mock_ws_handler_class: MagicMock, api_for_ws_tests: HyperliquidAPI
    ) -> None:
        """Test _route_ws_message calls handle_public_trades_payload for trades channel."""
        mock_app_handler = AsyncMock()
        topic = "trades:BTC"
        api_for_ws_tests._ws_handlers[topic] = mock_app_handler  # pyright: ignore[reportPrivateUsage]

        raw_trade_item = {
            "coin": "BTC",
            "px": "1",
            "sz": "1",
            "side": "B",
            "time": 123,
            "hash": "h1",
        }
        raw_trades_data = [raw_trade_item]  # trades sends a list
        ws_message = {
            "channel": "trades",
            "data": raw_trades_data,
            "coin": "BTC",
        }  # coin in top for routing key

        mock_validated_trade_model = MagicMock(spec=HyperliquidRawWsTradeEvent)
        mock_dumped_trade_model = {"validated": "trade_data"}
        mock_validated_trade_model.model_dump.return_value = mock_dumped_trade_model
        mock_ws_handler_class.handle_public_trades_payload.return_value = [
            mock_validated_trade_model
        ]

        await api_for_ws_tests._route_ws_message(ws_message)  # pyright: ignore[reportPrivateUsage]

        mock_ws_handler_class.handle_public_trades_payload.assert_called_once_with([raw_trade_item])
        mock_app_handler.assert_awaited_once_with(mock_dumped_trade_model, ws_message)

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidWsRawMessageHandler")
    async def test_route_ws_message_allmids_calls_handler(
        self, mock_ws_handler_class: MagicMock, api_for_ws_tests: HyperliquidAPI
    ) -> None:
        """Test _route_ws_message calls handle_all_mids_payload for allMids channel."""
        mock_app_handler = AsyncMock()
        topic = "allMids"
        api_for_ws_tests._ws_handlers[topic] = mock_app_handler  # pyright: ignore[reportPrivateUsage]

        raw_all_mids_data = {"BTC": "60000.0", "ETH": "3000.0"}
        ws_message = {"channel": "allMids", "data": raw_all_mids_data}

        mock_validated_all_mids_model = MagicMock(spec=HyperliquidRawAllMids)
        mock_dumped_all_mids_model = {"validated": "all_mids_data"}
        mock_validated_all_mids_model.model_dump.return_value = mock_dumped_all_mids_model
        mock_ws_handler_class.handle_all_mids_payload.return_value = mock_validated_all_mids_model

        await api_for_ws_tests._route_ws_message(ws_message)  # pyright: ignore[reportPrivateUsage]

        mock_ws_handler_class.handle_all_mids_payload.assert_called_once_with(raw_all_mids_data)
        mock_app_handler.assert_awaited_once_with(mock_dumped_all_mids_model, ws_message)

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidWsRawMessageHandler")
    @pytest.mark.parametrize(
        "event_type, raw_event_data, handler_method_name, model_spec, dump_key",
        [
            (
                "fill",
                {
                    "type": "fill",
                    "coin": "ETH",
                    "px": "1",
                    "sz": "1",
                    "side": "B",
                    "time": 1,
                    "hash": "h",
                    "oid": 1,
                },
                "handle_user_fill_event_payload",
                HyperliquidRawWsFillEvent,
                "fill_data",
            ),
            (
                "positionUpdate",
                {"type": "positionUpdate", "asset": "ETH", "position": {}, "time": 1},
                "handle_user_position_update_event_payload",
                HyperliquidRawWsPositionUpdateEvent,
                "pos_update_data",
            ),
        ],
    )
    async def test_route_ws_message_user_events_simple_calls_handler(
        self,
        mock_ws_handler_class: MagicMock,
        api_for_ws_tests: HyperliquidAPI,
        event_type: str,
        raw_event_data: dict[str, Any],
        handler_method_name: str,
        model_spec: Any,  # noqa: ANN401 - Parametrized test with varying model types
        dump_key: str,
    ) -> None:
        """Test _route_ws_message for simple userEvents (fill, positionUpdate)."""
        mock_app_handler = AsyncMock()
        topic = "userEvents"
        api_for_ws_tests._ws_handlers[topic] = mock_app_handler  # pyright: ignore[reportPrivateUsage]

        ws_message = {"channel": "userEvents", "data": [raw_event_data]}

        mock_validated_model = MagicMock(spec=model_spec)
        mock_dumped_model = {"validated": dump_key}
        mock_validated_model.model_dump.return_value = mock_dumped_model
        getattr(mock_ws_handler_class, handler_method_name).return_value = mock_validated_model

        await api_for_ws_tests._route_ws_message(ws_message)  # pyright: ignore[reportPrivateUsage]

        getattr(mock_ws_handler_class, handler_method_name).assert_called_once_with(raw_event_data)
        mock_app_handler.assert_awaited_once_with(mock_dumped_model, ws_message)

    @pytest.mark.asyncio
    @patch(f"{HL_API_PATH}.HyperliquidWsRawMessageHandler.handle_user_order_update_wrapper_payload")
    @patch(f"{HL_API_PATH}.HyperliquidWsRawMessageHandler.handle_user_order_event_payload")
    async def test_route_ws_message_user_event_order_calls_handlers(
        self,
        mock_handle_order_event: MagicMock,
        mock_handle_order_wrapper: MagicMock,
        api_for_ws_tests: HyperliquidAPI,
    ) -> None:
        """Test that user 'order' events correctly call both wrapper and detail handlers."""
        mock_app_handler = AsyncMock()
        api_for_ws_tests._ws_handlers["userEvents"] = mock_app_handler  # noqa: SLF001

        raw_event_data: dict[str, Any] = {  # This is the event_item_dict
            "type": "order",
            "data": {  # This is what the wrapper's 'data' field should contain
                "oid": 123,
                "cloid": "cloid123",
                "asset": "TEST",
                "side": "B",
                "limitPx": "100",
                "sz": "1",
                "timestamp": 1000,
                "orderType": {"limit": {"tif": "Gtc"}},
                "reduceOnly": False,
                "remainingSz": "1",
                "status": "open",
                "statusTimestamp": 1001,
            },
        }
        ws_message: dict[str, Any] = {
            "channel": "userEvents",
            "data": [raw_event_data],  # userEvents data is a list of event items
        }

        # Simpler mock for HyperliquidRawWsOrderUpdateWrapper instance
        mock_validated_order_wrapper = MagicMock()  # Removed spec
        # The 'data' attribute of the wrapper mock should return the inner dictionary
        # Ensure raw_event_data["data"] is treated as a dict before copying
        inner_data_dict = cast(dict[str, Any], raw_event_data["data"])
        mock_inner_order_data_dict = inner_data_dict.copy()
        mock_validated_order_wrapper.data = mock_inner_order_data_dict  # Direct assignment

        # mock_handle_order_wrapper handles the full raw_event_data
        mock_handle_order_wrapper.return_value = mock_validated_order_wrapper

        # Mock for HyperliquidRawOrder instance (result of inner handler)
        mock_validated_order_detail = MagicMock(spec=HyperliquidRawOrder)  # Keep spec here
        mock_dumped_order_detail = {"dumped": "order_detail_content_xyz"}
        mock_validated_order_detail.model_dump.return_value = mock_dumped_order_detail

        # mock_handle_order_event handles the mock_inner_order_data_dict
        mock_handle_order_event.return_value = mock_validated_order_detail

        await api_for_ws_tests._route_ws_message(ws_message)  # noqa: SLF001

        mock_handle_order_wrapper.assert_called_once_with(raw_event_data)
        mock_handle_order_event.assert_called_once_with(mock_inner_order_data_dict)
        mock_app_handler.assert_awaited_once_with(mock_dumped_order_detail, ws_message)


# --- Get Account Summary Tests --- #


@pytest.fixture
def mock_raw_user_state_fixture() -> HyperliquidRawClearinghouseState:
    """Provides a valid HyperliquidRawClearinghouseState fixture."""
    return HyperliquidRawClearinghouseState(
        assetPositions=[
            HyperliquidRawAssetPosition(
                asset="ETH-PERP",
                position=HyperliquidRawPositionInfo(
                    coin="ETH-PERP",
                    szi="1.0",
                    entryPx="3000.0",
                    leverage=HyperliquidRawLeverage(type="cross", value=10),
                    liquidationPx="2700.0",
                    marginUsed="300.0",
                    maxLeverage=50,
                    positionValue="3000.0",
                    returnOnEquity="0.0",
                    unrealizedPnl="50.0",
                ),
            ),
            HyperliquidRawAssetPosition(
                asset="BTC-PERP",
                position=HyperliquidRawPositionInfo(
                    coin="BTC-PERP",
                    szi="-0.1",
                    entryPx="60000.0",
                    leverage=HyperliquidRawLeverage(type="isolated", value=5),
                    liquidationPx="65000.0",
                    marginUsed="1200.0",
                    maxLeverage=20,
                    positionValue="-6000.0",
                    returnOnEquity="0.0",
                    unrealizedPnl="-100.0",
                ),
            ),
        ],
        crossMaintenanceMarginUsed="30.0",
        crossMarginSummary=HyperliquidRawMarginSummary(
            accountValue="10000.0",
            totalMarginUsed="1500.0",
            totalNtlPos="9000.0",
            totalRawUsd="8500.0",
        ),
        marginSummary=HyperliquidRawMarginSummary(
            accountValue="10000.0",
            totalMarginUsed="1500.0",
            totalNtlPos="9000.0",
            totalRawUsd="8500.0",
        ),
        isolatedMaintenanceMarginUsed="120.0",
        isolatedMarginSummary=HyperliquidRawMarginSummary(
            accountValue="0",
            totalMarginUsed="0",
            totalNtlPos="0",
            totalRawUsd="0",
        ),
        withdrawable="8500.0",
    )


@pytest.fixture
def expected_margin_account_summary_from_hl_fixture() -> MarginAccountSummary:
    """Provides an expected MarginAccountSummary fixture for HL tests."""
    # This should align with how HyperliquidMapper transforms mock_raw_user_state_fixture
    # Specifically, total_unrealized_pnl should be sum of mapped positions' PnL.
    # Mapped positions from mock_raw_user_state_fixture:
    # ETH-PERP: pnl = 50
    # BTC-PERP: pnl = -100
    # Total unrealized = 50 - 100 = -50

    # Ensure UTC is defined correctly
    # For Pydantic v2, datetime objects should be timezone-aware when comparing.
    # If datetime.now(UTC) was intended, define UTC = timezone.utc
    current_utc_time = datetime.now(UTC)

    return MarginAccountSummary(
        exchange="hyperliquid",
        timestamp=current_utc_time,
        total_equity=Decimal("10000.0"),
        available_equity=Decimal("8500.0"),
        total_initial_margin_required=None,
        total_maintenance_margin_required=Decimal("150.0"),
        total_position_notional=Decimal("9000.0"),
        total_unrealized_pnl=Decimal("-50.0"),
        hl_details=HyperliquidMarginDetails(
            cross_maintenance_margin_used=Decimal("30.0"),
            isolated_maintenance_margin_used=Decimal("120.0"),
        ),
        bp_details=None,
    )


@pytest.mark.asyncio
async def test_get_account_summary_success(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
    mock_raw_user_state_fixture: HyperliquidRawClearinghouseState,
    expected_margin_account_summary_from_hl_fixture: MarginAccountSummary,
) -> None:
    """Test successful retrieval and mapping of account summary."""
    _mock_auth_class, _mock_auth_instance = mock_hl_auth_init
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_WITH_KEY)
    raw_user_state_dict_from_api = mock_raw_user_state_fixture.model_dump(by_alias=True)

    mock_api_request = AsyncMock(return_value=raw_user_state_dict_from_api)
    mock_handle_user_state_response = MagicMock(return_value=mock_raw_user_state_fixture)

    fixed_timestamp = datetime(2023, 10, 26, 12, 0, 0, tzinfo=UTC)
    # Create a mutable copy for modification if fixture is frozen or for clarity
    current_expected_summary = expected_margin_account_summary_from_hl_fixture.model_copy(
        update={"timestamp": fixed_timestamp}
    )
    mock_map_to_margin_summary = MagicMock(return_value=current_expected_summary)

    with (
        patch.object(api._info_http_client, "request", mock_api_request),
        patch(
            "cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler.handle_info_user_state_response",
            mock_handle_user_state_response,
        ) as patched_handler,
        patch.object(
            api._hl_mapper,
            "map_raw_clearinghouse_state_to_margin_summary",
            mock_map_to_margin_summary,
        ),
        patch("cyberdelta.apis.hyperliquid.hl_mapper.datetime") as mock_datetime_in_mapper,
    ):
        mock_datetime_in_mapper.now.return_value = fixed_timestamp
        result = await api.get_account_summary()

    assert result is not None
    assert result == current_expected_summary

    mock_api_request.assert_awaited_once_with(
        method="POST",
        endpoint=api.INFO_URL,  # Corrected from "/info"
        data={"type": "clearinghouseState", "user": TEST_WALLET_ADDRESS},
        is_signed=False,
    )
    patched_handler.assert_called_once_with(
        raw_response_content=raw_user_state_dict_from_api, user_address=TEST_WALLET_ADDRESS
    )
    mock_map_to_margin_summary.assert_called_once_with(raw_state=mock_raw_user_state_fixture)


@pytest.mark.asyncio
async def test_get_account_summary_request_fails(
    mock_hl_auth_init: tuple[MagicMock, MagicMock], caplog: LogCaptureFixture
) -> None:
    """Test get_account_summary when the initial _request call fails."""
    _mock_auth_class, _mock_auth_instance = mock_hl_auth_init
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_WITH_KEY)

    # Simulate an HttpRequestFailedError that would result from a 503, as if mapped
    # or if HttpClient directly produced it with the correct code for a specific scenario.
    # The crucial part for this test is that the APIError raised by get_account_summary
    # should have the SERVICE_UNAVAILABLE code.
    simulated_failure = HttpRequestFailedError(
        message="Simulated Service Unavailable",
        http_status_code=503,  # The underlying HTTP issue
        response_body="Service temporarily down",
        api_error_code=APIErrorCode.SERVICE_UNAVAILABLE,  # Expected final code
    )

    # Patch account_service.get_account_summary_raw to raise this simulated error
    with patch.object(
        api.account_service, "get_account_summary_raw", AsyncMock(side_effect=simulated_failure)
    ):
        with pytest.raises(APIError) as exc_info:
            await api.get_account_summary()

    assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
    assert exc_info.value.http_status == 503
    assert "Simulated Service Unavailable" in exc_info.value.message
    # Ensure the exception raised is the one we simulated, or one that wraps it
    # if the API layer adds further wrapping (which it does with with_original_exception)
    original_found = False
    current_exception_for_loop: Exception | None = exc_info.value
    while current_exception_for_loop is not None:
        if current_exception_for_loop is simulated_failure:
            original_found = True
            break
        # Check if it's an APIError and has original_exception
        if isinstance(current_exception_for_loop, APIError) and hasattr(
            current_exception_for_loop, "original_exception"
        ):
            current_exception_for_loop = current_exception_for_loop.original_exception
        else:  # Not an APIError or doesn't have original_exception (e.g. base Exception)
            break

    # If the direct raised exception isn't the simulated_failure, check if it's the original_exception
    assert exc_info.value is simulated_failure or original_found, (
        "The raised APIError should be or contain the simulated HttpRequestFailedError"
    )

    # Verify logs if necessary based on actual logging in get_account_summary error path
    assert any(
        record.levelno == logging.ERROR
        and "API Error getting account summary" in record.message
        and "Simulated Service Unavailable" in record.message
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_get_account_summary_handler_fails(
    mock_hl_auth_init: tuple[MagicMock, MagicMock], caplog: LogCaptureFixture
) -> None:
    """Test get_account_summary when HyperliquidResponseHandler fails."""
    caplog.set_level(logging.ERROR, logger="cyberdelta.apis.hyperliquid.hl_api")
    _mock_auth_class, _mock_auth_instance = mock_hl_auth_init
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_WITH_KEY)

    raw_user_state_dict_from_api = {"some": "invalid_data"}
    mock_api_request = AsyncMock(return_value=raw_user_state_dict_from_api)

    mock_handle_user_state_response = MagicMock(
        side_effect=ValidationError.from_exception_data(
            title="HyperliquidRawClearinghouseState",
            line_errors=[
                {
                    "type": "missing",
                    "loc": ("asset_positions",),
                    "input": {},
                }
            ],
        )
    )

    with (
        patch.object(api._info_http_client, "request", mock_api_request),
        patch(
            "cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler.handle_info_user_state_response",
            mock_handle_user_state_response,
        ) as patched_handler,
    ):
        with pytest.raises(APIError) as exc_info:
            await api.get_account_summary()

    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "Validation error processing account summary (user_state)" in str(exc_info.value.message)
    assert "Pydantic ValidationError in get_account_summary (user_state)" in caplog.text
    mock_api_request.assert_awaited_once()
    patched_handler.assert_called_once_with(
        raw_response_content=raw_user_state_dict_from_api, user_address=TEST_WALLET_ADDRESS
    )


@pytest.mark.asyncio
async def test_get_account_summary_mapper_fails(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
    mock_raw_user_state_fixture: HyperliquidRawClearinghouseState,
    caplog: LogCaptureFixture,
) -> None:
    """Test get_account_summary when the mapper fails."""
    caplog.set_level(logging.ERROR, logger="cyberdelta.apis.hyperliquid.hl_api")
    _mock_auth_class, _mock_auth_instance = mock_hl_auth_init
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_WITH_KEY)

    raw_user_state_dict_from_api = mock_raw_user_state_fixture.model_dump(by_alias=True)
    mock_api_request = AsyncMock(return_value=raw_user_state_dict_from_api)
    mock_handle_user_state_response = MagicMock(return_value=mock_raw_user_state_fixture)
    mock_map_to_margin_summary = MagicMock(side_effect=ValueError("Mapper transformation error"))

    with (
        patch.object(api._info_http_client, "request", mock_api_request),
        patch(
            "cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler.handle_info_user_state_response",
            mock_handle_user_state_response,
        ) as patched_handler,
        patch.object(
            api._hl_mapper,
            "map_raw_clearinghouse_state_to_margin_summary",
            mock_map_to_margin_summary,
        ) as patched_mapper,
    ):
        with pytest.raises(APIError) as exc_info:
            await api.get_account_summary()

    assert exc_info.value.code == APIErrorCode.UNKNOWN.value
    assert "Unexpected error getting account summary (user_state)" in str(exc_info.value.message)
    assert "Mapper transformation error" in str(exc_info.value.original_exception)
    assert "Unexpected error in get_account_summary (user_state)" in caplog.text
    patched_mapper.assert_called_once_with(raw_state=mock_raw_user_state_fixture)


@pytest.mark.asyncio
async def test_get_account_summary_no_wallet_address(
    mock_hl_auth_init: tuple[MagicMock, MagicMock], caplog: LogCaptureFixture
) -> None:
    """Test get_account_summary when the wallet address is missing."""
    caplog.set_level(logging.ERROR, logger="cyberdelta.apis.hyperliquid.hl_api")
    _mock_auth_class, _mock_auth_instance = mock_hl_auth_init
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_NO_ADDRESS)

    with pytest.raises(APIError) as exc_info:
        await api.get_account_summary()

    assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
    assert "HLAPI: Wallet address required" in str(exc_info.value)
    assert (
        "[hyperliquid] Wallet address not available, cannot fetch user state/account summary."
        in caplog.text
    )


@pytest.mark.asyncio
async def test_get_asset_index_success(
    hl_api_instance: HyperliquidAPI,
    mock_meta_response_content: list[RawJsonResponse],
) -> None:
    """Test _get_asset_index successfully fetches and caches the asset index."""
    symbol = "ETH"
    expected_index = 1  # Based on mock_meta_response_content

    # Configure the mock for _info_http_client.request
    with patch.object(
        hl_api_instance._info_http_client,
        "request",
        AsyncMock(return_value=(mock_meta_response_content, 200, MagicMock())),
    ) as mock_request:
        # First call - should fetch
        index1 = await hl_api_instance._get_asset_index(symbol)
        assert index1 == expected_index
        mock_request.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=None,  # build_info_request_payload returns None
            rate_limiter_service=hl_api_instance._rate_limiter_service,
        )

        # Second call - should use cache
        mock_request.reset_mock()
        index2 = await hl_api_instance._get_asset_index(symbol)
        assert index2 == expected_index
        mock_request.assert_not_called()


@pytest.mark.asyncio
async def test_get_asset_index_not_found_after_fetch(
    hl_api_instance: HyperliquidAPI,
    mock_meta_response_content_missing_symbol: list[RawJsonResponse],
) -> None:
    """Test _get_asset_index raises APIError if symbol not in fetched meta."""
    symbol = "UNKNOWN_SYMBOL"
    with patch.object(
        hl_api_instance._info_http_client,
        "request",
        AsyncMock(return_value=(mock_meta_response_content_missing_symbol, 200, MagicMock())),
    ) as mock_request:
        with pytest.raises(APIError) as exc_info:
            await hl_api_instance._get_asset_index(symbol)

        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value
        mock_request.assert_called_once()


@pytest.mark.asyncio
async def test_get_asset_index_api_error(hl_api_instance: HyperliquidAPI) -> None:
    """Test _get_asset_index re-raises APIError from HTTP client."""
    symbol = "SOL"
    # TODO: Verify if NETWORK_ERROR should exist in APIErrorCode or if SERVER_ERROR is appropriate here.
    expected_error = APIError("Network error", code=APIErrorCode.SERVER_ERROR.value)
    with patch.object(
        hl_api_instance._info_http_client, "request", AsyncMock(side_effect=expected_error)
    ) as mock_request:
        with pytest.raises(APIError) as exc_info:
            await hl_api_instance._get_asset_index(symbol)

        assert exc_info.value == expected_error
        mock_request.assert_called_once()


@pytest.mark.asyncio
async def test_get_funding_rates_success(
    hl_api_instance: HyperliquidAPI,
    mock_meta_response_content: list[RawJsonResponse],
    mock_hyperliquid_mapper: MagicMock,
) -> None:
    """Test get_funding_rates successfully fetches and maps funding rates."""
    # Mock the _info_http_client.request call for /info endpoint
    with patch.object(
        hl_api_instance._info_http_client,
        "request",
        AsyncMock(return_value=(mock_meta_response_content, 200, MagicMock())),
    ) as mock_request:
        # Mock the mapper results
        current_time = datetime.now(UTC)
        mock_funding_rate_btc = FundingRate(
            symbol="BTC",
            funding_rate=Decimal("0.0001"),
            timestamp=current_time,
            next_funding_time=current_time,  # Placeholder, adjust if specific logic needed
        )
        mock_funding_rate_eth = FundingRate(
            symbol="ETH",
            funding_rate=Decimal("0.0002"),
            timestamp=current_time,
            next_funding_time=current_time,  # Placeholder, adjust if specific logic needed
        )

        def mock_map_raw_ctx_to_funding_rate(ctx: HyperliquidRawAssetCtx) -> FundingRate | None:
            if ctx.name == "BTC":
                return mock_funding_rate_btc
            if ctx.name == "ETH":
                return mock_funding_rate_eth
            return None

        mock_hyperliquid_mapper.map_raw_ctx_to_funding_rate.side_effect = (
            mock_map_raw_ctx_to_funding_rate
        )
        hl_api_instance._hl_mapper = mock_hyperliquid_mapper

        result = await hl_api_instance.get_funding_rates()

        mock_request.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data=None,  # build_info_request_payload returns None
            rate_limiter_service=hl_api_instance._rate_limiter_service,
        )
        assert len(result) == 2
        assert mock_funding_rate_btc in result
        assert mock_funding_rate_eth in result


@pytest.mark.asyncio
async def test_get_funding_rates_api_error(hl_api_instance: HyperliquidAPI) -> None:
    """Test get_funding_rates handles APIError from the underlying request."""
    expected_error = APIError("Test API Error", code=APIErrorCode.UNKNOWN.value)
    with patch.object(
        hl_api_instance._info_http_client, "request", AsyncMock(side_effect=expected_error)
    ) as mock_request:
        with pytest.raises(APIError) as exc_info:
            await hl_api_instance.get_funding_rates()

        assert exc_info.value == expected_error
        mock_request.assert_called_once()
