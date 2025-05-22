"""
Unit tests for the HyperliquidAPI client implementation.
"""

import logging
from collections.abc import AsyncGenerator, Generator
from copy import deepcopy
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
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    RawJsonResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawOrder
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawLimitOrderTypeDetails,
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
def mock_hyperliquid_mapper() -> MagicMock:
    from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidMapper

    return MagicMock(spec=HyperliquidMapper)


@pytest.fixture
def mock_hl_auth_init() -> Generator[tuple[MagicMock, MagicMock], Any]:
    """Mocks the HyperliquidEip712Authenticator initialization."""
    with patch(
        "cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"
    ) as mock_auth_class:
        mock_instance = MagicMock(spec=HyperliquidEip712Authenticator)
        mock_instance.prepare_request = AsyncMock()
        mock_auth_class.return_value = mock_instance
        yield mock_auth_class, mock_instance


@pytest_asyncio.fixture
async def hl_api_instance(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> AsyncGenerator[HyperliquidAPI]:
    """Provides an initialized HyperliquidAPI instance for testing.
    This fixture includes an implicit test of the _authenticate method's delegation
    to the authenticator by setting up its return value and asserting its call,
    avoiding direct call to the protected method in a dedicated test.
    """
    _mock_auth_class, _mock_auth_instance = mock_hl_auth_init
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    yield api
    await api.close()


@pytest.fixture
def mock_meta_response_content() -> list[RawJsonResponse]:
    """Provides a mock raw JSON response for /info endpoint (meta and asset contexts)."""
    return [
        {
            "universe": [
                {"name": "BTC", "szDecimals": 5, "maxLeverage": 100, "onlyIsolated": False},
                {"name": "ETH", "szDecimals": 4, "maxLeverage": 80, "onlyIsolated": False},
                {"name": "SOL", "szDecimals": 2, "maxLeverage": 50, "onlyIsolated": False},
            ]
        },
        [
            {
                "name": "BTC",
                "funding": "0.0001",
                "markPx": "60000",
                "prevDayPx": "59000",
                "dayNtlVlm": "100000000",
            },
            {
                "name": "ETH",
                "funding": "0.0002",
                "markPx": "3000",
                "prevDayPx": "2950",
                "dayNtlVlm": "50000000",
            },
            {
                "name": "SOL",
                "funding": "0.0003",
                "markPx": "150",
                "prevDayPx": "145",
                "dayNtlVlm": "20000000",
            },
        ],
    ]


@pytest.fixture
def mock_meta_response_content_for_btc_only() -> list[RawJsonResponse]:
    """Provides a mock raw JSON response for /info endpoint (meta and asset contexts)
    with only BTC.
    """
    return [
        {"universe": [{"name": "BTC", "szDecimals": 5, "maxLeverage": 100, "onlyIsolated": False}]},
        [
            {
                "name": "BTC",
                "funding": "0.0001",
                "markPx": "60000",
                "prevDayPx": "59000",
                "dayNtlVlm": "100000000",
                "impactPx": "59999.5",
            }
        ],
    ]


@pytest.fixture
def mock_meta_response_content_missing_symbol() -> list[RawJsonResponse]:
    """Mock /info response content where a specific symbol (e.g., SOL) is missing."""
    return [
        {
            "universe": [
                {"name": "BTC", "szDecimals": 5, "maxLeverage": 100, "onlyIsolated": False},
                {"name": "ETH", "szDecimals": 4, "maxLeverage": 80, "onlyIsolated": False},
            ]
        },
        [
            {
                "name": "BTC",
                "funding": "0.0001",
                "markPx": "60000",
                "prevDayPx": "59000",
                "dayNtlVlm": "100000000",
            },
            {
                "name": "ETH",
                "funding": "0.0002",
                "markPx": "3000",
                "prevDayPx": "2950",
                "dayNtlVlm": "50000000",
            },
        ],
    ]


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

        mock_auth_class.assert_called_once()
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

        mock_auth_class.assert_not_called()
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
async def test_authenticate_no_authenticator_via_public_method(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
    mock_meta_response_content_for_btc_only: list[RawJsonResponse],
) -> None:
    """Test APIError is raised when calling a signed public method
    if no authenticator is configured."""
    _mock_auth_class, _mock_auth_instance = mock_hl_auth_init
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_NO_KEY)

    assert api._authenticator is None

    # Mock _info_http_client.request to return valid meta content, preventing _get_asset_index from failing
    with patch.object(
        api._info_http_client, "request", new_callable=AsyncMock
    ) as mock_info_http_client_request:
        mock_info_http_client_request.return_value = (
            mock_meta_response_content_for_btc_only,  # Use the fixture content
            200,
            MagicMock(),
            MagicMock(),
        )
        # Patch _get_asset_index_callable on the trading_service to bypass the call
        with patch.object(
            api.trading_service, "_get_asset_index_callable", AsyncMock(return_value=0)
        ):
            with patch(
                f"{HL_API_PATH}.HttpClient.request",  # This mock is for the /exchange call
                new_callable=AsyncMock,
            ) as mock_exchange_http_client_request:  # This is self._http_client.request
                # Set a default valid 4-tuple return, though we expect it not to be called
                mock_exchange_http_client_request.return_value = (
                    None,
                    200,
                    MagicMock(),
                    MagicMock(),
                )
                with pytest.raises(APIError, match="HL authenticator not initialized") as excinfo:
                    await api.place_order(
                        symbol="BTC",
                        side=OrderSide.BUY,
                        order_type=OrderType.LIMIT,
                        quantity=Decimal("0.001"),
                        price=Decimal("1.0"),
                        time_in_force=TimeInForce.GTC,
                    )
            assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
            # The /exchange call should not be made if authenticator is missing
            mock_exchange_http_client_request.assert_not_called()


@pytest.mark.asyncio
async def test_authenticate_prepare_request_fails_via_public_method(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
    mock_meta_response_content_for_btc_only: list[RawJsonResponse],
) -> None:
    """Test APIError propagates from authenticator's prepare_request
    when calling a signed public method."""
    _mock_auth_class, mock_auth_instance = mock_hl_auth_init
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_WITH_KEY)
    # Ensure the instance from mock_hl_auth_init is used, or re-assign if necessary
    # For this test, we directly assign the authenticator after API init
    # to ensure our specific mock_auth_instance with the side_effect is used.
    api._authenticator = mock_auth_instance
    api._hl_authenticator = mock_auth_instance
    # Also update the trading service's authenticator reference
    api.trading_service._authenticator = mock_auth_instance

    mock_auth_instance.prepare_request.side_effect = APIError(
        "Signing failed internally", code=APIErrorCode.AUTHENTICATION_FAILED.value
    )

    # Mock _info_http_client.request to return valid meta content, preventing _get_asset_index from failing
    with patch.object(
        api._info_http_client, "request", new_callable=AsyncMock
    ) as mock_info_http_client_request:
        mock_info_http_client_request.return_value = (
            mock_meta_response_content_for_btc_only,  # Use the fixture content
            200,
            MagicMock(),
            MagicMock(),
        )
    # Patch _get_asset_index_callable on the trading_service to bypass the call
    with patch.object(api.trading_service, "_get_asset_index_callable", AsyncMock(return_value=0)):
        with pytest.raises(APIError, match="Signing failed internally") as excinfo:
            await api.place_order(
                symbol="BTC",
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("0.001"),
                price=Decimal("1.0"),
                time_in_force=TimeInForce.GTC,
            )
        assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
        # Ensure prepare_request was called
        mock_auth_instance.prepare_request.assert_awaited_once()


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
            *args: Any,  # Keep *args for flexibility if other calls use it
            **kwargs: Any,
        ) -> tuple[dict[str, Any], int, MagicMock, MagicMock]:  # Added return type hint
            # When the service's requester is called, it passes method and endpoint_path
            # as explicit positional arguments, followed by kwargs.
            # Corrected to access from kwargs as per plan
            method = kwargs["method"]
            endpoint_path = kwargs["endpoint"]  # Based on _request signature in API

            if kwargs.get("is_signed") is True and api._authenticator:
                # The data passed to prepare_request should be the Pydantic model's dump
                # Use mock_auth_for_test as api._authenticator points to it in this test context
                await mock_auth_for_test.prepare_request(
                    method=method,
                    path=endpoint_path,
                    params=kwargs.get("params"),
                    data=kwargs.get("data"),  # This data is already model_dumped by api.place_order
                    headers=dict(api.default_headers),
                )
            return (mock_http_response_content, 200, MagicMock(), MagicMock())  # Return a tuple

        # Patch the _request method on the API instance
        # The service requester is an alias to HyperliquidAPI._request.
        # Patching api._request directly.
        with patch.object(
            api,
            "_request",  # Patching HyperliquidAPI._request directly
            new_callable=AsyncMock,  # Use new_callable to return an AsyncMock
        ) as mock_api_request_method:
            # Set the return value for _request to be a successful response
            mock_api_request_method.return_value = (
                mock_http_response_content,
                200,
                MagicMock(),
                MagicMock(),
            )
            # Patch the _info_http_client.request call that _get_asset_index makes
            # This is CRITICAL to prevent real network calls from _get_asset_index
            with patch.object(
                api._info_http_client, "request", new_callable=AsyncMock
            ) as mock_info_http_client_request:
                mock_info_http_client_request.return_value = (
                    # Content part: a list containing [meta_dict, asset_ctx_list_of_dicts]
                    [
                        {
                            "universe": [
                                {
                                    "name": "BTC",
                                    "szDecimals": 5,
                                    "maxLeverage": 100,
                                    "onlyIsolated": False,
                                }
                            ]
                        },
                        [
                            {
                                "name": "BTC",
                                "funding": "0.0000125",
                                "markPx": "110355.0",
                                "prevDayPx": "106375.0",
                                "dayNtlVlm": "11460776239.7850627899",
                                "impactPx": "110340.0",
                            }
                        ],
                    ],
                    200,
                    MagicMock(),
                    MagicMock(),
                )

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

                        # First call to place_order for this symbol - should fetch asset_index
                        final_order_call_1 = await api.place_order(
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
                        # Assertions for first call
                        mock_info_http_client_request.assert_called_once_with(
                            method="POST",
                            endpoint_path="/info",
                            data={"type": "metaAndAssetCtxs"},
                            # Direct access for verification
                            rate_limiter_service=api._rate_limiter_service,
                        )
                        mock_build_payload.assert_called_once_with(
                            asset_index=0,  # This is the asset_index returned by _get_asset_index
                            side=side_val,
                            order_type=order_type_val,
                            quantity=quantity_val,
                            time_in_force=time_in_force_val,
                            price=price_val,
                            stop_price=stop_price_val,
                            client_order_id=client_order_id_val,
                            reduce_only=reduce_only_val,
                            post_only=post_only_val,
                        )
                        # Assert prepare_request was called with the correct data (model_dumped)
                        mock_auth_for_test.prepare_request.assert_awaited_once_with(
                            method="POST",
                            path="/exchange",
                            params=None,  # Assuming no params for this call
                            data=expected_data_for_request,
                            headers=dict(api.default_headers),
                        )
                        mock_api_request_method.assert_awaited_once_with(
                            method="POST",
                            endpoint="/exchange",
                            data=auth_prepared_components[
                                "data"
                            ],  # This should be the data returned by prepare_request
                            is_signed=True,
                        )
                        assert final_order_call_1 == mock_mapped_order_obj

                        # Reset mocks for the second call
                        mock_info_http_client_request.reset_mock()
                        mock_build_payload.reset_mock()
                        mock_auth_for_test.prepare_request.reset_mock()
                        mock_api_request_method.reset_mock()
                        mock_get_status.reset_mock()
                        mock_get_status.return_value = (
                            mock_mapped_order_obj  # Re-assign return value
                        )

                        # Second call to place_order for the same symbol - should use
                        # cached asset_index
                        final_order_call_2 = await api.place_order(
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
                        # Assertions for second call
                        mock_info_http_client_request.assert_not_called()  # Should use cache
                        mock_build_payload.assert_called_once_with(  # Builder still called with cached asset_index
                            asset_index=0,
                            side=side_val,
                            order_type=order_type_val,
                            quantity=quantity_val,
                            time_in_force=time_in_force_val,
                            price=price_val,
                            stop_price=stop_price_val,
                            client_order_id=client_order_id_val,
                            reduce_only=reduce_only_val,
                            post_only=post_only_val,
                        )
                        mock_auth_for_test.prepare_request.assert_awaited_once_with(  # Auth still called
                            method="POST",
                            path="/exchange",
                            params=None,
                            data=expected_data_for_request,
                            headers=dict(api.default_headers),
                        )
                        mock_api_request_method.assert_awaited_once_with(  # Assert new mock target
                            method="POST",
                            endpoint="/exchange",
                            data=auth_prepared_components["data"],
                            is_signed=True,
                        )
                        assert final_order_call_2 == mock_mapped_order_obj

    # --- END OF PRE-SETUP PATCH CONTEXT --- #


@pytest.mark.asyncio
async def test_place_order_asset_index_not_found(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
    mock_meta_response_content_missing_symbol: list[RawJsonResponse],
) -> None:
    """Test place_order raises APIError if symbol for asset_index is not in fetched meta."""
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
    symbol_to_test = (
        "UNKNOWN_SYMBOL"  # This symbol is not in mock_meta_response_content_missing_symbol
    )

    with patch.object(
        api._info_http_client,  # Accessing protected member for test setup
        "request",
        AsyncMock(
            return_value=(mock_meta_response_content_missing_symbol, 200, MagicMock(), MagicMock())
        ),
    ) as mock_info_request:
        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol=symbol_to_test,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1"),
                price=Decimal("1"),
                time_in_force=TimeInForce.GTC,
            )
        assert exc_info.value.code == APIErrorCode.SYMBOL_NOT_FOUND.value
        mock_info_request.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data={"type": "metaAndAssetCtxs"},
            rate_limiter_service=api._rate_limiter_service,  # Direct access for verification
        )


@pytest.mark.asyncio
async def test_place_order_asset_index_fetch_api_error(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
) -> None:
    """Test place_order handles APIError from the asset_index fetch correctly."""
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
    symbol_to_test = "ETH"

    original_error_msg = "Test API Error During Asset Index Fetch"
    mock_raised_error_during_fetch = APIError(
        original_error_msg, code=APIErrorCode.SERVICE_UNAVAILABLE.value
    )

    with patch.object(
        api._info_http_client,  # Accessing protected member for test setup
        "request",
        AsyncMock(side_effect=mock_raised_error_during_fetch),
    ) as mock_info_request:
        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol=symbol_to_test,
                side=OrderSide.BUY,
                order_type=OrderType.LIMIT,
                quantity=Decimal("1"),
                price=Decimal("1"),
                time_in_force=TimeInForce.GTC,
            )

        # The error from _get_asset_index is wrapped, so we check the propagated error
        assert (
            exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
        )  # The original code should be preserved
        assert (
            original_error_msg in exc_info.value.message
        )  # Original message should be part of the new one
        assert exc_info.value.original_exception == mock_raised_error_during_fetch

        mock_info_request.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data={"type": "metaAndAssetCtxs"},
            rate_limiter_service=api._rate_limiter_service,  # Direct access for verification
        )


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
            api_error_code=APIErrorCode.SERVICE_UNAVAILABLE,
        )
        mock_http_client_request.side_effect = http_failure

        with pytest.raises(APIError) as exc_info:
            await api.get_ticker(symbol="BTC")

        assert exc_info.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
        assert exc_info.value.http_status == http_status_from_exchange
        assert exc_info.value.message == error_body_from_exchange
        assert exc_info.value.original_exception is http_failure
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
        mock_get_asset_index.return_value = 0

        error_string_from_hl = "User has insufficient margin"
        mock_hl_response_with_internal_error = {
            "status": "ok",
            "data": {"type": "order", "statuses": [error_string_from_hl]},
        }
        mock_hl_request.return_value = (
            mock_hl_response_with_internal_error,
            200,
            MagicMock(),
            MagicMock(),
        )

        with pytest.raises(APIError) as exc_info:
            await api.place_order(
                symbol="ETH",
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=Decimal("1"),
                price=Decimal("0.001"),
                time_in_force=TimeInForce.IOC,
            )

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "User has insufficient margin" in str(exc_info.value.original_exception)
        assert "statuses.0" in str(exc_info.value.original_exception)

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
        mock_get_asset_index.return_value = 1

        error_message_from_hl = "Order size too small"
        mock_hl_response_with_error_obj = {
            "status": "ok",
            "data": {"type": "order", "statuses": [{"error": error_message_from_hl}]},
        }

        mock_hl_request.return_value = (
            mock_hl_response_with_error_obj,
            200,
            MagicMock(),
            MagicMock(),
        )

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
        assert error_message_from_hl in str(exc_info.value)
        assert exc_info.value.exchange_message == error_message_from_hl


class TestHyperliquidAPIWebSocketRouting:
    @pytest_asyncio.fixture
    async def api_for_ws_tests(
        self, mock_hl_auth_init: tuple[MagicMock, MagicMock]
    ) -> AsyncGenerator[HyperliquidAPI]:
        # mock_hl_auth_init ensures authenticator is mocked if needed
        # We are primarily testing routing, not live connection
        api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)
        mock_ws_manager_instance = AsyncMock()
        mock_ws_manager_instance.close = AsyncMock()

        with patch.object(api, "_ws_manager", new=mock_ws_manager_instance):
            yield api
        # api.close() if called by the test would operate on the instance after the patch
        # on _ws_manager has expired if not handled carefully by test structure.
        # For robust cleanup, tests might need to call api.close() inside a try/finally
        # that ensures the fixture's full lifecycle or manage ws_manager state explicitly.

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
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_construct_subscription_payload_valid_topics(
        self, api_for_ws_tests: HyperliquidAPI, topic: str, expected_sub_details: dict[str, Any]
    ) -> None:
        payload = api_for_ws_tests._construct_subscription_payload(topic)
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

        assert sorted(typed_actual_subscription.items()) == sorted(
            typed_expected_sub_details.items()
        )

    @pytest.mark.asyncio
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_construct_subscription_payload_invalid_topic(
        self, api_for_ws_tests: HyperliquidAPI
    ) -> None:
        payload = api_for_ws_tests._construct_subscription_payload("invalidTopicFormat")
        assert payload is None

    @pytest.mark.asyncio
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
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
        with patch.object(
            api_for_ws_tests, "_wallet_address", new=None
        ):  # Use new=None for patching attributes to None
            # mock_auth_class.assert_not_called() # This assertion is tricky with shared fixture

            payload = api_for_ws_tests._construct_subscription_payload("userEvents")
            assert payload is None, "Should not construct userEvents payload without address"

    @pytest.mark.asyncio
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_known_channel(self, api_for_ws_tests: HyperliquidAPI) -> None:
        mock_handler: AsyncMock = AsyncMock()
        channel_name = "l2Book:ETH"  # Example specific channel name
        # api_for_ws_tests._ws_handlers[channel_name] = mock_handler # Original direct assignment

        test_data_payload: dict[str, Any] = {
            "coin": "ETH",
            "levels": [["100", "1"], ["101", "2"]],  # bids, asks
            "time": 1234567890,
        }
        test_message: dict[str, Any] = {"channel": channel_name, "data": test_data_payload}

        # Patch _ws_handlers for the scope of this test
        with patch.object(api_for_ws_tests, "_ws_handlers", new={channel_name: mock_handler}):
            await api_for_ws_tests._handle_websocket_message(test_message)

        mock_handler.assert_awaited_once_with(test_data_payload, test_message)

    @pytest.mark.asyncio
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_pong(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        test_message = {"channel": "pong"}  # Data for pong is often None or just the channel
        await api_for_ws_tests._handle_websocket_message(test_message)

        # Check for key components in the log message
        assert f"[{api_for_ws_tests.exchange_name}]" in caplog.text
        assert "Control message on 'pong'" in caplog.text
        assert str(test_message) in caplog.text  # Ensure the message dict representation is there

    @pytest.mark.asyncio
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_error_channel(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        error_payload = "Connection timed out"
        test_message = {"channel": "error", "data": error_payload}
        await api_for_ws_tests._handle_websocket_message(test_message)
        # The log message should be: "[hyperliquid] No WS handler for 'error'. Msg: ..."
        # Since topic_key_for_handler == channel ('error'), the "(or base ...)" part is skipped.
        expected_log = (
            f"[{api_for_ws_tests.exchange_name}] No WS handler for 'error'. Msg: {test_message}"
        )
        assert expected_log in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_subscription_response(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        response_payload = {"subscription": {"type": "l2Book", "coin": "ETH"}, "status": "ok"}
        test_message = {"channel": "subscriptionResponse", "data": response_payload}
        await api_for_ws_tests._handle_websocket_message(test_message)

        # Check for key components in the log message
        assert f"[{api_for_ws_tests.exchange_name}]" in caplog.text
        assert "Control message on 'subscriptionResponse'" in caplog.text
        assert str(test_message) in caplog.text  # Ensure the message dict representation is there

    @pytest.mark.asyncio
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_no_handler(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        test_message = {"channel": "unknownChannel", "data": {"some": "payload"}}
        await api_for_ws_tests._handle_websocket_message(test_message)
        # The log message should be: "[hyperliquid] No WS handler for 'unknownChannel'. Msg: ..."
        # Since topic_key_for_handler == channel ('unknownChannel'), the "(or base ...)" part
        # is skipped.
        expected_log = (
            f"[{api_for_ws_tests.exchange_name}] No WS handler for 'unknownChannel'. "
            f"Msg: {test_message}"
        )
        assert expected_log in caplog.text

    @pytest.mark.asyncio
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_no_channel(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.WARNING, logger="cyberdelta.apis.hyperliquid.hl_api")
        mock_handler: AsyncMock = AsyncMock()
        channel_name = "dataCheckChannel"
        # Register a handler so it doesn't fall into "No WS handler" path
        # api_for_ws_tests._ws_handlers[channel_name] = mock_handler # Original
        test_message: dict[str, Any] = {"channel": channel_name}  # No 'data' field

        with patch.object(api_for_ws_tests, "_ws_handlers", {channel_name: mock_handler}):
            await api_for_ws_tests._handle_websocket_message(test_message)
        mock_handler.assert_not_called()  # Handler should not be called if no data

        expected_log_part = (
            f"[{api_for_ws_tests.exchange_name}] WS '{channel_name}' "
            f"has no data. Msg: {test_message}"
        )
        assert expected_log_part in caplog.text, (
            f'Expected log substring "{expected_log_part}" not found. caplog.text: {caplog.text!r}'
        )

    @pytest.mark.asyncio
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_unknown_channel(
        self, api_for_ws_tests: HyperliquidAPI, caplog: LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="cyberdelta.apis.hyperliquid.hl_api")
        test_message = {"type": "someType", "data": {"other": "data"}}  # No channel
        await api_for_ws_tests._handle_websocket_message(test_message)
        assert (
            f"[{api_for_ws_tests.exchange_name}] Unroutable WS message (no channel): {test_message}"
        ) in caplog.text

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidWsRawMessageHandler")
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_l2book_calls_handler(
        self, mock_ws_handler_class: MagicMock, api_for_ws_tests: HyperliquidAPI
    ) -> None:
        """Test _route_ws_message calls handle_l2book_payload for l2Book channel."""
        mock_app_handler = AsyncMock()
        topic = "l2Book:ETH"
        # api_for_ws_tests._ws_handlers[topic] = mock_app_handler # Original

        raw_l2_data = {"coin": "ETH", "levels": [["100.0", "1.0"], ["101.0", "2.5"]], "time": 123}  # pyright: ignore [reportUnknownVariableType]
        # Test data; type checker struggles with inline dict structure for nested lists.
        # Actual validation is done by Pydantic in the (mocked) handler.
        ws_message = {"channel": "l2Book", "data": raw_l2_data}  # pyright: ignore [reportUnknownVariableType]
        # Test data; type checker struggles with inline dict structure.

        mock_validated_l2_model = MagicMock(spec=HyperliquidRawWsBookUpdate)
        mock_dumped_l2_model = {"validated": "l2book_data"}
        mock_validated_l2_model.model_dump.return_value = mock_dumped_l2_model
        mock_ws_handler_class.handle_l2book_payload.return_value = mock_validated_l2_model

        with patch.object(api_for_ws_tests, "_ws_handlers", {topic: mock_app_handler}):
            await api_for_ws_tests._route_ws_message(ws_message)
        # Testing protected routing method directly. Arg-type ignore for ws_message due to
        # test data structure.

        mock_ws_handler_class.handle_l2book_payload.assert_called_once_with(raw_l2_data)
        mock_app_handler.assert_awaited_once_with(mock_dumped_l2_model, ws_message)

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidWsRawMessageHandler")
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_trades_calls_handler(
        self, mock_ws_handler_class: MagicMock, api_for_ws_tests: HyperliquidAPI
    ) -> None:
        """Test _route_ws_message calls handle_public_trades_payload for trades channel."""
        mock_app_handler = AsyncMock()
        topic = "trades:BTC"
        # api_for_ws_tests._ws_handlers[topic] = mock_app_handler # Original

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

        with patch.object(api_for_ws_tests, "_ws_handlers", {topic: mock_app_handler}):
            await api_for_ws_tests._route_ws_message(ws_message)

        mock_ws_handler_class.handle_public_trades_payload.assert_called_once_with([raw_trade_item])
        mock_app_handler.assert_awaited_once_with(mock_dumped_trade_model, ws_message)

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidWsRawMessageHandler")
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_allmids_calls_handler(
        self, mock_ws_handler_class: MagicMock, api_for_ws_tests: HyperliquidAPI
    ) -> None:
        """Test _route_ws_message calls handle_all_mids_payload for allMids channel."""
        mock_app_handler = AsyncMock()
        topic = "allMids"
        # api_for_ws_tests._ws_handlers[topic] = mock_app_handler # Original

        raw_all_mids_data = {"BTC": "60000.0", "ETH": "3000.0"}
        ws_message = {"channel": "allMids", "data": raw_all_mids_data}

        mock_validated_all_mids_model = MagicMock(spec=HyperliquidRawAllMids)
        mock_dumped_all_mids_model = {"validated": "all_mids_data"}
        mock_validated_all_mids_model.model_dump.return_value = mock_dumped_all_mids_model
        mock_ws_handler_class.handle_all_mids_payload.return_value = mock_validated_all_mids_model

        with patch.object(api_for_ws_tests, "_ws_handlers", {topic: mock_app_handler}):
            await api_for_ws_tests._route_ws_message(ws_message)

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
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_user_events_simple_calls_handler(
        self,
        mock_ws_handler_class: MagicMock,
        api_for_ws_tests: HyperliquidAPI,
        event_type: str,
        raw_event_data: dict[str, Any],
        handler_method_name: str,
        model_spec: Any,
        dump_key: str,
    ) -> None:
        """Test _route_ws_message for simple userEvents (fill, positionUpdate)."""
        mock_app_handler = AsyncMock()
        topic = "userEvents"

        ws_message = {"channel": "userEvents", "data": [raw_event_data]}

        mock_validated_model = MagicMock(spec=model_spec)
        mock_dumped_model = {"validated": dump_key}
        mock_validated_model.model_dump.return_value = mock_dumped_model
        getattr(mock_ws_handler_class, handler_method_name).return_value = mock_validated_model

        with patch.object(api_for_ws_tests, "_ws_handlers", {topic: mock_app_handler}):
            await api_for_ws_tests._route_ws_message(ws_message)

        getattr(mock_ws_handler_class, handler_method_name).assert_called_once_with(raw_event_data)
        mock_app_handler.assert_awaited_once_with(mock_dumped_model, ws_message)

    @pytest.mark.asyncio
    @patch(f"{HL_API_PATH}.HyperliquidWsRawMessageHandler.handle_user_order_update_wrapper_payload")
    @patch(f"{HL_API_PATH}.HyperliquidWsRawMessageHandler.handle_user_order_event_payload")
    @pytest.mark.skip(
        "Protected method access - requires refactoring to public API or"
        " explicit decision to allow testing protected method."
    )
    async def test_route_ws_message_user_event_order_calls_handlers(
        self,
        mock_handle_order_event: MagicMock,
        mock_handle_order_wrapper: MagicMock,
        api_for_ws_tests: HyperliquidAPI,
    ) -> None:
        """Test that user 'order' events correctly call both wrapper and detail handlers."""
        mock_app_handler = AsyncMock()

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

        with patch.object(api_for_ws_tests, "_ws_handlers", {"userEvents": mock_app_handler}):
            await api_for_ws_tests._route_ws_message(ws_message)

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
        total_initial_margin_required=None,  # Hyperliquid does not provide this directly
        total_maintenance_margin_required=Decimal("150.0"),  # cross (30) + isolated (120)
        total_position_notional=Decimal("9000.0"),  # Based on mock raw user state
        total_unrealized_pnl=Decimal("-50.0"),  # Sum of PnL from positions
        hl_details=HyperliquidMarginDetails(
            cross_maintenance_margin_used=Decimal("30.0"),
            isolated_maintenance_margin_used=Decimal("120.0"),
        ),
        bp_details=None,  # Explicitly None
    )


@pytest.mark.asyncio
async def test_get_account_summary_success(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
    mock_raw_user_state_fixture: HyperliquidRawClearinghouseState,
    expected_margin_account_summary_from_hl_fixture: MarginAccountSummary,
) -> None:
    """Test successful retrieval and mapping of account summary by mocking service call."""
    _mock_auth_class, _mock_auth_instance = mock_hl_auth_init
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_WITH_KEY)

    # Use a fixed timestamp for consistent test results
    fixed_timestamp = datetime(2023, 10, 26, 12, 0, 0, tzinfo=UTC)
    # Ensure the expected summary uses this fixed timestamp
    current_expected_summary = expected_margin_account_summary_from_hl_fixture.model_copy(
        update={"timestamp": fixed_timestamp}
    )

    # Mock the direct service call on the api instance
    with patch.object(
        api.account_service, "get_account_summary", AsyncMock(return_value=current_expected_summary)
    ) as mock_service_get_summary:
        # We also need to ensure datetime.now(UTC) called within the API method
        # (if any for top-level timestamping)
        # or by the mapper (if we were testing it) is controlled.
        # Since we mock the service's get_account_summary,
        # the mapper's timestamping is bypassed here.
        # If API.get_account_summary itself adds a timestamp, that would need mocking.
        # However, the responsibility for the MarginAccountSummary's timestamp
        # lies with the service/mapper.
        result = await api.get_account_summary()

    assert result is not None
    assert result == current_expected_summary
    mock_service_get_summary.assert_awaited_once()  # Verify the service method was called


@pytest.mark.asyncio
async def test_get_account_summary_request_fails(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
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

    # Patch account_service.get_account_summary to raise this simulated error
    with patch.object(
        api.account_service, "get_account_summary", AsyncMock(side_effect=simulated_failure)
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

        # If the direct raised exception isn't the simulated_failure, check if it's
        # the original_exception
        assert exc_info.value is simulated_failure or original_found, (
            "The raised APIError should be or contain the simulated HttpRequestFailedError"
        )


@pytest.mark.asyncio
async def test_get_account_summary_handler_fails(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
    mock_raw_user_state_fixture: HyperliquidRawClearinghouseState,
) -> None:
    """Test get_account_summary when the response handler fails."""
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_WITH_KEY)

    # Simulate an APIError that would result from an internal handler/mapper failure
    simulated_failure = APIError(
        message="Simulated internal mapper error",
        code=APIErrorCode.INVALID_RESPONSE.value,
        original_exception=ValueError("Internal mapper validation error"),
    )

    # Patch account_service.get_account_summary to raise this simulated error
    with patch.object(
        api.account_service, "get_account_summary", AsyncMock(side_effect=simulated_failure)
    ):
        with pytest.raises(APIError) as exc_info:
            await api.get_account_summary()

        assert exc_info.value is simulated_failure
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Simulated internal mapper error" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValueError)


@pytest.mark.asyncio
async def test_get_account_summary_mapper_fails(
    mock_hl_auth_init: tuple[MagicMock, MagicMock],
    mock_raw_user_state_fixture: HyperliquidRawClearinghouseState,
) -> None:
    """Test get_account_summary when the API's direct mapper call would fail (if service didn't)."""
    api = HyperliquidAPI(BASE_API_CONFIG, SECRETS_WITH_KEY)
    # This test assumes a scenario where the API layer itself would do mapping,
    # which is less likely if a service layer is responsible.
    # For robustness, let's assume the service call succeeds but returns something
    # that the API layer then tries to process (if it had such logic).
    # However, with `api.account_service.get_account_summary` being the point of interaction,
    # errors from mapping should ideally be encapsulated within the service's APIError.

    # Let's assume the test intends to check what happens if the API's _hl_mapper is directly used.
    # This test will be more illustrative of testing the mapper itself, or a different API flow.

    service_error = APIError(
        "Service internal mapper error",
        code=APIErrorCode.INVALID_RESPONSE.value,
        original_exception=ValueError("Test mapper validation error"),
    )
    with patch.object(
        api.account_service, "get_account_summary", AsyncMock(side_effect=service_error)
    ):
        with pytest.raises(APIError) as exc_info:
            await api.get_account_summary()

        assert exc_info.value is service_error  # Error from service should propagate


@pytest.mark.asyncio
async def test_get_funding_rates_success(
    hl_api_instance: HyperliquidAPI,
    mock_meta_response_content: list[RawJsonResponse],
    mock_hyperliquid_mapper: MagicMock,
) -> None:
    """Test get_funding_rates successfully fetches and maps funding rates using
    _info_http_client."""
    # Mock the _info_http_client.request call on the specific hl_api_instance
    with patch.object(
        hl_api_instance._info_http_client,
        "request",
        new_callable=AsyncMock,
        return_value=(
            deepcopy(mock_meta_response_content),  # Use deepcopy
            200,
            MagicMock(),
            MagicMock(),
        ),
    ) as mock_info_http_client_request_call:
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

        # Patch the _hl_mapper for the scope of this test
        with patch.object(hl_api_instance, "_hl_mapper", mock_hyperliquid_mapper):
            result = await hl_api_instance.get_funding_rates()

        # The payload built by HyperliquidRequestBuilder.build_info_request_payload()
        # is HyperliquidRawMetaAndAssetCtxsRequestPayload(type="metaAndAssetCtxs")
        # We need to assert that the .request() method was called with the .model_dump() of this.
        expected_data_dict = {"type": "metaAndAssetCtxs"}  # type is not aliased in the model

        mock_info_http_client_request_call.assert_awaited_once_with(
            method="POST",
            endpoint_path="/info",
            data=expected_data_dict,
            rate_limiter_service=hl_api_instance._rate_limiter_service,
        )
        assert len(result) == 2
        assert mock_funding_rate_btc in result
        assert mock_funding_rate_eth in result


@pytest.mark.asyncio
async def test_get_funding_rates_api_error(hl_api_instance: HyperliquidAPI) -> None:
    """Test get_funding_rates raises APIError when HTTP request fails."""
    # The error here should be an HttpRequestFailedError, which gets wrapped.
    # The Pydantic validation error for missing 'name' should not occur if the HTTP
    # call itself fails first, as intended by this test.
    # However, if the mock data used *before* the intended failure had issues,
    # it could mask this. We ensure the side_effect raises before Pydantic validation.

    with patch.object(
        hl_api_instance._info_http_client,
        "request",
        new_callable=AsyncMock,
        side_effect=HttpRequestFailedError(
            message="Mock HTTP Error from /info endpoint",
            http_status_code=500,  # Example status code
            api_error_code=APIErrorCode.SERVICE_UNAVAILABLE,  # Example error code
        ),
    ) as mock_request_call:
        with pytest.raises(APIError) as excinfo:
            await hl_api_instance.get_funding_rates()

        assert excinfo.value.code == APIErrorCode.SERVICE_UNAVAILABLE.value
        assert isinstance(excinfo.value.original_exception, HttpRequestFailedError)
        assert "Mock HTTP Error from /info endpoint" in str(excinfo.value.message)

        mock_request_call.assert_called_once_with(
            method="POST",
            endpoint_path="/info",
            data={"type": "metaAndAssetCtxs"},
            rate_limiter_service=hl_api_instance._rate_limiter_service,
        )
