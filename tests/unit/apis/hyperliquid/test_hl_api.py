"""
Unit tests for the HyperliquidAPI class, focusing on authenticator integration.
"""

from collections.abc import Generator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from _pytest.logging import LogCaptureFixture

from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.hl_auth import HL_Eip712Authenticator
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

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
    """Mocks the HL_Eip712Authenticator initialization."""
    with patch("cyberdelta.apis.hyperliquid.hl_api.HL_Eip712Authenticator") as mock_auth_class:
        mock_instance = MagicMock(spec=HL_Eip712Authenticator)
        mock_instance.prepare_request = AsyncMock()  # Add async mock for prepare_request
        mock_auth_class.return_value = mock_instance
        yield mock_auth_class, mock_instance  # Return class and instance mock


# --- Initialization Tests --- #


def test_hl_api_init_with_key(mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any]):
    """Test successful initialization when private key is provided."""
    mock_auth_class, mock_instance = next(mock_hl_auth_init)
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    mock_auth_class.assert_called_once_with(
        private_key_hex=TEST_PRIVATE_KEY,
        wallet_address=TEST_WALLET_ADDRESS,
        chain_id=HyperliquidAPI.CHAIN_ID,
    )
    assert api.authenticator is mock_instance
    assert api._hl_authenticator is mock_instance  # noqa: SLF001 # Check internal ref too


def test_hl_api_init_without_key(mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any]):
    """Test initialization when private key is None."""
    mock_auth_class, _ = next(mock_hl_auth_init)
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_NO_KEY)

    mock_auth_class.assert_not_called()
    assert api.authenticator is None
    assert api._hl_authenticator is None  # noqa: SLF001


def test_hl_api_init_auth_init_fails(
    mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any], caplog: LogCaptureFixture
):
    """Test initialization when HL_Eip712Authenticator fails to initialize."""
    mock_auth_class, _ = next(mock_hl_auth_init)
    mock_auth_class.side_effect = ValueError("Bad key format")

    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_WITH_KEY)

    mock_auth_class.assert_called_once()  # Still attempted
    assert api.authenticator is None
    assert api._hl_authenticator is None  # noqa: SLF001
    assert "Failed to initialize Hyperliquid authenticator: Bad key format" in caplog.text


def test_hl_api_init_no_address(
    mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any], caplog: LogCaptureFixture
):
    """Test initialization logs error if wallet address is missing."""
    mock_auth_class, _ = next(mock_hl_auth_init)
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_NO_ADDRESS)

    mock_auth_class.assert_not_called()  # Authenticator shouldn't be called without address
    assert api.authenticator is None
    assert api._hl_authenticator is None  # noqa: SLF001
    assert "Wallet address is required but not provided" in caplog.text


# --- _authenticate Method Tests --- #


@pytest.mark.asyncio
async def test_authenticate_success(mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any]):
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
):
    """Test _authenticate raises APIError if no authenticator is configured."""
    _, _ = next(mock_hl_auth_init)
    # Initialize without key so authenticator is None
    api = HyperliquidAPI(api_config=BASE_API_CONFIG, secrets=SECRETS_NO_KEY)
    assert api.authenticator is None

    with pytest.raises(APIError, match="without a configured Hyperliquid authenticator") as excinfo:
        await api._authenticate("POST", "/exchange", None, {"d": 1})  # noqa: SLF001
    assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value


@pytest.mark.asyncio
async def test_authenticate_prepare_request_fails(
    mock_hl_auth_init: Generator[tuple[MagicMock, MagicMock], Any],
):
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
):
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

            from cyberdelta.core.models import OrderSide, OrderType, TimeInForce

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
            assert call_args[3] == order_action_data
            assert call_args[4] == api.default_headers.copy()

            mock_request.assert_awaited_once()
            req_call_args, req_call_kwargs = mock_request.call_args
            assert req_call_args[0] == "POST"
            assert req_call_args[1] == "/exchange"
            assert req_call_kwargs.get("data") == auth_data
            final_headers = api.default_headers.copy()
            final_headers.update(auth_headers)
            assert req_call_kwargs.get("headers") == final_headers
            assert req_call_kwargs.get("is_signed") is True

            assert final_order is mock_final_order
