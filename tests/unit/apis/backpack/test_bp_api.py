import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pytest import LogCaptureFixture

from cyberdelta.apis.backpack.bp_auth import BackpackHmacAuthenticator
from cyberdelta.apis.backpack_api import BackpackAPI
from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
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
