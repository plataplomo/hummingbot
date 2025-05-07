from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from pytest import LogCaptureFixture

from cyberdelta.apis.backpack.bp_auth import BackpackHmacAuthenticator
from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


@pytest.fixture
def mock_time_patch() -> Generator[MagicMock]:
    with patch("time.time", return_value=1678886400.0) as mock_time:
        yield mock_time


class TestBackpackHmacAuthenticator:
    def test_initialization(self) -> None:
        auth = BackpackHmacAuthenticator(api_key="test_key", api_secret="test_secret")
        assert auth._api_key == "test_key"
        assert auth._api_secret == "test_secret"

    def test_initialization_missing_credentials_logs_warning(
        self, caplog: LogCaptureFixture
    ) -> None:
        BackpackHmacAuthenticator(api_key="", api_secret="test_secret")
        assert (
            "BackpackHmacAuthenticator initialized with missing API key or secret." in caplog.text
        )
        caplog.clear()
        BackpackHmacAuthenticator(api_key="test_key", api_secret="")
        assert (
            "BackpackHmacAuthenticator initialized with missing API key or secret." in caplog.text
        )

    @pytest.mark.asyncio
    async def test_prepare_request_missing_credentials_raises_api_error(self) -> None:
        auth = BackpackHmacAuthenticator(api_key="", api_secret="")
        with pytest.raises(APIError) as exc_info:
            await auth.prepare_request("GET", "/test", None, None, None)
        assert exc_info.value.code == APIErrorCode.AUTHENTICATION_FAILED.value
        assert "API key and secret are required" in exc_info.value.message

    @pytest.mark.asyncio
    async def test_prepare_request_get_no_params(self, mock_time_patch: MagicMock) -> None:
        auth = BackpackHmacAuthenticator(api_key="testkey123", api_secret="secretkey456")
        fixed_timestamp_str = "1678886400000"
        # Expected signature: HMAC_SHA256("secretkey456", "1678886400000")
        # echo -n "1678886400000" | openssl dgst -sha256 -hmac "secretkey456"
        # -> (stdin)= c8b0b03025942d62f1039a273171939a1303758dd07966f020a014256905c172
        expected_signature = "c8b0b03025942d62f1039a273171939a1303758dd07966f020a014256905c172"

        components: AuthenticatedRequestComponents = await auth.prepare_request(
            method="GET", path="/api/v1/capital", params=None, data=None, headers=None
        )
        assert components["headers"]["X-Api-Key"] == "testkey123"
        assert components["headers"]["X-Timestamp"] == fixed_timestamp_str
        assert components["headers"]["X-Signature"] == expected_signature
        assert components["params"] is None
        assert components["data"] is None

    @pytest.mark.asyncio
    async def test_prepare_request_get_with_params(self, mock_time_patch: MagicMock) -> None:
        auth = BackpackHmacAuthenticator(api_key="testkey123", api_secret="secretkey456")
        params = {"symbol": "SOL_USDC", "limit": "10"}
        # Payload: timestamp + sorted query string
        # "1678886400000limit=10&symbol=SOL_USDC"
        # echo -n "1678886400000limit=10&symbol=SOL_USDC" |
        #   openssl dgst -sha256 -hmac "secretkey456"
        # -> (stdin)= 719299c86960f7247986608254250d14c248b3f4f78ff7129bfbe788c6519952
        expected_signature = "719299c86960f7247986608254250d14c248b3f4f78ff7129bfbe788c6519952"

        components = await auth.prepare_request(
            method="GET", path="/api/v1/orders", params=params, data=None, headers=None
        )
        assert components["headers"]["X-Signature"] == expected_signature
        assert components["params"] == params  # Params should be returned as is

    @pytest.mark.asyncio
    async def test_prepare_request_post_with_data(self, mock_time_patch: MagicMock) -> None:
        auth = BackpackHmacAuthenticator(api_key="testkey123", api_secret="secretkey456")
        data = {"symbol": "SOL_USDC", "quantity": "1.0", "side": "buy", "orderType": "market"}
        # Payload: timestamp + JSON string of data (sorted keys, no spaces)
        # '1678886400000{"orderType":"market","quantity":"1.0","side":"buy","symbol":"SOL_USDC"}'
        # echo -n '1678886400000{"orderType":"market","quantity":"1.0",' \
        #   '"side":"buy","symbol":"SOL_USDC"}' | openssl dgst -sha256 -hmac "secretkey456"
        # -> (stdin)= 673a805590bc592e2097a5f00e857d7060906a2f0c0d09833615e87c57193745
        expected_signature = "673a805590bc592e2097a5f00e857d7060906a2f0c0d09833615e87c57193745"

        components = await auth.prepare_request(
            method="POST", path="/api/v1/order", params=None, data=data, headers=None
        )
        assert components["headers"]["X-Signature"] == expected_signature
        assert components["headers"]["Content-Type"] == "application/json; charset=utf-8"
        assert components["data"] == data  # Data should be returned as is

    @pytest.mark.asyncio
    async def test_prepare_request_post_no_data(self, mock_time_patch: MagicMock) -> None:
        auth = BackpackHmacAuthenticator(api_key="testkey123", api_secret="secretkey456")
        # Expected signature (same as GET with no params)
        expected_signature = "c8b0b03025942d62f1039a273171939a1303758dd07966f020a014256905c172"

        components = await auth.prepare_request(
            method="POST",
            path="/api/v1/orderCancelAll",
            params=None,
            data=None,  # No data for this POST
            headers=None,
        )
        assert components["headers"]["X-Signature"] == expected_signature
        # Content-Type should not be added if there's no data
        assert "Content-Type" not in components["headers"]

    @pytest.mark.asyncio
    async def test_prepare_request_merges_existing_headers(
        self, mock_time_patch: MagicMock
    ) -> None:
        auth = BackpackHmacAuthenticator(api_key="testkey123", api_secret="secretkey456")
        existing_headers = {"X-Custom-Header": "CustomValue", "Content-Type": "application/xml"}
        data = {"key": "value"}
        components = await auth.prepare_request(
            method="POST", path="/api/v1/order", params=None, data=data, headers=existing_headers
        )
        assert components["headers"]["X-Custom-Header"] == "CustomValue"
        assert (
            components["headers"]["X-Api-Key"] == "testkey123"
        )  # Auth header takes precedence if conflict
        # Content-Type from existing_headers should be preserved if present
        assert components["headers"]["Content-Type"] == "application/xml"

    @pytest.mark.asyncio
    async def test_prepare_request_adds_content_type_if_missing_for_post_with_data(
        self, mock_time_patch: MagicMock
    ) -> None:
        auth = BackpackHmacAuthenticator(api_key="testkey123", api_secret="secretkey456")
        data = {"key": "value"}
        components = await auth.prepare_request(
            method="POST",
            path="/api/v1/order",
            params=None,
            data=data,
            headers={"X-Another": "Header"},  # No Content-Type here
        )
        assert components["headers"]["Content-Type"] == "application/json; charset=utf-8"
