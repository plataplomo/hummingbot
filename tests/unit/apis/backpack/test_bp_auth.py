from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest
from pytest import LogCaptureFixture

from cyberdelta.apis.backpack.bp_auth import BackpackHmacAuthenticator
from cyberdelta.apis.base.authenticator_interface import AuthenticatedRequestComponents


@pytest.fixture
def mock_time_patch() -> Generator[MagicMock]:
    with patch("time.time", return_value=1678886400.0) as mock_time:
        yield mock_time


class TestBackpackHmacAuthenticator:
    def test_initialization(self) -> None:
        auth = BackpackHmacAuthenticator(api_key="test_key", api_secret="test_secret")
        assert auth._api_key == "test_key"
        assert auth._api_secret == "test_secret"

    def test_initialization_missing_key_raises_value_error(self, caplog: LogCaptureFixture) -> None:
        with pytest.raises(ValueError, match="API key cannot be empty"):
            BackpackHmacAuthenticator(api_key="", api_secret="test_secret")
        assert "API key cannot be empty for BackpackHmacAuthenticator." in caplog.text

    def test_initialization_missing_secret_raises_value_error(
        self, caplog: LogCaptureFixture
    ) -> None:
        with pytest.raises(ValueError, match="API secret cannot be empty"):
            BackpackHmacAuthenticator(api_key="test_api_key", api_secret="")
        assert "API secret cannot be empty for BackpackHmacAuthenticator." in caplog.text

    @pytest.mark.asyncio
    async def test_init_missing_both_credentials_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="API key cannot be empty"):
            BackpackHmacAuthenticator(api_key="", api_secret="")

    @pytest.mark.asyncio
    async def test_prepare_request_get_no_params(self, mock_time_patch: MagicMock) -> None:
        auth = BackpackHmacAuthenticator(api_key="testkey123", api_secret="secretkey456")
        fixed_timestamp_str = "1678886400000"
        # Expected signature: HMAC_SHA256("secretkey456", "1678886400000")
        # echo -n "1678886400000" | openssl dgst -sha256 -hmac "secretkey456"
        # -> (stdin)= c8b0b03025942d62f1039a273171939a1303758dd07966f020a014256905c172
        expected_signature = "fa7dedc8ff5e7dbe49d0458e1db1b205324c7feb03dea0d321fcc9724bb3e581"

        components: AuthenticatedRequestComponents = await auth.prepare_request(
            method="GET", path="/api/v1/capital", params=None, data=None, headers=None
        )
        print(
            f"[DEBUG BP_TESTS] Actual signature for GET no_params: "
            f"{components['headers']['X-Signature']}"
        )  # DEBUG PRINT
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
        expected_signature = "b3b424d113a609f040d92c8bf5ee0b37dd0bbed0fef8232a8fd6f055c0decaa8"

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
        expected_signature = "af7bea788a14abcfd2fb1c96c6344d962fc1232dfa4d13dd5d997243708bdfb7"

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
        expected_signature = "fa7dedc8ff5e7dbe49d0458e1db1b205324c7feb03dea0d321fcc9724bb3e581"

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
