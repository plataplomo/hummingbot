from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from multidict import CIMultiDict, CIMultiDictProxy

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.connectivity.http_client import (
    HttpClient,
    HttpRequestFailedError,
)
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.rate_limiter import (
    TokenBucketRateLimiterRuntime,  # For type hinting limiter mock
)


@pytest.fixture
def mock_rate_limiter_service() -> RateLimiterService:
    service = MagicMock(spec=RateLimiterService)
    limiter_instance = AsyncMock(spec=TokenBucketRateLimiterRuntime)
    service.get_limiter.return_value = limiter_instance
    return service


@pytest.fixture
def mock_authenticator() -> IAuthenticator:
    auth = AsyncMock(spec=IAuthenticator)
    auth.prepare_request.return_value = AuthenticatedRequestComponents(
        headers={"X-Auth": "dummy_sig"},
        params={"auth_param": "val"},
        data={"auth_data": "val"},  # Authenticator could potentially modify data
    )
    return auth


@pytest.fixture
async def http_client_instance() -> AsyncGenerator[HttpClient]:
    client = HttpClient(exchange_name="test_exchange", rest_endpoint="http://test.api")
    yield client
    await client.close_session()  # Ensure session is closed after test


class TestHttpClient:
    @pytest.mark.asyncio
    async def test_get_session_creation_and_reuse(self, http_client_instance: HttpClient) -> None:
        """Test that a session is created and reused."""
        session1 = await http_client_instance._get_session()  # noqa: SLF001
        assert isinstance(session1, aiohttp.ClientSession)
        assert not session1.closed

        session2 = await http_client_instance._get_session()  # noqa: SLF001
        assert session1 is session2  # Should be the same session instance

        await http_client_instance.close_session()
        assert http_client_instance._session is None  # noqa: SLF001

        session3 = await http_client_instance._get_session()  # noqa: SLF001
        assert isinstance(session3, aiohttp.ClientSession)
        assert session1 is not session3  # Should be a new session instance
        assert not session3.closed

    @pytest.mark.asyncio
    async def test_close_session_idempotent(self, http_client_instance: HttpClient) -> None:
        """Test that closing the session is idempotent."""
        await http_client_instance._get_session()  # noqa: SLF001 # Create session
        await http_client_instance.close_session()
        assert http_client_instance._session is None  # noqa: SLF001
        await http_client_instance.close_session()  # Closing again should not error
        assert http_client_instance._session is None  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_async_context_manager(self) -> None:
        """Test the async context manager behavior for session management."""
        async with HttpClient(exchange_name="test_ctx", rest_endpoint="http://test.ctx") as client:
            assert client._session is not None  # noqa: SLF001
            assert not client._session.closed  # noqa: SLF001
        assert client._session is None  # noqa: SLF001

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_successful_json(
        self,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test a successful request returning JSON."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 200
        mock_response.headers = CIMultiDictProxy(CIMultiDict({"Content-Type": "application/json"}))
        mock_response.json = AsyncMock(return_value={"data": "success"})
        mock_response.text = AsyncMock(return_value='{"data": "success"}')
        mock_request.return_value.__aenter__.return_value = mock_response

        content, headers = await http_client_instance.request(
            method="GET", endpoint_path="/test", rate_limiter_service=mock_rate_limiter_service
        )
        assert content == {"data": "success"}
        assert headers["Content-Type"] == "application/json"
        mock_rate_limiter_service.get_limiter.assert_called_once_with("GET", "/test")  # type: ignore [attr-defined]
        mock_rate_limiter_service.get_limiter.return_value.acquire.assert_called_once()  # type: ignore [attr-defined]
        session_for_headers = await http_client_instance._get_session()  # noqa: SLF001
        mock_request.assert_called_once_with(
            "GET",
            "http://test.api/test",
            params=None,
            json=None,
            data=None,
            headers=session_for_headers.headers,
            timeout=aiohttp.ClientTimeout(total=http_client_instance.default_request_timeout),
        )

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_successful_text(
        self,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test a successful request returning plain text."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 200
        mock_response.headers = CIMultiDictProxy(CIMultiDict({"Content-Type": "text/plain"}))
        mock_response.text = AsyncMock(return_value="Hello World")
        mock_request.return_value.__aenter__.return_value = mock_response

        content, headers = await http_client_instance.request(
            method="GET", endpoint_path="/text", rate_limiter_service=mock_rate_limiter_service
        )
        assert content == "Hello World"
        assert headers["Content-Type"] == "text/plain"

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_204_no_content(
        self,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test a request that returns 204 No Content."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 204
        mock_response.headers = CIMultiDictProxy(CIMultiDict())
        mock_response.text = AsyncMock(return_value="")  # Should not be called if status is 204
        mock_request.return_value.__aenter__.return_value = mock_response

        content, _ = await http_client_instance.request(  # headers variable removed as unused
            method="POST",
            endpoint_path="/empty",
            rate_limiter_service=mock_rate_limiter_service,
            data={},
        )
        assert content is None
        mock_response.text.assert_not_called()  # Text should not be read for 204

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_signed(
        self,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
        mock_authenticator: IAuthenticator,
    ) -> None:
        """Test a signed request correctly uses the authenticator."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 200
        mock_response.headers = CIMultiDictProxy(CIMultiDict({"Content-Type": "application/json"}))
        mock_response.json = AsyncMock(return_value={"status": "ok"})
        mock_response.text = AsyncMock(return_value='{"status": "ok"}')
        mock_request.return_value.__aenter__.return_value = mock_response

        original_headers = {"X-Client-Header": "client_val"}
        original_params = {"client_param": "val"}
        original_data = {"client_data": "val"}

        await http_client_instance.request(
            method="POST",
            endpoint_path="/signed_action",
            rate_limiter_service=mock_rate_limiter_service,
            authenticator=mock_authenticator,
            params=original_params.copy(),
            data=original_data.copy(),
            headers=original_headers.copy(),
            is_signed=True,
        )

        mock_authenticator.prepare_request.assert_called_once_with(  # type: ignore [attr-defined]
            method="POST",
            path="/signed_action",
            params=original_params,  # Authenticator receives original params
            data=original_data,  # Authenticator receives original data
            headers=original_headers,  # Authenticator receives original headers
        )
        # Check that headers from authenticator are in the final request headers
        # and original client headers are also present if not overwritten
        final_call_headers = mock_request.call_args[1]["headers"]
        assert final_call_headers["X-Auth"] == "dummy_sig"
        assert final_call_headers["X-Client-Header"] == "client_val"
        # Check that params from authenticator are used
        final_call_params = mock_request.call_args[1]["params"]
        assert final_call_params["auth_param"] == "val"
        assert final_call_params["client_param"] == "val"
        # Check that data for the request body is the authenticator's modified data
        # if it returns one, or original data if authenticator returns None for data.
        # Current mock_authenticator returns data, so that should be used.
        # HttpClient.request logic:
        # json=request_data if method not GET/DELETE and request_data is not None else None,
        # data=None if method not GET/DELETE and request_data is not None else request_data
        # This means original_data is used for JSON body in POST/PUT.
        # The authenticator mock returns 'auth_data', but this isn't directly used as body.
        # The test should reflect HttpClient's actual behavior.
        final_call_json_data = mock_request.call_args[1]["json"]
        assert (
            final_call_json_data == original_data
        )  # HttpClient currently sends original data as json

    @pytest.mark.asyncio
    async def test_request_signed_no_authenticator_raises_api_error(
        self, http_client_instance: HttpClient, mock_rate_limiter_service: RateLimiterService
    ) -> None:
        """Test signed request raises APIError if no authenticator is provided."""
        with pytest.raises(APIError) as excinfo:
            await http_client_instance.request(
                method="POST",
                endpoint_path="/needs_auth",
                rate_limiter_service=mock_rate_limiter_service,
                is_signed=True,  # But no authenticator passed
            )
        assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value

    @pytest.mark.asyncio
    async def test_authenticator_prepare_request_raises_api_error(
        self,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
        mock_authenticator: IAuthenticator,
    ) -> None:
        """Test that if authenticator.prepare_request fails, the APIError is propagated."""
        mock_authenticator.prepare_request.side_effect = APIError(  # type: ignore [attr-defined]
            "Auth Prep Failed", code=APIErrorCode.AUTHENTICATION_FAILED.value
        )
        with pytest.raises(APIError) as excinfo:
            await http_client_instance.request(
                method="POST",
                endpoint_path="/auth_fail",
                rate_limiter_service=mock_rate_limiter_service,
                authenticator=mock_authenticator,
                is_signed=True,
            )
        assert excinfo.value.message == "Auth Prep Failed"
        assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_http_error_400_raises_HttpRequestFailedError_no_retry(
        self,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test that a 400 error raises HttpRequestFailedError immediately without retry."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 400
        mock_response.headers = CIMultiDictProxy(CIMultiDict())
        error_body = '{"error": "Bad Request"}'
        mock_response.text = AsyncMock(return_value=error_body)
        mock_request.return_value.__aenter__.return_value = mock_response

        http_client_instance.max_retries = 3  # Ensure retries are configured

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/bad_req", mock_rate_limiter_service)

        assert excinfo.value.http_status == 400
        assert excinfo.value.exchange_message == error_body
        mock_request.assert_called_once()  # Should only be called once, no retry

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    @patch("asyncio.sleep", new_callable=AsyncMock)  # Mock asyncio.sleep
    async def test_http_error_500_retries_then_raises(
        self,
        mock_sleep: AsyncMock,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test that a 500 error is retried and then raises HttpRequestFailedError."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 500
        mock_response.headers = CIMultiDictProxy(CIMultiDict())
        error_body = '{"error": "Server Error"}'
        mock_response.text = AsyncMock(return_value=error_body)
        mock_request.return_value.__aenter__.return_value = mock_response

        http_client_instance.max_retries = 2  # Configure for 2 retries (3 attempts total)
        http_client_instance.retry_delay_seconds = 0.01  # Speed up test

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/server_err", mock_rate_limiter_service)

        assert excinfo.value.http_status == 500
        assert excinfo.value.exchange_message == error_body
        assert mock_request.call_count == 3  # Initial call + 2 retries
        assert mock_sleep.call_count == 2  # Sleep called before each retry
        mock_sleep.assert_any_call(0.01 * (2**0))  # Delay for first retry
        mock_sleep.assert_any_call(0.01 * (2**1))  # Delay for second retry

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_client_error_retries_then_raises(
        self,
        mock_sleep: AsyncMock,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test aiohttp.ClientError is retried and then re-raised."""
        mock_request.side_effect = aiohttp.ClientConnectorError(MagicMock(), MagicMock())

        http_client_instance.max_retries = 1  # 1 retry (2 attempts total)
        http_client_instance.retry_delay_seconds = 0.01

        with pytest.raises(aiohttp.ClientConnectorError):
            await http_client_instance.request("GET", "/client_err", mock_rate_limiter_service)

        assert mock_request.call_count == 2
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_with(0.01)

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_url_construction(
        self,
        mock_request: AsyncMock,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test correct URL construction including stripping slashes."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 204
        mock_response.headers = CIMultiDictProxy(CIMultiDict())
        mock_response.text = AsyncMock(return_value="")
        mock_request.return_value.__aenter__.return_value = mock_response

        # Test with leading/trailing slashes in different places
        client_no_trailing_slash = HttpClient("test", "http://test.api")
        session_no_trailing = await client_no_trailing_slash._get_session()  # noqa: SLF001
        await client_no_trailing_slash.request("GET", "/path1", mock_rate_limiter_service)
        mock_request.assert_called_with(
            "GET",
            "http://test.api/path1",
            params=None,
            json=None,
            data=None,
            headers=session_no_trailing.headers,
            timeout=aiohttp.ClientTimeout(total=client_no_trailing_slash.default_request_timeout),
        )
        await client_no_trailing_slash.close_session()
        mock_request.reset_mock()

        client_with_trailing_slash = HttpClient("test", "http://test.api/")
        session_with_trailing = await client_with_trailing_slash._get_session()  # noqa: SLF001
        await client_with_trailing_slash.request("GET", "path2", mock_rate_limiter_service)
        mock_request.assert_called_with(
            "GET",
            "http://test.api/path2",
            params=None,
            json=None,
            data=None,
            headers=session_with_trailing.headers,
            timeout=aiohttp.ClientTimeout(total=client_with_trailing_slash.default_request_timeout),
        )
        await client_with_trailing_slash.close_session()
        mock_request.reset_mock()

        client_both_slashes = HttpClient("test", "http://test.api/")
        session_both_slashes = await client_both_slashes._get_session()  # noqa: SLF001
        await client_both_slashes.request("GET", "/path3/", mock_rate_limiter_service)
        mock_request.assert_called_with(
            "GET",
            "http://test.api/path3/",
            params=None,
            json=None,
            data=None,
            headers=session_both_slashes.headers,
            timeout=aiohttp.ClientTimeout(total=client_both_slashes.default_request_timeout),
        )  # Path with trailing slash is preserved
        await client_both_slashes.close_session()

    # TODO: Add tests for:
    # - JSON decode error handling (returning raw text)
    # - TimeoutError retry and raise
    # - Correct passing of request_timeout override
    # - Correct handling of data vs json parameter in
    #   aiohttp.ClientSession.request for different methods
