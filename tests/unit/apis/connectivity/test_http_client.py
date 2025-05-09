import json  # For JSONDecodeError test
from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import pytest_asyncio
from multidict import CIMultiDict, CIMultiDictProxy
from pydantic import HttpUrl, ValidationError  # Added ValidationError

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.connectivity.connectivity_models import (
    MAX_CONTENT_TYPE_LENGTH,  # For testing invalid content type
    HttpClientConfig,
    ProcessedResponseHeaders,
)
from cyberdelta.apis.connectivity.http_client import (
    HttpClient,
    HttpRequestFailedError,
)
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.rate_limiter import (
    TokenBucketRateLimiterRuntime,
)


@pytest.fixture
def default_http_client_config() -> HttpClientConfig:
    return HttpClientConfig(rest_endpoint=HttpUrl("http://test.api"))


@pytest.fixture
def mock_rate_limiter_service() -> RateLimiterService:
    service = MagicMock(spec=RateLimiterService)
    limiter_instance = AsyncMock(spec=TokenBucketRateLimiterRuntime)
    service.get_limiter.return_value = limiter_instance
    return service  # Workaround for type checker struggling with MagicMock as RateLimiterService


@pytest.fixture
def mock_authenticator() -> IAuthenticator:
    auth = AsyncMock(spec=IAuthenticator)
    auth.prepare_request.return_value = AuthenticatedRequestComponents(
        headers={"X-Auth": "dummy_sig"},
        params={"auth_param": "val"},
        data=None,  # Authenticator typically doesn't modify body data, just params/headers
    )
    return auth  # Workaround


@pytest_asyncio.fixture
async def http_client_instance(
    default_http_client_config: HttpClientConfig,
) -> AsyncGenerator[HttpClient]:  # Added None to AsyncGenerator type
    client = HttpClient(exchange_name="test_exchange", config=default_http_client_config)
    yield client
    await client.close_session()


class TestHttpClient:
    @pytest.mark.asyncio
    async def test_get_session_creation_and_reuse(self, http_client_instance: HttpClient) -> None:
        """Test that a session is created and reused."""
        session1 = await http_client_instance._get_session()  # noqa: SLF001
        assert isinstance(session1, aiohttp.ClientSession)
        assert not session1.closed

        session2 = await http_client_instance._get_session()  # noqa: SLF001
        assert session1 is session2

        await http_client_instance.close_session()
        assert http_client_instance._session is None  # noqa: SLF001

        session3 = await http_client_instance._get_session()  # noqa: SLF001
        assert isinstance(session3, aiohttp.ClientSession)
        assert session1 is not session3
        assert not session3.closed

    @pytest.mark.asyncio
    async def test_close_session_idempotent(self, http_client_instance: HttpClient) -> None:
        """Test that closing the session is idempotent."""
        await http_client_instance._get_session()  # noqa: SLF001
        await http_client_instance.close_session()
        assert http_client_instance._session is None  # noqa: SLF001
        await http_client_instance.close_session()
        assert http_client_instance._session is None  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_async_context_manager(
        self, default_http_client_config: HttpClientConfig
    ) -> None:
        """Test the async context manager behavior for session management."""
        async with HttpClient(
            exchange_name="test_ctx", config=default_http_client_config
        ) as client:
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
        default_http_client_config: HttpClientConfig,
    ) -> None:
        """Test a successful request returning JSON."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 200
        mock_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json; charset=utf-8"})
        )
        mock_response.text = AsyncMock(return_value='{"data": "success"}')
        mock_request.return_value.__aenter__.return_value = mock_response

        content, processed_headers, raw_headers = await http_client_instance.request(
            method="GET", endpoint_path="/test", rate_limiter_service=mock_rate_limiter_service
        )
        assert content == {"data": "success"}
        assert isinstance(processed_headers, ProcessedResponseHeaders)
        assert processed_headers.content_type == "application/json; charset=utf-8"
        assert isinstance(raw_headers, CIMultiDictProxy)
        assert raw_headers.get("Content-Type") == "application/json; charset=utf-8"

        mock_rate_limiter_service.get_limiter.assert_called_once_with("GET", "/test")  # type: ignore[attr-defined]
        mock_rate_limiter_service.get_limiter.return_value.acquire.assert_called_once()  # type: ignore[attr-defined]
        session_for_headers = await http_client_instance._get_session()  # noqa: SLF001
        mock_request.assert_called_once_with(
            "GET",
            str(default_http_client_config.rest_endpoint).rstrip("/") + "/test",
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
        default_http_client_config: HttpClientConfig,  # Added fixture
    ) -> None:
        """Test a successful request returning plain text."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 200
        mock_response.headers = CIMultiDictProxy(CIMultiDict[str]({"Content-Type": "text/plain"}))
        mock_response.text = AsyncMock(return_value="Hello World")
        mock_request.return_value.__aenter__.return_value = mock_response

        content, processed_headers, raw_headers = await http_client_instance.request(
            method="GET", endpoint_path="/text", rate_limiter_service=mock_rate_limiter_service
        )
        assert content == "Hello World"
        assert isinstance(processed_headers, ProcessedResponseHeaders)
        assert processed_headers.content_type == "text/plain"
        assert isinstance(raw_headers, CIMultiDictProxy)
        assert raw_headers.get("Content-Type") == "text/plain"

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_204_no_content(
        self,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
        default_http_client_config: HttpClientConfig,  # Added fixture
    ) -> None:
        """Test a request that returns 204 No Content."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 204
        mock_response.headers = CIMultiDictProxy(CIMultiDict[str]())
        mock_response.text = AsyncMock(return_value="")
        mock_request.return_value.__aenter__.return_value = mock_response

        content, processed_headers, raw_headers = await http_client_instance.request(
            method="POST",
            endpoint_path="/empty",
            rate_limiter_service=mock_rate_limiter_service,
            data={},
        )
        assert content is None
        assert isinstance(processed_headers, ProcessedResponseHeaders)
        assert processed_headers.content_type == ""
        assert isinstance(raw_headers, CIMultiDictProxy)
        mock_response.text.assert_not_called()

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_signed(
        self,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
        mock_authenticator: IAuthenticator,
        default_http_client_config: HttpClientConfig,
    ) -> None:
        """Test a signed request correctly uses the authenticator."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 200
        mock_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json"})
        )
        mock_response.text = AsyncMock(return_value='{"status": "ok"}')
        mock_request.return_value.__aenter__.return_value = mock_response

        original_headers = {"X-Client-Header": "client_val"}
        original_params = {"client_param": "val"}
        original_data = {"client_data": "val"}

        _, processed_headers, raw_headers = await http_client_instance.request(
            method="POST",
            endpoint_path="/signed_action",
            rate_limiter_service=mock_rate_limiter_service,
            authenticator=mock_authenticator,
            params=original_params.copy(),
            data=original_data.copy(),
            headers=original_headers.copy(),
            is_signed=True,
        )
        assert isinstance(processed_headers, ProcessedResponseHeaders)
        assert processed_headers.content_type == "application/json"
        assert isinstance(raw_headers, CIMultiDictProxy)

        session = await http_client_instance._get_session()  # noqa: SLF001
        expected_headers_for_auth = session.headers.copy()
        expected_headers_for_auth.update(original_headers)

        mock_authenticator.prepare_request.assert_called_once_with(  # type: ignore[attr-defined]
            method="POST",
            path="/signed_action",
            params=original_params,
            data=original_data,
            headers=dict(expected_headers_for_auth),
        )
        final_call_headers = mock_request.call_args[1]["headers"]
        assert final_call_headers["X-Auth"] == "dummy_sig"
        assert final_call_headers["X-Client-Header"] == "client_val"
        final_call_params = mock_request.call_args[1]["params"]
        assert final_call_params["auth_param"] == "val"
        # Check if original_params were augmented or replaced
        if mock_authenticator.prepare_request.return_value["params"] is not original_params:  # type: ignore[attr-defined]
            assert "client_param" not in final_call_params
        else:
            assert final_call_params["client_param"] == "val"

        final_call_json_data = mock_request.call_args[1]["json"]
        assert final_call_json_data == original_data

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
                is_signed=True,
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
        auth_error = APIError("Auth Prep Failed", code=APIErrorCode.AUTHENTICATION_FAILED.value)
        mock_authenticator.prepare_request.side_effect = auth_error  # type: ignore[attr-defined]
        with pytest.raises(APIError) as excinfo:
            await http_client_instance.request(
                method="POST",
                endpoint_path="/auth_fail",
                rate_limiter_service=mock_rate_limiter_service,
                authenticator=mock_authenticator,
                is_signed=True,
            )
        assert excinfo.value is auth_error  # Check it's the same exception instance

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
        mock_response.headers = CIMultiDictProxy(CIMultiDict[str]())
        error_body = '{"error": "Bad Request"}'
        mock_response.text = AsyncMock(return_value=error_body)
        mock_request.return_value.__aenter__.return_value = mock_response

        http_client_instance.max_retries = 3

        with pytest.raises(HttpRequestFailedError) as excinfo:
            # Unpack all three return values even if not all are used in assertions for this test
            await http_client_instance.request("GET", "/bad_req", mock_rate_limiter_service)

        assert excinfo.value.http_status == 400
        assert excinfo.value.exchange_message == error_body
        mock_request.assert_called_once()

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    @patch("asyncio.sleep", new_callable=AsyncMock)
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
        mock_response.headers = CIMultiDictProxy(CIMultiDict[str]())
        error_body = '{"error": "Server Error"}'
        mock_response.text = AsyncMock(return_value=error_body)
        mock_request.return_value.__aenter__.return_value = mock_response

        http_client_instance.max_retries = 2
        http_client_instance.retry_delay_seconds = 0.01

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/server_err", mock_rate_limiter_service)

        assert excinfo.value.http_status == 500
        assert excinfo.value.exchange_message == error_body
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(0.01 * (2**0))
        mock_sleep.assert_any_call(0.01 * (2**1))

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
        client_error = aiohttp.ClientConnectorError(MagicMock(), MagicMock())
        mock_request.side_effect = client_error

        http_client_instance.max_retries = 1
        http_client_instance.retry_delay_seconds = 0.01

        with pytest.raises(aiohttp.ClientConnectorError) as excinfo:
            await http_client_instance.request("GET", "/client_err", mock_rate_limiter_service)

        assert excinfo.value is client_error  # Check it's the same exception instance
        assert mock_request.call_count == 2
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_with(0.01)

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_url_construction(
        self,
        mock_request: AsyncMock,
        mock_rate_limiter_service: RateLimiterService,
        default_http_client_config: HttpClientConfig,  # Added fixture
    ) -> None:
        """Test that URLs are constructed correctly."""
        # Scenario 1: Relative path with base URL not ending in slash
        config1 = HttpClientConfig(rest_endpoint=HttpUrl("http://base.url/v1"))
        async with HttpClient(exchange_name="url_test1", config=config1) as client1:
            session1 = await client1._get_session()  # noqa: SLF001
            mock_response1 = AsyncMock(spec=aiohttp.ClientResponse)
            mock_response1.status = 200
            mock_response1.headers = CIMultiDictProxy(CIMultiDict[str]())
            mock_response1.text = AsyncMock(return_value="")
            mock_request.return_value.__aenter__.return_value = mock_response1

            await client1.request("GET", "/path1", rate_limiter_service=mock_rate_limiter_service)
            mock_request.assert_called_with(
                "GET",
                "http://base.url/v1/path1",
                headers=session1.headers,
                params=None,
                json=None,
                data=None,
                timeout=aiohttp.ClientTimeout(total=client1.default_request_timeout),
            )
            mock_request.reset_mock()  # Reset for next call

            # Scenario 2: Relative path (no leading slash) with base URL ending in slash
            await client1.request(
                "GET", "path1_no_lead_slash", rate_limiter_service=mock_rate_limiter_service
            )
            mock_request.assert_called_with(
                "GET",
                "http://base.url/v1/path1_no_lead_slash",  # Check slash logic
                headers=session1.headers,
                params=None,
                json=None,
                data=None,
                timeout=aiohttp.ClientTimeout(total=client1.default_request_timeout),
            )
            mock_request.reset_mock()

        # Scenario 3: Absolute URL in endpoint_path
        config2 = HttpClientConfig(rest_endpoint=HttpUrl("http://shouldbeignored.com"))
        async with HttpClient(exchange_name="url_test2", config=config2) as client2:
            session2 = await client2._get_session()  # noqa: SLF001
            mock_response2 = AsyncMock(spec=aiohttp.ClientResponse)
            mock_response2.status = 200
            mock_response2.headers = CIMultiDictProxy(CIMultiDict[str]())
            mock_response2.text = AsyncMock(return_value="")
            mock_request.return_value.__aenter__.return_value = mock_response2

            await client2.request(
                "GET",
                "https://specific.api.com/specific/path",
                rate_limiter_service=mock_rate_limiter_service,
            )
            mock_request.assert_called_with(
                "GET",
                "https://specific.api.com/specific/path",
                headers=session2.headers,
                params=None,
                json=None,
                data=None,
                timeout=aiohttp.ClientTimeout(total=client2.default_request_timeout),
            )

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_custom_timeout(
        self,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
        default_http_client_config: HttpClientConfig,
    ) -> None:
        """Test that a custom request_timeout is used."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 200
        mock_response.headers = CIMultiDictProxy(CIMultiDict[str]({}))
        mock_response.text = AsyncMock(return_value="ok")
        mock_request.return_value.__aenter__.return_value = mock_response

        custom_timeout = 15.5
        await http_client_instance.request(
            method="GET",
            endpoint_path="/custom_timeout_test",
            rate_limiter_service=mock_rate_limiter_service,
            request_timeout=custom_timeout,
        )
        session_for_headers = await http_client_instance._get_session()  # noqa: SLF001
        mock_request.assert_called_once_with(
            "GET",
            str(default_http_client_config.rest_endpoint).rstrip("/") + "/custom_timeout_test",
            params=None,
            json=None,
            data=None,
            headers=session_for_headers.headers,
            timeout=aiohttp.ClientTimeout(total=custom_timeout),  # Check custom timeout here
        )

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_json_decode_error(
        self,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test request raises HttpRequestFailedError on JSONDecodeError."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 200
        mock_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json"})
        )
        invalid_json_text = "this is not json"
        mock_response.text = AsyncMock(return_value=invalid_json_text)
        mock_request.return_value.__aenter__.return_value = mock_response

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/invalid_json", mock_rate_limiter_service)

        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert invalid_json_text in str(excinfo.value.exchange_message or "")
        assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_payload_error_on_body_read(
        self,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test request raises HttpRequestFailedError on ClientPayloadError during body read."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 200  # Successful status initially
        mock_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json"})
        )
        payload_error = aiohttp.ClientPayloadError("Failed to read payload")
        mock_response.text = AsyncMock(side_effect=payload_error)
        mock_request.return_value.__aenter__.return_value = mock_response

        # HttpClient is configured for retries by default, but ClientPayloadError on a 200
        # might not be retryable by default depending on how it's caught.
        # Current implementation: it raises HttpRequestFailedError, which then triggers retries
        # if status is retryable (e.g. 5xx) or if it's a ClientError.
        # For a 200 response, a ClientPayloadError might not be retried based on status.
        # Let's test the immediate raise first, assuming default retry settings might not catch this for 200.

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request(
                "GET", "/payload_error_path", mock_rate_limiter_service
            )

        assert excinfo.value.http_status == 200  # Status from before body read failed
        assert excinfo.value.code == APIErrorCode.NETWORK_ISSUE.value
        assert "Failed to read response body" in excinfo.value.message
        assert isinstance(excinfo.value.__cause__, aiohttp.ClientPayloadError)

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_invalid_content_type_header_from_server(
        self,
        mock_request: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test request raises HttpRequestFailedError for invalid Content-Type from server."""
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 200
        # Invalid content type: too long
        invalid_content_type = "a" * (MAX_CONTENT_TYPE_LENGTH + 10)
        mock_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": invalid_content_type})
        )
        mock_response.text = AsyncMock(return_value='{"data":"success"}')  # Body might be fine
        mock_request.return_value.__aenter__.return_value = mock_response

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request(
                "GET", "/invalid_content_type", mock_rate_limiter_service
            )

        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid Content-Type header from server" in excinfo.value.message
        assert isinstance(excinfo.value.__cause__, ValidationError)
