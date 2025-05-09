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
        session1 = await http_client_instance._get_session()
        assert isinstance(session1, aiohttp.ClientSession)
        assert not session1.closed

        session2 = await http_client_instance._get_session()
        assert session1 is session2

        await http_client_instance.close_session()
        assert http_client_instance._session is None

        session3 = await http_client_instance._get_session()
        assert isinstance(session3, aiohttp.ClientSession)
        assert session1 is not session3

    @pytest.mark.asyncio
    async def test_close_session_idempotent(self, http_client_instance: HttpClient) -> None:
        """Test that closing the session is idempotent."""
        await http_client_instance._get_session()
        await http_client_instance.close_session()
        assert http_client_instance._session is None
        await http_client_instance.close_session()
        assert http_client_instance._session is None

    @pytest.mark.asyncio
    async def test_async_context_manager(
        self, default_http_client_config: HttpClientConfig
    ) -> None:
        """Test the async context manager behavior for session management."""
        async with HttpClient(
            exchange_name="test_ctx", config=default_http_client_config
        ) as client:
            assert client._session is not None
            assert not client._session.closed
        assert client._session is None

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.HttpClient._parse_and_validate_response")
    @patch("aiohttp.ClientSession.request")
    async def test_request_successful_json(
        self,
        mock_session_request: AsyncMock,
        mock_parse_response: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
        default_http_client_config: HttpClientConfig,
    ) -> None:
        """Test a successful request returning JSON, with parsing mocked."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        # Headers/text on mock_aio_response are less critical now as parsing is mocked
        mock_session_request.return_value.__aenter__.return_value = mock_aio_response

        expected_content = {"data": "success"}
        expected_processed_headers = ProcessedResponseHeaders(
            content_type="application/json; charset=utf-8"
        )
        expected_raw_headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json; charset=utf-8"})
        )
        mock_parse_response.return_value = (
            expected_content,
            expected_processed_headers,
            expected_raw_headers,
        )

        content, processed_headers, raw_headers = await http_client_instance.request(
            method="GET", endpoint_path="/test", rate_limiter_service=mock_rate_limiter_service
        )

        assert content == expected_content
        assert processed_headers == expected_processed_headers
        assert raw_headers == expected_raw_headers

        mock_rate_limiter_service.get_limiter.assert_called_once_with("GET", "/test")  # type: ignore[attr-defined]
        mock_rate_limiter_service.get_limiter.return_value.acquire.assert_called_once()  # type: ignore[attr-defined]

        full_expected_url = str(default_http_client_config.rest_endpoint).rstrip("/") + "/test"
        session_for_headers = await http_client_instance._get_session()
        mock_session_request.assert_called_once_with(
            "GET",
            full_expected_url,
            params=None,
            json=None,
            data=None,
            headers=session_for_headers.headers,
            timeout=aiohttp.ClientTimeout(total=http_client_instance.default_request_timeout),
        )
        mock_parse_response.assert_called_once_with(mock_aio_response, full_expected_url)

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.HttpClient._parse_and_validate_response")
    @patch("aiohttp.ClientSession.request")
    async def test_request_successful_text(
        self,
        mock_session_request: AsyncMock,
        mock_parse_response: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
        default_http_client_config: HttpClientConfig,
    ) -> None:
        """Test a successful request returning plain text, with parsing mocked."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_session_request.return_value.__aenter__.return_value = mock_aio_response

        expected_content = "Hello World"
        expected_processed_headers = ProcessedResponseHeaders(content_type="text/plain")
        expected_raw_headers = CIMultiDictProxy(CIMultiDict[str]({"Content-Type": "text/plain"}))
        mock_parse_response.return_value = (
            expected_content,
            expected_processed_headers,
            expected_raw_headers,
        )

        content, processed_headers, raw_headers = await http_client_instance.request(
            method="GET", endpoint_path="/text", rate_limiter_service=mock_rate_limiter_service
        )

        assert content == expected_content
        assert processed_headers == expected_processed_headers
        assert raw_headers == expected_raw_headers

        full_expected_url = str(default_http_client_config.rest_endpoint).rstrip("/") + "/text"
        mock_parse_response.assert_called_once_with(mock_aio_response, full_expected_url)

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.HttpClient._parse_and_validate_response")
    @patch("aiohttp.ClientSession.request")
    async def test_request_204_no_content(
        self,
        mock_session_request: AsyncMock,
        mock_parse_response: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
        default_http_client_config: HttpClientConfig,
    ) -> None:
        """Test a request that returns 204 No Content, with parsing mocked."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200  # _parse_and_validate_response handles 204 logic
        mock_session_request.return_value.__aenter__.return_value = mock_aio_response

        # _parse_and_validate_response will return (None, ..., ...) if it processes a 204
        # or if the actual response it gets (mock_aio_response) leads to that.
        # Here, we simulate that it was called and it correctly determined a "No Content" outcome.
        expected_processed_headers = ProcessedResponseHeaders(content_type="")
        expected_raw_headers = CIMultiDictProxy(CIMultiDict[str]())
        mock_parse_response.return_value = (None, expected_processed_headers, expected_raw_headers)

        content, processed_headers, raw_headers = await http_client_instance.request(
            method="POST",
            endpoint_path="/empty",
            rate_limiter_service=mock_rate_limiter_service,
            data={},
        )
        assert content is None
        assert processed_headers == expected_processed_headers
        assert raw_headers == expected_raw_headers

        full_expected_url = str(default_http_client_config.rest_endpoint).rstrip("/") + "/empty"
        # Verify _parse_and_validate_response was called with the response from session.request
        mock_parse_response.assert_called_once_with(mock_aio_response, full_expected_url)

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

        session = await http_client_instance._get_session()
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

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/client_err", mock_rate_limiter_service)

        assert excinfo.value.code == APIErrorCode.NETWORK_ISSUE.value
        assert isinstance(excinfo.value.__cause__, aiohttp.ClientConnectorError)
        assert mock_request.call_count == http_client_instance.max_retries + 1
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
            session1 = await client1._get_session()
            mock_response1 = AsyncMock(spec=aiohttp.ClientResponse)
            mock_response1.status = 200
            mock_response1.headers = CIMultiDictProxy(CIMultiDict[str]())
            mock_response1.text = AsyncMock(return_value="")
            mock_request.return_value.__aenter__.return_value = mock_response1

            await client1.request("GET", "/path1", rate_limiter_service=mock_rate_limiter_service)
            mock_request.assert_called_with(
                "GET",
                "http://base.url/v1/path1",
                headers=session1.headers,  # pyright: ignore [reportUnknownMemberType]
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
                headers=session1.headers,  # pyright: ignore [reportUnknownMemberType]
                params=None,
                json=None,
                data=None,
                timeout=aiohttp.ClientTimeout(total=client1.default_request_timeout),
            )
            mock_request.reset_mock()

        # Scenario 3: Absolute URL in endpoint_path
        config2 = HttpClientConfig(rest_endpoint=HttpUrl("http://shouldbeignored.com"))
        async with HttpClient(exchange_name="url_test2", config=config2) as client2:
            session2 = await client2._get_session()
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
                headers=session2.headers,  # pyright: ignore [reportUnknownMemberType]
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
        session_for_headers = await http_client_instance._get_session()
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
    @patch("cyberdelta.apis.connectivity.http_client.HttpClient._parse_and_validate_response")
    @patch("aiohttp.ClientSession.request")
    async def test_request_json_decode_error(
        self,
        mock_session_request: AsyncMock,
        mock_parse_response: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test HttpRequestFailedError if _parse_and_validate_response indicates JSONDecodeError."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200  # Successful HTTP status
        mock_session_request.return_value.__aenter__.return_value = mock_aio_response

        invalid_json_text = "this is not json"
        # Simulate _parse_and_validate_response raising the error
        json_decode_error_cause = json.JSONDecodeError(
            msg="Simulated decode error", doc=invalid_json_text, pos=0
        )
        expected_error = HttpRequestFailedError(
            message="Failed to decode JSON response.",
            http_status_code=200,
            response_body=invalid_json_text,
            api_error_code=APIErrorCode.INVALID_RESPONSE,
        )
        expected_error.__cause__ = json_decode_error_cause  # Manually set cause
        mock_parse_response.side_effect = expected_error

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/invalid_json", mock_rate_limiter_service)

        assert excinfo.value is expected_error  # Check if the exact error is propagated
        assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)  # Check cause type
        full_expected_url = str(http_client_instance.rest_endpoint).rstrip("/") + "/invalid_json"
        mock_parse_response.assert_called_once_with(mock_aio_response, full_expected_url)

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.HttpClient._parse_and_validate_response")
    @patch("aiohttp.ClientSession.request")
    async def test_request_payload_error_on_body_read(
        self,
        mock_session_request: AsyncMock,
        mock_parse_response: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test _parse_and_validate_response raises error for ClientPayloadError."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200  # Successful HTTP status
        mock_aio_response.headers = CIMultiDictProxy(CIMultiDict[str]())
        mock_aio_response.text = AsyncMock(return_value="simulated text from response.text()")
        mock_session_request.return_value.__aenter__.return_value = mock_aio_response

        payload_error_cause = aiohttp.ClientPayloadError("Simulated payload read failure")
        expected_error_from_parser = HttpRequestFailedError(
            message="Failed to read response body during parse/validate.",
            http_status_code=200,
            response_body=None,
            api_error_code=APIErrorCode.NETWORK_ISSUE,
        )
        expected_error_from_parser.__cause__ = payload_error_cause
        mock_parse_response.side_effect = expected_error_from_parser

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request(
                "GET", "/payload_error_path", mock_rate_limiter_service
            )

        # With the `continue` in HttpClient, the originally raised expected_error_from_parser
        # should be the one caught after all retries.
        assert excinfo.value is expected_error_from_parser
        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.NETWORK_ISSUE.value
        assert (
            excinfo.value.exchange_message is None
        )  # From expected_error_from_parser.response_body
        assert excinfo.value.__cause__ is payload_error_cause

        # Check _parse_and_validate_response was called for each attempt
        # Default max_retries is 3 for HttpClientConfig, so 1 initial + 3 retries = 4 calls.
        assert mock_parse_response.call_count == http_client_instance.max_retries + 1
        full_expected_url = (
            str(http_client_instance.rest_endpoint).rstrip("/") + "/payload_error_path"
        )
        mock_parse_response.assert_any_call(mock_aio_response, full_expected_url)

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.HttpClient._parse_and_validate_response")
    @patch("aiohttp.ClientSession.request")
    async def test_request_invalid_content_type_header_from_server(
        self,
        mock_session_request: AsyncMock,
        mock_parse_response: AsyncMock,
        http_client_instance: HttpClient,
        mock_rate_limiter_service: RateLimiterService,
    ) -> None:
        """Test HttpRequestFailedError if _parse_and_validate_response indicates invalid C-Type."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200  # Successful HTTP status
        mock_session_request.return_value.__aenter__.return_value = mock_aio_response

        # Simulate _parse_and_validate_response raising the error due to invalid content type
        validation_error_cause = ValidationError.from_exception_data(
            title="ProcessedResponseHeaders",
            line_errors=[],  # Simplified for test
        )
        expected_error = HttpRequestFailedError(
            message="Invalid Content-Type header from server.",
            http_status_code=200,
            response_body="Invalid Content-Type: some_invalid_header_value",  # Example text
            api_error_code=APIErrorCode.INVALID_RESPONSE,
        )
        expected_error.__cause__ = validation_error_cause  # Manually set cause for the test
        mock_parse_response.side_effect = expected_error

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request(
                "GET", "/invalid_content_type", mock_rate_limiter_service
            )

        assert excinfo.value is expected_error
        # Assert the cause type if validation_error_cause was used implicitly
        assert isinstance(excinfo.value.__cause__, ValidationError)
        full_expected_url = (
            str(http_client_instance.rest_endpoint).rstrip("/") + "/invalid_content_type"
        )
        mock_parse_response.assert_called_once_with(mock_aio_response, full_expected_url)


# New Test Class for _parse_and_validate_response
class TestHttpClientResponseParsing:
    @pytest.mark.asyncio
    async def test_parse_valid_json_response(self, http_client_instance: HttpClient) -> None:
        """Test _parse_and_validate_response with a valid JSON response."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str]([("Content-Type", "application/json; charset=utf-8")])
        )
        mock_aio_response.text = AsyncMock(return_value='{"key": "value", "num": 123}')
        full_url = f"{http_client_instance.rest_endpoint}/test_json"

        (
            content,
            processed_headers,
            raw_headers,
        ) = await http_client_instance._parse_and_validate_response(mock_aio_response, full_url)

        assert content == {"key": "value", "num": 123}
        assert isinstance(processed_headers, ProcessedResponseHeaders)
        assert processed_headers.content_type == "application/json; charset=utf-8"
        assert isinstance(raw_headers, CIMultiDictProxy)
        assert raw_headers.get("Content-Type") == "application/json; charset=utf-8"
        mock_aio_response.text.assert_called_once()

    @pytest.mark.asyncio
    async def test_parse_valid_text_response(self, http_client_instance: HttpClient) -> None:
        """Test _parse_and_validate_response with a valid plain text response."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str]([("Content-Type", "text/plain")])
        )
        mock_aio_response.text = AsyncMock(return_value="Hello, World!")
        full_url = f"{http_client_instance.rest_endpoint}/test_text"

        (
            content,
            processed_headers,
            raw_headers,
        ) = await http_client_instance._parse_and_validate_response(mock_aio_response, full_url)

        assert content == "Hello, World!"
        assert isinstance(processed_headers, ProcessedResponseHeaders)
        assert processed_headers.content_type == "text/plain"
        assert isinstance(raw_headers, CIMultiDictProxy)
        assert raw_headers.get("Content-Type") == "text/plain"
        mock_aio_response.text.assert_called_once()

    @pytest.mark.asyncio
    async def test_parse_204_no_content_response(self, http_client_instance: HttpClient) -> None:
        """Test _parse_and_validate_response with a 204 No Content response."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 204
        # Content-Type might be missing or empty for 204
        mock_aio_response.headers = CIMultiDictProxy(CIMultiDict[str]())
        mock_aio_response.text = AsyncMock(return_value="")  # Should not be called
        full_url = f"{http_client_instance.rest_endpoint}/test_204"

        (
            content,
            processed_headers,
            raw_headers,
        ) = await http_client_instance._parse_and_validate_response(mock_aio_response, full_url)

        assert content is None
        assert isinstance(processed_headers, ProcessedResponseHeaders)
        assert processed_headers.content_type == ""  # Default if not present
        assert isinstance(raw_headers, CIMultiDictProxy)
        mock_aio_response.text.assert_not_called()

    @pytest.mark.asyncio
    async def test_parse_invalid_content_type_too_long(
        self, http_client_instance: HttpClient
    ) -> None:
        """Test _parse_and_validate_response with Content-Type too long."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        invalid_ct = "a" * (MAX_CONTENT_TYPE_LENGTH + 5)
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str]([("Content-Type", invalid_ct)])
        )
        mock_aio_response.text = AsyncMock(return_value='{"key": "value"}')  # Body is fine
        full_url = f"{http_client_instance.rest_endpoint}/test_ct_too_long"

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance._parse_and_validate_response(mock_aio_response, full_url)
        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid Content-Type" in excinfo.value.message
        assert isinstance(excinfo.value.__cause__, ValidationError)

    @pytest.mark.asyncio
    async def test_parse_missing_content_type(self, http_client_instance: HttpClient) -> None:
        """Test _parse_and_validate_response with missing Content-Type (defaults to empty)."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_aio_response.headers = CIMultiDictProxy(CIMultiDict[str]())
        mock_aio_response.text = AsyncMock(return_value="some text")
        full_url = f"{http_client_instance.rest_endpoint}/test_ct_missing"

        content, processed_headers, _ = await http_client_instance._parse_and_validate_response(
            mock_aio_response, full_url
        )
        assert content == "some text"
        assert processed_headers.content_type == ""  # ProcessedResponseHeaders defaults to empty

    @pytest.mark.asyncio
    async def test_parse_json_decode_error(self, http_client_instance: HttpClient) -> None:
        """Test _parse_and_validate_response with a JSONDecodeError."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str]([("Content-Type", "application/json")])
        )
        malformed_json = "not valid json{"
        mock_aio_response.text = AsyncMock(return_value=malformed_json)
        full_url = f"{http_client_instance.rest_endpoint}/test_json_decode_err"

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance._parse_and_validate_response(mock_aio_response, full_url)
        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to decode JSON" in excinfo.value.message
        assert excinfo.value.exchange_message == malformed_json
        assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)

    @pytest.mark.asyncio
    async def test_parse_client_payload_error_on_text_read(
        self, http_client_instance: HttpClient
    ) -> None:
        """Test _parse_and_validate_response with ClientPayloadError on response.text()."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str]([("Content-Type", "application/json")])
        )
        payload_error = aiohttp.ClientPayloadError("Simulated payload read failure")
        mock_aio_response.text = AsyncMock(side_effect=payload_error)
        full_url = f"{http_client_instance.rest_endpoint}/test_payload_err"

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance._parse_and_validate_response(mock_aio_response, full_url)
        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.NETWORK_ISSUE.value
        assert "Failed to read response body" in excinfo.value.message
        assert excinfo.value.exchange_message is None
        assert isinstance(excinfo.value.__cause__, aiohttp.ClientPayloadError)

    @pytest.mark.asyncio
    async def test_parse_json_content_type_but_none_body(
        self, http_client_instance: HttpClient
    ) -> None:
        """Test _parse_and_validate_response with JSON Content-Type but None body (defensive)."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200  # Not 204
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str]([("Content-Type", "application/json")])
        )
        mock_aio_response.text = AsyncMock(return_value=None)
        full_url = f"{http_client_instance.rest_endpoint}/test_json_none_body"

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance._parse_and_validate_response(mock_aio_response, full_url)
        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"JSON content type with empty/None body from {full_url}" in excinfo.value.message

    # Placeholder for tests of ProcessedResponseHeaders itself if not covered elsewhere
    # (though its direct tests are usually in test_connectivity_models.py)


class TestRateLimiterIntegration:
    @pytest.mark.asyncio
    async def test_rate_limiter_integration(self, http_client_instance: HttpClient) -> None:
        """Test that rate limiter integration works correctly."""
        # This test should be implemented to verify that the rate limiter integration
        # is working as expected. You might want to mock the rate limiter service
        # and check if the rate limits are enforced correctly.
        pass
