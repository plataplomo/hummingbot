"""Unit tests for HttpClient.

Tests HTTP client functionality including session management, request handling,
and error scenarios with comprehensive mocking.
"""

import json  # For JSONDecodeError test
from collections.abc import AsyncGenerator, Callable
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import ClientSession as RealAiohttpClientSession
from multidict import CIMultiDict, CIMultiDictProxy
from pydantic import HttpUrl, ValidationError  # Added ValidationError

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.base.infrastructure_config_domain import RequestAuthMode
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.connectivity.connectivity_models import (
    MAX_CONTENT_TYPE_LENGTH,  # For testing invalid content type
    HttpClientConfig,
    ProcessedResponseHeaders,
)
from cyberdelta.apis.connectivity.http_client import (
    HttpClient,
    HttpRequestFailedError,
)


pytestmark = pytest.mark.timing


@pytest.fixture
def default_http_client_config() -> HttpClientConfig:
    """Provide default http client config.

    Returns:
        HttpClientConfig instance for testing.
    """
    return HttpClientConfig(rest_endpoint=HttpUrl("http://test.api"))


@pytest.fixture
def mock_authenticator() -> IAuthenticator:
    """Return mock authenticator for testing."""
    auth = AsyncMock(spec=IAuthenticator)
    auth.prepare_request.return_value = AuthenticatedRequestComponents(
        headers={"X-Auth": "dummy_sig"},
        params={"auth_param": "val"},
        data=None,
    )
    return auth


@pytest_asyncio.fixture
async def http_client_instance(
    default_http_client_config: HttpClientConfig,
) -> AsyncGenerator[HttpClient]:
    """Provide HttpClient instance for testing.

    Yields:
        HttpClient: HTTP client instance for testing.
    """
    client = HttpClient(exchange_name="test_exchange", config=default_http_client_config)
    yield client
    await client.close_session()


@pytest.fixture
def mock_aiohttp_response_factory() -> Callable[..., AsyncMock]:
    """Create AsyncMock(spec=aiohttp.ClientResponse) instances.

    Returns:
        Factory function for creating mock aiohttp ClientResponse instances.
    """

    def _factory(
        status_code: int = 200,
        headers_dict: dict[str, str] | None = None,
        body_text: str | None = None,
        json_body: dict[str, Any] | list[Any] | None = None,
        side_effect_for_text: Exception | None = None,
        reason: str | None = None,  # Added reason for status line
    ) -> AsyncMock:
        response_mock = AsyncMock(spec=aiohttp.ClientResponse)
        response_mock.status = status_code
        response_mock.reason = reason  # Standard attribute for HTTP status reason
        response_mock.headers = CIMultiDictProxy(CIMultiDict[str](headers_dict or {}))

        if side_effect_for_text:
            response_mock.text = AsyncMock(side_effect=side_effect_for_text)
        elif json_body is not None:
            response_mock.text = AsyncMock(return_value=json.dumps(json_body))
        else:
            response_mock.text = AsyncMock(return_value=body_text or "")
        return response_mock

    return _factory


class TestHttpClient:
    """Test suite for HttpClient functionality."""

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_internal_session_creation_reuse_and_closure(
        self,
        MockAiohttpSession: MagicMock,  # Patched class constructor
        http_client_instance: HttpClient,  # Uses internal session by default
    ) -> None:
        """Test internal session is created on first request, reused, and closed correctly.

        Relies on public HttpClient.request() and HttpClient.close_session().
        """
        # --- First request: Session Creation ---
        mock_session_instance1 = AsyncMock(spec=RealAiohttpClientSession)
        mock_session_instance1.closed = False
        mock_response1 = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response1.status = 200
        mock_response1.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json"}),
        )
        mock_response1.text = AsyncMock(return_value="{}")
        mock_session_instance1.request.return_value.__aenter__.return_value = mock_response1
        MockAiohttpSession.return_value = mock_session_instance1

        await http_client_instance.request("GET", "/test1")

        MockAiohttpSession.assert_called_once()  # Constructor called
        # Check headers passed to ClientSession constructor
        _constructor_args, constructor_kwargs = MockAiohttpSession.call_args  # _ to denote unused
        assert constructor_kwargs["headers"] == {
            "User-Agent": f"CyberDeltaEngine/{http_client_instance.exchange_name}",
        }
        mock_session_instance1.request.assert_called_once()  # Session's request method called

        # --- Second request: Session Reuse ---
        await http_client_instance.request("GET", "/test2")
        MockAiohttpSession.assert_called_once()  # Constructor NOT called again
        assert mock_session_instance1.request.call_count == 2  # Session's request called again

        # --- Close session ---
        await http_client_instance.close_session()
        mock_session_instance1.close.assert_called_once()  # Session's close method called

        # --- Third request: New Session Creation after closure ---
        mock_session_instance2 = AsyncMock(spec=RealAiohttpClientSession)
        mock_session_instance2.closed = False
        mock_response2 = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response2.status = 200
        mock_response2.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json"}),
        )
        mock_response2.text = AsyncMock(return_value="{}")
        mock_session_instance2.request.return_value.__aenter__.return_value = mock_response2
        MockAiohttpSession.return_value = mock_session_instance2  # Point constructor to new mock

        await http_client_instance.request("GET", "/test3")

        assert MockAiohttpSession.call_count == 2  # Constructor called again for new session
        mock_session_instance2.request.assert_called_once()
        assert (
            mock_session_instance1.request.call_count == 2
        )  # Old session's request count unchanged

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_close_session_idempotent_internal_session(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,  # Uses internal session
    ) -> None:
        """Test close_session() is idempotent for internally managed sessions."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        mock_session_instance.closed = False
        mock_response = AsyncMock(spec=aiohttp.ClientResponse, status=200)
        mock_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json"}),
        )
        mock_response.text = AsyncMock(return_value="{}")
        mock_session_instance.request.return_value.__aenter__.return_value = mock_response
        MockAiohttpSession.return_value = mock_session_instance

        # Make a request to ensure session is created
        await http_client_instance.request("GET", "/test_idempotent")
        MockAiohttpSession.assert_called_once()
        mock_session_instance.request.assert_called_once()

        await http_client_instance.close_session()
        mock_session_instance.close.assert_called_once()

        # Call close_session again
        await http_client_instance.close_session()
        mock_session_instance.close.assert_called_once()  # Should still be 1, not called again

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_async_context_manager_internal_session(
        self,
        MockAiohttpSession: MagicMock,
        default_http_client_config: HttpClientConfig,  # For new HttpClient instance
    ) -> None:
        """Test async context manager properly creates and closes internal session."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        mock_session_instance.closed = False
        mock_response = AsyncMock(spec=aiohttp.ClientResponse, status=200)
        mock_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json"}),
        )
        mock_response.text = AsyncMock(return_value="{}")
        mock_session_instance.request.return_value.__aenter__.return_value = mock_response
        MockAiohttpSession.return_value = mock_session_instance

        expected_headers = {
            "User-Agent": "CyberDeltaEngine/test_ctx",
        }

        async with HttpClient(
            exchange_name="test_ctx",
            config=default_http_client_config,
        ) as _:  # Changed 'client' to '_' as it's unused
            # Verify the ClientSession was called with both headers and connector
            MockAiohttpSession.assert_called_once()
            call_args = MockAiohttpSession.call_args
            assert call_args.kwargs["headers"] == expected_headers
            assert "connector" in call_args.kwargs
            assert hasattr(call_args.kwargs["connector"], "limit")  # It's a TCPConnector

        # Verify the session instance was closed on __aexit__
        mock_session_instance.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_external_session_is_used_and_not_closed(
        self,
        default_http_client_config: HttpClientConfig,
    ) -> None:
        """Test HttpClient uses a provided external session and doesn't close it."""
        external_session_mock = AsyncMock(spec=aiohttp.ClientSession)
        external_session_mock.closed = False
        mock_response = AsyncMock(spec=aiohttp.ClientResponse, status=200)
        mock_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json"}),
        )
        mock_response.text = AsyncMock(return_value="{}")
        external_session_mock.request.return_value.__aenter__.return_value = mock_response

        # Patching aiohttp.ClientSession to ensure it's NOT called when an external one is provided
        with patch(
            "cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession",
        ) as MockAiohttpSessionClsConstruction:
            client_with_external_session = HttpClient(
                exchange_name="test_external",
                config=default_http_client_config,
                session=external_session_mock,
            )

            await client_with_external_session.request("GET", "/test_ext")

            MockAiohttpSessionClsConstruction.assert_not_called()  # Internal session ctor !called
            external_session_mock.request.assert_called_once()

            await client_with_external_session.close_session()
            external_session_mock.close.assert_not_called()  # Ext session !closed by HTTPClient

        # Verify context manager also doesn't close external session
        with patch(
            "cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession",
        ) as MockAiohttpSessionClsConstructionCtx:
            async with HttpClient(
                exchange_name="test_external_ctx",
                config=default_http_client_config,
                session=external_session_mock,
            ) as client_ctx:
                await client_ctx.request("GET", "/test_ext_ctx")
                MockAiohttpSessionClsConstructionCtx.assert_not_called()
                assert external_session_mock.request.call_count == 2

            external_session_mock.close.assert_not_called()  # Still not closed

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_successful_json(
        self,
        mock_session_request_method: AsyncMock,
        http_client_instance: HttpClient,
        default_http_client_config: HttpClientConfig,
    ) -> None:
        """Test a successful request returning JSON."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json; charset=utf-8"}),
        )
        expected_body_dict = {"data": "success"}
        mock_aio_response.text = AsyncMock(return_value=json.dumps(expected_body_dict))
        mock_session_request_method.return_value.__aenter__.return_value = mock_aio_response

        content, status_code, processed_headers, raw_headers = await http_client_instance.request(
            method="GET",
            endpoint_path="/test",
        )

        assert content == expected_body_dict
        assert status_code == 200
        assert processed_headers.content_type == "application/json; charset=utf-8"
        assert raw_headers.get("Content-Type") == "application/json; charset=utf-8"

        # Rate limiting is no longer handled in the HttpClient request method

        # E501: Break long assignment
        full_expected_url = str(default_http_client_config.rest_endpoint).rstrip("/") + "/test"
        # We can't easily get the session_for_headers without private access.
        # Instead, we trust that _get_session() was called internally and prepared headers.
        # The key check is that mock_session_request_method (the one on the session instance)
        # was called correctly.
        # To do this robustly, we need to patch aiohttp.ClientSession constructor
        # to control the instance whose request method is mock_session_request_method.

        # This test needs further refinement if we want to assert headers passed to session.request
        # without private access. For now, focuses on returned content.
        args, _kwargs = mock_session_request_method.call_args  # _ to denote unused
        assert args[0] == "GET"
        assert args[1] == full_expected_url
        # assert _kwargs["headers"] contains the User-Agent, etc. This requires more setup.

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_successful_text(
        self,
        mock_session_request_method: AsyncMock,
        http_client_instance: HttpClient,
        default_http_client_config: HttpClientConfig,
    ) -> None:
        """Test a successful request returning plain text."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "text/plain"}),
        )
        expected_text_content = "Hello World"
        mock_aio_response.text = AsyncMock(return_value=expected_text_content)
        mock_session_request_method.return_value.__aenter__.return_value = mock_aio_response

        content, status_code, processed_headers, raw_headers = await http_client_instance.request(
            method="GET",
            endpoint_path="/text",
        )

        assert content == expected_text_content
        assert status_code == 200
        assert processed_headers.content_type == "text/plain"
        assert raw_headers.get("Content-Type") == "text/plain"
        # Further assertions on mock_session_request_method call similar to above test

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_204_no_content(
        self,
        mock_session_request_method: AsyncMock,
        http_client_instance: HttpClient,
        default_http_client_config: HttpClientConfig,
    ) -> None:
        """Test a request that returns 204 No Content."""
        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 204
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str](),
        )  # No content-type typically
        mock_aio_response.text = AsyncMock(
            return_value="",
        )  # Should not be called by parser for 204
        mock_session_request_method.return_value.__aenter__.return_value = mock_aio_response

        content, status_code, processed_headers, raw_headers = await http_client_instance.request(
            method="POST",
            endpoint_path="/empty",
            data={},
        )
        assert content is None
        assert status_code == 204
        assert not processed_headers.content_type  # Defaults to empty if not present
        assert raw_headers == CIMultiDictProxy(CIMultiDict[str]())
        mock_aio_response.text.assert_not_called()  # Key check for 204 handling

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.request")
    async def test_request_signed(
        self,
        mock_session_request_method: AsyncMock,
        http_client_instance: HttpClient,
        mock_authenticator: IAuthenticator,
        default_http_client_config: HttpClientConfig,
    ) -> None:
        """Test that request sends a signed request when an authenticator is provided."""
        # This test expects http_client_instance to use its internally managed session.
        # The session's 'request' method is mocked by mock_session_request_method.

        # Ensure the http_client_instance has an internal session created
        # (it's usually created on first request, or by __aenter__ if used as ctx mngr)
        # For this test, we assume it will create one when its .request() is called.

        # Setup the mock authenticator to return specific components
        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.status = 200
        mock_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json"}),
        )
        mock_response.text = AsyncMock(return_value='{"status": "ok"}')
        mock_session_request_method.return_value.__aenter__.return_value = mock_response

        original_headers = {"X-Client-Header": "client_val"}
        original_params = {"client_param": "val"}
        original_data = {"client_data": "val"}

        # Patch aiohttp.ClientSession to get hold of the session instance
        # and its headers, so we can verify the authenticator's behavior.
        with patch(
            "cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession",
        ) as MockAiohttpSessionCls:
            mock_created_session_instance = AsyncMock(spec=RealAiohttpClientSession)
            mock_created_session_instance.closed = False
            mock_created_session_instance.request = mock_session_request_method
            initial_session_headers = {
                "User-Agent": f"CyberDeltaEngine/{http_client_instance.exchange_name}",
            }
            mock_created_session_instance.headers = initial_session_headers.copy()
            MockAiohttpSessionCls.return_value = mock_created_session_instance

            _, status_code, processed_headers, raw_headers = await http_client_instance.request(
                method="POST",
                endpoint_path="/signed_action",
                authenticator=mock_authenticator,
                params=original_params.copy(),
                data=original_data.copy(),
                headers=original_headers.copy(),
                auth_mode=RequestAuthMode.SIGNED,
            )
            assert status_code == 200
        assert isinstance(processed_headers, ProcessedResponseHeaders)
        assert processed_headers.content_type == "application/json"
        assert isinstance(raw_headers, CIMultiDictProxy)

        # Construct the headers as HttpClient would pass to authenticator.prepare_request
        # These are the session's initial headers merged with request-specific headers.
        expected_headers_for_auth_prep = initial_session_headers.copy()
        expected_headers_for_auth_prep.update(original_headers)

        # JUSTIFICATION: mock_authenticator.prepare_request is an attribute from an
        # AsyncMock(spec=IAuthenticator). At runtime, unittest.mock ensures that
        # accessing an attribute that is an async method on the spec (like prepare_request)
        # results in a new AsyncMock instance.
        # The type checker, relying on the IAuthenticator spec, only sees the original
        # method signature. To access mock-specific attributes like assert_called_once_with,
        # we must cast to inform the type checker of its true runtime nature as an AsyncMock.
        # This cast is safe because the object *is* an AsyncMock here.
        prepare_request_mock_signed = cast("AsyncMock", mock_authenticator.prepare_request)
        assert isinstance(prepare_request_mock_signed, AsyncMock)
        # Verify the authenticator was called with the original parameters
        prepare_request_mock_signed.assert_called_once_with(
            method="POST",
            path="/signed_action",
            params=original_params,
            data=original_data,
            headers=expected_headers_for_auth_prep,
        )

        # Assertions on what was *actually* sent by aiohttp
        _final_call_args, final_call_kwargs = (
            mock_session_request_method.call_args
        )  # _final_call_args unused
        final_sent_headers = final_call_kwargs["headers"]

        assert final_sent_headers["X-Auth"] == "dummy_sig"
        assert final_sent_headers["X-Client-Header"] == "client_val"
        # User-Agent should also be there from the session
        assert final_sent_headers["User-Agent"] == initial_session_headers["User-Agent"]

        final_sent_params = final_call_kwargs["params"]
        assert final_sent_params["auth_param"] == "val"
        # Check if original_params were augmented or replaced based on authenticator mock behavior
        auth_result_params = prepare_request_mock_signed.return_value.params
        if auth_result_params is not original_params:
            assert "client_param" not in final_sent_params  # Assuming authenticator replaces params
        else:
            assert (
                final_sent_params["client_param"] == "val"
            )  # Assuming authenticator augments params

        final_call_json_data = final_call_kwargs.get("json")
        # HttpClient always uses auth_components.data when authenticator is present
        # even if it's None - the authenticator has full control over the data
        auth_result_data = prepare_request_mock_signed.return_value.data
        assert final_call_json_data == auth_result_data

    @pytest.mark.asyncio
    async def test_request_signed_no_authenticator_raises_api_error(
        self,
        http_client_instance: HttpClient,
    ) -> None:
        """Test signed request raises APIError if no authenticator is provided."""
        with pytest.raises(APIError) as excinfo:
            await http_client_instance.request(
                method="POST",
                endpoint_path="/needs_auth",
                auth_mode=RequestAuthMode.SIGNED,
            )
        assert excinfo.value.code == APIErrorCode.AUTHENTICATION_FAILED.value

    @pytest.mark.asyncio
    async def test_authenticator_prepare_request_raises_api_error(
        self,
        http_client_instance: HttpClient,
        mock_authenticator: IAuthenticator,
    ) -> None:
        """Test that if authenticator.prepare_request fails, the APIError is propagated."""
        auth_error = APIError("Auth Prep Failed", code=APIErrorCode.AUTHENTICATION_FAILED.value)

        # JUSTIFICATION: Similar to prepare_request_mock_signed, mock_authenticator.prepare_request
        # is an AsyncMock at runtime. The type checker sees the original IAuthenticator signature.
        # We need to cast to AsyncMock to assign to its .side_effect attribute.
        # This cast is safe because the object *is* an AsyncMock here.
        prepare_request_mock_error = cast("AsyncMock", mock_authenticator.prepare_request)
        assert isinstance(prepare_request_mock_error, AsyncMock)
        prepare_request_mock_error.side_effect = auth_error

        with pytest.raises(APIError) as excinfo:
            await http_client_instance.request(
                method="POST",
                endpoint_path="/auth_fail",
                authenticator=mock_authenticator,
                auth_mode=RequestAuthMode.SIGNED,
            )
        assert excinfo.value is auth_error  # Check it's the same exception instance

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_http_error_400_raises_HttpRequestFailedError_no_retry(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
    ) -> None:
        """Test that a 400 error raises HttpRequestFailedError immediately without retry."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 400
        mock_aio_response.headers = CIMultiDictProxy(CIMultiDict[str]())  # Empty headers
        error_body = '{"error": "Bad Request"}'
        mock_aio_response.text = AsyncMock(return_value=error_body)
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        http_client_instance.max_retries = 3  # Ensure retries are configured

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/bad_req")

        assert excinfo.value.http_status == 400
        assert excinfo.value.exchange_message == error_body
        # For a 400 error, no retries should occur, so session.request is called once.
        mock_session_instance.request.assert_called_once()

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)  # Patch sleep for retry tests
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_http_error_500_retries_then_raises(
        self,
        MockAiohttpSession: MagicMock,
        mock_sleep: AsyncMock,
        http_client_instance: HttpClient,
    ) -> None:
        """Test that a 500 error is retried and then raises HttpRequestFailedError."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 500
        mock_aio_response.headers = CIMultiDictProxy(CIMultiDict[str]())  # Empty headers
        error_body = '{"error": "Server Error"}'
        mock_aio_response.text = AsyncMock(return_value=error_body)
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        http_client_instance.max_retries = 2
        http_client_instance.retry_delay_seconds = 0.01

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/server_err")

        assert excinfo.value.http_status == 500
        assert excinfo.value.exchange_message == error_body
        # Called once initially, then for each of the 2 retries = 3 times
        assert mock_session_instance.request.call_count == http_client_instance.max_retries + 1
        assert mock_sleep.call_count == http_client_instance.max_retries
        mock_sleep.assert_any_call(0.01 * (2**0))
        mock_sleep.assert_any_call(0.01 * (2**1))

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)  # Patch sleep for retry tests
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_client_error_retries_then_raises(
        self,
        MockAiohttpSession: MagicMock,
        mock_sleep: AsyncMock,
        http_client_instance: HttpClient,
    ) -> None:
        """Test aiohttp.ClientError is retried and then HttpRequestFailedError is raised."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        client_error = aiohttp.ClientConnectorError(MagicMock(), OSError("Connection failed"))
        # Make the session's request method raise the ClientError
        mock_session_instance.request.side_effect = client_error

        http_client_instance.max_retries = 1
        http_client_instance.retry_delay_seconds = 0.01

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/client_err")

        assert excinfo.value.code == APIErrorCode.NETWORK_ISSUE.value
        assert isinstance(excinfo.value.__cause__, aiohttp.ClientConnectorError)
        # Called once initially, then for the 1 retry = 2 times
        assert mock_session_instance.request.call_count == http_client_instance.max_retries + 1
        assert mock_sleep.call_count == http_client_instance.max_retries
        mock_sleep.assert_called_with(0.01 * (2**0))

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_url_construction(
        self,
        MockAiohttpSession: MagicMock,
        # http_client_instance is not used directly to allow re-init with different configs
    ) -> None:
        """Test that URLs are constructed correctly through public request method."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_aio_response.headers = CIMultiDictProxy(CIMultiDict[str]({}))  # Minimal headers
        mock_aio_response.text = AsyncMock(return_value="ok")  # Minimal body
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        # Scenario 1: Relative path with base URL not ending in slash
        config1 = HttpClientConfig(rest_endpoint=HttpUrl("http://base.url/v1"))
        async with HttpClient(exchange_name="url_test1", config=config1) as client1:
            await client1.request("GET", "/path1")
            args, _kwargs = mock_session_instance.request.call_args  # _kwargs unused
            assert args[0] == "GET"
            assert args[1] == "http://base.url/v1/path1"
            mock_session_instance.request.reset_mock()  # Reset for next call within same client

            # Scenario 2: Relative path (no leading slash) with base URL not ending in slash
            # HttpClient's logic should ensure the slash is correctly handled by urljoin
            await client1.request("GET", "path1_no_lead_slash")
            args, _kwargs = mock_session_instance.request.call_args  # _kwargs unused
            assert args[0] == "GET"
            assert args[1] == "http://base.url/v1/path1_no_lead_slash"
            mock_session_instance.request.reset_mock()

        # Reset class-level mock for new HttpClient instance
        MockAiohttpSession.reset_mock()
        mock_session_instance.reset_mock()  # Also reset the instance methods if needed
        MockAiohttpSession.return_value = (
            mock_session_instance  # Reassign, as reset_mock might clear it
        )

        # Scenario 3: Absolute URL in endpoint_path, base URL should be ignored
        config2 = HttpClientConfig(rest_endpoint=HttpUrl("http://shouldbeignored.com"))
        async with HttpClient(exchange_name="url_test2", config=config2) as client2:
            await client2.request(
                "GET",
                "https://specific.api.com/specific/path",
            )
            args, _kwargs = mock_session_instance.request.call_args  # _kwargs unused
            assert args[0] == "GET"
            assert args[1] == "https://specific.api.com/specific/path"

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_request_custom_timeout(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
        default_http_client_config: HttpClientConfig,  # To check against default
    ) -> None:
        """Test that a custom request_timeout is used when calling request()."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_aio_response.headers = CIMultiDictProxy(CIMultiDict[str]({}))
        mock_aio_response.text = AsyncMock(return_value="ok")
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        custom_timeout = 15.5
        assert (
            custom_timeout != http_client_instance.default_request_timeout
        )  # Ensure it's different

        await http_client_instance.request(
            method="GET",
            endpoint_path="/custom_timeout_test",
            request_timeout=custom_timeout,
        )

        args, kwargs = mock_session_instance.request.call_args
        assert args[0] == "GET"
        assert (
            args[1]
            == str(default_http_client_config.rest_endpoint).rstrip("/") + "/custom_timeout_test"
        )
        assert isinstance(kwargs["timeout"], aiohttp.ClientTimeout)
        assert kwargs["timeout"].total == custom_timeout

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_request_json_decode_error(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
    ) -> None:
        """Test HttpRequestFailedError for JSONDecodeError during parsing."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json"}),
        )
        invalid_json_text = "this is not json{"
        mock_aio_response.text = AsyncMock(return_value=invalid_json_text)
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/invalid_json")

        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to decode JSON" in excinfo.value.message
        assert excinfo.value.exchange_message == invalid_json_text
        assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)
        mock_session_instance.request.assert_called_once()

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_request_payload_error_on_body_read(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
    ) -> None:
        """Test HttpRequestFailedError for ClientPayloadError during response.text()."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": "application/json"}),
        )
        payload_error_cause = aiohttp.ClientPayloadError("Simulated payload read failure")
        mock_aio_response.text = AsyncMock(side_effect=payload_error_cause)
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        http_client_instance.max_retries = 0

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/payload_error_path")

        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.NETWORK_ISSUE.value
        assert "Failed to read response body" in excinfo.value.message
        assert excinfo.value.exchange_message is None
        assert excinfo.value.__cause__ is payload_error_cause
        mock_session_instance.request.assert_called_once()

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_request_invalid_content_type_header_from_server(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
    ) -> None:
        """Test HttpRequestFailedError for invalid Content-Type header from server."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        mock_aio_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_aio_response.status = 200
        invalid_ct_header = "a" * (MAX_CONTENT_TYPE_LENGTH + 5)
        mock_aio_response.headers = CIMultiDictProxy(
            CIMultiDict[str]({"Content-Type": invalid_ct_header}),
        )
        mock_aio_response.text = AsyncMock(return_value='{"key": "value"}')
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/invalid_content_type")

        assert excinfo.value.http_status == 0
        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid Content-Type" in excinfo.value.message
        assert excinfo.value.exchange_message == f"Invalid Content-Type: {invalid_ct_header}"
        assert isinstance(excinfo.value.__cause__, ValidationError)
        mock_session_instance.request.assert_called_once()


# New Test Class for _parse_and_validate_response logic, tested via public request()
class TestHttpClientRequestResponseParsing:
    """Test suite for HttpClient request and response parsing logic."""

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_parse_valid_json_response(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
        mock_aiohttp_response_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test request() with a valid JSON response."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        expected_data = {"key": "value", "num": 123}
        headers = {"Content-Type": "application/json; charset=utf-8"}
        mock_aio_response = mock_aiohttp_response_factory(
            status_code=200,
            json_body=expected_data,
            headers_dict=headers,
        )
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        content, status_code, processed_headers, raw_headers = await http_client_instance.request(
            "GET",
            "/test_json",
        )
        assert status_code == 200

        assert content == expected_data
        assert processed_headers.content_type == "application/json; charset=utf-8"
        assert raw_headers.get("Content-Type") == "application/json; charset=utf-8"
        mock_session_instance.request.assert_called_once()

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_parse_valid_text_response(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
        mock_aiohttp_response_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test request() with a valid plain text response."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        expected_text = "Hello, World!"
        headers = {"Content-Type": "text/plain"}
        mock_aio_response = mock_aiohttp_response_factory(
            status_code=200,
            body_text=expected_text,
            headers_dict=headers,
        )
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        content, status_code, processed_headers, raw_headers = await http_client_instance.request(
            "GET",
            "/test_text",
        )
        assert status_code == 200

        assert content == expected_text
        assert processed_headers.content_type == "text/plain"
        assert raw_headers.get("Content-Type") == "text/plain"
        mock_session_instance.request.assert_called_once()

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_parse_204_no_content_response(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
        mock_aiohttp_response_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test request() with a 204 No Content response."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        mock_aio_response = mock_aiohttp_response_factory(
            status_code=204,
            headers_dict={},
        )  # Empty headers for 204
        # For 204, text() should ideally not be called by the client code,
        # but if it were, factory provides "" by default for body_text=None.
        # The key is that HttpClient._parse_and_validate_response should not call .text() for 204.
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        content, status_code, processed_headers, raw_headers = await http_client_instance.request(
            "POST",
            "/test_204",
            data={},
        )
        assert status_code == 204

        assert content is None
        assert not processed_headers.content_type  # Default if not present
        assert isinstance(raw_headers, CIMultiDictProxy)
        assert not raw_headers  # Empty
        # To assert that response.text() was not called by HttpClient._parse_and_validate_response,
        # we need to access the .text mock on mock_aio_response created by the factory.
        mock_aio_response.text.assert_not_called()
        mock_session_instance.request.assert_called_once()

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_parse_invalid_content_type_too_long(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
        mock_aiohttp_response_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test request() with Content-Type too long."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        invalid_ct = "a" * (MAX_CONTENT_TYPE_LENGTH + 5)
        headers = {"Content-Type": invalid_ct}
        mock_aio_response = mock_aiohttp_response_factory(
            status_code=200,
            headers_dict=headers,
            body_text='{"key": "value"}',  # Body itself is fine
        )
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/test_ct_long")

        assert excinfo.value.http_status == 0
        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid Content-Type" in excinfo.value.message
        assert isinstance(excinfo.value.__cause__, ValidationError)
        mock_session_instance.request.assert_called_once()

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_parse_missing_content_type(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
        mock_aiohttp_response_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test request() with missing Content-Type (defaults to empty string)."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        expected_text = "some text"
        mock_aio_response = mock_aiohttp_response_factory(
            status_code=200,
            headers_dict={},  # No Content-Type header
            body_text=expected_text,
        )
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        content, status_code, processed_headers, _ = await http_client_instance.request(
            "GET",
            "/test_ct_missing",
        )
        assert status_code == 200
        assert content == expected_text  # Should be treated as text
        assert not processed_headers.content_type  # ProcessedResponseHeaders defaults to empty
        mock_session_instance.request.assert_called_once()

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_parse_json_decode_error(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
        mock_aiohttp_response_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test request() raising HttpRequestFailedError with a JSONDecodeError cause."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        malformed_json = "not valid json{"
        headers = {"Content-Type": "application/json"}
        mock_aio_response = mock_aiohttp_response_factory(
            status_code=200,
            headers_dict=headers,
            body_text=malformed_json,
        )
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/test_json_err")

        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Failed to decode JSON" in excinfo.value.message
        assert excinfo.value.exchange_message == malformed_json
        assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)
        mock_session_instance.request.assert_called_once()

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_parse_client_payload_error_on_text_read(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
        mock_aiohttp_response_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test request() with ClientPayloadError on response.text()."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        payload_error = aiohttp.ClientPayloadError("Simulated payload read failure")
        # Content-Type doesn't matter here as .text() will fail first
        mock_aio_response = mock_aiohttp_response_factory(
            status_code=200,
            headers_dict={"Content-Type": "application/json"},
            side_effect_for_text=payload_error,
        )
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        http_client_instance.max_retries = 0  # Test with no retries for direct error
        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/test_payload_err")

        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.NETWORK_ISSUE.value
        assert "Failed to read response body" in excinfo.value.message
        assert excinfo.value.exchange_message is None
        assert isinstance(excinfo.value.__cause__, aiohttp.ClientPayloadError)
        mock_session_instance.request.assert_called_once()

    @pytest.mark.asyncio
    @patch("cyberdelta.apis.connectivity.http_client.aiohttp.ClientSession")
    async def test_parse_json_content_type_but_none_body(
        self,
        MockAiohttpSession: MagicMock,
        http_client_instance: HttpClient,
        mock_aiohttp_response_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test request() with JSON Content-Type but None/empty body (not 204)."""
        mock_session_instance = AsyncMock(spec=RealAiohttpClientSession)
        MockAiohttpSession.return_value = mock_session_instance

        headers = {"Content-Type": "application/json"}
        mock_aio_response = mock_aiohttp_response_factory(
            status_code=200,  # Not 204
            headers_dict=headers,
            body_text="",  # Simulate empty body string
        )
        mock_session_instance.request.return_value.__aenter__.return_value = mock_aio_response

        with pytest.raises(HttpRequestFailedError) as excinfo:
            await http_client_instance.request("GET", "/test_json_empty_body")

        assert excinfo.value.http_status == 200
        assert excinfo.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "JSON content type with empty/None body" in excinfo.value.message
        mock_session_instance.request.assert_called_once()

    # Placeholder for tests of ProcessedResponseHeaders itself if not covered elsewhere
    # (though its direct tests are usually in test_connectivity_models.py)


class TestRateLimiterIntegration:
    """Test suite for rate limiter integration with HttpClient."""

    @pytest.mark.asyncio
    async def test_rate_limiter_integration(self, http_client_instance: HttpClient) -> None:
        """Test that rate limiter integration works correctly."""
        # This test would need a more involved setup to truly test rate limiting behavior,
        # e.g. by patching asyncio.sleep within the rate limiter or by using a real
        # rate limiter with a very small token bucket and fast refill to observe delays.
        # Rate limiting integration is tested elsewhere in the API clients that use HttpClient.
