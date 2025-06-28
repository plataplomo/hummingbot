"""HTTP Client for Exchange API Communication.

This module provides a robust HTTP client implementation for communicating with
exchange APIs, including rate limiting, retry logic, and error handling.
"""

from __future__ import annotations

import asyncio
import json
import urllib.parse
from http import HTTPStatus
from types import TracebackType
from typing import Any

import aiohttp
from multidict import CIMultiDictProxy
from pydantic import ValidationError

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.common import APIError, APIErrorCode

# Import the model and constants from the new location
from cyberdelta.apis.connectivity.connectivity_models import (
    MAX_CONTENT_TYPE_LENGTH,
    HttpClientConfig,
    ProcessedResponseHeaders,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)


class HttpRequestFailedError(APIError):
    """Custom exception for HTTP request failures within HttpClient."""

    def __init__(
        self,
        message: str,
        http_status_code: int,
        response_body: str | None = None,
        api_error_code: APIErrorCode = APIErrorCode.NETWORK_ISSUE,
        metadata: dict[str, Any] | None = None,
        original_exception: Exception | None = None,
    ) -> None:
        """Initialize HttpRequestFailedError with HTTP-specific details.

        Args:
            message: Descriptive error message
            http_status_code: HTTP status code from the failed request
            response_body: Optional response body content
            api_error_code: API error classification code
            metadata: Optional additional error metadata
            original_exception: Original exception that caused this error
        """
        super().__init__(
            message=message,
            code=api_error_code.value,
            exchange_message=response_body,
            http_status=http_status_code,
            metadata=metadata,
            original_exception=original_exception,
        )

    def __str__(self) -> str:
        """Return a human-readable string representation of the HTTP error."""
        status_str = (
            f"HTTP {self.http_status}" if self.http_status is not None else "HTTP UnknownStatus"
        )
        body_preview = self.exchange_message or "N/A"
        body_str = str(body_preview)
        return f"HttpRequestFailedError ({status_str}): {self.message}. Body: {body_str[:50]}..."


class HttpClient:
    """Generic HTTP client for making requests to exchange APIs.

    Handles session management, request signing, and retries.
    """

    _session: aiohttp.ClientSession | None  # Explicit type hint for instance variable
    _external_session: bool  # Explicit type hint for instance variable

    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY_SECONDS = 5.0  # Default base delay for retries

    def __init__(
        self,
        exchange_name: str,
        config: HttpClientConfig,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        """Initializes the HttpClient.

        Args:
            exchange_name: Name of the exchange (for logging).
            config: HttpClientConfig object with all necessary parameters.
            session: Optional pre-existing aiohttp.ClientSession.

        """
        self.exchange_name = exchange_name
        self.rest_endpoint = str(config.rest_endpoint).rstrip("/")
        self.default_request_timeout = config.default_request_timeout
        self.max_retries = (
            config.max_retries if config.max_retries is not None else self.DEFAULT_MAX_RETRIES
        )
        self.retry_delay_seconds = (
            config.retry_delay_seconds
            if config.retry_delay_seconds is not None
            else self.DEFAULT_RETRY_DELAY_SECONDS
        )

        self._session_lock = asyncio.Lock()
        if session:
            self._session = session
            self._external_session = True
            logger.info(
                "http_client_initialized_with_external_session",
                action="init",
                exchange=self.exchange_name,
                message=f"[{self.exchange_name}] HttpClient initialized with external session.",
            )
        else:
            self._session = None
            self._external_session = False
            logger.info(
                "http_client_initialized",
                exchange_name=self.exchange_name,
                rest_endpoint=self.rest_endpoint,
                message="HttpClient initialized for endpoint",
            )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Provides an active aiohttp.ClientSession.

        Uses an externally provided session if available and valid,
        otherwise creates and manages one internally.
        """
        async with self._session_lock:
            if self._external_session and self._session and not self._session.closed:
                logger.debug(
                    "using_external_aiohttp_session",
                    action="get_session",
                    exchange=self.exchange_name,
                    message=f"[{self.exchange_name}] Using external aiohttp ClientSession.",
                )
                return self._session

            # If external session is not usable, or we are managing internally
            if self._session is None or self._session.closed:
                logger.info(
                    "http_client_creating_session",
                    exchange_name=self.exchange_name,
                    external_session=self._external_session,
                    message=(
                        f"[{self.exchange_name}] Creating new internal aiohttp ClientSession "
                        f"(external_session={self._external_session})."
                    ),
                )
                # Create optimized connector for better connection pooling
                connector = aiohttp.TCPConnector(
                    limit=100,  # Total connection pool size
                    limit_per_host=30,  # Connections per host
                    ttl_dns_cache=300,  # DNS cache timeout in seconds
                    keepalive_timeout=30,  # Keep connections alive for 30s
                    force_close=False,  # Reuse connections
                )
                self._session = aiohttp.ClientSession(
                    headers={"User-Agent": f"CyberDeltaEngine/{self.exchange_name}"},
                    connector=connector,
                )
                self._external_session = False  # Now internally managed
            else:
                logger.debug(
                    "internal_session_already_closed_or_none",
                    action="get_session",
                    exchange=self.exchange_name,
                    message=f"[{self.exchange_name}] Internal ClientSession already closed/None.",
                )
            return self._session

    async def close_session(self) -> None:
        """Closes the aiohttp.ClientSession if it's an internally managed one and is open."""
        async with self._session_lock:
            if not self._external_session and self._session and not self._session.closed:
                logger.info(
                    "closing_internally_managed_session",
                    action="close_session",
                    exchange=self.exchange_name,
                    message=f"[{self.exchange_name}] Closing internally managed ClientSession.",
                )
                await self._session.close()
                self._session = None
            elif self._external_session:
                logger.debug(
                    "external_session_not_closed_by_http_client",
                    action="close_session",
                    exchange=self.exchange_name,
                    message=f"[{self.exchange_name}] External session not closed by HttpClient.",
                )
            else:
                logger.debug(
                    "internal_session_already_closed_or_none",
                    action="close_session",
                    exchange=self.exchange_name,
                    message=f"[{self.exchange_name}] Internal ClientSession already closed/None.",
                )

    async def _parse_and_validate_response(
        self,
        response: aiohttp.ClientResponse,
        full_url: str,  # For logging context
    ) -> tuple[
        ParsedJsonResponse | str | None,
        int,
        ProcessedResponseHeaders,
        CIMultiDictProxy[str],
    ]:
        """Parses the HTTP response, validates headers, and extracts content.

        Returns content, status code, processed headers, and raw headers.

        Raises HttpRequestFailedError for issues like invalid Content-Type, body read errors,
        or JSON decoding failures.
        """
        response_text: str | None = None
        raw_response_headers: CIMultiDictProxy[str] = response.headers

        # Process relevant headers into the Pydantic model
        try:
            processed_content_type_str = raw_response_headers.get("Content-Type", "").lower()
            processed_headers = ProcessedResponseHeaders(content_type=processed_content_type_str)
        except ValidationError as ve:
            original_content_type = raw_response_headers.get("Content-Type", "")
            logger.warning(
                "invalid_content_type_from_response",
                action="parse_and_validate_response",
                exchange=self.exchange_name,
                full_url=full_url,
                validation_error=str(ve),
                original_content_type_preview=original_content_type[: MAX_CONTENT_TYPE_LENGTH + 20],
                message=(
                    f"[{self.exchange_name}] Invalid Content-Type from {full_url}: {ve}. "
                    f"Raw (first {MAX_CONTENT_TYPE_LENGTH + 20} chars): "
                    f"'{original_content_type[: MAX_CONTENT_TYPE_LENGTH + 20]}...'"
                ),
            )
            raise HttpRequestFailedError(
                message=f"Invalid Content-Type header from server at {full_url}.",
                http_status_code=response.status,
                response_body=f"Invalid Content-Type: {original_content_type}",
                api_error_code=APIErrorCode.INVALID_RESPONSE,
            ) from ve

        # Check for 204 No Content BEFORE attempting to read body
        if response.status == HTTPStatus.NO_CONTENT.value:
            logger.debug(
                "received_204_no_content",
                action="parse_and_validate_response",
                exchange=self.exchange_name,
                full_url=full_url,
                message=f"[{self.exchange_name}] Received 204 No Content for {full_url}.",
            )
            return (
                None,
                response.status,
                processed_headers,
                raw_response_headers,
            )  # Should have already returned

        try:
            response_text = await response.text()
        except aiohttp.ClientPayloadError as e_payload:
            logger.warning(
                "error_reading_response_body",
                action="parse_and_validate_response",
                exchange=self.exchange_name,
                full_url=full_url,
                status_code=response.status,
                error_details=str(e_payload),
                message=(
                    f"[{self.exchange_name}] Error reading response body for {full_url} "
                    f"(status {response.status}): {e_payload}"
                ),
            )
            raise HttpRequestFailedError(
                message=f"Failed to read response body from {full_url}. Status: {response.status}",
                http_status_code=response.status,
                response_body=None,
                api_error_code=APIErrorCode.NETWORK_ISSUE,
            ) from e_payload

        logger.debug(
            "response_received",
            action="parse_and_validate_response",
            exchange=self.exchange_name,
            full_url=full_url,
            status_code=response.status,
            response_headers=dict(raw_response_headers),
            body_preview=response_text[:200] if response_text else "[Empty]",
            message=(
                f"[{self.exchange_name}] Response from {full_url} (status {response.status}): "
                f"Headers={raw_response_headers}, "
                f"Body='{response_text[:200] if response_text else '[Empty]'}'..."
            ),
        )

        # Double check 204, though it should be caught above. response.text() might be called.
        if response.status == HTTPStatus.NO_CONTENT.value:
            return (
                None,
                response.status,
                processed_headers,
                raw_response_headers,
            )  # Should have already returned

        # Ensure we don't try to parse JSON if there's no body, even if headers suggest it.
        # This handles cases where response.text() might yield an empty string for an empty body.
        if not response_text:  # Handles both None and empty string for non-204
            if "application/json" in processed_headers.content_type:
                logger.warning(
                    "json_content_type_with_empty_body",
                    action="parse_and_validate_response",
                    exchange=self.exchange_name,
                    full_url=full_url,
                    status_code=response.status,
                    message=(
                        f"[{self.exchange_name}] JSON content type, but response body is "
                        f"empty/None "
                        f"for {full_url} (status {response.status})."
                    ),
                )
                raise HttpRequestFailedError(
                    message=f"JSON content type with empty/None body from {full_url}",
                    http_status_code=response.status,
                    response_body=response_text,  # Pass the original empty/None text
                    api_error_code=APIErrorCode.INVALID_RESPONSE,  # Use the enum member directly
                )
            # If not JSON and empty, it could be valid (e.g. just headers)
            return (None, response.status, processed_headers, raw_response_headers)

        # For 200-299 (excluding 204 handled above)
        if "application/json" in processed_headers.content_type:
            try:
                # Ensure response_text is a string for json.loads
                # The check `if not response_text:` above handles None or empty string.
                # So here, response_text should be a non-empty string.
                logger.debug(
                    "raw_json_response_text",
                    action="parse_and_validate_response",
                    response_text=response_text,
                    message=f"Raw JSON response_text in HttpClient: {response_text}",
                )
                parsed_json: ParsedJsonResponse = json.loads(response_text)
                return parsed_json, response.status, processed_headers, raw_response_headers
            except json.JSONDecodeError as je:
                logger.warning(
                    "json_decode_failed",
                    action="parse_and_validate_response",
                    exchange=self.exchange_name,
                    full_url=full_url,
                    status_code=response.status,
                    content_type=processed_headers.content_type,
                    json_error=str(je),
                    response_text_preview=response_text[:70],
                    message=(
                        f"[{self.exchange_name}] JSON decode failed for {full_url} "
                        f"(status {response.status}, type: {processed_headers.content_type}). "
                        f"Error: {je}. Text: '{response_text[:70]}...'"
                    ),
                )
                raise HttpRequestFailedError(
                    message=(
                        f"Failed to decode JSON response from {full_url}. Status: {response.status}"
                    ),
                    http_status_code=response.status,
                    response_body=response_text,
                    api_error_code=APIErrorCode.INVALID_RESPONSE,
                ) from je
        else:  # Not JSON, return raw text
            # response_text is guaranteed to be non-None and non-empty at this point
            return response_text, response.status, processed_headers, raw_response_headers

    async def request(
        self,
        method: str,
        endpoint_path: str,
        authenticator: IAuthenticator | None = None,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        is_signed: bool = False,
        request_timeout: float | None = None,
        serialize_none_as_null: bool = False,
    ) -> tuple[
        ParsedJsonResponse | str | None,
        int,
        ProcessedResponseHeaders,
        CIMultiDictProxy[str],
    ]:
        """Executes an HTTP request with authentication and retries.

        Response parsing and validation are delegated to _parse_and_validate_response.
        Now returns content, status_code, processed_headers, and raw_headers.

        Args:
            method: HTTP method ('GET', 'POST', 'PUT', 'DELETE', etc.)
            endpoint_path: The API endpoint path or full URL
            authenticator: Optional authenticator for signing requests
            params: Optional query parameters for the request
            data: Optional request body data (for POST/PUT requests)
            headers: Optional additional headers to include
            is_signed: Whether the request requires authentication/signing
            request_timeout: Optional timeout for the request in seconds
            serialize_none_as_null: If True, apply special Hyperliquid order type cleaning
                                  to remove None values from order type fields.

        Returns:
            Tuple of (parsed_response, status_code, processed_headers, raw_headers)
        """
        # Prepare request components
        request_params, json_payload, full_url, effective_timeout = (
            self._prepare_request_components(method, endpoint_path, params, data, request_timeout)
        )

        # Setup headers and authentication
        request_headers, request_params, json_payload = await self._setup_request_headers_and_auth(
            authenticator,
            is_signed,
            headers,
            method,
            endpoint_path,
            request_params,
            json_payload,
            full_url,
        )

        # Execute request with retries
        return await self._execute_request_with_retries(
            method,
            full_url,
            request_params,
            json_payload,
            request_headers,
            effective_timeout,
            endpoint_path,
            params,
        )

    def _prepare_request_components(
        self,
        method: str,
        endpoint_path: str,
        params: dict[str, Any] | None,
        data: dict[str, Any] | None,
        request_timeout: float | None,
    ) -> tuple[dict[str, Any], dict[str, Any] | None, str, float]:
        """Prepare basic request components."""
        request_params = (params or {}).copy()
        json_payload: dict[str, Any] | None = None

        # Prepare JSON payload for non-GET requests
        # Note: Some exchanges (like Backpack) require JSON bodies in DELETE requests
        if method.upper() != "GET" and data is not None:
            json_payload = data
            # Log the actual JSON string for debugging
            json_string = json.dumps(json_payload)
            logger.info(
                "json_payload_to_be_sent",
                action="prepare_request_components",
                exchange=self.exchange_name,
                endpoint_path=endpoint_path,
                json_payload=json_string,
                message=(
                    f"[{self.exchange_name}] JSON payload to be sent to {endpoint_path}: "
                    f"{json_string}"
                ),
            )

        # Build full URL
        if endpoint_path.startswith(("http://", "https://")):
            full_url = endpoint_path
        else:
            clean_base = self.rest_endpoint.rstrip("/")
            clean_relative_path = endpoint_path.lstrip("/")
            full_url = urllib.parse.urljoin(f"{clean_base}/", clean_relative_path)

        # Determine effective timeout
        effective_timeout = (
            request_timeout if request_timeout is not None else self.default_request_timeout
        )

        return request_params, json_payload, full_url, effective_timeout

    async def _setup_request_headers_and_auth(
        self,
        authenticator: IAuthenticator | None,
        is_signed: bool,
        headers: dict[str, Any] | None,
        method: str,
        endpoint_path: str,
        request_params: dict[str, Any],
        json_payload: dict[str, Any] | None,
        full_url: str,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
        """Setup request headers and handle authentication."""
        session_for_initial_headers = await self._get_session()
        request_headers = dict(session_for_initial_headers.headers.copy())
        request_headers.update(headers or {})

        if is_signed:
            if not authenticator:
                logger.error(
                    "signed_request_missing_authenticator",
                    action="setup_request_headers_and_auth",
                    exchange=self.exchange_name,
                    full_url=full_url,
                    message=(
                        f"[{self.exchange_name}] Signed request to {full_url} needs authenticator."
                    ),
                )
                raise APIError(
                    "Authenticator is required for signed requests.",
                    code=APIErrorCode.AUTHENTICATION_FAILED.value,
                )

            try:
                auth_components: AuthenticatedRequestComponents = (
                    await authenticator.prepare_request(
                        method=method,
                        path=endpoint_path,
                        params=dict(request_params) if request_params else None,
                        data=json_payload,
                        headers=dict(request_headers),
                    )
                )
                request_headers.update(auth_components.headers)
                if auth_components.params is not None:
                    request_params = auth_components.params
                # Update json_payload with authenticated data
                json_payload = auth_components.data
            except APIError as e:
                logger.error(
                    "authentication_preparation_failed",
                    action="setup_request_headers_and_auth",
                    exchange=self.exchange_name,
                    full_url=full_url,
                    error_details=str(e),
                    message=f"[{self.exchange_name}] Auth prep failed for {full_url}: {e}",
                )
                raise

        return request_headers, request_params, json_payload

    async def _execute_request_with_retries(
        self,
        method: str,
        full_url: str,
        request_params: dict[str, Any],
        json_payload: dict[str, Any] | None,
        request_headers: dict[str, Any],
        effective_timeout: float,
        endpoint_path: str,
        original_params: dict[str, Any] | None,
    ) -> tuple[
        ParsedJsonResponse | str | None,
        int,
        ProcessedResponseHeaders,
        CIMultiDictProxy[str],
    ]:
        """Execute the HTTP request with retry logic."""
        current_attempt = 0
        last_exception: Exception | None = None

        while current_attempt <= self.max_retries:
            current_attempt += 1

            try:
                return await self._execute_single_request(
                    method,
                    full_url,
                    request_params,
                    json_payload,
                    request_headers,
                    effective_timeout,
                    current_attempt,
                    original_params,
                )
            except HttpRequestFailedError as e_http:
                last_exception = e_http
                if self._should_fail_fast(e_http):
                    raise
            except (TimeoutError, aiohttp.ClientError) as e_client:
                logger.warning(
                    "request_failed_on_attempt",
                    action="execute_request_with_retries",
                    exchange=self.exchange_name,
                    full_url=full_url,
                    attempt=current_attempt,
                    exception_type=type(e_client).__name__,
                    error_details=str(e_client),
                    message=(
                        f"[{self.exchange_name}] Request to {full_url} failed on attempt "
                        f"{current_attempt}: {type(e_client).__name__} - {e_client}"
                    ),
                )
                last_exception = e_client

            # Check if we should retry
            if current_attempt > self.max_retries:
                return self._handle_final_failure(last_exception, full_url, endpoint_path)

            # Apply retry delay
            if last_exception and not self._should_skip_retry_delay(last_exception):
                await self._apply_retry_delay(current_attempt, full_url)

        # This should not be reached, but handle it defensively
        raise APIError(
            f"Request processing loop exited unexpectedly for {full_url}.",
            code=APIErrorCode.UNKNOWN.value,
        )

    async def _execute_single_request(
        self,
        method: str,
        full_url: str,
        request_params: dict[str, Any],
        json_payload: dict[str, Any] | None,
        request_headers: dict[str, Any],
        effective_timeout: float,
        current_attempt: int,
        original_params: dict[str, Any] | None,
    ) -> tuple[
        ParsedJsonResponse | str | None,
        int,
        ProcessedResponseHeaders,
        CIMultiDictProxy[str],
    ]:
        """Execute a single HTTP request attempt."""
        session = await self._get_session()

        # Log request details
        request_log_details = (
            f"Attempt {current_attempt}/{self.max_retries + 1} - {method} {full_url}"
        )
        if original_params:
            request_log_details += f" | Params: {original_params}"
        logger.info(
            "http_request_attempt",
            action="execute_single_request",
            exchange=self.exchange_name,
            request_details=request_log_details,
            message=f"[{self.exchange_name}] {request_log_details}",
        )

        async with session.request(
            method,
            full_url,
            params=request_params or None,
            json=json_payload if method.upper() != "GET" and json_payload is not None else None,
            data=None,
            headers=request_headers,
            timeout=aiohttp.ClientTimeout(total=effective_timeout),
        ) as response:
            return await self._handle_response(response, full_url)

    async def _handle_response(
        self,
        response: aiohttp.ClientResponse,
        full_url: str,
    ) -> tuple[
        ParsedJsonResponse | str | None,
        int,
        ProcessedResponseHeaders,
        CIMultiDictProxy[str],
    ]:
        """Handle the HTTP response."""
        if HTTPStatus.OK.value <= response.status < HTTPStatus.MULTIPLE_CHOICES.value:
            try:
                return await self._parse_and_validate_response(response, full_url)
            except HttpRequestFailedError as e_parse:
                logger.warning(
                    "response_parse_validation_failed",
                    action="handle_response",
                    exchange=self.exchange_name,
                    full_url=full_url,
                    status_code=response.status,
                    parse_error_message=e_parse.message,
                    message=(
                        f"[{self.exchange_name}] Response parse/validation failed for {full_url} "
                        f"(status {response.status}): {e_parse.message}"
                    ),
                )
                if e_parse.code == APIErrorCode.INVALID_RESPONSE.value:
                    raise  # Fail fast on bad content from server
                raise  # Re-raise for retry handling

        # Handle non-2xx responses
        return await self._handle_error_response(response, full_url)

    async def _handle_error_response(
        self,
        response: aiohttp.ClientResponse,
        full_url: str,
    ) -> tuple[
        ParsedJsonResponse | str | None,
        int,
        ProcessedResponseHeaders,
        CIMultiDictProxy[str],
    ]:
        """Handle error responses (non-2xx status codes)."""
        logger.warning(
            "http_error_response",
            action="handle_error_response",
            exchange=self.exchange_name,
            status_code=response.status,
            full_url=full_url,
            message=f"[{self.exchange_name}] HTTP Error {response.status} for {full_url}.",
        )

        error_body_text: str | None = None
        try:
            error_body_text = await response.text()
            logger.debug(
                "error_response_body",
                action="handle_error_response",
                exchange=self.exchange_name,
                error_body_preview=error_body_text[:200],
                message=f"[{self.exchange_name}] Error body: {error_body_text[:200]}",
            )
        except Exception as e_text:
            logger.warning(
                "could_not_read_error_response_body",
                action="handle_error_response",
                exchange=self.exchange_name,
                error_details=str(e_text),
                message=f"[{self.exchange_name}] Could not read error response body: {e_text}",
            )

        error = HttpRequestFailedError(
            message=f"HTTP req failed with status {response.status}",
            http_status_code=response.status,
            response_body=error_body_text,
        )

        if response.status in {400, 401, 403, 404, 405, 406, 415}:
            logger.warning(
                "non_retryable_client_error",
                action="handle_error_response",
                exchange=self.exchange_name,
                status_code=response.status,
                full_url=full_url,
                message=(
                    f"[{self.exchange_name}] Non-retryable client error {response.status} "
                    f"for {full_url}. Failing fast."
                ),
            )

        raise error

    def _should_fail_fast(self, error: HttpRequestFailedError) -> bool:
        """Determine if an error should cause immediate failure without retries."""
        if error.code == APIErrorCode.INVALID_RESPONSE.value:
            return True
        return bool(
            hasattr(error, "http_status")
            and error.http_status in {400, 401, 403, 404, 405, 406, 415},
        )

    def _should_skip_retry_delay(self, exception: Exception) -> bool:
        """Determine if retry delay should be skipped for certain exceptions."""
        if isinstance(exception, HttpRequestFailedError):
            return exception.http_status in {400, 401, 403, 404, 405, 406, 415}
        return False

    async def _apply_retry_delay(self, current_attempt: int, full_url: str) -> None:
        """Apply exponential backoff delay before retry."""
        delay = self.retry_delay_seconds * (2 ** (current_attempt - 1))
        logger.info(
            "retrying_request_with_delay",
            action="apply_retry_delay",
            exchange=self.exchange_name,
            full_url=full_url,
            delay_seconds=delay,
            message=f"[{self.exchange_name}] Retrying {full_url} in {delay:.2f} seconds...",
        )
        await asyncio.sleep(delay)

    def _handle_final_failure(
        self,
        last_exception: Exception | None,
        full_url: str,
        endpoint_path: str,
    ) -> tuple[
        ParsedJsonResponse | str | None,
        int,
        ProcessedResponseHeaders,
        CIMultiDictProxy[str],
    ]:
        """Handle final failure after all retries exhausted."""
        logger.error(
            "request_failed_after_all_retries",
            action="handle_final_failure",
            exchange=self.exchange_name,
            full_url=full_url,
            total_attempts=self.max_retries + 1,
            last_error=str(last_exception),
            message=(
                f"[{self.exchange_name}] Request to {full_url} failed after "
                f"{self.max_retries + 1} attempts. Last error: {last_exception}"
            ),
        )

        if isinstance(last_exception, APIError | HttpRequestFailedError):
            raise last_exception

        if last_exception is None:
            raise ValueError("DEFENSIVE: last_exception is None after all retries failed")

        # Wrap other exceptions
        if isinstance(last_exception, TimeoutError):
            raise HttpRequestFailedError(
                message=(
                    f"Request failed after retries: "
                    f"{type(last_exception).__name__} - {last_exception}"
                ),
                http_status_code=0,
                response_body=str(last_exception),
                api_error_code=APIErrorCode.TIMEOUT,
            ) from last_exception
        raise HttpRequestFailedError(
            message=(
                f"Request failed after retries: {type(last_exception).__name__} - {last_exception}"
            ),
            http_status_code=0,
            response_body=str(last_exception),
            api_error_code=APIErrorCode.NETWORK_ISSUE,
        ) from last_exception

    async def __aenter__(self) -> HttpClient:
        """Enter the async context manager and ensure session is ready."""
        await self._get_session()  # Ensure session is created if used in "async with"
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the async context manager and clean up the session."""
        await self.close_session()
