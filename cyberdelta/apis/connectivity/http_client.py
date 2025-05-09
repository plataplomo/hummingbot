from __future__ import annotations

import asyncio
import json
import urllib.parse
from types import TracebackType
from typing import Any

import aiohttp
from multidict import CIMultiDictProxy

# Removed pydantic imports, now in connectivity_models
from pydantic import ValidationError

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)

# Import the model and constants from the new location
from cyberdelta.apis.connectivity.connectivity_models import (
    MAX_CONTENT_TYPE_LENGTH,
    HttpClientConfig,
    ProcessedResponseHeaders,
)
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)

# Type alias for parsed JSON responses
ParsedJsonResponse = dict[str, Any] | list[Any] | str


class HttpRequestFailedError(APIError):
    """Custom exception for HTTP request failures within HttpClient."""

    def __init__(
        self,
        message: str,
        http_status_code: int,
        response_body: str | None = None,
        api_error_code: APIErrorCode = APIErrorCode.NETWORK_ISSUE,
    ) -> None:
        super().__init__(
            message=message,
            code=api_error_code.value,
            exchange_message=response_body,
            http_status=http_status_code,
        )

    def __str__(self) -> str:
        status_str = (
            f"HTTP {self.http_status}" if self.http_status is not None else "HTTP UnknownStatus"
        )
        body_preview = self.exchange_message or "N/A"
        body_str = str(body_preview)
        return f"HttpRequestFailedError ({status_str}): {self.message}. Body: {body_str[:50]}..."


class HttpClient:
    """
    Generic HTTP client for making requests to exchange APIs.
    Handles session management, request signing, rate limiting, and retries.
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
        """
        Initializes the HttpClient.

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
            logger.info(f"[{self.exchange_name}] HttpClient initialized with external session.")
        else:
            self._session = None
            self._external_session = False
            logger.info(
                f"[{self.exchange_name}] HttpClient initialized for endpoint: {self.rest_endpoint}"
            )

    async def _get_session(self) -> aiohttp.ClientSession:
        """
        Provides an active aiohttp.ClientSession.
        Uses an externally provided session if available and valid,
        otherwise creates and manages one internally.
        """
        async with self._session_lock:
            if self._external_session and self._session and not self._session.closed:
                logger.debug(f"[{self.exchange_name}] Using external aiohttp ClientSession.")
                return self._session

            # If external session is not usable, or we are managing internally
            if self._session is None or self._session.closed:
                logger.info(
                    f"[{self.exchange_name}] Creating new internal aiohttp ClientSession "
                    f"(external_session={self._external_session})."
                )
                self._session = aiohttp.ClientSession(
                    headers={"User-Agent": f"CyberDeltaEngine/{self.exchange_name}"}
                )
                self._external_session = False  # Now internally managed
            else:
                logger.debug(f"[{self.exchange_name}] Internal ClientSession already closed/None.")
            return self._session

    async def close_session(self) -> None:
        """Closes the aiohttp.ClientSession if it's an internally managed one and is open."""
        async with self._session_lock:
            if not self._external_session and self._session and not self._session.closed:
                logger.info(f"[{self.exchange_name}] Closing internally managed ClientSession.")
                await self._session.close()
                self._session = None
            elif self._external_session:
                logger.debug(f"[{self.exchange_name}] External session not closed by HttpClient.")
            else:
                logger.debug(f"[{self.exchange_name}] Internal ClientSession already closed/None.")

    async def _parse_and_validate_response(
        self,
        response: aiohttp.ClientResponse,
        full_url: str,  # For logging context
    ) -> tuple[ParsedJsonResponse | str | None, ProcessedResponseHeaders, CIMultiDictProxy[str]]:
        """
        Parses the HTTP response, validates headers, and extracts content.

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
                f"[{self.exchange_name}] Invalid Content-Type from {full_url}: {ve}. "
                f"Raw (first {MAX_CONTENT_TYPE_LENGTH + 20} chars): "
                f"'{original_content_type[: MAX_CONTENT_TYPE_LENGTH + 20]}...'"
            )
            raise HttpRequestFailedError(
                message=f"Invalid Content-Type header from server at {full_url}.",
                http_status_code=response.status,
                response_body=f"Invalid Content-Type: {original_content_type}",
                api_error_code=APIErrorCode.INVALID_RESPONSE,
            ) from ve

        # Check for 204 No Content BEFORE attempting to read body
        if response.status == 204:
            logger.debug(f"[{self.exchange_name}] Received 204 No Content for {full_url}.")
            return None, processed_headers, raw_response_headers

        try:
            response_text = await response.text()
        except aiohttp.ClientPayloadError as e_payload:
            logger.warning(
                f"[{self.exchange_name}] Error reading response body for {full_url} "
                f"(status {response.status}): {e_payload}"
            )
            raise HttpRequestFailedError(
                message=f"Failed to read response body from {full_url}. Status: {response.status}",
                http_status_code=response.status,
                response_body=None,
                api_error_code=APIErrorCode.NETWORK_ISSUE,
            ) from e_payload

        logger.debug(
            f"[{self.exchange_name}] Response from {full_url} (status {response.status}): "
            f"Headers={raw_response_headers}, "
            f"Body='{response_text[:200] if response_text else '[Empty]'}'...'"
        )

        # Double check 204, though it should be caught above. response.text() might be called.
        if response.status == 204:
            return None, processed_headers, raw_response_headers  # Should have already returned

        # Ensure we don't try to parse JSON if there's no body, even if headers suggest it.
        # This handles cases where response.text() might yield an empty string for an empty body.
        if not response_text:  # Handles both None and empty string for non-204
            if "application/json" in processed_headers.content_type:
                logger.warning(
                    f"[{self.exchange_name}] JSON content type, but response body is empty/None"
                    f" for {full_url} (status {response.status})."
                )
                raise HttpRequestFailedError(
                    message=f"JSON content type with empty/None body from {full_url}",
                    http_status_code=response.status,
                    response_body=response_text,  # Pass the original empty/None text
                    api_error_code=APIErrorCode.INVALID_RESPONSE,  # Use the enum member directly
                )
            # If not JSON and empty, it could be valid (e.g. just headers)
            return None, processed_headers, raw_response_headers

        # For 200-299 (excluding 204 handled above)
        if "application/json" in processed_headers.content_type:
            try:
                # Ensure response_text is a string for json.loads
                # The check `if not response_text:` above handles None or empty string.
                # So here, response_text should be a non-empty string.
                parsed_json: ParsedJsonResponse = json.loads(response_text)
                return parsed_json, processed_headers, raw_response_headers
            except json.JSONDecodeError as je:
                logger.warning(
                    f"[{self.exchange_name}] JSON decode failed for {full_url} "
                    f"(status {response.status}, type: "
                    f"{processed_headers.content_type}). Error: {je}. "
                    f"Text: '{response_text[:70]}...'"
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
            assert response_text is not None, (
                f"DEFENSIVE: response_text is None for {full_url} with non-JSON 2xx non-204 status"
            )
            return response_text, processed_headers, raw_response_headers

    async def request(
        self,
        method: str,
        endpoint_path: str,
        rate_limiter_service: RateLimiterService,
        authenticator: IAuthenticator | None = None,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        is_signed: bool = False,
        request_timeout: float | None = None,
    ) -> tuple[ParsedJsonResponse | str | None, ProcessedResponseHeaders, CIMultiDictProxy[str]]:
        """
        Executes an HTTP request with authentication, rate limiting, and retries.
        Response parsing and validation are delegated to _parse_and_validate_response.
        """
        request_params = (params or {}).copy()
        request_data = data

        if endpoint_path.startswith(("http://", "https://")):
            full_url = endpoint_path
        else:
            # Ensure rest_endpoint has no trailing slash before appending one for urljoin
            clean_base = self.rest_endpoint.rstrip("/")
            # Ensure relative_path has no leading slash
            clean_relative_path = endpoint_path.lstrip("/")
            full_url = urllib.parse.urljoin(f"{clean_base}/", clean_relative_path)

        effective_timeout = (
            request_timeout if request_timeout is not None else self.default_request_timeout
        )

        session_for_initial_headers = await self._get_session()
        request_headers = session_for_initial_headers.headers.copy()
        request_headers.update(headers or {})

        current_attempt = 0
        last_exception: Exception | None = None

        if is_signed:
            if not authenticator:
                logger.error(
                    f"[{self.exchange_name}] Signed request to {full_url} needs authenticator."
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
                        params=request_params if request_params else None,
                        data=request_data if request_data else None,
                        headers=dict(request_headers),
                    )
                )
                request_headers.update(auth_components["headers"])
                if auth_components["params"] is not None:
                    request_params = auth_components["params"]
            except APIError as e:
                logger.error(f"[{self.exchange_name}] Auth prep failed for {full_url}: {e}")
                raise

        while current_attempt <= self.max_retries:
            current_attempt += 1
            limiter: TokenBucketRateLimiterRuntime = rate_limiter_service.get_limiter(
                method, endpoint_path
            )
            await limiter.acquire()

            session = await self._get_session()
            request_log_details = (
                f"Attempt {current_attempt}/{self.max_retries + 1} - {method} {full_url}"
            )
            if params:  # Changed from request_params to params for original log
                request_log_details += f" | Params: {params}"
            logger.info(f"[{self.exchange_name}] {request_log_details}")

            try:
                async with session.request(
                    method,
                    full_url,
                    params=request_params if request_params else None,
                    json=request_data
                    if method.upper() not in ["GET", "DELETE"] and request_data is not None
                    else None,
                    data=None
                    if method.upper() not in ["GET", "DELETE"] and request_data is not None
                    else request_data,
                    headers=request_headers,
                    timeout=aiohttp.ClientTimeout(total=effective_timeout),
                ) as response:
                    # Delegate to the new method for 2xx responses
                    if 200 <= response.status < 300:
                        try:
                            # _parse_and_validate_response handles 204 correctly as well
                            return await self._parse_and_validate_response(response, full_url)
                        except HttpRequestFailedError as e_parse:  # Catch errors from parsing
                            # Critical parse/validation errors, usually not retryable on same data
                            logger.warning(
                                f"[{self.exchange_name}] Response parse/validation failed for "
                                f"{full_url} (status {response.status}): {e_parse.message}"
                            )
                            last_exception = e_parse  # Store it
                            # Decide if this specific parsing error should allow a retry.
                            if e_parse.code == APIErrorCode.INVALID_RESPONSE.value:
                                raise  # Fail fast on bad content from server
                            # For other parse errors (like NETWORK_ISSUE from payload problem),
                            # treat this attempt as failed and proceed to next retry.
                            continue  # Ensure we go to the next retry attempt

                    # Handle HTTP errors (>= 400) or other non-2xx/non-3xx cases
                    # This block is reached if status is not 2xx, or if a 2xx parsing error
                    # (not INVALID_RESPONSE) occurred and we continued, but then the outer
                    # try block finishes.
                    # To prevent re-processing a response that already had a parse error,
                    # we check last_exception.
                    # However, the `continue` above should prevent falling through here
                    # for handled parse errors. This part is primarily for non-2xx.
                    logger.warning(
                        f"[{self.exchange_name}] HTTP Error {response.status} for {full_url}. "
                        # Attempt to read body for error context, but guard it
                    )
                    error_body_text: str | None = None
                    try:
                        error_body_text = await response.text()
                        logger.debug(f"[{self.exchange_name}] Error body: {error_body_text[:200]}")
                    except Exception as e_text:
                        logger.warning(
                            f"[{self.exchange_name}] Could not read error response body: {e_text}"
                        )

                    last_exception = HttpRequestFailedError(
                        message=f"HTTP req to {endpoint_path} failed with status {response.status}",
                        http_status_code=response.status,
                        response_body=error_body_text,
                    )
                    if response.status in [400, 401, 403, 404, 405, 406, 415]:
                        logger.warning(
                            f"[{self.exchange_name}] Non-retryable client error {response.status} "
                            f"for {full_url}. Failing fast."
                        )
                        raise last_exception

            except (TimeoutError, aiohttp.ClientError) as e:
                logger.warning(
                    f"[{self.exchange_name}] Request to {full_url} failed on attempt "
                    f"{current_attempt}: {type(e).__name__} - {e}"
                )
                last_exception = e
            except HttpRequestFailedError as e_http_direct:  # Catch if raised directly
                logger.warning(
                    f"[{self.exchange_name}] Error from response processing: "
                    f"{e_http_direct.message}"
                )
                last_exception = e_http_direct
                if e_http_direct.code == APIErrorCode.INVALID_RESPONSE.value:
                    raise  # Do not retry if server sent invalid content structure/type

            if current_attempt > self.max_retries:
                logger.error(
                    f"[{self.exchange_name}] Request to {full_url} failed after "
                    f"{self.max_retries + 1} attempts. Last error: {last_exception}"
                )
                if isinstance(last_exception, APIError | HttpRequestFailedError):  # UP038
                    raise last_exception
                assert last_exception is not None, (
                    "DEFENSIVE: last_exception is None after all retries failed"
                )
                # Wrap other client-side exceptions specifically
                if isinstance(last_exception, TimeoutError):
                    raise HttpRequestFailedError(
                        message=f"Request failed after retries: "
                        f"{type(last_exception).__name__} - {last_exception}",
                        http_status_code=0,
                        response_body=str(last_exception),
                        api_error_code=APIErrorCode.TIMEOUT,  # Specific code for timeout
                    ) from last_exception
                # If not an APIError, HttpRequestFailedError, or TimeoutError, assume it's another
                # client-side error (likely aiohttp.ClientError) or an unexpected one.
                # Wrap it as a generic network issue.
                else:
                    raise HttpRequestFailedError(
                        message=f"Request failed after retries: "
                        f"{type(last_exception).__name__} - {last_exception}",
                        http_status_code=0,
                        response_body=str(last_exception),
                        api_error_code=APIErrorCode.NETWORK_ISSUE,
                    ) from last_exception

            if last_exception:
                # Check if the specific last_exception should prevent a retry
                # (e.g., non-retryable HttpRequestFailedError already raised and caught)
                if isinstance(
                    last_exception, HttpRequestFailedError
                ) and last_exception.http_status in [400, 401, 403, 404, 405, 406, 415]:
                    # This should have been raised and exited loop already
                    logger.debug(
                        f"Non-retryable error caught again, should have exited: {last_exception}"
                    )
                    raise last_exception

                delay = self.retry_delay_seconds * (2 ** (current_attempt - 1))
                logger.info(f"[{self.exchange_name}] Retrying {full_url} in {delay:.2f} seconds...")
                await asyncio.sleep(delay)

        logger.error(f"[{self.exchange_name}] Exited request loop for {full_url} unexpectedly.")
        # Ensure last_exception exists if loop finishes without returning/raising explicitly
        final_error_message = f"Request processing loop exited unexpectedly for {full_url}."
        if last_exception:
            final_error_message += (
                f" Last known error: {type(last_exception).__name__} - {last_exception}"
            )
            if isinstance(last_exception, APIError | HttpRequestFailedError):  # UP038
                raise last_exception
            elif isinstance(last_exception, TimeoutError):
                raise HttpRequestFailedError(
                    message=final_error_message,
                    http_status_code=0,
                    response_body=str(last_exception),
                    api_error_code=APIErrorCode.TIMEOUT,
                ) from last_exception
            # If not APIError/HttpRequestFailedError or TimeoutError, the linter implies
            # the remaining type for last_exception (from the try/excepts above) must be
            # aiohttp.ClientError or a subtype. We treat this as a NETWORK_ISSUE.
            # Any truly other unexpected exception types would ideally not reach here
            # or would be caught by a broader Exception handler if logic was different.
            else:  # Assumed to be aiohttp.ClientError or related based on linter feedback
                raise HttpRequestFailedError(
                    message=final_error_message,
                    http_status_code=0,
                    response_body=str(last_exception),
                    api_error_code=APIErrorCode.NETWORK_ISSUE,  # Default for other client errors
                ) from last_exception
        # If loop exited and last_exception is somehow None (should not happen)
        raise APIError(final_error_message, code=APIErrorCode.UNKNOWN.value)

    async def __aenter__(self) -> HttpClient:
        await self._get_session()  # Ensure session is created if used in "async with"
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close_session()
