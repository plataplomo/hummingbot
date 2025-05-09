from __future__ import annotations

import asyncio
import json
from types import TracebackType
from typing import Any

import aiohttp
from multidict import CIMultiDictProxy

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
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

    DEFAULT_MAX_RETRIES = 3
    DEFAULT_RETRY_DELAY_SECONDS = 5.0  # Default base delay for retries

    def __init__(
        self,
        exchange_name: str,
        rest_endpoint: str,
        default_request_timeout: float = 30.0,
        max_retries: int | None = None,
        retry_delay_seconds: float | None = None,
    ) -> None:
        """
        Initializes the HttpClient.

        Args:
            exchange_name: Name of the exchange (for logging).
            rest_endpoint: Base REST API endpoint URL.
            default_request_timeout: Default timeout for requests in seconds.
            max_retries: Maximum number of retries for transient errors.
            retry_delay_seconds: Base delay in seconds between retries (exponential backoff).
        """
        self.exchange_name = exchange_name
        self.rest_endpoint = rest_endpoint.rstrip("/")
        self.default_request_timeout = default_request_timeout
        self.max_retries = max_retries if max_retries is not None else self.DEFAULT_MAX_RETRIES
        self.retry_delay_seconds = (
            retry_delay_seconds
            if retry_delay_seconds is not None
            else self.DEFAULT_RETRY_DELAY_SECONDS
        )

        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()
        logger.info(
            f"[{self.exchange_name}] HttpClient initialized for endpoint: {self.rest_endpoint}"
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """
        Provides an active aiohttp.ClientSession.
        Creates a new session if one doesn't exist or is closed.
        """
        async with self._session_lock:
            if self._session is None or self._session.closed:
                logger.info(f"[{self.exchange_name}] Creating new aiohttp ClientSession.")
                # Consider adding connector options like limits if needed later
                self._session = aiohttp.ClientSession(
                    headers={"User-Agent": f"CyberDeltaEngine/{self.exchange_name}"}
                )
            return self._session

    async def close_session(self) -> None:
        """Closes the aiohttp.ClientSession if it exists and is open."""
        async with self._session_lock:
            if self._session and not self._session.closed:
                logger.info(f"[{self.exchange_name}] Closing aiohttp ClientSession.")
                await self._session.close()
                self._session = None
            else:
                logger.debug(f"[{self.exchange_name}] ClientSession already closed or None.")

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
    ) -> tuple[ParsedJsonResponse | str | None, CIMultiDictProxy[str]]:
        """
        Executes an HTTP request with authentication, rate limiting, and retries.

        Args:
            method: HTTP method (e.g., "GET", "POST").
            endpoint_path: API endpoint path (e.g., "/v1/orders").
            rate_limiter_service: Service to handle rate limiting.
            authenticator: Optional IAuthenticator instance for signed requests.
            params: Optional dictionary of query parameters.
            data: Optional dictionary of request body data (for POST/PUT).
            headers: Optional dictionary of request headers.
            is_signed: Boolean indicating if the request should be signed.
            request_timeout: Optional specific timeout for this request.

        Returns:
            A tuple containing:
            - The parsed response content (JSON dict/list, raw text, or None for 204).
            - The response headers.

        Raises:
            HttpRequestFailedError: For HTTP status codes >= 400 after retries.
            aiohttp.ClientError: For unrecoverable client-side network issues after retries.
            asyncio.TimeoutError: If the request times out after retries.
            APIError: If a signed request is attempted without an authenticator.
        """
        request_params = (params or {}).copy()
        request_data = data  # Keep original data for potential re-signing in retries

        # URL Construction - Handle absolute URLs in endpoint_path
        if endpoint_path.startswith(("http://", "https://")):
            full_url = endpoint_path
        else:
            full_url = f"{self.rest_endpoint}/{endpoint_path.lstrip('/')}"

        effective_timeout = (
            request_timeout if request_timeout is not None else self.default_request_timeout
        )

        session = await self._get_session()  # Get session early for its defaults
        request_headers = (
            session.headers.copy()
        )  # Start with session default headers (e.g. User-Agent)
        request_headers.update(headers or {})  # Merge with any explicitly passed headers

        current_attempt = 0
        last_exception: Exception | None = None  # Initialize last_exception

        # Authentication (if required) - outside retry loop if auth components don't change per try
        # However, if nonce/timestamp is part of auth, it might need to be in the loop
        # For now, assume auth components are prepared once before retries.
        # If an auth method requires fresh components per try, this needs adjustment.
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
                        path=endpoint_path,  # Path relative to base endpoint for authenticator
                        params=request_params if request_params else None,  # Pass current params
                        data=request_data if request_data else None,  # Pass current data
                        headers=dict(request_headers),  # Pass current headers as a plain dict
                    )
                )
                request_headers.update(auth_components["headers"])
                if auth_components["params"] is not None:  # authenticator might modify params
                    request_params = auth_components["params"]
                # Data is usually transformed into a signable format by authenticator,
                # but actual `data` for request body should be the original `request_data`
                # unless authenticator explicitly modifies it for the body.
                # For now, assuming authenticator mainly adds headers/params,
                # and `request_data` remains the body.
            except APIError as e:  # Catch APIErrors from authenticator (e.g. signing failed)
                logger.error(f"[{self.exchange_name}] Auth prep failed for {full_url}: {e}")
                raise  # Re-raise critical auth errors immediately

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
            if params:
                request_log_details += f" | Params: {params}"
            logger.info(f"[{self.exchange_name}] {request_log_details}")

            try:
                async with session.request(
                    method,
                    full_url,
                    params=request_params if request_params else None,
                    json=request_data
                    if method.upper() not in ["GET", "DELETE"] and request_data is not None
                    else None,  # Send as JSON if data exists and not GET/DELETE
                    data=None
                    if method.upper() not in ["GET", "DELETE"] and request_data is not None
                    else request_data,  # Pass data directly for GET/DELETE or non-json POST/PUT
                    headers=request_headers,
                    timeout=aiohttp.ClientTimeout(total=effective_timeout),
                ) as response:
                    response_text: str | None = None
                    response_headers: CIMultiDictProxy[str] = response.headers

                    # Check for 204 No Content BEFORE attempting to read body
                    if response.status == 204:
                        logger.debug(
                            f"[{self.exchange_name}] Received 204 No Content for {full_url}."
                        )
                        return None, response_headers

                    try:
                        response_text = await response.text()
                    except aiohttp.ClientPayloadError as e_payload:
                        logger.warning(
                            f"[{self.exchange_name}] Error reading response body for {full_url} "
                            f"(status {response.status}): {e_payload}"
                        )
                        # Treat as an HTTP request failure if we can't read the body of an otherwise
                        # successful (pre-400) response, as we can't parse it.
                        # If response.headers was accessible, it could be returned,
                        # but it might not be if the connection is already broken.
                        # For simplicity, raising without headers if body read fails.
                        request_error = HttpRequestFailedError(
                            message=f"Failed to read response body. Status: {response.status}",
                            http_status_code=response.status,
                            response_body=None,
                            api_error_code=APIErrorCode.NETWORK_ISSUE,
                        )
                        # This exception will be caught by the outer except block below
                        # to handle retries if applicable
                        # Raise the specific error to be caught by the except block below
                        raise request_error from e_payload

                    logger.debug(
                        f"[{self.exchange_name}] Response from {method} {full_url}: "
                        f"Status={response.status}, Headers={response_headers}, "
                        f"Body='{response_text[:200] if response_text else '[Empty]'}'...'"
                    )

                    if response.status == 204:  # No Content
                        logger.info(
                            f"[{self.exchange_name}] Received 204 No Content for {full_url}."
                        )
                        return None, response_headers

                    if response.status >= 200 and response.status < 300:
                        # Attempt to parse JSON if content type suggests it
                        content_type = response_headers.get("Content-Type", "").lower()
                        if "application/json" in content_type:
                            try:
                                parsed_json: ParsedJsonResponse = json.loads(response_text or "")
                                return parsed_json, response_headers
                            except json.JSONDecodeError as je:
                                logger.warning(
                                    f"[{self.exchange_name}] JSON decode failed for {full_url} "
                                    f"(status {response.status}, type: {content_type}). "
                                    f"Error: {je}. "
                                    f"Text: '{response_text[:70]}...'"
                                )
                                raise HttpRequestFailedError(
                                    message=(
                                        f"Failed to decode JSON response. Status: {response.status}"
                                    ),
                                    http_status_code=response.status,
                                    response_body=response_text,
                                    api_error_code=APIErrorCode.INVALID_RESPONSE,
                                ) from je
                        else:  # Not JSON, return raw text
                            assert response_text is not None, (
                                "DEFENSIVE: response_text is None post-read for 2xx non-204 status"
                            )
                            return response_text, response_headers

                    # If status is >= 400, it's an HTTP error
                    logger.warning(
                        f"[{self.exchange_name}] HTTP Error {response.status} for {full_url}. "
                        f"Body: {response_text[:200] if response_text else '[Empty]'}"
                    )
                    # This error will be caught by the outer try-except for retries or final raise
                    # We create it here to capture status and body correctly
                    last_exception = HttpRequestFailedError(
                        message=f"HTTP req to {endpoint_path} failed with status {response.status}",
                        http_status_code=response.status,
                        response_body=response_text,
                        # Default api_error_code is NETWORK_ISSUE, ExchangeAPI can map it better
                    )
                    # For client errors (4xx) that are not rate limits (429), or server errors (5xx)
                    # decide if retry is appropriate.
                    # Typically, 400, 401, 403, 404 are not retried. 429, 500, 502, 503, 504 are.
                    if response.status in [
                        400,
                        401,
                        403,
                        404,
                        405,
                        406,
                        415,
                    ]:  # Non-retryable client errors
                        logger.warning(
                            f"[{self.exchange_name}] Non-retryable client error {response.status} "
                            f"for {full_url}. Failing fast."
                        )
                        raise last_exception  # Fail fast

                    # Fall through to retry for other errors like 429, 5xx,
                    # or if last_exception was set by other means

            except (TimeoutError, aiohttp.ClientError) as e:
                logger.warning(
                    f"[{self.exchange_name}] Request to {full_url} failed on attempt "
                    f"{current_attempt}: {type(e).__name__} - {e}"
                )
                last_exception = e
                # These are generally retryable

            if current_attempt > self.max_retries:
                logger.error(
                    f"[{self.exchange_name}] Request to {full_url} failed after "
                    f"{self.max_retries + 1} attempts. Last error: {last_exception}"
                )
                if isinstance(last_exception, HttpRequestFailedError):
                    raise last_exception
                # Pylance indicates last_exception cannot be None here.
                # Need to ensure last_exception is always assigned before raising
                assert last_exception is not None, (
                    "DEFENSIVE: last_exception is None after all retries failed"
                )
                raise last_exception  # Re-raise other aiohttp/asyncio/APIError exceptions

            if last_exception:  # If an exception occurred that qualifies for retry
                delay = self.retry_delay_seconds * (
                    2 ** (current_attempt - 1)
                )  # Exponential backoff
                logger.info(f"[{self.exchange_name}] Retrying {full_url} in {delay:.2f} seconds...")
                await asyncio.sleep(delay)

        # Should be unreachable due to the loop condition and raise within the loop
        # but as a fallback:
        logger.error(f"[{self.exchange_name}] Exited request loop for {full_url} unexpectedly.")
        raise APIError(
            f"Request processing loop exited unexpectedly for {full_url}.",
            code=APIErrorCode.UNKNOWN.value,
        )

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
