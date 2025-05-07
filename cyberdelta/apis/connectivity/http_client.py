from __future__ import annotations

import asyncio
import json
from types import TracebackType
from typing import Any, Self, Union

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
ParsedJsonResponse = Union[dict[str, Any], list[Any], str]


class HttpRequestFailedError(APIError):
    """Custom exception for HTTP request failures within HttpClient."""

    def __init__(
        self,
        message: str,
        http_status_code: int,
        response_body: str | None = None,
        api_error_code: APIErrorCode = APIErrorCode.NETWORK_ISSUE,
    ):
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
        return f"HttpRequestFailedError ({status_str}): {self.message}. Body: {self.exchange_message or 'N/A'}"


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
    ):
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
        full_url = f"{self.rest_endpoint}/{endpoint_path.lstrip('/')}"
        effective_timeout = (
            request_timeout if request_timeout is not None else self.default_request_timeout
        )

        request_headers = (headers or {}).copy()
        request_params = (params or {}).copy()
        request_data = data  # Keep original data for potential re-signing in retries

        current_attempt = 0
        last_exception: Exception | None = None

        # Authentication (if required) - outside retry loop if auth components don't change per try
        # However, if nonce/timestamp is part of auth, it might need to be in the loop
        # For now, assume auth components are prepared once before retries.
        # If an auth method requires fresh components per try, this needs adjustment.
        if is_signed:
            if not authenticator:
                logger.error(
                    f"[{self.exchange_name}] Attempted to make a signed request to {full_url} without an authenticator."
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
                        headers=request_headers,  # Pass current headers
                    )
                )
                request_headers.update(auth_components["headers"])
                if auth_components["params"] is not None:  # authenticator might modify params
                    request_params = auth_components["params"]
                # Data is usually transformed into a signable format by authenticator, but actual `data` for request body
                # should be the original `request_data` unless authenticator explicitly modifies it for the body.
                # For now, assuming authenticator mainly adds headers/params, and `request_data` remains the body.
                # If `auth_components["data"]` is meant to be the new body, this would change:
                # request_data_for_body = auth_components["data"] if auth_components["data"] is not None else request_data
                # For now, we assume `data` passed to `session.request` is `request_data`.
            except APIError as e:  # Catch APIErrors from authenticator (e.g. signing failed)
                logger.error(
                    f"[{self.exchange_name}] Authentication preparation failed for {full_url}: {e}"
                )
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
            if request_params:
                request_log_details += f" | Params: {request_params}"
            # Avoid logging full data payload if it's large or sensitive by default
            # logger.debug(f"[{self.exchange_name}] {request_log_details} | Headers: {request_headers} | Data: {request_data}")
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
                    else request_data,  # handle plain data if not json
                    headers=request_headers,
                    timeout=aiohttp.ClientTimeout(total=effective_timeout),
                ) as response:
                    response_text: str | None = None
                    response_headers: CIMultiDictProxy[str] = response.headers
                    try:
                        response_text = await response.text()
                    except Exception as e_text:
                        logger.warning(
                            f"[{self.exchange_name}] Error reading response text from {full_url}: {e_text}"
                        )
                        # Continue to process status code, response_text will be None

                    logger.debug(
                        f"[{self.exchange_name}] Response from {method} {full_url}: "
                        f"Status={response.status}, Headers={response_headers}, Body='{response_text[:500] if response_text else '[None or Unreadable]'}...'"
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
                                parsed_json: ParsedJsonResponse = await response.json()
                                return parsed_json, response_headers
                            except json.JSONDecodeError as je:
                                logger.warning(
                                    f"[{self.exchange_name}] Failed to decode JSON response from {full_url} "
                                    f"(status {response.status}, content-type: {content_type}), "
                                    f"returning raw text. Error: {je}. Text: '{response_text[:200] if response_text else ''}...'"
                                )
                                if response_text is None:
                                    raise HttpRequestFailedError(
                                        message=f"Failed to read response body after JSON decode error. Status: {response.status}",
                                        http_status_code=response.status,
                                        response_body=None,
                                        api_error_code=APIErrorCode.UNKNOWN,
                                    )
                                return response_text, response_headers
                        else:  # Not JSON, return raw text
                            if response_text is None:
                                raise HttpRequestFailedError(
                                    message=f"Successfully received status {response.status} but failed to read response body.",
                                    http_status_code=response.status,
                                    response_body=None,
                                    api_error_code=APIErrorCode.UNKNOWN,
                                )
                            return response_text, response_headers

                    # If status is >= 400, it's an HTTP error
                    logger.warning(
                        f"[{self.exchange_name}] HTTP Error {response.status} for {full_url}. "
                        f"Body: {response_text[:500] if response_text else '[N/A]'}"
                    )
                    # This error will be caught by the outer try-except for retries or final raise
                    # We create it here to capture status and body correctly
                    last_exception = HttpRequestFailedError(
                        message=f"HTTP request to {endpoint_path} failed with status {response.status}",
                        http_status_code=response.status,
                        response_body=response_text,
                        # Default api_error_code is NETWORK_ISSUE, ExchangeAPI can map it better via _map_error_response
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
                            f"[{self.exchange_name}] Non-retryable client error {response.status} for {full_url}. Failing fast."
                        )
                        raise last_exception  # Fail fast

                    # Fall through to retry for other errors like 429, 5xx, or if last_exception was set by other means

            except (TimeoutError, aiohttp.ClientError) as e:
                logger.warning(
                    f"[{self.exchange_name}] Request to {full_url} failed on attempt {current_attempt}: {type(e).__name__} - {e}"
                )
                last_exception = e
                # These are generally retryable

            if current_attempt > self.max_retries:
                logger.error(
                    f"[{self.exchange_name}] Request to {full_url} failed after {self.max_retries + 1} attempts. Last error: {last_exception}"
                )
                if isinstance(last_exception, HttpRequestFailedError):
                    raise last_exception  # Already the correct type
                elif isinstance(last_exception, aiohttp.ClientError | asyncio.TimeoutError):
                    # Wrap generic aiohttp/asyncio errors in our HttpRequestFailedError or a more specific APIError if possible
                    # For simplicity, re-raising them directly is also an option if ExchangeAPI._request handles them.
                    # Prompt: "Error Handling: This method should primarily raise aiohttp.ClientError, asyncio.TimeoutError, or a simple custom HttpRequestFailedError(APIError)"
                    # So, we re-raise them directly.
                    raise last_exception
                else:  # Should not happen if last_exception is always set
                    raise APIError(
                        f"Unknown error after retries for {full_url}",
                        code=APIErrorCode.UNKNOWN.value,
                    ) from last_exception

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

    async def __aenter__(self) -> Self:
        await self._get_session()  # Ensure session is created if used in "async with"
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close_session()
