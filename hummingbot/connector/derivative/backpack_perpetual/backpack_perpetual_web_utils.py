"""Web utilities for Backpack Perpetual Exchange connector.
"""

import asyncio
import json
import time
from collections.abc import Callable
from typing import Any

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


_bpwu_logger: HummingbotLogger | None = None


def build_api_factory(
    throttler: AsyncThrottler | None = None,
    time_synchronizer: TimeSynchronizer | None = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    time_provider: Callable | None = None,
    auth: AuthBase | None = None,
) -> WebAssistantsFactory:
    """Build a WebAssistantsFactory for Backpack Perpetual.

    Args:
        throttler: Rate limiter for API requests
        time_synchronizer: Time synchronization handler
        domain: Exchange domain
        time_provider: Function to get current time
        auth: Authentication handler

    Returns:
        Configured WebAssistantsFactory instance
    """
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time(
        throttler=throttler,
        domain=domain,
    ))

    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ])

    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(
    throttler: AsyncThrottler | None = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    auth: AuthBase | None = None,
) -> WebAssistantsFactory:
    """Build a WebAssistantsFactory without time synchronization.

    This is useful for initial connections where time sync isn't available yet.

    Args:
        throttler: Rate limiter for API requests
        domain: Exchange domain
        auth: Authentication handler

    Returns:
        Configured WebAssistantsFactory instance
    """
    throttler = throttler or create_throttler()

    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
    )

    return api_factory


def create_throttler() -> AsyncThrottler:
    """Create a throttler with Backpack Perpetual rate limits.

    Returns:
        Configured AsyncThrottler instance
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
    throttler: AsyncThrottler | None = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """Get current server time from Backpack.

    Args:
        throttler: Rate limiter for API requests
        domain: Exchange domain

    Returns:
        Server timestamp in seconds
    """
    throttler = throttler or create_throttler()

    # Create a simple REST assistant without auth
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(
        throttler=throttler,
        domain=domain,
    )

    rest_assistant = await api_factory.get_rest_assistant()

    # Query server time endpoint
    base_url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN])
    url = f"{base_url}{CONSTANTS.TIME_URL}"

    async with throttler.execute_task(CONSTANTS.PUBLIC_ENDPOINT_LIMIT_ID):
        response = await rest_assistant.execute_request(
            url=url,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.PUBLIC_ENDPOINT_LIMIT_ID,
        )

        # Response could be dict or string from execute_request
        if isinstance(response, str):
            # Try to parse as JSON if it's a string
            data: dict[str, Any] = json.loads(response)
        else:
            data = response

        # Backpack returns time in milliseconds
        server_time_ms = data.get("serverTime", data.get("timestamp"))
        if server_time_ms is None:
            raise OSError(f"No time field in response: {data}")

        return int(server_time_ms) / 1000.0  # Convert to seconds


async def api_request(
    path: str,
    api_factory: WebAssistantsFactory | None = None,
    throttler: AsyncThrottler | None = None,
    time_synchronizer: TimeSynchronizer | None = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    method: RESTMethod = RESTMethod.GET,
    is_auth_required: bool = False,
    return_err: bool = False,
    limit_id: str | None = None,
    timeout: float = CONSTANTS.REQUEST_TIMEOUT,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Make an API request to Backpack Perpetual.

    Args:
        path: API endpoint path
        api_factory: WebAssistantsFactory instance
        throttler: Rate limiter
        time_synchronizer: Time synchronizer
        domain: Exchange domain
        params: Query parameters
        data: Request body data
        method: HTTP method
        is_auth_required: Whether authentication is required
        return_err: Whether to return error response
        limit_id: Rate limit ID
        timeout: Request timeout
        headers: Additional headers

    Returns:
        Response data as dictionary
    """
    throttler = throttler or create_throttler()

    # Build API factory if not provided
    if api_factory is None:
        api_factory = build_api_factory(
            throttler=throttler,
            time_synchronizer=time_synchronizer,
            domain=domain,
        )

    # Determine rate limit ID
    if limit_id is None:
        limit_id = CONSTANTS.PRIVATE_ENDPOINT_LIMIT_ID if is_auth_required else CONSTANTS.PUBLIC_ENDPOINT_LIMIT_ID

    # Build full URL
    base_url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN])
    url = f"{base_url}{path}"

    # Get REST assistant
    rest_assistant = await api_factory.get_rest_assistant()

    # Execute request with rate limiting
    async with throttler.execute_task(limit_id):
        try:
            response = await rest_assistant.execute_request(
                url=url,
                method=method,
                throttler_limit_id=limit_id,
                params=params,
                data=data,
                headers=headers,
                timeout=timeout,
            )

            # Response could be str or dict from execute_request
            if isinstance(response, str):
                # Try to parse as JSON if it's a string
                return json.loads(response)
            return response

        except asyncio.TimeoutError as e:
            raise OSError(f"API request timeout {method} {url}") from e
        except Exception as e:
            raise OSError(f"Error in API request {method} {url}: {e!s}") from e


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Get the full URL for a public REST endpoint.

    Args:
        path_url: API endpoint path
        domain: Exchange domain

    Returns:
        Full URL for the public endpoint
    """
    base_url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN])
    return f"{base_url}{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Get the full URL for a private REST endpoint.

    Args:
        path_url: API endpoint path
        domain: Exchange domain

    Returns:
        Full URL for the private endpoint
    """
    base_url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN])
    return f"{base_url}{path_url}"


def get_rest_url_for_endpoint(
    endpoint: str,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> str:
    """Get the full REST URL for an endpoint.

    Args:
        endpoint: API endpoint path
        domain: Exchange domain

    Returns:
        Full URL for the endpoint
    """
    base_url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN])
    return f"{base_url}{endpoint}"


def get_ws_url_for_endpoint(
    endpoint: str,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    public: bool = True,
) -> str:
    """Get the WebSocket URL for an endpoint.

    Args:
        endpoint: WebSocket endpoint
        domain: Exchange domain
        public: Whether this is a public endpoint

    Returns:
        WebSocket URL
    """
    # Backpack uses the same WebSocket URL for public and private channels
    # Authentication determines access to private channels
    base_ws_url = CONSTANTS.WSS_URLS.get(domain, CONSTANTS.WSS_URLS[CONSTANTS.DEFAULT_DOMAIN])
    return f"{base_ws_url}{endpoint}" if endpoint else base_ws_url


async def build_ws_connection(
    api_factory: WebAssistantsFactory,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    public: bool = True,
) -> WSAssistant:
    """Build a WebSocket connection.

    Args:
        api_factory: WebAssistantsFactory instance
        domain: Exchange domain
        public: Whether this is a public connection

    Returns:
        Connected WSAssistant instance
    """
    ws_url = get_ws_url_for_endpoint("", domain, public)

    ws_assistant = await api_factory.get_ws_assistant()
    await ws_assistant.connect(
        ws_url=ws_url,
        message_timeout=CONSTANTS.WS_MESSAGE_TIMEOUT,
    )

    return ws_assistant


def next_message_id() -> int:
    """Generate the next message ID for WebSocket messages.

    Returns:
        Incrementing message ID
    """
    return int(time.time() * 1000) % 1000000000
