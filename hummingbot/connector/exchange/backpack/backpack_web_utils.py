"""Web utility functions for Backpack exchange connector.
Provides factory methods for creating web assistants and helpers for API communication.
"""

import time
from collections.abc import Callable
from typing import Any

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class BackpackRESTPreProcessor(RESTPreProcessorBase):
    """Pre-processor for Backpack REST requests."""

    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        """Add default headers and process request before sending.

        Args:
            request: REST request to process

        Returns:
            Processed request
        """
        if request.headers is None:
            request.headers = {}

        # Add default headers
        request.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "Hummingbot-Backpack-Connector/1.0",
        })

        return request


def build_api_factory(
    throttler: AsyncThrottler | None = None,
    time_synchronizer: TimeSynchronizer | None = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    time_provider: Callable | None = None,
    auth: AuthBase | None = None,
) -> WebAssistantsFactory:
    """Build WebAssistantsFactory for Backpack API communication.

    Args:
        throttler: Async throttler for rate limiting
        time_synchronizer: Time synchronizer for server time sync
        domain: Exchange domain
        time_provider: Function to get current server time
        auth: Authentication instance (optional)

    Returns:
        Configured WebAssistantsFactory
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
            BackpackRESTPreProcessor(),
        ],
    )

    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(
    throttler: AsyncThrottler,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    auth: AuthBase | None = None,
) -> WebAssistantsFactory:
    """Build WebAssistantsFactory without time synchronization pre-processor.
    Used for initial time sync requests.

    Args:
        throttler: Async throttler for rate limiting
        domain: Exchange domain
        auth: Authentication instance (optional)

    Returns:
        Configured WebAssistantsFactory
    """
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            BackpackRESTPreProcessor(),
        ],
    )

    return api_factory


async def get_current_server_time(
    throttler: AsyncThrottler | None = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """Get current server time from Backpack exchange.

    Args:
        throttler: Async throttler for rate limiting
        domain: Exchange domain

    Returns:
        Server time as Unix timestamp in seconds
    """
    throttler = throttler or create_throttler()
    # Create temporary factory without time sync to avoid circular dependency
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(
        throttler=throttler,
        domain=domain,
    )

    rest_assistant = await api_factory.get_rest_assistant()

    async with throttler.execute_task(limit_id=CONSTANTS.TIME_URL):
        base_url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN])
        url = f"{base_url}{CONSTANTS.TIME_URL}"
        request = RESTRequest(
            method=RESTMethod.GET,
            url=url,
        )

        response = await rest_assistant.call(request)

        if response.status == 200:
            data = await response.json()
            # Backpack returns server time in milliseconds
            return data.get("serverTime", time.time() * 1000) / 1000
        # Fallback to local time if server time not available
        return time.time()


def create_throttler(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> AsyncThrottler:
    """Create and configure async throttler for Backpack exchange.

    Args:
        domain: Exchange domain

    Returns:
        Configured AsyncThrottler
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def get_rest_url_for_endpoint(endpoint: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Get full REST URL for an API endpoint.

    Args:
        endpoint: API endpoint path
        domain: Exchange domain

    Returns:
        Full URL for the endpoint
    """
    base_url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN])
    endpoint = endpoint.removeprefix("/")

    return f"{base_url}{endpoint}"


def get_ws_url(domain: str = CONSTANTS.DEFAULT_DOMAIN, private: bool = False) -> str:
    """Get WebSocket URL for the specified domain.

    Args:
        domain: Exchange domain
        private: Whether to get private or public WebSocket URL

    Returns:
        WebSocket URL
    """
    ws_url = CONSTANTS.WSS_URLS.get(domain, CONSTANTS.WSS_URLS[CONSTANTS.DEFAULT_DOMAIN])
    return ws_url


class BackpackTimeSynchronizer(TimeSynchronizer):
    """Time synchronizer for Backpack exchange."""

    def __init__(self, throttler: AsyncThrottler, domain: str = CONSTANTS.DEFAULT_DOMAIN):
        """Initialize time synchronizer.

        Args:
            throttler: Async throttler for rate limiting
            domain: Exchange domain
        """
        self._throttler = throttler
        self._domain = domain
        super().__init__()

    async def _update_server_time_offset(self):
        """Update server time offset by querying Backpack time API."""
        try:
            server_time = await get_current_server_time(self._throttler, self._domain)
            local_time = time.time()
            self._time_offset = server_time - local_time
        except Exception:
            # If time sync fails, continue with local time
            self._time_offset = 0


def create_throttler(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> AsyncThrottler:
    """Create and configure async throttler for Backpack exchange.

    Args:
        domain: Exchange domain

    Returns:
        Configured AsyncThrottler
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def build_rate_limits_by_tier() -> dict[str, Any]:
    """Build rate limits configuration by tier.

    Returns:
        Rate limits configuration
    """
    return {
        "default": CONSTANTS.RATE_LIMITS,
    }


def public_rest_url(path: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Get full URL for public REST endpoint.

    Args:
        path: API endpoint path
        domain: Exchange domain

    Returns:
        Full URL
    """
    return get_rest_url_for_endpoint(path, domain)


def private_rest_url(path: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Get full URL for private REST endpoint.

    Args:
        path: API endpoint path
        domain: Exchange domain

    Returns:
        Full URL
    """
    return get_rest_url_for_endpoint(path, domain)


def ws_public_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Get public WebSocket URL.

    Args:
        domain: Exchange domain

    Returns:
        Public WebSocket URL
    """
    return get_ws_url(domain, private=False)


def ws_private_url(domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """Get private WebSocket URL.

    Args:
        domain: Exchange domain

    Returns:
        Private WebSocket URL
    """
    return get_ws_url(domain, private=True)
