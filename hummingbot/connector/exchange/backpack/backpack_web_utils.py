"""Web utility functions for Backpack exchange connector."""

import time
from collections.abc import Callable
from typing import Any, Optional

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint
    :param path_url: a public REST endpoint
    :param domain: the Backpack domain to connect to
    :return: the full URL to the endpoint
    """
    base_url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN])
    return base_url + path_url


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    time_synchronizer: Optional[TimeSynchronizer] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    time_provider: Optional[Callable] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (
        lambda: get_current_server_time(
            throttler=throttler,
            domain=domain,
        )
    )

    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ],
    )

    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(
    throttler: AsyncThrottler,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
    )

    return api_factory


async def get_current_server_time(
    throttler: Optional[AsyncThrottler] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH_URL, domain=domain),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH_URL,
    )
    if isinstance(response, dict):
        server_time = response.get("serverTime", response.get("timestamp"))
        if server_time is not None:
            return float(server_time)
    if isinstance(response, (int, float)):
        return float(response)
    if isinstance(response, str):
        try:
            return float(response)
        except ValueError:
            pass
    return time.time() * 1e3


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided private REST endpoint
    :param path_url: a private REST endpoint
    :param domain: the Backpack domain to connect to
    :return: the full URL to the endpoint
    """
    base_url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN])
    return base_url + path_url


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


def normalize_response_to_list(response: Any) -> list[Any]:
    """Normalize API response to a list format.

    Backpack API returns either:
    - A list directly
    - A dict with a data key containing a list
    - A dict that should be wrapped in a list

    Args:
        response: Raw API response

    Returns:
        Normalized list response
    """
    if isinstance(response, list):
        return response
    elif isinstance(response, dict):
        # Check for common keys that contain list data
        for key in ["positions", "orders", "fills", "trades", "balances", "data"]:
            if key in response and isinstance(response[key], list):
                return response[key]
        # If it's a single item response, wrap it in a list
        return [response]
    else:
        return []


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


def build_rate_limits_by_tier() -> dict[str, Any]:
    """Build rate limits configuration by tier.

    Returns:
        Rate limits configuration
    """
    return {
        "default": CONSTANTS.RATE_LIMITS,
    }


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
