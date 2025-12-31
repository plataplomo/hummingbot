"""Web utilities for Backpack Perpetual Exchange connector."""

import asyncio
import json
import time
from collections.abc import Callable
from typing import Any, Optional, TypeVar

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

# Type variable for generic response handling
T = TypeVar("T")


_bpwu_logger: Optional[HummingbotLogger] = None


class HeadersContentRESTPreProcessor(RESTPreProcessorBase):
    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        request.headers = request.headers or {}
        request.headers["Content-Type"] = "application/json"
        return request


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided public REST endpoint
    :param path_url: a public REST endpoint
    :param domain: the Backpack domain to connect to
    :return: the full URL to the endpoint
    """
    base_url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN])
    return base_url + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for provided private REST endpoint
    :param path_url: a private REST endpoint
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
            HeadersContentRESTPreProcessor(),
        ],
    )

    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(
    throttler: Optional[AsyncThrottler] = None,
    auth: Optional[AuthBase] = None,
) -> WebAssistantsFactory:
    """Build a WebAssistantsFactory without time synchronization.

    This is useful for initial connections where time sync isn't available yet.

    Args:
        throttler: Rate limiter for API requests
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
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


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


def endpoint_from_message(message: dict[str, Any]) -> Optional[str]:
    endpoint = None
    if "request" in message:
        message = message["request"]
    if isinstance(message, dict):
        if "op" in message:
            endpoint = message["op"]
        elif "stream" in message:
            endpoint = message["stream"]
        elif "type" in message:
            endpoint = message["type"]
    return endpoint


def payload_from_message(message: dict[str, Any]) -> Any:
    payload = message
    if "data" in message:
        payload = message["data"]
    return payload


async def api_request(
    path: str,
    api_factory: Optional[WebAssistantsFactory] = None,
    throttler: Optional[AsyncThrottler] = None,
    time_synchronizer: Optional[TimeSynchronizer] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
    params: Optional[dict[str, Any]] = None,
    data: Optional[dict[str, Any]] = None,
    method: RESTMethod = RESTMethod.GET,
    is_auth_required: bool = False,
    return_err: bool = False,
    limit_id: Optional[str] = None,
    timeout: float = CONSTANTS.REQUEST_TIMEOUT,
    headers: Optional[dict[str, str]] = None,
) -> Any:
    """Make an API request to Backpack Perpetual.

    Note: Returns Any because Backpack API can return either dict or list depending on the endpoint.
    Use api_request_dict() or api_request_list() for type-safe access.

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
        Response data (can be dict or list)
    """
    throttler = throttler or create_throttler()

    # Build API factory if not provided
    if api_factory is None:
        api_factory = build_api_factory(
            throttler=throttler,
            time_synchronizer=time_synchronizer,
            domain=domain,
        )

    normalized_path = path.lstrip("/")
    if limit_id is None:
        limit_id = normalized_path

    # Build full URL
    base_url = CONSTANTS.REST_URLS.get(domain, CONSTANTS.REST_URLS[CONSTANTS.DEFAULT_DOMAIN])
    url = f"{base_url}{normalized_path}"

    # Get REST assistant
    rest_assistant = await api_factory.get_rest_assistant()

    try:
        response = await rest_assistant.execute_request(
            url=url,
            method=method,
            throttler_limit_id=limit_id,
            params=params,
            data=data,
            headers=headers,
            timeout=timeout,
            is_auth_required=is_auth_required,
        )

        # For PATCH requests that return 200 with no content, return empty dict
        if method == RESTMethod.PATCH and not response:
            return {}

        # Response could be str or dict from execute_request
        if isinstance(response, str):
            return json.loads(response) if response else {}
        return response

    except asyncio.TimeoutError as e:
        raise OSError(f"API request timeout {method} {url}") from e
    except Exception as e:
        raise OSError(f"Error in API request {method} {url}: {e!s}") from e


async def api_request_dict(
    path: str,
    **kwargs,
) -> dict[str, Any]:
    """Make an API request that is expected to return a dict.

    This is a type-safe wrapper around api_request for endpoints that always return dicts.

    Args:
        path: API endpoint path
        **kwargs: Additional arguments passed to api_request

    Returns:
        Response data as dictionary

    Raises:
        ValueError: If response is not a dictionary
    """
    response = await api_request(path, **kwargs)
    if not isinstance(response, dict):
        raise ValueError(f"Expected dict response from {path}, got {type(response).__name__}")
    return response


async def api_request_list(
    path: str,
    **kwargs,
) -> list[Any]:
    """Make an API request that is expected to return a list.

    This is a type-safe wrapper around api_request for endpoints that always return lists.

    Args:
        path: API endpoint path
        **kwargs: Additional arguments passed to api_request

    Returns:
        Response data as list

    Raises:
        ValueError: If response is not a list
    """
    response = await api_request(path, **kwargs)
    if not isinstance(response, list):
        raise ValueError(f"Expected list response from {path}, got {type(response).__name__}")
    return response


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
