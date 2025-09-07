import os
from typing import TYPE_CHECKING, TypeVar

import aiohttp

from hummingbot.core.web_assistant.connections.rest_connection import RESTConnection
from hummingbot.core.web_assistant.connections.ws_connection import WSConnection

if TYPE_CHECKING:
    pass

ConnectionsFactoryT = TypeVar("ConnectionsFactoryT", bound="ConnectionsFactory")


class ConnectionsFactory:
    """This class is a thin wrapper around the underlying REST and WebSocket third-party library.

    The purpose of the class is to isolate the general `web_assistant` infrastructure from the underlying library
    (in this case, `aiohttp`) to enable dependency change with minimal refactoring of the code.

    Note: One future possibility is to enable injection of a specific connection factory implementation in the
    `WebAssistantsFactory` to accommodate cases such as Bittrex that uses a specific WebSocket technology requiring
    a separate third-party library. In that case, a factory can be created that returns `RESTConnection`s using
    `aiohttp` and `WSConnection`s using `signalr_aio`.
    """
    _instance: ConnectionsFactoryT | None = None
    _ws_independent_session: aiohttp.ClientSession | None = None
    _shared_client: aiohttp.ClientSession | None = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def get_rest_connection(self) -> RESTConnection:
        """
        Get a REST connection using a shared aiohttp.ClientSession.
        """
        client = await self._get_shared_client()
        return RESTConnection(aiohttp_client_session=client)

    async def get_ws_connection(self) -> WSConnection:
        """
        Get a WebSocket connection using either the independent session (if set)
        or the shared client.
        """
        client = self._ws_independent_session or await self._get_shared_client()
        return WSConnection(aiohttp_client_session=client)

    async def _get_shared_client(self) -> aiohttp.ClientSession:
        """
        Lazily create a shared aiohttp.ClientSession if not already available.
        Supports HTTP proxy via client configuration or environment variables.
        """
        if self._shared_client is None:
            # Try to get proxy settings from client config first
            use_proxy = False
            proxy_from_config = False

            try:
                # Import here to avoid circular dependency
                from hummingbot.client.config.config_helpers import load_client_config_map_from_file  # noqa: PLC0415
                client_config = load_client_config_map_from_file()

                if hasattr(client_config, "http_proxy_enabled") and client_config.http_proxy_enabled:
                    use_proxy = True
                    proxy_from_config = True

                    # Set environment variables from config if proxy URLs are provided
                    if client_config.http_proxy_url:
                        os.environ["HTTP_PROXY"] = str(client_config.http_proxy_url)
                    if client_config.https_proxy_url:
                        os.environ["HTTPS_PROXY"] = str(client_config.https_proxy_url)
                    if client_config.no_proxy_hosts:
                        os.environ["NO_PROXY"] = str(client_config.no_proxy_hosts)
            except Exception:  # noqa: S110
                # If config loading fails, fall back to environment variable
                pass

            # Fall back to environment variable if config doesn't enable proxy
            if not proxy_from_config:
                use_proxy = os.getenv("HUMMINGBOT_USE_PROXY", "").lower() in ("true", "1", "yes")

            if use_proxy:
                # Enable proxy support by setting trust_env=True
                # This will make aiohttp respect HTTP_PROXY, HTTPS_PROXY, and NO_PROXY env vars
                self._shared_client = aiohttp.ClientSession(trust_env=True)
            else:
                # Default behavior - no proxy support
                self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def close(self) -> None:
        """
        Close any open aiohttp.ClientSession instances.
        """
        if self._shared_client is not None:
            await self._shared_client.close()
            self._shared_client = None
        if self._ws_independent_session is not None:
            await self._ws_independent_session.close()
            self._ws_independent_session = None

    async def __aenter__(self) -> ConnectionsFactoryT:
        """
        Enter the async context manager.
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the async context manager by closing client sessions.
        """
        await self.close()
