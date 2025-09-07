import asyncio
import logging
import os

import aiohttp

from hummingbot.core.network_base import NetworkBase
from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.logger import HummingbotLogger


class DataFeedBase(NetworkBase):
    dfb_logger: HummingbotLogger | None = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls.dfb_logger is None:
            cls.dfb_logger = logging.getLogger(__name__)
        return cls.dfb_logger

    def __init__(self):
        super().__init__()
        self._ready_event = asyncio.Event()
        self._shared_client: aiohttp.ClientSession | None = None

    @property
    def name(self):
        raise NotImplementedError

    @property
    def price_dict(self) -> dict[str, float]:
        raise NotImplementedError

    @property
    def health_check_endpoint(self) -> str:
        raise NotImplementedError

    @property
    def ready(self) -> bool:
        return self._ready_event.is_set()

    def get_price(self, asset: str) -> float:
        raise NotImplementedError

    async def _http_client(self) -> aiohttp.ClientSession:
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
                self._shared_client = aiohttp.ClientSession(trust_env=True)
            else:
                # Default behavior - no proxy support
                self._shared_client = aiohttp.ClientSession()
        return self._shared_client

    async def get_ready(self):
        try:
            if not self._ready_event.is_set():
                await self._ready_event.wait()
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error("Unexpected error while waiting for data feed to get ready.",
                                exc_info=True)

    async def start_network(self):
        raise NotImplementedError

    async def stop_network(self):
        raise NotImplementedError

    async def check_network(self) -> NetworkStatus:
        try:
            # Try to get proxy settings from client config first
            use_proxy = False

            try:
                # Import here to avoid circular dependency
                from hummingbot.client.config.config_helpers import load_client_config_map_from_file  # noqa: PLC0415
                client_config = load_client_config_map_from_file()

                if hasattr(client_config, "http_proxy_enabled") and client_config.http_proxy_enabled:
                    use_proxy = True

                    # Set environment variables from config if proxy URLs are provided
                    if client_config.http_proxy_url:
                        os.environ["HTTP_PROXY"] = str(client_config.http_proxy_url)
                    if client_config.https_proxy_url:
                        os.environ["HTTPS_PROXY"] = str(client_config.https_proxy_url)
                    if client_config.no_proxy_hosts:
                        os.environ["NO_PROXY"] = str(client_config.no_proxy_hosts)
            except Exception:
                # If config loading fails, fall back to environment variable
                use_proxy = os.getenv("HUMMINGBOT_USE_PROXY", "").lower() in ("true", "1", "yes")

            # Create session with or without proxy support
            session = aiohttp.ClientSession(trust_env=True) if use_proxy else aiohttp.ClientSession()

            async with session, \
                    session.get(self.health_check_endpoint) as resp:
                status_text = await resp.text()
                if resp.status != 200:
                    raise Exception(f"Data feed {self.name} server is down. Status is {status_text}")
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    def start(self):
        NetworkBase.start(self)

    def stop(self):
        NetworkBase.stop(self)
