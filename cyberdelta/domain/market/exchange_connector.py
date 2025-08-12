"""Exchange connection management.

This module handles connections to exchange APIs including
initialization, lifecycle management, and configuration.
"""

from __future__ import annotations

import asyncio

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.models.service_args.market_data import GetMarketsArgs
from cyberdelta.config.models import AppSettings, ExchangeSpecificConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName


logger = get_logger(__name__)


class ExchangeConnector:
    """Exchange connection manager.

    This class handles:
    - Exchange API client connections
    - Connection initialization and testing
    - Lifecycle management (start/stop)
    - Enabled exchange tracking

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL timeouts from AppSettings configuration
    - NO assumptions about exchange availability
    - Explicit error handling with context
    - NO hardcoded connection parameters
    """

    def __init__(self, config: AppSettings, api_clients: dict[str, ExchangeAPI]) -> None:
        """Initialize exchange connector with configuration and clients.

        Args:
            config: Application settings containing exchange configuration
            api_clients: Dict of exchange API clients (keyed by string for config compat)
        """
        self.config = config
        self._api_clients = api_clients

        # Track enabled exchanges for iteration
        self._enabled_exchanges: dict[str, ExchangeSpecificConfig] = {
            name: exchange_config
            for name, exchange_config in self.config.exchanges.items()
            if exchange_config.enabled
        }

        # Track actual connection states - initialize all to False
        self._connection_states: dict[str, bool] = dict.fromkeys(self._enabled_exchanges, False)

        logger.debug(
            "exchange_connector_initialized",
            enabled_exchanges=list(self._enabled_exchanges.keys()),
            api_client_count=len(api_clients),
        )

    async def start_connections(self) -> None:
        """Start connections to all enabled exchanges.

        Initializes connections to all enabled exchanges and tests
        their availability.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured timeouts for initialization
        - NO assumptions about exchange availability
        - Explicit error handling with context
        """
        logger.info("exchange_connections_starting")

        # Initialize connections to enabled exchanges
        for exchange_name, exchange_config in self._enabled_exchanges.items():
            try:
                api_client = self._api_clients.get(exchange_name)
                if not api_client:
                    logger.warning(
                        "exchange_api_client_missing",
                        exchange=exchange_name,
                        reason="not_in_api_clients",
                    )
                    continue

                # Initialize with configured timeout
                timeout = exchange_config.request_timeout_seconds
                await asyncio.wait_for(
                    self._initialize_exchange_connection(api_client, exchange_name), timeout=timeout
                )

                logger.info(
                    "exchange_connection_initialized", exchange=exchange_name, timeout_used=timeout
                )

            except Exception as e:
                logger.exception(
                    "exchange_initialization_failed", exchange=exchange_name, error=str(e)
                )
                # Continue with other exchanges - NO silent failures

        logger.info("exchange_connections_started")

    async def stop_connections(self) -> None:
        """Stop all exchange connections.

        Cleanly shuts down all exchange connections.

        IMPORTANT: Following CODING_STANDARDS.md:
        - Explicit cleanup sequence
        - NO assumptions about shutdown timing
        """
        logger.info("exchange_connections_stopping")

        # Close actual connections for each exchange
        for exchange_name in self._enabled_exchanges:
            api_client = self._api_clients.get(exchange_name)
            if api_client:
                try:
                    # Close WebSocket connection if exists
                    await api_client.close_websocket()
                    # Close HTTP session and cleanup
                    await api_client.close()
                    logger.info(
                        "exchange_connection_closed",
                        exchange=exchange_name,
                        message=f"Successfully closed connections for {exchange_name}",
                    )
                except (ValueError, TypeError, KeyError, AttributeError, OSError) as e:
                    logger.warning(
                        "exchange_connection_close_error",
                        exchange=exchange_name,
                        error=str(e),
                        message=f"Error closing connections for {exchange_name}",
                    )

            # Mark connection as closed
            self._connection_states[exchange_name] = False

        logger.debug(
            "exchange_connections_marked_closed", exchanges=list(self._connection_states.keys())
        )

        logger.info("exchange_connections_stopped")

    async def _initialize_exchange_connection(
        self, api_client: ExchangeAPI, exchange_name: str
    ) -> None:
        """Initialize connection to exchange.

        Args:
            api_client: Exchange API client instance
            exchange_name: Name of the exchange (string for logging compatibility)

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about API client interface
        - Explicit connection validation
        """
        # First, establish WebSocket connection if available
        try:
            # Connect WebSocket for real-time data
            await api_client.connect_websocket()
            logger.info(
                "exchange_websocket_connected",
                exchange=exchange_name,
                message=f"WebSocket connection established for {exchange_name}",
            )
        except (ValueError, TypeError, KeyError, AttributeError, OSError) as ws_error:
            # WebSocket connection is optional - some operations work without it
            logger.warning(
                "exchange_websocket_connection_failed",
                exchange=exchange_name,
                error=str(ws_error),
                message=(
                    f"WebSocket connection failed for {exchange_name}, continuing with REST only"
                ),
            )

        # Test REST API connection by getting markets
        try:
            await api_client.get_markets(GetMarketsArgs())
            self._connection_states[exchange_name] = True
            logger.debug("exchange_rest_api_tested", exchange=exchange_name)
        except (TimeoutError, ConnectionError, OSError) as e:
            logger.warning("exchange_rest_api_test_failed", exchange=exchange_name, error=str(e))
            self._connection_states[exchange_name] = False
            # Don't re-raise - continue with other exchanges
        except Exception as e:
            logger.exception(
                "exchange_connection_unexpected_error", exchange=exchange_name, error=str(e)
            )
            self._connection_states[exchange_name] = False
            # Don't re-raise - let other exchanges try to connect

    def get_enabled_exchanges(self) -> dict[str, ExchangeSpecificConfig]:
        """Get dictionary of enabled exchanges and their configurations.

        Returns:
            Dictionary mapping exchange names to their configurations
        """
        return self._enabled_exchanges.copy()

    def get_api_client(self, exchange_name: ExchangeName) -> ExchangeAPI | None:
        """Get API client for specific exchange.

        Args:
            exchange_name: Name of the exchange

        Returns:
            API client if available, None otherwise
        """
        return self._api_clients.get(exchange_name.value)

    def is_exchange_enabled(self, exchange_name: ExchangeName) -> bool:
        """Check if exchange is enabled.

        Args:
            exchange_name: Name of the exchange

        Returns:
            True if exchange is enabled, False otherwise
        """
        return exchange_name.value in self._enabled_exchanges

    def is_connected(self) -> bool:
        """Check if connected to at least one exchange.

        Returns:
            True if connected to at least one exchange, False otherwise
        """
        # Check both REST API connection state and WebSocket connection if available
        for exchange_name, is_rest_connected in self._connection_states.items():
            api_client = self._api_clients.get(exchange_name)
            # Return True if either REST or WebSocket connection is active
            if is_rest_connected or (api_client and api_client.is_connected):
                return True
        return False

    def get_connection_status(self, exchange_name: ExchangeName) -> bool:
        """Get connection status for a specific exchange.

        Args:
            exchange_name: Name of the exchange

        Returns:
            True if connected, False otherwise
        """
        return self._connection_states.get(exchange_name.value, False)
