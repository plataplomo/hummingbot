"""
Backpack API User Stream Data Source.
Handles private WebSocket streams for account updates.
"""

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS, backpack_web_utils as web_utils
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth


class BackpackAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Backpack API User Stream Data Source for private account data.

    Handles:
    - Account balance updates
    - Order status updates
    - Trade fill notifications
    - Position updates
    """

    HEARTBEAT_TIME_INTERVAL = 30.0
    LISTEN_KEY_KEEP_ALIVE_INTERVAL = 1800  # 30 minutes

    _logger: Optional[HummingbotLogger] = None

    def __init__(
        self,
        auth: 'BackpackAuth',
        trading_pairs: list,
        connector,
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN
    ):
        """
        Initialize the user stream data source.

        Args:
            auth: Authentication instance
            trading_pairs: List of trading pairs
            connector: Exchange connector instance
            api_factory: Web assistants factory
            domain: Exchange domain
        """
        super().__init__()
        self._auth = auth
        self._trading_pairs = trading_pairs
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._last_recv_time = 0

    @property
    def last_recv_time(self) -> float:
        """Get timestamp of last received message."""
        return self._last_recv_time

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Create and connect WebSocket assistant for private streams.

        Returns:
            Connected and authenticated WebSocket assistant
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.ws_private_url(self._domain),
            message_timeout=CONSTANTS.REQUEST_TIMEOUT
        )

        # Authenticate the WebSocket connection
        await self._authenticate_websocket(ws)

        return ws

    async def _authenticate_websocket(self, ws: WSAssistant) -> bool:
        """
        Authenticate the WebSocket connection.

        Args:
            ws: WebSocket assistant to authenticate

        Returns:
            True if authentication successful, False otherwise
        """
        try:
            # Get authentication message from auth instance
            auth_message = self._auth.get_ws_auth_message()
            auth_request = WSJSONRequest(payload=auth_message)

            # Send authentication message
            await ws.send(auth_request)

            # Wait for authentication response
            auth_response = await asyncio.wait_for(
                ws.receive(),
                timeout=CONSTANTS.REQUEST_TIMEOUT
            )

            # Parse response
            response_data = json.loads(auth_response.data)

            if response_data.get("type") == "auth":
                if response_data.get("success"):
                    self.logger().info("WebSocket authentication successful")
                    return True
                else:
                    error_msg = response_data.get("error", "Unknown authentication error")
                    self.logger().error(f"WebSocket authentication failed: {error_msg}")
                    return False

            # If we don't get an auth response, assume success for now
            self.logger().info("WebSocket connected, assuming authentication successful")
            return True

        except Exception:
            self.logger().error("Error during WebSocket authentication", exc_info=True)
            return False

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribe to private WebSocket channels.

        Args:
            ws: Authenticated WebSocket assistant
        """
        try:
            # Subscribe to account channels
            subscriptions = [
                CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL,
                CONSTANTS.WS_ACCOUNT_BALANCES_CHANNEL,
                CONSTANTS.WS_ACCOUNT_POSITIONS_CHANNEL,
                CONSTANTS.WS_ACCOUNT_TRANSACTIONS_CHANNEL
            ]

            subscription_payload = {
                "method": "subscribe",
                "params": {
                    "subscriptions": subscriptions
                }
            }

            subscribe_request = WSJSONRequest(payload=subscription_payload)
            await ws.send(subscribe_request)

            self.logger().info(f"Subscribed to private channels: {subscriptions}")

        except Exception:
            self.logger().error("Error subscribing to private channels", exc_info=True)
            raise

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Listen for user stream messages and add them to the output queue.

        Args:
            output: Queue to add messages to
        """
        while True:
            try:
                ws = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)

                async for ws_response in ws.iter_messages():
                    try:
                        self._last_recv_time = time.time()
                        data = json.loads(ws_response.data)

                        # Process different message types
                        await self._process_user_stream_message(data, output)

                    except Exception:
                        self.logger().error(
                            f"Error processing user stream message: {ws_response.data}",
                            exc_info=True
                        )

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                    exc_info=True
                )
                await asyncio.sleep(30.0)

    async def _process_user_stream_message(
        self,
        message: Dict[str, Any],
        output: asyncio.Queue
    ):
        """
        Process incoming user stream message and route to appropriate handler.

        Args:
            message: Raw WebSocket message
            output: Output queue for processed messages
        """
        try:
            message_type = message.get("type")

            if message_type == CONSTANTS.WS_MESSAGE_TYPE_ORDER_UPDATE:
                await self._process_order_update(message, output)
            elif message_type == CONSTANTS.WS_MESSAGE_TYPE_BALANCE_UPDATE:
                await self._process_balance_update(message, output)
            elif message_type == CONSTANTS.WS_MESSAGE_TYPE_TRADE_UPDATE:
                await self._process_trade_update(message, output)
            else:
                # Log unknown message types for debugging
                self.logger().debug(f"Unknown user stream message type: {message_type}")

        except Exception:
            self.logger().error(
                f"Error processing user stream message: {message}",
                exc_info=True
            )

    async def _process_order_update(
        self,
        message: Dict[str, Any],
        output: asyncio.Queue
    ):
        """
        Process order update message.

        Args:
            message: Order update message
            output: Output queue
        """
        try:
            # Add message type identifier
            message["message_type"] = "order_update"
            output.put_nowait(message)

        except Exception:
            self.logger().error(
                f"Error processing order update: {message}",
                exc_info=True
            )

    async def _process_balance_update(
        self,
        message: Dict[str, Any],
        output: asyncio.Queue
    ):
        """
        Process balance update message.

        Args:
            message: Balance update message
            output: Output queue
        """
        try:
            # Add message type identifier
            message["message_type"] = "balance_update"
            output.put_nowait(message)

        except Exception:
            self.logger().error(
                f"Error processing balance update: {message}",
                exc_info=True
            )

    async def _process_trade_update(
        self,
        message: Dict[str, Any],
        output: asyncio.Queue
    ):
        """
        Process trade update message.

        Args:
            message: Trade update message
            output: Output queue
        """
        try:
            # Add message type identifier
            message["message_type"] = "trade_update"
            output.put_nowait(message)

        except Exception:
            self.logger().error(
                f"Error processing trade update: {message}",
                exc_info=True
            )

    async def _iter_user_event_queue(self) -> asyncio.Queue:
        """
        Iterate over user events from the WebSocket stream.

        Returns:
            Queue of user events
        """
        event_queue = asyncio.Queue()
        asyncio.create_task(self.listen_for_user_stream(event_queue))

        while True:
            try:
                yield await event_queue.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Error in user event queue iterator", exc_info=True)
                continue

    async def get_account_balances(self) -> Dict[str, Any]:
        """
        Get current account balances from REST API.

        Returns:
            Account balances data
        """
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            data = await rest_assistant.execute_request(
                url=web_utils.private_rest_url(CONSTANTS.BALANCES_URL, self._domain),
                method="GET",
                throttler_limit_id=CONSTANTS.BALANCES_URL,
                headers=await self._auth._generate_auth_headers("GET", f"/{CONSTANTS.BALANCES_URL}")
            )
            return data

        except Exception:
            self.logger().error("Error fetching account balances", exc_info=True)
            return {}

    async def get_open_orders(self) -> Dict[str, Any]:
        """
        Get current open orders from REST API.

        Returns:
            Open orders data
        """
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            data = await rest_assistant.execute_request(
                url=web_utils.private_rest_url(CONSTANTS.OPEN_ORDERS_URL, self._domain),
                method="GET",
                throttler_limit_id=CONSTANTS.OPEN_ORDERS_URL,
                headers=await self._auth._generate_auth_headers("GET", f"/{CONSTANTS.OPEN_ORDERS_URL}")
            )
            return data

        except Exception:
            self.logger().error("Error fetching open orders", exc_info=True)
            return {}
