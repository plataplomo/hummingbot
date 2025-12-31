"""Backpack API User Stream Data Source.
Handles private WebSocket streams for account updates.
"""

import asyncio
import json
import time
from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS, backpack_web_utils as web_utils
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.backpack.backpack_auth import BackpackAuth


class BackpackAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """Backpack API User Stream Data Source for private account data.

    Handles:
    - Account balance updates
    - Order status updates
    - Trade fill notifications
    - Position updates
    """

    # Use constants from constants file
    HEARTBEAT_TIME_INTERVAL = CONSTANTS.HEARTBEAT_TIME_INTERVAL
    # Backpack doesn't use listen keys, removed unused LISTEN_KEY_KEEP_ALIVE_INTERVAL

    _logger: HummingbotLogger | None = None

    def __init__(
        self,
        auth: "BackpackAuth",
        trading_pairs: list,
        connector,
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """Initialize the user stream data source.

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
        self._ws_assistant: WSAssistant | None = None

    @property
    def last_recv_time(self) -> float:
        """Get timestamp of last received message."""
        if self._ws_assistant:
            return self._ws_assistant.last_recv_time
        return 0

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """Create and connect WebSocket assistant for private streams.

        Returns:
            Connected and authenticated WebSocket assistant
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.ws_private_url(self._domain),
            ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL,
            # No message_timeout for user streams - we may not receive messages for extended periods
        )

        # Authenticate the WebSocket connection
        await self._authenticate_websocket(ws)

        return ws

    async def _authenticate_websocket(self, ws: WSAssistant) -> bool:
        """Authenticate the WebSocket connection.

        Args:
            ws: WebSocket assistant to authenticate

        Returns:
            True if authentication successful, False otherwise
        """
        # For Backpack, authentication happens with the subscription message
        # We don't send a separate auth message
        # Authentication params are included with each private channel subscription
        self.logger().info("WebSocket connected, authentication will happen with subscription")
        return True

    async def _subscribe_channels(self, ws: WSAssistant):
        """Subscribe to private WebSocket channels.

        Args:
            ws: Authenticated WebSocket assistant
        """
        try:
            # For Backpack, authentication and subscription are combined
            # We need to send auth params with each private channel subscription
            timestamp = str(int(time.time() * 1000))
            window = str(CONSTANTS.AUTH_WINDOW_MS)

            # Build auth payload
            auth_payload = f"instruction={CONSTANTS.WS_AUTH_INSTRUCTION}&timestamp={timestamp}&window={window}"
            signature = self._auth._generate_signature(auth_payload)

            # Subscribe to private channels with authentication
            # Note: account.balanceUpdate doesn't exist in Backpack API
            # Balance updates come through orderUpdate events
            subscriptions = [
                CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL,  # Only documented private stream
            ]

            # Include auth params with the subscription
            subscription_payload = {
                "method": "SUBSCRIBE",
                "params": subscriptions,
                "signature": [
                    self._auth.api_key,
                    signature,
                    timestamp,
                    window,
                ],
            }

            subscribe_request = WSJSONRequest(payload=subscription_payload)
            await ws.send(subscribe_request)

            self.logger().info(f"Sent authenticated subscription to private channels: {subscriptions}")

            # Wait for subscription confirmation
            try:
                sub_response = await asyncio.wait_for(
                    ws.receive(),
                    timeout=5.0,
                )
                if sub_response and sub_response.data:
                    response_data = (
                        json.loads(sub_response.data)
                        if isinstance(sub_response.data, str)
                        else sub_response.data
                    )
                    if "error" in response_data:
                        self.logger().error(f"Subscription error: {response_data['error']}")
                        # Don't raise on error, just log it
                    else:
                        self.logger().info(f"Subscription response: {response_data}")
            except asyncio.TimeoutError:
                # No response for private stream subscriptions is expected behavior
                # Backpack only sends responses for errors, not confirmations
                # Tested empirically: private streams give no response when successful
                self.logger().debug("No response received - subscription successful (expected for private streams)")

        except Exception:
            self.logger().error("Error subscribing to private channels", exc_info=True)
            # Don't raise, try to continue

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """Listen for user stream messages and add them to the output queue.

        Args:
            output: Queue to add messages to
        """
        while True:
            try:
                ws = await self._connected_websocket_assistant()
                self._ws_assistant = ws  # Store reference for last_recv_time
                await self._subscribe_channels(ws)

                async for ws_response in ws.iter_messages():
                    try:
                        if ws_response is None or ws_response.data is None:
                            continue
                        # Handle both string and dict responses
                        data = ws_response.data if isinstance(ws_response.data, dict) else json.loads(ws_response.data)

                        # Process different message types
                        await self._process_user_stream_message(data, output)

                    except Exception:
                        self.logger().error(
                            "Error processing user stream message",
                            exc_info=True,
                        )

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                    exc_info=True,
                )
                await asyncio.sleep(30.0)

    async def _process_user_stream_message(
        self,
        message: dict[str, Any],
        output: asyncio.Queue,
    ):
        """Process incoming user stream message and route to appropriate handler.

        Args:
            message: Raw WebSocket message
            output: Output queue for processed messages
        """
        try:
            # Backpack wraps all stream data in {"stream": "<stream>", "data": "<payload>"}
            # But auth confirmations might come without stream
            stream_name = message.get("stream") if "stream" in message else ""
            data = message.get("data") if "data" in message else message

            # Check for authentication/subscription confirmations
            if message.get("result") == "success" or message.get("type") == "authenticated":
                self.logger().info(f"WebSocket confirmation: {message}")
                return

            # Route based on stream name
            if stream_name:
                if stream_name == CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL or "orderUpdate" in stream_name:
                    # Order updates may contain balance information
                    await self._process_order_update({"stream": stream_name, "data": data}, output)
                    # Check if order update contains balance changes
                    if data and isinstance(data, dict) and "balances" in data:
                        await self._process_balance_update({"stream": stream_name, "data": data}, output)
                elif "fill" in stream_name.lower() or "trade" in stream_name.lower():
                    await self._process_trade_update({"stream": stream_name, "data": data}, output)
            else:
                # Log unknown message types for debugging
                self.logger().debug(f"Unknown user stream: {stream_name}")

        except Exception:
            self.logger().error(
                f"Error processing user stream message: {message}",
                exc_info=True,
            )

    async def _process_order_update(
        self,
        message: dict[str, Any],
        output: asyncio.Queue,
    ):
        """Process order update message.

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
                exc_info=True,
            )

    async def _process_balance_update(
        self,
        message: dict[str, Any],
        output: asyncio.Queue,
    ):
        """Process balance update message.

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
                exc_info=True,
            )

    async def _process_trade_update(
        self,
        message: dict[str, Any],
        output: asyncio.Queue,
    ):
        """Process trade update message.

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
                exc_info=True,
            )

    async def _iter_user_event_queue(self) -> AsyncGenerator[dict[str, Any], None]:
        """Iterate over user events from the WebSocket stream.

        Returns:
            Queue of user events
        """
        event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._listen_task = asyncio.create_task(self.listen_for_user_stream(event_queue))

        try:
            while True:
                # Queue.get() shouldn't raise exceptions in normal operation
                # Moving try-except outside the loop for better performance
                yield await event_queue.get()
        except asyncio.CancelledError:
            raise
        except Exception:
            # This should rarely happen as Queue.get() is very stable
            self.logger().error("Error in user event queue iterator", exc_info=True)
            # Re-raise to properly handle unexpected errors
            raise

    async def get_account_balances(self) -> dict[str, Any]:
        """Get current account balances from REST API.

        Returns:
            Account balances data
        """
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            data = await rest_assistant.execute_request(
                url=web_utils.private_rest_url(CONSTANTS.BALANCES_URL, self._domain),
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.BALANCES_URL,
            )
            return data if isinstance(data, dict) else {}

        except Exception:
            self.logger().error("Error fetching account balances", exc_info=True)
            return {}

    async def get_open_orders(self) -> dict[str, Any]:
        """Get current open orders from REST API.

        Returns:
            Open orders data
        """
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            data = await rest_assistant.execute_request(
                url=web_utils.private_rest_url(CONSTANTS.OPEN_ORDERS_URL, self._domain),
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.OPEN_ORDERS_URL,
            )
            return data if isinstance(data, dict) else {}

        except Exception:
            self.logger().error("Error fetching open orders", exc_info=True)
            return {}
