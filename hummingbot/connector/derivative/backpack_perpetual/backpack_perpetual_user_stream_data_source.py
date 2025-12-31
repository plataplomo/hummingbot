"""User stream data source for Backpack Perpetual Exchange.
Handles private WebSocket streams for account updates, orders, positions, and funding.
"""

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any

from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_utils as utils,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth import BackpackPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import (
        BackpackPerpetualDerivative,
    )


class BackpackPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    """Data source for Backpack Perpetual user stream updates.
    Manages private WebSocket connections for account data.
    """

    def __init__(
        self,
        auth: BackpackPerpetualAuth,
        connector: "BackpackPerpetualDerivative",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """Initialize the user stream data source.

        Args:
            auth: Authentication handler
            connector: Parent connector instance
            api_factory: Web assistants factory for API connections
            domain: Exchange domain
        """
        super().__init__()
        self._auth = auth
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._ws_assistant: WSAssistant | None = None
        self._last_recv_time: float = 0
        self._message_id_counter = 0

    @property
    def last_recv_time(self) -> float:
        """Returns the timestamp of the last received message.

        Returns:
            Timestamp in seconds
        """
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """Listen to user stream including position updates and funding payments.

        Args:
            output: Queue to put user stream messages
        """
        while True:
            try:
                ws = await self._create_websocket_connection()

                # Authenticate the connection
                await self._authenticate_websocket(ws)

                # Subscribe to private channels
                await self._subscribe_to_private_channels(ws)

                # Listen for messages
                async for ws_response in ws.iter_messages():
                    if ws_response is None:
                        continue
                    self._last_recv_time = self._time()

                    # Handle both string and dict responses
                    data = ws_response.data if isinstance(ws_response.data, dict) else json.loads(ws_response.data)

                    # Process different message types
                    processed_message = self._process_event(data)
                    if processed_message:
                        await output.put(processed_message)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error in user stream WebSocket")
                await self._sleep(5.0)
            finally:
                if self._ws_assistant:
                    await self._ws_assistant.disconnect()
                    self._ws_assistant = None

    async def _create_websocket_connection(self) -> WSAssistant:
        """Create and return a WebSocket connection for private streams.

        Returns:
            Connected WSAssistant instance
        """
        self._ws_assistant = await self._api_factory.get_ws_assistant()
        # Get WebSocket URL using proper domain-based lookup (same URL for public and private)
        ws_url = CONSTANTS.WSS_URLS.get(self._domain, CONSTANTS.WSS_URLS[CONSTANTS.DEFAULT_DOMAIN])
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECTION_LIMIT_ID):
            await self._ws_assistant.connect(
                ws_url=ws_url,
                ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL,
            )
        return self._ws_assistant

    async def _authenticate_websocket(self, ws: WSAssistant) -> bool:
        """Authenticate WebSocket connection.

        Args:
            ws: WebSocket assistant to authenticate

        Returns:
            True if authentication successful
        """
        # For Backpack, authentication happens with the subscription message
        # We don't send a separate auth message
        # Authentication params are included with each private channel subscription
        self.logger().debug("WebSocket connected, authentication will happen with subscription")
        return True

    async def _subscribe_to_private_channels(self, ws: WSAssistant):
        """Subscribe to private WebSocket channels.

        Args:
            ws: WebSocket assistant
        """
        # For Backpack, authentication and subscription are combined
        # We need to send auth params with each private channel subscription
        timestamp = str(int(time.time() * 1000))
        window = str(CONSTANTS.AUTH_WINDOW_MS)

        # Build auth payload
        auth_payload = f"instruction={CONSTANTS.WS_AUTH_INSTRUCTION}&timestamp={timestamp}&window={window}"
        signature = self._auth._generate_signature(auth_payload)

        # Subscribe to private channels with authentication
        # Note: account.balanceUpdate might not exist in perpetual API
        channels = [
            CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL,  # Order updates (includes fills)
            CONSTANTS.WS_ACCOUNT_POSITIONS_CHANNEL,  # Position updates (perpetual-specific)
            # Note: Funding and liquidation events are typically included in position updates
        ]

        # Include auth params with the subscription
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": channels,
            "signature": [
                self._auth.api_key,
                signature,
                timestamp,
                window,
            ],
        }

        subscribe_request = WSJSONRequest(payload=subscribe_msg)
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_PRIVATE_SUBSCRIPTION_LIMIT_ID):
            await ws.send(subscribe_request)
        self.logger().debug(f"Subscribed to private channels: {channels}")

    def _process_event(self, event: dict[str, Any]) -> dict[str, Any] | None:
        """Process WebSocket events and route them appropriately.

        Args:
            event: Raw WebSocket event

        Returns:
            Processed event or None if not relevant
        """
        try:
            # Backpack wraps all stream data in {"stream": "<stream>", "data": "<payload>"}
            # Check if this is a wrapped message
            if "stream" in event and "data" in event:
                event_type = event["stream"]
                inner_data = event["data"]
            else:
                # Direct format (for auth responses or other non-stream messages)
                event_type = event.get("type", "")
                inner_data = event

            # Skip auth responses and subscription confirmations
            if event_type in ["authenticated", "subscribed"] or event.get("result") == "success":
                return None

            # Route based on event type/stream name
            # Create wrapped data for stream events
            wrapped_data = {"stream": event_type, "data": inner_data} if "stream" in event else inner_data

            if "order" in event_type.lower():
                return self._process_order_event(wrapped_data)
            if "balance" in event_type.lower():
                return self._process_balance_event(wrapped_data)
            if "position" in event_type.lower():
                return self._process_position_event(wrapped_data)
            if "fill" in event_type.lower() or "trade" in event_type.lower():
                return self._process_fill_event(wrapped_data)
            if "funding" in event_type.lower():
                return self._process_funding_event(wrapped_data)
            if "liquidation" in event_type.lower():
                return self._process_liquidation_event(wrapped_data)
            # Unknown event type - log for debugging
            self.logger().debug(f"Unknown event type: {event_type}")
            return None

        except Exception:
            self.logger().exception(f"Error processing event: {event}")
            return None

    def _process_order_event(self, event: dict[str, Any]) -> dict[str, Any]:
        """Process order update event.

        Args:
            event: Raw order event

        Returns:
            Processed order event
        """
        data = event.get("data", event)

        # Normalize symbol key and convert to trading pair
        symbol = data.get("symbol") or data.get("s")
        if symbol:
            data["symbol"] = symbol
            trading_pair = utils.convert_from_exchange_trading_pair(symbol)
            if trading_pair:
                data["trading_pair"] = trading_pair

        return {
            "type": "order",
            "data": data,
        }

    def _process_balance_event(self, event: dict[str, Any]) -> dict[str, Any]:
        """Process balance update event.

        Args:
            event: Raw balance event

        Returns:
            Processed balance event
        """
        data = event.get("data", event)

        return {
            "type": "balance",
            "data": data,
        }

    def _process_position_event(self, event: dict[str, Any]) -> dict[str, Any]:
        """Process position update event (perpetual-specific).

        Args:
            event: Raw position event

        Returns:
            Processed position event
        """
        data = event.get("data", event)

        # Normalize symbol key and convert to trading pair
        symbol = data.get("symbol") or data.get("s")
        if symbol:
            data["symbol"] = symbol
            trading_pair = utils.convert_from_exchange_trading_pair(symbol)
            if trading_pair:
                data["trading_pair"] = trading_pair

        return {
            "type": "position",
            "data": data,
        }

    def _process_fill_event(self, event: dict[str, Any]) -> dict[str, Any]:
        """Process trade fill event.

        Args:
            event: Raw fill event

        Returns:
            Processed fill event
        """
        data = event.get("data", event)

        # Normalize symbol key and convert to trading pair
        symbol = data.get("symbol") or data.get("s")
        if symbol:
            data["symbol"] = symbol
            trading_pair = utils.convert_from_exchange_trading_pair(symbol)
            if trading_pair:
                data["trading_pair"] = trading_pair

        return {
            "type": "fill",
            "data": data,
        }

    def _process_funding_event(self, event: dict[str, Any]) -> dict[str, Any]:
        """Process funding payment event (perpetual-specific).

        Args:
            event: Raw funding event

        Returns:
            Processed funding event
        """
        data = event.get("data", event)

        # Normalize symbol key and convert to trading pair
        symbol = data.get("symbol") or data.get("s")
        if symbol:
            data["symbol"] = symbol
            trading_pair = utils.convert_from_exchange_trading_pair(symbol)
            if trading_pair:
                data["trading_pair"] = trading_pair

        return {
            "type": "funding",
            "data": data,
        }

    def _process_liquidation_event(self, event: dict[str, Any]) -> dict[str, Any]:
        """Process liquidation warning event (perpetual-specific).

        Args:
            event: Raw liquidation event

        Returns:
            Processed liquidation event
        """
        data = event.get("data", event)

        # Normalize symbol key and convert to trading pair
        symbol = data.get("symbol") or data.get("s")
        if symbol:
            data["symbol"] = symbol
            trading_pair = utils.convert_from_exchange_trading_pair(symbol)
            if trading_pair:
                data["trading_pair"] = trading_pair

        # Log liquidation warnings prominently
        self.logger().warning(
            f"LIQUIDATION WARNING for {data.get('trading_pair', data.get('symbol'))}: "
            f"Mark price {data.get('markPrice')} approaching "
            f"liquidation price {data.get('liquidationPrice')}",
        )

        return {
            "type": "liquidation",
            "data": data,
        }

    async def _send_ping(self, ws: WSAssistant):
        """Send ping message to keep connection alive.

        Args:
            ws: WebSocket assistant
        """
        self._message_id_counter += 1

        ping_msg = {
            "id": self._message_id_counter,
            "method": "ping",
        }

        ping_request = WSJSONRequest(payload=ping_msg)
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_PRIVATE_SUBSCRIPTION_LIMIT_ID):
            await ws.send(ping_request)

    async def _sleep(self, delay: float):
        """Sleep for specified delay.

        Args:
            delay: Seconds to sleep
        """
        await asyncio.sleep(delay)

    def _time(self) -> float:
        """Get current time in seconds.

        Returns:
            Current timestamp
        """
        return time.time()

    async def stop(self):
        """Stop the user stream data source and clean up connections."""
        if self._ws_assistant:
            await self._ws_assistant.disconnect()
            self._ws_assistant = None
