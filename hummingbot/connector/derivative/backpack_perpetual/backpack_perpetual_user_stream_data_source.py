"""
User stream data source for Backpack Perpetual Exchange.
Handles private WebSocket streams for account updates, orders, positions, and funding.
"""

import asyncio
import json
from typing import TYPE_CHECKING, Any, Dict, Optional

from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_utils as utils,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_auth import BackpackPerpetualAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import (
        BackpackPerpetualDerivative,
    )

_logger: Optional[HummingbotLogger] = None


class BackpackPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Data source for Backpack Perpetual user stream updates.
    Manages private WebSocket connections for account data.
    """

    def __init__(
        self,
        auth: BackpackPerpetualAuth,
        connector: "BackpackPerpetualDerivative",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """
        Initialize the user stream data source.

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
        self._ws_assistant: Optional[WSAssistant] = None
        self._last_recv_time: float = 0
        self._message_id_counter = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _logger
        if _logger is None:
            _logger = HummingbotLogger(__name__)
        return _logger

    @property
    def last_recv_time(self) -> float:
        """
        Returns the timestamp of the last received message.

        Returns:
            Timestamp in seconds
        """
        return self._last_recv_time

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Listen to user stream including position updates and funding payments.

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
                    self._last_recv_time = self._time()

                    data = json.loads(ws_response.data)

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
        """
        Create and return a WebSocket connection for private streams.

        Returns:
            Connected WSAssistant instance
        """
        self._ws_assistant = await self._api_factory.get_ws_assistant()
        await self._ws_assistant.connect(
            ws_url=CONSTANTS.WS_PRIVATE_URL,
            message_timeout=CONSTANTS.WS_MESSAGE_TIMEOUT,
        )
        return self._ws_assistant

    async def _authenticate_websocket(self, ws: WSAssistant) -> bool:
        """
        Authenticate WebSocket connection.

        Args:
            ws: WebSocket assistant to authenticate

        Returns:
            True if authentication successful
        """
        try:
            # Send authentication message
            auth_msg = self._auth.get_ws_auth_message()
            await ws.send(json.dumps(auth_msg))

            # Wait for auth response
            auth_response_timeout = 10.0
            auth_response = await asyncio.wait_for(
                ws.receive(),
                timeout=auth_response_timeout
            )

            response_data = json.loads(auth_response)

            # Check auth success
            if response_data.get("result") == "success" or response_data.get("type") == "authenticated":
                self.logger().info("WebSocket authentication successful")
                return True
            else:
                self.logger().error(f"WebSocket authentication failed: {response_data}")
                return False

        except asyncio.TimeoutError:
            self.logger().error("WebSocket authentication timeout")
            return False
        except Exception:
            self.logger().exception("Error during WebSocket authentication")
            return False

    async def _subscribe_to_private_channels(self, ws: WSAssistant):
        """
        Subscribe to private WebSocket channels.

        Args:
            ws: WebSocket assistant
        """
        # Subscribe to all required private channels
        # Backpack uses the format: {"method": "SUBSCRIBE", "params": ["stream1", "stream2", ...]}
        channels = [
            CONSTANTS.WS_ACCOUNT_ORDERS_CHANNEL,      # Order updates
            CONSTANTS.WS_ACCOUNT_BALANCES_CHANNEL,    # Balance updates
            CONSTANTS.WS_ACCOUNT_FILLS_CHANNEL,       # Trade fills
            CONSTANTS.WS_ACCOUNT_POSITIONS_CHANNEL,   # Position updates (perpetual-specific)
            CONSTANTS.WS_FUNDING_RATE_CHANNEL,        # Funding payments (perpetual-specific)
            CONSTANTS.WS_LIQUIDATION_CHANNEL,         # Liquidation warnings (perpetual-specific)
        ]

        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": channels
        }

        await ws.send(json.dumps(subscribe_msg))
        self.logger().info(f"Subscribed to private channels: {channels}")

    def _process_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process WebSocket events and route them appropriately.

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
            if "order" in event_type.lower():
                return self._process_order_event({"stream": event_type, "data": inner_data} if "stream" in event else inner_data)
            elif "balance" in event_type.lower():
                return self._process_balance_event({"stream": event_type, "data": inner_data} if "stream" in event else inner_data)
            elif "position" in event_type.lower():
                return self._process_position_event({"stream": event_type, "data": inner_data} if "stream" in event else inner_data)
            elif "fill" in event_type.lower() or "trade" in event_type.lower():
                return self._process_fill_event({"stream": event_type, "data": inner_data} if "stream" in event else inner_data)
            elif "funding" in event_type.lower():
                return self._process_funding_event({"stream": event_type, "data": inner_data} if "stream" in event else inner_data)
            elif "liquidation" in event_type.lower():
                return self._process_liquidation_event({"stream": event_type, "data": inner_data} if "stream" in event else inner_data)
            else:
                # Unknown event type - log for debugging
                self.logger().debug(f"Unknown event type: {event_type}")
                return None

        except Exception:
            self.logger().exception(f"Error processing event: {event}")
            return None

    def _process_order_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process order update event.

        Args:
            event: Raw order event

        Returns:
            Processed order event
        """
        data = event.get("data", event)

        # Convert symbol format
        if "symbol" in data:
            trading_pair = utils.convert_from_exchange_trading_pair(data["symbol"])
            if trading_pair:
                data["trading_pair"] = trading_pair

        return {
            "type": "order",
            "data": data
        }

    def _process_balance_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process balance update event.

        Args:
            event: Raw balance event

        Returns:
            Processed balance event
        """
        data = event.get("data", event)

        return {
            "type": "balance",
            "data": data
        }

    def _process_position_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process position update event (perpetual-specific).

        Args:
            event: Raw position event

        Returns:
            Processed position event
        """
        data = event.get("data", event)

        # Convert symbol format
        if "symbol" in data:
            trading_pair = utils.convert_from_exchange_trading_pair(data["symbol"])
            if trading_pair:
                data["trading_pair"] = trading_pair

        return {
            "type": "position",
            "data": data
        }

    def _process_fill_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process trade fill event.

        Args:
            event: Raw fill event

        Returns:
            Processed fill event
        """
        data = event.get("data", event)

        # Convert symbol format
        if "symbol" in data:
            trading_pair = utils.convert_from_exchange_trading_pair(data["symbol"])
            if trading_pair:
                data["trading_pair"] = trading_pair

        return {
            "type": "fill",
            "data": data
        }

    def _process_funding_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process funding payment event (perpetual-specific).

        Args:
            event: Raw funding event

        Returns:
            Processed funding event
        """
        data = event.get("data", event)

        # Convert symbol format
        if "symbol" in data:
            trading_pair = utils.convert_from_exchange_trading_pair(data["symbol"])
            if trading_pair:
                data["trading_pair"] = trading_pair

        return {
            "type": "funding",
            "data": data
        }

    def _process_liquidation_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process liquidation warning event (perpetual-specific).

        Args:
            event: Raw liquidation event

        Returns:
            Processed liquidation event
        """
        data = event.get("data", event)

        # Convert symbol format
        if "symbol" in data:
            trading_pair = utils.convert_from_exchange_trading_pair(data["symbol"])
            if trading_pair:
                data["trading_pair"] = trading_pair

        # Log liquidation warnings prominently
        self.logger().warning(
            f"LIQUIDATION WARNING for {data.get('trading_pair', data.get('symbol'))}: "
            f"Mark price {data.get('markPrice')} approaching "
            f"liquidation price {data.get('liquidationPrice')}"
        )

        return {
            "type": "liquidation",
            "data": data
        }

    async def _send_ping(self, ws: WSAssistant):
        """
        Send ping message to keep connection alive.

        Args:
            ws: WebSocket assistant
        """
        self._message_id_counter += 1

        ping_msg = {
            "id": self._message_id_counter,
            "method": "ping"
        }

        await ws.send(json.dumps(ping_msg))

    async def _sleep(self, delay: float):
        """
        Sleep for specified delay.

        Args:
            delay: Seconds to sleep
        """
        await asyncio.sleep(delay)

    def _time(self) -> float:
        """
        Get current time in seconds.

        Returns:
            Current timestamp
        """
        import time
        return time.time()

    async def stop(self):
        """
        Stop the user stream data source and clean up connections.
        """
        if self._ws_assistant:
            await self._ws_assistant.disconnect()
            self._ws_assistant = None
