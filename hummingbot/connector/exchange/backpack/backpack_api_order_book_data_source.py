"""Backpack API Order Book Data Source.
Handles public WebSocket streams for order book and trade data.
"""

import asyncio
import time
from typing import TYPE_CHECKING, Any

from hummingbot.connector.exchange.backpack import (
    backpack_constants as CONSTANTS,
    backpack_utils as utils,
    backpack_web_utils as web_utils,
)
from hummingbot.connector.exchange.backpack.backpack_order_book import BackpackOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.backpack.backpack_exchange import BackpackExchange


class BackpackAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """Backpack API Order Book Data Source for public market data.

    Follows the parent class pattern for WebSocket message routing.
    """

    HEARTBEAT_TIME_INTERVAL = 30.0
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: HummingbotLogger | None = None

    def __init__(
        self,
        trading_pairs: list[str],
        connector: "BackpackExchange",
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """Initialize the order book data source.

        Args:
            trading_pairs: List of trading pairs to track
            connector: Exchange connector instance
            api_factory: Web assistants factory
            domain: Exchange domain
        """
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(
        self,
        trading_pairs: list[str],
        domain: str | None = None,
    ) -> dict[str, float]:
        """Get last traded prices for the specified trading pairs.

        Args:
            trading_pairs: List of trading pairs
            domain: Exchange domain (optional)

        Returns:
            Dictionary mapping trading pair to last price
        """
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> dict[str, Any]:
        """Retrieve order book snapshot from REST API.

        Args:
            trading_pair: Trading pair to get snapshot for

        Returns:
            Order book snapshot data
        """
        exchange_symbol = utils.convert_to_exchange_trading_pair(trading_pair)
        params = {
            "symbol": exchange_symbol,
            "limit": CONSTANTS.ORDER_BOOK_DEPTH_LIMIT,  # Get deep order book
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.DEPTH_URL, self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_URL,
        )

        return data if isinstance(data, dict) else {}

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """Get order book snapshot message.

        Args:
            trading_pair: Trading pair

        Returns:
            Order book snapshot message
        """
        snapshot_data = await self._request_order_book_snapshot(trading_pair)

        # Convert Backpack format [["price", "amount"], ...] to numeric format
        if "bids" not in snapshot_data or "asks" not in snapshot_data:
            raise ValueError(f"Missing bids or asks in snapshot: {snapshot_data}")
        raw_bids = snapshot_data["bids"]
        raw_asks = snapshot_data["asks"]

        formatted_bids = [[float(bid[0]), float(bid[1])] for bid in raw_bids if len(bid) >= 2]
        formatted_asks = [[float(ask[0]), float(ask[1])] for ask in raw_asks if len(ask) >= 2]

        snapshot_payload = {
            "trading_pair": trading_pair,
            "lastUpdateId": int(snapshot_data.get("lastUpdateId", 0)),
            "bids": formatted_bids,
            "asks": formatted_asks,
        }

        return BackpackOrderBook.snapshot_message_from_exchange(
            snapshot_payload,
            timestamp=time.time(),
        )

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """Create and connect WebSocket assistant for public streams.

        Implements parent class pattern.

        Returns:
            Connected WebSocket assistant
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.ws_public_url(self._domain),
            ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL,
            # Public streams should have regular updates, but using ping_timeout for consistency
        )
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        """Subscribe to WebSocket channels for order book and trade data.

        Implements parent class pattern.

        Args:
            ws: WebSocket assistant
        """
        try:
            # Prepare subscription parameters
            subscriptions = []

            for trading_pair in self._trading_pairs:
                exchange_symbol = utils.convert_to_exchange_trading_pair(trading_pair)

                # Subscribe to order book depth and trade updates
                subscriptions.extend([
                    f"{CONSTANTS.WS_DEPTH_CHANNEL}.{exchange_symbol}",
                    f"{CONSTANTS.WS_TRADES_CHANNEL}.{exchange_symbol}",
                ])

            # Send subscription request
            subscription_payload = {
                "method": "SUBSCRIBE",
                "params": subscriptions,
            }

            subscribe_request = WSJSONRequest(payload=subscription_payload)
            await ws.send(subscribe_request)

            self.logger().info(f"Subscribed to public channels: {subscriptions}")

        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book and trade streams",
                exc_info=True,
            )
            raise

    def _channel_originating_message(self, event_message: dict[str, Any]) -> str:
        """Identifies the channel for a particular event message.

        Implements parent class pattern for message routing.

        Args:
            event_message: The event received through the websocket connection

        Returns:
            The message channel key for routing
        """
        channel = ""

        # Skip subscription confirmations
        if event_message.get("result") == "success" or event_message.get("type") == "subscribed":
            return channel

        stream = event_message.get("stream", "")

        if stream.startswith("depth."):
            channel = self._diff_messages_queue_key
        elif stream.startswith("trade."):
            channel = self._trade_messages_queue_key
        else:
            # Fallback: Check data.e field for event type
            if "data" in event_message:
                data = event_message["data"]
                event_type = data.get("e", "")

                if event_type == "depth":
                    channel = self._diff_messages_queue_key
                elif event_type == "trade":
                    channel = self._trade_messages_queue_key
                # Also check for actual content as last resort
                elif "a" in data or "b" in data:  # 'a' for asks, 'b' for bids
                    channel = self._diff_messages_queue_key
                elif "p" in data and "q" in data:  # 'p' for price, 'q' for quantity
                    channel = self._trade_messages_queue_key

        return channel

    async def _parse_order_book_diff_message(
        self,
        raw_message: dict[str, Any],
        message_queue: asyncio.Queue,
    ):
        """Parse order book diff message and add to queue.

        Args:
            raw_message: Raw WebSocket message
            message_queue: Queue to add parsed message to
        """
        try:
            data = raw_message.get("data", {})

            # Backpack uses abbreviated field names in WebSocket:
            # 's' for symbol, 'a' for asks, 'b' for bids, 'T' for timestamp, 'u' for update_id
            exchange_symbol = data.get("s") or data.get("symbol")

            if exchange_symbol:
                trading_pair = utils.convert_from_exchange_trading_pair(exchange_symbol)

                if trading_pair in self._trading_pairs:
                    timestamp = data.get("T", data.get("E", time.time() * 1_000_000)) / 1_000_000
                    diff_payload = {
                        "trading_pair": trading_pair,
                        "b": data.get("b", data.get("bids", [])),
                        "a": data.get("a", data.get("asks", [])),
                        "U": data.get("U", data.get("u", 0)),
                        "u": data.get("u", data.get("U", 0)),
                    }

                    order_book_message = BackpackOrderBook.diff_message_from_exchange(
                        diff_payload,
                        timestamp=timestamp,
                    )

                    await message_queue.put(order_book_message)

        except Exception:
            self.logger().error(
                f"Error parsing order book diff message: {raw_message}",
                exc_info=True,
            )

    async def _parse_trade_message(
        self,
        raw_message: dict[str, Any],
        message_queue: asyncio.Queue,
    ):
        """Parse trade message and add to queue.

        Args:
            raw_message: Raw WebSocket message
            message_queue: Queue to add parsed message to
        """
        try:
            data = raw_message.get("data", {})

            # Backpack uses abbreviated field names in WebSocket:
            # 's' for symbol, 'p' for price, 'q' for quantity, 't' for trade_id
            exchange_symbol = data.get("s") or data.get("symbol")

            if exchange_symbol:
                trading_pair = utils.convert_from_exchange_trading_pair(exchange_symbol)

                if trading_pair in self._trading_pairs:
                    trade_payload = {
                        "trading_pair": trading_pair,
                        "t": data.get("t", data.get("tradeId")),
                        "p": data.get("p", data.get("price", 0)),
                        "q": data.get("q", data.get("quantity", 0)),
                        "m": data.get("m", False),
                        "E": data.get("E", data.get("T", time.time() * 1_000_000)),
                    }

                    if trade_payload["t"] is None:
                        return

                    trade_message = BackpackOrderBook.trade_message_from_exchange(
                        trade_payload,
                        metadata={"trading_pair": trading_pair},
                    )

                    await message_queue.put(trade_message)

        except Exception:
            self.logger().error(
                f"Error parsing trade message: {raw_message}",
                exc_info=True,
            )

    async def _parse_order_book_snapshot_message(
        self,
        raw_message: dict[str, Any],
        message_queue: asyncio.Queue,
    ):
        """Parse order book snapshot message.

        Args:
            raw_message: Raw snapshot data
            message_queue: Queue to add parsed message to
        """
        # In Backpack, snapshots come from REST API, not WebSocket
        # This method handles snapshots placed in queue by _request_order_book_snapshots
        try:
            # If this is a REST snapshot, it will have the structure we expect
            # These are created by _request_order_book_snapshots with guaranteed fields
            if "trading_pair" not in raw_message:
                self.logger().error(f"Missing trading_pair in snapshot: {raw_message}")
                return

            trading_pair = raw_message["trading_pair"]

            # These fields are guaranteed from our own snapshot creation
            snapshot_message = OrderBookMessage(
                message_type=OrderBookMessageType.SNAPSHOT,
                content={
                    "trading_pair": trading_pair,
                    "update_id": raw_message["update_id"],
                    "bids": raw_message["bids"],
                    "asks": raw_message["asks"],
                },
                timestamp=time.time(),
            )

            await message_queue.put(snapshot_message)

        except Exception:
            self.logger().error(
                f"Error parsing snapshot message: {raw_message}",
                exc_info=True,
            )

    async def fetch_trading_pairs(self, domain: str | None = None) -> list[str]:
        """Fetch available trading pairs from the exchange.

        Args:
            domain: Exchange domain (optional)

        Returns:
            List of available trading pairs in Hummingbot format
        """
        try:
            rest_assistant = await self._api_factory.get_rest_assistant()
            data = await rest_assistant.execute_request(
                url=web_utils.public_rest_url(CONSTANTS.EXCHANGE_INFO_URL, domain or self._domain),
                method=RESTMethod.GET,
                throttler_limit_id=CONSTANTS.EXCHANGE_INFO_URL,
            )

            trading_pairs = []
            markets = web_utils.normalize_response_to_list(data)
            for symbol_data in markets:
                if not isinstance(symbol_data, dict):
                    continue
                market_type = symbol_data.get("marketType")
                if market_type and market_type != "SPOT":
                    continue
                status = symbol_data.get("status")
                if status and status != "TRADING":
                    continue
                exchange_symbol = symbol_data.get("symbol")
                if exchange_symbol:
                    trading_pair = utils.convert_from_exchange_trading_pair(exchange_symbol)
                    trading_pairs.append(trading_pair)

            return trading_pairs

        except Exception:
            self.logger().error("Error fetching trading pairs", exc_info=True)
            return []

    async def get_order_book_data(self, trading_pair: str) -> dict[str, Any]:
        """Get order book data for a specific trading pair.

        Args:
            trading_pair: Trading pair to get order book for

        Returns:
            Order book data
        """
        return await self._request_order_book_snapshot(trading_pair)
