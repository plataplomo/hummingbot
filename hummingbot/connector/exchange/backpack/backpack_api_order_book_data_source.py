"""Backpack API Order Book Data Source.
Handles public WebSocket streams for order book and trade data.
"""

import asyncio
import json
import time
from typing import TYPE_CHECKING, Any

from hummingbot.connector.exchange.backpack import (
    backpack_constants as CONSTANTS,
    backpack_utils as utils,
    backpack_web_utils as web_utils,
)
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

    Handles:
    - Order book snapshots via REST API
    - Order book updates via WebSocket
    - Trade data via WebSocket
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
        self._trade_messages_queue_key = CONSTANTS.WS_TRADES_CHANNEL
        self._diff_messages_queue_key = CONSTANTS.WS_DEPTH_CHANNEL

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
            "limit": 1000,  # Get deep order book
        }

        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(CONSTANTS.DEPTH_URL, self._domain),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.DEPTH_URL,
        )

        return data if isinstance(data, dict) else {}

    async def _subscribe_channels(self, ws: WSAssistant):
        """Subscribe to WebSocket channels for order book and trade data.

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

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """Create and connect WebSocket assistant for public streams.

        Returns:
            Connected WebSocket assistant
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(
            ws_url=web_utils.ws_public_url(self._domain),
            message_timeout=CONSTANTS.REQUEST_TIMEOUT,
        )
        return ws

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
            if raw_message.get("type") == CONSTANTS.WS_DEPTH_CHANNEL:
                data = raw_message.get("data", {})
                exchange_symbol = data.get("symbol")

                if exchange_symbol:
                    trading_pair = utils.convert_from_exchange_trading_pair(exchange_symbol)

                    if trading_pair in self._trading_pairs:
                        # WebSocket timestamps are in microseconds (new API)
                        timestamp = data.get("timestamp", time.time() * 1_000_000)

                        order_book_message = OrderBookMessage(
                            message_type=OrderBookMessageType.DIFF,
                            content={
                                "trading_pair": trading_pair,
                                "update_id": data.get("lastUpdateId", 0),
                                "bids": data.get("bids", []),
                                "asks": data.get("asks", []),
                            },
                            timestamp=timestamp / 1_000_000,  # Convert microseconds to seconds
                        )

                        message_queue.put_nowait(order_book_message)

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
            if raw_message.get("type") == CONSTANTS.WS_TRADES_CHANNEL:
                data = raw_message.get("data", {})
                exchange_symbol = data.get("symbol")

                if exchange_symbol:
                    trading_pair = utils.convert_from_exchange_trading_pair(exchange_symbol)

                    if trading_pair in self._trading_pairs:
                        # WebSocket timestamps are in microseconds (new API)
                        timestamp = data.get("timestamp", time.time() * 1_000_000)

                        trade_message = OrderBookMessage(
                            message_type=OrderBookMessageType.TRADE,
                            content={
                                "trading_pair": trading_pair,
                                "trade_type": data.get("side", "").upper(),
                                "trade_id": data.get("tradeId"),
                                "price": float(data.get("price", 0)),
                                "amount": float(data.get("quantity", 0)),
                            },
                            timestamp=timestamp / 1_000_000,  # Convert microseconds to seconds
                        )

                        message_queue.put_nowait(trade_message)

        except Exception:
            self.logger().error(
                f"Error parsing trade message: {raw_message}",
                exc_info=True,
            )

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """Listen for trade messages from WebSocket stream.

        Args:
            ev_loop: Event loop
            output: Output queue for trade messages
        """
        while True:
            try:
                ws = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)

                async for ws_response in ws.iter_messages():
                    try:
                        if ws_response is None or ws_response.data is None:
                            continue
                        data = json.loads(ws_response.data)
                        await self._parse_trade_message(data, output)
                    except Exception:
                        self.logger().error(
                            "Error processing trade message",
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

    async def listen_for_order_book_diffs(
        self,
        ev_loop: asyncio.AbstractEventLoop,
        output: asyncio.Queue,
    ):
        """Listen for order book diff messages from WebSocket stream.

        Args:
            ev_loop: Event loop
            output: Output queue for order book messages
        """
        while True:
            try:
                ws = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)

                async for ws_response in ws.iter_messages():
                    try:
                        if ws_response is None or ws_response.data is None:
                            continue
                        data = json.loads(ws_response.data)
                        await self._parse_order_book_diff_message(data, output)
                    except Exception:
                        self.logger().error(
                            "Error processing order book diff message",
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

    async def listen_for_order_book_snapshots(
        self,
        ev_loop: asyncio.AbstractEventLoop,
        output: asyncio.Queue,
    ):
        """Periodically fetch order book snapshots from REST API.

        Args:
            ev_loop: Event loop
            output: Output queue for order book snapshots
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot_data = await self._request_order_book_snapshot(trading_pair)

                        snapshot_message = OrderBookMessage(
                            message_type=OrderBookMessageType.SNAPSHOT,
                            content={
                                "trading_pair": trading_pair,
                                "update_id": snapshot_data.get("lastUpdateId", 0),
                                "bids": snapshot_data.get("bids", []),
                                "asks": snapshot_data.get("asks", []),
                            },
                            timestamp=time.time(),
                        )

                        output.put_nowait(snapshot_message)

                    except Exception:
                        self.logger().error(
                            f"Error fetching order book snapshot for {trading_pair}",
                            exc_info=True,
                        )

                # Wait before next snapshot cycle
                await asyncio.sleep(self.ONE_HOUR)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error in order book snapshot loop",
                    exc_info=True,
                )
                await asyncio.sleep(60.0)

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
            if not isinstance(data, dict):
                return []
            for symbol_data in data.get("symbols", []):
                if symbol_data.get("status") == "TRADING":
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
