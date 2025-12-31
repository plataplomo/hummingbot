"""Order book data source for Backpack Perpetual Exchange.
Handles public market data streams including order books, trades, and tickers.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_utils as utils,
    backpack_perpetual_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import (
        BackpackPerpetualDerivative,
    )


class BackpackPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """Data source for Backpack Perpetual order book updates.
    Follows the parent class pattern for WebSocket message routing.
    """

    def __init__(
        self,
        trading_pairs: list[str],
        connector: BackpackPerpetualDerivative,
        api_factory: WebAssistantsFactory,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
    ):
        """Initialize the order book data source.

        Args:
            trading_pairs: List of trading pairs to track
            connector: Parent connector instance
            api_factory: Web assistants factory for API connections
            domain: Exchange domain
        """
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain

    async def get_last_traded_prices(
        self,
        trading_pairs: list[str],
        domain: str | None = None,
    ) -> dict[str, float]:
        """Get last traded prices for trading pairs."""
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """Get funding rate information for a trading pair.

        Args:
            trading_pair: Trading pair to fetch funding info for

        Returns:
            FundingInfo object containing funding rate data
        """
        symbol = utils.convert_to_exchange_trading_pair(trading_pair)

        mark_prices_response = await web_utils.api_request(
            path=CONSTANTS.MARK_PRICE_URL,
            api_factory=self._api_factory,
            params={"symbol": symbol},
            method=RESTMethod.GET,
            is_auth_required=False,
        )

        mark_price_list = utils.normalize_response_to_list(mark_prices_response)

        mark_price_data: dict[str, Any] = (
            mark_price_list[0] if mark_price_list and isinstance(mark_price_list[0], dict) else {}
        )

        next_funding_time = mark_price_data.get("nextFundingTimestamp", 0)
        next_funding_seconds = int(next_funding_time) / 1000 if next_funding_time else 0

        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(str(mark_price_data.get("indexPrice", 0))),
            mark_price=Decimal(str(mark_price_data.get("markPrice", 0))),
            next_funding_utc_timestamp=next_funding_seconds,
            rate=Decimal(str(mark_price_data.get("fundingRate", 0))),
        )

        return funding_info

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """Get order book snapshot for a trading pair.

        Args:
            trading_pair: Trading pair to fetch snapshot for

        Returns:
            OrderBookMessage containing snapshot data
        """
        snapshot_data = await self._request_order_book_snapshot(trading_pair)
        if "bids" not in snapshot_data or "asks" not in snapshot_data:
            raise ValueError(f"Missing bids or asks in snapshot: {snapshot_data}")

        bids = snapshot_data["bids"]
        asks = snapshot_data["asks"]
        formatted_bids = [[float(bid[0]), float(bid[1])] for bid in bids if len(bid) >= 2]
        formatted_asks = [[float(ask[0]), float(ask[1])] for ask in asks if len(ask) >= 2]

        snapshot_msg = OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "bids": formatted_bids,
                "asks": formatted_asks,
                "update_id": int(snapshot_data.get("lastUpdateId", 0)),
            },
            # REST depth endpoint returns timestamp in microseconds
            timestamp=snapshot_data.get("timestamp", time.time() * 1_000_000) / 1_000_000,
        )

        return snapshot_msg

    async def _request_order_book_snapshot(self, trading_pair: str) -> dict[str, Any]:
        """Get order book snapshot from REST API.

        Args:
            trading_pair: Trading pair to fetch

        Returns:
            Order book snapshot data
        """
        symbol = utils.convert_to_exchange_trading_pair(trading_pair)

        response = await web_utils.api_request(
            path=CONSTANTS.ORDER_BOOK_URL,
            api_factory=self._api_factory,
            params={"symbol": symbol, "limit": CONSTANTS.ORDER_BOOK_DEPTH_LIMIT},
            method=RESTMethod.GET,
            is_auth_required=False,
        )
        return response if isinstance(response, dict) else {}

    async def get_order_book_data(self, trading_pair: str) -> dict[str, Any]:
        """Get order book data for a specific trading pair."""
        return await self._request_order_book_snapshot(trading_pair)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """Create and connect WebSocket assistant for public streams.

        Implements parent class pattern.

        Returns:
            Connected WebSocket assistant
        """
        ws = await self._api_factory.get_ws_assistant()
        ws_url = CONSTANTS.WSS_URLS.get(self._domain, CONSTANTS.WSS_URLS[CONSTANTS.DEFAULT_DOMAIN])
        async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_CONNECTION_LIMIT_ID):
            await ws.connect(
                ws_url=ws_url,
                ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL,
            )
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        """Subscribe to WebSocket channels for order book and trade data.

        Implements parent class pattern.

        Args:
            ws: WebSocket assistant
        """
        try:
            subscriptions = []

            for trading_pair in self._trading_pairs:
                symbol = utils.convert_to_exchange_trading_pair(trading_pair)

                # Subscribe to required channels
                subscriptions.extend([
                    f"{CONSTANTS.WS_DEPTH_CHANNEL}.{symbol}",
                    f"{CONSTANTS.WS_TRADES_CHANNEL}.{symbol}",
                    f"{CONSTANTS.WS_MARK_PRICE_CHANNEL}.{symbol}",  # For funding info
                ])

            # Send subscription request
            subscription_payload = {
                "method": "SUBSCRIBE",
                "params": subscriptions,
            }

            subscribe_request = WSJSONRequest(payload=subscription_payload)
            async with self._api_factory.throttler.execute_task(limit_id=CONSTANTS.WS_PUBLIC_SUBSCRIPTION_LIMIT_ID):
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

        # Check stream field if present (wrapped format)
        stream_name = event_message.get("stream", "")

        # Route based on stream name or data content
        if "depth" in stream_name or self._is_order_book_message(event_message):
            channel = self._diff_messages_queue_key
        elif "trade" in stream_name or self._is_trade_message(event_message):
            channel = self._trade_messages_queue_key
        elif "markPrice" in stream_name or self._is_funding_message(event_message):
            channel = self._funding_info_messages_queue_key

        return channel

    def _is_order_book_message(self, data: dict[str, Any]) -> bool:
        """Check if message is an order book update."""
        inner_data = data.get("data", data)
        return (
            inner_data.get("e") == "depth"
            or inner_data.get("type") == "depth"
            or "bids" in inner_data
            or "asks" in inner_data
            or "a" in inner_data
            or "b" in inner_data
        )

    def _is_order_book_diff_message(self, data: dict[str, Any]) -> bool:
        """Check if message is an order book diff update."""
        return self._is_order_book_message(data)

    async def fetch_trading_pairs(self, domain: str | None = None) -> list[str]:
        """Fetch available trading pairs from the exchange."""
        response = await web_utils.api_request(
            path=CONSTANTS.EXCHANGE_INFO_URL,
            api_factory=self._api_factory,
            params=None,
            method=RESTMethod.GET,
            is_auth_required=False,
        )

        symbols = web_utils.normalize_response_to_list(response)
        trading_pairs = []
        for symbol_info in symbols:
            try:
                market_type = symbol_info.get("marketType")
                if market_type and market_type not in ["PERP", "IPERP"]:
                    continue
                symbol = symbol_info.get("symbol")
                if not symbol or not utils.is_perpetual_symbol(symbol):
                    continue
                trading_pair = utils.convert_from_exchange_trading_pair(symbol)
                if trading_pair:
                    trading_pairs.append(trading_pair)
            except Exception:
                continue
        return trading_pairs

    def _is_trade_message(self, data: dict[str, Any]) -> bool:
        """Check if message is a trade update."""
        inner_data = data.get("data", data)
        return (
            inner_data.get("e") == "trade"
            or inner_data.get("type") == "trade"
            or ("p" in inner_data and "q" in inner_data and "m" in inner_data)
            or ("price" in inner_data and "quantity" in inner_data and "side" in inner_data)
        )

    def _is_funding_message(self, data: dict[str, Any]) -> bool:
        """Check if message is a funding/mark price update."""
        inner_data = data.get("data", data)
        return (
            inner_data.get("e") in ["markPrice", "funding"]
            or inner_data.get("type") in ["markPrice", "funding"]
            or ("markPrice" in inner_data and "fundingRate" in inner_data)
        )

    async def _parse_trade_message(self, raw_message: dict[str, Any], message_queue: asyncio.Queue):
        """Parse trade message from WebSocket.

        Args:
            raw_message: Raw trade data
            message_queue: Queue to put the parsed message
        """
        trade_data = raw_message.get("data", raw_message)

        symbol = trade_data.get("s", trade_data.get("symbol", ""))
        if not symbol:
            return

        trading_pair = utils.convert_from_exchange_trading_pair(symbol)
        if not trading_pair or trading_pair not in self._trading_pairs:
            return

        trade_id = trade_data.get("t", trade_data.get("tradeId", trade_data.get("id", trade_data.get("a"))))
        if trade_id is None:
            return

        side = trade_data.get("S", trade_data.get("side"))
        if side:
            trade_type = (
                float(TradeType.BUY.value)
                if str(side).upper() in ["BUY", "BID"]
                else float(TradeType.SELL.value)
            )
        else:
            is_buyer_maker = trade_data.get("m", False)
            trade_type = float(TradeType.SELL.value) if is_buyer_maker else float(TradeType.BUY.value)

        timestamp = trade_data.get("T", trade_data.get("E", trade_data.get("timestamp", time.time() * 1_000_000)))

        trade_message = OrderBookMessage(
            message_type=OrderBookMessageType.TRADE,
            content={
                "trading_pair": trading_pair,
                "trade_id": str(trade_id),
                "price": trade_data.get("p", trade_data.get("price", 0)),
                "amount": trade_data.get("q", trade_data.get("quantity", 0)),
                "trade_type": trade_type,
            },
            timestamp=timestamp / 1_000_000,
        )

        await message_queue.put(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: dict[str, Any], message_queue: asyncio.Queue):
        """Parse order book diff message from WebSocket.

        Args:
            raw_message: Raw order book diff data
            message_queue: Queue to put the parsed message
        """
        depth_data = raw_message.get("data", raw_message)

        symbol = depth_data.get("s", depth_data.get("symbol", ""))
        if not symbol:
            return

        trading_pair = utils.convert_from_exchange_trading_pair(symbol)
        if not trading_pair or trading_pair not in self._trading_pairs:
            return

        timestamp = depth_data.get("T", depth_data.get("timestamp", time.time() * 1_000_000))
        update_id = int(depth_data.get("u", depth_data.get("lastUpdateId", 0)))

        diff_message = OrderBookMessage(
            message_type=OrderBookMessageType.DIFF,
            content={
                "trading_pair": trading_pair,
                "bids": depth_data.get("b", depth_data.get("bids", [])),
                "asks": depth_data.get("a", depth_data.get("asks", [])),
                "update_id": update_id,
                "first_update_id": int(depth_data.get("U", depth_data.get("firstUpdateId", update_id))),
            },
            timestamp=timestamp / 1_000_000,
        )

        await message_queue.put(diff_message)

    async def _parse_order_book_snapshot_message(self, raw_message: dict[str, Any], message_queue: asyncio.Queue):
        """Parse order book snapshot message.

        Args:
            raw_message: Raw snapshot data
            message_queue: Queue to put the parsed message
        """
        snapshot_data = raw_message.get("data", raw_message)

        trading_pair = snapshot_data.get("trading_pair")
        if trading_pair is None:
            symbol = snapshot_data.get("symbol", snapshot_data.get("s", ""))
            if not symbol:
                return
            trading_pair = utils.convert_from_exchange_trading_pair(symbol)
            if not trading_pair:
                return

        bids = snapshot_data.get("bids", [])
        asks = snapshot_data.get("asks", [])
        formatted_bids = [[float(bid[0]), float(bid[1])] for bid in bids if len(bid) >= 2]
        formatted_asks = [[float(ask[0]), float(ask[1])] for ask in asks if len(ask) >= 2]

        snapshot_message = OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "bids": formatted_bids,
                "asks": formatted_asks,
                "update_id": int(snapshot_data.get("lastUpdateId", 0)),
            },
            # REST depth endpoint returns timestamp in microseconds
            timestamp=snapshot_data.get("timestamp", time.time() * 1_000_000) / 1_000_000,
        )

        await message_queue.put(snapshot_message)

    async def _parse_funding_info_message(self, raw_message: dict[str, Any], message_queue: asyncio.Queue):
        """Parse funding info messages from WebSocket.

        Args:
            raw_message: Raw funding info message
            message_queue: Queue to put parsed messages
        """
        data = raw_message.get("data", raw_message)

        symbol = data.get("s", data.get("symbol", ""))
        if not symbol:
            return

        trading_pair = utils.convert_from_exchange_trading_pair(symbol)
        if not trading_pair or trading_pair not in self._trading_pairs:
            return

        funding_info = FundingInfoUpdate(trading_pair=trading_pair)

        if "p" in data or "markPrice" in data:
            funding_info.mark_price = Decimal(str(data.get("p", data.get("markPrice", 0))))

        if "i" in data or "indexPrice" in data:
            funding_info.index_price = Decimal(str(data.get("i", data.get("indexPrice", 0))))

        if "f" in data or "fundingRate" in data:
            funding_info.rate = Decimal(str(data.get("f", data.get("fundingRate", 0))))

        if "n" in data or "nextFundingTimestamp" in data:
            timestamp = data.get("n", data.get("nextFundingTimestamp", 0))
            if timestamp > 1e15:  # Microseconds
                funding_info.next_funding_utc_timestamp = int(timestamp) // 1_000_000
            else:
                funding_info.next_funding_utc_timestamp = int(timestamp) // 1000

        await message_queue.put(funding_info)
