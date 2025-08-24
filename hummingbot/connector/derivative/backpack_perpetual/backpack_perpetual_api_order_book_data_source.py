"""Order book data source for Backpack Perpetual Exchange.
Handles public market data streams including order books, trades, and tickers.
"""

import asyncio
import json
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_utils as utils,
    backpack_perpetual_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger


if TYPE_CHECKING:
    from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_derivative import (
        BackpackPerpetualDerivative,
    )

_logger: HummingbotLogger | None = None


class BackpackPerpetualAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """Data source for Backpack Perpetual order book updates.
    Manages WebSocket connections for real-time market data.
    """

    def __init__(
        self,
        trading_pairs: list[str],
        connector: "BackpackPerpetualDerivative",
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
        self._ws_assistant: WSAssistant | None = None
        self._message_id_counter = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global _logger
        if _logger is None:
            _logger = HummingbotLogger(__name__)
        return _logger

    async def get_last_traded_prices(
        self,
        trading_pairs: list[str],
        domain: str | None = None,
    ) -> dict[str, float]:
        """Get last traded prices for trading pairs.

        Args:
            trading_pairs: List of trading pairs
            domain: Exchange domain

        Returns:
            Dictionary mapping trading pairs to their last traded prices
        """
        domain = domain or self._domain
        results = {}

        # Query ticker endpoint for all pairs
        for trading_pair in trading_pairs:
            try:
                symbol = utils.convert_to_exchange_trading_pair(trading_pair)

                response = await web_utils.api_request(
                    path=CONSTANTS.TICKER_URL,
                    api_factory=self._api_factory,
                    params={"symbol": symbol},
                    method=RESTMethod.GET,
                    is_auth_required=False,
                )

                if "lastPrice" in response:
                    results[trading_pair] = float(response["lastPrice"])

            except Exception:
                self.logger().exception(f"Error fetching last price for {trading_pair}")
                continue

        return results

    async def fetch_trading_pairs(
        self,
        domain: str | None = None,
    ) -> list[str]:
        """Fetch all available trading pairs from the exchange.

        Args:
            domain: Exchange domain

        Returns:
            List of available trading pairs
        """
        domain = domain or self._domain

        response = await web_utils.api_request(
            path=CONSTANTS.EXCHANGE_INFO_URL,
            api_factory=self._api_factory,
            method=RESTMethod.GET,
            is_auth_required=False,
        )

        trading_pairs = []

        for market_info in response.get("symbols", []):
            try:
                # Only include perpetual markets
                if utils.is_perpetual_symbol(market_info["symbol"]):
                    trading_pair = utils.convert_from_exchange_trading_pair(market_info["symbol"])
                    if trading_pair:
                        trading_pairs.append(trading_pair)
            except Exception:
                continue

        return trading_pairs

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """Get funding rate information for a trading pair.

        Args:
            trading_pair: Trading pair to fetch funding info for

        Returns:
            FundingInfo object containing funding rate data
        """
        symbol = utils.convert_to_exchange_trading_pair(trading_pair)
        
        # Get mark prices endpoint which includes funding rate info per OpenAPI spec
        mark_prices_response = await web_utils.api_request(
            path=CONSTANTS.MARK_PRICE_URL,
            api_factory=self._api_factory,
            params={"symbol": symbol},
            method=RESTMethod.GET,
            is_auth_required=False,
        )
        
        # The response is an array, even when filtered by symbol
        if isinstance(mark_prices_response, list):
            mark_price_data = mark_prices_response[0] if mark_prices_response else {}
        else:
            mark_price_data = mark_prices_response
        
        # Trust exchange data structure per OpenAPI spec
        # Convert to appropriate types
        funding_info = FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal(str(mark_price_data["indexPrice"])),
            mark_price=Decimal(str(mark_price_data["markPrice"])),
            next_funding_utc_timestamp=mark_price_data["nextFundingTimestamp"] / 1000,  # Convert ms to seconds
            rate=Decimal(str(mark_price_data["fundingRate"])),
        )
        
        return funding_info

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        """Get order book snapshot for a trading pair.

        Args:
            trading_pair: Trading pair to fetch snapshot for

        Returns:
            OrderBookMessage containing snapshot data
        """
        snapshot_data = await self.get_order_book_data(trading_pair)
        
        snapshot_msg = OrderBookMessage(
            message_type=OrderBookMessageType.SNAPSHOT,
            content={
                "trading_pair": trading_pair,
                "bids": snapshot_data["bids"],
                "asks": snapshot_data["asks"],
                "update_id": int(snapshot_data["timestamp"]),
            },
            timestamp=snapshot_data["timestamp"] / 1000.0,  # Convert to seconds
        )
        
        return snapshot_msg

    async def get_order_book_data(
        self,
        trading_pair: str,
    ) -> dict[str, Any]:
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
            params={"symbol": symbol, "limit": 100},
            method=RESTMethod.GET,
            is_auth_required=False,
        )

        return {
            "trading_pair": trading_pair,
            "symbol": symbol,
            "bids": response["bids"],
            "asks": response["asks"],
            "timestamp": response["timestamp"],
        }

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """Listen for trade data via WebSocket.

        Args:
            ev_loop: Event loop
            output: Queue to put trade messages
        """
        while True:
            try:
                ws = await self._create_websocket_connection()

                # Subscribe to trade channels
                for trading_pair in self._trading_pairs:
                    symbol = utils.convert_to_exchange_trading_pair(trading_pair)
                    await self._subscribe_to_channel(ws, CONSTANTS.WS_TRADES_CHANNEL, symbol)

                async for ws_response in ws.iter_messages():
                    data = json.loads(ws_response.data)

                    if self._is_trade_message(data):
                        trade_msg = self._parse_trade_message(data)
                        if trade_msg:
                            await output.put(trade_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error in trade WebSocket listener")
                await self._sleep(5.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """Listen for order book updates via WebSocket.

        Args:
            ev_loop: Event loop
            output: Queue to put order book diff messages
        """
        while True:
            try:
                ws = await self._create_websocket_connection()

                # Subscribe to depth channels
                for trading_pair in self._trading_pairs:
                    symbol = utils.convert_to_exchange_trading_pair(trading_pair)
                    await self._subscribe_to_channel(ws, CONSTANTS.WS_DEPTH_CHANNEL, symbol)

                async for ws_response in ws.iter_messages():
                    data = json.loads(ws_response.data)

                    if self._is_order_book_diff_message(data):
                        diff_msg = self._parse_order_book_diff_message(data)
                        if diff_msg:
                            await output.put(diff_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error in order book diff WebSocket listener")
                await self._sleep(5.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """Periodically fetch order book snapshots via REST API.

        Args:
            ev_loop: Event loop
            output: Queue to put order book snapshot messages
        """
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    try:
                        snapshot_data = await self.get_order_book_data(trading_pair)

                        snapshot_msg = OrderBookMessage(
                            message_type=OrderBookMessageType.SNAPSHOT,
                            content={
                                "trading_pair": trading_pair,
                                "bids": snapshot_data["bids"],
                                "asks": snapshot_data["asks"],
                                "update_id": int(snapshot_data["timestamp"]),
                            },
                            timestamp=self._time(),
                        )

                        await output.put(snapshot_msg)

                    except Exception:
                        self.logger().exception(f"Error fetching snapshot for {trading_pair}")

                # Sleep for snapshot interval (60 seconds)
                await self._sleep(60.0)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error in snapshot fetcher")
                await self._sleep(5.0)

    async def listen_for_funding_info(self, output: asyncio.Queue):
        """Listen for funding rate updates.

        Args:
            output: Queue to put funding info messages
        """
        while True:
            try:
                ws = await self._create_websocket_connection()

                # Subscribe to mark price channels (which include funding rate info)
                for trading_pair in self._trading_pairs:
                    symbol = utils.convert_to_exchange_trading_pair(trading_pair)
                    await self._subscribe_to_channel(ws, CONSTANTS.WS_MARK_PRICE_CHANNEL, symbol)

                async for ws_response in ws.iter_messages():
                    data = json.loads(ws_response.data)

                    if self._is_funding_rate_message(data):
                        funding_msg = self._parse_funding_rate_message(data)
                        if funding_msg:
                            await output.put(funding_msg)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Error in funding rate WebSocket listener")
                await self._sleep(5.0)

    # Helper methods
    async def _create_websocket_connection(self) -> WSAssistant:
        """Create and return a WebSocket connection."""
        ws = await self._api_factory.get_ws_assistant()
        # Get WebSocket URL using proper domain-based lookup
        ws_url = CONSTANTS.WSS_URLS.get(self._domain, CONSTANTS.WSS_URLS[CONSTANTS.DEFAULT_DOMAIN])
        await ws.connect(
            ws_url=ws_url,
            message_timeout=CONSTANTS.WS_MESSAGE_TIMEOUT,
        )
        return ws

    async def _subscribe_to_channel(self, ws: WSAssistant, channel: str, symbol: str):
        """Subscribe to a WebSocket channel.

        Args:
            ws: WebSocket assistant
            channel: Channel name
            symbol: Trading symbol
        """
        # Backpack uses the format: {"method": "SUBSCRIBE", "params": ["stream_name"]}
        subscribe_msg = {
            "method": "SUBSCRIBE",
            "params": [f"{channel}.{symbol}"],  # Use dot notation as per Backpack docs
        }

        await ws.send(json.dumps(subscribe_msg))

    def _is_trade_message(self, data: dict[str, Any]) -> bool:
        """Check if message is a trade update."""
        # Check both wrapped format (stream + data) and direct format
        if "stream" in data and data.get("stream", "").startswith("trade."):
            return True
        inner_data = data.get("data", data)
        return inner_data.get("e") == "trade" or inner_data.get("type") == "trade"

    def _is_order_book_diff_message(self, data: dict[str, Any]) -> bool:
        """Check if message is an order book diff."""
        # Check both wrapped format (stream + data) and direct format
        if "stream" in data and data.get("stream", "").startswith("depth."):
            return True
        # Also check the data field if it exists
        inner_data = data.get("data", data)
        return inner_data.get("e") == "depth" or inner_data.get("type") == "depth"

    def _is_funding_rate_message(self, data: dict[str, Any]) -> bool:
        """Check if message is a funding rate update."""
        # Check both wrapped format (stream + data) and direct format
        if "stream" in data and (data.get("stream", "").startswith("funding.") or 
                                  data.get("stream", "").startswith("markPrice.")):
            return True
        inner_data = data.get("data", data)
        return inner_data.get("e") in ["funding", "markPrice"] or inner_data.get("type") == "funding"

    def _parse_trade_message(self, data: dict[str, Any]) -> OrderBookMessage | None:
        """Parse trade message from WebSocket.

        Args:
            data: Raw trade data

        Returns:
            OrderBookMessage or None if parsing fails
        """
        try:
            # Extract trade data
            trade_data = data.get("data", data)

            # Get trading pair
            symbol = trade_data["symbol"]
            trading_pair = utils.convert_from_exchange_trading_pair(symbol)
            if not trading_pair:
                return None  # Not a supported trading pair

            # Trust exchange data structure - access fields directly
            return OrderBookMessage(
                message_type=OrderBookMessageType.TRADE,
                content={
                    "trading_pair": trading_pair,
                    "trade_id": str(trade_data.get("tradeId", trade_data.get("id"))),
                    "price": trade_data["price"],
                    "amount": trade_data["quantity"],
                    "trade_type": float(TradeType.BUY.value) if trade_data["side"] == "Buy" else float(TradeType.SELL.value),
                },
                timestamp=trade_data["timestamp"] / 1000.0,
            )

        except Exception:
            self.logger().exception("Error parsing trade message")
            return None

    def _parse_order_book_diff_message(self, data: dict[str, Any]) -> OrderBookMessage | None:
        """Parse order book diff message from WebSocket.

        Args:
            data: Raw order book diff data

        Returns:
            OrderBookMessage or None if parsing fails
        """
        try:
            # Extract depth data
            depth_data = data.get("data", data)

            # Get trading pair
            symbol = depth_data["symbol"]
            trading_pair = utils.convert_from_exchange_trading_pair(symbol)
            if not trading_pair:
                return None  # Not a supported trading pair

            # Trust exchange data - try alternate field names for compatibility
            return OrderBookMessage(
                message_type=OrderBookMessageType.DIFF,
                content={
                    "trading_pair": trading_pair,
                    "bids": depth_data.get("b", depth_data.get("bids", [])),
                    "asks": depth_data.get("a", depth_data.get("asks", [])),
                    "update_id": depth_data.get("lastUpdateId", depth_data.get("u")),
                    "first_update_id": depth_data.get("firstUpdateId", depth_data.get("U")),
                },
                timestamp=(depth_data.get("timestamp", depth_data.get("T", self._time() * 1000))) / 1000.0,
            )

        except Exception:
            self.logger().exception("Error parsing order book diff message")
            return None

    def _parse_funding_rate_message(self, data: dict[str, Any]) -> FundingInfo | None:
        """Parse funding rate message from WebSocket.

        Args:
            data: Raw funding rate data

        Returns:
            Parsed FundingInfo or None if parsing fails
        """
        try:
            # Extract funding data
            funding_data = data.get("data", data)

            # Get trading pair from 's' field (symbol)
            symbol = funding_data.get("s", funding_data.get("symbol", ""))
            trading_pair = utils.convert_from_exchange_trading_pair(symbol)

            if not trading_pair:
                return None

            # Parse markPrice format per OpenAPI spec
            # Trust exchange data structure - access fields directly
            return FundingInfo(
                trading_pair=trading_pair,
                index_price=Decimal(str(funding_data["indexPrice"])),
                mark_price=Decimal(str(funding_data["markPrice"])),
                next_funding_utc_timestamp=funding_data["nextFundingTimestamp"] / 1000,  # ms to seconds
                rate=Decimal(str(funding_data["fundingRate"])),
            )

        except Exception:
            self.logger().exception("Error parsing funding rate message")
            return None

    async def _sleep(self, delay: float):
        """Sleep for specified delay."""
        await asyncio.sleep(delay)

    def _time(self) -> float:
        """Get current time in seconds."""
        import time
        return time.time()
    
