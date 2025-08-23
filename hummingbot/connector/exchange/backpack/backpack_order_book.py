"""Backpack Order Book implementation."""

from typing import Dict, Optional

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


class BackpackOrderBook(OrderBook):
    """
    Order book implementation for Backpack spot exchange.

    Handles the conversion of Backpack-specific order book messages
    to the standard Hummingbot format.
    """

    @classmethod
    def snapshot_message_from_exchange(
        cls,
        msg: Dict[str, any],
        timestamp: float,
        metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        """
        Create a snapshot message from exchange data.

        Args:
            msg: The snapshot data from the exchange
            timestamp: The message timestamp
            metadata: Additional metadata to include

        Returns:
            OrderBookMessage containing the snapshot
        """
        if metadata:
            msg.update(metadata)

        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": msg["trading_pair"],
                "update_id": msg["lastUpdateId"],
                "bids": msg["bids"],
                "asks": msg["asks"]
            },
            timestamp=timestamp
        )

    @classmethod
    def diff_message_from_exchange(
        cls,
        msg: Dict[str, any],
        timestamp: Optional[float] = None,
        metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        """
        Create a diff message from exchange data.

        Args:
            msg: The diff data from the exchange
            timestamp: The message timestamp
            metadata: Additional metadata to include

        Returns:
            OrderBookMessage containing the diff
        """
        if metadata:
            msg.update(metadata)

        return OrderBookMessage(
            OrderBookMessageType.DIFF,
            {
                "trading_pair": msg["trading_pair"],
                "first_update_id": msg["U"],
                "update_id": msg["u"],
                "bids": msg["b"],
                "asks": msg["a"]
            },
            timestamp=timestamp
        )

    @classmethod
    def trade_message_from_exchange(
        cls,
        msg: Dict[str, any],
        metadata: Optional[Dict] = None
    ) -> OrderBookMessage:
        """
        Create a trade message from exchange data.

        Args:
            msg: The trade data from the exchange
            metadata: Additional metadata to include

        Returns:
            OrderBookMessage containing the trade
        """
        if metadata:
            msg.update(metadata)

        ts = msg["E"]
        return OrderBookMessage(
            OrderBookMessageType.TRADE,
            {
                "trading_pair": msg["trading_pair"],
                "trade_type": float(TradeType.SELL.value) if msg["m"] else float(TradeType.BUY.value),
                "trade_id": msg["t"],
                "update_id": ts,
                "price": msg["p"],
                "amount": msg["q"]
            },
            timestamp=ts * 1e-3
        )
