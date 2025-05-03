__all__ = ["HyperliquidWebsocketMapper"]

import logging
import time
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

import structlog

from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsOrderUpdate,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.core.models import (
    Order,
    OrderBook,
    OrderSide,
    OrderStatus,
    OrderType,
    Trade,
)

logger = structlog.get_logger(__name__)


class HyperliquidWebsocketMapper:
    """
    Strict boundary validator and transformer for Hyperliquid WebSocket events.
    All methods are static and validate incoming data using Pydantic models before mapping to
    internal models.
    """

    @staticmethod
    def parse_fill_message(message: dict[str, Any], logger: logging.Logger) -> list[Trade]:
        trades: list[Trade] = []
        if message.get("type") == "userFill":
            fill_data_raw = message.get("data", {})
            try:
                validated = HyperliquidRawWsFillEvent.model_validate(fill_data_raw)
            except Exception as e:
                logger.warning(
                    f"[Hyperliquid] Invalid fill event (dropped): {e} | Data: {fill_data_raw}"
                )
                return trades
            coin = validated.coin
            side_str = validated.side
            px_str = validated.px
            sz_str = validated.sz
            time_ms = validated.time
            order_id = str(validated.oid)
            try:
                side = OrderSide.BUY if side_str == "B" else OrderSide.SELL
                price = Decimal(str(px_str))
                quantity = Decimal(str(sz_str))
                trade = Trade(
                    id=f"{order_id}_{time_ms}",
                    symbol=coin,
                    executed_at=datetime.fromtimestamp(time_ms / 1000, tz=UTC),
                    side=side,
                    order_id=order_id,
                    exchange="hyperliquid",
                    client_order_id=validated.cloid or "",
                    price=price,
                    quantity=quantity,
                    fee=Decimal("0"),
                    fee_asset="USDC",
                    is_maker=validated.is_maker,
                )
                trades.append(trade)
            except (ValueError, TypeError) as e:
                logger.warning(f"Error parsing validated fill data: {e}")
        return trades

    @staticmethod
    def parse_trade_message(message: dict[str, Any], logger: logging.Logger) -> Trade | None:
        if message.get("channel") == "allMids":
            logger.warning(
                "[Hyperliquid] parse_ticker_message needs specific implementation "
                "for 'allMids' structure."
            )
            return None
        if "channel" in message and message["channel"] == "trades":
            data_raw: list[Any] = message.get("data", [])
            for trade_dict_any_item in data_raw:
                if not isinstance(trade_dict_any_item, dict):
                    continue
                trade_dict: dict[str, Any] = cast(dict[str, Any], trade_dict_any_item)
                try:
                    validated = HyperliquidRawWsTradeEvent.model_validate(trade_dict)
                except Exception as e:
                    logger.warning(
                        f"[Hyperliquid] Invalid trade event (dropped): {e} | Data: {trade_dict}"
                    )
                    continue
                try:
                    side = OrderSide.BUY if validated.side == "B" else OrderSide.SELL
                    price = Decimal(validated.px)
                    quantity = Decimal(validated.sz)
                    return Trade(
                        id=validated.hash,
                        symbol=validated.coin,
                        executed_at=datetime.fromtimestamp(validated.time / 1000, tz=UTC),
                        side=side,
                        order_id="",
                        exchange="hyperliquid",
                        client_order_id="",
                        price=price,
                        quantity=quantity,
                        fee=Decimal("0"),
                        fee_asset="USDC",
                        is_maker=None,
                    )
                except Exception as e:
                    logger.warning(f"[Hyperliquid] Error parsing validated trade event: {e}")
        return None

    @staticmethod
    def parse_orderbook_message(
        message: dict[str, Any], logger: logging.Logger
    ) -> OrderBook | None:
        if message.get("channel", "").startswith("l2Book:"):
            data_raw = message.get("data", {})
            try:
                validated = HyperliquidRawWsBookUpdate.model_validate(data_raw)
            except Exception as e:
                logger.warning(
                    f"[Hyperliquid] Invalid order book event (dropped): {e} | Data: {data_raw}"
                )
                return None
            try:
                bids = (
                    [(Decimal(level.px), Decimal(level.sz)) for level in validated.levels[0]]
                    if validated.levels
                    else []
                )
                asks = (
                    [(Decimal(level.px), Decimal(level.sz)) for level in validated.levels[1]]
                    if validated.levels and len(validated.levels) > 1
                    else []
                )
                return OrderBook(
                    symbol=validated.coin,
                    bids=bids,
                    asks=asks,
                    timestamp=validated.time,
                )
            except Exception as e:
                logger.warning(f"[Hyperliquid] Error parsing validated order book event: {e}")
        return None

    @staticmethod
    def parse_order_update_message(
        message: dict[str, Any],
        logger: logging.Logger,
        parse_order_fn: "Callable[[dict[str, Any]], Order]",
    ) -> Order | None:
        """
        Parse an order update message using Pydantic validation and a provided order parsing
        function.
        Args:
            message: The WebSocket message dict.
            logger: Logger for warnings/errors.
            parse_order_fn: Callable that parses order data dict to Order.
        Returns:
            Order object or None if not found/invalid.
        """
        if "type" in message and message["type"] == "userEvent" and "userEvents" in message:
            events_raw = message.get("userEvents", [])
            for event in events_raw:
                try:
                    validated = HyperliquidRawWsOrderUpdate.model_validate(event)
                except Exception as e:
                    logger.warning(
                        f"[Hyperliquid] Invalid order update event (dropped): {e} | Data: {event}"
                    )
                    continue
                if validated.event_type == "order":
                    order_data = validated.data
                    try:
                        order = parse_order_fn(order_data)
                        return order
                    except Exception as e:
                        logger.error(f"Error parsing order update message: {e}")
        return None

    @staticmethod
    def parse_order(
        data: dict[str, Any],
    ) -> Order:
        """
        Parse exchange-specific order format to standard Order object.
        Args:
            data: Exchange-specific order data (dictionary)
        Returns:
            Standardized Order object
        """
        if not data:
            raise ValueError("Invalid order data provided")
        try:
            order_id = str(data.get("oid", ""))
            if not order_id:
                raise ValueError("Order ID missing from order data")
            symbol = data.get("coin", "")
            if not symbol:
                raise ValueError("Symbol missing from order data")
            side_str = data.get("side", "B")
            type_str = data.get("orderType", "limit").lower()
            status_str = data.get("status", "open").lower()
            price_str = data.get("limitPx", "0")
            size_str = data.get("sz", "0")
            remaining_str = data.get("remainingSz", size_str)
            filled_qty = (
                Decimal(str(size_str)) - Decimal(str(remaining_str))
                if remaining_str
                else Decimal("0")
            )
            timestamp = data.get("time", int(time.time() * 1000))
            side = OrderSide.BUY if side_str == "B" else OrderSide.SELL
            order_type = OrderType.LIMIT
            if type_str == "market":
                order_type = OrderType.MARKET
            elif type_str == "postonly":
                order_type = OrderType.LIMIT  # Post-only is a flag, not an order type
            status = OrderStatus.OPEN
            if status_str == "filled":
                status = OrderStatus.FILLED
            elif status_str in ["cancelled", "canceled"]:
                status = OrderStatus.CANCELED
            elif status_str == "rejected":
                status = OrderStatus.REJECTED
            elif Decimal(str(remaining_str)) < Decimal(str(size_str)):
                status = OrderStatus.PARTIALLY_FILLED
            return Order(
                client_order_id=data.get("cloid", order_id),
                exchange_order_id=order_id,
                related_order_id=None,
                symbol=symbol,
                side=side,
                order_type=order_type,
                status=status,
                quantity_requested=Decimal(str(size_str)),
                quantity_filled=filled_qty,
                price=Decimal(str(price_str)),
                average_fill_price=None,  # Set if available
                created_at=datetime.fromtimestamp(timestamp / 1000, tz=UTC),
                exchange="hyperliquid",
                executed_quote_quantity=None,
                trigger_by=None,
                self_trade_prevention=None,
                updated_at=None,
                triggered_at=None,
                expiry_reason=None,
                origin=None,
                strategy_name=None,
                signal_id=None,
            )
        except Exception as e:
            logger.error(f"Error parsing order data: {e}")
            raise ValueError(f"Failed to parse order: {e}") from e
