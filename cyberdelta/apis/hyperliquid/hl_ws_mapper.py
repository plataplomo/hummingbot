import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import structlog

from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.hyperliquid.hl_mapper import HyperliquidOrderMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawOrder
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsTradeEvent,
)
from cyberdelta.core.models import (
    Order,
    OrderBook,
    Trade,
)
from cyberdelta.core.models.market.trade import HyperliquidTradeDetails

logger = structlog.get_logger(__name__)


class HyperliquidWebsocketMapper:
    """
    Strict boundary validator and transformer for Hyperliquid WebSocket events.
    All methods are static and validate incoming data using Pydantic models before mapping to
    internal models.
    """

    @staticmethod
    def parse_fill_message(message: dict[str, Any], logger: logging.Logger) -> Trade | None:
        """Parses a single user fill event from a WebSocket message.
        Input `message` is expected to be the raw fill data dictionary.
        """
        try:
            validated_fill = HyperliquidRawWsFillEvent.model_validate(message)
        except Exception as e:
            logger.warning(
                f"[HyperliquidWsMapper] Invalid raw fill data (dropped): {e} | Data: {message!r}"
            )
            return None

        try:
            side = HyperliquidOrderMapper.map_side_to_internal(validated_fill.side)
            price = Decimal(validated_fill.px)
            quantity = Decimal(validated_fill.sz)
            executed_at = datetime.fromtimestamp(validated_fill.time / 1000, tz=UTC)

            details = HyperliquidTradeDetails(
                trade_hash=validated_fill.hash,
                liquidation_mark_px=None,
                start_position=None,
                dir=None,
            )

            return Trade(
                id=validated_fill.hash,
                symbol=validated_fill.coin,
                executed_at=executed_at,
                side=side,
                order_id=str(validated_fill.oid),
                exchange=ExchangeName.HYPERLIQUID.value,
                client_order_id=validated_fill.cloid,
                price=price,
                quantity=quantity,
                fee=Decimal("0"),
                fee_asset=None,
                is_maker=validated_fill.is_maker,
                hl_details=details,
            )
        except (ValueError, TypeError, Exception) as e:
            logger.warning(
                f"[HyperliquidWsMapper] Error parsing validated fill data: {e}. "
                f"Data: {validated_fill.model_dump()!r}"
            )
            return None

    @staticmethod
    def parse_trade_message(message: dict[str, Any], logger: logging.Logger) -> list[Trade]:
        """Parses a list of public trades from a WebSocket message."""
        trades: list[Trade] = []
        if "channel" in message and message["channel"] == "trades":
            data_raw_list: list[Any] = message.get("data", [])
            for trade_dict_item in data_raw_list:
                if not isinstance(trade_dict_item, dict):
                    logger.warning(
                        f"[HyperliquidWsMapper] Skipping non-dict item in public trades list: "
                        f"{trade_dict_item!r}"
                    )
                    continue
                try:
                    validated_trade = HyperliquidRawWsTradeEvent.model_validate(trade_dict_item)
                except Exception as e:
                    logger.warning(
                        f"[HyperliquidWsMapper] Invalid raw public trade event (dropped): {e} | "
                        f"Data: {trade_dict_item!r}"
                    )
                    continue  # Skip this trade, process others

                try:
                    side = HyperliquidOrderMapper.map_side_to_internal(validated_trade.side)
                    price = Decimal(validated_trade.px)
                    quantity = Decimal(validated_trade.sz)
                    executed_at = datetime.fromtimestamp(validated_trade.time / 1000, tz=UTC)

                    details = HyperliquidTradeDetails(
                        trade_hash=validated_trade.hash,
                        liquidation_mark_px=None,
                        start_position=None,
                        dir=None,
                    )

                    trade_obj = Trade(
                        id=validated_trade.hash,
                        symbol=validated_trade.coin,
                        executed_at=executed_at,
                        side=side,
                        order_id="UNKNOWN_PUBLIC_TRADE",
                        exchange=ExchangeName.HYPERLIQUID.value,
                        client_order_id=None,
                        price=price,
                        quantity=quantity,
                        fee=Decimal("0"),
                        fee_asset=None,
                        is_maker=None,
                        hl_details=details,
                    )
                    trades.append(trade_obj)
                except (ValueError, TypeError, Exception) as e:
                    logger.warning(
                        f"[HyperliquidWsMapper] Error parsing validated public trade event: {e}. "
                        f"Data: {validated_trade.model_dump()!r}"
                    )
                    # Continue to process other trades in the list
        return trades

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
                    timestamp=datetime.fromtimestamp(validated.time / 1000, tz=UTC),
                )
            except Exception as e:
                logger.warning(f"[Hyperliquid] Error parsing validated order book event: {e}")
        return None

    @staticmethod
    def parse_order_update_message(
        event_order_payload: dict[str, Any],
        logger: logging.Logger,
    ) -> Order | None:
        """
        Parse an order update payload from a WebSocket user event.
        Validates the payload using HyperliquidRawOrder and transforms it using HyperliquidOrderMapper.

        Args:
            event_order_payload: The raw dictionary payload for the order, extracted from
                                 the 'data' field of a validated HyperliquidRawWsOrderUpdate event.
            logger: Logger for warnings/errors.
        Returns:
            Order object or None if parsing/transformation fails.
        """
        try:
            # Validate the raw order payload (which is event_order_payload here)
            raw_order_obj = HyperliquidRawOrder.model_validate(event_order_payload)
        except Exception as e:
            logger.warning(
                f"[HyperliquidWsMapper] Invalid order data in WS order update (dropped): {e} | "
                f"Data: {event_order_payload!r}"
            )
            return None

        try:
            # Transform using the robust HyperliquidOrderMapper.
            # Trigger info is typically not part of WS order update payload in this flat structure.
            internal_order = HyperliquidOrderMapper.transform_raw_order_to_internal(
                raw_order_obj, trigger=None
            )
            return internal_order
        except Exception as e:
            logger.error(
                f"[HyperliquidWsMapper] Error transforming raw order object from WS event: {e}. "
                f"Raw: {raw_order_obj.model_dump()!r}",
                exc_info=True,
            )
            return None

    def map_order_update(self, payload: dict[str, Any], timestamp: datetime) -> Order | None:
        """
        Parse an order update payload from a WebSocket user event.
        Validates the payload using HyperliquidRawOrder and transforms it using
        HyperliquidOrderMapper.

        Args:
            payload: The raw dictionary payload for the order, extracted from
                     the 'data' field of a validated HyperliquidRawWsOrderUpdate event.
            timestamp: The timestamp of the event.
        Returns:
            Order object or None if parsing/transformation fails.
        """
        try:
            # Validate the raw order payload (which is payload here)
            raw_order_obj = HyperliquidRawOrder.model_validate(payload)
        except Exception as e:
            logger.warning(
                f"[HyperliquidWsMapper] Invalid order data in WS order update (dropped): {e} | "
                f"Data: {payload!r}"
            )
            return None

        try:
            # Transform using the robust HyperliquidOrderMapper.
            # Trigger info is typically not part of WS order update payload in this flat structure.
            internal_order = HyperliquidOrderMapper.transform_raw_order_to_internal(
                raw_order_obj, trigger=None
            )
            return internal_order
        except Exception as e:
            logger.error(
                f"[HyperliquidWsMapper] Error transforming raw order object from WS event: {e}. "
                f"Raw: {raw_order_obj.model_dump()!r}",
                exc_info=True,
            )
            return None
