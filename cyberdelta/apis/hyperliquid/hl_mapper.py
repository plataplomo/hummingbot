"""
HyperliquidOrderMapper: Maps validated Hyperliquid raw order models to CyberDeltaEngine's internal Order model.
"""

import logging
from decimal import Decimal
from typing import Any, cast

from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import Order
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class HyperliquidOrderMapper:
    """
    Utility for transforming Hyperliquid raw order models to CyberDeltaEngine internal Order model.
    """

    @staticmethod
    def map_side_to_internal(hl_side: str) -> OrderSide:
        if hl_side == "B":
            return OrderSide.BUY
        elif hl_side == "A":
            return OrderSide.SELL
        logger.warning(
            f"[HyperliquidOrderMapper] Unknown order side '{hl_side}', defaulting to BUY."
        )
        return OrderSide.BUY

    @staticmethod
    def map_status_to_internal(hl_status: str) -> OrderStatus:
        status_map = {
            "open": OrderStatus.OPEN,
            "filled": OrderStatus.FILLED,
            "cancelled": OrderStatus.CANCELED,
            "canceled": OrderStatus.CANCELED,
            "rejected": OrderStatus.REJECTED,
            # Add more mappings as needed
        }
        return status_map.get(hl_status.lower(), OrderStatus.UNKNOWN)

    @staticmethod
    def map_type_to_internal(
        order_type: dict[str, Any], trigger: HyperliquidRawTriggerInfo | None
    ) -> OrderType:
        # Hyperliquid uses nested dicts for orderType, e.g. {"limit": {"tif": "Gtc"}}, {"market": {}}
        if "limit" in order_type:
            if trigger:
                if getattr(trigger, "tpsl", None) == "sl":
                    return OrderType.STOP_LIMIT
                elif getattr(trigger, "tpsl", None) == "tp":
                    return OrderType.TAKE_PROFIT_LIMIT
            return OrderType.LIMIT
        elif "market" in order_type:
            if trigger:
                if getattr(trigger, "tpsl", None) == "sl":
                    return OrderType.STOP_MARKET
                elif getattr(trigger, "tpsl", None) == "tp":
                    return OrderType.TAKE_PROFIT_MARKET
            return OrderType.MARKET
        logger.warning(
            f"[HyperliquidOrderMapper] Unknown orderType structure: {order_type}. "
            "Defaulting to LIMIT."
        )
        return OrderType.LIMIT

    @staticmethod
    def map_time_in_force(order_type: dict[str, Any]) -> TimeInForce:
        # Only limit orders have TIF in HL
        if "limit" in order_type and isinstance(order_type["limit"], dict):
            limit_dict = cast(dict[str, Any], order_type["limit"])
            tif_val = limit_dict.get("tif", "")
            tif_str = str(tif_val)
            tif = tif_str.upper()
            if tif == "GTC":
                return TimeInForce.GTC
            elif tif == "IOC":
                return TimeInForce.IOC
            elif tif == "ALO":
                return TimeInForce.ALO
        return TimeInForce.GTC

    @staticmethod
    def transform_raw_order_to_internal(
        raw: HyperliquidRawOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> Order:
        # Defensive parsing and mapping
        side = HyperliquidOrderMapper.map_side_to_internal(raw.side)
        order_type = HyperliquidOrderMapper.map_type_to_internal(raw.order_type, trigger)
        status = HyperliquidOrderMapper.map_status_to_internal(raw.status)
        time_in_force = HyperliquidOrderMapper.map_time_in_force(raw.order_type)

        quantity_requested = parse_decimal_value(raw.sz, allow_none=False, field_name="sz")
        if quantity_requested is None:
            raise ValueError("quantity_requested (sz) is required and could not be parsed.")
        remaining_sz = parse_decimal_value(
            str(raw.remaining_sz), allow_none=True, field_name="remainingSz"
        )
        if remaining_sz is None:
            remaining_sz = Decimal("0")
        quantity_filled = quantity_requested - remaining_sz
        price = parse_decimal_value(str(raw.limit_px), allow_none=True, field_name="limitPx")
        created_at = parse_datetime_utc(raw.timestamp, field_name="timestamp")
        if created_at is None:
            raise ValueError("created_at (timestamp) is required and could not be parsed.")
        updated_at = parse_datetime_utc(raw.status_timestamp, field_name="statusTimestamp")

        # Trigger/stop logic
        stop_price = None
        trigger_by = None
        if trigger:
            stop_price = parse_decimal_value(
                str(getattr(trigger, "trigger_px", "")), allow_none=True, field_name="triggerPx"
            )
            # Hyperliquid does not specify trigger_by (Mark/Last/Index), so leave as None or infer if possible

        return Order(
            client_order_id=raw.cloid or str(raw.oid),
            exchange_order_id=str(raw.oid),
            related_order_id=None,
            exchange="hyperliquid",
            symbol=raw.asset,
            side=side,
            order_type=order_type,
            status=status,
            quantity_requested=quantity_requested,
            quantity_filled=quantity_filled,
            executed_quote_quantity=None,
            price=price,
            stop_price=stop_price,
            average_fill_price=None,  # HL does not provide this in open order
            trigger_by=trigger_by,
            time_in_force=time_in_force,
            reduce_only=raw.reduce_only,
            post_only=(time_in_force == TimeInForce.ALO),
            self_trade_prevention=None,
            created_at=created_at,
            updated_at=updated_at,
            triggered_at=None,
            expiry_reason=None,
            origin=None,
            strategy_name=None,
            signal_id=None,
            trades=[],
        )
