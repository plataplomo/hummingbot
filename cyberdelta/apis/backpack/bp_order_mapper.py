import logging
from decimal import Decimal

from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    OrderExpiryReason,
    OrderSide,
    OrderStatus,
    OrderType,
    OrderUpdateOrigin,
    SelfTradePrevention,
    TimeInForce,
    TriggerType,
)
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class BackpackOrderMapper:
    """
    Utility for transforming Backpack raw order/event models to CyberDeltaEngine internal models.

    - Maps Backpack string enums to internal enums (OrderSide, OrderStatus, etc.).
    - Handles defensive parsing and validation of all fields.
    - Used by BackpackAPI for all order-related transformations.
    """

    @staticmethod
    def map_side_to_internal(bp_side: str) -> OrderSide:
        side_lower = bp_side.lower() if bp_side else ""
        if side_lower in ("buy", "bid"):
            return OrderSide.BUY
        elif side_lower in ("sell", "ask"):
            return OrderSide.SELL
        logger.warning(f"[BackpackOrderMapper] Unknown order side '{bp_side}', defaulting to BUY.")
        return OrderSide.BUY

    @staticmethod
    def map_status_to_internal(bp_status: str) -> OrderStatus:
        status_upper = (bp_status or "").upper()
        mapping = {
            "NEW": OrderStatus.NEW,
            "OPEN": OrderStatus.OPEN,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELLED": OrderStatus.CANCELED,
            "EXPIRED": OrderStatus.EXPIRED,
            "REJECTED": OrderStatus.REJECTED,
            "TRIGGER_PENDING": OrderStatus.TRIGGER_PENDING,
            "FAILED": OrderStatus.FAILED,
        }
        if status_upper in mapping:
            return mapping[status_upper]
        logger.warning(
            f"[BackpackOrderMapper] Unknown order status '{bp_status}', mapping to UNKNOWN."
        )
        return OrderStatus.UNKNOWN

    @staticmethod
    def map_type_to_internal(bp_type: str) -> OrderType:
        type_upper = (bp_type or "").upper()
        mapping = {
            "LIMIT": OrderType.LIMIT,
            "MARKET": OrderType.MARKET,
            "STOP_MARKET": OrderType.STOP_MARKET,
            "STOP_LIMIT": OrderType.STOP_LIMIT,
            "TAKE_PROFIT_MARKET": OrderType.TAKE_PROFIT_MARKET,
            "TAKE_PROFIT_LIMIT": OrderType.TAKE_PROFIT_LIMIT,
        }
        if type_upper in mapping:
            return mapping[type_upper]
        logger.warning(
            f"[BackpackOrderMapper] Unknown order type '{bp_type}', defaulting to LIMIT."
        )
        return OrderType.LIMIT

    @staticmethod
    def map_tif_to_internal(bp_tif: str | None) -> TimeInForce:
        if not bp_tif:
            return TimeInForce.GTC
        try:
            return TimeInForce(bp_tif.upper())
        except Exception:
            logger.warning(f"[BackpackOrderMapper] Unknown TIF '{bp_tif}', defaulting to GTC.")
            return TimeInForce.GTC

    @staticmethod
    def map_trigger_by_to_internal(trigger_by: str | None) -> TriggerType | None:
        if not trigger_by:
            return None
        try:
            return TriggerType(trigger_by)
        except Exception:
            logger.warning(
                f"[BackpackOrderMapper] Unknown trigger_by '{trigger_by}', returning None."
            )
            return None

    @staticmethod
    def map_stp_to_internal(stp: str | None) -> SelfTradePrevention | None:
        if not stp:
            return None
        try:
            return SelfTradePrevention(stp)
        except Exception:
            logger.warning(
                f"[BackpackOrderMapper] Unknown self_trade_prevention '{stp}', returning None."
            )
            return None

    @staticmethod
    def map_expiry_reason_to_internal(reason: str | None) -> OrderExpiryReason | None:
        if not reason:
            return None
        try:
            return OrderExpiryReason(reason)
        except Exception:
            logger.warning(
                f"[BackpackOrderMapper] Unknown expiry_reason '{reason}', returning None."
            )
            return None

    @staticmethod
    def map_origin_to_internal(origin: str | None) -> OrderUpdateOrigin | None:
        if not origin:
            return None
        try:
            return OrderUpdateOrigin(origin)
        except Exception:
            logger.warning(f"[BackpackOrderMapper] Unknown origin '{origin}', returning None.")
            return None

    @staticmethod
    def transform_raw_order_to_internal(raw: BackpackRawOrder) -> Order:
        # Defensive: ensure required fields are present and valid
        parsed_quantity = parse_decimal_value(raw.quantity, allow_none=False)
        if parsed_quantity is None:
            raise ValueError("quantity missing/invalid in BackpackRawOrder")
        parsed_created_at = parse_datetime_utc(raw.createdAt)
        if parsed_created_at is None:
            raise ValueError("createdAt missing/invalid in BackpackRawOrder")
        # Optional fields
        parsed_quantity_filled = parse_decimal_value(raw.executedQuantity) or Decimal("0.0")
        parsed_executed_quote_quantity = parse_decimal_value(raw.executedQuoteQuantity)
        parsed_price = parse_decimal_value(raw.price)
        parsed_stop_price = parse_decimal_value(raw.triggerPrice)
        parsed_avg_fill_price = parse_decimal_value(raw.avgFillPrice)
        return Order(
            client_order_id=raw.clientId or "",
            exchange_order_id=raw.id,
            related_order_id=raw.relatedOrderId,
            exchange=ExchangeName.BACKPACK,
            symbol=raw.symbol,
            side=BackpackOrderMapper.map_side_to_internal(raw.side),
            order_type=BackpackOrderMapper.map_type_to_internal(raw.orderType),
            status=BackpackOrderMapper.map_status_to_internal(raw.status),
            quantity_requested=parsed_quantity,
            quantity_filled=parsed_quantity_filled,
            executed_quote_quantity=parsed_executed_quote_quantity,
            price=parsed_price,
            stop_price=parsed_stop_price,
            average_fill_price=parsed_avg_fill_price,
            trigger_by=BackpackOrderMapper.map_trigger_by_to_internal(raw.triggerBy),
            time_in_force=BackpackOrderMapper.map_tif_to_internal(raw.timeInForce),
            reduce_only=raw.reduceOnly or False,
            post_only=raw.postOnly or False,
            self_trade_prevention=BackpackOrderMapper.map_stp_to_internal(raw.selfTradePrevention),
            created_at=parsed_created_at,
            updated_at=parse_datetime_utc(raw.updatedAt),
            triggered_at=parse_datetime_utc(raw.triggeredAt),
            expiry_reason=BackpackOrderMapper.map_expiry_reason_to_internal(raw.expiryReason),
            origin=BackpackOrderMapper.map_origin_to_internal(raw.origin),
            # The following fields are not present in BackpackRawOrder; set to None/[] explicitly.
            strategy_name=None,  # Not present in BackpackRawOrder
            signal_id=None,  # Not present in BackpackRawOrder
            trades=[],  # Not present in BackpackRawOrder
        )
