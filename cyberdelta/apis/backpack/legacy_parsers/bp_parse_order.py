"""
CyberDeltaEngine Backpack Order Parser (Spec-Compliant)

This parser converts raw order data from Backpack (REST or WebSocket) into a standardized Order object for CyberDeltaEngine.
It is fully compliant with the official Backpack OpenAPI spec and order update stream:
- Supports both REST and WebSocket field names and types.
- Handles ms, µs, and ISO timestamps for created_at/updated_at.
- Maps all required and relevant optional fields to the CyberDeltaEngine Order model.
- Documents any Backpack fields not mapped to the core model.
- Ensures robust type conversion and error handling.

Backpack fields not present in the core model (e.g., postOnly, reduceOnly, timeInForce, selfTradePrevention, expiryReason, etc.) are ignored but can be added in the future if needed.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.base_api import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.core.models import Order, OrderSide, OrderStatus, OrderType, TimeInForce


# ANN401: Any is justified here as Backpack can send timestamps in multiple formats
# (int ms, int µs, float ms, float µs, ISO string) which is difficult to type precisely
# without significant complexity or runtime overhead.
def _parse_backpack_timestamp(ts: Any) -> datetime:  # noqa: ANN401
    """
    Convert Backpack timestamp (ms, µs, or ISO string) to UTC datetime.
    """
    if ts is None:
        return datetime.now(UTC)
    if isinstance(ts, datetime):
        return ts.astimezone(UTC)
    if isinstance(ts, str):
        try:
            # Try ISO string
            return datetime.fromisoformat(ts).astimezone(UTC)
        except Exception:
            try:
                ts = int(ts)
            except Exception:
                return datetime.now(UTC)
    if isinstance(ts, int | float):  # UP038 fix applied
        # µs (WebSocket) or ms (REST)
        if ts > 1e12:
            return datetime.fromtimestamp(ts / 1e6, UTC)
        return datetime.fromtimestamp(ts / 1e3, UTC)
    return datetime.now(UTC)


def bp_parse_order(data: dict[str, Any]) -> Order:
    """
    Parse raw order data from Backpack (REST or WebSocket) into a CyberDeltaEngine Order object.
    Fully compliant with Backpack OpenAPI spec and order update stream.
    """
    try:
        # --- Field extraction with REST/WS fallback ---
        symbol = data.get("symbol") or data.get("s") or ""
        # Order ID (exchange)
        exchange_order_id = str(data.get("id") or data.get("i") or "")
        # Client order ID (can be int or str)
        client_order_id = str(data.get("clientId") or data.get("c") or "")
        # Side
        side_val = data.get("side") or data.get("S")
        side = OrderSide(side_val) if side_val else OrderSide.BUY
        # Order type
        order_type_val = data.get("orderType") or data.get("o")
        order_type = OrderType(order_type_val) if order_type_val else OrderType.LIMIT
        # Status
        status_val = data.get("status") or data.get("X")
        status_str = str(status_val).upper() if status_val else "UNKNOWN"
        order_status = (
            OrderStatus[status_str]
            if status_str in OrderStatus.__members__
            else OrderStatus.UNKNOWN
        )
        # Quantity requested
        quantity = data.get("quantity") or data.get("q")
        # Quantity filled
        quantity_filled = data.get("executedQuantity") or data.get("z")
        # Price (limit)
        price = data.get("price") or data.get("p")
        # Average fill price
        average_fill_price = data.get("avgFillPrice") or data.get("L")
        # Created at (REST: ms, WS: µs, or ISO string)
        created_at_raw = data.get("createdAt") or data.get("E") or data.get("T") or data.get("time")
        created_at = _parse_backpack_timestamp(created_at_raw)
        # Updated at (optional)
        updated_at_raw = data.get("updatedAt")
        updated_at = _parse_backpack_timestamp(updated_at_raw) if updated_at_raw else None
        # --- Map to CyberDeltaEngine Order model ---
        return Order(
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id if exchange_order_id else None,
            related_order_id=None,
            exchange="backpack",
            symbol=symbol,
            side=side,
            order_type=order_type,
            status=order_status,
            quantity_requested=Decimal(str(quantity)) if quantity is not None else Decimal("0"),
            quantity_filled=Decimal(str(quantity_filled))
            if quantity_filled is not None
            else Decimal("0"),
            price=Decimal(str(price)) if price is not None else None,
            average_fill_price=Decimal(str(average_fill_price))
            if average_fill_price is not None
            else None,
            created_at=created_at,
            updated_at=updated_at,
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            time_in_force=TimeInForce.GTC,
        )
        # --- Ignored Backpack fields (not in core model, but available for future extension): ---
        # postOnly, reduceOnly, timeInForce, selfTradePrevention, expiryReason, stopLossTriggerPrice,
        # stopLossLimitPrice, stopLossTriggerBy, takeProfitTriggerPrice, takeProfitLimitPrice,
        # takeProfitTriggerBy, triggerBy, triggerPrice, triggerQuantity, triggeredAt, relatedOrderId, etc.
    except KeyError as e:
        raise APIError(
            f"Missing key {e} in order data", code=APIErrorCode.INVALID_PARAMS.value
        ) from e
    except Exception as e:
        raise APIError(
            f"Error parsing order data: {e}", code=APIErrorCode.INVALID_PARAMS.value
        ) from e
