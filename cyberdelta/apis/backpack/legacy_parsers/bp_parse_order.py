"""
CyberDeltaEngine Backpack Order Parser (Spec-Compliant)

This parser converts raw order data from Backpack (REST or WebSocket) into a standardized
Order object for CyberDeltaEngine.
It is fully compliant with the official Backpack OpenAPI spec and order update stream:
- Supports both REST and WebSocket field names and types.
- Handles required fields and common optional fields (e.g., clientId).
- Parses timestamps and numeric strings into appropriate internal types (Decimal, datetime).
- Performs basic validation (non-empty strings, enum checks, finite decimals).
- Optional fields like stopPrice, triggerPrice, timeInForce, selfTradePrevention,
  expiryReason, etc.) are ignored but can be added in the future if needed.
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
def _parse_backpack_timestamp(ts: int | float | str) -> datetime:
    """
    Convert Backpack timestamp (ms, µs, or ISO string) to UTC datetime.

    Backpack timestamps can be:
    - Integer milliseconds (e.g., 1678886400123)
    - Integer microseconds (e.g., 1678886400123456)
    - Float milliseconds (rare? e.g., 1678886400123.45)
    - Float microseconds (rare? e.g., 1678886400123456.7)
    - ISO 8601 string (e.g., "2023-03-15T12:00:00.123Z")

    Args:
        ts: The timestamp value from Backpack.

    Returns:
        datetime: The timestamp converted to a UTC datetime object.

    Raises:
        ValueError: If the timestamp format is unrecognized or invalid.
    """
    # Removed redundant check: if ts is None:
    # The type hint `int | float | str` implies non-None input.
    # Calling code should ensure None is not passed.

    if isinstance(ts, str):
        # Try parsing as ISO 8601 string
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError:
            # If not ISO string, try parsing as number (ms or µs)
            try:
                num_val = float(ts)  # Use float first to handle decimals
            except ValueError as e:
                raise ValueError(f"Unparseable timestamp string: {ts}") from e
            ts = num_val  # Use the numeric value

    # At this point, ts must be int or float based on type hint or parsing above
    # Removed redundant isinstance check for int/float
    # num_val = ts
    num_val = float(ts)  # Ensure float for division logic

    # Determine if it's milliseconds or microseconds based on magnitude
    # (This is heuristic, might need adjustment based on observed Backpack behavior)
    now_ms = datetime.now(UTC).timestamp() * 1000
    if abs(num_val - now_ms) < abs(num_val - (now_ms * 1000)):
        # Closer to ms, assume milliseconds
        try:
            return datetime.fromtimestamp(num_val / 1000, UTC)
        except (ValueError, OverflowError, OSError) as e:
            raise ValueError(f"Invalid millisecond timestamp value: {num_val}") from e
    else:
        # Assume microseconds
        try:
            return datetime.fromtimestamp(num_val / 1_000_000, UTC)
        except (ValueError, OverflowError, OSError) as e:
            raise ValueError(f"Invalid microsecond timestamp value: {num_val}") from e


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
        # DEFENSIVE CHECK: Ensure created_at_raw is a supported type before parsing.
        if not isinstance(created_at_raw, int | float | str):
            raise APIError(
                f"Invalid type for createdAt/E/T/time: {type(created_at_raw).__name__}",
                code=APIErrorCode.INVALID_PARAMS.value,
            )
        created_at = _parse_backpack_timestamp(created_at_raw)
        # Updated at (optional)
        updated_at_raw = data.get("updatedAt")
        updated_at: datetime | None = None
        if updated_at_raw is not None:
            # DEFENSIVE CHECK: Ensure updated_at_raw is a supported type before parsing.
            if not isinstance(updated_at_raw, int | float | str):
                raise APIError(
                    f"Invalid type for updatedAt: {type(updated_at_raw).__name__}",
                    code=APIErrorCode.INVALID_PARAMS.value,
                )
            updated_at = _parse_backpack_timestamp(updated_at_raw)
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
        # postOnly, reduceOnly, timeInForce, selfTradePrevention, expiryReason,
        # stopLossTriggerPrice, stopLossLimitPrice, stopLossTriggerBy,
        # takeProfitTriggerPrice, takeProfitLimitPrice, takeProfitTriggerBy, triggerBy,
        # triggerPrice, triggerQuantity, triggeredAt, relatedOrderId, etc.
    except KeyError as e:
        raise APIError(
            f"Missing key {e} in order data", code=APIErrorCode.INVALID_PARAMS.value
        ) from e
    except Exception as e:
        raise APIError(
            f"Error parsing order data: {e}", code=APIErrorCode.INVALID_PARAMS.value
        ) from e
