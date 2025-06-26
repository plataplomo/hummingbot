"""CyberDeltaEngine: Backpack Trading Data Mapper.

---------------------------------------------

This module provides the BackpackTradingDataMapper class for transforming
Backpack Raw Trading Data models into Internal Domain Models.

Responsibilities:
- Transform Raw Orders to Internal Order models
- Transform Raw Order responses from trading operations to Internal Order models
- Transform WebSocket Order Update events to Internal Order models

All transformation methods follow the standard pattern:
- Take a validated Raw Pydantic Model as primary input
- Return fully populated Internal Domain Model with Details slots
- Handle type conversions, enum mapping, and error cases
- Raise TransformationError for unmappable data
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder, BackpackRawOrderUpdate
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import BackpackOrderDetails
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackTradingDataMapper:
    """Domain-focused mapper for Backpack trading data transformations.

    This class contains static methods for transforming validated Backpack Raw models
    related to trading operations into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(bp_side: str) -> OrderSide:
        """Map a Backpack order side string to internal OrderSide enum.

        Args:
            bp_side: Raw side string from Backpack ("Buy", "Sell", "Bid", "Ask")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            TransformationError: If side cannot be mapped

        """
        side_lower = bp_side.lower() if bp_side else ""
        if side_lower in ("buy", "bid"):
            return OrderSide.BUY
        elif side_lower in ("sell", "ask"):
            return OrderSide.SELL

        raise TransformationError(f"Unknown Backpack order side: '{bp_side}'")

    @staticmethod
    def _map_status_to_internal(bp_status: str) -> OrderStatus:
        """Map a Backpack order status string to internal OrderStatus enum.

        Args:
            bp_status: Raw status string from Backpack

        Returns:
            OrderStatus: Mapped internal enum value

        """
        status_map = {
            "new": OrderStatus.OPEN,
            "filled": OrderStatus.FILLED,
            "cancelled": OrderStatus.CANCELED,
            "canceled": OrderStatus.CANCELED,
            "rejected": OrderStatus.REJECTED,
            "partially_filled": OrderStatus.PARTIALLY_FILLED,
            "pending": OrderStatus.OPEN,
            "trigger_pending": OrderStatus.TRIGGER_PENDING,
            "triggerpending": OrderStatus.TRIGGER_PENDING,
        }
        return status_map.get(bp_status.lower(), OrderStatus.UNKNOWN)

    @staticmethod
    def _map_type_to_internal(
        bp_type: str,
        trigger_price: str | None = None,
        raw_order: BackpackRawOrder | None = None,
    ) -> OrderType:
        """Map a Backpack order type string to internal OrderType enum.

        Args:
            bp_type: Raw order type string from Backpack
            trigger_price: Trigger price if present (indicates stop/take profit order)
            raw_order: Raw order object to check for specific trigger price fields

        Returns:
            OrderType: Mapped internal enum value

        """
        bp_type_lower = bp_type.lower()

        # If there's a trigger price, it indicates this was a stop/take profit order
        if trigger_price:
            # Check if this is a take profit order by examining specific fields
            if raw_order and raw_order.takeProfitTriggerPrice:
                if bp_type_lower == "market":
                    return OrderType.TAKE_PROFIT_MARKET
                elif bp_type_lower == "limit":
                    return OrderType.TAKE_PROFIT_LIMIT
            # Otherwise it's a stop loss order
            else:
                if bp_type_lower == "market":
                    return OrderType.STOP_MARKET
                elif bp_type_lower == "limit":
                    return OrderType.STOP_LIMIT

        type_map = {
            "limit": OrderType.LIMIT,
            "market": OrderType.MARKET,
            "stop": OrderType.STOP_MARKET,
            "stop_limit": OrderType.STOP_LIMIT,
            "trailing_stop": OrderType.STOP_MARKET,
            "take_profit": OrderType.LIMIT,
        }
        return type_map.get(bp_type_lower, OrderType.LIMIT)

    @staticmethod
    def _map_time_in_force(bp_tif: str) -> TimeInForce:
        """Map a Backpack time in force string to internal TimeInForce enum.

        Args:
            bp_tif: Raw time in force string from Backpack

        Returns:
            TimeInForce: Mapped internal enum value

        """
        tif_map = {
            "gtc": TimeInForce.GTC,
            "ioc": TimeInForce.IOC,
            "fok": TimeInForce.FOK,
        }
        return tif_map.get(bp_tif.lower(), TimeInForce.GTC)

    @staticmethod
    def transform_order_data_to_internal(
        order_id: str,
        symbol: str,
        side: str,
        order_type: str,
        status: str,
        quantity: str,
        price: str | None = None,
        client_order_id: str | None = None,
        time_in_force: str | None = None,
        created_at: str | None = None,
        updated_at: str | None = None,
    ) -> Order:
        """Transform Backpack order data to an Internal Order model.

        Args:
            order_id: Order ID
            symbol: Trading symbol
            side: Order side string
            order_type: Order type string
            status: Order status string
            quantity: Order quantity as string
            price: Order price as string (optional)
            client_order_id: Client order ID (optional)
            time_in_force: Time in force string (optional)
            created_at: Creation timestamp (optional)
            updated_at: Update timestamp (optional)

        Returns:
            Order: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Map enums
            mapped_side = BackpackTradingDataMapper._map_side_to_internal(side)
            mapped_type = BackpackTradingDataMapper._map_type_to_internal(order_type, None, None)
            mapped_status = BackpackTradingDataMapper._map_status_to_internal(status)
            mapped_tif = BackpackTradingDataMapper._map_time_in_force(time_in_force or "gtc")

            # Parse quantities
            quantity_requested = parse_decimal_value(
                quantity,
                allow_none=False,
                field_name="quantity",
            )
            if quantity_requested is None:
                raise TransformationError("quantity_requested is required")

            # For now, assume no filled quantity available in basic order data
            quantity_filled = Decimal("0")

            # Parse price
            order_price = None
            if price:
                order_price = parse_decimal_value(price, allow_none=True, field_name="price")

            # Parse timestamps
            created_timestamp = None
            if created_at:
                created_timestamp = parse_datetime_utc(created_at, field_name="created_at")

            updated_timestamp = None
            if updated_at:
                updated_timestamp = parse_datetime_utc(updated_at, field_name="updated_at")

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            order_data: dict[str, Any] = {
                "exchange_order_id": order_id,
                "symbol": symbol,
                "side": mapped_side.value,
                "order_type": mapped_type.value,
                "status": mapped_status.value,
                "quantity_requested": str(quantity_requested),
                "quantity_filled": str(quantity_filled),
                "price": str(order_price) if order_price is not None else None,
                "time_in_force": mapped_tif.value,
                "exchange": ExchangeName.BACKPACK.value,
                "client_order_id": client_order_id or str(uuid.uuid4()),
                "created_at": (created_timestamp or datetime.now(UTC)).isoformat(),
                "updated_at": updated_timestamp.isoformat() if updated_timestamp else None,
                "triggered_at": None,
                "strategy_name": None,
                "signal_id": None,
                "bp_details": None,
                "hl_details": None,
            }

            return secure_transform(
                data=order_data,
                model_class=Order,
                context="backpack_simple_order_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform Backpack order data to Order: {e}",
            ) from e

    @staticmethod
    def _parse_order_quantities(raw_order: BackpackRawOrder) -> tuple[Decimal, Decimal]:
        """Parse and validate order quantities."""
        quantity_requested = parse_decimal_value(
            raw_order.quantity,
            allow_none=True,
            field_name="quantity",
        )

        # For stop orders, quantity might be 0 and the actual quantity is in triggerQuantity
        if quantity_requested is None or quantity_requested == Decimal("0"):
            if raw_order.triggerQuantity:
                quantity_requested = parse_decimal_value(
                    raw_order.triggerQuantity,
                    allow_none=False,
                    field_name="triggerQuantity",
                )

        if quantity_requested is None or quantity_requested <= Decimal("0"):
            raise TransformationError("quantity_requested is required and must be > 0")

        quantity_filled = parse_decimal_value(
            raw_order.executedQuantity,
            allow_none=True,
            field_name="executedQuantity",
        ) or Decimal("0")

        return quantity_requested, quantity_filled

    @staticmethod
    def _parse_order_price(price_value: str | None, field_name: str) -> Decimal | None:
        """Parse order price field, returning None for zero or invalid values."""
        if not price_value or price_value == "0":
            return None

        parsed_price = parse_decimal_value(
            price_value,
            allow_none=True,
            field_name=field_name,
        )
        return parsed_price if parsed_price is not None and parsed_price > 0 else None

    @staticmethod
    def _calculate_average_fill_price(
        raw_order: BackpackRawOrder, quantity_filled: Decimal
    ) -> Decimal | None:
        """Calculate average fill price for filled orders.

        For Backpack orders, if avgFillPrice is not provided but order has fills,
        calculate it from executedQuoteQuantity / executedQuantity.

        Args:
            raw_order: Raw order data
            quantity_filled: Already parsed quantity filled

        Returns:
            Average fill price or None if cannot be determined
        """
        # First try to use the provided avgFillPrice
        avg_fill_price = BackpackTradingDataMapper._parse_order_price(
            raw_order.avgFillPrice, "avgFillPrice"
        )

        if avg_fill_price is not None:
            logger.debug(
                "using_provided_avg_fill_price",
                action="calculate_fill_price",
                avg_fill_price=avg_fill_price,
                order_id=raw_order.id,
                message=f"Using provided avgFillPrice {avg_fill_price} for order {raw_order.id}",
            )
            return avg_fill_price

        # If no avgFillPrice but order has fills, calculate from quote quantity
        if quantity_filled > 0 and raw_order.executedQuoteQuantity:
            executed_quote = parse_decimal_value(
                raw_order.executedQuoteQuantity, allow_none=True, field_name="executedQuoteQuantity"
            )

            if executed_quote is not None and executed_quote > 0:
                calculated_avg_price = executed_quote / quantity_filled
                return calculated_avg_price
            else:
                logger.warning(
                    f"Could not calculate average fill price for order {raw_order.id}: "
                    f"executed_quote={executed_quote}, quantity_filled={quantity_filled}"
                )

        logger.warning(
            f"No average fill price available for order {raw_order.id}: "
            f"avgFillPrice={raw_order.avgFillPrice}, quantity_filled={quantity_filled}, "
            f"executedQuoteQuantity={raw_order.executedQuoteQuantity}"
        )
        return None

    @staticmethod
    def _parse_order_timestamps(
        raw_order: BackpackRawOrder,
    ) -> tuple[datetime, datetime | None, datetime | None]:
        """Parse order timestamps."""
        created_timestamp = parse_datetime_utc(raw_order.createdAt, field_name="createdAt")
        if created_timestamp is None:
            raise TransformationError("createdAt is required")

        updated_timestamp = None
        if raw_order.updatedAt:
            updated_timestamp = parse_datetime_utc(raw_order.updatedAt, field_name="updatedAt")

        triggered_timestamp = None
        if raw_order.triggeredAt:
            triggered_timestamp = parse_datetime_utc(
                raw_order.triggeredAt,
                field_name="triggeredAt",
            )

        return created_timestamp, updated_timestamp, triggered_timestamp

    @staticmethod
    def transform_raw_order_to_internal(raw_order: BackpackRawOrder) -> Order:
        """Transform a BackpackRawOrder to an Internal Order model.

        Args:
            raw_order: Validated raw order data from Backpack

        Returns:
            Order: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Map enums
            mapped_side = BackpackTradingDataMapper._map_side_to_internal(raw_order.side)
            mapped_type = BackpackTradingDataMapper._map_type_to_internal(
                raw_order.orderType, raw_order.triggerPrice, raw_order
            )
            mapped_status = BackpackTradingDataMapper._map_status_to_internal(raw_order.status)
            mapped_tif = BackpackTradingDataMapper._map_time_in_force(
                raw_order.timeInForce or "gtc",
            )

            # Parse quantities
            quantity_requested, quantity_filled = BackpackTradingDataMapper._parse_order_quantities(
                raw_order,
            )

            # Parse prices
            order_price = BackpackTradingDataMapper._parse_order_price(raw_order.price, "price")
            stop_price = BackpackTradingDataMapper._parse_order_price(
                raw_order.triggerPrice,
                "triggerPrice",
            )
            average_fill_price = BackpackTradingDataMapper._calculate_average_fill_price(
                raw_order, quantity_filled
            )

            # Parse timestamps
            created_timestamp, updated_timestamp, triggered_timestamp = (
                BackpackTradingDataMapper._parse_order_timestamps(raw_order)
            )

            # Create BackpackOrderDetails from raw order data
            bp_details = BackpackOrderDetails(
                executed_quote_quantity=parse_decimal_value(
                    raw_order.executedQuoteQuantity,
                    field_name="executedQuoteQuantity",
                    allow_none=True,
                )
                if raw_order.executedQuoteQuantity
                else None,
                self_trade_prevention=None,  # Can be mapped if needed
                expiry_reason=None,  # Can be mapped if needed
                origin=None,  # Can be mapped if needed
                sl_trigger_price=parse_decimal_value(
                    raw_order.stopLossTriggerPrice,
                    field_name="stopLossTriggerPrice",
                    allow_none=True,
                )
                if raw_order.stopLossTriggerPrice
                else None,
                sl_limit_price=parse_decimal_value(
                    raw_order.stopLossLimitPrice, field_name="stopLossLimitPrice", allow_none=True
                )
                if raw_order.stopLossLimitPrice
                else None,
                sl_trigger_by=None,  # Can be mapped from stopLossTriggerBy if needed
                tp_trigger_price=parse_decimal_value(
                    raw_order.takeProfitTriggerPrice,
                    field_name="takeProfitTriggerPrice",
                    allow_none=True,
                )
                if raw_order.takeProfitTriggerPrice
                else None,
                tp_limit_price=parse_decimal_value(
                    raw_order.takeProfitLimitPrice,
                    field_name="takeProfitLimitPrice",
                    allow_none=True,
                )
                if raw_order.takeProfitLimitPrice
                else None,
                tp_trigger_by=None,  # Can be mapped from takeProfitTriggerBy if needed
                trigger_quantity=parse_decimal_value(
                    raw_order.triggerQuantity, field_name="triggerQuantity", allow_none=True
                )
                if raw_order.triggerQuantity
                else None,
            )

            # Create Order directly with all parameters
            # SECURITY FIX: Use secure_transform instead of direct instantiation
            order_data: dict[str, Any] = {
                "exchange_order_id": raw_order.id,
                "symbol": raw_order.symbol,
                "side": mapped_side.value,
                "order_type": mapped_type.value,
                "status": mapped_status.value,
                "quantity_requested": str(quantity_requested),
                "quantity_filled": str(quantity_filled),
                "price": str(order_price) if order_price is not None else None,
                "stop_price": str(stop_price) if stop_price is not None else None,
                "average_fill_price": str(average_fill_price)
                if average_fill_price is not None
                else None,
                "time_in_force": mapped_tif.value,
                "exchange": ExchangeName.BACKPACK.value,
                "client_order_id": raw_order.clientId or str(uuid.uuid4()),
                "created_at": created_timestamp.isoformat(),
                "updated_at": updated_timestamp.isoformat() if updated_timestamp else None,
                "triggered_at": triggered_timestamp.isoformat() if triggered_timestamp else None,
                "strategy_name": None,
                "signal_id": None,
                "reduce_only": raw_order.reduceOnly or False,
                "post_only": raw_order.postOnly or False,
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=order_data,
                model_class=Order,
                context="backpack_raw_order_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            logger.error(
                "order_transformation_failed",
                action="transform_order",
                order_id=raw_order.id,
                error=str(e),
                message=f"Failed to transform order {raw_order.id}: {e}",
            )
            raise TransformationError(f"Failed to transform BackpackRawOrder to Order: {e}") from e

    @staticmethod
    def transform_ws_order_update_to_internal_order(
        raw_order_update: BackpackRawOrderUpdate,
    ) -> Order:
        """Transform a BackpackRawOrderUpdate (WebSocket order update event) to an Internal Order.

        This method converts WebSocket order update events from Backpack into internal
        Order models, handling all necessary field mappings and type conversions.

        Args:
            raw_order_update: Validated raw order update event data from Backpack WebSocket

        Returns:
            Order: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Map enums
            mapped_side = BackpackTradingDataMapper._map_side_to_internal(raw_order_update.side)
            mapped_type = BackpackTradingDataMapper._map_type_to_internal(
                raw_order_update.order_type, None, None
            )
            mapped_status = BackpackTradingDataMapper._map_status_to_internal(
                raw_order_update.order_status,
            )
            mapped_tif = BackpackTradingDataMapper._map_time_in_force(
                raw_order_update.time_in_force or "gtc",
            )

            # Parse quantities
            if raw_order_update.quantity:
                quantity_requested = parse_decimal_value(
                    raw_order_update.quantity,
                    allow_none=False,
                    field_name="quantity",
                )
                # DEFENSIVE CHECK: Ensure quantity_requested is not None after parsing.
                # Mypy=[unreachable] Ruff=[unreachable]
                if quantity_requested is None:
                    quantity_requested = Decimal("0")
            else:
                quantity_requested = Decimal("0")

            # For WebSocket order updates, we don't have filled quantity info
            quantity_filled = Decimal("0")

            # Parse price
            order_price = None
            if raw_order_update.price:
                order_price = parse_decimal_value(
                    raw_order_update.price,
                    allow_none=True,
                    field_name="price",
                )

            # Parse timestamps
            event_timestamp = None
            if raw_order_update.event_time:
                event_timestamp = parse_datetime_utc(
                    raw_order_update.event_time,
                    field_name="event_time",
                )

            if event_timestamp is None:
                event_timestamp = datetime.now(UTC)

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            order_data: dict[str, Any] = {
                "exchange_order_id": (
                    f"ws_order_{raw_order_update.event_type}_{int(event_timestamp.timestamp())}"
                ),
                "symbol": raw_order_update.symbol,
                "side": mapped_side.value,
                "order_type": mapped_type.value,
                "status": mapped_status.value,
                "quantity_requested": str(quantity_requested),
                "quantity_filled": str(quantity_filled),
                "price": str(order_price) if order_price is not None else None,
                "time_in_force": mapped_tif.value,
                "exchange": ExchangeName.BACKPACK.value,
                "client_order_id": raw_order_update.client_order_id or str(uuid.uuid4()),
                "created_at": event_timestamp.isoformat(),
                "updated_at": event_timestamp.isoformat(),
                "triggered_at": None,
                "strategy_name": None,
                "signal_id": None,
                "reduce_only": False,
                "post_only": False,
                "bp_details": None,
                "hl_details": None,
            }

            return secure_transform(
                data=order_data,
                model_class=Order,
                context="backpack_ws_order_update_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform BackpackRawOrderUpdate to Order: {e}",
            ) from e
