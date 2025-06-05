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

import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder, BackpackRawOrderUpdate
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


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
        }
        return status_map.get(bp_status.lower(), OrderStatus.UNKNOWN)

    @staticmethod
    def _map_type_to_internal(bp_type: str) -> OrderType:
        """Map a Backpack order type string to internal OrderType enum.

        Args:
            bp_type: Raw order type string from Backpack

        Returns:
            OrderType: Mapped internal enum value

        """
        type_map = {
            "limit": OrderType.LIMIT,
            "market": OrderType.MARKET,
            "stop": OrderType.STOP_MARKET,
            "stop_limit": OrderType.STOP_LIMIT,
            "trailing_stop": OrderType.STOP_MARKET,
            "take_profit": OrderType.LIMIT,
        }
        return type_map.get(bp_type.lower(), OrderType.LIMIT)

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
            mapped_type = BackpackTradingDataMapper._map_type_to_internal(order_type)
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

            return Order(
                exchange_order_id=order_id,
                symbol=symbol,
                side=mapped_side,
                order_type=mapped_type,
                status=mapped_status,
                quantity_requested=quantity_requested,
                quantity_filled=quantity_filled,
                price=order_price,
                time_in_force=mapped_tif,
                exchange=ExchangeName.BACKPACK.value,
                client_order_id=client_order_id or str(uuid.uuid4()),
                created_at=created_timestamp or datetime.now(UTC),
                updated_at=updated_timestamp,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform Backpack order data to Order: {e}",
            ) from e

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
            mapped_type = BackpackTradingDataMapper._map_type_to_internal(raw_order.orderType)
            mapped_status = BackpackTradingDataMapper._map_status_to_internal(raw_order.status)
            mapped_tif = BackpackTradingDataMapper._map_time_in_force(
                raw_order.timeInForce or "gtc",
            )

            # Parse quantities
            quantity_requested = parse_decimal_value(
                raw_order.quantity,
                allow_none=False,
                field_name="quantity",
            )
            if quantity_requested is None:
                raise TransformationError("quantity_requested is required")

            quantity_filled = parse_decimal_value(
                raw_order.executedQuantity,
                allow_none=True,
                field_name="executedQuantity",
            ) or Decimal("0")

            # Parse price
            order_price = None
            if raw_order.price and raw_order.price != "0":
                parsed_price = parse_decimal_value(
                    raw_order.price,
                    allow_none=True,
                    field_name="price",
                )
                if parsed_price is not None and parsed_price > 0:
                    order_price = parsed_price

            # Parse stop price
            stop_price = None
            if raw_order.triggerPrice and raw_order.triggerPrice != "0":
                parsed_stop_price = parse_decimal_value(
                    raw_order.triggerPrice,
                    allow_none=True,
                    field_name="triggerPrice",
                )
                if parsed_stop_price is not None and parsed_stop_price > 0:
                    stop_price = parsed_stop_price

            # Parse average fill price
            average_fill_price = None
            if raw_order.avgFillPrice and raw_order.avgFillPrice != "0":
                parsed_avg_price = parse_decimal_value(
                    raw_order.avgFillPrice,
                    allow_none=True,
                    field_name="avgFillPrice",
                )
                if parsed_avg_price is not None and parsed_avg_price > 0:
                    average_fill_price = parsed_avg_price

            # Parse timestamps
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

            return Order(
                exchange_order_id=raw_order.id,
                symbol=raw_order.symbol,
                side=mapped_side,
                order_type=mapped_type,
                status=mapped_status,
                quantity_requested=quantity_requested,
                quantity_filled=quantity_filled,
                price=order_price,
                stop_price=stop_price,
                average_fill_price=average_fill_price,
                time_in_force=mapped_tif,
                exchange=ExchangeName.BACKPACK.value,
                client_order_id=raw_order.clientId or str(uuid.uuid4()),
                created_at=created_timestamp,
                updated_at=updated_timestamp,
                triggered_at=triggered_timestamp,
                strategy_name=None,
                signal_id=None,
                reduce_only=raw_order.reduceOnly or False,
                post_only=raw_order.postOnly or False,
            )

        except Exception as e:
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
                raw_order_update.order_type,
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

            return Order(
                exchange_order_id=f"ws_order_{raw_order_update.event_type}_{int(event_timestamp.timestamp())}",
                symbol=raw_order_update.symbol,
                side=mapped_side,
                order_type=mapped_type,
                status=mapped_status,
                quantity_requested=quantity_requested,
                quantity_filled=quantity_filled,
                price=order_price,
                time_in_force=mapped_tif,
                exchange=ExchangeName.BACKPACK.value,
                client_order_id=raw_order_update.client_order_id or str(uuid.uuid4()),
                created_at=event_timestamp,
                updated_at=event_timestamp,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                reduce_only=False,
                post_only=False,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform BackpackRawOrderUpdate to Order: {e}",
            ) from e
