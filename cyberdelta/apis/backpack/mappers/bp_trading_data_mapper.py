"""
CyberDeltaEngine: Backpack Trading Data Mapper
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
from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class TransformationError(ValueError):
    """Raised when a validated Raw model cannot be transformed to Internal model."""

    pass


class BackpackTradingDataMapper:
    """
    Domain-focused mapper for Backpack trading data transformations.

    This class contains static methods for transforming validated Backpack Raw models
    related to trading operations into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(bp_side: str) -> OrderSide:
        """
        Maps a Backpack order side string to internal OrderSide enum.

        Args:
            bp_side: Raw side string from Backpack ("Buy", "Sell")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            TransformationError: If side cannot be mapped
        """
        side_lower = bp_side.lower() if bp_side else ""
        if side_lower == "buy":
            return OrderSide.BUY
        elif side_lower == "sell":
            return OrderSide.SELL

        raise TransformationError(f"Unknown Backpack order side: '{bp_side}'")

    @staticmethod
    def _map_status_to_internal(bp_status: str) -> OrderStatus:
        """
        Maps a Backpack order status string to internal OrderStatus enum.

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
        """
        Maps a Backpack order type string to internal OrderType enum.

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
        }
        return type_map.get(bp_type.lower(), OrderType.LIMIT)

    @staticmethod
    def _map_time_in_force(bp_tif: str) -> TimeInForce:
        """
        Maps a Backpack time in force string to internal TimeInForce enum.

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
        """
        Transforms Backpack order data to an Internal Order model.

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
                quantity, allow_none=False, field_name="quantity"
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
                client_order_id=client_order_id or "",
                created_at=created_timestamp or datetime.now(UTC),
                updated_at=updated_timestamp,
                triggered_at=None,
                strategy_name="",
                signal_id="",
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform Backpack order data to Order: {e}"
            ) from e
