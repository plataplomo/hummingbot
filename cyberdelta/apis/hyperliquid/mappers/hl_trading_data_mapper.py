"""
CyberDeltaEngine: Hyperliquid Trading Data Mapper
------------------------------------------------

This module provides the HyperliquidTradingDataMapper class for transforming
Hyperliquid Raw Trading Data models into Internal Domain Models.

Responsibilities:
- Transform Raw Orders (HyperliquidRawOrder) to Internal Order models
- Transform Raw Historical Orders to Internal Order models
- Transform Raw Order responses from trading operations to Internal Order models
- Transform WebSocket Order Update events to Internal Order models

All transformation methods follow the standard pattern:
- Take a validated Raw Pydantic Model as primary input
- Return fully populated Internal Domain Model with Details slots
- Handle type conversions, enum mapping, and error cases
- Raise TransformationError for unmappable data
"""

import logging
from decimal import Decimal
from typing import Any

from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    TriggerType,
)
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class TransformationError(ValueError):
    """Raised when a validated Raw model cannot be transformed to Internal model."""

    pass


class HyperliquidTradingDataMapper:
    """
    Domain-focused mapper for Hyperliquid trading data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw models
    related to trading operations into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(hl_side: str) -> OrderSide:
        """
        Maps a Hyperliquid order side string to internal OrderSide enum.

        Args:
            hl_side: Raw side string from Hyperliquid ("B" or "A")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            TransformationError: If side cannot be mapped
        """
        if hl_side == "B":
            return OrderSide.BUY
        elif hl_side == "A":
            return OrderSide.SELL

        raise TransformationError(f"Unknown Hyperliquid order side: '{hl_side}'")

    @staticmethod
    def _map_status_to_internal(hl_status: str) -> OrderStatus:
        """
        Maps a Hyperliquid order status string to internal OrderStatus enum.

        Args:
            hl_status: Raw status string from Hyperliquid

        Returns:
            OrderStatus: Mapped internal enum value
        """
        status_map = {
            "open": OrderStatus.OPEN,
            "filled": OrderStatus.FILLED,
            "cancelled": OrderStatus.CANCELED,
            "canceled": OrderStatus.CANCELED,
            "rejected": OrderStatus.REJECTED,
            "partially_filled": OrderStatus.PARTIALLY_FILLED,
        }
        return status_map.get(hl_status.lower(), OrderStatus.UNKNOWN)

    @staticmethod
    def _map_type_to_internal(
        order_type: dict[str, Any], trigger: HyperliquidRawTriggerInfo | None
    ) -> OrderType:
        """
        Maps a Hyperliquid order type dict to internal OrderType enum.

        Args:
            order_type: Raw order type dict from Hyperliquid
            trigger: Optional trigger info for stop/take profit orders

        Returns:
            OrderType: Mapped internal enum value
        """
        # Hyperliquid uses nested dicts for orderType,
        # e.g. {"limit": {"tif": "Gtc"}}, {"market": {}}
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
            f"[HyperliquidTradingDataMapper] Unknown orderType structure: {order_type}. "
            "Defaulting to LIMIT."
        )
        return OrderType.LIMIT

    @staticmethod
    def _map_time_in_force(order_type: dict[str, Any]) -> TimeInForce:
        """
        Maps a Hyperliquid order type dict to internal TimeInForce enum.

        Args:
            order_type: Raw order type dict from Hyperliquid

        Returns:
            TimeInForce: Mapped internal enum value
        """
        # Only limit orders have TIF in HL
        if "limit" in order_type and isinstance(order_type["limit"], dict):
            limit_dict: dict[str, Any] = order_type["limit"]
            tif_val: Any = limit_dict.get("tif", "")
            tif_str = str(tif_val).upper()

            if tif_str == "GTC":
                return TimeInForce.GTC
            elif tif_str == "IOC":
                return TimeInForce.IOC
            elif tif_str == "ALO":
                return TimeInForce.ALO

        return TimeInForce.GTC

    @staticmethod
    def transform_raw_order_to_internal(
        raw_order: HyperliquidRawOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> Order:
        """
        Transforms a HyperliquidRawOrder to an Internal Order model.

        Args:
            raw_order: Validated raw order data from Hyperliquid
            trigger: Optional trigger info for stop/take profit orders

        Returns:
            Order: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Map enums
            side = HyperliquidTradingDataMapper._map_side_to_internal(raw_order.side)
            order_type = HyperliquidTradingDataMapper._map_type_to_internal(
                raw_order.order_type, trigger
            )
            status = HyperliquidTradingDataMapper._map_status_to_internal(raw_order.status)
            time_in_force = HyperliquidTradingDataMapper._map_time_in_force(raw_order.order_type)

            # Parse quantities
            quantity_requested = parse_decimal_value(
                raw_order.sz, allow_none=False, field_name="sz"
            )
            if quantity_requested is None:
                raise TransformationError("quantity_requested (sz) is required")

            remaining_sz = parse_decimal_value(
                str(raw_order.remaining_sz), allow_none=True, field_name="remainingSz"
            )
            if remaining_sz is None:
                remaining_sz = Decimal("0")

            quantity_filled = quantity_requested - remaining_sz

            # Parse price
            price = parse_decimal_value(
                str(raw_order.limit_px), allow_none=True, field_name="limitPx"
            )

            # Parse timestamps
            created_at = parse_datetime_utc(raw_order.timestamp, field_name="timestamp")
            if created_at is None:
                raise TransformationError("created_at (timestamp) is required")

            updated_at = parse_datetime_utc(
                raw_order.status_timestamp, field_name="statusTimestamp"
            )

            # Parse trigger/stop logic
            stop_price = None
            trigger_by = None
            if trigger:
                stop_price = parse_decimal_value(
                    str(getattr(trigger, "trigger_px", "")), allow_none=True, field_name="triggerPx"
                )

                # Map trigger type if available
                trigger_type_str = getattr(trigger, "trigger_type", None)
                if trigger_type_str:
                    if trigger_type_str.lower() == "mark":
                        trigger_by = TriggerType.MARK_PRICE
                    elif trigger_type_str.lower() == "last":
                        trigger_by = TriggerType.LAST_PRICE

            return Order(
                exchange_order_id=str(raw_order.oid),
                symbol=raw_order.asset,
                side=side,
                order_type=order_type,
                status=status,
                quantity_requested=quantity_requested,
                quantity_filled=quantity_filled,
                price=price,
                stop_price=stop_price,
                time_in_force=time_in_force,
                trigger_by=trigger_by,
                exchange=ExchangeName.HYPERLIQUID.value,
                client_order_id=raw_order.cloid or "",
                created_at=created_at,
                updated_at=updated_at,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawOrder to Order: {e}"
            ) from e

    @staticmethod
    def transform_raw_historical_order_to_internal(
        raw_historical_order: HyperliquidRawHistoricalOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> Order:
        """
        Transforms a HyperliquidRawHistoricalOrder to an Internal Order model.

        Args:
            raw_historical_order: Validated raw historical order data from Hyperliquid
            trigger: Optional trigger info for stop/take profit orders

        Returns:
            Order: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Map enums
            side = HyperliquidTradingDataMapper._map_side_to_internal(raw_historical_order.side)
            order_type = HyperliquidTradingDataMapper._map_type_to_internal(
                raw_historical_order.order_type, trigger
            )
            status = HyperliquidTradingDataMapper._map_status_to_internal(
                raw_historical_order.status
            )
            time_in_force = HyperliquidTradingDataMapper._map_time_in_force(
                raw_historical_order.order_type
            )

            # Parse quantities
            quantity_requested = parse_decimal_value(
                raw_historical_order.sz, allow_none=False, field_name="sz"
            )
            if quantity_requested is None:
                raise TransformationError("quantity_requested (sz) is required")

            # For historical orders, calculate filled quantity from original size and remaining
            remaining_sz = parse_decimal_value(
                str(getattr(raw_historical_order, "remaining_sz", "0")),
                allow_none=True,
                field_name="remainingSz",
            )
            if remaining_sz is None:
                remaining_sz = Decimal("0")

            quantity_filled = quantity_requested - remaining_sz

            # Parse price
            price = parse_decimal_value(
                str(raw_historical_order.limit_px), allow_none=True, field_name="limitPx"
            )

            # Parse timestamps
            created_at = parse_datetime_utc(raw_historical_order.timestamp, field_name="timestamp")
            if created_at is None:
                raise TransformationError("created_at (timestamp) is required")

            # Historical orders might not have separate status timestamp
            updated_at = (
                parse_datetime_utc(
                    getattr(raw_historical_order, "status_timestamp", None),
                    field_name="statusTimestamp",
                )
                or created_at
            )

            # Parse trigger/stop logic
            stop_price = None
            trigger_by = None
            if trigger:
                stop_price = parse_decimal_value(
                    str(getattr(trigger, "trigger_px", "")), allow_none=True, field_name="triggerPx"
                )

                # Map trigger type if available
                trigger_type_str = getattr(trigger, "trigger_type", None)
                if trigger_type_str:
                    if trigger_type_str.lower() == "mark":
                        trigger_by = TriggerType.MARK_PRICE
                    elif trigger_type_str.lower() == "last":
                        trigger_by = TriggerType.LAST_PRICE

            return Order(
                exchange_order_id=str(raw_historical_order.oid),
                symbol=raw_historical_order.asset,
                side=side,
                order_type=order_type,
                status=status,
                quantity_requested=quantity_requested,
                quantity_filled=quantity_filled,
                price=price,
                stop_price=stop_price,
                time_in_force=time_in_force,
                trigger_by=trigger_by,
                exchange=ExchangeName.HYPERLIQUID.value,
                client_order_id=getattr(raw_historical_order, "cloid", None) or "",
                created_at=created_at,
                updated_at=updated_at,
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform HyperliquidRawHistoricalOrder to Order: {e}"
            ) from e
