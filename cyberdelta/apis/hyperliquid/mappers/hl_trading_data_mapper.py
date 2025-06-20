"""CyberDeltaEngine: Hyperliquid Trading Data Mapper.

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
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, TypedDict, TypeGuard

from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawSimpleOpenOrder,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    TriggerType,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class OrderComponents(TypedDict):
    """Type-safe components for Order creation."""

    side: OrderSide
    order_type: OrderType
    status: OrderStatus
    time_in_force: TimeInForce
    quantity_requested: Decimal
    quantity_filled: Decimal
    price: Decimal | None
    average_fill_price: Decimal | None
    stop_price: Decimal | None
    trigger_by: TriggerType | None
    created_at: datetime
    updated_at: datetime


def _is_dict_str_any(value: object) -> TypeGuard[dict[str, Any]]:
    """Type guard to check if value is a dict[str, Any]."""
    return isinstance(value, dict)


class HyperliquidTradingDataMapper:
    """Domain-focused mapper for Hyperliquid trading data transformations.

    This class contains static methods for transforming validated Hyperliquid Raw models
    related to trading operations into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(hl_side: str) -> OrderSide:
        """Maps a Hyperliquid order side string to internal OrderSide enum.

        Args:
            hl_side: Raw side string from Hyperliquid ("B" or "A")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            TransformationError: If side cannot be mapped

        """
        try:
            if hl_side == "B":
                return OrderSide.BUY
            elif hl_side == "A":
                return OrderSide.SELL

            raise TransformationError(
                f"Unknown Hyperliquid order side: '{hl_side}'",
                field_name="side",
                source_value=hl_side,
            )
        except TransformationError:
            # Re-raise TransformationError as-is per ERROR_HANDLING.md
            raise
        except Exception as e:
            raise TransformationError(
                f"Failed to map order side: {e}",
                field_name="side",
                source_value=hl_side,
                original_exception=e,
            ) from e

    @staticmethod
    def _map_status_to_internal(hl_status: str) -> OrderStatus:
        """Maps a Hyperliquid order status string to internal OrderStatus enum.

        Args:
            hl_status: Raw status string from Hyperliquid

        Returns:
            OrderStatus: Mapped internal enum value

        """
        try:
            status_map = {
                "open": OrderStatus.OPEN,
                "filled": OrderStatus.FILLED,
                "canceled": OrderStatus.CANCELED,  # Hyperliquid uses "canceled"
                "rejected": OrderStatus.REJECTED,
                # Map expired to UNKNOWN since we don't have an EXPIRED status
                "expired": OrderStatus.UNKNOWN,
            }
            mapped_status = status_map.get(hl_status.lower(), OrderStatus.UNKNOWN)

            if mapped_status == OrderStatus.UNKNOWN and hl_status.lower() not in status_map:
                logger.warning(
                    f"[HyperliquidTradingDataMapper] Unknown order status '{hl_status}', "
                    f"mapping to UNKNOWN"
                )

            return mapped_status
        except TransformationError:
            # Re-raise TransformationError as-is per ERROR_HANDLING.md
            raise
        except Exception as e:
            logger.error(f"Failed to map order status '{hl_status}': {e}")
            raise TransformationError(
                f"Failed to map order status: {e}",
                field_name="status",
                source_value=hl_status,
                original_exception=e,
            ) from e

    @staticmethod
    def _get_trigger_type(trigger: HyperliquidRawTriggerInfo | None) -> str | None:
        """Extract trigger type from trigger info."""
        return getattr(trigger, "tpsl", None) if trigger else None

    @staticmethod
    def _map_limit_order_type(trigger: HyperliquidRawTriggerInfo | None) -> OrderType:
        """Map limit order types with optional trigger."""
        trigger_type = HyperliquidTradingDataMapper._get_trigger_type(trigger)
        if trigger_type == "sl":
            return OrderType.STOP_LIMIT
        elif trigger_type == "tp":
            return OrderType.TAKE_PROFIT_LIMIT
        return OrderType.LIMIT

    @staticmethod
    def _map_market_order_type(trigger: HyperliquidRawTriggerInfo | None) -> OrderType:
        """Map market order types with optional trigger."""
        trigger_type = HyperliquidTradingDataMapper._get_trigger_type(trigger)
        if trigger_type == "sl":
            return OrderType.STOP_MARKET
        elif trigger_type == "tp":
            return OrderType.TAKE_PROFIT_MARKET
        return OrderType.MARKET

    @staticmethod
    def _map_type_to_internal(
        order_type: dict[str, Any],
        trigger: HyperliquidRawTriggerInfo | None,
    ) -> OrderType:
        """Maps a Hyperliquid order type dict to internal OrderType enum.

        Args:
            order_type: Raw order type dict from Hyperliquid
            trigger: Optional trigger info for stop/take profit orders

        Returns:
            OrderType: Mapped internal enum value

        Raises:
            TransformationError: If order type structure is invalid

        """
        try:
            # Hyperliquid uses nested dicts for orderType,
            # e.g. {"limit": {"tif": "Gtc"}}, {"market": {}}
            if "limit" in order_type:
                return HyperliquidTradingDataMapper._map_limit_order_type(trigger)
            elif "market" in order_type:
                return HyperliquidTradingDataMapper._map_market_order_type(trigger)

            logger.warning(
                f"[HyperliquidTradingDataMapper] Unknown orderType structure: {order_type}. "
                "Defaulting to LIMIT.",
            )
            return OrderType.LIMIT
        except TransformationError:
            # Re-raise TransformationError as-is per ERROR_HANDLING.md
            raise
        except Exception as e:
            logger.error(f"Failed to map order type: {e}")
            raise TransformationError(
                f"Failed to map order type: {e}",
                field_name="order_type",
                source_value=str(order_type),
                original_exception=e,
            ) from e

    @staticmethod
    def _map_time_in_force(order_type: dict[str, Any]) -> TimeInForce:
        """Maps a Hyperliquid order type dict to internal TimeInForce enum.

        Args:
            order_type: Raw order type dict from Hyperliquid

        Returns:
            TimeInForce: Mapped internal enum value

        """
        try:
            # Only limit orders have TIF in HL
            if "limit" in order_type and _is_dict_str_any(order_type["limit"]):
                # TypeGuard confirms it's a dict[str, Any]
                limit_dict = order_type["limit"]
                tif_val: Any = limit_dict.get("tif", "")
                tif_str = str(tif_val).upper()

                if tif_str == "GTC":
                    return TimeInForce.GTC
                elif tif_str == "IOC":
                    return TimeInForce.IOC
                elif tif_str == "ALO":
                    return TimeInForce.ALO
                elif tif_str:
                    logger.warning(
                        f"[HyperliquidTradingDataMapper] Unknown TIF value '{tif_str}', "
                        f"defaulting to GTC"
                    )

            return TimeInForce.GTC
        except TransformationError:
            # Re-raise TransformationError as-is per ERROR_HANDLING.md
            raise
        except Exception as e:
            logger.error(f"Failed to map time in force: {e}")
            # Default to GTC on error rather than raising per business logic
            return TimeInForce.GTC

    @staticmethod
    def _parse_order_enums(
        raw_order: HyperliquidRawOrder,
        trigger: HyperliquidRawTriggerInfo | None,
    ) -> tuple[OrderSide, OrderType, OrderStatus, TimeInForce]:
        """Parse order enums from raw order data."""
        side = HyperliquidTradingDataMapper._map_side_to_internal(raw_order.side)
        order_type = HyperliquidTradingDataMapper._map_type_to_internal(
            raw_order.order_type,
            trigger,
        )
        status = HyperliquidTradingDataMapper._map_status_to_internal(raw_order.status)
        time_in_force = HyperliquidTradingDataMapper._map_time_in_force(raw_order.order_type)
        return side, order_type, status, time_in_force

    @staticmethod
    def _parse_order_quantities_and_price(
        raw_order: HyperliquidRawOrder,
    ) -> tuple[Decimal, Decimal, Decimal | None]:
        """Parse quantities and price from raw order data."""
        try:
            # Parse quantities
            quantity_requested = parse_decimal_value(
                raw_order.sz,
                allow_none=False,
                field_name="sz",
            )
            if quantity_requested is None:
                raise TransformationError(
                    "quantity_requested (sz) is required",
                    field_name="sz",
                    source_value=raw_order.sz,
                )

            remaining_sz = parse_decimal_value(
                str(raw_order.remaining_sz),
                allow_none=True,
                field_name="remainingSz",
            )
            if remaining_sz is None:
                remaining_sz = Decimal("0")

            quantity_filled = quantity_requested - remaining_sz

            # Parse price - handle market orders correctly
            price = parse_decimal_value(
                str(raw_order.limit_px),
                allow_none=True,
                field_name="limitPx",
            )
            # For market orders, Hyperliquid uses limit_px="0", but internal Order
            # expects price=None
            if price is not None and price == Decimal("0"):
                price = None

            return quantity_requested, quantity_filled, price
        except TransformationError:
            raise
        except Exception as e:
            logger.error(f"Failed to parse order quantities and price: {e}")
            raise TransformationError(
                f"Failed to parse order quantities and price: {e}",
                source_data={"sz": raw_order.sz, "remaining_sz": raw_order.remaining_sz},
            ) from e

    @staticmethod
    def transform_raw_order_to_internal(
        raw_order: HyperliquidRawOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> Order:
        """Transforms a HyperliquidRawOrder to an Internal Order model.

        Args:
            raw_order: Validated raw order data from Hyperliquid
            trigger: Optional trigger info for stop/take profit orders

        Returns:
            Order: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse all order components
            order_components = HyperliquidTradingDataMapper._parse_order_components(
                raw_order, trigger
            )

            # Create and return the Order object
            return HyperliquidTradingDataMapper._create_order_from_components(
                raw_order, order_components
            )

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.error(
                f"[HyperliquidTradingDataMapper] Failed to transform order: {e}. "
                f"Raw order: {raw_order.model_dump_json()}"
            )
            raise TransformationError(
                f"Failed to transform HyperliquidRawOrder to Order: {e}",
                source_data=raw_order.model_dump(),
            ) from e

    def transform_raw_simple_open_order_to_internal(
        self, raw_simple_order: HyperliquidRawSimpleOpenOrder
    ) -> Order:
        """Transforms a HyperliquidRawSimpleOpenOrder to an Internal Order model.

        This method handles the flat structure returned by the openOrders endpoint,
        which differs from the nested structure of other order endpoints.

        Args:
            raw_simple_order: Validated raw simple order data from Hyperliquid

        Returns:
            Order: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Parse basic components from flat structure
            side = HyperliquidTradingDataMapper._map_side_to_internal(raw_simple_order.side)

            # For simple open orders, we know they are limit orders and open status
            order_type = OrderType.LIMIT
            status = OrderStatus.OPEN
            time_in_force = TimeInForce.GTC  # Default for open orders

            # Parse quantities and price
            quantity_requested = parse_decimal_value(raw_simple_order.orig_sz, field_name="orig_sz")
            quantity_sz = parse_decimal_value(raw_simple_order.sz, field_name="sz")

            # Calculate quantity filled if both values are available
            if quantity_requested is not None and quantity_sz is not None:
                quantity_filled = quantity_requested - quantity_sz
            else:
                quantity_filled = Decimal("0")
            price = parse_decimal_value(raw_simple_order.limit_px, field_name="limit_px")

            # For simple orders, these are not available
            average_fill_price = None if quantity_filled == Decimal("0") else None
            stop_price = None
            trigger_by = None

            # Parse timestamp
            created_at = parse_datetime_utc(raw_simple_order.timestamp, field_name="timestamp")
            updated_at = created_at  # No separate updated timestamp in simple orders

            # Create Order object
            return Order(
                exchange_order_id=str(raw_simple_order.oid),
                # client_order_id will use default UUID generation
                symbol=raw_simple_order.coin,
                side=side,
                order_type=order_type,
                status=status,
                time_in_force=time_in_force,
                quantity_requested=(
                    quantity_requested if quantity_requested is not None else Decimal("0")
                ),
                quantity_filled=quantity_filled,
                price=price,
                average_fill_price=average_fill_price,
                stop_price=stop_price,
                trigger_by=trigger_by,
                created_at=created_at if created_at is not None else datetime.now(UTC),
                updated_at=updated_at,
                triggered_at=None,  # Not available in simple order format
                strategy_name=None,  # Not available in simple order format
                signal_id=None,  # Not available in simple order format
                exchange=ExchangeName.HYPERLIQUID.value,
                hl_details=None,  # Could be populated if needed
            )

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.error(
                f"[HyperliquidTradingDataMapper] Failed to transform simple order: {e}. "
                f"Raw order: {raw_simple_order.model_dump_json()}"
            )
            raise TransformationError(
                f"Failed to transform HyperliquidRawSimpleOpenOrder to Order: {e}",
                source_data=raw_simple_order.model_dump(),
            ) from e

    @staticmethod
    def _parse_order_components(
        raw_order: HyperliquidRawOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> OrderComponents:
        """Parse all components needed for Order creation."""
        # Map enums
        side, order_type, status, time_in_force = HyperliquidTradingDataMapper._parse_order_enums(
            raw_order, trigger
        )

        # Parse quantities and price
        quantity_requested, quantity_filled, price = (
            HyperliquidTradingDataMapper._parse_order_quantities_and_price(raw_order)
        )

        # Parse timestamps
        created_at, updated_at = HyperliquidTradingDataMapper._parse_order_timestamps(raw_order)

        # Parse trigger/stop logic
        stop_price, trigger_by = HyperliquidTradingDataMapper._parse_trigger_info(trigger)

        # Calculate average_fill_price
        average_fill_price, quantity_filled = (
            HyperliquidTradingDataMapper._calculate_average_fill_price(
                raw_order, quantity_filled, price
            )
        )

        return OrderComponents(
            side=side,
            order_type=order_type,
            status=status,
            time_in_force=time_in_force,
            quantity_requested=quantity_requested,
            quantity_filled=quantity_filled,
            price=price,
            average_fill_price=average_fill_price,
            stop_price=stop_price,
            trigger_by=trigger_by,
            created_at=created_at,
            updated_at=updated_at,
        )

    @staticmethod
    def _parse_order_timestamps(
        raw_order: HyperliquidRawOrder,
    ) -> tuple[datetime, datetime]:
        """Parse order timestamps."""
        try:
            created_at = parse_datetime_utc(raw_order.timestamp, field_name="timestamp")
            if created_at is None:
                raise TransformationError(
                    "created_at (timestamp) is required",
                    field_name="timestamp",
                    source_value=raw_order.timestamp,
                )

            updated_at = parse_datetime_utc(
                raw_order.status_timestamp,
                field_name="statusTimestamp",
            )
            if updated_at is None:
                updated_at = created_at

            return created_at, updated_at
        except TransformationError:
            raise
        except Exception as e:
            logger.error(f"Failed to parse order timestamps: {e}")
            raise TransformationError(
                f"Failed to parse order timestamps: {e}",
                source_data={
                    "timestamp": raw_order.timestamp,
                    "status_timestamp": raw_order.status_timestamp,
                },
            ) from e

    @staticmethod
    def _parse_trigger_info(
        trigger: HyperliquidRawTriggerInfo | None,
    ) -> tuple[Decimal | None, TriggerType | None]:
        """Parse trigger/stop logic."""
        stop_price = None
        trigger_by = None

        if trigger:
            stop_price = parse_decimal_value(
                str(getattr(trigger, "trigger_px", "")),
                allow_none=True,
                field_name="triggerPx",
            )

            # Map trigger type if available
            trigger_type_str = getattr(trigger, "trigger_type", None)
            if trigger_type_str:
                if trigger_type_str.lower() == "mark":
                    trigger_by = TriggerType.MARK_PRICE
                elif trigger_type_str.lower() == "last":
                    trigger_by = TriggerType.LAST_PRICE

        return stop_price, trigger_by

    @staticmethod
    def _calculate_average_fill_price(
        raw_order: HyperliquidRawOrder,
        quantity_filled: Decimal,
        price: Decimal | None,
    ) -> tuple[Decimal | None, Decimal]:
        """Calculate average_fill_price based on business rules."""
        average_fill_price = None

        if quantity_filled > 0:
            # For Hyperliquid orders, avg_px is not available in order data
            # Use limit price as approximation when available
            if price is not None and price > 0:
                average_fill_price = price
            else:
                # If no valid price available but quantity is filled,
                # this indicates an inconsistent state. For market orders with fills
                # but no price data, we cannot determine a valid average_fill_price.
                # Set quantity_filled to 0 to maintain model consistency.
                logger.warning(
                    f"Order {raw_order.oid}: quantity_filled={quantity_filled} "
                    f"but no valid price available. Setting quantity_filled=0 to "
                    f"maintain model consistency.",
                )
                quantity_filled = Decimal("0")

        return average_fill_price, quantity_filled

    @staticmethod
    def _create_order_from_components(
        raw_order: HyperliquidRawOrder,
        components: OrderComponents,
    ) -> Order:
        """Create Order object from parsed components."""
        # Create order directly - no dict unpacking to avoid pyright issues
        if raw_order.cloid is not None:
            return Order(
                client_order_id=raw_order.cloid,
                exchange_order_id=str(raw_order.oid),
                symbol=raw_order.asset,
                exchange=ExchangeName.HYPERLIQUID.value,
                side=components["side"],
                order_type=components["order_type"],
                status=components["status"],
                quantity_requested=components["quantity_requested"],
                quantity_filled=components["quantity_filled"],
                price=components["price"],
                stop_price=components["stop_price"],
                average_fill_price=components["average_fill_price"],
                trigger_by=components["trigger_by"],
                time_in_force=components["time_in_force"],
                created_at=components["created_at"],
                updated_at=components["updated_at"],
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                quote_quantity_requested=None,
                reduce_only=False,
                post_only=False,
                trades=[],
                hl_details=None,
                bp_details=None,
            )
        else:
            return Order(
                exchange_order_id=str(raw_order.oid),
                symbol=raw_order.asset,
                exchange=ExchangeName.HYPERLIQUID.value,
                side=components["side"],
                order_type=components["order_type"],
                status=components["status"],
                quantity_requested=components["quantity_requested"],
                quantity_filled=components["quantity_filled"],
                price=components["price"],
                stop_price=components["stop_price"],
                average_fill_price=components["average_fill_price"],
                trigger_by=components["trigger_by"],
                time_in_force=components["time_in_force"],
                created_at=components["created_at"],
                updated_at=components["updated_at"],
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                quote_quantity_requested=None,
                reduce_only=False,
                post_only=False,
                trades=[],
                hl_details=None,
                bp_details=None,
            )

    @staticmethod
    def transform_raw_historical_order_to_internal(
        raw_historical_order: HyperliquidRawHistoricalOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> Order:
        """Transforms a HyperliquidRawHistoricalOrder to an Internal Order model.

        Args:
            raw_historical_order: Validated raw historical order data from Hyperliquid
            trigger: Optional trigger info for stop/take profit orders

        Returns:
            Order: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse all historical order components
            order_components = HyperliquidTradingDataMapper._parse_historical_order_components(
                raw_historical_order, trigger
            )

            # Create and return the Order object
            return HyperliquidTradingDataMapper._create_historical_order_from_components(
                raw_historical_order, order_components
            )

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.error(
                f"[HyperliquidTradingDataMapper] Failed to transform historical order: {e}. "
                f"Raw order: {raw_historical_order.model_dump_json()}"
            )
            raise TransformationError(
                f"Failed to transform HyperliquidRawHistoricalOrder to Order: {e}",
                source_data=raw_historical_order.model_dump(),
            ) from e

    @staticmethod
    def _parse_historical_order_components(
        raw_historical_order: HyperliquidRawHistoricalOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> OrderComponents:
        """Parse all components needed for historical Order creation."""
        # Map enums
        side = HyperliquidTradingDataMapper._map_side_to_internal(raw_historical_order.side)

        # Convert string order type to dict format for processing
        # Use the TIF field from historical order data when available
        order_type_lower = raw_historical_order.order_type.lower()

        # Check if historical order has TIF information
        tif_from_raw = getattr(raw_historical_order, "tif", None)
        if tif_from_raw and order_type_lower == "limit":
            # For limit orders, preserve the TIF from the raw data
            order_type_dict: dict[str, Any] = {"limit": {"tif": tif_from_raw}}
        elif order_type_lower == "market":
            # Market orders in Hyperliquid are limit IOC orders
            order_type_dict = {"limit": {"tif": "Ioc"}}
        else:
            order_type_dict = {order_type_lower: {}}

        order_type = HyperliquidTradingDataMapper._map_type_to_internal(
            order_type_dict,
            trigger,
        )
        status = HyperliquidTradingDataMapper._map_status_to_internal(
            raw_historical_order.status,
        )
        time_in_force = HyperliquidTradingDataMapper._map_time_in_force(
            order_type_dict,
        )

        # Parse quantities and price
        quantity_requested, quantity_filled, price = (
            HyperliquidTradingDataMapper._parse_historical_quantities_and_price(
                raw_historical_order
            )
        )

        # Parse timestamps
        created_at, updated_at = HyperliquidTradingDataMapper._parse_historical_timestamps(
            raw_historical_order
        )

        # Parse trigger/stop logic (reuse existing method)
        stop_price, trigger_by = HyperliquidTradingDataMapper._parse_trigger_info(trigger)

        # Calculate average_fill_price (reuse existing method with different order ID)
        average_fill_price, quantity_filled = (
            HyperliquidTradingDataMapper._calculate_historical_average_fill_price(
                raw_historical_order, quantity_filled, price
            )
        )

        return OrderComponents(
            side=side,
            order_type=order_type,
            status=status,
            time_in_force=time_in_force,
            quantity_requested=quantity_requested,
            quantity_filled=quantity_filled,
            price=price,
            average_fill_price=average_fill_price,
            stop_price=stop_price,
            trigger_by=trigger_by,
            created_at=created_at,
            updated_at=updated_at,
        )

    @staticmethod
    def _parse_historical_quantities_and_price(
        raw_historical_order: HyperliquidRawHistoricalOrder,
    ) -> tuple[Decimal, Decimal, Decimal | None]:
        """Parse quantities and price for historical orders."""
        # Parse quantities - use orig_sz for quantity_requested (sz is remaining quantity)
        quantity_requested = parse_decimal_value(
            raw_historical_order.orig_sz,
            allow_none=False,
            field_name="orig_sz",
        )
        if quantity_requested is None:
            raise TransformationError("quantity_requested (orig_sz) is required")

        # For historical orders, calculate filled quantity from original size and remaining
        remaining_sz = parse_decimal_value(
            str(getattr(raw_historical_order, "remaining_sz", "0")),
            allow_none=True,
            field_name="remainingSz",
        )
        if remaining_sz is None:
            remaining_sz = Decimal("0")

        quantity_filled = quantity_requested - remaining_sz

        # Parse price - handle market orders correctly
        price = parse_decimal_value(
            str(raw_historical_order.limit_px),
            allow_none=True,
            field_name="limitPx",
        )
        # For market orders, Hyperliquid uses limit_px="0", but internal Order
        # expects price=None
        if price is not None and price == Decimal("0"):
            price = None

        return quantity_requested, quantity_filled, price

    @staticmethod
    def _parse_historical_timestamps(
        raw_historical_order: HyperliquidRawHistoricalOrder,
    ) -> tuple[datetime, datetime]:
        """Parse timestamps for historical orders."""
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

        return created_at, updated_at

    @staticmethod
    def _calculate_historical_average_fill_price(
        raw_historical_order: HyperliquidRawHistoricalOrder,
        quantity_filled: Decimal,
        price: Decimal | None,
    ) -> tuple[Decimal | None, Decimal]:
        """Calculate average_fill_price for historical orders."""
        average_fill_price = None

        if quantity_filled > 0:
            # For Hyperliquid orders, avg_px is not available in order data
            # Use limit price as approximation when available
            if price is not None and price > 0:
                average_fill_price = price
            else:
                # If no valid price available but quantity is filled,
                # this indicates an inconsistent state. For market orders with fills
                # but no price data, we cannot determine a valid average_fill_price.
                # Set quantity_filled to 0 to maintain model consistency.
                logger.warning(
                    f"Order {raw_historical_order.oid}: quantity_filled={quantity_filled} "
                    f"but no valid price available. Setting quantity_filled=0 to "
                    f"maintain model consistency.",
                )
                quantity_filled = Decimal("0")

        return average_fill_price, quantity_filled

    @staticmethod
    def _create_historical_order_from_components(
        raw_historical_order: HyperliquidRawHistoricalOrder,
        components: OrderComponents,
    ) -> Order:
        """Create Order object from parsed historical order components."""
        # Get client order ID
        cloid = getattr(raw_historical_order, "cloid", None)

        # Create order directly - no dict unpacking to avoid pyright issues
        if cloid is not None:
            return Order(
                client_order_id=cloid,
                exchange_order_id=str(raw_historical_order.oid),
                symbol=raw_historical_order.asset,
                exchange=ExchangeName.HYPERLIQUID.value,
                side=components["side"],
                order_type=components["order_type"],
                status=components["status"],
                quantity_requested=components["quantity_requested"],
                quantity_filled=components["quantity_filled"],
                price=components["price"],
                stop_price=components["stop_price"],
                average_fill_price=components["average_fill_price"],
                trigger_by=components["trigger_by"],
                time_in_force=components["time_in_force"],
                created_at=components["created_at"],
                updated_at=components["updated_at"],
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                quote_quantity_requested=None,
                reduce_only=False,
                post_only=False,
                trades=[],
                hl_details=None,
                bp_details=None,
            )
        else:
            return Order(
                exchange_order_id=str(raw_historical_order.oid),
                symbol=raw_historical_order.asset,
                exchange=ExchangeName.HYPERLIQUID.value,
                side=components["side"],
                order_type=components["order_type"],
                status=components["status"],
                quantity_requested=components["quantity_requested"],
                quantity_filled=components["quantity_filled"],
                price=components["price"],
                stop_price=components["stop_price"],
                average_fill_price=components["average_fill_price"],
                trigger_by=components["trigger_by"],
                time_in_force=components["time_in_force"],
                created_at=components["created_at"],
                updated_at=components["updated_at"],
                triggered_at=None,
                strategy_name=None,
                signal_id=None,
                quote_quantity_requested=None,
                reduce_only=False,
                post_only=False,
                trades=[],
                hl_details=None,
                bp_details=None,
            )

    @staticmethod
    def transform_ws_order_update_to_internal_order(
        raw_order: HyperliquidRawOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> Order:
        """Transforms a HyperliquidRawOrder from WebSocket order update event to Internal Order.

        This is an alias for transform_raw_order_to_internal for consistency with
        WebSocket naming.

        Args:
            raw_order: Validated raw order data from Hyperliquid WebSocket
            trigger: Optional trigger info for stop/take profit orders

        Returns:
            Order: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        return HyperliquidTradingDataMapper.transform_raw_order_to_internal(raw_order, trigger)
