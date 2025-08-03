"""Backpack Order Mapper.

This mapper handles transformations for order-related data from the Backpack exchange.

Focused on:
- Order transformations from raw order data
- Order status, type, and side mapping
- WebSocket order update transformations
- Order-specific data validation and error handling
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.mappers.utils.backpack_enum_mappers import BackpackEnumMappers
from cyberdelta.apis.backpack.models.bp_raw_order import (
    BackpackRawOrderResponse,
    BackpackRawOrderUpdate,
)
from cyberdelta.apis.backpack.protocols.mapper_protocols import OrderMapperProtocol
from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin
from cyberdelta.apis.exceptions import (
    InvalidQuantityError,
    MissingQuantityError,
    MissingTimestampError,
    OrderTransformationError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import (
    OrderStatus,
)
from cyberdelta.core.models import Order
from cyberdelta.core.models.market.order import BackpackOrderDetails
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import (
    OrderType,
    TimeInForce,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackOrderMapper(CommonDataParserMixin, OrderMapperProtocol):
    """Focused mapper for Backpack order data transformations.

    This class contains static methods for transforming validated Backpack Raw order models
    into CyberDeltaEngine Internal Order Domain Models.
    """

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
        raw_order: BackpackRawOrderResponse | None = None,
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

        # Check systemOrderType for triggered stop orders
        if raw_order and raw_order.systemOrderType:
            result = BackpackOrderMapper._check_system_order_type(
                bp_type_lower,
                raw_order.systemOrderType.lower(),
            )
            if result:
                return result

        # Check if this was a triggered stop order
        if raw_order and raw_order.triggeredAt and bp_type_lower == "market":
            result = BackpackOrderMapper._check_triggered_order(raw_order)
            if result:
                return result

        # Check trigger price
        if trigger_price:
            return BackpackOrderMapper._check_trigger_price_order(bp_type_lower, raw_order)

        # Default mapping
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
    def _check_system_order_type(bp_type_lower: str, system_type: str) -> OrderType | None:
        """Check systemOrderType for stop/take profit orders.

        Args:
            bp_type_lower: Lowercased order type string
            system_type: System order type field from Backpack

        Returns:
            OrderType enum if system type indicates stop/take profit order, None otherwise
        """
        if "stop" in system_type:
            if bp_type_lower == "market":
                return OrderType.STOP_MARKET
            if bp_type_lower == "limit":
                return OrderType.STOP_LIMIT
        elif "take_profit" in system_type or "tp" in system_type:
            if bp_type_lower == "market":
                return OrderType.TAKE_PROFIT_MARKET
            if bp_type_lower == "limit":
                return OrderType.TAKE_PROFIT_LIMIT
        return None

    @staticmethod
    def _check_triggered_order(raw_order: BackpackRawOrderResponse) -> OrderType | None:
        """Check if order was a triggered stop/take profit order.

        This method analyzes the raw order data to determine if it was triggered
        as a stop loss or take profit order by examining the trigger price fields.

        Args:
            raw_order: Raw order response from Backpack containing trigger price fields

        Returns:
            OrderType.STOP_MARKET if stop loss triggered,
            OrderType.TAKE_PROFIT_MARKET if take profit triggered,
            None if not a triggered order
        """
        if raw_order.stopLossTriggerPrice or raw_order.stopLossLimitPrice:
            return OrderType.STOP_MARKET
        if raw_order.takeProfitTriggerPrice or raw_order.takeProfitLimitPrice:
            return OrderType.TAKE_PROFIT_MARKET
        return None

    @staticmethod
    def _check_trigger_price_order(
        bp_type_lower: str,
        raw_order: BackpackRawOrderResponse | None,
    ) -> OrderType:
        """Determine order type based on trigger price.

        Args:
            bp_type_lower: Lowercased order type string
            raw_order: Raw order data from Backpack (optional)

        Returns:
            OrderType enum based on trigger price presence and order type
        """
        # Check if this is a take profit order
        if raw_order and raw_order.takeProfitTriggerPrice:
            if bp_type_lower == "market":
                return OrderType.TAKE_PROFIT_MARKET
            if bp_type_lower == "limit":
                return OrderType.TAKE_PROFIT_LIMIT
        # Otherwise it's a stop loss order
        else:
            if bp_type_lower == "market":
                return OrderType.STOP_MARKET
            if bp_type_lower == "limit":
                return OrderType.STOP_LIMIT
            if bp_type_lower == "stop":
                return OrderType.STOP_MARKET
            if bp_type_lower == "trailing_stop":
                return OrderType.STOP_MARKET
        return OrderType.LIMIT  # Fallback

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

    def transform_order_data_to_internal(
        self,
        order_id: str,
        symbol: Symbol,
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
            MissingQuantityError: If quantity is missing or invalid
            OrderTransformationError: If transformation fails

        """
        try:
            # Map enums
            mapped_side = BackpackEnumMappers.map_side_to_internal(side, "order")
            mapped_type = BackpackOrderMapper._map_type_to_internal(order_type, None, None)
            mapped_status = BackpackOrderMapper._map_status_to_internal(status)
            mapped_tif = BackpackOrderMapper._map_time_in_force(time_in_force or "gtc")

            # Parse quantities
            quantity_requested = self.parse_decimal_safely(quantity)
            if quantity_requested is None:
                raise MissingQuantityError
            BackpackOrderMapper._validate_quantity_requested(quantity_requested)

            # For now, assume no filled quantity available in basic order data
            quantity_filled = Decimal(0)

            # Parse price
            order_price = self.parse_decimal_safely(price, default=None) if price else None

            # Parse timestamps
            created_timestamp = self.parse_timestamp(created_at)
            updated_timestamp = self.parse_timestamp(updated_at)

            # Symbol is already a domain object
            exchange_symbol = symbol

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            order_data: dict[str, Any] = {
                "exchange_order_id": order_id,
                "symbol": exchange_symbol,  # Domain object!
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

        except (ValueError, TypeError, KeyError, AttributeError) as e:
            raise OrderTransformationError(
                order_id=order_id,
                reason=str(e),
                order_data=None,
                original_error=e,
            ) from e

    def _parse_order_quantities(
        self, raw_order: BackpackRawOrderResponse
    ) -> tuple[Decimal, Decimal]:
        """Parse and validate order quantities.

        Args:
            raw_order: Raw order data from Backpack

        Returns:
            Tuple of (quantity_requested, quantity_filled) as Decimal values

        Raises:
            InvalidQuantityError: If quantity requirements are not met
        """
        quantity_requested = self.parse_decimal_safely(raw_order.quantity, default=None)

        # For stop orders, quantity might be 0 and the actual quantity is in triggerQuantity
        if (quantity_requested is None or quantity_requested == Decimal(0)) and (
            raw_order.triggerQuantity
        ):
            quantity_requested = self.parse_decimal_safely(raw_order.triggerQuantity)

        if quantity_requested is None or quantity_requested <= Decimal(0):
            raise InvalidQuantityError(
                field_name="quantity_requested",
                value=quantity_requested,
            )

        quantity_filled = self.parse_decimal_safely(
            raw_order.executedQuantity, default=Decimal(0)
        ) or Decimal(0)

        return quantity_requested, quantity_filled

    def _parse_order_price(self, price_value: str | None, field_name: str) -> Decimal | None:
        """Parse order price field, returning None for zero or invalid values.

        Args:
            price_value: Price value as string or None
            field_name: Name of the field for error reporting

        Returns:
            Parsed price as Decimal or None if price is zero/invalid
        """
        if not price_value or price_value == "0":
            return None

        parsed_price = self.parse_decimal_safely(price_value, default=None)
        return parsed_price if parsed_price is not None and parsed_price > 0 else None

    def _calculate_average_fill_price(
        self,
        raw_order: BackpackRawOrderResponse,
        quantity_filled: Decimal,
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
        avg_fill_price = self._parse_order_price(
            raw_order.avgFillPrice,
            "avgFillPrice",
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
            executed_quote = self.parse_decimal_safely(
                raw_order.executedQuoteQuantity, default=None
            )

            if executed_quote is not None and executed_quote > 0:
                return executed_quote / quantity_filled
            logger.warning(
                "avg_fill_price_calc_failed: Could not calculate average fill price for order",
                order_id=raw_order.id,
                executed_quote=executed_quote,
                quantity_filled=quantity_filled,
            )

        logger.warning(
            "avg_fill_price_unavailable: No average fill price available for order",
            order_id=raw_order.id,
            avg_fill_price=raw_order.avgFillPrice,
            quantity_filled=quantity_filled,
            executed_quote_quantity=raw_order.executedQuoteQuantity,
        )
        return None

    def _parse_order_timestamps(
        self,
        raw_order: BackpackRawOrderResponse,
    ) -> tuple[datetime, datetime | None, datetime | None]:
        """Parse order timestamps.

        Args:
            raw_order: Raw order data from Backpack

        Returns:
            Tuple of (created_timestamp, updated_timestamp, triggered_timestamp) where
            created_timestamp is required and others may be None

        Raises:
            MissingTimestampError: If required timestamp is missing
        """
        created_timestamp = self.parse_timestamp(raw_order.createdAt)
        if created_timestamp is None:
            raise MissingTimestampError(
                field_name="createdAt",
                order_id=raw_order.id,
            )

        updated_timestamp = self.parse_timestamp(raw_order.updatedAt)
        triggered_timestamp = self.parse_timestamp(raw_order.triggeredAt)

        return created_timestamp, updated_timestamp, triggered_timestamp

    def transform_raw_order_to_internal(self, raw_order: BackpackRawOrderResponse) -> Order:
        """Transform a BackpackRawOrderResponse to an Internal Order model.

        Args:
            raw_order: Validated raw order data from Backpack

        Returns:
            Order: Internal domain model with populated fields

        Raises:
            OrderTransformationError: If transformation fails

        """
        try:
            # Map enums
            mapped_side = BackpackEnumMappers.map_side_to_internal(raw_order.side, "order")
            mapped_type = BackpackOrderMapper._map_type_to_internal(
                raw_order.orderType,
                raw_order.triggerPrice,
                raw_order,
            )
            mapped_status = BackpackOrderMapper._map_status_to_internal(raw_order.status)
            mapped_tif = BackpackOrderMapper._map_time_in_force(
                raw_order.timeInForce or "gtc",
            )

            # Parse quantities
            quantity_requested, quantity_filled = self._parse_order_quantities(
                raw_order,
            )

            # Parse prices
            order_price = self._parse_order_price(raw_order.price, "price")
            stop_price = self._parse_order_price(
                raw_order.triggerPrice,
                "triggerPrice",
            )
            average_fill_price = self._calculate_average_fill_price(
                raw_order,
                quantity_filled,
            )

            # Parse timestamps
            created_timestamp, updated_timestamp, triggered_timestamp = (
                self._parse_order_timestamps(raw_order)
            )

            # Create BackpackOrderDetails from raw order data
            bp_details = BackpackOrderDetails(
                executed_quote_quantity=self.parse_decimal_safely(
                    raw_order.executedQuoteQuantity, default=None
                )
                if raw_order.executedQuoteQuantity
                else None,
                self_trade_prevention=None,  # Can be mapped if needed
                expiry_reason=None,  # Can be mapped if needed
                origin=None,  # Can be mapped if needed
                sl_trigger_price=self.parse_decimal_safely(
                    raw_order.stopLossTriggerPrice, default=None
                )
                if raw_order.stopLossTriggerPrice
                else None,
                sl_limit_price=self.parse_decimal_safely(raw_order.stopLossLimitPrice, default=None)
                if raw_order.stopLossLimitPrice
                else None,
                sl_trigger_by=None,  # Can be mapped from stopLossTriggerBy if needed
                tp_trigger_price=self.parse_decimal_safely(
                    raw_order.takeProfitTriggerPrice, default=None
                )
                if raw_order.takeProfitTriggerPrice
                else None,
                tp_limit_price=self.parse_decimal_safely(
                    raw_order.takeProfitLimitPrice, default=None
                )
                if raw_order.takeProfitLimitPrice
                else None,
                tp_trigger_by=None,  # Can be mapped from takeProfitTriggerBy if needed
                trigger_quantity=self.parse_decimal_safely(raw_order.triggerQuantity, default=None)
                if raw_order.triggerQuantity
                else None,
            )

            # Parse symbol to domain object at entry point
            exchange_symbol = exchanges.backpack(
                value=raw_order.symbol,  # e.g., "BTC_USD_PERP"
                symbol_id=getattr(raw_order, "symbol_id", None),
            )

            # Create Order directly with all parameters
            # SECURITY FIX: Use secure_transform instead of direct instantiation
            order_data: dict[str, Any] = {
                "exchange_order_id": raw_order.id,
                "symbol": exchange_symbol,  # Domain object!
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

        except (ValueError, TypeError, KeyError, AttributeError) as e:
            logger.exception(
                "order_transformation_failed",
                action="transform_order",
                order_id=raw_order.id,
                error=str(e),
                message=f"Failed to transform order {raw_order.id}: {e}",
            )
            raise OrderTransformationError(
                order_id=raw_order.id,
                reason=str(e),
                order_data=None,
                original_error=e,
            ) from e

    def transform_ws_order_update_to_internal_order(
        self,
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
            MissingQuantityError: If quantity is missing or invalid
            OrderTransformationError: If transformation fails

        """
        try:
            # Map enums
            mapped_side = BackpackEnumMappers.map_side_to_internal(raw_order_update.side, "order")
            mapped_type = BackpackOrderMapper._map_type_to_internal(
                raw_order_update.order_type,
                None,
                None,
            )
            mapped_status = BackpackOrderMapper._map_status_to_internal(
                raw_order_update.order_status,
            )
            mapped_tif = BackpackOrderMapper._map_time_in_force(
                raw_order_update.time_in_force or "gtc",
            )

            # Parse quantities
            if raw_order_update.quantity:
                quantity_requested = self.parse_decimal_safely(raw_order_update.quantity)
                if quantity_requested is None:
                    raise MissingQuantityError
                # parse_decimal_value with allow_none=False guarantees non-None result
            else:
                quantity_requested = Decimal(0)

            # For WebSocket order updates, we don't have filled quantity info
            quantity_filled = Decimal(0)

            # Parse price
            order_price = None
            if raw_order_update.price:
                order_price = self.parse_decimal_safely(raw_order_update.price, default=None)

            # Parse timestamps
            event_timestamp = self.parse_timestamp(raw_order_update.event_time)

            if event_timestamp is None:
                event_timestamp = datetime.now(UTC)

            # Parse symbol to domain object at entry point
            exchange_symbol = exchanges.backpack(
                value=raw_order_update.symbol,  # e.g., "BTC_USD_PERP"
                symbol_id=getattr(raw_order_update, "symbol_id", None),
            )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            order_data: dict[str, Any] = {
                "exchange_order_id": (
                    f"ws_order_{raw_order_update.event_type}_{int(event_timestamp.timestamp())}"
                ),
                "symbol": exchange_symbol,  # Domain object!
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

        except (ValueError, TypeError, KeyError, AttributeError) as e:
            raise OrderTransformationError(
                order_id=raw_order_update.client_order_id,
                reason=str(e),
                order_data=None,
                original_error=e,
            ) from e

    @staticmethod
    def _validate_quantity_requested(quantity_requested: Decimal | None) -> None:
        """Validate quantity_requested is not None.

        Args:
            quantity_requested: The quantity to validate

        Raises:
            MissingQuantityError: If quantity is None
        """
        if quantity_requested is None:
            raise MissingQuantityError
