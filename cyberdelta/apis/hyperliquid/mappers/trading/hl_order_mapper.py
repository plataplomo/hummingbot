"""Hyperliquid Order Mapper.

This mapper handles order transformations from the Hyperliquid exchange,
extracted from the monolithic trading data mapper to improve maintainability and testability.

Focused on:
- Raw order to internal Order transformations
- Simple open order transformations
- Historical order transformations
- WebSocket order update transformations
- Order component parsing and validation
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, NoReturn, TypedDict

from cyberdelta.apis.base.protocols.mapper_protocols import CommonDataParserMixin
from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions import (
    MissingRequiredFieldError,
    OrderTransformationError,
)
from cyberdelta.apis.hyperliquid.mappers.account.hl_transaction_mapper import (
    HyperliquidTransactionMapper,
)
from cyberdelta.apis.hyperliquid.mappers.trading.hl_trading_enum_mapper import (
    HyperliquidTradingEnumMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrder,
    HyperliquidRawSimpleOpenOrder,
    HyperliquidRawTriggerInfo,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFill
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import OrderMapperProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import (
    OrderStatus,
    TriggerType,
)
from cyberdelta.enums import (
    OrderSide,
    OrderType,
    TimeInForce,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.models import Order
from cyberdelta.models.market.fill import Fill
from cyberdelta.symbols import exchanges
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


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


class HyperliquidOrderMapper(CommonDataParserMixin, OrderMapperProtocol):
    """Focused mapper for Hyperliquid order transformations.

    Handles transformations of various order formats from Hyperliquid
    into the internal Order model.
    """

    # Protocol method implementations (delegated to common utilities)

    # Protocol-specific methods
    def transform_raw_order_to_internal(self, raw_order: HyperliquidRawOrder) -> Order:
        """Transform raw order data to internal model.

        Args:
            raw_order: Raw order data from API

        Returns:
            Order domain model
        """
        return self._transform_raw_order_to_internal_impl(raw_order, trigger=None)

    def transform_raw_fill_to_internal(self, raw_fill: HyperliquidRawUserFill) -> Fill:
        """Transform raw fill data to internal fill model.

        Args:
            raw_fill: Raw fill data from API

        Returns:
            Fill domain model
        """
        transaction_mapper = HyperliquidTransactionMapper()
        return transaction_mapper.transform_raw_user_fill_to_internal(raw_fill)

    @staticmethod
    def _raise_timestamp_validation_error() -> None:
        """Raise timestamp validation error.

        Raises:
            RuntimeError: Always raised for timestamp validation errors.
        """
        msg = "Internal error: timestamp is None after validation"
        raise RuntimeError(msg)

    @staticmethod
    def _raise_runtime_validation_error(field_name: str) -> NoReturn:
        """Raise runtime validation error for a field.

        Raises:
            RuntimeError: Always raised for field validation errors.
        """
        msg = f"Internal error: {field_name} is None after validation"
        raise RuntimeError(msg)

    # Protocol-specific methods

    @staticmethod
    def _ensure_quantity_not_none(
        quantity: Decimal | None,
        field_name: str,
        context: str,
        raw_data: dict[str, Any],
    ) -> None:
        """Ensure quantity is not None, raise if it is.

        Args:
            quantity: The quantity value to check
            field_name: Name of the field
            context: Context for the error (e.g., "HyperliquidRawOrder")
            raw_data: Raw data for debugging

        Raises:
            MissingRequiredFieldError: If quantity is None
        """
        if quantity is None:
            raise MissingRequiredFieldError(
                field_names=field_name,
                context=context,
                source_data=raw_data,
            )

    @staticmethod
    def _ensure_timestamp_not_none(
        timestamp: datetime | None,
        field_name: str,
        context: str,
        raw_timestamp: object,
    ) -> None:
        """Ensure timestamp is not None, raise if it is.

        Args:
            timestamp: The parsed timestamp to check
            field_name: Name of the field
            context: Context for the error
            raw_timestamp: Raw timestamp value for debugging

        Raises:
            MissingRequiredFieldError: If timestamp is None
        """
        if timestamp is None:
            raise MissingRequiredFieldError(
                field_names=field_name,
                context=context,
                source_data={field_name: raw_timestamp},
            )

    def _transform_raw_order_to_internal_impl(
        self,
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
            TransformationError: If transformation fails during parsing.
            OrderTransformationError: If transformation fails

        """
        try:
            # Parse all order components
            order_components = self._parse_order_components(raw_order, trigger)

            # Create and return the Order object
            return HyperliquidOrderMapper._create_order_from_components(raw_order, order_components)

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            logger.exception(
                "order_transform_failed",
                component="HyperliquidOrderMapper",
                action="transform_raw_order_to_internal",
                error=str(e),
                raw_order=raw_order.model_dump_json(),
            )
            raise OrderTransformationError(
                order_id=None,
                reason=f"Failed to transform HyperliquidRawOrder to Order: {e}",
                order_data=raw_order.model_dump(),
                original_error=e,
            ) from e

    def transform_raw_simple_open_order_to_internal(
        self,
        raw_simple_order: HyperliquidRawSimpleOpenOrder,
    ) -> Order:
        """Transforms a HyperliquidRawSimpleOpenOrder to an Internal Order model.

        This method handles the flat structure returned by the openOrders endpoint,
        which differs from the nested structure of other order endpoints.

        Args:
            raw_simple_order: Validated raw simple order data from Hyperliquid

        Returns:
            Order: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails during parsing.
            OrderTransformationError: If transformation fails
        """
        try:
            # Parse basic components from flat structure
            side = HyperliquidTradingEnumMapper.map_side_to_internal(raw_simple_order.side)

            # For simple open orders, we know they are limit orders and open status
            order_type = OrderType.LIMIT
            status = OrderStatus.OPEN
            time_in_force = TimeInForce.GTC  # Default for open orders

            # Parse quantities and price
            quantity_requested = self.parse_decimal_safely(raw_simple_order.orig_sz)
            quantity_sz = self.parse_decimal_safely(raw_simple_order.sz)

            # Calculate quantity filled if both values are available
            if quantity_requested is not None and quantity_sz is not None:
                quantity_filled = quantity_requested - quantity_sz
            else:
                quantity_filled = Decimal(0)
            price = self.parse_decimal_safely(raw_simple_order.limit_px)

            # For simple orders, these are not available
            average_fill_price = None
            stop_price = None
            trigger_by = None

            # Parse timestamp
            created_at = self.parse_timestamp(raw_simple_order.timestamp)
            updated_at = created_at  # No separate updated timestamp in simple orders

            # Parse symbol to domain object at entry point
            exchange_symbol = exchanges.hyperliquid(
                value=raw_simple_order.coin,  # e.g., "BTC"
                asset_index=getattr(raw_simple_order, "asset_index", None),
            )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            order_data: dict[str, Any] = {
                "exchange_order_id": str(raw_simple_order.oid),
                "symbol": exchange_symbol,  # Domain object!
                "side": side.value,
                "order_type": order_type.value,
                "status": status.value,
                "time_in_force": time_in_force.value,
                "quantity_requested": str(
                    quantity_requested if quantity_requested is not None else Decimal(0),
                ),
                "quantity_filled": str(quantity_filled),
                "price": str(price) if price is not None else None,
                "average_fill_price": str(average_fill_price)
                if average_fill_price is not None
                else None,
                "stop_price": str(stop_price) if stop_price is not None else None,
                "trigger_by": trigger_by.value if trigger_by is not None else None,
                "created_at": (
                    created_at if created_at is not None else datetime.now(UTC)
                ).isoformat(),
                "updated_at": updated_at.isoformat() if updated_at is not None else None,
                "triggered_at": None,  # Not available in simple order format
                "strategy_name": None,  # Not available in simple order format
                "signal_id": None,  # Not available in simple order format
                "exchange": ExchangeName.HYPERLIQUID.value,
                # "client_order_id" not set - will use default UUID generation
                "hl_details": None,  # Could be populated if needed
                "bp_details": None,
                "reduce_only": False,
                "post_only": False,
            }

            return secure_transform(
                data=order_data,
                model_class=Order,
                context="hyperliquid_simple_order_transform",
                source_exchange=ExchangeName.HYPERLIQUID,
            )

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            logger.exception(
                "simple_order_transform_failed",
                component="HyperliquidOrderMapper",
                action="transform_raw_simple_open_order_to_internal",
                error=str(e),
                raw_order=raw_simple_order.model_dump_json(),
            )
            raise OrderTransformationError(
                order_id=None,
                reason=f"Failed to transform HyperliquidRawSimpleOpenOrder to Order: {e}",
                order_data=raw_simple_order.model_dump(),
                original_error=e,
            ) from e

    def transform_raw_historical_order_to_internal(
        self,
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
            TransformationError: If transformation fails during parsing.
            OrderTransformationError: If transformation fails

        """
        try:
            # Parse all historical order components
            order_components = self._parse_historical_order_components(
                raw_historical_order,
                trigger,
            )

            # Create and return the Order object
            return HyperliquidOrderMapper._create_historical_order_from_components(
                raw_historical_order,
                order_components,
            )

        except TransformationError:
            # Re-raise TransformationError as-is
            raise
        except Exception as e:
            logger.exception(
                "historical_order_transform_failed",
                component="HyperliquidOrderMapper",
                action="transform_raw_historical_order_to_internal",
                error=str(e),
                raw_order=raw_historical_order.model_dump_json(),
            )
            raise OrderTransformationError(
                order_id=None,
                reason=f"Failed to transform HyperliquidRawHistoricalOrder to Order: {e}",
                order_data=raw_historical_order.model_dump(),
                original_error=e,
            ) from e

    def transform_ws_order_update_to_internal_order(
        self,
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

        """
        return self._transform_raw_order_to_internal_impl(raw_order, trigger)

    def _parse_order_components(
        self,
        raw_order: HyperliquidRawOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> OrderComponents:
        """Parse all components needed for Order creation.

        Returns:
            OrderComponents: Parsed order components ready for Order creation.
        """
        # Map enums
        side, order_type, status, time_in_force = HyperliquidOrderMapper._parse_order_enums(
            raw_order,
            trigger,
        )

        # Parse quantities and price
        quantity_requested, quantity_filled, price = self._parse_order_quantities_and_price(
            raw_order
        )

        # Parse timestamps
        created_at, updated_at = self._parse_order_timestamps(raw_order)

        # Parse trigger/stop logic
        stop_price, trigger_by = self._parse_trigger_info(trigger)

        # Calculate average_fill_price
        average_fill_price, quantity_filled = HyperliquidOrderMapper._calculate_average_fill_price(
            raw_order,
            quantity_filled,
            price,
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
    def _parse_order_enums(
        raw_order: HyperliquidRawOrder,
        trigger: HyperliquidRawTriggerInfo | None,
    ) -> tuple[OrderSide, OrderType, OrderStatus, TimeInForce]:
        """Parse order enums from raw order data.

        Returns:
            tuple[OrderSide, OrderType, OrderStatus, TimeInForce]: Tuple containing parsed enums.
        """
        side = HyperliquidTradingEnumMapper.map_side_to_internal(raw_order.side)
        order_type = HyperliquidTradingEnumMapper.map_type_to_internal(
            raw_order.order_type,
            trigger,
        )
        status = HyperliquidTradingEnumMapper.map_status_to_internal(raw_order.status)
        time_in_force = HyperliquidTradingEnumMapper.map_time_in_force(raw_order.order_type)
        return side, order_type, status, time_in_force

    def _parse_order_quantities_and_price(
        self,
        raw_order: HyperliquidRawOrder,
    ) -> tuple[Decimal, Decimal, Decimal | None]:
        """Parse quantities and price from raw order data.

        Returns:
            tuple[Decimal, Decimal, Decimal | None]: Tuple containing quantity requested,
                quantity filled, and price.

        Raises:
            TransformationError: If parsing fails.
            OrderTransformationError: If transformation fails.
        """
        try:
            # Parse quantities
            quantity_requested = self.parse_decimal_safely(
                raw_order.sz,
                default=None,
            )
            HyperliquidOrderMapper._ensure_quantity_not_none(
                quantity_requested,
                field_name="sz",
                context="HyperliquidRawOrder",
                raw_data=raw_order.model_dump(),
            )
            # After validation, quantity_requested is guaranteed to be not None
            # Type narrowing: validation function raises if None
            if quantity_requested is None:  # This should never happen after validation
                HyperliquidOrderMapper._raise_runtime_validation_error("quantity_requested")

            remaining_sz = self.parse_decimal_safely(
                str(raw_order.remaining_sz),
                default=None,
            )
            if remaining_sz is None:
                remaining_sz = Decimal(0)

            quantity_filled = quantity_requested - remaining_sz

            # Parse price - handle market orders correctly
            price = self.parse_decimal_safely(
                str(raw_order.limit_px),
                default=None,
            )
            # For market orders, Hyperliquid uses limit_px="0", but internal Order
            # expects price=None
            if price is not None and price == Decimal(0):
                price = None

        except TransformationError:
            raise
        except Exception as e:
            logger.exception(
                "hl_order_mapper_parse_quantities_failed",
                action="parse_order_quantities_and_price",
                message="Failed to parse order quantities and price",
                error=str(e),
            )
            raise OrderTransformationError(
                order_id=None,
                reason=f"Failed to parse order quantities and price: {e}",
                order_data=raw_order.model_dump(),
                original_error=e,
            ) from e
        else:
            return quantity_requested, quantity_filled, price

    def _parse_order_timestamps(
        self,
        raw_order: HyperliquidRawOrder,
    ) -> tuple[datetime, datetime]:
        """Parse order timestamps.

        Returns:
            tuple[datetime, datetime]: Tuple containing created_at and updated_at timestamps.

        Raises:
            TransformationError: If timestamp parsing fails.
            OrderTransformationError: If transformation fails.
        """
        try:
            created_at = self.parse_timestamp(raw_order.timestamp)
            HyperliquidOrderMapper._ensure_timestamp_not_none(
                created_at,
                field_name="timestamp",
                context="HyperliquidRawOrder",
                raw_timestamp=raw_order.timestamp,
            )
            # After validation, created_at is guaranteed to be not None
            # Type narrowing: validation function raises if None
            if created_at is None:  # This should never happen after validation
                HyperliquidOrderMapper._raise_timestamp_validation_error()

            updated_at = self.parse_timestamp(raw_order.status_timestamp)
            if updated_at is None:
                updated_at = created_at

            # At this point both values are guaranteed to be non-None
            # If somehow they are still None, raise an error
            if created_at is None:
                HyperliquidOrderMapper._raise_runtime_validation_error("created_at")
            if updated_at is None:
                HyperliquidOrderMapper._raise_runtime_validation_error("updated_at")

        except TransformationError:
            raise
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            logger.exception(
                "hl_order_mapper_parse_timestamps_failed",
                action="parse_order_timestamps",
                message="Failed to parse order timestamps",
                error=str(e),
            )
            raise OrderTransformationError(
                order_id=None,
                reason=f"Failed to parse order timestamps: {e}",
                order_data=raw_order.model_dump(),
                original_error=e,
            ) from e
        else:
            # Return the validated timestamps
            return created_at, updated_at

    def _parse_trigger_info(
        self,
        trigger: HyperliquidRawTriggerInfo | None,
    ) -> tuple[Decimal | None, TriggerType | None]:
        """Parse trigger/stop logic.

        Returns:
            tuple[Decimal | None, TriggerType | None]: Tuple containing stop price and trigger type.
        """
        stop_price = None
        trigger_by = None

        if trigger:
            stop_price = self.parse_decimal_safely(
                str(getattr(trigger, "trigger_px", "")),
                default=None,
            )

            # Map trigger type if available
            trigger_type_str = getattr(trigger, "trigger_type", None)
            trigger_by = HyperliquidTradingEnumMapper.map_trigger_type(trigger_type_str)

        return stop_price, trigger_by

    @staticmethod
    def _calculate_average_fill_price(
        raw_order: HyperliquidRawOrder,
        quantity_filled: Decimal,
        price: Decimal | None,
    ) -> tuple[Decimal | None, Decimal]:
        """Calculate average_fill_price based on business rules.

        Returns:
            tuple[Decimal | None, Decimal]: Tuple containing average fill price and
                updated quantity filled.
        """
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
                    "order_quantity_filled_reset_due_to_missing_price",
                    action="calculate_average_fill_price",
                    order_id=str(raw_order.oid),
                    quantity_filled=str(quantity_filled),
                    message="Setting quantity_filled=0 due to missing price for consistency",
                )
                quantity_filled = Decimal(0)

        return average_fill_price, quantity_filled

    @staticmethod
    def _create_order_from_components(
        raw_order: HyperliquidRawOrder,
        components: OrderComponents,
    ) -> Order:
        """Create Order object with Symbol domain object.

        Returns:
            Order: The created order object
        """
        # Parse symbol to domain object at entry point
        exchange_symbol = exchanges.hyperliquid(
            value=raw_order.asset,  # e.g., "BTC"
            asset_index=getattr(raw_order, "asset_index", None),
        )

        # SECURITY FIX: Use secure_transform instead of direct instantiation
        order_data: dict[str, Any] = {
            "exchange_order_id": str(raw_order.oid),
            "symbol": exchange_symbol,  # Domain object!
            "exchange": ExchangeName.HYPERLIQUID.value,
            "side": components["side"].value,
            "order_type": components["order_type"].value,
            "status": components["status"].value,
            "quantity_requested": str(components["quantity_requested"]),
            "quantity_filled": str(components["quantity_filled"]),
            "price": str(components["price"]) if components["price"] is not None else None,
            "stop_price": str(components["stop_price"])
            if components["stop_price"] is not None
            else None,
            "average_fill_price": str(components["average_fill_price"])
            if components["average_fill_price"] is not None
            else None,
            "trigger_by": components["trigger_by"].value
            if components["trigger_by"] is not None
            else None,
            "time_in_force": components["time_in_force"].value,
            "created_at": components["created_at"].isoformat(),
            "updated_at": components["updated_at"].isoformat(),
            "triggered_at": None,
            "strategy_name": None,
            "signal_id": None,
            "quote_quantity_requested": None,
            "reduce_only": False,
            "post_only": False,
            "trades": [],
            "hl_details": None,
            "bp_details": None,
        }

        # Include client_order_id if present, otherwise let default factory generate UUID
        if raw_order.cloid is not None:
            order_data["client_order_id"] = raw_order.cloid
        # Do not set client_order_id to None - let the model's default factory handle it

        return secure_transform(
            data=order_data,
            model_class=Order,
            context="hyperliquid_order_transform",
            source_exchange=ExchangeName.HYPERLIQUID,
        )

    def _parse_historical_order_components(
        self,
        raw_historical_order: HyperliquidRawHistoricalOrder,
        trigger: HyperliquidRawTriggerInfo | None = None,
    ) -> OrderComponents:
        """Parse all components needed for historical Order creation.

        Returns:
            OrderComponents: Parsed order components ready for Order creation.
        """
        # Map enums
        side = HyperliquidTradingEnumMapper.map_side_to_internal(raw_historical_order.side)

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

        order_type = HyperliquidTradingEnumMapper.map_type_to_internal(
            order_type_dict,
            trigger,
        )
        status = HyperliquidTradingEnumMapper.map_status_to_internal(
            raw_historical_order.status,
        )
        time_in_force = HyperliquidTradingEnumMapper.map_time_in_force(
            order_type_dict,
        )

        # Parse quantities and price
        quantity_requested, quantity_filled, price = self._parse_historical_quantities_and_price(
            raw_historical_order,
        )

        # Parse timestamps
        created_at, updated_at = self._parse_historical_timestamps(raw_historical_order)

        # Parse trigger/stop logic (reuse existing method)
        stop_price, trigger_by = self._parse_trigger_info(trigger)

        # Calculate average_fill_price (reuse existing method with different order ID)
        average_fill_price, quantity_filled = (
            HyperliquidOrderMapper._calculate_historical_average_fill_price(
                raw_historical_order,
                quantity_filled,
                price,
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

    def _parse_historical_quantities_and_price(
        self,
        raw_historical_order: HyperliquidRawHistoricalOrder,
    ) -> tuple[Decimal, Decimal, Decimal | None]:
        """Parse quantities and price for historical orders.

        Returns:
            tuple[Decimal, Decimal, Decimal | None]: Tuple containing quantity requested,
                quantity filled, and price.

        Raises:
            MissingRequiredFieldError: If required fields are missing.
        """
        # Parse quantities - use orig_sz for quantity_requested (sz is remaining quantity)
        quantity_requested = self.parse_decimal_safely(
            raw_historical_order.orig_sz,
            default=None,
        )
        if quantity_requested is None:
            raise MissingRequiredFieldError("orig_sz", "original quantity")

        # For historical orders, calculate filled quantity from original size and remaining
        remaining_sz = self.parse_decimal_safely(
            str(getattr(raw_historical_order, "remaining_sz", "0")),
            default=Decimal(0),
        )
        # remaining_sz is guaranteed to be Decimal when default is not None
        if remaining_sz is None:
            remaining_sz = Decimal(0)
        quantity_filled = quantity_requested - remaining_sz

        # Parse price - handle market orders correctly
        price = self.parse_decimal_safely(
            str(raw_historical_order.limit_px),
            default=None,
        )
        # For market orders, Hyperliquid uses limit_px="0", but internal Order
        # expects price=None
        if price is not None and price == Decimal(0):
            price = None

        return quantity_requested, quantity_filled, price

    def _parse_historical_timestamps(
        self,
        raw_historical_order: HyperliquidRawHistoricalOrder,
    ) -> tuple[datetime, datetime]:
        """Parse timestamps for historical orders.

        Returns:
            tuple[datetime, datetime]: Tuple containing created_at and updated_at timestamps.

        Raises:
            MissingRequiredFieldError: If timestamp parsing fails.
        """
        created_at = self.parse_timestamp(raw_historical_order.timestamp)
        if created_at is None:
            raise MissingRequiredFieldError(
                field_names="timestamp",
                context="HyperliquidRawHistoricalOrder",
                source_data={"timestamp": raw_historical_order.timestamp},
            )

        # Historical orders might not have separate status timestamp
        updated_at = (
            self.parse_timestamp(getattr(raw_historical_order, "status_timestamp", None))
            or created_at
        )

        return created_at, updated_at

    @staticmethod
    def _calculate_historical_average_fill_price(
        raw_historical_order: HyperliquidRawHistoricalOrder,
        quantity_filled: Decimal,
        price: Decimal | None,
    ) -> tuple[Decimal | None, Decimal]:
        """Calculate average_fill_price for historical orders.

        Returns:
            tuple[Decimal | None, Decimal]: Tuple containing average fill price and
                updated quantity filled.
        """
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
                    "historical_order_quantity_filled_reset_due_to_missing_price",
                    action="calculate_historical_average_fill_price",
                    order_id=str(raw_historical_order.oid),
                    quantity_filled=str(quantity_filled),
                    message="Setting quantity_filled=0 due to missing price for consistency",
                )
                quantity_filled = Decimal(0)

        return average_fill_price, quantity_filled

    @staticmethod
    def _create_historical_order_from_components(
        raw_historical_order: HyperliquidRawHistoricalOrder,
        components: OrderComponents,
    ) -> Order:
        """Create Order object from parsed historical order components.

        Returns:
            Order: The created Order object.
        """
        # Get client order ID
        cloid = getattr(raw_historical_order, "cloid", None)

        # Parse symbol to domain object at entry point
        exchange_symbol = exchanges.hyperliquid(
            value=raw_historical_order.asset,  # e.g., "BTC"
            asset_index=getattr(raw_historical_order, "asset_index", None),
        )

        # SECURITY FIX: Use secure_transform instead of direct instantiation
        order_data: dict[str, Any] = {
            "exchange_order_id": str(raw_historical_order.oid),
            "symbol": exchange_symbol,  # Domain object!
            "exchange": ExchangeName.HYPERLIQUID.value,
            "side": components["side"].value,
            "order_type": components["order_type"].value,
            "status": components["status"].value,
            "quantity_requested": str(components["quantity_requested"]),
            "quantity_filled": str(components["quantity_filled"]),
            "price": str(components["price"]) if components["price"] is not None else None,
            "stop_price": str(components["stop_price"])
            if components["stop_price"] is not None
            else None,
            "average_fill_price": str(components["average_fill_price"])
            if components["average_fill_price"] is not None
            else None,
            "trigger_by": components["trigger_by"].value
            if components["trigger_by"] is not None
            else None,
            "time_in_force": components["time_in_force"].value,
            "created_at": components["created_at"].isoformat(),
            "updated_at": components["updated_at"].isoformat(),
            "triggered_at": None,
            "strategy_name": None,
            "signal_id": None,
            "quote_quantity_requested": None,
            "reduce_only": False,
            "post_only": False,
            "trades": [],
            "hl_details": None,
            "bp_details": None,
        }

        # Include client_order_id if present, otherwise let default factory generate UUID
        if cloid is not None:
            order_data["client_order_id"] = cloid
        # Do not set client_order_id to None - let the model's default factory handle it

        return secure_transform(
            data=order_data,
            model_class=Order,
            context="hyperliquid_historical_order_transform",
            source_exchange=ExchangeName.HYPERLIQUID,
        )
