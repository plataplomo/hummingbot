"""Order data screening and validation components."""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime as dt, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, NamedTuple

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.validation.screening.base.base_validator import BaseValidator
from cyberdelta.enums import OrderSide, OrderType, TimeInForce


if TYPE_CHECKING:
    from cyberdelta.core.models import Order
else:
    # Import Order at runtime for isinstance checks
    from cyberdelta.core.models import Order

logger = get_logger(__name__)

# Constants
MIN_ORDER_ID_LENGTH = 3
MAX_ORDER_ID_LENGTH = 100
MIN_CLIENT_ORDER_ID_LENGTH = 1
MAX_CLIENT_ORDER_ID_LENGTH = 100
MIN_SYMBOL_LENGTH = 2
MAX_SYMBOL_LENGTH = 20
MIN_EXCHANGE_ID_LENGTH = 2
MAX_EXCHANGE_ID_LENGTH = 50
DECIMAL_PRECISION_OFFSET = -8


class OrderValidationResult(NamedTuple):
    """Result of order validation."""

    is_valid: bool
    errors: list[str]
    warnings: list[str]
    cleaned_order: Order | None = None


class OrderDataValidator(BaseValidator):
    """Validates and screens order data before processing.

    Provides comprehensive validation of order data including
    prices, quantities, order types, and status validation.
    """

    def __init__(
        self,
        name: str = "OrderDataScreener",
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the order data screener.

        Args:
            name: Screener name
            config: Configuration dictionary
        """
        cfg = config or {}
        super().__init__(name, config)

        # Order validation configuration
        self.allow_zero_quantities = cfg.get("allow_zero_quantities", False)
        self.allow_negative_prices = cfg.get("allow_negative_prices", False)
        self.max_order_price = Decimal(cfg.get("max_order_price", "1000000"))
        self.max_order_quantity = Decimal(cfg.get("max_order_quantity", "1000000"))
        self.min_order_price = Decimal(cfg.get("min_order_price", "0.00001"))
        self.min_order_quantity = Decimal(cfg.get("min_order_quantity", "0.00001"))
        self.max_order_value = Decimal(cfg.get("max_order_value", "100000000"))

        # Order type validation
        self.allowed_order_types = cfg.get(
            "allowed_order_types",
            ["MARKET", "LIMIT", "STOP", "STOP_LIMIT", "TAKE_PROFIT", "TAKE_PROFIT_LIMIT"],
        )
        self.allowed_order_sides = cfg.get("allowed_order_sides", ["BUY", "SELL"])
        self.allowed_time_in_force = cfg.get("allowed_time_in_force", ["GTC", "IOC", "FOK", "DAY"])

        # Order status validation
        self.allowed_order_statuses = cfg.get(
            "allowed_order_statuses",
            ["NEW", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED", "EXPIRED"],
        )

        # Symbol and exchange validation
        self.symbol_validation_enabled = cfg.get("symbol_validation_enabled", True)
        self.require_exchange_id = cfg.get("require_exchange_id", True)
        self.allowed_exchanges = cfg.get("allowed_exchanges", [])

        # Order metadata validation
        self.require_order_id = cfg.get("require_order_id", True)
        self.require_client_order_id = cfg.get("require_client_order_id", False)
        self.validate_timestamps = cfg.get("validate_timestamps", True)

        logger.info(
            "order_data_screener_created",
            screener_name=name,
            allow_zero_quantities=self.allow_zero_quantities,
            allow_negative_prices=self.allow_negative_prices,
            max_order_price=self.max_order_price,
        )

    async def _initialize_internal(self) -> None:
        """Initialize internal state."""
        logger.info("order_data_screener_initializing")

    async def _shutdown_internal(self) -> None:
        """Shutdown internal state."""
        logger.info("order_data_screener_shutting_down")

    async def validate_order(self, order: Order | None) -> OrderValidationResult:
        """Validate order data comprehensively.

        Args:
            order: Order object to validate

        Returns:
            OrderValidationResult with validation details
        """
        self._ensure_initialized()

        errors: list[str] = []
        warnings: list[str] = []

        # Basic structure validation
        if order is None:
            errors.append("Order object is None")
            return OrderValidationResult(
                is_valid=False,
                errors=errors,
                warnings=warnings,
            )

        # Validate required fields
        field_errors = await self._validate_required_fields(order)
        errors.extend(field_errors)

        # Validate order ID
        if self.require_order_id:
            id_errors = await self._validate_order_id(order)
            errors.extend(id_errors)

        # Validate client order ID
        if self.require_client_order_id:
            client_id_errors = await self._validate_client_order_id(order)
            errors.extend(client_id_errors)

        # Validate symbol
        if self.symbol_validation_enabled:
            symbol_errors, symbol_warnings = await self._validate_symbol(order)
            errors.extend(symbol_errors)
            warnings.extend(symbol_warnings)

        # Validate exchange
        if self.require_exchange_id:
            exchange_errors = await self._validate_exchange(order)
            errors.extend(exchange_errors)

        # Validate order type
        type_errors = await self._validate_order_type(order)
        errors.extend(type_errors)

        # Validate order side
        side_errors = await self._validate_order_side(order)
        errors.extend(side_errors)

        # Validate order status
        status_errors = await self._validate_order_status(order)
        errors.extend(status_errors)

        # Validate price
        price_errors, price_warnings = await self._validate_price(order)
        errors.extend(price_errors)
        warnings.extend(price_warnings)

        # Validate quantity
        quantity_errors, quantity_warnings = await self._validate_quantity(order)
        errors.extend(quantity_errors)
        warnings.extend(quantity_warnings)

        # Validate time in force
        tif_errors = await self._validate_time_in_force(order)
        errors.extend(tif_errors)

        # Validate timestamps
        if self.validate_timestamps:
            timestamp_errors, timestamp_warnings = await self._validate_timestamps(order)
            errors.extend(timestamp_errors)
            warnings.extend(timestamp_warnings)

        # Cross-field validation
        cross_errors, cross_warnings = await self._validate_cross_fields(order)
        errors.extend(cross_errors)
        warnings.extend(cross_warnings)

        is_valid = len(errors) == 0

        # Update statistics
        self._update_validation_stats(is_valid, len(errors), len(warnings))

        if not is_valid:
            logger.warning(
                "order_validation_failed",
                order_id=order.client_order_id or "unknown",
                symbol=order.symbol or "unknown",
                exchange_id=order.exchange,
                error_count=len(errors),
                warning_count=len(warnings),
            )
        elif warnings:
            logger.info(
                "order_validation_warnings",
                order_id=order.client_order_id or "unknown",
                symbol=order.symbol or "unknown",
                exchange_id=order.exchange,
                warning_count=len(warnings),
            )

        return OrderValidationResult(
            is_valid=is_valid,
            errors=errors,
            warnings=warnings,
            cleaned_order=order if is_valid else None,
        )

    async def sanitize_order(self, order: Order) -> Order:
        """Sanitize order data by cleaning common issues.

        Args:
            order: Order object to sanitize

        Returns:
            Sanitized order object
        """
        self._ensure_initialized()

        # Create a copy to avoid modifying the original
        sanitized = order

        # Clean basic fields
        self._clean_basic_order_fields(sanitized)

        # Clean enum fields
        self._clean_order_enums(sanitized)

        # Clean numeric fields
        self._clean_order_numeric_fields(sanitized)

        # Clean additional fields
        self._clean_additional_order_fields(sanitized)

        logger.debug(
            "order_sanitized",
            order_id=sanitized.client_order_id,
            symbol=sanitized.symbol,
            exchange_id=sanitized.exchange,
        )

        return sanitized

    def _clean_basic_order_fields(self, order: Order) -> None:
        """Clean basic string fields."""
        # Symbol is now an Symbol domain object, no cleaning needed
        # Symbol validation happens at creation time

        if order.exchange:
            order.exchange = order.exchange.strip().lower()

    def _clean_order_enums(self, order: Order) -> None:
        """Clean enum fields."""
        # Clean order type
        if order.order_type and isinstance(order.order_type, str):
            with contextlib.suppress(ValueError):
                order.order_type = OrderType(order.order_type.strip().upper())

        # Clean order side
        if order.side and isinstance(order.side, str):
            with contextlib.suppress(ValueError):
                order.side = OrderSide(order.side.strip().upper())

        # Clean order status
        if order.status and isinstance(order.status, str):
            with contextlib.suppress(ValueError):
                order.status = OrderStatus(order.status.strip().upper())

    def _clean_order_numeric_fields(self, order: Order) -> None:
        """Clean numeric fields."""
        # Ensure decimal precision for price and quantity
        if order.price is not None:
            order.price = Decimal(str(order.price))

        # quantity_requested is the Order model field
        order.quantity_requested = Decimal(str(order.quantity_requested))

    def _clean_additional_order_fields(self, order: Order) -> None:
        """Clean additional order fields."""
        # Clean time in force
        if order.time_in_force and isinstance(order.time_in_force, str):
            with contextlib.suppress(ValueError):
                order.time_in_force = TimeInForce(order.time_in_force.strip().upper())

        # Ensure decimal precision for quantity_filled
        order.quantity_filled = Decimal(str(order.quantity_filled))

    async def _validate_required_fields(self, order: Order) -> list[str]:
        """Validate that required fields are present.

        Args:
            order: Order object

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # Order is a concrete type, no protocol check needed

        # Validate required fields - Order model guarantees these are not None
        # client_order_id has default_factory, so it's always present
        if not order.client_order_id.strip():
            errors.append("Required field client_order_id is empty")

        # Symbol is now an Symbol which is validated at creation
        # No need to check if empty since Symbol.value has min_length=1

        # side, order_type, status are enums and required - no None check needed
        # quantity_requested is required with gt=Decimal(0) - always positive
        if not order.exchange.strip():
            errors.append("Required field exchange is empty")

        return errors

    async def _validate_order_id(self, order: Order) -> list[str]:
        """Validate order ID format.

        Args:
            order: Order object

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # Order is a concrete type with all fields

        if not order.client_order_id:
            errors.append("Order ID is required")
            return errors

        order_id = str(order.client_order_id).strip()

        # Basic length validation
        if len(order_id) < MIN_ORDER_ID_LENGTH:
            errors.append("Order ID too short (minimum 3 characters)")
        elif len(order_id) > MAX_ORDER_ID_LENGTH:
            errors.append("Order ID too long (maximum 100 characters)")

        return errors

    async def _validate_client_order_id(self, order: Order) -> list[str]:
        """Validate client order ID format.

        Args:
            order: Order object

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # Check if order supports client order ID through protocol
        if order.client_order_id:
            client_order_id = str(order.client_order_id).strip()

            # Basic length validation
            if len(client_order_id) < MIN_CLIENT_ORDER_ID_LENGTH:
                errors.append("Client order ID cannot be empty")
            elif len(client_order_id) > MAX_CLIENT_ORDER_ID_LENGTH:
                errors.append("Client order ID too long (maximum 100 characters)")

        return errors

    async def _validate_symbol(self, order: Order) -> tuple[list[str], list[str]]:
        """Validate trading symbol format.

        Args:
            order: Order object

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Order is a concrete type with all fields

        if not order.symbol:
            errors.append("Symbol is required")
            return errors, warnings

        symbol = str(order.symbol).strip().upper()

        # Basic format validation
        if len(symbol) < MIN_SYMBOL_LENGTH:
            errors.append("Symbol too short (minimum 2 characters)")
        elif len(symbol) > MAX_SYMBOL_LENGTH:
            errors.append("Symbol too long (maximum 20 characters)")

        # Character validation
        if not symbol.replace("-", "").replace("/", "").replace("_", "").isalnum():
            errors.append("Symbol contains invalid characters")

        # Common format warnings
        if not any(sep in symbol for sep in ["-", "/", "_"]):
            warnings.append("Symbol may not be a trading pair (no separator found)")

        return errors, warnings

    async def _validate_exchange(self, order: Order) -> list[str]:
        """Validate exchange ID.

        Args:
            order: Order object

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # Order is a concrete type with all fields

        if not order.exchange:
            errors.append("Exchange is required")
            return errors

        exchange_id = str(order.exchange).strip().lower()

        # Allowed exchanges validation
        if self.allowed_exchanges and exchange_id not in self.allowed_exchanges:
            errors.append(f"Exchange {exchange_id} not in allowed list: {self.allowed_exchanges}")

        # Basic format validation
        if len(exchange_id) < MIN_EXCHANGE_ID_LENGTH:
            errors.append("Exchange ID too short")
        elif len(exchange_id) > MAX_EXCHANGE_ID_LENGTH:
            errors.append("Exchange ID too long")

        return errors

    async def _validate_order_type(self, order: Order) -> list[str]:
        """Validate order type.

        Args:
            order: Order object

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # Order is a concrete type with all fields
        # order_type is an enum and required - cannot be None

        order_type = str(order.order_type).strip().upper()

        if order_type not in self.allowed_order_types:
            errors.append(
                f"Invalid order type '{order_type}'. Must be one of: {self.allowed_order_types}"
            )

        return errors

    async def _validate_order_side(self, order: Order) -> list[str]:
        """Validate order side.

        Args:
            order: Order object

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # Order is a concrete type with all fields
        # side is an enum and required - cannot be None

        side = str(order.side).strip().upper()

        if side not in self.allowed_order_sides:
            errors.append(
                f"Invalid order side '{side}'. Must be one of: {self.allowed_order_sides}"
            )

        return errors

    async def _validate_order_status(self, order: Order) -> list[str]:
        """Validate order status.

        Args:
            order: Order object

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # Order is a concrete type with all fields
        # status is an enum with default=OrderStatus.NEW - cannot be None

        status = str(order.status).strip().upper()

        if status not in self.allowed_order_statuses:
            errors.append(
                f"Invalid order status '{status}'. Must be one of: {self.allowed_order_statuses}"
            )

        return errors

    async def _validate_price(self, order: Order) -> tuple[list[str], list[str]]:
        """Validate order price.

        Args:
            order: Order object

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Order is a concrete type with all fields

        # Price may be optional for market orders
        if order.price is None:
            # Check if this is a market order
            if str(order.order_type).upper() == "MARKET":
                return errors, warnings
            errors.append("Price is required for non-market orders")
            return errors, warnings

        try:
            price = Decimal(str(order.price))
        except (ValueError, TypeError):
            errors.append("Price must be a valid decimal number")
            return errors, warnings

        # Zero price validation
        if price == 0:
            errors.append("Price cannot be zero")

        # Negative price validation
        if price < 0 and not self.allow_negative_prices:
            errors.append("Negative prices are not allowed")

        # Range validation
        if price > 0 and price < self.min_order_price:
            errors.append(f"Price {price} below minimum {self.min_order_price}")
        elif price > self.max_order_price:
            errors.append(f"Price {price} above maximum {self.max_order_price}")

        # Precision warnings
        exponent = price.as_tuple().exponent
        if isinstance(exponent, int) and exponent < DECIMAL_PRECISION_OFFSET:
            warnings.append("Price has very high precision (>8 decimal places)")

        return errors, warnings

    async def _validate_quantity(self, order: Order) -> tuple[list[str], list[str]]:
        """Validate order quantity.

        Args:
            order: Order object

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Order is a concrete type with all fields
        # quantity_requested is required with gt=Decimal(0) - always positive, warnings

        try:
            quantity = Decimal(str(order.quantity_requested))
        except (ValueError, TypeError):
            errors.append("Quantity must be a valid decimal number")
            return errors, warnings

        # Zero quantity validation
        if quantity == 0 and not self.allow_zero_quantities:
            errors.append("Zero quantities are not allowed")

        # Negative quantity validation
        if quantity < 0:
            errors.append("Quantity cannot be negative")

        # Range validation
        if quantity > 0 and quantity < self.min_order_quantity:
            errors.append(f"Quantity {quantity} below minimum {self.min_order_quantity}")
        elif quantity > self.max_order_quantity:
            errors.append(f"Quantity {quantity} above maximum {self.max_order_quantity}")

        # Precision warnings
        exponent = quantity.as_tuple().exponent
        if isinstance(exponent, int) and exponent < DECIMAL_PRECISION_OFFSET:
            warnings.append("Quantity has very high precision (>8 decimal places)")

        return errors, warnings

    async def _validate_time_in_force(self, order: Order) -> list[str]:
        """Validate time in force.

        Args:
            order: Order object

        Returns:
            List of validation errors
        """
        errors: list[str] = []

        # Check if order supports time_in_force through protocol
        if order.time_in_force:
            tif = str(order.time_in_force).strip().upper()

            if tif not in self.allowed_time_in_force:
                errors.append(
                    f"Invalid time in force '{tif}'. Must be one of: {self.allowed_time_in_force}"
                )

        return errors

    async def _validate_timestamps(self, order: Order) -> tuple[list[str], list[str]]:
        """Validate order timestamps.

        Args:
            order: Order object

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Check timestamps for orders with metadata
        # Order has all metadata fields
        timestamp_fields = [("created_at", order.created_at), ("updated_at", order.updated_at)]

        for field_name, timestamp in timestamp_fields:
            if timestamp is not None:
                # Order model uses datetime for created_at/updated_at
                # created_at is always datetime, updated_at can be datetime or None
                # No need to check for other types since Order model guarantees these types
                # Range validation
                current_time = dt.now(UTC)
                one_year_ago = current_time - timedelta(days=365)
                one_hour_future = current_time + timedelta(hours=1)

                # timestamp is guaranteed to be datetime by Order model
                if timestamp < one_year_ago:
                    warnings.append(f"{field_name} is more than one year old")
                elif timestamp > one_hour_future:
                    warnings.append(f"{field_name} is more than one hour in the future")

        return errors, warnings

    async def _validate_cross_fields(self, order: Order) -> tuple[list[str], list[str]]:
        """Validate relationships between fields.

        Args:
            order: Order object

        Returns:
            Tuple of (errors, warnings)
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Validate order value
        value_errors, value_warnings = self._validate_order_value(order)
        errors.extend(value_errors)
        warnings.extend(value_warnings)

        # Validate quantities
        qty_errors = self._validate_order_quantities(order)
        errors.extend(qty_errors)

        # Validate order type and price relationship
        type_errors, type_warnings = self._validate_order_type_price_relationship(order)
        errors.extend(type_errors)
        warnings.extend(type_warnings)

        # Validate timestamp consistency
        timestamp_errors = self._validate_order_timestamps(order)
        errors.extend(timestamp_errors)

        return errors, warnings

    def _validate_order_value(self, order: Order) -> tuple[list[str], list[str]]:
        """Validate order value calculations.

        Args:
            order: Order object to validate

        Returns:
            Tuple of (validation errors, warnings) for order value
        """
        errors: list[str] = []
        warnings: list[str] = []

        try:
            price = Decimal(str(order.price)) if order.price else Decimal(0)
            quantity = Decimal(str(order.quantity_requested))

            if price > 0 and quantity > 0:
                order_value = price * quantity

                # Check for extremely large order values
                if order_value > self.max_order_value:
                    errors.append(
                        f"Order value {order_value} exceeds maximum {self.max_order_value}"
                    )

                # Check for extremely small order values
                if order_value < Decimal("0.01"):  # 1 cent
                    warnings.append(f"Very small order value: {order_value}")

        except (ValueError, TypeError) as e:
            # Log error for debugging but continue validation
            errors.append(f"Error validating order value calculations: {e}")

        return errors, warnings

    def _validate_order_quantities(self, order: Order) -> list[str]:
        """Validate order quantity relationships.

        Args:
            order: Order object to validate

        Returns:
            List of validation errors for quantity relationships
        """
        errors: list[str] = []

        try:
            filled_qty = (
                Decimal(str(order.quantity_filled)) if order.quantity_filled else Decimal(0)
            )
            total_qty = Decimal(str(order.quantity_requested))

            if filled_qty > total_qty:
                errors.append(f"Filled quantity {filled_qty} exceeds total quantity {total_qty}")
            elif filled_qty < 0:
                errors.append(f"Filled quantity cannot be negative: {filled_qty}")

        except (ValueError, TypeError):
            # Already handled in individual field validation
            pass

        return errors

    def _validate_order_type_price_relationship(self, order: Order) -> tuple[list[str], list[str]]:
        """Validate order type and price relationship.

        Args:
            order: Order object to validate

        Returns:
            Tuple of (validation errors, warnings) for order type/price relationship
        """
        errors: list[str] = []
        warnings: list[str] = []

        order_type = str(order.order_type).upper()
        order_price: Decimal | None = order.price

        if order_type == "MARKET" and order_price is not None:
            warnings.append("Market orders typically don't have a price")
        elif order_type in {"LIMIT", "STOP_LIMIT"} and order_price is None:
            errors.append(f"{order_type} orders require a price")

        return errors, warnings

    def _validate_order_timestamps(self, order: Order) -> list[str]:
        """Validate order timestamp consistency.

        Args:
            order: Order object to validate

        Returns:
            List of validation errors for timestamp consistency
        """
        errors: list[str] = []

        created_at = order.created_at
        updated_at = order.updated_at

        if updated_at is not None and updated_at < created_at:
            errors.append("Order updated_at cannot be before created_at")

        return errors

    async def validate_order_list(self, orders: list[Order]) -> dict[str, Any]:
        """Validate a list of orders and return aggregate results.

        Args:
            orders: List of orders to validate

        Returns:
            Dictionary with aggregate validation results
        """
        if not orders:
            return {
                "total_orders": 0,
                "valid_orders": 0,
                "invalid_orders": 0,
                "total_errors": 0,
                "total_warnings": 0,
                "validation_results": [],
                "status_summary": {},
                "type_summary": {},
                "exchange_summary": {},
            }

        results: list[OrderValidationResult] = []
        total_errors = 0
        total_warnings = 0
        valid_count = 0
        status_summary: dict[str, int] = {}
        type_summary: dict[str, int] = {}
        exchange_summary: dict[str, int] = {}

        for order in orders:
            result = await self.validate_order(order)
            results.append(result)

            total_errors += len(result.errors)
            total_warnings += len(result.warnings)

            if result.is_valid:
                valid_count += 1

                # Track order statistics
                status = order.status.value if order.status else "unknown"
                order_type = order.order_type.value if order.order_type else "unknown"
                exchange = order.exchange

                status_summary[status] = status_summary.get(status, 0) + 1
                type_summary[order_type] = type_summary.get(order_type, 0) + 1
                exchange_summary[exchange] = exchange_summary.get(exchange, 0) + 1

        return {
            "total_orders": len(orders),
            "valid_orders": valid_count,
            "invalid_orders": len(orders) - valid_count,
            "total_errors": total_errors,
            "total_warnings": total_warnings,
            "validation_results": results,
            "status_summary": status_summary,
            "type_summary": type_summary,
            "exchange_summary": exchange_summary,
        }

    def get_validation_stats(self) -> dict[str, Any]:
        """Get validation statistics.

        Returns:
            Dictionary with validation statistics
        """
        base_stats = super().get_validation_stats()

        return {
            **base_stats,
            "allow_zero_quantities": self.allow_zero_quantities,
            "allow_negative_prices": self.allow_negative_prices,
            "max_order_price": str(self.max_order_price),
            "max_order_quantity": str(self.max_order_quantity),
            "min_order_price": str(self.min_order_price),
            "min_order_quantity": str(self.min_order_quantity),
            "max_order_value": str(self.max_order_value),
            "allowed_order_types": self.allowed_order_types,
            "allowed_order_sides": self.allowed_order_sides,
            "allowed_time_in_force": self.allowed_time_in_force,
            "allowed_order_statuses": self.allowed_order_statuses,
            "symbol_validation_enabled": self.symbol_validation_enabled,
            "require_exchange_id": self.require_exchange_id,
            "require_order_id": self.require_order_id,
            "require_client_order_id": self.require_client_order_id,
            "validate_timestamps": self.validate_timestamps,
            "allowed_exchanges": self.allowed_exchanges,
        }
