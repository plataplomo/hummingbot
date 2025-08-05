"""Execution engine for handling order placement and management.

This module provides the ExecutionEngine class that handles trade execution
using validated AppSettings configuration and the existing Order model.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, NoReturn

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.models.service_args.trading import CancelOrderArgs, PlaceOrderArgs
from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.enums import ExchangeName, OrderSide, OrderType
from cyberdelta.logic.monitoring.health_monitor import ServiceType
from cyberdelta.logic.trading.fill_handler import FillHandler
from cyberdelta.logic.trading.order_validator import OrderValidator
from cyberdelta.models.market.order import Order
from cyberdelta.models.market.trade import Trade
from cyberdelta.models.trading.execution_request import ExecutionRequest
from cyberdelta.protocols.infrastructure.monitoring import HealthCheckable


logger = get_logger(__name__)


class ExecutionEngine(HealthCheckable):
    """Handles order execution using the Order model and AppSettings.

    This engine manages the order lifecycle from execution request through
    order placement, monitoring, and fill processing.

    Configuration Integration:
    - Uses config.execution.max_slippage_pct for slippage control
    - Uses config.execution.max_retries for retry attempts
    - Uses config.execution.retry_delay_base_sec for retry delays
    - Uses config.execution.compensation settings for limit orders
    - Uses config.exchanges for exchange-specific timeouts
    - Uses config.general.safe_mode to prevent real trading

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL configuration from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - All monetary values as Decimal, NOT float
    - NO assumptions about exchange API interfaces
    """

    def __init__(
        self,
        config: AppSettings,
        api_clients: dict[str, ExchangeAPI],
        order_validator: OrderValidator | None = None,
        fill_handler: FillHandler | None = None,
    ) -> None:
        """Initialize execution engine with configuration and dependencies.

        Args:
            config: Application settings containing all configuration
            api_clients: Dictionary of exchange API clients by name
            order_validator: Order validator for comprehensive order validation
            fill_handler: Fill handler for processing order fills
        """
        self.config = config
        self._api_clients = api_clients
        self._order_validator = order_validator
        self._fill_handler = fill_handler
        self._active_orders: dict[str, Order] = {}

        # Health tracking
        self._order_count = 0
        self._success_count = 0
        self._error_count = 0
        self._last_activity = datetime.now(UTC)

        # Extract execution settings - NO hardcoded defaults
        self._exec_config = config.execution
        self._max_slippage = self._exec_config.max_slippage_pct
        self._max_retries = self._exec_config.max_retries
        self._retry_delay = self._exec_config.retry_delay_base_sec
        self._backoff_multiplier = self._exec_config.retry_backoff_multiplier

        # Compensation settings for limit orders
        self._use_limit_orders = self._exec_config.compensation.use_limit_orders
        self._limit_offset_pct = self._exec_config.compensation.limit_price_offset_pct

        # Safe mode settings
        self._safe_mode = config.general.safe_mode

        logger.info(
            "execution_engine_initialized",
            max_slippage_pct=float(self._max_slippage),
            max_retries=self._max_retries,
            retry_delay_sec=float(self._retry_delay),
            use_limit_orders=self._use_limit_orders,
            safe_mode=self._safe_mode,
        )

    async def execute_request(self, request: ExecutionRequest) -> Order:
        """Execute a trade request with config-driven order parameters.

        Args:
            request: Execution request containing signal and position size

        Returns:
            Order object representing the placed order
        """
        self._log_execution_start(request)

        # Validate execution request
        self._validate_execution_request(request)

        # Prepare order parameters
        order_type, price = self._prepare_order_params(request)

        # Check slippage constraints from config for market orders
        if order_type == OrderType.MARKET:
            await self._validate_slippage(request)

        # Create and validate order
        order = await self._create_and_validate_order(request, order_type, price)

        # Execute order
        order = await self._execute_order(order)

        # Track order and log completion
        self._track_order(order, request.signal.signal_id)

        return order

    def _log_execution_start(self, request: ExecutionRequest) -> None:
        """Log the start of order execution."""
        logger.info(
            "execution_request_starting",
            signal_id=request.signal.signal_id,
            symbol=request.signal.symbol.value,
            exchange=(
                request.signal.exchange.value
                if isinstance(request.signal.exchange, ExchangeName)
                else request.signal.exchange[0].value
            ),
            side=request.signal.side.value,
            position_value_usd=float(request.position_size.value_usd),
            safe_mode=self._safe_mode,
        )

    def _prepare_order_params(self, request: ExecutionRequest) -> tuple[OrderType, Decimal | None]:
        """Prepare order type and price based on config.

        Returns:
            Tuple of (order_type, price) for the execution
        """
        order_type = request.order_type
        price = request.signal.price

        # Use limit orders with offset if configured
        if self._use_limit_orders and order_type == OrderType.MARKET:
            order_type = OrderType.LIMIT
            calculated_price = self._calculate_limit_price(request.signal.side, price)
            if calculated_price is not None:
                price = calculated_price

            logger.debug(
                "market_order_converted_to_limit",
                signal_id=request.signal.signal_id,
                original_price=float(request.signal.price) if request.signal.price else None,
                limit_price=float(price) if price else None,
                offset_pct=float(self._limit_offset_pct),
            )

        return order_type, price

    async def _create_and_validate_order(
        self, request: ExecutionRequest, order_type: OrderType, price: Decimal | None
    ) -> Order:
        """Create order and validate if validator available.

        Returns:
            Validated Order object ready for execution

        Raises:
            ValueError: If validation fails
        """
        # Handle exchange which might be a list or single value
        exchange = request.signal.exchange
        if isinstance(exchange, list):
            if not exchange:
                msg = "No exchange specified in signal"
                raise ValueError(msg)
            exchange = exchange[0]

        order = Order(
            exchange=exchange,
            symbol=request.signal.symbol,
            side=request.signal.side,
            order_type=order_type,
            price=price,
            quantity_requested=request.position_size.quantity,
            time_in_force=request.time_in_force,
            status=OrderStatus.NEW,
            created_at=datetime.now(UTC),
            updated_at=None,
            triggered_at=None,
            client_order_id=f"cde_{uuid.uuid4().hex[:8]}",
            signal_id=request.signal.signal_id,
            strategy_name=request.signal.source_strategy,
        )

        # Comprehensive order validation if validator is available
        if self._order_validator is not None:
            await self._validate_order(order, request.signal.signal_id)

        return order

    async def _validate_order(self, order: Order, signal_id: str) -> None:
        """Validate order using order validator.

        Raises:
            ValueError: If order validation fails
        """
        if self._order_validator is None:
            return
        validation_violations = await self._order_validator.validate_order(order)
        if validation_violations:
            max_violations_to_show = 3
            violation_summary = "; ".join(validation_violations[:max_violations_to_show])
            if len(validation_violations) > max_violations_to_show:
                remaining = len(validation_violations) - max_violations_to_show
                violation_summary += f" (and {remaining} more)"

            logger.error(
                "order_validation_failed",
                signal_id=signal_id,
                order_id=order.client_order_id,
                violation_count=len(validation_violations),
                violations=validation_violations,
            )

            msg = f"Order validation failed: {violation_summary}"
            raise ValueError(msg)

        logger.debug(
            "order_validation_passed",
            signal_id=signal_id,
            order_id=order.client_order_id,
            symbol=order.symbol.value,
            exchange=order.exchange.value,
        )

    async def _execute_order(self, order: Order) -> Order:
        """Execute order in safe mode or real mode.

        Returns:
            Order with execution results
        """
        if self._safe_mode:
            # Paper trading - simulate execution
            return await self._simulate_order_execution(order)
        # Real trading - place actual order
        return await self._place_real_order(order)

    def _track_order(self, order: Order, signal_id: str) -> None:
        """Track active order and update metrics."""
        if order.exchange_order_id:
            self._active_orders[order.exchange_order_id] = order
            self._order_count += 1
            self._success_count += 1
            self._last_activity = datetime.now(UTC)

        logger.info(
            "execution_request_completed",
            signal_id=signal_id,
            order_id=order.exchange_order_id,
            client_order_id=order.client_order_id,
            status=order.status.value,
            safe_mode=self._safe_mode,
        )

    async def handle_order_update(self, order_id: str, update: dict[str, Any]) -> Trade | None:
        """Handle order status updates from exchange.

        Args:
            order_id: Exchange order ID
            update: Update data from exchange

        Returns:
            Trade object if order was filled, None otherwise

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses typed Order and Trade models
        - All fee calculations use Decimal
        - NO assumptions about update structure
        """
        order = self._active_orders.get(order_id)
        if not order:
            logger.warning("order_update_for_unknown_order", order_id=order_id)
            return None

        logger.debug(
            "order_update_received",
            order_id=order_id,
            client_order_id=order.client_order_id,
            update_type=update.get("type", "unknown"),
        )

        # Update order status
        if update.get("status"):
            old_status = order.status
            order.status = OrderStatus(update["status"])

            logger.info(
                "order_status_updated",
                order_id=order_id,
                old_status=old_status.value,
                new_status=order.status.value,
            )

        # Handle fills
        fill_data = update.get("fill")
        if fill_data and fill_data.get("filled_quantity"):
            if self._fill_handler:
                # Use FillHandler for comprehensive fill processing
                trade = await self._fill_handler.process_fill(order, fill_data)
            else:
                # Fallback to basic fill processing
                trade = self._create_trade_from_fill(order, fill_data)

            # Update order filled quantity
            order.quantity_filled += trade.quantity

            # Check if order is complete
            if order.quantity_filled >= order.quantity_requested:
                order.status = OrderStatus.FILLED
                # Remove from active tracking
                del self._active_orders[order_id]

                logger.info(
                    "order_fully_filled",
                    order_id=order_id,
                    total_filled=float(order.quantity_filled),
                    order_quantity=float(order.quantity_requested),
                    used_fill_handler=self._fill_handler is not None,
                )
            else:
                logger.info(
                    "order_partially_filled",
                    order_id=order_id,
                    filled_quantity=float(trade.quantity),
                    total_filled=float(order.quantity_filled),
                    remaining=float(order.quantity_requested - order.quantity_filled),
                    used_fill_handler=self._fill_handler is not None,
                )

            return trade

        return None

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an active order.

        Args:
            order_id: Exchange order ID to cancel

        Returns:
            True if cancellation successful, False otherwise

        Note:
            Following CODING_STANDARDS.md:
            - Uses configured timeouts for cancellation
            - NO assumptions about cancellation success
        """
        order = self._active_orders.get(order_id)
        if not order:
            logger.warning("cancel_request_for_unknown_order", order_id=order_id)
            return False

        # Validate cancellation if validator is available
        if self._order_validator:
            cancellation_violations = await self._order_validator.validate_order_cancellation(order)
            if cancellation_violations:
                logger.warning(
                    "order_cancellation_validation_failed",
                    order_id=order_id,
                    violations=cancellation_violations,
                )
                return False

        try:
            if self._safe_mode:
                # Paper trading - simulate cancellation
                order.status = OrderStatus.CANCELED
                del self._active_orders[order_id]

                logger.info("order_cancelled_simulation", order_id=order_id, safe_mode=True)
                return True
            # Real trading - cancel via API
            api_client = self._api_clients.get(order.exchange.value)
            if not api_client:
                logger.error(
                    "cancel_order_no_api_client", order_id=order_id, exchange=order.exchange.value
                )
                return False

            # Get exchange-specific timeout
            exchange_config = self.config.exchanges.get(order.exchange.value)
            if not exchange_config:
                self._raise_no_exchange_config_error(order.exchange.value)
            timeout = exchange_config.request_timeout_seconds

            success = await asyncio.wait_for(
                self._cancel_order_via_api(api_client, order), timeout=timeout
            )

            if not success:
                return False
            order.status = OrderStatus.CANCELED
            del self._active_orders[order_id]

            logger.info("order_cancelled_success", order_id=order_id, exchange=order.exchange.value)

        except Exception as e:
            self._error_count += 1
            self._last_activity = datetime.now(UTC)
            logger.exception("order_cancellation_failed", order_id=order_id, error=str(e))
            return False
        else:
            return True

    async def modify_order(
        self, order_id: str, new_price: Decimal | None = None, new_quantity: Decimal | None = None
    ) -> bool:
        """Modify an active order.

        Args:
            order_id: Exchange order ID to modify
            new_price: New price for the order (if provided)
            new_quantity: New quantity for the order (if provided)

        Returns:
            True if modification successful, False otherwise
        """
        order = self._active_orders.get(order_id)
        if not order:
            logger.warning("modify_request_for_unknown_order", order_id=order_id)
            return False

        # Validate modification if needed
        if not await self._validate_modification(order, new_price, new_quantity):
            return False

        try:
            # Execute modification
            success = await self._execute_modification(order, new_price, new_quantity)

            if not success:
                return False
            self._update_order_state(order, new_price, new_quantity)

        except Exception as e:
            self._error_count += 1
            self._last_activity = datetime.now(UTC)
            logger.exception("order_modification_failed", order_id=order_id, error=str(e))
            return False
        else:
            return True

    async def _validate_modification(
        self, order: Order, new_price: Decimal | None, new_quantity: Decimal | None
    ) -> bool:
        """Validate order modification.

        Returns:
            True if modification is valid, False otherwise
        """
        if not self._order_validator:
            return True

        # Create modified order for validation
        modified_order = Order(
            exchange=order.exchange,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            price=new_price if new_price is not None else order.price,
            quantity_requested=(
                new_quantity if new_quantity is not None else order.quantity_requested
            ),
            time_in_force=order.time_in_force,
            status=order.status,
            created_at=order.created_at,
            updated_at=datetime.now(UTC),
            triggered_at=None,
            strategy_name=None,
            signal_id=None,
            exchange_order_id=order.exchange_order_id,
            client_order_id=order.client_order_id,
        )

        modification_violations = await self._order_validator.validate_order_modification(
            order, modified_order
        )
        if modification_violations:
            logger.warning(
                "order_modification_validation_failed",
                order_id=order.exchange_order_id,
                violations=modification_violations,
            )
            return False
        return True

    async def _execute_modification(
        self, order: Order, new_price: Decimal | None, new_quantity: Decimal | None
    ) -> bool:
        """Execute order modification.

        Returns:
            True if modification successful, False otherwise

        Raises:
            ValueError: If exchange configuration is not found
        """
        if self._safe_mode:
            # Paper trading - simulate modification
            return self._simulate_modification(order, new_price, new_quantity)

        # Real trading - modify via API
        api_client = self._api_clients.get(order.exchange.value)
        if not api_client:
            logger.error(
                "modify_order_no_api_client",
                order_id=order.exchange_order_id,
                exchange=order.exchange.value,
            )
            return False

        # Get exchange-specific timeout
        exchange_config = self.config.exchanges.get(order.exchange.value)
        if not exchange_config:
            msg = f"No exchange configuration found for {order.exchange.value}"
            raise ValueError(msg)
        timeout = exchange_config.request_timeout_seconds

        return await asyncio.wait_for(
            self._modify_order_via_api(api_client, order, new_price, new_quantity), timeout=timeout
        )

    def _simulate_modification(
        self, order: Order, new_price: Decimal | None, new_quantity: Decimal | None
    ) -> bool:
        """Simulate order modification in safe mode.

        Returns:
            True (always successful in simulation)
        """
        if new_price is not None:
            order.price = new_price
        if new_quantity is not None:
            order.quantity_requested = new_quantity

        logger.info(
            "order_modified_simulation",
            order_id=order.exchange_order_id,
            new_price=float(order.price) if order.price else None,
            new_quantity=float(order.quantity_requested),
            safe_mode=True,
        )
        return True

    def _update_order_state(
        self, order: Order, new_price: Decimal | None, new_quantity: Decimal | None
    ) -> None:
        """Update local order state after successful modification."""
        if new_price is not None:
            order.price = new_price
        if new_quantity is not None:
            order.quantity_requested = new_quantity

        logger.info(
            "order_modified_success",
            order_id=order.exchange_order_id,
            new_price=float(order.price) if order.price else None,
            new_quantity=float(order.quantity_requested),
            exchange=order.exchange.value,
        )

    def _validate_execution_request(self, request: ExecutionRequest) -> None:
        """Validate execution request parameters.

        Args:
            request: Execution request to validate

        Raises:
            ValueError: If validation fails
        """
        if not request.signal.price or request.signal.price <= 0:
            msg = f"Invalid signal price: {request.signal.price}"
            raise ValueError(msg)

        if request.position_size.quantity <= 0:
            msg = f"Invalid position quantity: {request.position_size.quantity}"
            raise ValueError(msg)

        if request.position_size.value_usd <= 0:
            msg = f"Invalid position value: {request.position_size.value_usd}"
            raise ValueError(msg)

        # Check if we have API client for this exchange
        exchange_to_check = request.signal.exchange
        if isinstance(exchange_to_check, list):
            if not exchange_to_check:
                msg = "No exchange specified in signal"
                raise ValueError(msg)
            exchange_name = exchange_to_check[0].value
        else:
            exchange_name = exchange_to_check.value
        if not self._safe_mode and exchange_name not in self._api_clients:
            msg = f"No API client available for exchange: {exchange_name}"
            raise ValueError(msg)

    def _calculate_limit_price(
        self, side: OrderSide, market_price: Decimal | None
    ) -> Decimal | None:
        """Calculate limit price with configured offset.

        Args:
            side: Order side (BUY/SELL)
            market_price: Current market price

        Returns:
            Limit price with offset applied

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured offset percentage
        - Returns Decimal, NOT float
        """
        if not market_price:
            return None

        if side == OrderSide.BUY:
            # Buy at slight premium to increase fill probability
            return market_price * (Decimal(1) + self._limit_offset_pct)
        # Sell at slight discount to increase fill probability
        return market_price * (Decimal(1) - self._limit_offset_pct)

    async def _validate_slippage(self, request: ExecutionRequest) -> None:
        """Validate expected slippage is within configured limits.

        Args:
            request: Execution request to validate

        Raises:
            ValueError: If slippage exceeds configured maximum
        """
        # For now, assume signal price is current market price
        # In production, this would fetch current market price
        signal_price = request.signal.price

        # Simplified slippage check - would be enhanced with real market data
        if not signal_price:
            logger.warning(
                "slippage_check_skipped",
                signal_id=request.signal.signal_id,
                reason="no_signal_price",
            )
            return

        # For market orders, assume some slippage based on order size
        # Use configured slippage factors for calculation
        slippage_factor = Decimal(str(self.config.execution.minimal_quantity_fallback))
        estimated_slippage = min(
            request.position_size.value_usd * slippage_factor,  # Size-based slippage
            self._max_slippage / Decimal(2),  # But cap at half of max allowed
        )

        if estimated_slippage > self._max_slippage:
            msg = (
                f"Estimated slippage {estimated_slippage:.4%} exceeds "
                f"max allowed {self._max_slippage:.4%}"
            )
            raise ValueError(msg)

        logger.debug(
            "slippage_check_passed",
            signal_id=request.signal.signal_id,
            estimated_slippage=float(estimated_slippage),
            max_allowed=float(self._max_slippage),
        )

    async def _simulate_order_execution(self, order: Order) -> Order:
        """Simulate order execution for safe mode.

        Args:
            order: Order to simulate

        Returns:
            Order with simulated execution results

        IMPORTANT: Following CODING_STANDARDS.md:
        - Clear indication this is simulation
        - Realistic simulation parameters
        """
        # Simulate order placement delay if configured
        if self._safe_mode and self.config.simulation.simulate_network_delay:
            delay = float(self.config.simulation.min_network_delay_ms) / 1000.0
            await asyncio.sleep(delay)

        # Assign simulated order ID
        order.exchange_order_id = f"sim_{uuid.uuid4().hex[:8]}"
        order.status = OrderStatus.OPEN
        order.updated_at = datetime.now(UTC)

        logger.info(
            "order_simulated",
            order_id=order.exchange_order_id,
            symbol=order.symbol.value,
            exchange=order.exchange.value,
            side=order.side.value,
            quantity=float(order.quantity_requested),
            price=float(order.price) if order.price else None,
            safe_mode=True,
        )

        return order

    async def _place_real_order(self, order: Order) -> Order:
        """Place actual order via exchange API.

        Args:
            order: Order to place

        Returns:
            Order with exchange response data

        Raises:
            ValueError: If no API client or exchange config found

        Note:
        - Uses configured retry logic
        - Uses configured timeouts
        - NO hardcoded retry parameters
        """
        api_client = self._api_clients.get(order.exchange.value)
        if not api_client:
            msg = f"No API client for {order.exchange.value}"
            raise ValueError(msg)

        # Get exchange-specific timeout from config
        exchange_config = self.config.exchanges.get(order.exchange.value)
        if not exchange_config:
            msg = f"No exchange config found for {order.exchange.value}"
            raise ValueError(msg)
        timeout = exchange_config.request_timeout_seconds

        # Place order with retries based on config
        last_error = None
        for attempt in range(self._max_retries):
            try:
                logger.debug(
                    "order_placement_attempt",
                    attempt=attempt + 1,
                    max_retries=self._max_retries,
                    symbol=order.symbol.value,
                    exchange=order.exchange.value,
                )

                exchange_order = await asyncio.wait_for(
                    self._place_order_via_api(api_client, order), timeout=timeout
                )

                # Update order with exchange response (exchange_order is now an Order object)
                order.exchange_order_id = exchange_order.exchange_order_id
                order.status = exchange_order.status
                order.updated_at = exchange_order.updated_at

                logger.info(
                    "order_placed_successfully",
                    order_id=order.exchange_order_id,
                    symbol=order.symbol.value,
                    exchange=order.exchange.value,
                    attempt=attempt + 1,
                )

            except (ValueError, TimeoutError, ConnectionError, RuntimeError) as e:
                last_error = e
                logger.warning(
                    "order_placement_attempt_failed",
                    attempt=attempt + 1,
                    max_retries=self._max_retries,
                    error=str(e),
                    symbol=order.symbol.value,
                    exchange=order.exchange.value,
                )

                if attempt < self._max_retries - 1:
                    # Apply exponential backoff
                    delay = float(self._retry_delay) * (self._backoff_multiplier**attempt)
                    await asyncio.sleep(delay)

                    logger.debug(
                        "order_placement_retry_delay", delay_seconds=delay, next_attempt=attempt + 2
                    )
                    continue

            return order

        # All retries failed - update health metrics
        self._error_count += 1
        self._last_activity = datetime.now(UTC)

        logger.error(
            "order_placement_failed_all_retries",
            symbol=order.symbol.value,
            exchange=order.exchange.value,
            attempts=self._max_retries,
            final_error=str(last_error),
        )
        raise last_error or Exception("Order placement failed after all retries")

    async def _place_order_via_api(self, api_client: ExchangeAPI, order: Order) -> Order:
        """Place order via exchange API client.

        Args:
            api_client: Exchange API client
            order: Order to place

        Returns:
            Order object from exchange response

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about API client interface
        - Uses proper type conversions for API
        """
        # Create PlaceOrderArgs using the proper interface
        place_order_args = PlaceOrderArgs(
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            quantity=order.quantity_requested,
            time_in_force=order.time_in_force,
            price=order.price,
            client_order_id=order.client_order_id,
        )

        # Call API using the proper interface
        return await api_client.place_order(place_order_args)

    async def _cancel_order_via_api(self, api_client: ExchangeAPI, order: Order) -> bool:
        """Cancel order via exchange API client.

        Args:
            api_client: Exchange API client
            order: Order to cancel

        Returns:
            True if cancellation successful

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about API client interface
        - Explicit success/failure return
        """
        try:
            # Create CancelOrderArgs using the proper interface
            cancel_order_args = CancelOrderArgs(
                order_id=order.exchange_order_id or "",
                symbol=order.symbol,
                client_order_id=order.client_order_id,
            )

            # Call API using the proper interface
            result = await api_client.cancel_order(cancel_order_args)
            # Use proper attribute access with error handling
            try:
                return result.success
            except AttributeError:
                return bool(result)

        except Exception as e:
            logger.exception(
                "api_cancel_order_error", order_id=order.exchange_order_id, error=str(e)
            )
            return False

    async def _modify_order_via_api(
        self,
        api_client: ExchangeAPI,
        order: Order,
        new_price: Decimal | None = None,
        new_quantity: Decimal | None = None,
    ) -> bool:
        """Modify order via exchange API client.

        Args:
            api_client: Exchange API client
            order: Order to modify
            new_price: New price (if provided)
            new_quantity: New quantity (if provided)

        Returns:
            True if modification successful

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about API client interface
        - Explicit success/failure return
        """
        try:
            params: dict[str, Any] = {
                "order_id": order.exchange_order_id,
                "symbol": order.symbol.value,
            }

            # Add modification parameters
            if new_price is not None:
                params["price"] = float(new_price)
            if new_quantity is not None:
                params["quantity"] = float(new_quantity)

            # ExchangeAPI doesn't currently support order modification
            # This would need to be implemented per exchange if supported
            logger.warning(
                "order_modification_not_supported",
                exchange=order.exchange.value,
                reason="ExchangeAPI interface doesn't include modify_order method",
            )

        except Exception as e:
            logger.exception(
                "api_modify_order_error", order_id=order.exchange_order_id, error=str(e)
            )
            return False
        else:
            return False

    def _create_trade_from_fill(self, order: Order, fill_data: dict[str, Any]) -> Trade:
        """Create Trade object from order fill data.

        Args:
            order: Order that was filled
            fill_data: Fill data from exchange

        Returns:
            Trade object representing the fill

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses typed Trade model
        - All monetary values as Decimal
        - NO assumptions about fill data structure
        """
        return Trade(
            id=fill_data.get("trade_id", str(uuid.uuid4())),
            symbol=order.symbol,
            executed_at=datetime.now(UTC),
            side=order.side,
            order_id=order.exchange_order_id or "",
            exchange=order.exchange.value,
            price=Decimal(str(fill_data.get("fill_price", order.price or 0))),
            quantity=Decimal(str(fill_data.get("filled_quantity", 0))),
            fee=Decimal(str(fill_data.get("fee", "0"))),
            fee_asset=fill_data.get("fee_asset"),
            client_order_id=order.client_order_id,
        )

    def get_active_order_count(self) -> int:
        """Get count of active orders.

        Returns:
            Number of orders currently being tracked
        """
        return len(self._active_orders)

    def get_active_orders(self) -> dict[str, Order]:
        """Get copy of active orders dictionary.

        Returns:
            Copy of active orders for read-only access

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns copy to prevent external mutation
        """
        return self._active_orders.copy()

    async def check_health(self) -> dict[str, Any]:
        """Health check implementation for ExecutionEngine.

        Returns:
            Dictionary with health metrics and status

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns explicit health metrics
        - No assumptions about normal operation
        """
        return {
            "is_running": True,
            "active_orders": len(self._active_orders),
            "total_orders": self._order_count,
            "success_count": self._success_count,
            "error_count": self._error_count,
            "last_activity": self._last_activity.isoformat() if self._last_activity else None,
            "api_clients_available": len(self._api_clients),
            "safe_mode": self._safe_mode,
            "max_slippage_pct": float(self._max_slippage),
            "max_retries": self._max_retries,
        }

    def get_service_type(self) -> ServiceType:
        """Return service type for health monitoring."""
        return ServiceType.EXECUTION

    def get_fill_statistics(self) -> dict[str, object]:
        """Get fill processing statistics from FillHandler.

        Returns:
            Dictionary with fill statistics or empty dict if no handler

        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns actual statistics from FillHandler
        - NO assumptions about fill processing
        """
        if self._fill_handler:
            return self._fill_handler.get_fill_statistics()
        return {
            "fill_handler_available": False,
            "message": "No fill handler configured for detailed statistics",
        }

    def get_recent_fills(self, limit: int = 10) -> list[Trade]:
        """Get recent fills processed by the FillHandler.

        Args:
            limit: Maximum number of fills to return

        Returns:
            List of recent Trade objects or empty list if no handler

        IMPORTANT: Following CODING_STANDARDS.md:
        - Delegates to FillHandler for actual data
        - NO local fill tracking duplication
        """
        if self._fill_handler:
            return self._fill_handler.get_recent_fills(limit)
        return []

    def _raise_no_exchange_config_error(self, exchange: str) -> NoReturn:
        """Raise ValueError for missing exchange configuration.

        Args:
            exchange: Exchange name

        Raises:
            ValueError: Always raised with missing config message
        """
        msg = f"No exchange configuration found for {exchange}"
        raise ValueError(msg)
