"""Compensation service for ExecutionHandler refactoring.

This module provides position compensation when execution legs fail,
with proper monitoring, alerting, and error handling.
"""

from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import TraceLevelLogger, get_logger
from cyberdelta.core.models import OrderSide, OrderType
from cyberdelta.core.services.interfaces import (
    BaseAsyncService,
    CompensationConfig,
    ExecutionError,
    ExecutionErrorType,
    ExecutionResult,
    IAlertService,
    ICompensationService,
    IErrorHandler,
    IOrderService,
    OrderRequest,
)


if TYPE_CHECKING:
    from cyberdelta.core.models.execution import TradeExecution


class CompensationStatus(Enum):
    """Status of compensation operations."""

    PENDING = auto()
    PLACING_ORDER = auto()
    MONITORING = auto()
    PARTIAL_FILL = auto()
    COMPLETED = auto()
    FAILED = auto()
    TIMEOUT = auto()


class CompensationService(BaseAsyncService, ICompensationService):
    """Service for handling position compensation when executions fail."""

    def __init__(
        self,
        order_service: IOrderService,
        error_handler: IErrorHandler,
        alert_service: IAlertService | None = None,
        config: CompensationConfig | None = None,
        logger: TraceLevelLogger | None = None,
    ) -> None:
        """Initialize compensation service.

        Args:
            order_service: Order management service
            error_handler: Error handling service
            alert_service: Optional alert service for notifications
            config: Compensation configuration
            logger: Optional logger instance
        """
        super().__init__(logger)
        self.order_service = order_service
        self.error_handler = error_handler
        self.alert_service = alert_service
        self.config = config or CompensationConfig()
        self.logger = logger or get_logger(__name__)

        # Track active compensation operations
        self._active_compensations: dict[str, dict[str, Any]] = {}
        self._compensation_lock = asyncio.Lock()

    async def compensate_position(
        self, execution: TradeExecution, failed_leg: str, quantity_to_compensate: Decimal
    ) -> ExecutionResult:
        """Attempt to compensate a failed position.

        Args:
            execution: The execution that requires compensation
            failed_leg: Which leg failed ("long" or "short")
            quantity_to_compensate: Amount to compensate

        Returns:
            ExecutionResult with compensation status
        """
        compensation_id = f"{execution.id}_{failed_leg}_{int(time.time())}"

        self.logger.info(
            "Starting position compensation",
            execution_id=execution.id,
            compensation_id=compensation_id,
            failed_leg=failed_leg,
            quantity=str(quantity_to_compensate),
        )

        # Determine compensation parameters
        compensation_params = self._calculate_compensation_params(
            execution, failed_leg, quantity_to_compensate
        )

        if not compensation_params:
            return ExecutionResult.error_result(
                ExecutionError(
                    error_type=ExecutionErrorType.COMPENSATION_ERROR,
                    message="Could not calculate compensation parameters",
                    details={"execution_id": execution.id, "failed_leg": failed_leg},
                    recoverable=False,
                    retry_suggested=False,
                )
            )

        # Track compensation operation
        async with self._compensation_lock:
            self._active_compensations[compensation_id] = {
                "execution_id": execution.id,
                "failed_leg": failed_leg,
                "quantity": quantity_to_compensate,
                "status": CompensationStatus.PENDING,
                "created_at": time.time(),
                "order_id": None,
                "params": compensation_params,
            }

        try:
            # Place compensation order
            order_result = await self._place_compensation_order(
                compensation_id, compensation_params
            )

            if not order_result.success:
                await self._update_compensation_status(compensation_id, CompensationStatus.FAILED)
                return order_result

            order = order_result.data

            # Update tracking with order ID
            async with self._compensation_lock:
                if compensation_id in self._active_compensations:
                    self._active_compensations[compensation_id]["order_id"] = order.client_order_id
                    self._active_compensations[compensation_id]["status"] = (
                        CompensationStatus.MONITORING
                    )

            # Monitor compensation order
            return await self.monitor_compensation_order(
                compensation_id,
                order.client_order_id,
                compensation_params["exchange_id"],
                quantity_to_compensate,
            )

        except Exception as e:  # noqa: BLE001
            await self._update_compensation_status(compensation_id, CompensationStatus.FAILED)

            # Send critical alert
            if self.alert_service:
                await self.alert_service.send_critical_alert(
                    "Compensation Failed",
                    f"Failed to compensate {failed_leg} position for execution {execution.id}",
                    {
                        "execution_id": execution.id,
                        "compensation_id": compensation_id,
                        "failed_leg": failed_leg,
                        "error": str(e),
                    },
                )

            return await self.error_handler.handle_system_error(
                e, f"compensation for execution {execution.id}", recoverable=False
            )

    async def monitor_compensation_order(
        self,
        compensation_id: str,
        order_id: str,
        exchange_id: str,
        target_quantity: Decimal,
        timeout_seconds: float = 300.0,
    ) -> ExecutionResult:
        """Monitor compensation order until completion.

        Args:
            compensation_id: Compensation operation identifier
            order_id: Order identifier to monitor
            exchange_id: Exchange identifier
            target_quantity: Target quantity to fill
            timeout_seconds: Maximum time to wait

        Returns:
            ExecutionResult with monitoring outcome
        """
        start_time = time.time()
        poll_interval = 2.0

        self.logger.info(
            "Monitoring compensation order",
            compensation_id=compensation_id,
            order_id=order_id,
            exchange_id=exchange_id,
            target_quantity=str(target_quantity),
            timeout_seconds=timeout_seconds,
        )

        while time.time() - start_time < timeout_seconds:
            # Check order status
            status_result = await self.order_service.get_order_status(order_id, exchange_id)

            if not status_result.success:
                # Log error but continue monitoring
                self.logger.warning(
                    "Failed to get compensation order status",
                    compensation_id=compensation_id,
                    order_id=order_id,
                    error=status_result.error,
                )
                await asyncio.sleep(poll_interval)
                continue

            order = status_result.data
            filled_quantity = getattr(order, "filled_quantity", Decimal(0))

            # Check if order is sufficiently filled
            fill_percentage = (filled_quantity / target_quantity) * 100

            if fill_percentage >= (self.config.partial_fill_threshold_pct * 100):
                # Compensation successful
                await self._update_compensation_status(
                    compensation_id, CompensationStatus.COMPLETED
                )

                self.logger.info(
                    "Compensation order completed",
                    compensation_id=compensation_id,
                    order_id=order_id,
                    filled_quantity=str(filled_quantity),
                    target_quantity=str(target_quantity),
                    fill_percentage=fill_percentage,
                )

                return ExecutionResult.success_result({
                    "compensation_id": compensation_id,
                    "order_id": order_id,
                    "filled_quantity": filled_quantity,
                    "fill_percentage": fill_percentage,
                    "status": "completed",
                })

            # Check for partial fills
            if filled_quantity > 0:
                await self._update_compensation_status(
                    compensation_id, CompensationStatus.PARTIAL_FILL
                )

            # Check if order failed
            if hasattr(order, "status") and order.status in {"CANCELLED", "REJECTED", "EXPIRED"}:
                await self._update_compensation_status(compensation_id, CompensationStatus.FAILED)

                # Send alert for failed compensation
                if self.alert_service:
                    await self.alert_service.send_critical_alert(
                        "Compensation Order Failed",
                        f"Compensation order {order_id} failed with status {order.status}",
                        {
                            "compensation_id": compensation_id,
                            "order_id": order_id,
                            "order_status": order.status,
                            "filled_quantity": str(filled_quantity),
                            "target_quantity": str(target_quantity),
                        },
                    )

                return ExecutionResult.error_result(
                    ExecutionError(
                        error_type=ExecutionErrorType.COMPENSATION_ERROR,
                        message=f"Order failed with status: {order.status}",
                        details={
                            "compensation_id": compensation_id,
                            "order_id": order_id,
                            "order_status": order.status,
                            "filled_quantity": str(filled_quantity),
                        },
                        recoverable=False,
                        retry_suggested=False,
                    )
                )

            # Wait before next check
            await asyncio.sleep(poll_interval)

        # Timeout reached
        await self._update_compensation_status(compensation_id, CompensationStatus.TIMEOUT)

        # Send timeout alert
        if self.alert_service:
            await self.alert_service.send_critical_alert(
                "Compensation Timeout",
                f"Compensation order {order_id} timed out after {timeout_seconds}s",
                {
                    "compensation_id": compensation_id,
                    "order_id": order_id,
                    "timeout_seconds": timeout_seconds,
                },
            )

        return await self.error_handler.handle_timeout_error(
            "compensation monitoring",
            timeout_seconds,
            {
                "compensation_id": compensation_id,
                "order_id": order_id,
                "exchange_id": exchange_id,
            },
        )

    def _calculate_compensation_params(
        self, execution: TradeExecution, failed_leg: str, quantity: Decimal
    ) -> dict[str, Any] | None:
        """Calculate parameters for compensation order.

        Args:
            execution: The execution requiring compensation
            failed_leg: Which leg failed ("long" or "short")
            quantity: Quantity to compensate

        Returns:
            Dictionary with compensation parameters or None if impossible
        """
        try:
            # Determine the opposite side and exchange
            if failed_leg == "long":
                # If long leg failed, we need to sell on short exchange
                exchange_id = execution.opportunity.opportunity.short_exchange
                side = OrderSide.SELL
                symbol = execution.opportunity.opportunity.symbol
            elif failed_leg == "short":
                # If short leg failed, we need to buy on long exchange
                exchange_id = execution.opportunity.opportunity.long_exchange
                side = OrderSide.BUY
                symbol = execution.opportunity.opportunity.symbol
            else:
                self.logger.error(
                    "Invalid failed leg specified", failed_leg=failed_leg, execution_id=execution.id
                )
                return None

            # Determine order type and price
            if self.config.use_limit_orders:
                order_type = OrderType.LIMIT

                # Calculate limit price with offset
                if failed_leg == "long":
                    # Selling - use slightly lower price
                    base_price = getattr(execution.opportunity.opportunity, "short_price", None)
                    if base_price:
                        price = base_price * (1 - self.config.limit_price_offset_pct)
                    else:
                        # Fallback to market order if no price available
                        order_type = OrderType.MARKET
                        price = None
                else:
                    # Buying - use slightly higher price
                    base_price = getattr(execution.opportunity.opportunity, "long_price", None)
                    if base_price:
                        price = base_price * (1 + self.config.limit_price_offset_pct)
                    else:
                        # Fallback to market order if no price available
                        order_type = OrderType.MARKET
                        price = None
            else:
                order_type = OrderType.MARKET
                price = None

        except Exception as e:
            self.logger.exception(
                "Failed to calculate compensation parameters",
                execution_id=execution.id,
                failed_leg=failed_leg,
                error=str(e),
            )
            return None

        return {
            "exchange_id": exchange_id,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "order_type": order_type,
            "price": price,
            "reduce_only": True,  # Compensation should only reduce position
        }

    async def _place_compensation_order(
        self, compensation_id: str, params: dict[str, Any]
    ) -> ExecutionResult:
        """Place compensation order with the calculated parameters.

        Args:
            compensation_id: Compensation operation identifier
            params: Compensation order parameters

        Returns:
            ExecutionResult with order placement outcome
        """
        await self._update_compensation_status(compensation_id, CompensationStatus.PLACING_ORDER)

        # Create order request
        order_request = OrderRequest(
            exchange_id=params["exchange_id"],
            symbol=params["symbol"],
            side=params["side"],
            quantity=params["quantity"],
            order_type=params["order_type"],
            price=params.get("price"),
            reduce_only=params.get("reduce_only", True),
        )

        self.logger.info(
            "Placing compensation order",
            compensation_id=compensation_id,
            exchange_id=params["exchange_id"],
            symbol=params["symbol"],
            side=params["side"],
            quantity=str(params["quantity"]),
            order_type=params["order_type"],
        )

        # Place order through order service
        result = await self.order_service.place_order_with_retry(order_request)

        if result.success:
            self.logger.info(
                "Compensation order placed successfully",
                compensation_id=compensation_id,
                order_id=result.data.client_order_id if result.data else "unknown",
            )
        else:
            self.logger.error(
                "Failed to place compensation order",
                compensation_id=compensation_id,
                error=result.error,
            )

        return result

    async def _update_compensation_status(
        self, compensation_id: str, status: CompensationStatus
    ) -> None:
        """Update compensation operation status.

        Args:
            compensation_id: Compensation operation identifier
            status: New status
        """
        async with self._compensation_lock:
            if compensation_id in self._active_compensations:
                old_status = self._active_compensations[compensation_id]["status"]
                self._active_compensations[compensation_id]["status"] = status
                self._active_compensations[compensation_id]["updated_at"] = time.time()

                self.logger.debug(
                    "Compensation status updated",
                    compensation_id=compensation_id,
                    old_status=old_status,
                    new_status=status,
                )

    async def get_compensation_status(self, compensation_id: str) -> dict[str, Any] | None:
        """Get status of a compensation operation.

        Args:
            compensation_id: Compensation operation identifier

        Returns:
            Compensation status dictionary or None if not found
        """
        async with self._compensation_lock:
            return self._active_compensations.get(compensation_id)

    async def get_active_compensations(self) -> list[dict[str, Any]]:
        """Get all active compensation operations.

        Returns:
            List of active compensation operations
        """
        async with self._compensation_lock:
            return list(self._active_compensations.values())

    async def cleanup_completed_compensations(self) -> int:
        """Clean up completed compensation operations.

        Returns:
            Number of compensations cleaned up
        """
        current_time = time.time()
        cleanup_age = 3600  # 1 hour
        removed_count = 0

        async with self._compensation_lock:
            to_remove: list[str] = []

            for comp_id, comp_data in self._active_compensations.items():
                # Remove completed or failed compensations older than 1 hour
                status_completed = comp_data["status"] in {
                    CompensationStatus.COMPLETED,
                    CompensationStatus.FAILED,
                }
                last_update = comp_data.get("updated_at", comp_data["created_at"])
                time_expired = current_time - last_update > cleanup_age
                if status_completed and time_expired:
                    to_remove.append(comp_id)

            for comp_id in to_remove:
                del self._active_compensations[comp_id]
                removed_count += 1

        if removed_count > 0:
            self.logger.info(
                "Cleaned up completed compensations",
                removed_count=removed_count,
                remaining_count=len(self._active_compensations),
            )

        return removed_count
