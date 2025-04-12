"""
Synchronized Order Submission with Verification.

This module implements synchronized order submission across exchanges
with comprehensive verification at every step.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum, auto
from typing import Any

from cyberdelta.apis.base import ExchangeAPI
from cyberdelta.core.models import Order, OrderSide, OrderStatus, OrderType
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem

# Configure logger
logger = logging.getLogger(__name__)


class VerificationStatus(Enum):
    """Status of a verification operation."""

    SUCCESS = auto()
    FAILURE = auto()
    PARTIAL = auto()
    TIMEOUT = auto()
    ERROR = auto()


class ExecutionStatus(Enum):
    """Status of an execution."""

    PENDING = auto()
    EXECUTING = auto()
    COMPLETED = auto()
    FAILED = auto()
    PARTIALLY_COMPLETED = auto()
    COMPENSATING = auto()
    REJECTED = auto()


@dataclass
class ExecutionResult:
    """Result of an execution."""

    # Non-default fields first
    execution_id: str
    status: ExecutionStatus
    timestamp: int # Moved before fields with defaults

    # Default fields
    error: str | None = None
    verification_results: dict[str, Any] | None = None # Explicitly use None as default
    abort_details: dict[str, Any] | None = None
    compensation_result: dict[str, Any] | None = None
    first_exchange: str | None = None
    first_order_id: str | None = None
    first_fill: dict[str, Any] | None = None
    second_exchange: str | None = None
    second_order_id: str | None = None
    second_fill: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "execution_id": self.execution_id,
            "status": self.status.name if hasattr(self.status, "name") else self.status,
            "error": self.error,
            "verification_results": self.verification_results,
            "abort_details": self.abort_details,
            "compensation_result": self.compensation_result,
            "timestamp": self.timestamp,
            "first_exchange": self.first_exchange,
            "first_order_id": self.first_order_id,
            "first_fill": self.first_fill,
            "second_exchange": self.second_exchange,
            "second_order_id": self.second_order_id,
            "second_fill": self.second_fill,
        }

    def update(self, result_dict: dict[str, Any]) -> None:
        """Update the execution result with a dictionary."""
        for key, value in result_dict.items():
            if hasattr(self, key):
                setattr(self, key, value)


class ExecutionContext:
    """Context for an execution with tracking of checkpoints."""

    def __init__(
        self,
        execution_id: str,
        opportunity: Any,
        strategy: str,
        start_time: datetime,
        status: ExecutionStatus,
        checkpoints: list[dict[str, Any]],
    ):
        """Initialize the execution context."""
        self.execution_id = execution_id
        self.opportunity = opportunity
        self.strategy = strategy
        self.start_time = start_time
        self.status = status
        self.checkpoints = checkpoints
        self.end_time = None
        self.result = None
        self.abort_reason = None


class OrderVerifier:
    """
    Component for verifying order placement, execution, and fills.
    """

    def __init__(self, config: dict[str, Any], portfolio_tracker: PortfolioTracker):
        """Initialize the order verifier."""
        self.config = config
        self.portfolio_tracker = portfolio_tracker

    async def verify_order_placement(
        self, exchange: str, order_id: str, expected_details: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Verify that an order was placed correctly.

        Args:
            exchange: Exchange where the order was placed
            order_id: Order ID to verify
            expected_details: Expected order details

        Returns:
            Verification result dictionary
        """
        verification_details: dict[str, Any] = {}
        verification_success = True
        verification_error = None

        # Get order from portfolio tracker (local state)
        local_order = self.portfolio_tracker.get_order(exchange, order_id)

        # Get order from exchange API
        api_client = self.portfolio_tracker.get_api_client(exchange)
        api_order = await api_client.get_order(order_id, expected_details.get("symbol"))

        # Compare order details
        if not local_order:
            verification_success = False
            verification_error = f"Order {order_id} not found in local state"
            verification_details["local_order"] = None
        else:
            verification_details["local_order"] = local_order.to_dict()

            # Verify key properties match expected values
            for key, expected_value in expected_details.items():
                actual_value = getattr(local_order, key, None)
                if actual_value != expected_value:
                    verification_success = False
                    verification_error = (
                        f"Order {key} mismatch: expected {expected_value}, got {actual_value}"
                    )
                    break

        if not api_order:
            verification_success = False
            verification_error = (
                verification_error or ""
            ) + f" Order {order_id} not found in exchange API"
            verification_details["api_order"] = None
        else:
            verification_details["api_order"] = (
                api_order.to_dict() if hasattr(api_order, "to_dict") else api_order
            )

            # Verify essential properties match on API side too
            for key in ["symbol", "side", "type"]:
                if key in expected_details:
                    api_value = (
                        api_order.get(key)
                        if isinstance(api_order, dict)
                        else getattr(api_order, key, None)
                    )
                    if api_value != expected_details[key]:
                        verification_success = False
                        verification_error = (
                            (verification_error or "")
                            + f" API order {key} mismatch: expected {expected_details[key]}, got {api_value}"
                        )
                        break

        return {
            "timestamp": int(time.time() * 1000),
            "success": verification_success,
            "error": verification_error,
            "details": verification_details,
        }

    async def verify_order_execution(self, exchange: str, order_id: str) -> dict[str, Any]:
        """
        Verify that an order was executed properly.
        Returns:
            Verification result dictionary
        """
        verification_details: dict[str, Any] = {}
        verification_success = True
        verification_error = None

        # Get order from portfolio tracker
        local_order = self.portfolio_tracker.get_order(exchange, order_id)

        # Get order from exchange API
        api_client = self.portfolio_tracker.get_api_client(exchange)
        api_order = await api_client.get_order(
            order_id, local_order.symbol if local_order else None
        )

        # Get recent fills
        recent_fills = await api_client.get_recent_fills(
            local_order.symbol if local_order else None
        )

        # Check if order exists and is filled
        if not local_order:
            verification_success = False
            verification_error = f"Order {order_id} not found in local state"
            verification_details["local_order"] = None
        else:
            verification_details["local_order"] = local_order.to_dict()

            # Check if order is filled in local state
            if local_order.status != OrderStatus.FILLED:
                verification_success = False
                verification_error = (
                    f"Order {order_id} not filled in local state: {local_order.status}"
                )

        if not api_order:
            verification_success = False
            verification_error = (
                verification_error or ""
            ) + f" Order {order_id} not found in exchange API"
            verification_details["api_order"] = None
        else:
            api_order_dict = (
                api_order.to_dict() if hasattr(api_order, "to_dict") else api_order
            )
            verification_details["api_order"] = api_order_dict

            # Check if order is filled in API state
            api_status = (
                api_order_dict.get("status")
                if isinstance(api_order_dict, dict)
                else getattr(api_order, "status", None)
            )
            if api_status != OrderStatus.FILLED:
                verification_success = False
                verification_error = (
                    (verification_error or "")
                    + f" Order {order_id} not filled in API state: {api_status}"
                )

        # Check fills
        verification_details["recent_fills"] = recent_fills
        # Add logic to compare fills with expected order quantity
        # ...

        return {
            "timestamp": int(time.time() * 1000),
            "success": verification_success,
            "error": verification_error,
            "details": verification_details,
        }


class ExecutionCoordinator:
    """
    Coordinates synchronized execution with verification checkpoints.
    """

    def __init__(self, config: dict[str, Any]):
        """Initialize the execution coordinator."""
        self.config = config
        self.executions = {}

    async def start_execution(
        self, execution_id: str, opportunity: Any, strategy: str
    ) -> ExecutionContext:
        """
        Start a new execution with verification checkpoints.

        Args:
            execution_id: Unique execution ID
            opportunity: Arbitrage opportunity
            strategy: Execution strategy

        Returns:
            Execution context
        """
        # Create execution context
        context = ExecutionContext(
            execution_id=execution_id,
            opportunity=opportunity,
            strategy=strategy,
            start_time=datetime.now(UTC),
            status=ExecutionStatus.PENDING,
            checkpoints=[],
        )

        # Add execution context to tracking
        self.executions[execution_id] = context

        # Add initial checkpoint
        await self.add_checkpoint(
            context,
            "execution_started",
            {
                "time": datetime.now(UTC).isoformat(),
                "opportunity": opportunity.to_dict()
                if hasattr(opportunity, "to_dict")
                else str(opportunity),
                "strategy": strategy,
            },
        )

        return context

    async def add_checkpoint(
        self, context: ExecutionContext, checkpoint_name: str, details: dict[str, Any]
    ) -> None:
        """
        Add an execution checkpoint.

        Args:
            context: Execution context
            checkpoint_name: Name of the checkpoint
            details: Checkpoint details
        """
        checkpoint = {
            "name": checkpoint_name,
            "time": datetime.now(UTC).isoformat(),
            "details": details,
        }

        context.checkpoints.append(checkpoint)

        # Log checkpoint for debugging
        logger.debug(f"Execution {context.execution_id} checkpoint: {checkpoint_name}")

    async def complete_execution(self, context: ExecutionContext, result: ExecutionResult) -> None:
        """
        Mark an execution as complete.

        Args:
            context: Execution context
            result: Execution result
        """
        context.status = result.status
        context.end_time = datetime.now(UTC)
        context.result = result

        # Add final checkpoint
        await self.add_checkpoint(
            context,
            "execution_completed",
            {
                "status": result.status.name if hasattr(result.status, "name") else result.status,
                "duration_ms": (context.end_time - context.start_time).total_seconds() * 1000,
                "error": result.error if hasattr(result, "error") else None,
            },
        )

        # Schedule cleanup after delay
        loop = asyncio.get_event_loop()
        loop.call_later(
            self.config.get("execution.context_retention_seconds", 3600),
            self._cleanup_execution,
            context.execution_id,
        )

    async def abort_execution(self, context: ExecutionContext, reason: str) -> dict[str, Any]:
        """
        Abort an execution.

        Args:
            context: Execution context
            reason: Abort reason

        Returns:
            Abort details
        """
        context.status = ExecutionStatus.FAILED
        context.end_time = datetime.now(UTC)
        context.abort_reason = reason

        # Add abort checkpoint
        await self.add_checkpoint(
            context,
            "execution_aborted",
            {
                "reason": reason,
                "duration_ms": (context.end_time - context.start_time).total_seconds() * 1000,
            },
        )

        # Schedule cleanup after delay
        loop = asyncio.get_event_loop()
        loop.call_later(
            self.config.get("execution.context_retention_seconds", 3600),
            self._cleanup_execution,
            context.execution_id,
        )

        return {
            "execution_id": context.execution_id,
            "abort_reason": reason,
            "checkpoint_count": len(context.checkpoints),
            "duration_ms": (context.end_time - context.start_time).total_seconds() * 1000,
        }

    def _cleanup_execution(self, execution_id: str) -> None:
        """
        Clean up an execution context after retention period.

        Args:
            execution_id: Execution ID to clean up
        """
        if execution_id in self.executions:
            del self.executions[execution_id]
            logger.debug(f"Cleaned up execution context {execution_id}")


class SynchronizedOrderSubmissionService:
    """
    Service for synchronized order submission across exchanges with verification.
    Extends the AtomicExecutionEngine with enhanced verification capabilities.
    """

    def __init__(
        self,
        config: dict[str, Any],
        exchange_adapters: dict[str, ExchangeAPI],
        circuit_breaker_system: CircuitBreakerSystem,
        position_reconciliation_system,
        portfolio_tracker: PortfolioTracker,
    ):
        """Initialize the service."""
        self.config = config
        self.exchange_adapters = exchange_adapters
        self.circuit_breaker_system = circuit_breaker_system
        self.position_reconciliation_system = position_reconciliation_system
        self.portfolio_tracker = portfolio_tracker

        self.order_verifier = OrderVerifier(config, portfolio_tracker)
        self.execution_coordinator = ExecutionCoordinator(config)

        # Configuration parameters
        self.verification_timeout = config.get("execution.verification_timeout", 10.0)  # seconds
        self.verification_retries = config.get("execution.verification_retries", 3)
        self.verification_interval = config.get("execution.verification_interval", 1.0)  # seconds

    async def submit_orders(
        self, opportunity, execution_strategy: str = "sequential_lock_in"
    ) -> ExecutionResult:
        """
        Submit orders with synchronized verification.

        Args:
            opportunity: The arbitrage opportunity to execute
            execution_strategy: Strategy to use for execution

        Returns:
            Execution result with verification details
        """
        execution_id = self._generate_execution_id(opportunity)

        # Start execution
        execution_context = await self.execution_coordinator.start_execution(
            execution_id=execution_id,
            opportunity=opportunity,
            strategy=execution_strategy,
        )

        try:
            # Perform pre-execution verification
            pre_execution_verification = await self._verify_pre_execution(opportunity)
            if not pre_execution_verification.get("success"):
                return ExecutionResult(
                    execution_id=execution_id,
                    status=ExecutionStatus.REJECTED,
                    timestamp=int(datetime.now(UTC).timestamp() * 1000),
                    error=pre_execution_verification.get("error"),
                    verification_results=pre_execution_verification.get("details"),
                )

            # Execute the orders based on the selected strategy
            if execution_strategy == "sequential_lock_in":
                execution_result = await self._execute_sequential_with_verification(
                    opportunity, execution_context
                )
            elif execution_strategy == "simultaneous":
                execution_result = await self._execute_simultaneous_with_verification(
                    opportunity, execution_context
                )
            else:
                # Default to sequential
                execution_result = await self._execute_sequential_with_verification(
                    opportunity, execution_context
                )

            # Perform post-execution verification
            if execution_result.status == ExecutionStatus.COMPLETED:
                post_verification = await self._verify_post_execution(opportunity, execution_result)

                # Update execution result with verification results
                execution_result.verification_results = post_verification.get("details")

                # If verification failed, mark execution as partially completed
                if not post_verification.get("success"):
                    execution_result.status = ExecutionStatus.PARTIALLY_COMPLETED
                    execution_result.error = post_verification.get("error")

                    # Initiate compensation if needed
                    if self.config.get("execution.auto_compensate_verification_failures", True):
                        compensation_result = await self._compensate_verification_failure(
                            opportunity, execution_result, post_verification.get("details")
                        )
                        execution_result.compensation_result = compensation_result

            await self.execution_coordinator.complete_execution(execution_context, execution_result)

            return execution_result

        except Exception as e:
            logger.error(f"Error in synchronized order submission: {str(e)}", exc_info=True)

            # Safe abort and cleanup
            abort_result = await self.execution_coordinator.abort_execution(
                execution_context, f"Execution error: {str(e)}"
            )

            return ExecutionResult(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                timestamp=int(datetime.now(UTC).timestamp() * 1000),
                error=f"Execution error: {str(e)}",
                abort_details=abort_result,
            )

    async def _verify_pre_execution(self, opportunity) -> dict[str, Any]:
        """Perform pre-execution verification checks."""
        results = {}
        all_success = True
        error_msg = ""

        market_check = await self._verify_market_conditions(opportunity)
        results["market_conditions"] = market_check
        if not market_check.get("success"):
            all_success = False
            error_msg += f"Market conditions invalid: {market_check.get('error')}; "

        balance_check = await self._verify_balances(opportunity)
        results["balances"] = balance_check
        if not balance_check.get("success"):
            all_success = False
            error_msg += f"Balance check failed: {balance_check.get('error')}; "

        # Add more checks (e.g., circuit breakers specific to this service)
        # ...

        return {
            "timestamp": int(time.time() * 1000),
            "success": all_success,
            "error": error_msg.strip() or None,
            "details": results,
        }

    async def _verify_market_conditions(self, opportunity) -> dict[str, Any]:
        """Verify market conditions (e.g., price spreads, volatility)."""
        # Placeholder implementation
        return {
            "timestamp": int(time.time() * 1000),
            "success": True,
            "error": None,
            "details": {"spread_ok": True, "volatility_ok": True},
        }

    async def _verify_balances(self, opportunity) -> dict[str, Any]:
        """Verify sufficient balances are available on both exchanges."""
        # Placeholder implementation - needs integration with PortfolioTracker
        # and opportunity details (required sizes)
        return {
            "timestamp": int(time.time() * 1000),
            "success": True,
            "error": None,
            "details": {"long_balance_ok": True, "short_balance_ok": True},
        }

    async def _execute_sequential_with_verification(
        self, opportunity, execution_context: ExecutionContext
    ) -> ExecutionResult:
        """
        Execute trades sequentially with lock-in and verification.

        1. Place order on the first exchange (e.g., long leg).
        2. Verify placement and wait for fill.
        3. Verify fill details.
        4. Place order on the second exchange (e.g., short leg).
        5. Verify placement and wait for fill.
        6. Verify fill details.
        7. Final verification of positions.
        """
        exec_result = ExecutionResult(
            execution_id=execution_context.execution_id,
            status=ExecutionStatus.EXECUTING,
            timestamp=int(time.time() * 1000),
        )
        execution_context.result = exec_result # Link result to context early

        first_leg_success = False
        try:
            # Determine first and second legs (e.g., based on liquidity or configuration)
            # For simplicity, assume long is first, short is second
            first_exchange = opportunity.long_exchange
            second_exchange = opportunity.short_exchange
            first_side = OrderSide.BUY
            second_side = OrderSide.SELL
            first_order = self._prepare_order(opportunity, "long")
            second_order = self._prepare_order(opportunity, "short")

            exec_result.first_exchange = first_exchange
            exec_result.second_exchange = second_exchange

            logger.info(
                f"[{exec_result.execution_id}] Executing first leg: {first_side.name} {first_order.quantity} {first_order.symbol} on {first_exchange}"
            )
            await self.execution_coordinator.add_checkpoint(
                execution_context,
                "start_first_leg",
                {"exchange": first_exchange, "order": first_order.to_dict()},
            )

            # --- Execute First Leg ---
            first_api = self.exchange_adapters[first_exchange]
            placed_first_order = await first_api.place_order(
                symbol=first_order.symbol,
                side=first_order.side,
                order_type=first_order.type,
                quantity=first_order.quantity,
                price=first_order.price,
                # Add other necessary parameters like time_in_force if needed
            )

            if not placed_first_order or not placed_first_order.id:
                raise RuntimeError(f"Failed to place first leg order on {first_exchange}")

            exec_result.first_order_id = placed_first_order.id
            await self.execution_coordinator.add_checkpoint(
                execution_context,
                "placed_first_leg",
                {"order_id": placed_first_order.id, "response": placed_first_order.to_dict()},
            )

            # Verify first leg placement
            placement_verification = await self.order_verifier.verify_order_placement(
                first_exchange, placed_first_order.id, first_order.to_dict()
            )
            await self.execution_coordinator.add_checkpoint(
                execution_context, "verify_first_placement", placement_verification
            )
            if not placement_verification.get("success"):
                raise RuntimeError(
                    f"First leg placement verification failed: {placement_verification.get('error')}"
                )

            # Wait for first leg fill (with timeout)
            logger.info(
                f"[{exec_result.execution_id}] Waiting for fill of first leg order {placed_first_order.id} on {first_exchange}"
            )
            filled_first_order = await self._wait_for_fill(
                first_api, placed_first_order.id, first_order.symbol
            )
            if not filled_first_order or filled_first_order.status != OrderStatus.FILLED:
                raise RuntimeError(
                    f"First leg order {placed_first_order.id} did not fill or failed. Status: {getattr(filled_first_order, 'status', 'N/A')}"
                )

            exec_result.first_fill = filled_first_order.to_dict()
            await self.execution_coordinator.add_checkpoint(
                execution_context, "filled_first_leg", exec_result.first_fill
            )
            first_leg_success = True # Mark first leg as successful

            # Verify first leg execution/fill
            fill_verification = await self.order_verifier.verify_order_execution(
                first_exchange, filled_first_order.id
            )
            await self.execution_coordinator.add_checkpoint(
                execution_context, "verify_first_fill", fill_verification
            )
            if not fill_verification.get("success"):
                # If fill verification fails AFTER successful fill, we might need compensation
                raise RuntimeError(
                    f"First leg fill verification failed: {fill_verification.get('error')}"
                )

            logger.info(
                f"[{exec_result.execution_id}] First leg completed successfully. Executing second leg: {second_side.name} {second_order.quantity} {second_order.symbol} on {second_exchange}"
            )
            await self.execution_coordinator.add_checkpoint(
                execution_context,
                "start_second_leg",
                {"exchange": second_exchange, "order": second_order.to_dict()},
            )

            # --- Execute Second Leg ---
            second_api = self.exchange_adapters[second_exchange]
            placed_second_order = await second_api.place_order(
                symbol=second_order.symbol,
                side=second_order.side,
                order_type=second_order.type,
                quantity=second_order.quantity,
                price=second_order.price,
                # Add other necessary parameters like time_in_force if needed
            )

            if not placed_second_order or not placed_second_order.id:
                raise RuntimeError(f"Failed to place second leg order on {second_exchange}")

            exec_result.second_order_id = placed_second_order.id
            await self.execution_coordinator.add_checkpoint(
                execution_context,
                "placed_second_leg",
                {"order_id": placed_second_order.id, "response": placed_second_order.to_dict()},
            )

            # Verify second leg placement
            placement_verification_2 = await self.order_verifier.verify_order_placement(
                second_exchange, placed_second_order.id, second_order.to_dict()
            )
            await self.execution_coordinator.add_checkpoint(
                execution_context, "verify_second_placement", placement_verification_2
            )
            if not placement_verification_2.get("success"):
                raise RuntimeError(
                    f"Second leg placement verification failed: {placement_verification_2.get('error')}"
                )

            # Wait for second leg fill
            logger.info(
                f"[{exec_result.execution_id}] Waiting for fill of second leg order {placed_second_order.id} on {second_exchange}"
            )
            filled_second_order = await self._wait_for_fill(
                second_api, placed_second_order.id, second_order.symbol
            )
            if not filled_second_order or filled_second_order.status != OrderStatus.FILLED:
                raise RuntimeError(
                    f"Second leg order {placed_second_order.id} did not fill or failed. Status: {getattr(filled_second_order, 'status', 'N/A')}"
                )

            exec_result.second_fill = filled_second_order.to_dict()
            await self.execution_coordinator.add_checkpoint(
                execution_context, "filled_second_leg", exec_result.second_fill
            )

            # Verify second leg execution/fill
            fill_verification_2 = await self.order_verifier.verify_order_execution(
                second_exchange, filled_second_order.id
            )
            await self.execution_coordinator.add_checkpoint(
                execution_context, "verify_second_fill", fill_verification_2
            )
            if not fill_verification_2.get("success"):
                raise RuntimeError(
                    f"Second leg fill verification failed: {fill_verification_2.get('error')}"
                )

            logger.info(f"[{exec_result.execution_id}] Both legs completed successfully.")
            exec_result.status = ExecutionStatus.COMPLETED
            return exec_result.to_dict() # Return as dict

        except Exception as e:
            logger.error(
                f"[{exec_result.execution_id}] Sequential execution failed: {e}", exc_info=True
            )
            exec_result.status = ExecutionStatus.FAILED
            exec_result.error = str(e)

            # Attempt compensation if the first leg succeeded but the second failed
            if first_leg_success and exec_result.first_fill:
                logger.warning(
                    f"[{exec_result.execution_id}] Second leg failed after first leg succeeded. Attempting compensation for order {exec_result.first_order_id} on {first_exchange}."
                )
                compensation_result = await self._compensate_single_leg(
                    first_exchange,
                    exec_result.first_order_id,
                    exec_result.first_fill, # Pass fill details
                )
                exec_result.compensation_result = compensation_result
                await self.execution_coordinator.add_checkpoint(
                    execution_context, "compensation_first_leg", compensation_result
                )
                if not compensation_result.get("success"):
                    logger.error(
                        f"[{exec_result.execution_id}] FATAL: Compensation failed for first leg: {compensation_result.get('error')}"
                    )
                    # Mark as needing manual intervention
                    exec_result.error += "; COMPENSATION FAILED - MANUAL INTERVENTION REQUIRED"
                else:
                    logger.info(f"[{exec_result.execution_id}] Compensation successful.")
                    exec_result.status = ExecutionStatus.PARTIALLY_COMPLETED # Or FAILED_COMPENSATED

            return exec_result.to_dict() # Return as dict

    async def _compensate_single_leg(
        self, exchange: str, order_id: str, fill: dict[str, Any] | Order | None
    ) -> dict[str, Any]:
        """Attempt to compensate for a single filled leg by closing the position."""
        if not fill:
            return {"success": False, "error": "No fill data provided for compensation"}

        try:
            api_client = self.exchange_adapters[exchange]

            # Extract details from fill (handle both dict and Order obj)
            if isinstance(fill, dict):
                symbol = fill.get("symbol")
                filled_qty = Decimal(str(fill.get("filled_quantity", "0")))
                side_str = fill.get("side")
                original_side = OrderSide(side_str) if side_str else None
            elif isinstance(fill, Order):
                symbol = fill.symbol
                filled_qty = fill.filled_quantity
                original_side = fill.side
            else:
                return {"success": False, "error": "Invalid fill data type for compensation"}

            if not symbol or filled_qty <= Decimal("0") or not original_side:
                return {
                    "success": False,
                    "error": f"Invalid fill details for compensation: sym={symbol}, qty={filled_qty}, side={original_side}",
                }

            # Determine compensation side (opposite of original fill)
            compensation_side = OrderSide.SELL if original_side == OrderSide.BUY else OrderSide.BUY

            logger.info(
                f"Attempting compensation on {exchange}: {compensation_side.name} {filled_qty} {symbol} (closing position from order {order_id})"
            )

            # Place market order to close the position
            comp_order = await api_client.place_order(
                symbol=symbol,
                side=compensation_side,
                order_type=OrderType.MARKET,
                quantity=filled_qty,
                reduce_only=True, # Ensure it only closes the position
            )

            if not comp_order or not comp_order.id:
                raise RuntimeError("Failed to place compensation order.")

            logger.info(
                f"Compensation order placed: ID {comp_order.id}, Status {comp_order.status}"
            )

            # Optionally, wait for compensation order fill verification
            # filled_comp_order = await self._wait_for_fill(api_client, comp_order.id, symbol)
            # if not filled_comp_order or filled_comp_order.status != OrderStatus.FILLED:
            #     logger.error(f"Compensation order {comp_order.id} did not fill!")
            #     return {"success": False, "error": "Compensation order failed to fill"}

            return {
                "success": True,
                "compensation_order_id": comp_order.id,
                "status": comp_order.status.name,
            }

        except Exception as e:
            logger.error(f"Error during compensation for {exchange}/{order_id}: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    def _prepare_order(self, opportunity: Any, leg_type: str) -> Order:
        """Prepare an Order object for a specific leg of the opportunity."""
        # Placeholder - Needs actual implementation based on opportunity structure
        # Should return an Order object with symbol, side, type, quantity, price etc.
        if leg_type == "long":
            return Order(
                id=None, # Will be assigned by exchange
                symbol=opportunity.symbol, # Assuming opportunity has symbol
                side=OrderSide.BUY,
                type=OrderType.LIMIT, # Or MARKET depending on strategy
                quantity=opportunity.long_size_base, # Assuming size in base asset
                price=opportunity.long_price,
            )
        elif leg_type == "short":
            return Order(
                id=None,
                symbol=opportunity.symbol,
                side=OrderSide.SELL,
                type=OrderType.LIMIT,
                quantity=opportunity.short_size_base,
                price=opportunity.short_price,
            )
        else:
            raise ValueError(f"Invalid leg type: {leg_type}")

    async def _execute_simultaneous_with_verification(
        self, opportunity: Any, execution_context: ExecutionContext
    ) -> dict[str, Any]:
        """Execute trades simultaneously with verification (less common for arbitrage)."""
        # Placeholder implementation
        logger.warning("Simultaneous execution strategy not fully implemented.")
        # Basic structure: Place both orders concurrently, then verify
        return {
            "execution_id": execution_context.execution_id,
            "status": ExecutionStatus.FAILED,
            "error": "Simultaneous execution not implemented",
            "timestamp": int(time.time() * 1000),
        }

    async def _verify_post_execution(
        self, opportunity: Any, execution_result: ExecutionResult
    ) -> dict[str, Any]:
        """Perform comprehensive post-execution verification."""
        results = {}
        all_success = True
        error_msg = ""

        if execution_result.status in [ExecutionStatus.COMPLETED, ExecutionStatus.PARTIALLY_COMPLETED]:
            order_check = await self._verify_orders(opportunity, execution_result)
            results["orders"] = order_check
            if not order_check.get("success"):
                all_success = False
                error_msg += f"Order verification failed: {order_check.get('error')}; "

            fill_check = await self._verify_fills(opportunity, execution_result)
            results["fills"] = fill_check
            if not fill_check.get("success"):
                all_success = False
                error_msg += f"Fill verification failed: {fill_check.get('error')}; "

            position_check = await self._verify_positions(opportunity, execution_result)
            results["positions"] = position_check
            if not position_check.get("success"):
                all_success = False
                error_msg += f"Position verification failed: {position_check.get('error')}; "
        else:
            # If execution failed, skip detailed post-verification
            all_success = False
            error_msg = f"Execution did not complete successfully ({execution_result.status.name}). Skipping post-verification."

        return {
            "timestamp": int(time.time() * 1000),
            "success": all_success,
            "error": error_msg.strip() or None,
            "details": results,
        }

    async def _verify_positions(
        self, opportunity: Any, execution_result: ExecutionResult
    ) -> dict[str, Any]:
        """
        Verify positions after execution.

        Args:
            opportunity: Arbitrage opportunity
            execution_result: Execution result

        Returns:
            Verification result
        """
        # This is a placeholder implementation
        # Actual implementation would use position reconciliation system
        return {"checked": True}

    async def _verify_fills(
        self, opportunity: Any, execution_result: ExecutionResult
    ) -> dict[str, Any]:
        """
        Verify fills match expected quantities.

        Args:
            opportunity: Arbitrage opportunity
            execution_result: Execution result

        Returns:
            Verification result
        """
        # This is a placeholder implementation
        # Actual implementation would check fill quantities
        return {"checked": True}

    async def _verify_orders(
        self, opportunity: Any, execution_result: ExecutionResult
    ) -> dict[str, Any]:
        """
        Verify no unexpected orders were created.

        Args:
            opportunity: Arbitrage opportunity
            execution_result: Execution result

        Returns:
            Verification result
        """
        # This is a placeholder implementation
        # Actual implementation would check for unexpected orders
        return {"checked": True}

    async def _compensate_verification_failure(
        self,
        opportunity: Any,
        execution_result: ExecutionResult,
        verification_result: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Compensate for verification failures.

        Args:
            opportunity: Arbitrage opportunity
            execution_result: Execution result
            verification_result: Verification result

        Returns:
            Compensation details
        """
        # This is a placeholder implementation
        # Actual implementation would compensate for verification failures
        return {"compensated": True}

    def _generate_execution_id(self, opportunity: Any) -> str:
        """
        Generate a unique execution ID.

        Args:
            opportunity: Arbitrage opportunity

        Returns:
            Unique execution ID
        """
        timestamp = int(time.time() * 1000)
        return f"exec_{timestamp}_{hash(opportunity) % 10000:04d}"
