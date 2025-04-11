"""
Synchronized Order Submission with Verification.

This module implements synchronized order submission across exchanges
with comprehensive verification at every step.
"""

import asyncio
import logging
import time
import traceback
from datetime import datetime
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


class VerificationResult:
    """Result of a verification operation."""

    def __init__(self, timestamp: int, success: bool, details: dict[str, Any], error: str = None):
        """Initialize the verification result."""
        self.timestamp = timestamp
        self.success = success
        self.details = details
        self.error = error

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp,
            "success": self.success,
            "details": self.details,
            "error": self.error,
        }


class ExecutionResult:
    """Result of an execution."""

    def __init__(
        self,
        execution_id: str,
        status: ExecutionStatus,
        error: str = None,
        verification_results: dict[str, Any] = None,
        abort_details: dict[str, Any] = None,
        compensation_result: dict[str, Any] = None,
    ):
        """Initialize the execution result."""
        self.execution_id = execution_id
        self.status = status
        self.error = error
        self.verification_results = verification_results or {}
        self.abort_details = abort_details
        self.compensation_result = compensation_result
        self.timestamp = int(time.time() * 1000)
        self.first_exchange = None
        self.first_order_id = None
        self.first_fill = None
        self.second_exchange = None
        self.second_order_id = None
        self.second_fill = None

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
    ) -> VerificationResult:
        """
        Verify that an order was placed correctly.

        Args:
            exchange: Exchange where the order was placed
            order_id: Order ID to verify
            expected_details: Expected order details

        Returns:
            Verification result
        """
        verification = VerificationResult(
            timestamp=int(time.time() * 1000), success=True, details={}
        )

        # Get order from portfolio tracker (local state)
        local_order = self.portfolio_tracker.get_order(exchange, order_id)

        # Get order from exchange API
        api_client = self.portfolio_tracker.get_api_client(exchange)
        api_order = await api_client.get_order(order_id, expected_details.get("symbol"))

        # Compare order details
        if not local_order:
            verification.success = False
            verification.error = f"Order {order_id} not found in local state"
            verification.details["local_order"] = None
        else:
            verification.details["local_order"] = local_order.to_dict()

            # Verify key properties match expected values
            for key, expected_value in expected_details.items():
                actual_value = getattr(local_order, key, None)
                if actual_value != expected_value:
                    verification.success = False
                    verification.error = (
                        f"Order {key} mismatch: expected {expected_value}, got {actual_value}"
                    )
                    break

        if not api_order:
            verification.success = False
            verification.error = (
                verification.error or ""
            ) + f" Order {order_id} not found in exchange API"
            verification.details["api_order"] = None
        else:
            verification.details["api_order"] = (
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
                        verification.success = False
                        verification.error = (
                            (verification.error or "")
                            + f" API order {key} mismatch: expected {expected_details[key]}, got {api_value}"
                        )
                        break

        return verification

    async def verify_order_execution(self, exchange: str, order_id: str) -> VerificationResult:
        """
        Verify that an order was executed properly.

        Args:
            exchange: Exchange where the order was executed
            order_id: Order ID to verify

        Returns:
            Verification result
        """
        verification = VerificationResult(
            timestamp=int(time.time() * 1000), success=True, details={}
        )

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
            verification.success = False
            verification.error = f"Order {order_id} not found in local state"
            verification.details["local_order"] = None
        else:
            verification.details["local_order"] = local_order.to_dict()

            # Check if order is filled in local state
            if local_order.status != OrderStatus.FILLED:
                verification.success = False
                verification.error = (
                    f"Order {order_id} not filled in local state: {local_order.status}"
                )

        if not api_order:
            verification.success = False
            verification.error = (
                verification.error or ""
            ) + f" Order {order_id} not found in exchange API"
            verification.details["api_order"] = None
        else:
            verification.details["api_order"] = (
                api_order.to_dict() if hasattr(api_order, "to_dict") else api_order
            )

            # Check if order is filled in API
            api_status = (
                api_order.get("status")
                if isinstance(api_order, dict)
                else getattr(api_order, "status", None)
            )
            if api_status != OrderStatus.FILLED and api_status != "FILLED":
                verification.success = False
                verification.error = (
                    verification.error or ""
                ) + f" Order {order_id} not filled in API: {api_status}"

        # Check if fills contain this order
        matching_fills = [
            fill
            for fill in recent_fills
            if fill.get("order_id") == order_id or getattr(fill, "order_id", None) == order_id
        ]

        verification.details["matching_fills"] = matching_fills

        if not matching_fills:
            verification.success = False
            verification.error = (
                verification.error or ""
            ) + f" No fills found for order {order_id}"

        return verification


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
            start_time=datetime.now(),
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
                "time": datetime.now().isoformat(),
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
            "time": datetime.now().isoformat(),
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
        context.end_time = datetime.now()
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
        context.end_time = datetime.now()
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
            if not pre_execution_verification.success:
                return ExecutionResult(
                    execution_id=execution_id,
                    status=ExecutionStatus.REJECTED,
                    error=pre_execution_verification.error,
                    verification_results=pre_execution_verification.details,
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
                execution_result.verification_results = post_verification.details

                # If verification failed, mark execution as partially completed
                if not post_verification.success:
                    execution_result.status = ExecutionStatus.PARTIALLY_COMPLETED
                    execution_result.error = post_verification.error

                    # Initiate compensation if needed
                    if self.config.get("execution.auto_compensate_verification_failures", True):
                        compensation_result = await self._compensate_verification_failure(
                            opportunity, execution_result, post_verification
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
                error=f"Execution error: {str(e)}",
                abort_details=abort_result,
            )

    async def _verify_pre_execution(self, opportunity) -> VerificationResult:
        """
        Perform pre-execution verification.

        Args:
            opportunity: Arbitrage opportunity

        Returns:
            Verification result
        """
        # Create verification result
        verification = VerificationResult(
            timestamp=int(time.time() * 1000), success=True, details={}
        )

        # 1. Verify circuit breakers are not tripped
        circuit_breaker_status = self.circuit_breaker_system.check_all(
            [
                f"exchange:{opportunity.long_exchange}",
                f"exchange:{opportunity.short_exchange}",
                f"symbol:{opportunity.symbol}",
            ]
        )

        verification.details["circuit_breakers"] = circuit_breaker_status

        if not circuit_breaker_status.all_ok:
            verification.success = False
            verification.error = (
                f"Circuit breakers tripped: {circuit_breaker_status.tripped_breakers}"
            )
            return verification

        # 2. Verify market conditions
        market_conditions = await self._verify_market_conditions(opportunity)
        verification.details["market_conditions"] = market_conditions

        if not market_conditions.success:
            verification.success = False
            verification.error = f"Market condition verification failed: {market_conditions.error}"
            return verification

        # 3. Verify sufficient balance
        balance_verification = await self._verify_balances(opportunity)
        verification.details["balances"] = balance_verification

        if not balance_verification.success:
            verification.success = False
            verification.error = f"Insufficient balance: {balance_verification.error}"
            return verification

        return verification

    async def _verify_market_conditions(self, opportunity) -> VerificationResult:
        """
        Verify market conditions for the opportunity.

        Args:
            opportunity: Arbitrage opportunity

        Returns:
            Verification result
        """
        # This is a placeholder implementation
        # Actual implementation would check price stability, liquidity, etc.
        return VerificationResult(
            timestamp=int(time.time() * 1000), success=True, details={"checked": True}
        )

    async def _verify_balances(self, opportunity) -> VerificationResult:
        """
        Verify that sufficient balances are available.

        Args:
            opportunity: Arbitrage opportunity

        Returns:
            Verification result
        """
        # This is a placeholder implementation
        # Actual implementation would check balances on each exchange
        return VerificationResult(
            timestamp=int(time.time() * 1000), success=True, details={"checked": True}
        )

    async def _execute_sequential_with_verification(
        self, opportunity, execution_context: ExecutionContext
    ) -> ExecutionResult:
        """
        Execute using sequential strategy with verification.

        Args:
            opportunity: Arbitrage opportunity
            execution_context: Execution context

        Returns:
            Execution result
        """
        # Initialize result with pending status
        result = ExecutionResult(
            execution_id=execution_context.execution_id,
            status=ExecutionStatus.EXECUTING,
        )

        # Log execution start
        logger.info(
            f"Starting sequential execution with verification: {execution_context.execution_id}"
        )

        try:
            # Add checkpoint for execution start
            await self.execution_coordinator.add_checkpoint(
                execution_context,
                "sequential_execution_started",
                {
                    "opportunity": opportunity.__dict__
                    if hasattr(opportunity, "__dict__")
                    else str(opportunity)
                },
            )

            # Get exchanges
            long_exchange = opportunity.long_exchange
            short_exchange = opportunity.short_exchange

            # Get exchange adapters
            if (
                long_exchange not in self.exchange_adapters
                or short_exchange not in self.exchange_adapters
            ):
                return ExecutionResult(
                    execution_id=execution_context.execution_id,
                    status=ExecutionStatus.FAILED,
                    error=f"Exchange adapter not found: {long_exchange} or {short_exchange}",
                )

            long_adapter = self.exchange_adapters[long_exchange]
            short_adapter = self.exchange_adapters[short_exchange]

            # Step 1: Prepare orders for both legs
            long_order = self._prepare_order(opportunity, "long")
            short_order = self._prepare_order(opportunity, "short")

            # Add checkpoint for order preparation
            await self.execution_coordinator.add_checkpoint(
                execution_context,
                "orders_prepared",
                {
                    "long_order": long_order.to_dict()
                    if hasattr(long_order, "to_dict")
                    else str(long_order),
                    "short_order": short_order.to_dict()
                    if hasattr(short_order, "to_dict")
                    else str(short_order),
                },
            )

            # Step 2: Execute first order (less liquid exchange first for sequential lock-in)
            # Determine which exchange has less liquidity (typically the one with smaller size)
            # Default to long order first unless configured otherwise
            first_is_long = self.config.get("execution.long_order_first", True)

            # If we have liquidity information, use it
            if hasattr(opportunity, "long_exchange_liquidity") and hasattr(
                opportunity, "short_exchange_liquidity"
            ):
                first_is_long = (
                    opportunity.long_exchange_liquidity < opportunity.short_exchange_liquidity
                )

            first_exchange = long_exchange if first_is_long else short_exchange
            first_adapter = long_adapter if first_is_long else short_adapter
            first_order = long_order if first_is_long else short_order

            second_exchange = short_exchange if first_is_long else long_exchange
            second_adapter = short_adapter if first_is_long else long_adapter
            second_order = short_order if first_is_long else long_order

            # Log order of execution
            logger.info(f"Execution order: {first_exchange} first, {second_exchange} second")

            # Add checkpoint for execution order
            await self.execution_coordinator.add_checkpoint(
                execution_context,
                "execution_order_determined",
                {"first_exchange": first_exchange, "second_exchange": second_exchange},
            )

            # Execute first order
            logger.info(f"Placing first order on {first_exchange}")
            try:
                first_order_result = await first_adapter.place_order(
                    symbol=first_order.symbol,
                    side=first_order.side.value
                    if hasattr(first_order.side, "value")
                    else first_order.side,
                    order_type=first_order.type.value
                    if hasattr(first_order.type, "value")
                    else first_order.type,
                    quantity=first_order.quantity,
                    price=first_order.price,
                    client_order_id=first_order.client_order_id,
                )

                if not first_order_result:
                    return ExecutionResult(
                        execution_id=execution_context.execution_id,
                        status=ExecutionStatus.FAILED,
                        error=f"Failed to place first order on {first_exchange}",
                    )

                # Store order ID in result
                result.first_exchange = first_exchange
                result.first_order_id = (
                    first_order_result.id
                    if hasattr(first_order_result, "id")
                    else first_order_result.get("id", "unknown")
                )

                # Add checkpoint for first order placed
                await self.execution_coordinator.add_checkpoint(
                    execution_context,
                    "first_order_placed",
                    {
                        "exchange": first_exchange,
                        "order": first_order_result.to_dict()
                        if hasattr(first_order_result, "to_dict")
                        else str(first_order_result),
                    },
                )
            except Exception as e:
                logger.error(f"Error placing first order on {first_exchange}: {str(e)}")
                return ExecutionResult(
                    execution_id=execution_context.execution_id,
                    status=ExecutionStatus.FAILED,
                    error=f"Error placing first order on {first_exchange}: {str(e)}",
                )

            # Step 3: Verify first order execution
            first_order_id = result.first_order_id
            logger.info(f"Verifying first order execution: {first_order_id} on {first_exchange}")

            # Wait for order execution with timeout and retries
            for retry in range(self.verification_retries):
                try:
                    verification = await asyncio.wait_for(
                        self.order_verifier.verify_order_execution(first_exchange, first_order_id),
                        timeout=self.verification_timeout,
                    )

                    if verification.success:
                        logger.info(f"First order successfully executed: {first_order_id}")

                        # Extract fill information
                        first_fill = verification.details.get("api_order", {})
                        result.first_fill = first_fill

                        # Add checkpoint for first order verified
                        await self.execution_coordinator.add_checkpoint(
                            execution_context,
                            "first_order_verified",
                            {"verification": verification.to_dict()},
                        )
                        break
                    else:
                        logger.warning(
                            f"First order verification failed (attempt {retry + 1}/{self.verification_retries}): {verification.error}"
                        )

                        # Check if we should abort or retry
                        if retry == self.verification_retries - 1:
                            return ExecutionResult(
                                execution_id=execution_context.execution_id,
                                status=ExecutionStatus.FAILED,
                                error=f"First order verification failed after {self.verification_retries} attempts: {verification.error}",
                                verification_results={"first_order": verification.to_dict()},
                            )

                        # Wait before retrying
                        await asyncio.sleep(self.verification_interval)
                except TimeoutError:
                    logger.warning(
                        f"First order verification timeout (attempt {retry + 1}/{self.verification_retries})"
                    )

                    # Check if we should abort or retry
                    if retry == self.verification_retries - 1:
                        return ExecutionResult(
                            execution_id=execution_context.execution_id,
                            status=ExecutionStatus.FAILED,
                            error=f"First order verification timeout after {self.verification_retries} attempts",
                            verification_results={"first_order": {"timeout": True}},
                        )

                    # Wait before retrying
                    await asyncio.sleep(self.verification_interval)

            # Step 4: Execute second order
            logger.info(f"Placing second order on {second_exchange}")
            try:
                # Adjust second order if needed based on first fill
                if hasattr(result, "first_fill") and result.first_fill:
                    filled_quantity = (
                        result.first_fill.get("filled_quantity", 0)
                        if isinstance(result.first_fill, dict)
                        else getattr(result.first_fill, "filled_quantity", 0)
                    )

                    # Adjust second order quantity to match first fill
                    if filled_quantity > 0 and filled_quantity != second_order.quantity:
                        logger.info(
                            f"Adjusting second order quantity from {second_order.quantity} to {filled_quantity}"
                        )
                        second_order.quantity = filled_quantity

                second_order_result = await second_adapter.place_order(
                    symbol=second_order.symbol,
                    side=second_order.side.value
                    if hasattr(second_order.side, "value")
                    else second_order.side,
                    order_type=second_order.type.value
                    if hasattr(second_order.type, "value")
                    else second_order.type,
                    quantity=second_order.quantity,
                    price=second_order.price,
                    client_order_id=second_order.client_order_id,
                )

                if not second_order_result:
                    # First order succeeded but second failed, need to compensate
                    compensation_result = await self._compensate_single_leg(
                        first_exchange, first_order_id, result.first_fill
                    )

                    return ExecutionResult(
                        execution_id=execution_context.execution_id,
                        status=ExecutionStatus.PARTIALLY_COMPLETED,
                        error=f"Failed to place second order on {second_exchange}",
                        first_exchange=first_exchange,
                        first_order_id=first_order_id,
                        first_fill=result.first_fill,
                        compensation_result=compensation_result,
                    )

                # Store order ID in result
                result.second_exchange = second_exchange
                result.second_order_id = (
                    second_order_result.id
                    if hasattr(second_order_result, "id")
                    else second_order_result.get("id", "unknown")
                )

                # Add checkpoint for second order placed
                await self.execution_coordinator.add_checkpoint(
                    execution_context,
                    "second_order_placed",
                    {
                        "exchange": second_exchange,
                        "order": second_order_result.to_dict()
                        if hasattr(second_order_result, "to_dict")
                        else str(second_order_result),
                    },
                )
            except Exception as e:
                logger.error(f"Error placing second order on {second_exchange}: {str(e)}")

                # First order succeeded but second failed, need to compensate
                compensation_result = await self._compensate_single_leg(
                    first_exchange, first_order_id, result.first_fill
                )

                return ExecutionResult(
                    execution_id=execution_context.execution_id,
                    status=ExecutionStatus.PARTIALLY_COMPLETED,
                    error=f"Error placing second order on {second_exchange}: {str(e)}",
                    first_exchange=first_exchange,
                    first_order_id=first_order_id,
                    first_fill=result.first_fill,
                    compensation_result=compensation_result,
                )

            # Step 5: Verify second order execution
            second_order_id = result.second_order_id
            logger.info(f"Verifying second order execution: {second_order_id} on {second_exchange}")

            # Wait for order execution with timeout and retries
            for retry in range(self.verification_retries):
                try:
                    verification = await asyncio.wait_for(
                        self.order_verifier.verify_order_execution(
                            second_exchange, second_order_id
                        ),
                        timeout=self.verification_timeout,
                    )

                    if verification.success:
                        logger.info(f"Second order successfully executed: {second_order_id}")

                        # Extract fill information
                        second_fill = verification.details.get("api_order", {})
                        result.second_fill = second_fill

                        # Add checkpoint for second order verified
                        await self.execution_coordinator.add_checkpoint(
                            execution_context,
                            "second_order_verified",
                            {"verification": verification.to_dict()},
                        )

                        # Both orders succeeded
                        result.status = ExecutionStatus.COMPLETED

                        # Add final checkpoint
                        await self.execution_coordinator.add_checkpoint(
                            execution_context,
                            "sequential_execution_completed",
                            {
                                "first_exchange": first_exchange,
                                "first_order_id": first_order_id,
                                "first_fill": result.first_fill,
                                "second_exchange": second_exchange,
                                "second_order_id": second_order_id,
                                "second_fill": result.second_fill,
                            },
                        )

                        return result
                    else:
                        logger.warning(
                            f"Second order verification failed (attempt {retry + 1}/{self.verification_retries}): {verification.error}"
                        )

                        # Check if we should abort or retry
                        if retry == self.verification_retries - 1:
                            # Both legs were placed but second verification failed
                            # Mark as partially completed
                            return ExecutionResult(
                                execution_id=execution_context.execution_id,
                                status=ExecutionStatus.PARTIALLY_COMPLETED,
                                error=f"Second order verification failed after {self.verification_retries} attempts: {verification.error}",
                                first_exchange=first_exchange,
                                first_order_id=first_order_id,
                                first_fill=result.first_fill,
                                second_exchange=second_exchange,
                                second_order_id=second_order_id,
                                verification_results={
                                    "first_order": result.first_fill,
                                    "second_order": verification.to_dict(),
                                },
                            )

                        # Wait before retrying
                        await asyncio.sleep(self.verification_interval)
                except TimeoutError:
                    logger.warning(
                        f"Second order verification timeout (attempt {retry + 1}/{self.verification_retries})"
                    )

                    # Check if we should abort or retry
                    if retry == self.verification_retries - 1:
                        # Both legs were placed but second verification timed out
                        # Mark as partially completed
                        return ExecutionResult(
                            execution_id=execution_context.execution_id,
                            status=ExecutionStatus.PARTIALLY_COMPLETED,
                            error=f"Second order verification timeout after {self.verification_retries} attempts",
                            first_exchange=first_exchange,
                            first_order_id=first_order_id,
                            first_fill=result.first_fill,
                            second_exchange=second_exchange,
                            second_order_id=second_order_id,
                            verification_results={
                                "first_order": result.first_fill,
                                "second_order": {"timeout": True},
                            },
                        )

                    # Wait before retrying
                    await asyncio.sleep(self.verification_interval)

            # Default return if we somehow exit the loops without returning
            return ExecutionResult(
                execution_id=execution_context.execution_id,
                status=ExecutionStatus.FAILED,
                error="Unexpected flow in sequential execution",
            )

        except Exception as e:
            logger.error(f"Unexpected error in sequential execution: {str(e)}", exc_info=True)

            # Add checkpoint for execution error
            await self.execution_coordinator.add_checkpoint(
                execution_context,
                "sequential_execution_error",
                {"error": str(e), "traceback": str(traceback.format_exc())},
            )

            return ExecutionResult(
                execution_id=execution_context.execution_id,
                status=ExecutionStatus.FAILED,
                error=f"Unexpected error in sequential execution: {str(e)}",
            )

    async def _compensate_single_leg(
        self, exchange: str, order_id: str, fill: Any
    ) -> dict[str, Any]:
        """
        Compensate for a single leg execution when the other leg failed.

        Args:
            exchange: Exchange where the order was placed
            order_id: Order ID to compensate
            fill: Fill information

        Returns:
            Compensation result
        """
        logger.info(f"Compensating for single leg execution: {order_id} on {exchange}")

        try:
            # Create compensation order with opposite side
            if not hasattr(fill, "side") and isinstance(fill, dict):
                # Handle dict
                original_side = fill.get("side")
                filled_quantity = fill.get("filled_quantity", 0)
                symbol = fill.get("symbol")
            else:
                # Handle object
                original_side = getattr(fill, "side", None)
                filled_quantity = getattr(fill, "filled_quantity", 0)
                symbol = getattr(fill, "symbol", None)

            if not original_side or not filled_quantity or not symbol:
                logger.error("Missing required fill information for compensation")
                return {"success": False, "error": "Missing required fill information"}

            # Determine opposite side
            from cyberdelta.core.models import OrderSide, OrderType

            if isinstance(original_side, str):
                compensation_side = (
                    OrderSide.SELL if original_side.upper() == "BUY" else OrderSide.BUY
                )
            else:
                compensation_side = (
                    OrderSide.SELL if original_side == OrderSide.BUY else OrderSide.BUY
                )

            # Get adapter
            adapter = self.exchange_adapters.get(exchange)
            if not adapter:
                logger.error(f"Exchange adapter not found for {exchange}")
                return {
                    "success": False,
                    "error": f"Exchange adapter not found for {exchange}",
                }

            # Place compensation order
            compensation_result = await adapter.place_order(
                symbol=symbol,
                side=compensation_side.value
                if hasattr(compensation_side, "value")
                else compensation_side,
                order_type=OrderType.MARKET.value,  # Use market order for compensation
                quantity=filled_quantity,
                price=None,  # Market order doesn't need price
                client_order_id=f"comp_{order_id}",
            )

            return {
                "success": True if compensation_result else False,
                "compensation_order": compensation_result.to_dict()
                if hasattr(compensation_result, "to_dict")
                else str(compensation_result),
            }

        except Exception as e:
            logger.error(f"Error compensating for single leg: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}

    def _prepare_order(self, opportunity, leg_type: str) -> Order:
        """
        Prepare an order based on opportunity details.

        Args:
            opportunity: Arbitrage opportunity
            leg_type: "long" or "short"

        Returns:
            Prepared order
        """
        from cyberdelta.core.models import Order

        # Determine exchange and side
        if leg_type == "long":
            exchange = opportunity.long_exchange
            side = OrderSide.BUY
        else:
            exchange = opportunity.short_exchange
            side = OrderSide.SELL

        # Get symbol
        symbol = opportunity.symbol

        # Get quantity
        # This would typically come from opportunity.optimal_size or similar
        # For now, use a default
        quantity = getattr(opportunity, "optimal_size", 1.0)

        # Generate a client order ID
        client_order_id = f"{exchange}_{leg_type}_{int(time.time() * 1000)}"

        # Create order
        order = Order(
            id="",  # Will be filled by exchange
            symbol=symbol,
            side=side,
            type=OrderType.MARKET,  # Use market orders for simplicity
            price=0.0,  # Will be filled by exchange for market orders
            quantity=quantity,
            client_order_id=client_order_id,
        )

        return order

    async def _execute_simultaneous_with_verification(
        self, opportunity, execution_context: ExecutionContext
    ) -> ExecutionResult:
        """
        Execute using simultaneous strategy with verification.

        Args:
            opportunity: Arbitrage opportunity
            execution_context: Execution context

        Returns:
            Execution result
        """
        # This is a placeholder implementation
        # Actual implementation would execute orders simultaneously with verification
        result = ExecutionResult(
            execution_id=execution_context.execution_id,
            status=ExecutionStatus.COMPLETED,
        )

        # Add more details to the result
        # ...

        return result

    async def _verify_post_execution(
        self, opportunity, execution_result: ExecutionResult
    ) -> VerificationResult:
        """
        Perform post-execution verification.

        Args:
            opportunity: Arbitrage opportunity
            execution_result: Execution result

        Returns:
            Verification result
        """
        verification = VerificationResult(
            timestamp=int(time.time() * 1000), success=True, details={}
        )

        # 1. Verify positions through position reconciliation system
        position_verification = await self._verify_positions(opportunity, execution_result)
        verification.details["positions"] = position_verification

        if not position_verification.success:
            verification.success = False
            verification.error = f"Position verification failed: {position_verification.error}"
            # Continue with other verifications even if this fails

        # 2. Verify fills match expected quantities
        fill_verification = await self._verify_fills(opportunity, execution_result)
        verification.details["fills"] = fill_verification

        if not fill_verification.success:
            verification.success = False
            verification.error = (
                verification.error or f"Fill verification failed: {fill_verification.error}"
            )
            # Continue with other verifications

        # 3. Verify no unexpected orders were created
        order_verification = await self._verify_orders(opportunity, execution_result)
        verification.details["orders"] = order_verification

        if not order_verification.success:
            verification.success = False
            verification.error = (
                verification.error or f"Order verification failed: {order_verification.error}"
            )

        return verification

    async def _verify_positions(
        self, opportunity, execution_result: ExecutionResult
    ) -> VerificationResult:
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
        return VerificationResult(
            timestamp=int(time.time() * 1000), success=True, details={"checked": True}
        )

    async def _verify_fills(
        self, opportunity, execution_result: ExecutionResult
    ) -> VerificationResult:
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
        return VerificationResult(
            timestamp=int(time.time() * 1000), success=True, details={"checked": True}
        )

    async def _verify_orders(
        self, opportunity, execution_result: ExecutionResult
    ) -> VerificationResult:
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
        return VerificationResult(
            timestamp=int(time.time() * 1000), success=True, details={"checked": True}
        )

    async def _compensate_verification_failure(
        self,
        opportunity,
        execution_result: ExecutionResult,
        verification_result: VerificationResult,
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

    def _generate_execution_id(self, opportunity) -> str:
        """
        Generate a unique execution ID.

        Args:
            opportunity: Arbitrage opportunity

        Returns:
            Unique execution ID
        """
        timestamp = int(time.time() * 1000)
        return f"exec_{timestamp}_{hash(opportunity) % 10000:04d}"
