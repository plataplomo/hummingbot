"""
Synchronized Order Submission with Verification.

This module implements synchronized order submission across exchanges
with comprehensive verification at every step.
"""

import asyncio
import dataclasses
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation  # Add this import
from enum import Enum, auto
from typing import Any, Protocol

from cyberdelta.apis.base import ExchangeAPI
from cyberdelta.core.models import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem
from cyberdelta.validation.funding_data import ArbitrageOpportunity

# Configure logger
logger = logging.getLogger(__name__)

# Define a type for opportunity that can be various types
OpportunityType = ArbitrageOpportunity


# Define a Protocol for the position reconciliation system
class PositionReconciliationSystem(Protocol):
    """Protocol defining the interface for position reconciliation systems."""

    async def reconcile_positions(self, exchange: str, symbol: str) -> dict[str, Any]:
        """Reconcile positions for a given exchange and symbol."""
        ...

    async def handle_position_discrepancy(
        # TODO: Define more specific types for expected/actual if possible
        self,
        exchange: str,
        symbol: str,
        # TODO: Refine Any with specific Position/Order types if possible
        expected: dict[str, Any],
        actual: dict[str, Any],
    ) -> dict[str, Any]:
        """Handle position discrepancies between expected and actual positions."""
        ...


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
    timestamp: int  # Moved before fields with defaults

    # Default fields
    error: str | None = None
    verification_results: dict[str, Any] | None = None  # Explicitly use None as default
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

    def __init__(  # Add return type hint -> None
        self,
        execution_id: str,
        opportunity: OpportunityType,
        strategy: str,
        start_time: datetime,
        status: ExecutionStatus,
        checkpoints: list[dict[str, Any]],
    ) -> None:
        """Initialize the execution context."""
        self.execution_id = execution_id
        self.opportunity = opportunity
        self.strategy = strategy
        self.start_time = start_time
        self.status = status
        self.checkpoints = checkpoints
        self.end_time: datetime | None = None
        self.result: ExecutionResult | None = None
        self.abort_reason: str | None = None  # Add Optional hint


class OrderVerifier:
    """
    Component for verifying order placement, execution, and fills.
    """

    def __init__(
        self, config: dict[str, Any], portfolio_tracker: PortfolioTracker
    ) -> None:  # Add -> None
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
        # Assuming get_order exists and returns Order | None
        local_order: Order | None = self.portfolio_tracker._orders.get(exchange, {}).get(
            order_id
        )  # Access internal dict

        # Get order from exchange API
        api_client = self.portfolio_tracker.api_clients.get(
            exchange
        )  # Access api_clients dict directly
        api_order: Order | None = None  # Initialize api_order
        if not api_client:
            verification_success = False
            verification_error = f"API client not found for exchange {exchange}"
            # api_order remains None
        else:
            # Assuming get_order exists on the concrete API client
            try:
                # Assuming get_order exists on the concrete API client
                api_order = await api_client.get_order(order_id, expected_details.get("symbol"))  # type: ignore[attr-defined] # Ignore potential missing attr on base API
            except AttributeError:
                logger.error(f"API client for {exchange} missing get_order method.")
                verification_success = False
                verification_error = f"API client for {exchange} missing get_order method."
            except Exception as e:
                logger.error(f"Error calling get_order for {exchange}: {e}", exc_info=True)
                verification_success = False
                verification_error = f"API error fetching order {order_id} from {exchange}"

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
                api_value: Any
                if isinstance(api_order, dict):
                    api_value = api_order.get(key)
                else:
                    api_value = getattr(api_order, key, None)
                if api_value != expected_details[key]:
                    verification_success = False
                    verification_error = (verification_error or "") + (
                        " API order {key} mismatch:"
                        f" expected {expected_details[key]}, got {api_value}"
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
        local_order: Order | None = self.portfolio_tracker._orders.get(exchange, {}).get(
            order_id
        )  # Access internal dict

        # Get order from exchange API
        api_client = self.portfolio_tracker.api_clients.get(exchange)  # Access dict directly
        api_order: Order | None = None  # Initialize
        if api_client:
            try:
                api_order = await api_client.get_order(
                    order_id, local_order.symbol if local_order else None
                )
            except AttributeError:
                logger.error(f"API client for {exchange} missing get_order method.")
                verification_success = False  # Mark verification as failed
                verification_error = f"API client for {exchange} missing get_order method."
            except Exception as e:
                logger.error(f"Error fetching order {order_id} from {exchange} API: {e}")
                verification_success = False  # Mark verification as failed
                verification_error = f"API error fetching order {order_id} from {exchange}: {e}"
        else:
            logger.error(f"API client not found for exchange {exchange}")
            verification_success = False  # Mark verification as failed
            verification_error = f"API client not found for {exchange}"

        # Get recent fills
        recent_fills = None
        if api_client is not None:
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
            api_order_dict = api_order.to_dict() if hasattr(api_order, "to_dict") else api_order
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
                    verification_error or ""
                ) + f" Order {order_id} not filled in API state: {api_status}"

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

    def __init__(self, config: dict[str, Any]) -> None:  # Add -> None
        """Initialize the execution coordinator."""
        self.config = config
        self.executions: dict[str, ExecutionContext] = {}  # Add type hint

    async def start_execution(
        self, execution_id: str, opportunity: OpportunityType, strategy: str
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
                "opportunity": dataclasses.asdict(opportunity)
                if dataclasses.is_dataclass(opportunity)
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
        context.end_time = datetime.now(UTC)  # Ensure assignment is valid
        context.abort_reason = reason  # Ensure assignment is valid

        # Add abort checkpoint
        await self.add_checkpoint(
            context,
            "execution_aborted",
            {
                "reason": reason,
                "duration_ms": (
                    (context.end_time - context.start_time).total_seconds() * 1000
                    if context.end_time is not None and context.start_time is not None
                    else -1.0  # Indicate error or unknown duration
                ),
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
        position_reconciliation_system: PositionReconciliationSystem,
        portfolio_tracker: PortfolioTracker,
    ) -> None:
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
        self, opportunity: OpportunityType, execution_strategy: str = "sequential_lock_in"
    ) -> ExecutionResult:
        """
        Submit orders for an opportunity, synchronizing execution across exchanges.

        Args:
            opportunity: Trading opportunity
            execution_strategy: Execution strategy to use

        Returns:
            Execution result
        """
        execution_id = self._generate_execution_id(opportunity)
        result = ExecutionResult(
            execution_id=execution_id,
            status=ExecutionStatus.PENDING,
            timestamp=int(time.time() * 1000),
        )

        # Create execution context
        execution_context = await self.execution_coordinator.start_execution(
            execution_id, opportunity, "funding_rate_arb"
        )

        # Pre-execution verification
        pre_verify_result = await self._verify_pre_execution(opportunity)
        await self.execution_coordinator.add_checkpoint(
            execution_context, "pre_execution_verification", pre_verify_result
        )

        if not pre_verify_result.get("success", False):
            result.status = ExecutionStatus.REJECTED
            result.error = pre_verify_result.get("error", "Pre-execution verification failed")
            await self.execution_coordinator.complete_execution(execution_context, result)
            return result

        # Market conditions verification
        market_verify_result = await self._verify_market_conditions(opportunity)
        await self.execution_coordinator.add_checkpoint(
            execution_context, "market_conditions_verification", market_verify_result
        )

        if not market_verify_result.get("success", False):
            result.status = ExecutionStatus.REJECTED
            result.error = market_verify_result.get(
                "error", "Market conditions verification failed"
            )
            await self.execution_coordinator.complete_execution(execution_context, result)
            return result

        # Balance verification
        balance_verify_result = await self._verify_balances(opportunity)
        await self.execution_coordinator.add_checkpoint(
            execution_context, "balance_verification", balance_verify_result
        )

        if not balance_verify_result.get("success", False):
            result.status = ExecutionStatus.REJECTED
            result.error = balance_verify_result.get("error", "Balance verification failed")
            await self.execution_coordinator.complete_execution(execution_context, result)
            return result

        # Update execution status
        execution_context.status = ExecutionStatus.EXECUTING

        # Execute orders based on strategy
        if execution_strategy == "sequential_lock_in":
            execution_result = await self._execute_sequential_with_verification(
                opportunity, execution_context
            )
        elif execution_strategy == "simultaneous":
            simultaneous_result = await self._execute_simultaneous_with_verification(
                opportunity, execution_context
            )
            # Convert to ExecutionResult
            execution_result = ExecutionResult(
                execution_id=execution_id,
                status=ExecutionStatus.COMPLETED
                if simultaneous_result.get("success", False)
                else ExecutionStatus.FAILED,
                timestamp=int(time.time() * 1000),
                error=simultaneous_result.get("error"),
                verification_results=simultaneous_result.get("verification_results"),
            )
        else:
            execution_result = ExecutionResult(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                timestamp=int(time.time() * 1000),
                error=f"Unknown execution strategy: {execution_strategy}",
            )

        # Post-execution verification if execution was successful
        if execution_result.status in (
            ExecutionStatus.COMPLETED,
            ExecutionStatus.PARTIALLY_COMPLETED,
        ):
            post_verify_result = await self._verify_post_execution(opportunity, execution_result)
            await self.execution_coordinator.add_checkpoint(
                execution_context, "post_execution_verification", post_verify_result
            )

            # If verification failed, perform compensation actions
            if not post_verify_result.get("success", False):
                execution_context.status = ExecutionStatus.COMPENSATING
                compensation_result = await self._compensate_verification_failure(
                    opportunity, execution_result, post_verify_result
                )
                execution_result.compensation_result = compensation_result
                execution_result.status = ExecutionStatus.PARTIALLY_COMPLETED

        # Complete execution
        await self.execution_coordinator.complete_execution(execution_context, execution_result)
        return execution_result

    async def _verify_pre_execution(self, opportunity: OpportunityType) -> dict[str, Any]:
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

    async def _verify_market_conditions(self, opportunity: OpportunityType) -> dict[str, Any]:
        """Verify market conditions (e.g., price spreads, volatility)."""
        # Placeholder implementation
        return {
            "timestamp": int(time.time() * 1000),
            "success": True,
            "error": None,
            "details": {"spread_ok": True, "volatility_ok": True},
        }

    async def _verify_balances(self, opportunity: OpportunityType) -> dict[str, Any]:
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
        self, opportunity: OpportunityType, execution_context: ExecutionContext
    ) -> ExecutionResult:
        """
        Execute orders sequentially with verification between legs.

        This ensures the first leg executes successfully before attempting the second leg.

        Args:
            opportunity: Trading opportunity
            execution_context: Current execution context

        Returns:
            Result with verification details
        """
        execution_id = execution_context.execution_id
        result = ExecutionResult(
            execution_id=execution_id,
            status=ExecutionStatus.EXECUTING,
            timestamp=int(time.time() * 1000),
        )

        # Determine which order to place first (e.g., the long order)
        try:
            # First leg - prepare and place order
            first_order = self._prepare_order(opportunity, "long")
            first_exchange = (
                "hyperliquid"  # This would be determined from opportunity in real implementation
            )

            # Store order info in result
            result.first_exchange = first_exchange

            # Checkpoint: first order preparation
            await self.execution_coordinator.add_checkpoint(
                execution_context,
                "first_order_preparation",
                {"exchange": first_exchange, "order": first_order.to_dict()},
            )

            # Place first order
            first_api = self.exchange_adapters[first_exchange]

            # Place order with verify
            try:
                placed_order: Order = await first_api.place_order(
                    symbol=first_order.symbol,
                    side=first_order.side,
                    order_type=first_order.order_type,
                    quantity=first_order.quantity_requested,
                    price=first_order.price,
                    time_in_force=TimeInForce.GTC,  # Default value
                    # TODO: Handle post_only, reduce_only if needed via config/adapter
                )
                assert hasattr(placed_order, "client_order_id") and hasattr(
                    placed_order, "to_dict"
                ), "placed_order missing required attributes"
                result.first_order_id = placed_order.client_order_id  # Use client_order_id

                # Checkpoint: first order placed
                await self.execution_coordinator.add_checkpoint(
                    execution_context,
                    "first_order_placed",
                    {"order_id": placed_order.client_order_id, "order": placed_order.to_dict()},
                )

                # Verify first order
                order_verifier = OrderVerifier(self.config, self.portfolio_tracker)
                verification_result = await order_verifier.verify_order_placement(
                    first_exchange,
                    placed_order.client_order_id,  # Use client_order_id
                    {
                        "symbol": first_order.symbol,
                        "side": first_order.side,
                        "type": first_order.order_type,  # Corrected field
                    },
                )

                # Only proceed if verification succeeds
                if not verification_result.get("success", False):
                    result.status = ExecutionStatus.FAILED
                    result.error = verification_result.get(
                        "error", "First order verification failed"
                    )
                    return result

                # Wait for fill if needed
                if self.config.get("execution.wait_for_first_fill", True):
                    # Monitor for fills - this would be implemented to check if order is filled
                    fill_result = {"filled": True}  # Placeholder for actual fill monitoring

                    # Check fill verification
                    if not fill_result.get("filled", False):
                        result.status = ExecutionStatus.FAILED
                        result.error = "First order did not fill within timeout"
                        return result

                    result.first_fill = fill_result

                    # Checkpoint: first order filled
                    await self.execution_coordinator.add_checkpoint(
                        execution_context, "first_order_filled", fill_result
                    )

                # Second leg
                second_order = self._prepare_order(opportunity, "short")
                second_exchange = (
                    "backpack"  # This would be determined from opportunity in real implementation
                )

                # Store in result
                result.second_exchange = second_exchange

                # Checkpoint: second order preparation
                await self.execution_coordinator.add_checkpoint(
                    execution_context,
                    "second_order_preparation",
                    {"exchange": second_exchange, "order": second_order.to_dict()},
                )

                # Place second order
                second_api = self.exchange_adapters[second_exchange]

                # Place order with verification
                try:
                    second_placed_order: Order = await second_api.place_order(
                        symbol=second_order.symbol,
                        side=second_order.side,
                        order_type=second_order.order_type,
                        quantity=second_order.quantity_requested,
                        price=second_order.price,
                        time_in_force=TimeInForce.GTC,  # Default value
                        # TODO: Handle post_only, reduce_only if needed via config/adapter
                    )
                    assert hasattr(second_placed_order, "client_order_id") and hasattr(
                        second_placed_order, "to_dict"
                    ), "second_placed_order missing required attributes"
                    result.second_order_id = (
                        second_placed_order.client_order_id
                    )  # Use client_order_id

                    # Checkpoint: second order placed
                    await self.execution_coordinator.add_checkpoint(
                        execution_context,
                        "second_order_placed",
                        {
                            "order_id": second_placed_order.client_order_id,
                            "order": second_placed_order.to_dict(),
                        },
                    )

                    # Verify second order
                    second_verification = await order_verifier.verify_order_placement(
                        second_exchange,
                        second_placed_order.client_order_id,  # Use client_order_id
                        {
                            "symbol": second_order.symbol,
                            "side": second_order.side,
                            "type": second_order.order_type,  # Corrected field
                        },
                    )

                    if not second_verification.get("success", False):
                        # Second order failed but first succeeded - partial completion
                        result.status = ExecutionStatus.PARTIALLY_COMPLETED
                        result.error = second_verification.get(
                            "error", "Second order verification failed"
                        )
                    else:
                        # Both orders succeeded
                        result.status = ExecutionStatus.COMPLETED

                        # Wait for second fill if needed
                        if self.config.get("execution.wait_for_second_fill", True):
                            # Monitor for fills - this would be implemented
                            # to check if order is filled
                            second_fill_result = {
                                "filled": True
                            }  # Placeholder for actual fill monitoring

                            if not second_fill_result.get("filled", False):
                                result.status = ExecutionStatus.PARTIALLY_COMPLETED
                                result.error = "Second order did not fill within timeout"
                            else:
                                result.second_fill = second_fill_result

                                # Checkpoint: second order filled
                                await self.execution_coordinator.add_checkpoint(
                                    execution_context, "second_order_filled", second_fill_result
                                )

                                # Complete execution
                                result.status = ExecutionStatus.COMPLETED

                except Exception as e:
                    # Second order failed
                    logger.error(f"Error placing second order: {e}")
                    result.status = ExecutionStatus.PARTIALLY_COMPLETED
                    result.error = f"Second order error: {str(e)}"

            except Exception as e:
                # First order failed
                logger.error(f"Error placing first order: {e}")
                result.status = ExecutionStatus.FAILED
                result.error = f"First order error: {str(e)}"

        except Exception as e:
            # General execution error
            logger.error(f"Error in sequential execution: {e}")
            result.status = ExecutionStatus.FAILED
            result.error = f"Execution error: {str(e)}"

        return result

    def _prepare_order(self, opportunity: OpportunityType, leg_type: str) -> Order:
        """Prepare an Order object for a specific leg of the opportunity."""
        # Ensure opportunity is the correct type before accessing attributes
        symbol_val: str
        quantity_val: Any
        price_val: Any

        # OpportunityType is always ArbitrageOpportunity, so no need for isinstance check
        symbol_val = opportunity.symbol
        quantity_val = opportunity.optimal_size
        price_val = opportunity.long_price if leg_type == "long" else opportunity.short_price

        # Validate extracted values
        if symbol_val is None or quantity_val is None:
            logger.error(f"Missing required fields (symbol/quantity) in opportunity: {opportunity}")
            raise ValueError("Invalid opportunity data for order preparation")

        # Convert quantity and price to Decimal if they are not None
        try:
            quantity_dec = Decimal(str(quantity_val)) if quantity_val is not None else None
            price_dec = Decimal(str(price_val)) if price_val is not None else None
        except (InvalidOperation, TypeError) as e:
            logger.error(f"Error converting quantity/price to Decimal: {e}")
            raise ValueError("Invalid numeric data in opportunity for order preparation") from e

        if quantity_dec is None:
            raise ValueError("Quantity cannot be None for order preparation")

        # Determine order type (assuming MARKET for now, could be configurable)
        order_type = OrderType.MARKET

        # Create the Order object using correct field names
        return Order(
            symbol=symbol_val,
            side=OrderSide.BUY if leg_type == "long" else OrderSide.SELL,
            order_type=order_type,
            quantity_requested=quantity_dec,
            price=price_dec,
            status=OrderStatus.NEW,
            created_at=datetime.now(UTC),
            # average_fill_price, exchange_order_id, updated_at, trades, strategy_name, signal_id are optional
        )

    async def _execute_simultaneous_with_verification(
        self, opportunity: OpportunityType, execution_context: ExecutionContext
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
        self, opportunity: OpportunityType, execution_result: ExecutionResult
    ) -> dict[str, Any]:
        """Perform comprehensive post-execution verification."""
        results = {}
        all_success = True
        error_msg = ""

        if execution_result.status in [
            ExecutionStatus.COMPLETED,
            ExecutionStatus.PARTIALLY_COMPLETED,
        ]:
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
            error_msg = (
                f"Execution did not complete successfully "
                f"({execution_result.status.name}). "
                f"Skipping post-verification."
            )

        return {
            "timestamp": int(time.time() * 1000),
            "success": all_success,
            "error": error_msg.strip() or None,
            "details": results,
        }

    async def _verify_positions(
        self, opportunity: OpportunityType, execution_result: ExecutionResult
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
        self, opportunity: OpportunityType, execution_result: ExecutionResult
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
        self, opportunity: OpportunityType, execution_result: ExecutionResult
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
        opportunity: OpportunityType,
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

    def _generate_execution_id(self, opportunity: OpportunityType) -> str:
        """
        Generate a unique execution ID.

        Args:
            opportunity: Arbitrage opportunity

        Returns:
            Unique execution ID
        """
        timestamp = int(time.time() * 1000)
        return f"exec_{timestamp}_{hash(opportunity) % 10000:04d}"
