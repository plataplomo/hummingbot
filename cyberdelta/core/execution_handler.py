"""CyberDeltaEngine execution handler module.

Provides the ExecutionHandler class for reliable trade execution across multiple exchanges,
including order placement, monitoring, retry logic, and circuit breaker integration.
"""

from __future__ import annotations  # Enable postponed evaluation

import asyncio
import secrets
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum, auto
from typing import Any

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args_models import GetOrderArgs, PlaceOrderArgs
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    Trade,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.validation.circuit_breaker import (
    CircuitBreakerSystem,
    CircuitBreakerTrippedError,
)
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Keep imports for type checking only if they cause circular dependencies otherwise

logger = get_logger(__name__)


class LongExchangeCircuitBreakerError(CircuitBreakerTrippedError):
    """Circuit breaker tripped for long exchange."""

    def __init__(self, exchange: str, reason: str) -> None:
        """Initialize LongExchangeCircuitBreakerError.

        Args:
            exchange: The exchange where the circuit breaker was tripped
            reason: The reason for the circuit breaker trip
        """
        self.exchange = exchange
        self.reason = reason
        super().__init__(f"Circuit breaker tripped for long exchange {exchange}: {reason}")


class ShortExchangeCircuitBreakerError(CircuitBreakerTrippedError):
    """Circuit breaker tripped for short exchange."""

    def __init__(self, exchange: str, reason: str) -> None:
        """Initialize ShortExchangeCircuitBreakerError.

        Args:
            exchange: The exchange where the circuit breaker was tripped
            reason: The reason for the circuit breaker trip
        """
        self.exchange = exchange
        self.reason = reason
        super().__init__(f"Circuit breaker tripped for short exchange {exchange}: {reason}")


class MissingClientError(APIError):
    """Missing API client error."""

    def __init__(self, exchange: str) -> None:
        """Initialize MissingClientError.

        Args:
            exchange: The exchange that is missing an API client
        """
        self.exchange = exchange
        super().__init__(f"Missing API client for '{exchange}'", "MISSING_CLIENT")


class SymbolMappingError(APIError):
    """Symbol mapping failed error."""

    def __init__(self, symbol: str, leg: str, exchange: str) -> None:
        """Initialize SymbolMappingError.

        Args:
            symbol: The symbol that could not be mapped
            leg: The leg type (long/short)
            exchange: The exchange where mapping failed
        """
        self.symbol = symbol
        self.leg = leg
        self.exchange = exchange
        super().__init__(
            f"Could not map symbol '{symbol}' for {leg} leg on exchange '{exchange}'",
            "SYMBOL_MAPPING_FAILED",
        )


class AverageFillPriceError(ValueError):
    """Average fill price cannot be None for synthetic trade."""

    def __init__(self) -> None:
        """Initialize AverageFillPriceError."""
        super().__init__("average_fill_price cannot be None for synthetic trade")


# NOTE: CyberDeltaEngine Order model uses 'client_order_id' as the unique identifier,
# 'quantity_requested' for order size, 'quantity_filled' for filled size, and
# 'average_fill_price' for fill price. There is no 'id', 'quantity',
# or 'avg_fill_price' attribute.


class ExecutionStatus(Enum):
    """Status of an execution."""

    PENDING = auto()
    EXECUTING = auto()
    COMPLETED = auto()
    FAILED = auto()
    PARTIALLY_COMPLETED = auto()
    COMPENSATING = auto()
    REJECTED = auto()


class TradeExecution:
    """Represents a trade execution across multiple exchanges."""

    def __init__(self, opportunity: SizedOpportunity) -> None:
        """Initialize a trade execution.

        Args:
            opportunity: Sized arbitrage opportunity

        """
        self.id = str(uuid.uuid4())
        self.opportunity = opportunity
        self.status = ExecutionStatus.PENDING
        self.error_message: str | None = None
        self.long_order_id: str | None = None
        self.short_order_id: str | None = None
        self.long_position_id: str | None = None
        self.short_position_id: str | None = None
        self.long_order_response: dict[str, Any] | None = None
        self.short_order_response: dict[str, Any] | None = None
        self.start_time: datetime | None = None
        self.end_time: datetime | None = None
        self.long_fill_price: Decimal | None = None
        self.short_fill_price: Decimal | None = None
        self.long_fill_quantity: Decimal | None = None
        self.short_fill_quantity: Decimal | None = None
        self.realized_pnl: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "opportunity": {
                "symbol": self.opportunity.opportunity.symbol,
                "long_exchange": self.opportunity.opportunity.long_exchange,
                "short_exchange": self.opportunity.opportunity.short_exchange,
                "long_size": str(self.opportunity.long_size),
                "short_size": str(self.opportunity.short_size),
                "expected_profit": str(self.opportunity.expected_profit),
            },
            "status": self.status.name,
            "error_message": self.error_message,
            "long_order_id": self.long_order_id,
            "short_order_id": self.short_order_id,
            "long_position_id": self.long_position_id,
            "short_position_id": self.short_position_id,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "long_fill_price": str(self.long_fill_price) if self.long_fill_price else None,
            "short_fill_price": str(self.short_fill_price) if self.short_fill_price else None,
            "long_fill_quantity": str(self.long_fill_quantity) if self.long_fill_quantity else None,
            "short_fill_quantity": str(self.short_fill_quantity)
            if self.short_fill_quantity
            else None,
            "realized_pnl": str(self.realized_pnl) if self.realized_pnl is not None else None,
        }

    def __str__(self) -> str:
        """Return string representation of the execution."""
        return (
            f"TradeExecution: {self.opportunity.opportunity.symbol} - "
            f"Long: {self.opportunity.opportunity.long_exchange} "
            f"${self.opportunity.long_size:.2f}, "
            f"Short: {self.opportunity.opportunity.short_exchange} "
            f"${self.opportunity.short_size:.2f}, "
            f"Status: {self.status.name}"
        )


class ExecutionHandler:
    """Execute trades on exchanges reliably.

    Responsible for:
    - Placing orders on exchanges
    - Monitoring order status and fills
    - Handling partial fills and cancellations
    - Implementing sequenced execution for multi-leg strategies
    - Applying retry logic for temporary failures
    - Implementing circuit breakers for critical failures
    """

    def __init__(
        self,
        app_settings: AppSettings,
        portfolio_tracker: PortfolioTracker,
        symbol_mapper: SymbolMapper,
        circuit_breaker_system: CircuitBreakerSystem | None = None,
    ) -> None:
        """Initialize the execution handler.

        Args:
            app_settings: Application configuration settings
            portfolio_tracker: Portfolio tracker for position updates
            symbol_mapper: SymbolMapper for translating symbols
            circuit_breaker_system: The main circuit breaker system (optional)

        """
        self.app_settings = app_settings
        self.portfolio_tracker = portfolio_tracker
        self.symbol_mapper = symbol_mapper
        self.circuit_breaker_system = circuit_breaker_system
        self.api_clients: dict[str, ExchangeAPI] = {}

        # Configuration values from AppSettings
        self.max_slippage = self.app_settings.execution.max_slippage_pct
        self.max_retries = self.app_settings.execution.max_retries
        self.retry_delay_base = float(self.app_settings.execution.retry_delay_base_sec)
        # TODO: Add execution history configuration to AppSettings when needed
        self.max_execution_history = 100  # Default history size

        self.executions: list[TradeExecution] = []
        self.active_executions: dict[str, TradeExecution] = {}
        self.logger = get_logger(__name__)
        logger.info("ExecutionHandler initialized.")

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """Register an API client for an exchange.

        Args:
            exchange_id: Exchange identifier
            client: ExchangeAPI implementation

        """
        self.api_clients[exchange_id] = client
        logger.info(
            "api_client_registered",
            exchange_id=exchange_id,
            component="ExecutionHandler",
            action="register_api_client",
            message=f"Registered API client for {exchange_id} in ExecutionHandler",
        )

    async def execute_opportunity(self, opportunity: SizedOpportunity) -> TradeExecution:
        """Execute an arbitrage opportunity.

        Uses SymbolMapper to get exchange-specific symbols.

        Args:
            opportunity: Sized arbitrage opportunity

        Returns:
            TradeExecution object representing the outcome

        """
        execution = TradeExecution(opportunity)
        self.active_executions[execution.id] = execution
        execution.start_time = datetime.now(UTC)

        logger.info(
            "execution_starting",
            execution_id=execution.id,
            symbol=opportunity.opportunity.symbol,
            message="Starting execution for opportunity",
        )

        try:
            # Check circuit breakers
            self._check_circuit_breakers(execution, opportunity)

            # Setup and validate clients and symbols
            (
                _,
                _,
                long_symbol,
                short_symbol,
            ) = await self._setup_execution_prerequisites(execution, opportunity)

            # Execute the main order placement logic
            await self._place_orders_for_opportunity(execution, long_symbol, short_symbol)

        except CircuitBreakerTrippedError as e:
            return self._handle_circuit_breaker_rejection(execution, e)
        except APIError as e:
            return self._handle_api_error_during_execution(execution, opportunity, e)
        except (ValueError, TypeError, OSError) as e:
            return self._handle_unexpected_error_during_execution(execution, opportunity, e)

        # Finalize execution
        return self._finalize_execution(execution)

    def _check_circuit_breakers(
        self,
        execution: TradeExecution,
        opportunity: SizedOpportunity,
    ) -> None:
        """Check circuit breakers for both exchanges.

        Raises:
            CircuitBreakerTrippedError: If circuit breaker is tripped for either exchange.
        """
        if not self.circuit_breaker_system:
            return

        can_long, long_reason = self.circuit_breaker_system.can_execute(
            opportunity.opportunity.long_exchange,
        )
        if not can_long:
            raise LongExchangeCircuitBreakerError(
                opportunity.opportunity.long_exchange,
                long_reason or "Circuit breaker tripped for long exchange",
            )

        can_short, short_reason = self.circuit_breaker_system.can_execute(
            opportunity.opportunity.short_exchange,
        )
        if not can_short:
            raise ShortExchangeCircuitBreakerError(
                opportunity.opportunity.short_exchange,
                short_reason or "Circuit breaker tripped for short exchange",
            )

    async def _setup_execution_prerequisites(
        self,
        execution: TradeExecution,
        opportunity: SizedOpportunity,
    ) -> tuple[ExchangeAPI, ExchangeAPI, str, str]:
        """Setup and validate API clients and symbols for execution.

        Raises:
            APIError: If API clients are missing or symbol mapping fails.
        """
        # Get API clients
        long_client = self.api_clients.get(opportunity.opportunity.long_exchange)
        short_client = self.api_clients.get(opportunity.opportunity.short_exchange)

        if not long_client or not short_client:
            missing_client_exchange_name = (
                opportunity.opportunity.long_exchange
                if not long_client
                else opportunity.opportunity.short_exchange
            )
            raise MissingClientError(missing_client_exchange_name)

        # Get exchange-specific symbols
        long_symbol = self.symbol_mapper.get_exchange_symbol(
            opportunity.opportunity.symbol,
            opportunity.opportunity.long_exchange,
        )
        short_symbol = self.symbol_mapper.get_exchange_symbol(
            opportunity.opportunity.symbol,
            opportunity.opportunity.short_exchange,
        )

        if not long_symbol or not short_symbol:
            missing_leg = "long" if not long_symbol else "short"
            missing_symbol_exchange_name = (
                opportunity.opportunity.long_exchange
                if not long_symbol
                else opportunity.opportunity.short_exchange
            )
            raise SymbolMappingError(
                opportunity.opportunity.symbol,
                missing_leg,
                missing_symbol_exchange_name,
            )

        return long_client, short_client, long_symbol, short_symbol

    def _handle_circuit_breaker_rejection(
        self,
        execution: TradeExecution,
        e: CircuitBreakerTrippedError,
    ) -> TradeExecution:
        """Handle circuit breaker rejection."""
        op_error_msg = f"Execution {execution.id} rejected by circuit breaker: {e}"
        logger.error(
            "execution_rejected_by_circuit_breaker",
            action="handle_circuit_breaker",
            execution_id=execution.id,
            error=str(e),
            message=op_error_msg,
        )
        execution.error_message = str(e)
        execution.status = ExecutionStatus.REJECTED
        execution.end_time = datetime.now(UTC)
        self._add_to_history(execution)
        return execution

    def _handle_api_error_during_execution(
        self,
        execution: TradeExecution,
        opportunity: SizedOpportunity,
        e: APIError,
    ) -> TradeExecution:
        """Handle API errors during execution."""
        op_error_msg = (
            f"APIError during execution {execution.id} for {opportunity.opportunity.symbol}: {e}"
        )
        logger.error(
            "api_error_during_execution",
            action="handle_api_error",
            execution_id=execution.id,
            symbol=opportunity.opportunity.symbol,
            error=str(e),
            message=op_error_msg,
        )
        execution.error_message = str(e)
        execution.status = ExecutionStatus.FAILED

        if self.circuit_breaker_system:
            self._record_api_error_for_circuit_breaker(execution, opportunity, e)

        execution.end_time = datetime.now(UTC)
        self._add_to_history(execution)
        return execution

    def _handle_unexpected_error_during_execution(
        self,
        execution: TradeExecution,
        opportunity: SizedOpportunity,
        e: Exception,
    ) -> TradeExecution:
        """Handle unexpected errors during execution."""
        op_error_msg = (
            f"Unexpected error during execution {execution.id} "
            f"for {opportunity.opportunity.symbol}: {e}"
        )
        logger.error(op_error_msg)
        execution.error_message = str(e)
        execution.status = ExecutionStatus.FAILED
        execution.end_time = datetime.now(UTC)
        self._add_to_history(execution)
        return execution

    def _record_api_error_for_circuit_breaker(
        self,
        execution: TradeExecution,
        opportunity: SizedOpportunity,
        e: APIError,
    ) -> None:
        """Record API error for circuit breaker system."""
        # Determine which exchange caused the error
        long_not_filled = False
        if execution.long_order_response is not None:
            long_not_filled = not execution.long_order_response.get("is_filled", True)

        short_not_filled = False
        if execution.short_order_response is not None:
            short_not_filled = not execution.short_order_response.get("is_filled", True)

        if execution.long_order_id and not execution.short_order_id and long_not_filled:
            failed_exchange_str = opportunity.opportunity.long_exchange
        elif execution.short_order_id and short_not_filled:
            failed_exchange_str = opportunity.opportunity.short_exchange
        else:
            failed_exchange_str = opportunity.opportunity.long_exchange  # Default to long
            logger.warning(
                "api_error_exchange_determination_failed",
                execution_id=execution.id,
                error=str(e),
                default_exchange=failed_exchange_str,
                message=(
                    "Could not determine failing exchange for APIError, defaulting to long exchange"
                ),
            )

        if self.circuit_breaker_system is not None:
            self.circuit_breaker_system.record_api_error(failed_exchange_str, str(e))

    def _finalize_execution(self, execution: TradeExecution) -> TradeExecution:
        """Finalize execution and add to history."""
        # Ensure end_time is set if not already by failure paths
        if execution.end_time is None:
            execution.end_time = datetime.now(UTC)

        # Log final status before adding to history
        if execution.status == ExecutionStatus.FAILED:
            logger.error(
                "execution_failed",
                execution_id=execution.id,
                error_message=execution.error_message,
                message="Execution ultimately FAILED",
            )
        elif execution.status == ExecutionStatus.REJECTED:
            logger.warning(
                "execution_rejected",
                execution_id=execution.id,
                error_message=execution.error_message,
                message="Execution was REJECTED",
            )
        else:
            logger.info(
                "execution_finished",
                execution_id=execution.id,
                status=execution.status.name,
                action="finish_execution",
                message=f"Execution {execution.id} finished with status: {execution.status.name}",
            )

        self._add_to_history(execution)
        return execution

    async def _place_orders_for_opportunity(
        self,
        execution: TradeExecution,
        long_symbol: str,
        short_symbol: str,
    ) -> None:
        """Places orders for both legs of the opportunity.

        Handles sequential placement and compensation.
        This is a reconstructed method body.
        """
        logger.info(
            "execution_placing_orders",
            execution_id=execution.id,
            symbol=execution.opportunity.opportunity.symbol,
            message="Placing orders for execution",
        )
        opportunity = execution.opportunity.opportunity  # Convenience
        sized_opp = execution.opportunity  # Convenience

        # Calculate base asset quantities
        base_quantities = self._calculate_base_asset_quantities(execution, opportunity, sized_opp)
        if base_quantities is None:
            return  # Error already set in execution

        base_asset_quantity_long, base_asset_quantity_short = base_quantities

        # Get default time in force
        default_tif = self._get_default_time_in_force()

        # Place long order
        long_order_result = await self._place_long_order(
            execution,
            opportunity,
            long_symbol,
            base_asset_quantity_long,
            default_tif,
        )
        if long_order_result is None:
            return  # Error already set in execution

        # Place short order with compensation handling
        await self._place_short_order_with_compensation(
            execution,
            opportunity,
            short_symbol,
            long_symbol,
            base_asset_quantity_short,
            base_asset_quantity_long,
            default_tif,
        )

    def _calculate_base_asset_quantities(
        self,
        execution: TradeExecution,
        opportunity: ArbitrageOpportunity,
        sized_opp: SizedOpportunity,
    ) -> tuple[Decimal, Decimal] | None:
        """Calculate base asset quantities for both legs."""
        # --- Calculate Base Asset Quantities ---
        base_asset_quantity_long: Decimal | None = None
        if opportunity.long_price and opportunity.long_price > Decimal(0):
            base_asset_quantity_long = sized_opp.long_size / opportunity.long_price
        else:
            err_msg = (
                f"Execution {execution.id}: Cannot derive long base asset quantity. "
                f"Size: {sized_opp.long_size}, Price: {opportunity.long_price}"
            )
            logger.error(
                "cannot_derive_long_quantity",
                action="calculate_quantity",
                execution_id=execution.id,
                long_size=sized_opp.long_size,
                long_price=opportunity.long_price,
                message=err_msg,
            )
            execution.error_message = err_msg
            execution.status = ExecutionStatus.FAILED
            return None

        base_asset_quantity_short: Decimal | None = None
        if opportunity.short_price and opportunity.short_price > Decimal(0):
            base_asset_quantity_short = sized_opp.short_size / opportunity.short_price
        else:
            err_msg = (
                f"Execution {execution.id}: Cannot derive short base asset quantity. "
                f"Size: {sized_opp.short_size}, Price: {opportunity.short_price}"
            )
            logger.error(
                "cannot_derive_short_quantity",
                action="calculate_quantity",
                execution_id=execution.id,
                short_size=sized_opp.short_size,
                short_price=opportunity.short_price,
                message=err_msg,
            )
            execution.error_message = err_msg
            execution.status = ExecutionStatus.FAILED
            return None

        return base_asset_quantity_long, base_asset_quantity_short

    def _get_default_time_in_force(self) -> TimeInForce:
        """Get default time in force configuration."""
        # Determine default TimeInForce for initial legs
        # TODO: Add execution.default_time_in_force to AppSettings when needed
        tif_config_str = "IOC"  # Default to IOC
        try:
            default_tif = TimeInForce(tif_config_str)
        except ValueError:
            logger.warning(
                "invalid_time_in_force_config",
                config_value=tif_config_str,
                fallback_value="IOC",
                action="parse_time_in_force",
                issue="invalid_config",
                message=f"Invalid TimeInForce config '{tif_config_str}', using IOC",
            )
            default_tif = TimeInForce.IOC
        return default_tif

    async def _place_long_order(
        self,
        execution: TradeExecution,
        opportunity: ArbitrageOpportunity,
        long_symbol: str,
        base_asset_quantity_long: Decimal,
        default_tif: TimeInForce,
    ) -> Order | None:
        """Place the long order."""
        # --- Place Long Order ---
        execution.status = ExecutionStatus.EXECUTING
        long_order_result: Order | None = await self._place_order_with_retry(
            execution=execution,
            exchange_id=opportunity.long_exchange,
            symbol=long_symbol,
            side=OrderSide.BUY,
            quantity=base_asset_quantity_long,
            order_type=OrderType.MARKET,  # TODO: Configurable order type
            is_long_leg=True,
            time_in_force=default_tif,
            reduce_only=False,
            post_only=False,
        )

        if long_order_result is None or long_order_result.status != OrderStatus.FILLED:
            err_msg = (
                f"Execution {execution.id}: Long order placement failed or not filled. "
                f"Status: {long_order_result.status if long_order_result else 'None'}"
            )
            logger.error(
                "long_order_placement_failed",
                action="place_long_order",
                execution_id=execution.id,
                order_status=long_order_result.status.value if long_order_result else None,
                message=err_msg,
            )
            execution.error_message = execution.error_message or err_msg
            execution.status = ExecutionStatus.FAILED
            # No compensation needed if the first leg fails before any fill
            return None

        execution.long_order_id = long_order_result.exchange_order_id
        execution.long_order_response = long_order_result.model_dump(mode="json")
        execution.long_fill_price = long_order_result.average_fill_price
        execution.long_fill_quantity = long_order_result.quantity_filled
        logger.info(
            "execution_long_order_placed_and_filled",
            execution_id=execution.id,
            order_id=execution.long_order_id,
            message="Long order placed and filled successfully",
        )
        await self._handle_filled_order(
            execution,
            long_order_result,
            opportunity.long_exchange,
            is_long_leg=True,
            expected_long_price=opportunity.long_price,
        )

        return long_order_result

    async def _place_short_order_with_compensation(
        self,
        execution: TradeExecution,
        opportunity: ArbitrageOpportunity,
        short_symbol: str,
        long_symbol: str,
        base_asset_quantity_short: Decimal,
        base_asset_quantity_long: Decimal,
        default_tif: TimeInForce,
    ) -> None:
        """Place short order and handle compensation if needed."""
        # --- Place Short Order ---
        short_order_result: Order | None = None
        try:
            short_order_result = await self._place_order_with_retry(
                execution=execution,
                exchange_id=opportunity.short_exchange,
                symbol=short_symbol,
                side=OrderSide.SELL,
                quantity=base_asset_quantity_short,
                order_type=OrderType.MARKET,  # TODO: Configurable order type
                is_long_leg=False,
                time_in_force=default_tif,
                reduce_only=False,
                post_only=False,
            )
        except APIError as e_short_leg:
            await self._handle_short_order_api_error(execution, opportunity, e_short_leg)

        if short_order_result is None or short_order_result.status != OrderStatus.FILLED:
            await self._handle_short_order_failure(
                execution,
                opportunity,
                short_order_result,
                long_symbol,
                base_asset_quantity_long,
            )
        else:
            # Short order successful
            execution.short_order_id = short_order_result.exchange_order_id
            execution.short_order_response = short_order_result.model_dump(mode="json")
            execution.short_fill_price = short_order_result.average_fill_price
            execution.short_fill_quantity = short_order_result.quantity_filled
            execution.status = ExecutionStatus.COMPLETED
            logger.info(
                "execution_completed_successfully",
                action="complete_execution",
                execution_id=execution.id,
                short_order_id=execution.short_order_id,
                message=(
                    f"Execution {execution.id}: Both orders completed successfully. "
                    f"Short order: {execution.short_order_id}"
                ),
            )
            await self._handle_filled_order(
                execution,
                short_order_result,
                opportunity.short_exchange,
                is_long_leg=False,
                expected_short_price=opportunity.short_price,
            )

    async def _handle_short_order_api_error(
        self,
        execution: TradeExecution,
        opportunity: ArbitrageOpportunity,
        e_short_leg: APIError,
    ) -> None:
        """Handle API error during short order placement."""
        err_msg = (
            f"Execution {execution.id}: Short order placement failed with APIError: "
            f"{e_short_leg.message}"
        )
        logger.error(
            "short_order_api_error",
            action="place_short_order",
            execution_id=execution.id,
            error_message=e_short_leg.message,
            message=err_msg,
        )
        execution.error_message = e_short_leg.message  # Preserve specific API error
        execution.status = ExecutionStatus.COMPENSATING  # Mark for compensation
        # Short leg failed, proceed to compensation if long leg was filled (which it was)
        # This block is reached only if _place_order_with_retry re-raises the APIError.
        # Ensure circuit breaker for short leg is also recorded here.
        if self.circuit_breaker_system:
            self.circuit_breaker_system.record_api_error(
                opportunity.short_exchange,
                str(e_short_leg.code),
            )

    async def _handle_short_order_failure(
        self,
        execution: TradeExecution,
        opportunity: ArbitrageOpportunity,
        short_order_result: Order | None,
        long_symbol: str,
        base_asset_quantity_long: Decimal,
    ) -> None:
        """Handle short order failure and attempt compensation."""
        # This block will be entered if _place_order_with_retry returned None
        # (e.g. max retries exhausted without non-APIError failure, unlikely with current logic)
        # OR if an APIError was caught above and short_order_result remained None,
        # OR if it returned an order that was not FILLED (e.g. CANCELED by exchange quickly).

        if (
            execution.status != ExecutionStatus.COMPENSATING
        ):  # If not already marked by APIError catch
            err_msg_detail = (
                f"Status: {short_order_result.status.name}"
                if short_order_result
                else "No order object returned (likely due to retries or pre-APIError issue)"
            )
            err_msg = (
                f"Execution {execution.id}: Short order placement failed or not filled. "
                f"{err_msg_detail}"
            )
            logger.error(
                "short_order_placement_failed",
                action="place_short_order",
                execution_id=execution.id,
                error_detail=err_msg_detail,
                message=err_msg,
            )
            if execution.error_message is None:  # Only set if not already set by APIError
                execution.error_message = err_msg
            execution.status = ExecutionStatus.COMPENSATING

        logger.info(
            "execution_compensation_attempt",
            execution_id=execution.id,
            leg="long",
            action="compensate_leg",
            message=f"Execution {execution.id}: Attempting to compensate long leg...",
        )
        compensated = False
        try:
            compensated = await self._compensate_position(
                execution=execution,
                exchange_id=opportunity.long_exchange,
                symbol=long_symbol,
                side=OrderSide.SELL,  # Compensate by selling the long
                quantity=base_asset_quantity_long,
            )
        except (ValueError, TypeError, RuntimeError) as e_comp:
            logger.critical(
                "execution_compensation_exception",
                execution_id=execution.id,
                error=str(e_comp),
                action="compensate_position",
                message="Exception during compensation position call",
            )
            execution.error_message = (
                (execution.error_message + " | Compensation attempt raised error: " + str(e_comp))
                if execution.error_message
                else "Compensation attempt raised error: " + str(e_comp)
            )

        if compensated:
            logger.info(
                "execution_compensation_successful",
                execution_id=execution.id,
                leg="long",
                action="compensate_leg",
                status="successful",
                message=f"Execution {execution.id}: Compensation of long leg successful.",
            )
            execution.error_message = (
                (execution.error_message + " | Long leg compensated.")
                if execution.error_message
                else "Long leg compensated."
            )
        else:
            logger.critical(
                "execution_compensation_failed_critical",
                execution_id=execution.id,
                symbol=long_symbol,
                exchange=opportunity.long_exchange,
                action="compensate_long_leg",
                status="failed",
                message="COMPENSATION OF LONG LEG FAILED! Manual intervention required",
            )
            execution.error_message = (
                (execution.error_message + " | COMPENSATION FAILED!")
                if execution.error_message
                else "COMPENSATION FAILED!"
            )

    async def _handle_filled_order(
        self,
        execution: TradeExecution,
        order: Order,
        exchange_id: str,
        is_long_leg: bool,
        expected_long_price: Decimal | None = None,
        expected_short_price: Decimal | None = None,
    ) -> None:
        """Handle a filled order, updating execution details and portfolio.

        Centralized logic for processing fills for both initial and compensation orders.
        """
        logger.info(
            "execution_handling_filled_order",
            exchange_id=exchange_id,
            order_id=order.exchange_order_id or order.client_order_id,
            execution_id=execution.id,
            is_long_leg=is_long_leg,
            action="handle_filled_order",
            message="Handling filled order for execution",
        )

        # Validate order status and data
        if not self._validate_filled_order(execution, order, exchange_id):
            return

        # Update execution details
        self._update_execution_fill_details(execution, order, is_long_leg)

        # Check for slippage
        self._check_slippage(
            execution,
            order,
            is_long_leg,
            expected_long_price,
            expected_short_price,
        )

        # Process trades
        await self._process_order_trades(execution, order, exchange_id)

    def _validate_filled_order(
        self,
        execution: TradeExecution,
        order: Order,
        exchange_id: str,
    ) -> bool:
        """Validate that the order is properly filled with valid data."""
        if order.status != OrderStatus.FILLED:
            logger.warning(
                "execution_unexpected_order_status",
                execution_id=execution.id,
                order_status=order.status.value
                if hasattr(order.status, "value")
                else str(order.status),
                order_id=order.exchange_order_id or order.client_order_id,
                exchange_id=exchange_id,
                action="validate_filled_order",
                message="Unexpected order status for filled order",
            )
            return False

        # After confirming FILLED status, average_fill_price and quantity_filled should be valid.
        # Add checks for robustness and to satisfy type checkers.
        if (
            order.average_fill_price is None
            # order.quantity_filled is None, # This check is redundant as quantity_filled is Decimal
            or order.quantity_filled == Decimal(0)  # quantity_filled can be 0 if not filled.
        ):
            logger.error(
                "execution_filled_order_missing_data",
                execution_id=execution.id,
                order_id=order.exchange_order_id or order.client_order_id,
                exchange_id=exchange_id,
                avg_fill_price=str(order.average_fill_price) if order.average_fill_price else None,
                quantity_filled=str(order.quantity_filled),
                action="validate_filled_order",
                message="Filled order has missing avg_fill_price or zero quantity_filled",
            )
            execution.status = ExecutionStatus.FAILED
            execution.error_message = (
                f"Filled order {order.exchange_order_id or order.client_order_id} had "
                f"inconsistent data (price/qty)."
            )
            return False

        return True

    def _update_execution_fill_details(
        self,
        execution: TradeExecution,
        order: Order,
        is_long_leg: bool,
    ) -> None:
        """Update execution details with fill information."""
        # Update execution details with now-guaranteed Decimal values
        if is_long_leg:
            execution.long_fill_price = order.average_fill_price
            execution.long_fill_quantity = order.quantity_filled
        else:
            execution.short_fill_price = order.average_fill_price
            execution.short_fill_quantity = order.quantity_filled

    def _check_slippage(
        self,
        execution: TradeExecution,
        order: Order,
        is_long_leg: bool,
        expected_long_price: Decimal | None,
        expected_short_price: Decimal | None,
    ) -> None:
        """Check for slippage against expected prices."""
        # Check slippage (using guaranteed Decimal for order.average_fill_price)
        if is_long_leg and expected_long_price is not None:
            if order.average_fill_price and order.average_fill_price > expected_long_price * (
                Decimal(1) + self.max_slippage
            ):
                logger.warning(
                    "execution_slippage_detected_long",
                    execution_id=execution.id,
                    fill_price=str(order.average_fill_price),
                    expected_price=str(expected_long_price),
                    leg="long",
                    action="check_slippage",
                    message="Slippage detected for long leg",
                )
        elif (
            not is_long_leg
            and expected_short_price is not None
            and order.average_fill_price
            and order.average_fill_price < expected_short_price * (Decimal(1) - self.max_slippage)
        ):
            logger.warning(
                "execution_slippage_detected_short",
                execution_id=execution.id,
                fill_price=str(order.average_fill_price),
                expected_price=str(expected_short_price),
                leg="short",
                action="check_slippage",
                message="Slippage detected for short leg",
            )

    async def _process_order_trades(
        self,
        execution: TradeExecution,
        order: Order,
        exchange_id: str,
    ) -> None:
        """Process trades associated with the order."""
        # Process trades associated with the order
        if order.trades:  # order.trades is list[Trade]
            logger.info(
                "execution_processing_trades",
                execution_id=execution.id,
                trade_count=len(order.trades),
                order_id=order.exchange_order_id or order.client_order_id,
                action="process_order_trades",
                message="Processing trades from order",
            )
            await self._process_individual_trades(execution, order, exchange_id)
        elif order.quantity_filled > Decimal(0):
            # Fallback: Order is FILLED, has aggregate fill data, but no individual trades.
            await self._create_and_process_synthetic_trade(execution, order, exchange_id)
        else:
            # Order is FILLED, but no trades and no/invalid aggregate fill price/quantity
            logger.error(
                "execution_order_inconsistent_state",
                execution_id=execution.id,
                order_id=order.exchange_order_id or order.client_order_id,
                avg_price=str(order.average_fill_price) if order.average_fill_price else None,
                qty_filled=str(order.quantity_filled),
                action="process_order_trades",
                message="Order is FILLED but has no trades and insufficient aggregate data",
            )

    async def _process_individual_trades(
        self,
        execution: TradeExecution,
        order: Order,
        exchange_id: str,
    ) -> None:
        """Process individual trades from the order."""
        for trade_from_order in order.trades:
            try:
                # Ensure the trade from order has the correct exchange_id if not already set or
                # different.
                # This might be redundant if mappers always set it correctly.
                # For now, we assume portfolio_tracker uses the exchange_id passed to it.
                if trade_from_order.exchange != exchange_id:
                    logger.warning(
                        "execution_trade_exchange_mismatch",
                        trade_id=trade_from_order.id,
                        order_id=order.exchange_order_id or order.client_order_id,
                        trade_exchange=trade_from_order.exchange,
                        context_exchange=exchange_id,
                        action="process_individual_trades",
                        message="Trade exchange differs from handling context exchange",
                    )

                await self.portfolio_tracker.process_trade(exchange_id, trade_from_order)
                logger.info(
                    "execution_trade_processed",
                    trade_id=trade_from_order.id,
                    order_id=order.exchange_order_id or order.client_order_id,
                    execution_id=execution.id,
                    action="process_individual_trades",
                    message="Trade processed successfully",
                )
            except (ValueError, TypeError, RuntimeError) as e_process_trade:
                logger.exception(
                    "execution_trade_processing_failed",
                    execution_id=execution.id,
                    trade_id=trade_from_order.id,
                    order_id=order.exchange_order_id or order.client_order_id,
                    exchange_id=exchange_id,
                    error=str(e_process_trade),
                    action="process_individual_trades",
                    message="Failed to process trade from order",
                )
                execution.status = ExecutionStatus.FAILED
                execution.error_message = (
                    f"Failed to process constituent trade {trade_from_order.id} for order "
                    f"{order.exchange_order_id or order.client_order_id}."
                )
                return  # Stop further processing for this order if a trade fails.

    async def _create_and_process_synthetic_trade(
        self,
        execution: TradeExecution,
        order: Order,
        exchange_id: str,
    ) -> None:
        """Create and process a synthetic trade for orders without individual trades."""
        # Create a synthetic Trade object.
        logger.info(
            "execution_creating_synthetic_trade",
            execution_id=execution.id,
            order_id=order.exchange_order_id or order.client_order_id,
            action="create_synthetic_trade",
            message=(
                "Order is FILLED with aggregate data but no individual trades, "
                "creating synthetic trade"
            ),
        )
        try:
            # Ensure average_fill_price and quantity_filled are not None before use,
            # which is guaranteed by the elif condition.
            # DEFENSIVE CHECK: Mypy=[arg-type] Ruff=[none]
            if order.average_fill_price is None:
                raise AverageFillPriceError()

            synthetic_trade = Trade(
                id=f"synth_{order.exchange_order_id or order.client_order_id}_"
                f"{uuid.uuid4().hex[:8]}",
                symbol=self.symbol_mapper.get_internal_symbol(order.symbol, exchange_id)
                or order.symbol,
                side=order.side,
                order_id=str(order.exchange_order_id or order.client_order_id),
                exchange=exchange_id,
                client_order_id=order.client_order_id,
                price=order.average_fill_price,  # Not None here
                quantity=order.quantity_filled,  # Not None here
                fee=Decimal(
                    "0.0",
                ),  # Default to 0.0 for synthetic trade if Mypy complains about None
                fee_asset=None,  # Cannot get from Order model; Trade model allows None
                executed_at=order.updated_at or datetime.now(UTC),
                is_maker=False,  # Default for synthetic trade; Trade model defaults to False
            )
            await self.portfolio_tracker.process_trade(exchange_id, synthetic_trade)
            logger.info(
                "execution_synthetic_trade_processed",
                order_id=synthetic_trade.order_id,
                execution_id=execution.id,
                action="create_synthetic_trade",
                message="Synthetic trade processed successfully",
            )
        except (ValueError, TypeError, RuntimeError) as e_synth_trade:
            logger.exception(
                "execution_synthetic_trade_failed",
                execution_id=execution.id,
                order_id=order.exchange_order_id or order.client_order_id,
                exchange_id=exchange_id,
                error=str(e_synth_trade),
                action="create_synthetic_trade",
                message="Failed to create or process synthetic trade",
            )
            execution.status = ExecutionStatus.FAILED
            execution.error_message = (
                f"Failed to process synthetic trade for order "
                f"{order.exchange_order_id or order.client_order_id}."
            )
            return

    async def _get_order_status(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str,
        symbol: str | None = None,  # Required by some exchanges
        client_order_id: str | None = None,  # Required by some exchanges
    ) -> Order | None:
        """Get order status from the exchange with retry logic.

        Args:
            execution: The parent TradeExecution object.
            exchange_id: Target exchange.
            order_id: Exchange order ID.
            symbol: Optional symbol (required by some exchanges).
            client_order_id: Optional client order ID (required by some exchanges).

        Returns:
            Order object with updated status, or None if fetching failed.

        """
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(
                "execution_no_api_client",
                execution_id=execution.id,
                exchange_id=exchange_id,
                action="place_order",
                error="no_api_client",
                message=f"Execution {execution.id}: No API client for {exchange_id}",
            )
            return None

        context = f"getting status for order {order_id}"

        for attempt in range(self.max_retries):
            try:
                logger.debug(
                    "execution_order_status_attempt",
                    execution_id=execution.id,
                    context=context,
                    exchange_id=exchange_id,
                    attempt=attempt + 1,
                    action="get_order_status",
                    message="Getting order status attempt",
                )
                order_status = await client.get_order_status(
                    args=GetOrderArgs(
                        order_id=order_id,
                        symbol=symbol,
                        client_order_id=client_order_id,
                    ),
                )
                if order_status:
                    logger.debug(
                        "execution_order_status_retrieved",
                        execution_id=execution.id,
                        order_id=order_id,
                        status=order_status.status.value
                        if hasattr(order_status.status, "value")
                        else str(order_status.status),
                        action="get_order_status",
                        message="Got order status successfully",
                    )
                    return order_status
                # Handle case where get_order_status returns None without exception
                logger.warning(
                    "execution_order_status_none",
                    execution_id=execution.id,
                    order_id=order_id,
                    action="get_order_status",
                    message="_get_order_status returned None",
                )
                # Decide if retryable or assume failed/cancelled
                if attempt == self.max_retries - 1:
                    return None

            except APIError as e:
                # Handle API error with circuit breaker and specific error handling
                should_continue = await self._handle_order_status_api_error(
                    execution,
                    exchange_id,
                    order_id,
                    e,
                    attempt,
                )
                if not should_continue:
                    return None

                # Exponential backoff for retryable errors
                await self._apply_retry_delay(execution, context, attempt)

            except (ValueError, TypeError, OSError, RuntimeError) as e:
                # Catch unexpected errors
                logger.exception(
                    "execution_unexpected_error_during_context",
                    execution_id=execution.id,
                    context=context,
                    error=str(e),
                    action="get_order_status",
                    message="Unexpected error during order status check",
                )
                # Decide if unexpected errors are retryable (maybe not)
                return None

        logger.error(
            "execution_order_status_retries_exhausted",
            execution_id=execution.id,
            order_id=order_id,
            max_retries=self.max_retries,
            action="get_order_status",
            message="Failed to get order status after max retries",
        )
        return None

    async def _handle_order_status_api_error(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str,
        e: APIError,
        attempt: int,
    ) -> bool:
        """Handle API error during order status check.

        Returns:
            True if should continue retrying, False if should stop.
        """
        # Record failure with circuit breaker if configured
        if self.circuit_breaker_system:
            self.circuit_breaker_system.record_api_error(exchange_id, str(e.code))

        # Handle specific error codes
        return self._handle_specific_api_error_codes(execution, exchange_id, order_id, e, attempt)

    def _handle_specific_api_error_codes(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str,
        e: APIError,
        attempt: int,
    ) -> bool:
        """Handle specific API error codes during order status check."""
        if e.code == APIErrorCode.ORDER_NOT_FOUND.value:
            return self._handle_order_not_found_error(order_id, exchange_id, attempt, e)
        if e.code == APIErrorCode.RATE_LIMITED.value:
            return self._handle_rate_limited_error(order_id, exchange_id, e)
        if e.code == APIErrorCode.AUTHENTICATION_FAILED.value:
            return self._handle_authentication_failed_error(order_id, exchange_id, e)
        if e.code == APIErrorCode.INVALID_REQUEST.value:
            return self._handle_invalid_request_error(order_id, e)
        if e.code == APIErrorCode.SERVER_ERROR.value:
            return self._handle_server_error(order_id, exchange_id, e)
        if not e.is_retryable:
            return self._handle_non_retryable_error(order_id, e)
        return self._handle_other_retryable_error(order_id, e)

    def _handle_order_not_found_error(
        self,
        order_id: str,
        exchange_id: str,
        attempt: int,
        e: APIError,
    ) -> bool:
        """Handle ORDER_NOT_FOUND error."""
        # DEFENSIVE CHECK: Mypy=[comparison-overlap] Ruff=[none]
        logger.warning(
            "execution_order_not_found",
            order_id=order_id,
            exchange_id=exchange_id,
            error_message=e.message,
            action="handle_order_not_found",
            message="Order not found during status check, assuming cancelled or filled",
        )
        # Decide if this is terminal or retryable (might appear with delay)
        return attempt != self.max_retries - 1  # Stop if not found after retries

    def _handle_rate_limited_error(self, order_id: str, exchange_id: str, e: APIError) -> bool:
        """Handle RATE_LIMITED error."""
        # DEFENSIVE CHECK: Mypy=[comparison-overlap] Ruff=[none]
        logger.warning(
            "execution_rate_limited_order_status",
            order_id=order_id,
            exchange_id=exchange_id,
            error_message=e.message,
            action="handle_rate_limited",
            message="Rate limited getting order status, retrying",
        )
        # Retry delay handled by caller
        return True

    def _handle_authentication_failed_error(
        self,
        order_id: str,
        exchange_id: str,
        e: APIError,
    ) -> bool:
        """Handle AUTHENTICATION_FAILED error."""
        # DEFENSIVE CHECK: Mypy=[comparison-overlap] Ruff=[none]
        logger.error(
            "order_check_authentication_failed",
            order_id=order_id,
            action="check_order_status",
            error="authentication_failed",
            status="aborting",
            message=f"Authentication failed checking order {order_id}. Aborting.",
        )
        if self.circuit_breaker_system:
            # Use record_api_error method from CircuitBreakerSystem
            self.circuit_breaker_system.record_api_error(
                exchange_id,
                f"Authentication failed: {e.message}",
            )
        return False

    def _handle_invalid_request_error(self, order_id: str, e: APIError) -> bool:
        """Handle INVALID_REQUEST error."""
        # DEFENSIVE CHECK: Mypy=[comparison-overlap] Ruff=[none]
        logger.error(
            "execution_invalid_request_order_check",
            order_id=order_id,
            error_message=e.message,
            action="handle_invalid_request",
            status="aborting",
            message="Invalid request checking order, aborting",
        )
        return False

    def _handle_server_error(self, order_id: str, exchange_id: str, e: APIError) -> bool:
        """Handle SERVER_ERROR."""
        # DEFENSIVE CHECK: Mypy=[comparison-overlap] Ruff=[none]
        logger.warning(
            "execution_server_error_order_check",
            order_id=order_id,
            exchange_id=exchange_id,
            error_message=e.message,
            action="handle_server_error",
            message="Server error checking order, retrying",
        )
        # Decide if retryable based on specific exchange error message?
        return e.is_retryable  # Stop if error is explicitly not retryable

    def _handle_non_retryable_error(self, order_id: str, e: APIError) -> bool:
        """Handle non-retryable errors."""
        logger.warning(
            "execution_non_retryable_api_error",
            order_id=order_id,
            error_message=e.message,
            action="handle_non_retryable_error",
            message="Non-retryable API error getting order status",
        )
        return False  # Stop if error is explicitly not retryable

    def _handle_other_retryable_error(self, order_id: str, e: APIError) -> bool:
        """Handle other potentially retryable errors."""
        logger.warning(
            "execution_retryable_api_error",
            order_id=order_id,
            error_message=e.message,
            action="handle_retryable_error",
            message="Retryable API error getting order status",
        )
        return True  # Continue retrying

    async def _apply_retry_delay(
        self,
        execution: TradeExecution,
        context: str,
        attempt: int,
    ) -> None:
        """Apply exponential backoff delay for retries."""
        jitter = secrets.SystemRandom().uniform(-0.2, 0.2)
        delay = self.retry_delay_base * (2**attempt) * (1 + jitter)
        logger.info(
            "execution_retry_scheduled",
            execution_id=execution.id,
            context=context,
            delay_seconds=delay,
            action="schedule_retry",
            message=f"Execution {execution.id}: Retrying {context} in {delay:.2f}s...",
        )
        await asyncio.sleep(delay)

    async def _compensate_position(
        self,
        execution: TradeExecution,
        exchange_id: str,
        symbol: str,
        side: OrderSide,  # The side of the *compensating* order
        quantity: Decimal,
    ) -> bool:
        """Attempt to place a compensating order to flatten a position after a partial failure.

        Args:
            execution: The parent TradeExecution object.
            exchange_id: The exchange where compensation is needed.
            symbol: The exchange-specific symbol to compensate.
            side: The side of the compensating order (opposite of the failed leg).
            quantity: The quantity to compensate.

        Returns:
            True if compensation order placed successfully (or seemed filled), False otherwise.

        """
        logger.info(
            "execution_compensate_position_attempt",
            quantity=str(quantity),
            symbol=symbol,
            exchange_id=exchange_id,
            side=side.name,
            execution_id=execution.id,
            action="compensate_position",
            message="Attempting to compensate position",
        )
        # Configuration values from AppSettings
        compensation_config = self.app_settings.execution.compensation
        use_limit_orders_config = compensation_config.use_limit_orders
        limit_price_offset_pct = compensation_config.limit_price_offset_pct

        # Log the config values being used AFTER they are defined
        logger.info(
            "execution_compensation_config_limit_orders",
            use_limit_orders=use_limit_orders_config,
            config_type=str(type(use_limit_orders_config)),
            action="compensate_position",
            message="Compensation configuration for limit orders",
        )
        logger.info(
            "execution_compensation_config_price_offset",
            limit_price_offset_pct=str(limit_price_offset_pct),
            config_type=str(type(limit_price_offset_pct)),
            action="compensate_position",
            message="Compensation configuration for price offset",
        )

        order_type = OrderType.MARKET
        price = None

        if use_limit_orders_config:
            # This block determines if a LIMIT order should be used for compensation
            try:
                # Get ticker to calculate limit price
                ticker = await self.api_clients[exchange_id].get_ticker(symbol)
                if ticker:
                    if side == OrderSide.BUY and ticker.bid:
                        price = ticker.bid * (Decimal(1) + limit_price_offset_pct)
                        order_type = OrderType.LIMIT
                    elif side == OrderSide.SELL and ticker.ask:
                        price = ticker.ask * (Decimal(1) - limit_price_offset_pct)
                        order_type = OrderType.LIMIT
                    else:
                        logger.warning(
                            "execution_compensation_limit_price_missing_bid_ask",
                            execution_id=execution.id,
                            exchange_id=exchange_id,
                            symbol=symbol,
                            action="compensate_position",
                            message=(
                                "Could not determine limit price for compensation, missing bid/ask"
                            ),
                        )
                else:
                    logger.warning(
                        "execution_compensation_ticker_unavailable",
                        execution_id=execution.id,
                        exchange_id=exchange_id,
                        symbol=symbol,
                        action="compensate_position",
                        message="Could not get ticker to calculate compensation limit price",
                    )
            except (InvalidOperation, APIError, ValueError) as e:
                logger.warning(
                    "execution_compensation_limit_price_error",
                    execution_id=execution.id,
                    exchange_id=exchange_id,
                    symbol=symbol,
                    error=str(e),
                    fallback="MARKET",
                    action="compensate_position",
                    message="Error processing limit price for compensation, defaulting to MARKET",
                )
                order_type = OrderType.MARKET  # Fallback to MARKET on error
                price = None
        # If not using limit orders or if there was an issue, it remains MARKET with price=None

        compensation_order = await self._place_order_with_retry(
            execution,
            exchange_id,
            symbol,
            side,
            quantity,
            order_type,
            price,
            time_in_force=TimeInForce.GTC,
            reduce_only=True,  # CRITICAL for compensation
        )

        if compensation_order:
            logger.info(
                "execution_compensation_order_placed",
                execution_id=execution.id,
                order_id=compensation_order.exchange_order_id,
                action="compensate_position",
                message="Compensation order placed successfully",
            )
            # Basic check: If status is already filled, assume compensation worked
            # A more robust check would monitor the compensation order status
            if compensation_order.status == OrderStatus.FILLED:
                logger.info(
                    "execution_compensation_filled_immediately",
                    execution_id=execution.id,
                    action="place_compensation_order",
                    status="filled_immediately",
                    message=f"Execution {execution.id}: Compensation order filled immediately.",
                )
                return True
            # If not filled immediately, we assume it might fill. A better implementation
            # would monitor this order's status properly.
            logger.warning(
                "execution_compensation_order_not_filled",
                execution_id=execution.id,
                order_id=compensation_order.exchange_order_id,
                status=compensation_order.status.value
                if hasattr(compensation_order.status, "value")
                else str(compensation_order.status),
                action="compensate_position",
                message="Compensation order not immediately filled, monitoring needed",
            )
            # For now, optimistically return True if placed, but log warning
            return True
        logger.error(
            "execution_compensation_placement_failed",
            execution_id=execution.id,
            action="place_compensation_order",
            status="failed",
            message=f"Execution {execution.id}: Failed to place compensation order.",
        )
        return False

    def get_active_executions(self) -> list[TradeExecution]:
        """Get a list of currently active trade executions."""
        return list(self.active_executions.values())

    def reset_circuit_breaker(self, exchange_id: str) -> None:
        """Reset circuit breaker for a specific exchange."""
        if self.circuit_breaker_system:
            self.circuit_breaker_system.reset_breaker(exchange_id)
            logger.info(
                "circuit_breaker_reset",
                exchange_id=exchange_id,
                action="reset_circuit_breaker",
                message=f"Circuit breaker reset for {exchange_id}",
            )

    async def _verify_order_state(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str,
        expected_status: OrderStatus,
        symbol: str | None = None,
        client_order_id: str | None = None,
    ) -> bool:
        """Verify the final state of an order after execution attempt.

        Args:
            execution: The trade execution context.
            exchange_id: Exchange ID.
            order_id: Exchange Order ID.
            expected_status: The status the order should ideally be in.
            symbol: Optional symbol.
            client_order_id: Optional client order ID.

        Returns:
            True if the order is in the expected state, False otherwise.

        """
        logger.debug(
            "order_final_state_verification",
            order_id=order_id,
            exchange_id=exchange_id,
            action="verify_order_final_state",
            message=f"Verifying final state for order {order_id} on {exchange_id}...",
        )
        order = await self._get_order_status(
            execution,
            exchange_id,
            order_id,
            symbol=symbol,
            client_order_id=client_order_id,
        )

        if order:
            logger.debug(
                "order_status_verified",
                order_id=order_id,
                status=order.status.name if hasattr(order.status, "name") else str(order.status),
                action="verify_order_status",
                message=f"Verified Order {order_id}: Status={order.status}",
            )
            # Check status and potentially filled quantity based on expected status
            if expected_status == OrderStatus.FILLED:
                # For FILLED, check status and that filled quantity matches requested
                if (
                    order.status == OrderStatus.FILLED
                    and order.quantity_filled == order.quantity_requested
                ):
                    return True
                logger.warning(
                    "execution_order_state_mismatch_filled",
                    order_id=order_id,
                    expected_status="FILLED",
                    actual_status=order.status.value
                    if hasattr(order.status, "value")
                    else str(order.status),
                    quantity_filled=str(order.quantity_filled),
                    quantity_requested=str(order.quantity_requested),
                    action="verify_order_state",
                    message="Order state mismatch for FILLED status",
                )
                return False
            if expected_status in {OrderStatus.CANCELED, OrderStatus.REJECTED}:
                # For CANCELED/REJECTED, just check the status
                if order.status == expected_status:
                    return True
                logger.warning(
                    "execution_order_state_mismatch_canceled_rejected",
                    order_id=order_id,
                    expected_status=expected_status.name,
                    actual_status=order.status.name,
                    action="verify_order_state",
                    message="Order state mismatch for CANCELED/REJECTED status",
                )
                return False
            # For other statuses (e.g., NEW, PARTIALLY_FILLED), just check status matches
            if order.status == expected_status:
                return True
            logger.warning(
                "execution_order_state_mismatch_other",
                order_id=order_id,
                expected_status=expected_status.name,
                actual_status=order.status.name,
                action="verify_order_state",
                message="Order state mismatch for other status",
            )
            return False
        # Failed to get order status - verification fails
        logger.warning(
            "order_status_verification_failed",
            order_id=order_id,
            action="verify_order_status",
            issue="failed_to_get_status",
            message=f"Failed to get status for order {order_id} during verification.",
        )
        # If we expected CANCELLED/REJECTED and couldn't find it, maybe treat as success?
        if expected_status in {OrderStatus.CANCELED, OrderStatus.REJECTED}:
            logger.info(
                "execution_status_fetch_failed_success",
                order_id=order_id,
                expected_status=expected_status.name,
                action="verify_order_state",
                message=(
                    "Treating failed status fetch as verification success for CANCELED/REJECTED"
                ),
            )
            return True
        return False

    async def _update_pnl(self, execution: TradeExecution) -> None:
        """Calculate and update the realized PnL for a completed execution.

        This is a basic implementation assuming market orders and fills match requests.
        Needs refinement for limit orders, partial fills, and accurate fee data.
        """
        if execution.status != ExecutionStatus.COMPLETED:
            logger.warning(
                "execution_pnl_update_skipped_not_completed",
                execution_id=execution.id,
                action="update_execution_pnl",
                issue="execution_not_completed",
                message=f"Cannot update PnL for execution {execution.id}: Not completed.",
            )
            return

        if not execution.long_fill_price or not execution.short_fill_price:
            logger.warning(
                "execution_pnl_update_skipped_missing_prices",
                execution_id=execution.id,
                action="update_execution_pnl",
                issue="missing_fill_prices",
                message=f"Cannot update PnL for execution {execution.id}: Missing fill prices.",
            )
            return
        if not execution.long_fill_quantity or not execution.short_fill_quantity:
            logger.warning(
                "execution_pnl_update_missing_quantities",
                execution_id=execution.id,
                action="update_pnl",
                message="Cannot update PnL, missing fill quantities",
            )
            return

        # Simple PnL calculation (assumes quantities match, fees are zero)
        # TODO: Incorporate actual fees when available
        long_cost = execution.long_fill_price * execution.long_fill_quantity
        short_proceeds = execution.short_fill_price * execution.short_fill_quantity

        # Assuming long_fill_quantity and short_fill_quantity are the same base asset amount
        # This might not hold true if sizing is in quote currency and prices differ significantly
        execution.realized_pnl = short_proceeds - long_cost

        logger.info(
            "execution_pnl_calculated",
            execution_id=execution.id,
            realized_pnl=f"{execution.realized_pnl:.4f}",
            action="update_pnl",
            message="Calculated realized PnL for execution",
        )

        # Persist PnL or notify other systems if needed

    async def _monitor_order_status(
        self,
        execution: TradeExecution,
        exchange_id: str,
        order_id: str | None,
        is_long_leg: bool,
        timeout_sec: float = 60.0,  # Example timeout
        poll_interval_sec: float = 2.0,
    ) -> OrderStatus | None:
        """Monitor the status of a single order until it reaches a terminal state or times out.

        Basic polling implementation - should be replaced by WebSocket updates where possible.

        Args:
            execution: The trade execution context.
            exchange_id: Exchange ID.
            order_id: Exchange Order ID. Can be None if initial placement failed implicitly.
            is_long_leg: True if monitoring the long leg.
            timeout_sec: How long to monitor before giving up.
            poll_interval_sec: How often to poll for status.

        Returns:
            The final OrderStatus if terminal, or None if timed out or failed.

        """
        if order_id is None:
            logger.error(
                "execution_monitor_order_id_none",
                execution_id=execution.id,
                exchange_id=exchange_id,
                action="monitor_order_status",
                message="Cannot monitor order, order ID is None",
            )
            return None  # Or perhaps OrderStatus.REJECTED?

        start_time = time.monotonic()
        symbol = (
            self.symbol_mapper.get_exchange_symbol(
                execution.opportunity.opportunity.symbol,
                exchange_id,
            )
            if execution.opportunity
            else None
        )

        while time.monotonic() - start_time < timeout_sec:
            order = await self._get_order_status(execution, exchange_id, order_id, symbol=symbol)
            if order:
                logger.debug(
                    "execution_order_status_monitoring",
                    execution_id=execution.id,
                    order_id=order_id,
                    status=order.status.name
                    if hasattr(order.status, "name")
                    else str(order.status),
                    action="monitor_order_status",
                    message=f"Execution {execution.id}: Order {order_id} Status: {order.status}",
                )
                if order.status in {
                    OrderStatus.FILLED,
                    OrderStatus.CANCELED,
                    OrderStatus.REJECTED,
                    OrderStatus.EXPIRED,
                }:
                    logger.info(
                        "execution_order_terminal_state",
                        execution_id=execution.id,
                        order_id=order_id,
                        status=order.status.value
                        if hasattr(order.status, "value")
                        else str(order.status),
                        action="monitor_order_status",
                        message="Order reached terminal state",
                    )
                    # Process the final state (e.g., record fill)
                    await self._handle_filled_order(
                        execution,
                        order,
                        exchange_id,
                        is_long_leg,
                        # Pass expected prices if needed for slippage check here
                    )
                    return order.status
                # Handle PARTIALLY_FILLED if needed - might require partial compensation logic
                if order.status == OrderStatus.PARTIALLY_FILLED:
                    logger.info(
                        "execution_order_partially_filled",
                        execution_id=execution.id,
                        order_id=order_id,
                        quantity_filled=str(order.quantity_filled),
                        quantity_requested=str(order.quantity_requested),
                        action="monitor_order_status",
                        message="Order is partially filled",
                    )
                    # TODO: Implement logic for partial fills if required by strategy
                    # For now, continue monitoring
            else:
                # _get_order_status failed after retries
                logger.error(
                    "execution_monitor_status_failed",
                    execution_id=execution.id,
                    order_id=order_id,
                    action="monitor_order_status",
                    message="Failed to get order status, assuming failure",
                )
                return None  # Indicate monitoring failure

            await asyncio.sleep(poll_interval_sec)

        logger.warning(
            "execution_order_monitoring_timeout",
            execution_id=execution.id,
            order_id=order_id,
            action="monitor_order_fills",
            issue="timeout",
            message=f"Execution {execution.id}: Timed out monitoring order {order_id}.",
        )
        return None  # Indicate timeout

    def _add_to_history(self, execution: TradeExecution) -> None:
        """Add a completed or failed execution to the history, maintaining max size.

        Removes the execution from the active dictionary.
        """
        if execution.id in self.active_executions:
            del self.active_executions[execution.id]

        self.executions.append(execution)
        if len(self.executions) > self.max_execution_history:
            self.executions.pop(0)  # Remove the oldest entry
        logger.debug(
            "execution_added_to_history",
            execution_id=execution.id,
            history_size=len(self.executions),
            action="add_to_history",
            message="Execution added to history",
        )

    async def _handle_api_error(
        self,
        e: APIError,
        exchange_id: str,
        context: str,
        is_retryable: bool = True,
    ) -> bool:
        """Centralized handling of API errors, including circuit breaker recording.

        Args:
            e: The APIError exception.
            exchange_id: The exchange where the error occurred.
            context: Description of the operation (e.g., "placing order").
            is_retryable: Hint whether the operation itself is generally retryable.

        Returns:
            bool: True if the operation should be retried based on the error, False otherwise.

        """
        logger.warning(
            "execution_api_error",
            exchange_id=exchange_id,
            context=context,
            error_code=e.code,
            error_message=e.message,
            http_status=e.http_status,
            exchange_code=e.exchange_code,
            exchange_message=e.exchange_message,
            action="handle_api_error",
            message="API error during operation",
        )

        # Record failure with circuit breaker if configured
        if self.circuit_breaker_system:
            self.circuit_breaker_system.record_api_error(exchange_id, str(e.code))

        # Determine retry based on error type and context
        return is_retryable and e.is_retryable

    def _create_place_order_args(
        self,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        time_in_force: TimeInForce,
        price: Decimal | None,
        client_order_id: str,
        reduce_only: bool,
        post_only: bool,
    ) -> PlaceOrderArgs:
        """Create PlaceOrderArgs object for the API call."""
        return PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=order_type,
            quantity=quantity,
            time_in_force=time_in_force,
            price=price,
            client_order_id=client_order_id,
            reduce_only=reduce_only,
            post_only=post_only,
        )

    def _log_order_placement_attempt(
        self,
        execution_id: str,
        context: str,
        attempt: int,
        side: OrderSide,
        quantity: Decimal,
        symbol: str,
        exchange_id: str,
        client_order_id: str,
    ) -> None:
        """Log order placement attempt."""
        logger.info(
            "execution_order_placement_attempt",
            execution_id=execution_id,
            context=context,
            attempt=attempt,
            side=side.name,
            quantity=f"{quantity:.8f}",
            symbol=symbol,
            exchange_id=exchange_id,
            client_order_id=client_order_id,
            action="place_order_with_retry",
            message="Attempting order placement",
        )

    def _handle_order_placement_success(
        self,
        execution_id: str,
        exchange_id: str,
        order_result: Order,
        context: str,
    ) -> None:
        """Handle successful order placement."""
        logger.info(
            "execution_order_placed_successfully",
            execution_id=execution_id,
            exchange_id=exchange_id,
            exchange_order_id=order_result.exchange_order_id,
            status=order_result.status.value
            if hasattr(order_result.status, "value")
            else str(order_result.status),
            action="place_order_with_retry",
            message="Order placed successfully",
        )
        # Record success with circuit breaker if configured
        if self.circuit_breaker_system:
            self.circuit_breaker_system.record_api_success(
                exchange_id,
                context=f"Order {order_result.exchange_order_id} placed",
            )

    async def _place_order_with_retry(
        self,
        execution: TradeExecution,
        exchange_id: str,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        order_type: OrderType,
        price: Decimal | None = None,
        time_in_force: TimeInForce = TimeInForce.GTC,
        reduce_only: bool = False,
        post_only: bool = False,
        is_long_leg: bool = True,
    ) -> Order | None:
        """Place an order with retry logic for transient errors.

        Args:
            execution: The parent TradeExecution object.
            exchange_id: Target exchange.
            symbol: Exchange-specific symbol.
            side: BUY or SELL.
            quantity: Order quantity.
            order_type: MARKET, LIMIT, etc.
            price: Limit price (for LIMIT orders).
            time_in_force: Order time in force.
            reduce_only: True if this is a reduce_only order
            post_only: True if this is a post-only order
            is_long_leg: True if this is the long leg of the arbitrage.

        Returns:
            Order object if successful, None otherwise.

        """
        client = self.api_clients.get(exchange_id)
        if not client:
            execution.error_message = f"No API client for {exchange_id}"
            logger.error(
                "execution_error",
                execution_id=execution.id,
                error_message=execution.error_message,
                action="handle_execution_error",
                message=f"Execution {execution.id}: {execution.error_message}",
            )
            return None

        context = "placing compensation order" if reduce_only else "placing order"
        client_order_id = f"cde_{execution.id[:8]}_{exchange_id[:3]}_{str(uuid.uuid4())[:8]}"

        last_api_error_for_reraise: APIError | None = None

        for attempt in range(self.max_retries):
            try:
                self._log_order_placement_attempt(
                    execution.id,
                    context,
                    attempt + 1,
                    side,
                    quantity,
                    symbol,
                    exchange_id,
                    client_order_id,
                )

                # Create PlaceOrderArgs object for the API call
                place_order_args = self._create_place_order_args(
                    symbol,
                    side,
                    order_type,
                    quantity,
                    time_in_force,
                    price,
                    client_order_id,
                    reduce_only,
                    post_only,
                )

                order_result = await client.place_order(place_order_args)
                self._handle_order_placement_success(
                    execution.id, exchange_id, order_result, context
                )
            except APIError as e:
                last_api_error_for_reraise = e  # Store the API error
                should_retry = await self._handle_api_error(e, exchange_id, context)
                if not should_retry:
                    execution.error_message = (
                        f"Non-retryable API error during {context} on {exchange_id}: {e.message}"
                    )
                    logger.exception(
                        "execution_error",
                        execution_id=execution.id,
                        error_message=execution.error_message,
                        action="handle_execution_error",
                        message=f"Execution {execution.id}: {execution.error_message}",
                    )
                    raise  # Re-raise current exception (e)
                # Exponential backoff
                jitter = secrets.SystemRandom().uniform(-0.2, 0.2)
                delay = self.retry_delay_base * (2**attempt) * (1 + jitter)
                logger.info(
                    "execution_retry_scheduled_after_api_error",
                    execution_id=execution.id,
                    context=context,
                    delay_seconds=delay,
                    attempt=attempt,
                    action="schedule_retry_after_api_error",
                    message=f"Execution {execution.id}: Retrying {context} in {delay:.2f}s...",
                )
                await asyncio.sleep(delay)
            except (ValueError, TypeError, OSError, RuntimeError) as e:
                # Catch unexpected errors
                execution.error_message = (
                    f"Unexpected error during {context} on {exchange_id}: {e!s}"
                )
                logger.exception(
                    "execution_unexpected_error",
                    execution_id=execution.id,
                    error_message=execution.error_message,
                    action="place_order_with_retry",
                    message="Unexpected error during order placement",
                )
                # Decide if unexpected errors are retryable (maybe not)
                return None
            else:
                # Success - return the order result
                return order_result

        # If loop finishes, it means all retries were exhausted for an APIError,
        # or another exception occurred.
        # The error_message should already be set by the last attempt or the
        # unexpected exception block.
        # If the reason for exhausting retries was specifically an APIError,
        # re-raise it as per test expectation.
        if last_api_error_for_reraise:
            # Ensure error_message reflects this final attempt if not already set by
            # non-retryable path
            if not execution.error_message or "Non-retryable" not in execution.error_message:
                execution.error_message = (
                    f"Failed to place order on {exchange_id} after "
                    f"{self.max_retries} retries due to: {last_api_error_for_reraise.message}"
                )
            logger.error(
                "execution_retries_exhausted",
                execution_id=execution.id,
                last_api_error=str(last_api_error_for_reraise),
                action="place_order_with_retry",
                message="Exhausted retries for order placement",
            )
            raise last_api_error_for_reraise
        # This path should ideally not be hit if an APIError occurred and was stored.
        # If it's another exception, it would have been raised or returned None from the loop.
        # If it's just max_retries without a specific APIError stored
        # (e.g. unexpected error returned None),
        # set a generic message if not already set.
        if not execution.error_message:
            execution.error_message = (
                f"Failed to place order on {exchange_id} after "
                f"{self.max_retries} retries (unknown reason)."
            )
        logger.error(
            "execution_error",
            execution_id=execution.id,
            error_message=execution.error_message,
            action="handle_execution_error",
            message=f"Execution {execution.id}: {execution.error_message}",
        )

        return None  # Fallback, though raising last_api_error_for_reraise is preferred if it exists
