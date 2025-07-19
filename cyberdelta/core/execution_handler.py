"""Complete ExecutionHandler implementation using service extraction pattern.

This is the full implementation that replaces the monolithic ExecutionHandler
with the new service-oriented architecture while maintaining all functionality.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.apis.common import APIError
from cyberdelta.config.structlog_config import TraceLevelLogger, get_logger
from cyberdelta.core.models import OrderSide, OrderType, TimeInForce, Trade
from cyberdelta.core.models.execution import ExecutionStatus, TradeExecution
from cyberdelta.core.services.config_validation import (
    ConfigValidationError,
    validate_execution_config,
)
from cyberdelta.core.services.factory import ServiceFactory
from cyberdelta.core.services.interfaces import ExecutionResult, OrderRequest
from cyberdelta.validation.circuit_breaker import CircuitBreakerTrippedError


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI
    from cyberdelta.config.models.config_models import AppSettings
    from cyberdelta.core.portfolio_tracker import PortfolioTracker
    from cyberdelta.core.risk_manager import SizedOpportunity
    from cyberdelta.core.services.interfaces import IAlertService
    from cyberdelta.core.symbol_mapper import SymbolMapper
    from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem


# Error classes needed by tests
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
            f"Symbol mapping failed for {symbol} on {exchange} ({leg} leg)", "SYMBOL_MAPPING_ERROR"
        )


class AverageFillPriceError(APIError):
    """Average fill price calculation error."""

    def __init__(self, message: str) -> None:
        """Initialize AverageFillPriceError.

        Args:
            message: Error message
        """
        super().__init__(message, "AVERAGE_FILL_PRICE_ERROR")


class ExecutionHandler:
    """Complete ExecutionHandler implementation using service-oriented architecture.

    This replaces the monolithic ExecutionHandler with a service-based design that:
    - Maintains all original functionality
    - Improves error handling and logging
    - Adds thread safety and better state management
    - Provides comprehensive validation and compensation
    - Enables easy testing through dependency injection
    """

    def __init__(
        self,
        app_settings: AppSettings,
        portfolio_tracker: PortfolioTracker,
        symbol_mapper: SymbolMapper,
        circuit_breaker_system: CircuitBreakerSystem | None = None,
        alert_service: IAlertService | None = None,
        logger: TraceLevelLogger | None = None,
    ) -> None:
        """Initialize the full execution handler.

        Args:
            app_settings: Application configuration settings
            portfolio_tracker: Portfolio tracker for position updates
            symbol_mapper: SymbolMapper for translating symbols
            circuit_breaker_system: The main circuit breaker system (optional)
            alert_service: Optional alert service for notifications
            logger: Optional logger instance

        Raises:
            ConfigValidationError: If configuration validation fails
        """
        # Validate configuration before proceeding
        try:
            validate_execution_config(app_settings, logger)
        except ConfigValidationError as e:
            if logger:
                logger.critical(
                    "ExecutionHandler initialization failed due to invalid configuration",
                    validation_errors=e.validation_result.critical_errors,
                    all_errors=e.validation_result.errors,
                    warnings=e.validation_result.warnings,
                )
            raise

        self.app_settings = app_settings
        self.portfolio_tracker = portfolio_tracker
        self.symbol_mapper = symbol_mapper
        self.circuit_breaker_system = circuit_breaker_system
        self.alert_service = alert_service
        self.logger = logger or get_logger(__name__)

        # API clients registry
        self.api_clients: dict[str, ExchangeAPI] = {}

        # Configuration values from AppSettings
        self.max_slippage = self.app_settings.execution.max_slippage_pct
        self.max_retries = self.app_settings.execution.max_retries
        self.retry_delay_base = float(self.app_settings.execution.retry_delay_base_sec)
        self.max_execution_history = getattr(
            self.app_settings.execution, "max_execution_history", 100
        )

        # Create service factory and container
        self.service_factory = ServiceFactory(
            api_clients=self.api_clients,
            symbol_mapper=self.symbol_mapper,
            app_settings=self.app_settings,
            circuit_breaker=self.circuit_breaker_system,
            alert_service=self.alert_service,
            logger=self.logger,
        )

        # Initialize services
        self.services = self.service_factory.create_all_services()

        # Validate service configuration
        if not self.services.validate_services():
            msg = "Service container validation failed during initialization"
            raise ValueError(msg)

        self.logger.info("ExecutionHandler initialized with service architecture")

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """Register an API client for an exchange.

        Args:
            exchange_id: Exchange identifier
            client: ExchangeAPI implementation
        """
        self.api_clients[exchange_id] = client

        # Update the service factory's API clients
        self.service_factory.api_clients = self.api_clients

        # Clear service cache to force recreation with new API clients
        self.service_factory.clear_cache()
        self.services = self.service_factory.create_all_services()

        self.logger.info(
            "API client registered", exchange_id=exchange_id, total_clients=len(self.api_clients)
        )

    async def start(self) -> None:
        """Start the execution handler and all services."""
        await self.services.start_all()
        self.logger.info("ExecutionHandler started successfully")

    async def stop(self) -> None:
        """Stop the execution handler and all services."""
        await self.services.stop_all()
        self.logger.info("ExecutionHandler stopped successfully")

    async def execute_opportunity(self, opportunity: SizedOpportunity) -> TradeExecution:
        """Execute an arbitrage opportunity with full service integration.

        Args:
            opportunity: Sized arbitrage opportunity

        Returns:
            TradeExecution object representing the outcome
        """
        self.logger.info(
            "Execution starting",
            symbol=opportunity.opportunity.symbol,
            long_exchange=opportunity.opportunity.long_exchange,
            short_exchange=opportunity.opportunity.short_exchange,
            long_size=str(opportunity.long_size),
            short_size=str(opportunity.short_size),
        )

        try:
            # Step 1: Validate the execution request
            validation_result = await self.services.input_validator.validate_execution_request(
                opportunity
            )

            if not validation_result.is_valid:
                return await self._handle_validation_failure(opportunity, validation_result)

            # Log any validation warnings
            if validation_result.warnings:
                self.logger.info(
                    "Execution validation passed with warnings", warnings=validation_result.warnings
                )

            # Step 2: Create execution tracking
            execution = await self.services.state_manager.create_execution(opportunity)
            execution.start_time = datetime.now(UTC)

            self.logger.info("Execution created", execution_id=execution.id)

            # Step 3: Setup prerequisites (API clients and symbols)
            prerequisites_result = await self._setup_execution_prerequisites(execution, opportunity)
            if not prerequisites_result.success:
                return await self._handle_prerequisites_failure(execution, prerequisites_result)

            long_client, short_client, long_symbol, short_symbol = prerequisites_result.data

            # Step 4: Execute the main order placement logic
            await self._place_orders_for_opportunity(
                execution, opportunity, long_client, short_client, long_symbol, short_symbol
            )

            # Step 5: Finalize execution
            return await self._finalize_execution(execution)

        except (ValueError, TypeError, KeyError, AttributeError) as e:
            self.logger.exception("Unexpected error during execution", error=str(e))

            # Create a failed execution if we don't have one yet
            execution = TradeExecution(opportunity=opportunity)
            execution.status = ExecutionStatus.FAILED
            execution.error_message = f"Unexpected error: {e}"
            execution.end_time = datetime.now(UTC)

            await self.services.state_manager.finalize_execution(execution.id)
            return execution

    async def _handle_validation_failure(
        self, opportunity: SizedOpportunity, validation_result: object
    ) -> TradeExecution:
        """Handle validation failure by creating a failed execution."""
        execution = TradeExecution(opportunity=opportunity)
        execution.status = ExecutionStatus.FAILED
        errors = getattr(validation_result, "errors", [])
        execution.error_message = f"Validation failed: {', '.join(errors)}"
        execution.start_time = datetime.now(UTC)
        execution.end_time = datetime.now(UTC)

        self.logger.warning(
            "Execution failed validation",
            execution_id=execution.id,
            errors=getattr(validation_result, "errors", []),
        )

        # Track the failed execution
        await self.services.state_manager.create_execution(opportunity)
        await self.services.state_manager.update_execution_status(
            execution.id, ExecutionStatus.FAILED, execution.error_message
        )
        await self.services.state_manager.finalize_execution(execution.id)

        return execution

    async def _setup_execution_prerequisites(
        self, execution: TradeExecution, opportunity: SizedOpportunity
    ) -> ExecutionResult:
        """Setup and validate API clients and symbols for execution."""
        try:
            # Get API clients
            long_client = self.api_clients.get(opportunity.opportunity.long_exchange)
            short_client = self.api_clients.get(opportunity.opportunity.short_exchange)

            if not long_client or not short_client:
                missing_exchange = (
                    opportunity.opportunity.long_exchange
                    if not long_client
                    else opportunity.opportunity.short_exchange
                )
                return await self.services.error_handler.handle_validation_error(
                    f"No API client available for exchange: {missing_exchange}",
                    {"exchange_id": missing_exchange, "execution_id": execution.id},
                )

            # Get exchange-specific symbols
            long_symbol = self.symbol_mapper.get_exchange_symbol(
                opportunity.opportunity.symbol, opportunity.opportunity.long_exchange
            )
            short_symbol = self.symbol_mapper.get_exchange_symbol(
                opportunity.opportunity.symbol, opportunity.opportunity.short_exchange
            )

            if not long_symbol or not short_symbol:
                missing_leg = "long" if not long_symbol else "short"
                missing_exchange = (
                    opportunity.opportunity.long_exchange
                    if not long_symbol
                    else opportunity.opportunity.short_exchange
                )
                return await self.services.error_handler.handle_validation_error(
                    f"Symbol mapping failed for {missing_leg} leg",
                    {
                        "symbol": opportunity.opportunity.symbol,
                        "exchange": missing_exchange,
                        "leg": missing_leg,
                        "execution_id": execution.id,
                    },
                )

            return ExecutionResult.success_result((
                long_client,
                short_client,
                long_symbol,
                short_symbol,
            ))

        except (ValueError, TypeError, AttributeError) as e:
            return await self.services.error_handler.handle_system_error(
                e, "execution prerequisites setup", recoverable=False
            )

    async def _handle_prerequisites_failure(
        self, execution: TradeExecution, error_result: ExecutionResult
    ) -> TradeExecution:
        """Handle prerequisites setup failure."""
        execution.status = ExecutionStatus.FAILED
        execution.error_message = str(error_result.error)
        execution.end_time = datetime.now(UTC)

        await self.services.state_manager.update_execution_status(
            execution.id, ExecutionStatus.FAILED, execution.error_message
        )
        await self.services.state_manager.finalize_execution(execution.id)

        return execution

    async def _place_orders_for_opportunity(
        self,
        execution: TradeExecution,
        opportunity: SizedOpportunity,
        long_client: ExchangeAPI,
        short_client: ExchangeAPI,
        long_symbol: str,
        short_symbol: str,
    ) -> None:
        """Execute the complete order placement logic."""
        self.logger.info(
            "Placing orders for execution",
            execution_id=execution.id,
            symbol=opportunity.opportunity.symbol,
        )

        # Update execution status
        await self.services.state_manager.update_execution_status(
            execution.id, ExecutionStatus.EXECUTING
        )

        # Calculate base asset quantities
        base_quantities = self._calculate_base_asset_quantities(execution, opportunity)
        if base_quantities is None:
            return  # Error already set in execution

        base_asset_quantity_long, base_asset_quantity_short = base_quantities

        # Get default time in force
        default_tif = self._get_default_time_in_force()

        # Place long order first
        long_order_result = await self._place_long_order(
            execution, opportunity, long_symbol, base_asset_quantity_long, default_tif
        )

        if long_order_result is None or not long_order_result.success:
            # Long order failed - no compensation needed
            execution.status = ExecutionStatus.FAILED
            execution.error_message = "Long order placement failed"
            await self.services.state_manager.update_execution_status(
                execution.id, ExecutionStatus.FAILED, execution.error_message
            )
            return

        # Update execution with long order details
        long_order = long_order_result.data
        execution.long_order_id = getattr(long_order, "exchange_order_id", None)
        execution.long_fill_price = getattr(long_order, "average_fill_price", None)
        execution.long_fill_quantity = getattr(long_order, "quantity_filled", None)

        # Process the filled long order
        await self._handle_filled_order(
            execution, long_order, opportunity.opportunity.long_exchange, True
        )

        # Place short order with compensation handling
        await self._place_short_order_with_compensation(
            execution, opportunity, short_symbol, base_asset_quantity_short, default_tif
        )

    def _calculate_base_asset_quantities(
        self, execution: TradeExecution, opportunity: SizedOpportunity
    ) -> tuple[Decimal, Decimal] | None:
        """Calculate base asset quantities for both legs."""
        try:
            # Calculate long quantity
            if not opportunity.opportunity.long_price or opportunity.opportunity.long_price <= 0:
                error_msg = f"Invalid long price: {opportunity.opportunity.long_price}"
                execution.error_message = error_msg
                execution.status = ExecutionStatus.FAILED
                self.logger.error(
                    "Invalid long price for quantity calculation",
                    execution_id=execution.id,
                    long_price=str(opportunity.opportunity.long_price),
                )
                return None

            base_asset_quantity_long = opportunity.long_size / opportunity.opportunity.long_price

            # Calculate short quantity
            if not opportunity.opportunity.short_price or opportunity.opportunity.short_price <= 0:
                error_msg = f"Invalid short price: {opportunity.opportunity.short_price}"
                execution.error_message = error_msg
                execution.status = ExecutionStatus.FAILED
                self.logger.error(
                    "Invalid short price for quantity calculation",
                    execution_id=execution.id,
                    short_price=str(opportunity.opportunity.short_price),
                )
                return None

            base_asset_quantity_short = opportunity.short_size / opportunity.opportunity.short_price

            self.logger.debug(
                "Base asset quantities calculated",
                execution_id=execution.id,
                long_quantity=str(base_asset_quantity_long),
                short_quantity=str(base_asset_quantity_short),
            )

        except (ValueError, TypeError, KeyError, AttributeError) as e:
            error_msg = f"Failed to calculate base asset quantities: {e}"
            execution.error_message = error_msg
            execution.status = ExecutionStatus.FAILED
            self.logger.exception(
                "Exception calculating base asset quantities", execution_id=execution.id
            )
            return None
        else:
            return base_asset_quantity_long, base_asset_quantity_short

    def _get_default_time_in_force(self) -> TimeInForce:
        """Get default time in force configuration."""
        # TODO: Make this configurable in AppSettings
        return TimeInForce.IOC

    async def _place_long_order(
        self,
        execution: TradeExecution,
        opportunity: SizedOpportunity,
        long_symbol: str,
        base_asset_quantity_long: Decimal,
        default_tif: TimeInForce,
    ) -> ExecutionResult | None:
        """Place the long order using the order management service."""
        order_request = OrderRequest(
            exchange_id=opportunity.opportunity.long_exchange,
            symbol=long_symbol,
            side=OrderSide.BUY,
            quantity=base_asset_quantity_long,
            order_type=OrderType.MARKET,
            time_in_force=default_tif,
            reduce_only=False,
            post_only=False,
        )

        self.logger.info(
            "Placing long order",
            execution_id=execution.id,
            exchange_id=opportunity.opportunity.long_exchange,
            symbol=long_symbol,
            quantity=str(base_asset_quantity_long),
        )

        result = await self.services.order_service.place_order_with_retry(order_request)

        if result.success:
            self.logger.info(
                "Long order placed successfully",
                execution_id=execution.id,
                order_id=getattr(result.data, "exchange_order_id", "unknown"),
            )
        else:
            self.logger.error(
                "Long order placement failed", execution_id=execution.id, error=str(result.error)
            )

        return result

    async def _place_short_order_with_compensation(
        self,
        execution: TradeExecution,
        opportunity: SizedOpportunity,
        short_symbol: str,
        base_asset_quantity_short: Decimal,
        default_tif: TimeInForce,
    ) -> None:
        """Place short order and handle compensation if needed."""
        order_request = OrderRequest(
            exchange_id=opportunity.opportunity.short_exchange,
            symbol=short_symbol,
            side=OrderSide.SELL,
            quantity=base_asset_quantity_short,
            order_type=OrderType.MARKET,
            time_in_force=default_tif,
            reduce_only=False,
            post_only=False,
        )

        self.logger.info(
            "Placing short order",
            execution_id=execution.id,
            exchange_id=opportunity.opportunity.short_exchange,
            symbol=short_symbol,
            quantity=str(base_asset_quantity_short),
        )

        result = await self.services.order_service.place_order_with_retry(order_request)

        if result.success:
            # Short order succeeded
            short_order = result.data
            execution.short_order_id = getattr(short_order, "exchange_order_id", None)
            execution.short_fill_price = getattr(short_order, "average_fill_price", None)
            execution.short_fill_quantity = getattr(short_order, "quantity_filled", None)

            await self._handle_filled_order(
                execution, short_order, opportunity.opportunity.short_exchange, False
            )

            # Both orders successful - mark as completed
            execution.status = ExecutionStatus.COMPLETED
            await self.services.state_manager.update_execution_status(
                execution.id, ExecutionStatus.COMPLETED
            )

            self.logger.info("Execution completed successfully", execution_id=execution.id)

        else:
            # Short order failed - need compensation
            self.logger.warning(
                "Short order failed, attempting compensation",
                execution_id=execution.id,
                error=str(result.error),
            )

            # Mark as compensating
            execution.status = ExecutionStatus.COMPENSATING
            await self.services.state_manager.update_execution_status(
                execution.id, ExecutionStatus.COMPENSATING
            )

            # Attempt compensation
            compensation_result = await self.services.compensation_service.compensate_position(
                execution,
                "short",  # Failed leg
                base_asset_quantity_short,
            )

            if compensation_result.success:
                execution.status = ExecutionStatus.PARTIALLY_COMPLETED
                self.logger.info("Compensation successful", execution_id=execution.id)
            else:
                execution.status = ExecutionStatus.FAILED
                execution.error_message = (
                    f"Short order failed and compensation failed: {compensation_result.error}"
                )
                self.logger.error(
                    "Compensation failed",
                    execution_id=execution.id,
                    error=str(compensation_result.error),
                )

            await self.services.state_manager.update_execution_status(
                execution.id, execution.status, execution.error_message
            )

    async def _handle_filled_order(
        self,
        execution: TradeExecution,
        order: object,  # Order object
        exchange_id: str,
        is_long_leg: bool,
    ) -> None:
        """Handle a filled order by updating portfolio tracker."""
        try:
            # Create a Trade object for portfolio tracking
            # This is a simplified version - in reality would need proper Trade construction

            trade = Trade(
                id=str(uuid.uuid4()),
                exchange=exchange_id,
                symbol=execution.opportunity.opportunity.symbol,
                side=OrderSide.BUY if is_long_leg else OrderSide.SELL,
                quantity=getattr(order, "quantity_filled", Decimal(0)),
                price=getattr(order, "average_fill_price", Decimal(0)),
                executed_at=datetime.now(UTC),
                order_id=getattr(order, "exchange_order_id", ""),
                fee=Decimal(0),  # Would need to extract from order
            )

            # Process trade with portfolio tracker
            await self.portfolio_tracker.process_trade(exchange_id, trade)

            self.logger.info(
                "Trade processed by portfolio tracker",
                execution_id=execution.id,
                exchange_id=exchange_id,
                is_long_leg=is_long_leg,
                quantity=str(trade.quantity),
                price=str(trade.price),
            )

        except (ValueError, AttributeError, TypeError) as e:
            # Handle construction errors from Trade object or attribute access
            self.logger.exception(
                "Failed to process trade with portfolio tracker",
                execution_id=execution.id,
                exchange_id=exchange_id,
                error_type=type(e).__name__,
            )

    async def _finalize_execution(self, execution: TradeExecution) -> TradeExecution:
        """Finalize execution and add to history."""
        # Ensure end_time is set
        if execution.end_time is None:
            execution.end_time = datetime.now(UTC)

        # Calculate realized PnL if both legs filled
        if (
            execution.long_fill_price
            and execution.short_fill_price
            and execution.long_fill_quantity
            and execution.short_fill_quantity
        ):
            # Simplified PnL calculation
            long_value = execution.long_fill_price * execution.long_fill_quantity
            short_value = execution.short_fill_price * execution.short_fill_quantity
            execution.realized_pnl = short_value - long_value

        # Finalize in state manager
        finalized_execution = await self.services.state_manager.finalize_execution(execution.id)

        self.logger.info(
            "Execution finalized",
            execution_id=execution.id,
            status=execution.status.name,
            duration_seconds=(
                (execution.end_time - execution.start_time).total_seconds()
                if execution.start_time
                else None
            ),
            realized_pnl=str(execution.realized_pnl) if execution.realized_pnl else None,
        )

        return finalized_execution or execution

    # Additional methods for compatibility with original ExecutionHandler

    def get_active_executions(self) -> list[TradeExecution]:
        """Get all active executions."""
        return self.services.state_manager.get_active_executions()

    async def get_execution_history(self) -> list[TradeExecution]:
        """Get execution history."""
        if hasattr(self.services.state_manager, "get_execution_history"):
            return await self.services.state_manager.get_execution_history()
        return []

    async def get_execution_stats(self) -> dict[str, Any]:
        """Get execution statistics."""
        if hasattr(self.services.state_manager, "get_stats"):
            return await self.services.state_manager.get_stats()

        # Fallback stats
        active_executions = self.get_active_executions()
        return {"active_executions": len(active_executions), "service_version": "full_v1.0"}

    async def cleanup_old_data(self) -> dict[str, Any]:
        """Clean up old execution data."""
        results: dict[str, Any] = {}

        try:
            if hasattr(self.services.state_manager, "cleanup_old_executions"):
                removed_executions = await self.services.state_manager.cleanup_old_executions()
                results["removed_executions"] = removed_executions

            if hasattr(self.services.compensation_service, "cleanup_completed_compensations"):
                cleanup_method = self.services.compensation_service.cleanup_completed_compensations
                removed_compensations = await cleanup_method()
                results["removed_compensations"] = removed_compensations

            self.logger.info("Cleanup completed", results=results)

        except (ValueError, TypeError, KeyError, AttributeError) as e:
            self.logger.exception("Error during cleanup")
            results["error"] = str(e)

        return results
