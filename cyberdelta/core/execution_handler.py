from __future__ import annotations  # Enable postponed evaluation

import asyncio
import random
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from cyberdelta.apis.base import APIError, APIErrorCode, ExchangeAPI
from cyberdelta.core.models import (
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
    Ticker,
    TimeInForce,
    Trade,
)
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.utils.config import Config
from cyberdelta.utils.logging_config import get_logger
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem, CircuitBreakerTrippedError

# Keep imports for type checking only if they cause circular dependencies otherwise
if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


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
    """
    Represents a trade execution across multiple exchanges.
    """
    def __init__(self, opportunity: SizedOpportunity) -> None:
        """
        Initialize a trade execution.

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

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        # Ensure Decimal values are converted to strings for JSON compatibility if needed
        # This basic implementation assumes direct serialization is okay for now
        return {
            "opportunity": {
                "symbol": self.opportunity.opportunity.symbol,
                "long_exchange": self.opportunity.opportunity.long_exchange,
                "short_exchange": self.opportunity.opportunity.short_exchange,
                "long_size": str(self.opportunity.long_size), # Example: Convert Decimal to str
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
            "short_fill_quantity": str(self.short_fill_quantity) if self.short_fill_quantity else None,
        }

    def __str__(self) -> str:
        """String representation of the execution."""
        return (
            f"TradeExecution: {self.opportunity.opportunity.symbol} - "
            f"Long: {self.opportunity.opportunity.long_exchange} "
            f"${self.opportunity.long_size:.2f}, "
            f"Short: {self.opportunity.opportunity.short_exchange} "
            f"${self.opportunity.short_size:.2f}, "
            f"Status: {self.status.name}"
        )


class ExecutionHandler:
    """
    Execute trades on exchanges reliably.

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
        config: Config,
        portfolio_tracker: PortfolioTracker,
        symbol_mapper: SymbolMapper,
        circuit_breaker_system: CircuitBreakerSystem | None = None,
    ) -> None:
        """
        Initialize the execution handler.

        Args:
            config: Application configuration (Config object)
            portfolio_tracker: Portfolio tracker for position updates
            symbol_mapper: SymbolMapper for translating symbols
            circuit_breaker_system: The main circuit breaker system (optional)
        """
        self.config = config
        self.portfolio_tracker = portfolio_tracker
        self.symbol_mapper = symbol_mapper
        self.circuit_breaker_system = circuit_breaker_system
        self.api_clients: dict[str, ExchangeAPI] = {}
        self.max_slippage: Decimal = Decimal(
            str(config.get("execution.max_slippage", "0.002"))
        )
        self.max_retries: int = config.get("execution.max_retries", 3)
        self.retry_delay_base: float = float(
            config.get("execution.retry_delay_base_sec", "1.0")
        )
        self.max_execution_history: int = config.get("execution.max_history", 100)
        self.executions: list[TradeExecution] = []
        self.active_executions: dict[str, TradeExecution] = {}
        self.logger = get_logger(__name__)
        logger.info("ExecutionHandler initialized.")

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """
        Register an API client for an exchange.

        Args:
            exchange_id: Exchange identifier
            client: ExchangeAPI implementation
        """
        self.api_clients[exchange_id] = client
        logger.info(f"Registered API client for {exchange_id} in ExecutionHandler")

    async def execute_opportunity(self, opportunity: SizedOpportunity) -> TradeExecution:
        """
        Execute an arbitrage opportunity.
        Uses SymbolMapper to get exchange-specific symbols.

        Args:
            opportunity: Sized arbitrage opportunity

        Returns:
            TradeExecution object representing the outcome
        """
        execution = TradeExecution(opportunity)
        self.active_executions[execution.id] = execution
        execution.start_time = datetime.now(UTC)
        logger.info(f"Starting execution {execution.id} for opportunity: {opportunity.opportunity.symbol}")

        # --- Circuit Breaker Check ---
        if self.circuit_breaker_system:
            try:
                can_long, long_reason = self.circuit_breaker_system.can_execute(
                    opportunity.opportunity.long_exchange
                )
                if not can_long:
                    raise CircuitBreakerTrippedError(
                        f"Circuit breaker tripped for long exchange "
                        f"{opportunity.opportunity.long_exchange}: {long_reason}"
                    )
                can_short, short_reason = self.circuit_breaker_system.can_execute(
                    opportunity.opportunity.short_exchange
                )
                if not can_short:
                    raise CircuitBreakerTrippedError(
                        f"Circuit breaker tripped for short exchange "
                        f"{opportunity.opportunity.short_exchange}: {short_reason}"
                    )
            except CircuitBreakerTrippedError as e:
                error_msg = f"Execution {execution.id} rejected due to circuit breaker: {e}"
                logger.warning(error_msg)
                execution.status = ExecutionStatus.REJECTED
                execution.error_message = str(e)
                execution.end_time = datetime.now(UTC)
                self._add_to_history(execution)
                if execution.id in self.active_executions:
                    del self.active_executions[execution.id]
                return execution

        # --- Get API Clients and Symbols ---
        try:
            long_client = self.api_clients.get(opportunity.opportunity.long_exchange)
            short_client = self.api_clients.get(opportunity.opportunity.short_exchange)
            if not long_client or not short_client:
                raise ValueError("API client not registered for one or both exchanges")
            long_symbol = self.symbol_mapper.get_exchange_symbol(
                opportunity.opportunity.symbol, opportunity.opportunity.long_exchange
            )
            short_symbol = self.symbol_mapper.get_exchange_symbol(
                opportunity.opportunity.symbol, opportunity.opportunity.short_exchange
            )
            if not long_symbol or not short_symbol:
                raise ValueError(
                    f"Failed to map symbol '{opportunity.opportunity.symbol}' for one or both exchanges"
                )
        except ValueError as e:
            error_msg = f"Execution {execution.id} failed during setup (API client/symbol mapping): {e}"
            logger.error(error_msg)
            execution.status = ExecutionStatus.FAILED
            execution.error_message = str(e)
            execution.end_time = datetime.now(UTC)
            self._add_to_history(execution)
            if execution.id in self.active_executions:
                del self.active_executions[execution.id]
            return execution

        # --- Determine Order Type ---
        use_market_orders = self.config.get("execution.use_market_orders", False)
        order_type = OrderType.MARKET if use_market_orders else OrderType.LIMIT
        time_in_force = TimeInForce.IOC if use_market_orders else TimeInForce.GTC

        # --- Get Tickers for Limit Order Pricing (if needed) ---
        long_price: Decimal | None = None
        short_price: Decimal | None = None
        if order_type == OrderType.LIMIT:
            try:
                ticker_tasks = [
                    long_client.get_ticker(long_symbol),
                    short_client.get_ticker(short_symbol),
                ]
                # Corrected: Removed unused type: ignore
                tickers: list[Ticker | None] = await asyncio.gather(*ticker_tasks, return_exceptions=True)
                long_ticker = tickers[0]
                short_ticker = tickers[1]
                if isinstance(long_ticker, Exception):
                    raise APIError(f"Failed to get ticker for long leg: {long_ticker}") from long_ticker
                if isinstance(short_ticker, Exception):
                    raise APIError(f"Failed to get ticker for short leg: {short_ticker}") from short_ticker
                if not long_ticker or not long_ticker.ask:
                    raise ValueError(f"Missing ask price in ticker for long leg: {long_symbol}")
                if not short_ticker or not short_ticker.bid:
                    raise ValueError(f"Missing bid price in ticker for short leg: {short_symbol}")
                long_price = long_ticker.ask
                short_price = short_ticker.bid
                logger.debug(f"Execution {execution.id}: Using limit prices - Long Ask: {long_price}, Short Bid: {short_price}")
            except (APIError, ValueError, asyncio.TimeoutError) as e:
                error_msg = f"Execution {execution.id} failed getting tickers for limit orders: {e}"
                logger.error(error_msg)
                execution.status = ExecutionStatus.FAILED
                execution.error_message = str(e)
                execution.end_time = datetime.now(UTC)
                self._add_to_history(execution)
                if execution.id in self.active_executions:
                    del self.active_executions[execution.id]
                if self.circuit_breaker_system:
                    self.circuit_breaker_system.record_critical_failure(
                        opportunity.opportunity.long_exchange, "TickerFetchError"
                    )
                    self.circuit_breaker_system.record_critical_failure(
                        opportunity.opportunity.short_exchange, "TickerFetchError"
                    )
                return execution

        # --- Place Orders ---
        execution.status = ExecutionStatus.EXECUTING
        long_order_result: Order | None = None
        short_order_result: Order | None = None
        compensation_needed = False
        failed_leg_exchange: str | None = None
        failed_leg_symbol: str | None = None
        filled_leg_order: Order | None = None
        compensating_exchange_id: str | None = None

        try:
            placement_type = self.config.get("execution.order_placement_type", "concurrent")
            if placement_type == "sequential":
                logger.info(f"Execution {execution.id}: Placing orders sequentially...")
                long_order_result = await self._place_order_with_retry(
                    exchange_id=opportunity.opportunity.long_exchange,
                    client=long_client,
                    symbol=long_symbol,
                    side=OrderSide.BUY,
                    order_type=order_type,
                    quantity=opportunity.long_size,
                    price=long_price,
                    time_in_force=time_in_force,
                    client_order_id=f"cde_long_{execution.id[:8]}",
                )
                execution.long_order_id = long_order_result.id
                execution.long_order_response = long_order_result.to_dict()
                logger.info(f"Execution {execution.id}: Long order placed: {long_order_result.id}")
                # Corrected: Pass exchange_id to update_order
                self.portfolio_tracker.update_order(opportunity.opportunity.long_exchange, long_order_result)
                if long_order_result.status in (OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED, OrderStatus.OPEN, OrderStatus.NEW):
                    try:
                        short_order_result = await self._place_order_with_retry(
                            exchange_id=opportunity.opportunity.short_exchange,
                            client=short_client,
                            symbol=short_symbol,
                            side=OrderSide.SELL,
                            order_type=order_type,
                            quantity=opportunity.short_size,
                            price=short_price,
                            time_in_force=time_in_force,
                            client_order_id=f"cde_short_{execution.id[:8]}",
                        )
                        execution.short_order_id = short_order_result.id
                        execution.short_order_response = short_order_result.to_dict()
                        logger.info(f"Execution {execution.id}: Short order placed: {short_order_result.id}")
                        # Corrected: Pass exchange_id to update_order
                        self.portfolio_tracker.update_order(opportunity.opportunity.short_exchange, short_order_result)
                    except APIError as e:
                        logger.warning(f"Execution {execution.id}: Short order failed: {e}. Compensation needed.")
                        compensation_needed = True
                        failed_leg_exchange = opportunity.opportunity.short_exchange
                        failed_leg_symbol = opportunity.opportunity.symbol
                        filled_leg_order = long_order_result
                        compensating_exchange_id = opportunity.opportunity.long_exchange
                        execution.error_message = f"Short leg failed: {e}"
                        if self.circuit_breaker_system:
                            # Corrected: Pass error message string
                            self.circuit_breaker_system.record_api_error(failed_leg_exchange, str(e.code))
                else:
                    raise APIError(f"Long order failed immediately with status: {long_order_result.status}")
            elif placement_type == "concurrent":
                logger.info(f"Execution {execution.id}: Placing orders concurrently...")
                long_task = self._place_order_with_retry(
                    exchange_id=opportunity.opportunity.long_exchange,
                    client=long_client,
                    symbol=long_symbol,
                    side=OrderSide.BUY,
                    order_type=order_type,
                    quantity=opportunity.long_size,
                    price=long_price,
                    time_in_force=time_in_force,
                    client_order_id=f"cde_long_{execution.id[:8]}",
                )
                short_task = self._place_order_with_retry(
                    exchange_id=opportunity.opportunity.short_exchange,
                    client=short_client,
                    symbol=short_symbol,
                    side=OrderSide.SELL,
                    order_type=order_type,
                    quantity=opportunity.short_size,
                    price=short_price,
                    time_in_force=time_in_force,
                    client_order_id=f"cde_short_{execution.id[:8]}",
                )
                results = await asyncio.gather(long_task, short_task, return_exceptions=True)
                if isinstance(results[0], Order):
                    long_order_result = results[0]
                    execution.long_order_id = long_order_result.id
                    execution.long_order_response = long_order_result.to_dict()
                    logger.info(f"Execution {execution.id}: Long order placed: {long_order_result.id}")
                    # Corrected: Pass exchange_id to update_order
                    self.portfolio_tracker.update_order(opportunity.opportunity.long_exchange, long_order_result)
                    if self.circuit_breaker_system:
                         # Corrected: Use record_api_success
                         self.circuit_breaker_system.record_api_success(opportunity.opportunity.long_exchange)
                elif isinstance(results[0], APIError):
                    logger.error(f"Execution {execution.id}: Long order failed: {results[0]}")
                    execution.error_message = f"Long leg failed: {results[0]}"
                    if self.circuit_breaker_system:
                        # Corrected: Pass error message string
                        self.circuit_breaker_system.record_api_error(opportunity.opportunity.long_exchange, str(results[0].code))
                elif isinstance(results[0], Exception):
                     logger.error(f"Execution {execution.id}: Unexpected error placing long order: {results[0]}", exc_info=True)
                     execution.error_message = f"Unexpected long leg error: {results[0]}"
                     if self.circuit_breaker_system:
                         self.circuit_breaker_system.record_critical_failure(opportunity.opportunity.long_exchange, "PlaceOrderException")
                if isinstance(results[1], Order):
                    short_order_result = results[1]
                    execution.short_order_id = short_order_result.id
                    execution.short_order_response = short_order_result.to_dict()
                    logger.info(f"Execution {execution.id}: Short order placed: {short_order_result.id}")
                    # Corrected: Pass exchange_id to update_order
                    self.portfolio_tracker.update_order(opportunity.opportunity.short_exchange, short_order_result)
                    if self.circuit_breaker_system:
                         # Corrected: Use record_api_success
                         self.circuit_breaker_system.record_api_success(opportunity.opportunity.short_exchange)
                elif isinstance(results[1], APIError):
                    logger.warning(f"Execution {execution.id}: Short order failed: {results[1]}. Checking if compensation needed.")
                    execution.error_message = (execution.error_message + "; " if execution.error_message else "") + f"Short leg failed: {results[1]}"
                    if long_order_result:
                        compensation_needed = True
                        failed_leg_exchange = opportunity.opportunity.short_exchange
                        failed_leg_symbol = opportunity.opportunity.symbol
                        filled_leg_order = long_order_result
                        compensating_exchange_id = opportunity.opportunity.long_exchange
                    if self.circuit_breaker_system:
                        # Corrected: Pass error message string
                        self.circuit_breaker_system.record_api_error(opportunity.opportunity.short_exchange, str(results[1].code))
                elif isinstance(results[1], Exception):
                     logger.error(f"Execution {execution.id}: Unexpected error placing short order: {results[1]}", exc_info=True)
                     execution.error_message = (execution.error_message + "; " if execution.error_message else "") + f"Unexpected short leg error: {results[1]}"
                     if long_order_result:
                         compensation_needed = True
                         failed_leg_exchange = opportunity.opportunity.short_exchange
                         failed_leg_symbol = opportunity.opportunity.symbol
                         filled_leg_order = long_order_result
                         compensating_exchange_id = opportunity.opportunity.long_exchange
                     if self.circuit_breaker_system:
                         self.circuit_breaker_system.record_critical_failure(opportunity.opportunity.short_exchange, "PlaceOrderException")
                if not long_order_result and not short_order_result:
                    execution.status = ExecutionStatus.FAILED
                elif not short_order_result and not compensation_needed:
                     execution.status = ExecutionStatus.FAILED
                elif not long_order_result:
                     execution.status = ExecutionStatus.FAILED
                     logger.warning(f"Execution {execution.id}: Long leg failed, short leg succeeded. Manual review may be needed.")
            else:
                raise ValueError(f"Unsupported order_placement_type: {placement_type}")

            # --- Handle Compensation (if needed) ---
            if compensation_needed and filled_leg_order and compensating_exchange_id:
                logger.warning(f"Execution {execution.id}: Initiating compensation for failed leg on {failed_leg_exchange}")
                execution.status = ExecutionStatus.COMPENSATING
                try:
                    compensating_client = self.api_clients.get(compensating_exchange_id)
                    compensating_internal_symbol = self.symbol_mapper.get_internal_symbol(filled_leg_order.symbol, compensating_exchange_id)
                    if not compensating_client or not compensating_internal_symbol:
                         raise ValueError("Could not determine client or internal symbol for compensation")
                    comp_success = await self._compensate_position(
                        client=compensating_client,
                        exchange_id=compensating_exchange_id,
                        internal_symbol=compensating_internal_symbol,
                        quantity=filled_leg_order.filled_quantity or filled_leg_order.quantity,
                        original_order=filled_leg_order,
                    )
                    if comp_success:
                        logger.info(f"Execution {execution.id}: Compensation successful.")
                        execution.status = ExecutionStatus.COMPLETED
                        # Corrected: Handle None case for error_message
                        execution.error_message = (execution.error_message + "; " if execution.error_message else "") + "Compensation successful"
                    else:
                        logger.error(f"Execution {execution.id}: Compensation attempt failed.")
                        execution.status = ExecutionStatus.FAILED
                        # Corrected: Handle None case for error_message
                        execution.error_message = (execution.error_message + "; " if execution.error_message else "") + "Compensation FAILED"
                        if self.circuit_breaker_system:
                            self.circuit_breaker_system.record_critical_failure(
                                compensating_exchange_id, "CompensationFailure"
                            )
                except Exception as comp_e:
                    logger.exception(f"Execution {execution.id}: Error during compensation: {comp_e}")
                    execution.status = ExecutionStatus.FAILED
                    # Corrected: Handle None case for error_message
                    execution.error_message = (execution.error_message + "; " if execution.error_message else "") + f"Compensation Error: {comp_e}"
                    if self.circuit_breaker_system and compensating_exchange_id:
                        self.circuit_breaker_system.record_critical_failure(
                            compensating_exchange_id, "CompensationException"
                        )

            # --- Final Status Update (if not already failed/compensated) ---
            if execution.status == ExecutionStatus.EXECUTING:
                 logger.info(f"Execution {execution.id}: Both legs placed successfully (monitoring needed for fills). Marking COMPLETED for now.")
                 execution.status = ExecutionStatus.COMPLETED
        except APIError as e:
            error_msg = f"Execution {execution.id} failed placing orders: {e}"
            logger.error(error_msg)
            execution.status = ExecutionStatus.FAILED
            execution.error_message = str(e)
            if self.circuit_breaker_system:
                 # Corrected: Pass error message string
                 self.circuit_breaker_system.record_api_error(opportunity.opportunity.long_exchange, str(e.code))
                 self.circuit_breaker_system.record_api_error(opportunity.opportunity.short_exchange, str(e.code))
        except Exception as e:
            error_msg = f"Execution {execution.id} failed with unexpected error: {e}"
            logger.exception(error_msg)
            execution.status = ExecutionStatus.FAILED
            execution.error_message = f"Unexpected error: {e}"
            if self.circuit_breaker_system:
                self.circuit_breaker_system.record_critical_failure(
                    opportunity.opportunity.long_exchange, "ExecuteOpportunityException"
                )
                self.circuit_breaker_system.record_critical_failure(
                    opportunity.opportunity.short_exchange, "ExecuteOpportunityException"
                )
        finally:
            execution.end_time = datetime.now(UTC)
            self._add_to_history(execution)
            if execution.id in self.active_executions:
                 del self.active_executions[execution.id]
            logger.info(f"Execution {execution.id} finished with status: {execution.status.name}")
        return execution

    async def _place_order_with_retry(
        self,
        client: ExchangeAPI,
        exchange_id: str,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: Decimal,
        price: Decimal | None = None,
        time_in_force: TimeInForce | None = None,
        retry_delay: float = 0.5,
        max_retries: int = 3,
        reduce_only: bool = False,
        client_order_id: str | None = None,
    ) -> Order:
        """
        Place an order with retry logic.
        ... (Args, Returns, Raises docstrings omitted for brevity) ...
        """
        if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT) and price is None:
            raise ValueError(f"Price is required for {order_type.name} orders.")

        # Corrected: Ensure time_in_force is not None if required by place_order
        effective_time_in_force = time_in_force or TimeInForce.GTC

        retries = 0
        last_exception: Exception | None = None

        while retries <= max_retries:
            try:
                logger.debug(
                    f"Attempt {retries + 1}/{max_retries + 1}: Placing order on {exchange_id} - "
                    f"{side.name} {quantity} {symbol} @ {price or 'MARKET'} ({order_type.name})"
                    f"{' ReduceOnly' if reduce_only else ''}"
                    f"{' CID: ' + client_order_id if client_order_id else ''}"
                )
                order = await client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price,
                    time_in_force=effective_time_in_force, # Pass non-None TIF
                    reduce_only=reduce_only,
                    client_order_id=client_order_id,
                )
                logger.info(
                    f"Order placed successfully on {exchange_id}: {order.id} "
                    f"(Client ID: {order.client_order_id or 'N/A'})"
                )
                if self.circuit_breaker_system:
                    # Corrected: Use record_api_success
                    self.circuit_breaker_system.record_api_success(exchange_id)
                return order
            except APIError as e:
                last_exception = e
                logger.warning(
                    f"APIError placing order on {exchange_id} (Attempt {retries + 1}): {e.code} - {e.message}"
                )
                if self.circuit_breaker_system:
                    # Corrected: Pass error message string
                    self.circuit_breaker_system.record_api_error(exchange_id, str(e.code))
                if not e.is_retryable or retries >= max_retries:
                    logger.error(
                        f"Order placement failed permanently on {exchange_id} after {retries + 1} attempts: {e}"
                    )
                    # Corrected: Re-raise the final exception
                    raise e
                delay = (retry_delay * (2**retries)) + (random.uniform(0, retry_delay * 0.5))
                logger.info(f"Retrying order placement on {exchange_id} in {delay:.2f} seconds...")
                await asyncio.sleep(delay)
                retries += 1
            except Exception as e:
                last_exception = e
                logger.exception(
                     f"Unexpected error placing order on {exchange_id} (Attempt {retries + 1}): {e}"
                )
                if self.circuit_breaker_system:
                    self.circuit_breaker_system.record_critical_failure(exchange_id, "PlaceOrderException")
                logger.error(
                    f"Order placement failed permanently on {exchange_id} due to unexpected error: {e}"
                )
                raise
        # Corrected: Added explicit raise for logically unreachable end
        raise RuntimeError("Order placement loop exited unexpectedly.")


    async def _get_order_status(
        self,
        client: ExchangeAPI,
        exchange_id: str,
        order_id: str | None = None,
        symbol: str | None = None,
        client_order_id: str | None = None,
        retry_delay: float = 0.5,
        max_retries: int = 3,
    ) -> Order | None:
        """
        Get the status of an order with retry logic.
         ... (Args, Returns, Raises docstrings omitted for brevity) ...
       """
        if not order_id and not client_order_id:
             raise ValueError("Either order_id or client_order_id must be provided to get order status.")

        retries = 0
        last_exception: Exception | None = None
        lookup_id = order_id or client_order_id

        while retries <= max_retries:
            try:
                # Corrected: Check if client *has* get_order_status before calling
                if not hasattr(client, 'get_order_status'):
                    logger.error(f"ExchangeAPI client for {exchange_id} does not implement get_order_status")
                    # Cannot proceed without the method, treat as failure
                    raise NotImplementedError(f"get_order_status not implemented for {exchange_id}")

                order_status: Order | None = await client.get_order_status(order_id, symbol, client_order_id)
                if order_status:
                    logger.debug(f"Successfully retrieved status for order {lookup_id} on {exchange_id}: {order_status.status}")
                    if self.circuit_breaker_system:
                        # Corrected: Use record_api_success
                        self.circuit_breaker_system.record_api_success(exchange_id)
                    return order_status # Returns Order | None, matching signature
                else:
                    logger.warning(f"Order {lookup_id} not found on {exchange_id} (Attempt {retries + 1}).")
                    if retries >= max_retries:
                        logger.error(f"Order {lookup_id} not found on {exchange_id} after {max_retries + 1} attempts.")
                        return None # Returns None, matching signature
            except APIError as e:
                last_exception = e
                logger.warning(
                    f"APIError getting status for order {lookup_id} on {exchange_id} (Attempt {retries + 1}): {e.code} - {e.message}"
                )
                if self.circuit_breaker_system:
                    # Corrected: Pass error message string
                    self.circuit_breaker_system.record_api_error(exchange_id, str(e.code))
                if e.code == APIErrorCode.ORDER_NOT_FOUND and retries < max_retries:
                     pass
                elif not e.is_retryable or retries >= max_retries:
                    logger.error(
                        f"Failed permanently getting status for order {lookup_id} on {exchange_id}: {e}"
                    )
                    # Corrected: Re-raise the final exception
                    raise e
            except Exception as e:
                last_exception = e
                logger.exception(
                    f"Unexpected error getting status for order {lookup_id} on {exchange_id} (Attempt {retries + 1}): {e}"
                )
                if self.circuit_breaker_system:
                    self.circuit_breaker_system.record_critical_failure(exchange_id, "GetOrderStatusException")
                raise
            delay = (retry_delay * (2**retries)) + (random.uniform(0, retry_delay * 0.5))
            logger.info(f"Retrying status check for order {lookup_id} on {exchange_id} in {delay:.2f} seconds...")
            await asyncio.sleep(delay)
            retries += 1
        logger.error(f"Order {lookup_id} still not found on {exchange_id} after all retries.")
        return None # Returns None, matching signature

    async def _compensate_position(
        self,
        client: ExchangeAPI,
        exchange_id: str,
        internal_symbol: str,
        quantity: Decimal,
        original_order: Order,
    ) -> bool:
        """Attempts to compensate for a partially filled or failed order leg."""
        current_position = self.portfolio_tracker.get_position(exchange_id, internal_symbol)
        current_pos_size = current_position.size if current_position else Decimal("0")
        logger.debug(
            f"Attempting compensation on {exchange_id} for {internal_symbol}. "
            f"Original Order Side: {original_order.side.name}, Quantity: {quantity}. "
            f"Current Position Size: {current_pos_size}"
        )
        compensation_side = OrderSide.SELL if original_order.side == OrderSide.BUY else OrderSide.BUY
        exchange_symbol = self.symbol_mapper.get_exchange_symbol(internal_symbol, exchange_id)
        if not exchange_symbol:
            raise ValueError(f"Failed to map internal symbol '{internal_symbol}' back to exchange symbol for {exchange_id}")
        use_limit = self.config.get("execution.compensation.use_limit_orders", False)
        comp_order_type = OrderType.LIMIT if use_limit else OrderType.MARKET
        comp_price: Decimal | None = None
        comp_tif = TimeInForce.GTC
        if comp_order_type == OrderType.LIMIT:
            try:
                ticker = await client.get_ticker(exchange_symbol)
                if not ticker:
                    raise ValueError(f"Could not get ticker for {exchange_symbol} on {exchange_id} for compensation limit price.")
                price_offset_pct_str = self.config.get("execution.compensation.limit_price_offset_pct", "0.05")
                price_offset_pct = Decimal(price_offset_pct_str)
                if compensation_side == OrderSide.SELL:
                    if not ticker.bid:
                         raise ValueError(f"Missing bid price for {exchange_symbol} on {exchange_id}")
                    comp_price = ticker.bid * (Decimal("1") - price_offset_pct)
                    logger.info(f"Compensation: Selling {quantity} {exchange_symbol} at limit price {comp_price} (Bid: {ticker.bid})")
                else:
                    if not ticker.ask:
                         raise ValueError(f"Missing ask price for {exchange_symbol} on {exchange_id}")
                    comp_price = ticker.ask * (Decimal("1") + price_offset_pct)
                    logger.info(f"Compensation: Buying {quantity} {exchange_symbol} at limit price {comp_price} (Ask: {ticker.ask})")
            except (APIError, ValueError, InvalidOperation) as e:
                logger.error(f"Failed to determine compensation limit price for {exchange_symbol} on {exchange_id}: {e}. Falling back to MARKET order.")
                comp_order_type = OrderType.MARKET
                comp_price = None
                comp_tif = TimeInForce.IOC
        try:
            logger.info(
                f"Placing compensation order on {exchange_id}: {compensation_side.name} "
                f"{quantity} {exchange_symbol} ({comp_order_type.name} @ {comp_price or 'MARKET'})"
            )
            compensating_order = await self._place_order_with_retry(
                client=client,
                exchange_id=exchange_id,
                symbol=exchange_symbol,
                side=compensation_side,
                order_type=comp_order_type,
                quantity=quantity,
                price=comp_price,
                time_in_force=comp_tif,
                reduce_only=True,
                client_order_id=f"cde_comp_{original_order.id[:8]}_{int(time.time())}",
            )
            logger.info(f"Compensation order placed: {compensating_order.id}")
            # Corrected: Pass exchange_id to update_order
            self.portfolio_tracker.update_order(exchange_id, compensating_order)
            return True
        except Exception as e:
            logger.exception(f"Failed to place compensation order on {exchange_id} for {internal_symbol}: {e}")
            if self.circuit_breaker_system:
                self.circuit_breaker_system.record_critical_failure(exchange_id, "CompensationPlacementError")
            return False

    def get_execution_history(self) -> list[TradeExecution]:
        """Return a copy of the execution history."""
        return list(self.executions)

    def get_active_executions(self) -> list[TradeExecution]:
        """Return a list of currently active executions."""
        return [
            exec_obj for exec_obj in self.active_executions.values()
            if exec_obj.status not in (ExecutionStatus.COMPLETED, ExecutionStatus.FAILED, ExecutionStatus.REJECTED)
        ]

    def reset_circuit_breaker(self, exchange_id: str) -> None:
        """Resets the circuit breaker for a specific exchange."""
        if self.circuit_breaker_system:
            logger.info(f"Attempting to reset circuit breaker for {exchange_id}")
            reset_count = self.circuit_breaker_system.reset_exchange_breakers(exchange_id)
            logger.info(f"Reset {reset_count} circuit breaker(s) for {exchange_id}.")
        else:
            logger.warning("Circuit breaker system not configured. Cannot reset.")

    async def _verify_order_state(
        self,
        execution: TradeExecution,
        order_id: str,
        exchange_id: str,
        expected_side: OrderSide,
        expected_quantity: Decimal,
    ) -> bool:
        """
        Verify the state of an order after placement.
         ... (Args, Returns docstrings omitted for brevity) ...
        """
        try:
            client = self.api_clients[exchange_id]
            order_status = await self._get_order_status(client, exchange_id, order_id)
            if not order_status:
                logger.warning(f"Execution {execution.id}: Order {order_id} on {exchange_id} not found during verification.")
                return False
            # Corrected: Pass exchange_id to update_order
            self.portfolio_tracker.update_order(exchange_id, order_status)
            if order_status.side != expected_side:
                 logger.error(f"Execution {execution.id}: Order {order_id} side mismatch! Expected {expected_side.name}, got {order_status.side.name}")
                 return False
            if order_status.quantity != expected_quantity:
                 logger.warning(f"Execution {execution.id}: Order {order_id} quantity mismatch. Expected {expected_quantity}, got {order_status.quantity}. Might be due to partial fill acceptance.")
            if order_status.status in (OrderStatus.REJECTED, OrderStatus.CANCELED, OrderStatus.EXPIRED, OrderStatus.FAILED):
                logger.error(f"Execution {execution.id}: Order {order_id} on {exchange_id} is in terminal error state: {order_status.status.name}")
                return False
            logger.info(f"Execution {execution.id}: Order {order_id} on {exchange_id} verified. Status: {order_status.status.name}")
            return True
        except Exception as e:
            logger.exception(f"Execution {execution.id}: Error verifying order {order_id} on {exchange_id}: {e}")
            return False

    async def _update_pnl(self, execution: TradeExecution) -> None:
        """
        Update PnL in the portfolio tracker after an execution is completed or partially filled.
        Requires fill information to be populated in the TradeExecution object.
        """
        if not execution.long_order_response or not execution.short_order_response:
            logger.warning(f"Execution {execution.id}: Cannot update PnL, missing order response data.")
            return
        try:
            long_id = execution.long_order_response.get("id")
            short_id = execution.short_order_response.get("id")
            # symbol = execution.opportunity.opportunity.symbol # Symbol not needed for _update_realized_pnl
            long_filled_qty = execution.long_fill_quantity or execution.long_order_response.get("filled_quantity")
            long_avg_price = execution.long_fill_price or execution.long_order_response.get("avg_fill_price")
            short_filled_qty = execution.short_fill_quantity or execution.short_order_response.get("filled_quantity")
            short_avg_price = execution.short_fill_price or execution.short_order_response.get("avg_fill_price")
            if not all([long_id, short_id, long_filled_qty, long_avg_price, short_filled_qty, short_avg_price]):
                 logger.warning(f"Execution {execution.id}: Missing required data for PnL calculation. "
                                f"IDs: ({long_id}, {short_id}), "
                                f"Long Fill: ({long_filled_qty} @ {long_avg_price}), "
                                f"Short Fill: ({short_filled_qty} @ {short_avg_price})")
                 return
            long_filled_qty_dec = Decimal(str(long_filled_qty))
            long_avg_price_dec = Decimal(str(long_avg_price))
            short_filled_qty_dec = Decimal(str(short_filled_qty))
            short_avg_price_dec = Decimal(str(short_avg_price))
            if long_filled_qty_dec != short_filled_qty_dec:
                logger.warning(f"Execution {execution.id}: Mismatched filled quantities ({long_filled_qty_dec} vs {short_filled_qty_dec}). PnL calculation might be inaccurate.")
            realized_pnl = (short_avg_price_dec * long_filled_qty_dec) - (long_avg_price_dec * long_filled_qty_dec)
            logger.info(f"Execution {execution.id}: Calculated Realized PnL: {realized_pnl:.4f}")
            # Corrected: Call internal method with only the amount
            self.portfolio_tracker._update_realized_pnl(realized_pnl)
        except (TypeError, InvalidOperation, KeyError, AttributeError) as e:
            logger.exception(f"Execution {execution.id}: Error calculating or updating PnL: {e}")

    async def _handle_filled_order(
            self,
            execution: TradeExecution,
            order: Order,
            exchange_id: str,
            is_long_leg: bool
        ) -> None:
        """
        Process a filled order, update execution details, and notify portfolio tracker.
        """
        logger.info(f"Execution {execution.id}: Handling filled order {order.id} on {exchange_id} ({'LONG' if is_long_leg else 'SHORT'})")
        fill_qty = order.filled_quantity
        avg_price = order.avg_fill_price
        order_id = order.id
        if fill_qty is None or avg_price is None or order_id is None:
            logger.warning(f"Execution {execution.id}: Order {order.id} status is FILLED but missing fill quantity or price. Cannot process fill.")
            return
        if is_long_leg:
            execution.long_fill_quantity = fill_qty
            execution.long_fill_price = avg_price
            execution.long_order_id = order_id
            execution.long_order_response = order.to_dict()
        else:
            execution.short_fill_quantity = fill_qty
            execution.short_fill_price = avg_price
            execution.short_order_id = order_id
            execution.short_order_response = order.to_dict()
        # Corrected: Pass exchange_id to update_order
        self.portfolio_tracker.update_order(exchange_id, order)
        try:
            trade_id = (
                f"trade_{execution.id[:8]}_{order_id[:8]}_{int(time.time()*1000)}"
            )
            # Corrected: Pass int timestamp, remove execution_id
            trade_timestamp = int((order.time or datetime.now(UTC)).timestamp() * 1000)
            trade = Trade(
                id=trade_id,
                # execution_id=execution.id, # Removed - not in Trade model
                order_id=order_id,
                exchange=exchange_id,
                symbol=order.symbol,
                side=order.side,
                quantity=fill_qty,
                price=avg_price,
                fee=order.metadata.get("fee", Decimal("0")),
                timestamp=trade_timestamp, # Corrected: Pass int timestamp
            )
            # self.portfolio_tracker.record_trade(trade) # Assuming this method exists
            logger.debug(f"Execution {execution.id}: Recorded trade {trade.id} for order {order_id}")
        except Exception as e:
            logger.exception(f"Execution {execution.id}: Failed to create or record Trade object for order {order_id}: {e}")

    async def _monitor_order_status(
            self,
            execution: TradeExecution,
            order_id: str,
            exchange_id: str,
            is_long_leg: bool,
            timeout_sec: float = 60.0
        ) -> OrderStatus | None:
        """
        Monitor the status of a single order until it reaches a terminal state or times out.
         ... (Args, Returns docstrings omitted for brevity) ...
        """
        start_time = time.monotonic()
        client = self.api_clients[exchange_id]
        order_response = execution.long_order_response if is_long_leg else execution.short_order_response
        symbol = order_response.get("symbol") if order_response else None
        client_order_id = order_response.get("client_order_id") if order_response else None
        logger.info(f"Execution {execution.id}: Monitoring order {order_id} on {exchange_id}...")
        terminal_states = [
            OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED,
            OrderStatus.EXPIRED, OrderStatus.FAILED,
        ]
        while time.monotonic() - start_time < timeout_sec:
            try:
                order = await self._get_order_status(
                    client=client, exchange_id=exchange_id, order_id=order_id,
                    symbol=symbol, client_order_id=client_order_id
                )
                if not order:
                    logger.warning(f"Execution {execution.id}: Order {order_id} not found during monitoring.")
                    await asyncio.sleep(self.retry_delay_base * 2)
                    continue
                current_status = order.status
                logger.debug(f"Execution {execution.id}: Order {order_id} status: {current_status.name}")
                # Corrected: Pass exchange_id to update_order
                self.portfolio_tracker.update_order(exchange_id, order)
                if current_status in terminal_states:
                    logger.info(f"Execution {execution.id}: Order {order_id} reached terminal state: {current_status.name}")
                    if current_status == OrderStatus.FILLED:
                         await self._handle_filled_order(execution, order, exchange_id, is_long_leg)
                    elif current_status in (OrderStatus.REJECTED, OrderStatus.FAILED):
                         if self.circuit_breaker_system:
                              error_code = APIErrorCode.ORDER_REJECTED
                              # Corrected: Pass error message string
                              self.circuit_breaker_system.record_api_error(exchange_id, str(error_code))
                    return current_status
                if current_status == OrderStatus.PARTIALLY_FILLED:
                    logger.info(f"Execution {execution.id}: Order {order_id} partially filled ({order.filled_quantity}/{order.quantity}). Continuing monitoring.")
                    if is_long_leg:
                        execution.long_fill_quantity = order.filled_quantity
                        execution.long_fill_price = order.avg_fill_price
                    else:
                        execution.short_fill_quantity = order.filled_quantity
                        execution.short_fill_price = order.avg_fill_price
                await asyncio.sleep(self.retry_delay_base)
            except APIError as e:
                logger.error(f"Execution {execution.id}: APIError monitoring order {order_id}: {e}")
                return OrderStatus.FAILED
            except Exception as e:
                logger.exception(f"Execution {execution.id}: Unexpected error monitoring order {order_id}: {e}")
                return OrderStatus.FAILED
        logger.warning(f"Execution {execution.id}: Timeout monitoring order {order_id} on {exchange_id}.")
        return None

    def _add_to_history(self, execution: TradeExecution) -> None:
        """Add execution to history, maintaining max size."""
        self.executions.append(execution)
        if len(self.executions) > self.max_execution_history:
            self.executions.pop(0)
