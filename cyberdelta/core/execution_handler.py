import logging
import asyncio
import time
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime
from enum import Enum, auto

from cyberdelta.apis.base import ExchangeAPI, APIError
from cyberdelta.core.models import Order, Position, OrderStatus, OrderSide, OrderType
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import SizedOpportunity
from cyberdelta.utils.config import Config

logger = logging.getLogger(__name__)

class ExecutionStatus(Enum):
    """Status of an execution."""
    PENDING = auto()
    EXECUTING = auto()
    COMPLETED = auto()
    FAILED = auto()
    PARTIALLY_COMPLETED = auto()
    COMPENSATING = auto()


class TradeExecution:
    """
    Represents a trade execution across multiple exchanges.
    """
    
    def __init__(self, opportunity: SizedOpportunity):
        """
        Initialize a trade execution.
        
        Args:
            opportunity: Sized arbitrage opportunity
        """
        self.opportunity = opportunity
        self.status = ExecutionStatus.PENDING
        self.error_message = None
        
        # Orders
        self.long_order_id = None
        self.short_order_id = None
        
        # Positions
        self.long_position_id = None
        self.short_position_id = None
        
        # Order response details
        self.long_order_response = None
        self.short_order_response = None
        
        # Timestamps
        self.start_time = None
        self.end_time = None
        
        # Execution details
        self.long_fill_price = None
        self.short_fill_price = None
        self.long_fill_quantity = None
        self.short_fill_quantity = None
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'opportunity': {
                'symbol': self.opportunity.opportunity.symbol,
                'long_exchange': self.opportunity.opportunity.long_exchange,
                'short_exchange': self.opportunity.opportunity.short_exchange,
                'long_size': self.opportunity.long_size,
                'short_size': self.opportunity.short_size,
                'expected_profit': self.opportunity.expected_profit,
            },
            'status': self.status.name,
            'error_message': self.error_message,
            'long_order_id': self.long_order_id,
            'short_order_id': self.short_order_id,
            'long_position_id': self.long_position_id,
            'short_position_id': self.short_position_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'long_fill_price': self.long_fill_price,
            'short_fill_price': self.short_fill_price,
            'long_fill_quantity': self.long_fill_quantity,
            'short_fill_quantity': self.short_fill_quantity,
        }
        
    def __str__(self) -> str:
        """String representation of the execution."""
        return (f"TradeExecution: {self.opportunity.opportunity.symbol} - "
                f"Long: {self.opportunity.opportunity.long_exchange} ${self.opportunity.long_size:.2f}, "
                f"Short: {self.opportunity.opportunity.short_exchange} ${self.opportunity.short_size:.2f}, "
                f"Status: {self.status.name}")


class CircuitBreaker:
    """
    Circuit breaker to prevent excessive trading during failing conditions.
    """
    
    def __init__(self, config: Config):
        """
        Initialize the circuit breaker.
        
        Args:
            config: Application configuration
        """
        self.config = config
        
        # Load circuit breaker parameters
        try:
            self.loss_threshold = config.get('execution.circuit_breaker.loss_threshold', 100.0)
            self.failed_trades_threshold = config.get('execution.circuit_breaker.failed_trades', 3)
        except Exception as e:
            # Set default values if config is not accessible
            logger.warning(f"Failed to load circuit breaker configuration: {e}. Using defaults.")
            self.loss_threshold = 100.0
            self.failed_trades_threshold = 3
        
        # State
        self.failed_trades_count = 0
        self.consecutive_failures = 0
        self.total_loss = 0.0
        self.open = False
        self.last_failure_time = None
        
    def record_success(self):
        """Record a successful trade execution."""
        self.consecutive_failures = 0
        
    def record_failure(self, loss_amount: float = 0.0):
        """
        Record a failed trade execution.
        
        Args:
            loss_amount: Amount lost in the failed trade
        """
        self.failed_trades_count += 1
        self.consecutive_failures += 1
        self.total_loss += loss_amount
        self.last_failure_time = datetime.now()
        
        # Check if circuit breaker should open
        if self.consecutive_failures >= self.failed_trades_threshold:
            self.open = True
            logger.warning(f"Circuit breaker opened after {self.consecutive_failures} consecutive failures")
        elif self.total_loss >= self.loss_threshold:
            self.open = True
            logger.warning(f"Circuit breaker opened after losses of ${self.total_loss:.2f}")
        # Don't automatically open the circuit breaker if thresholds aren't reached
    
    def reset(self):
        """Reset the circuit breaker."""
        self.failed_trades_count = 0
        self.consecutive_failures = 0
        self.total_loss = 0.0
        self.open = False
        logger.info("Circuit breaker reset")
        
    def is_open(self) -> bool:
        """Check if the circuit breaker is open."""
        return self.open


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
    
    def __init__(self, config: Config, portfolio_tracker: PortfolioTracker):
        """
        Initialize the execution handler.
        
        Args:
            config: Application configuration
            portfolio_tracker: Portfolio tracker for position updates
        """
        self.config = config
        self.portfolio_tracker = portfolio_tracker
        
        # API clients
        self.api_clients: Dict[str, ExchangeAPI] = {}
        
        # Execution parameters
        self.max_slippage = config.get('execution.max_slippage', 0.002)  # 0.2% max slippage
        self.max_retries = config.get('execution.max_retries', 3)
        self.retry_delay_base = config.get('execution.retry_delay_base', 1.0)  # seconds
        
        # Circuit breaker
        self.circuit_breaker = CircuitBreaker(config)
        
        # Execution history (keep last 100)
        self.execution_history: List[TradeExecution] = []
        self.max_execution_history = 100
        
        # Currently active executions
        self.active_executions: Dict[str, TradeExecution] = {}
    
    def register_api_client(self, exchange_id: str, client: ExchangeAPI):
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
        
        Args:
            opportunity: Sized arbitrage opportunity
            
        Returns:
            Trade execution result
        """
        # Check if circuit breaker is open
        if self.circuit_breaker.is_open():
            logger.warning("Circuit breaker is open, rejecting execution")
            execution = TradeExecution(opportunity)
            execution.status = ExecutionStatus.FAILED
            execution.error_message = "Circuit breaker is open"
            return execution
        
        # Create execution object
        execution = TradeExecution(opportunity)
        execution.start_time = datetime.now()
        execution.status = ExecutionStatus.EXECUTING
        
        # Generate a unique execution ID
        execution_id = f"{opportunity.opportunity.symbol}_{int(time.time())}"
        self.active_executions[execution_id] = execution
        
        try:
            logger.info(f"Executing opportunity: {opportunity}")

            long_exchange = opportunity.opportunity.long_exchange
            short_exchange = opportunity.opportunity.short_exchange
            long_client = self.api_clients[long_exchange]
            short_client = self.api_clients[short_exchange]
            internal_symbol = opportunity.opportunity.symbol

            # --- Calculate Long Quantity ---
            long_exchange_symbol = self.config.get(f'exchanges.{long_exchange}.symbols.{internal_symbol}')
            if not long_exchange_symbol:
                raise ValueError(f"Symbol mapping not found for {internal_symbol} on {long_exchange}")
            try:
                long_ticker = await long_client.get_ticker(long_exchange_symbol)
                if not long_ticker or long_ticker.ask == 0:
                    raise ValueError(f"Invalid ticker or zero ask price for {long_exchange_symbol} on {long_exchange}")
                long_base_quantity = opportunity.long_size / long_ticker.ask
                logger.debug(f"Calculated long quantity for {long_exchange}: {opportunity.long_size} USD / {long_ticker.ask} = {long_base_quantity:.8f} {internal_symbol}")
            except (APIError, ValueError, ZeroDivisionError) as e:
                execution.status = ExecutionStatus.FAILED
                execution.error_message = f"Failed to get ticker or calculate long quantity: {e}"
                self.circuit_breaker.record_failure()
                return execution
            # --------------------------------

            # Place long order with retry using base quantity and exchange symbol
            long_order = await self._place_order_with_retry(
                client=long_client,
                exchange_id=long_exchange,
                symbol=long_exchange_symbol, # Use EXCHANGE-SPECIFIC symbol
                side=OrderSide.BUY,
                order_type=OrderType.MARKET,
                quantity=long_base_quantity # Pass calculated base quantity
            )

            if not long_order:
                execution.status = ExecutionStatus.FAILED
                execution.error_message = "Failed to place long order"
                self.circuit_breaker.record_failure()
                return execution

            execution.long_order_id = long_order.id
            execution.long_order_response = long_order.to_dict()

            # --- Calculate Short Quantity ---
            short_exchange_symbol = self.config.get(f'exchanges.{short_exchange}.symbols.{internal_symbol}')
            if not short_exchange_symbol:
                raise ValueError(f"Symbol mapping not found for {internal_symbol} on {short_exchange}")
            try:
                short_ticker = await short_client.get_ticker(short_exchange_symbol)
                if not short_ticker or short_ticker.bid == 0:
                    raise ValueError(f"Invalid ticker or zero bid price for {short_exchange_symbol} on {short_exchange}")
                short_base_quantity = opportunity.short_size / short_ticker.bid
                logger.debug(f"Calculated short quantity for {short_exchange}: {opportunity.short_size} USD / {short_ticker.bid} = {short_base_quantity:.8f} {internal_symbol}")
            except (APIError, ValueError, ZeroDivisionError) as e:
                # If short quantity calc fails, we might still need to compensate the long leg
                execution.status = ExecutionStatus.COMPENSATING
                execution.error_message = f"Failed to get ticker or calculate short quantity: {e}. Compensating long."
                logger.warning(f"{execution.error_message}")
                await self._compensate_position(
                    client=long_client, exchange_id=long_exchange, symbol=internal_symbol,
                    original_failed_side=OrderSide.SELL, quantity=long_order.filled_quantity # Compensate what was filled
                )
                execution.status = ExecutionStatus.FAILED # Mark as failed after compensation attempt
                self.circuit_breaker.record_failure()
                return execution
            # ---------------------------------

            # Place short order with retry using base quantity and exchange symbol
            short_order = await self._place_order_with_retry(
                client=short_client,
                exchange_id=short_exchange,
                symbol=short_exchange_symbol, # Use EXCHANGE-SPECIFIC symbol
                side=OrderSide.SELL,
                order_type=OrderType.MARKET,
                quantity=short_base_quantity # Pass calculated base quantity
            )

            # --- ADDED: Compensation logic if short fails after long succeeds ---
            if not short_order and long_order and long_order.status == OrderStatus.FILLED:
                # Short order failed, but long order was placed and presumably filled
                execution.status = ExecutionStatus.COMPENSATING
                execution.error_message = f"Failed to place short order ({short_exchange_symbol}), compensating long ({long_exchange_symbol})"
                logger.warning(execution.error_message)
                # Compensate the filled quantity of the long order
                compensation_result = await self._compensate_position(
                    client=long_client, 
                    exchange_id=long_exchange, 
                    symbol=long_exchange_symbol, # Use exchange-specific symbol for compensation
                    original_failed_side=OrderSide.SELL, # We failed to SELL short, so compensate the BUY long
                    quantity=long_order.filled_quantity 
                )
                # Regardless of compensation success, mark final status as FAILED
                execution.status = ExecutionStatus.FAILED 
                # Attach specific failure reason if compensation also failed
                if not compensation_result:
                    execution.error_message += "; Compensation attempt also failed."
                # Record failure and return
                self.circuit_breaker.record_failure() 
                return execution
            # --- END ADDED Compensation Logic ---
            
            # Original logic continues if short_order was successful (or if long failed earlier)
            elif not short_order: # This case handles if long failed initially, short wasn't attempted, or some other error
                # If we reach here and short_order is None, it implies long_order likely also failed or wasn't filled
                # The initial check after long_order placement should have caught long failure.
                # This path might be redundant or needs refinement based on _place_order_with_retry's exact return on failure.
                # Let's assume for now the earlier checks handle long-failure returns correctly.
                # If short fails but long didn't fill, we might not need compensation yet.
                # Let's refine the error message if we land here unexpectedly.
                if execution.status != ExecutionStatus.FAILED: # Avoid overwriting specific long failure message
                    execution.status = ExecutionStatus.FAILED
                    execution.error_message = "Failed to place short order (reason unclear, check logs)" 
                    logger.error(f"Execution reached unexpected state: short order failed but long order status unclear or not filled. Long: {long_order}")
                    self.circuit_breaker.record_failure()
                return execution # Return if short failed and compensation wasn't triggered above

            # --- Short order was placed successfully ---            
            execution.short_order_id = short_order.id
            execution.short_order_response = short_order.to_dict()

            # Wait for orders to settle (can be improved with WS updates)
            await asyncio.sleep(self.config.get('execution.settlement_delay', 2.0)) # Use config value
            
            # Update order status (fetch latest state)
            updated_long_order = await self._get_order_status(long_client, long_exchange, execution.long_order_id)
            updated_short_order = await self._get_order_status(short_client, short_exchange, execution.short_order_id)
            
            # Use the updated order objects for status checks
            long_order = updated_long_order if updated_long_order else long_order
            short_order = updated_short_order if updated_short_order else short_order
            
            # Check if orders were filled
            long_filled = long_order and long_order.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED] and long_order.filled_quantity > 0
            short_filled = short_order and short_order.status in [OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED] and short_order.filled_quantity > 0
            
            if long_filled and short_filled:
                # --- Both orders have at least partial fills --- 
                # Check for FULL completion vs PARTIAL completion of the pair
                if long_order.status == OrderStatus.FILLED and short_order.status == OrderStatus.FILLED: 
                    # Both legs fully filled
                    execution.status = ExecutionStatus.COMPLETED
                    execution.long_fill_price = long_order.price # Use price attribute
                    execution.short_fill_price = short_order.price # Use price attribute
                    execution.long_fill_quantity = long_order.filled_quantity
                    execution.short_fill_quantity = short_order.filled_quantity
                    
                    # Update portfolio tracker after successful execution
                    logger.debug(f"Updating portfolio tracker for successful execution {long_order.id}/{short_order.id}")
                    
                    # Create Position objects from filled orders
                    long_position = Position(
                        symbol=opportunity.opportunity.symbol,
                        size=long_order.filled_quantity,
                        entry_price=long_order.price, # Use price
                        mark_price=long_order.price, # Use fill price as initial mark price
                        side=OrderSide.BUY,
                        id=f"pos_{long_order.id}" # Create a simple position ID
                    )
                    short_position = Position(
                        symbol=opportunity.opportunity.symbol,
                        size=short_order.filled_quantity,
                        entry_price=short_order.price, # Use price
                        mark_price=short_order.price, # Use fill price as initial mark price
                        side=OrderSide.SELL,
                        id=f"pos_{short_order.id}" # Create a simple position ID
                    )
                    
                    # Update tracker with Position objects
                    self.portfolio_tracker.update_position(
                        exchange_id=long_exchange,
                        position=long_position
                    )
                    self.portfolio_tracker.update_position(
                        exchange_id=short_exchange,
                        position=short_position
                    )
                    
                    # Record success
                    self.circuit_breaker.record_success()
                    
                    logger.info(f"Successfully executed opportunity: {opportunity}")
                else:
                    # At least one leg is PARTIALLY_FILLED
                    execution.status = ExecutionStatus.PARTIALLY_COMPLETED
                    execution.error_message = "Trade partially completed. At least one leg not fully filled."
                    logger.warning(f"{execution.error_message} Long: {long_order.status.name} ({long_order.filled_quantity}/{long_order.quantity}), Short: {short_order.status.name} ({short_order.filled_quantity}/{short_order.quantity})")
                    # Record partial failure
                    # Calculate potential loss based on unfilled portion or slippage (simplified estimate)
                    loss_estimate = opportunity.expected_profit * 0.5 # Crude estimate
                    self.circuit_breaker.record_failure(loss_estimate)
                    # TODO: Implement logic to handle partial fills (e.g., try to fill remainder, or compensate unfilled portion)

                # Record fill details regardless of full/partial
                execution.long_fill_price = long_order.price if long_order and long_order.filled_quantity > 0 else None # Use price
                execution.short_fill_price = short_order.price if short_order and short_order.filled_quantity > 0 else None # Use price
                execution.long_fill_quantity = long_order.filled_quantity if long_order else 0
                execution.short_fill_quantity = short_order.filled_quantity if short_order else 0
                
                # Update portfolio tracker only if COMPLETED? Or update with partials too?
                # Current logic updates only on FULL completion. Let's stick to that for now.
                if execution.status == ExecutionStatus.COMPLETED:
                    logger.debug(f"Updating portfolio tracker for successful execution {long_order.id}/{short_order.id}")
                    # ... (Create Position objects and update tracker - logic remains the same) ...
                    self.circuit_breaker.record_success()
                    logger.info(f"Successfully executed opportunity: {opportunity}")
                
            elif long_filled or short_filled:
                 # --- Only one leg has any fill (other is NEW, CANCELED, REJECTED etc) --- 
                execution.status = ExecutionStatus.COMPENSATING # Need to compensate the filled leg
                filled_leg_desc = "long" if long_filled else "short"
                failed_leg_desc = "short" if long_filled else "long"
                execution.error_message = f"Only {filled_leg_desc} order filled ({long_order.status.name if long_filled else short_order.status.name}). {failed_leg_desc.capitalize()} order failed ({short_order.status.name if long_filled else long_order.status.name}). Compensating."
                logger.warning(execution.error_message)

                # Determine which leg to compensate
                if long_filled:
                    comp_client, comp_exch, comp_symbol, comp_qty, comp_failed_side = \
                        long_client, long_exchange, long_exchange_symbol, long_order.filled_quantity, OrderSide.SELL
                else: # short_filled
                    comp_client, comp_exch, comp_symbol, comp_qty, comp_failed_side = \
                        short_client, short_exchange, short_exchange_symbol, short_order.filled_quantity, OrderSide.BUY

                compensation_result = await self._compensate_position(
                    client=comp_client, exchange_id=comp_exch, symbol=comp_symbol, 
                    original_failed_side=comp_failed_side, quantity=comp_qty
                )
                
                execution.status = ExecutionStatus.FAILED # Final status is FAILED after compensation attempt
                if not compensation_result:
                     execution.error_message += "; Compensation attempt also failed."
                self.circuit_breaker.record_failure() # Record failure regardless of compensation success

            else:
                # --- Neither order had any fill --- 
                execution.status = ExecutionStatus.FAILED
                execution.error_message = f"Neither order was filled. Long: {long_order.status.name if long_order else 'N/A'}, Short: {short_order.status.name if short_order else 'N/A'}"
                self.circuit_breaker.record_failure()
                logger.error(f"Failed to execute opportunity: {opportunity} - {execution.error_message}")
            
            return execution
            
        except Exception as e:
            # Handle unexpected errors
            logger.error(f"Error executing opportunity: {str(e)}", exc_info=True)
            
            execution.status = ExecutionStatus.FAILED
            execution.error_message = f"Unexpected error: {str(e)}"
            
            # Record failure
            self.circuit_breaker.record_failure()
            
            return execution
        finally:
            # Update execution history
            execution.end_time = datetime.now()
            self.execution_history.append(execution)
            
            # Remove from active executions
            if execution_id in self.active_executions:
                del self.active_executions[execution_id]
            
            # Trim history if needed
            if len(self.execution_history) > self.max_execution_history:
                self.execution_history = self.execution_history[-self.max_execution_history:]
    
    async def _place_order_with_retry(self, 
                                    client: ExchangeAPI,
                                    exchange_id: str,
                                    symbol: str,
                                    side: OrderSide,
                                    order_type: OrderType,
                                    quantity: float,
                                    price: Optional[float] = None) -> Optional[Order]:
        """
        Place an order with retry logic.
        
        Args:
            client: API client
            exchange_id: Exchange identifier
            symbol: Trading symbol
            side: Order side
            order_type: Order type
            quantity: Order quantity
            price: Optional limit price
            
        Returns:
            Order if successful, None otherwise
        """
        for attempt in range(self.max_retries):
            try:
                # Calculate retry delay with exponential backoff
                if attempt > 0:
                    delay = self.retry_delay_base * (2 ** (attempt - 1))
                    logger.info(f"Retrying order placement (attempt {attempt+1}/{self.max_retries}) after {delay:.1f}s")
                    await asyncio.sleep(delay)
                
                # Place the order
                order = await client.place_order(
                    symbol=symbol,
                    side=side,
                    order_type=order_type,
                    quantity=quantity,
                    price=price
                )
                
                # Update portfolio tracker
                self.portfolio_tracker.update_order(exchange_id, order)
                
                return order
                
            except APIError as e: # Catch APIError specifically first
                logger.error(f"API Error placing order on {exchange_id}: {e.message} (Code: {e.code})", exc_info=False) # exc_info=False for brevity unless debugging
                # Check if the error is retryable
                if e.is_retryable and attempt < self.max_retries - 1:
                    # If retryable and more attempts left, continue to next iteration
                    continue 
                else:
                    # If not retryable OR no retries left, LOG the error and RETURN None
                    logger.error(f"Non-retryable API error or max retries reached for order placement on {exchange_id}. Aborting order attempt.")
                    # raise e # DO NOT re-raise, return None
                    return None
            except Exception as e:
                # Catch unexpected non-API errors
                logger.error(f"Unexpected error placing order on {exchange_id}: {str(e)}", exc_info=True)
                # Re-raise unexpected errors immediately? Or return None?
                # Let's return None for consistency, the caller should handle unexpected failures.
                # raise e 
                return None
        
        # All retries failed for a retryable error
        logger.error(f"Order placement failed on {exchange_id} after {self.max_retries} attempts (retryable error).")
        return None
    
    async def _get_order_status(self, 
                               client: ExchangeAPI,
                               exchange_id: str,
                               order_id: str) -> Optional[Order]:
        """
        Get the current status of an order.
        
        Args:
            client: API client
            exchange_id: Exchange identifier
            order_id: Order identifier
            
        Returns:
            Order if successful, None otherwise
        """
        try:
            order = await client.get_order(order_id)
            
            # Update portfolio tracker
            if order:
                self.portfolio_tracker.update_order(exchange_id, order)
            
            return order
            
        except Exception as e:
            logger.error(f"Error getting order status from {exchange_id}: {str(e)}", exc_info=True)
            return None
    
    async def _compensate_position(self,
                                  client: ExchangeAPI,
                                  exchange_id: str,
                                  symbol: str,
                                  original_failed_side: OrderSide, 
                                  quantity: float) -> bool:
        """
        Compensate for a failed trade leg by placing an opposite market order.

        Args:
            client: API client for the exchange where compensation is needed.
            exchange_id: Exchange identifier.
            symbol: Trading symbol.
            original_failed_side: The side (BUY/SELL) of the original order that failed 
                                   or was partially filled, requiring compensation.
            quantity: The quantity that needs to be compensated (e.g., the unfilled amount).

        Returns:
            The compensation Order object if successful, None otherwise.
        """
        try:
            # Determine the side for the compensating order (opposite of the original)
            # compensating_side = OrderSide.BUY if original_failed_side == OrderSide.SELL else OrderSide.SELL # Incorrect logic
            # Correct logic: The compensating order side is the *same* as the side that failed elsewhere
            # If the original SELL failed, we need to SELL the long position we opened.
            # If the original BUY failed, we need to BUY back the short position we opened.
            compensating_side = original_failed_side 
            logger.info(f"Compensating position on {exchange_id} for {symbol}: placing {compensating_side.value} order for {quantity}")
            
            # Place a market order to close/compensate the position
            order = await client.place_order(
                symbol=symbol,
                side=compensating_side, 
                order_type=OrderType.MARKET,
                quantity=quantity
            )

            # Update portfolio tracker with the compensation order
            if order:
                logger.info(f"Compensation order placed successfully: {order.id}")
                self.portfolio_tracker.update_order(exchange_id, order)
                # Return the compensation order object on success
                return order 
            else:
                logger.error(f"Compensation order placement failed for {symbol} on {exchange_id}")
                return None

        except Exception as e:
            logger.error(f"Error compensating position on {exchange_id} for {symbol}: {str(e)}", exc_info=True)
            return None
    
    def get_execution_history(self) -> List[TradeExecution]:
        """
        Get the execution history.
        
        Returns:
            List of trade executions
        """
        return self.execution_history.copy()
    
    def get_active_executions(self) -> List[TradeExecution]:
        """
        Get the currently active executions.
        
        Returns:
            List of active trade executions
        """
        return list(self.active_executions.values())
    
    def reset_circuit_breaker(self):
        """Reset the circuit breaker."""
        self.circuit_breaker.reset()
