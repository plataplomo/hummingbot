import logging
import asyncio
import time
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime
from enum import Enum, auto

from cyberdelta.apis.base import ExchangeAPI
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
            # Execute the trades
            logger.info(f"Executing opportunity: {opportunity}")
            
            # First, place the long order
            long_exchange = opportunity.opportunity.long_exchange
            long_client = self.api_clients[long_exchange]
            
            # Prepare long order parameters
            symbol = opportunity.opportunity.symbol
            side = OrderSide.BUY
            order_type = OrderType.MARKET  # Using market orders for simplicity
            quantity = opportunity.long_size  # In a real implementation, convert from USD to asset quantity
            
            # Place long order with retry
            long_order = await self._place_order_with_retry(
                client=long_client,
                exchange_id=long_exchange,
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity
            )
            
            if not long_order:
                # Failed to place long order
                execution.status = ExecutionStatus.FAILED
                execution.error_message = "Failed to place long order"
                self.circuit_breaker.record_failure()
                return execution
            
            # Store long order details
            execution.long_order_id = long_order.id
            execution.long_order_response = long_order.to_dict()
            
            # Now place the short order
            short_exchange = opportunity.opportunity.short_exchange
            short_client = self.api_clients[short_exchange]
            
            # Prepare short order parameters
            side = OrderSide.SELL
            quantity = opportunity.short_size  # In a real implementation, convert from USD to asset quantity
            
            # Place short order with retry
            short_order = await self._place_order_with_retry(
                client=short_client,
                exchange_id=short_exchange,
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=quantity
            )
            
            if not short_order:
                # Failed to place short order, need to reverse the long order
                execution.status = ExecutionStatus.COMPENSATING
                execution.error_message = "Failed to place short order, compensating"
                logger.warning(f"Failed to place short order, compensating by closing long position")
                
                # Attempt to close the long position
                await self._compensate_position(
                    client=long_client,
                    exchange_id=long_exchange,
                    symbol=symbol,
                    side=OrderSide.SELL,  # Opposite of the long order
                    quantity=quantity
                )
                
                # Record the failure
                self.circuit_breaker.record_failure()
                
                # Mark execution as failed
                execution.status = ExecutionStatus.FAILED
                return execution
            
            # Store short order details
            execution.short_order_id = short_order.id
            execution.short_order_response = short_order.to_dict()
            
            # Wait for orders to settle
            await asyncio.sleep(2)  # In a real implementation, wait for order updates
            
            # Update order status
            long_order = await self._get_order_status(long_client, long_exchange, execution.long_order_id)
            short_order = await self._get_order_status(short_client, short_exchange, execution.short_order_id)
            
            # Check if orders were filled
            long_filled = long_order and long_order.status == OrderStatus.FILLED
            short_filled = short_order and short_order.status == OrderStatus.FILLED
            
            if long_filled and short_filled:
                # Both orders were filled
                execution.status = ExecutionStatus.COMPLETED
                execution.long_fill_price = long_order.price
                execution.short_fill_price = short_order.price
                execution.long_fill_quantity = long_order.filled_quantity
                execution.short_fill_quantity = short_order.filled_quantity
                
                # Record success
                self.circuit_breaker.record_success()
                
                logger.info(f"Successfully executed opportunity: {opportunity}")
            elif long_filled or short_filled:
                # Only one order was filled
                execution.status = ExecutionStatus.PARTIALLY_COMPLETED
                execution.error_message = "Only one order was filled"
                
                if long_filled:
                    execution.long_fill_price = long_order.price
                    execution.long_fill_quantity = long_order.filled_quantity
                
                if short_filled:
                    execution.short_fill_price = short_order.price
                    execution.short_fill_quantity = short_order.filled_quantity
                
                # Record partial failure
                self.circuit_breaker.record_failure(opportunity.expected_profit * 0.5)  # Estimate loss as half of expected profit
                
                logger.warning(f"Partially executed opportunity: {opportunity}")
            else:
                # Neither order was filled
                execution.status = ExecutionStatus.FAILED
                execution.error_message = "Neither order was filled"
                
                # Record failure
                self.circuit_breaker.record_failure()
                
                logger.error(f"Failed to execute opportunity: {opportunity}")
            
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
                
            except Exception as e:
                logger.error(f"Error placing order on {exchange_id}: {str(e)}", exc_info=True)
                
                # Continue to retry on temporary errors
                if "rate limit" in str(e).lower() or "timeout" in str(e).lower():
                    continue
                    
                # Break on permanent errors
                if "insufficient balance" in str(e).lower() or "invalid parameter" in str(e).lower():
                    break
        
        # All retries failed
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
                                  side: OrderSide,
                                  quantity: float) -> bool:
        """
        Compensate for a failed trade by placing an opposite order.
        
        Args:
            client: API client
            exchange_id: Exchange identifier
            symbol: Trading symbol
            side: Order side (should be opposite of the original order)
            quantity: Order quantity
            
        Returns:
            True if compensation was successful, False otherwise
        """
        try:
            # Place a market order to close the position
            order = await client.place_order(
                symbol=symbol,
                side=side,
                order_type=OrderType.MARKET,
                quantity=quantity
            )
            
            # Update portfolio tracker
            if order:
                self.portfolio_tracker.update_order(exchange_id, order)
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error compensating position on {exchange_id}: {str(e)}", exc_info=True)
            return False
    
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
