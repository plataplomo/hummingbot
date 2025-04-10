# Atomic Execution Design

**Status: Design Complete - Implementation/Testing DEFERRED (Revised Aug 6, 2025)**

**Note:** Based on critic feedback prioritizing foundational stability and testing, the implementation and rigorous testing of atomic execution guarantees described below are **deferred** for Prototype 0.0.1. The immediate focus is on stabilizing and testing the core execution path with basic order handling, robust safety systems, and clear alerting/manual intervention for execution failures, rather than complex automated rollbacks.

## Overview

This document outlines the design for implementing atomic execution patterns for cross-exchange trades in the CyberDeltaEngine. Building on the previously implemented signal priority queue, this component will focus on reliable execution of arbitrage opportunities across multiple exchanges while minimizing legging risk and handling error conditions gracefully.

## Design Goals

1. **Minimize Legging Risk**: Reduce the risk of having one leg of a trade execute while the other fails
2. **Handle Partial Fills**: Implement strategies for dealing with partially filled orders
3. **Ensure Atomicity**: Make cross-exchange trades as atomic as possible
4. **Integrate Safety Systems**: Utilize circuit breakers and position reconciliation
5. **Support Recovery**: Provide mechanisms for recovering from failed executions

## Architecture

### Component Structure

The atomic execution system will consist of the following components:

1. **AtomicExecutionEngine**: Central coordinator for atomic trade execution
2. **ExchangeAdapter**: Exchange-specific implementations for standardized operations
3. **TransactionCoordinator**: Manages the sequencing and synchronization of operations
4. **ExecutionMonitor**: Tracks execution status and handles timeouts
5. **CompensationManager**: Handles unwinding failed or partial trades

```
┌─────────────────────────────────┐
│      AtomicExecutionEngine      │
├─────────────────────────────────┤
│ - Coordinates execution flow    │
│ - Handles execution requests    │
│ - Manages execution lifecycle   │
└───────────────┬─────────────────┘
                │
                ▼
┌─────────────────────────────────┐
│      TransactionCoordinator     │
├─────────────────────────────────┤
│ - Sequences operations          │
│ - Synchronizes execution        │
│ - Manages transaction state     │
└┬──────────────┬─────────────────┘
 │              │
 ▼              ▼
┌───────────┐  ┌───────────────────┐
│ExchangeA  │  │    ExchangeB      │
│  Adapter  │  │     Adapter       │
└───────────┘  └───────────────────┘
      │                  │
      ▼                  ▼
┌─────────────────────────────────┐
│       ExecutionMonitor          │
├─────────────────────────────────┤
│ - Tracks execution status       │
│ - Handles timeouts              │
│ - Collects metrics              │
└───────────────┬─────────────────┘
                │
                ▼
┌─────────────────────────────────┐
│      CompensationManager        │
├─────────────────────────────────┤
│ - Handles partial fills         │
│ - Unwinds failed trades         │
│ - Records compensations         │
└─────────────────────────────────┘
```

## Execution Strategies

We will implement multiple execution strategies that can be selected based on market conditions and risk profile:

### 1. Simultaneous Execution

- Place all trade legs simultaneously
- Monitor status with tight timeouts
- Handle partial fills through compensation
- High throughput but highest legging risk

### 2. Sequential with Lock-in

- Execute less liquid leg first
- Verify execution before proceeding
- Execute more liquid leg with wider price acceptance
- Lower legging risk but may have more slippage

### 3. Two-Phase Commit

- Pre-validate all conditions on both exchanges
- Lock in execution parameters
- Execute in quick succession
- Verify and commit or compensate
- Best atomic properties but higher complexity

## Implementation Details

### AtomicExecutionEngine

```python
class AtomicExecutionEngine:
    """
    Coordinates atomic execution of cross-exchange trades.
    """
    
    def __init__(self, 
                 config: Dict[str, Any], 
                 exchange_adapters: Dict[str, ExchangeAdapter],
                 circuit_breaker_system,
                 position_reconciliation_system):
        """Initialize the execution engine."""
        self.config = config
        self.exchange_adapters = exchange_adapters
        self.circuit_breaker_system = circuit_breaker_system
        self.position_reconciliation_system = position_reconciliation_system
        
        self.transaction_coordinator = TransactionCoordinator(config)
        self.execution_monitor = ExecutionMonitor(config)
        self.compensation_manager = CompensationManager(config)
        
        # Configuration parameters
        self.execution_strategy = config.get("execution.strategy", "sequential_lock_in")
        self.max_execution_time = config.get("execution.max_execution_time", 10.0)  # seconds
        self.partial_fill_threshold = config.get("execution.partial_fill_threshold", 0.95)  # 95%
        
    async def execute_opportunity(self, 
                                  opportunity: ArbitrageOpportunity, 
                                  signal: TradeSignal) -> ExecutionResult:
        """
        Execute a cross-exchange arbitrage opportunity atomically.
        
        Args:
            opportunity: The arbitrage opportunity to execute
            signal: The trade signal from the priority queue
            
        Returns:
            Execution result with details
        """
        # Check if both exchanges are available
        if not self._check_exchanges_available(opportunity):
            return ExecutionResult(status=ExecutionStatus.REJECTED, error="Exchanges unavailable")
            
        # Start execution
        execution_id = self._generate_execution_id(opportunity)
        execution_result = ExecutionResult(
            execution_id=execution_id,
            opportunity=opportunity,
            status=ExecutionStatus.PENDING
        )
        
        # Select execution strategy
        if self.execution_strategy == "simultaneous":
            result = await self._execute_simultaneous(opportunity, execution_id)
        elif self.execution_strategy == "sequential_lock_in":
            result = await self._execute_sequential_lock_in(opportunity, execution_id)
        elif self.execution_strategy == "two_phase_commit":
            result = await self._execute_two_phase_commit(opportunity, execution_id)
        else:
            # Default to sequential lock-in
            result = await self._execute_sequential_lock_in(opportunity, execution_id)
            
        # Update execution result
        execution_result.update(result)
        
        # Verify position reconciliation if execution was successful
        if execution_result.status == ExecutionStatus.COMPLETED:
            await self._verify_positions(opportunity, execution_result)
            
        return execution_result
```

### TransactionCoordinator

```python
class TransactionCoordinator:
    """
    Coordinates the execution of multi-exchange transactions.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the transaction coordinator."""
        self.config = config
        self.active_transactions = {}
        
    async def start_transaction(self, execution_id: str, opportunity: ArbitrageOpportunity) -> TransactionContext:
        """
        Start a new transaction for atomic execution.
        
        Args:
            execution_id: Unique execution identifier
            opportunity: Arbitrage opportunity to execute
            
        Returns:
            Transaction context to be used throughout execution
        """
        # Create transaction context with execution details
        context = TransactionContext(
            execution_id=execution_id,
            opportunity=opportunity,
            start_time=datetime.now(),
            status=TransactionStatus.STARTING
        )
        
        # Store in active transactions
        self.active_transactions[execution_id] = context
        
        return context
        
    async def commit_transaction(self, context: TransactionContext) -> bool:
        """
        Commit a transaction after successful execution.
        
        Args:
            context: Transaction context
            
        Returns:
            True if committed successfully, False otherwise
        """
        # Update transaction status
        context.status = TransactionStatus.COMMITTED
        context.end_time = datetime.now()
        
        # Remove from active transactions after a timeout
        # (keeping it around briefly for monitoring)
        async def cleanup():
            await asyncio.sleep(self.config.get("execution.transaction_cleanup_delay", 60))
            if execution_id in self.active_transactions:
                del self.active_transactions[execution_id]
                
        asyncio.create_task(cleanup())
        
        return True
        
    async def abort_transaction(self, context: TransactionContext, reason: str) -> bool:
        """
        Abort a transaction due to failure.
        
        Args:
            context: Transaction context
            reason: Reason for aborting
            
        Returns:
            True if aborted successfully, False otherwise
        """
        # Update transaction status
        context.status = TransactionStatus.ABORTED
        context.end_time = datetime.now()
        context.abort_reason = reason
        
        # Remove from active transactions after a timeout
        async def cleanup():
            await asyncio.sleep(self.config.get("execution.transaction_cleanup_delay", 60))
            if execution_id in self.active_transactions:
                del self.active_transactions[execution_id]
                
        asyncio.create_task(cleanup())
        
        return True
```

### Sequential Lock-in Strategy Implementation

```python
async def _execute_sequential_lock_in(self, 
                                    opportunity: ArbitrageOpportunity, 
                                    execution_id: str) -> Dict[str, Any]:
    """
    Execute using sequential lock-in strategy.
    
    Args:
        opportunity: Arbitrage opportunity
        execution_id: Unique execution identifier
        
    Returns:
        Execution details
    """
    # Start transaction
    context = await self.transaction_coordinator.start_transaction(execution_id, opportunity)
    
    try:
        # Determine order of execution (less liquid first)
        first_exchange, second_exchange = self._determine_execution_order(opportunity)
        
        # Get adapters
        first_adapter = self.exchange_adapters[first_exchange]
        second_adapter = self.exchange_adapters[second_exchange]
        
        # Prepare orders
        first_order = self._prepare_order(opportunity, first_exchange)
        second_order = self._prepare_order(opportunity, second_exchange)
        
        # Execute first leg with tight price constraints
        first_result = await first_adapter.place_order(first_order)
        
        # Start monitoring execution
        monitor_task = asyncio.create_task(
            self.execution_monitor.monitor_execution(first_exchange, first_result.order_id)
        )
        
        # Wait for first order to complete or timeout
        try:
            first_fill = await asyncio.wait_for(
                monitor_task, 
                timeout=self.config.get("execution.first_leg_timeout", 5.0)
            )
        except asyncio.TimeoutError:
            # Handle timeout - check if order was filled
            first_fill = await first_adapter.get_order_status(first_result.order_id)
            
            if not first_fill or first_fill.status != OrderStatus.FILLED:
                # First leg failed - abort transaction
                await self.transaction_coordinator.abort_transaction(
                    context, "First leg execution timeout"
                )
                
                # Try to cancel the order if it's still open
                if first_fill and first_fill.status == OrderStatus.NEW:
                    await first_adapter.cancel_order(first_result.order_id)
                    
                return {
                    "status": ExecutionStatus.FAILED,
                    "error": "First leg execution timeout",
                    "first_exchange": first_exchange,
                    "first_order_id": first_result.order_id,
                    "first_fill": first_fill.to_dict() if first_fill else None
                }
        
        # First leg succeeded - execute second leg with wider price acceptance
        # Adjust second order parameters based on first fill
        second_order = self._adjust_second_leg(
            second_order, 
            first_fill, 
            opportunity
        )
        
        # Execute second leg
        second_result = await second_adapter.place_order(second_order)
        
        # Monitor second leg execution
        try:
            second_fill = await asyncio.wait_for(
                self.execution_monitor.monitor_execution(
                    second_exchange, second_result.order_id
                ),
                timeout=self.config.get("execution.second_leg_timeout", 5.0)
            )
        except asyncio.TimeoutError:
            # Handle timeout - check if order was filled
            second_fill = await second_adapter.get_order_status(second_result.order_id)
            
            if not second_fill or second_fill.status != OrderStatus.FILLED:
                # Second leg failed - initiate compensation
                await self.transaction_coordinator.abort_transaction(
                    context, "Second leg execution timeout"
                )
                
                # Try to cancel the order if it's still open
                if second_fill and second_fill.status == OrderStatus.NEW:
                    await second_adapter.cancel_order(second_result.order_id)
                
                # Compensate the first leg
                compensation_result = await self.compensation_manager.compensate_leg(
                    first_exchange,
                    first_fill,
                    opportunity
                )
                
                return {
                    "status": ExecutionStatus.COMPENSATED,
                    "error": "Second leg execution timeout",
                    "first_exchange": first_exchange,
                    "first_order_id": first_result.order_id,
                    "first_fill": first_fill.to_dict(),
                    "second_exchange": second_exchange,
                    "second_order_id": second_result.order_id,
                    "second_fill": second_fill.to_dict() if second_fill else None,
                    "compensation_result": compensation_result
                }
        
        # Both legs executed - check for partial fills
        if self._is_partial_fill(first_fill, second_fill, opportunity):
            # Handle partial fill
            adjustment_result = await self.compensation_manager.adjust_for_partial_fill(
                first_exchange, first_fill,
                second_exchange, second_fill,
                opportunity
            )
            
            # Commit transaction with partial fill
            await self.transaction_coordinator.commit_transaction(context)
            
            return {
                "status": ExecutionStatus.PARTIALLY_COMPLETED,
                "first_exchange": first_exchange,
                "first_order_id": first_result.order_id,
                "first_fill": first_fill.to_dict(),
                "second_exchange": second_exchange,
                "second_order_id": second_result.order_id,
                "second_fill": second_fill.to_dict(),
                "adjustment_result": adjustment_result
            }
        
        # Successful execution
        await self.transaction_coordinator.commit_transaction(context)
        
        return {
            "status": ExecutionStatus.COMPLETED,
            "first_exchange": first_exchange,
            "first_order_id": first_result.order_id,
            "first_fill": first_fill.to_dict(),
            "second_exchange": second_exchange,
            "second_order_id": second_result.order_id,
            "second_fill": second_fill.to_dict()
        }
        
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Error in sequential lock-in execution: {str(e)}", exc_info=True)
        
        # Abort transaction
        await self.transaction_coordinator.abort_transaction(context, f"Execution error: {str(e)}")
        
        # Try to compensate if possible
        compensation_result = None
        if 'first_fill' in locals() and first_fill:
            compensation_result = await self.compensation_manager.compensate_leg(
                first_exchange,
                first_fill,
                opportunity
            )
        
        return {
            "status": ExecutionStatus.FAILED,
            "error": f"Execution error: {str(e)}",
            "compensation_result": compensation_result
        }
```

## Exchange Adapter Interface

To standardize interactions with different exchanges, we'll define an ExchangeAdapter interface:

```python
class ExchangeAdapter:
    """
    Interface for exchange-specific operations.
    """
    
    def __init__(self, config: Dict[str, Any], exchange_id: str):
        """Initialize the exchange adapter."""
        self.config = config
        self.exchange_id = exchange_id
        
    async def place_order(self, order: Order) -> OrderResult:
        """
        Place an order on the exchange.
        
        Args:
            order: Order to place
            
        Returns:
            Order result
        """
        raise NotImplementedError("Subclasses must implement place_order")
        
    async def cancel_order(self, order_id: str) -> CancelResult:
        """
        Cancel an order.
        
        Args:
            order_id: Order ID to cancel
            
        Returns:
            Cancel result
        """
        raise NotImplementedError("Subclasses must implement cancel_order")
        
    async def get_order_status(self, order_id: str) -> OrderStatus:
        """
        Get the status of an order.
        
        Args:
            order_id: Order ID to check
            
        Returns:
            Order status
        """
        raise NotImplementedError("Subclasses must implement get_order_status")
        
    async def get_market_data(self, symbol: str) -> MarketData:
        """
        Get market data for a symbol.
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Market data
        """
        raise NotImplementedError("Subclasses must implement get_market_data")
```

## Compensation Strategies

The CompensationManager will implement various strategies for handling failed or partial executions:

1. **Direct Reversal**: Place an opposite order on the same exchange to neutralize the position
2. **Cross-Exchange Hedge**: Place a compensating order on a different exchange if the original is unavailable
3. **Partial Adjustment**: Adjust the position size on one exchange to match the partially filled order on the other

```python
class CompensationManager:
    """
    Manages compensation for failed or partial executions.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize the compensation manager."""
        self.config = config
        
    async def compensate_leg(self, 
                           exchange: str, 
                           fill: OrderFill, 
                           opportunity: ArbitrageOpportunity) -> CompensationResult:
        """
        Compensate for a filled leg when the other leg failed.
        
        Args:
            exchange: Exchange where the leg was executed
            fill: Fill information
            opportunity: Original arbitrage opportunity
            
        Returns:
            Compensation result
        """
        # Create compensating order (opposite side)
        compensating_order = Order(
            exchange=exchange,
            symbol=fill.symbol,
            side=self._get_opposite_side(fill.side),
            quantity=fill.filled_quantity,
            order_type=OrderType.MARKET,
            price=0.0,  # Market order
            client_order_id=f"comp_{fill.order_id}"
        )
        
        # Place compensating order
        adapter = self.exchange_adapters[exchange]
        result = await adapter.place_order(compensating_order)
        
        # Monitor compensation execution
        try:
            comp_fill = await asyncio.wait_for(
                self.execution_monitor.monitor_execution(
                    exchange, result.order_id
                ),
                timeout=self.config.get("execution.compensation_timeout", 5.0)
            )
        except asyncio.TimeoutError:
            # Check order status directly
            comp_fill = await adapter.get_order_status(result.order_id)
            
        # Return compensation result
        return {
            "original_exchange": exchange,
            "original_order_id": fill.order_id,
            "original_fill": fill.to_dict(),
            "compensation_order_id": result.order_id,
            "compensation_fill": comp_fill.to_dict() if comp_fill else None,
            "success": comp_fill and comp_fill.status == OrderStatus.FILLED
        }
    
    async def adjust_for_partial_fill(self,
                                    first_exchange: str,
                                    first_fill: OrderFill,
                                    second_exchange: str,
                                    second_fill: OrderFill,
                                    opportunity: ArbitrageOpportunity) -> AdjustmentResult:
        """
        Adjust for partial fills on one or both legs.
        
        Args:
            first_exchange: First exchange
            first_fill: First fill information
            second_exchange: Second exchange
            second_fill: Second fill information
            opportunity: Original arbitrage opportunity
            
        Returns:
            Adjustment result
        """
        # Calculate adjustment needed
        adjustment_needed = abs(first_fill.filled_quantity - second_fill.filled_quantity)
        
        # Determine which side needs adjustment
        if first_fill.filled_quantity > second_fill.filled_quantity:
            # First leg filled more - compensate on first exchange
            exchange_to_adjust = first_exchange
            fill_to_adjust = first_fill
            adjustment_side = self._get_opposite_side(first_fill.side)
        else:
            # Second leg filled more - compensate on second exchange
            exchange_to_adjust = second_exchange
            fill_to_adjust = second_fill
            adjustment_side = self._get_opposite_side(second_fill.side)
            
        # Create adjustment order
        adjustment_order = Order(
            exchange=exchange_to_adjust,
            symbol=fill_to_adjust.symbol,
            side=adjustment_side,
            quantity=adjustment_needed,
            order_type=OrderType.MARKET,
            price=0.0,  # Market order
            client_order_id=f"adj_{fill_to_adjust.order_id}"
        )
        
        # Place adjustment order
        adapter = self.exchange_adapters[exchange_to_adjust]
        result = await adapter.place_order(adjustment_order)
        
        # Monitor adjustment execution
        try:
            adj_fill = await asyncio.wait_for(
                self.execution_monitor.monitor_execution(
                    exchange_to_adjust, result.order_id
                ),
                timeout=self.config.get("execution.adjustment_timeout", 5.0)
            )
        except asyncio.TimeoutError:
            # Check order status directly
            adj_fill = await adapter.get_order_status(result.order_id)
            
        # Return adjustment result
        return {
            "adjustment_exchange": exchange_to_adjust,
            "adjustment_order_id": result.order_id,
            "adjustment_fill": adj_fill.to_dict() if adj_fill else None,
            "adjustment_quantity": adjustment_needed,
            "success": adj_fill and adj_fill.status == OrderStatus.FILLED
        }
```

## Integration with Safety Systems

### Circuit Breaker Integration

```python
def _check_exchanges_available(self, opportunity: ArbitrageOpportunity) -> bool:
    """
    Check if both exchanges are available according to circuit breakers.
    
    Args:
        opportunity: Arbitrage opportunity
        
    Returns:
        True if both exchanges are available, False otherwise
    """
    long_exchange = opportunity.long_exchange
    short_exchange = opportunity.short_exchange
    
    # Check exchange circuit breakers
    long_available = self.circuit_breaker_system.check_exchange(long_exchange)
    short_available = self.circuit_breaker_system.check_exchange(short_exchange)
    
    # Check symbol circuit breakers
    symbol_available = self.circuit_breaker_system.check_symbol(opportunity.symbol)
    
    return long_available and short_available and symbol_available
```

### Position Reconciliation Integration

```python
async def _verify_positions(self, 
                           opportunity: ArbitrageOpportunity, 
                           execution_result: ExecutionResult) -> None:
    """
    Verify positions after execution using position reconciliation.
    
    Args:
        opportunity: Arbitrage opportunity
        execution_result: Execution result
    """
    # Allow some time for exchange positions to update
    await asyncio.sleep(self.config.get("execution.position_verification_delay", 2.0))
    
    # Verify positions on both exchanges
    long_exchange = opportunity.long_exchange
    short_exchange = opportunity.short_exchange
    
    # Get position details from execution result
    long_position = {
        "symbol": opportunity.symbol,
        "size": execution_result.first_fill["filled_quantity"] if execution_result.first_exchange == long_exchange else execution_result.second_fill["filled_quantity"],
        "side": "LONG"
    }
    
    short_position = {
        "symbol": opportunity.symbol,
        "size": execution_result.first_fill["filled_quantity"] if execution_result.first_exchange == short_exchange else execution_result.second_fill["filled_quantity"],
        "side": "SHORT"
    }
    
    # Verify positions with reconciliation system
    long_reconciliation = await self.position_reconciliation_system.verify_position(
        long_exchange, long_position
    )
    
    short_reconciliation = await self.position_reconciliation_system.verify_position(
        short_exchange, short_position
    )
    
    # Update execution result with reconciliation results
    execution_result.position_verification = {
        "long_exchange": long_exchange,
        "long_verification": long_reconciliation,
        "short_exchange": short_exchange,
        "short_verification": short_reconciliation
    }
    
    # Handle significant discrepancies
    if not long_reconciliation["verified"] or not short_reconciliation["verified"]:
        # Log warning for manual review
        logger.warning(
            f"Position verification failed for execution {execution_result.execution_id}. "
            f"Long discrepancy: {long_reconciliation.get('discrepancy')}, "
            f"Short discrepancy: {short_reconciliation.get('discrepancy')}"
        )
        
        # Trigger automatic reconciliation if configured
        if self.config.get("execution.auto_reconcile_positions", True):
            await self.position_reconciliation_system.reconcile_position(
                long_exchange, long_position, long_reconciliation
            )
            
            await self.position_reconciliation_system.reconcile_position(
                short_exchange, short_position, short_reconciliation
            )
```

## Implementation Plan

1. **Create Base Classes**
   - Implement the AtomicExecutionEngine
   - Implement the TransactionCoordinator
   - Define the ExchangeAdapter interface

2. **Implement Exchange Adapters**
   - Create adapters for supported exchanges (Hyperliquid, Backpack)
   - Implement exchange-specific order types and parameters
   - Add support for monitoring order status

3. **Implement Execution Strategies**
   - Start with Sequential Lock-In strategy
   - Add Simultaneous Execution strategy
   - Implement Two-Phase Commit strategy

4. **Implement Compensation Mechanisms**
   - Create the CompensationManager
   - Implement different compensation strategies
   - Add support for partial fills

5. **Safety System Integration**
   - Integrate with the CircuitBreakerSystem
   - Connect to the PositionReconciliationSystem
   - Add reporting for execution failures

6. **Metrics and Monitoring**
   - Add detailed logging for executions
   - Implement performance metrics
   - Create alerts for failed executions

## Testing Strategy

1. **Unit Tests**
   - Test individual components in isolation
   - Mock exchange API responses for reproducible tests
   - Test error handling and compensation logic

2. **Integration Tests**
   - Test interaction between components
   - Verify correct transaction state management
   - Verify circuit breaker and position reconciliation integration

3. **Simulation Tests**
   - Test with simulated exchanges and latency
   - Simulate partial fills and execution failures
   - Measure performance metrics in simulated environments

4. **End-to-End Tests**
   - Test with exchange testnet where available
   - Verify full execution flow with small amounts
   - Measure execution latency and success rates

## Success Criteria

The implementation will be considered successful when:

1. Cross-exchange trades are executed reliably with minimal legging risk
2. Partial fills and failures are handled gracefully with appropriate compensation
3. Integration with safety systems prevents catastrophic failures
4. Performance metrics show acceptable latency and success rates
5. The system is testable and maintainable

## Conclusion

This design provides a robust framework for atomic execution of cross-exchange trades that minimizes risk while maximizing execution success rates. By implementing multiple execution strategies and integrating with safety systems, the engine will provide reliable execution of arbitrage opportunities identified by the signal priority queue. 