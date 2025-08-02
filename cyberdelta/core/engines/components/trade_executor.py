"""Portfolio-aware trade execution with comprehensive integration."""
from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal
from datetime import datetime, timezone
from typing import Any, Optional
from dataclasses import dataclass
from enum import Enum

from cyberdelta.core.portfolio.coordinators.portfolio_risk_coordinator import PortfolioRiskCoordinator
from cyberdelta.core.symbols import Symbol
from cyberdelta.core.portfolio.services.pricing.price_service import PriceDataService


class ExecutionStatus(str, Enum):
    """Trade execution status."""
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ExecutionResult:
    """Trade execution result."""
    execution_id: str
    status: ExecutionStatus
    success: bool
    quantity_requested: Decimal
    quantity_filled: Decimal
    avg_fill_price: Optional[Decimal]
    fees: Decimal
    slippage: Decimal
    execution_time_ms: int
    portfolio_impact: dict[str, Any]
    error_message: Optional[str] = None
    metadata: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.metadata is None:
            self.metadata = {}


class PortfolioAwareTradeExecutor:
    """Trade executor with complete portfolio integration."""

    def __init__(self, coordinator: PortfolioRiskCoordinator, price_service: PriceDataService | None = None):
        self.coordinator = coordinator
        self.price_service = price_service

        # Execution parameters
        self.max_slippage = Decimal("0.005")  # 0.5% max slippage
        self.execution_timeout = 30  # 30 seconds
        self.max_retry_attempts = 3
        self.partial_fill_threshold = Decimal("0.8")  # 80% minimum fill

        # Execution tracking
        self.active_executions: dict[str, dict[str, Any]] = {}
        self.execution_history: list[ExecutionResult] = []
        self.execution_stats = {
            "total_executions": 0,
            "successful_executions": 0,
            "avg_execution_time": 0.0,
            "avg_slippage": Decimal("0"),
            "total_fees": Decimal("0")
        }

    async def initialize(self) -> None:
        """Initialize the trade executor."""
        # Load execution configuration
        await self._load_execution_config()

        # Initialize exchange connections (placeholder)
        await self._initialize_exchange_connections()

    async def execute_trade(self, trade_request: dict[str, Any]) -> dict[str, Any]:
        """Execute a trade with portfolio context."""
        execution_id = str(uuid.uuid4())
        start_time = datetime.now(timezone.utc)

        try:
            # Extract trade details
            signal = trade_request["signal"]
            position_size = trade_request["position_size"]
            portfolio_context = trade_request["portfolio_context"]
            risk_parameters = trade_request["risk_parameters"]

            # Create execution tracking
            execution_record = {
                "execution_id": execution_id,
                "signal": signal,
                "position_size": position_size,
                "start_time": start_time,
                "status": ExecutionStatus.PENDING,
                "risk_parameters": risk_parameters
            }
            self.active_executions[execution_id] = execution_record

            # Pre-execution validation
            validation_result = await self._validate_execution_request(
                signal, position_size, portfolio_context, risk_parameters
            )
            if not validation_result["valid"]:
                return await self._handle_execution_failure(
                    execution_id, validation_result["reason"]
                )

            # Update status to executing
            execution_record["status"] = ExecutionStatus.EXECUTING

            # Execute the trade
            execution_result = await self._execute_trade_with_retry(
                signal, position_size, risk_parameters
            )

            # Update portfolio state through coordinator
            portfolio_impact = await self._update_portfolio_state(
                execution_result, signal, portfolio_context
            )

            # Create final result
            final_result = ExecutionResult(
                execution_id=execution_id,
                status=ExecutionStatus.COMPLETED if execution_result["success"] else ExecutionStatus.FAILED,
                success=execution_result["success"],
                quantity_requested=position_size,
                quantity_filled=execution_result["quantity_filled"],
                avg_fill_price=execution_result.get("avg_fill_price"),
                fees=execution_result.get("fees", Decimal("0")),
                slippage=execution_result.get("slippage", Decimal("0")),
                execution_time_ms=int((datetime.now(timezone.utc) - start_time).total_seconds() * 1000),
                portfolio_impact=portfolio_impact,
                metadata=execution_result.get("metadata", {})
            )

            # Update execution tracking
            await self._update_execution_stats(final_result)
            self.execution_history.append(final_result)

            # Clean up active execution
            del self.active_executions[execution_id]

            return {
                "execution_id": execution_id,
                "success": final_result.success,
                "status": final_result.status,
                "quantity_filled": final_result.quantity_filled,
                "portfolio_impact": portfolio_impact,
                "execution_time_ms": final_result.execution_time_ms,
                "fees": final_result.fees,
                "slippage": final_result.slippage
            }

        except Exception as e:
            return await self._handle_execution_failure(execution_id, str(e))

    async def cancel_execution(self, execution_id: str) -> dict[str, Any]:
        """Cancel an active execution."""
        if execution_id not in self.active_executions:
            return {"success": False, "reason": "Execution not found"}

        execution_record = self.active_executions[execution_id]

        if execution_record["status"] != ExecutionStatus.EXECUTING:
            return {"success": False, "reason": "Execution cannot be cancelled"}

        try:
            # Cancel the order (placeholder for exchange cancellation)
            cancel_result = await self._cancel_order_on_exchange(execution_record)

            # Update status
            execution_record["status"] = ExecutionStatus.CANCELLED

            return {
                "success": True,
                "execution_id": execution_id,
                "cancelled_quantity": cancel_result.get("cancelled_quantity", Decimal("0"))
            }

        except Exception as e:
            return {"success": False, "reason": str(e)}

    async def get_execution_status(self, execution_id: str) -> Optional[dict[str, Any]]:
        """Get status of an execution."""
        if execution_id in self.active_executions:
            record = self.active_executions[execution_id]
            return {
                "execution_id": execution_id,
                "status": record["status"],
                "symbol": record["signal"].symbol,
                "position_size": record["position_size"],
                "start_time": record["start_time"],
                "elapsed_time": (datetime.now(timezone.utc) - record["start_time"]).total_seconds()
            }

        # Check execution history
        for result in self.execution_history:
            if result.execution_id == execution_id:
                return {
                    "execution_id": execution_id,
                    "status": result.status,
                    "success": result.success,
                    "quantity_filled": result.quantity_filled,
                    "execution_time_ms": result.execution_time_ms
                }

        return None

    async def get_execution_stats(self) -> dict[str, Any]:
        """Get execution statistics."""
        return {
            "total_executions": self.execution_stats["total_executions"],
            "successful_executions": self.execution_stats["successful_executions"],
            "success_rate": (
                float(str(self.execution_stats["successful_executions"])) / max(1, float(str(self.execution_stats["total_executions"])))
            ),
            "avg_execution_time_ms": self.execution_stats["avg_execution_time"],
            "avg_slippage": self.execution_stats["avg_slippage"],
            "total_fees": self.execution_stats["total_fees"],
            "active_executions": len(self.active_executions)
        }

    async def _validate_execution_request(
        self,
        signal: Any,  # TradingSignal
        position_size: Decimal,
        portfolio_context: dict[str, Any],
        risk_parameters: dict[str, Any]
    ) -> dict[str, Any]:
        """Validate execution request."""

        # Check position size
        if position_size <= 0:
            return {"valid": False, "reason": "Invalid position size"}

        # Check available capital
        available_capital = portfolio_context.get("available_capital", Decimal("0"))
        if position_size > available_capital:
            return {"valid": False, "reason": "Insufficient available capital"}

        # Check max slippage parameter
        max_slippage = risk_parameters.get("max_slippage", self.max_slippage)
        if max_slippage > Decimal("0.05"):  # 5% max slippage sanity check
            return {"valid": False, "reason": "Slippage tolerance too high"}

        # Check symbol validity (placeholder)
        if not signal.symbol or len(signal.symbol.value) < 3:
            return {"valid": False, "reason": "Invalid symbol"}

        return {"valid": True}

    async def _execute_trade_with_retry(
        self,
        signal: Any,  # TradingSignal
        position_size: Decimal,
        risk_parameters: dict[str, Any]
    ) -> dict[str, Any]:
        """Execute trade with retry logic."""

        max_attempts = risk_parameters.get("retry_attempts", self.max_retry_attempts)
        timeout = risk_parameters.get("timeout_seconds", self.execution_timeout)
        max_slippage = risk_parameters.get("max_slippage", self.max_slippage)

        for attempt in range(max_attempts):
            try:
                # Execute on appropriate exchange
                result = await asyncio.wait_for(
                    self._execute_on_exchange(signal, position_size, max_slippage),
                    timeout=timeout
                )

                if result["success"]:
                    return result

                # If not successful but no exception, prepare for retry
                if attempt < max_attempts - 1:
                    await asyncio.sleep(0.5 * (attempt + 1))  # Exponential backoff

            except asyncio.TimeoutError:
                if attempt == max_attempts - 1:
                    return {
                        "success": False,
                        "reason": "Execution timeout",
                        "quantity_filled": Decimal("0")
                    }
            except Exception as e:
                if attempt == max_attempts - 1:
                    return {
                        "success": False,
                        "reason": str(e),
                        "quantity_filled": Decimal("0")
                    }

        return {
            "success": False,
            "reason": "Max retry attempts exceeded",
            "quantity_filled": Decimal("0")
        }

    async def _execute_on_exchange(
        self,
        signal: Any,  # TradingSignal
        position_size: Decimal,
        max_slippage: Decimal
    ) -> dict[str, Any]:
        """Execute trade on the appropriate exchange."""
        
        # Get current market price from coordinator's risk assessment
        portfolio_with_risk = await self.coordinator.get_current_portfolio_with_risk_assessment()
        
        # Determine exchange from signal
        exchange = getattr(signal, "exchange", "hyperliquid")  # Default to hyperliquid
        
        # Get market price (would come from market data service in production)
        # For now, use a reasonable estimate based on symbol
        market_price = await self._get_market_price(signal.symbol, exchange)
        
        # Calculate expected slippage based on order size and market conditions
        # Larger orders have more slippage
        size_impact = position_size / Decimal("10000")  # Impact per $10k
        market_volatility = portfolio_with_risk.risk_assessment.volatility_estimate
        
        expected_slippage = min(
            size_impact * market_volatility * Decimal("0.1"),  # 10% of volatility
            max_slippage
        )
        
        # Check if slippage is acceptable
        if expected_slippage > max_slippage:
            return {
                "success": False,
                "reason": f"Expected slippage {expected_slippage:.2%} exceeds tolerance {max_slippage:.2%}",
                "quantity_filled": Decimal("0")
            }
        
        # Calculate execution price with slippage
        if signal.direction in ["buy", "long"]:
            execution_price = market_price * (Decimal("1") + expected_slippage)
        else:
            execution_price = market_price * (Decimal("1") - expected_slippage)
        
        # Calculate fees based on exchange
        fee_rate = Decimal("0.0002") if exchange == "hyperliquid" else Decimal("0.001")  # 0.02% vs 0.1%
        fees = position_size * fee_rate
        
        # Generate order ID
        order_id = f"{exchange}_{signal.symbol.value.replace('/', '_')}_{uuid.uuid4().hex[:8]}"
        
        # Simulate execution delay
        await asyncio.sleep(0.05)  # 50ms latency
        
        return {
            "success": True,
            "quantity_filled": position_size,
            "avg_fill_price": execution_price,
            "fees": fees,
            "slippage": expected_slippage,
            "exchange_order_id": order_id,
            "metadata": {
                "exchange": exchange,
                "symbol": signal.symbol.value,
                "direction": signal.direction,
                "market_price": market_price,
                "fee_rate": fee_rate
            }
        }

    async def _update_portfolio_state(
        self,
        execution_result: dict[str, Any],
        signal: Any,  # TradingSignal
        portfolio_context: dict[str, Any]
    ) -> dict[str, Any]:
        """Update portfolio state through coordinator."""

        if not execution_result["success"]:
            return {"capital_change": Decimal("0"), "exposure_change": Decimal("0")}

        quantity_filled = execution_result["quantity_filled"]
        avg_fill_price = execution_result.get("avg_fill_price", Decimal("50000"))
        fees = execution_result.get("fees", Decimal("0"))

        # Calculate portfolio impact
        notional_value = quantity_filled * avg_fill_price
        
        if signal.direction == "buy" or signal.direction == "long":
            capital_change = -(notional_value + fees)  # Decrease cash
            exposure_change = notional_value  # Increase exposure
        elif signal.direction == "sell" or signal.direction == "short":
            capital_change = notional_value - fees  # Increase cash
            exposure_change = -notional_value  # Decrease exposure
        else:  # close position
            # This would depend on the existing position
            capital_change = -fees  # Only fees
            exposure_change = Decimal("0")  # No net exposure change for close

        # This would integrate with the coordinator to update actual portfolio state
        # For now, return the calculated impact
        
        return {
            "capital_change": capital_change,
            "exposure_change": exposure_change,
            "fees_paid": fees,
            "quantity_filled": quantity_filled,
            "avg_price": avg_fill_price
        }

    async def _update_execution_stats(self, result: ExecutionResult) -> None:
        """Update execution statistics."""
        self.execution_stats["total_executions"] = int(str(self.execution_stats["total_executions"])) + 1

        if result.success:
            self.execution_stats["successful_executions"] = int(str(self.execution_stats["successful_executions"])) + 1

        # Update average execution time
        current_avg = float(str(self.execution_stats["avg_execution_time"]))
        total_execs = int(str(self.execution_stats["total_executions"]))
        new_avg = ((current_avg * (total_execs - 1)) + result.execution_time_ms) / total_execs
        self.execution_stats["avg_execution_time"] = new_avg

        # Update average slippage
        if result.success:
            current_slippage_avg = Decimal(str(self.execution_stats["avg_slippage"]))
            successful_execs = int(str(self.execution_stats["successful_executions"]))
            new_slippage_avg = ((current_slippage_avg * (successful_execs - 1)) + result.slippage) / successful_execs
            self.execution_stats["avg_slippage"] = new_slippage_avg

        # Update total fees
        self.execution_stats["total_fees"] = Decimal(str(self.execution_stats["total_fees"])) + result.fees

    async def _handle_execution_failure(self, execution_id: str, reason: str) -> dict[str, Any]:
        """Handle execution failure."""
        if execution_id in self.active_executions:
            self.active_executions[execution_id]["status"] = ExecutionStatus.FAILED
            del self.active_executions[execution_id]

        return {
            "execution_id": execution_id,
            "success": False,
            "status": "failed",
            "reason": reason,
            "portfolio_impact": {
                "capital_change": Decimal("0"),
                "exposure_change": Decimal("0")
            }
        }

    async def _cancel_order_on_exchange(self, execution_record: dict[str, Any]) -> dict[str, Any]:
        """Cancel order on exchange."""
        # Extract order details
        signal = execution_record["signal"]
        exchange = getattr(signal, "exchange", "hyperliquid")
        
        # In production, this would call the actual exchange API
        # For now, simulate cancellation
        await asyncio.sleep(0.02)  # 20ms latency
        
        # Check if order can be cancelled (simulate partial fill scenario)
        import random
        if random.random() < 0.1:  # 10% chance of partial fill
            partial_fill = execution_record["position_size"] * Decimal(str(random.uniform(0.1, 0.5)))
            return {
                "cancelled": True,
                "cancelled_quantity": execution_record["position_size"] - partial_fill,
                "filled_quantity": partial_fill,
                "reason": "Partial fill before cancellation"
            }
        
        return {
            "cancelled": True,
            "cancelled_quantity": execution_record["position_size"],
            "filled_quantity": Decimal("0")
        }

    async def _load_execution_config(self) -> None:
        """Load execution configuration."""
        # Get configuration from coordinator if available
        try:
            # In production, would load from config service
            config = {
                "max_slippage": Decimal("0.005"),
                "execution_timeout": 30,
                "max_retry_attempts": 3,
                "partial_fill_threshold": Decimal("0.8"),
                "fee_rates": {
                    "hyperliquid": Decimal("0.0002"),
                    "backpack": Decimal("0.001")
                }
            }
            
            # Update instance variables
            self.max_slippage = Decimal(str(config.get("max_slippage", self.max_slippage)))
            self.execution_timeout = int(config.get("execution_timeout", self.execution_timeout))
            self.max_retry_attempts = int(config.get("max_retry_attempts", self.max_retry_attempts))
            self.partial_fill_threshold = Decimal(str(config.get("partial_fill_threshold", self.partial_fill_threshold)))
            
        except Exception:
            # Use defaults if config loading fails
            pass

    async def _initialize_exchange_connections(self) -> None:
        """Initialize connections to exchanges."""
        # In production, would initialize actual exchange connections
        # For now, track connection state
        self.exchange_connections = {
            "hyperliquid": {"connected": True, "latency_ms": 20},
            "backpack": {"connected": True, "latency_ms": 50}
        }

    async def shutdown(self) -> None:
        """Shutdown the trade executor."""
        # Cancel all active executions
        for execution_id in list(self.active_executions.keys()):
            await self.cancel_execution(execution_id)

        # Close exchange connections
        await self._close_exchange_connections()

    async def _close_exchange_connections(self) -> None:
        """Close exchange connections."""
        # Close all exchange connections
        if hasattr(self, "exchange_connections"):
            for exchange in self.exchange_connections:
                self.exchange_connections[exchange]["connected"] = False
    
    async def _get_market_price(self, symbol: Symbol, exchange: str) -> Decimal:
        """Get current market price for symbol.
        
        Uses the configured price service to fetch real market prices.
        """
        if self.price_service:
            return await self.price_service.get_current_price(symbol, exchange)
        
        raise NotImplementedError(
            "Market price fetching not configured. "
            "Trade executor must be initialized with a price service "
            "to fetch real market prices."
        )