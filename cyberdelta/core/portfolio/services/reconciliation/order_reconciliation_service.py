"""Order reconciliation service - checks order status consistency."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService
from cyberdelta.core.symbols import Symbol, symbol as create_symbol
from cyberdelta.enums.exchange_names import ExchangeName

if TYPE_CHECKING:
    from cyberdelta.core.models import Order

logger = get_logger(__name__)


class OrderDiscrepancy(BaseModel):
    """Represents an order discrepancy found during reconciliation."""
    
    exchange: str = Field(..., description="Exchange where discrepancy found")
    order_id: str = Field(..., description="Order ID with discrepancy")
    symbol: Symbol = Field(..., description="Symbol for the order")
    discrepancy_type: str = Field(..., description="Type of discrepancy")
    expected_value: str | Decimal = Field(..., description="Expected value")
    actual_value: str | Decimal = Field(..., description="Actual value")
    severity: str = Field(..., description="Severity level")
    message: str = Field(..., description="Detailed message")
    
    model_config = ConfigDict(extra="forbid", frozen=True)


class OrderReconciliationService(BasePortfolioService):
    """Checks order status consistency and validity."""
    
    # Configuration
    max_order_age_seconds: int = Field(
        default=86400, 
        gt=0, 
        description="Maximum order age in seconds (24 hours)"
    )
    max_open_orders_per_symbol: int = Field(
        default=10,
        gt=0,
        description="Maximum open orders per symbol"
    )
    max_total_open_orders: int = Field(
        default=100,
        gt=0,
        description="Maximum total open orders"
    )
    
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    
    async def reconcile_orders(
        self,
        orders: dict[str, list[Order]],
    ) -> list[OrderDiscrepancy]:
        """Reconcile orders across exchanges.
        
        Args:
            orders: Orders by exchange
            
        Returns:
            List of order discrepancies found
        """
        discrepancies: list[OrderDiscrepancy] = []
        current_time = time.time()
        
        # Track orders per symbol for concentration check
        orders_per_symbol: dict[Symbol, int] = {}
        total_open_orders = 0
        
        for exchange, exchange_orders in orders.items():
            for order in exchange_orders:
                # Only check open orders
                if order.status not in [OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED]:
                    continue
                
                total_open_orders += 1
                
                # Track orders per symbol
                if order.symbol not in orders_per_symbol:
                    orders_per_symbol[order.symbol] = 0
                orders_per_symbol[order.symbol] += 1
                
                # Check order age
                order_age = current_time - order.created_at.timestamp()
                if order_age > self.max_order_age_seconds:
                    discrepancies.append(OrderDiscrepancy(
                        exchange=exchange,
                        order_id=order.exchange_order_id or order.client_order_id,
                        symbol=order.symbol,
                        discrepancy_type="stale_order",
                        expected_value=str(self.max_order_age_seconds),
                        actual_value=str(int(order_age)),
                        severity="warning",
                        message=f"Order {order.exchange_order_id or order.client_order_id} is {order_age/3600:.1f} hours old",
                    ))
                
                # Check order consistency
                await self._check_order_consistency(order, exchange, discrepancies)
                
                # Check partially filled orders
                if order.status == OrderStatus.PARTIALLY_FILLED:
                    await self._check_partial_fill(order, exchange, discrepancies)
        
        # Check order concentration
        for symbol, count in orders_per_symbol.items():
            if count > self.max_open_orders_per_symbol:
                discrepancies.append(OrderDiscrepancy(
                    exchange="all",
                    order_id="multiple",
                    symbol=symbol,
                    discrepancy_type="excessive_orders_per_symbol",
                    expected_value=str(self.max_open_orders_per_symbol),
                    actual_value=str(count),
                    severity="warning",
                    message=f"Too many open orders ({count}) for {symbol}",
                ))
        
        # Check total open orders
        if total_open_orders > self.max_total_open_orders:
            discrepancies.append(OrderDiscrepancy(
                exchange="all",
                order_id="total",
                symbol=create_symbol("ALL", ExchangeName.HYPERLIQUID),
                discrepancy_type="excessive_total_orders",
                expected_value=str(self.max_total_open_orders),
                actual_value=str(total_open_orders),
                severity="warning",
                message=f"Too many total open orders ({total_open_orders})",
            ))
        
        return discrepancies
    
    async def _check_order_consistency(
        self, 
        order: Order, 
        exchange: str,
        discrepancies: list[OrderDiscrepancy]
    ) -> None:
        """Check internal consistency of an order."""
        # Check filled vs size
        if order.quantity_filled > order.quantity_requested:
            discrepancies.append(OrderDiscrepancy(
                exchange=exchange,
                order_id=order.exchange_order_id or order.client_order_id,
                symbol=order.symbol,
                discrepancy_type="overfilled_order",
                expected_value=order.quantity_requested,
                actual_value=order.quantity_filled,
                severity="error",
                message=f"Order filled amount exceeds size",
            ))
        
        # Check remaining calculation consistency
        expected_remaining = order.quantity_requested - order.quantity_filled
        actual_remaining = self._get_order_remaining(order)
        if actual_remaining is not None and abs(expected_remaining - actual_remaining) > Decimal("0.00001"):
            discrepancies.append(OrderDiscrepancy(
                exchange=exchange,
                order_id=order.exchange_order_id or order.client_order_id,
                symbol=order.symbol,
                discrepancy_type="remaining_mismatch",
                expected_value=expected_remaining,
                actual_value=actual_remaining,
                severity="error",
                message=f"Order remaining calculation mismatch",
            ))
        
        # Check price validity
        if order.price <= 0:
            discrepancies.append(OrderDiscrepancy(
                exchange=exchange,
                order_id=order.exchange_order_id or order.client_order_id,
                symbol=order.symbol,
                discrepancy_type="invalid_price",
                expected_value=Decimal("0.01"),
                actual_value=order.price,
                severity="error",
                message=f"Invalid order price",
            ))
        
        # Check size validity
        if order.quantity_requested <= 0:
            discrepancies.append(OrderDiscrepancy(
                exchange=exchange,
                order_id=order.exchange_order_id or order.client_order_id,
                symbol=order.symbol,
                discrepancy_type="invalid_size",
                expected_value=Decimal("0.01"),
                actual_value=order.quantity_requested,
                severity="error",
                message=f"Invalid order size",
            ))
    
    async def _check_partial_fill(
        self,
        order: Order,
        exchange: str,
        discrepancies: list[OrderDiscrepancy]
    ) -> None:
        """Check partially filled orders for issues."""
        # Check if partially filled order has been stuck for too long
        if order.status == OrderStatus.PARTIALLY_FILLED:
            fill_percentage = (order.quantity_filled / order.quantity_requested) * 100
            
            # Warning if order is less than 10% filled after significant time
            order_age = time.time() - order.created_at.timestamp()
            if order_age > 3600 and fill_percentage < 10:  # 1 hour and <10% filled
                discrepancies.append(OrderDiscrepancy(
                    exchange=exchange,
                    order_id=order.exchange_order_id or order.client_order_id,
                    symbol=order.symbol,
                    discrepancy_type="stuck_partial_fill",
                    expected_value="10%",
                    actual_value=f"{fill_percentage:.1f}%",
                    severity="warning",
                    message=f"Order only {fill_percentage:.1f}% filled after {order_age/3600:.1f} hours",
                ))
    
    def _get_order_remaining(self, order: Order) -> Decimal | None:
        """Get remaining quantity from order based on exchange-specific details."""
        # Try Hyperliquid details
        if order.hl_details and order.hl_details.remaining_sz is not None:
            return order.hl_details.remaining_sz
        
        # For other exchanges or if no exchange details, calculate from core fields
        return order.quantity_requested - order.quantity_filled