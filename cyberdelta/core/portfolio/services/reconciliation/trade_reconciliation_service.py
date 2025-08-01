"""Trade reconciliation service - validates trade integrity."""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService
from cyberdelta.core.symbols import Symbol

if TYPE_CHECKING:
    from cyberdelta.core.models import Trade

logger = get_logger(__name__)


class TradeDiscrepancy(BaseModel):
    """Represents a trade discrepancy found during reconciliation."""
    
    exchange: str = Field(..., description="Exchange where discrepancy found")
    trade_id: str = Field(..., description="Trade ID with discrepancy")
    symbol: Symbol = Field(..., description="Symbol for the trade")
    discrepancy_type: str = Field(..., description="Type of discrepancy")
    expected_value: str | Decimal = Field(..., description="Expected value")
    actual_value: str | Decimal = Field(..., description="Actual value")
    severity: str = Field(..., description="Severity level")
    message: str = Field(..., description="Detailed message")
    
    model_config = ConfigDict(extra="forbid", frozen=True)


class TradeReconciliationService(BasePortfolioService):
    """Validates trade integrity and consistency."""
    
    # Configuration
    max_trade_age_seconds: int = Field(
        default=86400 * 7,  # 7 days
        gt=0, 
        description="Maximum trade age for recent trades"
    )
    min_trade_size: Decimal = Field(
        default=Decimal("0.00001"),
        gt=0,
        description="Minimum valid trade size"
    )
    max_price_deviation: Decimal = Field(
        default=Decimal("0.1"),  # 10%
        gt=0,
        le=1,
        description="Maximum price deviation from expected"
    )
    duplicate_window_seconds: int = Field(
        default=5,
        gt=0,
        description="Window for detecting duplicate trades"
    )
    
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    
    async def reconcile_trades(
        self,
        trades: dict[str, list[Trade]],
    ) -> list[TradeDiscrepancy]:
        """Reconcile trades across exchanges.
        
        Args:
            trades: Trades by exchange
            
        Returns:
            List of trade discrepancies found
        """
        discrepancies: list[TradeDiscrepancy] = []
        
        for exchange, exchange_trades in trades.items():
            # Check individual trade integrity
            for trade in exchange_trades:
                await self._check_trade_integrity(trade, exchange, discrepancies)
            
            # Check for duplicate trades
            await self._check_duplicate_trades(exchange_trades, exchange, discrepancies)
            
            # Check trade sequence
            await self._check_trade_sequence(exchange_trades, exchange, discrepancies)
        
        # Cross-exchange checks if multiple exchanges
        if len(trades) > 1:
            await self._check_cross_exchange_trades(trades, discrepancies)
        
        return discrepancies
    
    async def _check_trade_integrity(
        self,
        trade: Trade,
        exchange: str,
        discrepancies: list[TradeDiscrepancy]
    ) -> None:
        """Check integrity of individual trade."""
        # Check price validity
        if trade.price <= 0:
            discrepancies.append(TradeDiscrepancy(
                exchange=exchange,
                trade_id=trade.id,
                symbol=trade.symbol,
                discrepancy_type="invalid_price",
                expected_value=Decimal("0.01"),
                actual_value=trade.price,
                severity="error",
                message=f"Invalid trade price",
            ))
        
        # Check size validity
        if trade.quantity < self.min_trade_size:
            discrepancies.append(TradeDiscrepancy(
                exchange=exchange,
                trade_id=trade.id,
                symbol=trade.symbol,
                discrepancy_type="invalid_size",
                expected_value=self.min_trade_size,
                actual_value=trade.quantity,
                severity="error",
                message=f"Trade size below minimum",
            ))
        
        # Check fee validity
        if trade.fee < 0:
            discrepancies.append(TradeDiscrepancy(
                exchange=exchange,
                trade_id=trade.id,
                symbol=trade.symbol,
                discrepancy_type="negative_fee",
                expected_value=Decimal("0"),
                actual_value=trade.fee,
                severity="error",
                message=f"Negative trade fee",
            ))
        
        # Check cost calculation
        expected_cost = trade.price * trade.quantity
        cost_difference = abs(expected_cost - trade.cost)
        if cost_difference > Decimal("0.01"):  # Allow small rounding differences
            discrepancies.append(TradeDiscrepancy(
                exchange=exchange,
                trade_id=trade.id,
                symbol=trade.symbol,
                discrepancy_type="cost_mismatch",
                expected_value=expected_cost,
                actual_value=trade.cost,
                severity="error",
                message=f"Trade cost calculation mismatch",
            ))
        
        # Check timestamp validity
        current_time = time.time()
        trade_timestamp = trade.executed_at.timestamp()
        if trade_timestamp > current_time:
            discrepancies.append(TradeDiscrepancy(
                exchange=exchange,
                trade_id=trade.id,
                symbol=trade.symbol,
                discrepancy_type="future_timestamp",
                expected_value=str(int(current_time)),
                actual_value=str(int(trade_timestamp)),
                severity="error",
                message=f"Trade timestamp in the future",
            ))
    
    async def _check_duplicate_trades(
        self,
        trades: list[Trade],
        exchange: str,
        discrepancies: list[TradeDiscrepancy]
    ) -> None:
        """Check for duplicate trades within time window."""
        # Sort trades by timestamp
        sorted_trades = sorted(trades, key=lambda t: t.executed_at)
        
        for i in range(len(sorted_trades) - 1):
            current_trade = sorted_trades[i]
            
            # Check subsequent trades within window
            j = i + 1
            while j < len(sorted_trades):
                next_trade = sorted_trades[j]
                
                # Stop if outside duplicate window
                time_diff = (next_trade.executed_at - current_trade.executed_at).total_seconds()
                if time_diff > self.duplicate_window_seconds:
                    break
                
                # Check if trades are suspiciously similar
                if (current_trade.symbol == next_trade.symbol and
                    current_trade.price == next_trade.price and
                    current_trade.quantity == next_trade.quantity and
                    current_trade.side == next_trade.side):
                    
                    discrepancies.append(TradeDiscrepancy(
                        exchange=exchange,
                        trade_id=f"{current_trade.id},{next_trade.id}",
                        symbol=current_trade.symbol,
                        discrepancy_type="duplicate_trade",
                        expected_value="unique",
                        actual_value="duplicate",
                        severity="warning",
                        message=f"Potential duplicate trades detected",
                    ))
                
                j += 1
    
    async def _check_trade_sequence(
        self,
        trades: list[Trade],
        exchange: str,
        discrepancies: list[TradeDiscrepancy]
    ) -> None:
        """Check trade sequence for anomalies."""
        if not trades:
            return
        
        # Sort by timestamp
        sorted_trades = sorted(trades, key=lambda t: t.executed_at)
        
        # Track price movements by symbol
        last_price_by_symbol: dict[Symbol, Decimal] = {}
        
        for trade in sorted_trades:
            if trade.symbol in last_price_by_symbol:
                last_price = last_price_by_symbol[trade.symbol]
                price_change = abs(trade.price - last_price) / last_price
                
                # Check for extreme price movements
                if price_change > self.max_price_deviation:
                    discrepancies.append(TradeDiscrepancy(
                        exchange=exchange,
                        trade_id=trade.id,
                        symbol=trade.symbol,
                        discrepancy_type="extreme_price_movement",
                        expected_value=str(self.max_price_deviation),
                        actual_value=str(round(price_change, 4)),
                        severity="warning",
                        message=f"Extreme price movement {price_change*100:.1f}% between trades",
                    ))
            
            last_price_by_symbol[trade.symbol] = trade.price
    
    async def _check_cross_exchange_trades(
        self,
        trades: dict[str, list[Trade]],
        discrepancies: list[TradeDiscrepancy]
    ) -> None:
        """Check for cross-exchange anomalies."""
        # Group trades by timestamp window and symbol
        time_window = 60  # 1 minute window
        
        # Flatten all trades with exchange info
        all_trades = []
        for exchange, exchange_trades in trades.items():
            for trade in exchange_trades:
                all_trades.append((exchange, trade))
        
        # Sort by timestamp
        all_trades.sort(key=lambda x: x[1].executed_at)
        
        # Check for price discrepancies in same time window
        for i, (exchange1, trade1) in enumerate(all_trades):
            for j in range(i + 1, len(all_trades)):
                exchange2, trade2 = all_trades[j]
                
                # Stop if outside time window
                time_diff = (trade2.executed_at - trade1.executed_at).total_seconds()
                if time_diff > time_window:
                    break
                
                # Check same symbol on different exchanges
                if (trade1.symbol == trade2.symbol and 
                    exchange1 != exchange2):
                    
                    price_diff = abs(trade1.price - trade2.price) / min(trade1.price, trade2.price)
                    
                    # Warning if significant price difference
                    if price_diff > 0.02:  # 2% threshold
                        discrepancies.append(TradeDiscrepancy(
                            exchange=f"{exchange1},{exchange2}",
                            trade_id=f"{trade1.id},{trade2.id}",
                            symbol=trade1.symbol,
                            discrepancy_type="cross_exchange_price_discrepancy",
                            expected_value=trade1.price,
                            actual_value=trade2.price,
                            severity="warning",
                            message=f"Price discrepancy {price_diff*100:.1f}% between exchanges",
                        ))