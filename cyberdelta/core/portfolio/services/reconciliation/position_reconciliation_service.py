"""Position reconciliation service - validates position calculations."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService

if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition

logger = get_logger(__name__)


class PositionDiscrepancy(BaseModel):
    """Represents a position discrepancy found during reconciliation."""
    
    exchange: str = Field(..., description="Exchange where discrepancy found")
    symbol: str = Field(..., description="Symbol with discrepancy")
    discrepancy_type: str = Field(..., description="Type of discrepancy")
    expected_value: Decimal = Field(..., description="Expected value")
    actual_value: Decimal = Field(..., description="Actual value")
    difference: Decimal = Field(..., description="Difference between expected and actual")
    severity: str = Field(..., description="Severity level")
    message: str = Field(..., description="Detailed message")
    
    model_config = ConfigDict(extra="forbid", frozen=True)


class PositionReconciliationService(BasePortfolioService):
    """Validates position calculations and consistency."""
    
    # Configuration
    tolerance: Decimal = Field(
        default=Decimal("0.00001"), 
        gt=0, 
        description="Tolerance for position checks"
    )
    position_warning_threshold: Decimal = Field(
        default=Decimal("0.01"), 
        gt=0, 
        description="Position warning threshold"
    )
    max_position_size: Decimal = Field(
        default=Decimal(1000000), 
        gt=0, 
        description="Maximum position size"
    )
    max_position_value: Decimal = Field(
        default=Decimal(10000000), 
        gt=0, 
        description="Maximum position value"
    )
    max_leverage: Decimal = Field(
        default=Decimal(100), 
        gt=0, 
        description="Maximum leverage allowed"
    )
    
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    
    async def reconcile_positions(
        self,
        positions: dict[str, dict[str, DerivativePosition]],
    ) -> list[PositionDiscrepancy]:
        """Reconcile positions across exchanges.
        
        Args:
            positions: Positions by exchange and symbol
            
        Returns:
            List of position discrepancies found
        """
        discrepancies: list[PositionDiscrepancy] = []
        
        for exchange, exchange_positions in positions.items():
            for symbol, position in exchange_positions.items():
                # Check position size limits
                if abs(position.size) > self.max_position_size:
                    discrepancies.append(PositionDiscrepancy(
                        exchange=exchange,
                        symbol=symbol,
                        discrepancy_type="excessive_size",
                        expected_value=self.max_position_size,
                        actual_value=abs(position.size),
                        difference=abs(position.size) - self.max_position_size,
                        severity="warning",
                        message=f"Position size exceeds limit for {symbol} on {exchange}",
                    ))
                
                # Check position value limits
                position_value = abs(position.size * position.mark_price)
                if position_value > self.max_position_value:
                    discrepancies.append(PositionDiscrepancy(
                        exchange=exchange,
                        symbol=symbol,
                        discrepancy_type="excessive_value",
                        expected_value=self.max_position_value,
                        actual_value=position_value,
                        difference=position_value - self.max_position_value,
                        severity="warning",
                        message=f"Position value exceeds limit for {symbol} on {exchange}",
                    ))
                
                # Check PnL calculation consistency
                calculated_pnl = await self._calculate_pnl(position)
                if abs(calculated_pnl - position.unrealized_pnl) > self.tolerance:
                    discrepancies.append(PositionDiscrepancy(
                        exchange=exchange,
                        symbol=symbol,
                        discrepancy_type="pnl_mismatch",
                        expected_value=calculated_pnl,
                        actual_value=position.unrealized_pnl,
                        difference=abs(calculated_pnl - position.unrealized_pnl),
                        severity="error",
                        message=f"PnL calculation mismatch for {symbol} on {exchange}",
                    ))
                
                # Check leverage
                if position.leverage and position.leverage > self.max_leverage:
                    discrepancies.append(PositionDiscrepancy(
                        exchange=exchange,
                        symbol=symbol,
                        discrepancy_type="excessive_leverage",
                        expected_value=self.max_leverage,
                        actual_value=position.leverage,
                        difference=position.leverage - self.max_leverage,
                        severity="error",
                        message=f"Leverage exceeds limit for {symbol} on {exchange}",
                    ))
                
                # Check mark price validity
                if position.mark_price <= 0:
                    discrepancies.append(PositionDiscrepancy(
                        exchange=exchange,
                        symbol=symbol,
                        discrepancy_type="invalid_mark_price",
                        expected_value=Decimal("0.01"),
                        actual_value=position.mark_price,
                        difference=Decimal("0.01") - position.mark_price,
                        severity="error",
                        message=f"Invalid mark price for {symbol} on {exchange}",
                    ))
        
        # Check position concentration across exchanges
        await self._check_position_concentration(positions, discrepancies)
        
        return discrepancies
    
    async def _calculate_pnl(self, position: DerivativePosition) -> Decimal:
        """Calculate expected PnL for validation."""
        if position.side == "long":
            return position.size * (position.mark_price - position.entry_price)
        else:  # short
            return position.size * (position.entry_price - position.mark_price)
    
    async def _check_position_concentration(
        self,
        positions: dict[str, dict[str, DerivativePosition]],
        discrepancies: list[PositionDiscrepancy],
    ) -> None:
        """Check for position concentration issues."""
        # Track total exposure by symbol
        symbol_exposure: dict[str, Decimal] = {}
        total_exposure = Decimal("0")
        
        for exchange_positions in positions.values():
            for symbol, position in exchange_positions.items():
                exposure = abs(position.size * position.mark_price)
                if symbol not in symbol_exposure:
                    symbol_exposure[symbol] = Decimal("0")
                symbol_exposure[symbol] += exposure
                total_exposure += exposure
        
        # Check if any symbol has excessive concentration
        if total_exposure > 0:
            for symbol, exposure in symbol_exposure.items():
                concentration = (exposure / total_exposure) * 100
                if concentration > 50:  # Warning if >50% in single symbol
                    discrepancies.append(PositionDiscrepancy(
                        exchange="all",
                        symbol=symbol,
                        discrepancy_type="high_concentration",
                        expected_value=Decimal("50"),
                        actual_value=Decimal(str(round(concentration, 2))),
                        difference=Decimal(str(round(concentration - 50, 2))),
                        severity="warning",
                        message=f"High position concentration {concentration:.1f}% in {symbol}",
                    ))