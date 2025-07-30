"""Balance reconciliation service - verifies balance consistency."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.services.base.base_service import BasePortfolioService

if TYPE_CHECKING:
    from cyberdelta.core.models import SpotBalance

logger = get_logger(__name__)


class BalanceDiscrepancy(BaseModel):
    """Represents a balance discrepancy found during reconciliation."""
    
    exchange: str = Field(..., description="Exchange where discrepancy found")
    asset: str = Field(..., description="Asset with discrepancy")
    discrepancy_type: str = Field(..., description="Type of discrepancy")
    expected_value: Decimal = Field(..., description="Expected value")
    actual_value: Decimal = Field(..., description="Actual value")
    difference: Decimal = Field(..., description="Difference between expected and actual")
    severity: str = Field(..., description="Severity level")
    message: str = Field(..., description="Detailed message")
    
    model_config = ConfigDict(extra="forbid", frozen=True)


class BalanceReconciliationService(BasePortfolioService):
    """Verifies balance consistency across exchanges."""
    
    # Configuration
    tolerance: Decimal = Field(
        default=Decimal("0.00001"), 
        gt=0, 
        description="Tolerance for balance checks"
    )
    balance_warning_threshold: Decimal = Field(
        default=Decimal("0.01"), 
        gt=0, 
        description="Balance warning threshold"
    )
    allow_negative_balances: bool = Field(
        default=False, 
        description="Allow negative balances"
    )
    max_balance_warning: Decimal = Field(
        default=Decimal(1000000), 
        gt=0, 
        description="Maximum balance before warning"
    )
    balance_concentration_threshold: int = Field(
        default=90, 
        ge=0, 
        le=100,
        description="Percentage threshold for balance concentration warning"
    )
    
    model_config = ConfigDict(extra="forbid", validate_assignment=True)
    
    async def reconcile_balances(
        self,
        balances: dict[str, dict[str, SpotBalance]],
    ) -> list[BalanceDiscrepancy]:
        """Reconcile balances across exchanges.
        
        Args:
            balances: Balances by exchange and asset
            
        Returns:
            List of balance discrepancies found
        """
        discrepancies: list[BalanceDiscrepancy] = []
        
        for exchange, exchange_balances in balances.items():
            for asset, balance in exchange_balances.items():
                # Check for negative balances
                if not self.allow_negative_balances and balance.free < 0:
                    discrepancies.append(BalanceDiscrepancy(
                        exchange=exchange,
                        asset=asset,
                        discrepancy_type="negative_balance",
                        expected_value=Decimal("0"),
                        actual_value=balance.free,
                        difference=balance.free,
                        severity="error",
                        message=f"Negative free balance for {asset} on {exchange}",
                    ))
                
                # Check for unusually large balances
                if balance.total > self.max_balance_warning:
                    discrepancies.append(BalanceDiscrepancy(
                        exchange=exchange,
                        asset=asset,
                        discrepancy_type="large_balance",
                        expected_value=self.max_balance_warning,
                        actual_value=balance.total,
                        difference=balance.total - self.max_balance_warning,
                        severity="warning",
                        message=f"Large balance detected for {asset} on {exchange}",
                    ))
                
                # Check balance consistency (free + used = total)
                calculated_total = balance.free + balance.used
                if abs(calculated_total - balance.total) > self.tolerance:
                    discrepancies.append(BalanceDiscrepancy(
                        exchange=exchange,
                        asset=asset,
                        discrepancy_type="balance_mismatch",
                        expected_value=balance.total,
                        actual_value=calculated_total,
                        difference=abs(calculated_total - balance.total),
                        severity="error",
                        message=f"Balance mismatch: free + used != total for {asset} on {exchange}",
                    ))
        
        # Check balance concentration
        await self._check_balance_concentration(balances, discrepancies)
        
        return discrepancies
    
    async def _check_balance_concentration(
        self,
        balances: dict[str, dict[str, SpotBalance]],
        discrepancies: list[BalanceDiscrepancy],
    ) -> None:
        """Check for balance concentration issues."""
        # Calculate total value by asset across all exchanges
        total_by_asset: dict[str, Decimal] = {}
        
        for exchange_balances in balances.values():
            for asset, balance in exchange_balances.items():
                if asset not in total_by_asset:
                    total_by_asset[asset] = Decimal("0")
                total_by_asset[asset] += balance.total
        
        # Check concentration for each exchange
        for exchange, exchange_balances in balances.items():
            for asset, balance in exchange_balances.items():
                if asset in total_by_asset and total_by_asset[asset] > 0:
                    concentration = (balance.total / total_by_asset[asset]) * 100
                    
                    if concentration > self.balance_concentration_threshold:
                        discrepancies.append(BalanceDiscrepancy(
                            exchange=exchange,
                            asset=asset,
                            discrepancy_type="high_concentration",
                            expected_value=Decimal(str(self.balance_concentration_threshold)),
                            actual_value=Decimal(str(round(concentration, 2))),
                            difference=Decimal(str(round(concentration - self.balance_concentration_threshold, 2))),
                            severity="warning",
                            message=f"High balance concentration {concentration:.1f}% for {asset} on {exchange}",
                        ))