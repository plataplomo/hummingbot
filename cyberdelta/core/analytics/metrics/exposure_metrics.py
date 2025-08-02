"""Focused exposure metrics calculation service."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.models.portfolio_state import PortfolioStateData as PortfolioState
from cyberdelta.core.portfolio.portfolio_types.models import ExposureMetrics


class ExposureMetricsService(BaseModel):
    """Calculates exposure metrics only."""

    base_currency: str = Field(default="USDC", description="Base currency for calculations")

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    async def calculate_exposure_metrics(
        self,
        portfolio_state: PortfolioState
    ) -> ExposureMetrics:
        """Calculate comprehensive exposure metrics."""
        
        # Calculate total exposure
        total_exposure = await self._calculate_total_exposure(portfolio_state)
        
        # Calculate currency exposures
        currency_exposures = await self._calculate_currency_exposures(portfolio_state)
        
        # Use position count from portfolio state
        position_count = portfolio_state.active_positions
        
        # Calculate leverage ratio
        leverage_ratio = await self._calculate_leverage_ratio(portfolio_state)

        return ExposureMetrics(
            total_exposure=total_exposure,
            currency_exposures=currency_exposures,
            position_count=position_count,
            leverage_ratio=leverage_ratio
        )

    async def calculate_sector_exposure(
        self,
        portfolio_state: PortfolioState
    ) -> dict[str, Decimal]:
        """Calculate exposure by sector/asset class."""
        # For crypto trading, use gross exposure as all crypto
        return {"crypto": portfolio_state.gross_exposure}

    async def calculate_exchange_exposure(
        self,
        portfolio_state: PortfolioState
    ) -> dict[str, Decimal]:
        """Calculate exposure by exchange."""
        # Use exchange summaries for exposure data
        exposure_by_exchange = {}
        
        for exchange_id, summary in portfolio_state.exchange_summaries.items():
            # Use exposure field from exchange summary
            exposure_by_exchange[exchange_id] = summary.exposure
        
        return exposure_by_exchange

    async def calculate_concentration_metrics(
        self,
        portfolio_state: PortfolioState
    ) -> dict[str, Any]:
        """Calculate concentration risk metrics."""
        # Use exchange data as proxy for concentration
        total_exposure = portfolio_state.gross_exposure
        
        if total_exposure == 0:
            return {
                "largest_position_pct": Decimal(0),
                "top_5_positions_pct": Decimal(0),
                "herfindahl_index": Decimal(0),
            }
        
        # Calculate exchange concentration
        exchange_exposures = []
        for summary in portfolio_state.exchange_summaries.values():
            exchange_exposures.append(summary.exposure)
        
        if not exchange_exposures:
            return {
                "largest_position_pct": Decimal(0),
                "top_5_positions_pct": Decimal(0),
                "herfindahl_index": Decimal(0),
            }
        
        # Sort by exposure size
        exchange_exposures.sort(reverse=True)
        
        # Calculate metrics based on exchange concentration
        largest_pct = exchange_exposures[0] / total_exposure if exchange_exposures else Decimal(0)
        top_5_pct = sum(exchange_exposures[:5]) / total_exposure if exchange_exposures else Decimal(0)
        
        # Herfindahl index
        herfindahl = sum((exp / total_exposure) ** 2 for exp in exchange_exposures) if exchange_exposures else Decimal(0)
        
        return {
            "largest_position_pct": largest_pct,
            "top_5_positions_pct": top_5_pct,
            "herfindahl_index": herfindahl,
        }

    async def get_exposure_limits(self) -> dict[str, Decimal]:
        """Get configured exposure limits."""
        return {
            "max_single_position": Decimal("0.1"),  # 10%
            "max_exchange_exposure": Decimal("0.5"),  # 50%
            "max_leverage": Decimal("3.0"),  # 3x
        }

    async def _calculate_total_exposure(self, state: PortfolioState) -> Decimal:
        """Calculate total portfolio exposure."""
        # Use gross exposure from portfolio state
        return state.gross_exposure

    async def _calculate_currency_exposures(self, state: PortfolioState) -> dict[str, Decimal]:
        """Calculate exposure by currency."""
        # For now, return base currency exposure
        # Would need position-level data for accurate currency breakdown
        return {
            self.base_currency: state.gross_exposure
        }

    async def _calculate_leverage_ratio(self, state: PortfolioState) -> Decimal | None:
        """Calculate portfolio leverage ratio."""
        if state.total_account_value == 0:
            return None
        
        total_exposure = await self._calculate_total_exposure(state)
        return total_exposure / state.total_account_value

    # NOTE: _calculate_position_exposure removed as individual position data
    # is not available in PortfolioStateData. Using aggregated metrics instead.