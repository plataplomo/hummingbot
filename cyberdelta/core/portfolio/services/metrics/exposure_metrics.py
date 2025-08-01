"""Focused exposure metrics calculation service."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.portfolio.portfolio_types.models import PortfolioState, ExposureMetrics, Position


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
        
        # Count positions
        position_count = sum(len(positions) for positions in portfolio_state.positions.values())
        
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
        # For crypto trading, classify by asset type
        sector_exposure = {"crypto": Decimal(0)}
        
        for positions in portfolio_state.positions.values():
            for position in positions:
                position_exposure = await self._calculate_position_exposure(position)
                sector_exposure["crypto"] += abs(position_exposure)
        
        return sector_exposure

    async def calculate_exchange_exposure(
        self,
        portfolio_state: PortfolioState
    ) -> dict[str, Decimal]:
        """Calculate exposure by exchange."""
        exposure_by_exchange = {}
        
        for exchange, positions in portfolio_state.positions.items():
            exchange_exposure = Decimal(0)
            for position in positions:
                position_exposure = await self._calculate_position_exposure(position)
                exchange_exposure += abs(position_exposure)
            
            exposure_by_exchange[exchange] = exchange_exposure
        
        return exposure_by_exchange

    async def calculate_concentration_metrics(
        self,
        portfolio_state: PortfolioState
    ) -> dict[str, Any]:
        """Calculate concentration risk metrics."""
        # Calculate position exposures
        position_exposures = []
        total_exposure = Decimal(0)
        
        for positions in portfolio_state.positions.values():
            for position in positions:
                exposure = await self._calculate_position_exposure(position)
                abs_exposure = abs(exposure)
                position_exposures.append(abs_exposure)
                total_exposure += abs_exposure
        
        if not position_exposures or total_exposure == 0:
            return {
                "largest_position_pct": Decimal(0),
                "top_5_positions_pct": Decimal(0),
                "herfindahl_index": Decimal(0),
            }
        
        # Sort by exposure size
        position_exposures.sort(reverse=True)
        
        # Calculate metrics
        largest_pct = position_exposures[0] / total_exposure
        top_5_pct = sum(position_exposures[:5]) / total_exposure
        
        # Herfindahl index (sum of squared market shares)
        herfindahl = sum((exp / total_exposure) ** 2 for exp in position_exposures)
        
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
        total = Decimal(0)
        
        for exchange, positions in state.positions.items():
            for position in positions:
                position_exposure = await self._calculate_position_exposure(position)
                total += abs(position_exposure)
        
        return total

    async def _calculate_currency_exposures(self, state: PortfolioState) -> dict[str, Decimal]:
        """Calculate exposure by currency."""
        currency_exposure = {}
        
        # Calculate from positions
        for exchange, positions in state.positions.items():
            for position in positions:
                # Extract currency from symbol (simplified) - use Symbol value for string operations
                symbol_value = position.symbol.value
                currency = symbol_value.split("-")[0] if "-" in symbol_value else symbol_value
                
                if currency not in currency_exposure:
                    currency_exposure[currency] = Decimal(0)
                
                position_exposure = await self._calculate_position_exposure(position)
                currency_exposure[currency] += position_exposure
        
        return currency_exposure

    async def _calculate_leverage_ratio(self, state: PortfolioState) -> Decimal | None:
        """Calculate portfolio leverage ratio."""
        if state.total_capital == 0:
            return None
        
        total_exposure = await self._calculate_total_exposure(state)
        return total_exposure / state.total_capital

    async def _calculate_position_exposure(self, position: Position) -> Decimal:
        """Calculate exposure for a single position."""
        # Use mark price if available, otherwise entry price
        price = position.mark_price or position.entry_price
        if price is None:
            return Decimal(0)
        
        # Calculate notional exposure (size * price)
        return abs(position.size * price)