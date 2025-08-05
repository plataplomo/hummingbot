"""Simulation mode configuration models.

This module contains configuration for simulation/paper trading modes
that are used in production (not just for testing).
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class SimulationSettings(BaseModel):
    """Simulation mode configuration for paper trading in production."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Simulated balance settings
    initial_balance_usd: float = Field(
        default=10000.0, gt=0, description="Initial simulated balance in USD"
    )

    # Order simulation settings
    fill_probability: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Probability of orders getting filled (1.0 = all orders fill)",
    )
    limit_fill_delay_seconds: float = Field(
        default=5.0,
        gt=0.0,
        le=60.0,
        description="Delay in seconds before limit orders potentially fill",
    )

    # Slippage simulation
    max_slippage_pct: float = Field(
        default=0.001, ge=0.0, le=0.01, description="Maximum slippage percentage (0.001 = 0.1%)"
    )

    # Fee simulation - should match real exchange fees
    maker_fee_rate: float = Field(
        default=0.0002, ge=0.0, le=0.01, description="Maker fee rate (0.0002 = 0.02%)"
    )
    taker_fee_rate: float = Field(
        default=0.0005, ge=0.0, le=0.01, description="Taker fee rate (0.0005 = 0.05%)"
    )

    # Network simulation
    simulate_network_delay: bool = Field(
        default=True, description="Simulate realistic network delays"
    )
    min_network_delay_ms: int = Field(
        default=10, ge=0, le=1000, description="Minimum network delay in milliseconds"
    )
    max_network_delay_ms: int = Field(
        default=100, ge=0, le=5000, description="Maximum network delay in milliseconds"
    )
