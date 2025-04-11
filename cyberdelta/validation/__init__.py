"""
Validation module for CyberDeltaEngine.

This module provides components for validating trading signals,
reconciling positions, and implementing safety mechanisms like
circuit breakers and funding rate validation.
"""

# Original validation components
try:
    from cyberdelta.validation.circuit_breaker import (
        CircuitBreaker,
        CircuitBreakerSystem,
    )
    from cyberdelta.validation.funding_rate_validator import FundingRateValidator
    from cyberdelta.validation.position_reconciliation import (
        PositionReconciliationSystem,
    )
except ImportError:
    # These may not exist yet during development
    pass

# New multi-tier components
from .funding_data import (
    ArbitrageOpportunity,
    ConfidenceFactors,
    FundingData,
    FundingRatePrediction,
    FundingRateValidationMetrics,
    HistoricalTrade,
    IntegratedFundingData,
    SourceReliability,
    SourceType,
)
from .multi_tier_funding_provider import (
    FundingRateSourceError,
    MultiTierFundingProvider,
)

__all__ = [
    # Original validation components
    "FundingRateValidator",
    "PositionReconciliationSystem",
    "CircuitBreakerSystem",
    "CircuitBreaker",
    # funding_data.py
    "SourceType",
    "SourceReliability",
    "FundingData",
    "IntegratedFundingData",
    "FundingRateValidationMetrics",
    "ConfidenceFactors",
    "FundingRatePrediction",
    "ArbitrageOpportunity",
    "HistoricalTrade",
    # multi_tier_funding_provider.py
    "FundingRateSourceError",
    "MultiTierFundingProvider",
]
