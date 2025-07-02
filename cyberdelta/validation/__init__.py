"""Validation module for CyberDeltaEngine.

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
# Import exceptions from their proper location
from cyberdelta.exceptions import FundingRateSourceError

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
    MultiTierFundingProvider,
)


__all__ = [
    "ArbitrageOpportunity",
    "CircuitBreaker",
    "CircuitBreakerSystem",
    "ConfidenceFactors",
    "FundingData",
    "FundingRatePrediction",
    # multi_tier_funding_provider.py
    "FundingRateSourceError",
    "FundingRateValidationMetrics",
    # Original validation components
    "FundingRateValidator",
    "HistoricalTrade",
    "IntegratedFundingData",
    "MultiTierFundingProvider",
    "PositionReconciliationSystem",
    "SourceReliability",
    # funding_data.py
    "SourceType",
]
