"""
Data structures for funding rate data from multiple sources.

This module contains the data structures used for the multi-tier signal
verification system, supporting the collection, integration, and validation
of funding rate data from multiple sources.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple, Any


class SourceType(Enum):
    """Type of funding rate data source."""
    PRIMARY = "primary"        # Direct exchange API
    SECONDARY = "secondary"    # Alternative API endpoint or cached data
    TERTIARY = "tertiary"      # Third-party data or derived calculation
    FALLBACK = "fallback"      # Used when other sources fail


class SourceReliability(Enum):
    """Reliability category of a data source."""
    HIGH = "high"              # Highly reliable (direct exchange API)
    MEDIUM = "medium"          # Moderately reliable (third-party provider)
    LOW = "low"                # Less reliable (derived or estimated)


@dataclass
class FundingData:
    """Base funding rate data from a single source."""
    exchange: str
    symbol: str
    rate: float
    timestamp: datetime
    source_type: SourceType
    source_reliability: SourceReliability
    raw_data: Optional[Dict[str, Any]] = None
    staleness: float = 0.0  # Measured in seconds
    
    def is_stale(self, max_age_seconds: float) -> bool:
        """
        Check if the funding data is considered stale.
        
        Args:
            max_age_seconds: Maximum acceptable age in seconds
            
        Returns:
            True if data is stale, False otherwise
        """
        age = (datetime.now() - self.timestamp).total_seconds()
        return age > max_age_seconds


@dataclass
class IntegratedFundingData:
    """Integrated funding rate data from multiple sources."""
    exchange: str
    symbol: str
    rate: float
    timestamp: datetime
    dispersion: float  # Measure of disagreement between sources
    sources_count: int
    primary_available: bool
    secondary_available: bool
    tertiary_available: bool
    confidence_score: float
    source_data: Dict[SourceType, FundingData] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_age(self) -> float:
        """
        Get the age of the integrated data in seconds.
        
        Returns:
            Age in seconds
        """
        return (datetime.now() - self.timestamp).total_seconds()
    
    def is_stale(self, max_age_seconds: float) -> bool:
        """
        Check if the integrated funding data is considered stale.
        
        Args:
            max_age_seconds: Maximum acceptable age in seconds
            
        Returns:
            True if data is stale, False otherwise
        """
        return self.get_age() > max_age_seconds


@dataclass
class FundingRateValidationMetrics:
    """Validation metrics for funding rate predictions."""
    exchange: str
    symbol: str
    rmse: float  # Root Mean Square Error
    mae: float   # Mean Absolute Error
    bias: float  # Systematic bias (positive = over-prediction)
    sample_count: int
    period_days: int
    calculation_time: datetime = field(default_factory=datetime.now)


@dataclass
class ConfidenceFactors:
    """Factors contributing to the confidence score."""
    historical_accuracy: float = 0.0
    source_count_factor: float = 0.0
    dispersion_factor: float = 0.0
    freshness_factor: float = 0.0
    
    def get_weighted_score(self, 
                          historical_weight: float = 0.4, 
                          source_count_weight: float = 0.2,
                          dispersion_weight: float = 0.3,
                          freshness_weight: float = 0.1) -> float:
        """
        Calculate weighted confidence score from factors.
        
        Args:
            historical_weight: Weight for historical accuracy
            source_count_weight: Weight for source count factor
            dispersion_weight: Weight for dispersion factor
            freshness_weight: Weight for data freshness
            
        Returns:
            Weighted confidence score between 0.0 and 1.0
        """
        score = (
            self.historical_accuracy * historical_weight +
            self.source_count_factor * source_count_weight +
            self.dispersion_factor * dispersion_weight +
            self.freshness_factor * freshness_weight
        )
        
        # Ensure score is between 0 and 1
        return max(0.0, min(1.0, score))


@dataclass
class FundingRatePrediction:
    """Prediction of future funding rate."""
    exchange: str
    symbol: str
    predicted_rate: float
    prediction_time: datetime
    target_time: datetime
    confidence: float
    method: str
    confidence_factors: Optional[ConfidenceFactors] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ArbitrageOpportunity:
    """Identified arbitrage opportunity with confidence scoring."""
    symbol: str
    long_exchange: str
    short_exchange: str
    long_funding_rate: float
    short_funding_rate: float
    net_funding_differential: float
    timestamp: datetime
    expected_profit: float
    utility_score: float
    confidence_score: float
    basis_volatility: float
    integrated_funding_data: Optional[IntegratedFundingData] = None
    adjusted_thresholds: Dict[str, float] = field(default_factory=dict)
    expiration: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_expired(self) -> bool:
        """
        Check if the opportunity has expired.
        
        Returns:
            True if expired, False otherwise
        """
        if self.expiration is None:
            return False
        
        return datetime.now() > self.expiration


@dataclass
class HistoricalTrade:
    """Historical trade record for probability estimation."""
    exchange: str
    symbol: str
    entry_time: datetime
    exit_time: Optional[datetime]
    entry_funding_rate: float
    exit_funding_rate: Optional[float]
    profit: float
    position_size: float
    side: str  # "LONG" or "SHORT"
    is_complete: bool
    metadata: Dict[str, Any] = field(default_factory=dict) 