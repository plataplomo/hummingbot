"""
Data structures for funding rate data from multiple sources.

This module contains the data structures used for the multi-tier signal
verification system, supporting the collection, integration, and validation
of funding rate data from multiple sources.
"""

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value


class SourceType(Enum):
    """Type of funding rate data source."""

    PRIMARY = "primary"  # Direct exchange API
    SECONDARY = "secondary"  # Alternative API endpoint or cached data
    TERTIARY = "tertiary"  # Third-party data or derived calculation
    FALLBACK = "fallback"  # Used when other sources fail


class SourceReliability(Enum):
    """Reliability category of a data source."""

    HIGH = "high"  # Highly reliable (direct exchange API)
    MEDIUM = "medium"  # Moderately reliable (third-party provider)
    LOW = "low"  # Less reliable (derived or estimated)


@dataclass
class FundingData:
    """Base funding rate data from a single source."""

    exchange: str
    symbol: str
    rate: float
    timestamp: datetime
    source_type: SourceType
    source_reliability: SourceReliability
    raw_data: dict[str, Any] | None = None
    staleness: float = 0.0  # Measured in seconds

    def is_stale(self, max_age_seconds: float) -> bool:
        """
        Check if the funding data is considered stale.

        Args:
            max_age_seconds: Maximum acceptable age in seconds

        Returns:
            True if data is stale, False otherwise
        """
        age = (datetime.now(UTC) - self.timestamp).total_seconds()
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
    source_data: dict[SourceType, FundingData] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def get_age(self) -> float:
        """
        Get the age of the integrated data in seconds.

        Returns:
            Age in seconds
        """
        return (datetime.now(UTC) - self.timestamp).total_seconds()

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
    mae: float  # Mean Absolute Error
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

    def get_weighted_score(
        self,
        historical_weight: float = 0.4,
        source_count_weight: float = 0.2,
        dispersion_weight: float = 0.3,
        freshness_weight: float = 0.1,
    ) -> float:
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
            self.historical_accuracy * historical_weight
            + self.source_count_factor * source_count_weight
            + self.dispersion_factor * dispersion_weight
            + self.freshness_factor * freshness_weight
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
    confidence_factors: ConfidenceFactors | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class HistoricalTrade:
    """Historical trade record for probability estimation."""

    exchange: str
    symbol: str
    entry_time: datetime
    exit_time: datetime | None
    entry_funding_rate: float
    exit_funding_rate: float | None
    profit: float
    position_size: float
    side: str  # "LONG" or "SHORT"
    is_complete: bool
    metadata: dict[str, Any] = field(default_factory=dict)


class ArbitrageOpportunity(BaseModel):
    """
    ArbitrageOpportunity represents a funding rate arbitrage opportunity between two exchanges for a
    given symbol. This model is mutable because it may be updated with analytics, sizing, or
    confidence scores after initial creation.

    Fields:
        symbol (str): Trading symbol.
        long_exchange (str): Exchange to go long.
        short_exchange (str): Exchange to go short.
        long_price (Decimal): Long entry price (must be positive).
        short_price (Decimal): Short entry price (must be positive).
        long_funding_rate (Decimal): Funding rate on long exchange.
        short_funding_rate (Decimal): Funding rate on short exchange.
        net_funding_differential (Decimal): Net funding advantage (long - short).
        timestamp (datetime): UTC timestamp of opportunity detection.
        expected_profit (Decimal | None): Optional expected profit estimate.
        basis_volatility (float | None): Optional basis volatility measure.
        utility_score (float | None): Optional utility score for ranking.
        optimal_size (Decimal | None): Optional optimal trade size.
        confidence_score (float | None): Optional confidence score (model-derived or validation).
        integrated_funding_data (IntegratedFundingData | None): Optional reference to\
            integrated funding data.
        adjusted_thresholds (dict[str, float] | None): Optional adjusted thresholds for\
            risk/validation.
        metadata (dict[str, Any] | None): Optional extra metadata for analytics/debugging.
        expiration_timestamp (float | None): Optional expiry (epoch seconds).
        id (str): Unique identifier (UUID).

    Notes:
        - All financial fields use Decimal for accuracy.
        - Use this model for opportunity tracking, analytics, and strategy input.
        - This model is mutable to allow enrichment after creation.
    """

    symbol: str
    long_exchange: str
    short_exchange: str
    long_price: Decimal = Field(gt=0, description="Long price must be positive.")
    short_price: Decimal = Field(gt=0, description="Short price must be positive.")
    long_funding_rate: Decimal
    short_funding_rate: Decimal
    net_funding_differential: Decimal
    timestamp: datetime
    # Optional fields: may not be available at opportunity creation
    expected_profit: Decimal | None = None
    basis_volatility: float | None = None
    utility_score: float | None = None
    optimal_size: Decimal | None = Field(
        default=None, gt=0, description="Optimal size, if calculated by risk/position sizing."
    )
    confidence_score: float | None = None
    integrated_funding_data: IntegratedFundingData | None = None
    adjusted_thresholds: dict[str, float] | None = None
    metadata: dict[str, Any] | None = None
    expiration_timestamp: float | None = None
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))

    model_config = ConfigDict(extra="forbid", validate_assignment=True, coerce_numbers_to_str=True)

    @field_validator(
        "long_price",
        "short_price",
        "long_funding_rate",
        "short_funding_rate",
        "net_funding_differential",
        "optimal_size",
        "expected_profit",
        mode="before",
    )
    @classmethod
    def parse_decimal_fields(
        cls, v: str | int | float | Decimal | None, info: object
    ) -> Decimal | None:
        return parse_decimal_value(v)

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v: str | int | float | datetime | None, info: object) -> datetime:
        dt = parse_datetime_utc(v)
        if dt is None:
            raise ValueError("timestamp cannot be None")
        return dt

    @model_validator(mode="after")
    def set_expiration(self) -> Self:
        """
        Set expiration_timestamp to 1 hour after timestamp (UTC).
        """
        if self.timestamp.tzinfo:
            object.__setattr__(self, "expiration_timestamp", self.timestamp.timestamp() + 3600)
        else:
            object.__setattr__(
                self, "expiration_timestamp", self.timestamp.replace(tzinfo=UTC).timestamp() + 3600
            )
        return self

    @model_validator(mode="after")
    def validate_required_fields(self) -> Self:
        # All required fields are enforced by Pydantic; no need to check for None.
        return self

    @model_validator(mode="after")
    def check_arbitrage_logic(self) -> Self:
        """
        Ensure all required financial fields are positive where appropriate.
        """
        if self.long_price <= 0:
            raise ValueError("Long price must be positive.")
        if self.short_price <= 0:
            raise ValueError("Short price must be positive.")
        if self.optimal_size is not None and self.optimal_size <= 0:
            raise ValueError("Optimal size must be positive if present.")
        # No need to check for None or always-true conditions on required fields
        return self
