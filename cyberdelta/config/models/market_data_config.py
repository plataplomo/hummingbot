"""Market data configuration models.

This module contains Pydantic models for market data service configuration,
including cache settings, data fetching parameters, and aggregation settings.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class MarketDataCacheSettings(BaseModel):
    """Market data cache configuration settings."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = True
    default_ttl: float = Field(
        default=60.0, gt=0, le=3600, description="Default TTL for cached market data in seconds"
    )
    stale_while_revalidate: float = Field(
        default=10.0,
        gt=0,
        le=600,
        description="Time in seconds to serve stale data while revalidating",
    )
    max_order_books_per_exchange: int = Field(
        default=10, gt=0, le=100, description="Maximum number of order books to cache per exchange"
    )
    max_tickers_per_exchange: int = Field(
        default=50, gt=0, le=500, description="Maximum number of tickers to cache per exchange"
    )


class MarketDataFetchSettings(BaseModel):
    """Settings for fetching market data from exchanges."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    parallel_fetch_enabled: bool = Field(
        default=True, description="Whether to fetch data from multiple exchanges in parallel"
    )
    max_concurrent_requests: int = Field(
        default=10, gt=0, le=50, description="Maximum concurrent requests per exchange"
    )
    order_book_depth: int = Field(
        default=20, gt=0, le=100, description="Default depth for order book fetching"
    )
    retry_on_empty_response: bool = Field(
        default=True, description="Whether to retry if exchange returns empty response"
    )
    empty_response_max_retries: int = Field(
        default=3, gt=0, le=10, description="Maximum retries for empty responses"
    )


class MarketDataAggregationSettings(BaseModel):
    """Settings for market data aggregation across exchanges."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    cross_exchange_aggregation: bool = Field(
        default=True, description="Whether to aggregate data across exchanges"
    )
    prefer_recent_data: bool = Field(
        default=True, description="Whether to prefer more recent data when aggregating"
    )
    max_age_difference_seconds: float = Field(
        default=5.0,
        gt=0,
        le=60,
        description="Maximum age difference in seconds for data to be considered comparable",
    )


class MarketDataSettings(BaseModel):
    """Complete market data configuration."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    cache: MarketDataCacheSettings = Field(
        default_factory=MarketDataCacheSettings, description="Cache configuration for market data"
    )
    fetch: MarketDataFetchSettings = Field(
        default_factory=MarketDataFetchSettings, description="Settings for fetching market data"
    )
    aggregation: MarketDataAggregationSettings = Field(
        default_factory=MarketDataAggregationSettings,
        description="Settings for aggregating market data",
    )
