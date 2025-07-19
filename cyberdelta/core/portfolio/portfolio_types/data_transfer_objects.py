"""Data Transfer Objects (DTOs) for portfolio data structures."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.core.enums import OrderSide, OrderStatus, OrderType
from cyberdelta.utils.constants import PositionSide


# Type aliases for metadata
MetadataDict = dict[str, str | int | float | bool]
MetricsDict = dict[str, str | int | float | bool]


# Default factory functions for Pydantic fields
def _metadata_factory() -> MetadataDict:
    """Factory function for MetadataDict."""
    return {}


def _metrics_factory() -> MetricsDict:
    """Factory function for MetricsDict."""
    return {}


class PositionDTO(BaseModel):
    """DTO for derivative position data."""
    
    model_config = ConfigDict(frozen=True)

    exchange_id: str
    symbol: str
    side: PositionSide
    quantity: Decimal
    entry_price: Decimal
    mark_price: Decimal
    liquidation_price: Decimal | None
    unrealized_pnl: Decimal
    realized_pnl: Decimal | None
    total_pnl: Decimal | None
    margin: Decimal | None
    leverage: Decimal | None
    last_update: datetime
    metadata: MetadataDict = Field(default_factory=_metadata_factory)


class BalanceDTO(BaseModel):
    """DTO for spot balance data."""
    
    model_config = ConfigDict(frozen=True)

    exchange_id: str
    asset: str
    total_quantity: Decimal
    available_quantity: Decimal
    locked_quantity: Decimal
    last_update: datetime
    metadata: MetadataDict = Field(default_factory=_metadata_factory)


class OrderDTO(BaseModel):
    """DTO for order data."""
    
    model_config = ConfigDict(frozen=True)

    exchange_id: str
    order_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    status: OrderStatus
    price: Decimal | None
    quantity: Decimal
    filled_quantity: Decimal
    remaining_quantity: Decimal
    created_at: datetime
    updated_at: datetime
    metadata: MetadataDict = Field(default_factory=_metadata_factory)


class PortfolioSnapshotDTO(BaseModel):
    """DTO for complete portfolio snapshot."""
    
    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    positions: list[PositionDTO]
    balances: list[BalanceDTO]
    orders: list[OrderDTO]
    metrics: MetricsDict = Field(default_factory=_metrics_factory)
    metadata: MetadataDict = Field(default_factory=_metadata_factory)


class ManagerStatsDTO(BaseModel):
    """DTO for manager statistics."""
    
    model_config = ConfigDict(frozen=True)

    manager_name: str
    total_operations: int
    successful_operations: int
    failed_operations: int
    last_operation_time: datetime | None
    average_operation_time_ms: float
    error_rate: float
    metadata: MetadataDict = Field(default_factory=_metadata_factory)

