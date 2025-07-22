"""Balance-related portfolio events."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Unpack

from pydantic import BaseModel, Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.core.portfolio.events.base.base_event import (
    BasePortfolioEvent,
    EventMetadata,
    EventMetadataKwargs,
    EventMetadataKwargsWithoutExchange,
    EventType,
)
from cyberdelta.core.portfolio.exceptions import (
    EmptyBalanceFieldError,
    NegativeBalanceError,
    NonFiniteBalanceError,
    NonPositiveTimestampError,
)


class BalanceDiscrepancy(BaseModel):
    """Simple model for balance discrepancy data."""

    expected: Decimal = Field(description="Expected balance amount")
    actual: Decimal = Field(description="Actual balance amount")
    difference: Decimal = Field(description="Difference between expected and actual")
    percentage_diff: float = Field(description="Percentage difference")


@dataclass
class BalanceChange:
    """Represents a balance change."""

    exchange_id: str
    asset: str
    change_amount: Decimal
    change_reason: str  # e.g., "trade", "deposit", "withdrawal", "fee"
    previous_balance: Decimal
    new_balance: Decimal
    reference_id: str | None = None  # e.g., trade_id, withdrawal_id

    @field_validator("exchange_id", "asset", "change_reason", mode="before")
    @classmethod
    def validate_strings(cls, v: str, info: ValidationInfo) -> str:
        """Validate string fields are non-empty."""
        if not v or not v.strip():
            raise EmptyBalanceFieldError(field_name=info.field_name or "balance_field")
        return v.strip()

    @field_validator("previous_balance", "new_balance", mode="before")
    @classmethod
    def validate_balances(cls, v: Decimal, info: ValidationInfo) -> Decimal:
        """Validate balance amounts are finite and non-negative."""
        if not v.is_finite():
            raise NonFiniteBalanceError(field_name=info.field_name or "balance")
        if v < 0:
            raise NegativeBalanceError(field_name=info.field_name or "balance")
        return v

    @field_validator("reference_id", mode="before")
    @classmethod
    def validate_reference_id(cls, v: str | None) -> str | None:
        """Validate reference ID is either None or non-empty."""
        if v is not None and not v.strip():
            return None
        return v


@dataclass
class BalanceSnapshot:
    """Represents a complete balance snapshot."""

    exchange_id: str
    balances: dict[str, Decimal]  # asset -> balance
    timestamp: float
    total_value_usd: Decimal | None = None

    @field_validator("exchange_id", mode="before")
    @classmethod
    def validate_exchange_id(cls, v: str) -> str:
        """Validate exchange ID is non-empty."""
        if not v or not v.strip():
            raise EmptyBalanceFieldError(field_name="exchange_id")
        return v.strip()

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v: float) -> float:
        """Validate timestamp is positive."""
        if v <= 0:
            raise NonPositiveTimestampError
        return v

    @field_validator("balances", mode="before")
    @classmethod
    def validate_balances_dict(cls, v: dict[str, Decimal]) -> dict[str, Decimal]:
        """Validate all balances in the dictionary are finite and non-negative."""
        for asset, balance in v.items():
            if not balance.is_finite():
                raise NonFiniteBalanceError(field_name=f"balance[{asset}]")
            if balance < 0:
                raise NegativeBalanceError(field_name=f"balance[{asset}]")
        return v

    @field_validator("total_value_usd", mode="before")
    @classmethod
    def validate_total_value(cls, v: Decimal | None) -> Decimal | None:
        """Validate total value is finite if provided."""
        if v is not None:
            if not v.is_finite():
                raise NonFiniteBalanceError(field_name="total_value_usd")
            if v < 0:
                raise NegativeBalanceError(field_name="total_value_usd")
        return v


@dataclass
class BalanceUpdatedEvent(BasePortfolioEvent[BalanceChange]):
    """Event fired when a balance is updated."""

    @classmethod
    def create(
        cls, balance_change: BalanceChange, **kwargs: Unpack[EventMetadataKwargs]
    ) -> BalanceUpdatedEvent:
        """Create a balance updated event with proper initialization.

        Args:
            balance_change: The balance change details
            **kwargs: Additional metadata fields
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(exchange_id=balance_change.exchange_id)

        # Apply additional fields from kwargs
        if "source_component" in kwargs:
            metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "symbol" in kwargs:
            metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        # Set standard tags
        metadata.tags["asset"] = balance_change.asset
        metadata.tags["change_reason"] = balance_change.change_reason
        if balance_change.reference_id:
            metadata.tags["reference_id"] = balance_change.reference_id

        return cls(event_type=EventType.BALANCE_UPDATED, data=balance_change, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize balance change data."""
        return {
            "exchange_id": self.data.exchange_id,
            "asset": self.data.asset,
            "previous_balance": str(self.data.previous_balance),
            "new_balance": str(self.data.new_balance),
            "change_amount": str(self.data.change_amount),
            "change_reason": self.data.change_reason,
            "reference_id": self.data.reference_id,
        }


@dataclass
class BalanceReconciledEvent(BasePortfolioEvent[BalanceSnapshot]):
    """Event fired when balances are reconciled with exchange."""

    @classmethod
    def create(
        cls,
        balance_snapshot: BalanceSnapshot,
        discrepancies: dict[str, BalanceDiscrepancy] | None = None,
        **kwargs: Unpack[EventMetadataKwargs],
    ) -> BalanceReconciledEvent:
        """Create a balance reconciled event with proper initialization.

        Args:
            balance_snapshot: Current balance snapshot
            discrepancies: Any found discrepancies
            **kwargs: Additional metadata fields
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(exchange_id=balance_snapshot.exchange_id)

        # Apply additional fields from kwargs
        if "source_component" in kwargs:
            metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "symbol" in kwargs:
            metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        # Store reconciliation results
        if discrepancies:
            metadata.tags["has_discrepancies"] = "true"
            metadata.tags["discrepancy_count"] = str(len(discrepancies))
        else:
            metadata.tags["has_discrepancies"] = "false"

        return cls(
            event_type=EventType.BALANCE_RECONCILED, data=balance_snapshot, metadata=metadata
        )

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize balance snapshot data."""
        return {
            "exchange_id": self.data.exchange_id,
            "balances": {asset: str(balance) for asset, balance in self.data.balances.items()},
            "timestamp": self.data.timestamp,
            "total_value_usd": str(self.data.total_value_usd)
            if self.data.total_value_usd
            else None,
        }


class BalanceErrorData(BaseModel):
    """Data for balance-related errors."""

    exchange_id: str = Field(description="Exchange where error occurred")
    asset: str | None = Field(default=None, description="Asset involved if applicable")
    error_type: str = Field(description="Type of error")
    error_message: str = Field(description="Error message")
    error_data: dict[str, Any] = Field(default_factory=dict, description="Additional error data")


@dataclass
class BalanceErrorEvent(BasePortfolioEvent[BalanceErrorData]):
    """Event fired when a balance operation fails."""

    @classmethod
    def create(
        cls,
        exchange_id: str,
        asset: str | None,
        error_type: str,
        error_message: str,
        error_data: dict[str, Any] | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchange],
    ) -> BalanceErrorEvent:
        """Create a balance error event with proper initialization.

        Args:
            exchange_id: Exchange where error occurred
            asset: Asset involved (if applicable)
            error_type: Type of error
            error_message: Error message
            error_data: Additional error data
            **kwargs: Additional metadata fields
        """
        # Create BalanceErrorData
        data = BalanceErrorData(
            exchange_id=exchange_id,
            asset=asset,
            error_type=error_type,
            error_message=error_message,
            error_data=error_data or {},
        )

        # Build metadata with explicit fields first
        metadata = EventMetadata(exchange_id=exchange_id)

        # Apply additional fields from kwargs
        if "source_component" in kwargs:
            metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "symbol" in kwargs:
            metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])
        metadata.tags["error_type"] = error_type

        if asset:
            metadata.tags["asset"] = asset

        return cls(event_type=EventType.BALANCE_ERROR, data=data, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize error data."""
        return {
            "exchange_id": self.data.exchange_id,
            "asset": self.data.asset,
            "error_type": self.data.error_type,
            "error_message": self.data.error_message,
            "error_data": self.data.error_data,
        }
