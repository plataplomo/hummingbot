"""Balance-related portfolio events."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Unpack

from cyberdelta.core.portfolio.events.base.base_event import (
    BasePortfolioEvent,
    EventMetadataKwargs,
    EventMetadataKwargsWithoutExchange,
    EventType,
)


@dataclass
class BalanceChange:
    """Represents a balance change."""

    exchange_id: str
    asset: str
    previous_balance: Decimal
    new_balance: Decimal
    change_amount: Decimal
    change_reason: str  # e.g., "trade", "deposit", "withdrawal", "fee"
    reference_id: str | None = None  # e.g., trade_id, withdrawal_id


@dataclass
class BalanceSnapshot:
    """Represents a complete balance snapshot."""

    exchange_id: str
    balances: dict[str, Decimal]  # asset -> balance
    timestamp: float
    total_value_usd: Decimal | None = None


@dataclass
class BalanceUpdatedEvent(BasePortfolioEvent[BalanceChange]):
    """Event fired when a balance is updated."""

    def __init__(
        self, balance_change: BalanceChange, **kwargs: Unpack[EventMetadataKwargs]
    ) -> None:
        """Initialize balance updated event.

        Args:
            balance_change: The balance change details
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.BALANCE_UPDATED,
            data=balance_change,
        )

        self.metadata.exchange_id = balance_change.exchange_id
        self.metadata.tags["asset"] = balance_change.asset
        self.metadata.tags["change_reason"] = balance_change.change_reason

        if balance_change.reference_id:
            self.metadata.tags["reference_id"] = balance_change.reference_id

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "symbol" in kwargs:
            self.metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

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

    def __init__(
        self,
        balance_snapshot: BalanceSnapshot,
        discrepancies: dict[str, dict[str, Any]] | None = None,
        **kwargs: Unpack[EventMetadataKwargs],
    ) -> None:
        """Initialize balance reconciled event.

        Args:
            balance_snapshot: Current balance snapshot
            discrepancies: Any found discrepancies
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.BALANCE_RECONCILED,
            data=balance_snapshot,
        )

        self.metadata.exchange_id = balance_snapshot.exchange_id

        # Store reconciliation results
        if discrepancies:
            self.metadata.tags["has_discrepancies"] = "true"
            self.metadata.tags["discrepancy_count"] = str(len(discrepancies))
        else:
            self.metadata.tags["has_discrepancies"] = "false"

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "symbol" in kwargs:
            self.metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

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


@dataclass
class BalanceErrorEvent(BasePortfolioEvent[dict[str, Any]]):
    """Event fired when a balance operation fails."""

    def __init__(
        self,
        exchange_id: str,
        asset: str | None,
        error_type: str,
        error_message: str,
        error_data: dict[str, Any] | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchange],
    ) -> None:
        """Initialize balance error event.

        Args:
            exchange_id: Exchange where error occurred
            asset: Asset involved (if applicable)
            error_type: Type of error
            error_message: Error message
            error_data: Additional error data
            **kwargs: Additional metadata fields
        """
        data = {
            "exchange_id": exchange_id,
            "asset": asset,
            "error_type": error_type,
            "error_message": error_message,
            "error_data": error_data or {},
        }

        super().__init__(
            event_type=EventType.BALANCE_ERROR,
            data=data,
        )

        self.metadata.exchange_id = exchange_id
        self.metadata.tags["error_type"] = error_type

        if asset:
            self.metadata.tags["asset"] = asset

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "symbol" in kwargs:
            self.metadata.symbol = kwargs["symbol"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize error data."""
        return self.data
