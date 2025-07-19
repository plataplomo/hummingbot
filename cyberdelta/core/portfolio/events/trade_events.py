"""Trade-related portfolio events."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Unpack

from cyberdelta.core.models import Trade
from cyberdelta.core.portfolio.events.base.base_event import (
    BasePortfolioEvent,
    EventMetadataKwargsWithoutExchangeSymbol,
    EventType,
)


@dataclass
class TradeReceivedEvent(BasePortfolioEvent[Trade]):
    """Event fired when a new trade is received."""

    def __init__(
        self, trade: Trade, **kwargs: Unpack[EventMetadataKwargsWithoutExchangeSymbol]
    ) -> None:
        """Initialize trade received event.

        Args:
            trade: The received trade
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.TRADE_RECEIVED,
            data=trade,
        )

        # Set exchange and symbol metadata
        self.metadata.exchange_id = trade.exchange
        self.metadata.symbol = trade.symbol

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags = kwargs["tags"]

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize trade data."""
        return {
            "trade_id": self.data.id,
            "exchange_id": self.data.exchange,
            "symbol": self.data.symbol,
            "side": self.data.side,
            "size": str(self.data.quantity),
            "price": str(self.data.price),
            "fee": str(self.data.fee),
            "fee_currency": self.data.fee_asset,
            "executed_at": self.data.executed_at,
            "order_id": self.data.order_id,
        }


@dataclass
class TradeValidatedEvent(BasePortfolioEvent[Trade]):
    """Event fired when a trade passes validation."""

    def __init__(
        self,
        trade: Trade,
        validation_results: dict[str, Any] | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchangeSymbol],
    ) -> None:
        """Initialize trade validated event.

        Args:
            trade: The validated trade
            validation_results: Optional validation results
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.TRADE_VALIDATED,
            data=trade,
        )

        self.metadata.exchange_id = trade.exchange
        self.metadata.symbol = trade.symbol

        # Store validation results in tags
        if validation_results:
            self.metadata.tags["validation_passed"] = "true"
            self.metadata.tags["validation_checks"] = str(len(validation_results))

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize trade data."""
        return {
            "trade_id": self.data.id,
            "exchange_id": self.data.exchange,
            "symbol": self.data.symbol,
            "side": self.data.side,
            "size": str(self.data.quantity),
            "price": str(self.data.price),
            "executed_at": self.data.executed_at,
        }


@dataclass
class TradeProcessedEvent(BasePortfolioEvent[Trade]):
    """Event fired when a trade is successfully processed."""

    def __init__(
        self,
        trade: Trade,
        position_id: str | None = None,
        realized_pnl: float | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchangeSymbol],
    ) -> None:
        """Initialize trade processed event.

        Args:
            trade: The processed trade
            position_id: ID of affected position
            realized_pnl: Realized P&L from trade
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.TRADE_PROCESSED,
            data=trade,
        )

        self.metadata.exchange_id = trade.exchange
        self.metadata.symbol = trade.symbol

        # Store processing results
        if position_id:
            self.metadata.tags["position_id"] = position_id
        if realized_pnl is not None:
            self.metadata.tags["realized_pnl"] = str(realized_pnl)

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize trade data."""
        return {
            "trade_id": self.data.id,
            "exchange_id": self.data.exchange,
            "symbol": self.data.symbol,
            "side": self.data.side,
            "size": str(self.data.quantity),
            "price": str(self.data.price),
            "fee": str(self.data.fee),
            "executed_at": self.data.executed_at,
        }


@dataclass
class TradeRejectedEvent(BasePortfolioEvent[dict[str, Any]]):
    """Event fired when a trade is rejected."""

    def __init__(
        self,
        trade_data: dict[str, Any],
        reason: str,
        error_code: str | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchangeSymbol],
    ) -> None:
        """Initialize trade rejected event.

        Args:
            trade_data: Raw trade data that was rejected
            reason: Rejection reason
            error_code: Optional error code
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.TRADE_REJECTED,
            data=trade_data,
        )

        # Extract exchange and symbol if available
        if "exchange_id" in trade_data:
            self.metadata.exchange_id = trade_data["exchange_id"]
        if "symbol" in trade_data:
            self.metadata.symbol = trade_data["symbol"]

        # Store rejection details
        self.metadata.tags["rejection_reason"] = reason
        if error_code:
            self.metadata.tags["error_code"] = error_code

        # Apply any additional metadata using typed fields
        if "source_component" in kwargs:
            self.metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            self.metadata.correlation_id = kwargs["correlation_id"]
        if "priority" in kwargs:
            self.metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            self.metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            self.metadata.tags.update(kwargs["tags"])

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize rejection data."""
        return {
            "trade_data": self.data,
            "rejection_reason": self.metadata.tags.get("rejection_reason", ""),
            "error_code": self.metadata.tags.get("error_code"),
        }
