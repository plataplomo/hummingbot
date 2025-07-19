"""Position-related portfolio events."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Unpack

from cyberdelta.core.portfolio.events.base.base_event import (
    BasePortfolioEvent,
    EventMetadataKwargsWithoutExchange,
    EventMetadataKwargsWithoutExchangeSymbol,
    EventType,
)


@dataclass
class PositionData:
    """Represents position data for events."""

    position_id: str
    exchange_id: str
    symbol: str
    side: str  # "LONG" or "SHORT"
    size: Decimal
    entry_price: Decimal
    current_price: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    margin_used: Decimal | None = None
    leverage: Decimal | None = None


@dataclass
class PositionOpenedEvent(BasePortfolioEvent[PositionData]):
    """Event fired when a new position is opened."""

    def __init__(
        self,
        position: PositionData,
        opening_trade_id: str | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchange],
    ) -> None:
        """Initialize position opened event.

        Args:
            position: The opened position data
            opening_trade_id: ID of trade that opened position
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.POSITION_OPENED,
            data=position,
        )

        self.metadata.exchange_id = position.exchange_id
        self.metadata.symbol = position.symbol
        self.metadata.tags["position_id"] = position.position_id
        self.metadata.tags["side"] = position.side

        if opening_trade_id:
            self.metadata.tags["opening_trade_id"] = opening_trade_id

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
        """Serialize position data."""
        return {
            "position_id": self.data.position_id,
            "exchange_id": self.data.exchange_id,
            "symbol": self.data.symbol,
            "side": self.data.side,
            "size": str(self.data.size),
            "entry_price": str(self.data.entry_price),
            "current_price": str(self.data.current_price),
            "unrealized_pnl": str(self.data.unrealized_pnl),
            "realized_pnl": str(self.data.realized_pnl),
            "margin_used": str(self.data.margin_used) if self.data.margin_used else None,
            "leverage": str(self.data.leverage) if self.data.leverage else None,
        }


@dataclass
class PositionUpdatedEvent(BasePortfolioEvent[PositionData]):
    """Event fired when a position is updated."""

    def __init__(
        self,
        position: PositionData,
        update_reason: str,
        previous_size: Decimal | None = None,
        size_change: Decimal | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchange],
    ) -> None:
        """Initialize position updated event.

        Args:
            position: The updated position data
            update_reason: Reason for update (e.g., "trade", "price_change")
            previous_size: Previous position size
            size_change: Change in position size
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.POSITION_UPDATED,
            data=position,
        )

        self.metadata.exchange_id = position.exchange_id
        self.metadata.symbol = position.symbol
        self.metadata.tags["position_id"] = position.position_id
        self.metadata.tags["side"] = position.side
        self.metadata.tags["update_reason"] = update_reason

        if previous_size is not None:
            self.metadata.tags["previous_size"] = str(previous_size)
        if size_change is not None:
            self.metadata.tags["size_change"] = str(size_change)

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
        """Serialize position data."""
        return {
            "position_id": self.data.position_id,
            "exchange_id": self.data.exchange_id,
            "symbol": self.data.symbol,
            "side": self.data.side,
            "size": str(self.data.size),
            "entry_price": str(self.data.entry_price),
            "current_price": str(self.data.current_price),
            "unrealized_pnl": str(self.data.unrealized_pnl),
            "realized_pnl": str(self.data.realized_pnl),
            "margin_used": str(self.data.margin_used) if self.data.margin_used else None,
            "leverage": str(self.data.leverage) if self.data.leverage else None,
        }


@dataclass
class PositionClosedEvent(BasePortfolioEvent[PositionData]):
    """Event fired when a position is closed."""

    def __init__(
        self,
        position: PositionData,
        closing_trade_id: str | None = None,
        close_reason: str | None = None,
        final_pnl: Decimal | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchange],
    ) -> None:
        """Initialize position closed event.

        Args:
            position: The closed position data
            closing_trade_id: ID of trade that closed position
            close_reason: Reason for closure (e.g., "manual", "stop_loss", "liquidation")
            final_pnl: Final realized P&L
            **kwargs: Additional metadata fields
        """
        super().__init__(
            event_type=EventType.POSITION_CLOSED,
            data=position,
        )

        self.metadata.exchange_id = position.exchange_id
        self.metadata.symbol = position.symbol
        self.metadata.tags["position_id"] = position.position_id
        self.metadata.tags["side"] = position.side

        if closing_trade_id:
            self.metadata.tags["closing_trade_id"] = closing_trade_id
        if close_reason:
            self.metadata.tags["close_reason"] = close_reason
        if final_pnl is not None:
            self.metadata.tags["final_pnl"] = str(final_pnl)

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
        """Serialize position data."""
        return {
            "position_id": self.data.position_id,
            "exchange_id": self.data.exchange_id,
            "symbol": self.data.symbol,
            "side": self.data.side,
            "size": str(self.data.size),
            "entry_price": str(self.data.entry_price),
            "current_price": str(self.data.current_price),
            "unrealized_pnl": str(self.data.unrealized_pnl),
            "realized_pnl": str(self.data.realized_pnl),
            "margin_used": str(self.data.margin_used) if self.data.margin_used else None,
            "leverage": str(self.data.leverage) if self.data.leverage else None,
        }


@dataclass
class PositionErrorEvent(BasePortfolioEvent[dict[str, Any]]):
    """Event fired when a position operation fails."""

    def __init__(
        self,
        exchange_id: str,
        symbol: str,
        position_id: str | None,
        error_type: str,
        error_message: str,
        error_data: dict[str, Any] | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchangeSymbol],
    ) -> None:
        """Initialize position error event.

        Args:
            exchange_id: Exchange where error occurred
            symbol: Symbol involved
            position_id: Position ID if applicable
            error_type: Type of error
            error_message: Error message
            error_data: Additional error data
            **kwargs: Additional metadata fields
        """
        data = {
            "exchange_id": exchange_id,
            "symbol": symbol,
            "position_id": position_id,
            "error_type": error_type,
            "error_message": error_message,
            "error_data": error_data or {},
        }

        super().__init__(
            event_type=EventType.POSITION_ERROR,
            data=data,
        )

        self.metadata.exchange_id = exchange_id
        self.metadata.symbol = symbol
        self.metadata.tags["error_type"] = error_type

        if position_id:
            self.metadata.tags["position_id"] = position_id

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
        """Serialize error data."""
        return self.data
