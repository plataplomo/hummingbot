"""Trade-related portfolio events."""

from __future__ import annotations

from typing import Any, Unpack

from pydantic import BaseModel, Field
from pydantic.dataclasses import dataclass

from cyberdelta.core.models import Trade
from cyberdelta.core.portfolio.events.base.base_event import (
    BasePortfolioEvent,
    EventMetadata,
    EventMetadataKwargsWithoutExchangeSymbol,
    EventType,
)


class ValidationResults(BaseModel):
    """Results from trade validation."""

    checks_passed: int = Field(ge=0, description="Number of validation checks passed")
    checks_failed: int = Field(ge=0, description="Number of validation checks failed")
    warnings: list[str] = Field(default_factory=list, description="Validation warnings")
    errors: list[str] = Field(default_factory=list, description="Validation errors")
    is_valid: bool = Field(description="Overall validation result")


@dataclass
class TradeReceivedEvent(BasePortfolioEvent[Trade]):
    """Event fired when a new trade is received."""

    @classmethod
    def create(
        cls, trade: Trade, **kwargs: Unpack[EventMetadataKwargsWithoutExchangeSymbol]
    ) -> TradeReceivedEvent:
        """Create a trade received event with proper initialization.

        Args:
            trade: The received trade
            **kwargs: Additional metadata fields
            
        Returns:
            TradeReceivedEvent: A new trade received event instance.
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(exchange_id=trade.exchange, symbol=trade.symbol)

        # Apply additional fields from kwargs
        if "source_component" in kwargs:
            metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        return cls(event_type=EventType.TRADE_RECEIVED, data=trade, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize trade data.
        
        Returns:
            dict[str, Any]: Serialized trade data with trade ID, exchange, symbol, 
                side, size, price, fee details, execution time, and order ID.
        """
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

    @classmethod
    def create(
        cls,
        trade: Trade,
        validation_results: ValidationResults | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchangeSymbol],
    ) -> TradeValidatedEvent:
        """Create a trade validated event with proper initialization.

        Args:
            trade: The validated trade
            validation_results: Optional validation results
            **kwargs: Additional metadata fields
            
        Returns:
            TradeValidatedEvent: A new trade validated event instance.
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(exchange_id=trade.exchange, symbol=trade.symbol)

        # Apply additional fields from kwargs
        if "source_component" in kwargs:
            metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        # Store validation results in tags
        if validation_results:
            metadata.tags["validation_passed"] = str(validation_results.is_valid).lower()
            metadata.tags["validation_checks"] = str(
                validation_results.checks_passed + validation_results.checks_failed
            )

        return cls(event_type=EventType.TRADE_VALIDATED, data=trade, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize trade data.
        
        Returns:
            dict[str, Any]: Serialized trade data with trade ID, exchange, symbol,
                side, size, price, and execution time.
        """
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

    @classmethod
    def create(
        cls,
        trade: Trade,
        position_id: str | None = None,
        realized_pnl: float | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchangeSymbol],
    ) -> TradeProcessedEvent:
        """Create a trade processed event with proper initialization.

        Args:
            trade: The processed trade
            position_id: ID of affected position
            realized_pnl: Realized P&L from trade
            **kwargs: Additional metadata fields
            
        Returns:
            TradeProcessedEvent: A new trade processed event instance.
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(exchange_id=trade.exchange, symbol=trade.symbol)

        # Apply additional fields from kwargs
        if "source_component" in kwargs:
            metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        # Store processing results
        if position_id:
            metadata.tags["position_id"] = position_id
        if realized_pnl is not None:
            metadata.tags["realized_pnl"] = str(realized_pnl)

        return cls(event_type=EventType.TRADE_PROCESSED, data=trade, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize trade data.
        
        Returns:
            dict[str, Any]: Serialized trade data with trade ID, exchange, symbol,
                side, size, price, fee details, and execution time.
        """
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


class RejectedTradeData(BaseModel):
    """Data for a rejected trade."""

    trade_id: str | None = Field(default=None, description="Trade ID if available")
    exchange_id: str | None = Field(default=None, description="Exchange ID")
    symbol: str | None = Field(default=None, description="Trading symbol")
    side: str | None = Field(default=None, description="Trade side")
    size: str | None = Field(default=None, description="Trade size")
    price: str | None = Field(default=None, description="Trade price")
    order_id: str | None = Field(default=None, description="Associated order ID")
    rejection_reason: str = Field(description="Reason for rejection")
    error_code: str | None = Field(default=None, description="Error code if available")
    raw_data: dict[str, Any] = Field(default_factory=dict, description="Raw trade data")


@dataclass
class TradeRejectedEvent(BasePortfolioEvent[RejectedTradeData]):
    """Event fired when a trade is rejected."""

    @classmethod
    def create(
        cls,
        trade_data: dict[str, Any],
        reason: str,
        error_code: str | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchangeSymbol],
    ) -> TradeRejectedEvent:
        """Create a trade rejected event with proper initialization.

        Args:
            trade_data: Raw trade data that was rejected
            reason: Rejection reason
            error_code: Optional error code
            **kwargs: Additional metadata fields
            
        Returns:
            TradeRejectedEvent: A new trade rejected event instance.
        """
        # Create RejectedTradeData from raw data
        rejected_data = RejectedTradeData(
            trade_id=trade_data.get("trade_id"),
            exchange_id=trade_data.get("exchange_id"),
            symbol=trade_data.get("symbol"),
            side=trade_data.get("side"),
            size=str(trade_data["size"]) if "size" in trade_data else None,
            price=str(trade_data["price"]) if "price" in trade_data else None,
            order_id=trade_data.get("order_id"),
            rejection_reason=reason,
            error_code=error_code,
            raw_data=trade_data,
        )

        # Build metadata with explicit fields first
        metadata = EventMetadata(
            exchange_id=rejected_data.exchange_id,
            symbol=rejected_data.symbol,
        )

        # Apply additional fields from kwargs
        if "source_component" in kwargs:
            metadata.source_component = kwargs["source_component"]
        if "correlation_id" in kwargs:
            metadata.correlation_id = kwargs["correlation_id"]
        if "priority" in kwargs:
            metadata.priority = kwargs["priority"]
        if "retry_count" in kwargs:
            metadata.retry_count = kwargs["retry_count"]
        if "tags" in kwargs:
            metadata.tags.update(kwargs["tags"])

        return cls(event_type=EventType.TRADE_REJECTED, data=rejected_data, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize rejection data.
        
        Returns:
            dict[str, Any]: Serialized rejection data with trade ID, exchange, symbol,
                side, size, price, order ID, rejection reason, error code, and raw data.
        """
        return {
            "trade_id": self.data.trade_id,
            "exchange_id": self.data.exchange_id,
            "symbol": self.data.symbol,
            "side": self.data.side,
            "size": self.data.size,
            "price": self.data.price,
            "order_id": self.data.order_id,
            "rejection_reason": self.data.rejection_reason,
            "error_code": self.data.error_code,
            "raw_data": self.data.raw_data,
        }
