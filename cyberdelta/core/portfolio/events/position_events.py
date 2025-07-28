"""Position-related portfolio events."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Unpack

from pydantic import BaseModel, Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.core.portfolio.events.base.base_event import (
    BasePortfolioEvent,
    EventMetadata,
    EventMetadataKwargsWithoutExchange,
    EventMetadataKwargsWithoutExchangeSymbol,
    EventType,
)
from cyberdelta.core.portfolio.exceptions import (
    EmptyPositionFieldError,
    InvalidPositionSideError,
    NegativeMarginError,
    NonFiniteFinancialValueError,
    NonFiniteLeverageError,
    NonFiniteMarginError,
    NonFinitePriceError,
    NonPositiveLeverageError,
    NonPositivePriceError,
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

    @field_validator("position_id", "exchange_id", "symbol", mode="before")
    @classmethod
    def validate_strings(cls, v: str, info: ValidationInfo) -> str:
        """Validate required string fields are non-empty.
        
        Returns:
            str: The validated and trimmed string value.
            
        Raises:
            EmptyPositionFieldError: If the field is empty or contains only whitespace.
        """
        if not v or not v.strip():
            raise EmptyPositionFieldError(field_name=info.field_name or "position_field")
        return v.strip()

    @field_validator("side", mode="before")
    @classmethod
    def validate_side(cls, v: str) -> str:
        """Validate position side is LONG or SHORT.
        
        Returns:
            str: The validated side in uppercase (LONG or SHORT).
            
        Raises:
            InvalidPositionSideError: If the side is not LONG or SHORT.
        """
        if v.upper() not in {"LONG", "SHORT"}:
            raise InvalidPositionSideError(side=v)
        return v.upper()

    @field_validator("entry_price", "current_price", mode="before")
    @classmethod
    def validate_prices(cls, v: Decimal) -> Decimal:
        """Validate prices are finite and positive.
        
        Returns:
            Decimal: The validated price.
            
        Raises:
            NonFinitePriceError: If the price is not finite (inf/nan).
            NonPositivePriceError: If the price is zero or negative.
        """
        if not v.is_finite():
            raise NonFinitePriceError
        if v <= 0:
            raise NonPositivePriceError
        return v

    @field_validator("size", "unrealized_pnl", "realized_pnl", mode="before")
    @classmethod
    def validate_decimals(cls, v: Decimal) -> Decimal:
        """Validate decimal values are finite.
        
        Returns:
            Decimal: The validated decimal value.
            
        Raises:
            NonFiniteFinancialValueError: If the value is not finite (inf/nan).
        """
        if not v.is_finite():
            raise NonFiniteFinancialValueError
        return v

    @field_validator("margin_used", mode="before")
    @classmethod
    def validate_margin_used(cls, v: Decimal | None) -> Decimal | None:
        """Validate margin used is finite and non-negative if provided.
        
        Returns:
            Decimal | None: The validated margin value or None.
            
        Raises:
            NonFiniteMarginError: If the margin is not finite (inf/nan).
            NegativeMarginError: If the margin is negative.
        """
        if v is not None:
            if not v.is_finite():
                raise NonFiniteMarginError
            if v < 0:
                raise NegativeMarginError
        return v

    @field_validator("leverage", mode="before")
    @classmethod
    def validate_leverage(cls, v: Decimal | None) -> Decimal | None:
        """Validate leverage is finite and positive if provided.
        
        Returns:
            Decimal | None: The validated leverage value or None.
            
        Raises:
            NonFiniteLeverageError: If the leverage is not finite (inf/nan).
            NonPositiveLeverageError: If the leverage is zero or negative.
        """
        if v is not None:
            if not v.is_finite():
                raise NonFiniteLeverageError
            if v <= 0:
                raise NonPositiveLeverageError
        return v


@dataclass
class PositionOpenedEvent(BasePortfolioEvent[PositionData]):
    """Event fired when a new position is opened."""

    @classmethod
    def create(
        cls,
        position: PositionData,
        opening_trade_id: str | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchange],
    ) -> PositionOpenedEvent:
        """Create a position opened event with proper initialization.

        Args:
            position: The opened position data
            opening_trade_id: ID of trade that opened position
            **kwargs: Additional metadata fields
            
        Returns:
            PositionOpenedEvent: The created position opened event.
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(exchange_id=position.exchange_id, symbol=position.symbol)

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

        # Set standard tags
        metadata.tags["position_id"] = position.position_id
        metadata.tags["side"] = position.side

        if opening_trade_id:
            metadata.tags["opening_trade_id"] = opening_trade_id

        return cls(event_type=EventType.POSITION_OPENED, data=position, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize position data.
        
        Returns:
            dict[str, Any]: Serialized position data with all fields as strings.
        """
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

    @classmethod
    def create(
        cls,
        position: PositionData,
        update_reason: str,
        previous_size: Decimal | None = None,
        size_change: Decimal | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchange],
    ) -> PositionUpdatedEvent:
        """Create a position updated event with proper initialization.

        Args:
            position: The updated position data
            update_reason: Reason for update (e.g., "trade", "price_change")
            previous_size: Previous position size
            size_change: Change in position size
            **kwargs: Additional metadata fields
            
        Returns:
            PositionUpdatedEvent: The created position updated event.
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(exchange_id=position.exchange_id, symbol=position.symbol)

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

        # Set standard tags
        metadata.tags["position_id"] = position.position_id
        metadata.tags["side"] = position.side
        metadata.tags["update_reason"] = update_reason

        if previous_size is not None:
            metadata.tags["previous_size"] = str(previous_size)
        if size_change is not None:
            metadata.tags["size_change"] = str(size_change)

        return cls(event_type=EventType.POSITION_UPDATED, data=position, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize position data.
        
        Returns:
            dict[str, Any]: Serialized position data with all fields as strings.
        """
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

    @classmethod
    def create(
        cls,
        position: PositionData,
        closing_trade_id: str | None = None,
        close_reason: str | None = None,
        final_pnl: Decimal | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchange],
    ) -> PositionClosedEvent:
        """Create a position closed event with proper initialization.

        Args:
            position: The closed position data
            closing_trade_id: ID of trade that closed position
            close_reason: Reason for closure (e.g., "manual", "stop_loss", "liquidation")
            final_pnl: Final realized P&L
            **kwargs: Additional metadata fields
            
        Returns:
            PositionClosedEvent: The created position closed event.
        """
        # Build metadata with explicit fields first
        metadata = EventMetadata(exchange_id=position.exchange_id, symbol=position.symbol)

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

        # Set standard tags
        metadata.tags["position_id"] = position.position_id
        metadata.tags["side"] = position.side

        if closing_trade_id:
            metadata.tags["closing_trade_id"] = closing_trade_id
        if close_reason:
            metadata.tags["close_reason"] = close_reason
        if final_pnl is not None:
            metadata.tags["final_pnl"] = str(final_pnl)

        return cls(event_type=EventType.POSITION_CLOSED, data=position, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize position data.
        
        Returns:
            dict[str, Any]: Serialized position data with all fields as strings.
        """
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


class PositionErrorData(BaseModel):
    """Data for position-related errors."""

    exchange_id: str = Field(description="Exchange where error occurred")
    symbol: str = Field(description="Trading symbol")
    position_id: str | None = Field(default=None, description="Position ID if applicable")
    error_type: str = Field(description="Type of error")
    error_message: str = Field(description="Error message")
    error_data: dict[str, Any] = Field(default_factory=dict, description="Additional error data")


@dataclass
class PositionErrorEvent(BasePortfolioEvent[PositionErrorData]):
    """Event fired when a position operation fails."""

    @classmethod
    def create(
        cls,
        exchange_id: str,
        symbol: str,
        position_id: str | None,
        error_type: str,
        error_message: str,
        error_data: dict[str, Any] | None = None,
        **kwargs: Unpack[EventMetadataKwargsWithoutExchangeSymbol],
    ) -> PositionErrorEvent:
        """Create a position error event with proper initialization.

        Args:
            exchange_id: Exchange where error occurred
            symbol: Symbol involved
            position_id: Position ID if applicable
            error_type: Type of error
            error_message: Error message
            error_data: Additional error data
            **kwargs: Additional metadata fields
            
        Returns:
            PositionErrorEvent: The created position error event.
        """
        # Create PositionErrorData
        data = PositionErrorData(
            exchange_id=exchange_id,
            symbol=symbol,
            position_id=position_id,
            error_type=error_type,
            error_message=error_message,
            error_data=error_data or {},
        )

        # Build metadata with explicit fields first
        metadata = EventMetadata(exchange_id=exchange_id, symbol=symbol)

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
        metadata.tags["error_type"] = error_type

        if position_id:
            metadata.tags["position_id"] = position_id

        return cls(event_type=EventType.POSITION_ERROR, data=data, metadata=metadata)

    def _serialize_data(self) -> dict[str, Any]:
        """Serialize error data.
        
        Returns:
            dict[str, Any]: Serialized error data including all error details.
        """
        return {
            "exchange_id": self.data.exchange_id,
            "symbol": self.data.symbol,
            "position_id": self.data.position_id,
            "error_type": self.data.error_type,
            "error_message": self.data.error_message,
            "error_data": self.data.error_data,
        }
