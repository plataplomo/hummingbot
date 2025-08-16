"""Enhanced Backpack WebSocket Router - Proof of Concept.

This module demonstrates the new WebSocket architecture with the Backpack exchange,
using the base abstractions for type-safe, validated message processing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from cyberdelta.apis.backpack.bp_registry_builder import BackpackRegistryBuilder
from cyberdelta.apis.backpack.bp_validators import BackpackValidators
from cyberdelta.apis.backpack.models import (
    BackpackRawOrderUpdate,
    BackpackRawPositionUpdate,
    BackpackRawPublicTradeEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.backpack.transformers.bp_depth_state_transformer import (
    BackpackDepthStateTransformer,
)
from cyberdelta.apis.websocket.ws_processor import (
    ProcessorFactory,
    PydanticWebSocketProcessor,
)
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_router import BaseWebSocketRouter
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor
from cyberdelta.enums import ExchangeName
from cyberdelta.symbols import exchanges


# Type alias for envelope that can be either validated model or dict
class EnvelopeProtocol(Protocol):
    """Protocol for envelope objects that may have stream/data attributes."""

    def __getattr__(self, name: str) -> object:
        """Allow dynamic attribute access for envelope objects."""
        ...


EnvelopeType = dict[str, Any] | EnvelopeProtocol


if TYPE_CHECKING:
    from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
        BackpackOrderBookMapper,
    )
    from cyberdelta.apis.backpack.mappers.market_data.bp_ticker_mapper import BackpackTickerMapper
    from cyberdelta.apis.backpack.mappers.market_data.bp_trade_mapper import BackpackFillMapper


class SymbolExtractionError(ValueError):
    """Raised when symbol cannot be extracted from context or validated data."""

    def __init__(self) -> None:
        """Initialize with descriptive message."""
        super().__init__("Unable to extract symbol from context or validated data")


if TYPE_CHECKING:
    from cyberdelta.apis.websocket.error_handling.websocket_error_handler import (
        WebSocketErrorHandler,
    )
    from cyberdelta.models.market import (
        Fill,
        OrderBook,
        Ticker,
    )


class BackpackDepthTransformer:
    """Transforms Backpack raw depth updates to internal OrderBook models."""

    def __init__(self, order_book_mapper: BackpackOrderBookMapper) -> None:
        """Initialize with order book mapper.

        Args:
            order_book_mapper: Mapper for transforming order book data.

        """
        self.order_book_mapper = order_book_mapper

    def transform(
        self,
        validated: BackpackRawDepthUpdateEvent,
        context: WebSocketContextProtocol | None = None,
    ) -> OrderBook:
        """Transform validated depth update to OrderBook.

        Args:
            validated: Validated Backpack depth update.
            context: Processing context containing symbol information.

        Returns:
            Internal OrderBook model.

        """
        # Extract symbol from context or validated envelope
        symbol_str = self._extract_symbol_from_context(validated, context)

        # Convert string symbol to Symbol object for the mapper
        symbol = exchanges.backpack(symbol_str)

        # Use existing mapper to transform to internal model
        return self.order_book_mapper.transform_ws_depth_event_to_internal(
            symbol,
            validated,
        )

    def _extract_symbol_from_context(
        self,
        validated: BackpackRawDepthUpdateEvent,
        context: WebSocketContextProtocol | None,
    ) -> str:
        """Extract symbol from context or validated envelope.

        Args:
            validated: Validated Backpack depth update.
            context: Processing context.

        Returns:
            Symbol string.

        Raises:
            SymbolExtractionError: If symbol cannot be extracted from context or validated data.
        """
        # Try to get symbol from context first
        if context:
            envelope = context.validated_envelope
            # Type-narrow to BackpackRawWebSocketEnvelope
            if isinstance(envelope, BackpackRawWebSocketEnvelope):
                try:
                    _, symbol = BackpackValidators.validate_backpack_topic(envelope.stream)
                except ValueError:
                    pass
                else:
                    return symbol  # symbol is str from tuple[str, str]

        # BackpackRawDepthUpdateEvent doesn't have a symbol field directly
        # Symbol must be extracted from the envelope context
        # Last resort: raise error for production safety
        raise SymbolExtractionError


class BackpackTickerTransformer:
    """Transforms Backpack raw ticker events to internal Ticker models."""

    def __init__(self, ticker_mapper: BackpackTickerMapper) -> None:
        """Initialize with ticker mapper.

        Args:
            ticker_mapper: Mapper for transforming ticker data.

        """
        self.ticker_mapper = ticker_mapper

    def transform(
        self,
        validated: BackpackRawTickerEvent,
        context: WebSocketContextProtocol | None = None,
    ) -> Ticker:
        """Transform validated ticker event to Ticker.

        Args:
            validated: Validated Backpack ticker event.
            context: Optional context (ignored).

        Returns:
            Internal Ticker model.

        """
        # Use existing mapper to transform to internal model
        return self.ticker_mapper.transform_ws_ticker_event_to_internal(validated)


class BackpackFillTransformer:
    """Transforms Backpack raw trade events to internal Fill models."""

    def __init__(self, trade_mapper: BackpackFillMapper) -> None:
        """Initialize with trade mapper.

        Args:
            trade_mapper: Mapper for transforming trade data.

        """
        self.trade_mapper = trade_mapper

    def transform(
        self,
        validated: BackpackRawPublicTradeEvent,
        context: WebSocketContextProtocol | None = None,
    ) -> Fill:
        """Transform validated trade event to Fill.

        Args:
            validated: Validated Backpack trade event.
            context: Optional context (ignored).

        Returns:
            Internal Fill model.

        """
        # Use existing mapper to transform to internal model
        return self.trade_mapper.transform_ws_fill_event_to_internal_fill(validated)


class BackpackWebSocketRouterV2(BaseWebSocketRouter[BackpackRawWebSocketEnvelope]):
    """Enhanced Backpack WebSocket router using base abstractions.

    This is a proof of concept demonstrating how the new architecture
    reduces code duplication and improves type safety.
    """

    def __init__(
        self,
        stream_error_handler: WebSocketErrorHandler,
        order_book_mapper: BackpackOrderBookMapper,
        ticker_mapper: BackpackTickerMapper,
        trade_mapper: BackpackFillMapper,
    ) -> None:
        """Initialize the Backpack WebSocket router.

        Args:
            stream_error_handler: Stream error handler for new architecture.
            order_book_mapper: Mapper for order book transformations.
            ticker_mapper: Mapper for ticker transformations.
            trade_mapper: Mapper for trade transformations.

        """
        self.order_book_mapper = order_book_mapper
        self.ticker_mapper = ticker_mapper
        self.trade_mapper = trade_mapper

        # Create registry using Backpack-specific builder
        builder = BackpackRegistryBuilder()
        registry = builder.build_registry()
        typed_processor = TypeSafeWebSocketProcessor(registry)

        super().__init__(
            exchange_name=ExchangeName.BACKPACK,
            stream_error_handler=stream_error_handler,
            typed_processor=typed_processor,
        )

    def _setup_processors(self) -> None:
        """Setup Backpack-specific message processors."""
        # Market data processors - use stateful transformer for depth
        self.processors["depth"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawDepthUpdateEvent,
            transformer=BackpackDepthStateTransformer(self.order_book_mapper),
            stream_error_handler=self.stream_error_handler,
            processor_name="backpack_depth",
        )

        self.processors["ticker"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawTickerEvent,
            transformer=BackpackTickerTransformer(self.ticker_mapper),
            stream_error_handler=self.stream_error_handler,
            processor_name="backpack_ticker",
        )

        self.processors["trade"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawPublicTradeEvent,
            transformer=BackpackFillTransformer(self.trade_mapper),
            stream_error_handler=self.stream_error_handler,
            processor_name="backpack_trade",
        )

        # Account data processors (using simple transformers for now)
        self.processors["order"] = ProcessorFactory.create_simple_processor(
            raw_model=BackpackRawOrderUpdate,
            stream_error_handler=self.stream_error_handler,
            processor_name="backpack_order",
        )

        self.processors["position"] = ProcessorFactory.create_simple_processor(
            raw_model=BackpackRawPositionUpdate,
            stream_error_handler=self.stream_error_handler,
            processor_name="backpack_position",
        )

    def _extract_topic_from_object_envelope(
        self,
        envelope: BackpackRawWebSocketEnvelope,
    ) -> str | None:
        """Extract topic from object-style envelope.

        Args:
            envelope: The validated WebSocket envelope to extract topic from.

        Returns:
            Topic string from the envelope's stream attribute.
        """
        return envelope.stream

    def _extract_topic_from_dict_envelope(self, envelope: dict[str, Any]) -> str | None:
        """Extract topic from dict-style envelope.

        Args:
            envelope: Dictionary-style envelope containing stream or topic information.

        Returns:
            Topic string if found in envelope, None otherwise.
        """
        stream_value = envelope.get("stream")
        if isinstance(stream_value, str):
            return stream_value

        topic_value = envelope.get("topic")
        if isinstance(topic_value, str):
            return topic_value

        return None

    def _validate_and_extract_topic_type(self, topic: str) -> str | None:
        """Validate topic and extract topic type.

        Args:
            topic: Topic string to validate and extract type from.

        Returns:
            Topic type string if validation succeeds, None if validation fails.
        """
        try:
            topic_type, _ = BackpackValidators.validate_backpack_topic(topic)
        except ValueError:
            return None
        else:
            return topic_type

    def _extract_routing_key_from_envelope(
        self,
        envelope: BackpackRawWebSocketEnvelope,
    ) -> str | None:
        """Extract routing key from validated envelope for processor lookup.

        Args:
            envelope: The validated WebSocket envelope to extract routing key from.

        Returns:
            Routing key string or None if no valid key found.

        """
        # BackpackRawWebSocketEnvelope is always an object, not a dict
        topic = self._extract_topic_from_object_envelope(envelope)

        if topic is not None:
            return self._validate_and_extract_topic_type(topic)

        return None

    def _extract_payload_from_envelope(
        self,
        envelope: BackpackRawWebSocketEnvelope,
    ) -> dict[str, Any] | list[Any]:
        """Extract the payload data from the validated envelope.

        Args:
            envelope: The validated WebSocket envelope to extract payload from.

        Returns:
            The payload data (dict, list, or other type).

        """
        # BackpackRawWebSocketEnvelope always has data attribute as dict[str, Any] | list[Any]
        data = envelope.data
        # Return data if it's dict, otherwise wrap in dict
        if isinstance(data, dict):
            return data
        return {"data": data}

    def _extract_routing_key(self, message: dict[str, Any]) -> str | None:
        """Extract routing key from Backpack WebSocket message.

        Backpack uses topic-based routing with format: "type.symbol"
        (e.g., "depth.BTC_USDC", "ticker.SOL_USDC")

        Args:
            message: The WebSocket message.

        Returns:
            Routing key (topic type) or None if not found.

        """
        topic = message.get("topic")
        if not topic or not isinstance(topic, str):
            return None

        try:
            # Validate and parse topic using exchange-specific validator
            topic_type, _ = BackpackValidators.validate_backpack_topic(topic)
        except ValueError:
            # Log warning but don't fail - let error handler deal with it
            self.logger.warning(
                "invalid_backpack_topic_format",
                topic=topic,
                exchange=self.exchange_name,
            )
            return None
        else:
            return topic_type

    def _extract_payload(self, message: dict[str, Any]) -> dict[str, Any]:
        """Extract payload from Backpack WebSocket message.

        Args:
            message: The WebSocket message.

        Returns:
            The payload data (usually the entire message for Backpack).

        """
        # For Backpack, the entire message is typically the payload
        # but we need to validate it has expected structure
        try:
            self.payload_validator.validate_required_fields(
                message,
                required_fields=["topic"],  # At minimum need topic
                context="Backpack WebSocket message",
            )
        except ValueError as e:
            self.logger.warning(
                "backpack_message_missing_fields",
                error=str(e),
                message_keys=list(message.keys()),
            )

        return message

    def get_symbol_from_context(self, context: dict[str, Any]) -> str | None:
        """Extract symbol from processing context.

        Args:
            context: Processing context containing validated envelope.

        Returns:
            Extracted symbol or None if not found.

        """
        # Extract from validated envelope in context (new pattern)
        envelope = context.get("validated_envelope")
        if envelope and isinstance(envelope, BackpackRawWebSocketEnvelope):
            try:
                _, symbol = BackpackValidators.validate_backpack_topic(envelope.stream)
            except ValueError:
                pass
            else:
                return symbol

        # No valid envelope found - cannot extract symbol
        return None
