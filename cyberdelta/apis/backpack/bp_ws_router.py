"""Full-featured Backpack WebSocket Router using new architecture.

This module implements the complete Backpack WebSocket router that replaces
the existing bp_ws_message_router.py with the new validated, type-safe architecture.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

# Type safety imports for future enhancement
from cyberdelta.apis.backpack.models import (
    BackpackRawOrderUpdate,
    BackpackRawPositionUpdate,
    BackpackRawPublicTradeEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFill
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.backpack.models.bp_ws_envelope import (
    BackpackRawWebSocketEnvelope,
    BackpackSubscriptionResponse,
    validate_backpack_envelope,
)
from cyberdelta.apis.backpack.models.bp_ws_payloads import (
    BackpackRawWsSubscriptionRequest,
    BackpackWsSignatureComponents,
)
from cyberdelta.apis.base.ws_context import (
    ExchangeType,
    WebSocketContextUnion,
)
from cyberdelta.apis.base.ws_processor import (
    PydanticWebSocketProcessor,
)
from cyberdelta.apis.base.ws_router import BaseWebSocketRouter
from cyberdelta.apis.base.ws_transformer import (
    ControlMessageTransformer,
    MapperTransformer,
    extract_symbol_from_context,
)
from cyberdelta.apis.base.ws_validators import ExchangeSpecificValidators
from cyberdelta.apis.common.types import MessageHandler
from cyberdelta.apis.exceptions import (
    UnsupportedWebSocketTopicError,
    WebSocketSubscriptionError,
)
from cyberdelta.core.models import DerivativePosition, Order, OrderBook, Ticker, Trade
from cyberdelta.exceptions.service_validation import EmptyStringParameterError


if TYPE_CHECKING:
    from cyberdelta.apis.backpack.protocols.mapper_protocols import (
        BalanceMapperProtocol,
        OrderBookMapperProtocol,
        OrderMapperProtocol,
        PositionMapperProtocol,
        TickerMapperProtocol,
        TradeMapperProtocol,
        TransactionMapperProtocol,
    )
    from cyberdelta.apis.base.ws_error_handler import BaseErrorHandler


class TransformationError(ValueError):
    """Raised when a transformation fails."""

    def __init__(self, event_type: str, data: object) -> None:
        """Initialize transformation error.

        Args:
            event_type: Type of event that failed to transform
            data: The data that failed to transform
        """
        super().__init__(f"Failed to transform {event_type} event: {data}")


class BackpackWebSocketRouter(
    BaseWebSocketRouter[BackpackRawWebSocketEnvelope | BackpackSubscriptionResponse]
):
    """Full-featured Backpack WebSocket router using new architecture.

    This router provides complete functionality for Backpack WebSocket communication:
    - Message routing and processing with type safety
    - Subscription payload construction
    - Multi-mapper transformation support
    - Enhanced error handling and validation
    """

    def __init__(
        self,
        error_handler: BaseErrorHandler,
        order_book_mapper: OrderBookMapperProtocol,
        ticker_mapper: TickerMapperProtocol,
        trade_mapper: TradeMapperProtocol,
        balance_mapper: BalanceMapperProtocol,
        position_mapper: PositionMapperProtocol,
        order_mapper: OrderMapperProtocol,
        transaction_mapper: TransactionMapperProtocol,
    ) -> None:
        """Initialize the Backpack WebSocket router.

        Args:
            error_handler: Error handler for centralized error management.
            order_book_mapper: Mapper for order book transformations.
            ticker_mapper: Mapper for ticker transformations.
            trade_mapper: Mapper for trade transformations.
            balance_mapper: Mapper for balance transformations.
            position_mapper: Mapper for position transformations.
            order_mapper: Mapper for order transformations.
            transaction_mapper: Mapper for transaction transformations.

        """
        self.order_book_mapper = order_book_mapper
        self.ticker_mapper = ticker_mapper
        self.trade_mapper = trade_mapper
        self.balance_mapper = balance_mapper
        self.position_mapper = position_mapper
        self.order_mapper = order_mapper
        self.transaction_mapper = transaction_mapper

        super().__init__(
            exchange_name="backpack",
            exchange_type=ExchangeType.BACKPACK,
            error_handler=error_handler,
            envelope_validator=validate_backpack_envelope,
        )

    def _setup_processors(self) -> None:
        """Setup Backpack-specific message processors for all message types."""
        # Market data processors
        self.processors["depth"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawDepthUpdateEvent,
            transformer=MapperTransformer[BackpackRawDepthUpdateEvent, OrderBook](
                mapper_method=self.order_book_mapper.transform_ws_depth_event_to_internal,
                context_extractor=extract_symbol_from_context,
            ),
            error_handler=self.error_handler,
            processor_name="backpack_depth",
        )

        self.processors["ticker"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawTickerEvent,
            transformer=MapperTransformer[BackpackRawTickerEvent, Ticker](
                mapper_method=self.ticker_mapper.transform_ws_ticker_event_to_internal,
            ),
            error_handler=self.error_handler,
            processor_name="backpack_ticker",
        )

        # Public trade processor - NOTE: Backpack uses "trade" (singular) not "trades"
        # Register under both "trade" and "trades" for compatibility
        trade_processor = PydanticWebSocketProcessor(
            raw_model=BackpackRawPublicTradeEvent,
            transformer=MapperTransformer[BackpackRawPublicTradeEvent, Trade](
                mapper_method=self.trade_mapper.transform_ws_trade_event_to_internal,
            ),
            error_handler=self.error_handler,
            processor_name="backpack_trades",
        )
        self.processors["trade"] = trade_processor
        self.processors["trades"] = trade_processor  # Compatibility alias

        # Account data processors
        self.processors["orders"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawOrderUpdate,
            transformer=MapperTransformer[BackpackRawOrderUpdate, Order](
                mapper_method=self.order_mapper.transform_ws_order_update_to_internal_order,
            ),
            error_handler=self.error_handler,
            processor_name="backpack_orders",
        )

        self.processors["positionUpdate"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawPositionUpdate,
            transformer=MapperTransformer[BackpackRawPositionUpdate, DerivativePosition](
                mapper_method=self.position_mapper.transform_ws_position_update_to_internal_position,
            ),
            error_handler=self.error_handler,
            processor_name="backpack_positions",
        )

        # Account fills processor (different transformer than public trades)
        self.processors["fills"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawFill,
            transformer=MapperTransformer[BackpackRawFill, Trade](
                mapper_method=self.transaction_mapper.transform_ws_fill_event_to_internal_trade,
            ),
            error_handler=self.error_handler,
            processor_name="backpack_fills",
        )

        # Subscription response processor - control message with no domain model
        # Applications can register handlers for "subscriptionResponse" to track subscription state
        # The validated BackpackSubscriptionResponse will be available in context.raw_model
        self.processors["subscriptionResponse"] = PydanticWebSocketProcessor(
            raw_model=BackpackSubscriptionResponse,
            transformer=ControlMessageTransformer[BackpackSubscriptionResponse](),
            error_handler=self.error_handler,
            processor_name="backpack_subscription_response",
        )

    def _extract_routing_key(self, message: dict[str, Any]) -> str | None:
        """Extract routing key from raw message - required by base class.

        This is only used if route_message is not overridden.
        Since we override route_message, this is not used.

        Args:
            message: Raw message dictionary.

        Returns:
            None - we use _extract_routing_key_from_envelope instead.

        """
        return None

    def _extract_routing_key_from_envelope(
        self, envelope: BackpackRawWebSocketEnvelope | BackpackSubscriptionResponse
    ) -> str | None:
        """Extract routing key from validated Backpack WebSocket message envelope.

        According to Backpack API specification, messages follow these formats:
        1. Current: {"stream": "depth.SOL_USDC", "data": {...}}
        2. Legacy topic: {"topic": "depth.BTC_USDC", "data": {...}}
        3. Legacy flat: {"type": "fills", ...}
        4. Subscription response: {"result": true, "id": <number>}

        Args:
            envelope: Validated WebSocket message envelope.

        Returns:
            Routing key for processor lookup or None if not found.

        """
        # Handle subscription confirmation responses
        if isinstance(envelope, BackpackSubscriptionResponse):
            self.logger.info(
                "backpack_subscription_confirmed",
                result=envelope.result,
                id=envelope.id,
                message="Subscription confirmation received",
            )
            # Return a routing key to allow handlers to process subscription responses
            return "subscriptionResponse"

        # Extract stream from envelope
        stream_or_topic = envelope.stream

        # Handle simple stream names (fills, orders) that don't have symbols
        if stream_or_topic in {"fills", "orders", "liquidation"}:
            return stream_or_topic

        # Parse stream/topic format with symbols
        try:
            # Validate the topic format
            ExchangeSpecificValidators.validate_backpack_topic(stream_or_topic)
        except ValueError:
            self.logger.warning(
                "invalid_backpack_stream_format",
                stream=stream_or_topic,
                exchange=self.exchange_name,
            )
            return None
        else:
            # For Backpack, the routing key should be the full stream (e.g., "ticker.SOL_USDC")
            # not just the type, as handlers are registered with full topic names
            return stream_or_topic

    def _extract_payload_from_envelope(
        self, envelope: BackpackRawWebSocketEnvelope | BackpackSubscriptionResponse
    ) -> dict[str, Any]:
        """Extract payload from validated Backpack WebSocket message envelope.

        This method now works with properly validated envelope models, eliminating
        the need for type: ignore statements.

        Args:
            envelope: Validated WebSocket message envelope.

        Returns:
            The payload data for processing.

        """
        # Subscription responses don't have payloads to extract
        if isinstance(envelope, BackpackSubscriptionResponse):
            return {}

        # Extract data from the envelope
        data = envelope.data
        if isinstance(data, dict):
            return data
        # Handle list payloads by wrapping in dict
        return {"items": data}

    def construct_subscription_payload(
        self,
        topic: str,
        signature_components: BackpackWsSignatureComponents | None = None,
    ) -> BackpackRawWsSubscriptionRequest:
        """Construct the subscription payload for a given topic for Backpack.

        This method creates the subscription request structure and includes signature
        components for private streams when provided.

        Backpack uses the format:
        {"method": "SUBSCRIBE", "params": ["topic"]} for public streams
        {"method": "SUBSCRIBE", "params": ["topic"], "signature": [...]} for private streams

        Args:
            topic: The WebSocket topic to subscribe to
            signature_components: Optional BackpackWsSignatureComponents model
                                for private stream authentication

        Returns:
            BackpackRawWsSubscriptionRequest model

        Raises:
            EmptyStringParameterError: If topic is empty
            ValueError: If topic format is invalid

        """
        # Basic topic validation
        if not topic or not topic.strip():
            raise EmptyStringParameterError(
                parameter_name="topic",
                method_name="construct_subscription_payload",
            )

        # Format validation for Backpack topics
        # Most Backpack topics require the format "type.symbol" (except fills, orders, etc.)
        # Simple private streams like "fills", "orders" are allowed without dots
        if topic not in {"fills", "orders", "liquidation"} and "." not in topic:
            raise UnsupportedWebSocketTopicError(
                topic, supported_formats=["type.symbol", "fills", "orders", "liquidation"]
            )

        # Additional validation for topics with symbols
        if "." in topic:
            try:
                # Validate the topic format using the exchange-specific validator
                ExchangeSpecificValidators.validate_backpack_topic(topic)
            except ValueError as e:
                raise WebSocketSubscriptionError(topic, str(e)) from e

        # Build the subscription request
        signature_tuple = None
        if signature_components:
            signature_tuple = (
                signature_components.api_key,
                signature_components.signature,
                signature_components.timestamp,
                signature_components.window,
            )

        return BackpackRawWsSubscriptionRequest(
            method="SUBSCRIBE",
            params=[topic],
            signature=signature_tuple,
        )

    def construct_unsubscription_payload(
        self,
        topic: str,
        signature_components: BackpackWsSignatureComponents | None = None,
    ) -> BackpackRawWsSubscriptionRequest:
        """Construct the unsubscription payload for a given topic for Backpack.

        Args:
            topic: The WebSocket topic to unsubscribe from
            signature_components: Optional BackpackWsSignatureComponents model
                                for private stream authentication

        Returns:
            BackpackRawWsSubscriptionRequest model

        Raises:
            EmptyStringParameterError: If topic is empty
            ValueError: If topic format is invalid

        """
        # Basic topic validation
        if not topic or not topic.strip():
            raise EmptyStringParameterError(
                parameter_name="topic",
                method_name="construct_subscription_payload",
            )

        # Build the unsubscription request
        signature_tuple = None
        if signature_components:
            signature_tuple = (
                signature_components.api_key,
                signature_components.signature,
                signature_components.timestamp,
                signature_components.window,
            )

        return BackpackRawWsSubscriptionRequest(
            method="UNSUBSCRIBE",
            params=[topic],
            signature=signature_tuple,
        )

    async def _enhance_typed_context(
        self,
        context: WebSocketContextUnion,
        routing_key: str,
    ) -> WebSocketContextUnion:
        """Enhance typed context with Backpack-specific data.

        The typed context already includes computed fields for symbol extraction,
        so minimal enhancement is needed.

        Args:
            context: Typed context
            routing_key: Message routing key

        Returns:
            Enhanced typed context
        """
        # Typed context already has all computed fields
        return context

    def get_symbol_from_context(self, context: WebSocketContextUnion) -> str | None:
        """Extract symbol from typed WebSocket context.

        Args:
            context: Typed WebSocket context

        Returns:
            Symbol extracted from context, or None if not found
        """
        # All WebSocket contexts have a symbol field (may be None)
        return context.symbol

    async def route_message(
        self,
        message: dict[str, Any],
        handlers: dict[str, MessageHandler],
    ) -> None:
        """Route WebSocket message to appropriate processor and handler.

        Override to handle Backpack's topic.symbol handler registration pattern.

        Args:
            message: The WebSocket message to route.
            handlers: Dictionary mapping full topics (e.g., "ticker.SOL_USDC") to handlers.
        """
        if self.envelope_validator is None:
            msg = "Envelope validator is required"
            raise ValueError(msg)

        try:
            # Step 1: Validate envelope
            validated_envelope = self.envelope_validator(message)
        except (ValidationError, ValueError) as e:
            await self._handle_envelope_validation_error(e, message)
            return

        # Step 2: Extract routing key and symbol
        routing_key = self._extract_routing_key_from_envelope(validated_envelope)
        if not routing_key:
            await self._handle_missing_routing_key(message, validated_envelope)
            return

        # Step 3: Create typed context and enhance it
        message_id = str(uuid.uuid4())
        typed_context = self._create_typed_context(validated_envelope, routing_key, message_id)
        typed_context = await self._enhance_typed_context(typed_context, routing_key)

        # Step 4: Find appropriate handler
        # For Backpack, handlers are registered as "type.symbol" (e.g., "ticker.SOL_USDC")
        # The routing key is now the full topic, so we can use it directly
        handler = handlers.get(routing_key)

        if not handler:
            await self._handle_missing_handler(message, routing_key, handlers)
            return

        # Step 5: Extract payload and process
        payload = self._extract_payload_from_envelope(validated_envelope)

        # For processor lookup, we need just the type part (e.g., "ticker" from "ticker.SOL_USDC")
        processor_key = routing_key.split(".")[0] if "." in routing_key else routing_key
        processor = self.processors.get(processor_key)

        if processor:
            await processor.process(payload, handler, typed_context)
        else:
            await self._handle_missing_processor(routing_key, payload, typed_context)
