"""Full-featured Backpack WebSocket Router using new architecture.

This module implements the complete Backpack WebSocket router that replaces
the existing bp_ws_message_router.py with the new validated, type-safe architecture.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_validators import BackpackValidators

# Type safety imports for future enhancement
from cyberdelta.apis.backpack.models import (
    BackpackRawOrderUpdate,
    BackpackRawPositionUpdate,
    BackpackRawPublicTradeEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
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
    BackpackRawWsSignatureComponents,
    BackpackRawWsSubscriptionRequest,
)
from cyberdelta.apis.backpack.transformers.bp_depth_state_transformer import (
    BackpackDepthStateTransformer,
)
from cyberdelta.apis.common.types import MessageHandler
from cyberdelta.apis.websocket.enums import WebSocketErrorCode
from cyberdelta.apis.websocket.exceptions import WebSocketStreamError, WebSocketSubscriptionError
from cyberdelta.apis.websocket.ws_processor import (
    PydanticWebSocketProcessor,
)
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_router import BaseWebSocketRouter
from cyberdelta.apis.websocket.ws_stream_context import StreamErrorContext
from cyberdelta.apis.websocket.ws_transformer import (
    ControlMessageTransformer,
    MapperTransformer,
)
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor
from cyberdelta.enums import ExchangeName
from cyberdelta.exceptions.service_validation import EmptyStringParameterError
from cyberdelta.models import DerivativePosition, Fill, Order, Ticker


if TYPE_CHECKING:
    from cyberdelta.apis.backpack.protocols.mapper_protocols import (
        BalanceMapperProtocol,
        FillMapperProtocol,
        OrderBookMapperProtocol,
        OrderMapperProtocol,
        PositionMapperProtocol,
        TickerMapperProtocol,
        TransactionMapperProtocol,
    )
    # BaseErrorHandler import removed - deprecated and not used
from cyberdelta.apis.websocket.error_handling.stream_error_handler import (
    WebSocketStreamErrorHandler,
)


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
    BaseWebSocketRouter[BackpackRawWebSocketEnvelope | BackpackSubscriptionResponse],
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
        stream_error_handler: WebSocketStreamErrorHandler,
        typed_processor: TypeSafeWebSocketProcessor,
        order_book_mapper: OrderBookMapperProtocol,
        ticker_mapper: TickerMapperProtocol,
        trade_mapper: FillMapperProtocol,
        balance_mapper: BalanceMapperProtocol,
        position_mapper: PositionMapperProtocol,
        order_mapper: OrderMapperProtocol,
        transaction_mapper: TransactionMapperProtocol,
    ) -> None:
        """Initialize the Backpack WebSocket router.

        Args:
            stream_error_handler: Stream error handler for new architecture.
            typed_processor: Required typed processor (use WebSocketRegistryFactory to create).
            order_book_mapper: Mapper for order book transformations.
            ticker_mapper: Mapper for ticker transformations.
            trade_mapper: Mapper for fill transformations.
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
            exchange_name=ExchangeName.BACKPACK,
            typed_processor=typed_processor,
            stream_error_handler=stream_error_handler,
            envelope_validator=validate_backpack_envelope,
        )

    def _setup_processors(self) -> None:
        """Setup Backpack-specific message processors for all message types."""
        # Market data processors
        # Use stateful transformer for depth updates to handle incremental updates
        self.processors["depth"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawDepthUpdateEvent,
            transformer=BackpackDepthStateTransformer(self.order_book_mapper),
            stream_error_handler=self.stream_error_handler,
            processor_name="backpack_depth",
        )

        self.processors["ticker"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawTickerEvent,
            transformer=MapperTransformer[BackpackRawTickerEvent, Ticker](
                mapper_method=self.ticker_mapper.transform_ws_ticker_event_to_internal,
            ),
            stream_error_handler=self.stream_error_handler,
            processor_name="backpack_ticker",
        )

        # Public trade processor - NOTE: Backpack uses "trade" (singular) not "trades"
        # Register under both "trade" and "trades" for compatibility
        trade_processor = PydanticWebSocketProcessor(
            raw_model=BackpackRawPublicTradeEvent,
            transformer=MapperTransformer[BackpackRawPublicTradeEvent, Fill](
                mapper_method=self.trade_mapper.transform_ws_fill_event_to_internal_fill,
            ),
            stream_error_handler=self.stream_error_handler,
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
            stream_error_handler=self.stream_error_handler,
            processor_name="backpack_orders",
        )

        self.processors["positionUpdate"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawPositionUpdate,
            transformer=MapperTransformer[BackpackRawPositionUpdate, DerivativePosition](
                mapper_method=self.position_mapper.transform_ws_position_update_to_internal_position,
            ),
            stream_error_handler=self.stream_error_handler,
            processor_name="backpack_positions",
        )

        # Account fills processor (different transformer than public trades)
        self.processors["fills"] = PydanticWebSocketProcessor(
            raw_model=BackpackRawFillResponse,
            transformer=MapperTransformer[BackpackRawFillResponse, Fill](
                mapper_method=self.transaction_mapper.transform_ws_fill_event_to_internal_fill,
            ),
            stream_error_handler=self.stream_error_handler,
            processor_name="backpack_fills",
        )

        # Subscription response processor - control message with no domain model
        # Applications can register handlers for "subscriptionResponse" to track subscription state
        # The validated BackpackSubscriptionResponse will be available in context.raw_model
        self.processors["subscriptionResponse"] = PydanticWebSocketProcessor(
            raw_model=BackpackSubscriptionResponse,
            transformer=ControlMessageTransformer[BackpackSubscriptionResponse](),
            stream_error_handler=self.stream_error_handler,
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
        self,
        envelope: BackpackRawWebSocketEnvelope | BackpackSubscriptionResponse,
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
            BackpackValidators.validate_backpack_topic(stream_or_topic)
        except ValueError:
            self.logger.warning(
                "invalid_backpack_stream_format",
                stream=stream_or_topic,
                exchange=self.exchange_name.value,
            )
            return None
        else:
            # For Backpack, the routing key should be the full stream (e.g., "ticker.SOL_USDC")
            # not just the type, as handlers are registered with full topic names
            return stream_or_topic

    def _extract_payload_from_envelope(
        self,
        envelope: BackpackRawWebSocketEnvelope | BackpackSubscriptionResponse,
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
        signature_components: BackpackRawWsSignatureComponents | None = None,
    ) -> BackpackRawWsSubscriptionRequest:
        """Construct the subscription payload for a given topic for Backpack.

        This method creates the subscription request structure and includes signature
        components for private streams when provided.

        Backpack uses the format:
        {"method": "SUBSCRIBE", "params": ["topic"]} for public streams
        {"method": "SUBSCRIBE", "params": ["topic"], "signature": [...]} for private streams

        Args:
            topic: The WebSocket topic to subscribe to
            signature_components: Optional BackpackRawWsSignatureComponents model
                                for private stream authentication

        Returns:
            BackpackRawWsSubscriptionRequest model

        Raises:
            EmptyStringParameterError: If topic is empty
            WebSocketStreamError: If topic format is not supported by Backpack
            WebSocketSubscriptionError: If topic validation fails

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
            context = StreamErrorContext(
                connection_id=str(uuid.uuid4()),
                exchange=ExchangeName.BACKPACK,
                topic=topic,
                extra_context={
                    "supported_formats": ["type.symbol", "fills", "orders", "liquidation"],
                    "validation_type": "topic_format",
                },
            )
            raise WebSocketStreamError(
                message=(
                    f"Unsupported topic format '{topic}'. "
                    "Expected formats: type.symbol, fills, orders, liquidation"
                ),
                code=WebSocketErrorCode.INVALID_TOPIC,
                context=context,
            )

        # Additional validation for topics with symbols
        if "." in topic:
            try:
                # Validate the topic format using the exchange-specific validator
                BackpackValidators.validate_backpack_topic(topic)
            except ValueError as e:
                # Create a proper error context for the subscription error
                error_context = StreamErrorContext(
                    connection_id=self._connection_id,
                    exchange=self.exchange_name,
                    channel=topic,
                )
                raise WebSocketSubscriptionError(
                    message=f"Invalid topic format: {e!s}",
                    context=error_context,
                    channel=topic,
                    code=WebSocketErrorCode.SUBSCRIPTION_FAILED,
                    cause=e,
                ) from e

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
        signature_components: BackpackRawWsSignatureComponents | None = None,
    ) -> BackpackRawWsSubscriptionRequest:
        """Construct the unsubscription payload for a given topic for Backpack.

        Args:
            topic: The WebSocket topic to unsubscribe from
            signature_components: Optional BackpackRawWsSignatureComponents model
                                for private stream authentication

        Returns:
            BackpackRawWsSubscriptionRequest model

        Raises:
            EmptyStringParameterError: If topic is empty

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
        context: WebSocketContextProtocol,
        routing_key: str,
    ) -> WebSocketContextProtocol:
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

    def get_symbol_from_context(self, context: WebSocketContextProtocol) -> str | None:
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

        Raises:
            ValueError: If envelope validator is not configured.

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
