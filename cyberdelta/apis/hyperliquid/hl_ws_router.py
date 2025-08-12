"""Full-featured Hyperliquid WebSocket Router using new architecture.

This module implements the complete Hyperliquid WebSocket router that replaces
the existing hl_ws_message_router.py with the new validated, type-safe architecture.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from cyberdelta.apis.common.types import MessageHandler
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import (
    HyperliquidRawAllMids,
    HyperliquidRawAllMidsWrapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawWsCandle
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawOrder
from cyberdelta.apis.hyperliquid.models.hl_raw_ws_events import (
    HyperliquidRawWsBookUpdate,
    HyperliquidRawWsFillEvent,
    HyperliquidRawWsOrderUpdate,
    HyperliquidRawWsPositionUpdateEvent,
    HyperliquidRawWsTradeEventsList,
)
from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import (
    HyperliquidSubscriptionResponse,
    HyperliquidUserEventEnvelope,
    HyperliquidWebSocketMessage,
    validate_hyperliquid_envelope,
)
from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import (
    HyperliquidRawWsAllMidsSubscriptionPayload,
    HyperliquidRawWsCandleSubscriptionPayload,
    HyperliquidRawWsL2BookSubscriptionPayload,
    HyperliquidRawWsSubscribeRequest,
    HyperliquidRawWsTradesSubscriptionPayload,
    HyperliquidRawWsUserEventsSubscriptionPayload,
)
from cyberdelta.apis.websocket.ws_processor import (
    PydanticWebSocketProcessor,
)
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.apis.websocket.ws_router import BaseWebSocketRouter, EnvelopeValidatorNotSetError
from cyberdelta.apis.websocket.ws_transformer import (
    BatchMapperTransformer,
    ControlMessageTransformer,
    MapperTransformer,
)
from cyberdelta.apis.websocket.ws_typed_processor import TypeSafeWebSocketProcessor

# Type safety imports for future enhancement
from cyberdelta.enums import ExchangeName
from cyberdelta.exceptions.service_validation import EmptyStringParameterError
from cyberdelta.models import DerivativePosition, Fill, Order, OrderBook
from cyberdelta.models.market import Candle
from cyberdelta.models.market.mid_prices import MidPrices


if TYPE_CHECKING:
    from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
        BalanceMapperProtocol,
        HistoricalDataMapperProtocol,
        OrderBookMapperProtocol,
        OrderMapperProtocol,
        PositionMapperProtocol,
        PriceTickerMapperProtocol,
        TransactionMapperProtocol,
    )
    from cyberdelta.apis.websocket.ws_error_handler import BaseErrorHandler
from cyberdelta.apis.websocket.ws_stream_error_handler import WebSocketStreamErrorHandler


class UserAddressRequiredError(ValueError):
    """Raised when user address is required but not provided."""

    def __init__(self) -> None:
        """Initialize with specific message."""
        super().__init__("User address required for userEvents subscription")


class InvalidCandleTopicFormatError(ValueError):
    """Raised when candle topic format is invalid."""

    def __init__(self, topic: str) -> None:
        """Initialize with topic information."""
        super().__init__(f"Invalid candle topic format: {topic}")


class UnsupportedTopicFormatError(ValueError):
    """Raised when topic format is not supported."""

    def __init__(self, topic: str) -> None:
        """Initialize with topic information."""
        super().__init__(f"Unsupported topic format: {topic}")


# Constants
CANDLE_TOPIC_PARTS_COUNT = 3  # Expected parts in candle:coin:interval format


class HyperliquidWebSocketRouter(BaseWebSocketRouter[HyperliquidWebSocketMessage]):
    """Full-featured Hyperliquid WebSocket router using new architecture.

    This router provides complete functionality for Hyperliquid WebSocket communication:
    - Message routing and processing with type safety
    - Subscription payload construction for all Hyperliquid channels
    - Multi-mapper transformation support
    - Enhanced error handling and validation
    """

    def __init__(
        self,
        error_handler: BaseErrorHandler,
        stream_error_handler: WebSocketStreamErrorHandler,
        typed_processor: TypeSafeWebSocketProcessor,
        order_book_mapper: OrderBookMapperProtocol,
        price_ticker_mapper: PriceTickerMapperProtocol,
        balance_mapper: BalanceMapperProtocol,
        position_mapper: PositionMapperProtocol,
        order_mapper: OrderMapperProtocol,
        transaction_mapper: TransactionMapperProtocol,
        historical_data_mapper: HistoricalDataMapperProtocol,
    ) -> None:
        """Initialize the Hyperliquid WebSocket router.

        Args:
            error_handler: Error handler for centralized error management.
            stream_error_handler: Stream error handler for new architecture.
            typed_processor: Required typed processor (use WebSocketRegistryFactory to create).
            order_book_mapper: Mapper for order book and trade transformations.
            price_ticker_mapper: Mapper for price ticker transformations.
            balance_mapper: Mapper for balance transformations.
            position_mapper: Mapper for position transformations.
            order_mapper: Mapper for order transformations.
            transaction_mapper: Mapper for transaction transformations.
            historical_data_mapper: Mapper for historical data transformations.

        """
        self.order_book_mapper = order_book_mapper
        self.price_ticker_mapper = price_ticker_mapper
        self.balance_mapper = balance_mapper
        self.position_mapper = position_mapper
        self.order_mapper = order_mapper
        self.transaction_mapper = transaction_mapper
        self.historical_data_mapper = historical_data_mapper

        # Track active fill subscriptions to handle empty fills lists
        # Maps channel -> set of coins we're subscribed to
        self._fill_subscriptions: dict[str, set[str]] = {"trades": set()}

        # Track active candle subscriptions to handle candle messages (which don't include coin)
        self._candle_subscriptions: dict[str, set[str]] = {"candle": set()}

        super().__init__(
            exchange_name=ExchangeName.HYPERLIQUID,
            error_handler=error_handler,
            typed_processor=typed_processor,
            stream_error_handler=stream_error_handler,
            envelope_validator=validate_hyperliquid_envelope,
        )

    def _transform_order_update(self, validated: HyperliquidRawWsOrderUpdate) -> Order:
        """Transform Hyperliquid order update to internal Order model.

        This method handles the special case where Hyperliquid order updates
        need data extraction before transformation.

        Args:
            validated: Validated Hyperliquid order update.

        Returns:
            Internal Order model.
        """
        # Extract the order data from the WebSocket update event
        order_data = HyperliquidRawOrder.model_validate(validated.data)
        return self.order_mapper.transform_ws_order_update_to_internal_order(order_data)

    def _transform_all_mids_wrapper(self, validated: HyperliquidRawAllMidsWrapper) -> MidPrices:
        """Transform WebSocket allMids wrapper to internal ticker mapping.

        Args:
            validated: Validated allMids wrapper RootModel.

        Returns:
            MidPrices mapping symbols to mid prices.
        """
        # Extract the mids data from the RootModel wrapper
        mids_data = validated.root["mids"]
        # Create HyperliquidRawAllMids from the mids data
        all_mids = HyperliquidRawAllMids(mids_data)
        # Use the existing mapper method
        return self.price_ticker_mapper.transform_raw_all_mids_to_internal(all_mids)

    def _transform_ws_candle(self, validated: HyperliquidRawWsCandle) -> Candle:
        """Transform WebSocket candle to internal Candle model.

        This method delegates to the market data mapper following the
        established pattern of separation of concerns.

        Args:
            validated: Validated WebSocket candle data.

        Returns:
            Internal Candle model.
        """
        # Delegate to historical data mapper for WebSocket candle transformation
        return self.historical_data_mapper.transform_ws_candle_to_internal(validated)

    def _transform_fills_list(self, validated: HyperliquidRawWsTradeEventsList) -> list[Fill]:
        """Transform WebSocket fills list to internal Fill models.

        Args:
            validated: Validated fills list (RootModel).

        Returns:
            List of internal Fill objects.
        """
        fills: list[Fill] = []
        for fill_event in validated.root:  # Access RootModel's root
            # Transform each individual fill using the order book mapper method
            fill: Fill = self.order_book_mapper.transform_ws_trade_event_to_internal(fill_event)
            fills.append(fill)
        return fills

    def _setup_processors(self) -> None:
        """Setup Hyperliquid-specific message processors for all message types."""
        # Market data processors
        self.processors["l2Book"] = PydanticWebSocketProcessor(
            raw_model=HyperliquidRawWsBookUpdate,
            transformer=MapperTransformer[HyperliquidRawWsBookUpdate, OrderBook](
                mapper_method=self.order_book_mapper.transform_ws_book_update_to_internal,
            ),
            stream_error_handler=self.stream_error_handler,
            processor_name="hyperliquid_l2book",
        )

        self.processors["trades"] = PydanticWebSocketProcessor(
            raw_model=HyperliquidRawWsTradeEventsList,
            transformer=BatchMapperTransformer[HyperliquidRawWsTradeEventsList, Fill](
                mapper_method=self._transform_fills_list,
            ),
            stream_error_handler=self.stream_error_handler,
            processor_name="hyperliquid_fills",
        )

        # Account/user data processors (position updates)
        self.processors["userEvents"] = PydanticWebSocketProcessor(
            raw_model=HyperliquidRawWsPositionUpdateEvent,
            transformer=MapperTransformer[HyperliquidRawWsPositionUpdateEvent, DerivativePosition](
                mapper_method=self.position_mapper.transform_ws_position_update_to_internal_position,
            ),
            stream_error_handler=self.stream_error_handler,
            processor_name="hyperliquid_user_events",
        )

        # Order updates (typically part of userEvents but can be separate)
        self.processors["orders"] = PydanticWebSocketProcessor(
            raw_model=HyperliquidRawWsOrderUpdate,
            transformer=MapperTransformer[HyperliquidRawWsOrderUpdate, Order](
                mapper_method=self._transform_order_update,
            ),
            stream_error_handler=self.stream_error_handler,
            processor_name="hyperliquid_orders",
        )

        # Fill events (typically part of userEvents but can be separate)
        self.processors["fills"] = PydanticWebSocketProcessor(
            raw_model=HyperliquidRawWsFillEvent,
            transformer=MapperTransformer[HyperliquidRawWsFillEvent, Fill](
                mapper_method=self.transaction_mapper.transform_ws_fill_event_to_internal,
            ),
            stream_error_handler=self.stream_error_handler,
            processor_name="hyperliquid_fill_events",
        )

        # AllMids channel - provides real-time mid prices for all assets
        # WebSocket sends data wrapped in 'mids' field, so we use the wrapper model
        self.processors["allMids"] = PydanticWebSocketProcessor(
            raw_model=HyperliquidRawAllMidsWrapper,
            transformer=MapperTransformer[HyperliquidRawAllMidsWrapper, MidPrices](
                mapper_method=self._transform_all_mids_wrapper,
            ),
            stream_error_handler=self.stream_error_handler,
            processor_name="hyperliquid_all_mids",
        )

        # Candle channel - provides OHLCV data
        self.processors["candle"] = PydanticWebSocketProcessor(
            raw_model=HyperliquidRawWsCandle,
            transformer=MapperTransformer[HyperliquidRawWsCandle, Candle](
                mapper_method=self._transform_ws_candle,
            ),
            stream_error_handler=self.stream_error_handler,
            processor_name="hyperliquid_candles",
        )

        # Subscription response processor - control message with no domain model
        # Applications can register handlers for "subscriptionResponse" to track subscription state
        # The validated HyperliquidSubscriptionResponse will be available in context.raw_model
        self.processors["subscriptionResponse"] = PydanticWebSocketProcessor(
            raw_model=HyperliquidSubscriptionResponse,
            transformer=ControlMessageTransformer[HyperliquidSubscriptionResponse](),
            stream_error_handler=self.stream_error_handler,
            processor_name="hyperliquid_subscription_response",
        )

    def _extract_routing_key_from_envelope(
        self,
        envelope: HyperliquidWebSocketMessage,
    ) -> str | None:
        """Extract routing key from validated Hyperliquid WebSocket message envelope.

        Hyperliquid uses channel-based routing with coin/symbol specificity.
        For market data channels (l2Book, trades), the routing key includes the coin
        to match how handlers are registered (e.g., "l2Book:SOL").

        Args:
            envelope: Validated WebSocket message envelope.

        Returns:
            Routing key for handler lookup or None if not found.

        """
        channel = envelope.channel

        # For market data channels, include coin in routing key
        if channel in {"l2Book", "trades"}:
            return self._handle_market_data_routing(envelope, channel)

        # Handle candle channel specially - needs full topic with interval
        if channel == "candle":
            return self._handle_candle_routing(envelope)

        # AllMids doesn't need coin specificity
        if channel in {"allMids", "notification", "webData2"}:
            return channel

        # Handle subscription response messages with proper model
        if channel == "subscriptionResponse":
            if isinstance(envelope, HyperliquidSubscriptionResponse):
                self.logger.info(
                    "hyperliquid_subscription_confirmed",
                    exchange=self.exchange_name.value,
                    subscription_type=envelope.subscription_type,
                    subscription_coin=envelope.subscription_coin,
                    is_successful=envelope.is_successful,
                    message="Subscription confirmation received",
                )
                # Track subscription state if needed
                # Note: For now we're not tracking state here because we already
                # track it when constructing the subscription. This could be enhanced
                # to verify successful subscriptions match what we requested.
            else:
                self.logger.warning(
                    "unexpected_subscription_response_type",
                    exchange=self.exchange_name.value,
                    envelope_type=type(envelope).__name__,
                    message="Unexpected envelope type for subscriptionResponse",
                )
            # Return the channel to allow handlers to process subscription responses
            return channel

        # Special handling for userEvents channel
        if isinstance(envelope, HyperliquidUserEventEnvelope) or channel == "userEvents":
            return "userEvents"

        # For channels like "orders" and "fills" that might come from older APIs
        if channel in {"orders", "fills"}:
            return channel

        # Unknown channel - log warning
        self.logger.warning(
            "unknown_hyperliquid_channel",
            channel=channel,
            exchange=self.exchange_name.value,
        )
        return None

    def _handle_market_data_routing(
        self,
        envelope: HyperliquidWebSocketMessage,
        channel: str,
    ) -> str:
        """Handle routing for market data channels (l2Book, trades).

        Returns:
            The routing key combining channel and coin (e.g., 'l2Book:SOL')
        """
        coin = self._extract_coin_from_envelope(envelope)
        if coin:
            # Return full routing key: "l2Book:SOL", "trades:BTC", etc.
            return f"{channel}:{coin}"
        # Fallback to channel-only routing if coin extraction fails
        self.logger.warning(
            "missing_coin_in_market_data",
            channel=channel,
            exchange=self.exchange_name.value,
            message=f"No coin found in {channel} message, using channel-only routing",
        )
        return channel

    def _handle_candle_routing(self, envelope: HyperliquidWebSocketMessage) -> str | None:
        """Handle routing for candle channel messages.

        Returns:
            The candle topic routing key or None if routing fails
        """
        subscribed_topics = self._candle_subscriptions.get("candle", set())

        self.logger.debug(
            "candle_routing_attempt",
            channel="candle",
            subscribed_topics=list(subscribed_topics),
            envelope_data_type=type(envelope.data).__name__,
            has_data_attr=True,  # HyperliquidWebSocketMessage always has data attribute
            message="Attempting to route candle message",
        )

        # Extract interval from candle data
        interval = self._extract_candle_interval(envelope)

        # Try to match with interval if available
        if interval:
            return self._match_candle_topic_by_interval(subscribed_topics, interval)

        # Fallback: if exactly one subscription, use it
        if len(subscribed_topics) == 1:
            topic = next(iter(subscribed_topics))
            self.logger.debug(
                "candle_single_subscription_fallback",
                topic=topic,
                message="Using single subscription for candle routing",
            )
            return topic

        # Multiple or no subscriptions - cannot route
        self.logger.warning(
            "candle_routing_failed",
            channel="candle",
            subscribed_topics=list(subscribed_topics),
            interval=interval,
            message="Cannot determine candle routing",
        )
        return None

    def _extract_candle_interval(self, envelope: HyperliquidWebSocketMessage) -> str | None:
        """Extract interval from candle envelope data.

        Returns:
            The interval string (e.g., '1m', '1h') or None if not found
        """
        if isinstance(envelope.data, dict):
            # Candle data structure: {"t": 123, "T": 456, "s": "BTC", "i": "1m", ...}
            interval = envelope.data.get("i")
            self.logger.debug(
                "candle_interval_found",
                interval=interval,
                data_keys=list(envelope.data.keys()) if envelope.data else [],
                message="Extracted interval from candle data",
            )
            return interval
        return None

    def _match_candle_topic_by_interval(
        self,
        subscribed_topics: set[str],
        interval: str,
    ) -> str | None:
        """Match candle topic by interval from subscribed topics.

        Returns:
            The matching topic or None if no unique match found
        """
        matching_topics = [topic for topic in subscribed_topics if topic.endswith(f":{interval}")]
        if len(matching_topics) == 1:
            return matching_topics[0]
        if len(matching_topics) > 1:
            # Multiple coins for same interval - ambiguous
            self.logger.warning(
                "candle_ambiguous_routing",
                interval=interval,
                matching_topics=matching_topics,
                message="Multiple coins subscribed for same interval",
            )
            return matching_topics[0]  # Use first match
        return None

    def _extract_coin_from_envelope(self, envelope: HyperliquidWebSocketMessage) -> str | None:
        """Extract coin from envelope data for routing key construction.

        Args:
            envelope: Validated WebSocket message envelope.

        Returns:
            Coin string if found, None otherwise.
        """
        # Handle different envelope types based on the union definition
        if isinstance(envelope, HyperliquidSubscriptionResponse):
            # Subscription responses don't have coin info
            return None

        # For all other envelope types (HyperliquidRawWebSocketEnvelope and subclasses),
        # use the extract_coin method which handles both dict and list data efficiently
        # This covers both HyperliquidRawWebSocketEnvelope and HyperliquidUserEventEnvelope
        coin = envelope.extract_coin()
        if coin:
            return coin

        # Handle empty fills lists using subscription tracking
        if envelope.channel == "trades" and isinstance(envelope.data, list) and not envelope.data:
            subscribed_coins = self._fill_subscriptions.get("trades", set())
            if len(subscribed_coins) == 1:
                coin_from_subscription = next(iter(subscribed_coins))
                self.logger.debug(
                    "empty_fills_using_subscription",
                    channel="trades",
                    exchange=self.exchange_name.value,
                    coin=coin_from_subscription,
                    message=(
                        f"Empty fills list routed to {coin_from_subscription} via subscription"
                    ),
                )
                return coin_from_subscription

        return None

    def _extract_payload_from_envelope(
        self,
        envelope: HyperliquidWebSocketMessage,
    ) -> dict[str, Any] | list[Any]:
        """Extract payload from validated Hyperliquid WebSocket message envelope.

        This method works with properly validated envelope models, providing
        full type safety without any type: ignore statements.

        Args:
            envelope: Validated WebSocket message envelope.

        Returns:
            The payload data for processing.

        """
        # The envelope is already validated, so we can safely access the data field
        # which is typed as dict[str, Any] | list[Any]
        return envelope.data

    async def _enhance_typed_context(
        self,
        context: WebSocketContextProtocol,
        routing_key: str,
    ) -> WebSocketContextProtocol:
        """Enhance typed context with Hyperliquid-specific data.

        The typed context already includes computed fields for coin extraction
        and channel information, so minimal enhancement is needed.

        Args:
            context: Typed context
            routing_key: Message routing key

        Returns:
            Enhanced typed context
        """
        # Typed context already has all computed fields
        return context

    def _get_processor_key_from_routing_key(self, routing_key: str) -> str:
        """Extract processor key from routing key.

        For market data channels with coin specificity (e.g., "l2Book:SOL"),
        extract just the channel part ("l2Book") for processor lookup.

        Args:
            routing_key: The full routing key (e.g., "l2Book:SOL", "trades:BTC")

        Returns:
            The processor key (e.g., "l2Book", "trades")
        """
        # For market data channels, extract channel part before colon
        if ":" in routing_key:
            channel = routing_key.split(":", 1)[0]
            if channel in {"l2Book", "trades", "candle"}:
                return channel

        # For other channels, use routing key as-is
        return routing_key

    async def _route_with_envelope_validation(
        self,
        message: dict[str, Any],
        handlers: dict[str, MessageHandler],
    ) -> None:
        """Override to handle processor lookup with channel extraction.

        Raises:
            EnvelopeValidatorNotSetError: If envelope validator is not configured
        """
        # Step 1: Validate envelope structure first - eliminates type safety issues
        try:
            if self.envelope_validator is None:
                raise EnvelopeValidatorNotSetError
            validated_envelope = self.envelope_validator(message)
        except (ValidationError, ValueError) as e:
            await self._handle_envelope_validation_error(e, message)
            return

        # Step 2: Extract routing key from validated envelope (type-safe!)
        routing_key = self._extract_routing_key_from_envelope(validated_envelope)

        if not routing_key:
            await self._handle_missing_routing_key(message, validated_envelope)
            return

        # Step 3: Get the appropriate handler
        handler = handlers.get(routing_key)
        if not handler:
            # Special handling for fills channel when we can't determine the coin
            if routing_key == "trades" and validated_envelope.channel == "trades":
                # Route to all fills:* handlers
                await self._route_fills_to_all_handlers(validated_envelope, handlers, message)
                return
            await self._handle_missing_handler(message, routing_key, handlers)
            return

        # Step 4: Extract payload from validated envelope (type-safe!)
        payload = self._extract_payload_from_envelope(validated_envelope)

        # Step 5: Create typed context with validated envelope
        message_id = str(uuid.uuid4())
        typed_context = self._create_typed_context(validated_envelope, routing_key, message_id)

        # Step 6: Allow exchanges to enhance typed context
        typed_context = await self._enhance_typed_context(typed_context, routing_key)

        # Step 7: Get processor using channel-based lookup for Hyperliquid
        processor_key = self._get_processor_key_from_routing_key(routing_key)
        processor = self.processors.get(processor_key)
        if processor:
            await processor.process(payload, handler, typed_context)
        else:
            await self._handle_missing_processor(routing_key, payload, typed_context)

    def construct_l2book_subscription_payload(
        self,
        coin: str,
    ) -> HyperliquidRawWsSubscribeRequest:
        """Construct L2 book subscription payload for Hyperliquid.

        Args:
            coin: The coin/asset to subscribe to (e.g., "BTC", "ETH").

        Returns:
            HyperliquidRawWsSubscribeRequest model.

        Raises:
            EmptyStringParameterError: If coin is empty.

        """
        if not coin or not coin.strip():
            raise EmptyStringParameterError(
                parameter_name="coin",
                method_name="construct_l2book_subscription_payload",
            )

        payload = HyperliquidRawWsL2BookSubscriptionPayload(type="l2Book", coin=coin.strip())

        return HyperliquidRawWsSubscribeRequest(
            method="subscribe",
            subscription=payload,
        )

    def construct_trades_subscription_payload(
        self,
        coin: str,
    ) -> HyperliquidRawWsSubscribeRequest:
        """Construct trades subscription payload for Hyperliquid.

        Args:
            coin: The coin/asset to subscribe to (e.g., "BTC", "ETH").

        Returns:
            HyperliquidRawWsSubscribeRequest model.

        Raises:
            EmptyStringParameterError: If coin is empty.

        """
        if not coin or not coin.strip():
            raise EmptyStringParameterError(
                parameter_name="coin",
                method_name="construct_trades_subscription_payload",
            )

        # Track this subscription for routing empty fills lists
        clean_coin = coin.strip()
        self._fill_subscriptions["trades"].add(clean_coin)

        payload = HyperliquidRawWsTradesSubscriptionPayload(type="trades", coin=clean_coin)

        return HyperliquidRawWsSubscribeRequest(
            method="subscribe",
            subscription=payload,
        )

    def construct_user_events_subscription_payload(
        self,
        user_address: str,
    ) -> HyperliquidRawWsSubscribeRequest:
        """Construct user events subscription payload for Hyperliquid.

        Args:
            user_address: The wallet address to subscribe to.

        Returns:
            HyperliquidRawWsSubscribeRequest model.

        Raises:
            EmptyStringParameterError: If user_address is empty.

        """
        if not user_address or not user_address.strip():
            raise EmptyStringParameterError(
                parameter_name="user_address",
                method_name="construct_user_events_subscription_payload",
            )

        payload = HyperliquidRawWsUserEventsSubscriptionPayload(
            type="userEvents",
            user=user_address.strip(),
        )

        return HyperliquidRawWsSubscribeRequest(
            method="subscribe",
            subscription=payload,
        )

    def construct_candle_subscription_payload(
        self,
        coin: str,
        interval: str,
    ) -> HyperliquidRawWsSubscribeRequest:
        """Construct candle subscription payload for Hyperliquid.

        Args:
            coin: The coin/asset to subscribe to (e.g., "BTC", "ETH").
            interval: The timeframe (e.g., "1m", "1h", "1d").

        Returns:
            HyperliquidRawWsSubscribeRequest model.

        Raises:
            EmptyStringParameterError: If coin or interval is empty.

        """
        if not coin or not coin.strip():
            raise EmptyStringParameterError(
                parameter_name="coin",
                method_name="construct_candle_subscription_payload",
            )

        if not interval or not interval.strip():
            raise EmptyStringParameterError(
                parameter_name="interval",
                method_name="construct_candle_subscription_payload",
            )

        # Track this subscription for routing candle messages
        clean_coin = coin.strip()
        clean_interval = interval.strip()
        # Store the full topic for routing consistency with handler registration
        candle_topic = f"candle:{clean_coin}:{clean_interval}"
        self._candle_subscriptions["candle"].add(candle_topic)

        payload = HyperliquidRawWsCandleSubscriptionPayload(
            type="candle",
            coin=clean_coin,
            interval=interval.strip(),
        )

        return HyperliquidRawWsSubscribeRequest(
            method="subscribe",
            subscription=payload,
        )

    def construct_all_mids_subscription_payload(self) -> HyperliquidRawWsSubscribeRequest:
        """Construct all mids subscription payload for Hyperliquid.

        Returns:
            HyperliquidRawWsSubscribeRequest model.

        """
        payload = HyperliquidRawWsAllMidsSubscriptionPayload(type="allMids")

        return HyperliquidRawWsSubscribeRequest(
            method="subscribe",
            subscription=payload,
        )

    def _handle_l2book_topic(self, topic: str) -> HyperliquidRawWsSubscribeRequest:
        """Handle l2Book topic subscription.

        Returns:
            The subscription request for l2Book data
        """
        coin = topic[7:]  # Remove "l2Book:" prefix
        return self.construct_l2book_subscription_payload(coin)

    def _handle_trades_topic(self, topic: str) -> HyperliquidRawWsSubscribeRequest:
        """Handle trades topic subscription.

        Returns:
            The subscription request for trades data
        """
        coin = topic[7:]  # Remove "trades:" prefix
        return self.construct_trades_subscription_payload(coin)

    def _handle_user_events_topic(
        self,
        topic: str,
        wallet_address: str | None,
    ) -> HyperliquidRawWsSubscribeRequest:
        """Handle userEvents topic subscription.

        Returns:
            The subscription request for user events data

        Raises:
            UserAddressRequiredError: If user address is required but not provided
        """
        if topic == "userEvents":
            # Format: userEvents (use provided wallet_address)
            if not wallet_address:
                raise UserAddressRequiredError
            return self.construct_user_events_subscription_payload(wallet_address)

        # Format: userEvents:0x...
        user_address = topic[11:]  # Remove "userEvents:" prefix
        if not user_address and wallet_address:
            user_address = wallet_address
        if not user_address:
            raise UserAddressRequiredError
        return self.construct_user_events_subscription_payload(user_address)

    def _handle_candle_topic(self, topic: str) -> HyperliquidRawWsSubscribeRequest:
        """Handle candle topic subscription.

        Returns:
            The subscription request for candle data

        Raises:
            InvalidCandleTopicFormatError: If topic format is invalid
        """
        parts = topic.split(":")
        if len(parts) != CANDLE_TOPIC_PARTS_COUNT:
            raise InvalidCandleTopicFormatError(topic)
        coin = parts[1]
        interval = parts[2]
        return self.construct_candle_subscription_payload(coin, interval)

    def construct_subscription_payload(
        self,
        topic: str,
        wallet_address: str | None = None,
    ) -> HyperliquidRawWsSubscribeRequest:
        """Construct the subscription payload for a given topic for Hyperliquid.

        This is a generic method that delegates to specific subscription methods
        based on the topic format for backwards compatibility with the API.

        Args:
            topic: The WebSocket topic to subscribe to
            wallet_address: User's wallet address (required for userEvents)

        Returns:
            HyperliquidRawWsSubscribeRequest model

        Raises:
            UnsupportedTopicFormatError: If topic format is not supported

        """
        # Parse topic format and delegate to appropriate method
        if topic == "allMids":
            return self.construct_all_mids_subscription_payload()
        if topic.startswith("l2Book:"):
            return self._handle_l2book_topic(topic)
        if topic.startswith("trades:"):
            return self._handle_trades_topic(topic)
        if topic == "userEvents" or topic.startswith("userEvents:"):
            return self._handle_user_events_topic(topic, wallet_address)
        if topic.startswith("candle:"):
            return self._handle_candle_topic(topic)
        raise UnsupportedTopicFormatError(topic)

    async def _route_fills_to_all_handlers(
        self,
        validated_envelope: HyperliquidWebSocketMessage,
        handlers: dict[str, MessageHandler],
        message: dict[str, Any],
    ) -> None:
        """Route fills messages to all fills:* handlers when coin can't be determined.

        This handles the case where fills data is empty (common on testnet) and we
        can't determine which coin the fills are for.

        Args:
            validated_envelope: The validated envelope
            handlers: All registered handlers
            message: The original message
        """
        # Find all handlers that start with "trades:"
        fill_handlers = {
            key: handler for key, handler in handlers.items() if key.startswith("trades:")
        }

        if not fill_handlers:
            self.logger.debug(
                "no_fill_handlers_found",
                exchange=self.exchange_name.value,
                message="No fills:* handlers found for empty fills message",
            )
            return

        # Extract payload once
        payload = self._extract_payload_from_envelope(validated_envelope)

        # Get the processor
        processor = self.processors.get("trades")
        if not processor:
            # Create a minimal typed context for error handling
            message_id = str(uuid.uuid4())
            error_context = self._create_typed_context(validated_envelope, "trades", message_id)
            await self._handle_missing_processor("trades", payload, error_context)
            return

        # Route to each fill handler
        for routing_key, handler in fill_handlers.items():
            # Create typed context for this specific handler
            message_id = str(uuid.uuid4())
            typed_context = self._create_typed_context(validated_envelope, routing_key, message_id)
            typed_context = await self._enhance_typed_context(typed_context, routing_key)

            # Process with this handler
            await processor.process(payload, handler, typed_context)

    # Note: route_message is implemented above to use envelope validation
