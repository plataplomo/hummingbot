"""CyberDeltaEngine: Hyperliquid WebSocket Message Router.

-----------------------------------------------------

This module implements the `HyperliquidWsMessageRouter` class, responsible for:
- Constructing subscription payloads for WebSocket topics
- Routing and processing incoming WebSocket messages
- Validating raw payloads and transforming them to internal domain models

This router isolates all WebSocket-specific logic from the main HyperliquidAPI client,
providing a cleaner separation of concerns and better testability.
"""

from __future__ import annotations

from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.common import APIError, MessageHandler, TransformationError
from cyberdelta.apis.exceptions import (
    InvalidWebSocketDataError,
    UnsupportedWebSocketTopicError,
    UserEventsSubscriptionError,
    WebSocketSubscriptionError,
)
from cyberdelta.apis.hyperliquid.hl_ws_raw_message_handler import HyperliquidWsRawMessageHandler
from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import (
    HyperliquidRawWsAllMidsSubscriptionPayload,
    HyperliquidRawWsCandleSubscriptionPayload,
    HyperliquidRawWsL2BookSubscriptionPayload,
    HyperliquidRawWsSubscribeRequest,
    HyperliquidRawWsTradesSubscriptionPayload,
    HyperliquidRawWsUserEventsSubscriptionPayload,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    OrderBookMapperProtocol,
    OrderMapperProtocol,
    PositionMapperProtocol,
    TransactionMapperProtocol,
)
from cyberdelta.config.structlog_config import get_logger


# WebSocket subscription parsing constants
MIN_PARTS_FOR_COIN_SUBSCRIPTION = 2  # Minimum parts needed for coin-based subscriptions
MIN_PARTS_FOR_CANDLE_SUBSCRIPTION = 3  # Minimum parts needed for candle subscriptions


class HyperliquidWsMessageRouter:
    """Routes and processes WebSocket messages for Hyperliquid exchange.

    This class handles the entire WebSocket message processing pipeline:
    1. Constructs subscription payloads for various topics
    2. Validates incoming raw WebSocket payloads
    3. Transforms raw data to internal domain models
    4. Routes processed data to appropriate application handlers
    """

    def __init__(
        self,
        order_book_mapper: OrderBookMapperProtocol,
        transaction_mapper: TransactionMapperProtocol,
        position_mapper: PositionMapperProtocol,
        order_mapper: OrderMapperProtocol,
        raw_ws_handler: HyperliquidWsRawMessageHandler,
        exchange_name: str,
    ) -> None:
        """Initialize the WebSocket message router.

        Args:
            order_book_mapper: Mapper for order book and trade transformations
            transaction_mapper: Mapper for transaction/fill transformations
            position_mapper: Mapper for position transformations
            order_mapper: Mapper for order transformations
            raw_ws_handler: Handler for raw WebSocket message validation
            exchange_name: Name of the exchange for logging purposes

        """
        self._order_book_mapper = order_book_mapper
        self._transaction_mapper = transaction_mapper
        self._position_mapper = position_mapper
        self._order_mapper = order_mapper
        self._raw_ws_handler = raw_ws_handler
        self._exchange_name = exchange_name
        self.logger = get_logger(__name__)

    def construct_subscription_payload(
        self,
        topic: str,
        wallet_address: str | None,
    ) -> HyperliquidRawWsSubscribeRequest:
        """Construct the subscription payload for a given topic for Hyperliquid.

        Hyperliquid uses a format like:
        {"method": "subscribe", "subscription": payload}
        Payload depends on the subscription type.

        Args:
            topic: The WebSocket topic to subscribe to
            wallet_address: User's wallet address (required for userEvents)

        Returns:
            HyperliquidRawWsSubscribeRequest model

        Raises:
            ValueError: If topic format is invalid or required info is missing
            APIError: If topic is not supported by the exchange

        """
        try:
            inner_payload = self._create_subscription_payload(topic, wallet_address)

            # Create the top-level subscription request
            return HyperliquidRawWsSubscribeRequest(
                method="subscribe",  # Hyperliquid uses lowercase
                subscription=inner_payload,
            )

        except (ValueError, APIError):
            # Re-raise these exceptions as they are already informative
            raise
        except Exception as e:
            # Wrap unexpected exceptions
            raise WebSocketSubscriptionError(topic, e) from e

    def _create_subscription_payload(
        self,
        topic: str,
        wallet_address: str | None,
    ) -> (
        HyperliquidRawWsL2BookSubscriptionPayload
        | HyperliquidRawWsTradesSubscriptionPayload
        | HyperliquidRawWsUserEventsSubscriptionPayload
        | HyperliquidRawWsCandleSubscriptionPayload
        | HyperliquidRawWsAllMidsSubscriptionPayload
    ):
        """Create the inner subscription payload based on topic.

        Args:
            topic: The WebSocket topic to subscribe to
            wallet_address: User's wallet address (required for userEvents)

        Returns:
            The appropriate subscription payload model

        Raises:
            UnsupportedWebSocketTopicError: If topic is not supported
        """
        # Parse the topic to determine subscription type and parameters
        # Hyperliquid topics: "l2Book:ETH", "trades:BTC", "userEvents", "candle:BTC:1m", "allMids"
        parts = topic.split(":", 2)  # Split into at most 3 parts
        sub_type = parts[0]

        if sub_type == "l2Book" and len(parts) >= MIN_PARTS_FOR_COIN_SUBSCRIPTION:
            coin = parts[1]
            return HyperliquidRawWsL2BookSubscriptionPayload(type="l2Book", coin=coin)
        if sub_type == "trades" and len(parts) >= MIN_PARTS_FOR_COIN_SUBSCRIPTION:
            coin = parts[1]
            return HyperliquidRawWsTradesSubscriptionPayload(type="trades", coin=coin)
        if sub_type == "userEvents":
            validated_wallet = self._validate_wallet_address(wallet_address)
            return HyperliquidRawWsUserEventsSubscriptionPayload(
                type="userEvents",
                user=validated_wallet,
            )
        if sub_type == "candle" and len(parts) >= MIN_PARTS_FOR_CANDLE_SUBSCRIPTION:
            coin = parts[1]
            interval = parts[2]
            return HyperliquidRawWsCandleSubscriptionPayload(
                type="candle",
                coin=coin,
                interval=interval,
            )
        if sub_type == "allMids":
            return HyperliquidRawWsAllMidsSubscriptionPayload(type="allMids")
        raise UnsupportedWebSocketTopicError(topic)

    async def route_message(
        self,
        message: dict[str, Any],
        ws_handlers: dict[str, MessageHandler],
    ) -> None:
        """Route incoming WebSocket messages from Hyperliquid.

        This method processes the complete WebSocket message handling pipeline:
        1. Extracts channel and data from the message
        2. Finds the appropriate application handler
        3. Validates the raw payload using HyperliquidWsRawMessageHandler
        4. Transforms the validated payload to internal domain models
        5. Calls the application handler with the transformed data

        Args:
            message: The raw WebSocket message dictionary
            ws_handlers: Mapping of topic strings to application MessageHandler callbacks

        """
        channel: str | None = message.get("channel")
        raw_data_any: Any = message.get("data")  # Keep as Any initially

        if not channel:
            self.logger.debug(
                "unroutable_ws_message_no_channel",
                action="route_message",
                exchange=self._exchange_name,
                message="[%s] Unroutable WS message (no channel): %s",
                message_args=(self._exchange_name, message),
            )
            return

        if channel in {"pong", "subscriptionResponse"}:
            # Control messages are routine - use trace level to reduce spam
            self.logger.trace(
                "control_message_received",
                action="handle_control_message",
                exchange=self._exchange_name,
                channel=channel,
                message=f"[{self._exchange_name}] Control message on '{channel}'",
            )
            return

        # Determine topic key for handler lookup
        topic_key_for_handler = self._determine_topic_key(channel, raw_data_any, message)

        # Find application handler
        app_handler = self._find_app_handler(ws_handlers, topic_key_for_handler, channel, message)
        if not app_handler:
            return

        if raw_data_any is None:
            self.logger.warning(
                "ws_channel_no_data",
                action="route_message",
                exchange=self._exchange_name,
                channel=channel,
                message="[%s] WS '%s' has no data. Msg: %s",
                message_args=(self._exchange_name, channel, message),
            )
            return

        try:
            await self._process_channel_data(channel, raw_data_any, app_handler, message)
        except APIError as e:
            self.logger.exception(
                "api_error_in_ws_routing",
                action="route_message",
                exchange=self._exchange_name,
                channel=channel,
                error_message=e.message,
                message="[%s] APIError in WS routing for %s: %s",
                message_args=(self._exchange_name, channel, e.message),
            )
        except ValidationError as e_val:
            self.logger.exception(
                "unexpected_pydantic_validation_error",
                action="route_message",
                exchange=self._exchange_name,
                channel=channel,
                validation_error=str(e_val),
                message="[%s] Unexpected Pydantic ValidationErr for %s: %s",
                message_args=(self._exchange_name, channel, e_val),
            )
        except Exception as e_app:
            self.logger.exception(
                "error_in_app_handler",
                action="route_message",
                exchange=self._exchange_name,
                channel=channel,
                error_details=str(e_app),
                message="[%s] Error in app_handler for %s: %s",
                message_args=(self._exchange_name, channel, e_app),
            )

    def _determine_topic_key(
        self,
        channel: str,
        raw_data_any: object,
        message: dict[str, Any],
    ) -> str:
        """Determine the topic key for handler lookup based on channel and data."""
        topic_key_for_handler = channel

        if channel == "l2Book":
            topic_key_for_handler = self._get_l2book_topic_key(channel, raw_data_any, message)
        elif channel == "trades":
            topic_key_for_handler = self._get_trades_topic_key(channel, raw_data_any)
        elif channel == "userEvents":
            topic_key_for_handler = "userEvents"

        return topic_key_for_handler

    def _get_l2book_topic_key(
        self,
        channel: str,
        raw_data_any: object,
        message: dict[str, Any],
    ) -> str:
        """Get topic key for l2Book channel."""
        if isinstance(raw_data_any, dict):
            raw_data_dict = cast("dict[str, Any]", raw_data_any)
            coin_from_data_any: Any = raw_data_dict.get("coin")
            if isinstance(coin_from_data_any, str):
                return f"{channel}:{coin_from_data_any}"
        else:
            self.logger.warning(
                "l2book_data_unexpected_type",
                action="get_l2book_topic_key",
                exchange=self._exchange_name,
                data_type=type(raw_data_any).__name__,
                message=(
                    "[%s] Expected dict for 'l2Book' data to derive topic key, "
                    "received other type. Msg: %s"
                ),
                message_args=(self._exchange_name, message),
            )
        return channel

    def _get_trades_topic_key(self, channel: str, raw_data_any: object) -> str:
        """Get topic key for trades channel."""
        coin_for_topic_str: str | None = None
        if isinstance(raw_data_any, list):
            # Explicitly type the list after check, elements are still Any
            # After isinstance check, we know it's a list
            # DEFENSIVE CHECK: raw_data_any is confirmed as list[Any] by isinstance.
            raw_list: list[Any] = cast("list[Any]", raw_data_any)  # type: ignore[redundant-cast]
            checked_list_for_topic_derivation: list[dict[str, Any]] = [
                cast("dict[str, Any]", item) for item in raw_list if isinstance(item, dict)
            ]

            if checked_list_for_topic_derivation:
                first_item_for_topic_any: Any = checked_list_for_topic_derivation[0]
                if isinstance(first_item_for_topic_any, dict):
                    first_item_dict = cast("dict[str, Any]", first_item_for_topic_any)
                    coin_from_item_any: Any = first_item_dict.get("coin")
                    if isinstance(coin_from_item_any, str):
                        coin_for_topic_str = coin_from_item_any

        if coin_for_topic_str:
            return f"{channel}:{coin_for_topic_str}"
        return channel

    def _find_app_handler(
        self,
        ws_handlers: dict[str, MessageHandler],
        topic_key_for_handler: str,
        channel: str,
        message: dict[str, Any],
    ) -> MessageHandler | None:
        """Find the appropriate application handler for the message."""
        app_handler = ws_handlers.get(topic_key_for_handler)
        if not app_handler:
            generic_app_handler = ws_handlers.get(channel)
            if generic_app_handler:
                app_handler = generic_app_handler
            else:
                log_parts = [f"[{self._exchange_name}] No WS handler for '{topic_key_for_handler}'"]
                if topic_key_for_handler != channel:
                    log_parts.append(f" (or base '{channel}')")
                log_parts.append(f". Msg: {message}")
                self.logger.debug("".join(log_parts))
                return None
        return app_handler

    async def _process_channel_data(
        self,
        channel: str,
        raw_data_any: object,
        app_handler: MessageHandler,
        message: dict[str, Any],
    ) -> None:
        """Process data for different channel types."""
        if channel == "l2Book":
            await self._process_l2book_data(raw_data_any, app_handler, message)
        elif channel == "trades":
            await self._process_trades_data(raw_data_any, app_handler, message)
        elif channel == "userEvents":
            await self._process_user_events_data(raw_data_any, app_handler, message)
        elif channel == "allMids":
            await self._process_allmids_data(raw_data_any, app_handler, message)
        elif channel in {"pong", "subscriptionResponse"}:
            await self._process_control_message_data(raw_data_any, app_handler, message, channel)
        else:
            await self._process_unhandled_channel_data(raw_data_any, app_handler, message, channel)

    async def _process_l2book_data(
        self,
        raw_data_any: object,
        app_handler: MessageHandler,
        message: dict[str, Any],
    ) -> None:
        """Process l2Book channel data."""
        if not isinstance(raw_data_any, dict):
            raise InvalidWebSocketDataError(
                channel="l2Book",
                expected_type="dict",
                data=raw_data_any,
            )
        validated_book_model = self._raw_ws_handler.handle_l2book_payload(
            cast("dict[str, Any]", raw_data_any),
        )

        try:
            # Transform raw validated model to internal domain model
            internal_orderbook = self._order_book_mapper.transform_ws_book_update_to_internal(
                validated_book_model,
            )
            # Convert internal model to dict for handler compatibility
            orderbook_dict = internal_orderbook.model_dump(mode="json")
            await app_handler(orderbook_dict, message)
        except TransformationError as e_transform:
            self.logger.exception(
                "l2book_transformation_failed",
                action="process_l2book_data",
                exchange=self._exchange_name,
                error_details=str(e_transform),
                message="[%s] Failed to transform l2Book data: %s",
                message_args=(self._exchange_name, e_transform),
            )

    async def _process_trades_data(
        self,
        raw_data_any: object,
        app_handler: MessageHandler,
        message: dict[str, Any],
    ) -> None:
        """Process trades channel data."""
        if not isinstance(raw_data_any, list):
            raise InvalidWebSocketDataError(
                channel="trades",
                expected_type="list",
                data=raw_data_any,
            )

        # Convert list items to dict format for validation
        trade_payloads: list[dict[str, Any]] = []
        # Explicitly type the list after check, elements are still Any
        checked_list_of_trades: list[dict[str, Any]] = []
        # After isinstance check, we know it's a list
        # DEFENSIVE CHECK: raw_data_any is confirmed as list[Any] by isinstance.
        raw_list_trades: list[Any] = cast("list[Any]", raw_data_any)  # type: ignore[redundant-cast]
        for item in raw_list_trades:
            if isinstance(item, dict):
                item_dict = cast("dict[str, Any]", item)
                checked_list_of_trades.append(item_dict)
                trade_payloads.append(item_dict)

        if trade_payloads:
            validated_trade_models = self._raw_ws_handler.handle_public_trades_payload(
                trade_payloads,
            )
            for validated_trade_model in validated_trade_models:
                try:
                    # Transform raw validated model to internal domain model
                    internal_trade = self._order_book_mapper.transform_ws_trade_event_to_internal(
                        validated_trade_model,
                    )
                    # Convert internal model to dict for handler compatibility
                    trade_dict = internal_trade.model_dump(mode="json")
                    await app_handler(trade_dict, message)
                except TransformationError as e_transform:
                    self.logger.exception(
                        "trade_transformation_failed",
                        action="process_trades_data",
                        exchange=self._exchange_name,
                        error_details=str(e_transform),
                        message="[%s] Failed to transform trade data: %s",
                        message_args=(self._exchange_name, e_transform),
                    )

    async def _process_user_events_data(
        self,
        raw_data_any: object,
        app_handler: MessageHandler,
        message: dict[str, Any],
    ) -> None:
        """Process userEvents channel data."""
        if not isinstance(raw_data_any, list):
            raise InvalidWebSocketDataError(
                channel="userEvents",
                expected_type="list",
                data=raw_data_any,
            )
        # Explicitly type the list after check, elements are still Any
        # After isinstance check, we know it's a list
        # DEFENSIVE CHECK: raw_data_any is confirmed as list[Any] by isinstance.
        raw_list_events: list[Any] = cast("list[Any]", raw_data_any)  # type: ignore[redundant-cast]
        checked_list_of_any_events: list[dict[str, Any]] = [
            cast("dict[str, Any]", event_loop_var_any)
            for event_loop_var_any in raw_list_events
            if isinstance(event_loop_var_any, dict)
        ]

        for event_item_dict in checked_list_of_any_events:
            await self._process_single_user_event(event_item_dict, app_handler, message)

    async def _process_single_user_event(
        self,
        event_item_dict: dict[str, Any],
        app_handler: MessageHandler,
        message: dict[str, Any],
    ) -> None:
        """Process a single user event item."""
        event_type_any = event_item_dict.get("type")

        if not isinstance(event_type_any, str):
            self.logger.warning(
                "user_event_no_type_string",
                action="process_single_user_event",
                exchange=self._exchange_name,
                event_item=event_item_dict,
                message="[%s] userEvent item has no 'type' string: %s, skipping.",
                message_args=(self._exchange_name, event_item_dict),
            )
            return

        event_type_str: str = event_type_any

        try:
            if event_type_str == "fill":
                await self._process_fill_event(event_item_dict, app_handler, message)
            elif event_type_str == "order":
                await self._process_order_event(event_item_dict, app_handler, message)
            elif event_type_str == "positionUpdate":
                await self._process_position_update_event(event_item_dict, app_handler, message)
            else:
                self.logger.debug(
                    "unhandled_user_event_type",
                    action="process_single_user_event",
                    exchange=self._exchange_name,
                    event_type=event_type_str,
                    event_item=event_item_dict,
                    message="[%s] Unhandled userEvent type: %s. Passing raw item: %s",
                    message_args=(self._exchange_name, event_type_str, event_item_dict),
                )
                await app_handler(event_item_dict, message)

        except (APIError, ValidationError) as e_user_event_item:
            self.logger.exception(
                "user_event_processing_error",
                action="process_single_user_event",
                exchange=self._exchange_name,
                event_type=event_type_str,
                error_details=str(e_user_event_item),
                event_item=event_item_dict,
                message=(
                    "[%s] Error processing userEvent item (type: %s): %s. Item: %s. Skipping item."
                ),
                message_args=(
                    self._exchange_name,
                    event_type_str,
                    e_user_event_item,
                    event_item_dict,
                ),
            )

    async def _process_fill_event(
        self,
        event_item_dict: dict[str, Any],
        app_handler: MessageHandler,
        message: dict[str, Any],
    ) -> None:
        """Process a fill event."""
        validated_fill = self._raw_ws_handler.handle_user_fill_event_payload(
            event_item_dict,
        )
        try:
            # Transform raw validated model to internal domain model
            transform_method = self._transaction_mapper.transform_ws_fill_event_to_internal
            internal_trade = transform_method(validated_fill)
            # Convert internal model to dict for handler compatibility
            trade_dict = internal_trade.model_dump(mode="json")
            await app_handler(trade_dict, message)
        except TransformationError as e_transform:
            self.logger.exception(
                "fill_event_transformation_failed",
                action="process_fill_event",
                exchange=self._exchange_name,
                error_details=str(e_transform),
                message="[%s] Failed to transform fill event: %s",
                message_args=(self._exchange_name, e_transform),
            )

    async def _process_order_event(
        self,
        event_item_dict: dict[str, Any],
        app_handler: MessageHandler,
        message: dict[str, Any],
    ) -> None:
        """Process an order event."""
        handle_order_wrapper = self._raw_ws_handler.handle_user_order_update_wrapper_payload
        order_update_wrapper = handle_order_wrapper(event_item_dict)
        handle_order_event = self._raw_ws_handler.handle_user_order_event_payload
        validated_order_details = handle_order_event(order_update_wrapper.data)
        try:
            # Transform raw validated model to internal domain model
            order_transform_method = self._order_mapper.transform_ws_order_update_to_internal_order
            internal_order = order_transform_method(validated_order_details)
            # Convert internal model to dict for handler compatibility
            order_dict = internal_order.model_dump(mode="json")
            await app_handler(order_dict, message)
        except TransformationError as e_transform:
            self.logger.exception(
                "order_event_transformation_failed",
                action="process_order_event",
                exchange=self._exchange_name,
                error_details=str(e_transform),
                message="[%s] Failed to transform order event: %s",
                message_args=(self._exchange_name, e_transform),
            )

    async def _process_position_update_event(
        self,
        event_item_dict: dict[str, Any],
        app_handler: MessageHandler,
        message: dict[str, Any],
    ) -> None:
        """Process a position update event."""
        handle_pos_update = self._raw_ws_handler.handle_user_position_update_event_payload
        validated_position_update = handle_pos_update(event_item_dict)
        try:
            # Transform raw validated model to internal domain model
            position_transform_method = (
                self._position_mapper.transform_ws_position_update_to_internal_position
            )
            internal_position = position_transform_method(
                validated_position_update,
            )
            # Convert internal model to dict for handler compatibility
            position_dict = internal_position.model_dump(mode="json")
            await app_handler(position_dict, message)
        except TransformationError as e_transform:
            self.logger.exception(
                "position_event_transformation_failed",
                action="process_position_update_event",
                exchange=self._exchange_name,
                error_details=str(e_transform),
                message="[%s] Failed to transform position event: %s",
                message_args=(self._exchange_name, e_transform),
            )

    async def _process_allmids_data(
        self,
        raw_data_any: object,
        app_handler: MessageHandler,
        message: dict[str, Any],
    ) -> None:
        """Process allMids channel data."""
        if not isinstance(raw_data_any, dict):
            self.logger.warning(
                "allmids_data_not_dict",
                action="process_allmids_data",
                exchange=self._exchange_name,
                data_type=type(raw_data_any).__name__,
                data_repr=repr(raw_data_any),
                message="[%s] 'allMids' channel data is not a dict or is None. Data: %r. Skipping.",
                message_args=(self._exchange_name, raw_data_any),
            )
            raise InvalidWebSocketDataError(
                channel="allMids",
                expected_type="dict",
                data=raw_data_any,
            )

        raw_data_dict_all_mids = cast("dict[str, Any]", raw_data_any)

        validated_all_mids = self._raw_ws_handler.handle_all_mids_payload(
            raw_data_dict_all_mids,
        )
        payload_for_handler = validated_all_mids.model_dump(mode="json")
        await app_handler(payload_for_handler, message)

    async def _process_control_message_data(
        self,
        raw_data_any: object,
        app_handler: MessageHandler,
        message: dict[str, Any],
        channel: str,
    ) -> None:
        """Process control message data (pong, subscriptionResponse)."""
        self.logger.debug(
            "control_message_processed",
            action="process_control_message_data",
            exchange=self._exchange_name,
            channel=channel,
            message_content=message,
            message="[%s] Control message on '%s': %s",
            message_args=(self._exchange_name, channel, message),
        )
        payload_for_handler = (
            cast("dict[str, Any]", raw_data_any) if isinstance(raw_data_any, dict) else {}
        )
        await app_handler(payload_for_handler, message)

    async def _process_unhandled_channel_data(
        self,
        raw_data_any: object,
        app_handler: MessageHandler,
        message: dict[str, Any],
        channel: str,
    ) -> None:
        """Process data for unhandled channel types."""
        self.logger.debug(
            "unhandled_channel_processed",
            action="process_unhandled_channel_data",
            exchange=self._exchange_name,
            channel=channel,
            message_content=message,
            message=(
                "[%s] Unhandled channel '%s' by specific validation, "
                "passing raw data if dict. Msg: %s"
            ),
            message_args=(self._exchange_name, channel, message),
        )
        payload_for_handler = (
            cast("dict[str, Any]", raw_data_any) if isinstance(raw_data_any, dict) else {}
        )
        await app_handler(payload_for_handler, message)

    def _validate_wallet_address(self, wallet_address: str | None) -> str:
        """Validate wallet address is provided for userEvents subscription.

        Args:
            wallet_address: The wallet address to validate

        Returns:
            The validated non-None wallet address

        Raises:
            UserEventsSubscriptionError: If wallet address is None
        """
        if wallet_address is None:
            raise UserEventsSubscriptionError
        return wallet_address
