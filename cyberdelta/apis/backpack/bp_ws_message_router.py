"""CyberDeltaEngine: Backpack WebSocket Message Router.
--------------------------------------------------

This module implements the `BackpackWsMessageRouter` class, responsible for:
- Constructing subscription payloads for WebSocket topics
- Routing and processing incoming WebSocket messages
- Validating raw payloads and transforming them to internal domain models

This router isolates all WebSocket-specific logic from the main BackpackAPI client,
providing a cleaner separation of concerns and better testability.
"""

from __future__ import annotations

from typing import Any, Literal

from cyberdelta.apis.backpack.bp_ws_raw_message_handler import BackpackWsRawMessageHandler
from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.mappers.bp_market_data_mapper import BackpackMarketDataMapper
from cyberdelta.apis.backpack.mappers.bp_trading_data_mapper import BackpackTradingDataMapper
from cyberdelta.apis.backpack.models.bp_ws_payloads import (
    BackpackRawWsSubscriptionRequest,
    BackpackWsSignatureComponents,
)
from cyberdelta.apis.base.exchange_api import MessageHandler
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.config.logging_config import get_logger


class BackpackWsMessageRouter:
    """Routes and processes WebSocket messages for Backpack exchange.

    This class handles the entire WebSocket message processing pipeline:
    1. Constructs subscription payloads for various topics
    2. Validates incoming raw WebSocket payloads
    3. Transforms raw data to internal domain models
    4. Routes processed data to appropriate application handlers
    """

    def __init__(
        self,
        market_data_mapper: BackpackMarketDataMapper,
        account_data_mapper: BackpackAccountDataMapper,
        trading_data_mapper: BackpackTradingDataMapper,
        raw_ws_handler: BackpackWsRawMessageHandler,
        exchange_name: str,
    ) -> None:
        """Initialize the WebSocket message router.

        Args:
            market_data_mapper: Mapper for market data transformations
            account_data_mapper: Mapper for account data transformations
            trading_data_mapper: Mapper for trading data transformations
            raw_ws_handler: Handler for raw WebSocket message validation
            exchange_name: Name of the exchange for logging purposes

        """
        self._market_data_mapper = market_data_mapper
        self._account_data_mapper = account_data_mapper
        self._trading_data_mapper = trading_data_mapper
        self._raw_ws_handler = raw_ws_handler
        self._exchange_name = exchange_name
        self.logger = get_logger(__name__)

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
            ValueError: If topic format is invalid or empty

        """
        # Basic topic validation
        if not topic or not topic.strip():
            raise ValueError("Topic cannot be empty for Backpack subscription.")

        # For Backpack, the standard subscription format is used
        method_val: Literal["SUBSCRIBE", "UNSUBSCRIBE"] = "SUBSCRIBE"

        # Create params list with the topic
        params_val = [topic]

        # Convert BackpackWsSignatureComponents to tuple format if provided
        signature_val_tuple: tuple[str, str, str, str] | None = None
        if signature_components:
            signature_val_tuple = (
                signature_components.api_key,  # verifying key
                signature_components.signature,  # signature
                signature_components.timestamp,  # timestamp
                signature_components.window,  # window
            )

        # Create the subscription request
        try:
            return BackpackRawWsSubscriptionRequest(
                method=method_val,
                params=params_val,
                signature=signature_val_tuple,
            )
        except Exception as e:
            # Wrap unexpected exceptions
            raise ValueError(
                f"Failed to construct subscription payload for topic '{topic}': {e}",
            ) from e

    async def route_message(
        self,
        message: dict[str, Any],
        ws_handlers: dict[str, MessageHandler],
    ) -> None:
        """Route incoming WebSocket messages to the appropriate handler based on topic.

        This method processes the complete WebSocket message handling pipeline:
        1. Extracts topic and data from the message
        2. Finds the appropriate application handler
        3. Validates the raw payload using BackpackWsRawMessageHandler
        4. Transforms the validated payload to internal domain models
        5. Calls the application handler with the transformed data

        Args:
            message: The raw WebSocket message dictionary
            ws_handlers: Mapping of topic strings to application MessageHandler callbacks

        """
        topic_str: str | None = message.get("topic")
        data_payload: dict[str, Any] | None = message.get("data")
        event_type_str: str | None = None

        # Handle different message formats
        if not topic_str:
            raw_event_type = message.get("type")
            if isinstance(raw_event_type, str) and raw_event_type in [
                "fills",
                "orders",
                "positionUpdate",
            ]:
                event_type_str = raw_event_type
                topic_str = event_type_str
            else:
                self.logger.debug(
                    f"[{self._exchange_name}] Unroutable message - no clear string topic "
                    f"and not a known event type: {message}",
                )
                return

        if data_payload is None:
            self.logger.debug(
                f"[{self._exchange_name}] Received message with topic/type '{topic_str}' "
                f"but no data_payload: {message}",
            )
            return

        # topic_str is now guaranteed to be a string
        app_handler = ws_handlers.get(str(topic_str))

        # Determine base topic for handler lookup
        base_topic = str(topic_str)
        if base_topic.startswith("depth."):
            base_topic = "depth"
        elif base_topic.startswith("ticker."):
            base_topic = "ticker"

        # Try base topic if specific topic handler not found
        if not app_handler:
            app_handler = ws_handlers.get(base_topic)

        if not app_handler:
            self.logger.debug(
                f"[{self._exchange_name}] No application handler registered for topic: "
                f"{topic_str} (or base topic: {base_topic})",
            )
            return

        try:
            validated_payload: Any = None
            internal_model: Any = None

            # Process based on base topic type
            if base_topic == "depth":
                validated_payload = self._raw_ws_handler.handle_depth_payload(data_payload)
                # Extract symbol from topic (e.g., "depth.SOL_USDC" -> "SOL_USDC")
                symbol_from_topic = topic_str.split(".", 1)[1] if "." in topic_str else "UNKNOWN"
                internal_model = self._market_data_mapper.transform_ws_depth_event_to_internal(
                    symbol_from_topic,
                    validated_payload,
                )
            elif base_topic == "ticker":
                validated_payload = self._raw_ws_handler.handle_ticker_payload(data_payload)
                internal_model = self._market_data_mapper.transform_ws_ticker_event_to_internal(
                    validated_payload,
                )
            elif base_topic == "fills":
                validated_payload = self._raw_ws_handler.handle_trade_event_payload(data_payload)
                # For fills topic, this could be either public trades or private fills
                # Based on the context, assume this is private account fills
                internal_model = (
                    self._account_data_mapper.transform_ws_fill_event_to_internal_trade(
                        validated_payload,
                    )
                )
            elif base_topic == "orders":
                validated_payload = self._raw_ws_handler.handle_order_update_payload(data_payload)
                internal_model = (
                    self._trading_data_mapper.transform_ws_order_update_to_internal_order(
                        validated_payload,
                    )
                )
            elif base_topic == "positionUpdate":
                validated_payload = self._raw_ws_handler.handle_position_update_payload(
                    data_payload,
                )
                internal_model = (
                    self._account_data_mapper.transform_ws_position_update_to_internal_position(
                        validated_payload,
                    )
                )
            else:
                self.logger.warning(
                    f"[{self._exchange_name}] No specific raw WS validator for topic "
                    f"'{topic_str}' (base: '{base_topic}'). "
                    f"Application handler will receive raw payload.",
                )
                await app_handler(data_payload, message)
                return

            # Pass the Internal Domain Model to the application handler
            await app_handler(internal_model, message)

        except APIError as e:
            self.logger.error(
                f"[{self._exchange_name}] APIError validating WS payload for topic "
                f"{topic_str} (base: {base_topic}): {e.message}",
                exc_info=True,
            )
        except TransformationError as e_transform:
            self.logger.error(
                f"[{self._exchange_name}] TransformationError transforming WS payload for topic "
                f"{topic_str} (base: {base_topic}): {e_transform}",
                exc_info=True,
            )
        except Exception as e_app:
            self.logger.error(
                f"[{self._exchange_name}] Error in application handler for topic {topic_str} "
                f"(base: {base_topic}): {e_app}",
                exc_info=True,
            )
