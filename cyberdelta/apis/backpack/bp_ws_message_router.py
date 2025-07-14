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

import time
from typing import Any, Literal

from cyberdelta.apis.backpack.bp_ws_raw_message_handler import BackpackWsRawMessageHandler
from cyberdelta.apis.backpack.mappers.account.bp_position_mapper import BackpackPositionMapper
from cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper import BackpackTransactionMapper
from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.mappers.market_data.bp_ticker_mapper import BackpackTickerMapper
from cyberdelta.apis.backpack.mappers.trading.bp_order_mapper import BackpackOrderMapper
from cyberdelta.apis.backpack.models.bp_ws_payloads import (
    BackpackRawWsSubscriptionRequest,
    BackpackWsSignatureComponents,
)
from cyberdelta.apis.common import APIError, MessageHandler, TransformationError
from cyberdelta.apis.exceptions import DataTransformationError
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.exceptions.service_validation import EmptyStringParameterError


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
        trading_data_mapper: BackpackOrderMapper,
        raw_ws_handler: BackpackWsRawMessageHandler,
        exchange_name: str,
    ) -> None:
        """Initialize the WebSocket message router.

        Args:
            trading_data_mapper: Mapper for order data transformations
            raw_ws_handler: Handler for raw WebSocket message validation
            exchange_name: Name of the exchange for logging purposes

        """
        self._trading_data_mapper = trading_data_mapper
        self._raw_ws_handler = raw_ws_handler
        self._exchange_name = exchange_name
        self.logger = get_logger(__name__)

        # Error suppression for repeated unroutable messages
        self._suppressed_errors: dict[str, float] = {}
        self._error_counts: dict[str, int] = {}
        self._suppression_duration = 300  # 5 minutes

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
            raise EmptyStringParameterError(
                parameter_name="topic",
                method_name="construct_subscription_payload",
            )

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
            raise DataTransformationError(
                source_model="subscription_params",
                target_model="BackpackRawWsSubscriptionRequest",
                reason=str(e),
                original_error=e,
            ) from e

    def _extract_topic_and_data(
        self,
        message: dict[str, Any],
    ) -> tuple[str | None, dict[str, Any] | None]:
        """Extract topic and data from WebSocket message.

        Returns:
            Tuple of (topic string, data payload) from the message.
        """
        topic_str: str | None = message.get("topic")
        data_payload: dict[str, Any] | None = message.get("data")

        # Handle different message formats
        if not topic_str:
            raw_event_type = message.get("type")
            if isinstance(raw_event_type, str) and raw_event_type in {
                "fills",
                "orders",
                "positionUpdate",
            }:
                topic_str = raw_event_type

        return topic_str, data_payload

    def _get_base_topic(self, topic_str: str) -> str:
        """Determine base topic for handler lookup.

        Returns:
            Base topic string for handler lookup.
        """
        base_topic = topic_str
        if base_topic.startswith("depth."):
            base_topic = "depth"
        elif base_topic.startswith("ticker."):
            base_topic = "ticker"
        return base_topic

    def _find_handler(
        self,
        topic_str: str,
        base_topic: str,
        ws_handlers: dict[str, MessageHandler],
    ) -> MessageHandler | None:
        """Find appropriate handler for the topic.

        Returns:
            Message handler if found, None otherwise.
        """
        app_handler = ws_handlers.get(topic_str)
        if not app_handler:
            app_handler = ws_handlers.get(base_topic)
        return app_handler

    async def _process_topic_specific_payload(
        self,
        base_topic: str,
        topic_str: str,
        data_payload: dict[str, Any],
    ) -> tuple[Any, Any]:
        """Process payload based on topic type and return validated payload and internal model.

        Returns:
            Tuple of (validated_payload, internal_model) for the specific topic.
        """
        validated_payload: Any = None
        internal_model: Any = None

        if base_topic == "depth":
            validated_payload = self._raw_ws_handler.handle_depth_payload(data_payload)
            # Extract symbol from topic (e.g., "depth.SOL_USDC" -> "SOL_USDC")
            symbol_from_topic = topic_str.split(".", 1)[1] if "." in topic_str else "UNKNOWN"
            internal_model = BackpackOrderBookMapper.transform_ws_depth_event_to_internal(
                symbol_from_topic,
                validated_payload,
            )
        elif base_topic == "ticker":
            validated_payload = self._raw_ws_handler.handle_ticker_payload(data_payload)
            internal_model = BackpackTickerMapper.transform_ws_ticker_event_to_internal(
                validated_payload,
            )
        elif base_topic == "fills":
            validated_payload = self._raw_ws_handler.handle_trade_event_payload(data_payload)
            internal_model = BackpackTransactionMapper.transform_ws_fill_event_to_internal_trade(
                validated_payload,
            )
        elif base_topic == "orders":
            validated_payload = self._raw_ws_handler.handle_order_update_payload(data_payload)
            internal_model = self._trading_data_mapper.transform_ws_order_update_to_internal_order(
                validated_payload,
            )
        elif base_topic == "positionUpdate":
            validated_payload = self._raw_ws_handler.handle_position_update_payload(
                data_payload,
            )
            internal_model = (
                BackpackPositionMapper.transform_ws_position_update_to_internal_position(
                    validated_payload,
                )
            )

        return validated_payload, internal_model

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
        # Extract topic and data
        topic_str, data_payload = self._extract_topic_and_data(message)

        if not topic_str:
            # Create error key for suppression
            error_key = str(message.get("error", {}).get("code", "unknown"))
            current_time = time.time()

            # Track error count
            self._error_counts[error_key] = self._error_counts.get(error_key, 0) + 1

            # Check if we should suppress this error
            if error_key not in self._suppressed_errors:
                # First occurrence - log and suppress future
                self.logger.warning(
                    "backpack_unroutable_message_suppressing",
                    exchange_name=self._exchange_name,
                    message=message,
                    error_key=error_key,
                    action="suppressing_future",
                    message_text="Unroutable message (suppressing future) - no clear string topic",
                )
                self._suppressed_errors[error_key] = current_time
            elif current_time - self._suppressed_errors[error_key] > self._suppression_duration:
                # Re-log after suppression duration
                self.logger.warning(
                    "backpack_repeated_unroutable_message",
                    exchange_name=self._exchange_name,
                    message=message,
                    error_key=error_key,
                    error_count=self._error_counts[error_key],
                    action="re_logging_after_suppression",
                    message_text="Repeated unroutable message - no clear string topic",
                )
                self._suppressed_errors[error_key] = current_time
                self._error_counts[error_key] = 0  # Reset count
            # Else: suppress the log
            return

        if data_payload is None:
            self.logger.debug(
                "backpack_no_data_payload",
                exchange_name=self._exchange_name,
                topic_str=topic_str,
                message=message,
                message_text="Received message with topic/type but no data_payload",
            )
            return

        # Get base topic and find handler
        base_topic = self._get_base_topic(topic_str)
        app_handler = self._find_handler(topic_str, base_topic, ws_handlers)

        if not app_handler:
            self.logger.debug(
                "backpack_no_handler_registered",
                exchange_name=self._exchange_name,
                topic_str=topic_str,
                base_topic=base_topic,
                message_text="No application handler registered for topic",
            )
            return

        try:
            # Process payload based on topic type
            validated_payload, internal_model = await self._process_topic_specific_payload(
                base_topic,
                topic_str,
                data_payload,
            )

            if validated_payload is None and internal_model is None:
                # Unknown topic, send raw payload
                self.logger.warning(
                    "backpack_no_raw_ws_validator",
                    exchange_name=self._exchange_name,
                    topic_str=topic_str,
                    base_topic=base_topic,
                    action="sending_raw_payload",
                    message_text="No specific raw WS validator for topic, "
                    "application handler will receive raw payload",
                )
                await app_handler(data_payload, message)
                return

            # Pass the Internal Domain Model to the application handler
            await app_handler(internal_model, message)

        except APIError as e:
            self.logger.exception(
                "backpack_api_error_validating_ws",
                exchange_name=self._exchange_name,
                topic_str=topic_str,
                base_topic=base_topic,
                error_message=e.message,
                message_text="APIError validating WS payload for topic",
            )
        except TransformationError as e_transform:
            self.logger.exception(
                "backpack_transformation_error_ws",
                exchange_name=self._exchange_name,
                topic_str=topic_str,
                base_topic=base_topic,
                error=str(e_transform),
                message_text="TransformationError transforming WS payload for topic",
            )
        except Exception as e_app:
            self.logger.exception(
                "backpack_application_handler_error",
                exchange_name=self._exchange_name,
                topic_str=topic_str,
                base_topic=base_topic,
                error=str(e_app),
                message_text="Error in application handler for topic",
            )
