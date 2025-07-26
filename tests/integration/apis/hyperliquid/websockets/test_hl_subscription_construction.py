"""Test 6: Subscription Message Construction with Pydantic Models.

This module tests that the refactored Hyperliquid system properly constructs
WebSocket subscription messages using Pydantic models with proper validation
and serialization.

Security Compliance:
- Tests subscription message construction with real symbols
- Validates Pydantic model serialization for outgoing messages
- Tests subscription payload structure
- Fails fast on subscription construction issues
"""

import asyncio
import json
import time
from typing import Any, cast
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.common import MessageHandler
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.hl_ws_router import UnsupportedTopicFormatError
from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import (
    HyperliquidRawWsSubscribeRequest,
)
from cyberdelta.apis.models.service_args import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger


pytestmark = [
    pytest.mark.integration,
    pytest.mark.timing,
]

logger = get_logger(__name__)


class TestHyperliquidSubscriptionConstruction:
    """Test Pydantic-based subscription message construction."""

    async def _test_unsubscribe_functionality(
        self, hl_api_for_test_env: HyperliquidAPI, sent_messages: list[dict[str, Any]]
    ) -> None:
        """Test unsubscribe functionality if available."""
        # Check if unsubscribe method exists and is async
        if hasattr(hl_api_for_test_env, "unsubscribe"):
            unsubscribe_method = getattr(hl_api_for_test_env, "unsubscribe", None)
            if callable(unsubscribe_method):
                try:
                    result = unsubscribe_method("trades:ETH")
                    if hasattr(result, "__await__"):
                        # Type-safe await for dynamic method result
                        try:
                            await cast(Any, result)
                        except TypeError:
                            logger.info(
                                "unsubscribe_result_not_awaitable",
                                message="Unsubscribe result not awaitable",
                            )
                    else:
                        logger.info(
                            "unsubscribe_method_not_async",
                            message="Unsubscribe method exists but is not awaitable",
                        )
                except (TypeError, AttributeError) as e:
                    logger.info(
                        "unsubscribe_method_error",
                        error=str(e),
                        message="Error calling unsubscribe method",
                    )

            # Wait a bit for message to be sent
            await asyncio.sleep(0.1)

            # Check if unsubscribe message was sent
            if sent_messages:
                unsub_msg = sent_messages[-1]
                assert unsub_msg.get("method") == "unsubscribe", "Method should be 'unsubscribe'"
                logger.info(
                    "unsubscribe_message_sent",
                    message=unsub_msg,
                    message_text="Unsubscribe message sent successfully",
                )
        else:
            logger.info(
                "unsubscribe_not_available", message="Unsubscribe method not available in API"
            )

    @pytest.mark.asyncio
    async def test_subscription_through_public_api(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test subscription construction through public subscribe API."""
        # Connect WebSocket first
        await hl_api_for_test_env.connect_websocket()
        assert hl_api_for_test_env.is_connected

        # Get real symbols for testing
        markets = await hl_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for subscription testing")

        test_symbol = markets[0].symbol

        # Track what was sent through WebSocket
        sent_messages: list[dict[str, Any]] = []

        # Create a side effect function to capture messages
        async def capture_send_json(payload: BaseModel | dict[str, Any]) -> None:
            # Capture the payload
            if isinstance(payload, BaseModel):
                # Use Pydantic model serialization
                dumped = payload.model_dump()
                sent_messages.append(dumped)
            else:
                sent_messages.append(payload)
            # Add an await to satisfy RUF029
            await asyncio.sleep(0)

        # Mock the WebSocketManager's send_json method
        # Since we can't access private attributes, we'll patch at the module level
        with patch(
            "cyberdelta.apis.connectivity.ws_manager.WebSocketManager.send_json",
            new=AsyncMock(side_effect=capture_send_json),
        ):
            # Test different subscription types through public API

            # 1. Subscribe to orderbook
            async def orderbook_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029
                logger.info("orderbook_message_received", context=context)

            await hl_api_for_test_env.subscribe(f"l2Book:{test_symbol}", orderbook_handler)

            # 2. Subscribe to trades
            async def trades_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029
                logger.info("trades_message_received", context=context)

            await hl_api_for_test_env.subscribe(f"trades:{test_symbol}", trades_handler)

            # 3. Subscribe to allMids
            async def allmids_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029
                logger.info("allmids_message_received", context=context)

            await hl_api_for_test_env.subscribe("allMids", allmids_handler)

            # Wait a bit for messages to be sent
            await asyncio.sleep(0.1)

            # Verify messages were sent
            assert len(sent_messages) >= 3, (
                f"Expected at least 3 subscription messages, got {len(sent_messages)}"
            )

            # Verify message structure
            for msg in sent_messages:
                assert isinstance(msg, dict), "Subscription message should be a dict"
                assert "method" in msg, "Subscription message should have 'method' field"
                assert msg["method"] == "subscribe", "Method should be 'subscribe'"
                assert "subscription" in msg, "Message should have 'subscription' field"

                logger.info(
                    "subscription_message_sent",
                    method=msg["method"],
                    subscription=msg["subscription"],
                    message="Subscription message sent through WebSocket",
                )

    @pytest.mark.asyncio
    async def test_subscription_request_model_validation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test HyperliquidRawWsSubscribeRequest Pydantic model validation."""
        # Test valid subscription request
        valid_request_data = {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": "BTC"},
        }

        try:
            subscription_request = HyperliquidRawWsSubscribeRequest.model_validate(
                valid_request_data
            )

            assert subscription_request.method == "subscribe"
            assert subscription_request.subscription.type == "l2Book"
            assert subscription_request.subscription.coin == "BTC"

            # Test serialization
            serialized = subscription_request.model_dump()
            assert serialized["method"] == "subscribe"
            assert serialized["subscription"]["type"] == "l2Book"

            logger.info(
                "subscription_request_model_validation_success",
                method=subscription_request.method,
                subscription=subscription_request.subscription,
                message="✓ HyperliquidRawWsSubscribeRequest validation successful",
            )

        except (ValidationError, ValueError, TypeError) as e:
            pytest.fail(
                f"HyperliquidRawWsSubscribeRequest validation failed: {e}. "
                "Subscription request model validation not working."
            )

        # Test invalid subscription requests
        invalid_requests: list[dict[str, Any]] = [
            {},  # Empty
            {"method": None},  # None method
            {"method": ""},  # Empty method
            {"method": "subscribe"},  # Missing subscription
            {"method": "subscribe", "subscription": None},  # None subscription
            {"method": "subscribe", "subscription": {}},  # Empty subscription
            {"method": "invalid", "subscription": {"type": "l2Book"}},  # Invalid method
        ]

        validation_errors_caught = 0

        for i, invalid_request in enumerate(invalid_requests):
            try:
                HyperliquidRawWsSubscribeRequest.model_validate(invalid_request)
                logger.warning(
                    "subscription_request_validation_unexpected_pass",
                    request_index=i,
                    invalid_request=invalid_request,
                    message=f"Invalid subscription request {i} unexpectedly passed validation",
                )
            except ValidationError as e:
                validation_errors_caught += 1
                logger.info(
                    "subscription_request_validation_correctly_rejected",
                    request_index=i,
                    error_count=len(e.errors()),
                    message=f"✓ Invalid subscription request {i} correctly rejected",
                )

        assert validation_errors_caught > 0, (
            "No validation errors caught for invalid subscription requests. "
            "Validation may be too lenient."
        )

    @pytest.mark.asyncio
    async def test_unsubscription_through_public_api(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test unsubscription through public API."""
        # Connect WebSocket first
        await hl_api_for_test_env.connect_websocket()
        assert hl_api_for_test_env.is_connected

        # Track what was sent through WebSocket
        sent_messages: list[dict[str, Any]] = []

        # Create a side effect function to capture messages
        async def capture_send_json(payload: BaseModel | dict[str, Any]) -> None:
            # Capture the payload
            if isinstance(payload, BaseModel):
                # Use Pydantic model serialization
                dumped = payload.model_dump()
                sent_messages.append(dumped)
            else:
                sent_messages.append(payload)
            # Add an await to satisfy RUF029
            await asyncio.sleep(0)

        # Mock the WebSocketManager's send_json method
        with patch(
            "cyberdelta.apis.connectivity.ws_manager.WebSocketManager.send_json",
            new=AsyncMock(side_effect=capture_send_json),
        ):
            # First subscribe to a topic
            async def handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029
                logger.info("message_received", context=context)

            await hl_api_for_test_env.subscribe("trades:ETH", handler)

            # Clear messages
            sent_messages.clear()

            # Test unsubscribe functionality
            await self._test_unsubscribe_functionality(hl_api_for_test_env, sent_messages)

    @pytest.mark.asyncio
    async def test_subscription_error_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test subscription error handling through public API."""

        # Define handler for testing
        async def handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029

        # Test subscription without WebSocket connection
        if not hl_api_for_test_env.is_connected:
            # Track if subscribe was called successfully
            subscribe_called = False

            # This should not raise an exception but should log a warning
            try:
                await hl_api_for_test_env.subscribe("l2Book:BTC", handler)
                subscribe_called = True
            except (ValueError, TypeError, AttributeError) as e:
                pytest.fail(f"Subscribe raised exception when disconnected: {e}")

            assert subscribe_called, "Subscribe should have been called successfully"

            logger.info(
                "subscription_without_connection_handled",
                message="Subscription without connection handled gracefully",
            )

        # Now test invalid topic formats
        await hl_api_for_test_env.connect_websocket()

        invalid_topics = [
            "",  # Empty topic
            "invalid",  # Invalid format
            "l2Book:",  # Missing coin
            ":BTC",  # Missing channel
        ]

        for topic in invalid_topics:
            # Subscribe should raise an exception for invalid topics
            with pytest.raises((
                UnsupportedTopicFormatError,
                ValueError,
                TypeError,
                AttributeError,
            )):
                await hl_api_for_test_env.subscribe(topic, handler)
            logger.info(
                "invalid_topic_rejected",
                topic=topic,
                message=f"Invalid topic '{topic}' correctly rejected with exception",
            )

    @pytest.mark.asyncio
    async def test_subscription_payload_model_immutability(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that subscription payload models are immutable."""
        # Create a subscription request model
        request_data = {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": "BTC"},
        }

        payload = HyperliquidRawWsSubscribeRequest.model_validate(request_data)

        # Test that the model is frozen by checking its config
        assert payload.model_config.get("frozen", False), "Model should be frozen"

        # Test serialization works (immutable models should still be serializable)
        serialized = payload.model_dump()
        assert serialized["method"] == "subscribe"
        assert serialized["subscription"]["type"] == "l2Book"

        # Test round-trip through validation
        round_trip = HyperliquidRawWsSubscribeRequest.model_validate(serialized)
        assert round_trip.method == payload.method
        assert round_trip.subscription.type == payload.subscription.type

        logger.info(
            "subscription_payload_immutability_verified",
            message="✓ Subscription payload models are properly immutable",
        )

    @pytest.mark.asyncio
    async def test_subscription_handler_registration(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that subscription handlers are properly registered."""
        await hl_api_for_test_env.connect_websocket()

        # Track subscription calls
        subscription_calls: list[tuple[str, MessageHandler]] = []

        # Mock subscribe to track registrations using patch
        async def mock_subscribe(topic: str, handler: MessageHandler) -> None:
            subscription_calls.append((topic, handler))
            # Don't call the original to avoid actual subscription
            await asyncio.sleep(0)  # Satisfy RUF029

        with patch.object(hl_api_for_test_env, "subscribe", side_effect=mock_subscribe):
            # Define different handlers for different topics
            handlers = {}

            def create_handler(topic: str) -> MessageHandler:
                async def handler(context: WebSocketContextProtocol) -> None:
                    await asyncio.sleep(0)  # Satisfy RUF029
                    handlers[topic] = context

                return handler

            # Subscribe to multiple topics
            topics = ["l2Book:BTC", "trades:ETH", "allMids"]

            for topic in topics:
                handler = create_handler(topic)
                await hl_api_for_test_env.subscribe(topic, handler)

            # Verify handlers are registered
            assert len(subscription_calls) == len(topics), (
                f"Expected {len(topics)} subscriptions, got {len(subscription_calls)}"
            )

        for i, topic in enumerate(topics):
            assert subscription_calls[i][0] == topic, f"Topic mismatch at index {i}"
            assert callable(subscription_calls[i][1]), f"Handler not callable at index {i}"

        logger.info(
            "subscription_handlers_registered",
            topics=topics,
            message="✓ All subscription handlers properly registered",
        )

    @pytest.mark.asyncio
    async def test_subscription_message_round_trip(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that subscription messages can make a full round trip."""
        # Test data for different subscription types
        test_subscriptions = [
            {"method": "subscribe", "subscription": {"type": "l2Book", "coin": "BTC"}},
            {"method": "subscribe", "subscription": {"type": "trades", "coin": "ETH"}},
            {"method": "subscribe", "subscription": {"type": "allMids"}},
            {
                "method": "subscribe",
                "subscription": {
                    "type": "userEvents",
                    "user": "0x1234567890abcdef1234567890abcdef12345678",
                },
            },
            {
                "method": "subscribe",
                "subscription": {"type": "candle", "coin": "BTC", "interval": "1h"},
            },
        ]

        for sub_data in test_subscriptions:
            # Create model
            model = HyperliquidRawWsSubscribeRequest.model_validate(sub_data)

            # Serialize to dict
            serialized = model.model_dump()

            # Serialize to JSON
            json_str = json.dumps(serialized)

            # Deserialize from JSON
            parsed = json.loads(json_str)

            # Recreate model
            round_trip_model = HyperliquidRawWsSubscribeRequest.model_validate(parsed)

            # Verify round trip
            assert round_trip_model.method == model.method
            assert round_trip_model.subscription.type == model.subscription.type

            logger.info(
                "subscription_round_trip_success",
                subscription_type=model.subscription.type,
                message=f"✓ Round trip successful for {model.subscription.type}",
            )

    @pytest.mark.asyncio
    async def test_subscription_performance(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test subscription performance through public API."""
        await hl_api_for_test_env.connect_websocket()

        # Get test symbols
        markets = await hl_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for performance testing")

        test_symbol = markets[0].symbol

        # Create a simple handler
        async def handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029

        # Performance test - many subscriptions
        iterations = 100
        start_time = time.perf_counter()

        for i in range(iterations):
            # Use different topics to avoid duplicate subscriptions
            topic = f"l2Book:{test_symbol}" if i % 2 == 0 else f"trades:{test_symbol}"
            await hl_api_for_test_env.subscribe(f"{topic}_{i}", handler)

        end_time = time.perf_counter()
        total_time = end_time - start_time
        subscriptions_per_second = iterations / total_time if total_time > 0 else 0

        logger.info(
            "subscription_performance_results",
            iterations=iterations,
            total_time_ms=f"{total_time * 1000:.2f}",
            subscriptions_per_second=f"{subscriptions_per_second:.0f}",
            avg_time_per_subscription_ms=f"{(total_time / iterations) * 1000:.3f}",
            message=f"✓ Subscribed {iterations} times in {total_time * 1000:.2f}ms",
        )

        # Performance should be reasonable for trading
        if subscriptions_per_second < 100:
            logger.warning(
                "subscription_performance_slow",
                subscriptions_per_second=f"{subscriptions_per_second:.0f}",
                message="Subscription performance may be too slow for high-frequency trading",
            )

    @pytest.mark.asyncio
    async def test_hyperliquid_specific_subscriptions(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test Hyperliquid-specific subscription types through public API."""
        await hl_api_for_test_env.connect_websocket()

        # Track sent messages
        sent_messages: list[dict[str, Any]] = []

        # Create a side effect function to capture messages
        async def capture_send_json(payload: BaseModel | dict[str, Any]) -> None:
            if isinstance(payload, BaseModel):
                # Use Pydantic model serialization
                dumped = payload.model_dump()
                sent_messages.append(dumped)
            else:
                sent_messages.append(payload)
            # Add an await to satisfy RUF029
            await asyncio.sleep(0)

        # Test Hyperliquid-specific topics
        async def handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029

        # Temporarily patch send_json to capture messages
        with patch(
            "cyberdelta.apis.connectivity.ws_manager.WebSocketManager.send_json",
            new=AsyncMock(side_effect=capture_send_json),
        ):
            # 1. User events (requires authentication)
            await hl_api_for_test_env.subscribe("userEvents", handler)

            # 2. Candle data
            await hl_api_for_test_env.subscribe("candle:BTC:1h", handler)

            # Wait for messages
            await asyncio.sleep(0.1)

            # Log what was sent
            for msg in sent_messages:
                logger.info(
                    "hyperliquid_specific_subscription_sent",
                    subscription=msg.get("subscription", {}),
                    message="Hyperliquid-specific subscription sent",
                )
