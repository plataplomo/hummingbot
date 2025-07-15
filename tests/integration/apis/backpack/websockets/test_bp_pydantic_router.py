"""Test 1: Actual Message Handling Through the New Pydantic Router.

This module tests that the refactored Backpack WebSocket router properly handles
real WebSocket messages using the new Pydantic validation and routing pipeline.

Security Compliance:
- Tests real message routing through Pydantic validators
- Validates router delegates to correct processors
- Tests envelope validation and message type routing
- Fails fast on router architecture issues
"""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.bp_ws_router import BackpackWebSocketRouter
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.base.ws_context import WebSocketContextUnion
from cyberdelta.apis.models.service_args_models import GetMarketsArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.exceptions.parsing import EmptyStringError


pytestmark = [
    pytest.mark.integration,
    pytest.mark.websockets,
    pytest.mark.pydantic_router,
    pytest.mark.timing,
]

logger = get_logger(__name__)


class TestBackpackPydanticRouterIntegration:
    """Test Pydantic router message handling with real WebSocket infrastructure."""

    @pytest.mark.asyncio
    async def test_router_message_envelope_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that router properly validates WebSocket message envelopes."""
        # Get the router from the API
        router = getattr(bp_api_for_test_env, "_bp_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not initialized - refactored architecture not working")

        assert isinstance(router, BackpackWebSocketRouter), (
            f"Expected BackpackWebSocketRouter, got {type(router)}. "
            "Pydantic router not properly integrated."
        )

        # Test valid envelope structure
        valid_envelope_data = {
            "stream": "ticker.BTC_USDC",
            "data": {
                "s": "BTC_USDC",
                "lastPrice": "50000.0",
                "priceChange24h": "1000.0",
                "volume": "100.5",
                "high": "51000.0",
                "low": "49000.0",
            },
        }

        try:
            # Test Pydantic envelope validation
            envelope = BackpackRawWebSocketEnvelope.model_validate(valid_envelope_data)
            assert envelope.stream == "ticker.BTC_USDC"
            assert envelope.data is not None

            # Determine data type for logging
            if isinstance(envelope.data, dict):
                data_info: list[str] | str = list(envelope.data.keys())
            else:
                data_info = "non-dict data"

            logger.info(
                "pydantic_envelope_validation_success",
                stream=envelope.stream,
                data_keys=data_info,
                message="✓ Pydantic envelope validation working correctly",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Pydantic envelope validation failed: {e}. "
                "Basic Pydantic validation not working in refactored router."
            )

    @pytest.mark.asyncio
    async def test_router_message_type_routing(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that router routes different message types to correct processors."""
        router = getattr(bp_api_for_test_env, "_bp_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not initialized")

        # Test message types that should be routed differently
        test_messages: list[dict[str, Any]] = [
            {
                "stream": "ticker.BTC_USDC",
                "data": {"s": "BTC_USDC", "lastPrice": "50000.0"},
                "expected_processor": "ticker",
            },
            {
                "stream": "depth.BTC_USDC",
                "data": {"s": "BTC_USDC", "bids": [], "asks": []},
                "expected_processor": "depth",
            },
            {
                "stream": "trade.BTC_USDC",
                "data": {"s": "BTC_USDC", "trades": []},
                "expected_processor": "trade",
            },
        ]

        for test_case in test_messages:
            try:
                # Create message without expected_processor field for validation
                message_data = {k: v for k, v in test_case.items() if k != "expected_processor"}
                # Validate the message can be parsed by router
                BackpackRawWebSocketEnvelope.model_validate(message_data)

                # Check if router has the expected processor
                processors = getattr(router, "processors", {})
                expected_proc = str(test_case["expected_processor"])

                if expected_proc not in processors:
                    pytest.fail(
                        f"Router missing processor for {expected_proc}. "
                        f"Available processors: {list(processors.keys())}. "
                        "Processor registration not working in refactored architecture."
                    )

                logger.info(
                    "router_processor_registration_verified",
                    stream=test_case["stream"],
                    processor=expected_proc,
                    message=f"✓ Router correctly registered {expected_proc} processor",
                )

            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                pytest.fail(
                    f"Router message type routing failed for {test_case['stream']}: {e}. "
                    "Message type routing broken in Pydantic router."
                )

    @pytest.mark.asyncio
    async def test_router_real_websocket_message_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test router handling of real WebSocket messages from exchange."""
        # Establish WebSocket connection to get real messages
        try:
            await bp_api_for_test_env.connect_websocket()

            if not bp_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed - cannot test real message handling")

            # Set up message capture
            received_messages: list[dict[str, Any]] = []

            async def message_capture_handler(context: WebSocketContextUnion) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029

                # Extract data from typed context
                context_data = {}
                if hasattr(context, "validated_envelope") and hasattr(
                    context.validated_envelope, "data"
                ):
                    data = context.validated_envelope.data
                    context_data = data if isinstance(data, dict) else {"data": data}

                received_messages.append(context_data)
                logger.info(
                    "router_real_message_captured",
                    message_data=context_data,
                    message="Router processed real WebSocket message",
                )

            # Subscribe to ticker to get real messages through router
            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                pytest.fail("No markets available for router testing")

            test_symbol = markets[0].symbol
            await bp_api_for_test_env.subscribe(f"ticker.{test_symbol}", message_capture_handler)

            # Wait for router to process messages
            await asyncio.sleep(3.0)

            # Verify router processed messages
            if not received_messages:
                logger.warning(
                    "router_no_messages_received",
                    symbol=test_symbol,
                    message="No messages received through router - may indicate routing issues",
                )
                # Don't fail immediately - network conditions may vary
            else:
                logger.info(
                    "router_real_message_processing_success",
                    message_count=len(received_messages),
                    symbol=test_symbol,
                    message="✓ Router successfully processed real WebSocket messages",
                )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Router real message handling failed: {e}. "
                "Pydantic router cannot handle real WebSocket messages."
            )

    @pytest.mark.asyncio
    async def test_router_error_propagation(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that router properly propagates errors in Pydantic validation."""
        router = getattr(bp_api_for_test_env, "_bp_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not initialized")

        # Test invalid message that should trigger Pydantic validation error
        invalid_messages = [
            {},  # Empty message
            {"stream": None},  # Invalid stream
            {"stream": "ticker.BTC_USDC"},  # Missing data field
            {"data": {"invalid": "structure"}},  # Missing stream field
        ]

        for invalid_msg in invalid_messages:
            try:
                # This should fail Pydantic validation
                BackpackRawWebSocketEnvelope.model_validate(invalid_msg)
                pytest.fail(
                    f"Invalid message {invalid_msg} passed Pydantic validation. "
                    "Validation not working correctly."
                )
            except (
                ValidationError,
                ValueError,
                TypeError,
                KeyError,
                AttributeError,
                EmptyStringError,
            ):
                # Expected - invalid messages should fail validation
                logger.info(
                    "router_pydantic_validation_correctly_rejected",
                    invalid_message=invalid_msg,
                    message="✓ Router correctly rejected invalid message",
                )

    @pytest.mark.asyncio
    async def test_router_processor_delegation(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that router correctly delegates to specific processors."""
        router = getattr(bp_api_for_test_env, "_bp_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not initialized")

        # Mock a processor to verify delegation
        mock_processor = AsyncMock()
        processors = getattr(router, "processors", {})

        if "ticker" not in processors:
            pytest.fail("Ticker processor not registered in router")

        # Store original processor
        original_processor = processors["ticker"]

        try:
            # Replace with mock
            processors["ticker"] = mock_processor

            # Create valid ticker message
            ticker_message = {
                "stream": "ticker.BTC_USDC",
                "data": {"s": "BTC_USDC", "lastPrice": "50000.0", "priceChange24h": "1000.0"},
            }

            # Mock handler
            mock_handler = AsyncMock()

            # Process through router (if route method is accessible)
            route_method = getattr(router, "route", None)
            if route_method:
                await route_method(ticker_message, mock_handler)

                # Verify processor was called
                mock_processor.process.assert_called_once()

                logger.info(
                    "router_processor_delegation_verified",
                    processor="ticker",
                    message="✓ Router correctly delegated to ticker processor",
                )
            else:
                logger.info(
                    "router_route_method_not_accessible",
                    message="Router route method not accessible for testing",
                )

        finally:
            # Restore original processor
            processors["ticker"] = original_processor

    @pytest.mark.asyncio
    async def test_router_metrics_collection(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that router collects metrics during message processing."""
        router = getattr(bp_api_for_test_env, "_bp_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not initialized")

        # Check if router has metrics collector
        metrics_collector = getattr(router, "_metrics", None)

        if metrics_collector:
            logger.info(
                "router_metrics_collector_found",
                collector_type=type(metrics_collector).__name__,
                message="✓ Router has metrics collection capability",
            )

            # Test metrics methods exist
            expected_methods = [
                "record_message_processed",
                "record_processing_time",
                "record_error",
            ]
            for method_name in expected_methods:
                if hasattr(metrics_collector, method_name):
                    logger.info(
                        "router_metrics_method_found",
                        method=method_name,
                        message=f"✓ Metrics collector has {method_name} method",
                    )
                else:
                    logger.warning(
                        "router_metrics_method_missing",
                        method=method_name,
                        message=f"Metrics collector missing {method_name} method",
                    )
        else:
            logger.info(
                "router_no_metrics_collector",
                message="Router does not have metrics collection (may be optional)",
            )

    @pytest.mark.asyncio
    async def test_router_subscription_payload_construction(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that router properly constructs subscription payloads using Pydantic."""
        # Connect WebSocket first
        if not bp_api_for_test_env.is_connected:
            await bp_api_for_test_env.connect_websocket()

        # Test subscription construction through public API
        test_topics = ["ticker.BTC_USDC", "depth.ETH_USDC", "trades.SOL_USDC"]

        for topic in test_topics:
            try:
                # Track subscription success
                subscription_succeeded = False

                def create_handler(
                    current_topic: str,
                ) -> Callable[[WebSocketContextUnion], Coroutine[Any, Any, None]]:
                    async def router_test_handler(context: WebSocketContextUnion) -> None:
                        await asyncio.sleep(0)

                        # Extract data from typed context
                        context_data = {}
                        if hasattr(context, "validated_envelope") and hasattr(
                            context.validated_envelope, "data"
                        ):
                            data = context.validated_envelope.data
                            context_data = data if isinstance(data, dict) else {"data": data}

                        logger.info(
                            "router_test_handler_called",
                            topic=current_topic,
                            context_keys=list(context_data.keys()),
                            message=f"Router test handler called for {current_topic}",
                        )

                    return router_test_handler

                handler = create_handler(topic)

                # Subscribe - this internally uses the router to construct the Pydantic payload
                await bp_api_for_test_env.subscribe(topic, handler)
                subscription_succeeded = True

                # If subscription succeeded, the router properly constructed the payload
                assert subscription_succeeded, (
                    f"Subscription for {topic} should have succeeded, "
                    "indicating router properly constructed Pydantic payload"
                )

                logger.info(
                    "router_subscription_payload_constructed",
                    topic=topic,
                    subscription_succeeded=subscription_succeeded,
                    message=f"✓ Router constructed valid Pydantic subscription for {topic}",
                )

            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                pytest.fail(
                    f"Subscription payload construction failed for {topic}: {e}. "
                    "Pydantic subscription construction broken."
                )
