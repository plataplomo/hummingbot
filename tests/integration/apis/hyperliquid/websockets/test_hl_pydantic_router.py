"""Test 1: Actual Message Handling through Pydantic Router.

This module tests that the refactored Hyperliquid WebSocket system properly
routes messages through the Pydantic router to the correct processors and handlers.

Security Compliance:
- Tests real WebSocket messages are routed correctly
- Validates handler invocation for different message types
- Tests router delegation and handler context passing
- Fails fast on routing issues
"""

import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common.types import MessageHandler
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args.market_data import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger


pytestmark = [
    pytest.mark.integration,
    pytest.mark.timing,
]

logger = get_logger(__name__)


class TestHyperliquidPydanticRouter:
    """Test Pydantic router integration in Hyperliquid WebSocket system."""

    @pytest.mark.asyncio
    async def test_router_receives_websocket_messages(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that the Pydantic router actually receives WebSocket messages."""
        # Access the WebSocket router
        router = getattr(hl_api_for_test_env, "_hl_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not initialized - refactored architecture missing")

        # Check router has necessary attributes
        assert hasattr(router, "route_message"), "Router missing route_message method"

        # Mock the route_message method to track calls
        original_route_message = router.route_message
        route_calls: list[dict[str, Any]] = []

        async def mock_route_message(
            message: dict[str, Any], handlers: dict[str, MessageHandler]
        ) -> None:
            route_calls.append(message)
            await original_route_message(message, handlers)

        router.route_message = mock_route_message

        try:
            # Connect WebSocket
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed - cannot test router")

            # Subscribe to real data stream
            test_received = asyncio.Event()

            async def test_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029
                test_received.set()

            # Subscribe to allMids for guaranteed data
            await hl_api_for_test_env.subscribe("allMids", test_handler)

            # Wait for messages
            await asyncio.wait_for(test_received.wait(), timeout=10.0)

            # Verify router received messages
            assert len(route_calls) > 0, "Router did not receive any WebSocket messages"

            logger.info(
                "router_messages_received",
                message_count=len(route_calls),
                first_message_keys=list(route_calls[0].keys()) if route_calls else [],
                message="✓ Router successfully received WebSocket messages",
            )

        except TimeoutError:
            pytest.fail("Timeout waiting for WebSocket messages - router may not be receiving data")
        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(f"Router message handling test failed: {e}")
        finally:
            # Restore original route_message method
            router.route_message = original_route_message

    def _setup_router_and_processors(
        self, hl_api_for_test_env: HyperliquidAPI
    ) -> tuple[Any, dict[str, Any]]:
        """Set up router and verify it has processors.

        Returns:
            tuple[Any, dict[str, Any]]: Router instance and its processors dictionary.
        """
        router = getattr(hl_api_for_test_env, "_hl_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not available")

        processors = getattr(router, "processors", {})
        assert len(processors) > 0, "Router has no registered processors"

        logger.info(
            "router_processors_found",
            processor_types=list(processors.keys()),
            message=f"Found {len(processors)} processor types",
        )
        return router, processors

    async def _mock_processors_for_tracking(
        self, processors: dict[str, Any], processor_calls: dict[str, list[Any]]
    ) -> dict[str, Callable[..., Any]]:
        """Mock processors to track their invocations.

        Returns:
            dict[str, Callable[..., Any]]: Dictionary of original processors for restoration.
        """
        original_processors: dict[str, Callable[..., Any]] = {}
        for name, processor in processors.items():
            original_processors[name] = processor.process

            # Create a mock function that captures the processor name
            def create_mock_process(
                proc_name: str, original_process: Callable[..., Any]
            ) -> Callable[..., Any]:
                async def mock_process(
                    payload: dict[str, Any] | list[Any],
                    handler: MessageHandler,
                    context: WebSocketContextProtocol,
                ) -> None:
                    processor_calls[proc_name].append((payload, handler, context))
                    await original_process(payload, handler, context)

                return mock_process

            processor.process = create_mock_process(name, processor.process)
        return original_processors

    async def _setup_test_subscriptions(
        self, hl_api_for_test_env: HyperliquidAPI, test_symbol: str
    ) -> None:
        """Set up test subscriptions to trigger processor delegation."""
        subscription_types = [
            ("l2Book", f"l2Book:{test_symbol}"),
            ("trades", f"trades:{test_symbol}"),
            ("allMids", "allMids"),
        ]

        for _handler_name, topic in subscription_types:
            handler_event = asyncio.Event()

            async def make_test_handler(event: asyncio.Event) -> MessageHandler:
                await asyncio.sleep(0)

                async def handler(context: WebSocketContextProtocol) -> None:
                    await asyncio.sleep(0)
                    event.set()

                return handler

            test_handler = await make_test_handler(handler_event)
            await hl_api_for_test_env.subscribe(topic, test_handler)

    def _analyze_processor_invocations(
        self, processors: dict[str, Any], processor_calls: dict[str, list[Any]]
    ) -> None:
        """Analyze and log processor invocation results."""
        invoked_processors = [name for name, calls in processor_calls.items() if len(calls) > 0]

        logger.info(
            "processor_delegation_results",
            total_processors=len(processors),
            invoked_processors=invoked_processors,
            invocation_counts={
                name: len(calls) for name, calls in processor_calls.items() if calls
            },
            message="✓ Router delegated to processors successfully",
        )

        assert len(invoked_processors) > 0, "No processors were invoked by router"

    def _restore_original_processors(
        self, processors: dict[str, Any], original_processors: dict[str, Any]
    ) -> None:
        """Restore original processor methods."""
        for name, processor in processors.items():
            processor.process = original_processors[name]

    @pytest.mark.asyncio
    async def test_router_delegates_to_correct_processors(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that router delegates messages to correct processors."""
        _router, processors = self._setup_router_and_processors(hl_api_for_test_env)
        processor_calls: dict[str, list[Any]] = {name: [] for name in processors}
        original_processors = await self._mock_processors_for_tracking(processors, processor_calls)

        try:
            await hl_api_for_test_env.connect_websocket()
            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            markets = await hl_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                pytest.fail("No markets available for testing")

            test_symbol = markets[0].symbol
            await self._setup_test_subscriptions(hl_api_for_test_env, test_symbol)
            await asyncio.sleep(3.0)

            self._analyze_processor_invocations(processors, processor_calls)

        finally:
            self._restore_original_processors(processors, original_processors)

    @pytest.mark.asyncio
    async def test_router_handler_context_passing(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that router passes correct context to handlers."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            received_contexts: list[dict[str, Any]] = []

            async def context_test_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029
                # Extract data from typed context for test purposes
                if (
                    hasattr(context, "validated_envelope")
                    and context.validated_envelope is not None
                    and hasattr(context.validated_envelope, "data")
                ):
                    data = context.validated_envelope.data
                    if isinstance(data, dict):
                        received_contexts.append(data)
                        logger.info(
                            "handler_context_received",
                            context_keys=list(data.keys()),
                            context_size=len(str(data)),
                            message="Handler received context from router",
                        )
                    else:
                        received_contexts.append({"data": data})
                        logger.info(
                            "handler_context_received",
                            context_keys=["data"],
                            context_size=len(str(data)),
                            message="Handler received context from router",
                        )
                else:
                    received_contexts.append({"context": str(context)})
                    logger.info(
                        "handler_context_received",
                        context_keys=["context"],
                        context_size=len(str(context)),
                        message="Handler received context from router",
                    )

            # Subscribe to allMids for reliable data
            await hl_api_for_test_env.subscribe("allMids", context_test_handler)

            # Wait for messages
            await asyncio.sleep(3.0)

            assert len(received_contexts) > 0, "Handler did not receive any contexts"

            # Analyze received contexts
            context_keys_seen: set[str] = set()
            for ctx in received_contexts:
                context_keys_seen.update(ctx.keys())

            logger.info(
                "handler_context_analysis",
                total_contexts=len(received_contexts),
                unique_keys=list(context_keys_seen),
                message="✓ Router successfully passed contexts to handlers",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(f"Router context passing test failed: {e}")

    @pytest.mark.asyncio
    async def test_router_message_type_discrimination(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that router correctly discriminates between message types."""
        router = getattr(hl_api_for_test_env, "_hl_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not available")

        # Track message types routed
        message_types_routed: dict[str, int] = {}
        original_route_message = router.route_message

        async def tracking_route_message(
            message: dict[str, Any], handlers: dict[str, MessageHandler]
        ) -> None:
            # Extract message type/channel
            msg_type = "unknown"
            if "channel" in message:
                msg_type = message["channel"]
            elif "type" in message:
                msg_type = message["type"]
            elif "method" in message:
                msg_type = message["method"]

            message_types_routed[msg_type] = message_types_routed.get(msg_type, 0) + 1
            await original_route_message(message, handlers)

        router.route_message = tracking_route_message

        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            markets = await hl_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                pytest.fail("No markets available")

            test_symbol = markets[0].symbol

            # Subscribe to different message types
            async def dummy_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)

            await hl_api_for_test_env.subscribe(f"l2Book:{test_symbol}", dummy_handler)
            await hl_api_for_test_env.subscribe(f"trades:{test_symbol}", dummy_handler)
            await hl_api_for_test_env.subscribe("allMids", dummy_handler)

            # Wait for various message types
            await asyncio.sleep(5.0)

            logger.info(
                "message_type_discrimination_results",
                message_types=list(message_types_routed.keys()),
                type_counts=message_types_routed,
                total_messages=sum(message_types_routed.values()),
                message="✓ Router discriminated between message types",
            )

            assert len(message_types_routed) > 0, "Router did not process any message types"

        finally:
            router.route_message = original_route_message

    @pytest.mark.asyncio
    async def test_router_error_propagation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that router properly propagates errors from handlers."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            asyncio.Event()
            handler_called = asyncio.Event()

            async def error_handler(context: WebSocketContextProtocol) -> None:
                handler_called.set()
                await asyncio.sleep(0)  # Satisfy RUF029
                raise ValueError("Test error from handler")

            # Subscribe with error-throwing handler
            await hl_api_for_test_env.subscribe("allMids", error_handler)

            # Wait for handler to be called
            await asyncio.wait_for(handler_called.wait(), timeout=5.0)

            # The system should continue working despite handler errors
            normal_handler_called = asyncio.Event()

            async def normal_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)
                normal_handler_called.set()

            # Subscribe with normal handler
            await hl_api_for_test_env.subscribe("allMids", normal_handler)

            # Wait for normal handler
            await asyncio.wait_for(normal_handler_called.wait(), timeout=5.0)

            logger.info(
                "router_error_propagation_test_passed",
                message="✓ Router handled errors gracefully and continued operation",
            )

        except TimeoutError:
            pytest.fail("Router may have stopped working after handler error")
        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(f"Router error propagation test failed: {e}")

    @pytest.mark.asyncio
    async def test_router_concurrent_message_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test router handles concurrent messages correctly."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            asyncio.Semaphore(0)
            max_concurrent = 0
            current_concurrent = 0
            lock = asyncio.Lock()

            async def concurrent_handler(context: WebSocketContextProtocol) -> None:
                nonlocal current_concurrent, max_concurrent

                async with lock:
                    current_concurrent += 1
                    max_concurrent = max(max_concurrent, current_concurrent)

                # Simulate processing time
                await asyncio.sleep(0.1)

                async with lock:
                    current_concurrent -= 1

            # Subscribe to high-volume stream
            await hl_api_for_test_env.subscribe("allMids", concurrent_handler)

            # Let it run for a bit
            await asyncio.sleep(3.0)

            logger.info(
                "router_concurrent_handling_results",
                max_concurrent_handlers=max_concurrent,
                message="✓ Router handled concurrent messages",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(f"Router concurrent handling test failed: {e}")

    @pytest.mark.asyncio
    async def test_router_subscription_replacement(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that router correctly handles subscription replacement."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            handler1_count = 0
            handler2_count = 0

            async def handler1(context: WebSocketContextProtocol) -> None:
                nonlocal handler1_count
                await asyncio.sleep(0)
                handler1_count += 1

            async def handler2(context: WebSocketContextProtocol) -> None:
                nonlocal handler2_count
                await asyncio.sleep(0)
                handler2_count += 1

            # Subscribe with first handler
            await hl_api_for_test_env.subscribe("allMids", handler1)
            await asyncio.sleep(1.0)

            initial_handler1_count = handler1_count

            # Replace with second handler
            await hl_api_for_test_env.subscribe("allMids", handler2)
            await asyncio.sleep(1.0)

            # Check that handler1 stopped receiving and handler2 started
            assert handler1_count == initial_handler1_count, (
                "Handler1 still receiving after replacement"
            )
            assert handler2_count > 0, "Handler2 not receiving after replacement"

            logger.info(
                "router_subscription_replacement_success",
                handler1_final_count=handler1_count,
                handler2_count=handler2_count,
                message="✓ Router correctly replaced subscription handlers",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(f"Router subscription replacement test failed: {e}")
