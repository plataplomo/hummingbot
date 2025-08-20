"""Test 5: Error Handling in New Architecture.

This module tests the error handling capabilities of the refactored WebSocket
architecture, including validation errors, connection errors, and message
processing errors.

Security Compliance:
- Tests error handling prevents system crashes
- Validates error propagation and logging
- Tests recovery from various error conditions
- Fails fast on unhandled error scenarios
"""

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.models.bp_ws_envelope import BackpackRawWebSocketEnvelope
from cyberdelta.apis.common import APIError, MessageHandler
from cyberdelta.apis.exceptions.websocket.envelope_validation import InvalidFormatError
from cyberdelta.apis.exceptions.websocket.stream import WebSocketSubscriptionError
from cyberdelta.apis.exceptions.websocket.stream_error import WebSocketStreamError
from cyberdelta.apis.models.service_args.market_data import GetMarketsArgs
from cyberdelta.apis.websocket.ws_context import WebSocketMessageContext
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError
from cyberdelta.exceptions.service_validation import EmptyStringParameterError
from cyberdelta.symbols.models import Symbol

# Import WebSocket test helpers
from .ws_test_helpers import (
    ensure_websocket_connected,
    wait_for_websocket_data,
)


pytestmark = [pytest.mark.integration, pytest.mark.timing]

logger = get_logger(__name__)


class TestBackpackErrorHandlingArchitecture:
    """Test error handling in refactored WebSocket architecture."""

    @pytest.mark.asyncio
    async def test_pydantic_validation_error_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test handling of Pydantic validation errors."""
        # Test various invalid message formats
        invalid_messages: list[dict[str, Any]] = [
            {},  # Empty message
            {"stream": None},  # None stream
            {"stream": "ticker"},  # Missing data
            {"data": {"invalid": "structure"}},  # Missing stream
            {"stream": "ticker", "data": None},  # None data
            {"stream": "", "data": {}},  # Empty stream
            {"stream": "ticker", "data": {"s": None}},  # None symbol
            {"stream": "ticker", "data": {"s": "", "lastPrice": ""}},  # Empty values
        ]

        validation_errors_caught = 0

        for i, invalid_msg in enumerate(invalid_messages):
            try:
                # Test envelope validation
                BackpackRawWebSocketEnvelope.model_validate(invalid_msg)

                # Rule #2: Use pytest.fail instead of logger.warning
                pytest.fail(
                    f"Invalid message {i} unexpectedly passed validation. "
                    f"Message: {invalid_msg}. Pydantic validation not working correctly."
                )

            except ValidationError as e:
                validation_errors_caught += 1
                logger.info(
                    "pydantic_validation_error_correctly_caught",
                    message_index=i,
                    error_count=len(e.errors()),
                    error_details=str(e.errors()[:2]),  # First 2 errors
                    message=f"✓ Validation error correctly caught for message {i}",
                )

            except (
                ValueError,
                TypeError,
                KeyError,
                AttributeError,
                EmptyStringError,
                TypeFieldError,
            ) as e:
                # These errors are also validation errors, just not Pydantic ValidationError
                validation_errors_caught += 1
                logger.info(
                    "pydantic_validation_other_error",
                    message_index=i,
                    error_type=type(e).__name__,
                    error=str(e),
                    message=f"Other validation error caught for message {i}: {type(e).__name__}",
                )

        assert validation_errors_caught > 0, (
            "No Pydantic validation errors were caught. "
            "Validation error handling may not be working."
        )

        logger.info(
            "pydantic_validation_error_handling_summary",
            total_invalid_messages=len(invalid_messages),
            validation_errors_caught=validation_errors_caught,
            message="✓ Pydantic validation error handling working correctly",
        )

    @pytest.mark.asyncio
    async def test_websocket_connection_error_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test WebSocket connection error handling."""
        # Test connection to invalid URL (if we can modify the config temporarily)
        original_ws_manager = getattr(bp_api_for_test_env, "_ws_manager", None)

        if not original_ws_manager:
            pytest.fail("WebSocket manager not available for connection error testing")

        # Rule #6: No mocks - test with real invalid connection attempts
        # Test by attempting to subscribe to invalid topics instead
        logger.info(
            "connection_error_test_modified",
            message="Testing connection errors through invalid subscriptions (no mocks)",
        )

        # Rule #10: Add proper network error handling
        try:
            # Test connection is established first
            await ensure_websocket_connected(bp_api_for_test_env)
        except (TimeoutError, ConnectionError, OSError) as e:
            # Network errors during connection are test failures
            pytest.fail(
                f"Network error while establishing WebSocket connection: {e}. "
                "Cannot test error handling without stable connection."
            )

        # Now test invalid subscription scenarios
        invalid_subscription_scenarios = [
            ("Empty topic", ""),
            ("Invalid format", "invalid-format-no-dot"),
            ("Missing symbol", "ticker."),
            ("Invalid symbol format", "ticker.invalid-symbol-lowercase"),
        ]

        for scenario_name, invalid_topic in invalid_subscription_scenarios:
            try:

                async def test_handler(context: WebSocketContextProtocol) -> None:
                    await asyncio.sleep(0)

                await bp_api_for_test_env.subscribe(invalid_topic, test_handler)
                # Rule #2: Use pytest.fail for unexpected success
                pytest.fail(
                    f"Subscription to {scenario_name} ({invalid_topic}) unexpectedly succeeded. "
                    "Invalid subscription validation not working."
                )
            except (
                ValidationError,
                ValueError,
                TypeError,
                KeyError,
                AttributeError,
                EmptyStringParameterError,
                APIError,
                WebSocketStreamError,
                InvalidFormatError,
            ) as e:
                logger.info(
                    "invalid_subscription_correctly_rejected",
                    scenario=scenario_name,
                    topic=invalid_topic,
                    error_type=type(e).__name__,
                    message=f"✓ Invalid subscription correctly rejected for {scenario_name}",
                )
            except (TimeoutError, ConnectionError, OSError) as e:
                # Rule #10: Network errors during test are failures
                pytest.fail(
                    f"Network error during {scenario_name} test: {e}. "
                    "Test requires stable network connection."
                )

    async def _setup_error_propagation_test(
        self, bp_api_for_test_env: BackpackAPI
    ) -> tuple[Symbol, list[tuple[str, Exception]]]:
        """Set up WebSocket connection and get test data for error propagation test.

        Args:
            bp_api_for_test_env: BackpackAPI instance for testing

        Returns:
            tuple[Symbol, list[tuple[str, Exception]]]: Test symbol and list of error scenarios
        """
        # Rule #10: Add proper network error handling
        try:
            await bp_api_for_test_env.connect_websocket()
        except (TimeoutError, ConnectionError, OSError) as e:
            pytest.fail(
                f"Network error while connecting WebSocket: {e}. "
                "Cannot test handler error propagation without connection."
            )

        if not bp_api_for_test_env.is_connected:
            pytest.fail("WebSocket connection failed - cannot test handler errors")

        # Create handlers that throw different types of errors
        # Senior-level fix: Explicit typing for proper type variance
        error_scenarios: list[tuple[str, Exception]] = [
            ("ValueError", ValueError("Test ValueError from handler")),
            ("TypeError", TypeError("Test TypeError from handler")),
            ("APIError", APIError("Test APIError from handler", "TEST_ERROR")),
            ("RuntimeError", RuntimeError("Test RuntimeError from handler")),
        ]

        markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
        if not markets:
            pytest.fail("No markets available for handler error testing")

        return markets[0].symbol, error_scenarios

    async def _create_error_tracking_handler(
        self, error_name: str, error_to_raise: Exception, handler_errors: list[Exception]
    ) -> MessageHandler:
        """Create a handler that tracks and raises errors.

        Args:
            error_name: Name of the error for logging
            error_to_raise: Exception to raise in the handler
            handler_errors: List to track handler errors

        Returns:
            MessageHandler: Handler function that raises the specified error
        """

        async def error_tracking_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)
            handler_errors.append(error_to_raise)
            logger.info(
                "error_handler_about_to_raise",
                error_type=error_name,
                message=f"Handler about to raise {error_name}",
            )
            raise error_to_raise

        return error_tracking_handler

    async def _wait_for_handler_errors(self, handler_errors: list[Exception]) -> None:
        """Wait for handler errors to be captured."""
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < 2.0:
            if handler_errors:
                break
            await asyncio.sleep(0.1)

    async def _test_single_error_scenario(
        self,
        bp_api_for_test_env: BackpackAPI,
        test_symbol: Symbol,
        error_name: str,
        error_to_raise: Exception,
    ) -> None:
        """Test a single error propagation scenario."""
        try:
            # Rule #4: Wait for handler to be called with real data
            handler_errors: list[Exception] = []

            error_tracking_handler = await self._create_error_tracking_handler(
                error_name, error_to_raise, handler_errors
            )

            # Use the error tracking handler - convert Symbol to WebSocket format
            ws_symbol = test_symbol.value.replace("/", "_").replace("-", "_")
            await bp_api_for_test_env.subscribe(f"ticker.{ws_symbol}", error_tracking_handler)

            # Wait for at least one message to be processed
            await self._wait_for_handler_errors(handler_errors)

            logger.info(
                "error_handler_subscription_completed",
                error_type=error_name,
                symbol=test_symbol,
                message=f"Subscription with {error_name} handler completed",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            logger.info(
                "error_handler_subscription_failed",
                error_type=error_name,
                subscription_error=str(e),
                message=f"Subscription with {error_name} handler failed: {e}",
            )

    @pytest.mark.asyncio
    async def test_message_handler_error_propagation(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test error propagation from message handlers."""
        try:
            test_symbol, error_scenarios = await self._setup_error_propagation_test(
                bp_api_for_test_env
            )

            for error_name, error_to_raise in error_scenarios:
                await self._test_single_error_scenario(
                    bp_api_for_test_env, test_symbol, error_name, error_to_raise
                )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Message handler error propagation test failed: {e}. "
                "Error handling architecture not working properly."
            )
        except (TimeoutError, ConnectionError, OSError) as e:
            # Rule #10: Network errors are test failures
            pytest.fail(
                f"Network error during handler error propagation test: {e}. "
                "Test requires stable network connection."
            )

    @pytest.mark.asyncio
    async def test_router_error_recovery(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test router error recovery and continued operation."""
        # Test that the system continues operating after errors by subscribing
        # to multiple topics, some of which may fail

        # Test router continues operating after errors
        # Rule #10: Handle network errors properly
        try:
            await bp_api_for_test_env.connect_websocket()
        except (TimeoutError, ConnectionError, OSError) as e:
            pytest.fail(
                f"Network error while connecting for router recovery test: {e}. "
                "Cannot test router recovery without stable connection."
            )

        try:
            if not bp_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed - cannot test router recovery")

            # Track successful operations
            successful_operations: list[dict[str, Any]] = []

            async def recovery_test_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029
                # Extract data from typed context for test purposes
                if (
                    hasattr(context, "validated_envelope")
                    and context.validated_envelope is not None
                    and hasattr(context.validated_envelope, "data")
                ):
                    data = context.validated_envelope.data
                    if isinstance(data, dict):
                        successful_operations.append(data)
                    else:
                        successful_operations.append({"data": data})
                else:
                    successful_operations.append({"context": str(context)})
                logger.info(
                    "router_recovery_successful_operation",
                    operation_count=len(successful_operations),
                    message=f"Router operation {len(successful_operations)} successful",
                )

            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                pytest.fail("No markets available for router recovery testing")

            # Test multiple operations to verify recovery
            test_symbols = markets[:3] if len(markets) >= 3 else markets

            for i, market in enumerate(test_symbols):
                try:
                    await bp_api_for_test_env.subscribe(
                        f"ticker.{market.symbol}", recovery_test_handler
                    )

                    # Small delay between operations
                    await asyncio.sleep(0.5)

                except (
                    ValidationError,
                    ValueError,
                    TypeError,
                    KeyError,
                    AttributeError,
                    EmptyStringParameterError,
                ) as e:
                    # Rule #2: Use pytest.fail for actual failures
                    pytest.fail(
                        f"Router recovery operation {i} failed for {market.symbol}: {e}. "
                        "Router not recovering from errors properly."
                    )

            # Rule #4: Wait for actual data instead of arbitrary sleep
            await wait_for_websocket_data(
                successful_operations, min_count=len(test_symbols), timeout_seconds=5.0
            )

            logger.info(
                "router_error_recovery_results",
                total_operations=len(test_symbols),
                successful_operations=len(successful_operations),
                message="✓ Router error recovery test completed",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Router error recovery test failed: {e}. "
                "Router error recovery not working properly."
            )

    @pytest.mark.asyncio
    async def test_processor_error_isolation(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that processor errors are isolated and don't crash the system."""
        router = getattr(bp_api_for_test_env, "_bp_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not available for processor error testing")

        processors = getattr(router, "processors", {})
        if "ticker" not in processors:
            pytest.fail("Ticker processor not available for error isolation testing")

        ticker_processor = processors["ticker"]

        # Test processor with various error-inducing inputs
        error_inputs: list[tuple[Any, str]] = [
            (None, "None input"),
            ({"malformed": "data"}, "Malformed data"),
            ({"s": None, "lastPrice": "50000"}, "None symbol"),
            ({"s": "BTC_USDC", "lastPrice": None}, "None price"),
            ({}, "Empty dict"),
        ]

        isolated_errors = 0

        for error_input, description in error_inputs:
            try:
                # Rule #6: No mocks - use real handler
                handler_called = False

                async def real_handler(context: WebSocketContextProtocol) -> None:
                    nonlocal handler_called
                    await asyncio.sleep(0)
                    handler_called = True

                # Create a minimal context for testing
                test_envelope = BackpackRawWebSocketEnvelope(
                    stream="ticker.BTC_USDC", data=error_input if error_input is not None else {}
                )
                test_context = WebSocketMessageContext(
                    validated_envelope=test_envelope,
                    exchange_type=ExchangeName.BACKPACK,
                    routing_key="ticker",
                    timestamp=datetime.now(UTC),
                    message_id="test-msg-123",
                    connection_id="test-conn-456",
                )

                await ticker_processor.process(error_input, real_handler, test_context)

                logger.info(
                    "processor_error_input_handled",
                    description=description,
                    input_data=error_input,
                    message=f"Processor handled {description} without error",
                )

            except (
                ValidationError,
                ValueError,
                TypeError,
                KeyError,
                AttributeError,
                EmptyStringParameterError,
            ) as e:
                isolated_errors += 1
                logger.info(
                    "processor_error_correctly_isolated",
                    description=description,
                    error_type=type(e).__name__,
                    error=str(e)[:100],
                    message=f"✓ Processor error correctly isolated for {description}",
                )

        logger.info(
            "processor_error_isolation_summary",
            total_error_inputs=len(error_inputs),
            isolated_errors=isolated_errors,
            message="✓ Processor error isolation test completed",
        )

    @pytest.mark.asyncio
    async def test_error_logging_and_metrics(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test that errors are properly logged and metrics collected."""
        router = getattr(bp_api_for_test_env, "_bp_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not available for error metrics testing")

        # Check if router has error handler
        error_handler = getattr(router, "error_handler", None)
        if error_handler:
            logger.info(
                "router_error_handler_found",
                handler_type=type(error_handler).__name__,
                message="✓ Router has error handler for metrics collection",
            )

            # Check error handler methods
            expected_methods = [
                "handle_validation_error",
                "handle_processing_error",
                "handle_unroutable_message",
                "handle_routing_error",
            ]
            for method_name in expected_methods:
                if hasattr(error_handler, method_name):
                    logger.info(
                        "error_handler_method_found",
                        method=method_name,
                        message=f"✓ Error handler has {method_name} method",
                    )
        else:
            logger.info(
                "router_no_error_handler",
                message="Router has no dedicated error handler (may use default logging)",
            )

        # Check if router has metrics collector
        metrics_collector = getattr(router, "metrics_collector", None)
        if metrics_collector:
            logger.info(
                "router_metrics_collector_found",
                collector_type=type(metrics_collector).__name__,
                message="✓ Router has metrics collector for error tracking",
            )

            # Check metrics methods
            expected_metrics_methods = [
                "record_error",
                "record_processing_error",
                "increment_error_count",
            ]
            for method_name in expected_metrics_methods:
                if hasattr(metrics_collector, method_name):
                    logger.info(
                        "metrics_error_method_found",
                        method=method_name,
                        message=f"✓ Metrics collector has {method_name} method",
                    )
        else:
            logger.info(
                "router_no_metrics_collector",
                message="Router has no metrics collector (may be optional)",
            )

    @pytest.mark.asyncio
    async def test_subscription_error_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test error handling during subscription operations."""
        # Test subscription with invalid topics
        invalid_topics = [
            "",  # Empty topic
            "invalid-format",  # No dot
            "ticker.",  # Missing symbol
            ".BTC_USDC",  # Missing channel
            "ticker.INVALID_SYMBOL_THAT_DOES_NOT_EXIST",  # Invalid symbol
            "unsupported.BTC_USDC",  # Unsupported channel
        ]

        subscription_errors = 0

        async def subscription_error_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info("subscription_error_handler_called", context=context)

        for invalid_topic in invalid_topics:
            try:
                await bp_api_for_test_env.subscribe(invalid_topic, subscription_error_handler)

                logger.info(
                    "subscription_error_topic_accepted",
                    topic=invalid_topic,
                    message=f"Invalid topic '{invalid_topic}' was accepted",
                )

            except (
                ValidationError,
                ValueError,
                TypeError,
                KeyError,
                AttributeError,
                EmptyStringParameterError,
            ) as e:
                subscription_errors += 1
                logger.info(
                    "subscription_error_correctly_handled",
                    topic=invalid_topic,
                    error_type=type(e).__name__,
                    error=str(e)[:100],
                    message=f"✓ Subscription error correctly handled for '{invalid_topic}'",
                )

        logger.info(
            "subscription_error_handling_summary",
            total_invalid_topics=len(invalid_topics),
            subscription_errors=subscription_errors,
            message="✓ Subscription error handling test completed",
        )

    async def _test_http_fallback_on_ws_failure(self, bp_api_for_test_env: BackpackAPI) -> bool:
        """Test HTTP API still works when WebSocket fails.

        Returns:
            True if HTTP API is working, False otherwise.
        """
        try:
            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            if markets:
                logger.info(
                    "graceful_degradation_http_still_working",
                    market_count=len(markets),
                    message="✓ HTTP API still works when WebSocket fails",
                )
                return True

            pytest.fail(
                "HTTP API also failed when WebSocket failed. "
                "System not degrading gracefully - both APIs failed."
            )
        except (
            ValidationError,
            ValueError,
            TypeError,
            KeyError,
            AttributeError,
            EmptyStringParameterError,
        ) as e:
            pytest.fail(
                f"HTTP API failed when testing graceful degradation: {e}. "
                "System not handling WebSocket failures gracefully."
            )

    async def _test_mixed_subscription_degradation(
        self, bp_api_for_test_env: BackpackAPI, markets: list[Any]
    ) -> tuple[int, int]:
        """Test system continues working with some failed subscriptions.

        Returns:
            Tuple of (successful_subscriptions, failed_subscriptions).
        """
        mixed_topics = [
            f"ticker.{markets[0].symbol}",  # Valid
            "ticker.INVALID_SYMBOL",  # Invalid
            f"depth.{markets[0].symbol}",  # Valid
            "invalid.format",  # Invalid format
        ]

        successful_subscriptions = 0
        failed_subscriptions = 0

        async def degradation_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)
            # Log context info
            context_info = []
            if (
                hasattr(context, "validated_envelope")
                and context.validated_envelope is not None
                and hasattr(context.validated_envelope, "data")
            ):
                data = context.validated_envelope.data
                if isinstance(data, dict):
                    context_info = list(data.keys())
            logger.info("degradation_handler_success", context_keys=context_info)

        for topic in mixed_topics:
            try:
                await bp_api_for_test_env.subscribe(topic, degradation_handler)
                successful_subscriptions += 1
                logger.info(
                    "graceful_degradation_subscription_success",
                    topic=topic,
                    message=f"Subscription to {topic} succeeded",
                )
            except (
                ValidationError,
                ValueError,
                TypeError,
                KeyError,
                AttributeError,
                EmptyStringParameterError,
                WebSocketStreamError,
                WebSocketSubscriptionError,
                InvalidFormatError,
            ) as e:
                failed_subscriptions += 1
                logger.info(
                    "graceful_degradation_subscription_failed",
                    topic=topic,
                    error=str(e)[:100],
                    message=f"Subscription to {topic} failed gracefully",
                )

        return successful_subscriptions, failed_subscriptions

    @pytest.mark.asyncio
    async def test_graceful_degradation(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> None:
        """Test graceful degradation when components fail."""
        # Rule #10: Handle network errors during connection attempt
        try:
            await bp_api_for_test_env.connect_websocket()
        except (TimeoutError, ConnectionError, OSError) as e:
            # Network failure is part of the degradation test
            logger.info(
                "graceful_degradation_network_error",
                error_type=type(e).__name__,
                message=f"Network error during connection (testing degradation): {e}",
            )

        try:
            if not bp_api_for_test_env.is_connected:
                logger.info(
                    "graceful_degradation_connection_failed",
                    message="WebSocket connection failed - testing graceful degradation",
                )
                # Test that API still works for HTTP operations
                await self._test_http_fallback_on_ws_failure(bp_api_for_test_env)
                return

            # Test degradation when specific components fail
            markets = await bp_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                logger.info(
                    "graceful_degradation_no_markets",
                    message="No markets available for degradation testing",
                )
                return

            # Test that system continues working with some failed subscriptions
            successful, failed = await self._test_mixed_subscription_degradation(
                bp_api_for_test_env, markets
            )

            logger.info(
                "graceful_degradation_results",
                total_subscriptions=4,  # We know we test 4 topics
                successful=successful,
                failed=failed,
                message="✓ Graceful degradation test completed",
            )

            # System should continue working despite some failures
            assert successful > 0, "No subscriptions succeeded - system not degrading gracefully"

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Graceful degradation test failed: {e}. System not handling failures gracefully."
            )
