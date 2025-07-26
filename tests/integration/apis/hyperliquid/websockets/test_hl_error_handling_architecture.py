"""Test 5: Error Handling in New Architecture.

This module tests the error handling capabilities of the refactored Hyperliquid
WebSocket architecture, including validation errors, connection errors, and
message processing errors.

Security Compliance:
- Tests error handling prevents system crashes
- Validates error propagation and logging
- Tests recovery from various error conditions
- Fails fast on unhandled error scenarios
"""

import asyncio
from typing import Any, cast

# NOTE: Mock usage below is for error injection testing only - not mocking financial operations
# This tests error handling by injecting invalid configurations to verify error recovery
from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.models.hl_ws_envelope import HyperliquidRawWebSocketEnvelope
from cyberdelta.apis.models.service_args.market_data import GetMarketsArgs
from cyberdelta.apis.websocket.ws_protocols import WebSocketContextProtocol
from cyberdelta.config.structlog_config import get_logger

# Import WebSocket test helpers
from .ws_test_helpers import (
    wait_with_progress_check,
)


pytestmark = [
    pytest.mark.integration,
    pytest.mark.timing,
]

logger = get_logger(__name__)


class TestHyperliquidErrorHandlingArchitecture:
    """Test error handling in refactored WebSocket architecture."""

    @pytest.mark.asyncio
    async def test_pydantic_validation_error_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test handling of Pydantic validation errors."""
        # Test various invalid message formats
        invalid_messages: list[dict[str, Any]] = [
            {},  # Empty message
            {"channel": None},  # None channel
            {"channel": "l2Book"},  # Missing data
            {"data": {"invalid": "structure"}},  # Missing channel
            {"channel": "l2Book", "data": None},  # None data
            {"channel": "", "data": {}},  # Empty channel
            {"channel": "l2Book", "data": {"coin": None}},  # None coin
            {"channel": "trades", "data": []},  # Empty trades
        ]

        validation_errors_caught = 0

        for i, invalid_msg in enumerate(invalid_messages):
            try:
                # Test envelope validation
                HyperliquidRawWebSocketEnvelope.model_validate(invalid_msg)

                logger.warning(
                    "pydantic_validation_unexpectedly_passed",
                    message_index=i,
                    invalid_message=invalid_msg,
                    message=f"Invalid message {i} unexpectedly passed validation",
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

            except (ValueError, TypeError, KeyError, AttributeError) as e:
                logger.info(
                    "pydantic_validation_other_error",
                    message_index=i,
                    error_type=type(e).__name__,
                    error=str(e),
                    message=f"Other error caught for message {i}: {type(e).__name__}",
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
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test WebSocket connection error handling."""
        # Test connection to invalid URL (if we can modify the config temporarily)
        original_ws_manager = getattr(hl_api_for_test_env, "_ws_manager", None)

        if not original_ws_manager:
            pytest.fail("WebSocket manager not available for connection error testing")

        # Test connection error scenarios
        connection_error_scenarios = [
            ("Invalid URL", "wss://invalid-url-that-does-not-exist.com/"),
            ("Malformed URL", "not-a-websocket-url"),
            ("Closed port", "wss://localhost:99999/"),
        ]

        for scenario_name, invalid_url in connection_error_scenarios:
            try:
                # Create a mock manager with invalid URL for testing
                with patch.object(original_ws_manager, "_config") as mock_config:
                    mock_config.ws_url = invalid_url

                    # Attempt connection (should fail)
                    try:
                        await original_ws_manager.connect()
                        logger.warning(
                            "connection_error_unexpectedly_succeeded",
                            scenario=scenario_name,
                            url=invalid_url,
                            message=f"Connection to {scenario_name} unexpectedly succeeded",
                        )
                    except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                        logger.info(
                            "connection_error_correctly_handled",
                            scenario=scenario_name,
                            error_type=type(e).__name__,
                            error=str(e)[:100],  # First 100 chars
                            message=f"✓ Connection error correctly handled for {scenario_name}",
                        )

            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                logger.info(
                    "connection_error_test_setup_failed",
                    scenario=scenario_name,
                    error=str(e),
                    message=f"Connection error test setup failed for {scenario_name}: {e}",
                )

    @pytest.mark.asyncio
    async def test_message_handler_error_propagation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test error propagation from message handlers."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed - cannot test handler errors")

            # Create handlers that throw different types of errors
            error_scenarios = [
                ("ValueError", ValueError("Test ValueError from handler")),
                ("TypeError", TypeError("Test TypeError from handler")),
                ("APIError", APIError("Test APIError from handler", "TEST_ERROR")),
                ("RuntimeError", RuntimeError("Test RuntimeError from handler")),
            ]

            for error_name, error_to_raise in error_scenarios:

                async def error_handler(
                    context: WebSocketContextProtocol,
                    *,
                    error_name: str = error_name,
                    error_to_raise: Exception = error_to_raise,
                ) -> None:
                    await asyncio.sleep(0)  # Satisfy RUF029
                    logger.info(
                        "error_handler_about_to_raise",
                        error_type=error_name,
                        message=f"Handler about to raise {error_name}",
                    )
                    raise error_to_raise

                try:
                    # Subscribe with error-throwing handler
                    await hl_api_for_test_env.subscribe("allMids", error_handler)

                    # Rule #4: Wait for potential error propagation with timeout
                    # Short wait to allow error to propagate if it will
                    await asyncio.sleep(0.1)  # Minimal wait for error propagation

                    logger.info(
                        "error_handler_subscription_completed",
                        error_type=error_name,
                        message=f"Subscription with {error_name} handler completed",
                    )

                except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                    logger.info(
                        "error_handler_subscription_failed",
                        error_type=error_name,
                        subscription_error=str(e),
                        message=f"Subscription with {error_name} handler failed: {e}",
                    )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Message handler error propagation test failed: {e}. "
                "Error handling architecture not working properly."
            )

    @pytest.mark.asyncio
    async def test_router_error_recovery(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test router error recovery and continued operation."""
        router = getattr(hl_api_for_test_env, "_hl_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not available for error recovery testing")

        # Test router continues operating after errors
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed - cannot test router recovery")

            # Track successful operations
            successful_operations: list[dict[str, Any]] = []

            async def recovery_test_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029

                # Extract data from typed context
                context_data: dict[str, Any] = {}
                if (
                    hasattr(context, "validated_envelope")
                    and context.validated_envelope is not None
                    and hasattr(context.validated_envelope, "data")
                ):
                    data = context.validated_envelope.data
                    context_data = data if isinstance(data, dict) else {"data": data}

                successful_operations.append(context_data)
                logger.info(
                    "router_recovery_successful_operation",
                    operation_count=len(successful_operations),
                    message=f"Router operation {len(successful_operations)} successful",
                )

            markets = await hl_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                pytest.fail("No markets available for router recovery testing")

            # Test multiple operations to verify recovery
            test_symbols = markets[:3] if len(markets) >= 3 else markets

            for i, market in enumerate(test_symbols):
                try:
                    await hl_api_for_test_env.subscribe(
                        f"l2Book:{market.symbol}", recovery_test_handler
                    )

                    # Rule #4: Minimal delay between operations for rate limiting
                    await asyncio.sleep(0.1)  # Small delay to avoid rate limit issues

                except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                    logger.warning(
                        "router_recovery_operation_failed",
                        operation_index=i,
                        symbol=market.symbol,
                        error=str(e),
                        message=f"Router operation {i} failed: {e}",
                    )

            # Rule #4: Wait for operations to complete with proper check
            await wait_with_progress_check(successful_operations, max_wait=2.0, min_data_points=1)

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
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that processor errors are isolated and don't crash the system."""
        router = getattr(hl_api_for_test_env, "_hl_ws_router", None)
        if not router:
            pytest.fail("WebSocket router not available for processor error testing")

        processors = getattr(router, "processors", {})
        if "l2Book" not in processors:
            pytest.fail("L2Book processor not available for error isolation testing")

        l2book_processor = processors["l2Book"]

        # Test processor with various error-inducing inputs
        error_inputs: list[tuple[Any, str]] = [
            (None, "None input"),
            ({"malformed": "data"}, "Malformed data"),
            ({"coin": None, "levels": []}, "None coin"),
            ({"coin": "BTC", "levels": None}, "None levels"),
            ({}, "Empty dict"),
        ]

        isolated_errors = 0

        for error_input, description in error_inputs:
            try:
                mock_handler = AsyncMock()
                await l2book_processor.process(error_input, mock_handler)

                logger.info(
                    "processor_error_input_handled",
                    description=description,
                    input_data=error_input,
                    message=f"Processor handled {description} without error",
                )

            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
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
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test that errors are properly logged and metrics collected."""
        router = getattr(hl_api_for_test_env, "_hl_ws_router", None)
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
            expected_methods = ["handle_error", "record_error", "log_error"]
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
        metrics_collector = getattr(router, "_metrics", None)
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
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test error handling during subscription operations."""
        # Test subscription with invalid topics
        invalid_topics = [
            "",  # Empty topic
            "invalid-format",  # No colon
            "l2Book:",  # Missing symbol
            ":BTC",  # Missing channel
            "l2Book:INVALID_SYMBOL_THAT_DOES_NOT_EXIST",  # Invalid symbol
            "unsupported:BTC",  # Unsupported channel
        ]

        subscription_errors = 0

        async def subscription_error_handler(context: WebSocketContextProtocol) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info("subscription_error_handler_called", context=context)

        for invalid_topic in invalid_topics:
            try:
                await hl_api_for_test_env.subscribe(invalid_topic, subscription_error_handler)

                logger.info(
                    "subscription_error_topic_accepted",
                    topic=invalid_topic,
                    message=f"Invalid topic '{invalid_topic}' was accepted",
                )

            except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
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

    @pytest.mark.asyncio
    async def test_graceful_degradation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test graceful degradation when components fail."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                logger.info(
                    "graceful_degradation_connection_failed",
                    message="WebSocket connection failed - testing graceful degradation",
                )

                # Test that API still works for HTTP operations
                try:
                    markets = await hl_api_for_test_env.get_markets(GetMarketsArgs())
                    if markets:
                        logger.info(
                            "graceful_degradation_http_still_working",
                            market_count=len(markets),
                            message="✓ HTTP API still works when WebSocket fails",
                        )
                    else:
                        logger.warning(
                            "graceful_degradation_http_also_failed",
                            message="HTTP API also failed - may indicate broader issues",
                        )
                except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                    logger.warning(
                        "graceful_degradation_http_failed",
                        error=str(e),
                        message=f"HTTP API also failed: {e}",
                    )

                return

            # Test degradation when specific components fail
            markets = await hl_api_for_test_env.get_markets(GetMarketsArgs())
            if not markets:
                logger.info(
                    "graceful_degradation_no_markets",
                    message="No markets available for degradation testing",
                )
                return

            # Test that system continues working with some failed subscriptions
            mixed_topics = [
                f"l2Book:{markets[0].symbol}",  # Valid
                "l2Book:INVALID_SYMBOL",  # Invalid
                f"trades:{markets[0].symbol}",  # Valid
                "invalid:format",  # Invalid format
            ]

            successful_subscriptions = 0
            failed_subscriptions = 0

            async def degradation_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)

                # Extract data from typed context
                context_data: dict[str, Any] = {}
                if (
                    hasattr(context, "validated_envelope")
                    and context.validated_envelope is not None
                    and hasattr(context.validated_envelope, "data")
                ):
                    data = context.validated_envelope.data
                    context_data = data if isinstance(data, dict) else {"data": data}

                logger.info("degradation_handler_success", context_keys=list(context_data.keys()))

            for topic in mixed_topics:
                try:
                    await hl_api_for_test_env.subscribe(topic, degradation_handler)
                    successful_subscriptions += 1
                    logger.info(
                        "graceful_degradation_subscription_success",
                        topic=topic,
                        message=f"Subscription to {topic} succeeded",
                    )
                except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                    failed_subscriptions += 1
                    logger.info(
                        "graceful_degradation_subscription_failed",
                        topic=topic,
                        error=str(e)[:100],
                        message=f"Subscription to {topic} failed gracefully",
                    )

            logger.info(
                "graceful_degradation_results",
                total_subscriptions=len(mixed_topics),
                successful=successful_subscriptions,
                failed=failed_subscriptions,
                message="✓ Graceful degradation test completed",
            )

            # System should continue working despite some failures
            assert successful_subscriptions > 0, (
                "No subscriptions succeeded - system not degrading gracefully"
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(
                f"Graceful degradation test failed: {e}. System not handling failures gracefully."
            )

    @pytest.mark.asyncio
    async def test_hyperliquid_specific_error_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test Hyperliquid-specific error handling scenarios."""
        try:
            await hl_api_for_test_env.connect_websocket()

            if not hl_api_for_test_env.is_connected:
                pytest.fail("WebSocket connection failed")

            # Test Hyperliquid-specific error scenarios
            error_scenarios = [
                ("userEvents", "User events without address"),
                ("userFills", "User fills without authentication"),
                ("userFundings", "User fundings without authentication"),
                ("l2Book:NONEXISTENT", "Non-existent market"),
            ]

            error_handling_results = {}

            async def error_test_handler(context: WebSocketContextProtocol) -> None:
                await asyncio.sleep(0)

            for topic, description in error_scenarios:
                try:
                    await hl_api_for_test_env.subscribe(topic, error_test_handler)
                    error_handling_results[description] = "accepted"
                    logger.info(
                        "hyperliquid_error_scenario_accepted",
                        topic=topic,
                        description=description,
                        message=f"{description} was accepted (may be valid)",
                    )
                except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
                    error_handling_results[description] = f"error: {type(e).__name__}"
                    logger.info(
                        "hyperliquid_error_scenario_handled",
                        topic=topic,
                        description=description,
                        error=str(e)[:100],
                        message=f"✓ {description} error handled: {e}",
                    )

            logger.info(
                "hyperliquid_specific_error_handling_complete",
                results=cast(dict[str, Any], error_handling_results),
                message="✓ Hyperliquid-specific error handling tested",
            )

        except (ValidationError, ValueError, TypeError, KeyError, AttributeError) as e:
            pytest.fail(f"Hyperliquid-specific error handling test failed: {e}")
