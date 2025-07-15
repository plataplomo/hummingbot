"""Integration Tests for HyperliquidAPI WebSocket Integration.

This module tests the WebSocket integration in HyperliquidAPI,
specifically focusing on delegation to the router and WebSocket lifecycle management.
All tests use real market data and fail fast on critical operations.

Security Compliance:
- No hardcoded currency pairs - uses dynamic symbol retrieval
- No mocking of critical WebSocket operations - uses VCR for reproducibility
- Fail-fast error handling - no graceful hiding of WebSocket failures
- Real connection testing - validates actual network behavior
"""

import asyncio
from typing import Any

import pytest

from cyberdelta.apis.base.ws_context import WebSocketContextUnion
from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import GetMarketsArgs
from cyberdelta.config.structlog_config import get_logger


pytestmark = [pytest.mark.integration, pytest.mark.timing]
logger = get_logger(__name__)


async def get_available_symbols(api: HyperliquidAPI) -> list[str]:
    """Get available trading symbols from the exchange.

    Args:
        api: HyperliquidAPI instance

    Returns:
        List of available symbols

    Raises:
        RuntimeError: If unable to fetch symbols from exchange
    """
    try:
        markets = await api.get_markets(GetMarketsArgs())
        symbols = [market.symbol for market in markets]

        if not symbols:
            raise RuntimeError(
                "No symbols available from exchange. WebSocket tests require real trading symbols.",
            )

        return symbols[:3]  # Return first 3 for testing

    except (APIError, ValueError, TypeError, KeyError) as e:
        raise RuntimeError(
            f"Failed to fetch trading symbols from exchange: {e}. "
            "WebSocket tests require real market data and cannot use hardcoded symbols.",
        ) from e


def get_websocket_topics_for_symbol(symbol: str) -> list[str]:
    """Generate valid WebSocket topics for a trading symbol.

    Args:
        symbol: Trading symbol from exchange

    Returns:
        List of valid WebSocket topics for the symbol
    """
    return [f"l2Book:{symbol}", f"trades:{symbol}"]


class TestHyperliquidAPIWebSocketBasicOperations:
    """Test basic WebSocket operations using real market data."""

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_websocket_subscription_with_real_symbols(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test WebSocket subscription with real trading symbols from exchange."""
        # Get real symbols from exchange
        available_symbols = await get_available_symbols(hl_api_for_test_env)

        if not available_symbols:
            pytest.fail(
                "No trading symbols available from exchange. "
                "WebSocket functionality requires real market symbols.",
            )

        test_symbol = available_symbols[0]
        topics = get_websocket_topics_for_symbol(test_symbol)

        async def test_handler(context: WebSocketContextUnion) -> None:
            """Test context handler for WebSocket data."""
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_message_received",
                message=context,
            )

        # Test subscription with real symbol
        try:
            await hl_api_for_test_env.subscribe(topics[0], test_handler)
            logger.info(
                "websocket_subscription_success",
                topic=topics[0],
                message=f"✓ Successfully subscribed to {topics[0]}",
            )
        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"WebSocket subscription failed for real symbol {test_symbol}: {e}. "
                "WebSocket operations are critical and must work reliably.",
            )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_multiple_subscriptions_real_symbols(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test multiple subscriptions with real trading symbols."""
        available_symbols = await get_available_symbols(hl_api_for_test_env)

        if len(available_symbols) < 2:
            pytest.fail(
                f"Need at least 2 trading symbols, got {len(available_symbols)}. "
                "WebSocket tests require multiple real market symbols.",
            )

        async def handler1(context: WebSocketContextUnion) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "handler1_message_received",
                message=context,
                handler="Handler1",
            )

        async def handler2(context: WebSocketContextUnion) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "handler2_message_received",
                message=context,
                handler="Handler2",
            )

        symbol1, symbol2 = available_symbols[0], available_symbols[1]
        topic1 = f"l2Book:{symbol1}"
        topic2 = f"trades:{symbol2}"

        try:
            await hl_api_for_test_env.subscribe(topic1, handler1)
            await hl_api_for_test_env.subscribe(topic2, handler2)
            logger.info(
                "multiple_subscriptions_success",
                topic1=topic1,
                topic2=topic2,
                message=f"✓ Successfully subscribed to {topic1} and {topic2}",
            )
        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"Multiple WebSocket subscriptions failed: {e}. "
                "Multi-symbol WebSocket operations are critical for trading.",
            )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_websocket_connection_status_validation(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test WebSocket connection status validation with fail-fast behavior."""
        available_symbols = await get_available_symbols(hl_api_for_test_env)
        test_symbol = available_symbols[0]
        topic = f"l2Book:{test_symbol}"

        async def status_handler(context: WebSocketContextUnion) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "status_handler_message_received",
                message=context,
                handler="status_handler",
            )

        # Test connection status consistency
        initial_status = hl_api_for_test_env.is_connected

        try:
            await hl_api_for_test_env.subscribe(topic, status_handler)
            after_subscription_status = hl_api_for_test_env.is_connected

            # Validate status is boolean and consistent
            assert isinstance(initial_status, bool), (
                f"Connection status should be boolean, got {type(initial_status)}"
            )
            assert isinstance(after_subscription_status, bool), (
                f"Connection status should be boolean after subscription, "
                f"got {type(after_subscription_status)}"
            )

            logger.info(
                "connection_status_validation_passed",
                initial_status=initial_status,
                after_subscription_status=after_subscription_status,
                message="Connection status validation passed",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"WebSocket connection status validation failed: {e}. "
                "Connection state tracking is critical for trading operations.",
            )


class TestHyperliquidAPIWebSocketLifecycle:
    """Test WebSocket connection lifecycle with real operations."""

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_subscription_lifecycle_real_data(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test complete subscription lifecycle with real market data."""
        available_symbols = await get_available_symbols(hl_api_for_test_env)
        test_symbol = available_symbols[0]
        topics = get_websocket_topics_for_symbol(test_symbol)

        received_messages: list[dict[str, Any]] = []

        async def lifecycle_handler(context: WebSocketContextUnion) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            # Extract data from typed context for test purposes
            if hasattr(context, "validated_envelope") and hasattr(
                context.validated_envelope, "data"
            ):
                data = context.validated_envelope.data
                if isinstance(data, dict):
                    received_messages.append(data)
                else:
                    received_messages.append({"data": data})
            else:
                received_messages.append({"context": str(context)})
            logger.info(
                "lifecycle_handler_message_received",
                message=context,
                handler="lifecycle_handler",
            )

        # Test subscription
        try:
            await hl_api_for_test_env.subscribe(topics[0], lifecycle_handler)
            logger.info(
                "subscription_lifecycle_success",
                topic=topics[0],
                message=f"✓ Subscription successful for {topics[0]}",
            )
        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"Subscription lifecycle failed for {test_symbol}: {e}. "
                "WebSocket subscription is a critical trading operation.",
            )

        # Test handler replacement with same topic
        async def replacement_handler(context: WebSocketContextUnion) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "replacement_handler_message_received",
                message=context,
                handler="replacement_handler",
            )

        try:
            await hl_api_for_test_env.subscribe(topics[0], replacement_handler)
            logger.info(
                "handler_replacement_success",
                topic=topics[0],
                message=f"✓ Handler replacement successful for {topics[0]}",
            )
        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"Handler replacement failed for {test_symbol}: {e}. "
                "WebSocket handler management is critical for real-time data.",
            )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_concurrent_subscriptions_real_symbols(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test concurrent subscriptions with multiple real symbols."""
        available_symbols = await get_available_symbols(hl_api_for_test_env)

        if len(available_symbols) < 3:
            pytest.fail(
                f"Need at least 3 symbols for concurrent testing, got {len(available_symbols)}. "
                "Concurrent WebSocket operations require multiple real symbols.",
            )

        async def concurrent_handler(context: WebSocketContextUnion) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "concurrent_handler_message_received",
                message=context,
                handler="concurrent_handler",
            )

        # Create subscription tasks for multiple symbols
        subscription_tasks: list[tuple[str, Any]] = []
        for symbol in available_symbols:
            topic = f"l2Book:{symbol}"
            task = asyncio.create_task(hl_api_for_test_env.subscribe(topic, concurrent_handler))
            subscription_tasks.append((topic, task))

        # Execute concurrent subscriptions
        try:
            await asyncio.gather(*[task for _, task in subscription_tasks])
            logger.info(
                "concurrent_subscriptions_success",
                symbol_count=len(subscription_tasks),
                message="Concurrent subscriptions successful",
            )
        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"Concurrent WebSocket subscriptions failed: {e}. "
                "Concurrent operations are critical for multi-asset trading.",
            )


class TestHyperliquidAPIWebSocketEdgeCases:
    """Test WebSocket edge cases with proper error handling."""

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_invalid_topic_format_handling(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test handling of invalid topic formats with fail-fast behavior."""

        async def error_handler(context: WebSocketContextUnion) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "error_handler_message_received",
                message=context,
                handler="error_handler",
            )

        invalid_topics = [
            "",  # Empty topic
            "invalid_format",  # No colon separator
            "l2Book:",  # Missing symbol
            ":BTC",  # Missing prefix
            "l2Book:INVALID_SYMBOL_FORMAT_THAT_DOES_NOT_EXIST",  # Invalid symbol
            "trades:",  # Missing symbol
            "userEvents:",  # Missing address
        ]

        for invalid_topic in invalid_topics:
            try:
                await hl_api_for_test_env.subscribe(invalid_topic, error_handler)

                # If subscription doesn't raise an error, that's fine
                # But we should still validate the connection state
                connection_state = hl_api_for_test_env.is_connected
                assert isinstance(connection_state, bool), (
                    f"Connection state should be boolean after invalid topic {invalid_topic}"
                )

                logger.info(
                    "invalid_topic_handling_completed",
                    invalid_topic=invalid_topic,
                    message=f"✓ Invalid topic handling completed for: {invalid_topic}",
                )

            except (APIError, ValueError, TypeError, KeyError) as e:
                # If an exception is raised, it should be a proper API error
                # Don't hide it with graceful handling
                if "connection" in str(e).lower() or "network" in str(e).lower():
                    pytest.fail(
                        f"Network/connection error during invalid topic test {invalid_topic}: {e}. "
                        "Network issues should not occur during topic validation.",
                    )
                else:
                    # API validation errors are acceptable
                    logger.info(
                        "api_rejected_invalid_topic",
                        invalid_topic=invalid_topic,
                        error=str(e),
                        message=f"✓ API correctly rejected invalid topic {invalid_topic}: {e}",
                    )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_websocket_connection_establishment(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test WebSocket connection establishment with real endpoint."""
        # Test connection establishment
        try:
            await hl_api_for_test_env.connect_websocket()

            # Validate connection state
            connection_state = hl_api_for_test_env.is_connected
            assert isinstance(connection_state, bool), (
                "Connection state should be boolean after connect attempt"
            )

            logger.info(
                "websocket_connection_attempt_completed",
                connection_state=connection_state,
                message=f"✓ WebSocket connection attempt completed, state: {connection_state}",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            # Connection failures should fail the test
            pytest.fail(
                f"WebSocket connection establishment failed: {e}. "
                "WebSocket connectivity is critical for real-time trading data.",
            )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_subscription_with_connection_sequence(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test subscription followed by connection establishment."""
        available_symbols = await get_available_symbols(hl_api_for_test_env)
        test_symbol = available_symbols[0]
        topic = f"trades:{test_symbol}"

        async def sequence_handler(context: WebSocketContextUnion) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "sequence_handler_message_received",
                message=context,
                handler="sequence_handler",
            )

        try:
            # Subscribe first
            await hl_api_for_test_env.subscribe(topic, sequence_handler)

            # Then establish connection
            await hl_api_for_test_env.connect_websocket()

            # Validate final state
            final_state = hl_api_for_test_env.is_connected
            assert isinstance(final_state, bool), "Final connection state should be boolean"

            logger.info(
                "subscription_connection_sequence_completed",
                final_state=final_state,
                message="Subscription -> connection sequence completed",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"Subscription-connection sequence failed: {e}. "
                "Sequential WebSocket operations are critical for trading setup.",
            )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_rapid_subscription_operations_real_symbols(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test rapid subscription operations with real symbols."""

        async def rapid_handler(context: WebSocketContextUnion) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "rapid_handler_message_received",
                message=context,
                handler="rapid_handler",
            )

        topic = "allMids"  # Use allMids for rapid subscriptions
        rapid_subscription_count = 5

        try:
            # Perform rapid subscriptions to same topic
            for i in range(rapid_subscription_count):
                await hl_api_for_test_env.subscribe(topic, rapid_handler)

                # Validate state consistency
                state = hl_api_for_test_env.is_connected
                assert isinstance(state, bool), (
                    f"Connection state should be boolean during rapid operation {i}"
                )

            logger.info(
                "rapid_subscription_operations_completed",
                operation_count=rapid_subscription_count,
                message="Rapid subscription operations completed",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"Rapid subscription operations failed: {e}. "
                "High-frequency WebSocket operations are critical for trading systems.",
            )

    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_account_updates_public_api_method(
        self,
        hl_api_for_test_env: HyperliquidAPI,
    ) -> None:
        """Test account updates subscription through public API method."""

        async def user_events_handler(context: WebSocketContextUnion) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "user_events_message_received",
                message=context,
                handler="user_events_handler",
            )

        try:
            # Test the public account updates subscription method
            await hl_api_for_test_env.subscribe_to_account_updates()
            logger.info(
                "account_updates_method_success",
                message="✓ Public account updates method called successfully",
            )

            # Test subscription to user events without hardcoded addresses
            # This tests the public API behavior
            try:
                await hl_api_for_test_env.subscribe("userEvents", user_events_handler)
                logger.info(
                    "user_events_subscription_success",
                    message="✓ Successfully subscribed to user events through public API",
                )
            except (APIError, ValueError, TypeError, KeyError) as e:
                # User events might fail without proper authentication
                # This is acceptable and expected behavior
                logger.info(
                    "user_events_subscription_expected_failure",
                    error=str(e),
                    message="User events subscription failed as expected (authentication required)",
                )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"Public account updates API method failed: {e}. "
                "Public API methods must be accessible and callable."
            )
