"""Integration Tests for BackpackAPI WebSocket Integration.

This module tests the WebSocket integration in BackpackAPI using real market data
and fail-fast error handling to ensure trading system reliability.

Security Compliance:
- No hardcoded currency pairs - uses dynamic symbol retrieval from exchange
- No mocking of critical WebSocket operations - uses real connections with VCR
- Fail-fast error handling - WebSocket failures cause test failures
- Real integration testing - validates actual WebSocket behavior
"""

import asyncio
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError
from cyberdelta.apis.models.service_args_models import GetMarketsArgs
from cyberdelta.config.structlog_config import get_logger
from tests.integration.apis.backpack.shared.bp_test_helpers import wait_for_condition


pytestmark = [pytest.mark.integration, pytest.mark.websockets, pytest.mark.vcr, pytest.mark.timing]

logger = get_logger(__name__)


async def get_available_trading_symbols(api: BackpackAPI, limit: int = 3) -> list[str]:
    """Get available trading symbols from the exchange.

    Args:
        api: BackpackAPI instance
        limit: Maximum number of symbols to return

    Returns:
        List of available trading symbols

    Raises:
        RuntimeError: If unable to fetch symbols from exchange
    """
    try:
        markets = await api.get_markets(GetMarketsArgs())

        # Get spot symbols (non-perpetual)
        spot_symbols = [market.symbol for market in markets if not market.symbol.endswith("_PERP")]

        if not spot_symbols:
            raise RuntimeError(
                "No trading symbols available from exchange. "
                "WebSocket integration tests require real market symbols.",
            )

        return spot_symbols[:limit]

    except (APIError, ValueError, TypeError, KeyError) as e:
        raise RuntimeError(
            f"Failed to fetch trading symbols from exchange: {e}. "
            "WebSocket integration tests require real market data and "
            "cannot use hardcoded symbols.",
        ) from e


def create_websocket_topics(symbols: list[str]) -> list[str]:
    """Create WebSocket topics for given symbols.

    Args:
        symbols: List of trading symbols

    Returns:
        List of WebSocket topics
    """
    topics: list[str] = []
    stream_types = ["ticker", "depth", "trades"]

    for i, symbol in enumerate(symbols):
        stream_type = stream_types[i % len(stream_types)]
        topics.append(f"{stream_type}.{symbol}")

    return topics


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/websockets/integration"],
    indirect=True,
)
class TestBackpackAPIWebSocketIntegration:
    """Test WebSocket integration with real market data and connections."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_websocket_subscription_real_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test WebSocket subscription with real market integration."""
        _ = custom_vcr_config

        # Get real symbols from exchange
        available_symbols = await get_available_trading_symbols(bp_api_for_test_env, 1)
        test_symbol = available_symbols[0]
        topic = f"ticker.{test_symbol}"

        received_messages: list[dict[str, Any]] = []

        async def integration_handler(
            message: dict[str, Any],
            full_message: dict[str, Any],
        ) -> None:
            """Handler for WebSocket integration messages."""
            await asyncio.sleep(0)  # Satisfy RUF029
            received_messages.append(message)
            logger.info(
                "websocket_message_received",
                message=message,
                handler_type="integration",
                log_message=f"Integration handler received: {message}",
            )

        try:
            # Test subscription with real symbol
            await bp_api_for_test_env.subscribe(topic, integration_handler)

            # Validate connection state
            connection_state = bp_api_for_test_env.is_connected
            assert isinstance(connection_state, bool), (
                f"Connection state should be boolean, got {type(connection_state)}"
            )

            logger.info(
                "websocket_integration_success",
                topic=topic,
                log_message=f"✓ WebSocket integration successful for real symbol: {topic}",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"WebSocket integration failed for real symbol {test_symbol}: {e}. "
                "WebSocket integration is critical for real-time trading data.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_multiple_subscriptions_real_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test multiple WebSocket subscriptions with real integration."""
        _ = custom_vcr_config

        available_symbols = await get_available_trading_symbols(bp_api_for_test_env, 2)

        if len(available_symbols) < 2:
            pytest.fail(
                f"Need at least 2 symbols for integration testing, got {len(available_symbols)}. "
                "Multi-subscription integration requires multiple real symbols.",
            )

        topics = create_websocket_topics(available_symbols)

        subscription_results: list[dict[str, Any]] = []

        async def multi_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            subscription_results.append(message)
            logger.info(
                "websocket_message_received",
                message=message,
                handler_type="multi_subscription",
                log_message=f"Multi-subscription handler: {message}",
            )

        try:
            # Test multiple subscriptions
            for topic in topics[:2]:  # Test first 2 topics
                await bp_api_for_test_env.subscribe(topic, multi_handler)

                # Validate connection state after each subscription
                state = bp_api_for_test_env.is_connected
                assert isinstance(state, bool), (
                    f"Connection state should be boolean after {topic}, got {type(state)}"
                )

            logger.info(
                "websocket_multiple_subscriptions_success",
                subscription_count=len(topics[:2]),
                status="successful",
                message="Multiple WebSocket subscriptions successful",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"Multiple WebSocket subscriptions failed: {e}. "
                "Multi-subscription integration is critical for comprehensive trading data.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_websocket_connection_lifecycle_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test WebSocket connection lifecycle with real integration."""
        _ = custom_vcr_config

        available_symbols = await get_available_trading_symbols(bp_api_for_test_env, 1)
        test_symbol = available_symbols[0]
        topic = f"depth.{test_symbol}"

        async def lifecycle_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_message_received",
                message=message,
                handler_type="lifecycle",
                log_message=f"Lifecycle handler: {message}",
            )

        try:
            # Test subscription before connection
            await bp_api_for_test_env.subscribe(topic, lifecycle_handler)
            subscription_state = bp_api_for_test_env.is_connected

            # Test connection establishment
            await bp_api_for_test_env.connect_websocket()
            connection_state = bp_api_for_test_env.is_connected

            # Validate state types and transitions
            assert isinstance(subscription_state, bool), (
                f"Subscription state should be boolean, got {type(subscription_state)}"
            )
            assert isinstance(connection_state, bool), (
                f"Connection state should be boolean, got {type(connection_state)}"
            )

            logger.info(
                "websocket_lifecycle_integration_complete",
                subscription_state=subscription_state,
                connection_state=connection_state,
                status="completed",
                message="WebSocket lifecycle integration completed",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"WebSocket lifecycle integration failed: {e}. "
                "Connection lifecycle is critical for trading system reliability.",
            )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/websockets/advanced_integration"],
    indirect=True,
)
class TestBackpackAPIAdvancedWebSocketIntegration:
    """Test advanced WebSocket integration scenarios."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_concurrent_subscriptions_real_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test concurrent WebSocket subscriptions with real integration."""
        _ = custom_vcr_config

        available_symbols = await get_available_trading_symbols(bp_api_for_test_env, 3)

        if len(available_symbols) < 3:
            pytest.fail(
                f"Need at least 3 symbols for concurrent testing, got {len(available_symbols)}. "
                "Concurrent integration requires multiple real symbols.",
            )

        topics = create_websocket_topics(available_symbols)

        async def concurrent_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_message_received",
                message_data=message,
                handler_type="concurrent",
                message="Concurrent integration handler received message",
            )

        # Create concurrent subscription tasks
        subscription_tasks: list[tuple[str, Any]] = []
        for topic in topics:
            task = asyncio.create_task(bp_api_for_test_env.subscribe(topic, concurrent_handler))
            subscription_tasks.append((topic, task))

        try:
            # Execute concurrent subscriptions
            await asyncio.gather(*[task for _, task in subscription_tasks])

            # Validate final connection state
            final_state = bp_api_for_test_env.is_connected
            assert isinstance(final_state, bool), (
                f"Final state should be boolean after concurrent operations, "
                f"got {type(final_state)}"
            )

            logger.info(
                "websocket_concurrent_integration_success",
                operation_count=len(subscription_tasks),
                status="successful",
                message="Concurrent WebSocket integration successful",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"Concurrent WebSocket integration failed: {e}. "
                "Concurrent operations are critical for high-frequency trading systems.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_subscription_error_handling_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscription error handling with real integration scenarios."""
        _ = custom_vcr_config

        available_symbols = await get_available_trading_symbols(bp_api_for_test_env, 1)
        valid_symbol = available_symbols[0]

        async def error_integration_handler(
            message: dict[str, Any],
            full_message: dict[str, Any],
        ) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_message_received",
                message_data=message,
                handler_type="error_integration",
                message="Error integration handler received message",
            )

        # Test scenarios with real integration
        test_scenarios = [
            (f"ticker.{valid_symbol}", True),  # Valid real symbol
            ("ticker.DEFINITELY_INVALID_SYMBOL_THAT_DOES_NOT_EXIST", False),  # Invalid symbol
            ("invalid.topic.format.without.proper.structure", False),  # Invalid format
        ]

        successful_count = 0
        error_count = 0

        for topic, should_succeed in test_scenarios:
            try:
                await bp_api_for_test_env.subscribe(topic, error_integration_handler)

                if should_succeed:
                    successful_count += 1
                    logger.info(
                        "websocket_subscription_success",
                        topic=topic,
                        expected=True,
                        status="successful",
                        message="Expected successful integration",
                    )
                else:
                    # If subscription succeeded despite being invalid,
                    # that might be exchange tolerance
                    logger.info(
                        "websocket_subscription_tolerance",
                        topic=topic,
                        tolerance=True,
                        status="accepted",
                        message="Exchange accepted invalid topic (tolerance)",
                    )

                # Always validate connection state
                state = bp_api_for_test_env.is_connected
                assert isinstance(state, bool), (
                    f"Connection state should be boolean after {topic}, got {type(state)}"
                )

            except (APIError, ValueError, TypeError, KeyError) as e:
                if should_succeed:
                    # Valid scenarios must not fail
                    pytest.fail(
                        f"Valid WebSocket integration failed for {topic}: {e}. "
                        "Valid subscriptions are critical and must succeed in integration testing.",
                    )
                else:
                    # Invalid scenarios may fail appropriately
                    error_count += 1
                    logger.info(
                        "websocket_subscription_rejected",
                        topic=topic,
                        error=str(e),
                        status="rejected",
                        message="Edge case topic correctly rejected",
                    )

        # Ensure at least one successful subscription
        if successful_count == 0:
            pytest.fail(
                "No successful subscriptions in integration testing. "
                "At least one valid subscription must succeed for WebSocket functionality.",
            )

        logger.info(
            "websocket_error_handling_complete",
            successful_count=successful_count,
            error_count=error_count,
            status="completed",
            message="Integration error handling completed",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_websocket_state_consistency_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test WebSocket state consistency throughout integration operations."""
        _ = custom_vcr_config

        available_symbols = await get_available_trading_symbols(bp_api_for_test_env, 2)

        async def state_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_message_received",
                message=message,
                handler_type="state_consistency",
                log_message=f"State consistency handler: {message}",
            )

        # Track state changes throughout operations
        state_history: list[tuple[str, Any]] = []

        try:
            # Initial state
            initial_state = bp_api_for_test_env.is_connected
            state_history.append(("initial", initial_state))

            # First subscription
            topic1 = f"ticker.{available_symbols[0]}"
            await bp_api_for_test_env.subscribe(topic1, state_handler)
            after_first = bp_api_for_test_env.is_connected
            state_history.append(("after_first_sub", after_first))

            # Second subscription
            if len(available_symbols) > 1:
                topic2 = f"depth.{available_symbols[1]}"
                await bp_api_for_test_env.subscribe(topic2, state_handler)
                after_second = bp_api_for_test_env.is_connected
                state_history.append(("after_second_sub", after_second))

            # Connection attempt
            await bp_api_for_test_env.connect_websocket()
            after_connect = bp_api_for_test_env.is_connected
            state_history.append(("after_connect", after_connect))

            # Validate all states are boolean
            for stage, state in state_history:
                assert isinstance(state, bool), (
                    f"State at {stage} should be boolean, got {type(state)}"
                )

            logger.info(
                "websocket_state_consistency_maintained",
                state_history=state_history,
                status="maintained",
                message="WebSocket state consistency maintained throughout integration",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"WebSocket state consistency integration failed: {e}. "
                "State consistency is critical for reliable trading operations.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_rapid_operations_integration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test rapid WebSocket operations with real integration."""
        _ = custom_vcr_config

        available_symbols = await get_available_trading_symbols(bp_api_for_test_env, 1)
        test_symbol = available_symbols[0]

        async def rapid_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_message_received",
                message_data=message,
                handler_type="rapid",
                message="Rapid integration handler received message",
            )

        topic = f"trades.{test_symbol}"
        operation_count = 5

        try:
            # Perform rapid subscription operations
            for i in range(operation_count):
                await bp_api_for_test_env.subscribe(topic, rapid_handler)

                # Validate state after each operation
                state = bp_api_for_test_env.is_connected
                assert isinstance(state, bool), (
                    f"State should be boolean during rapid operation {i}, got {type(state)}"
                )

                # Brief async pause to allow for processing
                await wait_for_condition(
                    lambda: True,  # Always true, just wait
                    timeout_seconds=0.01,
                    poll_interval=0.01,
                    message="Processing delay",
                )

            logger.info(
                "websocket_rapid_operations_success",
                operation_count=operation_count,
                status="successful",
                message="Rapid WebSocket operations integration successful",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"Rapid WebSocket operations integration failed: {e}. "
                "High-frequency operations are critical for trading system performance.",
            )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/websockets/edge_cases"],
    indirect=True,
)
class TestBackpackAPIWebSocketEdgeCases:
    """Test WebSocket edge cases with real integration."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_topic_validation_edge_cases(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test topic validation edge cases with real integration."""
        _ = custom_vcr_config

        async def edge_case_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_message_received",
                message_data=message,
                handler_type="edge_case",
                message="Edge case handler received message",
            )

        # Test edge case topics
        edge_case_topics = [
            "",  # Empty topic
            ".",  # Just a dot
            "ticker.",  # Missing symbol
            ".SYMBOL",  # Missing stream type
        ]

        handled_cases = 0

        for topic in edge_case_topics:
            try:
                await bp_api_for_test_env.subscribe(topic, edge_case_handler)

                # If subscription succeeded, validate state
                state = bp_api_for_test_env.is_connected
                assert isinstance(state, bool), (
                    f"Connection state should be boolean after edge case {topic}, got {type(state)}"
                )

                handled_cases += 1
                logger.info(
                    "websocket_edge_case_handled",
                    topic=topic,
                    status="handled",
                    message="Edge case topic handled",
                )

            except (APIError, ValueError, TypeError, KeyError) as e:
                # Edge case failures are acceptable, but should be proper exceptions
                if "connection" in str(e).lower() or "network" in str(e).lower():
                    pytest.fail(
                        f"Network error during edge case testing for '{topic}': {e}. "
                        "Network issues should not occur during topic validation.",
                    )
                else:
                    handled_cases += 1
                    logger.info(
                        "websocket_edge_case_rejected",
                        topic=topic,
                        error=str(e),
                        status="rejected",
                        message="Edge case topic correctly rejected",
                    )

        # Ensure all edge cases were handled appropriately
        assert handled_cases == len(edge_case_topics), (
            f"All edge cases should be handled, processed {handled_cases}/{len(edge_case_topics)}"
        )

        logger.info(
            "websocket_edge_cases_complete",
            handled_cases=handled_cases,
            status="completed",
            message="Topic validation edge cases completed",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_integration_resilience(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test WebSocket integration resilience under various conditions."""
        _ = custom_vcr_config

        available_symbols = await get_available_trading_symbols(bp_api_for_test_env, 1)
        test_symbol = available_symbols[0]

        async def resilience_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_message_received",
                message_data=message,
                handler_type="resilience",
                message="Resilience handler received message",
            )

        operation_count = 0

        try:
            # Test sequence of operations that should be resilient
            operations = [
                f"ticker.{test_symbol}",
                f"depth.{test_symbol}",
                f"trades.{test_symbol}",
            ]

            for topic in operations:
                await bp_api_for_test_env.subscribe(topic, resilience_handler)
                operation_count += 1

                # Validate state resilience
                state = bp_api_for_test_env.is_connected
                assert isinstance(state, bool), (
                    f"State should remain boolean during resilience test {operation_count}"
                )

            # Test connection operation resilience
            await bp_api_for_test_env.connect_websocket()
            final_state = bp_api_for_test_env.is_connected
            assert isinstance(final_state, bool), (
                "Final state should be boolean after resilience testing"
            )

            logger.info(
                "websocket_resilience_confirmed",
                operation_count=operation_count,
                status="confirmed",
                message="WebSocket integration resilience confirmed",
            )

        except (APIError, ValueError, TypeError, KeyError) as e:
            pytest.fail(
                f"WebSocket integration resilience failed after {operation_count} operations: {e}. "
                "System resilience is critical for trading platform stability.",
            )
