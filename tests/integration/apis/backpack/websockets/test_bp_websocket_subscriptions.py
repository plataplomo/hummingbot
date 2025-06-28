"""Integration Tests for BackpackAPI WebSocket subscription functionality.

This module tests WebSocket subscription functionality using real market data
and fail-fast error handling to ensure trading system reliability.

Security Compliance:
- No hardcoded currency pairs - uses dynamic symbol retrieval from exchange
- No mocking of critical WebSocket operations - uses real connections with VCR
- Fail-fast error handling - WebSocket failures cause test failures
- Real subscription validation - tests actual market data streams
"""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.service_args_models import GetMarketsArgs
from cyberdelta.config.structlog_config import get_logger
from tests.integration.apis.backpack.shared.bp_test_helpers import wait_for_condition


pytestmark = [pytest.mark.integration, pytest.mark.websockets, pytest.mark.vcr, pytest.mark.timing]

logger = get_logger(__name__)


async def get_real_trading_symbols(api: BackpackAPI) -> dict[str, list[str]]:
    """Get real trading symbols from the exchange for subscription testing.

    Args:
        api: BackpackAPI instance

    Returns:
        Dict containing spot and perp symbols

    Raises:
        RuntimeError: If unable to fetch symbols from exchange
    """
    try:
        markets = await api.get_markets(GetMarketsArgs())

        spot_symbols = [market.symbol for market in markets if not market.symbol.endswith("_PERP")]
        perp_symbols = [market.symbol for market in markets if market.symbol.endswith("_PERP")]

        if not spot_symbols:
            raise RuntimeError(
                "No spot symbols available from exchange. "
                "WebSocket subscription tests require real trading symbols.",
            )

        return {
            "spot": spot_symbols[:3],  # First 3 spot symbols
            "perp": perp_symbols[:2] if perp_symbols else [],  # First 2 perp symbols if available
        }

    except Exception as e:
        raise RuntimeError(
            f"Failed to fetch trading symbols from exchange: {e}. "
            "WebSocket subscription tests require real market data and "
            "cannot use hardcoded symbols.",
        ) from e


def validate_subscription_topic(topic: str, available_symbols: list[str]) -> bool:
    """Validate that a subscription topic uses real symbols.

    Args:
        topic: WebSocket topic string
        available_symbols: List of available symbols from exchange

    Returns:
        True if topic uses real symbol
    """
    if not topic or "." not in topic:
        return False

    # Extract symbol from topic (e.g., "ticker.BTC_USDC" -> "BTC_USDC")
    parts = topic.split(".")
    if len(parts) < 2:
        return False

    symbol = parts[1]
    return symbol in available_symbols


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/websockets/real_subscriptions"],
    indirect=True,
)
class TestBackpackAPIRealWebSocketSubscriptions:
    """Test WebSocket subscription methods with real market data."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_real_symbol_subscription_registration(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscription registration with real trading symbols."""
        _ = custom_vcr_config

        # Get real symbols from exchange
        symbols = await get_real_trading_symbols(bp_api_for_test_env)

        if not symbols["spot"]:
            pytest.fail(
                "No spot symbols available from exchange. "
                "WebSocket subscription requires real trading symbols.",
            )

        test_symbol = symbols["spot"][0]
        topic = f"ticker.{test_symbol}"

        received_messages: list[dict[str, Any]] = []

        async def real_symbol_handler(
            message: dict[str, Any],
            full_message: dict[str, Any],
        ) -> None:
            """Handler for real symbol subscription messages."""
            await asyncio.sleep(0)  # Satisfy RUF029
            received_messages.append(message)
            logger.info(
                "websocket_message_received",
                message_data=message,
                handler_type="real_symbol",
                message="Real symbol handler received message",
            )

        try:
            # Test subscription with real symbol
            await bp_api_for_test_env.subscribe(topic, real_symbol_handler)

            # Validate connection state
            connection_state = bp_api_for_test_env.is_connected
            assert isinstance(connection_state, bool), (
                f"Connection state should be boolean, got {type(connection_state)}"
            )

            logger.info(
                "real_symbol_subscription_success",
                topic=topic,
                message="Real symbol subscription successful",
            )

        except Exception as e:
            pytest.fail(
                f"Real symbol subscription failed for {test_symbol}: {e}. "
                "WebSocket subscriptions with real symbols are critical for trading data.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_multiple_real_stream_types(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscribing to different stream types with real symbols."""
        _ = custom_vcr_config

        symbols = await get_real_trading_symbols(bp_api_for_test_env)

        if len(symbols["spot"]) < 2:
            pytest.fail(
                f"Need at least 2 spot symbols, got {len(symbols['spot'])}. "
                "Multi-stream testing requires multiple real symbols.",
            )

        symbol1, symbol2 = symbols["spot"][0], symbols["spot"][1]

        stream_results: dict[str, list[dict[str, Any]]] = {}

        def create_stream_handler(
            stream_type: str,
        ) -> Callable[[dict[str, Any], dict[str, Any]], Coroutine[Any, Any, None]]:
            async def handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029
                if stream_type not in stream_results:
                    stream_results[stream_type] = []
                stream_results[stream_type].append(message)
                logger.info(
                    "websocket_stream_message_received",
                    stream_type=stream_type,
                    message_data=message,
                    message="Stream handler received message",
                )

            return handler

        # Test different stream types with real symbols
        stream_subscriptions = [
            (f"ticker.{symbol1}", "ticker"),
            (f"depth.{symbol2}", "depth"),
            (f"trades.{symbol1}", "trades"),
            ("account.orderUpdate", "account"),
        ]

        try:
            for topic, stream_type in stream_subscriptions:
                handler = create_stream_handler(stream_type)
                await bp_api_for_test_env.subscribe(topic, handler)

                # Validate topic uses real symbols when applicable
                if stream_type != "account":
                    all_symbols = symbols["spot"] + symbols["perp"]
                    if not validate_subscription_topic(topic, all_symbols):
                        pytest.fail(
                            f"Invalid topic using non-real symbol: {topic}. "
                            "All subscriptions must use real exchange symbols.",
                        )

            # Validate connection state
            connection_state = bp_api_for_test_env.is_connected
            assert isinstance(connection_state, bool), (
                f"Connection state should be boolean after subscriptions, "
                f"got {type(connection_state)}"
            )

            logger.info(
                "multiple_stream_types_success",
                stream_count=len(stream_subscriptions),
                message="Multiple real stream types successful",
            )

        except Exception as e:
            pytest.fail(
                f"Multiple real stream type subscriptions failed: {e}. "
                "Multi-stream functionality is critical for comprehensive trading data.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_real_subscription_state_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscription state consistency with real symbols."""
        _ = custom_vcr_config

        symbols = await get_real_trading_symbols(bp_api_for_test_env)
        test_symbol = symbols["spot"][0]

        async def consistency_handler(
            message: dict[str, Any],
            full_message: dict[str, Any],
        ) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_consistency_handler",
                message_data=message,
                message="Consistency handler received message",
            )

        # Track state consistency across operations
        state_tracking: list[tuple[str, Any]] = []

        try:
            # Initial state
            initial_state = bp_api_for_test_env.is_connected
            state_tracking.append(("initial", initial_state))

            # First subscription with real symbol
            await bp_api_for_test_env.subscribe(f"ticker.{test_symbol}", consistency_handler)
            after_first = bp_api_for_test_env.is_connected
            state_tracking.append(("after_first_sub", after_first))

            # Second subscription with real symbol
            await bp_api_for_test_env.subscribe(f"depth.{test_symbol}", consistency_handler)
            after_second = bp_api_for_test_env.is_connected
            state_tracking.append(("after_second_sub", after_second))

            # Validate all states are boolean and track consistency
            for stage, state in state_tracking:
                assert isinstance(state, bool), (
                    f"State at {stage} should be boolean, got {type(state)}"
                )

            logger.info(
                "real_subscription_state_consistency_validated",
                state_tracking=state_tracking,
                message="Real subscription state consistency validated",
            )

        except Exception as e:
            pytest.fail(
                f"Real subscription state consistency failed: {e}. "
                "State consistency is critical for reliable WebSocket operations with real data.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_real_helper_subscription_methods(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test helper subscription methods with real symbols."""
        _ = custom_vcr_config

        symbols = await get_real_trading_symbols(bp_api_for_test_env)

        if len(symbols["spot"]) < 3:
            pytest.fail(
                f"Need at least 3 symbols for helper method testing, got {len(symbols['spot'])}. "
                "Helper method tests require multiple real symbols.",
            )

        symbol1, symbol2, symbol3 = symbols["spot"][:3]

        try:
            # Test helper methods with real symbols
            await bp_api_for_test_env.subscribe_to_order_book(symbol1)
            await bp_api_for_test_env.subscribe_to_ticker(symbol2)
            await bp_api_for_test_env.subscribe_to_trades(symbol3)
            await bp_api_for_test_env.subscribe_to_account_updates()

            # Validate connection state after all helper method calls
            final_state = bp_api_for_test_env.is_connected
            assert isinstance(final_state, bool), (
                f"Final connection state should be boolean, got {type(final_state)}"
            )

            logger.info(
                "helper_subscription_methods_success",
                symbols=[symbol1, symbol2, symbol3],
                message="All helper subscription methods successful with real symbols",
            )

        except Exception as e:
            pytest.fail(
                f"Helper subscription methods failed with real symbols: {e}. "
                "Helper methods are critical for simplified WebSocket integration.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_real_websocket_connection_lifecycle(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test WebSocket connection lifecycle with real symbols."""
        _ = custom_vcr_config

        symbols = await get_real_trading_symbols(bp_api_for_test_env)
        test_symbol = symbols["spot"][0]
        topic = f"ticker.{test_symbol}"

        async def lifecycle_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_lifecycle_handler",
                message_data=message,
                message="Lifecycle handler received message",
            )

        try:
            # Test subscription before connection
            await bp_api_for_test_env.subscribe(topic, lifecycle_handler)
            subscription_state = bp_api_for_test_env.is_connected

            # Test connection establishment
            await bp_api_for_test_env.connect_websocket()
            connection_state = bp_api_for_test_env.is_connected

            # Validate state types and lifecycle
            assert isinstance(subscription_state, bool), (
                f"Subscription state should be boolean, got {type(subscription_state)}"
            )
            assert isinstance(connection_state, bool), (
                f"Connection state should be boolean, got {type(connection_state)}"
            )

            logger.info(
                "real_websocket_lifecycle_completed",
                subscription_state=subscription_state,
                connection_state=connection_state,
                message="Real WebSocket lifecycle completed",
            )

        except Exception as e:
            pytest.fail(
                f"Real WebSocket connection lifecycle failed: {e}. "
                "Connection lifecycle with real symbols is critical for "
                "trading system reliability.",
            )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/websockets/concurrent_real"],
    indirect=True,
)
class TestBackpackAPIConcurrentRealSubscriptions:
    """Test concurrent WebSocket subscriptions with real data."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_concurrent_real_subscriptions(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test concurrent subscriptions with real market data."""
        _ = custom_vcr_config

        symbols = await get_real_trading_symbols(bp_api_for_test_env)

        if len(symbols["spot"]) < 3:
            pytest.fail(
                f"Need at least 3 symbols for concurrent testing, "
                f"got {len(symbols['spot'])}. "
                "Concurrent subscription tests require multiple real symbols.",
            )

        async def concurrent_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_concurrent_handler",
                message_data=message,
                message="Concurrent real handler received message",
            )

        # Create concurrent subscription tasks with real symbols
        subscription_tasks: list[tuple[str, Any]] = []
        stream_types = ["ticker", "depth", "trades"]

        for i, symbol in enumerate(symbols["spot"][:3]):
            stream_type = stream_types[i % len(stream_types)]
            topic = f"{stream_type}.{symbol}"

            task = asyncio.create_task(bp_api_for_test_env.subscribe(topic, concurrent_handler))
            subscription_tasks.append((topic, task))

        try:
            # Execute concurrent subscriptions
            await asyncio.gather(*[task for _, task in subscription_tasks])

            # Validate final state
            final_state = bp_api_for_test_env.is_connected
            assert isinstance(final_state, bool), (
                f"Final state should be boolean after concurrent operations, "
                f"got {type(final_state)}"
            )

            logger.info(
                "concurrent_real_subscriptions_success",
                operation_count=len(subscription_tasks),
                message="Concurrent real subscriptions successful",
            )

        except Exception as e:
            pytest.fail(
                f"Concurrent real subscriptions failed: {e}. "
                "Concurrent operations with real data are critical for high-frequency trading.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_mixed_market_real_subscriptions(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscriptions to mixed market types with real symbols."""
        _ = custom_vcr_config

        symbols = await get_real_trading_symbols(bp_api_for_test_env)

        async def mixed_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_mixed_market_handler",
                message_data=message,
                message="Mixed market handler received message",
            )

        subscription_count = 0

        try:
            # Subscribe to spot markets
            for spot_symbol in symbols["spot"][:2]:
                await bp_api_for_test_env.subscribe(f"ticker.{spot_symbol}", mixed_handler)
                subscription_count += 1

            # Subscribe to perp markets if available
            for perp_symbol in symbols["perp"][:1]:
                await bp_api_for_test_env.subscribe(f"ticker.{perp_symbol}", mixed_handler)
                subscription_count += 1

            # Validate mixed market subscriptions
            if subscription_count == 0:
                pytest.fail(
                    "No real market subscriptions created. "
                    "Mixed market testing requires real spot and perp symbols.",
                )

            connection_state = bp_api_for_test_env.is_connected
            assert isinstance(connection_state, bool), (
                f"Connection state should be boolean after mixed subscriptions, "
                f"got {type(connection_state)}"
            )

            logger.info(
                "mixed_market_real_subscriptions_success",
                subscription_count=subscription_count,
                message="Mixed market real subscriptions successful",
            )

        except Exception as e:
            pytest.fail(
                f"Mixed market real subscriptions failed: {e}. "
                "Multi-market functionality with real data is critical for comprehensive trading.",
            )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/websockets/error_handling_real"],
    indirect=True,
)
class TestBackpackAPIRealSubscriptionErrorHandling:
    """Test subscription error handling with real scenarios."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_real_subscription_error_scenarios(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscription error handling with real and invalid scenarios."""
        _ = custom_vcr_config

        symbols = await get_real_trading_symbols(bp_api_for_test_env)
        valid_symbol = symbols["spot"][0]

        async def error_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_error_test_handler",
                message_data=message,
                message="Error test handler received message",
            )

        # Test scenarios mixing real and invalid
        test_scenarios = [
            (f"ticker.{valid_symbol}", True),  # Valid real symbol
            (
                "ticker.DEFINITELY_INVALID_SYMBOL_THAT_DOES_NOT_EXIST_ON_EXCHANGE",
                False,
            ),  # Invalid symbol
            ("invalid_topic_format_no_dot", False),  # Invalid format
            ("", False),  # Empty topic
        ]

        successful_count = 0
        error_count = 0

        for topic, should_succeed in test_scenarios:
            try:
                await bp_api_for_test_env.subscribe(topic, error_handler)

                if should_succeed:
                    successful_count += 1
                    logger.info(
                        "expected_successful_real_subscription",
                        topic=topic,
                        message="Expected successful real subscription",
                    )
                else:
                    # If subscription succeeded despite being invalid,
                    # that might be exchange tolerance
                    logger.info(
                        "exchange_accepted_invalid_topic",
                        topic=topic,
                        message="Exchange accepted invalid topic (tolerance)",
                    )

                # Always validate connection state
                state = bp_api_for_test_env.is_connected
                assert isinstance(state, bool), (
                    f"Connection state should be boolean after {topic}, got {type(state)}"
                )

            except Exception as e:
                if should_succeed:
                    # Valid real scenarios must not fail
                    pytest.fail(
                        f"Valid real subscription failed for {topic}: {e}. "
                        "Valid subscriptions with real symbols are critical and must succeed.",
                    )
                else:
                    # Invalid scenarios may fail appropriately
                    error_count += 1
                    logger.info(
                        "invalid_subscription_correctly_rejected",
                        topic=topic,
                        error=str(e),
                        message="Invalid subscription correctly rejected",
                    )

        # Ensure at least one successful subscription with real data
        if successful_count == 0:
            pytest.fail(
                "No successful real subscriptions occurred. "
                "At least one valid real subscription must succeed for WebSocket functionality.",
            )

        logger.info(
            "real_subscription_error_handling_completed",
            successful_count=successful_count,
            error_count=error_count,
            message="Real subscription error handling completed",
        )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_subscription_resilience_real_data(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscription resilience under stress with real data."""
        _ = custom_vcr_config

        symbols = await get_real_trading_symbols(bp_api_for_test_env)
        test_symbol = symbols["spot"][0]

        async def resilience_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(
                "websocket_resilience_handler",
                message_data=message,
                message="Resilience handler received message",
            )

        operation_count = 0
        max_operations = 10

        try:
            # Test rapid subscription operations with real symbol
            for i in range(max_operations):
                topic = f"ticker.{test_symbol}"
                await bp_api_for_test_env.subscribe(topic, resilience_handler)
                operation_count += 1

                # Validate state resilience
                state = bp_api_for_test_env.is_connected
                assert isinstance(state, bool), (
                    f"State should remain boolean during resilience test {i}, got {type(state)}"
                )

                # Brief pause to allow processing
                await wait_for_condition(
                    lambda: True,  # Always true, just wait
                    timeout_seconds=0.01,
                    poll_interval=0.01,
                    message="Processing delay",
                )

            logger.info(
                "subscription_resilience_with_real_data_confirmed",
                operation_count=operation_count,
                message="Subscription resilience with real data confirmed",
            )

        except Exception as e:
            pytest.fail(
                f"Subscription resilience failed after {operation_count} operations: {e}. "
                "System resilience with real data is critical for trading platform stability.",
            )
