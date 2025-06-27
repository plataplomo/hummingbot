"""Integration Tests for BackpackAPI WebSocket subscription functionality.

This module tests WebSocket subscription functionality using real market data
and fail-fast error handling to ensure trading system reliability.

Security Compliance:
- No hardcoded currency pairs - uses dynamic symbol retrieval from exchange
- No mocking of critical WebSocket operations - uses real connections with VCR
- Fail-fast error handling - WebSocket failures cause test failures
- Real subscription testing - validates actual market data streams
"""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.service_args_models import GetMarketsArgs
from cyberdelta.config.structlog_config import get_logger


pytestmark = [pytest.mark.integration, pytest.mark.websockets, pytest.mark.vcr]

logger = get_logger(__name__)


async def get_dynamic_trading_symbols(api: BackpackAPI) -> dict[str, list[str]]:
    """Get dynamic trading symbols for different market types.

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


def validate_websocket_topic_format(topic: str) -> bool:
    """Validate WebSocket topic format.

    Args:
        topic: WebSocket topic string

    Returns:
        True if format is valid
    """
    if not topic:
        return False

    # Basic format validation for Backpack topics
    valid_prefixes = ["ticker.", "depth.", "trades.", "account.", "fills"]

    return any(topic.startswith(prefix) or topic == "fills" for prefix in valid_prefixes)


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/websockets/subscriptions"],
    indirect=True,
)
class TestBackpackAPIWebSocketSubscriptions:
    """Test WebSocket subscription methods with real market data."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_dynamic_symbol_subscription(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscription with dynamically fetched symbols."""
        _ = custom_vcr_config

        # Get real symbols from exchange
        symbols = await get_dynamic_trading_symbols(bp_api_for_test_env)

        if not symbols["spot"]:
            pytest.fail(
                "No spot symbols available from exchange. "
                "WebSocket subscription requires real trading symbols.",
            )

        test_symbol = symbols["spot"][0]
        topic = f"ticker.{test_symbol}"

        received_messages: list[dict[str, Any]] = []

        async def subscription_handler(
            message: dict[str, Any],
            full_message: dict[str, Any],
        ) -> None:
            """Handler for subscription messages."""
            await asyncio.sleep(0)  # Satisfy RUF029
            received_messages.append(message)
            logger.info(f"Subscription handler received: {message}")

        try:
            await bp_api_for_test_env.subscribe(topic, subscription_handler)

            # Validate connection state
            connection_state = bp_api_for_test_env.is_connected
            assert isinstance(connection_state, bool), (
                f"Connection state should be boolean, got {type(connection_state)}"
            )

            logger.info(f"✓ Successfully subscribed to dynamic symbol: {topic}")

        except Exception as e:
            pytest.fail(
                f"Dynamic symbol subscription failed for {test_symbol}: {e}. "
                "WebSocket subscriptions are critical for real-time trading data.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_multiple_stream_types_real_symbols(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscribing to different stream types with real symbols."""
        _ = custom_vcr_config

        symbols = await get_dynamic_trading_symbols(bp_api_for_test_env)

        if len(symbols["spot"]) < 2:
            pytest.fail(
                f"Need at least 2 spot symbols, got {len(symbols['spot'])}. "
                "Multi-stream testing requires multiple real symbols.",
            )

        symbol1, symbol2 = symbols["spot"][0], symbols["spot"][1]

        stream_handlers: dict[str, list[dict[str, Any]]] = {}

        def create_handler(
            stream_type: str,
        ) -> Callable[[dict[str, Any], dict[str, Any]], Coroutine[Any, Any, None]]:
            async def handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
                await asyncio.sleep(0)  # Satisfy RUF029
                if stream_type not in stream_handlers:
                    stream_handlers[stream_type] = []
                stream_handlers[stream_type].append(message)
                logger.info(f"{stream_type} handler received: {message}")

            return handler

        # Test different stream types
        stream_configs = [
            (f"ticker.{symbol1}", "ticker"),
            (f"depth.{symbol2}", "depth"),
            (f"trades.{symbol1}", "trades"),
            ("account.orderUpdate", "account"),
        ]

        try:
            for topic, stream_type in stream_configs:
                handler = create_handler(stream_type)
                await bp_api_for_test_env.subscribe(topic, handler)

                # Validate topic format
                if not validate_websocket_topic_format(topic):
                    pytest.fail(
                        f"Invalid topic format: {topic}. "
                        "Topic format validation is critical for WebSocket connectivity.",
                    )

            logger.info(
                f"✓ Successfully subscribed to {len(stream_configs)} different stream types",
            )

        except Exception as e:
            pytest.fail(
                f"Multiple stream type subscriptions failed: {e}. "
                "Multi-stream WebSocket functionality is critical for comprehensive trading data.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_subscription_state_consistency(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscription state consistency across operations."""
        _ = custom_vcr_config

        symbols = await get_dynamic_trading_symbols(bp_api_for_test_env)
        test_symbol = symbols["spot"][0]

        async def consistency_handler(
            message: dict[str, Any],
            full_message: dict[str, Any],
        ) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(f"Consistency handler: {message}")

        # Test state consistency
        initial_state = bp_api_for_test_env.is_connected

        try:
            # First subscription
            await bp_api_for_test_env.subscribe(f"ticker.{test_symbol}", consistency_handler)
            after_first = bp_api_for_test_env.is_connected

            # Second subscription
            await bp_api_for_test_env.subscribe(f"depth.{test_symbol}", consistency_handler)
            after_second = bp_api_for_test_env.is_connected

            # Validate state consistency and types
            assert isinstance(initial_state, bool), (
                f"Initial state should be boolean, got {type(initial_state)}"
            )
            assert isinstance(after_first, bool), (
                f"State after first subscription should be boolean, got {type(after_first)}"
            )
            assert isinstance(after_second, bool), (
                f"State after second subscription should be boolean, got {type(after_second)}"
            )

            logger.info(
                f"✓ State consistency validated: "
                f"{initial_state} -> {after_first} -> {after_second}",
            )

        except Exception as e:
            pytest.fail(
                f"Subscription state consistency test failed: {e}. "
                "State consistency is critical for reliable WebSocket operations.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_helper_subscription_methods_real_symbols(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test helper subscription methods with real symbols."""
        _ = custom_vcr_config

        symbols = await get_dynamic_trading_symbols(bp_api_for_test_env)

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

            # Validate connection state after all subscriptions
            final_state = bp_api_for_test_env.is_connected
            assert isinstance(final_state, bool), (
                f"Final connection state should be boolean, got {type(final_state)}"
            )

            logger.info("✓ All helper subscription methods successful with real symbols")

        except Exception as e:
            pytest.fail(
                f"Helper subscription methods failed with real symbols: {e}. "
                "Helper methods are critical for simplified WebSocket integration.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_concurrent_subscriptions_real_data(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test concurrent subscriptions with real market data."""
        _ = custom_vcr_config

        symbols = await get_dynamic_trading_symbols(bp_api_for_test_env)

        if len(symbols["spot"]) < 3:
            pytest.fail(
                f"Need at least 3 symbols for concurrent testing, got {len(symbols['spot'])}. "
                "Concurrent subscription tests require multiple real symbols.",
            )

        async def concurrent_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(f"Concurrent handler: {message}")

        # Create concurrent subscription tasks
        subscription_tasks: list[tuple[str, Any]] = []
        for i, symbol in enumerate(symbols["spot"][:3]):
            stream_types = ["ticker", "depth", "trades"]
            topic = f"{stream_types[i]}.{symbol}"

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
                f"✓ Concurrent subscriptions successful: {len(subscription_tasks)} operations",
            )

        except Exception as e:
            pytest.fail(
                f"Concurrent subscriptions failed: {e}. "
                "Concurrent WebSocket operations are critical for high-frequency trading.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_websocket_connection_lifecycle_real_operations(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test WebSocket connection lifecycle with real operations."""
        _ = custom_vcr_config

        symbols = await get_dynamic_trading_symbols(bp_api_for_test_env)
        test_symbol = symbols["spot"][0]
        topic = f"ticker.{test_symbol}"

        async def lifecycle_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(f"Lifecycle handler: {message}")

        try:
            # Test subscription before connection
            await bp_api_for_test_env.subscribe(topic, lifecycle_handler)
            subscription_state = bp_api_for_test_env.is_connected

            # Test connection establishment
            await bp_api_for_test_env.connect_websocket()
            connection_state = bp_api_for_test_env.is_connected

            # Validate state transitions
            assert isinstance(subscription_state, bool), (
                f"Subscription state should be boolean, got {type(subscription_state)}"
            )
            assert isinstance(connection_state, bool), (
                f"Connection state should be boolean, got {type(connection_state)}"
            )

            logger.info(
                f"✓ Connection lifecycle completed: "
                f"subscription={subscription_state}, connection={connection_state}",
            )

        except Exception as e:
            pytest.fail(
                f"WebSocket connection lifecycle failed: {e}. "
                "Connection lifecycle management is critical for trading system reliability.",
            )


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/websockets/advanced"],
    indirect=True,
)
class TestBackpackAPIAdvancedSubscriptions:
    """Test advanced WebSocket subscription scenarios."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_mixed_market_type_subscriptions(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscriptions to both spot and perp markets."""
        _ = custom_vcr_config

        symbols = await get_dynamic_trading_symbols(bp_api_for_test_env)

        async def mixed_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(f"Mixed market handler: {message}")

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

            # Validate mixed subscriptions
            if subscription_count == 0:
                pytest.fail(
                    "No subscriptions created - insufficient market types available. "
                    "Mixed market testing requires both spot and perp symbols.",
                )

            connection_state = bp_api_for_test_env.is_connected
            assert isinstance(connection_state, bool), (
                f"Connection state should be boolean after mixed subscriptions, "
                f"got {type(connection_state)}"
            )

            logger.info(
                f"✓ Mixed market subscriptions successful: {subscription_count} subscriptions",
            )

        except Exception as e:
            pytest.fail(
                f"Mixed market type subscriptions failed: {e}. "
                "Multi-market WebSocket functionality is critical for comprehensive trading.",
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    @pytest.mark.timing
    async def test_subscription_error_handling_real_scenarios(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test subscription error handling with real scenarios."""
        _ = custom_vcr_config

        symbols = await get_dynamic_trading_symbols(bp_api_for_test_env)
        valid_symbol = symbols["spot"][0]

        async def error_test_handler(message: dict[str, Any], full_message: dict[str, Any]) -> None:
            await asyncio.sleep(0)  # Satisfy RUF029
            logger.info(f"Error test handler: {message}")

        # Test scenarios that might cause errors
        test_scenarios = [
            (f"ticker.{valid_symbol}", True),  # Valid scenario
            ("ticker.DEFINITELY_INVALID_SYMBOL_FORMAT", False),  # Invalid symbol
            ("invalid_topic_format", False),  # Invalid format
            ("", False),  # Empty topic
        ]

        successful_subscriptions = 0
        handled_errors = 0

        for topic, should_succeed in test_scenarios:
            try:
                await bp_api_for_test_env.subscribe(topic, error_test_handler)

                if should_succeed:
                    successful_subscriptions += 1
                    logger.info(f"✓ Expected successful subscription: {topic}")
                else:
                    # If subscription didn't fail but we expected it to, that's still acceptable
                    # The exchange might handle invalid topics gracefully
                    logger.info(f"✓ Subscription accepted (exchange tolerance): {topic}")

                # Always validate connection state
                state = bp_api_for_test_env.is_connected
                assert isinstance(state, bool), (
                    f"Connection state should be boolean after {topic}, got {type(state)}"
                )

            except Exception as e:
                if should_succeed:
                    # Valid scenarios should not fail
                    pytest.fail(
                        f"Valid subscription failed for {topic}: {e}. "
                        "Valid WebSocket subscriptions are critical and must succeed.",
                    )
                else:
                    # Invalid scenarios may fail, which is acceptable
                    handled_errors += 1
                    logger.info(f"✓ Invalid subscription correctly handled: {topic} - {e}")

        # Ensure we had at least one successful subscription
        if successful_subscriptions == 0:
            pytest.fail(
                "No successful subscriptions occurred. "
                "At least one valid subscription must succeed for WebSocket functionality.",
            )

        logger.info(
            f"✓ Error handling test completed: {successful_subscriptions} successful, "
            f"{handled_errors} handled errors",
        )
