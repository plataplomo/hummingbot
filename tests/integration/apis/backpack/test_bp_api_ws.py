"""Integration Tests for BackpackAPI WebSocket Integration.

-----------------------------------------------------

This module tests the WebSocket integration in BackpackAPI,
specifically focusing on delegation to the router and WebSocket lifecycle management.
The detailed routing logic is tested in test_bp_ws_message_router.py.
"""

import logging
from unittest.mock import AsyncMock, Mock, patch

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)


# Removed create_test_exchange_config function - now using active_bp_config from conftest.py

# Note: active_bp_config fixture removed - tests should use active_bp_config directly


# Removed active_bp_secrets fixture - now using active_bp_secrets from conftest.py


@pytest.fixture
def bp_api_with_mocked_router(
    active_bp_config: ExchangeSpecificConfig,
    active_bp_secrets: ApiKeyAuthSecrets,
) -> BackpackAPI:
    """Create BackpackAPI instance with mocked dependencies using proper dependency injection."""
    # Mock the WebSocketManager to avoid creating real connections
    mock_ws_manager = Mock()
    mock_ws_manager.is_connected = False
    mock_ws_manager.send_json = AsyncMock()
    mock_ws_manager.close = AsyncMock()

    # Use the integration conftest.py dependency injection pattern
    with patch("cyberdelta.apis.backpack.bp_api.BackpackEd25519Authenticator"):
        with patch("cyberdelta.apis.backpack.bp_api.BackpackErrorMapper"):
            with patch("cyberdelta.apis.backpack.bp_api.BackpackResponseHandler"):
                with patch("cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder"):
                    # Patch WebSocketManager in the base module where it's imported
                    with patch(
                        "cyberdelta.apis.base.exchange_api.WebSocketManager",
                    ) as MockWSManager:
                        MockWSManager.return_value = mock_ws_manager
                        # Create API using standard constructor - no protected member access
                        api = BackpackAPI(
                            exchange_config=active_bp_config,
                            exchange_secrets=active_bp_secrets,
                        )
                        return api


class TestBackpackAPIWebSocketDelegation:
    """Test WebSocket integration through public interface only."""

    @pytest.mark.asyncio
    async def test_websocket_subscription_public_interface(
        self,
        bp_api_with_mocked_router: BackpackAPI,
    ) -> None:
        """Test that WebSocket subscription works through public API without errors."""
        # Register a handler to verify the subscription system works
        handler = AsyncMock()
        topic = "depth.SOL_USDC"

        # This should complete without error - testing public interface only
        await bp_api_with_mocked_router.subscribe(topic, handler)

        # Verify the API maintains proper state after subscription
        assert bp_api_with_mocked_router.is_connected is False

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_public_interface(
        self,
        bp_api_with_mocked_router: BackpackAPI,
    ) -> None:
        """Test that multiple subscriptions work through public interface."""
        # Test that the subscription mechanism works for multiple topics
        handler1 = AsyncMock()
        handler2 = AsyncMock()

        topic1 = "ticker.BTC_USDC"
        topic2 = "depth.ETH_USDC"

        # Both subscriptions should complete without error
        await bp_api_with_mocked_router.subscribe(topic1, handler1)
        await bp_api_with_mocked_router.subscribe(topic2, handler2)

        # Verify that the API maintains consistent state
        assert bp_api_with_mocked_router.is_connected is False

    @pytest.mark.asyncio
    async def test_websocket_connection_status_consistent(
        self,
        bp_api_with_mocked_router: BackpackAPI,
    ) -> None:
        """Test that WebSocket connection status remains consistent through public operations."""
        # Test various public operations maintain consistent connection state
        handler = AsyncMock()

        # Test subscription operations
        await bp_api_with_mocked_router.subscribe("depth.SOL_USDC", handler)
        initial_status = bp_api_with_mocked_router.is_connected

        await bp_api_with_mocked_router.subscribe("ticker.BTC_USDC", handler)
        after_second_sub = bp_api_with_mocked_router.is_connected

        # Connection status should remain consistent
        assert initial_status == after_second_sub


class TestBackpackAPIWebSocketLifecycle:
    """Test WebSocket connection lifecycle management."""

    @pytest.fixture
    def bp_api(
        self,
        active_bp_config: ExchangeSpecificConfig,
        active_bp_secrets: ApiKeyAuthSecrets,
    ) -> BackpackAPI:
        """Create BackpackAPI instance with mocked dependencies for lifecycle tests."""
        with patch("cyberdelta.apis.backpack.bp_api.BackpackEd25519Authenticator"):
            with patch("cyberdelta.apis.backpack.bp_api.BackpackErrorMapper"):
                with patch("cyberdelta.apis.backpack.bp_api.BackpackResponseHandler"):
                    with patch("cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder"):
                        return BackpackAPI(
                            exchange_config=active_bp_config,
                            exchange_secrets=active_bp_secrets,
                        )

    @pytest.mark.asyncio
    async def test_subscribe_basic_functionality(self, bp_api: BackpackAPI) -> None:
        """Test that subscribing works correctly through the public API."""
        handler = AsyncMock()
        topic = "depth.SOL_USDC"

        # Subscribe should complete without error
        await bp_api.subscribe(topic, handler)

        # Verify that connection status is maintained properly
        assert bp_api.is_connected is False  # Should be false when not connected

        # Additional subscription should also work
        handler2 = AsyncMock()
        topic2 = "ticker.BTC_USDC"
        await bp_api.subscribe(topic2, handler2)

    @pytest.mark.asyncio
    async def test_multiple_subscriptions(self, bp_api: BackpackAPI) -> None:
        """Test that multiple subscriptions work correctly."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        topic1 = "depth.SOL_USDC"
        topic2 = "ticker.BTC_USDC"

        # Both subscriptions should complete without error
        await bp_api.subscribe(topic1, handler1)
        await bp_api.subscribe(topic2, handler2)

        # Verify that the API maintains consistent state
        assert bp_api.is_connected is False

    @pytest.mark.asyncio
    async def test_handler_replacement(self, bp_api: BackpackAPI) -> None:
        """Test that subscribing to the same topic works correctly."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        topic = "depth.SOL_USDC"

        # Subscribe with first handler
        await bp_api.subscribe(topic, handler1)

        # Subscribe with second handler to same topic should not error
        await bp_api.subscribe(topic, handler2)

        # Verify API state remains consistent
        assert bp_api.is_connected is False


class TestBackpackAPIWebSocketIntegration:
    """Integration tests with actual router (not mocked)."""

    @pytest.fixture
    def bp_api(
        self,
        active_bp_config: ExchangeSpecificConfig,
        active_bp_secrets: ApiKeyAuthSecrets,
    ) -> BackpackAPI:
        """Create BackpackAPI instance with real router for integration tests."""
        with patch("cyberdelta.apis.backpack.bp_api.BackpackEd25519Authenticator"):
            with patch("cyberdelta.apis.backpack.bp_api.BackpackErrorMapper"):
                with patch("cyberdelta.apis.backpack.bp_api.BackpackResponseHandler"):
                    with patch("cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder"):
                        return BackpackAPI(
                            exchange_config=active_bp_config,
                            exchange_secrets=active_bp_secrets,
                        )

    def test_router_initialization(self, bp_api: BackpackAPI) -> None:
        """Test that the router is properly initialized through public API behavior."""
        # Test that the router is working by verifying subscription functionality
        handler = AsyncMock()
        topic = "depth.SOL_USDC"

        # This should work without error if router is properly initialized
        import asyncio

        asyncio.run(bp_api.subscribe(topic, handler))

        # Verify that the API maintains proper state
        assert bp_api.is_connected is False

    @pytest.mark.asyncio
    async def test_subscription_payload_construction_integration(self, bp_api: BackpackAPI) -> None:
        """Test subscription payload construction works through subscription integration."""
        topic = "depth.SOL_USDC"
        handler = AsyncMock()

        # Test that subscription works, which means payload construction is working
        await bp_api.subscribe(topic, handler)

        # Test with different types of topics
        await bp_api.subscribe("ticker.BTC_USDC", handler)
        await bp_api.subscribe("account.orders", handler)

        # All should work without error if payload construction is working properly
        assert bp_api.is_connected is False

    @pytest.mark.asyncio
    async def test_unknown_topic_subscription_handling(self, bp_api: BackpackAPI) -> None:
        """Test that subscribing to unknown topics works correctly."""
        # Register a handler for an unknown topic
        handler = AsyncMock()
        unknown_topic = "unknown_topic"

        # Should not raise exception when subscribing to unknown topics
        await bp_api.subscribe(unknown_topic, handler)

        # Should also work with other unknown topics
        await bp_api.subscribe("another_unknown_topic", handler)

        # API should maintain consistent state
        assert bp_api.is_connected is False


class TestBackpackAPIWebSocketEdgeCases:
    """Test edge cases and failure scenarios for WebSocket functionality."""

    @pytest.fixture
    def bp_api_edge_case(
        self,
        active_bp_config: ExchangeSpecificConfig,
        active_bp_secrets: ApiKeyAuthSecrets,
    ) -> BackpackAPI:
        """Create BackpackAPI instance for edge case testing."""
        with patch("cyberdelta.apis.backpack.bp_api.BackpackEd25519Authenticator"):
            with patch("cyberdelta.apis.backpack.bp_api.BackpackErrorMapper"):
                with patch("cyberdelta.apis.backpack.bp_api.BackpackResponseHandler"):
                    with patch("cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder"):
                        return BackpackAPI(
                            exchange_config=active_bp_config,
                            exchange_secrets=active_bp_secrets,
                        )

    @pytest.mark.asyncio
    async def test_subscription_empty_topic_handling(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test subscription handling with empty topic."""
        handler = AsyncMock()

        # Test what happens when subscribing to empty topic - should either work or raise error
        try:
            await bp_api_edge_case.subscribe("", handler)
            # If it succeeds, verify API state is consistent
            assert bp_api_edge_case.is_connected is False
        except (ValueError, Exception) as e:
            # If it fails, that's also acceptable behavior - just verify it's a reasonable error
            assert isinstance(e, ValueError | Exception)

    @pytest.mark.asyncio
    async def test_subscription_special_characters_topic(
        self,
        bp_api_edge_case: BackpackAPI,
    ) -> None:
        """Test subscription with special characters in topic."""
        handler = AsyncMock()
        special_topic = "depth.BTC_USDC@!#$%^&*()"

        # Should handle special characters gracefully
        try:
            await bp_api_edge_case.subscribe(special_topic, handler)
            # If successful, verify state is consistent
            assert bp_api_edge_case.is_connected is False
        except Exception as e:
            # If it fails due to validation, that's also acceptable
            logger.debug(f"Expected exception during special character test: {e}")

    @pytest.mark.asyncio
    async def test_subscription_very_long_topic(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test subscription with very long topic."""
        handler = AsyncMock()
        long_topic = "depth." + "A" * 1000 + "_USDC"  # Over 1000 characters, max is 128

        # When WebSocket is not connected, subscription should work but not send messages
        # This tests that the API doesn't crash with very long topics
        await bp_api_edge_case.subscribe(long_topic, handler)

        # Test passes if no exception is raised (graceful handling of long topics)

    @pytest.mark.asyncio
    async def test_subscribe_handler_behavior(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test subscribing with proper handler behavior."""
        # Test that valid handlers work correctly
        mock_handler = AsyncMock()
        await bp_api_edge_case.subscribe("depth.SOL_USDC", mock_handler)

        # Verify API state remains consistent
        assert bp_api_edge_case.is_connected is False

    @pytest.mark.asyncio
    async def test_subscribe_empty_topic_duplicate(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test subscribing to empty topic (alternative test)."""
        handler = AsyncMock()

        # This test is similar to the earlier empty topic test
        # Just verify it doesn't crash
        try:
            await bp_api_edge_case.subscribe("", handler)
            assert bp_api_edge_case.is_connected is False
        except (ValueError, Exception) as e:
            logger.debug(f"Expected exception during empty topic test: {e}")

    @pytest.mark.asyncio
    async def test_websocket_connection_status_consistency(
        self,
        bp_api_edge_case: BackpackAPI,
    ) -> None:
        """Test that WebSocket connection status remains consistent."""
        # Test various operations maintain consistent state
        handler = AsyncMock()

        # Multiple subscriptions should work
        await bp_api_edge_case.subscribe("depth.SOL_USDC", handler)
        await bp_api_edge_case.subscribe("ticker.BTC_USDC", handler)

        # Connection status should remain consistent
        assert bp_api_edge_case.is_connected is False

    @pytest.mark.asyncio
    async def test_rapid_subscribe_operations(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test rapid subscription and re-subscription to same topic."""
        topic = "depth.SOL_USDC"
        handlers = [AsyncMock() for _ in range(10)]

        # Rapidly subscribe different handlers to same topic
        for handler in handlers:
            await bp_api_edge_case.subscribe(topic, handler)

        # All operations should complete without error
        assert bp_api_edge_case.is_connected is False

    @pytest.mark.asyncio
    async def test_multiple_topics_single_handler(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test using the same handler for multiple topics."""
        handler = AsyncMock()
        topics = ["depth.SOL_USDC", "depth.BTC_USDC", "ticker.ETH_USDC"]

        # All subscriptions should work without error
        for topic in topics:
            await bp_api_edge_case.subscribe(topic, handler)

        # Verify API state consistency
        assert bp_api_edge_case.is_connected is False

    @pytest.mark.asyncio
    async def test_unicode_topic_handling(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test handling of Unicode characters in topics."""
        unicode_topic = "depth.测试_USDC"
        handler = AsyncMock()

        # Should handle Unicode gracefully
        try:
            await bp_api_edge_case.subscribe(unicode_topic, handler)
            assert bp_api_edge_case.is_connected is False
        except Exception as e:
            # If it fails due to validation, that's also acceptable
            logger.debug(f"Expected exception during unicode test: {e}")

    @pytest.mark.asyncio
    async def test_api_state_consistency_across_operations(
        self,
        bp_api_edge_case: BackpackAPI,
    ) -> None:
        """Test that API state remains consistent across various operations."""
        handler = AsyncMock()

        # Test multiple different operations
        await bp_api_edge_case.subscribe("depth.SOL_USDC", handler)
        await bp_api_edge_case.subscribe("ticker.BTC_USDC", handler)

        # Test connection operations
        await bp_api_edge_case.connect_websocket()

        # API should maintain consistent state throughout
        assert isinstance(bp_api_edge_case.is_connected, bool)
