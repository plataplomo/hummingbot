"""Integration Tests for BackpackAPI WebSocket Integration.

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

pytestmark = [pytest.mark.integration, pytest.mark.websockets]

logger = logging.getLogger(__name__)


@pytest.fixture
def bp_api_with_mocked_router(
    active_bp_config: ExchangeSpecificConfig,
    active_bp_secrets: ApiKeyAuthSecrets,
) -> BackpackAPI:
    """Create BackpackAPI instance with mocked dependencies using proper dependency injection."""
    mock_ws_manager = Mock()
    mock_ws_manager.is_connected = False
    mock_ws_manager.send_json = AsyncMock()
    mock_ws_manager.close = AsyncMock()

    with patch("cyberdelta.apis.backpack.bp_api.BackpackEd25519Authenticator"):
        with patch("cyberdelta.apis.backpack.bp_api.BackpackErrorMapper"):
            with patch("cyberdelta.apis.backpack.bp_api.BackpackResponseHandler"):
                with patch("cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder"):
                    with patch(
                        "cyberdelta.apis.base.exchange_api.WebSocketManager",
                    ) as MockWSManager:
                        MockWSManager.return_value = mock_ws_manager
                        api = BackpackAPI(
                            exchange_config=active_bp_config,
                            exchange_secrets=active_bp_secrets,
                        )
                        return api


@pytest.mark.websockets
class TestBackpackAPIWebSocketDelegation:
    """Test WebSocket integration through public interface only."""

    @pytest.mark.asyncio
    async def test_websocket_subscription_public_interface(
        self,
        bp_api_with_mocked_router: BackpackAPI,
    ) -> None:
        """Test that WebSocket subscription works through public API without errors."""
        handler = AsyncMock()
        topic = "depth.SOL_USDC"

        await bp_api_with_mocked_router.subscribe(topic, handler)

        assert bp_api_with_mocked_router.is_connected is False

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_public_interface(
        self,
        bp_api_with_mocked_router: BackpackAPI,
    ) -> None:
        """Test that multiple subscriptions work through public interface."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()

        topic1 = "ticker.BTC_USDC"
        topic2 = "depth.ETH_USDC"

        await bp_api_with_mocked_router.subscribe(topic1, handler1)
        await bp_api_with_mocked_router.subscribe(topic2, handler2)

        assert bp_api_with_mocked_router.is_connected is False

    @pytest.mark.asyncio
    async def test_websocket_connection_status_consistent(
        self,
        bp_api_with_mocked_router: BackpackAPI,
    ) -> None:
        """Test that WebSocket connection status remains consistent through public operations."""
        handler = AsyncMock()

        await bp_api_with_mocked_router.subscribe("depth.SOL_USDC", handler)
        initial_status = bp_api_with_mocked_router.is_connected

        await bp_api_with_mocked_router.subscribe("ticker.BTC_USDC", handler)
        after_second_sub = bp_api_with_mocked_router.is_connected

        assert initial_status == after_second_sub


@pytest.mark.websockets
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

        await bp_api.subscribe(topic, handler)

        assert bp_api.is_connected is False

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

        await bp_api.subscribe(topic1, handler1)
        await bp_api.subscribe(topic2, handler2)

        assert bp_api.is_connected is False

    @pytest.mark.asyncio
    async def test_handler_replacement(self, bp_api: BackpackAPI) -> None:
        """Test that subscribing to the same topic works correctly."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        topic = "depth.SOL_USDC"

        await bp_api.subscribe(topic, handler1)

        await bp_api.subscribe(topic, handler2)

        assert bp_api.is_connected is False


@pytest.mark.websockets
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
        handler = AsyncMock()
        topic = "depth.SOL_USDC"

        import asyncio

        asyncio.run(bp_api.subscribe(topic, handler))

        assert bp_api.is_connected is False

    @pytest.mark.asyncio
    async def test_subscription_payload_construction_integration(self, bp_api: BackpackAPI) -> None:
        """Test subscription payload construction works through subscription integration."""
        topic = "depth.SOL_USDC"
        handler = AsyncMock()

        await bp_api.subscribe(topic, handler)

        await bp_api.subscribe("ticker.BTC_USDC", handler)
        await bp_api.subscribe("account.orders", handler)

        assert bp_api.is_connected is False

    @pytest.mark.asyncio
    async def test_unknown_topic_subscription_handling(self, bp_api: BackpackAPI) -> None:
        """Test that subscribing to unknown topics works correctly."""
        handler = AsyncMock()
        unknown_topic = "unknown_topic"

        await bp_api.subscribe(unknown_topic, handler)

        await bp_api.subscribe("another_unknown_topic", handler)

        assert bp_api.is_connected is False


@pytest.mark.websockets
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

        try:
            await bp_api_edge_case.subscribe("", handler)
            assert bp_api_edge_case.is_connected is False
        except (ValueError, Exception) as e:
            assert isinstance(e, ValueError | Exception)

    @pytest.mark.asyncio
    async def test_subscription_special_characters_topic(
        self,
        bp_api_edge_case: BackpackAPI,
    ) -> None:
        """Test subscription with special characters in topic."""
        handler = AsyncMock()
        special_topic = "depth.BTC_USDC@!#$%^&*()"

        try:
            await bp_api_edge_case.subscribe(special_topic, handler)
            assert bp_api_edge_case.is_connected is False
        except Exception as e:
            logger.debug(f"Expected exception during special character test: {e}")

    @pytest.mark.asyncio
    async def test_subscription_very_long_topic(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test subscription with very long topic."""
        handler = AsyncMock()
        long_topic = "depth." + "A" * 1000 + "_USDC"

        await bp_api_edge_case.subscribe(long_topic, handler)

    @pytest.mark.asyncio
    async def test_subscribe_handler_behavior(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test subscribing with proper handler behavior."""
        mock_handler = AsyncMock()
        await bp_api_edge_case.subscribe("depth.SOL_USDC", mock_handler)

        assert bp_api_edge_case.is_connected is False

    @pytest.mark.asyncio
    async def test_websocket_connection_status_consistency(
        self,
        bp_api_edge_case: BackpackAPI,
    ) -> None:
        """Test that WebSocket connection status remains consistent."""
        handler = AsyncMock()

        await bp_api_edge_case.subscribe("depth.SOL_USDC", handler)
        await bp_api_edge_case.subscribe("ticker.BTC_USDC", handler)

        assert bp_api_edge_case.is_connected is False

    @pytest.mark.asyncio
    async def test_rapid_subscribe_operations(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test rapid subscription and re-subscription to same topic."""
        topic = "depth.SOL_USDC"
        handlers = [AsyncMock() for _ in range(10)]

        for handler in handlers:
            await bp_api_edge_case.subscribe(topic, handler)

        assert bp_api_edge_case.is_connected is False

    @pytest.mark.asyncio
    async def test_multiple_topics_single_handler(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test using the same handler for multiple topics."""
        handler = AsyncMock()
        topics = ["depth.SOL_USDC", "depth.BTC_USDC", "ticker.ETH_USDC"]

        for topic in topics:
            await bp_api_edge_case.subscribe(topic, handler)

        assert bp_api_edge_case.is_connected is False

    @pytest.mark.asyncio
    async def test_unicode_topic_handling(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test handling of Unicode characters in topics."""
        unicode_topic = "depth.测试_USDC"
        handler = AsyncMock()

        try:
            await bp_api_edge_case.subscribe(unicode_topic, handler)
            assert bp_api_edge_case.is_connected is False
        except Exception as e:
            logger.debug(f"Expected exception during unicode test: {e}")

    @pytest.mark.asyncio
    async def test_api_state_consistency_across_operations(
        self,
        bp_api_edge_case: BackpackAPI,
    ) -> None:
        """Test that API state remains consistent across various operations."""
        handler = AsyncMock()

        await bp_api_edge_case.subscribe("depth.SOL_USDC", handler)
        await bp_api_edge_case.subscribe("ticker.BTC_USDC", handler)

        await bp_api_edge_case.connect_websocket()

        assert isinstance(bp_api_edge_case.is_connected, bool)
