"""
Tests for BackpackAPI WebSocket Integration
------------------------------------------

This module tests the WebSocket integration in BackpackAPI,
specifically focusing on delegation to the router and WebSocket lifecycle management.
The detailed routing logic is tested in test_bp_ws_message_router.py.
"""

from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.bp_ws_message_router import BackpackWsMessageRouter
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ExchangeSecrets
from cyberdelta.enums.exchange_names import ExchangeName


def create_test_exchange_config(
    api_base_url: str = "https://api.backpack.exchange",
    ws_url: str = "wss://ws.backpack.exchange",
    **kwargs: object,
) -> ExchangeSpecificConfig:
    """
    Create ExchangeSpecificConfig for testing by parsing from dict.
    This works with the validator that expects string inputs.
    """
    config_dict = {
        "exchange_name": ExchangeName.BACKPACK,
        "api_base_url": api_base_url,
        "ws_url": ws_url,
        "rate_limit_per_minute": 120,
        "symbols": {"SOL_USDC": "SOL_USDC", "BTC_USDC": "BTC_USDC"},
        **kwargs,
    }
    return ExchangeSpecificConfig.model_validate(config_dict)


@pytest.fixture
def mock_exchange_config() -> ExchangeSpecificConfig:
    """Mock ExchangeSpecificConfig."""
    return create_test_exchange_config(request_timeout_seconds=30.0)


@pytest.fixture
def mock_exchange_secrets() -> ExchangeSecrets:
    """Mock ExchangeSecrets configuration."""
    return ExchangeSecrets(api_key=SecretStr("test_key"), api_secret=SecretStr("test_secret"))


@pytest.fixture
def mock_bp_ws_router() -> Mock:
    """Mock the BackpackWsMessageRouter."""
    router = Mock(spec=BackpackWsMessageRouter)
    router.construct_subscription_payload = Mock(
        return_value={"op": "subscribe", "channel": "test"}
    )
    router.route_message = AsyncMock()
    return router


@pytest.fixture
def bp_api_with_mocked_router(
    mock_exchange_config: ExchangeSpecificConfig,
    mock_exchange_secrets: ExchangeSecrets,
    mock_bp_ws_router: Mock,
) -> BackpackAPI:
    """Create BackpackAPI instance with mocked router and other dependencies."""
    with patch("cyberdelta.apis.backpack.bp_api.BackpackHmacAuthenticator"):
        with patch("cyberdelta.apis.backpack.bp_api.BackpackErrorMapper"):
            with patch("cyberdelta.apis.backpack.bp_api.BackpackResponseHandler"):
                with patch("cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder"):
                    api = BackpackAPI(
                        exchange_config=mock_exchange_config, exchange_secrets=mock_exchange_secrets
                    )
                    # Use object.__setattr__ to bypass protection for testing
                    object.__setattr__(api, "_bp_ws_router", mock_bp_ws_router)
                    return api


class TestBackpackAPIWebSocketDelegation:
    """Test WebSocket delegation to router."""

    @pytest.mark.asyncio
    async def test_handle_websocket_message_delegates_to_router(
        self, bp_api_with_mocked_router: BackpackAPI, mock_bp_ws_router: Mock
    ) -> None:
        """Test that WebSocket message handling delegates to router."""
        message: dict[str, Any] = {"topic": "depth.SOL_USDC", "data": {"bids": [], "asks": []}}

        # Use object.__getattribute__ to access protected method for testing
        handle_method = object.__getattribute__(
            bp_api_with_mocked_router, "_handle_websocket_message"
        )
        await handle_method(message)

        # Verify delegation to router
        ws_handlers = object.__getattribute__(bp_api_with_mocked_router, "_ws_handlers")
        mock_bp_ws_router.route_message.assert_called_once_with(message, ws_handlers)

    @pytest.mark.asyncio
    async def test_route_ws_message_delegates_to_router(
        self, bp_api_with_mocked_router: BackpackAPI, mock_bp_ws_router: Mock
    ) -> None:
        """Test that _route_ws_message delegates to router."""
        message: dict[str, Any] = {"topic": "ticker.BTC_USDC", "data": {"price": "50000"}}

        # Use object.__getattribute__ to access protected method for testing
        route_method = object.__getattribute__(bp_api_with_mocked_router, "_route_ws_message")
        await route_method(message)

        # Verify delegation to router
        ws_handlers = object.__getattribute__(bp_api_with_mocked_router, "_ws_handlers")
        mock_bp_ws_router.route_message.assert_called_once_with(message, ws_handlers)

    def test_construct_subscription_payload_delegates_to_router(
        self, bp_api_with_mocked_router: BackpackAPI, mock_bp_ws_router: Mock
    ) -> None:
        """Test that subscription payload construction delegates to router."""
        topic = "depth.SOL_USDC"

        # Use object.__getattribute__ to access protected method for testing
        construct_method = object.__getattribute__(
            bp_api_with_mocked_router, "_construct_subscription_payload"
        )
        result = construct_method(topic)

        # Verify delegation to router
        mock_bp_ws_router.construct_subscription_payload.assert_called_once_with(topic)
        assert result == {"op": "subscribe", "channel": "test"}

    @pytest.mark.asyncio
    async def test_router_delegation_preserves_ws_handlers(
        self, bp_api_with_mocked_router: BackpackAPI, mock_bp_ws_router: Mock
    ) -> None:
        """Test that router receives the correct ws_handlers dictionary."""
        # Register some handlers using the public subscribe method
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        await bp_api_with_mocked_router.subscribe("depth.SOL_USDC", handler1)
        await bp_api_with_mocked_router.subscribe("ticker.BTC_USDC", handler2)

        message: dict[str, Any] = {"topic": "depth.SOL_USDC", "data": {}}
        route_method = object.__getattribute__(bp_api_with_mocked_router, "_route_ws_message")
        await route_method(message)

        # Verify the router received the correct handlers dict
        mock_bp_ws_router.route_message.assert_called_once()
        call_args = mock_bp_ws_router.route_message.call_args
        passed_handlers = call_args[0][1]  # Second argument

        ws_handlers = object.__getattribute__(bp_api_with_mocked_router, "_ws_handlers")
        assert passed_handlers is ws_handlers
        assert "depth.SOL_USDC" in passed_handlers
        assert "ticker.BTC_USDC" in passed_handlers


class TestBackpackAPIWebSocketLifecycle:
    """Test WebSocket connection lifecycle management."""

    @pytest.fixture
    def bp_api(
        self, mock_exchange_config: ExchangeSpecificConfig, mock_exchange_secrets: ExchangeSecrets
    ) -> BackpackAPI:
        """Create BackpackAPI instance with mocked dependencies for lifecycle tests."""
        with patch("cyberdelta.apis.backpack.bp_api.BackpackHmacAuthenticator"):
            with patch("cyberdelta.apis.backpack.bp_api.BackpackErrorMapper"):
                with patch("cyberdelta.apis.backpack.bp_api.BackpackResponseHandler"):
                    with patch("cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder"):
                        return BackpackAPI(
                            exchange_config=mock_exchange_config,
                            exchange_secrets=mock_exchange_secrets,
                        )

    @pytest.mark.asyncio
    async def test_subscribe_adds_handler_to_handlers_dict(self, bp_api: BackpackAPI) -> None:
        """Test that subscribing adds handlers to the handlers dictionary."""
        handler = AsyncMock()
        topic = "depth.SOL_USDC"

        await bp_api.subscribe(topic, handler)

        # Use object.__getattribute__ to access protected attribute for verification
        ws_handlers = object.__getattribute__(bp_api, "_ws_handlers")
        assert topic in ws_handlers
        assert ws_handlers[topic] is handler

    @pytest.mark.asyncio
    async def test_multiple_subscriptions(self, bp_api: BackpackAPI) -> None:
        """Test that multiple subscriptions work correctly."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        topic1 = "depth.SOL_USDC"
        topic2 = "ticker.BTC_USDC"

        await bp_api.subscribe(topic1, handler1)
        await bp_api.subscribe(topic2, handler2)

        # Verify both handlers are registered
        ws_handlers = object.__getattribute__(bp_api, "_ws_handlers")
        assert topic1 in ws_handlers
        assert topic2 in ws_handlers
        assert ws_handlers[topic1] is handler1
        assert ws_handlers[topic2] is handler2

    @pytest.mark.asyncio
    async def test_handler_replacement(self, bp_api: BackpackAPI) -> None:
        """Test that subscribing to the same topic replaces the handler."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        topic = "depth.SOL_USDC"

        # Subscribe with first handler
        await bp_api.subscribe(topic, handler1)
        ws_handlers = object.__getattribute__(bp_api, "_ws_handlers")
        assert ws_handlers[topic] is handler1

        # Subscribe with second handler to same topic
        await bp_api.subscribe(topic, handler2)
        assert ws_handlers[topic] is handler2  # Should be replaced


class TestBackpackAPIWebSocketIntegration:
    """Integration tests with actual router (not mocked)."""

    @pytest.fixture
    def bp_api(
        self, mock_exchange_config: ExchangeSpecificConfig, mock_exchange_secrets: ExchangeSecrets
    ) -> BackpackAPI:
        """Create BackpackAPI instance with real router for integration tests."""
        with patch("cyberdelta.apis.backpack.bp_api.BackpackHmacAuthenticator"):
            with patch("cyberdelta.apis.backpack.bp_api.BackpackErrorMapper"):
                with patch("cyberdelta.apis.backpack.bp_api.BackpackResponseHandler"):
                    with patch("cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder"):
                        return BackpackAPI(
                            exchange_config=mock_exchange_config,
                            exchange_secrets=mock_exchange_secrets,
                        )

    def test_router_initialization(self, bp_api: BackpackAPI) -> None:
        """Test that the router is properly initialized."""
        # Use object.__getattribute__ to access protected attribute for testing
        router = object.__getattribute__(bp_api, "_bp_ws_router")
        assert router is not None
        assert isinstance(router, BackpackWsMessageRouter)

    def test_subscription_payload_construction_integration(self, bp_api: BackpackAPI) -> None:
        """Test subscription payload construction through the actual router."""
        topic = "depth.SOL_USDC"

        # Use object.__getattribute__ to access protected method for testing
        construct_method = object.__getattribute__(bp_api, "_construct_subscription_payload")
        result = construct_method(topic)

        expected: dict[str, Any] = {
            "op": "subscribe",
            "channel": topic,
            "args": {},
        }
        assert result == expected

    @pytest.mark.asyncio
    async def test_message_routing_integration_unknown_topic(self, bp_api: BackpackAPI) -> None:
        """Test message routing integration with unknown topic (should not crash)."""
        # Register a handler for an unknown topic
        handler = AsyncMock()
        await bp_api.subscribe("unknown_topic", handler)

        message: dict[str, Any] = {
            "topic": "unknown_topic",
            "data": {"some": "data"},
        }

        # Should not raise exception and should call handler with raw data
        route_method = object.__getattribute__(bp_api, "_route_ws_message")
        await route_method(message)

        # Handler should be called with raw data for unknown topics
        handler.assert_called_once_with({"some": "data"}, message)


class TestBackpackAPIWebSocketEdgeCases:
    """Test edge cases and failure scenarios for WebSocket functionality."""

    @pytest.fixture
    def bp_api_edge_case(
        self, mock_exchange_config: ExchangeSpecificConfig, mock_exchange_secrets: ExchangeSecrets
    ) -> BackpackAPI:
        """Create BackpackAPI instance for edge case testing."""
        with patch("cyberdelta.apis.backpack.bp_api.BackpackHmacAuthenticator"):
            with patch("cyberdelta.apis.backpack.bp_api.BackpackErrorMapper"):
                with patch("cyberdelta.apis.backpack.bp_api.BackpackResponseHandler"):
                    with patch("cyberdelta.apis.backpack.bp_api.BackpackRequestBuilder"):
                        return BackpackAPI(
                            exchange_config=mock_exchange_config,
                            exchange_secrets=mock_exchange_secrets,
                        )

    def test_subscription_payload_empty_topic(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test subscription payload construction with empty topic."""
        construct_method = object.__getattribute__(
            bp_api_edge_case, "_construct_subscription_payload"
        )
        result = construct_method("")

        expected: dict[str, Any] = {
            "op": "subscribe",
            "channel": "",
            "args": {},
        }
        assert result == expected

    def test_subscription_payload_special_characters(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test subscription payload construction with special characters in topic."""
        special_topic = "depth.BTC_USDC@!#$%^&*()"
        construct_method = object.__getattribute__(
            bp_api_edge_case, "_construct_subscription_payload"
        )
        result = construct_method(special_topic)

        expected: dict[str, Any] = {
            "op": "subscribe",
            "channel": special_topic,
            "args": {},
        }
        assert result == expected

    def test_subscription_payload_very_long_topic(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test subscription payload construction with very long topic."""
        long_topic = "depth." + "A" * 1000 + "_USDC"
        construct_method = object.__getattribute__(
            bp_api_edge_case, "_construct_subscription_payload"
        )
        result = construct_method(long_topic)

        expected: dict[str, Any] = {
            "op": "subscribe",
            "channel": long_topic,
            "args": {},
        }
        assert result == expected

    @pytest.mark.asyncio
    async def test_subscribe_with_none_handler_behavior(
        self, bp_api_edge_case: BackpackAPI
    ) -> None:
        """Test subscribing with None handler behavior."""
        # Create a mock handler instead of None
        mock_handler = AsyncMock()
        await bp_api_edge_case.subscribe("depth.SOL_USDC", mock_handler)

        ws_handlers = object.__getattribute__(bp_api_edge_case, "_ws_handlers")
        # Check that handler was stored
        assert "depth.SOL_USDC" in ws_handlers
        assert ws_handlers["depth.SOL_USDC"] is mock_handler

    @pytest.mark.asyncio
    async def test_subscribe_empty_topic(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test subscribing to empty topic."""
        handler = AsyncMock()
        await bp_api_edge_case.subscribe("", handler)

        ws_handlers = object.__getattribute__(bp_api_edge_case, "_ws_handlers")
        assert "" in ws_handlers
        assert ws_handlers[""] is handler

    @pytest.mark.asyncio
    async def test_message_routing_malformed_message_missing_topic(
        self, bp_api_edge_case: BackpackAPI
    ) -> None:
        """Test message routing with malformed message missing topic field."""
        malformed_message: dict[str, Any] = {
            "data": {"some": "data"},
            # Missing 'topic' field
        }

        # Should not raise exception (router should handle gracefully)
        route_method = object.__getattribute__(bp_api_edge_case, "_route_ws_message")
        await route_method(malformed_message)

    @pytest.mark.asyncio
    async def test_message_routing_malformed_message_missing_data(
        self, bp_api_edge_case: BackpackAPI
    ) -> None:
        """Test message routing with malformed message missing data field."""
        malformed_message: dict[str, Any] = {
            "topic": "depth.SOL_USDC",
            # Missing 'data' field
        }

        # Should not raise exception (router should handle gracefully)
        route_method = object.__getattribute__(bp_api_edge_case, "_route_ws_message")
        await route_method(malformed_message)

    @pytest.mark.asyncio
    async def test_message_routing_none_data(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test message routing with None data."""
        message_with_none: dict[str, Any] = {
            "topic": "depth.SOL_USDC",
            "data": None,
        }

        # Should not raise exception
        route_method = object.__getattribute__(bp_api_edge_case, "_route_ws_message")
        await route_method(message_with_none)

    @pytest.mark.asyncio
    async def test_message_routing_empty_message(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test message routing with completely empty message."""
        empty_message: dict[str, Any] = {}

        # Should not raise exception
        route_method = object.__getattribute__(bp_api_edge_case, "_route_ws_message")
        await route_method(empty_message)

    @pytest.mark.asyncio
    async def test_message_routing_invalid_data_types(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test message routing with invalid data types."""
        invalid_message: dict[str, Any] = {
            "topic": 123,  # Should be string
            "data": "not_a_dict",  # Should be dict
        }

        # Should not raise exception (router should handle gracefully)
        route_method = object.__getattribute__(bp_api_edge_case, "_route_ws_message")
        await route_method(invalid_message)

    @pytest.mark.asyncio
    async def test_rapid_subscribe_unsubscribe(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test rapid subscription and re-subscription to same topic."""
        topic = "depth.SOL_USDC"
        handlers = [AsyncMock() for _ in range(10)]

        # Rapidly subscribe different handlers to same topic
        for handler in handlers:
            await bp_api_edge_case.subscribe(topic, handler)

        # Only the last handler should be registered
        ws_handlers = object.__getattribute__(bp_api_edge_case, "_ws_handlers")
        assert ws_handlers[topic] is handlers[-1]

    @pytest.mark.asyncio
    async def test_handler_exception_during_call(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test that handler exceptions don't crash the system."""
        failing_handler = AsyncMock(side_effect=Exception("Handler failed"))
        topic = "depth.SOL_USDC"

        await bp_api_edge_case.subscribe(topic, failing_handler)

        # Create a properly formatted message that passes validation
        message: dict[str, Any] = {
            "topic": topic,
            "data": {
                "bids": [],
                "asks": [],
                "lastUpdateId": "12345",  # Required field for Backpack depth messages (string)
            },
        }

        # Should not raise exception (WebSocket manager handles handler exceptions)
        route_method = object.__getattribute__(bp_api_edge_case, "_route_ws_message")
        await route_method(message)

        # Handler should have been called despite failing
        failing_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_topics_single_handler(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test using the same handler for multiple topics."""
        handler = AsyncMock()
        topics = ["depth.SOL_USDC", "depth.BTC_USDC", "ticker.ETH_USDC"]

        for topic in topics:
            await bp_api_edge_case.subscribe(topic, handler)

        ws_handlers = object.__getattribute__(bp_api_edge_case, "_ws_handlers")
        for topic in topics:
            assert ws_handlers[topic] is handler

    def test_unicode_topic_handling(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test handling of Unicode characters in topics."""
        unicode_topic = "depth.测试_USDC"
        construct_method = object.__getattribute__(
            bp_api_edge_case, "_construct_subscription_payload"
        )
        result = construct_method(unicode_topic)

        expected: dict[str, Any] = {
            "op": "subscribe",
            "channel": unicode_topic,
            "args": {},
        }
        assert result == expected

    @pytest.mark.asyncio
    async def test_deeply_nested_message_data(self, bp_api_edge_case: BackpackAPI) -> None:
        """Test handling of deeply nested message data structures."""
        complex_message: dict[str, Any] = {
            "topic": "depth.SOL_USDC",
            "data": {
                "level1": {
                    "level2": {"level3": {"level4": {"level5": ["deep", "data", {"nested": True}]}}}
                }
            },
        }

        # Should handle complex nested structures without issues
        route_method = object.__getattribute__(bp_api_edge_case, "_route_ws_message")
        await route_method(complex_message)
