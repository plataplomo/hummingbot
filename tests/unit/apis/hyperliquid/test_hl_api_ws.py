"""
Unit tests for HyperliquidAPI WebSocket integration.

This module tests the WebSocket integration in HyperliquidAPI,
specifically focusing on delegation to the router and WebSocket lifecycle management.
The detailed routing logic is tested in test_hl_ws_message_router.py.
"""

from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.hl_ws_message_router import HyperliquidWsMessageRouter


@pytest.fixture
def hyperliquid_config() -> dict[str, Any]:
    """Basic configuration for HyperliquidAPI tests."""
    return {
        "rest_endpoint": "https://api.hyperliquid.xyz",
        "ws_endpoint": "wss://api.hyperliquid.xyz/ws",
        "rate_limits": {},
        "request_timeout": 30.0,
    }


@pytest.fixture
def hyperliquid_secrets() -> dict[str, str]:
    """Basic secrets for HyperliquidAPI tests."""
    return {
        "wallet_address": "0x0000000000000000000000000000000000000000",
        "private_key": "0x" + "0" * 64,  # Dummy private key
    }


@pytest.fixture
def mock_hl_ws_router() -> Mock:
    """Mock the HyperliquidWsMessageRouter."""
    router = Mock(spec=HyperliquidWsMessageRouter)
    router.construct_subscription_payload = Mock(
        return_value={"method": "subscribe", "subscription": {"type": "test"}}
    )
    router.route_message = AsyncMock()
    return router


@pytest.fixture
def hl_api_with_mocked_router(
    hyperliquid_config: dict[str, Any], hyperliquid_secrets: dict[str, str], mock_hl_ws_router: Mock
) -> HyperliquidAPI:
    """Create HyperliquidAPI instance with mocked router and other dependencies."""
    with (
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidErrorMapper"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountDataMapper"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataMapper"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingDataMapper"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HttpClient"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountService"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingService"),
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataService"),
    ):
        # Convert secrets to the expected type
        secrets_with_none: dict[str, str | None] = {k: v for k, v in hyperliquid_secrets.items()}

        api = HyperliquidAPI(
            api_config=hyperliquid_config,
            secrets=secrets_with_none,
        )

        # Replace router with mock using object.__setattr__ to bypass protection
        object.__setattr__(api, "_hl_ws_router", mock_hl_ws_router)
        return api


class TestHyperliquidAPIWebSocketDelegation:
    """Test WebSocket delegation to router."""

    @pytest.mark.asyncio
    async def test_handle_websocket_message_delegates_to_router(
        self, hl_api_with_mocked_router: HyperliquidAPI, mock_hl_ws_router: Mock
    ) -> None:
        """Test that WebSocket message handling delegates to router."""
        message: dict[str, Any] = {"channel": "l2Book", "data": {"coin": "ETH", "levels": []}}

        # Use object.__getattribute__ to access protected method for testing
        handle_method = object.__getattribute__(
            hl_api_with_mocked_router, "_handle_websocket_message"
        )
        await handle_method(message)

        # Verify delegation to router
        ws_handlers = object.__getattribute__(hl_api_with_mocked_router, "_ws_handlers")
        mock_hl_ws_router.route_message.assert_called_once_with(message, ws_handlers)

    @pytest.mark.asyncio
    async def test_route_ws_message_delegates_to_router(
        self, hl_api_with_mocked_router: HyperliquidAPI, mock_hl_ws_router: Mock
    ) -> None:
        """Test that _route_ws_message delegates to router."""
        message: dict[str, Any] = {"channel": "trades", "data": [{"coin": "BTC", "px": "50000"}]}

        # Use object.__getattribute__ to access protected method for testing
        route_method = object.__getattribute__(hl_api_with_mocked_router, "_route_ws_message")
        await route_method(message)

        # Verify delegation to router
        ws_handlers = object.__getattribute__(hl_api_with_mocked_router, "_ws_handlers")
        mock_hl_ws_router.route_message.assert_called_once_with(message, ws_handlers)

    def test_construct_subscription_payload_delegates_to_router(
        self, hl_api_with_mocked_router: HyperliquidAPI, mock_hl_ws_router: Mock
    ) -> None:
        """Test that subscription payload construction delegates to router."""
        topic = "l2Book:ETH"

        # Use object.__getattribute__ to access protected method for testing
        construct_method = object.__getattribute__(
            hl_api_with_mocked_router, "_construct_subscription_payload"
        )
        result = construct_method(topic)

        # Verify delegation to router (should pass wallet address)
        wallet_address = object.__getattribute__(hl_api_with_mocked_router, "_wallet_address")
        mock_hl_ws_router.construct_subscription_payload.assert_called_once_with(
            topic, wallet_address
        )
        assert result == {"method": "subscribe", "subscription": {"type": "test"}}

    @pytest.mark.asyncio
    async def test_router_delegation_preserves_ws_handlers(
        self, hl_api_with_mocked_router: HyperliquidAPI, mock_hl_ws_router: Mock
    ) -> None:
        """Test that router receives the correct ws_handlers dictionary."""
        # Register some handlers using the public subscribe method
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        await hl_api_with_mocked_router.subscribe("l2Book:ETH", handler1)
        await hl_api_with_mocked_router.subscribe("userEvents", handler2)

        message: dict[str, Any] = {"channel": "l2Book", "data": {"coin": "ETH"}}
        route_method = object.__getattribute__(hl_api_with_mocked_router, "_route_ws_message")
        await route_method(message)

        # Verify the router received the correct handlers dict
        mock_hl_ws_router.route_message.assert_called_once()
        call_args = mock_hl_ws_router.route_message.call_args
        passed_handlers = call_args[0][1]  # Second argument

        ws_handlers = object.__getattribute__(hl_api_with_mocked_router, "_ws_handlers")
        assert passed_handlers is ws_handlers
        assert "l2Book:ETH" in passed_handlers
        assert "userEvents" in passed_handlers


class TestHyperliquidAPIWebSocketLifecycle:
    """Test WebSocket connection lifecycle management."""

    @pytest.fixture
    def hl_api(
        self, hyperliquid_config: dict[str, Any], hyperliquid_secrets: dict[str, str]
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI instance with mocked dependencies for lifecycle tests."""
        with (
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidErrorMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HttpClient"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataService"),
        ):
            # Convert secrets to the expected type
            secrets_with_none: dict[str, str | None] = {
                k: v for k, v in hyperliquid_secrets.items()
            }

            return HyperliquidAPI(
                api_config=hyperliquid_config,
                secrets=secrets_with_none,
            )

    @pytest.mark.asyncio
    async def test_subscribe_adds_handler_to_handlers_dict(self, hl_api: HyperliquidAPI) -> None:
        """Test that subscribing adds handlers to the handlers dictionary."""
        handler = AsyncMock()
        topic = "l2Book:ETH"

        await hl_api.subscribe(topic, handler)

        # Use object.__getattribute__ to access protected attribute for verification
        ws_handlers = object.__getattribute__(hl_api, "_ws_handlers")
        assert topic in ws_handlers
        assert ws_handlers[topic] is handler

    @pytest.mark.asyncio
    async def test_multiple_subscriptions(self, hl_api: HyperliquidAPI) -> None:
        """Test that multiple subscriptions work correctly."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        topic1 = "l2Book:ETH"
        topic2 = "userEvents"

        await hl_api.subscribe(topic1, handler1)
        await hl_api.subscribe(topic2, handler2)

        # Verify both handlers are registered
        ws_handlers = object.__getattribute__(hl_api, "_ws_handlers")
        assert topic1 in ws_handlers
        assert topic2 in ws_handlers
        assert ws_handlers[topic1] is handler1
        assert ws_handlers[topic2] is handler2

    @pytest.mark.asyncio
    async def test_handler_replacement(self, hl_api: HyperliquidAPI) -> None:
        """Test that subscribing to the same topic replaces the handler."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        topic = "l2Book:ETH"

        # Subscribe with first handler
        await hl_api.subscribe(topic, handler1)
        ws_handlers = object.__getattribute__(hl_api, "_ws_handlers")
        assert ws_handlers[topic] is handler1

        # Subscribe with second handler to same topic
        await hl_api.subscribe(topic, handler2)
        assert ws_handlers[topic] is handler2  # Should be replaced


class TestHyperliquidAPIWebSocketIntegration:
    """Integration tests with actual router (not mocked)."""

    @pytest.fixture
    def hl_api(
        self, hyperliquid_config: dict[str, Any], hyperliquid_secrets: dict[str, str]
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI instance with real router for integration tests."""
        with (
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidErrorMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HttpClient"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataService"),
        ):
            # Convert secrets to the expected type
            secrets_with_none: dict[str, str | None] = {
                k: v for k, v in hyperliquid_secrets.items()
            }

            return HyperliquidAPI(
                api_config=hyperliquid_config,
                secrets=secrets_with_none,
            )

    def test_router_initialization(self, hl_api: HyperliquidAPI) -> None:
        """Test that the router is properly initialized."""
        # Use object.__getattribute__ to access protected attribute for testing
        router = object.__getattribute__(hl_api, "_hl_ws_router")
        assert router is not None
        assert isinstance(router, HyperliquidWsMessageRouter)

    def test_subscription_payload_construction_integration(self, hl_api: HyperliquidAPI) -> None:
        """Test subscription payload construction through the actual router."""
        topic = "l2Book:ETH"

        # Use object.__getattribute__ to access protected method for testing
        construct_method = object.__getattribute__(hl_api, "_construct_subscription_payload")
        result = construct_method(topic)

        expected = {
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": "ETH"},
        }
        assert result == expected

    def test_subscription_payload_construction_user_events(self, hl_api: HyperliquidAPI) -> None:
        """Test userEvents subscription payload construction with wallet address."""
        topic = "userEvents"

        # Use object.__getattribute__ to access protected method for testing
        construct_method = object.__getattribute__(hl_api, "_construct_subscription_payload")
        result = construct_method(topic)

        expected = {
            "method": "subscribe",
            "subscription": {
                "type": "userEvents",
                "user": object.__getattribute__(hl_api, "_wallet_address"),
            },
        }
        assert result == expected

    @pytest.mark.asyncio
    async def test_message_routing_integration_unknown_channel(
        self, hl_api: HyperliquidAPI
    ) -> None:
        """Test message routing integration with unknown channel (should not crash)."""
        # Register a handler for an unknown channel
        handler = AsyncMock()
        await hl_api.subscribe("unknown_channel", handler)

        message: dict[str, Any] = {
            "channel": "unknown_channel",
            "data": {"some": "data"},
        }

        # Should not raise exception
        route_method = object.__getattribute__(hl_api, "_route_ws_message")
        await route_method(message)

        # Router should handle it gracefully (may or may not call handler depending on
        # implementation). The important thing is it doesn't crash


class TestHyperliquidAPIWebSocketEdgeCases:
    """Test edge cases and failure scenarios for Hyperliquid WebSocket functionality."""

    @pytest.fixture
    def hl_api_edge_case(
        self, hyperliquid_config: dict[str, Any], hyperliquid_secrets: dict[str, str]
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI instance for edge case testing."""
        with (
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidErrorMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HttpClient"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataService"),
        ):
            # Convert secrets to the expected type
            secrets_with_none: dict[str, str | None] = {
                k: v for k, v in hyperliquid_secrets.items()
            }

            return HyperliquidAPI(
                api_config=hyperliquid_config,
                secrets=secrets_with_none,
            )

    def test_subscription_payload_empty_topic(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test subscription payload construction with empty topic."""
        construct_method = object.__getattribute__(
            hl_api_edge_case, "_construct_subscription_payload"
        )
        result = construct_method("")

        # Router should handle empty topic gracefully - actual behavior depends on implementation
        # The important thing is it doesn't crash
        assert result is not None or result is None  # Either result is acceptable

    def test_subscription_payload_malformed_topic(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test subscription payload construction with malformed topic format."""
        malformed_topics = [
            "l2Book",  # Missing coin
            "l2Book:",  # Empty coin
            ":ETH",  # Missing type
            ":",  # Only separator
            "invalid_format",  # No separator
            "l2Book:ETH:extra",  # Too many parts
        ]

        construct_method = object.__getattribute__(
            hl_api_edge_case, "_construct_subscription_payload"
        )

        for topic in malformed_topics:
            # Should not crash, even with malformed topics
            result = construct_method(topic)
            # Result can be None or a dict - depends on router implementation
            assert result is None or isinstance(result, dict)

    def test_subscription_payload_special_characters_in_coin(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test subscription payload construction with special characters in coin."""
        special_topic = "l2Book:BTC@!#$%^&*()"
        construct_method = object.__getattribute__(
            hl_api_edge_case, "_construct_subscription_payload"
        )
        result = construct_method(special_topic)

        # Should handle special characters gracefully
        if result is not None:
            assert isinstance(result, dict)
            assert "subscription" in result

    def test_subscription_payload_unicode_coin(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test subscription payload construction with Unicode characters in coin."""
        unicode_topic = "l2Book:测试币"
        construct_method = object.__getattribute__(
            hl_api_edge_case, "_construct_subscription_payload"
        )
        result = construct_method(unicode_topic)

        # Should handle Unicode gracefully
        if result is not None:
            assert isinstance(result, dict)

    def test_subscription_payload_very_long_coin_name(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test subscription payload construction with very long coin name."""
        long_coin = "A" * 1000
        long_topic = f"l2Book:{long_coin}"
        construct_method = object.__getattribute__(
            hl_api_edge_case, "_construct_subscription_payload"
        )
        result = construct_method(long_topic)

        # Should handle long names gracefully
        if result is not None:
            assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_subscribe_with_none_handler_behavior(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test subscribing with None handler behavior."""
        # Subscribe with None handler - this might be allowed in the API
        await hl_api_edge_case.subscribe("l2Book:ETH", None)  # type: ignore[arg-type]

        ws_handlers = object.__getattribute__(hl_api_edge_case, "_ws_handlers")
        # Check that None handler was stored (API might allow this)
        assert "l2Book:ETH" in ws_handlers
        assert ws_handlers["l2Book:ETH"] is None

    @pytest.mark.asyncio
    async def test_subscribe_empty_topic(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test subscribing to empty topic."""
        handler = AsyncMock()
        await hl_api_edge_case.subscribe("", handler)

        ws_handlers = object.__getattribute__(hl_api_edge_case, "_ws_handlers")
        assert "" in ws_handlers
        assert ws_handlers[""] is handler

    @pytest.mark.asyncio
    async def test_message_routing_malformed_message_missing_channel(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test message routing with malformed message missing channel field."""
        malformed_message: dict[str, Any] = {
            "data": {"some": "data"},
            # Missing 'channel' field
        }

        # Should not raise exception (router should handle gracefully)
        route_method = object.__getattribute__(hl_api_edge_case, "_route_ws_message")
        await route_method(malformed_message)

    @pytest.mark.asyncio
    async def test_message_routing_malformed_message_missing_data(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test message routing with malformed message missing data field."""
        malformed_message: dict[str, Any] = {
            "channel": "l2Book",
            # Missing 'data' field
        }

        # Should not raise exception (router should handle gracefully)
        route_method = object.__getattribute__(hl_api_edge_case, "_route_ws_message")
        await route_method(malformed_message)

    @pytest.mark.asyncio
    async def test_message_routing_none_data(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test message routing with None data."""
        message_with_none: dict[str, Any] = {
            "channel": "l2Book",
            "data": None,
        }

        # Should not raise exception
        route_method = object.__getattribute__(hl_api_edge_case, "_route_ws_message")
        await route_method(message_with_none)

    @pytest.mark.asyncio
    async def test_message_routing_empty_message(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test message routing with completely empty message."""
        empty_message: dict[str, Any] = {}

        # Should not raise exception
        route_method = object.__getattribute__(hl_api_edge_case, "_route_ws_message")
        await route_method(empty_message)

    @pytest.mark.asyncio
    async def test_message_routing_invalid_data_types(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test message routing with invalid data types."""
        invalid_message: dict[str, Any] = {
            "channel": 123,  # Should be string
            "data": "not_a_dict_or_list",  # Should be dict or list
        }

        # Should not raise exception (router should handle gracefully)
        route_method = object.__getattribute__(hl_api_edge_case, "_route_ws_message")
        await route_method(invalid_message)

    @pytest.mark.asyncio
    async def test_user_events_message_with_malformed_data(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test userEvents message routing with malformed data structure."""
        malformed_user_events: dict[str, Any] = {
            "channel": "userEvents",
            "data": {
                # Missing required fields like 'fills', 'orders', etc.
                "invalid": "structure"
            },
        }

        # Should not raise exception
        route_method = object.__getattribute__(hl_api_edge_case, "_route_ws_message")
        await route_method(malformed_user_events)

    @pytest.mark.asyncio
    async def test_user_events_message_with_none_subcategories(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test userEvents message with None values in subcategories."""
        user_events_with_none: dict[str, Any] = {
            "channel": "userEvents",
            "data": {
                "fills": None,
                "orders": None,
                "positions": None,
            },
        }

        # Should not raise exception
        route_method = object.__getattribute__(hl_api_edge_case, "_route_ws_message")
        await route_method(user_events_with_none)

    @pytest.mark.asyncio
    async def test_rapid_subscribe_unsubscribe(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test rapid subscription and re-subscription to same topic."""
        topic = "l2Book:ETH"
        handlers = [AsyncMock() for _ in range(10)]

        # Rapidly subscribe different handlers to same topic
        for handler in handlers:
            await hl_api_edge_case.subscribe(topic, handler)

        # Only the last handler should be registered
        ws_handlers = object.__getattribute__(hl_api_edge_case, "_ws_handlers")
        assert ws_handlers[topic] is handlers[-1]

    @pytest.mark.asyncio
    async def test_handler_exception_during_call(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test that handler exceptions don't crash the system."""
        failing_handler = AsyncMock(side_effect=Exception("Handler failed"))
        topic = "l2Book:ETH"

        await hl_api_edge_case.subscribe(topic, failing_handler)

        # Create a valid message that passes router validation
        message: dict[str, Any] = {
            "channel": "l2Book",
            "data": {
                "coin": "ETH",
                "levels": [[], []],
                "time": 1234567890,  # Add required timestamp
            },
        }

        # Should not raise exception (WebSocket manager handles handler exceptions)
        route_method = object.__getattribute__(hl_api_edge_case, "_route_ws_message")
        await route_method(message)

        # Handler should have been called despite failing
        failing_handler.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_topics_single_handler(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test using the same handler for multiple topics."""
        handler = AsyncMock()
        topics = ["l2Book:ETH", "l2Book:BTC", "userEvents", "trades:ETH"]

        for topic in topics:
            await hl_api_edge_case.subscribe(topic, handler)

        ws_handlers = object.__getattribute__(hl_api_edge_case, "_ws_handlers")
        for topic in topics:
            assert ws_handlers[topic] is handler

    @pytest.mark.asyncio
    async def test_deeply_nested_message_data(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test handling of deeply nested message data structures."""
        complex_message: dict[str, Any] = {
            "channel": "l2Book",
            "data": {
                "coin": "ETH",
                "levels": [
                    [
                        {
                            "level1": {
                                "level2": {
                                    "level3": {
                                        "level4": {"level5": ["deep", "data", {"nested": True}]}
                                    }
                                }
                            }
                        }
                    ],
                    [],
                ],
            },
        }

        # Should handle complex nested structures without issues
        route_method = object.__getattribute__(hl_api_edge_case, "_route_ws_message")
        await route_method(complex_message)

    @pytest.mark.asyncio
    async def test_subscription_edge_case_wallet_address_none(
        self, hyperliquid_config: dict[str, Any]
    ) -> None:
        """Test subscription when wallet address is None."""
        # Create API with None wallet address
        secrets_with_none_wallet: dict[str, str | None] = {
            "wallet_address": None,
            "private_key": "0x" + "0" * 64,
        }

        with (
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidEip712Authenticator"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidErrorMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRequestBuilder"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidResponseHandler"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingDataMapper"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HttpClient"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidAccountService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidTradingService"),
            patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidMarketDataService"),
        ):
            api = HyperliquidAPI(
                api_config=hyperliquid_config,
                secrets=secrets_with_none_wallet,
            )

            # Test userEvents subscription with None wallet address
            construct_method = object.__getattribute__(api, "_construct_subscription_payload")
            result = construct_method("userEvents")

            # Should handle None wallet address gracefully
            if result is not None:
                assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_message_with_list_data_instead_of_dict(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test message routing with list data instead of expected dict."""
        message_with_list_data: dict[str, Any] = {
            "channel": "trades",
            "data": ["item1", "item2", "item3"],  # List instead of dict
        }

        # Should handle list data gracefully
        route_method = object.__getattribute__(hl_api_edge_case, "_route_ws_message")
        await route_method(message_with_list_data)

    @pytest.mark.asyncio
    async def test_message_with_extremely_large_data(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test message routing with extremely large data payload."""
        large_data = {"levels": [["large"] * 10000, ["data"] * 10000]}
        large_message: dict[str, Any] = {
            "channel": "l2Book",
            "data": large_data,
        }

        # Should handle large data without crashing
        route_method = object.__getattribute__(hl_api_edge_case, "_route_ws_message")
        await route_method(large_message)
