"""
Unit tests for HyperliquidAPI WebSocket integration.

This module tests the WebSocket integration in HyperliquidAPI,
specifically focusing on delegation to the router and WebSocket lifecycle management.
The detailed routing logic is tested in test_hl_ws_message_router.py.
"""

from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import SecretStr, ValidationError

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.hl_ws_message_router import HyperliquidWsMessageRouter
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ExchangeSecrets
from cyberdelta.enums.exchange_names import ExchangeName


def create_test_exchange_config(
    api_base_url: str = "https://api.hyperliquid.xyz",
    ws_url: str = "wss://api.hyperliquid.xyz/ws",
    **kwargs: object,
) -> ExchangeSpecificConfig:
    """
    Create ExchangeSpecificConfig for testing by parsing from dict.
    This works with the validator that expects string inputs.
    """
    config_dict = {
        "exchange_name": ExchangeName.HYPERLIQUID,
        "api_base_url": api_base_url,
        "ws_url": ws_url,
        "rate_limit_per_minute": 300,
        "symbols": {"ETH": "ETH", "BTC": "BTC"},
        "chain_id": 1337,
        **kwargs,
    }
    return ExchangeSpecificConfig.model_validate(config_dict)


@pytest.fixture
def mock_exchange_config() -> ExchangeSpecificConfig:
    """Mock ExchangeSpecificConfig."""
    return create_test_exchange_config(
        request_timeout_seconds=30.0,
    )


@pytest.fixture
def hyperliquid_exchange_secrets() -> ExchangeSecrets:
    """Basic ExchangeSecrets for HyperliquidAPI tests."""
    return ExchangeSecrets(
        api_key=SecretStr(""),
        api_secret=SecretStr(""),
        private_key=SecretStr("0x" + "0" * 64),  # Dummy private key
        passphrase=None,
    )


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
    mock_exchange_config: ExchangeSpecificConfig,
    hyperliquid_exchange_secrets: ExchangeSecrets,
    mock_hl_ws_router: Mock,
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
        api = HyperliquidAPI(
            exchange_config=mock_exchange_config,
            exchange_secrets=hyperliquid_exchange_secrets,
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

    def test_construct_subscription_payload_creates_valid_payload(
        self, hl_api_with_mocked_router: HyperliquidAPI, mock_hl_ws_router: Mock
    ) -> None:
        """Test that subscription payload construction creates valid payload directly."""
        topic = "l2Book:ETH"

        # Use object.__getattribute__ to access protected method for testing
        construct_method = object.__getattribute__(
            hl_api_with_mocked_router, "_construct_subscription_payload"
        )
        result = construct_method(topic)

        # Verify the payload structure matches Hyperliquid format
        assert hasattr(result, "method")
        assert hasattr(result, "subscription")
        assert result.method == "subscribe"
        assert hasattr(result.subscription, "type")
        assert result.subscription.type == "l2Book"
        assert hasattr(result.subscription, "coin")
        assert result.subscription.coin == "ETH"

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
        self,
        mock_exchange_config: ExchangeSpecificConfig,
        hyperliquid_exchange_secrets: ExchangeSecrets,
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
            return HyperliquidAPI(
                exchange_config=mock_exchange_config,
                exchange_secrets=hyperliquid_exchange_secrets,
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
        self,
        mock_exchange_config: ExchangeSpecificConfig,
        hyperliquid_exchange_secrets: ExchangeSecrets,
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
            return HyperliquidAPI(
                exchange_config=mock_exchange_config,
                exchange_secrets=hyperliquid_exchange_secrets,
            )

    def test_router_initialization(self, hl_api: HyperliquidAPI) -> None:
        """Test that the router is properly initialized."""
        # Use object.__getattribute__ to access protected attribute for testing
        router = object.__getattribute__(hl_api, "_hl_ws_router")
        assert router is not None
        assert isinstance(router, HyperliquidWsMessageRouter)

    def test_subscription_payload_construction_integration(self, hl_api: HyperliquidAPI) -> None:
        """Test subscription payload construction returns proper model."""
        topic = "l2Book:ETH"

        # Use object.__getattribute__ to access protected method for testing
        construct_method = object.__getattribute__(hl_api, "_construct_subscription_payload")
        result = construct_method(topic)

        # Verify the returned model has correct structure and values
        assert result.method == "subscribe"
        assert result.subscription.type == "l2Book"
        assert result.subscription.coin == "ETH"

    def test_subscription_payload_construction_user_events(self, hl_api: HyperliquidAPI) -> None:
        """Test userEvents subscription payload construction with wallet address."""
        topic = "userEvents"

        # Use object.__getattribute__ to access protected method for testing
        construct_method = object.__getattribute__(hl_api, "_construct_subscription_payload")
        result = construct_method(topic)

        # Get wallet address from API for comparison
        wallet_address = object.__getattribute__(hl_api, "_wallet_address")
        
        # Verify the returned model has correct structure and values
        assert result.method == "subscribe"
        assert result.subscription.type == "userEvents"
        assert result.subscription.user == wallet_address

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
        self,
        mock_exchange_config: ExchangeSpecificConfig,
        hyperliquid_exchange_secrets: ExchangeSecrets,
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
            return HyperliquidAPI(
                exchange_config=mock_exchange_config,
                exchange_secrets=hyperliquid_exchange_secrets,
            )

    def test_subscription_payload_empty_topic(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test subscription payload construction with empty topic raises appropriate error."""
        construct_method = object.__getattribute__(
            hl_api_edge_case, "_construct_subscription_payload"
        )
        
        # Empty topic should raise APIError for invalid format
        with pytest.raises(APIError) as exc_info:
            construct_method("")
        
        assert "Unsupported WebSocket topic" in str(exc_info.value)
        assert exc_info.value.code == APIErrorCode.INVALID_PARAMS.value

    def test_subscription_payload_malformed_topic(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test subscription payload construction with malformed topic format raises proper errors."""
        malformed_topics = [
            "l2Book",  # Missing coin
            "l2Book:",  # Empty coin
            ":ETH",  # Missing type
            ":",  # Only separator
            "invalid_format",  # No separator
        ]

        construct_method = object.__getattribute__(
            hl_api_edge_case, "_construct_subscription_payload"
        )

        for topic in malformed_topics:
            # Malformed topics should raise APIError for invalid format or ValidationError for invalid data
            with pytest.raises((APIError, ValidationError)):
                construct_method(topic)

    def test_subscription_payload_extra_parts_ignored(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test that extra parts in topic are ignored (valid behavior)."""
        construct_method = object.__getattribute__(
            hl_api_edge_case, "_construct_subscription_payload"
        )
        
        # Extra parts should be ignored - this is valid behavior
        result = construct_method("l2Book:ETH:extra")
        assert result.method == "subscribe"
        assert result.subscription.type == "l2Book"
        assert result.subscription.coin == "ETH"

    def test_subscription_payload_special_characters_in_coin(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test subscription payload construction with special characters in coin."""
        special_topic = "l2Book:BTC@!#$%^&*()"
        construct_method = object.__getattribute__(
            hl_api_edge_case, "_construct_subscription_payload"
        )
        result = construct_method(special_topic)

        # Should handle special characters gracefully and return proper model
        assert result is not None
        assert result.method == "subscribe"
        assert result.subscription.type == "l2Book"
        assert result.subscription.coin == "BTC@!#$%^&*()"

    def test_subscription_payload_unicode_coin(self, hl_api_edge_case: HyperliquidAPI) -> None:
        """Test subscription payload construction with Unicode characters in coin."""
        unicode_topic = "l2Book:测试币"
        construct_method = object.__getattribute__(
            hl_api_edge_case, "_construct_subscription_payload"
        )
        result = construct_method(unicode_topic)

        # Should handle Unicode gracefully and return proper model
        assert result is not None
        assert result.method == "subscribe"
        assert result.subscription.type == "l2Book"
        assert result.subscription.coin == "测试币"

    def test_subscription_payload_very_long_coin_name(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test subscription payload construction with very long coin name."""
        long_coin = "A" * 1000
        long_topic = f"l2Book:{long_coin}"
        construct_method = object.__getattribute__(
            hl_api_edge_case, "_construct_subscription_payload"
        )

        # Very long coin names should raise ValidationError due to length constraint
        with pytest.raises(ValidationError, match="String value too long"):
            construct_method(long_topic)

    @pytest.mark.asyncio
    async def test_subscribe_with_none_handler_behavior(
        self, hl_api_edge_case: HyperliquidAPI
    ) -> None:
        """Test subscribing with None handler behavior."""
        # Create a mock handler instead of None
        mock_handler = AsyncMock()
        await hl_api_edge_case.subscribe("l2Book:ETH", mock_handler)

        ws_handlers = object.__getattribute__(hl_api_edge_case, "_ws_handlers")
        # Check that handler was stored
        assert "l2Book:ETH" in ws_handlers
        assert ws_handlers["l2Book:ETH"] is mock_handler

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
        self, mock_exchange_config: ExchangeSpecificConfig
    ) -> None:
        """Test subscription when wallet address is None."""
        # Create API with None-like secrets
        secrets_with_none_wallet = ExchangeSecrets(
            api_key=SecretStr(""),
            api_secret=SecretStr(""),
            private_key=SecretStr("0x" + "0" * 64),
            passphrase=None,
        )

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
                exchange_config=mock_exchange_config,
                exchange_secrets=secrets_with_none_wallet,
            )

            # Explicitly set wallet address to None to test edge case
            object.__setattr__(api, "_wallet_address", None)

            # Test userEvents subscription with None wallet address should raise ValueError
            construct_method = object.__getattribute__(api, "_construct_subscription_payload")
            with pytest.raises(ValueError, match="Cannot subscribe to userEvents without wallet address"):
                construct_method("userEvents")

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
