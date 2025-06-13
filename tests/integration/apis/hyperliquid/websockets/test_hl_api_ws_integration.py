"""Integration tests for HyperliquidAPI WebSocket functionality.

These tests verify the WebSocket integration behavior including message routing,
subscription management, and delegation to the router. They test the interaction
between multiple components through the WebSocket interface.
"""

from typing import Any
from unittest.mock import ANY, AsyncMock, Mock, patch

import pytest

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.hl_ws_message_router import HyperliquidWsMessageRouter
from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import (
    HyperliquidRawWsL2BookSubscriptionPayload,
    HyperliquidRawWsSubscribeRequest,
)
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets

pytestmark = [pytest.mark.integration, pytest.mark.zero_balance]


@pytest.fixture
def mock_hl_ws_router() -> Mock:
    """Mock the HyperliquidWsMessageRouter."""
    router = Mock(spec=HyperliquidWsMessageRouter)
    router.construct_subscription_payload = Mock(
        return_value={"method": "subscribe", "subscription": {"type": "test"}},
    )
    router.route_message = AsyncMock()
    return router


@pytest.fixture
def hl_api_with_mocked_router(
    active_hl_config: ExchangeSpecificConfig,
    active_hl_secrets: PrivateKeyAuthSecrets,
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
        patch("cyberdelta.apis.hyperliquid.hl_api.HyperliquidRateLimitStrategy"),
    ):
        api = HyperliquidAPI(
            exchange_config=active_hl_config,
            exchange_secrets=active_hl_secrets,
        )

        object.__setattr__(api, "_hl_ws_router", mock_hl_ws_router)
        return api


class TestHyperliquidAPIWebSocketDelegationIntegration:
    """Integration tests for WebSocket delegation to router."""

    @pytest.mark.asyncio
    async def test_handle_websocket_message_delegates_to_router(
        self,
        hl_api_with_mocked_router: HyperliquidAPI,
        mock_hl_ws_router: Mock,
    ) -> None:
        """Test that WebSocket message handling delegates to router."""
        message: dict[str, Any] = {"channel": "l2Book", "data": {"coin": "ETH", "levels": []}}

        handle_method = object.__getattribute__(
            hl_api_with_mocked_router,
            "_handle_websocket_message",
        )
        await handle_method(message)

        ws_handlers = object.__getattribute__(hl_api_with_mocked_router, "_ws_handlers")
        mock_hl_ws_router.route_message.assert_called_once_with(message, ws_handlers)

    @pytest.mark.asyncio
    async def test_route_ws_message_delegates_to_router(
        self,
        hl_api_with_mocked_router: HyperliquidAPI,
        mock_hl_ws_router: Mock,
    ) -> None:
        """Test that _route_ws_message delegates to router."""
        message: dict[str, Any] = {"channel": "trades", "data": [{"coin": "BTC", "px": "50000"}]}

        route_method = object.__getattribute__(hl_api_with_mocked_router, "_route_ws_message")
        await route_method(message)

        ws_handlers = object.__getattribute__(hl_api_with_mocked_router, "_ws_handlers")
        mock_hl_ws_router.route_message.assert_called_once_with(message, ws_handlers)

    def test_construct_subscription_payload_creates_valid_payload(
        self,
        hl_api_with_mocked_router: HyperliquidAPI,
        mock_hl_ws_router: Mock,
    ) -> None:
        """Test that subscription payload construction delegates to router."""
        from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import (
            HyperliquidRawWsL2BookSubscriptionPayload,
            HyperliquidRawWsSubscribeRequest,
        )

        topic = "l2Book:ETH"

        expected_payload = HyperliquidRawWsSubscribeRequest(
            method="subscribe",
            subscription=HyperliquidRawWsL2BookSubscriptionPayload(type="l2Book", coin="ETH"),
        )
        mock_hl_ws_router.construct_subscription_payload.return_value = expected_payload

        construct_method = object.__getattribute__(
            hl_api_with_mocked_router,
            "_construct_subscription_payload",
        )
        result = construct_method(topic)

        mock_hl_ws_router.construct_subscription_payload.assert_called_once_with(
            topic,
            ANY,
        )

        assert result == expected_payload

    @pytest.mark.asyncio
    async def test_router_delegation_preserves_ws_handlers(
        self,
        hl_api_with_mocked_router: HyperliquidAPI,
        mock_hl_ws_router: Mock,
    ) -> None:
        """Test that router receives the correct ws_handlers dictionary."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        await hl_api_with_mocked_router.subscribe("l2Book:ETH", handler1)
        await hl_api_with_mocked_router.subscribe("userEvents", handler2)

        message: dict[str, Any] = {"channel": "l2Book", "data": {"coin": "ETH"}}

        handle_method = object.__getattribute__(
            hl_api_with_mocked_router,
            "_handle_websocket_message",
        )
        await handle_method(message)

        call_args = mock_hl_ws_router.route_message.call_args
        assert call_args is not None
        handlers_dict = call_args[0][1]

        assert "l2Book:ETH" in handlers_dict
        assert "userEvents" in handlers_dict
        assert handlers_dict["l2Book:ETH"] == handler1
        assert handlers_dict["userEvents"] == handler2


class TestHyperliquidAPIWebSocketSubscriptionIntegration:
    """Integration tests for WebSocket subscription functionality."""

    @pytest.fixture
    def mock_ws_manager(self) -> AsyncMock:
        """Create a mock WebSocketManager."""
        manager = AsyncMock()
        manager.is_connected = True
        manager.send_json = AsyncMock(return_value=True)
        return manager

    @pytest.fixture
    def hl_api(
        self,
        active_hl_config: ExchangeSpecificConfig,
        active_hl_secrets: PrivateKeyAuthSecrets,
        mock_ws_manager: AsyncMock,
    ) -> HyperliquidAPI:
        """Create HyperliquidAPI instance with mocked dependencies."""
        with patch(
            "cyberdelta.apis.connectivity.ws_manager.WebSocketManager",
            return_value=mock_ws_manager,
        ):
            api = HyperliquidAPI(
                exchange_config=active_hl_config,
                exchange_secrets=active_hl_secrets,
            )
            object.__setattr__(api, "_ws_manager", mock_ws_manager)
            return api

    @pytest.mark.asyncio
    async def test_subscribe_l2book_sends_correct_payload(
        self,
        hl_api: HyperliquidAPI,
        mock_ws_manager: AsyncMock,
    ) -> None:
        """Test that subscribing to L2Book sends correct payload."""
        handler = AsyncMock()

        await hl_api.subscribe("l2Book:ETH", handler)

        mock_ws_manager.send_json.assert_called_once()
        sent_payload = mock_ws_manager.send_json.call_args[0][0]

        assert isinstance(sent_payload, HyperliquidRawWsSubscribeRequest)
        assert sent_payload.method == "subscribe"
        assert isinstance(sent_payload.subscription, HyperliquidRawWsL2BookSubscriptionPayload)
        assert sent_payload.subscription.type == "l2Book"
        assert sent_payload.subscription.coin == "ETH"

    @pytest.mark.asyncio
    async def test_subscribe_user_events_with_wallet_address(
        self,
        hl_api: HyperliquidAPI,
        mock_ws_manager: AsyncMock,
    ) -> None:
        """Test that subscribing to userEvents sends correct payload when wallet address is set."""
        handler = AsyncMock()

        wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
        object.__setattr__(hl_api, "_wallet_address", wallet_address)

        await hl_api.subscribe("userEvents", handler)

        mock_ws_manager.send_json.assert_called_once()
        sent_payload = mock_ws_manager.send_json.call_args[0][0]

        assert sent_payload.method == "subscribe"
        assert sent_payload.subscription.type == "userEvents"
        assert sent_payload.subscription.user == wallet_address

    @pytest.mark.asyncio
    async def test_subscribe_user_events_without_wallet_address(
        self,
        hl_api: HyperliquidAPI,
        mock_ws_manager: AsyncMock,
    ) -> None:
        """Test that userEvents without wallet address handles gracefully."""
        handler = AsyncMock()

        object.__setattr__(hl_api, "_wallet_address", None)

        await hl_api.subscribe("userEvents", handler)

        mock_ws_manager.send_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_integration(
        self,
        hl_api: HyperliquidAPI,
        mock_ws_manager: AsyncMock,
    ) -> None:
        """Test multiple subscriptions work correctly in integration."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        handler3 = AsyncMock()

        await hl_api.subscribe("l2Book:ETH", handler1)
        await hl_api.subscribe("trades:BTC", handler2)
        await hl_api.subscribe("allMids", handler3)

        assert mock_ws_manager.send_json.call_count == 3

        calls = mock_ws_manager.send_json.call_args_list

        payload1 = calls[0][0][0]
        assert payload1.subscription.type == "l2Book"
        assert payload1.subscription.coin == "ETH"

        payload2 = calls[1][0][0]
        assert payload2.subscription.type == "trades"
        assert payload2.subscription.coin == "BTC"

        payload3 = calls[2][0][0]
        assert payload3.subscription.type == "allMids"