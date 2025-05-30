"""
Tests for HyperliquidAPI WebSocket subscription functionality.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import (
    HyperliquidRawWsL2BookSubscriptionPayload,
    HyperliquidRawWsSubscribeRequest,
)
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ExchangeSecrets
from cyberdelta.enums.exchange_names import ExchangeName


@pytest.fixture
def hl_config() -> ExchangeSpecificConfig:
    """Create test configuration for HyperliquidAPI."""
    config_dict: dict[str, Any] = {
        "exchange_name": ExchangeName.HYPERLIQUID,
        "symbols": {},  # Add required symbols field
        "api_base_url": "https://api.hyperliquid.xyz",
        "ws_url": "wss://api.hyperliquid.xyz/ws",
        "rate_limit_per_minute": 1200,
        "chain_id": 1337,
    }
    return ExchangeSpecificConfig.model_validate(config_dict)


@pytest.fixture
def hl_secrets() -> ExchangeSecrets:
    """Create test secrets for HyperliquidAPI."""
    return ExchangeSecrets(
        api_key=SecretStr("test_key"),
        api_secret=SecretStr("test_secret"),
        private_key=SecretStr("0x" + "a" * 64),  # Mock private key
    )


@pytest.fixture
def mock_ws_manager() -> MagicMock:
    """Create a mock WebSocketManager."""
    manager = MagicMock()
    manager.is_connected = True
    manager.send_json = AsyncMock(return_value=True)
    return manager


@pytest.fixture
def hl_api(
    hl_config: ExchangeSpecificConfig, hl_secrets: ExchangeSecrets, mock_ws_manager: MagicMock
) -> HyperliquidAPI:
    """Create HyperliquidAPI instance with mocked dependencies."""
    with patch(
        "cyberdelta.apis.connectivity.ws_manager.WebSocketManager", return_value=mock_ws_manager
    ):
        api = HyperliquidAPI(exchange_config=hl_config, exchange_secrets=hl_secrets)
        # Use object.__setattr__ to bypass protection for testing setup
        object.__setattr__(api, "_ws_manager", mock_ws_manager)
        return api


class TestHyperliquidAPIWsSubscriptions:
    """Test HyperliquidAPI WebSocket subscription methods."""

    @pytest.mark.asyncio
    async def test_subscribe_l2book_sends_correct_payload(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that subscribing to L2Book sends correct payload."""
        handler = AsyncMock()
        
        # Subscribe to L2Book
        await hl_api.subscribe("l2Book:ETH", handler)

        # Verify WebSocket message was sent
        mock_ws_manager.send_json.assert_called_once()
        sent_payload = mock_ws_manager.send_json.call_args[0][0]

        assert isinstance(sent_payload, HyperliquidRawWsSubscribeRequest)
        assert sent_payload.method == "subscribe"
        assert isinstance(sent_payload.subscription, HyperliquidRawWsL2BookSubscriptionPayload)
        assert sent_payload.subscription.type == "l2Book"
        assert sent_payload.subscription.coin == "ETH"

    @pytest.mark.asyncio
    async def test_subscribe_trades_sends_correct_payload(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that subscribing to trades sends correct payload."""
        handler = AsyncMock()
        
        await hl_api.subscribe("trades:BTC", handler)

        # Verify WebSocket message was sent
        mock_ws_manager.send_json.assert_called_once()
        sent_payload = mock_ws_manager.send_json.call_args[0][0]

        assert sent_payload.method == "subscribe"
        assert sent_payload.subscription.type == "trades"
        assert sent_payload.subscription.coin == "BTC"

    @pytest.mark.asyncio
    async def test_subscribe_user_events_sends_correct_payload(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that subscribing to userEvents sends correct payload."""
        handler = AsyncMock()
        
        # Set wallet address using object.__setattr__ to bypass protection
        wallet_address = "0x1234567890abcdef1234567890abcdef12345678"
        object.__setattr__(hl_api, "_wallet_address", wallet_address)

        await hl_api.subscribe("userEvents", handler)

        # Verify WebSocket message was sent
        mock_ws_manager.send_json.assert_called_once()
        sent_payload = mock_ws_manager.send_json.call_args[0][0]

        assert sent_payload.method == "subscribe"
        assert sent_payload.subscription.type == "userEvents"
        assert sent_payload.subscription.user == wallet_address

    @pytest.mark.asyncio
    async def test_subscribe_user_events_without_wallet_logs_warning(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that userEvents without wallet address logs warning but doesn't crash."""
        handler = AsyncMock()
        
        # Clear wallet address using object.__setattr__ to bypass protection
        object.__setattr__(hl_api, "_wallet_address", None)

        # Subscribe should not crash but should not send payload
        await hl_api.subscribe("userEvents", handler)

        # The method should complete without error, but no WebSocket message should be sent
        # due to error in payload construction
        mock_ws_manager.send_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_subscribe_candle_sends_correct_payload(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that subscribing to candle sends correct payload."""
        handler = AsyncMock()
        
        await hl_api.subscribe("candle:ETH:1m", handler)

        # Verify WebSocket message was sent
        mock_ws_manager.send_json.assert_called_once()
        sent_payload = mock_ws_manager.send_json.call_args[0][0]

        assert sent_payload.method == "subscribe"
        assert sent_payload.subscription.type == "candle"
        assert sent_payload.subscription.coin == "ETH"
        assert sent_payload.subscription.interval == "1m"

    @pytest.mark.asyncio
    async def test_subscribe_all_mids_sends_correct_payload(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that subscribing to allMids sends correct payload."""
        handler = AsyncMock()
        
        await hl_api.subscribe("allMids", handler)

        # Verify WebSocket message was sent
        mock_ws_manager.send_json.assert_called_once()
        sent_payload = mock_ws_manager.send_json.call_args[0][0]

        assert sent_payload.method == "subscribe"
        assert sent_payload.subscription.type == "allMids"

    @pytest.mark.asyncio
    async def test_subscribe_invalid_topic_logs_warning(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that subscribing to invalid topic logs warning but doesn't crash."""
        handler = AsyncMock()

        # Subscribe to invalid topic
        await hl_api.subscribe("invalid_topic", handler)

        # The method should complete without error, but no WebSocket message should be sent 
        # due to error in payload construction
        mock_ws_manager.send_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_subscribe_malformed_topic_logs_warning(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that subscribing to malformed topic logs warning but doesn't crash."""
        handler = AsyncMock()
        
        # L2Book without coin
        await hl_api.subscribe("l2Book", handler)

        # The method should complete without error, but no WebSocket message should be sent 
        # due to error in payload construction
        mock_ws_manager.send_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_subscribe_candle_without_interval_logs_warning(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that subscribing to candle without interval logs warning but doesn't crash."""
        handler = AsyncMock()
        
        await hl_api.subscribe("candle:ETH", handler)

        # The method should complete without error, but no WebSocket message should be sent 
        # due to error in payload construction
        mock_ws_manager.send_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_subscribe_when_not_connected(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test subscribe behavior when WebSocket is not connected."""
        # Set WebSocket as not connected
        mock_ws_manager.is_connected = False

        handler = AsyncMock()
        await hl_api.subscribe("l2Book:ETH", handler)

        # Verify the API properly handled the disconnected state
        assert not hl_api.is_connected

        # No WebSocket message should be sent when disconnected
        mock_ws_manager.send_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_send_correct_payloads(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that multiple subscriptions are handled correctly."""
        # Subscribe to multiple topics
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        
        await hl_api.subscribe("l2Book:ETH", handler1)
        await hl_api.subscribe("trades:BTC", handler2)

        # Verify both subscriptions were sent
        assert mock_ws_manager.send_json.call_count == 2

        # Check the payloads
        calls = mock_ws_manager.send_json.call_args_list
        payloads = [call[0][0] for call in calls]

        # Find each payload type
        l2book_payload = next(p for p in payloads if p.subscription.type == "l2Book")
        trades_payload = next(p for p in payloads if p.subscription.type == "trades")

        assert l2book_payload.subscription.coin == "ETH"
        assert trades_payload.subscription.coin == "BTC"

    @pytest.mark.asyncio
    async def test_subscription_behavior_with_mixed_validity(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test subscription behavior with mix of valid and invalid topics."""
        # Subscribe to valid topics
        await hl_api.subscribe("l2Book:ETH", AsyncMock())
        await hl_api.subscribe("trades:BTC", AsyncMock())
        
        # Subscribe to invalid topic - should not send payload but should not crash
        await hl_api.subscribe("invalid:topic", AsyncMock())

        # Should only send 2 successful subscriptions
        assert mock_ws_manager.send_json.call_count == 2

        # Verify the successful subscriptions
        calls = mock_ws_manager.send_json.call_args_list
        payloads = [call[0][0] for call in calls]

        # Should have l2Book and trades but not invalid topic
        types = [p.subscription.type for p in payloads]
        assert "l2Book" in types
        assert "trades" in types
        assert len(types) == 2  # Only 2 valid subscriptions
