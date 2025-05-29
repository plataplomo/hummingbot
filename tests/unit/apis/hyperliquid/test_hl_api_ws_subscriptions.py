"""
Tests for HyperliquidAPI WebSocket subscription functionality.
"""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import AnyUrl, HttpUrl, SecretStr

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.hyperliquid.models.hl_ws_payloads import (
    HyperliquidRawWsL2BookSubscriptionPayload,
    HyperliquidRawWsSubscribeRequest,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ExchangeSecrets
from cyberdelta.enums.exchange_names import ExchangeName


@pytest.fixture
def hl_config() -> ExchangeSpecificConfig:
    """Create test configuration for HyperliquidAPI."""
    return ExchangeSpecificConfig(
        exchange_name=ExchangeName.HYPERLIQUID,
        symbols={},  # Add required symbols field
        api_base_url=HttpUrl("https://api.hyperliquid.xyz"),
        ws_url=AnyUrl("wss://api.hyperliquid.xyz/ws"),
        rate_limit_per_minute=1200,
        chain_id=1337,
    )


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
async def hl_api(
    hl_config: ExchangeSpecificConfig, hl_secrets: ExchangeSecrets, mock_ws_manager: MagicMock
) -> AsyncGenerator[HyperliquidAPI]:
    """Create HyperliquidAPI instance with mocked dependencies."""
    with patch("cyberdelta.apis.hyperliquid.hl_api.WebSocketManager", return_value=mock_ws_manager):
        api = HyperliquidAPI(exchange_config=hl_config, exchange_secrets=hl_secrets)
        api._ws_manager = mock_ws_manager
        yield api
        await api.close()


class TestHyperliquidAPIWsSubscriptions:
    """Test HyperliquidAPI WebSocket subscription methods."""

    def test_construct_l2book_subscription_payload(self, hl_api: HyperliquidAPI) -> None:
        """Test constructing L2Book subscription payload."""
        payload = hl_api._construct_subscription_payload("l2Book:ETH")

        assert isinstance(payload, HyperliquidRawWsSubscribeRequest)
        assert payload.method == "subscribe"
        assert isinstance(payload.subscription, HyperliquidRawWsL2BookSubscriptionPayload)
        assert payload.subscription.type == "l2Book"
        assert payload.subscription.coin == "ETH"

    def test_construct_trades_subscription_payload(self, hl_api: HyperliquidAPI) -> None:
        """Test constructing trades subscription payload."""
        payload = hl_api._construct_subscription_payload("trades:BTC")

        assert payload.method == "subscribe"
        assert payload.subscription.type == "trades"
        assert payload.subscription.coin == "BTC"

    def test_construct_user_events_subscription_payload(self, hl_api: HyperliquidAPI) -> None:
        """Test constructing userEvents subscription payload."""
        # Set wallet address
        hl_api._wallet_address = "0x1234567890abcdef1234567890abcdef12345678"

        payload = hl_api._construct_subscription_payload("userEvents")

        assert payload.method == "subscribe"
        assert payload.subscription.type == "userEvents"
        assert payload.subscription.user == hl_api._wallet_address

    def test_construct_user_events_without_wallet_raises_error(
        self, hl_api: HyperliquidAPI
    ) -> None:
        """Test that userEvents without wallet address raises ValueError."""
        # Clear wallet address
        hl_api._wallet_address = None

        with pytest.raises(ValueError) as exc_info:
            hl_api._construct_subscription_payload("userEvents")

        assert "Cannot subscribe to userEvents without wallet address" in str(exc_info.value)
        assert "Ensure private_key is configured" in str(exc_info.value)

    def test_construct_candle_subscription_payload(self, hl_api: HyperliquidAPI) -> None:
        """Test constructing candle subscription payload."""
        payload = hl_api._construct_subscription_payload("candle:ETH:1m")

        assert payload.method == "subscribe"
        assert payload.subscription.type == "candle"
        assert payload.subscription.coin == "ETH"
        assert payload.subscription.interval == "1m"

    def test_construct_all_mids_subscription_payload(self, hl_api: HyperliquidAPI) -> None:
        """Test constructing allMids subscription payload."""
        payload = hl_api._construct_subscription_payload("allMids")

        assert payload.method == "subscribe"
        assert payload.subscription.type == "allMids"

    def test_invalid_topic_raises_api_error(self, hl_api: HyperliquidAPI) -> None:
        """Test that invalid topic raises APIError."""
        with pytest.raises(APIError) as exc_info:
            hl_api._construct_subscription_payload("invalid_topic")

        error = exc_info.value
        assert error.code == APIErrorCode.INVALID_PARAMS.value
        assert "Unsupported WebSocket topic" in error.message
        assert "invalid_topic" in error.message
        assert "Supported formats" in error.message

    def test_malformed_topic_raises_api_error(self, hl_api: HyperliquidAPI) -> None:
        """Test that malformed topic raises APIError."""
        # L2Book without coin
        with pytest.raises(APIError) as exc_info:
            hl_api._construct_subscription_payload("l2Book")

        assert exc_info.value.code == APIErrorCode.INVALID_PARAMS.value

    def test_candle_without_interval_raises_api_error(self, hl_api: HyperliquidAPI) -> None:
        """Test that candle without interval raises APIError."""
        with pytest.raises(APIError) as exc_info:
            hl_api._construct_subscription_payload("candle:ETH")

        assert exc_info.value.code == APIErrorCode.INVALID_PARAMS.value

    @pytest.mark.asyncio
    async def test_subscribe_sends_payload(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that subscribe sends correct payload to WebSocketManager."""
        # Mock handler
        handler = AsyncMock()

        # Subscribe to L2Book
        await hl_api.subscribe("l2Book:ETH", handler)

        # Verify handler was registered
        assert "l2Book:ETH" in hl_api._ws_handlers
        assert hl_api._ws_handlers["l2Book:ETH"] == handler

        # Verify WebSocket message was sent
        mock_ws_manager.send_json.assert_called_once()
        sent_payload = mock_ws_manager.send_json.call_args[0][0]

        assert isinstance(sent_payload, HyperliquidRawWsSubscribeRequest)
        assert sent_payload.method == "subscribe"
        assert sent_payload.subscription.type == "l2Book"
        assert sent_payload.subscription.coin == "ETH"

    @pytest.mark.asyncio
    async def test_subscribe_with_invalid_topic_logs_warning(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that subscribe with invalid topic logs warning but doesn't crash."""
        handler = AsyncMock()

        # Subscribe to invalid topic
        await hl_api.subscribe("invalid:topic:format", handler)

        # Handler should still be registered
        assert "invalid:topic:format" in hl_api._ws_handlers

        # But no WebSocket message should be sent
        mock_ws_manager.send_json.assert_called_once()

    @pytest.mark.asyncio
    async def test_subscribe_when_not_connected(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test subscribe behavior when WebSocket is not connected."""
        # Set WebSocket as not connected
        mock_ws_manager.is_connected = False

        handler = AsyncMock()
        await hl_api.subscribe("l2Book:ETH", handler)

        # Handler should be registered
        assert "l2Book:ETH" in hl_api._ws_handlers

        # But no WebSocket message should be sent
        mock_ws_manager.send_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_resubscribe_on_reconnect(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that topics are resubscribed on reconnection."""
        # Register some handlers
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        hl_api._ws_handlers = {"l2Book:ETH": handler1, "trades:BTC": handler2}

        # Trigger resubscribe
        await hl_api._resubscribe()

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
    async def test_resubscribe_handles_errors_gracefully(
        self, hl_api: HyperliquidAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that resubscribe continues even if some topics fail."""
        # Register handlers including one that will fail
        hl_api._ws_handlers = {
            "l2Book:ETH": AsyncMock(),
            "userEvents": AsyncMock(),  # Will fail without wallet
            "trades:BTC": AsyncMock(),
        }
        hl_api._wallet_address = None  # Ensure userEvents will fail

        # Trigger resubscribe
        await hl_api._resubscribe()

        # Should still send 2 successful subscriptions
        assert mock_ws_manager.send_json.call_count == 2

        # Verify the successful subscriptions
        calls = mock_ws_manager.send_json.call_args_list
        payloads = [call[0][0] for call in calls]

        # Should have l2Book and trades but not userEvents
        types = [p.subscription.type for p in payloads]
        assert "l2Book" in types
        assert "trades" in types
        assert "userEvents" not in types
