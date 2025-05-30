"""
Tests for BackpackAPI WebSocket subscription functionality.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.backpack.models.bp_ws_payloads import BackpackRawWsSubscriptionRequest
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ExchangeSecrets
from cyberdelta.enums.exchange_names import ExchangeName


@pytest.fixture
def bp_config() -> ExchangeSpecificConfig:
    """Create test configuration for BackpackAPI."""
    config_dict = {
        "exchange_name": ExchangeName.BACKPACK,
        "symbols": {},  # Add required symbols field
        "api_base_url": "https://api.backpack.exchange",
        "ws_url": "wss://ws.backpack.exchange",
        "rate_limit_per_minute": 1200,
    }
    return ExchangeSpecificConfig.model_validate(config_dict)


@pytest.fixture
def bp_secrets() -> ExchangeSecrets:
    """Create test secrets for BackpackAPI."""
    return ExchangeSecrets(
        api_key=SecretStr("61D/XTRs1Es8SgdZN4xO438vv1ls0aWhJSs//JDNxLk="),
        api_secret=SecretStr("7s6pf6Xs8VJDMTNmcseiLge61XCSZeQ6GW8PP6odR1c="),
    )


@pytest.fixture
def mock_ws_manager() -> MagicMock:
    """Create a mock WebSocketManager."""
    manager = MagicMock()
    manager.is_connected = True
    manager.send_json = AsyncMock(return_value=True)
    return manager


@pytest.fixture
def bp_api(
    bp_config: ExchangeSpecificConfig, bp_secrets: ExchangeSecrets, mock_ws_manager: MagicMock
) -> BackpackAPI:
    """Create BackpackAPI instance with mocked dependencies."""
    with patch(
        "cyberdelta.apis.connectivity.ws_manager.WebSocketManager", return_value=mock_ws_manager
    ):
        api = BackpackAPI(exchange_config=bp_config, exchange_secrets=bp_secrets)
        api._ws_manager = mock_ws_manager
        return api


class TestBackpackAPIWsSubscriptions:
    """Test BackpackAPI WebSocket subscription methods."""

    def test_construct_public_stream_subscription(self, bp_api: BackpackAPI) -> None:
        """Test constructing public stream subscription payload."""
        payload = bp_api._construct_subscription_payload("ticker.BTC_USDC")

        assert isinstance(payload, BackpackRawWsSubscriptionRequest)
        assert payload.method == "SUBSCRIBE"
        assert payload.params == ["ticker.BTC_USDC"]
        assert payload.signature is None

    def test_construct_depth_subscription(self, bp_api: BackpackAPI) -> None:
        """Test constructing depth (order book) subscription."""
        payload = bp_api._construct_subscription_payload("depth.ETH_USDC")

        assert payload.method == "SUBSCRIBE"
        assert payload.params == ["depth.ETH_USDC"]
        assert payload.signature is None

    def test_construct_trades_subscription(self, bp_api: BackpackAPI) -> None:
        """Test constructing trades subscription."""
        payload = bp_api._construct_subscription_payload("trades.SOL_USDC")

        assert payload.method == "SUBSCRIBE"
        assert payload.params == ["trades.SOL_USDC"]
        assert payload.signature is None

    def test_construct_private_stream_subscription(self, bp_api: BackpackAPI) -> None:
        """Test constructing private stream subscription."""
        # Private streams start with "account."
        payload = bp_api._construct_subscription_payload("account.orderUpdate")

        assert payload.method == "SUBSCRIBE"
        assert payload.params == ["account.orderUpdate"]
        # account.* streams require authentication
        assert payload.signature is not None
        assert len(payload.signature) == 4  # (api_key, signature, timestamp, window)

    def test_construct_fills_subscription(self, bp_api: BackpackAPI) -> None:
        """Test constructing fills subscription."""
        payload = bp_api._construct_subscription_payload("fills")

        assert payload.method == "SUBSCRIBE"
        assert payload.params == ["fills"]
        # fills is a public stream, no signature required
        assert payload.signature is None

    def test_any_topic_format_accepted(self, bp_api: BackpackAPI) -> None:
        """Test that any topic format is accepted (no validation in Backpack)."""
        # Unlike Hyperliquid, Backpack doesn't validate topic format
        payload = bp_api._construct_subscription_payload("custom.stream.format")

        assert payload.method == "SUBSCRIBE"
        assert payload.params == ["custom.stream.format"]

    @pytest.mark.asyncio
    async def test_subscribe_sends_payload(
        self, bp_api: BackpackAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that subscribe sends correct payload to WebSocketManager."""
        handler = AsyncMock()

        # Subscribe to ticker
        await bp_api.subscribe("ticker.BTC_USDC", handler)

        # Verify handler was registered
        assert "ticker.BTC_USDC" in bp_api._ws_handlers
        assert bp_api._ws_handlers["ticker.BTC_USDC"] == handler

        # Verify WebSocket message was sent
        mock_ws_manager.send_json.assert_called_once()
        sent_payload = mock_ws_manager.send_json.call_args[0][0]

        assert isinstance(sent_payload, BackpackRawWsSubscriptionRequest)
        assert sent_payload.method == "SUBSCRIBE"
        assert sent_payload.params == ["ticker.BTC_USDC"]

    @pytest.mark.asyncio
    async def test_subscribe_multiple_topics(
        self, bp_api: BackpackAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test subscribing to multiple topics."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        handler3 = AsyncMock()

        await bp_api.subscribe("ticker.BTC_USDC", handler1)
        await bp_api.subscribe("depth.ETH_USDC", handler2)
        await bp_api.subscribe("trades.SOL_USDC", handler3)

        # All handlers should be registered
        assert len(bp_api._ws_handlers) == 3
        assert bp_api._ws_handlers["ticker.BTC_USDC"] == handler1
        assert bp_api._ws_handlers["depth.ETH_USDC"] == handler2
        assert bp_api._ws_handlers["trades.SOL_USDC"] == handler3

        # Three messages should be sent
        assert mock_ws_manager.send_json.call_count == 3

    @pytest.mark.asyncio
    async def test_subscribe_when_not_connected(
        self, bp_api: BackpackAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test subscribe behavior when WebSocket is not connected."""
        mock_ws_manager.is_connected = False

        handler = AsyncMock()
        await bp_api.subscribe("ticker.BTC_USDC", handler)

        # Handler should be registered
        assert "ticker.BTC_USDC" in bp_api._ws_handlers

        # But no WebSocket message should be sent
        mock_ws_manager.send_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_resubscribe_on_reconnect(
        self, bp_api: BackpackAPI, mock_ws_manager: MagicMock
    ) -> None:
        """Test that topics are resubscribed on reconnection."""
        # Register some handlers
        bp_api._ws_handlers = {
            "ticker.BTC_USDC": AsyncMock(),
            "depth.ETH_USDC": AsyncMock(),
            "account.orderUpdate": AsyncMock(),
        }

        # Trigger resubscribe
        await bp_api._resubscribe()

        # Verify all subscriptions were sent
        assert mock_ws_manager.send_json.call_count == 3

        # Check the payloads
        calls = mock_ws_manager.send_json.call_args_list
        sent_topics: list[str] = []
        for call in calls:
            payload = call[0][0]
            assert isinstance(payload, BackpackRawWsSubscriptionRequest)
            assert payload.method == "SUBSCRIBE"
            sent_topics.extend(payload.params)

        assert "ticker.BTC_USDC" in sent_topics
        assert "depth.ETH_USDC" in sent_topics
        assert "account.orderUpdate" in sent_topics

    @pytest.mark.asyncio
    async def test_subscribe_to_helper_methods(self, bp_api: BackpackAPI) -> None:
        """Test the helper subscription methods."""
        # These methods just prepare the topic, actual subscription is done separately

        # Test subscribe_to_order_book
        await bp_api.subscribe_to_order_book("ETH_USDC")
        # Should not actually subscribe yet (no handler provided)
        assert "depth.ETH_USDC" not in bp_api._ws_handlers

        # Test subscribe_to_ticker
        await bp_api.subscribe_to_ticker("BTC_USDC")
        assert "ticker.BTC_USDC" not in bp_api._ws_handlers

        # Test subscribe_to_trades
        await bp_api.subscribe_to_trades("SOL_USDC")
        assert "trades.SOL_USDC" not in bp_api._ws_handlers

        # Test subscribe_to_account_updates
        await bp_api.subscribe_to_account_updates()
        assert "fills" not in bp_api._ws_handlers
        assert "orders" not in bp_api._ws_handlers

    def test_payload_serialization(self, bp_api: BackpackAPI) -> None:
        """Test that payload is correctly serialized."""
        payload = bp_api._construct_subscription_payload("ticker.BTC_USDC")

        # Test serialization with by_alias and exclude_none
        data = payload.model_dump(by_alias=True, exclude_none=True)

        assert data == {"method": "SUBSCRIBE", "params": ["ticker.BTC_USDC"]}
        # signature should not be included when None

    def test_private_stream_signature_generation(self, bp_api: BackpackAPI) -> None:
        """Test that private stream subscription generates signature properly."""
        payload = bp_api._construct_subscription_payload("account.orderUpdate")

        # Payload should be created with signature
        assert payload is not None
        assert payload.params == ["account.orderUpdate"]
        assert payload.signature is not None
        assert len(payload.signature) == 4  # (api_key, signature, timestamp, window)
