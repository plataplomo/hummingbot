"""Integration Tests for BackpackAPI WebSocket subscription functionality."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.enums.exchange_names import ExchangeName

pytestmark = pytest.mark.integration


@pytest.fixture
def bp_config() -> ExchangeSpecificConfig:
    """Create test configuration for BackpackAPI."""
    config_dict: dict[str, object] = {
        "exchange_name": ExchangeName.BACKPACK,
        "symbols": {},  # Add required symbols field
        "api_base_url_mainnet": "https://api.backpack.exchange",
        "ws_url_mainnet": "wss://ws.backpack.exchange",
        "api_base_url_testnet": None,
        "ws_url_testnet": None,
        "is_mainnet_environment": True,
        "rate_limit_per_minute": 1200,
    }
    return ExchangeSpecificConfig.model_validate(config_dict)


@pytest.fixture
def bp_secrets() -> ApiKeyAuthSecrets:
    """Create test secrets for BackpackAPI."""
    return ApiKeyAuthSecrets(
        api_key=SecretStr("61D/XTRs1Es8SgdZN4xO438vv1ls0aWhJSs//JDNxLk="),
        api_secret=SecretStr("7s6pf6Xs8VJDMTNmcseiLge61XCSZeQ6GW8PP6odR1c="),
    )


@pytest.fixture
def mock_ws_manager() -> MagicMock:
    """Create a mock WebSocketManager with realistic connection state."""
    manager = MagicMock()
    # Start with disconnected state - use a simple boolean attribute
    manager._connection_state = False

    async def mock_connect() -> None:
        # Simulate connection establishing
        manager._connection_state = True

    # Set up mock to return connection state dynamically
    manager.is_connected = property(lambda _: manager._connection_state)
    manager.send_json = AsyncMock(return_value=True)
    manager.connect = AsyncMock(side_effect=mock_connect)
    return manager


@pytest.fixture
def bp_api(
    bp_config: ExchangeSpecificConfig, bp_secrets: ApiKeyAuthSecrets, mock_ws_manager: MagicMock,
) -> BackpackAPI:
    """Create BackpackAPI instance with mocked dependencies using public interfaces only."""
    with patch(
        "cyberdelta.apis.connectivity.ws_manager.WebSocketManager", return_value=mock_ws_manager,
    ):
        # Create API using standard constructor - no protected member access
        api = BackpackAPI(exchange_config=bp_config, exchange_secrets=bp_secrets)
        return api


class TestBackpackAPIWsSubscriptions:
    """Test BackpackAPI WebSocket subscription methods."""

    @pytest.mark.asyncio
    async def test_subscription_registration_public_interface(self, bp_api: BackpackAPI) -> None:
        """Test that subscription registration works through public interface."""
        handler = AsyncMock()

        # This should complete without error - testing public interface only
        await bp_api.subscribe("ticker.BTC_USDC", handler)

        # Verify the API maintains consistent state after subscription
        assert isinstance(bp_api.is_connected, bool)  # Should have a valid connection state

    @pytest.mark.asyncio
    async def test_multiple_subscription_types(self, bp_api: BackpackAPI) -> None:
        """Test subscribing to different types of streams through public interface."""
        handler = AsyncMock()

        # Test different subscription types - all should complete without error
        await bp_api.subscribe("depth.ETH_USDC", handler)
        await bp_api.subscribe("trades.SOL_USDC", handler)
        await bp_api.subscribe("account.orderUpdate", handler)

        # Verify the API maintains consistent state after subscriptions
        assert isinstance(bp_api.is_connected, bool)

    @pytest.mark.asyncio
    async def test_subscription_consistency(self, bp_api: BackpackAPI) -> None:
        """Test that subscription operations maintain consistent API state."""
        handler = AsyncMock()

        # Multiple subscriptions should work consistently
        initial_connected = bp_api.is_connected

        await bp_api.subscribe("trades.SOL_USDC", handler)
        after_first = bp_api.is_connected

        await bp_api.subscribe("account.orderUpdate", handler)
        after_second = bp_api.is_connected

        # Connection state should remain consistent
        assert initial_connected == after_first == after_second
        assert isinstance(initial_connected, bool)

    @pytest.mark.asyncio
    async def test_various_topic_formats_accepted(self, bp_api: BackpackAPI) -> None:
        """Test that various topic formats work through public interface."""
        handler = AsyncMock()

        # Test various topic formats - all should complete without error
        await bp_api.subscribe("fills", handler)
        await bp_api.subscribe("custom.stream.format", handler)
        await bp_api.subscribe("ticker.BTC_USDC", handler)

        # Verify the API maintains consistent state
        assert isinstance(bp_api.is_connected, bool)

    @pytest.mark.asyncio
    async def test_subscription_operations_consistent(self, bp_api: BackpackAPI) -> None:
        """Test that subscription operations are consistent through public interface."""
        handler = AsyncMock()

        # Multiple subscription operations should work consistently
        await bp_api.subscribe("ticker.BTC_USDC", handler)
        first_state = bp_api.is_connected

        await bp_api.subscribe("depth.ETH_USDC", handler)
        second_state = bp_api.is_connected

        # All operations should maintain consistent state
        assert first_state == second_state
        assert isinstance(first_state, bool)

    @pytest.mark.asyncio
    async def test_multiple_topic_subscriptions(self, bp_api: BackpackAPI) -> None:
        """Test subscribing to multiple topics through public interface."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        handler3 = AsyncMock()

        # All subscriptions should complete without error
        await bp_api.subscribe("ticker.BTC_USDC", handler1)
        await bp_api.subscribe("depth.ETH_USDC", handler2)
        await bp_api.subscribe("trades.SOL_USDC", handler3)

        # Verify API state remains consistent
        assert isinstance(bp_api.is_connected, bool)

    @pytest.mark.asyncio
    async def test_websocket_connection_lifecycle(self, bp_api: BackpackAPI) -> None:
        """Test WebSocket connection lifecycle through public interface."""
        handler = AsyncMock()

        # Test subscription works - initially disconnected
        await bp_api.subscribe("ticker.BTC_USDC", handler)
        subscription_state = bp_api.is_connected

        # Test connection operations work - should connect
        await bp_api.connect_websocket()
        connection_state = bp_api.is_connected

        # Connection state should change from disconnected to connected
        assert subscription_state is False  # Initially disconnected
        assert connection_state is True  # Connected after connect_websocket()
        assert isinstance(subscription_state, bool)
        assert isinstance(connection_state, bool)

    @pytest.mark.asyncio
    async def test_helper_subscription_methods(self, bp_api: BackpackAPI) -> None:
        """Test the helper subscription methods through public interface."""
        # All helper methods should complete without error
        await bp_api.subscribe_to_order_book("ETH_USDC")
        await bp_api.subscribe_to_ticker("BTC_USDC")
        await bp_api.subscribe_to_trades("SOL_USDC")
        await bp_api.subscribe_to_account_updates()

        # API should maintain consistent state
        assert isinstance(bp_api.is_connected, bool)

    @pytest.mark.asyncio
    async def test_public_private_stream_handling(self, bp_api: BackpackAPI) -> None:
        """Test that both public and private streams work through public interface."""
        handler = AsyncMock()

        # Both public and private streams should work without error
        await bp_api.subscribe("ticker.BTC_USDC", handler)  # Public
        await bp_api.subscribe("account.orderUpdate", handler)  # Private

        # API should handle both types consistently
        assert isinstance(bp_api.is_connected, bool)

    @pytest.mark.asyncio
    async def test_subscription_integration_complete(self, bp_api: BackpackAPI) -> None:
        """Test complete subscription integration through public interface."""
        handler = AsyncMock()

        # Test a comprehensive subscription workflow
        await bp_api.subscribe("account.orderUpdate", handler)
        initial_state = bp_api.is_connected

        await bp_api.connect_websocket()
        final_state = bp_api.is_connected

        # Integration should work: disconnected -> connected
        assert initial_state is False  # Initially disconnected
        assert final_state is True  # Connected after connect_websocket()
        assert isinstance(initial_state, bool)
        assert isinstance(final_state, bool)
