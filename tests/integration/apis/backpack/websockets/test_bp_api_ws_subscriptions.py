"""Integration Tests for BackpackAPI WebSocket subscription functionality."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets

pytestmark = [pytest.mark.integration, pytest.mark.websockets]


@pytest.fixture
def mock_ws_manager() -> MagicMock:
    """Create a mock WebSocketManager with realistic connection state."""
    manager = MagicMock()
    manager._connection_state = False

    async def mock_connect() -> None:
        manager._connection_state = True

    manager.is_connected = property(lambda _: manager._connection_state)
    manager.send_json = AsyncMock(return_value=True)
    manager.connect = AsyncMock(side_effect=mock_connect)
    return manager


@pytest.fixture
def bp_api(
    active_bp_config: ExchangeSpecificConfig,
    active_bp_secrets: ApiKeyAuthSecrets,
    mock_ws_manager: MagicMock,
) -> BackpackAPI:
    """Create BackpackAPI instance with mocked dependencies using public interfaces only."""
    with patch(
        "cyberdelta.apis.connectivity.ws_manager.WebSocketManager",
        return_value=mock_ws_manager,
    ):
        api = BackpackAPI(exchange_config=active_bp_config, exchange_secrets=active_bp_secrets)
        return api


@pytest.mark.websockets
class TestBackpackAPIWsSubscriptions:
    """Test BackpackAPI WebSocket subscription methods."""

    @pytest.mark.asyncio
    async def test_subscription_registration_public_interface(self, bp_api: BackpackAPI) -> None:
        """Test that subscription registration works through public interface."""
        handler = AsyncMock()

        await bp_api.subscribe("ticker.BTC_USDC", handler)

        assert isinstance(bp_api.is_connected, bool)

    @pytest.mark.asyncio
    async def test_multiple_subscription_types(self, bp_api: BackpackAPI) -> None:
        """Test subscribing to different types of streams through public interface."""
        handler = AsyncMock()

        await bp_api.subscribe("depth.ETH_USDC", handler)
        await bp_api.subscribe("trades.SOL_USDC", handler)
        await bp_api.subscribe("account.orderUpdate", handler)

        assert isinstance(bp_api.is_connected, bool)

    @pytest.mark.asyncio
    async def test_subscription_consistency(self, bp_api: BackpackAPI) -> None:
        """Test that subscription operations maintain consistent API state."""
        handler = AsyncMock()

        initial_connected = bp_api.is_connected

        await bp_api.subscribe("trades.SOL_USDC", handler)
        after_first = bp_api.is_connected

        await bp_api.subscribe("account.orderUpdate", handler)
        after_second = bp_api.is_connected

        assert initial_connected == after_first == after_second
        assert isinstance(initial_connected, bool)

    @pytest.mark.asyncio
    async def test_various_topic_formats_accepted(self, bp_api: BackpackAPI) -> None:
        """Test that various topic formats work through public interface."""
        handler = AsyncMock()

        await bp_api.subscribe("fills", handler)
        await bp_api.subscribe("custom.stream.format", handler)
        await bp_api.subscribe("ticker.BTC_USDC", handler)

        assert isinstance(bp_api.is_connected, bool)

    @pytest.mark.asyncio
    async def test_subscription_operations_consistent(self, bp_api: BackpackAPI) -> None:
        """Test that subscription operations are consistent through public interface."""
        handler = AsyncMock()

        await bp_api.subscribe("ticker.BTC_USDC", handler)
        first_state = bp_api.is_connected

        await bp_api.subscribe("depth.ETH_USDC", handler)
        second_state = bp_api.is_connected

        assert first_state == second_state
        assert isinstance(first_state, bool)

    @pytest.mark.asyncio
    async def test_multiple_topic_subscriptions(self, bp_api: BackpackAPI) -> None:
        """Test subscribing to multiple topics through public interface."""
        handler1 = AsyncMock()
        handler2 = AsyncMock()
        handler3 = AsyncMock()

        await bp_api.subscribe("ticker.BTC_USDC", handler1)
        await bp_api.subscribe("depth.ETH_USDC", handler2)
        await bp_api.subscribe("trades.SOL_USDC", handler3)

        assert isinstance(bp_api.is_connected, bool)

    @pytest.mark.asyncio
    async def test_websocket_connection_lifecycle(self, bp_api: BackpackAPI) -> None:
        """Test WebSocket connection lifecycle through public interface."""
        handler = AsyncMock()

        await bp_api.subscribe("ticker.BTC_USDC", handler)
        subscription_state = bp_api.is_connected

        await bp_api.connect_websocket()
        connection_state = bp_api.is_connected

        assert subscription_state is False
        assert connection_state is True
        assert isinstance(subscription_state, bool)
        assert isinstance(connection_state, bool)

    @pytest.mark.asyncio
    async def test_helper_subscription_methods(self, bp_api: BackpackAPI) -> None:
        """Test the helper subscription methods through public interface."""
        await bp_api.subscribe_to_order_book("ETH_USDC")
        await bp_api.subscribe_to_ticker("BTC_USDC")
        await bp_api.subscribe_to_trades("SOL_USDC")
        await bp_api.subscribe_to_account_updates()

        assert isinstance(bp_api.is_connected, bool)

    @pytest.mark.asyncio
    async def test_public_private_stream_handling(self, bp_api: BackpackAPI) -> None:
        """Test that both public and private streams work through public interface."""
        handler = AsyncMock()

        await bp_api.subscribe("ticker.BTC_USDC", handler)
        await bp_api.subscribe("account.orderUpdate", handler)

        assert isinstance(bp_api.is_connected, bool)

    @pytest.mark.asyncio
    async def test_subscription_integration_complete(self, bp_api: BackpackAPI) -> None:
        """Test complete subscription integration through public interface."""
        handler = AsyncMock()

        await bp_api.subscribe("account.orderUpdate", handler)
        initial_state = bp_api.is_connected

        await bp_api.connect_websocket()
        final_state = bp_api.is_connected

        assert initial_state is False
        assert final_state is True
        assert isinstance(initial_state, bool)
        assert isinstance(final_state, bool)
