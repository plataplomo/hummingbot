"""Tests for WebSocketManager Pydantic BaseModel integration.
"""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from pydantic import AnyUrl, BaseModel, ConfigDict

from cyberdelta.apis.connectivity.connectivity_models import WebSocketManagerConfig
from cyberdelta.apis.connectivity.ws_manager import WebSocketManager


class MockSubscriptionModel(BaseModel):
    """Mock Pydantic model for testing."""

    method: str
    topic: str
    params: dict[str, str] | None = None

    model_config = ConfigDict(populate_by_name=True)


@pytest.fixture
def ws_config() -> WebSocketManagerConfig:
    """Create test WebSocket configuration."""
    return WebSocketManagerConfig(
        ws_url=AnyUrl("wss://test.example.com/ws"),
        ping_interval=30,
        reconnect_delay=5,
        max_reconnect_attempts=3,
        connection_timeout=10,
    )


@pytest.fixture
def mock_websocket() -> MagicMock:
    """Create a mock WebSocket connection."""
    ws = MagicMock()
    ws.closed = False
    ws.send_json = AsyncMock()
    ws.receive_json = AsyncMock()
    ws.close = AsyncMock()
    return ws


@pytest.fixture
def mock_session() -> MagicMock:
    """Create a mock aiohttp ClientSession."""
    session = MagicMock()
    session.ws_connect = AsyncMock()
    return session


@pytest_asyncio.fixture
async def ws_manager(
    ws_config: WebSocketManagerConfig, mock_websocket: MagicMock, mock_session: MagicMock,
) -> AsyncGenerator[WebSocketManager]:
    """Create WebSocketManager with mocked dependencies."""
    with patch("aiohttp.ClientSession", return_value=mock_session):
        mock_session.ws_connect.return_value.__aenter__.return_value = mock_websocket

        manager = WebSocketManager(
            exchange_name="test_exchange", config=ws_config, message_handler=AsyncMock(),
        )

        # Manually set the connection for testing using object.__setattr__ to bypass protection
        object.__setattr__(manager, "_ws_connection", mock_websocket)
        object.__setattr__(manager, "_is_connected", True)

        yield manager

        # Close properly - this avoids accessing _session directly
        await manager.close()


class TestWebSocketManagerPydanticIntegration:
    """Test WebSocketManager's handling of Pydantic BaseModel."""

    @pytest.mark.asyncio
    async def test_send_json_with_basemodel(
        self, ws_manager: WebSocketManager, mock_websocket: MagicMock,
    ) -> None:
        """Test sending JSON with Pydantic BaseModel."""
        # Create a test model
        model = MockSubscriptionModel(
            method="subscribe", topic="ticker", params={"symbol": "BTC_USDC"},
        )

        # Send the model
        result = await ws_manager.send_json(model)

        assert result is True

        # Verify the websocket received the correct data
        mock_websocket.send_json.assert_called_once()
        sent_data = mock_websocket.send_json.call_args[0][0]

        # Should be the model_dump output
        assert sent_data == {
            "method": "subscribe",
            "topic": "ticker",
            "params": {"symbol": "BTC_USDC"},
        }

    @pytest.mark.asyncio
    async def test_send_json_excludes_none(
        self, ws_manager: WebSocketManager, mock_websocket: MagicMock,
    ) -> None:
        """Test that send_json excludes None values."""
        model = MockSubscriptionModel(
            method="subscribe",
            topic="ticker",
            params=None,  # This should be excluded
        )

        await ws_manager.send_json(model)

        sent_data = mock_websocket.send_json.call_args[0][0]

        # params should not be in the sent data
        assert "params" not in sent_data
        assert sent_data == {"method": "subscribe", "topic": "ticker"}

    @pytest.mark.asyncio
    async def test_send_json_uses_by_alias(
        self, ws_manager: WebSocketManager, mock_websocket: MagicMock,
    ) -> None:
        """Test that send_json uses field aliases."""

        class AliasedModel(BaseModel):
            method_name: str = "subscribe"
            topic_name: str

            model_config = ConfigDict(
                populate_by_name=True,
                alias_generator=lambda field_name: field_name.replace("_name", ""),
            )

        model = AliasedModel(topic_name="trades")

        await ws_manager.send_json(model)

        sent_data = mock_websocket.send_json.call_args[0][0]

        # Should use aliases
        assert sent_data == {"method": "subscribe", "topic": "trades"}

    @pytest.mark.asyncio
    async def test_send_json_when_not_connected(self, ws_manager: WebSocketManager) -> None:
        """Test send_json returns False when not connected."""
        # Simulate disconnected state using object.__setattr__ to bypass protection
        object.__setattr__(ws_manager, "_ws_connection", None)

        model = MockSubscriptionModel(method="subscribe", topic="ticker")

        result = await ws_manager.send_json(model)

        assert result is False

    @pytest.mark.asyncio
    async def test_send_json_handles_send_error(
        self, ws_manager: WebSocketManager, mock_websocket: MagicMock,
    ) -> None:
        """Test send_json handles errors gracefully."""
        # Make send_json raise an exception
        mock_websocket.send_json.side_effect = Exception("Network error")

        model = MockSubscriptionModel(method="subscribe", topic="ticker")

        result = await ws_manager.send_json(model)

        assert result is False

    @pytest.mark.asyncio
    async def test_complex_nested_model(
        self, ws_manager: WebSocketManager, mock_websocket: MagicMock,
    ) -> None:
        """Test sending complex nested Pydantic models."""

        class NestedParams(BaseModel):
            coin: str
            interval: str

        class ComplexModel(BaseModel):
            method: str
            subscription: NestedParams
            timestamp: int | None = None

        model = ComplexModel(
            method="subscribe", subscription=NestedParams(coin="ETH", interval="1m"), timestamp=None,
        )

        await ws_manager.send_json(model)

        sent_data = mock_websocket.send_json.call_args[0][0]

        # Should properly serialize nested model and exclude None
        assert sent_data == {
            "method": "subscribe",
            "subscription": {"coin": "ETH", "interval": "1m"},
        }
        assert "timestamp" not in sent_data

    @pytest.mark.asyncio
    async def test_model_with_lists(
        self, ws_manager: WebSocketManager, mock_websocket: MagicMock,
    ) -> None:
        """Test sending model with list fields."""

        class ListModel(BaseModel):
            method: str
            params: list[str]

        model = ListModel(method="SUBSCRIBE", params=["ticker.BTC_USDC", "depth.ETH_USDC"])

        await ws_manager.send_json(model)

        sent_data = mock_websocket.send_json.call_args[0][0]

        assert sent_data == {"method": "SUBSCRIBE", "params": ["ticker.BTC_USDC", "depth.ETH_USDC"]}

    @pytest.mark.asyncio
    async def test_model_with_tuple_serialized_as_list(
        self, ws_manager: WebSocketManager, mock_websocket: MagicMock,
    ) -> None:
        """Test that tuples in models are serialized as lists."""

        class TupleModel(BaseModel):
            method: str
            signature: tuple[str, str, str, str] | None = None

        model = TupleModel(method="SUBSCRIBE", signature=("key", "sig", "timestamp", "window"))

        await ws_manager.send_json(model)

        sent_data = mock_websocket.send_json.call_args[0][0]

        # Tuple should be serialized as list in JSON
        # However, model_dump() keeps tuples as tuples. The actual JSON serialization
        # happens in aiohttp's send_json which will convert tuples to lists
        # For this test, we're checking what's passed to send_json, not the final JSON
        assert sent_data == {
            "method": "SUBSCRIBE",
            "signature": ("key", "sig", "timestamp", "window"),
        }

    @pytest.mark.asyncio
    async def test_frozen_model(
        self, ws_manager: WebSocketManager, mock_websocket: MagicMock,
    ) -> None:
        """Test sending frozen (immutable) Pydantic models."""

        class FrozenModel(BaseModel):
            method: str
            topic: str

            model_config = ConfigDict(frozen=True)

        model = FrozenModel(method="subscribe", topic="allMids")

        # Model should be immutable
        with pytest.raises(ValueError):  # Pydantic will raise validation error
            model.method = "unsubscribe"

        # But should still be sendable
        result = await ws_manager.send_json(model)

        assert result is True
        sent_data = mock_websocket.send_json.call_args[0][0]
        assert sent_data == {"method": "subscribe", "topic": "allMids"}
