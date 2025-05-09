import asyncio
import json
from collections.abc import AsyncGenerator, Coroutine
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import pytest_asyncio
from pydantic import AnyUrl, ValidationError

from cyberdelta.apis.connectivity.connectivity_models import WebSocketManagerConfig
from cyberdelta.apis.connectivity.ws_manager import (
    WebSocketManager,
)


# Define a dummy message handler for tests
async def dummy_message_handler(message: dict[str, Any]) -> None:
    pass


async def dummy_on_connected_callback() -> None:
    pass


@pytest.fixture
def default_ws_manager_config() -> WebSocketManagerConfig:
    """Provides a default WebSocketManagerConfig for tests."""
    return WebSocketManagerConfig(
        ws_url=AnyUrl("ws://test.websocket.api/ws"),
        connection_timeout=10.0,
        max_reconnect_attempts=2,
        reconnect_delay=0.1,
        ping_interval=30.0,
    )


@pytest_asyncio.fixture
async def ws_manager_instance(
    default_ws_manager_config: WebSocketManagerConfig,
) -> AsyncGenerator[WebSocketManager]:
    """Provides a WebSocketManager instance for testing."""
    manager = WebSocketManager(
        exchange_name="test_exchange_ws",
        message_handler=dummy_message_handler,
        config=default_ws_manager_config,
        on_connected_callback=dummy_on_connected_callback,
        session=None,  # Ensures internal session management
    )
    yield manager
    await manager.close()


class TestWebSocketManager:
    """Tests for the WebSocketManager class."""

    @pytest.mark.asyncio
    async def test_connect_returns_task_and_idempotency(
        self,
        ws_manager_instance: WebSocketManager,
    ) -> None:
        """Test connect() returns a task and is idempotent while task is running."""
        establish_conn_coro_mock = AsyncMock(return_value=None)

        with patch.object(
            ws_manager_instance, "_establish_connection", side_effect=establish_conn_coro_mock
        ) as mock_establish_connection_method:
            connection_task = ws_manager_instance.connect()
            assert connection_task is not None
            assert isinstance(connection_task, asyncio.Task)

            coro = connection_task.get_coro()
            assert coro is not None
            # Check if the task is running the correct coroutine object from the instance
            # This assertion verifies that connect() is indeed starting _establish_connection.
            # Accessing __qualname__ is to ensure it's the method from the instance.
            assert coro.__qualname__ == mock_establish_connection_method.__qualname__, (
                "Task is not running the mocked _establish_connection method"
            )

            same_task = ws_manager_instance.connect()
            assert same_task is connection_task

            await asyncio.sleep(0)  # Allow the mocked task to run
            mock_establish_connection_method.assert_called_once()

            if not connection_task.done():
                connection_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await connection_task

            establish_conn_coro_mock_2 = AsyncMock(return_value=None)
            with patch.object(
                ws_manager_instance, "_establish_connection", side_effect=establish_conn_coro_mock_2
            ) as mock_establish_connection_method_2:
                new_connection_task = ws_manager_instance.connect()
                assert new_connection_task is not None
                assert new_connection_task is not connection_task
                await asyncio.sleep(0)
                mock_establish_connection_method_2.assert_called_once()
                if not new_connection_task.done():
                    new_connection_task.cancel()
                    await asyncio.gather(new_connection_task, return_exceptions=True)

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_connection_success_flow(
        self,
        mock_ws_connect: AsyncMock,
        ws_manager_instance: WebSocketManager,
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test the full flow of a successful connection and starting internal tasks."""
        mock_ws_response = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        mock_ws_response.closed = False
        mock_ws_connect.return_value = mock_ws_response

        # Mock internal methods that would be called upon successful connection
        mock_listen_method = AsyncMock()
        mock_keep_alive_method = AsyncMock()
        mock_on_connected_cb = AsyncMock()

        # Assign mock for test to control callback behavior. This is a test-specific setup.
        ws_manager_instance._on_connected_callback = mock_on_connected_cb  # pyright: ignore [reportPrivateUsage]

        with (
            patch.object(ws_manager_instance, "_logger") as mock_logger,
            patch.object(ws_manager_instance, "_listen", side_effect=mock_listen_method) as _,
            patch.object(
                ws_manager_instance, "_keep_alive", side_effect=mock_keep_alive_method
            ) as _,
        ):
            connection_task = ws_manager_instance.connect()
            assert connection_task is not None
            await connection_task

            assert ws_manager_instance.is_connected is True
            expected_heartbeat = (
                default_ws_manager_config.ping_interval * WebSocketManager.HEARTBEAT_FACTOR
                if default_ws_manager_config.ping_interval > 0
                else 0
            )
            # Note: ws_manager.py timeout is float + ignore, test expects ClientTimeout object
            # This is fine as ws_connect itself expects ClientTimeout.
            mock_ws_connect.assert_called_once_with(
                str(default_ws_manager_config.ws_url),
                heartbeat=expected_heartbeat,
                timeout=default_ws_manager_config.connection_timeout,
            )
            mock_logger.info.assert_any_call(
                f"Successfully connected to {default_ws_manager_config.ws_url}."
            )
            mock_on_connected_cb.assert_called_once()
            mock_listen_method.assert_called_once()
            if default_ws_manager_config.ping_interval > 0:
                mock_keep_alive_method.assert_called_once()
            else:
                mock_keep_alive_method.assert_not_called()

    @pytest.mark.asyncio
    @patch(
        "aiohttp.ClientSession.ws_connect",
    )
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_connection_failure_retries_and_gives_up(
        self,
        mock_sleep: AsyncMock,  # noqa: F841 - asyncio.sleep is patched
        mock_ws_connect: AsyncMock,
        default_ws_manager_config: WebSocketManagerConfig,
        # Loop needed for new manager's internal session creation/closure if not externally managed
        event_loop: asyncio.AbstractEventLoop,  # pyright: ignore [reportUnusedVariable]
    ) -> None:
        """Test connection retries on failure and eventually gives up."""
        mock_ws_connect.side_effect = aiohttp.ClientConnectorError(
            MagicMock(), OSError("Connection failed")
        )

        # Create a config with minimal retries for this test
        retry_test_config = default_ws_manager_config.model_copy(
            update={"max_reconnect_attempts": 1, "reconnect_delay": 0.01}
        )
        # Instantiate a new manager with this specific config
        # This manager will use its own internal session
        retry_manager = WebSocketManager(
            exchange_name="retry_test_ws",
            message_handler=dummy_message_handler,
            config=retry_test_config,
        )

        try:
            with patch.object(retry_manager, "_logger") as mock_logger:
                connection_task = retry_manager.connect()
                assert connection_task is not None
                await connection_task

            assert retry_manager.is_connected is False
            # Total attempts = initial_attempt (1) + max_reconnect_attempts
            assert mock_ws_connect.call_count == retry_test_config.max_reconnect_attempts + 1
            assert mock_sleep.call_count == retry_test_config.max_reconnect_attempts

            mock_logger.critical.assert_called_once()
            assert (
                f"Failed to connect to {retry_test_config.ws_url} after "
                f"{retry_test_config.max_reconnect_attempts} attempts. Giving up."
            ) in mock_logger.critical.call_args[0][0]
        finally:
            await retry_manager.close()  # Ensure cleanup for the locally created manager

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    @patch("cyberdelta.apis.connectivity.ws_manager.asyncio.create_task")
    async def test_close_cancels_tasks_and_closes_connection_session(
        self,
        mock_create_task: MagicMock,
        mock_ws_connect: AsyncMock,
        ws_manager_instance: WebSocketManager,
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test close() cancels internal tasks and closes WS connection and session."""
        mock_ws_response = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        mock_ws_response.closed = False
        mock_ws_connect.return_value = mock_ws_response

        created_tasks_map: dict[str, asyncio.Task[Any]] = {}
        original_create_task = asyncio.create_task

        def side_effect_create_task(
            coro: Coroutine[Any, Any, None], *, name: str | None = None
        ) -> asyncio.Task[None]:
            task = original_create_task(coro, name=name)
            if name:
                created_tasks_map[name] = task
            return task

        mock_create_task.side_effect = side_effect_create_task

        with patch.object(ws_manager_instance, "_on_connected_callback", AsyncMock()):
            connection_establishment_task = ws_manager_instance.connect()
            assert connection_establishment_task is not None
            await connection_establishment_task

        assert ws_manager_instance.is_connected is True
        # Construct expected task names using the known exchange_name from the fixture
        exchange_name_for_test = "test_exchange_ws"  # From ws_manager_instance fixture
        listener_task_name = f"{exchange_name_for_test}_ws_listen"
        ping_task_name = f"{exchange_name_for_test}_ws_ping"

        listener_task = created_tasks_map.get(listener_task_name)
        ping_task = created_tasks_map.get(ping_task_name)

        assert listener_task is not None and not listener_task.done()
        # Use the known config from the fixture to check ping_interval condition
        if default_ws_manager_config.ping_interval > 0:
            assert ping_task is not None and not ping_task.done()

        # To verify internal session state without direct access to _session or _external_session:
        # 1. Rely on mock_ws_response.close.assert_called_once() for WS object closure.
        # 2. Rely on logging for internal ClientSession closure.
        # The direct checks for _session and _external_session were too invasive.

        with patch.object(ws_manager_instance, "_logger") as mock_logger_close:
            await ws_manager_instance.close()

        assert ws_manager_instance.is_connected is False
        # The following assertion is logically reachable and important for verifying
        # that the underlying WebSocket connection object itself was closed.
        # Mypy sometimes flags complex mocking scenarios as unreachable.
        mock_ws_response.close.assert_called_once()  # type: ignore[unreachable]

        if listener_task:
            assert listener_task.cancelled()
        if ping_task:
            assert ping_task.cancelled()

        assert connection_establishment_task.done()
        # Check if the internal session was logged as closed
        # This assumes ws_manager_instance created with session=None (uses internal session),
        # which is true for the fixture.
        mock_logger_close.info.assert_any_call("Internally created ClientSession closed.")
        # Direct _session check removed (avoid SLF001/reportPrivateUsage), relying on log.

    @pytest.mark.asyncio
    async def test_close_when_not_connected_closes_idle_internal_session(
        self,
        default_ws_manager_config: WebSocketManagerConfig,
        event_loop: asyncio.AbstractEventLoop,  # pyright: ignore [reportUnusedVariable]
    ) -> None:
        """Test close() on a never-connected manager with an internal session closes it."""
        # Patch aiohttp.ClientSession to control its creation and capture the instance
        with patch(
            "cyberdelta.apis.connectivity.ws_manager.aiohttp.ClientSession"
        ) as MockSessionConstructor:
            mock_session_instance = AsyncMock(spec=aiohttp.ClientSession)
            mock_session_instance.closed = False
            MockSessionConstructor.return_value = mock_session_instance

            # Create a new manager that will attempt to create an internal session
            manager = WebSocketManager(
                exchange_name="test_close_idle",
                message_handler=dummy_message_handler,
                config=default_ws_manager_config,
                session=None,  # Crucial for testing internal session handling
            )
            try:
                # Ensure the session is created if it's lazy.
                # Calling connect() briefly and then close() would be one way to trigger
                # session creation.
                # For this specific test, to ensure an internal session *would* be closed,
                # we rely on the manager's internal logic triggered by close().
                # We can check if the logger indicates an internal session was closed.
                with patch.object(manager, "_logger") as mock_logger_idle_close:
                    await manager.close()

                assert manager.is_connected is False
                # Assert that the ClientSession constructor was called (implying internal
                # session creation)
                MockSessionConstructor.assert_called_once()
                # Assert that the created session instance had its close() method called.
                mock_session_instance.close.assert_called_once()
                # And verify through logs that manager believes it closed an internal session.
                mock_logger_idle_close.info.assert_any_call(
                    "Internally created ClientSession closed."
                )

            finally:
                # Defensive close if test fails before manager.close()
                # To avoid direct _session access here, we rely on manager.close() being idempotent
                # and that the fixture/test structure ensures eventual cleanup.
                if not mock_session_instance.closed:
                    await manager.close()  # Call manager's public close method

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_send_json_successful_when_connected(
        self,
        mock_ws_connect: AsyncMock,
        ws_manager_instance: WebSocketManager,
    ) -> None:
        """Test send_json sends data if connected."""
        mock_ws_response = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        mock_ws_response.closed = False
        mock_ws_connect.return_value = mock_ws_response
        mock_ws_response.send_json = AsyncMock()  # Mock send_json on the response

        # Connect the manager
        connect_task = ws_manager_instance.connect()
        assert connect_task is not None
        # Patch internal tasks to prevent them from running complex logic for this test
        with (
            patch.object(ws_manager_instance, "_listen", AsyncMock()),
            patch.object(ws_manager_instance, "_keep_alive", AsyncMock()),
            patch.object(ws_manager_instance, "_on_connected_callback", AsyncMock()),
        ):
            await connect_task

        assert ws_manager_instance.is_connected is True

        payload = {"command": "test"}
        result = await ws_manager_instance.send_json(payload)

        assert result is True
        mock_ws_response.send_json.assert_called_once_with(payload)

    @pytest.mark.asyncio
    async def test_send_json_returns_false_when_not_connected(
        self, ws_manager_instance: WebSocketManager
    ) -> None:
        """Test send_json returns False if not connected."""
        assert ws_manager_instance.is_connected is False  # Should be false by default from fixture
        with patch.object(ws_manager_instance, "_logger") as mock_logger:
            result = await ws_manager_instance.send_json({"data": "test"})
        assert result is False
        mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    @patch("cyberdelta.apis.connectivity.ws_manager.asyncio.create_task")
    async def test_listen_loop_processes_message_and_reconnects_on_close(
        self,
        mock_create_task: MagicMock,
        mock_ws_connect: AsyncMock,
        default_ws_manager_config: WebSocketManagerConfig,
        event_loop: asyncio.AbstractEventLoop,  # pyright: ignore [reportUnusedVariable]
    ) -> None:
        """Test _listen processes messages, calls handler, and attempts reconnect on close."""
        mock_handler_for_test = AsyncMock()
        manager = WebSocketManager(
            exchange_name="test_listen_exchange",
            message_handler=mock_handler_for_test,
            config=default_ws_manager_config,
        )

        mock_ws_response1 = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        test_payload = {"type": "data"}
        messages_to_yield1 = [
            aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, json.dumps(test_payload), None),
            aiohttp.WSMessage(aiohttp.WSMsgType.CLOSED, None, None),  # Simulate server closing
        ]

        async def mock_aiter1() -> AsyncGenerator[aiohttp.WSMessage]:
            for msg in messages_to_yield1:
                yield msg

        mock_ws_response1.__aiter__ = mock_aiter1
        mock_ws_response1.closed = False  # Start as open

        # Second connection attempt after close
        mock_ws_response2 = AsyncMock(spec=aiohttp.ClientWebSocketResponse)

        async def mock_aiter2() -> AsyncGenerator[
            aiohttp.WSMessage
        ]:  # This one immediately closes to stop listen loop
            yield aiohttp.WSMessage(aiohttp.WSMsgType.CLOSING, None, None)

        mock_ws_response2.__aiter__ = mock_aiter2
        mock_ws_response2.closed = False

        # ws_connect will be called twice: once for initial, once for reconnect
        mock_ws_connect.side_effect = [mock_ws_response1, mock_ws_response2]

        try:
            with (
                patch.object(manager, "_keep_alive", AsyncMock()),
                patch.object(manager, "_on_connected_callback", AsyncMock()),
            ):
                connect_task = manager.connect()
                assert connect_task is not None
                await asyncio.wait_for(connect_task, timeout=1.0)  # Allow first connection
                assert manager.is_connected is True

                # Wait for listener to process messages from mock_ws_response1
                # The CLOSED message should trigger listener termination and reconnect logic
                # Allow time for the listener to process, close, and reconnect to start
                await asyncio.sleep(default_ws_manager_config.reconnect_delay + 0.1)

            mock_handler_for_test.assert_called_once_with(test_payload)
            # After CLOSED message and reconnect delay, second ws_connect should have
            # been called
            assert mock_ws_connect.call_count == 2
            # is_connected might be true if second connection is very fast, or false
            # if it also closes fast.
            # The main check is that reconnect was attempted (mock_ws_connect.call_count == 2).
            assert manager.is_connected is True  # Assuming second connect established and is open

        finally:
            await manager.close()

    def test_config_validation_invalid_url(self) -> None:
        with pytest.raises(ValidationError):
            WebSocketManagerConfig(ws_url="not_a_valid_ws_url")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "field, invalid_value, error_part",
        [
            ("ping_interval", -1, "Input should be greater than 0.0"),
            ("reconnect_delay", 0, "Input should be greater than 0.0"),
            ("max_reconnect_attempts", -1, "Input should be greater than or equal to 0"),
            ("connection_timeout", 0, "Input should be greater than 0.0"),
        ],
    )
    def test_config_validation_numeric_bounds(
        self,
        field: str,
        invalid_value: Any,  # noqa: ANN401 - Testing with various invalid types is intended here
        error_part: str,
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        valid_data = default_ws_manager_config.model_dump()
        # Ensure field exists before trying to pop, useful for future model changes
        if field not in valid_data:
            pytest.skip(f"Field {field} not in WebSocketManagerConfig model keys.")
        valid_data[field] = invalid_value
        with pytest.raises(ValidationError) as exc_info:
            WebSocketManagerConfig(**valid_data)
        assert error_part.lower() in str(exc_info.value).lower()

    def test_config_frozen_and_extra_forbid(
        self, default_ws_manager_config: WebSocketManagerConfig
    ) -> None:
        assert default_ws_manager_config.model_config.get("frozen") is True
        assert default_ws_manager_config.model_config.get("extra") == "forbid"
        with pytest.raises(ValidationError, match="frozen"):
            # Testing assignment to a frozen model attribute.
            default_ws_manager_config.connection_timeout = 5.0
        with pytest.raises(ValidationError, match="extra fields not permitted"):
            WebSocketManagerConfig(
                ws_url=AnyUrl("ws://example.com/ws"),
                extra_field="should_fail",  # type: ignore[call-arg] # Testing extra='forbid'
            )
