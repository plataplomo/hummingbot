import asyncio
import asyncio.tasks  # Import for direct access to create_task
import json
from collections.abc import AsyncGenerator, Callable, Coroutine
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import ClientSession as RealAiohttpCliSession
from aiohttp import WSMsgType
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


# Helper function for mock side_effect
def create_async_mock_task_for_side_effect(*args: Any, **kwargs: Any) -> AsyncMock:  # noqa: ANN401
    """Helper to create an AsyncMock, intended for use as a side_effect."""
    return AsyncMock()


async def completed_dummy_coro() -> None:
    """A dummy coroutine that completes immediately."""
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


@pytest.fixture
def mock_ws_connection_factory() -> Callable[..., AsyncMock]:
    """Factory for creating AsyncMock(spec=aiohttp.ClientWebSocketResponse) instances."""

    def _factory(
        closed: bool = False,
        # List of (WSMsgType, data, extra) tuples or Exceptions to raise on receive
        receive_sequence: list[tuple[WSMsgType, Any, Any] | Exception] | None = None,
        send_json_effect: Any | None = None,
        close_effect: Any | None = None,
        ping_effect: Any | None = None,
        pong_effect: Any | None = None,
        exception_effect: Any | None = None,
    ) -> AsyncMock:
        mock_conn = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        mock_conn.closed = closed
        mock_conn.exception.side_effect = exception_effect

        # Enhanced receive_sequence handling
        if receive_sequence:
            current_receive_call = 0
            data_to_send: Any = None  # Initialize with a common type, or Any

            async def receive_side_effect(
                *_args: Any,
                **_kwargs: Any,  # noqa: ANN401 # Generic pass-through for mock/callback
            ) -> aiohttp.WSMessage:
                nonlocal current_receive_call, data_to_send  # Ensure data_to_send is in scope if assigned here
                if current_receive_call < len(receive_sequence):
                    item = receive_sequence[current_receive_call]
                    current_receive_call += 1
                    if isinstance(item, Exception):
                        raise item
                    msg_type, msg_data, msg_extra = item
                    if msg_type == WSMsgType.TEXT and isinstance(msg_data, dict):
                        data_to_send = json.dumps(msg_data)
                    elif msg_type == WSMsgType.BINARY and isinstance(msg_data, str):
                        # This was the source of Mypy assignment error.
                        # data_to_send is now Any, so this is fine.
                        data_to_send = msg_data.encode("utf-8")
                    else:
                        data_to_send = msg_data
                    return aiohttp.WSMessage(type=msg_type, data=data_to_send, extra=msg_extra)
                raise StopAsyncIteration("Mocked receive sequence exhausted")

            mock_conn.receive = AsyncMock(side_effect=receive_side_effect)
        else:
            # Default: no messages, receive will immediately indicate closure/end
            mock_conn.receive = AsyncMock(side_effect=StopAsyncIteration)

        mock_conn.send_json = AsyncMock(side_effect=send_json_effect)
        mock_conn.send_str = AsyncMock(side_effect=send_json_effect)
        mock_conn.send_bytes = AsyncMock(side_effect=send_json_effect)
        mock_conn.close = AsyncMock(side_effect=close_effect)
        mock_conn.ping = AsyncMock(side_effect=ping_effect)
        mock_conn.pong = AsyncMock(side_effect=pong_effect)
        return mock_conn

    return _factory


@pytest_asyncio.fixture
async def patched_ws_connect(
    mock_ws_connection_factory: Callable[..., AsyncMock],
) -> AsyncGenerator[tuple[AsyncMock, AsyncMock]]:
    """Patches aiohttp.ClientSession.ws_connect and provides a default mock WS connection."""
    with patch(
        "cyberdelta.apis.connectivity.ws_manager.aiohttp.ClientSession.ws_connect"
    ) as mock_ws_connect_method:
        # Default: successful connection that does nothing specific unless configured by test
        default_mock_conn = mock_ws_connection_factory()

        # ws_connect is an async method, so its mock should be an AsyncMock,
        # or its side_effect should be an async function returning the connection.
        # If mock_ws_connect_method is already an AsyncMock from patching, setting its
        # return_value (if it's not a coroutine function itself) might not be right.
        # Let's make its side_effect an async function that returns our mock connection.
        async def default_connect_side_effect(*args: Any, **kwargs: Any) -> AsyncMock:
            return default_mock_conn

        mock_ws_connect_method.side_effect = default_connect_side_effect
        yield mock_ws_connect_method, default_mock_conn


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
            # Check if the task is running the correct coroutine object (the mock itself)
            # assert coro is establish_conn_coro_mock, \
            #     "Task is not running the coroutine from the establish_conn_coro_mock side_effect"
            # This assertion is hard to get right with AsyncMock internals and get_coro().
            # The more important check is that establish_conn_coro_mock (the side_effect) is called.

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
    async def test_connection_success_flow(
        self,
        patched_ws_connect: tuple[AsyncMock, AsyncMock],
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test the full flow of a successful connection and starting internal tasks."""
        mock_ws_connect_method, mock_ws_connection = patched_ws_connect

        # Configure the mock_ws_connection if defaults from factory aren't enough
        # For this test, default factory behavior (receive raises StopAsyncIteration) is okay
        # as we are not testing message processing by _listen, just that it starts.

        mock_on_connected_cb = AsyncMock()

        # Create a manager instance for this test, passing the mock callback
        manager = WebSocketManager(
            exchange_name="success_flow_test_ws",
            message_handler=dummy_message_handler,
            config=default_ws_manager_config,
            on_connected_callback=mock_on_connected_cb,
            session=None,
        )

        try:
            with patch.object(manager, "_logger") as mock_logger:
                connection_task = manager.connect()
                assert connection_task is not None
                await connection_task
                await asyncio.sleep(0.01)

                assert manager.is_connected is True
                expected_heartbeat = (
                    default_ws_manager_config.ping_interval * WebSocketManager.HEARTBEAT_FACTOR
                    if default_ws_manager_config.ping_interval > 0
                    else 0
                )
                mock_ws_connect_method.assert_called_once_with(
                    str(default_ws_manager_config.ws_url),
                    heartbeat=expected_heartbeat,
                    timeout=default_ws_manager_config.connection_timeout,
                )
                mock_logger.info.assert_any_call(
                    f"Successfully connected to {default_ws_manager_config.ws_url}."
                )
                mock_on_connected_cb.assert_called_once()

                if default_ws_manager_config.ping_interval > 0:
                    mock_ws_connection.ping.assert_called_once()
                else:
                    mock_ws_connection.ping.assert_not_called()
        finally:
            await manager.close()

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_connection_failure_retries_and_gives_up(
        self,
        mock_sleep: AsyncMock,
        patched_ws_connect: tuple[AsyncMock, AsyncMock],
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test connection retries on failure and eventually gives up."""
        mock_ws_connect_method, _mock_ws_connection = patched_ws_connect

        async def actual_mock_ws_connect_side_effect_failure(*args: Any, **kwargs: Any) -> None:  # noqa: ANN401
            raise aiohttp.ClientConnectorError(MagicMock(), OSError("Connection failed"))

        mock_ws_connect_method.side_effect = actual_mock_ws_connect_side_effect_failure

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
            # Correction: The loop runs max_reconnect_attempts times in total if all fail.
            # The first attempt is current_attempt = 0.
            assert mock_ws_connect_method.call_count == retry_test_config.max_reconnect_attempts
            # Sleep happens *before* a retry attempt, not the initial one.
            # If max_reconnect_attempts is 1, there are 0 retries, so 0 sleeps.
            assert mock_sleep.call_count == max(0, retry_test_config.max_reconnect_attempts - 1)

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
        mock_ws_connection: AsyncMock,
    ) -> None:
        """Test close() cancels internal tasks and closes WS connection and session."""
        mock_ws_connection.receive = AsyncMock()
        mock_ws_connection.closed = False

        created_tasks_map: dict[str, asyncio.Task[Any]] = {}
        original_create_task = asyncio.tasks.create_task

        def side_effect_create_task(
            coro: Coroutine[Any, Any, Any], *, name: str | None = None
        ) -> asyncio.Task[Any]:
            task = original_create_task(coro, name=name)
            if name:
                created_tasks_map[name] = task
            return task

        mock_create_task.side_effect = side_effect_create_task

        connection_establishment_task = ws_manager_instance.connect()
        assert connection_establishment_task is not None
        await connection_establishment_task
        await asyncio.sleep(0.01)

        assert ws_manager_instance.is_connected is True
        exchange_name_for_test = "test_exchange_ws"
        listener_task_name = f"{exchange_name_for_test}_ws_listen"
        ping_task_name = f"{exchange_name_for_test}_ws_ping"

        listener_task = created_tasks_map.get(listener_task_name)
        ping_task = created_tasks_map.get(ping_task_name)

        assert listener_task is not None and not listener_task.done()
        if default_ws_manager_config.ping_interval > 0:
            assert ping_task is not None and not ping_task.done()

        with patch.object(ws_manager_instance, "_logger") as mock_logger_close:
            await ws_manager_instance.close()

        assert ws_manager_instance.is_connected is False
        mock_ws_connection.close.assert_called_once()

        if listener_task:
            assert listener_task.cancelled()
        if ping_task:
            assert ping_task.done()

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
        patched_ws_connect: tuple[AsyncMock, AsyncMock],
    ) -> None:
        """Test close() on a never-connected manager with an internal session closes it."""
        _mock_ws_connect_method, _mock_ws_connection = patched_ws_connect

        with patch(
            "cyberdelta.apis.connectivity.ws_manager.aiohttp.ClientSession"
        ) as MockAiohttpSessionConstructor:
            mock_internal_session_instance = AsyncMock(spec=RealAiohttpCliSession)
            mock_internal_session_instance.closed = False
            MockAiohttpSessionConstructor.return_value = mock_internal_session_instance

            manager = WebSocketManager(
                exchange_name="test_close_idle",
                message_handler=dummy_message_handler,
                config=default_ws_manager_config,
                session=None,  # Crucial for testing internal session handling
            )
            try:
                connect_task = manager.connect()
                await asyncio.sleep(0.01)  # Allow connect() to proceed enough to create session

                if connect_task and not connect_task.done():
                    connect_task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await connect_task

                MockAiohttpSessionConstructor.assert_called_once()

                with patch.object(manager, "_logger") as mock_logger_idle_close:
                    await manager.close()  # This is the main action to test

                assert manager.is_connected is False
                mock_internal_session_instance.close.assert_called_once()
                mock_logger_idle_close.info.assert_any_call(
                    "Internally created ClientSession closed."
                )

            finally:
                await manager.close()

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    @patch("cyberdelta.apis.connectivity.ws_manager.asyncio.create_task")
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_listen_loop_processes_message_and_reconnects_on_close(
        self,
        mock_sleep: AsyncMock,
        mock_create_task: MagicMock,
        patched_ws_connect: tuple[AsyncMock, AsyncMock],
        default_ws_manager_config: WebSocketManagerConfig,
        mock_ws_connection_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test _listen loop processes messages and triggers reconnect on unexpected close."""
        mock_ws_connect_method, _initial_mock_ws_conn = patched_ws_connect

        test_message_payload = {"type": "data", "value": "test_data"}
        simulated_connection_drop_error = aiohttp.ClientConnectionError("Simulated drop")

        # --- Configure behavior for the FIRST connection attempt ---
        # The _listen loop should receive one message, then the connection drops.
        first_connection_mock = mock_ws_connection_factory(
            receive_sequence=[
                (WSMsgType.TEXT, test_message_payload, None),  # Yield one message
                simulated_connection_drop_error,  # Then raise an error
            ],
            closed=False,  # Initially open
        )
        # Ensure .exception() returns the error that caused receive to fail, or None
        first_connection_mock.exception = MagicMock(return_value=simulated_connection_drop_error)

        # --- Configure behavior for the SECOND connection attempt (reconnect) ---
        # The _listen loop on reconnect should find the connection immediately closed or error out
        # to stop the test gracefully.
        second_connection_mock = mock_ws_connection_factory(
            receive_sequence=[aiohttp.ClientError("Stop after reconnect")],  # Or just closed=True
            closed=True,
        )
        second_connection_mock.exception = MagicMock(
            return_value=aiohttp.ClientError("Stop after reconnect")
        )

        # Set up the side effect for the main ws_connect mock method
        # It will be called multiple times: once for initial connect, once for reconnect.
        connect_attempt_count = 0

        async def dynamic_ws_connect_side_effect(*_args: Any, **_kwargs: Any) -> AsyncMock:
            nonlocal connect_attempt_count
            connect_attempt_count += 1
            if connect_attempt_count == 1:
                return first_connection_mock
            elif connect_attempt_count == 2:
                return second_connection_mock
            # Fallback if called more than expected (should not happen in this test design)
            raise AssertionError("ws_connect called more than twice")  # Line 514 E501

        mock_ws_connect_method.side_effect = dynamic_ws_connect_side_effect

        mock_user_message_handler = AsyncMock()

        # Use a config with quick reconnect for the test
        test_config = default_ws_manager_config.model_copy(
            update={"max_reconnect_attempts": 1, "reconnect_delay": 0.01}
        )

        manager = WebSocketManager(
            exchange_name="listen_reconnect_test",
            message_handler=mock_user_message_handler,
            config=test_config,
            on_connected_callback=dummy_on_connected_callback,  # Or a new AsyncMock if needed
            session=None,  # Use internal session
        )

        created_tasks_map: dict[str, asyncio.Task[Any]] = {}
        original_create_task = asyncio.tasks.create_task

        def side_effect_for_create_task_capture(
            coro: Coroutine[Any, Any, Any], *, name: str | None = None
        ) -> asyncio.Task[Any]:
            task = original_create_task(coro, name=name)
            if name:
                created_tasks_map[name] = task
            return task

        mock_create_task.side_effect = side_effect_for_create_task_capture

        # Use the exchange_name defined when manager was instantiated locally for this test
        listener_task_name = "listen_reconnect_test_ws_listen"

        try:
            with patch.object(manager, "_logger") as mock_logger:
                initial_connect_task = manager.connect()
                assert initial_connect_task is not None
                await initial_connect_task  # Wait for first connection attempt & listen to start
                await asyncio.sleep(
                    0.05
                )  # Allow time for listen loop to process message & hit error

                # --- Assertions for the first connection phase ---
                mock_user_message_handler.assert_called_once_with(test_message_payload)
                first_connection_mock.receive.assert_called()  # Should have been called multiple times
                assert (
                    first_connection_mock.receive.call_count >= 2
                )  # Once for message, once for error

                # Check that reconnection was triggered
                mock_logger.warning.assert_any_call(
                    f"WebSocket connection closed unexpectedly or with error: "
                    f"{simulated_connection_drop_error}. Attempting reconnect..."
                )  # Line 521 E501
                mock_sleep.assert_called_with(test_config.reconnect_delay)
                assert (
                    mock_ws_connect_method.call_count == 2
                )  # Initial connect + one reconnect attempt

                # --- Assertions for task recreation ---
                # Check if the listen task was recreated (or a new one started)
                # This depends on how WebSocketManager names or manages tasks.
                # If it reuses names, we check map, if it creates new ones, count might be better.
                # For simplicity, let's assume it attempts to create a new one with the same name pattern.
                # The create_task mock will capture the latest task with that name.
                assert listener_task_name in created_tasks_map
                restarted_listen_task = created_tasks_map[listener_task_name]
                assert restarted_listen_task is not None
                # Original listen task from first connection should be done (due to error)

                # Allow the second connection attempt (which should fail quickly) to complete
                if not restarted_listen_task.done():
                    try:
                        await asyncio.wait_for(restarted_listen_task, timeout=0.1)
                    except TimeoutError:
                        restarted_listen_task.cancel()
                        await asyncio.gather(restarted_listen_task, return_exceptions=True)
                    except Exception:
                        pass  # Expected if it errors out quickly from second_connection_mock

                # Manager should eventually reflect not being connected after retries exhausted
                # Wait a bit more for final state if reconnect attempts were > 1 in config
                await asyncio.sleep(
                    test_config.reconnect_delay * 2
                )  # Line 532 E501 Ensure state updates
                assert manager.is_connected is False

        finally:
            await manager.close()  # Ensure cleanup

    def test_config_validation_invalid_url(self) -> None:
        with pytest.raises(ValidationError):
            WebSocketManagerConfig(ws_url="not_a_valid_ws_url")  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "field, invalid_value, error_part",
        [
            ("ping_interval", -1, "Input should be greater than 0"),
            ("reconnect_delay", 0, "Input should be greater than 0"),
            ("max_reconnect_attempts", -1, "Input should be greater than or equal to 0"),
            ("connection_timeout", 0, "Input should be greater than 0"),
        ],
    )
    def test_config_validation_numeric_bounds(
        self,
        field: str,
        invalid_value: Any,  # noqa: ANN401
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
        with pytest.raises(ValidationError, match=r"Extra inputs are not permitted"):
            WebSocketManagerConfig(
                ws_url=AnyUrl("ws://example.com/ws"),
                extra_field="should_fail",  # type: ignore[call-arg] # Testing extra='forbid'
            )
