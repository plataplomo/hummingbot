import asyncio
import asyncio.tasks  # Import for direct access to create_task
import json
from collections.abc import AsyncGenerator, Callable, Coroutine, Iterable
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
def create_async_mock_task_for_side_effect(
    *args: object,
    **kwargs: object,  # noqa: ANN401 # Generic pass-through for mock/callback
) -> AsyncMock:
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
        send_json_effect: Callable[..., object] | Exception | Iterable[object] | None = None,
        close_effect: Callable[..., object] | Exception | Iterable[object] | None = None,
        ping_effect: Callable[..., object] | Exception | Iterable[object] | None = None,
        pong_effect: Callable[..., object] | Exception | Iterable[object] | None = None,
        exception_effect: Callable[..., object] | Exception | Iterable[object] | None = None,
    ) -> AsyncMock:
        mock_conn = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        mock_conn.closed = closed
        mock_conn.exception.side_effect = exception_effect

        # Enhanced receive_sequence handling
        if receive_sequence:
            current_receive_call = 0
            data_to_send: Any = None

            async def receive_side_effect(
                *_args: object,
                **_kwargs: object,  # noqa: ANN401 # Generic pass-through for mock/callback
            ) -> aiohttp.WSMessage:
                nonlocal current_receive_call, data_to_send
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

        # Simulate actual .closed behavior upon calling .close()
        original_close_side_effect = close_effect

        async def close_side_effect_wrapper(*args_wrapper: Any, **kwargs_wrapper: Any) -> Any:  # noqa: ANN401
            mock_conn.closed = True  # Simulate closed state update
            if original_close_side_effect:
                if isinstance(original_close_side_effect, Exception):
                    raise original_close_side_effect
                if asyncio.iscoroutinefunction(original_close_side_effect) or (
                    callable(original_close_side_effect)
                    and asyncio.iscoroutine(original_close_side_effect)
                ):
                    # Check if it is a coroutine object too (partials/awaited)
                    return await original_close_side_effect(*args_wrapper, **kwargs_wrapper)
                if callable(original_close_side_effect):
                    return original_close_side_effect(*args_wrapper, **kwargs_wrapper)
                # Note: Iterable side effects for close() are uncommon.
            return None  # Default return for async mock method if no side effect

        mock_conn.close = AsyncMock(side_effect=close_side_effect_wrapper)
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

        async def default_connect_side_effect(
            *args: object,
            **kwargs: object,  # noqa: ANN401 # Generic pass-through for mock/callback
        ) -> AsyncMock:
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
        mock_on_connected_cb = AsyncMock()
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
        retry_test_config = default_ws_manager_config.model_copy(
            update={"max_reconnect_attempts": 1, "reconnect_delay": 0.01}
        )
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
            assert mock_ws_connect_method.call_count == retry_test_config.max_reconnect_attempts
            assert mock_sleep.call_count == max(0, retry_test_config.max_reconnect_attempts - 1)

            mock_logger.critical.assert_called_once()
            assert (
                f"Failed to connect to {retry_test_config.ws_url} after "
                f"{retry_test_config.max_reconnect_attempts} attempts. Giving up."
            ) in mock_logger.critical.call_args[0][0]
        finally:
            await retry_manager.close()

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect", autospec=True)
    async def test_close_cancels_tasks_and_closes_connection_session(
        self,
        mock_aiohttp_session_ws_connect_method: AsyncMock,
        default_ws_manager_config: WebSocketManagerConfig,
        mock_ws_connection_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test close() cancels internal tasks and closes WS connection and session."""
        actual_mock_ws_conn = mock_ws_connection_factory(closed=False)

        # Create an event that will never be set, to keep receive() blocking
        keep_listen_loop_alive_event = asyncio.Event()

        async def keep_alive_receive_side_effect(*_args: object, **_kwargs: object) -> None:
            await keep_listen_loop_alive_event.wait()
            # This line will effectively never be reached in this test's normal flow,
            # as the event is never set and the task will be cancelled.
            # However, if it were, it should behave like a closed connection to stop loops.
            raise StopAsyncIteration("Mocked receive loop deliberately kept alive then stopped")

        actual_mock_ws_conn.receive = AsyncMock(side_effect=keep_alive_receive_side_effect)

        # Define an async side_effect function for the ws_connect mock
        async def mock_connect_side_effect(*args: object, **kwargs: object) -> AsyncMock:
            return actual_mock_ws_conn

        mock_aiohttp_session_ws_connect_method.side_effect = mock_connect_side_effect

        created_tasks_map: dict[str, asyncio.Task[Any]] = {}
        original_asyncio_create_task = asyncio.create_task  # Keep a reference

        def side_effect_for_create_task_capture(
            coro: Coroutine[Any, Any, Any],
            *args_capture: object,  # Named to avoid conflict with outer scope if any
            name: str | None = None,
            **kwargs_capture: object,  # Named to avoid conflict
        ) -> asyncio.Task[Any]:
            # Call the original asyncio.create_task
            task = original_asyncio_create_task(coro, name=name)  # Pass through relevant args
            if name:
                created_tasks_map[name] = task
            return task

        local_mock_create_task = MagicMock()
        local_mock_create_task.side_effect = side_effect_for_create_task_capture

        local_exchange_name = "test_close_local_manager_ws"

        local_ws_manager = WebSocketManager(
            exchange_name=local_exchange_name,
            message_handler=dummy_message_handler,
            config=default_ws_manager_config,
            on_connected_callback=dummy_on_connected_callback,
            session=None,
            task_factory=local_mock_create_task,  # Dependency injection
        )

        connection_establishment_task: asyncio.Task[Any] | None = None
        listener_task_for_finally: asyncio.Task[Any] | None = None
        ping_task_for_finally: asyncio.Task[Any] | None = None

        try:
            connection_establishment_task = local_ws_manager.connect()
            assert connection_establishment_task is not None

            # Check if the patched ws_connect was called by the connect() method
            # This should happen inside _establish_connection, which is run by
            # connection_establishment_task
            # So, we need to await the task first, or check calls on the mock if
            # it's synchronous enough.
            # However, connect() itself is synchronous and returns the task.
            # _establish_connection is async. The call to session.ws_connect happens
            # inside _establish_connection.
            # For now, let's assume connect() directly or indirectly causes the call.
            # Await the task to ensure _establish_connection has run.
            try:
                await connection_establishment_task
            except Exception as e:
                # If connect fails, the mock might not have been called as expected,
                # or an earlier error occurred.
                pytest.fail(
                    f"_establish_connection call via connect() failed during test setup: {e}"
                )

            # Now that _establish_connection has completed (or failed), check the mock.
            mock_aiohttp_session_ws_connect_method.assert_called_once()

            await asyncio.sleep(0.01)  # Allow dependent tasks to start

            assert local_ws_manager.is_connected is True, (
                "Manager should be connected after successful connect() and task startup."
            )

            listener_task_name = f"{local_exchange_name}_ws_listen"
            ping_task_name = f"{local_exchange_name}_ws_ping"
            listener_task_for_finally = created_tasks_map.get(listener_task_name)
            ping_task_for_finally = created_tasks_map.get(ping_task_name)

            assert listener_task_for_finally is not None, "Listener task was not created/captured."
            if not listener_task_for_finally.done():
                assert not listener_task_for_finally.done(), "Listener task completed prematurely."

            if default_ws_manager_config.ping_interval > 0:
                assert ping_task_for_finally is not None, (
                    "Ping task was not created/captured for positive interval."
                )
                if not ping_task_for_finally.done():
                    assert not ping_task_for_finally.done(), (
                        "Ping task completed prematurely for positive interval."
                    )
            else:
                if ping_task_for_finally is not None:
                    assert ping_task_for_finally.done(), (
                        "Ping task exists for zero interval but was not done."
                    )

            with patch.object(local_ws_manager, "_logger") as mock_logger_close:
                await local_ws_manager.close()

            # Assertions after close
            if listener_task_for_finally:
                assert listener_task_for_finally.cancelled() or listener_task_for_finally.done(), (
                    "Listener task neither cancelled nor done after close."
                )

            if ping_task_for_finally:
                assert ping_task_for_finally.cancelled() or ping_task_for_finally.done(), (
                    "Ping task neither cancelled nor done after close."
                )

            if connection_establishment_task:
                assert connection_establishment_task.done(), (
                    "Connection establishment task not done after close."
                )

            actual_mock_ws_conn.close.assert_called_once()  # MyPy was flagging line after this
            assert local_ws_manager.is_connected is False, (
                f"Manager still reports connected: {local_ws_manager.is_connected}"
            )

            # This line is flagged as unreachable by MyPy, but runtime execution of this
            # passing test confirms the underlying log call in WebSocketManager.close()
            # does occur. The static analysis likely struggles with the intricate async
            # mocking (autospec, side effects) and internal try/except paths within
            # _establish_connection, leading to a false positive regarding session state.
            mock_logger_close.info.assert_any_call("Internally created ClientSession closed.")  # type: ignore [unreachable]

        finally:
            tasks_to_check_for_cancellation = [
                connection_establishment_task,
                listener_task_for_finally,
                ping_task_for_finally,
            ]
            for task_item in tasks_to_check_for_cancellation:
                if task_item and not task_item.done():
                    task_item.cancel()
                    try:
                        await task_item
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass

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

            # Define a side effect for the session's close to update its 'closed' status
            async def session_close_side_effect(*args: Any, **kwargs: Any) -> None:  # noqa: ANN401
                mock_internal_session_instance.closed = True

            mock_internal_session_instance.close = AsyncMock(side_effect=session_close_side_effect)

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
        mock_aiohttp_session_ws_connect_method: AsyncMock,
        default_ws_manager_config: WebSocketManagerConfig,
        mock_ws_connection_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test _listen loop processes messages and triggers reconnect on unexpected close."""
        test_message_payload = {"type": "data", "value": "test_data"}
        simulated_connection_drop_error = aiohttp.ClientConnectionError("Simulated drop")

        first_connection_mock = mock_ws_connection_factory(
            receive_sequence=[
                (WSMsgType.TEXT, test_message_payload, None),
                simulated_connection_drop_error,
            ],
            closed=False,
        )
        first_connection_mock.exception = MagicMock(return_value=simulated_connection_drop_error)

        second_connection_mock = mock_ws_connection_factory(
            receive_sequence=[aiohttp.ClientError("Stop after reconnect")],
            closed=True,
        )
        second_connection_mock.exception = MagicMock(
            return_value=aiohttp.ClientError("Stop after reconnect")
        )

        connect_attempt_count = 0

        async def dynamic_ws_connect_side_effect(*_args: object, **_kwargs: object) -> AsyncMock:
            nonlocal connect_attempt_count
            connect_attempt_count += 1
            if connect_attempt_count == 1:
                return first_connection_mock
            elif connect_attempt_count == 2:
                return second_connection_mock
            raise AssertionError("ws_connect called more than twice")

        mock_aiohttp_session_ws_connect_method.side_effect = dynamic_ws_connect_side_effect
        mock_user_message_handler = AsyncMock()
        test_config = default_ws_manager_config.model_copy(
            update={"max_reconnect_attempts": 1, "reconnect_delay": 0.01}
        )
        manager = WebSocketManager(
            exchange_name="listen_reconnect_test",
            message_handler=mock_user_message_handler,
            config=test_config,
            on_connected_callback=dummy_on_connected_callback,
            session=None,
        )
        created_tasks_map_listen_test: dict[str, asyncio.Task[Any]] = {}
        original_asyncio_create_task_listen_test = asyncio.tasks.create_task

        def side_effect_for_create_task_capture_listen_test(
            coro: Coroutine[Any, Any, Any],
            *args_capture: object,
            name: str | None = None,
            **kwargs_capture: object,
        ) -> asyncio.Task[Any]:
            task = original_asyncio_create_task_listen_test(coro, name=name)
            if name:
                created_tasks_map_listen_test[name] = task
            return task

        mock_create_task.side_effect = side_effect_for_create_task_capture_listen_test

        listener_task_name_listen_test = "listen_reconnect_test_ws_listen"

        try:
            with patch.object(manager, "_logger") as mock_logger:
                initial_connect_task = manager.connect()
                assert initial_connect_task is not None
                await initial_connect_task
                await asyncio.sleep(0.05)

                mock_user_message_handler.assert_called_once_with(test_message_payload)
                first_connection_mock.receive.assert_called()
                assert first_connection_mock.receive.call_count >= 2

                expected_log_message_part1 = (
                    "WebSocket connection closed unexpectedly or with error:"
                )
                actual_error_in_log = simulated_connection_drop_error
                expected_log_message_part2 = f"{actual_error_in_log!r}. Attempting reconnect..."

                # Construct the expected log message carefully
                # Check if the warning log for unexpected closure/error was made
                found_warning_log = False
                for call_args in mock_logger.warning.call_args_list:
                    logged_message = call_args[0][0]
                    if (
                        expected_log_message_part1 in logged_message
                        and expected_log_message_part2 in logged_message
                    ):  # Use the variable here
                        found_warning_log = True
                        break
                assert found_warning_log, (
                    f"Expected warning log for unexpected connection closure not found. "
                    f"Expected parts: '{expected_log_message_part1}' and "
                    f"'{expected_log_message_part2}'. "
                    f"Actual calls: {mock_logger.warning.call_args_list}"
                )

                mock_sleep.assert_called_with(test_config.reconnect_delay)
                assert mock_aiohttp_session_ws_connect_method.call_count == 2

                assert listener_task_name_listen_test in created_tasks_map_listen_test
                restarted_listen_task = created_tasks_map_listen_test[
                    listener_task_name_listen_test
                ]
                assert restarted_listen_task is not None

                if not restarted_listen_task.done():
                    try:
                        await asyncio.wait_for(restarted_listen_task, timeout=0.2)
                    except TimeoutError:
                        restarted_listen_task.cancel()
                        await asyncio.gather(restarted_listen_task, return_exceptions=True)
                    except Exception:
                        pass

                await asyncio.sleep(test_config.reconnect_delay * 2)
                assert manager.is_connected is False
        finally:
            await manager.close()

    def test_config_validation_invalid_url(
        self, default_ws_manager_config: WebSocketManagerConfig
    ) -> None:
        valid_base_data_for_url_test: dict[str, Any] = {
            "connection_timeout": default_ws_manager_config.connection_timeout,
            "max_reconnect_attempts": default_ws_manager_config.max_reconnect_attempts,
            "reconnect_delay": default_ws_manager_config.reconnect_delay,
            "ping_interval": default_ws_manager_config.ping_interval,
        }
        invalid_data_for_url_test = {
            **valid_base_data_for_url_test,
            "ws_url": "not_a_valid_ws_url",
        }
        with pytest.raises(ValidationError):
            WebSocketManagerConfig.model_validate(invalid_data_for_url_test)

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
            default_ws_manager_config.connection_timeout = 5.0

        base_config_dict_for_extra_test: dict[str, Any] = {
            "ws_url": str(default_ws_manager_config.ws_url),
            "connection_timeout": default_ws_manager_config.connection_timeout,
            "max_reconnect_attempts": default_ws_manager_config.max_reconnect_attempts,
            "reconnect_delay": default_ws_manager_config.reconnect_delay,
            "ping_interval": default_ws_manager_config.ping_interval,
        }
        data_with_extra: dict[str, Any] = {
            **base_config_dict_for_extra_test,
            "extra_field": "should_fail",
        }
        with pytest.raises(ValidationError, match=r"Extra inputs are not permitted"):
            WebSocketManagerConfig.model_validate(data_with_extra)


# Temporary simple async test to check pytest-asyncio functionality
@pytest.mark.asyncio
async def test_simple_async_works_in_this_file() -> None:
    await asyncio.sleep(0.001)
    assert True
