import asyncio
import asyncio.tasks  # Import for direct access to create_task
import json
import logging
from collections.abc import AsyncGenerator, Callable, Coroutine, Iterable
from contextlib import suppress
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import ClientSession as RealAiohttpCliSession
from aiohttp import WSMessage, WSMsgType
from aiohttp.helpers import sentinel
from pydantic import AnyUrl, ValidationError
from pytest import LogCaptureFixture

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


@pytest_asyncio.fixture
async def mock_aiohttp_client_session() -> AsyncMock:
    """Provides a mock aiohttp.ClientSession instance."""
    session_mock = AsyncMock(spec=aiohttp.ClientSession)
    session_mock.closed = False
    session_mock.close = AsyncMock()
    # ws_connect will be attached by patched_ws_connect fixture if that fixture uses this one.
    # For global patching, this fixture might not be directly used by patched_ws_connect.
    return session_mock


@pytest.fixture
def mock_ws_connection_factory() -> Callable[..., AsyncMock]:
    """Factory to create mock WebSocket connection objects."""

    def _factory(
        closed: bool = False,
        receive_sequence: Iterable[Any] | None = None,
        ping_pong_passthrough: bool = False,
        block_indefinitely: bool = False,
    ) -> AsyncMock:
        mock_conn = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        mock_conn.closed = closed
        seq_iterator = iter(receive_sequence) if receive_sequence else None
        # mock_conn._never_ending_event_for_iteration = asyncio.Event()
        # No longer used with asyncio.Future()

        async def mock_receive_internal() -> WSMessage:
            await asyncio.tasks.sleep(0)  # MODIFIED: Ensure yield
            if seq_iterator:
                try:
                    item = next(seq_iterator)
                    if isinstance(item, Exception):
                        raise item
                    return WSMessage(item[0], item[1], item[2])
                except StopIteration as e_stop:
                    mock_conn.closed = True
                    # B904: Explicitly raise from the StopIteration context
                    raise TimeoutError("Mocked receive sequence exhausted, timing out.") from e_stop
            mock_conn.closed = True
            raise TimeoutError("Default Mocked receive: no sequence, timing out.")

        mock_conn.receive = AsyncMock(side_effect=mock_receive_internal)
        mock_conn.__aiter__ = MagicMock(return_value=mock_conn)

        # Define a custom exception for clarity if the blocking event is unexpectedly set.
        # class TestIndefiniteBlockEventSetError(Exception): # No longer used
        #     pass

        async def anext_for_mock() -> WSMessage:
            if block_indefinitely:
                # Special logger for this test path
                test_logger = logging.getLogger("TestAnextBlockIndefinitely")
                test_logger.info("[TestAnextBlockIndefinitely] Entering await asyncio.Future()")
                try:
                    await asyncio.Future()
                    # This path is not expected to be reached if Future() blocks and the task
                    # awaiting it is cancelled, as CancelledError should propagate.
                    test_logger.warning(
                        "[TestAnextBlockIndefinitely] asyncio.Future() completed "
                        "WITHOUT CancelledError."
                    )
                    raise StopAsyncIteration(
                        "Future unblocked unexpectedly in block_indefinitely mode"
                    )
                except asyncio.CancelledError:
                    test_logger.info(
                        "[TestAnextBlockIndefinitely] asyncio.Future() was CANCELLED as expected."
                    )
                    raise
                except Exception as e_anext:
                    test_logger.error(
                        f"[TestAnextBlockIndefinitely] asyncio.Future() raised "
                        f"UNEXPECTED error: {e_anext!r}",
                        exc_info=True,
                    )
                    raise

            # Standard path if not blocking indefinitely (from original logic)
            if seq_iterator:
                try:
                    msg_any = await mock_conn.receive()
                    # JUSTIFICATION FOR CAST:
                    # AsyncMock with a side_effect (mock_receive_internal) often leads Mypy to
                    # infer 'Any' for the awaited result, even if the side_effect function
                    # is correctly typed (mock_receive_internal returns WSMessage).
                    # This cast clarifies the known type.
                    # Alternative typing solutions (e.g. more complex AsyncMock wrapping)
                    # are overly complex for this test mock.
                    # The developer is certain mock_receive_internal returns WSMessage.
                    # #[CAST-REVIEW-REQUIRED]
                    msg = cast(WSMessage, msg_any)
                    assert isinstance(msg, WSMessage)  # Runtime verification

                    if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.CLOSING):
                        mock_conn.closed = True
                        raise StopAsyncIteration
                    return msg
                except (TimeoutError, aiohttp.ClientError) as e:
                    mock_conn.closed = True
                    raise StopAsyncIteration from e
            else:  # seq_iterator is None, so no messages can be produced.
                mock_conn.closed = True
                raise StopAsyncIteration

        mock_conn.__anext__ = AsyncMock(side_effect=anext_for_mock)

        mock_conn.send_str = AsyncMock()
        mock_conn.send_bytes = AsyncMock()
        mock_conn.ping = AsyncMock()
        mock_conn.pong = AsyncMock()
        mock_conn.close = AsyncMock(return_value=True)
        mock_conn.exception = MagicMock(return_value=None)  # Default to no exception

        if ping_pong_passthrough:
            # If passthrough, pong should send a PONG WSMsgType back via receive
            # This is a simplified simulation.
            async def mock_ping_with_pong_effect() -> None:  # Removed *args, **kwargs
                # Simulate that a PONG message would be received next
                # This requires receive_sequence to be an iterator that we can manipulate or
                # a more complex side_effect for receive. For now, this is conceptual.
                pass  # Actual pong simulation via receive is complex

            mock_conn.ping = AsyncMock(side_effect=mock_ping_with_pong_effect)

        return mock_conn

    return _factory


@pytest_asyncio.fixture
async def patched_ws_connect(
    mock_ws_connection_factory: Callable[..., AsyncMock],
) -> AsyncGenerator[tuple[AsyncMock, AsyncMock]]:
    """Patches aiohttp.ClientSession.ws_connect globally and provides a default mock
    WS connection."""
    default_mock_conn = mock_ws_connection_factory(closed=False, block_indefinitely=False)

    async def default_connect_side_effect() -> AsyncMock:  # Removed *args, **kwargs
        return default_mock_conn

    with patch("aiohttp.ClientSession.ws_connect") as mock_ws_connect_method:
        mock_ws_connect_method.side_effect = default_connect_side_effect
        yield mock_ws_connect_method, default_mock_conn


class TestWebSocketManager:
    """Tests for the WebSocketManager class."""

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_connect_returns_task_and_idempotency(
        self,
        mock_ws_connect_patch: AsyncMock,
        ws_manager_instance: WebSocketManager,
    ) -> None:
        """Test connect() returns a task and is idempotent while task is running."""
        mock_ws_connect_patch.return_value = AsyncMock(spec=aiohttp.ClientWebSocketResponse)

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
    @patch("aiohttp.ClientSession")  # Patch the ClientSession class
    async def test_connection_success_flow(
        self,
        MockAiohttpSessionConstructor: MagicMock,  # Patched class
        default_ws_manager_config: WebSocketManagerConfig,
        mock_ws_connection_factory: Callable[..., AsyncMock],
        caplog: LogCaptureFixture,
    ) -> None:
        """Test the full flow of a successful connection and starting internal tasks."""
        caplog.set_level(logging.INFO, logger="TestAnextBlockIndefinitely")
        caplog.set_level(logging.DEBUG, logger="WebSocketManager.success_flow_test_ws")

        # Configure the mock session instance that will be returned by AiohttpSessionConstructor()
        mock_session_instance = MockAiohttpSessionConstructor.return_value

        # This is the WebSocket connection object itself (mocked)
        mock_ws_connection = mock_ws_connection_factory(closed=False, block_indefinitely=True)

        # Make the ws_connect method on the mock session instance an AsyncMock
        # that, when awaited, returns our mock_ws_connection.
        mock_session_instance.ws_connect = AsyncMock(return_value=mock_ws_connection)

        mock_on_connected_cb = AsyncMock()
        manager = WebSocketManager(
            exchange_name="success_flow_test_ws",
            message_handler=dummy_message_handler,
            config=default_ws_manager_config,
            on_connected_callback=mock_on_connected_cb,
            session=None,
        )
        try:
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
            mock_session_instance.ws_connect.assert_called_once_with(
                str(default_ws_manager_config.ws_url),
                heartbeat=expected_heartbeat,
                timeout=sentinel,
            )
            mock_on_connected_cb.assert_called_once()

            if default_ws_manager_config.ping_interval > 0:
                mock_ws_connection.ping.assert_called_once()
        finally:
            print("\nCaptured logs for test_connection_success_flow:")
            print(caplog.text)
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

        async def actual_mock_ws_connect_side_effect_failure() -> None:
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
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_close_cancels_tasks_and_closes_connection_session(
        self,
        mock_aiohttp_session_ws_connect_method: AsyncMock,
        default_ws_manager_config: WebSocketManagerConfig,
        mock_ws_connection_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test close() cancels internal tasks and closes WS connection and session."""
        actual_mock_ws_conn = mock_ws_connection_factory(closed=False, block_indefinitely=True)
        mock_aiohttp_session_ws_connect_method.return_value = actual_mock_ws_conn

        created_tasks_map: dict[str, asyncio.Task[Any]] = {}
        original_asyncio_create_task = asyncio.tasks.create_task

        def side_effect_for_create_task_capture(
            coro: Coroutine[Any, Any, Any],
            *args_capture: object,
            name: str | None = None,
            **kwargs_capture: object,
        ) -> asyncio.Task[Any]:
            task = original_asyncio_create_task(coro, name=name)
            if name:
                created_tasks_map[name] = task
            return task

        capturing_task_factory = MagicMock(side_effect=side_effect_for_create_task_capture)

        local_exchange_name = "test_close_local_manager_ws"
        local_ws_manager = WebSocketManager(
            exchange_name=local_exchange_name,
            message_handler=dummy_message_handler,
            config=default_ws_manager_config,
            on_connected_callback=dummy_on_connected_callback,
            session=None,
            task_factory=capturing_task_factory,
        )

        connection_establishment_task: asyncio.Task[Any] | None = None
        listener_task_for_close: asyncio.Task[Any] | None = None
        ping_task_for_close: asyncio.Task[Any] | None = None

        try:
            connection_establishment_task = local_ws_manager.connect()
            assert connection_establishment_task is not None
            try:
                await connection_establishment_task
            except Exception as e:
                pytest.fail(
                    f"_establish_connection call via connect() failed during test setup: {e}"
                )
            mock_aiohttp_session_ws_connect_method.assert_called_once()
            await asyncio.sleep(0.01)
            assert local_ws_manager.is_connected is True, (
                "Manager should be connected after successful connect() and task startup."
            )

            listener_task_name = f"{local_exchange_name}_ws_listen"
            ping_task_name = f"{local_exchange_name}_ws_ping"
            assert listener_task_name in created_tasks_map
            listener_task_for_close = created_tasks_map[listener_task_name]

            if default_ws_manager_config.ping_interval > 0:
                assert ping_task_name in created_tasks_map
                ping_task_for_close = created_tasks_map[ping_task_name]
            else:  # pragma: no cover
                assert ping_task_name not in created_tasks_map

            await local_ws_manager.close()

            assert local_ws_manager.is_connected is False
            # DEFENSIVE CHECK: _ws_connection cleared by close(). Mypy=[unreachable] Ruff=[SLF001]
            assert local_ws_manager._ws_connection is None  # noqa: SLF001
            assert local_ws_manager._should_reconnect is False  # noqa: SLF001

            assert listener_task_for_close.cancelled()
            if ping_task_for_close:
                assert ping_task_for_close.done()

            actual_mock_ws_conn.close.assert_called_once()
            assert local_ws_manager._session is None  # noqa: SLF001

        finally:
            if local_ws_manager.is_connected:
                await local_ws_manager.close()
            if connection_establishment_task and not connection_establishment_task.done():
                connection_establishment_task.cancel()
                with suppress(asyncio.CancelledError):
                    await connection_establishment_task
            if listener_task_for_close and not listener_task_for_close.done():
                listener_task_for_close.cancel()
                with suppress(asyncio.CancelledError):
                    await listener_task_for_close
            if ping_task_for_close and not ping_task_for_close.done():
                ping_task_for_close.cancel()
                with suppress(asyncio.CancelledError):
                    await ping_task_for_close

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
            async def session_close_side_effect() -> None:
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

                await manager.close()  # This is the main action to test

                assert manager.is_connected is False
                mock_internal_session_instance.close.assert_called_once()

            finally:
                await manager.close()

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
        caplog: LogCaptureFixture,
    ) -> None:
        """Test _listen loop processes messages and triggers reconnect on unexpected close."""
        caplog.set_level(logging.INFO, logger="WebSocketManager.listen_reconnect_test")
        caplog.set_level(logging.DEBUG, logger="WebSocketManager.listen_reconnect_test")

        test_message_payload = {"type": "data", "value": "test_data"}
        simulated_connection_drop_exception = aiohttp.ClientConnectionError(
            "Simulated drop for WSMsgType.ERROR"
        )

        first_connection_mock = mock_ws_connection_factory(
            receive_sequence=[
                (WSMsgType.TEXT, json.dumps(test_message_payload), None),
                (WSMsgType.ERROR, simulated_connection_drop_exception, None),
            ],
            closed=False,
        )
        first_connection_mock.exception = MagicMock(
            return_value=simulated_connection_drop_exception
        )

        second_connection_mock = mock_ws_connection_factory(
            receive_sequence=[(WSMsgType.CLOSED, None, None)],
            closed=True,
        )
        second_connection_mock.exception = MagicMock(return_value=None)

        connect_attempt_count = 0

        async def dynamic_ws_connect_side_effect(*_args: object, **_kwargs: object) -> AsyncMock:
            nonlocal connect_attempt_count
            connect_attempt_count += 1
            if connect_attempt_count == 1:
                return first_connection_mock
            return second_connection_mock

        mock_aiohttp_session_ws_connect_method.side_effect = dynamic_ws_connect_side_effect
        mock_user_message_handler = AsyncMock()
        test_config = default_ws_manager_config.model_copy(
            update={"max_reconnect_attempts": 2, "reconnect_delay": 0.01}
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
            with patch.object(manager, "_logger") as mock_logger_patch:
                initial_connect_task = manager.connect()
                assert initial_connect_task is not None
                await initial_connect_task
                await asyncio.sleep(1.0)

                mock_user_message_handler.assert_called_once_with(test_message_payload)
                assert first_connection_mock.receive.call_count == 2

                expected_log_message = (
                    f"WebSocket connection error: {simulated_connection_drop_exception!r}"
                )
                found_error_log = False
                for call_args in mock_logger_patch.error.call_args_list:
                    logged_message = call_args[0][0]
                    if expected_log_message in logged_message:
                        found_error_log = True
                        break
                assert found_error_log, (
                    f"Expected error log for WSMsgType.ERROR not found. "
                    f"Expected part: '{expected_log_message}'. "
                    f"Actual error calls: {mock_logger_patch.error.call_args_list}"
                )

                mock_sleep.assert_called_once_with(1.0)

                assert mock_aiohttp_session_ws_connect_method.call_count == 2

                assert listener_task_name_listen_test in created_tasks_map_listen_test
                restarted_listen_task = created_tasks_map_listen_test[
                    listener_task_name_listen_test
                ]
                assert restarted_listen_task is not None

                if not restarted_listen_task.done():
                    try:
                        await asyncio.wait_for(restarted_listen_task, timeout=0.5)
                    except TimeoutError:
                        manager._logger.warning(
                            "[TEST] Restarted listener task timed out waiting for completion."
                        )
                        restarted_listen_task.cancel()
                        await asyncio.gather(restarted_listen_task, return_exceptions=True)
                    except Exception as e_wait:
                        manager._logger.error(
                            f"[TEST] Error awaiting restarted_listen_task: {e_wait!r}"
                        )

                await asyncio.sleep(0.1)

                assert manager.is_connected is False
        finally:
            print("\n--- Captured logs for listen_reconnect_test ---")
            for record in caplog.records:
                print(f"{record.levelname}: {record.name}: {record.getMessage()}")
            print("--- End captured logs ---")
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
