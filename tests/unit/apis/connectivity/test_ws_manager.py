"""Unit tests for WebSocketManager.

Tests WebSocket connection management, message handling, and error scenarios.
"""

import asyncio
import asyncio.tasks  # Import for direct access to create_task
import logging
from collections.abc import AsyncGenerator, Awaitable, Callable, Coroutine, Iterable
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import ClientSession as RealAiohttpCliSession
from aiohttp import WSMessage, WSMsgType
from aiohttp.helpers import sentinel
from pydantic import AnyUrl, BaseModel, ValidationError
from pytest import LogCaptureFixture

from cyberdelta.apis.connectivity.connectivity_models import WebSocketManagerConfig
from cyberdelta.apis.connectivity.ws_manager import (
    WebSocketManager,
)

logger = logging.getLogger(__name__)


# Define a dummy message handler for tests
async def dummy_message_handler(message: dict[str, Any]) -> None:
    """Handle WebSocket messages for testing purposes."""
    pass


async def dummy_on_connected_callback() -> None:
    """Handle WebSocket connection events for testing purposes."""
    pass


class MockMessage(BaseModel):
    """Simple BaseModel for testing WebSocket messages."""

    test: str


# Helper function for mock side_effect
def create_async_mock_task_for_side_effect(
    *args: object,
    **kwargs: object,  # Generic pass-through for mock/callback
) -> AsyncMock:
    """Create an AsyncMock, intended for use as a side_effect."""
    return AsyncMock()


async def completed_dummy_coro() -> None:
    """Complete immediately as a dummy coroutine."""


@pytest.fixture
def default_ws_manager_config() -> WebSocketManagerConfig:
    """Provide a default WebSocketManagerConfig for tests."""
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
    """Provide a WebSocketManager instance for testing."""
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
    """Provide a mock aiohttp.ClientSession instance."""
    session_mock = AsyncMock(spec=aiohttp.ClientSession)
    session_mock.closed = False
    session_mock.close = AsyncMock()
    # ws_connect will be attached by patched_ws_connect fixture if that fixture uses this one.
    # For global patching, this fixture might not be directly used by patched_ws_connect.
    return session_mock


def _create_mock_receive_behavior(
    mock_conn: AsyncMock, 
    receive_sequence: Iterable[Any] | None,
    block_indefinitely: bool
) -> Callable[[], Awaitable[WSMessage]]:
    """Create the mock receive behavior for a WebSocket connection."""
    seq_iterator = iter(receive_sequence) if receive_sequence else None

    async def mock_receive_internal() -> WSMessage:
        await asyncio.tasks.sleep(0)  # Ensure yield
        if seq_iterator:
            try:
                item = next(seq_iterator)
                if isinstance(item, Exception):
                    raise item
                return WSMessage(item[0], item[1], item[2])
            except StopIteration as e_stop:
                mock_conn.closed = True
                raise TimeoutError("Mocked receive sequence exhausted, timing out.") from e_stop
        mock_conn.closed = True
        
        if block_indefinitely:
            # Block indefinitely when receive_sequence is None or exhausted
            future: asyncio.Future[WSMessage] = asyncio.Future()
            return await future  # This will block forever unless cancelled
        
        raise TimeoutError("Mocked receive timeout")
    
    return mock_receive_internal


def _setup_ping_pong_behavior(mock_conn: AsyncMock, ping_pong_passthrough: bool) -> None:
    """Setup ping/pong behavior for mock WebSocket connection."""
    if ping_pong_passthrough:
        async def mock_ping() -> None:
            pass
        async def mock_pong() -> None:
            pass
        mock_conn.ping = AsyncMock(side_effect=mock_ping)
        mock_conn.pong = AsyncMock(side_effect=mock_pong)


@pytest.fixture
def mock_ws_connection_factory() -> Callable[..., AsyncMock]:
    """Create factory to create mock WebSocket connection objects."""

    def _factory(
        closed: bool = False,
        receive_sequence: Iterable[Any] | None = None,
        ping_pong_passthrough: bool = False,
        block_indefinitely: bool = False,
        spec_arg: type[aiohttp.ClientWebSocketResponse]
        | None = aiohttp.ClientWebSocketResponse,
    ) -> AsyncMock:
        mock_conn = AsyncMock(spec=spec_arg)
        mock_conn.closed = closed
        
        # Setup receive behavior
        mock_receive_func = _create_mock_receive_behavior(
            mock_conn, receive_sequence, block_indefinitely
        )
        mock_conn.receive = AsyncMock(side_effect=mock_receive_func)
        mock_conn.__aiter__ = MagicMock(return_value=mock_conn)

        # Create anext behavior  
        async def anext_for_mock() -> WSMessage:
            result = await mock_conn.receive()
            # Mock returns Any but we know it's WSMessage based on our test setup
            return result  # type: ignore[no-any-return]

        mock_conn.__anext__ = AsyncMock(side_effect=anext_for_mock)

        # Setup basic mock methods
        mock_conn.send_str = AsyncMock()
        mock_conn.send_bytes = AsyncMock()
        mock_conn.ping = AsyncMock()
        mock_conn.pong = AsyncMock()
        mock_conn.close = AsyncMock(return_value=True)
        mock_conn.exception = MagicMock(return_value=None)
        
        # Setup ping/pong behavior if needed
        _setup_ping_pong_behavior(mock_conn, ping_pong_passthrough)

        return mock_conn

    return _factory


@pytest_asyncio.fixture
async def patched_ws_connect(
    mock_ws_connection_factory: Callable[..., AsyncMock],
) -> AsyncGenerator[tuple[AsyncMock, AsyncMock]]:
    """Patch aiohttp.ClientSession.ws_connect globally and provide a default mock WS connection."""
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
            ws_manager_instance,
            "_establish_connection",
            side_effect=establish_conn_coro_mock,
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
                ws_manager_instance,
                "_establish_connection",
                side_effect=establish_conn_coro_mock_2,
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
            logger.debug("\nCaptured logs for test_connection_success_flow:")
            logger.debug(caplog.text)
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
            update={"max_reconnect_attempts": 1, "reconnect_delay": 0.01},
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

def _create_task_capture_side_effect(
    original_create_task: Callable[..., asyncio.Task[Any]],
    created_tasks_map: dict[str, asyncio.Task[Any]]
) -> Callable[..., asyncio.Task[Any]]:
    """Create side effect for capturing created tasks."""
    def side_effect_for_create_task_capture(
        coro: Coroutine[Any, Any, Any],
        *args_capture: object,
        name: str | None = None,
        **kwargs_capture: object,
    ) -> asyncio.Task[Any]:
        """Create side effect for create task capture."""
        task = original_create_task(coro, name=name)
        if name:
            created_tasks_map[name] = task
        return task
    return side_effect_for_create_task_capture


def _setup_test_close_environment(
    mock_aiohttp_session_ws_connect_method: AsyncMock,
    default_ws_manager_config: WebSocketManagerConfig,
    mock_ws_connection_factory: Callable[..., AsyncMock],
) -> tuple[WebSocketManager, dict[str, asyncio.Task[Any]], str]:
    """Setup test environment for close functionality test."""
    actual_mock_ws_conn = mock_ws_connection_factory(closed=False, block_indefinitely=True)
    mock_aiohttp_session_ws_connect_method.return_value = actual_mock_ws_conn

    created_tasks_map: dict[str, asyncio.Task[Any]] = {}
    original_asyncio_create_task = asyncio.tasks.create_task
    
    capturing_task_factory = MagicMock(
        side_effect=_create_task_capture_side_effect(
            original_asyncio_create_task, created_tasks_map
        )
    )

    local_exchange_name = "test_close_local_manager_ws"
    local_ws_manager = WebSocketManager(
        exchange_name=local_exchange_name,
        message_handler=dummy_message_handler,
        config=default_ws_manager_config,
        on_connected_callback=dummy_on_connected_callback,
        session=None,
        task_factory=capturing_task_factory,
    )
    
    return local_ws_manager, created_tasks_map, local_exchange_name


async def _establish_connection_and_verify(
    local_ws_manager: WebSocketManager,
    mock_aiohttp_session_ws_connect_method: AsyncMock,
) -> asyncio.Task[Any]:
    """Establish connection and verify it was successful."""
    connection_establishment_task = local_ws_manager.connect()
    assert connection_establishment_task is not None
    try:
        await connection_establishment_task
    except Exception as e:
        pytest.fail(
            f"_establish_connection call via connect() failed during test setup: {e}",
        )
    mock_aiohttp_session_ws_connect_method.assert_called_once()
    await asyncio.sleep(0.01)
    assert local_ws_manager.is_connected is True, (
        "Manager should be connected after successful connect() and task startup."
    )
    return connection_establishment_task


def _verify_tasks_created(
    created_tasks_map: dict[str, asyncio.Task[Any]],
    local_exchange_name: str,
    default_ws_manager_config: WebSocketManagerConfig,
) -> tuple[asyncio.Task[Any] | None, asyncio.Task[Any] | None]:
    """Verify that expected tasks were created."""
    listener_task_name = f"{local_exchange_name}_ws_listen"
    ping_task_name = f"{local_exchange_name}_ws_ping"
    
    assert listener_task_name in created_tasks_map
    listener_task_for_close = created_tasks_map[listener_task_name]

    ping_task_for_close = None
    if default_ws_manager_config.ping_interval > 0:
        assert ping_task_name in created_tasks_map
        ping_task_for_close = created_tasks_map[ping_task_name]
    else:  # pragma: no cover
        assert ping_task_name not in created_tasks_map
        
    return listener_task_for_close, ping_task_for_close


async def _perform_close_and_verify(
    local_ws_manager: WebSocketManager,
    mock_aiohttp_session_ws_connect_method: AsyncMock,
    actual_mock_ws_conn: AsyncMock,
    listener_task_for_close: asyncio.Task[Any] | None,
    ping_task_for_close: asyncio.Task[Any] | None,
) -> None:
    """Perform close operation and verify results."""
    await local_ws_manager.close()

    assert local_ws_manager.is_connected is False
    # After close(), the manager should be in a state where it won't reconnect
    # We can verify this by attempting to send a message, which should fail
    # Test that sending fails when connection is closed
    test_message = MockMessage(test="message")
    send_success = await local_ws_manager.send_json(test_message)
    assert not send_success

    # Assert that ws_connect was not called AGAIN during or after close
    mock_aiohttp_session_ws_connect_method.assert_called_once()

    # Check session was closed
    assert actual_mock_ws_conn.close.call_count == 1

    # Check _listen_task and _ping_task were cancelled if they existed (they shouldn't here)
    if listener_task_for_close:
        assert listener_task_for_close.cancelled()
    if ping_task_for_close:
        assert ping_task_for_close.done()
    # Session cleanup is verified through the fact that no exceptions
    # occurred during close()


async def _cleanup_test_tasks(
    local_ws_manager: WebSocketManager,
    connection_establishment_task: asyncio.Task[Any] | None,
    listener_task_for_close: asyncio.Task[Any] | None,
    ping_task_for_close: asyncio.Task[Any] | None,
) -> None:
    """Clean up test tasks."""
    if local_ws_manager.is_connected:
        await local_ws_manager.close()
    
    tasks_to_cancel = [
        (connection_establishment_task, "connection_establishment_task"),
        (listener_task_for_close, "listener_task_for_close"),
        (ping_task_for_close, "ping_task_for_close"),
    ]
    
    for task, _task_name in tasks_to_cancel:
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect", new_callable=AsyncMock)
    async def test_close_cancels_tasks_and_closes_connection_session(
        self: TestWebSocketManager,
        mock_aiohttp_session_ws_connect_method: AsyncMock,
        default_ws_manager_config: WebSocketManagerConfig,
        mock_ws_connection_factory: Callable[..., AsyncMock],
    ) -> None:
        """Test close() cancels internal tasks and closes WS connection and session."""
        # Setup test environment
        local_ws_manager, created_tasks_map, local_exchange_name = _setup_test_close_environment(
            mock_aiohttp_session_ws_connect_method,
            default_ws_manager_config,
            mock_ws_connection_factory,
        )
        
        actual_mock_ws_conn = mock_aiohttp_session_ws_connect_method.return_value
        connection_establishment_task: asyncio.Task[Any] | None = None
        listener_task_for_close: asyncio.Task[Any] | None = None
        ping_task_for_close: asyncio.Task[Any] | None = None

        try:
            # Establish connection and verify
            connection_establishment_task = await _establish_connection_and_verify(
                local_ws_manager, mock_aiohttp_session_ws_connect_method
            )
            
            # Verify tasks were created
            listener_task_for_close, ping_task_for_close = _verify_tasks_created(
                created_tasks_map, local_exchange_name, default_ws_manager_config
            )
            
            # Assert that connection happened once during setup
            mock_aiohttp_session_ws_connect_method.assert_called_once()
            
            # Perform close and verify
            await _perform_close_and_verify(
                local_ws_manager, mock_aiohttp_session_ws_connect_method, actual_mock_ws_conn,
                listener_task_for_close, ping_task_for_close
            )

        finally:
            await _cleanup_test_tasks(
                local_ws_manager,
                connection_establishment_task,
                listener_task_for_close,
                ping_task_for_close,
            )

    @pytest.mark.asyncio
    async def test_close_when_not_connected_closes_idle_internal_session(
        self: TestWebSocketManager,
        default_ws_manager_config: WebSocketManagerConfig,
        patched_ws_connect: tuple[AsyncMock, AsyncMock],
    ) -> None:
        """Test close() on a never-connected manager with an internal session closes it."""
        _mock_ws_connect_method, _mock_ws_connection = patched_ws_connect

        with patch(
            "cyberdelta.apis.connectivity.ws_manager.aiohttp.ClientSession",
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

def _setup_listen_test_logger() -> logging.Logger:
    """Setup logger for listen loop test."""
    test_case_logger = logging.getLogger("UnitTestListenLoopReconnect")
    test_case_logger.setLevel(logging.INFO)
    test_case_logger.info(
        "--- Test: test_listen_loop_processes_message_and_reconnects_on_close starting ---",
    )
    return test_case_logger


def _create_dynamic_ws_connect_side_effect(
    mock_ws_connection_factory: Callable[..., AsyncMock],
    test_case_logger: logging.Logger,
) -> tuple[Callable[..., Awaitable[AsyncMock]], AsyncMock]:
    """Create dynamic WebSocket connect side effect for testing."""
    second_connection_mock = mock_ws_connection_factory(
        receive_sequence=[(WSMsgType.CLOSED, None, None)],
        closed=True,
    )
    second_connection_mock.exception = MagicMock(return_value=None)
    
    connect_attempt_count = 0

    async def dynamic_ws_connect_side_effect(*_args: object, **_kwargs: object) -> AsyncMock:
        nonlocal connect_attempt_count
        connect_attempt_count += 1
        test_case_logger.info(
            f"[dynamic_ws_connect_side_effect] Called. Attempt: {connect_attempt_count}",
        )
        if connect_attempt_count == 1:
            test_case_logger.info(
                "[dynamic_ws_connect_side_effect] Attempt 1: Raising ClientConnectorError.",
            )
            raise aiohttp.ClientConnectorError(
                MagicMock(),
                OSError("Simulated immediate connection failure for attempt 1"),
            )

        test_case_logger.info(
            f"[dynamic_ws_connect_side_effect] Attempt {connect_attempt_count}: "
            f"Returning second_connection_mock.",
        )
        return second_connection_mock
    
    return dynamic_ws_connect_side_effect, second_connection_mock


def _setup_listen_test_manager(
    default_ws_manager_config: WebSocketManagerConfig,
    test_case_logger: logging.Logger,
    mock_create_task: MagicMock,
) -> tuple[WebSocketManager, AsyncMock, dict[str, asyncio.Task[Any]]]:
    """Setup WebSocket manager for listen test."""
    mock_user_message_handler = AsyncMock()  # Will not be called
    test_config = default_ws_manager_config.model_copy(
        update={"max_reconnect_attempts": 2, "reconnect_delay": 0.01},
    )
    test_case_logger.info(f"Creating WebSocketManager with config: {test_config}")
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
        """Create side effect for create task capture listen test."""
        task = original_asyncio_create_task_listen_test(coro, name=name)
        if name:
            created_tasks_map_listen_test[name] = task
        return task

    mock_create_task.side_effect = side_effect_for_create_task_capture_listen_test
    
    return manager, mock_user_message_handler, created_tasks_map_listen_test


def _verify_log_messages(mock_logger_patch: MagicMock) -> None:
    """Verify expected log messages are present."""
    found_first_attempt_fail_log = False
    
    # Check warnings first
    for call_args in mock_logger_patch.warning.call_args_list:
        logged_message = str(call_args)
        if (
            "Connection attempt 1 failed" in logged_message
            and "Cannot connect to host" in logged_message
        ):
            found_first_attempt_fail_log = True
            break
    
    # Check errors if not found in warnings
    if not found_first_attempt_fail_log:
        for call_args in mock_logger_patch.error.call_args_list:
            logged_message = str(call_args)
            if (
                "Connection attempt 1 failed" in logged_message
                and "Cannot connect to host" in logged_message
            ):
                found_first_attempt_fail_log = True
                break
    
    assert found_first_attempt_fail_log, (
        f"Expected log for first connection attempt failing with 'Cannot connect to "
        f"host' not found. Warnings: {mock_logger_patch.warning.call_args_list}, "
        f"Errors: {mock_logger_patch.error.call_args_list}"
    )


def _verify_sleep_calls(mock_sleep: AsyncMock, test_config: WebSocketManagerConfig) -> None:
    """Verify expected sleep calls were made."""
    assert mock_sleep.call_count == 2, (
        f"Expected 2 calls to sleep, got {mock_sleep.call_count}. "
        f"Calls: {mock_sleep.call_args_list}"
    )

    found_retry_sleep = False
    found_test_sleep = False

    effective_min_retry_sleep = 1.0
    effective_max_retry_sleep = (
        1.0 + abs(test_config.reconnect_delay * 0.2 * 0.5) + 0.01
    )

    for call_item in mock_sleep.call_args_list:
        args, _kwargs = call_item
        sleep_duration = args[0]
        if effective_min_retry_sleep <= sleep_duration <= effective_max_retry_sleep:
            found_retry_sleep = True
        elif sleep_duration == 0.05:
            found_test_sleep = True

    assert found_retry_sleep, (
        f"Expected a retry sleep (approx {effective_min_retry_sleep}-"
        f"{effective_max_retry_sleep}s). Calls: {mock_sleep.call_args_list}"
    )
    assert found_test_sleep, (
        f"Expected a test sleep (0.05s). Calls: {mock_sleep.call_args_list}"
    )


async def _handle_restarted_listen_task(
    created_tasks_map_listen_test: dict[str, asyncio.Task[Any]],
    listener_task_name_listen_test: str,
    test_case_logger: logging.Logger,
) -> None:
    """Handle restarted listen task."""
    assert listener_task_name_listen_test in created_tasks_map_listen_test
    restarted_listen_task = created_tasks_map_listen_test[listener_task_name_listen_test]
    assert restarted_listen_task is not None

    if not restarted_listen_task.done():
        try:
            await asyncio.wait_for(restarted_listen_task, timeout=0.5)
        except TimeoutError:
            test_case_logger.warning(
                "[TEST] Restarted listener task timed out waiting for completion.",
            )
            restarted_listen_task.cancel()
            await asyncio.gather(restarted_listen_task, return_exceptions=True)
        except Exception as e_wait:
            test_case_logger.error(
                f"[TEST] Error awaiting restarted_listen_task: {e_wait!r}",
            )

    await asyncio.sleep(0.1)
    test_case_logger.info(
        "Slept 0.1s after awaiting restarted_listen_task potentially.",
    )


    @patch("aiohttp.ClientSession.ws_connect")
    @patch("cyberdelta.apis.connectivity.ws_manager.asyncio.create_task")
    @patch("asyncio.sleep", new_callable=AsyncMock)
    @pytest.mark.asyncio
    async def test_listen_loop_processes_message_and_reconnects_on_close(
        self: TestWebSocketManager,
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

        # Setup test components
        test_case_logger = _setup_listen_test_logger()
        
        (
            dynamic_ws_connect_side_effect,
            _second_connection_mock,
        ) = _create_dynamic_ws_connect_side_effect(
            mock_ws_connection_factory, test_case_logger
        )
        mock_aiohttp_session_ws_connect_method.side_effect = dynamic_ws_connect_side_effect
        
        (
            manager,
            mock_user_message_handler,
            created_tasks_map_listen_test,
        ) = _setup_listen_test_manager(
            default_ws_manager_config, test_case_logger, mock_create_task
        )
        
        listener_task_name_listen_test = "listen_reconnect_test_ws_listen"

        try:
            with patch.object(manager, "_logger") as mock_logger_patch:
                test_case_logger.info("Attempting initial manager.connect()")
                initial_connect_task = manager.connect()
                assert initial_connect_task is not None
                test_case_logger.info(
                    f"Initial connect task created: {initial_connect_task.get_name()}",
                )
                await initial_connect_task
                test_case_logger.info(
                    f"Initial connect task awaited. State: "
                    f"done={initial_connect_task.done()}, "
                    f"cancelled={initial_connect_task.cancelled()}",
                )
                await asyncio.sleep(0.05)
                test_case_logger.info("Slept 0.05s after initial connect.")

                # Verify message handler was not called
                mock_user_message_handler.assert_not_called()

                # Verify log messages
                _verify_log_messages(mock_logger_patch)

                # Verify sleep calls
                _verify_sleep_calls(mock_sleep, default_ws_manager_config.model_copy(
                    update={"max_reconnect_attempts": 2, "reconnect_delay": 0.01}
                ))

                # Verify connection attempts
                assert mock_aiohttp_session_ws_connect_method.call_count == 2

                # Handle restarted listen task
                await _handle_restarted_listen_task(
                    created_tasks_map_listen_test, listener_task_name_listen_test, test_case_logger
                )

                assert manager.is_connected is False
                test_case_logger.info("Asserted manager.is_connected is False.")
        finally:
            # This ensures that the finally block in _listen_loop is reached upon cancellation
            test_case_logger.info(
                "--- Test: test_listen_loop_processes_message_and_reconnects_on_close "
                "entering finally block ---",
            )
            logger.debug("\n--- Captured logs for listen_reconnect_test ---")
            for record in caplog.records:
                logger.debug(f"{record.levelname}: {record.name}: {record.getMessage()}")
            logger.debug("--- End captured logs ---")
            await manager.close()

    def test_config_validation_invalid_url(
        self: TestWebSocketManager,
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test that config validation fails with invalid WebSocket URL."""
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
        self: TestWebSocketManager,
        field: str,
        invalid_value: int,
        error_part: str,
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test that config validation enforces numeric bounds on fields."""
        valid_data = default_ws_manager_config.model_dump()
        if field not in valid_data:
            pytest.skip(f"Field {field} not in WebSocketManagerConfig model keys.")
        valid_data[field] = invalid_value
        with pytest.raises(ValidationError) as exc_info:
            WebSocketManagerConfig(**valid_data)
        assert error_part.lower() in str(exc_info.value).lower()

    def test_config_frozen_and_extra_forbid(
        self: TestWebSocketManager,
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test that config model is frozen and forbids extra fields."""
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
    """Simple test to verify pytest-asyncio functionality."""
    await asyncio.sleep(0.001)
    assert True


class TestWebSocketManagerComprehensiveErrorHandling:
    """Comprehensive edge case and failure scenario testing for WebSocketManager."""

    # =============================================================================
    # I. CONNECTION FAILURE SCENARIOS
    # =============================================================================

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_connection_timeout_during_handshake(
        self,
        mock_ws_connect: AsyncMock,
        default_ws_manager_config: WebSocketManagerConfig,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test connection timeout during WebSocket handshake."""
        # Configure mock to raise timeout during connection
        mock_ws_connect.side_effect = TimeoutError("Connection handshake timeout")

        manager = WebSocketManager(
            exchange_name="test_exchange",
            message_handler=dummy_message_handler,
            config=default_ws_manager_config,
            on_connected_callback=dummy_on_connected_callback,
        )

        # Attempt connection - should fail and retry
        connect_task = manager.connect()
        if connect_task:
            await connect_task

        # Wait for connection attempts to complete
        await asyncio.sleep(0.2)  # Allow retries to happen

        # Should not be connected after timeout
        assert not manager.is_connected

        # Should have logged timeout errors
        assert "Connection handshake timeout" in caplog.text or "TimeoutError" in caplog.text

        await manager.close()

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_connection_refused_error(
        self,
        mock_ws_connect: AsyncMock,
        default_ws_manager_config: WebSocketManagerConfig,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test connection refused error handling."""
        # Configure mock to raise connection refused
        mock_ws_connect.side_effect = OSError("Connection refused")

        manager = WebSocketManager(
            exchange_name="test_exchange",
            message_handler=dummy_message_handler,
            config=default_ws_manager_config,
            on_connected_callback=dummy_on_connected_callback,
        )

        connect_task = manager.connect()
        if connect_task:
            await connect_task
        await asyncio.sleep(0.2)  # Allow retries

        assert not manager.is_connected
        assert "Connection refused" in caplog.text or "OSError" in caplog.text

        await manager.close()

    # =============================================================================
    # II. MESSAGE HANDLING FAILURE SCENARIOS
    # =============================================================================

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_message_handler_exception_isolation(
        self,
        mock_ws_connect: AsyncMock,
        default_ws_manager_config: WebSocketManagerConfig,
        mock_ws_connection_factory: Callable[..., AsyncMock],
        caplog: LogCaptureFixture,
    ) -> None:
        """Test that message handler exceptions don't crash the connection."""

        # Create a message handler that raises an exception
        async def failing_message_handler(message: dict[str, Any]) -> None:
            raise ValueError("Message handler intentionally failed")

        # Create mock connection with a message sequence
        mock_conn = mock_ws_connection_factory(
            receive_sequence=[
                (WSMsgType.TEXT, '{"test": "message1"}', None),
                (WSMsgType.TEXT, '{"test": "message2"}', None),
                (WSMsgType.CLOSE, None, None),
            ],
        )
        # Make ws_connect properly awaitable using side_effect
        mock_ws_connect.side_effect = AsyncMock(return_value=mock_conn)

        manager = WebSocketManager(
            exchange_name="test_exchange",
            message_handler=failing_message_handler,
            config=default_ws_manager_config,
            on_connected_callback=dummy_on_connected_callback,
        )

        connect_task = manager.connect()
        if connect_task:
            await connect_task
        await asyncio.sleep(0.2)  # Allow message processing

        # Connection should still be active despite handler failures
        assert "Message handler intentionally failed" in caplog.text
        assert "Error processing WebSocket message" in caplog.text

        await manager.close()

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_malformed_json_message_handling(
        self,
        mock_ws_connect: AsyncMock,
        default_ws_manager_config: WebSocketManagerConfig,
        mock_ws_connection_factory: Callable[..., AsyncMock],
        caplog: LogCaptureFixture,
    ) -> None:
        """Test handling of malformed JSON messages."""
        # Track processed messages
        processed_messages: list[dict[str, Any]] = []

        async def tracking_message_handler(message: dict[str, Any]) -> None:
            processed_messages.append(message)

        # Create mock connection with malformed JSON
        mock_conn = mock_ws_connection_factory(
            receive_sequence=[
                (WSMsgType.TEXT, '{"valid": "json"}', None),
                (WSMsgType.TEXT, "{invalid json}", None),  # Malformed
                (WSMsgType.TEXT, '{"another": "valid"}', None),
                (WSMsgType.CLOSE, None, None),
            ],
        )
        # Make ws_connect properly awaitable using side_effect
        mock_ws_connect.side_effect = AsyncMock(return_value=mock_conn)

        manager = WebSocketManager(
            exchange_name="test_exchange",
            message_handler=tracking_message_handler,
            config=default_ws_manager_config,
            on_connected_callback=dummy_on_connected_callback,
        )

        connect_task = manager.connect()
        if connect_task:
            await connect_task
        await asyncio.sleep(0.2)

        # Should have processed valid messages and logged error for invalid
        assert len(processed_messages) == 2
        assert processed_messages[0] == {"valid": "json"}
        assert processed_messages[1] == {"another": "valid"}
        assert "Received non-JSON WebSocket message" in caplog.text

        await manager.close()

    # =============================================================================
    # III. SEND OPERATION FAILURE SCENARIOS
    # =============================================================================

    @pytest.mark.asyncio
    async def test_send_when_not_connected(
        self,
        default_ws_manager_config: WebSocketManagerConfig,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test send operation when WebSocket is not connected."""
        manager = WebSocketManager(
            exchange_name="test_exchange",
            message_handler=dummy_message_handler,
            config=default_ws_manager_config,
            on_connected_callback=dummy_on_connected_callback,
        )

        # Try to send without connecting
        success = await manager.send_json(MockMessage(test="message"))

        assert not success
        assert "Cannot send JSON, WebSocket not connected" in caplog.text

        await manager.close()

    # =============================================================================
    # IV. RESOURCE MANAGEMENT AND CLEANUP SCENARIOS
    # =============================================================================

    @pytest.mark.asyncio
    async def test_double_close_safety(
        self,
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test that calling close() multiple times is safe."""
        manager = WebSocketManager(
            exchange_name="test_exchange",
            message_handler=dummy_message_handler,
            config=default_ws_manager_config,
            on_connected_callback=dummy_on_connected_callback,
        )

        # Close multiple times
        await manager.close()
        await manager.close()
        await manager.close()

        # Should not raise exceptions or cause issues
        assert not manager.is_connected

    # =============================================================================
    # V. CONFIGURATION EDGE CASES
    # =============================================================================

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_zero_max_reconnect_attempts(
        self,
        mock_ws_connect: AsyncMock,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test behavior with zero max reconnect attempts."""
        config = WebSocketManagerConfig(
            ws_url=AnyUrl("ws://test.websocket.api/ws"),
            connection_timeout=1.0,
            max_reconnect_attempts=0,  # No retries
            reconnect_delay=0.1,
            ping_interval=30.0,
        )

        mock_ws_connect.side_effect = OSError("Connection failed")

        manager = WebSocketManager(
            exchange_name="test_exchange",
            message_handler=dummy_message_handler,
            config=config,
            on_connected_callback=dummy_on_connected_callback,
        )

        connect_task = manager.connect()
        if connect_task:
            await connect_task
        await asyncio.sleep(0.2)

        # Should fail immediately without retries
        assert not manager.is_connected

        await manager.close()
