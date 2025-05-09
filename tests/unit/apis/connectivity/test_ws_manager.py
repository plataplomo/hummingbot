import asyncio
import json
from collections.abc import AsyncGenerator
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
        max_reconnect_attempts=3,
        reconnect_delay=1.0,
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
        session=None,
    )
    # Allow the manager to potentially create its session if connect is called
    yield manager
    # Ensure manager resources are cleaned up; close() handles internal session.
    await manager.close()


class TestWebSocketManager:
    """Tests for the WebSocketManager class."""

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_connect_flow_successful_unauthenticated(
        self,
        mock_ws_connect: AsyncMock,
        ws_manager_instance: WebSocketManager,
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test successful unauthenticated connection flow: connect() calls _establish_connection()."""
        mock_ws_response = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        mock_ws_response.closed = False
        mock_ws_response.close_code = None
        mock_ws_connect.return_value = mock_ws_response

        # Mock the logger to verify log messages
        with patch.object(ws_manager_instance, "_logger") as mock_logger:
            # Mock _listen and _keep_alive to prevent them from running indefinitely or erroring
            with (
                patch.object(ws_manager_instance, "_listen", new_callable=AsyncMock) as mock_listen,
                patch.object(
                    ws_manager_instance, "_keep_alive", new_callable=AsyncMock
                ) as mock_keep_alive,
            ):
                await (
                    ws_manager_instance.connect()
                )  # This creates and awaits _establish_connection task

                # Verify that _establish_connection called ws_connect correctly
                expected_heartbeat = (
                    default_ws_manager_config.ping_interval * WebSocketManager.HEARTBEAT_FACTOR
                    if default_ws_manager_config.ping_interval > 0
                    else 0
                )

                # ws_manager creates an internal session if one isn't provided.
                # The ws_connect timeout is based on config.connection_timeout (aiohttp.ClientTimeout not directly used)
                mock_ws_connect.assert_called_once_with(
                    str(default_ws_manager_config.ws_url),
                    heartbeat=expected_heartbeat,
                    timeout=default_ws_manager_config.connection_timeout,
                    # max_msg_size is not passed by current ws_manager.py
                )
                assert ws_manager_instance.is_connected is True

                # Verify logging
                mock_logger.info.assert_any_call(
                    f"Successfully connected to {default_ws_manager_config.ws_url}."
                )
                mock_listen.assert_called_once()  # Check that _listen task was started
                if default_ws_manager_config.ping_interval > 0:
                    mock_keep_alive.assert_called_once()  # Check that _keep_alive task was started

    @pytest.mark.asyncio
    @patch(
        "aiohttp.ClientSession.ws_connect",
        side_effect=aiohttp.ClientConnectorError(MagicMock(), OSError("Connection failed")),
    )
    async def test_establish_connection_failure_logs_and_retries_then_gives_up(
        self,
        mock_ws_connect: AsyncMock,
        ws_manager_instance: WebSocketManager,
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test _establish_connection logs failures, retries, and gives up after max_reconnect_attempts."""
        # Configure for 1 initial attempt + 1 retry (total 2 attempts as max_reconnect_attempts is 1 here)
        ws_manager_instance._max_reconnect_attempts = 1  # noqa: SLF001 - Testing specific retry count
        ws_manager_instance._reconnect_delay = 0.01  # noqa: SLF001 - Short delay for test

        with (
            patch.object(ws_manager_instance, "_logger") as mock_logger,
            patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep,
        ):
            # connect() will call _establish_connection internally
            await ws_manager_instance.connect()

        assert ws_manager_instance.is_connected is False
        # Total attempts = 1 (initial) + _max_reconnect_attempts (1) = 2
        assert mock_ws_connect.call_count == 2
        mock_sleep.assert_called_once_with(
            pytest.approx(0.01, abs=0.005)
        )  # Check sleep before retry (with jitter allowance)

        # Check logging for failure and giving up
        assert any(
            "Connection attempt 1 failed" in call_args[0][0]
            for call_args in mock_logger.warning.call_args_list
        )
        assert any(
            "Reconnection attempt 2/1 in" in call_args[0][0]
            for call_args in mock_logger.info.call_args_list
        )  # Note: log says "2/1" if max_attempts is 1
        mock_logger.critical.assert_called_once()
        assert (
            f"Failed to connect to {default_ws_manager_config.ws_url} after 1 attempts. Giving up."
            in mock_logger.critical.call_args[0][0]
        )

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_close_successful_after_connection(
        self,
        mock_ws_connect: AsyncMock,
        ws_manager_instance: WebSocketManager,
    ) -> None:
        """Test successful close via manager.close() after a connection was established."""
        mock_ws_response = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        mock_ws_response.closed = False
        mock_ws_connect.return_value = mock_ws_response

        # Mock internal tasks that would be started on successful connection
        mock_listen_task = AsyncMock()
        mock_ping_task = AsyncMock()
        mock_conn_task = AsyncMock()  # The task that runs _establish_connection

        with (
            patch.object(ws_manager_instance, "_listen", return_value=mock_listen_task),
            patch.object(ws_manager_instance, "_keep_alive", return_value=mock_ping_task),
            patch.object(ws_manager_instance, "_establish_connection") as mock_establish,
        ):
            # Simulate successful connection by _establish_connection
            async def side_effect_establish_connection():
                ws_manager_instance._ws_connection = mock_ws_response  # noqa: SLF001
                ws_manager_instance._is_connected = True  # noqa: SLF001
                # Simulate tasks being created and stored
                ws_manager_instance._listener_task = asyncio.create_task(asyncio.sleep(0.01))  # noqa: SLF001
                ws_manager_instance._ping_task = asyncio.create_task(asyncio.sleep(0.01))  # noqa: SLF001

            mock_establish.side_effect = side_effect_establish_connection

            # Simulate that connect() creates a task for _establish_connection
            # and that this task is stored in self._connection_task
            async def connect_side_effect():
                # Store the task created by connect so close() can cancel it
                ws_manager_instance._connection_task = asyncio.create_task(mock_establish())  # noqa: SLF001
                await ws_manager_instance._connection_task  # noqa: SLF001

            with patch.object(
                ws_manager_instance, "connect", side_effect=connect_side_effect
            ) as mock_connect_method:
                await ws_manager_instance.connect()  # Call the patched connect
                mock_connect_method.assert_called_once()  # Ensure our patched connect was called

        assert ws_manager_instance.is_connected is True  # Pre-condition for this part of the test

        # Simulate an internally created session for testing session closure
        internal_session_mock = AsyncMock(spec=aiohttp.ClientSession)
        internal_session_mock.closed = False
        ws_manager_instance._session = internal_session_mock  # noqa: SLF001
        ws_manager_instance._external_session = False  # noqa: SLF001

        await ws_manager_instance.close()

        mock_ws_response.close.assert_called_once()
        assert ws_manager_instance.is_connected is False

        # Check tasks were cancelled
        # Accessing tasks via _<name> to check cancellation state
        if hasattr(ws_manager_instance, "_listener_task") and ws_manager_instance._listener_task:  # noqa: SLF001
            assert ws_manager_instance._listener_task.cancelled()  # noqa: SLF001
        if hasattr(ws_manager_instance, "_ping_task") and ws_manager_instance._ping_task:  # noqa: SLF001
            assert ws_manager_instance._ping_task.cancelled()  # noqa: SLF001
        if (
            hasattr(ws_manager_instance, "_connection_task")
            and ws_manager_instance._connection_task
        ):  # noqa: SLF001
            assert ws_manager_instance._connection_task.cancelled()  # noqa: SLF001

        assert ws_manager_instance._ws_connection is None  # noqa: SLF001
        internal_session_mock.close.assert_called_once()  # Check internal session was closed
        assert ws_manager_instance._session is None  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_close_when_not_connected_closes_internal_session(
        self, ws_manager_instance: WebSocketManager
    ) -> None:
        """Test close() when not connected still closes an internally managed session."""
        assert ws_manager_instance.is_connected is False  # Pre-condition

        internal_session_mock = AsyncMock(spec=aiohttp.ClientSession)
        internal_session_mock.closed = False

        ws_manager_instance._session = internal_session_mock  # noqa: SLF001
        ws_manager_instance._external_session = False  # noqa: SLF001

        await ws_manager_instance.close()

        assert ws_manager_instance.is_connected is False
        internal_session_mock.close.assert_called_once()
        assert ws_manager_instance._session is None  # noqa: SLF001

    @pytest.mark.asyncio
    @patch("aiohttp.ClientWebSocketResponse.send_json")  # Patch send_json on the response object
    async def test_send_json_successful_when_connected(
        self,
        mock_send_json: AsyncMock,
        ws_manager_instance: WebSocketManager,
    ) -> None:
        """Test send_json calls underlying method when connected."""
        # Simulate connected state
        ws_manager_instance._is_connected = True  # noqa: SLF001
        mock_ws_conn = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        ws_manager_instance._ws_connection = mock_ws_conn  # noqa: SLF001
        # Point the mock_ws_conn's send_json to our high-level mock_send_json
        # so we can assert it was called.
        mock_ws_conn.send_json = mock_send_json

        payload = {"command": "subscribe", "channel": "trades"}
        result = await ws_manager_instance.send_json(payload)

        assert result is True
        mock_send_json.assert_called_once_with(payload)

    @pytest.mark.asyncio
    async def test_send_json_returns_false_when_not_connected(
        self, ws_manager_instance: WebSocketManager
    ) -> None:
        """Test send_json returns False and logs if not connected."""
        ws_manager_instance._is_connected = False  # noqa: SLF001
        ws_manager_instance._ws_connection = None  # noqa: SLF001

        with patch.object(ws_manager_instance, "_logger") as mock_logger:
            result = await ws_manager_instance.send_json({"data": "test"})

        assert result is False
        mock_logger.error.assert_called_once()
        assert "Cannot send JSON, WebSocket not connected" in mock_logger.error.call_args[0][0]

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_listen_receives_and_handles_text_message(
        self,
        mock_ws_connect: AsyncMock,
        ws_manager_instance: WebSocketManager,
    ) -> None:
        """Test _listen loop receives a text message and calls message_handler."""
        mock_ws_response = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        test_payload = {"type": "update", "data": "test_data"}

        messages_to_yield = [
            aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, json.dumps(test_payload), None),
            # Simulate connection closing to stop the listener loop gracefully for the test
            aiohttp.WSMessage(aiohttp.WSMsgType.CLOSED, None, None),
        ]

        async def mock_aiter():
            for msg in messages_to_yield:
                yield msg

        mock_ws_response.__aiter__ = mock_aiter
        mock_ws_connect.return_value = mock_ws_response

        # Replace the dummy_message_handler with a mock for assertion
        mock_message_handler_for_test = AsyncMock()
        ws_manager_instance._message_handler = mock_message_handler_for_test  # noqa: SLF001

        # Manually set up state as if _establish_connection succeeded and started _listen
        ws_manager_instance._ws_connection = mock_ws_response  # noqa: SLF001
        ws_manager_instance._is_connected = True  # noqa: SLF001

        # Run the _listen method directly for this test
        listen_task = asyncio.create_task(ws_manager_instance._listen())  # noqa: SLF001

        try:
            await asyncio.wait_for(listen_task, timeout=1.0)
        except TimeoutError:
            listen_task.cancel()  # Ensure task is cancelled if it times out
            await asyncio.gather(listen_task, return_exceptions=True)
            pytest.fail("_listen task timed out")

        mock_message_handler_for_test.assert_called_once_with(test_payload)
        # _listen sets _is_connected to False when it exits after a CLOSED message
        assert ws_manager_instance.is_connected is False

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    async def test_listen_handles_json_decode_error_gracefully(
        self,
        mock_ws_connect: AsyncMock,
        ws_manager_instance: WebSocketManager,
    ) -> None:
        """Test _listen loop handles JSONDecodeError and logs a warning."""
        mock_ws_response = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        invalid_json_text = "this is not valid json"

        messages_to_yield = [
            aiohttp.WSMessage(aiohttp.WSMsgType.TEXT, invalid_json_text, None),
            aiohttp.WSMessage(aiohttp.WSMsgType.CLOSED, None, None),
        ]

        async def mock_aiter():
            for msg in messages_to_yield:
                yield msg

        mock_ws_response.__aiter__ = mock_aiter
        mock_ws_connect.return_value = mock_ws_response

        mock_message_handler_for_test = AsyncMock()
        ws_manager_instance._message_handler = mock_message_handler_for_test  # noqa: SLF001

        ws_manager_instance._ws_connection = mock_ws_response  # noqa: SLF001
        ws_manager_instance._is_connected = True  # noqa: SLF001

        with patch.object(ws_manager_instance, "_logger") as mock_logger:
            listen_task = asyncio.create_task(ws_manager_instance._listen())  # noqa: SLF001
            try:
                await asyncio.wait_for(listen_task, timeout=1.0)
            except TimeoutError:
                listen_task.cancel()
                await asyncio.gather(listen_task, return_exceptions=True)
                pytest.fail("_listen task timed out")

        mock_message_handler_for_test.assert_not_called()
        mock_logger.warning.assert_called_once()
        assert "Received non-JSON WebSocket message" in mock_logger.warning.call_args[0][0]
        assert ws_manager_instance.is_connected is False

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_establish_connection_reconnect_successful_first_attempt(
        self,
        mock_sleep: AsyncMock,
        mock_ws_connect: AsyncMock,
        ws_manager_instance: WebSocketManager,
    ) -> None:
        """Test _establish_connection succeeds on first attempt (simulating reconnect path)."""
        mock_ws_response = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        mock_ws_response.closed = False
        mock_ws_connect.return_value = mock_ws_response

        # Simulate conditions for _establish_connection to run as if it's a reconnect
        ws_manager_instance._ws_connection = None  # noqa: SLF001
        ws_manager_instance._is_connected = False  # noqa: SLF001
        ws_manager_instance._should_reconnect = True  # noqa: SLF001

        with (
            patch.object(ws_manager_instance, "_listen", new_callable=AsyncMock),
            patch.object(ws_manager_instance, "_keep_alive", new_callable=AsyncMock),
        ):
            await ws_manager_instance._establish_connection()  # noqa: SLF001

        mock_ws_connect.assert_called_once()
        assert ws_manager_instance.is_connected is True
        mock_sleep.assert_not_called()  # No sleep on the first successful attempt

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_establish_connection_reconnect_succeeds_after_failures(
        self,
        mock_sleep: AsyncMock,
        mock_ws_connect: AsyncMock,
        ws_manager_instance: WebSocketManager,
        # default_ws_manager_config: WebSocketManagerConfig, # Not needed if accessing instance._X
    ) -> None:
        """Test _establish_connection succeeds after a few failed attempts."""
        failed_attempt_exception = aiohttp.ClientConnectorError(MagicMock(), OSError("conn failed"))
        mock_ws_response_success = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        mock_ws_response_success.closed = False

        num_failures = 2
        side_effects = [failed_attempt_exception] * num_failures + [mock_ws_response_success]
        mock_ws_connect.side_effect = side_effects

        ws_manager_instance._ws_connection = None  # noqa: SLF001
        ws_manager_instance._is_connected = False  # noqa: SLF001
        ws_manager_instance._should_reconnect = True  # noqa: SLF001
        ws_manager_instance._max_reconnect_attempts = num_failures + 1  # noqa: SLF001
        # _reconnect_delay is already set from config during init

        with (
            patch.object(ws_manager_instance, "_listen", new_callable=AsyncMock),
            patch.object(ws_manager_instance, "_keep_alive", new_callable=AsyncMock),
        ):
            await ws_manager_instance._establish_connection()  # noqa: SLF001

        assert mock_ws_connect.call_count == num_failures + 1
        assert mock_sleep.call_count == num_failures

        # Verify sleep durations approximately match exponential backoff with jitter
        # For simplicity, just check they were called with positive, increasing values
        # This requires direct access to instance's _reconnect_delay for assertion
        current_delay_base = ws_manager_instance._reconnect_delay  # noqa: SLF001
        for i in range(num_failures):
            # Actual delay includes jitter, so check it's around the base
            assert mock_sleep.call_args_list[i].args[0] == pytest.approx(
                current_delay_base, rel=0.2, abs=1.0
            )
            current_delay_base *= 2  # For next expected base backoff

        assert ws_manager_instance.is_connected is True

    @pytest.mark.asyncio
    @patch("aiohttp.ClientSession.ws_connect")
    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_establish_connection_reconnect_max_attempts_exceeded(
        self,
        mock_sleep: AsyncMock,
        mock_ws_connect: AsyncMock,
        ws_manager_instance: WebSocketManager,
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test _establish_connection gives up after max_reconnect_attempts."""
        failed_attempt_exception = aiohttp.ClientConnectorError(MagicMock(), OSError("conn failed"))

        max_attempts_from_config = default_ws_manager_config.max_reconnect_attempts
        ws_manager_instance._max_reconnect_attempts = max_attempts_from_config  # noqa: SLF001

        mock_ws_connect.side_effect = [failed_attempt_exception] * max_attempts_from_config

        ws_manager_instance._ws_connection = None  # noqa: SLF001
        ws_manager_instance._is_connected = False  # noqa: SLF001
        ws_manager_instance._should_reconnect = True  # noqa: SLF001

        with patch.object(ws_manager_instance, "_logger") as mock_logger:
            await ws_manager_instance._establish_connection()  # noqa: SLF001

        assert mock_ws_connect.call_count == max_attempts_from_config
        assert mock_sleep.call_count == (
            max_attempts_from_config - 1 if max_attempts_from_config > 0 else 0
        )
        assert ws_manager_instance.is_connected is False
        mock_logger.critical.assert_called_once()
        assert "Failed to connect" in mock_logger.critical.call_args[0][0]
        assert "giving up" in mock_logger.critical.call_args[0][0]

    @pytest.mark.asyncio
    async def test_manual_connect_and_close_flow(
        self, default_ws_manager_config: WebSocketManagerConfig
    ) -> None:
        """Test manual connect() and close() flow, simulating public API usage."""
        manager = WebSocketManager(
            exchange_name="manual_flow_test",
            config=default_ws_manager_config,
            message_handler=dummy_message_handler,
            on_connected_callback=dummy_on_connected_callback,
        )

        mock_ws_response = AsyncMock(spec=aiohttp.ClientWebSocketResponse)
        mock_ws_response.closed = False

        # Patch ws_connect within the session that manager will create/use
        # This requires knowing how manager gets its session.
        # Assuming it uses aiohttp.ClientSession() if none is passed.
        with (
            patch(
                "aiohttp.ClientSession.ws_connect", return_value=mock_ws_response
            ) as mock_session_ws_connect,
            patch.object(manager, "_listen", new_callable=AsyncMock),
            patch.object(manager, "_keep_alive", new_callable=AsyncMock),
            patch.object(manager, "_logger") as mock_logger,
        ):
            await manager.connect()  # Should call _establish_connection, then session.ws_connect

            mock_session_ws_connect.assert_called_once()
            assert manager.is_connected is True
            mock_logger.info.assert_any_call(
                f"Successfully connected to {default_ws_manager_config.ws_url}."
            )

            # Now test close
            # To check internal session closure, we need a reference if manager._session is reset to None
            # For this test, let's assume close() handles it correctly if ws_response.close() is called.
            internal_session_ref = manager._session  # noqa: SLF001

            await manager.close()

            mock_ws_response.close.assert_called_once()
            assert manager.is_connected is False
            assert manager._ws_connection is None  # noqa: SLF001
            if internal_session_ref and not manager._external_session:  # noqa: SLF001
                assert internal_session_ref.closed

    def test_config_validation_invalid_url(self) -> None:
        """Test WebSocketManagerConfig validation for invalid URL."""
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
        invalid_value: Any,
        error_part: str,
        default_ws_manager_config: WebSocketManagerConfig,
    ) -> None:
        """Test WebSocketManagerConfig validation for numeric field bounds."""
        valid_data = default_ws_manager_config.model_dump()
        if field not in valid_data:  # Should always be in valid_data for these fields
            pytest.skip(
                f"Field {field} not in current WebSocketManagerConfig model keys, skipping bound test."
            )

        valid_data[field] = invalid_value
        with pytest.raises(ValidationError) as exc_info:
            WebSocketManagerConfig(**valid_data)  # type: ignore[arg-type]
        assert error_part.lower() in str(exc_info.value).lower()

    def test_config_frozen_and_extra_forbid(
        self, default_ws_manager_config: WebSocketManagerConfig
    ) -> None:
        """Test that WebSocketManagerConfig is frozen and forbids extra fields."""
        assert default_ws_manager_config.model_config.get("frozen") is True
        assert default_ws_manager_config.model_config.get("extra") == "forbid"

        with pytest.raises(ValidationError, match="frozen"):
            default_ws_manager_config.connection_timeout = 5.0  # type: ignore[misc]

        with pytest.raises(ValidationError, match="extra fields not permitted"):
            WebSocketManagerConfig(
                ws_url=AnyUrl("ws://example.com/ws"),
                extra_field="should_fail",  # type: ignore[call-arg]
            )
