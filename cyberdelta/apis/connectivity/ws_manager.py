"""CyberDeltaEngine: Generic WebSocket Manager.

--------------------------------------------

This module defines the `WebSocketManager` class, a reusable component for managing
WebSocket connections, including connection lifecycle (connect, reconnect, ping, close),
message handling, and error recovery.

It aims to abstract common WebSocket complexities away from specific exchange API
implementations.
"""

import asyncio
import json
import secrets
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

import aiohttp
from aiohttp import ClientTimeout, ClientWebSocketResponse
from aiohttp.helpers import sentinel
from pydantic import BaseModel

from cyberdelta.config.structlog_config import get_logger

# Import the config model
from .connectivity_models import WebSocketManagerConfig


if TYPE_CHECKING:
    from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime

# Define MessageHandler type alias
MessageHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]
TaskFactory = Callable[..., asyncio.Task[Any]]  # Type alias for our task factory


class WebSocketManager:
    """Manages a WebSocket connection, including automatic reconnections and keep-alive pings."""

    DEFAULT_PING_INTERVAL: float = 30.0
    DEFAULT_RECONNECT_DELAY: float = 5.0
    DEFAULT_MAX_RECONNECT_ATTEMPTS: int = 10
    DEFAULT_CONNECTION_TIMEOUT: float = 30.0
    HEARTBEAT_FACTOR: float = 2.0

    def __init__(
        self,
        exchange_name: str,
        message_handler: MessageHandler,
        config: WebSocketManagerConfig,
        on_connected_callback: Callable[[], Coroutine[Any, Any, None]] | None = None,
        session: aiohttp.ClientSession | None = None,
        task_factory: TaskFactory | None = None,  # New parameter
        outgoing_message_limiter: "TokenBucketRateLimiterRuntime | None" = None,
    ) -> None:
        """Initialize the WebSocketManager.

        Args:
            exchange_name: Name of the exchange for logging purposes.
            message_handler: Async callable to process incoming messages.
            config: WebSocketManagerConfig object with connectivity parameters.
            on_connected_callback: Optional async callback to execute after successful
                connection.
            session: Optional pre-existing aiohttp.ClientSession. If None, one will be
                created.
            task_factory: Optional callable to create asyncio tasks. Defaults to
                asyncio.create_task.
            outgoing_message_limiter: Optional rate limiter for outgoing WebSocket messages.

        """
        self._exchange_name: str = exchange_name
        self._ws_url: str = str(config.ws_url)
        self._message_handler: MessageHandler = message_handler
        self._on_connected_callback: Callable[[], Coroutine[Any, Any, None]] | None = (
            on_connected_callback
        )
        self._external_session: bool = session is not None
        self._session: aiohttp.ClientSession | None = session

        self._ping_interval: float = config.ping_interval
        self._reconnect_delay: float = config.reconnect_delay
        self._max_reconnect_attempts: int = config.max_reconnect_attempts
        self._connection_timeout: float = config.connection_timeout

        self._ws_connection: ClientWebSocketResponse | None = None
        self._is_connected: bool = False
        self._should_reconnect: bool = True

        self._listener_task: asyncio.Task[None] | None = None
        self._ping_task: asyncio.Task[None] | None = None
        self._connection_task: asyncio.Task[None] | None = None

        self._task_factory: TaskFactory = task_factory or asyncio.create_task  # Store task factory
        self._outgoing_message_limiter = outgoing_message_limiter

        self._reconnect_lock = asyncio.Lock()
        self._logger = get_logger(f"WebSocketManager.{self._exchange_name}")
        self._logger.info(
            "websocket_manager_initialized",
            exchange=self._exchange_name,
            ws_url=self._ws_url,
            message=f"Initialized WebSocketManager for {self._exchange_name} at {self._ws_url}",
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp ClientSession."""
        if self._session is None or self._session.closed:
            self._logger.info(
                "creating_new_aiohttp_session",
                action="get_session",
                connect_timeout=self._connection_timeout,
                message=(
                    f"Creating new aiohttp.ClientSession with "
                    f"connect timeout: {self._connection_timeout}"
                ),
            )
            # Use self._connection_timeout for the 'connect' part of ClientTimeout
            session_timeout = ClientTimeout(connect=self._connection_timeout)
            self._session = aiohttp.ClientSession(timeout=session_timeout)
            self._external_session = False
        return self._session

    @property
    def is_connected(self) -> bool:
        """Returns true if the WebSocket is currently connected and active."""
        return (
            self._is_connected
            and self._ws_connection is not None
            and not self._ws_connection.closed
        )

    def connect(self) -> asyncio.Task[None] | None:
        """Initiates the WebSocket connection process by creating a task for _establish_connection.

        Does not await the task itself.
        This method is idempotent based on task status.

        Returns:
            The asyncio.Task for the connection attempt, or None if already connecting.

        """
        self._should_reconnect = True
        if self._connection_task and not self._connection_task.done():
            self._logger.debug(
                "connection_attempt_already_in_progress",
                action="connect",
                ws_url=self._ws_url,
                message=(
                    f"Connection attempt for {self._ws_url} already in progress. "
                    f"Using existing task."
                ),
            )
            return self._connection_task

        self._logger.info(
            "connection_requested",
            action="connect",
            ws_url=self._ws_url,
            message=f"Connection requested for {self._ws_url}. Creating new task.",
        )
        connection_coroutine = None
        try:
            connection_coroutine = self._establish_connection()
            self._connection_task = self._task_factory(
                connection_coroutine,
                name=f"{self._exchange_name}_ws_establish_conn",
            )
            return self._connection_task
        except RuntimeError as e:
            if "no running event loop" in str(e):
                self._logger.debug(
                    "connection_task_creation_no_event_loop",
                    action="connect",
                    reason="no_running_event_loop",
                    message=(
                        "Cannot create connection task: no running event loop (likely test cleanup)"
                    ),
                )
                # Close the coroutine to prevent the warning
                if connection_coroutine:
                    connection_coroutine.close()
                return None
            else:
                raise
        except Exception as e:
            self._logger.error(
                "connection_task_creation_failed",
                action="connect",
                error_details=str(e),
                message=f"Failed to create connection task: {e}",
            )
            # Close the coroutine to prevent the warning
            if connection_coroutine:
                connection_coroutine.close()
            return None

    async def _establish_connection(self) -> None:
        """Establishes and maintains the WebSocket connection.

        Includes retry logic with exponential backoff.
        """
        self._logger.info(
            "establish_connection_entered",
            action="establish_connection",
            exchange=self._exchange_name,
            should_reconnect=self._should_reconnect,
            is_connected=self.is_connected,
            message=(
                f"[{self._exchange_name} _establish_connection] Entered method. Should_reconnect: "
                f"{self._should_reconnect}, Is_connected: {self.is_connected}"
            ),
        )
        async with self._reconnect_lock:
            if not await self._should_proceed_with_connection():
                return

            await self._connection_retry_loop()

    async def _should_proceed_with_connection(self) -> bool:
        """Check if connection should proceed after acquiring lock."""
        self._logger.info(
            "reconnect_lock_acquired",
            action="should_proceed_with_connection",
            exchange=self._exchange_name,
            should_reconnect=self._should_reconnect,
            message=(
                f"[{self._exchange_name} _establish_connection] Acquired reconnect_lock. "
                f"Should_reconnect: "
                f"{self._should_reconnect}"
            ),
        )
        if not self._should_reconnect:
            self._logger.info(
                "reconnection_disabled_after_lock",
                action="should_proceed_with_connection",
                message="Reconnection disabled after lock, _establish_connection not proceeding.",
            )
            return False

        if self.is_connected:
            self._logger.info(
                "already_connected_not_proceeding",
                action="should_proceed_with_connection",
                message="Already connected, _establish_connection not proceeding.",
            )
            return False

        return True

    async def _connection_retry_loop(self) -> None:
        """Main retry loop for connection attempts."""
        current_attempt = 0
        self._logger.info(
            "connection_attempts_loop_started",
            action="connection_retry_loop",
            exchange=self._exchange_name,
            max_attempts=self._max_reconnect_attempts,
            message=(
                f"[{self._exchange_name} _establish_connection] Starting connection attempts loop. "
                f"Max attempts: "
                f"{self._max_reconnect_attempts}"
            ),
        )

        while self._should_reconnect and current_attempt < self._max_reconnect_attempts:
            self._logger.info(
                "connection_loop_iteration",
                action="connection_retry_loop",
                exchange=self._exchange_name,
                current_attempt=current_attempt + 1,
                message=(
                    f"[{self._exchange_name} _establish_connection] Loop Iteration. "
                    f"Current attempt: "
                    f"{current_attempt + 1}"
                ),
            )

            if current_attempt > 0:
                await self._apply_reconnect_delay(current_attempt)

            connection_successful = await self._attempt_single_connection(current_attempt)
            if connection_successful:
                return

            current_attempt += 1

        await self._handle_connection_failure(current_attempt)

    async def _apply_reconnect_delay(self, attempt: int) -> None:
        """Apply exponential backoff delay before reconnection attempt."""
        backoff_base = self._reconnect_delay * (2 ** (attempt - 1))
        jitter = backoff_base * 0.2 * (secrets.SystemRandom().random() - 0.5)
        actual_delay = max(1.0, backoff_base + jitter)
        self._logger.info(
            "reconnection_delay_applied",
            action="apply_reconnect_delay",
            attempt=attempt + 1,
            max_attempts=self._max_reconnect_attempts,
            delay_seconds=actual_delay,
            message=(
                f"Reconnection attempt {attempt + 1}/{self._max_reconnect_attempts} "
                f"in {actual_delay:.2f}s..."
            ),
        )
        await asyncio.sleep(actual_delay)

    async def _attempt_single_connection(self, attempt: int) -> bool:
        """Attempt a single WebSocket connection.

        Returns:
            True if connection was successful, False otherwise.
        """
        try:
            self._logger.info(
                "connection_attempt_started",
                action="attempt_single_connection",
                ws_url=self._ws_url,
                attempt=attempt + 1,
                message=f"Attempting to connect to {self._ws_url} (Attempt {attempt + 1})",
            )

            await self._establish_websocket_connection()
            await self._setup_connection_tasks()
            await self._execute_connection_callback()

            return True

        except asyncio.CancelledError:
            self._logger.error(
                "connection_attempt_cancelled",
                action="attempt_single_connection",
                message="Connection attempt cancelled.",
            )
            self._should_reconnect = False
            return False
        except (TimeoutError, aiohttp.ClientError) as e:
            self._logger.warning(
                "connection_attempt_failed",
                action="attempt_single_connection",
                exchange=self._exchange_name,
                attempt=attempt + 1,
                error_details=str(e),
                message=(
                    f"[{self._exchange_name} _establish_connection] Connection attempt "
                    f"{attempt + 1} failed: {e}"
                ),
            )
            self._reset_connection_state()
            return False
        except Exception as e:
            self._logger.exception(
                "connection_attempt_unexpected_error",
                action="attempt_single_connection",
                exchange=self._exchange_name,
                attempt=attempt + 1,
                error_details=str(e),
                message=(
                    f"[{self._exchange_name} _establish_connection] Unexpected error during "
                    f"connection attempt {attempt + 1}: {e}"
                ),
            )
            self._reset_connection_state()
            return False

    async def _establish_websocket_connection(self) -> None:
        """Establish the actual WebSocket connection."""
        session = await self._get_session()
        self._logger.info(
            "websocket_connect_about_to_call",
            action="establish_websocket_connection",
            exchange=self._exchange_name,
            ws_url=self._ws_url,
            message=(
                f"[{self._exchange_name} _establish_connection] About to call "
                f"session.ws_connect for "
                f"{self._ws_url}"
            ),
        )

        server_expected_ping_interval = (
            self._ping_interval * self.HEARTBEAT_FACTOR if self._ping_interval > 0 else 0
        )

        self._logger.info(
            "websocket_session_connection_attempt",
            action="establish_websocket_connection",
            exchange=self._exchange_name,
            ws_url=self._ws_url,
            session_id=id(session) if session else None,
            message=(
                f"[{self._exchange_name} _establish_connection] Attempting to "
                f"connect to {self._ws_url} via session {id(session) if session else 'None'}"
            ),
        )

        self._ws_connection = await session.ws_connect(
            self._ws_url,
            heartbeat=server_expected_ping_interval,
            timeout=sentinel,
        )

        self._logger.info(
            "websocket_connect_completed",
            action="establish_websocket_connection",
            exchange=self._exchange_name,
            connection_type=str(type(self._ws_connection)),
            message=(
                f"[{self._exchange_name} _establish_connection] session.ws_connect call completed. "
                f"self._ws_connection is now: "
                f"{type(self._ws_connection)}"
            ),
        )

        self._is_connected = True

    async def _setup_connection_tasks(self) -> None:
        """Setup listener and ping tasks after successful connection."""
        self._logger.info(
            "connection_successful_tasks_starting",
            action="setup_connection_tasks",
            exchange=self._exchange_name,
            ws_url=self._ws_url,
            message=(
                f"[{self._exchange_name} _establish_connection] Successfully connected to "
                f"{self._ws_url}. Starting listener/ping tasks."
            ),
        )

        await self._cancel_existing_tasks()
        await self._create_new_tasks()

    async def _cancel_existing_tasks(self) -> None:
        """Cancel any existing listener and ping tasks."""
        if self._listener_task and not self._listener_task.done():
            self._logger.info(
                "existing_listener_task_cancelled",
                action="cancel_existing_tasks",
                exchange=self._exchange_name,
                task_name=self._listener_task.get_name(),
                message=(
                    f"[{self._exchange_name} _establish_connection] Cancelling existing "
                    f"listener task: "
                    f"{self._listener_task.get_name()}"
                ),
            )
            self._listener_task.cancel()

        if self._ping_task and not self._ping_task.done():
            self._logger.info(
                "existing_ping_task_cancelled",
                action="cancel_existing_tasks",
                exchange=self._exchange_name,
                task_name=self._ping_task.get_name(),
                message=(
                    f"[{self._exchange_name} _establish_connection] Cancelling existing ping task: "
                    f"{self._ping_task.get_name()}"
                ),
            )
            self._ping_task.cancel()

    async def _create_new_tasks(self) -> None:
        """Create new listener and ping tasks."""
        self._listener_task = self._task_factory(
            self._listen(),
            name=f"{self._exchange_name}_ws_listen",
        )

        if self._ping_interval > 0:
            self._ping_task = self._task_factory(
                self._keep_alive(),
                name=f"{self._exchange_name}_ws_ping",
            )

    async def _execute_connection_callback(self) -> None:
        """Execute the on_connected callback if provided."""
        if self._on_connected_callback:
            try:
                self._logger.info(
                    "executing_connection_callback",
                    action="execute_connection_callback",
                    message="Executing on_connected_callback...",
                )
                await self._on_connected_callback()
                self._logger.info(
                    "connection_callback_executed_successfully",
                    action="execute_connection_callback",
                    message="on_connected_callback executed successfully.",
                )
            except Exception as cb_exc:
                self._logger.error(
                    "connection_callback_error",
                    action="execute_connection_callback",
                    error_details=str(cb_exc),
                    message=f"Error during on_connected_callback: {cb_exc}",
                    exc_info=True,
                )

    def _reset_connection_state(self) -> None:
        """Reset connection state after failed attempt."""
        self._is_connected = False
        self._ws_connection = None

    async def _handle_connection_failure(self, current_attempt: int) -> None:
        """Handle the case where all connection attempts failed."""
        if self._should_reconnect and current_attempt >= self._max_reconnect_attempts:
            self._logger.critical(
                "connection_attempts_exhausted",
                action="handle_connection_failure",
                ws_url=self._ws_url,
                max_attempts=self._max_reconnect_attempts,
                message=(
                    f"Failed to connect to {self._ws_url} after "
                    f"{self._max_reconnect_attempts} attempts. Giving up."
                ),
            )
            self._should_reconnect = False
        elif not self._should_reconnect:
            self._logger.info(
                "connection_process_stopped_reconnect_disabled",
                action="handle_connection_failure",
                ws_url=self._ws_url,
                message=(
                    f"Connection process for {self._ws_url} stopped because "
                    f"reconnections are disabled."
                ),
            )

    async def _listen(self) -> None:
        """Listens for messages on the WebSocket and handles them."""
        self._logger.info(
            "listener_task_started",
            action="listen",
            exchange=self._exchange_name,
            message=f"[{self._exchange_name} _listen] Task started.",
        )

        if not self._ws_connection:
            self._logger.error(
                f"[{self._exchange_name} _listen] Listener started without a valid "
                f"WebSocket connection.",
            )
            return

        await self._listen_loop()

    async def _listen_loop(self) -> None:
        """Main listening loop for WebSocket messages."""
        self._logger.info(
            "listener_main_loop_entered",
            action="listen_loop",
            exchange=self._exchange_name,
            message=f"[{self._exchange_name} _listen] Entering main loop.",
        )
        original_connection = self._ws_connection
        was_cancelled_flag = False
        loop_iteration_count = 0

        try:
            # DEFENSIVE CHECK: self._ws_connection is confirmed not None by caller.
            # Mypy=[union-attr]
            async for msg in self._ws_connection:  # type: ignore[union-attr]
                loop_iteration_count += 1

                if not await self._should_continue_listening(
                    original_connection, loop_iteration_count
                ):
                    break

                await self._handle_websocket_message(msg, loop_iteration_count)

        except asyncio.CancelledError:
            was_cancelled_flag = True
            await self._handle_cancelled_listener()
            raise
        except Exception as e:
            await self._handle_listener_exception(e, original_connection)
        finally:
            await self._cleanup_listener(original_connection, was_cancelled_flag)

    async def _should_continue_listening(
        self, original_connection: ClientWebSocketResponse | None, iteration: int
    ) -> bool:
        """Check if listening should continue."""
        current_task_listen_loop = asyncio.current_task()
        task_name_listen_loop = (
            current_task_listen_loop.get_name() if current_task_listen_loop else "UnknownTask"
        )
        cancelled_state = (
            current_task_listen_loop.cancelled() if current_task_listen_loop else "N/A"
        )

        self._logger.debug(
            f"[{self._exchange_name} _listen::{task_name_listen_loop}] "
            f"Iteration {iteration}. "
            f"Cancelled state: {cancelled_state}",
        )

        if self._ws_connection is not original_connection or (
            self._ws_connection is not None and self._ws_connection.closed
        ):
            self._logger.warning(
                "websocket_connection_changed_or_closed",
                action="should_continue_listening",
                message=(
                    "WebSocket connection changed or closed during iteration. "
                    "Stopping listener for old connection."
                ),
            )
            return False

        return True

    async def _handle_websocket_message(self, msg: aiohttp.WSMessage, iteration: int) -> None:
        """Handle a single WebSocket message based on its type."""
        current_task = asyncio.current_task()
        task_name = current_task.get_name() if current_task else "UnknownTask"

        self._logger.debug(
            f"[{self._exchange_name} _listen::{task_name}] "
            f"Iteration {iteration}. "
            f"Msg type: {msg.type if msg else 'None'}.",
        )

        if msg.type == aiohttp.WSMsgType.TEXT:
            await self._handle_text_message(msg)
        elif msg.type == aiohttp.WSMsgType.BINARY:
            self._handle_binary_message(msg)
        elif msg.type == aiohttp.WSMsgType.ERROR:
            self._handle_error_message()
            raise ConnectionError("WebSocket error received")
        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
            await self._handle_close_message(task_name, iteration)
            raise ConnectionError("WebSocket closed")

    async def _handle_text_message(self, msg: aiohttp.WSMessage) -> None:
        """Handle TEXT type WebSocket messages."""
        try:
            data = json.loads(msg.data)
            await self._message_handler(data)
        except json.JSONDecodeError:
            self._logger.warning(
                "received_non_json_websocket_message",
                action="handle_text_message",
                message_data_preview=str(msg.data[:200]),
                message=f"Received non-JSON WebSocket message: {msg.data[:200]}...",
            )
        except Exception as e:
            self._logger.exception(
                "websocket_message_processing_error",
                action="handle_text_message",
                error_details=str(e),
                message=f"Error processing WebSocket message: {e}",
            )

    def _handle_binary_message(self, msg: aiohttp.WSMessage) -> None:
        """Handle BINARY type WebSocket messages."""
        self._logger.debug(
            f"Received binary WebSocket message (length: {len(msg.data)}). "
            "Handler for binary not implemented.",
        )

    def _handle_error_message(self) -> None:
        """Handle ERROR type WebSocket messages."""
        exception_info = (
            self._ws_connection.exception() if self._ws_connection is not None else "Unknown"
        )
        self._logger.error(
            f"WebSocket connection error: {exception_info!r}",
        )

    async def _handle_close_message(self, task_name: str, iteration: int) -> None:
        """Handle CLOSED/CLOSING type WebSocket messages."""
        self._logger.info(
            f"[{self._exchange_name} _listen::{task_name}] "
            f"Received WSMsgType.CLOSED/CLOSING. "
            f"Iteration {iteration}. Terminating listener loop.",
        )

    async def _handle_cancelled_listener(self) -> None:
        """Handle cancellation of the listener task."""
        current_task_cancelled = asyncio.current_task()
        task_name_cancelled = (
            current_task_cancelled.get_name() if current_task_cancelled else "UnknownTask"
        )
        # Check if this is a graceful shutdown (should_reconnect=False) or unexpected cancellation
        if not self._should_reconnect:
            self._logger.info(
                f"[{self._exchange_name} _listen::{task_name_cancelled}] "
                f"Listener task cancelled during graceful shutdown.",
                action="graceful_shutdown",
                exchange=self._exchange_name,
                task_name=task_name_cancelled,
            )
        else:
            self._logger.error(
                f"[{self._exchange_name} _listen::{task_name_cancelled}] "
                f"Listener task unexpectedly CANCELLED. Re-raising CancelledError.",
                action="unexpected_cancellation",
                exchange=self._exchange_name,
                task_name=task_name_cancelled,
            )

    async def _handle_listener_exception(
        self, error: Exception, original_connection: ClientWebSocketResponse | None
    ) -> None:
        """Handle unexpected exceptions in the listener."""
        self._logger.exception(
            "websocket_listener_unexpected_error",
            action="handle_listener_exception",
            error_details=str(error),
            message=f"[DEBUG_LISTEN] Unexpected error in WebSocket listener: {error}",
        )
        if self._ws_connection is original_connection:
            self._is_connected = False
            self._ws_connection = None

    async def _cleanup_listener(
        self, original_connection: ClientWebSocketResponse | None, was_cancelled: bool
    ) -> None:
        """Cleanup after listener loop ends."""
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            # No running event loop (e.g., during test cleanup)
            current_task = None

        final_is_cancelled_state = (current_task and current_task.cancelled()) or was_cancelled

        self._logger.info(
            f"[{self._exchange_name} _listen] Finally block. "
            f"Task: {current_task.get_name() if current_task else 'None'}, "
            f"Cancelled state: {final_is_cancelled_state}, "
            f"Should Reconnect: {self._should_reconnect}",
        )

        if current_task:  # pragma: no cover
            pass  # Keep block for structure if needed later

        if (
            current_task
            and not final_is_cancelled_state
            and self._ws_connection is original_connection
        ):
            self._is_connected = False
            self._ws_connection = None

        await self._schedule_reconnection_if_needed(final_is_cancelled_state)

    async def _schedule_reconnection_if_needed(self, final_is_cancelled_state: bool) -> None:
        """Schedule reconnection if needed and conditions are met."""
        if self._should_reconnect and not final_is_cancelled_state:
            if self._connection_task is None or self._connection_task.done():
                self._logger.info(
                    "scheduling_reconnection_from_listener",
                    action="schedule_reconnection_if_needed",
                    message="Scheduling reconnection from listener task termination.",
                )
                try:
                    reconnect_task = self.connect()
                    if reconnect_task:
                        self._logger.debug(
                            "reconnection_task_created_from_listener",
                            action="schedule_reconnection_if_needed",
                            task_name=reconnect_task.get_name(),
                            message=f"Reconnection task created: {reconnect_task.get_name()}",
                        )
                    else:
                        self._logger.debug(
                            "Reconnection task was not created (already connecting)",
                        )
                except RuntimeError as e:
                    if "no running event loop" in str(e):
                        self._logger.debug(
                            "reconnection_scheduling_no_event_loop",
                            action="schedule_reconnection_if_needed",
                            reason="no_running_event_loop",
                            message=(
                                "Cannot schedule reconnection: no running event loop "
                                "(likely test cleanup)"
                            ),
                        )
                    else:
                        raise

    async def _keep_alive(self) -> None:
        """Periodically sends a ping to keep the connection alive."""
        self._logger.info(
            "keep_alive_task_started",
            action="keep_alive",
            exchange=self._exchange_name,
            message=f"[{self._exchange_name} _keep_alive] Task started.",
        )

        if not self._ping_interval or self._ping_interval <= 0:
            self._logger.info(
                f"[{self._exchange_name} _keep_alive] "
                f"Ping interval zero/negative, task will not run.",
            )
            return

        try:
            self._logger.info(
                "keep_alive_main_loop_entered",
                action="keep_alive",
                exchange=self._exchange_name,
                message=f"[{self._exchange_name} _keep_alive] Entering main loop.",
            )
            while self.is_connected and self._should_reconnect:
                if not self._ws_connection or self._ws_connection.closed:
                    self._logger.warning(
                        "Keep-alive: WebSocket connection is not available or closed.",
                    )
                    break

                try:
                    self._logger.debug(
                        "ping_frame_sent",
                        action="keep_alive",
                        exchange=self._exchange_name,
                        message=f"Sending ping frame for {self._exchange_name}",
                    )
                    await self._ws_connection.ping()
                except ConnectionResetError:
                    self._logger.warning(
                        "keep_alive_connection_reset_during_ping",
                        action="keep_alive",
                        message=(
                            "Keep-alive: Connection reset during ping. Attempting to reconnect."
                        ),
                    )
                    self._is_connected = False
                    reconnect_task = self.connect()
                    if reconnect_task:
                        self._logger.debug(
                            "reconnection_task_created_from_keep_alive",
                            action="keep_alive",
                            task_name=reconnect_task.get_name(),
                            message=f"Reconnection task created: {reconnect_task.get_name()}",
                        )
                    else:
                        self._logger.debug(
                            "reconnection_task_not_created_already_connecting",
                            action="schedule_reconnection_if_needed",
                            message="Reconnection task was not created (already connecting)",
                        )
                    break

                self._logger.debug(
                    "keep_alive_sleep_after_ping",
                    action="keep_alive",
                    sleep_duration=self._ping_interval,
                    message=f"Keep-alive: sleeping for {self._ping_interval}s after ping.",
                )
                await asyncio.sleep(self._ping_interval)

        except asyncio.CancelledError:
            self._logger.info(
                "keep_alive_task_cancelled",
                action="keep_alive",
                exchange=self._exchange_name,
                message=f"[{self._exchange_name} _keep_alive] Task cancelled.",
            )
        except Exception as e:
            self._logger.error(
                "keep_alive_unexpected_error",
                action="keep_alive",
                exchange=self._exchange_name,
                error_details=str(e),
                message=f"[{self._exchange_name} _keep_alive] Unexpected error in loop: {e}",
                exc_info=True,
            )
            self._is_connected = False
            if self._should_reconnect:
                self._logger.info(
                    "keep_alive_attempting_reconnect_due_to_error",
                    action="keep_alive",
                    exchange=self._exchange_name,
                    message=(
                        f"[{self._exchange_name} _keep_alive] Attempting reconnect due to error."
                    ),
                )
                new_connection_attempt_task = self.connect()
                if new_connection_attempt_task:
                    self._logger.debug(
                        "keep_alive_reconnect_task_created",
                        action="keep_alive",
                        exchange=self._exchange_name,
                        task_name=new_connection_attempt_task.get_name(),
                        message=(
                            f"[{self._exchange_name} _keep_alive] Reconnect task created: "
                            f"{new_connection_attempt_task.get_name()}"
                        ),
                    )
                else:
                    self._logger.debug(
                        "keep_alive_reconnect_not_started_already_connecting",
                        action="keep_alive",
                        exchange=self._exchange_name,
                        message=(
                            f"[{self._exchange_name} _keep_alive] Reconnect from "
                            f"error did not start new task (already connecting)."
                        ),
                    )
        finally:
            self._logger.info(
                "keep_alive_task_ending",
                action="keep_alive",
                exchange=self._exchange_name,
                message=f"[{self._exchange_name} _keep_alive] Task ending.",
            )

    async def send_json(self, data: BaseModel) -> bool:
        """Sends a JSON payload over the WebSocket connection.

        Args:
            data: The Pydantic BaseModel to serialize and send as JSON.

        Returns:
            True if the message was sent successfully, False otherwise.

        """
        if not self.is_connected or not self._ws_connection:
            self._logger.error(
                "send_json_websocket_not_connected",
                action="send_json",
                ws_url=self._ws_url,
                message=f"Cannot send JSON, WebSocket not connected to {self._ws_url}.",
            )
            return False
        try:
            # Apply rate limiting if configured
            if self._outgoing_message_limiter:
                await self._outgoing_message_limiter.acquire(1)

            # Serialize the Pydantic model
            payload_to_send = data.model_dump(by_alias=True, exclude_none=True)
            self._logger.debug(
                "websocket_json_sent",
                action="send_json",
                exchange=self._exchange_name,
                payload=payload_to_send,
                message=f"[{self._exchange_name}] Sending WS JSON: {payload_to_send}",
            )
            await self._ws_connection.send_json(payload_to_send)
            return True
        except asyncio.CancelledError:
            self._logger.warning(
                "send_json_operation_cancelled",
                action="send_json",
                message="Send JSON operation cancelled.",
            )
            return False
        except ConnectionResetError:
            self._logger.error(
                "connection_reset_during_send_json",
                action="send_json",
                ws_url=self._ws_url,
                message=(
                    f"Connection reset while trying to "
                    f"send JSON to {self._ws_url}. Marking as disconnected."
                ),
            )
            self._is_connected = False
            self._ws_connection = None
            return False
        except Exception as e:
            self._logger.error(
                "send_json_serialize_send_error",
                action="send_json",
                exchange=self._exchange_name,
                error_details=str(e),
                message=f"[{self._exchange_name}] Error during WS send_json (serialize/send): {e}",
                exc_info=True,
            )
            return False

    async def close(self) -> None:
        """Gracefully closes the WebSocket connection and cleans up resources."""
        self._logger.info(
            "websocket_close_requested",
            action="close",
            ws_url=self._ws_url,
            message=f"Close requested for WebSocket connection to {self._ws_url}.",
        )
        self._should_reconnect = False

        ws_conn_at_close_start = self._ws_connection

        await self._cancel_connection_task()
        await self._cancel_listener_task()
        await self._cancel_ping_task()
        await self._close_websocket_connection(ws_conn_at_close_start)
        await self._close_session()

        self._logger.info(
            "websocket_manager_fully_closed",
            action="close",
            ws_url=self._ws_url,
            message=f"WebSocketManager for {self._ws_url} is fully closed.",
        )

    async def _cancel_connection_task(self) -> None:
        """Cancel the connection task if it's running."""
        if self._connection_task and not self._connection_task.done():
            self._logger.debug(
                "cancelling_connection_task",
                action="cancel_connection_task",
                message="Cancelling in-progress connection task.",
            )
            self._connection_task.cancel()
            try:
                await self._connection_task
            except asyncio.CancelledError:
                self._logger.debug(
                    "connection_task_cancelled_successfully",
                    action="cancel_connection_task",
                    message="Connection task successfully cancelled during close.",
                )
            except Exception as e:
                self._logger.warning(
                    "connection_task_cancellation_error",
                    action="cancel_connection_task",
                    error_details=str(e),
                    message=f"Error awaiting cancelled connection task: {e}",
                )

    async def _cancel_listener_task(self) -> None:
        """Cancel the listener task if it's running."""
        if self._listener_task and not self._listener_task.done():
            self._logger.debug(
                "listener_task_cancelling",
                action="cancel_listener_task",
                task_name=self._listener_task.get_name(),
                message=f"Cancelling listener task: {self._listener_task.get_name()}",
            )
            self._listener_task.cancel()
            await self._await_task_cancellation(
                self._listener_task, "listener", self._listener_task.get_name()
            )

    async def _cancel_ping_task(self) -> None:
        """Cancel the ping task if it's running."""
        if self._ping_task and not self._ping_task.done():
            self._logger.debug(
                "ping_task_cancelling",
                action="cancel_ping_task",
                task_name=self._ping_task.get_name(),
                message=f"Cancelling ping task: {self._ping_task.get_name()}",
            )
            self._ping_task.cancel()
            await self._await_task_cancellation(self._ping_task, "ping", self._ping_task.get_name())

    async def _await_task_cancellation(
        self, task: asyncio.Task[None], task_type: str, task_name: str
    ) -> None:
        """Wait for a task to be cancelled with timeout handling."""
        try:
            self._logger.debug(
                f"Awaiting {task_type} task: {task_name}, cancelled state: {task.cancelled()}",
            )
            await asyncio.wait_for(task, timeout=5.0)
            self._logger.debug(
                f"{task_type.title()} task {task_name} awaited. "
                f"Done: {task.done()}, "
                f"Cancelled: {task.cancelled()}",
            )
        except asyncio.CancelledError:
            self._logger.debug(
                f"{task_type.title()} task {task_name} successfully cancelled and awaited.",
            )
        except TimeoutError:
            self._logger.error(
                f"Timeout (5s) waiting for {task_type} task {task_name} "
                f"to complete during close! Task state: "
                f"Done={task.done()}, "
                f"Cancelled={task.cancelled()}",
            )
        except Exception as e:
            self._logger.warning(
                f"Error awaiting cancelled {task_type} task {task_name}: {e}",
                exc_info=True,
            )

    async def _close_websocket_connection(
        self, ws_conn_at_close_start: ClientWebSocketResponse | None
    ) -> None:
        """Close the WebSocket connection object."""
        closed_ws_successfully = await self._attempt_websocket_close(ws_conn_at_close_start)

        self._ws_connection = None
        if closed_ws_successfully:
            self._is_connected = False
        self._is_connected = False

    async def _attempt_websocket_close(self, ws_conn: ClientWebSocketResponse | None) -> bool:
        """Attempt to close the WebSocket connection.

        Returns:
            True if connection was closed successfully or was already closed.
        """
        if ws_conn and not ws_conn.closed:
            return await self._close_active_websocket(ws_conn)
        elif ws_conn and ws_conn.closed:
            self._logger.debug(
                "websocket_connection_already_closed",
                action="attempt_websocket_close",
                connection_id=id(ws_conn),
                ws_url=self._ws_url,
                message=f"WS connection {id(ws_conn)} for {self._ws_url} was already closed.",
            )
            return True
        else:
            self._logger.debug(
                "no_active_websocket_connection_to_close",
                action="attempt_websocket_close",
                ws_url=self._ws_url,
                message=f"No active WS connection object to close explicitly for {self._ws_url}.",
            )
            return True

    async def _close_active_websocket(self, ws_conn: ClientWebSocketResponse) -> bool:
        """Close an active WebSocket connection."""
        self._logger.debug(
            "closing_websocket_connection_object",
            action="close_active_websocket",
            connection_id=id(ws_conn),
            ws_url=self._ws_url,
            message=f"Closing WebSocket connection object {id(ws_conn)} for {self._ws_url}.",
        )
        try:
            await ws_conn.close()
            self._logger.info(
                "websocket_connection_closed_explicitly",
                action="close_active_websocket",
                ws_url=self._ws_url,
                connection_id=id(ws_conn),
                message=(
                    f"WS connection to {self._ws_url} (id: {id(ws_conn)}) closed by explicit call."
                ),
            )
            return True
        except Exception as e:
            self._logger.error(
                "websocket_connection_close_error",
                action="close_active_websocket",
                connection_id=id(ws_conn),
                error_details=str(e),
                message=f"Error during explicit close of WS connection {id(ws_conn)}: {e}",
                exc_info=True,
            )
            return False

    async def _close_session(self) -> None:
        """Close the aiohttp session if it was created internally."""
        if self._session and not self._external_session and not self._session.closed:
            self._logger.info(
                "closing_internal_session",
                action="close_session",
                message="Closing internally created aiohttp.ClientSession.",
            )
            await self._session.close()
            self._logger.info(
                "internal_session_closed",
                action="close_session",
                message="Internally created ClientSession closed.",
            )
        self._session = None
