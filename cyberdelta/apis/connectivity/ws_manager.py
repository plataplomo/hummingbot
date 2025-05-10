"""
CyberDeltaEngine: Generic WebSocket Manager
--------------------------------------------

This module defines the `WebSocketManager` class, a reusable component for managing
WebSocket connections, including connection lifecycle (connect, reconnect, ping, close),
message handling, and error recovery.

It aims to abstract common WebSocket complexities away from specific exchange API
implementations.
"""

import asyncio
import json
import logging
import random
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

import aiohttp
from aiohttp import ClientTimeout, ClientWebSocketResponse
from aiohttp.helpers import sentinel

# Import the config model
from .connectivity_models import WebSocketManagerConfig

if TYPE_CHECKING:
    pass

# Define MessageHandler type alias
MessageHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]
TaskFactory = Callable[..., asyncio.Task[Any]]  # Type alias for our task factory


class WebSocketManager:
    """
    Manages a WebSocket connection, including automatic reconnections and keep-alive pings.
    """

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
    ) -> None:
        """
        Initialize the WebSocketManager.

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

        self._reconnect_lock = asyncio.Lock()
        self._logger = logging.getLogger(f"WebSocketManager.{self._exchange_name}")
        self._logger.info(f"Initialized for {self._ws_url}")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp ClientSession."""
        if self._session is None or self._session.closed:
            self._logger.info(
                "Creating new aiohttp.ClientSession with connect timeout: %s",
                self._connection_timeout,
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
        """
        Initiates the WebSocket connection process by creating and returning a task
        for _establish_connection. Does not await the task itself.
        This method is idempotent based on task status.

        Returns:
            The asyncio.Task for the connection attempt, or None if already connecting.
        """
        self._should_reconnect = True
        if self._connection_task and not self._connection_task.done():
            self._logger.debug(
                f"Connection attempt for {self._ws_url} already in progress. Using existing task."
            )
            return self._connection_task

        self._logger.info(f"Connection requested for {self._ws_url}. Creating new task.")
        self._connection_task = self._task_factory(
            self._establish_connection(), name=f"{self._exchange_name}_ws_establish_conn"
        )
        return self._connection_task

    async def _establish_connection(self) -> None:
        """
        Establishes and maintains the WebSocket connection.
        Includes retry logic with exponential backoff.
        """
        async with self._reconnect_lock:
            if not self._should_reconnect:
                self._logger.info(
                    "Reconnection disabled after lock, _establish_connection not proceeding."
                )
                return

            if self.is_connected:
                self._logger.info("Already connected, _establish_connection not proceeding.")
                return

            current_attempt = 0
            while self._should_reconnect and current_attempt < self._max_reconnect_attempts:
                if current_attempt > 0:
                    backoff_base = self._reconnect_delay * (2 ** (current_attempt - 1))
                    jitter = backoff_base * 0.2 * (random.random() - 0.5)
                    actual_delay = max(1.0, backoff_base + jitter)
                    self._logger.info(
                        f"Reconnection attempt {current_attempt + 1}/"
                        f"{self._max_reconnect_attempts} in {actual_delay:.2f}s..."
                    )
                    await asyncio.sleep(actual_delay)

                try:
                    self._logger.info(
                        f"Attempting to connect to {self._ws_url} (Attempt {current_attempt + 1})"
                    )
                    session = await self._get_session()
                    server_expected_ping_interval = (
                        self._ping_interval * self.HEARTBEAT_FACTOR
                        if self._ping_interval > 0
                        else 0
                    )

                    self._ws_connection = await session.ws_connect(
                        self._ws_url,
                        heartbeat=server_expected_ping_interval,
                        timeout=sentinel,
                    )
                    self._is_connected = True
                    current_attempt = 0
                    self._logger.info(f"Successfully connected to {self._ws_url}.")

                    if self._listener_task and not self._listener_task.done():
                        self._listener_task.cancel()
                    if self._ping_task and not self._ping_task.done():
                        self._ping_task.cancel()

                    self._listener_task = self._task_factory(
                        self._listen(), name=f"{self._exchange_name}_ws_listen"
                    )
                    if self._ping_interval > 0:
                        self._ping_task = self._task_factory(
                            self._keep_alive(), name=f"{self._exchange_name}_ws_ping"
                        )

                    if self._on_connected_callback:
                        try:
                            self._logger.info("Executing on_connected_callback...")
                            await self._on_connected_callback()
                            self._logger.info("on_connected_callback executed successfully.")
                        except Exception as cb_exc:
                            self._logger.error(
                                f"Error during on_connected_callback: {cb_exc}", exc_info=True
                            )
                    return

                except asyncio.CancelledError:
                    self._logger.error("Connection attempt cancelled.")
                    self._should_reconnect = False
                    break
                except (TimeoutError, aiohttp.ClientError) as e:
                    self._logger.warning(f"Connection attempt {current_attempt + 1} failed: {e}")
                    self._is_connected = False
                    self._ws_connection = None
                    current_attempt += 1
                except Exception as e:
                    self._logger.exception(
                        f"Unexpected error during connection attempt {current_attempt + 1}: {e}"
                    )
                    self._is_connected = False
                    self._ws_connection = None
                    current_attempt += 1

            if self._should_reconnect and current_attempt >= self._max_reconnect_attempts:
                self._logger.critical(
                    f"Failed to connect to {self._ws_url} after "
                    f"{self._max_reconnect_attempts} attempts. Giving up."
                )
                self._should_reconnect = False
            elif not self._should_reconnect:
                self._logger.info(
                    f"Connection process for {self._ws_url} stopped "
                    f"because reconnections are disabled."
                )

    async def _listen(self) -> None:
        """Listens for messages on the WebSocket and handles them."""
        self._logger.info(f"[{self._exchange_name} _listen] Task started.")
        if not self._ws_connection:
            self._logger.error(
                f"[{self._exchange_name} _listen] Listener started without a valid "
                f"WebSocket connection."
            )
            return

        self._logger.info(f"[{self._exchange_name} _listen] Entering main loop.")
        original_connection = self._ws_connection
        was_cancelled_flag = False
        try:
            async for msg in self._ws_connection:
                if self._ws_connection is not original_connection or self._ws_connection.closed:
                    self._logger.warning(
                        "WebSocket connection changed or closed during iteration. "
                        "Stopping listener for old connection."
                    )
                    break

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self._message_handler(data)
                    except json.JSONDecodeError:
                        self._logger.warning(
                            f"Received non-JSON WebSocket message: {msg.data[:200]}..."
                        )
                    except Exception as e:
                        self._logger.exception(f"Error processing WebSocket message: {e}")

                elif msg.type == aiohttp.WSMsgType.BINARY:
                    self._logger.debug(
                        f"Received binary WebSocket message (length: {len(msg.data)}). "
                        "Handler for binary not implemented."
                    )
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self._logger.error(
                        f"WebSocket connection error: {self._ws_connection.exception()!r}"
                    )
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    self._logger.info("WebSocket connection closed by server.")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSING:
                    self._logger.info("WebSocket connection is closing.")
                    break

        except asyncio.CancelledError:
            self._logger.error(
                "[DEBUG_LISTEN] Listener task explicitly CANCELLED. Re-raising CancelledError."
            )
            was_cancelled_flag = True
            raise
        except Exception as e:
            self._logger.exception(f"[DEBUG_LISTEN] Unexpected error in WebSocket listener: {e}")
            if self._ws_connection is original_connection:
                self._is_connected = False
                self._ws_connection = None
        finally:
            current_task = asyncio.current_task()
            final_is_cancelled_state = (
                current_task and current_task.cancelled()
            ) or was_cancelled_flag

            self._logger.info(
                f"[{self._exchange_name} _listen] Finally block. Task: {current_task.get_name() if current_task else 'None'}, "
                f"Cancelled state: {final_is_cancelled_state}, Should Reconnect: {self._should_reconnect}"
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

            if self._should_reconnect and not final_is_cancelled_state:
                if self._connection_task is None or self._connection_task.done():
                    self._logger.info("Scheduling reconnection from listener task termination.")
                    self.connect()

    async def _keep_alive(self) -> None:
        """Periodically sends a ping to keep the connection alive."""
        self._logger.info(f"[{self._exchange_name} _keep_alive] Task started.")

        if not self._ping_interval or self._ping_interval <= 0:
            self._logger.info(
                f"[{self._exchange_name} _keep_alive] Ping interval zero/negative, task will not run."
            )
            return

        try:
            self._logger.info(f"[{self._exchange_name} _keep_alive] Entering main loop.")
            while self.is_connected and self._should_reconnect:
                if not self._ws_connection or self._ws_connection.closed:
                    self._logger.warning(
                        "Keep-alive: WebSocket connection is not available or closed."
                    )
                    break

                try:
                    self._logger.debug(f"Sending ping frame for {self._exchange_name}")
                    await self._ws_connection.ping()
                except ConnectionResetError:
                    self._logger.warning(
                        "Keep-alive: Connection reset during ping. Attempting to reconnect."
                    )
                    self._is_connected = False
                    self.connect()
                    break

                self._logger.debug(f"Keep-alive: sleeping for {self._ping_interval}s after ping.")
                await asyncio.sleep(self._ping_interval)

        except asyncio.CancelledError:
            self._logger.info(f"[{self._exchange_name} _keep_alive] Task cancelled.")
        except Exception as e:
            self._logger.error(
                f"[{self._exchange_name} _keep_alive] Unexpected error in loop: {e}",
                exc_info=True,
            )
            self._is_connected = False
            if self._should_reconnect:
                self._logger.info(
                    f"[{self._exchange_name} _keep_alive] Attempting reconnect due to error."
                )
                new_connection_attempt_task = self.connect()
                if not new_connection_attempt_task:
                    self._logger.debug(
                        f"[{self._exchange_name} _keep_alive] Reconnect from error did not start new task."
                    )
        finally:
            self._logger.info(f"[{self._exchange_name} _keep_alive] Task ending.")

    async def send_json(self, data: dict[str, Any]) -> bool:
        """
        Sends a JSON payload over the WebSocket connection.

        Args:
            data: The dictionary to send as JSON.

        Returns:
            True if the message was sent successfully, False otherwise.
        """
        if not self.is_connected or not self._ws_connection:
            self._logger.error(f"Cannot send JSON, WebSocket not connected to {self._ws_url}.")
            return False
        try:
            self._logger.debug(f"Sending JSON: {data}")
            await self._ws_connection.send_json(data)
            return True
        except asyncio.CancelledError:
            self._logger.warning("Send JSON operation cancelled.")
            return False
        except ConnectionResetError:
            self._logger.error(
                f"Connection reset while trying to send JSON to {self._ws_url}. "
                "Marking as disconnected."
            )
            self._is_connected = False
            self._ws_connection = None
            return False
        except Exception as e:
            self._logger.error(f"Error sending JSON to {self._ws_url}: {e}", exc_info=True)
            return False

    async def close(self) -> None:
        """
        Gracefully closes the WebSocket connection and cleans up resources.
        """
        self._logger.info(f"Close requested for WebSocket connection to {self._ws_url}.")
        self._should_reconnect = False

        ws_conn_at_close_start = self._ws_connection

        if self._connection_task and not self._connection_task.done():
            self._logger.debug("Cancelling in-progress connection task.")
            self._connection_task.cancel()
            try:
                await self._connection_task
            except asyncio.CancelledError:
                self._logger.debug("Connection task successfully cancelled during close.")
            except Exception as e:
                self._logger.warning(f"Error awaiting cancelled connection task: {e}")

        if self._listener_task and not self._listener_task.done():
            self._logger.debug("Cancelling listener task.")
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                self._logger.debug("Listener task successfully cancelled.")
            except Exception as e:
                self._logger.warning(f"Error awaiting cancelled listener task: {e}", exc_info=True)

        if self._ping_task and not self._ping_task.done():
            self._logger.debug("Cancelling ping task.")
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                self._logger.debug("Ping task successfully cancelled.")
            except Exception as e:
                self._logger.warning(f"Error awaiting cancelled ping task: {e}", exc_info=True)

        closed_ws_successfully = False
        if ws_conn_at_close_start and not ws_conn_at_close_start.closed:
            self._logger.debug(
                f"Closing WebSocket connection object {id(ws_conn_at_close_start)} "
                f"for {self._ws_url}."
            )
            try:
                await ws_conn_at_close_start.close()
                self._logger.info(
                    f"WS connection to {self._ws_url} (id: "
                    f"{id(ws_conn_at_close_start)}) closed by explicit call."
                )
                closed_ws_successfully = True
            except Exception as e:
                self._logger.error(
                    f"Error during explicit close of WS connection "
                    f"{id(ws_conn_at_close_start)}: {e}",
                    exc_info=True,
                )
        elif ws_conn_at_close_start and ws_conn_at_close_start.closed:
            self._logger.debug(
                f"WS connection {id(ws_conn_at_close_start)} for {self._ws_url} was already closed."
            )
            closed_ws_successfully = True
        else:
            self._logger.debug(
                f"No active WS connection object to close explicitly for {self._ws_url}."
            )
            closed_ws_successfully = True

        self._ws_connection = None
        if closed_ws_successfully:
            self._is_connected = False
        self._is_connected = False

        if self._session and not self._external_session and not self._session.closed:
            self._logger.info("Closing internally created aiohttp.ClientSession.")
            await self._session.close()
            self._logger.info("Internally created ClientSession closed.")
        self._session = None

        self._logger.info(f"WebSocketManager for {self._ws_url} is fully closed.")
