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
from aiohttp import ClientWebSocketResponse

# Import the config model
from .connectivity_models import WebSocketManagerConfig

if TYPE_CHECKING:
    pass

# Define MessageHandler type alias
MessageHandler = Callable[[dict[str, Any]], Coroutine[Any, Any, None]]


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
    ) -> None:
        """
        Initialize the WebSocketManager.

        Args:
            exchange_name: Name of the exchange for logging purposes.
            message_handler: Async callable to process incoming messages.
            config: WebSocketManagerConfig object with connectivity parameters.
            on_connected_callback: Optional async callback to execute after successful connection.
            session: Optional pre-existing aiohttp.ClientSession. If None, one will be created.
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

        self._reconnect_lock = asyncio.Lock()
        self._logger = logging.getLogger(f"WebSocketManager.{self._exchange_name}")
        self._logger.info(f"Initialized for {self._ws_url}")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create an aiohttp ClientSession."""
        if self._session is None or self._session.closed:
            self._logger.info("Creating new aiohttp.ClientSession.")
            self._session = aiohttp.ClientSession()
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
            return self._connection_task  # Return existing task

        self._logger.info(f"Connection requested for {self._ws_url}. Creating new task.")
        # Create and store the task, but do not await it here.
        self._connection_task = asyncio.create_task(
            self._establish_connection(), name=f"{self._exchange_name}_ws_establish_conn"
        )
        return self._connection_task  # Return the new task
        # Removed internal try/except await for self._connection_task

    async def _establish_connection(self) -> None:
        """
        Establishes and maintains the WebSocket connection.
        Includes retry logic with exponential backoff.
        """
        if not self._should_reconnect:
            self._logger.info("Reconnection disabled, not attempting to connect.")
            return

        async with self._reconnect_lock:
            if self.is_connected:
                self._logger.info("Already connected.")
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
                        timeout=self._connection_timeout,  # type: ignore[arg-type] # Reverting to float + ignore
                    )
                    self._is_connected = True
                    current_attempt = 0
                    self._logger.info(f"Successfully connected to {self._ws_url}.")

                    if self._listener_task and not self._listener_task.done():
                        self._listener_task.cancel()
                    if self._ping_task and not self._ping_task.done():
                        self._ping_task.cancel()

                    self._listener_task = asyncio.create_task(
                        self._listen(), name=f"{self._exchange_name}_ws_listen"
                    )
                    if self._ping_interval > 0:
                        self._ping_task = asyncio.create_task(
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
                    self._logger.info("Connection attempt cancelled.")
                    self._should_reconnect = False
                    break
                except (TimeoutError, aiohttp.ClientError) as e:
                    self._logger.warning(f"Connection attempt {current_attempt + 1} failed: {e}")
                    self._is_connected = False
                    self._ws_connection = None
                    current_attempt += 1
                except Exception as e:
                    self._logger.error(
                        f"Unexpected error during connection attempt {current_attempt + 1}: {e}",
                        exc_info=True,
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
        if not self._ws_connection:
            self._logger.error("Listener started without a valid WebSocket connection.")
            return

        self._logger.info("Listener task started.")
        original_connection = self._ws_connection
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
                        f"WebSocket connection error: {self._ws_connection.exception()}"
                    )
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    self._logger.info("WebSocket connection closed by server.")
                    break
                elif msg.type == aiohttp.WSMsgType.CLOSING:
                    self._logger.info("WebSocket connection is closing.")
                    break

        except asyncio.CancelledError:
            self._logger.info("Listener task cancelled.")
        except Exception as e:
            if (
                self._ws_connection
                and self._ws_connection is original_connection
                and not self._ws_connection.closed
            ):
                self._logger.exception(f"Unexpected error in WebSocket listener: {e}")
            else:
                self._logger.info(
                    f"Listener loop for {self._ws_url} exited with error "
                    f"on a stale/closed connection: {e}"
                )
        finally:
            self._logger.info("Listener task stopped.")
            if self._ws_connection is original_connection:
                self._is_connected = False
                self._ws_connection = None

            if self._should_reconnect:
                self._logger.info("Scheduling reconnection from listener task termination.")
                if self._connection_task is None or self._connection_task.done():
                    new_connection_attempt_task = self.connect()
                    if not new_connection_attempt_task:
                        self._logger.debug(
                            "Reconnect from _listen did not start new task (already running?)."
                        )
                else:
                    self._logger.debug("Reconnect task from _listen already running.")

    async def _keep_alive(self) -> None:
        """Periodically sends a ping to keep the connection alive."""
        if not self._ping_interval or self._ping_interval <= 0:
            self._logger.info("Ping interval is zero or negative, keep-alive task will not run.")
            return

        try:  # Outer try for CancelledError and general loop/sleep issues
            while self.is_connected and self._should_reconnect:
                if not self._ws_connection or self._ws_connection.closed:
                    self._logger.warning(
                        "Keep-alive: WebSocket connection is not available or closed."
                    )
                    break  # Exit loop if connection is no longer valid

                try:  # Inner try specifically for ping operation
                    self._logger.debug(f"Sending ping frame for {self._exchange_name}")
                    await self._ws_connection.ping()
                except ConnectionResetError:
                    self._logger.warning(
                        "Keep-alive: Connection reset during ping. Attempting to reconnect."
                    )
                    self._is_connected = False  # Mark as disconnected
                    self.connect()  # Attempt to reconnect - connect() handles task creation
                    break  # Exit _keep_alive loop as connection is reset

                # If there was a custom ping message to send via self._ws_connection.send_str()
                # or similar, it would go here, also within a try-except if needed.

                self._logger.debug(f"Keep-alive: sleeping for {self._ping_interval}s after ping.")
                await asyncio.sleep(self._ping_interval)

        except asyncio.CancelledError:
            self._logger.info("Keep-alive task cancelled.")
            # self._is_connected = False # Optional: update state, though task is ending
            # self._ws_connection = None
        except Exception as e:  # Catch other unexpected errors in the loop/sleep
            self._logger.error(
                f"Unexpected error in keep-alive loop for {self._exchange_name}: {e}",
                exc_info=True,
            )
            self._is_connected = False  # Ensure state reflects potential issue
            # self._ws_connection = None # Connection is likely broken
            if self._should_reconnect:  # Only attempt reconnect if it's enabled
                self._logger.info(
                    f"Attempting to reconnect {self._exchange_name} due to "
                    f"unexpected error in keep-alive."
                )
                new_connection_attempt_task = self.connect()
                if not new_connection_attempt_task:
                    self._logger.debug(
                        "Reconnect from _keep_alive did not start new task (already running?)."
                    )
        finally:
            self._logger.debug(f"Keep-alive for {self._exchange_name} ending.")

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

        if self._ws_connection and not self._ws_connection.closed:
            self._logger.debug(f"Closing WebSocket connection object for {self._ws_url}.")
            await self._ws_connection.close()
            self._logger.info(f"WebSocket connection to {self._ws_url} closed.")

        self._ws_connection = None
        self._is_connected = False

        if self._session and not self._external_session and not self._session.closed:
            self._logger.debug("Closing internally created aiohttp.ClientSession.")
            await self._session.close()
            self._logger.info("Internally created ClientSession closed.")
        self._session = None

        self._logger.info(f"WebSocketManager for {self._ws_url} is fully closed.")
