"""Async save_state implementation for PortfolioTracker.

This module extends PortfolioTracker with async state persistence capabilities.
"""

from __future__ import annotations

import asyncio
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.core.portfolio_tracker import PortfolioTracker

logger = get_logger(__name__)


async def save_state(self: PortfolioTracker, state_file_path: str | None = None) -> None:
    """Save portfolio state to persistent storage asynchronously.

    This method serializes the current portfolio state and saves it to a file.
    Uses async I/O for better performance with large state data.

    Args:
        self: The PortfolioTracker instance.
        state_file_path: Optional path to save state file. If not provided,
                        uses default from config.

    Raises:
        IOError: If unable to write to the state file.
        Exception: For other errors during state serialization.
    """
    try:
        logger.info("Starting async portfolio state save...")

        # Determine save path
        if state_file_path is None:
            # Use default path from config if available
            state_file_path = getattr(
                self.app_settings.general, "portfolio_state_file", "data/portfolio_state.json"
            )

        # Ensure state_file_path is not None for type checker
        if state_file_path is None:
            raise ValueError("State file path cannot be None")

        # Ensure directory exists
        save_dir = Path(state_file_path).parent
        save_dir.mkdir(parents=True, exist_ok=True)

        # Prepare state data with metadata
        state_data = {
            "version": "1.0",
            "timestamp": datetime.now(UTC).isoformat(),
            "portfolio_state": self.to_dict(),
            "metadata": {
                "exchanges": list(self.api_clients.keys()),
                "active_symbols": list(self.active_symbols),
                "watchlist": list(self.watchlist),
                "high_watermark": str(self.high_watermark),
                "realized_pnl": str(self.realized_pnl),
            },
        }

        # Convert to JSON string
        json_data = json.dumps(state_data, indent=2, default=str)

        # Use asyncio for file I/O
        await _async_write_file(state_file_path, json_data)

        logger.info(
            "portfolio_state_saved",
            action="save",
            file_path=state_file_path,
            message=f"Portfolio state saved successfully to {state_file_path}",
        )

    except Exception as e:
        logger.error(
            "portfolio_state_save_failed",
            action="save",
            error=str(e),
            message=f"Error saving portfolio state: {e}",
            exc_info=True,
        )
        raise


async def load_state(self: PortfolioTracker, state_file_path: str | None = None) -> bool:
    """Load portfolio state from persistent storage asynchronously.

    This method loads previously saved portfolio state including positions,
    balances, and order history from persistent storage to restore the
    portfolio tracker to its previous state.

    Args:
        self: The PortfolioTracker instance.
        state_file_path: Optional path to load state file from. If not provided,
                        uses default from config.

    Returns:
        True if state was loaded successfully, False otherwise.
    """
    try:
        logger.info("Starting async portfolio state load...")

        # Determine load path
        if state_file_path is None:
            state_file_path = getattr(
                self.app_settings.general, "portfolio_state_file", "data/portfolio_state.json"
            )

        # Ensure state_file_path is not None for type checker
        if state_file_path is None:
            raise ValueError("State file path cannot be None")

        # Check if file exists
        if not os.path.exists(state_file_path):
            logger.warning(
                "state_file_not_found",
                action="load",
                file_path=state_file_path,
                message=f"State file {state_file_path} does not exist",
            )
            return False

        # Read file asynchronously
        json_data = await _async_read_file(state_file_path)

        # Parse JSON
        state_data = json.loads(json_data)

        # Validate version
        version = state_data.get("version", "unknown")
        if version != "1.0":
            logger.warning(
                "unsupported_state_version",
                action="load",
                version=version,
                message=f"Unsupported state file version: {version}",
            )
            return False

        # Restore portfolio state
        portfolio_state = state_data.get("portfolio_state", {})
        if portfolio_state:
            # Use the from_dict class method to create a new instance
            # then copy its state to self
            temp_tracker = self.from_dict(portfolio_state, self.app_settings, self.pt_config)

            # Copy state from temp_tracker to self
            self.balances = temp_tracker.balances
            self.positions = temp_tracker.positions
            self.orders = temp_tracker.orders
            self.last_update_time = temp_tracker.last_update_time
            self.last_reconciliation_time = temp_tracker.last_reconciliation_time
            self.high_watermark = temp_tracker.high_watermark
            self.realized_pnl = temp_tracker.realized_pnl

        # Restore metadata
        metadata = state_data.get("metadata", {})
        if metadata:
            self.active_symbols = set(metadata.get("active_symbols", []))
            self.watchlist = set(metadata.get("watchlist", []))

        logger.info(
            "portfolio_state_loaded",
            action="load",
            file_path=state_file_path,
            timestamp=state_data.get("timestamp", "unknown"),
            message=(
                f"Portfolio state loaded successfully from {state_file_path}. "
                f"Timestamp: {state_data.get('timestamp', 'unknown')}"
            ),
        )
        return True

    except json.JSONDecodeError as e:
        logger.error(
            "state_file_decode_failed",
            action="load",
            file_path=state_file_path,
            error=str(e),
            message=f"Error decoding state file {state_file_path}: {e}",
        )
        return False
    except Exception as e:
        logger.error(
            "portfolio_state_load_failed",
            action="load",
            error=str(e),
            message=f"Error loading portfolio state: {e}",
            exc_info=True,
        )
        return False


async def _async_write_file(file_path: str, content: str) -> None:
    """Write content to a file asynchronously.

    Args:
        file_path: Path to the file to write.
        content: Content to write to the file.
    """
    loop = asyncio.get_event_loop()

    def _write_sync() -> None:
        # Write to temporary file first for atomicity
        temp_path = f"{file_path}.tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(content)
        # Atomic rename
        os.replace(temp_path, file_path)

    await loop.run_in_executor(None, _write_sync)


async def _async_read_file(file_path: str) -> str:
    """Read content from a file asynchronously.

    Args:
        file_path: Path to the file to read.

    Returns:
        Content of the file as a string.
    """
    loop = asyncio.get_event_loop()

    def _read_sync() -> str:
        with open(file_path, encoding="utf-8") as f:
            return f.read()

    return await loop.run_in_executor(None, _read_sync)


# Monkey-patch the methods to PortfolioTracker
def patch_portfolio_tracker() -> None:
    """Patch the PortfolioTracker class with async save/load methods."""
    from cyberdelta.core.portfolio_tracker import PortfolioTracker

    # Replace the placeholder methods with our async implementations
    # Cast to Any to satisfy type checker, then assign methods
    pt_any = cast(Any, PortfolioTracker)
    pt_any.save_state = save_state
    pt_any.load_state = load_state

    logger.info("PortfolioTracker patched with async save_state and load_state methods")
