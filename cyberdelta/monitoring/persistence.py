"""Handles persistence (saving and loading) of performance tracking data."""

from __future__ import annotations

import json
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog


# Assuming Decimal might be used in trade/signal data, import if needed
# from decimal import Decimal
# Using the centralized encoder is recommended

logger = structlog.get_logger(__name__)


class PerformanceDataPersistence:
    """Handles saving and loading of performance data to/from JSON files."""

    def __init__(self, output_dir: str) -> None:
        """Initialize the persistence handler.

        Args:
            output_dir: Directory for saving/loading performance data files.

        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Lock to ensure atomic writes/reads if accessed concurrently (optional, depends on usage)
        # If PerformanceTracker handles locking before calling save/load, this might be redundant.
        # For safety, let's include it here.
        self.lock = threading.RLock()
        logger.info(f"Performance data persistence initialized for directory: {self.output_dir}")

    def _get_filepath(self, data_type: str, filename: str) -> Path:
        """Get the full file path for a data type and filename.

        Args:
            data_type: Type of data (e.g., 'returns', 'trades')
            filename: Name of the file

        Returns:
            Full path to the file

        """
        dir_path = self.output_dir / data_type
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path / filename

    def save_data(self, data_type: str, filename: str, data: dict[str, Any] | list[Any]) -> None:
        """Save data to a JSON file.

        Args:
            data_type: Type of data (e.g., 'returns', 'trades')
            filename: Name of the file
            data: Data to save

        """
        filepath = self._get_filepath(data_type, filename)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Make data serializable
            serializable_data = self._make_serializable(data)

            with filepath.open("w", encoding="utf-8") as f:
                json.dump(serializable_data, f, indent=2, ensure_ascii=False)

            logger.debug(f"Saved {data_type} data to {filepath}")

        except Exception as e:
            logger.error(f"Failed to save {data_type} data to {filepath}: {e}")

    def load_data(self, data_type: str, filename: str) -> dict[str, Any] | list[Any] | None:
        """Load data from a JSON file.

        Args:
            data_type: Type of data (e.g., 'returns', 'trades')
            filename: Name of the file

        Returns:
            Loaded data or None if file doesn't exist or loading fails

        """
        filepath = self._get_filepath(data_type, filename)

        if not filepath.exists():
            logger.debug(f"File {filepath} does not exist")
            return None

        try:
            with filepath.open("r", encoding="utf-8") as f:
                loaded_data: dict[str, Any] | list[Any] = json.load(f)

            # Post-process the loaded data (e.g., convert datetime strings back to datetime objects)
            processed_data = self.post_process_loaded_data(data_type, loaded_data)

            logger.debug(f"Loaded {data_type} data from {filepath}")
            return processed_data

        except Exception as e:
            logger.error(f"Failed to load {data_type} data from {filepath}: {e}")
            return None

    def _make_serializable(self, data: dict[str, Any] | list[Any]) -> dict[str, Any] | list[Any]:
        """Make data JSON serializable by converting datetime objects to ISO strings.

        Args:
            data: Data to make serializable

        Returns:
            Serializable data

        """
        if isinstance(data, dict):
            return self._make_dict_serializable(data)
        else:  # data is list[Any]
            serializable_list: list[Any] = []
            for item in data:
                if isinstance(item, dict):
                    serializable_list.append(self._make_dict_serializable(item))
                elif isinstance(item, datetime):
                    serializable_list.append(item.isoformat())
                else:
                    serializable_list.append(item)
            return serializable_list

    def _make_dict_serializable(self, item: dict[str, Any]) -> dict[str, Any]:
        """Make a dictionary JSON serializable.

        Args:
            item: Dictionary to make serializable

        Returns:
            Serializable dictionary

        """
        serializable_item: dict[str, Any] = {}
        for key, value in item.items():
            if isinstance(value, datetime):
                serializable_item[key] = value.isoformat()
            elif isinstance(value, dict):
                # DEFENSIVE CHECK: Recursively handle nested dicts.
                # Pyright=[reportUnknownArgumentType]
                serializable_item[key] = self._make_dict_serializable(value)
            elif isinstance(value, list):
                # DEFENSIVE CHECK: Recursively handle nested lists.
                # Pyright=[reportUnknownArgumentType]
                serializable_item[key] = self._make_serializable(value)
            else:
                serializable_item[key] = value
        return serializable_item

    def post_process_loaded_data(
        self,
        data_type: str,
        loaded_data: dict[str, Any] | list[Any] | None,
    ) -> dict[str, Any] | list[Any] | None:
        """Post-process loaded data to convert datetime strings back to datetime objects.

        Args:
            data_type: Type of data being loaded
            loaded_data: Raw loaded data

        Returns:
            Post-processed data

        """
        if loaded_data is None:
            return None

        if data_type == "returns":
            return self._post_process_returns_data(loaded_data)
        elif data_type in ["trades", "signals", "funding_rates"]:
            return self._post_process_list_data(loaded_data)
        else:
            # Unknown data type, return as-is
            return loaded_data

    def _post_process_returns_data(
        self, loaded_data: dict[str, Any] | list[Any]
    ) -> dict[str, Any] | list[Any]:
        """Post-process returns data to convert timestamp keys back to datetime objects."""
        # For returns data, convert timestamp keys back to datetime objects
        if isinstance(loaded_data, dict):
            processed_returns: dict[str, Any] = {}
            for strategy_name, strategy_data in loaded_data.items():
                if isinstance(strategy_data, dict):
                    processed_strategy_data: dict[datetime, float] = {}
                    # DEFENSIVE CHECK: Handle unknown types from JSON.
                    # Pyright=[reportUnknownVariableType, reportUnknownArgumentType]
                    for ts_str, val in strategy_data.items():
                        try:
                            timestamp = datetime.fromisoformat(str(ts_str))
                            processed_strategy_data[timestamp] = float(val)
                        except (ValueError, TypeError):
                            logger.warning(f"Could not parse timestamp: {ts_str}")
                            continue
                    processed_returns[strategy_name] = processed_strategy_data
                else:
                    processed_returns[strategy_name] = strategy_data
            return processed_returns
        else:
            return loaded_data

    def _post_process_list_data(
        self, loaded_data: dict[str, Any] | list[Any]
    ) -> dict[str, Any] | list[Any]:
        """Post-process list-based data to convert datetime fields in each item."""
        # For list-based data, convert datetime fields in each item
        if isinstance(loaded_data, list):
            processed_list: list[dict[str, Any]] = []
            for item in loaded_data:
                if isinstance(item, dict):
                    # DEFENSIVE CHECK: Handle unknown dict types from JSON.
                    # Pyright=[reportUnknownArgumentType]
                    processed_list.append(self._post_process_dict(item))
                else:
                    processed_list.append(item)
            return processed_list
        else:
            return loaded_data

    def _post_process_dict(self, item: dict[str, Any]) -> dict[str, Any]:
        """Post-process a dictionary to convert datetime strings back to datetime objects.

        Args:
            item: Dictionary to post-process

        Returns:
            Post-processed dictionary

        """
        processed_item: dict[str, Any] = {}
        datetime_fields = [
            "timestamp",
            "entry_time",
            "exit_time",
            "created_at",
            "updated_at",
            "triggered_at",
        ]

        for key, value in item.items():
            if key in datetime_fields and isinstance(value, str):
                try:
                    processed_item[key] = datetime.fromisoformat(value)
                except (ValueError, TypeError):
                    logger.warning(f"Could not parse datetime field {key}: {value}")
                    processed_item[key] = value
            else:
                processed_item[key] = value

        return processed_item

    # --- Specific Load/Save Methods --- #

    def save_returns(self, strategy_name: str, returns_data: dict[datetime, float]) -> None:
        """Save returns data for a strategy."""
        # Convert datetime keys to strings for JSON serialization
        serializable_data = {ts.isoformat(): val for ts, val in returns_data.items()}
        self.save_data("returns", f"{strategy_name}.json", serializable_data)

    def load_all_returns(self) -> dict[str, dict[datetime, float]]:
        """Load all returns data from files.

        Returns:
            Dictionary mapping strategy names to their returns data

        """
        returns_dir = self.output_dir / "returns"
        all_returns: dict[str, dict[datetime, float]] = {}

        if not returns_dir.exists():
            return all_returns

        try:
            for file_path in returns_dir.glob("*.json"):
                strategy_name = file_path.stem
                loaded_data = self.load_data("returns", file_path.name)

                if loaded_data and isinstance(loaded_data, dict):
                    # Extract the strategy data from the loaded data
                    if strategy_name in loaded_data:
                        strategy_data = loaded_data[strategy_name]
                        if isinstance(strategy_data, dict):
                            # DEFENSIVE CHECK: Type conversion for loaded data.
                            # Pyright=[reportArgumentType]
                            all_returns[strategy_name] = strategy_data
                    else:
                        # If the file contains the strategy data directly
                        # DEFENSIVE CHECK: Type conversion for loaded data.
                        # Pyright=[reportArgumentType]
                        all_returns[strategy_name] = loaded_data  # type: ignore[assignment]

        except Exception as e:
            logger.error(f"Failed to load returns data: {e}")

        return all_returns

    def save_trades(self, trades_data: list[dict[str, Any]]) -> None:
        """Save trades data."""
        self.save_data("trades", "trades.json", trades_data)

    def load_trades(self) -> list[dict[str, Any]]:
        """Load trades data."""
        loaded_data = self.load_data("trades", "trades.json")
        if isinstance(loaded_data, list):
            return loaded_data
        return []

    def save_signals(self, signals_data: list[dict[str, Any]]) -> None:
        """Save signals data."""
        self.save_data("signals", "signals.json", signals_data)

    def load_signals(self) -> list[dict[str, Any]]:
        """Load signals data."""
        loaded_data = self.load_data("signals", "signals.json")
        if isinstance(loaded_data, list):
            return loaded_data
        return []

    def save_funding_rates(self, funding_rates_data: list[dict[str, Any]]) -> None:
        """Save funding rates data."""
        self.save_data("funding_rates", "funding_rates.json", funding_rates_data)

    def load_funding_rates(self) -> list[dict[str, Any]]:
        """Load funding rates data."""
        loaded_data = self.load_data("funding_rates", "funding_rates.json")
        if isinstance(loaded_data, list):
            return loaded_data
        return []
