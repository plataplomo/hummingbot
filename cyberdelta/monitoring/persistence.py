"""
Handles persistence (saving and loading) of performance tracking data.
"""

import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

# Assuming Decimal might be used in trade/signal data, import if needed
# from decimal import Decimal
# Using the centralized encoder is recommended
from cyberdelta.utils.serialization import CyberDeltaJSONEncoder

logger = logging.getLogger(__name__)


class PerformanceDataPersistence:
    """Handles loading and saving performance data to/from JSON files."""

    def __init__(self, output_dir: str) -> None:
        """
        Initialize the persistence handler.

        Args:
            output_dir: The base directory where data files are stored.
        """
        self.output_dir = Path(output_dir)
        # Ensure base directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Lock to ensure atomic writes/reads if accessed concurrently (optional, depends on usage)
        # If PerformanceTracker handles locking before calling save/load, this might be redundant.
        # For safety, let's include it here.
        self.lock = threading.RLock()
        logger.info(f"Performance data persistence initialized for directory: {self.output_dir}")

    def _get_filepath(self, data_type: str, filename: str) -> Path:
        """Constructs the full path for a data file."""
        dir_path = self.output_dir / data_type
        dir_path.mkdir(parents=True, exist_ok=True)
        return dir_path / filename

    # Changed data type from Any to dict | list for better type safety where possible
    def save_data(self, data_type: str, filename: str, data: dict[str, Any] | list[Any]) -> None:
        """Saves generic data (dict or list) to a JSON file."""
        filepath = self._get_filepath(data_type, filename)
        serializable_data = self._make_serializable(data)
        try:
            with self.lock:
                with open(filepath, "w", encoding="utf-8") as f:
                    # Use standard json.dump with our custom encoder
                    json.dump(serializable_data, f, indent=2, cls=CyberDeltaJSONEncoder)
            logger.debug(f"Saved {data_type} data to {filepath}")
        except TypeError as e:
            logger.error(
                f"Serialization error saving {filepath}: {e}. "
                f"Data type: {type(data)}. Check contents.",
                exc_info=True,
            )
        except OSError as e:
            logger.error(f"File system error saving {filepath}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error saving {filepath}: {e}", exc_info=True)

    # Changed return type from Any | None
    def load_data(self, data_type: str, filename: str) -> dict[str, Any] | list[Any] | None:
        """Loads generic data (dict or list) from a JSON file."""
        filepath = self._get_filepath(data_type, filename)
        if not filepath.exists():
            logger.debug(f"Data file not found, skipping load: {filepath}")
            return None
        try:
            with self.lock:
                with open(filepath, encoding="utf-8") as f:
                    # Use the utility load_json or standard json.load
                    loaded_data = json.load(f)
            logger.debug(f"Loaded {data_type} data from {filepath}")
            # Post-processing (e.g., datetime conversion) should happen AFTER loading
            # Ensure loaded data is dict or list before returning
            # Ruff UP038 Fix: Use X | Y
            if isinstance(loaded_data, dict | list):
                return loaded_data  # Ignore partially unknown type
            else:
                logger.warning(
                    f"Loaded data from {filepath} is not dict or list: {type(loaded_data)}"
                )
                return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON decoding error loading {filepath}: {e}", exc_info=True)
        except OSError as e:
            logger.error(f"File system error loading {filepath}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Unexpected error loading {filepath}: {e}", exc_info=True)
        return None

    # Changed input/output types from Any
    def _make_serializable(self, data: dict[str, Any] | list[Any]) -> dict[str, Any] | list[Any]:
        """Converts data structures containing non-serializable types (like datetime)."""
        if isinstance(data, dict):
            # Handle returns dict {strategy: {timestamp: value}}
            # Check if values are dicts and keys within those are datetime
            is_returns_dict = False
            if data:
                first_val = next(iter(data.values()))
                if isinstance(first_val, dict) and first_val:
                    # Ignore unknown type for key k during iteration
                    is_returns_dict = all(isinstance(k, datetime) for k in first_val.keys())

            if is_returns_dict:
                return {
                    strategy: {
                        ts.isoformat(): val for ts, val in returns.items()
                    }  # Ignore potentially undefined 'returns'
                    for strategy, returns in data.items()
                }
            # General dict processing
            # Ruff UP038 fix: Use X | Y
            return {
                # Ignore unknown type for v
                k: self._make_serializable(v) if isinstance(v, dict | list) else v
                for k, v in data.items()
            }
        # Remove unnecessary elif check, if not dict, it must be list based on type hint
        else:
            # Handle list of trades/signals/funding_rates (which are dicts)
            return [
                self._make_dict_serializable(item) for item in data
            ]  # Ignore iterating over list[Any]

    def _make_dict_serializable(self, item: dict[str, Any]) -> dict[str, Any]:
        """Makes a single dictionary (like a trade or signal) serializable."""
        item_copy = item.copy()
        for key, value in item_copy.items():
            if isinstance(value, datetime):
                item_copy[key] = value.isoformat()
            # Recursively handle nested dicts/lists if necessary
            elif isinstance(value, dict | list):
                item_copy[key] = self._make_serializable(value)  # Ignore unknown type for value
            # Add Decimal handling if needed and not using encoder that stringifies it
            # elif isinstance(value, Decimal):
            #     item_copy[key] = str(value)
        return item_copy

    # Changed loaded_data type from Any, return type from Any
    def post_process_loaded_data(
        self, data_type: str, loaded_data: dict[str, Any] | list[Any] | None
    ) -> dict[str, Any] | list[Any] | None:
        """Converts loaded data structures back (e.g., string to datetime)."""
        if loaded_data is None:
            return None  # Or appropriate default (e.g., empty list/dict)

        if data_type == "returns" and isinstance(loaded_data, dict):
            # Input: {strategy: {iso_timestamp_str: value}}
            # Mypy fix: Add type annotation
            processed_data: dict[str, dict[datetime, float]] = {}
            for strategy, returns_dict in loaded_data.items():
                if isinstance(returns_dict, dict):
                    processed_data[strategy] = {}
                    # Ignore unknown types for ts_str, val
                    for ts_str, val in returns_dict.items():
                        if isinstance(ts_str, str):
                            try:
                                # Assuming val is float or compatible
                                # Ignore unknown type for val
                                processed_data[strategy][datetime.fromisoformat(ts_str)] = float(
                                    val
                                )
                            except (ValueError, TypeError):
                                logger.warning(
                                    f"Could not parse timestamp {ts_str} or value "
                                    f"{val} in {data_type} data"
                                )
                        else:
                            logger.warning(
                                f"Non-string timestamp key '{ts_str}' found in returns for "
                                f"{strategy}"
                            )
                else:
                    logger.warning(
                        f"Invalid returns format for strategy {strategy}: "
                        f"expected dict, got {type(returns_dict)}"
                    )
            return processed_data
        elif data_type in ["trades", "signals", "funding_rates"] and isinstance(loaded_data, list):
            # Input: list[dict]
            processed_list = []
            # Ignore unknown type for item_dict
            for item_dict in loaded_data:
                if isinstance(item_dict, dict):
                    processed_list.append(self._post_process_dict(item_dict))
                else:
                    logger.warning(f"Non-dict item found in {data_type} list: {type(item_dict)}")
            return processed_list  # Ignore partially unknown return type
        else:
            logger.warning(
                f"Loaded data for {data_type} is not the expected type (dict/list): "
                f"{type(loaded_data)}"
            )
            return loaded_data  # Return original if type mismatch

    def _post_process_dict(self, item: dict[str, Any]) -> dict[str, Any]:
        """Converts known string fields back to datetime in a loaded dict."""
        item_copy = item.copy()
        # Define keys that might contain ISO datetime strings
        datetime_keys = ["timestamp", "entry_time", "exit_time"]
        for key in datetime_keys:
            if key in item_copy and isinstance(item_copy[key], str):
                try:
                    item_copy[key] = datetime.fromisoformat(item_copy[key])
                except (ValueError, TypeError):
                    logger.warning(
                        f"Could not parse datetime string '{item_copy[key]}' for key '{key}'"
                    )
                    item_copy[key] = None  # Set to None if parsing fails
        # Recursively handle nested structures if needed
        for key, value in item_copy.items():
            if isinstance(value, dict):
                item_copy[key] = self._post_process_dict(value)
            # Could add list handling if nested lists with datetimes are expected

        return item_copy

    # --- Specific Load/Save Methods --- #

    def save_returns(self, strategy_name: str, returns_data: dict[datetime, float]) -> None:
        """Saves returns for a specific strategy."""
        # Data needs conversion {datetime: val} -> {iso_str: val}
        serializable_data = {ts.isoformat(): val for ts, val in returns_data.items()}
        self.save_data("returns", f"{strategy_name}.json", serializable_data)

    def load_all_returns(self) -> dict[str, dict[datetime, float]]:
        """Loads returns for all strategies found in the returns directory."""
        # Mypy fix: Add type annotation
        all_returns: dict[str, dict[datetime, float]] = {}
        returns_dir = self.output_dir / "returns"
        if not returns_dir.is_dir():
            return all_returns
        with self.lock:
            for filepath in returns_dir.glob("*.json"):
                strategy_name = filepath.stem
                # Load raw dict {str: val}
                loaded_data = self.load_data("returns", filepath.name)
                if isinstance(loaded_data, dict):
                    # Post-process: convert keys back to datetime
                    all_returns[strategy_name] = {}
                    # Ignore unknown types
                    for ts_str, val in loaded_data.items():
                        # Ignore unnecessary isinstance check
                        # if isinstance(ts_str, str):
                        try:
                            # Assuming val is float or compatible
                            # Ignore unknown type for val
                            all_returns[strategy_name][datetime.fromisoformat(ts_str)] = float(val)
                        except (ValueError, TypeError):
                            logger.warning(
                                f"Could not parse timestamp {ts_str} or value "
                                f"{val} in returns file {filepath.name}"
                            )
                        # else:
                        #     logger.warning(
                        #         f"Non-string timestamp key found in returns file {filepath.name}"
                        #     )
                else:
                    logger.warning(
                        f"Loaded data for {strategy_name} returns is not a dict: "
                        f"{type(loaded_data)}"
                    )
        return all_returns

    def save_trades(self, trades_data: list[dict[str, Any]]) -> None:
        """Saves the list of trades."""
        # Needs conversion for datetime objects within the dicts
        self.save_data("trades", "trades.json", trades_data)

    def load_trades(self) -> list[dict[str, Any]]:
        """Loads the list of trades."""
        loaded_data = self.load_data("trades", "trades.json")
        # Ensure loaded_data is list before passing to post-processing
        if not isinstance(loaded_data, list):
            logger.warning(f"Loaded trades data is not a list: {type(loaded_data)}")
            loaded_data = []
        processed_data = self.post_process_loaded_data("trades", loaded_data)
        return processed_data if isinstance(processed_data, list) else []

    def save_signals(self, signals_data: list[dict[str, Any]]) -> None:
        """Saves the list of signals."""
        self.save_data("signals", "signals.json", signals_data)

    def load_signals(self) -> list[dict[str, Any]]:
        """Loads the list of signals."""
        loaded_data = self.load_data("signals", "signals.json")
        if not isinstance(loaded_data, list):
            logger.warning(f"Loaded signals data is not a list: {type(loaded_data)}")
            loaded_data = []
        processed_data = self.post_process_loaded_data("signals", loaded_data)
        return processed_data if isinstance(processed_data, list) else []

    def save_funding_rates(self, funding_rates_data: list[dict[str, Any]]) -> None:
        """Saves the list of funding rates."""
        self.save_data("funding_rates", "funding_rates.json", funding_rates_data)

    def load_funding_rates(self) -> list[dict[str, Any]]:
        """Loads the list of funding rates."""
        loaded_data = self.load_data("funding_rates", "funding_rates.json")
        if not isinstance(loaded_data, list):
            logger.warning(f"Loaded funding_rates data is not a list: {type(loaded_data)}")
            loaded_data = []
        processed_data = self.post_process_loaded_data("funding_rates", loaded_data)
        return processed_data if isinstance(processed_data, list) else []
