"""
Position Reconciliation System for the CyberDeltaEngine.

This module provides validation between various position tracking systems to ensure consistency.
"""

import asyncio
import logging
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any

from pydantic import ValidationError  # Add ValidationError

from cyberdelta.apis.base.exchange_api import ExchangeAPI  # Add ExchangeAPI
from cyberdelta.core.models import DerivativePosition, OrderSide
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config
from cyberdelta.utils.parsing import parse_decimal_value  # Add parse_decimal_value

logger = logging.getLogger(__name__)


class PositionReconciliationSystem:
    """
    Validates and reconciles positions between different sources:
    1. Exchange API-reported positions
    2. Fill history-derived positions
    3. Local state tracking

    The system detects discrepancies, provides alerts, and optionally corrects the local state
    to match the authoritative source.
    """

    def __init__(self, config: Config, portfolio_tracker: PortfolioTracker) -> None:
        """
        Initialize the position reconciliation system.

        Args:
            config: Application configuration
            portfolio_tracker: Reference to the portfolio tracker (optional, can be set later)
        """
        self._config = config
        self._portfolio_tracker = portfolio_tracker
        self.latest_results: dict[str, dict[str, Any]] = {}

        # Configuration parameters
        self.reconciliation_threshold = config.get(
            "validation.position_reconciliation.threshold", 0.05
        )  # 5% discrepancy threshold
        self.auto_correct = config.get("validation.position_reconciliation.auto_correct", False)
        self.check_interval = config.get(
            "validation.position_reconciliation.check_interval", 3600
        )  # seconds

        # Track the last reconciliation time
        self.last_check_time: datetime | None = None

        # Record of discrepancies found
        self.discrepancy_history: list[dict[str, Any]] = []

        # Initialize last run time to a long time ago
        getcontext().prec = 50  # Set precision for Decimal operations

        # Internal state
        self._last_reconciliation_run: datetime = datetime.min.replace(tzinfo=UTC)

        # Get interval, ensuring it's a float
        interval_val = config.get("validation.position_reconciliation.interval_seconds", 300.0)
        # DEFENSIVE CHECK: Ensure interval_val is numeric before calculation. Mypy=[operator]
        if isinstance(interval_val, int | float):
            self._reconciliation_interval_secs: float = float(interval_val)
        else:
            logger.warning(
                f"Invalid reconciliation interval type ('{type(interval_val)}'), "
                f"defaulting to 300.0 seconds."
            )
            self._reconciliation_interval_secs = 300.0  # Default float value

        # Initialize next_reconciliation_time (Moved from potentially conditional block)
        self._next_reconciliation_time: datetime = datetime.now(UTC) + timedelta(
            seconds=self._reconciliation_interval_secs
        )

        # Get threshold
        threshold_val = config.get("validation.position_reconciliation.threshold_percent", "5.0")
        self._discrepancy_threshold_percent: Decimal = Decimal(str(threshold_val))

        # Get action mode, ensuring it's a string
        action_mode_val = config.get("validation.position_reconciliation.action_mode", "log")
        if isinstance(action_mode_val, str):
            self._action_mode: str = action_mode_val
        else:
            logger.warning(
                f"Invalid action mode type ('{type(action_mode_val)}'), defaulting to 'log'."
            )
            self._action_mode = "log"

        # Ensure check_interval is timedelta. It's read as int/float from config.
        check_interval_seconds = config.get(
            "validation.position_reconciliation.check_interval", 3600
        )
        # self.check_interval is used by __init__
        _check_interval_td: timedelta
        if isinstance(check_interval_seconds, (int, float)):
            _check_interval_td = timedelta(seconds=check_interval_seconds)
        else:
            logger.warning(
                f"Invalid check_interval type ('{type(check_interval_seconds)}'), defaulting to 3600s."
            )
            _check_interval_td = timedelta(seconds=3600)  # Default timedelta
        self.check_interval = _check_interval_td

        # self.reconciliation_interval is used by check_positions, ensure it is also timedelta
        # This seems to be the same as check_interval in current logic.
        # If interval_seconds from config is the intended value for reconciliation_interval:
        _reconciliation_interval_td: timedelta
        if isinstance(interval_val, (int, float)):
            _reconciliation_interval_td = timedelta(seconds=float(interval_val))
        else:
            # Fallback if interval_val was not numeric
            _reconciliation_interval_td = timedelta(seconds=300.0)
        self.reconciliation_interval = _reconciliation_interval_td

    def register_portfolio_tracker(self, portfolio_tracker: PortfolioTracker) -> None:
        """
        Register the portfolio tracker instance.

        Args:
            portfolio_tracker: The portfolio tracker instance
        """
        self._portfolio_tracker = portfolio_tracker

    async def check_positions(self, force: bool = False) -> dict[str, dict[str, Any]]:
        """
        Checks positions if interval has passed or force=True.
        Returns a dictionary mapping exchange ID to reconciliation results.
        """
        now = datetime.now(UTC)

        interval = self.reconciliation_interval
        # Runtime check for interval type - this should always be timedelta due to __init__
        # if not isinstance(interval, timedelta):
        #     logger.error("Reconciliation interval is not a timedelta. Using default 300s.")
        #     interval = timedelta(seconds=300)

        # Check if interval has passed or if forced
        should_run = force
        if not should_run and self.last_check_time is not None:
            if now >= self.last_check_time + interval:
                should_run = True
        elif self.last_check_time is None:  # First run
            should_run = True

        if not should_run:
            logger.debug("Reconciliation interval not yet passed and not forced.")
            return self.latest_results

        # Get API clients safely
        api_clients = getattr(self._portfolio_tracker, "api_clients", None)
        if not isinstance(api_clients, dict):  # type: ignore[unreachable]
            logger.error("PortfolioTracker api_clients is missing or not a dict.")
            return {}

        self.last_check_time = now  # Update last check time *before* starting

        tasks: dict[str, Awaitable[dict[str, Any]]] = {}
        if api_clients:
            exchange_id_str: str  # Explicit type hint
            for exchange_id_str in api_clients.keys():  # .keys() returns KeysView[str]
                # Ensure exchange_id_str is definitely treated as str by the linter
                current_exchange_id: str = str(exchange_id_str)
                tasks[current_exchange_id] = self._reconcile_exchange(current_exchange_id)
        else:
            logger.warning("No API clients found in PortfolioTracker.")
            return {}

        # Run reconciliation for all exchanges concurrently
        # Original gather logic (may need adjustment later)
        results: list[Any] = await asyncio.gather(*tasks.values(), return_exceptions=True)

        # Original result processing logic (may need adjustment later)
        results_dict: dict[str, Any] = {}
        exchange_keys: list[str] = list(tasks.keys())
        for i, task_result in enumerate(results):
            exchange_name: str = exchange_keys[i]
            if isinstance(task_result, Exception):
                logger.error(f"Reconciliation task for {exchange_name} failed: {task_result}")
                results_dict[exchange_name] = {
                    "success": False,
                    "error": str(task_result),
                    "timestamp": now,
                    "discrepancies": [],
                }
            elif isinstance(task_result, dict):
                results_dict[exchange_name] = task_result
            else:
                logger.error(
                    f"Unexpected result type from gather for {exchange_name}: {type(task_result)}"
                )
                results_dict[exchange_name] = {
                    "success": False,
                    "error": f"Unexpected result type: {type(task_result)}",
                    "timestamp": now,
                    "discrepancies": [],
                }

        self.latest_results = results_dict
        return results_dict

    def _record_discrepancy(self, exchange: str, results: dict[str, Any]) -> None:
        """
        Record a discrepancy for historical tracking.

        Args:
            exchange: Exchange identifier
            results: Reconciliation results from reconcile_positions (contains a "discrepancies" list)
        """
        timestamp = results.get("timestamp", datetime.now(UTC).isoformat())  # Get timestamp safely

        # Ensure "discrepancies" key exists and is a list
        discrepancy_item_list = results.get("discrepancies", [])
        if not isinstance(discrepancy_item_list, list):
            logger.error(
                f"_record_discrepancy: 'discrepancies' in results is not a list for {exchange}. Got: {type(discrepancy_item_list)}"
            )
            return

        for item_any in discrepancy_item_list:  # item_any is Any from list[Any]
            discrepancy: dict[str, Any]
            if isinstance(item_any, dict):
                discrepancy = item_any
            else:
                logger.error(
                    f"_record_discrepancy: item in 'discrepancies' list is not a dict for {exchange}. Item: {item_any}, Type: {type(item_any)}"
                )
                continue

            logger.debug(
                f"_record_discrepancy processing item: {discrepancy}, type: {type(discrepancy)}"
            )
            try:
                # Safely access keys from the discrepancy dictionary
                symbol = discrepancy.get("symbol", "UNKNOWN_SYMBOL")
                exchange_value = discrepancy.get("exchange_value", "N/A")
                local_value = discrepancy.get("local_value", "N/A")
                discrepancy_amount = discrepancy.get(
                    "discrepancy", "N/A"
                )  # Renamed from "discrepancy" to avoid confusion

                record = {
                    "timestamp": timestamp,
                    "exchange": exchange,
                    "symbol": symbol,
                    "exchange_value": exchange_value,
                    "local_value": local_value,
                    "discrepancy_amount": discrepancy_amount,  # Use new var name
                    "corrected": False,  # Default corrected status
                    "raw_details": discrepancy.get("details", "No details provided"),
                    "raw_api_position_summary": discrepancy.get("api_position_summary", "N/A"),
                    "raw_local_position_summary": discrepancy.get("local_position_summary", "N/A"),
                }
                self.discrepancy_history.append(record)

                # Log the successfully processed discrepancy
                logger.warning(
                    f"Position discrepancy recorded: {exchange} {symbol} "
                    f"[Exchange Value: {exchange_value}, Local Value: {local_value}, Diff: {discrepancy_amount}]"
                )

            except TypeError as te:
                # This might catch issues if .get still results in data that can't be processed (e.g. if a Decimal is expected but None is .get)
                logger.error(
                    f"TypeError during discrepancy record creation for {exchange}/{discrepancy.get('symbol', 'ERR_SYM')}: {te}. Discrepancy data: {discrepancy}",
                    exc_info=True,
                )
            except Exception as e:
                logger.error(
                    f"Unexpected error during discrepancy record creation for {exchange}/{discrepancy.get('symbol', 'ERR_SYM')}: {e}. Discrepancy data: {discrepancy}",
                    exc_info=True,
                )

    def _apply_corrections(
        self,
        exchange: str,
        results: dict[str, Any],
        api_positions_map: dict[str, DerivativePosition],  # Pass the map for lookups
    ) -> None:
        """
        Apply corrections to the portfolio tracker based on reconciliation results.

        Args:
            exchange: Exchange identifier.
            results: Reconciliation results containing discrepancies.
            api_positions_map: A map of symbol to API DerivativePosition for the current exchange.
        """
        if not self._portfolio_tracker:
            logger.error("Cannot apply corrections: Portfolio tracker not registered")
            return

        if not results.get("discrepancies"):
            return

        for discrepancy_item in results["discrepancies"]:
            if not isinstance(discrepancy_item, dict):
                logger.warning(
                    f"Skipping non-dict discrepancy item in _apply_corrections: {discrepancy_item}"
                )
                continue

            symbol_val = discrepancy_item.get("symbol")
            symbol: str | None = str(symbol_val) if symbol_val is not None else None

            if not symbol:
                logger.warning(
                    f"Skipping discrepancy with invalid or missing symbol: {discrepancy_item}"
                )
                continue

            try:
                exchange_value_raw = discrepancy_item.get("exchange_value")
                exchange_value_str: str | None = (
                    str(exchange_value_raw) if exchange_value_raw is not None else None
                )

                if exchange_value_str is None:
                    logger.warning(
                        f"Skipping discrepancy for {symbol} due to missing 'exchange_value'"
                    )
                    continue
                exchange_value = Decimal(exchange_value_str)  # This is the corrected size from API
            except InvalidOperation:
                logger.warning(
                    f"Could not parse 'exchange_value' for {symbol} as Decimal: {exchange_value_str}"
                )
                continue

            current_local_position = self._portfolio_tracker.get_position(exchange, symbol)
            api_position_for_correction = api_positions_map.get(symbol)  # Get the full API position

            timestamp_to_use = datetime.now(UTC)  # Default timestamp
            if api_position_for_correction:
                timestamp_to_use = api_position_for_correction.timestamp
            elif current_local_position:
                timestamp_to_use = current_local_position.timestamp

            if current_local_position is None:
                # Position exists on API (or is corrected to exist), but not locally. Create it.
                if exchange_value != Decimal("0"):  # Only create if size is non-zero
                    logger.info(
                        f"Creating missing local position: {exchange} {symbol} size={exchange_value}"
                    )
                    if api_position_for_correction:
                        # Create new position based on full API data, but with corrected size
                        new_local_pos = DerivativePosition(
                            exchange=exchange,
                            symbol=symbol,
                            side=OrderSide.BUY
                            if exchange_value > Decimal("0")
                            else OrderSide.SELL
                            if exchange_value < Decimal("0")
                            else api_position_for_correction.side,
                            size=exchange_value,
                            entry_price=api_position_for_correction.entry_price
                            if exchange_value != Decimal("0")
                            else None,
                            timestamp=api_position_for_correction.timestamp,
                            mark_price=api_position_for_correction.mark_price,
                            liquidation_price=api_position_for_correction.liquidation_price,
                            unrealized_pnl=api_position_for_correction.unrealized_pnl,
                            realized_pnl=api_position_for_correction.realized_pnl,
                            strategy_name=api_position_for_correction.strategy_name,
                            signal_id=api_position_for_correction.signal_id,
                            hl_details=api_position_for_correction.hl_details,
                            bp_details=api_position_for_correction.bp_details,
                        )
                        self._portfolio_tracker.update_position(exchange, new_local_pos)
                    else:
                        # This case should be rare if reconcile_positions ensures API data for discrepancies
                        logger.warning(
                            f"Cannot create local position for {exchange}/{symbol} - missing full API data for discrepancy correction, only size {exchange_value} is known."
                        )
                        # Minimal creation if absolutely necessary:
                        # new_local_pos = DerivativePosition(exchange=exchange, symbol=symbol, side=OrderSide.BUY if exchange_value > 0 else OrderSide.SELL, size=exchange_value, entry_price=None, timestamp=timestamp_to_use)
                        # self._portfolio_tracker.update_position(exchange, new_local_pos)

            elif current_local_position.size != exchange_value:
                # Position exists locally, but size is different. Update it.
                logger.info(
                    f"Correcting local position: {exchange} {symbol} "
                    f"from {current_local_position.size} to {exchange_value}"
                )
                # Update existing position: copy most fields, update size and related
                updated_local_position = current_local_position.model_copy(
                    update={
                        "size": exchange_value,
                        "side": OrderSide.BUY
                        if exchange_value > Decimal("0")
                        else OrderSide.SELL
                        if exchange_value < Decimal("0")
                        else current_local_position.side,
                        "entry_price": current_local_position.entry_price
                        if exchange_value != Decimal("0")
                        else None,
                        "timestamp": timestamp_to_use,  # Update timestamp
                        # Potentially update PnL fields if they can be recalculated or are part of API data
                        # For now, keeping existing PnL, mark_price etc. unless API data provides a better source
                        "unrealized_pnl": api_position_for_correction.unrealized_pnl
                        if api_position_for_correction
                        else current_local_position.unrealized_pnl,
                        "mark_price": api_position_for_correction.mark_price
                        if api_position_for_correction
                        else current_local_position.mark_price,
                        "liquidation_price": api_position_for_correction.liquidation_price
                        if api_position_for_correction
                        else current_local_position.liquidation_price,
                    }
                )
                self._portfolio_tracker.update_position(exchange, updated_local_position)
            else:
                # Sizes match, no correction needed for this discrepancy (might be price diff etc.)
                logger.debug(
                    f"Local position size for {exchange}/{symbol} already matches exchange value {exchange_value}. No size correction needed based on this discrepancy."
                )

            # Mark as corrected in history (if this discrepancy was about size)
            # This logic might need refinement if a discrepancy isn't just size
            if discrepancy_item.get("type") == "size":
                for record in self.discrepancy_history:
                    # Ensure results.get("timestamp") is handled if it could be None
                    results_timestamp = results.get("timestamp")
                    discrepancy_recorded_amount = record.get("discrepancy_amount")
                    discrepancy_item_amount = discrepancy_item.get("discrepancy")

                    if (
                        record["exchange"] == exchange
                        and record["symbol"] == symbol
                        and results_timestamp is not None  # Check for None before comparison
                        and record["timestamp"] == results_timestamp
                        and discrepancy_recorded_amount is not None  # Check for None
                        and discrepancy_item_amount is not None  # Check for None
                        and discrepancy_recorded_amount == discrepancy_item_amount
                    ):
                        record["corrected"] = True
                        break

    def get_discrepancy_history(self, days: int = 7) -> list[dict[str, Any]]:
        """
        Get the history of position discrepancies.

        Args:
            days: Number of days to include in history

        Returns:
            List of discrepancy records
        """
        # Use timezone-aware UTC for comparison with stored aware timestamps
        cutoff_time = datetime.now(UTC) - timedelta(days=days)
        return [r for r in self.discrepancy_history if r["timestamp"] >= cutoff_time]

    def get_latest_results(self) -> dict[str, dict[str, Any]]:
        """
        Get the latest reconciliation results.

        Returns:
            Dictionary of reconciliation results by exchange
        """
        return self.latest_results

    def get_reconciliation_report(self) -> dict[str, Any]:
        """
        Generate a summary report of position reconciliation.

        Returns:
            Report with summary statistics and recent discrepancies
        """
        now = datetime.now(UTC)
        recent_discrepancies = self.get_discrepancy_history(days=1)

        # Group discrepancies by exchange
        exchange_stats: dict[str, dict[str, Any]] = {}  # Add type hint
        for record in recent_discrepancies:
            exchange = record["exchange"]
            if exchange not in exchange_stats:
                # Explicitly define the structure for mypy
                exchange_stats[exchange] = {
                    "total_discrepancies": 0,
                    "symbols_affected": set(),  # Type is set[str]
                    "corrected": 0,
                    "uncorrected": 0,
                }

            # Ensure types are correct before incrementing/adding
            exchange_stats[exchange]["total_discrepancies"] = (
                exchange_stats[exchange]["total_discrepancies"] + 1
            )
            # Mypy knows 'symbols_affected' is a set now
            exchange_stats[exchange]["symbols_affected"].add(
                str(record["symbol"])
            )  # Ensure symbol is str

            if record["corrected"]:
                exchange_stats[exchange]["corrected"] = exchange_stats[exchange]["corrected"] + 1
            else:
                exchange_stats[exchange]["uncorrected"] = (
                    exchange_stats[exchange]["uncorrected"] + 1
                )

        # Convert sets to counts for serialization
        for exchange in exchange_stats:
            # Mypy knows 'symbols_affected' is a set here
            exchange_stats[exchange]["symbols_affected_count"] = len(
                exchange_stats[exchange]["symbols_affected"]
            )  # Store count in a new key
            # Optionally remove the set if not needed further:
            del exchange_stats[exchange]["symbols_affected"]

        return {
            "timestamp": now,
            "total_discrepancies_24h": len(recent_discrepancies),
            "exchange_stats": exchange_stats,
            "recent_discrepancies": recent_discrepancies[:10],  # Latest 10
            "last_check_time": self.last_check_time,
            "auto_correct_enabled": self.auto_correct,
            "reconciliation_threshold": self.reconciliation_threshold,
        }

    async def run_reconciliation(self, force_run: bool = False) -> dict[str, Any]:
        """Run the reconciliation process if the interval has passed or forced."""
        now = datetime.now(UTC)
        # Check if interval has passed
        time_since_last_run = (now - self._last_reconciliation_run).total_seconds()

        # interval_secs_float is already float
        if not force_run and time_since_last_run < self._reconciliation_interval_secs:
            logger.debug(
                f"Skipping reconciliation run. Time since last: {time_since_last_run:.2f}s, "
                f"Interval: {self._reconciliation_interval_secs:.2f}s"
            )
            return self.latest_results

        self._last_reconciliation_run = now

        overall_results: dict[str, Any] = {
            "success": True,
            "timestamp": now,
            "discrepancies": [],
            "symbols_checked": 0,
            "has_discrepancies": False,
            "exchange_results": {},  # Store individual results
        }

        # Check portfolio tracker exists
        # Removed 'is None' check as tracker type is guaranteed by __init__

        reconciliation_tasks: list[Awaitable[dict[str, Any]]] = []
        # Ensure api_clients attribute exists and is a dict before iterating
        api_clients_dict = getattr(self._portfolio_tracker, "api_clients", None)
        if isinstance(api_clients_dict, dict):
            exchange_id_key: Any  # Key from .keys() can be Any initially to the linter
            for exchange_id_key in api_clients_dict.keys():
                exchange_id: str = str(exchange_id_key)  # Explicitly cast to string
                if not isinstance(exchange_id, str):  # Redundant if cast works, but for linter
                    logger.warning(
                        f"Unexpected key type in api_clients_dict: {type(exchange_id)}. Skipping."
                    )
                    continue
                reconciliation_tasks.append(self._reconcile_exchange(exchange_id))
        else:
            logger.warning(
                "PortfolioTracker has no api_clients attribute or it's not a dict. "
                "Skipping reconciliation."
            )
            overall_results["success"] = False
            overall_results["error"] = "PortfolioTracker missing or invalid api_clients"
            return overall_results  # Return early if no clients

        # Run reconciliation for all exchanges concurrently
        results = await asyncio.gather(*reconciliation_tasks, return_exceptions=True)

        # Aggregate results
        for result_item in results:
            if isinstance(result_item, Exception):
                logger.error(
                    f"Exception during exchange reconciliation: {result_item}", exc_info=result_item
                )
                overall_results["success"] = False
                continue  # Move to the next result item

            # Ensure result_item is a dict before accessing keys
            if isinstance(result_item, dict):
                if not result_item.get("success", False):  # Check if 'success' is False
                    overall_results["success"] = False
                    overall_results["error"] = result_item.get(
                        "error", "Unknown error during exchange reconciliation"
                    )
                    overall_results["discrepancies"].extend(result_item.get("discrepancies", []))
                    if result_item.get("discrepancies"):
                        overall_results["has_discrepancies"] = True
                else:
                    overall_results["symbols_checked"] += result_item.get("symbols_checked", 0)
                    overall_results["discrepancies"].extend(result_item.get("discrepancies", []))
                    if result_item.get("has_discrepancies"):
                        overall_results["has_discrepancies"] = True

                exchange_name = result_item.get("exchange", "UNKNOWN_EXCHANGE")
                overall_results["exchange_results"][exchange_name] = result_item
            else:
                logger.error(
                    f"Unexpected item type in reconciliation results: {type(result_item)}. Item: {result_item}"
                )
                overall_results["success"] = False
                overall_results["error"] = f"Unexpected item type in results: {type(result_item)}"

        # Discrepancy recording and auto-correction are handled within _reconcile_exchange.
        # The global calls previously here were problematic and have been removed.

        self.latest_results = overall_results
        return overall_results

    async def _reconcile_exchange(self, exchange: str) -> dict[str, Any]:
        """Reconcile positions for a single exchange."""
        now = datetime.now(UTC)

        api_client = self._portfolio_tracker.api_clients.get(exchange)
        if not api_client:
            logger.warning(f"No API client registered for {exchange}, skipping reconciliation")
            return {
                "success": False,
                "error": f"No API client registered for {exchange}",
                "timestamp": now,
                "discrepancies": [],
            }

        try:
            # 1. Exchange API positions
            exchange_positions_list = (
                await api_client.get_positions()
            )  # Returns list[DerivativePosition]
            logger.info(f"Raw exchange positions from {exchange}: {exchange_positions_list}")

            # 2. Fill history-derived positions (REMOVED - Incorrect dependency/method)
            # fill_positions: list[DerivativePosition] = [] # Unused

            # 3. Local state tracking
            local_positions_list = self._portfolio_tracker.get_positions_by_exchange(
                exchange
            )  # Returns list[DerivativePosition]

            # Convert lists to maps by symbol for reconcile_positions
            # Ensure items are not None before adding to map if API/local can return None in list
            api_positions_map = {
                pos.symbol: pos for pos in exchange_positions_list if pos and hasattr(pos, "symbol")
            }
            local_positions_map = {
                pos.symbol: pos for pos in local_positions_list if pos and hasattr(pos, "symbol")
            }
            # Ensure `results` is properly initialized
            results: dict[str, Any] = {}

            # Call the method that takes maps and iterates symbols
            exchange_results_dict = await self.reconcile_positions(
                exchange, api_positions_map, local_positions_map
            )

            # Store the results
            results = exchange_results_dict

            # Record any discrepancies
            if exchange_results_dict.get("discrepancies"):
                self._record_discrepancy(exchange, exchange_results_dict)

                # Auto-correct if enabled
                if self.auto_correct:
                    self._apply_corrections(exchange, exchange_results_dict, api_positions_map)

            return results

        except Exception as e:
            logger.error(
                f"Error reconciling positions for {exchange} in _reconcile_exchange: {e}",
                exc_info=True,
            )
            # Re-raise the exception to get the full traceback in the test output
            raise

    async def _fetch_api_positions(self, api_clients: dict[str, ExchangeAPI]) -> dict[str, Any]:
        """Fetch positions from all API clients concurrently."""
        tasks: dict[str, asyncio.Task[list[DerivativePosition]]] = {}  # Corrected task type hint
        # isinstance check for api_clients is redundant due to type hint
        # if isinstance(api_clients, dict):
        for exchange_id, client in api_clients.items():
            tasks[exchange_id] = asyncio.create_task(client.get_positions())
        # else:
        #     logger.error("api_clients is not a dictionary, cannot fetch positions.")
        #     return {}

        # Use return_exceptions=True
        results_gather = await asyncio.gather(*tasks.values(), return_exceptions=True)

        positions: dict[str, Any] = {}
        # Map results back using keys
        exchange_ids = list(tasks.keys())  # exchange_ids will be list[str]
        for i, result_item in enumerate(
            results_gather
        ):  # result_item is list[DerivativePosition] | BaseException
            exchange_id = exchange_ids[i]  # exchange_id is str
            if isinstance(result_item, Exception):
                logger.error(f"Failed to fetch positions from {exchange_id}: {result_item}")
                positions[exchange_id] = {"error": str(result_item)}  # Store error
            else:
                # result_item is list[DerivativePosition] here
                positions[exchange_id] = result_item
        return positions

    def _parse_local_position(
        self,
        exchange_id: str,
        symbol: str,
        position_data: DerivativePosition | dict[str, Any] | None,
    ) -> DerivativePosition | None:
        """Safely parse local position data into DerivativePosition model."""
        if position_data is None:
            logger.debug(f"[{exchange_id}/{symbol}] Local position data is None, returning None.")
            return None

        if isinstance(position_data, DerivativePosition):
            # Already a DerivativePosition, perform validation
            if (
                not isinstance(position_data.size, Decimal)
                or not isinstance(position_data.entry_price, Decimal)
                or not position_data.size.is_finite()
                or not position_data.entry_price.is_finite()
            ):
                logger.error(
                    f"[{exchange_id}/{symbol}] Invalid or non-finite values in local "
                    f"DerivativePosition: size={position_data.size} (type: {type(position_data.size)}), "
                    f"price={position_data.entry_price} (type: {type(position_data.entry_price)})"
                )
                return None
            return position_data
        elif isinstance(position_data, dict):
            logger.debug(
                f"[{exchange_id}/{symbol}] Parsing local position from dict. Data: {position_data}"
            )
            try:
                # Ensure 'side' is an OrderSide enum if it's a string
                side_val = position_data["side"]
                if isinstance(side_val, str):
                    side = OrderSide[side_val.upper()]
                elif isinstance(side_val, OrderSide):
                    side = side_val
                else:
                    raise ValueError(f"Invalid type for side: {type(side_val)}")

                return DerivativePosition(
                    symbol=symbol,
                    exchange=exchange_id,
                    timestamp=position_data.get("timestamp", datetime.now(UTC)),
                    side=side,
                    size=Decimal(str(position_data["size"])),
                    entry_price=Decimal(str(position_data["entry_price"])),
                    # Add other fields as necessary, with defaults
                    # mark_price, liquidation_price, unrealized_pnl, etc.
                    mark_price=Decimal(str(position_data.get("mark_price", "0"))),
                    liquidation_price=Decimal(str(position_data.get("liquidation_price", "0"))),
                    unrealized_pnl=Decimal(str(position_data.get("unrealized_pnl", "0"))),
                )
            except (ValidationError, InvalidOperation, AttributeError, TypeError, KeyError) as e:
                logger.error(
                    f"Error parsing local position for {exchange_id}/{symbol} from dict: {e}",
                    exc_info=True,
                )
                return None
        else:
            logger.error(
                f"[{exchange_id}/{symbol}] Unparseable local position data type: {type(position_data)}. "
                f"Expected DerivativePosition or dict. Data: {position_data}"
            )
            return None

    def _parse_api_position(
        self, exchange_id: str, symbol: str, position_data: Any
    ) -> DerivativePosition | None:
        """
        Parses raw position data from an exchange API into a DerivativePosition object.
        If position_data is already a DerivativePosition, it's returned directly after validation.
        Otherwise, attempts to parse from a dictionary structure (e.g. Hyperliquid raw format).

        Args:
            exchange_id: Identifier for the exchange.
            symbol: Trading symbol.
            position_data: Raw position data from the API or an existing DerivativePosition.

        Returns:
            A DerivativePosition object or None if parsing fails.
        """
        if position_data is None:
            logger.debug(f"[{exchange_id}/{symbol}] API position data is None, returning None.")
            return None

        if isinstance(position_data, DerivativePosition):
            # It's already a DerivativePosition, assume it's correctly formed by the API layer.
            # Perform essential validation.
            if (
                not isinstance(position_data.size, Decimal)
                or not isinstance(position_data.entry_price, Decimal)
                or not position_data.size.is_finite()
                or not position_data.entry_price.is_finite()
            ):
                logger.error(
                    f"[{exchange_id}/{symbol}] Invalid or non-finite values in pre-parsed "
                    f"DerivativePosition: size={position_data.size} (type: {type(position_data.size)}), "
                    f"price={position_data.entry_price} (type: {type(position_data.entry_price)})"
                )
                return None
            return position_data
        elif isinstance(position_data, dict):
            logger.debug(
                f"[{exchange_id}/{symbol}] Parsing API position from dict. Data: {position_data}"
            )
            try:
                # Hyperliquid specific fields - adapt if other raw formats needed
                size_str = position_data.get("szi")
                price_str = position_data.get("px")
                timestamp_ms_str = position_data.get("ts")
                # Hyperliquid 'side' is 'B' or 'S', maps to 'long' or 'short' for 'posSide'
                # For simplicity, we'll derive side from size sign if not explicit
                # Or use an explicit side field if available, e.g., position_data.get("side")

                if size_str is None or price_str is None or timestamp_ms_str is None:
                    logger.error(
                        f"[{exchange_id}/{symbol}] Missing essential fields (szi, px, ts) "
                        f"in API position dict. Data: {position_data}"
                    )
                    return None

                size = parse_decimal_value(
                    size_str, field_name="size_str", allow_none=False
                )  # Add field_name
                entry_price = parse_decimal_value(
                    price_str, field_name="price_str", allow_none=False
                )  # Add field_name

                # Ensure timestamp_ms_str is not None before parsing
                if timestamp_ms_str is None:  # Should be caught by earlier check, but defensive
                    logger.error(f"[{exchange_id}/{symbol}] timestamp_ms_str is None unexpectedly.")
                    return None
                timestamp_ms_dec = parse_decimal_value(
                    timestamp_ms_str, field_name="timestamp_ms_str", allow_none=False
                )  # Add field_name
                if timestamp_ms_dec is None:  # Should not happen with allow_none=False
                    logger.error(
                        f"[{exchange_id}/{symbol}] Failed to parse timestamp_ms_str: {timestamp_ms_str}"
                    )
                    return None
                timestamp_ms = int(timestamp_ms_dec)

                if not size.is_finite() or not entry_price.is_finite():
                    logger.error(
                        f"[{exchange_id}/{symbol}] Parsed non-finite numeric values from API dict: "
                        f"size={size}, entry_price={entry_price}"
                    )
                    return None

                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)

                # Determine side from size. Positive size = BUY, Negative size = SELL.
                if size > Decimal("0"):
                    side = OrderSide.BUY
                elif size < Decimal("0"):
                    side = OrderSide.SELL
                else:  # size is zero
                    # For zero size, side is ambiguous. Default to BUY or based on context if available.
                    # Some APIs might provide an explicit side for zero positions.
                    # If not, it might mean the position is flat.
                    # We need a convention for side when size is zero.
                    # For now, if size is 0, we may not have a clear side from this data alone.
                    # Let's assume it implies no active side, or treat as error if side is crucial.
                    logger.info(
                        f"[{exchange_id}/{symbol}] API position size is zero. Side is ambiguous."
                    )
                    # Fallback to a default side (e.g. BUY) or handle as per requirements.
                    # For reconciliation, a zero size position should match a local zero size.
                    side = OrderSide.BUY  # Default, or could be None if model allows

                return DerivativePosition(
                    exchange=exchange_id,
                    symbol=symbol,
                    side=side,
                    size=size,
                    entry_price=entry_price,
                    timestamp=timestamp,
                    # mark_price, liquidation_price, etc., would need to be sourced from dict if available
                )
            except (InvalidOperation, ValueError, TypeError, KeyError) as e:
                logger.error(
                    f"[{exchange_id}/{symbol}] Error parsing API position data from dict: {position_data}. Error: {e}"
                )
                return None
        else:
            logger.error(
                f"[{exchange_id}/{symbol}] Unparseable API position data type: {type(position_data)}. "
                f"Expected DerivativePosition or dict. Data: {position_data}"
            )
            return None

    async def reconcile_positions(
        self, exchange_id: str, api_positions: dict[str, Any], local_positions: dict[str, Any]
    ) -> dict[str, Any]:
        """Reconcile positions for a given exchange."""
        now = datetime.now(UTC)

        overall_results: dict[str, Any] = {
            "success": True,
            "timestamp": now,
            "discrepancies": [],
            "symbols_checked": 0,
            "has_discrepancies": False,
            # "exchange_results": {}, # This key is not meaningfully populated here for a single exchange
        }

        # Check portfolio tracker exists
        # Removed 'is None' check as tracker type is guaranteed by __init__

        reconciliation_tasks: list[Awaitable[dict[str, Any]]] = []
        # Ensure api_clients attribute exists and is a dict before iterating
        # This check for api_clients_dict is not relevant here as we are reconciling symbols for a specific exchange_id
        # The api_positions and local_positions are already provided for this exchange.
        for symbol_key in api_positions:  # Iterate through symbols of the current exchange
            reconciliation_tasks.append(
                self._reconcile_symbol(
                    exchange_id,
                    symbol_key,
                    api_positions[symbol_key],
                    local_positions.get(symbol_key),
                )
            )
        # Also reconcile symbols present only in local_positions
        for symbol_key in local_positions:
            if symbol_key not in api_positions:
                reconciliation_tasks.append(
                    self._reconcile_symbol(
                        exchange_id, symbol_key, None, local_positions[symbol_key]
                    )
                )

        if not reconciliation_tasks:
            logger.info(f"No symbols to reconcile for {exchange_id}.")
            return overall_results  # Return early if no tasks

        # Run reconciliation for all symbols concurrently
        symbol_results_list = await asyncio.gather(*reconciliation_tasks, return_exceptions=True)

        # Aggregate results for the current exchange_id
        for res_item in symbol_results_list:
            if isinstance(res_item, Exception):
                logger.error(
                    f"Exception during symbol reconciliation for {exchange_id}: {res_item}",
                    exc_info=res_item,
                )
                overall_results["success"] = False
                # Attempt to get symbol if possible from the exception or context if available
                # For now, adding a general error entry
                discrep_detail = {
                    "symbol": "UNKNOWN_SYMBOL_DUE_TO_EXCEPTION",
                    "type": "reconciliation_error",
                    "details": str(res_item),
                }
                overall_results["discrepancies"].extend(
                    [discrep_detail]
                )  # Ensure it's a list of dicts
                overall_results["has_discrepancies"] = True
                continue

            if not isinstance(res_item, dict):
                logger.error(
                    f"Unexpected result type in symbol_results_list for {exchange_id}: {type(res_item)}. Item: {res_item}"
                )
                overall_results["success"] = False
                discrep_detail = {
                    "symbol": "UNKNOWN_SYMBOL_DUE_TO_BAD_RESULT_TYPE",
                    "type": "reconciliation_error",
                    "details": f"Unexpected result type: {type(res_item)}",
                }
                overall_results["discrepancies"].extend([discrep_detail])
                overall_results["has_discrepancies"] = True
                continue

            # Now res_item is confirmed to be a dict
            if not res_item.get("success", False):
                overall_results["success"] = False
                overall_results["error"] = res_item.get(
                    "error", f"Unknown error during symbol reconciliation for {exchange_id}"
                )
                # Extend with discrepancies from this symbol's result
                overall_results["discrepancies"].extend(res_item.get("discrepancies", []))
                if res_item.get("discrepancies"):
                    overall_results["has_discrepancies"] = True
            else:
                overall_results["symbols_checked"] += res_item.get("symbols_checked", 0)
                # Extend with discrepancies from this symbol's result
                overall_results["discrepancies"].extend(res_item.get("discrepancies", []))
                if res_item.get("has_discrepancies"):
                    overall_results["has_discrepancies"] = True
            # No longer trying to populate overall_results["exchange_results"] here

        # Record discrepancies for the current exchange_id if any were found
        if overall_results["has_discrepancies"]:
            self._record_discrepancy(exchange_id, overall_results)

            # Auto-correct if enabled
            if self.auto_correct:
                self._apply_corrections(exchange_id, overall_results, api_positions)

        self.latest_results = overall_results
        return overall_results

    async def _reconcile_symbol(
        self, exchange_id: str, symbol: str, api_position: Any, local_position: Any | None
    ) -> dict[str, Any]:
        """Reconcile a single position between API and local state."""
        now = datetime.now(UTC)

        # Parse API position
        parsed_api_position = self._parse_api_position(exchange_id, symbol, api_position)
        if parsed_api_position is None:
            return {
                "success": False,
                "error": f"Failed to parse API position for {symbol} on {exchange_id}",
                "timestamp": now,
                "discrepancies": [],
            }

        # Parse local position
        parsed_local_position = self._parse_local_position(exchange_id, symbol, local_position)
        if parsed_local_position is None:
            return {
                "success": False,
                "error": f"Failed to parse local position for {symbol} on {exchange_id}",
                "timestamp": now,
                "discrepancies": [],
            }

        # Compare positions
        discrepancies = []
        if parsed_api_position.size != parsed_local_position.size:
            discrepancy_details = {
                "symbol": symbol,
                "type": "size",
                "exchange_value": str(parsed_api_position.size),
                "local_value": str(parsed_local_position.size),
                "discrepancy": str(abs(parsed_api_position.size - parsed_local_position.size)),
            }
            discrepancies.append(discrepancy_details)
            logger.warning(f"Discrepancy found for {exchange_id}/{symbol}: {discrepancy_details}")

        if parsed_api_position.entry_price != parsed_local_position.entry_price:
            # Only compare entry_price if both are not None, or if one is None and other is not (for flat vs non-flat)
            # If one is None and other is not, it's a discrepancy unless size is also zero for the one with None entry_price
            is_discrepancy = False
            if (
                parsed_api_position.entry_price is None
                and parsed_local_position.entry_price is not None
            ):
                if parsed_api_position.size != Decimal(
                    "0"
                ):  # API flat but has entry price, or local not flat
                    is_discrepancy = True
            elif (
                parsed_api_position.entry_price is not None
                and parsed_local_position.entry_price is None
            ):
                if parsed_local_position.size != Decimal("0"):
                    is_discrepancy = True
            elif (
                parsed_api_position.entry_price is not None
                and parsed_local_position.entry_price is not None
            ):
                if parsed_api_position.entry_price != parsed_local_position.entry_price:
                    is_discrepancy = True

            if is_discrepancy:
                discrepancy_details = {
                    "symbol": symbol,
                    "type": "entry_price",
                    "exchange_value": str(parsed_api_position.entry_price)
                    if parsed_api_position.entry_price is not None
                    else "None",
                    "local_value": str(parsed_local_position.entry_price)
                    if parsed_local_position.entry_price is not None
                    else "None",
                    "discrepancy": "N/A",  # Diff calculation for None is tricky, just note the difference
                }
                if (
                    parsed_api_position.entry_price is not None
                    and parsed_local_position.entry_price is not None
                ):
                    discrepancy_details["discrepancy"] = str(
                        abs(parsed_api_position.entry_price - parsed_local_position.entry_price)
                    )
                discrepancies.append(discrepancy_details)
                logger.warning(
                    f"Discrepancy found for {exchange_id}/{symbol}: {discrepancy_details}"
                )

        # Handle optional fields (mark_price, liquidation_price, unrealized_pnl)
        # These can be None, so compare carefully
        optional_fields_to_compare = ["mark_price", "liquidation_price", "unrealized_pnl"]
        for field_name in optional_fields_to_compare:
            api_val = getattr(parsed_api_position, field_name)
            local_val = getattr(parsed_local_position, field_name)

            if api_val != local_val:  # Handles None vs Non-None, and Value vs Value
                discrepancy_val_str = "N/A"
                if isinstance(api_val, Decimal) and isinstance(local_val, Decimal):
                    discrepancy_val_str = str(abs(api_val - local_val))

                discrepancy_details = {
                    "symbol": symbol,
                    "type": field_name,
                    "exchange_value": str(api_val) if api_val is not None else "None",
                    "local_value": str(local_val) if local_val is not None else "None",
                    "discrepancy": discrepancy_val_str,
                }
                discrepancies.append(discrepancy_details)
                logger.warning(
                    f"Discrepancy found for {exchange_id}/{symbol}: {discrepancy_details}"
                )
        return {
            "success": True,
            "timestamp": now,
            "discrepancies": discrepancies,
            "symbols_checked": 1,
            "has_discrepancies": len(discrepancies) > 0,
        }

    async def _compare_positions(
        self, api_positions: dict[str, Any], local_positions: dict[str, Any]
    ) -> dict[str, Any]:
        """Compare positions between API and local state."""
        now = datetime.now(UTC)

        overall_results: dict[str, Any] = {
            "success": True,
            "timestamp": now,
            "discrepancies": [],
            "symbols_checked": 0,
            "has_discrepancies": False,
            "exchange_results": {},  # Store individual results
        }

        reconciliation_tasks: list[Awaitable[dict[str, Any]]] = []
        api_clients_dict = getattr(self._portfolio_tracker, "api_clients", None)
        if isinstance(api_clients_dict, dict):
            for symbol in api_positions:
                reconciliation_tasks.append(
                    self._reconcile_symbol(
                        "UNKNOWN_EXCHANGE_IN_COMPARE",  # Placeholder, as exchange_id is not available here
                        symbol,
                        api_positions[symbol],
                        local_positions.get(symbol),
                    )
                )
        else:
            logger.warning("PortfolioTracker missing or invalid api_clients. Skipping comparison.")
            overall_results["success"] = False
            overall_results["error"] = "PortfolioTracker missing or invalid api_clients"
            return overall_results

        results = await asyncio.gather(*reconciliation_tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                # Handle exceptions from gather
                logger.error(f"Error during symbol reconciliation: {result}")
                overall_results["success"] = False
                overall_results["error"] = str(result)  # Or more specific error handling
                # Decide if one error fails the whole comparison or just mark it
            elif isinstance(result, dict) and not result.get("success", False):
                overall_results["success"] = False
                overall_results["error"] = result.get(
                    "error", "Unknown symbol reconciliation error"
                )
                overall_results["discrepancies"].extend(result.get("discrepancies", []))
                overall_results["has_discrepancies"] = True
            elif isinstance(result, dict):
                overall_results["symbols_checked"] += result.get("symbols_checked", 0)
                overall_results["discrepancies"].extend(result.get("discrepancies", []))
                if result.get("has_discrepancies", False):
                    overall_results["has_discrepancies"] = True
                # Optionally store individual symbol results if needed
                # overall_results["exchange_results"][result.get("symbol")] = result

        # Original logic for recording/correcting remains, but might need exchange_id context
        if overall_results["has_discrepancies"]:
            # Need exchange_id here if _record_discrepancy requires it
            # self._record_discrepancy(exchange_id, overall_results)
            logger.warning(
                f"Discrepancies found during comparison: {overall_results['discrepancies']}"
            )
            # if self.auto_correct:
            #     self._apply_corrections(exchange_id, overall_results)

        self.latest_results = overall_results  # Assuming latest_results is for the whole comparison
        return overall_results
