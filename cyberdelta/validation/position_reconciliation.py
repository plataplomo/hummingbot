"""
Position Reconciliation System for the CyberDeltaEngine.

This module provides validation between various position tracking systems to ensure consistency.
"""

import asyncio
import logging
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any, Literal, cast

from cyberdelta.apis.base.exchange_api import ExchangeAPI  # Add ExchangeAPI
from cyberdelta.core.models import DerivativePosition, OrderSide
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config
from cyberdelta.validation.models.discrepancy_detail import (
    DiscrepancyDetail,
    HistoricalDiscrepancyRecord,
)

logger = logging.getLogger(__name__)

# Type Aliases for parsed position data and errors
type ErrorDict = dict[Literal["error", "message", "raw_data"], Any]
type ParsedPosition = dict[
    Literal[
        "side",
        "size",
        "entry_price",
        "mark_price",
        "liquidation_price",
        "unrealized_pnl",
        # Potentially other common fields if needed for comparison
    ],
    Decimal
    | OrderSide
    | str
    | None,  # Allow str for raw values before conversion, ensure final is Decimal/OrderSide/None
]


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
        self.logger = logger
        self._config = config
        self._portfolio_tracker = portfolio_tracker
        self.latest_results: dict[str, dict[str, Any | list[DiscrepancyDetail]]] = {}

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
        self.discrepancy_history: list[HistoricalDiscrepancyRecord] = []

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
        if isinstance(check_interval_seconds, int | float):
            _check_interval_td = timedelta(seconds=check_interval_seconds)
        else:
            logger.warning(
                f"Invalid check_interval type ('{type(check_interval_seconds)}'), "
                f"defaulting to 3600s."
            )
            _check_interval_td = timedelta(seconds=3600)  # Default timedelta
        self.check_interval = _check_interval_td

        # self.reconciliation_interval is used by check_positions, ensure it is also timedelta
        # This seems to be the same as check_interval in current logic.
        # If interval_seconds from config is the intended value for reconciliation_interval:
        _reconciliation_interval_td: timedelta
        if isinstance(interval_val, int | float):
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
        api_clients_any = getattr(self._portfolio_tracker, "api_clients", None)
        if not isinstance(api_clients_any, dict):
            logger.error("PortfolioTracker api_clients is missing or not a dict.")
            return {}
        api_clients: dict[str, ExchangeAPI] = cast(dict[str, ExchangeAPI], api_clients_any)

        self.last_check_time = now  # Update last check time *before* starting

        tasks: dict[str, Awaitable[dict[str, Any]]] = {}
        # exchange_id_str is already known to be str from api_clients type hint
        for exchange_id_str in api_clients.keys():
            tasks[exchange_id_str] = self._reconcile_exchange(exchange_id_str)

        # Run reconciliation for all exchanges concurrently
        results_any_list: list[Any] = await asyncio.gather(*tasks.values(), return_exceptions=True)

        results_dict: dict[str, Any] = {}
        exchange_keys: list[str] = list(tasks.keys())
        for i, task_result_any in enumerate(results_any_list):
            exchange_name: str = exchange_keys[i]
            if isinstance(task_result_any, Exception):
                logger.error(f"Reconciliation task for {exchange_name} failed: {task_result_any}")
                results_dict[exchange_name] = {
                    "success": False,
                    "error": str(task_result_any),
                    "timestamp": now,
                    "discrepancies": [],  # Expected list[DiscrepancyDetail]
                }
            elif isinstance(task_result_any, dict):
                results_dict[exchange_name] = cast(dict[str, Any], task_result_any)
            else:
                logger.error(
                    f"Unexpected result type from gather for {exchange_name}: "
                    f"{type(task_result_any)}"
                )
                results_dict[exchange_name] = {
                    "success": False,
                    "error": f"Unexpected result type: {type(task_result_any)}",
                    "timestamp": now,
                    "discrepancies": [],  # Expected list[DiscrepancyDetail]
                }

        self.latest_results = results_dict
        return results_dict

    def _record_discrepancy(
        self,
        exchange_id: str,
        symbol: str,
        discrepancy_type: Literal[
            "size",
            "entry_price",
            "mark_price",
            "liquidation_price",
            "unrealized_pnl",
            "api_parsing_error",
            "local_parsing_error",
            "reconciliation_error",
            "unknown_api_symbol",
            "unknown_local_symbol",
        ],
        api_val: Decimal | OrderSide | str | None,
        local_val: Decimal | OrderSide | str | None,
        details: str | None = None,
    ) -> HistoricalDiscrepancyRecord:
        """Helper to create and log a DiscrepancyDetail, then record it historically."""
        # Ensure values are stringified for DiscrepancyDetail storage
        exchange_value_str = str(api_val) if api_val is not None else "None"
        local_value_str = str(local_val) if local_val is not None else "None"

        discrepancy_detail = DiscrepancyDetail(
            symbol=symbol,
            discrepancy_type=discrepancy_type,
            exchange_value=exchange_value_str,
            local_value=local_value_str,
            details=details,
        )

        # Create and append HistoricalDiscrepancyRecord
        historical_record = HistoricalDiscrepancyRecord(
            detail=discrepancy_detail,
            exchange_id=exchange_id,
            recorded_at=datetime.now(UTC),
            is_corrected=False,
        )
        self.discrepancy_history.append(historical_record)

        logger.warning(
            f"Position discrepancy recorded: {historical_record.exchange_id} "
            f"{historical_record.detail.symbol} "
            f"Type: {historical_record.detail.discrepancy_type} "
            f"[Exchange Value: {historical_record.detail.exchange_value}, "
            f"Local Value: {historical_record.detail.local_value}] "
            f"Details: {historical_record.detail.details or 'N/A'}"
        )

        return historical_record

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

        # The `results["discrepancies"]` list should now contain DiscrepancyDetail objects.
        discrepancy_details_list_any = results.get("discrepancies", [])
        if not isinstance(discrepancy_details_list_any, list):
            logger.error(
                "_apply_corrections: results['discrepancies'] is not a list. "
                f"Got: {type(discrepancy_details_list_any)}"
            )
            return

        discrepancy_details_list = cast(list[DiscrepancyDetail], discrepancy_details_list_any)

        for discrepancy_detail in discrepancy_details_list:  # This is a DiscrepancyDetail model
            # We are primarily correcting size discrepancies here.
            if discrepancy_detail.discrepancy_type != "size":
                logger.debug(
                    f"Skipping correction for non-size discrepancy type: "
                    f"{discrepancy_detail.discrepancy_type} for symbol {discrepancy_detail.symbol}"
                )
                continue

            symbol = discrepancy_detail.symbol  # Directly from the DiscrepancyDetail model

            try:
                # exchange_value in DiscrepancyDetail is already a string representation or None
                exchange_value_str = discrepancy_detail.exchange_value
                if exchange_value_str is None:
                    logger.warning(
                        f"Skipping discrepancy for {symbol} due to missing "
                        f"'exchange_value' in DiscrepancyDetail"
                    )
                    continue
                exchange_value = Decimal(exchange_value_str)  # Corrected size from API as Decimal
            except InvalidOperation:  # This except block uses exchange_value_str
                # DEFENSIVE CHECK: exchange_value_str might be flagged by linter as possibly unbound
                # due to control flow with 'continue' in try-except. Logic is sound.
                logger.warning(
                    f"Could not parse 'exchange_value' from DiscrepancyDetail for {symbol} "
                    f"as Decimal: {exchange_value_str}"
                )
                continue

            current_local_position = self._portfolio_tracker.get_position(exchange, symbol)
            api_position_for_correction = api_positions_map.get(
                symbol
            )  # Full API DerivativePosition

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

            # Mark as corrected in history if a portfolio change was made for this size discrepancy
            results_timestamp_dt = results.get("timestamp")  # Timestamp of the reconciliation run
            if results_timestamp_dt is None:
                logger.warning(
                    f"Missing 'timestamp' in results dict during correction for "
                    f"{exchange}/{symbol}, cannot accurately mark historical record."
                )
                continue  # Cannot match without timestamp
            if not isinstance(results_timestamp_dt, datetime):
                logger.warning(
                    f"Invalid 'timestamp' type in results dict: "
                    f"{type(results_timestamp_dt)}. Cannot mark historical record."
                )
                continue

            for historical_record in (
                self.discrepancy_history
            ):  # self.discrepancy_history is list[HistoricalDiscrepancyRecord]
                # Match against the specific DiscrepancyDetail that triggered this correction
                if (
                    historical_record.exchange_id == exchange
                    and historical_record.detail.symbol
                    == symbol  # symbol is from discrepancy_detail.symbol
                    and historical_record.recorded_at == results_timestamp_dt
                    and historical_record.detail.discrepancy_type
                    == "size"  # We are correcting a size discrepancy
                    and historical_record.detail
                    == discrepancy_detail  # Match the exact DiscrepancyDetail instance
                    and not historical_record.is_corrected
                ):
                    historical_record.is_corrected = True
                    logger.info(
                        f"Marked historical discrepancy as corrected: "
                        f"{historical_record.model_dump_json(indent=2)}"
                    )
                    break  # Found and updated the specific historical record

    def get_discrepancy_history(self, days: int = 7) -> list[HistoricalDiscrepancyRecord]:
        """
        Get the history of position discrepancies.
        Returns a list of HistoricalDiscrepancyRecord objects, filtered by days.

        Args:
            days: Number of days to include in history.

        Returns:
            List of HistoricalDiscrepancyRecord records within the specified timeframe.
        """
        cutoff_time = datetime.now(UTC) - timedelta(days=days)
        return [r for r in self.discrepancy_history if r.recorded_at >= cutoff_time]

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
        """
        now = datetime.now(UTC)
        # Get historical records for the last 24 hours (1 day)
        recent_historical_records: list[HistoricalDiscrepancyRecord] = self.get_discrepancy_history(
            days=1
        )

        exchange_stats: dict[str, dict[str, Any]] = {}
        reportable_discrepancies_for_summary: list[dict[str, Any]] = []

        for record in recent_historical_records:
            # Construct a dictionary for the 'recent_discrepancies' part of the report
            discrepancy_summary_item = {
                "timestamp": record.recorded_at.isoformat(),  # Use recorded_at from the historical record
                "exchange": record.exchange_id,
                "symbol": record.detail.symbol,
                "discrepancy_type": record.detail.discrepancy_type,
                "exchange_value": record.detail.exchange_value,
                "local_value": record.detail.local_value,
                "details": record.detail.details,
                "corrected": record.is_corrected,  # Use is_corrected from the historical record
            }
            reportable_discrepancies_for_summary.append(discrepancy_summary_item)

            # Aggregate stats per exchange
            current_exchange_id = record.exchange_id
            if current_exchange_id not in exchange_stats:
                exchange_stats[current_exchange_id] = {
                    "total_discrepancies": 0,
                    "symbols_affected_list": [],  # Temp list to count unique symbols
                    "corrected_count": 0,  # Renamed for clarity
                    "uncorrected_count": 0,  # Renamed for clarity
                }

            exchange_stats[current_exchange_id]["total_discrepancies"] = (
                cast(int, exchange_stats[current_exchange_id]["total_discrepancies"]) + 1
            )
            if (
                record.detail.symbol
                not in exchange_stats[current_exchange_id]["symbols_affected_list"]
            ):
                exchange_stats[current_exchange_id]["symbols_affected_list"].append(
                    record.detail.symbol
                )

            if record.is_corrected:
                exchange_stats[current_exchange_id]["corrected_count"] = (
                    cast(int, exchange_stats[current_exchange_id]["corrected_count"]) + 1
                )
            else:
                exchange_stats[current_exchange_id]["uncorrected_count"] = (
                    cast(int, exchange_stats[current_exchange_id]["uncorrected_count"]) + 1
                )

        # Finalize exchange_stats by calculating symbols_affected_count
        for exchange_key in exchange_stats:
            exchange_stats[exchange_key]["symbols_affected_count"] = len(
                exchange_stats[exchange_key].pop("symbols_affected_list")  # Pop and get length
            )

        return {
            "report_generated_at": now.isoformat(),  # Changed key for clarity
            "total_discrepancies_24h": len(reportable_discrepancies_for_summary),
            "exchange_specific_stats": exchange_stats,  # Changed key for clarity
            "recent_discrepancies_summary": reportable_discrepancies_for_summary[
                :10
            ],  # Latest 10 summary items
            "last_reconciliation_check_time": self.last_check_time.isoformat()
            if self.last_check_time
            else None,  # Changed key
            "auto_correct_enabled": self.auto_correct,
            "reconciliation_threshold_config": str(
                self.reconciliation_threshold
            ),  # Changed key & made str
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
        api_clients_dict_any = getattr(self._portfolio_tracker, "api_clients", None)
        if isinstance(api_clients_dict_any, dict):
            api_clients_dict = cast(dict[str, ExchangeAPI], api_clients_dict_any)
            for exchange_id_key in api_clients_dict.keys():  # exchange_id_key is str
                exchange_id: str = str(
                    exchange_id_key
                )  # Explicitly cast to string, though already str
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
        self.logger.debug(f"PRS._reconcile_exchange: Starting for {exchange}")

        api_client = self._portfolio_tracker.api_clients.get(exchange)
        if not api_client:
            self.logger.warning(f"PRS._reconcile_exchange: No API client for {exchange}, skipping.")
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

            self.logger.debug(
                f"PRS._reconcile_exchange: API map for {exchange} has {len(api_positions_map)} symbols. Local map has {len(local_positions_map)} symbols."
            )

            # Call the method that takes maps and iterates symbols
            # This method returns a dict similar to overall_results in its own scope.
            current_exchange_results = await self.reconcile_positions(
                exchange, api_positions_map, local_positions_map
            )

            # Auto-correct if enabled and discrepancies found (already handled in reconcile_positions)
            # The _apply_corrections call was moved into reconcile_positions for clarity
            # No need for explicit _record_discrepancy here, as reconcile_positions and _reconcile_symbol handle it.
            self.logger.info(
                f"PRS._reconcile_exchange: Finished reconcile_positions for {exchange}. Success: {current_exchange_results.get('success')}, Has Discrepancies: {current_exchange_results.get('has_discrepancies')}"
            )
            return current_exchange_results

        except Exception as e:
            self.logger.exception(
                f"PRS_RECONCILE_EXCHANGE_ERROR: Unhandled exception during position reconciliation for {exchange}: {e}"
            )
            # Create a result dictionary for the error case
            error_result_for_exchange: dict[str, Any] = {
                "success": False,
                "error": f"Unhandled exception in _reconcile_exchange: {e}",
                "timestamp": now,
                "discrepancies": [],
                "symbols_checked": 0,
                "has_discrepancies": True,  # Mark as having discrepancies due to the error
            }
            # Create a generic HistoricalDiscrepancyRecord for the exchange-level error
            exchange_error_historical_record = self._record_discrepancy(
                exchange_id=exchange,
                symbol="EXCHANGE_WIDE_PROCESSING_ERROR",
                discrepancy_type="reconciliation_error",
                api_val=None,  # No specific API value for a general error
                local_val=None,  # No specific local value
                details=f"Unhandled exception during _reconcile_exchange for {exchange}: {e}",
            )
            error_result_for_exchange["discrepancies"].append(exchange_error_historical_record)
            # self.latest_results should be updated by the caller (check_positions)
            return error_result_for_exchange

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
        self, exchange_id: str, symbol: str, pos_data: DerivativePosition | None
    ) -> ParsedPosition | ErrorDict | None:
        if pos_data is None:
            return None

        # pos_data is confirmed to be DerivativePosition due to the check above.
        # Removed redundant isinstance check for pos_data here.

        try:
            size = pos_data.size  # Assuming pos_data.size is Decimal, not Optional[Decimal]
            entry_price = pos_data.entry_price
            mark_price = pos_data.mark_price
            liquidation_price = pos_data.liquidation_price
            unrealized_pnl = pos_data.unrealized_pnl

            # is_finite() implies it's a Decimal. Type hints on DerivativePosition should ensure these are Decimal or None.
            if not size.is_finite():
                raise ValueError(f"Invalid or non-finite size: {size}")
            if entry_price is not None and not entry_price.is_finite():
                raise ValueError(f"Invalid or non-finite entry_price: {entry_price}")
            if mark_price is not None and not mark_price.is_finite():
                raise ValueError(f"Invalid or non-finite mark_price: {mark_price}")
            if liquidation_price is not None and not liquidation_price.is_finite():
                raise ValueError(f"Invalid or non-finite liquidation_price: {liquidation_price}")
            if unrealized_pnl is not None and not unrealized_pnl.is_finite():
                raise ValueError(f"Invalid or non-finite unrealized_pnl: {unrealized_pnl}")

            return cast(
                ParsedPosition,
                {
                    "side": pos_data.side,
                    "size": size,
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "liquidation_price": liquidation_price,
                    "unrealized_pnl": unrealized_pnl,
                },
            )
        except (AttributeError, TypeError, ValueError, InvalidOperation) as e:
            self.logger.error(
                f"PRS_PARSE_LOCAL_ATTR_ERROR: Error extracting attributes from local DerivativePosition for {exchange_id}/{symbol}: {e}. Position: {pos_data}",
                exc_info=True,
            )
            return cast(
                ErrorDict,
                {
                    "error": "local_parsing_error",
                    "message": f"Attribute error parsing local DerivativePosition for {symbol} on {exchange_id}: {e}",
                    "raw_data": str(pos_data),
                },
            )

    def _parse_api_position(
        self, exchange_id: str, symbol: str, pos_data: DerivativePosition | None
    ) -> ParsedPosition | ErrorDict:
        if pos_data is None:
            return cast(
                ParsedPosition,
                {
                    "side": OrderSide.BUY,  # Default, actual side for flat is often irrelevant
                    "size": Decimal("0"),
                    "entry_price": None,
                    "mark_price": None,
                    "liquidation_price": None,
                    "unrealized_pnl": None,
                },
            )

        # pos_data is confirmed to be DerivativePosition here due to the None check above.
        # Removed redundant isinstance check.
        # if not isinstance(pos_data, DerivativePosition):
        #     self.logger.warning(
        #         f"PRS_PARSE_API_UNEXPECTED_TYPE: API position for {exchange_id}/{symbol} "
        #         f"is not DerivativePosition type: {type(pos_data)}. Raw: {pos_data}"
        #     )
        #     return cast(ErrorDict, {
        #         "error": "api_parsing_error",
        #         "message": (
        #             f"Unexpected data type {type(pos_data)} from API for {symbol} "
        #             f"on {exchange_id}. Expected DerivativePosition."
        #         ),
        #         "raw_data": str(pos_data),
        #     })

        try:
            size = pos_data.size  # Assuming pos_data.size is Decimal, not Optional[Decimal]
            entry_price = pos_data.entry_price  # Can be None for flat positions
            mark_price = pos_data.mark_price
            liquidation_price = pos_data.liquidation_price
            unrealized_pnl = pos_data.unrealized_pnl

            # is_finite() implies it's a Decimal. Type hints on DerivativePosition should ensure these are Decimal or None.
            if not size.is_finite():
                raise ValueError(f"Invalid or non-finite size: {size}")
            if entry_price is not None and not entry_price.is_finite():
                raise ValueError(f"Invalid or non-finite entry_price: {entry_price}")
            if mark_price is not None and not mark_price.is_finite():
                raise ValueError(f"Invalid or non-finite mark_price: {mark_price}")
            if liquidation_price is not None and not liquidation_price.is_finite():
                raise ValueError(f"Invalid or non-finite liquidation_price: {liquidation_price}")
            if unrealized_pnl is not None and not unrealized_pnl.is_finite():
                raise ValueError(f"Invalid or non-finite unrealized_pnl: {unrealized_pnl}")

            return cast(
                ParsedPosition,
                {
                    "side": pos_data.side,
                    "size": size,
                    "entry_price": entry_price,
                    "mark_price": mark_price,
                    "liquidation_price": liquidation_price,
                    "unrealized_pnl": unrealized_pnl,
                },
            )
        except (AttributeError, TypeError, ValueError, InvalidOperation) as e:
            self.logger.error(
                f"PRS_PARSE_API_ATTR_ERROR: Error extracting attributes from API DerivativePosition for {exchange_id}/{symbol}: {e}. Position: {pos_data}",
                exc_info=True,
            )
            return cast(
                ErrorDict,
                {
                    "error": "api_parsing_error",
                    "message": f"Attribute error parsing API DerivativePosition for {symbol} on {exchange_id}: {e}",
                    "raw_data": str(pos_data),
                },
            )

    async def reconcile_positions(
        self, exchange_id: str, api_positions: dict[str, Any], local_positions: dict[str, Any]
    ) -> dict[str, Any]:
        """Reconcile positions for a given exchange."""
        now = datetime.now(UTC)
        self.logger.info(
            f"Starting reconcile_positions for {exchange_id} with {len(api_positions)} API and {len(local_positions)} local positions."
        )

        overall_results: dict[str, Any] = {
            "success": True,
            "timestamp": now,
            "discrepancies": [],
            "symbols_checked": 0,
            "has_discrepancies": False,
            "exchange_results": {},  # Store individual results
        }

        reconciliation_tasks: list[
            Awaitable[list[HistoricalDiscrepancyRecord]]
        ] = []  # Corrected return type of _reconcile_symbol

        all_symbols = set(api_positions.keys()) | set(local_positions.keys())
        self.logger.debug(
            f"PRS.reconcile_positions: Reconciling symbols for {exchange_id}: {all_symbols}"
        )

        for symbol_key in all_symbols:
            api_pos_raw = api_positions.get(symbol_key)
            local_pos_raw = local_positions.get(symbol_key)

            # Ensure local_pos_raw is DerivativePosition | None for _parse_local_position
            parsed_local_pos_input: DerivativePosition | None = None
            if isinstance(local_pos_raw, DerivativePosition):
                parsed_local_pos_input = local_pos_raw
            elif local_pos_raw is not None:
                self.logger.warning(
                    f"PRS.reconcile_positions: Unexpected type for local_pos_raw for {symbol_key}: {type(local_pos_raw)}. Treating as None."
                )

            parsed_api_pos = self._parse_api_position(
                exchange_id, symbol_key, cast(DerivativePosition | None, api_pos_raw)
            )
            parsed_local_pos = self._parse_local_position(
                exchange_id, symbol_key, parsed_local_pos_input
            )

            # Type-safe check for errors in parsed_api_pos and parsed_local_pos for logging
            api_pos_has_error = (
                "error" in parsed_api_pos
            )  # simplified: ParsedPosition | ErrorDict is always a dict
            local_pos_has_error = (
                isinstance(parsed_local_pos, dict) and "error" in parsed_local_pos
            )  # parsed_local_pos can be None

            self.logger.debug(
                f"PRS.reconcile_positions: Adding task for {exchange_id}/{symbol_key}. API Parsed Successfully: {not api_pos_has_error}, Local Parsed Successfully: {parsed_local_pos is not None and not local_pos_has_error}"
            )

            reconciliation_tasks.append(
                self._reconcile_symbol(
                    exchange_id,
                    symbol_key,
                    parsed_api_pos,
                    parsed_local_pos,
                )
            )

        if not reconciliation_tasks:
            logger.info(f"No symbols to reconcile for {exchange_id}.")
            return overall_results  # Return early if no tasks

        symbol_results_list = await asyncio.gather(*reconciliation_tasks, return_exceptions=True)

        # Aggregate results for the current exchange_id
        aggregated_discrepancies: list[HistoricalDiscrepancyRecord] = []

        for res_item in symbol_results_list:
            if isinstance(res_item, Exception):
                self.logger.error(
                    f"PRS.reconcile_positions: Exception during symbol reconciliation for {exchange_id}: {res_item}",
                    exc_info=res_item,
                )
                overall_results["success"] = False
                # Create a HistoricalDiscrepancyRecord for the error
                # For _record_discrepancy, we need specific fields.
                # We'll create a generic one here for now.
                # This part might need refinement on how to get symbol or more details.
                error_record = self._record_discrepancy(
                    exchange_id=exchange_id,
                    symbol="UNKNOWN_SYMBOL_DUE_TO_EXCEPTION",
                    discrepancy_type="reconciliation_error",
                    api_val=None,
                    local_val=None,
                    details=f"Gather exception: {str(res_item)}",
                )
                aggregated_discrepancies.append(error_record)
                overall_results["has_discrepancies"] = True
                continue

            if not isinstance(
                res_item, list
            ):  # _reconcile_symbol returns list[HistoricalDiscrepancyRecord]
                self.logger.error(
                    f"PRS.reconcile_positions: Unexpected result type in symbol_results_list for {exchange_id}: {type(res_item)}. Item: {res_item}"
                )
                overall_results["success"] = False
                error_record = self._record_discrepancy(
                    exchange_id=exchange_id,
                    symbol="UNKNOWN_SYMBOL_DUE_TO_BAD_RESULT_TYPE",
                    discrepancy_type="reconciliation_error",
                    api_val=None,
                    local_val=None,
                    details=f"Unexpected result type from _reconcile_symbol: {type(res_item)}",
                )
                aggregated_discrepancies.append(error_record)
                overall_results["has_discrepancies"] = True
                continue

            # res_item is list[HistoricalDiscrepancyRecord]
            aggregated_discrepancies.extend(res_item)
            if res_item:  # If the list is not empty, there were discrepancies for this symbol
                overall_results["has_discrepancies"] = True

        overall_results["discrepancies"] = aggregated_discrepancies
        overall_results["symbols_checked"] = len(all_symbols)

        # Record discrepancies for the current exchange_id if any were found
        # The _record_discrepancy method is for individual discrepancies.
        # The historical record is already built in aggregated_discrepancies.
        # We might want a different method to log/process overall_results.
        # For now, self.discrepancy_history is extended by _record_discrepancy calls within _reconcile_symbol.
        # The line below calling self._record_discrepancy with overall_results is incorrect as per its signature.
        # if overall_results["has_discrepancies"]:
        #    self._record_discrepancy(exchange_id, overall_results) # This line is problematic, removing.

        # Auto-correct if enabled
        if self.auto_correct and overall_results["has_discrepancies"]:
            self.logger.info(
                f"PRS.reconcile_positions: Auto-correction check for {exchange_id}. Discrepancies found: {overall_results['has_discrepancies']}"
            )
            # _apply_corrections expects api_positions_map: dict[str, DerivativePosition]
            # The current api_positions is dict[str, Any]. We need to ensure it contains DerivativePosition
            # or adapt _apply_corrections or how api_positions_map is constructed.
            # For now, this might be an issue if api_positions doesn't hold DerivativePosition objects.

            # Rebuild api_positions_map for _apply_corrections, ensuring values are DerivativePosition
            api_positions_map_for_correction: dict[str, DerivativePosition] = {}
            for sym, pos_data in api_positions.items():
                if isinstance(pos_data, DerivativePosition):
                    api_positions_map_for_correction[sym] = pos_data
                elif pos_data is not None:  # Log if it's something else but not None
                    self.logger.warning(
                        f"PRS.reconcile_positions: Item '{sym}' in api_positions is not a DerivativePosition for correction: {type(pos_data)}"
                    )
            # DEFENSIVE CHECK: exchange_value_str at line 325 (original 331) might be flagged by linter as possibly unbound
            # due to control flow with 'continue' in try-except. Logic is sound.
            if (
                api_positions_map_for_correction
            ):  # Only call if we have valid positions for correction base
                self._apply_corrections(
                    exchange_id, overall_results, api_positions_map_for_correction
                )
            else:
                self.logger.warning(
                    f"PRS.reconcile_positions: Cannot apply corrections for {exchange_id} as no valid DerivativePosition found in api_positions input."
                )

        self.latest_results[exchange_id] = overall_results  # Store per-exchange results
        self.logger.info(
            f"Finished reconcile_positions for {exchange_id}. Success: {overall_results['success']}, Discrepancies found: {overall_results['has_discrepancies']}, Symbols checked: {overall_results['symbols_checked']}"
        )
        return overall_results

    async def _reconcile_symbol(
        self,
        exchange_id: str,
        symbol: str,
        parsed_api_pos: ParsedPosition | ErrorDict,
        parsed_local_pos: ParsedPosition | ErrorDict | None,
    ) -> list[HistoricalDiscrepancyRecord]:
        discrepancy_records: list[HistoricalDiscrepancyRecord] = []

        # --- Handle Parsing Errors First ---
        # Robust check if parsed_api_pos is an ErrorDict
        if isinstance(parsed_api_pos, dict) and "error" in parsed_api_pos:
            error_dict_api = cast(ErrorDict, parsed_api_pos)
            record = self._record_discrepancy(
                exchange_id=exchange_id,
                symbol=symbol,
                discrepancy_type="api_parsing_error",
                api_val=str(error_dict_api.get("raw_data", "N/A")),
                local_val=None,  # Local value is not relevant to API parsing error
                details=str(error_dict_api.get("message", "Unknown API parsing error.")),
            )
            discrepancy_records.append(record)
            return discrepancy_records  # Stop further checks for this symbol if API data is bad

        # Similar robust check for parsed_local_pos
        if isinstance(parsed_local_pos, dict) and "error" in parsed_local_pos:
            error_dict_local = cast(ErrorDict, parsed_local_pos)
            record = self._record_discrepancy(
                exchange_id=exchange_id,
                symbol=symbol,
                discrepancy_type="local_parsing_error",
                api_val=None,  # API data might be fine, local is the issue
                local_val=str(error_dict_local.get("raw_data", "N/A")),
                details=str(error_dict_local.get("message", "Unknown local parsing error.")),
            )
            discrepancy_records.append(record)
            return discrepancy_records  # Stop further checks if local parsing failed.

        # At this point, parsed_api_pos should be ParsedPosition (a dict, and not an ErrorDict).
        # If it's NOT a dict, then _parse_api_position has a bug or was bypassed.
        if not isinstance(parsed_api_pos, dict):
            self.logger.error(
                f"PRS._reconcile_symbol: CRITICAL: parsed_api_pos is not a dict for {exchange_id}/{symbol}. "
                f"Type: {type(parsed_api_pos)}. Value: {parsed_api_pos!r}. This indicates a problem with _parse_api_position."
            )
            record = self._record_discrepancy(
                exchange_id=exchange_id,
                symbol=symbol,
                discrepancy_type="reconciliation_error",  # Or a new specific type like "internal_parsing_failure"
                api_val=str(parsed_api_pos),  # Log the problematic value
                local_val=None,
                details=(
                    f"Internal error: API position data was not a dictionary after parsing. "
                    f"Type: {type(parsed_api_pos)}. Expected ParsedPosition dict."
                ),
            )
            discrepancy_records.append(record)
            return discrepancy_records

        # Now, parsed_api_pos is confirmed to be a dict, and not an ErrorDict. So it must be ParsedPosition.
        api_pos_data = cast(ParsedPosition, parsed_api_pos)

        # parsed_local_pos is ParsedPosition | None (and not ErrorDict).
        # It can be None if not tracked, or ParsedPosition if tracked and parsed successfully.
        local_pos_data: ParsedPosition | None
        if parsed_local_pos is None:
            local_pos_data = None
        elif isinstance(
            parsed_local_pos, dict
        ):  # Should always be true if not None and no error previously
            local_pos_data = cast(ParsedPosition, parsed_local_pos)
        else:
            # This case should ideally not be reached if prior checks are exhaustive
            self.logger.error(
                f"PRS._reconcile_symbol: CRITICAL: parsed_local_pos is unexpected type for {exchange_id}/{symbol}. "
                f"Type: {type(parsed_local_pos)}. Value: {parsed_local_pos!r}."
            )
            record = self._record_discrepancy(
                exchange_id=exchange_id,
                symbol=symbol,
                discrepancy_type="reconciliation_error",
                api_val=api_pos_data.get("size", "N/A"),  # api_pos_data should be valid here
                local_val=str(parsed_local_pos),
                details=(
                    f"Internal error: Local position data has unexpected type after parsing. "
                    f"Type: {type(parsed_local_pos)}."
                ),
            )
            discrepancy_records.append(record)
            return discrepancy_records

        # --- Core Discrepancy Logic ---

        # Case 1: Position exists on API but not in Tracker (local_pos_data is None)
        if local_pos_data is None:
            # If API also reports flat (size 0), it's not a discrepancy of "missing" but rather both are flat.
            if api_pos_data["size"] != Decimal("0"):
                record = self._record_discrepancy(
                    exchange_id=exchange_id,
                    symbol=symbol,
                    discrepancy_type="size",  # Changed from "unknown_local_symbol" to "size"
                    api_val=api_pos_data["size"],  # Report API size
                    local_val=Decimal("0"),  # Explicitly state local is zero for size comparison
                    details=f"Position for {symbol} found on {exchange_id} (size {api_pos_data['size']}) but not in PortfolioTracker (or size 0).",
                )
                discrepancy_records.append(record)
            # If both api_pos_data["size"] == 0 and local_pos_data is None, they agree (both flat/absent).
            return discrepancy_records  # No further comparison needed if local is None

        # Case 2: Position exists in Tracker but not on API (API reports flat size 0)
        # api_pos_data here is guaranteed to be a ParsedPosition (not an error dict)
        if api_pos_data["size"] == Decimal("0"):
            # local_pos_data is NOT None here because Case 1 would have caught it (function would have returned).
            # So local_pos_data is ParsedPosition.
            if local_pos_data["size"] != Decimal("0"):
                record = self._record_discrepancy(
                    exchange_id=exchange_id,
                    symbol=symbol,
                    discrepancy_type="size",  # Changed from "unknown_api_symbol" to "size"
                    api_val=Decimal("0"),  # Explicitly state API is zero for size comparison
                    local_val=local_pos_data["size"],  # Report local size
                    details=f"Position for {symbol} in PortfolioTracker (size {local_pos_data['size']}) but flat or not found on {exchange_id}.",
                )
                discrepancy_records.append(record)
            return discrepancy_records  # No further comparison if API is flat.

        # Case 3: Position exists in both API and Tracker, and both are non-flat. Compare attributes.
        # If we reach here:
        # - local_pos_data is ParsedPosition (not None, due to Case 1 returning if it was None).
        # - api_pos_data is ParsedPosition and api_pos_data["size"] != Decimal("0") (due to Case 2 returning if it was 0).
        # Thus, both api_pos_data and local_pos_data are valid ParsedPosition objects representing non-flat positions.

        # The `if local_pos_data is None:` block previously here for Case 3 was redundant and has been removed.

        # Size comparison
        if api_pos_data["size"] != local_pos_data["size"]:
            record = self._record_discrepancy(
                exchange_id=exchange_id,
                symbol=symbol,
                discrepancy_type="size",
                api_val=api_pos_data["size"],
                local_val=local_pos_data["size"],
            )
            discrepancy_records.append(record)

        # Entry Price Comparison (only if both sides are same and sizes are non-zero and match)
        # Price comparison is complex if sides/sizes differ. Simplification: only if size/side match.
        if (
            api_pos_data["side"] == local_pos_data["side"]
            and api_pos_data["size"]
            != Decimal("0")  # Ensure not comparing prices for flat positions
            and api_pos_data["size"]
            == local_pos_data["size"]  # Only compare price if size is already aligned
        ):
            if api_pos_data["entry_price"] != local_pos_data["entry_price"]:
                # Handle None for entry_price carefully if that's valid for non-flat positions
                record = self._record_discrepancy(
                    exchange_id=exchange_id,
                    symbol=symbol,
                    discrepancy_type="entry_price",
                    api_val=api_pos_data["entry_price"],
                    local_val=local_pos_data["entry_price"],
                )
                discrepancy_records.append(record)

        # Optional: Compare mark_price, liquidation_price, unrealized_pnl if available and makes sense
        # For example, mark_price:
        # local_pos_data is guaranteed to be ParsedPosition here, so `local_pos_data and ...` check is redundant.
        if api_pos_data.get("mark_price") != local_pos_data.get("mark_price") and api_pos_data[
            "size"
        ] != Decimal("0"):  # Only if not flat
            record = self._record_discrepancy(
                exchange_id=exchange_id,
                symbol=symbol,
                discrepancy_type="mark_price",
                api_val=api_pos_data.get("mark_price"),
                local_val=local_pos_data.get("mark_price"),
            )
            discrepancy_records.append(record)

        return discrepancy_records

    async def _compare_positions(
        self, api_positions: dict[str, Any], local_positions: dict[str, Any]
    ) -> dict[str, Any]:
        """Compare positions between API and local state."""
        now = datetime.now(UTC)
        exchange_id_placeholder = (
            "UNKNOWN_EXCHANGE_IN_COMPARE"  # Placeholder for this generic method
        )

        overall_results: dict[str, Any] = {
            "success": True,
            "timestamp": now,
            "discrepancies": [],
            "symbols_checked": 0,
            "has_discrepancies": False,
            # "exchange_results": {}, # Not directly applicable here or needs careful thought
        }

        reconciliation_tasks: list[Awaitable[list[HistoricalDiscrepancyRecord]]] = []

        all_symbols = set(api_positions.keys()) | set(local_positions.keys())
        if not all_symbols:
            self.logger.info("PRS._compare_positions: No symbols to compare.")
            return overall_results

        for symbol_key in all_symbols:
            api_pos_raw = api_positions.get(symbol_key)
            local_pos_raw = local_positions.get(symbol_key)

            # Ensure local_pos_raw is DerivativePosition | None for _parse_local_position
            parsed_local_pos_input: DerivativePosition | None = None
            if isinstance(local_pos_raw, DerivativePosition):
                parsed_local_pos_input = local_pos_raw
            elif local_pos_raw is not None:
                self.logger.warning(
                    f"PRS._compare_positions: Unexpected type for local_pos_raw for {symbol_key}: {type(local_pos_raw)}. Treating as None."
                )

            parsed_api = self._parse_api_position(
                exchange_id_placeholder, symbol_key, cast(DerivativePosition | None, api_pos_raw)
            )
            # Assuming local_pos_raw is DerivativePosition | None for _parse_local_position
            # If local_positions can contain other types, this cast might be problematic
            parsed_local = self._parse_local_position(
                exchange_id_placeholder, symbol_key, parsed_local_pos_input
            )

            reconciliation_tasks.append(
                self._reconcile_symbol(
                    exchange_id_placeholder,
                    symbol_key,
                    parsed_api,
                    parsed_local,
                )
            )

        results_gather = await asyncio.gather(*reconciliation_tasks, return_exceptions=True)

        aggregated_discrepancies: list[HistoricalDiscrepancyRecord] = []
        for res_item in results_gather:
            if isinstance(res_item, Exception):
                self.logger.error(
                    f"PRS._compare_positions: Error during symbol reconciliation task: {res_item}",
                    exc_info=res_item,
                )
                overall_results["success"] = False
                # Create a historical record for the exception
                error_record = self._record_discrepancy(
                    exchange_id=exchange_id_placeholder,
                    symbol="UNKNOWN_SYMBOL_DUE_TO_COMPARE_EXCEPTION",
                    discrepancy_type="reconciliation_error",
                    api_val=None,
                    local_val=None,
                    details=f"Gather exception in _compare_positions: {str(res_item)}",
                )
                aggregated_discrepancies.append(error_record)
                overall_results["has_discrepancies"] = True
                continue

            if not isinstance(
                res_item, list
            ):  # _reconcile_symbol returns list[HistoricalDiscrepancyRecord]
                self.logger.error(
                    f"PRS._compare_positions: Unexpected result type from _reconcile_symbol: {type(res_item)}. Item: {res_item}"
                )
                overall_results["success"] = False
                error_record = self._record_discrepancy(
                    exchange_id=exchange_id_placeholder,
                    symbol="UNKNOWN_SYMBOL_DUE_TO_COMPARE_BAD_RESULT",
                    discrepancy_type="reconciliation_error",
                    api_val=None,
                    local_val=None,
                    details=f"Unexpected result type from _reconcile_symbol in _compare_positions: {type(res_item)}",
                )
                aggregated_discrepancies.append(error_record)
                overall_results["has_discrepancies"] = True
                continue

            # res_item is list[HistoricalDiscrepancyRecord]
            aggregated_discrepancies.extend(res_item)
            if res_item:  # If the list is not empty, there were discrepancies for this symbol
                overall_results["has_discrepancies"] = True

        overall_results["discrepancies"] = aggregated_discrepancies
        overall_results["symbols_checked"] = len(all_symbols)

        # The original logic for recording/correcting is not directly applicable here as _compare_positions
        # is more of a utility. Discrepancies are collected in overall_results["discrepancies"].
        if overall_results["has_discrepancies"]:
            self.logger.warning(
                f"PRS._compare_positions: Discrepancies found: {overall_results['discrepancies']}"
            )

        # self.latest_results should probably not be updated by this generic comparison method.
        # It's more specific to the main reconciliation flow.
        return overall_results
