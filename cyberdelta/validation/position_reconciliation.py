"""Position Reconciliation System for the CyberDeltaEngine.

This module provides validation between various position tracking systems to ensure consistency.
"""

import asyncio
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, getcontext
from typing import Any, Literal, cast

from cyberdelta.apis.base.exchange_api import ExchangeAPI  # Add ExchangeAPI
from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import DerivativePosition, OrderSide
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.validation.models.discrepancy_detail import (
    DiscrepancyDetail,
    HistoricalDiscrepancyRecord,
)


logger = get_logger(__name__)

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
    """Validates and reconciles positions between different sources.

    This system compares positions from:
    1. Exchange API-reported positions
    2. Fill history-derived positions
    3. Local state tracking

    The system detects discrepancies, provides alerts, and optionally corrects the local state
    to match the authoritative source.
    """

    def __init__(self, app_settings: AppSettings, portfolio_tracker: PortfolioTracker) -> None:
        """Initialize the position reconciliation system.

        Args:
            app_settings: Application configuration
            portfolio_tracker: Reference to the portfolio tracker (optional, can be set later)

        """
        self.logger = logger
        self._config = app_settings
        self._portfolio_tracker = portfolio_tracker
        self.latest_results: dict[str, dict[str, Any | list[DiscrepancyDetail]]] = {}

        # Configuration parameters from AppSettings
        pos_recon_config = app_settings.safety_systems.position_reconciliation
        self.reconciliation_threshold = float(pos_recon_config.max_discrepancy_pct)
        # TODO: Add auto_correct configuration to PositionReconciliationSettings when needed
        self.auto_correct = False  # Default value
        self.check_interval: timedelta = timedelta(seconds=pos_recon_config.check_interval_sec)

        # Track the last reconciliation time
        self.last_check_time: datetime | None = None

        # Record of discrepancies found
        self.discrepancy_history: list[HistoricalDiscrepancyRecord] = []

        # Initialize last run time to a long time ago
        getcontext().prec = 50  # Set precision for Decimal operations

        # Internal state
        self._last_reconciliation_run: datetime = datetime.min.replace(tzinfo=UTC)

        # Get interval from configuration
        self._reconciliation_interval_secs: float = float(pos_recon_config.check_interval_sec)

        # Initialize next_reconciliation_time
        self._next_reconciliation_time: datetime = datetime.now(UTC) + timedelta(
            seconds=self._reconciliation_interval_secs,
        )

        # Get threshold from configuration
        self._discrepancy_threshold_percent: Decimal = pos_recon_config.max_discrepancy_pct

        # TODO: Add action_mode configuration to PositionReconciliationSettings when needed
        self._action_mode: str = "log"  # Default value

        # Ensure check_interval is timedelta
        check_interval_td = timedelta(seconds=pos_recon_config.check_interval_sec)
        self.check_interval = check_interval_td

        # Set reconciliation_interval as timedelta
        self.reconciliation_interval = timedelta(seconds=self._reconciliation_interval_secs)

    def register_portfolio_tracker(self, portfolio_tracker: PortfolioTracker) -> None:
        """Register the portfolio tracker instance.

        Args:
            portfolio_tracker: The portfolio tracker instance

        """
        self._portfolio_tracker = portfolio_tracker

    async def check_positions(self, force: bool = False) -> dict[str, dict[str, Any]]:
        """Check positions if interval has passed or force=True.

        Returns a dictionary mapping exchange ID to reconciliation results.

        Args:
            force: Whether to force reconciliation regardless of interval.

        Returns:
            Dictionary mapping exchange IDs to reconciliation results.

        """
        now = datetime.now(UTC)

        interval = self.reconciliation_interval
        # Runtime check for interval type - this should always be timedelta due to __init__
        # if not isinstance(interval, timedelta):

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
        api_clients: dict[str, ExchangeAPI] = cast("dict[str, ExchangeAPI]", api_clients_any)

        self.last_check_time = now  # Update last check time *before* starting

        tasks: dict[str, Awaitable[dict[str, Any]]] = {}
        # exchange_id_str is already known to be str from api_clients type hint
        for exchange_id_str in api_clients:
            tasks[exchange_id_str] = self._reconcile_exchange(exchange_id_str)

        # Run reconciliation for all exchanges concurrently
        results_any_list: list[Any] = await asyncio.gather(*tasks.values(), return_exceptions=True)

        results_dict: dict[str, Any] = {}
        exchange_keys: list[str] = list(tasks.keys())
        for i, task_result_any in enumerate(results_any_list):
            exchange_name: str = exchange_keys[i]
            if isinstance(task_result_any, Exception):
                logger.error(
                    "reconciliation_task_failed",
                    action="reconcile",
                    exchange_name=exchange_name,
                    error=str(task_result_any),
                    message=f"Reconciliation task for {exchange_name} failed: {task_result_any}",
                )
                results_dict[exchange_name] = {
                    "success": False,
                    "error": str(task_result_any),
                    "timestamp": now,
                    "discrepancies": [],  # Expected list[DiscrepancyDetail]
                }
            elif isinstance(task_result_any, dict):
                results_dict[exchange_name] = cast("dict[str, Any]", task_result_any)
            else:
                logger.error(
                    "unexpected_reconciliation_result",
                    action="reconcile",
                    exchange_name=exchange_name,
                    result_type=type(task_result_any).__name__,
                    message=(
                        f"Unexpected result type from gather for {exchange_name}: "
                        f"{type(task_result_any)}"
                    ),
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
        """Create and log a DiscrepancyDetail, then record it historically.

        Helper method to create a discrepancy record and add it to the historical log.

        Args:
            exchange_id: Exchange identifier where discrepancy occurred.
            symbol: Trading symbol with the discrepancy.
            discrepancy_type: Type of discrepancy detected.
            api_val: Value from the exchange API.
            local_val: Value from local tracking.
            details: Additional details about the discrepancy.

        Returns:
            The created HistoricalDiscrepancyRecord.

        """
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
            "position_discrepancy_recorded",
            action="record",
            exchange_id=historical_record.exchange_id,
            symbol=historical_record.detail.symbol,
            discrepancy_type=historical_record.detail.discrepancy_type,
            exchange_value=historical_record.detail.exchange_value,
            local_value=historical_record.detail.local_value,
            details=historical_record.detail.details or "N/A",
            message=(
                f"Position discrepancy recorded: {historical_record.exchange_id} "
                f"{historical_record.detail.symbol} Type: "
                f"{historical_record.detail.discrepancy_type} "
                f"[Exchange Value: {historical_record.detail.exchange_value}, "
                f"Local Value: {historical_record.detail.local_value}] "
                f"Details: {historical_record.detail.details or 'N/A'}"
            ),
        )

        return historical_record

    def _apply_corrections(
        self,
        exchange: str,
        results: dict[str, Any],
        api_positions_map: dict[str, DerivativePosition],  # Pass the map for lookups
    ) -> None:
        """Apply corrections to the portfolio tracker based on reconciliation results.

        Args:
            exchange: Exchange identifier.
            results: Reconciliation results containing discrepancies.
            api_positions_map: A map of symbol to API DerivativePosition for the current exchange.

        """
        if not self._portfolio_tracker:
            logger.error("Cannot apply corrections: Portfolio tracker not registered")
            return

        # Validate and get discrepancy details
        discrepancy_details_list = self._get_discrepancy_details(results)
        if discrepancy_details_list is None:
            return

        # Process each discrepancy
        for discrepancy_detail in discrepancy_details_list:
            self._process_single_discrepancy(
                exchange,
                discrepancy_detail,
                api_positions_map,
                results,
            )

    def _get_discrepancy_details(self, results: dict[str, Any]) -> list[DiscrepancyDetail] | None:
        """Extract and validate discrepancy details from results."""
        discrepancy_details_list_any = results.get("discrepancies", [])
        if not isinstance(discrepancy_details_list_any, list):
            logger.error(
                "apply_corrections_invalid_discrepancies_type",
                expected_type="list",
                actual_type=str(type(discrepancy_details_list_any)),
                message=(
                    f"_apply_corrections: results['discrepancies'] is not a list. "
                    f"Got: {type(discrepancy_details_list_any)}"
                ),
            )
            return None

        return cast("list[DiscrepancyDetail]", discrepancy_details_list_any)

    def _process_single_discrepancy(
        self,
        exchange: str,
        discrepancy_detail: DiscrepancyDetail,
        api_positions_map: dict[str, DerivativePosition],
        results: dict[str, Any],
    ) -> None:
        """Process a single discrepancy and apply corrections."""
        # We are primarily correcting size discrepancies here.
        if discrepancy_detail.discrepancy_type != "size":
            logger.debug(
                "skipping_non_size_correction",
                action="correct",
                discrepancy_type=discrepancy_detail.discrepancy_type,
                symbol=discrepancy_detail.symbol,
                message=(
                    f"Skipping correction for non-size discrepancy type: "
                    f"{discrepancy_detail.discrepancy_type} for symbol {discrepancy_detail.symbol}"
                ),
            )
            return

        symbol = discrepancy_detail.symbol
        exchange_value = self._parse_exchange_value(discrepancy_detail, symbol)
        if exchange_value is None:
            return

        # Apply the position correction
        self._apply_position_correction(exchange, symbol, exchange_value, api_positions_map)

        # Mark as corrected in history
        self._mark_discrepancy_corrected(exchange, symbol, discrepancy_detail, results)

    def _parse_exchange_value(
        self,
        discrepancy_detail: DiscrepancyDetail,
        symbol: str,
    ) -> Decimal | None:
        """Parse exchange value from discrepancy detail."""
        exchange_value_str = discrepancy_detail.exchange_value
        if exchange_value_str is None:
            logger.warning(
                "missing_exchange_value",
                action="parse",
                symbol=symbol,
                message=(
                    f"Skipping discrepancy for {symbol} due to missing 'exchange_value' "
                    f"in DiscrepancyDetail"
                ),
            )
            return None

        try:
            return Decimal(exchange_value_str)
        except InvalidOperation:
            logger.warning(
                "exchange_value_parse_failed",
                action="parse",
                symbol=symbol,
                exchange_value_str=exchange_value_str,
                message=(
                    f"Could not parse 'exchange_value' from DiscrepancyDetail for {symbol} "
                    f"as Decimal: {exchange_value_str}"
                ),
            )
            return None

    def _apply_position_correction(
        self,
        exchange: str,
        symbol: str,
        exchange_value: Decimal,
        api_positions_map: dict[str, DerivativePosition],
    ) -> None:
        """Apply position correction based on exchange value."""
        current_local_position = self._portfolio_tracker.get_position(exchange, symbol)
        api_position_for_correction = api_positions_map.get(symbol)

        timestamp_to_use = self._get_correction_timestamp(
            api_position_for_correction,
            current_local_position,
        )

        if current_local_position is None:
            self._create_missing_position(
                exchange,
                symbol,
                exchange_value,
                api_position_for_correction,
                timestamp_to_use,
            )
        elif current_local_position.size != exchange_value:
            self._update_existing_position(
                exchange,
                symbol,
                exchange_value,
                current_local_position,
                api_position_for_correction,
                timestamp_to_use,
            )
        else:
            logger.info(
                "position_already_matches_exchange",
                exchange=exchange,
                symbol=symbol,
                exchange_value=float(exchange_value),
                message=(
                    f"Position for {exchange}/{symbol} already matches exchange value "
                    f"{exchange_value}. No size correction needed based on this discrepancy."
                ),
            )

    def _get_correction_timestamp(
        self,
        api_position: DerivativePosition | None,
        local_position: DerivativePosition | None,
    ) -> datetime:
        """Get appropriate timestamp for correction."""
        if api_position:
            return api_position.timestamp
        if local_position:
            return local_position.timestamp
        return datetime.now(UTC)

    def _create_missing_position(
        self,
        exchange: str,
        symbol: str,
        exchange_value: Decimal,
        api_position: DerivativePosition | None,
        timestamp: datetime,
    ) -> None:
        """Create a missing local position."""
        if exchange_value == Decimal(0):
            return  # Don't create zero-size positions

        logger.info(
            "creating_missing_local_position",
            exchange=exchange,
            symbol=symbol,
            size=float(exchange_value),
            message=f"Creating missing local position: {exchange} {symbol} size={exchange_value}",
        )

        if api_position:
            new_local_pos = self._create_position_from_api_data(
                exchange,
                symbol,
                exchange_value,
                api_position,
            )
        else:
            new_local_pos = self._create_minimal_position(
                exchange,
                symbol,
                exchange_value,
                timestamp,
            )

        self._portfolio_tracker.update_position(exchange, new_local_pos)

    def _create_position_from_api_data(
        self,
        exchange: str,
        symbol: str,
        exchange_value: Decimal,
        api_position: DerivativePosition,
    ) -> DerivativePosition:
        """Create position from full API data."""
        return DerivativePosition(
            exchange=exchange,
            symbol=symbol,
            side=OrderSide.BUY
            if exchange_value > Decimal(0)
            else OrderSide.SELL
            if exchange_value < Decimal(0)
            else api_position.side,
            size=exchange_value,
            entry_price=api_position.entry_price if exchange_value != Decimal(0) else None,
            timestamp=api_position.timestamp,
            mark_price=api_position.mark_price,
            liquidation_price=api_position.liquidation_price,
            unrealized_pnl=api_position.unrealized_pnl,
            realized_pnl=api_position.realized_pnl,
            strategy_name=api_position.strategy_name,
            signal_id=api_position.signal_id,
            hl_details=api_position.hl_details,
            bp_details=api_position.bp_details,
        )

    def _create_minimal_position(
        self,
        exchange: str,
        symbol: str,
        exchange_value: Decimal,
        timestamp: datetime,
    ) -> DerivativePosition:
        """Create minimal position when API data is not available."""
        logger.warning(
            "cannot_create_position_missing_api_data",
            exchange=exchange,
            symbol=symbol,
            exchange_value=float(exchange_value),
            message=(
                f"Cannot create local position for {exchange}/{symbol} - "
                f"missing full API data for discrepancy correction, "
                f"only size {exchange_value} is known."
            ),
        )

        return DerivativePosition(
            exchange=exchange,
            symbol=symbol,
            side=OrderSide.BUY if exchange_value > 0 else OrderSide.SELL,
            size=exchange_value,
            entry_price=None,
            timestamp=timestamp,
        )

    def _update_existing_position(
        self,
        exchange: str,
        symbol: str,
        exchange_value: Decimal,
        current_position: DerivativePosition,
        api_position: DerivativePosition | None,
        timestamp: datetime,
    ) -> None:
        """Update existing position with corrected values."""
        logger.info(
            "correcting_local_position",
            exchange=exchange,
            symbol=symbol,
            old_size=float(current_position.size),
            new_size=float(exchange_value),
            message=(
                f"Correcting local position: {exchange} {symbol} "
                f"from {current_position.size} to {exchange_value}"
            ),
        )

        updated_local_position = current_position.model_copy(
            update={
                "size": exchange_value,
                "side": OrderSide.BUY
                if exchange_value > Decimal(0)
                else OrderSide.SELL
                if exchange_value < Decimal(0)
                else current_position.side,
                "entry_price": current_position.entry_price
                if exchange_value != Decimal(0)
                else None,
                "timestamp": timestamp,
                "unrealized_pnl": api_position.unrealized_pnl
                if api_position
                else current_position.unrealized_pnl,
                "mark_price": api_position.mark_price
                if api_position
                else current_position.mark_price,
                "liquidation_price": api_position.liquidation_price
                if api_position
                else current_position.liquidation_price,
            },
        )
        self._portfolio_tracker.update_position(exchange, updated_local_position)

    def _mark_discrepancy_corrected(
        self,
        exchange: str,
        symbol: str,
        discrepancy_detail: DiscrepancyDetail,
        results: dict[str, Any],
    ) -> None:
        """Mark discrepancy as corrected in historical records."""
        results_timestamp_dt = results.get("timestamp")
        if results_timestamp_dt is None:
            logger.warning(
                "missing_timestamp_in_results",
                exchange=exchange,
                symbol=symbol,
                message=(
                    f"Missing 'timestamp' in results dict during correction for "
                    f"{exchange}/{symbol}, cannot accurately mark historical record."
                ),
            )
            return

        if not isinstance(results_timestamp_dt, datetime):
            logger.warning(
                "invalid_timestamp_type_in_results",
                actual_type=str(type(results_timestamp_dt)),
                expected_type="datetime",
                message=(
                    f"Invalid 'timestamp' type in results dict: "
                    f"{type(results_timestamp_dt)}. Cannot mark historical record."
                ),
            )
            return

        for historical_record in self.discrepancy_history:
            if self._matches_historical_record(
                historical_record,
                exchange,
                symbol,
                discrepancy_detail,
                results_timestamp_dt,
            ):
                historical_record.is_corrected = True
                logger.info(
                    "marked_historical_discrepancy_corrected",
                    exchange=exchange,
                    symbol=symbol,
                    historical_record=historical_record.model_dump(),
                    message=(
                        f"Marked historical discrepancy as corrected: "
                        f"{historical_record.model_dump_json(indent=2)}"
                    ),
                )
                break

    def _matches_historical_record(
        self,
        historical_record: HistoricalDiscrepancyRecord,
        exchange: str,
        symbol: str,
        discrepancy_detail: DiscrepancyDetail,
        results_timestamp_dt: datetime,
    ) -> bool:
        """Check if historical record matches the current discrepancy being corrected."""
        return (
            historical_record.exchange_id == exchange
            and historical_record.detail.symbol == symbol
            and historical_record.recorded_at == results_timestamp_dt
            and historical_record.detail.discrepancy_type == "size"
            and historical_record.detail == discrepancy_detail
            and not historical_record.is_corrected
        )

    def get_discrepancy_history(self, days: int = 7) -> list[HistoricalDiscrepancyRecord]:
        """Get the history of position discrepancies.

        Returns a list of HistoricalDiscrepancyRecord objects, filtered by days.

        Args:
            days: Number of days to include in history.

        Returns:
            List of HistoricalDiscrepancyRecord records within the specified timeframe.

        """
        cutoff_time = datetime.now(UTC) - timedelta(days=days)
        return [r for r in self.discrepancy_history if r.recorded_at >= cutoff_time]

    def get_latest_results(self) -> dict[str, dict[str, Any]]:
        """Get the latest reconciliation results.

        Returns:
            Dictionary of reconciliation results by exchange

        """
        return self.latest_results

    def get_reconciliation_report(self) -> dict[str, Any]:
        """Generate a summary report of position reconciliation."""
        now = datetime.now(UTC)
        # Get historical records for the last 24 hours (1 day)
        recent_historical_records: list[HistoricalDiscrepancyRecord] = self.get_discrepancy_history(
            days=1,
        )

        exchange_stats: dict[str, dict[str, Any]] = {}
        reportable_discrepancies_for_summary: list[dict[str, Any]] = []

        for record in recent_historical_records:
            # Construct a dictionary for the 'recent_discrepancies' part of the report
            discrepancy_summary_item = {
                "timestamp": record.recorded_at.isoformat(),  # Use recorded_at from
                # the historical record
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
                cast("int", exchange_stats[current_exchange_id]["total_discrepancies"]) + 1
            )
            if (
                record.detail.symbol
                not in exchange_stats[current_exchange_id]["symbols_affected_list"]
            ):
                exchange_stats[current_exchange_id]["symbols_affected_list"].append(
                    record.detail.symbol,
                )

            if record.is_corrected:
                exchange_stats[current_exchange_id]["corrected_count"] = (
                    cast("int", exchange_stats[current_exchange_id]["corrected_count"]) + 1
                )
            else:
                exchange_stats[current_exchange_id]["uncorrected_count"] = (
                    cast("int", exchange_stats[current_exchange_id]["uncorrected_count"]) + 1
                )

        # Finalize exchange_stats by calculating symbols_affected_count
        for exchange_data in exchange_stats.values():
            exchange_data["symbols_affected_count"] = len(
                exchange_data.pop("symbols_affected_list"),  # Pop and get length
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
                self.reconciliation_threshold,
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
                "skipping_reconciliation_run",
                time_since_last_run=round(time_since_last_run, 2),
                interval=round(self._reconciliation_interval_secs, 2),
                message=(
                    f"Skipping reconciliation run. Time since last: {time_since_last_run:.2f}s, "
                    f"Interval: {self._reconciliation_interval_secs:.2f}s"
                ),
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
            api_clients_dict = cast("dict[str, ExchangeAPI]", api_clients_dict_any)
            for exchange_id_key in api_clients_dict:  # exchange_id_key is str
                exchange_id: str = str(
                    exchange_id_key,
                )  # Explicitly cast to string, though already str
                reconciliation_tasks.append(self._reconcile_exchange(exchange_id))
        else:
            logger.warning(
                "PortfolioTracker has no api_clients attribute or it's not a dict. "
                "Skipping reconciliation.",
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
                    "exception_during_exchange_reconciliation",
                    error=str(result_item),
                    error_type=type(result_item).__name__,
                    message=f"Exception during exchange reconciliation: {result_item}",
                    exc_info=result_item,
                )
                overall_results["success"] = False
                continue  # Move to the next result item

            # Ensure result_item is a dict before accessing keys
            if isinstance(result_item, dict):
                if not result_item.get("success", False):  # Check if 'success' is False
                    overall_results["success"] = False
                    overall_results["error"] = result_item.get(
                        "error",
                        "Unknown error during exchange reconciliation",
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
                    "unexpected_item_type_in_reconciliation_results",
                    item_type=str(type(result_item)),
                    item=str(result_item),
                    message=(
                        f"Unexpected item type in reconciliation results: "
                        f"{type(result_item)}. Item: {result_item}"
                    ),
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
        self.logger.debug(
            "position_reconciliation_start",
            action="reconcile_exchange",
            exchange=exchange,
            message=f"PRS._reconcile_exchange: Starting for {exchange}",
        )

        api_client = self._portfolio_tracker.api_clients.get(exchange)
        if not api_client:
            self.logger.warning(
                "no_api_client_for_exchange",
                action="reconcile_exchange",
                exchange=exchange,
                message=f"PRS._reconcile_exchange: No API client for {exchange}, skipping.",
            )
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
            logger.info(
                "raw_exchange_positions_fetched",
                action="reconcile_exchange",
                exchange=exchange,
                position_count=len(exchange_positions_list),
                positions=exchange_positions_list,
                message=f"Raw exchange positions from {exchange}: {exchange_positions_list}",
            )

            # 2. Fill history-derived positions (REMOVED - Incorrect dependency/method)

            # 3. Local state tracking
            local_positions_list = self._portfolio_tracker.get_positions_by_exchange(
                exchange,
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
                "prs_reconcile_exchange_position_counts",
                exchange=exchange,
                api_positions_count=len(api_positions_map),
                local_positions_count=len(local_positions_map),
                message=(
                    f"PRS._reconcile_exchange: API position map for {exchange} has "
                    f"{len(api_positions_map)} symbols. "
                    f"Local map has {len(local_positions_map)} symbols."
                ),
            )

            # Call the method that takes maps and iterates symbols
            # This method returns a dict similar to overall_results in its own scope.
            current_exchange_results = await self.reconcile_positions(
                exchange,
                api_positions_map,
                local_positions_map,
            )

            # Auto-correct if enabled and discrepancies found
            # (already handled in reconcile_positions)
            # The _apply_corrections call was moved into reconcile_positions for clarity
            # No need for explicit _record_discrepancy here, as reconcile_positions
            # and _reconcile_symbol handle it.
            self.logger.info(
                "prs_reconcile_exchange_finished",
                exchange=exchange,
                success=current_exchange_results.get("success"),
                has_discrepancies=current_exchange_results.get("has_discrepancies"),
                message=(
                    f"PRS._reconcile_exchange: Finished reconcile_positions for {exchange}. "
                    f"Success: {current_exchange_results.get('success')}, "
                    f"Has Discrepancies: {current_exchange_results.get('has_discrepancies')}"
                ),
            )
        except Exception as e:
            self.logger.exception(
                "prs_reconcile_exchange_error",
                exchange=exchange,
                error=str(e),
                error_type=type(e).__name__,
                message=(
                    f"PRS_RECONCILE_EXCHANGE_ERROR: Unhandled exception during position "
                    f"reconciliation for {exchange}: {e}"
                ),
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
        else:
            return current_exchange_results

    async def _fetch_api_positions(self, api_clients: dict[str, ExchangeAPI]) -> dict[str, Any]:
        """Fetch positions from all API clients concurrently."""
        tasks: dict[str, asyncio.Task[list[DerivativePosition]]] = {}  # Corrected task type hint
        # isinstance check for api_clients is redundant due to type hint
        # if isinstance(api_clients, dict):
        for exchange_id, client in api_clients.items():
            tasks[exchange_id] = asyncio.create_task(client.get_positions())

        # Use return_exceptions=True
        results_gather = await asyncio.gather(*tasks.values(), return_exceptions=True)

        positions: dict[str, Any] = {}
        # Map results back using keys
        exchange_ids = list(tasks.keys())  # exchange_ids will be list[str]
        for i, result_item in enumerate(
            results_gather,
        ):  # result_item is list[DerivativePosition] | BaseException
            exchange_id = exchange_ids[i]  # exchange_id is str
            if isinstance(result_item, Exception):
                logger.error(
                    "position_fetch_failed",
                    action="fetch_api_positions",
                    exchange_id=exchange_id,
                    error=str(result_item),
                    message=f"Failed to fetch positions from {exchange_id}: {result_item}",
                )
                positions[exchange_id] = {"error": str(result_item)}  # Store error
            else:
                # result_item is list[DerivativePosition] here
                positions[exchange_id] = result_item
        return positions

    def _parse_local_position(
        self,
        exchange_id: str,
        symbol: str,
        pos_data: DerivativePosition | None,
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

            # is_finite() implies it's a Decimal. Type hints on DerivativePosition
            # should ensure these are Decimal or None.
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
                "ParsedPosition",
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
            self.logger.exception(
                "prs_parse_local_attr_error",
                exchange_id=exchange_id,
                symbol=symbol,
                error=str(e),
                error_type=type(e).__name__,
                position_data=str(pos_data),
                message=(
                    f"PRS_PARSE_LOCAL_ATTR_ERROR: Error extracting attributes "
                    f"from local DerivativePosition for {exchange_id}/{symbol}: {e}. "
                    f"Position: {pos_data}"
                ),
            )
            return cast(
                "ErrorDict",
                {
                    "error": "local_parsing_error",
                    "message": (
                        f"Attribute error parsing local DerivativePosition for "
                        f"{symbol} on {exchange_id}: {e}"
                    ),
                    "raw_data": str(pos_data),
                },
            )

    def _parse_api_position(
        self,
        exchange_id: str,
        symbol: str,
        pos_data: DerivativePosition | None,
    ) -> ParsedPosition | ErrorDict:
        if pos_data is None:
            return cast(
                "ParsedPosition",
                {
                    "side": OrderSide.BUY,  # Default, actual side for flat is often irrelevant
                    "size": Decimal(0),
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
        #     return cast(ErrorDict, {
        #         "message": (
        #         ),

        try:
            size = pos_data.size  # Assuming pos_data.size is Decimal, not Optional[Decimal]
            entry_price = pos_data.entry_price  # Can be None for flat positions
            mark_price = pos_data.mark_price
            liquidation_price = pos_data.liquidation_price
            unrealized_pnl = pos_data.unrealized_pnl

            # is_finite() implies it's a Decimal. Type hints on DerivativePosition
            # should ensure these are Decimal or None.
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
                "ParsedPosition",
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
            self.logger.exception(
                "prs_parse_api_attr_error",
                exchange_id=exchange_id,
                symbol=symbol,
                error=str(e),
                error_type=type(e).__name__,
                position_data=str(pos_data),
                message=(
                    f"PRS_PARSE_API_ATTR_ERROR: Error extracting attributes from API "
                    f"DerivativePosition for {exchange_id}/{symbol}: {e}. Position: {pos_data}"
                ),
            )
            return cast(
                "ErrorDict",
                {
                    "error": "api_parsing_error",
                    "message": (
                        f"Attribute error parsing API DerivativePosition for "
                        f"{symbol} on {exchange_id}: {e}"
                    ),
                    "raw_data": str(pos_data),
                },
            )

    async def reconcile_positions(
        self,
        exchange_id: str,
        api_positions: dict[str, Any],
        local_positions: dict[str, Any],
    ) -> dict[str, Any]:
        """Reconcile positions for a given exchange."""
        now = datetime.now(UTC)
        self.logger.debug(
            "prs_reconcile_positions_starting",
            exchange_id=exchange_id,
            api_positions_count=len(api_positions),
            local_positions_count=len(local_positions),
            message=(
                f"PRS.reconcile_positions: Starting reconciliation for {exchange_id} with "
                f"{len(api_positions)} API and {len(local_positions)} local positions."
            ),
        )

        overall_results = self._initialize_reconciliation_results(now)

        # Get all symbols to reconcile
        all_symbols = set(api_positions.keys()) | set(local_positions.keys())
        self.logger.debug(
            "prs_reconcile_positions_symbols",
            exchange_id=exchange_id,
            symbols=list(all_symbols),
            symbols_count=len(all_symbols),
            message=(
                f"PRS.reconcile_positions: Reconciling symbols for {exchange_id}: {all_symbols}"
            ),
        )

        # Create reconciliation tasks
        reconciliation_tasks = self._create_reconciliation_tasks(
            exchange_id,
            all_symbols,
            api_positions,
            local_positions,
        )

        if not reconciliation_tasks:
            logger.info(
                "no_symbols_to_reconcile",
                action="reconcile_positions",
                exchange_id=exchange_id,
                message=f"No symbols to reconcile for {exchange_id}.",
            )
            return overall_results

        # Execute reconciliation and process results
        await self._execute_and_process_reconciliation(
            exchange_id,
            reconciliation_tasks,
            overall_results,
            api_positions,
        )

        self.logger.info(
            "prs_reconcile_positions_complete",
            exchange_id=exchange_id,
            success=overall_results["success"],
            has_discrepancies=overall_results["has_discrepancies"],
            symbols_checked=overall_results["symbols_checked"],
            message=(
                f"PRS.reconcile_positions: Complete for {exchange_id}. Success: "
                f"{overall_results['success']}, Discrepancies found: "
                f"{overall_results['has_discrepancies']}, Symbols checked: "
                f"{overall_results['symbols_checked']}"
            ),
        )
        return overall_results

    def _initialize_reconciliation_results(self, now: datetime) -> dict[str, Any]:
        """Initialize the reconciliation results structure."""
        return {
            "success": True,
            "timestamp": now,
            "discrepancies": [],
            "symbols_checked": 0,
            "has_discrepancies": False,
            "exchange_results": {},
        }

    def _create_reconciliation_tasks(
        self,
        exchange_id: str,
        all_symbols: set[str],
        api_positions: dict[str, Any],
        local_positions: dict[str, Any],
    ) -> list[Awaitable[list[HistoricalDiscrepancyRecord]]]:
        """Create reconciliation tasks for all symbols."""
        reconciliation_tasks: list[Awaitable[list[HistoricalDiscrepancyRecord]]] = []

        for symbol_key in all_symbols:
            api_pos_raw = api_positions.get(symbol_key)
            local_pos_raw = local_positions.get(symbol_key)

            # Parse positions
            parsed_api_pos, parsed_local_pos = self._parse_symbol_positions(
                exchange_id,
                symbol_key,
                api_pos_raw,
                local_pos_raw,
            )

            # Log parsing results
            self._log_parsing_results(exchange_id, symbol_key, parsed_api_pos, parsed_local_pos)

            # Create reconciliation task
            reconciliation_tasks.append(
                self._reconcile_symbol(
                    exchange_id,
                    symbol_key,
                    parsed_api_pos,
                    parsed_local_pos,
                ),
            )

        return reconciliation_tasks

    def _parse_symbol_positions(
        self,
        exchange_id: str,
        symbol_key: str,
        api_pos_raw: object,
        local_pos_raw: object,
    ) -> tuple[ParsedPosition | ErrorDict, ParsedPosition | ErrorDict | None]:
        """Parse API and local positions for a symbol."""
        # Ensure local_pos_raw is DerivativePosition | None for _parse_local_position
        parsed_local_pos_input: DerivativePosition | None = None
        if isinstance(local_pos_raw, DerivativePosition):
            parsed_local_pos_input = local_pos_raw
        elif local_pos_raw is not None:
            self.logger.warning(
                "prs_reconcile_positions_unexpected_local_type",
                symbol_key=symbol_key,
                local_pos_type=type(local_pos_raw).__name__,
                message=(
                    f"PRS.reconcile_positions: Unexpected type for local_pos_raw for "
                    f"{symbol_key}: {type(local_pos_raw)}. Treating as None."
                ),
            )

        # Ensure api_pos_raw is DerivativePosition | None for _parse_api_position
        parsed_api_pos_input: DerivativePosition | None = None
        if isinstance(api_pos_raw, DerivativePosition):
            parsed_api_pos_input = api_pos_raw
        elif api_pos_raw is not None:
            self.logger.warning(
                "prs_reconcile_positions_unexpected_api_pos_type",
                symbol=symbol_key,
                actual_type=str(type(api_pos_raw)),
                expected_type="DerivativePosition",
                message=(
                    f"PRS.reconcile_positions: Unexpected type for api_pos_raw for "
                    f"{symbol_key}: {type(api_pos_raw)}. Treating as None."
                ),
            )

        parsed_api_pos = self._parse_api_position(
            exchange_id,
            symbol_key,
            parsed_api_pos_input,
        )
        parsed_local_pos = self._parse_local_position(
            exchange_id,
            symbol_key,
            parsed_local_pos_input,
        )

        return parsed_api_pos, parsed_local_pos

    def _log_parsing_results(
        self,
        exchange_id: str,
        symbol_key: str,
        parsed_api_pos: ParsedPosition | ErrorDict,
        parsed_local_pos: ParsedPosition | ErrorDict | None,
    ) -> None:
        """Log the results of position parsing."""
        api_pos_has_error = "error" in parsed_api_pos
        local_pos_has_error = isinstance(parsed_local_pos, dict) and "error" in parsed_local_pos

        self.logger.debug(
            "prs_reconcile_symbol_parse_results",
            symbol=symbol_key,
            exchange_id=exchange_id,
            api_parsed_successfully=not api_pos_has_error,
            local_parsed_successfully=(parsed_local_pos is not None and not local_pos_has_error),
            message=(
                f"PRS._reconcile_symbol: Symbol {symbol_key} on {exchange_id}, API Parsed "
                f"Successfully: {not api_pos_has_error}, Local Parsed Successfully: "
                f"{parsed_local_pos is not None and not local_pos_has_error}"
            ),
        )

    async def _execute_and_process_reconciliation(
        self,
        exchange_id: str,
        reconciliation_tasks: list[Awaitable[list[HistoricalDiscrepancyRecord]]],
        overall_results: dict[str, Any],
        api_positions: dict[str, Any],
    ) -> None:
        """Execute reconciliation tasks and process the results."""
        symbol_results_list = await asyncio.gather(*reconciliation_tasks, return_exceptions=True)

        # Process results
        aggregated_discrepancies = self._process_reconciliation_results(
            exchange_id,
            symbol_results_list,
            overall_results,
        )

        # Update overall results
        overall_results["discrepancies"] = aggregated_discrepancies
        overall_results["symbols_checked"] = len(reconciliation_tasks)

        # Apply corrections if needed
        if overall_results["has_discrepancies"]:
            await self._handle_discrepancy_corrections(exchange_id, overall_results, api_positions)

    def _process_reconciliation_results(
        self,
        exchange_id: str,
        symbol_results_list: list[list[HistoricalDiscrepancyRecord] | BaseException],
        overall_results: dict[str, Any],
    ) -> list[HistoricalDiscrepancyRecord]:
        """Process the results from symbol reconciliation tasks."""
        aggregated_discrepancies: list[HistoricalDiscrepancyRecord] = []

        for res_item in symbol_results_list:
            if isinstance(res_item, Exception):
                self._handle_reconciliation_exception(
                    exchange_id,
                    res_item,
                    overall_results,
                    aggregated_discrepancies,
                )
                continue

            if not isinstance(res_item, list):
                self._handle_unexpected_result_type(
                    exchange_id,
                    res_item,
                    overall_results,
                    aggregated_discrepancies,
                )
                continue

            aggregated_discrepancies.extend(res_item)
            if res_item:  # If the list is not empty, there were discrepancies
                overall_results["has_discrepancies"] = True

        return aggregated_discrepancies

    def _handle_reconciliation_exception(
        self,
        exchange_id: str,
        exception: Exception,
        overall_results: dict[str, Any],
        aggregated_discrepancies: list[HistoricalDiscrepancyRecord],
    ) -> None:
        """Handle exceptions during reconciliation."""
        self.logger.error(
            "prs_reconcile_positions_exception",
            exchange_id=exchange_id,
            error=str(exception),
            error_type=type(exception).__name__,
            message=(
                f"PRS.reconcile_positions: Exception during symbol reconciliation for "
                f"{exchange_id}: {exception}"
            ),
            exc_info=exception,
        )
        overall_results["success"] = False

        error_record = self._record_discrepancy(
            exchange_id=exchange_id,
            symbol="UNKNOWN_SYMBOL_DUE_TO_EXCEPTION",
            discrepancy_type="reconciliation_error",
            api_val=None,
            local_val=None,
            details=f"Gather exception: {exception!s}",
        )
        aggregated_discrepancies.append(error_record)
        overall_results["has_discrepancies"] = True

    def _handle_unexpected_result_type(
        self,
        exchange_id: str,
        res_item: list[HistoricalDiscrepancyRecord] | BaseException,
        overall_results: dict[str, Any],
        aggregated_discrepancies: list[HistoricalDiscrepancyRecord],
    ) -> None:
        """Handle unexpected result types from reconciliation."""
        self.logger.error(
            "prs_reconcile_positions_unexpected_result_type",
            exchange_id=exchange_id,
            result_type=str(type(res_item)),
            result_item=str(res_item),
            message=(
                f"PRS.reconcile_positions: Unexpected result type in symbol_results_list "
                f"for {exchange_id}: {type(res_item)}. Item: {res_item}"
            ),
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

    async def _handle_discrepancy_corrections(
        self,
        exchange_id: str,
        overall_results: dict[str, Any],
        api_positions: dict[str, Any],
    ) -> None:
        """Handle corrections for found discrepancies."""
        self.logger.info(
            "prs_reconcile_positions_starting_correction_check",
            exchange_id=exchange_id,
            has_discrepancies=overall_results["has_discrepancies"],
            message=(
                f"PRS.reconcile_positions: Starting correction check for {exchange_id}. "
                f"Discrepancies found: {overall_results['has_discrepancies']}"
            ),
        )

        # Build API positions map for corrections
        api_positions_map_for_correction = self._build_api_positions_map(exchange_id, api_positions)

        # Apply corrections if conditions are met
        if (
            overall_results.get("auto_correct")
            and api_positions_map_for_correction
            and self._portfolio_tracker
        ):
            self._apply_corrections(
                exchange_id,
                overall_results,
                api_positions_map_for_correction,
            )
        elif not api_positions_map_for_correction:
            self.logger.warning(
                "prs_reconcile_positions_no_api_positions_for_correction",
                exchange_id=exchange_id,
                message=(
                    f"PRS.reconcile_positions: Cannot apply corrections for {exchange_id} "
                    f"as no valid DerivativePosition found in api_positions input."
                ),
            )

    def _build_api_positions_map(
        self,
        exchange_id: str,
        api_positions: dict[str, Any],
    ) -> dict[str, DerivativePosition]:
        """Build a map of API positions for corrections."""
        api_positions_map_for_correction: dict[str, DerivativePosition] = {}

        for sym, pos_data in api_positions.items():
            if isinstance(pos_data, DerivativePosition):
                api_positions_map_for_correction[sym] = pos_data
            else:
                self.logger.warning(
                    "prs_reconcile_positions_invalid_api_position_type",
                    symbol=sym,
                    actual_type=str(type(pos_data)),
                    expected_type="DerivativePosition",
                    message=(
                        f"PRS.reconcile_positions: Item '{sym}' in api_positions is not a "
                        f"DerivativePosition for correction: {type(pos_data)}"
                    ),
                )

        return api_positions_map_for_correction

    async def _reconcile_symbol(
        self,
        exchange_id: str,
        symbol: str,
        parsed_api_pos: ParsedPosition | ErrorDict,
        parsed_local_pos: ParsedPosition | ErrorDict | None,
    ) -> list[HistoricalDiscrepancyRecord]:
        discrepancy_records: list[HistoricalDiscrepancyRecord] = []

        # Handle parsing errors first
        error_records = self._handle_parsing_errors(
            exchange_id,
            symbol,
            parsed_api_pos,
            parsed_local_pos,
        )
        if error_records:
            return error_records

        # At this point, both positions are valid ParsedPosition objects
        api_pos_data = cast("ParsedPosition", parsed_api_pos)
        local_pos_data = cast("ParsedPosition", parsed_local_pos) if parsed_local_pos else None

        # Handle position existence discrepancies
        existence_records = self._handle_position_existence_discrepancies(
            exchange_id,
            symbol,
            api_pos_data,
            local_pos_data,
        )
        discrepancy_records.extend(existence_records)

        # If one position is missing or flat, no need for detailed comparison
        if local_pos_data is None or api_pos_data["size"] == Decimal(0):
            return discrepancy_records

        # Compare position attributes for non-flat positions
        attribute_records = self._compare_position_attributes(
            exchange_id,
            symbol,
            api_pos_data,
            local_pos_data,
        )
        discrepancy_records.extend(attribute_records)

        return discrepancy_records

    def _handle_parsing_errors(
        self,
        exchange_id: str,
        symbol: str,
        parsed_api_pos: ParsedPosition | ErrorDict,
        parsed_local_pos: ParsedPosition | ErrorDict | None,
    ) -> list[HistoricalDiscrepancyRecord]:
        """Handle parsing errors for API and local positions."""
        # Check if parsed_api_pos is an ErrorDict
        if "error" in parsed_api_pos:
            error_dict_api = cast("ErrorDict", parsed_api_pos)
            record = self._record_discrepancy(
                exchange_id=exchange_id,
                symbol=symbol,
                discrepancy_type="api_parsing_error",
                api_val=str(error_dict_api.get("raw_data", "N/A")),
                local_val=None,
                details=str(error_dict_api.get("message", "Unknown API parsing error.")),
            )
            return [record]

        # Check if parsed_local_pos is an ErrorDict
        if isinstance(parsed_local_pos, dict) and "error" in parsed_local_pos:
            error_dict_local = cast("ErrorDict", parsed_local_pos)
            record = self._record_discrepancy(
                exchange_id=exchange_id,
                symbol=symbol,
                discrepancy_type="local_parsing_error",
                api_val=None,
                local_val=str(error_dict_local.get("raw_data", "N/A")),
                details=str(error_dict_local.get("message", "Unknown local parsing error.")),
            )
            return [record]

        return []

    def _handle_position_existence_discrepancies(
        self,
        exchange_id: str,
        symbol: str,
        api_pos_data: ParsedPosition,
        local_pos_data: ParsedPosition | None,
    ) -> list[HistoricalDiscrepancyRecord]:
        """Handle discrepancies related to position existence."""
        discrepancy_records: list[HistoricalDiscrepancyRecord] = []

        # Case 1: Position exists on API but not locally
        if local_pos_data is None:
            if api_pos_data["size"] != Decimal(0):
                record = self._record_discrepancy(
                    exchange_id=exchange_id,
                    symbol=symbol,
                    discrepancy_type="size",
                    api_val=api_pos_data["size"],
                    local_val=Decimal(0),
                    details=(
                        f"Position exists on {exchange_id} (size {api_pos_data['size']}) "
                        f"but not in PortfolioTracker (or size 0)."
                    ),
                )
                discrepancy_records.append(record)
            return discrepancy_records

        # Case 2: Position exists locally but API reports flat
        if api_pos_data["size"] == Decimal(0) and local_pos_data["size"] != Decimal(0):
            record = self._record_discrepancy(
                exchange_id=exchange_id,
                symbol=symbol,
                discrepancy_type="size",
                api_val=Decimal(0),
                local_val=local_pos_data["size"],
                details=(
                    f"Position exists in PortfolioTracker (size {local_pos_data['size']}) "
                    f"but flat or not found on {exchange_id}."
                ),
            )
            discrepancy_records.append(record)

        return discrepancy_records

    def _compare_position_attributes(
        self,
        exchange_id: str,
        symbol: str,
        api_pos_data: ParsedPosition,
        local_pos_data: ParsedPosition,
    ) -> list[HistoricalDiscrepancyRecord]:
        """Compare attributes of two non-flat positions."""
        discrepancy_records: list[HistoricalDiscrepancyRecord] = []

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

        # Entry price comparison (only if sides and sizes match)
        if (self._should_compare_entry_price(api_pos_data, local_pos_data)) and (
            api_pos_data["entry_price"] != local_pos_data["entry_price"]
        ):
            record = self._record_discrepancy(
                exchange_id=exchange_id,
                symbol=symbol,
                discrepancy_type="entry_price",
                api_val=api_pos_data["entry_price"],
                local_val=local_pos_data["entry_price"],
            )
            discrepancy_records.append(record)

        # Mark price comparison
        if self._should_compare_mark_price(api_pos_data, local_pos_data):
            record = self._record_discrepancy(
                exchange_id=exchange_id,
                symbol=symbol,
                discrepancy_type="mark_price",
                api_val=api_pos_data.get("mark_price"),
                local_val=local_pos_data.get("mark_price"),
            )
            discrepancy_records.append(record)

        return discrepancy_records

    def _should_compare_entry_price(
        self,
        api_pos_data: ParsedPosition,
        local_pos_data: ParsedPosition,
    ) -> bool:
        """Determine if entry prices should be compared."""
        return (
            api_pos_data["side"] == local_pos_data["side"]
            and api_pos_data["size"] != Decimal(0)
            and api_pos_data["size"] == local_pos_data["size"]
        )

    def _should_compare_mark_price(
        self,
        api_pos_data: ParsedPosition,
        local_pos_data: ParsedPosition,
    ) -> bool:
        """Determine if mark prices should be compared."""
        return api_pos_data.get("mark_price") != local_pos_data.get("mark_price") and api_pos_data[
            "size"
        ] != Decimal(0)

    async def _compare_positions(
        self,
        api_positions: dict[str, Any],
        local_positions: dict[str, Any],
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
                    "prs_compare_positions_unexpected_local_pos_type",
                    symbol=symbol_key,
                    actual_type=str(type(local_pos_raw)),
                    expected_type="DerivativePosition",
                    message=(
                        f"PRS._compare_positions: Unexpected type for local_pos_raw "
                        f"for {symbol_key}: {type(local_pos_raw)}. Treating as None."
                    ),
                )

            parsed_api = self._parse_api_position(
                exchange_id_placeholder,
                symbol_key,
                cast("DerivativePosition | None", api_pos_raw),
            )
            # Assuming local_pos_raw is DerivativePosition | None for _parse_local_position
            # If local_positions can contain other types, this cast might be problematic
            parsed_local = self._parse_local_position(
                exchange_id_placeholder,
                symbol_key,
                parsed_local_pos_input,
            )

            reconciliation_tasks.append(
                self._reconcile_symbol(
                    exchange_id_placeholder,
                    symbol_key,
                    parsed_api,
                    parsed_local,
                ),
            )

        results_gather = await asyncio.gather(*reconciliation_tasks, return_exceptions=True)

        aggregated_discrepancies: list[HistoricalDiscrepancyRecord] = []
        for res_item in results_gather:
            if isinstance(res_item, Exception):
                self.logger.error(
                    "prs_compare_positions_reconciliation_error",
                    error=str(res_item),
                    error_type=type(res_item).__name__,
                    message=(
                        f"PRS._compare_positions: Error during symbol reconciliation task: "
                        f"{res_item}"
                    ),
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
                    details=f"Gather exception in _compare_positions: {res_item!s}",
                )
                aggregated_discrepancies.append(error_record)
                overall_results["has_discrepancies"] = True
                continue

            if not isinstance(
                res_item,
                list,
            ):  # _reconcile_symbol returns list[HistoricalDiscrepancyRecord]
                self.logger.error(
                    "prs_compare_positions_unexpected_result_type",
                    result_type=str(type(res_item)),
                    result_item=str(res_item),
                    message=(
                        f"PRS._compare_positions: Unexpected result type from _reconcile_symbol: "
                        f"{type(res_item)}. Item: {res_item}"
                    ),
                )
                overall_results["success"] = False
                error_record = self._record_discrepancy(
                    exchange_id=exchange_id_placeholder,
                    symbol="UNKNOWN_SYMBOL_DUE_TO_COMPARE_BAD_RESULT",
                    discrepancy_type="reconciliation_error",
                    api_val=None,
                    local_val=None,
                    details=f"Unexpected result type from _reconcile_symbol in "
                    f"_compare_positions: {type(res_item)}",
                )
                aggregated_discrepancies.append(error_record)
                overall_results["has_discrepancies"] = True
                continue

            aggregated_discrepancies.extend(res_item)
            if res_item:  # If the list is not empty, there were discrepancies
                overall_results["has_discrepancies"] = True

        overall_results["discrepancies"] = aggregated_discrepancies
        overall_results["symbols_checked"] = len(all_symbols)

        # The original logic for recording/correcting is not directly applicable here
        # as _compare_positions is more of a utility. Discrepancies are collected
        # in overall_results["discrepancies"].
        if overall_results["has_discrepancies"]:
            self.logger.warning(
                "prs_compare_positions_discrepancies_found",
                discrepancies_count=len(overall_results["discrepancies"]),
                discrepancies=overall_results["discrepancies"],
                message=(
                    f"PRS._compare_positions: Discrepancies found: "
                    f"{overall_results['discrepancies']}"
                ),
            )

        # self.latest_results should probably not be updated by this generic comparison method.
        # It's more specific to the main reconciliation flow.
        return overall_results
