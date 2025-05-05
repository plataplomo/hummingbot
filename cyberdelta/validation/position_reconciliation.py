"""
Position Reconciliation System for the CyberDeltaEngine.

This module provides validation between various position tracking systems to ensure consistency.
"""

import asyncio
import logging
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta
from decimal import Decimal, getcontext
from typing import Any

from cyberdelta.core.models import DerivativePosition, OrderSide
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.utils.config import Config

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
        self.last_check_time = datetime.now(UTC) - timedelta(seconds=self.check_interval + 1)

        # Record of discrepancies found
        self.discrepancy_history: list[dict[str, Any]] = []

        # Initialize last run time to a long time ago
        getcontext().prec = 50  # Set precision for Decimal operations

        # Internal state
        self._last_reconciliation_run: datetime = datetime.min.replace(tzinfo=UTC)

        # Get interval, ensuring it's a float
        interval_val = config.get("validation.position_reconciliation.interval_seconds", 300.0)
        if isinstance(interval_val, int | float):
            self._reconciliation_interval_secs: float = float(interval_val)
            self._next_reconciliation_time: datetime = datetime.now(UTC) + timedelta(
                seconds=self._reconciliation_interval_secs
            )
        else:
            logger.warning(
                f"Invalid reconciliation interval type ('{type(interval_val)}'), defaulting to 300.0 seconds."
            )
            self._reconciliation_interval_secs = 300.0  # Default float value

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

    def register_portfolio_tracker(self, portfolio_tracker: PortfolioTracker) -> None:
        """
        Register the portfolio tracker instance.

        Args:
            portfolio_tracker: The portfolio tracker instance
        """
        self._portfolio_tracker = portfolio_tracker

    async def check_positions(self, force: bool = False) -> dict[str, dict[str, Any]]:
        """
        Check for position discrepancies across all exchanges.

        Args:
            force: Force a check regardless of the interval

        Returns:
            Dictionary of reconciliation results by exchange
        """
        now = datetime.now(UTC)

        # Check if we should run reconciliation
        if not force and (now - self.last_check_time).total_seconds() < self.check_interval:
            logger.debug("Skipping position reconciliation, not due yet")
            return self.latest_results

        if self._portfolio_tracker is None:
            logger.error("Cannot reconcile positions: Portfolio tracker not registered")
            return {}

        logger.info("Running position reconciliation check")
        self.last_check_time = now

        # Get all active exchanges
        exchanges: list[str] = []
        for exchange_id in self._config.get("exchanges", {}).keys():
            if self._config.get(f"exchanges.{exchange_id}.enabled", False):
                exchanges.append(exchange_id)

        # Results by exchange
        results: dict[str, dict[str, Any]] = {}

        # Check each exchange
        for exchange in exchanges:
            api_client = self._portfolio_tracker.api_clients.get(exchange)
            if not api_client:
                logger.warning(f"No API client registered for {exchange}, skipping reconciliation")
                continue

            # Get positions from all three sources
            try:
                # 1. Exchange API positions
                exchange_positions = await api_client.get_positions()
                logger.info(f"Raw exchange positions from {exchange}: {exchange_positions}")

                # 2. Fill history-derived positions (REMOVED - Incorrect dependency/method)
                # execution_handler = self._portfolio_tracker.get_execution_handler(exchange)
                # fill_positions = execution_handler.get_derived_positions() \
                #     if execution_handler else []
                fill_positions: list[
                    DerivativePosition
                ] = []  # Add type hint List[DerivativePosition]

                # 3. Local state tracking
                local_positions = self._portfolio_tracker.get_positions_by_exchange(exchange)

                # Perform the reconciliation (pass empty fill_positions)
                exchange_results = self._reconcile_positions(
                    exchange, exchange_positions, fill_positions, local_positions
                )

                # Store the results
                results[exchange] = exchange_results

                # Record any discrepancies
                if exchange_results["discrepancies"]:
                    self._record_discrepancy(exchange, exchange_results)

                    # Auto-correct if enabled
                    if self.auto_correct:
                        self._apply_corrections(exchange, exchange_results)

            except Exception as e:
                logger.error(f"Error reconciling positions for {exchange}: {str(e)}")
                results[exchange] = {
                    "success": False,
                    "error": str(e),
                    "timestamp": now,
                    "discrepancies": [],
                }

        self.latest_results = results
        return results

    def _reconcile_positions(
        self,
        exchange: str,
        exchange_positions: list[DerivativePosition],
        fill_positions: list[DerivativePosition],
        local_positions: list[DerivativePosition],
    ) -> dict[str, Any]:
        """
        Reconcile positions from different sources for a given exchange.

        Args:
            exchange: Exchange identifier
            exchange_positions: Positions reported by exchange API
            fill_positions: Positions derived from fill history
            local_positions: Positions in the local state

        Returns:
            Reconciliation results
        """
        now = datetime.now(UTC)

        # Create position maps for easier comparison
        exchange_map = {p.symbol: p for p in exchange_positions}
        local_map = {p.symbol: p for p in local_positions}

        # Get all unique symbols from relevant sources
        all_symbols = set(exchange_map.keys()) | set(local_map.keys())

        # === ADDED Logging ===
        logger.info(
            f"Reconciling {exchange}: ExchangeMap={exchange_map}, "
            f"LocalMap={local_map}, AllSymbols={all_symbols}"
        )
        # === END Logging ===

        # Check for discrepancies
        discrepancies = []

        for symbol in all_symbols:
            # Get positions from each source (or create empty position)
            exch_pos = exchange_map.get(
                symbol,
                DerivativePosition(
                    symbol=symbol,
                    size=Decimal("0"),
                    entry_price=Decimal("0"),
                    mark_price=Decimal("0"),
                    side=OrderSide.BUY,
                    leverage=Decimal("1"),
                ),
            )

            local_pos = local_map.get(
                symbol,
                DerivativePosition(
                    symbol=symbol,
                    size=Decimal("0"),
                    entry_price=Decimal("0"),
                    mark_price=Decimal("0"),
                    side=OrderSide.BUY,
                    leverage=Decimal("1"),
                ),
            )

            # === ADDED Logging ===
            logger.info(
                f"Reconciling {exchange}/{symbol}: ExchSize={exch_pos.size}, "
                f"LocalSize={local_pos.size}"
            )
            # === END Logging ===

            # Check size discrepancy (Exchange vs Local)
            # Ensure sizes are Decimal before calculation
            exch_size_dec = (
                Decimal(str(exch_pos.size))
                if not isinstance(exch_pos.size, Decimal)
                else exch_pos.size
            )
            local_size_dec = (
                Decimal(str(local_pos.size))
                if not isinstance(local_pos.size, Decimal)
                else local_pos.size
            )
            size_discrepancy = abs(exch_size_dec - local_size_dec)

            # Convert float threshold to Decimal for comparison
            size_threshold_dec = Decimal(str(self.reconciliation_threshold))

            # --- Threshold calculation fix ---
            # Calculate absolute threshold based on the larger of the two position sizes (or a minimum value)
            # This handles cases where one position is zero.
            max_abs_size = max(abs(exch_size_dec), abs(local_size_dec))
            if max_abs_size == Decimal("0.0"):
                # If both are zero, there's no discrepancy relative to size
                size_threshold_amount = Decimal("0.0")
            else:
                # Threshold is a percentage of the larger absolute size
                size_threshold_amount = size_threshold_dec * max_abs_size + Decimal("0.00001")
            # Add a small absolute minimum threshold to catch discrepancies when positions are very small
            # or one is zero (e.g., detecting 0 vs 0.001)
            # This value should be configurable or based on asset precision.
            final_threshold = max(size_threshold_amount, Decimal("0.000001"))
            # --- End Threshold calculation fix ---

            # Use the final calculated threshold for comparison
            if size_discrepancy > final_threshold:
                discrepancy_details = {
                    "symbol": symbol,
                    "type": "size",
                    "exchange_value": str(exch_pos.size),  # Keep original string representation
                    "local_value": str(local_pos.size),  # Keep original string representation
                    "discrepancy": str(size_discrepancy),
                }
                discrepancies.append(discrepancy_details)
                logger.warning(f"Discrepancy found for {exchange}/{symbol}: {discrepancy_details}")

            # Optionally, add checks for entry price or side if needed
            # (Compare exch_pos vs local_pos)

        return {
            "success": True,
            "timestamp": now,
            "discrepancies": discrepancies,
            "symbols_checked": len(all_symbols),
            "has_discrepancies": len(discrepancies) > 0,
        }

    def _record_discrepancy(self, exchange: str, results: dict[str, Any]) -> None:
        """
        Record a discrepancy for historical tracking.

        Args:
            exchange: Exchange identifier
            results: Reconciliation results
        """
        timestamp = results["timestamp"]

        for discrepancy in results["discrepancies"]:
            record = {
                "timestamp": timestamp,
                "exchange": exchange,
                "symbol": discrepancy["symbol"],
                "exchange_value": discrepancy["exchange_value"],
                "local_value": discrepancy["local_value"],
                "discrepancy": discrepancy["discrepancy"],
                "corrected": False,
            }

            self.discrepancy_history.append(record)

            # Log the discrepancy
            logger.warning(
                f"Position discrepancy detected: {exchange} {discrepancy['symbol']} "
                f"[Exchange: {discrepancy['exchange_value']}, "
                f"Local: {discrepancy['local_value']}]"
            )

    def _apply_corrections(self, exchange: str, results: dict[str, Any]) -> None:
        """
        Apply corrections to the portfolio tracker based on reconciliation results.

        Args:
            exchange: Exchange identifier
            results: Reconciliation results
        """
        if not self._portfolio_tracker:
            logger.error("Cannot apply corrections: Portfolio tracker not registered")
            return

        for discrepancy in results["discrepancies"]:
            symbol = discrepancy["symbol"]
            exchange_value = Decimal(discrepancy["exchange_value"])
            # local_value = Decimal(discrepancy["local_value"]) # F841 Unused

            # Get current position
            current_position = self._portfolio_tracker.get_position(exchange, symbol)

            if current_position is None:
                # Create a new position with correct size
                if exchange_value != 0:
                    logger.info(
                        f"Creating missing position: {exchange} {symbol} size={exchange_value}"
                    )

                    # Use exchange position data to create the position
                    exchange_position = next(
                        (
                            p
                            for p in self._portfolio_tracker._fetch_exchange_positions(exchange)
                            if p.symbol == symbol
                        ),
                        None,
                    )

                    if exchange_position:
                        self._portfolio_tracker.update_position(exchange, exchange_position)
            else:
                # Update existing position with correct size
                if current_position.size != exchange_value:
                    logger.info(
                        f"Correcting position: {exchange} {symbol} "
                        f"from {current_position.size} to {exchange_value}"
                    )

                    # Create updated position
                    updated_position = DerivativePosition(
                        symbol=current_position.symbol,
                        size=exchange_value,
                        entry_price=current_position.entry_price,
                        mark_price=current_position.mark_price,
                        liquidation_price=current_position.liquidation_price,
                        unrealized_pnl=current_position.unrealized_pnl,
                        leverage=current_position.leverage,
                        side=current_position.side,
                    )

                    self._portfolio_tracker.update_position(exchange, updated_position)

            # Mark as corrected in history
            for record in self.discrepancy_history:
                if (
                    record["exchange"] == exchange
                    and record["symbol"] == symbol
                    and record["timestamp"] == results["timestamp"]
                ):
                    record["corrected"] = True

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
            for exchange_id in api_clients_dict.keys():  # Iterate over validated dict
                reconciliation_tasks.append(self._reconcile_exchange(exchange_id))
        else:
            logger.warning(
                "PortfolioTracker has no api_clients attribute or it's not a dict. Skipping reconciliation."
            )
            overall_results["success"] = False
            overall_results["error"] = "PortfolioTracker missing or invalid api_clients"
            return overall_results  # Return early if no clients

        # Run reconciliation for all exchanges concurrently
        results = await asyncio.gather(*reconciliation_tasks, return_exceptions=True)

        # Aggregate results
        for result in results:
            if not result["success"]:
                overall_results["success"] = False
                overall_results["error"] = result["error"]
                overall_results["discrepancies"].append(result["discrepancies"])
                overall_results["has_discrepancies"] = True
            else:
                overall_results["symbols_checked"] += result["symbols_checked"]
                overall_results["discrepancies"].extend(result["discrepancies"])
                overall_results["exchange_results"][result["exchange"]] = result

        # Record discrepancies
        if overall_results["has_discrepancies"]:
            self._record_discrepancy(None, overall_results)

            # Auto-correct if enabled
            if self.auto_correct:
                self._apply_corrections(None, overall_results)

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
            exchange_positions = await api_client.get_positions()
            logger.info(f"Raw exchange positions from {exchange}: {exchange_positions}")

            # 2. Fill history-derived positions (REMOVED - Incorrect dependency/method)
            # execution_handler = self._portfolio_tracker.get_execution_handler(exchange)
            # fill_positions = execution_handler.get_derived_positions() \
            #     if execution_handler else []
            fill_positions: list[DerivativePosition] = []  # Add type hint List[DerivativePosition]

            # 3. Local state tracking
            local_positions = self._portfolio_tracker.get_positions_by_exchange(exchange)

            # Perform the reconciliation (pass empty fill_positions)
            exchange_results = self._reconcile_positions(
                exchange, exchange_positions, fill_positions, local_positions
            )

            # Store the results
            results = exchange_results

            # Record any discrepancies
            if exchange_results["discrepancies"]:
                self._record_discrepancy(exchange, exchange_results)

                # Auto-correct if enabled
                if self.auto_correct:
                    self._apply_corrections(exchange, exchange_results)

            return results

        except Exception as e:
            logger.error(f"Error reconciling positions for {exchange}: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "timestamp": now,
                "discrepancies": [],
            }
