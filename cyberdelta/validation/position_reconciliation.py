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
            # Ensure reconciliation can happen, initialize next time
            self._next_reconciliation_time: datetime = datetime.now(UTC) + timedelta(
                seconds=self._reconciliation_interval_secs
            )
        else:
            logger.warning(
                f"Invalid reconciliation interval type ('{type(interval_val)}'), "
                f"defaulting to 300.0 seconds."
            )
            self._reconciliation_interval_secs = 300.0  # Default float value
            # Initialize even with default
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

        self.reconciliation_interval: timedelta = timedelta(
            seconds=300
        )  # Example default, ensure type

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
        # Runtime check for interval type, even if type hint exists
        if not isinstance(interval, timedelta):
            logger.error("Reconciliation interval is not a timedelta. Using default 300s.")
            interval = timedelta(seconds=300)

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
        if not isinstance(api_clients, dict):
            logger.error("PortfolioTracker api_clients is missing or not a dict.")
            return {}

        self.last_check_time = now  # Update last check time *before* starting

        tasks: dict[str, asyncio.Task[dict[str, Any]]] = {}
        if api_clients:
            for exchange in api_clients.keys():
                tasks[exchange] = self._reconcile_exchange(exchange)
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
                "PortfolioTracker has no api_clients attribute or it's not a dict. "
                "Skipping reconciliation."
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

            # Convert lists to maps by symbol for reconcile_positions
            api_positions_map = {pos.symbol: pos for pos in exchange_positions if pos}
            local_positions_map = {pos.symbol: pos for pos in local_positions if pos}

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
                    self._apply_corrections(exchange, exchange_results_dict)

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
        tasks = {}
        # Check type before iterating keys
        if isinstance(api_clients, dict):
            for exchange_id, client in api_clients.items():
                tasks[exchange_id] = asyncio.create_task(client.get_positions())
        else:
            logger.error("api_clients is not a dictionary, cannot fetch positions.")
            return {}

        # Use return_exceptions=True
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        positions: dict[str, Any] = {}
        # Map results back using keys
        exchange_ids = list(tasks.keys())
        for i, result in enumerate(results):
            exchange_id = exchange_ids[i]
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch positions from {exchange_id}: {result}")
                positions[exchange_id] = {"error": str(result)}  # Store error
            else:
                positions[exchange_id] = (
                    result  # Store successful result (list[DerivativePosition])
                )

        return positions

    def _parse_local_position(
        self, exchange_id: str, symbol: str, position_data: dict[str, Any]
    ) -> DerivativePosition | None:
        try:
            size_dec = Decimal(str(position_data.get("size", "0")))
            entry_price = Decimal(position_data.get("entryPrice", "0"))
            mark_price = Decimal(position_data.get("markPrice", "0"))
            liq_price = Decimal(position_data.get("liquidationPrice", "0"))
            pnl = Decimal(position_data.get("unrealizedPnl", "0"))

            return DerivativePosition(
                exchange=exchange_id,
                symbol=symbol,
                side=OrderSide.BUY if size_dec > 0 else OrderSide.SELL,
                size=size_dec,
                entry_price=entry_price,
                mark_price=mark_price,
                liquidation_price=liq_price,
                unrealized_pnl=pnl,
                timestamp=datetime.now(UTC),
            )
        except (ValidationError, KeyError, InvalidOperation, TypeError) as e:
            logger.error(f"Position reconciliation failed: {e}")
            return None

    def _parse_api_position(
        self, exchange_id: str, symbol: str, position_data: Any
    ) -> DerivativePosition | None:
        """Safely parse API position data into DerivativePosition model."""
        try:
            # Assuming position_data is already a dict-like structure from API
            size = parse_decimal_value(position_data.get("szi"), allow_none=False)
            entry_price = parse_decimal_value(position_data.get("entryPx"), allow_none=True)
            mark_price = parse_decimal_value(position_data.get("markPx"), allow_none=True)
            unrealized_pnl = parse_decimal_value(
                position_data.get("unrealizedPnl"), allow_none=True
            )
            liquidation_price = parse_decimal_value(
                position_data.get("liquidationPx"), allow_none=True
            )
            margin_used = parse_decimal_value(position_data.get("marginUsed"), allow_none=True)

            if size is None:
                logger.warning(f"Missing or invalid size for {exchange_id}/{symbol} in API data")
                return None

            timestamp = datetime.now(UTC)  # Placeholder timestamp

            return DerivativePosition(
                symbol=symbol,
                exchange=exchange_id,
                timestamp=timestamp,
                size=size,
                side=OrderSide.BUY if size > 0 else OrderSide.SELL if size < 0 else None,
                entry_price=entry_price,
                mark_price=mark_price,
                unrealized_pnl=unrealized_pnl,
                liquidation_price=liquidation_price,
                margin_used=margin_used,
            )
        except (ValidationError, InvalidOperation, AttributeError, TypeError, KeyError) as e:
            logger.error(
                f"Error parsing API position for {exchange_id}/{symbol}: {e}", exc_info=True
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
            "exchange_results": {},  # Store individual results
        }

        # Check portfolio tracker exists
        # Removed 'is None' check as tracker type is guaranteed by __init__

        reconciliation_tasks: list[Awaitable[dict[str, Any]]] = []
        # Ensure api_clients attribute exists and is a dict before iterating
        api_clients_dict = getattr(self._portfolio_tracker, "api_clients", None)
        if isinstance(api_clients_dict, dict):
            for symbol in api_positions:
                reconciliation_tasks.append(
                    self._reconcile_symbol(
                        exchange_id, symbol, api_positions[symbol], local_positions.get(symbol)
                    )
                )
        else:
            logger.warning(
                "PortfolioTracker has no api_clients attribute or it's not a dict. "
                "Skipping reconciliation."
            )
            overall_results["success"] = False
            overall_results["error"] = "PortfolioTracker missing or invalid api_clients"
            return overall_results  # Return early if no clients

        # Run reconciliation for all symbols concurrently
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
            self._record_discrepancy(exchange_id, overall_results)

            # Auto-correct if enabled
            if self.auto_correct:
                self._apply_corrections(exchange_id, overall_results)

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
            discrepancy_details = {
                "symbol": symbol,
                "type": "entry_price",
                "exchange_value": str(parsed_api_position.entry_price),
                "local_value": str(parsed_local_position.entry_price),
                "discrepancy": str(
                    abs(parsed_api_position.entry_price - parsed_local_position.entry_price)
                ),
            }
            discrepancies.append(discrepancy_details)
            logger.warning(f"Discrepancy found for {exchange_id}/{symbol}: {discrepancy_details}")

        if parsed_api_position.mark_price != parsed_local_position.mark_price:
            discrepancy_details = {
                "symbol": symbol,
                "type": "mark_price",
                "exchange_value": str(parsed_api_position.mark_price),
                "local_value": str(parsed_local_position.mark_price),
                "discrepancy": str(
                    abs(parsed_api_position.mark_price - parsed_local_position.mark_price)
                ),
            }
            discrepancies.append(discrepancy_details)
            logger.warning(f"Discrepancy found for {exchange_id}/{symbol}: {discrepancy_details}")

        if parsed_api_position.liquidation_price != parsed_local_position.liquidation_price:
            discrepancy_details = {
                "symbol": symbol,
                "type": "liquidation_price",
                "exchange_value": str(parsed_api_position.liquidation_price),
                "local_value": str(parsed_local_position.liquidation_price),
                "discrepancy": str(
                    abs(
                        parsed_api_position.liquidation_price
                        - parsed_local_position.liquidation_price
                    )
                ),
            }
            discrepancies.append(discrepancy_details)
            logger.warning(f"Discrepancy found for {exchange_id}/{symbol}: {discrepancy_details}")

        if parsed_api_position.unrealized_pnl != parsed_local_position.unrealized_pnl:
            discrepancy_details = {
                "symbol": symbol,
                "type": "unrealized_pnl",
                "exchange_value": str(parsed_api_position.unrealized_pnl),
                "local_value": str(parsed_local_position.unrealized_pnl),
                "discrepancy": str(
                    abs(parsed_api_position.unrealized_pnl - parsed_local_position.unrealized_pnl)
                ),
            }
            discrepancies.append(discrepancy_details)
            logger.warning(f"Discrepancy found for {exchange_id}/{symbol}: {discrepancy_details}")

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
                        None, symbol, api_positions[symbol], local_positions.get(symbol)
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
