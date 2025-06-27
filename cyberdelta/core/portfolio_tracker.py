"""Portfolio Tracker for CyberDeltaEngine.

This module contains the PortfolioTracker class, which is responsible for tracking
and managing the current state of the trading portfolio across multiple exchanges.
It handles balance tracking, position monitoring, order management, P&L calculations,
and portfolio reconciliation with exchange data.

The PortfolioTracker serves as the central state management component for the
trading engine, providing real-time portfolio information to other system components.
"""

from __future__ import annotations  # Enable postponed evaluation

import asyncio
from builtins import BaseException
from collections import defaultdict
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config.models.config_models import AppSettings, PortfolioTrackerConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,
    Order,
    OrderSide,
    OrderStatus,
    SpotBalance,
    Ticker,
    Trade,
)
from cyberdelta.core.symbol_mapper import SymbolMapper  # IMPORT IS PRESENT
from cyberdelta.utils.parsing import parse_datetime_utc


logger = get_logger(__name__)

# Temporarily define ExchangeType, Symbol as str TypeAlias to unblock linter
# TODO: Find or create the canonical definitions for these types
type Symbol = str
type ExchangeType = str


class PortfolioTracker:
    """Track and manage the current state of the portfolio.

    Responsible for:
    - Tracking balances across exchanges
    - Monitoring open positions and their P&L
    - Tracking order status
    - Calculating exposure metrics
    - Maintaining portfolio state
    - Performing reconciliation with exchange data
    """

    def __init__(
        self,
        app_settings: AppSettings,
        pt_config: PortfolioTrackerConfig,
        api_clients: dict[str, ExchangeAPI] | None = None,
        exchange_factories: dict[ExchangeType, Callable[..., ExchangeAPI]] | None = None,
        symbol_mapper: SymbolMapper | None = None,  # ADDED PARAMETER
    ) -> None:
        """Initialize the portfolio tracker.

        Args:
            app_settings: Application configuration
            pt_config: Portfolio tracker configuration
            api_clients: Dictionary of exchange API clients
            exchange_factories: Dictionary of exchange API factory functions
            symbol_mapper: Symbol mapper instance # ADDED DOC

        """
        self.logger = get_logger(__name__ + "." + self.__class__.__name__)
        self.app_settings: AppSettings = app_settings
        self.api_clients: dict[str, ExchangeAPI] = api_clients or {}
        self.exchange_factories = exchange_factories or {}
        self.symbol_mapper: SymbolMapper | None = symbol_mapper  # STORE AS SELF.SYMBOL_MAPPER
        self._lock = asyncio.Lock()
        self._background_tasks: set[asyncio.Task[Any]] = set()

        # Internal state
        self.balances: defaultdict[str, defaultdict[str, SpotBalance]] = defaultdict(
            lambda: defaultdict(
                # Ensure a default SpotBalance that makes sense if an asset is queried
                # before it's set
                lambda: SpotBalance(
                    exchange="",  # Should be overridden or considered invalid
                    asset="",  # Should be overridden or considered invalid
                    total_quantity=Decimal(0),
                    available_quantity=Decimal(0),
                    timestamp=datetime.min.replace(tzinfo=UTC),  # Clearly old timestamp
                ),
            ),
        )
        self.positions: defaultdict[str, defaultdict[str, DerivativePosition]] = defaultdict(
            lambda: defaultdict(
                # Placeholder for a non-existent position
                lambda: DerivativePosition(
                    exchange="",
                    symbol="",
                    side=OrderSide.BUY,  # Default for a zero-size position; arbitrary
                    size=Decimal(0),
                    entry_price=None,  # Must be None if size is 0
                    timestamp=datetime.min.replace(tzinfo=UTC),
                ),
            ),
        )
        self.orders: defaultdict[str, dict[str, Order]] = defaultdict(
            dict,
        )  # client_order_id -> Order

        # Initialize last update times
        # Consolidate individual last update times
        self.last_update_time: defaultdict[str, datetime] = defaultdict(
            lambda: datetime.min.replace(tzinfo=UTC),
        )
        self.last_reconciliation_time: defaultdict[str, datetime] = defaultdict(
            lambda: datetime.min.replace(tzinfo=UTC),
        )
        # Removed individual _last_balance_update_times, _last_position_update_times, etc.

        self.tickers: dict[str, Ticker] = {}

        # Use the passed portfolio tracker config
        self.pt_config = pt_config

        self._initialize_from_config()

        # Reconciliation interval (5 minutes by default)
        self.reconciliation_interval = 300  # seconds

        # Initialize data structures
        self._initialize_data_structures()

        # Initialize high watermark (as Decimal)
        self.high_watermark: Decimal = Decimal(
            "0.0",
        )  # Track highest portfolio value for drawdown calculation

        # Add internal tracking for realized PNL
        self.realized_pnl: Decimal = Decimal("0.0")  # Track realized PNL

        # Add active symbols and watchlist
        self.active_symbols: set[str] = set()
        self.watchlist: set[str] = set()

    def _initialize_data_structures(self) -> None:
        """Initialize data structures for all configured exchanges."""
        for exchange_id in self.app_settings.exchanges:
            if not self.app_settings.exchanges[exchange_id].enabled:
                continue

            # Ensure last_update_time and last_reconciliation_time have initial entries
            # if not already set by defaultdict lambda (which they are)
            _ = self.last_update_time[exchange_id]
            _ = self.last_reconciliation_time[exchange_id]

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """Register an API client for an exchange.

        Args:
            exchange_id: Exchange identifier
            client: ExchangeAPI implementation

        """
        self.api_clients[exchange_id] = client
        self.logger.info(
            "api_client_registered",
            component="PT_REGISTER",
            exchange_id=exchange_id,
            api_client_keys=list(self.api_clients.keys()),
            message=(
                f"PT_REGISTER: Registered API client for {exchange_id}. "
                f"Current api_clients keys: {list(self.api_clients.keys())}"
            ),
        )

    async def initialize(self) -> None:
        """Initialize portfolio state from exchanges."""
        initialization_tasks: list[Awaitable[Any]] = []
        logger.info(
            "portfolio_tracker_init_start",
            tracker_id=id(self),
            phase="gathering_init_tasks",
            balance_exchanges=list(self.balances.keys()),
            message=(
                f"PortfolioTracker {id(self)}: About to gather init tasks. "
                f"Balance dict: {self.balances}"
            ),
        )
        for exchange_id, client in self.api_clients.items():
            if not self.app_settings.exchanges[exchange_id].enabled:
                continue
            initialization_tasks.extend((
                self._fetch_exchange_account_summary(client, exchange_id),
                self._fetch_exchange_balances(exchange_id),
                self._fetch_exchange_positions(exchange_id),
                self._fetch_exchange_orders(exchange_id),
            ))
        logger.info(
            "portfolio_tracker_pre_gather",
            tracker_id=id(self),
            phase="pre_gather",
            balance_exchanges=list(self.balances.keys()),
            message=(
                f"PortfolioTracker {id(self)}: About to gather init tasks. "
                f"Balances before: {self.balances}"
            ),
        )
        results: list[bool | BaseException] = await asyncio.gather(
            *initialization_tasks,
            return_exceptions=True,
        )
        logger.info(
            "portfolio_tracker_post_gather_balances",
            phase="post_gather",
            balance_exchanges=list(self.balances.keys()),
            message=f"---> State of self.balances immediately after init gather: {self.balances}",
        )
        initialization_failed = False
        failed_tasks_info: list[str] = []
        for i, result in enumerate(results):
            if isinstance(result, BaseException):
                task_description = f"task index {i}"
                logger.critical(
                    "portfolio_tracker_init_critical_error",
                    task_index=i,
                    task_description=task_description,
                    error_type=type(result).__name__,
                    error=str(result),
                    message=(
                        f"CRITICAL ERROR during PortfolioTracker initialization "
                        f"({task_description}): {result}"
                    ),
                    exc_info=result,
                )
                failed_tasks_info.append(f"{task_description}: {result}")
                initialization_failed = True
        if initialization_failed:
            error_summary = "; ".join(failed_tasks_info)
            logger.critical(
                "portfolio_tracker_init_failed",
                error_summary=error_summary,
                failed_task_count=len(failed_tasks_info),
                action="cannot_proceed_reliably",
                message=(
                    f"PortfolioTracker failed to initialize essential data from "
                    f"one or more exchanges. Cannot proceed reliably. "
                    f"Errors: {error_summary}"
                ),
            )

        # Set initial reconciliation time AFTER fetching
        now_utc = datetime.now(UTC)
        logger.info(
            "setting_reconciliation_times",
            component="PT_INIT",
            phase="before_setting",
            last_reconciliation_time_before=dict(self.last_reconciliation_time),
            message=(
                f"PT INIT: Setting reconciliation times. Current "
                f"self.last_reconciliation_time before: {self.last_reconciliation_time}"
            ),
        )
        for exchange_id in self.api_clients:
            self.last_reconciliation_time[exchange_id] = now_utc
        logger.info(
            "reconciliation_times_set",
            component="PT_INIT",
            phase="after_setting",
            last_reconciliation_time_after=dict(self.last_reconciliation_time),
            exchanges_count=len(self.last_reconciliation_time),
            message=(
                f"PT INIT: Reconciliation times set. Current "
                f"self.last_reconciliation_time after: {self.last_reconciliation_time}"
            ),
        )

        # Calculate initial portfolio capital and set high watermark
        initial_capital = await self.get_total_capital()
        self.high_watermark = initial_capital
        if initial_capital > Decimal("0.0"):
            logger.info(
                "initial_high_watermark_set",
                high_watermark=float(self.high_watermark),
                initial_capital=float(initial_capital),
                message=f"Initial high watermark set to: {self.high_watermark}",
            )
        else:
            logger.warning(
                "initial_capital_zero_or_negative",
                initial_capital=float(initial_capital),
                action="high_watermark_not_set",
                message=f"Initial capital is {initial_capital}. High watermark not set.",
            )
        logger.info(
            "portfolio_state_initialized",
            status="initialized",
            exchanges=list(self.api_clients.keys()),
            high_watermark=float(self.high_watermark) if self.high_watermark else None,
            message="Portfolio state initialized",
        )

    async def _fetch_exchange_balances(self, exchange_id: str) -> bool:
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(
                "api_client_not_found",
                exchange_id=exchange_id,
                function="fetch_balances",
                action="returning_false",
                message=f"No API client found for {exchange_id} in fetch_balances",
            )
            return False

        try:
            balances_data = await client.get_balances()  # Returns dict[str, SpotBalance]
            updated_balances = self._process_balances_data(exchange_id, balances_data)

            if updated_balances:
                await self._update_balances_state(exchange_id, updated_balances)
            else:
                self._handle_empty_balances(exchange_id, balances_data)

            return True

        except Exception as e:
            logger.exception(
                "balance_fetch_error",
                component="FETCH_BALANCES",
                exchange_id=exchange_id,
                error_type=type(e).__name__,
                error=str(e),
                message=f"[FETCH_BALANCES:{exchange_id}] Error during balance fetch: {e}",
            )
            return False

    def _process_balances_data(
        self,
        exchange_id: str,
        balances_data: dict[str, SpotBalance],
    ) -> dict[str, SpotBalance]:
        """Process raw balances data and filter valid balances."""
        updated_balances: dict[str, SpotBalance] = {}

        # If balances_data is empty dict
        if not balances_data:  # Empty dict received
            logger.info(
                "empty_balances_received",
                exchange_id=exchange_id,
                action="processing_empty_dict",
                message=f"[{exchange_id}] API returned an empty dictionary of balances.",
            )
            return updated_balances

        for asset, balance_obj in balances_data.items():
            if balance_obj.exchange == exchange_id:
                updated_balances[asset] = balance_obj
            else:
                logger.warning(
                    "balance_exchange_id_mismatch",
                    expected_exchange_id=exchange_id,
                    received_exchange_id=balance_obj.exchange,
                    asset=asset,
                    action="skipping_balance",
                    message=(
                        f"[{exchange_id}] Skipping balance for asset {asset} due to "
                        f"mismatched exchange ID ({balance_obj.exchange}) in received "
                        f"SpotBalance object (from dict)."
                    ),
                )

        return updated_balances

    async def _update_balances_state(
        self,
        exchange_id: str,
        updated_balances: dict[str, SpotBalance],
    ) -> None:
        """Update internal balances state with new data."""
        async with self._lock:
            current_assets_for_exchange = set(self.balances[exchange_id].keys())
            newly_updated_asset_symbols = set(updated_balances.keys())

            # Remove stale assets
            assets_to_remove = current_assets_for_exchange - newly_updated_asset_symbols
            self._remove_stale_assets(exchange_id, assets_to_remove)

            # Add/update balances from updated_balances
            for asset, balance in updated_balances.items():
                self.balances[exchange_id][asset] = balance  # Corrected access
            self.last_update_time[exchange_id] = datetime.now(UTC)

        logger.info(
            "balances_updated_successfully",
            component="FETCH_BALANCES",
            exchange_id=exchange_id,
            updated_count=len(updated_balances),
            removed_count=len(assets_to_remove),
            updated_assets=list(updated_balances.keys()),
            removed_assets=list(assets_to_remove),
            message=(
                f"[FETCH_BALANCES:{exchange_id}] Balances updated successfully "
                f"with {len(updated_balances)} items. Assets removed: {len(assets_to_remove)}."
            ),
        )

    def _remove_stale_assets(self, exchange_id: str, assets_to_remove: set[str]) -> None:
        """Remove stale assets from balances."""
        for asset_to_remove in assets_to_remove:
            # Ensure asset actually exists before trying to delete to avoid
            # KeyError if logic is imperfect
            # Check against the inner dict for the specific exchange_id
            if asset_to_remove in self.balances[exchange_id]:
                del self.balances[exchange_id][asset_to_remove]
                logger.debug(
                    "stale_balance_removed",
                    exchange_id=exchange_id,
                    asset=asset_to_remove,
                    action="removed_stale_balance",
                    message=f"[{exchange_id}] Removed stale balance for asset {asset_to_remove}.",
                )

    def _handle_empty_balances(
        self,
        exchange_id: str,
        balances_data: dict[str, SpotBalance],
    ) -> None:
        """Handle the case when no valid balances were processed."""
        # This implies that balances_data was not None,
        # and if it was a list or dict, it was empty,
        # and no balances were processed into updated_balances.
        # If balances_data was some other unexpected type,
        # it would have been caught by an earlier `else`
        # or the initial type hint for `client.get_balances()` would be violated.
        logger.warning(
            "no_valid_balances_processed",
            component="FETCH_BALANCES",
            exchange_id=exchange_id,
            data_type=type(balances_data).__name__,
            action="handling_empty_balances",
            message=(
                f"[FETCH_BALANCES:{exchange_id}] No valid balances processed "
                f"or API returned empty data. Type: {type(balances_data)}"
            ),
        )
        # Not returning False here, as an empty (but valid) response is not an error.

    def _parse_balance_info(
        self,
        exchange_id: str,
        asset: str,
        balance_info: dict[str, Any] | SpotBalance,
    ) -> SpotBalance | None:
        """Parse balance information into a SpotBalance object."""
        if isinstance(balance_info, SpotBalance):
            if balance_info.exchange != exchange_id:
                logger.error(
                    "spotbalance_exchange_id_mismatch",
                    expected_exchange_id=exchange_id,
                    actual_exchange_id=balance_info.exchange,
                    asset=asset,
                    action="returning_none",
                    message=(
                        f"Mismatched exchange ID in provided SpotBalance object: "
                        f"expected {exchange_id}, got {balance_info.exchange}"
                    ),
                )
                return None
            return balance_info

        # Since the type hint is dict[str, Any] | SpotBalance, and SpotBalance is handled above,
        # balance_info must be a dict here.

        # Construct the data dictionary for SpotBalance, adding the exchange
        balance_data = balance_info.copy()
        balance_data["exchange"] = exchange_id
        balance_data["asset"] = asset  # Ensure asset is explicitly set

        try:
            # Validate and create the SpotBalance object
            # Pydantic handles parsing 'total', 'available' from str/int/float via validators
            parsed = SpotBalance(**balance_data)
            # Additional runtime checks (redundant with Pydantic ge=0 but defensive)
            if parsed.total_quantity < Decimal(0) or parsed.available_quantity < Decimal(0):
                logger.error(
                    "negative_balance_values",
                    exchange_id=exchange_id,
                    asset=asset,
                    total_quantity=float(parsed.total_quantity),
                    available_quantity=float(parsed.available_quantity),
                    action="returning_none",
                    message=(
                        f"Parsed balance has negative values: "
                        f"Total={parsed.total_quantity}, Available={parsed.available_quantity}"
                    ),
                )
                return None
            return parsed
        except (ValidationError, TypeError, InvalidOperation) as e:
            logger.exception(
                "balance_parse_error",
                exchange_id=exchange_id,
                asset=asset,
                error_type=type(e).__name__,
                error=str(e),
                action="returning_none",
                message=f"Failed to parse balance for {asset} on {exchange_id}: {e}",
            )
            return None

    @staticmethod
    def _safe_decimal_convert(
        value: str | Decimal | float | None,
        field_name: str,
        asset: str,
        exchange_id: str,
    ) -> Decimal | None:
        """Safely convert a value to Decimal, logging errors."""
        if value is None:
            return None
        try:
            # Handle potential scientific notation strings from some APIs
            if isinstance(value, str) and ("e" in value or "E" in value):
                return Decimal(value)
                # Optional: Convert very small numbers near zero to actual zero if desired
                # if abs(dec_value) < Decimal('1e-18'): # Adjust threshold as needed
            if isinstance(value, Decimal):
                return value
            return Decimal(str(value))  # Convert via string for precision
        except (InvalidOperation, ValueError, TypeError) as e:
            logger.error(
                "decimal_conversion_failed",
                field_name=field_name,
                value=value,
                value_type=type(value).__name__,
                asset=asset,
                exchange_id=exchange_id,
                error=str(e),
                message="Failed to convert value to Decimal",
            )
            return None

    async def _fetch_exchange_positions(self, exchange_id: str) -> bool:
        """Fetch and update positions for a specific exchange."""
        client = self.api_clients.get(exchange_id)
        if not client:
            logger.error(
                "no_api_client_for_fetch_positions",
                exchange_id=exchange_id,
                action="fetch_positions",
                error="no_api_client_registered",
                message=f"No API client registered for {exchange_id} in fetch_positions",
            )
            return False
        try:
            positions_data_raw = await client.get_positions()  # -> list[DerivativePosition]

            # positions_data_raw is guaranteed to be a list by the ExchangeAPI interface
            processed_positions_list = positions_data_raw

            updated_positions: dict[str, DerivativePosition] = {}
            for position_info in processed_positions_list:
                # Use symbol as the key for now
                # TODO: Revisit position identification strategy
                pos_key = position_info.symbol
                if pos_key:
                    # Assume position_info is already a validated DerivativePosition
                    # No need for _safe_decimal_convert if API layer provides validated models
                    updated_positions[pos_key] = position_info
                else:
                    logger.warning(
                        "position_missing_symbol",
                        exchange_id=exchange_id,
                        position_info=str(position_info),
                        message="Skipping DerivativePosition object without symbol",
                    )

            # This completely replaces the inner dict for the exchange_id
            self.positions[exchange_id] = defaultdict(
                lambda: DerivativePosition(
                    exchange="",
                    symbol="",
                    side=OrderSide.BUY,
                    size=Decimal(0),
                    entry_price=None,
                    timestamp=datetime.min.replace(tzinfo=UTC),
                ),
                updated_positions,
            )
            self.last_update_time[exchange_id] = datetime.now(UTC)
            logger.debug(
                "positions_updated_successfully",
                exchange_id=exchange_id,
                position_count=len(updated_positions),
                message="Successfully updated positions",
            )
            return True
        except Exception as e:
            logger.exception(
                "position_fetch_failed",
                action="fetch_positions",
                exchange_id=exchange_id,
                error=str(e),
                message=f"Failed to fetch positions for {exchange_id}: {e}",
            )
            return False

    async def _fetch_exchange_orders(self, exchange_id: str) -> bool:
        """Fetch and update open orders for a specific exchange."""
        try:
            client = self.api_clients.get(exchange_id)
            if not client:
                logger.error(
                    "no_api_client_found",
                    exchange_id=exchange_id,
                    action="reconcile_portfolio",
                    error="no_api_client_found",
                    message=f"No API client found for {exchange_id}",
                )
                return False
            # ExchangeAPI.get_open_orders returns list[Order], Pyright should infer this.
            orders_data: list[Order] = await client.get_open_orders()
            updated_orders: dict[str, Order] = {}
            for order_info in orders_data:
                order_instance = None
                order_id = None
                # order_info is always Order
                order_instance = order_info
                order_id = order_instance.client_order_id
                if order_instance and order_id:
                    order_instance.price = self._safe_decimal_convert(
                        order_instance.price,
                        "price",
                        order_instance.symbol,
                        exchange_id,
                    )
                    order_instance.quantity_requested = self._safe_decimal_convert(
                        order_instance.quantity_requested,
                        "quantity_requested",
                        order_instance.symbol,
                        exchange_id,
                    ) or Decimal(0)
                    order_instance.quantity_filled = self._safe_decimal_convert(
                        order_instance.quantity_filled,
                        "quantity_filled",
                        order_instance.symbol,
                        exchange_id,
                    ) or Decimal(0)
                    if isinstance(order_instance.status, str):
                        try:
                            order_instance.status = OrderStatus(order_instance.status)
                        except ValueError:
                            logger.warning(
                                "invalid_order_status",
                                order_id=order_id,
                                status=order_instance.status,
                                message="Invalid status string for order",
                            )
                            order_instance.status = OrderStatus.UNKNOWN
                    updated_orders[str(order_id)] = order_instance
            self.orders[exchange_id] = updated_orders  # This replaces the inner dict
            self.last_update_time[exchange_id] = datetime.now(UTC)
            logger.debug(
                "orders_updated_successfully",
                exchange_id=exchange_id,
                order_count=len(updated_orders),
                message="Successfully updated open orders",
            )
            return True
        except Exception as e:
            logger.exception(
                "order_fetch_error",
                action="fetch_orders",
                exchange_id=exchange_id,
                error=str(e),
                message=f"Unexpected error fetching orders for {exchange_id}: {e}",
            )
            return False

    async def update(self) -> None:
        """Update portfolio state by fetching data from exchanges."""
        now = datetime.now(UTC)
        update_tasks: list[Awaitable[Any]] = []
        for exchange_id, client in self.api_clients.items():
            if not self.app_settings.exchanges[exchange_id].enabled:
                continue
            last_reconciliation = self.last_reconciliation_time.get(
                exchange_id,
                datetime.min.replace(tzinfo=UTC),
            )
            needs_reconciliation = (
                now - last_reconciliation
            ).total_seconds() >= self.reconciliation_interval
            if needs_reconciliation:
                logger.info(
                    "reconciliation_needed",
                    exchange_id=exchange_id,
                    action="reconcile_portfolio",
                    data_scope="all_data",
                    message=f"Reconciliation needed for {exchange_id}. Fetching all data.",
                )
                update_tasks.extend((
                    self._fetch_exchange_account_summary(client, exchange_id),
                    self._fetch_exchange_balances(exchange_id),
                    self._fetch_exchange_positions(exchange_id),
                    self._fetch_exchange_orders(exchange_id),
                ))
                self.last_reconciliation_time[exchange_id] = now
            else:
                logger.debug(
                    "fetching_orders_only",
                    exchange_id=exchange_id,
                    action="reconcile_portfolio",
                    data_scope="orders_only",
                    reason="no_reconciliation_needed",
                    message=f"Fetching only orders for {exchange_id} (no reconciliation needed).",
                )
                update_tasks.append(self._fetch_exchange_orders(exchange_id))
        if update_tasks:
            results: list[bool | BaseException] = await asyncio.gather(
                *update_tasks,
                return_exceptions=True,
            )
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(
                        "portfolio_update_task_error",
                        task_index=i,
                        error=str(result),
                        message="Error during portfolio update task",
                        exc_info=result,
                    )
                elif result is False:
                    logger.warning(
                        "portfolio_update_task_failed",
                        task_index=i,
                        message="Portfolio update task indicated failure",
                    )
        current_capital = await self.get_total_capital()
        self.high_watermark = max(self.high_watermark, current_capital)

    def update_order(self, exchange_id: str, order: Order) -> None:
        """Update the status of a single order.

        Args:
            exchange_id: Exchange identifier
            order: Order object with updated information

        """
        # self.orders is defaultdict(dict), so exchange_id will be auto-created if missing
        if not order.client_order_id:
            logger.error(
                "order_update_missing_client_order_id",
                exchange_id=exchange_id,
                order=order.model_dump() if hasattr(order, "model_dump") else str(order),
                action="update_order",
                error="missing_client_order_id",
                message=f"Received order update without client_order_id on {exchange_id}: {order}",
            )
            return
        order_id_str = str(order.client_order_id)
        self.orders[exchange_id][order_id_str] = order
        self.last_update_time[exchange_id] = datetime.now(UTC)  # Mark update for this exchange

        # Handle order state transitions
        # In particular, we want to detect when an order reaches FILLED or PARTIALLY_FILLED
        # and generate a corresponding trade for the position tracker
        if order.status in {
            OrderStatus.FILLED,
            OrderStatus.PARTIALLY_FILLED,
        } and order.quantity_filled > Decimal(0):
            logger.info(
                "order_filled_trigger_trade",
                order_id=order_id_str,
                exchange_id=exchange_id,
                status=order.status.value if hasattr(order.status, "value") else str(order.status),
                message="Order filled, triggering trade processing",
            )

    def update_position(self, exchange_id: str, position: DerivativePosition) -> None:
        """Update the state of a single position.

        Args:
            exchange_id: Exchange identifier
            position: DerivativePosition object with updated information

        """
        # self.positions is defaultdict(defaultdict), so exchange_id will be auto-created
        pos_key = position.symbol
        if not pos_key:
            logger.error(
                "position_update_missing_symbol",
                exchange_id=exchange_id,
                position=position.model_dump()
                if hasattr(position, "model_dump")
                else str(position),
                action="update_position",
                error="missing_symbol",
                message=f"Received position update without symbol on {exchange_id}: {position}",
            )
            return

        self.positions[exchange_id][pos_key] = position
        self.last_update_time[exchange_id] = datetime.now(UTC)
        logger.debug(
            "position_updated",
            position_key=pos_key,
            exchange_id=exchange_id,
            action="update_position",
            message=f"Updated position {pos_key} for {exchange_id}",
        )

    async def process_trade(self, exchange_id: str, trade: Trade) -> None:
        """Process a trade execution and update relevant portfolio state.

        Updates positions, balances (placeholder, needs full implementation),
        and realized P&L based on the executed trade.

        Args:
            exchange_id: The exchange where the trade occurred.
            trade: The Trade object.

        """
        if not self._validate_trade(exchange_id, trade):
            return

        base_symbol = self._get_base_symbol(exchange_id, trade)

        logger.info(
            "trade_processing",
            exchange_id=exchange_id,
            side=trade.side.value if hasattr(trade.side, "value") else str(trade.side),
            quantity=str(trade.quantity),
            base_symbol=base_symbol,
            original_symbol=trade.symbol,
            price=str(trade.price),
            message="Processing trade",
        )

        await self._update_position_from_trade(exchange_id, trade, base_symbol)

        # Placeholder: Balance update logic
        self._update_balances_from_trade(exchange_id, trade)
        logger.debug(
            "placeholder_balance_update_for_trade_process",
            trade_id=trade.id,
            exchange_id=exchange_id,
            action="update_balances_for_trade",
            status="placeholder",
            context="process_trade",
            message=f"Placeholder: Update balances for trade {trade.id} on {exchange_id}",
        )

    def _validate_trade(self, exchange_id: str, trade: Trade) -> bool:
        """Validate trade data before processing."""
        if not trade or not trade.symbol or not trade.quantity or trade.quantity <= Decimal(0):
            logger.warning(
                "ignoring_invalid_trade",
                exchange_id=exchange_id,
                trade_id=trade.id if hasattr(trade, "id") else None,
                trade_quantity=float(trade.quantity) if hasattr(trade, "quantity") else None,
                action="process_trade",
                issue="invalid_or_zero_quantity",
                message=f"Ignoring invalid or zero-quantity trade on {exchange_id}: {trade}",
            )
            return False
        return True

    def _get_base_symbol(self, exchange_id: str, trade: Trade) -> str:
        """Get the base symbol for the trade."""
        # Ensure symbol_mapper is available
        if not hasattr(self, "symbol_mapper") or self.symbol_mapper is None:
            logger.error("SymbolMapper not initialized in PortfolioTracker. Cannot process trade.")
            # Attempt to get it from config if possible, or raise
            # This indicates a setup issue if self.symbol_mapper is None.
            # For now, proceed with a basic fallback for base_symbol if absolutely necessary,
            # but this should be fixed by ensuring proper initialization.
            base_symbol = trade.symbol.split("-")[0].split("/")[0]  # Basic fallback
            logger.warning(
                "symbol_mapper_missing_fallback",
                base_symbol=base_symbol,
                message="SymbolMapper missing, using basic fallback for base symbol",
            )
        else:
            base_symbol = (
                self.symbol_mapper.get_internal_symbol(exchange_id, trade.symbol)
                or trade.symbol.split("-")[0].split("/")[0]
            )
        return base_symbol

    async def _update_position_from_trade(
        self,
        exchange_id: str,
        trade: Trade,
        base_symbol: str,
    ) -> None:
        """Update position based on trade execution."""

        async def _update_position_async() -> None:
            async with self._lock:
                current_position = self.positions[exchange_id].get(base_symbol)
                trade_price = trade.price
                trade_quantity = trade.quantity
                trade_side = trade.side
                trade_timestamp = trade.executed_at  # USE executed_at

                if not current_position or current_position.size == Decimal(0):
                    self._open_new_position(
                        exchange_id,
                        base_symbol,
                        trade_side,
                        trade_quantity,
                        trade_price,
                        trade_timestamp,
                    )
                else:
                    self._modify_existing_position(
                        current_position,
                        trade,
                        base_symbol,
                        exchange_id,
                    )

        await _update_position_async()

    def _open_new_position(
        self,
        exchange_id: str,
        base_symbol: str,
        trade_side: OrderSide,
        trade_quantity: Decimal,
        trade_price: Decimal,
        trade_timestamp: datetime,
    ) -> None:
        """Open a new position."""
        logger.debug(
            "position_opening_new",
            base_symbol=base_symbol,
            exchange_id=exchange_id,
            message="Opening new position",
        )
        new_pos = DerivativePosition(
            exchange=exchange_id,
            symbol=base_symbol,
            side=trade_side,
            size=trade_quantity,
            entry_price=trade_price,
            timestamp=trade_timestamp,  # Uses corrected trade_timestamp
            mark_price=trade_price,  # Initial mark price
        )
        self.positions[exchange_id][base_symbol] = new_pos
        self.active_symbols.add(base_symbol)

    def _modify_existing_position(
        self,
        current_position: DerivativePosition,
        trade: Trade,
        base_symbol: str,
        exchange_id: str,
    ) -> None:
        """Modify an existing position based on trade."""
        logger.debug(
            "modifying_existing_position",
            base_symbol=base_symbol,
            exchange_id=exchange_id,
            trade_id=trade.id,
            current_size=current_position.size,
            current_side=current_position.side,
            message="Modifying existing position from trade",
        )

        if current_position.side == trade.side:
            self._increase_position(current_position, trade, base_symbol)
        else:
            self._decrease_or_flip_position(current_position, trade, base_symbol)

        # Update position metadata
        current_position.timestamp = trade.executed_at
        current_position.mark_price = trade.price  # Update mark price to last trade price
        self.positions[exchange_id][base_symbol] = current_position

    def _increase_position(
        self,
        current_position: DerivativePosition,
        trade: Trade,
        base_symbol: str,
    ) -> None:
        """Increase an existing position."""
        logger.debug(
            "position_increasing",
            base_symbol=base_symbol,
            trade_quantity=float(trade.quantity),
            action="increase_position",
            message=f"Increasing position for {base_symbol}. Trade qty: {trade.quantity}",
        )

        current_entry_price = current_position.entry_price or Decimal(0)

        if current_position.size < Decimal(0) and trade.side == OrderSide.SELL:
            # Increasing short
            new_avg_price = (
                (abs(current_position.size) * current_entry_price) + (trade.quantity * trade.price)
            ) / (abs(current_position.size) + trade.quantity)
            current_position.size -= trade.quantity
        elif current_position.size > Decimal(0) and trade.side == OrderSide.BUY:
            # Increasing long
            new_avg_price = (
                (current_position.size * current_entry_price) + (trade.quantity * trade.price)
            ) / (current_position.size + trade.quantity)
            current_position.size += trade.quantity
        else:
            logger.error(
                "logical_error_increasing_position",
                base_symbol=base_symbol,
                current_size=current_position.size,
                current_side=current_position.side,
                trade_quantity=trade.quantity,
                trade_side=trade.side,
                message="Logical error in increasing position",
            )
            new_avg_price = trade.price  # Fallback

        current_position.entry_price = new_avg_price
        logger.debug(
            "position_increased",
            base_symbol=base_symbol,
            new_size=current_position.size,
            new_avg_entry=current_position.entry_price,
            message="Position increased with new size and average entry price",
        )

    def _decrease_or_flip_position(
        self,
        current_position: DerivativePosition,
        trade: Trade,
        base_symbol: str,
    ) -> None:
        """Decrease or flip an existing position."""
        logger.debug(
            "reducing_or_flipping_position",
            base_symbol=base_symbol,
            trade_quantity=trade.quantity,
            message="Reducing or flipping position",
        )

        # Calculate PNL on the portion of the position affected by this trade
        qty_affected = min(trade.quantity, abs(current_position.size))
        current_entry_price = current_position.entry_price or Decimal(0)

        self._calculate_realized_pnl(current_position, trade, qty_affected, current_entry_price)

        self._update_position_size(current_position, trade, base_symbol)

    def _calculate_realized_pnl(
        self,
        current_position: DerivativePosition,
        trade: Trade,
        qty_affected: Decimal,
        current_entry_price: Decimal,
    ) -> Decimal:
        """Calculate realized PnL for the trade."""
        realized_pnl_for_this_trade = Decimal(0)

        if current_entry_price != Decimal(0):  # Avoid PNL calc if entry was 0
            if current_position.side == OrderSide.BUY:  # Closing/reducing a long
                realized_pnl_for_this_trade = (trade.price - current_entry_price) * qty_affected
            else:  # Closing/reducing a short
                realized_pnl_for_this_trade = (current_entry_price - trade.price) * qty_affected

            self._update_realized_pnl(realized_pnl_for_this_trade)
            current_position.realized_pnl = (
                current_position.realized_pnl or Decimal(0)
            ) + realized_pnl_for_this_trade
            logger.info(
                "trade_realized_pnl_calculated",
                trade_id=trade.id,
                symbol=trade.symbol,
                realized_pnl=realized_pnl_for_this_trade,
                position_realized_pnl=current_position.realized_pnl,
                message="Realized PNL calculated for trade",
            )

        return realized_pnl_for_this_trade

    def _update_position_size(
        self,
        current_position: DerivativePosition,
        trade: Trade,
        base_symbol: str,
    ) -> None:
        """Update position size based on trade."""
        if trade.quantity < abs(current_position.size):
            # Reducing position, not closing or flipping
            logger.debug(
                "position_reducing",
                base_symbol=base_symbol,
                action="reduce_position",
                message=f"Reducing position for {base_symbol}",
            )
            if current_position.side == OrderSide.BUY:
                current_position.size -= trade.quantity
            else:  # SELL side
                current_position.size += trade.quantity
            # Entry price remains the same

        elif trade.quantity == abs(current_position.size):
            # Closing position to flat
            logger.debug(
                "position_closing_to_flat",
                base_symbol=base_symbol,
                action="close_position",
                status="flat",
                message=f"Closing position for {base_symbol} to flat.",
            )
            current_position.size = Decimal(0)
            current_position.entry_price = None  # Flat position has no entry price
            self.active_symbols.discard(base_symbol)  # Symbol might become inactive

        else:  # Flipping position (trade.quantity > abs(current_position.size))
            remaining_qty = trade.quantity - abs(current_position.size)
            current_position.size = remaining_qty if trade.side == OrderSide.BUY else -remaining_qty
            current_position.side = trade.side
            current_position.entry_price = trade.price  # Entry price for the new portion
            logger.debug(
                "position_flipping",
                base_symbol=base_symbol,
                remaining_quantity=remaining_qty,
                message="Flipping position after closing",
            )

    def _update_balances_from_trade(self, exchange_id: str, trade: Trade) -> None:
        # Placeholder for balance update logic based on trade details
        logger.debug(
            "placeholder_balance_update_for_trade_method",
            trade_id=trade.id,
            exchange_id=exchange_id,
            action="update_balances_from_trade",
            status="placeholder",
            context="_update_balances_from_trade_method",
            message=f"Placeholder: Update balances for trade {trade.id} on {exchange_id}",
        )

    def _update_realized_pnl(self, amount: Decimal) -> None:
        """Update the total realized PNL."""
        if not amount.is_finite():  # Check finiteness directly
            logger.error(
                "realized_pnl_update_invalid_amount",
                amount=str(amount),
                action="update_realized_pnl",
                error="invalid_amount",
                message=f"Attempted to update realized PNL with invalid amount: {amount}",
            )
            return
        self.realized_pnl += amount
        logger.info(
            "realized_pnl_updated",
            amount_delta=float(amount),
            new_total=float(self.realized_pnl),
            action="update_realized_pnl",
            message=f"Realized PNL updated by {amount:.4f}. New total: {self.realized_pnl:.4f}",
        )

    # --- Position Access Methods ---
    def get_position(self, exchange_id: str, symbol: str) -> DerivativePosition | None:
        """Return the position for a specific symbol on a specific exchange."""
        if exchange_id not in self.positions:
            return None
        return self.positions[exchange_id].get(symbol)  # Direct access to inner dict

    def get_positions_by_symbol(self, exchange_id: str, symbol: str) -> list[DerivativePosition]:
        """Retrieve all positions for a specific symbol on a given exchange."""
        if exchange_id not in self.positions:
            return []
        exchange_positions = self.positions[exchange_id]  # Direct access to inner dict
        # Iterate over values() as key is unused
        return [pos for pos in exchange_positions.values() if pos.symbol == symbol]

    def get_all_positions(self) -> Sequence[tuple[str, DerivativePosition]]:
        """Retrieve all derivative positions across all exchanges.

        Conforms to PortfolioTrackerProtocol where DerivativePosition implements
        Position protocol implicitly.

        Returns:
            Sequence of tuples containing (exchange_id, position) pairs.

        """
        all_positions_list: list[tuple[str, DerivativePosition]] = []
        for exchange_id, symbol_positions_map in self.positions.items():
            for _symbol, position_obj in symbol_positions_map.items():
                # Only include positions with non-zero size
                if position_obj.size != Decimal(0):
                    all_positions_list.append((exchange_id, position_obj))
        return all_positions_list

    def get_positions_by_exchange(self, exchange_id: str) -> list[DerivativePosition]:
        """Get all positions for a specific exchange."""
        if exchange_id not in self.positions:
            logger.warning(
                "get_positions_unknown_exchange",
                exchange_id=exchange_id,
                action="get_positions_by_exchange",
                issue="unknown_exchange",
                message=f"Attempted to get positions for unknown exchange: {exchange_id}",
            )
            return []
        # Filter out placeholders
        return [pos for pos in self.positions[exchange_id].values() if pos.size != Decimal(0)]

    async def get_total_capital(self, base_currency: str = "USDC") -> Decimal:
        """Calculate the total portfolio capital in the specified base currency.

        Args:
            base_currency: Currency to calculate total capital in (default: USDC).

        Returns:
            Total portfolio value as a Decimal.

        """
        logger.debug(
            "calculating_total_capital",
            base_currency=base_currency,
            action="get_total_capital",
            message=f"Calculating total capital in {base_currency}...",
        )
        total_value = Decimal("0.0")

        # 1. Calculate value of all spot balances
        for exchange_id, balances in self.balances.items():
            for asset, balance in balances.items():
                # Defensive check for zero quantity
                # (is None check removed as total_quantity is not Optional)
                if balance.total_quantity == Decimal(0):
                    continue

                price = await self._get_asset_price_in_base(exchange_id, asset, base_currency)
                if price is not None:
                    try:
                        asset_value = balance.total_quantity * price
                        total_value += asset_value
                        logger.debug(
                            "spot_balance_calculated",
                            exchange_id=exchange_id,
                            asset=asset,
                            total_quantity=balance.total_quantity,
                            price=price,
                            base_currency=base_currency,
                            asset_value=asset_value,
                        )
                    except (TypeError, InvalidOperation) as e:
                        logger.error(
                            "spot_balance_value_calculation_error",
                            asset=asset,
                            exchange_id=exchange_id,
                            error=str(e),
                        )
                else:
                    logger.warning(
                        "spot_asset_price_unavailable",
                        asset=asset,
                        exchange_id=exchange_id,
                        base_currency=base_currency,
                        message=(
                            "Could not determine price for spot asset. "
                            "Skipping in capital calculation."
                        ),
                    )

        # 2. Calculate the equity value of all derivative positions
        # This is simplified: Assumes margin is held in base_currency and PnL reflects value.
        # A more accurate calculation might need margin details per exchange.
        # We add unrealized PnL here as a proxy for position value change.
        _realized, unrealized = await self.get_pnl(base_currency)
        total_value += unrealized
        logger.debug(
            "adding_unrealized_pnl_to_capital",
            unrealized_pnl=float(unrealized),
            base_currency=base_currency,
            action="calculate_total_capital",
            message=f"  Adding total unrealized PNL to capital: {unrealized} {base_currency}",
        )

        # Update high watermark
        if total_value.is_finite() and total_value > self.high_watermark:
            self.high_watermark = total_value
            logger.debug(
                "new_high_watermark_reached",
                high_watermark=float(self.high_watermark),
                action="update_high_watermark",
                message=f"New high watermark reached: {self.high_watermark}",
            )

        logger.info(
            "total_portfolio_capital_calculated",
            total_value=float(total_value),
            base_currency=base_currency,
            action="get_total_capital",
            message=f"Total portfolio capital calculated: {total_value} {base_currency}",
        )
        return total_value if total_value.is_finite() else Decimal("0.0")

    async def get_exchange_exposure(
        self,
        exchange_id: str,
        valuation_asset: str = "USDC",
    ) -> Decimal:
        """Calculate the total market exposure for a given exchange in a valuation asset."""
        logger.debug(
            "calculating_exchange_exposure",
            exchange_id=exchange_id,
            valuation_asset=valuation_asset,
            action="get_exchange_exposure",
            message=f"Calculating exposure for {exchange_id} in {valuation_asset}...",
        )
        exchange_exposure = Decimal("0.0")
        positions = self.positions[exchange_id]  # Direct access to inner dict

        for position_key, position in positions.items():
            # Defensive checks (is None checks removed as size/symbol are not Optional)
            if position.size == Decimal(0):
                continue  # Skip zero size positions

            # Get current market price
            mark_price = await self._get_asset_price_in_base(
                exchange_id,
                position.symbol,
                valuation_asset,
            )

            if mark_price is not None and mark_price.is_finite():
                try:
                    # Use absolute value of size for exposure calculation
                    position_value = abs(position.size) * mark_price
                    exchange_exposure += position_value
                    logger.debug(
                        "position_exposure_calculated",
                        exchange_id=exchange_id,
                        symbol=position.symbol,
                        size=position.size,
                        mark_price=mark_price,
                        valuation_asset=valuation_asset,
                        position_value=position_value,
                    )
                except (TypeError, InvalidOperation) as e:
                    logger.error(
                        "position_value_calculation_error",
                        position_key=position_key,
                        symbol=position.symbol,
                        exchange_id=exchange_id,
                        error=str(e),
                    )
            else:
                logger.warning(
                    "position_mark_price_unavailable",
                    position_key=position_key,
                    symbol=position.symbol,
                    exchange_id=exchange_id,
                    valuation_asset=valuation_asset,
                    message=(
                        "Could not determine mark price for position. "
                        "Skipping in exposure calculation."
                    ),
                )

        logger.info(
            "total_exchange_exposure_calculated",
            exchange_id=exchange_id,
            exchange_exposure=float(exchange_exposure),
            valuation_asset=valuation_asset,
            action="get_exchange_exposure",
            message=f"Total exposure for {exchange_id}: {exchange_exposure} {valuation_asset}",
        )
        return exchange_exposure if exchange_exposure.is_finite() else Decimal("0.0")

    async def get_total_exposure_usd(self, valuation_asset: str = "USDC") -> Decimal:
        """Calculate the total market exposure across all exchanges."""
        logger.debug(
            "calculating_total_exposure_all_exchanges",
            valuation_asset=valuation_asset,
            action="get_total_exposure_usd",
            message=f"Calculating total exposure across all exchanges in {valuation_asset}...",
        )
        total_exposure = Decimal("0.0")
        for exchange_id in self.api_clients:
            if self.app_settings.exchanges[exchange_id].enabled:
                total_exposure += await self.get_exchange_exposure(exchange_id, valuation_asset)

        logger.info(
            "total_portfolio_exposure_calculated",
            total_exposure=float(total_exposure),
            valuation_asset=valuation_asset,
            action="get_total_exposure_usd",
            message=f"Total portfolio exposure calculated: {total_exposure} {valuation_asset}",
        )
        return total_exposure if total_exposure.is_finite() else Decimal("0.0")

    async def get_pnl(self, base_currency: str = "USDC") -> tuple[Decimal, Decimal]:
        """Calculate the total realized and unrealized PNL across all exchanges.

        Args:
            base_currency: The currency to report PNL in.

        Returns:
            A tuple containing (total_realized_pnl, total_unrealized_pnl).

        """
        logger.debug(
            "calculating_pnl",
            base_currency=base_currency,
            action="get_pnl",
            message=f"Calculating PNL in {base_currency}...",
        )
        total_unrealized_pnl = Decimal("0.0")
        total_realized_pnl = self.realized_pnl  # Start with globally tracked realized PNL

        for exchange_id, positions in self.positions.items():
            for position_key, position in positions.items():
                # Process realized PNL
                total_realized_pnl = await self._process_position_realized_pnl(
                    position,
                    position_key,
                    exchange_id,
                    base_currency,
                    total_realized_pnl,
                )

                # Process unrealized PNL
                unrealized_pnl = await self._calculate_position_unrealized_pnl(
                    position,
                    position_key,
                    exchange_id,
                    base_currency,
                )
                if unrealized_pnl is not None:
                    total_unrealized_pnl += unrealized_pnl

        # Ensure finite results
        finite_realized = total_realized_pnl if total_realized_pnl.is_finite() else Decimal("0.0")
        finite_unrealized = (
            total_unrealized_pnl if total_unrealized_pnl.is_finite() else Decimal("0.0")
        )

        logger.info(
            "total_pnl_calculated",
            realized_pnl=finite_realized,
            unrealized_pnl=finite_unrealized,
            base_currency=base_currency,
        )
        return finite_realized, finite_unrealized

    async def _process_position_realized_pnl(
        self,
        position: DerivativePosition,
        position_key: str,
        exchange_id: str,
        base_currency: str,
        total_realized_pnl: Decimal,
    ) -> Decimal:
        """Process realized PNL for a single position."""
        # Add position's own realized PNL if it's valid
        # Re-adding None check for safety, along with finiteness
        if position.realized_pnl is not None and position.realized_pnl.is_finite():
            pnl_quote_asset = (
                position.symbol.split("_")[-1]
                if "_" in position.symbol
                else position.symbol.split("-")[-1]
            )  # Simple guess
            conversion_rate = await self._get_asset_price_in_base(
                exchange_id,
                pnl_quote_asset,
                base_currency,
            )

            # Check conversion rate validity
            if conversion_rate is not None and conversion_rate.is_finite():
                try:
                    # Ensure we only add Decimal to Decimal
                    converted_pnl = position.realized_pnl * conversion_rate
                    if converted_pnl.is_finite():  # Final check before adding
                        total_realized_pnl += converted_pnl
                    else:
                        logger.warning(
                            "converted_realized_pnl_not_finite",
                            position_key=position_key,
                            converted_pnl=converted_pnl,
                            message="Converted realized PNL is not finite. Skipping addition.",
                        )
                except (TypeError, InvalidOperation) as e:
                    logger.error(
                        "realized_pnl_conversion_error",
                        position_key=position_key,
                        error=str(e),
                    )
            else:
                logger.warning(
                    "cannot_convert_realized_pnl",
                    position_key=position_key,
                    exchange_id=exchange_id,
                    base_currency=base_currency,
                    realized_pnl=position.realized_pnl,
                    conversion_rate=conversion_rate,
                )
        elif position.realized_pnl is not None:  # Log if it exists but isn't finite
            logger.warning(
                "position_realized_pnl_not_finite",
                position_key=position_key,
                realized_pnl=position.realized_pnl,
                message="Position realized PNL is not finite",
            )

        return total_realized_pnl

    async def _calculate_position_unrealized_pnl(
        self,
        position: DerivativePosition,
        position_key: str,
        exchange_id: str,
        base_currency: str,
    ) -> Decimal | None:
        """Calculate unrealized PNL for a single position."""
        # DEFENSIVE CHECK: Check entry_price is not None *before* size check
        # because a non-zero size *requires* a non-None entry_price (model validation)
        if position.size == Decimal(0) or position.entry_price is None:
            logger.debug(
                "skipping_unrealized_pnl_calc",
                position_key=position_key,
                exchange_id=exchange_id,
                size=position.size,
                entry_price=position.entry_price,
                message="Skipping unrealized PNL calc due to zero size or missing entry price",
            )
            return None

        # Get prices in base currency
        mark_price_in_base, entry_price_in_base = await self._get_position_prices_in_base(
            position,
            exchange_id,
            base_currency,
        )

        # Calculate unrealized PNL if possible
        if self._can_calculate_unrealized_pnl(mark_price_in_base, entry_price_in_base):
            # DEFENSIVE CHECK: Type narrowing for mypy. Mypy=[unreachable] Ruff=[]
            if mark_price_in_base is not None and entry_price_in_base is not None:
                return self._compute_unrealized_pnl(
                    position,
                    mark_price_in_base,
                    entry_price_in_base,
                    exchange_id,
                )
        else:
            logger.warning(
                "skipping_unrealized_pnl_invalid_prices",
                position_key=position_key,
                symbol=position.symbol,
                exchange_id=exchange_id,
                mark_price_base=mark_price_in_base,
                entry_price_base=entry_price_in_base,
                message=(
                    "Skipping unrealized PNL calculation due to missing/invalid converted prices"
                ),
            )
            return None

        # Fallback return - should not reach here
        return None

    async def _get_position_prices_in_base(
        self,
        position: DerivativePosition,
        exchange_id: str,
        base_currency: str,
    ) -> tuple[Decimal | None, Decimal | None]:
        """Get mark price and entry price converted to base currency."""
        # 1. Get Mark Price in the requested Base Currency
        mark_price_in_base = await self._get_asset_price_in_base(
            exchange_id,
            position.symbol,
            base_currency,
        )

        # 2. Get Entry Price (which is in the Quote currency of the symbol)
        entry_price_in_quote = position.entry_price

        # 3. Determine the Quote Currency from the symbol
        quote_currency = self._extract_quote_currency(position.symbol)
        if quote_currency is None:
            logger.warning(
                "cannot_determine_quote_currency",
                symbol=position.symbol,
                message=(
                    "Cannot determine quote currency for symbol, "
                    "cannot calculate unrealized PNL accurately"
                ),
            )
            return mark_price_in_base, None

        # 4. Convert Entry Price from Quote Currency to Base Currency
        if entry_price_in_quote is not None:
            entry_price_in_base = await self._convert_entry_price_to_base(
                entry_price_in_quote,
                quote_currency,
                base_currency,
                exchange_id,
                position.symbol,
            )
        else:
            entry_price_in_base = None

        return mark_price_in_base, entry_price_in_base

    def _extract_quote_currency(self, symbol: str) -> str | None:
        """Extract quote currency from symbol."""
        if "-" in symbol:
            return symbol.split("-")[-1]
        if "_" in symbol:
            return symbol.split("_")[-1]
        return None

    async def _convert_entry_price_to_base(
        self,
        entry_price_in_quote: Decimal,
        quote_currency: str,
        base_currency: str,
        exchange_id: str,
        symbol: str,
    ) -> Decimal | None:
        """Convert entry price from quote currency to base currency."""
        if quote_currency == base_currency:
            return entry_price_in_quote

        # Need conversion rate from quote to base
        quote_to_base_rate = await self._get_asset_price_in_base(
            exchange_id,
            quote_currency,
            base_currency,
        )
        if quote_to_base_rate is not None:
            return entry_price_in_quote * quote_to_base_rate
        logger.warning(
            "cannot_convert_entry_price",
            symbol=symbol,
            quote_currency=quote_currency,
            base_currency=base_currency,
            message="Cannot convert entry price. Skipping unrealized PNL.",
        )
        return None

    def _can_calculate_unrealized_pnl(
        self,
        mark_price_in_base: Decimal | None,
        entry_price_in_base: Decimal | None,
    ) -> bool:
        """Check if unrealized PNL can be calculated."""
        return (
            mark_price_in_base is not None
            and entry_price_in_base is not None
            and mark_price_in_base.is_finite()
            and entry_price_in_base.is_finite()
        )

    def _compute_unrealized_pnl(
        self,
        position: DerivativePosition,
        mark_price_in_base: Decimal,
        entry_price_in_base: Decimal,
        exchange_id: str,
    ) -> Decimal | None:
        """Compute the unrealized PNL for a position."""
        try:
            # Unrealized PNL = Size * (Mark Price in Base - Entry Price in Base)
            unrealized_pnl = position.size * (mark_price_in_base - entry_price_in_base)
            logger.debug(
                "position_unrealized_pnl_calculated",
                exchange_id=exchange_id,
                symbol=position.symbol,
                size=position.size,
                entry_price_base=entry_price_in_base,
                mark_price_base=mark_price_in_base,
                unrealized_pnl=unrealized_pnl,
                message="Position unrealized PNL calculated",
            )
            return unrealized_pnl
        except (TypeError, InvalidOperation) as e:
            logger.error(
                "error_calculating_unrealized_pnl",
                symbol=position.symbol,
                exchange_id=exchange_id,
                error=str(e),
                message="Error calculating unrealized PNL",
            )
            return None

    async def get_current_drawdown(self, base_currency: str = "USDC") -> Decimal | None:
        """Calculate the current drawdown from the high watermark.

        Args:
            base_currency: Currency to calculate drawdown in (default: USDC).

        Returns:
            Current drawdown as a Decimal, or None if no high watermark exists.

        """
        current_capital = await self.get_total_capital()

        if self.high_watermark <= Decimal("0.0"):
            logger.warning("High watermark is not positive. Cannot calculate drawdown.")
            return Decimal("0.0")

        if not current_capital.is_finite() or current_capital <= Decimal("0.0"):
            logger.warning(
                "current_capital_invalid",
                current_capital=current_capital,
                message="Current capital is not positive or finite. Cannot calculate drawdown.",
            )
            return Decimal("0.0")

        drawdown = (self.high_watermark - current_capital) / self.high_watermark
        result = max(Decimal("0.0"), drawdown)
        logger.debug(
            "drawdown_calculated",
            high_watermark=self.high_watermark,
            current_capital=current_capital,
            drawdown=result,
        )
        return result

    # --- Order Access Methods ---

    def get_open_orders(self, exchange_id: str, symbol: str | None = None) -> list[Order]:
        """Get all open orders for a given exchange and optionally a symbol."""
        if exchange_id not in self.orders:
            return []

        exchange_orders_dict = self.orders[exchange_id]

        open_orders_collected: list[Order] = []
        for order in exchange_orders_dict.values():
            is_status_open = order.status.is_open()
            is_symbol_match = symbol is None or order.symbol == symbol

            if is_status_open and is_symbol_match:
                open_orders_collected.append(order)

        return open_orders_collected

    def get_order_history(self, exchange_id: str, symbol: str | None = None) -> list[Order]:
        """Get all orders (open and closed) for a given exchange and optionally a symbol."""
        all_orders: list[Order] = []
        exchange_orders = self.orders.get(exchange_id, {})
        for order in exchange_orders.values():
            if symbol is None or order.symbol == symbol:
                all_orders.append(order)
        return all_orders

    def get_order_by_id(self, exchange_id: str, order_id: str) -> Order | None:
        """Retrieve a specific order by its ID from the internal tracking.

        Args:
            exchange_id: The exchange the order belongs to.
            order_id: The unique identifier of the order.

        Returns:
            The Order object if found, otherwise None.

        """
        exchange_orders = self.orders.get(exchange_id, {})

        # First, try direct lookup using the provided order_id as a client_order_id
        order = exchange_orders.get(order_id)
        if order:
            return order

        # If not found, search for the order_id in the client_order_id format
        for client_order_id, order in exchange_orders.items():
            if client_order_id.endswith(f"-{order_id}"):
                return order

        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize the portfolio state to a dictionary suitable for JSON.

        Note: Returns dict containing Pydantic models. Serialization handled by encoder.
        """
        # Convert defaultdicts to dict for serialization if necessary,
        # though Pydantic's default_encoders might handle it.
        # For explicit control:
        balances_dict = {ex: dict(assets) for ex, assets in self.balances.items()}
        positions_dict = {ex: dict(syms) for ex, syms in self.positions.items()}
        orders_dict = {ex: dict(ords) for ex, ords in self.orders.items()}
        last_update_dict = dict(self.last_update_time)
        last_reconciliation_dict = dict(self.last_reconciliation_time)

        return {
            "balances": balances_dict,
            "positions": positions_dict,
            "orders": orders_dict,
            "last_update_time": last_update_dict,
            "last_reconciliation_time": last_reconciliation_dict,
            "high_watermark": self.high_watermark,
            "realized_pnl": self.realized_pnl,
            # Active symbols and watchlist could be added if needed for persistence
        }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
        app_settings: AppSettings,
        pt_config: PortfolioTrackerConfig,
    ) -> PortfolioTracker:
        """Deserialize the portfolio state from a dictionary."""
        tracker = cls(app_settings, pt_config)

        # Load each data section
        cls._load_balances_from_dict(tracker, data)
        cls._load_positions_from_dict(tracker, data)
        cls._load_orders_from_dict(tracker, data)
        cls._load_timestamps_from_dict(tracker, data)
        cls._load_scalar_fields_from_dict(tracker, data)

        logger.info("PortfolioTracker state loaded from dict (object deserialization attempted).")
        return tracker

    @classmethod
    def _load_balances_from_dict(cls, tracker: PortfolioTracker, data: dict[str, Any]) -> None:
        """Load balances data from dictionary."""
        balances_data_get = data.get("balances", {})
        if not isinstance(balances_data_get, dict):
            return

        balances_data_typed = cast("dict[str, Any]", balances_data_get)
        for ex_id_str, assets_dict_any in balances_data_typed.items():
            if not isinstance(assets_dict_any, dict):
                continue

            current_assets_items = cast("dict[str, Any]", assets_dict_any)
            for k_asset_raw, bal_data_any in current_assets_items.items():
                asset_str = str(k_asset_raw)
                cls._process_single_balance(tracker, ex_id_str, asset_str, bal_data_any)

    @classmethod
    def _process_single_balance(
        cls,
        tracker: PortfolioTracker,
        ex_id_str: str,
        asset_str: str,
        bal_data_any: dict[str, Any] | SpotBalance,
    ) -> None:
        """Process a single balance entry."""
        if isinstance(bal_data_any, dict):
            try:
                # Ensure keys are str for model_validate
                validated_bal_dict: dict[str, Any] = {str(k): v for k, v in bal_data_any.items()}
                tracker.balances[ex_id_str][asset_str] = SpotBalance.model_validate(
                    validated_bal_dict,
                )
            except ValidationError as e:
                logger.error(
                    "spot_balance_validation_error",
                    asset=asset_str,
                    exchange_id=ex_id_str,
                    error=str(e),
                )
            except Exception as e:  # Catch other potential errors from str(k)
                logger.warning(
                    "balance_processing_error",
                    exchange_id=ex_id_str,
                    asset=asset_str,
                    error=str(e),
                    context="from_dict",
                )
        else:  # bal_data_any is SpotBalance after type narrowing
            tracker.balances[ex_id_str][asset_str] = bal_data_any

    @classmethod
    def _load_positions_from_dict(cls, tracker: PortfolioTracker, data: dict[str, Any]) -> None:
        """Load positions data from dictionary."""
        positions_data_get = data.get("positions", {})
        if not isinstance(positions_data_get, dict):
            return

        positions_data_typed = cast("dict[str, Any]", positions_data_get)
        for ex_id_str_pos, syms_dict_any in positions_data_typed.items():
            if not isinstance(syms_dict_any, dict):
                continue

            syms_dict_typed = cast("dict[str, Any]", syms_dict_any)
            for sym_str, pos_data_any in syms_dict_typed.items():
                cls._process_single_position(tracker, ex_id_str_pos, sym_str, pos_data_any)

    @classmethod
    def _process_single_position(
        cls,
        tracker: PortfolioTracker,
        ex_id_str_pos: str,
        sym_str: str,
        pos_data_any: dict[str, Any] | DerivativePosition,
    ) -> None:
        """Process a single position entry."""
        if isinstance(pos_data_any, dict):
            try:
                # Ensure keys are str for model_validate
                validated_pos_dict_for_model: dict[str, Any] = {
                    str(k): v for k, v in pos_data_any.items()
                }
                tracker.positions[ex_id_str_pos][sym_str] = DerivativePosition.model_validate(
                    validated_pos_dict_for_model,
                )
            except ValidationError as e:
                logger.error(
                    "derivative_position_validation_error",
                    symbol=sym_str,
                    exchange_id=ex_id_str_pos,
                    error=str(e),
                )
            except Exception as e:  # Catch other potential errors from str(k)
                logger.warning(
                    "position_processing_error",
                    exchange_id=ex_id_str_pos,
                    symbol=sym_str,
                    error=str(e),
                    context="from_dict",
                )
        else:  # pos_data_any is DerivativePosition after type narrowing
            tracker.positions[ex_id_str_pos][sym_str] = pos_data_any

    @classmethod
    def _load_orders_from_dict(cls, tracker: PortfolioTracker, data: dict[str, Any]) -> None:
        """Load orders data from dictionary."""
        orders_data_get = data.get("orders", {})
        if not isinstance(orders_data_get, dict):
            return

        orders_data_typed = cast("dict[str, Any]", orders_data_get)
        for ex_id_str_ord, ords_dict_any in orders_data_typed.items():
            if not isinstance(ords_dict_any, dict):
                continue

            ords_dict_typed = cast("dict[str, Any]", ords_dict_any)
            for ord_id_str, order_data_any in ords_dict_typed.items():
                cls._process_single_order(tracker, ex_id_str_ord, ord_id_str, order_data_any)

    @classmethod
    def _process_single_order(
        cls,
        tracker: PortfolioTracker,
        ex_id_str_ord: str,
        ord_id_str: str,
        order_data_any: dict[str, Any] | Order,
    ) -> None:
        """Process a single order entry."""
        if isinstance(order_data_any, dict):
            try:
                # Ensure keys are str for model_validate
                validated_order_dict_for_model: dict[str, Any] = {
                    str(k): v for k, v in order_data_any.items()
                }
                tracker.orders[ex_id_str_ord][ord_id_str] = Order.model_validate(
                    validated_order_dict_for_model,
                )
            except ValidationError as e:
                logger.error(
                    "order_validation_error",
                    order_id=ord_id_str,
                    exchange_id=ex_id_str_ord,
                    error=str(e),
                )
            except Exception as e:  # Catch other potential errors from str(k)
                logger.warning(
                    "order_processing_error",
                    exchange_id=ex_id_str_ord,
                    order_id=ord_id_str,
                    error=str(e),
                    context="from_dict",
                )
        else:  # order_data_any is Order after type narrowing
            tracker.orders[ex_id_str_ord][ord_id_str] = order_data_any

    @classmethod
    def _load_timestamps_from_dict(cls, tracker: PortfolioTracker, data: dict[str, Any]) -> None:
        """Load timestamp data from dictionary."""
        cls._load_last_update_times(tracker, data)
        cls._load_last_reconciliation_times(tracker, data)

    @classmethod
    def _load_last_update_times(cls, tracker: PortfolioTracker, data: dict[str, Any]) -> None:
        """Load last update times from dictionary."""
        last_update_data_get = data.get("last_update_time", {})
        if not isinstance(last_update_data_get, dict):
            return

        last_update_data_typed = cast("dict[str, Any]", last_update_data_get)
        for ex_id_str_lut, ts_data_any_lut in last_update_data_typed.items():
            cls._process_timestamp(
                tracker.last_update_time,
                ex_id_str_lut,
                ts_data_any_lut,
                "last_update_time",
            )

    @classmethod
    def _load_last_reconciliation_times(
        cls,
        tracker: PortfolioTracker,
        data: dict[str, Any],
    ) -> None:
        """Load last reconciliation times from dictionary."""
        last_reconciliation_data_get = data.get("last_reconciliation_time", {})
        if not isinstance(last_reconciliation_data_get, dict):
            return

        last_reconciliation_data_typed = cast("dict[str, Any]", last_reconciliation_data_get)
        for ex_id_str_lrt, ts_data_any_lrt in last_reconciliation_data_typed.items():
            cls._process_timestamp(
                tracker.last_reconciliation_time,
                ex_id_str_lrt,
                ts_data_any_lrt,
                "last_reconciliation_time",
            )

    @classmethod
    def _process_timestamp(
        cls,
        target_dict: dict[str, datetime],
        ex_id_str: str,
        ts_data_any: str | datetime | None,
        field_name: str,
    ) -> None:
        """Process a single timestamp entry."""
        try:
            if isinstance(ts_data_any, datetime):
                target_dict[ex_id_str] = ts_data_any
            # Ensure ts_data_any is not None before passing to parse_datetime_utc
            elif ts_data_any is not None:
                parsed_ts = parse_datetime_utc(
                    ts_data_any,
                    field_name=f"{field_name}.{ex_id_str}",
                )
                if parsed_ts:
                    target_dict[ex_id_str] = parsed_ts
            else:
                logger.warning(
                    "timestamp_none_value",
                    field_name=field_name,
                    exchange_id=ex_id_str,
                    message="Received None for timestamp field, skipping.",
                )
        except Exception as e:
            logger.error(
                "timestamp_deserialization_error",
                field_name=field_name,
                exchange_id=ex_id_str,
                error_message=str(e),
                action="process_timestamp",
                error="deserialization_failed",
                message=f"Error deserializing {field_name} for {ex_id_str}: {e}",
            )

    @classmethod
    def _load_scalar_fields_from_dict(cls, tracker: PortfolioTracker, data: dict[str, Any]) -> None:
        """Load scalar fields from dictionary."""
        tracker.high_watermark = Decimal(str(data.get("high_watermark", "0.0")))
        tracker.realized_pnl = Decimal(str(data.get("realized_pnl", "0.0")))

    # --- Watchlist/Active Symbols ---
    def add_symbol_to_watchlist(self, symbol: str) -> None:
        """Add a symbol to the watchlist."""
        if symbol not in self.watchlist:
            self.watchlist.add(symbol)
            logger.info(
                "symbol_added_to_watchlist",
                symbol=symbol,
                action="add_symbol_to_watchlist",
                message=f"Added {symbol} to portfolio watchlist.",
            )
            # Potentially trigger subscription logic if needed

    def remove_symbol_from_watchlist(self, symbol: str) -> None:
        """Remove a symbol from the watchlist."""
        if symbol in self.watchlist:
            self.watchlist.remove(symbol)
            logger.info(
                "symbol_removed_from_watchlist",
                symbol=symbol,
                action="remove_symbol_from_watchlist",
                message=f"Removed {symbol} from portfolio watchlist.",
            )
            # Potentially trigger unsubscription logic

    def get_watchlist(self) -> set[str]:
        """Get the current set of watched symbols."""
        return self.watchlist.copy()

    def update_active_symbols(self) -> None:
        """Update the set of symbols with active positions or open orders."""
        active: set[str] = set()
        for positions in self.positions.values():
            for pos in positions.values():
                if pos.size != Decimal(0):
                    active.add(pos.symbol)
        for orders in self.orders.values():
            for order in orders.values():
                if order.status in {
                    OrderStatus.NEW,
                    OrderStatus.OPEN,
                    OrderStatus.PARTIALLY_FILLED,
                }:
                    active.add(order.symbol)
        self.active_symbols = active

    def get_active_symbols(self) -> set[str]:
        """Get the current set of symbols with active positions or orders."""
        # Ensure it's up-to-date before returning
        self.update_active_symbols()
        return self.active_symbols.copy()

    def get_relevant_symbols(self) -> set[str]:
        """Get all symbols relevant to the portfolio (active + watchlist)."""
        self.update_active_symbols()  # Ensure active symbols are current
        return self.active_symbols.union(self.watchlist)

    def _update_balance(self, exchange_id: str, balance: SpotBalance | None) -> None:
        if balance is None:
            return
        self.balances[exchange_id][balance.asset] = balance

    def _parse_positions(
        self,
        exchange_id: str,
        positions_data: list[DerivativePosition] | dict[str, Any],
    ) -> None:
        logger.warning("_parse_positions needs implementation based on API data format.")
        if isinstance(positions_data, dict):
            for _pos_key, _pos_data in positions_data.items():
                # TODO: Parse pos_data dict into DerivativePosition
                pass
        else:  # If it wasn't a dict, it must be a list[DerivativePosition] due to type hint
            for (
                _pos_data
            ) in positions_data:  # positions_data is now known to be list[DerivativePosition]
                # TODO: Parse _pos_data (DerivativePosition object) - likely no parsing needed
                pass
        # No else needed based on type hint

    def _parse_orders(self, exchange_id: str, orders_data: list[Order] | dict[str, Any]) -> None:
        async def _do_parse() -> None:
            async with self._lock:
                current_orders = self.orders.get(exchange_id, {})
                updated_count = 0
                new_count = 0

                # Prepare items to process
                items_to_process = self._prepare_order_items(orders_data)

                # Process each order item
                for order_data_item in items_to_process:
                    if isinstance(order_data_item, dict):
                        if self._process_order_dict(order_data_item, current_orders, exchange_id):
                            updated_count += 1
                    elif self._process_order_object(order_data_item, current_orders, exchange_id):
                        new_count += 1

                self.orders[exchange_id] = current_orders
                self.logger.info(
                    "orders_parsed",
                    exchange_id=exchange_id,
                    total_items=len(items_to_process),
                    new_count=new_count,
                    updated_count=updated_count,
                    message="Parsed order items for exchange",
                )

        task = asyncio.create_task(_do_parse())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _prepare_order_items(
        self,
        orders_data: list[Order] | dict[str, Any],
    ) -> list[Order | dict[str, Any]]:
        """Prepare order items for processing."""
        items_to_process: list[Order | dict[str, Any]] = []

        if isinstance(orders_data, dict):
            for item_val in orders_data.values():
                if isinstance(item_val, Order):
                    items_to_process.append(item_val)
                elif isinstance(item_val, dict):
                    # Explicitly cast to the expected dict type for the list
                    items_to_process.append(cast("dict[str, Any]", item_val))
                else:
                    self.logger.warning(
                        "unexpected_order_value_type",
                        value_type=type(item_val).__name__,
                        message="Skipping unexpected value type in orders_data dict",
                    )
        else:  # orders_data is list[Order]
            for item_in_list in orders_data:
                items_to_process.append(item_in_list)

        return items_to_process

    def _process_order_dict(
        self,
        order_dict_data: dict[str, Any],
        current_orders: dict[str, Order],
        exchange_id: str,
    ) -> bool:
        """Process an order from dictionary data. Returns True if successful."""
        try:
            order = Order.model_validate(order_dict_data)
            self._normalize_order_fields(order, exchange_id)
            self._normalize_order_status(order)
            current_orders[order.client_order_id] = order
            return True
        except ValidationError as e:
            client_id_for_log = order_dict_data.get(
                "clientOrderId",
                order_dict_data.get("client_order_id", "UnknownClientOrderID"),
            )
            self.logger.error(
                "order_validation_error_dict",
                client_order_id=client_id_for_log,
                exchange_id=exchange_id,
                error=str(e),
            )
            return False

    def _process_order_object(
        self,
        order_obj: Order,
        current_orders: dict[str, Order],
        exchange_id: str,
    ) -> bool:
        """Process an order object. Returns True if successful."""
        self._normalize_order_fields(order_obj, exchange_id)
        self._normalize_order_status(order_obj)
        current_orders[order_obj.client_order_id] = order_obj
        return True

    def _normalize_order_fields(self, order: Order, exchange_id: str) -> None:
        """Normalize order fields using safe decimal conversion."""
        order.price = self._safe_decimal_convert(
            order.price,
            "price",
            order.symbol,
            exchange_id,
        )
        order.quantity_requested = self._safe_decimal_convert(
            order.quantity_requested,
            "quantity_requested",
            order.symbol,
            exchange_id,
        ) or Decimal(0)
        order.quantity_filled = self._safe_decimal_convert(
            order.quantity_filled,
            "quantity_filled",
            order.symbol,
            exchange_id,
        ) or Decimal(0)

    def _normalize_order_status(self, order: Order) -> None:
        """Normalize order status field."""
        if isinstance(order.status, str):
            try:
                order.status = OrderStatus(order.status)
            except ValueError:
                self.logger.warning(
                    "invalid_order_status",
                    status=order.status,
                    client_order_id=order.client_order_id,
                    message="Invalid status string for order",
                )
                order.status = OrderStatus.UNKNOWN

    def reset(self) -> None:
        """Reset the portfolio tracker to a clean initial state.

        This method clears all tracked balances, positions, orders, timestamps, high watermark,
        realized PNL, active symbols, and watchlist. It is intended for use in tests or integration
        scenarios where a fresh portfolio state is required.
        """
        self.balances.clear()
        self.positions.clear()
        self.orders.clear()
        self.tickers.clear()
        self.last_update_time.clear()
        self.last_reconciliation_time.clear()
        self.high_watermark = Decimal("0.0")
        self.realized_pnl = Decimal("0.0")
        self.active_symbols.clear()
        self.watchlist.clear()
        self._initialize_data_structures()
        # Optionally, log the reset event
        logger.info("PortfolioTracker state has been reset.")

    async def load_state(self) -> None:
        """Load portfolio state from persistent storage.

        This method loads previously saved portfolio state including positions,
        balances, and order history from persistent storage to restore the
        portfolio tracker to its previous state.
        """
        # ... (Implementation as before) ...
        # Placeholder

    async def initialize_portfolio(self) -> None:
        """Initialize portfolio state from exchange APIs.

        This method performs the initial setup of the portfolio tracker by
        fetching current balances, positions, and orders from all configured
        exchanges to establish the starting state.
        """
        # ... (Implementation as before) ...
        # Placeholder

    # --- Price Helper --- #
    async def _get_asset_price_in_base(
        self,
        exchange_id: str,
        asset: str,  # The asset whose price we want (e.g., 'BTC', 'ETH')
        base_currency: str,  # The currency to get the price in (e.g., 'USDC')
        price_override: Decimal
        | None = None,  # Allow overriding for specific cases like entry price conversion
    ) -> Decimal | None:
        """Get the price of an asset in the base currency.

        Helper method to retrieve or calculate the current price of a given asset
        in terms of the specified base currency, with optional price override.

        Args:
            exchange_id: Exchange identifier.
            asset: Asset symbol to get price for.
            base_currency: Target currency for price conversion.
            price_override: Optional price override for specific cases.

        Returns:
            Asset price in base currency, or None if unavailable.

        """
        # TODO: Revisit price override logic - assumes override is already in base_currency
        if price_override is not None:
            logger.debug(
                "price_override_used",
                exchange_id=exchange_id,
                asset=asset,
                base_currency=base_currency,
                price_override=price_override,
            )
            return price_override

        if asset == base_currency:
            return Decimal("1.0")

        client = self.api_clients.get(exchange_id)
        if not client:
            logger.warning(
                "no_api_client_for_price",
                exchange_id=exchange_id,
                asset=asset,
                base_currency=base_currency,
                message="No API client to fetch price",
            )
            return None

        # DEBUG LOGGING START
        logger.info(
            "get_asset_price_in_base_called",
            exchange_id=exchange_id,
            asset=asset,
            base_currency=base_currency,
            price_override=price_override,
        )
        # DEBUG LOGGING END

        # 1. Direct match (e.g., BTC/USDC)
        symbol_direct = f"{asset.upper()}-{base_currency.upper()}"
        logger.debug(
            "get_asset_price_attempting_direct_lookup",
            exchange_id=exchange_id,
            symbol_direct=symbol_direct,
            message="_get_asset_price_in_base: Attempting direct lookup",
        )
        ticker_direct = await client.get_ticker(symbol_direct)
        logger.debug(
            "get_asset_price_received_ticker_direct",
            exchange_id=exchange_id,
            symbol_direct=symbol_direct,
            ticker_direct=ticker_direct,
            ticker_type=type(ticker_direct).__name__,
            message="_get_asset_price_in_base: Received ticker_direct",
        )
        if ticker_direct:
            logger.debug(
                "get_asset_price_ticker_direct_price",
                exchange_id=exchange_id,
                symbol_direct=symbol_direct,
                price=getattr(ticker_direct, "price", "N/A"),
                message="_get_asset_price_in_base: ticker_direct.price",
            )
            # Debug logging for ticker price checks
            has_valid_price = (
                ticker_direct
                and ticker_direct.price is not None
                and ticker_direct.price > Decimal(0)
            )
            logger.debug(
                "ticker_direct_price_check",
                exchange_id=exchange_id,
                symbol_direct=symbol_direct,
                has_valid_price=has_valid_price,
                message="Ticker direct price check",
            )

        if ticker_direct and ticker_direct.price is not None and ticker_direct.price > Decimal(0):
            logger.debug(
                "get_asset_price_ticker_direct_final_price",
                exchange_id=exchange_id,
                symbol_direct=symbol_direct,
                price=ticker_direct.price,
                message="_get_asset_price_in_base: ticker_direct.price final",
            )
            return ticker_direct.price

        # Try inverse pair: BASE-ASSET (e.g., USDC-BTC)
        symbol_inverse = f"{base_currency}-{asset}"
        logger.debug(
            "get_asset_price_attempting_inverse_lookup",
            exchange_id=exchange_id,
            symbol_inverse=symbol_inverse,
            message="_get_asset_price_in_base: Attempting inverse lookup",
        )
        ticker_inverse = await client.get_ticker(symbol_inverse)
        logger.debug(
            "get_asset_price_received_ticker_inverse",
            exchange_id=exchange_id,
            symbol_inverse=symbol_inverse,
            ticker_inverse=ticker_inverse,
            ticker_type=type(ticker_inverse).__name__,
            message="_get_asset_price_in_base: Received ticker_inverse",
        )
        if ticker_inverse:
            logger.debug(
                "get_asset_price_ticker_inverse_price",
                exchange_id=exchange_id,
                symbol_inverse=symbol_inverse,
                price=getattr(ticker_inverse, "price", "N/A"),
                message="_get_asset_price_in_base: ticker_inverse.price",
            )
            # Debug logging for inverse ticker price checks
            has_valid_inverse_price = (
                ticker_inverse
                and ticker_inverse.price is not None
                and ticker_inverse.price > Decimal(0)
            )
            logger.debug(
                "ticker_inverse_price_check",
                exchange_id=exchange_id,
                symbol_inverse=symbol_inverse,
                has_valid_inverse_price=has_valid_inverse_price,
                message="Ticker inverse price check",
            )

        if (
            ticker_inverse
            and ticker_inverse.price is not None
            and ticker_inverse.price > Decimal(0)
        ):
            price = Decimal("1.0") / ticker_inverse.price
            logger.debug(
                "get_asset_price_ticker_inverse_calculated",
                exchange_id=exchange_id,
                symbol_inverse=symbol_inverse,
                inverse_price=ticker_inverse.price,
                calculated_price=price,
                message="_get_asset_price_in_base: ticker_inverse calculated price",
            )
            return price

        # TODO: Implement simple triangulation if needed
        logger.warning(
            "price_conversion_failed",
            exchange_id=exchange_id,
            asset=asset,
            base_currency=base_currency,
            message="Price conversion failed. Returning None.",
        )
        return None

    def get_exchange_balance(self, exchange: str, asset: str) -> SpotBalance | None:
        """Retrieve the balance for a specific asset on a specific exchange.

        Conforms to PortfolioTrackerProtocol by providing access to asset balances
        for a given exchange.

        Args:
            exchange: Exchange identifier.
            asset: Asset symbol to retrieve balance for.

        Returns:
            SpotBalance object if found, None otherwise.

        """
        # Internally, self.balances uses exchange_id which is equivalent to 'exchange' here.
        if exchange in self.balances and asset in self.balances[exchange]:
            return self.balances[exchange][asset]
        self.logger.debug(
            "balance_not_found",
            asset=asset,
            exchange=exchange,
            action="get_exchange_balance",
            issue="balance_not_found",
            message=f"Balance for {asset} on exchange '{exchange}' not found.",
        )  # DEBUG log
        return None

    def _initialize_from_config(self) -> None:
        logger.info("Initializing PortfolioTracker from config...")
        # Initialize balances
        for exchange_id, balances in self.pt_config.initial_balances.items():
            for asset, quantity_str in balances.items():
                try:
                    quantity = Decimal(quantity_str)
                    self.balances[exchange_id][asset] = SpotBalance(
                        exchange=exchange_id,
                        asset=asset,
                        total_quantity=quantity,
                        available_quantity=quantity,  # Assume all available initially
                        timestamp=datetime.now(UTC),
                    )
                    logger.info(
                        "balance_initialized_from_config",
                        asset=asset,
                        exchange_id=exchange_id,
                        quantity=float(quantity),
                        action="initialize_from_config",
                        message=f"Initialized balance for {asset} on {exchange_id}: {quantity}",
                    )
                except InvalidOperation:
                    logger.error(
                        "invalid_decimal_initial_balance",
                        asset=asset,
                        exchange_id=exchange_id,
                        quantity_str=quantity_str,
                        message="Invalid decimal value for initial balance",
                    )
        # Initialize positions
        for pos_dict in self.pt_config.initial_positions:
            try:
                pos = DerivativePosition(**pos_dict)
                self.positions[pos.exchange][pos.symbol] = pos
                logger.info(
                    "position_initialized",
                    symbol=pos.symbol,
                    exchange=pos.exchange,
                    side=pos.side,
                    size=pos.size,
                    message="Initialized position",
                )
            except (ValidationError, TypeError) as e:
                logger.error(
                    "derivative_position_creation_failed",
                    error_message=str(e),
                    error_type=type(e).__name__,
                    action="initialize_from_config",
                    error="position_creation_failed",
                    message=f"Failed to create DerivativePosition from config: {e}",
                )
        logger.info("PortfolioTracker initialized.")

    async def _fetch_exchange_account_summary(
        self,
        client: ExchangeAPI,
        exchange_id: str,
    ) -> tuple[str, MarginAccountSummary | None] | None:
        try:
            summary = await client.get_account_summary()
            if summary:
                return exchange_id, summary
            logger.warning(
                "no_account_summary_found",
                exchange_id=exchange_id,
                action="fetch_exchange_account_summary",
                issue="no_account_summary",
                message=f"No account summary found for {exchange_id}",
            )
            return None
        except Exception as e:
            logger.exception(
                "account_summary_fetch_error",
                action="fetch_account_summary",
                exchange_id=exchange_id,
                error=str(e),
                message=f"Error fetching account summary for {exchange_id}: {e}",
            )
            return None

    def get_all_derivative_positions_for_exchange(
        self,
        exchange_id: str,
    ) -> dict[Symbol, DerivativePosition] | None:
        """Return all derivative positions for a given exchange.

        Args:
            exchange_id: Exchange identifier to get positions for.

        Returns:
            Dictionary mapping symbols to positions, or None if exchange not found.

        """
        normalized_exchange_id = exchange_id.lower()

        # ADDED DETAILED LOGGING (NOW AS WARNING)
        self.logger.warning(
            "pt_get_all_deriv_pos_critical_debug",
            self_id=id(self),
            api_clients_id=id(self.api_clients),
            api_clients=self.api_clients,
            exchange_id=exchange_id,
            normalized_exchange_id=normalized_exchange_id,
            message="PT_GET_ALL_DERIV_POS_CRITICAL_DEBUG",
        )

        if normalized_exchange_id not in self.api_clients:
            self.logger.warning(
                "pt_get_all_deriv_pos_unknown_exchange",
                exchange_id=exchange_id,
                normalized_exchange_id=normalized_exchange_id,
                message=(
                    "PT_GET_ALL_DERIV_POS: Attempted to get positions "
                    "for unknown or unregistered exchange"
                ),
            )
            return None

        return self.positions[normalized_exchange_id]
