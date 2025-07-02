"""Portfolio Orchestrator for CyberDeltaEngine.

This module contains the PortfolioOrchestrator class, which orchestrates
portfolio data fetching from exchanges and feeds the PortfolioTracker.
It handles all API interactions, reconciliation timing, and data transformation,
keeping the PortfolioTracker as a pure state manager.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import (
    Ticker,
)


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI
    from cyberdelta.core.portfolio_tracker import PortfolioTracker


logger = get_logger(__name__)


class PortfolioOrchestrator:
    """Orchestrates portfolio data fetching and coordination across multiple exchanges.

    This service acts as the coordination layer between exchange APIs and the PortfolioTracker,
    implementing the orchestration pattern to manage complex multi-exchange data flows.
    It eliminates circular dependencies by serving as the bridge between the core and APIs layers.

    Key Responsibilities:
    - **API Coordination**: Managing API calls to multiple exchanges in parallel
    - **Data Orchestration**: Fetching data and feeding it to PortfolioTracker
    - **Rate Limiting**: Preventing API rate limit violations with semaphore-based throttling
    - **Error Handling**: Robust error handling with logging and graceful degradation
    - **Timing Management**: Controlling reconciliation intervals and update schedules
    - **Background Tasks**: Managing asynchronous operations and resource cleanup
    - **Performance Optimization**: Parallel execution and efficient resource utilization

    Architecture Benefits:
    - **Separation of Concerns**: Clean separation between API calls and state management
    - **Dependency Isolation**: Eliminates circular dependencies (core ↔ apis)
    - **Scalability**: Concurrent processing of multiple exchanges
    - **Reliability**: Isolated error handling prevents cascade failures
    - **Maintainability**: Clear responsibility boundaries and testable components

    Data Flow Pattern:
    1. **Trigger**: Reconciliation timer or manual update request
    2. **Fetch**: Parallel API calls to all configured exchanges
    3. **Transform**: Data validation and format standardization
    4. **Update**: Feed processed data to PortfolioTracker
    5. **Monitor**: Log results and update timing statistics

    Rate Limiting Strategy:
    - Per-exchange semaphores limit concurrent requests
    - Configurable limits respect exchange-specific quotas
    - Graceful backoff on rate limit violations

    Example Usage:
        ```python
        # Initialize with dependencies
        orchestrator = PortfolioOrchestrator(
            app_settings=settings,
            portfolio_tracker=tracker,
            api_clients={"hyperliquid": hl_client, "backpack": bp_client},
        )

        # Full reconciliation across all exchanges
        await orchestrator.orchestrate_full_reconciliation()

        # Selective updates based on timing
        await orchestrator.orchestrate_periodic_updates()

        # Manual exchange-specific updates
        success = await orchestrator.fetch_and_update_balances("hyperliquid")
        ```

    See Also:
        - PortfolioTracker: Pure state manager that receives orchestrated data
        - ExchangeAPI: Individual exchange API clients
        - PriceDataService: Specialized service for ticker/price data management
    """

    def __init__(
        self,
        app_settings: AppSettings,
        portfolio_tracker: PortfolioTracker,
        api_clients: dict[str, ExchangeAPI] | None = None,
    ) -> None:
        """Initialize the portfolio orchestrator.

        Args:
            app_settings: Application configuration
            portfolio_tracker: The portfolio tracker instance to update with data
            api_clients: Dictionary of exchange API clients, keyed by exchange ID
        """
        self.logger = get_logger(__name__ + "." + self.__class__.__name__)
        self.app_settings = app_settings
        self.portfolio_tracker = portfolio_tracker
        self.api_clients: dict[str, ExchangeAPI] = api_clients or {}

        # Reconciliation timing configuration
        self.reconciliation_interval: int = 300  # Default 5 minutes
        self.last_reconciliation_time: dict[str, datetime] = {}

        # Rate limiting and optimization features
        self._rate_limit_semaphores: dict[str, asyncio.Semaphore] = {}
        self._max_concurrent_requests_per_exchange = 5  # Configurable limit

        # Background task management
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._lock = asyncio.Lock()

        self.logger.info(
            "PortfolioOrchestrator initialized",
            exchanges=list(self.api_clients.keys()),
            reconciliation_interval=self.reconciliation_interval,
        )

    def _get_rate_limit_semaphore(self, exchange_id: str) -> asyncio.Semaphore:
        """Get or create a rate limiting semaphore for an exchange.

        This helps prevent overwhelming exchange APIs with too many concurrent requests.

        Args:
            exchange_id: The exchange identifier

        Returns:
            asyncio.Semaphore: Semaphore for rate limiting this exchange
        """
        if exchange_id not in self._rate_limit_semaphores:
            self._rate_limit_semaphores[exchange_id] = asyncio.Semaphore(
                self._max_concurrent_requests_per_exchange
            )
        return self._rate_limit_semaphores[exchange_id]

    def register_api_client(self, exchange_id: str, client: ExchangeAPI) -> None:
        """Register an API client for a specific exchange.

        Args:
            exchange_id: Unique identifier for the exchange
            client: The exchange API client instance
        """
        self.api_clients[exchange_id] = client
        self.logger.info(
            "Registered API client",
            exchange_id=exchange_id,
            client_type=type(client).__name__,
        )

    async def fetch_and_update_balances(self, exchange_id: str) -> bool:
        """Fetch balances from exchange and update portfolio tracker.

        Args:
            exchange_id: The exchange to fetch balances from

        Returns:
            True if successful, False otherwise
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            self.logger.error(
                "api_client_not_found",
                exchange_id=exchange_id,
                function="fetch_balances",
                action="returning_false",
                message=f"No API client found for {exchange_id} in fetch_balances",
            )
            return False

        # Apply rate limiting
        rate_limiter = self._get_rate_limit_semaphore(exchange_id)

        try:
            async with rate_limiter:
                # Fetch balances from the exchange API
                balances_data = await client.get_balances()  # Returns dict[str, SpotBalance]

            # Update the last reconciliation time
            self.last_reconciliation_time[exchange_id] = datetime.now(UTC)

            # Feed the data to the portfolio tracker
            await self.portfolio_tracker.update_balances(exchange_id, balances_data)

            self.logger.info(
                "balances_fetched_successfully",
                component="PORTFOLIO_ORCHESTRATOR",
                exchange_id=exchange_id,
                num_balances=len(balances_data),
                message=f"Successfully fetched balances for {exchange_id}",
            )

        except Exception as e:
            self.logger.exception(
                "balance_fetch_error",
                component="PORTFOLIO_ORCHESTRATOR",
                exchange_id=exchange_id,
                error_type=type(e).__name__,
                error=str(e),
                message=f"[PORTFOLIO_ORCHESTRATOR:{exchange_id}] Error during balance fetch: {e}",
            )
            return False
        else:
            return True

    async def fetch_and_update_positions(self, exchange_id: str) -> bool:
        """Fetch positions from exchange and update portfolio tracker.

        Args:
            exchange_id: The exchange to fetch positions from

        Returns:
            True if successful, False otherwise
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            self.logger.error(
                "no_api_client_for_fetch_positions",
                exchange_id=exchange_id,
                action="fetch_positions",
                error="no_api_client_registered",
                message=f"No API client registered for {exchange_id} in fetch_positions",
            )
            return False

        try:
            # Fetch positions from the exchange API
            positions_data = await client.get_positions()  # Returns list[DerivativePosition]

            # Update the last reconciliation time
            self.last_reconciliation_time[exchange_id] = datetime.now(UTC)

            # Feed the data to the portfolio tracker
            await self.portfolio_tracker.update_positions(exchange_id, positions_data)

            self.logger.info(
                "positions_fetched_successfully",
                component="PORTFOLIO_ORCHESTRATOR",
                exchange_id=exchange_id,
                position_count=len(positions_data),
                message=f"Successfully fetched positions for {exchange_id}",
            )

        except Exception as e:
            self.logger.exception(
                "position_fetch_failed",
                component="PORTFOLIO_ORCHESTRATOR",
                exchange_id=exchange_id,
                error=str(e),
                message=f"Failed to fetch positions for {exchange_id}: {e}",
            )
            return False
        else:
            return True

    async def fetch_and_update_orders(self, exchange_id: str) -> bool:
        """Fetch open orders from exchange and update portfolio tracker.

        Args:
            exchange_id: The exchange to fetch orders from

        Returns:
            True if successful, False otherwise
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            self.logger.error(
                "no_api_client_found",
                exchange_id=exchange_id,
                action="fetch_orders",
                error="no_api_client_found",
                message=f"No API client found for {exchange_id}",
            )
            return False

        try:
            # Fetch open orders from the exchange API
            orders_data = await client.get_open_orders()  # Returns list[Order]

            # Update the last reconciliation time
            self.last_reconciliation_time[exchange_id] = datetime.now(UTC)

            # Feed the data to the portfolio tracker
            await self.portfolio_tracker.update_orders(exchange_id, orders_data)

            self.logger.info(
                "orders_fetched_successfully",
                component="PORTFOLIO_ORCHESTRATOR",
                exchange_id=exchange_id,
                order_count=len(orders_data),
                message=f"Successfully fetched orders for {exchange_id}",
            )

        except Exception as e:
            self.logger.exception(
                "order_fetch_failed",
                component="PORTFOLIO_ORCHESTRATOR",
                exchange_id=exchange_id,
                error=str(e),
                message=f"Failed to fetch orders for {exchange_id}: {e}",
            )
            return False
        else:
            return True

    async def fetch_and_update_account_summary(self, exchange_id: str) -> bool:
        """Fetch account summary from exchange and update portfolio tracker.

        Args:
            exchange_id: The exchange to fetch account summary from

        Returns:
            True if successful, False otherwise
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            self.logger.error(
                "no_api_client_found",
                exchange_id=exchange_id,
                action="fetch_account_summary",
                error="no_api_client_found",
                message=f"No API client found for {exchange_id}",
            )
            return False

        try:
            # Fetch account summary from the exchange API
            summary = await client.get_account_summary()

            if not summary:
                self.logger.warning(
                    "no_account_summary_found",
                    exchange_id=exchange_id,
                    action="fetch_exchange_account_summary",
                    issue="no_account_summary",
                    message=f"No account summary found for {exchange_id}",
                )
                return False

            # Update the last reconciliation time
            self.last_reconciliation_time[exchange_id] = datetime.now(UTC)

            # Feed the data to the portfolio tracker
            await self.portfolio_tracker.update_account_summary(exchange_id, summary)

            self.logger.info(
                "account_summary_fetched_successfully",
                component="PORTFOLIO_ORCHESTRATOR",
                exchange_id=exchange_id,
                message=f"Successfully fetched account summary for {exchange_id}",
            )

        except Exception as e:
            self.logger.exception(
                "account_summary_fetch_error",
                component="PORTFOLIO_ORCHESTRATOR",
                exchange_id=exchange_id,
                error=str(e),
                message=f"Error fetching account summary for {exchange_id}: {e}",
            )
            return False
        else:
            return True

    async def fetch_ticker_data(self, exchange_id: str, symbol: str) -> Ticker | None:
        """Fetch ticker data for a specific symbol from an exchange.

        Args:
            exchange_id: The exchange to fetch ticker from
            symbol: The symbol to fetch ticker for

        Returns:
            Ticker data if successful, None otherwise
        """
        client = self.api_clients.get(exchange_id)
        if not client:
            self.logger.error(
                "no_api_client_found",
                exchange_id=exchange_id,
                action="fetch_ticker",
                symbol=symbol,
                error="no_api_client_found",
                message=f"No API client found for {exchange_id}",
            )
            return None

        try:
            # Fetch ticker from the exchange API
            ticker = await client.get_ticker(symbol)
        except Exception as e:
            self.logger.exception(
                "ticker_fetch_error",
                component="PORTFOLIO_ORCHESTRATOR",
                exchange_id=exchange_id,
                symbol=symbol,
                error=str(e),
                message=f"Error fetching ticker for {symbol} on {exchange_id}: {e}",
            )
            return None
        else:
            if ticker:
                self.logger.debug(
                    "ticker_fetched_successfully",
                    component="PORTFOLIO_ORCHESTRATOR",
                    exchange_id=exchange_id,
                    symbol=symbol,
                    bid=float(ticker.bid) if ticker.bid else None,
                    ask=float(ticker.ask) if ticker.ask else None,
                    message=f"Successfully fetched ticker for {symbol} on {exchange_id}",
                )

                # Feed the data to the portfolio tracker
                await self.portfolio_tracker.update_ticker_data(exchange_id, symbol, ticker)

                return ticker
            self.logger.warning(
                "ticker_not_found",
                component="PORTFOLIO_ORCHESTRATOR",
                exchange_id=exchange_id,
                symbol=symbol,
                message=f"No ticker found for {symbol} on {exchange_id}",
            )
            return None

    async def orchestrate_full_reconciliation(self) -> None:
        """Orchestrate a full reconciliation across all exchanges.

        This method coordinates fetching all data types (balances, positions,
        orders, account summaries) from all registered exchanges in parallel.
        """
        if not self.api_clients:
            self.logger.warning(
                "no_api_clients_registered",
                component="PORTFOLIO_ORCHESTRATOR",
                message="No API clients registered, skipping reconciliation",
            )
            return

        self.logger.info(
            "starting_full_reconciliation",
            component="PORTFOLIO_ORCHESTRATOR",
            exchanges=list(self.api_clients.keys()),
            message="Starting full portfolio reconciliation",
        )

        # Create tasks for all exchanges and all data types
        tasks: list[asyncio.Task[bool]] = []

        for exchange_id in self.api_clients:
            # Create tasks for each data type
            tasks.extend([
                asyncio.create_task(
                    self.fetch_and_update_balances(exchange_id),
                    name=f"fetch_balances_{exchange_id}",
                ),
                asyncio.create_task(
                    self.fetch_and_update_positions(exchange_id),
                    name=f"fetch_positions_{exchange_id}",
                ),
                asyncio.create_task(
                    self.fetch_and_update_orders(exchange_id), name=f"fetch_orders_{exchange_id}"
                ),
                asyncio.create_task(
                    self.fetch_and_update_account_summary(exchange_id),
                    name=f"fetch_account_summary_{exchange_id}",
                ),
            ])

        # Execute all tasks in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log results
        successful_tasks = sum(1 for r in results if r is True)
        failed_tasks = sum(1 for r in results if r is False or isinstance(r, Exception))

        self.logger.info(
            "full_reconciliation_completed",
            component="PORTFOLIO_ORCHESTRATOR",
            total_tasks=len(tasks),
            successful=successful_tasks,
            failed=failed_tasks,
            message=f"Full reconciliation completed: {successful_tasks}/{len(tasks)} successful",
        )

        # Log any exceptions
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                task_name = tasks[i].get_name() if i < len(tasks) else f"task_{i}"
                self.logger.error(
                    "reconciliation_task_exception",
                    component="PORTFOLIO_ORCHESTRATOR",
                    task=task_name,
                    error_type=type(result).__name__,
                    error=str(result),
                    message=f"Task {task_name} raised exception: {result}",
                )

    async def orchestrate_periodic_updates(self) -> None:
        """Orchestrate periodic updates based on reconciliation timing.

        This method manages the timing of reconciliation updates, ensuring
        data is refreshed at appropriate intervals while respecting rate limits.
        """
        if not self.api_clients:
            self.logger.warning(
                "no_api_clients_registered",
                component="PORTFOLIO_ORCHESTRATOR",
                message="No API clients registered, skipping periodic updates",
            )
            return

        # Create tasks for exchanges that need reconciliation
        tasks: list[asyncio.Task[bool]] = []
        exchanges_to_reconcile: list[str] = []

        for exchange_id in self.api_clients:
            if self.should_reconcile(exchange_id):
                exchanges_to_reconcile.append(exchange_id)
                # Create tasks for each data type
                tasks.extend([
                    asyncio.create_task(
                        self.fetch_and_update_balances(exchange_id),
                        name=f"fetch_balances_{exchange_id}",
                    ),
                    asyncio.create_task(
                        self.fetch_and_update_positions(exchange_id),
                        name=f"fetch_positions_{exchange_id}",
                    ),
                    asyncio.create_task(
                        self.fetch_and_update_orders(exchange_id),
                        name=f"fetch_orders_{exchange_id}",
                    ),
                    asyncio.create_task(
                        self.fetch_and_update_account_summary(exchange_id),
                        name=f"fetch_account_summary_{exchange_id}",
                    ),
                ])

        if not tasks:
            self.logger.debug(
                "no_exchanges_need_reconciliation",
                component="PORTFOLIO_ORCHESTRATOR",
                message="No exchanges need reconciliation at this time",
            )
            return

        self.logger.info(
            "starting_periodic_updates",
            component="PORTFOLIO_ORCHESTRATOR",
            exchanges=exchanges_to_reconcile,
            num_tasks=len(tasks),
            message=f"Starting periodic updates for {len(exchanges_to_reconcile)} exchanges",
        )

        # Execute all tasks in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log results
        successful_tasks = sum(1 for r in results if r is True)
        failed_tasks = sum(1 for r in results if r is False or isinstance(r, Exception))

        self.logger.info(
            "periodic_updates_completed",
            component="PORTFOLIO_ORCHESTRATOR",
            total_tasks=len(tasks),
            successful=successful_tasks,
            failed=failed_tasks,
            message=f"Periodic updates completed: {successful_tasks}/{len(tasks)} successful",
        )

    def should_reconcile(self, exchange_id: str) -> bool:
        """Check if reconciliation should be performed for an exchange.

        Args:
            exchange_id: The exchange to check

        Returns:
            True if reconciliation should be performed, False otherwise
        """
        if exchange_id not in self.last_reconciliation_time:
            return True

        time_since_last = (
            datetime.now(UTC) - self.last_reconciliation_time[exchange_id]
        ).total_seconds()

        return time_since_last >= self.reconciliation_interval

    async def shutdown(self) -> None:
        """Shutdown the orchestrator and cancel any background tasks."""
        self.logger.info("Shutting down PortfolioOrchestrator")

        # Cancel all background tasks
        for task in self._background_tasks:
            if not task.done():
                task.cancel()

        # Wait for all tasks to complete
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)

        self._background_tasks.clear()
        self.logger.info("PortfolioOrchestrator shutdown complete")
