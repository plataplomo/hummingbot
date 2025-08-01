"""Exchange Data Service - handles all exchange API interactions for portfolio data."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, cast

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger

# Remove type import - use Any for exchange API clients
# This maintains clean architecture by not importing from API layer

logger = get_logger(__name__)


class ExchangeDataService(BaseModel):
    """Handles all exchange API interactions for portfolio data."""

    service_name: str = Field(default="exchange_data", description="Service name")
    api_clients: dict[str, Any] = Field(default_factory=dict, description="Exchange API clients by exchange ID")

    model_config = ConfigDict(extra="forbid", validate_assignment=True, arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any) -> None:
        """Initialize service after Pydantic validation."""
        logger.info(f"ExchangeDataService '{self.service_name}' initialized")

    def register_api_client(self, exchange_id: str, api_client: Any) -> None:
        """Register an exchange API client."""
        self.api_clients[exchange_id.lower()] = api_client
        logger.info(f"Registered API client for {exchange_id}")

    async def fetch_all_portfolio_data(self) -> dict[str, dict[str, Any]]:
        """Fetch portfolio data from all exchanges in parallel."""
        logger.info("Fetching portfolio data from all exchanges")

        tasks = []
        for exchange_id, api_client in self.api_clients.items():
            tasks.append(self._fetch_exchange_data_async(exchange_id, api_client))

        if not tasks:
            logger.warning("No API clients available for portfolio data fetch")
            return {}

        results = await asyncio.gather(*tasks, return_exceptions=True)

        exchange_data: dict[str, dict[str, Any]] = {}
        for result in results:
            if isinstance(result, Exception):
                # Log error but continue with other exchanges
                self._log_exchange_error(result)
                continue
            exchange_data.update(cast(dict[str, dict[str, Any]], result))

        logger.info(f"Successfully fetched portfolio data from {len(exchange_data)} exchanges")
        return exchange_data

    async def fetch_exchange_data(self, exchange_id: str) -> dict[str, Any]:
        """Fetch portfolio data from specific exchange."""
        logger.info(f"Fetching portfolio data from exchange: {exchange_id}")

        api_client = self.api_clients.get(exchange_id.lower())
        if not api_client:
            raise ValueError(f"No API client available for {exchange_id}")
        
        result = await self._fetch_exchange_data_async(exchange_id, api_client)
        return result.get(exchange_id.lower(), {})

    async def _fetch_exchange_data_async(self, exchange_id: str, api_client: Any) -> dict[str, dict[str, Any]]:
        """Fetch portfolio data from specific exchange API client."""
        try:
            logger.debug(f"Fetching {exchange_id} portfolio data")
            
            # Fetch data in parallel using generic API interface
            balances_task = api_client.get_balances()
            positions_task = api_client.get_positions()
            orders_task = api_client.get_open_orders()
            
            gather_results = await asyncio.gather(
                balances_task, positions_task, orders_task,
                return_exceptions=True
            )
            balances: Any = gather_results[0]
            positions: Any = gather_results[1]
            orders: Any = gather_results[2]
            
            # Handle individual failures
            result_data: dict[str, Any] = {}
            if not isinstance(balances, Exception):
                result_data["balances"] = balances
            else:
                logger.warning(f"Failed to fetch {exchange_id} balances: {balances}")
                
            if not isinstance(positions, Exception):
                result_data["positions"] = positions
            else:
                logger.warning(f"Failed to fetch {exchange_id} positions: {positions}")
                
            if not isinstance(orders, Exception):
                result_data["orders"] = orders
            else:
                logger.warning(f"Failed to fetch {exchange_id} orders: {orders}")

            return {exchange_id.lower(): result_data}
            
        except Exception as e:
            logger.exception(f"Error fetching {exchange_id} portfolio data")
            raise

    def _log_exchange_error(self, error: Exception) -> None:
        """Log exchange API error."""
        logger.error(
            "Exchange API error during portfolio data fetch",
            error_type=type(error).__name__,
            error_message=str(error)
        )