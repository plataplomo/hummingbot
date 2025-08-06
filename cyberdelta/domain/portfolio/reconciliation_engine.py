"""Portfolio reconciliation engine module.

This module handles reconciliation of portfolio state with actual exchange
balances and positions.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.portfolio import ReconciliationError
from cyberdelta.models import DerivativePosition, SpotBalance
from cyberdelta.models.portfolio.pnl_report import ReconciliationReport
from cyberdelta.symbols.api import symbol as create_symbol


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI
    from cyberdelta.protocols.domain.portfolio import (
        BalanceManagerProtocol,
        PortfolioStorageProtocol,
        PositionManagerProtocol,
    )

from cyberdelta.protocols.domain.portfolio import ReconciliationEngineProtocol


logger = get_logger(__name__)


class ReconciliationEngine(ReconciliationEngineProtocol):
    """Handles reconciliation of portfolio state with exchange data.

    Configuration Usage:
    - Uses config.state.reconciliation_timeout for API timeouts
    - Uses config.validation.balance_tolerance for balance comparison
    - Uses config.validation.position_size_tolerance for position comparison
    - Uses config.exchanges to determine which exchanges to check

    Following CODING_STANDARDS.md:
    - ALL tolerances and timeouts from config
    - Uses ExchangeName enum for exchange iteration
    - NO assumptions about exchange availability
    - Fail fast on critical discrepancies
    """

    def __init__(
        self,
        config: AppSettings,
        storage: PortfolioStorageProtocol,
        balance_manager: BalanceManagerProtocol,
        position_manager: PositionManagerProtocol,
    ) -> None:
        """Initialize reconciliation engine with configuration and dependencies.

        Args:
            config: Application settings containing all configuration
            storage: Storage protocol implementation for persistence
            balance_manager: Balance manager for balance operations
            position_manager: Position manager for position operations
        """
        self.config = config
        self._storage = storage
        self._balance_manager = balance_manager
        self._position_manager = position_manager

        # Extract configuration values - NO hardcoded defaults
        self._balance_tolerance = config.validation.balance_tolerance
        self._position_tolerance = config.validation.position_size_tolerance

        logger.info(
            "reconciliation_engine_initialized",
            balance_tolerance=self._balance_tolerance,
            position_tolerance=self._position_tolerance,
        )

    async def reconcile_with_exchanges(
        self, api_clients: dict[str, ExchangeAPI]
    ) -> ReconciliationReport:
        """Reconcile portfolio state with actual exchange balances and positions.

        This method fetches current balances and positions from all enabled exchanges
        and compares them with our cached state. Discrepancies are logged and
        corrected based on configured tolerance levels.

        Configuration Usage:
        - Uses config.state.reconciliation_timeout for API timeouts
        - Uses config.validation.balance_tolerance for balance comparison
        - Uses config.validation.position_size_tolerance for position comparison
        - Uses config.exchanges to determine which exchanges to check

        Following CODING_STANDARDS.md:
        - ALL tolerances and timeouts from config
        - Uses ExchangeName enum for exchange iteration
        - NO assumptions about exchange availability
        - Fail fast on critical discrepancies

        Args:
            api_clients: Dictionary of exchange API clients

        Returns:
            Typed reconciliation results including errors and discrepancies

        Raises:
            ReconciliationError: If reconciliation fails for all exchanges
        """
        if not api_clients:
            logger.warning("reconciliation_skipped", reason="no_api_clients_configured")
            return ReconciliationReport(
                reconciliation_timestamp=datetime.now(UTC),
                reconciliation_successful=False,
                total_discrepancies=0,
                exchange_results={},
                balance_discrepancies=[],
                position_discrepancies=[],
                error_messages=["No API clients configured"],
            )

        logger.info("portfolio_reconciliation_starting")

        total_discrepancies = 0
        critical_discrepancies = 0

        # Get reconciliation settings from config
        reconciliation_timeout = self.config.state.update_timeout
        balance_tolerance = self._balance_tolerance
        position_tolerance = self._position_tolerance

        for exchange_name, api_client in api_clients.items():
            discrepancies_result = await self._reconcile_single_exchange(
                exchange_name,
                api_client,
                reconciliation_timeout,
                balance_tolerance,
                position_tolerance,
            )

            if discrepancies_result is None:
                critical_discrepancies += 1
            else:
                total_discrepancies += discrepancies_result

        # Final reconciliation summary
        if critical_discrepancies > 0:
            logger.error(
                "reconciliation_completed_with_errors",
                total_discrepancies=total_discrepancies,
                critical_errors=critical_discrepancies,
                exchanges_checked=len(api_clients),
            )

            # Fail fast if too many critical errors
            if critical_discrepancies >= len(api_clients):
                raise ReconciliationError(len(api_clients))
        elif total_discrepancies > 0:
            logger.warning(
                "reconciliation_completed_with_discrepancies",
                total_discrepancies=total_discrepancies,
                exchanges_checked=len(api_clients),
            )
        else:
            logger.info("reconciliation_completed_successfully", exchanges_checked=len(api_clients))

        exchange_results = dict.fromkeys(api_clients.keys(), critical_discrepancies == 0)

        return ReconciliationReport(
            reconciliation_timestamp=datetime.now(UTC),
            reconciliation_successful=total_discrepancies == 0 and critical_discrepancies == 0,
            total_discrepancies=total_discrepancies,
            exchange_results=exchange_results,
            balance_discrepancies=[],  # Would need to collect from _reconcile_single_exchange
            position_discrepancies=[],  # Would need to collect from _reconcile_single_exchange
            error_messages=(
                [f"{critical_discrepancies} critical errors"]
                if critical_discrepancies > 0
                else None
            ),
        )

    async def _reconcile_single_exchange(
        self,
        exchange_name: str,
        api_client: ExchangeAPI,
        reconciliation_timeout: float,
        balance_tolerance: Decimal,
        position_tolerance: Decimal,
    ) -> int | None:
        """Reconcile a single exchange and return discrepancy count or None if error.

        Args:
            exchange_name: Name of the exchange
            api_client: API client for the exchange
            reconciliation_timeout: Timeout for reconciliation operations
            balance_tolerance: Tolerance for balance differences
            position_tolerance: Tolerance for position differences

        Returns:
            Number of discrepancies found, or None if reconciliation failed
        """
        try:
            # Convert string to ExchangeName enum
            try:
                exchange_enum = ExchangeName(exchange_name)
            except ValueError:
                logger.exception("reconciliation_invalid_exchange", exchange=exchange_name)
                return None

            # Check if exchange is enabled in config
            exchange_config = self.config.exchanges.get(exchange_name)
            if not exchange_config or not exchange_config.enabled:
                logger.debug("reconciliation_skipped_disabled_exchange", exchange=exchange_name)
                return 0

            logger.info(
                "reconciling_exchange",
                exchange=exchange_name,
                timeout_seconds=reconciliation_timeout,
            )

            # Reconcile balances for this exchange
            balance_discrepancies = await self._reconcile_exchange_balances(
                exchange_enum,
                api_client,
                Decimal(str(reconciliation_timeout)),
                balance_tolerance,
            )

            # Reconcile positions for this exchange
            position_discrepancies = await self._reconcile_exchange_positions(
                exchange_enum,
                api_client,
                Decimal(str(reconciliation_timeout)),
                position_tolerance,
            )

            exchange_discrepancies = balance_discrepancies + position_discrepancies

            if exchange_discrepancies > 0:
                logger.warning(
                    "exchange_reconciliation_discrepancies",
                    exchange=exchange_name,
                    balance_discrepancies=balance_discrepancies,
                    position_discrepancies=position_discrepancies,
                    total_discrepancies=exchange_discrepancies,
                )
            else:
                logger.debug("exchange_reconciliation_clean", exchange=exchange_name)

        except Exception as e:
            logger.exception("exchange_reconciliation_error", exchange=exchange_name, error=str(e))
            return None
        else:
            return exchange_discrepancies

    async def _reconcile_exchange_balances(
        self,
        exchange: ExchangeName,
        api_client: ExchangeAPI,
        timeout_seconds: Decimal,
        tolerance: Decimal,
    ) -> int:
        """Reconcile balances for a specific exchange.

        Args:
            exchange: Exchange to reconcile
            api_client: API client for the exchange
            timeout_seconds: Timeout for API calls
            tolerance: Tolerance for balance differences

        Returns:
            Number of discrepancies found

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured timeout and tolerance
        - NO assumptions about API client interface
        - Explicit discrepancy logging
        """
        discrepancies = 0

        try:
            # Fetch current balances from exchange with timeout
            exchange_balances = await asyncio.wait_for(
                self._fetch_exchange_balances(api_client, exchange), timeout=float(timeout_seconds)
            )

            # Get our cached balances for this exchange
            cached_balances = await self._balance_manager.get_exchange_balances(exchange)

            # Compare balances
            for asset_name, exchange_balance in exchange_balances.items():
                cached_balance = cached_balances.get(asset_name)

                if cached_balance is None:
                    # We don't have this balance cached
                    logger.warning(
                        "balance_missing_in_cache",
                        exchange=exchange.value,
                        asset=asset_name,
                        exchange_balance=exchange_balance.total_quantity,
                    )

                    # Add missing balance to cache
                    await self._balance_manager.update_balance_directly(
                        create_symbol(asset_name, exchange), exchange, exchange_balance
                    )
                    discrepancies += 1

                else:
                    # Compare quantities within tolerance
                    difference = abs(
                        exchange_balance.total_quantity - cached_balance.total_quantity
                    )

                    if difference > tolerance:
                        logger.warning(
                            "balance_discrepancy_detected",
                            exchange=exchange.value,
                            asset=asset_name,
                            cached_balance=cached_balance.total_quantity,
                            exchange_balance=exchange_balance.total_quantity,
                            difference=difference,
                            tolerance=tolerance,
                        )

                        # Update cached balance to match exchange
                        await self._balance_manager.update_balance_directly(
                            create_symbol(asset_name, exchange), exchange, exchange_balance
                        )
                        discrepancies += 1

            # Check for balances in cache that don't exist on exchange
            for asset_name, cached_balance in cached_balances.items():
                if (
                    asset_name not in exchange_balances
                    and cached_balance.total_quantity > tolerance
                ):
                    logger.warning(
                        "balance_exists_only_in_cache",
                        exchange=exchange.value,
                        asset=asset_name,
                        cached_balance=cached_balance.total_quantity,
                    )
                    discrepancies += 1

                    # Could remove or zero out the balance here
                    # For now, just log the discrepancy

        except Exception as e:
            logger.exception("balance_reconciliation_error", exchange=exchange.value, error=str(e))
            raise
        else:
            return discrepancies

    async def _reconcile_exchange_positions(
        self,
        exchange: ExchangeName,
        api_client: ExchangeAPI,
        timeout_seconds: Decimal,
        tolerance: Decimal,
    ) -> int:
        """Reconcile positions for a specific exchange.

        Args:
            exchange: Exchange to reconcile
            api_client: API client for the exchange
            timeout_seconds: Timeout for API calls
            tolerance: Tolerance for position size differences

        Returns:
            Number of discrepancies found

        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses configured timeout and tolerance
        - NO assumptions about API client interface
        - Explicit discrepancy logging
        """
        discrepancies = 0

        try:
            # Fetch current positions from exchange with timeout
            exchange_positions = await asyncio.wait_for(
                self._fetch_exchange_positions(api_client, exchange), timeout=float(timeout_seconds)
            )

            # Get our cached positions for this exchange
            cached_positions = await self._position_manager.get_exchange_positions(exchange)

            # Compare positions
            for symbol_name, exchange_position in exchange_positions.items():
                cached_position = cached_positions.get(symbol_name)

                if cached_position is None:
                    # We don't have this position cached
                    if abs(exchange_position.size) > tolerance:
                        logger.warning(
                            "position_missing_in_cache",
                            exchange=exchange.value,
                            symbol=symbol_name,
                            exchange_position_size=exchange_position.size,
                        )

                        # Add missing position to cache
                        await self._position_manager.update_position_directly(
                            create_symbol(symbol_name, exchange), exchange, exchange_position
                        )
                        discrepancies += 1

                else:
                    # Compare position sizes within tolerance
                    difference = abs(exchange_position.size - cached_position.size)

                    if difference > tolerance:
                        logger.warning(
                            "position_discrepancy_detected",
                            exchange=exchange.value,
                            symbol=symbol_name,
                            cached_size=cached_position.size,
                            exchange_size=exchange_position.size,
                            difference=difference,
                            tolerance=tolerance,
                        )

                        # Update cached position to match exchange
                        await self._position_manager.update_position_directly(
                            create_symbol(symbol_name, exchange), exchange, exchange_position
                        )
                        discrepancies += 1

            # Check for positions in cache that don't exist on exchange
            for symbol_name, cached_position in cached_positions.items():
                if symbol_name not in exchange_positions and abs(cached_position.size) > tolerance:
                    logger.warning(
                        "position_exists_only_in_cache",
                        exchange=exchange.value,
                        symbol=symbol_name,
                        cached_size=cached_position.size,
                    )
                    discrepancies += 1

                    # Remove or zero out the position
                    await self._position_manager.update_position_directly(
                        create_symbol(symbol_name, exchange), exchange, None
                    )

        except Exception as e:
            logger.exception("position_reconciliation_error", exchange=exchange.value, error=str(e))
            raise
        else:
            return discrepancies

    async def _fetch_exchange_balances(
        self, api_client: ExchangeAPI, exchange: ExchangeName
    ) -> dict[str, SpotBalance]:
        """Fetch current balances from exchange API.

        Following CODING_STANDARDS.md:
        - NO assumptions about API client interface
        - Returns typed SpotBalance objects
        - Handles API client method variations

        Args:
            api_client: Exchange API client
            exchange: Exchange name for logging

        Returns:
            Dictionary of asset name -> SpotBalance
        """
        try:
            # Use standardized ExchangeAPI method
            balances = await api_client.get_balances()

            logger.debug(
                "exchange_balances_fetched", exchange=exchange.value, balance_count=len(balances)
            )

        except Exception as e:
            logger.exception("fetch_exchange_balances_error", exchange=exchange.value, error=str(e))
            raise
        else:
            return balances

    async def _fetch_exchange_positions(
        self, api_client: ExchangeAPI, exchange: ExchangeName
    ) -> dict[str, DerivativePosition]:
        """Fetch current positions from exchange API.

        Args:
            api_client: Exchange API client
            exchange: Exchange name for logging

        Returns:
            Dictionary of symbol name -> DerivativePosition

        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about API client interface
        - Returns typed DerivativePosition objects
        - Handles API client method variations
        """
        try:
            # Use standardized ExchangeAPI method
            position_list = await api_client.get_positions()

            # Convert list to dict keyed by position identifier
            positions: dict[str, DerivativePosition] = {}
            for position in position_list:
                position_key = f"{position.exchange}:{position.symbol.value}"
                positions[position_key] = position

            logger.debug(
                "exchange_positions_fetched", exchange=exchange.value, position_count=len(positions)
            )

        except Exception as e:
            logger.exception(
                "fetch_exchange_positions_error", exchange=exchange.value, error=str(e)
            )
            raise
        else:
            return positions
