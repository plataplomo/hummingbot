"""Reconciliation protocols for portfolio operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI
    from cyberdelta.models.portfolio.pnl_report import ReconciliationReport


class ReconciliationEngineProtocol(Protocol):
    """Protocol for exchange reconciliation operations.

    Defines the interface for reconciling portfolio state with
    actual exchange balances and positions to ensure data consistency.
    """

    async def reconcile_with_exchanges(
        self, api_clients: dict[str, ExchangeAPI]
    ) -> ReconciliationReport:
        """Reconcile portfolio state with actual exchange balances and positions.

        Args:
            api_clients: Dictionary of exchange API clients

        Returns:
            Typed reconciliation results including errors and discrepancies
        """
        ...
