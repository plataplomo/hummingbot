"""Portfolio Reconciliation Service - replaces PortfolioOrchestrator with clean service architecture."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.portfolio_types.infrastructure import EventType, PortfolioEvent

if TYPE_CHECKING:
    from cyberdelta.core.portfolio.services import PortfolioServiceFactory

logger = get_logger(__name__)


class PortfolioReconciliationService(BaseModel):
    """Replaces PortfolioOrchestrator with clean service architecture."""

    service_factory: PortfolioServiceFactory = Field(..., description="Portfolio service factory")

    model_config = ConfigDict(extra="forbid", validate_assignment=True, arbitrary_types_allowed=True)

    def model_post_init(self, __context: Any) -> None:
        """Initialize service instances after Pydantic validation."""
        self.portfolio_manager = self.service_factory.create_portfolio_state_manager()
        self.exchange_service = self.service_factory.create_exchange_service()
        self.validation_service = self.service_factory.create_validation_service()
        
        # Event dispatcher if available
        if hasattr(self.service_factory, 'create_event_dispatcher'):
            self.event_dispatcher = self.service_factory.create_event_dispatcher()
        else:
            self.event_dispatcher = None
            
        logger.info("PortfolioReconciliationService initialized")

    async def reconcile_all_exchanges(self) -> None:
        """Full portfolio reconciliation across all exchanges."""
        logger.info("Starting full portfolio reconciliation")

        try:
            # Fetch data from all exchanges in parallel
            exchange_data = await self.exchange_service.fetch_all_portfolio_data()
            logger.info(f"Fetched data from {len(exchange_data)} exchanges")

            # Validate incoming data
            validation_result = await self.validation_service.validate_portfolio_data(exchange_data)
            if not validation_result.is_valid:
                await self._handle_validation_errors(validation_result)
                return

            # Update portfolio state
            await self.portfolio_manager.update_from_exchange_data(exchange_data)
            logger.info("Portfolio state updated from exchange data")

            # Dispatch reconciliation complete event
            if self.event_dispatcher:
                event = PortfolioEvent(
                    event_type=EventType.RECONCILIATION_COMPLETE,
                    exchange_id="all",
                    timestamp=datetime.now(UTC),
                    data={"exchange_count": len(exchange_data)}
                )
                await self.event_dispatcher.dispatch(event)

            logger.info("Full portfolio reconciliation completed successfully")

        except Exception as e:
            logger.exception("Failed to complete full portfolio reconciliation", error=str(e))
            raise

    async def reconcile_exchange(self, exchange_id: str) -> None:
        """Reconcile specific exchange."""
        logger.info(f"Starting reconciliation for exchange: {exchange_id}")

        try:
            # Fetch data from specific exchange
            exchange_data = await self.exchange_service.fetch_exchange_data(exchange_id)
            logger.info(f"Fetched data for exchange: {exchange_id}")

            # Update portfolio state for this exchange
            await self.portfolio_manager.update_exchange_data(exchange_id, exchange_data)
            logger.info(f"Portfolio state updated for exchange: {exchange_id}")

            # Dispatch exchange reconciliation complete event
            if self.event_dispatcher:
                event = PortfolioEvent(
                    event_type=EventType.EXCHANGE_RECONCILED,
                    exchange_id=exchange_id,
                    timestamp=datetime.now(UTC),
                    data={"exchange_id": exchange_id}
                )
                await self.event_dispatcher.dispatch(event)

        except Exception as e:
            logger.exception(f"Failed to reconcile exchange {exchange_id}", error=str(e))
            raise

    async def _handle_validation_errors(self, validation_result: Any) -> None:
        """Handle validation errors during reconciliation."""
        errors = getattr(validation_result, 'errors', [])
        logger.error(
            "Portfolio data validation failed during reconciliation",
            errors=errors,
            validation_result=str(validation_result)
        )
        
        # Create validation failure event
        if self.event_dispatcher:
            event = PortfolioEvent(
                event_type=EventType.VALIDATION_FAILED,
                exchange_id="validation",
                timestamp=datetime.now(UTC),
                data={"errors": errors, "validation_result": str(validation_result)}
            )
            await self.event_dispatcher.dispatch(event)