"""Shared helpers for structured logging of financial events."""

from __future__ import annotations

import structlog
from pydantic import BaseModel

from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.margin_account import MarginAccountSummary
from cyberdelta.core.models.market.order import Order
from cyberdelta.core.models.market.trade import Trade
from cyberdelta.core.models.trade_signal import TradeSignal


# Define sensitive fields to exclude per model type
SENSITIVE_FIELDS: dict[type[BaseModel], set[str]] = {
    Order: {"trades", "hl_details", "bp_details"},
    Trade: {"hl_details", "bp_details"},
    TradeSignal: {"metadata"},  # May contain strategy-specific sensitive data
    DerivativePosition: {"hl_details", "bp_details"},
    MarginAccountSummary: {"total_equity", "available_equity", "hl_details", "bp_details"},
}


def log_trading_event(
    logger: structlog.BoundLogger,
    event_type: str,
    model: BaseModel,
    exclude_sensitive: bool = True,
    **extra_context: object,
) -> None:
    """Log a trading event using existing models.

    Args:
        logger: Structlog logger instance
        event_type: Type of event (e.g., "order_placed", "position_opened")
        model: Pydantic model instance to log
        exclude_sensitive: Whether to exclude sensitive fields
        **extra_context: Additional context to include in the log
    """
    exclude_fields: set[str] = set()
    if exclude_sensitive:
        exclude_fields = SENSITIVE_FIELDS.get(type(model), set())

    logger.info(
        event_type,
        **model.model_dump(mode="json", exclude=exclude_fields),
        **extra_context,
    )


def log_order_lifecycle(
    logger: structlog.BoundLogger,
    order: Order,
    event: str,
    **context: object,
) -> None:
    """Log order lifecycle events with consistent structure.

    Args:
        logger: Structlog logger instance
        order: Order instance
        event: Lifecycle event (placed, filled, cancelled, etc.)
        **context: Additional context
    """
    log_trading_event(
        logger,
        f"order_{event}",
        order,
        True,  # exclude_sensitive
        order_lifecycle_event=event,
        **context,
    )


def log_position_update(
    logger: structlog.BoundLogger,
    position: DerivativePosition,
    action: str,
    **context: object,
) -> None:
    """Log position updates with consistent structure.

    Args:
        logger: Structlog logger instance
        position: Position instance
        action: Position action (opened, closed, updated, liquidated)
        **context: Additional context
    """
    log_trading_event(
        logger,
        f"position_{action}",
        position,
        True,  # exclude_sensitive
        position_action=action,
        **context,
    )
