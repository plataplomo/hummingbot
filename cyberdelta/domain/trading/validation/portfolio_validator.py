"""Portfolio constraint validation against available balances.

This module validates orders against portfolio constraints like available balance
using validated AppSettings configuration.
"""

from __future__ import annotations

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.enums import ExchangeName, OrderSide
from cyberdelta.models.market.order import Order
from cyberdelta.symbols import bp_symbol, hl_symbol


logger = get_logger(__name__)


class PortfolioValidator:
    """Validates orders against portfolio constraints.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Check available balance from portfolio service
    - Uses Symbol objects, NOT strings
    - NO assumptions about balance availability
    - All monetary values as Decimal, NOT float
    """

    def __init__(self, portfolio_service: PortfolioService) -> None:
        """Initialize portfolio validator with dependencies.

        Args:
            portfolio_service: Portfolio service for balance validation
        """
        self._portfolio_service = portfolio_service

    async def validate(self, order: Order) -> list[str]:
        """Validate order against portfolio constraints.

        Args:
            order: Order to validate

        Returns:
            List of portfolio constraint violations

        Note:
        - Check available balance from portfolio service
        - Uses Symbol objects, NOT strings
        - NO assumptions about balance availability
        """
        violations: list[str] = []

        try:
            # For buy orders, check quote currency balance
            if order.side == OrderSide.BUY and order.price:
                violations.extend(await self._validate_buy_balance(order))

            # For sell orders, check base currency balance
            elif order.side == OrderSide.SELL:
                violations.extend(await self._validate_sell_balance(order))

        except Exception as e:
            logger.exception(
                "portfolio_constraint_validation_error",
                order_id=order.exchange_order_id,
                error=str(e),
            )
            violations.append(f"Portfolio constraint validation failed: {e!s}")

        return violations

    async def _validate_buy_balance(self, order: Order) -> list[str]:
        """Validate balance for buy order.

        Args:
            order: Buy order to validate

        Returns:
            List of violations if any
        """
        violations: list[str] = []

        # Get quote asset (simplified - would use symbol service in practice)
        quote_asset_str = order.symbol.value.split("_")[-1] if "_" in order.symbol.value else "USDC"
        if order.exchange == ExchangeName.HYPERLIQUID:
            quote_symbol = hl_symbol(quote_asset_str)
        else:
            quote_symbol = bp_symbol(quote_asset_str)

        balance = await self._portfolio_service.get_balance(quote_symbol, order.exchange)
        if order.price is None:
            violations.append("Cannot validate buy order without price")
            return violations
        required_amount = order.quantity_requested * order.price

        if not balance:
            violations.append(
                f"No {quote_asset_str} balance found on {order.exchange} for buy order"
            )
        elif balance.available_quantity < required_amount:
            violations.append(
                f"Insufficient {quote_asset_str} balance: need {required_amount}, "
                f"available {balance.available_quantity}"
            )

        return violations

    async def _validate_sell_balance(self, order: Order) -> list[str]:
        """Validate balance for sell order.

        Args:
            order: Sell order to validate

        Returns:
            List of violations if any
        """
        violations: list[str] = []

        # Get base asset
        base_asset_str = (
            order.symbol.value.split("_")[0] if "_" in order.symbol.value else order.symbol.value
        )
        if order.exchange == ExchangeName.HYPERLIQUID:
            base_symbol = hl_symbol(base_asset_str)
        else:
            base_symbol = bp_symbol(base_asset_str)

        balance = await self._portfolio_service.get_balance(base_symbol, order.exchange)

        if not balance:
            violations.append(
                f"No {base_asset_str} balance found on {order.exchange} for sell order"
            )
        elif balance.available_quantity < order.quantity_requested:
            violations.append(
                f"Insufficient {base_asset_str} balance: need {order.quantity_requested}, "
                f"available {balance.available_quantity}"
            )

        return violations
