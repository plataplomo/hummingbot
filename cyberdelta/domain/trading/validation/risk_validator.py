"""Risk constraint validation against configured risk limits.

This module validates orders against risk constraints like position limits
using validated AppSettings configuration.
"""

from __future__ import annotations

from cyberdelta.config.models import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.models.market.order import Order


logger = get_logger(__name__)


class RiskValidator:
    """Validates orders against risk constraints from configuration.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Check against config.risk.global_risk limits
    - Uses configured risk parameters
    - NO hardcoded risk limits
    - All monetary values as Decimal, NOT float
    """

    def __init__(self, config: AppSettings) -> None:
        """Initialize risk validator with configuration.

        Args:
            config: Application settings containing all risk configuration
        """
        self.config = config

    async def validate(self, order: Order) -> list[str]:
        """Validate order against risk constraints.

        Args:
            order: Order to validate

        Returns:
            List of risk constraint violations

        Note:
        - Check against config.risk.global_risk limits
        - Uses configured risk parameters
        - NO hardcoded risk limits
        """
        violations: list[str] = []

        try:
            # Check against global risk limits
            # Risk configuration is always available in AppSettings
            global_risk = self.config.risk.global_risk

            # Check maximum position size
            if order.price:
                max_position = global_risk.max_position_usd
                order_value = order.quantity_requested * order.price

                if order_value > max_position:
                    violations.append(
                        f"Order value ${order_value} exceeds maximum position ${max_position}"
                    )

            # Check validation-specific constraints
            # Validation configuration is always available in AppSettings
            validation_config = self.config.validation

            # Check order value bounds
            if order.price:
                # min_trade_value validation
                min_value = validation_config.min_trade_value
                order_value = order.quantity_requested * order.price

                if order_value < min_value:
                    violations.append(f"Order value ${order_value} below minimum ${min_value}")

                # max_trade_value validation
                max_value = validation_config.max_trade_value
                if order_value > max_value:
                    violations.append(f"Order value ${order_value} exceeds maximum ${max_value}")

        except Exception as e:
            logger.exception(
                "risk_constraint_validation_error",
                order_id=order.exchange_order_id,
                error=str(e),
            )
            violations.append(f"Risk constraint validation failed: {e!s}")

        return violations
