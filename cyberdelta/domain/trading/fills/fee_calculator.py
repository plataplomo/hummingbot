"""Fee calculation using exchange-specific configurations.

This module calculates fees for order fills using exchange-specific fee structures
from validated AppSettings configuration.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.models.fee_config import FeeStructureConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName
from cyberdelta.models.market.order import Order


logger = get_logger(__name__)


class FeeCalculator:
    """Calculates fees using exchange-specific fee structures from configuration.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Uses config.exchanges[exchange].fee_structure for all calculations
    - NO hardcoded fee rates or structures
    - Handles maker/taker differences from configuration
    - Returns Decimal fee amount, NOT float
    """

    @staticmethod
    def calculate_fee(
        order: Order,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fill_data: dict[str, object],
        exchange_config: ExchangeSpecificConfig,
    ) -> tuple[Decimal, str]:
        """Calculate fee for order fill using exchange configuration.

        Args:
            order: Order that was filled
            fill_price: Price at which order was filled
            fill_quantity: Quantity that was filled
            fill_data: Additional fill data from exchange
            exchange_config: Exchange configuration from AppSettings

        Returns:
            Tuple of (fee_amount, fee_asset)

        Note:
        - Uses config.exchanges[exchange].fee_structure for all calculations
        - NO hardcoded fee rates or structures
        - Handles maker/taker differences from configuration
        - Returns Decimal fee amount, NOT float
        """
        fee_structure = FeeCalculator._get_fee_structure(exchange_config, order.exchange)

        # Get fee rate based on liquidity
        fee_rate = FeeCalculator._get_fee_rate(fee_structure, fill_data)

        # Calculate base fee amount
        fee_amount = FeeCalculator._calculate_base_fee(
            fee_structure,
            fill_price,
            fill_quantity,
            fee_rate,
        )

        # Apply fee limits
        fee_amount = FeeCalculator._apply_fee_limits(fee_structure, fee_amount, order.exchange)

        # Determine fee asset
        fee_asset = FeeCalculator._determine_fee_asset(fee_structure, order)

        FeeCalculator._log_fee_calculation(
            order.exchange,
            fill_data,
            fee_rate,
            fee_structure,
            fill_price,
            fill_quantity,
            fee_amount,
            fee_asset,
        )

        return fee_amount, fee_asset

    @staticmethod
    def _get_fee_structure(
        exchange_config: ExchangeSpecificConfig,
        exchange: ExchangeName,
    ) -> FeeStructureConfig:
        """Get fee structure from exchange config.

        Returns:
            FeeStructureConfig: Fee structure settings

        Raises:
            ValueError: If fee structure is not configured
        """
        if not exchange_config.fee_structure:
            msg = (
                f"Fee structure not configured for exchange {exchange.value}. "
                "Configure fee_structure in exchange config."
            )
            raise ValueError(msg)

        return exchange_config.fee_structure

    @staticmethod
    def _get_fee_rate(
        fee_structure: FeeStructureConfig,
        fill_data: dict[str, object],
    ) -> Decimal:
        """Get appropriate fee rate based on liquidity.

        Returns:
            Decimal: Fee rate as Decimal
        """
        liquidity = fill_data.get("liquidity", "taker")

        if liquidity == "maker":
            return Decimal(str(fee_structure.maker_fee_rate))
        if liquidity == "taker":
            return Decimal(str(fee_structure.taker_fee_rate))

        # Default to taker fee if no specific liquidity type
        return Decimal(str(fee_structure.taker_fee_rate))

    @staticmethod
    def _calculate_base_fee(
        fee_structure: FeeStructureConfig,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fee_rate: Decimal,
    ) -> Decimal:
        """Calculate base fee amount.

        Returns:
            Decimal: Calculated fee amount
        """
        fee_method = fee_structure.fee_calculation_method

        if fee_method == "percentage":
            trade_value = fill_price * fill_quantity
            return trade_value * fee_rate

        # Default case: fixed fee method
        return fee_rate

    @staticmethod
    def _apply_fee_limits(
        fee_structure: FeeStructureConfig,
        fee_amount: Decimal,
        exchange: ExchangeName,
    ) -> Decimal:
        """Apply minimum and maximum fee limits.

        Returns:
            Decimal: Fee amount after applying limits
        """
        # Apply minimum fee if configured
        if fee_structure.minimum_fee is not None:
            fee_amount = max(fee_amount, Decimal(str(fee_structure.minimum_fee)))

        # Apply maximum fee if configured
        if fee_structure.maximum_fee is not None:
            fee_amount = min(fee_amount, Decimal(str(fee_structure.maximum_fee)))

        return fee_amount

    @staticmethod
    def _determine_fee_asset(fee_structure: FeeStructureConfig, order: Order) -> str:
        """Determine fee asset from structure or symbol.

        Returns:
            str: Fee asset symbol
        """
        if fee_structure.fee_asset:
            return fee_structure.fee_asset

        # Default: use quote currency from symbol
        symbol_parts = order.symbol.value.split("_")
        return symbol_parts[-1] if len(symbol_parts) > 1 else "USDC"

    @staticmethod
    def _log_fee_calculation(
        exchange: ExchangeName,
        fill_data: dict[str, object],
        fee_rate: Decimal,
        fee_structure: FeeStructureConfig,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fee_amount: Decimal,
        fee_asset: str,
    ) -> None:
        """Log fee calculation details."""
        exchange_name = exchange.value
        liquidity = fill_data.get("liquidity", "taker")
        fee_method = fee_structure.fee_calculation_method

        logger.debug(
            "fee_calculated",
            exchange=exchange_name,
            liquidity=liquidity,
            fee_rate=float(fee_rate),
            fee_method=fee_method,
            trade_value=float(fill_price * fill_quantity),
            fee_amount=float(fee_amount),
            fee_asset=fee_asset,
        )
