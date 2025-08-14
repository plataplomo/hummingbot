"""Fee calculation using exchange-specific configurations.

This module calculates fees for order fills using exchange-specific fee structures
from validated AppSettings configuration. Moved from trading domain to financial domain
for consolidation of all financial calculations.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.models.fee_config import FeeStructureConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName, MakerTaker
from cyberdelta.models.financial import FeeResult
from cyberdelta.models.market.fill import Fill


logger = get_logger(__name__)


class FeeCalculator:
    """Calculates fees using exchange-specific fee structures from configuration.

    IMPORTANT: Following CODING_STANDARDS.md:
    - Uses config.exchanges[exchange].fee_structure for all calculations
    - NO hardcoded fee rates or structures
    - Handles maker/taker differences from configuration
    - Returns comprehensive FeeResult model, NOT simple tuple
    - Implements FeeCalculatorProtocol interface
    """

    def calculate_fee(
        self,
        fill: Fill,
        exchange_config: ExchangeSpecificConfig | dict[str, Any],
    ) -> FeeResult:
        """Calculate fees for a fill based on exchange-specific rules.

        This method implements the FeeCalculatorProtocol interface and provides
        comprehensive fee calculation with detailed result metadata.

        Args:
            fill: The fill to calculate fees for
            exchange_config: Exchange-specific configuration including
                           fee rates, VIP tiers, discount tokens, etc.

        Returns:
            FeeResult with calculated fee amount, currency, and calculation details
        """
        # Convert dict config to proper config object for compatibility
        if isinstance(exchange_config, dict):
            config_obj = ExchangeSpecificConfig(**exchange_config)
        else:
            config_obj = exchange_config

        fee_structure = self._get_fee_structure(config_obj, fill.exchange)

        # Get fee rate based on liquidity
        fee_rate = self._get_fee_rate(fee_structure, fill)

        # Calculate base fee amount
        notional_value = fill.price * fill.quantity
        base_fee = self._calculate_base_fee(
            fee_structure,
            fill.price,
            fill.quantity,
            fee_rate,
        )

        # Apply fee limits
        final_fee = self._apply_fee_limits(fee_structure, base_fee, fill.exchange)

        # Calculate discount if applicable
        discount_amount = base_fee - final_fee if final_fee < base_fee else Decimal(0)

        # Determine fee asset
        fee_currency = self._determine_fee_asset(fee_structure, fill)

        # Determine calculation method description
        maker_taker_str = fill.maker_taker.value.lower() if fill.maker_taker else "taker"
        calculation_method = f"{fee_structure.fee_calculation_method}_{maker_taker_str}"

        self._log_fee_calculation(
            fill.exchange,
            fill,
            fee_rate,
            fee_structure,
            fill.price,
            fill.quantity,
            final_fee,
            fee_currency,
        )

        return FeeResult(
            amount=final_fee,
            currency=fee_currency,
            calculation_method=calculation_method,
            calculation_timestamp=datetime.now(UTC),
            exchange=fill.exchange.value,
            fee_rate=fee_rate,
            is_maker=fill.maker_taker == MakerTaker.MAKER if fill.maker_taker else False,
            notional_value=notional_value,
            base_fee=base_fee,
            discount_amount=discount_amount if discount_amount > 0 else None,
            discount_type="fee_limits" if discount_amount > 0 else None,
            precision=None,
        )

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
        fill: Fill,
    ) -> Decimal:
        """Get appropriate fee rate based on liquidity.

        Returns:
            Decimal: Fee rate as Decimal
        """
        # Use maker_taker field from Fill model, default to TAKER if not specified
        maker_taker = fill.maker_taker if fill.maker_taker is not None else MakerTaker.TAKER

        if maker_taker == MakerTaker.MAKER:
            return Decimal(str(fee_structure.maker_fee_rate))
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

    def _determine_fee_asset(self, fee_structure: FeeStructureConfig, fill: Fill) -> str:
        """Determine fee asset from structure or fill information.

        Returns:
            str: Fee asset symbol

        Raises:
            ValueError: If fee asset cannot be determined from configuration or fill
        """
        if fee_structure.fee_asset:
            return fee_structure.fee_asset

        # Extract quote currency from symbol - following CODING_STANDARDS.md
        if hasattr(fill, "symbol") and fill.symbol:
            symbol_parts = fill.symbol.value.split("_")
            if len(symbol_parts) > 1:
                return symbol_parts[-1]

        # Following CODING_STANDARDS.md: NO hardcoded fallbacks
        msg = (
            f"Cannot determine fee asset for fill on {fill.exchange.value}. "
            f"Configure fee_asset in exchange fee_structure or ensure "
            f"symbol format contains quote currency: {getattr(fill, 'symbol', 'unknown')}"
        )
        raise ValueError(msg)

    @staticmethod
    def _log_fee_calculation(
        exchange: ExchangeName,
        fill: Fill,
        fee_rate: Decimal,
        fee_structure: FeeStructureConfig,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fee_amount: Decimal,
        fee_asset: str,
    ) -> None:
        """Log fee calculation details."""
        exchange_name = exchange.value
        # Use maker_taker field from Fill model to determine liquidity type
        liquidity = fill.maker_taker.value.lower() if fill.maker_taker else "taker"
        fee_method = fee_structure.fee_calculation_method

        logger.debug(
            "fee_calculated",
            exchange=exchange_name,
            liquidity=liquidity,
            fee_rate=fee_rate,
            fee_method=fee_method,
            trade_value=fill_price * fill_quantity,
            fee_amount=fee_amount,
            fee_asset=fee_asset,
        )
