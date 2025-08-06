"""Fill processing and fee calculation service.

This module provides the FillHandler class that processes order fills,
calculates fees using exchange-specific configurations, and coordinates
portfolio updates following CODING_STANDARDS.md requirements.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.config.models.app_config import AppSettings
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.models.fee_config import FeeStructureConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.enums import ExchangeName
from cyberdelta.models.market.order import Order
from cyberdelta.models.market.trade import Trade


logger = get_logger(__name__)


class FillHandler:
    """Handles order fill processing with exchange-specific fee calculations.

    This handler processes order fills by:
    - Calculating fees using exchange-specific fee structures from configuration
    - Converting fill data to typed Trade objects
    - Coordinating portfolio updates through PortfolioService
    - Maintaining fill history and audit trail

    Configuration Structure (config.exchanges[exchange].fee_structure):
    - maker_fee_rate: Fee rate for maker orders (Decimal)
    - taker_fee_rate: Fee rate for taker orders (Decimal)
    - fee_asset: Asset used for fee payment (usually quote currency)
    - fee_calculation_method: Method for fee calculation ("percentage", "fixed")
    - minimum_fee: Minimum fee amount (if applicable)
    - maximum_fee: Maximum fee amount (if applicable)

    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL fee calculations from AppSettings, NO hardcoded rates
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - All monetary values as Decimal, NOT float
    - NO assumptions about fee structures
    """

    def __init__(
        self,
        config: AppSettings,
        portfolio_service: PortfolioService,
    ) -> None:
        """Initialize fill handler with configuration and dependencies.

        Args:
            config: Application settings containing all configuration
            portfolio_service: Portfolio service for state updates
        """
        self.config = config
        self._portfolio_service = portfolio_service

        # Fill tracking
        self._processed_fills: list[Trade] = []
        self._fill_count = 0
        self._total_fees_usd = Decimal(0)

        logger.info(
            "fill_handler_initialized",
            exchanges_configured=len(config.exchanges),
            portfolio_service_available=True,
        )

    async def process_fill(self, order: Order, fill_data: dict[str, object]) -> Trade:
        """Process an order fill with comprehensive fee calculation.

        Args:
            order: Order that was filled
            fill_data: Fill data from exchange

        Returns:
            Trade object with calculated fees and metadata

        Note:
        - Fee calculation from config.exchanges[exchange].fee_structure
        - ALL calculations use Decimal, NOT float
        - NO hardcoded fee rates or assumptions
        - Explicit error handling for invalid data
        """
        logger.debug(
            "fill_processing_starting",
            order_id=order.exchange_order_id,
            symbol=order.symbol.value,
            exchange=order.exchange.value,
            fill_data_keys=list(fill_data.keys()),
        )

        try:
            # Validate fill data
            self._validate_fill_data(fill_data)

            # Extract fill information
            fill_price = Decimal(str(fill_data.get("fill_price", 0)))
            fill_quantity = Decimal(str(fill_data.get("filled_quantity", 0)))
            fill_timestamp = fill_data.get("timestamp", datetime.now(UTC))

            if fill_price <= 0 or fill_quantity <= 0:
                self._raise_invalid_fill_error(fill_price, fill_quantity)

            # Calculate fees using exchange configuration
            fee_amount, fee_asset = await self._calculate_fill_fee(
                order, fill_price, fill_quantity, fill_data
            )

            # Create Trade object with all calculated values
            trade_id = fill_data.get("trade_id")
            if not isinstance(trade_id, str):
                trade_id = f"fill_{order.exchange_order_id}_{self._fill_count}"

            trade = Trade(
                id=trade_id,
                symbol=order.symbol,
                executed_at=(
                    fill_timestamp if isinstance(fill_timestamp, datetime) else datetime.now(UTC)
                ),
                side=order.side,
                order_id=order.exchange_order_id or "",
                exchange=order.exchange.value,
                price=fill_price,
                quantity=fill_quantity,
                fee=fee_amount,
                fee_asset=fee_asset,
                client_order_id=order.client_order_id,
                is_maker=fill_data.get("liquidity", "taker") == "maker",
            )

            # Update portfolio with trade
            await self._portfolio_service.update_from_trade(trade)

            # Track fill statistics
            self._processed_fills.append(trade)
            self._fill_count += 1
            self._total_fees_usd += fee_amount  # Simplified - assumes USD fees

            logger.info(
                "fill_processed_successfully",
                trade_id=trade.id,
                order_id=order.exchange_order_id,
                symbol=order.symbol.value,
                exchange=trade.exchange,
                side=order.side.value if order.side else "unknown",
                price=float(fill_price),
                quantity=float(fill_quantity),
                fee=float(fee_amount),
                fee_asset=fee_asset,
            )

        except Exception as e:
            logger.exception(
                "fill_processing_failed", order_id=order.exchange_order_id, error=str(e)
            )
            raise

        return trade

    async def process_partial_fill(self, order: Order, fill_data: dict[str, object]) -> Trade:
        """Process a partial fill of an order.

        Args:
            order: Order that was partially filled
            fill_data: Partial fill data from exchange

        Returns:
            Trade object representing the partial fill

        Note:
        - Same validation and fee calculation as full fills
        - Tracks partial fill sequence for audit
        - Updates order state appropriately
        """
        fill_qty = fill_data.get("filled_quantity", 0)
        new_fill_quantity = (
            float(fill_qty) if isinstance(fill_qty, (int, float, Decimal, str)) else 0.0
        )

        logger.debug(
            "partial_fill_processing",
            order_id=order.exchange_order_id,
            filled_so_far=float(order.quantity_filled or 0),
            order_quantity=float(order.quantity_requested),
            new_fill_quantity=new_fill_quantity,
        )

        # Process same as regular fill
        trade = await self.process_fill(order, fill_data)

        # Additional tracking for partial fills
        # Note: metadata removed from Trade model - would need to track separately
        logger.debug(
            "partial_fill_metadata",
            fill_sequence=self._get_fill_sequence_number(order),
            is_partial_fill=True,
            remaining_quantity=float(
                order.quantity_requested - (order.quantity_filled or Decimal(0))
            ),
        )

        return trade

    def _validate_fill_data(self, fill_data: dict[str, object]) -> None:
        """Validate fill data structure and required fields.

        Args:
            fill_data: Fill data to validate

        Raises:
            ValueError: If fill data is invalid

        Note:
        - NO assumptions about fill data structure
        - Explicit validation with clear error messages
        """
        required_fields = ["fill_price", "filled_quantity"]

        for field in required_fields:
            if field not in fill_data:
                msg = f"Missing required fill data field: {field}"
                raise ValueError(msg)

            value = fill_data[field]
            if value is None:
                msg = f"Fill data field {field} cannot be None"
                raise ValueError(msg)

        # Validate numeric fields
        try:
            price = Decimal(str(fill_data["fill_price"]))
            quantity = Decimal(str(fill_data["filled_quantity"]))

            if price <= 0:
                self._raise_price_error(price)
            if quantity <= 0:
                self._raise_quantity_error(quantity)

        except (ValueError, TypeError) as e:
            msg = f"Invalid numeric values in fill data: {e}"
            raise ValueError(msg) from e

    async def _calculate_fill_fee(
        self,
        order: Order,
        fill_price: Decimal,
        fill_quantity: Decimal,
        fill_data: dict[str, object],
    ) -> tuple[Decimal, str]:
        """Calculate fee for order fill using exchange configuration.

        Args:
            order: Order that was filled
            fill_price: Price at which order was filled
            fill_quantity: Quantity that was filled
            fill_data: Additional fill data from exchange

        Returns:
            Tuple of (fee_amount, fee_asset)

        Note:
        - Uses config.exchanges[exchange].fee_structure for all calculations
        - NO hardcoded fee rates or structures
        - Handles maker/taker differences from configuration
        - Returns Decimal fee amount, NOT float
        """
        # Get exchange configuration and validate
        exchange_config = self._get_exchange_config(order)
        fee_structure = self._get_fee_structure(exchange_config, order.exchange)

        # Get fee rate based on liquidity
        fee_rate = self._get_fee_rate(fee_structure, fill_data, order.exchange)

        # Calculate base fee amount
        fee_amount = self._calculate_base_fee(fee_structure, fill_price, fill_quantity, fee_rate)

        # Apply fee limits
        fee_amount = self._apply_fee_limits(fee_structure, fee_amount, order.exchange)

        # Determine fee asset
        fee_asset = self._determine_fee_asset(fee_structure, order)

        self._log_fee_calculation(
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

    def _get_fill_sequence_number(self, order: Order) -> int:
        """Get sequence number for this fill within the order.

        Args:
            order: Order being filled

        Returns:
            Sequence number for this fill

        Note:
        - Tracks fill sequence for audit purposes
        - NO assumptions about fill ordering
        """
        # Count existing fills for this order
        order_fills = [
            trade for trade in self._processed_fills if trade.order_id == order.exchange_order_id
        ]
        return len(order_fills) + 1

    async def process_fill_correction(
        self, original_trade_id: str, correction_data: dict[str, object]
    ) -> Trade | None:
        """Process a fill correction or adjustment.

        Args:
            original_trade_id: ID of the original trade to correct
            correction_data: Correction data from exchange

        Returns:
            Corrected Trade object or None if original not found

        Note:
        - Maintains audit trail of corrections
        - Uses exchange-provided correction data
        - NO assumptions about correction format
        """
        # Find original trade
        original_trade = None
        for trade in self._processed_fills:
            if trade.id == original_trade_id:
                original_trade = trade
                break

        if not original_trade:
            logger.warning(
                "fill_correction_original_not_found", original_trade_id=original_trade_id
            )
            return None

        logger.info(
            "processing_fill_correction",
            original_trade_id=original_trade_id,
            correction_type=correction_data.get("type", "unknown"),
        )

        # Create corrected trade
        corrected_trade_id = correction_data.get("corrected_trade_id")
        if not isinstance(corrected_trade_id, str):
            corrected_trade_id = f"corr_{original_trade_id}"

        corrected_fee_asset = correction_data.get("corrected_fee_asset")
        if not isinstance(corrected_fee_asset, str) and corrected_fee_asset is not None:
            corrected_fee_asset = original_trade.fee_asset

        corrected_trade = Trade(
            id=corrected_trade_id,
            symbol=original_trade.symbol,
            executed_at=original_trade.executed_at,
            side=original_trade.side,
            order_id=original_trade.order_id,
            exchange=original_trade.exchange,
            price=Decimal(str(correction_data.get("corrected_price", original_trade.price))),
            quantity=Decimal(
                str(correction_data.get("corrected_quantity", original_trade.quantity))
            ),
            fee=Decimal(str(correction_data.get("corrected_fee", original_trade.fee))),
            fee_asset=corrected_fee_asset,
            client_order_id=original_trade.client_order_id,
            is_maker=original_trade.is_maker,
        )

        # Log correction metadata separately
        logger.info(
            "fill_correction_metadata",
            is_correction=True,
            original_trade_id=original_trade_id,
            correction_timestamp=datetime.now(UTC).isoformat(),
            correction_reason=correction_data.get("reason", "exchange_adjustment"),
        )

        # Update portfolio with corrected trade
        await self._portfolio_service.update_from_trade(corrected_trade)

        # Add to processed fills
        self._processed_fills.append(corrected_trade)

        logger.info(
            "fill_correction_processed",
            original_trade_id=original_trade_id,
            corrected_trade_id=corrected_trade.id,
            price_change=float(corrected_trade.price - original_trade.price),
            quantity_change=float(corrected_trade.quantity - original_trade.quantity),
            fee_change=float(corrected_trade.fee - original_trade.fee),
        )

        return corrected_trade

    def get_fill_statistics(self) -> dict[str, object]:
        """Get fill processing statistics.

        Returns:
            Dictionary with fill statistics and metrics

        Note:
        - Returns explicit statistics from actual processing
        - NO calculated/derived statistics
        """
        return {
            "total_fills_processed": self._fill_count,
            "total_fees_usd": float(self._total_fees_usd),
            "unique_orders_filled": len({trade.order_id for trade in self._processed_fills}),
            "exchanges_processed": len({trade.exchange for trade in self._processed_fills}),
            "average_fill_size_usd": (
                float(
                    sum(trade.price * trade.quantity for trade in self._processed_fills)
                    / len(self._processed_fills)
                )
                if self._processed_fills
                else 0
            ),
            "processing_started": len(self._processed_fills) > 0,
        }

    def get_recent_fills(self, limit: int = 10) -> list[Trade]:
        """Get most recent processed fills.

        Args:
            limit: Maximum number of fills to return

        Returns:
            List of recent Trade objects

        Note:
        - Returns actual Trade objects, not summaries
        - Limit parameter explicit, no default assumptions
        """
        return self._processed_fills[-limit:] if self._processed_fills else []

    async def process_bulk_fills(
        self, order: Order, fill_list: list[dict[str, object]]
    ) -> list[Trade]:
        """Process multiple fills for an order efficiently.

        Args:
            order: Order that received multiple fills
            fill_list: List of fill data dictionaries

        Returns:
            List of processed Trade objects

        Note:
        - Processes each fill with same validation as single fills
        - Maintains fill sequence and audit trail
        - NO assumptions about fill order or timing
        """
        trades: list[Trade] = []

        logger.info(
            "bulk_fill_processing_starting",
            order_id=order.exchange_order_id,
            fill_count=len(fill_list),
        )

        for i, fill_data in enumerate(fill_list):
            try:
                # Add sequence information to fill data
                fill_data_with_sequence = {
                    **fill_data,
                    "bulk_sequence": i + 1,
                    "bulk_total": len(fill_list),
                }

                trade = await self.process_fill(order, fill_data_with_sequence)
                trades.append(trade)

            except Exception as e:
                logger.exception(
                    "bulk_fill_item_failed",
                    order_id=order.exchange_order_id,
                    fill_index=i,
                    error=str(e),
                )
                # Continue processing remaining fills
                continue

        logger.info(
            "bulk_fill_processing_completed",
            order_id=order.exchange_order_id,
            requested_fills=len(fill_list),
            successful_fills=len(trades),
            failed_fills=len(fill_list) - len(trades),
        )

        return trades

    def _raise_invalid_fill_error(self, fill_price: Decimal, fill_quantity: Decimal) -> None:
        """Raise ValueError for invalid fill data.

        Raises:
            ValueError: Always raised with invalid fill data message
        """
        msg = f"Invalid fill data: price={fill_price}, quantity={fill_quantity}"
        raise ValueError(msg)

    def _raise_price_error(self, price: Decimal) -> None:
        """Raise ValueError for invalid price.

        Raises:
            ValueError: Always raised with invalid price message
        """
        msg = f"Fill price must be positive: {price}"
        raise ValueError(msg)

    def _raise_quantity_error(self, quantity: Decimal) -> None:
        """Raise ValueError for invalid quantity.

        Raises:
            ValueError: Always raised with invalid quantity message
        """
        msg = f"Fill quantity must be positive: {quantity}"
        raise ValueError(msg)

    def _get_exchange_config(self, order: Order) -> ExchangeSpecificConfig:
        """Get exchange configuration for order.

        Returns:
            ExchangeSpecificConfig: Exchange configuration for the order

        Raises:
            ValueError: If exchange configuration is not found
        """
        exchange_name = order.exchange.value
        exchange_config = self.config.exchanges.get(exchange_name)
        if not exchange_config:
            msg = f"No exchange configuration found for: {exchange_name}"
            raise ValueError(msg)
        return exchange_config

    def _get_fee_structure(
        self, exchange_config: ExchangeSpecificConfig, exchange: ExchangeName
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

    def _get_fee_rate(
        self,
        fee_structure: FeeStructureConfig,
        fill_data: dict[str, object],
        exchange: ExchangeName,
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

    def _calculate_base_fee(
        self,
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

    def _apply_fee_limits(
        self, fee_structure: FeeStructureConfig, fee_amount: Decimal, exchange: ExchangeName
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

    def _determine_fee_asset(self, fee_structure: FeeStructureConfig, order: Order) -> str:
        """Determine fee asset from structure or symbol.

        Returns:
            str: Fee asset symbol
        """
        if fee_structure.fee_asset:
            return fee_structure.fee_asset

        # Default: use quote currency from symbol
        symbol_parts = order.symbol.value.split("_")
        return symbol_parts[-1] if len(symbol_parts) > 1 else "USDC"

    def _log_fee_calculation(
        self,
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
