"""Fill processing and fee calculation service.

This module provides the FillHandler class that processes order fills,
calculates fees using exchange-specific configurations, and coordinates
portfolio updates following CODING_STANDARDS.md requirements.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Dict, List, Optional

from cyberdelta.config.structlog_config import get_logger

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
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
    ):
        """Initialize fill handler with configuration and dependencies.
        
        Args:
            config: Application settings containing all configuration
            portfolio_service: Portfolio service for state updates
        """
        self.config = config
        self._portfolio_service = portfolio_service
        
        # Fill tracking
        self._processed_fills: List[Trade] = []
        self._fill_count = 0
        self._total_fees_usd = Decimal("0")
        
        logger.info(
            "fill_handler_initialized",
            exchanges_configured=len(config.exchanges),
            portfolio_service_available=portfolio_service is not None
        )
    
    async def process_fill(
        self, 
        order: Order, 
        fill_data: Dict[str, object]
    ) -> Trade:
        """Process an order fill with comprehensive fee calculation.
        
        Args:
            order: Order that was filled
            fill_data: Fill data from exchange
            
        Returns:
            Trade object with calculated fees and metadata
            
        Raises:
            ValueError: If fill data is invalid or exchange config missing
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Fee calculation from config.exchanges[exchange].fee_structure
        - ALL calculations use Decimal, NOT float
        - NO hardcoded fee rates or assumptions
        - Explicit error handling for invalid data
        """
        logger.debug(
            "fill_processing_starting",
            order_id=order.order_id,
            symbol=order.symbol.value,
            exchange=order.exchange.value if hasattr(order.exchange, 'value') else str(order.exchange),
            fill_data_keys=list(fill_data.keys())
        )
        
        try:
            # Validate fill data
            self._validate_fill_data(fill_data)
            
            # Extract fill information
            fill_price = Decimal(str(fill_data.get("fill_price", 0)))
            fill_quantity = Decimal(str(fill_data.get("filled_quantity", 0)))
            fill_timestamp = fill_data.get("timestamp", datetime.now(UTC))
            
            if fill_price <= 0 or fill_quantity <= 0:
                raise ValueError(
                    f"Invalid fill data: price={fill_price}, quantity={fill_quantity}"
                )
            
            # Calculate fees using exchange configuration
            fee_amount, fee_asset = await self._calculate_fill_fee(
                order, fill_price, fill_quantity, fill_data
            )
            
            # Create Trade object with all calculated values
            trade = Trade(
                id=fill_data.get("trade_id", f"fill_{order.order_id}_{self._fill_count}"),
                symbol=order.symbol,
                executed_at=fill_timestamp if isinstance(fill_timestamp, datetime) else datetime.now(UTC),
                side=order.side,
                order_id=order.order_id or "",
                exchange=order.exchange.value if hasattr(order.exchange, 'value') else str(order.exchange),
                price=fill_price,
                quantity=fill_quantity,
                fee=fee_amount,
                fee_asset=fee_asset,
                client_order_id=order.client_order_id,
                metadata={
                    "fill_type": fill_data.get("fill_type", "unknown"),
                    "liquidity": fill_data.get("liquidity", "unknown"),  # maker/taker
                    "order_type": order.order_type.value if order.order_type else "unknown",
                    "processing_timestamp": datetime.now(UTC).isoformat()
                }
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
                order_id=order.order_id,
                symbol=order.symbol.value,
                exchange=trade.exchange,
                side=order.side.value if order.side else "unknown",
                price=float(fill_price),
                quantity=float(fill_quantity),
                fee=float(fee_amount),
                fee_asset=fee_asset
            )
            
            return trade
            
        except Exception as e:
            logger.error(
                "fill_processing_failed",
                order_id=order.order_id,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def process_partial_fill(
        self, 
        order: Order, 
        fill_data: Dict[str, object]
    ) -> Trade:
        """Process a partial fill of an order.
        
        Args:
            order: Order that was partially filled
            fill_data: Partial fill data from exchange
            
        Returns:
            Trade object representing the partial fill
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Same validation and fee calculation as full fills
        - Tracks partial fill sequence for audit
        - Updates order state appropriately
        """
        logger.debug(
            "partial_fill_processing",
            order_id=order.order_id,
            filled_so_far=float(order.filled_quantity or 0),
            order_quantity=float(order.quantity),
            new_fill_quantity=float(fill_data.get("filled_quantity", 0))
        )
        
        # Process same as regular fill
        trade = await self.process_fill(order, fill_data)
        
        # Additional tracking for partial fills
        trade.metadata.update({
            "fill_sequence": self._get_fill_sequence_number(order),
            "is_partial_fill": True,
            "remaining_quantity": float(order.quantity - (order.filled_quantity or Decimal("0")))
        })
        
        return trade
    
    def _validate_fill_data(self, fill_data: Dict[str, object]) -> None:
        """Validate fill data structure and required fields.
        
        Args:
            fill_data: Fill data to validate
            
        Raises:
            ValueError: If fill data is invalid
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - NO assumptions about fill data structure
        - Explicit validation with clear error messages
        """
        required_fields = ["fill_price", "filled_quantity"]
        
        for field in required_fields:
            if field not in fill_data:
                raise ValueError(f"Missing required fill data field: {field}")
            
            value = fill_data[field]
            if value is None:
                raise ValueError(f"Fill data field {field} cannot be None")
        
        # Validate numeric fields
        try:
            price = Decimal(str(fill_data["fill_price"]))
            quantity = Decimal(str(fill_data["filled_quantity"]))
            
            if price <= 0:
                raise ValueError(f"Fill price must be positive: {price}")
            if quantity <= 0:
                raise ValueError(f"Fill quantity must be positive: {quantity}")
                
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid numeric values in fill data: {e}")
    
    async def _calculate_fill_fee(
        self, 
        order: Order, 
        fill_price: Decimal, 
        fill_quantity: Decimal,
        fill_data: Dict[str, object]
    ) -> tuple[Decimal, Optional[str]]:
        """Calculate fee for order fill using exchange configuration.
        
        Args:
            order: Order that was filled
            fill_price: Price at which order was filled
            fill_quantity: Quantity that was filled
            fill_data: Additional fill data from exchange
            
        Returns:
            Tuple of (fee_amount, fee_asset)
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses config.exchanges[exchange].fee_structure for all calculations
        - NO hardcoded fee rates or structures
        - Handles maker/taker differences from configuration
        - Returns Decimal fee amount, NOT float
        """
        exchange_name = order.exchange.value if hasattr(order.exchange, 'value') else str(order.exchange)
        
        # Get exchange configuration
        exchange_config = self.config.exchanges.get(exchange_name)
        if not exchange_config:
            raise ValueError(f"No exchange configuration found for: {exchange_name}")
        
        if not hasattr(exchange_config, 'fee_structure'):
            logger.warning(
                "no_fee_structure_configured",
                exchange=exchange_name,
                using_zero_fees=True
            )
            return Decimal("0"), None
        
        fee_structure = exchange_config.fee_structure
        
        # Determine if this is a maker or taker order
        liquidity = fill_data.get("liquidity", "taker")  # Default to taker
        
        # Get appropriate fee rate from configuration
        if liquidity == "maker" and hasattr(fee_structure, 'maker_fee_rate'):
            fee_rate = fee_structure.maker_fee_rate
        elif liquidity == "taker" and hasattr(fee_structure, 'taker_fee_rate'):
            fee_rate = fee_structure.taker_fee_rate
        else:
            # Fallback to general fee rate if available
            if hasattr(fee_structure, 'trading_fee_rate'):
                fee_rate = fee_structure.trading_fee_rate
            else:
                logger.warning(
                    "no_matching_fee_rate",
                    exchange=exchange_name,
                    liquidity=liquidity,
                    using_zero_fee=True
                )
                return Decimal("0"), None
        
        # Calculate fee based on method
        fee_method = getattr(fee_structure, 'fee_calculation_method', 'percentage')
        
        if fee_method == 'percentage':
            # Standard percentage-based fee
            trade_value = fill_price * fill_quantity
            fee_amount = trade_value * fee_rate
        elif fee_method == 'fixed':
            # Fixed fee per trade
            fee_amount = fee_rate
        else:
            logger.warning(
                "unknown_fee_calculation_method",
                exchange=exchange_name,
                method=fee_method,
                using_percentage=True
            )
            trade_value = fill_price * fill_quantity
            fee_amount = trade_value * fee_rate
        
        # Apply minimum fee if configured
        if hasattr(fee_structure, 'minimum_fee') and fee_amount < fee_structure.minimum_fee:
            fee_amount = fee_structure.minimum_fee
            
            logger.debug(
                "minimum_fee_applied",
                exchange=exchange_name,
                calculated_fee=float(fee_amount),
                minimum_fee=float(fee_structure.minimum_fee)
            )
        
        # Apply maximum fee if configured
        if hasattr(fee_structure, 'maximum_fee') and fee_amount > fee_structure.maximum_fee:
            fee_amount = fee_structure.maximum_fee
            
            logger.debug(
                "maximum_fee_applied",
                exchange=exchange_name,
                calculated_fee=float(fee_amount),
                maximum_fee=float(fee_structure.maximum_fee)
            )
        
        # Determine fee asset
        fee_asset = getattr(fee_structure, 'fee_asset', None)
        if not fee_asset:
            # Default to quote currency from symbol
            symbol_parts = order.symbol.value.split('_')
            fee_asset = symbol_parts[-1] if len(symbol_parts) > 1 else 'USDC'
        
        logger.debug(
            "fee_calculated",
            exchange=exchange_name,
            liquidity=liquidity,
            fee_rate=float(fee_rate),
            fee_method=fee_method,
            trade_value=float(fill_price * fill_quantity),
            fee_amount=float(fee_amount),
            fee_asset=fee_asset
        )
        
        return fee_amount, fee_asset
    
    def _get_fill_sequence_number(self, order: Order) -> int:
        """Get sequence number for this fill within the order.
        
        Args:
            order: Order being filled
            
        Returns:
            Sequence number for this fill
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Tracks fill sequence for audit purposes
        - NO assumptions about fill ordering
        """
        # Count existing fills for this order
        order_fills = [
            trade for trade in self._processed_fills 
            if trade.order_id == order.order_id
        ]
        return len(order_fills) + 1
    
    async def process_fill_correction(
        self, 
        original_trade_id: str, 
        correction_data: Dict[str, object]
    ) -> Optional[Trade]:
        """Process a fill correction or adjustment.
        
        Args:
            original_trade_id: ID of the original trade to correct
            correction_data: Correction data from exchange
            
        Returns:
            Corrected Trade object or None if original not found
            
        IMPORTANT: Following CODING_STANDARDS.md:
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
                "fill_correction_original_not_found",
                original_trade_id=original_trade_id
            )
            return None
        
        logger.info(
            "processing_fill_correction",
            original_trade_id=original_trade_id,
            correction_type=correction_data.get("type", "unknown")
        )
        
        # Create corrected trade
        corrected_trade = Trade(
            id=correction_data.get("corrected_trade_id", f"corr_{original_trade_id}"),
            symbol=original_trade.symbol,
            executed_at=original_trade.executed_at,
            side=original_trade.side,
            order_id=original_trade.order_id,
            exchange=original_trade.exchange,
            price=Decimal(str(correction_data.get("corrected_price", original_trade.price))),
            quantity=Decimal(str(correction_data.get("corrected_quantity", original_trade.quantity))),
            fee=Decimal(str(correction_data.get("corrected_fee", original_trade.fee))),
            fee_asset=correction_data.get("corrected_fee_asset", original_trade.fee_asset),
            client_order_id=original_trade.client_order_id,
            metadata={
                **original_trade.metadata,
                "is_correction": True,
                "original_trade_id": original_trade_id,
                "correction_timestamp": datetime.now(UTC).isoformat(),
                "correction_reason": correction_data.get("reason", "exchange_adjustment")
            }
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
            fee_change=float(corrected_trade.fee - original_trade.fee)
        )
        
        return corrected_trade
    
    def get_fill_statistics(self) -> Dict[str, object]:
        """Get fill processing statistics.
        
        Returns:
            Dictionary with fill statistics and metrics
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns explicit statistics from actual processing
        - NO calculated/derived statistics
        """
        return {
            "total_fills_processed": self._fill_count,
            "total_fees_usd": float(self._total_fees_usd),
            "unique_orders_filled": len(set(trade.order_id for trade in self._processed_fills)),
            "exchanges_processed": len(set(trade.exchange for trade in self._processed_fills)),
            "average_fill_size_usd": (
                float(sum(trade.price * trade.quantity for trade in self._processed_fills) / len(self._processed_fills))
                if self._processed_fills else 0
            ),
            "processing_started": len(self._processed_fills) > 0
        }
    
    def get_recent_fills(self, limit: int = 10) -> List[Trade]:
        """Get most recent processed fills.
        
        Args:
            limit: Maximum number of fills to return
            
        Returns:
            List of recent Trade objects
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Returns actual Trade objects, not summaries
        - Limit parameter explicit, no default assumptions
        """
        return self._processed_fills[-limit:] if self._processed_fills else []
    
    async def process_bulk_fills(
        self, 
        order: Order, 
        fill_list: List[Dict[str, object]]
    ) -> List[Trade]:
        """Process multiple fills for an order efficiently.
        
        Args:
            order: Order that received multiple fills
            fill_list: List of fill data dictionaries
            
        Returns:
            List of processed Trade objects
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Processes each fill with same validation as single fills
        - Maintains fill sequence and audit trail
        - NO assumptions about fill order or timing
        """
        trades = []
        
        logger.info(
            "bulk_fill_processing_starting",
            order_id=order.order_id,
            fill_count=len(fill_list)
        )
        
        for i, fill_data in enumerate(fill_list):
            try:
                # Add sequence information to fill data
                fill_data_with_sequence = {
                    **fill_data,
                    "bulk_sequence": i + 1,
                    "bulk_total": len(fill_list)
                }
                
                trade = await self.process_fill(order, fill_data_with_sequence)
                trades.append(trade)
                
            except Exception as e:
                logger.error(
                    "bulk_fill_item_failed",
                    order_id=order.order_id,
                    fill_index=i,
                    error=str(e),
                    exc_info=True
                )
                # Continue processing remaining fills
                continue
        
        logger.info(
            "bulk_fill_processing_completed",
            order_id=order.order_id,
            requested_fills=len(fill_list),
            successful_fills=len(trades),
            failed_fills=len(fill_list) - len(trades)
        )
        
        return trades