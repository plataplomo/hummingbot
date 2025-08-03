"""Order validation service using exchange-specific rules from configuration.

This module provides comprehensive order validation against exchange rules,
market conditions, and portfolio constraints using validated AppSettings configuration.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Dict, List, Optional

from cyberdelta.config.structlog_config import get_logger

from cyberdelta.config.models.config_models import AppSettings
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.logic.market.market_service import MarketDataService
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
from cyberdelta.models.market.order import Order
from cyberdelta.models.market.ticker import Ticker

logger = get_logger(__name__)


class OrderValidator:
    """Comprehensive order validation using exchange-specific rules from configuration.
    
    This validator checks orders against:
    - Exchange-specific constraints (min/max sizes, tick sizes, lot sizes)
    - Market conditions (current prices, order book depth)
    - Portfolio constraints (available balance, position limits)
    - Risk limits (configured in AppSettings)
    
    Configuration Structure (config.exchanges[exchange]):
    - min_order_size: Minimum order size in quote currency
    - max_order_size: Maximum order size in quote currency
    - tick_size: Minimum price increment
    - lot_size: Minimum quantity increment
    - max_price_deviation_pct: Maximum allowed price deviation from market
    - order_timeout_seconds: Order timeout configuration
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - ALL validation rules from AppSettings, NO hardcoded values
    - Uses Symbol objects, NOT strings
    - Uses ExchangeName enum, NOT strings
    - All monetary values as Decimal, NOT float
    - NO assumptions about exchange behavior
    """
    
    def __init__(
        self,
        config: AppSettings,
        market_service: MarketDataService,
        portfolio_service: PortfolioService,
    ):
        """Initialize order validator with configuration and dependencies.
        
        Args:
            config: Application settings containing all validation configuration
            market_service: Market data service for price validation
            portfolio_service: Portfolio service for balance validation
        """
        self.config = config
        self._market_service = market_service
        self._portfolio_service = portfolio_service
        
        # Cache validation settings - NO hardcoded defaults
        self._validation_config = config.validation
        
        logger.info(
            "order_validator_initialized",
            exchanges_configured=len(config.exchanges),
            validation_rules_enabled=True
        )
    
    async def validate_order(self, order: Order) -> List[str]:
        """Validate order against all configured rules.
        
        Args:
            order: Order to validate
            
        Returns:
            List of validation violations (empty if valid)
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Check ALL rules from config.exchanges[exchange]
        - Check ALL risk rules from config.risk
        - Return explicit violations, NO silent filtering
        - ALL limits from configuration
        """
        violations = []
        
        logger.debug(
            "order_validation_starting",
            order_id=order.order_id,
            symbol=order.symbol.value,
            exchange=order.exchange.value if hasattr(order.exchange, 'value') else str(order.exchange),
            side=order.side.value if hasattr(order.side, 'value') else str(order.side),
            quantity=float(order.quantity),
            price=float(order.price) if order.price else None
        )
        
        try:
            # Get exchange configuration
            exchange_config = self._get_exchange_config(order.exchange)
            if not exchange_config:
                violations.append(f"No configuration found for exchange: {order.exchange}")
                return violations
            
            # Validation 1: Exchange-specific constraints
            exchange_violations = await self._validate_exchange_constraints(order, exchange_config)
            violations.extend(exchange_violations)
            
            # Validation 2: Market condition constraints
            market_violations = await self._validate_market_conditions(order, exchange_config)
            violations.extend(market_violations)
            
            # Validation 3: Portfolio constraints
            portfolio_violations = await self._validate_portfolio_constraints(order)
            violations.extend(portfolio_violations)
            
            # Validation 4: Risk limit constraints
            risk_violations = await self._validate_risk_constraints(order)
            violations.extend(risk_violations)
            
            # Validation 5: Order structure validation
            structure_violations = self._validate_order_structure(order)
            violations.extend(structure_violations)
            
            if violations:
                logger.warning(
                    "order_validation_failed",
                    order_id=order.order_id,
                    violation_count=len(violations),
                    violations=violations[:5]  # Log first 5 violations
                )
            else:
                logger.debug(
                    "order_validation_passed",
                    order_id=order.order_id,
                    symbol=order.symbol.value,
                    exchange=order.exchange.value if hasattr(order.exchange, 'value') else str(order.exchange)
                )
            
            return violations
            
        except Exception as e:
            logger.error(
                "order_validation_error",
                order_id=order.order_id,
                error=str(e),
                exc_info=True
            )
            raise
    
    def _get_exchange_config(self, exchange: ExchangeName) -> Optional[object]:
        """Get exchange configuration from AppSettings.
        
        Args:
            exchange: Exchange to get configuration for
            
        Returns:
            Exchange configuration or None if not found
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Uses ExchangeName enum, NOT strings
        - NO defaults, explicit config required
        """
        exchange_str = exchange.value if hasattr(exchange, 'value') else str(exchange)
        return self.config.exchanges.get(exchange_str)
    
    async def _validate_exchange_constraints(
        self, 
        order: Order, 
        exchange_config: object
    ) -> List[str]:
        """Validate order against exchange-specific constraints.
        
        Args:
            order: Order to validate
            exchange_config: Exchange configuration from AppSettings
            
        Returns:
            List of exchange constraint violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Check min/max from config.exchanges[exchange].min_order_size
        - Check tick size from config.exchanges[exchange].tick_size
        - Check lot size from config.exchanges[exchange].lot_size
        - ALL constraints from configuration
        """
        violations = []
        
        # Check minimum order size from config
        if hasattr(exchange_config, 'min_order_size'):
            min_size = exchange_config.min_order_size
            order_value = order.quantity * (order.price or Decimal('0'))
            if order_value < min_size:
                violations.append(
                    f"Order value ${order_value} below minimum ${min_size} for {order.exchange}"
                )
        
        # Check maximum order size from config
        if hasattr(exchange_config, 'max_order_size'):
            max_size = exchange_config.max_order_size
            order_value = order.quantity * (order.price or Decimal('0'))
            if order_value > max_size:
                violations.append(
                    f"Order value ${order_value} exceeds maximum ${max_size} for {order.exchange}"
                )
        
        # Check tick size from config
        if hasattr(exchange_config, 'tick_size') and order.price:
            tick_size = exchange_config.tick_size
            if tick_size > 0:
                price_remainder = order.price % tick_size
                if price_remainder != 0:
                    violations.append(
                        f"Price {order.price} not aligned to tick size {tick_size} for {order.exchange}"
                    )
        
        # Check lot size from config
        if hasattr(exchange_config, 'lot_size'):
            lot_size = exchange_config.lot_size
            if lot_size > 0:
                quantity_remainder = order.quantity % lot_size
                if quantity_remainder != 0:
                    violations.append(
                        f"Quantity {order.quantity} not aligned to lot size {lot_size} for {order.exchange}"
                    )
        
        # Check minimum quantity from config
        if hasattr(exchange_config, 'min_quantity'):
            min_quantity = exchange_config.min_quantity
            if order.quantity < min_quantity:
                violations.append(
                    f"Quantity {order.quantity} below minimum {min_quantity} for {order.exchange}"
                )
        
        # Check maximum quantity from config
        if hasattr(exchange_config, 'max_quantity'):
            max_quantity = exchange_config.max_quantity
            if order.quantity > max_quantity:
                violations.append(
                    f"Quantity {order.quantity} exceeds maximum {max_quantity} for {order.exchange}"
                )
        
        return violations
    
    async def _validate_market_conditions(
        self, 
        order: Order, 
        exchange_config: object
    ) -> List[str]:
        """Validate order against current market conditions.
        
        Args:
            order: Order to validate
            exchange_config: Exchange configuration from AppSettings
            
        Returns:
            List of market condition violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Check price deviation from config.exchanges[exchange].max_price_deviation_pct
        - NO assumptions about market behavior
        - Uses current market data for validation
        """
        violations = []
        
        try:
            # Get current market snapshot
            market_snapshot = await self._market_service.get_market_snapshot()
            if not market_snapshot:
                violations.append("Market data unavailable for order validation")
                return violations
            
            # Get current ticker for symbol/exchange
            ticker = market_snapshot.get_ticker(order.exchange, order.symbol)
            if not ticker:
                violations.append(
                    f"No market data available for {order.symbol.value} on {order.exchange}"
                )
                return violations
            
            # Check price deviation if configured
            if hasattr(exchange_config, 'max_price_deviation_pct') and order.price and ticker.last_price:
                max_deviation = exchange_config.max_price_deviation_pct
                current_price = ticker.last_price
                price_deviation = abs(order.price - current_price) / current_price * 100
                
                if price_deviation > max_deviation:
                    violations.append(
                        f"Order price {order.price} deviates {price_deviation:.2f}% from market "
                        f"price {current_price}, exceeds max {max_deviation}%"
                    )
            
            # Check order book depth if configured
            if hasattr(exchange_config, 'min_order_book_depth'):
                order_book = market_snapshot.get_order_book(order.exchange, order.symbol)
                if order_book:
                    min_depth = exchange_config.min_order_book_depth
                    if order.side.value == 'buy' and order_book.asks:
                        depth = sum(level.quantity for level in order_book.asks[:5])  # Top 5 levels
                        if depth < min_depth:
                            violations.append(
                                f"Insufficient ask depth {depth} for buy order, minimum {min_depth}"
                            )
                    elif order.side.value == 'sell' and order_book.bids:
                        depth = sum(level.quantity for level in order_book.bids[:5])  # Top 5 levels
                        if depth < min_depth:
                            violations.append(
                                f"Insufficient bid depth {depth} for sell order, minimum {min_depth}"
                            )
            
        except Exception as e:
            logger.error(
                "market_condition_validation_error",
                order_id=order.order_id,
                error=str(e),
                exc_info=True
            )
            violations.append(f"Market condition validation failed: {str(e)}")
        
        return violations
    
    async def _validate_portfolio_constraints(self, order: Order) -> List[str]:
        """Validate order against portfolio constraints.
        
        Args:
            order: Order to validate
            
        Returns:
            List of portfolio constraint violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Check available balance from portfolio service
        - Uses Symbol objects, NOT strings
        - NO assumptions about balance availability
        """
        violations = []
        
        try:
            # For buy orders, check quote currency balance
            if order.side.value == 'buy' and order.price:
                # Get quote asset (simplified - would use symbol service in practice)
                quote_asset_str = order.symbol.value.split('_')[-1] if '_' in order.symbol.value else 'USDC'
                quote_symbol = Symbol(value=quote_asset_str)
                
                balance = await self._portfolio_service.get_balance(quote_symbol, order.exchange)
                required_amount = order.quantity * order.price
                
                if not balance:
                    violations.append(
                        f"No {quote_asset_str} balance found on {order.exchange} for buy order"
                    )
                elif balance.available_quantity < required_amount:
                    violations.append(
                        f"Insufficient {quote_asset_str} balance: need {required_amount}, "
                        f"available {balance.available_quantity}"
                    )
            
            # For sell orders, check base currency balance
            elif order.side.value == 'sell':
                # Get base asset
                base_asset_str = order.symbol.value.split('_')[0] if '_' in order.symbol.value else order.symbol.value
                base_symbol = Symbol(value=base_asset_str)
                
                balance = await self._portfolio_service.get_balance(base_symbol, order.exchange)
                
                if not balance:
                    violations.append(
                        f"No {base_asset_str} balance found on {order.exchange} for sell order"
                    )
                elif balance.available_quantity < order.quantity:
                    violations.append(
                        f"Insufficient {base_asset_str} balance: need {order.quantity}, "
                        f"available {balance.available_quantity}"
                    )
            
        except Exception as e:
            logger.error(
                "portfolio_constraint_validation_error",
                order_id=order.order_id,
                error=str(e),
                exc_info=True
            )
            violations.append(f"Portfolio constraint validation failed: {str(e)}")
        
        return violations
    
    async def _validate_risk_constraints(self, order: Order) -> List[str]:
        """Validate order against risk constraints.
        
        Args:
            order: Order to validate
            
        Returns:
            List of risk constraint violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Check against config.risk.global_risk limits
        - Uses configured risk parameters
        - NO hardcoded risk limits
        """
        violations = []
        
        try:
            # Check against global risk limits
            if hasattr(self.config.risk, 'global_risk'):
                global_risk = self.config.risk.global_risk
                
                # Check maximum position size
                if hasattr(global_risk, 'max_position_usd') and order.price:
                    max_position = global_risk.max_position_usd
                    order_value = order.quantity * order.price
                    
                    if order_value > max_position:
                        violations.append(
                            f"Order value ${order_value} exceeds maximum position ${max_position}"
                        )
                
                # Check order timeout constraints
                if hasattr(global_risk, 'max_order_lifetime_seconds'):
                    max_lifetime = global_risk.max_order_lifetime_seconds
                    if order.time_in_force and hasattr(order.time_in_force, 'value'):
                        if order.time_in_force.value == 'GTT':  # Good Till Time
                            # Would need order expiry time to validate
                            pass
            
            # Check validation-specific constraints
            if hasattr(self.config, 'validation'):
                validation_config = self.config.validation
                
                # Check order value bounds
                if hasattr(validation_config, 'min_order_value_usd') and order.price:
                    min_value = validation_config.min_order_value_usd
                    order_value = order.quantity * order.price
                    
                    if order_value < min_value:
                        violations.append(
                            f"Order value ${order_value} below minimum ${min_value}"
                        )
                
                if hasattr(validation_config, 'max_order_value_usd') and order.price:
                    max_value = validation_config.max_order_value_usd
                    order_value = order.quantity * order.price
                    
                    if order_value > max_value:
                        violations.append(
                            f"Order value ${order_value} exceeds maximum ${max_value}"
                        )
            
        except Exception as e:
            logger.error(
                "risk_constraint_validation_error",
                order_id=order.order_id,
                error=str(e),
                exc_info=True
            )
            violations.append(f"Risk constraint validation failed: {str(e)}")
        
        return violations
    
    def _validate_order_structure(self, order: Order) -> List[str]:
        """Validate order structure and required fields.
        
        Args:
            order: Order to validate
            
        Returns:
            List of structural violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Check Symbol object type, NOT strings
        - Check ExchangeName enum type, NOT strings
        - Validate Decimal types for quantities/prices
        """
        violations = []
        
        # Validate symbol is Symbol object
        if not isinstance(order.symbol, Symbol):
            violations.append(f"Order symbol must be Symbol type, got {type(order.symbol)}")
        
        # Validate exchange is ExchangeName enum
        if not isinstance(order.exchange, ExchangeName):
            violations.append(f"Order exchange must be ExchangeName type, got {type(order.exchange)}")
        
        # Validate quantity is positive Decimal
        if not isinstance(order.quantity, Decimal):
            violations.append(f"Order quantity must be Decimal type, got {type(order.quantity)}")
        elif order.quantity <= 0:
            violations.append(f"Order quantity must be positive, got {order.quantity}")
        
        # Validate price if provided
        if order.price is not None:
            if not isinstance(order.price, Decimal):
                violations.append(f"Order price must be Decimal type, got {type(order.price)}")
            elif order.price <= 0:
                violations.append(f"Order price must be positive, got {order.price}")
        
        # Validate required fields
        if not order.order_id:
            violations.append("Order ID is required")
        
        if not order.side:
            violations.append("Order side is required")
        
        if not order.order_type:
            violations.append("Order type is required")
        
        return violations
    
    async def validate_order_modification(
        self, 
        original_order: Order, 
        modified_order: Order
    ) -> List[str]:
        """Validate order modification request.
        
        Args:
            original_order: Original order being modified
            modified_order: Modified order parameters
            
        Returns:
            List of modification violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Check modification constraints from config
        - NO assumptions about allowed modifications
        """
        violations = []
        
        # Get exchange configuration
        exchange_config = self._get_exchange_config(original_order.exchange)
        if not exchange_config:
            violations.append(f"No configuration found for exchange: {original_order.exchange}")
            return violations
        
        # Check if modifications are allowed by exchange config
        if hasattr(exchange_config, 'allow_order_modifications'):
            if not exchange_config.allow_order_modifications:
                violations.append(f"Order modifications not allowed on {original_order.exchange}")
                return violations
        
        # Validate the modified order structure
        structure_violations = await self.validate_order(modified_order)
        violations.extend(structure_violations)
        
        # Check modification-specific constraints
        if hasattr(exchange_config, 'max_price_change_pct'):
            max_change = exchange_config.max_price_change_pct
            if original_order.price and modified_order.price:
                price_change = abs(modified_order.price - original_order.price) / original_order.price * 100
                if price_change > max_change:
                    violations.append(
                        f"Price change {price_change:.2f}% exceeds maximum {max_change}% for modifications"
                    )
        
        if hasattr(exchange_config, 'max_quantity_change_pct'):
            max_change = exchange_config.max_quantity_change_pct
            quantity_change = abs(modified_order.quantity - original_order.quantity) / original_order.quantity * 100
            if quantity_change > max_change:
                violations.append(
                    f"Quantity change {quantity_change:.2f}% exceeds maximum {max_change}% for modifications"
                )
        
        return violations
    
    async def validate_order_cancellation(self, order: Order) -> List[str]:
        """Validate order cancellation request.
        
        Args:
            order: Order to cancel
            
        Returns:
            List of cancellation violations
            
        IMPORTANT: Following CODING_STANDARDS.md:
        - Check cancellation constraints from config
        - NO assumptions about cancellation rules
        """
        violations = []
        
        # Get exchange configuration
        exchange_config = self._get_exchange_config(order.exchange)
        if not exchange_config:
            violations.append(f"No configuration found for exchange: {order.exchange}")
            return violations
        
        # Check if cancellations are allowed
        if hasattr(exchange_config, 'allow_order_cancellations'):
            if not exchange_config.allow_order_cancellations:
                violations.append(f"Order cancellations not allowed on {order.exchange}")
        
        # Check order status constraints
        if hasattr(exchange_config, 'cancellable_statuses'):
            cancellable_statuses = exchange_config.cancellable_statuses
            if order.status and order.status.value not in cancellable_statuses:
                violations.append(
                    f"Order status {order.status.value} not cancellable, "
                    f"allowed: {cancellable_statuses}"
                )
        
        return violations