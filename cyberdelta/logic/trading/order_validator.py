"""Order validation service using exchange-specific rules from configuration.

This module provides comprehensive order validation against exchange rules,
market conditions, and portfolio constraints using validated AppSettings configuration.
"""

from __future__ import annotations

from decimal import Decimal

from cyberdelta.config.models import AppSettings
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols import bp_symbol, hl_symbol
from cyberdelta.enums import OrderSide, OrderType
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.logic.market.market_service import MarketDataService
from cyberdelta.logic.portfolio.portfolio_service import PortfolioService
from cyberdelta.models.market.market_snapshot import MarketSnapshot
from cyberdelta.models.market.order import Order
from cyberdelta.models.market.order_book import OrderBook
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
    ) -> None:
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
            validation_rules_enabled=True,
        )

    async def validate_order(self, order: Order) -> list[str]:
        """Validate order against all configured rules.

        Args:
            order: Order to validate

        Returns:
            List of validation violations (empty if valid)

        Note:
        - Check ALL rules from config.exchanges[exchange]
        - Check ALL risk rules from config.risk
        - Return explicit violations, NO silent filtering
        - ALL limits from configuration
        """
        violations: list[str] = []

        logger.debug(
            "order_validation_starting",
            order_id=order.exchange_order_id,
            symbol=order.symbol.value,
            exchange=order.exchange.value,
            side=order.side.value,
            quantity=float(order.quantity_requested),
            price=float(order.price) if order.price else None,
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
                    order_id=order.exchange_order_id,
                    violation_count=len(violations),
                    violations=violations[:5],  # Log first 5 violations
                )
            else:
                logger.debug(
                    "order_validation_passed",
                    order_id=order.exchange_order_id,
                    symbol=order.symbol.value,
                    exchange=order.exchange.value,
                )

        except Exception as e:
            logger.exception(
                "order_validation_error", order_id=order.exchange_order_id, error=str(e)
            )
            raise
        else:
            return violations

    def _get_exchange_config(self, exchange: ExchangeName) -> ExchangeSpecificConfig | None:
        """Get exchange configuration from AppSettings.

        Args:
            exchange: Exchange to get configuration for

        Returns:
            Exchange configuration or None if not found

        Note:
        - Uses ExchangeName enum, NOT strings
        - NO defaults, explicit config required
        """
        exchange_str = exchange.value
        return self.config.exchanges.get(exchange_str)

    async def _validate_exchange_constraints(
        self, order: Order, exchange_config: ExchangeSpecificConfig
    ) -> list[str]:
        """Validate order against exchange-specific constraints.

        Args:
            order: Order to validate
            exchange_config: Exchange configuration from AppSettings

        Returns:
            List of exchange constraint violations

        Note:
        - Check min/max from config.exchanges[exchange].min_order_size
        - Check tick size from config.exchanges[exchange].tick_size
        - Check lot size from config.exchanges[exchange].lot_size
        - ALL constraints from configuration
        """
        violations: list[str] = []

        # Check order value constraints
        violations.extend(self._check_order_value_constraints(order, exchange_config))

        # Check price alignment constraints
        violations.extend(self._check_price_alignment(order, exchange_config))

        # Check quantity alignment constraints
        violations.extend(self._check_quantity_alignment(order, exchange_config))

        # Check quantity limits
        violations.extend(self._check_quantity_limits(order, exchange_config))

        return violations

    def _check_order_value_constraints(
        self, order: Order, exchange_config: ExchangeSpecificConfig
    ) -> list[str]:
        """Check order value against min/max constraints.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        # For market orders without a price, we cannot calculate order value
        # These need special handling or should be validated differently
        if order.order_type == OrderType.MARKET and not order.price:
            # Market orders should be validated by quantity limits, not value
            # Skip value validation for market orders
            logger.debug(
                "skipping_value_validation_for_market_order",
                order_id=order.exchange_order_id,
                symbol=order.symbol.value,
                exchange=order.exchange.value,
            )
            return violations

        # For limit orders, price must be specified
        if order.order_type == OrderType.LIMIT and not order.price:
            violations.append("Limit order must have a price specified")
            return violations

        # Calculate order value only when we have a valid price
        if not order.price:
            violations.append(
                f"Cannot calculate order value without price for {order.order_type.value} order"
            )
            return violations

        order_value = order.quantity_requested * order.price

        # Check minimum order size from config
        if exchange_config.min_order_size is not None:
            min_size = Decimal(str(exchange_config.min_order_size))
            if order_value < min_size:
                violations.append(
                    f"Order value ${order_value} below minimum ${min_size} "
                    f"for {order.exchange.value}"
                )

        # Check maximum order size from config
        if exchange_config.max_order_size is not None:
            max_size = Decimal(str(exchange_config.max_order_size))
            if order_value > max_size:
                violations.append(
                    f"Order value ${order_value} exceeds maximum ${max_size} "
                    f"for {order.exchange.value}"
                )

        return violations

    def _check_price_alignment(
        self, order: Order, exchange_config: ExchangeSpecificConfig
    ) -> list[str]:
        """Check price alignment against tick size.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        # Market orders don't have price constraints
        if order.order_type == OrderType.MARKET:
            return violations

        # For limit orders, check tick size alignment
        if exchange_config.tick_size is not None and order.price:
            tick_size = Decimal(str(exchange_config.tick_size))
            if tick_size > 0:
                price_remainder = order.price % tick_size
                if price_remainder != 0:
                    violations.append(
                        f"Price {order.price} not aligned to tick size {tick_size} "
                        f"for {order.exchange}"
                    )

        return violations

    def _check_quantity_alignment(
        self, order: Order, exchange_config: ExchangeSpecificConfig
    ) -> list[str]:
        """Check quantity alignment against lot size.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        if exchange_config.lot_size is not None:
            lot_size = Decimal(str(exchange_config.lot_size))
            if lot_size > 0:
                quantity_remainder = order.quantity_requested % lot_size
                if quantity_remainder != 0:
                    violations.append(
                        f"Quantity {order.quantity_requested} not aligned to lot size {lot_size} "
                        f"for {order.exchange}"
                    )

        return violations

    def _check_quantity_limits(
        self, order: Order, exchange_config: ExchangeSpecificConfig
    ) -> list[str]:
        """Check quantity against min/max limits.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        # Check minimum quantity from config
        if exchange_config.min_quantity is not None:
            min_quantity = exchange_config.min_quantity
            if order.quantity_requested < min_quantity:
                violations.append(
                    f"Quantity {order.quantity_requested} below minimum {min_quantity} "
                    f"for {order.exchange}"
                )

        # Check maximum quantity from config
        if exchange_config.max_quantity is not None:
            max_quantity = exchange_config.max_quantity
            if order.quantity_requested > max_quantity:
                violations.append(
                    f"Quantity {order.quantity_requested} exceeds maximum {max_quantity} "
                    f"for {order.exchange}"
                )

        return violations

    async def _validate_market_conditions(
        self, order: Order, exchange_config: ExchangeSpecificConfig
    ) -> list[str]:
        """Validate order against current market conditions.

        Args:
            order: Order to validate
            exchange_config: Exchange configuration from AppSettings

        Returns:
            List of market condition violations

        Note:
        - Check price deviation from config.exchanges[exchange].max_price_deviation_pct
        - NO assumptions about market behavior
        - Uses current market data for validation
        """
        violations: list[str] = []

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

            # Check price deviation
            violations.extend(self._check_price_deviation(order, ticker, exchange_config))

            # Check order book depth
            violations.extend(
                await self._check_order_book_depth(order, market_snapshot, exchange_config)
            )

        except Exception as e:
            logger.exception(
                "market_condition_validation_error", order_id=order.exchange_order_id, error=str(e)
            )
            violations.append(f"Market condition validation failed: {e!s}")

        return violations

    def _check_price_deviation(
        self, order: Order, ticker: Ticker, exchange_config: ExchangeSpecificConfig
    ) -> list[str]:
        """Check order price deviation from market.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        if exchange_config.max_price_deviation_pct is not None and order.price and ticker.price:
            max_deviation = exchange_config.max_price_deviation_pct
            current_price = ticker.price
            price_deviation = abs(order.price - current_price) / current_price * 100

            if price_deviation > max_deviation:
                violations.append(
                    f"Order price {order.price} deviates {price_deviation:.2f}% from market "
                    f"price {current_price}, exceeds max {max_deviation}%"
                )

        return violations

    async def _check_order_book_depth(
        self, order: Order, market_snapshot: MarketSnapshot, exchange_config: ExchangeSpecificConfig
    ) -> list[str]:
        """Check order book depth for sufficient liquidity.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        if exchange_config.min_order_book_depth is not None:
            order_book = market_snapshot.get_order_book(order.exchange, order.symbol)
            if order_book:
                violations.extend(
                    self._validate_book_depth(
                        order, order_book, exchange_config.min_order_book_depth
                    )
                )

        return violations

    def _validate_book_depth(
        self, order: Order, order_book: OrderBook, min_depth: float
    ) -> list[str]:
        """Validate order book has sufficient depth.

        Returns:
            List of violations if any.
        """
        violations: list[str] = []

        if order.side == OrderSide.BUY and order_book.asks:
            depth = sum(ask[1] for ask in order_book.asks[:5])  # Top 5 levels
            if depth < min_depth:
                violations.append(
                    f"Insufficient ask depth {depth} for buy order, minimum {min_depth}"
                )
        elif order.side == OrderSide.SELL and order_book.bids:
            depth = sum(bid[1] for bid in order_book.bids[:5])  # Top 5 levels
            if depth < min_depth:
                violations.append(
                    f"Insufficient bid depth {depth} for sell order, minimum {min_depth}"
                )

        return violations

    async def _validate_portfolio_constraints(self, order: Order) -> list[str]:
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
                # Get quote asset (simplified - would use symbol service in practice)
                quote_asset_str = (
                    order.symbol.value.split("_")[-1] if "_" in order.symbol.value else "USDC"
                )
                if order.exchange == ExchangeName.HYPERLIQUID:
                    quote_symbol = hl_symbol(quote_asset_str)
                else:
                    quote_symbol = bp_symbol(quote_asset_str)

                balance = await self._portfolio_service.get_balance(quote_symbol, order.exchange)
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

            # For sell orders, check base currency balance
            elif order.side == OrderSide.SELL:
                # Get base asset
                base_asset_str = (
                    order.symbol.value.split("_")[0]
                    if "_" in order.symbol.value
                    else order.symbol.value
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

        except Exception as e:
            logger.exception(
                "portfolio_constraint_validation_error",
                order_id=order.exchange_order_id,
                error=str(e),
            )
            violations.append(f"Portfolio constraint validation failed: {e!s}")

        return violations

    async def _validate_risk_constraints(self, order: Order) -> list[str]:
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
            # Check maximum position size from config
            if order.price:
                max_position = global_risk.max_position_usd
                order_value = order.quantity_requested * order.price

                if order_value > max_position:
                    violations.append(
                        f"Order value ${order_value} exceeds maximum position ${max_position}"
                    )

                # Check order timeout constraints
                # Order lifetime validation would go here if needed
                # (Currently not implemented in risk config model)

            # Check validation-specific constraints
            # Validation configuration is always available in AppSettings
            validation_config = self.config.validation

            # Check order value bounds
            if order.price:  # min_trade_value validation
                min_value = validation_config.min_trade_value
                order_value = order.quantity_requested * order.price

                if order_value < min_value:
                    violations.append(f"Order value ${order_value} below minimum ${min_value}")

                # max_trade_value validation
                max_value = validation_config.max_trade_value
                order_value = order.quantity_requested * order.price

                if order_value > max_value:
                    violations.append(f"Order value ${order_value} exceeds maximum ${max_value}")

        except Exception as e:
            logger.exception(
                "risk_constraint_validation_error", order_id=order.exchange_order_id, error=str(e)
            )
            violations.append(f"Risk constraint validation failed: {e!s}")

        return violations

    def _validate_order_structure(self, order: Order) -> list[str]:
        """Validate order structure and required fields.

        Args:
            order: Order to validate

        Returns:
            List of structural violations

        Note:
        - Check Symbol object type, NOT strings
        - Check ExchangeName enum type, NOT strings
        - Validate Decimal types for quantities/prices
        """
        violations: list[str] = []

        # Validate symbol is Symbol object - guaranteed by type system
        # Symbol objects always have a value attribute

        # Validate exchange is ExchangeName enum - already guaranteed by type system

        # Validate quantity is positive Decimal - type already guaranteed
        if order.quantity_requested <= 0:
            violations.append(f"Order quantity must be positive, got {order.quantity_requested}")

        # Validate price if provided
        if order.price is not None and order.price <= 0:
            violations.append(f"Order price must be positive, got {order.price}")

        # Validate required fields
        if not order.exchange_order_id and not order.client_order_id:
            violations.append("Order ID (exchange_order_id or client_order_id) is required")

        # Side and order_type are always set in Order model (enums)

        return violations

    async def validate_order_modification(
        self, original_order: Order, modified_order: Order
    ) -> list[str]:
        """Validate order modification request.

        Args:
            original_order: Original order being modified
            modified_order: Modified order parameters

        Returns:
            List of modification violations

        Note:
        - Check modification constraints from config
        - NO assumptions about allowed modifications
        """
        violations: list[str] = []

        # Get exchange configuration
        exchange_config = self._get_exchange_config(original_order.exchange)
        if not exchange_config:
            violations.append(f"No configuration found for exchange: {original_order.exchange}")
            return violations

        # Check if modifications are allowed by exchange config
        if (
            exchange_config.allow_order_modifications is not None
            and not exchange_config.allow_order_modifications
        ):
            violations.append(f"Order modifications not allowed on {original_order.exchange}")
            return violations

        # Validate the modified order structure
        structure_violations = await self.validate_order(modified_order)
        violations.extend(structure_violations)

        # Check modification-specific constraints
        if exchange_config.max_price_change_pct is not None:
            max_change = exchange_config.max_price_change_pct
            if original_order.price and modified_order.price:
                price_change = (
                    abs(modified_order.price - original_order.price) / original_order.price * 100
                )
                if price_change > max_change:
                    violations.append(
                        f"Price change {price_change:.2f}% exceeds maximum "
                        f"{max_change}% for modifications"
                    )

        if exchange_config.max_quantity_change_pct is not None:
            max_change = exchange_config.max_quantity_change_pct
            quantity_change = (
                abs(modified_order.quantity_requested - original_order.quantity_requested)
                / original_order.quantity_requested
                * 100
            )
            if quantity_change > max_change:
                violations.append(
                    f"Quantity change {quantity_change:.2f}% exceeds maximum "
                    f"{max_change}% for modifications"
                )

        return violations

    async def validate_order_cancellation(self, order: Order) -> list[str]:
        """Validate order cancellation request.

        Args:
            order: Order to cancel

        Returns:
            List of cancellation violations

        Note:
        - Check cancellation constraints from config
        - NO assumptions about cancellation rules
        """
        violations: list[str] = []

        # Get exchange configuration
        exchange_config = self._get_exchange_config(order.exchange)
        if not exchange_config:
            violations.append(f"No configuration found for exchange: {order.exchange}")
            return violations

        # Check if cancellations are allowed
        if (
            exchange_config.allow_order_cancellations is not None
            and not exchange_config.allow_order_cancellations
        ):
            violations.append(f"Order cancellations not allowed on {order.exchange}")

        # Check order status constraints
        if exchange_config.cancellable_statuses is not None:
            cancellable_statuses = exchange_config.cancellable_statuses
            if order.status and order.status.value not in cancellable_statuses:
                violations.append(
                    f"Order status {order.status.value} not cancellable, "
                    f"allowed: {cancellable_statuses}"
                )

        return violations
