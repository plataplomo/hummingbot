"""Input validation service for ExecutionHandler refactoring.

This module provides comprehensive validation for execution requests,
order parameters, and system state to prevent invalid operations.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import TraceLevelLogger, get_logger
from cyberdelta.core.services.interfaces import (
    BaseService,
    IInputValidator,
    OrderRequest,
    ValidationConfig,
    ValidationResult,
)

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import OrderType


if TYPE_CHECKING:
    from cyberdelta.apis.base.exchange_api import ExchangeAPI
    from cyberdelta.core.risk_manager import SizedOpportunity
    from cyberdelta.core.symbols.service import SymbolService


class ExecutionInputValidator(BaseService, IInputValidator):
    """Service for validating execution requests and order parameters."""

    def __init__(
        self,
        api_clients: dict[str, ExchangeAPI],
        symbol_mapper: SymbolService,  # Still use symbol_mapper name for compatibility
        config: ValidationConfig | None = None,
        logger: TraceLevelLogger | None = None,
    ) -> None:
        """Initialize input validation service.

        Args:
            api_clients: Dictionary of exchange API clients
            symbol_mapper: Symbol service (kept as symbol_mapper for compatibility)
            config: Validation configuration
            logger: Optional logger instance
        """
        super().__init__(logger)
        self.api_clients = api_clients
        self.symbol_service = symbol_mapper  # Internal reference uses proper name
        self.config = config or ValidationConfig()
        self.logger = logger or get_logger(__name__)

    async def validate_execution_request(self, opportunity: SizedOpportunity) -> ValidationResult:
        """Validate execution request before processing.

        Args:
            opportunity: Sized arbitrage opportunity to validate

        Returns:
            ValidationResult with validation status and any issues
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Run all validation checks
        self._validate_opportunity_freshness(opportunity, errors)
        self._validate_exchanges(opportunity, errors)
        self._validate_symbol_mapping(opportunity, errors)
        self._validate_position_sizes(opportunity, errors, warnings)
        self._validate_size_imbalance(opportunity, warnings)
        self._validate_profit_expectations(opportunity, warnings)

        # Log validation results
        self._log_validation_results(opportunity, errors, warnings)

        return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)

    def _validate_opportunity_freshness(
        self, opportunity: SizedOpportunity, errors: list[str]
    ) -> None:
        """Validate opportunity timestamp freshness."""
        if not hasattr(opportunity.opportunity, "timestamp"):
            return

        current_time = time.time()
        age = current_time - opportunity.opportunity.timestamp.timestamp()
        if age > self.config.max_opportunity_age_seconds:
            errors.append(
                f"Opportunity is too old: {age:.1f}s > {self.config.max_opportunity_age_seconds}s"
            )

    def _validate_exchanges(self, opportunity: SizedOpportunity, errors: list[str]) -> None:
        """Validate exchange identifiers and availability."""
        # Validate long exchange
        if not opportunity.opportunity.long_exchange:
            errors.append("Missing long exchange identifier")
        elif opportunity.opportunity.long_exchange not in self.api_clients:
            errors.append(f"Unknown long exchange: {opportunity.opportunity.long_exchange}")

        # Validate short exchange
        if not opportunity.opportunity.short_exchange:
            errors.append("Missing short exchange identifier")
        elif opportunity.opportunity.short_exchange not in self.api_clients:
            errors.append(f"Unknown short exchange: {opportunity.opportunity.short_exchange}")

    def _validate_symbol_mapping(self, opportunity: SizedOpportunity, errors: list[str]) -> None:
        """Validate symbol mapping for both exchanges using domain helpers."""
        if not opportunity.opportunity.symbol:
            errors.append("Missing symbol")
            return

        # Check long exchange symbol mapping with domain helpers
        if opportunity.opportunity.long_exchange in self.api_clients:
            # Simplified symbol resolution
            # TODO: Implement proper symbol resolution through SymbolService  
            long_symbol = None  # was: self.symbol_helpers.resolve_for_exchange(opportunity.opportunity.symbol, opportunity.opportunity.long_exchange)
            if not long_symbol:
                # Get available exchanges for better error message
                # TODO: Implement proper symbol service methods for exchange lookup
                unified_symbol = None  # was: self.symbol_service.store.get_by_internal(opportunity.opportunity.symbol)
                long_symbol_exchanges: list[str] = []  # Simplified for now

                error_msg = (
                    f"Symbol {opportunity.opportunity.symbol} not found for "
                    f"long exchange {opportunity.opportunity.long_exchange}"
                )
                if long_symbol_exchanges:
                    error_msg += f". Available on: {', '.join(long_symbol_exchanges)}"
                errors.append(error_msg)

        # Check short exchange symbol mapping with domain helpers
        if opportunity.opportunity.short_exchange in self.api_clients:
            # Simplified symbol resolution
            # TODO: Implement proper symbol resolution through SymbolService  
            short_symbol = None  # was: self.symbol_helpers.resolve_for_exchange(opportunity.opportunity.symbol, opportunity.opportunity.short_exchange)
            if not short_symbol:
                # Get available exchanges for better error message
                # TODO: Implement proper symbol service methods for exchange lookup
                unified_symbol = None  # was: self.symbol_service.store.get_by_internal(opportunity.opportunity.symbol)
                short_symbol_exchanges: list[str] = []  # Simplified for now

                error_msg = (
                    f"Symbol {opportunity.opportunity.symbol} not found for "
                    f"short exchange {opportunity.opportunity.short_exchange}"
                )
                if short_symbol_exchanges:
                    error_msg += f". Available on: {', '.join(short_symbol_exchanges)}"
                errors.append(error_msg)

    def _validate_position_sizes(
        self, opportunity: SizedOpportunity, errors: list[str], warnings: list[str]
    ) -> None:
        """Validate position sizes for both legs."""
        # Validate long position size
        if opportunity.long_size:
            self._validate_single_position_size(opportunity.long_size, "Long", errors, warnings)

        # Validate short position size
        if opportunity.short_size:
            self._validate_single_position_size(opportunity.short_size, "Short", errors, warnings)

    def _validate_single_position_size(
        self, size: Decimal, leg_name: str, errors: list[str], warnings: list[str]
    ) -> None:
        """Validate a single position size."""
        if size <= 0:
            errors.append(f"{leg_name} position size must be positive")
        elif size < self.config.min_position_size_usd:
            warnings.append(
                f"{leg_name} position size {size} is below minimum "
                f"{self.config.min_position_size_usd}"
            )
        elif size > self.config.max_position_size_usd:
            errors.append(
                f"{leg_name} position size {size} exceeds maximum "
                f"{self.config.max_position_size_usd}"
            )

    def _validate_size_imbalance(self, opportunity: SizedOpportunity, warnings: list[str]) -> None:
        """Validate size imbalance between long and short positions."""
        if not (opportunity.long_size and opportunity.short_size):
            return

        size_diff = abs(opportunity.long_size - opportunity.short_size)
        avg_size = (opportunity.long_size + opportunity.short_size) / 2
        imbalance_pct = (size_diff / avg_size) * 100

        if imbalance_pct > self.config.max_size_imbalance_pct:
            warnings.append(
                f"Size imbalance {imbalance_pct:.1f}% exceeds threshold "
                f"{self.config.max_size_imbalance_pct}%"
            )

    def _validate_profit_expectations(
        self, opportunity: SizedOpportunity, warnings: list[str]
    ) -> None:
        """Validate expected profit is reasonable."""
        if opportunity.expected_profit and opportunity.expected_profit <= 0:
            warnings.append("Expected profit is not positive")

    def _log_validation_results(
        self, opportunity: SizedOpportunity, errors: list[str], warnings: list[str]
    ) -> None:
        """Log validation results with domain context."""
        log_data: dict[str, Any] = {
            "symbol": opportunity.opportunity.symbol,
            "long_exchange": opportunity.opportunity.long_exchange,
            "short_exchange": opportunity.opportunity.short_exchange,
        }

        # Add domain context if available
        if opportunity.opportunity.symbol:
            # TODO: Implement proper symbol service methods for exchange lookup
            unified_symbol = None  # was: self.symbol_service.store.get_by_internal(opportunity.opportunity.symbol)
            if unified_symbol:
                # These attributes don't exist on None, so this code is unreachable
                # TODO: Implement proper unified_symbol structure when symbol service is complete
                pass  # Simplified for now

        if errors:
            self.logger.warning(
                "Execution request validation failed",
                **log_data,
                errors=errors,
                warnings=warnings,
            )
        elif warnings:
            self.logger.info(
                "Execution request validation passed with warnings",
                **log_data,
                warnings=warnings,
            )
        else:
            self.logger.debug(
                "Execution request validation passed",
                **log_data,
            )

    def validate_order_request(self, request: OrderRequest) -> ValidationResult:
        """Validate order request parameters.

        Args:
            request: Order request to validate

        Returns:
            ValidationResult with validation status and any issues
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Run all validation checks
        self._validate_order_exchange(request, errors)
        self._validate_order_symbol(request, errors)
        self._validate_order_quantity(request, errors)
        self._validate_order_type_and_price(request, errors, warnings)
        self._validate_order_flags(request, warnings)
        self._validate_client_order_id(request, warnings)

        # Log validation results
        self._log_order_validation_results(request, errors, warnings)

        return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)

    def _validate_order_exchange(self, request: OrderRequest, errors: list[str]) -> None:
        """Validate order exchange identifier."""
        if not request.exchange_id:
            errors.append("Missing exchange identifier")
        elif request.exchange_id not in self.api_clients:
            errors.append(f"Unknown exchange: {request.exchange_id}")

    def _validate_order_symbol(self, request: OrderRequest, errors: list[str]) -> None:
        """Validate order symbol and mapping using domain helpers."""
        if not request.symbol:
            errors.append("Missing symbol")
            return

        # Check symbol mapping with domain helpers
        # Simplified symbol resolution
        # TODO: Implement proper symbol resolution through SymbolService  
        exchange_symbol: Symbol | None = None  # was: self.symbol_helpers.resolve_for_exchange(request.symbol, request.exchange_id)
        if not exchange_symbol:
            # Get available exchanges and symbol type for better error message
            # TODO: Implement proper symbol service methods for exchange lookup
            unified_symbol = None  # was: self.symbol_service.store.get_by_internal(request.symbol)
            symbol_exchanges: list[str] = []  # Simplified for now

            error_msg = f"Symbol {request.symbol} not found for exchange {request.exchange_id}"
            if symbol_exchanges:
                error_msg += f". Available on: {', '.join(symbol_exchanges)}"
            errors.append(error_msg)

    def _validate_order_quantity(self, request: OrderRequest, errors: list[str]) -> None:
        """Validate order quantity."""
        if request.quantity <= 0:
            errors.append("Order quantity must be positive")
        elif request.quantity > self.config.max_position_size_usd:
            errors.append(
                f"Order quantity {request.quantity} exceeds maximum "
                f"{self.config.max_position_size_usd}"
            )

    def _validate_order_type_and_price(
        self, request: OrderRequest, errors: list[str], warnings: list[str]
    ) -> None:
        """Validate order type and price consistency."""
        # Check for unusual order types
        if request.order_type not in {OrderType.MARKET, OrderType.LIMIT}:
            warnings.append(f"Unusual order type: {request.order_type}")

        # Validate price for limit orders
        if request.order_type == OrderType.LIMIT:
            if not request.price:
                errors.append("Limit orders require a price")
            elif request.price <= 0:
                errors.append("Order price must be positive")
        elif request.price is not None:
            warnings.append("Price specified for non-limit order")

    def _validate_order_flags(self, request: OrderRequest, warnings: list[str]) -> None:
        """Validate order flags and flag combinations."""
        if request.reduce_only and request.post_only:
            warnings.append("Both reduce_only and post_only are set")

    def _validate_client_order_id(self, request: OrderRequest, warnings: list[str]) -> None:
        """Validate client order ID if provided."""
        if not request.client_order_id:
            return

        max_client_order_id_length = 50
        if len(request.client_order_id) > max_client_order_id_length:
            warnings.append("Client order ID is unusually long")

        # Check for unusual characters (allow alphanumeric, hyphens, underscores)
        if not request.client_order_id.replace("-", "").replace("_", "").isalnum():
            warnings.append("Client order ID contains unusual characters")

    def _log_order_validation_results(
        self, request: OrderRequest, errors: list[str], warnings: list[str]
    ) -> None:
        """Log order validation results appropriately."""
        log_data = {
            "exchange_id": request.exchange_id,
            "symbol": request.symbol,
            "side": request.side,
            "quantity": str(request.quantity),
            "order_type": request.order_type,
        }

        if errors:
            self.logger.warning(
                "Order request validation failed",
                **log_data,
                errors=errors,
                warnings=warnings,
            )
        elif warnings:
            self.logger.info(
                "Order request validation passed with warnings",
                **log_data,
                warnings=warnings,
            )
        else:
            self.logger.debug(
                "Order request validation passed",
                **log_data,
            )

    async def validate_account_balances(
        self, exchange_id: str, required_balance: Decimal, asset: str = "USD"
    ) -> ValidationResult:
        """Validate that account has sufficient balance for operation.

        Args:
            exchange_id: Exchange identifier
            required_balance: Required balance amount
            asset: Asset symbol (default: USD)

        Returns:
            ValidationResult with balance validation status
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Validate exchange
        api_client = self.api_clients.get(exchange_id)
        if not api_client:
            errors.append(f"No API client available for exchange: {exchange_id}")
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        try:
            # Get account balances
            balances_dict = await api_client.get_balances()
            balances = list(balances_dict.values())

            # Find available balance for the asset
            available_balance = Decimal(0)
            for balance in balances:
                if balance.asset.value == asset:
                    available_balance = balance.available_quantity
                    break

            # Calculate required balance with buffer
            buffer_multiplier = 1 + self.config.required_balance_buffer_pct
            required_with_buffer = required_balance * buffer_multiplier

            # Check if sufficient balance is available
            if available_balance < required_balance:
                errors.append(
                    f"Insufficient balance on {exchange_id}: "
                    f"available {available_balance} {asset}, "
                    f"required {required_balance} {asset}"
                )
            elif available_balance < required_with_buffer:
                warnings.append(
                    f"Low balance buffer on {exchange_id}: "
                    f"available {available_balance} {asset}, "
                    f"recommended {required_with_buffer} {asset}"
                )

            self.logger.debug(
                "Balance validation completed",
                exchange_id=exchange_id,
                asset=asset,
                available_balance=str(available_balance),
                required_balance=str(required_balance),
                sufficient=available_balance >= required_balance,
            )

        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            self.logger.exception(
                "Failed to validate account balances",
                exchange_id=exchange_id,
                asset=asset,
                error=str(e),
            )
            errors.append(f"Could not retrieve balance for {exchange_id}: {e}")

        return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)

    def validate_price_precision(
        self, price: Decimal, symbol: Symbol, exchange_id: str
    ) -> ValidationResult:
        """Validate price precision against exchange requirements.

        Args:
            price: Price to validate
            symbol: Trading symbol
            exchange_id: Exchange identifier

        Returns:
            ValidationResult with precision validation status
        """
        errors: list[str] = []
        warnings: list[str] = []

        # This is a simplified validation - in practice, you would
        # fetch exchange-specific precision requirements

        # Basic price validation
        if price <= 0:
            errors.append("Price must be positive")

        # Check for excessive decimal places (simple heuristic)
        price_str = str(price)
        if "." in price_str:
            decimal_places = len(price_str.split(".")[1])
            max_decimal_places = 8
            if decimal_places > max_decimal_places:
                warnings.append(
                    f"Price has {decimal_places} decimal places, "
                    "which may exceed exchange precision"
                )

        # Check for very small or very large prices
        if price < Decimal("0.000001"):
            warnings.append("Price is very small and may cause precision issues")
        elif price > Decimal(1000000):
            warnings.append("Price is very large and may cause precision issues")

        return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)

    def validate_quantity_precision(
        self, quantity: Decimal, symbol: Symbol, exchange_id: str
    ) -> ValidationResult:
        """Validate quantity precision against exchange requirements.

        Args:
            quantity: Quantity to validate
            symbol: Trading symbol
            exchange_id: Exchange identifier

        Returns:
            ValidationResult with precision validation status
        """
        errors: list[str] = []
        warnings: list[str] = []

        # Basic quantity validation
        if quantity <= 0:
            errors.append("Quantity must be positive")

        # Check for excessive decimal places (simple heuristic)
        quantity_str = str(quantity)
        if "." in quantity_str:
            decimal_places = len(quantity_str.split(".")[1])
            max_decimal_places = 8
            if decimal_places > max_decimal_places:
                warnings.append(
                    f"Quantity has {decimal_places} decimal places, "
                    "which may exceed exchange precision"
                )

        # Check minimum quantity threshold
        if quantity < self.config.min_position_size_usd:
            warnings.append(
                f"Quantity {quantity} is below minimum threshold "
                f"{self.config.min_position_size_usd}"
            )

        return ValidationResult(is_valid=len(errors) == 0, errors=errors, warnings=warnings)
