"""Unified symbol validation system.

This module provides comprehensive validation for symbols across different
contexts (internal, exchange, WebSocket) with exchange-specific rules and
error correction suggestions.
"""

import re
from typing import Any, ClassVar

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums.enums import MarketType
from cyberdelta.core.symbols.exceptions import SymbolError, SymbolValidationError
from cyberdelta.core.symbols.models import (
    ExchangeSymbol,
    InternalSymbol,
    SymbolFormat,
    SymbolType,
    UnifiedSymbol,
)

# Import service function at module level to avoid circular imports
from cyberdelta.core.symbols.service import SymbolService
from cyberdelta.enums.exchange_names import ExchangeName


logger = get_logger(__name__)


# Constants for validation
MAX_HYPHEN_COUNT = 2
MIN_PAIR_LENGTH = 6
MIN_SYMBOL_PARTS = 2
MIN_MULTI_EXCHANGE_SYMBOLS = 2


class SymbolValidator:
    """Unified symbol validation system with exchange-specific rules."""

    # Precompiled patterns for performance
    _PATTERNS: ClassVar[dict[SymbolType, Any]] = {
        SymbolType.INTERNAL: SymbolFormat.PATTERNS[SymbolType.INTERNAL],
        SymbolType.EXCHANGE: SymbolFormat.PATTERNS[SymbolType.EXCHANGE],
        SymbolType.WEBSOCKET: SymbolFormat.PATTERNS[SymbolType.WEBSOCKET],
        SymbolType.CONFIGURATION: SymbolFormat.PATTERNS[SymbolType.CONFIGURATION],
    }

    # Exchange-specific patterns
    _HYPERLIQUID_INDEX_PATTERN = re.compile(r"^@\d+$")
    _BACKPACK_PAIR_PATTERN = re.compile(r"^[A-Z0-9]+_[A-Z0-9]+$")
    _BACKPACK_PERP_PATTERN = re.compile(r"^[A-Z0-9]+_PERP$")

    @classmethod
    def validate_symbol(
        cls,
        value: str | int,
        symbol_type: SymbolType,
        exchange_id: ExchangeName | None = None,
        field_name: str | None = None,
    ) -> str:
        """Validate symbol with unified rules and exchange-specific logic.

        Args:
            value: Symbol value to validate
            symbol_type: Type of symbol for context
            exchange_id: Optional exchange for specific rules
            field_name: Optional field name for WebSocket integer handling

        Returns:
            Validated and normalized symbol string

        Raises:
            SymbolValidationError: If validation fails
        """
        # Handle integer symbols (Backpack WebSocket case)
        if isinstance(value, int):
            if exchange_id == ExchangeName.BACKPACK and field_name in {"symbol", "s"}:
                value = str(value)
            else:
                raise SymbolValidationError(
                    str(value),
                    f"Integer symbols only supported for Backpack WebSocket fields, "
                    f"got {field_name}",
                    details={
                        "value": value,
                        "value_type": "int",
                        "exchange_id": exchange_id.value if exchange_id else None,
                        "field_name": field_name,
                    },
                )

        # At this point, value is guaranteed to be a string (converted above if int)
        # Basic normalization
        value = value.strip().upper()

        if not value:
            raise SymbolValidationError("", "Symbol cannot be empty")

        # Length validation
        max_length = SymbolFormat.MAX_LENGTHS.get(symbol_type, SymbolFormat.MAX_SYMBOL_LENGTH)
        if len(value) > max_length:
            raise SymbolValidationError(
                value,
                f"Symbol exceeds max length {max_length}",
                expected_format=f"Length <= {max_length}",
                details={"length": len(value), "max_length": max_length},
            )

        # Pattern validation
        pattern = cls._PATTERNS[symbol_type]
        if not pattern.match(value):
            raise SymbolValidationError(
                value,
                f"Symbol doesn't match pattern for {symbol_type.value}",
                expected_format=pattern.pattern,
                details={
                    "symbol_type": symbol_type.value,
                    "pattern": pattern.pattern,
                },
            )

        # Exchange-specific validation
        if exchange_id:
            cls._validate_exchange_specific(value, exchange_id, symbol_type)

        # By this point, value is guaranteed to be a string
        return value

    @classmethod
    def _validate_exchange_specific(
        cls,
        value: str,
        exchange_id: ExchangeName,
        symbol_type: SymbolType,
    ) -> None:
        """Apply exchange-specific validation rules."""
        if exchange_id == ExchangeName.HYPERLIQUID:
            cls._validate_hyperliquid(value, symbol_type)
        elif exchange_id == ExchangeName.BACKPACK:
            cls._validate_backpack(value, symbol_type)

    @classmethod
    def _validate_hyperliquid(cls, value: str, symbol_type: SymbolType) -> None:
        """Hyperliquid-specific validation rules.
        
        Validates Hyperliquid exchange symbols including:
        - Asset index format (@N for spot assets)
        - Hyphen count restrictions for non-index symbols
        
        Args:
            value: Symbol value to validate
            symbol_type: Type of symbol being validated
            
        Raises:
            SymbolValidationError: If symbol violates Hyperliquid-specific rules
        """
        if symbol_type == SymbolType.EXCHANGE:
            # Allow @N format for spot assets
            if value.startswith("@"):
                if not cls._HYPERLIQUID_INDEX_PATTERN.match(value):
                    raise SymbolValidationError(
                        value,
                        "Invalid Hyperliquid asset index format",
                        expected_format="@{number}",
                        details={"value": value, "exchange": "hyperliquid"},
                    )

            # Check for invalid characters in non-index format
            elif "-" in value and value.count("-") > MAX_HYPHEN_COUNT:
                raise SymbolValidationError(
                    value,
                    "Too many hyphens in Hyperliquid symbol",
                    expected_format="BASE or BASE-QUOTE or BASE-QUOTE-TYPE",
                    details={"value": value, "exchange": "hyperliquid"},
                )

    @classmethod
    def _validate_backpack(cls, value: str, symbol_type: SymbolType) -> None:
        """Backpack-specific validation rules.
        
        Raises:
            SymbolValidationError: If validation fails
        """
        if symbol_type == SymbolType.EXCHANGE:
            # Check for proper underscore usage
            if "_" in value:
                # Should be either BASE_QUOTE or BASE_PERP format
                if not (
                    cls._BACKPACK_PAIR_PATTERN.match(value)
                    or cls._BACKPACK_PERP_PATTERN.match(value)
                ):
                    raise SymbolValidationError(
                        value,
                        "Invalid Backpack symbol format",
                        expected_format="BASE_QUOTE or BASE_PERP",
                        details={"value": value, "exchange": "backpack"},
                    )

            # Require underscores for pairs (except perpetuals ending in PERP)
            elif len(value) > MIN_PAIR_LENGTH and not value.endswith("PERP"):
                # Likely a pair without proper underscore
                raise SymbolValidationError(
                    value,
                    "Backpack symbols require underscore separation",
                    expected_format="BASE_QUOTE or use PERP suffix",
                    details={"value": value, "exchange": "backpack"},
                )

    @classmethod
    def validate_symbol_pair(
        cls,
        base: str,
        quote: str,
        exchange_id: ExchangeName | None = None,
    ) -> dict[str, str]:
        """Validate a symbol pair (base/quote assets).

        Args:
            base: Base asset symbol
            quote: Quote asset symbol
            exchange_id: Optional exchange for specific rules

        Returns:
            Dictionary with validated base, quote, and pair

        Raises:
            SymbolValidationError: If validation fails
        """
        # Validate individual assets as internal symbols
        validated_base = cls.validate_symbol(base, SymbolType.INTERNAL)
        validated_quote = cls.validate_symbol(quote, SymbolType.INTERNAL)

        # Check for same asset
        if validated_base == validated_quote:
            raise SymbolValidationError(
                f"{validated_base}/{validated_quote}",
                "Base and quote assets cannot be the same",
                details={"base": validated_base, "quote": validated_quote},
            )

        # Create pair based on exchange conventions
        if exchange_id == ExchangeName.BACKPACK:
            pair = f"{validated_base}_{validated_quote}"
        elif exchange_id == ExchangeName.HYPERLIQUID:
            pair = f"{validated_base}/{validated_quote}"
        else:
            pair = f"{validated_base}_{validated_quote}"  # Default format

        return {
            "base": validated_base,
            "quote": validated_quote,
            "pair": pair,
        }

    @classmethod
    def is_valid_symbol(
        cls,
        value: str | int,
        symbol_type: SymbolType,
        exchange_id: ExchangeName | None = None,
        field_name: str | None = None,
    ) -> bool:
        """Check if symbol is valid without raising exceptions.

        Args:
            value: Symbol value to check
            symbol_type: Type of symbol
            exchange_id: Optional exchange
            field_name: Optional field name

        Returns:
            True if valid, False otherwise
        """
        try:
            cls.validate_symbol(value, symbol_type, exchange_id, field_name)
        except SymbolValidationError:
            return False
        else:
            return True

    @classmethod
    def validate_websocket_symbol(
        cls,
        value: str | int,
        field_name: str,
        exchange_id: ExchangeName,
    ) -> str:
        """Validate symbol from WebSocket with integer support.

        Args:
            value: Symbol value (string or integer)
            field_name: Field name for context
            exchange_id: Exchange identifier

        Returns:
            Validated symbol string
        """
        return cls.validate_symbol(
            value,
            SymbolType.WEBSOCKET,
            exchange_id,
            field_name,
        )


class CrossExchangeValidator:
    """Validate symbol compatibility across multiple exchanges."""

    @staticmethod
    def validate_arbitrage_pair(
        internal_symbol: str,
        exchange_ids: list[ExchangeName],
    ) -> dict[str, Any]:
        """Validate symbol is available on all required exchanges.

        Args:
            internal_symbol: Internal symbol to check
            exchange_ids: List of exchanges to validate against

        Returns:
            Dictionary with validation results
        """
        service = SymbolService()
        results: dict[str, Any] = {
            "valid": True,
            "internal_symbol": internal_symbol,
            "exchanges": {},
            "issues": [],
        }

        for exchange_id in exchange_ids:
            try:
                exchange_symbol = service.get_exchange_symbol(internal_symbol, exchange_id.value)
                results["exchanges"][exchange_id.value] = {
                    "available": True,
                    "symbol": exchange_symbol.value,
                    "asset_index": exchange_symbol.asset_index,
                    "symbol_id": exchange_symbol.symbol_id,
                }
            except SymbolError as e:
                results["valid"] = False
                results["exchanges"][exchange_id.value] = {
                    "available": False,
                    "error": str(e),
                }
                results["issues"].append(
                    f"Symbol '{internal_symbol}' not available on {exchange_id.value}: {e}"
                )

        return results


class DomainObjectValidator:
    """Validation utilities for symbol domain objects."""

    @staticmethod
    def _validate_basic_internal_fields(symbol: InternalSymbol) -> list[str]:
        """Validate basic internal symbol fields.
        
        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        if not symbol.value:
            errors.append("Internal symbol value must be a non-empty string")

        if not symbol.base_asset:
            errors.append("Base asset must be a non-empty string")

        return errors

    @staticmethod
    def _validate_internal_business_logic(symbol: InternalSymbol) -> list[str]:
        """Validate internal symbol business logic.
        
        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        if symbol.market_type == MarketType.SPOT and symbol.quote_asset is None:
            errors.append("SPOT market type requires a quote asset")

        if symbol.market_type == MarketType.PERP and symbol.quote_asset != "USD":
            errors.append("PERP market type should have USD as quote asset")

        return errors

    @staticmethod
    def _validate_internal_computed_fields(symbol: InternalSymbol) -> list[str]:
        """Validate internal symbol computed fields.
        
        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        try:
            canonical_name = symbol.canonical_name
            if not canonical_name:
                errors.append("Canonical name computation failed")
        except (AttributeError, ValueError) as e:
            errors.append(f"Canonical name computation error: {e}")

        try:
            is_pair = symbol.is_pair
            if symbol.quote_asset and not is_pair:
                errors.append("Symbol with quote asset should be detected as pair")
        except (AttributeError, ValueError) as e:
            errors.append(f"Pair detection computation error: {e}")

        return errors

    @staticmethod
    def _validate_internal_format_consistency(symbol: InternalSymbol) -> list[str]:
        """Validate internal symbol format consistency.
        
        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        if symbol.quote_asset:
            expected_format = f"{symbol.base_asset}_{symbol.quote_asset}"
            if symbol.value != expected_format:
                errors.append(
                    f"Symbol value '{symbol.value}' doesn't match "
                    f"expected format '{expected_format}'"
                )

        return errors

    @staticmethod
    def validate_internal_symbol(symbol: InternalSymbol) -> list[str]:
        """Validate InternalSymbol domain object integrity.

        Args:
            symbol: InternalSymbol to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors: list[str] = []

        # Delegate to helper methods to reduce complexity
        errors.extend(DomainObjectValidator._validate_basic_internal_fields(symbol))
        errors.extend(DomainObjectValidator._validate_internal_business_logic(symbol))
        errors.extend(DomainObjectValidator._validate_internal_computed_fields(symbol))
        errors.extend(DomainObjectValidator._validate_internal_format_consistency(symbol))

        logger.debug(
            "internal_symbol_validation_completed",
            symbol_value=symbol.value,
            base_asset=symbol.base_asset,
            quote_asset=symbol.quote_asset,
            market_type=symbol.market_type.value,
            error_count=len(errors),
            is_valid=len(errors) == 0,
        )

        return errors

    @staticmethod
    def _validate_exchange_basic_fields(symbol: ExchangeSymbol) -> list[str]:
        """Validate basic exchange symbol fields.
        
        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        if not symbol.value:
            errors.append("Exchange symbol value must be a non-empty string")

        return errors

    @staticmethod
    def _validate_exchange_specific_fields(symbol: ExchangeSymbol) -> list[str]:
        """Validate exchange-specific fields.
        
        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        # Asset index validation (Hyperliquid specific)
        if symbol.asset_index is not None:
            if symbol.asset_index < 0:
                errors.append("Asset index must be a non-negative integer")

            if symbol.exchange_id != ExchangeName.HYPERLIQUID:
                errors.append("Asset index is only valid for Hyperliquid exchange")

        # Symbol ID validation (Backpack specific)
        if symbol.symbol_id is not None:
            if symbol.symbol_id < 0:
                errors.append("Symbol ID must be a non-negative integer")

            if symbol.exchange_id != ExchangeName.BACKPACK:
                errors.append("Symbol ID is only valid for Backpack exchange")

        return errors

    @staticmethod
    def _validate_exchange_format(symbol: ExchangeSymbol) -> list[str]:
        """Validate exchange-specific format rules.
        
        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        if symbol.exchange_id == ExchangeName.HYPERLIQUID:
            if not (symbol.value.count("-") <= MAX_HYPHEN_COUNT or symbol.value.startswith("@")):
                errors.append(
                    "Hyperliquid symbol format invalid (too many hyphens or invalid index)"
                )

        elif (
            symbol.exchange_id == ExchangeName.BACKPACK
            and "_" in symbol.value
            and not re.match(r"^[A-Z0-9]+_[A-Z0-9]+$", symbol.value)
        ):
            errors.append("Backpack symbol format invalid (underscore usage)")

        return errors

    @staticmethod
    def _validate_exchange_computed_fields(symbol: ExchangeSymbol) -> list[str]:
        """Validate exchange symbol computed fields.
        
        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        try:
            is_indexed = symbol.is_indexed
            if symbol.asset_index is not None and not is_indexed:
                errors.append("Symbol with asset index should be detected as indexed")
        except (AttributeError, ValueError) as e:
            errors.append(f"Indexed detection computation error: {e}")

        return errors

    @staticmethod
    def validate_exchange_symbol(symbol: ExchangeSymbol) -> list[str]:
        """Validate ExchangeSymbol domain object integrity.

        Args:
            symbol: ExchangeSymbol to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors: list[str] = []

        # Delegate to helper methods to reduce complexity
        errors.extend(DomainObjectValidator._validate_exchange_basic_fields(symbol))
        errors.extend(DomainObjectValidator._validate_exchange_specific_fields(symbol))

        # Internal symbol validation if present
        if symbol.internal_symbol:
            internal_errors = DomainObjectValidator.validate_internal_symbol(symbol.internal_symbol)
            if internal_errors:
                errors.extend([f"Internal symbol error: {err}" for err in internal_errors])

        errors.extend(DomainObjectValidator._validate_exchange_format(symbol))
        errors.extend(DomainObjectValidator._validate_exchange_computed_fields(symbol))

        logger.debug(
            "exchange_symbol_validation_completed",
            symbol_value=symbol.value,
            exchange_id=symbol.exchange_id.value,
            has_asset_index=symbol.asset_index is not None,
            has_symbol_id=symbol.symbol_id is not None,
            has_internal_symbol=symbol.internal_symbol is not None,
            error_count=len(errors),
            is_valid=len(errors) == 0,
        )

        return errors

    @staticmethod
    def _validate_unified_exchange_mappings(symbol: UnifiedSymbol) -> list[str]:
        """Validate unified symbol exchange mappings.
        
        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        if not symbol.exchange_mappings:
            errors.append("Unified symbol must have at least one exchange mapping")
            return errors

        for exchange_name, exchange_symbol in symbol.exchange_mappings.items():
            # Validate each exchange symbol
            exchange_errors = DomainObjectValidator.validate_exchange_symbol(exchange_symbol)
            if exchange_errors:
                errors.extend([
                    f"Exchange '{exchange_name}' error: {err}" for err in exchange_errors
                ])

            # Consistency check: exchange symbol should reference this internal symbol
            if (
                exchange_symbol.internal_symbol
                and exchange_symbol.internal_symbol.value != symbol.internal.value
            ):
                errors.append(f"Exchange '{exchange_name}' internal symbol mismatch")

        return errors

    @staticmethod
    def _validate_unified_trading_specs(symbol: UnifiedSymbol) -> list[str]:
        """Validate unified symbol trading specifications.
        
        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        if symbol.tick_size is not None and symbol.tick_size <= 0:
            errors.append("Tick size must be a positive Decimal")

        if symbol.min_order_size is not None and symbol.min_order_size <= 0:
            errors.append("Min order size must be a positive Decimal")

        if symbol.max_order_size is not None:
            if symbol.max_order_size <= 0:
                errors.append("Max order size must be a positive Decimal")

            if symbol.min_order_size and symbol.max_order_size < symbol.min_order_size:
                errors.append("Max order size must be greater than or equal to min order size")

        return errors

    @staticmethod
    def _validate_unified_timestamps(symbol: UnifiedSymbol) -> list[str]:
        """Validate unified symbol timestamps.
        
        Returns:
            List of validation error messages
        """
        errors: list[str] = []

        if symbol.updated_at and symbol.created_at and symbol.updated_at < symbol.created_at:
            errors.append("updated_at cannot be before created_at")

        return errors

    @staticmethod
    def validate_unified_symbol(symbol: UnifiedSymbol) -> list[str]:
        """Validate UnifiedSymbol domain object integrity.

        Args:
            symbol: UnifiedSymbol to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors: list[str] = []

        # Internal symbol validation
        internal_errors = DomainObjectValidator.validate_internal_symbol(symbol.internal)
        if internal_errors:
            errors.extend([f"Internal symbol error: {err}" for err in internal_errors])

        # Delegate to helper methods to reduce complexity
        errors.extend(DomainObjectValidator._validate_unified_exchange_mappings(symbol))
        errors.extend(DomainObjectValidator._validate_unified_trading_specs(symbol))
        errors.extend(DomainObjectValidator._validate_unified_timestamps(symbol))

        logger.debug(
            "unified_symbol_validation_completed",
            internal_symbol=symbol.internal.value,
            exchange_count=len(symbol.exchange_mappings),
            has_tick_size=symbol.tick_size is not None,
            has_min_order_size=symbol.min_order_size is not None,
            has_max_order_size=symbol.max_order_size is not None,
            is_active=symbol.is_active,
            is_tradeable=symbol.is_tradeable,
            error_count=len(errors),
            is_valid=len(errors) == 0,
        )

        return errors

    @staticmethod
    def validate_symbol_object(
        symbol: InternalSymbol | ExchangeSymbol | UnifiedSymbol,
    ) -> list[str]:
        """Validate any symbol domain object.

        Args:
            symbol: Symbol domain object to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        if isinstance(symbol, InternalSymbol):
            return DomainObjectValidator.validate_internal_symbol(symbol)
        if isinstance(symbol, ExchangeSymbol):
            return DomainObjectValidator.validate_exchange_symbol(symbol)
        # Must be UnifiedSymbol based on type hints
        return DomainObjectValidator.validate_unified_symbol(symbol)

    @staticmethod
    def is_valid_symbol_object(symbol: InternalSymbol | ExchangeSymbol | UnifiedSymbol) -> bool:
        """Check if symbol domain object is valid.

        Args:
            symbol: Symbol domain object to check

        Returns:
            True if valid, False otherwise
        """
        errors = DomainObjectValidator.validate_symbol_object(symbol)
        return len(errors) == 0


class ArbitrageValidator:
    """Advanced validation for arbitrage-specific symbol operations."""

    @staticmethod
    def validate_arbitrage_symbol_pair(
        long_symbol: ExchangeSymbol,
        short_symbol: ExchangeSymbol,
        internal_symbol: InternalSymbol | None = None,
    ) -> list[str]:
        """Validate symbol pair for arbitrage compatibility.

        Args:
            long_symbol: Long position exchange symbol
            short_symbol: Short position exchange symbol
            internal_symbol: Optional internal symbol for consistency checks

        Returns:
            List of validation error messages (empty if valid)
        """
        errors: list[str] = []

        # Validate individual symbols
        long_errors = DomainObjectValidator.validate_exchange_symbol(long_symbol)
        if long_errors:
            errors.extend([f"Long symbol error: {err}" for err in long_errors])

        short_errors = DomainObjectValidator.validate_exchange_symbol(short_symbol)
        if short_errors:
            errors.extend([f"Short symbol error: {err}" for err in short_errors])

        # Cross-exchange arbitrage validation
        if long_symbol.exchange_id == short_symbol.exchange_id:
            errors.append("Arbitrage requires different exchanges for long and short positions")

        # Internal symbol consistency
        if internal_symbol:
            if (
                long_symbol.internal_symbol
                and long_symbol.internal_symbol.value != internal_symbol.value
            ):
                errors.append("Long symbol internal symbol mismatch")

            if (
                short_symbol.internal_symbol
                and short_symbol.internal_symbol.value != internal_symbol.value
            ):
                errors.append("Short symbol internal symbol mismatch")

        # Market type consistency (both should be same type)
        if (
            long_symbol.internal_symbol
            and short_symbol.internal_symbol
            and long_symbol.internal_symbol.market_type != short_symbol.internal_symbol.market_type
        ):
            errors.append("Long and short symbols must have same market type for arbitrage")

        # Exchange-specific arbitrage rules
        if (
            long_symbol.exchange_id == ExchangeName.HYPERLIQUID
            and long_symbol.asset_index is None
            and not long_symbol.value.endswith("-PERP")
        ):
            errors.append("Hyperliquid perpetual symbols should end with -PERP")

        logger.info(
            "arbitrage_pair_validation_completed",
            long_exchange=long_symbol.exchange_id.value,
            short_exchange=short_symbol.exchange_id.value,
            long_symbol=long_symbol.value,
            short_symbol=short_symbol.value,
            cross_exchange=long_symbol.exchange_id != short_symbol.exchange_id,
            error_count=len(errors),
            is_valid=len(errors) == 0,
        )

        return errors

    @staticmethod
    def validate_multi_exchange_compatibility(
        internal_symbol: str, exchange_symbols: list[ExchangeSymbol]
    ) -> list[str]:
        """Validate symbol compatibility across multiple exchanges.

        Args:
            internal_symbol: Internal symbol name
            exchange_symbols: List of exchange symbols to validate

        Returns:
            List of validation error messages (empty if valid)
        """
        errors: list[str] = []

        if len(exchange_symbols) < MIN_MULTI_EXCHANGE_SYMBOLS:
            errors.append("Multi-exchange validation requires at least 2 exchange symbols")
            return errors

        # Validate each exchange symbol
        for i, exchange_symbol in enumerate(exchange_symbols):
            symbol_errors = DomainObjectValidator.validate_exchange_symbol(exchange_symbol)
            if symbol_errors:
                errors.extend([f"Exchange symbol {i} error: {err}" for err in symbol_errors])

        # Check for duplicate exchanges
        exchanges = [symbol.exchange_id for symbol in exchange_symbols]
        if len(set(exchanges)) != len(exchanges):
            errors.append("Duplicate exchanges detected in symbol list")

        # Internal symbol consistency
        for i, exchange_symbol in enumerate(exchange_symbols):
            if (
                exchange_symbol.internal_symbol
                and exchange_symbol.internal_symbol.value != internal_symbol
            ):
                errors.append(f"Exchange symbol {i} internal symbol mismatch")

        # Market type consistency
        market_types: set[MarketType] = set()
        for exchange_symbol in exchange_symbols:
            if exchange_symbol.internal_symbol:
                market_types.add(exchange_symbol.internal_symbol.market_type)

        if len(market_types) > 1:
            errors.append("All exchange symbols must have same market type")

        logger.info(
            "multi_exchange_validation_completed",
            internal_symbol=internal_symbol,
            exchange_count=len(exchange_symbols),
            unique_exchanges=len(set(exchanges)),
            market_types=len(market_types),
            error_count=len(errors),
            is_valid=len(errors) == 0,
        )

        return errors


# Convenience functions for common validation patterns
def validate_domain_object(symbol: InternalSymbol | ExchangeSymbol | UnifiedSymbol) -> bool:
    """Quick validation check for any symbol domain object.

    Args:
        symbol: Symbol domain object to validate

    Returns:
        True if valid, False otherwise
    """
    return DomainObjectValidator.is_valid_symbol_object(symbol)


def get_validation_errors(symbol: InternalSymbol | ExchangeSymbol | UnifiedSymbol) -> list[str]:
    """Get detailed validation errors for symbol domain object.

    Args:
        symbol: Symbol domain object to validate

    Returns:
        List of validation error messages
    """
    return DomainObjectValidator.validate_symbol_object(symbol)


def validate_arbitrage_pair(long_symbol: ExchangeSymbol, short_symbol: ExchangeSymbol) -> bool:
    """Quick arbitrage pair validation.

    Args:
        long_symbol: Long position exchange symbol
        short_symbol: Short position exchange symbol

    Returns:
        True if valid arbitrage pair, False otherwise
    """
    errors = ArbitrageValidator.validate_arbitrage_symbol_pair(long_symbol, short_symbol)
    return len(errors) == 0
